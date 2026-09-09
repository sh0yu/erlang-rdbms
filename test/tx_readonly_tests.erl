%%%-------------------------------------------------------------------
%%% 読み取り専用トランザクションの並行実行。
%%%
%%% これまで tx_mng は同時に1本しかトランザクションを動かさなかった。
%%% 読み取り専用の照会同士すら互いを待っていた。
%%%
%%% 緩めるための前提は「コミットの適用を読み手から見て原子的にすること」。
%%% apply_changes/1 は1行ずつ書き、走査は data_buffer への呼び出しを
%%% 繰り返すので、その隙間に適用が挟まると読み手がコミットの途中を見る。
%%%
%%% はじめはラッチで囲っていたが、それだと長い読み手がコミットを止める。
%%% いまは snapshot_mng が変更前の値(undo)を持ち、読み手は自分が始めた
%%% 時点の値へ巻き戻して見る。**読み手と書き手は互いを待たない。**
%%%-------------------------------------------------------------------
-module(tx_readonly_tests).

-include_lib("eunit/include/eunit.hrl").

readonly_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun select_works_in_read_only/1,
      fun writes_are_rejected/1,
      fun ddl_is_rejected/1,
      fun readers_do_not_block_each_other/1,
      fun writers_run_concurrently/1,
      fun commit_does_not_wait_for_readers/1,
      fun reader_sees_all_or_nothing_of_a_commit/1,
      fun reader_sees_old_values_after_update/1,
      fun reader_sees_deleted_rows/1,
      fun reader_uses_index_and_still_sees_old_values/1,
      fun snapshot_is_released_on_commit_and_rollback/1,
      fun snapshot_is_released_when_connection_dies/1,
      fun nested_begin_is_rejected/1]}.

select_works_in_read_only(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        ?assertMatch({ok, _, [[1], [2]]}, q(C, "SELECT id FROM t ORDER BY id")),
        ok = q(C, "COMMIT")
    end.

writes_are_rejected(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        ?assertEqual({error, read_only_transaction},
                     q(C, "INSERT INTO t VALUES (9)")),
        ?assertEqual({error, read_only_transaction},
                     q(C, "UPDATE t SET id = 9 WHERE id = 1")),
        ?assertEqual({error, read_only_transaction},
                     q(C, "DELETE FROM t WHERE id = 1")),
        ok = q(C, "ROLLBACK"),
        %% 断られただけで、状態は壊れていない
        ?assertEqual(2, count(C))
    end.

ddl_is_rejected(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        ?assertEqual({error, ddl_in_transaction}, q(C, "CREATE TABLE u (a INTEGER)")),
        ok = q(C, "ROLLBACK")
    end.

nested_begin_is_rejected(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        ?assertEqual({error, transaction_already_started}, q(C, "BEGIN READ ONLY")),
        ?assertEqual({error, transaction_already_started}, q(C, "BEGIN")),
        ok = q(C, "ROLLBACK")
    end.

%%%===================================================================
%%% 並行性
%%%===================================================================

%% **これがこの変更の目的。** 読み手が開いたままでも、別の読み手が走る。
%% 以前はここで2本目が1本目のCOMMITまで止まっていた。
readers_do_not_block_each_other(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN READ ONLY"),
        {ok, _, _} = q(C1, "SELECT id FROM t"),
        %% C1 は開いたまま。C2 が待たされないこと
        R = async(fun() ->
                          ok = q(C2, "BEGIN READ ONLY"),
                          {ok, _, _} = q(C2, "SELECT id FROM t"),
                          q(C2, "ROLLBACK")
                  end),
        ?assertEqual({ok, ok}, await(R, 1000)),
        ?assertMatch(#{snapshots := 1}, snapshot_mng:status()),
        ok = q(C1, "COMMIT"),
        ?assertMatch(#{snapshots := 0}, snapshot_mng:status())
    end.

%% 読み書きトランザクションも並行に走る。
%% 待つのは同じ行を書こうとしたときだけ。
writers_run_concurrently(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        {ok, _} = q(C1, "INSERT INTO t VALUES (3)"),
        Done = async(fun() ->
                             ok = q(C2, "BEGIN"),
                             {ok, _} = q(C2, "INSERT INTO t VALUES (4)"),
                             q(C2, "COMMIT")
                     end),
        ?assertEqual({ok, ok}, await(Done, 5000)),
        ok = q(C1, "COMMIT"),
        ?assertEqual(4, count(C1))
    end.

%% **これがスナップショットにした目的。** 読み手が開いていても
%% コミットは通る。以前はここでラッチ待ちになり、長い照会がある間
%% 書き込みが一切進まなかった。
commit_does_not_wait_for_readers(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN READ ONLY"),
        {ok, _, _} = q(C1, "SELECT id FROM t"),

        ok = q(C2, "BEGIN"),
        {ok, _} = q(C2, "INSERT INTO t VALUES (3)"),
        Done = async(fun() -> q(C2, "COMMIT") end),
        ?assertEqual({ok, ok}, await(Done, 5000)),

        %% コミットは済んでいるが、開いたままの読み手には見えない
        ?assertEqual(2, count_in(C1)),
        ok = q(C1, "COMMIT"),
        %% 閉じて開き直せば見える
        ?assertEqual(3, count(C1))
    end.

%% 読み手は、あるコミットの全部を見るか、全部を見ないか。
%% 同じトランザクションの中では何度読んでも同じ状態が見える。
reader_sees_all_or_nothing_of_a_commit(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN READ ONLY"),
        ?assertEqual(2, count_in(C1)),

        %% 別の接続が3行入れてコミットする(もう待たされない)
        Parent = self(),
        spawn(fun() ->
                      ok = q(C2, "BEGIN"),
                      {ok, _} = q(C2, "INSERT INTO t VALUES (10)"),
                      {ok, _} = q(C2, "INSERT INTO t VALUES (11)"),
                      {ok, _} = q(C2, "INSERT INTO t VALUES (12)"),
                      Parent ! ready,
                      ok = q(C2, "COMMIT"),
                      Parent ! committed
              end),
        receive ready -> ok after 2000 -> error(timeout) end,

        %% 何度読んでも 2 のまま。途中(3行のうち1行だけ)は見えない
        ?assertEqual(2, count_in(C1)),
        ?assertEqual(2, count_in(C1)),
        ok = q(C1, "COMMIT"),

        receive committed -> ok after 5000 -> error(commit_stuck) end,
        %% 読んでいる間にコミットは済んでいたが、見えていたのは2行のまま
        ?assertEqual(5, count(C1))
    end.

%% 更新は「消して入れ直す」ではなく値の巻き戻しで見える。
reader_sees_old_values_after_update(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN READ ONLY"),
        ?assertMatch({ok, _, [[1], [2]]}, q(C1, "SELECT id FROM t ORDER BY id")),

        ok = q(C2, "BEGIN"),
        {ok, _} = q(C2, "UPDATE t SET id = 99 WHERE id = 1"),
        ok = q(C2, "COMMIT"),

        %% 読み手には昔の値のまま
        ?assertMatch({ok, _, [[1], [2]]}, q(C1, "SELECT id FROM t ORDER BY id")),
        ok = q(C1, "COMMIT"),
        ?assertMatch({ok, _, [[2], [99]]}, one_shot(C1, "SELECT id FROM t ORDER BY id"))
    end.

%% 消された行も、スナップショットの時点では在ったので見える。
reader_sees_deleted_rows(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN READ ONLY"),
        ?assertEqual(2, count_in(C1)),

        ok = q(C2, "BEGIN"),
        {ok, _} = q(C2, "DELETE FROM t WHERE id = 1"),
        ok = q(C2, "COMMIT"),

        ?assertEqual(2, count_in(C1)),
        ?assertMatch({ok, _, [[1], [2]]}, q(C1, "SELECT id FROM t ORDER BY id")),
        ok = q(C1, "COMMIT"),
        ?assertEqual(1, count(C1))
    end.

%% 索引は現在の値で引かれるので、そのままでは巻き戻せない。
%% 候補を「いま索引に出る行」+「スナップショット以降に変わった行」に
%% 広げてから絞り直すので、結果は全表走査と一致する。
reader_uses_index_and_still_sees_old_values(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "CREATE INDEX t_id ON t (id)"),
        Sql = "SELECT id FROM t WHERE id = 1",
        ?assert(uses_index(one_shot(C1, "EXPLAIN " ++ Sql))),

        ok = q(C1, "BEGIN READ ONLY"),
        ?assertMatch({ok, _, [[1]]}, q(C1, Sql)),

        %% 索引付きの列を書き換える。索引からは id=1 が消える
        ok = q(C2, "BEGIN"),
        {ok, _} = q(C2, "UPDATE t SET id = 7 WHERE id = 1"),
        ok = q(C2, "COMMIT"),

        %% 索引に無くなった行が、それでも見える
        ?assertMatch({ok, _, [[1]]}, q(C1, Sql)),
        %% 逆に、新しく id=7 になった行は見えてはいけない
        ?assertMatch({ok, _, []}, q(C1, "SELECT id FROM t WHERE id = 7")),
        ok = q(C1, "COMMIT"),

        ?assertMatch({ok, _, []}, one_shot(C1, Sql)),
        ?assertMatch({ok, _, [[7]]}, one_shot(C1, "SELECT id FROM t WHERE id = 7"))
    end.

snapshot_is_released_on_commit_and_rollback(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        ?assertMatch(#{snapshots := 1}, snapshot_mng:status()),
        ok = q(C, "COMMIT"),
        ?assertMatch(#{snapshots := 0}, snapshot_mng:status()),
        ok = q(C, "BEGIN READ ONLY"),
        ?assertMatch(#{snapshots := 1}, snapshot_mng:status()),
        ok = q(C, "ROLLBACK"),
        ?assertMatch(#{snapshots := 0}, snapshot_mng:status())
    end.

%% スナップショットを持ったまま接続が死んでも、undo が溜まり続けない。
%% 誰も外さないと、その版より新しい undo を永久に捨てられなくなる。
snapshot_is_released_when_connection_dies(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C2, "BEGIN READ ONLY"),
        ?assertMatch(#{snapshots := 1}, snapshot_mng:status()),
        Ref = erlang:monitor(process, C2),
        exit(C2, kill),
        receive {'DOWN', Ref, _, _, _} -> ok after 1000 -> error(timeout) end,
        ok = wait_until(fun() -> maps:get(snapshots, snapshot_mng:status()) =:= 0 end),
        %% 書き手が通ること
        W = async(fun() ->
                          ok = q(C1, "BEGIN"),
                          {ok, _} = q(C1, "INSERT INTO t VALUES (7)"),
                          q(C1, "COMMIT")
                  end),
        ?assertEqual({ok, ok}, await(W, 5000))
    end.

%%%===================================================================

%% Fun を別プロセスで走らせて合図を待つ。
%% 呼び出し元を殺してもサーバ側の処理は止まらないので、
%% 「終わったか」は合図で見るしかない。
async(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    _ = spawn(fun() -> Parent ! {Ref, Fun()} end),
    Ref.

await(Ref, Ms) ->
    receive {Ref, R} -> {ok, R}
    after Ms -> timeout
    end.

wait_until(F) -> wait_until(F, 100).
wait_until(_F, 0) -> error(timeout);
wait_until(F, N) ->
    case F() of
        true  -> ok;
        false -> timer:sleep(10), wait_until(F, N - 1)
    end.

uses_index({ok, _, Lines}) ->
    lists:any(fun([L]) -> binary:match(L, <<"Index Scan">>) =/= nomatch end, Lines).

%% トランザクションの外で1文だけ実行する
one_shot(C, Sql) ->
    ok = q(C, "BEGIN"),
    R = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

count(C) ->
    ok = q(C, "BEGIN READ ONLY"),
    N = count_in(C),
    ok = q(C, "COMMIT"),
    N.

count_in(C) ->
    {ok, _, [[N]]} = q(C, "SELECT COUNT(*) FROM t"),
    N.

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE t (id INTEGER)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO t VALUES (1)"),
    {ok, _} = q(C, "INSERT INTO t VALUES (2)"),
    ok = q(C, "COMMIT"),
    C.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
