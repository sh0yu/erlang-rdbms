%%%-------------------------------------------------------------------
%%% 読み取り専用トランザクションの並行実行。
%%%
%%% これまで tx_mng は同時に1本しかトランザクションを動かさなかった。
%%% 読み取り専用の照会同士すら互いを待っていた。
%%%
%%% 緩めるための前提は「コミットの適用を読み手から見て原子的にすること」。
%%% apply_changes/1 は1行ずつ書き、走査は data_buffer への呼び出しを
%%% 繰り返すので、その隙間に適用が挟まると読み手がコミットの途中を見る。
%%% commit_latch がその区間を囲う。
%%%-------------------------------------------------------------------
-module(tx_readonly_tests).

-include_lib("eunit/include/eunit.hrl").

readonly_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun select_works_in_read_only/1,
      fun writes_are_rejected/1,
      fun ddl_is_rejected/1,
      fun readers_do_not_block_each_other/1,
      fun writer_blocks_readers_out/1,
      fun commit_waits_for_readers/1,
      fun reader_sees_all_or_nothing_of_a_commit/1,
      fun latch_is_released_on_commit_and_rollback/1,
      fun latch_is_released_when_connection_dies/1,
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
        ?assertMatch(#{readers := 1}, commit_latch:status()),
        ok = q(C1, "COMMIT"),
        ?assertMatch(#{readers := 0}, commit_latch:status())
    end.

%% 読み書きトランザクションはこれまでどおり直列。
%% 開いたままにすると、別の読み書きトランザクションは待つ。
writer_blocks_readers_out(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        Done = async(fun() ->
                             ok = q(C2, "BEGIN"),
                             q(C2, "SELECT id FROM t")
                     end),
        ?assertEqual(timeout, await(Done, 300)),
        ok = q(C1, "ROLLBACK"),
        ?assertMatch({ok, {ok, _, _}}, await(Done, 5000))
    end.

%% 読み手が動いている間、コミットの適用は待つ。
%% 待たないと、読み手がコミットの途中を見る。
commit_waits_for_readers(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN READ ONLY"),
        {ok, _, _} = q(C1, "SELECT id FROM t"),

        ok = q(C2, "BEGIN"),
        {ok, _} = q(C2, "INSERT INTO t VALUES (3)"),

        %% COMMIT はラッチ待ちで止まる。
        %% **呼び出し元を殺しても止まらない**(サーバ側の handle_call は
        %% 進み続ける)ので、終わったかどうかは合図で見る
        Done = async(fun() -> q(C2, "COMMIT") end),
        ?assertEqual(timeout, await(Done, 300)),

        %% 読み手が終われば通る
        ok = q(C1, "COMMIT"),
        ?assertEqual({ok, ok}, await(Done, 5000)),
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

        %% 別の接続が3行入れてコミットしようとする(ラッチ待ちで止まる)
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
        ?assertEqual(5, count(C1))
    end.

latch_is_released_on_commit_and_rollback(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        ?assertMatch(#{readers := 1}, commit_latch:status()),
        ok = q(C, "COMMIT"),
        ?assertMatch(#{readers := 0}, commit_latch:status()),
        ok = q(C, "BEGIN READ ONLY"),
        ?assertMatch(#{readers := 1}, commit_latch:status()),
        ok = q(C, "ROLLBACK"),
        ?assertMatch(#{readers := 0}, commit_latch:status())
    end.

%% ラッチを持ったまま接続が死んでも、書き手が永久に待たされない。
latch_is_released_when_connection_dies(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C2, "BEGIN READ ONLY"),
        ?assertMatch(#{readers := 1}, commit_latch:status()),
        Ref = erlang:monitor(process, C2),
        exit(C2, kill),
        receive {'DOWN', Ref, _, _, _} -> ok after 1000 -> error(timeout) end,
        ok = wait_until(fun() -> maps:get(readers, commit_latch:status()) =:= 0 end),
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
