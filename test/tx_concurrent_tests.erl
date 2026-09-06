%%%-------------------------------------------------------------------
%%% 読み書きトランザクションの並行実行(並行制御 段階3)。
%%%
%%% これまで tx_mng が同時に1本しか active にせず、全トランザクションが
%%% 直列に走っていた。走査が読みロックを取らないので、直列でなければ
%%% 安全でなかったため。
%%%
%%% スナップショットが入ったので、その前提が要らなくなった。
%%%   読み: 自分のスナップショットと undo で決まる。ロックは取らない
%%%   書き: 行の書き込みロック + コミット直前の衝突検査
%%%
%%% 得られる分離水準はスナップショット分離で、直列化可能ではない。
%%% ライトスキューは防げない(下の test がそれを示す)。
%%%-------------------------------------------------------------------
-module(tx_concurrent_tests).

-include_lib("eunit/include/eunit.hrl").

concurrent_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun writers_on_different_rows_do_not_wait/1,
      fun writer_waits_for_the_row_lock/1,
      fun lost_update_is_refused/1,
      fun deadlock_is_refused_and_rolled_back/1,
      fun write_skew_is_not_prevented/1,
      fun read_committed_sees_other_commits/1,
      fun repeatable_read_does_not/1,
      fun read_committed_retries_instead_of_refusing/1,
      fun concurrent_increments_all_land/1,
      fun ddl_waits_for_a_writer_on_that_table/1,
      fun ddl_does_not_wait_for_another_table/1]}.

%% 別々の行なら互いを待たない。
writers_on_different_rows_do_not_wait(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        {ok, 1} = q(C1, "UPDATE t SET v = 10 WHERE id = 1"),
        Done = async(fun() ->
                             ok = q(C2, "BEGIN"),
                             {ok, 1} = q(C2, "UPDATE t SET v = 20 WHERE id = 2"),
                             q(C2, "COMMIT")
                     end),
        ?assertEqual({ok, ok}, await(Done, 5000)),
        ok = q(C1, "COMMIT"),
        ?assertEqual([[1, 10], [2, 20]], rows(C1))
    end.

%% 同じ行なら待つ。待たせないと後の書き手が前の書き手を踏み潰す。
writer_waits_for_the_row_lock(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        {ok, 1} = q(C1, "UPDATE t SET v = 10 WHERE id = 1"),
        Done = async(fun() ->
                             ok = q(C2, "BEGIN"),
                             q(C2, "UPDATE t SET v = 20 WHERE id = 1")
                     end),
        ?assertEqual(timeout, await(Done, 300)),
        ok = q(C1, "ROLLBACK"),
        ?assertEqual({ok, {ok, 1}}, await(Done, 5000))
    end.

%% 自分が読んだ後に他人が同じ行を変えていたら、自分を捨てる。
%% 黙って書くと先にコミットした方の更新が消える。
%%
%% 断るのは **UPDATE の時点**。コミットまで待つと、残りの文を全部
%% やってから捨てることになる。PostgreSQL も同じ位置で返す。
lost_update_is_refused(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        ok = q(C2, "BEGIN"),
        %% 両方とも v = 1 を見ている
        ?assertMatch({ok, _, [[1]]}, q(C1, "SELECT v FROM t WHERE id = 1")),
        ?assertMatch({ok, _, [[1]]}, q(C2, "SELECT v FROM t WHERE id = 1")),

        {ok, 1} = q(C1, "UPDATE t SET v = 10 WHERE id = 1"),
        ok = q(C1, "COMMIT"),

        ?assertEqual({error, serialization_failure},
                     q(C2, "UPDATE t SET v = 20 WHERE id = 1")),
        %% 断られた側はロールバック済み
        ?assertEqual(transaction_not_found, q(C2, "COMMIT")),
        %% 先にコミットした方が残る
        ?assertEqual([[1, 10], [2, 2]], rows(C1))
    end.

%% 互いに相手の持つ行を取りに行くと、片方を断る。
%% 断らないと双方が永久に止まる。
deadlock_is_refused_and_rolled_back(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        ok = q(C2, "BEGIN"),
        {ok, 1} = q(C1, "UPDATE t SET v = 10 WHERE id = 1"),
        {ok, 1} = q(C2, "UPDATE t SET v = 20 WHERE id = 2"),

        %% C1 が 2 を待つ(まだ閉路ではない)
        W = async(fun() -> q(C1, "UPDATE t SET v = 11 WHERE id = 2") end),
        ?assertEqual(timeout, await(W, 300)),
        %% C2 が 1 を取りに行くと閉路。C2 が断られる
        ?assertEqual({error, deadlock}, q(C2, "UPDATE t SET v = 21 WHERE id = 1")),

        %% 断られた側はロールバック済み。ロックも返っているので C1 は進む
        ?assertEqual({ok, {ok, 1}}, await(W, 5000)),
        ok = q(C1, "COMMIT"),
        ?assertEqual([[1, 10], [2, 11]], rows(C1))
    end.

%% **これは防げない。** スナップショット分離の限界。
%%
%% 互いに「相手が読んだ行」を書くので、書き込みは重ならず、
%% 行ロックにも衝突検査にも引っかからない。それぞれ単独なら正しいのに、
%% 両方通ると「vの合計は2以上」という不変条件が壊れる。
%%
%% 直すには SSI か述語ロックが要る。ここでは防げないことを記録しておく。
write_skew_is_not_prevented(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        ok = q(C2, "BEGIN"),
        %% 双方が「合計は3。片方を0にしても2残る」と判断する
        ?assertMatch({ok, _, [[3]]}, q(C1, "SELECT SUM(v) FROM t")),
        ?assertMatch({ok, _, [[3]]}, q(C2, "SELECT SUM(v) FROM t")),
        {ok, 1} = q(C1, "UPDATE t SET v = 0 WHERE id = 1"),
        {ok, 1} = q(C2, "UPDATE t SET v = 0 WHERE id = 2"),
        ok = q(C1, "COMMIT"),
        ok = q(C2, "COMMIT"),
        %% 直列化可能なら 0 にはならないが、なる
        ?assertEqual([[1, 0], [2, 0]], rows(C1))
    end.

%%%===================================================================
%%% READ COMMITTED
%%%===================================================================

%% 文ごとにスナップショットを取り直すので、途中で入ったコミットが見える。
%% これが READ COMMITTED の定義そのもの(反復可能読み取りを失う)。
read_committed_sees_other_commits(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN READ COMMITTED"),
        ?assertMatch({ok, _, [[1]]}, q(C1, "SELECT v FROM t WHERE id = 1")),

        ok = q(C2, "BEGIN"),
        {ok, 1} = q(C2, "UPDATE t SET v = 99 WHERE id = 1"),
        ok = q(C2, "COMMIT"),

        %% 同じトランザクションの中なのに、値が変わって見える
        ?assertMatch({ok, _, [[99]]}, q(C1, "SELECT v FROM t WHERE id = 1")),
        ok = q(C1, "COMMIT")
    end.

%% 既定(REPEATABLE READ)は変わらない。
repeatable_read_does_not(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        ?assertMatch({ok, _, [[1]]}, q(C1, "SELECT v FROM t WHERE id = 1")),

        ok = q(C2, "BEGIN"),
        {ok, 1} = q(C2, "UPDATE t SET v = 99 WHERE id = 1"),
        ok = q(C2, "COMMIT"),

        ?assertMatch({ok, _, [[1]]}, q(C1, "SELECT v FROM t WHERE id = 1")),
        ok = q(C1, "COMMIT")
    end.

%% 同じ行を待たされても断られない。読み直して文をやり直す。
read_committed_retries_instead_of_refusing(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        ok = q(C2, "BEGIN READ COMMITTED"),
        %% 双方が v = 1 を見ている
        ?assertMatch({ok, _, [[1]]}, q(C2, "SELECT v FROM t WHERE id = 1")),

        {ok, 1} = q(C1, "UPDATE t SET v = 10 WHERE id = 1"),
        ok = q(C1, "COMMIT"),

        %% REPEATABLE READ ならここで serialization_failure。
        %% READ COMMITTED は通る
        ?assertEqual({ok, 1}, q(C2, "UPDATE t SET v = v + 1 WHERE id = 1")),
        ok = q(C2, "COMMIT"),
        %% 読み直した後の値(10)に +1 されている。10 を踏み潰していない
        ?assertEqual([[1, 11], [2, 2]], rows(C1))
    end.

%% 多数の接続が同じ行を +1 する。1つも取りこぼさないこと。
concurrent_increments_all_land(_) ->
    fun() ->
        C1 = seeded(),
        N = 20,
        Parent = self(),
        [spawn(fun() ->
                       C = connect(),
                       ok = q(C, "BEGIN READ COMMITTED"),
                       {ok, 1} = q(C, "UPDATE t SET v = v + 1 WHERE id = 1"),
                       ok = q(C, "COMMIT"),
                       Parent ! done
               end) || _ <- lists:seq(1, N)],
        [receive done -> ok after 10000 -> error(timeout) end || _ <- lists:seq(1, N)],
        ?assertEqual([[1, 1 + N], [2, 2]], rows(C1))
    end.

%% DDL はその表を書きかけのトランザクションを待つ。
%% 待たないと、コミットしようとした先の表が消えている。
ddl_waits_for_a_writer_on_that_table(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        {ok, 1} = q(C1, "UPDATE t SET v = 10 WHERE id = 1"),
        Done = async(fun() -> q(C2, "DROP TABLE t") end),
        ?assertEqual(timeout, await(Done, 300)),
        ok = q(C1, "COMMIT"),
        ?assertEqual({ok, ok}, await(Done, 5000))
    end.

%% 関係のない表の DDL は待たない。
ddl_does_not_wait_for_another_table(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "CREATE TABLE u (a INTEGER)"),
        ok = q(C1, "BEGIN"),
        {ok, 1} = q(C1, "UPDATE t SET v = 10 WHERE id = 1"),
        Done = async(fun() -> q(C2, "DROP TABLE u") end),
        ?assertEqual({ok, ok}, await(Done, 5000)),
        ok = q(C1, "COMMIT")
    end.

%%%===================================================================

async(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    _ = spawn(fun() -> Parent ! {Ref, Fun()} end),
    Ref.

await(Ref, Ms) ->
    receive {Ref, R} -> {ok, R}
    after Ms -> timeout
    end.

%% トランザクションの外から現在の中身を読む
rows(C) ->
    ok = q(C, "BEGIN READ ONLY"),
    {ok, _, Rows} = q(C, "SELECT id, v FROM t ORDER BY id"),
    ok = q(C, "COMMIT"),
    Rows.

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE t (id INTEGER, v INTEGER)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO t VALUES (1, 1)"),
    {ok, _} = q(C, "INSERT INTO t VALUES (2, 2)"),
    ok = q(C, "COMMIT"),
    C.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
