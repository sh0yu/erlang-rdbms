%%%-------------------------------------------------------------------
%%% 列制約(PRIMARY KEY / UNIQUE / NOT NULL)。
%%%
%%% PRIMARY KEY は UNIQUE かつ NOT NULL。規格どおり意味解析で開く。
%%%
%%% 一意性の検査だけは**分離水準の外**にある。自分のスナップショットで
%%% 探すと、自分が始めた後に入った行を見落として重複を通す。よって
%%% ここだけ共有データを直に引く。PostgreSQL も同じ。
%%%-------------------------------------------------------------------
-module(sql_constraint_tests).

-include_lib("eunit/include/eunit.hrl").

constraint_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun duplicate_is_refused/1,
      fun null_is_refused_on_primary_key/1,
      fun unique_allows_many_nulls/1,
      fun update_into_a_taken_value_is_refused/1,
      fun update_to_its_own_value_is_allowed/1,
      fun deleting_frees_the_value/1,
      fun rolled_back_insert_frees_the_value/1,
      fun concurrent_inserts_of_the_same_key/1,
      fun concurrent_inserts_of_different_keys_do_not_wait/1]}.

duplicate_is_refused(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN"),
        ?assertMatch({error, {unique_violation, id, 1}},
                     q(C, "INSERT INTO t VALUES (1, 'dup')")),
        ok = q(C, "ROLLBACK"),
        ?assertEqual(2, count())
    end.

null_is_refused_on_primary_key(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN"),
        ?assertMatch({error, {not_null_violation, id}},
                     q(C, "INSERT INTO t VALUES (NULL, 'x')")),
        ok = q(C, "ROLLBACK")
    end.

%% UNIQUE は NULL を対象にしない。NULL 同士は等しくないので重複ではない。
unique_allows_many_nulls(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE u (a INTEGER UNIQUE, b INTEGER)"),
        ok = q(C, "BEGIN"),
        {ok, _} = q(C, "INSERT INTO u VALUES (NULL, 1)"),
        {ok, _} = q(C, "INSERT INTO u VALUES (NULL, 2)"),
        ok = q(C, "COMMIT"),
        ?assertMatch({ok, _, [[2]]}, one_shot(C, "SELECT COUNT(*) FROM u"))
    end.

update_into_a_taken_value_is_refused(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN"),
        ?assertMatch({error, {unique_violation, id, 2}},
                     q(C, "UPDATE t SET id = 2 WHERE id = 1")),
        ok = q(C, "ROLLBACK")
    end.

%% 自分自身は衝突相手にしない。
update_to_its_own_value_is_allowed(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN"),
        ?assertEqual({ok, 1}, q(C, "UPDATE t SET id = 1, name = 'again' WHERE id = 1")),
        ok = q(C, "COMMIT"),
        ?assertMatch({ok, _, [[<<"again">>]]},
                     one_shot(C, "SELECT name FROM t WHERE id = 1"))
    end.

deleting_frees_the_value(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN"),
        {ok, 1} = q(C, "DELETE FROM t WHERE id = 1"),
        %% 同じトランザクションの中で入れ直せる(ローカルの削除も見る)
        ?assertMatch({ok, _}, q(C, "INSERT INTO t VALUES (1, 'reused')")),
        ok = q(C, "COMMIT"),
        ?assertEqual(2, count())
    end.

%% 捨てられた挿入は値を押さえたままにしない。
rolled_back_insert_frees_the_value(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        {ok, _} = q(C1, "INSERT INTO t VALUES (9, 'a')"),
        ok = q(C1, "ROLLBACK"),
        ok = q(C2, "BEGIN"),
        ?assertMatch({ok, _}, q(C2, "INSERT INTO t VALUES (9, 'b')")),
        ok = q(C2, "COMMIT")
    end.

%%%===================================================================
%%% 並行して同じ鍵を入れる
%%%
%%% 双方とも自分のスナップショットでは「まだ無い」と見える。相手の行は
%%% 未コミット領域にあって共有データに現れていないため。鍵そのものに
%%% ロックを取って直列にする。
%%%===================================================================
concurrent_inserts_of_the_same_key(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        ok = q(C2, "BEGIN"),
        {ok, _} = q(C1, "INSERT INTO t VALUES (9, 'first')"),
        %% C2 は鍵のロックで止まる
        B = async(fun() -> q(C2, "INSERT INTO t VALUES (9, 'second')") end),
        ?assertEqual(timeout, await(B, 300)),
        ok = q(C1, "COMMIT"),
        %% C1 の行が見えるようになってから判定するので、重複と分かる
        ?assertEqual({ok, {error, {unique_violation, id, 9}}}, await(B, 5000)),
        ?assertMatch({ok, _, [[<<"first">>]]},
                     one_shot(C1, "SELECT name FROM t WHERE id = 9"))
    end.

%% 違う鍵なら待たない。ロックは鍵ごと。
concurrent_inserts_of_different_keys_do_not_wait(_) ->
    fun() ->
        C1 = seeded(),
        C2 = connect(),
        ok = q(C1, "BEGIN"),
        {ok, _} = q(C1, "INSERT INTO t VALUES (9, 'a')"),
        B = async(fun() ->
                          ok = q(C2, "BEGIN"),
                          {ok, _} = q(C2, "INSERT INTO t VALUES (10, 'b')"),
                          q(C2, "COMMIT")
                  end),
        ?assertEqual({ok, ok}, await(B, 5000)),
        ok = q(C1, "COMMIT"),
        ?assertEqual(4, count())
    end.

%%%===================================================================

count() ->
    C = connect(),
    {ok, _, [[N]]} = one_shot(C, "SELECT COUNT(*) FROM t"),
    N.

one_shot(C, Sql) ->
    ok = q(C, "BEGIN READ ONLY"),
    R = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(20))"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO t VALUES (1, 'one')"),
    {ok, _} = q(C, "INSERT INTO t VALUES (2, 'two')"),
    ok = q(C, "COMMIT"),
    C.

async(Fun) ->
    Parent = self(),
    Ref = make_ref(),
    _ = spawn(fun() -> Parent ! {Ref, Fun()} end),
    Ref.

await(Ref, Ms) ->
    receive {Ref, R} -> {ok, R}
    after Ms -> timeout
    end.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
