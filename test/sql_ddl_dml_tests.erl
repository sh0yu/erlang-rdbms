%%%-------------------------------------------------------------------
%%% SQLのDDL / DML / トランザクション制御。
%%%-------------------------------------------------------------------
-module(sql_ddl_dml_tests).

-include_lib("eunit/include/eunit.hrl").

ddl_dml_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun create_and_drop/1,
      fun create_rejects_bad_definitions/1,
      fun insert_and_select/1,
      fun insert_with_column_list/1,
      fun omitted_columns_become_null/1,
      fun insert_checks_types/1,
      fun insert_rejects_wrong_arity/1,
      fun integer_is_promoted_to_float/1,
      fun negative_literals/1,
      fun update_all_and_filtered/1,
      fun update_checks_types/1,
      fun delete_all_and_filtered/1,
      fun transaction_control_via_sql/1,
      fun rollback_via_sql/1,
      fun varchar_column_matches_string_literal/1,
      fun untyped_table_still_works/1,
      fun update_does_not_loop_on_its_own_writes/1]}.

%%%===================================================================
%%% DDL
%%%===================================================================

create_and_drop(_) ->
    fun() ->
        C = connect(),
        ?assertEqual(ok, q(C, "CREATE TABLE t (a INTEGER, b VARCHAR)")),
        ?assertEqual({error, table_already_exists}, q(C, "CREATE TABLE t (a INTEGER)")),
        ?assertEqual(ok, q(C, "DROP TABLE t")),
        ?assertMatch({error, {table_not_found, _}}, q(C, "DROP TABLE t"))
    end.

create_rejects_bad_definitions(_) ->
    fun() ->
        C = connect(),
        %% カラム名はまだ解決前なので文字列で返る
        %% ({table_not_found, "nosuch"} などと同じ形)
        ?assertEqual({error, {duplicate_columns, ["a"]}},
                     q(C, "CREATE TABLE t (a INTEGER, a VARCHAR)")),
        %% 型名は必須
        ?assertMatch({error, {syntax_error, _, _}}, q(C, "CREATE TABLE t (a)"))
    end.

%%%===================================================================
%%% INSERT / SELECT
%%%===================================================================

insert_and_select(_) ->
    fun() ->
        C = fixture(),
        ?assertMatch({ok, _}, q(C, "INSERT INTO t VALUES ('x', 1, true)")),
        ?assertEqual({ok, [[<<"x">>, 1, true]]}, q(C, "SELECT * FROM t"))
    end.

insert_with_column_list(_) ->
    fun() ->
        C = fixture(),
        %% 宣言順と違う順序でも、カラム名どおりに入ること
        ?assertMatch({ok, _}, q(C, "INSERT INTO t (b, a) VALUES (7, 'y')")),
        ?assertEqual({ok, [[<<"y">>, 7, null]]}, q(C, "SELECT * FROM t"))
    end.

omitted_columns_become_null(_) ->
    fun() ->
        C = fixture(),
        ?assertMatch({ok, _}, q(C, "INSERT INTO t (a) VALUES ('z')")),
        ?assertEqual({ok, [[<<"z">>, null, null]]}, q(C, "SELECT * FROM t"))
    end.

insert_checks_types(_) ->
    fun() ->
        C = fixture(),
        ?assertEqual({error, {type_mismatch, b, integer, <<"nope">>}},
                     q(C, "INSERT INTO t VALUES ('x', 'nope', true)")),
        ?assertEqual({error, {type_mismatch, a, varchar, 1}},
                     q(C, "INSERT INTO t VALUES (1, 1, true)")),
        %% 何も入っていないこと
        ?assertEqual({ok, []}, q(C, "SELECT * FROM t"))
    end.

insert_rejects_wrong_arity(_) ->
    fun() ->
        C = fixture(),
        ?assertEqual({error, column_count_mismatch}, q(C, "INSERT INTO t VALUES ('x', 1)")),
        ?assertEqual({error, column_count_mismatch},
                     q(C, "INSERT INTO t (a, b) VALUES ('x')"))
    end.

%% 整数はFLOATのカラムに入れてよい(格上げ)。逆は許さない。
integer_is_promoted_to_float(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE f (x FLOAT, y INTEGER)"),
        ok = q(C, "BEGIN"),
        ?assertMatch({ok, _}, q(C, "INSERT INTO f VALUES (1, 2)")),
        ?assertEqual({error, {type_mismatch, y, integer, 1.5}},
                     q(C, "INSERT INTO f VALUES (1.5, 1.5)"))
    end.

negative_literals(_) ->
    fun() ->
        C = fixture(),
        ?assertMatch({ok, _}, q(C, "INSERT INTO t VALUES ('n', -42, false)")),
        ?assertEqual({ok, [[<<"n">>, -42, false]]}, q(C, "SELECT * FROM t WHERE b = -42"))
    end.

%%%===================================================================
%%% UPDATE / DELETE
%%%===================================================================

update_all_and_filtered(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({ok, 1}, q(C, "UPDATE t SET b = 99 WHERE a = 'x'")),
        ?assertEqual({ok, [[<<"x">>, 99, true]]}, q(C, "SELECT * FROM t WHERE a = 'x'")),
        %% WHERE なしは全件
        ?assertEqual({ok, 3}, q(C, "UPDATE t SET b = 0")),
        {ok, Rows} = q(C, "SELECT * FROM t"),
        ?assertEqual([0, 0, 0], [B || [_, B, _] <- Rows]),
        %% 複数カラムの代入
        ?assertEqual({ok, 3}, q(C, "UPDATE t SET b = 1, c = false")),
        {ok, Rows2} = q(C, "SELECT * FROM t"),
        ?assertEqual([{1, false}, {1, false}, {1, false}],
                     [{B, Cc} || [_, B, Cc] <- Rows2])
    end.

update_checks_types(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {type_mismatch, b, integer, <<"no">>}},
                     q(C, "UPDATE t SET b = 'no'")),
        %% 何も変わっていないこと
        {ok, Rows} = q(C, "SELECT * FROM t"),
        ?assertEqual(3, length(Rows))
    end.

delete_all_and_filtered(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({ok, 1}, q(C, "DELETE FROM t WHERE a = 'x'")),
        ?assertEqual({ok, 2}, count(C)),
        ?assertEqual({ok, 2}, q(C, "DELETE FROM t")),
        ?assertEqual({ok, 0}, count(C))
    end.

%%%===================================================================
%%% トランザクション制御
%%%===================================================================

transaction_control_via_sql(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE t (a INTEGER)"),
        ok = q(C, "BEGIN"),
        {ok, _} = q(C, "INSERT INTO t VALUES (1)"),
        ok = q(C, "COMMIT"),
        ok = q(C, "BEGIN"),
        ?assertEqual({ok, [[1]]}, q(C, "SELECT * FROM t")),
        ok = q(C, "COMMIT")
    end.

rollback_via_sql(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE t (a INTEGER)"),
        ok = q(C, "BEGIN"),
        {ok, _} = q(C, "INSERT INTO t VALUES (1)"),
        ok = q(C, "ROLLBACK"),
        ok = q(C, "BEGIN"),
        ?assertEqual({ok, []}, q(C, "SELECT * FROM t")),
        ok = q(C, "COMMIT")
    end.

%%%===================================================================
%%% 型がもたらす違い
%%%===================================================================

%% VARCHARと宣言したカラムなら、文字列リテラルで引ける。
%% 型宣言が無かった頃はここが一致しなかった。
varchar_column_matches_string_literal(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({ok, [[<<"x">>, 1, true]]}, q(C, "SELECT * FROM t WHERE a = 'x'"))
    end.

%% 型宣言のない古いタプルAPIのテーブルは any 型になり、
%% アトムがそのまま入る。文字列リテラルとは一致しない(暗黙変換しない)。
untyped_table_still_works(_) ->
    fun() ->
        C = connect(),
        ok = query_exec:exec_query(C, {create_table, old, [name, price]}),
        ok = q(C, "BEGIN"),
        {ok, _} = query_exec:exec_query(C, {insert, old, [apple, 100]}),
        ?assertEqual({ok, [[apple, 100]]}, q(C, "SELECT * FROM old WHERE price = 100")),
        ?assertEqual({ok, []}, q(C, "SELECT * FROM old WHERE name = 'apple'"))
    end.

%% Halloween problem: 更新した行を走査が拾い直して延々と更新し続けないこと。
%% 対象のOidを先に確定させてから適用することで防いでいる。
update_does_not_loop_on_its_own_writes(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE n (v INTEGER)"),
        ok = q(C, "BEGIN"),
        [{ok, _} = q(C, "INSERT INTO n VALUES (1)") || _ <- lists:seq(1, 3)],
        %% 1 -> 2 に更新する。パイプライン実行だと 2 を拾い直して
        %% 条件を外れるまで加算し続ける形の典型
        ?assertEqual({ok, 3}, q(C, "UPDATE n SET v = 2 WHERE v = 1")),
        {ok, Rows} = q(C, "SELECT * FROM n"),
        ?assertEqual([[2], [2], [2]], Rows)
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

fixture() ->
    C = connect(),
    ok = q(C, "CREATE TABLE t (a VARCHAR, b INTEGER, c BOOLEAN)"),
    ok = q(C, "BEGIN"),
    C.

seeded() ->
    C = fixture(),
    {ok, _} = q(C, "INSERT INTO t VALUES ('x', 1, true)"),
    {ok, _} = q(C, "INSERT INTO t VALUES ('y', 2, false)"),
    {ok, _} = q(C, "INSERT INTO t VALUES ('z', 3, true)"),
    C.

count(C) ->
    {ok, Rows} = q(C, "SELECT * FROM t"),
    {ok, length(Rows)}.

connect() ->
    {ok, Pid} = gen_connection:connect(),
    Pid.

q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
