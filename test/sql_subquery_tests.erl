%%%-------------------------------------------------------------------
%%% 導出表(FROM句の副問い合わせ)と列別名。
%%%
%%% 導出表があると問い合わせが入れ子にできるようになる。集約の結果を
%%% さらに絞る、集合演算の結果を並べ替える、といった形が書ける。
%%%
%%% 中の射影はリストを出すが、外側の演算子は位置参照(element/2)で引く。
%%% 導出表がタプルへ戻す。
%%%-------------------------------------------------------------------
-module(sql_subquery_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/plan.hrl").

subquery_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun derived_table_basic/1,
      fun derived_table_over_aggregate/1,
      fun derived_table_joined_with_table/1,
      fun derived_table_over_set_op/1,
      fun derived_table_requires_alias/1,
      fun nested_derived_tables/1,
      fun column_alias_names_the_output/1,
      fun column_alias_on_aggregate/1,
      fun column_alias_is_usable_from_outside/1,
      fun explain_shows_subquery/1]}.

derived_table_basic(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>], [<<"cy">>]],
                     rows(C, "SELECT d.name FROM (SELECT name, sal FROM emp "
                             "WHERE sal > 400) AS d ORDER BY d.name"))
    end.

%% 集約の結果をさらに絞る。HAVING でも書けるが、
%% 導出表なら結果に対する任意の条件が書ける。
derived_table_over_aggregate(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[10, 1200]],
                     rows(C, "SELECT * FROM (SELECT dept, SUM(sal) AS total "
                             "FROM emp GROUP BY dept) AS d WHERE d.total > 600"))
    end.

derived_table_joined_with_table(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>], [<<"bob">>], [<<"cy">>]],
                     rows(C, "SELECT x.name FROM (SELECT name FROM emp WHERE sal > 250) AS x "
                             "JOIN emp e ON x.name = e.name ORDER BY x.name"))
    end.

derived_table_over_set_op(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [2], [3], [4], [10], [20]],
                     rows(C, "SELECT * FROM (SELECT id FROM emp "
                             "UNION SELECT dept FROM emp) AS u ORDER BY id"))
    end.

%% 別名が無いと列を修飾できない。
derived_table_requires_alias(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, derived_table_requires_alias},
                     q(C, "SELECT * FROM (SELECT id FROM emp)"))
    end.

nested_derived_tables(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[10, 1200]],
                     rows(C, "SELECT * FROM (SELECT * FROM "
                             "(SELECT dept, SUM(sal) AS total FROM emp GROUP BY dept) AS a "
                             "WHERE a.total > 600) AS b"))
    end.

%%%===================================================================
%%% 列別名
%%%===================================================================

column_alias_names_the_output(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        {ok, Cols, _} = q(C, "SELECT name AS who, sal AS amount FROM emp"),
        ok = q(C, "COMMIT"),
        ?assertEqual([who, amount], Cols)
    end.

column_alias_on_aggregate(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        {ok, Cols, Rows} = q(C, "SELECT dept, COUNT(*) AS n FROM emp GROUP BY dept "
                                "ORDER BY dept"),
        ok = q(C, "COMMIT"),
        ?assertEqual([dept, n], Cols),
        ?assertEqual([[10, 2], [20, 2]], Rows)
    end.

%% 別名を付けた列は、導出表の外から引ける。
column_alias_is_usable_from_outside(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1200]],
                     rows(C, "SELECT d.total FROM (SELECT dept, SUM(sal) AS total "
                             "FROM emp GROUP BY dept) AS d WHERE d.dept = 10"))
    end.

explain_shows_subquery(_) ->
    fun() ->
        C = seeded(),
        {ok, _, R} = q(C, "EXPLAIN SELECT d.dept FROM "
                          "(SELECT dept, SUM(sal) AS total FROM emp GROUP BY dept) AS d "
                          "WHERE d.dept = 10"),
        ?assertEqual([<<"Project (dept)">>,
                      <<"  Filter (dept = 10)">>,
                      <<"    Subquery (dept, total)">>,
                      <<"      Project (dept, total)">>,
                      <<"        HashAggregate group by (dept) -> (sum(sal))">>,
                      <<"          Seq Scan on emp">>],
                     [L || [L] <- R])
    end.

%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE emp (id INTEGER, name VARCHAR, dept INTEGER, sal INTEGER)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (1, 'ada', 10, 500)"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (2, 'bob', 20, 300)"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (3, 'cy', 10, 700)"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (4, 'dan', 20, 200)"),
    ok = q(C, "COMMIT"),
    C.

rows(C, Sql) ->
    ok = q(C, "BEGIN READ ONLY"),
    {ok, _, R} = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
