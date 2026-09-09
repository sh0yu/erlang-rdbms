%%%-------------------------------------------------------------------
%%% 式の中の副問い合わせ。IN / EXISTS / スカラー。
%%%
%%% **相関しないものだけ**を扱う。外側の行に依存しないので、本体を
%%% 動かす前に1回実行して定数へ畳める。外側を参照する副問い合わせは
%%% 1行ごとに実行し直す必要があり、別の仕組みになる。
%%%
%%% IN の3値論理が要点。
%%%   x が NULL          → unknown
%%%   一致がある         → true
%%%   一致が無くNULL有り → unknown(そのNULLが x かもしれない)
%%%   一致が無くNULL無し → false
%%%-------------------------------------------------------------------
-module(sql_expr_subquery_tests).

-include_lib("eunit/include/eunit.hrl").

subquery_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun scalar_subquery_in_where/1,
      fun scalar_subquery_in_select_list/1,
      fun scalar_subquery_with_no_rows_is_null/1,
      fun scalar_subquery_with_many_rows_is_an_error/1,
      fun in_subquery/1,
      fun not_in_subquery/1,
      fun in_value_list/1,
      fun in_is_three_valued/1,
      fun not_in_with_null_is_never_true/1,
      fun exists_and_not_exists/1,
      fun subquery_must_return_one_column/1,
      fun correlated_exists/1,
      fun correlated_scalar/1,
      fun correlated_in/1,
      fun correlated_aggregate/1,
      fun nested_correlation/1,
      fun uncorrelated_is_folded_once/1,
      fun explain_shows_the_subplan/1]}.

scalar_subquery_in_where(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"cy">>]],
                     rows(C, "SELECT name FROM emp WHERE sal = (SELECT MAX(sal) FROM emp)"))
    end.

scalar_subquery_in_select_list(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>, 1]],
                     rows(C, "SELECT name, (SELECT COUNT(*) FROM dept) AS n "
                             "FROM emp WHERE id = 1"))
    end.

%% 0行なら NULL(標準SQL)。
scalar_subquery_with_no_rows_is_null(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[null]],
                     rows(C, "SELECT (SELECT dname FROM dept WHERE id = 99) AS d "
                             "FROM emp WHERE id = 1"))
    end.

scalar_subquery_with_many_rows_is_an_error(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        ?assertEqual({error, {scalar_subquery_returned_rows, 4}},
                     q(C, "SELECT name FROM emp WHERE sal = (SELECT sal FROM emp)")),
        ok = q(C, "COMMIT")
    end.

in_subquery(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>], [<<"cy">>]],
                     rows(C, "SELECT name FROM emp WHERE dept IN (SELECT id FROM dept) "
                             "ORDER BY name"))
    end.

not_in_subquery(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"bob">>]],
                     rows(C, "SELECT name FROM emp WHERE dept NOT IN (SELECT id FROM dept)"))
    end.

in_value_list(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>], [<<"cy">>]],
                     rows(C, "SELECT name FROM emp WHERE dept IN (10, 30) ORDER BY name")),
        ?assertEqual([[<<"bob">>]],
                     rows(C, "SELECT name FROM emp WHERE dept NOT IN (10, 30)"))
    end.

%% 左辺が NULL なら結果は unknown。WHERE は通さない。
in_is_three_valued(_) ->
    fun() ->
        C = seeded(),
        %% dan は dept が NULL
        ?assertEqual([], rows(C, "SELECT name FROM emp WHERE dept IN (10, 20) "
                                 "AND name = 'dan'")),
        ?assertEqual([], rows(C, "SELECT name FROM emp WHERE dept NOT IN (10, 20) "
                                 "AND name = 'dan'"))
    end.

%% 一致が無くても、候補に NULL があれば unknown。決して true にならない。
not_in_with_null_is_never_true(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([], rows(C, "SELECT name FROM emp WHERE dept NOT IN (10, null)")),
        %% NULL が無ければ普通に効く
        ?assertEqual([[<<"bob">>]],
                     rows(C, "SELECT name FROM emp WHERE dept NOT IN (10)"))
    end.

exists_and_not_exists(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual(4, length(rows(C, "SELECT name FROM emp "
                                       "WHERE EXISTS (SELECT 1 FROM dept)"))),
        ?assertEqual([], rows(C, "SELECT name FROM emp "
                                 "WHERE EXISTS (SELECT id FROM dept WHERE id = 99)")),
        ?assertEqual(4, length(rows(C, "SELECT name FROM emp "
                                       "WHERE NOT EXISTS (SELECT id FROM dept "
                                       "WHERE id = 99)")))
    end.

subquery_must_return_one_column(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {subquery_must_return_one_column, 2}},
                     q(C, "SELECT name FROM emp WHERE dept IN (SELECT id, dname FROM dept)"))
    end.

%%%===================================================================
%%% 相関副問い合わせ
%%%
%%% 外側の列を参照するので、**外側の1行ごとに実行し直す**。
%%% 相関しないものは実行前に1回だけ評価して定数へ畳む。
%%%===================================================================

correlated_exists(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>], [<<"cy">>]],
                     rows(C, "SELECT e.name FROM emp e WHERE EXISTS "
                             "(SELECT 1 FROM dept d WHERE d.id = e.dept) "
                             "ORDER BY e.name")),
        %% dan は dept が NULL。d.id = NULL は unknown なので一致しない
        ?assertEqual([[<<"bob">>], [<<"dan">>]],
                     rows(C, "SELECT e.name FROM emp e WHERE NOT EXISTS "
                             "(SELECT 1 FROM dept d WHERE d.id = e.dept) "
                             "ORDER BY e.name"))
    end.

correlated_scalar(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>, <<"eng">>],
                      [<<"bob">>, null],
                      [<<"cy">>, <<"eng">>],
                      [<<"dan">>, null]],
                     rows(C, "SELECT e.name, (SELECT d.dname FROM dept d "
                             "WHERE d.id = e.dept) AS dn FROM emp e ORDER BY e.name"))
    end.

correlated_in(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>], [<<"cy">>]],
                     rows(C, "SELECT e.name FROM emp e WHERE e.dept IN "
                             "(SELECT d.id FROM dept d WHERE d.id = e.dept) "
                             "ORDER BY e.name"))
    end.

%% 外側の行ごとに集約をやり直す。
correlated_aggregate(_) ->
    fun() ->
        C = seeded(),
        %% dept 10 の平均は (500+700)/2 = 600。cy(700) だけが超える
        ?assertEqual([[<<"cy">>]],
                     rows(C, "SELECT e.name FROM emp e WHERE e.sal > "
                             "(SELECT AVG(x.sal) FROM emp x WHERE x.dept = e.dept) "
                             "ORDER BY e.name"))
    end.

%% 2段の入れ子。内側は1段外(中間)を見る。
nested_correlation(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>], [<<"cy">>]],
                     rows(C, "SELECT e.name FROM emp e WHERE EXISTS "
                             "(SELECT 1 FROM dept d WHERE d.id = e.dept "
                             " AND EXISTS (SELECT 1 FROM emp x WHERE x.dept = d.id)) "
                             "ORDER BY e.name"))
    end.

%% 相関しないものは1回だけ実行される。EXPLAIN では定数に畳まれた形は
%% 見えない(EXPLAINは畳む前の計画を出す)ので、結果で確かめる。
uncorrelated_is_folded_once(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual(4, length(rows(C, "SELECT name FROM emp "
                                       "WHERE EXISTS (SELECT 1 FROM dept)")))
    end.

%% 副問い合わせの中身も木として出す。出さないと何をしているか分からない。
explain_shows_the_subplan(_) ->
    fun() ->
        C = seeded(),
        {ok, _, R} = q(C, "EXPLAIN SELECT name FROM emp "
                          "WHERE dept IN (SELECT id FROM dept)"),
        ?assertEqual([<<"Project (name)">>,
                      <<"  Filter (dept IN (subquery))">>,
                      <<"    Seq Scan on emp">>,
                      <<"    Project (id)">>,
                      <<"      Seq Scan on dept">>],
                     [L || [L] <- R])
    end.

%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE emp (id INTEGER, name VARCHAR, dept INTEGER, sal INTEGER)"),
    ok = q(C, "CREATE TABLE dept (id INTEGER, dname VARCHAR)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (1, 'ada', 10, 500)"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (2, 'bob', 20, 300)"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (3, 'cy', 10, 700)"),
    {ok, _} = q(C, "INSERT INTO emp (id, name, sal) VALUES (4, 'dan', 100)"),
    {ok, _} = q(C, "INSERT INTO dept VALUES (10, 'eng')"),
    ok = q(C, "COMMIT"),
    C.

rows(C, Sql) ->
    ok = q(C, "BEGIN READ ONLY"),
    {ok, _, R} = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
