%%%-------------------------------------------------------------------
%%% JOIN。
%%%
%%% 結合すると複数テーブルのカラムが1つの行に並ぶので、名前解決が
%%% スコープ全体に広がる。修飾なしの名前が一意でなければ弾く。
%%%-------------------------------------------------------------------
-module(sql_join_tests).

-include_lib("eunit/include/eunit.hrl").

join_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun inner_join/1,
      fun inner_join_with_aliases/1,
      fun unqualified_names_resolve/1,
      fun left_join_pads_with_nulls/1,
      fun cross_join_via_comma/1,
      fun explicit_cross_join/1,
      fun join_with_where/1,
      fun join_with_group_by/1,
      fun join_with_order_and_limit/1,
      fun self_join/1,
      fun three_way_join/1,
      fun ambiguous_column_is_rejected/1,
      fun unknown_qualifier_is_rejected/1,
      fun join_on_non_equality/1]}.

inner_join(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>, <<"eng">>], [<<"bob">>, <<"sales">>], [<<"cy">>, <<"eng">>]],
                     rows(C, "SELECT emp.name, dept.dname FROM emp "
                             "JOIN dept ON emp.dept = dept.id"))
    end.

inner_join_with_aliases(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual(3, length(rows(C, "SELECT e.name, d.dname FROM emp e "
                                       "JOIN dept d ON e.dept = d.id")))
    end.

%% 一意に定まる名前は修飾しなくてよい
unqualified_names_resolve(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual(3, length(rows(C, "SELECT name, dname FROM emp "
                                       "JOIN dept ON emp.dept = dept.id")))
    end.

%% 一致しない左の行は、右側をNULLで埋めて残る
left_join_pads_with_nulls(_) ->
    fun() ->
        C = seeded(),
        Rows = rows(C, "SELECT e.name, d.dname FROM emp e "
                       "LEFT JOIN dept d ON e.dept = d.id"),
        ?assertEqual(4, length(Rows)),
        ?assert(lists:member([<<"dan">>, null], Rows))
    end.

cross_join_via_comma(_) ->
    fun() ->
        C = seeded(),
        %% 4人 x 3部署
        ?assertEqual(12, length(rows(C, "SELECT e.name, d.dname FROM emp e, dept d")))
    end.

explicit_cross_join(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual(12, length(rows(C, "SELECT e.name, d.dname FROM emp e CROSS JOIN dept d")))
    end.

join_with_where(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>], [<<"cy">>]],
                     rows(C, "SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id "
                             "WHERE d.dname = 'eng' ORDER BY e.name"))
    end.

join_with_group_by(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"eng">>, 2], [<<"sales">>, 1]],
                     lists:sort(rows(C, "SELECT d.dname, COUNT(*) FROM emp e "
                                        "JOIN dept d ON e.dept = d.id GROUP BY d.dname")))
    end.

join_with_order_and_limit(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"cy">>], [<<"bob">>]],
                     rows(C, "SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id "
                             "ORDER BY e.name DESC LIMIT 2"))
    end.

%% 同じテーブルを別名で2回使う
self_join(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"ada">>, <<"cy">>]],
                     rows(C, "SELECT a.name, b.name FROM emp a JOIN emp b "
                             "ON a.dept = b.dept WHERE a.id < b.id"))
    end.

three_way_join(_) ->
    fun() ->
        %% DDLはトランザクションの中では実行できないので、
        %% BEGIN する前にテーブルを作っておく
        C = seeded(fun(Conn) ->
                           ok = q(Conn, "CREATE TABLE loc (dept INTEGER, city VARCHAR)")
                   end),
        {ok, _} = q(C, "INSERT INTO loc VALUES (10, 'tokyo')"),
        ?assertEqual([[<<"ada">>, <<"eng">>, <<"tokyo">>], [<<"cy">>, <<"eng">>, <<"tokyo">>]],
                     rows(C, "SELECT e.name, d.dname, l.city FROM emp e "
                             "JOIN dept d ON e.dept = d.id "
                             "JOIN loc l ON l.dept = d.id ORDER BY e.name"))
    end.

%% 両方のテーブルに id があるので、修飾なしでは決まらない
ambiguous_column_is_rejected(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {ambiguous_column, "id"}},
                     q(C, "SELECT id FROM emp JOIN dept ON emp.dept = dept.id"))
    end.

unknown_qualifier_is_rejected(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {column_not_found, "x.name"}},
                     q(C, "SELECT x.name FROM emp JOIN dept ON emp.dept = dept.id"))
    end.

%% ON は等値でなくてよい(入れ子ループなので任意の述語が書ける)
join_on_non_equality(_) ->
    fun() ->
        C = seeded(),
        Rows = rows(C, "SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept < d.id"),
        ?assert(length(Rows) > 0)
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

seeded() ->
    seeded(fun(_) -> ok end).

seeded(ExtraDdl) ->
    C = connect(),
    ok = q(C, "CREATE TABLE emp (id INTEGER, name VARCHAR, dept INTEGER)"),
    ok = q(C, "CREATE TABLE dept (id INTEGER, dname VARCHAR)"),
    ok = ExtraDdl(C),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (1, 'ada', 10)"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (2, 'bob', 20)"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (3, 'cy', 10)"),
    {ok, _} = q(C, "INSERT INTO emp (id, name) VALUES (4, 'dan')"),
    {ok, _} = q(C, "INSERT INTO dept VALUES (10, 'eng')"),
    {ok, _} = q(C, "INSERT INTO dept VALUES (20, 'sales')"),
    {ok, _} = q(C, "INSERT INTO dept VALUES (30, 'hr')"),
    C.

rows(C, Sql) ->
    {ok, _Cols, Rows} = q(C, Sql),
    Rows.

connect() ->
    {ok, Pid} = gen_connection:connect(),
    Pid.

q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
