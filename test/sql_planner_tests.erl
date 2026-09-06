%%%-------------------------------------------------------------------
%%% プランナ(論理→物理)と EXPLAIN。
%%%
%%% プランナのテストは実行結果では書けない。「述語が結合の下に落ちた」も
%%% 「索引スキャンを選んだ」も結果を変えないからである。
%%% だから**構造**に対して書く。
%%%-------------------------------------------------------------------
-module(sql_planner_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/logical.hrl").
-include("../include/plan.hrl").

planner_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun scan_maps_to_seq_scan/1,
      fun filter_and_project_pass_through/1,
      fun join_maps_to_nested_loop/1,
      fun agg_sort_limit_distinct_pass_through/1,
      fun rewrite_is_identity_for_now/1,
      fun explain_renders_tree/1,
      fun explain_names_positions/1,
      fun explain_schema_of_join_is_concatenation/1,
      fun explain_via_sql/1,
      fun explain_rejects_non_select/1,
      fun explain_does_not_execute/1]}.

%%%===================================================================
%%% 論理 → 物理
%%%===================================================================

scan_maps_to_seq_scan(_) ->
    fun() ->
        seed(),
        ?assertMatch(#p_seq_scan{table = emp}, (plan("SELECT * FROM emp"))#p_project.input)
    end.

filter_and_project_pass_through(_) ->
    fun() ->
        seed(),
        ?assertMatch(#p_project{input = #p_filter{input = #p_seq_scan{table = emp}}},
                     plan("SELECT name FROM emp WHERE id = 1"))
    end.

join_maps_to_nested_loop(_) ->
    fun() ->
        seed(),
        P = plan("SELECT e.name, d.dname FROM emp e JOIN dept d ON e.dept = d.id"),
        ?assertMatch(#p_project{input = #p_nl_join{type = inner,
                                                   left = #p_seq_scan{table = emp},
                                                   right = #p_seq_scan{table = dept}}},
                     P)
    end.

agg_sort_limit_distinct_pass_through(_) ->
    fun() ->
        seed(),
        ?assertMatch(#p_limit{input = #p_distinct{input = #p_project{
                        input = #p_sort{input = #p_agg{input = #p_seq_scan{table = emp}}}}}},
                     plan("SELECT DISTINCT dept, COUNT(*) FROM emp "
                          "GROUP BY dept ORDER BY dept LIMIT 2"))
    end.

%% いまの rewrite/1 は恒等。7-2 で述語を落とすようになったら、
%% このテストが落ちて変更に気づける。
rewrite_is_identity_for_now(_) ->
    fun() ->
        seed(),
        L = logical("SELECT name FROM emp WHERE id = 1"),
        ?assertEqual(L, sql_planner:rewrite(L)),
        ?assertMatch(#lp_project{input = #lp_filter{input = #lp_scan{table = emp}}}, L)
    end.

%%%===================================================================
%%% EXPLAIN
%%%===================================================================

explain_renders_tree(_) ->
    fun() ->
        seed(),
        ?assertEqual([<<"Project (name)">>,
                      <<"  Filter (id = 1)">>,
                      <<"    Seq Scan on emp">>],
                     sql_explain:explain(plan("SELECT name FROM emp WHERE id = 1")))
    end.

%% 位置参照はカラム名に戻して出す。#2 のままでは読めない。
explain_names_positions(_) ->
    fun() ->
        seed(),
        Lines = sql_explain:explain(plan("SELECT * FROM emp WHERE dept = 10 ORDER BY name DESC")),
        ?assert(lists:any(fun(L) -> binary:match(L, <<"(dept = 10)">>) =/= nomatch end, Lines)),
        ?assert(lists:any(fun(L) -> binary:match(L, <<"Sort (name DESC">>) =/= nomatch end, Lines))
    end.

explain_schema_of_join_is_concatenation(_) ->
    fun() ->
        seed(),
        P = plan("SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id"),
        #p_project{input = Join} = P,
        ?assertEqual([id, name, dept, id, dname], sql_explain:schema(Join))
    end.

explain_via_sql(_) ->
    fun() ->
        C = seed(),
        {ok, Cols, Rows} = q(C, "EXPLAIN SELECT name FROM emp WHERE id = 1"),
        ?assertEqual(['QUERY PLAN'], Cols),
        ?assertEqual([[<<"Project (name)">>],
                      [<<"  Filter (id = 1)">>],
                      [<<"    Seq Scan on emp">>]], Rows)
    end.

explain_rejects_non_select(_) ->
    fun() ->
        C = seed(),
        ?assertEqual({error, explain_requires_select},
                     q(C, "EXPLAIN INSERT INTO emp VALUES (9, 'z', 1)"))
    end.

%% EXPLAIN は実行しない。トランザクションも要らない。
explain_does_not_execute(_) ->
    fun() ->
        C = seed(),
        {ok, _, _} = q(C, "EXPLAIN SELECT * FROM emp"),
        %% BEGIN していないのにエラーにならないこと、
        %% かつデータが増えていないこと
        ok = q(C, "BEGIN"),
        {ok, _, Rows} = q(C, "SELECT * FROM emp"),
        ok = q(C, "ROLLBACK"),
        ?assertEqual(2, length(Rows))
    end.

%%%===================================================================
%%% 土台
%%%===================================================================

seed() ->
    C = connect(),
    ok = q(C, "CREATE TABLE emp (id INTEGER, name VARCHAR, dept INTEGER)"),
    ok = q(C, "CREATE TABLE dept (id INTEGER, dname VARCHAR)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (1, 'ada', 10)"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (2, 'bob', 20)"),
    {ok, _} = q(C, "INSERT INTO dept VALUES (10, 'eng')"),
    ok = q(C, "COMMIT"),
    C.

logical(Sql) ->
    {ok, Ast} = sql:parse(Sql),
    {ok, {select, L}} = sql_analyzer:analyze(Ast),
    L.

plan(Sql) -> sql_planner:plan(logical(Sql)).

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
