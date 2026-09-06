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
      fun single_table_filter_is_unchanged/1,
      fun pushdown_splits_across_inner_join/1,
      fun pushdown_shifts_positions_to_right_side/1,
      fun left_join_where_on_null_side_stays_above/1,
      fun left_join_on_condition_on_null_side_is_pushed/1,
      fun left_join_on_condition_on_preserved_side_stays/1,
      fun cross_predicate_stays_above/1,
      fun left_join_results_unchanged_by_pushdown/1,
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

%% 落とす先が無い(結合が無い)ときは何も変わらない。
single_table_filter_is_unchanged(_) ->
    fun() ->
        seed(),
        L = logical("SELECT name FROM emp WHERE id = 1"),
        ?assertEqual(L, sql_planner:rewrite(L)),
        ?assertMatch(#lp_project{input = #lp_filter{input = #lp_scan{table = emp}}}, L)
    end.

%%%===================================================================
%%% 述語のプッシュダウン
%%%===================================================================

%% 片側だけを見ている条件は、その側へ落ちる。
pushdown_splits_across_inner_join(_) ->
    fun() ->
        seed2(),
        ?assertEqual([<<"Project (name, dname)">>,
                      <<"  Nested Loop INNER Join on (dept = id)">>,
                      <<"    Filter (sal > 100)">>,
                      <<"      Seq Scan on emp">>,
                      <<"    Filter (budget < 900)">>,
                      <<"      Seq Scan on dept">>],
                     lines("SELECT e.name, d.dname FROM emp e JOIN dept d "
                           "ON e.dept = d.id WHERE e.sal > 100 AND d.budget < 900"))
    end.

%% 右側へ落とすときは位置を左の幅だけ戻す。
%% 戻し忘れると別のカラムを見ることになる。
pushdown_shifts_positions_to_right_side(_) ->
    fun() ->
        seed2(),
        P = plan("SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id "
                 "WHERE d.budget < 900"),
        #p_project{input = #p_nl_join{right = Right}} = P,
        %% dept の budget は dept 単体では3番目。結合後は 4+3=7番目。
        ?assertMatch(#p_filter{pred = {comp, '<', {ref, 3}, {const, 900}}}, Right)
    end.

%% LEFT JOIN の NULL を供給する側に対する WHERE は落とせない。
%% 落とすと、一致しない左の行がNULL埋めで残ってしまい結果が変わる。
left_join_where_on_null_side_stays_above(_) ->
    fun() ->
        seed2(),
        ?assertEqual([<<"Project (name, dname)">>,
                      <<"  Filter (budget < 900)">>,
                      <<"    Nested Loop LEFT Join on (dept = id)">>,
                      <<"      Seq Scan on emp">>,
                      <<"      Seq Scan on dept">>],
                     lines("SELECT e.name, d.dname FROM emp e LEFT JOIN dept d "
                           "ON e.dept = d.id WHERE d.budget < 900"))
    end.

%% 同じ条件でも ON に書いてあれば落とせる。
%% ON は「何を一致とみなすか」なので、先に絞っても一致集合は変わらない。
left_join_on_condition_on_null_side_is_pushed(_) ->
    fun() ->
        seed2(),
        ?assertEqual([<<"Project (name)">>,
                      <<"  Nested Loop LEFT Join on (dept = id)">>,
                      <<"    Seq Scan on emp">>,
                      <<"    Filter (budget < 900)">>,
                      <<"      Seq Scan on dept">>],
                     lines("SELECT e.name FROM emp e LEFT JOIN dept d "
                           "ON e.dept = d.id AND d.budget < 900"))
    end.

%% 保存される側(左)の ON 条件は落とせない。
%% 落とすと、条件を満たさない左の行が消える。本来はNULL埋めで残る。
left_join_on_condition_on_preserved_side_stays(_) ->
    fun() ->
        seed2(),
        ?assertEqual([<<"Project (name)">>,
                      <<"  Nested Loop LEFT Join on ((dept = id) AND (sal > 100))">>,
                      <<"    Seq Scan on emp">>,
                      <<"    Seq Scan on dept">>],
                     lines("SELECT e.name FROM emp e LEFT JOIN dept d "
                           "ON e.dept = d.id AND e.sal > 100"))
    end.

%% 両側を見ている条件はどちらにも落とせない。
cross_predicate_stays_above(_) ->
    fun() ->
        seed2(),
        L = lines("SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id "
                  "WHERE e.sal < d.budget"),
        ?assertEqual([<<"Project (name)">>,
                      <<"  Filter (sal < budget)">>,
                      <<"    Nested Loop INNER Join on (dept = id)">>,
                      <<"      Seq Scan on emp">>,
                      <<"      Seq Scan on dept">>], L)
    end.

%% 書き換えは結果を変えてはならない。危ないのは外部結合なので、
%% 実際に走らせて確かめる。
left_join_results_unchanged_by_pushdown(_) ->
    fun() ->
        C = seed2(),
        %% dan(dept未設定) は一致しないのでNULL埋め。
        %% budget < 900 は NULL に対して unknown なので落ちる。
        ?assertEqual([[<<"ada">>, <<"eng">>]],
                     rows(C, "SELECT e.name, d.dname FROM emp e LEFT JOIN dept d "
                             "ON e.dept = d.id WHERE d.budget < 900")),
        %% 同じ条件を ON に書くと、一致しない行がNULL埋めで残る
        ?assertEqual([[<<"ada">>, <<"eng">>],
                      [<<"bob">>, null],
                      [<<"dan">>, null]],
                     rows(C, "SELECT e.name, d.dname FROM emp e LEFT JOIN dept d "
                             "ON e.dept = d.id AND d.budget < 900 ORDER BY e.name"))
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

%% budget を持つ dept と、一致しない行を含む emp。
seed2() ->
    C = connect(),
    ok = q(C, "CREATE TABLE emp (id INTEGER, name VARCHAR, dept INTEGER, sal INTEGER)"),
    ok = q(C, "CREATE TABLE dept (id INTEGER, dname VARCHAR, budget INTEGER)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (1, 'ada', 10, 500)"),
    {ok, _} = q(C, "INSERT INTO emp VALUES (2, 'bob', 20, 300)"),
    {ok, _} = q(C, "INSERT INTO emp (id, name, sal) VALUES (3, 'dan', 50)"),
    {ok, _} = q(C, "INSERT INTO dept VALUES (10, 'eng', 800)"),
    {ok, _} = q(C, "INSERT INTO dept VALUES (20, 'sales', 1200)"),
    ok = q(C, "COMMIT"),
    C.

lines(Sql) -> sql_explain:explain(plan(Sql)).

%% SELECT はトランザクションを要求する(暗黙には開かない)。
rows(C, Sql) ->
    ok = q(C, "BEGIN"),
    {ok, _, R} = q(C, Sql),
    ok = q(C, "ROLLBACK"),
    R.

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
