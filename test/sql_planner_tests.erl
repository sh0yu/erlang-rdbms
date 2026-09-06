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
      fun cross_predicate_joins_the_on_clause/1,
      fun left_join_results_unchanged_by_pushdown/1,
      fun index_scan_when_selective/1,
      fun no_index_scan_when_not_selective/1,
      fun no_index_scan_on_unindexed_column/1,
      fun residual_conjuncts_stay_as_filter/1,
      fun most_selective_index_is_chosen/1,
      fun index_scan_returns_same_rows/1,
      fun index_scan_sees_uncommitted_changes/1,
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
%% 内部結合では ON と WHERE が等価なので、結合の ON にまとめる。
cross_predicate_joins_the_on_clause(_) ->
    fun() ->
        seed2(),
        L = lines("SELECT e.name FROM emp e JOIN dept d ON e.dept = d.id "
                  "WHERE e.sal < d.budget"),
        ?assertEqual([<<"Project (name)">>,
                      <<"  Nested Loop INNER Join on ((dept = id) AND (sal < budget))">>,
                      <<"    Seq Scan on emp">>,
                      <<"    Seq Scan on dept">>], L)
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
%%% アクセスパスの選択
%%%
%%% カタログを関数で渡せるので、索引や統計を偽って書き分けられる。
%%% 実データを作らずに「この統計ならこの計画」を直接試験できる。
%%%===================================================================

%% 異なり値が多い(選択率が良い)なら索引を使う。
index_scan_when_selective(_) ->
    fun() ->
        seed2(),
        L = logical("SELECT name FROM emp WHERE id = 2"),
        P = sql_planner:plan(L, cat(emp, [id], 1000, [{id, 1000}])),
        ?assertMatch(#p_project{input = #p_index_scan{table = emp, column = id, value = 2}}, P)
    end.

%% 異なり値が2しかないカラムでは、索引で半分の行をランダムに読むより
%% 順に舐めた方が速い。**索引があっても使わない**のが正しい。
no_index_scan_when_not_selective(_) ->
    fun() ->
        seed2(),
        L = logical("SELECT name FROM emp WHERE dept = 10"),
        P = sql_planner:plan(L, cat(emp, [dept], 1000, [{dept, 2}])),
        ?assertMatch(#p_project{input = #p_filter{input = #p_seq_scan{table = emp}}}, P)
    end.

no_index_scan_on_unindexed_column(_) ->
    fun() ->
        seed2(),
        L = logical("SELECT name FROM emp WHERE id = 2"),
        P = sql_planner:plan(L, cat(emp, [], 1000, [{id, 1000}])),
        ?assertMatch(#p_project{input = #p_filter{input = #p_seq_scan{}}}, P)
    end.

%% 索引で使わなかった条件は選択として残る。落とすと結果が変わる。
residual_conjuncts_stay_as_filter(_) ->
    fun() ->
        seed2(),
        L = logical("SELECT name FROM emp WHERE id = 2 AND sal > 100"),
        P = sql_planner:plan(L, cat(emp, [id], 1000, [{id, 1000}])),
        ?assertEqual([<<"Project (name)">>,
                      <<"  Filter (sal > 100)">>,
                      <<"    Index Scan on emp (id = 2)">>],
                     sql_explain:explain(P))
    end.

%% 索引が複数使えるときは、選択率のいちばん良いものを選ぶ。
most_selective_index_is_chosen(_) ->
    fun() ->
        seed2(),
        L = logical("SELECT name FROM emp WHERE dept = 10 AND id = 2"),
        P = sql_planner:plan(L, cat(emp, [id, dept], 1000, [{id, 1000}, {dept, 4}])),
        ?assertMatch(#p_project{input = #p_filter{input = #p_index_scan{column = id}}}, P)
    end.

%% 索引を使っても使わなくても結果は同じ。
index_scan_returns_same_rows(_) ->
    fun() ->
        C = seed2(),
        Before = rows(C, "SELECT name FROM emp WHERE dept = 10"),
        ok = q(C, "CREATE INDEX emp_dept ON emp (dept)"),
        {ok, _} = q(C, "ANALYZE emp"),
        After = rows(C, "SELECT name FROM emp WHERE dept = 10"),
        ?assertEqual([[<<"ada">>]], Before),
        ?assertEqual(Before, After)
    end.

%% **索引スキャンでも自分の未コミット変更が見えること。**
%% 実行器から索引を直接引かせると、さっき入れた行が見えない。
index_scan_sees_uncommitted_changes(_) ->
    fun() ->
        C = seed2(),
        %% 索引が選ばれるだけの行数を入れる。3行では
        %% 全表走査の方が安いので、索引は(正しく)選ばれない。
        ok = q(C, "BEGIN"),
        _ = [q(C, lists:flatten(io_lib:format(
                    "INSERT INTO emp VALUES (~p, 'x~p', 99, 1)", [I, I])))
             || I <- lists:seq(100, 160)],
        ok = q(C, "COMMIT"),
        ok = q(C, "CREATE INDEX emp_id ON emp (id)"),
        {ok, _} = q(C, "ANALYZE emp"),
        %% 索引が選ばれていることを確かめてから
        ?assertMatch([_, <<"  Index Scan on emp (id = 9)">>],
                     explain_rows(C, "SELECT name FROM emp WHERE id = 9")),
        ok = q(C, "BEGIN"),
        {ok, _} = q(C, "INSERT INTO emp VALUES (9, 'zed', 10, 1)"),
        {ok, _, R1} = q(C, "SELECT name FROM emp WHERE id = 9"),
        ?assertEqual([[<<"zed">>]], R1),
        %% 自分の削除も見えないこと
        {ok, 1} = q(C, "DELETE FROM emp WHERE id = 1"),
        {ok, _, R2} = q(C, "SELECT name FROM emp WHERE id = 1"),
        ?assertEqual([], R2),
        ok = q(C, "ROLLBACK"),
        %% 巻き戻したら元通り
        ?assertEqual([[<<"ada">>]], rows(C, "SELECT name FROM emp WHERE id = 1"))
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

%% 偽のカタログ。索引と統計を指定して、計画の選択だけを試験する。
cat(Table, Indexed, Rows, Distincts) ->
    Cols = [{N, {col_stats, D, 0, undefined, undefined}} || {N, D} <- Distincts],
    Stats = {table_stats, Table, Rows, Cols, 0},
    fun(T) when T =:= Table -> #{indexed => Indexed, stats => Stats};
       (_) -> #{indexed => [], stats => none}
    end.

explain_rows(C, Sql) ->
    {ok, _, R} = q(C, "EXPLAIN " ++ Sql),
    [L || [L] <- R].

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
