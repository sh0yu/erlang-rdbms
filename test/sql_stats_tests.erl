%%%-------------------------------------------------------------------
%%% 統計の採取(ANALYZE)。
%%%
%%% 統計は古くてよい。見積もりが外れても結果は変わらず、遅くなるだけ。
%%% だから挿入・削除では更新せず、ANALYZE で採り直す。
%%%-------------------------------------------------------------------
-module(sql_stats_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/catalog.hrl").

%%%===================================================================
%%% 畳み込み(純粋)
%%%===================================================================

empty_has_one_slot_per_column_test() ->
    ?assertEqual(3, length(sql_stats:empty([a, b, c]))).

accumulate_counts_distinct_and_nulls_test() ->
    Acc0 = sql_stats:empty([a, b]),
    Acc = lists:foldl(fun sql_stats:accumulate/2, Acc0,
                      [[1, <<"x">>], [1, <<"y">>], [2, null], [3, <<"x">>]]),
    S = sql_stats:finish([a, b], 4, Acc),
    ?assertEqual(4, sql_stats:rows(S)),
    ?assertEqual(3, sql_stats:distinct(S, a)),      % 1,2,3
    ?assertEqual(0, sql_stats:nulls(S, a)),
    ?assertEqual(2, sql_stats:distinct(S, b)),      % x,y (NULLは数えない)
    ?assertEqual(1, sql_stats:nulls(S, b)).

min_max_use_sql_ordering_test() ->
    %% Erlangの項順序だと 100 < null が真になる。
    %% NULLが最小・最大に紛れ込んではいけない。
    Acc = lists:foldl(fun sql_stats:accumulate/2, sql_stats:empty([a]),
                      [[100], [null], [5], [50]]),
    S = sql_stats:finish([a], 4, Acc),
    ?assertMatch(#col_stats{min = 5, max = 100}, sql_stats:column(S, a)).

rows_mismatch_is_ignored_test() ->
    %% カラム数が合わない行はカタログと整合しないので数えない
    Acc = sql_stats:accumulate([1, 2, 3], sql_stats:empty([a, b])),
    ?assertEqual(sql_stats:empty([a, b]), Acc).

%% 統計を採っていないテーブルは既定値で見積もる。
%% 「統計が無いから最適化しない」ではなく「分からないなりに見積もる」。
missing_stats_fall_back_to_defaults_test() ->
    ?assertEqual(sql_stats:default_rows(), sql_stats:rows(none)),
    ?assertEqual(undefined, sql_stats:column(none, a)),
    ?assertEqual(0, sql_stats:distinct(none, a)).

unknown_column_is_undefined_test() ->
    S = sql_stats:finish([a], 1, sql_stats:accumulate([1], sql_stats:empty([a]))),
    ?assertEqual(undefined, sql_stats:column(S, nosuch)).

%%%===================================================================
%%% ANALYZE(実際に走らせる)
%%%===================================================================

analyze_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun analyze_collects_stats/1,
      fun analyze_all_tables/1,
      fun stats_are_absent_before_analyze/1,
      fun analyze_rejects_unknown_table/1,
      fun stats_survive_and_are_replaced/1,
      fun dropping_table_drops_stats/1]}.

stats_are_absent_before_analyze(_) ->
    fun() ->
        _ = seeded(),
        ?assertEqual(none, sys_tbl_mng:get_stats(sys_tbl_mng, t))
    end.

analyze_collects_stats(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({ok, 4}, q(C, "ANALYZE t")),
        {ok, S} = sys_tbl_mng:get_stats(sys_tbl_mng, t),
        ?assertEqual(t, S#table_stats.table),
        ?assertEqual(4, sql_stats:rows(S)),
        ?assertEqual(4, sql_stats:distinct(S, a)),        % 1,2,3,4
        ?assertEqual(3, sql_stats:distinct(S, b)),        % x,y,z
        ?assertEqual(2, sql_stats:distinct(S, c)),        % 10,20
        ?assertEqual(1, sql_stats:nulls(S, c)),           % 4行目のc
        ?assertMatch(#col_stats{min = 1, max = 4}, sql_stats:column(S, a)),
        ?assert(is_integer(S#table_stats.analyzed))
    end.

analyze_all_tables(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "CREATE TABLE u (x INTEGER)"),
        ?assertEqual({ok, 2}, q(C, "ANALYZE")),
        ?assertMatch({ok, #table_stats{rows = 4}}, sys_tbl_mng:get_stats(sys_tbl_mng, t)),
        ?assertMatch({ok, #table_stats{rows = 0}}, sys_tbl_mng:get_stats(sys_tbl_mng, u))
    end.

analyze_rejects_unknown_table(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {table_not_found, "nosuch"}}, q(C, "ANALYZE nosuch"))
    end.

%% 統計は行を足しても自動では動かない。採り直して初めて反映される。
stats_survive_and_are_replaced(_) ->
    fun() ->
        C = seeded(),
        {ok, 4} = q(C, "ANALYZE t"),
        ok = q(C, "BEGIN"),
        {ok, _} = q(C, "INSERT INTO t VALUES (9, 'w', 90)"),
        ok = q(C, "COMMIT"),
        %% まだ古い
        ?assertMatch({ok, #table_stats{rows = 4}}, sys_tbl_mng:get_stats(sys_tbl_mng, t)),
        {ok, 5} = q(C, "ANALYZE t"),
        ?assertMatch({ok, #table_stats{rows = 5}}, sys_tbl_mng:get_stats(sys_tbl_mng, t))
    end.

dropping_table_drops_stats(_) ->
    fun() ->
        C = seeded(),
        {ok, 4} = q(C, "ANALYZE t"),
        ok = q(C, "DROP TABLE t"),
        ?assertEqual(none, sys_tbl_mng:get_stats(sys_tbl_mng, t))
    end.

%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE t (a INTEGER, b VARCHAR, c INTEGER)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO t VALUES (1, 'x', 10)"),
    {ok, _} = q(C, "INSERT INTO t VALUES (2, 'y', 10)"),
    {ok, _} = q(C, "INSERT INTO t VALUES (3, 'x', 20)"),
    {ok, _} = q(C, "INSERT INTO t (a, b) VALUES (4, 'z')"),
    ok = q(C, "COMMIT"),
    C.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).

%%%===================================================================
%%% 見積もり
%%%===================================================================

selectivity_of_equality_test() ->
    S = stats(1000, [{a, 100}, {b, 2}]),
    %% 異なり値が多いほど選択率は良い(小さい)
    ?assertEqual(0.01, sql_stats:selectivity({comp, '=', {ref, 1}, {const, 5}}, [a, b], S)),
    ?assertEqual(0.5,  sql_stats:selectivity({comp, '=', {ref, 2}, {const, 5}}, [a, b], S)),
    %% 左右どちらに定数があっても同じ
    ?assertEqual(0.01, sql_stats:selectivity({comp, '=', {const, 5}, {ref, 1}}, [a, b], S)).

selectivity_of_and_or_not_test() ->
    S = stats(1000, [{a, 100}, {b, 100}]),
    E1 = {comp, '=', {ref, 1}, {const, 1}},
    E2 = {comp, '=', {ref, 2}, {const, 2}},
    ?assertEqual(0.0001, sql_stats:selectivity({'and', [E1, E2]}, [a, b], S)),
    ?assert(abs(sql_stats:selectivity({'or', [E1, E2]}, [a, b], S) - 0.0199) < 1.0e-9),
    ?assertEqual(0.99, sql_stats:selectivity({'not', E1}, [a, b], S)).

selectivity_without_stats_falls_back_test() ->
    %% 統計が無くても見積もる。「分からないから最適化しない」ではない
    ?assertEqual(0.1, sql_stats:selectivity({comp, '=', {ref, 1}, {const, 1}}, [a], none)),
    ?assertEqual(0.5, sql_stats:selectivity({some, unknown, form}, [a], none)).

selectivity_of_null_tests_test() ->
    S = {table_stats, t, 100, [{a, {col_stats, 10, 25, undefined, undefined}}], 0},
    ?assertEqual(0.25, sql_stats:selectivity({is_null, {ref, 1}}, [a], S)),
    ?assertEqual(0.75, sql_stats:selectivity({is_not_null, {ref, 1}}, [a], S)).

%% 索引を使うかどうかは、この2つの比較だけで決まる。
cost_decides_index_vs_seq_test() ->
    S = stats(1000, [{a, 1000}, {b, 2}]),
    Good = sql_stats:selectivity({comp, '=', {ref, 1}, {const, 1}}, [a, b], S),
    Bad  = sql_stats:selectivity({comp, '=', {ref, 2}, {const, 1}}, [a, b], S),
    ?assert(sql_stats:cost_index_scan(S, Good) < sql_stats:cost_seq_scan(S)),
    ?assert(sql_stats:cost_index_scan(S, Bad)  > sql_stats:cost_seq_scan(S)).

stats(Rows, Distincts) ->
    {table_stats, t, Rows,
     [{N, {col_stats, D, 0, undefined, undefined}} || {N, D} <- Distincts], 0}.
