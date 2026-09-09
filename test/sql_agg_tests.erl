%%%-------------------------------------------------------------------
%%% 集約 / GROUP BY / HAVING。
%%%
%%% 空集合の扱いに非対称がある(COUNT は 0、それ以外は NULL)。
%%% NULL の扱いも COUNT(*) だけが別。ここを網羅する。
%%%-------------------------------------------------------------------
-module(sql_agg_tests).

-include_lib("eunit/include/eunit.hrl").

agg_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun count_star_counts_every_row/1,
      fun count_column_skips_nulls/1,
      fun sum_avg_min_max/1,
      fun aggregates_skip_nulls/1,
      fun all_null_group_gives_null/1,
      fun empty_table_count_is_zero_others_null/1,
      fun group_by_single_key/1,
      fun group_by_multiple_keys/1,
      fun group_by_puts_nulls_in_one_group/1,
      fun having/1,
      fun having_can_use_an_aggregate_not_selected/1,
      fun count_distinct/1,
      fun expression_over_aggregate/1,
      fun aggregate_with_where/1,
      fun order_by_aggregate/1,
      fun group_by_with_limit/1,
      fun ungrouped_column_is_rejected/1,
      fun unknown_function_is_rejected/1,
      fun star_with_group_by_is_rejected/1]}.

%%%===================================================================
%%% 集約関数
%%%===================================================================

%% COUNT(*) は NULL の行も数える
count_star_counts_every_row(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[5]], rows(C, "SELECT COUNT(*) FROM s"))
    end.

%% COUNT(x) は x が NULL の行を数えない
count_column_skips_nulls(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[4]], rows(C, "SELECT COUNT(a) FROM s"))
    end.

sum_avg_min_max(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1000, 250, 100, 400]],
                     rows(C, "SELECT SUM(a), AVG(a), MIN(a), MAX(a) FROM s"))
    end.

%% NULL は集約の入力から外れる。AVG の分母にも入らない。
aggregates_skip_nulls(_) ->
    fun() ->
        C = seeded(),
        %% 4件(100,200,300,400)の平均は250。NULLを0とみなすなら200になる
        ?assertEqual([[250]], rows(C, "SELECT AVG(a) FROM s"))
    end.

%% グループの全行が NULL なら、COUNT 以外は NULL
all_null_group_gives_null(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"north">>, null, 0, 1]],
                     rows(C, "SELECT r, SUM(a), COUNT(a), COUNT(*) FROM s "
                             "GROUP BY r HAVING r = 'north'"))
    end.

%% 空集合でも COUNT は 0 を返し、それ以外は NULL を返す
empty_table_count_is_zero_others_null(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE e (a INTEGER)"),
        ok = q(C, "BEGIN"),
        ?assertEqual([[0, 0, null, null, null, null]],
                     rows(C, "SELECT COUNT(*), COUNT(a), SUM(a), AVG(a), MIN(a), MAX(a) FROM e"))
    end.

%%%===================================================================
%%% GROUP BY / HAVING
%%%===================================================================

group_by_single_key(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"east">>, 2], [<<"north">>, 1], [<<"west">>, 2]],
                     lists:sort(rows(C, "SELECT r, COUNT(*) FROM s GROUP BY r")))
    end.

group_by_multiple_keys(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual(4, length(rows(C, "SELECT r, p, COUNT(*) FROM s GROUP BY r, p")))
    end.

%% GROUP BY では NULL 同士が同じ組にまとまる(`=` の意味論とは逆)
group_by_puts_nulls_in_one_group(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE g (k INTEGER)"),
        ok = q(C, "BEGIN"),
        [{ok, _} = q(C, "INSERT INTO g (k) VALUES (NULL)") || _ <- lists:seq(1, 3)],
        {ok, _} = q(C, "INSERT INTO g VALUES (1)"),
        ?assertEqual([[null, 3], [1, 1]],
                     rows(C, "SELECT k, COUNT(*) FROM g GROUP BY k ORDER BY k DESC"))
    end.

having(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"east">>, 2], [<<"west">>, 2]],
                     lists:sort(rows(C, "SELECT r, COUNT(*) FROM s GROUP BY r "
                                        "HAVING COUNT(*) > 1")))
    end.

%% HAVING には SELECT に出していない集約も書ける
having_can_use_an_aggregate_not_selected(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"west">>]],
                     rows(C, "SELECT r FROM s GROUP BY r HAVING SUM(a) > 500"))
    end.

count_distinct(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[3]], rows(C, "SELECT COUNT(DISTINCT p) FROM s"))
    end.

expression_over_aggregate(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[6]], rows(C, "SELECT COUNT(*) + 1 FROM s"))
    end.

aggregate_with_where(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[3]], rows(C, "SELECT COUNT(*) FROM s WHERE a > 150"))
    end.

order_by_aggregate(_) ->
    fun() ->
        C = seeded(),
        %% DESC の既定は NULLS FIRST なので north(SUM=NULL)が先頭
        ?assertEqual([[<<"north">>, null], [<<"west">>, 700], [<<"east">>, 300]],
                     rows(C, "SELECT r, SUM(a) FROM s GROUP BY r ORDER BY SUM(a) DESC"))
    end.

group_by_with_limit(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"east">>], [<<"north">>]],
                     rows(C, "SELECT r FROM s GROUP BY r ORDER BY r LIMIT 2"))
    end.

%%%===================================================================
%%% 誤りの検出
%%%===================================================================

%% GROUP BY に無く集約でもないカラムは、どの行の値か決まらない
ungrouped_column_is_rejected(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {not_grouped, "p"}},
                     q(C, "SELECT p FROM s GROUP BY r"))
    end.

unknown_function_is_rejected(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {unknown_function, "nosuchfn"}},
                     q(C, "SELECT nosuchfn(a) FROM s")),
        %% COUNT 以外の集約に * は使えない
        ?assertEqual({error, {star_not_allowed, "sum"}},
                     q(C, "SELECT SUM(*) FROM s"))
    end.

star_with_group_by_is_rejected(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, star_with_group_by}, q(C, "SELECT * FROM s GROUP BY r"))
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE s (r VARCHAR, p VARCHAR, a INTEGER)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO s VALUES ('east', 'apple', 100)"),
    {ok, _} = q(C, "INSERT INTO s VALUES ('east', 'banana', 200)"),
    {ok, _} = q(C, "INSERT INTO s VALUES ('west', 'apple', 300)"),
    {ok, _} = q(C, "INSERT INTO s VALUES ('west', 'apple', 400)"),
    {ok, _} = q(C, "INSERT INTO s (r, p) VALUES ('north', 'grape')"),
    C.

rows(C, Sql) ->
    {ok, _Cols, Rows} = q(C, Sql),
    Rows.

connect() ->
    {ok, Pid} = gen_connection:connect(),
    Pid.

q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
