%%%-------------------------------------------------------------------
%%% 結合順序。
%%%
%%% **アナライザで決める必要がある。** カラム参照は build_from/1 の中で
%%% 行タプル内の位置に束縛されるので、順序を後から入れ替えると位置が
%%% ずれる。sql_planner ではもう動かせない。
%%%-------------------------------------------------------------------
-module(sql_join_order_tests).

-include_lib("eunit/include/eunit.hrl").

order_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun smallest_table_goes_first/1,
      fun written_order_is_kept_without_stats/1,
      fun left_join_is_not_reordered/1,
      fun two_tables_are_not_reordered/1,
      fun reordering_does_not_change_results/1,
      fun cartesian_product_is_deferred/1]}.

%% 書いた順は big → mid → small。統計を採ると small から始まる。
smallest_table_goes_first(_) ->
    fun() ->
        C = seeded(),
        {ok, _} = q(C, "ANALYZE"),
        %% 鍵の左右も入れ替わる(左が small/mid 側になるため)
        ?assertEqual([<<"Project (id)">>,
                      <<"  Hash INNER Join on id = m">>,
                      <<"    Hash INNER Join on id = s">>,
                      <<"      Seq Scan on small">>,
                      <<"      Seq Scan on mid">>,
                      <<"    Seq Scan on big">>],
                     plan(C, "SELECT big.id FROM big "
                             "JOIN mid ON big.m = mid.id "
                             "JOIN small ON mid.s = small.id"))
    end.

%% 統計が無ければどの表も同じ大きさに見えるので、書いた順のまま。
written_order_is_kept_without_stats(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([<<"Project (id)">>,
                      <<"  Hash INNER Join on s = id">>,
                      <<"    Hash INNER Join on m = id">>,
                      <<"      Seq Scan on big">>,
                      <<"      Seq Scan on mid">>,
                      <<"    Seq Scan on small">>],
                     plan(C, "SELECT big.id FROM big "
                             "JOIN mid ON big.m = mid.id "
                             "JOIN small ON mid.s = small.id"))
    end.

%% LEFT JOIN は可換でも結合的でもない。1つでも混ざったら動かさない。
left_join_is_not_reordered(_) ->
    fun() ->
        C = seeded(),
        {ok, _} = q(C, "ANALYZE"),
        L = plan(C, "SELECT big.id FROM big "
                    "LEFT JOIN mid ON big.m = mid.id "
                    "JOIN small ON mid.s = small.id"),
        %% いちばん下は書いた順どおり big
        ?assert(lists:any(fun(X) -> binary:match(X, <<"Seq Scan on big">>) =/= nomatch end, L)),
        ?assertEqual(<<"      Seq Scan on big">>, lists:nth(4, L))
    end.

two_tables_are_not_reordered(_) ->
    fun() ->
        C = seeded(),
        {ok, _} = q(C, "ANALYZE"),
        ?assertEqual([<<"Project (id)">>,
                      <<"  Hash INNER Join on m = id">>,
                      <<"    Seq Scan on big">>,
                      <<"    Seq Scan on mid">>],
                     plan(C, "SELECT big.id FROM big JOIN mid ON big.m = mid.id"))
    end.

%% 並べ替えは結果を変えてはならない。
reordering_does_not_change_results(_) ->
    fun() ->
        C = seeded(),
        Sql = "SELECT big.id FROM big JOIN mid ON big.m = mid.id "
              "JOIN small ON mid.s = small.id ORDER BY big.id",
        Before = rows(C, Sql),
        {ok, _} = q(C, "ANALYZE"),
        After = rows(C, Sql),
        ?assert(length(Before) > 0),
        ?assertEqual(Before, After)
    end.

%% 条件でつながっていない表は後回しにする。先に直積を作ると
%% 中間結果が掛け算で膨らむ。
cartesian_product_is_deferred(_) ->
    fun() ->
        C = seeded(),
        {ok, _} = q(C, "ANALYZE"),
        L = plan(C, "SELECT big.id FROM big, mid, small "
                    "WHERE big.m = mid.id"),
        %% つながっている big と mid が先に結合され、small が最後
        ?assertEqual(<<"    Seq Scan on small">>, lists:last(L))
    end.

%%%===================================================================
%%% 修飾なしの列名
%%%===================================================================

%% `FROM a, b WHERE x = y` のように表名を書かない書き方でも、
%% 結合条件として見えていること。
%%
%% 見えないと「つながりが無い」と判断して**直積**を選ぶ。エラーには
%% ならず、答えも合うので、表が増えるまで気づけない。
%% sqllogictest の select5(最大62表)がこれで終わらなくなっていた。
unqualified_predicates_are_join_conditions_test_() ->
    {setup, fun start/0, fun stop/1,
     fun({_Ctx, C}) ->
         [%% 修飾ありと修飾なしで同じ計画になること
          ?_assertEqual(
             plan(C, "SELECT v FROM small, mid, big "
                     "WHERE big.m = mid.id2 AND mid.s = small.id3"),
             plan(C, "SELECT v FROM small, mid, big WHERE m = id2 AND s = id3")),
          %% 直積(条件のない入れ子ループ)が出ていないこと
          ?_assertEqual([], [L || L <- plan(C, "SELECT v FROM small, mid, big "
                                               "WHERE m = id2 AND s = id3"),
                                  binary:match(L, <<"Nested Loop">>) =/= nomatch])]
     end}.

%%%===================================================================

start() ->
    Ctx = db_test_helper:start_db(),
    C = connect(),
    ok = q(C, "CREATE TABLE big (id1 INTEGER, m INTEGER)"),
    ok = q(C, "CREATE TABLE mid (id2 INTEGER, s INTEGER)"),
    ok = q(C, "CREATE TABLE small (id3 INTEGER, v VARCHAR)"),
    ok = q(C, "BEGIN"),
    _ = [{ok, _} = q(C, ins("big", I, I rem 10 + 1)) || I <- lists:seq(1, 40)],
    _ = [{ok, _} = q(C, ins("mid", I, I rem 3 + 1))  || I <- lists:seq(1, 10)],
    _ = [{ok, _} = q(C, lists:flatten(io_lib:format(
                          "INSERT INTO small VALUES (~p, 'v~p')", [I, I])))
         || I <- lists:seq(1, 3)],
    ok = q(C, "COMMIT"),
    {Ctx, C}.

stop({Ctx, _C}) -> db_test_helper:stop_db(Ctx).

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE big (id INTEGER, m INTEGER)"),
    ok = q(C, "CREATE TABLE mid (id INTEGER, s INTEGER)"),
    ok = q(C, "CREATE TABLE small (id INTEGER, v VARCHAR)"),
    ok = q(C, "BEGIN"),
    _ = [{ok, _} = q(C, ins("big", I, I rem 10 + 1)) || I <- lists:seq(1, 40)],
    _ = [{ok, _} = q(C, ins("mid", I, I rem 3 + 1))  || I <- lists:seq(1, 10)],
    _ = [{ok, _} = q(C, lists:flatten(io_lib:format(
                          "INSERT INTO small VALUES (~p, 'v~p')", [I, I])))
         || I <- lists:seq(1, 3)],
    ok = q(C, "COMMIT"),
    C.

ins(T, A, B) ->
    lists:flatten(io_lib:format("INSERT INTO ~s VALUES (~p, ~p)", [T, A, B])).

plan(C, Sql) ->
    {ok, _, R} = q(C, "EXPLAIN " ++ Sql),
    [L || [L] <- R].

rows(C, Sql) ->
    ok = q(C, "BEGIN"),
    {ok, _, R} = q(C, Sql),
    ok = q(C, "ROLLBACK"),
    R.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
