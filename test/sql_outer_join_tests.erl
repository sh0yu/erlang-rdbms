%%%-------------------------------------------------------------------
%%% 外部結合。LEFT / RIGHT / FULL。
%%%
%%% 実装は入れ子ループもハッシュ結合も同じ状態機械で、違いは
%%% 「右側の候補をどこから取るか」だけ。外部結合のために足したのは
%%% **右側の行に添字を振って、一致したものを覚えること**。
%%% 左を読み切ったあと、一致しなかった右の行を左をNULLで埋めて出す。
%%%-------------------------------------------------------------------
-module(sql_outer_join_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/plan.hrl").

outer_join_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun left_join/1,
      fun right_join/1,
      fun full_join/1,
      fun nulls_in_the_key_never_match/1,
      fun nested_loop_agrees_with_hash/1,
      fun right_join_pushdown_is_mirrored/1,
      fun full_join_pushes_nothing/1,
      fun explain_shows_the_type/1]}.

left_join(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"a1">>, null], [<<"a2">>, <<"b2">>], [<<"anull">>, null]],
                     rows(C, "SELECT a.v, b.w FROM a LEFT JOIN b ON a.k = b.k "
                             "ORDER BY a.v"))
    end.

right_join(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"a2">>, <<"b2">>], [null, <<"b3">>], [null, <<"bnull">>]],
                     rows(C, "SELECT a.v, b.w FROM a RIGHT JOIN b ON a.k = b.k "
                             "ORDER BY b.w"))
    end.

full_join(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"a1">>, null],
                      [<<"a2">>, <<"b2">>],
                      [<<"anull">>, null],
                      [null, <<"b3">>],
                      [null, <<"bnull">>]],
                     rows(C, "SELECT a.v, b.w FROM a FULL OUTER JOIN b ON a.k = b.k "
                             "ORDER BY a.v, b.w"))
    end.

%% 鍵が NULL の行は一致しない。ハッシュ表に入れると衝突するので
%% 入れないが、そのぶん**未一致として出す**必要がある。
nulls_in_the_key_never_match(_) ->
    fun() ->
        C = seeded(),
        %% b の bnull は k が NULL。RIGHT なので必ず出る
        R = rows(C, "SELECT b.w FROM a RIGHT JOIN b ON a.k = b.k ORDER BY b.w"),
        ?assert(lists:member([<<"bnull">>], R)),
        %% a の anull も同様に LEFT では必ず出る
        L = rows(C, "SELECT a.v FROM a LEFT JOIN b ON a.k = b.k ORDER BY a.v"),
        ?assert(lists:member([<<"anull">>], L))
    end.

%% 等値を消すと入れ子ループが選ばれる。どちらでも同じ結果になること。
nested_loop_agrees_with_hash(_) ->
    fun() ->
        C = seeded(),
        Hash = rows(C, "SELECT a.v, b.w FROM a FULL OUTER JOIN b ON a.k = b.k "
                       "ORDER BY a.v, b.w"),
        Nl = rows(C, "SELECT a.v, b.w FROM a FULL OUTER JOIN b "
                     "ON NOT (a.k < b.k) AND NOT (a.k > b.k) ORDER BY a.v, b.w"),
        ?assertMatch(#p_project{input = #p_nl_join{type = full}},
                     plan("SELECT a.v FROM a FULL OUTER JOIN b "
                          "ON NOT (a.k < b.k) AND NOT (a.k > b.k)")),
        ?assertEqual(Hash, Nl)
    end.

%% RIGHT は LEFT の鏡像。保存される側(右)の WHERE は落とせるが、
%% NULLを供給する側(左)の WHERE は落とせない。
right_join_pushdown_is_mirrored(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([<<"Project (v)">>,
                      <<"  Filter (v = 'x')">>,
                      <<"    Hash RIGHT Join on k = k">>,
                      <<"      Seq Scan on a">>,
                      <<"      Filter (w = 'b3')">>,
                      <<"        Seq Scan on b">>],
                     explain(C, "SELECT a.v FROM a RIGHT JOIN b ON a.k = b.k "
                                "WHERE b.w = 'b3' AND a.v = 'x'"))
    end.

%% FULL は両側が「NULLを供給する側」なので何も落とせない。
full_join_pushes_nothing(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([<<"Project (v)">>,
                      <<"  Filter ((w = 'b3') AND (v = 'x'))">>,
                      <<"    Hash FULL Join on k = k">>,
                      <<"      Seq Scan on a">>,
                      <<"      Seq Scan on b">>],
                     explain(C, "SELECT a.v FROM a FULL JOIN b ON a.k = b.k "
                                "WHERE b.w = 'b3' AND a.v = 'x'"))
    end.

explain_shows_the_type(_) ->
    fun() ->
        C = seeded(),
        ?assert(lists:any(fun(L) -> binary:match(L, <<"Hash FULL Join">>) =/= nomatch end,
                          explain(C, "SELECT a.v FROM a FULL JOIN b ON a.k = b.k")))
    end.

%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE a (k INTEGER, v VARCHAR)"),
    ok = q(C, "CREATE TABLE b (k INTEGER, w VARCHAR)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO a VALUES (1, 'a1')"),
    {ok, _} = q(C, "INSERT INTO a VALUES (2, 'a2')"),
    {ok, _} = q(C, "INSERT INTO a (v) VALUES ('anull')"),
    {ok, _} = q(C, "INSERT INTO b VALUES (2, 'b2')"),
    {ok, _} = q(C, "INSERT INTO b VALUES (3, 'b3')"),
    {ok, _} = q(C, "INSERT INTO b (w) VALUES ('bnull')"),
    ok = q(C, "COMMIT"),
    C.

plan(Sql) ->
    {ok, Ast} = sql:parse(Sql),
    {ok, {select, L}} = sql_analyzer:analyze(Ast),
    sql_planner:plan(L).

explain(C, Sql) ->
    {ok, _, R} = q(C, "EXPLAIN " ++ Sql),
    [L || [L] <- R].

rows(C, Sql) ->
    ok = q(C, "BEGIN READ ONLY"),
    {ok, _, R} = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
