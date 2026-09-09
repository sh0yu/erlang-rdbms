%%%-------------------------------------------------------------------
%%% ハッシュ結合。
%%%
%%% 等値で結べるときに使う。入れ子ループが |左|×|右| 回の比較をするのに対し
%%% |左|+|右| で済む。右側はどちらの方式でもメモリに載せるので、
%%% 使うメモリは変わらない。
%%%
%%% 危ないのは NULL。NULL = NULL は unknown なので決して一致しないが、
%%% ハッシュ表に入れると NULL 同士が同じ鍵で衝突して一致してしまう。
%%%-------------------------------------------------------------------
-module(sql_hash_join_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/plan.hrl").

hash_join_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun nulls_never_match/1,
      fun left_join_nulls_are_padded_not_matched/1,
      fun integer_and_float_keys_match/1,
      fun composite_key/1,
      fun residual_predicate_is_applied/1,
      fun same_result_as_nested_loop/1,
      fun output_order_matches_nested_loop/1]}.

%% **NULL は決して一致しない。** ハッシュ表に入れると衝突する。
nulls_never_match(_) ->
    fun() ->
        C = seeded(),
        %% a.k と b.k はどちらも NULL の行を持つ
        ?assertEqual([[<<"a1">>, <<"b1">>]],
                     rows(C, "SELECT a.v, b.v FROM a JOIN b ON a.k = b.k "
                             "ORDER BY a.v")),
        %% 計画がハッシュ結合であることを確かめておく
        ?assertMatch(#p_project{input = #p_hash_join{}},
                     plan(C, "SELECT a.v FROM a JOIN b ON a.k = b.k"))
    end.

%% LEFT JOIN では、NULL の鍵を持つ左の行は「一致無し」としてNULL埋めされる。
%% 一致してしまうと行が消える(あるいは増える)。
left_join_nulls_are_padded_not_matched(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"a1">>, <<"b1">>],
                      [<<"a2">>, null],
                      [<<"a3">>, null]],
                     rows(C, "SELECT a.v, b.v FROM a LEFT JOIN b ON a.k = b.k "
                             "ORDER BY a.v"))
    end.

%% SQL では 100 = 100.0。素の項を鍵にすると別物になる。
integer_and_float_keys_match(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE x (k INTEGER, v VARCHAR)"),
        ok = q(C, "CREATE TABLE y (k FLOAT, v VARCHAR)"),
        ok = q(C, "BEGIN"),
        {ok, _} = q(C, "INSERT INTO x VALUES (100, 'x100')"),
        {ok, _} = q(C, "INSERT INTO y VALUES (100.0, 'y100')"),
        ok = q(C, "COMMIT"),
        ?assertEqual([[<<"x100">>, <<"y100">>]],
                     rows(C, "SELECT x.v, y.v FROM x JOIN y ON x.k = y.k"))
    end.

%% 等値が2つあれば鍵は2つ組になる。
composite_key(_) ->
    fun() ->
        C = seeded(),
        P = plan(C, "SELECT a.v FROM a JOIN b ON a.k = b.k AND a.g = b.g"),
        #p_project{input = HJ} = P,
        ?assertEqual(2, length(HJ#p_hash_join.left_keys)),
        ?assertEqual(2, length(HJ#p_hash_join.right_keys)),
        ?assertEqual([[<<"a1">>]],
                     rows(C, "SELECT a.v FROM a JOIN b ON a.k = b.k AND a.g = b.g"))
    end.

%% 等値以外の条件は、一致した組に対して評価する。
residual_predicate_is_applied(_) ->
    fun() ->
        C = seeded(),
        P = plan(C, "SELECT a.v FROM a JOIN b ON a.k = b.k AND a.g > b.g"),
        ?assertMatch(#p_project{input = #p_hash_join{}}, P),
        #p_project{input = HJ} = P,
        ?assertNotEqual(undefined, HJ#p_hash_join.pred),
        ?assertEqual([], rows(C, "SELECT a.v FROM a JOIN b ON a.k = b.k AND a.g > b.g"))
    end.

%% 入れ子ループと同じ結果になること。
%% 等値を含まない条件にすると入れ子ループが選ばれるので、
%% 同じ意味の条件を2通りに書いて突き合わせる。
same_result_as_nested_loop(_) ->
    fun() ->
        C = seeded(),
        Hash = rows(C, "SELECT a.v, b.v FROM a JOIN b ON a.k = b.k ORDER BY a.v"),
        %% k IS NOT NULL を足しても意味は変わらないが、
        %% 等値を消すために不等号2つで書き換える
        Nl = rows(C, "SELECT a.v, b.v FROM a JOIN b "
                     "ON NOT (a.k < b.k) AND NOT (a.k > b.k) ORDER BY a.v"),
        ?assertMatch(#p_project{input = #p_nl_join{}},
                     plan(C, "SELECT a.v FROM a JOIN b "
                             "ON NOT (a.k < b.k) AND NOT (a.k > b.k)")),
        ?assertEqual(Hash, Nl)
    end.

%% 右側の候補を返す順序が入れ子ループと同じであること。
%% ハッシュ表に積むと逆順になるので、積み直している。
output_order_matches_nested_loop(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE p (k INTEGER)"),
        ok = q(C, "CREATE TABLE r (k INTEGER, tag VARCHAR)"),
        ok = q(C, "BEGIN"),
        {ok, _} = q(C, "INSERT INTO p VALUES (1)"),
        {ok, _} = q(C, "INSERT INTO r VALUES (1, 'first')"),
        {ok, _} = q(C, "INSERT INTO r VALUES (1, 'second')"),
        {ok, _} = q(C, "INSERT INTO r VALUES (1, 'third')"),
        ok = q(C, "COMMIT"),
        ?assertEqual([[<<"first">>], [<<"second">>], [<<"third">>]],
                     rows(C, "SELECT r.tag FROM p JOIN r ON p.k = r.k"))
    end.

%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE a (k INTEGER, g INTEGER, v VARCHAR)"),
    ok = q(C, "CREATE TABLE b (k INTEGER, g INTEGER, v VARCHAR)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO a VALUES (1, 5, 'a1')"),
    {ok, _} = q(C, "INSERT INTO a (g, v) VALUES (5, 'a2')"),   % k は NULL
    {ok, _} = q(C, "INSERT INTO a VALUES (9, 5, 'a3')"),
    {ok, _} = q(C, "INSERT INTO b VALUES (1, 5, 'b1')"),
    {ok, _} = q(C, "INSERT INTO b (g, v) VALUES (5, 'b2')"),   % k は NULL
    ok = q(C, "COMMIT"),
    C.

plan(C, Sql) ->
    {ok, Ast} = sql:parse(Sql),
    {ok, {select, L}} = sql_analyzer:analyze(Ast),
    _ = C,
    sql_planner:plan(L).

rows(C, Sql) ->
    ok = q(C, "BEGIN READ ONLY"),
    {ok, _, R} = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
