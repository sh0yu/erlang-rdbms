%%%-------------------------------------------------------------------
%%% スカラー関数・CASE・LIKE。
%%%
%%% スカラー関数は集約とは別物で、1行の中で値から値を作る。
%%% 名前で見分けるので、区別を誤ると集約プランに載って落ちる。
%%%
%%% NULL の扱いが要点。既定は「引数のどれかが NULL なら結果も NULL」。
%%% 例外は COALESCE と NULLIF で、この2つは NULL を見て分岐するのが仕事。
%%%-------------------------------------------------------------------
-module(sql_scalar_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% 関数そのもの(純粋)
%%%===================================================================

null_propagates_test() ->
    ?assertEqual(null, sql_func:apply("upper", [null])),
    ?assertEqual(null, sql_func:apply("round", [null, 2])),
    ?assertEqual(null, sql_func:apply("concat", [<<"a">>, null])).

coalesce_and_nullif_see_null_test() ->
    ?assertEqual(<<"b">>, sql_func:apply("coalesce", [null, <<"b">>, <<"c">>])),
    ?assertEqual(null, sql_func:apply("coalesce", [null, null])),
    ?assertEqual(null, sql_func:apply("nullif", [1, 1])),
    ?assertEqual(1, sql_func:apply("nullif", [1, 2])).

%% 型が合わないものは落とさず NULL。算術・比較と揃える。
type_mismatch_is_null_test() ->
    ?assertEqual(null, sql_func:apply("upper", [1])),
    ?assertEqual(null, sql_func:apply("abs", [<<"x">>])).

substr_is_one_based_test() ->
    ?assertEqual(<<"bcd">>, sql_func:apply("substr", [<<"abcdef">>, 2, 3])),
    ?assertEqual(<<"cdef">>, sql_func:apply("substr", [<<"abcdef">>, 3])),
    %% 範囲外は空文字列。落とさない
    ?assertEqual(<<>>, sql_func:apply("substr", [<<"abc">>, 9, 2])),
    ?assertEqual(<<>>, sql_func:apply("substr", [<<"abc">>, 1, 0])).

%% GREATEST / LEAST は SQL の順序で比べる。
%% Erlang の項順序だと 100 < <<"a">> が通ってしまう。
greatest_least_use_sql_ordering_test() ->
    ?assertEqual(5, sql_func:apply("greatest", [1, 5, 3])),
    ?assertEqual(1, sql_func:apply("least", [1, 5, 3])),
    %% 型が混ざると比較できないので、先頭が残る(安全側)
    ?assertEqual(1, sql_func:apply("greatest", [1, <<"a">>])).

arity_is_checked_test() ->
    ?assert(sql_func:is_scalar("round", 1)),
    ?assert(sql_func:is_scalar("round", 2)),
    ?assertNot(sql_func:is_scalar("round", 3)),
    ?assertNot(sql_func:is_scalar("nosuch", 1)),
    %% 可変長
    ?assert(sql_func:is_scalar("coalesce", 5)).

like_matcher_test() ->
    ?assertEqual(true,  sql_value:like(<<"apple">>, <<"ap%">>)),
    ?assertEqual(true,  sql_value:like(<<"apple">>, <<"a_ple">>)),
    ?assertEqual(true,  sql_value:like(<<"apple">>, <<"%le">>)),
    ?assertEqual(true,  sql_value:like(<<"apple">>, <<"%">>)),
    ?assertEqual(false, sql_value:like(<<"apple">>, <<"ap_">>)),
    ?assertEqual(false, sql_value:like(<<"apple">>, <<"banana">>)),
    %% 3値論理
    ?assertEqual(null,  sql_value:like(null, <<"a%">>)),
    ?assertEqual(null,  sql_value:like(<<"a">>, null)).

%% 文字単位で照合する。バイト単位だと多バイト文字で `_` が1文字にならない。
like_counts_characters_not_bytes_test() ->
    ?assertEqual(true, sql_value:like(<<"あい"/utf8>>, <<"_い"/utf8>>)),
    ?assertEqual(true, sql_value:like(<<"あい"/utf8>>, <<"__"/utf8>>)),
    ?assertEqual(false, sql_value:like(<<"あい"/utf8>>, <<"___"/utf8>>)).

%%%===================================================================
%%% SQL から
%%%===================================================================

sql_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun scalar_functions_in_select/1,
      fun aliased_aggregate_without_group_by/1,
      fun scalar_function_with_group_by/1,
      fun case_expression/1,
      fun case_without_else_is_null/1,
      fun like_and_not_like/1,
      fun unknown_function_is_reported/1,
      fun wrong_arity_is_reported/1,
      fun predicate_with_function_is_pushed_down/1]}.

scalar_functions_in_select(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"APPLE">>, 5]],
                     rows(C, "SELECT UPPER(s) AS u, LENGTH(s) AS n FROM t WHERE a = 1")),
        ?assertEqual([[2, 0.33]],
                     rows(C, "SELECT ABS(-2) AS x, ROUND(1 / 3.0, 2) AS r FROM t WHERE a = 1"))
    end.

%% `SELECT COUNT(*) AS n FROM t` は集約プランに載らなければならない。
%% 別名を見落とすと bind_expr が #func{} を知らずに落ちる(実際に落ちていた)。
aliased_aggregate_without_group_by(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[4]], rows(C, "SELECT COUNT(*) AS n FROM t")),
        ?assertEqual([[4]], rows(C, "SELECT COUNT(*) FROM t"))
    end.

scalar_function_with_group_by(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1, 1], [2, 1]],
                     rows(C, "SELECT ABS(a) AS x, COUNT(*) AS n FROM t "
                             "WHERE a < 3 GROUP BY a ORDER BY a"))
    end.

case_expression(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"low">>], [<<"mid">>], [<<"high">>], [<<"high">>]],
                     rows(C, "SELECT CASE WHEN a < 2 THEN 'low' "
                             "WHEN a < 3 THEN 'mid' ELSE 'high' END AS b "
                             "FROM t ORDER BY a"))
    end.

%% どの枝も真でなく ELSE も無ければ NULL。
%% 条件が NULL の枝は false と同じく飛ばす(3値論理)。
case_without_else_is_null(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[null]],
                     rows(C, "SELECT CASE WHEN a > 10 THEN 'big' END AS c "
                             "FROM t WHERE a = 1")),
        %% s が NULL の行では条件が unknown になり、枝は選ばれない
        ?assertEqual([[null]],
                     rows(C, "SELECT CASE WHEN s = 'x' THEN 'hit' END AS c "
                             "FROM t WHERE a = 4"))
    end.

like_and_not_like(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"apple">>], [<<"apricot">>]],
                     rows(C, "SELECT s FROM t WHERE s LIKE 'ap%' ORDER BY a")),
        %% NULL の行は NOT LIKE でも出てこない(unknown)
        ?assertEqual([[<<"banana">>]],
                     rows(C, "SELECT s FROM t WHERE s NOT LIKE 'ap%' ORDER BY a"))
    end.

unknown_function_is_reported(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {unknown_function, "nosuch"}},
                     q(C, "SELECT NOSUCH(a) FROM t"))
    end.

wrong_arity_is_reported(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {wrong_number_of_arguments, "upper", 2}},
                     q(C, "SELECT UPPER(s, a) FROM t"))
    end.

%% 関数を含む条件も結合の下へ落ちること。
%% refs/1 が関数を知らないと unknown になり、落とせなくなる。
predicate_with_function_is_pushed_down(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "CREATE TABLE u (a INTEGER)"),
        {ok, _, R} = q(C, "EXPLAIN SELECT t.s FROM t JOIN u ON t.a = u.a "
                          "WHERE UPPER(t.s) = 'APPLE'"),
        L = [X || [X] <- R],
        ?assert(lists:any(fun(X) ->
                                  binary:match(X, <<"Filter (UPPER(s) = 'APPLE')">>)
                                      =/= nomatch
                          end, L))
    end.

%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE t (a INTEGER, s VARCHAR)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO t VALUES (1, 'apple')"),
    {ok, _} = q(C, "INSERT INTO t VALUES (2, 'apricot')"),
    {ok, _} = q(C, "INSERT INTO t VALUES (3, 'banana')"),
    {ok, _} = q(C, "INSERT INTO t (a) VALUES (4)"),
    ok = q(C, "COMMIT"),
    C.

rows(C, Sql) ->
    ok = q(C, "BEGIN READ ONLY"),
    {ok, _, R} = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).

%%%===================================================================
%%% BETWEEN と簡易CASE
%%%
%%% どちらも糖衣で、意味解析で開いてから束縛する。
%%% 開き方が規格どおりか(特に NULL)をここで押さえる。
%%%===================================================================

between_test_() ->
    {setup, fun start/0, fun stop/1,
     fun({_Ctx, C}) ->
         [?_assertEqual([[2], [3], [4]], rows(C, "a", "a BETWEEN 2 AND 4")),
          %% 境界を含む
          ?_assertEqual([[2]], rows(C, "a", "a BETWEEN 2 AND 2")),
          %% 下限 > 上限なら空。x >= lo AND x <= hi に開くので自然にそうなる
          ?_assertEqual([], rows(C, "a", "a BETWEEN 4 AND 2")),
          %% NOT BETWEEN は NOT (…) で、NULL は通さない
          ?_assertEqual([[1], [5]], rows(C, "a", "a NOT BETWEEN 2 AND 4")),
          %% NULL は BETWEEN でも NOT BETWEEN でも通らない(unknown)
          ?_assertEqual([], rows(C, "a", "a IS NULL AND a BETWEEN 1 AND 9")),
          ?_assertEqual([], rows(C, "a", "a IS NULL AND a NOT BETWEEN 1 AND 9")),
          %% 両辺は算術式でよい
          ?_assertEqual([[2], [3], [4]], rows(C, "a", "b / 10 BETWEEN 1 + 1 AND 2 * 2")),
          %% 後ろに AND が続いても、BETWEEN の AND と取り違えない
          ?_assertEqual([[3], [4]], rows(C, "a", "a BETWEEN 2 AND 4 AND b > 20")),
          ?_assertEqual([[1], [2], [3], [4]],
                        rows(C, "a", "a BETWEEN 1 AND 2 OR a BETWEEN 3 AND 4")),
          %% 括弧で囲めば中はフル式に戻る
          ?_assertEqual([[1], [2]],
                        rows(C, "a", "a BETWEEN (SELECT min(a) FROM t) AND 2"))]
     end}.

simple_case_test_() ->
    {setup, fun start/0, fun stop/1,
     fun({_Ctx, C}) ->
         [?_assertEqual([[<<"one">>]],
                        rows(C, "CASE a WHEN 1 THEN 'one' END", "a = 1")),
          %% 一致しなければ ELSE
          ?_assertEqual([[<<"other">>]],
                        rows(C, "CASE a WHEN 1 THEN 'one' ELSE 'other' END", "a = 3")),
          %% ELSE が無ければ NULL
          ?_assertEqual([[null]], rows(C, "CASE a WHEN 1 THEN 'one' END", "a = 3")),
          %% NULL はどの枝にも一致しない(x = NULL は unknown)
          ?_assertEqual([[<<"other">>]],
                        rows(C, "CASE a WHEN NULL THEN 'null' ELSE 'other' END",
                             "a IS NULL")),
          %% 先に書いた枝が勝つ
          ?_assertEqual([[<<"first">>]],
                        rows(C, "CASE a WHEN 1 THEN 'first' WHEN 1 THEN 'second' END",
                             "a = 1"))]
     end}.

%%%===================================================================

rows(C, Select, Where) ->
    Sql = "SELECT " ++ Select ++ " FROM t WHERE " ++ Where ++ " ORDER BY 1",
    ok = q(C, "BEGIN READ ONLY"),
    {ok, _, R} = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

start() ->
    Ctx = db_test_helper:start_db(),
    C = connect(),
    ok = q(C, "CREATE TABLE t (a INTEGER, b INTEGER)"),
    ok = q(C, "BEGIN"),
    [{ok, _} = q(C, lists:flatten(io_lib:format("INSERT INTO t VALUES (~p, ~p)", [A, A * 10])))
     || A <- [1, 2, 3, 4, 5]],
    {ok, _} = q(C, "INSERT INTO t VALUES (NULL, 0)"),
    ok = q(C, "COMMIT"),
    {Ctx, C}.

stop({Ctx, _C}) ->
    db_test_helper:stop_db(Ctx).
