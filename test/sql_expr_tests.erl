%%%-------------------------------------------------------------------
%%% 式(比較・論理・算術・IS NULL)。
%%%
%%% 演算子の優先順位はASTに対して直接固定する。結果だけを見ていると、
%%% たまたま同じ行が返って誤った結合を見逃す。
%%%-------------------------------------------------------------------
-module(sql_expr_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/sql.hrl").

where(Sql) ->
    {ok, #select_stmt{where = W}} = sql:parse("SELECT * FROM t WHERE " ++ Sql),
    W.

col(N) -> #col_ref{name = N}.
c(V) -> #const{value = V}.

%%%===================================================================
%%% 優先順位
%%%
%%% yeccは規則の優先順位を「規則中の最後の終端記号」から決める。
%%% 比較演算子を comp_op のような非終端記号にまとめると規則に終端記号が
%%% 無くなり、優先順位が効かずに a >= (1 AND (a < 3)) と解釈される。
%%%===================================================================

comparison_binds_tighter_than_and_test() ->
    ?assertEqual(#binop{op = 'and',
                        left = #binop{op = '>=', left = col("a"), right = c(1)},
                        right = #binop{op = '<', left = col("a"), right = c(3)}},
                 where("a >= 1 AND a < 3")).

and_binds_tighter_than_or_test() ->
    ?assertEqual(#binop{op = 'or',
                        left = #binop{op = '=', left = col("a"), right = c(1)},
                        right = #binop{op = 'and',
                                       left = #binop{op = '=', left = col("a"), right = c(2)},
                                       right = #binop{op = '=', left = col("b"), right = c(3)}}},
                 where("a = 1 OR a = 2 AND b = 3")).

multiply_binds_tighter_than_add_test() ->
    ?assertEqual(#binop{op = '=',
                        left = #binop{op = '+', left = col("a"),
                                      right = #binop{op = '*', left = c(1), right = c(2)}},
                        right = c(5)},
                 where("a + 1 * 2 = 5")).

parens_override_precedence_test() ->
    #binop{op = '=', left = Lhs} = where("(a + 1) * 2 = 5"),
    ?assertEqual(#binop{op = '*',
                        left = #binop{op = '+', left = col("a"), right = c(1)},
                        right = c(2)},
                 Lhs).

%% NOT は比較全体にかかる。NOT (a) = 1 ではない。
not_applies_to_the_comparison_test() ->
    ?assertEqual(#unop{op = 'not',
                       arg = #binop{op = '=', left = col("a"), right = c(1)}},
                 where("NOT a = 1")).

%%%===================================================================
%%% end-to-end
%%%===================================================================

expr_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun comparisons/1,
      fun logic/1,
      fun is_null/1,
      fun three_valued_logic/1,
      fun arithmetic/1,
      fun arithmetic_in_projection/1,
      fun update_with_expression/1,
      fun assignment_uses_the_old_row/1,
      fun division_by_zero_is_null/1]}.

comparisons(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[2], [3], [4]], vals(C, "SELECT n FROM t WHERE n > 1")),
        ?assertEqual([[1], [2]], vals(C, "SELECT n FROM t WHERE n < 3")),
        ?assertEqual([[2], [3], [4]], vals(C, "SELECT n FROM t WHERE n >= 2")),
        ?assertEqual([[1], [2]], vals(C, "SELECT n FROM t WHERE n <= 2")),
        ?assertEqual([[1], [3], [4]], vals(C, "SELECT n FROM t WHERE n <> 2")),
        %% != は <> の別名
        ?assertEqual([[1], [3], [4]], vals(C, "SELECT n FROM t WHERE n != 2"))
    end.

logic(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[2]], vals(C, "SELECT n FROM t WHERE n >= 2 AND n < 3")),
        ?assertEqual([[1], [3], [4]], vals(C, "SELECT n FROM t WHERE n < 2 OR n > 2")),
        ?assertEqual([[1], [3], [4]], vals(C, "SELECT n FROM t WHERE NOT n = 2")),
        %% 括弧で結合を変えられること
        ?assertEqual([[1]], vals(C, "SELECT n FROM t WHERE (n = 1 OR n = 2) AND n < 2"))
    end.

is_null(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[4]], vals(C, "SELECT n FROM t WHERE s IS NULL")),
        ?assertEqual([[1], [2], [3]], vals(C, "SELECT n FROM t WHERE s IS NOT NULL"))
    end.

%% NULL を含む比較は unknown になり、WHERE を通らない。
%% NOT をつけても通らない、というのが3値論理でいちばん驚くところ。
three_valued_logic(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([], vals(C, "SELECT n FROM t WHERE s = 'zzz' AND n = 4")),
        ?assertEqual([], vals(C, "SELECT n FROM t WHERE NOT s = 'zzz' AND n = 4")),
        %% FALSE AND NULL は FALSE なので、行は当然通らない
        ?assertEqual([], vals(C, "SELECT n FROM t WHERE n = 999 AND s = 'a'")),
        %% TRUE OR NULL は TRUE。n=4 は s が NULL だが n=4 が真なので通る
        ?assertEqual([[1], [4]], vals(C, "SELECT n FROM t WHERE n = 4 OR s = 'a'"))
    end.

arithmetic(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[3]], vals(C, "SELECT n FROM t WHERE n + 1 = 4")),
        ?assertEqual([[3]], vals(C, "SELECT n FROM t WHERE n * 2 = 6")),
        ?assertEqual([[3]], vals(C, "SELECT n FROM t WHERE n - 1 = 2")),
        ?assertEqual([[2]], vals(C, "SELECT n FROM t WHERE n / 2 = 1"))
    end.

arithmetic_in_projection(_) ->
    fun() ->
        C = seeded(),
        {ok, Cols, Rows} = q(C, "SELECT n, n * 10 FROM t WHERE n = 2"),
        ?assertEqual([n, column2], Cols),
        ?assertEqual([[2, 20]], Rows)
    end.

update_with_expression(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({ok, 4}, q(C, "UPDATE t SET n = n + 100")),
        ?assertEqual([[101], [102], [103], [104]], vals(C, "SELECT n FROM t"))
    end.

%% 代入の右辺は更新前の行に対して評価する。
%% 途中結果を使うと SET a = b, b = a が入れ替えにならない。
assignment_uses_the_old_row(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE sw (a INTEGER, b INTEGER)"),
        ok = q(C, "BEGIN"),
        {ok, _} = q(C, "INSERT INTO sw VALUES (1, 2)"),
        {ok, 1} = q(C, "UPDATE sw SET a = b, b = a"),
        {ok, _, Rows} = q(C, "SELECT * FROM sw"),
        ?assertEqual([[2, 1]], Rows)
    end.

division_by_zero_is_null(_) ->
    fun() ->
        C = seeded(),
        {ok, _, Rows} = q(C, "SELECT n / 0 FROM t WHERE n = 1"),
        ?assertEqual([[null]], Rows)
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE t (n INTEGER, s VARCHAR)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO t VALUES (1, 'a')"),
    {ok, _} = q(C, "INSERT INTO t VALUES (2, 'b')"),
    {ok, _} = q(C, "INSERT INTO t VALUES (3, 'c')"),
    {ok, _} = q(C, "INSERT INTO t (n) VALUES (4)"),
    C.

vals(C, Sql) ->
    {ok, _Cols, Rows} = q(C, Sql),
    lists:sort(Rows).

connect() ->
    {ok, Pid} = gen_connection:connect(),
    Pid.

q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
