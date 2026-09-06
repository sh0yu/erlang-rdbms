%%%-------------------------------------------------------------------
%%% 集合演算。UNION(∪) / INTERSECT(∩) / EXCEPT(−)。
%%%
%%% 重複の扱いが要点。ALL は重複度を保つ。
%%%   a = [1, 2, 2, 3]、b = [2] のとき
%%%     INTERSECT ALL  → [2]        右の2は1つしか無いので1回だけ一致
%%%     EXCEPT ALL     → [1, 2, 3]  右の2が左の2を1つだけ打ち消す
%%%
%%% NULL の扱いは `=` と違う。集合演算の重複判定では NULL 同士は
%%% 等しいとみなす(標準SQLの "not distinct from")。
%%%-------------------------------------------------------------------
-module(sql_setop_tests).

-include_lib("eunit/include/eunit.hrl").

setop_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun union_removes_duplicates/1,
      fun union_all_keeps_duplicates/1,
      fun intersect_multiplicity/1,
      fun except_multiplicity/1,
      fun nulls_are_equal_for_set_ops/1,
      fun arity_must_match/1,
      fun column_names_come_from_the_left/1,
      fun order_by_name_and_position/1,
      fun order_by_expression_is_rejected/1,
      fun order_by_out_of_range_is_rejected/1,
      fun intersect_binds_tighter_than_union/1,
      fun limit_applies_to_the_whole/1,
      fun chained_unions/1]}.

union_removes_duplicates(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [2], [3], [4]],
                     rows(C, "SELECT k FROM a UNION SELECT k FROM b ORDER BY k"))
    end.

union_all_keeps_duplicates(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [2], [2], [3], [2], [4]],
                     rows(C, "SELECT k FROM a UNION ALL SELECT k FROM b"))
    end.

intersect_multiplicity(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[2]], rows(C, "SELECT k FROM a INTERSECT SELECT k FROM b")),
        %% 左に2つ、右に1つ → 1つだけ
        ?assertEqual([[2]], rows(C, "SELECT k FROM a INTERSECT ALL SELECT k FROM b"))
    end.

except_multiplicity(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [3]],
                     rows(C, "SELECT k FROM a EXCEPT SELECT k FROM b ORDER BY k")),
        %% 右の2が左の2を1つだけ打ち消す。もう1つは残る
        ?assertEqual([[1], [2], [3]],
                     rows(C, "SELECT k FROM a EXCEPT ALL SELECT k FROM b ORDER BY k"))
    end.

%% 集合演算の重複判定では NULL 同士は等しい。
%% `=` の意味論(NULL = NULL は unknown)をそのまま使うと、
%% NULL の行が UNION で重複除去されずに残る。
nulls_are_equal_for_set_ops(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE n1 (k INTEGER)"),
        ok = q(C, "CREATE TABLE n2 (k INTEGER)"),
        ok = q(C, "BEGIN"),
        {ok, _} = q(C, "INSERT INTO n1 VALUES (1)"),
        {ok, _} = q(C, "INSERT INTO n1 VALUES (null)"),
        {ok, _} = q(C, "INSERT INTO n2 VALUES (null)"),
        ok = q(C, "COMMIT"),
        ?assertEqual([[1], [null]],
                     rows(C, "SELECT k FROM n1 UNION SELECT k FROM n2 ORDER BY k")),
        ?assertEqual([[null]],
                     rows(C, "SELECT k FROM n1 INTERSECT SELECT k FROM n2")),
        ?assertEqual([[1]],
                     rows(C, "SELECT k FROM n1 EXCEPT SELECT k FROM n2"))
    end.

arity_must_match(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {set_op_arity_mismatch, 1, 2}},
                     q(C, "SELECT k FROM a UNION SELECT k, v FROM b"))
    end.

column_names_come_from_the_left(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "BEGIN READ ONLY"),
        {ok, Cols, _} = q(C, "SELECT k FROM a UNION SELECT v FROM b"),
        ok = q(C, "COMMIT"),
        ?assertEqual([k], Cols)
    end.

order_by_name_and_position(_) ->
    fun() ->
        C = seeded(),
        ByName = rows(C, "SELECT k FROM a UNION SELECT k FROM b ORDER BY k DESC"),
        ByPos  = rows(C, "SELECT k FROM a UNION SELECT k FROM b ORDER BY 1 DESC"),
        ?assertEqual([[4], [3], [2], [1]], ByName),
        ?assertEqual(ByName, ByPos)
    end.

%% 集合演算の結果には元のスコープが無いので、任意の式は書けない。
order_by_expression_is_rejected(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, set_op_order_by_must_be_column_or_position},
                     q(C, "SELECT k FROM a UNION SELECT k FROM b ORDER BY k + 1"))
    end.

order_by_out_of_range_is_rejected(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual({error, {order_by_position_out_of_range, 3, 1}},
                     q(C, "SELECT k FROM a UNION SELECT k FROM b ORDER BY 3"))
    end.

%% 標準SQLでは INTERSECT が UNION / EXCEPT より強い。
intersect_binds_tighter_than_union(_) ->
    fun() ->
        C = seeded(),
        %% a ∪ (b ∩ b) = a ∪ b
        ?assertEqual([[1], [2], [3], [4]],
                     rows(C, "SELECT k FROM a UNION SELECT k FROM b "
                             "INTERSECT SELECT k FROM b ORDER BY k"))
    end.

limit_applies_to_the_whole(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [2]],
                     rows(C, "SELECT k FROM a UNION SELECT k FROM b "
                             "ORDER BY k LIMIT 2"))
    end.

chained_unions(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [2], [3], [4]],
                     rows(C, "SELECT k FROM a UNION SELECT k FROM b "
                             "UNION SELECT k FROM a ORDER BY k"))
    end.

%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE a (k INTEGER, v VARCHAR)"),
    ok = q(C, "CREATE TABLE b (k INTEGER, v VARCHAR)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO a VALUES (1, 'x')"),
    {ok, _} = q(C, "INSERT INTO a VALUES (2, 'y')"),
    {ok, _} = q(C, "INSERT INTO a VALUES (2, 'y')"),
    {ok, _} = q(C, "INSERT INTO a VALUES (3, 'z')"),
    {ok, _} = q(C, "INSERT INTO b VALUES (2, 'y')"),
    {ok, _} = q(C, "INSERT INTO b VALUES (4, 'w')"),
    ok = q(C, "COMMIT"),
    C.

rows(C, Sql) ->
    ok = q(C, "BEGIN READ ONLY"),
    {ok, _, R} = q(C, Sql),
    ok = q(C, "COMMIT"),
    R.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
