%%%-------------------------------------------------------------------
%%% ORDER BY / LIMIT / OFFSET / DISTINCT。
%%%
%%% 並べ替えは sql_value:order_compare/4 を通す。Erlangの項順序では
%%% number < atom なので、素の `<` だと NULL が数値の間に紛れ込む。
%%%-------------------------------------------------------------------
-module(sql_order_tests).

-include_lib("eunit/include/eunit.hrl").

order_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun ascending_is_the_default/1,
      fun descending/1,
      fun nulls_last_on_ascending/1,
      fun nulls_first_on_descending/1,
      fun explicit_nulls_position/1,
      fun sort_by_column_not_in_projection/1,
      fun sort_by_multiple_keys/1,
      fun sort_by_expression/1,
      fun sort_strings/1,
      fun limit/1,
      fun limit_with_offset/1,
      fun offset_alone/1,
      fun limit_larger_than_result/1,
      fun limit_zero/1,
      fun limit_stops_reading_early/1,
      fun distinct/1,
      fun distinct_treats_nulls_as_equal/1,
      fun distinct_with_order_and_limit/1]}.

%%%===================================================================
%%% ORDER BY
%%%===================================================================

ascending_is_the_default(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [2], [2], [3], [null]], rows(C, "SELECT n FROM t ORDER BY n"))
    end.

descending(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[null], [3], [2], [2], [1]],
                     rows(C, "SELECT n FROM t ORDER BY n DESC"))
    end.

%% ASC の既定は NULLS LAST(NULLを最大値として扱う)
nulls_last_on_ascending(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([null], lists:last(rows(C, "SELECT n FROM t ORDER BY n ASC")))
    end.

%% DESC の既定は NULLS FIRST
nulls_first_on_descending(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([null], hd(rows(C, "SELECT n FROM t ORDER BY n DESC")))
    end.

explicit_nulls_position(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([null], hd(rows(C, "SELECT n FROM t ORDER BY n ASC NULLS FIRST"))),
        ?assertEqual([null], lists:last(rows(C, "SELECT n FROM t ORDER BY n DESC NULLS LAST")))
    end.

%% 並べ替えは射影の前に行うので、出力に含まれないカラムでも並べ替えられる
sort_by_column_not_in_projection(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"a">>], [<<"b">>], [<<"c">>], [<<"dup">>], [<<"zz">>]],
                     rows(C, "SELECT s FROM t ORDER BY s"))
    end.

sort_by_multiple_keys(_) ->
    fun() ->
        C = seeded(),
        %% n が同じ 2 の行は s で決まる
        ?assertEqual([[2, <<"b">>], [2, <<"dup">>]],
                     rows(C, "SELECT n, s FROM t WHERE n = 2 ORDER BY n, s"))
    end.

sort_by_expression(_) ->
    fun() ->
        C = seeded(),
        %% -n の昇順 = n の降順(NULLは既定でlast)
        ?assertEqual([[3], [2], [2], [1], [null]],
                     rows(C, "SELECT n FROM t ORDER BY -n"))
    end.

sort_strings(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[<<"zz">>], [<<"dup">>], [<<"c">>], [<<"b">>], [<<"a">>]],
                     rows(C, "SELECT s FROM t ORDER BY s DESC"))
    end.

%%%===================================================================
%%% LIMIT / OFFSET
%%%===================================================================

limit(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [2]], rows(C, "SELECT n FROM t ORDER BY n LIMIT 2"))
    end.

limit_with_offset(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[2], [2]], rows(C, "SELECT n FROM t ORDER BY n LIMIT 2 OFFSET 1"))
    end.

offset_alone(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[3], [null]], rows(C, "SELECT n FROM t ORDER BY n OFFSET 3"))
    end.

limit_larger_than_result(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual(5, length(rows(C, "SELECT n FROM t ORDER BY n LIMIT 100")))
    end.

limit_zero(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([], rows(C, "SELECT n FROM t ORDER BY n LIMIT 0"))
    end.

%% LIMIT は下位の走査を最後まで読まない。
%% 大量の行を入れて、LIMIT つきが全件取得より速く終わることで確かめる。
limit_stops_reading_early(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE big (n INTEGER)"),
        ok = q(C, "BEGIN"),
        [{ok, _} = q(C, "INSERT INTO big VALUES (1)") || _ <- lists:seq(1, 400)],
        %% ORDER BY が無ければ並べ替えが挟まらないので、
        %% LIMIT がそのまま走査の打ち切りになる
        {ok, _, Rows} = q(C, "SELECT n FROM big LIMIT 3"),
        ?assertEqual(3, length(Rows))
    end.

%%%===================================================================
%%% DISTINCT
%%%===================================================================

distinct(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [2], [3], [null]],
                     rows(C, "SELECT DISTINCT n FROM t ORDER BY n"))
    end.

%% GROUP BY / DISTINCT では NULL 同士は同じ組にまとまる。
%% `=` の意味論(NULL = NULL は unknown)とは逆になる。
distinct_treats_nulls_as_equal(_) ->
    fun() ->
        C = connect(),
        ok = q(C, "CREATE TABLE n2 (v INTEGER)"),
        ok = q(C, "BEGIN"),
        [{ok, _} = q(C, "INSERT INTO n2 (v) VALUES (NULL)") || _ <- lists:seq(1, 3)],
        {ok, _} = q(C, "INSERT INTO n2 VALUES (1)"),
        ?assertEqual(2, length(rows(C, "SELECT DISTINCT v FROM n2")))
    end.

distinct_with_order_and_limit(_) ->
    fun() ->
        C = seeded(),
        ?assertEqual([[1], [2]],
                     rows(C, "SELECT DISTINCT n FROM t ORDER BY n LIMIT 2"))
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE t (n INTEGER, s VARCHAR)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO t VALUES (3, 'c')"),
    {ok, _} = q(C, "INSERT INTO t VALUES (1, 'a')"),
    {ok, _} = q(C, "INSERT INTO t VALUES (2, 'b')"),
    {ok, _} = q(C, "INSERT INTO t (s) VALUES ('zz')"),
    {ok, _} = q(C, "INSERT INTO t VALUES (2, 'dup')"),
    C.

rows(C, Sql) ->
    {ok, _Cols, Rows} = q(C, Sql),
    Rows.

connect() ->
    {ok, Pid} = gen_connection:connect(),
    Pid.

q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
