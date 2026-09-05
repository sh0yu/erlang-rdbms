%%%-------------------------------------------------------------------
%%% SQL層のテスト。
%%%   前半: 文字列 -> AST(構文解析だけ。カタログを見ない)
%%%   後半: 文字列 -> 行(表駆動のend-to-end)
%%%-------------------------------------------------------------------
-module(sql_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/sql.hrl").

%%%===================================================================
%%% 構文解析
%%%===================================================================

parse_select_star_test() ->
    ?assertMatch({ok, #select_stmt{columns = [#star{}],
                                   from = #table_ref{name = "fruit"},
                                   where = undefined}},
                 sql:parse("SELECT * FROM fruit")).

parse_column_list_test() ->
    {ok, #select_stmt{columns = Cols}} = sql:parse("SELECT name, price FROM fruit"),
    ?assertEqual([#col_ref{name = "name"}, #col_ref{name = "price"}], Cols).

parse_where_test() ->
    {ok, #select_stmt{where = W}} = sql:parse("SELECT * FROM fruit WHERE price = 150"),
    ?assertEqual(#binop{op = '=',
                        left = #col_ref{name = "price"},
                        right = #const{value = 150}}, W).

%% キーワードも識別子も大文字小文字を区別しない
parse_is_case_insensitive_test() ->
    {ok, A} = sql:parse("SELECT name FROM fruit"),
    {ok, B} = sql:parse("select NAME from FRUIT"),
    {ok, C} = sql:parse("SeLeCt Name FrOm Fruit"),
    ?assertEqual(A, B),
    ?assertEqual(A, C).

parse_trailing_semicolon_test() ->
    {ok, A} = sql:parse("SELECT * FROM fruit"),
    {ok, B} = sql:parse("SELECT * FROM fruit;"),
    ?assertEqual(A, B).

%% 文字列リテラルはbinaryになる。'' で単一引用符をエスケープする。
parse_string_literal_test() ->
    {ok, #select_stmt{where = W}} = sql:parse("SELECT * FROM t WHERE a = 'x'"),
    ?assertMatch(#binop{right = #const{value = <<"x">>}}, W),
    {ok, #select_stmt{where = W2}} = sql:parse("SELECT * FROM t WHERE a = 'it''s'"),
    ?assertMatch(#binop{right = #const{value = <<"it's">>}}, W2).

parse_errors_test() ->
    ?assertMatch({error, {syntax_error, _, _}}, sql:parse("SELECT FROM")),
    ?assertMatch({error, {syntax_error, _, _}}, sql:parse("SELECT * FROM")),
    ?assertMatch({error, {syntax_error, _, _}}, sql:parse("")),
    ?assertMatch({error, {syntax_error, _, _}}, sql:parse("NOTASTATEMENT")).

%%%===================================================================
%%% end-to-end
%%%===================================================================

sql_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun select_cases/1,
      fun error_cases/1,
      fun sees_uncommitted_changes/1,
      fun string_literal_does_not_match_atom/1,
      fun unsupported_syntax_is_reported/1]}.

%% 表駆動。{SQL, 期待する行(順不同)}
select_cases(_) ->
    fun() ->
        C = fixture(),
        Cases =
            [{"SELECT * FROM fruit",
              [[apple,100],[orange,150],[banana,150],[grape,300]]},
             {"SELECT name FROM fruit",
              [[apple],[orange],[banana],[grape]]},
             {"SELECT price, name FROM fruit WHERE price = 150",
              [[150,orange],[150,banana]]},
             {"SELECT * FROM fruit WHERE price = 100",
              [[apple,100]]},
             {"SELECT * FROM fruit WHERE price = 999",
              []},
             {"select * from FRUIT where PRICE = 300",
              [[grape,300]]}],
        [begin
             {ok, _Cols, Rows} = q(C, Sql),
             ?assertEqual(lists:sort(Expected), lists:sort(Rows))
         end || {Sql, Expected} <- Cases],
        ok
    end.

error_cases(_) ->
    fun() ->
        C = fixture(),
        ?assertEqual({error, {table_not_found, "nosuch"}}, q(C, "SELECT * FROM nosuch")),
        ?assertEqual({error, {column_not_found, "nocol"}}, q(C, "SELECT nocol FROM fruit")),
        ?assertEqual({error, {column_not_found, "nocol"}},
                     q(C, "SELECT * FROM fruit WHERE nocol = 1")),
        ?assertMatch({error, {syntax_error, _, _}}, q(C, "SELECT FROM"))
    end.

%% SQLの走査もトランザクションの未コミット変更を見ること
sees_uncommitted_changes(_) ->
    fun() ->
        C = fixture(),
        {ok, _} = query_exec:exec_query(C, {insert, fruit, [melon, 400]}),
        {ok, _Cols, Rows} = q(C, "SELECT * FROM fruit WHERE price = 400"),
        ?assertEqual([[melon, 400]], Rows),
        {ok, 1} = query_exec:exec_query(C, {delete, fruit, name, apple}),
        {ok, _Cols2, Rows2} = q(C, "SELECT * FROM fruit WHERE price = 100"),
        ?assertEqual([], Rows2)
    end.

%% 既知の制限: 型宣言がまだ無いため、古いタプルAPIで入れた値はアトムで、
%% SQLの文字列リテラルはbinaryになる。型が違う比較はNULLになり
%% WHEREを通らない。暗黙変換で誤魔化さず、Stage 2の型導入で解消する。
string_literal_does_not_match_atom(_) ->
    fun() ->
        C = fixture(),
        ?assertMatch({ok, _, []},
                     q(C, "SELECT * FROM fruit WHERE name = 'banana'"))
    end.

%% まだ受理しない構文が、黙って無視されるのではなく構文エラーになること
unsupported_syntax_is_reported(_) ->
    fun() ->
        C = fixture(),
        %% JOIN と修飾カラム名
        ?assertMatch({error, _},
                     q(C, "SELECT a.name FROM fruit a JOIN fruit b ON a.name = b.name")),
        %% 副問い合わせ
        ?assertMatch({error, {syntax_error, _, _}},
                     q(C, "SELECT * FROM fruit WHERE price = (SELECT price FROM fruit)"))
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

fixture() ->
    {ok, C} = gen_connection:connect(),
    ok = query_exec:exec_query(C, {create_table, fruit, [name, price]}),
    _ = query_exec:exec_query(C, {begin_tx}),
    [{ok, _} = query_exec:exec_query(C, {insert, fruit, R})
     || R <- [[apple,100],[orange,150],[banana,150],[grape,300]]],
    ok = query_exec:exec_query(C, {commit_tx}),
    _ = query_exec:exec_query(C, {begin_tx}),
    C.

q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
