%%%-------------------------------------------------------------------
%%% SQLシェルの純粋な部分(文の分割・値の整形・エラー文言)。
%%%-------------------------------------------------------------------
-module(sql_shell_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% スクリプトの分割
%%%===================================================================

split_single_test() ->
    ?assertEqual(["SELECT * FROM t"], sql_shell:split_statements("SELECT * FROM t;")).

split_multiple_test() ->
    ?assertEqual(["BEGIN", "SELECT * FROM t", "COMMIT"],
                 sql_shell:split_statements("BEGIN; SELECT * FROM t; COMMIT;")).

split_across_lines_test() ->
    ?assertEqual(["SELECT * FROM t WHERE a = 1"],
                 sql_shell:split_statements("SELECT *\n  FROM t\n  WHERE a = 1;")).

%% -- から行末まではコメント
split_strips_comments_test() ->
    ?assertEqual(["SELECT 1 FROM t"],
                 sql_shell:split_statements("-- これはコメント\nSELECT 1 FROM t; -- 末尾コメント")).

split_ignores_blank_and_comment_only_test() ->
    ?assertEqual([], sql_shell:split_statements("\n\n-- comment only\n\n")),
    ?assertEqual([], sql_shell:split_statements(";;;")).

%%%===================================================================
%%% 値の整形
%%%===================================================================

render_null_test() ->
    ?assertEqual("NULL", sql_shell:render(null)).

render_booleans_test() ->
    ?assertEqual("true", sql_shell:render(true)),
    ?assertEqual("false", sql_shell:render(false)).

%% 文字列は引用符なしで出す
render_binary_test() ->
    ?assertEqual("apple", sql_shell:render(<<"apple">>)).

render_numbers_test() ->
    ?assertEqual("100", sql_shell:render(100)),
    ?assertEqual("-1", sql_shell:render(-1)),
    ?assertEqual("1.5", sql_shell:render(1.5)).

render_atom_test() ->
    ?assertEqual("apple", sql_shell:render(apple)).

%% 長い値は打ち切る。表が崩れないようにするため。
%% 幅そのものは実装の都合(EXPLAIN の行が入るかどうか)で動くので、
%% 「入力より短くなり、打ち切りが分かる」ことだけを見る。
render_truncates_long_values_test() ->
    Long = list_to_binary(lists:duplicate(500, $x)),
    R = sql_shell:render(Long),
    ?assert(length(R) < 500),
    ?assert(lists:suffix("...", R)),
    %% 実行計画の行(80桁程度)は切られないこと
    Plan = "      Nested Loop LEFT Join on ((dept = id) AND (sal > 100))",
    ?assertEqual(Plan, sql_shell:render(list_to_binary(Plan))).

%%%===================================================================
%%% エラー文言
%%%===================================================================

%% 内部のタプルをそのまま出さず、読める文にすること
format_error_test_() ->
    Cases =
        [{{table_not_found, "nosuch"}, "no such table: nosuch"},
         {{column_not_found, "nocol"}, "no such column: nocol"},
         {column_count_mismatch,
          "number of values does not match the number of columns"},
         {{type_mismatch, price, integer, <<"abc">>},
          "column price expects INTEGER, got abc"},
         {{duplicate_columns, ["a", "b"]}, "duplicate columns: a, b"},
         {{syntax_error, 1, "syntax error before: from"},
          "syntax error at line 1: syntax error before: from"}],
    [?_assertEqual(Expected, lists:flatten(sql_shell:format_error(Input)))
     || {Input, Expected} <- Cases].

%% 知らない形でも落ちないこと
format_error_unknown_test() ->
    ?assertEqual("{weird,thing}", lists:flatten(sql_shell:format_error({weird, thing}))).
