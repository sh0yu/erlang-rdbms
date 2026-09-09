%%%-------------------------------------------------------------------
%%% SQLの値の意味論。3値論理と、Erlangの項順序との食い違い。
%%%-------------------------------------------------------------------
-module(sql_value_tests).

-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% 比較
%%%===================================================================

compare_numbers_test() ->
    ?assertEqual(lt, sql_value:compare(1, 2)),
    ?assertEqual(eq, sql_value:compare(2, 2)),
    ?assertEqual(gt, sql_value:compare(3, 2)),
    %% 整数と浮動小数点数は比較できる
    ?assertEqual(eq, sql_value:compare(2, 2.0)).

compare_binaries_test() ->
    ?assertEqual(lt, sql_value:compare(<<"apple">>, <<"banana">>)),
    ?assertEqual(eq, sql_value:compare(<<"apple">>, <<"apple">>)).

%% 比較の一方がNULLなら結果はNULL(unknown)
compare_with_null_test() ->
    ?assertEqual(null, sql_value:compare(null, 1)),
    ?assertEqual(null, sql_value:compare(1, null)),
    ?assertEqual(null, sql_value:compare(null, null)).

%% 型が違うものの比較は暗黙変換せずNULLにする
compare_type_mismatch_test() ->
    ?assertEqual(null, sql_value:compare(1, <<"1">>)),
    ?assertEqual(null, sql_value:compare(apple, <<"apple">>)).

%% Erlangの項順序では number < atom なので 100 < null が true になる。
%% SQLの比較がそれに引きずられていないこと。
does_not_use_erlang_term_order_test() ->
    ?assert(100 < null),                                  % Erlangの項順序
    ?assertEqual(null, sql_value:compare(100, null)).     % SQLの比較

%%%===================================================================
%%% 3値論理
%%%===================================================================

truth_and_test() ->
    ?assertEqual(true,  sql_value:truth_and(true, true)),
    ?assertEqual(false, sql_value:truth_and(true, false)),
    ?assertEqual(false, sql_value:truth_and(false, true)),
    ?assertEqual(false, sql_value:truth_and(false, false)),
    %% FALSE AND NULL = FALSE (NULLに伝播しない)
    ?assertEqual(false, sql_value:truth_and(false, null)),
    ?assertEqual(false, sql_value:truth_and(null, false)),
    ?assertEqual(null,  sql_value:truth_and(true, null)),
    ?assertEqual(null,  sql_value:truth_and(null, true)),
    ?assertEqual(null,  sql_value:truth_and(null, null)).

truth_or_test() ->
    ?assertEqual(true,  sql_value:truth_or(true, true)),
    ?assertEqual(true,  sql_value:truth_or(true, false)),
    ?assertEqual(true,  sql_value:truth_or(false, true)),
    ?assertEqual(false, sql_value:truth_or(false, false)),
    %% TRUE OR NULL = TRUE (NULLに伝播しない)
    ?assertEqual(true,  sql_value:truth_or(true, null)),
    ?assertEqual(true,  sql_value:truth_or(null, true)),
    ?assertEqual(null,  sql_value:truth_or(false, null)),
    ?assertEqual(null,  sql_value:truth_or(null, false)),
    ?assertEqual(null,  sql_value:truth_or(null, null)).

truth_not_test() ->
    ?assertEqual(false, sql_value:truth_not(true)),
    ?assertEqual(true,  sql_value:truth_not(false)),
    ?assertEqual(null,  sql_value:truth_not(null)).

%% WHERE が通すのは true だけ。null と false は等しく捨てる。
keep_test() ->
    ?assertEqual(true,  sql_value:keep(true)),
    ?assertEqual(false, sql_value:keep(false)),
    ?assertEqual(false, sql_value:keep(null)).

%%%===================================================================
%%% ORDER BY 用の順序
%%%===================================================================

order_puts_nulls_last_by_default_test() ->
    ?assertEqual(gt, sql_value:order_compare(null, 1, asc, nulls_last)),
    ?assertEqual(lt, sql_value:order_compare(1, null, asc, nulls_last)).

order_nulls_first_test() ->
    ?assertEqual(lt, sql_value:order_compare(null, 1, asc, nulls_first)),
    ?assertEqual(gt, sql_value:order_compare(1, null, asc, nulls_first)).

order_desc_reverses_non_null_test() ->
    ?assertEqual(lt, sql_value:order_compare(1, 2, asc, nulls_last)),
    ?assertEqual(gt, sql_value:order_compare(1, 2, desc, nulls_last)).

%% 実際に並べ替えてみて、NULLが数値の間に紛れ込まないこと
sorting_with_nulls_test() ->
    Vals = [3, null, 1, null, 2],
    Sorted = lists:sort(fun(A, B) ->
                                sql_value:order_compare(A, B, asc, nulls_last) =/= gt
                        end, Vals),
    ?assertEqual([1, 2, 3, null, null], Sorted).

%%%===================================================================
%%% グルーピング
%%%===================================================================

%% GROUP BY では NULL 同士は同じ組になる(`=` の意味論とは逆)
group_key_null_test() ->
    ?assertEqual(sql_value:group_key(null), sql_value:group_key(null)).

%% ETSのsetはキー比較に =:= を使うので、正規化しないと
%% 100 と 100.0 が別のグループになる
group_key_normalises_float_test() ->
    ?assertEqual(sql_value:group_key(100), sql_value:group_key(100.0)),
    ?assertNotEqual(sql_value:group_key(100.5), sql_value:group_key(100)).
