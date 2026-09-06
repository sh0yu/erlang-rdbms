-module(tether_data_tests).
-include_lib("eunit/include/eunit.hrl").

-define(K(C, K), {<<C>>, <<K>>}).

put_get_test() ->
    {ok, [ok], D1} = tether_data:apply_ops([{put, ?K("u", "1"), <<"ada">>}],
                                           ctx(), tether_data:new()),
    ?assertEqual({ok, <<"ada">>}, tether_data:get(?K("u", "1"), D1)),
    ?assertEqual(not_found, tether_data:get(?K("u", "2"), D1)).

delete_is_idempotent_test() ->
    D0 = tether_data:new(),
    {ok, [ok], D1} = tether_data:apply_ops([{delete, ?K("u", "9")}], ctx(), D0),
    ?assertEqual(D0, D1).

multi_op_results_test() ->
    D0 = tether_data:new(),
    {ok, R, D1} = tether_data:apply_ops(
                    [{put, ?K("u", "1"), <<"a">>},
                     {get, ?K("u", "1")},
                     {get, ?K("u", "2")},
                     {delete, ?K("u", "1")},
                     {get, ?K("u", "1")}], ctx(), D0),
    ?assertEqual([ok, {ok, <<"a">>}, not_found, ok, not_found], R),
    ?assertEqual(0, tether_data:size(D1)).

%%%===================================================================
%%% 原子性
%%%===================================================================

%% 途中で失敗したら、前半の書き込みも残らない。
all_or_nothing_test() ->
    {ok, _, D0} = tether_data:apply_ops([{put, ?K("a", "x"), <<"0">>}],
                                        ctx(), tether_data:new()),
    Ops = [{put, ?K("a", "y"), <<"1">>},
           {put, ?K("a", "z"), <<"2">>},
           {cas, ?K("a", "x"), <<"ちがう">>, <<"9">>},   % ここで失敗
           {put, ?K("a", "w"), <<"3">>}],
    {error, N, R, D1} = tether_data:apply_ops(Ops, ctx(), D0),
    ?assertEqual(3, N),
    ?assertEqual({conflict, <<"0">>}, R),
    %% 失敗する前に書いた y と z が残っていないこと
    ?assertEqual([?K("a", "x")], tether_data:keys(D1)),
    ?assertEqual({ok, <<"0">>}, tether_data:get(?K("a", "x"), D1)).

%%%===================================================================
%%% cas — read-modify-write を1往復にする
%%%===================================================================

cas_expects_absent_test() ->
    {ok, [ok], D1} = tether_data:apply_ops(
                       [{cas, ?K("a", "k"), undefined, <<"first">>}],
                       ctx(), tether_data:new()),
    ?assertEqual({ok, <<"first">>}, tether_data:get(?K("a", "k"), D1)),
    %% 2度目は既にあるので失敗する
    ?assertMatch({error, 1, {conflict, <<"first">>}, _},
                 tether_data:apply_ops(
                   [{cas, ?K("a", "k"), undefined, <<"second">>}], ctx(), D1)).

cas_expects_value_test() ->
    {ok, _, D1} = tether_data:apply_ops([{put, ?K("a", "k"), <<"v1">>}],
                                        ctx(), tether_data:new()),
    {ok, [ok], D2} = tether_data:apply_ops(
                       [{cas, ?K("a", "k"), <<"v1">>, <<"v2">>}], ctx(), D1),
    ?assertEqual({ok, <<"v2">>}, tether_data:get(?K("a", "k"), D2)),
    ?assertMatch({error, 1, {conflict, <<"v2">>}, _},
                 tether_data:apply_ops(
                   [{cas, ?K("a", "k"), <<"v1">>, <<"v3">>}], ctx(), D2)).

cas_on_missing_key_conflicts_test() ->
    ?assertMatch({error, 1, {conflict, undefined}, _},
                 tether_data:apply_ops(
                   [{cas, ?K("a", "nope"), <<"v">>, <<"w">>}], ctx(),
                   tether_data:new())).

%% 時刻とクライアントは環境ではなく引数から来る。
ctx() -> #{now => 1_700_000_000_000, client => <<"test">>}.
