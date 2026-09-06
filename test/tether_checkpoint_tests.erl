%%%-------------------------------------------------------------------
%%% @doc
%%% チェックポイント。ログの前半を捨てても、状態と記憶が失われないこと。
%%%
%%% ここで一番危ないのは順序である。スナップショットを永続化する前に
%%% ログを捨てると、その間の電源断で全部消える。逆(捨てるのが後)なら、
%%% 両方に同じ分が残るだけで安全 — ただし復旧側が読み飛ばす必要がある。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_checkpoint_tests).
-include_lib("eunit/include/eunit.hrl").

-import(tether_test_util, [tmpdir/0, rmrf/1]).

-define(K(K), {<<"t">>, <<K>>}).

with_db(F) ->
    _ = application:stop(tether),
    _ = application:load(tether),
    D = tmpdir(),
    application:set_env(tether, dir, D),
    {ok, _} = application:ensure_all_started(tether),
    try F(D) after ok = application:stop(tether), rmrf(D) end.

restart() ->
    ok = application:stop(tether),
    {ok, _} = application:ensure_all_started(tether),
    ok.

log_size(D) ->
    case file:read_file_info(tether_log:path(D)) of
        {ok, I} -> element(2, I);
        _       -> 0
    end.

%%%===================================================================

checkpoint_shrinks_log_and_keeps_state_test() ->
    with_db(fun(D) ->
        _ = [{ok, [ok]} = tether:request(<<"c">>, I,
                                         [{put, {<<"t">>, <<I:32>>}, <<I:32>>}])
             || I <- lists:seq(1, 100)],
        Before = log_size(D),
        ?assert(Before > 5000),

        ok = tether_store:checkpoint(),
        After = log_size(D),
        ?assertEqual(0, After),                    % 全部スナップショットに入った
        ?assert(filelib:is_regular(tether_snapshot:path(D))),

        %% 状態も記憶も残っている
        ?assertEqual(100, tether_store:size()),
        ?assertEqual({ok, <<50:32>>}, tether:read({<<"t">>, <<50:32>>})),
        restart(),
        ?assertEqual(100, tether_store:size()),
        ?assertEqual({ok, <<50:32>>}, tether:read({<<"t">>, <<50:32>>})),
        ?assertEqual(#{<<"c">> => {100, {ok, [{ok, [ok]}]}}}, tether_store:sessions()),

        %% 再送もちゃんと吸収される
        ?assertEqual({ok, [ok]},
                     tether:request(<<"c">>, 100, [{put, {<<"t">>, <<100:32>>},
                                                    <<100:32>>}]))
    end).

%% チェックポイントの後に書いたぶんは、ログに残って復旧で足される。
writes_after_checkpoint_are_replayed_test() ->
    with_db(fun(D) ->
        {ok, [ok]} = tether:request(<<"c">>, 1, [{put, ?K("a"), <<"1">>}]),
        ok = tether_store:checkpoint(),
        {ok, [ok]} = tether:request(<<"c">>, 2, [{put, ?K("b"), <<"2">>}]),
        {ok, [ok]} = tether:request(<<"c">>, 3, [{delete, ?K("a")}]),
        ?assert(log_size(D) > 0),

        restart(),
        ?assertEqual(not_found, tether:read(?K("a"))),
        ?assertEqual({ok, <<"2">>}, tether:read(?K("b"))),
        ?assertEqual(#{<<"c">> => {3, {ok, [{ok, [ok]}]}}}, tether_store:sessions())
    end).

%% スナップショットは書けたが、ログを捨てる前に電源が落ちた場合。
%% ログにはスナップショットに含まれる分も残っている。
%% 二重に適用してはいけない。
crash_between_snapshot_and_truncate_test() ->
    with_db(fun(D) ->
        {ok, [ok]} = tether:request(<<"c">>, 1, [{put, ?K("n"), <<"1">>}]),
        {ok, [ok]} = tether:request(<<"c">>, 2,
                                    [{cas, ?K("n"), <<"1">>, <<"2">>}]),
        %% ログを捨てずにスナップショットだけ書く(電源断の再現)
        ok = tether_snapshot:write(D, 2, snapshot_db(), tether_store:sessions()),
        ?assert(log_size(D) > 0),

        restart(),
        %% 読み飛ばしが効いていれば n は "2"。
        %% 二重適用なら cas が conflict になって食い違う。
        ?assertEqual({ok, <<"2">>}, tether:read(?K("n"))),
        ?assertEqual(#{<<"c">> => {2, {ok, [{ok, [ok]}]}}}, tether_store:sessions())
    end).

snapshot_db() ->
    lists:foldl(fun(K, Acc) ->
                        {ok, V} = tether_store:read(K),
                        Acc#{K => V}
                end, #{}, tether_store:keys()).

%% 壊れたスナップショットを黙って無視して起動してはいけない。
%% 無視すると、切り詰め済みのログしか無い状態で「空のDB」として
%% 立ち上がる。それは全件消失である。
corrupt_snapshot_refuses_to_start_test() ->
    with_db(fun(D) ->
        {ok, [ok]} = tether:request(<<"c">>, 1, [{put, ?K("a"), <<"1">>}]),
        ok = tether_store:checkpoint(),
        ok = application:stop(tether),

        P = tether_snapshot:path(D),
        {ok, Bin} = file:read_file(P),
        Off = 20,
        <<H:Off/binary, B:8, T/binary>> = Bin,
        ok = file:write_file(P, <<H/binary, (B bxor 16#FF):8, T/binary>>),

        ?assertMatch({error, {tether, {bad_snapshot, _}}},
                     catch_start()),
        %% 起動していないので、この後の stop に備えて戻しておく
        ok = file:write_file(P, Bin),
        {ok, _} = application:ensure_all_started(tether),
        ?assertEqual({ok, <<"1">>}, tether:read(?K("a")))
    end).

catch_start() ->
    case application:ensure_all_started(tether) of
        {error, {tether, {{shutdown, {failed_to_start_child, tether_store,
                                      {recovery_failed, R}}}, _}}} ->
            {error, {tether, R}};
        Other -> Other
    end.

%% ログが伸びたら自分でチェックポイントを取る。
%% 取らないとログが無限に伸び、復旧時間も伸び続ける。
auto_checkpoint_test_() ->
    {timeout, 60, fun() ->
        _ = application:stop(tether),
        _ = application:load(tether),
        D = tmpdir(),
        application:set_env(tether, dir, D),
        application:set_env(tether, checkpoint_after, 50),
        {ok, _} = application:ensure_all_started(tether),
        try
            _ = [{ok, [ok]} = tether:request(<<"c">>, I,
                                             [{put, {<<"t">>, <<I:32>>}, <<I:32>>}])
                 || I <- lists:seq(1, 200)],
            wait_small_log(D, 200),
            ?assert(log_size(D) < 200 * 100),
            ?assert(filelib:is_regular(tether_snapshot:path(D))),

            restart(),
            ?assertEqual(200, tether_store:size()),
            ?assertEqual({ok, <<7:32>>}, tether:read({<<"t">>, <<7:32>>})),
            ?assertEqual(200, tether_store:entries()),
            %% 通し番号が続いていること。振り直されていたら、
            %% 次のチェックポイントで読み飛ばしが狂う。
            {ok, [ok]} = tether:request(<<"c">>, 201,
                                        [{put, ?K("after"), <<"1">>}]),
            ?assertEqual(201, tether_store:entries()),
            restart(),
            ?assertEqual({ok, <<"1">>}, tether:read(?K("after")))
        after
            ok = application:stop(tether),
            application:unset_env(tether, checkpoint_after),
            rmrf(D)
        end
    end}.

wait_small_log(_D, 0) -> error(no_checkpoint);
wait_small_log(D, N) ->
    case filelib:is_regular(tether_snapshot:path(D)) of
        true  -> ok;
        false -> timer:sleep(20), wait_small_log(D, N - 1)
    end.
