-module(tether_store_tests).
-include_lib("eunit/include/eunit.hrl").

-import(tether_test_util, [tmpdir/0, rmrf/1]).

-define(K(K), {<<"t">>, <<K>>}).

%%%===================================================================
%%% 土台
%%%===================================================================

with_db(F) ->
    _ = application:stop(tether),
    %% load を先に済ませる。application:load/1 は .app の env を読み込むので、
    %% 未 load の状態で set_env しても、そこで上書きされて既定値に戻る。
    %% (VM 内で最初に走ったテストだけが既定の "data" を掴む、という
    %%  再現しにくい形で出る。実際に12MBの残骸を作って気づいた)
    _ = application:load(tether),
    D = tmpdir(),
    application:set_env(tether, dir, D),
    {ok, _} = application:ensure_all_started(tether),
    try F(D) after ok = application:stop(tether), rmrf(D) end.

restart() ->
    ok = application:stop(tether),
    {ok, _} = application:ensure_all_started(tether),
    ok.

%%%===================================================================
%%% 書いて読む
%%%===================================================================

submit_and_read_test() ->
    with_db(fun(_) ->
        ?assertEqual({ok, [ok]},
                     tether_store:submit(<<"c1">>, 1, [{put, ?K("a"), <<"1">>}])),
        ?assertEqual({ok, <<"1">>}, tether_store:read(?K("a"))),
        ?assertEqual(not_found, tether_store:read(?K("zzz")))
    end).

%%%===================================================================
%%% 永続性 — 答えたものは再起動を越えて残る
%%%===================================================================

durable_across_restart_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether_store:submit(<<"c1">>, 1, [{put, ?K("a"), <<"1">>}]),
        {ok, [ok]} = tether_store:submit(<<"c1">>, 2, [{put, ?K("b"), <<"2">>}]),
        {ok, [ok]} = tether_store:submit(<<"c1">>, 3, [{delete, ?K("a")}]),

        restart(),

        ?assertEqual(not_found, tether_store:read(?K("a"))),
        ?assertEqual({ok, <<"2">>}, tether_store:read(?K("b"))),
        ?assertEqual([?K("b")], tether_store:keys())
    end).

%% セッションの状態も復旧する。これが無いと、再起動後の再送に
%% 初回と同じ答えを返せない。
sessions_survive_restart_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether_store:submit(<<"alice">>, 7, [{put, ?K("a"), <<"1">>}]),
        {ok, [ok]} = tether_store:submit(<<"bob">>,   3, [{put, ?K("b"), <<"2">>}]),
        restart(),
        ?assertEqual(#{<<"alice">> => {7, {ok, [ok]}},
                       <<"bob">>   => {3, {ok, [ok]}}},
                     tether_store:sessions())
    end).

%%%===================================================================
%%% 原子性
%%%===================================================================

failed_request_applies_nothing_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether_store:submit(<<"c1">>, 1, [{put, ?K("x"), <<"0">>}]),
        R = tether_store:submit(<<"c1">>, 2,
                                [{put, ?K("y"), <<"1">>},
                                 {cas, ?K("x"), <<"ちがう">>, <<"9">>},
                                 {put, ?K("z"), <<"2">>}]),
        ?assertEqual({error, 2, {conflict, <<"0">>}}, R),
        ?assertEqual([?K("x")], tether_store:keys()),
        ?assertEqual({ok, <<"0">>}, tether_store:read(?K("x")))
    end).

%% 失敗した要求もログに残す。残さないと、再送されたときに
%% 再実行してしまい、そのときの状態次第で成功しうる。
%% それは「同じ要求に同じ答え」の破れになる。
failed_request_is_logged_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether_store:submit(<<"c1">>, 1, [{put, ?K("x"), <<"0">>}]),
        Err = tether_store:submit(<<"c1">>, 2,
                                  [{cas, ?K("x"), <<"ちがう">>, <<"9">>}]),
        ?assertMatch({error, _, _}, Err),
        restart(),
        ?assertEqual(#{<<"c1">> => {2, Err}}, tether_store:sessions())
    end).

%%%===================================================================
%%% ストアは fsync を待たない
%%%===================================================================

%% 同時に来た submit が1回の fsync にまとまること。
%% ストアがログを同期的に待つ実装だと、ここは必ず 1件/sync になる。
store_does_not_block_on_fsync_test_() ->
    {timeout, 30, fun() ->
        with_db(fun(_) ->
            N = 300,
            Parent = self(),
            Pids = [spawn_link(fun() ->
                        receive go -> ok end,
                        R = tether_store:submit(<<"c">>, I,
                                                [{put, {<<"t">>, <<I:64>>}, <<I:64>>}]),
                        Parent ! {done, R}
                    end) || I <- lists:seq(1, N)],
            _ = [P ! go || P <- Pids],
            _ = [receive {done, {ok, [ok]}} -> ok end || _ <- Pids],

            #{writes := W, syncs := Sy} = tether_log:stat(),
            ?assertEqual(N, W),
            ?assert(Sy < W),
            ?debugFmt("store: ~p submits / ~p syncs (~.1f 件/sync)", [W, Sy, W / Sy]),
            ?assertEqual(N, tether_store:size())
        end)
    end}.
