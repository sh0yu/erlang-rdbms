%%%-------------------------------------------------------------------
%%% @doc
%%% 購読と差分の押し出し。**local-first の本体。**
%%%
%%% セッションが「生きたプロセス」であることが効くのはここである。
%%%
%%%   行にはできない       圏外の間、変更を溜め続けること
%%%   接続に紐づけると消える オフラインで溜まらない
%%%   スレッドでは載らない  10^6 クライアントぶん
%%%
%%% そして溜めるからには上限が要る。上限を持たないと、戻ってこない
%%% クライアント1人がメモリを食い潰す。PostgreSQL の論理レプリケーション
%%% スロットが WAL を溜め続けてディスクを埋める事故と同じ形で、
%%% あちらはスロット単位で止められない。ここでは**そのクライアントだけ**が
%%% 「取り直し」に落ちる。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_sync_tests).
-include_lib("eunit/include/eunit.hrl").

-import(tether_test_util, [tmpdir/0, rmrf/1]).

-define(K(K), {<<"orders">>, <<K>>}).

with_db(F) -> with_db(#{}, F).

with_db(Env, F) ->
    _ = application:stop(tether),
    _ = application:load(tether),
    D = tmpdir(),
    application:set_env(tether, dir, D),
    maps:foreach(fun(K, V) -> application:set_env(tether, K, V) end, Env),
    {ok, _} = application:ensure_all_started(tether),
    try F(D)
    after
        ok = application:stop(tether),
        maps:foreach(fun(K, _) -> application:unset_env(tether, K) end, Env),
        rmrf(D)
    end.

put(C, Seq, K, V) ->
    {ok, [ok]} = tether:request(C, Seq, [{put, K, V}]), ok.

%% 配布は cast なので、届くまで少し待つ。
wait(Client, V) -> wait(Client, V, 200).
wait(_C, _V, 0) -> error(timeout);
wait(C, V, N) ->
    case tether:session_info(C) of
        #{version := Cur} when Cur >= V -> ok;
        _ -> timer:sleep(5), wait(C, V, N - 1)
    end.

%%%===================================================================
%%% 購読と差分
%%%===================================================================

subscribe_returns_snapshot_test() ->
    with_db(fun(_) ->
        ok = put(<<"shop">>, 1, ?K("o1"), <<"a">>),
        ok = put(<<"shop">>, 2, ?K("o2"), <<"b">>),
        {ok, V, Rows} = tether:subscribe(<<"alice">>, <<"orders">>),
        ?assert(V >= 2),
        ?assertEqual([{?K("o1"), <<"a">>}, {?K("o2"), <<"b">>}], Rows)
    end).

%% 購読した後の変更が差分として返る。**全件ではない。**
delta_after_subscribe_test() ->
    with_db(fun(_) ->
        ok = put(<<"shop">>, 1, ?K("o1"), <<"a">>),
        {ok, _, _} = tether:subscribe(<<"alice">>, <<"orders">>),

        ok = put(<<"shop">>, 2, ?K("o2"), <<"b">>),
        ok = put(<<"shop">>, 3, ?K("o3"), <<"c">>),
        wait(<<"alice">>, 3),

        {delta, _, Rows} = tether:sync(<<"alice">>),
        ?assertEqual([{?K("o2"), <<"b">>}, {?K("o3"), <<"c">>}], lists:sort(Rows)),

        %% 受け取ったら空になる
        ?assertMatch({delta, _, []}, tether:sync(<<"alice">>))
    end).

%% 削除も差分に出る
delete_appears_in_delta_test() ->
    with_db(fun(_) ->
        ok = put(<<"shop">>, 1, ?K("o1"), <<"a">>),
        {ok, _, _} = tether:subscribe(<<"alice">>, <<"orders">>),
        {ok, [ok]} = tether:request(<<"shop">>, 2, [{delete, ?K("o1")}]),
        wait(<<"alice">>, 2),
        ?assertMatch({delta, _, [{{<<"orders">>, <<"o1">>}, deleted}]},
                     tether:sync(<<"alice">>))
    end).

%%%===================================================================
%%% ここが本題 — 圏外の間も溜まり続ける
%%%===================================================================

%% クライアントは居ないが、セッションプロセスは生きていて記録し続ける。
%% 戻ってきたときに、**見落とした変更だけ**が渡る。
accumulates_while_offline_test() ->
    with_db(fun(_) ->
        {ok, _, []} = tether:subscribe(<<"alice">>, <<"orders">>),

        %% --- alice は圏外。300件の変更が起きる ---
        _ = [put(<<"shop">>, I, {<<"orders">>, <<I:32>>}, <<I:32>>)
             || I <- lists:seq(1, 300)],
        wait(<<"alice">>, 300),
        ?assertMatch(#{pending := 300, overflow := false},
                     tether:session_info(<<"alice">>)),

        %% --- 復帰。300件だけが返る ---
        {delta, _, Rows} = tether:sync(<<"alice">>),
        ?assertEqual(300, length(Rows))
    end).

%% 購読していないコレクションは配られない。
%% 内部用の鍵($pool など)も外に出ない。
only_subscribed_collections_test() ->
    with_db(fun(_) ->
        {ok, _, _} = tether:subscribe(<<"alice">>, <<"orders">>),
        ok = put(<<"shop">>, 1, ?K("o1"), <<"a">>),
        ok = put(<<"shop">>, 2, {<<"cart">>, <<"c1">>}, <<"x">>),
        {ok, _} = tether:stock(<<"sku:1">>, 100),          % $pool を書く
        wait(<<"alice">>, 1),
        timer:sleep(30),
        {delta, _, Rows} = tether:sync(<<"alice">>),
        ?assertEqual([{?K("o1"), <<"a">>}], Rows)
    end).

%%%===================================================================
%%% 溜めきれなくなったら、そのクライアントだけが取り直す
%%%===================================================================

overflow_falls_back_to_resync_test() ->
    with_db(#{sync_backlog => 50}, fun(_) ->
        {ok, _, []} = tether:subscribe(<<"alice">>, <<"orders">>),
        {ok, _, []} = tether:subscribe(<<"bob">>,   <<"orders">>),

        %% alice は圏外のまま。bob はこまめに受け取る
        _ = [begin
                 put(<<"shop">>, I, {<<"orders">>, <<I:32>>}, <<I:32>>),
                 case I rem 10 of 0 -> tether:sync(<<"bob">>); _ -> ok end
             end || I <- lists:seq(1, 200)],
        wait(<<"alice">>, 200),
        wait(<<"bob">>, 200),

        %% alice は溜めきれなかった
        ?assertMatch(#{overflow := true}, tether:session_info(<<"alice">>)),
        {resync, _, [{<<"orders">>, Rows}]} = tether:sync(<<"alice">>),
        ?assertEqual(200, length(Rows)),

        %% **bob は無傷。** 差分で追いつけている
        ?assertMatch(#{overflow := false}, tether:session_info(<<"bob">>)),
        ?assertMatch({delta, _, _}, tether:sync(<<"bob">>)),

        %% 取り直した後は、また差分に戻る
        ok = put(<<"shop">>, 201, ?K("after"), <<"z">>),
        wait(<<"alice">>, 201),
        ?assertMatch({delta, _, [{{<<"orders">>, <<"after">>}, <<"z">>}]},
                     tether:sync(<<"alice">>))
    end).

%%%===================================================================
%%% 扇形配信 — BEAM の出番
%%%===================================================================

%% 1件の書き込みが、購読している多数のセッションへ届く。
%% 配布の費用は全セッション数ではなく、**そのコレクションの購読者数**に比例する。
fanout_test_() ->
    {timeout, 60, fun() ->
        with_db(fun(_) ->
            N = 2000,
            Subs = [<<"c", I:32>> || I <- lists:seq(1, N)],
            _ = [{ok, _, _} = tether:subscribe(C, <<"orders">>) || C <- Subs],
            %% 購読していない側
            {ok, _, _} = tether:subscribe(<<"other">>, <<"cart">>),

            T0 = erlang:monotonic_time(microsecond),
            ok = put(<<"shop">>, 1, ?K("o1"), <<"a">>),
            _ = [wait(C, 1) || C <- Subs],
            T1 = erlang:monotonic_time(microsecond),
            ?debugFmt("~p 購読者への配布: ~p µs", [N, T1 - T0]),

            _ = [?assertMatch({delta, _, [{{<<"orders">>, <<"o1">>}, <<"a">>}]},
                              tether:sync(C)) || C <- Subs],
            %% 別のコレクションの購読者には届いていない
            ?assertMatch({delta, _, []}, tether:sync(<<"other">>))
        end)
    end}.

%% セッションが死んだら購読も消える。残すと配布先が増え続ける。
dead_session_is_unsubscribed_test() ->
    with_db(fun(_) ->
        {ok, _, _} = tether:subscribe(<<"alice">>, <<"orders">>),
        ?assertEqual(1, length(tether_sessions:subscribers(<<"orders">>))),
        {ok, Pid} = tether_sessions:lookup(<<"alice">>),
        Ref = erlang:monitor(process, Pid),
        exit(Pid, kill),
        receive {'DOWN', Ref, _, _, _} -> ok after 1000 -> error(timeout) end,
        timer:sleep(50),
        ?assertEqual([], tether_sessions:subscribers(<<"orders">>))
    end).
