%%%-------------------------------------------------------------------
%%% @doc
%%% クライアント側。**圏外を本当に切って試す。**
%%%
%%% 切った状態で読み書きが通らなければ local-first ではない。
%%% offline/1 は本当に通信を止める(サーバへ一切呼ばない)。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_client_tests).
-include_lib("eunit/include/eunit.hrl").

-import(tether_test_util, [tmpdir/0, rmrf/1]).

-define(O(K), {<<"orders">>, <<K>>}).
-define(SKU, <<"sku:1">>).

with_db(F) ->
    _ = application:stop(tether),
    _ = application:load(tether),
    D = tmpdir(),
    application:set_env(tether, dir, D),
    {ok, _} = application:ensure_all_started(tether),
    try F(D) after ok = application:stop(tether), rmrf(D) end.

shop(Seq, K, V) ->
    {ok, [ok]} = tether:request(<<"shop">>, Seq, [{put, K, V}]), ok.

%%%===================================================================
%%% 圏外で読む・書く
%%%===================================================================

reads_locally_while_offline_test() ->
    with_db(fun(_) ->
        ok = shop(1, ?O("o1"), <<"a">>),
        {ok, C} = tether_client:open(<<"alice">>, <<"orders">>),
        ok = tether_client:offline(C),

        %% サーバへ一切問い合わせずに読める
        ?assertEqual({ok, <<"a">>}, tether_client:read(C, ?O("o1"))),
        ?assertEqual(not_found, tether_client:read(C, ?O("nope"))),
        tether_client:close(C)
    end).

writes_locally_while_offline_test() ->
    with_db(fun(_) ->
        {ok, C} = tether_client:open(<<"alice">>, <<"orders">>),
        ok = tether_client:offline(C),

        {ok, [ok]} = tether_client:write(C, [{put, ?O("draft"), <<"x">>}]),
        %% 自分の書き込みがすぐ見える(read-your-writes)
        ?assertEqual({ok, <<"x">>}, tether_client:read(C, ?O("draft"))),
        ?assertMatch(#{queued := 1}, tether_client:state(C)),
        %% サーバにはまだ無い
        ?assertEqual(not_found, tether:read(?O("draft"))),

        %% 復帰して送る
        ok = tether_client:online(C),
        {ok, #{accepted := 1, rejected := none}} = tether_client:sync(C),
        ?assertEqual({ok, <<"x">>}, tether:read(?O("draft"))),
        ?assertMatch(#{queued := 0}, tether_client:state(C)),
        tether_client:close(C)
    end).

%% 圏外の間にサーバ側で起きた変更は、復帰時の差分で届く。
receives_changes_missed_while_offline_test() ->
    with_db(fun(_) ->
        {ok, C} = tether_client:open(<<"alice">>, <<"orders">>),
        ok = tether_client:offline(C),

        ok = shop(1, ?O("o1"), <<"from-server">>),
        ok = shop(2, ?O("o2"), <<"also">>),
        %% 圏外なので当然まだ見えない
        ?assertEqual(not_found, tether_client:read(C, ?O("o1"))),

        ok = tether_client:online(C),
        {ok, _} = tether_client:sync(C),
        ?assertEqual({ok, <<"from-server">>}, tether_client:read(C, ?O("o1"))),
        ?assertEqual({ok, <<"also">>}, tether_client:read(C, ?O("o2"))),
        tether_client:close(C)
    end).

%%%===================================================================
%%% 楽観的なものと、確定的なもの
%%%===================================================================

%% 圏外の put は**推測**。サーバが拒めば覆る。
%% そのとき、落ちた位置から後ろは実行されず、呼び出し側に返る。
optimistic_write_can_be_rejected_test() ->
    with_db(fun(_) ->
        ok = shop(1, ?O("x"), <<"0">>),
        {ok, C} = tether_client:open(<<"alice">>, <<"orders">>),
        ok = tether_client:offline(C),

        {ok, [ok]} = tether_client:write(C, [{put, ?O("a"), <<"1">>}]),
        {ok, [ok]} = tether_client:write(C, [{cas, ?O("x"), <<"0">>, <<"9">>}]),
        {ok, [ok]} = tether_client:write(C, [{put, ?O("b"), <<"2">>}]),

        %% 圏外の間に、別の誰かが x を動かした
        {ok, [ok]} = tether:request(<<"other">>, 1,
                                    [{cas, ?O("x"), <<"0">>, <<"7">>}]),

        ok = tether_client:online(C),
        {ok, Out} = tether_client:sync(C),
        ?assertMatch(#{accepted := 1, rejected := {2, {1, {conflict, <<"7">>}}}},
                     Out),
        %% **3番目は実行されず、呼び出し側に返る**
        ?assertEqual([[{put, ?O("b"), <<"2">>}]], maps:get(unsent, Out)),
        ?assertEqual({ok, <<"1">>}, tether:read(?O("a"))),
        ?assertEqual(not_found, tether:read(?O("b"))),
        %% 覆ったので、手元にも x の真値が来ている
        ?assertEqual({ok, <<"7">>}, tether_client:read(C, ?O("x"))),
        tether_client:close(C)
    end).

%% 圏外の consume は**確定的**。預かりの範囲なので覆らない。
authoritative_offline_consume_test() ->
    with_db(fun(_) ->
        {ok, 100} = tether:stock(?SKU, 100),
        {ok, C} = tether_client:open(<<"alice">>, <<"orders">>),
        {ok, G} = tether_client:acquire(C, ?SKU, 4, 600000),
        ?assert(G >= 2),

        ok = tether_client:offline(C),
        %% 圏外で、中央が空でも売れると**断言できる**
        {ok, [{consumed, _}, ok]} =
            tether_client:write(C, [{consume, ?SKU, 1},
                                    {put, ?O("o1"), <<"confirmed">>}]),
        {ok, [{consumed, _}, ok]} =
            tether_client:write(C, [{consume, ?SKU, 1},
                                    {put, ?O("o2"), <<"confirmed">>}]),
        ?assertMatch(#{grants := [{?SKU, N}]} when N =:= G - 2,
                     tether_client:state(C)),

        %% 圏外の間に、他のクライアントが中央を空にする
        drain(<<"bob">>),
        ?assertMatch({0, _}, tether:pool(?SKU)),

        %% 復帰。**1件も落ちない**
        ok = tether_client:online(C),
        {ok, #{accepted := 2, rejected := none}} = tether_client:sync(C),
        ?assertEqual({ok, <<"confirmed">>}, tether:read(?O("o2"))),
        tether_client:close(C)
    end).

%% 預かりを超える consume は、**サーバに聞かずにその場で断れる**。
%% 送信待ちにも積まれない。
over_grant_is_refused_locally_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, C} = tether_client:open(<<"alice">>, <<"orders">>),
        {ok, G} = tether_client:acquire(C, ?SKU, 4, 600000),
        ok = tether_client:offline(C),

        ?assertMatch({error, 1, {insufficient, G}},
                     tether_client:write(C, [{consume, ?SKU, G + 1}])),
        ?assertMatch(#{queued := 0}, tether_client:state(C)),
        tether_client:close(C)
    end).

%% 権利を取るのは協調そのもの。圏外ではできない。
acquire_needs_network_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, C} = tether_client:open(<<"alice">>, <<"orders">>),
        ok = tether_client:offline(C),
        ?assertEqual({error, offline}, tether_client:acquire(C, ?SKU, 4, 600000)),
        ?assertEqual({error, offline}, tether_client:sync(C)),
        tether_client:close(C)
    end).

%%%===================================================================
%%% 送信待ちは、送り終えるまで消えない
%%%===================================================================

%% sync に失敗しても書き込みは残る。復帰すればそのまま送られ、
%% **二重には実行されない**(通番が進んでいないため)。
failed_sync_keeps_queue_test() ->
    with_db(fun(_) ->
        {ok, C} = tether_client:open(<<"alice">>, <<"orders">>),
        ok = tether_client:offline(C),
        {ok, [ok]} = tether_client:write(C, [{cas, ?O("k"), undefined, <<"v">>}]),

        ?assertEqual({error, offline}, tether_client:sync(C)),
        ?assertMatch(#{queued := 1}, tether_client:state(C)),

        ok = tether_client:online(C),
        {ok, #{accepted := 1}} = tether_client:sync(C),
        %% もう一度 sync しても何も起きない
        {ok, #{accepted := 0}} = tether_client:sync(C),
        ?assertEqual({ok, <<"v">>}, tether:read(?O("k"))),
        tether_client:close(C)
    end).

%%%===================================================================

drain(C) -> drain(C, 1, 0).
drain(C, Seq, N) when N < 500 ->
    case tether:request(C, Seq, [{acquire, ?SKU, 1000000, 600000}]) of
        {ok, [{granted, G, _}]} ->
            {ok, [{consumed, 0}]} = tether:request(C, Seq+1, [{consume, ?SKU, G}]),
            drain(C, Seq + 2, N + 1);
        {error, 1, sold_out} -> ok
    end;
drain(_, _, _) -> error(drain_did_not_terminate).

%% 送信待ちがあるうちは acquire させない。
%% 内部で黙って sync すると、拒否された書き込みが握り潰される。
acquire_refuses_while_queued_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, C} = tether_client:open(<<"alice">>, <<"orders">>),
        {ok, [ok]} = tether_client:write(C, [{put, ?O("d"), <<"1">>}]),
        ?assertEqual({error, {sync_first, 1}},
                     tether_client:acquire(C, ?SKU, 4, 600000)),
        {ok, _} = tether_client:sync(C),
        ?assertMatch({ok, _}, tether_client:acquire(C, ?SKU, 4, 600000)),
        tether_client:close(C)
    end).
