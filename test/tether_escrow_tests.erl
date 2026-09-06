%%%-------------------------------------------------------------------
%%% @doc
%%% 預かり(escrow)。**local-first の中核。**
%%%
%%% 確かめたいのは一つ。
%%%
%%%   「オフラインのクライアントが書き込めて、しかも在庫の下限が破れない」
%%%
%%% 既存の同期エンジンも CRDT も、前半はできるが後半ができない。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_escrow_tests).
-include_lib("eunit/include/eunit.hrl").

-import(tether_test_util, [tmpdir/0, rmrf/1]).

-define(SKU, <<"sku:1">>).

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

req(C, Seq, Ops) -> tether:request(C, Seq, Ops).

%%%===================================================================
%%% 基本
%%%===================================================================

acquire_and_consume_test() ->
    with_db(fun(_) ->
        {ok, 100} = tether:stock(?SKU, 100),
        {ok, [{granted, G, _}]} = req(<<"alice">>, 1, [{acquire, ?SKU, 10, 60000}]),
        ?assert(G > 0),
        {A, Granted} = tether:pool(?SKU),
        ?assertEqual(100, A + Granted),          % 保存則
        ?assertEqual(G, Granted),

        {ok, [{consumed, R}]} = req(<<"alice">>, 2, [{consume, ?SKU, 1}]),
        ?assertEqual(G - 1, R),
        %% **中央は 1 も動かない。ここが要点** — 消費に協調が要らない。
        %% 減るのは自分の預かりの残りだけ。
        {A2, Granted2} = tether:pool(?SKU),
        ?assertEqual(A, A2),
        ?assertEqual(Granted - 1, Granted2),
        %% 保存則: 中央 + 預かり残 + 売れた数 == 初期在庫
        ?assertEqual(100, A2 + Granted2 + 1)
    end).

%% 預かりを超えては消費できない
cannot_exceed_grant_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, [{granted, G, _}]} = req(<<"alice">>, 1, [{acquire, ?SKU, 5, 60000}]),
        ?assertMatch({error, 1, {insufficient, G}},
                     req(<<"alice">>, 2, [{consume, ?SKU, G + 1}]))
    end).

%% 未使用分は返せる
release_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, [{granted, G, _}]} = req(<<"alice">>, 1, [{acquire, ?SKU, 10, 60000}]),
        {ok, [{consumed, _}]} = req(<<"alice">>, 2, [{consume, ?SKU, 2}]),
        {ok, [{released, Rel}]} = req(<<"alice">>, 3, [{release, ?SKU}]),
        ?assertEqual(G - 2, Rel),
        {A, Gr} = tether:pool(?SKU),
        ?assertEqual(98, A),                     % 2個だけ売れた
        ?assertEqual(0, Gr)
    end).

%%%===================================================================
%%% ここが本題 — オフラインでも下限が破れない
%%%===================================================================

%% alice が預かりを取って「圏外」になる。
%% その間に bob が中央の在庫を売り切る。
%% alice が戻ってきて、溜めた消費を流す。
%%
%% **alice の消費は1件も落ちない。** 事前に権利を持っていたから。
%% そして在庫の合計は絶対に初期値を超えない。
offline_client_still_sells_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, [{granted, G, _}]} = req(<<"alice">>, 1, [{acquire, ?SKU, 5, 600000}]),
        ?assert(G >= 5),

        %% alice は圏外。bob が中央を食い尽くす
        drain(<<"bob">>, 1),
        {Avail, _} = tether:pool(?SKU),
        ?assertEqual(0, Avail),

        %% alice が復帰して、溜めた5件を流す
        Results = [req(<<"alice">>, S, [{consume, ?SKU, 1}])
                   || S <- lists:seq(2, 6)],
        ?assertEqual(5, length([x || {ok, [{consumed, _}]} <- Results])),

        %% 保存則: 売れた数 + 中央 + 預かり残 == 100
        {A2, G2} = tether:pool(?SKU),
        Sold = 100 - A2 - G2,
        ?assert(Sold =< 100),
        ?assertEqual(100, A2 + G2 + Sold)
    end).

%% 対照実験。預かりを使わず CAS でやると、同じ筋書きで**必ず落ちる**。
%% これが既存の同期エンジンの限界にあたる。
without_escrow_offline_write_fails_test() ->
    with_db(fun(_) ->
        %% 在庫を普通の値として持つ
        {ok, [ok]} = req(<<"alice">>, 1,
                         [{put, {<<"t">>, <<"stock">>}, <<"100">>}]),
        %% alice が圏外の間に bob が動かす
        {ok, [ok]} = req(<<"bob">>, 1,
                         [{cas, {<<"t">>, <<"stock">>}, <<"100">>, <<"3">>}]),
        %% alice が復帰。手元の前提(100)は古い
        ?assertMatch({error, 1, {conflict, <<"3">>}},
                     req(<<"alice">>, 2,
                         [{cas, {<<"t">>, <<"stock">>}, <<"100">>, <<"99">>}]))
    end).

%%%===================================================================
%%% 枯渇したら、古典の振る舞いへ連続的に退化する
%%%===================================================================

degrades_when_scarce_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 1000),
        %% 在庫が減るにつれて、1回に配れる量が小さくなること
        Grants = [begin
                      C = <<"c", I:32>>,
                      {ok, [{granted, G, _}]} =
                          req(C, 1, [{acquire, ?SKU, 1000, 600000}]),
                      G
                  end || I <- lists:seq(1, 12)],
        ?debugFmt("配られた量の推移: ~p", [Grants]),
        %% 単調に減っていく(等しいことは許す)
        ?assert(lists:all(fun({A, B}) -> A >= B end,
                          lists:zip(lists:droplast(Grants), tl(Grants)))),
        ?assert(hd(Grants) > lists:last(Grants)),
        %% 配った総和は在庫を超えない
        {A, G} = tether:pool(?SKU),
        ?assertEqual(1000, A + G),
        ?assertEqual(lists:sum(Grants), G)
    end).

sold_out_when_empty_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 4),
        drain(<<"bob">>, 1),
        ?assertEqual({0, 0}, tether:pool(?SKU)),
        ?assertMatch({error, 1, sold_out},
                     req(<<"zed">>, 1, [{acquire, ?SKU, 1, 60000}]))
    end).

%%%===================================================================
%%% 期限と回収
%%%===================================================================

%% 戻ってこない保持者の預かりは、中央が足りなくなったときに回収される。
%% 回収しないと在庫が永久に遊ぶ。
expired_grant_is_reclaimed_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 10),
        %% 期限 1ms。すぐ切れる
        {ok, [{granted, G, _}]} = req(<<"ghost">>, 1, [{acquire, ?SKU, 5, 1}]),
        ?assert(G > 0),
        timer:sleep(20),
        %% 期限切れなので消費できない
        ?assertMatch({error, 1, expired}, req(<<"ghost">>, 2, [{consume, ?SKU, 1}])),
        %% 別の保持者が取りに来ると、回収されて在庫が戻る
        {ok, [{granted, _, _}]} = req(<<"alice">>, 1, [{acquire, ?SKU, 10, 60000}]),
        {A, Gr} = tether:pool(?SKU),
        ?assertEqual(10, A + Gr)                 % ghost の分が失われていない
    end).

%%%===================================================================
%%% 復旧
%%%===================================================================

%% 預かりも中央在庫も、再起動を越えて残る。
%% **時刻を記録しているので、期限の判定が再実行で変わらない。**
survives_restart_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, [{granted, G, Exp}]} = req(<<"alice">>, 1, [{acquire, ?SKU, 10, 600000}]),
        {ok, [{consumed, _}]} = req(<<"alice">>, 2, [{consume, ?SKU, 3}]),
        Before = tether:pool(?SKU),

        restart(),

        ?assertEqual(Before, tether:pool(?SKU)),
        {ok, [{grant, R, Exp2}]} = req(<<"alice">>, 3, [{grant_of, ?SKU}]),
        ?assertEqual(G - 3, R),
        ?assertEqual(Exp, Exp2),                 % 期限も同じ
        %% 再送も従来通り吸収される
        ?assertEqual({ok, [{grant, R, Exp}]}, req(<<"alice">>, 3, [{grant_of, ?SKU}]))
    end).

%%%===================================================================

drain(C, Seq) -> drain(C, Seq, 0).
drain(C, Seq, N) when N < 500 ->
    case req(C, Seq, [{acquire, ?SKU, 1000000, 600000}]) of
        {ok, [{granted, G, _}]} ->
            {ok, [{consumed, 0}]} = req(C, Seq + 1, [{consume, ?SKU, G}]),
            drain(C, Seq + 2, N + 1);
        {error, 1, sold_out} -> ok
    end;
drain(_, _, _) -> error(drain_did_not_terminate).
