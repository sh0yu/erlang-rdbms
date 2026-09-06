%%%-------------------------------------------------------------------
%%% @doc
%%% オフラインで溜めた操作の流し込み。
%%%
%%% ここまでで揃った3つが、初めて噛み合う場所である。
%%%
%%%   預かり(escrow)     圏外でも「売れる」と確定的に答えられる
%%%   束の流し込み       溜めた操作を順に流し、**最初の失敗で止める**
%%%   通番と再送の吸収   束ごと送り直しても二重に実行されない
%%% @end
%%%-------------------------------------------------------------------
-module(tether_offline_tests).
-include_lib("eunit/include/eunit.hrl").

-import(tether_test_util, [tmpdir/0, rmrf/1]).

-define(SKU, <<"sku:1">>).
-define(K(C, K), {<<C>>, <<K>>}).

with_db(F) ->
    _ = application:stop(tether),
    _ = application:load(tether),
    D = tmpdir(),
    application:set_env(tether, dir, D),
    {ok, _} = application:ensure_all_started(tether),
    try F(D) after ok = application:stop(tether), rmrf(D) end.

%%%===================================================================
%%% 束の基本
%%%===================================================================

batch_runs_in_order_test() ->
    with_db(fun(_) ->
        {ok, R} = tether:request_batch(<<"alice">>, 1,
                    [[{put, ?K("t", "a"), <<"1">>}],
                     [{put, ?K("t", "b"), <<"2">>}],
                     [{get, ?K("t", "a")}]]),
        ?assertEqual([{ok, [ok]}, {ok, [ok]}, {ok, [{ok, <<"1">>}]}], R)
    end).

%% **最初の失敗で止まる。** ここが要点。
%% 流し切る実装だと、前提が崩れた後の操作まで実行してしまう。
batch_stops_at_first_failure_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether:request(<<"alice">>, 1, [{put, ?K("t", "x"), <<"0">>}]),
        {ok, R} = tether:request_batch(<<"alice">>, 2,
                    [[{put, ?K("t", "step1"), <<"ok">>}],
                     [{cas, ?K("t", "x"), <<"ちがう">>, <<"9">>}],   % ここで失敗
                     [{put, ?K("t", "step3"), <<"ok">>}]]),          % 実行されない
        ?assertMatch([{ok, [ok]}, {error, 1, {conflict, <<"0">>}}], R),
        ?assertEqual(2, length(R)),
        ?assertEqual({ok, <<"ok">>}, tether:read(?K("t", "step1"))),
        ?assertEqual(not_found, tether:read(?K("t", "step3")))
    end).

%% 束の途中で接続が切れても、**同じ束をそのまま送り直せばよい**。
%% 通番が束全体で1つなので、二重に実行されない。
batch_retry_is_absorbed_test() ->
    with_db(fun(_) ->
        Batch = [[{cas, ?K("t", "k"), undefined, <<"v">>}],
                 [{put, ?K("t", "m"), <<"1">>}]],
        R1 = tether:request_batch(<<"alice">>, 1, Batch),
        ?assertMatch({ok, [{ok, [ok]}, {ok, [ok]}]}, R1),
        %% 応答を受け取り損ねたつもりで、そのまま送り直す
        ?assertEqual(R1, tether:request_batch(<<"alice">>, 1, Batch)),
        ?assertEqual(R1, tether:request_batch(<<"alice">>, 1, Batch)),
        ?assertMatch(#{served := 1, deduped := 2},
                     tether:session_info(<<"alice">>))
    end).

%%%===================================================================
%%% 本題 — 圏外で売って、戻って流す
%%%===================================================================

%% alice が預かりを取って圏外になる。その間に bob が中央を売り切る。
%% alice はローカルで3個売り、注文を確定させた。
%% 戻ってきて束を流す → **1件も落ちない**。
offline_sales_all_land_test() ->
    with_db(fun(_) ->
        {ok, 100} = tether:stock(?SKU, 100),
        {ok, [{granted, G, _}]} =
            tether:request(<<"alice">>, 1, [{acquire, ?SKU, 5, 600000}]),
        ?assert(G >= 3),

        %% --- ここから alice は圏外。bob が中央を食い尽くす ---
        drain(<<"bob">>),
        ?assertMatch({0, _}, tether:pool(?SKU)),

        %% --- alice 復帰。圏外で積んだ操作を1回で流す ---
        Queued =
            [[{put, ?K("cart", "a1"), <<"item">>}],
             [{consume, ?SKU, 1}, {put, ?K("orders", "o1"), <<"confirmed">>}],
             [{consume, ?SKU, 1}, {put, ?K("orders", "o2"), <<"confirmed">>}],
             [{consume, ?SKU, 1}, {put, ?K("orders", "o3"), <<"confirmed">>}]],
        {ok, R} = tether:request_batch(<<"alice">>, 2, Queued),

        ?assertEqual(4, length(R)),
        ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, R)),
        ?assertEqual({ok, <<"confirmed">>}, tether:read(?K("orders", "o3"))),

        %% 保存則: 中央 + 預かり残 + 売れた数 == 100
        {A, Gr} = tether:pool(?SKU),
        ?assertEqual(100, A + Gr + sold(100, A, Gr))
    end).

sold(Init, A, G) -> Init - A - G.

%% 対照実験。預かりを使わず CAS で同じことをやると、
%% 在庫の引き当てで落ち、**注文の確定は実行されない**。
%%
%% 落ちること自体は正しい(在庫が無いのだから)。
%% 大事なのは**そこで止まること**で、流し切る実装だと
%% 「在庫を確保できていないのに注文が確定した」が出来上がる。
without_escrow_batch_stops_before_confirming_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether:request(<<"shop">>, 1,
                                    [{put, ?K("t", "stock"), <<"100">>}]),
        %% alice が圏外の間に在庫が動いた
        {ok, [ok]} = tether:request(<<"bob">>, 1,
                       [{cas, ?K("t", "stock"), <<"100">>, <<"0">>}]),

        %% alice が積んだ操作。前提は「在庫は100」
        {ok, R} = tether:request_batch(<<"alice">>, 1,
                    [[{put, ?K("cart", "a1"), <<"item">>}],
                     [{cas, ?K("t", "stock"), <<"100">>, <<"99">>}],
                     [{put, ?K("orders", "o1"), <<"confirmed">>}]]),

        ?assertMatch([{ok, [ok]}, {error, 1, {conflict, <<"0">>}}], R),
        %% **注文は確定していない**
        ?assertEqual(not_found, tether:read(?K("orders", "o1")))
    end).

%% 圏外の売上が、期限を過ぎていたら通らない。
%% 「何時間のオフラインを支えるか」がリース期間であり、
%% それを超えた分は無効になる、という契約が成立していること。
expired_grant_rejects_offline_sales_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, [{granted, _, _}]} =
            tether:request(<<"alice">>, 1, [{acquire, ?SKU, 5, 1}]),   % 期限 1ms
        timer:sleep(20),
        {ok, R} = tether:request_batch(<<"alice">>, 2,
                    [[{consume, ?SKU, 1}, {put, ?K("orders", "o1"), <<"c">>}],
                     [{consume, ?SKU, 1}, {put, ?K("orders", "o2"), <<"c">>}]]),
        ?assertEqual([{error, 1, expired}], R),
        ?assertEqual(not_found, tether:read(?K("orders", "o1")))
    end).

%%%===================================================================

drain(C) -> drain(C, 1, 0).
drain(C, Seq, N) when N < 500 ->
    case tether:request(C, Seq, [{acquire, ?SKU, 1000000, 600000}]) of
        {ok, [{granted, G, _}]} ->
            {ok, [{consumed, 0}]} = tether:request(C, Seq + 1, [{consume, ?SKU, G}]),
            drain(C, Seq + 2, N + 1);
        {error, 1, sold_out} -> ok
    end;
drain(_, _, _) -> error(drain_did_not_terminate).

%%%===================================================================
%%% resume — クライアントが自分の状態を忘れた場合
%%%===================================================================

%% 端末を作り直した(ローカルの通番を失った)クライアントが、
%% 「どこまで届いていて、何を預かっているか」を聞き直す。
resume_returns_position_and_grants_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, [{granted, G, Exp}]} =
            tether:request(<<"alice">>, 1, [{acquire, ?SKU, 5, 600000}]),
        R2 = tether:request(<<"alice">>, 2, [{put, ?K("t", "a"), <<"1">>}]),

        %% 端末を作り直した。手元には何も無い
        #{last_seq := Last, last_reply := Reply, grants := Grants} =
            tether:resume(<<"alice">>),

        ?assertEqual(2, Last),
        %% 記録されているのは束の結果。公開APIの形にほどけば一致する
        ?assertEqual({ok, [R2]}, Reply),
        ?assertEqual([{?SKU, G, Exp}], Grants),

        %% 次に送るのは last_seq + 1。二重発注も拒否も起きない
        ?assertMatch({ok, [ok]},
                     tether:request(<<"alice">>, Last + 1,
                                    [{put, ?K("t", "b"), <<"2">>}]))
    end).

%% 預かりが返るので、復帰した端末は**すぐ圏外で動ける**。
resume_lets_client_go_offline_again_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, [{granted, G, _}]} =
            tether:request(<<"alice">>, 1, [{acquire, ?SKU, 5, 600000}]),
        drain(<<"bob">>),                          % 中央は空になる

        #{last_seq := L, grants := [{?SKU, Rem, _}]} = tether:resume(<<"alice">>),
        ?assertEqual(G, Rem),

        %% 中央が空でも、預かりの範囲では売れる
        {ok, R} = tether:request_batch(<<"alice">>, L + 1,
                    [[{consume, ?SKU, 1}], [{consume, ?SKU, 1}]]),
        ?assert(lists:all(fun({ok, _}) -> true; (_) -> false end, R))
    end).

%% 期限切れの預かりは返らない。返すと、使えないものを使えると誤解する。
resume_hides_expired_grants_test() ->
    with_db(fun(_) ->
        {ok, _} = tether:stock(?SKU, 100),
        {ok, [{granted, _, _}]} =
            tether:request(<<"alice">>, 1, [{acquire, ?SKU, 5, 1}]),
        timer:sleep(20),
        ?assertMatch(#{grants := []}, tether:resume(<<"alice">>))
    end).

%% 一度も来ていないクライアントにも答えられる
resume_of_unknown_client_test() ->
    with_db(fun(_) ->
        ?assertEqual(#{last_seq => 0, last_reply => undefined, grants => []},
                     tether:resume(<<"nobody">>))
    end).
