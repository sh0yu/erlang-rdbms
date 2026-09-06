%%%-------------------------------------------------------------------
%%% @doc
%%% 教材の中身が食い違っていないことを確かめる。
%%%
%%% 手順ごとの状態、判定、良し悪しの印が、すべて同じ方式の集合を
%%% 指していること。ここがずれると、画面に空欄や幽霊の列が出る。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_lab_tests).
-include_lib("eunit/include/eunit.hrl").

ids_are_unique_test() ->
    Ids = tether_lab:ids(),
    ?assertEqual(lists:usort(Ids), lists:sort(Ids)),
    ?assert(length(Ids) >= 5).

%% どの現象も、全手順・判定・印が同じ方式の集合を指していること
consistent_policies_test() ->
    [begin
         Keys = lists:sort([K || #{key := K} <- maps:get(policies, P)]),
         Id   = maps:get(id, P),
         ?assert(length(Keys) >= 2),
         [?assertEqual({Id, Keys}, {Id, lists:sort(maps:keys(St))})
          || #{state := St} <- maps:get(steps, P)],
         ?assertEqual({Id, Keys}, {Id, lists:sort(maps:keys(maps:get(verdict, P)))}),
         [?assert(lists:member(G, Keys)) || G <- maps:get(good, P)]
     end || P <- tether_lab:all()].

%% 良い方式と悪い方式が両方あること。片方しか無い比較には意味がない。
has_both_sides_test() ->
    [begin
         Keys = [K || #{key := K} <- maps:get(policies, P)],
         Good = maps:get(good, P),
         ?assert(length(Good) >= 1),
         ?assert(length(Keys) > length(Good))
     end || P <- tether_lab:all()].

%% 日本語が UTF-8 として正しいこと。
%% <<"日本語">> は1文字1バイトに切り詰められる(/utf8 が要る)。実際に踏んだ。
text_is_utf8_test() ->
    [begin
         [?assertNotEqual(error, unicode:characters_to_list(B))
          || B <- texts(P)]
     end || P <- tether_lab:all()].

texts(P) ->
    [maps:get(title, P), maps:get(why, P)]
        ++ [maps:get(say, S) || S <- maps:get(steps, P)]
        ++ maps:values(maps:get(verdict, P))
        ++ [N || #{name := N} <- maps:get(policies, P)].

%% 中心の現象: CRDT は破り、CAS と escrow は守る
stock_floor_outcome_test() ->
    P = tether_lab:get(<<"floor">>),
    ?assertEqual([<<"cas">>, <<"escrow">>], lists:sort(maps:get(good, P))),
    #{state := Last} = lists:last(maps:get(steps, P)),
    %% CRDT の結末にはマイナスが出ている
    ?assertMatch({_, _}, binary:match(maps:get(<<"crdt">>, Last), <<"-3">>)).

%% 再送: 素朴な実装は2回引く
retry_outcome_test() ->
    P = tether_lab:get(<<"retry">>),
    #{state := Last} = lists:last(maps:get(steps, P)),
    ?assertEqual(<<"98">>, maps:get(<<"naive">>, Last)),
    ?assertEqual(<<"99">>, maps:get(<<"session">>, Last)).
