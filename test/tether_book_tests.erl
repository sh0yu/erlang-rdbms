-module(tether_book_tests).
-include_lib("eunit/include/eunit.hrl").

%% 章と項目があること
structure_test() ->
    Chs = tether_book:chapters(),
    ?assert(length(Chs) >= 6),
    [begin
         ?assert(is_binary(maps:get(title, C))),
         ?assert(length(maps:get(items, C)) >= 1)
     end || C <- Chs].

ids_unique_test() ->
    Ids = [maps:get(id, I) || I <- tether_book:flat()],
    ?assertEqual(lists:usort(Ids), lists:sort(Ids)).

%% 形式ごとに、必要な欄が揃っていること
shape_test() ->
    [begin
         Id = maps:get(id, I),
         case maps:get(kind, I) of
             <<"timeline">> -> ?assert(length(maps:get(rows, I)) >= 2, Id);
             <<"compare">>  -> ?assert(length(maps:get(policies, I)) >= 2, Id),
                               ?assert(length(maps:get(steps, I)) >= 2, Id);
             <<"matrix">>   -> Cols = length(maps:get(cols, I)),
                               [?assertEqual({Id, Cols}, {Id, length(maps:get(cells, R))})
                                || R <- maps:get(rows, I)];
             <<"notes">>    -> ?assert(length(maps:get(sections, I)) >= 1, Id);
             <<"flow">>     -> check_flow(I)
         end
     end || I <- tether_book:flat()].

%% 選び方の木: 全ての枝の飛び先が実在し、全ての節へ到達できること
check_flow(I) ->
    Nodes = maps:get(nodes, I),
    Ids   = [maps:get(id, N) || N <- Nodes],
    ?assert(lists:member(maps:get(start, I), Ids)),
    Gotos = [maps:get(goto, O) || N <- Nodes, O <- maps:get(opts, N, [])],
    [?assert(lists:member(G, Ids), G) || G <- Gotos],
    %% 到達できない節が無いこと(始点を除く)
    Unreachable = [X || X <- Ids, X =/= maps:get(start, I),
                        not lists:member(X, Gotos)],
    ?assertEqual([], Unreachable),
    %% 答えの節には picks があること
    [?assert(length(maps:get(picks, N)) >= 1, maps:get(id, N))
     || N <- Nodes, maps:is_key(answer, N)].

%% 関連リンクの飛び先が実在すること
seealso_resolves_test() ->
    Ids = [maps:get(id, I) || I <- tether_book:flat()],
    [[?assert(lists:member(S, Ids), S) || S <- maps:get(seealso, I, [])]
     || I <- tether_book:flat()].

%% 全体が正しい UTF-8 であること。
%% <<"日本語">> は1文字1バイトに切り詰められるので、直接書いてはいけない。
utf8_test() ->
    [?assertNotEqual(error, unicode:characters_to_list(B))
     || B <- collect(tether_book:chapters())].

collect(M) when is_map(M)  -> lists:flatmap(fun collect/1, maps:values(M));
collect(L) when is_list(L) -> lists:flatmap(fun collect/1, L);
collect(B) when is_binary(B) -> [B];
collect(_) -> [].
