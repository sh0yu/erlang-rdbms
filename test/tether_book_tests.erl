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
             <<"notes">>    -> ?assert(length(maps:get(sections, I)) >= 1, Id)
         end
     end || I <- tether_book:flat()].

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
