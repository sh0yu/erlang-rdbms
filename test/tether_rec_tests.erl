-module(tether_rec_tests).
-include_lib("eunit/include/eunit.hrl").

%%%===================================================================
%%% 往復
%%%===================================================================

roundtrip_test() ->
    P = <<"hello">>,
    ?assertEqual({[P], byte_size(tether_rec:encode(P)), complete},
                 tether_rec:scan(tether_rec:encode(P))).

empty_payload_test() ->
    ?assertEqual({[<<>>], 12, complete}, tether_rec:scan(tether_rec:encode(<<>>))).

empty_log_test() ->
    ?assertEqual({[], 0, complete}, tether_rec:scan(<<>>)).

many_test() ->
    Ps = [<<"a">>, <<>>, <<0:8000>>, <<"last">>],
    Bin = tether_rec:encode_all(Ps),
    ?assertEqual({Ps, byte_size(Bin), complete}, tether_rec:scan(Bin)).

%%%===================================================================
%%% 切り捨て — 電源断で普通に起きる形
%%%===================================================================

%% 末尾をあらゆる位置で切っても、
%%   * 落ちない
%%   * でっち上げのレコードを返さない
%%   * 健全な部分の終わりを正しく指す
truncate_everywhere_test() ->
    Ps  = [<<"one">>, <<"two">>, <<"three">>],
    Bin = tether_rec:encode_all(Ps),
    Full = byte_size(Bin),
    Bounds = record_bounds(Ps),
    [begin
         Cut = binary:part(Bin, 0, N),
         {Recs, Off, St} = tether_rec:scan(Cut),
         %% 完全に収まっているレコードの数
         Expect = length([B || B <- Bounds, B =< N]),
         ?assertEqual({N, Expect}, {N, length(Recs)}),
         ?assertEqual({N, lists:sublist(Ps, Expect)}, {N, Recs}),
         ?assertEqual({N, lists:max([0 | [B || B <- Bounds, B =< N]])}, {N, Off}),
         %% レコードの境目でちょうど切れていれば、それは健全なログである。
         %% 境目でなければ末尾が切れている。
         case lists:member(N, [0 | Bounds]) of
             true  -> ?assertEqual({N, complete},  {N, St});
             false -> ?assertEqual({N, truncated}, {N, St})
         end
     end || N <- lists:seq(0, Full)].

full_is_complete_test() ->
    Bin = tether_rec:encode_all([<<"one">>, <<"two">>]),
    ?assertMatch({[<<"one">>, <<"two">>], _, complete}, tether_rec:scan(Bin)).

%% 各レコードの終端オフセット
record_bounds(Ps) ->
    {Bounds, _} = lists:foldl(
                    fun(P, {Acc, Off}) ->
                            E = Off + tether_rec:header_size() + byte_size(P),
                            {[E | Acc], E}
                    end, {[], 0}, Ps),
    lists:reverse(Bounds).

%%%===================================================================
%%% 破損 — 切り捨てとは区別しなければならない
%%%===================================================================

bad_crc_test() ->
    Bin  = tether_rec:encode_all([<<"one">>, <<"two">>]),
    %% 2つ目の本体を1バイトだけ書き換える
    Off  = tether_rec:header_size() + 3 + tether_rec:header_size(),
    Bad  = flip(Bin, Off),
    ?assertEqual({[<<"one">>], 15, {corrupt, bad_crc}}, tether_rec:scan(Bad)).

bad_magic_test() ->
    Bin = tether_rec:encode_all([<<"one">>, <<"two">>]),
    Bad = flip(Bin, 15),                      % 2レコード目のマジック
    ?assertEqual({[<<"one">>], 15, {corrupt, bad_magic}}, tether_rec:scan(Bad)).

%% 長さの欄が壊れて巨大になっても、それを信じて確保しに行かない
too_long_test() ->
    <<Magic:32, _Len:32, Rest/binary>> = tether_rec:encode(<<"x">>),
    Bad = <<Magic:32, 16#7FFFFFFF:32, Rest/binary>>,
    ?assertEqual({[], 0, {corrupt, too_long}}, tether_rec:scan(Bad)).

%% 破損は「読めるところまで読めた」で済ませてはいけない。
%% 切り捨てと同じ扱いにすると、ディスクの故障が
%% 「少しデータが古いだけ」に化けて見えなくなる。
corrupt_is_not_truncated_test() ->
    Bin = tether_rec:encode_all([<<"one">>, <<"two">>, <<"three">>]),
    %% 2レコード目の本体を壊す。後ろにまだレコードがある位置。
    {Recs, Off, St} = tether_rec:scan(flip(Bin, 28)),
    ?assertEqual([<<"one">>], Recs),
    ?assertEqual(15, Off),
    ?assertMatch({corrupt, _}, St),
    ?assertNotEqual(truncated, St).

%% 途中のレコードの長さの欄が壊れた場合も捕まえる。
%% 続きのバイトが存在するので、それを本体として読んで CRC が合わない。
corrupt_length_in_middle_test() ->
    Bin = tether_rec:encode_all([<<"one">>, <<"two">>, <<"three">>]),
    %% 1レコード目の長さ 3 を 5 に書き換える
    <<H:4/binary, _:32, T/binary>> = Bin,
    Bad = <<H/binary, 5:32, T/binary>>,
    ?assertEqual({[], 0, {corrupt, bad_crc}}, tether_rec:scan(Bad)).

%% 既知の限界。**最後の**レコードの長さの欄が壊れて大きくなると、
%% 本体が足りなくなるので truncated と区別できない。
%% どちらでも取るべき行動は同じ(末尾を捨てる)なので安全である。
corrupt_length_at_tail_degrades_to_truncated_test() ->
    Bin = tether_rec:encode_all([<<"one">>]),
    <<H:4/binary, _:32, T/binary>> = Bin,
    Bad = <<H/binary, 99:32, T/binary>>,
    ?assertEqual({[], 0, truncated}, tether_rec:scan(Bad)).

flip(Bin, Off) ->
    <<H:Off/binary, B:8, T/binary>> = Bin,
    <<H/binary, (B bxor 16#FF):8, T/binary>>.
