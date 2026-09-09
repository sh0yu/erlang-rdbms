-module(file_mng_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/simple_db_server.hrl").

%%%===================================================================
%%% ページのエンコード/デコード
%%%===================================================================

encode_page_is_page_sized_test() ->
    {ok, Bin, _Empty, _Count} =
        file_mng:encode_page(#disk_data{data_list = [#slot{slot_n = 1, data = [apple, 100]}]}),
    ?assertEqual(file_mng:page_size(), byte_size(Bin)).

roundtrip_single_slot_test() ->
    Slots = [#slot{slot_n = 1, data = [apple, 100]}],
    ?assertEqual(Slots, roundtrip(Slots)).

roundtrip_multiple_slots_test() ->
    Slots = [#slot{slot_n = 1, data = [apple, 100]},
             #slot{slot_n = 2, data = [orange, 120]},
             #slot{slot_n = 3, data = [grape, 150]}],
    ?assertEqual(Slots, roundtrip(Slots)).

%% スロット番号が飛んでいても各スロットの中身が入れ替わらないこと
roundtrip_sparse_slots_test() ->
    Slots = [#slot{slot_n = 2, data = [orange, 120]},
             #slot{slot_n = 5, data = [banana, 200]}],
    ?assertEqual(Slots, roundtrip(Slots)).

%% 入力の順序に関係なくスロット番号どおりに復元されること
roundtrip_unordered_input_test() ->
    Slots = [#slot{slot_n = 3, data = [grape, 150]},
             #slot{slot_n = 1, data = [apple, 100]},
             #slot{slot_n = 2, data = [orange, 120]}],
    ?assertEqual(lists:keysort(#slot.slot_n, Slots), roundtrip(Slots)).

roundtrip_empty_page_test() ->
    ?assertEqual([], roundtrip([])).

%% アトム・整数・文字列・ネストした項が型を保ったまま往復すること
roundtrip_preserves_erlang_terms_test() ->
    Data = [apple, 100, "text", <<"bin">>, {tuple, 1}, [nested, [list]], 3.5],
    [#slot{data = Got}] = roundtrip([#slot{slot_n = 1, data = Data}]),
    ?assertEqual(Data, Got).

header_reports_slot_count_and_empty_size_test() ->
    Slots = [#slot{slot_n = 1, data = [apple, 100]},
             #slot{slot_n = 2, data = [orange, 120]}],
    {ok, _Bin, EmptySize, SlotCount} =
        file_mng:encode_page(#disk_data{data_list = Slots}),
    ?assertEqual(2, SlotCount),
    Payloads = lists:sum([file_mng:payload_size(D) || #slot{data = D} <- Slots]),
    Expected = file_mng:page_size() - file_mng:header_size()
        - 2 * file_mng:slot_entry_size() - Payloads,
    ?assertEqual(Expected, EmptySize).

%% ページに収まらないデータはエラーになり、暗黙に切り詰められないこと
page_overflow_is_reported_test() ->
    Big = lists:duplicate(file_mng:page_size(), 16#41),
    ?assertEqual({error, page_overflow},
                 file_mng:encode_page(#disk_data{data_list = [#slot{slot_n = 1, data = Big}]})).

decode_zeroed_page_is_empty_test() ->
    Zero = <<0:(file_mng:page_size() * 8)>>,
    #disk_data{slot_count = SlotCount, data_list = DataList} = file_mng:decode_page(Zero),
    ?assertEqual(0, SlotCount),
    ?assertEqual([], DataList).

required_size_includes_directory_entry_test() ->
    Data = [apple, 100],
    ?assertEqual(file_mng:payload_size(Data) + file_mng:slot_entry_size(),
                 file_mng:required_size(Data)).

%%%===================================================================
%%% ファイルI/O
%%%===================================================================

file_io_test_() ->
    {setup, fun setup/0, fun cleanup/1, fun(Dir) -> [
        ?_test(write_then_read_page(Dir)),
        ?_test(read_beyond_eof_is_eof(Dir)),
        ?_test(page_header_matches_written_page(Dir)),
        ?_test(pages_are_independent(Dir)),
        ?_test(overwrite_page_replaces_contents(Dir))
    ] end}.

write_then_read_page(Dir) ->
    Fd = open(Dir, "rw.dat"),
    Slots = [#slot{slot_n = 1, data = [apple, 100]},
             #slot{slot_n = 2, data = [orange, 120]}],
    {ok, #disk_data{slot_count = 2}} =
        file_mng:write_page(Fd, 0, #disk_data{data_list = Slots}),
    {ok, #disk_data{data_list = Got}} = file_mng:load_page(Fd, 0),
    ?assertEqual(Slots, Got),
    ok = file_mng:close(Fd).

read_beyond_eof_is_eof(Dir) ->
    Fd = open(Dir, "eof.dat"),
    ?assertEqual(eof, file_mng:load_page(Fd, 0)),
    ?assertEqual(eof, file_mng:get_page_header(Fd, 0)),
    ok = file_mng:close(Fd).

page_header_matches_written_page(Dir) ->
    Fd = open(Dir, "hdr.dat"),
    Slots = [#slot{slot_n = 1, data = [apple, 100]}],
    {ok, #disk_data{empty_size = Empty, slot_count = Count}} =
        file_mng:write_page(Fd, 0, #disk_data{data_list = Slots}),
    ?assertEqual({Empty, Count}, file_mng:get_page_header(Fd, 0)),
    ok = file_mng:close(Fd).

pages_are_independent(Dir) ->
    Fd = open(Dir, "multi.dat"),
    P0 = [#slot{slot_n = 1, data = [apple, 100]}],
    P3 = [#slot{slot_n = 1, data = [banana, 200]}],
    {ok, _} = file_mng:write_page(Fd, 0, #disk_data{data_list = P0}),
    {ok, _} = file_mng:write_page(Fd, 3, #disk_data{data_list = P3}),
    {ok, #disk_data{data_list = Got0}} = file_mng:load_page(Fd, 0),
    {ok, #disk_data{data_list = Got3}} = file_mng:load_page(Fd, 3),
    ?assertEqual(P0, Got0),
    ?assertEqual(P3, Got3),
    %% 書いていない中間ページは空ページとして読める
    {ok, #disk_data{data_list = []}} = file_mng:load_page(Fd, 1),
    ?assertEqual(4, file_mng:page_count(Fd)),
    ok = file_mng:close(Fd).

overwrite_page_replaces_contents(Dir) ->
    Fd = open(Dir, "over.dat"),
    {ok, _} = file_mng:write_page(
                Fd, 0, #disk_data{data_list = [#slot{slot_n = 1, data = [apple, 100]},
                                               #slot{slot_n = 2, data = [orange, 120]}]}),
    %% スロット2を削除した状態で書き戻す
    {ok, _} = file_mng:write_page(
                Fd, 0, #disk_data{data_list = [#slot{slot_n = 1, data = [apple, 100]}]}),
    {ok, #disk_data{data_list = Got}} = file_mng:load_page(Fd, 0),
    ?assertEqual([#slot{slot_n = 1, data = [apple, 100]}], Got),
    ok = file_mng:close(Fd).

%%%===================================================================
%%% Helpers
%%%===================================================================

roundtrip(Slots) ->
    {ok, Bin, _Empty, _Count} = file_mng:encode_page(#disk_data{data_list = Slots}),
    #disk_data{data_list = DataList} = file_mng:decode_page(Bin),
    DataList.

open(Dir, Name) ->
    {ok, Fd} = file_mng:open(filename:join(Dir, Name)),
    Fd.

setup() ->
    Dir = filename:join("/tmp", "file_mng_tests_" ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

cleanup(Dir) ->
    _ = [file:delete(F) || F <- filelib:wildcard(filename:join(Dir, "*"))],
    _ = file:del_dir(Dir),
    ok.
