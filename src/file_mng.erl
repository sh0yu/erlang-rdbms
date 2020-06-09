-module(file_mng).
-compile(export_all).
-behaviour(gen_server).
-include("../include/simple_db_server.hrl").

-define(PAGE_SIZE, 16#100).
-define(SLOT_SIZE, 4).
-define(HEADER_SIZE, 12). %ヘッダのバイト数
-define(DATA_DELIMITER, " ").
-record(file, {
    fd,
    filepath,
    options,
    is_sys,
    is_ret = false,
    eof = 0
}).

%%----------------------------------------------------------------------
%% Args:   Valid Options are [create] and [create,overwrite].
%%  Files are opened in read/write mode.
%% Returns: On success, {ok, Fd}
%%  or {error, Reason} if the file could not be opened.
%%----------------------------------------------------------------------
open(Filepath) ->
    open(Filepath, []).

open(Filepath, Options) ->
    gen_server:start_link(file_mng, {Filepath, Options, self(), make_ref()}, []).

%%----------------------------------------------------------------------
%% Purpose: Truncate a file to the number of bytes.
%% Returns: ok
%%  or {error, Reason}.
%%----------------------------------------------------------------------
truncate(Fd) ->
    gen_server:call(Fd, {truncate}, infinity).

%%----------------------------------------------------------------------
%% Purpose: Close the file.
%% Returns: ok
%%----------------------------------------------------------------------
close(Fd) ->
    gen_server:call(Fd, close, infinity).

delete(Fd) ->
    gen_server:call(Fd, delete, infinity).

%%----------------------------------------------------------------------
%% Purpose: To append an Erlang binary to the end of the file.
%% Args:    Erlang term to serialize and append to the file.
%% Returns: {ok, Pos, NumBytesWritten} where Pos is the file offset to the
%%  beginning the serialized term. Use pread_term to read the term back.
%%  or {error, Reason}.
%%----------------------------------------------------------------------
append(Fd, DataList) ->
    gen_server:call(Fd, {append, DataList}).

%%----------------------------------------------------------------------
%% Purpose: Reads a binrary from a file that was written with append_binary
%% Args:    Pos, the offset into the file where the term is serialized.
%% Returns: {ok, Term}
%%  or {error, Reason}.
%%----------------------------------------------------------------------
read(Fd, Location, Number) ->
    gen_server:call(Fd, {read, Location, Number}).

load_page(Fd, PageId) ->
    gen_server:call(Fd, {load_page, PageId}).
get_page_header(Fd, PageId) ->
    gen_server:call(Fd, {get_page_header, PageId}).

write_page(Fd, PageId, SlotDataList) ->
    gen_server:call(Fd, {write_page, PageId, SlotDataList}).

init({Filepath, Options, ReturnPid, Ref}) ->
    SysFile = lists:member(sys, Options),
    OpenOptions = file_open_options(Options),
    filelib:ensure_dir(Filepath),
    case file:open(Filepath, OpenOptions) of
    {ok, Fd} ->
        file:position(Fd, eof),
        {ok, #file{fd=Fd, filepath=Filepath, options=Options, is_sys=SysFile, is_ret=SysFile}};
    Error ->
        init_status_error(ReturnPid, Ref, Error)
    end.

file_open_options(_Options) ->
    [read, write, raw, binary].
    
terminate(_Reason, #file{fd = nil}) ->
    ok;
terminate(_Reason, #file{fd = Fd}) ->
    ok = file:close(Fd).

handle_call(close, _From, #file{fd=Fd}=File) ->
    {stop, normal, file:close(Fd), File#file{fd = nil}};

handle_call(delete, _From, #file{fd=Fd, filepath=Filepath, options=Options}=File) ->
    ok = file:close(Fd),
    file:delete(Filepath),
    OpenOptions = file_open_options(Options),
    filelib:ensure_dir(Filepath),
    {ok, NewFd} = file:open(Filepath, OpenOptions),
    file:position(Fd, eof),
    {reply, ok, File#file{fd=NewFd}};

handle_call({read, Location, Number}, _From, #file{fd=Fd}=File) ->
    {ok, Res} = file:pread(Fd, Location, Number),
    {reply, {ok, Res}, File};

handle_call({load_page, PageId}, _From, #file{fd=Fd}=File) ->
    case file:pread(Fd, PageId * ?PAGE_SIZE, ?PAGE_SIZE) of
        {ok, Data} ->
            Ret = page2data(Data),
            io:format("FileData:~p~n", [Ret]),
            {reply, {ok, Ret}, File};
        eof ->
            {reply, {ok, eof}, File}
    end;

handle_call({get_page_header, PageId}, _From, #file{fd=Fd}=File) ->
    case file:pread(Fd, PageId * ?PAGE_SIZE, ?HEADER_SIZE) of
        eof -> {reply, eof, File};
        {ok, <<_Flg:4/binary, EmptySize:32/integer, SlotCount:32/integer>>} ->
            {reply, {EmptySize, SlotCount}, File}
    end;

handle_call({write_page, PageId, SlotDataList}, _From, #file{fd=Fd}=File) ->
    io:format("SlotDataList@disk:~p~n", [data2page(SlotDataList)]),
    D = data2page(SlotDataList),
    [<<0:32, EmptySize:32/integer, SlotCount:32/integer>> | _] = D,
    io:format("D:~p~n", [D]),
    case file:pwrite(Fd, PageId * ?PAGE_SIZE, D) of
        ok -> {reply, {ok, #disk_data{empty_size=EmptySize, slot_count=SlotCount}}, File};
        {error, Reason} ->
            {reply, {error, Reason}, File}
    end;

handle_call({truncate}, _From, #file{fd=Fd}=File) ->
    case file:truncate(Fd) of
    ok ->
        {reply, ok, File#file{eof = 0}};
    Error ->
        {reply, Error, File}
    end;

handle_call({append, DataList}, _From, #file{fd=Fd, is_ret=IsRet}=File) ->
    FlatDataList = lists:flatten(DataList),
    Data = lists:map(fun(X) -> atom_to_list(X) end, FlatDataList),
    Pos = file:position(Fd, eof),
    file:write(Fd, string:join(Data, " ")),
    case IsRet of
        true ->
            file:write(Fd, "\n");
        false -> nop
    end,
    {reply, ok, File#file{eof = Pos}}.

handle_cast(close, Fd) ->
    {stop,normal,Fd}.

%%----------------------------------------------------------------------
%% ファイルから読み込んだページをデータに変換する関数
%% ページはヘッダ(12バイト)とデータ部(?PAGE_SIZE-12バイト)から成る
%% ex) <<0,0,0,0,0,0,0,1,0,0,0,2,0,0,0,13,0,0,0,0,"apple 100orange 120">>
%% ヘッダは最初の4バイトが管理項目、次の4バイトがページの空き容量、次の4バイトがページに保持しているスロット数
%% データ部は(データへのオフセット*スロット数)＋パディング＋データ
%% パディングはページサイズになるよう詰める
%%----------------------------------------------------------------------
page2data(<<Flg:4/binary, EmptySize:32/integer, SlotCount:32/integer, Rest/binary>>) ->
    % io:format("Flg:~p, EmptySize:~p, SlotCount:~p, Rest:~p~n", [Flg, EmptySize, SlotCount, Rest]),
    #disk_data{empty_size=EmptySize, slot_count=SlotCount, data_list=read_slot(Rest, SlotCount, 1)}.

%% スロットが2つの場合のデータのイメージ<<0,0,0,13,0,0,0,0,"apple 100orange 120">>
%% 初めの4バイトがスロット1のデータへのオフセット、次の4バイトがスロット2のデータへのオフセットを保持
read_slot(_Data, 0, _) ->
    [];
read_slot(<<Offset:(?SLOT_SIZE*8)/integer, Rest/binary>>, SlotCount, SlotNum) ->
    <<RestData:Offset/binary, Data/binary>> = Rest,
    [#slot{slot_n=SlotNum, data=parse_data(binary_to_list(Data))} | read_slot(RestData, SlotCount - 1, SlotNum + 1)].

%% データを解釈する
parse_data([]) ->
    [];
parse_data(Data) ->
    string:split(Data, ?DATA_DELIMITER).

%%----------------------------------------------------------------------
%% データをディスクに書き込むページに変換する関数
%%----------------------------------------------------------------------
data2page(#disk_data{data_list=SlotDataList}) ->
    % SlotList = [{2,["orange","120"]},{1,["apple","100"]}]
    % SlotCount = 2
    {EmptySize, SlotCount, BinData} = construct_data(lists:reverse(lists:keysort(2, SlotDataList))),
    [construct_header(SlotDataList, EmptySize, SlotCount), BinData].

%% ヘッダを作成する(12バイト)
construct_header(_Data, EmptySize, SlotCount) ->
    <<0:32, EmptySize:32/integer, SlotCount:32/integer>>.

construct_data(Data) ->
    io:format("ConstructData:~p~n", [Data]),
    [#slot{slot_n=SlotCount} | _] = Data,
    {{OffsetList, DataList}, DataSize} = slot2page(Data, SlotCount, {[], []}, 0),
    PaddingSize = ?PAGE_SIZE - (?HEADER_SIZE + DataSize),
    Ret = [lists:map(fun (X) -> <<(X+PaddingSize):(?SLOT_SIZE*8)/integer>> end, OffsetList),
    <<0:(8*PaddingSize)>>,
    DataList],
    io:format("PageSize:~p, HeaderSize:~p, DataSize:~p, PaddingSize:~p~n", [?PAGE_SIZE, ?HEADER_SIZE, DataSize, PaddingSize]),
    io:format("Data:~p~n", [Ret]),
    {PaddingSize, SlotCount, Ret}.

% [{2,["orange","120"]},{1,["apple","100"]}]
%% -> {[15, 0], ["orange 120", "apple 100"]}
slot2page(_Rest, 0, Ret, CurOffset) ->
    {Ret, CurOffset};
slot2page([#slot{slot_n=SlotCount, data=Data} | Rest], SlotCount, {OffsetList, DataList}, CurOffset) ->
    PlainData = list_to_binary(lists:join(?DATA_DELIMITER, Data)),
    DataByteSize = byte_size(PlainData),
    slot2page(Rest, SlotCount - 1, {[CurOffset | OffsetList], [DataList, PlainData]}, CurOffset + DataByteSize + ?SLOT_SIZE);
slot2page(Rest, SlotCount, {OffsetList, DataList}, CurOffset) ->
    slot2page(Rest, SlotCount - 1, {[CurOffset | OffsetList], DataList}, CurOffset + ?SLOT_SIZE).

init_status_error(ReturnPid, Ref, Error) ->
    ReturnPid ! {Ref, self(), Error},
    ignore.
    
disk_io_test() ->
    {ok, Fd} = file:open("fruit.dat", [read,write,raw,binary]),
    Slot2 = #slot{slot_n=2, data=["orange", "120"]},
    Slot3 = #slot{slot_n=3, data=["grape", "150"]},
    Slot5 = #slot{slot_n=5, data=["banana", "200"]},
    PageW = data2page(#disk_data{data_list=[Slot2, Slot3, Slot5]}),
    io:format("PageW:~p~n", [PageW]),
    file:pwrite(Fd, 0, PageW),
    Slot6 = #slot{slot_n=1, data=["apple", "120"]},
    Slot7 = #slot{slot_n=2, data=["pineapple", "150"]},
    PageW2 = data2page(#disk_data{data_list=[Slot6, Slot7]}),
    io:format("PageW2:~p~n", [PageW2]),
    file:pwrite(Fd, ?PAGE_SIZE, PageW2),
    {ok, PageR} = file:pread(Fd, 0, ?PAGE_SIZE),
    #disk_data{empty_size=EmptySize, slot_count=SlotCount, data_list=DataR} = page2data(PageR),
    io:format("EmptySize:~p, SlotCount:~p, DataList:~p~n", [EmptySize, SlotCount, DataR]),
    {ok, PageR2} = file:pread(Fd, ?PAGE_SIZE, ?PAGE_SIZE),
    #disk_data{empty_size=EmptySize2, slot_count=SlotCount2, data_list=DataR2} = page2data(PageR2),
    io:format("EmptySize2:~p, SlotCount2:~p, DataList2:~p~n", [EmptySize2, SlotCount2, DataR2]).
