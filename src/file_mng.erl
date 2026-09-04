%%%-------------------------------------------------------------------
%%% @doc
%%% テーブルデータファイルへのページ単位のアクセスを担当するプロセス。
%%% 1テーブル(1ファイル)につき1プロセスが起動し、そのファイルのFdを保持する。
%%%
%%% ページのレイアウト(?PAGE_SIZEバイト固定長)
%%%
%%%   0            12                12+8*SlotCount                    ?PAGE_SIZE
%%%   +------------+-----------------+-----------------+---------------+
%%%   | header     | slot directory  | free space      | data          |
%%%   | (12 bytes) | (8 bytes/slot)  |                 | (末尾から前方へ) |
%%%   +------------+-----------------+-----------------+---------------+
%%%
%%%   header : <<?PAGE_MAGIC:32, EmptySize:32, SlotCount:32>>
%%%   slot N : <<Offset:32, Length:32>>
%%%            Offsetはページ先頭からのバイトオフセット。
%%%            Length =:= 0 は未使用(論理削除済み)スロットを表す。
%%%   data   : term_to_binary/1 でシリアライズしたスロットのデータ本体。
%%%            アトム・数値・文字列などの任意のErlang項をそのまま保存できる。
%%%
%%% EmptySizeはスロットディレクトリの末尾からデータ領域の先頭までの
%%% 空きバイト数。新しい行を1件追加するには
%%% `byte_size(term_to_binary(Data)) + ?SLOT_ENTRY_SIZE' バイト必要になる。
%%% @end
%%%-------------------------------------------------------------------
-module(file_mng).
-behaviour(gen_server).

%% Public API
-export([open/1, open/2, close/1, delete/1, truncate/1]).
-export([load_page/2, get_page_header/2, write_page/3, page_count/1]).
-export([page_size/0, header_size/0, slot_entry_size/0, max_payload_size/0]).
-export([encode_page/1, decode_page/1, payload_size/1, required_size/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("../include/simple_db_server.hrl").

-define(PAGE_SIZE, 4096).
-define(HEADER_SIZE, 12).       % <<Magic:32, EmptySize:32, SlotCount:32>>
-define(SLOT_ENTRY_SIZE, 8).    % <<Offset:32, Length:32>>
-define(PAGE_MAGIC, 16#5044424D). % "PDBM"

-record(file, {
    fd,
    filepath,
    options
}).

%%%===================================================================
%%% Public API
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc データファイルを読み書きモードで開く。存在しない場合は作成する。
%% Returns: {ok, Fd} | {error, Reason}
%%----------------------------------------------------------------------
open(Filepath) ->
    open(Filepath, []).

open(Filepath, Options) ->
    gen_server:start_link(?MODULE, {Filepath, Options}, []).

close(Fd) ->
    gen_server:call(Fd, close, infinity).

%%----------------------------------------------------------------------
%% @doc ファイルの内容を破棄して開き直す。
%%----------------------------------------------------------------------
delete(Fd) ->
    gen_server:call(Fd, delete, infinity).

truncate(Fd) ->
    gen_server:call(Fd, truncate, infinity).

%%----------------------------------------------------------------------
%% @doc PageIdのページを読み込む。
%% Returns: {ok, #disk_data{}} | eof
%%----------------------------------------------------------------------
load_page(Fd, PageId) ->
    gen_server:call(Fd, {load_page, PageId}, infinity).

%%----------------------------------------------------------------------
%% @doc PageIdのページヘッダのみ読み込む。空きページ探索で使う。
%% Returns: {EmptySize, SlotCount} | eof
%%----------------------------------------------------------------------
get_page_header(Fd, PageId) ->
    gen_server:call(Fd, {get_page_header, PageId}, infinity).

%%----------------------------------------------------------------------
%% @doc PageIdのページを書き込む。
%% Returns: {ok, #disk_data{}} 書き込み後のヘッダ情報 | {error, Reason}
%%----------------------------------------------------------------------
write_page(Fd, PageId, #disk_data{} = DiskData) ->
    gen_server:call(Fd, {write_page, PageId, DiskData}, infinity).

%%----------------------------------------------------------------------
%% @doc ファイルが保持しているページ数を返す。
%%----------------------------------------------------------------------
page_count(Fd) ->
    gen_server:call(Fd, page_count, infinity).

page_size() -> ?PAGE_SIZE.
header_size() -> ?HEADER_SIZE.
slot_entry_size() -> ?SLOT_ENTRY_SIZE.

%%----------------------------------------------------------------------
%% @doc 1ページに格納できるデータ本体の最大バイト数(スロット1個の場合)。
%%----------------------------------------------------------------------
max_payload_size() ->
    ?PAGE_SIZE - ?HEADER_SIZE - ?SLOT_ENTRY_SIZE.

%%----------------------------------------------------------------------
%% @doc データ本体のシリアライズ後のバイト数。
%%----------------------------------------------------------------------
payload_size(Data) ->
    byte_size(term_to_binary(Data)).

%%----------------------------------------------------------------------
%% @doc 新規スロットとしてデータを1件書き込むのに必要なバイト数。
%% データ本体に加えてスロットディレクトリのエントリ分が必要になる。
%%----------------------------------------------------------------------
required_size(Data) ->
    payload_size(Data) + ?SLOT_ENTRY_SIZE.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init({Filepath, Options}) ->
    ok = filelib:ensure_dir(Filepath),
    case file:open(Filepath, [read, write, raw, binary]) of
        {ok, Fd} ->
            {ok, #file{fd = Fd, filepath = Filepath, options = Options}};
        {error, Reason} ->
            {stop, {cannot_open, Filepath, Reason}}
    end.

handle_call(close, _From, #file{fd = Fd} = File) ->
    {stop, normal, file:close(Fd), File#file{fd = nil}};

handle_call(delete, _From, #file{fd = Fd, filepath = Filepath} = File) ->
    ok = file:close(Fd),
    _ = file:delete(Filepath),
    ok = filelib:ensure_dir(Filepath),
    {ok, NewFd} = file:open(Filepath, [read, write, raw, binary]),
    {reply, ok, File#file{fd = NewFd}};

handle_call(truncate, _From, #file{fd = Fd} = File) ->
    {ok, _} = file:position(Fd, bof),
    {reply, file:truncate(Fd), File};

handle_call(page_count, _From, #file{fd = Fd} = File) ->
    {reply, do_page_count(Fd), File};

handle_call({load_page, PageId}, _From, #file{fd = Fd} = File) ->
    Reply = case read_page(Fd, PageId) of
                eof -> eof;
                {ok, Bin} -> {ok, decode_page(Bin)}
            end,
    {reply, Reply, File};

handle_call({get_page_header, PageId}, _From, #file{fd = Fd} = File) ->
    Reply = case file:pread(Fd, PageId * ?PAGE_SIZE, ?HEADER_SIZE) of
                eof ->
                    eof;
                {ok, <<?PAGE_MAGIC:32, EmptySize:32, SlotCount:32>>} ->
                    {EmptySize, SlotCount};
                {ok, Short} when byte_size(Short) < ?HEADER_SIZE ->
                    %% 途中で切れたページはまだ書かれていないものとして扱う
                    eof;
                {ok, _Other} ->
                    {error, corrupt_page};
                {error, Reason} ->
                    {error, Reason}
            end,
    {reply, Reply, File};

handle_call({write_page, PageId, DiskData}, _From, #file{fd = Fd} = File) ->
    Reply = case encode_page(DiskData) of
                {ok, Bin, EmptySize, SlotCount} ->
                    case file:pwrite(Fd, PageId * ?PAGE_SIZE, Bin) of
                        ok ->
                            {ok, #disk_data{empty_size = EmptySize,
                                            slot_count = SlotCount}};
                        {error, Reason} ->
                            {error, Reason}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end,
    {reply, Reply, File};

handle_call(Request, _From, File) ->
    {reply, {error, {unknown_request, Request}}, File}.

handle_cast(close, File) ->
    {stop, normal, File};
handle_cast(_Msg, File) ->
    {noreply, File}.

handle_info(_Msg, File) ->
    {noreply, File}.

terminate(_Reason, #file{fd = nil}) ->
    ok;
terminate(_Reason, #file{fd = Fd}) ->
    _ = file:close(Fd),
    ok.

code_change(_OldVsn, File, _Extra) ->
    {ok, File}.

%%%===================================================================
%%% Page codec
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc #disk_data{} を ?PAGE_SIZE バイトのバイナリに変換する。
%%
%% data_listに現れないスロット番号は空きスロット(Length=0)として
%% ディレクトリに残す。これによりスロット番号がPhysLocのキーとして
%% 安定し、削除されたスロットを後から再利用できる。
%%
%% Returns: {ok, Binary, EmptySize, SlotCount} | {error, page_overflow}
%%----------------------------------------------------------------------
encode_page(#disk_data{data_list = SlotDataList}) ->
    Slots = lists:keysort(#slot.slot_n, SlotDataList),
    SlotCount = case Slots of
                    [] -> 0;
                    _ -> (lists:last(Slots))#slot.slot_n
                end,
    Payloads = [{N, term_to_binary(D)} || #slot{slot_n = N, data = D} <- Slots],
    DataBytes = lists:sum([byte_size(B) || {_N, B} <- Payloads]),
    DirSize = SlotCount * ?SLOT_ENTRY_SIZE,
    Used = ?HEADER_SIZE + DirSize + DataBytes,
    case Used > ?PAGE_SIZE of
        true ->
            {error, page_overflow};
        false ->
            EmptySize = ?PAGE_SIZE - Used,
            %% データはページ末尾から前方に向かって詰める
            {Dir, DataRegion} = build_data_region(Payloads, SlotCount),
            Free = <<0:(EmptySize * 8)>>,
            Bin = <<?PAGE_MAGIC:32, EmptySize:32, SlotCount:32,
                    Dir/binary, Free/binary, DataRegion/binary>>,
            ?PAGE_SIZE = byte_size(Bin),
            {ok, Bin, EmptySize, SlotCount}
    end.

%% スロットディレクトリとデータ領域を同時に組み立てる。
%% スロット1のデータがページ末尾側、以降前方に向かって並べる。
build_data_region(Payloads, SlotCount) ->
    build_data_region(1, SlotCount, Payloads, ?PAGE_SIZE, [], []).

build_data_region(N, SlotCount, _Payloads, _End, DirAcc, DataAcc)
  when N > SlotCount ->
    {iolist_to_binary(lists:reverse(DirAcc)), iolist_to_binary(DataAcc)};
build_data_region(N, SlotCount, Payloads, End, DirAcc, DataAcc) ->
    case lists:keyfind(N, 1, Payloads) of
        false ->
            %% 未使用スロット
            build_data_region(N + 1, SlotCount, Payloads, End,
                              [<<0:32, 0:32>> | DirAcc], DataAcc);
        {N, Bin} ->
            Len = byte_size(Bin),
            Offset = End - Len,
            build_data_region(N + 1, SlotCount, Payloads, Offset,
                              [<<Offset:32, Len:32>> | DirAcc], [Bin | DataAcc])
    end.

%%----------------------------------------------------------------------
%% @doc ページのバイナリを #disk_data{} に復元する。
%% 未使用スロットはdata_listに含めない。
%%----------------------------------------------------------------------
decode_page(<<?PAGE_MAGIC:32, EmptySize:32, SlotCount:32, _/binary>> = Page) ->
    DataList = read_slots(Page, 1, SlotCount),
    #disk_data{empty_size = EmptySize, slot_count = SlotCount,
               data_list = DataList};
decode_page(<<0:32, _/binary>>) ->
    %% 未初期化(ゼロ埋め)のページは空ページとみなす
    empty_disk_data();
decode_page(_) ->
    empty_disk_data().

read_slots(_Page, N, SlotCount) when N > SlotCount ->
    [];
read_slots(Page, N, SlotCount) ->
    EntryPos = ?HEADER_SIZE + (N - 1) * ?SLOT_ENTRY_SIZE,
    <<_:EntryPos/binary, Offset:32, Len:32, _/binary>> = Page,
    case Len of
        0 ->
            read_slots(Page, N + 1, SlotCount);
        _ ->
            <<_:Offset/binary, Payload:Len/binary, _/binary>> = Page,
            [#slot{slot_n = N, data = binary_to_term(Payload)}
             | read_slots(Page, N + 1, SlotCount)]
    end.

empty_disk_data() ->
    #disk_data{empty_size = ?PAGE_SIZE - ?HEADER_SIZE,
               slot_count = 0,
               data_list = []}.

%%%===================================================================
%%% Internal helpers
%%%===================================================================

%% ページ全体を読む。ファイル末尾で切れている場合はゼロ埋めして返す。
read_page(Fd, PageId) ->
    case file:pread(Fd, PageId * ?PAGE_SIZE, ?PAGE_SIZE) of
        eof ->
            eof;
        {ok, Bin} when byte_size(Bin) =:= ?PAGE_SIZE ->
            {ok, Bin};
        {ok, Short} ->
            Pad = (?PAGE_SIZE - byte_size(Short)) * 8,
            {ok, <<Short/binary, 0:Pad>>};
        {error, Reason} ->
            error({page_read_failed, PageId, Reason})
    end.

do_page_count(Fd) ->
    case file:position(Fd, eof) of
        {ok, Size} -> (Size + ?PAGE_SIZE - 1) div ?PAGE_SIZE;
        {error, _} -> 0
    end.
