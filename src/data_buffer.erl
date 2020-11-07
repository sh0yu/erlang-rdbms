-module(data_buffer).
-compile(export_all).
-behaviour(gen_server).

-include("../include/simple_db_server.hrl").

-define(BUF_N, 2).
-define(SLOT_SIZE, 4).
-record(buf_info, {
    buf_name,
    table_name=nil,
    page_id=nil,
    empty_size=0,
    slot_count=0,
    timestamp=0
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% DataBuffer Server APIs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% -> ["apple", "100"]
read_data(Pid, Oid) ->
    gen_server:call(Pid, {read_data, Oid}).

%% Val: ["apple", "100"]
write_data(Pid, TableName, Oid, Val) ->
    %% TODO: vacuumを実行
    gen_server:call(Pid, {write_data, TableName, Oid, Val}).

update_data(Pid, TableName, Oid, Val) ->
    % TODO: implement
    % TODO: DataSizeを固定長の倍数として扱うようにするか検討
    gen_server:call(Pid, {update_data, TableName, Oid, Val}).

delete_data(Pid, Oid) ->
    % インデックスから参照を削除することで、論理削除
    % データをinsertもしくはupdateする際に、vacuumを実行
    gen_server:call(Pid, {delete_data, Oid}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Callback functions of gen_server.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    %% [ETS]buf_info_list:バッファにロードされているテーブルとページ情報を保持する
    %% [ETS]fd_tables:テーブルデータが保存されているファイルのFdを管理する
    %% [ETS]bufxxx:ディスク上のデータをロードしたバッファ
    %% [DETS]oid_phys_loc:オブジェクトIDとファイル上のアドレスの紐付け
    %% [DETS]vacuum_oid:削除されたオブジェクトIDを保持。delete時に更新、vacuum時に参照・削除
    ets:new(buf_info_list, [set, named_table, public]),
    ets:new(fd_tables, [set, named_table, public]),
    dets:open_file(oid_phys_loc, []),
    dets:open_file(vacuum_oid, []),
    ok = init_buf_info_list(?BUF_N),
    {ok, []}.

init_buf_info_list(0) ->
    ok;
init_buf_info_list(Size) ->
    EtsName = list_to_atom("buf" ++ integer_to_list(Size)),
    ets:insert(buf_info_list, {EtsName, #buf_info{buf_name=EtsName}}),
    ets:new(EtsName, [set, named_table, public]),
    init_buf_info_list(Size - 1).

handle_call({read_data, Oid}, _From, State) ->
    case oid2physloc(Oid) of
        oid_not_found -> {reply, {error, oid_not_found}, State};
        PhysLoc ->
            #phys_loc{table_name=TableName, page_id=PageId} = PhysLoc,
            DataBuf = load_page(TableName, PageId),
            {reply, get_data_buf(DataBuf, PhysLoc), State}
    end;
    
%% データの書き込みの際は、書き込むページをバッファ上に読み込み、そこに書き込みデータを挿入・更新しディスクに書き込む
%% 読み込むためのバッファは、すでにバッファ上に読み込まれていて、空き容量が十分あるバッファ
%% なければデータファイルから空き容量のあるページを取り出す
%% 一つも空き容量があるページがなければ新規に作り出す
%% PhysLocを返すか
handle_call({write_data, TableName, Oid, Data}, _from, State) ->
    DataSize = get_data_size(Data),
    io:format("DataSize:~p~n", [DataSize]),

    %% データを挿入するバッファとロケーションを用意する
    case oid2physloc(Oid) of
        oid_not_found ->
            %% 新規挿入ケース
            PlentySpaceBuf = case get_plenty_space_buf(TableName, DataSize) of
                no_plenty_space_buf ->
                    %% 空き容量のあるバッファはバッファ上にないため、ディスクから空き容量のあるページを探す
                    FreeBuf = case get_empty_buf() of
                        [] -> get_oldest_buf();
                        EmptyBuf -> EmptyBuf
                    end,
                    io:format("FreeBuf:~p~n", [FreeBuf]),
                    NewBufInfo = get_plenty_space_page_from_disk(TableName, DataSize, FreeBuf),
                    ets:insert(buf_info_list, {FreeBuf, NewBufInfo}),
                    FreeBuf;
                EmptyBuf -> EmptyBuf
                %% 空き容量のあるバッファが見つかったケース
            end,
            io:format("PlentySpaceBuf:~p~n", [PlentySpaceBuf]),
            io:format("buf_info:~p~n", [ets:lookup(buf_info_list, PlentySpaceBuf)]),
            [{_PlentySpaceBuf, #buf_info{page_id=PageId, slot_count=Slot}}] = ets:lookup(buf_info_list, PlentySpaceBuf),
            %% TODO: どのスロットにデータを入れるか算出するスロット戦略の改善
            PhysLoc = #phys_loc{table_name=TableName, page_id=PageId, slot=Slot+1};
        #phys_loc{table_name=TableName, page_id=PageId} = PhysL ->
            %% 更新ケース(冪等性のため)
            PhysLoc = PhysL,
            PlentySpaceBuf = load_page(TableName, PageId)
        end,
        %%TODO: buf_infoのupdateできているか確認
    ets:insert(PlentySpaceBuf, {PhysLoc, Data}),
    io:format("BufInfoList:~p~n", [ets:match_object(buf_info_list, {'_', '_'})]),
    %% SlotDataListに整形する
    SlotDataList = lists:map(fun({#phys_loc{slot=SlotN}, SlotData}) -> #slot{slot_n=SlotN, data=SlotData} end,
    ets:match_object(PlentySpaceBuf, {'_', '_'})),
    io:format("SlotDataList:~p~n", [SlotDataList]),
    Fd = get_fd(TableName),
    case file_mng:write_page(Fd, PageId, #disk_data{data_list=SlotDataList}) of
        {ok, #disk_data{empty_size=EmptySize, slot_count=SlotCount}} ->
            ets:insert(buf_info_list, {PlentySpaceBuf, #buf_info{table_name=TableName, page_id=PageId, empty_size=EmptySize, slot_count=SlotCount}}),
            dets:insert(oid_phys_loc, {Oid, PhysLoc}),
            {reply, ok, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

%% データの更新は、指定されたPhysLocのデータをバッファ上に読み込み、データを更新し、ディスクに書き込む
%% すでにバッファ上にデータが読み込まれていれば更新してディスクに書きこむ
%% TODO: 更新前データサイズに対して更新データサイズが大きすぎる場合、
%% 更新前データをページから削除・ディスク書き込み、空きがあるページを読み込み、更新後データ挿入、ディスク書き込み
%% さらに、PhysLocが変わるので、インデックス更新。といった処理が必要
handle_call({update_data, TableName, #phys_loc{table_name=TableName, page_id=PageId}=PhysLoc, Data}, _from, State) ->
    DataBuf = load_page(TableName, PageId),
    ets:insert(DataBuf, {PhysLoc, Data}),
    SlotDataList = lists:map(fun({#phys_loc{slot=SlotN}, SlotData}) -> #slot{slot_n=SlotN, data=SlotData} end,
    ets:match_object(DataBuf, {'_', '_'})),
    io:format("SlotDataList:~p~n", [SlotDataList]),
    Fd = get_fd(TableName),
    BufInfo = case ets:lookup(buf_info_list, DataBuf) of
        [] -> conflict_error;
        [{DataBuf, BufI}] -> BufI
    end,
    case file_mng:write_page(Fd, PageId, #disk_data{data_list=SlotDataList}) of
        {ok, #disk_data{empty_size=EmptySize, slot_count=SlotCount}} ->
            ets:insert(buf_info_list, {DataBuf, BufInfo#buf_info{empty_size=EmptySize, slot_count=SlotCount}}),
            {reply, {ok, PhysLoc}, State};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end;

handle_call({delete_data, Oid}, _From, State) ->
    case oid2physloc(Oid) of
        oid_not_found -> {reply, {error, oid_not_found}, State};
        PhysLoc ->
            #phys_loc{table_name=TableName, page_id=PageId} = PhysLoc,
            TargetBuf = load_page(TableName, PageId),
            ets:delete(TargetBuf, PhysLoc),
            SlotDataList = lists:map(fun({#phys_loc{slot=SlotN}, SlotData}) -> #slot{slot_n=SlotN, data=SlotData} end,
            ets:match_object(TargetBuf, {'_', '_'})),
            io:format("SlotDataList:~p~n", [SlotDataList]),
            %% TODO: buf_infoの更新できているか確認
            Fd = get_fd(TableName),
            case file_mng:write_page(Fd, PageId, #disk_data{data_list=SlotDataList}) of
                {ok, #disk_data{empty_size=EmptySize, slot_count=SlotCount}} ->
                    ets:insert(buf_info_list, {TargetBuf, #buf_info{table_name=TableName, page_id=PageId, empty_size=EmptySize, slot_count=SlotCount}}),
                    dets:insert(oid_phys_loc, {Oid, PhysLoc}),
                    {reply, ok, State};
                {error, Reason} ->
                    {reply, {error, Reason}, State}
            end
    end.
    
handle_cast({get_config}, []) ->
    {noreply, []}.

handle_info(Msg, State) ->
    io:format("Unexpected message: ~p~n", [Msg]),
    {noreply, State}.

terminate(normal, _State) ->
    io:format("Server teminated.~n"),
    ok.

load_page(TableName, PageId) ->
    case is_data_in_buf(TableName, PageId) of
        false ->
            io:format("~p~n", [ets:match_object(buf_info_list, {'_', '_'})]),
            %% ディスクからデータを読み込むためのFreeBufは空っぽのバッファ
            %% それがなければ、最も古い時刻に読み込まれたバッファ
            FreeBuf = case get_empty_buf() of
                [] -> get_oldest_buf();
                EmptyBuf -> EmptyBuf
            end,
            NewBufInfo = get_page_from_disk(TableName, PageId, FreeBuf),
            ets:insert(buf_info_list, {FreeBuf, NewBufInfo}),
            io:format("cache miss. load table_name=~p, page_id=~p to [~p].~n", [TableName, PageId, NewBufInfo]),
            FreeBuf;
        BName ->
            io:format("cache hit [~p].~n", [BName]),
            BName
    end.

%% 取得したいデータがバッファに格納されているかチェックする
%% -> false | BufName
is_data_in_buf(TableName, PageId) ->
    case ets:match_object(buf_info_list,
            {'_', #buf_info{buf_name='_', table_name=TableName, page_id=PageId, empty_size='_', slot_count='_', timestamp='_'}}) of
        [] -> false;
        [{BufName, _BufInfo}] ->
            BufName
    end.

%% バッファからデータを取り出す
get_data_buf(BufName, PhysLoc) ->
    case ets:lookup(BufName, PhysLoc) of
        [] -> item_not_found;
        [{_PhysLoc, []}] ->
            item_deleted;
        [{_PhysLoc, Val}] ->
            Val
    end.

%% ディスク上のTableNameデータファイルからPageIdのデータを抽出して、FreeBufに格納する
%% バッファには、PhysLocを元に作成したキーとディスクから抽出したバリューをリスト形式で格納する
get_page_from_disk(TableName, PageId, FreeBuf) ->
    Fd = get_fd(TableName),
    {ok, #disk_data{empty_size=EmptySize, slot_count=SlotCount, data_list=SlotDataList}} = file_mng:load_page(Fd, PageId),
    ets:delete(FreeBuf),
    ets:new(FreeBuf, [set, named_table, public]),
    lists:map(fun(#slot{slot_n=Slot, data=Data}) ->
        ets:insert(FreeBuf, {#phys_loc{table_name=TableName, page_id=PageId, slot=Slot}, Data}) end,
        SlotDataList),
    #buf_info{buf_name=FreeBuf, table_name=TableName, page_id=PageId, empty_size=EmptySize, slot_count=SlotCount, timestamp=erlang:system_time(nanosecond)}.

get_plenty_space_page_from_disk(TableName, DataSize, FreeBuf) ->
    Fd = get_fd(TableName),
    case find_plenty_space_page_id(Fd, DataSize, 0) of
        {update, PageId} ->
            %% 既存ページにデータを挿入・更新して、ファイルに書き戻すケース
            io:format("PageId:~p~n", [PageId]),
            get_page_from_disk(TableName, PageId, FreeBuf);
        {new, PageId} ->
            %% 新規データを挿入したページをファイルの末尾に追加するケース
            ets:delete(FreeBuf),
            ets:new(FreeBuf, [set, named_table, public]),
            #buf_info{buf_name=FreeBuf, table_name=TableName, page_id=PageId, empty_size=0, slot_count=0, timestamp=erlang:system_time(nanosecond)}
    end.

find_plenty_space_page_id(Fd, DataSize, PageId) ->
    case file_mng:get_page_header(Fd, PageId) of
        eof -> {new, PageId};
        {EmptySize, _SlotCount} when EmptySize >= DataSize -> {update, PageId};
        _ ->
            find_plenty_space_page_id(Fd, DataSize, PageId + 1)
    end.

%% ディスクからデータを読み込むための空きバッファを返す
%% 全てのバッファが埋まっている時の戦略は要検討
get_empty_buf() ->
    case ets:match_object(buf_info_list, {'_', #buf_info{buf_name='_', table_name=nil, page_id=nil}}) of
        [] -> [];
        [{BufName, _BufInfo} | _] ->
            BufName
    end.

%% 最も古いバッファ名を返す
get_oldest_buf() ->
    get_oldest_buf(ets:match_object(buf_info_list, {'_', '_'}), 0, nil).
get_oldest_buf([], _OldestTimestamp, OldestBufName) ->
    OldestBufName;
get_oldest_buf([{BufName, #buf_info{timestamp=Timestamp}} | Rest], 0, nil) ->
    get_oldest_buf(Rest, Timestamp, BufName);
get_oldest_buf([{BufName, #buf_info{timestamp=Timestamp}} | Rest], OldestTimestamp, _OldestBufName)
    when Timestamp < OldestTimestamp ->
        get_oldest_buf(Rest, Timestamp, BufName);
get_oldest_buf([_H | Rest], OldestTimestamp, OldestBufName) ->
    get_oldest_buf(Rest, OldestTimestamp, OldestBufName).

get_plenty_space_buf(TableName, DataSize) ->
    io:format("get_plenty_space_buf/2, called~n"),
    BufInfoList = ets:match_object(buf_info_list,
        {'_', #buf_info{buf_name='_', table_name=fruit, page_id='_', empty_size='_', slot_count='_', timestamp='_'}}),
    io:format("BufInfoList:~p~n", [BufInfoList]),
    io:format("DataSize:~p~n", [DataSize]),
    get_plenty_space_buf(BufInfoList, TableName, DataSize).
get_plenty_space_buf([], _TableName, _DataSize) ->
    no_plenty_space_buf;
get_plenty_space_buf([{BufName, #buf_info{empty_size=EmptySize}} | _Rest], _TableName, DataSize)
    when EmptySize >= DataSize ->
    BufName;
get_plenty_space_buf([_H | Rest], _TableName, DataSize) ->
    get_plenty_space_buf(Rest, _TableName, DataSize).

%% テーブルに応じて、データファイルのFdを取り出す
get_fd(TableName) ->
    case ets:lookup(fd_tables, TableName) of
        [] ->
            {ok, Fd} = file_mng:open(get_filename(TableName)),
            ets:insert(fd_tables, {TableName, Fd}),
            Fd;
        [{_TableName, Fd}] ->
            Fd
    end.

get_filename(TableName) ->
    %% TODO:テーブルとファイル名の紐付けは文字列操作でよいか(テーブルの存在チェックなど)
    atom_to_list(TableName) ++ ".dat".

%% Data:["apple", "100"]
get_data_size(Data) ->
    byte_size(list_to_binary(lists:join(" ", Data))) + ?SLOT_SIZE.

oid2physloc(Oid) ->
    case dets:lookup(oid_phys_loc, Oid) of
        [] -> oid_not_found;
        [{Oid, PhysLoc}] -> PhysLoc
    end.