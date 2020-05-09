-module(data_buffer).
-compile(export_all).
-behaviour(gen_server).

-include("../include/simple_db_server.hrl").

-define(BUF_N, 1).
-record(buf_info, {
    buf_name,
    table_name=nil,
    page_id=nil,
    empty_size=0,
    slot_count=0
}).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% DataBuffer Server APIs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_data(Pid, Oid) ->
    gen_server:call(Pid, {get_data, Oid}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Callback functions of gen_server.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    %% [ETS]buf_info_list:バッファにロードされているテーブルとページ情報を保持する
    %% [ETS]fd_tables:テーブルデータが保存されているファイルのFdを管理する
    %% [ETS]bufxxx:ディスク上のデータをロードしたバッファ
    ets:new(buf_info_list, [set, named_table, public]),
    ets:new(fd_tables, [set, named_table, public]),
    ok = init_buf_info_list(?BUF_N),
    {ok, []}.

init_buf_info_list(0) ->
    ok;
init_buf_info_list(Size) ->
    EtsName = list_to_atom("buf" ++ integer_to_list(Size)),
    ets:insert(buf_info_list, {EtsName, #buf_info{buf_name=EtsName}}),
    ets:new(EtsName, [set, named_table, public]),
    init_buf_info_list(Size - 1).

handle_call({get_data, #oid{table_name=TableName, page_id=PageId}=Oid}, _From, State) ->
    BufName = case is_data_in_buf(TableName, PageId) of
        false ->
            #buf_info{buf_name=BName}=NewBufInfo = get_data_disk(TableName, PageId),
            ets:insert(buf_info_list, {BName, NewBufInfo}),
            io:format("cache miss. load table_name=~p, page_id=~p to [~p].~n", [TableName, PageId, NewBufInfo]),
            BName;
        BName ->
            io:format("cache hit [~p].~n", [BName]),
            BName
    end,
    {reply, get_data_buf(BufName, Oid), State}.
    

handle_cast({get_config}, []) ->
    {noreply, []}.

handle_info(Msg, State) ->
    io:format("Unexpected message: ~p~n", [Msg]),
    {noreply, State}.

terminate(normal, _State) ->
    io:format("Server teminated.~n"),
    ok.

%% 取得したいデータがバッファに格納されているかチェックする
%% -> false | BufName
is_data_in_buf(TableName, PageId) ->
    case ets:match_object(buf_info_list, {'_', #buf_info{buf_name='_', table_name=TableName, page_id=PageId, empty_size='_', slot_count='_'}}) of
        [] -> false;
        [{BufName, _BufInfo}] ->
            BufName
    end.

%% バッファETSのキーをOidから作成する
get_data_key(#oid{table_name=TableName, page_id=PageId, slot=Slot}) ->
    list_to_atom(atom_to_list(TableName)++"_"++integer_to_list(PageId)++"_"++integer_to_list(Slot)).

%% バッファからデータを取り出す
get_data_buf(BufName, Oid) ->
    case ets:lookup(BufName, get_data_key(Oid)) of
        [] -> item_not_found;
        [{_Oid, []}] ->
            item_deleted;
        [{_Oid, Val}] ->
            Val
    end.

%% ディスクからデータを抽出して、バッファに格納する
%% バッファには、Oidを元に作成したキーとディスクから抽出したバリューをリスト形式で格納する
get_data_disk(TableName, PageId) ->
    EmptyBuf = get_empty_buf(),
    Fd = get_fd(TableName),
    {ok, #disk_data{empty_size=EmptySize, slot_count=SlotCount, data_list=SlotDataList}} = file_mng:load_page(Fd, PageId),
    lists:map(fun(#slot{slot_n=Slot, data=Data}) ->
        ets:insert(EmptyBuf, {get_data_key(#oid{table_name=TableName, page_id=PageId, slot=Slot}), Data}) end,
        SlotDataList),
    #buf_info{buf_name=EmptyBuf, table_name=TableName, page_id=PageId, empty_size=EmptySize, slot_count=SlotCount}.

%% ディスクからデータを読み込むための空きバッファを返す
%% 全てのバッファが埋まっている時の戦略は要検討
get_empty_buf() ->
    case ets:match_object(buf_info_list, {'_', #buf_info{buf_name='_', table_name=nil, page_id='_'}}) of
        [] ->
            think_which_buf_select;
        [{BufName, _BufInfo}] ->
            BufName
    end.

%% テーブルに応じて、データファイルのFdを取り出す
get_fd(TableName) ->
    case ets:lookup(fd_tables, TableName) of
        [] ->
            {ok, Fd} = file_mng:open(get_filename(TableName)),
            ets:insert(fd_tables, {TableName, Fd}),
            Fd;
        [_TableName, Fd] ->
            Fd
    end.

get_filename(TableName) ->
    %% TODO:テーブルとファイル名の紐付けは文字列操作でよいか(テーブルの存在チェックなど)
    atom_to_list(TableName) ++ ".dat".
