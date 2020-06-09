-module(sys_tbl_mng).
-compile(export_all).
-behaviour(gen_server).

-include("../include/simple_db_server.hrl").
-record(st, {
    ms_tables
}).
-record(table_info, {
    name,
    numcols
}).
-record(column_info, {
    tblid,
    num,
    name,
    type,
    length,
    option
}).

%%TODO:カラムの統計情報などを保持するよう改良

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public APIs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
create_table(Pid, TableName, ColumnList) ->
    gen_server:call(Pid, {create_table, TableName, ColumnList}).

drop_table(Pid, TableName) ->
    gen_server:call(Pid, {drop_table, TableName}).

get_column_list(Pid, TableName) ->
    gen_server:call(Pid, {get_column_list, TableName}).

get_index_column_list(Pid, TableName) ->
    gen_server:call(Pid, {get_column_list, TableName}).

exist_table(Pid, TableName) ->
    gen_server:call(Pid, {exist_table, TableName}).

init([]) ->
    {ok, Name} = dets:open_file(ms_tables, [{file, "./ms_tables.sys"}]),
    {ok, #st{ms_tables=Name}}.
    
handle_call({create_table, TableName, ColumnList}, _From, #st{ms_tables=MsTables}=State) ->
    dets:insert(MsTables, {TableName, ColumnList}),
    {reply, ok, State};

handle_call({drop_table, TableName}, _From, #st{ms_tables=MsTables}=State) ->
    dets:delete(MsTables, TableName),
    {reply, ok, State};

handle_call({get_column_list, TableName}, _From, #st{ms_tables=MsTables}=State) ->
    case dets:lookup(MsTables, TableName) of
        [] -> {reply, {error, table_not_found}, State};
        [{TableName, ColumnList}] ->
            {reply, {ok, ColumnList}, State}
    end;

handle_call({exist_table, TableName}, _From, #st{ms_tables=MsTables}=State) ->
    Ret = case dets:lookup(MsTables, TableName) of
        [] -> false;
        [_] -> true
    end,
    {reply, Ret, State};

handle_call({}, _From, State) ->
{reply, ok, State}.

handle_cast({}, State) ->
    {reply, ok, State}.

handle_info(Msg, State) ->
    io:format("Unexpected message: ~p~n", [Msg]),
    {noreply, State}.

terminate(normal, _State) ->
    io:format("Server teminated.~n"),
    ok.


%% TODO:fun渡したらうまく整形して返却してくれるようにソースを修正(システムファイルの読み込み方法は使い回せるようにしたい)
% load_ms_tables() ->
%     ets:new(ms_tables, [set, named_table, public]),
%     ets:new(ms_tab_columns, [bag, named_table, public]),
%     {ok, FdMsTables} = file_mng:open("ms_tables.sys", [sys]),
%     case file:read_file("ms_tables.sys") of
%         {ok, <<>>} -> {ok, FdMsTables};
%         {ok, Data} ->
%             TableInfoList = string:split(binary_to_list(Data), "\n", all),
%             lists:map(
%                 fun ([]) -> nop;
%                 (TableInfo) ->
%                     [TableName | ColumnList] = string:split(TableInfo, " ", all),
%                     TableNameAtom = list_to_atom(TableName),
%                     ColumnListAtom = lists:map(fun(X) -> list_to_atom(X) end, ColumnList),
%                     ets:insert(ms_tables, {TableNameAtom, #table_info{name=TableNameAtom}}),
%                     register_column(TableNameAtom, ColumnListAtom)
%                  end
%                 , TableInfoList),
%             {ok, FdMsTables}
%     end.

%% テーブルを作成して、ms_tablesにテーブルを登録する
% register_table(TableName, ColumnList, FdMsTables) ->
%     file_mng:append(FdMsTables, [TableName, ColumnList]),
%     ets:insert(ms_tables, {TableName, #table_info{name=TableName}}).

%% ms_tablesからテーブル情報を削除し、テーブルを削除する
%% TODO:ms_tablesファイルから1行削除する際は、ファイルリネーム→新規ファイルにドロップするテーブル以外のテーブル情報を書き込み
%% うまくいけばリネームしたファイルを削除する方式にしたい(ファイル削除してからプロセスが落ちた場合に復元できないから)
% unregister_table(TableName, FdMsTables) ->
%     AllTable = ets:match_object(ms_tables, {'_', '_'}),
%     RestTable = lists:filter(fun({TableNm, _}) -> TableNm =/= TableName end, AllTable),
%     %% ディスク上のms_tablesファイルを一度削除して、ドロップしたテーブル以外のテーブル情報を書き込む
%     file_mng:delete(FdMsTables),
%     lists:map(fun({TableNm, _}) ->
%         file_mng:append(FdMsTables, [TableNm])
%     end, RestTable),
%     ets:delete(ms_tables, TableName).

%% ms_tab_columnsにカラムを登録する
% register_column(TableName, ColumnNameList) when is_list(ColumnNameList) ->
%     lists:map(fun(ColumnName) -> 
%         register_column(TableName, ColumnName) end,
%         ColumnNameList);
% register_column(TableName, ColumnName) ->
%     ColumnIndexName = get_tab_column_key(TableName, ColumnName),
%     ColumnIndexId = create_column_index(ColumnIndexName),
%     ets:insert(ms_tab_columns, {ColumnIndexName, TableName, ColumnName, ColumnIndexId}).

% %% ms_tab_columnsからカラムを削除する
% unregister_column(TableName, ColumnNameList) when is_list(ColumnNameList) ->
%     lists:map(fun(ColumnName) ->
%         unregister_column(TableName, ColumnName) end,
%         ColumnNameList);
% unregister_column(TableName, ColumnName) ->
%     ColumnIndexName = get_tab_column_key(TableName, ColumnName),
%     ets:delete(ms_tab_columns, ColumnIndexName),
%     drop_column_index(ColumnIndexName).

% get_column_list(TableName) ->
%     case ets:lookup_element(ms_tables, TableName, 3) of
%         [] -> not_found;
%         ColumnList -> ColumnList
% end.

% get_column_index_id(TableName, ColumnName) ->
%     case ets:lookup_element(ms_tab_columns, get_tab_column_key(TableName, ColumnName), 4) of
%         [] -> not_found;
%         ColumnIndexId -> ColumnIndexId
% end.
