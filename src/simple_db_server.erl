-module(simple_db_server).
-compile(export_all).
-behaviour(gen_server).

-include("../include/simple_db_server.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% DB Server APIs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
create_table(Pid, TableName, ColumnList) ->
    gen_server:call(Pid, {create_table, {TableName, ColumnList}}).

drop_table(Pid, TableName) ->
    gen_server:call(Pid, {drop_table, {TableName}}).

insert_data(Pid, TableName, Val) ->
    gen_server:call(Pid, {insert, {TableName, Val}}).

select_data(Pid, TableName, ColName, Val) ->
    gen_server:call(Pid, {select, {TableName, ColName, Val}}).

update_data(Pid, TableName, SetQuery, ColName, Val) ->
    gen_server:call(Pid, {update, {TableName, SetQuery, ColName, Val}}).
    
delete_data(Pid, TableName, ColName, Val) ->
    gen_server:call(Pid, {delete, {TableName, ColName, Val}}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Callback functions of gen_server.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    index:init(),
    {ok, []}.

handle_call({create_table, {TableName, ColumnList}}, _From, State) ->
    ok = sys_tbl_mng:create_table(whereis(sys_tbl_mng), TableName, ColumnList),
    ok = index:create_table(TableName, ColumnList),
    {reply, ok, State};

handle_call({drop_table, {TableName}}, _From, State) ->
    ok = sys_tbl_mng:drop_table(whereis(sys_tbl_mng), TableName),
    {reply, ok, State};

handle_call({insert, {TableName, Val}}, _From, State) ->
    case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
        false -> {reply, {error, table_not_found}, State};
        true ->
            {ok, Oid} = data_buffer:write_data(whereis(data_buffer), TableName, Val),
            {ok, IndexColumnList} = sys_tbl_mng:get_index_column_list(whereis(sys_tbl_mng), TableName),
            ok = index:insert_index(TableName, lists:zip(IndexColumnList, Val), Oid),
            {reply, Oid, State}
    end;

handle_call({select, {TableName, ColumnName, Val}}, _From, State) ->
    OidList = index:select_index(TableName, ColumnName, Val),
    case read_data_oid(TableName, OidList) of
            table_not_found -> table_not_found;
            [] -> {reply, not_found, State};
            RetVal -> {reply, RetVal, State}
    end;

handle_call({update, {TableName, SetQuery, ColumnName, Val}}, _From, State) ->
    OidList = index:get_oid(TableName, ColumnName, Val),
    {ok, ColumnList} = sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName),
    SetQueryConverted = convert_set_query(SetQuery, ColumnList),
    F = fun(Oid) ->
        %% OldVal -> [banana, 100]
        %% NewVal -> [apple, 100]
        OldVal = read_data_oid(TableName, Oid),
        OldValWithCol = read_data_oid_with_column(TableName, Oid),
        NewVal = build_new_val(OldVal, SetQueryConverted),
        %% kvstoreを更新する
        update_kvstore(TableName, Oid, NewVal)
        %% TODO:インデックスの更新処理
        %% 更新対象のカラムごとにカラムインデックスを更新する
        %% ex. {name, apple}
        % FF = fun({ColumnN, NewColVal}) ->
        %     ColumnIndexId = get_column_index_id(TableName, ColumnN),
        %     {_Key, OldColVal} = lists:keyfind(ColumnN, 1, OldValWithCol),
        %     update_column_index(ColumnIndexId, OldColVal, NewColVal, Oid)
        % end,
        % lists:map(FF, SetQuery)
    end,
    lists:map(F, OidList),
    {reply, ok, State};

handle_call({delete, {TableName, ColumnName, Val}}, _From, State) ->
    OidList = index:get_oid(TableName, ColumnName, Val),
    {ok, ColumnList} = sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName),
    F = fun(Oid) ->
        KvVal = read_data_oid(TableName, Oid),
        %% kvstoreを削除する
        delete_kvstore(TableName, Oid)
        %% TODO:インデックスの削除処理追加
        %% 各カラムごとにカラムインデックスを削除更新する
        %% ex. {name, apple}
        % FF = fun({ColumnN, ColVal}) ->
        %     %% 古いインデックス情報を削除する
        %     ColumnIndexId = get_column_index_id(TableName, ColumnN),
        %     delete_column_index(ColumnIndexId, ColVal, Oid)
        % end,
        % lists:map(FF, lists:zip(ColumnList, KvVal))
    end,
    lists:map(F, OidList),
    {reply, ok, State};

handle_call(terminate, _From, _State) ->
    {stop, normal, ok, []}.

handle_cast({get_config}, []) ->
    {noreply, []}.

handle_info(Msg, State) ->
    io:format("Unexpected message: ~p~n", [Msg]),
    {noreply, State}.

terminate(normal, _State) ->
    io:format("Server teminated.~n"),
    ok.
    
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Datastore mng functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
read_data_oid(TableName, OidList) when is_list(OidList) ->
    lists:map(fun(Oid) ->
        read_data_oid(TableName, Oid) end,
        OidList);
read_data_oid(TableName, Oid) ->
    case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
        false -> table_not_found;
        true -> data_buffer:read_data(whereis(data_buffer), Oid)
    end.

read_data_oid_with_column(TableName, Oid) ->
        case read_data_oid(TableName, Oid) of
            table_not_found -> table_not_found;
            Data ->
                {ok, ColumnList} = sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName),
                lists:zip(ColumnList, Data)
        end.
        
update_kvstore(TableName, Oid, Val) ->
    case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
        false -> table_not_found;
        true -> data_buffer:update_data(whereis(data_buffer), TableName, Oid, Val)
    end.

delete_kvstore(TableName, Oid) ->
    case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
        false -> table_not_found;
        true -> data_buffer:delete_data(whereis(data_buffer), TableName, Oid)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% System Table mng functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% システムテーブルを読み込む
load_system_tables() ->
    0.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% util functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% tab_column_keyを取得する
get_tab_column_key(TableName, ColumnNameList) when is_list(ColumnNameList) ->
    lists:map(fun(ColumnName) ->
        get_tab_column_key(TableName, ColumnName) end,
        ColumnNameList);
get_tab_column_key(TableName, ColumnName) ->
    TableNameStr = atom_to_list(TableName),
    ColumnNameStr = atom_to_list(ColumnName),
    list_to_atom(TableNameStr ++ "_" ++ ColumnNameStr).

%% OldVal -> [apple, 100]
%% SetQueryConverted -> [{1, banana}, {2, 200}]
build_new_val(OldVal, SetQueryConverted) ->
    Sub = fun Sub([], _SetQuery, Res) ->
                lists:reverse(Res);
            Sub(OldV, [{1, NewVal} | Rest], Res) ->
                Sub(tl(OldV), decrement_set_query(Rest), [NewVal | Res]);
            Sub(OldV, SetQ, Res) ->
                Sub(tl(OldV), decrement_set_query(SetQ), [hd(OldV) | Res])
            end,
    Sub(OldVal, SetQueryConverted, []).

%% decrement_set_query([{2, 200}, {3, apple}]) -> [{1, 200}, {2, apple}]
decrement_set_query([]) -> [];
decrement_set_query([{Num, Val} | T]) -> [{Num - 1, Val} | decrement_set_query(T)].

%% ([{name, banana}, {price, 200}], [name, price]) -> [{1, banana}, {2, 200}]
convert_set_query(SetQuery, ColumnList) ->
    %% ColumnListの先頭がSetQueryにあれば、タプルの左の値をカラムリストの番号に書き換える
    Sub = fun   Sub(_SetQ, [], _N, Res) ->
                    lists:reverse(Res);
                Sub(SetQ, ColumnL, N, Res) ->
                Key = hd(ColumnL),
                case lists:keymember(hd(ColumnL), 1, SetQ) of
                    true -> 
                        {_Key, Val} = lists:keyfind(Key, 1, SetQ),
                        Filter = fun (X) ->
                                case X of 
                                    ({Key, _Val}) -> false;
                                    (_) -> true
                                end
                            end,
                        Sub(lists:filter(Filter, SetQ), tl(ColumnL), N + 1, [{N, Val} | Res]);
                    false ->
                        Sub(SetQ, tl(ColumnL), N + 1, Res)
                end
            end,
    Sub(SetQuery, ColumnList, 1, []).