-module(simple_db_server).
-compile(export_all).
-behaviour(gen_server).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

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
    %% システムテーブル作成
    create_system_tables(),
    {ok, []}.

handle_call({create_table, {TableName, ColumnList}}, _From, _State) ->
    register_table(TableName, ColumnList),
    register_column(TableName, ColumnList),
    {reply, ok, []};
handle_call({drop_table, {TableName}}, _From, _State) ->
    ColumnList = get_column_list(TableName),
    unregister_table(TableName),
    unregister_column(TableName, ColumnList),
    {reply, ok, []};
handle_call({insert, {TableName, Val}}, _From, _State) ->
    Oid = generate_object_id(),
    insert_kvstore(TableName, Oid, Val),
    insert_all_column_index(TableName, Val, Oid),
    {reply, Oid, []};
handle_call({select, {TableName, ColumnName, Val}}, _From, _State) ->
    OidList = select_object_id_list(TableName, ColumnName, Val),
    case select_kvstore(TableName, OidList) of
            table_not_found -> table_not_found;
            [] -> {reply, not_found, []};
            RetVal -> {reply, RetVal, []}
    end;
handle_call({update, {TableName, SetQuery, ColumnName, Val}}, _From, _State) ->
    OidList = select_object_id_list(TableName, ColumnName, Val),
    ColumnList = get_column_list(TableName),
    SetQueryConverted = convert_set_query(SetQuery, ColumnList),
    F = fun(Oid) ->
        %% OldVal -> [banana, 100]
        %% NewVal -> [apple, 100]
        OldVal = select_kvstore(TableName, Oid),
        OldValWithCol = select_kvstore_with_column(TableName, Oid),
        NewVal = build_new_val(OldVal, SetQueryConverted),
        %% kvstoreを更新する
        update_kvstore(TableName, Oid, NewVal),
        %% 更新対象のカラムごとにカラムインデックスを更新する
        %% ex. {name, apple}
        FF = fun({ColumnN, NewColVal}) ->
            ColumnIndexId = get_column_index_id(TableName, ColumnN),
            {_Key, OldColVal} = lists:keyfind(ColumnN, 1, OldValWithCol),
            update_column_index(ColumnIndexId, OldColVal, NewColVal, Oid)
        end,
        lists:map(FF, SetQuery)
    end,
    lists:map(F, OidList),
    {reply, ok, []};
handle_call({delete, {TableName, ColumnName, Val}}, _From, _State) ->
    OidList = select_object_id_list(TableName, ColumnName, Val),
    ColumnList = get_column_list(TableName),
    F = fun(Oid) ->
        KvVal = select_kvstore(TableName, Oid),
        %% kvstoreを削除する
        delete_kvstore(TableName, Oid),
        %% 各カラムごとにカラムインデックスを削除更新する
        %% ex. {name, apple}
        FF = fun({ColumnN, ColVal}) ->
            %% 古いインデックス情報を削除する
            ColumnIndexId = get_column_index_id(TableName, ColumnN),
            delete_column_index(ColumnIndexId, ColVal, Oid)
        end,
        lists:map(FF, lists:zip(ColumnList, KvVal))
    end,
    lists:map(F, OidList),
    {reply, ok, []};
handle_call(terminate, _From, _State) ->
    {stop, normal, ok, []}.

handle_cast({get_config}, []) ->
    {noreply, []}.

handle_info(Msg, _State) ->
    io:format("Unexpected message: ~p~n", [Msg]),
    {noreply, _State}.

terminate(normal, _State) ->
    io:format("Server teminated.~n"),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% ColumnIndex mng functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% カラムインデックスを挿入する
insert_column_index(ColumnIndexId, Val, Oid) ->
    case ets:lookup(ColumnIndexId, Val) of
        %% カラムの値が新規登録されるパターン
        [] -> ets:insert(ColumnIndexId, {Val, [Oid]});
        %% すでにカラムの値が登録されているパターン
        %% 既存のオブジェクトIDのリストに新しいオブジェクトIDを追加する
        [{_Val, OidList}] ->
            case lists:member(Oid, OidList) of
                false ->
                    ets:insert(ColumnIndexId, {Val, [Oid | OidList]});
                %% ただし、すでに同じオブジェクトIDが登録されている場合は追加しない
                true -> nop
            end
    end.

%% カラムインデックスを更新する
update_column_index(ColumnIndexId, OldVal, NewVal, Oid) ->
    %% 更新前の値に紐づくオブジェクトIDを削除する
    delete_column_index(ColumnIndexId, OldVal, Oid),
    insert_column_index(ColumnIndexId, NewVal, Oid).

%% カラムインデックスからオブジェクトIDを削除する
delete_column_index(ColumnIndexId, Val, Oid) ->
    case ets:lookup(ColumnIndexId, Val) of
        [] -> nop;
        [{_Val, OidList}] ->
            NewOidList = lists:filter(fun(X) -> X =/= Oid end, OidList),
            case NewOidList of
                [] -> ets:delete(ColumnIndexId, Val);
                _ -> ets:insert(ColumnIndexId, {Val, NewOidList})
            end
    end.

%% カラムインデックスからオブジェクトIDを取得する
select_column_index(ColumnIndexId, Val) ->
    ets:lookup(ColumnIndexId, Val).

%% カラムインデックスを作成する
create_column_index(ColumnIndexName) ->
    ets:new(ColumnIndexName, [set, named_table, public]).

%% カラムインデックスを削除する
drop_column_index(ColumnIndexId) ->
    ets:delete(ColumnIndexId).

%% 複数のカラムインデックスに値を挿入する
insert_all_column_index(TableName, ValList, Oid) ->
    ColumnList = get_column_list(TableName),
    InsertColumnIndex = fun({ColumnName, ColumnVal}) ->
        ColumnIndexId = get_column_index_id(TableName, ColumnName),
        insert_column_index(ColumnIndexId, ColumnVal, Oid)
    end,
    lists:map(InsertColumnIndex, lists:zip(ColumnList, ValList)).
    
%% 指定されたテーブルで条件に当てはまるオブジェクトIDのリストを取得する
select_object_id_list(TableName, ColumnName, Val) ->
    ColumnIndexId = get_column_index_id(TableName, ColumnName),
    case ets:lookup(ColumnIndexId, Val) of
        [] -> [];
        [{_Val, OidList}] -> OidList
    end.
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Datastore mng functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
create_kvstore(TableName) ->
    ets:new(TableName, [set, named_table, public]).

drop_kvstore(TableName) ->
    ets:delete(TableName).

insert_kvstore(TableName, Oid, Val) ->
    case get_table_id(TableName) of
        not_found -> table_not_found;
        TableId -> ets:insert(TableId, {Oid, Val})
    end.

select_kvstore(TableName, OidList) when is_list(OidList) ->
    lists:map(fun(Oid) ->
        select_kvstore(TableName, Oid) end,
        OidList);
select_kvstore(TableName, Oid) ->
    case get_table_id(TableName) of
        not_found -> table_not_found;
        TableId ->
            case ets:lookup(TableId, Oid) of
            [] -> [];
            [{_Oid, Val}] -> Val
            end        
    end.

select_kvstore_with_column(TableName, Oid) ->
        case get_table_id(TableName) of
                not_found -> table_not_found;
                TableId ->
                    case ets:lookup(TableId, Oid) of
                    [] -> [];
                    [{_Oid, Val}] -> Val,
                    ColumnList = get_column_list(TableName),
                    lists:zip(ColumnList, Val)
                    end        
            end.
        
update_kvstore(TableName, Oid, Val) ->
    case get_table_id(TableName) of
        not_found -> table_not_found;
        TableId -> ets:insert(TableId, {Oid, Val})
    end.

delete_kvstore(TableName, Oid) ->
    case get_table_id(TableName) of
        not_found -> table_not_found;
        TableId -> ets:delete(TableId, Oid)
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% System Table mng functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% システムテーブルを作成する
create_system_tables() ->
    ets:new(ms_tables, [set, named_table, public]),
    ets:new(ms_tab_columns, [set, named_table, public]).

%% テーブルを作成して、ms_tablesにテーブルを登録する
register_table(TableName, ColumnList) ->
    TableId = create_kvstore(TableName),
    ets:insert(ms_tables, {TableName, TableId, ColumnList}).

%% ms_tablesからテーブル情報を削除し、テーブルを削除する
unregister_table(TableName) ->
    TableId = get_table_id(TableName),
    ets:delete(ms_tables, TableId),
    drop_kvstore(TableName).

%% テーブル名からテーブルIDを取得する
get_table_id(TableName)->
    ets:lookup_element(ms_tables, TableName, 2).

%% ms_tab_columnsにカラムを登録する
register_column(TableName, ColumnNameList) when is_list(ColumnNameList) ->
    lists:map(fun(ColumnName) -> 
        register_column(TableName, ColumnName) end,
        ColumnNameList);
register_column(TableName, ColumnName) ->
    ColumnIndexName = get_tab_column_key(TableName, ColumnName),
    ColumnIndexId = create_column_index(ColumnIndexName),
    ets:insert(ms_tab_columns, {ColumnIndexName, TableName, ColumnName, ColumnIndexId}).

%% ms_tab_columnsからカラムを削除する
unregister_column(TableName, ColumnNameList) when is_list(ColumnNameList) ->
    lists:map(fun(ColumnName) ->
        unregister_column(TableName, ColumnName) end,
        ColumnNameList);
unregister_column(TableName, ColumnName) ->
    ColumnIndexName = get_tab_column_key(TableName, ColumnName),
    ets:delete(ms_tab_columns, ColumnIndexName),
    drop_column_index(ColumnIndexName).

get_column_list(TableName) ->
    case ets:lookup_element(ms_tables, TableName, 3) of
        [] -> not_found;
        ColumnList -> ColumnList
end.

get_column_index_id(TableName, ColumnName) ->
    case ets:lookup_element(ms_tab_columns, get_tab_column_key(TableName, ColumnName), 4) of
        [] -> not_found;
        ColumnIndexId -> ColumnIndexId
end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% util functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% オブジェクトIDを取得する
generate_object_id() ->
    erlang:system_time(nanosecond).

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