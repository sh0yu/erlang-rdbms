-module(query_executor).
-compile(export_all).
-behaviour(gen_server).
-record(state, {sdsPid, txMngPid, txid, lKvstore, lColumnIndex, msQueryLog, queryId}).
-include_lib("kernel/include/logger.hrl").

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], [{debug, [log]}]).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public APIs.
% Client program call these APIs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Pid: query_executorサーバのPID
%% CPid: クライアントのPID
%% Query: SQLクエリ
exec_query(Pid, Query) ->
    gen_server:call(Pid, {exec_query, Query}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Callback functions of gen_server.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    SdsPid = whereis(simple_db_server),
    TxMngPid = whereis(tx_mng),
    {LKvstore, LColumnIndex, MsQueryLog} = create_local_tables(),
    {ok, #state{sdsPid = SdsPid, txMngPid = TxMngPid, lKvstore = LKvstore,
    lColumnIndex = LColumnIndex, msQueryLog = MsQueryLog, queryId = []}}.

handle_call({exec_query, {allow_tx}}, _From, State)->
    TPid = get_tx_mng_pid(State),
    Txid = get_txid(State),
    Rep = tx_mng:allow_tx(TPid, Txid),
    {reply, Rep, State};
handle_call({exec_query, {begin_tx}}, _From, State)->
    TPid = get_tx_mng_pid(State),
    %% トランザクションを開始する
    Txid = tx_mng:begin_tx(TPid),
    {reply, Txid, State#state{txid = Txid}};
handle_call({exec_query, {commit_tx}}, _From, State)->
    %% トランザクションが許可されている場合のみ次に進める
    case ask_transaction(State) of
        transaction_not_found ->
            {reply, transaction_not_found, State};
        ok -> 
            TPid = get_tx_mng_pid(State),
            Txid = get_txid(State),
            QueryIdList = get_query_id_list(State),
            lists:map(fun(QueryId) -> commit_local_index(State, QueryId) end, QueryIdList),
            lists:map(fun(QueryId) -> commit_kvstore(State, QueryId) end, QueryIdList),
            Rep = tx_mng:commit_tx(TPid, Txid),
            {reply, Rep, State#state{txid=undefined, queryId = []}}
    end;
handle_call({exec_query, {rollback_tx}}, _From, State)->
    %% トランザクションが許可されている場合のみ次に進める
    case ask_transaction(State) of
        transaction_not_found ->
            {reply, transaction_not_found, State};
        ok -> 
            TPid = get_tx_mng_pid(State),
            Txid = get_txid(State),
            QueryIdList = get_query_id_list(State),
            lists:map(fun(QueryId) -> rollback_local_index(State, QueryId) end, QueryIdList),
            lists:map(fun(QueryId) -> rollback_kvstore(State, QueryId) end, QueryIdList),
            Rep = tx_mng:rollback_tx(TPid, Txid),
            {reply, Rep, State#state{txid=undefined, queryId = []}}
    end;
handle_call({exec_query, {create_table, TableName, ColumnList}}, _From, State)->
    SPid = get_db_pid(State),
    Rep = simple_db_server:create_table(SPid, TableName, ColumnList),
    {reply, Rep, State};
handle_call({exec_query, {drop_table, TableName}}, _From, State)->
    SPid = get_db_pid(State),
    Rep = simple_db_server:drop_table(SPid, TableName),
    {reply, Rep, State};
handle_call({exec_query, {insert, TableName, Val}}, _From, State)->
    %% トランザクションが許可されている場合のみ次に進める
    case ask_transaction(State) of
        transaction_not_found ->
            {reply, transaction_not_found, State};
        ok -> 
        % timer:sleep(10000),
            QueryId = generate_query_id(),
            ObjectId = generate_object_id(),
            case check_table_exists(TableName, Val) of
                true ->
                    local_insert_data(State, QueryId, TableName, ObjectId, Val),
                    {reply, QueryId, State#state{queryId = get_query_id_list(State) ++ [QueryId]}};
                _ -> {reply, table_not_found, State}
            end
    end;
handle_call({exec_query, {select, TableName, ColName, Val}}, _From, State)->
    %% トランザクションが許可されている場合のみ次に進める
    case ask_transaction(State) of
        transaction_not_found ->
            {reply, transaction_not_found, State};
        ok -> 
            SPid = get_db_pid(State),
            QueryIdList = get_query_id_list(State),
            %% オブジェクトIDを取得する
            OidList = select_object_id_list(State, SPid, TableName, ColName, Val, QueryIdList),
            %% オブジェクトIDから値を取得する
            {reply, select_data(State, SPid, TableName, OidList, QueryIdList), State}
    end;
handle_call({exec_query, {update, TableName, SetQuery, ColName, Val}}, _From, State)->
    %% トランザクションが許可されている場合のみ次に進める
    case ask_transaction(State) of
        transaction_not_found ->
            {reply, transaction_not_found, State};
        ok -> 
            QueryId = generate_query_id(),
            LKvstore = get_local_kvstore(State),
            LColumnIndex = get_local_column_index(State),
            SPid = get_db_pid(State),
            QueryIdList = get_query_id_list(State),
            ColumnList = simple_db_server:get_column_list(TableName),
            SetQueryConverted = simple_db_server:convert_set_query(SetQuery, ColumnList),
            OidList = select_object_id_list(State, SPid, TableName, ColName, Val, QueryIdList),
            F = fun(Oid) ->
                %% OldVal -> [banana, 100]
                %% NewVal -> [apple, 100]
                OldVal = select_data(State, SPid, TableName, Oid, QueryIdList),
                OldValWithCol = lists:zip(simple_db_server:get_column_list(TableName),OldVal),
                NewVal = simple_db_server:build_new_val(OldVal, SetQueryConverted),
                %% 更新前値をローカルデータから削除
                ets:insert(LKvstore, {QueryId, del, TableName, Oid, OldVal}),
                %% 更新後値をローカルデータに挿入
                ets:insert(LKvstore, {QueryId, ins, TableName, Oid, NewVal}),
                %% 更新対象のカラムごとにカラムインデックスを更新する
                %% ex. {name, apple}
                FF = fun({ColumnN, NewColVal}) ->
                    {_Key, OldColVal} = lists:keyfind(ColumnN, 1, OldValWithCol),
                    ets:insert(LColumnIndex, {QueryId, del, TableName, ColumnN, OldColVal, Oid}),
                    ets:insert(LColumnIndex, {QueryId, ins, TableName, ColumnN, NewColVal, Oid})
                end,
                lists:map(FF, SetQuery)
            end,
            lists:map(F, OidList),
            {reply, ok, State#state{queryId = QueryIdList ++ [QueryId]}}
    end;
handle_call({exec_query, {delete, TableName, ColName, Val}}, _From, State)->
    %% トランザクションが許可されている場合のみ次に進める
    case ask_transaction(State) of
        transaction_not_found ->
            {reply, transaction_not_found, State};
        ok -> 
            QueryId = generate_query_id(),
            io:format("QueryId: ~p~n", [QueryId]),
            SPid = get_db_pid(State),
            QueryIdList = get_query_id_list(State),
            %% オブジェクトIDを取得する
            OidList = select_object_id_list(State, SPid, TableName, ColName, Val, QueryIdList),
            lists:map(fun(Oid) -> local_delete_data(State, QueryId, TableName, Oid, select_data(State, SPid, TableName, Oid, QueryIdList)) end,
            OidList),
            {reply, ok, State#state{queryId = QueryIdList ++ [QueryId]}}
    end;
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
% State Mng funcs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
get_db_pid(State) ->
    State#state.sdsPid.

get_tx_mng_pid(State) ->
    State#state.txMngPid.

get_txid(State) ->
    State#state.txid.

get_local_kvstore(State) ->
    State#state.lKvstore.

get_local_column_index(State) ->
    State#state.lColumnIndex.

get_ms_query_log(State) ->
    State#state.msQueryLog.

get_query_id_list(State) ->
    State#state.queryId.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Local Table mng funcs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% ローカルデータ領域にinsタグのついたデータを挿入する
local_insert_data(State, QueryId, TableName, Oid, Val) ->
    LKvstore = get_local_kvstore(State),
    LColumnIndex = get_local_column_index(State),
    ColList = simple_db_server:get_column_list(TableName),
    if
        ColList =:= not_found -> not_found;
        true ->
            lists:map(fun({ColName, ColVal}) ->
                ets:insert(LColumnIndex, {QueryId, ins, TableName, ColName, ColVal, Oid}) end,
                lists:zip(ColList, Val)),
            ets:insert(LKvstore, {QueryId, ins, TableName, Oid, Val})
    end.
%% ローカルデータ領域にdelタグのついたデータを挿入する
local_delete_data(State, QueryId, TableName, Oid, Val) ->
    LKvstore = get_local_kvstore(State),
    LColumnIndex = get_local_column_index(State),
    ColList = simple_db_server:get_column_list(TableName),
    if
        ColList =:= not_found -> not_found;
        true ->
            lists:map(fun({ColName, ColVal}) ->
                ets:insert(LColumnIndex, {QueryId, del, TableName, ColName, ColVal, Oid}) end,
                lists:zip(ColList, Val)),
            ets:insert(LKvstore, {QueryId, del, TableName, Oid, Val})
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Util funcs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% トランザクションが実行可能かチェックする
ask_transaction(#state{txid=undefined}) ->
    transaction_not_found;
ask_transaction(#state{txMngPid=TPid, txid=Txid}) ->
    tx_mng:allow_tx(TPid, Txid).
    
%% 各トランザクションがローカルで保持するデータを持つためのテーブル作成
create_local_tables() ->
    LKvstore = ets:new(local_kvstore, [bag]),
    LColumnIndex = ets:new(local_column_index, [bag]),
    MsQueryLog = ets:new(ms_query_log, [set]),
    {LKvstore, LColumnIndex, MsQueryLog}.

%% オブジェクトID生成する
generate_object_id() ->
    erlang:system_time(nanosecond).

%% クエリIDを生成する
generate_query_id() ->
    erlang:system_time(nanosecond).

%% 共有領域にテーブルとカラムが存在するかチェックする
check_table_exists(TableName, Val) ->
    %% TODO: 実装
    true.

%% オブジェクトIDのリストを取得する
%% 共有データを検索し、さらにローカルデータを検索する
select_object_id_list(State, SPid, TableName, ColName, Val, QueryIdList) ->
    io:format("QueryIdList: ~p~n", [QueryIdList]),
    %% 共有データを検索
    ShareData = simple_db_server:select_column_index(TableName, ColName, Val),
    %% QueryIdListの順にローカルデータを検索してマージする
    lists:foldl(fun(QueryId, SData) -> merge_local_index(State, TableName, ColName, Val, SData, QueryId) end,
    ShareData, QueryIdList).

%% 共有データから取得した値に対して、ローカルデータをマージしていく
merge_local_index(State, TableName, ColName, Val, ShareData, QueryId) ->
    LColumnIndex = get_local_column_index(State),
    case ets:match_object(LColumnIndex, {QueryId, '_', TableName, ColName, Val, '_'}) of 
        [] -> ShareData;
        LocalDataList ->
            lists:foldl(fun(LocalData, SData) -> merge_local_index_local(SData, LocalData) end,
            ShareData, LocalDataList)
    end.

merge_local_index_local(ShareData, {_QueryId, ins, _TableName, _ColName, _Val, Oid}) ->
    [Oid | ShareData];
merge_local_index_local(ShareData, {_QueryId, del, _TableName, _ColName, _Val, Oid}) ->
    lists:filter(fun(X) -> X =/= Oid end, ShareData).

%% オブジェクトIDを使ってデータを取得する
%% 共有データとローカルデータからデータを取得し、マージして返却する
select_data(State, SPid, TableName, OidList, QueryIdList) when is_list(OidList)->
    lists:map(fun(Oid) -> select_data(State, SPid, TableName, Oid, QueryIdList) end, OidList);
select_data(State, SPid, TableName, Oid, QueryIdList) ->
    ShareData = simple_db_server:select_kvstore(TableName, Oid),
    io:format("ShareData: ~p~n", [ShareData]),
    lists:foldl(fun(QueryId, SData) -> merge_local_data(State, SData, Oid, QueryId) end,
    ShareData, QueryIdList).

%% ローカルデータの値で上書きしていく
merge_local_data(State, ShareData, Oid, QueryId) ->
    LKvstore = get_local_kvstore(State),
    case ets:match_object(LKvstore, {QueryId, '_', '_', Oid, '_'}) of
        [] -> ShareData;
        LocalData -> merge_local_index_local(LocalData)
    end.

%% insタグのついたローカルデータがある場合は値を上書きする
%% delタグのついたローカルデータの場合は値は上書きしない
merge_local_index_local([]) ->
    [];
merge_local_index_local([{_QueryId, ins, _TableName, _Oid, Val} | _LocalData])->
    Val;
merge_local_index_local([{_QueryId, del, _TableName, _Oid, _Val} | LocalData]) -> 
    merge_local_index_local(LocalData).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Commit funcs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
commit_local_index(State, QueryId) ->
    LColumnIndex = get_local_column_index(State),
    LocalDataList = ets:lookup(LColumnIndex, QueryId),
    DelList = lists:filter(fun({_QueryId, Act, _TableName, _ColName, _Val, _Oid}) -> Act =:= del end, LocalDataList),
    InsList = lists:filter(fun({_QueryId, Act, _TableName, _ColName, _Val, _Oid}) -> Act =:= ins end, LocalDataList),
    io:format("DelListIndex: ~p, InsListIndex: ~p~n", [DelList, InsList]),
    FDel = fun({_QueryId, del, TableName, ColName, Val, Oid}) ->
        ColumnIndexId = simple_db_server:get_column_index_id(TableName, ColName),
        simple_db_server:delete_column_index(ColumnIndexId, Val, Oid)
    end,
    FIns = fun({_QueryId, ins, TableName, ColName, Val, Oid}) ->
        ColumnIndexId = simple_db_server:get_column_index_id(TableName, ColName),
        simple_db_server:insert_column_index(ColumnIndexId, Val, Oid)
    end,
    lists:map(FDel, DelList),
    lists:map(FIns, InsList),
    ets:delete(LColumnIndex, QueryId).

commit_kvstore(State, QueryId) ->
    LKvstore = get_local_kvstore(State),
    LocalDataList = ets:lookup(LKvstore, QueryId),
    DelList = lists:filter(fun({_QueryId, Act, _TableName, _Oid, _Val}) -> Act =:= del end, LocalDataList),
    InsList = lists:filter(fun({_QueryId, Act, _TableName, _Oid, _Val}) -> Act =:= ins end, LocalDataList),
    io:format("DelListKvstore: ~p, InsListKvstore: ~p~n", [DelList, InsList]),
    FDel = fun({_QueryId, del, TableName, Oid, Val}) ->
        simple_db_server:insert_kvstore(TableName, Oid, Val)
    end,
    FIns = fun({_QueryId, ins, TableName, Oid, Val}) ->
        simple_db_server:insert_kvstore(TableName, Oid, Val)
    end,
    lists:map(FDel, DelList),
    lists:map(FIns, InsList),
    ets:delete(LKvstore, QueryId).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Rollback funcs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
rollback_local_index(State, QueryId) ->
    LColumnIndex = get_local_column_index(State),
    ets:delete(LColumnIndex, QueryId).

rollback_kvstore(State, QueryId) ->
    LKvstore = get_local_kvstore(State),
    ets:delete(LKvstore, QueryId).