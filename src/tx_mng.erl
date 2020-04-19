-module(tx_mng).
-compile(export_all).
-behaviour(gen_server).
-include_lib("kernel/include/logger.hrl").
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

begin_tx(Pid) ->
    gen_server:call(Pid, {begin_tx}).

rollback_tx(Pid, Txid) ->
    gen_server:call(Pid, {rollback_tx, Txid}).

commit_tx(Pid, Txid) ->
    gen_server:call(Pid, {commit_tx, Txid}).        

allow_tx(Pid, Txid) ->
    gen_server:call(Pid, {allow_tx, Txid}, infinity).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Callback functions of gen_server.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    %% loggerの設定
    logger:set_primary_config(level, info),
    %% システムテーブル作成
    ets:new(ms_tx_mng, [set, named_table, public]),
    ets:new(ms_waiting_proc, [set, named_table, public]),
    {ok, []}.

handle_call(terminate, _From, _State) ->
    {stop, normal, ok, []};
handle_call({begin_tx}, _From, _State) ->
    Txid = generate_txid(),
    % ?LOG_INFO("TransactionId:~p", [Txid]),
    register_tx(Txid),
    {reply, Txid,[]};
handle_call({allow_tx, Txid}, From, _State) ->
    case is_active_tx(Txid) of
        true -> {reply, ok, []};
        false -> 
            stack_waiting_process(Txid, From),
            {noreply, [], infinity};
        transaction_not_found ->
            {reply, transaction_not_found, []}
    end;
handle_call({commit_tx, Txid}, _From, _State) ->
    commit_tx(Txid),
    {reply, ok, []};
handle_call({rollback_tx, Txid}, _From, _State) ->
    rollback_tx(Txid),
    {reply, ok, []}.
                        
handle_cast({get_config}, []) ->
    {noreply, []}.

handle_info(Msg, _State) ->
    io:format("Unexpected message: ~p~n", [Msg]),
    {noreply, _State}.

terminate(normal, _State) ->
    io:format("Server teminated.~n"),
    ok.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Transaction mng functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% 新規トランザクションを登録する
register_tx(Txid) ->
    Timestamp = get_timestamp(),
    ets:insert(ms_tx_mng, {Txid, Timestamp, inactive}),
    activate_tx().

%% トランザクションをアクティベートする
%% トランザクションたちはtx_mngテーブルに登録されている
%% activeとなっているトランザクションが1つもない場合のみ、
%% inactiveとなっているトランザクションのうち一番古いものをactiveにする
activate_tx() ->
    case ets:match_object(ms_tx_mng, {'_', '_', active}) of 
        %% activeなトランザクションが存在する
        [_] -> 
            transaction_not_acquired;
        %% activeになっているトランザクションがいない
        [] -> 
            case ets:match_object(ms_tx_mng, {'_', '_', inactive}) of 
                %% inactiveなトランザクションが存在しない場合、なにもしない
                [] -> no_transaction_waiting;
                %% inactiveのトランザクションが存在する場合、一番古いトランザクションをactiveにする
                %% アクティベートされたトランザクションに通知する
                InactiveTxList ->
                    {OldestTxid, OldestTimestamp, _Status} = get_oldest_tx(InactiveTxList),
                    ets:insert(ms_tx_mng, {OldestTxid, OldestTimestamp, active}),
                    notify_tx_active(ets:lookup(ms_waiting_proc, OldestTxid))
            end
    end.

%% トランザクションを完了させる
%% ステータスをcommittedにする
commit_tx(Txid) ->
    case ets:lookup(ms_tx_mng, Txid) of
        [{Txid, Timestamp, _Status}] ->
            ets:insert(ms_tx_mng, {Txid, Timestamp, committed});
        [] -> transaction_not_found
    end,
    activate_tx().

%% トランザクションをロールバックする
%% ステータスをabortedにする
rollback_tx(Txid) ->
    case ets:lookup(ms_tx_mng, Txid) of
       [{Txid, Timestamp, _Status}] ->
            ets:insert(ms_tx_mng, {Txid, Timestamp, aborted});
        [] -> transaction_not_found
    end,
    activate_tx().
    
%% トランザクションのリストを受け取って、一番古いトランザクションを返す
%% tx = {TxId, Timestamp, Status}
get_oldest_tx(TxList) ->
    Sub = fun Sub([], Item) -> Item;
            Sub(TxL, {_, OTime, _}=Item) ->
                case hd(TxL) of
                    {_, Time, _} when Time < OTime ->
                        Sub(tl(TxL), hd(TxL));
                    _ ->
                        Sub(tl(TxL), Item)
                end
            end,
    Sub(tl(TxList), hd(TxList)).

%% トランザクションIDを取得する
generate_txid() ->
    erlang:system_time(nanosecond).

%% タイムスタンプを取得する
get_timestamp() ->
    erlang:system_time(nanosecond).

%% トランザクションがactiveの場合のみtrue
is_active_tx(Txid) ->
    case ets:lookup(ms_tx_mng, Txid) of
        [] -> transaction_not_found;
        [{Txid, _Timestamp, active}] -> true;
        [{Txid, _Timestamp, _}] -> false
    end.

%% トランザクションの実行許可待ちリスト
%% 実行許可依頼がリクエストされた際に、トランザクションがactiveではない場合に追加される
stack_waiting_process(Txid, From) ->
    ets:insert(ms_waiting_proc, {Txid, From}).

notify_tx_active([]) ->
    transaction_not_found;
notify_tx_active([{Txid, From}]) ->
    ets:delete(ms_waiting_proc, Txid),
    gen_server:reply(From, ok).