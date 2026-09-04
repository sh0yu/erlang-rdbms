%%%-------------------------------------------------------------------
%%% @doc
%%% トランザクション管理。
%%%
%%% 直列化の方針: 同時にactiveになれるトランザクションは1つだけ。
%%% begin_txした順(タイムスタンプ順)に1つずつactiveにしていき、
%%% activeでないトランザクションからのallow_txは、自分の順番が来るまで
%%% ブロックする。これにより全トランザクションが直列に実行される。
%%%
%%% トランザクションを開始したプロセスはmonitorする。異常終了した場合は
%%% そのトランザクションをabortして次のトランザクションに順番を渡す。
%%% これがないと、クラッシュしたプロセスが順番を握ったままDB全体が
%%% 止まってしまう。
%%% @end
%%%-------------------------------------------------------------------
-module(tx_mng).
-behaviour(gen_server).

%% Public API
-export([start_link/0, stop/1]).
-export([begin_tx/1, commit_tx/2, rollback_tx/2, allow_tx/2, tx_status/2,
         active_tx/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

%%%===================================================================
%%% Public API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%----------------------------------------------------------------------
%% @doc トランザクションを開始してIDを返す。
%%----------------------------------------------------------------------
begin_tx(Pid) ->
    gen_server:call(Pid, {begin_tx, self()}).

rollback_tx(Pid, Txid) ->
    gen_server:call(Pid, {rollback_tx, Txid}).

commit_tx(Pid, Txid) ->
    gen_server:call(Pid, {commit_tx, Txid}).

%%----------------------------------------------------------------------
%% @doc 自分のトランザクションの順番が来るまで待つ。
%% Returns: ok | transaction_not_found
%%----------------------------------------------------------------------
allow_tx(Pid, Txid) ->
    gen_server:call(Pid, {allow_tx, Txid}, infinity).

%%----------------------------------------------------------------------
%% Returns: active | inactive | not_found
%%----------------------------------------------------------------------
tx_status(Pid, Txid) ->
    gen_server:call(Pid, {tx_status, Txid}).

%%----------------------------------------------------------------------
%% Returns: {ok, Txid} | none
%%----------------------------------------------------------------------
active_tx(Pid) ->
    gen_server:call(Pid, active_tx).

%%%===================================================================
%%% Callback functions of gen_server
%%%===================================================================

init([]) ->
    %% 前回の異常終了で適用しきれなかったコミット済みの更新を再実行する
    ok = recover:recover(),
    %% ms_tx_mng      : {Txid, Timestamp, Status, OwnerPid, MonitorRef}
    %% ms_waiting_proc: {Txid, From} 順番待ちのallow_tx呼び出し元
    ets:new(ms_tx_mng, [set, named_table, protected]),
    ets:new(ms_waiting_proc, [set, named_table, protected]),
    {ok, []}.

handle_call({begin_tx, Owner}, _From, State) ->
    Txid = db_id:new(),
    register_tx(Txid, Owner),
    {reply, Txid, State};

handle_call({allow_tx, Txid}, From, State) ->
    case is_active_tx(Txid) of
        true ->
            {reply, ok, State};
        false ->
            %% 順番が来たらnotify_tx_active/1がreplyする
            stack_waiting_process(Txid, From),
            {noreply, State};
        transaction_not_found ->
            {reply, transaction_not_found, State}
    end;

handle_call({commit_tx, Txid}, _From, State) ->
    {reply, finish_tx(Txid, committed), State};

handle_call({rollback_tx, Txid}, _From, State) ->
    {reply, finish_tx(Txid, aborted), State};

handle_call({tx_status, Txid}, _From, State) ->
    Reply = case ets:lookup(ms_tx_mng, Txid) of
                [] -> not_found;
                [{Txid, _Ts, Status, _Owner, _Ref}] -> Status
            end,
    {reply, Reply, State};

handle_call(active_tx, _From, State) ->
    Reply = case ets:match_object(ms_tx_mng, {'_', '_', active, '_', '_'}) of
                [] -> none;
                [{Txid, _Ts, active, _Owner, _Ref} | _] -> {ok, Txid}
            end,
    {reply, Reply, State};

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

%% トランザクションを開始したプロセスが落ちた場合、
%% そのトランザクションをabortして次に順番を渡す。
handle_info({'DOWN', Ref, process, _Pid, _Reason}, State) ->
    case ets:match_object(ms_tx_mng, {'_', '_', '_', '_', Ref}) of
        [{Txid, _Ts, _Status, _Owner, Ref}] ->
            _ = finish_tx(Txid, aborted),
            ok;
        _ ->
            ok
    end,
    {noreply, State};
handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Transaction mng functions
%%%===================================================================

%% 新規トランザクションを登録して、可能ならactiveにする。
register_tx(Txid, Owner) ->
    Ref = monitor(process, Owner),
    ets:insert(ms_tx_mng, {Txid, db_id:timestamp(Txid), inactive, Owner, Ref}),
    activate_tx().

%% activeなトランザクションが1つもない場合に限り、
%% inactiveのうち一番古いものをactiveにして、待っているプロセスに通知する。
activate_tx() ->
    case ets:match_object(ms_tx_mng, {'_', '_', active, '_', '_'}) of
        [] ->
            case ets:match_object(ms_tx_mng, {'_', '_', inactive, '_', '_'}) of
                [] ->
                    no_transaction_waiting;
                InactiveTxList ->
                    {OldestTxid, Timestamp, _Status, Owner, Ref} = get_oldest_tx(InactiveTxList),
                    ets:insert(ms_tx_mng, {OldestTxid, Timestamp, active, Owner, Ref}),
                    notify_tx_active(OldestTxid)
            end;
        _ ->
            transaction_not_acquired
    end.

%% トランザクションを終了させ、次のトランザクションに順番を渡す。
%% 終了したトランザクションは管理表から消す。残しておくと
%% activate_tx/0の走査対象が単調に増えてしまうため。
finish_tx(Txid, _Status) ->
    case ets:lookup(ms_tx_mng, Txid) of
        [] ->
            _ = activate_tx(),
            transaction_not_found;
        [{Txid, _Timestamp, _OldStatus, _Owner, Ref}] ->
            _ = demonitor(Ref, [flush]),
            ets:delete(ms_tx_mng, Txid),
            %% 順番待ちのまま終了した場合は待ち行列からも外す
            case ets:lookup(ms_waiting_proc, Txid) of
                [{Txid, From}] ->
                    ets:delete(ms_waiting_proc, Txid),
                    gen_server:reply(From, transaction_not_found);
                [] ->
                    ok
            end,
            %% このトランザクションが握っていたロックを解放する
            _ = lock_mng:release_lock(whereis(lock_mng), Txid),
            _ = activate_tx(),
            ok
    end.

%% トランザクションのリストから一番古いものを返す。
%% Txidは {採番時刻, 単調増加の整数} なので、タプルの比較が採番順と一致する。
%% 同じナノ秒に開始した2つを区別するため、時刻だけでなくTxid全体で比べる。
get_oldest_tx([H | T]) ->
    lists:foldl(fun({Txid, _, _, _, _} = Tx, {OTxid, _, _, _, _} = Oldest) ->
                        case Txid < OTxid of
                            true -> Tx;
                            false -> Oldest
                        end
                end, H, T).

%% トランザクションがactiveの場合のみtrue。
is_active_tx(Txid) ->
    case ets:lookup(ms_tx_mng, Txid) of
        [] -> transaction_not_found;
        [{Txid, _Timestamp, active, _Owner, _Ref}] -> true;
        [{Txid, _Timestamp, _Status, _Owner, _Ref}] -> false
    end.

%% activeになるのを待っているallow_tx呼び出しを記録する。
stack_waiting_process(Txid, From) ->
    ets:insert(ms_waiting_proc, {Txid, From}).

%% 順番が来たことを待っているプロセスに知らせる。
notify_tx_active(Txid) ->
    case ets:lookup(ms_waiting_proc, Txid) of
        [] ->
            %% まだallow_txを呼んでいない。呼んだ時点でactiveなので即座に通る。
            no_waiting_process;
        [{Txid, From}] ->
            ets:delete(ms_waiting_proc, Txid),
            gen_server:reply(From, ok)
    end.
