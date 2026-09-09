%%%-------------------------------------------------------------------
%%% @doc
%%% トランザクション管理。
%%%
%%% トランザクションは**並行に走る**。分離は tx_mng ではなく、
%%%   * 読み: snapshot_mng のスナップショットと undo
%%%   * 書き: lock_mng の行ロックと、コミット直前の衝突検査
%%% が担う。
%%%
%%% 以前はここが同時に1本しか active にせず、全トランザクションを
%%% 直列に実行していた。読みロックを取らない走査でも安全だったのは
%%% その直列化のおかげで、スナップショットが入るまでは外せなかった。
%%%
%%% DDL だけは今も直列で、`commit_latch` の排他ラッチで囲う。
%%% カタログの UNDO が無く、走査中に表が落ちると読み手が壊れるため。
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
%% @doc 自分のトランザクションがまだ生きているかを確かめる。
%%
%% 直列だった頃は「順番が来るまで待つ」関数だった。いまは待たない。
%% 名前と戻り値は呼び出し側の都合で残してある。
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

%% 新規トランザクションを登録する。順番待ちは無く、すぐ active。
register_tx(Txid, Owner) ->
    Ref = monitor(process, Owner),
    ets:insert(ms_tx_mng, {Txid, db_id:timestamp(Txid), active, Owner, Ref}),
    notify_tx_active(Txid).

%% トランザクションを終了させ、次のトランザクションに順番を渡す。
%% 終了したトランザクションは管理表から消す。残しておくと
%% activate_tx/0の走査対象が単調に増えてしまうため。
finish_tx(Txid, _Status) ->
    case ets:lookup(ms_tx_mng, Txid) of
        [] ->
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
            %% このトランザクションが握っていたロックを解放する。
            %% 待っている書き手はここで進む。
            _ = lock_mng:release_lock(whereis(lock_mng), Txid),
            ok
    end.

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
