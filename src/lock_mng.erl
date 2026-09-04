%%%-------------------------------------------------------------------
%%% @doc
%%% ロック管理(2相ロック)。オブジェクトID単位に共有(read)・
%%% 排他(write)ロックを管理する。
%%%
%%% ロックはトランザクションID(LockId)に紐づけて記録し、
%%% コミット・ロールバック時にまとめて解放する(strict 2PL)。
%%%
%%% 管理しているデータ
%%%   ms_locked_oid       : {Oid, Timestamp, LockId, RW} 誰がロック中か
%%%   ms_locking_oid      : {LockId, Oid, Timestamp} 自分が持つロック
%%%   ms_lock_waiting_proc: {Oid, LockId, RestOidList, Timestamp, From, RW}
%%%                         ロック待ち。RestOidListはこのOidが取れた後に
%%%                         続けて取るべきOidの残り。
%%% @end
%%%-------------------------------------------------------------------
-module(lock_mng).
-behaviour(gen_server).

%% Public API
-export([start_link/0, stop/1]).
-export([acquire_lock/4, release_lock/2, locks_held_by/2, is_locked/2]).

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
%% @doc OidListの全てにロックをかける。取れるまでブロックする。
%% ex) acquire_lock(P1, Txid, [Oid1, Oid2], read)
%% Returns: ok
%%----------------------------------------------------------------------
acquire_lock(Pid, LockId, OidList, RW) when is_list(OidList) ->
    gen_server:call(Pid, {acquire_lock, LockId, OidList, RW}, infinity);
acquire_lock(Pid, LockId, Oid, RW) ->
    acquire_lock(Pid, LockId, [Oid], RW).

%%----------------------------------------------------------------------
%% @doc LockIdが持つ全てのロックを解放する。
%%----------------------------------------------------------------------
release_lock(Pid, LockId) ->
    gen_server:call(Pid, {release_lock, LockId}, infinity).

%%----------------------------------------------------------------------
%% @doc LockIdが現在ロックしているOidの一覧(テスト・デバッグ用)。
%%----------------------------------------------------------------------
locks_held_by(Pid, LockId) ->
    gen_server:call(Pid, {locks_held_by, LockId}).

%%----------------------------------------------------------------------
%% @doc Oidが誰かにロックされているか(テスト・デバッグ用)。
%%----------------------------------------------------------------------
is_locked(Pid, Oid) ->
    gen_server:call(Pid, {is_locked, Oid}).

%%%===================================================================
%%% Callback functions of gen_server
%%%===================================================================

init([]) ->
    ets:new(ms_locked_oid, [bag, named_table, protected]),
    ets:new(ms_locking_oid, [bag, named_table, protected]),
    ets:new(ms_lock_waiting_proc, [bag, named_table, protected]),
    {ok, []}.

handle_call({acquire_lock, LockId, OidList, RW}, From, State) ->
    case acquire(LockId, OidList, get_timestamp(), From, RW) of
        ok ->
            {reply, ok, State};
        %% 1つでも取れなければ待ち行列に入る。取れた時点でreplyする。
        queued ->
            {noreply, State}
    end;

handle_call({release_lock, LockId}, _From, State) ->
    release_locking_oid(LockId),
    {reply, ok, State};

handle_call({locks_held_by, LockId}, _From, State) ->
    {reply, lists:usort([Oid || {_L, Oid, _Ts} <- ets:lookup(ms_locking_oid, LockId)]), State};

handle_call({is_locked, Oid}, _From, State) ->
    {reply, ets:lookup(ms_locked_oid, Oid) =/= [], State};

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Lock mng functions
%%%===================================================================

%% OidListの先頭から順にロックを取る。
%% 全て取れたら ok、途中で取れなければ残りごと待ち行列に入れて queued。
acquire(_LockId, [], _Timestamp, _From, _RW) ->
    ok;
acquire(LockId, [Oid | Rest], Timestamp, From, RW) ->
    case can_lock(LockId, Oid, RW) of
        already ->
            %% すでに十分なロックを持っている。残りのOidの処理は続ける。
            acquire(LockId, Rest, Timestamp, From, RW);
        upgrade ->
            %% 自分だけがreadロックを持っている場合に限りwriteへ格上げする
            upgrade_to_write(LockId, Oid),
            acquire(LockId, Rest, Timestamp, From, RW);
        true ->
            ets:insert(ms_locked_oid, {Oid, Timestamp, LockId, RW}),
            ets:insert(ms_locking_oid, {LockId, Oid, Timestamp}),
            acquire(LockId, Rest, Timestamp, From, RW);
        false ->
            %% このOidが解放されるまで待つ。解放時に残りのOidも続けて取る。
            ets:insert(ms_lock_waiting_proc, {Oid, LockId, Rest, Timestamp, From, RW}),
            queued
    end.

%% 自分が持つreadロックをwriteロックへ書き換える。
upgrade_to_write(LockId, Oid) ->
    lists:foreach(fun({_Oid, Ts, _L, _RW} = Object) ->
                          ets:delete_object(ms_locked_oid, Object),
                          ets:insert(ms_locked_oid, {Oid, Ts, LockId, write})
                  end, own_locks(LockId, Oid)).

%% true    : ロック可
%% false   : ロック不可(他のトランザクションと競合)
%% already : すでに十分なロックを持っている
%% upgrade : 自分のreadロックをwriteロックに格上げすればよい
can_lock(LockId, Oid, Mode) ->
    Holders = ets:lookup(ms_locked_oid, Oid),
    Own = [RW || {_O, _Ts, L, RW} <- Holders, L =:= LockId],
    Others = [RW || {_O, _Ts, L, RW} <- Holders, L =/= LockId],
    case {Mode, Own, Others} of
        %% 誰も持っていない
        {_, [], []} ->
            true;
        %% 自分がwriteロックを持っていれば読み書きとも足りている
        _ when Own =/= [] ->
            case lists:member(write, Own) of
                true ->
                    already;
                false when Mode =:= read ->
                    already;
                false ->
                    %% 自分はreadのみ。他に誰も持っていなければ格上げできる。
                    %% 他にreadを持つトランザクションがいる状態で格上げすると
                    %% その相手の読んだ値を壊すため、待たせる。
                    case Others of
                        [] -> upgrade;
                        _ -> false
                    end
            end;
        %% 他がreadのみ持っていて、自分もreadを取りたい
        {read, [], _} ->
            case lists:member(write, Others) of
                true -> false;
                false -> true
            end;
        %% write要求は他に誰かいる時点で待つ
        {write, [], _} ->
            false
    end.

own_locks(LockId, Oid) ->
    [Obj || {_O, _Ts, L, _RW} = Obj <- ets:lookup(ms_locked_oid, Oid), L =:= LockId].

%% Oidのロックが解放されたとき、待っているトランザクションに順番を渡す。
%% 待っていたトランザクションは残りのOidも続けて取ってから応答を受け取る。
dequeue_lock(Oid) ->
    case ets:lookup(ms_lock_waiting_proc, Oid) of
        [] ->
            no_waiting_process;
        Waiting ->
            {Oid, LockId, Rest, Timestamp, From, RW} = Lock = get_oldest_lock(Waiting),
            true = ets:delete_object(ms_lock_waiting_proc, Lock),
            case acquire(LockId, [Oid | Rest], Timestamp, From, RW) of
                ok -> gen_server:reply(From, ok);
                %% さらに別のOidで待つことになった
                queued -> queued
            end
    end.

%% 次にロックを取るのは一番古くから待っているトランザクション。
%% 到着順で選ぶことで、待ち続けるトランザクションが出ないようにする。
get_oldest_lock([H | T]) ->
    lists:foldl(fun({_O, _L, _R, Ts, _F, _RW} = W, {_O2, _L2, _R2, OTs, _F2, _RW2} = Oldest) ->
                        case Ts < OTs of
                            true -> W;
                            false -> Oldest
                        end
                end, H, T).

%% LockIdが獲得した全てのロックを解放する。
release_locking_oid(LockId) ->
    Oids = lists:usort([Oid || {_L, Oid, _Ts} <- ets:lookup(ms_locking_oid, LockId)]),
    ets:delete(ms_locking_oid, LockId),
    %% 待ち行列に残ったままの自分のエントリも消す(待っている最中の中断)
    lists:foreach(fun({_Oid, L, _R, _Ts, _F, _RW} = W) when L =:= LockId ->
                          ets:delete_object(ms_lock_waiting_proc, W);
                     (_) ->
                          ok
                  end, ets:tab2list(ms_lock_waiting_proc)),
    lists:foreach(fun(Oid) -> release_locked_oid(Oid, LockId) end, Oids),
    ok.

%% 指定のOidについて、指定のトランザクションのロックだけを解放する。
release_locked_oid(Oid, LockId) ->
    lists:foreach(fun(Object) -> ets:delete_object(ms_locked_oid, Object) end,
                  own_locks(LockId, Oid)),
    dequeue_lock(Oid).

%% 待ち行列の順序づけに使うので、同じ値が二度出ないことが要る。
%% 実時刻は同じナノ秒を返しうるため、単調増加の整数を使う。
get_timestamp() ->
    erlang:unique_integer([monotonic, positive]).
