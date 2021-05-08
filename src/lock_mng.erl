-module(lock_mng).
-compile(export_all).
-behaviour(gen_server).
% -include_lib("kernel/include/logger.hrl").
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

get_lock_id() ->
    erlang:system_time(nanosecond).

%% ex) acquire_lock(P1, [0001, 0002], read)
%% ex) acquire_lock(P1, [0001, 0002], write)
acquire_lock(Pid, LockId, OidList, RW) ->
    gen_server:call(Pid, {acquire_lock, LockId, OidList, RW}, infinity).

release_lock(Pid, LockId) ->
    gen_server:call(Pid, {release_lock, LockId}).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Callback functions of gen_server.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init([]) ->
    %% システムテーブル作成
    %% ロックされているオブジェクトIDを管理
    ets:new(ms_locked_oid, [bag, named_table, public]),
    %% 自分ががロックしているオブジェクトIDを管理
    ets:new(ms_locking_oid, [bag, named_table, public]),
    %% ロックが取れず待っているオブジェクトIDとプロセスIDを管理
    %% ロックが取れたとき、次にロックを獲得しなければならないオブジェクトIDのリストも保持
    ets:new(ms_lock_waiting_proc, [bag, named_table, public]),
    {ok, []}.

handle_call(terminate, _From, _State) ->
    {stop, normal, ok, []};
handle_call({acquire_lock, LockId, OidList, RW}, From, _State) ->
    Timestamp = get_timestamp(),
    io:format("[acquire]From:~p~n", [From]),
    case acquire(LockId, OidList, Timestamp, From, RW, ok) of
        ok -> {reply, ok, []};
        %% ロックが獲得できなかった場合は、replyせずにロック待ち状態にする
        queued -> {noreply, [], infinity}
    end;
handle_call({release_lock, LockId}, _From, _State) ->
    release_locking_oid(LockId),
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
%% オブジェクトをロックする
%% すべてのオブジェクトに対して即時ロックが取れればokを返す
%% 一つでもロックが取れずキューに入ることがあればqueuedを返す
acquire(LockId, [Oid | OidList], Timestamp, From, RW, Rep) ->
    %% TODO: lockをリリースした後、それを待っていたロックを取るときの不具合
    %% ms_locking_procにレコードが追加されていない
    case can_lock(LockId, Oid, RW) of
        already -> ok;
        change_write -> 
            %% readロックをwriteロックに書き換える
            [{Oid, OldestTimestamp, LockId, _RW}=Object] = ets:match_object(ms_locked_oid, {Oid, '$1', LockId, '$3'}),
            ets:delete_object(ms_locked_oid, Object),
            ets:insert(ms_locked_oid, {Oid, OldestTimestamp, LockId, write}),
            ok;
        true ->
            ets:insert(ms_locked_oid, {Oid, Timestamp, LockId, RW}),
            ets:insert(ms_locking_oid, {LockId, Oid, Timestamp}),
            acquire(LockId, OidList, Timestamp, From, RW, Rep);
        false ->
            % lockが取れなかった場合、そのOidのロックが解放されるまで待つ
            % ロックが解放されたら、残されたOidについてロックを獲得する
            ets:insert(ms_lock_waiting_proc, {Oid, LockId, OidList, Timestamp, From, RW}),
            queued
    end;
acquire(_LockId, [], _Timestamp, _From, _RW, Rep) ->
    Rep.

%% true: Read-Read ロック可
%% false: Read-Write ロック不可
%% false: Write-Write ロック不可
%% already: 自分がすでにロックを取っているケース
%% change_write: 自分がreadロックを取っており、writeロックを取るケース
can_lock(LockId, Oid, Mode) ->
    is_only_read_lock(LockId, Mode, ets:lookup(ms_locked_oid, Oid)).

%% 基本的にはそのオブジェクトに対して、Writeロックがとられていたら自分は取れないという戦略
%% しかし、自分がすでにそのオブジェクトのロックを取っている場合はロックが取れる
is_only_read_lock(_LockId, _Mode, [])->
    true;
%% 自分がreadロックを取っており、readロックを取りたいケース
%% ロック処理はスキップで良い
is_only_read_lock(LockId, read, [{_Oid, _Timestamp, LockId, read} | _LockList]) ->
    already;
%% 自分がreadロックを取っており、writeロックを取りたいケース
%% readロックをwriteロックに書き換える
is_only_read_lock(LockId, write, [{_Oid, _Timestamp, LockId, read} | _LockList]) ->
    change_write;
%% 自分がwriteロックを取っているケース
%% ロック処理はスキップで良い
is_only_read_lock(LockId, _Mode, [{_Oid, _Timestamp, LockId, write} | _LockList]) ->
    already;
%% 他人がreadロックを取っており、自分もreadロックを取りたいケース
is_only_read_lock(LockId, read, [{_Oid, _Timestamp, _LockId, read} | LockList]) ->
    is_only_read_lock(LockId, read, LockList);
%% ロック競合のケース
is_only_read_lock(_LockId, _Mode, _) ->
    false.

%% あるオブジェクトのロックが解放されたとき、次にロックをかけたいプロセスが存在すれば
%% そのプロセスがロックを獲得する
dequeue_lock(Oid) ->
    case ets:lookup(ms_lock_waiting_proc, Oid) of
        [] -> no;
        OidList -> 
            {Oid, LockId, OidL, Timestamp, From, RW}=Lock = get_oldest_lock(OidList),
            %% ロック待ちのキューから一番古いロックを削除
            true = ets:delete_object(ms_lock_waiting_proc, Lock),
            case acquire(LockId, [Oid | OidL], Timestamp, From, RW, ok) of
                %% ロック待ちが解消されればokを返す
                ok -> 
                io:format("[dequeue_lock]reply ok to ~p~n", [From]),    
                gen_server:reply(From, ok);
                queued -> queued
            end
    end.

%% 次にロックを取るプロセスをキューから選択する
get_oldest_lock([H | _T]) ->
    %% TODO:戦略改善
    H.

%% 自分が獲得したロックを解放する
release_locking_oid(LockId) ->
    io:format("[release_locking_oid]LockId:~p~n", [LockId]),
    case ets:lookup(ms_locking_oid, LockId) of
        LockList ->
            [release_locked_oid(Oid, LockId) || {_,Oid,_} <- LockList];
        [] -> ok
    end,
    ets:delete(ms_locking_oid, LockId).

%% 指定されたオブジェクトのロックを解放する
release_locked_oid(OidList, LockId) when is_list(OidList)->
    [release_locked_oid(Oid, LockId) || Oid <- OidList];
release_locked_oid(Oid, LockId) ->
    %% 指定のオブジェクトの指定のプロセスのロックのみ解放する
    [Object] = ets:match_object(ms_locked_oid, {Oid, '$1', LockId, '$3'}),
    % [Object] = ets:match_object(ms_locked_oid, {Oid, '$1', From, '$2'}),
    io:format("release_locked_oid:~p~n", [Object]),
    ets:delete_object(ms_locked_oid, Object),
    %% ロック待ちになっていればロックを獲得する
    dequeue_lock(Oid).

%% タイムスタンプを取得する
get_timestamp() ->
    erlang:system_time(nanosecond).
