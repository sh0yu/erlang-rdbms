-module(lock_mng_tests).

-include_lib("eunit/include/eunit.hrl").

lock_mng_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun read_locks_are_shared/1,
      fun write_lock_blocks_write/1,
      fun write_lock_blocks_read/1,
      fun read_lock_blocks_write/1,
      fun re_acquiring_own_lock_succeeds/1,
      fun own_read_lock_upgrades_to_write/1,
      fun all_oids_are_locked/1,
      fun lock_after_partial_conflict_gets_every_oid/1,
      fun release_wakes_one_waiter/1,
      fun release_of_unknown_lock_is_ok/1,
      fun waiters_are_served_in_arrival_order/1,
      fun two_way_deadlock_is_refused/1,
      fun three_way_deadlock_is_refused/1,
      fun waiting_without_a_cycle_is_allowed/1,
      fun refused_request_leaves_no_trace/1]}.

%%%===================================================================
%%% 競合の判定
%%%===================================================================

read_locks_are_shared(_) ->
    fun() ->
        ok = acquire(tx1, [1], read),
        ok = acquire(tx2, [1], read),
        ?assert(is_locked(1))
    end.

write_lock_blocks_write(_) ->
    fun() ->
        ok = acquire(tx1, [1], write),
        ?assertEqual(timeout, acquire_async(tx2, [1], write, 300))
    end.

write_lock_blocks_read(_) ->
    fun() ->
        ok = acquire(tx1, [1], write),
        ?assertEqual(timeout, acquire_async(tx2, [1], read, 300))
    end.

read_lock_blocks_write(_) ->
    fun() ->
        ok = acquire(tx1, [1], read),
        ?assertEqual(timeout, acquire_async(tx2, [1], write, 300))
    end.

%% 同じロックを取り直しても通ること
re_acquiring_own_lock_succeeds(_) ->
    fun() ->
        ok = acquire(tx1, [1], read),
        ok = acquire(tx1, [1], read),
        ok = acquire(tx1, [1], write),
        %% writeを持っていればreadも足りている
        ok = acquire(tx1, [1], read)
    end.

%% 自分だけがreadを持っている場合はwriteへ格上げできること。
%% 格上げ後は他のトランザクションのreadが待たされる。
own_read_lock_upgrades_to_write(_) ->
    fun() ->
        ok = acquire(tx1, [1], read),
        ok = acquire(tx1, [1], write),
        ?assertEqual(timeout, acquire_async(tx2, [1], read, 300))
    end.

%% 他のトランザクションもreadを持っている状態では格上げできないこと。
%% 格上げしてしまうと相手が読んだ値を壊す。
upgrade_blocked_by_other_reader_test() ->
    {ok, Pid} = lock_mng:start_link(),
    try
        ok = lock_mng:acquire_lock(Pid, tx1, [1], read),
        ok = lock_mng:acquire_lock(Pid, tx2, [1], read),
        Self = self(),
        spawn(fun() -> Self ! {done, lock_mng:acquire_lock(Pid, tx1, [1], write)} end),
        ?assertEqual(timeout, receive M -> M after 300 -> timeout end)
    after
        stop(Pid)
    end.

%%%===================================================================
%%% 複数Oid
%%%===================================================================

all_oids_are_locked(_) ->
    fun() ->
        ok = acquire(tx1, [1, 2, 3], write),
        ?assertEqual([1, 2, 3], lists:sort(held_by(tx1))),
        ?assert(is_locked(1)),
        ?assert(is_locked(2)),
        ?assert(is_locked(3))
    end.

%% リストの途中に「すでに自分が持っているOid」があっても、
%% 残りのOidのロックが取り漏らされないこと
lock_after_partial_conflict_gets_every_oid(_) ->
    fun() ->
        ok = acquire(tx1, [2], read),
        ok = acquire(tx1, [1, 2, 3], read),
        ?assertEqual([1, 2, 3], lists:sort(held_by(tx1)))
    end.

%%%===================================================================
%%% 解放と待ち行列
%%%===================================================================

release_wakes_one_waiter(_) ->
    fun() ->
        ok = acquire(tx1, [1], write),
        Self = self(),
        spawn_link(fun() -> Self ! {tx2, acquire(tx2, [1], write)} end),
        ?assertEqual(timeout, recv(300)),
        ok = release(tx1),
        ?assertEqual({tx2, ok}, recv(5000)),
        ?assertEqual([1], held_by(tx2))
    end.

release_of_unknown_lock_is_ok(_) ->
    fun() ->
        ?assertEqual(ok, release(nosuch))
    end.

%% 先に待ち始めたトランザクションが先にロックを得ること
waiters_are_served_in_arrival_order(_) ->
    fun() ->
        ok = acquire(tx1, [1], write),
        Self = self(),
        spawn_link(fun() -> Self ! {first, acquire(tx2, [1], write)} end),
        timer:sleep(100),
        spawn_link(fun() -> Self ! {second, acquire(tx3, [1], write)} end),
        timer:sleep(100),
        ok = release(tx1),
        ?assertEqual({first, ok}, recv(5000)),
        %% tx2が持っている間はtx3は待ち続ける
        ?assertEqual(timeout, recv(300)),
        ok = release(tx2),
        ?assertEqual({second, ok}, recv(5000))
    end.

%%%===================================================================
%%% デッドロック
%%%===================================================================

%% tx1 が 1 を、tx2 が 2 を持っている状態で互いの相手を取りに行く。
%% 先に待ちに入った側は通し、閉路を閉じる側を断る。
two_way_deadlock_is_refused(_) ->
    fun() ->
        ok = acquire(tx1, [1], write),
        ok = acquire(tx2, [2], write),
        %% tx1 → tx2 の待ち(まだ閉路ではない)
        ?assertEqual(timeout, acquire_async(tx1, [2], write, 300)),
        %% tx2 → tx1 を足すと閉路になる
        ?assertEqual({error, deadlock}, acquire(tx2, [1], write)),
        %% 断られただけで、tx2 の元のロックは残っている
        ?assertEqual([2], held_by(tx2)),
        %% tx2 が終われば tx1 の待ちは解ける
        ok = release(tx2),
        ?assertEqual({done, ok}, recv(5000))
    end.

%% 3本でも辿れる。tx1 → tx2 → tx3 → tx1
three_way_deadlock_is_refused(_) ->
    fun() ->
        ok = acquire(tx1, [1], write),
        ok = acquire(tx2, [2], write),
        ok = acquire(tx3, [3], write),
        ?assertEqual(timeout, acquire_async(tx1, [2], write, 200)),
        ?assertEqual(timeout, acquire_async(tx2, [3], write, 200)),
        ?assertEqual({error, deadlock}, acquire(tx3, [1], write))
    end.

%% 閉路が無ければ待つ。一直線の待ちを誤って断らないこと。
waiting_without_a_cycle_is_allowed(_) ->
    fun() ->
        ok = acquire(tx1, [1], write),
        ok = acquire(tx2, [2], write),
        %% tx3 → tx1、tx3 → tx2。tx1 も tx2 も何も待っていない
        ?assertEqual(timeout, acquire_async(tx3, [1], write, 200)),
        ?assertEqual(timeout, acquire_async(tx2, [1], write, 200)),
        ok = release(tx1),
        %% 待っていた2本のうち先に来た tx3 が取る
        ?assertEqual({done, ok}, recv(5000))
    end.

%% 断ったものが待ち行列に残っていると、後で誰かの解放時に
%% 起こされて二重に応答してしまう。
refused_request_leaves_no_trace(_) ->
    fun() ->
        ok = acquire(tx1, [1], write),
        ok = acquire(tx2, [2], write),
        ?assertEqual(timeout, acquire_async(tx1, [2], write, 200)),
        ?assertEqual({error, deadlock}, acquire(tx2, [1], write)),
        %% tx2 は 1 を待っていない
        ?assertEqual(#{tx1 => [tx2]}, lock_mng:wait_for_graph(lock_mng))
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

acquire(LockId, OidList, RW) ->
    lock_mng:acquire_lock(lock_mng, LockId, OidList, RW).

%% 別プロセスでロックを取りに行き、Timeout内に取れなければ timeout
acquire_async(LockId, OidList, RW, Timeout) ->
    Self = self(),
    spawn(fun() -> Self ! {done, acquire(LockId, OidList, RW)} end),
    recv(Timeout).

release(LockId) ->
    lock_mng:release_lock(lock_mng, LockId).

held_by(LockId) ->
    lock_mng:locks_held_by(lock_mng, LockId).

is_locked(Oid) ->
    lock_mng:is_locked(lock_mng, Oid).

recv(Timeout) ->
    receive Msg -> Msg
    after Timeout -> timeout
    end.

setup() ->
    {ok, Pid} = lock_mng:start_link(),
    Pid.

cleanup(Pid) ->
    stop(Pid).

stop(Pid) ->
    Ref = monitor(process, Pid),
    unlink(Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, process, Pid, _} -> ok
    after 5000 -> ok
    end.
