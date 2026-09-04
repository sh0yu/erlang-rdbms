-module(tx_mng_tests).

-include_lib("eunit/include/eunit.hrl").

tx_mng_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun first_transaction_is_active/1,
      fun second_transaction_waits/1,
      fun commit_activates_the_next/1,
      fun rollback_activates_the_next/1,
      fun oldest_waiting_transaction_goes_first/1,
      fun allow_tx_on_unknown_transaction/1,
      fun commit_of_unknown_transaction/1,
      fun finished_transactions_are_forgotten/1,
      fun owner_death_aborts_transaction/1]}.

%% 最初のトランザクションはすぐにactiveになること
first_transaction_is_active(_) ->
    fun() ->
        Txid = begin_tx(),
        ?assertEqual(active, status(Txid)),
        ?assertEqual(ok, allow(Txid)),
        ok = commit(Txid)
    end.

%% 2つ目はactiveにならず、allow_txがブロックすること
second_transaction_waits(_) ->
    fun() ->
        Tx1 = begin_tx(),
        Tx2 = begin_tx(),
        ?assertEqual(active, status(Tx1)),
        ?assertEqual(inactive, status(Tx2)),
        ?assertEqual(timeout, allow_async(Tx2, 300)),
        ok = commit(Tx1),
        ok = commit(Tx2)
    end.

commit_activates_the_next(_) ->
    fun() ->
        Tx1 = begin_tx(),
        Tx2 = begin_tx(),
        ok = commit(Tx1),
        ?assertEqual(active, status(Tx2)),
        ok = commit(Tx2)
    end.

rollback_activates_the_next(_) ->
    fun() ->
        Tx1 = begin_tx(),
        Tx2 = begin_tx(),
        ok = rollback(Tx1),
        ?assertEqual(active, status(Tx2)),
        ok = commit(Tx2)
    end.

%% 待っているトランザクションのうち、一番先に開始したものがactiveになること
oldest_waiting_transaction_goes_first(_) ->
    fun() ->
        Tx1 = begin_tx(),
        Tx2 = begin_tx(),
        Tx3 = begin_tx(),
        ok = commit(Tx1),
        ?assertEqual(active, status(Tx2)),
        ?assertEqual(inactive, status(Tx3)),
        ok = commit(Tx2),
        ?assertEqual(active, status(Tx3)),
        ok = commit(Tx3)
    end.

allow_tx_on_unknown_transaction(_) ->
    fun() ->
        ?assertEqual(transaction_not_found, allow(no_such_txid))
    end.

commit_of_unknown_transaction(_) ->
    fun() ->
        ?assertEqual(transaction_not_found, commit(no_such_txid))
    end.

%% 終了したトランザクションは管理表に残らないこと。
%% 残すとトランザクションを重ねるほどactivate_txの走査が重くなる。
finished_transactions_are_forgotten(_) ->
    fun() ->
        Txid = begin_tx(),
        ok = commit(Txid),
        ?assertEqual(not_found, status(Txid)),
        ?assertEqual(none, tx_mng:active_tx(tx_mng))
    end.

%% トランザクションを開始したプロセスが落ちたら、そのトランザクションを
%% abortして次に順番を渡すこと。渡さないとDB全体が止まる。
owner_death_aborts_transaction(_) ->
    fun() ->
        Self = self(),
        Owner = spawn(fun() ->
                              Self ! {txid, tx_mng:begin_tx(tx_mng)},
                              receive stop -> ok end
                      end),
        Tx1 = receive {txid, T} -> T after 5000 -> error(no_txid) end,
        ?assertEqual(active, status(Tx1)),

        Tx2 = begin_tx(),
        ?assertEqual(inactive, status(Tx2)),

        exit(Owner, kill),
        %% Tx1が片付けられ、Tx2に順番が回ってくる
        ?assertEqual(ok, wait_until(fun() -> status(Tx2) =:= active end, 5000)),
        ?assertEqual(not_found, status(Tx1)),
        ok = commit(Tx2)
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

begin_tx() -> tx_mng:begin_tx(tx_mng).
commit(Txid) -> tx_mng:commit_tx(tx_mng, Txid).
rollback(Txid) -> tx_mng:rollback_tx(tx_mng, Txid).
allow(Txid) -> tx_mng:allow_tx(tx_mng, Txid).
status(Txid) -> tx_mng:tx_status(tx_mng, Txid).

allow_async(Txid, Timeout) ->
    Self = self(),
    spawn(fun() -> Self ! {done, allow(Txid)} end),
    receive Msg -> Msg
    after Timeout -> timeout
    end.

wait_until(Fun, Timeout) when Timeout =< 0 ->
    case Fun() of
        true -> ok;
        false -> timeout
    end;
wait_until(Fun, Timeout) ->
    case Fun() of
        true -> ok;
        false ->
            timer:sleep(20),
            wait_until(Fun, Timeout - 20)
    end.
