%%%-------------------------------------------------------------------
%%% トランザクションから見えるテーブル走査。
%%%
%%% 索引経由の検索と違い、走査には「特定カラムが特定の値」という述語が
%%% 無いため、既存の重ね合わせがそのままでは使えない。
%%% 共有側にOidが存在しないローカル挿入行を取りこぼしやすいので、
%%% 可視性の5ケースを網羅して確かめる。
%%%-------------------------------------------------------------------
-module(tx_scan_tests).

-include_lib("eunit/include/eunit.hrl").

tx_scan_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun scan_needs_a_transaction/1,
      fun scan_unknown_table/1,
      fun scan_empty_table/1,
      fun sees_committed_rows/1,
      fun sees_own_insert/1,
      fun does_not_see_own_delete/1,
      fun sees_own_update/1,
      fun insert_then_delete_in_same_tx_is_invisible/1,
      fun insert_then_update_in_same_tx/1,
      fun does_not_see_other_uncommitted_rows/1,
      fun rollback_discards_scanned_rows/1,
      fun scan_spans_many_pages/1,
      fun scan_matches_index_lookup/1]}.

scan_needs_a_transaction(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        ?assertEqual(transaction_not_found, q(C, {scan, fruit}))
    end.

scan_unknown_table(_) ->
    fun() ->
        C = connect(),
        _ = q(C, {begin_tx}),
        ?assertEqual({error, table_not_found}, q(C, {scan, nosuch})),
        ok = q(C, {rollback_tx})
    end.

scan_empty_table(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        ?assertEqual([], q(C, {scan, fruit})),
        ok = q(C, {commit_tx})
    end.

%% (1) 共有側にあり、自分は触っていない -> そのまま見える
sees_committed_rows(_) ->
    fun() ->
        C = setup_fruit([[apple, 100], [orange, 150]]),
        _ = q(C, {begin_tx}),
        ?assertEqual([[apple, 100], [orange, 150]], lists:sort(q(C, {scan, fruit}))),
        ok = q(C, {commit_tx})
    end.

%% (2) 共有側に無く、自分が挿入した -> 見える
%% ここが既存の重ね合わせでは取りこぼす経路
sees_own_insert(_) ->
    fun() ->
        C = setup_fruit([[apple, 100]]),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [banana, 200]}),
        ?assertEqual([[apple, 100], [banana, 200]], lists:sort(q(C, {scan, fruit}))),
        ok = q(C, {rollback_tx})
    end.

%% (3) 共有側にあり、自分が削除した -> 見えない
does_not_see_own_delete(_) ->
    fun() ->
        C = setup_fruit([[apple, 100], [orange, 150]]),
        _ = q(C, {begin_tx}),
        {ok, 1} = q(C, {delete, fruit, name, apple}),
        ?assertEqual([[orange, 150]], q(C, {scan, fruit})),
        ok = q(C, {rollback_tx})
    end.

%% (4) 共有側にあり、自分が更新した -> 更新後の値が見える
sees_own_update(_) ->
    fun() ->
        C = setup_fruit([[apple, 100]]),
        _ = q(C, {begin_tx}),
        {ok, 1} = q(C, {update, fruit, [{price, 120}], name, apple}),
        ?assertEqual([[apple, 120]], q(C, {scan, fruit})),
        ok = q(C, {rollback_tx})
    end.

%% (5) 同一トランザクション内で挿入してから削除した -> 見えない
insert_then_delete_in_same_tx_is_invisible(_) ->
    fun() ->
        C = setup_fruit([[apple, 100]]),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [banana, 200]}),
        {ok, 1} = q(C, {delete, fruit, name, banana}),
        ?assertEqual([[apple, 100]], q(C, {scan, fruit})),
        ok = q(C, {rollback_tx})
    end.

%% 挿入してから同一トランザクション内で更新した行が、二重に出ないこと
insert_then_update_in_same_tx(_) ->
    fun() ->
        C = setup_fruit([]),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [banana, 200]}),
        {ok, 1} = q(C, {update, fruit, [{price, 250}], name, banana}),
        ?assertEqual([[banana, 250]], q(C, {scan, fruit})),
        ok = q(C, {commit_tx}),
        _ = q(C, {begin_tx}),
        ?assertEqual([[banana, 250]], q(C, {scan, fruit})),
        ok = q(C, {commit_tx})
    end.

%% 他のトランザクションの未コミットの挿入は見えないこと
does_not_see_other_uncommitted_rows(_) ->
    fun() ->
        C1 = setup_fruit([[apple, 100]]),
        _ = q(C1, {begin_tx}),
        {ok, _} = q(C1, {insert, fruit, [banana, 200]}),

        C2 = connect(),
        Self = self(),
        spawn_link(fun() ->
                           _ = q(C2, {begin_tx}),
                           Self ! {c2, q(C2, {scan, fruit})},
                           ok = q(C2, {commit_tx})
                   end),
        %% C1がコミットするまでC2は進めない(トランザクションは直列)
        ?assertEqual(timeout, recv(300)),
        ok = q(C1, {rollback_tx}),
        %% ロールバックしたのでbananaは見えない
        ?assertEqual({c2, [[apple, 100]]}, recv(5000))
    end.

rollback_discards_scanned_rows(_) ->
    fun() ->
        C = setup_fruit([[apple, 100]]),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [banana, 200]}),
        ?assertEqual(2, length(q(C, {scan, fruit}))),
        ok = q(C, {rollback_tx}),
        _ = q(C, {begin_tx}),
        ?assertEqual([[apple, 100]], q(C, {scan, fruit})),
        ok = q(C, {commit_tx})
    end.

scan_spans_many_pages(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        [{ok, _} = q(C, {insert, fruit, [apple, I]}) || I <- lists:seq(1, 300)],
        ok = q(C, {commit_tx}),
        _ = q(C, {begin_tx}),
        Rows = q(C, {scan, fruit}),
        ?assertEqual(300, length(Rows)),
        ?assertEqual(lists:seq(1, 300), lists:sort([P || [apple, P] <- Rows])),
        ok = q(C, {commit_tx})
    end.

%% 走査の結果が索引経由の検索と一致すること
scan_matches_index_lookup(_) ->
    fun() ->
        C = setup_fruit([[apple, 100], [orange, 150], [banana, 150]]),
        _ = q(C, {begin_tx}),
        All = q(C, {scan, fruit}),
        ViaIndex = [R || R <- All, lists:nth(2, R) =:= 150],
        ?assertEqual(lists:sort(ViaIndex), lists:sort(q(C, {select, fruit, price, 150}))),
        ok = q(C, {commit_tx})
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

setup_fruit(Rows) ->
    C = connect(),
    ok = q(C, {create_table, fruit, [name, price]}),
    _ = q(C, {begin_tx}),
    [{ok, _} = q(C, {insert, fruit, R}) || R <- Rows],
    ok = q(C, {commit_tx}),
    C.

connect() ->
    {ok, Pid} = gen_connection:connect(),
    Pid.

q(Pid, Query) -> query_exec:exec_query(Pid, Query).

recv(Timeout) ->
    receive Msg -> Msg after Timeout -> timeout end.
