%%%-------------------------------------------------------------------
%%% コミットの原子性と、索引とヒープの内部整合性。
%%%
%%% REDOのみでUNDOログが無い設計なので、いったん共有データへ適用すると
%%% 戻す手段が無い。したがって「適用が始まる前に、全変更が通ることを
%%% 確かめる」ことが原子性の唯一の担保になる。
%%%-------------------------------------------------------------------
-module(tx_atomicity_tests).

-include_lib("eunit/include/eunit.hrl").

%% ページに収まらない行を作る
big_row() ->
    list_to_binary(lists:duplicate(9000, $x)).

%%%===================================================================
%%% コミットの原子性
%%%===================================================================

commit_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun commit_is_all_or_nothing/1,
      fun failed_commit_reports_the_reason/1,
      fun failed_commit_releases_the_transaction/1,
      fun ddl_is_rejected_inside_a_transaction/1,
      fun ddl_waits_for_the_running_transaction/1,
      fun commit_works_with_durable_commit_off/1]}.

%% 1件でも適用できない変更があれば、そのトランザクションの変更は
%% ひとつも共有データに残らないこと。
commit_is_all_or_nothing(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, t, [a, b]}),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, t, [small, 1]}),
        {ok, _} = q(C, {insert, t, [big_row(), 2]}),
        ?assertEqual({error, row_too_large}, q(C, {commit_tx})),

        %% 1件目も残っていないこと
        _ = q(C, {begin_tx}),
        ?assertEqual([], q(C, {select, t, a, small})),
        ?assertEqual([], q(C, {scan, t})),
        ok = q(C, {commit_tx})
    end.

failed_commit_reports_the_reason(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, t, [a, b]}),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, t, [big_row(), 1]}),
        %% プロセスを落とさず、理由を返すこと
        ?assertEqual({error, row_too_large}, q(C, {commit_tx})),
        ?assert(is_process_alive(C))
    end.

%% コミットに失敗したトランザクションが順番を握ったままにならないこと
failed_commit_releases_the_transaction(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, t, [a, b]}),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, t, [big_row(), 1]}),
        {error, row_too_large} = q(C, {commit_tx}),
        %% 同じ接続で次のトランザクションが始められること
        Txid = q(C, {begin_tx}),
        ?assertNotMatch({error, _}, Txid),
        ?assertEqual([], q(C, {scan, t})),
        ok = q(C, {commit_tx})
    end.

%% 明示的なトランザクションの中のDDLは拒否されること。
%% カタログ変更を戻す仕組みが無いので、ロールバックできないものを
%% 黙って通さない。
ddl_is_rejected_inside_a_transaction(_) ->
    fun() ->
        C = connect(),
        _ = q(C, {begin_tx}),
        ?assertEqual({error, ddl_in_transaction}, q(C, {create_table, t, [a, b]})),
        ?assertEqual({error, ddl_in_transaction}, q(C, {drop_table, t})),
        ok = q(C, {rollback_tx}),
        %% トランザクションの外なら通る
        ?assertEqual(ok, q(C, {create_table, t, [a, b]}))
    end.

%% 他の接続のトランザクションが動いている最中に、DDLが割り込めないこと。
%% 割り込めると直列化可能性が成立しない。
ddl_waits_for_the_running_transaction(_) ->
    fun() ->
        C1 = connect(),
        ok = q(C1, {create_table, t, [a, b]}),
        _ = q(C1, {begin_tx}),
        {ok, _} = q(C1, {insert, t, [x, 1]}),

        C2 = connect(),
        Self = self(),
        spawn_link(fun() -> Self ! {dropped, q(C2, {drop_table, t})} end),
        %% C1がコミットするまでDROPは進めない
        ?assertEqual(timeout, recv(300)),
        ok = q(C1, {commit_tx}),
        ?assertEqual({dropped, ok}, recv(5000))
    end.

%% durable_commit を切っても結果は変わらないこと。
%% 変わるのは電源断に耐えるかどうかだけで、意味論は同じ。
commit_works_with_durable_commit_off(_) ->
    fun() ->
        application:set_env(transaction_db, durable_commit, false),
        try
            C = connect(),
            ok = q(C, {create_table, t, [a, b]}),
            _ = q(C, {begin_tx}),
            {ok, _} = q(C, {insert, t, [x, 1]}),
            ok = q(C, {commit_tx}),
            _ = q(C, {begin_tx}),
            ?assertEqual([[x, 1]], q(C, {select, t, a, x})),
            ok = q(C, {commit_tx})
        after
            application:set_env(transaction_db, durable_commit, true)
        end
    end.

%%%===================================================================
%%% 索引とヒープの内部整合性
%%%===================================================================

internal_consistency_test_() ->
    {foreach, fun db_test_helper:start_storage/0, fun db_test_helper:stop_storage/1,
     [fun failed_overwrite_keeps_index_and_heap_in_agreement/1,
      fun successful_overwrite_moves_the_index/1]}.

%% 上書きに失敗したとき、索引だけが先に消えて行が残る
%% (索引検索からは見えず走査からは見える幽霊行)ことがないこと
failed_overwrite_keeps_index_and_heap_in_agreement(_) ->
    fun() ->
        P = simple_db_server,
        Oid = {1, 1},
        ok = simple_db_server:create_table(P, t, [a, b]),
        ok = simple_db_server:insert_data(P, t, Oid, [apple, 100]),
        ?assertEqual({error, row_too_large},
                     simple_db_server:insert_data(P, t, Oid, [big_row(), 200])),
        %% 索引からも走査からも、元の行がそのまま見えること
        ?assertEqual([[apple, 100]], simple_db_server:select_data(P, t, a, apple)),
        ?assertEqual([[apple, 100]], scan(t))
    end.

successful_overwrite_moves_the_index(_) ->
    fun() ->
        P = simple_db_server,
        Oid = {1, 1},
        ok = simple_db_server:create_table(P, t, [a, b]),
        ok = simple_db_server:insert_data(P, t, Oid, [apple, 100]),
        ok = simple_db_server:insert_data(P, t, Oid, [banana, 200]),
        ?assertEqual([], simple_db_server:select_data(P, t, a, apple)),
        ?assertEqual([[banana, 200]], simple_db_server:select_data(P, t, a, banana)),
        ?assertEqual([[banana, 200]], scan(t))
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

scan(T) ->
    lists:reverse(simple_db_server:scan_fold(T, fun({_O, V}, Acc) -> [V | Acc] end, [])).

connect() ->
    {ok, Pid} = gen_connection:connect(),
    Pid.

q(Pid, Query) -> query_exec:exec_query(Pid, Query).

recv(Timeout) ->
    receive Msg -> Msg after Timeout -> timeout end.
