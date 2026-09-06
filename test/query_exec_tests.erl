-module(query_exec_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/simple_db_server.hrl").

query_exec_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun query_without_transaction/1,
      fun ddl_needs_no_transaction/1,
      fun insert_is_visible_to_self/1,
      fun insert_rejects_unknown_table/1,
      fun insert_rejects_wrong_column_count/1,
      fun commit_makes_changes_visible/1,
      fun rollback_discards_changes/1,
      fun update_within_transaction/1,
      fun update_rejects_unknown_column/1,
      fun delete_within_transaction/1,
      fun insert_then_delete_in_same_transaction/1,
      fun uncommitted_changes_are_invisible_to_others/1,
      fun nested_begin_is_rejected/1,
      fun unsupported_query_is_reported/1,
      fun select_on_unknown_table/1,
      fun transactions_run_concurrently/1,
      fun same_row_written_twice_is_refused/1,
      fun dead_connection_releases_transaction/1,
      fun disconnect_rolls_back/1]}.

%%%===================================================================
%%% トランザクション外
%%%===================================================================

%% begin_txしていないDMLは弾かれること
query_without_transaction(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        ?assertEqual(transaction_not_found, q(C, {insert, fruit, [apple, 100]})),
        ?assertEqual(transaction_not_found, q(C, {select, fruit, name, apple})),
        ?assertEqual(transaction_not_found, q(C, {commit_tx})),
        ?assertEqual(transaction_not_found, q(C, {rollback_tx}))
    end.

ddl_needs_no_transaction(_) ->
    fun() ->
        C = connect(),
        ?assertEqual(ok, q(C, {create_table, fruit, [name, price]})),
        ?assertEqual({error, table_already_exists}, q(C, {create_table, fruit, [name, price]})),
        ?assertEqual(ok, q(C, {drop_table, fruit})),
        ?assertEqual({error, table_not_found}, q(C, {drop_table, fruit}))
    end.

%%%===================================================================
%%% 単一トランザクション
%%%===================================================================

%% コミット前でも自分の変更は自分から見えること
insert_is_visible_to_self(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [apple, 100]}),
        ?assertEqual([[apple, 100]], q(C, {select, fruit, name, apple})),
        ok = q(C, {rollback_tx})
    end.

insert_rejects_unknown_table(_) ->
    fun() ->
        C = connect(),
        _ = q(C, {begin_tx}),
        ?assertEqual({error, table_not_found}, q(C, {insert, nosuch, [apple, 100]})),
        ok = q(C, {rollback_tx})
    end.

insert_rejects_wrong_column_count(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        ?assertEqual({error, column_count_mismatch}, q(C, {insert, fruit, [apple]})),
        ok = q(C, {rollback_tx})
    end.

commit_makes_changes_visible(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [apple, 100]}),
        ok = q(C, {commit_tx}),
        %% 別のトランザクションから見える
        _ = q(C, {begin_tx}),
        ?assertEqual([[apple, 100]], q(C, {select, fruit, name, apple})),
        ok = q(C, {commit_tx})
    end.

rollback_discards_changes(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [apple, 100]}),
        ok = q(C, {rollback_tx}),
        _ = q(C, {begin_tx}),
        ?assertEqual([], q(C, {select, fruit, name, apple})),
        ok = q(C, {commit_tx})
    end.

update_within_transaction(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [apple, 100]}),
        ?assertEqual({ok, 1}, q(C, {update, fruit, [{price, 120}], name, apple})),
        %% 更新後の値で引ける
        ?assertEqual([[apple, 120]], q(C, {select, fruit, price, 120})),
        %% 更新前の値では引けない
        ?assertEqual([], q(C, {select, fruit, price, 100})),
        ok = q(C, {commit_tx}),
        _ = q(C, {begin_tx}),
        ?assertEqual([[apple, 120]], q(C, {select, fruit, name, apple})),
        ?assertEqual([], q(C, {select, fruit, price, 100})),
        ok = q(C, {commit_tx})
    end.

update_rejects_unknown_column(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        ?assertEqual({error, {unknown_columns, [colour]}},
                     q(C, {update, fruit, [{colour, red}], name, apple})),
        ok = q(C, {rollback_tx})
    end.

delete_within_transaction(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [apple, 100]}),
        {ok, _} = q(C, {insert, fruit, [orange, 150]}),
        ok = q(C, {commit_tx}),
        _ = q(C, {begin_tx}),
        ?assertEqual({ok, 1}, q(C, {delete, fruit, name, apple})),
        ?assertEqual([], q(C, {select, fruit, name, apple})),
        ok = q(C, {commit_tx}),
        _ = q(C, {begin_tx}),
        ?assertEqual([], q(C, {select, fruit, name, apple})),
        ?assertEqual([[orange, 150]], q(C, {select, fruit, name, orange})),
        ok = q(C, {commit_tx})
    end.

%% 同一トランザクション内で入れてから消した行が、コミット後に残らないこと
insert_then_delete_in_same_transaction(_) ->
    fun() ->
        C = connect(),
        ok = q(C, {create_table, fruit, [name, price]}),
        _ = q(C, {begin_tx}),
        {ok, _} = q(C, {insert, fruit, [apple, 100]}),
        ?assertEqual({ok, 1}, q(C, {delete, fruit, name, apple})),
        ?assertEqual([], q(C, {select, fruit, name, apple})),
        ok = q(C, {commit_tx}),
        _ = q(C, {begin_tx}),
        ?assertEqual([], q(C, {select, fruit, name, apple})),
        ok = q(C, {commit_tx})
    end.

nested_begin_is_rejected(_) ->
    fun() ->
        C = connect(),
        _ = q(C, {begin_tx}),
        ?assertEqual({error, transaction_already_started}, q(C, {begin_tx})),
        ok = q(C, {rollback_tx})
    end.

%% 存在しないテーブルへのSELECTは、INSERTやUPDATEと同じくエラーを返すこと
select_on_unknown_table(_) ->
    fun() ->
        C = connect(),
        _ = q(C, {begin_tx}),
        ?assertEqual({error, table_not_found}, q(C, {select, nosuch, name, apple})),
        ok = q(C, {rollback_tx})
    end.

unsupported_query_is_reported(_) ->
    fun() ->
        C = connect(),
        ?assertMatch({error, {unsupported_query, _}}, q(C, {no_such_query, 1}))
    end.

%%%===================================================================
%%% 複数トランザクション
%%%===================================================================

%% コミットしていない変更が他の接続から見えないこと
uncommitted_changes_are_invisible_to_others(_) ->
    fun() ->
        C1 = connect(),
        ok = q(C1, {create_table, fruit, [name, price]}),
        _ = q(C1, {begin_tx}),
        {ok, _} = q(C1, {insert, fruit, [apple, 100]}),

        %% C2 は待たない。ただしC1の未コミットの挿入は見えない
        C2 = connect(),
        _ = q(C2, {begin_tx}),
        ?assertEqual([], q(C2, {select, fruit, name, apple})),
        ok = q(C1, {commit_tx}),
        %% C2 は自分のスナップショットを見続けるので、まだ見えない
        ?assertEqual([], q(C2, {select, fruit, name, apple})),
        ok = q(C2, {commit_tx}),
        %% 開き直せば見える
        _ = q(C2, {begin_tx}),
        ?assertEqual([[apple, 100]], q(C2, {select, fruit, name, apple})),
        ok = q(C2, {commit_tx})
    end.

%% **トランザクションは並行に走る。** 以前は2本目のクエリが
%% 1本目のコミットまでブロックしていた。
transactions_run_concurrently(_) ->
    fun() ->
        C1 = connect(),
        ok = q(C1, {create_table, fruit, [name, price]}),
        _ = q(C1, {begin_tx}),
        {ok, _} = q(C1, {insert, fruit, [apple, 100]}),

        C2 = connect(),
        Self = self(),
        spawn_link(fun() ->
                           _ = q(C2, {begin_tx}),
                           {ok, _} = q(C2, {insert, fruit, [orange, 150]}),
                           ok = q(C2, {commit_tx}),
                           Self ! c2_done
                   end),
        %% C1 が開いたままでも C2 は進む
        ?assertEqual(c2_done, recv(5000)),
        ok = q(C1, {commit_tx}),
        _ = q(C1, {begin_tx}),
        ?assertEqual(2, length(q(C1, {scan, fruit}))),
        ok = q(C1, {commit_tx})
    end.

%% 同じ行を2本が書いたら、後からコミットする方を捨てる。
%% 黙って上書きすると、先にコミットした方の更新が消える(lost update)。
same_row_written_twice_is_refused(_) ->
    fun() ->
        C1 = connect(),
        ok = q(C1, {create_table, fruit, [name, price]}),
        _ = q(C1, {begin_tx}),
        {ok, _} = q(C1, {insert, fruit, [apple, 100]}),
        ok = q(C1, {commit_tx}),

        C2 = connect(),
        _ = q(C1, {begin_tx}),
        _ = q(C2, {begin_tx}),
        {ok, 1} = q(C1, {update, fruit, [{price, 200}], name, apple}),
        ok = q(C1, {commit_tx}),

        %% C2 は自分のスナップショット(価格100)を見て書こうとする。
        %% コミットではなく UPDATE の時点で断られる
        ?assertEqual({error, serialization_failure},
                     q(C2, {update, fruit, [{price, 300}], name, apple})),

        %% 先にコミットした C1 の値が残る
        _ = q(C1, {begin_tx}),
        ?assertEqual([[apple, 200]], q(C1, {select, fruit, name, apple})),
        ok = q(C1, {commit_tx})
    end.

%% 接続プロセスが異常終了しても、トランザクションの順番が
%% 握られたままにならないこと
dead_connection_releases_transaction(_) ->
    fun() ->
        C1 = connect(),
        ok = q(C1, {create_table, fruit, [name, price]}),
        _ = q(C1, {begin_tx}),
        {ok, _} = q(C1, {insert, fruit, [apple, 100]}),
        %% コミットせずに接続を殺す
        exit(C1, kill),

        %% 次のトランザクションが進めること
        C2 = connect(),
        _ = q(C2, {begin_tx}),
        %% 殺されたトランザクションの変更は反映されていない
        ?assertEqual([], q(C2, {select, fruit, name, apple})),
        ok = q(C2, {commit_tx})
    end.

%% 正常に接続を閉じた場合も、未コミットの変更はロールバックされること
disconnect_rolls_back(_) ->
    fun() ->
        C1 = connect(),
        ok = q(C1, {create_table, fruit, [name, price]}),
        _ = q(C1, {begin_tx}),
        {ok, _} = q(C1, {insert, fruit, [apple, 100]}),
        ok = gen_connection:disconnect(C1),

        C2 = connect(),
        _ = q(C2, {begin_tx}),
        ?assertEqual([], q(C2, {select, fruit, name, apple})),
        ok = q(C2, {commit_tx})
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

connect() ->
    {ok, Pid} = gen_connection:connect(),
    Pid.

q(Pid, Query) ->
    query_exec:exec_query(Pid, Query).

recv(Timeout) ->
    receive Msg -> Msg
    after Timeout -> timeout
    end.
