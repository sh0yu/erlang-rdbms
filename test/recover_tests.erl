-module(recover_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/simple_db_server.hrl").

recover_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun replays_entries_after_last_checkpoint/1,
      fun entries_before_checkpoint_are_not_replayed/1,
      fun replay_is_idempotent/1,
      fun replays_delete/1,
      fun applies_entries_in_order/1,
      fun skips_entries_for_dropped_table/1,
      fun truncates_log_after_recovery/1,
      fun empty_log_is_a_noop/1]}.

%% コミット中に落ちた場合を模す: REDOログだけ書かれていて
%% 共有データにはまだ反映されていない状態からリカバリする
replays_entries_after_last_checkpoint(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = write_redo([{ins, fruit, 1, [apple, 100]}]),
        %% まだ反映されていない
        ?assertEqual([], select(fruit, name, apple)),
        ok = recover:recover(),
        ?assertEqual([[apple, 100]], select(fruit, name, apple))
    end.

%% checkpointより前のエントリは反映済みなので再実行しない
entries_before_checkpoint_are_not_replayed(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = write_redo([{ins, fruit, 1, [apple, 100]}]),
        ok = log_util:redo_log_put_checkpoint(),
        ?assertEqual([], recover:pending_redo_list()),
        ok = recover:recover(),
        %% 反映されないままであること(リカバリ対象外)
        ?assertEqual([], select(fruit, name, apple))
    end.

%% 一部だけ反映済みの状態から再実行しても二重にならないこと
replay_is_idempotent(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = write_redo([{ins, fruit, 1, [apple, 100]}]),
        ok = recover:recover(),
        ok = write_redo([{ins, fruit, 1, [apple, 100]}]),
        ok = recover:recover(),
        ?assertEqual([[apple, 100]], select(fruit, name, apple))
    end.

replays_delete(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ok = write_redo([{del, fruit, 1, [apple, 100]}]),
        ok = recover:recover(),
        ?assertEqual([], select(fruit, name, apple))
    end.

%% 同じOidに対するdel→insの順序が保たれること。
%% 逆順に適用すると行が消えてしまう。
applies_entries_in_order(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ok = write_redo([{del, fruit, 1, [apple, 100]},
                         {ins, fruit, 1, [banana, 200]}]),
        ok = recover:recover(),
        ?assertEqual([], select(fruit, name, apple)),
        ?assertEqual([[banana, 200]], select(fruit, name, banana))
    end.

%% ドロップ済みのテーブルへのREDOでリカバリ全体が落ちないこと
skips_entries_for_dropped_table(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = create(veggie, [name, price]),
        ok = write_redo([{ins, gone, 1, [apple, 100]},
                         {ins, veggie, 2, [carrot, 80]}]),
        ok = recover:recover(),
        %% 後続のエントリはきちんと適用される
        ?assertEqual([[carrot, 80]], select(veggie, name, carrot))
    end.

%% リカバリ後はログが切り詰められ、次の起動で再実行されないこと
truncates_log_after_recovery(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = write_redo([{ins, fruit, 1, [apple, 100]}]),
        ok = recover:recover(),
        ?assertEqual([], recover:pending_redo_list())
    end.

empty_log_is_a_noop(_) ->
    fun() ->
        ?assertEqual([], recover:pending_redo_list()),
        ?assertEqual(ok, recover:recover())
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

write_redo(Entries) ->
    Now = erlang:system_time(nanosecond),
    Logs = [#redo_log{timestamp = Now, txid = tx, query_id = q, action = Action,
                      table_name = T, oid = Oid, val = Val}
            || {Action, T, Oid, Val} <- Entries],
    ok = log_util:redo_log_write_many(Logs),
    ok = log_util:sync().

create(T, C) -> simple_db_server:create_table(simple_db_server, T, C).
insert(T, Oid, V) -> simple_db_server:insert_data(simple_db_server, T, Oid, V).
select(T, C, V) -> simple_db_server:select_data(simple_db_server, T, C, V).

setup() ->
    Dir = db_test_helper:tmp_dir("recover"),
    application:set_env(transaction_db, data_dir, Dir),
    {ok, _} = sys_tbl_mng:start_link(),
    {ok, _} = data_buffer:start_link(),
    {ok, _} = log_util:start_link(),
    {ok, _} = simple_db_server:start_link(),
    Dir.

cleanup(Dir) ->
    ok = db_test_helper:stop(simple_db_server),
    ok = db_test_helper:stop(log_util),
    ok = db_test_helper:stop(data_buffer),
    ok = db_test_helper:stop(sys_tbl_mng),
    application:unset_env(transaction_db, data_dir),
    db_test_helper:rm_rf(Dir),
    ok.
