-module(data_buffer_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/simple_db_server.hrl").

data_buffer_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun write_then_read/1,
      fun read_unknown_oid/1,
      fun write_is_idempotent/1,
      fun update_changes_value/1,
      fun delete_removes_row/1,
      fun delete_twice_is_reported/1,
      fun deleted_slot_is_reused/1,
      fun many_rows_span_multiple_pages/1,
      fun rows_survive_buffer_eviction/1,
      fun rows_survive_restart/1,
      fun two_tables_are_isolated/1,
      fun oversized_row_is_rejected/1,
      fun update_to_larger_value_relocates/1,
      fun drop_table_discards_data/1,
      fun vacuum_reclaims_pages/1]}.

%%%===================================================================
%%% 基本操作
%%%===================================================================

write_then_read(Pid) ->
    fun() ->
        ok = data_buffer:write_data(Pid, fruit, 1, [apple, 100]),
        ?assertEqual([apple, 100], data_buffer:read_data(Pid, 1))
    end.

read_unknown_oid(Pid) ->
    fun() ->
        ?assertEqual({error, oid_not_found}, data_buffer:read_data(Pid, 999))
    end.

%% リカバリでREDOを再実行しても壊れないよう、同じOidへの書き込みは冪等
write_is_idempotent(Pid) ->
    fun() ->
        ok = data_buffer:write_data(Pid, fruit, 1, [apple, 100]),
        ok = data_buffer:write_data(Pid, fruit, 1, [apple, 100]),
        ?assertEqual([apple, 100], data_buffer:read_data(Pid, 1)),
        ?assertEqual(1, row_count(Pid, fruit))
    end.

update_changes_value(Pid) ->
    fun() ->
        ok = data_buffer:write_data(Pid, fruit, 1, [apple, 100]),
        {ok, _} = data_buffer:update_data(Pid, fruit, 1, [apple, 120]),
        ?assertEqual([apple, 120], data_buffer:read_data(Pid, 1))
    end.

delete_removes_row(Pid) ->
    fun() ->
        ok = data_buffer:write_data(Pid, fruit, 1, [apple, 100]),
        ok = data_buffer:write_data(Pid, fruit, 2, [orange, 120]),
        ok = data_buffer:delete_data(Pid, 1),
        ?assertEqual({error, oid_not_found}, data_buffer:read_data(Pid, 1)),
        %% 同じページの他の行は消えない
        ?assertEqual([orange, 120], data_buffer:read_data(Pid, 2))
    end.

delete_twice_is_reported(Pid) ->
    fun() ->
        ok = data_buffer:write_data(Pid, fruit, 1, [apple, 100]),
        ok = data_buffer:delete_data(Pid, 1),
        ?assertEqual({error, oid_not_found}, data_buffer:delete_data(Pid, 1))
    end.

%% 削除で空いたスロット番号が次の挿入で再利用されること
deleted_slot_is_reused(Pid) ->
    fun() ->
        ok = data_buffer:write_data(Pid, fruit, 1, [apple, 100]),
        ok = data_buffer:write_data(Pid, fruit, 2, [orange, 120]),
        ok = data_buffer:delete_data(Pid, 1),
        ok = data_buffer:write_data(Pid, fruit, 3, [grape, 150]),
        ?assertEqual([grape, 150], data_buffer:read_data(Pid, 3)),
        ?assertEqual([orange, 120], data_buffer:read_data(Pid, 2))
    end.

%%%===================================================================
%%% ページ・バッファをまたぐケース
%%%===================================================================

%% 1ページに収まらない件数を書いても全件読めること
many_rows_span_multiple_pages(Pid) ->
    fun() ->
        N = 500,
        [ok = data_buffer:write_data(Pid, fruit, I, [apple, I]) || I <- lists:seq(1, N)],
        [?assertEqual([apple, I], data_buffer:read_data(Pid, I)) || I <- lists:seq(1, N)],
        ok
    end.

%% バッファフレーム数を超えるページを触っても、追い出されたページの内容が失われないこと
rows_survive_buffer_eviction(Pid) ->
    fun() ->
        N = 400,
        [ok = data_buffer:write_data(Pid, fruit, I, [apple, I]) || I <- lists:seq(1, N)],
        %% 逆順に読み直して、追い出し後の再ロードを踏ませる
        [?assertEqual([apple, I], data_buffer:read_data(Pid, I))
         || I <- lists:reverse(lists:seq(1, N))],
        ok
    end.

%% プロセスを落として再起動しても、書き込んだ行がディスクから読めること
rows_survive_restart(Pid) ->
    fun() ->
        [ok = data_buffer:write_data(Pid, fruit, I, [apple, I]) || I <- lists:seq(1, 50)],
        ok = data_buffer:flush(Pid),
        ok = stop(Pid),
        {ok, Pid2} = data_buffer:start_link(),
        try
            [?assertEqual([apple, I], data_buffer:read_data(Pid2, I)) || I <- lists:seq(1, 50)],
            ok
        after
            stop(Pid2)
        end
    end.

two_tables_are_isolated(Pid) ->
    fun() ->
        ok = data_buffer:write_data(Pid, fruit, 1, [apple, 100]),
        ok = data_buffer:write_data(Pid, veggie, 2, [carrot, 80]),
        ?assertEqual([apple, 100], data_buffer:read_data(Pid, 1)),
        ?assertEqual([carrot, 80], data_buffer:read_data(Pid, 2)),
        ?assertEqual(1, row_count(Pid, fruit)),
        ?assertEqual(1, row_count(Pid, veggie))
    end.

%% 1ページに収まらない行はエラーになり、黙って壊れないこと
oversized_row_is_rejected(Pid) ->
    fun() ->
        Big = list_to_binary(lists:duplicate(file_mng:page_size(), $x)),
        ?assertEqual({error, row_too_large}, data_buffer:write_data(Pid, fruit, 1, [Big]))
    end.

%% 更新後の行が元のページに収まらない場合、別ページへ移動しても読めること
update_to_larger_value_relocates(Pid) ->
    fun() ->
        %% ページをほぼ埋める
        Filler = list_to_binary(lists:duplicate(1000, $x)),
        ok = data_buffer:write_data(Pid, fruit, 1, [Filler]),
        ok = data_buffer:write_data(Pid, fruit, 2, [Filler]),
        ok = data_buffer:write_data(Pid, fruit, 3, [small]),
        Bigger = list_to_binary(lists:duplicate(2000, $y)),
        {ok, _} = data_buffer:update_data(Pid, fruit, 3, [Bigger]),
        ?assertEqual([Bigger], data_buffer:read_data(Pid, 3)),
        ?assertEqual([Filler], data_buffer:read_data(Pid, 1)),
        ?assertEqual([Filler], data_buffer:read_data(Pid, 2))
    end.

%%%===================================================================
%%% drop / vacuum
%%%===================================================================

drop_table_discards_data(Pid) ->
    fun() ->
        ok = data_buffer:write_data(Pid, fruit, 1, [apple, 100]),
        ok = data_buffer:write_data(Pid, veggie, 2, [carrot, 80]),
        ok = data_buffer:drop_table(Pid, fruit),
        ?assertEqual({error, oid_not_found}, data_buffer:read_data(Pid, 1)),
        %% 他のテーブルは影響を受けない
        ?assertEqual([carrot, 80], data_buffer:read_data(Pid, 2)),
        %% 作り直したテーブルに古いデータが残らない
        ok = data_buffer:write_data(Pid, fruit, 3, [grape, 150]),
        ?assertEqual([grape, 150], data_buffer:read_data(Pid, 3)),
        ?assertEqual(1, row_count(Pid, fruit))
    end.

vacuum_reclaims_pages(Pid) ->
    fun() ->
        N = 400,
        [ok = data_buffer:write_data(Pid, fruit, I, [apple, I]) || I <- lists:seq(1, N)],
        %% 大半を削除する
        [ok = data_buffer:delete_data(Pid, I) || I <- lists:seq(1, N - 10)],
        {ok, Reclaimed} = data_buffer:vacuum(Pid, fruit),
        ?assert(Reclaimed > 0),
        %% 残った行はvacuum後も読める
        [?assertEqual([apple, I], data_buffer:read_data(Pid, I))
         || I <- lists:seq(N - 9, N)],
        ?assertEqual(10, row_count(Pid, fruit))
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

row_count(Pid, TableName) ->
    ok = data_buffer:flush(Pid),
    dets:foldl(fun({_Oid, #phys_loc{table_name = T}}, Acc) when T =:= TableName -> Acc + 1;
                  (_, Acc) -> Acc
               end, 0, oid_phys_loc).

setup() ->
    Dir = "/tmp/data_buffer_tests_" ++ integer_to_list(erlang:unique_integer([positive])),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    application:set_env(transaction_db, data_dir, Dir),
    {ok, Pid} = data_buffer:start_link(),
    Pid.

cleanup(Pid) ->
    Dir = application:get_env(transaction_db, data_dir, "./data"),
    stop(Pid),
    _ = [file:delete(F) || F <- filelib:wildcard(filename:join(Dir, "*"))],
    _ = file:del_dir(Dir),
    application:unset_env(transaction_db, data_dir),
    ok.

stop(Pid) ->
    case is_process_alive(Pid) of
        false ->
            ok;
        true ->
            Ref = monitor(process, Pid),
            unlink(Pid),
            catch data_buffer:stop(Pid),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 -> ok
            end
    end.
