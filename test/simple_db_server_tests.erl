-module(simple_db_server_tests).

-include_lib("eunit/include/eunit.hrl").
-include("../include/simple_db_server.hrl").

%%%===================================================================
%%% 純粋関数
%%%===================================================================

convert_set_query_test() ->
    ?assertEqual([{1, banana}, {2, 200}],
                 simple_db_server:convert_set_query([{name, banana}, {price, 200}],
                                                    [name, price])).

convert_set_query_partial_test() ->
    ?assertEqual([{2, 200}],
                 simple_db_server:convert_set_query([{price, 200}], [name, price])).

convert_set_query_ignores_order_test() ->
    ?assertEqual([{1, banana}, {2, 200}],
                 simple_db_server:convert_set_query([{price, 200}, {name, banana}],
                                                    [name, price])).

build_new_val_test() ->
    ?assertEqual([banana, 200],
                 simple_db_server:build_new_val([apple, 100], [{1, banana}, {2, 200}])).

%% 途中のカラムだけを更新しても他のカラムがずれないこと
build_new_val_partial_test() ->
    ?assertEqual([apple, 200, red],
                 simple_db_server:build_new_val([apple, 100, red], [{2, 200}])),
    ?assertEqual([banana, 100, red],
                 simple_db_server:build_new_val([apple, 100, red], [{1, banana}])),
    ?assertEqual([apple, 100, blue],
                 simple_db_server:build_new_val([apple, 100, red], [{3, blue}])).

build_new_val_no_change_test() ->
    ?assertEqual([apple, 100], simple_db_server:build_new_val([apple, 100], [])).

%%%===================================================================
%%% ストレージエンジン
%%%===================================================================

storage_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun create_and_select/1,
      fun create_duplicate_table/1,
      fun create_table_rejects_bad_columns/1,
      fun insert_rejects_unknown_table/1,
      fun insert_rejects_wrong_column_count/1,
      fun insert_is_idempotent/1,
      fun select_unknown_table/1,
      fun select_no_match/1,
      fun select_by_each_column/1,
      fun update_changes_indexed_value/1,
      fun update_rejects_unknown_column/1,
      fun update_reports_count/1,
      fun delete_removes_from_index/1,
      fun drop_table_removes_everything/1,
      fun drop_unknown_table/1,
      fun indexes_are_rebuilt_on_restart/1]}.

create_and_select(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ?assertEqual([[apple, 100]], select(fruit, name, apple))
    end.

create_duplicate_table(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ?assertEqual({error, table_already_exists}, create(fruit, [name, price]))
    end.

create_table_rejects_bad_columns(_) ->
    fun() ->
        ?assertEqual({error, invalid_column_list}, create(fruit, [])),
        ?assertEqual({error, invalid_column_list}, create(fruit, [name, name]))
    end.

insert_rejects_unknown_table(_) ->
    fun() ->
        ?assertEqual({error, table_not_found}, insert(nosuch, 1, [apple, 100]))
    end.

insert_rejects_wrong_column_count(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ?assertEqual({error, column_count_mismatch}, insert(fruit, 1, [apple]))
    end.

%% リカバリでREDOを再適用しても行が二重にならないこと
insert_is_idempotent(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ok = insert(fruit, 1, [apple, 100]),
        ?assertEqual([[apple, 100]], select(fruit, name, apple))
    end.

%% 同じOidを別の値で上書きしたとき、古い値ではもう引けないこと
insert_overwrite_reindexes_test_() ->
    {setup, fun setup/0, fun cleanup/1,
     fun(_) ->
         {"上書き後は古い値で引けない",
          fun() ->
              ok = create(fruit, [name, price]),
              ok = insert(fruit, 1, [apple, 100]),
              ok = insert(fruit, 1, [banana, 200]),
              ?assertEqual([], select(fruit, name, apple)),
              ?assertEqual([[banana, 200]], select(fruit, name, banana))
          end}
     end}.

select_unknown_table(_) ->
    fun() ->
        ?assertEqual({error, table_not_found}, select(nosuch, name, apple))
    end.

select_no_match(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ?assertEqual([], select(fruit, name, durian))
    end.

select_by_each_column(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ok = insert(fruit, 2, [orange, 150]),
        ok = insert(fruit, 3, [banana, 150]),
        ?assertEqual([[apple, 100]], select(fruit, name, apple)),
        ?assertEqual([[banana, 150], [orange, 150]],
                     lists:sort(select(fruit, price, 150)))
    end.

update_changes_indexed_value(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        {ok, 1} = update(fruit, [{price, 120}], name, apple),
        ?assertEqual([[apple, 120]], select(fruit, name, apple)),
        %% 更新後の値でインデックスが引ける
        ?assertEqual([[apple, 120]], select(fruit, price, 120)),
        %% 更新前の値ではもう引けない
        ?assertEqual([], select(fruit, price, 100))
    end.

update_rejects_unknown_column(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ?assertEqual({error, {unknown_columns, [colour]}},
                     update(fruit, [{colour, red}], name, apple))
    end.

update_reports_count(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 150]),
        ok = insert(fruit, 2, [orange, 150]),
        ?assertEqual({ok, 2}, update(fruit, [{price, 160}], price, 150)),
        ?assertEqual({ok, 0}, update(fruit, [{price, 170}], price, 999))
    end.

delete_removes_from_index(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ok = insert(fruit, 2, [orange, 150]),
        ok = delete(fruit, 1),
        ?assertEqual([], select(fruit, name, apple)),
        ?assertEqual([], select(fruit, price, 100)),
        ?assertEqual([[orange, 150]], select(fruit, name, orange))
    end.

drop_table_removes_everything(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ok = drop(fruit),
        ?assertEqual({error, table_not_found}, select(fruit, name, apple)),
        %% 作り直したテーブルに以前の行が残らない
        ok = create(fruit, [name, price]),
        ?assertEqual([], select(fruit, name, apple))
    end.

drop_unknown_table(_) ->
    fun() ->
        ?assertEqual({error, table_not_found}, drop(nosuch))
    end.

%% インデックスはETS上にしかないので、再起動時に作り直せていること
indexes_are_rebuilt_on_restart(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = insert(fruit, 1, [apple, 100]),
        ok = insert(fruit, 2, [orange, 150]),
        ok = restart_engine(),
        ?assertEqual([[apple, 100]], select(fruit, name, apple)),
        ?assertEqual([[orange, 150]], select(fruit, price, 150))
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

create(T, C) -> simple_db_server:create_table(simple_db_server, T, C).
drop(T) -> simple_db_server:drop_table(simple_db_server, T).
insert(T, Oid, V) -> simple_db_server:insert_data(simple_db_server, T, Oid, V).
delete(T, Oid) -> simple_db_server:delete_data(simple_db_server, T, Oid).
select(T, C, V) -> simple_db_server:select_data(simple_db_server, T, C, V).
update(T, S, C, V) -> simple_db_server:update_data(simple_db_server, T, S, C, V).

%% simple_db_serverだけを再起動して、インデックス再構築を踏ませる
restart_engine() ->
    ok = db_test_helper:stop(simple_db_server),
    {ok, _} = simple_db_server:start_link(),
    ok.

setup() ->
    db_test_helper:start_storage().

cleanup(Ctx) ->
    db_test_helper:stop_storage(Ctx).
