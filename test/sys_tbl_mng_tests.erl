-module(sys_tbl_mng_tests).

-include_lib("eunit/include/eunit.hrl").

sys_tbl_mng_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun create_and_read_back/1,
      fun duplicate_create_is_rejected/1,
      fun invalid_column_lists_are_rejected/1,
      fun unknown_table_is_reported/1,
      fun drop_removes_definition/1,
      fun drop_unknown_table/1,
      fun list_tables_is_sorted/1,
      fun index_columns_default_to_all_columns/1,
      fun definitions_survive_restart/1]}.

create_and_read_back(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ?assertEqual(true, exist(fruit)),
        ?assertEqual({ok, [name, price]}, columns(fruit))
    end.

duplicate_create_is_rejected(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ?assertEqual({error, table_already_exists}, create(fruit, [other])),
        %% 元の定義が上書きされていないこと
        ?assertEqual({ok, [name, price]}, columns(fruit))
    end.

%% 不正な定義を通すと、行とカラムのzipが後段でクラッシュする
invalid_column_lists_are_rejected(_) ->
    fun() ->
        ?assertEqual({error, invalid_column_list}, create(fruit, [])),
        ?assertEqual({error, invalid_column_list}, create(fruit, [name, name])),
        ?assertEqual({error, invalid_column_list}, create(fruit, ["name"])),
        ?assertEqual({error, invalid_column_list}, create(fruit, not_a_list)),
        ?assertEqual(false, exist(fruit))
    end.

unknown_table_is_reported(_) ->
    fun() ->
        ?assertEqual(false, exist(nosuch)),
        ?assertEqual({error, table_not_found}, columns(nosuch))
    end.

drop_removes_definition(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = drop(fruit),
        ?assertEqual(false, exist(fruit)),
        ?assertEqual({error, table_not_found}, columns(fruit))
    end.

drop_unknown_table(_) ->
    fun() ->
        ?assertEqual({error, table_not_found}, drop(nosuch))
    end.

list_tables_is_sorted(_) ->
    fun() ->
        ok = create(veggie, [name]),
        ok = create(fruit, [name]),
        ?assertEqual({ok, [fruit, veggie]}, sys_tbl_mng:list_tables(sys_tbl_mng))
    end.

index_columns_default_to_all_columns(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ?assertEqual({ok, [name, price]},
                     sys_tbl_mng:get_index_column_list(sys_tbl_mng, fruit))
    end.

%% カタログはDETSに永続化されるので、再起動しても残ること
definitions_survive_restart(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = db_test_helper:stop(sys_tbl_mng),
        {ok, _} = sys_tbl_mng:start_link(),
        ?assertEqual({ok, [name, price]}, columns(fruit))
    end.

create(T, C) -> sys_tbl_mng:create_table(sys_tbl_mng, T, C).
drop(T) -> sys_tbl_mng:drop_table(sys_tbl_mng, T).
columns(T) -> sys_tbl_mng:get_column_list(sys_tbl_mng, T).
exist(T) -> sys_tbl_mng:exist_table(sys_tbl_mng, T).

setup() ->
    Dir = db_test_helper:tmp_dir("systbl"),
    application:set_env(transaction_db, data_dir, Dir),
    {ok, _} = sys_tbl_mng:start_link(),
    Dir.

cleanup(Dir) ->
    ok = db_test_helper:stop(sys_tbl_mng),
    application:unset_env(transaction_db, data_dir),
    db_test_helper:rm_rf(Dir),
    ok.
