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
      fun no_indexes_by_default/1,
      fun create_and_drop_index/1,
      fun index_rejects_unknown_table_or_column/1,
      fun index_name_and_column_are_unique/1,
      fun dropping_table_drops_its_indexes/1,
      fun indexes_survive_restart/1,
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

%% 索引は自動では張らない。
%% 以前は全カラムに張っていたが、それだと「索引があるか」が常に真になり、
%% プランナのアクセスパス選択が退化する。
no_indexes_by_default(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ?assertEqual({ok, []}, idx_cols(fruit)),
        ?assertEqual({ok, []}, sys_tbl_mng:get_indexes(sys_tbl_mng, fruit))
    end.

create_and_drop_index(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = sys_tbl_mng:create_index(sys_tbl_mng, fruit_price, fruit, price),
        ?assertEqual({ok, [price]}, idx_cols(fruit)),
        ?assertMatch({ok, [{index, fruit_price, fruit, price}]},
                     sys_tbl_mng:get_indexes(sys_tbl_mng, fruit)),
        ?assertEqual({ok, fruit, price}, sys_tbl_mng:drop_index(sys_tbl_mng, fruit_price)),
        ?assertEqual({ok, []}, idx_cols(fruit)),
        ?assertEqual({error, {index_not_found, fruit_price}},
                     sys_tbl_mng:drop_index(sys_tbl_mng, fruit_price))
    end.

index_rejects_unknown_table_or_column(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ?assertEqual({error, table_not_found},
                     sys_tbl_mng:create_index(sys_tbl_mng, i, nosuch, price)),
        ?assertEqual({error, {column_not_found, nosuch}},
                     sys_tbl_mng:create_index(sys_tbl_mng, i, fruit, nosuch))
    end.

index_name_and_column_are_unique(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = sys_tbl_mng:create_index(sys_tbl_mng, i1, fruit, price),
        ?assertEqual({error, index_already_exists},
                     sys_tbl_mng:create_index(sys_tbl_mng, i1, fruit, name)),
        ?assertEqual({error, already_indexed},
                     sys_tbl_mng:create_index(sys_tbl_mng, i2, fruit, price))
    end.

%% テーブルを落としたら索引の定義も消す。残すと、同名のテーブルを
%% 作り直したときに実体の無い索引があることになる。
dropping_table_drops_its_indexes(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = sys_tbl_mng:create_index(sys_tbl_mng, fruit_price, fruit, price),
        ok = sys_tbl_mng:drop_table(sys_tbl_mng, fruit),
        ?assertEqual({ok, []}, sys_tbl_mng:list_indexes(sys_tbl_mng)),
        ok = create(fruit, [name, price]),
        ?assertEqual({ok, []}, idx_cols(fruit))
    end.

indexes_survive_restart(_) ->
    fun() ->
        ok = create(fruit, [name, price]),
        ok = sys_tbl_mng:create_index(sys_tbl_mng, fruit_price, fruit, price),
        ok = db_test_helper:stop(sys_tbl_mng),
        {ok, _} = sys_tbl_mng:start_link(),
        ?assertEqual({ok, [price]}, idx_cols(fruit))
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
idx_cols(T) -> sys_tbl_mng:get_index_column_list(sys_tbl_mng, T).
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
