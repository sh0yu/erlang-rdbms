-module(simple_index_tests).

-include_lib("eunit/include/eunit.hrl").

simple_index_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun insert_then_select/1,
      fun duplicate_keys_share_a_slot/1,
      fun insert_is_idempotent/1,
      fun delete_removes_one_oid/1,
      fun delete_removes_key_when_empty/1,
      fun update_moves_oid/1,
      fun update_to_same_value_is_a_noop/1,
      fun missing_index_returns_empty/1,
      fun recreating_table_clears_index/1,
      fun drop_table_removes_index/1]}.

insert_then_select(_) ->
    fun() ->
        ok = simple_index:insert_index(fruit, [{name, apple}, {price, 100}], oid1),
        ?assertEqual([oid1], simple_index:select_index(fruit, name, apple)),
        ?assertEqual([oid1], simple_index:select_index(fruit, price, 100))
    end.

duplicate_keys_share_a_slot(_) ->
    fun() ->
        ok = simple_index:insert_index(fruit, [{price, 100}], oid1),
        ok = simple_index:insert_index(fruit, [{price, 100}], oid2),
        ?assertEqual([oid1, oid2], lists:sort(simple_index:select_index(fruit, price, 100)))
    end.

insert_is_idempotent(_) ->
    fun() ->
        ok = simple_index:insert_index(fruit, [{price, 100}], oid1),
        ok = simple_index:insert_index(fruit, [{price, 100}], oid1),
        ?assertEqual([oid1], simple_index:select_index(fruit, price, 100))
    end.

delete_removes_one_oid(_) ->
    fun() ->
        ok = simple_index:insert_index(fruit, [{price, 100}], oid1),
        ok = simple_index:insert_index(fruit, [{price, 100}], oid2),
        ok = simple_index:delete_index(fruit, price, 100, oid1),
        ?assertEqual([oid2], simple_index:select_index(fruit, price, 100))
    end.

delete_removes_key_when_empty(_) ->
    fun() ->
        ok = simple_index:insert_index(fruit, [{price, 100}], oid1),
        ok = simple_index:delete_index(fruit, price, 100, oid1),
        ?assertEqual([], simple_index:select_index(fruit, price, 100)),
        %% 存在しないものを消しても落ちない
        ok = simple_index:delete_index(fruit, price, 100, oid1),
        ok = simple_index:delete_index(fruit, price, 999, oid1)
    end.

update_moves_oid(_) ->
    fun() ->
        ok = simple_index:insert_index(fruit, [{price, 100}], oid1),
        ok = simple_index:update_index(fruit, price, 100, 200, oid1),
        ?assertEqual([], simple_index:select_index(fruit, price, 100)),
        ?assertEqual([oid1], simple_index:select_index(fruit, price, 200))
    end.

update_to_same_value_is_a_noop(_) ->
    fun() ->
        ok = simple_index:insert_index(fruit, [{price, 100}], oid1),
        ok = simple_index:update_index(fruit, price, 100, 100, oid1),
        ?assertEqual([oid1], simple_index:select_index(fruit, price, 100))
    end.

%% まだ作られていない(あるいは落とされた)インデックスを引いても
%% クラッシュせず空を返すこと。リカバリ中に起こりうる。
missing_index_returns_empty(_) ->
    fun() ->
        ?assertEqual([], simple_index:select_index(nosuch, name, apple)),
        ?assertEqual([], simple_index:select_index(fruit, nosuch_col, apple)),
        ?assertEqual(false, simple_index:exist_index(nosuch, name)),
        ok = simple_index:insert_index(nosuch, [{name, apple}], oid1),
        ok = simple_index:delete_index(nosuch, name, apple, oid1)
    end.

%% テーブルを作り直したら前のインデックスの中身が残らないこと
recreating_table_clears_index(_) ->
    fun() ->
        ok = simple_index:insert_index(fruit, [{name, apple}], oid1),
        ok = simple_index:create_table(fruit, [name, price]),
        ?assertEqual([], simple_index:select_index(fruit, name, apple))
    end.

drop_table_removes_index(_) ->
    fun() ->
        ok = simple_index:insert_index(fruit, [{name, apple}], oid1),
        ok = simple_index:drop_table(fruit, [name, price]),
        ?assertEqual(false, simple_index:exist_index(fruit, name)),
        ?assertEqual([], simple_index:select_index(fruit, name, apple))
    end.

setup() ->
    ok = simple_index:create_table(fruit, [name, price]),
    ok.

cleanup(_) ->
    catch simple_index:drop_table(fruit, [name, price]),
    ok.
