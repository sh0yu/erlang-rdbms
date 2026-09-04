-module(index_tests).

-include_lib("eunit/include/eunit.hrl").

%% ノード分割・併合が起きるだけの件数
-define(N, 200).

index_test_() ->
    {foreach, fun setup/0, fun cleanup/1,
     [fun empty_tree/1,
      fun insert_then_select/1,
      fun select_missing_key/1,
      fun duplicate_keys_share_a_slot/1,
      fun insert_is_idempotent/1,
      fun many_ascending_keys/1,
      fun many_descending_keys/1,
      fun many_random_keys/1,
      fun tree_stays_valid_while_inserting/1,
      fun delete_removes_single_oid/1,
      fun delete_keeps_other_oids_of_same_key/1,
      fun delete_missing_key_is_a_noop/1,
      fun delete_all_ascending/1,
      fun delete_all_descending/1,
      fun delete_all_random/1,
      fun tree_stays_valid_while_deleting/1,
      fun interleaved_insert_delete/1,
      fun update_moves_oid_to_new_key/1,
      fun range_scan/1,
      fun range_scan_bounds/1,
      fun non_integer_keys/1,
      fun drop_table_removes_index/1]}.

%%%===================================================================
%%% 検索
%%%===================================================================

empty_tree(_) ->
    fun() ->
        ?assertEqual([], index:select_index(fruit, name, apple)),
        ?assertEqual(ok, validate(name))
    end.

insert_then_select(_) ->
    fun() ->
        ok = insert(name, apple, oid1),
        ?assertEqual([oid1], index:select_index(fruit, name, apple))
    end.

%% キーが1つしかない葉で、存在しないキーを引いても壊れないこと
select_missing_key(_) ->
    fun() ->
        ok = insert(name, apple, oid1),
        ?assertEqual([], index:select_index(fruit, name, banana)),
        ?assertEqual([], index:select_index(fruit, name, aaa))
    end.

duplicate_keys_share_a_slot(_) ->
    fun() ->
        ok = insert(price, 100, oid1),
        ok = insert(price, 100, oid2),
        ?assertEqual([oid1, oid2], lists:sort(index:select_index(fruit, price, 100)))
    end.

insert_is_idempotent(_) ->
    fun() ->
        ok = insert(price, 100, oid1),
        ok = insert(price, 100, oid1),
        ?assertEqual([oid1], index:select_index(fruit, price, 100))
    end.

%%%===================================================================
%%% 分割
%%%===================================================================

many_ascending_keys(_) ->
    fun() -> insert_and_check(lists:seq(1, ?N)) end.

many_descending_keys(_) ->
    fun() -> insert_and_check(lists:reverse(lists:seq(1, ?N))) end.

many_random_keys(_) ->
    fun() -> insert_and_check(shuffle(lists:seq(1, ?N))) end.

%% 挿入のたびに木の不変条件(高さが揃う・item数の上下限・キー順)が保たれること
tree_stays_valid_while_inserting(_) ->
    fun() ->
        lists:foldl(fun(K, Inserted) ->
                            ok = insert(price, K, oid(K)),
                            ?assertEqual(ok, validate(price)),
                            Now = [K | Inserted],
                            ?assertEqual(lists:sort(Now), keys()),
                            Now
                    end, [], shuffle(lists:seq(1, 60))),
        ok
    end.

%%%===================================================================
%%% 削除
%%%===================================================================

delete_removes_single_oid(_) ->
    fun() ->
        ok = insert(price, 100, oid1),
        ok = delete(price, 100, oid1),
        ?assertEqual([], index:select_index(fruit, price, 100)),
        ?assertEqual(ok, validate(price))
    end.

delete_keeps_other_oids_of_same_key(_) ->
    fun() ->
        ok = insert(price, 100, oid1),
        ok = insert(price, 100, oid2),
        ok = delete(price, 100, oid1),
        ?assertEqual([oid2], index:select_index(fruit, price, 100))
    end.

delete_missing_key_is_a_noop(_) ->
    fun() ->
        ok = insert(price, 100, oid1),
        ok = delete(price, 999, oid1),
        ok = delete(price, 100, no_such_oid),
        ?assertEqual([oid1], index:select_index(fruit, price, 100)),
        ?assertEqual(ok, validate(price))
    end.

delete_all_ascending(_) ->
    fun() -> delete_all(lists:seq(1, ?N), lists:seq(1, ?N)) end.

delete_all_descending(_) ->
    fun() -> delete_all(lists:seq(1, ?N), lists:reverse(lists:seq(1, ?N))) end.

delete_all_random(_) ->
    fun() -> delete_all(shuffle(lists:seq(1, ?N)), shuffle(lists:seq(1, ?N))) end.

%% 削除のたびに不変条件が保たれること。
%% 借用・併合・木の縮みが正しく動いていないとここで落ちる。
tree_stays_valid_while_deleting(_) ->
    fun() ->
        Keys = lists:seq(1, 60),
        [ok = insert(price, K, oid(K)) || K <- shuffle(Keys)],
        lists:foldl(fun(K, Remaining) ->
                            ok = delete(price, K, oid(K)),
                            ?assertEqual(ok, validate(price)),
                            Left = lists:delete(K, Remaining),
                            ?assertEqual(Left, keys()),
                            ?assertEqual([], index:select_index(fruit, price, K)),
                            Left
                    end, Keys, shuffle(Keys)),
        ?assertEqual([], keys())
    end.

interleaved_insert_delete(_) ->
    fun() ->
        Final =
            lists:foldl(
              fun(K, Present) ->
                      case lists:member(K, Present) of
                          true ->
                              ok = delete(price, K, oid(K)),
                              lists:delete(K, Present);
                          false ->
                              ok = insert(price, K, oid(K)),
                              lists:sort([K | Present])
                      end
              end, [], [rand:uniform(40) || _ <- lists:seq(1, 400)]),
        ?assertEqual(ok, validate(price)),
        ?assertEqual(Final, keys())
    end.

update_moves_oid_to_new_key(_) ->
    fun() ->
        ok = insert(price, 100, oid1),
        ok = index:update_index(fruit, price, 100, 200, oid1),
        ?assertEqual([], index:select_index(fruit, price, 100)),
        ?assertEqual([oid1], index:select_index(fruit, price, 200)),
        %% 値が変わらない更新は何もしない
        ok = index:update_index(fruit, price, 200, 200, oid1),
        ?assertEqual([oid1], index:select_index(fruit, price, 200))
    end.

%%%===================================================================
%%% 範囲検索
%%%===================================================================

range_scan(_) ->
    fun() ->
        [ok = insert(price, K, oid(K)) || K <- shuffle(lists:seq(1, ?N))],
        ?assertEqual([oid(K) || K <- lists:seq(10, 20)],
                     index:select_range(fruit, price, 10, 20))
    end.

range_scan_bounds(_) ->
    fun() ->
        [ok = insert(price, K, oid(K)) || K <- lists:seq(1, 20)],
        %% 全件
        ?assertEqual([oid(K) || K <- lists:seq(1, 20)],
                     index:select_range(fruit, price, 0, 100)),
        %% 範囲外
        ?assertEqual([], index:select_range(fruit, price, 100, 200)),
        %% 1件だけ
        ?assertEqual([oid(5)], index:select_range(fruit, price, 5, 5))
    end.

%%%===================================================================
%%% その他
%%%===================================================================

%% 整数以外のキーでも順序が保たれること
non_integer_keys(_) ->
    fun() ->
        Names = [apple, banana, cherry, durian, elderberry, fig, grape],
        [ok = insert(name, N, oid(N)) || N <- shuffle(Names)],
        ?assertEqual(ok, validate(name)),
        ?assertEqual(lists:sort(Names), [K || {K, _} <- index:to_list(fruit_index, name)]),
        ?assertEqual([oid(cherry)], index:select_index(fruit, name, cherry))
    end.

drop_table_removes_index(_) ->
    fun() ->
        ok = insert(name, apple, oid1),
        ok = index:drop_table(fruit, [name, price]),
        %% 作り直したテーブルに前のデータが残らない
        ok = index:create_table(fruit, [name, price]),
        ?assertEqual([], index:select_index(fruit, name, apple))
    end.

%%%===================================================================
%%% Helpers
%%%===================================================================

insert_and_check(Keys) ->
    [ok = insert(price, K, oid(K)) || K <- Keys],
    ?assertEqual(ok, validate(price)),
    ?assertEqual(lists:sort(Keys), keys()),
    [?assertEqual([oid(K)], index:select_index(fruit, price, K)) || K <- Keys],
    ok.

delete_all(InsertOrder, DeleteOrder) ->
    [ok = insert(price, K, oid(K)) || K <- InsertOrder],
    [begin
         ok = delete(price, K, oid(K)),
         ?assertEqual(ok, validate(price))
     end || K <- DeleteOrder],
    ?assertEqual([], keys()),
    [?assertEqual([], index:select_index(fruit, price, K)) || K <- DeleteOrder],
    ok.

insert(Col, Val, Oid) ->
    index:insert_index(fruit, [{Col, Val}], Oid).

delete(Col, Val, Oid) ->
    index:delete_index(fruit, Col, Val, Oid).

keys() ->
    [K || {K, _Oids} <- index:to_list(fruit_index, price)].

validate(Col) ->
    index:validate(fruit_index, Col).

oid(K) -> {oid, K}.

shuffle(L) ->
    [X || {_, X} <- lists:sort([{rand:uniform(), E} || E <- L])].

setup() ->
    index:init(),
    ok = index:create_table(fruit, [name, price]),
    ok.

cleanup(_) ->
    catch index:drop_table(fruit, [name, price]),
    catch ets:delete(ms_index),
    ok.
