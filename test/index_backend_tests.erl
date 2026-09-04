%%%-------------------------------------------------------------------
%%% インデックス実装を差し替えても、エンジンの振る舞いが変わらないこと。
%%% simple_index(既定)とindex(B+tree)の両方で同じ検証を行う。
%%%-------------------------------------------------------------------
-module(index_backend_tests).

-include_lib("eunit/include/eunit.hrl").

backends_test_() ->
    [{"simple_index", {setup,
                       fun() -> setup(simple_index) end,
                       fun cleanup/1,
                       fun checks/1}},
     {"b+tree index", {setup,
                       fun() -> setup(index) end,
                       fun cleanup/1,
                       fun checks/1}}].

checks(_) ->
    [?_test(crud_works()),
     ?_test(restart_rebuilds_indexes())].

crud_works() ->
    ok = create(fruit, [name, price]),
    ok = insert(fruit, 1, [apple, 100]),
    ok = insert(fruit, 2, [orange, 150]),
    ok = insert(fruit, 3, [banana, 150]),
    ?assertEqual([[apple, 100]], select(fruit, name, apple)),
    ?assertEqual([[banana, 150], [orange, 150]], lists:sort(select(fruit, price, 150))),
    {ok, 1} = update(fruit, [{price, 120}], name, apple),
    ?assertEqual([[apple, 120]], select(fruit, price, 120)),
    ?assertEqual([], select(fruit, price, 100)),
    ok = delete(fruit, 2),
    ?assertEqual([], select(fruit, name, orange)),
    ?assertEqual([[banana, 150]], select(fruit, price, 150)),
    ok = drop(fruit).

restart_rebuilds_indexes() ->
    ok = create(veggie, [name, price]),
    [ok = insert(veggie, I, [carrot, I]) || I <- lists:seq(1, 30)],
    ok = db_test_helper:stop(simple_db_server),
    {ok, _} = simple_db_server:start_link(),
    ?assertEqual(30, length(select(veggie, name, carrot))),
    ?assertEqual([[carrot, 7]], select(veggie, price, 7)),
    ok = drop(veggie).

create(T, C) -> simple_db_server:create_table(simple_db_server, T, C).
drop(T) -> simple_db_server:drop_table(simple_db_server, T).
insert(T, Oid, V) -> simple_db_server:insert_data(simple_db_server, T, Oid, V).
delete(T, Oid) -> simple_db_server:delete_data(simple_db_server, T, Oid).
select(T, C, V) -> simple_db_server:select_data(simple_db_server, T, C, V).
update(T, S, C, V) -> simple_db_server:update_data(simple_db_server, T, S, C, V).

setup(Mod) ->
    application:set_env(transaction_db, index_module, Mod),
    db_test_helper:start_storage().

cleanup(Dir) ->
    db_test_helper:stop_storage(Dir),
    catch ets:delete(ms_index),
    application:unset_env(transaction_db, index_module),
    ok.
