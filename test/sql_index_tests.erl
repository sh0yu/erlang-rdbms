%%%-------------------------------------------------------------------
%%% CREATE INDEX / DROP INDEX。
%%%
%%% 以前は全カラムに自動で索引が張られていた。そのため
%%%   - 「索引があるか」が常に真になり、プランナの選択が退化する
%%%   - 索引の無いカラムを引く経路が一度も実行されない
%%% という二つの問題があった。後者は**黙って空を返す**という形で
%%% 潜んでいたので、走査への切り替えを含めて確かめる。
%%%-------------------------------------------------------------------
-module(sql_index_tests).

-include_lib("eunit/include/eunit.hrl").

index_test_() ->
    {foreach, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
     [fun no_index_by_default/1,
      fun create_index_declares_it/1,
      fun create_index_covers_existing_rows/1,
      fun unindexed_column_still_finds_rows/1,
      fun indexed_and_unindexed_agree/1,
      fun update_maintains_only_declared_indexes/1,
      fun delete_maintains_index/1,
      fun drop_index_falls_back_to_scan/1,
      fun drop_table_drops_indexes/1,
      fun index_errors/1]}.

%% 再起動の試験は supervisor の外(ストレージ層だけ)で行う。
%% sup 配下で simple_db_server を落とすと supervisor が作り直すので、
%% 起動をこちらで制御できない。
rebuild_test_() ->
    {foreach, fun db_test_helper:start_storage/0, fun db_test_helper:stop_storage/1,
     [fun only_declared_indexes_are_rebuilt/1]}.

no_index_by_default(_) ->
    fun() ->
        _ = seeded(),
        ?assertEqual({ok, []}, sys_tbl_mng:get_index_column_list(sys_tbl_mng, fruit))
    end.

create_index_declares_it(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "CREATE INDEX fruit_price ON fruit (price)"),
        ?assertEqual({ok, [price]}, sys_tbl_mng:get_index_column_list(sys_tbl_mng, fruit)),
        ?assertEqual(true, (simple_db_server:index_module()):exist_index(fruit, price))
    end.

%% 索引は作った時点で既にある行も拾う。空の索引を作って終わりでは、
%% 作る前に入っていた行が索引検索から見えなくなる。
create_index_covers_existing_rows(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "CREATE INDEX fruit_price ON fruit (price)"),
        ?assertEqual([[<<"apple">>, 100]],
                     simple_db_server:select_data(db(), fruit, price, 100))
    end.

%% 索引の無いカラムでも引ける(走査に落ちる)。
%% ここが無いと、CREATE INDEX していないカラムへの検索が黙って空になる。
unindexed_column_still_finds_rows(_) ->
    fun() ->
        _ = seeded(),
        ?assertEqual([[<<"apple">>, 100]],
                     simple_db_server:select_data(db(), fruit, price, 100)),
        ?assertEqual([], simple_db_server:select_data(db(), fruit, price, 999))
    end.

%% 索引があってもなくても同じ結果になること。
indexed_and_unindexed_agree(_) ->
    fun() ->
        C = seeded(),
        Before = simple_db_server:select_data(db(), fruit, price, 200),
        ok = q(C, "CREATE INDEX fruit_price ON fruit (price)"),
        After = simple_db_server:select_data(db(), fruit, price, 200),
        ?assertEqual([[<<"kiwi">>, 200]], Before),
        ?assertEqual(Before, After)
    end.

%% 索引の無いカラムを触っても壊れないこと。
%% 索引を全カラムに張っていた頃の実装は、存在しないETSへ書きに行って落ちる。
update_maintains_only_declared_indexes(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "CREATE INDEX fruit_price ON fruit (price)"),
        ok = q(C, "BEGIN"),
        {ok, 1} = q(C, "UPDATE fruit SET price = 150, name = 'APPLE' "
                       "WHERE name = 'apple'"),
        ok = q(C, "COMMIT"),
        ?assertEqual([[<<"APPLE">>, 150]],
                     simple_db_server:select_data(db(), fruit, price, 150)),
        ?assertEqual([], simple_db_server:select_data(db(), fruit, price, 100)),
        %% 索引の無い name でも走査で引ける
        ?assertEqual([[<<"APPLE">>, 150]],
                     simple_db_server:select_data(db(), fruit, name, <<"APPLE">>))
    end.

delete_maintains_index(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "CREATE INDEX fruit_price ON fruit (price)"),
        ok = q(C, "BEGIN"),
        {ok, 1} = q(C, "DELETE FROM fruit WHERE price = 100"),
        ok = q(C, "COMMIT"),
        ?assertEqual([], simple_db_server:select_data(db(), fruit, price, 100))
    end.

drop_index_falls_back_to_scan(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "CREATE INDEX fruit_price ON fruit (price)"),
        ok = q(C, "DROP INDEX fruit_price"),
        ?assertEqual({ok, []}, sys_tbl_mng:get_index_column_list(sys_tbl_mng, fruit)),
        %% 索引が消えても結果は変わらない
        ?assertEqual([[<<"apple">>, 100]],
                     simple_db_server:select_data(db(), fruit, price, 100))
    end.

drop_table_drops_indexes(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "CREATE INDEX fruit_price ON fruit (price)"),
        ok = q(C, "DROP TABLE fruit"),
        ?assertEqual({ok, []}, sys_tbl_mng:list_indexes(sys_tbl_mng))
    end.

index_errors(_) ->
    fun() ->
        C = seeded(),
        ok = q(C, "CREATE INDEX i1 ON fruit (price)"),
        ?assertEqual({error, index_already_exists},
                     q(C, "CREATE INDEX i1 ON fruit (name)")),
        ?assertEqual({error, already_indexed},
                     q(C, "CREATE INDEX i2 ON fruit (price)")),
        ?assertEqual({error, {no_such_column, "nosuch"}},
                     q(C, "CREATE INDEX i3 ON fruit (nosuch)")),
        ?assertEqual({error, {table_not_found, "nosuch"}},
                     q(C, "CREATE INDEX i4 ON nosuch (price)")),
        %% 名前が見つからないときの形は、解析で弾いても
        %% カタログで弾いても同じにする
        ?assertEqual({error, {index_not_found, "i1x"}}, q(C, "DROP INDEX i1x")),
        ?assertEqual({error, {index_not_found, i2}}, q(C, "DROP INDEX i2")),
        %% DDL は明示的なトランザクションの中では実行できない
        ok = q(C, "BEGIN"),
        ?assertEqual({error, ddl_in_transaction}, q(C, "CREATE INDEX i9 ON fruit (name)")),
        ok = q(C, "ROLLBACK")
    end.

%% 索引の実体はETS上にしかないので、起動のたびに作り直される。
%% **宣言されたものだけ**が作り直されること。全カラムを作り直すと
%% 索引を明示的にした意味が無くなる。
only_declared_indexes_are_rebuilt(_) ->
    fun() ->
        Db = db(),
        ok = simple_db_server:create_table(Db, fruit, [name, price]),
        ok = simple_db_server:insert_data(Db, fruit, {1, 1}, [<<"apple">>, 100]),
        ok = simple_db_server:insert_data(Db, fruit, {2, 2}, [<<"kiwi">>, 200]),
        ok = simple_db_server:create_index(Db, fruit_price, fruit, price),

        ok = db_test_helper:stop(simple_db_server),
        {ok, _} = simple_db_server:start_link(),

        M = simple_db_server:index_module(),
        ?assertEqual(true, M:exist_index(fruit, price)),
        ?assertEqual(false, M:exist_index(fruit, name)),
        %% 索引経由でも走査経由でも同じ結果
        ?assertEqual([[<<"kiwi">>, 200]],
                     simple_db_server:select_data(db(), fruit, price, 200)),
        ?assertEqual([[<<"kiwi">>, 200]],
                     simple_db_server:select_data(db(), fruit, name, <<"kiwi">>))
    end.

%%%===================================================================

seeded() ->
    C = connect(),
    ok = q(C, "CREATE TABLE fruit (name VARCHAR, price INTEGER)"),
    ok = q(C, "BEGIN"),
    {ok, _} = q(C, "INSERT INTO fruit VALUES ('apple', 100)"),
    {ok, _} = q(C, "INSERT INTO fruit VALUES ('kiwi', 200)"),
    ok = q(C, "COMMIT"),
    C.

db() -> whereis(simple_db_server).
connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
