%%%-------------------------------------------------------------------
%%% @doc
%%% テスト用のサーバ起動・停止ヘルパ。
%%% テストごとに専用のdata_dirを割り当てるため、
%%% ディスク上の状態が他のテストに漏れない。
%%% @end
%%%-------------------------------------------------------------------
-module(db_test_helper).

-export([start_storage/0, stop_storage/1, start_db/0, stop_db/1]).
-export([stop/1, tmp_dir/1, rm_rf/1, with_db/1]).

%%----------------------------------------------------------------------
%% @doc ストレージ層(sys_tbl_mng + data_buffer + simple_db_server)を起動する。
%%----------------------------------------------------------------------
start_storage() ->
    Dir = tmp_dir("storage"),
    application:set_env(transaction_db, data_dir, Dir),
    {ok, _} = sys_tbl_mng:start_link(),
    {ok, _} = data_buffer:start_link(),
    {ok, _} = simple_db_server:start_link(),
    Dir.

stop_storage(Dir) ->
    ok = stop(simple_db_server),
    ok = stop(data_buffer),
    ok = stop(sys_tbl_mng),
    application:unset_env(transaction_db, data_dir),
    rm_rf(Dir),
    ok.

%%----------------------------------------------------------------------
%% @doc トランザクション層まで含めた全サーバを起動する。
%%----------------------------------------------------------------------
start_db() ->
    Dir = tmp_dir("db"),
    application:set_env(transaction_db, data_dir, Dir),
    {ok, SupPid} = sup:start_link(),
    {SupPid, Dir}.

stop_db({SupPid, Dir}) ->
    Ref = monitor(process, SupPid),
    unlink(SupPid),
    exit(SupPid, shutdown),
    receive {'DOWN', Ref, process, SupPid, _} -> ok
    after 5000 -> ok
    end,
    application:unset_env(transaction_db, data_dir),
    rm_rf(Dir),
    ok.

%%----------------------------------------------------------------------
%% @doc 全サーバを起動してFunを実行し、必ず後片付けする。
%%----------------------------------------------------------------------
with_db(Fun) ->
    Ctx = start_db(),
    try Fun()
    after stop_db(Ctx)
    end.

%%----------------------------------------------------------------------
%% @doc 名前付きプロセスが完全に終了するまで待って止める。
%% ETSやDETSを持つプロセスは、終了前に次のテストが起動すると
%% 名前の衝突でクラッシュするため、DOWNを待つ必要がある。
%%----------------------------------------------------------------------
stop(Name) ->
    case whereis(Name) of
        undefined ->
            ok;
        Pid ->
            Ref = monitor(process, Pid),
            unlink(Pid),
            catch gen_server:call(Pid, terminate, 5000),
            receive {'DOWN', Ref, process, Pid, _} -> ok
            after 5000 ->
                    exit(Pid, kill),
                    receive {'DOWN', Ref, process, Pid, _} -> ok
                    after 5000 -> ok
                    end
            end
    end.

tmp_dir(Prefix) ->
    Dir = filename:join("/tmp", "erlang_rdbms_" ++ Prefix ++ "_"
                        ++ integer_to_list(erlang:unique_integer([positive]))),
    ok = filelib:ensure_dir(filename:join(Dir, "x")),
    Dir.

rm_rf(Dir) ->
    _ = [file:delete(F) || F <- filelib:wildcard(filename:join(Dir, "*"))],
    _ = file:del_dir(Dir),
    ok.
