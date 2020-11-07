-module(sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% 各サーバを起動する
init(_Args) ->
    SupFlags = {one_for_one, 3, 5},
    SimpleDbServerSpec = {simple_db_server, {simple_db_server, start_link, []}, permanent, brutal_kill, worker, [simple_db_server]},
    SysTblMngSpec = {sys_tbl_mng, {sys_tbl_mng, start_link, []}, permanent, brutal_kill, worker, [sys_tbl_mng]},
    TxMngSpec = {tx_mng, {tx_mng, start_link, []}, permanent, brutal_kill, worker, [tx_mng]},
    DataBufferSpec = {data_buffer, {data_buffer, start_link, []}, permanent, brutal_kill, worker, [data_buffer]},
    QueryExecSupSpec = {query_exec_sup, {query_exec_sup, start_link, []}, permanent, brutal_kill, supervisor, [query_exec_sup]},
    LogUtilSpec = {log_util, {log_util, start_link, []}, permanent, brutal_kill, supervisor, [log_util]},
    ChildSpec = [SysTblMngSpec, DataBufferSpec, LogUtilSpec, SimpleDbServerSpec, TxMngSpec, QueryExecSupSpec],
    {ok, {SupFlags, ChildSpec}}.

%% TODO:エラーが発生してプロセスが落ちた時、recoverプロセスのrecoverを実行し、コミット済みのデータをディスクに書き込む