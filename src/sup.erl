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
    TxMngSpec = {tx_mng, {tx_mng, start_link, []}, permanent, brutal_kill, worker, [tx_mng]},
    QueryExecSupSpec = {query_exec_sup, {query_exec_sup, start_link, []}, permanent, brutal_kill, worker, [query_exec_sup]},
    ChildSpec = [SimpleDbServerSpec, TxMngSpec, QueryExecSupSpec],
    {ok, {SupFlags, ChildSpec}}.