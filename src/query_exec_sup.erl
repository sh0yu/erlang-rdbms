-module(query_exec_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% 各サーバを起動する
init(_Args) ->
    SupFlags = {simple_one_for_one, 3, 30},
    QueryExecSpec = {query_exec, {query_exec, start_link, []}, temporary, 1000, worker, [query_exec]},
    {ok, {SupFlags, [QueryExecSpec]}}.