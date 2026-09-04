%%%-------------------------------------------------------------------
%%% @doc
%%% クライアント接続ごとのquery_execプロセスを起動するスーパーバイザ。
%%% 接続は使い捨てなのでrestartはtemporary(落ちても作り直さない)。
%%% @end
%%%-------------------------------------------------------------------
-module(query_exec_sup).
-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init(_Args) ->
    SupFlags = #{strategy => simple_one_for_one, intensity => 0, period => 1},
    ChildSpec = #{id => query_exec,
                  start => {query_exec, start_link, []},
                  restart => temporary,
                  shutdown => 5000,
                  type => worker,
                  modules => [query_exec]},
    {ok, {SupFlags, [ChildSpec]}}.
