%%%-------------------------------------------------------------------
%%% @doc 最上位の supervisor。
%%%
%%% 起動順に意味がある。
%%%   tether_log      ログファイルを所有する。これが無いと何も永続化できない
%%%   tether_store    データ本体
%%%   tether_session_sup  クライアントごとのセッションプロセス
%%%   tether_sessions     クライアントID → プロセス の名簿
%%%
%%% 戦略は rest_for_one。ログが死んだらストアもセッションも作り直す。
%%% ストアが死んだらセッションも作り直す。逆は不要。
%%% one_for_all にしないのは、セッションが死んでも下層は無傷だから。
%%%-------------------------------------------------------------------
-module(tether_sup).
-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Dir = application:get_env(tether, dir, "data"),
    Children =
        [#{id => tether_log,
           start => {tether_log, start_link, [Dir]},
           type => worker},
         #{id => tether_store,
           start => {tether_store, start_link, [Dir]},
           type => worker},
         #{id => tether_session_sup,
           start => {tether_session_sup, start_link, []},
           type => supervisor},
         #{id => tether_sessions,
           start => {tether_sessions, start_link, []},
           type => worker}],
    {ok, {#{strategy => rest_for_one, intensity => 3, period => 10}, Children}}.
