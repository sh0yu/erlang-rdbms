%%%-------------------------------------------------------------------
%%% @doc transaction_dbアプリケーションのエントリポイント。
%%%-------------------------------------------------------------------
-module(app).
-behaviour(application).

-export([start/2, stop/1]).

start(normal, _Args) ->
    sup:start_link();
start(_Type, _Args) ->
    {error, badarg}.

stop(_State) ->
    ok.
