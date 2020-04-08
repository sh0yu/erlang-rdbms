-module(app).

-behaviour(application).

-export([start/2, stop/1]).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% application callback functions

start(normal, _Args) ->
    case sup:start_link() of
	{ok, Pid} ->
	    {ok, Pid, {normal, _Args}};
	Error ->
	    Error
    end;
start(_, _) ->
    {error, badarg}.

stop(_StartArgs) ->
    ok.
