%%%-------------------------------------------------------------------
%%% @doc
%%% セッションプロセスの supervisor。
%%%
%%% 再起動方針は temporary。**セッションが死んでも自動では作り直さない。**
%%%
%%% 意図的な選択である。セッションの記憶はログにあるので、プロセスは
%%% いつ消えても構わない。次の要求が来たときに作り直せば、記憶は
%%% ログから戻ってくる。自動再起動にすると、暴走するクライアントが
%%% 再起動の嵐を起こし、supervisor の強度上限に触れて**他の
%%% 全セッションを巻き込む**。それは多テナントのサービスとして最悪の
%%% 振る舞いになる。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_session_sup).
-behaviour(supervisor).

-export([start_link/0, start_session/1, init/1]).

start_link() -> supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_session(binary()) -> {ok, pid()} | {error, term()}.
start_session(Client) -> supervisor:start_child(?MODULE, [Client]).

init([]) ->
    {ok, {#{strategy => simple_one_for_one, intensity => 0, period => 1},
          [#{id => tether_session,
             start => {tether_session, start_link, []},
             restart => temporary,
             shutdown => 5000,
             type => worker}]}}.
