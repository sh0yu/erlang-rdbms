%%%-------------------------------------------------------------------
%%% @doc
%%% クライアントIDからセッションプロセスを引く名簿。
%%%
%%% 引くのは ETS を直接読む。**プロセスを経由しない。**
%%% 名簿を gen_server にして毎回 call すると、全クライアントの
%%% 全要求がそこで直列化される。100万セッションを抱える設計で
%%% それをやると、名簿が唯一の詰まりどころになる。
%%%
%%% 作るときだけ gen_server を通す。同じクライアントが同時に2回
%%% 来ても1つしか作らないためで、これは稀な経路なので直列でよい。
%%%
%%% 死んだセッションは monitor で検知して名簿から消す。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_sessions).
-behaviour(gen_server).

-export([start_link/0, lookup/1, ensure/1, close/1, count/0, all/0]).
-export([subscribe/2, unsubscribe/2, publish/2, subscribers/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TAB,  ?MODULE).
%% コレクション → 購読しているセッション。bag。
%% これがあるので、書き込み1件あたりの配布費用が
%% 全セッション数ではなく**そのコレクションの購読者数**に比例する。
-define(SUBS, tether_subs).

-record(s, {mons = #{} :: #{reference() => binary()}}).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc 速い経路。プロセスを経由しない。
-spec lookup(binary()) -> {ok, pid()} | none.
lookup(Client) ->
    case ets:lookup(?TAB, Client) of
        [{_, Pid}] -> {ok, Pid};
        []         -> none
    end.

-spec ensure(binary()) -> {ok, pid()} | {error, term()}.
ensure(Client) when is_binary(Client) ->
    case lookup(Client) of
        {ok, Pid} -> {ok, Pid};
        none      -> gen_server:call(?MODULE, {start, Client})
    end.

-spec close(binary()) -> ok.
close(Client) -> gen_server:call(?MODULE, {close, Client}).

-spec count() -> non_neg_integer().
count() -> ets:info(?TAB, size).

-spec subscribe(binary(), pid()) -> ok.
subscribe(Coll, Pid) -> gen_server:call(?MODULE, {subscribe, Coll, Pid}).

-spec unsubscribe(binary(), pid()) -> ok.
unsubscribe(Coll, Pid) -> gen_server:call(?MODULE, {unsubscribe, Coll, Pid}).

-spec subscribers(binary()) -> [pid()].
subscribers(Coll) -> [P || {_, P} <- ets:lookup(?SUBS, Coll)].

%%----------------------------------------------------------------------
%% @doc 変更を購読者へ配る。
%%
%% **ストアからは cast で渡すだけ**にして、扇形配信はこちらでやる。
%% ストアの中で1件ずつ送ると、購読者数がそのまま書き込みの遅延になる。
%% 単一のプロセスで受けるので、配る順序は書き込みの順序と一致する。
%%----------------------------------------------------------------------
-spec publish(non_neg_integer(), [tether_data:key()]) -> ok.
publish(_Version, [])   -> ok;
publish(Version, Keys)  -> gen_server:cast(?MODULE, {publish, Version, Keys}).

-spec all() -> [{binary(), pid()}].
all() -> lists:sort(ets:tab2list(?TAB)).

%%%===================================================================

init([]) ->
    _ = ets:new(?TAB,  [named_table, protected, set, {read_concurrency, true}]),
    _ = ets:new(?SUBS, [named_table, protected, bag, {read_concurrency, true}]),
    {ok, #s{}}.

handle_call({start, Client}, _From, S) ->
    %% 自分の直列化の中でもう一度確かめる。待っている間に
    %% 別の呼び出しが作っているかもしれない。
    case lookup(Client) of
        {ok, Pid} -> {reply, {ok, Pid}, S};
        none ->
            case tether_session_sup:start_session(Client) of
                {ok, Pid} ->
                    Ref = erlang:monitor(process, Pid),
                    true = ets:insert(?TAB, {Client, Pid}),
                    {reply, {ok, Pid}, S#s{mons = (S#s.mons)#{Ref => Client}}};
                E ->
                    {reply, E, S}
            end
    end;
handle_call({close, Client}, _From, S) ->
    case lookup(Client) of
        {ok, Pid} -> ok = gen_server:stop(Pid);
        none      -> ok
    end,
    {reply, ok, S};
handle_call({subscribe, Coll, Pid}, _From, S) ->
    true = ets:insert(?SUBS, {Coll, Pid}),
    {reply, ok, S};
handle_call({unsubscribe, Coll, Pid}, _From, S) ->
    true = ets:delete_object(?SUBS, {Coll, Pid}),
    {reply, ok, S};
handle_call(_R, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({publish, Version, Keys}, S) ->
    ByColl = lists:foldl(fun({C, _} = K, M) ->
                                 M#{C => [K | maps:get(C, M, [])]}
                         end, #{}, Keys),
    maps:foreach(
      fun(Coll, Ks) ->
              _ = [Pid ! {changed, Version, Ks} || {_, Pid} <- ets:lookup(?SUBS, Coll)],
              ok
      end, ByColl),
    {noreply, S};
handle_cast(_M, S) -> {noreply, S}.

handle_info({'DOWN', Ref, process, Pid, _R}, #s{mons = M} = S) ->
    case maps:take(Ref, M) of
        {Client, M1} ->
            true = ets:delete(?TAB, Client),
            %% 死んだセッションの購読も消す。残すと配布先が増え続ける。
            _ = ets:match_delete(?SUBS, {'_', Pid}),
            {noreply, S#s{mons = M1}};
        error ->
            {noreply, S}
    end;
handle_info(_M, S) -> {noreply, S}.
