%%%-------------------------------------------------------------------
%%% @doc
%%% ブラウザで見る local-first のデモ。
%%%
%%%   bin/tether web   → http://127.0.0.1:8080
%%%
%%% 端末2台とサーバを並べて、圏外にした端末の状態が離れ、
%%% 復帰したときに揃うところを見る。テキストでは伝わりにくい。
%%%
%%% 外部依存は無い。OTP の inets(httpd + mod_esi) だけを使う。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_web).
-behaviour(gen_server).

-export([start/0, start/1, stop/0]).
-export([state/3, act/3]).                    % mod_esi
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(COLL, <<"orders">>).
-define(SKU,  <<"sku:1">>).
-define(NAMES, [<<"alice">>, <<"bob">>]).

-record(w, {clients = #{} :: #{binary() => pid()},
            counter = 0   :: non_neg_integer()}).

%%%===================================================================

start() -> start(8080).

start(Port) ->
    ok = logger:set_primary_config(level, error),
    Dir = filename:absname(filename:join("data", "web")),
    _ = application:load(tether),
    _ = [file:delete(F) || F <- filelib:wildcard(filename:join(Dir, "*"))],
    application:set_env(tether, dir, Dir),
    {ok, _} = application:ensure_all_started(tether),
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = gen_server:start_link({local, ?MODULE}, ?MODULE, [], []),
    Www = filename:absname(filename:join("priv", "www")),
    case inets:start(httpd, [{port, Port}, {bind_address, {127,0,0,1}},
                             {server_name, "tether"}, {server_root, Www},
                             {document_root, Www}, {directory_index, ["index.html"]},
                             %% mod_alias と mod_dir を外すと、/ が 403 になる(実際に踏んだ)
                             %% 順序に意味がある。mod_esi は mod_dir より**前**。
                             %% 逆にすると GET の ESI が 404 になる(実際に踏んだ)。
                             %% mod_alias を外すと / が 403 になる(これも踏んだ)。
                             {modules, [mod_alias, mod_esi, mod_dir,
                                        mod_get, mod_head, mod_log]},
                             {erl_script_alias, {"/api", [tether_web]}},
                             {mime_types, [{"html","text/html"},{"css","text/css"},
                                           {"js","application/javascript"}]}]) of
        {ok, Pid} ->
            io:format("~n  tether — local-first のデモ~n"
                      "  http://127.0.0.1:~p~n~n", [Port]),
            {ok, Pid};
        {error, R} ->
            io:format(standard_error, "ポート ~p を開けません: ~p~n", [Port, R]),
            halt(1)
    end.

stop() ->
    _ = gen_server:stop(?MODULE),
    application:stop(tether).

%%%===================================================================
%%% HTTP
%%%===================================================================

state(Sid, _Env, _In) -> json(Sid, gen_server:call(?MODULE, state, 30000)).

act(Sid, _Env, In) ->
    %% In は "client=alice&action=sell" 形式
    P = uri_string:dissect_query(In),
    C = list_to_binary(proplists:get_value("client", P, "alice")),
    A = list_to_atom(proplists:get_value("action", P, "noop")),
    _ = gen_server:call(?MODULE, {act, C, A}, 60000),
    json(Sid, gen_server:call(?MODULE, state, 30000)).

json(Sid, Term) ->
    ok = mod_esi:deliver(Sid, "Content-Type: application/json; charset=utf-8\r\n\r\n"),
    ok = mod_esi:deliver(Sid, binary_to_list(enc(Term))).

%%%===================================================================
%%% gen_server
%%%===================================================================

init([]) ->
    {ok, _} = tether:stock(?SKU, 20),
    Cs = maps:from_list([{N, begin {ok, P} = tether_client:open(N, ?COLL), P end}
                         || N <- ?NAMES]),
    {ok, #w{clients = Cs}}.

handle_call(state, _From, #w{clients = Cs} = S) ->
    {A, G} = tether:pool(?SKU),
    {_, ServerRows} = tether_store:snapshot(?COLL),
    {reply,
     #{server => #{version => tether_store:version(),
                   available => A, granted => G,
                   orders => rows(ServerRows)},
       clients => [client_state(N, maps:get(N, Cs)) || N <- ?NAMES]}, S};

handle_call({act, Name, Action}, _From, #w{clients = Cs} = S) ->
    Pid = maps:get(Name, Cs, undefined),
    {reply, ok, do(Action, Name, Pid, S)};

handle_call(_R, _From, S) -> {reply, {error, unknown}, S}.

handle_cast(_M, S) -> {noreply, S}.
handle_info(_M, S) -> {noreply, S}.
terminate(_R, #w{clients = Cs}) ->
    _ = [catch tether_client:close(P) || P <- maps:values(Cs)], ok.

%%%===================================================================
%%% 操作
%%%===================================================================

do(offline, _N, Pid, S) -> _ = tether_client:offline(Pid), S;
do(online,  _N, Pid, S) -> _ = tether_client:online(Pid),  S;
do(sync,    _N, Pid, S) -> _ = tether_client:sync(Pid),    S;

%% 手元にメモを書く(楽観的)。サーバが拒む可能性がある
do(write, Name, Pid, #w{counter = C} = S) ->
    Id = <<Name/binary, "-", (integer_to_binary(C + 1))/binary>>,
    _ = tether_client:write(Pid, [{put, {?COLL, Id}, <<"draft">>}]),
    S#w{counter = C + 1};

%% 預かりを取る(ネットワークが要る)
do(acquire, _N, Pid, S) ->
    _ = tether_client:acquire(Pid, ?SKU, 4, 600000), S;

%% 1個売る。預かりの範囲なら**圏外でも確定的**
do(sell, Name, Pid, #w{counter = C} = S) ->
    Id = <<Name/binary, "-o", (integer_to_binary(C + 1))/binary>>,
    _ = tether_client:write(Pid, [{consume, ?SKU, 1},
                                  {put, {?COLL, Id}, <<"sold">>}]),
    S#w{counter = C + 1};

%% 第三者がサーバ側を直接書き換える(圏外の端末が見落とす変更)
do(server_write, _N, _Pid, #w{counter = C} = S) ->
    Id = <<"srv-", (integer_to_binary(C + 1))/binary>>,
    _ = tether:request(<<"$web">>, srv_seq(), [{put, {?COLL, Id}, <<"server">>}]),
    S#w{counter = C + 1};

%% 別の誰かが中央在庫を空にする
do(drain, _N, _Pid, S) -> drain(), S;
do(_, _N, _Pid, S) -> S.

srv_seq() ->
    #{last_seq := L} = tether:resume(<<"$web">>), L + 1.

drain() ->
    #{last_seq := L} = tether:resume(<<"$drain">>),
    case tether:request(<<"$drain">>, L + 1, [{acquire, ?SKU, 1000000, 600000}]) of
        {ok, [{granted, G, _}]} ->
            _ = tether:request(<<"$drain">>, L + 2, [{consume, ?SKU, G}]),
            drain();
        _ -> ok
    end.

%%%===================================================================

client_state(Name, Pid) ->
    St = tether_client:state(Pid),
    #{name    => Name,
      online  => maps:get(online, St),
      version => maps:get(version, St),
      queued  => maps:get(queued, St),
      grant   => case maps:get(grants, St) of
                     [{_, N} | _] -> N;
                     []           -> 0
                 end,
      orders  => rows(tether_client:dump(Pid, ?COLL))}.

rows(Rows) -> [#{key => K, value => V} || {{_, K}, V} <- Rows].

%%%===================================================================
%%% JSON(手書き。依存を増やさないため)
%%%===================================================================

enc(M) when is_map(M) ->
    <<"{", (join([<<(estr(K))/binary, ":", (enc(V))/binary>>
                  || {K, V} <- maps:to_list(M)]))/binary, "}">>;
enc(L) when is_list(L) ->
    <<"[", (join([enc(X) || X <- L]))/binary, "]">>;
enc(B) when is_binary(B)  -> estr(B);
enc(I) when is_integer(I) -> integer_to_binary(I);
enc(true)  -> <<"true">>;
enc(false) -> <<"false">>;
enc(A) when is_atom(A) -> estr(atom_to_binary(A, utf8)).

estr(A) when is_atom(A) -> estr(atom_to_binary(A, utf8));
estr(B) -> <<"\"", (esc(B))/binary, "\"">>.

esc(B) -> << <<(ec(C))/binary>> || <<C>> <= B >>.
ec($")  -> <<"\\\"">>;
ec($\\) -> <<"\\\\">>;
ec($\n) -> <<"\\n">>;
ec(C) when C < 16#20 -> <<"\\u00", (integer_to_binary(C, 16))/binary>>;
ec(C) -> <<C>>.

join([])    -> <<>>;
join([H])   -> H;
join([H|T]) -> <<H/binary, ",", (join(T))/binary>>.
