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
-export([state/3, act/3, lab/3, book/3]).       % mod_esi
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(COLL, <<"orders">>).
-define(SKU,  <<"sku:1">>).
-define(NAMES, [<<"alice">>, <<"bob">>]).

-define(NOTE, <<"note">>).

-record(w, {clients = #{} :: #{binary() => pid()},
            counter = 0   :: non_neg_integer(),
            %% 何が起きたかの記録。**拒否を見せるために要る。**
            log     = []  :: [map()],
            hist    = []  :: [non_neg_integer()]}).   % 配られた量の推移

%%%===================================================================

start() -> start(8080).

start(Port) ->
    ok = logger:set_primary_config(level, error),
    Dir = filename:absname(filename:join("data", "web")),
    _ = application:load(tether),
    _ = [file:delete(F) || F <- filelib:wildcard(filename:join(Dir, "*"))],
    application:set_env(tether, dir, Dir),
    %% 溢れを押して起こせるように、溜められる量を小さくしておく。
    %% 既定は 10000 で、デモで溢れさせるには多すぎる。
    application:set_env(tether, sync_backlog, 30),
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
                      "  http://127.0.0.1:~p          端末2台とサーバ~n"
                      "  http://127.0.0.1:~p/lab.html  分散DBの系譜（復習用）~n~n",
                      [Port, Port]),
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

%% 分散の現象を方式ごとに並べたもの。状態を持たないので gen_server を通さない。
lab(Sid, _Env, _In) -> json(Sid, tether_lab:all()).

%% 章立ての教材。tether の実装とは独立した一般論。
book(Sid, _Env, _In) -> json(Sid, tether_book:chapters()).

act(Sid, _Env, In) when is_list(In) ->
    %% In は "client=alice&action=sell" 形式
    P = uri_string:dissect_query(In),
    C = list_to_binary(proplists:get_value("client", P, "alice")),
    A = list_to_atom(proplists:get_value("action", P, "noop")),
    _ = case A of
            reset -> gen_server:call(?MODULE, reset, 60000);
            _     -> gen_server:call(?MODULE, {act, C, A}, 60000)
        end,
    json(Sid, gen_server:call(?MODULE, state, 30000)).

json(Sid, Term) ->
    ok = mod_esi:deliver(Sid, "Content-Type: application/json; charset=utf-8\r\n\r\n"),
    ok = mod_esi:deliver(Sid, binary_to_list(enc(Term))).

%%%===================================================================
%%% gen_server
%%%===================================================================

init([]) ->
    {ok, _} = tether:stock(?SKU, 20),
    {ok, [ok]} = tether:request(<<"$web">>, srv_seq(),
                                [{put, {?COLL, ?NOTE}, <<"[]">>}]),
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
       hist    => lists:reverse(S#w.hist),
       log     => S#w.log,
       clients => [client_state(N, maps:get(N, Cs)) || N <- ?NAMES]}, S};

handle_call({act, Name, Action}, _From, #w{clients = Cs} = S) ->
    Pid = maps:get(Name, Cs, undefined),
    {reply, ok, do(Action, Name, Pid, S)};
handle_call(reset, _From, S) ->
    _ = [catch tether_client:close(P) || P <- maps:values(S#w.clients)],
    ok = application:stop(tether),
    %% **データも消す。** 消さずに起動し直すと、前の通番が復旧されて
    %% 初期化の書き込みが seq_too_old で弾かれる(実際に踏んだ)。
    {ok, Dir} = application:get_env(tether, dir),
    _ = [file:delete(F) || F <- filelib:wildcard(filename:join(Dir, "*"))],
    {ok, _} = application:ensure_all_started(tether),
    {ok, S1} = init([]),
    {reply, ok, S1};

handle_call(_R, _From, S) -> {reply, {error, unknown}, S}.

handle_cast(_M, S) -> {noreply, S}.
handle_info(_M, S) -> {noreply, S}.
terminate(_R, #w{clients = Cs}) ->
    _ = [catch tether_client:close(P) || P <- maps:values(Cs)], ok.

%%%===================================================================
%%% 操作
%%%===================================================================

do(offline, N, Pid, S) -> _ = tether_client:offline(Pid), note(S, N, "圏外にした", ok);
do(online,  N, Pid, S) -> _ = tether_client:online(Pid),  note(S, N, "圏内に戻した", ok);

%% 同期。**拒否があればそれを記録する。ここが見せ場。**
do(sync, N, Pid, S) ->
    case tether_client:sync(Pid) of
        {ok, #{rejected := {At, {_, {conflict, V}}}} = O} ->
            note(S, N, io_lib:format("同期 — ~p件目で競合。手元の変更が覆った(サーバの値: ~ts)",
                                     [At, V]), bad,
                 io_lib:format("実行された ~p 件 / 送られなかった ~p 件",
                               [maps:get(accepted, O), length(maps:get(unsent, O))]));
        {ok, #{rejected := {At, {_, Why}}} = O} ->
            note(S, N, io_lib:format("同期 — ~p件目で失敗(~p)", [At, Why]), bad,
                 io_lib:format("送られなかった ~p 件", [length(maps:get(unsent, O))]));
        {ok, #{resynced := true}} ->
            note(S, N, "同期 — 差分では追いつけず、全件を取り直した", warn);
        {ok, #{accepted := A}} ->
            note(S, N, io_lib:format("同期 — ~p 件が通った", [A]), ok);
        {error, E} ->
            note(S, N, io_lib:format("同期できず(~p)", [E]), bad)
    end;

%% 手元にメモを書く(楽観的)。サーバが拒む可能性がある
do(write, Name, Pid, #w{counter = C} = S) ->
    Id = <<Name/binary, "-", (integer_to_binary(C + 1))/binary>>,
    _ = tether_client:write(Pid, [{put, {?COLL, Id}, <<"draft">>}]),
    (note(S, Name, "手元にメモを書いた(楽観的)", ok))#w{counter = C + 1};

%% 共有の1行を書き換える。**見た値を前提にするので、競合しうる。**
do(edit, Name, Pid, S) ->
    Cur = case tether_client:read(Pid, {?COLL, ?NOTE}) of
              {ok, V}   -> V;
              not_found -> undefined
          end,
    New = append(Cur, Name),
    _ = tether_client:write(Pid, [{cas, {?COLL, ?NOTE}, Cur, New}]),
    note(S, Name, io_lib:format("共有メモを書き換えた(手元では ~ts)", [New]), ok);

%% 第三者が同じ共有メモを書き換える。**圏外の端末の前提が古くなる。**
do(conflict, _N, _Pid, S) ->
    Cur = case tether_store:read({?COLL, ?NOTE}) of
              {ok, V}   -> V;
              not_found -> undefined
          end,
    New = append(Cur, <<"rival">>),
    _ = tether:request(<<"$rival">>, rival_seq(),
                       [{cas, {?COLL, ?NOTE}, Cur, New}]),
    note(S, <<"rival">>,
         io_lib:format("第三者が共有メモを書き換えた → ~ts", [New]), warn);

%% 溜めきれない量の変更を起こす。圏外の端末は取り直しに落ちる。
do(flood, _N, _Pid, #w{counter = C} = S) ->
    _ = [tether:request(<<"$web">>, srv_seq(),
                        [{put, {?COLL, <<"f", (integer_to_binary(I))/binary>>},
                          <<"flood">>}]) || I <- lists:seq(C + 1, C + 40)],
    (note(S, <<"server">>, "40件を一度に書いた", warn))#w{counter = C + 40};

%% 預かりを取る(ネットワークが要る)
do(acquire, N, Pid, #w{hist = H} = S) ->
    case tether_client:acquire(Pid, ?SKU, 8, 600000) of
        {ok, G} -> note(S#w{hist = [G | H]}, N,
                        io_lib:format("預かりを取った → ~p 個", [G]), ok);
        {error, E} -> note(S, N, io_lib:format("預かりを取れず(~p)", [E]), bad)
    end;

%% 期限の短い預かり。圏外が長引いたときに何が起きるかを見る。
do(short, N, Pid, #w{hist = H} = S) ->
    case tether_client:acquire(Pid, ?SKU, 8, 8000) of
        {ok, G} -> note(S#w{hist = [G | H]}, N,
                        io_lib:format("8秒だけの預かりを取った → ~p 個", [G]), warn);
        {error, E} -> note(S, N, io_lib:format("預かりを取れず(~p)", [E]), bad)
    end;

%% 1個売る。預かりの範囲なら**圏外でも確定的**
do(sell, Name, Pid, #w{counter = C} = S) ->
    Id = <<Name/binary, "-o", (integer_to_binary(C + 1))/binary>>,
    R = tether_client:write(Pid, [{consume, ?SKU, 1},
                                  {put, {?COLL, Id}, <<"sold">>}]),
    S1 = case R of
             {ok, _} -> note(S, Name, "1個売った(圏外でも確定的)", ok);
             {error, _, Why} ->
                 note(S, Name,
                      io_lib:format("売れなかった(~p) — サーバに聞かずに断った", [Why]),
                      bad)
         end,
    S1#w{counter = C + 1};

%% 第三者がサーバ側を直接書き換える(圏外の端末が見落とす変更)
do(server_write, _N, _Pid, #w{counter = C} = S) ->
    Id = <<"srv-", (integer_to_binary(C + 1))/binary>>,
    _ = tether:request(<<"$web">>, srv_seq(), [{put, {?COLL, Id}, <<"server">>}]),
    S#w{counter = C + 1};

%% 別の誰かが中央在庫を空にする
do(drain, _N, _Pid, S) ->
    drain(), note(S, <<"server">>, "第三者が中央在庫を空にした", warn);
do(_, _N, _Pid, S) -> S.

append(undefined, N) -> <<"[", N/binary, "]">>;
append(Cur, N) ->
    Trim = binary:part(Cur, 0, byte_size(Cur) - 1),
    <<Trim/binary, " ", N/binary, "]">>.

rival_seq() ->
    #{last_seq := L} = tether:resume(<<"$rival">>), L + 1.

%% 記録。新しいものが上。
note(S, Who, What, Kind) -> note(S, Who, What, Kind, "").
note(#w{log = L} = S, Who, What, Kind, Detail) ->
    %% io_lib:format の結果は、日本語を含むと符号位置が 255 を超えるので
    %% iolist_to_binary/1 に渡せない。unicode:characters_to_binary/1 を使う。
    E = #{who => u(Who), what => u(What), kind => Kind, detail => u(Detail)},
    S#w{log = lists:sublist([E | L], 14)}.

u(X) ->
    case unicode:characters_to_binary(X) of
        B when is_binary(B) -> B;
        _                   -> <<"?">>
    end.

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
      expires => maps:get(expires, St) div 1000,
      last    => summarize(maps:get(last, St)),
      orders  => rows(tether_client:dump(Pid, ?COLL))}.

rows(Rows) -> [#{key => K, value => V} || {{_, K}, V} <- Rows].

%% 直近の同期の結果を、画面に出せる形へ。
%% unsent は操作の束(タプル)なので、そのままでは JSON にできない。件数だけ出す。
summarize(none) -> #{state => none};
summarize(#{error := _}) -> #{state => error};
summarize(#{accepted := A} = O) ->
    #{state    => case maps:get(rejected, O, none) of
                      none -> case maps:get(resynced, O, false) of
                                  true -> resynced;
                                  _    -> ok
                              end;
                      _    -> rejected
                  end,
      accepted => A,
      at       => case maps:get(rejected, O, none) of
                      none      -> 0;
                      {N, _}    -> N
                  end,
      unsent   => length(maps:get(unsent, O, []))};
summarize(_) -> #{state => none}.

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
