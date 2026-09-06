%%%-------------------------------------------------------------------
%%% @doc
%%% クライアント側の口。手元の複製を持ち、圏外でも読み書きできる。
%%%
%%% == どれがネットワークを要るか ==
%%%
%%%   read/2       要らない。**手元の複製から答える**
%%%   write/2      要らない。楽観的に手元へ適用し、送信待ちに積む
%%%   consume      要らない。**預かりの範囲なら確定的に答えられる**
%%%   acquire/4    **要る。** 権利を取るのは協調そのもの
%%%   sync/1       要る。溜めた分を送り、見落とした変更を受け取る
%%%
%%% offline/1 で本当に通信を止められる。試験とデモで「圏外」を
%%% 再現するためで、止めた状態で read と write が通ることを
%%% 確かめられなければ local-first を名乗る意味がない。
%%%
%%% == 再送 ==
%%%
%%% sync が途中で切れても、送信待ちは確定するまで消えない。
%%% 通番も進めない。だから**同じ束をそのまま送り直せる**。
%%% サーバ側が吸収するので、二重に実行されない。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_client).
-behaviour(gen_server).

-export([open/2, close/1, read/2, write/2, acquire/4, sync/1, state/1]).
-export([offline/1, online/1, is_online/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export([now_ms/0]).

-record(c, {
          replica       :: tether_replica:replica(),
          colls  = []   :: [binary()],
          online = true :: boolean()
         }).

%%%===================================================================
%%% 公開
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc 端末を開く。購読して複製を受け取り、自分の位置と権利を取り戻す。
%%----------------------------------------------------------------------
-spec open(binary(), binary()) -> {ok, pid()} | {error, term()}.
open(Client, Collection) ->
    gen_server:start_link(?MODULE, {Client, Collection}, []).

-spec close(pid()) -> ok.
close(Pid) -> gen_server:stop(Pid).

%% @doc 手元から読む。**ネットワークを使わない。**
-spec read(pid(), tether_data:key()) -> {ok, tether_data:value()} | not_found.
read(Pid, Key) -> gen_server:call(Pid, {read, Key}).

%% @doc 手元へ書く。**ネットワークを使わない。** 送信待ちに積まれる。
-spec write(pid(), [tether_data:op()]) -> tether_data:group_result().
write(Pid, Ops) -> gen_server:call(Pid, {write, Ops}).

%% @doc 預かりを取る。**ネットワークが要る。**
-spec acquire(pid(), binary(), pos_integer(), pos_integer()) ->
          {ok, non_neg_integer()} | {error, term()}.
acquire(Pid, Resource, Want, Ttl) ->
    gen_server:call(Pid, {acquire, Resource, Want, Ttl}, 30000).

%% @doc 溜めた分を送り、見落とした変更を受け取る。
-spec sync(pid()) -> {ok, tether_replica:outcome()} | {error, term()}.
sync(Pid) -> gen_server:call(Pid, sync, 60000).

-spec state(pid()) -> map().
state(Pid) -> gen_server:call(Pid, state).

-spec offline(pid()) -> ok.
offline(Pid) -> gen_server:call(Pid, {net, false}).

-spec online(pid()) -> ok.
online(Pid) -> gen_server:call(Pid, {net, true}).

-spec is_online(pid()) -> boolean().
is_online(Pid) -> gen_server:call(Pid, is_online).

now_ms() -> erlang:system_time(millisecond).

%%%===================================================================
%%% gen_server
%%%===================================================================

init({Client, Coll}) ->
    {ok, V, Rows} = tether:subscribe(Client, Coll),
    #{last_seq := Seq, grants := Grants} = tether:resume(Client),
    R0 = tether_replica:reset(V, Rows, now_ms(), tether_replica:new(Client)),
    R1 = set_seq(Seq, R0),
    R2 = lists:foldl(fun({Res, N, Exp}, R) ->
                             tether_replica:set_grant(Res, N, Exp, R)
                     end, R1, Grants),
    {ok, #c{replica = R2, colls = [Coll]}}.

handle_call({read, Key}, _From, #c{replica = R} = S) ->
    {reply, tether_replica:get(Key, now_ms(), R), S};

handle_call({write, Ops}, _From, #c{replica = R} = S) ->
    {Res, R1} = tether_replica:apply_local(Ops, now_ms(), R),
    {reply, Res, S#c{replica = R1}};

handle_call({acquire, _R, _W, _T}, _From, #c{online = false} = S) ->
    %% 権利を取るのは協調そのもの。圏外ではできない。
    {reply, {error, offline}, S};
handle_call({acquire, Res, Want, Ttl}, _From, #c{replica = R} = S) ->
    %% 送信待ちがあるうちは取らせない。
    %%
    %% ここで黙って sync すると、**溜めた書き込みが拒否されても
    %% 呼び出し側に伝わらない**。順序のために内部で流したくなるが、
    %% 流した結果を握り潰すことになる。先に sync させて、
    %% 拒否があればそれを見てから来てもらう。
    case tether_replica:pending(R) of
        [_ | _] = P ->
            {reply, {error, {sync_first, length(P)}}, S};
        [] ->
            Seq = tether_replica:seq(R) + 1,
            Cl  = tether_replica:client(R),
            case tether:request(Cl, Seq, [{acquire, Res, Want, Ttl}]) of
                {ok, [{granted, N, Exp}]} ->
                    R2 = tether_replica:set_grant(Res, N, Exp, set_seq(Seq, R)),
                    {reply, {ok, N}, S#c{replica = R2}};
                {error, _, Why} ->
                    {reply, {error, Why}, S#c{replica = set_seq(Seq, R)}};
                Other ->
                    {reply, {error, Other}, S}
            end
    end;

handle_call(sync, _From, S) ->
    {Reply, S1} = do_sync(S),
    {reply, Reply, S1};

handle_call(state, _From, #c{replica = R, online = On} = S) ->
    {reply, #{client  => tether_replica:client(R),
              version => tether_replica:version(R),
              seq     => tether_replica:seq(R),
              queued  => tether_replica:queued_count(R),
              grants  => tether_replica:grants(now_ms(), R),
              online  => On}, S};

handle_call({net, On}, _From, S)  -> {reply, ok, S#c{online = On}};
handle_call(is_online, _From, S)  -> {reply, S#c.online, S};
handle_call(_R, _From, S)         -> {reply, {error, unknown_call}, S}.

handle_cast(_M, S) -> {noreply, S}.
handle_info(_M, S) -> {noreply, S}.

%%%===================================================================
%%% 内部
%%%===================================================================

do_sync(#c{online = false} = S) ->
    {{error, offline}, S};
do_sync(#c{replica = R} = S) ->
    Cl   = tether_replica:client(R),
    Sent = tether_replica:pending(R),
    Now  = now_ms(),
    %% 送るものがあれば送る。通番は確定するまで進めないので、
    %% 途中で切れたら同じ束をそのまま送り直せる。
    case send(Cl, tether_replica:seq(R) + 1, Sent) of
        {error, E} ->
            {{error, E}, S};
        {ok, Results} ->
            case tether:sync(Cl) of
                {delta, V, Changes} ->
                    {Out, R1} = confirm(Results, Sent, {V, Changes}, Now, R),
                    {{ok, Out}, S#c{replica = R1}};
                {resync, V, Snaps} ->
                    Rows = lists:append([Rs || {_, Rs} <- Snaps]),
                    {Out, R1} = confirm(Results, Sent, {V, []}, Now, R),
                    {{ok, Out#{resynced => true}},
                     S#c{replica = tether_replica:reset(V, Rows, Now, R1)}};
                {error, E} ->
                    {{error, E}, S}
            end
    end.

%% 送るものが無ければ通信しない
-spec send(binary(), non_neg_integer(), [[tether_data:op()]]) ->
          {ok, [tether_data:group_result()]} | {error, term()}.
send(_Cl, _Seq, []) -> {ok, []};
send(Cl, Seq, Sent) ->
    case tether:request_batch(Cl, Seq, Sent) of
        {ok, Rs} -> {ok, Rs};
        Err      -> {error, Err}
    end.

%% 送るものが無かった場合は通番を進めない
confirm([], [], {V, Changes}, Now, R) ->
    {Out, R1} = tether_replica:confirm([], {V, Changes}, Now, R),
    {Out, set_seq(tether_replica:seq(R), R1)};
confirm(Results, _Sent, VC, Now, R) ->
    tether_replica:confirm(Results, VC, Now, R).

set_seq(Seq, R) ->
    %% replica の通番を明示的に合わせる(resume 直後と、空 sync の後)
    tether_replica:confirm_seq(Seq, R).
