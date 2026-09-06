%%%-------------------------------------------------------------------
%%% @doc
%%% 公開API。
%%%
%%% 中心にあるのは request/3 の通番である。**同じ通番で2度頼んでも、
%%% 2度実行されることはなく、返る答えは初回とまったく同じ**。
%%%
%%%     {ok, [ok]} = tether:request(<<"alice">>, 1, [{put, K, V}]),
%%%     {ok, [ok]} = tether:request(<<"alice">>, 1, [{put, K, V}]).   % 再送
%%%
%%% 応答が返る前に接続が切れても、プロセスが死んでも、ノードが
%%% 落ちても、同じ通番で聞き直せば答えが分かる。
%%% 「入ったのか入っていないのか分からない」が無い。
%%% @end
%%%-------------------------------------------------------------------
-module(tether).

-export([request/3, read/1, open/1, close/1, session_info/1]).
-export([stock/2, pool/1]).
-export([session_count/0, stat/0]).

-spec open(binary()) -> {ok, pid()} | {error, term()}.
open(Client) -> tether_sessions:ensure(Client).

%%----------------------------------------------------------------------
%% @doc 操作を原子的に実行する。返った時点で永続化されている。
%%
%% Seq は、そのクライアントの中で 1 から順に増やす。
%% 前回と同じ Seq を渡すのが「再送」であり、実行されずに
%% 前回の答えが返る。
%%
%% 大きすぎる要求はここで拒む。上限は tether_data:limits/0。
%%----------------------------------------------------------------------
-spec request(binary(), non_neg_integer(), [tether_data:op()]) ->
          tether_session:result() | {error, term()}.
request(Client, Seq, Ops) ->
    %% 大きさの検査は**セッションへ渡す前**に行う。
    %% セッションのメールボックスに入った時点で確保は済んでいるので、
    %% そこで弾いても遅い。本番では通信路の復号器が同じ検査をする。
    case tether_data:validate(Ops) of
        {error, R} -> {error, R};
        ok ->
            case tether_sessions:ensure(Client) of
                {ok, Pid} -> tether_session:request(Pid, Seq, Ops);
                E         -> E
            end
    end.

-spec read(tether_data:key()) -> {ok, tether_data:value()} | not_found.
read(Key) -> tether_store:read(Key).

%%----------------------------------------------------------------------
%% @doc 中央在庫を増減する(管理操作)。
%% クライアントの通番とは無関係なので、専用のクライアントIDで通す。
%%----------------------------------------------------------------------
-spec stock(binary(), integer()) -> {ok, non_neg_integer()} | {error, term()}.
stock(Resource, N) ->
    case tether_store:submit(<<"$admin">>, 0, [{stock, Resource, N}]) of
        {ok, [{pool, A, _}]} -> {ok, A};
        Other                -> {error, Other}
    end.

%% @doc 中央の残りと、配ってある量。読み取り。
-spec pool(binary()) -> {non_neg_integer(), non_neg_integer()}.
pool(Resource) -> tether_store:escrow_pool(Resource).

-spec close(binary()) -> ok.
close(Client) -> tether_sessions:close(Client).

-spec session_info(binary()) -> map() | none.
session_info(Client) ->
    case tether_sessions:lookup(Client) of
        {ok, Pid} -> tether_session:info(Pid);
        none      -> none
    end.

-spec session_count() -> non_neg_integer().
session_count() -> tether_sessions:count().

-spec stat() -> map().
stat() ->
    maps:merge(tether_log:stat(),
               #{keys => tether_store:size(), sessions => session_count()}).
