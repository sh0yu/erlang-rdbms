%%%-------------------------------------------------------------------
%%% @doc
%%% ログに載る1件の表現。**純粋。**
%%%
%%% 記録するのは「何をしたか」ではなく「何を頼まれたか」と
%%% 「何を返したか」である。
%%%
%%%   client, seq   誰の何番目の要求か。再送の判定に使う
%%%   ops           頼まれた操作。決定的なので再実行すれば同じ状態になる
%%%   reply         そのとき返した答え
%%%
%%% reply を記録するのがこの設計の要点である。復旧のためだけなら
%%% ops で足りるが、それでは**再送に対して初回と同じ答えを返せない**。
%%% 「入ったかどうか分からない」を消すには、答えを覚えている必要がある。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_entry).

-export([new/4, encode/1, decode/1]).
-export([client/1, seq/1, ops/1, reply/1]).

-export_type([entry/0]).

-record(entry, {
          client :: binary(),
          seq    :: non_neg_integer(),
          ops    :: [tether_data:op()],
          reply  :: term()
         }).

-opaque entry() :: #entry{}.

-spec new(binary(), non_neg_integer(), [tether_data:op()], term()) -> entry().
new(Client, Seq, Ops, Reply) ->
    #entry{client = Client, seq = Seq, ops = Ops, reply = Reply}.

client(#entry{client = C}) -> C.
seq(#entry{seq = S})       -> S.
ops(#entry{ops = O})       -> O.
reply(#entry{reply = R})   -> R.

-spec encode(entry()) -> binary().
encode(E) -> term_to_binary(E).

-spec decode(binary()) -> {ok, entry()} | {error, term()}.
decode(Bin) ->
    try binary_to_term(Bin, [safe]) of
        #entry{} = E -> {ok, E};
        Other        -> {error, {not_an_entry, Other}}
    catch
        _:R -> {error, {undecodable, R}}
    end.
