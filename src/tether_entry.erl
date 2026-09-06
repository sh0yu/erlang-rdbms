%%%-------------------------------------------------------------------
%%% @doc
%%% ログに載る1件の表現。**純粋。**
%%%
%%% 記録するのは「何をしたか」ではなく「何を頼まれたか」と
%%% 「何を返したか」である。
%%%
%%%   index         ログ全体を通した番号。復旧の読み飛ばしに使う
%%%   time          実行した時刻。**再実行ではこれを使う**
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

-export([new/6, encode/1, decode/1]).
-export([index/1, time/1, client/1, seq/1, ops/1, reply/1]).

-export_type([entry/0]).

-record(entry, {
          %% ログ全体を通した番号。1から増え、**切り詰めても振り直さない**。
          %% スナップショットは「どの番号まで反映しているか」を記録し、
          %% 復旧はそれ以下の番号を読み飛ばす。件数で数えると、
          %% 切り詰めの後に先頭がずれて破綻する(実際にそれで壊した)。
          index  :: pos_integer(),
          %% この要求を実行した時刻(ミリ秒)。**実行時に記録し、再実行では
          %% これを使う。** 復旧のときに時計を読み直すと、期限切れの
          %% 判定が本番と変わり、復旧後の状態が食い違う。
          %% 時刻は環境から取るものではなく、ログに載る入力である。
          time   :: integer(),
          client :: binary(),
          seq    :: non_neg_integer(),
          ops    :: [[tether_data:op()]],   % グループの列
          reply  :: term()
         }).

-type entry() :: #entry{}.

-spec new(pos_integer(), integer(), binary(), non_neg_integer(),
          [[tether_data:op()]], term()) -> entry().
new(Index, Time, Client, Seq, Ops, Reply) ->
    #entry{index = Index, time = Time, client = Client, seq = Seq,
           ops = Ops, reply = Reply}.

-spec index(entry()) -> pos_integer().
index(#entry{index = I}) -> I.

-spec time(entry()) -> integer().
time(#entry{time = T}) -> T.

-spec client(entry()) -> binary().
client(#entry{client = C}) -> C.

-spec seq(entry()) -> non_neg_integer().
seq(#entry{seq = S}) -> S.

-spec ops(entry()) -> [[tether_data:op()]].
ops(#entry{ops = O}) -> O.

-spec reply(entry()) -> term().
reply(#entry{reply = R}) -> R.

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
