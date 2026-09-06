%%%-------------------------------------------------------------------
%%% @doc
%%% データ本体。**このモジュールは純粋である。**
%%%
%%% 鍵は {コレクション, 鍵} の組。値はバイト列。
%%%
%%% == 原子性について ==
%%%
%%% 1つの要求に含まれる操作は、全部適用されるか、まったく適用されないか
%%% のどちらかである。**そしてそのために特別な仕掛けは何も要らない。**
%%%
%%% 状態が不変な値なので、途中まで適用した結果は「新しい値」として
%%% 別に存在するだけで、元の値は損なわれない。失敗したらそれを
%%% 捨てればよい。UNDOログも、書き込み前の待避も、巻き戻しも要らない。
%%%
%%% 破壊的に更新する実装では、これは自明ではない。だからこそ
%%% ARIES のような仕掛けが必要になる。ここで要らないのは、
%%% 設計が優れているからではなく、言語がそうなっているからである。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_data).

-export([new/0, apply_ops/2, get/2, size/1, keys/1, fold/3]).

-export_type([db/0, key/0, value/0, op/0, result/0]).

-type key()    :: {binary(), binary()}.        % {コレクション, 鍵}
-type value()  :: binary().
-type db()     :: #{key() => value()}.

-type op() :: {get, key()}
            | {put, key(), value()}
            | {delete, key()}
              %% 期待した値と一致したときだけ書く。undefined は「無いこと」を期待する。
              %% read-modify-write を1往復で書くための最小の道具。
            | {cas, key(), value() | undefined, value()}.

-type result() :: ok
                | {ok, value()}
                | not_found
                | {conflict, value() | undefined}.   % cas の期待外れ。実際の値を返す

-spec new() -> db().
new() -> #{}.

-spec get(key(), db()) -> {ok, value()} | not_found.
get(K, Db) ->
    case maps:find(K, Db) of
        {ok, V} -> {ok, V};
        error   -> not_found
    end.

-spec size(db()) -> non_neg_integer().
size(Db) -> maps:size(Db).

-spec keys(db()) -> [key()].
keys(Db) -> lists:sort(maps:keys(Db)).

-spec fold(fun((key(), value(), Acc) -> Acc), Acc, db()) -> Acc.
fold(F, Acc0, Db) -> maps:fold(F, Acc0, Db).

%%----------------------------------------------------------------------
%% @doc 操作の列を原子的に適用する。
%%
%% 1つでも失敗すれば、**何も適用しない**。返るのは元の db() であって、
%% 途中まで適用したものではない。
%%----------------------------------------------------------------------
-spec apply_ops([op()], db()) ->
          {ok, [result()], db()} | {error, pos_integer(), result(), db()}.
apply_ops(Ops, Db) -> apply_ops(Ops, Db, 1, [], Db).

apply_ops([], New, _N, Acc, _Orig) ->
    {ok, lists:reverse(Acc), New};
apply_ops([Op | T], Cur, N, Acc, Orig) ->
    case apply_one(Op, Cur) of
        {ok, R, Next} ->
            apply_ops(T, Next, N + 1, [R | Acc], Orig);
        {error, R} ->
            %% ここが原子性の全て。作りかけの Cur を捨てて Orig を返す。
            {error, N, R, Orig}
    end.

apply_one({get, K}, Db) ->
    {ok, get(K, Db), Db};
apply_one({put, K, V}, Db) when is_binary(V) ->
    {ok, ok, Db#{K => V}};
apply_one({delete, K}, Db) ->
    %% 無い鍵の削除は成功。冪等にしておく。
    {ok, ok, maps:remove(K, Db)};
apply_one({cas, K, Expect, New}, Db) when is_binary(New) ->
    case {maps:find(K, Db), Expect} of
        {{ok, Expect}, _}       -> {ok, ok, Db#{K => New}};
        {error, undefined}      -> {ok, ok, Db#{K => New}};
        {{ok, Actual}, _}       -> {error, {conflict, Actual}};
        {error, _}              -> {error, {conflict, undefined}}
    end.
