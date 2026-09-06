%%%-------------------------------------------------------------------
%%% @doc
%%% SQLの値の意味論。比較・順序・3値論理をここに集約する。
%%%
%%% **Erlangの項順序をそのまま使ってはいけない。**
%%% Erlangの全順序は number < atom < ... なので、NULLをアトム null で
%%% 表すと `100 < null` が true になる。素の `<` で並べ替えると、
%%% NULLを含む列の ORDER BY と、B+treeの範囲検索が壊れる。
%%% 比較・整列・グルーピングは必ずこのモジュールを経由すること。
%%%
%%% 3値論理(Kleene):
%%%   比較の一方がNULLなら結果はNULL
%%%   FALSE AND NULL = FALSE / TRUE OR NULL = TRUE
%%%   WHERE が通すのは true の行だけ。null と false は等しく捨てる。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_value).

-export([compare/2, order_compare/4]).
-export([truth_and/2, truth_or/2, truth_not/1, keep/1]).
-export([is_null/1, group_key/1, arith/3, like/2]).

-type value() :: term().
-type truth() :: true | false | null.
-type ordering() :: lt | eq | gt | null.
-export_type([value/0, truth/0, ordering/0]).

%%%===================================================================
%%% 比較
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc SQLの比較。どちらかがNULLなら結果はNULL(unknown)。
%% 型が違うものの比較もNULLにする(暗黙変換はしない)。
%%----------------------------------------------------------------------
-spec compare(value(), value()) -> ordering().
compare(null, _) -> null;
compare(_, null) -> null;
compare(A, B) when is_number(A), is_number(B) -> cmp(A, B);
compare(A, B) when is_binary(A), is_binary(B) -> cmp(A, B);
compare(A, B) when is_boolean(A), is_boolean(B) -> cmp(bool_rank(A), bool_rank(B));
compare(A, B) when is_list(A), is_list(B) -> cmp(A, B);
%% 型宣言がまだ無い段階のデータはアトムを値に持つ(例: apple)。
%% boolean は上で処理済みなのでここには来ない。
compare(A, B) when is_atom(A), is_atom(B) -> cmp(A, B);
compare(_, _) -> null.

cmp(A, B) when A < B -> lt;
cmp(A, B) when A > B -> gt;
cmp(_, _) -> eq.

bool_rank(false) -> 0;
bool_rank(true) -> 1.

%%----------------------------------------------------------------------
%% @doc ORDER BY 用の比較。
%% NULLの位置を明示的に決める。既定はPostgreSQLに合わせ、
%% ASCならNULLS LAST、DESCならNULLS FIRST(NULLを最大値として扱う)。
%% Returns: lt | eq | gt
%%----------------------------------------------------------------------
-spec order_compare(value(), value(), asc | desc, nulls_first | nulls_last) ->
          lt | eq | gt.
order_compare(null, null, _Dir, _Nulls) ->
    eq;
order_compare(null, _B, _Dir, nulls_first) -> lt;
order_compare(null, _B, _Dir, nulls_last) -> gt;
order_compare(_A, null, _Dir, nulls_first) -> gt;
order_compare(_A, null, _Dir, nulls_last) -> lt;
order_compare(A, B, Dir, _Nulls) ->
    case {compare(A, B), Dir} of
        {eq, _} -> eq;
        %% 型違いでnullが返った場合も順序は決めないといけないので
        %% 安定させるためにeq扱いにする
        {null, _} -> eq;
        {lt, asc} -> lt;
        {gt, asc} -> gt;
        {lt, desc} -> gt;
        {gt, desc} -> lt
    end.

%%%===================================================================
%%% 3値論理
%%%===================================================================

-spec truth_and(truth(), truth()) -> truth().
truth_and(false, _) -> false;      % FALSE AND NULL = FALSE
truth_and(_, false) -> false;
truth_and(true, true) -> true;
truth_and(_, _) -> null.

-spec truth_or(truth(), truth()) -> truth().
truth_or(true, _) -> true;         % TRUE OR NULL = TRUE
truth_or(_, true) -> true;
truth_or(false, false) -> false;
truth_or(_, _) -> null.

-spec truth_not(truth()) -> truth().
truth_not(true) -> false;
truth_not(false) -> true;
truth_not(null) -> null.

%%----------------------------------------------------------------------
%% @doc 選択(σ)が行を通すかどうか。
%%
%% この1行が3値論理でいちばん効くところ。true の行だけを通し、
%% null と false は等しく捨てる。「WHERE NOT (x = 1) が
%% x が NULL の行を返さない」という挙動はここから出てくる。
%%----------------------------------------------------------------------
-spec keep(truth()) -> boolean().
keep(true) -> true;
keep(_) -> false.

is_null(null) -> true;
is_null(_) -> false.

%%----------------------------------------------------------------------
%% @doc 算術。片方でもNULLなら結果はNULL。
%% 数値でないものの演算もNULLにする(暗黙変換しない)。
%% ゼロ除算はエラーではなくNULLを返す(SQLite寄り。SQL標準はエラー)。
%%----------------------------------------------------------------------
arith(_Op, null, _) -> null;
arith(_Op, _, null) -> null;
arith(Op, A, B) when is_number(A), is_number(B) -> arith_1(Op, A, B);
arith(_Op, _, _) -> null.

arith_1('+', A, B) -> A + B;
arith_1('-', A, B) -> A - B;
arith_1('*', A, B) -> A * B;
arith_1('/', _A, B) when B == 0 -> null;
arith_1('/', A, B) when is_integer(A), is_integer(B), A rem B =:= 0 -> A div B;
arith_1('/', A, B) -> A / B.

%%----------------------------------------------------------------------
%% @doc GROUP BY / DISTINCT 用のキー。
%%
%% グルーピングでは NULL 同士は同じ組にまとまる。`=` の意味論
%% (NULL = NULL は unknown)とは逆なので、比較とは別の関数が要る。
%%
%% 整数と浮動小数点数も揃えておく。ETSの set はキー比較に =:= を使うため、
%% 正規化しないと 100 と 100.0 が別のグループになる
%% (ordered_set は == なので挙動が違う、という罠もある)。
%%----------------------------------------------------------------------
group_key(null) -> '$null';
group_key(V) when is_float(V) ->
    T = trunc(V),
    case T == V of
        true -> T;
        false -> V
    end;
group_key(V) -> V.

%%%===================================================================
%%% LIKE
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc SQL の LIKE。`%` は任意の並び(空も可)、`_` は任意の1文字。
%% どちらかが NULL なら結果は NULL(3値論理)。
%%
%% 文字単位で照合する。バイト単位だと多バイト文字で `_` が
%% 1文字ぶんにならない。
%%----------------------------------------------------------------------
-spec like(value(), value()) -> truth().
like(null, _) -> null;
like(_, null) -> null;
like(S, P) when is_binary(S), is_binary(P) ->
    match(unicode:characters_to_list(P), unicode:characters_to_list(S));
like(_, _) ->
    null.

match([], [])                -> true;
match([$% | P], S)           -> match(P, S) orelse
                                (S =/= [] andalso match([$% | P], tl(S)));
match([$_ | P], [_ | S])     -> match(P, S);
match([C | P], [C | S])      -> match(P, S);
match(_, _)                  -> false.
