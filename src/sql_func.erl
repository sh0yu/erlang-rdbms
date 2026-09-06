%%%-------------------------------------------------------------------
%%% @doc
%%% スカラー関数。**このモジュールは純粋。**
%%%
%%% 集約(COUNT/SUM/...)とは別物である。集約は複数行を1つにまとめ、
%%% スカラー関数は1行の中で値から値を作る。名前で見分けるので、
%%% 登録簿はここ1箇所に置く。
%%%
%%% == NULL の扱い ==
%%%
%%% 既定は**引数のどれかが NULL なら結果も NULL**。SQLの標準的な
%%% 振る舞いで、「分からない値を加工しても分からない」ということ。
%%%
%%% 例外は COALESCE と NULLIF で、この2つは NULL を見て分岐するのが
%%% 仕事なので、NULL を受け取っても働く。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_func).

-export([is_scalar/1, is_scalar/2, arity_ok/2, apply/2, names/0]).

%%----------------------------------------------------------------------
%% 関数名 => 受け付ける引数の数。any は可変長。
%%----------------------------------------------------------------------
-define(FUNCS, #{
    %% 数値
    "abs"       => [1],
    "ceil"      => [1],
    "ceiling"   => [1],
    "floor"     => [1],
    "round"     => [1, 2],
    "mod"       => [2],
    "power"     => [2],
    "greatest"  => any,
    "least"     => any,
    %% 文字列
    "upper"     => [1],
    "lower"     => [1],
    "length"    => [1],
    "substr"    => [2, 3],
    "substring" => [2, 3],
    "trim"      => [1],
    "ltrim"     => [1],
    "rtrim"     => [1],
    "concat"    => any,
    "replace"   => [3],
    %% NULL
    "coalesce"  => any,
    "nullif"    => [2]
}).

%% @doc 名前だけで判定する(引数の数は arity_ok/2 で見る)。
-spec is_scalar(string()) -> boolean().
is_scalar(Name) -> maps:is_key(Name, ?FUNCS).

-spec is_scalar(string(), non_neg_integer()) -> boolean().
is_scalar(Name, Arity) -> is_scalar(Name) andalso arity_ok(Name, Arity).

-spec arity_ok(string(), non_neg_integer()) -> boolean().
arity_ok(Name, Arity) ->
    case maps:get(Name, ?FUNCS, undefined) of
        undefined -> false;
        any       -> Arity >= 1;
        List      -> lists:member(Arity, List)
    end.

-spec names() -> [string()].
names() -> lists:sort(maps:keys(?FUNCS)).

%%----------------------------------------------------------------------
%% @doc 値に適用する。
%%----------------------------------------------------------------------
-spec apply(string(), [term()]) -> term().

%% NULL を見て分岐するのが仕事の2つ。NULL 伝播より先に置く。
apply("coalesce", Args) ->
    case [V || V <- Args, V =/= null] of
        []      -> null;
        [V | _] -> V
    end;
apply("nullif", [A, B]) ->
    case sql_value:compare(A, B) of
        eq -> null;
        _  -> A
    end;

%% ここから下は、引数に NULL があれば NULL を返す。
apply(Name, Args) ->
    case lists:member(null, Args) of
        true  -> null;
        false -> apply_1(Name, Args)
    end.

%%%===================================================================
%%% 数値
%%%===================================================================

apply_1("abs", [V]) when is_number(V)          -> abs(V);
apply_1("ceil", [V])                           -> ceiling(V);
apply_1("ceiling", [V])                        -> ceiling(V);
apply_1("floor", [V]) when is_number(V)        -> floor(V);
apply_1("round", [V]) when is_number(V)        -> round(V);
apply_1("round", [V, D]) when is_number(V), is_integer(D), D >= 0 ->
    P = math:pow(10, D),
    round(V * P) / P;
apply_1("mod", [A, B]) when is_integer(A), is_integer(B), B =/= 0 -> A rem B;
apply_1("power", [A, B]) when is_number(A), is_number(B) -> math:pow(A, B);

%% GREATEST / LEAST は SQL の順序で比べる。
%% Erlang の項順序だと 100 < <<"a">> のような比較が通ってしまう。
apply_1("greatest", [H | T]) -> lists:foldl(fun(V, Acc) -> pick(gt, V, Acc) end, H, T);
apply_1("least", [H | T])    -> lists:foldl(fun(V, Acc) -> pick(lt, V, Acc) end, H, T);

%%%===================================================================
%%% 文字列
%%%===================================================================

apply_1("upper", [V]) when is_binary(V) ->
    unicode:characters_to_binary(string:uppercase(V));
apply_1("lower", [V]) when is_binary(V) ->
    unicode:characters_to_binary(string:lowercase(V));
apply_1("length", [V]) when is_binary(V) ->
    string:length(V);
apply_1("trim", [V]) when is_binary(V) ->
    unicode:characters_to_binary(string:trim(V));
apply_1("ltrim", [V]) when is_binary(V) ->
    unicode:characters_to_binary(string:trim(V, leading));
apply_1("rtrim", [V]) when is_binary(V) ->
    unicode:characters_to_binary(string:trim(V, trailing));
%% SQL の SUBSTR は1始まり。
apply_1("substr", [V, From]) when is_binary(V), is_integer(From) ->
    substr(V, From, infinity);
apply_1("substr", [V, From, Len]) when is_binary(V), is_integer(From), is_integer(Len) ->
    substr(V, From, Len);
apply_1("substring", Args) ->
    apply_1("substr", Args);
apply_1("replace", [V, From, To]) when is_binary(V), is_binary(From), is_binary(To) ->
    binary:replace(V, From, To, [global]);
apply_1("concat", Args) ->
    case lists:all(fun is_binary/1, Args) of
        true  -> iolist_to_binary(Args);
        false -> null
    end;

%% 型が合わないものは NULL。暗黙変換はしない。
%% 落とすよりNULLにするのは、SQLの他の演算(算術・比較)と揃えるため。
apply_1(_Name, _Args) ->
    null.

%%%===================================================================

ceiling(V) when is_number(V) ->
    T = trunc(V),
    case V > T of
        true  -> T + 1;
        false -> T
    end;
ceiling(_) ->
    null.

pick(Want, V, Acc) ->
    case sql_value:compare(V, Acc) of
        Want -> V;
        _    -> Acc
    end.

%% 1始まり。範囲外は空文字列にする(落とさない)。
substr(Bin, From, Len) ->
    L = string:length(Bin),
    Start = max(1, From),
    case Start > L of
        true ->
            <<>>;
        false ->
            Take = case Len of
                       infinity -> L - Start + 1;
                       N        -> min(N, L - Start + 1)
                   end,
            case Take =< 0 of
                true  -> <<>>;
                false -> unicode:characters_to_binary(
                           string:slice(Bin, Start - 1, Take))
            end
    end.
