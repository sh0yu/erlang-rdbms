%%%-------------------------------------------------------------------
%%% @doc
%%% 束縛済みの式の評価器。
%%%
%%% sql_analyzer がカラム参照を行タプル内の位置に解決済みなので、
%%% ここでは名前を引かない。評価は element/2 で O(1)。
%%%
%%% 束縛済みの式:
%%%   {const, Value}
%%%   {ref, Position}
%%%   {comp, Op, Left, Right}     Op :: '=' | '<>' | '<' | '<=' | '>' | '>='
%%%   {'and', [Expr]} | {'or', [Expr]} | {'not', Expr}
%%%   {arith, Op, Left, Right}    Op :: '+' | '-' | '*' | '/'
%%%   {neg, Expr}
%%%   {is_null, Expr} | {is_not_null, Expr}
%%%
%%% 比較と論理は必ず sql_value を経由する。Erlangの `<` や `andalso` を
%%% 直接使うと、NULLの扱いと項順序で誤る。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_expr).

-export([eval/2, eval_pred/2, map_subqueries/2]).

%%----------------------------------------------------------------------
%% @doc 式を1行に対して評価する。Row は行タプル。
%%----------------------------------------------------------------------
eval({const, Value}, _Row) ->
    Value;
eval({ref, Pos}, Row) ->
    element(Pos, Row);
eval({comp, Op, L, R}, Row) ->
    comp_result(Op, sql_value:compare(eval(L, Row), eval(R, Row)));
eval({'and', Exprs}, Row) ->
    lists:foldl(fun(E, Acc) -> sql_value:truth_and(Acc, eval(E, Row)) end, true, Exprs);
eval({'or', Exprs}, Row) ->
    lists:foldl(fun(E, Acc) -> sql_value:truth_or(Acc, eval(E, Row)) end, false, Exprs);
eval({'not', E}, Row) ->
    sql_value:truth_not(eval(E, Row));
eval({neg, E}, Row) ->
    sql_value:arith('-', 0, eval(E, Row));
eval({arith, Op, L, R}, Row) ->
    sql_value:arith(Op, eval(L, Row), eval(R, Row));
%% x IN (...) の3値論理。
%%   x が NULL          → unknown
%%   一致がある         → true
%%   一致が無くNULL有り → unknown(そのNULLが x かもしれない)
%%   一致が無くNULL無し → false
%% CASE。条件が **true** の最初の枝を採る。null と false は等しく飛ばす。
eval({'case', Whens, Else}, Row) ->
    case first_true(Whens, Row) of
        {ok, V}   -> V;
        not_found -> case Else of
                         undefined -> null;
                         _         -> eval(Else, Row)
                     end
    end;
eval({like, A, P}, Row) ->
    sql_value:like(eval(A, Row), eval(P, Row));
eval({func, Name, Args}, Row) ->
    sql_func:apply(Name, [eval(A, Row) || A <- Args]);
eval({in, A, Exprs}, Row) ->
    case eval(A, Row) of
        null -> null;
        V    -> in_truth(V, [eval(E, Row) || E <- Exprs])
    end;
%% 副問い合わせは実行前に定数へ畳まれている。ここに来たら組み立ての誤り。
eval({scalar_subquery, _}, _Row) ->
    error(unresolved_subquery);
eval({exists_subquery, _}, _Row) ->
    error(unresolved_subquery);
eval({in_subquery, _, _}, _Row) ->
    error(unresolved_subquery);
eval({is_null, E}, Row) ->
    sql_value:is_null(eval(E, Row));
eval({is_not_null, E}, Row) ->
    not sql_value:is_null(eval(E, Row)).

%%----------------------------------------------------------------------
%% @doc 述語として評価し、行を通すかどうかを返す。
%% NULL(unknown)は false と同じく通さない。
%%----------------------------------------------------------------------
first_true([], _Row) ->
    not_found;
first_true([{C, V} | T], Row) ->
    case sql_value:keep(eval(C, Row)) of
        true  -> {ok, eval(V, Row)};
        false -> first_true(T, Row)
    end.

in_truth(_V, []) ->
    false;
in_truth(V, Vals) ->
    case lists:any(fun(X) -> sql_value:compare(V, X) =:= eq end, Vals) of
        true ->
            true;
        false ->
            case lists:member(null, Vals) of
                true  -> null;
                false -> false
            end
    end.

eval_pred(undefined, _Row) ->
    true;
eval_pred(Expr, Row) ->
    sql_value:keep(eval(Expr, Row)).

%% 比較の結果(lt|eq|gt|null)を演算子に応じた真偽値に落とす。
%% 比較が null なら結果も null(3値論理)。
comp_result(_Op, null) -> null;
comp_result('=',  eq) -> true;
comp_result('=',  _)  -> false;
comp_result('<>', eq) -> false;
comp_result('<>', _)  -> true;
comp_result('<',  lt) -> true;
comp_result('<',  _)  -> false;
comp_result('<=', gt) -> false;
comp_result('<=', _)  -> true;
comp_result('>',  gt) -> true;
comp_result('>',  _)  -> false;
comp_result('>=', lt) -> false;
comp_result('>=', _)  -> true.


%%%===================================================================
%%% 副問い合わせ節点の走査
%%%
%%% プランナ(論理→物理の変換)と実行器(定数への畳み込み)の両方が
%%% 式の中の副問い合わせを触る。式の形を知っているのはこのモジュールなので、
%%% 走査だけをここに置いて共有する。
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc 式の中の副問い合わせ節点に F を当てる。
%% F は節点を受け取り、置き換える式を返す。
%%----------------------------------------------------------------------
map_subqueries(F, {scalar_subquery, _} = E) -> F(E);
map_subqueries(F, {exists_subquery, _} = E) -> F(E);
map_subqueries(F, {in_subquery, A, P})      -> F({in_subquery, map_subqueries(F, A), P});
map_subqueries(F, {in, A, Es})              -> {in, map_subqueries(F, A),
                                                [map_subqueries(F, E) || E <- Es]};
map_subqueries(F, {func, N, Args})          -> {func, N, [map_subqueries(F, A) || A <- Args]};
map_subqueries(F, {like, A, P})             -> {like, map_subqueries(F, A),
                                                map_subqueries(F, P)};
map_subqueries(F, {'case', Ws, E})          ->
    {'case', [{map_subqueries(F, C), map_subqueries(F, V)} || {C, V} <- Ws],
     case E of undefined -> undefined; _ -> map_subqueries(F, E) end};
map_subqueries(F, {comp, Op, L, R})         -> {comp, Op, map_subqueries(F, L),
                                                map_subqueries(F, R)};
map_subqueries(F, {arith, Op, L, R})        -> {arith, Op, map_subqueries(F, L),
                                                map_subqueries(F, R)};
map_subqueries(F, {'and', Es})              -> {'and', [map_subqueries(F, E) || E <- Es]};
map_subqueries(F, {'or', Es})               -> {'or', [map_subqueries(F, E) || E <- Es]};
map_subqueries(F, {'not', E})               -> {'not', map_subqueries(F, E)};
map_subqueries(F, {neg, E})                 -> {neg, map_subqueries(F, E)};
map_subqueries(F, {is_null, E})             -> {is_null, map_subqueries(F, E)};
map_subqueries(F, {is_not_null, E})         -> {is_not_null, map_subqueries(F, E)};
map_subqueries(_F, E)                       -> E.
