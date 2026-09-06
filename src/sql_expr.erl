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

-export([eval/2, eval/3, eval_pred/2, eval_pred/3, map_subqueries/2]).

%%----------------------------------------------------------------------
%% @doc 式を1行に対して評価する。Row は行タプル。
%%
%% Env は相関副問い合わせのためにある。
%%   outers  外側の行の並び(1つ目が1段外)
%%   run     副問い合わせを実行する関数。実行器が入れる
%% 相関を含まない式なら空のままでよい。
%%----------------------------------------------------------------------
eval(Expr, Row) -> eval(Expr, Row, #{}).

eval({const, Value}, _Row, _Env) ->
    Value;
eval({ref, Pos}, Row, _Env) ->
    element(Pos, Row);
eval({comp, Op, L, R}, Row, Env) ->
    comp_result(Op, sql_value:compare(eval(L, Row, Env), eval(R, Row, Env)));
eval({'and', Exprs}, Row, Env) ->
    lists:foldl(fun(E, Acc) -> sql_value:truth_and(Acc, eval(E, Row, Env)) end, true, Exprs);
eval({'or', Exprs}, Row, Env) ->
    lists:foldl(fun(E, Acc) -> sql_value:truth_or(Acc, eval(E, Row, Env)) end, false, Exprs);
eval({'not', E}, Row, Env) ->
    sql_value:truth_not(eval(E, Row, Env));
eval({neg, E}, Row, Env) ->
    sql_value:arith('-', 0, eval(E, Row, Env));
eval({arith, Op, L, R}, Row, Env) ->
    sql_value:arith(Op, eval(L, Row, Env), eval(R, Row, Env));
%% x IN (...) の3値論理。
%%   x が NULL          → unknown
%%   一致がある         → true
%%   一致が無くNULL有り → unknown(そのNULLが x かもしれない)
%%   一致が無くNULL無し → false
%% CASE。条件が **true** の最初の枝を採る。null と false は等しく飛ばす。
eval({'case', Whens, Else}, Row, Env) ->
    case first_true(Whens, Row, Env) of
        {ok, V}   -> V;
        not_found -> case Else of
                         undefined -> null;
                         _         -> eval(Else, Row, Env)
                     end
    end;
eval({like, A, P}, Row, Env) ->
    sql_value:like(eval(A, Row, Env), eval(P, Row, Env));
eval({func, Name, Args}, Row, Env) ->
    sql_func:apply(Name, [eval(A, Row, Env) || A <- Args]);
eval({in, A, Exprs}, Row, Env) ->
    case eval(A, Row, Env) of
        null -> null;
        V    -> in_truth(V, [eval(E, Row, Env) || E <- Exprs])
    end;
%% 副問い合わせは実行前に定数へ畳まれている。ここに来たら組み立ての誤り。
%% 外側の行の列。Level 段だけ外の行から引く。
eval({outer, Level, Pos}, _Row, Env) ->
    Outers = maps:get(outers, Env, []),
    case length(Outers) >= Level of
        true  -> element(Pos, lists:nth(Level, Outers));
        false -> error({no_outer_row, Level})
    end;

%% 相関副問い合わせ。**行ごとに実行し直す。**
%% 相関しないものは実行前に定数へ畳まれているので、ここへは来ない。
eval({scalar_subquery, Plan}, Row, Env) ->
    case run_sub(Plan, Row, Env) of
        []      -> null;
        [[V]]   -> V;
        Rows    -> error({scalar_subquery_returned_rows, length(Rows)})
    end;
eval({exists_subquery, Plan}, Row, Env) ->
    run_sub(Plan, Row, Env) =/= [];
eval({in_subquery, A, Plan}, Row, Env) ->
    case eval(A, Row, Env) of
        null -> null;
        V    -> in_truth(V, [X || [X] <- run_sub(Plan, Row, Env)])
    end;
eval({is_null, E}, Row, Env) ->
    sql_value:is_null(eval(E, Row, Env));
eval({is_not_null, E}, Row, Env) ->
    not sql_value:is_null(eval(E, Row, Env)).

%%----------------------------------------------------------------------
%% @doc 述語として評価し、行を通すかどうかを返す。
%% NULL(unknown)は false と同じく通さない。
%%----------------------------------------------------------------------
first_true([], _Row, _Env) ->
    not_found;
first_true([{C, V} | T], Row, Env) ->
    case sql_value:keep(eval(C, Row, Env)) of
        true  -> {ok, eval(V, Row, Env)};
        false -> first_true(T, Row, Env)
    end.

%% 副問い合わせを、いまの行を外側として実行する。
%% 実行器が run を入れていなければ、相関副問い合わせは使えない。
run_sub(Plan, Row, Env) ->
    case maps:get(run, Env, undefined) of
        undefined -> error(no_subquery_runner);
        Run       -> Run(Plan, Row)
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

eval_pred(Expr, Row) -> eval_pred(Expr, Row, #{}).

eval_pred(undefined, _Row, _Env) ->
    true;
eval_pred(Expr, Row, Env) ->
    sql_value:keep(eval(Expr, Row, Env)).

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
