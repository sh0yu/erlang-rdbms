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
%%%   {is_null, Expr} | {is_not_null, Expr}
%%%
%%% 比較と論理は必ず sql_value を経由する。Erlangの `<` や `andalso` を
%%% 直接使うと、NULLの扱いと項順序で誤る。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_expr).

-export([eval/2, eval_pred/2]).

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
eval({is_null, E}, Row) ->
    sql_value:is_null(eval(E, Row));
eval({is_not_null, E}, Row) ->
    not sql_value:is_null(eval(E, Row)).

%%----------------------------------------------------------------------
%% @doc 述語として評価し、行を通すかどうかを返す。
%% NULL(unknown)は false と同じく通さない。
%%----------------------------------------------------------------------
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
