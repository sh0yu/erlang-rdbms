%%%-------------------------------------------------------------------
%%% @doc
%%% 意味解析(binder)。ASTとカタログを突き合わせて実行プランを組み立てる。
%%%
%%% パーサは構文だけを見るのでカタログを知らない。ここが担うのは:
%%%   - テーブル名・カラム名の解決(存在チェック)
%%%   - `*` の展開
%%%   - **カラム参照を行タプル内の位置に変換する**
%%%
%%% 最後が要点。ここで位置に落としておくと、実行器は行ごとに
%%% 名前を引かずに element/2 で済む。実エンジンと同じやり方。
%%%
%%% 識別子は字句解析の時点では文字列のままで、ここで初めてアトムになる。
%%% カタログに載っている名前だけをアトム化するので、任意の入力で
%%% アトム表を増やされることがない。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_analyzer).

-export([analyze/1]).

-include("../include/sql.hrl").
-include("../include/plan.hrl").

%%----------------------------------------------------------------------
%% @doc AST を実行プランに変換する。
%% Returns: {ok, Plan} | {error, Reason}
%%----------------------------------------------------------------------
analyze(#select_stmt{from = #table_ref{name = TableName}} = Stmt) ->
    case resolve_table(TableName) of
        {error, Reason} ->
            {error, Reason};
        {ok, Table, Columns} ->
            build_select(Stmt, Table, Columns)
    end.

build_select(#select_stmt{columns = Cols, where = Where}, Table, Columns) ->
    case bind_where(Where, Columns) of
        {error, Reason} ->
            {error, Reason};
        {ok, Pred} ->
            case bind_projection(Cols, Columns) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Exprs, Names} ->
                    Scan = #p_seq_scan{table = Table, schema = Columns},
                    Filtered = case Pred of
                                   undefined -> Scan;
                                   _ -> #p_filter{pred = Pred, input = Scan}
                               end,
                    {ok, #p_project{exprs = Exprs, names = Names, input = Filtered}}
            end
    end.

%%%===================================================================
%%% 名前の解決
%%%===================================================================

%% テーブル名(文字列)をカタログと突き合わせてアトムにする。
resolve_table(NameStr) ->
    case to_existing_atom(NameStr) of
        error ->
            {error, {table_not_found, NameStr}};
        {ok, Table} ->
            case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), Table) of
                {error, table_not_found} -> {error, {table_not_found, NameStr}};
                {ok, Columns} -> {ok, Table, Columns}
            end
    end.

%% カラム名(文字列)を、行タプル内の位置に解決する。
resolve_column(NameStr, Columns) ->
    case to_existing_atom(NameStr) of
        error ->
            {error, {column_not_found, NameStr}};
        {ok, Col} ->
            case index_of(Col, Columns, 1) of
                not_found -> {error, {column_not_found, NameStr}};
                Pos -> {ok, Col, Pos}
            end
    end.

index_of(_Col, [], _N) -> not_found;
index_of(Col, [Col | _], N) -> N;
index_of(Col, [_ | T], N) -> index_of(Col, T, N + 1).

%% カタログに存在する名前だけをアトムにする。
%% 任意の入力を list_to_atom/1 に通すとアトム表を際限なく増やせてしまう。
to_existing_atom(Str) ->
    try {ok, list_to_existing_atom(Str)}
    catch error:badarg -> error
    end.

%%%===================================================================
%%% 射影
%%%===================================================================

bind_projection([#star{}], Columns) ->
    Exprs = [{ref, N} || N <- lists:seq(1, length(Columns))],
    {ok, Exprs, Columns};
bind_projection(Items, Columns) ->
    bind_projection_1(Items, Columns, [], []).

bind_projection_1([], _Columns, Exprs, Names) ->
    {ok, lists:reverse(Exprs), lists:reverse(Names)};
bind_projection_1([#col_ref{name = NameStr} | T], Columns, Exprs, Names) ->
    case resolve_column(NameStr, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, Col, Pos} -> bind_projection_1(T, Columns, [{ref, Pos} | Exprs], [Col | Names])
    end;
bind_projection_1([#star{} | _T], _Columns, _Exprs, _Names) ->
    %% SELECT a, * のような形は今は扱わない
    {error, star_must_be_alone}.

%%%===================================================================
%%% WHERE
%%%===================================================================

bind_where(undefined, _Columns) ->
    {ok, undefined};
bind_where(Expr, Columns) ->
    bind_expr(Expr, Columns).

bind_expr(#const{value = Value}, _Columns) ->
    {ok, {const, Value}};
bind_expr(#col_ref{name = NameStr}, Columns) ->
    case resolve_column(NameStr, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, _Col, Pos} -> {ok, {ref, Pos}}
    end;
bind_expr(#binop{op = Op, left = L, right = R}, Columns) ->
    case bind_expr(L, Columns) of
        {error, Reason} ->
            {error, Reason};
        {ok, BL} ->
            case bind_expr(R, Columns) of
                {error, Reason} -> {error, Reason};
                {ok, BR} -> {ok, {comp, Op, BL, BR}}
            end
    end.
