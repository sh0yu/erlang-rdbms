%%%-------------------------------------------------------------------
%%% @doc
%%% 意味解析(binder)。ASTとカタログを突き合わせて実行可能な形にする。
%%%
%%% パーサは構文だけを見るのでカタログを知らない。ここが担うのは:
%%%   - テーブル名・カラム名の解決(存在チェック)
%%%   - `*` の展開
%%%   - **カラム参照を行タプル内の位置に変換する**
%%%   - 値の型検査
%%%
%%% 3番目が要点。ここで位置に落としておくと、実行器は行ごとに
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
-include("../include/catalog.hrl").

%%----------------------------------------------------------------------
%% @doc AST を実行可能な形に変換する。
%%
%% SELECT はプラン木に、それ以外は実行器を経由しない操作記述になる。
%% Returns: {ok, Op} | {error, Reason}
%%----------------------------------------------------------------------
analyze(#tx_stmt{op = Op}) ->
    {ok, {tx, Op}};

analyze(#create_table_stmt{table = TableStr, columns = Defs}) ->
    case duplicate_names([N || {N, _T} <- Defs]) of
        [] -> {ok, {create_table, to_atom(TableStr), [{to_atom(N), T} || {N, T} <- Defs]}};
        Dups -> {error, {duplicate_columns, Dups}}
    end;

analyze(#drop_table_stmt{table = TableStr}) ->
    with_table(TableStr, fun(Table, _Columns) -> {ok, {drop_table, Table}} end);

analyze(#insert_stmt{table = TableStr, columns = ColStrs, values = Values}) ->
    with_table(TableStr, fun(Table, Columns) -> bind_insert(Table, Columns, ColStrs, Values) end);

analyze(#update_stmt{table = TableStr, set = Set, where = Where}) ->
    with_table(TableStr, fun(Table, Columns) -> bind_update(Table, Columns, Set, Where) end);

analyze(#delete_stmt{table = TableStr, where = Where}) ->
    with_table(TableStr,
               fun(Table, Columns) ->
                       case bind_where(Where, Columns) of
                           {error, Reason} -> {error, Reason};
                           {ok, Pred} -> {ok, {delete, Table, Pred}}
                       end
               end);

analyze(#select_stmt{from = #table_ref{name = TableStr}} = Stmt) ->
    with_table(TableStr, fun(Table, Columns) -> bind_select(Stmt, Table, Columns) end).

%%%===================================================================
%%% SELECT
%%%===================================================================

bind_select(#select_stmt{columns = Cols, where = Where}, Table, Columns) ->
    case bind_where(Where, Columns) of
        {error, Reason} ->
            {error, Reason};
        {ok, Pred} ->
            case bind_projection(Cols, Columns) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Exprs, Names} ->
                    Scan = #p_seq_scan{table = Table, schema = names(Columns)},
                    Filtered = case Pred of
                                   undefined -> Scan;
                                   _ -> #p_filter{pred = Pred, input = Scan}
                               end,
                    {ok, {select, #p_project{exprs = Exprs, names = Names, input = Filtered}}}
            end
    end.

bind_projection([#star{}], Columns) ->
    {ok, [{ref, N} || N <- lists:seq(1, length(Columns))], names(Columns)};
bind_projection(Items, Columns) ->
    bind_projection_1(Items, Columns, [], []).

bind_projection_1([], _Columns, Exprs, Names) ->
    {ok, lists:reverse(Exprs), lists:reverse(Names)};
bind_projection_1([#star{} | _T], _Columns, _Exprs, _Names) ->
    %% SELECT a, * のような形は今は扱わない
    {error, star_must_be_alone};
bind_projection_1([Item | T], Columns, Exprs, Names) ->
    case bind_expr(Item, Columns) of
        {error, Reason} ->
            {error, Reason};
        {ok, Bound} ->
            bind_projection_1(T, Columns, [Bound | Exprs],
                              [projection_name(Item, length(Exprs) + 1) | Names])
    end.

%% 出力カラムの名前。カラム参照ならその名前、式なら位置から作る。
projection_name(#col_ref{name = NameStr}, _N) ->
    list_to_existing_atom(NameStr);
projection_name(_Expr, N) ->
    list_to_atom("column" ++ integer_to_list(N)).

%%%===================================================================
%%% INSERT
%%%===================================================================

%% VALUES をテーブルのカラム順に並べ替え、型を検査する。
%% カラム名を省略した場合は宣言順とみなす。
bind_insert(Table, Columns, undefined, Values) ->
    case length(Values) =:= length(Columns) of
        false -> {error, column_count_mismatch};
        true -> typed_row(Table, Columns, lists:zip(names(Columns), Values))
    end;
bind_insert(Table, Columns, ColStrs, Values) ->
    case length(ColStrs) =:= length(Values) of
        false ->
            {error, column_count_mismatch};
        true ->
            case resolve_columns(ColStrs, Columns) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Named} ->
                    case duplicate_names([C#column.name || C <- Named]) of
                        [] ->
                            Pairs = lists:zip([C#column.name || C <- Named], Values),
                            typed_row(Table, Columns, Pairs);
                        Dups ->
                            {error, {duplicate_columns, Dups}}
                    end
            end
    end.

%% 指定のなかったカラムは NULL で埋める。
typed_row(Table, Columns, Pairs) ->
    try [value_for(C, Pairs) || C <- Columns] of
        Row ->
            case check_types(Columns, Row) of
                ok -> {ok, {insert, Table, Row}};
                {error, Reason} -> {error, Reason}
            end
    catch
        throw:{invalid_value_expression, _} -> {error, only_constants_in_values}
    end.

value_for(#column{name = Name}, Pairs) ->
    case lists:keyfind(Name, 1, Pairs) of
        {Name, Expr} -> const_value(Expr);
        false -> null
    end.

%% VALUES に書けるのは定数式だけ。カラム参照は入れられない
%% (挿入する行がまだ存在しないため)。
const_value(Expr) ->
    case bind_expr(Expr, []) of
        {ok, Bound} -> sql_expr:eval(Bound, {});
        {error, _} -> throw({invalid_value_expression, Expr})
    end.

check_types([], []) ->
    ok;
check_types([#column{name = Name, type = Type} | CT], [V | VT]) ->
    case sql_type:check(Type, V) of
        ok -> check_types(CT, VT);
        {error, Reason} -> {error, {Reason, Name, Type, V}}
    end.

%%%===================================================================
%%% UPDATE
%%%===================================================================

bind_update(Table, Columns, Set, Where) ->
    case bind_where(Where, Columns) of
        {error, Reason} ->
            {error, Reason};
        {ok, Pred} ->
            case bind_assignments(Set, Columns, []) of
                {error, Reason} -> {error, Reason};
                {ok, Assigns} -> {ok, {update, Table, Assigns, Pred}}
            end
    end.

%% 代入は {カラム位置, 新しい値} に落とす。
bind_assignments([], _Columns, Acc) ->
    {ok, lists:reverse(Acc)};
bind_assignments([{NameStr, Expr} | T], Columns, Acc) ->
    case resolve_column(NameStr, Columns) of
        {error, Reason} ->
            {error, Reason};
        {ok, #column{name = Name, type = Type, position = Pos}} ->
            case bind_expr(Expr, Columns) of
                {error, Reason} ->
                    {error, Reason};
                %% 定数ならこの場で型を検査できる
                {ok, {const, V}} ->
                    case sql_type:check(Type, V) of
                        ok -> bind_assignments(T, Columns, [{Pos, {const, V}} | Acc]);
                        {error, Reason} -> {error, {Reason, Name, Type, V}}
                    end;
                %% 式は行ごとに値が決まるので、検査は実行時
                {ok, Bound} ->
                    bind_assignments(T, Columns, [{Pos, Bound} | Acc])
            end
    end.

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
        {ok, #column{position = Pos}} -> {ok, {ref, Pos}}
    end;
bind_expr(#binop{op = Op, left = L, right = R}, Columns) ->
    case bind_pair(L, R, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, BL, BR} -> {ok, binop(Op, BL, BR)}
    end;
bind_expr(#unop{op = 'not', arg = A}, Columns) ->
    case bind_expr(A, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, BA} -> {ok, {'not', BA}}
    end;
bind_expr(#unop{op = '-', arg = A}, Columns) ->
    case bind_expr(A, Columns) of
        {error, Reason} -> {error, Reason};
        %% 定数なら畳んでおく。-1 が {neg,{const,1}} のままだと
        %% 型検査で「integerでない」と誤判定してしまう。
        {ok, {const, V}} when is_number(V) -> {ok, {const, -V}};
        {ok, BA} -> {ok, {neg, BA}}
    end;
bind_expr(#is_null{arg = A, negated = Neg}, Columns) ->
    case bind_expr(A, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, BA} when Neg -> {ok, {is_not_null, BA}};
        {ok, BA} -> {ok, {is_null, BA}}
    end.

bind_pair(L, R, Columns) ->
    case bind_expr(L, Columns) of
        {error, Reason} ->
            {error, Reason};
        {ok, BL} ->
            case bind_expr(R, Columns) of
                {error, Reason} -> {error, Reason};
                {ok, BR} -> {ok, BL, BR}
            end
    end.

%% 論理・算術・比較を、評価器が受ける形に振り分ける。
%% AND / OR は n項に平坦化しておく。述語のプッシュダウン(将来)で
%% 連言を分解するとき、2項木のままだと平坦化を何度も書くことになる。
binop('and', L, R) -> {'and', conjuncts(L) ++ conjuncts(R)};
binop('or', L, R) -> {'or', disjuncts(L) ++ disjuncts(R)};
binop(Op, L, R) when Op =:= '+'; Op =:= '-'; Op =:= '*'; Op =:= '/' ->
    {arith, Op, L, R};
binop(Op, L, R) -> {comp, Op, L, R}.

conjuncts({'and', Es}) -> Es;
conjuncts(E) -> [E].

disjuncts({'or', Es}) -> Es;
disjuncts(E) -> [E].

%%%===================================================================
%%% 名前の解決
%%%===================================================================

with_table(TableStr, Fun) ->
    case resolve_table(TableStr) of
        {error, Reason} -> {error, Reason};
        {ok, Table, Columns} -> Fun(Table, Columns)
    end.

resolve_table(NameStr) ->
    case to_existing_atom(NameStr) of
        error ->
            {error, {table_not_found, NameStr}};
        {ok, Table} ->
            case sys_tbl_mng:get_columns(whereis(sys_tbl_mng), Table) of
                {error, table_not_found} -> {error, {table_not_found, NameStr}};
                {ok, Columns} -> {ok, Table, Columns}
            end
    end.

resolve_column(NameStr, Columns) ->
    case to_existing_atom(NameStr) of
        error ->
            {error, {column_not_found, NameStr}};
        {ok, Name} ->
            case lists:keyfind(Name, #column.name, Columns) of
                false -> {error, {column_not_found, NameStr}};
                #column{} = C -> {ok, C}
            end
    end.

resolve_columns(NameStrs, Columns) ->
    resolve_columns(NameStrs, Columns, []).

resolve_columns([], _Columns, Acc) ->
    {ok, lists:reverse(Acc)};
resolve_columns([NameStr | T], Columns, Acc) ->
    case resolve_column(NameStr, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, C} -> resolve_columns(T, Columns, [C | Acc])
    end.

names(Columns) ->
    [C#column.name || C <- Columns].

duplicate_names(Names) ->
    lists:usort(Names -- lists:usort(Names)).

%% カタログに存在する名前だけをアトムにする。
%% 任意の入力を list_to_atom/1 に通すとアトム表を際限なく増やせてしまう。
to_existing_atom(Str) ->
    try {ok, list_to_existing_atom(Str)}
    catch error:badarg -> error
    end.

%% CREATE TABLE だけは、まだ存在しない名前をアトムにする必要がある。
to_atom(Str) ->
    list_to_atom(Str).
