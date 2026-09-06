%%%-------------------------------------------------------------------
%%% @doc
%%% 意味解析(binder)。ASTとカタログを突き合わせて**論理プラン**にする。
%%%
%%% ここが出すのは「何をするか」までで、どうやるかは決めない。
%%% 索引を使うか、どの順で結合するかは sql_planner の仕事。
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
-include("../include/logical.hrl").
-include("../include/catalog.hrl").

%% 名前解決のスコープ。結合すると複数テーブルのカラムが1つの行に並ぶので、
%% 別名とカラム名の組で引き、位置は連結後の行における位置になる。
-record(sc, {alias, name, type, pos}).

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

%% CREATE INDEX。索引名は新しい名前なので list_to_atom で作る。
%% テーブル名・カラム名はカタログに載っているものだけを解決する。
analyze(#create_index_stmt{name = NameStr, table = TableStr, column = ColStr}) ->
    with_table(TableStr,
               fun(Table, Columns) ->
                       case to_existing_atom(ColStr) of
                           error ->
                               {error, {no_such_column, ColStr}};
                           {ok, Col} ->
                               case lists:keyfind(Col, #column.name, Columns) of
                                   false -> {error, {no_such_column, ColStr}};
                                   _     -> {ok, {create_index, to_atom(NameStr), Table, Col}}
                               end
                       end
               end);

analyze(#drop_index_stmt{name = NameStr}) ->
    %% 存在しない索引名は「見つからない」であって新しい名前ではないので、
    %% ここでアトム表を増やさない。
    case to_existing_atom(NameStr) of
        error     -> {error, {index_not_found, NameStr}};
        {ok, Name} -> {ok, {drop_index, Name}}
    end;

analyze(#drop_table_stmt{table = TableStr}) ->
    with_table(TableStr, fun(Table, _Columns) -> {ok, {drop_table, Table}} end);

analyze(#insert_stmt{table = TableStr, columns = ColStrs, values = Values}) ->
    with_table(TableStr, fun(Table, Columns) -> bind_insert(Table, Columns, ColStrs, Values) end);

analyze(#update_stmt{table = TableStr, set = Set, where = Where}) ->
    with_table(TableStr,
               fun(Table, Columns) ->
                       bind_update(Table, Columns, scope_of(Table, Columns), Set, Where)
               end);

analyze(#delete_stmt{table = TableStr, where = Where}) ->
    with_table(TableStr,
               fun(Table, Columns) ->
                       case bind_where(Where, scope_of(Table, Columns)) of
                           {error, Reason} -> {error, Reason};
                           {ok, Pred} -> {ok, {delete, Table, Pred}}
                       end
               end);

%% EXPLAIN は中の文を解析するだけで実行しない。
%% 実行計画を持つのは SELECT だけなので、他は断る。
analyze(#explain_stmt{stmt = Inner}) ->
    case analyze(Inner) of
        {ok, {select, Logical}} -> {ok, {explain, Logical}};
        {ok, _Other}            -> {error, explain_requires_select};
        {error, Reason}         -> {error, Reason}
    end;

analyze(#select_stmt{from = From} = Stmt) ->
    case build_from(From) of
        {error, Reason} -> {error, Reason};
        {ok, Scope, Node} -> bind_select(Stmt, Scope, Node)
    end.

%%%===================================================================
%%% FROM句からスコープと走査プランを組み立てる
%%%===================================================================

build_from(#table_ref{name = TableStr, alias = Alias}) ->
    case resolve_table(TableStr) of
        {error, Reason} ->
            {error, Reason};
        {ok, Table, Columns} ->
            Name = case Alias of undefined -> Table; _ -> list_to_atom(Alias) end,
            Scope = [#sc{alias = Name, name = C#column.name, type = C#column.type,
                         pos = C#column.position}
                     || C <- Columns],
            {ok, Scope, #lp_scan{table = Table, schema = [C#column.name || C <- Columns]}}
    end;
build_from(#join{type = Type, left = L, right = R, on = On}) ->
    case build_from(L) of
        {error, Reason} ->
            {error, Reason};
        {ok, LScope, LNode} ->
            case build_from(R) of
                {error, Reason} ->
                    {error, Reason};
                {ok, RScope, RNode} ->
                    %% 右側のカラムは左の幅だけずれる
                    Width = length(LScope),
                    Shifted = [S#sc{pos = S#sc.pos + Width} || S <- RScope],
                    Scope = LScope ++ Shifted,
                    case bind_on(On, Scope) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, Pred} ->
                            {ok, Scope, #lp_join{type = Type, pred = Pred,
                                                   left = LNode, right = RNode,
                                                   right_width = length(RScope)}}
                    end
            end
    end.

bind_on(undefined, _Scope) -> {ok, undefined};
bind_on(Expr, Scope) -> bind_expr(Expr, Scope).

%% 単一テーブルの操作(INSERT/UPDATE/DELETE)用のスコープ。
scope_of(Table, Columns) ->
    [#sc{alias = Table, name = C#column.name, type = C#column.type,
         pos = C#column.position} || C <- Columns].

%%%===================================================================
%%% SELECT
%%%===================================================================

bind_select(#select_stmt{group_by = G, having = H} = Stmt, Scope, Node)
  when G =/= []; H =/= undefined ->
    bind_grouped_select(Stmt, Scope, Node);
bind_select(#select_stmt{columns = Cols} = Stmt, Scope, Node) ->
    case has_aggregate(Cols) of
        %% GROUP BY が無くても集約があれば、全体を1グループとして集約する
        true -> bind_grouped_select(Stmt, Scope, Node);
        false -> bind_plain_select(Stmt, Scope, Node)
    end.

bind_plain_select(#select_stmt{columns = Cols, where = Where, order_by = Order,
                              distinct = Distinct, limit = Limit, offset = Offset},
                  Scope, Node) ->
    Columns = Scope,
    case bind_where(Where, Columns) of
        {error, Reason} ->
            {error, Reason};
        {ok, Pred} ->
            case bind_projection(Cols, Columns) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Exprs, Names} ->
                    %% 並べ替えは射影の**前**に置く。ORDER BY のキーは
                    %% 出力に含まれないカラムでもよい(SELECT name ... ORDER BY price)。
                    case bind_order(Order, Columns) of
                        {error, Reason} ->
                            {error, Reason};
                        {ok, Keys} ->
                            Filtered = wrap_filter(Pred, Node),
                            Sorted = wrap_sort(Keys, Limit, Offset, Filtered),
                            Projected = #lp_project{exprs = Exprs, names = Names,
                                                   input = Sorted},
                            %% DISTINCT は射影の後(出力する列で重複を見る)
                            Distincted = wrap_distinct(Distinct, Projected),
                            {ok, {select, wrap_limit(Limit, Offset, Keys, Distincted)}}
                    end
            end
    end.

%%%===================================================================
%%% 集約つきSELECT
%%%
%%% 集約の出力行は [グループキー..., 集約結果...] の順に並ぶ。
%%% 射影・HAVING・ORDER BY はこの位置を参照する形に書き換える。
%%%===================================================================

bind_grouped_select(#select_stmt{columns = Cols, where = Where, group_by = Group,
                                 having = Having, order_by = Order,
                                 distinct = Distinct, limit = Limit, offset = Offset},
                    Scope, Node) ->
    Columns = Scope,
    with_ok(
      [fun() -> bind_where(Where, Columns) end,
       fun() -> bind_all(Group, Columns) end],
      fun([Pred, Keys]) ->
              %% 射影とHAVINGの中の集約を集めて、参照に置き換える
              Ctx0 = #{keys => Keys, aggs => [], columns => Columns},
              case rewrite_items(Cols, Ctx0) of
                  {error, Reason} ->
                      {error, Reason};
                  {ok, Exprs, Names, Ctx1} ->
                      case rewrite_having(Having, Ctx1) of
                          {error, Reason} ->
                              {error, Reason};
                          {ok, BHaving, #{aggs := Aggs}} ->
                              Agg = #lp_agg{group_by = Keys, aggs = lists:reverse(Aggs),
                                           having = BHaving,
                                           input = wrap_filter(Pred, Node)},
                              %% 並べ替えは集約と射影の**間**に置く。
                              %% ORDER BY のキーは集約後の行の位置を指しており、
                              %% 射影の後(出力行)ではその位置が変わる。
                              case bind_order_after_agg(Order, Ctx1) of
                                  {error, Reason} ->
                                      {error, Reason};
                                  {ok, SortKeys} ->
                                      Sorted = wrap_sort_after(SortKeys, Agg),
                                      Proj = #lp_project{exprs = Exprs, names = Names,
                                                        input = Sorted},
                                      D = wrap_distinct(Distinct, Proj),
                                      {ok, {select, wrap_limit(Limit, Offset, SortKeys, D)}}
                              end
                      end
              end
      end).

%% 集約の後ろに置く並べ替えは、射影済みの行に対して働く。
wrap_sort_after([], Node) -> Node;
wrap_sort_after(Keys, Node) -> #lp_sort{keys = Keys, input = Node}.

%% 射影の各項目を、集約後の行を指す形に書き換える。
rewrite_items(Items, Ctx) ->
    rewrite_items(Items, Ctx, [], []).

rewrite_items([], Ctx, Exprs, Names) ->
    {ok, lists:reverse(Exprs), lists:reverse(Names), Ctx};
rewrite_items([#star{} | _], _Ctx, _E, _N) ->
    {error, star_with_group_by};
rewrite_items([Item | T], Ctx, Exprs, Names) ->
    case rewrite(Item, Ctx) of
        {error, Reason} ->
            {error, Reason};
        {ok, Bound, Ctx2} ->
            rewrite_items(T, Ctx2, [Bound | Exprs],
                          [projection_name(Item, length(Exprs) + 1) | Names])
    end.

rewrite_having(undefined, Ctx) ->
    {ok, undefined, Ctx};
rewrite_having(Expr, Ctx) ->
    case rewrite(Expr, Ctx) of
        {error, Reason} -> {error, Reason};
        {ok, Bound, Ctx2} -> {ok, Bound, Ctx2}
    end.

%% 式を集約後の行を指す形に書き換える。
%%   集約呼び出し  -> 集約結果の位置への参照
%%   GROUP BY のキーと一致する式 -> キーの位置への参照
%%   それ以外のカラム参照 -> エラー(どの行の値か決まらない)
rewrite(#func{} = F, Ctx) ->
    add_aggregate(F, Ctx);
rewrite(#const{value = V}, Ctx) ->
    {ok, {const, V}, Ctx};
rewrite(Expr, Ctx) ->
    case contains_func(Expr) of
        %% 集約を含む式は元のカラムには束縛できない。
        %% (bind_expr は #func{} を知らない)
        true ->
            rewrite_children(Expr, undefined, Ctx);
        false ->
            rewrite_grouped(Expr, Ctx)
    end.

%% 集約を含まない式は、GROUP BY のキーと一致すればその位置を指す。
%% 一致しなければ「どの行の値か決まらない」のでエラー。
rewrite_grouped(Expr, #{keys := Keys, columns := Columns} = Ctx) ->
    case bind_expr(Expr, Columns) of
        {error, Reason} ->
            {error, Reason};
        {ok, Bound} ->
            case index_of_key(Bound, Keys) of
                {ok, Pos} -> {ok, {ref, Pos}, Ctx};
                not_found -> rewrite_children(Expr, Bound, Ctx)
            end
    end.

%% GROUP BY に無い式でも、中の部分式が集約なら書き換えられる
%% (例: COUNT(*) + 1)。カラム参照が残る場合だけエラーにする。
rewrite_children(#binop{op = Op, left = L, right = R}, _Bound, Ctx) ->
    case rewrite(L, Ctx) of
        {error, Reason} ->
            {error, Reason};
        {ok, BL, Ctx1} ->
            case rewrite(R, Ctx1) of
                {error, Reason} -> {error, Reason};
                {ok, BR, Ctx2} -> {ok, binop(Op, BL, BR), Ctx2}
            end
    end;
rewrite_children(#unop{op = Op, arg = A}, _Bound, Ctx) ->
    case rewrite(A, Ctx) of
        {error, Reason} -> {error, Reason};
        {ok, BA, Ctx2} when Op =:= 'not' -> {ok, {'not', BA}, Ctx2};
        {ok, BA, Ctx2} -> {ok, {neg, BA}, Ctx2}
    end;
rewrite_children(#col_ref{name = Name}, _Bound, _Ctx) ->
    {error, {not_grouped, Name}};
rewrite_children(_Expr, _Bound, _Ctx) ->
    {error, not_grouped}.

%% 集約を登録して、その結果を指す参照を返す。
%% 同じ集約が複数回出てきたら1つにまとめる。
add_aggregate(#func{name = NameStr, args = Args, distinct = Dist},
              #{keys := Keys, aggs := Aggs, columns := Columns} = Ctx) ->
    case agg_func(string:lowercase(NameStr), Args) of
        {error, Reason} ->
            {error, Reason};
        {ok, Func, ArgExpr} ->
            case bind_agg_arg(ArgExpr, Columns) of
                {error, Reason} ->
                    {error, Reason};
                {ok, BArg} ->
                    Agg = #agg{func = Func, arg = BArg, distinct = Dist},
                    case index_of(Agg, lists:reverse(Aggs)) of
                        {ok, N} ->
                            {ok, {ref, length(Keys) + N}, Ctx};
                        not_found ->
                            N = length(Aggs) + 1,
                            {ok, {ref, length(Keys) + N}, Ctx#{aggs => [Agg | Aggs]}}
                    end
            end
    end.

bind_agg_arg(undefined, _Columns) -> {ok, undefined};
bind_agg_arg(Expr, Columns) -> bind_expr(Expr, Columns).

agg_func("count", star) -> {ok, count_star, undefined};
agg_func("count", [A]) -> {ok, count, A};
agg_func("sum", [A]) -> {ok, sum, A};
agg_func("avg", [A]) -> {ok, avg, A};
agg_func("min", [A]) -> {ok, min, A};
agg_func("max", [A]) -> {ok, max, A};
agg_func(Name, star) -> {error, {star_not_allowed, Name}};
agg_func(Name, _) -> {error, {unknown_function, Name}}.

%% ORDER BY は射影後の行を指すので、出力カラム名で解決する。
bind_order_after_agg(Items, Ctx) ->
    bind_order_after_agg(Items, Ctx, []).

bind_order_after_agg([], _Ctx, Acc) ->
    {ok, lists:reverse(Acc)};
bind_order_after_agg([#sort_item{expr = E, dir = Dir, nulls = Nulls} | T], Ctx, Acc) ->
    case rewrite(E, Ctx) of
        {error, Reason} -> {error, Reason};
        {ok, Bound, _} -> bind_order_after_agg(T, Ctx, [{Bound, Dir, nulls_for(Dir, Nulls)} | Acc])
    end.

index_of_key(Bound, Keys) -> index_of(Bound, Keys).

index_of(X, L) -> index_of(X, L, 1).
index_of(_X, [], _N) -> not_found;
index_of(X, [X | _], N) -> {ok, N};
index_of(X, [_ | T], N) -> index_of(X, T, N + 1).

bind_all(Exprs, Columns) ->
    bind_all(Exprs, Columns, []).
bind_all([], _Columns, Acc) ->
    {ok, lists:reverse(Acc)};
bind_all([E | T], Columns, Acc) ->
    case bind_expr(E, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, B} -> bind_all(T, Columns, [B | Acc])
    end.

%% {ok, _} を返す関数を順に呼び、1つでも失敗したらそこで止める。
with_ok(Funs, Cont) ->
    with_ok(Funs, [], Cont).
with_ok([], Acc, Cont) ->
    Cont(lists:reverse(Acc));
with_ok([F | T], Acc, Cont) ->
    case F() of
        {error, Reason} -> {error, Reason};
        {ok, V} -> with_ok(T, [V | Acc], Cont)
    end.

%% 射影に集約が含まれるか。GROUP BY が無くても集約があれば集約プランになる。
has_aggregate(Items) ->
    lists:any(fun contains_func/1, Items).

contains_func(#func{}) -> true;
contains_func(#binop{left = L, right = R}) -> contains_func(L) orelse contains_func(R);
contains_func(#unop{arg = A}) -> contains_func(A);
contains_func(#is_null{arg = A}) -> contains_func(A);
contains_func(_) -> false.

wrap_filter(undefined, Node) -> Node;
wrap_filter(Pred, Node) -> #lp_filter{pred = Pred, input = Node}.

wrap_sort([], _Limit, _Offset, Node) ->
    Node;
wrap_sort(Keys, Limit, Offset, Node) ->
    %% LIMIT があるなら、全件並べずに上位 (offset + count) 件だけ保てばよい。
    %% DISTINCT があると件数が変わるので、その場合は上限を渡さない。
    #lp_sort{keys = Keys, limit = topn_limit(Limit, Offset), input = Node}.

topn_limit(undefined, _Offset) -> undefined;
topn_limit(Count, undefined) -> Count;
topn_limit(Count, Offset) -> Count + Offset.

wrap_distinct(false, Node) -> Node;
wrap_distinct(true, Node) -> #lp_distinct{input = Node}.

wrap_limit(undefined, undefined, _Keys, Node) ->
    Node;
wrap_limit(Count, Offset, _Keys, Node) ->
    #lp_limit{count = Count, offset = default_offset(Offset), input = Node}.

default_offset(undefined) -> 0;
default_offset(N) -> N.

%% ORDER BY のキーを束縛する。
%% NULLの位置の既定はPostgreSQLに合わせ、ASCならlast、DESCならfirst。
bind_order(Items, Columns) ->
    bind_order(Items, Columns, []).

bind_order([], _Columns, Acc) ->
    {ok, lists:reverse(Acc)};
bind_order([#sort_item{expr = E, dir = Dir, nulls = Nulls} | T], Columns, Acc) ->
    case bind_expr(E, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, Bound} -> bind_order(T, Columns, [{Bound, Dir, nulls_for(Dir, Nulls)} | Acc])
    end.

nulls_for(asc, default) -> nulls_last;
nulls_for(desc, default) -> nulls_first;
nulls_for(_Dir, Explicit) -> Explicit.

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
        true -> typed_row(Table, Columns,
                          lists:zip([C#column.name || C <- Columns], Values))
    end;
bind_insert(Table, Columns, ColStrs, Values) ->
    case length(ColStrs) =:= length(Values) of
        false ->
            {error, column_count_mismatch};
        true ->
            case resolve_columns(ColStrs, scope_of(Table, Columns)) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Named} ->
                    case duplicate_names([C#sc.name || C <- Named]) of
                        [] ->
                            Pairs = lists:zip([C#sc.name || C <- Named], Values),
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

bind_update(Table, Columns, Scope, Set, Where) ->
    case bind_where(Where, Scope) of
        {error, Reason} ->
            {error, Reason};
        {ok, Pred} ->
            case bind_assignments(Set, Columns, Scope, []) of
                {error, Reason} -> {error, Reason};
                {ok, Assigns} -> {ok, {update, Table, Assigns, Pred}}
            end
    end.

%% 代入は {カラム位置, 新しい値} に落とす。
bind_assignments([], _Columns, _Scope, Acc) ->
    {ok, lists:reverse(Acc)};
bind_assignments([{NameStr, Expr} | T], Columns, Scope, Acc) ->
    case resolve_column(NameStr, Scope) of
        {error, Reason} ->
            {error, Reason};
        {ok, #sc{name = Name, type = Type, pos = Pos}} ->
            case bind_expr(Expr, Scope) of
                {error, Reason} ->
                    {error, Reason};
                %% 定数ならこの場で型を検査できる
                {ok, {const, V}} ->
                    case sql_type:check(Type, V) of
                        ok -> bind_assignments(T, Columns, Scope, [{Pos, {const, V}} | Acc]);
                        {error, Reason} -> {error, {Reason, Name, Type, V}}
                    end;
                %% 式は行ごとに値が決まるので、検査は実行時
                {ok, Bound} ->
                    bind_assignments(T, Columns, Scope, [{Pos, Bound} | Acc])
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
bind_expr(#col_ref{table = Q, name = NameStr}, Columns) ->
    case resolve_column(Q, NameStr, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, #sc{pos = Pos}} -> {ok, {ref, Pos}}
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

%% 修飾なしの名前は、スコープ全体で1つに定まる必要がある。
%% 複数のテーブルに同じ名前があれば ambiguous_column。
resolve_column(NameStr, Scope) ->
    resolve_column(undefined, NameStr, Scope).

resolve_column(Qualifier, NameStr, Scope) ->
    case to_existing_atom(NameStr) of
        error ->
            {error, {column_not_found, NameStr}};
        {ok, Name} ->
            Matches = [S || #sc{alias = A, name = N} = S <- Scope,
                            N =:= Name,
                            Qualifier =:= undefined orelse matches_alias(Qualifier, A)],
            case Matches of
                [C] -> {ok, C};
                [] -> {error, {column_not_found, qualified(Qualifier, NameStr)}};
                _ -> {error, {ambiguous_column, NameStr}}
            end
    end.

matches_alias(Qualifier, Alias) ->
    case to_existing_atom(Qualifier) of
        {ok, A} -> A =:= Alias;
        error -> false
    end.

qualified(undefined, Name) -> Name;
qualified(Q, Name) -> Q ++ "." ++ Name.

resolve_columns(NameStrs, Columns) ->
    resolve_columns(NameStrs, Columns, []).

resolve_columns([], _Columns, Acc) ->
    {ok, lists:reverse(Acc)};
resolve_columns([NameStr | T], Columns, Acc) ->
    case resolve_column(NameStr, Columns) of
        {error, Reason} -> {error, Reason};
        {ok, C} -> resolve_columns(T, Columns, [C | Acc])
    end.

names(Scope) ->
    [S#sc.name || S <- Scope].

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
