%%%-------------------------------------------------------------------
%%% SQLの抽象構文木(AST)。
%%%
%%% パーサが組み立て、sql_analyzerがカタログと突き合わせて解決する。
%%% この段階では識別子は**文字列**で入っている。アナライザが
%%% カタログに存在するテーブル名・カラム名へ解決した時点でアトムになる。
%%%-------------------------------------------------------------------

%%%===================================================================
%%% 文
%%%===================================================================

-record(select_stmt, {
    distinct = false :: boolean(),
    columns,            % [式 | #star{}]
    from,               % #table_ref{}
    where,              % 式 | undefined
    group_by = [],      % [式]
    having = undefined, % 式 | undefined
    order_by = [],      % [#sort_item{}]
    limit = undefined,  % 非負整数 | undefined
    offset = undefined  % 非負整数 | undefined
}).

%% ORDER BY の1項目。
%% nulls の既定はASCならlast、DESCならfirst(NULLを最大値として扱う)。
-record(sort_item, {
    expr,
    dir = asc :: asc | desc,
    nulls = default :: default | nulls_first | nulls_last
}).

-record(create_table_stmt, {
    table,              % string() -> アナライザ後は atom()
    columns             % [{string(), sql_type()}]
}).

-record(drop_table_stmt, {
    table
}).

-record(insert_stmt, {
    table,
    columns = undefined, % [string()] | undefined(全カラム)
    values               % [式]
}).

-record(update_stmt, {
    table,
    set,                % [{string(), 式}]
    where
}).

-record(delete_stmt, {
    table,
    where
}).

%% BEGIN / COMMIT / ROLLBACK
-record(tx_stmt, {
    op                  % 'begin' | begin_read_only | begin_read_committed
                        % | commit | rollback
}).

%%%===================================================================
%%% FROM句
%%%===================================================================

%% 集合演算。UNION / INTERSECT / EXCEPT。
%%
%% ORDER BY / LIMIT は演算全体に掛かるので、ここが持つ。
%% 被演算子(#select_stmt{})の側は持たない。
%%   SELECT a FROM t UNION SELECT b FROM u ORDER BY 1
%% の ORDER BY は右の SELECT ではなく和集合に掛かる。
-record(set_op_stmt, {
    op,                 % 'union' | intersect | except
    all = false,        % ALL なら重複を残す
    left,
    right,
    order_by = [],
    limit,
    offset
}).

%% ANALYZE [table]。table が undefined なら全テーブル。
-record(analyze_stmt, {
    table
}).

%% CREATE INDEX name ON table (column)
-record(create_index_stmt, {
    name,
    table,
    column
}).

%% DROP INDEX name
-record(drop_index_stmt, {
    name
}).

%% EXPLAIN <SELECT文>。実行せずに、選ばれた実行計画を返す。
-record(explain_stmt, {
    stmt
}).

%% 導出表。FROM (SELECT ...) AS t
%% 別名は必須。列を修飾するのに要る。
-record(derived_table, {
    query,
    alias
}).

-record(table_ref, {
    name,               % string() -> アナライザ後は atom()
    alias = undefined   % string() | undefined
}).

%% FROM句の結合。
%% type は inner | left | cross。cross は ON を持たない。
-record(join, {
    type,
    left,
    right,
    on = undefined
}).

%%%===================================================================
%%% 式
%%%===================================================================

%% SELECT * の *
-record(star, {}).

%% CASE WHEN cond THEN val ... [ELSE val] END
-record(case_expr, {
    whens = [],   % [{Cond, Val}]
    else_          % 省略時は undefined(結果は NULL)
}).

%% x LIKE 'pat'。`%` は任意の並び、`_` は任意の1文字。
-record(like_expr, {
    arg,
    pattern,
    negated = false
}).

%% 式の中の副問い合わせ。いずれも**相関しない**ものだけを扱う。
%% 外側の行を参照する副問い合わせは、外側の1行ごとに実行し直す必要があり、
%% それは別の仕組みになる。

%% スカラー副問い合わせ。(SELECT ...) が1行1列を返すことを要求する。
-record(scalar_subquery, {query}).

%% EXISTS (SELECT ...)
-record(exists_expr, {query}).

%% expr IN (v, ...) または expr IN (SELECT ...)
-record(in_expr, {
    arg,
    values,       % [expr()] | undefined
    query         % 副問い合わせ | undefined
}).

%% 選択リストの項目に付けた別名。SELECT expr AS name
-record(aliased, {
    expr,
    name
}).

%% カラム参照。
%% slot はアナライザが埋める。実行時は名前ではなくこの位置で行を引く。
-record(col_ref, {
    table = undefined,  % 修飾子 t.c の t(string)。無ければ undefined
    name,               % string() -> アナライザ後は atom()
    slot = undefined    % 非負整数。アナライザが解決する
}).

%% 定数。SQL NULL はアトム null で表す。
%% 文字列はbinaryにするのでアトム null と衝突しない。
-record(const, {
    value
}).

%% 二項演算。op は比較('=' '<>' '<' '<=' '>' '>=')、
%% 論理('and' 'or')、算術('+' '-' '*' '/')。
-record(binop, {
    op,
    left,
    right
}).

%% 単項演算。op は 'not' または '-'。
-record(unop, {
    op,
    arg
}).

%% 関数呼び出し。いまは集約のみ。
%% 関数名を予約語にしていないので、未知の関数は意味解析で弾く。
-record(func, {
    name,               % string()
    args,               % [式] | star
    distinct = false
}).

%% IS NULL / IS NOT NULL
-record(is_null, {
    arg,
    negated = false
}).
