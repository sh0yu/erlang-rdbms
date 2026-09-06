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
    op                  % 'begin' | begin_read_only | commit | rollback
}).

%%%===================================================================
%%% FROM句
%%%===================================================================

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
