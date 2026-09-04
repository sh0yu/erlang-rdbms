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
    columns,            % [#col_ref{} | #star{}]
    from,               % #table_ref{}
    where               % 式 | undefined
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
    op                  % 'begin' | commit | rollback
}).

%%%===================================================================
%%% FROM句
%%%===================================================================

-record(table_ref, {
    name,               % string() -> アナライザ後は atom()
    alias = undefined   % string() | undefined
}).

%%%===================================================================
%%% 式
%%%===================================================================

%% SELECT * の *
-record(star, {}).

%% カラム参照。
%% slot はアナライザが埋める。実行時は名前ではなくこの位置で行を引く。
-record(col_ref, {
    table = undefined,  % 修飾子 t.c の t。無ければ undefined
    name,               % string() -> アナライザ後は atom()
    slot = undefined    % 非負整数。アナライザが解決する
}).

%% 定数。SQL NULL はアトム null で表す。
%% 文字列はbinaryにするのでアトム null と衝突しない。
-record(const, {
    value
}).

%% 二項演算。op は '=' など。
-record(binop, {
    op,
    left,
    right
}).
