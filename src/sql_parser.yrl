%%%-------------------------------------------------------------------
%%% SQLの構文解析器(yecc)。
%%%
%%% リテラルのトークンを int_lit / float_lit / string_lit としているのは、
%%% 型名のキーワード INTEGER / FLOAT と終端記号が衝突するため。
%%% Erlangでは 'integer' と integer が同じアトムなので、素直に名付けると
%%% yeccが duplicate terminal を出し、文法が壊れる。
%%%
%%% 対応する文:
%%%   CREATE TABLE t (c TYPE, ...)  / DROP TABLE t
%%%   INSERT INTO t [(c, ...)] VALUES (v, ...)
%%%   UPDATE t SET c = v, ... [WHERE c = v]
%%%   DELETE FROM t [WHERE c = v]
%%%   SELECT * | c, ... FROM t [WHERE c = v]
%%%   BEGIN / COMMIT / ROLLBACK
%%%
%%% WHEREの式はまだ `カラム = リテラル` だけ。比較演算子・AND/OR・
%%% 括弧を入れるときは、真偽式を階層(or_expr -> and_expr -> not_expr ->
%%% predicate)で書くこと。優先順位宣言で潰そうとすると、NOTと比較演算子、
%%% 単項マイナスの扱いで衝突が残りやすい。
%%%
%%% Expect 0. を宣言してあるので、衝突が出れば警告として現れる。
%%% yeccは衝突しても動くものを吐くため、放置すると黙って誤った結合になる。
%%%-------------------------------------------------------------------

Nonterminals
    stmt
    select_stmt create_stmt drop_stmt insert_stmt update_stmt delete_stmt tx_stmt
    select_list select_item table_ref opt_where expr literal signed_literal
    column_defs column_def type_name
    opt_column_names column_names
    value_list
    assignments assignment.

Terminals
    int_lit float_lit string_lit identifier
    select from where
    create table drop
    insert into values
    update set delete
    'begin' commit rollback
    'integer' 'float' 'varchar' 'boolean'
    true false null
    ',' '*' '=' '(' ')' ';' '-'.

Rootsymbol stmt.

Expect 0.

%%%===================================================================
%%% 文
%%%===================================================================

stmt -> select_stmt     : '$1'.
stmt -> create_stmt     : '$1'.
stmt -> drop_stmt       : '$1'.
stmt -> insert_stmt     : '$1'.
stmt -> update_stmt     : '$1'.
stmt -> delete_stmt     : '$1'.
stmt -> tx_stmt         : '$1'.
stmt -> select_stmt ';' : '$1'.
stmt -> create_stmt ';' : '$1'.
stmt -> drop_stmt ';'   : '$1'.
stmt -> insert_stmt ';' : '$1'.
stmt -> update_stmt ';' : '$1'.
stmt -> delete_stmt ';' : '$1'.
stmt -> tx_stmt ';'     : '$1'.

%%%===================================================================
%%% トランザクション制御
%%%===================================================================

tx_stmt -> 'begin'  : #tx_stmt{op = 'begin'}.
tx_stmt -> commit   : #tx_stmt{op = commit}.
tx_stmt -> rollback : #tx_stmt{op = rollback}.

%%%===================================================================
%%% DDL
%%%===================================================================

create_stmt -> create table identifier '(' column_defs ')' :
    #create_table_stmt{table = value_of('$3'), columns = '$5'}.

column_defs -> column_def                  : ['$1'].
column_defs -> column_def ',' column_defs  : ['$1' | '$3'].

column_def -> identifier type_name : {value_of('$1'), '$2'}.

type_name -> 'integer' : integer.
type_name -> 'float'   : float.
type_name -> 'varchar' : varchar.
type_name -> 'boolean' : boolean.

drop_stmt -> drop table identifier : #drop_table_stmt{table = value_of('$3')}.

%%%===================================================================
%%% INSERT
%%%===================================================================

insert_stmt -> insert into identifier opt_column_names values '(' value_list ')' :
    #insert_stmt{table = value_of('$3'), columns = '$4', values = '$7'}.

opt_column_names -> '$empty'                : undefined.
opt_column_names -> '(' column_names ')'    : '$2'.

column_names -> identifier                  : [value_of('$1')].
column_names -> identifier ',' column_names : [value_of('$1') | '$3'].

value_list -> signed_literal                : ['$1'].
value_list -> signed_literal ',' value_list : ['$1' | '$3'].

%%%===================================================================
%%% UPDATE / DELETE
%%%===================================================================

update_stmt -> update identifier set assignments opt_where :
    #update_stmt{table = value_of('$2'), set = '$4', where = '$5'}.

assignments -> assignment                 : ['$1'].
assignments -> assignment ',' assignments : ['$1' | '$3'].

assignment -> identifier '=' signed_literal : {value_of('$1'), '$3'}.

delete_stmt -> delete from identifier opt_where :
    #delete_stmt{table = value_of('$3'), where = '$4'}.

%%%===================================================================
%%% SELECT
%%%===================================================================

select_stmt -> select select_list from table_ref opt_where :
    #select_stmt{columns = '$2', from = '$4', where = '$5'}.

select_list -> '*'                         : [#star{}].
select_list -> select_item                 : ['$1'].
select_list -> select_item ',' select_list : ['$1' | '$3'].

select_item -> identifier : #col_ref{name = value_of('$1')}.

table_ref -> identifier : #table_ref{name = value_of('$1')}.

%%%===================================================================
%%% WHERE
%%%===================================================================

opt_where -> '$empty'    : undefined.
opt_where -> where expr  : '$2'.

expr -> identifier '=' signed_literal :
    #binop{op = '=', left = #col_ref{name = value_of('$1')}, right = '$3'}.

%%%===================================================================
%%% リテラル
%%%===================================================================

signed_literal -> literal     : '$1'.
signed_literal -> '-' literal : negate('$2').

literal -> int_lit    : #const{value = value_of('$1')}.
literal -> float_lit  : #const{value = value_of('$1')}.
literal -> string_lit : #const{value = list_to_binary(value_of('$1'))}.
literal -> true    : #const{value = true}.
literal -> false   : #const{value = false}.
literal -> null    : #const{value = null}.

Erlang code.

-include("../include/sql.hrl").

%% leexのトークンは {Type, Line, Value} または {Type, Line}
value_of({_Type, _Line, Value}) -> Value.

negate(#const{value = V}) when is_number(V) -> #const{value = -V};
negate(#const{} = C) -> C.
