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
    explain_stmt create_index_stmt drop_index_stmt analyze_stmt
    select_list select_item table_ref opt_where expr literal
    neg opt_distinct opt_order sort_list sort_item opt_dir opt_nulls opt_limit
    opt_group opt_having expr_list func_call
    from_item join_kw opt_alias
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
    'and' 'or' 'not' 'is'
    'order' 'by' 'asc' 'desc' 'limit' 'offset' 'distinct'
    'nulls' 'first' 'last'
    'group' 'having' 'explain' 'index' 'analyze'
    'join' 'inner' 'left' 'outer' 'cross' 'on' 'as' '.'
    ',' '*' '(' ')' ';'
    '=' '<>' '<' '<=' '>' '>=' '+' '-' '/'.

Rootsymbol stmt.

Expect 0.

%%%===================================================================
%%% 演算子の優先順位
%%%
%%% 真偽式と値式を1つの expr にまとめ、優先順位宣言で解決している。
%%% 階層(or_expr -> and_expr -> ...)で書く手もあるが、括弧が
%%% 真偽の括弧か値の括弧かで曖昧になる。1つにまとめれば `(` の扱いが
%%% 1箇所で済む(PostgreSQLの a_expr と同じ考え方)。
%%%
%%% yeccには規則ごとの %prec が無く、規則の優先順位は
%%% 「規則中の最後の終端記号」から決まる。単項マイナスは
%%% 非終端記号 neg に優先順位を宣言して解決している。
%%%===================================================================

Left  100 'or'.
Left  200 'and'.
Unary 300 'not'.
Nonassoc 400 'is'.
Nonassoc 500 '=' '<>' '<' '<=' '>' '>='.
Left  600 '+' '-'.
Left  700 '*' '/'.
Unary 800 neg.

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
stmt -> explain_stmt    : '$1'.
stmt -> create_index_stmt : '$1'.
stmt -> drop_index_stmt   : '$1'.
stmt -> analyze_stmt      : '$1'.
stmt -> select_stmt ';' : '$1'.
stmt -> create_stmt ';' : '$1'.
stmt -> drop_stmt ';'   : '$1'.
stmt -> insert_stmt ';' : '$1'.
stmt -> update_stmt ';' : '$1'.
stmt -> delete_stmt ';' : '$1'.
stmt -> tx_stmt ';'     : '$1'.
stmt -> explain_stmt ';' : '$1'.
stmt -> create_index_stmt ';' : '$1'.
stmt -> drop_index_stmt ';'   : '$1'.
stmt -> analyze_stmt ';'      : '$1'.

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

value_list -> expr                : ['$1'].
value_list -> expr ',' value_list : ['$1' | '$3'].

%%%===================================================================
%%% UPDATE / DELETE
%%%===================================================================

update_stmt -> update identifier set assignments opt_where :
    #update_stmt{table = value_of('$2'), set = '$4', where = '$5'}.

assignments -> assignment                 : ['$1'].
assignments -> assignment ',' assignments : ['$1' | '$3'].

assignment -> identifier '=' expr : {value_of('$1'), '$3'}.

delete_stmt -> delete from identifier opt_where :
    #delete_stmt{table = value_of('$3'), where = '$4'}.

%%%===================================================================
%%% SELECT
%%%===================================================================

%% EXPLAIN は SELECT にしか意味が無いが、文法では他の文も受ける。
%% ここで弾くと「構文エラー」になり、なぜ駄目なのかが伝わらない。
%% 受けておいて sql_analyzer が explain_requires_select を返す。
analyze_stmt -> 'analyze' : #analyze_stmt{}.
analyze_stmt -> 'analyze' identifier : #analyze_stmt{table = value_of('$2')}.

create_index_stmt -> create 'index' identifier on identifier '(' identifier ')' :
    #create_index_stmt{name = value_of('$3'), table = value_of('$5'), column = value_of('$7')}.

drop_index_stmt -> drop 'index' identifier :
    #drop_index_stmt{name = value_of('$3')}.

explain_stmt -> 'explain' select_stmt : #explain_stmt{stmt = '$2'}.
explain_stmt -> 'explain' insert_stmt : #explain_stmt{stmt = '$2'}.
explain_stmt -> 'explain' update_stmt : #explain_stmt{stmt = '$2'}.
explain_stmt -> 'explain' delete_stmt : #explain_stmt{stmt = '$2'}.
explain_stmt -> 'explain' create_stmt : #explain_stmt{stmt = '$2'}.
explain_stmt -> 'explain' drop_stmt   : #explain_stmt{stmt = '$2'}.

select_stmt -> select opt_distinct select_list from from_item opt_where
               opt_group opt_having opt_order opt_limit :
    {Limit, Offset} = '$10',
    #select_stmt{distinct = '$2', columns = '$3', from = '$5', where = '$6',
                 group_by = '$7', having = '$8', order_by = '$9',
                 limit = Limit, offset = Offset}.

opt_group -> '$empty'                : [].
opt_group -> 'group' 'by' expr_list  : '$3'.

opt_having -> '$empty'      : undefined.
opt_having -> 'having' expr : '$2'.

expr_list -> expr               : ['$1'].
expr_list -> expr ',' expr_list : ['$1' | '$3'].

opt_distinct -> '$empty'   : false.
opt_distinct -> 'distinct' : true.

opt_order -> '$empty'            : [].
opt_order -> 'order' 'by' sort_list : '$3'.

sort_list -> sort_item                : ['$1'].
sort_list -> sort_item ',' sort_list  : ['$1' | '$3'].

sort_item -> expr opt_dir opt_nulls :
    #sort_item{expr = '$1', dir = '$2', nulls = '$3'}.

opt_dir -> '$empty' : asc.
opt_dir -> 'asc'    : asc.
opt_dir -> 'desc'   : desc.

opt_nulls -> '$empty'          : default.
opt_nulls -> 'nulls' 'first'   : nulls_first.
opt_nulls -> 'nulls' 'last'    : nulls_last.

opt_limit -> '$empty'                            : {undefined, undefined}.
opt_limit -> 'limit' int_lit                     : {value_of('$2'), undefined}.
opt_limit -> 'limit' int_lit 'offset' int_lit    : {value_of('$2'), value_of('$4')}.
opt_limit -> 'offset' int_lit                    : {undefined, value_of('$2')}.

select_list -> '*'                         : [#star{}].
select_list -> select_item                 : ['$1'].
select_list -> select_item ',' select_list : ['$1' | '$3'].

select_item -> expr : '$1'.

%%%===================================================================
%%% FROM句
%%%===================================================================

from_item -> table_ref : '$1'.
%% カンマ区切りは直積(CROSS JOIN と同じ)
from_item -> from_item ',' table_ref :
    #join{type = cross, left = '$1', right = '$3'}.
from_item -> from_item join_kw table_ref 'on' expr :
    #join{type = '$2', left = '$1', right = '$3', on = '$5'}.
from_item -> from_item 'cross' 'join' table_ref :
    #join{type = cross, left = '$1', right = '$4'}.

join_kw -> 'join'                   : inner.
join_kw -> 'inner' 'join'           : inner.
join_kw -> 'left' 'join'            : left.
join_kw -> 'left' 'outer' 'join'    : left.

table_ref -> identifier opt_alias :
    #table_ref{name = value_of('$1'), alias = '$2'}.

opt_alias -> '$empty'         : undefined.
opt_alias -> identifier       : value_of('$1').
opt_alias -> 'as' identifier  : value_of('$2').

%%%===================================================================
%%% WHERE
%%%===================================================================

opt_where -> '$empty'    : undefined.
opt_where -> where expr  : '$2'.

%%%===================================================================
%%% 式
%%%===================================================================

expr -> expr 'or' expr   : #binop{op = 'or',  left = '$1', right = '$3'}.
expr -> expr 'and' expr  : #binop{op = 'and', left = '$1', right = '$3'}.
expr -> 'not' expr       : #unop{op = 'not', arg = '$2'}.

%% 比較演算子は規則に直接書く。comp_op のような非終端記号にまとめると、
%% 規則中に終端記号が無くなり優先順位が効かなくなる
%% (yeccは規則の優先順位を「規則中の最後の終端記号」から決めるため)。
%% まとめた場合 `a >= 1 AND a < 3` が `a >= (1 AND (a < 3))` と解釈される。
expr -> expr '='  expr : #binop{op = '=',  left = '$1', right = '$3'}.
expr -> expr '<>' expr : #binop{op = '<>', left = '$1', right = '$3'}.
expr -> expr '<'  expr : #binop{op = '<',  left = '$1', right = '$3'}.
expr -> expr '<=' expr : #binop{op = '<=', left = '$1', right = '$3'}.
expr -> expr '>'  expr : #binop{op = '>',  left = '$1', right = '$3'}.
expr -> expr '>=' expr : #binop{op = '>=', left = '$1', right = '$3'}.

expr -> expr 'is' null       : #is_null{arg = '$1', negated = false}.
expr -> expr 'is' 'not' null : #is_null{arg = '$1', negated = true}.

expr -> expr '+' expr : #binop{op = '+', left = '$1', right = '$3'}.
expr -> expr '-' expr : #binop{op = '-', left = '$1', right = '$3'}.
expr -> expr '*' expr : #binop{op = '*', left = '$1', right = '$3'}.
expr -> expr '/' expr : #binop{op = '/', left = '$1', right = '$3'}.

expr -> func_call     : '$1'.
expr -> neg expr      : #unop{op = '-', arg = '$2'}.

%% 関数名は予約語にしていない。未知の関数は意味解析で弾く。
%% 予約語を減らすほど、count や order という名前のカラムが作れる。
func_call -> identifier '(' '*' ')' :
    #func{name = value_of('$1'), args = star}.
func_call -> identifier '(' expr ')' :
    #func{name = value_of('$1'), args = ['$3']}.
func_call -> identifier '(' 'distinct' expr ')' :
    #func{name = value_of('$1'), args = ['$4'], distinct = true}.
expr -> '(' expr ')'  : '$2'.
expr -> identifier                  : #col_ref{name = value_of('$1')}.
expr -> identifier '.' identifier   : #col_ref{table = value_of('$1'),
                                               name = value_of('$3')}.
expr -> literal       : '$1'.

neg -> '-' : '-'.

%%%===================================================================
%%% リテラル
%%%===================================================================

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


