%%%-------------------------------------------------------------------
%%% SQLの構文解析器(yecc)。
%%%
%%% Stage 1 の範囲:
%%%   SELECT <select_list> FROM <table> [WHERE <col> = <literal>] [;]
%%%
%%% 式文法を広げる際は、shift/reduce衝突を警告のまま放置しないこと。
%%% yeccは衝突しても動くものを吐くため、黙って誤った結合になる。
%%%-------------------------------------------------------------------

Nonterminals
    stmt
    select_stmt
    select_list
    select_item
    table_ref
    opt_where
    expr
    literal.

Terminals
    integer string identifier
    select from where
    ',' '*' '=' ';'.

Rootsymbol stmt.

%%%===================================================================
%%% 文
%%%===================================================================

stmt -> select_stmt       : '$1'.
stmt -> select_stmt ';'   : '$1'.

select_stmt -> select select_list from table_ref opt_where :
    #select_stmt{columns = '$2', from = '$4', where = '$5'}.

%%%===================================================================
%%% SELECT句
%%%===================================================================

select_list -> '*'                      : [#star{}].
select_list -> select_item              : ['$1'].
select_list -> select_item ',' select_list : ['$1' | '$3'].

select_item -> identifier : #col_ref{name = value_of('$1')}.

%%%===================================================================
%%% FROM句
%%%===================================================================

table_ref -> identifier : #table_ref{name = value_of('$1')}.

%%%===================================================================
%%% WHERE句
%%%===================================================================

opt_where -> '$empty'    : undefined.
opt_where -> where expr  : '$2'.

expr -> identifier '=' literal :
    #binop{op = '=', left = #col_ref{name = value_of('$1')}, right = '$3'}.

literal -> integer : #const{value = value_of('$1')}.
literal -> string  : #const{value = list_to_binary(value_of('$1'))}.

Erlang code.

-include("../include/sql.hrl").

%% leexのトークンは {Type, Line, Value} または {Type, Line}
value_of({_Type, _Line, Value}) -> Value.
