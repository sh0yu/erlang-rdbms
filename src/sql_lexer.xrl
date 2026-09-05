%%%-------------------------------------------------------------------
%%% SQLの字句解析器(leex)。
%%%
%%% 識別子は**文字列のまま**トークンにする。ここでlist_to_atom/1すると、
%%% 任意の入力でアトム表を際限なく増やせてしまう。テーブル名・カラム名への
%%% 解決はカタログと突き合わせるsql_analyzerの仕事とする。
%%%
%%% キーワードは大文字小文字を区別しない(SELECT / select / Select)。
%%%-------------------------------------------------------------------

Definitions.

D    = [0-9]
L    = [A-Za-z_]
A    = [A-Za-z_0-9]
WS   = [\s\t\r\n]

Rules.

{D}+\.{D}+          : {token, {float_lit, TokenLine, list_to_float(TokenChars)}}.
{D}+                : {token, {int_lit, TokenLine, list_to_integer(TokenChars)}}.
'([^']|'')*'        : {token, {string_lit, TokenLine, unquote(TokenChars)}}.
{L}{A}*             : {token, keyword_or_identifier(TokenChars, TokenLine)}.
,                   : {token, {',', TokenLine}}.
\*                  : {token, {'*', TokenLine}}.
<>                  : {token, {'<>', TokenLine}}.
!=                  : {token, {'<>', TokenLine}}.
<=                  : {token, {'<=', TokenLine}}.
>=                  : {token, {'>=', TokenLine}}.
<                   : {token, {'<', TokenLine}}.
>                   : {token, {'>', TokenLine}}.
=                   : {token, {'=', TokenLine}}.
\+                  : {token, {'+', TokenLine}}.
/                   : {token, {'/', TokenLine}}.
\(                  : {token, {'(', TokenLine}}.
\)                  : {token, {')', TokenLine}}.
\.                  : {token, {'.', TokenLine}}.
\-                  : {token, {'-', TokenLine}}.
;                   : {token, {';', TokenLine}}.
{WS}+               : skip_token.

Erlang code.

-export([keywords/0]).

%% 予約語。ここに無い語はすべて識別子になる。
%% 段階的に文法を広げる際は、対応する終端記号をsql_parser.yrlにも足すこと。
%% 文字列で持つのが要点。アトムで比較すると、比較のために識別子を
%% list_to_atom/1することになり、この字句解析器で避けたいことと矛盾する。
keywords() ->
    ["select", "from", "where",
     "create", "table", "drop",
     "insert", "into", "values",
     "update", "set", "delete",
     "begin", "commit", "rollback",
     "integer", "float", "varchar", "boolean",
     "true", "false", "null",
     "and", "or", "not", "is",
     "order", "by", "asc", "desc", "limit", "offset", "distinct",
     "nulls", "first", "last",
     "group", "having"].

keyword_or_identifier(Chars, Line) ->
    Lower = string:lowercase(Chars),
    case lists:member(Lower, keywords()) of
        %% 予約語は有限集合なのでアトム化してよい
        true  -> {list_to_atom(Lower), Line};
        false -> {identifier, Line, Lower}
    end.

%% 'it''s' -> "it's"
unquote([$' | Rest]) ->
    unquote_1(lists:droplast(Rest)).

unquote_1([]) ->
    [];
unquote_1([$', $' | T]) ->
    [$' | unquote_1(T)];
unquote_1([H | T]) ->
    [H | unquote_1(T)].
