%%%-------------------------------------------------------------------
%%% @doc
%%% SQL文字列の入口。字句解析と構文解析をまとめる。
%%% @end
%%%-------------------------------------------------------------------
-module(sql).

-export([parse/1]).

%%----------------------------------------------------------------------
%% @doc SQL文字列を構文木にする。
%% Returns: {ok, Ast} | {error, Reason}
%%----------------------------------------------------------------------
parse(Sql) when is_binary(Sql) ->
    parse(binary_to_list(Sql));
parse(Sql) when is_list(Sql) ->
    case sql_lexer:string(Sql) of
        {ok, Tokens, _EndLine} ->
            case sql_parser:parse(Tokens) of
                {ok, Ast} -> {ok, Ast};
                {error, {Line, _Mod, Message}} ->
                    {error, {syntax_error, Line, lists:flatten(Message)}}
            end;
        {error, {Line, _Mod, Reason}, _} ->
            {error, {lex_error, Line, Reason}}
    end.
