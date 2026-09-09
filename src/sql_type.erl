%%%-------------------------------------------------------------------
%%% @doc
%%% SQLの型検査。
%%%
%%% 型はカタログだけが持ち、行の格納形式は変えない。ここでやるのは
%%% 「宣言された型に対して、この値を入れてよいか」の判定だけ。
%%%
%%% 暗黙変換はしない。varchar のカラムに数値を入れることも、
%%% integer のカラムに文字列を入れることも通さない。SQL標準にも無く、
%%% バグの温床になるため。integer -> float の格上げだけは許す。
%%%
%%% NULL はどの型にも入る(NOT NULL 制約はまだ無い)。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_type).

-export([check/2, name/1, of_value/1]).

-include("../include/catalog.hrl").

%%----------------------------------------------------------------------
%% @doc 宣言された型に値を入れてよいかを判定する。
%% Returns: ok | {error, type_mismatch}
%%----------------------------------------------------------------------
-spec check(sql_type(), term()) -> ok | {error, type_mismatch}.
%% NULLはどの型にも入る
check(_Type, null) -> ok;
%% any は型宣言のない古いテーブル。何でも入る
check(any, _V) -> ok;
check(integer, V) when is_integer(V) -> ok;
%% 整数はfloatのカラムに入れてよい(格上げ)。逆は情報が落ちるので許さない
check(float, V) when is_float(V); is_integer(V) -> ok;
check(varchar, V) when is_binary(V) -> ok;
check(boolean, V) when is_boolean(V) -> ok;
check(_Type, _V) -> {error, type_mismatch}.

%%----------------------------------------------------------------------
%% @doc 型名の表示用文字列。
%%----------------------------------------------------------------------
name(integer) -> "INTEGER";
name(float) -> "FLOAT";
name(varchar) -> "VARCHAR";
name(boolean) -> "BOOLEAN";
name(any) -> "ANY".

%%----------------------------------------------------------------------
%% @doc 値から推測される型。エラーメッセージ用。
%%----------------------------------------------------------------------
of_value(null) -> null;
of_value(V) when is_integer(V) -> integer;
of_value(V) when is_float(V) -> float;
of_value(V) when is_binary(V) -> varchar;
of_value(V) when is_boolean(V) -> boolean;
of_value(_) -> any.
