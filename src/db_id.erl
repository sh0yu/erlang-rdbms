%%%-------------------------------------------------------------------
%%% @doc
%%% オブジェクトID・トランザクションID・クエリIDの採番。
%%%
%%% 単に erlang:system_time(nanosecond) を使うと、同じナノ秒に採番した
%%% 別プロセス同士でIDが衝突しうる。衝突するとDETS上の行を互いに
%%% 上書きしてしまうため、時刻に加えてノード内で一意な整数を組にする。
%%%
%%% 時刻を先頭に置いているので、IDの大小比較は採番順とおおむね一致する
%%% (トランザクションの新旧判定はこの順序を使う)。
%%% @end
%%%-------------------------------------------------------------------
-module(db_id).

-export([new/0, timestamp/1]).

-type id() :: {integer(), integer()}.
-export_type([id/0]).

-spec new() -> id().
new() ->
    {erlang:system_time(nanosecond), erlang:unique_integer([monotonic, positive])}.

-spec timestamp(id()) -> integer().
timestamp({Timestamp, _Unique}) ->
    Timestamp.
