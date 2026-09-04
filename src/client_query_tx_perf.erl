%%%-------------------------------------------------------------------
%%% @doc
%%% 複数のクライアントが同じ行を同時に加算する例。
%%%
%%% トランザクションは直列に実行されるので、Count個の接続が
%%% それぞれ +1 すれば、最終値はちょうどCountになる。
%%% ここが合わなければ更新の取りこぼし(lost update)が起きている。
%%%
%%%   client_query_tx_perf:exec().
%%%   client_query_tx_perf:exec(500).
%%% @end
%%%-------------------------------------------------------------------
-module(client_query_tx_perf).

-export([exec/0, exec/1]).

-define(DEFAULT_COUNT, 200).

exec() ->
    exec(?DEFAULT_COUNT).

exec(Count) ->
    {ok, _} = sup:start_link(),
    Start = erlang:monotonic_time(millisecond),

    {ok, Pid} = gen_connection:connect(),
    ok = query_exec:exec_query(Pid, {create_table, counter, [name, value]}),
    _ = query_exec:exec_query(Pid, {begin_tx}),
    {ok, _} = query_exec:exec_query(Pid, {insert, counter, [total, 0]}),
    ok = query_exec:exec_query(Pid, {commit_tx}),

    Self = self(),
    [spawn_link(fun() -> Self ! {done, increment()} end) || _ <- lists:seq(1, Count)],
    ok = wait_for(Count),

    _ = query_exec:exec_query(Pid, {begin_tx}),
    [[total, Value]] = query_exec:exec_query(Pid, {select, counter, name, total}),
    ok = query_exec:exec_query(Pid, {commit_tx}),
    End = erlang:monotonic_time(millisecond),

    io:format("Final value: ~p (expected ~p)~n", [Value, Count]),
    io:format("Time: ~pms~n", [End - Start]),
    ok = query_exec:exec_query(Pid, {drop_table, counter}),
    Value = Count,
    ok.

%% 1接続で「読んで+1して書く」を1トランザクションで行う
increment() ->
    {ok, Pid} = gen_connection:connect(),
    _ = query_exec:exec_query(Pid, {begin_tx}),
    [[total, Value]] = query_exec:exec_query(Pid, {select, counter, name, total}),
    {ok, 1} = query_exec:exec_query(Pid, {update, counter, [{value, Value + 1}], name, total}),
    ok = query_exec:exec_query(Pid, {commit_tx}),
    ok = gen_connection:disconnect(Pid),
    ok.

wait_for(0) ->
    ok;
wait_for(N) ->
    receive {done, ok} -> wait_for(N - 1)
    after 60000 -> {error, {timeout, N}}
    end.
