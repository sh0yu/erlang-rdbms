-module(client_query_tx_perf).
-compile(export_all).

exec() ->
    Count = 10000,
    %% start supervisor of transaction_db including query_exec_sup.
    sup:start_link(),
    Start = erlang:system_time(millisecond),
    {ok, Pid} = gen_connection:connect(),
    query_exec:exec_query(Pid, {create_table, fruit, [name, price]}),
    query_exec:exec_query(Pid, {begin_tx}),
    query_exec:exec_query(Pid, {insert, fruit, [apple, 0]}),
    query_exec:exec_query(Pid, {commit_tx}),
    ok = loop(fun() -> child() end, Count),
    timer:sleep(1000),
    query_exec:exec_query(Pid, {begin_tx}),
    [[apple, Price]] = query_exec:exec_query(Pid, {select, fruit, name, apple}),
    io:format("Final Price: ~p~n", [Price]),
    query_exec:exec_query(Pid, {commit_tx}),
    End = erlang:system_time(millisecond),
    io:format("Time: ~pms.~n", [End - Start]),
    query_exec:exec_query(Pid, {drop_table, fruit}).

child() ->
    %% start query_exec process and get its Pid.
    {ok, Pid} = gen_connection:connect(),

    query_exec:exec_query(Pid, {begin_tx}),
    [[apple, Price]] = query_exec:exec_query(Pid, {select, fruit, name, apple}),
    query_exec:exec_query(Pid, {update, fruit, [{price, Price + 1}], price, Price}),
    query_exec:exec_query(Pid, {commit_tx}).

loop(_Fun, 0) ->
    ok;
loop(Fun, Count) ->
    spawn_link(Fun),
    loop(Fun, Count - 1).