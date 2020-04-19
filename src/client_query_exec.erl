-module(client_query_exec).
-compile(export_all).

exec() ->
    %% start supervisor of transaction_db including query_exec_sup.
    sup:start_link(),

    %% start query_exec process and get its Pid.
    {ok, Pid} = gen_connection:connect(),

    Price1 = 999,
    Price2 = 100,
    io:format("ClientPid: ~p, QueryExecutorPid: ~p~n", [self(), Pid]),
    query_exec:exec_query(Pid, {create_table, fruit, [name, price]}),

    %% Tx1
    Txid = query_exec:exec_query(Pid, {begin_tx}),
    query_exec:exec_query(Pid, {insert, fruit, [apple, Price1]}),
    query_exec:exec_query(Pid, {insert, fruit, [apple, Price2]}),
    io:format("[Sel1]ClientPid: ~p, SelectData: ~p~n", [self(), query_exec:exec_query(Pid, {select, fruit, name, apple})]),
    query_exec:exec_query(Pid, {update, fruit, [{price, 150}], price, Price1}),
    query_exec:exec_query(Pid, {update, fruit, [{name, banana}], price, 150}),
    io:format("[Sel2]ClientPid: ~p, SelectData: ~p~n", [self(), query_exec:exec_query(Pid, {select, fruit, name, banana})]),
    query_exec:exec_query(Pid, {delete, fruit, name, banana}),
    io:format("Client Txid: ~p~n", [Txid]),
    io:format("[Sel3]ClientPid: ~p, SelectData: ~p~n", [self(), query_exec:exec_query(Pid, {select, fruit, name, banana})]),
    query_exec:exec_query(Pid, {commit_tx}),

    %% Tx2
    query_exec:exec_query(Pid, {begin_tx}),
    io:format("[Sel4]ClientPid: ~p, SelectData: ~p~n", [self(), query_exec:exec_query(Pid, {select, fruit, name, banana})]),
    io:format("[Sel5]ClientPid: ~p, SelectData: ~p~n", [self(), query_exec:exec_query(Pid, {select, fruit, name, apple})]),
    query_exec:exec_query(Pid, {commit_tx}),

    query_exec:exec_query(Pid, {drop_table, fruit}).
