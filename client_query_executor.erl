-module(client_query_executor).
-compile(export_all).

exec([SdsPid, TxMngPid]) ->
    {ok, Pid} = query_executor:start_link([SdsPid, TxMngPid]),
    Price1 = rand:uniform(3),
    io:format("ClientPid: ~p, QueryExecutorPid: ~p~n", [self(), Pid]),
    % query_executor:exec_query(Pid, {create_table, fruit, [name, price]}),
    Txid = query_executor:exec_query(Pid, {begin_tx}),
    query_executor:exec_query(Pid, {insert, fruit, [apple, Price1]}),
    query_executor:exec_query(Pid, {insert, fruit, [apple, rand:uniform(3)]}),
    io:format("[Sel1]ClientPid: ~p, SelectData: ~p~n", [self(), query_executor:exec_query(Pid, {select, fruit, name, apple})]),
    query_executor:exec_query(Pid, {update, fruit, [{price, 150}], price, Price1}),
    query_executor:exec_query(Pid, {update, fruit, [{name, banana}], price, 150}),
    io:format("[Sel2]ClientPid: ~p, SelectData: ~p~n", [self(), query_executor:exec_query(Pid, {select, fruit, name, banana})]),
    query_executor:exec_query(Pid, {delete, fruit, name, banana}),
    io:format("Client Txid: ~p~n", [Txid]),
    timer:sleep(1),
    io:format("[Sel3]ClientPid: ~p, SelectData: ~p~n", [self(), query_executor:exec_query(Pid, {select, fruit, name, banana})]),
    query_executor:exec_query(Pid, {commit_tx}).