%%%-------------------------------------------------------------------
%%% @doc
%%% 1つのトランザクションでINSERT/SELECT/UPDATE/DELETEを一通り行い、
%%% コミット後に別のトランザクションから結果を確認するサンプル。
%%% @end
%%%-------------------------------------------------------------------
-module(client_query_exec).

-export([exec/0]).

exec() ->
    %% query_exec_supを含むtransaction_dbのサーバ群を起動する
    {ok, _} = sup:start_link(),

    %% query_execプロセスを起動して、そのPidを得る
    {ok, Pid} = gen_connection:connect(),

    Price1 = 999,
    Price2 = 100,
    io:format("ClientPid: ~p, QueryExecutorPid: ~p~n", [self(), Pid]),
    ok = query_exec:exec_query(Pid, {create_table, fruit, [name, price]}),

    %% Tx1
    Txid = query_exec:exec_query(Pid, {begin_tx}),
    {ok, _} = query_exec:exec_query(Pid, {insert, fruit, [apple, Price1]}),
    {ok, _} = query_exec:exec_query(Pid, {insert, fruit, [apple, Price2]}),
    io:format("[Sel1]SelectData: ~p~n", [query_exec:exec_query(Pid, {select, fruit, name, apple})]),
    {ok, 1} = query_exec:exec_query(Pid, {update, fruit, [{price, 150}], price, Price1}),
    {ok, 1} = query_exec:exec_query(Pid, {update, fruit, [{name, banana}], price, 150}),
    io:format("[Sel2]SelectData: ~p~n", [query_exec:exec_query(Pid, {select, fruit, name, banana})]),
    {ok, 1} = query_exec:exec_query(Pid, {delete, fruit, name, banana}),
    io:format("Client Txid: ~p~n", [Txid]),
    io:format("[Sel3]SelectData: ~p~n", [query_exec:exec_query(Pid, {select, fruit, name, banana})]),
    ok = query_exec:exec_query(Pid, {commit_tx}),

    %% Tx2: コミット後の状態を別のトランザクションから確認する
    _ = query_exec:exec_query(Pid, {begin_tx}),
    io:format("[Sel4]SelectData: ~p~n", [query_exec:exec_query(Pid, {select, fruit, name, banana})]),
    io:format("[Sel5]SelectData: ~p~n", [query_exec:exec_query(Pid, {select, fruit, name, apple})]),
    ok = query_exec:exec_query(Pid, {commit_tx}),

    ok = query_exec:exec_query(Pid, {drop_table, fruit}),
    ok.
