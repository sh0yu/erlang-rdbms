-module(client_tx).
-compile(export_all).

exec(Pid) ->
    Txid = tx_mng:begin_tx(Pid),
    io:format("Tx:Allowed? ~p~n", [tx_mng:allow_tx(Pid, Txid, self())]),
    % List = lists:seq(1, 100),
    % lists:map(fun(_) ->
    %     io:format("Allowed? ~p~n", [tx_mng:allow_tx(Pid, Txid1, self())]),
    %     io:format("Allowed? ~p~n", [tx_mng:allow_tx(Pid, Txid1, self())]),
    %     timer:sleep(1000)
    %     end,
    %     List),
    ok.
