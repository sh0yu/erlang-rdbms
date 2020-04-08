-module(client_1).
-compile(export_all).

exec() ->
    {ok, Pid} = simple_db_server:start_link(),
    simple_db_server:create_table(Pid, fruit, [name, price]),
    simple_db_server:insert_data(Pid, fruit, [apple, 100]),
    simple_db_server:insert_data(Pid, fruit, [banana, 150]),
    simple_db_server:insert_data(Pid, fruit, [orange, 150]),
    io:format("Step1:~p~n", [simple_db_server:select_data(Pid, fruit, name, apple)]),
    io:format("Step1:~p~n", [simple_db_server:select_data(Pid, fruit, name, banana)]),
    io:format("Step1:~p~n", [simple_db_server:select_data(Pid, fruit, name, orange)]),
    simple_db_server:update_data(Pid, fruit, [{price, 120}], name, apple),
    simple_db_server:update_data(Pid, fruit, [{price, 160}], price, 150),
    io:format("Step2:~p~n", [simple_db_server:select_data(Pid, fruit, name, apple)]),
    io:format("Step2:~p~n", [simple_db_server:select_data(Pid, fruit, name, banana)]),
    io:format("Step2:~p~n", [simple_db_server:select_data(Pid, fruit, name, orange)]),
    simple_db_server:delete_data(Pid, fruit, name, banana),
    io:format("Step3:~p~n", [simple_db_server:select_data(Pid, fruit, name, apple)]),
    io:format("Step3:~p~n", [simple_db_server:select_data(Pid, fruit, name, banana)]),
    io:format("Step3:~p~n", [simple_db_server:select_data(Pid, fruit, name, orange)]),
    simple_db_server:drop_table(Pid, fruit),
    %% ↓を実行するとets:lookupでエラー落ちする
    % io:format("Step4:~p~n", [simple_db_server:select_data(Pid, fruit, name, apple)]),
    % io:format("Step4:~p~n", [simple_db_server:select_data(Pid, fruit, name, banana)]),
    simple_db_server:stop(Pid),
    ok.
