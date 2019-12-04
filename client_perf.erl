-module(client_perf).
-compile(export_all).

exec() ->
    %% カーディナリティが小さい場合、カラムインデックスが肥大化しやすいため性能劣化が激しい
    %% Cardinality/Count > 10% ぐらいは必要
    Cardinality = 100000,
    Count = 1000000,
    {ok, Pid} = simple_db_server:start_link(),
    simple_db_server:create_table(Pid, random, [random_val]),
    RandomValList_2 = [rand:uniform(Cardinality) || _ <- lists:seq(1, Count)],
    io:format("start insert~n"),
    StartInsert = erlang:system_time(microsecond),
    lists:map(fun(Val) -> simple_db_server:insert_data(Pid, random, [Val]) end, RandomValList_2),
    EndInsert = erlang:system_time(microsecond),
    io:format("~p[microsec]~n", [EndInsert - StartInsert]),
    % io:format("~p~n", [ets:select(random, [{{'$1','$2'}, [{is_integer, '$1'}], ['$$']}])]),
    io:format("start update~n"),
    StartUpdate = erlang:system_time(microsecond),
    simple_db_server:update_data(Pid, random, [{random_val, 0}], random_val, 1),
    EndUpdate = erlang:system_time(microsecond),
    io:format("~p[microsec]~n", [EndUpdate - StartUpdate]),
    io:format("start delete~n"),
    StartDelete = erlang:system_time(microsecond),
    simple_db_server:delete_data(Pid, random, random_val, 0),
    EndDelete = erlang:system_time(microsecond),
    io:format("~p[microsec]~n", [EndDelete - StartDelete]),
    simple_db_server:drop_table(Pid, random),
    simple_db_server:stop(Pid),
    ok.
