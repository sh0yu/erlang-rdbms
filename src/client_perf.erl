-module(client_perf).
-compile(export_all).

exec() ->
    %% カーディナリティが小さい場合、カラムインデックスが肥大化しやすいため性能劣化が激しい
    %% Cardinality/Count > 10% ぐらいは必要
    Cardinality = 100000,
    Count = 1000000,
    {ok, Pid} = simple_db_server:start_link(),
    simple_db_server:create_table(Pid, random, [random_val]),
    RandomValList = [rand:uniform(Cardinality) || _ <- lists:seq(1, Count)],
    UsortedRandomValList = lists:usort(RandomValList),
    UpdatedRandomValList = lists:map(fun(X) -> X - 1 end, UsortedRandomValList),
    % io:format("~p~n~p~n", [RandomValList, UsortedRandomValList]),
    io:format("start insert~n"),
    StartInsert = erlang:system_time(microsecond),
    lists:map(fun(Val) -> simple_db_server:insert_data(Pid, random, [Val]) end, RandomValList),
    EndInsert = erlang:system_time(microsecond),
    io:format("~p[microsec]~n", [EndInsert - StartInsert]),
    % io:format("~p~n", [ets:select(random, [{{'$1','$2'}, [{is_integer, '$1'}], ['$$']}])]),
    io:format("start update~n"),
    StartUpdate = erlang:system_time(microsecond),
    lists:map(fun(Val) -> simple_db_server:update_data(Pid, random, [{random_val, Val-1}], random_val, Val) end,
    UsortedRandomValList),
    EndUpdate = erlang:system_time(microsecond),
    % io:format("~p~n", [ets:select(random, [{{'$1','$2'}, [{is_integer, '$1'}], ['$$']}])]),
    io:format("~p[microsec]~n", [EndUpdate - StartUpdate]),
    io:format("start delete~n"),
    StartDelete = erlang:system_time(microsecond),
    lists:map(fun(Val) -> simple_db_server:delete_data(Pid, random, random_val, Val) end,
    UpdatedRandomValList),
    EndDelete = erlang:system_time(microsecond),
    io:format("~p[microsec]~n", [EndDelete - StartDelete]),
    simple_db_server:drop_table(Pid, random),
    simple_db_server:stop(Pid),
    ok.
