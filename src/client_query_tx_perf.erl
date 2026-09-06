%%%-------------------------------------------------------------------
%%% @doc
%%% 複数のクライアントが同じ行を同時に加算する例。
%%%
%%% Count個の接続がそれぞれ +1 すれば、最終値はちょうどCountになる。
%%% ここが合わなければ更新の取りこぼし(lost update)が起きている。
%%%
%%% == やり直しが要る ==
%%%
%%% トランザクションが直列だった頃は、ただ実行すれば通った。
%%% いまはスナップショット分離なので、**同じ行を同時に更新すると
%%% 後からコミットする側が断られる**(serialization_failure)。
%%% 断らずに書かせると、先にコミットした側の +1 が消える。
%%%
%%% これはこのDBの都合ではなく、スナップショット分離を採る
%%% どのDBでも同じ。PostgreSQL の REPEATABLE READ でも、
%%% MySQL でも、クライアントはやり直しを書く必要がある。
%%%
%%% 待ちが閉路になったときの deadlock も同じ扱いで、やり直せばよい。
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
    Retries = wait_for(Count, 0),

    _ = query_exec:exec_query(Pid, {begin_tx}),
    [[total, Value]] = query_exec:exec_query(Pid, {select, counter, name, total}),
    ok = query_exec:exec_query(Pid, {commit_tx}),
    End = erlang:monotonic_time(millisecond),

    io:format("Final value: ~p (expected ~p)~n", [Value, Count]),
    io:format("Retries: ~p~n", [Retries]),
    io:format("Time: ~pms~n", [End - Start]),
    ok = query_exec:exec_query(Pid, {drop_table, counter}),
    Value = Count,
    ok.

%% 1接続で「読んで+1して書く」を1トランザクションで行う。
%% 断られたらやり直す。
%% Returns: {ok, やり直した回数}
increment() ->
    {ok, Pid} = gen_connection:connect(),
    ok = attempt(Pid),
    ok = gen_connection:disconnect(Pid),
    {ok, retry_count()}.

attempt(Pid) ->
    _ = query_exec:exec_query(Pid, {begin_tx}),
    [[total, Value]] = query_exec:exec_query(Pid, {select, counter, name, total}),
    case query_exec:exec_query(Pid, {update, counter, [{value, Value + 1}], name, total}) of
        {ok, 1} ->
            case query_exec:exec_query(Pid, {commit_tx}) of
                ok      -> ok;
                {error, Reason} -> retry(Pid, Reason)
            end;
        {error, Reason} ->
            %% デッドロックで断られた場合、その文の中でロールバック済み
            retry(Pid, Reason)
    end.

retry(Pid, Reason) when Reason =:= serialization_failure; Reason =:= deadlock ->
    _ = erlang:put(retries, retry_count() + 1),
    attempt(Pid);
retry(_Pid, Reason) ->
    {error, Reason}.

retry_count() ->
    case erlang:get(retries) of
        undefined -> 0;
        N         -> N
    end.

wait_for(0, Retries) ->
    Retries;
wait_for(N, Retries) ->
    receive {done, {ok, R}} -> wait_for(N - 1, Retries + R)
    after 60000 -> error({timeout, N})
    end.
