%%%-------------------------------------------------------------------
%%% @doc
%%% 複数のクライアントが同じ行を同時に加算する例。
%%%
%%% Count個の接続がそれぞれ +1 すれば、最終値はちょうどCountになる。
%%% 合わなければ更新の取りこぼし(lost update)が起きている。
%%%
%%% トランザクションが直列だった頃は、何も考えずに書けば通った。
%%% 並行に走るようになったので、**書き方で結果が変わる**。
%%% 3通り走らせて並べて出す。
%%%
%%%   1. REPEATABLE READ + クライアント側で読んで足す + やり直し
%%%      正しい。ただし待たされた側は必ず断られるので、全員が同時に
%%%      始まるとやり直しが n(n-1)/2 回になる
%%%
%%%   2. READ COMMITTED + クライアント側で読んで足す
%%%      **壊れる。** 断られないので一見うまくいくが、SELECT で読んだ値は
%%%      UPDATE を書いた時点では古い。やり直しも起きないので、
%%%      取りこぼしに気づけない。PostgreSQL の既定でも同じことが起きる
%%%
%%%   3. READ COMMITTED + SET value = value + 1
%%%      正しく、やり直しも要らない。加算がDBの中の1文で閉じているので、
%%%      行ロックを待った後に読み直して足し直せる
%%%
%%% 2 が示しているのは「READ COMMITTED が守るのは文の中だけ」ということ。
%%% 文をまたぐ読んで書くを守るには、REPEATABLE READ でやり直すか、
%%% SELECT ... FOR UPDATE(未実装)で読む時点でロックを取るしかない。
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
    io:format("~p connections, each +1 on the same row~n~n", [Count]),
    ok = run("REPEATABLE READ  read-modify-write", begin_tx,             read_add, Count, Count),
    %% ここだけ最終値が Count にならない。それがこの例の要点
    ok = run("READ COMMITTED   read-modify-write", begin_read_committed, read_add, Count, any),
    ok = run("READ COMMITTED   SET v = v + 1    ", begin_read_committed, in_place, Count, Count),
    ok.

run(Label, Begin, How, Count, Expected) ->
    {ok, Pid} = gen_connection:connect(),
    ok = query_exec:exec_query(Pid, {create_table, counter, [name, value]}),
    _ = query_exec:exec_query(Pid, {begin_tx}),
    {ok, _} = query_exec:exec_query(Pid, {insert, counter, [total, 0]}),
    ok = query_exec:exec_query(Pid, {commit_tx}),

    Start = erlang:monotonic_time(millisecond),
    Self = self(),
    [spawn_link(fun() -> Self ! {done, increment(Begin, How)} end) || _ <- lists:seq(1, Count)],
    Retries = wait_for(Count, 0),
    End = erlang:monotonic_time(millisecond),

    _ = query_exec:exec_query(Pid, {begin_tx}),
    [[total, Value]] = query_exec:exec_query(Pid, {select, counter, name, total}),
    ok = query_exec:exec_query(Pid, {commit_tx}),

    io:format("~s  value=~p (expected ~p)  retries=~p  ~pms~n",
              [Label, Value, Count, Retries, End - Start]),
    case Value =:= Count of
        true  -> ok;
        false -> io:format("  ^ 取りこぼし ~p 件~n", [Count - Value])
    end,
    ok = query_exec:exec_query(Pid, {drop_table, counter}),
    ok = check(Expected, Value).

check(any, _Value)      -> ok;
check(Value, Value)     -> ok;
check(Expected, Value)  -> {error, {expected, Expected, got, Value}}.

%% 1接続で +1 を1トランザクションで行う。断られたらやり直す。
increment(Begin, How) ->
    {ok, Pid} = gen_connection:connect(),
    _ = erlang:put(retries, 0),
    ok = attempt(Pid, Begin, How),
    ok = gen_connection:disconnect(Pid),
    {ok, retry_count()}.

attempt(Pid, Begin, How) ->
    _ = query_exec:exec_query(Pid, {Begin}),
    case add_one(Pid, How) of
        {ok, 1} ->
            case query_exec:exec_query(Pid, {commit_tx}) of
                ok              -> ok;
                {error, Reason} -> retry(Pid, Begin, How, Reason)
            end;
        {error, Reason} ->
            %% 断られた文の中でロールバック済み
            retry(Pid, Begin, How, Reason)
    end.

%% 読んでから足す。SELECT と UPDATE が別の文になる
add_one(Pid, read_add) ->
    [[total, Value]] = query_exec:exec_query(Pid, {select, counter, name, total}),
    query_exec:exec_query(Pid, {update, counter, [{value, Value + 1}], name, total});
%% DBの中で足す。1文で閉じている
add_one(Pid, in_place) ->
    query_exec:exec_query(Pid, {sql, "UPDATE counter SET value = value + 1"}).

retry(Pid, Begin, How, Reason)
  when Reason =:= serialization_failure; Reason =:= deadlock ->
    _ = erlang:put(retries, retry_count() + 1),
    attempt(Pid, Begin, How);
retry(_Pid, _Begin, _How, Reason) ->
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
