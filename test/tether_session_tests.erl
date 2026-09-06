-module(tether_session_tests).
-include_lib("eunit/include/eunit.hrl").

-import(tether_test_util, [tmpdir/0, rmrf/1]).

-define(K(K), {<<"t">>, <<K>>}).

with_db(F) ->
    D = tmpdir(),
    application:set_env(tether, dir, D),
    {ok, _} = application:ensure_all_started(tether),
    try F(D) after ok = application:stop(tether), rmrf(D) end.

restart() ->
    ok = application:stop(tether),
    {ok, _} = application:ensure_all_started(tether),
    ok.

%% セッションプロセスを問答無用で殺し、名簿から消えるまで待つ。
kill_session(Client) ->
    {ok, Pid} = tether_sessions:lookup(Client),
    Ref = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, _, _, _} -> ok after 1000 -> error(timeout) end,
    wait_gone(Client, 100).

wait_gone(_C, 0) -> error(still_registered);
wait_gone(C, N) ->
    case tether_sessions:lookup(C) of
        none -> ok;
        _    -> timer:sleep(5), wait_gone(C, N - 1)
    end.

%%%===================================================================
%%% exactly-once — この設計の存在理由
%%%===================================================================

%% 同じ通番の再送は実行されず、初回と同じ答えが返る。
%%
%% 「作成する。既にあれば失敗」という操作で試すのが要点である。
%% もし再実行されていたら、2度目は conflict になるので、
%% **答えが違えば必ず露見する**。
retry_returns_original_answer_test() ->
    with_db(fun(_) ->
        Ops = [{cas, ?K("acct"), undefined, <<"created">>}],
        R1 = tether:request(<<"alice">>, 1, Ops),
        ?assertEqual({ok, [ok]}, R1),

        %% 応答を受け取り損ねたつもりで、そのまま送り直す
        R2 = tether:request(<<"alice">>, 1, Ops),
        ?assertEqual(R1, R2),

        %% 3度でも4度でも同じ
        ?assertEqual(R1, tether:request(<<"alice">>, 1, Ops)),
        ?assertEqual(R1, tether:request(<<"alice">>, 1, Ops)),

        %% 実行は1回だけ、吸収が3回
        ?assertMatch(#{served := 1, deduped := 3}, tether:session_info(<<"alice">>))
    end).

%% 素朴な実装なら、ここで違う答えが返る。
%% 「同じ操作を送ったのに答えが違う」がまさに再実行の証拠になる。
naive_retry_would_differ_test() ->
    with_db(fun(_) ->
        Ops = [{cas, ?K("acct"), undefined, <<"created">>}],
        {ok, [ok]} = tether:request(<<"alice">>, 1, Ops),
        %% 通番を進めれば、それは別の要求なので当然実行される
        ?assertEqual({error, 1, {conflict, <<"created">>}},
                     tether:request(<<"alice">>, 2, Ops))
    end).

%% 失敗した要求の再送も、同じ失敗を返す。
%% ここを実行し直すと、状態が変わっていた場合に成功してしまう。
retry_of_failure_returns_same_failure_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether:request(<<"c">>, 1, [{put, ?K("x"), <<"0">>}]),
        Err = tether:request(<<"c">>, 2, [{cas, ?K("x"), <<"ちがう">>, <<"9">>}]),
        ?assertMatch({error, 1, {conflict, <<"0">>}}, Err),

        %% 誰かが x を消しても、再送の答えは変わらない
        {ok, [ok]} = tether:request(<<"other">>, 1, [{delete, ?K("x")}]),
        ?assertEqual(Err, tether:request(<<"c">>, 2,
                                         [{cas, ?K("x"), <<"ちがう">>, <<"9">>}]))
    end).

%%%===================================================================
%%% 記憶はプロセスではなくログにある
%%%===================================================================

%% セッションプロセスを kill しても、再送は吸収される。
memory_survives_process_death_test() ->
    with_db(fun(_) ->
        Ops = [{cas, ?K("k"), undefined, <<"v">>}],
        R1 = tether:request(<<"alice">>, 1, Ops),
        ?assertEqual({ok, [ok]}, R1),

        kill_session(<<"alice">>),
        ?assertEqual(none, tether:session_info(<<"alice">>)),

        %% 作り直され、記憶をログから取り戻す
        ?assertEqual(R1, tether:request(<<"alice">>, 1, Ops)),
        ?assertMatch(#{served := 0, deduped := 1}, tether:session_info(<<"alice">>))
    end).

%% ノードごと落として起動し直しても、再送は吸収される。
memory_survives_restart_test() ->
    with_db(fun(_) ->
        Ops = [{cas, ?K("k"), undefined, <<"v">>}],
        R1 = tether:request(<<"alice">>, 1, Ops),
        restart(),
        ?assertEqual(R1, tether:request(<<"alice">>, 1, Ops)),
        ?assertEqual({ok, <<"v">>}, tether:read(?K("k")))
    end).

%%%===================================================================
%%% 通番の規約
%%%===================================================================

seq_gap_is_rejected_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether:request(<<"c">>, 1, [{put, ?K("a"), <<"1">>}]),
        ?assertEqual({error, {seq_gap, 1}},
                     tether:request(<<"c">>, 3, [{put, ?K("b"), <<"2">>}])),
        ?assertEqual(not_found, tether:read(?K("b")))
    end).

seq_too_old_is_rejected_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether:request(<<"c">>, 1, [{put, ?K("a"), <<"1">>}]),
        {ok, [ok]} = tether:request(<<"c">>, 2, [{put, ?K("b"), <<"2">>}]),
        ?assertEqual({error, {seq_too_old, 2}},
                     tether:request(<<"c">>, 1, [{put, ?K("a"), <<"9">>}]))
    end).

%%%===================================================================
%%% クライアントは互いに独立
%%%===================================================================

clients_are_independent_test() ->
    with_db(fun(_) ->
        {ok, [ok]} = tether:request(<<"alice">>, 1, [{put, ?K("a"), <<"1">>}]),
        {ok, [ok]} = tether:request(<<"bob">>,   1, [{put, ?K("b"), <<"2">>}]),
        ?assertEqual(2, tether:session_count()),
        ?assertMatch(#{last := 1}, tether:session_info(<<"alice">>)),
        ?assertMatch(#{last := 1}, tether:session_info(<<"bob">>)),
        %% alice の再送は bob に影響しない
        {ok, [ok]} = tether:request(<<"alice">>, 1, [{put, ?K("a"), <<"1">>}]),
        ?assertMatch(#{deduped := 0}, tether:session_info(<<"bob">>))
    end).
