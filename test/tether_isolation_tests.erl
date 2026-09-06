%%%-------------------------------------------------------------------
%%% @doc
%%% 隔離の実証。
%%%
%%% この設計は「セッションを生きたプロセスにする」ことで成り立っている。
%%% しかし多テナントの共有サービスでそれをやるなら、**1人の都合が
%%% 他人に出ないこと**を示さなければならない。示せなければ、
%%% 既存のデータベースが接続=セッションにしている理由の方が正しい。
%%%
%%% ここで確かめるのは3つ。
%%%   1. 食い潰したクライアントは、自分だけ死ぬ
%%%   2. 異常終了したクライアントは、他人にも下層にも波及しない
%%%   3. セッションは実際に安い
%%% @end
%%%-------------------------------------------------------------------
-module(tether_isolation_tests).
-include_lib("eunit/include/eunit.hrl").

-import(tether_test_util, [tmpdir/0, rmrf/1]).

-define(K(K), {<<"t">>, <<K>>}).

with_db(MaxHeap, F) ->
    %% 前のテストが止め損ねていても、必ず新しい状態から始める。
    %% ensure_all_started は起動済みなら何もしないので、
    %% dir の設定だけ変えても古いディレクトリを掴んだままになる。
    _ = application:stop(tether),
    %% load を先に済ませる。application:load/1 は .app の env を読み込むので、
    %% 未 load の状態で set_env しても、そこで上書きされて既定値に戻る。
    %% (VM 内で最初に走ったテストだけが既定の "data" を掴む、という
    %%  再現しにくい形で出る。実際に12MBの残骸を作って気づいた)
    _ = application:load(tether),
    D = tmpdir(),
    application:set_env(tether, dir, D),
    application:set_env(tether, session_max_heap, MaxHeap),
    {ok, _} = application:ensure_all_started(tether),
    try F(D)
    after
        ok = application:stop(tether),
        application:unset_env(tether, session_max_heap),
        rmrf(D)
    end.

%%%===================================================================
%%% 1. 食い潰しても、自分だけ死ぬ
%%%===================================================================

%% 大きなバイナリを含む要求は、**境界で拒む**。
%%
%% ここは当初 max_heap_size に任せていて、実測で外れた。
%% 64バイトを超えるバイナリはプロセスヒープに載らず、参照計数される
%% 共有領域に置かれる。13MB ぶんのバイナリを投げつけたセッションの
%% ヒープは 34KB のままで、上限に一度も触れなかった。
%%
%% つまり max_heap_size は「プロセスのメモリ使用量の上限」ではない。
%% バイナリを縛るには、明示的に大きさを見て拒むしかない。
oversized_request_is_refused_test_() ->
    {timeout, 60, fun() ->
        with_db(200000, fun(_) ->
            {ok, [ok]} = tether:request(<<"bob">>, 1, [{put, ?K("bob"), <<"ok">>}]),

            Huge = [{put, {<<"t">>, <<I:64>>}, binary:copy(<<0>>, 65536)}
                    || I <- lists:seq(1, 200)],       % ≒ 13MB
            ?assertMatch({error, {request_too_large, _, _}},
                         tether:request(<<"greedy">>, 1, Huge)),

            %% 1つの値が上限を超えている場合も拒む
            ?assertMatch({error, {op, 1, {value_too_large, _, _}}},
                         tether:request(<<"greedy">>, 1,
                                        [{put, ?K("x"), binary:copy(<<0>>, 70000)}])),
            ?assertMatch({error, {too_many_ops, _, _}},
                         tether:request(<<"greedy">>, 1,
                                        [{get, ?K("x")} || _ <- lists:seq(1, 2000)])),

            %% 拒まれただけ。セッションは作られてすらいない
            ?assertEqual(none, tether_sessions:lookup(<<"greedy">>)),
            %% 他のクライアントも下層も無傷
            {ok, [ok]} = tether:request(<<"bob">>, 2, [{put, ?K("b2"), <<"ok">>}]),
            ?assertEqual({ok, <<"ok">>}, tether:read(?K("bob")))
        end)
    end}.

%% 上限をすり抜けてヒープ上で膨らんだ場合の、二段目の防壁。
%% 小さな項を大量に並べた要求は参照計数領域を使わないので、
%% max_heap_size がちゃんと効く。**そのセッションだけが死ぬ。**
%%
%% 共有ヒープの実行環境(JVM / Go / .NET)では、この上限を
%% プロセス単位で掛けられないので、同じことがサービス全体の OOM になる。
heap_runaway_dies_alone_test_() ->
    {timeout, 60, fun() ->
        with_db(200000, fun(_) ->              % 上限 200k ワード ≒ 1.6MB
            {ok, [ok]} = tether:request(<<"bob">>, 1, [{put, ?K("bob"), <<"ok">>}]),
            {ok, BobPid} = tether_sessions:lookup(<<"bob">>),

            %% 小さい項を大量に。全部ヒープに載る。
            %% 上限の検査を迂回してセッションへ直接送る(二段目の試験なので)
            Huge = [{put, {<<"t">>, <<I:64>>}, <<"x">>} || I <- lists:seq(1, 100000)],
            {ok, Pid} = tether:open(<<"greedy">>),
            Ref = erlang:monitor(process, Pid),
            _ = catch tether_session:request(Pid, 1, Huge),
            receive {'DOWN', Ref, _, _, R} -> ?assertEqual(killed, R)
            after 10000 -> error({greedy_survived, process_info(Pid, memory)})
            end,

            %% 他のクライアントは作り直されてすらいない
            ?assertEqual({ok, BobPid}, tether_sessions:lookup(<<"bob">>)),
            {ok, [ok]} = tether:request(<<"bob">>, 2, [{put, ?K("b2"), <<"ok">>}]),
            ?assertMatch(#{last := 2, served := 2}, tether:session_info(<<"bob">>)),

            %% 下層も無傷
            ?assertEqual({ok, <<"ok">>}, tether:read(?K("bob"))),
            ?assert(is_map(tether_log:stat())),

            %% 暴走した側も、次の要求で作り直される
            {ok, [ok]} = tether:request(<<"greedy">>, 1, [{put, ?K("g"), <<"1">>}]),
            ?assertMatch(#{last := 1}, tether:session_info(<<"greedy">>))
        end)
    end}.

%%%===================================================================
%%% 2. 異常終了は波及しない
%%%===================================================================

%% セッションが何度死んでも、supervisor の強度上限に触れて
%% 他のセッションを巻き込むことがない(restart => temporary)。
repeated_crashes_do_not_cascade_test() ->
    with_db(1000000, fun(_) ->
        {ok, [ok]} = tether:request(<<"bob">>, 1, [{put, ?K("b"), <<"1">>}]),
        {ok, BobPid} = tether_sessions:lookup(<<"bob">>),

        %% 別のクライアントのセッションを 50 回殺す
        _ = [begin
                 {ok, P} = tether:open(<<"flaky">>),
                 Ref = erlang:monitor(process, P),
                 exit(P, kill),
                 receive {'DOWN', Ref, _, _, _} -> ok after 1000 -> error(timeout) end
             end || _ <- lists:seq(1, 50)],

        %% bob のプロセスは同一のまま。作り直されてすらいない
        ?assertEqual({ok, BobPid}, tether_sessions:lookup(<<"bob">>)),
        {ok, [ok]} = tether:request(<<"bob">>, 2, [{put, ?K("b2"), <<"2">>}]),

        %% 名簿にも死骸が残っていない
        ?assertEqual(none, tether_sessions:lookup(<<"flaky">>))
    end).

%%%===================================================================
%%% 3. セッションは実際に安い
%%%===================================================================

%% 1セッションあたりのメモリを実測する。
%% 「100万セッションを1台に載せる」という主張が成り立つかどうかは、
%% ここの数字で決まる。
session_cost_test_() ->
    {timeout, 120, fun() ->
        with_db(1000000, fun(_) ->
            N = 20000,
            _ = [erlang:garbage_collect(P) || P <- processes()],
            Before = erlang:memory(processes),
            T0 = erlang:monotonic_time(microsecond),
            _ = [{ok, _} = tether:open(<<"c", I:64>>) || I <- lists:seq(1, N)],
            T1 = erlang:monotonic_time(microsecond),
            ?assertEqual(N, tether:session_count()),

            %% 暇なセッションは hibernate でヒープを最小まで縮める。
            %% 100万セッションのうち動いているのは一握り、という前提。
            _ = [begin {ok, P} = tether_sessions:lookup(<<"c", I:64>>),
                       erlang:garbage_collect(P)
                 end || I <- lists:seq(1, N)],
            After = erlang:memory(processes),

            Bytes = (After - Before) / N,
            Micros = (T1 - T0) / N,
            ?debugFmt("セッション ~p 個: ~.1f バイト/個, 生成 ~.1f µs/個 "
                      "(100万個なら ~.1f GB)",
                      [N, Bytes, Micros, Bytes * 1000000 / 1024 / 1024 / 1024]),
            ?assert(Bytes < 10000),
            ?assert(Micros < 100)
        end)
    end}.
