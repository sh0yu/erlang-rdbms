-module(tether_log_tests).
-include_lib("eunit/include/eunit.hrl").

-import(tether_test_util, [tmpdir/0, rmrf/1, read/1, corrupt_byte/2, truncate_file/2]).

%%%===================================================================
%%% 土台
%%%===================================================================

with_log(F) ->
    D = tmpdir(),
    try
        {ok, _} = tether_log:start_link(D),
        try F(D) after gen_server:stop(tether_log) end
    after rmrf(D)
    end.

collect(D) ->
    {ok, Acc, Info} = tether_log:fold(D, fun(P, L, A) -> [{L, P} | A] end, []),
    {lists:reverse(Acc), Info}.

%%%===================================================================
%%% 書いて読む
%%%===================================================================

write_read_test() ->
    with_log(fun(D) ->
        {ok, L1} = tether_log:write(<<"one">>),
        {ok, L2} = tether_log:write(<<"two">>),
        ?assertEqual(0, L1),
        ?assertEqual(15, L2),                 % 12 + 3
        {Recs, Info} = collect(D),
        ?assertEqual([{0, <<"one">>}, {15, <<"two">>}], Recs),
        ?assertEqual(2, maps:get(records, Info)),
        ?assertEqual(complete, maps:get(tail, Info))
    end).

%% write/1 が返った時点で、バイト列はファイルに届いている。
durable_on_return_test() ->
    with_log(fun(D) ->
        {ok, _} = tether_log:write(<<"durable">>),
        Bin = read(tether_log:path(D)),
        ?assertEqual(19, byte_size(Bin)),
        ?assertMatch({[<<"durable">>], _, complete}, tether_rec:scan(Bin))
    end).

empty_log_test() ->
    with_log(fun(D) ->
        ?assertEqual({[], #{records => 0, valid_bytes => 0, tail => complete}},
                     collect(D))
    end).

%%%===================================================================
%%% group commit
%%%===================================================================

%% 同時に来た書き込みは、1回の fsync にまとまる。
%% fsync はミリ秒の桁なので、まとまらなければスループットが
%% fsync の回数で頭打ちになる。
group_commit_test_() ->
    {timeout, 30, fun() ->
        with_log(fun(_D) ->
            N = 500,
            Parent = self(),
            Pids = [spawn_link(fun() ->
                        receive go -> ok end,
                        R = tether_log:write(<<I:64>>),
                        Parent ! {done, self(), R}
                    end) || I <- lists:seq(1, N)],
            _ = [P ! go || P <- Pids],
            Lsns = [receive {done, _, {ok, L}} -> L end || _ <- Pids],
            ?assertEqual(N, length(lists:usort(Lsns))),   % LSN は重ならない

            #{writes := W, syncs := Sy} = tether_log:stat(),
            ?assertEqual(N, W),
            ?assert(Sy < W),
            ?debugFmt("group commit: ~p writes / ~p syncs (~.1f 件/sync)",
                      [W, Sy, W / Sy])
        end)
    end}.

%%%===================================================================
%%% 再起動
%%%===================================================================

reopen_appends_test() ->
    D = tmpdir(),
    try
        {ok, _} = tether_log:start_link(D),
        {ok, 0} = tether_log:write(<<"a">>),
        ok = gen_server:stop(tether_log),

        {ok, _} = tether_log:start_link(D),
        {ok, L} = tether_log:write(<<"b">>),
        ?assertEqual(13, L),
        ok = gen_server:stop(tether_log),

        {Recs, _} = collect(D),
        ?assertEqual([{0, <<"a">>}, {13, <<"b">>}], Recs)
    after rmrf(D)
    end.

%% 電源断で末尾が切れているのは正常な状態。捨てて続行する。
truncated_tail_is_normal_test() ->
    D = tmpdir(),
    try
        {ok, _} = tether_log:start_link(D),
        {ok, 0}  = tether_log:write(<<"keep">>),
        {ok, 16} = tether_log:write(<<"lose">>),
        ok = gen_server:stop(tether_log),

        truncate_file(tether_log:path(D), 20),      % 2件目の途中で切る

        {ok, _} = tether_log:start_link(D),
        {ok, L} = tether_log:write(<<"next">>),
        ?assertEqual(16, L),                        % 切れた分は上書きされる
        ok = gen_server:stop(tether_log),

        {Recs, _} = collect(D),
        ?assertEqual([{0, <<"keep">>}, {16, <<"next">>}], Recs)
    after rmrf(D)
    end.

%% 途中の破損は起動を拒否する。読めるところまで読んで続行すると、
%% ディスクの故障が「データが少し古いだけ」に化けて表に出てこない。
corrupt_refuses_to_start_test() ->
    D = tmpdir(),
    process_flag(trap_exit, true),
    try
        {ok, _} = tether_log:start_link(D),
        {ok, _} = tether_log:write(<<"one">>),
        {ok, _} = tether_log:write(<<"two">>),
        {ok, _} = tether_log:write(<<"three">>),
        ok = gen_server:stop(tether_log),

        corrupt_byte(tether_log:path(D), 28),       % 2件目の本体

        ?assertMatch({error, {corrupt_log, bad_crc, 15}}, tether_log:start_link(D)),
        ?assertMatch({error, {corrupt, bad_crc, 15}}, collect_err(D)),

        %% 人が判断して repair を呼べば、有効な前半だけが残る
        {ok, #{discarded := Disc}} = tether_log:repair(D),
        ?assert(Disc > 0),
        {ok, _} = tether_log:start_link(D),
        ok = gen_server:stop(tether_log),
        {Recs, _} = collect(D),
        ?assertEqual([{0, <<"one">>}], Recs)
    after
        flush_exits(),
        process_flag(trap_exit, false),
        rmrf(D)
    end.

collect_err(D) -> tether_log:fold(D, fun(_, _, A) -> A end, []).

flush_exits() -> receive {'EXIT', _, _} -> flush_exits() after 0 -> ok end.
