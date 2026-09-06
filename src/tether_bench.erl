%%%-------------------------------------------------------------------
%%% @doc
%%% 預かり(escrow)が実際に効くかを測る。
%%%
%%%   bin/tether bench
%%%
%%% 測るのは主に**中央に触れた回数**である。
%%% 中央の行は全クライアントが争う一点で、そこに触れる回数が
%%% そのまま競合とスループットの上限を決める。
%%%
%%% 比較対象は「現場が実際にやっていること」でなければ意味がない。
%%% だから素朴な read-modify-write (get して cas) を基準に置く。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_bench).

-export([run/0, run/1]).

-define(SKU,   <<"sku:1">>).
-define(STOCK, {<<"bench">>, <<"stock">>}).

-record(r, {mode        :: atom(),
            sold    = 0 :: non_neg_integer(),
            central = 0 :: non_neg_integer(),   % 中央の行に触れた回数
            reqs    = 0 :: non_neg_integer(),   % 送った要求数
            retries = 0 :: non_neg_integer(),
            micros  = 0 :: non_neg_integer(),
            idle    = 0 :: non_neg_integer()}).  % 配ってあって未消費

run() ->
    _ = run(#{stock => 100000, clients => 200, per_client => 100,
              title => "在庫が潤沢な場合"}),
    _ = run(#{stock => 20000, clients => 200, per_client => 100,
              title => "在庫がちょうど尽きる場合"}),
    run(#{stock => 100000, clients => 200, per_client => 100, use => 4,
          title => "預かった量の1/4しか使わずに消える場合(遊休を測る)"}).

run(Opts) ->
    ok = io:setopts(standard_io, [{encoding, unicode}]),
    ok = logger:set_primary_config(level, critical),
    #{stock := S, clients := C, per_client := K} = Opts,
    io:format("~n  === ~ts ===~n", [maps:get(title, Opts, "")]),
    io:format("  在庫 ~p / クライアント ~p(並行) / 1人あたり ~p 件 (総要求 ~p)~n~n",
              [S, C, K, C * K]),
    Use  = maps:get(use, Opts, 1),
    Cas  = measure(cas,    S, C, K, Use),
    Decr = measure(decr,   S, C, K, Use),
    Esc  = measure(escrow, S, C, K, Use),
    report(Cas, Decr, Esc),
    {Cas, Decr, Esc}.

%%%===================================================================

%%----------------------------------------------------------------------
%% クライアントは**並行に**走らせる。直列に回すと競合が一度も起きず、
%% read-modify-write の側が実際よりずっと良く見える。
%% (最初にそれをやって、再試行 0 という嘘の数字を出した)
%%----------------------------------------------------------------------
measure(Mode, S, C, K, Use) ->
    ok = fresh(),
    ok = init_stock(Mode, S),
    Parent = self(),
    Pids = [spawn(fun() ->
                          receive go -> ok end,
                          Parent ! {done, client(Mode, <<"c", I:32>>, K, Use,
                                                 #r{mode = Mode})}
                  end) || I <- lists:seq(1, C)],
    T0 = erlang:monotonic_time(microsecond),
    _  = [P ! go || P <- Pids],
    R  = lists:foldl(fun(_, Acc) -> receive {done, X} -> merge(Acc, X) end end,
                     #r{mode = Mode}, Pids),
    T1 = erlang:monotonic_time(microsecond),
    R#r{micros = T1 - T0, idle = idle(Mode)}.

merge(A, B) ->
    A#r{sold = A#r.sold + B#r.sold, central = A#r.central + B#r.central,
        reqs = A#r.reqs + B#r.reqs, retries = A#r.retries + B#r.retries}.

fresh() ->
    _ = application:stop(tether),
    _ = application:load(tether),
    D = filename:join("data", "bench"),
    _ = [file:delete(F) || F <- filelib:wildcard(filename:join(D, "*"))],
    application:set_env(tether, dir, D),
    {ok, _} = application:ensure_all_started(tether),
    ok.

init_stock(M, S) when M =:= cas; M =:= decr ->
    {ok, [ok]} = tether:request(<<"$init">>, 1,
                                [{put, ?STOCK, integer_to_binary(S)}]),
    ok;
init_stock(escrow, S) ->
    {ok, S} = tether:stock(?SKU, S),
    ok.

idle(escrow) -> {_, G} = tether:pool(?SKU), G;
idle(_)      -> 0.

%%%===================================================================
%%% 素朴な read-modify-write。現場の基準。
%%%===================================================================

%% Use は「預かった量のうち何分の1しか使わずに消えるか」。
%% 1 なら全部使う。4 なら 1/4 だけ使って、残りは遊休になる。
client(cas, C, K, U, R)    -> cas_loop(C, 1, K div U, R);
client(decr, C, K, U, R)   -> decr_loop(C, 1, K div U, R);
client(escrow, C, K, U, R) -> esc_loop(C, 1, K, K div U, R).

%%%===================================================================
%%% 1文での原子的な減算。RDBMS が実際に提供する形。
%%% 再試行は起きないが、**その行に対する直列化は残る**。
%%%===================================================================

decr_loop(_C, _Seq, 0, R) -> R;
decr_loop(C, Seq, K, R) ->
    R1 = R#r{reqs = R#r.reqs + 1, central = R#r.central + 1},
    case tether:request(C, Seq, [{decr, ?STOCK, 1}]) of
        {ok, [{decremented, _}]} -> decr_loop(C, Seq + 1, K - 1,
                                              R1#r{sold = R1#r.sold + 1});
        {error, _, _}            -> R1
    end.

cas_loop(_C, _Seq, 0, R) -> R;
cas_loop(C, _Seq, _K, R) when R#r.retries > 100000 ->
    _ = C, R;                                    % 暴走の歯止め
cas_loop(C, Seq, K, R) ->
    %% 読む(中央に触れる) → 書く(中央に触れる)。2往復。
    {ok, [{ok, V}]} = tether:request(C, Seq, [{get, ?STOCK}]),
    N = binary_to_integer(V),
    R1 = R#r{reqs = R#r.reqs + 2, central = R#r.central + 2},
    case N of
        0 -> R1;
        _ ->
            case tether:request(C, Seq + 1,
                                [{cas, ?STOCK, V, integer_to_binary(N - 1)}]) of
                {ok, [ok]} ->
                    cas_loop(C, Seq + 2, K - 1, R1#r{sold = R1#r.sold + 1});
                {error, _, {conflict, _}} ->
                    %% 前提が古かった。読み直してやり直す。
                    cas_loop(C, Seq + 2, K, R1#r{retries = R1#r.retries + 1})
            end
    end.

%%%===================================================================
%%% 預かり。消費は中央に触れない。
%%%===================================================================

%% Want は一度に要求する量、Left は実際に売る残り。
esc_loop(_C, _Seq, _Want, 0, R) -> R;
esc_loop(C, Seq, Want, Left, R) ->
    case tether:request(C, Seq, [{consume, ?SKU, 1}]) of
        {ok, [{consumed, _}]} ->
            %% **中央に触れていない**
            esc_loop(C, Seq + 1, Want, Left - 1,
                     R#r{reqs = R#r.reqs + 1, sold = R#r.sold + 1});
        {error, _, _} ->
            %% 預かりが切れた。ここで初めて中央に触れる。
            R1 = R#r{reqs = R#r.reqs + 1},
            case tether:request(C, Seq + 1, [{acquire, ?SKU, Want, 600000}]) of
                {ok, [{granted, _, _}]} ->
                    esc_loop(C, Seq + 2, Want, Left,
                             R1#r{reqs = R1#r.reqs + 1,
                                  central = R1#r.central + 1});
                {error, _, sold_out} ->
                    R1#r{reqs = R1#r.reqs + 1, central = R1#r.central + 1}
            end
    end.

%%%===================================================================

report(A, D, B) ->
    line(),
    row("", ["get+cas", "atomic decr", "escrow"]),
    line(),
    row("売れた数",     [A#r.sold, D#r.sold, B#r.sold]),
    row("送った要求数", [A#r.reqs, D#r.reqs, B#r.reqs]),
    row("中央に触れた回数", [A#r.central, D#r.central, B#r.central]),
    row("再試行",       [A#r.retries, D#r.retries, B#r.retries]),
    row("経過(ms)",     [ms(A), ms(D), ms(B)]),
    row("遊休",         [A#r.idle, D#r.idle, B#r.idle]),
    line(),
    io:format("~n"),
    Net = case B#r.sold of 0 -> 0.0; S -> B#r.central * 100 / S end,
    io:format("    **接続が必要だったのは ~p 件中 ~p 回 (~.2f%)**~n",
              [B#r.sold, B#r.central, Net]),
    io:format("    残り ~.2f% は圏外でもできた~n", [100.0 - Net]),
    io:format("    1回の協調あたり ~.1f 件~n",
              [case B#r.central of 0 -> 0.0; N -> B#r.sold / N end]),
    io:format("    原子的減算に対する短縮: ~.1f 倍~n",
              [case ms(B) of 0 -> 0.0; M -> ms(D) / M end]),
    io:format("    遊休は総量の ~.2f%~n~n",
              [case B#r.sold + B#r.idle of
                   0 -> 0.0; T -> B#r.idle * 100 / T end]).

ms(#r{micros = M}) -> M div 1000.

line() -> io:format("  ~s~n", [lists:duplicate(64, $-)]).

row(Label, Vals) ->
    L = unicode:characters_to_binary(Label),
    io:format("  ~ts~s", [L, lists:duplicate(max(1, 28 - width(L)), $\s)]),
    _ = [io:format("~14ts", [fmt(V)]) || V <- Vals],
    io:format("~n").

fmt(V) when is_integer(V) -> integer_to_list(V);
fmt(V) -> unicode:characters_to_binary(V).

width(B) ->
    lists:sum([case C of _ when C > 16#2E80 -> 2; _ -> 1 end
               || C <- unicode:characters_to_list(B)]).
