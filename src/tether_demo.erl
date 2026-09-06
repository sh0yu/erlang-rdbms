%%%-------------------------------------------------------------------
%%% @doc
%%% exactly-once が効いていることを、目で見える形で示す。
%%%
%%%   bin/tether demo
%%% @end
%%%-------------------------------------------------------------------
-module(tether_demo).

-export([run/0, run/1]).

-define(ACCT, {<<"accounts">>, <<"alice">>}).

run() -> run("data/demo").

run(Dir) ->
    %% -noshell の標準出力は既定で latin1。日本語がエスケープされる。
    ok = io:setopts(standard_io, [{encoding, unicode}]),
    %% supervisor 報告で筋が読めなくなるので黙らせる。
    ok = logger:set_primary_config(level, critical),
    _ = application:load(tether),
    application:set_env(tether, dir, Dir),
    _ = [file:delete(F) || F <- filelib:wildcard(filename:join(Dir, "*"))],
    {ok, _} = application:ensure_all_started(tether),
    try
        %% 送った件数は呼び出し側で数える。セッションの中の
        %% カウンタは、プロセスを殺したり再起動したりすると消えるので
        %% (それ自体がこの筋書きの見せ場なので)ここでは使えない。
        Sent = scene_1() + scene_2() + scene_3() + scene_4() + scene_5()
             + summary(),
        report(Dir, Sent),
        local_first()
    after
        application:stop(tether)
    end.

%%%===================================================================

scene_1() ->
    title("1. 普通に書く"),
    say("口座を作る。既にあれば失敗する操作を使う。"),
    say("  cas(accounts/alice, 無いこと, <<\"100\">>)"),
    R = tether:request(<<"alice">>, 1, [{cas, ?ACCT, undefined, <<"100">>}]),
    result("client が受け取った答え", R),
    result("いま入っている値", tether:read(?ACCT)),
    1.

scene_2() ->
    title("2. 応答を受け取り損ねる"),
    say("ここが本題。ネットワークが切れて、client は答えを見ていない。"),
    say("入ったのか入っていないのか、client には分からない。"),
    say(""),
    say("Cassandra なら WriteTimeoutException。FoundationDB なら"),
    say("commit_unknown_result。どちらも「冪等に設計しろ」と言うだけ。"),
    0.

scene_3() ->
    title("3. そのまま送り直す(同じ通番)"),
    say("  tether:request(alice, seq=1, 同じ操作)"),
    R = tether:request(<<"alice">>, 1, [{cas, ?ACCT, undefined, <<"100">>}]),
    result("返ってきた答え", R),
    say(""),
    say("初回とまったく同じ答えが返った。"),
    say("**もし再実行されていたら、cas は conflict になるので必ず露見する。**"),
    say("実行されていないことが、答えの形そのもので分かる。"),
    #{served := Sv, deduped := D} = tether:session_info(<<"alice">>),
    say(io_lib:format("  セッションの記録: 実行 ~p 件 / 再送として吸収 ~p 件", [Sv, D])),
    1.

scene_4() ->
    title("4. セッションプロセスを殺す"),
    {ok, Pid} = tether_sessions:lookup(<<"alice">>),
    say(io_lib:format("  exit(~p, kill)", [Pid])),
    Ref = erlang:monitor(process, Pid),
    exit(Pid, kill),
    receive {'DOWN', Ref, _, _, _} -> ok after 1000 -> ok end,
    timer:sleep(50),
    result("名簿から消えたか", tether_sessions:lookup(<<"alice">>)),
    say("supervisor は作り直さない(restart => temporary)。"),
    say("暴走するクライアントが再起動の嵐を起こして、"),
    say("強度上限に触れて他のセッションを巻き込む、という事故を避けるため。"),
    say(""),
    say("この状態でもう一度、同じ通番で送る。"),
    R = tether:request(<<"alice">>, 1, [{cas, ?ACCT, undefined, <<"100">>}]),
    result("返ってきた答え", R),
    say("プロセスは作り直され、記憶はログから戻ってきた。"),
    1.

scene_5() ->
    title("5. ノードごと落として起動し直す"),
    say("  application:stop(tether) → ensure_all_started(tether)"),
    ok = application:stop(tether),
    {ok, _} = application:ensure_all_started(tether),
    R = tether:request(<<"alice">>, 1, [{cas, ?ACCT, undefined, <<"100">>}]),
    result("返ってきた答え", R),
    result("いま入っている値", tether:read(?ACCT)),
    say(""),
    say("再起動を越えても、同じ通番には同じ答えが返る。"),
    say("記憶はプロセスではなくログにあるので、プロセスが死んでも"),
    say("ノードが落ちても失われない。"),
    1.

summary() ->
    title("参考: 通番を進めれば、それは別の要求"),
    R = tether:request(<<"alice">>, 2, [{cas, ?ACCT, undefined, <<"100">>}]),
    result("seq=2 で同じ操作", R),
    say("今度はちゃんと実行され、既にあるので失敗した。"),
    say("「再送」と「別の要求」を取り違えていないことの確認。"),
    1.

report(Dir, Sent) ->
    title("まとめ"),
    {ok, _, Info} = tether_log:fold(Dir, fun(_, _, A) -> A end, []),
    Executed = maps:get(records, Info),
    num("client が送った要求", Sent),
    num("実際に実行された要求", Executed),
    num("再送として吸収された要求", Sent - Executed),
    num("ログに載ったレコード", Executed),
    say(""),
    say(io_lib:format("同じ操作を ~p 回投げて、実行されたのは ~p 回。",
                      [Sent, Executed])),
    say("残りは初回の答えをそのまま返しただけで、状態は動いていない。"),
    say("「入ったか分からない」が存在しない。"),
    io:format("~n"),
    ok.

%%%===================================================================
%%% 第二部 — 切断中も動ける
%%%===================================================================

local_first() ->
    title("6. 複製を受け取る — 購読"),
    _ = [tether:request(<<"shop">>, I, [{put, {<<"orders">>, <<I:32>>}, <<"old">>}])
         || I <- lists:seq(1, 3)],
    {ok, V0, Rows0} = tether:subscribe(<<"alice">>, <<"orders">>),
    result("版", V0),
    result("受け取った中身", length(Rows0)),
    say("クライアントは手元に複製を持った。以後は差分だけを受け取る。"),

    title("7. 圏外の間、セッションが変更を溜め続ける"),
    say("alice は圏外。しかし alice のプロセスはサーバで生きている。"),
    _ = [tether:request(<<"shop">>, I, [{put, {<<"orders">>, <<I:32>>}, <<"new">>}])
         || I <- lists:seq(4, 253)],
    ok = wait_version(<<"alice">>, 253),
    #{pending := P} = tether:session_info(<<"alice">>),
    result("溜まっている変更", P),
    say(""),
    say("**行にはこれができない。** 行は溜められない。"),
    say("接続に紐づける設計でもできない。切れたら消える。"),
    say("スレッドでは 10^6 クライアントぶん載らない。"),
    say(""),
    say("alice が復帰する。"),
    {delta, V1, Delta} = tether:sync(<<"alice">>),
    result("返ってきた差分", length(Delta)),
    result("版", V1),
    say("全件ではなく、見落とした分だけが返った。"),

    title("8. 溜めきれないときは、そのクライアントだけが取り直す"),
    say("上限を持たないと、戻ってこないクライアント1人がメモリを食い潰す。"),
    say("PostgreSQL の論理レプリケーションスロットが WAL を溜めて"),
    say("ディスクを埋める事故と同じ形で、あちらは個別に止められない。"),
    say(""),
    {ok, _, _} = tether:subscribe(<<"bob">>, <<"orders">>),
    say("alice は圏外のまま、bob はこまめに受け取る。変更を大量に起こす。"),
    _ = [begin
             tether:request(<<"shop">>, I, [{put, {<<"orders">>, <<I:32>>}, <<"x">>}]),
             case I rem 50 of 0 -> tether:sync(<<"bob">>); _ -> ok end
         end || I <- lists:seq(254, 253 + 12000)],
    ok = wait_version(<<"alice">>, 253 + 12000),
    ok = wait_version(<<"bob">>, 253 + 12000),
    result("alice: 溜めきれたか", not maps:get(overflow, tether:session_info(<<"alice">>))),
    result("bob:   溜めきれたか", not maps:get(overflow, tether:session_info(<<"bob">>))),
    case tether:sync(<<"alice">>) of
        {resync, _, [{_, Snap}]} ->
            result("alice → 取り直し(全件)", length(Snap));
        {delta, _, D} ->
            result("alice → 差分", length(D))
    end,
    {delta, _, BD} = tether:sync(<<"bob">>),
    result("bob   → 差分", length(BD)),
    say("**alice だけが取り直しに落ちた。bob は何も感じていない。**"),

    title("9. 圏外でも書ける — 預かり(escrow)"),
    say("読むだけなら差分で足りる。書くと、共有された有限資源が問題になる。"),
    {ok, 100} = tether:stock(<<"sku:1">>, 100),
    #{last_seq := L0} = tether:resume(<<"alice">>),
    {ok, [{granted, G, _}]} =
        tether:request(<<"alice">>, L0 + 1, [{acquire, <<"sku:1">>, 4, 600000}]),
    result("預かった量", G),
    say("この範囲なら、中央が空でも売れる。合計が在庫を超えることはない。"),
    say("CRDT は統合できるが在庫がマイナスになる。"),
    say("同期エンジンは competing write として弾く。"),
    {ok, R} = tether:request_batch(<<"alice">>, L0 + 2,
                [[{consume, <<"sku:1">>, 1}, {put, {<<"orders">>, <<"x1">>}, <<"c">>}],
                 [{consume, <<"sku:1">>, 1}, {put, {<<"orders">>, <<"x2">>}, <<"c">>}]]),
    result("圏外で積んだ2件", [element(1, X) || X <- R]),
    say("束は**最初の失敗で止まる**。在庫を確保できていないのに"),
    say("注文が確定する、という結果を作らないため。"),

    title("10. 端末を作り直した — resume"),
    #{last_seq := L, grants := Gr} = tether:resume(<<"alice">>),
    result("last_seq", L),
    result("いま有効な預かり", Gr),
    say("通番も預かりも返るので、そのまま圏外に戻れる。"),
    io:format("~n"),
    ok.

wait_version(C, V) -> wait_version(C, V, 400).
wait_version(_C, _V, 0) -> error(timeout);
wait_version(C, V, N) ->
    case tether:session_info(C) of
        #{version := Cur} when Cur >= V -> ok;
        _ -> timer:sleep(5), wait_version(C, V, N - 1)
    end.

num(Label, N) ->
    L = unicode:characters_to_binary(Label),
    io:format("      ~ts~s~p 件~n",
              [L, lists:duplicate(max(1, 28 - width(L)), $\s), N]).

%%%===================================================================

title(T) ->
    io:format("~n"), line(),
    io:format("  ~ts~n", [unicode:characters_to_binary(T)]),
    line().

line() -> io:format("  ~s~n", [lists:duplicate(66, $-)]).

say(S) -> io:format("    ~ts~n", [unicode:characters_to_binary(S)]).

result(Label, V) ->
    L = unicode:characters_to_binary(Label),
    Pad = lists:duplicate(max(1, 30 - width(L)), $\s),
    io:format("    ~ts~s~p~n", [L, Pad, V]).

%% 全角を2桁として数える。桁を揃えるためだけのもの。
width(B) ->
    lists:sum([case C of _ when C > 16#2E80 -> 2; _ -> 1 end
               || C <- unicode:characters_to_list(B)]).
