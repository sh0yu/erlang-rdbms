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
        report(Dir, Sent)
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
