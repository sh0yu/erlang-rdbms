%%%-------------------------------------------------------------------
%%% 画面のAPIを HTTP 越しに叩く。
%%%
%%% JSONの組み立て・文字符号・経路(mod_esi)まで通して確かめる。
%%% ここを部品ごとに試すと、実際にブラウザから見たときだけ壊れる、
%%% という形の失敗を拾えない。実際そうなった。
%%%
%%%   * 日本語を含む本文を符号位置のまま渡してチャンクが壊れた
%%%   * クエリ文字列(?c=B)を付けると mod_esi の経路に乗らず404
%%%   * 結果JSONの外側の括弧を削りすぎて壊れた
%%%
%%% どれも部品の試験では出ない。
%%%-------------------------------------------------------------------
-module(web_ui_http_tests).

-include_lib("eunit/include/eunit.hrl").

-define(PORT, 8471).

web_ui_test_() ->
    {timeout, 60, {setup, fun start/0, fun stop/1,
                   fun(_) ->
                       [fun demos_are_served/0,
                        fun autocommit_and_plan/0,
                        fun index_changes_the_plan/0,
                        fun two_connections_are_separate/0,
                        fun engine_state_is_reported/0]
                   end}}.

start() ->
    Dir = db_test_helper:tmp_dir("webui"),
    {ok, _} = web_ui:start(?PORT, #{data_dir => Dir}),
    {ok, _} = application:ensure_all_started(inets),
    %% 接続 A / B はセッション(Cookie)ごとに持つ。Cookie を返さないと
    %% 1文ごとに別のセッションになり、トランザクションが続かない。
    ok = httpc:set_options([{cookies, enabled}]),
    Dir.

stop(Dir) ->
    ok = httpc:set_options([{cookies, disabled}]),
    web_ui:stop(),
    _ = application:stop(transaction_db),
    db_test_helper:rm_rf(Dir),
    ok.

%%%===================================================================

%% 台本が読めて、日本語が壊れずに届くこと。
demos_are_served() ->
    #{<<"demos">> := Demos} = get_json("/api/web_ui:demos"),
    ?assert(length(Demos) >= 5),
    [First | _] = Demos,
    #{<<"title">> := Title, <<"steps">> := Steps} = First,
    %% 日本語が化けていないこと(UTF-8として読める)
    ?assertNotEqual(error, unicode:characters_to_list(Title)),
    ?assert(length(Steps) > 0),
    #{<<"conn">> := C, <<"sql">> := Sql, <<"expect">> := E} = hd(Steps),
    ?assert(lists:member(C, [<<"A">>, <<"B">>])),
    ?assert(lists:member(E, [<<"ok">>, <<"error">>])),
    ?assertNotEqual(<<>>, Sql).

%% BEGIN 無しで書けること、SELECT に計画が付いてくること。
autocommit_and_plan() ->
    #{<<"result">> := #{<<"kind">> := <<"ok">>}} =
        run("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR(20))"),
    #{<<"result">> := #{<<"kind">> := <<"ok">>}} =
        run("INSERT INTO t VALUES (1, 'a')"),
    R = run("SELECT * FROM t WHERE name = 'a'"),
    #{<<"result">> := #{<<"kind">> := <<"rows">>, <<"count">> := 1}} = R,
    #{<<"plan">> := #{<<"lines">> := Lines}} = R,
    ?assert(lists:any(fun(L) -> contains(L, <<"Seq Scan on t">>) end, Lines)),
    %% DDL には計画が無い、と書いてあること(黙って空にしない)
    #{<<"plan">> := Plan} = run("CREATE TABLE u (a INTEGER)"),
    ?assertMatch(#{<<"note">> := _}, Plan).

%% **これが画面の眼目。** 索引を作ると同じ SELECT の計画が変わる。
index_changes_the_plan() ->
    ?assert(plan_has("SELECT * FROM t WHERE name = 'a'", <<"Seq Scan">>)),
    _ = run("CREATE INDEX t_name ON t (name)"),
    ?assert(plan_has("SELECT * FROM t WHERE name = 'a'", <<"Index Scan">>)).

%% 接続 A と B が別のトランザクションであること。
two_connections_are_separate() ->
    _ = run_on("B", "BEGIN"),
    R = run_on("B", "SELECT 1 FROM t WHERE id = 1"),
    #{<<"state">> := #{<<"conns">> := Conns}} = R,
    [A, B] = Conns,
    ?assertMatch(#{<<"id">> := <<"A">>, <<"in_tx">> := false}, A),
    ?assertMatch(#{<<"id">> := <<"B">>, <<"in_tx">> := true}, B),
    %% B だけが版を押さえている
    ?assertNotEqual(<<"none">>, maps:get(<<"snapshot">>, B)),
    _ = run_on("B", "ROLLBACK").

%% 右側に出す内部の値が揃っていること。
engine_state_is_reported() ->
    #{<<"state">> := St} = run("SELECT * FROM t"),
    #{<<"snapshot">> := Snap, <<"waiting">> := Waiting, <<"tables">> := Tables} = St,
    lists:foreach(fun(K) -> ?assert(maps:is_key(K, Snap)) end,
                  [<<"next">>, <<"undo">>, <<"snapshots">>, <<"applying">>]),
    ?assert(is_list(Waiting)),
    %% カタログは索引と統計の有無まで見せる。計画が変わる理由がこれ
    [T | _] = [X || X <- Tables, maps:get(<<"name">>, X) =:= <<"t">>],
    ?assertMatch(#{<<"indexed">> := [<<"name">>]}, T),
    ?assert(maps:is_key(<<"rows">>, T)).

%%%===================================================================

plan_has(Sql, Needle) ->
    case run(Sql) of
        #{<<"plan">> := #{<<"lines">> := Lines}} ->
            lists:any(fun(L) -> contains(L, Needle) end, Lines);
        _ ->
            false
    end.

contains(Hay, Needle) -> binary:match(Hay, Needle) =/= nomatch.

run(Sql) -> run_on("A", Sql).

run_on("A", Sql) -> post_json("/api/web_ui:query", Sql);
run_on("B", Sql) -> post_json("/api/web_ui:query_b", Sql).

get_json(Path) ->
    {ok, {{_, 200, _}, _, Body}} = httpc:request(url(Path)),
    decode(Body).

post_json(Path, Body) ->
    {ok, {{_, 200, _}, _, Resp}} =
        httpc:request(post, {url(Path), [], "text/plain", Body}, [], []),
    decode(Resp).

url(Path) -> "http://127.0.0.1:" ++ integer_to_list(?PORT) ++ Path.

%% OTP 25 には json モジュールが無いので、試験用の最小の読み手。
decode(Body) ->
    Bin = unicode:characters_to_binary(Body),
    {Value, _Rest} = jdec(skip_ws(Bin)),
    Value.

jdec(<<"{", R/binary>>) -> jobj(skip_ws(R), #{});
jdec(<<"[", R/binary>>) -> jarr(skip_ws(R), []);
jdec(<<"\"", R/binary>>) -> jstr(R, <<>>);
jdec(<<"true", R/binary>>) -> {true, R};
jdec(<<"false", R/binary>>) -> {false, R};
jdec(<<"null", R/binary>>) -> {null, R};
jdec(B) -> jnum(B, <<>>).

jobj(<<"}", R/binary>>, Acc) -> {Acc, R};
jobj(B, Acc) ->
    {K, R1} = jdec(skip_ws(B)),
    <<":", R2/binary>> = skip_ws(R1),
    {V, R3} = jdec(skip_ws(R2)),
    case skip_ws(R3) of
        <<",", R4/binary>> -> jobj(skip_ws(R4), Acc#{K => V});
        <<"}", R4/binary>> -> {Acc#{K => V}, R4}
    end.

jarr(<<"]", R/binary>>, Acc) -> {lists:reverse(Acc), R};
jarr(B, Acc) ->
    {V, R1} = jdec(skip_ws(B)),
    case skip_ws(R1) of
        <<",", R2/binary>> -> jarr(skip_ws(R2), [V | Acc]);
        <<"]", R2/binary>> -> {lists:reverse([V | Acc]), R2}
    end.

jstr(<<"\\", C, R/binary>>, Acc) -> jstr(R, <<Acc/binary, (unesc(C))>>);
jstr(<<"\"", R/binary>>, Acc) -> {Acc, R};
jstr(<<C, R/binary>>, Acc) -> jstr(R, <<Acc/binary, C>>).

unesc($n) -> $\n;
unesc($t) -> $\t;
unesc($r) -> $\r;
unesc(C)  -> C.

jnum(<<C, R/binary>>, Acc) when (C >= $0 andalso C =< $9); C =:= $-; C =:= $. ->
    jnum(R, <<Acc/binary, C>>);
jnum(B, Acc) ->
    S = binary_to_list(Acc),
    Num = case lists:member($., S) of
              true  -> list_to_float(S);
              false -> list_to_integer(S)
          end,
    {Num, B}.

skip_ws(<<C, R/binary>>) when C =:= $\s; C =:= $\n; C =:= $\t; C =:= $\r -> skip_ws(R);
skip_ws(B) -> B.
