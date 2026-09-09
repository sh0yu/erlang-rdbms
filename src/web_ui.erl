%%%-------------------------------------------------------------------
%%% @doc
%%% ブラウザ用のSQLクライアント。
%%%
%%%   web_ui:start().        127.0.0.1:8080 で待ち受ける
%%%   web_ui:start(9000).    ポートを指定する
%%%   web_ui:stop().
%%%
%%% OTP同梱の inets httpd を使うので外部依存はない。
%%%
%%% == 何を見せる画面か ==
%%%
%%% SQLが動くだけの画面なら、DBの中で何が起きているかは分からない。
%%% ここでは1文流すたびに、
%%%
%%%   * その文の**実行計画**(プランナが何を選んだか)
%%%   * **エンジンの状態**(どの版を見ているか、undoが何件、誰が何をロック中か)
%%%
%%% を一緒に返す。索引を張ると Seq Scan が Index Scan に変わる、
%%% トランザクションを開くとスナップショットの版が固定される、といった
%%% 内部の動きが画面に出る。
%%%
%%% == 接続を2本持つ ==
%%%
%%% 並行制御は1本では見せられない。セッションごとに **A と B** の2本を
%%% 持ち、どちらで流すかを指定できるようにしてある。片方でBEGINしたまま
%%% 同じ行を書くと、もう片方が待たされる/断られるのがそのまま見える。
%%%
%%% 台本は priv/demos.sql。画面の左に並ぶ。
%%% @end
%%%-------------------------------------------------------------------
-module(web_ui).

-export([start/0, start/1, start/2, stop/0, url/0]).
%% mod_esi から呼ばれる
-export([query/3, query_b/3, tables/3, session/3, demos/3, plan/3, plan_b/3]).
%% 台本の検査(試験から使う)
-export([parsed_demos/0]).

-include("../include/catalog.hrl").

-define(DEFAULT_PORT, 8080).
-define(SESSIONS, web_ui_sessions).

%%%===================================================================
%%% 起動
%%%===================================================================

start() ->
    start(?DEFAULT_PORT).

start(Port) ->
    start(Port, #{}).

start(Port, Opts) ->
    ok = ensure_db(Opts),
    ok = ensure_sessions(),
    {ok, _} = application:ensure_all_started(inets),
    DocRoot = doc_root(),
    case inets:start(httpd, [
            {port, Port},
            {bind_address, {127, 0, 0, 1}},
            {server_name, "transaction_db"},
            {server_root, DocRoot},
            {document_root, DocRoot},
            {directory_index, ["index.html"]},
            %% ログ用のモジュールを外しておく。server_root配下に
            %% ログディレクトリを作らずに済む。
            {modules, [mod_alias, mod_esi, mod_get, mod_head]},
            {erl_script_alias, {"/api", [?MODULE]}},
            {mime_types, [{"html", "text/html"},
                          {"css", "text/css"},
                          {"js", "application/javascript"},
                          {"ico", "image/x-icon"}]}]) of
        {ok, Pid} ->
            io:format("~ntransaction_db web UI: ~ts~n", [url(Port)]),
            io:format("stop with web_ui:stop().~n~n"),
            {ok, Pid};
        {error, {already_started, _}} = E ->
            io:format("port ~p is already in use~n", [Port]),
            E;
        {error, Reason} = E ->
            %% 黙って戻ると、呼び出し側が待ち続けて
            %% 「起動したのに繋がらない」という分かりにくい状態になる
            io:format("cannot start web UI on port ~p: ~p~n", [Port, Reason]),
            E
    end.

stop() ->
    lists:foreach(fun({httpd, Pid}) -> inets:stop(httpd, Pid);
                     (_) -> ok
                  end, inets:services()),
    ok.

url() -> url(?DEFAULT_PORT).
url(Port) -> "http://127.0.0.1:" ++ integer_to_list(Port) ++ "/".

doc_root() ->
    case code:priv_dir(transaction_db) of
        {error, _} -> "priv/www";
        Priv -> filename:join(Priv, "www")
    end.

ensure_db(Opts) ->
    %% **load を先に呼ぶ。** application:load/1 は .app の env で
    %% それまでの set_env を上書きするので、順序を逆にすると
    %% data_dir の指定が黙って捨てられ、既定の ./data が使われる。
    _ = application:load(transaction_db),
    case maps:get(data_dir, Opts, undefined) of
        undefined -> ok;
        Dir -> application:set_env(transaction_db, data_dir, Dir)
    end,
    case application:ensure_all_started(transaction_db) of
        {ok, _} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%%===================================================================
%%% セッション(ブラウザ1つにつき接続1本)
%%%===================================================================

ensure_sessions() ->
    case ets:info(?SESSIONS) of
        undefined ->
            %% httpdのワーカプロセスから触るので public にする。
            %% 所有者はこの呼び出し元(通常はシェル)。
            ?SESSIONS = ets:new(?SESSIONS, [set, named_table, public]),
            ok;
        _ ->
            ok
    end.

%% CookieのセッションIDと接続名(A/B)から接続を引く。無ければ作る。
conn_for(Env) ->
    conn_for(Env, conn_name(Env)).

conn_for(Env, Name) ->
    Sid = session_id(Env),
    Key = {Sid, Name},
    case ets:lookup(?SESSIONS, Key) of
        [{Key, Pid}] ->
            case is_process_alive(Pid) of
                true  -> {Sid, Pid};
                false -> {Sid, new_conn(Key)}
            end;
        [] ->
            {Sid, new_conn(Key)}
    end.

%% 接続名は**呼ぶ関数**で決まる。
%%
%% mod_esi は /api/Module:Function という形しか見ないので、
%% ?c=B のようなクエリ文字列を付けると経路に乗らない(404になる)。
%% よって接続ごとに入口を分ける。
conn_name(_Env) -> "A".

new_conn(Key) ->
    {ok, Pid} = gen_connection:connect(),
    ets:insert(?SESSIONS, {Key, Pid}),
    Pid.

session_id(Env) ->
    case cookie_value("sid", proplists:get_value(http_cookie, Env, "")) of
        undefined -> new_session_id();
        Sid -> Sid
    end.

new_session_id() ->
    integer_to_list(erlang:unique_integer([positive, monotonic])).

cookie_value(Name, Cookies) ->
    Pairs = [string:trim(P) || P <- string:split(Cookies, ";", all)],
    find_cookie(Name, Pairs).

find_cookie(_Name, []) -> undefined;
find_cookie(Name, [P | T]) ->
    case string:split(P, "=") of
        [Name, V] -> V;
        _ -> find_cookie(Name, T)
    end.

%%%===================================================================
%%% API
%%%===================================================================

%%----------------------------------------------------------------------
%% POST /api/web_ui:query     本文がSQL文そのもの。接続 A
%% POST /api/web_ui:query_b   同上。接続 B
%%
%% 1文につき、結果・実行計画・エンジンの状態をまとめて返す。
%% 別々に取りに行くと、その間に状態が変わって画面が食い違う。
%%----------------------------------------------------------------------
query(SessionID, Env, Input) -> do_query(SessionID, Env, Input, "A").
query_b(SessionID, Env, Input) -> do_query(SessionID, Env, Input, "B").

do_query(SessionID, Env, Input, Name) ->
    {Sid, Conn} = conn_for(Env, Name),
    Sql = strip_semicolon(string:trim(unicode:characters_to_list(list_to_binary(Input)))),
    %% 計画は**実行の前**に採る。実行後だと、その文が変えた後の状態で
    %% 立てた計画になり、実際に走ったものと食い違う。
    Plan = plan_of(Conn, Sql),
    Result = query_exec:exec_query(Conn, {sql, Sql}),
    Body = "{\"conn\":" ++ json_str(Name)
        ++ "," ++ strip_braces(result_json(Sql, Result))
        ++ ",\"plan\":" ++ Plan
        ++ ",\"state\":" ++ state_json(Sid)
        ++ "}",
    deliver(SessionID, Sid, Body).

%% POST /api/web_ui:plan   実行せずに計画だけ見る
plan(SessionID, Env, Input) -> do_plan(SessionID, Env, Input, "A").
plan_b(SessionID, Env, Input) -> do_plan(SessionID, Env, Input, "B").

do_plan(SessionID, Env, Input, Name) ->
    {Sid, Conn} = conn_for(Env, Name),
    Sql = strip_semicolon(string:trim(unicode:characters_to_list(list_to_binary(Input)))),
    deliver(SessionID, Sid, "{\"plan\":" ++ plan_of(Conn, Sql) ++ "}").

%% GET /api/web_ui:demos   台本(priv/demos.sql)
demos(SessionID, Env, _Input) ->
    {Sid, _Conn} = conn_for(Env),
    deliver(SessionID, Sid, "{\"demos\":" ++ json_list(demo_json()) ++ "}").

%%----------------------------------------------------------------------
%% その文の実行計画。
%%
%% EXPLAIN はデータに触らないので、トランザクションの外でも動くし、
%% いまのトランザクションの状態も変えない。
%%
%% EXPLAIN が受けるのは SELECT だけ。理由まで画面に出す。
%% UPDATE / DELETE がプランナを通らないのは本当のことなので、
%% 「計画がありません」ではなく、そう書く。
%%----------------------------------------------------------------------
plan_of(Conn, Sql) ->
    case classify_sql(Sql) of
        select ->
            case query_exec:exec_query(Conn, {sql, "EXPLAIN " ++ Sql}) of
                {ok, _Cols, Rows} ->
                    "{\"lines\":"
                        ++ json_list([json_str(L) || [L] <- Rows]) ++ "}";
                {error, Reason} ->
                    "{\"note\":" ++ json_str(sql_shell:format_error(Reason)) ++ "}";
                _ ->
                    "{\"note\":\"計画を立てられませんでした\"}"
            end;
        explain ->
            "{\"note\":\"この文自体が計画の表示です\"}";
        write ->
            "{\"note\":\"UPDATE / DELETE / INSERT はプランナを通りません。"
            "対象行の絞り込みは全表走査で行います\"}";
        ddl ->
            "{\"note\":\"DDL に実行計画はありません\"}";
        tx ->
            "{\"note\":\"トランザクション制御に実行計画はありません\"}";
        other ->
            "{\"note\":\"実行計画はありません\"}"
    end.

classify_sql(Sql) ->
    case string:uppercase(string:trim(Sql)) of
        "SELECT" ++ _  -> select;
        "("      ++ _  -> select;
        "EXPLAIN" ++ _ -> explain;
        "INSERT" ++ _  -> write;
        "UPDATE" ++ _  -> write;
        "DELETE" ++ _  -> write;
        "CREATE" ++ _  -> ddl;
        "DROP"   ++ _  -> ddl;
        "ANALYZE" ++ _ -> ddl;
        "BEGIN"  ++ _  -> tx;
        "COMMIT" ++ _  -> tx;
        "ROLLBACK" ++ _ -> tx;
        _              -> other
    end.

%%----------------------------------------------------------------------
%% エンジンの状態。1文ごとにこれを返して画面の右に出す。
%%
%% 「いま自分がどの版を見ているか」「undo がいくつ残っているか」
%% 「誰が何を待っているか」が、SQLの結果とは別に見えないと、
%% 並行制御は理解のしようがない。
%%----------------------------------------------------------------------
state_json(Sid) ->
    "{\"conns\":" ++ json_list([conn_json(Sid, N) || N <- ["A", "B"]])
        ++ ",\"snapshot\":" ++ snapshot_json()
        ++ ",\"waiting\":" ++ waiting_json()
        ++ ",\"tables\":" ++ json_list(table_state_json())
        ++ "}".

conn_json(Sid, Name) ->
    case ets:lookup(?SESSIONS, {Sid, Name}) of
        [{_, Pid}] ->
            case is_process_alive(Pid) andalso query_exec:status(Pid) of
                false ->
                    "{\"id\":" ++ json_str(Name) ++ ",\"open\":false}";
                St ->
                    Txid = maps:get(txid, St),
                    "{\"id\":" ++ json_str(Name)
                        ++ ",\"open\":true"
                        ++ ",\"pid\":" ++ json_str(pid_to_list(Pid))
                        ++ ",\"in_tx\":" ++ atom_to_list(maps:get(in_transaction, St))
                        ++ ",\"isolation\":" ++ json_str(atom_to_list(maps:get(isolation, St)))
                        ++ ",\"snapshot\":" ++ json_str(io_lib:format("~p", [maps:get(snapshot, St)]))
                        ++ ",\"statements\":" ++ integer_to_list(maps:get(statements, St))
                        ++ ",\"locks\":" ++ json_list(locks_of(Txid))
                        ++ "}"
            end;
        [] ->
            "{\"id\":" ++ json_str(Name) ++ ",\"open\":false}"
    end.

locks_of(undefined) -> [];
locks_of(readonly)  -> [];
locks_of(Txid) ->
    [json_str(io_lib:format("~p", [Oid]))
     || Oid <- lists:sublist(lock_mng:locks_held_by(whereis(lock_mng), Txid), 12)].

snapshot_json() ->
    #{next := Next, undo := Undo, snapshots := Snaps,
      applying := Applying, discarded := Disc} = snapshot_mng:status(),
    "{\"next\":" ++ integer_to_list(Next)
        ++ ",\"undo\":" ++ integer_to_list(Undo)
        ++ ",\"snapshots\":" ++ integer_to_list(Snaps)
        ++ ",\"applying\":" ++ integer_to_list(Applying)
        ++ ",\"discarded\":" ++ integer_to_list(Disc) ++ "}".

%% 誰が誰を待っているか。空なら誰も待っていない。
waiting_json() ->
    Graph = lock_mng:wait_for_graph(whereis(lock_mng)),
    json_list([json_str(io_lib:format("~p が ~p を待っている", [W, H]))
               || {W, Hs} <- maps:to_list(Graph), H <- lists:usort(Hs)]).

%% プランナが見ているカタログ。索引と統計の有無で計画が変わる。
table_state_json() ->
    Sys = whereis(sys_tbl_mng),
    {ok, Names} = sys_tbl_mng:list_tables(Sys),
    [begin
         Indexed = case sys_tbl_mng:get_index_column_list(Sys, N) of
                       {ok, Cols} -> Cols;
                       _          -> []
                   end,
         Rows = case sys_tbl_mng:get_stats(Sys, N) of
                    {ok, S} -> integer_to_list(sql_stats:rows(S));
                    none    -> "null"
                end,
         "{\"name\":" ++ json_str(atom_to_list(N))
             ++ ",\"indexed\":" ++ json_list([json_str(atom_to_list(C)) || C <- Indexed])
             ++ ",\"rows\":" ++ Rows ++ "}"
     end || N <- Names].

%% result_json/2 は {"sql":..,"result":..} を返す。外側の {} を**1つだけ**
%% 外して、他の項目と混ぜられるようにする。
%%
%% string:trim/3 で削ると、末尾に並んだ } を全部落としてしまい、
%% result の閉じ括弧まで消える。
strip_braces(S) ->
    case lists:flatten(S) of
        [${ | Rest] -> lists:droplast(Rest);
        Other       -> Other
    end.

%% GET /api/web_ui:tables   テーブルと列定義の一覧
tables(SessionID, Env, _Input) ->
    {Sid, _Conn} = conn_for(Env),
    {ok, Names} = sys_tbl_mng:list_tables(whereis(sys_tbl_mng)),
    Json = json_list([table_json(N) || N <- Names]),
    deliver(SessionID, Sid, "{\"tables\":" ++ Json ++ "}").

%% GET /api/web_ui:session  いまの接続の状態
session(SessionID, Env, _Input) ->
    {Sid, Conn} = conn_for(Env),
    %% グローバルに誰かがトランザクション中かではなく、
    %% **この接続が**開いているかを見る
    #{in_transaction := InTx} = query_exec:status(Conn),
    Json = "{\"sid\":" ++ json_str(Sid)
        ++ ",\"conn\":" ++ json_str(pid_to_list(Conn))
        ++ ",\"tx_active\":" ++ atom_to_list(InTx) ++ "}",
    deliver(SessionID, Sid, Json).

table_json(Name) ->
    {ok, Columns} = sys_tbl_mng:get_columns(whereis(sys_tbl_mng), Name),
    Cols = [ "{\"name\":" ++ json_str(atom_to_list(C#column.name))
             ++ ",\"type\":" ++ json_str(sql_type:name(C#column.type)) ++ "}"
             || C <- Columns],
    "{\"name\":" ++ json_str(atom_to_list(Name))
        ++ ",\"columns\":" ++ json_list(Cols) ++ "}".

%% 本文は**UTF-8のバイト列**にしてから渡す。
%%
%% JSONの組み立ては符号位置の並び(日本語は255を超える)なので、そのまま
%% 渡すとチャンクの長さが合わず、ブラウザ側で壊れる。
deliver(SessionID, Sid, Body) ->
    mod_esi:deliver(SessionID,
                    ["Content-Type: application/json; charset=utf-8\r\n",
                     "Set-Cookie: sid=", Sid, "; Path=/; SameSite=Lax\r\n",
                     "Cache-Control: no-store\r\n",
                     "\r\n"]),
    mod_esi:deliver(SessionID, [unicode:characters_to_binary(Body)]).

strip_semicolon(Sql) ->
    string:trim(string:trim(string:trim(Sql), trailing, ";")).

%%%===================================================================
%%% 結果のJSON化
%%%===================================================================

result_json(Sql, {ok, Columns, Rows}) ->
    ok_json(Sql, "{\"kind\":\"rows\""
            ++ ",\"columns\":" ++ json_list([json_str(atom_to_list(C)) || C <- Columns])
            ++ ",\"rows\":" ++ json_list([json_list([cell(V) || V <- R]) || R <- Rows])
            ++ ",\"count\":" ++ integer_to_list(length(Rows)) ++ "}");
result_json(Sql, ok) ->
    ok_json(Sql, "{\"kind\":\"ok\",\"message\":\"OK\"}");
result_json(Sql, {ok, N}) when is_integer(N) ->
    ok_json(Sql, "{\"kind\":\"ok\",\"message\":" ++ json_str(io_lib:format("OK (~p rows affected)", [N])) ++ "}");
result_json(Sql, {ok, Oid}) ->
    ok_json(Sql, "{\"kind\":\"ok\",\"message\":"
            ++ json_str(io_lib:format("OK (1 row inserted, oid=~p)", [Oid])) ++ "}");
result_json(Sql, transaction_not_found) ->
    err_json(Sql, "no transaction in progress (use BEGIN)");
result_json(Sql, {error, Reason}) ->
    err_json(Sql, sql_shell:format_error(Reason));
result_json(Sql, Other) ->
    err_json(Sql, io_lib:format("~p", [Other])).

ok_json(Sql, Body) ->
    "{\"sql\":" ++ json_str(Sql) ++ ",\"result\":" ++ Body ++ "}".

err_json(Sql, Msg) ->
    "{\"sql\":" ++ json_str(Sql)
        ++ ",\"result\":{\"kind\":\"error\",\"message\":" ++ json_str(Msg) ++ "}}".

%% 値はJSONの型に落とさず、表示用の文字列と種別で返す。
%% NULLと文字列 "NULL" を画面で区別するため。
cell(null) -> "{\"t\":\"null\",\"v\":\"NULL\"}";
cell(V) when is_boolean(V) -> "{\"t\":\"bool\",\"v\":" ++ json_str(atom_to_list(V)) ++ "}";
cell(V) when is_integer(V) -> "{\"t\":\"num\",\"v\":" ++ json_str(integer_to_list(V)) ++ "}";
cell(V) when is_float(V) -> "{\"t\":\"num\",\"v\":" ++ json_str(io_lib:format("~p", [V])) ++ "}";
cell(V) when is_binary(V) -> "{\"t\":\"str\",\"v\":" ++ json_str(unicode:characters_to_list(V)) ++ "}";
cell(V) when is_atom(V) -> "{\"t\":\"atom\",\"v\":" ++ json_str(atom_to_list(V)) ++ "}";
cell(V) -> "{\"t\":\"term\",\"v\":" ++ json_str(io_lib:format("~p", [V])) ++ "}".

%%%===================================================================
%%% 台本(priv/demos.sql)の読み込み
%%%
%%% 書式:
%%%   === タイトル     新しいデモ
%%%   --- 説明         そのデモの説明(複数行)
%%%   -- 手順の説明     次の1文の説明
%%%   @A / @B          その文を流す接続(省略すると A)
%%%   !error           次の1文は**失敗するのが正しい**
%%%   SQL;             ; までが1文
%%%
%%% !error があるのは、断られること自体が見せたい動き(主キーの重複、
%%% 直列化の失敗)だからで、試験もそれを期待値として使う。
%%%
%%% 台本をコードに埋めずファイルにしてあるのは、書き足すのに
%%% 再コンパイルが要らないようにするため。
%%%===================================================================

demo_json() ->
    [demo_to_json(D) || D <- parsed_demos()].

%% @doc 台本を読んで組にする。試験がそのまま流して確かめる。
-spec parsed_demos() -> [{string(), [string()], [{string(), string(), string(), ok | error}]}].
parsed_demos() ->
    parse_demos(demo_lines()).

demo_lines() ->
    Path = filename:join(demo_dir(), "demos.sql"),
    case file:read_file(Path) of
        {ok, Bin} -> string:split(unicode:characters_to_list(Bin), "\n", all);
        {error, _} -> []
    end.

demo_dir() ->
    case code:priv_dir(transaction_db) of
        {error, _} -> "priv";
        Priv       -> Priv
    end.

parse_demos(Lines) ->
    lists:reverse(collect(Lines, undefined, [])).

%% Cur = {Title, [説明], [手順], 次の説明, 接続, 期待, 組み立て中のSQL}
collect([], Cur, Acc) ->
    close_demo(Cur, Acc);
collect([Line | T], Cur, Acc) ->
    case demo_line(string:trim(Line, trailing)) of
        {title, Title} ->
            collect(T, {Title, [], [], [], "A", ok, []}, close_demo(Cur, Acc));
        _ when Cur =:= undefined ->
            collect(T, Cur, Acc);
        {about, Text} ->
            {Ti, Ab, St, Nt, C, Ex, Sql} = Cur,
            collect(T, {Ti, Ab ++ [Text], St, Nt, C, Ex, Sql}, Acc);
        {note, Text} ->
            {Ti, Ab, St, _Nt, _C, _Ex, Sql} = Cur,
            %% 説明は次の1文につく。接続と期待はここで戻す
            collect(T, {Ti, Ab, St, Text, "A", ok, Sql}, Acc);
        {conn, Name, Rest} ->
            {Ti, Ab, St, Nt, _C, Ex, Sql} = Cur,
            collect([Rest | T], {Ti, Ab, St, Nt, Name, Ex, Sql}, Acc);
        {expect, Ex} ->
            {Ti, Ab, St, Nt, C, _Ex, Sql} = Cur,
            collect(T, {Ti, Ab, St, Nt, C, Ex, Sql}, Acc);
        {sql, Text} ->
            {Ti, Ab, St, Nt, C, Ex, Sql} = Cur,
            Sql2 = Sql ++ [Text],
            case lists:suffix(";", string:trim(Text)) of
                true ->
                    Step = {C, Nt, string:trim(string:join(Sql2, " ")), Ex},
                    collect(T, {Ti, Ab, St ++ [Step], [], "A", ok, []}, Acc);
                false ->
                    collect(T, {Ti, Ab, St, Nt, C, Ex, Sql2}, Acc)
            end;
        skip ->
            collect(T, Cur, Acc)
    end.

close_demo(undefined, Acc) -> Acc;
close_demo({_Ti, _Ab, [], _, _, _, _}, Acc) -> Acc;
close_demo({Ti, Ab, St, _, _, _, _}, Acc) -> [{Ti, Ab, St} | Acc].

demo_line("=== " ++ Title) -> {title, string:trim(Title)};
demo_line("--- " ++ Text)  -> {about, string:trim(Text)};
demo_line("-- " ++ Text)   -> {note, string:trim(Text)};
demo_line("@A " ++ Rest)   -> {conn, "A", Rest};
demo_line("@B " ++ Rest)   -> {conn, "B", Rest};
demo_line("!error")        -> {expect, error};
demo_line("#" ++ _)        -> skip;
demo_line("")              -> skip;
demo_line(Text)            -> {sql, Text}.

demo_to_json({Title, About, Steps}) ->
    "{\"title\":" ++ json_str(Title)
        ++ ",\"about\":" ++ json_str(string:join(About, "\n"))
        ++ ",\"steps\":" ++ json_list([step_to_json(S) || S <- Steps])
        ++ "}".

step_to_json({Conn, Note, Sql, Expect}) ->
    "{\"conn\":" ++ json_str(Conn)
        ++ ",\"note\":" ++ json_str(Note)
        ++ ",\"expect\":" ++ json_str(atom_to_list(Expect))
        ++ ",\"sql\":" ++ json_str(Sql) ++ "}".

%%%===================================================================
%%% 最小限のJSON出力
%%%===================================================================

json_list(Items) ->
    "[" ++ lists:join(",", Items) ++ "]".

%% 日本語を含むので、iolist_to_binary は使えない(符号位置が255を超える)。
%% ~ts で組んで平らにすれば、そのまま符号位置の並びになる。
json_str(S) ->
    [$" | escape(lists:flatten(io_lib:format("~ts", [S])))] ++ [$"].

escape([]) -> [];
escape([$" | T]) -> [$\\, $" | escape(T)];
escape([$\\ | T]) -> [$\\, $\\ | escape(T)];
escape([$\n | T]) -> [$\\, $n | escape(T)];
escape([$\r | T]) -> [$\\, $r | escape(T)];
escape([$\t | T]) -> [$\\, $t | escape(T)];
escape([C | T]) when C < 16#20 ->
    lists:flatten(io_lib:format("\\u~4.16.0b", [C])) ++ escape(T);
escape([C | T]) -> [C | escape(T)].
