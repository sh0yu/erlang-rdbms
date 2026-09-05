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
%%% 接続(query_exec)はブラウザのセッションごとに1本持つ。タブを2つ開けば
%%% 接続が2本になり、片方でBEGINしたままにすると、もう片方が順番待ちで
%%% 止まるのが観察できる。トランザクションが直列に実行される様子が
%%% そのまま画面に出る。
%%% @end
%%%-------------------------------------------------------------------
-module(web_ui).

-export([start/0, start/1, start/2, stop/0, url/0]).
%% mod_esi から呼ばれる
-export([query/3, tables/3, session/3]).

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

%% CookieのセッションIDから接続を引く。無ければ作る。
conn_for(Env) ->
    Sid = session_id(Env),
    case ets:lookup(?SESSIONS, Sid) of
        [{Sid, Pid}] ->
            case is_process_alive(Pid) of
                true -> {Sid, Pid};
                false -> {Sid, new_conn(Sid)}
            end;
        [] ->
            {Sid, new_conn(Sid)}
    end.

new_conn(Sid) ->
    {ok, Pid} = gen_connection:connect(),
    ets:insert(?SESSIONS, {Sid, Pid}),
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

%% POST /api/web_ui:query   本文がSQL文そのもの
query(SessionID, Env, Input) ->
    {Sid, Conn} = conn_for(Env),
    Sql = string:trim(unicode:characters_to_list(list_to_binary(Input))),
    Result = query_exec:exec_query(Conn, {sql, strip_semicolon(Sql)}),
    deliver(SessionID, Sid, result_json(Sql, Result)).

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

deliver(SessionID, Sid, Body) ->
    mod_esi:deliver(SessionID,
                    ["Content-Type: application/json; charset=utf-8\r\n",
                     "Set-Cookie: sid=", Sid, "; Path=/; SameSite=Lax\r\n",
                     "Cache-Control: no-store\r\n",
                     "\r\n",
                     Body]).

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
%%% 最小限のJSON出力
%%%===================================================================

json_list(Items) ->
    "[" ++ lists:join(",", Items) ++ "]".

json_str(S) ->
    Chars = unicode:characters_to_list(iolist_to_binary(io_lib:format("~ts", [S]))),
    [$" | escape(Chars)] ++ [$"].

escape([]) -> [];
escape([$" | T]) -> [$\\, $" | escape(T)];
escape([$\\ | T]) -> [$\\, $\\ | escape(T)];
escape([$\n | T]) -> [$\\, $n | escape(T)];
escape([$\r | T]) -> [$\\, $r | escape(T)];
escape([$\t | T]) -> [$\\, $t | escape(T)];
escape([C | T]) when C < 16#20 ->
    lists:flatten(io_lib:format("\\u~4.16.0b", [C])) ++ escape(T);
escape([C | T]) -> [C | escape(T)].
