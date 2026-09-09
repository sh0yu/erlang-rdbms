%%%-------------------------------------------------------------------
%%% @doc
%%% 対話SQLシェル。psql風の端末クライアント。
%%%
%%%   sql_shell:start().            対話シェルを開く
%%%   sql_shell:run_file("t.sql").  ファイルの文を順に実行する
%%%   sql_shell:tour().             一通りの機能をなぞるデモ
%%%
%%% 文は `;` で区切る。空行でも溜まっている文を実行する。
%%% `\` で始まる行はメタコマンド(`\?` で一覧)。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_shell).

-export([start/0, start/1, run_file/1, run_file/2, tour/0, tour/1]).
%% テストから使う純粋な補助関数
-export([split_statements/1, render/1, format_error/1]).

-include("../include/catalog.hrl").

-define(PROMPT, "sql> ").
-define(CONT_PROMPT, "  -> ").
-define(MAX_COL_WIDTH, 100).   %% 実行計画の行が入る幅にしてある

-record(sh, {conn, timing = false}).

%%%===================================================================
%%% 起動
%%%===================================================================

start() ->
    start(#{}).

start(Opts) ->
    ok = ensure_db(Opts),
    {ok, Conn} = gen_connection:connect(),
    banner(),
    try loop(#sh{conn = Conn}, [])
    after shutdown()
    end.

%%----------------------------------------------------------------------
%% @doc ファイルの文を順に実行して、入力と結果の両方を表示する。
%%----------------------------------------------------------------------
run_file(Path) ->
    run_file(Path, #{}).

run_file(Path, Opts) ->
    ok = ensure_db(Opts),
    {ok, Conn} = gen_connection:connect(),
    try
        case file:read_file(Path) of
            {ok, Bin} ->
                run_statements(#sh{conn = Conn}, split_statements(binary_to_list(Bin)));
            {error, Reason} ->
                io:format("cannot read ~ts: ~p~n", [Path, Reason]),
                {error, Reason}
        end
    after
        shutdown()
    end.

%% 終了時にアプリケーションを止める。
%%
%% 止めずに halt/0 すると DETS と disk_log が閉じられないままになり、
%% 次の起動で毎回 "not properly closed, repairing" が走る。
%% データは失われないが、テーブルが大きいと修復に時間がかかる。
shutdown() ->
    _ = application:stop(transaction_db),
    ok.

%%----------------------------------------------------------------------
%% @doc 同梱のツアー(priv/tour.sql)を実行する。
%%----------------------------------------------------------------------
tour() ->
    tour(#{}).

tour(Opts) ->
    run_file(tour_path(), Opts).

tour_path() ->
    case code:priv_dir(transaction_db) of
        {error, _} -> "priv/tour.sql";
        Priv -> filename:join(Priv, "tour.sql")
    end.

%% data_dir を指定できるようにしておくと、試すたびに
%% 前回のデータを引きずらずに済む。
ensure_db(Opts) ->
    %% load を先に済ませること。application:load/1 は .app の env を
    %% 読み込むので、未 load の状態で set_env しても上書きされて
    %% 既定値に戻る。VM内で最初に起動したときだけ data_dir が
    %% 無視される、という再現しにくい形で出る。
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
%%% 対話ループ
%%%===================================================================

loop(Sh, Buffer) ->
    Prompt = case Buffer of [] -> ?PROMPT; _ -> ?CONT_PROMPT end,
    case io:get_line(Prompt) of
        eof ->
            io:format("~n"),
            ok;
        {error, Reason} ->
            io:format("input error: ~p~n", [Reason]),
            ok;
        Line ->
            handle_line(Sh, Buffer, string:trim(Line, trailing, "\n"))
    end.

handle_line(Sh, [], "") ->
    loop(Sh, []);
handle_line(Sh, Buffer, Line) ->
    case {Buffer, string:trim(Line)} of
        %% メタコマンドは文の途中でないときだけ受ける
        {[], "\\" ++ Cmd} ->
            case meta(Sh, string:trim(Cmd)) of
                quit -> io:format("bye~n"), ok;
                Sh2 -> loop(Sh2, [])
            end;
        %% 空行は溜まっている文を実行する
        {_, ""} ->
            Sh2 = flush(Sh, Buffer),
            loop(Sh2, []);
        _ ->
            Buffer2 = Buffer ++ [Line],
            case lists:suffix(";", string:trim(Line)) of
                true -> loop(flush(Sh, Buffer2), []);
                false -> loop(Sh, Buffer2)
            end
    end.

flush(Sh, []) ->
    Sh;
flush(Sh, Buffer) ->
    Sql = string:trim(lists:flatten(lists:join(" ", Buffer))),
    case Sql of
        "" -> Sh;
        _ -> execute(Sh, Sql)
    end.

%%%===================================================================
%%% メタコマンド
%%%===================================================================

meta(_Sh, "q") -> quit;
meta(_Sh, "quit") -> quit;
meta(Sh, "?") -> help(), Sh;
meta(Sh, "h") -> help(), Sh;
meta(Sh, "d") -> list_tables(), Sh;
meta(Sh, "dt") -> list_tables(), Sh;
meta(Sh, "d " ++ Table) -> describe(string:trim(Table)), Sh;
meta(Sh, "di") -> list_indexes(), Sh;
meta(#sh{timing = T} = Sh, "timing") ->
    io:format("timing is ~ts~n", [case T of true -> "off"; false -> "on" end]),
    Sh#sh{timing = not T};
meta(Sh, Other) ->
    io:format("unknown command: \\~ts  (\\? for help)~n", [Other]),
    Sh.

help() ->
    io:format(
      "~n"
      "  \\?  \\h        this help~n"
      "  \\d            list tables~n"
      "  \\d NAME       describe a table~n"
      "  \\timing       toggle query timing~n"
      "  \\q            quit~n"
      "~n"
      "  Statements end with ';'. A blank line also runs the buffer.~n"
      "~n"
      "  CREATE TABLE t (a VARCHAR, b INTEGER, c BOOLEAN);~n"
      "  DROP TABLE t;~n"
      "  BEGIN;  COMMIT;  ROLLBACK;~n"
      "  INSERT INTO t [(cols)] VALUES (...);~n"
      "  SELECT * | cols FROM t [WHERE col = value];~n"
      "  UPDATE t SET col = value, ... [WHERE col = value];~n"
      "  DELETE FROM t [WHERE col = value];~n"
      "~n"
      "  Types: INTEGER FLOAT VARCHAR BOOLEAN.  NULL, TRUE, FALSE are literals.~n"
      "  DML needs a transaction: BEGIN first, then COMMIT or ROLLBACK.~n"
      "~n").

list_tables() ->
    case sys_tbl_mng:list_tables(whereis(sys_tbl_mng)) of
        {ok, []} -> io:format("no tables~n");
        {ok, Tables} -> print_table(["table"], [[T] || T <- Tables])
    end.

%% 名前はアトムのことも文字列のこともある
name_str(N) when is_atom(N) -> atom_to_list(N);
name_str(N)                 -> N.

list_indexes() ->
    case sys_tbl_mng:list_indexes(whereis(sys_tbl_mng)) of
        {ok, []} ->
            io:format("no indexes~n");
        {ok, Indexes} ->
            print_table(["index", "table", "column"],
                        [[N, T, C] || {index, N, T, C} <- Indexes])
    end.

describe(Name) ->
    case catch list_to_existing_atom(Name) of
        {'EXIT', _} ->
            io:format("no such table: ~ts~n", [Name]);
        Table ->
            case sys_tbl_mng:get_columns(whereis(sys_tbl_mng), Table) of
                {error, table_not_found} ->
                    io:format("no such table: ~ts~n", [Name]);
                {ok, Columns} ->
                    %% 型名は値ではなくメタデータなので、
                    %% render/1 の項表示(引用符つき)を通さずbinaryで渡す
                    print_table(["column", "type"],
                                [[C#column.name, list_to_binary(sql_type:name(C#column.type))]
                                 || C <- Columns])
            end
    end.

%%%===================================================================
%%% 実行と表示
%%%===================================================================

run_statements(_Sh, []) ->
    io:format("~n"),
    ok;
run_statements(Sh, [Sql | Rest]) ->
    io:format("~n~ts~ts~n", [?PROMPT, Sql]),
    Sh2 = execute(Sh, Sql),
    run_statements(Sh2, Rest).

execute(#sh{conn = Conn, timing = Timing} = Sh, Sql) ->
    T0 = erlang:monotonic_time(microsecond),
    Result = query_exec:exec_query(Conn, {sql, strip_semicolon(Sql)}),
    T1 = erlang:monotonic_time(microsecond),
    print_result(Result),
    case Timing of
        true -> io:format("Time: ~.3f ms~n", [(T1 - T0) / 1000]);
        false -> ok
    end,
    Sh.

strip_semicolon(Sql) ->
    string:trim(string:trim(string:trim(Sql), trailing, ";")).

print_result({ok, Columns, Rows}) ->
    print_table([atom_to_list(C) || C <- Columns], Rows),
    io:format("(~p row~ts)~n", [length(Rows), plural(length(Rows))]);
print_result(ok) ->
    io:format("OK~n");
print_result({ok, N}) when is_integer(N) ->
    io:format("OK (~p row~ts affected)~n", [N, plural(N)]);
print_result({ok, Oid}) ->
    io:format("OK (1 row inserted, oid=~p)~n", [Oid]);
print_result(transaction_not_found) ->
    io:format("ERROR: no transaction in progress (use BEGIN)~n");
print_result({error, Reason}) ->
    io:format("ERROR: ~ts~n", [format_error(Reason)]);
print_result(Other) ->
    io:format("~p~n", [Other]).

plural(1) -> "";
plural(_) -> "s".

format_error({syntax_error, Line, Msg}) ->
    io_lib:format("syntax error at line ~p: ~ts", [Line, Msg]);
format_error({lex_error, Line, Msg}) ->
    io_lib:format("lexical error at line ~p: ~p", [Line, Msg]);
format_error({table_not_found, Name}) ->
    io_lib:format("no such table: ~ts", [Name]);
format_error({column_not_found, Name}) ->
    io_lib:format("no such column: ~ts", [name_str(Name)]);
format_error({no_such_column, Name}) ->
    io_lib:format("no such column: ~ts", [name_str(Name)]);
format_error({index_not_found, Name}) ->
    io_lib:format("no such index: ~ts", [name_str(Name)]);
format_error(index_already_exists) ->
    "an index with that name already exists";
format_error(already_indexed) ->
    "that column is already indexed";
format_error({subquery_must_return_one_column, N}) ->
    io_lib:format("subquery must return exactly one column, got ~p", [N]);
format_error({scalar_subquery_returned_rows, N}) ->
    io_lib:format("scalar subquery returned ~p rows, expected at most one", [N]);
format_error(derived_table_requires_alias) ->
    "a derived table needs an alias: FROM (SELECT ...) AS name";
format_error({set_op_arity_mismatch, L, R}) ->
    io_lib:format("UNION/INTERSECT/EXCEPT need the same number of columns (~p vs ~p)",
                  [L, R]);
format_error(set_op_order_by_must_be_column_or_position) ->
    "ORDER BY on a set operation must name a result column or its position";
format_error({order_by_position_out_of_range, N, Max}) ->
    io_lib:format("ORDER BY ~p is out of range (1..~p)", [N, Max]);
format_error(explain_requires_select) ->
    "EXPLAIN only works on SELECT";
format_error({type_mismatch, Col, Type, Value}) ->
    io_lib:format("column ~ts expects ~ts, got ~ts",
                  [Col, sql_type:name(Type), render(Value)]);
format_error({duplicate_columns, Names}) ->
    io_lib:format("duplicate columns: ~ts", [lists:join(", ", Names)]);
format_error(column_count_mismatch) ->
    "number of values does not match the number of columns";
format_error({ambiguous_column, Name}) ->
    io_lib:format("column reference ~ts is ambiguous (qualify it, e.g. t.~ts)", [Name, Name]);
format_error({not_grouped, Name}) ->
    io_lib:format("column ~ts must appear in GROUP BY or be used in an aggregate", [Name]);
format_error(not_grouped) ->
    "expression must appear in GROUP BY or be used in an aggregate";
format_error({unknown_function, Name}) ->
    io_lib:format("no such function: ~ts", [Name]);
format_error({star_not_allowed, Name}) ->
    io_lib:format("~ts(*) is not allowed (only COUNT(*))", [Name]);
format_error(star_with_group_by) ->
    "SELECT * cannot be used with GROUP BY or aggregates";
format_error(only_constants_in_values) ->
    "VALUES accepts constant expressions only";
format_error(star_must_be_alone) ->
    "SELECT * cannot be combined with other columns";
format_error(ddl_in_transaction) ->
    "DDL cannot run inside a transaction (catalog changes are not rolled back)";
format_error(Other) ->
    io_lib:format("~p", [Other]).

%%%===================================================================
%%% 表の整形
%%%===================================================================

print_table(_Headers, []) ->
    ok;
print_table(Headers, Rows) ->
    Cells = [[render(V) || V <- Row] || Row <- Rows],
    Widths = widths(Headers, Cells),
    io:format(" ~ts~n", [lists:join(" | ", pad_all(Headers, Widths))]),
    io:format("-~ts~n", [lists:join("-+-", [dashes(W) || W <- Widths])]),
    lists:foreach(fun(Row) ->
                          io:format(" ~ts~n", [lists:join(" | ", pad_all(Row, Widths))])
                  end, Cells).

widths(Headers, Cells) ->
    Init = [len(H) || H <- Headers],
    lists:foldl(fun(Row, Acc) ->
                        [max(W, len(C)) || {W, C} <- zip_pad(Acc, Row)]
                end, Init, Cells).

%% 行のカラム数が見出しと食い違っても落ちないようにする
zip_pad(Ws, Cs) when length(Ws) =:= length(Cs) -> lists:zip(Ws, Cs);
zip_pad(Ws, Cs) ->
    N = max(length(Ws), length(Cs)),
    lists:zip(pad_list(Ws, N, 0), pad_list(Cs, N, "")).

pad_list(L, N, Fill) -> L ++ lists:duplicate(N - length(L), Fill).

pad_all(Cells, Widths) ->
    [pad(C, W) || {W, C} <- zip_pad(Widths, Cells)].

pad(S, W) ->
    Str = unicode:characters_to_list(S),
    Str ++ lists:duplicate(max(0, W - length(Str)), $\s).

dashes(W) -> lists:duplicate(W, $-).

len(S) -> length(unicode:characters_to_list(S)).

%%----------------------------------------------------------------------
%% 値の表示。SQLのNULLは NULL、文字列は引用符なしで出す。
%%----------------------------------------------------------------------
render(null) -> "NULL";
render(true) -> "true";
render(false) -> "false";
render(V) when is_binary(V) -> truncate(unicode:characters_to_list(V));
render(V) when is_integer(V) -> integer_to_list(V);
render(V) when is_float(V) -> lists:flatten(io_lib:format("~p", [V]));
render(V) when is_atom(V) -> atom_to_list(V);
render(V) when is_list(V) -> truncate(lists:flatten(io_lib:format("~p", [V])));
render(V) -> truncate(lists:flatten(io_lib:format("~p", [V]))).

truncate(S) when length(S) > ?MAX_COL_WIDTH ->
    lists:sublist(S, ?MAX_COL_WIDTH - 3) ++ "...";
truncate(S) -> S.

%%%===================================================================
%%% スクリプトの分割
%%%===================================================================

%% `--` から行末まではコメント。文は `;` で区切る。
split_statements(Text) ->
    %% 行ごとに前後の空白を落としてから連結する。落とさないと
    %% 複数行の文を表示するときに字下げがそのまま残る。
    Lines = [string:trim(strip_comment(L)) || L <- string:split(Text, "\n", all)],
    Joined = lists:flatten(lists:join(" ", [L || L <- Lines, L =/= ""])),
    [string:trim(S) || S <- string:split(Joined, ";", all),
                       string:trim(S) =/= ""].

strip_comment(Line) ->
    case string:find(Line, "--") of
        nomatch -> Line;
        Rest -> lists:sublist(Line, length(Line) - length(Rest))
    end.

%%%===================================================================
%%% バナー
%%%===================================================================

banner() ->
    io:format(
      "~n"
      "transaction_db SQL shell~n"
      "Type \\? for help, \\q to quit.  Statements end with ';'.~n"
      "~n").
