%%%-------------------------------------------------------------------
%%% @doc
%%% sqllogictest のランナー。
%%%
%%%   https://www.sqlite.org/sqllogictest/
%%%
%%% SQLite が「同じSQLを複数のDBに流して、結果が一致するか」を確かめる
%%% ために作った形式。クエリと期待結果が組で書いてあり、結果が多いときは
%%% md5 で畳んである。DuckDB や CockroachDB もこれを使っている。
%%%
%%% 形式:
%%%
%%%   statement ok          次の空行までがSQL。成功すること
%%%   statement error       同上。失敗すること
%%%   query IIT rowsort     I=整数 R=実数 T=文字列。並べ替えの指定が続く
%%%   <SQL>
%%%   ----
%%%   <期待結果。1値1行>            または  N values hashing to <md5>
%%%
%%%   onlyif <db> / skipif <db>     直後の1レコードにだけ効く
%%%   halt                          そこで打ち切り
%%%
%%% == 値の書き方 ==
%%%
%%% md5 を合わせるには、値の文字列化まで一致させる必要がある。
%%%
%%%   I  NULL は "NULL"、それ以外は整数。整数でなければ 0
%%%   R  NULL は "NULL"、それ以外は "%.3f"
%%%   T  NULL は "NULL"、空文字は "(empty)"、印字できない文字は @
%%%
%%% 1値1行にして連結し(各行の末尾に改行)、その md5 を取る。
%%%
%%% == このDBに合わせたところ ==
%%%
%%% 自動コミットが無いので、レコード1つを BEGIN/COMMIT で囲う。
%%% コーパスは1文ごとに確定する前提なので、意味は変わらない。
%%% @end
%%%-------------------------------------------------------------------
-module(slt).

-export([run_file/1, run_dir/1, format_report/1, summary/1]).
-export([parse/1]).

-record(rec, {kind,          % statement | query | halt
              expect,        % ok | error            (statement)
              types = "",    % "IIT"                 (query)
              sort = nosort, % nosort|rowsort|valuesort
              sql = [],
              result = [],   % 期待結果の行
              line = 0}).

%% 結果の集計。
%%   pass        期待どおり
%%   wrong       実行できたが答えが違う ← これが0でないのはバグ
%%   unsupported 構文や機能が無くて実行できない
-record(rep, {pass = 0, wrong = 0, unsupported = 0,
              wrongs = [], unsupported_kinds = #{}, samples = #{}}).

%%%===================================================================

run_dir(Dir) ->
    Files = lists:sort(filelib:wildcard(filename:join(Dir, "*.test"))),
    lists:foldl(fun(F, Acc) -> merge(Acc, run_file(F)) end, #rep{}, Files).

run_file(Path) ->
    {ok, Bin} = file:read_file(Path),
    Records = parse(binary_to_list(Bin)),
    C = connect(),
    %% ファイルごとに空のDBから始める。コーパスはどれも t1 を作るので、
    %% 前のファイルの表が残っていると CREATE TABLE が失敗し、
    %% そのあとの INSERT が前のファイルの行に積み上がって答えが狂う。
    ok = drop_all(C),
    try
        lists:foldl(fun(R, Acc) -> run_record(C, R, Acc) end, #rep{}, Records)
    after
        catch gen_connection:disconnect(C)
    end.

drop_all(C) ->
    {ok, Tables} = sys_tbl_mng:list_tables(whereis(sys_tbl_mng)),
    lists:foreach(fun(T) ->
                          _ = q(C, "DROP TABLE " ++ atom_to_list(T))
                  end, Tables),
    ok.

%% 集計だけを取り出す。試験から見るのはこちら。
summary(#rep{pass = P, wrong = W, unsupported = U, wrongs = Ws}) ->
    #{pass => P, wrong => W, unsupported => U,
      wrongs => [lists:flatten(io_lib:format("line ~p: ~s", [L, M])) || {L, M} <- Ws]}.

format_report(#rep{pass = P, wrong = W, unsupported = U} = R) ->
    Total = P + W + U,
    [io_lib:format("~p records: ~p pass, ~p wrong, ~p unsupported~n", [Total, P, W, U]),
     case R#rep.wrong of
         0 -> "";
         _ -> io_lib:format("~n-- wrong answers --~n~s",
                            [[io_lib:format("  line ~p: ~s~n", [L, S])
                              || {L, S} <- lists:sublist(R#rep.wrongs, 20)]])
     end,
     io_lib:format("~n-- unsupported by reason --~n~s",
                   [[io_lib:format("  ~5w  ~p~n         e.g. ~s~n",
                                   [N, K, sample(K, R)])
                     || {K, N} <- lists:reverse(
                                    lists:keysort(2, maps:to_list(R#rep.unsupported_kinds)))]])].

sample(Kind, #rep{samples = S}) ->
    string:slice(maps:get(Kind, S, ""), 0, 120).

%%%===================================================================
%%% 解析
%%%===================================================================

parse(Text) ->
    parse_lines(string:split(Text, "\n", all), 1, [], []).

parse_lines([], _N, _Cond, Acc) ->
    lists:reverse(Acc);
parse_lines([Line | T], N, Cond, Acc) ->
    case classify(string:trim(Line, trailing, "\r")) of
        blank    -> parse_lines(T, N + 1, Cond, Acc);
        comment  -> parse_lines(T, N + 1, Cond, Acc);
        {control, _} -> parse_lines(T, N + 1, Cond, Acc);
        %% onlyif/skipif は次の1レコードにだけ効く
        {cond_, C} -> parse_lines(T, N + 1, [C | Cond], Acc);
        halt -> lists:reverse(Acc);
        {statement, Expect} ->
            {Sql, Rest, N2} = take_until_blank(T, N + 1, []),
            Rec = #rec{kind = statement, expect = Expect, sql = Sql, line = N},
            parse_lines(Rest, N2, [], keep(Cond, Rec, Acc));
        {query, Types, Sort} ->
            {Sql, Rest1, N2} = take_until(T, N + 1, "----", []),
            {Res, Rest2, N3} = take_until_blank(Rest1, N2, []),
            Rec = #rec{kind = query, types = Types, sort = Sort,
                       sql = Sql, result = Res, line = N},
            parse_lines(Rest2, N3, [], keep(Cond, Rec, Acc));
        other ->
            parse_lines(T, N + 1, Cond, Acc)
    end.

%% onlyif/skipif の判定。このDBはどの既知エンジンでもないので、
%%   onlyif X  → 実行しない
%%   skipif X  → 実行する
keep(Cond, Rec, Acc) ->
    case lists:any(fun({onlyif, _}) -> true; (_) -> false end, Cond) of
        true  -> Acc;
        false -> [Rec | Acc]
    end.

classify("") -> blank;
classify("#" ++ _) -> comment;
classify("halt") -> halt;
classify("hash-threshold" ++ _) -> {control, hash_threshold};
classify("statement ok" ++ _) -> {statement, ok};
classify("statement error" ++ _) -> {statement, error};
classify("onlyif " ++ Db) -> {cond_, {onlyif, string:trim(Db)}};
classify("skipif " ++ Db) -> {cond_, {skipif, string:trim(Db)}};
classify("query " ++ Rest) ->
    case string:lexemes(Rest, " ") of
        [Types]            -> {query, Types, nosort};
        [Types, Sort | _]  -> {query, Types, sort_mode(Sort)}
    end;
classify(_) -> other.

sort_mode("rowsort")   -> rowsort;
sort_mode("valuesort") -> valuesort;
sort_mode(_)           -> nosort.

take_until_blank(Lines, N, Acc) -> take_until(Lines, N, "", Acc).

take_until([], N, _Stop, Acc) ->
    {lists:reverse(Acc), [], N};
take_until([Line | T], N, Stop, Acc) ->
    case string:trim(Line, trailing, "\r") of
        Stop -> {lists:reverse(Acc), T, N + 1};
        L    -> take_until(T, N + 1, Stop, [L | Acc])
    end.

%%%===================================================================
%%% 実行
%%%===================================================================

run_record(C, #rec{kind = statement, expect = Expect, sql = Sql, line = L}, Rep) ->
    case exec(C, join(Sql)) of
        {ok, _}                 when Expect =:= ok    -> bump(pass, Rep);
        {ok, _}                 when Expect =:= error -> wrong(L, "statement did not fail", Rep);
        {error, Reason}         when Expect =:= error -> ignore(Reason, pass, Rep);
        {error, Reason} ->
            classify_failure(L, Reason, join(Sql), Rep)
    end;
run_record(C, #rec{kind = query, sql = Sql, line = L} = R, Rep) ->
    case exec(C, join(Sql)) of
        {error, Reason} ->
            classify_failure(L, Reason, join(Sql), Rep);
        {ok, Rows} ->
            Got = render(Rows, R#rec.types, R#rec.sort),
            case matches(Got, R#rec.result) of
                true  -> bump(pass, Rep);
                false -> wrong(L, diff(Got, R#rec.result), Rep)
            end
    end.

ignore(_Reason, Kind, Rep) -> bump(Kind, Rep).

%% 実行できなかった理由を分類する。構文が無いのか、答えが違うのかを
%% 混ぜると「どこまで通るか」が分からなくなる。
classify_failure(_L, Reason, Sql, Rep) ->
    Kind = failure_kind(Reason),
    Kinds = maps:update_with(Kind, fun(N) -> N + 1 end, 1, Rep#rep.unsupported_kinds),
    Samples = case maps:is_key(Kind, Rep#rep.samples) of
                  true  -> Rep#rep.samples;
                  false -> (Rep#rep.samples)#{Kind => Sql}
              end,
    Rep#rep{unsupported = Rep#rep.unsupported + 1,
            unsupported_kinds = Kinds, samples = Samples}.

%% 構文エラーは「何の手前で落ちたか」まで見ないと、何が足りないのか
%% 分からない。yecc の文言から token を取り出す。
failure_kind({syntax_error, _Line, Msg}) -> {syntax_error_before, offending(Msg)};
failure_kind({syntax_error, _})        -> syntax_error;
failure_kind({unexpected_token, T, _}) -> {unexpected_token, T};
failure_kind({unknown_function, F})    -> {unknown_function, F};
failure_kind(R) when is_atom(R)        -> R;
failure_kind(R) when is_tuple(R)       -> element(1, R);
failure_kind(_)                        -> other.

offending(Msg) when is_list(Msg) ->
    case string:find(Msg, "before: ") of
        nomatch -> unknown;
        Rest    -> string:trim(string:trim(lists:nthtail(8, Rest)), both, "'\"")
    end;
offending(_) -> unknown.

%% 自動コミットが無いので、1レコードを暗黙のトランザクションで囲う。
exec(C, Sql) ->
    case is_ddl(Sql) of
        true  -> reply(q(C, Sql));
        false ->
            ok = q(C, "BEGIN"),
            R = reply(q(C, Sql)),
            _ = case R of
                    {ok, _} -> q(C, "COMMIT");
                    _       -> q(C, "ROLLBACK")
                end,
            R
    end.

is_ddl(Sql) ->
    Up = string:uppercase(string:trim(Sql)),
    lists:any(fun(P) -> lists:prefix(P, Up) end,
              ["CREATE", "DROP", "ANALYZE"]).

reply({ok, _Names, Rows}) -> {ok, Rows};
reply({ok, _Count})       -> {ok, []};
reply(ok)                 -> {ok, []};
reply({error, Reason})    -> {error, Reason};
reply(transaction_not_found) -> {error, transaction_not_found};
reply(Other)              -> {error, {unexpected_reply, Other}}.

%%%===================================================================
%%% 値の書き方(md5 を合わせるにはここが一致していないといけない)
%%%===================================================================

render(Rows, Types, Sort) ->
    Cells = [[cell(T, V) || {T, V} <- zip_types(Types, Row)] || Row <- Rows],
    lists:append(sort_cells(Sort, Cells)).

%% 型指定より列が多い/少ない場合に落ちないようにする
zip_types(Types, Row) ->
    zip_types(Types, Row, []).
zip_types(_, [], Acc) -> lists:reverse(Acc);
zip_types([], [V | T], Acc) -> zip_types([], T, [{$T, V} | Acc]);
zip_types([Ty | TT], [V | T], Acc) -> zip_types(TT, T, [{Ty, V} | Acc]).

cell(_Ty, null) -> "NULL";
cell($I, V) when is_integer(V) -> integer_to_list(V);
cell($I, V) when is_float(V)   -> integer_to_list(trunc(V));
cell($I, _) -> "0";
cell($R, V) when is_number(V)  -> lists:flatten(io_lib:format("~.3f", [V * 1.0]));
cell($R, _) -> "0.000";
cell($T, V) -> text(V);
cell(_, V)  -> text(V).

text(V) when is_binary(V) -> text(binary_to_list(V));
text(V) when is_integer(V) -> integer_to_list(V);
text(V) when is_float(V) -> lists:flatten(io_lib:format("~.3f", [V]));
text(V) when is_atom(V) -> atom_to_list(V);
text("") -> "(empty)";
text(V) when is_list(V) -> [printable(Ch) || Ch <- V];
text(V) -> lists:flatten(io_lib:format("~p", [V])).

printable(Ch) when Ch >= 32, Ch =< 126 -> Ch;
printable(_) -> $@.

sort_cells(nosort, Cells)    -> Cells;
sort_cells(rowsort, Cells)   -> lists:sort(Cells);
sort_cells(valuesort, Cells) -> [lists:sort(lists:append(Cells))].

%%%===================================================================
%%% 期待結果との突き合わせ
%%%===================================================================

matches(Got, [Line]) ->
    case string:lexemes(Line, " ") of
        [NStr, "values", "hashing", "to", Md5] ->
            length(Got) =:= list_to_integer(NStr) andalso md5(Got) =:= Md5;
        _ ->
            Got =:= [Line]
    end;
matches(Got, Expected) ->
    Got =:= Expected.

md5(Values) ->
    Bin = iolist_to_binary([[V, $\n] || V <- Values]),
    lists:flatten([io_lib:format("~2.16.0b", [B]) || <<B>> <= erlang:md5(Bin)]).

diff(Got, Expected) ->
    io_lib:format("expected ~p got ~p",
                  [lists:sublist(Expected, 3), lists:sublist(Got, 3)]).

%%%===================================================================

join(Lines) -> string:join(Lines, " ").

bump(pass, R) -> R#rep{pass = R#rep.pass + 1}.

wrong(Line, Msg, R) ->
    R#rep{wrong = R#rep.wrong + 1,
          wrongs = R#rep.wrongs ++ [{Line, lists:flatten(Msg)}]}.

merge(A, B) ->
    #rep{pass = A#rep.pass + B#rep.pass,
         wrong = A#rep.wrong + B#rep.wrong,
         unsupported = A#rep.unsupported + B#rep.unsupported,
         wrongs = A#rep.wrongs ++ B#rep.wrongs,
         unsupported_kinds =
             maps:merge_with(fun(_K, X, Y) -> X + Y end,
                             A#rep.unsupported_kinds, B#rep.unsupported_kinds),
         samples = maps:merge(B#rep.samples, A#rep.samples)}.

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
q(C, Sql) -> query_exec:exec_query(C, {sql, Sql}).
