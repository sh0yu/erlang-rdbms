%%%-------------------------------------------------------------------
%%% 画面の台本(priv/demos.sql)が本当に動くか。
%%%
%%% 台本は「こう流すとこうなる」を見せるためのものなので、動かなければ
%%% 説明ごと嘘になる。ここで全手順をそのまま流して確かめる。
%%%
%%% 順に流すのが要点。デモ2は デモ1 が作った表を使う、というように
%%% 台本は前から続いている。画面でも上から順に押していく。
%%%
%%% !error と書いた手順は**断られるのが正しい**。断られること自体が
%%% 見せたい動き(主キーの重複、直列化の失敗)なので、期待値として扱う。
%%%-------------------------------------------------------------------
-module(web_ui_demo_tests).

-include_lib("eunit/include/eunit.hrl").

demos_test_() ->
    {timeout, 120,
     {setup, fun db_test_helper:start_db/0, fun db_test_helper:stop_db/1,
      fun(_) -> [fun script_is_well_formed/0, fun every_step_runs/0] end}}.

%% 台本として読めていること。手順が0のデモがあれば書式を間違えている。
script_is_well_formed() ->
    Demos = web_ui:parsed_demos(),
    ?assert(length(Demos) >= 5),
    lists:foreach(
      fun({Title, About, Steps}) ->
              ?assert(Title =/= ""),
              ?assert(About =/= ""),
              ?assertNotEqual([], Steps),
              lists:foreach(
                fun({Conn, _Note, Sql, Expect}) ->
                        ?assert(lists:member(Conn, ["A", "B"])),
                        ?assert(lists:member(Expect, [ok, error])),
                        %% ; は流す前に落とす。付いたままだと構文誤り
                        ?assert(lists:suffix(";", Sql))
                end, Steps)
      end, Demos).

%% 全部を順に流す。画面で上から押していったのと同じ順序。
every_step_runs() ->
    Conns = #{"A" => connect(), "B" => connect()},
    lists:foreach(fun(D) -> run_demo(Conns, D) end, web_ui:parsed_demos()).

run_demo(Conns, {Title, _About, Steps}) ->
    lists:foreach(fun(S) -> run_step(Conns, Title, S) end, Steps).

run_step(Conns, Title, {Conn, Note, Sql, Expect}) ->
    Pid = maps:get(Conn, Conns),
    Result = query_exec:exec_query(Pid, {sql, strip(Sql)}),
    case {Expect, is_error(Result)} of
        {ok, false} ->
            ok;
        {error, true} ->
            ok;
        {ok, true} ->
            erlang:error({step_failed, Title, Note, Sql, Result});
        {error, false} ->
            erlang:error({step_should_have_failed, Title, Note, Sql, Result})
    end.

is_error({error, _})           -> true;
is_error(transaction_not_found) -> true;
is_error(_)                     -> false.

strip(Sql) -> string:trim(string:trim(string:trim(Sql), trailing, ";")).

connect() -> {ok, Pid} = gen_connection:connect(), Pid.
