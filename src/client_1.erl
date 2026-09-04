%%%-------------------------------------------------------------------
%%% @doc
%%% ストレージエンジン(simple_db_server)を直接叩くサンプル。
%%% トランザクションを介さないので、書き込みは即座に共有データへ反映される。
%%% トランザクションつきの例はclient_query_execを参照。
%%% @end
%%%-------------------------------------------------------------------
-module(client_1).

-export([exec/0]).

exec() ->
    {ok, _} = sup:start_link(),
    P = simple_db_server,

    ok = simple_db_server:create_table(P, fruit, [name, price]),
    ok = simple_db_server:insert_data(P, fruit, oid1, [apple, 100]),
    ok = simple_db_server:insert_data(P, fruit, oid2, [banana, 150]),
    ok = simple_db_server:insert_data(P, fruit, oid3, [orange, 150]),
    show("Step1", P),

    %% appleを120円に、150円の行(banana と orange)を160円にする
    {ok, 1} = simple_db_server:update_data(P, fruit, [{price, 120}], name, apple),
    {ok, 2} = simple_db_server:update_data(P, fruit, [{price, 160}], price, 150),
    show("Step2", P),

    ok = simple_db_server:delete_data(P, fruit, oid2),
    show("Step3", P),

    ok = simple_db_server:drop_table(P, fruit),
    %% ドロップ後のSELECTはクラッシュせずエラーを返す
    io:format("Step4:~p~n", [simple_db_server:select_data(P, fruit, name, apple)]),
    ok.

show(Label, P) ->
    lists:foreach(
      fun(Name) ->
              io:format("~s:~p -> ~p~n",
                        [Label, Name, simple_db_server:select_data(P, fruit, name, Name)])
      end, [apple, banana, orange]).
