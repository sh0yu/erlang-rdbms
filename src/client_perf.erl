%%%-------------------------------------------------------------------
%%% @doc
%%% ストレージエンジンのINSERT/UPDATE/DELETEのスループットを測る。
%%%
%%% 書き込みはライトスルーで、1行書くたびにページ全体をディスクへ
%%% 書き戻すため、行数よりも「1ページに何行入るか」が効いてくる。
%%%
%%%   client_perf:exec().          %% 既定の件数で実行
%%%   client_perf:exec(10000).     %% 件数を指定して実行
%%% @end
%%%-------------------------------------------------------------------
-module(client_perf).

-export([exec/0, exec/1]).

-define(DEFAULT_COUNT, 5000).

exec() ->
    exec(?DEFAULT_COUNT).

exec(Count) ->
    {ok, _} = sup:start_link(),
    P = simple_db_server,
    ok = simple_db_server:create_table(P, perf, [random_val]),

    %% カーディナリティが低いと1つのキーにOidが集中してインデックスが
    %% 肥大化するため、キーは全て異なる値にする
    Keys = [I || I <- lists:seq(1, Count)],
    Oids = [{oid, I} || I <- lists:seq(1, Count)],

    Insert = measure(fun() ->
        lists:foreach(fun({Oid, Val}) ->
                              ok = simple_db_server:insert_data(P, perf, Oid, [Val])
                      end, lists:zip(Oids, Keys))
    end),
    report("insert", Count, Insert),

    Select = measure(fun() ->
        lists:foreach(fun(Val) ->
                              [[Val]] = simple_db_server:select_data(P, perf, random_val, Val)
                      end, Keys)
    end),
    report("select", Count, Select),

    Update = measure(fun() ->
        lists:foreach(fun(Val) ->
                              {ok, 1} = simple_db_server:update_data(
                                          P, perf, [{random_val, -Val}], random_val, Val)
                      end, Keys)
    end),
    report("update", Count, Update),

    Delete = measure(fun() ->
        lists:foreach(fun(Oid) ->
                              ok = simple_db_server:delete_data(P, perf, Oid)
                      end, Oids)
    end),
    report("delete", Count, Delete),

    ok = simple_db_server:drop_table(P, perf),
    ok.

measure(Fun) ->
    Start = erlang:monotonic_time(microsecond),
    Fun(),
    erlang:monotonic_time(microsecond) - Start.

report(Label, Count, Micros) ->
    io:format("~-7s ~8w rows ~10w us  (~.1f us/row)~n",
              [Label, Count, Micros, Micros / Count]).
