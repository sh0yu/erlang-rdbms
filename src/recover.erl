-module(recover).
-compile(export_all).

-include("../include/simple_db_server.hrl").

%% TODO: DB再起動時とコミット中の異常終了時に起動する仕組みを作る
%% RedoLogを順に読み込み、REDOするための操作のリストを作成する
%% do_recoverにてREDOを実行する
recover() ->
    recover(start, []).
recover(Cont, RedoList) ->
    case log_util:redo_log_chunk(Cont) of
        {Cont2, RedoLog} ->
            recover(Cont2, RedoLog++RedoList);
        eof -> 
            RedoL = seek_checkpoint(RedoList, []),
            do_recover(build_redo_list(RedoL)),
            log_util:redo_log_truncate()
    end.

%% REDOするための操作のリストを作成する
%% RedoListはすでに作成したリスト、RedoLogはこれから解釈するREDOログ
%% checkpointを通過した時などは、RedoListを破棄して、RedoListを作成し直す
build_redo_list(RedoList) ->
    %% TODO: chunkした結果を解釈してからリカバリを実施するよう修正。特にトランザクションの途中までDISK_INSERTが実行できている場合の考慮など
    lists:map(fun(#redo_log{action=Action, table_name=TableName, oid=Oid, val=Val}) ->
        {Action, TableName, Oid, Val}
    end, RedoList).

%% REDO操作を行う。基本的にはsimple_db_serverにクエリを投げるだけの想定
%% TODO: REDOが終わった後に、redo_logをきれいにする（チェックポイントを打つだけか、ログ再作成か）
do_recover([]) ->
    ok;
do_recover([{ins, TableName, Oid, Val} | T]) ->
    io:format("[REDO][ins]~p,~p,~p~n", [TableName, Oid, Val]),
    %% TODO: すでにINSERTの処理が行われており、指定のOidに対してデータINSERTが必要な場合の処理追加
    simple_db_server:insert_data(simple_db_server, TableName, Oid, Val),
    do_recover(T);
do_recover([{del, TableName, Oid, Val} | T]) ->
    io:format("[REDO][del]~p,~p,~p~n", [TableName, Oid, Val]),
    simple_db_server:delete_data(simple_db_server, TableName, Oid),
    do_recover(T).

%% TODO: チェックポイントを測る仕組みを作る
%% チェックポイントをどこで取るか、どのように実装するか
%% -> コミットが正常に完了した時点で、redo_logにcheckpointを書き込む

%% チェックポイント以降のREDOログだけ再実行すれば良いので、checkpointを探す
seek_checkpoint([], Ret) ->
    lists:reverse(Ret);
seek_checkpoint([#redo_log{action=checkpoint} | RedoList], _Ret) ->
    seek_checkpoint(RedoList, []);
seek_checkpoint([H | RedoList], Ret) ->
    seek_checkpoint(RedoList, [H | Ret]).

test_print_redo_log(RedoList) ->
    io:format("[PrintRedoLog]~p~n", [RedoList]).