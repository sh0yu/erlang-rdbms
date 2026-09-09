%%%-------------------------------------------------------------------
%%% @doc
%%% REDOログによるクラッシュリカバリ。
%%%
%%% コミットは「REDOログを書いて同期する」→「共有データへ反映する」→
%%% 「checkpointを書く」の順に進む。したがって最後のcheckpointより後に
%%% 残っているエントリは、反映が完了したか分からない更新である。
%%% これらを先頭から順に再実行すれば、コミット済みの状態に追いつける。
%%%
%%% 再実行は冪等である必要がある。simple_db_serverのinsert/deleteは
%%% Oidを指定した上書き・削除なので、二重に適用しても結果は変わらない。
%%%
%%% DB起動時(tx_mng:init/1)に呼ばれる。
%%% @end
%%%-------------------------------------------------------------------
-module(recover).

-export([recover/0, pending_redo_list/0]).

-include("../include/simple_db_server.hrl").

%%----------------------------------------------------------------------
%% @doc REDOログを読み、最後のcheckpoint以降の更新を再実行する。
%% 再実行後はログを切り詰める。
%%----------------------------------------------------------------------
recover() ->
    RedoList = pending_redo_list(),
    case RedoList of
        [] ->
            ok;
        _ ->
            error_logger:info_msg("[recover] redoing ~p entries~n", [length(RedoList)]),
            do_recover(RedoList)
    end,
    ok = log_util:redo_log_truncate(),
    ok.

%%----------------------------------------------------------------------
%% @doc 再実行が必要なREDOログのリストを、書かれた順に返す。
%%----------------------------------------------------------------------
pending_redo_list() ->
    seek_checkpoint(read_all(start, []), []).

%% ログを先頭から全て読む。chunkは書かれた順に返るので、
%% 読んだ順を保ったまま連結する。
read_all(Cont, Acc) ->
    case log_util:redo_log_chunk(Cont) of
        eof ->
            lists:append(lists:reverse(Acc));
        {error, Reason} ->
            error_logger:warning_msg("[recover] cannot read redo log: ~p~n", [Reason]),
            lists:append(lists:reverse(Acc));
        {Cont2, Terms} ->
            read_all(Cont2, [Terms | Acc]);
        {Cont2, Terms, BadBytes} ->
            %% 末尾が壊れている場合。読めたところまでを使う。
            error_logger:warning_msg("[recover] skipped ~p bad bytes in redo log~n", [BadBytes]),
            read_all(Cont2, [Terms | Acc])
    end.

%% 最後のcheckpoint以降のエントリだけを残す。
%% checkpointに出会うたびにそれまでの蓄積を捨てる。
seek_checkpoint([], Ret) ->
    lists:reverse(Ret);
seek_checkpoint([#redo_log{action = checkpoint} | RedoList], _Ret) ->
    seek_checkpoint(RedoList, []);
seek_checkpoint([#redo_log{} = H | RedoList], Ret) ->
    seek_checkpoint(RedoList, [H | Ret]);
seek_checkpoint([_Other | RedoList], Ret) ->
    %% 想定外の項は無視する
    seek_checkpoint(RedoList, Ret).

%% REDO操作を書かれた順に再実行する。
do_recover([]) ->
    ok;
do_recover([#redo_log{action = ins, table_name = TableName, oid = Oid, val = Val} | T]) ->
    log_result(ins, TableName, Oid,
               simple_db_server:insert_data(simple_db_server, TableName, Oid, Val)),
    do_recover(T);
do_recover([#redo_log{action = del, table_name = TableName, oid = Oid} | T]) ->
    log_result(del, TableName, Oid,
               simple_db_server:delete_data(simple_db_server, TableName, Oid)),
    do_recover(T);
do_recover([_Other | T]) ->
    do_recover(T).

%% ドロップ済みのテーブルへのREDOなど、再実行できないものは
%% 落とさずに記録だけ残して次へ進む。
log_result(_Action, _TableName, _Oid, ok) ->
    ok;
log_result(Action, TableName, Oid, Error) ->
    error_logger:warning_msg("[recover] skipped ~p ~p/~p: ~p~n",
                             [Action, TableName, Oid, Error]),
    ok.
