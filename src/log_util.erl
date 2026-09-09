%%%-------------------------------------------------------------------
%%% @doc
%%% REDOログ(WAL)。コミット時に確定した更新をディスクへ反映する前に
%%% ここへ書き出す。反映の途中で落ちても、次回起動時にrecoverが
%%% ログを読み直して再実行できる。
%%%
%%% WALの規則として、共有データへ書き込む前に必ず sync/0 でログを
%%% ディスクへ同期する。ログが残っていない更新は再実行できないため。
%%%
%%% コミットが最後まで終わるとcheckpointを書く。recoverは最後の
%%% checkpoint以降のエントリだけを再実行すればよい。
%%% @end
%%%-------------------------------------------------------------------
-module(log_util).
-behaviour(gen_server).

%% Public API
-export([start_link/0, stop/1]).
-export([redo_log_write/1, redo_log_write_many/1, redo_log_chunk/1,
         redo_log_put_checkpoint/0, redo_log_truncate/0, sync/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("../include/simple_db_server.hrl").

-define(LOG_NAME, redo_log).

-record(st, {
    log_name
}).

%%%===================================================================
%%% Public API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%----------------------------------------------------------------------
%% @doc REDOログを1件書く。まだディスクへの同期は保証されない。
%%----------------------------------------------------------------------
redo_log_write(#redo_log{} = RedoLog) ->
    disk_log:log(?LOG_NAME, RedoLog).

%%----------------------------------------------------------------------
%% @doc REDOログをまとめて書く。
%%----------------------------------------------------------------------
redo_log_write_many([]) ->
    ok;
redo_log_write_many(RedoLogs) when is_list(RedoLogs) ->
    disk_log:log_terms(?LOG_NAME, RedoLogs).

%%----------------------------------------------------------------------
%% @doc ここまでに書いたログをディスクへ同期する。
%% 共有データを更新する前に必ず呼ぶこと。
%%----------------------------------------------------------------------
sync() ->
    disk_log:sync(?LOG_NAME).

%%----------------------------------------------------------------------
%% @doc ログを先頭から順に読む。Contには start か前回の継続を渡す。
%%----------------------------------------------------------------------
redo_log_chunk(Cont) ->
    disk_log:chunk(?LOG_NAME, Cont).

%%----------------------------------------------------------------------
%% @doc コミットが完了したことを示すcheckpointを書いて同期する。
%%----------------------------------------------------------------------
redo_log_put_checkpoint() ->
    ok = disk_log:log(?LOG_NAME, #redo_log{timestamp = erlang:system_time(nanosecond),
                                           action = checkpoint}),
    sync().

redo_log_truncate() ->
    disk_log:truncate(?LOG_NAME).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    DataDir = application:get_env(transaction_db, data_dir, "./data"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    File = filename:join(DataDir, "redo_log"),
    case disk_log:open([{name, ?LOG_NAME}, {file, File}]) of
        {ok, ?LOG_NAME} ->
            {ok, #st{log_name = ?LOG_NAME}};
        {repaired, ?LOG_NAME, _Recovered, _Bad} ->
            %% 前回の異常終了で末尾が壊れていた場合。
            %% 壊れたエントリは捨てられるが、それ以前は読めるのでこのまま続ける。
            {ok, #st{log_name = ?LOG_NAME}};
        {error, Reason} ->
            {stop, {cannot_open_redo_log, File, Reason}}
    end.

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State};
handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #st{log_name = LogName}) ->
    _ = disk_log:sync(LogName),
    _ = disk_log:close(LogName),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.
