%%%-------------------------------------------------------------------
%%% @doc
%%% システムカタログ。テーブル定義(テーブル名とカラム定義)をDETSに
%%% 永続化して管理する。DB再起動後もテーブル定義が残るため、
%%% recoverがREDOログを適用する前提となる。
%%%
%%% 現状は全カラムにインデックスを張る方針のため、
%%% get_index_column_list/2 は get_column_list/2 と同じ結果を返す。
%%% @end
%%%-------------------------------------------------------------------
-module(sys_tbl_mng).
-behaviour(gen_server).

%% Public API
-export([start_link/0, stop/1]).
-export([create_table/3, drop_table/2, exist_table/2,
         get_column_list/2, get_index_column_list/2, list_tables/1,
         get_columns/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("../include/simple_db_server.hrl").
-include("../include/catalog.hrl").

-record(st, {
    ms_tables
}).

%%%===================================================================
%%% Public API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%----------------------------------------------------------------------
%% @doc テーブル定義を登録する。
%%
%% ColumnList は2つの形を受ける。
%%   [name, price]                     型なし(すべて any)。古いタプルAPI用
%%   [{name, varchar}, {price, integer}]  型つき。SQLのCREATE TABLE用
%%
%% Returns: ok | {error, table_already_exists} | {error, invalid_column_list}
%%----------------------------------------------------------------------
create_table(Pid, TableName, ColumnList) ->
    gen_server:call(Pid, {create_table, TableName, ColumnList}).

%%----------------------------------------------------------------------
%% @doc テーブル定義を削除する。
%% Returns: ok | {error, table_not_found}
%%----------------------------------------------------------------------
drop_table(Pid, TableName) ->
    gen_server:call(Pid, {drop_table, TableName}).

%%----------------------------------------------------------------------
%% Returns: {ok, ColumnList} | {error, table_not_found}
%%----------------------------------------------------------------------
get_column_list(Pid, TableName) ->
    gen_server:call(Pid, {get_column_list, TableName}).

%%----------------------------------------------------------------------
%% @doc 型を含むカラム定義を返す。SQL層だけが使う。
%%
%% get_column_list/2 の戻り値の形([atom()])は変えていない。
%% ストレージ層・索引・query_exec はすべてそちらを使っており、
%% 形を変えると広範囲に影響するため。
%% Returns: {ok, [#column{}]} | {error, table_not_found}
%%----------------------------------------------------------------------
get_columns(Pid, TableName) ->
    gen_server:call(Pid, {get_columns, TableName}).

%%----------------------------------------------------------------------
%% @doc インデックスを張るカラムの一覧。現状は全カラムが対象。
%% Returns: {ok, ColumnList} | {error, table_not_found}
%%----------------------------------------------------------------------
get_index_column_list(Pid, TableName) ->
    gen_server:call(Pid, {get_column_list, TableName}).

exist_table(Pid, TableName) ->
    gen_server:call(Pid, {exist_table, TableName}).

list_tables(Pid) ->
    gen_server:call(Pid, list_tables).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    DataDir = application:get_env(transaction_db, data_dir, "./data"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    {ok, Name} = dets:open_file(ms_tables, [{file, filename:join(DataDir, "ms_tables.sys")}]),
    {ok, #st{ms_tables = Name}}.

handle_call({create_table, TableName, ColumnList}, _From, #st{ms_tables = MsTables} = State) ->
    Reply = case validate(TableName, ColumnList) of
                ok ->
                    case dets:lookup(MsTables, TableName) of
                        [] ->
                            ok = dets:insert(MsTables, {TableName, normalize(ColumnList)}),
                            ok = dets:sync(MsTables),
                            ok;
                        [_] ->
                            {error, table_already_exists}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end,
    {reply, Reply, State};

handle_call({drop_table, TableName}, _From, #st{ms_tables = MsTables} = State) ->
    Reply = case dets:lookup(MsTables, TableName) of
                [] ->
                    {error, table_not_found};
                [_] ->
                    ok = dets:delete(MsTables, TableName),
                    ok = dets:sync(MsTables),
                    ok
            end,
    {reply, Reply, State};

handle_call({get_column_list, TableName}, _From, #st{ms_tables = MsTables} = State) ->
    Reply = case lookup_columns(MsTables, TableName) of
                {error, Reason} -> {error, Reason};
                {ok, Columns} -> {ok, [C#column.name || C <- Columns]}
            end,
    {reply, Reply, State};

handle_call({get_columns, TableName}, _From, #st{ms_tables = MsTables} = State) ->
    {reply, lookup_columns(MsTables, TableName), State};

handle_call({exist_table, TableName}, _From, #st{ms_tables = MsTables} = State) ->
    Reply = case dets:lookup(MsTables, TableName) of
                [] -> false;
                [_] -> true
            end,
    {reply, Reply, State};

handle_call(list_tables, _From, #st{ms_tables = MsTables} = State) ->
    Tables = dets:foldl(fun({TableName, _Cols}, Acc) -> [TableName | Acc] end, [], MsTables),
    {reply, {ok, lists:sort(Tables)}, State};

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #st{ms_tables = MsTables}) ->
    _ = dets:close(MsTables),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal
%%%===================================================================

%% 不正な定義をここで弾かないと、行とカラムのzipが後段でクラッシュする。
validate(TableName, ColumnList) when is_atom(TableName), is_list(ColumnList) ->
    Names = [name_of(C) || C <- ColumnList],
    case ColumnList =/= []
        andalso lists:all(fun valid_column/1, ColumnList)
        andalso length(lists:usort(Names)) =:= length(Names) of
        true -> ok;
        false -> {error, invalid_column_list}
    end;
validate(_TableName, _ColumnList) ->
    {error, invalid_column_list}.

valid_column(Name) when is_atom(Name) -> true;
valid_column({Name, Type}) when is_atom(Name) -> lists:member(Type, types());
valid_column(_) -> false.

name_of(Name) when is_atom(Name) -> Name;
name_of({Name, _Type}) -> Name;
name_of(_) -> undefined.

types() -> [integer, float, varchar, boolean, any].

%% 2つの入力形を #column{} のリストに揃える。
normalize(ColumnList) ->
    normalize(ColumnList, 1, []).

normalize([], _N, Acc) ->
    lists:reverse(Acc);
normalize([{Name, Type} | T], N, Acc) ->
    normalize(T, N + 1, [#column{name = Name, type = Type, position = N} | Acc]);
normalize([Name | T], N, Acc) ->
    %% 型宣言のない古い形。any にしておくと比較で暗黙変換されない
    normalize(T, N + 1, [#column{name = Name, type = any, position = N} | Acc]).

lookup_columns(MsTables, TableName) ->
    case dets:lookup(MsTables, TableName) of
        [] -> {error, table_not_found};
        [{TableName, Columns}] -> {ok, Columns}
    end.
