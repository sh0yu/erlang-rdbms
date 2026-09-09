%%%-------------------------------------------------------------------
%%% @doc
%%% システムカタログ。テーブル定義(テーブル名とカラム定義)をDETSに
%%% 永続化して管理する。DB再起動後もテーブル定義が残るため、
%%% recoverがREDOログを適用する前提となる。
%%%
%%% == 読みは gen_server を通さない ==
%%%
%%% カタログは**読みが極端に多く、書きは DDL のときだけ**。
%%% 以前は読みも gen_server:call だったため、全接続がこの1プロセスの
%%% 前に一列に並んでいた。read_data_oid/2 は行ごとに exist_table/2 を
%%% 呼ぶので、索引で100行引けば往復が100回になる。
%%%
%%% 実測では、32接続が同時に走査するとこのプロセスのメッセージキューが
%%% 31まで伸びていた。接続数と一致する = 全員が待っている、ということ。
%%%
%%% よって DETS の内容を public な ETS(?CACHE)に写しておき、
%%% **読みは呼び出し元のプロセスが直接引く**。gen_server が受け持つのは
%%% 書き込みだけで、DETS と ETS の両方を更新する。
%%%
%%% 順序は「DETS を書く → ETS を写す」。逆にすると、まだ永続化して
%%% いないものが他のプロセスから見える。
%%%
%%% ETS の持ち主はこの gen_server なので、落ちれば ETS も消える。
%%% sup は rest_for_one なので、その後ろは全部作り直される。
%%% @end
%%%-------------------------------------------------------------------
-module(sys_tbl_mng).
-behaviour(gen_server).

%% Public API
-export([start_link/0, stop/1]).
-export([create_index/4, drop_index/2, get_indexes/2, list_indexes/1]).
-export([put_stats/3, get_stats/2]).
-export([create_table/3, drop_table/2, exist_table/2,
         get_column_list/2, get_index_column_list/2, list_tables/1,
         get_columns/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("../include/simple_db_server.hrl").
-include("../include/catalog.hrl").

-record(st, {
    ms_tables,
    ms_indexes,
    ms_stats
}).

%% 読み専用のキャッシュ。持ち主はこの gen_server。
%%   {{table, Name}, [#column{}]}
%%   {{indexes, Name}, [#index{}]}   名前順
%%   {{stats, Name}, #table_stats{}}
-define(CACHE, sys_tbl_cache).

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
%%   [name, price]                          型なし(すべて any)。タプルAPI用
%%   [{name, varchar}, {price, integer}]     型つき
%%   [{id, integer, [unique, not_null]}]     型と列制約つき。SQLのCREATE TABLE用
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
get_column_list(_Pid, TableName) ->
    case cached_columns(TableName) of
        {error, Reason} -> {error, Reason};
        {ok, Columns}   -> {ok, [C#column.name || C <- Columns]}
    end.

%%----------------------------------------------------------------------
%% @doc 型を含むカラム定義を返す。SQL層だけが使う。
%%
%% get_column_list/2 の戻り値の形([atom()])は変えていない。
%% ストレージ層・索引・query_exec はすべてそちらを使っており、
%% 形を変えると広範囲に影響するため。
%% Returns: {ok, [#column{}]} | {error, table_not_found}
%%----------------------------------------------------------------------
get_columns(_Pid, TableName) ->
    cached_columns(TableName).

%%----------------------------------------------------------------------
%% @doc 索引が張られているカラムの一覧。
%%
%% **以前は全カラムを返していた。** 全カラムに自動で索引が張られていたので
%% 「索引があるか」が常に真になり、プランナのアクセスパス選択が退化する。
%% いまは CREATE INDEX で明示的に宣言されたものだけを返す。
%% Returns: {ok, ColumnList} | {error, table_not_found}
%%----------------------------------------------------------------------
get_index_column_list(Pid, TableName) ->
    case get_indexes(Pid, TableName) of
        {error, Reason} -> {error, Reason};
        {ok, Indexes}   -> {ok, [I#index.column || I <- Indexes]}
    end.

%%----------------------------------------------------------------------
%% @doc 索引を定義する。
%% Returns: ok | {error, table_not_found | column_not_found
%%                     | index_already_exists | already_indexed}
%%----------------------------------------------------------------------
create_index(Pid, IndexName, TableName, ColumnName) ->
    gen_server:call(Pid, {create_index, IndexName, TableName, ColumnName}).

%%----------------------------------------------------------------------
%% @doc 索引の定義を消す。落とすべきETSを呼び出し側に伝えるため、
%% どのテーブルのどのカラムだったかを返す。
%% Returns: {ok, TableName, ColumnName} | {error, {index_not_found, Name}}
%%----------------------------------------------------------------------
drop_index(Pid, IndexName) ->
    gen_server:call(Pid, {drop_index, IndexName}).

%% @doc テーブルに定義されている索引。Returns: {ok, [#index{}]} | {error, _}
get_indexes(_Pid, TableName) ->
    case exists(TableName) of
        false -> {error, table_not_found};
        true  -> {ok, lookup(?CACHE, {indexes, TableName}, [])}
    end.

%% @doc 全索引。名前順。
list_indexes(_Pid) ->
    All = [I || {{indexes, _T}, Is} <- ets:match_object(?CACHE, {{indexes, '_'}, '_'}),
                I <- Is],
    {ok, lists:keysort(#index.name, All)}.

%%----------------------------------------------------------------------
%% @doc 統計を書く / 読む。
%% 採取していないテーブルは none を返す。プランナは既定値で見積もる。
%%----------------------------------------------------------------------
put_stats(Pid, TableName, Stats) ->
    gen_server:call(Pid, {put_stats, TableName, Stats}).

-spec get_stats(pid() | atom(), atom()) -> {ok, #table_stats{}} | none.
get_stats(_Pid, TableName) ->
    case ets:lookup(?CACHE, {stats, TableName}) of
        [{_, Stats}] -> {ok, Stats};
        []           -> none
    end.

exist_table(_Pid, TableName) ->
    exists(TableName).

list_tables(_Pid) ->
    {ok, lists:sort([T || {{table, T}, _} <- ets:match_object(?CACHE, {{table, '_'}, '_'})])}.

%%%===================================================================
%%% キャッシュの読み(呼び出し元のプロセスで動く)
%%%===================================================================

exists(TableName) ->
    ets:member(?CACHE, {table, TableName}).

cached_columns(TableName) ->
    case ets:lookup(?CACHE, {table, TableName}) of
        [{_, Columns}] -> {ok, Columns};
        []             -> {error, table_not_found}
    end.

lookup(Tab, Key, Default) ->
    case ets:lookup(Tab, Key) of
        [{_, V}] -> V;
        []       -> Default
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    DataDir = application:get_env(transaction_db, data_dir, "./data"),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    {ok, Name} = dets:open_file(ms_tables, [{file, filename:join(DataDir, "ms_tables.sys")}]),
    %% 索引の定義は別のDETSに置く。テーブルの行 {Name, Columns} の形を
    %% 変えると、既存のデータファイルとの互換が切れる。
    {ok, IdxName} = dets:open_file(ms_indexes,
                                   [{file, filename:join(DataDir, "ms_indexes.sys")}]),
    {ok, StatName} = dets:open_file(ms_stats,
                                    [{file, filename:join(DataDir, "ms_stats.sys")}]),
    %% 読みはここから引く。DETS の中身を丸ごと写す。
    %% カタログはテーブル数ぶんしかないので、全部載せてよい。
    ?CACHE = ets:new(?CACHE, [set, named_table, protected, {read_concurrency, true}]),
    ok = load_cache(Name, IdxName, StatName),
    {ok, #st{ms_tables = Name, ms_indexes = IdxName, ms_stats = StatName}}.

%% 起動時に DETS から ETS へ写す。
load_cache(MsTables, MsIdx, MsStat) ->
    ok = dets:foldl(fun({T, Cols}, ok) ->
                            true = ets:insert(?CACHE, {{table, T}, Cols}),
                            ok
                    end, ok, MsTables),
    ok = dets:foldl(fun({_N, I}, ok) ->
                            cache_indexes(MsIdx, I#index.table),
                            ok
                    end, ok, MsIdx),
    ok = dets:foldl(fun({T, S}, ok) ->
                            true = ets:insert(?CACHE, {{stats, T}, S}),
                            ok
                    end, ok, MsStat),
    ok.

%% そのテーブルの索引一覧を写し直す。
cache_indexes(MsIdx, TableName) ->
    true = ets:insert(?CACHE, {{indexes, TableName}, indexes_of(MsIdx, TableName)}),
    ok.

handle_call({create_table, TableName, ColumnList}, _From, #st{ms_tables = MsTables} = State) ->
    Reply = case validate(TableName, ColumnList) of
                ok ->
                    case dets:lookup(MsTables, TableName) of
                        [] ->
                            Columns = normalize(ColumnList),
                            %% **順序が要る。** DETS を書いてから ETS に写す。
                            %% 逆にすると、まだ永続化していないものが
                            %% 他のプロセスから見える。
                            ok = dets:insert(MsTables, {TableName, Columns}),
                            ok = dets:sync(MsTables),
                            true = ets:insert(?CACHE, {{table, TableName}, Columns}),
                            ok;
                        [_] ->
                            {error, table_already_exists}
                    end;
                {error, Reason} ->
                    {error, Reason}
            end,
    {reply, Reply, State};

handle_call({drop_table, TableName}, _From,
            #st{ms_tables = MsTables, ms_indexes = MsIdx} = State) ->
    Reply = case dets:lookup(MsTables, TableName) of
                [] ->
                    {error, table_not_found};
                [_] ->
                    %% テーブルに付いていた索引の定義も消す。
                    %% 残すと、同名のテーブルを作り直したときに
                    %% 実体の無い索引があることになる。
                    _ = [dets:delete(MsIdx, I#index.name)
                         || I <- indexes_of(MsIdx, TableName)],
                    ok = dets:delete(State#st.ms_stats, TableName),
                    ok = dets:delete(MsTables, TableName),
                    ok = dets:sync(MsTables),
                    ok = dets:sync(MsIdx),
                    true = ets:delete(?CACHE, {table, TableName}),
                    true = ets:delete(?CACHE, {indexes, TableName}),
                    true = ets:delete(?CACHE, {stats, TableName}),
                    ok
            end,
    {reply, Reply, State};

handle_call({create_index, IndexName, TableName, ColumnName}, _From,
            #st{ms_tables = MsTables, ms_indexes = MsIdx} = State) ->
    Reply =
        case lookup_columns(MsTables, TableName) of
            {error, Reason} ->
                {error, Reason};
            {ok, Columns} ->
                case lists:keyfind(ColumnName, #column.name, Columns) of
                    false ->
                        {error, {column_not_found, ColumnName}};
                    _ ->
                        case dets:lookup(MsIdx, IndexName) of
                            [_] ->
                                {error, index_already_exists};
                            [] ->
                                case [I || I <- indexes_of(MsIdx, TableName),
                                           I#index.column =:= ColumnName] of
                                    [_ | _] ->
                                        {error, already_indexed};
                                    [] ->
                                        Idx = #index{name = IndexName,
                                                     table = TableName,
                                                     column = ColumnName},
                                        ok = dets:insert(MsIdx, {IndexName, Idx}),
                                        ok = dets:sync(MsIdx),
                                        ok = cache_indexes(MsIdx, TableName),
                                        ok
                                end
                        end
                end
        end,
    {reply, Reply, State};

handle_call({drop_index, IndexName}, _From, #st{ms_indexes = MsIdx} = State) ->
    Reply = case dets:lookup(MsIdx, IndexName) of
                [] ->
                    {error, {index_not_found, IndexName}};
                [{IndexName, #index{table = T, column = C}}] ->
                    ok = dets:delete(MsIdx, IndexName),
                    ok = dets:sync(MsIdx),
                    ok = cache_indexes(MsIdx, T),
                    {ok, T, C}
            end,
    {reply, Reply, State};

handle_call({put_stats, TableName, Stats}, _From, #st{ms_stats = MsStat} = State) ->
    ok = dets:insert(MsStat, {TableName, Stats}),
    ok = dets:sync(MsStat),
    true = ets:insert(?CACHE, {{stats, TableName}, Stats}),
    {reply, ok, State};

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, #st{ms_tables = MsTables, ms_indexes = MsIdx, ms_stats = MsStat}) ->
    _ = dets:sync(MsStat),
    _ = dets:close(MsStat),
    _ = dets:sync(MsIdx),
    _ = dets:close(MsIdx),
    _ = dets:close(MsTables),
    ok.

%% テーブルに付いている索引。名前順に揃えておく。
indexes_of(MsIdx, TableName) ->
    All = dets:foldl(fun({_N, I}, Acc) ->
                             case I#index.table =:= TableName of
                                 true -> [I | Acc];
                                 false -> Acc
                             end
                     end, [], MsIdx),
    lists:keysort(#index.name, All).

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
valid_column({Name, Type, Cs}) when is_atom(Name), is_list(Cs) ->
    lists:member(Type, types())
        andalso lists:all(fun(C) -> lists:member(C, [unique, not_null]) end, Cs);
valid_column(_) -> false.

name_of(Name) when is_atom(Name) -> Name;
name_of({Name, _Type}) -> Name;
name_of({Name, _Type, _Cs}) -> Name;
name_of(_) -> undefined.

types() -> [integer, float, varchar, boolean, any].

%% 2つの入力形を #column{} のリストに揃える。
normalize(ColumnList) ->
    normalize(ColumnList, 1, []).

normalize([], _N, Acc) ->
    lists:reverse(Acc);
normalize([{Name, Type, Cs} | T], N, Acc) ->
    normalize(T, N + 1,
              [#column{name = Name, type = Type, position = N, constraints = Cs} | Acc]);
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
