%%%-------------------------------------------------------------------
%%% @doc
%%% ストレージエンジンのファサード。
%%% システムカタログ(sys_tbl_mng)、カラムインデックス(simple_index)、
%%% バッファプール(data_buffer)をまとめ、トランザクションを意識しない
%%% 単発の読み書きAPIを提供する。
%%%
%%% トランザクション制御はこの上位のquery_execが担当する。
%%% query_execはコミット時にここへ確定した行を書き込む。
%%%
%%% カラムインデックスはETS上にしか存在しないため、起動時に
%%% データファイルから読み直して再構築する(rebuild_indexes/0)。
%%% @end
%%%-------------------------------------------------------------------
-module(simple_db_server).
-behaviour(gen_server).

%% Public API
-export([start_link/0, stop/1]).
-export([create_table/3, drop_table/2, insert_data/4, delete_data/3,
         select_data/4, update_data/5, vacuum/2, list_tables/1]).
-export([read_data_oid/2, read_data_oid_with_column/2]).
-export([convert_set_query/2, build_new_val/2, get_tab_column_key/2]).
-export([index_module/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("../include/simple_db_server.hrl").

%%%===================================================================
%%% DB Server APIs
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%----------------------------------------------------------------------
%% @doc テーブルを作る。
%% Returns: ok | {error, table_already_exists} | {error, invalid_column_list}
%%----------------------------------------------------------------------
create_table(Pid, TableName, ColumnList) ->
    gen_server:call(Pid, {create_table, {TableName, ColumnList}}, infinity).

%%----------------------------------------------------------------------
%% @doc テーブルと、その行・インデックス・データファイルを破棄する。
%% Returns: ok | {error, table_not_found}
%%----------------------------------------------------------------------
drop_table(Pid, TableName) ->
    gen_server:call(Pid, {drop_table, {TableName}}, infinity).

%%----------------------------------------------------------------------
%% @doc 行を書き込む。同じOidで再実行しても二重にならない(冪等)。
%% Returns: ok | {error, Reason}
%%----------------------------------------------------------------------
insert_data(Pid, TableName, Oid, Val) ->
    gen_server:call(Pid, {insert, {TableName, Oid, Val}}, infinity).

%%----------------------------------------------------------------------
%% @doc Oidを指定して行を消す。
%% Returns: ok | {error, Reason}
%%----------------------------------------------------------------------
delete_data(Pid, TableName, Oid) ->
    gen_server:call(Pid, {delete, {TableName, Oid}}, infinity).

%%----------------------------------------------------------------------
%% @doc 等値条件に一致する行を返す。
%% Returns: [Val] | {error, table_not_found}
%%----------------------------------------------------------------------
select_data(Pid, TableName, ColName, Val) ->
    gen_server:call(Pid, {select, {TableName, ColName, Val}}, infinity).

%%----------------------------------------------------------------------
%% @doc 等値条件に一致する行を更新する。
%% SetQuery : [{ColumnName, NewVal}, ...]
%% Returns: {ok, UpdatedCount} | {error, Reason}
%%----------------------------------------------------------------------
update_data(Pid, TableName, SetQuery, ColName, Val) ->
    gen_server:call(Pid, {update, {TableName, SetQuery, ColName, Val}}, infinity).

vacuum(Pid, TableName) ->
    gen_server:call(Pid, {vacuum, TableName}, infinity).

list_tables(_Pid) ->
    sys_tbl_mng:list_tables(whereis(sys_tbl_mng)).

%%%===================================================================
%%% Callback functions of gen_server
%%%===================================================================

init([]) ->
    ok = (index_module()):init(),
    ok = rebuild_indexes(),
    {ok, []}.

handle_call({create_table, {TableName, ColumnList}}, _From, State) ->
    Reply = case sys_tbl_mng:create_table(whereis(sys_tbl_mng), TableName, ColumnList) of
                ok ->
                    ok = (index_module()):create_table(TableName, ColumnList),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end,
    {reply, Reply, State};

handle_call({drop_table, {TableName}}, _From, State) ->
    %% インデックスを先に落とすため、カタログを消す前にカラム一覧を取る
    Reply = case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
                {error, table_not_found} ->
                    {error, table_not_found};
                {ok, ColumnList} ->
                    ok = (index_module()):drop_table(TableName, ColumnList),
                    ok = data_buffer:drop_table(whereis(data_buffer), TableName),
                    ok = sys_tbl_mng:drop_table(whereis(sys_tbl_mng), TableName),
                    ok
            end,
    {reply, Reply, State};

handle_call({insert, {TableName, Oid, Val}}, _From, State) ->
    Reply = case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
                {error, table_not_found} ->
                    {error, table_not_found};
                {ok, ColumnList} when length(ColumnList) =/= length(Val) ->
                    {error, column_count_mismatch};
                {ok, ColumnList} ->
                    %% 同じOidの上書きなら、古い値のインデックスを先に外す
                    ok = unindex_existing(TableName, ColumnList, Oid),
                    case data_buffer:write_data(whereis(data_buffer), TableName, Oid, Val) of
                        ok ->
                            ok = (index_module()):insert_index(
                                   TableName, lists:zip(ColumnList, Val), Oid),
                            ok;
                        {error, Reason} ->
                            {error, Reason}
                    end
            end,
    {reply, Reply, State};

handle_call({select, {TableName, ColumnName, Val}}, _From, State) ->
    Reply = case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
                false ->
                    {error, table_not_found};
                true ->
                    OidList = (index_module()):select_index(TableName, ColumnName, Val),
                    Rows = [read_data_oid(TableName, Oid) || Oid <- OidList],
                    [R || R <- Rows, R =/= not_found]
            end,
    {reply, Reply, State};

handle_call({update, {TableName, SetQuery, ColumnName, Val}}, _From, State) ->
    {reply, do_update(TableName, SetQuery, ColumnName, Val), State};

%% delete処理は、行を消してから各カラムインデックスの参照を外す
handle_call({delete, {TableName, Oid}}, _From, State) ->
    Reply = case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
                {error, table_not_found} ->
                    {error, table_not_found};
                {ok, ColumnList} ->
                    ok = unindex_existing(TableName, ColumnList, Oid),
                    case data_buffer:delete_data(whereis(data_buffer), Oid) of
                        ok -> ok;
                        %% すでに消えている場合も削除は成功とみなす(冪等)
                        {error, oid_not_found} -> ok;
                        {error, Reason} -> {error, Reason}
                    end
            end,
    {reply, Reply, State};

handle_call({vacuum, TableName}, _From, State) ->
    {reply, data_buffer:vacuum(whereis(data_buffer), TableName), State};

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Datastore mng functions
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc Oidから行を読む。query_execがクライアントプロセス上から直接呼ぶ。
%% Returns: Val | not_found | {error, table_not_found}
%%----------------------------------------------------------------------
read_data_oid(TableName, OidList) when is_list(OidList) ->
    [read_data_oid(TableName, Oid) || Oid <- OidList];
read_data_oid(TableName, Oid) ->
    case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
        false ->
            {error, table_not_found};
        true ->
            case data_buffer:read_data(whereis(data_buffer), Oid) of
                {error, oid_not_found} -> not_found;
                Val -> Val
            end
    end.

%%----------------------------------------------------------------------
%% @doc 行をカラム名付きで読む。
%% Returns: [{ColName, Val}] | not_found | {error, table_not_found}
%%----------------------------------------------------------------------
read_data_oid_with_column(TableName, Oid) ->
    case read_data_oid(TableName, Oid) of
        not_found ->
            not_found;
        {error, Reason} ->
            {error, Reason};
        Data ->
            {ok, ColumnList} = sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName),
            lists:zip(ColumnList, Data)
    end.

%%%===================================================================
%%% Update
%%%===================================================================

do_update(TableName, SetQuery, ColumnName, Val) ->
    case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
        {error, table_not_found} ->
            {error, table_not_found};
        {ok, ColumnList} ->
            case unknown_columns(SetQuery, ColumnList) of
                [] ->
                    SetQueryConverted = convert_set_query(SetQuery, ColumnList),
                    OidList = (index_module()):select_index(TableName, ColumnName, Val),
                    update_rows(TableName, ColumnList, SetQueryConverted, OidList, 0);
                Unknown ->
                    {error, {unknown_columns, Unknown}}
            end
    end.

update_rows(_TableName, _ColumnList, _SetQueryConverted, [], Count) ->
    {ok, Count};
update_rows(TableName, ColumnList, SetQueryConverted, [Oid | Rest], Count) ->
    case read_data_oid(TableName, Oid) of
        not_found ->
            update_rows(TableName, ColumnList, SetQueryConverted, Rest, Count);
        OldVal ->
            NewVal = build_new_val(OldVal, SetQueryConverted),
            case data_buffer:update_data(whereis(data_buffer), TableName, Oid, NewVal) of
                {ok, _PhysLoc} ->
                    %% 値が変わったカラムだけインデックスを張り替える
                    lists:foreach(
                      fun({ColumnN, OldColVal, NewColVal}) ->
                              ok = (index_module()):update_index(
                                     TableName, ColumnN, OldColVal, NewColVal, Oid)
                      end, lists:zip3(ColumnList, OldVal, NewVal)),
                    update_rows(TableName, ColumnList, SetQueryConverted, Rest, Count + 1);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% SetQueryに定義されていないカラムが混ざっていないかを調べる。
unknown_columns(SetQuery, ColumnList) ->
    [Col || {Col, _V} <- SetQuery, not lists:member(Col, ColumnList)].

%% すでに同じOidの行がある場合、その古い値のインデックス参照を外す。
%% 外しておかないと、上書き後に古い値でも引けてしまう。
unindex_existing(TableName, ColumnList, Oid) ->
    case read_data_oid(TableName, Oid) of
        not_found ->
            ok;
        {error, _} ->
            ok;
        OldVal when length(OldVal) =:= length(ColumnList) ->
            lists:foreach(fun({ColName, ColVal}) ->
                                  ok = (index_module()):delete_index(TableName, ColName, ColVal, Oid)
                          end, lists:zip(ColumnList, OldVal)),
            ok;
        _ ->
            ok
    end.

%%%===================================================================
%%% 起動時のインデックス再構築
%%%===================================================================

%% カラムインデックスはETS上にしか存在しないので、
%% 起動時にカタログとデータファイルから作り直す。
%% これがないと再起動後のSELECTが何も返さなくなる。
rebuild_indexes() ->
    {ok, Tables} = sys_tbl_mng:list_tables(whereis(sys_tbl_mng)),
    lists:foreach(fun rebuild_table_index/1, Tables),
    ok.

rebuild_table_index(TableName) ->
    {ok, ColumnList} = sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName),
    ok = (index_module()):create_table(TableName, ColumnList),
    Rows = data_buffer:all_rows(whereis(data_buffer), TableName),
    lists:foreach(
      fun({Oid, Val}) when length(Val) =:= length(ColumnList) ->
              ok = (index_module()):insert_index(TableName, lists:zip(ColumnList, Val), Oid);
         ({_Oid, _Val}) ->
              %% カラム数が合わない行はカタログと整合しないので飛ばす
              ok
      end, Rows).

%%%===================================================================
%%% util functions
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc 使用するインデックス実装。
%% simple_index(既定): ETSのハッシュ表。等値検索のみ。
%% index             : B+tree。等値検索に加えて範囲検索ができる。
%% どちらも同じ関数群を備えているので差し替えられる。
%%----------------------------------------------------------------------
index_module() ->
    application:get_env(transaction_db, index_module, simple_index).

%% テーブル名とカラム名からインデックスのキーを作る。
get_tab_column_key(TableName, ColumnName) ->
    simple_index:get_tab_column_key(TableName, ColumnName).

%%----------------------------------------------------------------------
%% @doc 更新後の行を組み立てる。
%% OldVal            -> [apple, 100]
%% SetQueryConverted -> [{1, banana}, {2, 200}]
%% -> [banana, 200]
%%----------------------------------------------------------------------
build_new_val(OldVal, SetQueryConverted) ->
    build_new_val(OldVal, SetQueryConverted, 1, []).

build_new_val([], _SetQuery, _N, Acc) ->
    lists:reverse(Acc);
build_new_val([Old | Rest], SetQuery, N, Acc) ->
    Value = case lists:keyfind(N, 1, SetQuery) of
                {N, NewVal} -> NewVal;
                false -> Old
            end,
    build_new_val(Rest, SetQuery, N + 1, [Value | Acc]).

%%----------------------------------------------------------------------
%% @doc SetQueryのカラム名をカラムの位置に変換する。
%% ([{name, banana}, {price, 200}], [name, price]) -> [{1, banana}, {2, 200}]
%%----------------------------------------------------------------------
convert_set_query(SetQuery, ColumnList) ->
    convert_set_query(SetQuery, ColumnList, 1, []).

convert_set_query(_SetQuery, [], _N, Acc) ->
    lists:reverse(Acc);
convert_set_query(SetQuery, [Col | Rest], N, Acc) ->
    case lists:keyfind(Col, 1, SetQuery) of
        {Col, Val} -> convert_set_query(SetQuery, Rest, N + 1, [{N, Val} | Acc]);
        false -> convert_set_query(SetQuery, Rest, N + 1, Acc)
    end.
