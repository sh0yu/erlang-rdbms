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
-export([scan_open/1, scan_next/1, scan_fold/3]).
-export([oids_matching/3]).
-export([validate_insert/3, validate_delete/1]).
-export([convert_set_query/2, build_new_val/2, get_tab_column_key/2]).
-export([index_module/0]).
-export([create_index/4, drop_index/2, list_indexes/1]).
-export([analyze/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("../include/simple_db_server.hrl").
-include("../include/catalog.hrl").

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
                    %% **索引は張らない。** 以前は全カラムに自動で張っていたが、
                    %% それだと「索引があるか」が常に真になり、プランナの
                    %% アクセスパス選択が退化する。索引は CREATE INDEX で
                    %% 明示的に作る。
                    ok = (index_module()):create_table(TableName, []),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end,
    {reply, Reply, State};

handle_call({drop_table, {TableName}}, _From, State) ->
    %% インデックスを先に落とすため、カタログを消す前にカラム一覧を取る
    Reply = case sys_tbl_mng:get_index_column_list(whereis(sys_tbl_mng), TableName) of
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
    Reply = case validate_insert(TableName, Oid, Val) of
                {error, Reason} ->
                    {error, Reason};
                {ok, ColumnList} ->
                    %% 索引より先に行を書く。
                    %%
                    %% 逆順にすると、書き込みが失敗したときに古い値の索引だけが
                    %% 消えて行が残り、索引とヒープが食い違う(索引検索からは
                    %% 見えず走査からは見える幽霊行になる)。
                    %% 古い値は索引を外すのに要るので、上書きする前に控えておく。
                    OldVal = existing_row(TableName, ColumnList, Oid),
                    IdxCols = indexed_columns(TableName),
                    case data_buffer:write_data(whereis(data_buffer), TableName, Oid, Val) of
                        ok ->
                            ok = unindex(TableName, ColumnList, IdxCols, Oid, OldVal),
                            ok = (index_module()):insert_index(
                                   TableName, only_indexed(IdxCols, ColumnList, Val), Oid),
                            ok;
                        {error, Reason} ->
                            {error, Reason}
                    end
            end,
    {reply, Reply, State};

%% 索引があれば索引で引き、無ければ全表走査に落ちる。
%%
%% **落ちる先が無いと、索引の無いカラムへの検索が黙って空を返す。**
%% 全カラムに自動で索引が張られていた頃はこの経路に入らなかったが、
%% 索引を明示的にするなら走査は必須になる。
%%
%% 走査を gen_server の中で回すのでエンジンを占有するが、
%% トランザクションはどのみち直列なので実害は無い。
handle_call({select, {TableName, ColumnName, Val}}, _From, State) ->
    Reply = case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
                {error, table_not_found} ->
                    {error, table_not_found};
                {ok, ColumnList} ->
                    case lists:member(ColumnName, indexed_columns(TableName)) of
                        true ->
                            OidList = (index_module()):select_index(
                                        TableName, ColumnName, Val),
                            Rows = [read_data_oid(TableName, Oid) || Oid <- OidList],
                            [R || R <- Rows, R =/= not_found];
                        false ->
                            scan_select(TableName, ColumnList, ColumnName, Val)
                    end
            end,
    {reply, Reply, State};

handle_call({analyze, {TableName}}, _From, State) ->
    {reply, do_analyze(TableName), State};

handle_call({create_index, {IndexName, TableName, ColumnName}}, _From, State) ->
    {reply, do_create_index(IndexName, TableName, ColumnName), State};

handle_call({drop_index, {IndexName}}, _From, State) ->
    Reply = case sys_tbl_mng:drop_index(whereis(sys_tbl_mng), IndexName) of
                {error, Reason} -> {error, Reason};
                {ok, Table, Column} ->
                    ok = (index_module()):drop_index(Table, Column),
                    ok
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
                    %% 挿入と同じ理由で、行を消してから索引を外す
                    IdxCols = indexed_columns(TableName),
                    OldVal = existing_row(TableName, ColumnList, Oid),
                    case data_buffer:delete_data(whereis(data_buffer), Oid) of
                        ok ->
                            unindex(TableName, ColumnList, IdxCols, Oid, OldVal);
                        %% すでに消えている場合も削除は成功とみなす(冪等)。
                        %% 索引に参照が残っていれば外しておく。
                        {error, oid_not_found} ->
                            unindex(TableName, ColumnList, IdxCols, Oid, OldVal);
                        {error, Reason} ->
                            {error, Reason}
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
%%% 順次走査
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc テーブルの順次走査。read_data_oid/2と同じく、呼び出し元の
%% プロセスから直接呼ぶ(simple_db_serverのgen_serverを経由しない)。
%% 走査の間じゅうエンジンを占有させないため。
%%----------------------------------------------------------------------
scan_open(TableName) ->
    case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
        false -> {error, table_not_found};
        true -> data_buffer:scan_open(whereis(data_buffer), TableName)
    end.

scan_next(Cursor) ->
    data_buffer:scan_next(whereis(data_buffer), Cursor).

%%----------------------------------------------------------------------
%% @doc テーブルの全行をページ順に畳み込む。
%% Fun は {Oid, Val} を受け取る。
%%----------------------------------------------------------------------
scan_fold(TableName, Fun, Acc0) ->
    case scan_open(TableName) of
        {error, Reason} ->
            {error, Reason};
        {ok, Cursor} ->
            scan_fold_1(Cursor, Fun, Acc0)
    end.

scan_fold_1(Cursor, Fun, Acc) ->
    case scan_next(Cursor) of
        eof -> Acc;
        {rows, Rows, Cursor2} -> scan_fold_1(Cursor2, Fun, lists:foldl(Fun, Acc, Rows))
    end.

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
                    %% 索引が無ければ走査で対象を集める。
                    %% ここを索引だけにしておくと、索引の無いカラムを
                    %% 条件にした UPDATE が黙って0件になる。
                    case oids_matching(TableName, ColumnList, ColumnName, Val) of
                        {error, Reason} ->
                            {error, Reason};
                        OidList ->
                            IdxCols = indexed_columns(TableName),
                            update_rows(TableName, ColumnList, IdxCols,
                                        SetQueryConverted, OidList, 0)
                    end;
                Unknown ->
                    {error, {unknown_columns, Unknown}}
            end
    end.

update_rows(_TableName, _ColumnList, _IdxCols, _SetQueryConverted, [], Count) ->
    {ok, Count};
update_rows(TableName, ColumnList, IdxCols, SetQueryConverted, [Oid | Rest], Count) ->
    case read_data_oid(TableName, Oid) of
        not_found ->
            update_rows(TableName, ColumnList, IdxCols, SetQueryConverted, Rest, Count);
        OldVal ->
            NewVal = build_new_val(OldVal, SetQueryConverted),
            case data_buffer:update_data(whereis(data_buffer), TableName, Oid, NewVal) of
                {ok, _PhysLoc} ->
                    %% 値が変わったカラムだけインデックスを張り替える。
                    %% 索引の無いカラムは触らない(ETSが存在しない)。
                    lists:foreach(
                      fun({ColumnN, OldColVal, NewColVal}) ->
                              case lists:member(ColumnN, IdxCols) of
                                  true ->
                                      ok = (index_module()):update_index(
                                             TableName, ColumnN, OldColVal, NewColVal, Oid);
                                  false -> ok
                              end
                      end, lists:zip3(ColumnList, OldVal, NewVal)),
                    update_rows(TableName, ColumnList, IdxCols, SetQueryConverted,
                                Rest, Count + 1);
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% SetQueryに定義されていないカラムが混ざっていないかを調べる。
unknown_columns(SetQuery, ColumnList) ->
    [Col || {Col, _V} <- SetQuery, not lists:member(Col, ColumnList)].

%% すでに同じOidの行がある場合、その古い値のインデックス参照を外す。
%% 外しておかないと、上書き後に古い値でも引けてしまう。
%%----------------------------------------------------------------------
%% @doc 書き込む前に、すでに入っている行を控える。索引を外すのに要る。
%% Returns: [Val] | none
%%----------------------------------------------------------------------
existing_row(TableName, ColumnList, Oid) ->
    case read_data_oid(TableName, Oid) of
        OldVal when is_list(OldVal), length(OldVal) =:= length(ColumnList) -> OldVal;
        _ -> none
    end.

%% 控えておいた古い値の索引参照を外す。
unindex(_TableName, _ColumnList, _IdxCols, _Oid, none) ->
    ok;
unindex(TableName, ColumnList, IdxCols, Oid, OldVal) ->
    lists:foreach(fun({ColName, ColVal}) ->
                          ok = (index_module()):delete_index(TableName, ColName, ColVal, Oid)
                  end, only_indexed(IdxCols, ColumnList, OldVal)),
    ok.

%%----------------------------------------------------------------------
%% 索引まわりの補助
%%
%% 索引の張られたカラムはカタログにしかない。1行ごとに引くとDETSを
%% 舐め直すことになるので、**操作の頭で1回だけ引いて持ち回る**。
%%----------------------------------------------------------------------
indexed_columns(TableName) ->
    case sys_tbl_mng:get_index_column_list(whereis(sys_tbl_mng), TableName) of
        {ok, Cols}      -> Cols;
        {error, _}      -> []
    end.

%% {カラム名, 値} の組を、索引のあるカラムだけに絞る。
only_indexed(IdxCols, ColumnList, Val) ->
    [P || {C, _} = P <- lists:zip(ColumnList, Val), lists:member(C, IdxCols)].

%% 索引が無いカラムへの等値検索。全表走査して突き合わせる。
scan_select(TableName, ColumnList, ColumnName, Val) ->
    case scan_matching(TableName, ColumnList, ColumnName, Val) of
        {error, Reason} -> {error, Reason};
        Pairs           -> [Row || {_Oid, Row} <- Pairs]
    end.

%%----------------------------------------------------------------------
%% @doc 条件に合う行のOid。索引があれば索引を、無ければ走査を使う。
%% read_data_oid/2 と同じく、呼び出し元のプロセスから直接呼ぶ。
%%----------------------------------------------------------------------
oids_matching(TableName, ColumnName, Val) ->
    case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
        {error, Reason}  -> {error, Reason};
        {ok, ColumnList} -> oids_matching(TableName, ColumnList, ColumnName, Val)
    end.

oids_matching(TableName, ColumnList, ColumnName, Val) ->
    case lists:member(ColumnName, indexed_columns(TableName)) of
        true ->
            (index_module()):select_index(TableName, ColumnName, Val);
        false ->
            case scan_matching(TableName, ColumnList, ColumnName, Val) of
                {error, Reason} -> {error, Reason};
                Pairs           -> [Oid || {Oid, _Row} <- Pairs]
            end
    end.

scan_matching(TableName, ColumnList, ColumnName, Val) ->
    case position_of(ColumnName, ColumnList) of
        not_found ->
            {error, {column_not_found, ColumnName}};
        Pos ->
            lists:reverse(
              scan_fold(TableName,
                        fun({Oid, Row}, Acc) when length(Row) >= Pos ->
                                case lists:nth(Pos, Row) of
                                    Val -> [{Oid, Row} | Acc];
                                    _   -> Acc
                                end;
                           ({_Oid, _Row}, Acc) ->
                                Acc
                        end, []))
    end.

%% カラム名から行タプル内の位置(1始まり)。
position_of(Name, List) -> position_of(Name, List, 1).

position_of(_Name, [], _N)       -> not_found;
position_of(Name, [Name | _], N) -> N;
position_of(Name, [_ | T], N)    -> position_of(Name, T, N + 1).

%%----------------------------------------------------------------------
%% ANALYZE。1回走査して行数と各カラムの異なり値・NULL数・最小最大を採る。
%% all を渡すと全テーブル。
%%----------------------------------------------------------------------
do_analyze(all) ->
    {ok, Tables} = sys_tbl_mng:list_tables(whereis(sys_tbl_mng)),
    Results = [{T, do_analyze(T)} || T <- Tables],
    case [R || {_T, {error, _} = R} <- Results] of
        []      -> {ok, length(Results)};
        [E | _] -> E
    end;
do_analyze(TableName) ->
    case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
        {error, Reason} ->
            {error, Reason};
        {ok, Columns} ->
            {Count, Acc} =
                scan_fold(TableName,
                          fun({_Oid, Row}, {N, A}) ->
                                  {N + 1, sql_stats:accumulate(Row, A)}
                          end, {0, sql_stats:empty(Columns)}),
            Stats = (sql_stats:finish(Columns, Count, Acc))#table_stats{
                      table = TableName,
                      analyzed = erlang:system_time(second)},
            ok = sys_tbl_mng:put_stats(whereis(sys_tbl_mng), TableName, Stats),
            {ok, Count}
    end.

%% CREATE INDEX。定義を登録してから、既存の行で索引を作り直す。
do_create_index(IndexName, TableName, ColumnName) ->
    case sys_tbl_mng:create_index(whereis(sys_tbl_mng), IndexName, TableName, ColumnName) of
        {error, Reason} ->
            {error, Reason};
        ok ->
            {ok, ColumnList} = sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName),
            ok = (index_module()):create_index(TableName, ColumnName),
            Pos = position_of(ColumnName, ColumnList),
            scan_fold(TableName,
                      fun({Oid, Row}, Acc) when length(Row) >= Pos ->
                              ok = (index_module()):insert_index(
                                     TableName, [{ColumnName, lists:nth(Pos, Row)}], Oid),
                              Acc;
                         ({_Oid, _Row}, Acc) ->
                              Acc
                      end, ok)
    end.

%%----------------------------------------------------------------------
%% @doc 挿入が成功しうるかを、共有データを一切変更せずに確かめる。
%% コミットが途中で失敗して千切れるのを防ぐため、適用の前に全件これを通す。
%% Returns: {ok, ColumnList} | {error, Reason}
%%----------------------------------------------------------------------
validate_insert(TableName, Oid, Val) ->
    case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
        {error, table_not_found} ->
            {error, table_not_found};
        {ok, ColumnList} when length(ColumnList) =/= length(Val) ->
            {error, column_count_mismatch};
        {ok, ColumnList} ->
            case data_buffer:row_fits(Oid, Val) of
                true -> {ok, ColumnList};
                false -> {error, row_too_large}
            end
    end.

%%----------------------------------------------------------------------
%% @doc 削除が成功しうるかを確かめる。
%%----------------------------------------------------------------------
validate_delete(TableName) ->
    case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
        true -> ok;
        false -> {error, table_not_found}
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
    IdxCols = indexed_columns(TableName),
    ok = (index_module()):create_table(TableName, IdxCols),
    %% 新しい順次走査を使う。起動のたびに走るので、これが走査の
    %% 実利用者となり、既存の再起動テストがそのまま回帰検出器になる。
    scan_fold(TableName,
              fun({Oid, Val}, Acc) when length(Val) =:= length(ColumnList) ->
                      ok = (index_module()):insert_index(
                             TableName, only_indexed(IdxCols, ColumnList, Val), Oid),
                      Acc;
                 ({_Oid, _Val}, Acc) ->
                      %% カラム数が合わない行はカタログと整合しないので飛ばす
                      Acc
              end, ok).

%%%===================================================================
%%% util functions
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc 使用するインデックス実装。
%% simple_index(既定): ETSのハッシュ表。等値検索のみ。
%% index             : B+tree。等値検索に加えて範囲検索ができる。
%% どちらも同じ関数群を備えているので差し替えられる。
%%----------------------------------------------------------------------
%%----------------------------------------------------------------------
%% @doc 索引を作る / 落とす / 一覧する。
%%----------------------------------------------------------------------
%% @doc 統計を採る。TableName に all を渡すと全テーブル。
analyze(Pid, TableName) ->
    gen_server:call(Pid, {analyze, {TableName}}, infinity).

create_index(Pid, IndexName, TableName, ColumnName) ->
    gen_server:call(Pid, {create_index, {IndexName, TableName, ColumnName}}, infinity).

drop_index(Pid, IndexName) ->
    gen_server:call(Pid, {drop_index, {IndexName}}, infinity).

list_indexes(_Pid) ->
    sys_tbl_mng:list_indexes(whereis(sys_tbl_mng)).

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
