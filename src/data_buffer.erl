%%%-------------------------------------------------------------------
%%% @doc
%%% バッファプール。ディスク上のページをメモリ上のバッファに読み込み、
%%% オブジェクトID(Oid)単位の読み書きを提供する。
%%%
%%% 管理しているデータ
%%%   [ETS] buf_info_list : 各バッファフレームがどのテーブル・ページを
%%%                         保持しているか(#buf_info{})
%%%   [ETS] fd_tables     : テーブルごとのデータファイルプロセス(file_mng)
%%%   [ETS] bufN          : バッファフレーム実体。{#phys_loc{}, Data}
%%%   [DETS]oid_phys_loc  : Oid -> #phys_loc{} の対応
%%%   [DETS]vacuum_oid    : 削除済みで未回収のページ。vacuumで参照する
%%%
%%% 書き込みはライトスルー。write_data/update_data/delete_dataは必ず
%%% file_mng:write_page/3まで到達してから復帰するため、バッファ上に
%%% ディスクへ未反映のダーティページは存在しない。したがってフレームを
%%% 追い出す際にフラッシュは不要。
%%% @end
%%%-------------------------------------------------------------------
-module(data_buffer).
-behaviour(gen_server).

%% Public API
-export([start_link/0, stop/1]).
-export([read_data/2, write_data/4, update_data/4, delete_data/2,
         drop_table/2, vacuum/2, flush/1, all_rows/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("../include/simple_db_server.hrl").

-define(BUF_N, 8).

-record(buf_info, {
    buf_name,
    table_name = nil,
    page_id = nil,
    empty_size = 0,
    slot_count = 0,
    timestamp = 0
}).

-record(st, {
    data_dir
}).

%%%===================================================================
%%% Public API
%%%===================================================================

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%----------------------------------------------------------------------
%% @doc Oidに対応する行を読む。
%% Returns: Data | {error, oid_not_found}
%%----------------------------------------------------------------------
read_data(Pid, Oid) ->
    gen_server:call(Pid, {read_data, Oid}, infinity).

%%----------------------------------------------------------------------
%% @doc 行を書き込む。同じOidに対する再実行は冪等(上書き)。
%% Returns: ok | {error, Reason}
%%----------------------------------------------------------------------
write_data(Pid, TableName, Oid, Val) ->
    gen_server:call(Pid, {write_data, TableName, Oid, Val}, infinity).

%%----------------------------------------------------------------------
%% @doc 行を更新する。更新後のデータが元のページに収まらない場合は
%% 空きのあるページへ移し、oid_phys_locを更新する。
%% Returns: {ok, #phys_loc{}} | {error, Reason}
%%----------------------------------------------------------------------
update_data(Pid, TableName, Oid, Val) ->
    gen_server:call(Pid, {update_data, TableName, Oid, Val}, infinity).

%%----------------------------------------------------------------------
%% @doc 行を削除する。スロットを空きスロットにし、Oidの対応も削除する。
%% Returns: ok | {error, oid_not_found}
%%----------------------------------------------------------------------
delete_data(Pid, Oid) ->
    gen_server:call(Pid, {delete_data, Oid}, infinity).

%%----------------------------------------------------------------------
%% @doc テーブルのデータファイルとOid対応をまとめて破棄する。
%%----------------------------------------------------------------------
drop_table(Pid, TableName) ->
    gen_server:call(Pid, {drop_table, TableName}, infinity).

%%----------------------------------------------------------------------
%% @doc 削除により空いたスロットを詰め、末尾の空ページを切り捨てる。
%% 行の移動に伴いoid_phys_locを更新する。
%% Returns: {ok, ReclaimedPages}
%%----------------------------------------------------------------------
vacuum(Pid, TableName) ->
    gen_server:call(Pid, {vacuum, TableName}, infinity).

%%----------------------------------------------------------------------
%% @doc DETSの内容をディスクに同期する。
%%----------------------------------------------------------------------
flush(Pid) ->
    gen_server:call(Pid, flush, infinity).

%%----------------------------------------------------------------------
%% @doc テーブルの全行を返す。再起動後のインデックス再構築で使う。
%% Returns: [{Oid, Val}]
%%----------------------------------------------------------------------
all_rows(Pid, TableName) ->
    gen_server:call(Pid, {all_rows, TableName}, infinity).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    DataDir = data_dir(),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ets:new(buf_info_list, [set, named_table, public]),
    ets:new(fd_tables, [set, named_table, public]),
    {ok, oid_phys_loc} =
        dets:open_file(oid_phys_loc, [{file, filename:join(DataDir, "oid_phys_loc.sys")}]),
    {ok, vacuum_oid} =
        dets:open_file(vacuum_oid, [{file, filename:join(DataDir, "vacuum_oid.sys")}, {type, bag}]),
    ok = init_buf_frames(?BUF_N),
    {ok, #st{data_dir = DataDir}}.

handle_call({read_data, Oid}, _From, State) ->
    Reply = case oid2physloc(Oid) of
                oid_not_found ->
                    {error, oid_not_found};
                #phys_loc{table_name = TableName, page_id = PageId} = PhysLoc ->
                    Buf = load_page(TableName, PageId),
                    get_data_buf(Buf, PhysLoc)
            end,
    {reply, Reply, State};

handle_call({write_data, TableName, Oid, Data}, _From, State) ->
    {reply, do_write(TableName, Oid, Data), State};

handle_call({update_data, TableName, Oid, Data}, _From, State) ->
    Reply = case do_write(TableName, Oid, Data) of
                ok -> {ok, oid2physloc(Oid)};
                {error, Reason} -> {error, Reason}
            end,
    {reply, Reply, State};

handle_call({delete_data, Oid}, _From, State) ->
    {reply, do_delete(Oid), State};

handle_call({drop_table, TableName}, _From, State) ->
    {reply, do_drop_table(TableName), State};

handle_call({vacuum, TableName}, _From, State) ->
    {reply, do_vacuum(TableName), State};

handle_call({all_rows, TableName}, _From, State) ->
    Rows = [{Oid, read_row(TableName, Oid)} || Oid <- lists:sort(table_oids(TableName))],
    {reply, [{Oid, V} || {Oid, V} <- Rows, V =/= {error, oid_not_found}], State};

handle_call(flush, _From, State) ->
    _ = dets:sync(oid_phys_loc),
    _ = dets:sync(vacuum_oid),
    {reply, ok, State};

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    _ = [file_mng:close(Fd) || {_T, Fd} <- ets:tab2list(fd_tables)],
    _ = dets:close(oid_phys_loc),
    _ = dets:close(vacuum_oid),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% 読み書きの実体
%%%===================================================================

%% 新規挿入と上書きの両方を扱う。
%% 既存Oidの場合は同じPhysLocに書き戻し、収まらなければ別ページへ移す。
do_write(TableName, Oid, Data) ->
    case file_mng:payload_size(Data) > file_mng:max_payload_size() of
        true ->
            {error, row_too_large};
        false ->
            case oid2physloc(Oid) of
                oid_not_found ->
                    insert_new(TableName, Oid, Data);
                #phys_loc{table_name = TableName} = PhysLoc ->
                    overwrite(TableName, Oid, PhysLoc, Data);
                #phys_loc{table_name = Other} ->
                    {error, {oid_belongs_to_other_table, Other}}
            end
    end.

%% 空きのあるページを探して新しいスロットに書き込む。
insert_new(TableName, Oid, Data) ->
    Need = file_mng:required_size(Data),
    Buf = acquire_page_with_space(TableName, Need),
    [{Buf, #buf_info{page_id = PageId, slot_count = SlotCount}}] =
        ets:lookup(buf_info_list, Buf),
    Slot = next_free_slot(Buf, TableName, PageId, SlotCount),
    PhysLoc = #phys_loc{table_name = TableName, page_id = PageId, slot = Slot},
    case store(TableName, Buf, PageId, PhysLoc, Data) of
        ok ->
            ok = dets:insert(oid_phys_loc, {Oid, PhysLoc}),
            ok;
        {error, page_overflow} ->
            %% EmptySizeの見積りより実際のページが詰まっていたケース。
            %% そのページを候補から外して次のページへ回す。
            ets:delete(Buf, PhysLoc),
            insert_new_from(TableName, Oid, Data, PageId + 1);
        {error, Reason} ->
            {error, Reason}
    end.

insert_new_from(TableName, Oid, Data, FromPageId) ->
    Need = file_mng:required_size(Data),
    Fd = get_fd(TableName),
    PageId = find_page_with_space(Fd, Need, FromPageId),
    Buf = load_page(TableName, PageId),
    [{Buf, #buf_info{slot_count = SlotCount}}] = ets:lookup(buf_info_list, Buf),
    Slot = next_free_slot(Buf, TableName, PageId, SlotCount),
    PhysLoc = #phys_loc{table_name = TableName, page_id = PageId, slot = Slot},
    case store(TableName, Buf, PageId, PhysLoc, Data) of
        ok ->
            ok = dets:insert(oid_phys_loc, {Oid, PhysLoc}),
            ok;
        {error, page_overflow} ->
            ets:delete(Buf, PhysLoc),
            insert_new_from(TableName, Oid, Data, PageId + 1);
        {error, Reason} ->
            {error, Reason}
    end.

%% 既存のPhysLocに書き戻す。収まらなければ古いスロットを消して移動する。
overwrite(TableName, Oid, #phys_loc{page_id = PageId} = PhysLoc, Data) ->
    Buf = load_page(TableName, PageId),
    Old = ets:lookup(Buf, PhysLoc),
    case store(TableName, Buf, PageId, PhysLoc, Data) of
        ok ->
            ok;
        {error, page_overflow} ->
            %% 元のページに戻せないので、古いスロットを解放してから移動する
            restore(Buf, PhysLoc, Old),
            case do_delete(Oid) of
                ok -> insert_new(TableName, Oid, Data);
                {error, Reason} -> {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.

do_delete(Oid) ->
    case oid2physloc(Oid) of
        oid_not_found ->
            {error, oid_not_found};
        #phys_loc{table_name = TableName, page_id = PageId} = PhysLoc ->
            Buf = load_page(TableName, PageId),
            ets:delete(Buf, PhysLoc),
            case flush_page(TableName, Buf, PageId) of
                ok ->
                    ok = dets:delete(oid_phys_loc, Oid),
                    ok = dets:insert(vacuum_oid, {TableName, PageId}),
                    ok;
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% バッファに値を入れてページごとディスクに書き戻す。
store(TableName, Buf, PageId, PhysLoc, Data) ->
    ets:insert(Buf, {PhysLoc, Data}),
    flush_page(TableName, Buf, PageId).

restore(Buf, PhysLoc, []) ->
    ets:delete(Buf, PhysLoc);
restore(Buf, _PhysLoc, [Entry]) ->
    ets:insert(Buf, Entry).

%% バッファの内容をページとしてディスクに書き出し、buf_infoを更新する。
flush_page(TableName, Buf, PageId) ->
    SlotDataList = [#slot{slot_n = SlotN, data = Data}
                    || {#phys_loc{slot = SlotN}, Data} <- ets:tab2list(Buf)],
    Fd = get_fd(TableName),
    case file_mng:write_page(Fd, PageId, #disk_data{data_list = SlotDataList}) of
        {ok, #disk_data{empty_size = EmptySize, slot_count = SlotCount}} ->
            ets:insert(buf_info_list,
                       {Buf, #buf_info{buf_name = Buf,
                                       table_name = TableName,
                                       page_id = PageId,
                                       empty_size = EmptySize,
                                       slot_count = SlotCount,
                                       timestamp = erlang:system_time(nanosecond)}}),
            ok;
        {error, Reason} ->
            {error, Reason}
    end.

%%%===================================================================
%%% ページとバッファフレームの割り当て
%%%===================================================================

%% Needバイトの空きがあるページをバッファ上に用意して、そのフレーム名を返す。
acquire_page_with_space(TableName, Need) ->
    case find_buffered_page_with_space(TableName, Need) of
        no_plenty_space_buf ->
            Fd = get_fd(TableName),
            PageId = find_page_with_space(Fd, Need, 0),
            load_page(TableName, PageId);
        Buf ->
            Buf
    end.

%% すでにバッファ上にあるページから空きのあるものを探す。
find_buffered_page_with_space(TableName, Need) ->
    Candidates = ets:match_object(
                   buf_info_list,
                   {'_', #buf_info{buf_name = '_', table_name = TableName,
                                   page_id = '_', empty_size = '_',
                                   slot_count = '_', timestamp = '_'}}),
    find_buffered_page_with_space_1(Candidates, Need).

find_buffered_page_with_space_1([], _Need) ->
    no_plenty_space_buf;
find_buffered_page_with_space_1([{BufName, #buf_info{empty_size = EmptySize}} | _], Need)
  when EmptySize >= Need ->
    BufName;
find_buffered_page_with_space_1([_ | Rest], Need) ->
    find_buffered_page_with_space_1(Rest, Need).

%% ディスク上のページを先頭から走査して、空きのあるページIDを返す。
%% 見つからなければ末尾に新しいページを作る(そのページIDを返す)。
find_page_with_space(Fd, Need, PageId) ->
    case file_mng:get_page_header(Fd, PageId) of
        eof ->
            PageId;
        {EmptySize, _SlotCount} when EmptySize >= Need ->
            PageId;
        _ ->
            find_page_with_space(Fd, Need, PageId + 1)
    end.

%% ページ内の未使用スロット番号のうち最小のものを返す。
%% 削除されたスロットを再利用することでページが虫食いのまま伸びるのを防ぐ。
next_free_slot(Buf, TableName, PageId, SlotCount) ->
    Used = [S || {#phys_loc{slot = S}, _} <- ets:tab2list(Buf)],
    next_free_slot_1(1, SlotCount, lists:sort(Used), TableName, PageId).

next_free_slot_1(N, SlotCount, _Used, _T, _P) when N > SlotCount ->
    N;
next_free_slot_1(N, SlotCount, Used, T, P) ->
    case lists:member(N, Used) of
        true -> next_free_slot_1(N + 1, SlotCount, Used, T, P);
        false -> N
    end.

%%----------------------------------------------------------------------
%% ページをバッファに載せる。すでに載っていればそのフレームを返す。
%%----------------------------------------------------------------------
load_page(TableName, PageId) ->
    case is_data_in_buf(TableName, PageId) of
        false ->
            FreeBuf = case get_empty_buf() of
                          none -> get_oldest_buf();
                          EmptyBuf -> EmptyBuf
                      end,
            NewBufInfo = read_page_into(TableName, PageId, FreeBuf),
            ets:insert(buf_info_list, {FreeBuf, NewBufInfo}),
            FreeBuf;
        BufName ->
            touch_buf(BufName),
            BufName
    end.

%% 取得したいページがバッファに載っているかを調べる。
%% -> false | BufName
is_data_in_buf(TableName, PageId) ->
    case ets:match_object(
           buf_info_list,
           {'_', #buf_info{buf_name = '_', table_name = TableName, page_id = PageId,
                           empty_size = '_', slot_count = '_', timestamp = '_'}}) of
        [] -> false;
        [{BufName, _BufInfo} | _] -> BufName
    end.

touch_buf(BufName) ->
    case ets:lookup(buf_info_list, BufName) of
        [{BufName, BufInfo}] ->
            ets:insert(buf_info_list,
                       {BufName, BufInfo#buf_info{timestamp = erlang:system_time(nanosecond)}});
        [] ->
            ok
    end.

%% バッファからデータを取り出す。
get_data_buf(BufName, PhysLoc) ->
    case ets:lookup(BufName, PhysLoc) of
        [] -> {error, oid_not_found};
        [{_PhysLoc, Val}] -> Val
    end.

%% ディスクのページをフレームに読み込む。
%% ライトスルーのためフレームの旧内容は破棄してよい。
read_page_into(TableName, PageId, FreeBuf) ->
    Fd = get_fd(TableName),
    #disk_data{empty_size = EmptySize, slot_count = SlotCount, data_list = SlotDataList} =
        case file_mng:load_page(Fd, PageId) of
            eof -> new_page_disk_data();
            {ok, DiskData} -> DiskData
        end,
    reset_buf(FreeBuf),
    lists:foreach(
      fun(#slot{slot_n = Slot, data = Data}) ->
              ets:insert(FreeBuf,
                         {#phys_loc{table_name = TableName, page_id = PageId, slot = Slot}, Data})
      end, SlotDataList),
    #buf_info{buf_name = FreeBuf, table_name = TableName, page_id = PageId,
              empty_size = EmptySize, slot_count = SlotCount,
              timestamp = erlang:system_time(nanosecond)}.

new_page_disk_data() ->
    #disk_data{empty_size = file_mng:page_size() - file_mng:header_size(),
               slot_count = 0, data_list = []}.

reset_buf(BufName) ->
    ets:delete_all_objects(BufName).

init_buf_frames(0) ->
    ok;
init_buf_frames(N) ->
    EtsName = buf_name(N),
    ets:new(EtsName, [set, named_table, public]),
    ets:insert(buf_info_list, {EtsName, #buf_info{buf_name = EtsName}}),
    init_buf_frames(N - 1).

buf_name(N) ->
    list_to_atom("buf" ++ integer_to_list(N)).

%% 未使用のフレームを返す。なければ none。
get_empty_buf() ->
    case ets:match_object(buf_info_list,
                          {'_', #buf_info{buf_name = '_', table_name = nil, page_id = nil,
                                          empty_size = '_', slot_count = '_', timestamp = '_'}}) of
        [] -> none;
        [{BufName, _BufInfo} | _] -> BufName
    end.

%% 最も古くに使われたフレームを返す(LRU)。
get_oldest_buf() ->
    [{BufName, _} | _] =
        lists:sort(fun({_, #buf_info{timestamp = A}}, {_, #buf_info{timestamp = B}}) ->
                           A =< B
                   end, ets:tab2list(buf_info_list)),
    BufName.

%%%===================================================================
%%% テーブル破棄とvacuum
%%%===================================================================

do_drop_table(TableName) ->
    %% このテーブルのページを載せているフレームを解放する
    lists:foreach(
      fun({BufName, #buf_info{table_name = T}}) when T =:= TableName ->
              reset_buf(BufName),
              ets:insert(buf_info_list, {BufName, #buf_info{buf_name = BufName}});
         (_) ->
              ok
      end, ets:tab2list(buf_info_list)),
    %% このテーブルに属するOidの対応を削除する
    lists:foreach(fun(Oid) -> ok = dets:delete(oid_phys_loc, Oid) end, table_oids(TableName)),
    ok = dets:delete(vacuum_oid, TableName),
    %% データファイルを閉じて削除する
    case ets:lookup(fd_tables, TableName) of
        [] ->
            ok;
        [{TableName, Fd}] ->
            ok = file_mng:close(Fd),
            ets:delete(fd_tables, TableName)
    end,
    _ = file:delete(table_filepath(TableName)),
    ok.

%% 削除によって空いたスロットを前方のページに詰め直し、
%% 末尾の空ページをファイルから切り捨てる。
do_vacuum(TableName) ->
    Fd = get_fd(TableName),
    PageCount = file_mng:page_count(Fd),
    %% 全行を読み出してから、先頭ページから順に詰め直す
    Values = [{Oid, read_row(TableName, Oid)} || Oid <- lists:sort(table_oids(TableName))],
    Live = [{Oid, V} || {Oid, V} <- Values, V =/= {error, oid_not_found}],
    %% 一旦すべてのページを捨てる
    lists:foreach(fun({BufName, #buf_info{table_name = T}}) when T =:= TableName ->
                          reset_buf(BufName),
                          ets:insert(buf_info_list, {BufName, #buf_info{buf_name = BufName}});
                     (_) ->
                          ok
                  end, ets:tab2list(buf_info_list)),
    lists:foreach(fun({Oid, _}) -> ok = dets:delete(oid_phys_loc, Oid) end, Live),
    ok = file_mng:truncate(Fd),
    %% 先頭から詰め直す
    lists:foreach(fun({Oid, Val}) -> ok = insert_new(TableName, Oid, Val) end, Live),
    ok = dets:delete(vacuum_oid, TableName),
    NewPageCount = file_mng:page_count(Fd),
    {ok, max(0, PageCount - NewPageCount)}.

read_row(TableName, Oid) ->
    case oid2physloc(Oid) of
        oid_not_found ->
            {error, oid_not_found};
        #phys_loc{table_name = TableName, page_id = PageId} = PhysLoc ->
            Buf = load_page(TableName, PageId),
            get_data_buf(Buf, PhysLoc);
        _ ->
            {error, oid_not_found}
    end.

%%%===================================================================
%%% ファイルとOidの対応
%%%===================================================================

%% テーブルに対応するデータファイルプロセスを返す。未オープンなら開く。
get_fd(TableName) ->
    case ets:lookup(fd_tables, TableName) of
        [] ->
            {ok, Fd} = file_mng:open(table_filepath(TableName)),
            ets:insert(fd_tables, {TableName, Fd}),
            Fd;
        [{_TableName, Fd}] ->
            Fd
    end.

table_filepath(TableName) ->
    filename:join(data_dir(), atom_to_list(TableName) ++ ".dat").

data_dir() ->
    application:get_env(transaction_db, data_dir, "./data").

%% テーブルに属するOidの一覧。
table_oids(TableName) ->
    dets:foldl(fun({Oid, #phys_loc{table_name = T}}, Acc) when T =:= TableName -> [Oid | Acc];
                  (_, Acc) -> Acc
               end, [], oid_phys_loc).

oid2physloc(Oid) ->
    case dets:lookup(oid_phys_loc, Oid) of
        [] -> oid_not_found;
        [{Oid, PhysLoc}] -> PhysLoc
    end.
