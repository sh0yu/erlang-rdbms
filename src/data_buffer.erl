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
%%%
%%% == 走査は gen_server を通さない ==
%%%
%%% 以前はページを1枚読むたびに gen_server:call だった。全接続がこの
%%% 1プロセスの前に並ぶので、走査は並列に走れない。実測では32接続の
%%% 同時走査でメッセージキューが32まで伸びていた(接続数と一致 =
%%% 全員が待っている)。
%%%
%%% フレームは public な ETS なので、**載っているページを読むだけなら
%%% 呼び出し元のプロセスが直接引ける**。そのために「どのページが
%%% どのフレームにあるか」の索引(buf_dir)を置く。
%%%
%%%   buf_dir : {TableName, PageId} -> {BufName, Gen}
%%%
%%% 読み手は
%%%   1. buf_dir を引いて {BufName, Gen} を得る
%%%   2. BufName から行を読む
%%%   3. buf_dir をもう一度引き、まだ同じ {BufName, Gen} かを確かめる
%%% 3で違っていたら、読んでいる最中に追い出されたということなので
%%% gen_server へ回す(そちらがページを読み直す)。
%%%
%%% Gen が要るのは A→B→A と入れ替わった場合を捕まえるため。
%%% フレーム名だけを見ると同じに見えてしまう。
%%%
%%% 追い出す側は「buf_dir から消す → フレームを空にする」の順で進む。
%%% 逆にすると、空になったフレームを読み手が「有効」と見なす。
%%% @end
%%%-------------------------------------------------------------------
-module(data_buffer).
-behaviour(gen_server).

%% Public API
-export([start_link/0, stop/1]).
-export([read_data/2, write_data/4, update_data/4, delete_data/2,
         drop_table/2, vacuum/2, flush/1, all_rows/2, sync/1]).
-export([scan_open/2, scan_next/2]).
-export([row_fits/2]).
-export_type([scan_cursor/0]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-include("../include/simple_db_server.hrl").

%% バッファフレーム数。1フレーム = 1ページ。
%%
%% 以前は 8 固定だった。走査が gen_server の中で直列に走っていたので
%% 少なくても目立たなかったが、並列に走らせると取り合いになる。
-define(DEFAULT_BUF_N, 256).

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

%% 順次走査のカーソル。data_buffer側では状態を持たず、呼び出し側が持ち回る。
%% こうしておくと1つのクエリが複数のカーソルを同時に開ける
%% (nested loop joinで内側を何度も走査する場合に要る)。
%% クローズ漏れによるリソース漏れも原理的に起きない。
-record(scan_cursor, {
    table_name,
    page_id = 0,
    page_count
}).
-opaque scan_cursor() :: #scan_cursor{}.

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
%% @doc ここまでの書き込みを実際にディスクへ落とす。
%%
%% file:pwrite/3 も dets:insert/2 も、呼んだ時点ではOSのページキャッシュや
%% DETSのバッファに入るだけで、ディスクには届いていない。
%% コミットのチェックポイントを書く前にこれを呼ばないと、
%% 「チェックポイントより前は適用済み」という宣言が嘘になる
%% (リカバリが再実行しないのに、実際のデータは失われている)。
%%----------------------------------------------------------------------
sync(Pid) ->
    gen_server:call(Pid, sync, infinity).

%%----------------------------------------------------------------------
%% @doc テーブルの全行を返す。再起動後のインデックス再構築で使う。
%% Returns: [{Oid, Val}]
%%----------------------------------------------------------------------
all_rows(Pid, TableName) ->
    gen_server:call(Pid, {all_rows, TableName}, infinity).

%%----------------------------------------------------------------------
%% @doc 順次走査を始める。
%%
%% all_rows/2 との違い: all_rows/2 はOid順に1行ずつ読むため
%% ページアクセスがランダムになり、しかも全件をメモリに載せる。
%% こちらはページを先頭から順に読み、1ページ分ずつ返すので、
%% メモリは1ページ分で一定に保たれ、LIMITでの早期終了も効く。
%%----------------------------------------------------------------------
-spec scan_open(pid(), atom()) -> {ok, scan_cursor()}.
scan_open(Pid, TableName) ->
    %% ファイルが開いていれば呼び出し元で数える。data_buffer は
    %% 全テーブル共通の1プロセスなので、そこを通さないほうがよい。
    %% file_mng はテーブルごとに1プロセスなので、通っても他のテーブルの
    %% 走査は止めない。
    case ets:lookup(fd_tables, TableName) of
        [{TableName, Fd}] ->
            {ok, #scan_cursor{table_name = TableName, page_id = 0,
                              page_count = file_mng:page_count(Fd)}};
        [] ->
            %% まだ開いていない。開くのは所有者(data_buffer)の仕事
            gen_server:call(Pid, {scan_open, TableName}, infinity)
    end.

%%----------------------------------------------------------------------
%% @doc 次の1ページ分の行を返す。
%% Returns: {rows, [{Oid, Val}], scan_cursor()} | eof
%%----------------------------------------------------------------------
-spec scan_next(pid(), scan_cursor()) ->
          {rows, [{term(), term()}], scan_cursor()} | eof.
scan_next(_Pid, #scan_cursor{page_id = P, page_count = N}) when P >= N ->
    eof;
scan_next(Pid, #scan_cursor{table_name = T, page_id = P} = C) ->
    %% バッファに載っていれば呼び出し元で読む。載っていなければ
    %% ディスクから読む必要があるので gen_server に頼む。
    case read_page_rows(T, P) of
        miss ->
            gen_server:call(Pid, {scan_next, C}, infinity);
        [] ->
            %% 全スロットが削除済みのページは飛ばす。呼び出し側に空の
            %% バッチを見せないほうが、上位の演算子の実装が単純になる。
            scan_next(Pid, C#scan_cursor{page_id = P + 1});
        Rows ->
            {rows, Rows, C#scan_cursor{page_id = P + 1}}
    end.

%%----------------------------------------------------------------------
%% バッファに載っているページの行を、呼び出し元のプロセスで読む。
%% 読んでいる最中に追い出されていたら miss を返す。
%%----------------------------------------------------------------------
read_page_rows(TableName, PageId) ->
    case ets:lookup(buf_dir, {TableName, PageId}) of
        [] ->
            miss;
        [{_, Stamp}] ->
            Rows = collect_rows(buf_name_of(Stamp), PageId),
            %% 読み終わってから、まだ同じフレーム・同じ世代かを確かめる
            case ets:lookup(buf_dir, {TableName, PageId}) of
                [{_, Stamp}] -> Rows;
                _            -> miss
            end
    end.

buf_name_of({BufName, _Gen}) -> BufName.

%% スロット番号順に整える。ets:tab2list/1 はsetの内部順で返るので、
%% これが無いと同じテーブルを2回走査したときに行順が変わる。
collect_rows(Buf, PageId) ->
    Sorted = lists:sort([{Slot, Stored}
                         || {#phys_loc{page_id = PId, slot = Slot}, Stored}
                                <- ets:tab2list(Buf),
                            PId =:= PageId]),
    [{Oid, Val} || {_Slot, {Oid, Val}} <- Sorted].

%%----------------------------------------------------------------------
%% @doc 行が1ページに収まるかどうか。
%% 純粋な計算なのでプロセスを経由しない。コミット前の検証で使う。
%%----------------------------------------------------------------------
-spec row_fits(term(), term()) -> boolean().
row_fits(Oid, Val) ->
    file_mng:payload_size(wrap(Oid, Val)) =< file_mng:max_payload_size().

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    DataDir = data_dir(),
    ok = filelib:ensure_dir(filename:join(DataDir, "x")),
    ets:new(buf_info_list, [set, named_table, public]),
    ets:new(fd_tables, [set, named_table, public]),
    %% 読み手が直接引く索引。書くのはこのプロセスだけ
    ets:new(buf_dir, [set, named_table, public, {read_concurrency, true}]),
    {ok, oid_phys_loc} =
        dets:open_file(oid_phys_loc, [{file, filename:join(DataDir, "oid_phys_loc.sys")}]),
    {ok, vacuum_oid} =
        dets:open_file(vacuum_oid, [{file, filename:join(DataDir, "vacuum_oid.sys")}, {type, bag}]),
    ok = init_buf_frames(application:get_env(transaction_db, buffer_frames,
                                             ?DEFAULT_BUF_N)),
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

handle_call({scan_open, TableName}, _From, State) ->
    %% トランザクションは直列に実行されるので、走査中に他のトランザクションが
    %% ページを増やすことはない。自分のINSERTはローカル領域に入るため
    %% ファイルには現れない。よってここでページ数を固定してよい。
    PageCount = file_mng:page_count(get_fd(TableName)),
    {reply, {ok, #scan_cursor{table_name = TableName, page_id = 0,
                              page_count = PageCount}}, State};

handle_call({scan_next, Cursor}, _From, State) ->
    {reply, do_scan_next(Cursor), State};

handle_call({all_rows, TableName}, _From, State) ->
    Rows = [{Oid, read_row(TableName, Oid)} || Oid <- lists:sort(table_oids(TableName))],
    {reply, [{Oid, V} || {Oid, V} <- Rows, V =/= {error, oid_not_found}], State};

handle_call(flush, _From, State) ->
    _ = dets:sync(oid_phys_loc),
    _ = dets:sync(vacuum_oid),
    {reply, ok, State};

handle_call(sync, _From, State) ->
    lists:foreach(fun({_T, Fd}) -> ok = file_mng:sync(Fd) end, ets:tab2list(fd_tables)),
    ok = dets:sync(oid_phys_loc),
    ok = dets:sync(vacuum_oid),
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
%%% 順次走査
%%%===================================================================

do_scan_next(#scan_cursor{page_id = P, page_count = N}) when P >= N ->
    eof;
do_scan_next(#scan_cursor{table_name = T, page_id = P} = C) ->
    Buf = load_page(T, P),
    case collect_rows(Buf, P) of
        %% 全スロットが削除済みのページは飛ばす
        [] -> do_scan_next(C#scan_cursor{page_id = P + 1});
        Rows -> {rows, Rows, C#scan_cursor{page_id = P + 1}}
    end.

%%%===================================================================
%%% 読み書きの実体
%%%===================================================================

%% 新規挿入と上書きの両方を扱う。
%% 既存Oidの場合は同じPhysLocに書き戻し、収まらなければ別ページへ移す。
do_write(TableName, Oid, Data) ->
    case row_fits(Oid, Data) of
        false ->
            {error, row_too_large};
        true ->
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
    Need = file_mng:required_size(wrap(Oid, Data)),
    Buf = acquire_page_with_space(TableName, Need),
    [{Buf, #buf_info{page_id = PageId, slot_count = SlotCount}}] =
        ets:lookup(buf_info_list, Buf),
    Slot = next_free_slot(Buf, TableName, PageId, SlotCount),
    PhysLoc = #phys_loc{table_name = TableName, page_id = PageId, slot = Slot},
    case store(TableName, Buf, PageId, PhysLoc, Oid, Data) of
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
    Need = file_mng:required_size(wrap(Oid, Data)),
    Fd = get_fd(TableName),
    PageId = find_page_with_space(Fd, Need, FromPageId),
    Buf = load_page(TableName, PageId),
    [{Buf, #buf_info{slot_count = SlotCount}}] = ets:lookup(buf_info_list, Buf),
    Slot = next_free_slot(Buf, TableName, PageId, SlotCount),
    PhysLoc = #phys_loc{table_name = TableName, page_id = PageId, slot = Slot},
    case store(TableName, Buf, PageId, PhysLoc, Oid, Data) of
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
    case store(TableName, Buf, PageId, PhysLoc, Oid, Data) of
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
store(TableName, Buf, PageId, PhysLoc, Oid, Data) ->
    ets:insert(Buf, {PhysLoc, wrap(Oid, Data)}),
    flush_page(TableName, Buf, PageId).

%% スロットに書くペイロードは {Oid, Val}。
%%
%% Oidを一緒に格納するのが要点。これが無いとページを順に読んでも
%% その行のOidが分からず、順次走査の結果を上位で使えない
%% (可視性の重ね合わせ・ロック・UPDATE/DELETEの対象特定はすべてOidが要る)。
%% oid_phys_loc のDETSは Oid -> PhysLoc の片方向なので逆は引けない。
%%
%% file_mngはterm_to_binary/1で任意の項を格納するため、
%% ページ形式の定義自体は変わらない。
wrap(Oid, Val) -> {Oid, Val}.

unwrap({_Oid, Val}) -> Val.

restore(Buf, PhysLoc, []) ->
    ets:delete(Buf, PhysLoc);
restore(Buf, _PhysLoc, [Entry]) ->
    ets:insert(Buf, Entry).

%% バッファの内容をページとしてディスクに書き出し、buf_infoを更新する。
flush_page(TableName, Buf, PageId) ->
    %% バッファ上の値はすでに {Oid, Val} なのでそのまま書く
    SlotDataList = [#slot{slot_n = SlotN, data = Stored}
                    || {#phys_loc{slot = SlotN}, Stored} <- ets:tab2list(Buf)],
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
            %% 中身が揃ってから索引に載せる。逆にすると、
            %% まだ読み込んでいないフレームを読み手が引く
            bind_buf(TableName, PageId, FreeBuf),
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
        [{_PhysLoc, Stored}] -> unwrap(Stored)
    end.

%% ディスクのページをフレームに読み込む。
%% ライトスルーのためフレームの旧内容は破棄してよい。
read_page_into(TableName, PageId, FreeBuf) ->
    %% このフレームが載せていたページを索引から外す。
    %% **空にする前に外す。** 逆にすると、空のフレームを読み手が
    %% 有効なページとして読む。
    ok = release_buf(FreeBuf),
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

%%----------------------------------------------------------------------
%% フレームを索引に載せる / 外す。
%%
%% Gen は世代。フレームが A→B→A と入れ替わったとき、名前だけでは
%% 同じに見えてしまうので、読み手が「読んでいる間に入れ替わっていない」
%% ことを確かめられるようにする。
%%----------------------------------------------------------------------
bind_buf(TableName, PageId, BufName) ->
    Gen = erlang:unique_integer([monotonic, positive]),
    true = ets:insert(buf_dir, {{TableName, PageId}, {BufName, Gen}}),
    ok.

release_buf(BufName) ->
    case ets:lookup(buf_info_list, BufName) of
        [{BufName, #buf_info{table_name = T, page_id = P}}] when T =/= nil, P =/= nil ->
            true = ets:delete(buf_dir, {T, P}),
            ok;
        _ ->
            ok
    end.

%% フレームを空に戻す。索引から外してから中身を捨てる。
free_buf(BufName) ->
    ok = release_buf(BufName),
    reset_buf(BufName),
    ets:insert(buf_info_list, {BufName, #buf_info{buf_name = BufName}}),
    ok.

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
              ok = free_buf(BufName);
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
                          ok = free_buf(BufName);
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
