%%%-------------------------------------------------------------------
%%% @doc
%%% クエリ実行プロセス。クライアント1接続につき1プロセスが起動し、
%%% そのコネクションのトランザクションを実行する。
%%%
%%% トランザクション中の更新は共有データには書かず、このプロセスが
%%% 持つローカル領域(lKvstore / lColumnIndex)にクエリID単位で溜める。
%%% SELECTは共有データを読んだ上に、自分のローカル領域の変更を
%%% 重ねて返す。これにより、コミット前の自分の変更は自分からは見え、
%%% 他のトランザクションからは見えない。
%%%
%%%   lKvstore     : {QueryId, ins|del, TableName, Oid, Val}
%%%   lColumnIndex : {QueryId, ins|del, TableName, ColName, Val, Oid}
%%%
%%% コミット時はまずREDOログを書いて同期し、その後で共有データへ
%%% 反映してcheckpointを書く。ロールバック時はローカル領域を捨てるだけ。
%%% @end
%%%-------------------------------------------------------------------
-module(query_exec).
-behaviour(gen_server).

%% Public API
-export([start_link/0, stop/1, exec_query/2, status/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {sdsPid, txMngPid, lockMngPid, txid, lKvstore, lColumnIndex,
                queryId = [],
                %% 読み取り専用トランザクションかどうか。
                %% true のとき tx_mng の直列化の列に並ばない。
                readonly = false,
                %% 読み取り専用トランザクションが見る版。
                %% これがあると、コミットの適用を待たずに
                %% 開始時点の状態を読める。
                snapshot = undefined}).

%% トランザクションから見えるテーブル走査の状態。
%% base   : 共有ページの走査カーソル
%% delta  : このトランザクションの最終的な差分 #{Oid => deleted | {row, Row}}
%% pending: 共有側にまだ存在しない、このトランザクションの挿入行
%% stage  : base を出し切ってから pending を出す
-record(tx_scan, {base, delta = #{}, pending = [], stage = base}).

-include("../include/simple_db_server.hrl").
-include("../include/catalog.hrl").

%%%===================================================================
%%% Public APIs. Client program call these APIs.
%%%===================================================================

start_link() ->
    gen_server:start_link(?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%----------------------------------------------------------------------
%% @doc クエリを実行する。
%%
%% サポートするクエリ:
%%   {begin_tx}                                 -> Txid
%%   {commit_tx}                                -> ok | transaction_not_found
%%   {rollback_tx}                              -> ok | transaction_not_found
%%   {create_table, TableName, ColumnList}      -> ok | {error, Reason}
%%   {drop_table, TableName}                    -> ok | {error, Reason}
%%   {insert, TableName, Val}                   -> {ok, Oid} | {error, Reason}
%%   {select, TableName, ColName, Val}          -> [Val] | {error, Reason}
%%   {update, TableName, SetQuery, ColName, Val}-> {ok, Count} | {error, Reason}
%%   {delete, TableName, ColName, Val}          -> {ok, Count} | {error, Reason}
%%----------------------------------------------------------------------
exec_query(Pid, Query) ->
    gen_server:call(Pid, {exec_query, Query}, infinity).

%%----------------------------------------------------------------------
%% @doc この接続の状態。
%% グローバルなトランザクションの有無ではなく、**この接続が**
%% トランザクションを開いているかを返す。
%% Returns: #{in_transaction => boolean(), txid => term()}
%%----------------------------------------------------------------------
status(Pid) ->
    gen_server:call(Pid, status).

%%%===================================================================
%%% Callback functions of gen_server
%%%===================================================================

init([]) ->
    {LKvstore, LColumnIndex} = create_local_tables(),
    {ok, #state{sdsPid = whereis(simple_db_server),
                txMngPid = whereis(tx_mng),
                lockMngPid = whereis(lock_mng),
                lKvstore = LKvstore,
                lColumnIndex = LColumnIndex,
                queryId = []}}.

%%----------------------------------------------------------------------
%% 読み書きトランザクション。
%%
%% 読み手と同じくスナップショットを取る。読むときは開始時点へ巻き戻す
%% ので、他の書き手が途中でコミットしても見え方が変わらない。
%% これで**走査に読みロックを取らずに済む**。走査は表の全行に触るので、
%% 読みロックを取ると事実上の表ロックになり、並行に走れなくなる。
%%
%% 書くときは行の書き込みロックを取る。同じ行を2本が同時に書けない。
%% ただしロックは取った後の変更しか防げないので、コミットの直前に
%% 「自分のスナップショット以降に、自分が書く行が変わっていないか」を
%% 確かめる(first-updater-wins)。
%%
%% 得られる分離水準は**スナップショット分離**で、直列化可能ではない。
%% ライトスキュー(互いに相手が読んだ行を書く)は防げない。
%%----------------------------------------------------------------------
handle_call({exec_query, {begin_tx}}, _From, #state{txid = undefined} = State) ->
    Txid = tx_mng:begin_tx(get_tx_mng_pid(State)),
    Snap = snapshot_mng:acquire(),
    {reply, Txid, State#state{txid = Txid, snapshot = Snap, queryId = []}};
handle_call({exec_query, {begin_tx}}, _From, State) ->
    %% ネストしたトランザクションは扱わない
    {reply, {error, transaction_already_started}, State};

%%----------------------------------------------------------------------
%% 読み取り専用トランザクション。
%%
%% tx_mng の直列化の列に**並ばない**。代わりに開始時点の版
%% (スナップショット)を取る。読むときは、その後のコミットの
%% 変更前の値(undo)を重ねて巻き戻す。
%%
%%   * 読み手同士は並行に走る
%%   * **読み手は書き手を待たせない**(ここが段階1からの前進)
%%   * 読み手は開始時点の状態を一貫して見る
%%
%% 読み取り専用のスナップショット分離は**直列化可能**である。
%% スナップショット分離が許す異常(ライトスキューなど)は書き込みが
%% 絡んで初めて起きるので、書かないトランザクションには現れない。
%%----------------------------------------------------------------------
handle_call({exec_query, {begin_read_only}}, _From, #state{txid = undefined} = State) ->
    Snap = snapshot_mng:acquire(),
    {reply, ok, State#state{txid = readonly, readonly = true,
                            snapshot = Snap, queryId = []}};
handle_call({exec_query, {begin_read_only}}, _From, State) ->
    {reply, {error, transaction_already_started}, State};

handle_call({exec_query, {commit_tx}}, _From, State) ->
    with_transaction(State, fun() -> do_commit(State) end);

handle_call({exec_query, {rollback_tx}}, _From, State) ->
    with_transaction(State, fun() -> do_rollback(State) end);

%% DDLは暗黙のトランザクションとして実行する(下の with_ddl/3 を参照)。
handle_call({exec_query, {create_table, TableName, ColumnList}}, _From, State) ->
    with_ddl(State, TableName,
             fun() -> simple_db_server:create_table(get_db_pid(State), TableName, ColumnList) end);

handle_call({exec_query, {drop_table, TableName}}, _From, State) ->
    with_ddl(State, TableName,
             fun() -> simple_db_server:drop_table(get_db_pid(State), TableName) end);

handle_call({exec_query, {insert, TableName, Val}}, _From, State) ->
    with_write_transaction(State, fun() -> do_insert(State, TableName, Val) end);

handle_call({exec_query, {select, TableName, ColName, Val}}, _From, State) ->
    with_transaction(State, fun() -> do_select(State, TableName, ColName, Val) end);

handle_call({exec_query, {scan, TableName}}, _From, State) ->
    with_transaction(State, fun() -> do_scan(State, TableName) end);

%% SQL文はここでトランザクションを要求しない。
%% do_sql/2 が文の種類ごとに、DDLなら with_ddl、DMLなら with_transaction、
%% BEGIN/COMMIT/ROLLBACK ならそのまま、と振り分ける。
handle_call({exec_query, {sql, Sql}}, _From, State) ->
    do_sql(State, Sql);

handle_call({exec_query, {update, TableName, SetQuery, ColName, Val}}, _From, State) ->
    with_write_transaction(State, fun() -> do_update(State, TableName, SetQuery, ColName, Val) end);

handle_call({exec_query, {delete, TableName, ColName, Val}}, _From, State) ->
    with_write_transaction(State, fun() -> do_delete(State, TableName, ColName, Val) end);

handle_call({exec_query, Query}, _From, State) ->
    {reply, {error, {unsupported_query, Query}}, State};

handle_call(status, _From, #state{txid = Txid} = State) ->
    {reply, #{in_transaction => Txid =/= undefined, txid => Txid}, State};

handle_call(terminate, _From, State) ->
    {stop, normal, ok, State};

handle_call(Request, _From, State) ->
    {reply, {error, {unknown_request, Request}}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Msg, State) ->
    {noreply, State}.

%% 未コミットのまま接続が切れた場合はロールバックしてロックを解放する。
%% これを怠るとトランザクションの順番を握ったままDBが止まる。
terminate(_Reason, #state{txid = undefined}) ->
    ok;
terminate(_Reason, State) ->
    _ = do_rollback(State),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% クエリ本体
%%%===================================================================

%% DDLを暗黙のトランザクションとして実行する。
%%
%% 以前はトランザクションを介さず即座に反映していたため、2つ問題があった。
%%   - 明示的なトランザクションの中で CREATE TABLE してロールバックしても
%%     テーブルが残る
%%   - 他の接続のトランザクションが実行中でも割り込めるので、
%%     直列化可能性が成立しない
%%
%% ここで暗黙のトランザクションに包むことで、DDLも他のトランザクションと
%% 同じ順番待ちの列に並ぶ。
%%
%% 明示的なトランザクションの中でのDDLは拒否する。カタログの変更を
%% 元に戻す仕組み(UNDOログ)が無いため、ロールバックできないものを
%% 黙って通すより、実行できないと言うほうが正直である。
with_ddl(#state{txid = undefined} = State, Table, Fun) ->
    TPid = get_tx_mng_pid(State),
    Txid = tx_mng:begin_tx(TPid),
    case tx_mng:allow_tx(TPid, Txid) of
        ok ->
            %% その表を書きかけのトランザクションが終わるまで待つ
            ok = ddl_table_lock(State#state{txid = Txid}, Table),
            %% DDLはカタログと索引を書き換える。走査中に表が落ちると
            %% 読み手が壊れた状態を見るので、適用と同じく排他で囲う。
            Reply = commit_latch:with_write(Fun),
            _ = tx_mng:commit_tx(TPid, Txid),
            {reply, Reply, State};
        transaction_not_found ->
            {reply, {error, transaction_not_found}, State}
    end;
with_ddl(State, _Table, _Fun) ->
    {reply, {error, ddl_in_transaction}, State}.

ddl_table_lock(_State, undefined) -> ok;
ddl_table_lock(State, Table)      -> acquire_table_lock(State, Table, write).

%%----------------------------------------------------------------------
%% 書き込みを含む操作。読み取り専用トランザクションでは断る。
%%
%% 黙って通すと、共有ラッチを持ったまま共有データを書くことになり、
%% 「読み手が動いている間はコミットが適用されない」が崩れる。
%%----------------------------------------------------------------------
with_write_transaction(#state{readonly = true} = State, _Fun) ->
    {reply, {error, read_only_transaction}, State};
with_write_transaction(State, Fun) ->
    with_transaction(State, Fun).

%% トランザクションが開始済みであることを確かめてから Fun を実行する。
%%
%% 文の途中で捨てるしかなくなったとき(デッドロック)は throw で
%% ここまで戻る。文の途中で止めると、ローカル領域に半端な変更が
%% 残ったままトランザクションが続いてしまうので、その場で捨てる。
with_transaction(State, Fun) ->
    case ask_transaction(State) of
        transaction_not_found ->
            {reply, transaction_not_found, State};
        ok ->
            try Fun()
            catch
                throw:{abort, Reason} ->
                    {reply, _, State2} = do_rollback(State),
                    {reply, {error, Reason}, State2}
            end
    end.

do_insert(State, TableName, Val) ->
    case check_table(State, TableName, Val) of
        ok ->
            QueryId = db_id:new(),
            Oid = db_id:new(),
            ok = local_insert_data(State, QueryId, TableName, Oid, Val),
            {reply, {ok, Oid}, add_query_id(State, QueryId)};
        {error, Reason} ->
            {reply, {error, Reason}, State}
    end.

%% タプルAPIの等値検索。SQLの索引スキャンと同じ経路を使う。
%% 別々に書くと、片方だけスナップショットを見ないといった食い違いが出る。
do_select(State, TableName, ColName, Val) ->
    case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
        false ->
            {reply, {error, table_not_found}, State};
        true ->
            case tx_index_lookup(State, TableName, ColName, Val) of
                {ok, Rows}      -> {reply, Rows, State};
                {error, Reason} -> {reply, {error, Reason}, State}
            end
    end.

%% SQL文を実行する。
%% 字句解析 -> 構文解析 -> 意味解析(カラムを位置に解決) -> 実行。
do_sql(State, Sql) ->
    case sql:parse(Sql) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {ok, Ast} ->
            case sql_analyzer:analyze(Ast) of
                {error, Reason} -> {reply, {error, Reason}, State};
                {ok, Op} -> run_sql(State, Op)
            end
    end.

%% トランザクション制御。
%% SQLの BEGIN は ok を返す。内部のトランザクションIDを外に出さない
%% (タプルAPIの {begin_tx} はIDを返すが、それは内部向けの口)。
run_sql(State, {tx, 'begin'}) ->
    case handle_call({exec_query, {begin_tx}}, undefined, State) of
        {reply, {error, Reason}, S} -> {reply, {error, Reason}, S};
        {reply, _Txid, S} -> {reply, ok, S}
    end;
run_sql(State, {tx, begin_read_only}) ->
    handle_call({exec_query, {begin_read_only}}, undefined, State);
run_sql(State, {tx, commit}) ->
    handle_call({exec_query, {commit_tx}}, undefined, State);
run_sql(State, {tx, rollback}) ->
    handle_call({exec_query, {rollback_tx}}, undefined, State);

%% DDL。既存のDDL経路(暗黙のトランザクション)に載せる
run_sql(State, {create_table, Table, Columns}) ->
    with_ddl(State, Table,
             fun() -> simple_db_server:create_table(get_db_pid(State), Table, Columns) end);
run_sql(State, {drop_table, Table}) ->
    with_ddl(State, Table, fun() -> simple_db_server:drop_table(get_db_pid(State), Table) end);
run_sql(State, {create_index, Name, Table, Column}) ->
    with_ddl(State, Table, fun() ->
                            simple_db_server:create_index(get_db_pid(State), Name, Table, Column)
                    end);
%% ANALYZE はカタログを書き換えるのでDDLと同じ扱い。
run_sql(State, {analyze, Table}) ->
    with_ddl(State, Table, fun() -> simple_db_server:analyze(get_db_pid(State), Table) end);
run_sql(State, {drop_index, Name}) ->
    with_ddl(State, undefined, fun() -> simple_db_server:drop_index(get_db_pid(State), Name) end);

%% DML。トランザクションが要る
run_sql(State, {select, Plan}) ->
    with_transaction(State, fun() -> do_sql_select(State, Plan) end);

%% EXPLAIN はデータに触らないのでトランザクションを要らない。
%% 必要なカタログの照合は解析の時点で済んでいる。
run_sql(State, {explain, Logical}) ->
    %% 実行するときと同じカタログを渡す。違うものを渡すと、
    %% 表示された計画と実際に走る計画がずれる。
    Lines = sql_explain:explain(sql_planner:plan(Logical, catalog_fun())),
    {reply, {ok, ['QUERY PLAN'], [[L] || L <- Lines]}, State};
run_sql(State, {insert, Table, Row}) ->
    with_write_transaction(State, fun() -> do_insert(State, Table, Row) end);
run_sql(State, {update, Table, Assigns, Pred}) ->
    with_write_transaction(State, fun() -> do_sql_update(State, Table, Assigns, Pred) end);
run_sql(State, {delete, Table, Pred}) ->
    with_write_transaction(State, fun() -> do_sql_delete(State, Table, Pred) end).

do_sql_select(State, Logical) ->
    %% 論理プランから物理プランを作る。実行方法(全表走査か索引か、
    %% どの結合アルゴリズムか)がここで決まる。
    Plan = sql_planner:plan(Logical, catalog_fun()),
    %% 実行器にはストレージへの入口を関数で渡す。
    %% こうしておくと実行器がquery_execの内部状態に触らずに済み、
    %% かつ走査がこのトランザクションの未コミット変更を見られる。
    {reply, run_plan(State, Plan), State}.

%% 読み取り専用トランザクションは tx_mng の順番待ちに並ばないので、
%% DDL と鉢合わせしうる。走査の間だけ共有ラッチを持って、
%% 表が消えたり索引が作り直されたりするのを待たせる。
%%
%% 共有ラッチどうしは互いを待たないので、読み手が読み手を止めることはない。
%% コミットの適用はスナップショットが吸収するので、もうラッチは要らない。
run_plan(#state{readonly = true} = State, Plan) ->
    commit_latch:with_read(fun() -> sql_exec:run(Plan, exec_ctx(State)) end);
run_plan(State, Plan) ->
    sql_exec:run(Plan, exec_ctx(State)).


exec_ctx(State) ->
    #{scan_open    => fun(T) -> tx_scan_open(State, T) end,
      scan_next    => fun(C) -> tx_scan_next(C) end,
      index_lookup => fun(T, Col, Val) -> tx_index_lookup(State, T, Col, Val) end}.

%%----------------------------------------------------------------------
%% プランナに渡すカタログ。索引の有無と統計だけを見せる。
%%
%% プランナがカタログを直接引かないのは、同じ入力から同じプランが
%% 出ることを保てるようにするためと、試験で偽のカタログを渡して
%% 索引がある場合と無い場合を書き分けられるようにするため。
%%----------------------------------------------------------------------
catalog_fun() ->
    Sys = whereis(sys_tbl_mng),
    fun(Table) ->
            Indexed = case sys_tbl_mng:get_index_column_list(Sys, Table) of
                          {ok, Cols} -> Cols;
                          {error, _} -> []
                      end,
            Stats = case sys_tbl_mng:get_stats(Sys, Table) of
                        {ok, S} -> S;
                        none    -> none
                    end,
            #{indexed => Indexed, stats => Stats}
    end.

%%----------------------------------------------------------------------
%% 索引で引いた結果に、このトランザクションの未コミット変更を重ねる。
%%
%% 実行器から索引を直接引かせると、自分がさっき入れた行が見えない。
%% 重ね合わせはタプルAPIのSELECTと同じ経路を使う。
%%----------------------------------------------------------------------
tx_index_lookup(State, Table, ColName, Val) ->
    case snapshot_overlay(State, Table) of
        {error, Reason} ->
            {error, Reason};
        {ok, Snap} when map_size(Snap) =:= 0 ->
            %% 巻き戻す必要が無い。索引の結果をそのまま使う
            QueryIdList = get_query_id_list(State),
            OidList = select_object_id_list(State, Table, ColName, Val, QueryIdList),
            ok = acquire_lock(State, OidList, read),
            {ok, [R || R <- select_data(State, Table, OidList, QueryIdList),
                       R =/= not_found]};
        {ok, Snap} ->
            snapshot_index_lookup(State, Table, ColName, Val, Snap)
    end.

%%----------------------------------------------------------------------
%% スナップショットの時点で ColName = Val だった行を返す。
%%
%% 索引は**いまの値**で引かれるので、そのままでは足りない。
%%   - 昔その値だった行は、いま別の値なら索引に出てこない
%%   - いまその値の行は、昔は別の値だったかもしれない
%%
%% 索引そのものを版管理していないので、候補を広げて絞り直す。
%% 候補は「いま索引に出てくる行」と「スナップショット以降に変わった行」の
%% 和。後者は undo の重ね合わせの鍵そのもので、スナップショットを取って
%% からの変更数しかないため、表の大きさには比例しない。
%%
%% 候補を巻き戻してから ColName = Val で絞るので、結果はその時点の
%% 全表走査と一致する。
%%----------------------------------------------------------------------
snapshot_index_lookup(State, Table, ColName, Val, Snap) ->
    case column_position(Table, ColName) of
        not_found ->
            {error, {column_not_found, ColName}};
        Pos ->
            QueryIdList = get_query_id_list(State),
            Indexed = select_object_id_list(State, Table, ColName, Val, QueryIdList),
            Candidates = lists:usort(Indexed ++ maps:keys(Snap)),
            ok = acquire_lock(State, Candidates, read),
            Local = build_delta(State, Table),
            {ok, [Row || Oid <- Candidates,
                         (Row = row_at(State, Table, Oid, Local, Snap)) =/= not_found,
                         element(Pos, list_to_tuple(Row)) =:= Val]}
    end.

%% Oid の、このトランザクションから見える値。
%% 自分の未コミット変更 > スナップショットの巻き戻し > 共有データ。
row_at(State, Table, Oid, Local, Snap) ->
    case maps:get(Oid, Local, none) of
        deleted    -> not_found;
        {row, V}   -> V;
        none       ->
            case maps:get(Oid, Snap, none) of
                deleted  -> not_found;
                {row, V} -> V;
                none     -> read_shared(State, Table, Oid)
            end
    end.

read_shared(_State, Table, Oid) ->
    simple_db_server:read_data_oid(Table, Oid).

column_position(Table, ColName) ->
    case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), Table) of
        {ok, ColList} -> position_in(ColName, ColList, 1);
        _             -> not_found
    end.

position_in(_Name, [], _N)       -> not_found;
position_in(Name, [Name | _], N) -> N;
position_in(Name, [_ | T], N)    -> position_in(Name, T, N + 1).

%% SQLのUPDATE / DELETE は、対象のOidを**先に確定させてから**適用する。
%%
%% これがHalloween problemへの対処になっている。走査と更新をパイプラインで
%% つなぐと、更新した行を走査が拾い直し、条件を外れるまで同じ行を何度も
%% 更新してしまう。対象を先に materialize すればその輪が切れる。
%% 実行器に更新演算子を載せるときは、文レベルスナップショットが要る。
do_sql_update(State, Table, Assigns, Pred) ->
    case matching_rows(State, Table, Pred) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {ok, Rows} ->
            {ok, Columns} = sys_tbl_mng:get_columns(whereis(sys_tbl_mng), Table),
            %% 新しい値を先に全部作って型を確かめる。1行でも通らなければ
            %% 何も書かない(コミット時の検証と同じ考え方)。
            Updated = [{Oid, OldVal, apply_assigns(OldVal, Assigns)} || {Oid, OldVal} <- Rows],
            case check_update_types(Columns, Updated) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};
                ok ->
                    {reply, {ok, length(Updated)},
                     record_update(State, Table, Columns, Updated)}
            end
    end.

%% ローカル領域へ直接書くので、local_data/6 と同じく表の共有ロックが要る。
record_update(State, Table, Columns, Updated) ->
    ok = acquire_table_lock(State, Table, read),
    QueryId = db_id:new(),
    LKvstore = get_local_kvstore(State),
    LColumnIndex = get_local_column_index(State),
    ColumnList = [C#column.name || C <- Columns],
    ok = lock_for_write(State, Table, [Oid || {Oid, _, _} <- Updated]),
    lists:foreach(
      fun({Oid, OldVal, NewVal}) ->
              ets:insert(LKvstore, {QueryId, del, Table, Oid, OldVal}),
              ets:insert(LKvstore, {QueryId, ins, Table, Oid, NewVal}),
              lists:foreach(
                fun({_Col, Same, Same}) -> ok;
                   ({Col, Old, New}) ->
                        ets:insert(LColumnIndex, {QueryId, del, Table, Col, Old, Oid}),
                        ets:insert(LColumnIndex, {QueryId, ins, Table, Col, New, Oid})
                end, lists:zip3(ColumnList, OldVal, NewVal))
      end, Updated),
    add_query_id(State, QueryId).

%% 代入が式の場合、値は行ごとに決まるので型検査は実行時になる。
check_update_types(_Columns, []) ->
    ok;
check_update_types(Columns, [{_Oid, _Old, NewVal} | T]) ->
    case check_row_types(Columns, NewVal) of
        ok -> check_update_types(Columns, T);
        {error, Reason} -> {error, Reason}
    end.

check_row_types([], []) ->
    ok;
check_row_types([#column{name = Name, type = Type} | CT], [V | VT]) ->
    case sql_type:check(Type, V) of
        ok -> check_row_types(CT, VT);
        {error, Reason} -> {error, {Reason, Name, Type, V}}
    end.

do_sql_delete(State, Table, Pred) ->
    case matching_rows(State, Table, Pred) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {ok, Rows} ->
            QueryId = db_id:new(),
            ok = lock_for_write(State, Table, [Oid || {Oid, _} <- Rows]),
            lists:foreach(fun({Oid, Val}) ->
                                  ok = local_delete_data(State, QueryId, Table, Oid, Val)
                          end, Rows),
            {reply, {ok, length(Rows)}, add_query_id(State, QueryId)}
    end.

%% 述語に一致する行を {Oid, Val} で集める。走査なので任意のカラムを見られる。
matching_rows(State, Table, Pred) ->
    case tx_scan_open(State, Table) of
        {error, Reason} ->
            {error, Reason};
        {ok, Scan} ->
            All = tx_scan_all(Scan, []),
            {ok, [{Oid, Val} || {Oid, Val} <- All,
                                sql_expr:eval_pred(Pred, list_to_tuple(Val))]}
    end.

%% 代入の右辺は**更新前の行**に対して評価する。
%% 途中の結果を使うと SET a = b, b = a が入れ替えにならない。
apply_assigns(Val, Assigns) ->
    Old = list_to_tuple(Val),
    lists:foldl(fun({Pos, Expr}, Acc) ->
                        setnth(Pos, Acc, sql_expr:eval(Expr, Old))
                end, Val, Assigns).

setnth(1, [_ | T], V) -> [V | T];
setnth(N, [H | T], V) -> [H | setnth(N - 1, T, V)].

%% テーブル全体を走査する。索引を使わないので任意のカラムの条件に使える。
%% 将来の実行器のSeqScan演算子はこの上に載る。
do_scan(State, TableName) ->
    case tx_scan_open(State, TableName) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {ok, Scan} ->
            Rows = tx_scan_all(Scan, []),
            ok = acquire_lock(State, [Oid || {Oid, _} <- Rows], read),
            {reply, [Row || {_Oid, Row} <- Rows], State}
    end.

do_update(State, TableName, SetQuery, ColName, Val) ->
    case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
        {error, table_not_found} ->
            {reply, {error, table_not_found}, State};
        {ok, ColumnList} ->
            case [C || {C, _V} <- SetQuery, not lists:member(C, ColumnList)] of
                [] ->
                    do_update_1(State, TableName, SetQuery, ColName, Val, ColumnList);
                Unknown ->
                    {reply, {error, {unknown_columns, Unknown}}, State}
            end
    end.

do_update_1(State, TableName, SetQuery, ColName, Val, ColumnList) ->
    QueryId = db_id:new(),
    LKvstore = get_local_kvstore(State),
    LColumnIndex = get_local_column_index(State),
    QueryIdList = get_query_id_list(State),
    SetQueryConverted = simple_db_server:convert_set_query(SetQuery, ColumnList),
    OidList = select_object_id_list(State, TableName, ColName, Val, QueryIdList),
    ok = lock_for_write(State, TableName, OidList),
    Updated =
        lists:foldl(
          fun(Oid, Count) ->
                  case select_data(State, TableName, Oid, QueryIdList) of
                      not_found ->
                          Count;
                      OldVal ->
                          NewVal = simple_db_server:build_new_val(OldVal, SetQueryConverted),
                          %% 更新前値を消して更新後値を入れる、をローカル領域に記録する
                          ets:insert(LKvstore, {QueryId, del, TableName, Oid, OldVal}),
                          ets:insert(LKvstore, {QueryId, ins, TableName, Oid, NewVal}),
                          %% 値が変わったカラムだけインデックスを張り替える
                          lists:foreach(
                            fun({_ColumnN, Same, Same}) ->
                                    ok;
                               ({ColumnN, OldColVal, NewColVal}) ->
                                    ets:insert(LColumnIndex,
                                               {QueryId, del, TableName, ColumnN, OldColVal, Oid}),
                                    ets:insert(LColumnIndex,
                                               {QueryId, ins, TableName, ColumnN, NewColVal, Oid})
                            end, lists:zip3(ColumnList, OldVal, NewVal)),
                          Count + 1
                  end
          end, 0, OidList),
    {reply, {ok, Updated}, add_query_id(State, QueryId)}.

do_delete(State, TableName, ColName, Val) ->
    QueryId = db_id:new(),
    QueryIdList = get_query_id_list(State),
    OidList = select_object_id_list(State, TableName, ColName, Val, QueryIdList),
    ok = lock_for_write(State, TableName, OidList),
    Deleted =
        lists:foldl(
          fun(Oid, Count) ->
                  case select_data(State, TableName, Oid, QueryIdList) of
                      not_found ->
                          Count;
                      RowVal ->
                          ok = local_delete_data(State, QueryId, TableName, Oid, RowVal),
                          Count + 1
                  end
          end, 0, OidList),
    {reply, {ok, Deleted}, add_query_id(State, QueryId)}.

%%%===================================================================
%%% トランザクションから見える走査
%%%===================================================================

%% コミット時にディスクへ同期する。
%%
%% 既定は同期する(durable_commit = true)。切ると1コミットあたりの
%% fsyncが無くなって速くなるが、電源断でコミット済みのデータを失う。
%% PostgreSQLの synchronous_commit と同じ性質のつまみ。
sync_for_commit() ->
    case application:get_env(transaction_db, durable_commit, true) of
        true -> data_buffer:sync(whereis(data_buffer));
        false -> ok
    end.

%% 共有データの走査に、このトランザクションのローカル差分を重ねる。
%%
%% 既存の重ね合わせ(merge_local_index/6)は「特定カラムが特定の値」という
%% 述語を前提にしており、述語の無い走査には使えない。merge_local_data/4 は
%% Oid単位なので、共有側にOidが存在しないローカル挿入行を取りこぼす。
%%
%% そこで走査の開始時に、このトランザクションの最終的な差分を1つのマップに
%% 畳んでおき、2相で返す。
tx_scan_open(State, TableName) ->
    case simple_db_server:scan_open(TableName) of
        {error, Reason} ->
            {error, Reason};
        {ok, Base} ->
            case snapshot_overlay(State, TableName) of
                {error, Reason} ->
                    {error, Reason};
                {ok, Snap} ->
                    %% 自分の未コミット変更はスナップショットより優先する。
                    %% maps:merge/2 は後の引数が勝つ
                    Delta = maps:merge(Snap, build_delta(State, TableName)),
                    {ok, #tx_scan{base = Base, delta = Delta,
                                  pending = local_only_rows(TableName, Delta)}}
            end
    end.

%% 読み取り専用トランザクションだけがスナップショットを持つ。
%% 読み書きトランザクションは直列化されているので巻き戻しは要らない。
snapshot_overlay(#state{snapshot = undefined}, _TableName) ->
    {ok, #{}};
snapshot_overlay(#state{snapshot = Snap}, TableName) ->
    snapshot_mng:overlay(Snap, TableName).

tx_scan_next(#tx_scan{stage = base, base = Base, delta = Delta} = S) ->
    case simple_db_server:scan_next(Base) of
        {rows, Rows, Base2} ->
            case visible_rows(Rows, Delta) of
                %% このページの行が全部自分には見えない場合、空のバッチを
                %% 返さずに次のページへ進む
                [] -> tx_scan_next(S#tx_scan{base = Base2});
                Visible -> {rows, Visible, S#tx_scan{base = Base2}}
            end;
        eof ->
            tx_scan_next(S#tx_scan{stage = local})
    end;
tx_scan_next(#tx_scan{stage = local, pending = []}) ->
    eof;
tx_scan_next(#tx_scan{stage = local, pending = Rows} = S) ->
    {rows, Rows, S#tx_scan{pending = []}}.

tx_scan_all(Scan, Acc) ->
    case tx_scan_next(Scan) of
        eof -> lists:append(lists:reverse(Acc));
        {rows, Rows, Scan2} -> tx_scan_all(Scan2, [Rows | Acc])
    end.

%% 共有側から読んだ行に差分を当てる。
visible_rows(Rows, Delta) ->
    lists:filtermap(
      fun({Oid, ShareRow}) ->
              case maps:get(Oid, Delta, none) of
                  none -> {true, {Oid, ShareRow}};
                  deleted -> false;
                  {row, Row} -> {true, {Oid, Row}}
              end
      end, Rows).

%% ローカル領域をクエリの実行順に畳んで、Oidごとの最終状態を求める。
%% 更新は同一Oidに対する del と ins として入るので、順に畳めば最終状態になる。
build_delta(State, TableName) ->
    LKvstore = get_local_kvstore(State),
    lists:foldl(
      fun(QueryId, Acc) ->
              Entries = [E || {_Q, _A, T, _O, _V} = E <- ets:lookup(LKvstore, QueryId),
                              T =:= TableName],
              %% 同一クエリ内では del を先に適用してから ins を適用する。
              %% bagの取り出し順に依存しないよう明示的に並べ替える。
              Dels = [E || {_Q, del, _T, _O, _V} = E <- Entries],
              Ins = [E || {_Q, ins, _T, _O, _V} = E <- Entries],
              lists:foldl(fun({_Q, del, _T, Oid, _V}, A) -> A#{Oid => deleted};
                             ({_Q, ins, _T, Oid, V}, A) -> A#{Oid => {row, V}}
                          end, Acc, Dels ++ Ins)
      end, #{}, get_query_id_list(State)).

%% このトランザクションが挿入し、共有側にまだ存在しない行。
%% 共有側にあるOidはベースの走査から出るのでここでは除く。
%% 走査するのは自分の書き込み集合だけなので、テーブルの大きさには依存しない。
local_only_rows(TableName, Delta) ->
    [{Oid, Row}
     || {Oid, {row, Row}} <- maps:to_list(Delta),
        simple_db_server:read_data_oid(TableName, Oid) =:= not_found].

%%%===================================================================
%%% Commit / Rollback
%%%===================================================================

%% コミットは以下の順に進む。
%%   1. このトランザクションの全更新をREDOログに書いて同期する
%%   2. 共有データへ反映する
%%   3. checkpointを書く(ここまで来ればリカバリ不要)
%%   4. ロックを解放してトランザクションを終了する
%% 2の途中で落ちても、1が済んでいるのでリカバリで再実行できる。
do_commit(#state{readonly = true} = State) ->
    do_rollback(State);
do_commit(State) ->
    TPid = get_tx_mng_pid(State),
    Txid = get_txid(State),
    QueryIdList = get_query_id_list(State),
    Changes = collect_changes(State, QueryIdList),
    %% 適用も、REDOログの書き込みもする前に、全変更が通ることを確かめる。
    %%
    %% これが無いと、apply_changes/1 が途中で失敗したときに、
    %% それまでに適用した分だけが共有データに残る(千切れたコミット)。
    %% REDOのみでUNDOログが無いので、適用済みの分を戻す手段が無い。
    %% 検証で弾けばまだ何も書いていないので、ロールバックが常に安全になる。
    case validate_changes(Changes) of
        {error, Reason} ->
            %% コミットできないトランザクションは破棄する。
            %% ログを書いていないのでリカバリが再実行することもない。
            {reply, _, State2} = do_rollback(State),
            {reply, {error, Reason}, State2};
        ok ->
            Undo = undo_for(Changes),
            case check_conflicts(State, maps:keys(Undo)) of
                {error, Reason} ->
                    {reply, _, State3} = do_rollback(State),
                    {reply, {error, Reason}, State3};
                ok ->
                    do_commit_1(State, Txid, TPid, QueryIdList, Changes, Undo)
            end
    end.

%%----------------------------------------------------------------------
%% 自分が書く行が、自分のスナップショット以降に変わっていないか。
%%
%% 書き込みロックは、そのロックを取った**後**の変更しか防げない。
%% 自分が読んでから書くまでの間に他の書き手がコミットしていると、
%% その更新を黙って踏み潰す(lost update)。
%%
%% 先にコミットした方を勝たせ、後から来た自分を捨てる。
%%----------------------------------------------------------------------
check_conflicts(#state{snapshot = undefined}, _Keys) ->
    ok;
check_conflicts(#state{snapshot = Snap}, Keys) ->
    case snapshot_mng:conflicts(Snap, Keys) of
        {error, Reason} -> {error, Reason};
        {ok, []}        -> ok;
        {ok, _Changed}  -> {error, serialization_failure}
    end.

do_commit_1(#state{snapshot = Snap} = State, Txid, TPid, QueryIdList, Changes, Undo) ->
    ok = write_redo_log(Txid, Changes),
    %% 変更前の値を undo として積む。**適用の前**に積むのが要点。
    %%
    %% 適用は1行ずつ進むので、その途中を読み手が見ることがある。
    %% 先に undo があれば、
    %%   適用済みの行 → undo が古い値へ戻す
    %%   未適用の行   → もともと古い値
    %% となって、どちらも同じ値に見える。逆順にすると、その隙間に
    %% 読んだ行だけが新しい値に見えてしまう。
    Seq = snapshot_mng:commit(Undo),
    %% 適用の間は共有ラッチを持つ。DDL が割り込むと、書いている先の
    %% 表が消えてコミットが千切れる。
    ok = commit_latch:with_read(fun() -> apply_changes(Changes) end),
    %% チェックポイントを書く前にディスクへ落とす。
    %%
    %% リカバリは最後のチェックポイント以降しか再実行しない。
    %% よってチェックポイントは「これより前は永続化済み」という
    %% 宣言になる。ページキャッシュに置いただけの状態でこれを
    %% 書くと、電源断のときにデータは失われるのにリカバリは
    %% 再実行せず、黙って消える。
    ok = sync_for_commit(),
    ok = log_util:redo_log_put_checkpoint(),
    %% 適用が終わったので、この版を見えるようにする。
    %% 並行するコミットがまだ適用中なら、見える版はそこで止まる。
    ok = snapshot_mng:publish(Seq),
    clear_local(State, QueryIdList),
    ok = release_snapshot(Snap),
    Rep = tx_mng:commit_tx(TPid, Txid),
    {reply, Rep, State#state{txid = undefined, snapshot = undefined, queryId = []}}.

release_snapshot(undefined) -> ok;
release_snapshot(Snap)      -> snapshot_mng:release(Snap).

%% 共有データを一切変更せずに、全変更が適用可能かを確かめる。
validate_changes([]) ->
    ok;
validate_changes([{_QId, ins, TableName, Oid, Val} | T]) ->
    case simple_db_server:validate_insert(TableName, Oid, Val) of
        {ok, _ColumnList} -> validate_changes(T);
        {error, Reason} -> {error, Reason}
    end;
validate_changes([{_QId, del, TableName, _Oid, _Val} | T]) ->
    case simple_db_server:validate_delete(TableName) of
        ok -> validate_changes(T);
        {error, Reason} -> {error, Reason}
    end.

%% ロールバックはローカル領域を捨てるだけでよい。
%% 共有データにはまだ何も書いていない。
%% 読み取り専用は共有データに何も書いていないので、
%% スナップショットを手放すだけ。
do_rollback(#state{readonly = true, snapshot = Snap} = State) ->
    ok = snapshot_mng:release(Snap),
    {reply, ok, State#state{txid = undefined, readonly = false,
                            snapshot = undefined, queryId = []}};
do_rollback(#state{snapshot = Snap} = State) ->
    TPid = get_tx_mng_pid(State),
    Txid = get_txid(State),
    clear_local(State, get_query_id_list(State)),
    ok = release_snapshot(Snap),
    Rep = tx_mng:rollback_tx(TPid, Txid),
    {reply, Rep, State#state{txid = undefined, snapshot = undefined,
                             queryId = []}}.

%% ローカル領域の変更をクエリの実行順に並べる。
%% 同じOidに対するdel→insの順序が保たれている必要がある。
collect_changes(State, QueryIdList) ->
    LKvstore = get_local_kvstore(State),
    lists:append(
      [begin
           Entries = ets:lookup(LKvstore, QueryId),
           Dels = [E || {_Q, del, _T, _O, _V} = E <- Entries],
           Ins = [E || {_Q, ins, _T, _O, _V} = E <- Entries],
           Dels ++ Ins
       end || QueryId <- QueryIdList]).

write_redo_log(Txid, Changes) ->
    Now = erlang:system_time(nanosecond),
    Logs = [#redo_log{timestamp = Now, txid = Txid, query_id = QId, action = Action,
                      table_name = TableName, oid = Oid, val = Val}
            || {QId, Action, TableName, Oid, Val} <- Changes],
    ok = log_util:redo_log_write_many(Logs),
    %% 共有データへ反映する前に必ず同期する(WAL)
    ok = log_util:sync(),
    ok.

apply_changes([]) ->
    ok;
apply_changes([{_QId, del, TableName, Oid, _Val} | T]) ->
    ok = ensure_ok(simple_db_server:delete_data(simple_db_server, TableName, Oid)),
    apply_changes(T);
apply_changes([{_QId, ins, TableName, Oid, Val} | T]) ->
    ok = ensure_ok(simple_db_server:insert_data(simple_db_server, TableName, Oid, Val)),
    apply_changes(T).

%%----------------------------------------------------------------------
%% 変更する行の**変更前の値**を集める。
%%
%% 同じ行が同じコミットの中で複数回変わることがある(del→ins)。
%% 欲しいのは「このコミットが始まる前の値」なので、
%% **最初に見たものを残す**。
%%----------------------------------------------------------------------
undo_for(Changes) ->
    lists:foldl(
      fun({_QId, _Act, TableName, Oid, _Val}, Acc) ->
              Key = {TableName, Oid},
              case maps:is_key(Key, Acc) of
                  true  -> Acc;
                  false -> Acc#{Key => before_value(TableName, Oid)}
              end
      end, #{}, Changes).

before_value(TableName, Oid) ->
    case simple_db_server:read_data_oid(TableName, Oid) of
        not_found -> absent;
        Val       -> {row, Val}
    end.

%% ここに来る変更は validate_changes/1 を通っている。
%% それでも失敗するなら想定外なので、握り潰さずに落とす
%% (REDOログは書いてありチェックポイントはまだなので、リカバリで再実行される)。
ensure_ok(ok) -> ok;
ensure_ok({error, Reason}) -> error({commit_failed, Reason}).

clear_local(State, QueryIdList) ->
    LKvstore = get_local_kvstore(State),
    LColumnIndex = get_local_column_index(State),
    lists:foreach(fun(QueryId) ->
                          ets:delete(LKvstore, QueryId),
                          ets:delete(LColumnIndex, QueryId)
                  end, QueryIdList),
    ok.

%%%===================================================================
%%% State Mng funcs
%%%===================================================================

get_db_pid(State) -> State#state.sdsPid.
get_tx_mng_pid(State) -> State#state.txMngPid.
get_lock_mng_pid(State) -> State#state.lockMngPid.
get_txid(State) -> State#state.txid.
get_local_kvstore(State) -> State#state.lKvstore.
get_local_column_index(State) -> State#state.lColumnIndex.
get_query_id_list(State) -> State#state.queryId.

%% クエリIDは実行順に並べる必要がある。ローカルデータの重ね方が
%% この順序に依存しているため。
add_query_id(State, QueryId) ->
    State#state{queryId = State#state.queryId ++ [QueryId]}.

%%%===================================================================
%%% Local Table mng funcs
%%%===================================================================

%% ローカル領域にinsタグのついたデータを入れる。
local_insert_data(State, QueryId, TableName, Oid, Val) ->
    local_data(State, QueryId, ins, TableName, Oid, Val).

%% ローカル領域にdelタグのついたデータを入れる。
local_delete_data(State, QueryId, TableName, Oid, Val) ->
    local_data(State, QueryId, del, TableName, Oid, Val).

local_data(State, QueryId, Action, TableName, Oid, Val) ->
    ok = acquire_table_lock(State, TableName, read),
    LKvstore = get_local_kvstore(State),
    LColumnIndex = get_local_column_index(State),
    {ok, ColList} = sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName),
    lists:foreach(fun({ColName, ColVal}) ->
                          ets:insert(LColumnIndex,
                                     {QueryId, Action, TableName, ColName, ColVal, Oid})
                  end, lists:zip(ColList, Val)),
    ets:insert(LKvstore, {QueryId, Action, TableName, Oid, Val}),
    ok.

%%%===================================================================
%%% Util funcs
%%%===================================================================

%% トランザクションが開始済みか確かめ、自分の順番が来るまで待つ。
%% 読み取り専用は直列化の列に並ばないので、順番待ちも要らない。
ask_transaction(#state{readonly = true}) ->
    ok;
ask_transaction(#state{txid = undefined}) ->
    transaction_not_found;
ask_transaction(#state{txMngPid = TPid, txid = Txid}) ->
    tx_mng:allow_tx(TPid, Txid).

%% 各トランザクションがローカルで保持するデータを持つためのテーブル作成。
%% このプロセスが所有するので、プロセスの終了と同時に消える。
create_local_tables() ->
    LKvstore = ets:new(local_kvstore, [bag]),
    LColumnIndex = ets:new(local_column_index, [bag]),
    {LKvstore, LColumnIndex}.

%% テーブルが存在し、カラム数が合っているかを確かめる。
check_table(_State, TableName, Val) ->
    case sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName) of
        {error, table_not_found} ->
            {error, table_not_found};
        {ok, ColumnList} when length(ColumnList) =/= length(Val) ->
            {error, column_count_mismatch};
        {ok, _ColumnList} ->
            ok
    end.

%% 条件に一致するオブジェクトIDのリストを返す。
%% 共有インデックスを引いた結果に、自分のローカルの変更を実行順に重ねる。
select_object_id_list(State, TableName, ColName, Val, QueryIdList) ->
    %% 索引が無ければ走査に落ちる。索引だけを見ていると、
    %% CREATE INDEX していないカラムへの検索が黙って空になる。
    %% 存在しないカラムは従来どおり空を返す(タプルAPIの互換)。
    ShareData = case simple_db_server:oids_matching(TableName, ColName, Val) of
                    {error, _Reason} -> [];
                    OidList          -> OidList
                end,
    lists:foldl(fun(QueryId, SData) ->
                        merge_local_index(State, TableName, ColName, Val, SData, QueryId)
                end, ShareData, QueryIdList).

merge_local_index(State, TableName, ColName, Val, ShareData, QueryId) ->
    LColumnIndex = get_local_column_index(State),
    case ets:match_object(LColumnIndex, {QueryId, '_', TableName, ColName, Val, '_'}) of
        [] ->
            ShareData;
        LocalDataList ->
            %% 同じクエリ内ではdelを先に適用してからinsを適用する。
            %% bagの取り出し順に依存しないよう明示的に並べ替える。
            Dels = [E || {_Q, del, _T, _C, _V, _O} = E <- LocalDataList],
            Ins = [E || {_Q, ins, _T, _C, _V, _O} = E <- LocalDataList],
            lists:foldl(fun apply_local_index/2, ShareData, Dels ++ Ins)
    end.

apply_local_index({_QueryId, ins, _TableName, _ColName, _Val, Oid}, ShareData) ->
    case lists:member(Oid, ShareData) of
        true -> ShareData;
        false -> ShareData ++ [Oid]
    end;
apply_local_index({_QueryId, del, _TableName, _ColName, _Val, Oid}, ShareData) ->
    lists:filter(fun(X) -> X =/= Oid end, ShareData).

%% オブジェクトIDから行を読む。
%% 共有データを読んだ上に、自分のローカルの変更を実行順に重ねる。
select_data(State, TableName, OidList, QueryIdList) when is_list(OidList) ->
    [select_data(State, TableName, Oid, QueryIdList) || Oid <- OidList];
select_data(State, TableName, Oid, QueryIdList) ->
    ShareData = case simple_db_server:read_data_oid(TableName, Oid) of
                    {error, _} -> not_found;
                    Val -> Val
                end,
    lists:foldl(fun(QueryId, SData) -> merge_local_data(State, SData, Oid, QueryId) end,
                ShareData, QueryIdList).

merge_local_data(State, ShareData, Oid, QueryId) ->
    LKvstore = get_local_kvstore(State),
    case ets:match_object(LKvstore, {QueryId, '_', '_', Oid, '_'}) of
        [] ->
            ShareData;
        LocalData ->
            %% insがあればその値、delだけならnot_found
            case [V || {_Q, ins, _T, _O, V} <- LocalData] of
                [Val | _] -> Val;
                [] -> not_found
            end
    end.

%%%===================================================================
%%% Lock funcs
%%%===================================================================

acquire_lock(_State, [], _RW) ->
    ok;
%% **読みロックは取らない。**
%%
%% 読む値はスナップショットと undo で決まるので、ロックしても
%% 見え方は変わらない。むしろ走査は表の全行に触るため、読みロックを
%% 取ると事実上の表ロックになって並行に走れなくなる。
%%
%% 代わりに失うのは直列化可能性で、得られるのはスナップショット分離。
acquire_lock(_State, _ObjectId, read) ->
    ok;
acquire_lock(State, ObjectId, write) ->
    lock(State, ObjectId, write).

%%----------------------------------------------------------------------
%% 書き込む行のロックを取り、**その場で**衝突を確かめる。
%%
%% ロックが取れた時点で「自分より先にこの行を書いた者はもういない」が、
%% 「自分がスナップショットを取った後に誰かが書いた」かもしれない。
%% そのまま進めると、先にコミットした側の更新を踏み潰す。
%%
%% コミットまで待ってから断ることもできる(実際そこでも確かめている)が、
%% それだと**残りの文を全部やってから捨てる**ことになる。PostgreSQL が
%% UPDATE の時点で "could not serialize access due to concurrent update"
%% を返すのと同じく、ここで返したほうがやり直しが安い。
%%----------------------------------------------------------------------
lock_for_write(_State, _Table, []) ->
    ok;
lock_for_write(State, Table, OidList) ->
    ok = acquire_lock(State, OidList, write),
    case check_conflicts(State, [{Table, Oid} || Oid <- OidList]) of
        ok              -> ok;
        {error, Reason} -> throw({abort, Reason})
    end.

%%----------------------------------------------------------------------
%% 表そのもののロック。
%%
%% 行ロックだけだと、書きかけのトランザクションの下で DROP TABLE が
%% 通ってしまう。行を触るトランザクションは表の共有ロックを、DDL は
%% 排他ロックを取る。DDL は書き手が終わるまで待つ。
%%
%% 共有ロックどうしは競合しないので、同じ表への書き手が互いを待つことは
%% ない。行の競合だけが待ちを作る。
%%----------------------------------------------------------------------
acquire_table_lock(State, TableName, RW) ->
    lock(State, {table, TableName}, RW).

lock(State, ObjectId, RW) ->
    case lock_mng:acquire_lock(get_lock_mng_pid(State), get_txid(State),
                               ObjectId, RW) of
        ok ->
            ok;
        %% 待つと閉路ができる。ここで捨てないと双方が永久に止まる。
        %% 呼び出し元は文の途中なので、throw で文の境界まで戻す。
        {error, deadlock} ->
            throw({abort, deadlock})
    end.
