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
-export([start_link/0, stop/1, exec_query/2]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2,
         code_change/3]).

-record(state, {sdsPid, txMngPid, lockMngPid, txid, lKvstore, lColumnIndex, queryId = []}).

%% トランザクションから見えるテーブル走査の状態。
%% base   : 共有ページの走査カーソル
%% delta  : このトランザクションの最終的な差分 #{Oid => deleted | {row, Row}}
%% pending: 共有側にまだ存在しない、このトランザクションの挿入行
%% stage  : base を出し切ってから pending を出す
-record(tx_scan, {base, delta = #{}, pending = [], stage = base}).

-include("../include/simple_db_server.hrl").

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

handle_call({exec_query, {begin_tx}}, _From, #state{txid = undefined} = State) ->
    Txid = tx_mng:begin_tx(get_tx_mng_pid(State)),
    {reply, Txid, State#state{txid = Txid, queryId = []}};
handle_call({exec_query, {begin_tx}}, _From, State) ->
    %% ネストしたトランザクションは扱わない
    {reply, {error, transaction_already_started}, State};

handle_call({exec_query, {commit_tx}}, _From, State) ->
    with_transaction(State, fun() -> do_commit(State) end);

handle_call({exec_query, {rollback_tx}}, _From, State) ->
    with_transaction(State, fun() -> do_rollback(State) end);

%% DDLはトランザクションの対象外。即座に共有データへ反映する。
handle_call({exec_query, {create_table, TableName, ColumnList}}, _From, State) ->
    {reply, simple_db_server:create_table(get_db_pid(State), TableName, ColumnList), State};

handle_call({exec_query, {drop_table, TableName}}, _From, State) ->
    {reply, simple_db_server:drop_table(get_db_pid(State), TableName), State};

handle_call({exec_query, {insert, TableName, Val}}, _From, State) ->
    with_transaction(State, fun() -> do_insert(State, TableName, Val) end);

handle_call({exec_query, {select, TableName, ColName, Val}}, _From, State) ->
    with_transaction(State, fun() -> do_select(State, TableName, ColName, Val) end);

handle_call({exec_query, {scan, TableName}}, _From, State) ->
    with_transaction(State, fun() -> do_scan(State, TableName) end);

handle_call({exec_query, {sql, Sql}}, _From, State) ->
    with_transaction(State, fun() -> do_sql(State, Sql) end);

handle_call({exec_query, {update, TableName, SetQuery, ColName, Val}}, _From, State) ->
    with_transaction(State, fun() -> do_update(State, TableName, SetQuery, ColName, Val) end);

handle_call({exec_query, {delete, TableName, ColName, Val}}, _From, State) ->
    with_transaction(State, fun() -> do_delete(State, TableName, ColName, Val) end);

handle_call({exec_query, Query}, _From, State) ->
    {reply, {error, {unsupported_query, Query}}, State};

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

%% トランザクションが開始済みで、かつ自分の順番が来ていることを確かめてから
%% Funを実行する。順番待ちの間はここでブロックする。
with_transaction(State, Fun) ->
    case ask_transaction(State) of
        transaction_not_found ->
            {reply, transaction_not_found, State};
        ok ->
            Fun()
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

do_select(State, TableName, ColName, Val) ->
    case sys_tbl_mng:exist_table(whereis(sys_tbl_mng), TableName) of
        false ->
            {reply, {error, table_not_found}, State};
        true ->
            QueryIdList = get_query_id_list(State),
            OidList = select_object_id_list(State, TableName, ColName, Val, QueryIdList),
            ok = acquire_lock(State, OidList, read),
            Rows = [R || R <- select_data(State, TableName, OidList, QueryIdList),
                         R =/= not_found],
            {reply, Rows, State}
    end.

%% SQL文を実行する。
%% 字句解析 -> 構文解析 -> 意味解析(カラムを位置に解決) -> 実行。
do_sql(State, Sql) ->
    case sql:parse(Sql) of
        {error, Reason} ->
            {reply, {error, Reason}, State};
        {ok, Ast} ->
            case sql_analyzer:analyze(Ast) of
                {error, Reason} ->
                    {reply, {error, Reason}, State};
                {ok, Plan} ->
                    %% 実行器にはストレージへの入口を関数で渡す。
                    %% こうしておくと実行器がquery_execの内部状態に触らずに済み、
                    %% かつ走査がこのトランザクションの未コミット変更を見られる。
                    Ctx = {fun(T) -> tx_scan_open(State, T) end,
                           fun(C) -> tx_scan_next(C) end},
                    {reply, sql_exec:run(Plan, Ctx), State}
            end
    end.

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
    ok = acquire_lock(State, OidList, write),
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
    ok = acquire_lock(State, OidList, write),
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
            Delta = build_delta(State, TableName),
            {ok, #tx_scan{base = Base, delta = Delta,
                          pending = local_only_rows(TableName, Delta)}}
    end.

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
do_commit(State) ->
    TPid = get_tx_mng_pid(State),
    Txid = get_txid(State),
    QueryIdList = get_query_id_list(State),
    Changes = collect_changes(State, QueryIdList),
    ok = write_redo_log(Txid, Changes),
    ok = apply_changes(Changes),
    ok = log_util:redo_log_put_checkpoint(),
    clear_local(State, QueryIdList),
    Rep = tx_mng:commit_tx(TPid, Txid),
    {reply, Rep, State#state{txid = undefined, queryId = []}}.

%% ロールバックはローカル領域を捨てるだけでよい。
%% 共有データにはまだ何も書いていない。
do_rollback(State) ->
    TPid = get_tx_mng_pid(State),
    Txid = get_txid(State),
    clear_local(State, get_query_id_list(State)),
    Rep = tx_mng:rollback_tx(TPid, Txid),
    {reply, Rep, State#state{txid = undefined, queryId = []}}.

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

%% コミット中にテーブルが消えているなど、反映できない変更は
%% 落とさずに読み飛ばす。REDOログにも同じ判断が入る。
ensure_ok(ok) -> ok;
ensure_ok({error, table_not_found}) -> ok;
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
    ShareData = (simple_db_server:index_module()):select_index(TableName, ColName, Val),
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
acquire_lock(State, ObjectId, RW) ->
    lock_mng:acquire_lock(get_lock_mng_pid(State), get_txid(State), ObjectId, RW).
