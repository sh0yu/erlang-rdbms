%%%-------------------------------------------------------------------
%%% @doc
%%% データ本体を持つプロセス。
%%%
%%% == なぜ fsync を待たないのか ==
%%%
%%% 素直に書くと、ここで tether_log:write/1 を呼んで永続化を待ってから
%%% 返したくなる。それをやると**スループットが fsync の回数で頭打ちに
%%% なる**。ストアは1件ずつ順番に fsync を待つので、group commit が
%%% あってもまとまる相手がいない。
%%%
%%% そこで、答える権利をログへ渡す。
%%%
%%%   1. 操作をメモリ上の状態に適用する(これは速い)
%%%   2. ログへ「これを永続化したら、この答えを From へ返しておいてくれ」と頼む
%%%   3. **返答せずに**次の要求へ移る
%%%
%%% クライアントから見れば、返ってきた時点で永続化は済んでいる。
%%% ストアから見れば、fsync を一度も待っていない。
%%%
%%% == 状態がログより先に進むことについて ==
%%%
%%% 2 と 3 の間で電源が落ちると、適用済みだがログに無い状態が消える。
%%% **それでよい。** その要求にはまだ誰も答えていないからである。
%%% 「答えたものは残る」は保たれる。
%%%
%%% == 失敗した要求もログに載せる ==
%%%
%%% cas が外れたような失敗も記録する。載せないと、再送されたときに
%%% 再実行してしまう。そのときには状態が変わっていて成功するかもしれない。
%%% それは exactly-once の破れである。**答えたことは、成功でも失敗でも
%%% 覚えていなければならない。**
%%% @end
%%%-------------------------------------------------------------------
-module(tether_store).
-behaviour(gen_server).

-export([start_link/1, submit/3, read/1, size/0, keys/0, sessions/0, session/1]).
-export([checkpoint/0, entries/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([reply/0]).

-type reply() :: {ok, [tether_data:result()]}
               | {error, pos_integer(), tether_data:result()}.

-record(s, {
          dir        :: file:filename_all(),
          db         :: tether_data:db(),
          sessions   :: tether_recovery:sessions(),
          %% ログ全体を通した番号。切り詰めても振り直さない。
          %% スナップショットは「ここまで反映済み」としてこれを記録する。
          index = 0 :: non_neg_integer(),
          %% いまログに載っているレコード数。切り詰める量を決めるのに使う。
          log_records = 0 :: non_neg_integer(),
          checkpointing = false :: boolean(),
          %% ログがこの件数を超えたら自動でチェックポイントを取る。
          %% 取らないとログが無限に伸び、復旧時間も伸び続ける。
          ckpt_after = 10000 :: pos_integer()
         }).

%%%===================================================================
%%% 公開
%%%===================================================================

-spec start_link(file:filename_all()) -> {ok, pid()} | {error, term()}.
start_link(Dir) -> gen_server:start_link({local, ?MODULE}, ?MODULE, Dir, []).

%%----------------------------------------------------------------------
%% @doc 操作を原子的に適用して永続化する。**返った時点で fsync 済み。**
%%----------------------------------------------------------------------
-spec submit(binary(), non_neg_integer(), [tether_data:op()]) ->
          reply() | {error, term()}.
submit(Client, Seq, Ops) ->
    gen_server:call(?MODULE, {submit, Client, Seq, Ops}, 5000).

-spec read(tether_data:key()) -> {ok, tether_data:value()} | not_found.
read(Key) -> gen_server:call(?MODULE, {read, Key}).

-spec size() -> non_neg_integer().
size() -> gen_server:call(?MODULE, size).

-spec keys() -> [tether_data:key()].
keys() -> gen_server:call(?MODULE, keys).

%% @doc 復旧で組み直したセッション状態。Step 4 のセッションが使う。
-spec sessions() -> tether_recovery:sessions().
sessions() -> gen_server:call(?MODULE, sessions).

%%----------------------------------------------------------------------
%% @doc 1クライアントぶんのセッション状態。
%% セッションプロセスが起き上がるときに、ここから自分の記憶を取り戻す。
%% プロセスが死んでもノードが落ちても、記憶はログにあるので失われない。
%%----------------------------------------------------------------------
-spec session(binary()) -> {non_neg_integer(), term()} | none.
session(Client) -> gen_server:call(?MODULE, {session, Client}).

%%----------------------------------------------------------------------
%% @doc 現在の状態を書き出して、ログの前半を捨てる。
%%
%% 書き出しは別のプロセスにやらせる。**状態が不変な値なので、
%% そのまま渡せる。** 破壊的に更新する実装なら、書き出している間
%% 状態を凍らせるか、写しを取るか、copy-on-write の仕掛けが要る。
%% ここでは渡した時点の値がそのまま残るので、ストアは待たずに
%% 次の要求へ進める。
%%
%% ログを捨てるのは、書き出しが**永続化されてから**。
%% 逆にすると、その間の電源断でデータが消える。
%%----------------------------------------------------------------------
-spec checkpoint() -> ok | {error, term()}.
checkpoint() -> gen_server:call(?MODULE, checkpoint, 60000).

-spec entries() -> non_neg_integer().
entries() -> gen_server:call(?MODULE, entries).

%%%===================================================================
%%% gen_server
%%%===================================================================

init(Dir) ->
    case tether_recovery:load(Dir) of
        {ok, Db, Sess, Info} ->
            logger:notice("tether_store: 復旧 — スナップショットが ~p 番まで、"
                          "ログから ~p 件を再実行 (~p 件読み飛ばし, ~p バイト, 末尾 ~p)",
                          [maps:get(upto, Info), maps:get(applied, Info),
                           maps:get(skipped, Info), maps:get(valid_bytes, Info),
                           maps:get(tail, Info)]),
            {ok, #s{dir = Dir, db = Db, sessions = Sess,
                    ckpt_after = application:get_env(tether, checkpoint_after,
                                                     10000),
                    %% 通し番号は連続しているので、
                    %% スナップショットの到達点 + 再実行した件数が最新。
                    index = maps:get(upto, Info) + maps:get(applied, Info),
                    log_records = maps:get(records, Info)}};
        {error, R} ->
            {stop, {recovery_failed, R}}
    end.

handle_call({submit, Client, Seq, Ops}, From, #s{db = Db, index = I} = S) ->
    {Reply, Db1} = case tether_data:apply_ops(Ops, Db) of
                       {ok, Results, D1} -> {{ok, Results}, D1};
                       {error, N, R, D1} -> {{error, N, R}, D1}
                   end,
    Entry = tether_entry:new(I + 1, Client, Seq, Ops, Reply),
    ok = tether_log:commit(tether_entry:encode(Entry), From, Reply),
    %% 返答しない。永続化されたらログが From へ返す。
    {noreply, maybe_checkpoint(
                S#s{db = Db1, index = I + 1, log_records = S#s.log_records + 1,
                    sessions = maps:put(Client, {Seq, Reply}, S#s.sessions)})};
handle_call({read, Key}, _From, #s{db = Db} = S) ->
    {reply, tether_data:get(Key, Db), S};
handle_call(size, _From, #s{db = Db} = S) ->
    {reply, tether_data:size(Db), S};
handle_call(keys, _From, #s{db = Db} = S) ->
    {reply, tether_data:keys(Db), S};
handle_call(sessions, _From, #s{sessions = Sess} = S) ->
    {reply, Sess, S};
handle_call({session, C}, _From, #s{sessions = Sess} = S) ->
    {reply, maps:get(C, Sess, none), S};
handle_call(entries, _From, #s{index = N} = S) ->
    {reply, N, S};
handle_call(checkpoint, From, #s{checkpointing = true} = S) ->
    _ = From,
    {reply, {error, already_running}, S};
handle_call(checkpoint, From, S) ->
    {noreply, start_snapshot(From, S)};
handle_call(_R, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_M, S) -> {noreply, S}.

%% ログが伸びすぎたら自分でチェックポイントを始める。
%% 呼び出し元が居ないので、返答先は undefined。
maybe_checkpoint(#s{checkpointing = true} = S) -> S;
maybe_checkpoint(#s{log_records = LR, ckpt_after = A} = S) when LR < A -> S;
maybe_checkpoint(S) -> start_snapshot(undefined, S).

start_snapshot(From, #s{dir = Dir, db = Db, sessions = Sess,
                        index = N, log_records = LR} = S) ->
    Store = self(),
    _ = spawn_link(fun() ->
                           R = tether_snapshot:write(Dir, N, Db, Sess),
                           Store ! {snapshot_done, From, LR, R}
                   end),
    S#s{checkpointing = true}.

reply_to(undefined, _R) -> ok;
reply_to(From, R)       -> gen_server:reply(From, R), ok.

%% スナップショットが永続化された。**ここで初めて**ログを捨てる。
%% compact をストア自身が呼ぶのが要点で、別プロセスから呼ぶと
%% ストアが送った追記との順序が保証されない。
handle_info({snapshot_done, From, Drop, ok}, S) ->
    {Reply, S1} =
        case tether_log:compact(Drop) of
            {ok, Info} ->
                logger:notice("tether_store: チェックポイント — "
                              "~p 件を捨て、~p 件を残した",
                              [maps:get(dropped, Info), maps:get(kept, Info)]),
                {ok, S#s{log_records = maps:get(kept, Info)}};
            E -> {E, S}
        end,
    ok = reply_to(From, Reply),
    {noreply, S1#s{checkpointing = false}};
handle_info({snapshot_done, From, _N, E}, S) ->
    logger:error("tether_store: スナップショットの書き出しに失敗: ~p", [E]),
    ok = reply_to(From, E),
    {noreply, S#s{checkpointing = false}};
handle_info(_M, S) -> {noreply, S}.
