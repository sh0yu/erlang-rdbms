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
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([reply/0]).

-type reply() :: {ok, [tether_data:result()]}
               | {error, pos_integer(), tether_data:result()}.

-record(s, {
          db       :: tether_data:db(),
          sessions :: tether_recovery:sessions()
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

%%%===================================================================
%%% gen_server
%%%===================================================================

init(Dir) ->
    case tether_recovery:load(Dir) of
        {ok, Db, Sess, Info} ->
            logger:notice("tether_store: ~p 件を再実行して復旧 (~p バイト, 末尾 ~p)",
                          [maps:get(records, Info), maps:get(valid_bytes, Info),
                           maps:get(tail, Info)]),
            {ok, #s{db = Db, sessions = Sess}};
        {error, R} ->
            {stop, {recovery_failed, R}}
    end.

handle_call({submit, Client, Seq, Ops}, From, #s{db = Db} = S) ->
    {Reply, Db1} = case tether_data:apply_ops(Ops, Db) of
                       {ok, Results, D1} -> {{ok, Results}, D1};
                       {error, N, R, D1} -> {{error, N, R}, D1}
                   end,
    Entry = tether_entry:new(Client, Seq, Ops, Reply),
    ok = tether_log:commit(tether_entry:encode(Entry), From, Reply),
    %% 返答しない。永続化されたらログが From へ返す。
    {noreply, S#s{db = Db1, sessions = maps:put(Client, {Seq, Reply}, S#s.sessions)}};
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
handle_call(_R, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_M, S) -> {noreply, S}.
handle_info(_M, S) -> {noreply, S}.
