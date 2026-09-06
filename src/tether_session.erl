%%%-------------------------------------------------------------------
%%% @doc
%%% クライアント1人につき1つのプロセス。**この設計の中心。**
%%%
%%% == なぜ接続ではなくクライアントなのか ==
%%%
%%% 既存のデータベースでは、セッションは接続と同じ寿命を持つ。
%%% PostgreSQL の backend も MySQL のスレッドも、接続が切れた瞬間に
%%% 消える。だから「さっきの要求は実行しましたか」に答えられない。
%%%
%%% その結果どのデータベースも、コミットが不明になったときの後始末を
%%% アプリケーションに押し付けている。冪等キーの表を自分で作れ、と。
%%%
%%% ここではセッションを接続から切り離す。切断しても、プロセスが
%%% 死んでも、ノードが落ちても、記憶は残る。記憶の置き場所はログで、
%%% プロセスはその写しを持っているだけである。
%%%
%%% == 通番の規約 ==
%%%
%%%   Seq =:= Last      再送。**実行せず、初回と同じ答えを返す**
%%%   Seq =:= Last + 1  新しい要求。実行して覚える
%%%   Seq  <  Last      古すぎる。答えはもう覚えていない
%%%   Seq  >  Last + 1  飛んでいる。受け付けない
%%%
%%% 覚えておくのは直前の1件だけでよい。Seq+1 を送ってきた時点で、
%%% クライアントは Seq の答えを受け取ったことになるからである。
%%% 未応答の要求は同時に1件、という制約と引き換えに、
%%% セッションの記憶が定数サイズに収まる。100万セッションを
%%% 抱える設計では、これが効く。
%%%
%%% == 直列化がただで手に入る ==
%%%
%%% 同じクライアントからの要求は、このプロセスのメールボックスに
%%% 並ぶので、自動的に1件ずつ処理される。再送が本体と競走することも、
%%% 二重に実行されることもない。ロックも比較交換も要らない。
%%%
%%% == 他のクライアントを巻き込まない ==
%%%
%%% * ヒープが独立しているので、このプロセスの GC は他を止めない
%%% * max_heap_size を超えれば、**このプロセスだけ**死ぬ
%%% * 暴走してもリダクション計数で先取りされ、他は動き続ける
%%% * 異常終了しても temporary なので、他へ波及しない。
%%%   次の要求で作り直され、記憶はログから取り戻す
%%% @end
%%%-------------------------------------------------------------------
-module(tether_session).
-behaviour(gen_server).

-export([start_link/1, request/3, info/1]).
-export([subscribe/2, unsubscribe/2, sync/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([result/0]).

-type result() :: {ok, [tether_data:group_result()]}
                | {error, {seq_too_old, non_neg_integer()}}
                | {error, {seq_gap, non_neg_integer()}}.

%% 何もしないセッションは、この時間で自分を畳む。
%% 記憶はログにあるので、消えても失われない。
-define(IDLE_MS, 300000).

%% 1セッションが使えるメモリの上限(ワード)。超えたらそのプロセスだけ死ぬ。
%% 多テナントの共有サービスとして、1人の都合が全体に出ないようにするため。
%% 共有ヒープの実行環境では、この上限をプロセス単位で掛けられないので、
%% 1人の食い潰しがサービス全体の OOM になる。
-define(DEFAULT_MAX_HEAP, 1000000).

%% 圏外のクライアントのために溜めておける変更の上限。
%% 越えたら差分を捨て、「取り直せ」に切り替える。
%%
%% **上限が要るのが要点。** 無制限に溜めると、戻ってこないクライアント
%% 1人がメモリを食い潰す。PostgreSQL の論理レプリケーションスロットが
%% WAL を溜め続けてディスクを埋める事故と同じ形で、あちらは
%% スロット単位で止められないので運用事故になる。
%% ここではプロセス単位に上限があるので、**そのクライアントだけ**が
%% 「取り直し」に落ちる。他は何も感じない。
-define(DEFAULT_BACKLOG, 10000).

-record(s, {
          client         :: binary(),
          last = 0       :: non_neg_integer(),
          reply          :: term(),
          served = 0     :: non_neg_integer(),   % 実行した件数
          deduped = 0    :: non_neg_integer(),   % 再送として吸収した件数
          %% --- クライアントの複製の管理 ---
          subs = []      :: [binary()],          % 購読しているコレクション
          pending = #{}  :: #{tether_data:key() => true},  % 未送信の変更
          version = 0    :: non_neg_integer(),   % pending が対応する版
          overflow = false :: boolean(),         % 溜めきれなくなった
          backlog        :: pos_integer()
         }).

%%%===================================================================
%%% 公開
%%%===================================================================

-spec start_link(binary()) -> {ok, pid()} | {error, term()}.
start_link(Client) -> gen_server:start_link(?MODULE, Client, []).

-spec request(pid(), non_neg_integer(), [[tether_data:op()]]) -> result().
request(Pid, Seq, Groups) -> gen_server:call(Pid, {request, Seq, Groups}, 30000).

-spec info(pid()) -> map().
info(Pid) -> gen_server:call(Pid, info).

%%----------------------------------------------------------------------
%% @doc コレクションを購読する。**現在の中身と版を返す。**
%% 以後の変更はこのプロセスが溜め、sync/1 で差分として渡す。
%%----------------------------------------------------------------------
-spec subscribe(pid(), binary()) ->
          {ok, non_neg_integer(), [{tether_data:key(), tether_data:value()}]}.
subscribe(Pid, Coll) -> gen_server:call(Pid, {subscribe, Coll}, 30000).

-spec unsubscribe(pid(), binary()) -> ok.
unsubscribe(Pid, Coll) -> gen_server:call(Pid, {unsubscribe, Coll}).

%%----------------------------------------------------------------------
%% @doc 前回から変わったぶんを受け取る。
%%
%%   {delta,  Version, [{Key, Value | deleted}]}   差分で追いつけた
%%   {resync, Version, [{Coll, Rows}]}             溜めきれなかった。取り直し
%%----------------------------------------------------------------------
-spec sync(pid()) -> {delta, non_neg_integer(), list()}
                   | {resync, non_neg_integer(), list()}.
sync(Pid) -> gen_server:call(Pid, sync, 30000).

%%%===================================================================
%%% gen_server
%%%===================================================================

init(Client) ->
    %% 1人が食い潰しても、他のクライアントには何も起きない。
    MaxHeap = application:get_env(tether, session_max_heap, ?DEFAULT_MAX_HEAP),
    _ = process_flag(max_heap_size, #{size => MaxHeap, kill => true,
                                      error_logger => false}),
    %% 記憶を取り戻す。ログから復旧済みの状態がストアにある。
    {Last, Reply} = case tether_store:session(Client) of
                        none -> {0, undefined};
                        LR   -> LR
                    end,
    {ok, #s{client = Client, last = Last, reply = Reply,
            backlog = application:get_env(tether, sync_backlog,
                                          ?DEFAULT_BACKLOG)}, ?IDLE_MS}.

handle_call({request, Seq, _Ops}, _From, #s{last = Last, reply = R} = S)
  when Seq =:= Last, Last > 0 ->
    %% 再送。**実行しない。** 初回とまったく同じ答えを返す。
    %% 「コミットが失敗と返ったのに入っている」が消えるのは、この一行。
    {reply, R, S#s{deduped = S#s.deduped + 1}, ?IDLE_MS};
handle_call({request, Seq, Groups}, _From, #s{last = Last, client = C} = S)
  when Seq =:= Last + 1 ->
    Reply = tether_store:submit_batch(C, Seq, Groups),
    {reply, Reply, S#s{last = Seq, reply = Reply, served = S#s.served + 1}, ?IDLE_MS};
handle_call({request, Seq, _Ops}, _From, #s{last = Last} = S) when Seq =< Last ->
    {reply, {error, {seq_too_old, Last}}, S, ?IDLE_MS};
handle_call({request, _Seq, _Ops}, _From, #s{last = Last} = S) ->
    {reply, {error, {seq_gap, Last}}, S, ?IDLE_MS};
handle_call({subscribe, Coll}, _From, #s{subs = Subs} = S) ->
    %% **名簿への登録が先、複製の取得が後。** 逆にすると、その隙間に
    %% 起きた変更をどちらも拾えない。この順なら重複するだけで済み、
    %% 差分は「現在値」なので重複は無害。
    ok = tether_sessions:subscribe(Coll, self()),
    {V, Rows} = tether_store:snapshot(Coll),
    {reply, {ok, V, Rows},
     S#s{subs = lists:usort([Coll | Subs]), version = max(V, S#s.version)},
     ?IDLE_MS};
handle_call({unsubscribe, Coll}, _From, #s{subs = Subs} = S) ->
    ok = tether_sessions:unsubscribe(Coll, self()),
    {reply, ok, S#s{subs = lists:delete(Coll, Subs)}, ?IDLE_MS};
handle_call(sync, _From, #s{overflow = true, subs = Subs} = S) ->
    %% 溜めきれなかった。全部取り直してもらう。
    Snaps = [{C, element(2, tether_store:snapshot(C))} || C <- Subs],
    V = tether_store:version(),
    {reply, {resync, V, Snaps},
     S#s{overflow = false, pending = #{}, version = V}, ?IDLE_MS};
handle_call(sync, _From, #s{pending = P, version = V} = S) ->
    Rows = tether_store:read_many(maps:keys(P)),
    {reply, {delta, V, Rows}, S#s{pending = #{}}, ?IDLE_MS};
handle_call(info, _From, #s{client = C, last = L, served = Sv, deduped = D} = S) ->
    {reply, #{client => C, last => L, served => Sv, deduped => D,
              subs => S#s.subs, pending => maps:size(S#s.pending),
              version => S#s.version, overflow => S#s.overflow}, S, ?IDLE_MS};
handle_call(_R, _From, S) ->
    {reply, {error, unknown_call}, S, ?IDLE_MS}.

handle_cast(_M, S) -> {noreply, S, ?IDLE_MS}.

%% 購読しているコレクションが書き換わった。**溜めておく。**
%% クライアントが圏外でも、このプロセスは生きていて記録し続ける。
%% 行にはこれができない。
handle_info({changed, V, Keys}, #s{pending = P, backlog = B} = S) ->
    P1 = lists:foldl(fun(K, M) -> M#{K => true} end, P, Keys),
    case maps:size(P1) > B of
        true ->
            %% 溜めきれない。差分を捨てて「取り直せ」に切り替える。
            %% **捨てるのはこのクライアントの分だけ。** 他には波及しない。
            {noreply, S#s{pending = #{}, overflow = true, version = V}, ?IDLE_MS};
        false ->
            {noreply, S#s{pending = P1, version = V}, ?IDLE_MS}
    end;
%% 暇なら畳む。100万セッションのうち動いているのは一握り、という
%% 前提で設計している。hibernate はヒープを最小まで縮める。
handle_info(timeout, S) -> {noreply, S, hibernate};
handle_info(_M, S)      -> {noreply, S, ?IDLE_MS}.
