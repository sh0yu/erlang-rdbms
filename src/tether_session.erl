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
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([result/0]).

-type result() :: {ok, [tether_data:result()]}
                | {error, pos_integer(), tether_data:result()}
                | {error, {seq_too_old, non_neg_integer()}}
                | {error, {seq_gap, non_neg_integer()}}.

%% 何もしないセッションは、この時間で自分を畳む。
%% 記憶はログにあるので、消えても失われない。
-define(IDLE_MS, 300000).

%% 1セッションが使えるメモリの上限(ワード)。超えたらそのプロセスだけ死ぬ。
%% 多テナントの共有サービスとして、1人の都合が全体に出ないようにするため。
-define(MAX_HEAP, 1000000).

-record(s, {
          client         :: binary(),
          last = 0       :: non_neg_integer(),
          reply          :: term(),
          served = 0     :: non_neg_integer(),   % 実行した件数
          deduped = 0    :: non_neg_integer()    % 再送として吸収した件数
         }).

%%%===================================================================
%%% 公開
%%%===================================================================

-spec start_link(binary()) -> {ok, pid()} | {error, term()}.
start_link(Client) -> gen_server:start_link(?MODULE, Client, []).

-spec request(pid(), non_neg_integer(), [tether_data:op()]) -> result().
request(Pid, Seq, Ops) -> gen_server:call(Pid, {request, Seq, Ops}, 10000).

-spec info(pid()) -> #{client := binary(), last := non_neg_integer(),
                       served := non_neg_integer(), deduped := non_neg_integer()}.
info(Pid) -> gen_server:call(Pid, info).

%%%===================================================================
%%% gen_server
%%%===================================================================

init(Client) ->
    %% 1人が食い潰しても、他のクライアントには何も起きない。
    process_flag(max_heap_size, #{size => ?MAX_HEAP, kill => true,
                                  error_logger => true}),
    %% 記憶を取り戻す。ログから復旧済みの状態がストアにある。
    {Last, Reply} = case tether_store:session(Client) of
                        none -> {0, undefined};
                        LR   -> LR
                    end,
    {ok, #s{client = Client, last = Last, reply = Reply}, ?IDLE_MS}.

handle_call({request, Seq, _Ops}, _From, #s{last = Last, reply = R} = S)
  when Seq =:= Last, Last > 0 ->
    %% 再送。**実行しない。** 初回とまったく同じ答えを返す。
    %% 「コミットが失敗と返ったのに入っている」が消えるのは、この一行。
    {reply, R, S#s{deduped = S#s.deduped + 1}, ?IDLE_MS};
handle_call({request, Seq, Ops}, _From, #s{last = Last, client = C} = S)
  when Seq =:= Last + 1 ->
    Reply = tether_store:submit(C, Seq, Ops),
    {reply, Reply, S#s{last = Seq, reply = Reply, served = S#s.served + 1}, ?IDLE_MS};
handle_call({request, Seq, _Ops}, _From, #s{last = Last} = S) when Seq =< Last ->
    {reply, {error, {seq_too_old, Last}}, S, ?IDLE_MS};
handle_call({request, _Seq, _Ops}, _From, #s{last = Last} = S) ->
    {reply, {error, {seq_gap, Last}}, S, ?IDLE_MS};
handle_call(info, _From, #s{client = C, last = L, served = Sv, deduped = D} = S) ->
    {reply, #{client => C, last => L, served => Sv, deduped => D}, S, ?IDLE_MS};
handle_call(_R, _From, S) ->
    {reply, {error, unknown_call}, S, ?IDLE_MS}.

handle_cast(_M, S) -> {noreply, S, ?IDLE_MS}.

%% 暇なら畳む。100万セッションのうち動いているのは一握り、という
%% 前提で設計している。hibernate はヒープを最小まで縮める。
handle_info(timeout, S) -> {noreply, S, hibernate};
handle_info(_M, S)      -> {noreply, S, ?IDLE_MS}.
