%%%-------------------------------------------------------------------
%%% @doc
%%% スナップショットと undo の管理。
%%%
%%% == なぜ要るのか ==
%%%
%%% これまで読み取り専用トランザクションは共有ラッチを持ち続けていた。
%%% 読み手同士は並行に走るが、**コミットの適用は読み手が終わるまで待つ**。
%%% 長い照会が書き込みを止める。
%%%
%%% スナップショットにすると、読み手は「開始時点の状態」を見るので
%%% コミットを待つ必要が無くなる。代わりに、変更された行の**古い値**を
%%% 取っておいて、読むときに巻き戻す。
%%%
%%% == どう巻き戻すか ==
%%%
%%% コミットのたびに、変更前の値を undo として積む。
%%%
%%%   版5 の undo: #{Oid1 => {row, 旧値}, Oid2 => absent}
%%%
%%% 版4 のスナップショットを持つ読み手は、版5以降の undo を重ねれば
%%% 版4 の状態が得られる。同じ行が何度も変更されていたら、
%%% **いちばん古い undo の値**が版4 の値になる。
%%%
%%% == コミットの順序 ==
%%%
%%%   1. undo を積む
%%%   2. 共有データへ適用する(1行ずつ)
%%%   3. 見える版を上げる
%%%
%%% この順序だと、適用の途中でも読み手の見え方が壊れない。
%%%   適用済みの行  → undo が古い値へ戻す
%%%   未適用の行    → もともと古い値
%%% どちらも同じ値になる。
%%%
%%% 逆順(適用してから undo を積む)にすると、その隙間に読んだ行だけが
%%% 新しい値に見える。
%%%
%%% == 古すぎるスナップショット ==
%%%
%%% undo は誰かが要るあいだ捨てられない。長生きする読み手がいると
%%% 際限なく溜まるので、上限を超えたら古い undo を捨て、それを必要と
%%% していたスナップショットを無効にする。以後その読み手は
%%% snapshot_too_old で断られる。PostgreSQL の old_snapshot_threshold と
%%% 同じ割り切りで、黙って古い値を返すよりは断る方がよい。
%%% @end
%%%-------------------------------------------------------------------
-module(snapshot_mng).
-behaviour(gen_server).

-export([start_link/0, commit/1, publish/1, acquire/0, release/1,
         overlay/2, status/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-export_type([seq/0, snapshot/0, overlay/0]).

-type seq()      :: non_neg_integer().
-type snapshot() :: {reference(), seq()}.
%% 走査の重ね合わせと同じ形。query_exec のローカル差分と混ぜられる。
-type overlay()  :: #{term() => deleted | {row, list()}}.

%% 保持する undo の上限(コミット数)。超えたら古いものを捨てる。
-define(DEFAULT_MAX_UNDO, 1000).

-record(s, {
          %% 見えている版。ここまでのコミットは適用済み
          visible = 0 :: seq(),
          %% 積んだ版(適用中を含む)
          next = 0 :: seq(),
          %% 新しい順。[{Seq, #{{Table,Oid} => absent | {row, Val}}}]
          undo = [] :: [{seq(), map()}],
          %% 生きているスナップショット Ref => Seq | invalid
          snaps = #{} :: #{reference() => seq() | invalid},
          %% 捨ててしまった undo の最大版。これ以下を要るスナップショットは無効
          discarded = 0 :: seq(),
          max_undo = ?DEFAULT_MAX_UNDO :: pos_integer(),
          %% 監視 MRef => Ref。接続が死んだらスナップショットを外す。
          %% 外さないと、その版より新しい undo を永久に捨てられない
          mons = #{} :: #{reference() => reference()}
         }).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%----------------------------------------------------------------------
%% @doc 変更前の値を undo として積む。**適用の前に呼ぶ。**
%% Undo は #{{Table, Oid} => absent | {row, Val}}。
%% 返るのはこのコミットの版。適用が終わったら publish/1 に渡す。
%%----------------------------------------------------------------------
-spec commit(map()) -> seq().
commit(Undo) -> gen_server:call(?MODULE, {commit, Undo}).

%% @doc 適用が終わったので、この版を見えるようにする。
-spec publish(seq()) -> ok.
publish(Seq) -> gen_server:call(?MODULE, {publish, Seq}).

%% @doc いまの状態のスナップショットを取る。
-spec acquire() -> snapshot().
acquire() -> gen_server:call(?MODULE, acquire).

-spec release(snapshot()) -> ok.
release({Ref, _Seq}) -> gen_server:call(?MODULE, {release, Ref}).

%%----------------------------------------------------------------------
%% @doc そのスナップショットの時点へ戻すための重ね合わせ。
%% Returns: {ok, overlay()} | {error, snapshot_too_old}
%%----------------------------------------------------------------------
-spec overlay(snapshot(), atom()) -> {ok, overlay()} | {error, snapshot_too_old}.
overlay({Ref, _Seq}, Table) -> gen_server:call(?MODULE, {overlay, Ref, Table}).

-spec status() -> map().
status() -> gen_server:call(?MODULE, status).

%%%===================================================================

init([]) ->
    {ok, #s{max_undo = application:get_env(transaction_db, max_undo, ?DEFAULT_MAX_UNDO)}}.

handle_call({commit, Undo}, _From, #s{next = N, undo = U} = S) ->
    Seq = N + 1,
    {reply, Seq, trim(S#s{next = Seq, undo = [{Seq, Undo} | U]})};

handle_call({publish, Seq}, _From, S) ->
    {reply, ok, S#s{visible = Seq}};

handle_call(acquire, {Pid, _}, #s{visible = V, snaps = Sn, mons = M} = S) ->
    Ref = make_ref(),
    MRef = erlang:monitor(process, Pid),
    {reply, {Ref, V}, S#s{snaps = Sn#{Ref => V}, mons = M#{MRef => Ref}}};

handle_call({release, Ref}, _From, S) ->
    {reply, ok, trim(forget(Ref, S))};

handle_call({overlay, Ref, Table}, _From, #s{snaps = Sn, undo = U} = S) ->
    Reply = case maps:get(Ref, Sn, invalid) of
                invalid -> {error, snapshot_too_old};
                Seq     -> {ok, build_overlay(Seq, Table, U)}
            end,
    {reply, Reply, S};

handle_call(status, _From, #s{visible = V, undo = U, snaps = Sn} = S) ->
    {reply, #{visible => V, undo => length(U), snapshots => map_size(Sn),
              discarded => S#s.discarded}, S};

handle_call(_R, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_M, S) -> {noreply, S}.

%% 接続が死んだ。持っていたスナップショットを外す
handle_info({'DOWN', MRef, process, _Pid, _Reason}, #s{mons = M} = S) ->
    case maps:take(MRef, M) of
        {Ref, M2} -> {noreply, trim(forget(Ref, S#s{mons = M2}))};
        error     -> {noreply, S}
    end;
handle_info(_M, S) -> {noreply, S}.

%% Ref のスナップショットと、それを見張っていた監視を落とす
forget(Ref, #s{snaps = Sn, mons = M} = S) ->
    M2 = maps:filter(fun(MRef, R) when R =:= Ref ->
                             erlang:demonitor(MRef, [flush]), false;
                        (_, _) -> true
                     end, M),
    S#s{snaps = maps:remove(Ref, Sn), mons = M2}.

%%%===================================================================

%%----------------------------------------------------------------------
%% Seq より新しい undo を重ねる。
%%
%% 同じ行が何度も変更されていたら、**いちばん古い undo の値**が
%% その時点の値になる。undo は新しい順に並んでいるので、
%% 順に上書きしていけば最後に残るのが最も古い値。
%%----------------------------------------------------------------------
build_overlay(Seq, Table, Undo) ->
    lists:foldl(
      fun({S, Map}, Acc) when S > Seq ->
              maps:fold(fun({T, Oid}, Before, A) when T =:= Table ->
                                A#{Oid => to_entry(Before)};
                           (_K, _V, A) -> A
                        end, Acc, Map);
         (_Older, Acc) ->
              Acc
      end, #{}, Undo).

%% 当時存在しなかった行は、いま見えていても見せない。
to_entry(absent)      -> deleted;
to_entry({row, Val})  -> {row, Val}.

%%----------------------------------------------------------------------
%% 誰も要らなくなった undo を捨てる。上限を超えていたら、
%% まだ要るものでも捨てて、そのスナップショットを無効にする。
%%----------------------------------------------------------------------
trim(#s{undo = U, snaps = Sn, max_undo = Max} = S) ->
    Keep = case [Seq || {_R, Seq} <- maps:to_list(Sn), Seq =/= invalid] of
               []   -> S#s.next;          % 読み手がいなければ全部要らない
               Seqs -> lists:min(Seqs)
           end,
    U1 = [E || {Seq, _} = E <- U, Seq > Keep],
    case length(U1) > Max of
        false ->
            S#s{undo = U1};
        true ->
            %% 上限超え。古い方から捨て、必要としていた読み手を無効にする
            Kept = lists:sublist(U1, Max),
            Cut = lists:min([Seq || {Seq, _} <- Kept]) - 1,
            Sn1 = maps:map(fun(_R, invalid) -> invalid;
                              (_R, Seq) when Seq < Cut -> invalid;
                              (_R, Seq) -> Seq
                           end, Sn),
            S#s{undo = Kept, snaps = Sn1, discarded = max(S#s.discarded, Cut)}
    end.
