%%%-------------------------------------------------------------------
%%% @doc
%%% コミットの適用を、読み手から見て原子的にするためのラッチ。
%%%
%%% == なぜ要るのか ==
%%%
%%% `apply_changes/1` は変更を1行ずつ共有データへ書く。走査は
%%% data_buffer への呼び出しを何度も繰り返すので、その隙間に
%%% 適用が挟まると、**読み手がコミットの途中を見る**。
%%%
%%% これまでは tx_mng が全トランザクションを1本ずつしか動かさない
%%% ことで隠れていた。並行を許すなら、まずここを閉じる必要がある。
%%% 「読み取り専用だけ並行にする」は一見安全に見えるが、この理由で
%%% 安全でない。
%%%
%%% == 何を保証するのか ==
%%%
%%% 読み手は共有ラッチを**トランザクションの間じゅう**保持する。
%%% 適用は排他ラッチを取る。したがって、
%%%
%%%   * 読み手が動いている間、どのコミットも適用されない
%%%   * 読み手は、あるコミットの全部を見るか、全部を見ないか
%%%
%%% 保持を文単位ではなくトランザクション単位にしているのは、
%%% 文単位だと同じトランザクション内の2つのSELECTが違う状態を
%%% 見てしまうため(反復不能読み取り)。トランザクション単位なら
%%% **直列化可能性が保たれる**。
%%%
%%% 代償は、長い読み取りが書き手を待たせること。ただしこれまでは
%%% 読み手同士すら待たせていたので、後退はしていない。
%%%
%%% == 書き手を飢えさせない ==
%%%
%%% 書き手が待ち始めたら、新しい読み手はその後ろに並ぶ。
%%% さもないと読み手が途切れない限り書き手が永久に待つ。
%%% @end
%%%-------------------------------------------------------------------
-module(commit_latch).
-behaviour(gen_server).

-export([start_link/0, read_lock/0, read_unlock/0, write_lock/0, write_unlock/0]).
-export([with_write/1, with_read/1, status/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2]).

-define(TIMEOUT, infinity).

-record(st, {
          %% 保持中の読み手 monitor参照 => pid
          readers = #{} :: #{reference() => pid()},
          %% 保持中の書き手
          writer = undefined :: undefined | {pid(), reference()},
          %% 待たされている呼び出し。順番に処理する
          waiting_readers = [] :: [{gen_server:from(), pid()}],
          waiting_writers = [] :: [{gen_server:from(), pid()}]
         }).

start_link() -> gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec read_lock() -> ok.
read_lock() -> gen_server:call(?MODULE, {lock, read}, ?TIMEOUT).

-spec read_unlock() -> ok.
read_unlock() -> gen_server:call(?MODULE, {unlock, read}, ?TIMEOUT).

-spec write_lock() -> ok.
write_lock() -> gen_server:call(?MODULE, {lock, write}, ?TIMEOUT).

-spec write_unlock() -> ok.
write_unlock() -> gen_server:call(?MODULE, {unlock, write}, ?TIMEOUT).

%%----------------------------------------------------------------------
%% @doc 排他ラッチの下で実行する。例外が出ても必ず外す。
%% 外し忘れると、以後すべての読み手と書き手が止まる。
%%----------------------------------------------------------------------
-spec with_write(fun(() -> T)) -> T.
with_write(Fun) ->
    ok = write_lock(),
    try Fun()
    after write_unlock()
    end.

%%----------------------------------------------------------------------
%% @doc 共有ラッチの下で実行する。文の実行を DDL から守るために使う。
%% 例外が出ても必ず外す。
%%----------------------------------------------------------------------
-spec with_read(fun(() -> T)) -> T.
with_read(Fun) ->
    ok = read_lock(),
    try Fun()
    after read_unlock()
    end.

-spec status() -> #{readers := non_neg_integer(), writer := boolean(),
                    waiting := non_neg_integer()}.
status() -> gen_server:call(?MODULE, status).

%%%===================================================================

init([]) ->
    {ok, #st{}}.

handle_call({lock, read}, {Pid, _} = From, S) ->
    case can_read(S) of
        true  -> {reply, ok, grant_read(Pid, S)};
        false -> {noreply, S#st{waiting_readers = S#st.waiting_readers ++ [{From, Pid}]}}
    end;
handle_call({lock, write}, {Pid, _} = From, S) ->
    case can_write(S) of
        true  -> {reply, ok, grant_write(Pid, S)};
        false -> {noreply, S#st{waiting_writers = S#st.waiting_writers ++ [{From, Pid}]}}
    end;
handle_call({unlock, read}, {Pid, _}, S) ->
    {reply, ok, dispatch(release_reader(Pid, S))};
handle_call({unlock, write}, {Pid, _}, S) ->
    {reply, ok, dispatch(release_writer(Pid, S))};
handle_call(status, _From, S) ->
    {reply, #{readers => map_size(S#st.readers),
              writer  => S#st.writer =/= undefined,
              waiting => length(S#st.waiting_readers) + length(S#st.waiting_writers)}, S};
handle_call(_R, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_M, S) -> {noreply, S}.

%% 保持したまま死んだら外す。外さないと全体が止まる。
handle_info({'DOWN', Ref, process, Pid, _Reason}, S) ->
    S1 = case S#st.writer of
             {Pid, Ref} -> S#st{writer = undefined};
             _          -> S#st{readers = maps:remove(Ref, S#st.readers)}
         end,
    %% 待ち行列に入ったまま死んだものも取り除く
    S2 = S1#st{waiting_readers = [W || {_, P} = W <- S1#st.waiting_readers, P =/= Pid],
               waiting_writers = [W || {_, P} = W <- S1#st.waiting_writers, P =/= Pid]},
    {noreply, dispatch(S2)};
handle_info(_M, S) -> {noreply, S}.

%%%===================================================================

%% 書き手が待っているなら、新しい読み手は後ろに並ぶ(書き手の飢え防止)。
can_read(#st{writer = undefined, waiting_writers = []}) -> true;
can_read(_) -> false.

can_write(#st{writer = undefined, readers = R}) -> map_size(R) =:= 0;
can_write(_) -> false.

grant_read(Pid, #st{readers = R} = S) ->
    Ref = erlang:monitor(process, Pid),
    S#st{readers = R#{Ref => Pid}}.

grant_write(Pid, S) ->
    S#st{writer = {Pid, erlang:monitor(process, Pid)}}.

release_reader(Pid, #st{readers = R} = S) ->
    case [Ref || {Ref, P} <- maps:to_list(R), P =:= Pid] of
        [Ref | _] ->
            true = erlang:demonitor(Ref, [flush]),
            S#st{readers = maps:remove(Ref, R)};
        [] ->
            S
    end.

release_writer(Pid, #st{writer = {Pid, Ref}} = S) ->
    true = erlang:demonitor(Ref, [flush]),
    S#st{writer = undefined};
release_writer(_Pid, S) ->
    S.

%% 空いたら待っているものへ渡す。書き手を先に通す。
dispatch(#st{waiting_writers = [{From, Pid} | Rest]} = S) ->
    case can_write(S) of
        true  -> gen_server:reply(From, ok),
                 dispatch(grant_write(Pid, S#st{waiting_writers = Rest}));
        false -> S
    end;
dispatch(#st{waiting_readers = [{From, Pid} | Rest]} = S) ->
    case can_read(S) of
        true  -> gen_server:reply(From, ok),
                 dispatch(grant_read(Pid, S#st{waiting_readers = Rest}));
        false -> S
    end;
dispatch(S) ->
    S.
