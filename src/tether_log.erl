%%%-------------------------------------------------------------------
%%% @doc
%%% 追記ログ。ファイル記述子を所有する唯一のプロセス。
%%%
%%% == 責務 ==
%%%
%%% write/1 が返ったとき、そのバイト列は fsync 済みである。
%%% **この約束がデータベース全体の永続性の土台**であり、他のどこにも
%%% 永続性の主張は存在しない。データ本体はメモリにあり、
%%% 復旧はこのログの再実行で行う。
%%%
%%% == group commit ==
%%%
%%% fsync は1回あたりミリ秒の桁で、書き込み1件ごとに呼ぶと
%%% スループットが fsync の回数で頭打ちになる。そこで、待っている
%%% 書き込みをまとめて1回の fsync で片付ける。
%%%
%%% 実装は gen_server の timeout をそのまま使う。handle_call が
%%% timeout 0 を返すと、**メールボックスが空になったときだけ**
%%% handle_info(timeout, _) が届く。つまり待っている呼び出しが
%%% 全部溜まってから1回だけ吐き出される。混んでいるほど
%%% まとまりが大きくなる、という望ましい性質が自動的に出る。
%%%
%%% == 起動時の判断 ==
%%%
%%% 末尾が切れている(truncated)のは、電源断の後の**正常な状態**である。
%%% 有効な部分まで切り詰めて続行する。
%%%
%%% 途中が壊れている(corrupt)場合は**起動を拒否する**。
%%% 読めるところまで読んで続行すると、ディスクの故障が
%%% 「少しデータが古いだけ」に化けて表に出てこない。
%%% 直すには repair/1 を明示的に呼ぶ。黙って捨てない。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_log).
-behaviour(gen_server).

-export([start_link/1, write/1, write/2, commit/3, compact/1, stat/0, path/1]).
-export([fold/3, repair/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([lsn/0]).

%% レコードの先頭バイト位置。ログ内で一意に増える。
-type lsn() :: non_neg_integer().

%% 待っている書き込み。返す内容は2種類ある。
%%   lsn        … write/1 の呼び出し元へ {ok, Lsn} を返す
%%   {term, T}  … 別のプロセスが用意した答えを、永続化後にそのまま返す
-type pending() :: {binary(), gen_server:from(), lsn | {term, term()}}.

-define(LOGFILE, "tether.log").

-record(s, {
          fd      :: file:fd(),
          path    :: file:filename_all(),
          pos = 0 :: non_neg_integer(),
          pending = [] :: [pending()],   % 逆順
          writes  = 0 :: non_neg_integer(),
          syncs   = 0 :: non_neg_integer()
         }).

%%%===================================================================
%%% 公開
%%%===================================================================

-spec start_link(file:filename_all()) -> {ok, pid()} | {error, term()}.
start_link(Dir) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Dir, []).

%%----------------------------------------------------------------------
%% @doc 1件書いて永続化する。**返った時点で fsync 済み。**
%%----------------------------------------------------------------------
-spec write(binary()) -> {ok, lsn()} | {error, term()}.
write(Payload) -> write(Payload, 5000).

-spec write(binary(), timeout()) -> {ok, lsn()} | {error, term()}.
write(Payload, Timeout) when is_binary(Payload) ->
    gen_server:call(?MODULE, {write, Payload}, Timeout).

%%----------------------------------------------------------------------
%% @doc 永続化してから、用意された答えを From へ返す。
%%
%% **返答の権利を委譲する**ための口。これがあるので、状態を持つ
%% プロセス(tether_store)が fsync を待って詰まる必要がなくなる。
%% ストアは即座に次の要求へ移り、返答はログが永続化した後に
%% ここから直接送られる。
%%
%% これがないと、group commit があっても意味がない。
%% ストアが1件ずつ fsync を待つなら、まとまる相手がいない。
%%----------------------------------------------------------------------
-spec commit(binary(), gen_server:from(), term()) -> ok.
commit(Payload, From, Reply) when is_binary(Payload) ->
    gen_server:cast(?MODULE, {commit, Payload, From, Reply}).

%%----------------------------------------------------------------------
%% @doc 先頭 N 件を捨てる。**スナップショットを永続化した後にだけ呼ぶ。**
%%
%% 呼ぶのはストア自身であること。別のプロセスから呼ぶと、
%% ストアが送った追記(cast)とこの呼び出しの順序が保証されず、
%% まだ届いていない分を「先頭N件」に数えてしまう。
%%----------------------------------------------------------------------
-spec compact(non_neg_integer()) -> {ok, #{dropped := non_neg_integer(),
                                           kept := non_neg_integer()}}
                                        | {error, term()}.
compact(N) -> gen_server:call(?MODULE, {compact, N}, 60000).

%% @doc 書き込み件数と fsync 回数。まとまり具合を測るため。
-spec stat() -> #{writes := non_neg_integer(), syncs := non_neg_integer(),
                  bytes := non_neg_integer()}.
stat() -> gen_server:call(?MODULE, stat).

-spec path(file:filename_all()) -> file:filename_all().
path(Dir) -> filename:join(Dir, ?LOGFILE).

%%----------------------------------------------------------------------
%% @doc ログを先頭から畳む。復旧で使う。プロセスは要らない。
%%
%% 途中が壊れていれば {error, {corrupt, _, _}} を返す。
%% **握り潰さないこと。**
%%----------------------------------------------------------------------
-spec fold(file:filename_all(), fun((binary(), lsn(), Acc) -> Acc), Acc) ->
          {ok, Acc, #{records := non_neg_integer(), valid_bytes := non_neg_integer(),
                      tail := tether_rec:status()}}
              | {error, term()}.
fold(Dir, Fun, Acc0) ->
    case read_all(path(Dir)) of
        {error, enoent} ->
            {ok, Acc0, #{records => 0, valid_bytes => 0, tail => complete}};
        {error, R} ->
            {error, R};
        {ok, Bin} ->
            {Recs, Off, Status} = tether_rec:scan(Bin),
            case Status of
                {corrupt, Why} ->
                    {error, {corrupt, Why, Off}};
                _ ->
                    {Acc, _} = lists:foldl(
                                 fun(P, {A, L}) ->
                                         {Fun(P, L, A),
                                          L + tether_rec:header_size() + byte_size(P)}
                                 end, {Acc0, 0}, Recs),
                    {ok, Acc, #{records => length(Recs), valid_bytes => Off,
                                tail => Status}}
            end
    end.

%%----------------------------------------------------------------------
%% @doc 壊れたログを、有効な前半まで切り詰める。**破壊的。**
%% 起動が corrupt で拒否されたときに、人が判断して呼ぶもの。
%%----------------------------------------------------------------------
-spec repair(file:filename_all()) -> {ok, #{discarded := non_neg_integer()}} | {error, term()}.
repair(Dir) ->
    P = path(Dir),
    case read_all(P) of
        {ok, Bin} ->
            {_, Off, _} = tether_rec:scan(Bin),
            case file:open(P, [read, write, raw, binary]) of
                {ok, Fd} ->
                    {ok, _} = file:position(Fd, Off),
                    ok = file:truncate(Fd),
                    ok = file:sync(Fd),
                    ok = file:close(Fd),
                    {ok, #{discarded => byte_size(Bin) - Off}};
                E -> E
            end;
        E -> E
    end.

%%%===================================================================
%%% gen_server
%%%===================================================================

init(Dir) ->
    process_flag(trap_exit, true),
    P = path(Dir),
    ok = filelib:ensure_dir(P),
    case read_all(P) of
        {error, enoent} -> open_at(P, 0);
        {error, R}      -> {stop, {log_unreadable, R}};
        {ok, Bin} ->
            {_, Off, Status} = tether_rec:scan(Bin),
            case Status of
                complete ->
                    open_at(P, Off);
                truncated ->
                    %% 電源断の後の正常な状態。有効な部分まで戻して続ける。
                    logger:notice("tether_log: 末尾 ~p バイトが切れていたので捨てた",
                                  [byte_size(Bin) - Off]),
                    open_at(P, Off);
                {corrupt, Why} ->
                    %% 黙って続けない。読めるところまで読む実装は、
                    %% ディスクの故障をデータの欠落として素通しする。
                    {stop, {corrupt_log, Why, Off}}
            end
    end.

open_at(P, Off) ->
    case file:open(P, [read, write, raw, binary]) of
        {ok, Fd} ->
            {ok, _} = file:position(Fd, Off),
            ok = file:truncate(Fd),
            {ok, #s{fd = Fd, path = P, pos = Off}};
        {error, R} ->
            {stop, {log_unopenable, R}}
    end.

%% timeout 0 を返すのが要点。溜まってから1回だけ吐き出される。
handle_call({write, Payload}, From, #s{pending = Ps} = S) ->
    {noreply, S#s{pending = [{Payload, From, lsn} | Ps]}, 0};
handle_call({compact, N}, _From, S0) ->
    %% 待っている追記を先に片付ける。捨てる範囲を数え間違えないため。
    {noreply, S} = flush(S0),
    {Reply, S1} = do_compact(N, S),
    {reply, Reply, S1};
handle_call(stat, _From, #s{writes = W, syncs = Sy, pos = Pos} = S) ->
    {reply, #{writes => W, syncs => Sy, bytes => Pos}, S};
handle_call(_R, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast({commit, Payload, From, Reply}, #s{pending = Ps} = S) ->
    {noreply, S#s{pending = [{Payload, From, {term, Reply}} | Ps]}, 0};
handle_cast(_M, S) -> {noreply, S}.

handle_info(timeout, S) -> flush(S);
handle_info(_M, S)      -> {noreply, S}.

terminate(_R, #s{fd = Fd}) ->
    _ = file:sync(Fd),
    _ = file:close(Fd),
    ok.

%%%===================================================================
%%% 内部
%%%===================================================================

flush(#s{pending = []} = S) ->
    {noreply, S};
flush(#s{pending = Rev, fd = Fd, pos = Pos, writes = W, syncs = Sy} = S) ->
    Pending = lists:reverse(Rev),
    {IoData, Lsns, End} = build(Pending, Pos, [], []),
    case file:write(Fd, IoData) of
        ok ->
            case file:sync(Fd) of
                ok ->
                    reply_all(Pending, Lsns),
                    {noreply, S#s{pending = [], pos = End,
                                  writes = W + length(Pending), syncs = Sy + 1}};
                {error, R} -> fail(Pending, {sync_failed, R}, S)
            end;
        {error, R} ->
            fail(Pending, {write_failed, R}, S)
    end.

%% fsync が失敗したら、永続性を約束できない。**続けてはいけない。**
%% 待っている呼び出しに正直に返してから死ぬ。supervisor が作り直す。
fail(Pending, Reason, S) ->
    _ = [gen_server:reply(From, {error, Reason}) || {_, From, _} <- Pending],
    {stop, Reason, S#s{pending = []}}.

reply_all(Pending, Lsns) ->
    _ = [gen_server:reply(From, reply_for(Spec, Lsn))
         || {{_, From, Spec}, Lsn} <- lists:zip(Pending, Lsns)],
    ok.

reply_for(lsn, Lsn)      -> {ok, Lsn};
reply_for({term, T}, _)  -> T.

build([], Pos, IoAcc, LsnAcc) ->
    {lists:reverse(IoAcc), lists:reverse(LsnAcc), Pos};
build([{P, _From, _Spec} | T], Pos, IoAcc, LsnAcc) ->
    Rec = tether_rec:encode(P),
    build(T, Pos + byte_size(Rec), [Rec | IoAcc], [Pos | LsnAcc]).

%%----------------------------------------------------------------------
%% 先頭 N 件を落として書き直す。
%% 一時ファイルに書いて rename する。書きかけのログが
%% 正規のものとして見えてはいけない。
%%----------------------------------------------------------------------
do_compact(N, #s{path = P} = S) ->
    case read_all(P) of
        {error, R} ->
            {{error, R}, S};
        {ok, Bin} ->
            case tether_rec:scan(Bin) of
                {_, _, {corrupt, Why}} ->
                    {{error, {corrupt, Why}}, S};
                {Recs, _, _} when length(Recs) < N ->
                    {{error, {not_enough_records, length(Recs), N}}, S};
                {Recs, _, _} ->
                    replace_log(lists:nthtail(N, Recs), N, S)
            end
    end.

replace_log(Keep, Dropped, #s{fd = Fd, path = P} = S) ->
    New = tether_rec:encode_all(Keep),
    case atomic_replace(iolist_to_binary([P, ".tmp"]), P, New) of
        ok ->
            _ = file:close(Fd),
            case file:open(P, [read, write, raw, binary]) of
                {ok, Fd1} ->
                    {ok, _} = file:position(Fd1, eof),
                    {{ok, #{dropped => Dropped, kept => length(Keep)}},
                     S#s{fd = Fd1, pos = byte_size(New)}};
                E ->
                    %% 書き直しは成功したのに開き直せない。
                    %% ここで生き延びると、以後の追記が全部消える。
                    exit({log_reopen_failed, E})
            end;
        E ->
            {E, S}
    end.

atomic_replace(Tmp, Dst, Bin) ->
    case file:open(Tmp, [write, raw, binary]) of
        {ok, Fd} ->
            R = case file:write(Fd, Bin) of
                    ok -> file:sync(Fd);
                    E1 -> E1
                end,
            _ = file:close(Fd),
            case R of
                ok -> file:rename(Tmp, Dst);
                E2 -> E2
            end;
        E -> E
    end.

read_all(P) ->
    case file:read_file(P) of
        {ok, Bin} -> {ok, Bin};
        E -> E
    end.
