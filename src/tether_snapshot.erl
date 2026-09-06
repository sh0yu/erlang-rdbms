%%%-------------------------------------------------------------------
%%% @doc
%%% スナップショット。ログを切り詰めるために要る。
%%%
%%% ログだけで復旧する設計は正しいが、そのままだとログが無限に伸びる。
%%% 復旧時間も伸び続けるので、どこかで「ここまでの結果」を書き出して
%%% 前半を捨てなければならない。
%%%
%%% == 順序が全て ==
%%%
%%%   1. ある時点の状態と、それが何件目までを反映しているかを取る
%%%   2. **スナップショットを永続化する**(一時ファイル→fsync→rename)
%%%   3. **その後で**ログの前半を捨てる
%%%
%%% 逆にすると、2と3の間で電源が落ちたときにデータが消える。
%%% 2 の後 3 の前で落ちるのは安全で、そのときログには
%%% スナップショットに含まれる分も残っている。だから復旧側は
%%% **先頭 N 件を読み飛ばす**ようになっている。
%%%
%%% rename を使うのは、書きかけのファイルが正規のものとして
%%% 見えないようにするため。rename は同一ファイルシステム上で
%%% 原子的である。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_snapshot).

-export([write/4, read/1, path/1, covered/1]).

-define(SNAP, "snapshot").
-define(TMP,  "snapshot.tmp").

-spec path(file:filename_all()) -> file:filename_all().
path(Dir) -> filename:join(Dir, ?SNAP).

%%----------------------------------------------------------------------
%% @doc 状態を書き出す。Entries は、この状態が反映しているログの件数。
%%----------------------------------------------------------------------
-spec write(file:filename_all(), non_neg_integer(),
            tether_data:db(), tether_recovery:sessions()) -> ok | {error, term()}.
write(Dir, Entries, Db, Sessions) ->
    Tmp = filename:join(Dir, ?TMP),
    Bin = tether_rec:encode(term_to_binary({Entries, Db, Sessions})),
    case file:open(Tmp, [write, raw, binary]) of
        {ok, Fd} ->
            R = write_sync(Fd, Bin),
            _ = file:close(Fd),
            case R of
                ok -> file:rename(Tmp, path(Dir));
                E  -> E
            end;
        E -> E
    end.

write_sync(Fd, Bin) ->
    case file:write(Fd, Bin) of
        ok -> file:sync(Fd);
        E  -> E
    end.

%%----------------------------------------------------------------------
%% @doc 読み込む。壊れていたら**黙って無視せず**エラーを返す。
%% 無視すると、切り詰め済みのログしか無い状態で「空のDB」として
%% 起動してしまう。それは全件消失である。
%%----------------------------------------------------------------------
-spec read(file:filename_all()) ->
          {ok, non_neg_integer(), tether_data:db(), tether_recovery:sessions()}
              | none | {error, term()}.
read(Dir) ->
    case file:read_file(path(Dir)) of
        {error, enoent} -> none;
        {error, R}      -> {error, R};
        {ok, Bin} ->
            case tether_rec:scan(Bin) of
                {[Payload], _, complete} ->
                    try binary_to_term(Payload, [safe]) of
                        {Entries, Db, Sessions} when is_integer(Entries),
                                                     is_map(Db), is_map(Sessions) ->
                            {ok, Entries, Db, Sessions};
                        Other -> {error, {bad_snapshot, Other}}
                    catch _:E -> {error, {undecodable_snapshot, E}}
                    end;
                {_, _, Status} ->
                    {error, {bad_snapshot, Status}}
            end
    end.

-spec covered(file:filename_all()) -> non_neg_integer().
covered(Dir) ->
    case read(Dir) of
        {ok, N, _, _} -> N;
        _             -> 0
    end.
