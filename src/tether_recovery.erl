%%%-------------------------------------------------------------------
%%% @doc
%%% ログからの復旧。
%%%
%%% データ本体はメモリにしかない。永続化されているのはログだけである。
%%% だから復旧は「ログを頭から実行し直す」だけになる。ページの状態を
%%% 復元する必要がないので、ARIES のような仕掛けは要らない。
%%%
%%% 同時に**セッションの状態も再構築する**。クライアントごとの
%%% 「最後に実行した通番」と「そのとき返した答え」は、ログに
%%% 記録されている。だから再起動しても、再送に対して初回と同じ
%%% 答えを返せる。
%%%
%%% == 再実行の一致検査 ==
%%%
%%% 再実行して得た答えが、記録されている答えと食い違ったら、その場で
%%% 止める。食い違うということは、操作が決定的でないか、ログが
%%% 壊れているかのどちらかで、どちらも黙って進んではいけない。
%%% この検査は安いので常に行う。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_recovery).

-export([load/1]).

-export_type([sessions/0]).

%% クライアント → {最後に実行した通番, そのとき返した答え}
-type sessions() :: #{binary() => {non_neg_integer(), term()}}.

-spec load(file:filename_all()) ->
          {ok, tether_data:db(), sessions(),
           #{records := non_neg_integer(), valid_bytes := non_neg_integer(),
             tail := tether_rec:status(), applied := non_neg_integer(),
             skipped := non_neg_integer(), upto := non_neg_integer()}}
              | {error, term()}.
load(Dir) ->
    case tether_snapshot:read(Dir) of
        {error, R} ->
            %% 壊れたスナップショットを無視して先へ進むと、
            %% 切り詰め済みのログしか無い状態で空のDBとして
            %% 起動してしまう。それは全件消失である。
            {error, {bad_snapshot, R}};
        none ->
            load_from(Dir, 0, tether_data:new(), #{});
        {ok, N, Db, Sess} ->
            load_from(Dir, N, Db, Sess)
    end.

%% Upto は、スナップショットが反映している最後の通し番号。
%% スナップショットを書いた後、ログを切り詰める前に電源が落ちると、
%% 両方に同じ分が入るので、番号で読み飛ばす。
%%
%% **件数で数えてはいけない。** 切り詰めが起きるとログの先頭が
%% ずれるので、「先頭 N 件を飛ばす」は別のレコードを飛ばすことになる。
load_from(Dir, Upto, Db0, Sess0) ->
    Init = {Upto, Db0, Sess0, 0},
    try tether_log:fold(Dir, fun replay/3, Init) of
        {ok, {_, Db, Sess, Applied}, Info} ->
            {ok, Db, Sess,
             Info#{applied => Applied,
                   skipped => maps:get(records, Info) - Applied,
                   upto => Upto}};
        {error, R} ->
            {error, R}
    catch
        throw:{recovery_failed, R} -> {error, R}
    end.

replay(Payload, Lsn, {Upto, Db, Sess, Applied}) ->
    case tether_entry:decode(Payload) of
        {error, R} ->
            throw({recovery_failed, {undecodable_entry, Lsn, R}});
        {ok, E} ->
            case tether_entry:index(E) =< Upto of
                true  -> {Upto, Db, Sess, Applied};      % スナップショット済み
                false ->
                    {Db1, Sess1} = apply_entry(E, Lsn, {Db, Sess}),
                    {Upto, Db1, Sess1, Applied + 1}
            end
    end.

apply_entry(E, Lsn, {Db, Sess}) ->
    %% **記録された時刻とクライアントで再実行する。**
    %% ここで時計を読み直すと、期限切れの判定が本番と変わり、
    %% 直後の乖離検査で落ちる(落ちるだけましだが、そもそも読まない)。
    Ctx = #{now => tether_entry:time(E), client => tether_entry:client(E)},
    {Reply, Db1} = tether_data_apply(tether_entry:ops(E), Ctx, Db),
    case Reply =:= tether_entry:reply(E) of
        true -> ok;
        false ->
            %% 決定的なはずのものが決定的でなかった。
            %% ここで止めないと、復旧後の状態が本番と違う
            %% データベースが黙って動き出す。
            throw({recovery_failed,
                   {replay_divergence, Lsn,
                    #{logged => tether_entry:reply(E), replayed => Reply}}})
    end,
    {Db1, Sess#{tether_entry:client(E) => {tether_entry:seq(E), Reply}}}.

tether_data_apply(Ops, Ctx, Db) ->
    case tether_data:apply_ops(Ops, Ctx, Db) of
        {ok, Results, Db1}   -> {{ok, Results}, Db1};
        {error, N, R, Db1}   -> {{error, N, R}, Db1}
    end.
