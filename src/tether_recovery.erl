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
             tail := tether_rec:status()}}
              | {error, term()}.
load(Dir) ->
    Init = {tether_data:new(), #{}},
    try tether_log:fold(Dir, fun replay/3, Init) of
        {ok, {Db, Sess}, Info} -> {ok, Db, Sess, Info};
        {error, R}             -> {error, R}
    catch
        throw:{recovery_failed, R} -> {error, R}
    end.

replay(Payload, Lsn, {Db, Sess}) ->
    case tether_entry:decode(Payload) of
        {error, R} ->
            throw({recovery_failed, {undecodable_entry, Lsn, R}});
        {ok, E} ->
            {Reply, Db1} = tether_data_apply(tether_entry:ops(E), Db),
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
            {Db1, Sess#{tether_entry:client(E) => {tether_entry:seq(E), Reply}}}
    end.

tether_data_apply(Ops, Db) ->
    case tether_data:apply_ops(Ops, Db) of
        {ok, Results, Db1}   -> {{ok, Results}, Db1};
        {error, N, R, Db1}   -> {{error, N, R}, Db1}
    end.
