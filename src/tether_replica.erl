%%%-------------------------------------------------------------------
%%% @doc
%%% クライアント側の複製。**このモジュールは純粋である。**
%%%
%%% == サーバと同じ関数で状態を作る ==
%%%
%%% 手元の見え方は `tether_data:apply_batch/3` で計算する。
%%% **サーバが使うのとまったく同じ純粋関数**である。
%%% 別々に実装すると、必ずどこかで意味がずれる。ずれは
%%% 「サーバでは通ったのに手元では通らない」という形で、
%%% 一番デバッグしにくい場所に出る。
%%%
%%% == 3つの状態 ==
%%%
%%%   confirmed  サーバが認めた状態。sync で受け取った差分を重ねたもの
%%%   queued     まだ送っていない操作の束
%%%   view       confirmed に queued を重ねたもの。**アプリが見るのはこれ**
%%%
%%% == 楽観的なものと、確定的なもの ==
%%%
%%% 圏外での書き込みには2種類ある。
%%%
%%%   楽観的   put / cas / delete。サーバが拒むかもしれない。
%%%            見えているのは推測であって、後で覆りうる
%%%   確定的   預かりの範囲内の consume。**サーバは必ず受け入れる。**
%%%            権利を先に取ってあるので、覆らない
%%%
%%% この区別が local-first の質を決める。同期エンジンは全部を楽観的に
%%% 扱うので、圏外で見せた結果が後で覆る。預かりがあると、
%%% 覆らないと断言できる範囲が生まれる。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_replica).

-export([new/1, seq/1, version/1, client/1]).
-export([apply_local/3, view/2, get/3, pending/1, queued_count/1]).
-export([confirm/4, confirm_seq/2, reset/4, set_grant/4, grants/2]).

-export_type([replica/0, outcome/0]).

-record(rep, {
          client            :: binary(),
          version = 0       :: non_neg_integer(),
          seq     = 0       :: non_neg_integer(),   % サーバが実行した最後の通番
          confirmed         :: tether_data:db(),
          queued  = []      :: [[tether_data:op()]]
         }).

-type replica() :: #rep{}.

%% sync の結果。
%%   accepted  サーバが実行した束の数
%%   rejected  そこで落ちた理由(あれば)
%%   unsent    落ちた後ろで**実行されなかった**束。判断は呼び出し側に返す
-type outcome() :: #{accepted := non_neg_integer(),
                     rejected := none | {pos_integer(), term()},
                     unsent   := [[tether_data:op()]]}.

%%%===================================================================

-spec new(binary()) -> replica().
new(Client) -> #rep{client = Client, confirmed = tether_data:new()}.

client(#rep{client = C})   -> C.
seq(#rep{seq = S})         -> S.
version(#rep{version = V}) -> V.
queued_count(#rep{queued = Q}) -> length(Q).

%% @doc まだ送っていない束。sync のときにこれを丸ごと送る。
-spec pending(replica()) -> [[tether_data:op()]].
pending(#rep{queued = Q}) -> lists:reverse(Q).

%%----------------------------------------------------------------------
%% @doc 手元で実行する。**ネットワークを使わない。**
%%
%% 成功したら積んで、後で送る。失敗したら積まない —
%% 手元で判定できるものは、圏外でもその場で答えが出る。
%% 預かりの範囲を超えた consume がその例で、**サーバに聞くまでもなく
%% 断れる**。
%%----------------------------------------------------------------------
-spec apply_local([tether_data:op()], integer(), replica()) ->
          {tether_data:group_result(), replica()}.
apply_local(Ops, Now, #rep{queued = Q} = R) ->
    case tether_data:apply_batch([Ops], ctx(Now, R), view(Now, R)) of
        {[{ok, _} = Res], _, _} -> {Res, R#rep{queued = [Ops | Q]}};
        {[{error, _, _} = Err], _, _} -> {Err, R}
    end.

%%----------------------------------------------------------------------
%% @doc アプリが見るべき状態。confirmed に未送信の束を重ねたもの。
%%----------------------------------------------------------------------
-spec view(integer(), replica()) -> tether_data:db().
view(Now, #rep{confirmed = C} = R) ->
    {_, Db, _} = tether_data:apply_batch(pending(R), ctx(Now, R), C),
    Db.

%% @doc ローカル読み取り。ネットワークを使わない。
-spec get(tether_data:key(), integer(), replica()) ->
          {ok, tether_data:value()} | not_found.
get(Key, Now, R) -> tether_data:get(Key, view(Now, R)).

%%----------------------------------------------------------------------
%% @doc sync の返答を取り込む。
%%
%% 送った束のうち、どこまでが実行されたかを数える。落ちた位置から
%% 後ろは**サーバで実行されていない**ので、呼び出し側へ返す。
%% 勝手に捨てもしないし、勝手に送り直しもしない。
%% 前提が崩れた後の操作をどうするかは、業務の判断である。
%%----------------------------------------------------------------------
-spec confirm([tether_data:group_result()],
              {non_neg_integer(), [{tether_data:key(),
                                    tether_data:value() | deleted}]},
              integer(), replica()) -> {outcome(), replica()}.
confirm(Results, {Version, Changes}, Now, #rep{confirmed = C} = R) ->
    Sent = pending(R),
    {Accepted, Rejected} = split(Results),
    %% 通ったぶんは確定。差分がまだ届いていなくても、手元では確定させる。
    %% (配布は非同期なので、次の sync まで見えないことがある)
    {_, C1, _} = tether_data:apply_batch(lists:sublist(Sent, Accepted),
                                         ctx(Now, R), C),
    C2 = merge(Changes, C1),
    Unsent = case Rejected of
                 none    -> [];
                 {N, _}  -> lists:nthtail(min(N, length(Sent)), Sent)
             end,
    {#{accepted => Accepted, rejected => Rejected, unsent => Unsent},
     R#rep{confirmed = C2, version = Version, queued = [],
           seq = R#rep.seq + 1}}.

%% @doc 通番を外から合わせる。resume 直後と、送るものが無かった sync の後。
-spec confirm_seq(non_neg_integer(), replica()) -> replica().
confirm_seq(Seq, R) -> R#rep{seq = Seq}.

%%----------------------------------------------------------------------
%% @doc 差分で追いつけなかったときの作り直し。
%% 未送信の束は保持する。まだ送っていないのだから捨てる理由がない。
%%----------------------------------------------------------------------
-spec reset(non_neg_integer(), [{tether_data:key(), tether_data:value()}],
            integer(), replica()) -> replica().
reset(Version, Rows, _Now, #rep{confirmed = C} = R) ->
    Keep = maps:filter(fun(K, _) -> tether_data:is_internal(K) end, C),
    R#rep{confirmed = maps:merge(Keep, maps:from_list(Rows)), version = Version}.

%%----------------------------------------------------------------------
%% @doc サーバから受け取った預かりを手元に写す。
%%
%% **これがあるから圏外で確定的に答えられる。** 預かりの記録を
%% サーバと同じ鍵・同じ形で持つので、consume の判定に
%% サーバと同一の純粋関数がそのまま使える。
%%----------------------------------------------------------------------
-spec set_grant(binary(), non_neg_integer(), integer(), replica()) -> replica().
set_grant(Resource, Remaining, Expires, #rep{client = Cl, confirmed = C} = R) ->
    K = tether_escrow:grant_key(Cl, Resource),
    G = #{remaining => Remaining, taken => 0,
          granted_at => Expires, expires_at => Expires},
    R#rep{confirmed = C#{K => term_to_binary(G)}}.

%% @doc 手元が把握している預かり(未送信の消費を差し引いたもの)。
-spec grants(integer(), replica()) -> [{binary(), non_neg_integer()}].
grants(Now, #rep{client = Cl} = R) ->
    Db = view(Now, R),
    [{Res, N} || Res <- tether_escrow:holdings(Cl, Db),
                 {N, _E} <- [tether_escrow:grant_view(Cl, Res, Db)]].

%%%===================================================================

ctx(Now, #rep{client = C}) -> #{now => Now, client => C}.

split(Results) -> split(Results, 0).
split([], N)                    -> {N, none};
split([{ok, _} | T], N)         -> split(T, N + 1);
split([{error, I, R} | _], N)   -> {N, {N + 1, {I, R}}}.

merge(Changes, Db) ->
    lists:foldl(fun({K, deleted}, D) -> maps:remove(K, D);
                   ({K, V}, D)       -> D#{K => V}
                end, Db, Changes).
