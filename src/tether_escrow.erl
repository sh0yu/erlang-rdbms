%%%-------------------------------------------------------------------
%%% @doc
%%% 預かり(escrow)。**このモジュールは純粋である。**
%%%
%%% == 何のためにあるか ==
%%%
%%% 「在庫 ≥ 0」のような数値の下限は、協調なしには守れない。
%%% 2人が同時に最後の1個を売れば、どちらかが間違う。
%%%
%%% しかし**協調が要るのは在庫が尽きるときだけ**である。潤沢なうちは、
%%% 中央から持ち分を配ってしまえば、保持者は問い合わせずに売れる。
%%% 配った総和が在庫を超えないので、下限は自動的に守られる。
%%%
%%%     在庫 1000 → alice に 5、bob に 5、中央に 990
%%%     alice は**圏外でも5個売れる**。協調ゼロ。合計は絶対に 1000 を超えない
%%%
%%% これが local-first の要点である。同期エンジン(ElectricSQL, PowerSync)も
%%% CRDT も、オフラインの書き込みは受け付けるが、**共有された有限資源の
%%% 下限は守れない**。CRDTカウンタで「10から1引く」と「10から7引く」を
%%% 統合すれば 2 になり、在庫が3しか無くても8個売れてしまう。
%%% 預かりなら、bob は 5 しか持っていないので 7 は**そもそも売れない**。
%%%
%%% == 配る量は自動で決める ==
%%%
%%% tcmalloc のスレッド局所キャッシュ、TCP の輻輳窓と同じ形。
%%% 消費率を測り、期限までに使う量を見積もり、中央の残量で頭を打つ。
%%%
%%%     在庫が潤沢 → 大きく配る → 協調がほぼ消える
%%%     在庫が僅か → 配る量が 1 に落ちる → **古典のロックと同じ振る舞いへ
%%%                  連続的に退化する**
%%%
%%% 崖が無く、設定も要らない。そして**壊れる設定が存在しない**:
%%% 配りすぎれば在庫が遊ぶだけ、配らなすぎれば協調が増えるだけで、
%%% 下限は配った総和の上限で守られているので設定値に依存しない。
%%%
%%% == 回収 ==
%%%
%%% 預かったまま戻らない保持者がいると、在庫が永久に遊ぶ。だから期限を付ける。
%%% 期限切れの回収は**中央が足りなくなったときにだけ**行う(遅延回収)。
%%% 掃除する常駐プロセスを持たずに済む。
%%% @end
%%%-------------------------------------------------------------------
-module(tether_escrow).

-export([apply/3, is_escrow_op/1, validate/1]).
-export([pool_key/1, grant_key/2, decode_pool/1, decode_grant/1]).
-export([holdings/2, grant_view/3]).

-define(POOL,  <<"$pool">>).
-define(GRANT, <<"$grant">>).
%% クライアント → 預かっている資源の一覧。resume で引くための索引。
%% 無いと「このクライアントの預かりを全部出せ」が全鍵の走査になる。
-define(HELD,  <<"$held">>).

%% 期限までに使い切る量の見積もりに掛ける安全率。
%% 大きいほど協調が減り、在庫が遊ぶ。
-define(SAFETY, 3).       % ÷2 して 1.5 倍
%% **初回に配る量。** 実績が無いうちは小さく配り、使い切るたびに倍にする
%% (TCP の slow start と同じ)。要求された量をそのまま渡すと、
%% 少ししか使わずに消えるクライアントが在庫を大量に抱えたまま
%% いなくなる。実測で遊休 75%% を出したので入れた。
-define(INITIAL, 4).
%% 1保持者が一度に持てるのは、中央の残りを保持者数で割った半分まで。
%% 少数の保持者が在庫を独占するのを防ぐ。
-define(SHARE_DIV, 2).

-type pool()  :: {Available :: non_neg_integer(), Holders :: [binary()]}.
-type grant() :: #{remaining := non_neg_integer(), taken := non_neg_integer(),
                   granted_at := integer(), expires_at := integer()}.

-export_type([pool/0, grant/0]).

%%%===================================================================
%%% 鍵
%%%===================================================================

-spec pool_key(binary()) -> tether_data:key().
pool_key(Res) -> {?POOL, Res}.

%% クライアントと資源で1つ。NUL は鍵の中に現れない前提の区切り。
-spec grant_key(binary(), binary()) -> tether_data:key().
grant_key(Client, Res) -> {?GRANT, <<Client/binary, 0, Res/binary>>}.

held_key(Client) -> {?HELD, Client}.

%% @doc このクライアントが預かっている資源の一覧。
-spec holdings(binary(), tether_data:db()) -> [binary()].
holdings(Client, Db) ->
    case maps:find(held_key(Client), Db) of
        {ok, Bin} -> binary_to_term(Bin, [safe]);
        error     -> []
    end.

%% @doc 預かりの中身を外から見る形にする。期限切れは none。
-spec grant_view(binary(), binary(), tether_data:db()) ->
          {non_neg_integer(), integer()} | none.
grant_view(Client, Res, Db) ->
    case grant(Client, Res, Db) of
        undefined -> none;
        G         -> {maps:get(remaining, G), maps:get(expires_at, G)}
    end.

%%%===================================================================
%%% 判定と検査
%%%===================================================================

is_escrow_op({stock, _, _})      -> true;
is_escrow_op({acquire, _, _, _}) -> true;
is_escrow_op({consume, _, _})    -> true;
is_escrow_op({release, _})       -> true;
is_escrow_op({grant_of, _})      -> true;
is_escrow_op({pool_of, _})       -> true;
is_escrow_op(_)                  -> false.

validate({stock, R, N}) when is_binary(R), is_integer(N) -> ok;
validate({acquire, R, W, T}) when is_binary(R), is_integer(W), W > 0,
                                  is_integer(T), T > 0 -> ok;
validate({consume, R, N}) when is_binary(R), is_integer(N), N > 0 -> ok;
validate({release, R})    when is_binary(R) -> ok;
validate({grant_of, R})   when is_binary(R) -> ok;
validate({pool_of, R})    when is_binary(R) -> ok;
validate(Op) -> {error, {bad_escrow_op, Op}}.

%%%===================================================================
%%% 実行
%%%===================================================================

-spec apply(tether_data:op(), tether_data:ctx(), tether_data:db()) ->
          {ok, tether_data:result(), tether_data:db()}
              | {error, tether_data:result()}.

%% 中央在庫を増減する(管理操作)
apply({stock, Res, N}, _Ctx, Db) ->
    {Avail, Holders} = pool(Res, Db),
    case Avail + N of
        A when A < 0 -> {error, {insufficient, Avail}};
        A            -> {ok, {pool, A, granted(Res, Db)}, put_pool(Res, {A, Holders}, Db)}
    end;

%% 自分の預かりを補充する
apply({acquire, Res, Want, Ttl}, #{now := Now, client := C}, Db) ->
    G0 = grant(C, Res, Db),
    %% 期限切れなら、まず自分の残りを中央へ返してから取り直す
    Db1 = case expired(G0, Now) of
              true  -> return_grant(C, Res, G0, Db);
              false -> Db
          end,
    G  = case expired(G0, Now) of true -> undefined; false -> G0 end,
    Need = size_request(G, Want, Ttl, Now),
    case take_from_pool(Res, C, Need, Now, Db1) of
        {0, _}      -> {error, sold_out};
        {Got, Db2}  ->
            Rem = case G of undefined -> 0; _ -> maps:get(remaining, G) end,
            New = #{remaining => Rem + Got, taken => 0,
                    granted_at => Now, expires_at => Now + Ttl},
            {ok, {granted, Rem + Got, Now + Ttl},
             put_grant(C, Res, New, Db2)}
    end;

%% 自分の預かりから引く。**ここは中央に一切触れない。オフラインでも成立する。**
apply({consume, Res, N}, #{now := Now, client := C}, Db) ->
    case grant(C, Res, Db) of
        undefined ->
            {error, {insufficient, 0}};
        G ->
            case expired(G, Now) of
                true  -> {error, expired};
                false ->
                    case maps:get(remaining, G) of
                        R when R < N -> {error, {insufficient, R}};
                        R ->
                            G1 = G#{remaining := R - N,
                                    taken := maps:get(taken, G) + N},
                            {ok, {consumed, R - N}, put_grant(C, Res, G1, Db)}
                    end
            end
    end;

%% 未使用分を中央へ返す
apply({release, Res}, #{client := C}, Db) ->
    case grant(C, Res, Db) of
        undefined -> {ok, {released, 0}, Db};
        G         -> {ok, {released, maps:get(remaining, G)},
                      return_grant(C, Res, G, Db)}
    end;

apply({grant_of, Res}, #{now := Now, client := C}, Db) ->
    case grant(C, Res, Db) of
        undefined -> {ok, none, Db};
        G ->
            case expired(G, Now) of
                true  -> {ok, none, Db};
                false -> {ok, {grant, maps:get(remaining, G),
                               maps:get(expires_at, G)}, Db}
            end
    end;

apply({pool_of, Res}, _Ctx, Db) ->
    {Avail, _} = pool(Res, Db),
    {ok, {pool, Avail, granted(Res, Db)}, Db}.

%%%===================================================================
%%% 配る量を決める
%%%===================================================================

%%----------------------------------------------------------------------
%% 配る量を決める。TCP の輻輳窓と同じ形。
%%
%%   初回          小さく配る(?INITIAL)。実績が無いので当てられない
%%   使い切った    倍にする(slow start)。この保持者は本当に要る
%%   余らせた      消費率からの見積もりに従う(乗算的減少に相当)
%%
%% 要求された量をそのまま渡すと、少ししか使わずに消えるクライアントが
%% 在庫を抱えたままいなくなる。実測で遊休 75%% になった。
%% 小さく始めて必要な分だけ伸ばすと、消える側の被害が上限で抑えられる。
%%----------------------------------------------------------------------
size_request(undefined, Want, _Ttl, _Now) ->
    min(Want, ?INITIAL);
size_request(G, Want, Ttl, Now) ->
    Prev    = maps:get(taken, G) + maps:get(remaining, G),   % 前回配られた量
    Target =
        case maps:get(remaining, G) of
            0 ->
                %% 使い切った。倍に伸ばす。
                max(?INITIAL, Prev * 2);
            _ ->
                %% 余らせた。消費率から見積もる。
                Elapsed = max(1, Now - maps:get(granted_at, G)),
                (maps:get(taken, G) * Ttl * ?SAFETY) div (Elapsed * 2)
        end,
    max(1, min(Want, Target)).

%%----------------------------------------------------------------------
%% 中央から取る。足りなければ期限切れを回収してから取り直す。
%%
%% **在庫が減ると配れる量が自動的に減り、最後は 1 になる。**
%% そこが古典のロックと同じ振る舞いで、退化に崖が無い。
%%----------------------------------------------------------------------
take_from_pool(Res, Client, Need, Now, Db) ->
    {Avail0, Holders0} = pool(Res, Db),
    {Avail, Holders, Db1} =
        case Avail0 < Need of
            true  -> reclaim(Res, Now, Db);
            false -> {Avail0, Holders0, Db}
        end,
    Active = max(1, length(Holders)),
    Share  = max(1, Avail div Active div ?SHARE_DIV),
    Got    = min(min(Need, Share), Avail),
    case Got of
        0 -> {0, Db1};
        _ ->
            H = case lists:member(Client, Holders) of
                    true  -> Holders;
                    false -> [Client | Holders]
                end,
            {Got, put_pool(Res, {Avail - Got, H}, Db1)}
    end.

%%----------------------------------------------------------------------
%% 遅延回収。**中央が足りなくなったときにだけ**期限切れを回収する。
%% 掃除の常駐プロセスを持たずに済む。
%%
%% 保持者の一覧を順に見るので O(保持者数)。保持者が非常に多い資源では
%% 期限順の索引が要る。いまは持っていない(DESIGN.md に記載)。
%%----------------------------------------------------------------------
reclaim(Res, Now, Db) ->
    {Avail, Holders} = pool(Res, Db),
    lists:foldl(
      fun(C, {A, Hs, D}) ->
              case grant(C, Res, D) of
                  undefined -> {A, Hs, D};
                  G ->
                      case expired(G, Now) of
                          false -> {A, [C | Hs], D};
                          true  -> {A + maps:get(remaining, G), Hs,
                                    drop_grant(C, Res, D)}
                      end
              end
      end, {Avail, [], Db}, Holders).

%%%===================================================================
%%% 内部
%%%===================================================================

expired(undefined, _Now) -> true;
expired(G, Now)          -> maps:get(expires_at, G) =< Now.

return_grant(_C, _Res, undefined, Db) -> Db;
return_grant(C, Res, G, Db) ->
    {Avail, Holders} = pool(Res, Db),
    Db1 = put_pool(Res, {Avail + maps:get(remaining, G),
                         lists:delete(C, Holders)}, Db),
    drop_grant(C, Res, Db1).

pool(Res, Db) ->
    case maps:find(pool_key(Res), Db) of
        {ok, Bin} -> decode_pool(Bin);
        error     -> {0, []}
    end.

granted(Res, Db) ->
    {_, Holders} = pool(Res, Db),
    lists:sum([case grant(C, Res, Db) of
                   undefined -> 0;
                   G -> maps:get(remaining, G)
               end || C <- Holders]).

grant(C, Res, Db) ->
    case maps:find(grant_key(C, Res), Db) of
        {ok, Bin} -> decode_grant(Bin);
        error     -> undefined
    end.

put_pool(Res, P, Db)     -> Db#{pool_key(Res) => term_to_binary(P)}.

put_grant(C, Res, G, Db) ->
    Held = holdings(C, Db),
    Db1  = case lists:member(Res, Held) of
               true  -> Db;
               false -> Db#{held_key(C) => term_to_binary([Res | Held])}
           end,
    Db1#{grant_key(C, Res) => term_to_binary(G)}.

drop_grant(C, Res, Db) ->
    Held = lists:delete(Res, holdings(C, Db)),
    Db1  = case Held of
               [] -> maps:remove(held_key(C), Db);
               _  -> Db#{held_key(C) => term_to_binary(Held)}
           end,
    maps:remove(grant_key(C, Res), Db1).

decode_pool(Bin)  -> binary_to_term(Bin, [safe]).
decode_grant(Bin) -> binary_to_term(Bin, [safe]).

