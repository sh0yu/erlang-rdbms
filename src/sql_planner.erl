%%%-------------------------------------------------------------------
%%% @doc
%%% プランナ。論理プランを書き換えてから物理プランへ変換する。
%%%
%%% 2つの仕事があり、混ぜてはいけない。
%%%
%%%   rewrite/1   論理 → 論理。関係代数として等価な変形。
%%%               述語のプッシュダウンなど。**結果を変えない**
%%%   physical/2  論理 → 物理。実行方法の決定。
%%%               全表走査か索引スキャンか、どの結合アルゴリズムか
%%%
%%% 分けておくと、書き換えの正しさ(等価性)と実行方法の選択(コスト)を
%%% 別々に検証できる。混ぜると「速くなったが結果が変わった」の
%%% 切り分けができなくなる。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_planner).

-export([plan/1, rewrite/1, physical/1]).

-include("../include/logical.hrl").
-include("../include/plan.hrl").

%%----------------------------------------------------------------------
%% @doc 論理プランから物理プランを作る。
%%----------------------------------------------------------------------
plan(Logical) ->
    physical(rewrite(Logical)).

%%%===================================================================
%%% 論理 → 論理
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc 等価な書き換え。いまは述語のプッシュダウンだけ。
%%
%% 結合の**上**にある選択を、可能なら結合の**下**へ落とす。
%% 結合してから捨てるより、捨ててから結合する方が中間結果が小さい。
%%----------------------------------------------------------------------
rewrite(#lp_filter{pred = P, input = In}) ->
    push(conjuncts(P), In);
rewrite(#lp_join{} = J) ->
    push([], J);
rewrite(#lp_project{input = In} = N)  -> N#lp_project{input = rewrite(In)};
rewrite(#lp_sort{input = In} = N)     -> N#lp_sort{input = rewrite(In)};
rewrite(#lp_limit{input = In} = N)    -> N#lp_limit{input = rewrite(In)};
rewrite(#lp_distinct{input = In} = N) -> N#lp_distinct{input = rewrite(In)};
rewrite(#lp_agg{input = In} = N)      -> N#lp_agg{input = rewrite(In)};
rewrite(#lp_scan{} = N)               -> N.

%%%===================================================================
%%% 述語のプッシュダウン
%%%===================================================================

%%----------------------------------------------------------------------
%% Preds を Node の中へ押し込む。押し込めなかったものは選択として上に残す。
%%
%% == 外部結合で何が押し込めるか ==
%%
%% ここを間違えると結果が変わる。内部結合なら ON と WHERE は等価で
%% どちらの側にも落とせるが、**LEFT JOIN では非対称になる**。
%%
%%   A LEFT JOIN B ON A.x = B.x WHERE B.y > 5
%%     落とさない: 結合で A の全行が出る(B側はNULL埋めもあり)。
%%                 その後 B.y > 5 が NULL の行を落とす → 実質は内部結合
%%     Bへ落とす:  先に B を絞ってから左結合するので、
%%                 一致しない A の行が**NULL埋めで残る** → 別の結果
%%     ⇒ 押し込めない
%%
%%   A LEFT JOIN B ON A.x = B.x AND B.y > 5
%%     ON は「何を一致とみなすか」なので、先に B を絞っても一致集合は同じ。
%%     一致しない A の行がNULL埋めされるのも同じ
%%     ⇒ 押し込める
%%
%%   A LEFT JOIN B ON A.z = 1
%%     A側のON条件。A.z≠1 の行は「一致無し」としてNULL埋めで残るべき。
%%     Aへ落とすと消えてしまう
%%     ⇒ 押し込めない
%%
%% 表にすると:
%%
%%              WHEREの条件      ONの条件
%%              左側  右側       左側  右側
%%   inner/cross ○    ○          ○    ○
%%   left        ○    ×          ×    ○
%%----------------------------------------------------------------------
push(Preds, #lp_join{type = Ty, pred = On, left = L, right = R} = J) ->
    LW = width(L),
    {OnL, OnR, OnKeep} = classify(conjuncts(On), LW, on, Ty),
    {WhL, WhR, WhKeep} = classify(Preds, LW, where, Ty),
    %% 右側へ落とすときは位置を左の幅だけ戻す。
    %% 結合後の行では右のカラムが LW だけずれているため。
    J1 = J#lp_join{pred = conj(OnKeep),
                   left  = push(OnL ++ WhL, L),
                   right = push([shift(E, -LW) || E <- OnR ++ WhR], R)},
    wrap(conj(WhKeep), J1);
push(Preds, #lp_filter{pred = P, input = In}) ->
    %% 選択が重なっていたら1つにまとめて、まとめて押し込む
    push(Preds ++ conjuncts(P), In);
push(Preds, #lp_scan{} = S) ->
    wrap(conj(Preds), S);
push(Preds, Other) ->
    wrap(conj(Preds), rewrite(Other)).

%% 各条件を「左へ落とす / 右へ落とす / 上に残す」に振り分ける。
classify(Conjs, LW, Kind, Ty) ->
    lists:foldr(
      fun(E, {AccL, AccR, AccK}) ->
              case side(E, LW) of
                  left  when Ty =/= left; Kind =:= where -> {[E | AccL], AccR, AccK};
                  right when Ty =/= left; Kind =:= on    -> {AccL, [E | AccR], AccK};
                  _ -> {AccL, AccR, [E | AccK]}
              end
      end, {[], [], []}, Conjs).

%% その条件がどちら側のカラムだけを見ているか。
%% 参照が読めない式は both 扱いにする(押し込まない)。安全側に倒す。
side(E, LW) ->
    case refs(E) of
        unknown -> both;
        []      -> both;                       % 定数条件は動かさない
        Ps ->
            case {lists:all(fun(P) -> P =< LW end, Ps),
                  lists:all(fun(P) -> P > LW end, Ps)} of
                {true, _}      -> left;
                {_, true}      -> right;
                _              -> both
            end
    end.

%% 式が参照する位置。知らない形が出たら unknown を返す。
%% ここで [] を返すと「何も参照していない」と誤って押し込んでしまう。
refs({ref, P})            -> [P];
refs({const, _})          -> [];
refs({comp, _, L, R})     -> merge(refs(L), refs(R));
refs({arith, _, L, R})    -> merge(refs(L), refs(R));
refs({'and', Es})         -> lists:foldl(fun(E, A) -> merge(refs(E), A) end, [], Es);
refs({'or', Es})          -> lists:foldl(fun(E, A) -> merge(refs(E), A) end, [], Es);
refs({'not', E})          -> refs(E);
refs({neg, E})            -> refs(E);
refs({is_null, E})        -> refs(E);
refs({is_not_null, E})    -> refs(E);
refs(_)                   -> unknown.

merge(unknown, _) -> unknown;
merge(_, unknown) -> unknown;
merge(A, B)       -> A ++ B.

%% 位置をずらす。右側へ落とすときに使う。
shift({ref, P}, D)         -> {ref, P + D};
shift({const, _} = E, _D)  -> E;
shift({comp, Op, L, R}, D) -> {comp, Op, shift(L, D), shift(R, D)};
shift({arith, Op, L, R}, D)-> {arith, Op, shift(L, D), shift(R, D)};
shift({'and', Es}, D)      -> {'and', [shift(E, D) || E <- Es]};
shift({'or', Es}, D)       -> {'or', [shift(E, D) || E <- Es]};
shift({'not', E}, D)       -> {'not', shift(E, D)};
shift({neg, E}, D)         -> {neg, shift(E, D)};
shift({is_null, E}, D)     -> {is_null, shift(E, D)};
shift({is_not_null, E}, D) -> {is_not_null, shift(E, D)}.

%% その部分木が出す行の幅。FROM句の中には走査・結合・選択しか現れない。
width(#lp_scan{schema = S})          -> length(S);
width(#lp_filter{input = In})        -> width(In);
width(#lp_join{left = L, right = R}) -> width(L) + width(R).

%% AND を平らにする / 組み直す
conjuncts(undefined)   -> [];
conjuncts({'and', Es}) -> lists:append([conjuncts(E) || E <- Es]);
conjuncts(E)           -> [E].

conj([])  -> undefined;
conj([E]) -> E;
conj(Es)  -> {'and', Es}.

wrap(undefined, Node) -> Node;
wrap(Pred, Node)      -> #lp_filter{pred = Pred, input = Node}.

%%%===================================================================
%%% 論理 → 物理
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc 実行方法を決める。
%%
%% いまは1対1の対応。走査は全表走査、結合は入れ子ループ。
%% Stage 7-5 でここに索引スキャンの選択が入る。
%%----------------------------------------------------------------------
physical(#lp_scan{table = T, schema = S}) ->
    #p_seq_scan{table = T, schema = S};
physical(#lp_filter{pred = P, input = In}) ->
    #p_filter{pred = P, input = physical(In)};
physical(#lp_join{type = Ty, pred = P, left = L, right = R, right_width = W}) ->
    #p_nl_join{type = Ty, pred = P, left = physical(L), right = physical(R),
               right_width = W};
physical(#lp_agg{group_by = G, aggs = A, having = H, input = In}) ->
    #p_agg{group_by = G, aggs = A, having = H, input = physical(In)};
physical(#lp_sort{keys = K, limit = L, input = In}) ->
    #p_sort{keys = K, limit = L, input = physical(In)};
physical(#lp_limit{count = C, offset = O, input = In}) ->
    #p_limit{count = C, offset = O, input = physical(In)};
physical(#lp_distinct{input = In}) ->
    #p_distinct{input = physical(In)};
physical(#lp_project{exprs = E, names = N, input = In}) ->
    #p_project{exprs = E, names = N, input = physical(In)}.
