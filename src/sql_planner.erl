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

-export([plan/1, plan/2, rewrite/1, physical/1, physical/2, no_catalog/0]).

-export_type([catalog/0]).

-include("../include/logical.hrl").
-include("../include/plan.hrl").
-include("../include/catalog.hrl").

%%----------------------------------------------------------------------
%% テーブルについて、実行方法を決めるのに要る情報を返す関数。
%%
%% プランナがカタログを直接引かずに関数で受け取るのは、
%%   - 純粋に保てる(同じ入力から同じプランが出る)
%%   - 試験で偽のカタログを渡して、索引がある場合と無い場合を書き分けられる
%% ため。
%%----------------------------------------------------------------------
-type catalog() :: fun((atom()) -> #{indexed := [atom()],
                                     stats := #table_stats{} | none}).

%%----------------------------------------------------------------------
%% @doc 論理プランから物理プランを作る。
%%----------------------------------------------------------------------
plan(Logical) -> plan(Logical, no_catalog()).

plan(Logical, Catalog) -> physical(rewrite(Logical), Catalog).

%% 索引も統計も無いものとして扱うカタログ。
%% 書き換えだけを試したいときや、カタログを引けない文脈で使う。
-spec no_catalog() -> catalog().
no_catalog() -> fun(_Table) -> #{indexed => [], stats => none} end.

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
rewrite(#lp_derived{input = In} = N) ->
    N#lp_derived{input = rewrite(In)};
rewrite(#lp_setop{left = L, right = R} = N) ->
    N#lp_setop{left = rewrite(L), right = rewrite(R)};
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
    %% 内部結合では ON と WHERE は等価なので、両側にまたがる条件は
    %% 結合の ON にまとめる。選択の節点が1つ減る。
    %% **LEFT JOIN ではまとめられない**(意味が変わる)。
    {Keep, Above} = case Ty of
                        left -> {OnKeep, WhKeep};
                        _    -> {OnKeep ++ WhKeep, []}
                    end,
    J1 = J#lp_join{pred = conj(Keep),
                   left  = push(OnL ++ WhL, L),
                   right = push([shift(E, -LW) || E <- OnR ++ WhR], R)},
    wrap(conj(Above), J1);
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
refs({func, _, Args})     -> lists:foldl(fun(E, A) -> merge(refs(E), A) end, [], Args);
refs({like, A, P})        -> merge(refs(A), refs(P));
refs({in, A, Es})         -> lists:foldl(fun(E, Acc) -> merge(refs(E), Acc) end,
                                         refs(A), Es);
refs({'case', Ws, E})     ->
    Base = case E of undefined -> []; _ -> refs(E) end,
    lists:foldl(fun({C, V}, Acc) -> merge(refs(C), merge(refs(V), Acc)) end, Base, Ws);
%% 副問い合わせを含む式は動かさない。中のプランの位置まで
%% 面倒を見る必要が出るので、安全側に倒す。
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
shift({is_not_null, E}, D) -> {is_not_null, shift(E, D)};
shift({func, N, Args}, D)  -> {func, N, [shift(E, D) || E <- Args]};
shift({like, A, P}, D)     -> {like, shift(A, D), shift(P, D)};
shift({in, A, Es}, D)      -> {in, shift(A, D), [shift(E, D) || E <- Es]};
shift({'case', Ws, E}, D)  ->
    {'case', [{shift(C, D), shift(V, D)} || {C, V} <- Ws],
     case E of undefined -> undefined; _ -> shift(E, D) end}.

%% その部分木が出す行の幅。FROM句の中には走査・結合・選択しか現れない。
width(#lp_scan{schema = S})          -> length(S);
width(#lp_filter{input = In})        -> width(In);
width(#lp_join{left = L, right = R}) -> width(L) + width(R);
width(#lp_setop{names = N})          -> length(N);
%% 導出表の中へは述語を落とさない。中の位置は射影の出力位置であって、
%% 集約やDISTINCTが挟まると外の条件をそのまま持ち込めない。
width(#lp_derived{schema = S})       -> length(S).

%% AND を平らにする / 組み直す
conjuncts(undefined)   -> [];
conjuncts({'and', Es}) -> lists:append([conjuncts(E) || E <- Es]);
conjuncts(E)           -> [E].

conj([])  -> undefined;
conj([E]) -> E;
conj(Es)  -> {'and', Es}.

wrap(undefined, Node) -> Node;
wrap(Pred, Node)      -> #lp_filter{pred = Pred, input = Node}.

wrap_phys(undefined, Node) -> Node;
wrap_phys(Pred, Node)      -> #p_filter{pred = Pred, input = Node}.

%%%===================================================================
%%% 論理 → 物理
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc 実行方法を決める。
%%----------------------------------------------------------------------
physical(Node) -> physical(Node, no_catalog()).

%% 走査の直上にある選択は、索引で置き換えられることがある。
%% 述語のプッシュダウン(rewrite/1)を先に済ませてあるので、
%% 1テーブルだけを見る条件はここまで落ちてきている。
physical(#lp_filter{pred = P, input = #lp_scan{table = T, schema = Sch}}, Cat) ->
    #{indexed := Indexed, stats := Stats} = Cat(T),
    Conjs = conjuncts(P),
    case best_index_path(Conjs, Sch, Indexed, Stats) of
        none ->
            #p_filter{pred = pexpr(P, Cat), input = #p_seq_scan{table = T, schema = Sch}};
        {Col, Val, Rest} ->
            %% 索引で使わなかった条件は選択として残す。
            %% **物理の選択を作ること。** 論理の wrap/2 を使うと
            %% 実行器が知らない節点が物理プランに混ざる
            wrap_phys(pexpr(conj(Rest), Cat),
                      #p_index_scan{table = T, schema = Sch, column = Col, value = Val})
    end;
physical(#lp_scan{table = T, schema = S}, _Cat) ->
    #p_seq_scan{table = T, schema = S};
physical(#lp_filter{pred = P, input = In}, Cat) ->
    #p_filter{pred = pexpr(P, Cat), input = physical(In, Cat)};
%% 等値で結べるならハッシュ結合。結べないなら入れ子ループ。
%%
%% 入れ子ループは |左|×|右| 回の比較をする。等値の条件が1つでもあれば
%% ハッシュ表で |左|+|右| に落ちる。右側はどちらの方式でも
%% メモリに載せるので、使うメモリは変わらない。
physical(#lp_join{type = Ty, pred = P, left = L, right = R, right_width = W}, Cat) ->
    PL = physical(L, Cat),
    PR = physical(R, Cat),
    case split_equijoin(conjuncts(P), width(L)) of
        {[], _} ->
            #p_nl_join{type = Ty, pred = pexpr(P, Cat), left = PL, right = PR,
                       right_width = W};
        {Eqs, Rest} ->
            #p_hash_join{type = Ty,
                         left_keys  = [LE || {LE, _} <- Eqs],
                         right_keys = [RE || {_, RE} <- Eqs],
                         pred = pexpr(conj(Rest), Cat),
                         left = PL, right = PR, right_width = W}
    end;
physical(#lp_agg{group_by = G, aggs = A, having = H, input = In}, Cat) ->
    #p_agg{group_by = [pexpr(E, Cat) || E <- G],
           aggs = [Ag#agg{arg = pexpr(Ag#agg.arg, Cat)} || Ag <- A],
           having = pexpr(H, Cat), input = physical(In, Cat)};
physical(#lp_sort{keys = K, limit = L, input = In}, Cat) ->
    #p_sort{keys = [{pexpr(E, Cat), D, N} || {E, D, N} <- K],
            limit = L, input = physical(In, Cat)};
physical(#lp_limit{count = C, offset = O, input = In}, Cat) ->
    #p_limit{count = C, offset = O, input = physical(In, Cat)};
physical(#lp_distinct{input = In}, Cat) ->
    #p_distinct{input = physical(In, Cat)};
physical(#lp_derived{input = In, schema = S}, Cat) ->
    #p_derived{input = physical(In, Cat), schema = S};
physical(#lp_setop{op = Op, all = All, left = L, right = R}, Cat) ->
    #p_setop{op = Op, all = All, left = physical(L, Cat), right = physical(R, Cat)};
physical(#lp_project{exprs = E, names = N, input = In}, Cat) ->
    #p_project{exprs = [pexpr(X, Cat) || X <- E], names = N,
               input = physical(In, Cat)}.

%% 式の中の副問い合わせも物理プランへ変換する。
%% 変換しないと、実行器が論理プランを渡されて動けない。
pexpr(E, Cat) ->
    sql_expr:map_subqueries(
      fun({scalar_subquery, P})  -> {scalar_subquery, plan(P, Cat)};
         ({exists_subquery, P})  -> {exists_subquery, plan(P, Cat)};
         ({in_subquery, A, P})   -> {in_subquery, A, plan(P, Cat)}
      end, E).


%%%===================================================================
%%% 結合アルゴリズムの選択
%%%===================================================================

%%----------------------------------------------------------------------
%% 条件を「等値で左右を結ぶもの」と「それ以外」に分ける。
%%
%% 右側の式は右側単体の位置に直す。結合後の行では右のカラムが
%% 左の幅だけずれているが、ハッシュ表を作るときに評価するのは
%% **右側だけの行**なので、そのままでは別のカラムを見る。
%%----------------------------------------------------------------------
split_equijoin(Conjs, LW) ->
    lists:foldr(
      fun(C, {Eqs, Rest}) ->
              case equijoin(C, LW) of
                  {ok, LE, RE} -> {[{LE, RE} | Eqs], Rest};
                  no           -> {Eqs, [C | Rest]}
              end
      end, {[], []}, Conjs).

equijoin({comp, '=', A, B}, LW) ->
    case {side(A, LW), side(B, LW)} of
        {left, right} -> {ok, A, shift(B, -LW)};
        {right, left} -> {ok, B, shift(A, -LW)};
        _             -> no
    end;
equijoin(_Other, _LW) ->
    no.

%%%===================================================================
%%% アクセスパスの選択
%%%===================================================================

%%----------------------------------------------------------------------
%% 索引で引ける条件のうち、いちばん選択率が良いものを選ぶ。
%% それでも全表走査より高くつくなら使わない。
%%
%% 「索引があるなら使う」ではないのが要点。異なり値が2しかない
%% カラムでは、索引で半分の行をランダムに読むより順に舐めた方が速い。
%%----------------------------------------------------------------------
best_index_path(Conjs, Schema, Indexed, Stats) ->
    Cands = [{sql_stats:selectivity(C, Schema, Stats), Col, Val, C}
             || C <- Conjs, {Col, Val} <- eq_on_indexed(C, Schema, Indexed)],
    case lists:sort(Cands) of
        [] ->
            none;
        [{Sel, Col, Val, Used} | _] ->
            case sql_stats:cost_index_scan(Stats, Sel) < sql_stats:cost_seq_scan(Stats) of
                false -> none;
                true  -> {Col, Val, Conjs -- [Used]}
            end
    end.

%% 索引の張られたカラムに対する等値条件なら {カラム名, 値} を返す。
%% それ以外は空リスト(内包表記でそのまま落ちる)。
eq_on_indexed({comp, '=', {ref, P}, {const, V}}, Schema, Indexed) ->
    indexed_col(P, Schema, Indexed, V);
eq_on_indexed({comp, '=', {const, V}, {ref, P}}, Schema, Indexed) ->
    indexed_col(P, Schema, Indexed, V);
eq_on_indexed(_Other, _Schema, _Indexed) ->
    [].

indexed_col(P, Schema, Indexed, V) when P >= 1, P =< length(Schema) ->
    Name = lists:nth(P, Schema),
    case lists:member(Name, Indexed) of
        true  -> [{Name, V}];
        false -> []
    end;
indexed_col(_P, _Schema, _Indexed, _V) ->
    [].
