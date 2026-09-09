%%%-------------------------------------------------------------------
%%% @doc
%%% 物理プランを人が読める形に落とす。**純粋。**
%%%
%%% プランナのテストは実行結果では書けない。「述語が結合の下に落ちた」も
%%% 「索引スキャンを選んだ」も、**結果を変えない**からである。
%%% 構造を検査する手段が無いまま最適化を入れると、効いているかどうかが
%%% 分からないまま進むことになる。だから最適化より先にこれを作る。
%%%
%%% 位置参照(`{ref, N}`)はカラム名に戻して出す。各演算子が出す
%%% カラム名の並びを上から辿れば、位置から名前が引ける。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_explain).

-export([explain/1, schema/1]).

-include("../include/plan.hrl").

%%----------------------------------------------------------------------
%% @doc プラン木を1行1演算子で描く。深さはインデントで表す。
%%----------------------------------------------------------------------
-spec explain(term()) -> [binary()].
explain(Plan) ->
    [iolist_to_binary(L) || L <- lines(Plan, 0, [])].

%% Outers は外側の問い合わせが出す列名の並び(1つ目が1段外)。
%% 相関副問い合わせの中の {outer, L, Pos} を名前に戻すのに使う。
lines(Node, Depth, Outers) ->
    Pad = lists:duplicate(Depth * 2, $\s),
    %% 式の中の副問い合わせも木として出す。出さないと
    %% 「(subquery)」とだけ書かれて中身が分からない。
    Inner = [{C, Outers} || C <- children(Node)],
    %% 副問い合わせから見ると、この節点の入力が1段外側になる
    Sub = [{P, [input_schema(Node) | Outers]} || P <- subplans(Node)],
    [[Pad, label(Node, Outers)]
     | lists:append([lines(C, Depth + 1, O) || {C, O} <- Inner ++ Sub])].

%% その節点が式を評価するときに見ている行の列名。
input_schema(#p_filter{input = In})   -> schema(In);
input_schema(#p_project{input = In})  -> schema(In);
input_schema(#p_sort{input = In})     -> schema(In);
input_schema(#p_agg{input = In})      -> schema(In);
input_schema(#p_nl_join{left = L, right = R})   -> schema(L) ++ schema(R);
input_schema(#p_hash_join{left = L, right = R}) -> schema(L) ++ schema(R);
input_schema(Node)                    -> schema(Node).

%% その節点の式に埋まっている副問い合わせのプラン。
subplans(Node) ->
    lists:append([collect_subplans(E) || E <- node_exprs(Node)]).

node_exprs(#p_filter{pred = P})                 -> [P];
node_exprs(#p_nl_join{pred = P})                -> [P];
node_exprs(#p_hash_join{pred = P})              -> [P];
node_exprs(#p_agg{group_by = G, aggs = A, having = H}) ->
    G ++ [Ag#agg.arg || Ag <- A] ++ [H];
node_exprs(#p_sort{keys = K})                   -> [E || {E, _, _} <- K];
node_exprs(#p_project{exprs = E})               -> E;
node_exprs(_Other)                              -> [].

collect_subplans(E) ->
    lists:reverse(fold_subplans(E, [])).

fold_subplans({scalar_subquery, P}, Acc)  -> [P | Acc];
fold_subplans({exists_subquery, P}, Acc)  -> [P | Acc];
fold_subplans({in_subquery, A, P}, Acc)   -> [P | fold_subplans(A, Acc)];
fold_subplans({in, A, Es}, Acc)           -> lists:foldl(fun fold_subplans/2,
                                                         fold_subplans(A, Acc), Es);
fold_subplans({comp, _, L, R}, Acc)       -> fold_subplans(R, fold_subplans(L, Acc));
fold_subplans({arith, _, L, R}, Acc)      -> fold_subplans(R, fold_subplans(L, Acc));
fold_subplans({'and', Es}, Acc)           -> lists:foldl(fun fold_subplans/2, Acc, Es);
fold_subplans({'or', Es}, Acc)            -> lists:foldl(fun fold_subplans/2, Acc, Es);
fold_subplans({'not', E}, Acc)            -> fold_subplans(E, Acc);
fold_subplans({neg, E}, Acc)              -> fold_subplans(E, Acc);
fold_subplans({is_null, E}, Acc)          -> fold_subplans(E, Acc);
fold_subplans({is_not_null, E}, Acc)      -> fold_subplans(E, Acc);
fold_subplans(_E, Acc)                    -> Acc.

children(#p_seq_scan{})                    -> [];
children(#p_index_scan{})                  -> [];
children(#p_filter{input = In})            -> [In];
children(#p_nl_join{left = L, right = R})  -> [L, R];
children(#p_hash_join{left = L, right = R}) -> [L, R];
children(#p_agg{input = In})               -> [In];
children(#p_sort{input = In})              -> [In];
children(#p_limit{input = In})             -> [In];
children(#p_distinct{input = In})          -> [In];
children(#p_setop{left = L, right = R})     -> [L, R];
children(#p_derived{input = In})           -> [In];
children(#p_project{input = In})           -> [In].

%%%===================================================================
%%% 1演算子ぶんの見出し
%%%===================================================================

label(#p_seq_scan{table = T}, _Outers) ->
    ["Seq Scan on ", atom_to_list(T)];
label(#p_index_scan{table = T, column = C, value = V}, _Outers) ->
    ["Index Scan on ", atom_to_list(T), " (", atom_to_list(C), " = ", value(V), ")"];
label(#p_filter{pred = P, input = In}, Outers) ->
    ["Filter ", expr(P, schema(In), Outers)];
label(#p_nl_join{type = Ty, pred = P, left = L, right = R}, Outers) ->
    S = schema(L) ++ schema(R),
    ["Nested Loop ", string:uppercase(atom_to_list(Ty)), " Join",
     case P of
         undefined -> "";
         _ -> [" on ", expr(P, S, Outers)]
     end];
label(#p_hash_join{type = Ty, left_keys = LK, right_keys = RK, pred = P,
                   left = L, right = R}, Outers) ->
    LS = schema(L),
    RS = schema(R),
    Keys = [[expr(LE, LS, Outers), " = ", expr(RE, RS, Outers)] || {LE, RE} <- lists:zip(LK, RK)],
    ["Hash ", string:uppercase(atom_to_list(Ty)), " Join on ", commas(Keys),
     case P of
         undefined -> "";
         _ -> [" filter ", expr(P, LS ++ RS, Outers)]
     end];
label(#p_agg{group_by = G, aggs = A, having = H, input = In}, Outers) ->
    S = schema(In),
    ["HashAggregate",
     case G of
         [] -> "";
         _ -> [" group by (", commas([expr(K, S, Outers) || K <- G]), ")"]
     end,
     " -> (", commas([agg_name(Ag, S, Outers) || Ag <- A]), ")",
     case H of
         undefined -> "";
         _ -> [" having ", expr(H, group_schema(G, A, S), Outers)]
     end];
label(#p_sort{keys = Keys, limit = Limit, input = In}, Outers) ->
    S = schema(In),
    ["Sort (", commas([sort_key(K, S, Outers) || K <- Keys]), ")",
     case Limit of
         undefined -> "";
         N -> [" top ", integer_to_list(N)]
     end];
label(#p_limit{count = C, offset = O}, _Outers) ->
    ["Limit ",
     case C of undefined -> "all"; _ -> integer_to_list(C) end,
     case O of 0 -> ""; _ -> [" offset ", integer_to_list(O)] end];
label(#p_distinct{}, _Outers) ->
    "Unique";
label(#p_derived{schema = S}, _Outers) ->
    ["Subquery (", commas([to_str(N) || N <- S]), ")"];
label(#p_setop{op = Op, all = All}, _Outers) ->
    [string:uppercase(atom_to_list(Op)), case All of true -> " ALL"; false -> "" end];
label(#p_project{names = Names}, _Outers) ->
    ["Project (", commas([to_str(N) || N <- Names]), ")"].

sort_key({E, Dir, Nulls}, S, Outers) ->
    [expr(E, S, Outers), " ", string:uppercase(atom_to_list(Dir)),
     case Nulls of
         nulls_first -> " NULLS FIRST";
         nulls_last  -> " NULLS LAST";
         _           -> ""
     end].

agg_name(#agg{func = count_star}, _S, _Outers) -> "count(*)";
agg_name(#agg{func = F, arg = A, distinct = D}, S, Outers) ->
    [atom_to_list(F), "(", case D of true -> "DISTINCT "; false -> "" end,
     expr(A, S, Outers), ")"].

%%%===================================================================
%%% 各演算子が出すカラム名
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc その演算子が出す行のカラム名(順序つき)。位置参照を名前に戻すのに使う。
%%----------------------------------------------------------------------
-spec schema(term()) -> [atom() | string()].
schema(#p_seq_scan{schema = S})                -> S;
schema(#p_index_scan{schema = S})              -> S;
schema(#p_filter{input = In})                  -> schema(In);
schema(#p_nl_join{left = L, right = R})        -> schema(L) ++ schema(R);
schema(#p_hash_join{left = L, right = R})      -> schema(L) ++ schema(R);
schema(#p_agg{group_by = G, aggs = A, input = In}) -> group_schema(G, A, schema(In));
schema(#p_sort{input = In})                    -> schema(In);
schema(#p_limit{input = In})                   -> schema(In);
schema(#p_distinct{input = In})                -> schema(In);
schema(#p_setop{left = L})                     -> schema(L);
schema(#p_derived{schema = S})                 -> S;
schema(#p_project{names = N})                  -> N.

%% 集約の出力は [グループキー..., 集約結果...] の順。
group_schema(Group, Aggs, InSchema) ->
    [key_name(K, InSchema) || K <- Group] ++
        [iolist_to_binary(agg_name(A, InSchema, [])) || A <- Aggs].

key_name({ref, P}, S) -> name_at(P, S);
key_name(E, S)        -> iolist_to_binary(expr(E, S, [])).

%%%===================================================================
%%% 式
%%%===================================================================

expr(undefined, _S, _Outers)         -> "true";
expr({const, V}, _S, _Outers)        -> value(V);
expr({ref, P}, S, _Outers)           -> to_str(name_at(P, S));
expr({comp, Op, L, R}, S, Outers)    -> ["(", expr(L, S, Outers), " ", op(Op), " ", expr(R, S, Outers), ")"];
expr({arith, Op, L, R}, S, Outers)   -> ["(", expr(L, S, Outers), " ", atom_to_list(Op), " ", expr(R, S, Outers), ")"];
expr({'and', Es}, S, Outers)         -> ["(", sep(" AND ", [expr(E, S, Outers) || E <- Es]), ")"];
expr({'or', Es}, S, Outers)          -> ["(", sep(" OR ", [expr(E, S, Outers) || E <- Es]), ")"];
expr({'not', E}, S, Outers)          -> ["NOT ", expr(E, S, Outers)];
expr({neg, E}, S, Outers)            -> ["-", expr(E, S, Outers)];
expr({is_null, E}, S, Outers)        -> [expr(E, S, Outers), " IS NULL"];
expr({is_not_null, E}, S, Outers)    -> [expr(E, S, Outers), " IS NOT NULL"];
expr({in, A, Es}, S, Outers)         -> ["(", expr(A, S, Outers), " IN (",
                                commas([expr(E, S, Outers) || E <- Es]), "))"];
expr({in_subquery, A, _}, S, Outers) -> ["(", expr(A, S, Outers), " IN (subquery))"];
expr({func, N, Args}, S, Outers)     -> [string:uppercase(N), "(",
                                commas([expr(A, S, Outers) || A <- Args]), ")"];
expr({like, A, P}, S, Outers)        -> ["(", expr(A, S, Outers), " LIKE ", expr(P, S, Outers), ")"];
expr({'case', Ws, E}, S, Outers)     ->
    ["CASE",
     [[" WHEN ", expr(C, S, Outers), " THEN ", expr(V, S, Outers)] || {C, V} <- Ws],
     case E of undefined -> ""; _ -> [" ELSE ", expr(E, S, Outers)] end,
     " END"];
expr({scalar_subquery, _}, _S, _Outers) -> "(subquery)";
expr({exists_subquery, _}, _S, _Outers) -> "EXISTS (subquery)";
%% 外側の列。何段外かと、その行の中の位置で指してある。
%% 名前が分かるなら名前で出す。
expr({outer, L, Pos}, _S, Outers) when L >= 1, L =< length(Outers) ->
    ["outer.", to_str(name_at(Pos, lists:nth(L, Outers)))];
expr({outer, L, Pos}, _S, _Outers) ->
    io_lib:format("outer~p.#~p", [L, Pos]);
expr(Other, _S, _Outers)              -> io_lib:format("~p", [Other]).

op('=')  -> "=";
op('<>') -> "<>";
op(Op)   -> atom_to_list(Op).

value(null)                       -> "NULL";
value(true)                       -> "true";
value(false)                      -> "false";
value(V) when is_binary(V)        -> ["'", V, "'"];
value(V) when is_integer(V)       -> integer_to_list(V);
value(V) when is_float(V)         -> io_lib:format("~p", [V]);
value(V)                          -> io_lib:format("~p", [V]).

%% 位置は1始まり。範囲外なら位置のまま出す(壊れたプランを隠さない)。
name_at(P, S) when P >= 1, P =< length(S) -> lists:nth(P, S);
name_at(P, _S)                            -> "#" ++ integer_to_list(P).

to_str(A) when is_atom(A)   -> atom_to_list(A);
to_str(B) when is_binary(B) -> binary_to_list(B);
to_str(L)                   -> L.

commas(Items) -> sep(", ", Items).

sep(_Sep, [])      -> [];
sep(_Sep, [X])     -> [X];
sep(Sep, [X | T])  -> [X, Sep | sep(Sep, T)].
