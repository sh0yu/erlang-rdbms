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
    [iolist_to_binary(L) || L <- lines(Plan, 0)].

lines(Node, Depth) ->
    Pad = lists:duplicate(Depth * 2, $\s),
    [[Pad, label(Node)] | lists:append([lines(C, Depth + 1) || C <- children(Node)])].

children(#p_seq_scan{})                    -> [];
children(#p_index_scan{})                  -> [];
children(#p_filter{input = In})            -> [In];
children(#p_nl_join{left = L, right = R})  -> [L, R];
children(#p_hash_join{left = L, right = R}) -> [L, R];
children(#p_agg{input = In})               -> [In];
children(#p_sort{input = In})              -> [In];
children(#p_limit{input = In})             -> [In];
children(#p_distinct{input = In})          -> [In];
children(#p_project{input = In})           -> [In].

%%%===================================================================
%%% 1演算子ぶんの見出し
%%%===================================================================

label(#p_seq_scan{table = T}) ->
    ["Seq Scan on ", atom_to_list(T)];
label(#p_index_scan{table = T, column = C, value = V}) ->
    ["Index Scan on ", atom_to_list(T), " (", atom_to_list(C), " = ", value(V), ")"];
label(#p_filter{pred = P, input = In}) ->
    ["Filter ", expr(P, schema(In))];
label(#p_nl_join{type = Ty, pred = P, left = L, right = R}) ->
    S = schema(L) ++ schema(R),
    ["Nested Loop ", string:uppercase(atom_to_list(Ty)), " Join",
     case P of
         undefined -> "";
         _ -> [" on ", expr(P, S)]
     end];
label(#p_hash_join{type = Ty, left_keys = LK, right_keys = RK, pred = P,
                   left = L, right = R}) ->
    LS = schema(L),
    RS = schema(R),
    Keys = [[expr(LE, LS), " = ", expr(RE, RS)] || {LE, RE} <- lists:zip(LK, RK)],
    ["Hash ", string:uppercase(atom_to_list(Ty)), " Join on ", commas(Keys),
     case P of
         undefined -> "";
         _ -> [" filter ", expr(P, LS ++ RS)]
     end];
label(#p_agg{group_by = G, aggs = A, having = H, input = In}) ->
    S = schema(In),
    ["HashAggregate",
     case G of
         [] -> "";
         _ -> [" group by (", commas([expr(K, S) || K <- G]), ")"]
     end,
     " -> (", commas([agg_name(Ag, S) || Ag <- A]), ")",
     case H of
         undefined -> "";
         _ -> [" having ", expr(H, group_schema(G, A, S))]
     end];
label(#p_sort{keys = Keys, limit = Limit, input = In}) ->
    S = schema(In),
    ["Sort (", commas([sort_key(K, S) || K <- Keys]), ")",
     case Limit of
         undefined -> "";
         N -> [" top ", integer_to_list(N)]
     end];
label(#p_limit{count = C, offset = O}) ->
    ["Limit ",
     case C of undefined -> "all"; _ -> integer_to_list(C) end,
     case O of 0 -> ""; _ -> [" offset ", integer_to_list(O)] end];
label(#p_distinct{}) ->
    "Unique";
label(#p_project{names = Names}) ->
    ["Project (", commas([to_str(N) || N <- Names]), ")"].

sort_key({E, Dir, Nulls}, S) ->
    [expr(E, S), " ", string:uppercase(atom_to_list(Dir)),
     case Nulls of
         nulls_first -> " NULLS FIRST";
         nulls_last  -> " NULLS LAST";
         _           -> ""
     end].

agg_name(#agg{func = count_star}, _S) -> "count(*)";
agg_name(#agg{func = F, arg = A, distinct = D}, S) ->
    [atom_to_list(F), "(", case D of true -> "DISTINCT "; false -> "" end,
     expr(A, S), ")"].

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
schema(#p_project{names = N})                  -> N.

%% 集約の出力は [グループキー..., 集約結果...] の順。
group_schema(Group, Aggs, InSchema) ->
    [key_name(K, InSchema) || K <- Group] ++
        [iolist_to_binary(agg_name(A, InSchema)) || A <- Aggs].

key_name({ref, P}, S) -> name_at(P, S);
key_name(E, S)        -> iolist_to_binary(expr(E, S)).

%%%===================================================================
%%% 式
%%%===================================================================

expr(undefined, _S)          -> "true";
expr({const, V}, _S)         -> value(V);
expr({ref, P}, S)            -> to_str(name_at(P, S));
expr({comp, Op, L, R}, S)    -> ["(", expr(L, S), " ", op(Op), " ", expr(R, S), ")"];
expr({arith, Op, L, R}, S)   -> ["(", expr(L, S), " ", atom_to_list(Op), " ", expr(R, S), ")"];
expr({'and', Es}, S)         -> ["(", sep(" AND ", [expr(E, S) || E <- Es]), ")"];
expr({'or', Es}, S)          -> ["(", sep(" OR ", [expr(E, S) || E <- Es]), ")"];
expr({'not', E}, S)          -> ["NOT ", expr(E, S)];
expr({neg, E}, S)            -> ["-", expr(E, S)];
expr({is_null, E}, S)        -> [expr(E, S), " IS NULL"];
expr({is_not_null, E}, S)    -> [expr(E, S), " IS NOT NULL"];
expr(Other, _S)              -> io_lib:format("~p", [Other]).

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
