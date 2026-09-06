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
%% @doc 等価な書き換え。いまは何もしない(Stage 7-2 で述語を落とす)。
%%----------------------------------------------------------------------
rewrite(Node) -> Node.

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
