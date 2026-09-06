-ifndef(LOGICAL_HRL).
-define(LOGICAL_HRL, true).

%%%-------------------------------------------------------------------
%%% 論理プラン。**何をするか**だけを表し、どうやるかは含まない。
%%%
%%% sql_analyzer が AST とカタログから組み立て、sql_planner が
%%% 書き換えたうえで物理プラン(plan.hrl)へ変換する。
%%%
%%% 分ける理由は最適化のためである。
%%%   - 述語のプッシュダウンは論理プラン上の**書き換え**
%%%   - 索引を使うかどうかは論理から物理への**変換時の判断**
%%% 物理演算子を直接組み立てていると、この2つを書く場所が無い。
%%%
%%% カラム参照はこの時点で既に行タプル内の位置に束縛されている。
%%% したがって**結合の順序をここで入れ替えることはできない**
%%% (位置がずれる)。順序の決定は位置を割り当てる前、
%%% sql_analyzer の中で行う。
%%%-------------------------------------------------------------------

%% 関係そのもの。アクセス方法は決まっていない。
-record(lp_scan, {
    table,
    schema = []     % この関係が出すカラム名(順序つき)
}).

%% 選択(σ)。
-record(lp_filter, {
    pred,
    input
}).

%% 結合(⋈)。アルゴリズムは決まっていない。
%% type は inner | left | cross。
-record(lp_join, {
    type = inner,
    pred = undefined,
    left,
    right,
    right_width = 0     % LEFT JOIN で埋めるNULLの個数
}).

%% 集約1つ分。func は count_star|count|sum|avg|min|max。
%% 「どの集約を計算するか」という論理的な仕様なので、物理側にも共有する。
-record(agg, {
    func,
    arg = undefined,
    distinct = false
}).

%% 集約(γ)。
-record(lp_agg, {
    group_by = [],
    aggs = [],
    having = undefined,
    input
}).

%% 並べ替え(τ)。
-record(lp_sort, {
    keys = [],
    limit = undefined,
    input
}).

-record(lp_limit, {
    count = undefined,
    offset = 0,
    input
}).

%% 重複除去(δ)。
-record(lp_distinct, {
    input
}).

%% 集合演算。UNION(∪) / INTERSECT(∩) / EXCEPT(−)。
%%
%% 左右は同じ列数でなければならない。出力の列名は左に従う。
%% all が false なら重複を落とす。true なら重複度を保つ
%% (INTERSECT ALL は min、EXCEPT ALL は差)。
-record(lp_setop, {
    op,                 % 'union' | intersect | except
    all = false,
    left,
    right,
    names = []
}).

%% 射影(π)。
-record(lp_project, {
    exprs = [],
    names = [],
    input
}).

-endif.
