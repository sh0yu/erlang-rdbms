%%%-------------------------------------------------------------------
%%% 物理プラン。**どうやるか**を表す。
%%%
%%% sql_planner が論理プラン(logical.hrl)から作る。
%%% 集約の仕様 #agg{} は論理側にあるので、こちらでも読み込む。
%%%
%%% この時点でカラム名は消えており、式は行タプル内の**位置**を指す。
%%% 実行時に名前で引くことがないので、評価は element/2 だけで済む。
%%%-------------------------------------------------------------------

-include("logical.hrl").

%% テーブルを丸ごと走査する。
%% schema はこの演算子が出す行のカラム名(順序つき)。上位の解決に使う。
-record(p_seq_scan, {
    table,
    schema = []
}).

%% 述語を満たす行だけを通す(σ)。
-record(p_filter, {
    pred,
    input
}).

%% 入れ子ループ結合(⋈)。
%% type は inner | left | cross。
%% 右側は左の行ごとに読み直すので、開始時にメモリへ載せる。
-record(p_nl_join, {
    type = inner,
    pred = undefined,
    left,
    right,
    right_width = 0     % LEFT JOIN で埋めるNULLの個数
}).

%% 集約(γ)。
%% 出力行は [グループキー..., 集約結果...] の順に並ぶ。
%% having は集約後の行に対する述語。
-record(p_agg, {
    group_by = [],
    aggs = [],
    having = undefined,
    input
}).

%% 並べ替え(τ)。keys は [{式, asc|desc, nulls_first|nulls_last}]。
%% limit を持つときは全件を並べずに上位N件だけ保つ(Top-N)。
-record(p_sort, {
    keys = [],
    limit = undefined,
    input
}).

%% 先頭 offset 件を捨てて count 件返す。
-record(p_limit, {
    count = undefined,
    offset = 0,
    input
}).

%% 重複を落とす(δ)。
-record(p_distinct, {
    input
}).

%% 式を評価して出力行を組み立てる(π)。
%% names は結果のカラム名。
-record(p_project, {
    exprs = [],
    names = [],
    input
}).
