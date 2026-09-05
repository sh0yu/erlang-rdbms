%%%-------------------------------------------------------------------
%%% 実行プラン。
%%%
%%% sql_analyzer が AST とカタログを突き合わせて組み立てる。
%%% この時点でカラム名は消えており、式は行タプル内の**位置**を指す。
%%% 実行時に名前で引くことがないので、評価は element/2 だけで済む。
%%%-------------------------------------------------------------------

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

%% 集約(γ)。
%% 出力行は [グループキー..., 集約結果...] の順に並ぶ。
%% having は集約後の行に対する述語。
-record(p_agg, {
    group_by = [],
    aggs = [],
    having = undefined,
    input
}).

%% 集約1つ分。func は count_star|count|sum|avg|min|max。
-record(agg, {
    func,
    arg = undefined,
    distinct = false
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
