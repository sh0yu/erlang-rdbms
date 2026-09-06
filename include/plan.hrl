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

%% 索引による等値検索。走査せずに一致する行だけを読む。
%%
%% **等値だけ。範囲は扱わない。** 未コミットのローカル変更を
%% 重ねる仕組み(query_exec の merge_local_index)が等値条件でしか
%% 動かないため、範囲で索引を引くと自分の変更が見えなくなる。
-record(p_index_scan, {
    table,
    schema = [],
    column,
    value
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

%% ハッシュ結合(⋈)。等値で結べるときに使う。
%%
%% 右側でハッシュ表を作り、左側で引く。入れ子ループが
%% |左|×|右| 回の比較をするのに対し、|左|+|右| で済む。
%%
%% left_keys / right_keys は、それぞれの側の**行に対して**評価する式。
%% right_keys は右側単体の位置に直してある(結合後の位置ではない)。
%% pred は等値以外の残りの条件で、一致した組に対して評価する。
-record(p_hash_join, {
    type = inner,
    left_keys = [],
    right_keys = [],
    pred = undefined,
    left,
    right,
    right_width = 0
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

%% 集合演算。両側を読み切ってから突き合わせるブロッキング演算子。
%%
%% 出力は**タプル**にする。被演算子の射影はリストを出すが、
%% 上に並べ替えが載ると位置参照(element/2)が要るため。
%% 最終的にリストへ戻すのは、上に必ず載せる射影の仕事。
-record(p_setop, {
    op,
    all = false,
    left,
    right
}).

%% 式を評価して出力行を組み立てる(π)。
%% names は結果のカラム名。
-record(p_project, {
    exprs = [],
    names = [],
    input
}).
