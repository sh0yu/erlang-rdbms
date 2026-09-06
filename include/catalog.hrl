%%%-------------------------------------------------------------------
%%% システムカタログの表現。
%%%
%%% 型はカタログだけが持つ。行の格納形式は変えない。
%%% 行に型タグを持たせると、既存のデータファイルとREDOログの互換が切れ、
%%% recover も書き直しになる。型はカラムの属性であって行の属性ではない。
%%%
%%% 型の用途は4つ:
%%%   - パース時のリテラルの解釈
%%%   - INSERT / UPDATE 時の検証
%%%   - 比較・整列の意味論
%%%   - オプティマイザの統計(将来)
%%%-------------------------------------------------------------------

%% any は SQL からは書けない内部型。
%% 型宣言のない古いタプルAPIで作られたテーブルとの互換のためだけに存在する。
-type sql_type() :: integer | float | varchar | boolean | any.

%% カラム1つぶんの統計。
%%   distinct  異なり値の数。等値条件の選択率 1/distinct に使う
%%   nulls     NULLの数
%%   min/max   範囲条件の選択率に使う。比較できない型では undefined
-record(col_stats, {
    distinct = 0 :: non_neg_integer(),
    nulls    = 0 :: non_neg_integer(),
    min          :: term(),
    max          :: term()
}).

%% テーブル1つぶんの統計。ANALYZE で採取してカタログに永続化する。
%%
%% 行を挿入・削除するたびに更新はしない。更新するとDETSへの書き込みが
%% 1行ごとに増えるうえ、トランザクションのロールバックで戻す必要が出る。
%% 統計は古くてよい(見積もりが外れるだけで結果は変わらない)。
%% PostgreSQL の ANALYZE と同じ割り切り。
-record(table_stats, {
    table        :: atom(),
    rows     = 0 :: non_neg_integer(),
    columns  = [] :: [{atom(), #col_stats{}}],
    analyzed     :: undefined | integer()   % 採取時刻(erlang:system_time(second))
}).

%% 索引の定義。カタログが持つ唯一の索引の真実。
%% 索引モジュール(ETS)の状態は起動のたびに作り直されるので、
%% 「どのカラムに索引があるか」はここにしか永続化されない。
-record(index, {
    name   :: atom(),
    table  :: atom(),
    column :: atom()
}).

-record(column, {
    name     :: atom(),
    type     = any :: sql_type(),
    position :: pos_integer()
}).
