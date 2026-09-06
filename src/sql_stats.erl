%%%-------------------------------------------------------------------
%%% @doc
%%% 統計の採取と、それを使った見積もり。**このモジュールは純粋。**
%%% 走査はストレージ層がやり、ここは1行ずつ受け取って畳むだけ。
%%%
%%% 統計は古くてよい。見積もりが外れても**結果は変わらない**、
%%% 遅くなるだけである。だから挿入・削除のたびに更新はせず、
%%% ANALYZE で採り直す(PostgreSQL と同じ割り切り)。
%%%
%%% 採っていないテーブルには既定値を使う。
%%% 「統計が無いから最適化しない」ではなく「分からないなりに見積もる」。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_stats).

-export([empty/1, accumulate/2, finish/3]).
-export([rows/1, distinct/2, nulls/2, column/2]).
-export([default_rows/0]).
-export([selectivity/3, cost_seq_scan/1, cost_index_scan/2]).

-include("../include/catalog.hrl").

%% 統計を採っていないテーブルの想定行数。
%% 大きすぎると索引を使いすぎ、小さすぎると使わなくなる。
-define(DEFAULT_ROWS, 1000).

%% 異なり値がこれを超えたら数えるのをやめる。
%% 全行ぶんの集合を持つと、大きな表でメモリを食う。
%% 選択率の見積もりに要るのは桁であって正確な値ではない。
-define(DISTINCT_CAP, 10000).

-spec default_rows() -> pos_integer().
default_rows() -> ?DEFAULT_ROWS.

%%%===================================================================
%%% 採取
%%%===================================================================

%% @doc カラム数ぶんの空の集計器。
empty(Columns) ->
    [{#{}, 0, undefined, undefined} || _ <- Columns].

%% @doc 1行を畳み込む。
accumulate(Row, Acc) when length(Row) =:= length(Acc) ->
    lists:zipwith(fun acc_val/2, Row, Acc);
accumulate(_Row, Acc) ->
    %% カラム数が合わない行はカタログと整合しないので数えない
    Acc.

acc_val(null, {Set, Nulls, Min, Max}) ->
    {Set, Nulls + 1, Min, Max};
acc_val(V, {Set, Nulls, Min, Max}) ->
    Set1 = case map_size(Set) >= ?DISTINCT_CAP of
               true  -> Set;
               false -> Set#{V => []}
           end,
    {Set1, Nulls, least(V, Min), greatest(V, Max)}.

%% 最小・最大は sql_value:compare/2 で比べる。
%% Erlangの項順序だと 100 < null が真になり、型が混ざると壊れる。
least(V, undefined) -> V;
least(V, Cur) ->
    case sql_value:compare(V, Cur) of
        lt -> V;
        _  -> Cur
    end.

greatest(V, undefined) -> V;
greatest(V, Cur) ->
    case sql_value:compare(V, Cur) of
        gt -> V;
        _  -> Cur
    end.

%% @doc 集計器を統計に落とす。
-spec finish([atom()], non_neg_integer(), list()) -> #table_stats{}.
finish(Columns, RowCount, Acc) when is_list(Columns) ->
    Cols = lists:zipwith(
             fun(Name, {Set, Nulls, Min, Max}) ->
                     {Name, #col_stats{distinct = map_size(Set), nulls = Nulls,
                                       min = Min, max = Max}}
             end, Columns, Acc),
    #table_stats{rows = RowCount, columns = Cols}.

%%%===================================================================
%%% 見積もり
%%%===================================================================

%% 異なり値が分からないときの等値条件の選択率。
-define(DEFAULT_EQ_SEL, 0.1).
%% 範囲条件の選択率。最小最大から求めることもできるが、
%% 型をまたぐと比較できないので定数にしてある(PostgreSQLも既定は 1/3)。
-define(RANGE_SEL, 0.33).
%% 読めない述語の選択率。半分通ると仮定する。
-define(UNKNOWN_SEL, 0.5).
%% 索引経由の1行はランダム読み。順次走査の1行より高くつく。
-define(RANDOM_PAGE_COST, 2.0).

%%----------------------------------------------------------------------
%% @doc 述語が何割の行を通すか。0.0〜1.0。
%% Schema は位置参照をカラム名に戻すための並び。
%%----------------------------------------------------------------------
-spec selectivity(term(), [atom()], #table_stats{} | none) -> float().
selectivity(undefined, _Schema, _Stats) ->
    1.0;
selectivity({'and', Es}, Schema, Stats) ->
    %% 条件は独立と仮定して掛ける。相関があると外れるが、
    %% 相関を測る統計を持っていない
    lists:foldl(fun(E, A) -> A * selectivity(E, Schema, Stats) end, 1.0, Es);
selectivity({'or', Es}, Schema, Stats) ->
    1.0 - lists:foldl(fun(E, A) -> A * (1.0 - selectivity(E, Schema, Stats)) end, 1.0, Es);
selectivity({'not', E}, Schema, Stats) ->
    1.0 - selectivity(E, Schema, Stats);
selectivity({comp, '=', {ref, P}, {const, _}}, Schema, Stats) ->
    eq_sel(P, Schema, Stats);
selectivity({comp, '=', {const, _}, {ref, P}}, Schema, Stats) ->
    eq_sel(P, Schema, Stats);
selectivity({comp, '<>', {ref, P}, {const, _}}, Schema, Stats) ->
    1.0 - eq_sel(P, Schema, Stats);
selectivity({comp, Op, _, _}, _Schema, _Stats)
  when Op =:= '<'; Op =:= '<='; Op =:= '>'; Op =:= '>=' ->
    ?RANGE_SEL;
selectivity({is_null, {ref, P}}, Schema, Stats) ->
    null_frac(P, Schema, Stats);
selectivity({is_not_null, {ref, P}}, Schema, Stats) ->
    1.0 - null_frac(P, Schema, Stats);
selectivity(_Other, _Schema, _Stats) ->
    ?UNKNOWN_SEL.

eq_sel(P, Schema, Stats) ->
    case name_at(P, Schema) of
        undefined -> ?DEFAULT_EQ_SEL;
        Name ->
            case distinct(Stats, Name) of
                0 -> ?DEFAULT_EQ_SEL;
                D -> 1.0 / D
            end
    end.

null_frac(P, Schema, Stats) ->
    case {name_at(P, Schema), rows(Stats)} of
        {undefined, _} -> ?UNKNOWN_SEL;
        {_, 0}         -> 0.0;
        {Name, R}      -> nulls(Stats, Name) / R
    end.

name_at(P, Schema) when P >= 1, P =< length(Schema) -> lists:nth(P, Schema);
name_at(_P, _Schema)                                -> undefined.

%%----------------------------------------------------------------------
%% @doc 費用。単位は「順次走査で1行読む」を 1.0 とする相対値。
%%----------------------------------------------------------------------
-spec cost_seq_scan(#table_stats{} | none) -> float().
cost_seq_scan(Stats) -> float(rows(Stats)).

%% 索引スキャンは一致した行だけを読むが、1行ごとにランダム読みになる。
%% 選択率が悪いと順次走査に負ける。異なり値が2しかないカラムに
%% 索引を張っても使われないのは、この式のため。
-spec cost_index_scan(#table_stats{} | none, float()) -> float().
cost_index_scan(Stats, Sel) ->
    1.0 + rows(Stats) * Sel * ?RANDOM_PAGE_COST.

%%%===================================================================
%%% 参照
%%%===================================================================

%% @doc 行数。統計が無ければ既定値。
-spec rows(#table_stats{} | none) -> non_neg_integer().
rows(none)                   -> ?DEFAULT_ROWS;
rows(#table_stats{rows = R}) -> R.

%% @doc カラムの統計。無ければ undefined。
-spec column(#table_stats{} | none, atom()) -> #col_stats{} | undefined.
column(none, _Name) -> undefined;
column(#table_stats{columns = Cols}, Name) ->
    case lists:keyfind(Name, 1, Cols) of
        {Name, CS} -> CS;
        false      -> undefined
    end.

%% @doc 異なり値の数。分からなければ 0 を返す(呼び出し側が既定に落とす)。
distinct(Stats, Name) ->
    case column(Stats, Name) of
        undefined              -> 0;
        #col_stats{distinct = D} -> D
    end.

nulls(Stats, Name) ->
    case column(Stats, Name) of
        undefined            -> 0;
        #col_stats{nulls = N} -> N
    end.
