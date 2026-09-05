%%%-------------------------------------------------------------------
%%% @doc
%%% 実行器。Volcano(反復子)モデル。
%%%
%%%   open(PlanNode, Ctx) -> Op
%%%   next(Op)            -> {row, Row, Op} | {eof, Op}
%%%   close(Op)           -> ok
%%%
%%% 各演算子が親の `next/1` に1行ずつ返す。全件をリストに落とさないので、
%%% LIMIT を足したときに下位の走査を最後まで読まずに済む。
%%%
%%% 演算子の状態は返り値で持ち回る(不変)。子演算子は親の状態の中に
%%% そのまま入るので、状態の受け渡しが自然に再帰する。
%%%
%%% 実行は query_exec プロセスの中で行う。走査がそのトランザクションの
%%% 未コミットの変更を見る必要があるため。ストレージへの入口は Ctx に
%%% 入っている関数(scan_open / scan_next)経由にしてある。
%%%
%%% 演算子が増えたら(join, aggregate, sort)モジュールに分ける。
%%% 今は3つしかないので1ファイルに置いている。
%%% @end
%%%-------------------------------------------------------------------
-module(sql_exec).

-export([run/2, open/2, next/1, close/1]).

-include("../include/plan.hrl").

%% 演算子。mod は種別、st はその状態。
-record(op, {kind, st}).

%% 実行文脈。ストレージへの入口を関数で渡すことで、
%% 実行器が query_exec の内部状態に直接触らないようにする。
-record(ctx, {scan_open, scan_next}).

-export_type([ctx/0]).
-opaque ctx() :: #ctx{}.

%%----------------------------------------------------------------------
%% @doc プランを最後まで走らせて結果セットを返す。
%%
%% Ctx は {ScanOpenFun, ScanNextFun}。
%% Returns: {ok, ColumnNames, Rows} | {error, Reason}
%%
%% カラム名を一緒に返すのは、結果セットが「名前つきの列の並び」だから。
%% クライアントが見出しを出すのに要る。
%%----------------------------------------------------------------------
run(Plan, {ScanOpen, ScanNext}) ->
    Ctx = #ctx{scan_open = ScanOpen, scan_next = ScanNext},
    case open(Plan, Ctx) of
        {error, Reason} ->
            {error, Reason};
        Op ->
            try
                case drain(Op, []) of
                    {ok, Rows} -> {ok, column_names(Plan), Rows};
                    {error, Reason} -> {error, Reason}
                end
            after
                close(Op)
            end
    end.

%% プランの最上位が出力するカラム名。
column_names(#p_project{names = Names}) -> Names;
column_names(#p_filter{input = In}) -> column_names(In);
column_names(#p_seq_scan{schema = Schema}) -> Schema.

drain(Op, Acc) ->
    case next(Op) of
        {row, Row, Op2} -> drain(Op2, [Row | Acc]);
        {eof, _Op2} -> {ok, lists:reverse(Acc)}
    end.

%%%===================================================================
%%% open
%%%===================================================================

open(#p_seq_scan{table = Table}, #ctx{scan_open = ScanOpen} = Ctx) ->
    case ScanOpen(Table) of
        {error, Reason} ->
            {error, Reason};
        {ok, Cursor} ->
            #op{kind = seq_scan, st = {Ctx, Cursor, []}}
    end;
open(#p_filter{pred = Pred, input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} -> {error, Reason};
        Child -> #op{kind = filter, st = {Pred, Child}}
    end;
open(#p_project{exprs = Exprs, input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} -> {error, Reason};
        Child -> #op{kind = project, st = {Exprs, Child}}
    end.

%%%===================================================================
%%% next
%%%===================================================================

%% 走査は1ページ分ずつ受け取り、手元のバッファから1行ずつ返す。
next(#op{kind = seq_scan, st = {Ctx, Cursor, [Row | Rest]}} = Op) ->
    {row, Row, Op#op{st = {Ctx, Cursor, Rest}}};
next(#op{kind = seq_scan, st = {#ctx{scan_next = ScanNext} = Ctx, Cursor, []}} = Op) ->
    case ScanNext(Cursor) of
        eof ->
            {eof, Op};
        {rows, Rows, Cursor2} ->
            %% 行はタプルにしておく。束縛済みの式が位置参照なので、
            %% element/2 で O(1) に引ける。リストのままだと
            %% lists:nth/2 になり、行数×カラム数のオーダで走る。
            Tuples = [list_to_tuple(Val) || {_Oid, Val} <- Rows],
            next(Op#op{st = {Ctx, Cursor2, Tuples}})
    end;

next(#op{kind = filter, st = {Pred, Child}} = Op) ->
    case next(Child) of
        {eof, Child2} ->
            {eof, Op#op{st = {Pred, Child2}}};
        {row, Row, Child2} ->
            case sql_expr:eval_pred(Pred, Row) of
                true -> {row, Row, Op#op{st = {Pred, Child2}}};
                %% 3値論理はここに集約されている。null は通さない。
                false -> next(Op#op{st = {Pred, Child2}})
            end
    end;

next(#op{kind = project, st = {Exprs, Child}} = Op) ->
    case next(Child) of
        {eof, Child2} ->
            {eof, Op#op{st = {Exprs, Child2}}};
        {row, Row, Child2} ->
            %% 出力はリストに戻す。既存APIの行の形に合わせるため。
            Out = [sql_expr:eval(E, Row) || E <- Exprs],
            {row, Out, Op#op{st = {Exprs, Child2}}}
    end.

%%%===================================================================
%%% close
%%%===================================================================

close(#op{kind = seq_scan}) ->
    ok;
close(#op{kind = filter, st = {_Pred, Child}}) ->
    close(Child);
close(#op{kind = project, st = {_Exprs, Child}}) ->
    close(Child);
close({error, _}) ->
    ok.
