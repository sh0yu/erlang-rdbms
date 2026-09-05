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
column_names(#p_sort{input = In}) -> column_names(In);
column_names(#p_limit{input = In}) -> column_names(In);
column_names(#p_distinct{input = In}) -> column_names(In);
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
    end;
%% 並べ替えはブロッキング演算子。open の時点で入力を読み切る。
open(#p_sort{keys = Keys, limit = Limit, input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} ->
            {error, Reason};
        Child ->
            case collect(Child, []) of
                {error, Reason} -> {error, Reason};
                {ok, Rows} -> #op{kind = sorted, st = sort_rows(Keys, Limit, Rows)}
            end
    end;
open(#p_limit{count = Count, offset = Offset, input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} -> {error, Reason};
        Child -> #op{kind = limit, st = {Count, Offset, Child}}
    end;
open(#p_distinct{input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} -> {error, Reason};
        Child -> #op{kind = distinct, st = {sets:new([{version, 2}]), Child}}
    end.

%% 入力を読み切る。並べ替えのようなブロッキング演算子で使う。
collect(Op, Acc) ->
    case next(Op) of
        {row, Row, Op2} -> collect(Op2, [Row | Acc]);
        {eof, _Op2} -> {ok, lists:reverse(Acc)}
    end.

%% 並べ替え。Erlangの項順序ではなく sql_value:order_compare/4 を使う。
%% 素の `<` だと 100 < null が真になり、NULLを含む列で順序が壊れる。
sort_rows(Keys, Limit, Rows) ->
    Sorted = lists:sort(fun(A, B) -> compare_rows(Keys, A, B) =/= gt end, Rows),
    case Limit of
        undefined -> Sorted;
        N -> lists:sublist(Sorted, N)
    end.

compare_rows([], _A, _B) ->
    eq;
compare_rows([{Expr, Dir, Nulls} | T], A, B) ->
    Va = sql_expr:eval(Expr, A),
    Vb = sql_expr:eval(Expr, B),
    case sql_value:order_compare(Va, Vb, Dir, Nulls) of
        eq -> compare_rows(T, A, B);
        Other -> Other
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

%% 並べ替え済みの行を1件ずつ返す
next(#op{kind = sorted, st = []} = Op) ->
    {eof, Op};
next(#op{kind = sorted, st = [Row | Rest]} = Op) ->
    {row, Row, Op#op{st = Rest}};

%% OFFSET 件を読み飛ばしてから COUNT 件返す。
%% 打ち切ったら下位の走査は最後まで読まない。
next(#op{kind = limit, st = {_Count, _Offset, _Child}} = Op) ->
    limit_next(Op);

next(#op{kind = distinct, st = {Seen, Child}} = Op) ->
    case next(Child) of
        {eof, Child2} ->
            {eof, Op#op{st = {Seen, Child2}}};
        {row, Row, Child2} ->
            %% 重複判定は sql_value:group_key/1 を通す。NULL同士は同じとみなし、
            %% 100 と 100.0 も同じ扱いにする(`=` の意味論とは別)。
            Key = [sql_value:group_key(V) || V <- Row],
            case sets:is_element(Key, Seen) of
                true -> next(Op#op{st = {Seen, Child2}});
                false -> {row, Row, Op#op{st = {sets:add_element(Key, Seen), Child2}}}
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

limit_next(#op{st = {0, _Offset, _Child}} = Op) ->
    {eof, Op};
limit_next(#op{st = {Count, Offset, Child}} = Op) when Offset > 0 ->
    case next(Child) of
        {eof, Child2} -> {eof, Op#op{st = {Count, Offset, Child2}}};
        {row, _Row, Child2} -> limit_next(Op#op{st = {Count, Offset - 1, Child2}})
    end;
limit_next(#op{st = {Count, 0, Child}} = Op) ->
    case next(Child) of
        {eof, Child2} -> {eof, Op#op{st = {Count, 0, Child2}}};
        {row, Row, Child2} -> {row, Row, Op#op{st = {decr(Count), 0, Child2}}}
    end.

decr(undefined) -> undefined;
decr(N) -> N - 1.

%%%===================================================================
%%% close
%%%===================================================================

close(#op{kind = seq_scan}) ->
    ok;
close(#op{kind = filter, st = {_Pred, Child}}) ->
    close(Child);
close(#op{kind = project, st = {_Exprs, Child}}) ->
    close(Child);
close(#op{kind = sorted}) ->
    ok;
close(#op{kind = limit, st = {_C, _O, Child}}) ->
    close(Child);
close(#op{kind = distinct, st = {_Seen, Child}}) ->
    close(Child);
close({error, _}) ->
    ok.
