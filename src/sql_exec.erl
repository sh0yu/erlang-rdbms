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
-record(op, {kind, st, env = #{}}).

%% 実行文脈。ストレージへの入口を関数で渡すことで、
%% 実行器が query_exec の内部状態に直接触らないようにする。
%% outers は外側の行の並び(1つ目が1段外)。相関副問い合わせのためにある。
-record(ctx, {scan_open, scan_next, index_lookup, outers = []}).

-export_type([ctx/0]).
-opaque ctx() :: #ctx{}.

%%----------------------------------------------------------------------
%% @doc プランを最後まで走らせて結果セットを返す。
%%
%% Ctx は #{scan_open => F, scan_next => F, index_lookup => F}。
%% index_lookup を渡さないと索引スキャンは実行できない
%% (プランナが索引を選ばなければ要らない)。
%% Returns: {ok, ColumnNames, Rows} | {error, Reason}
%%
%% カラム名を一緒に返すのは、結果セットが「名前つきの列の並び」だから。
%% クライアントが見出しを出すのに要る。
%%----------------------------------------------------------------------
run(Plan0, #{scan_open := ScanOpen, scan_next := ScanNext} = Fns) ->
    Ctx = #ctx{scan_open = ScanOpen, scan_next = ScanNext,
               index_lookup = maps:get(index_lookup, Fns, undefined)},
    %% 相関しない副問い合わせは、本体を動かす前に1回だけ実行して
    %% 定数へ畳む。行ごとに実行し直す必要が無いので。
    try resolve_subqueries(Plan0, Ctx) of
        Plan -> run_1(Plan, Ctx)
    catch
        throw:{subquery_error, Reason} -> {error, Reason}
    end.

run_1(Plan, Ctx) ->
    case open(Plan, Ctx) of
        {error, Reason} ->
            {error, Reason};
        Op ->
            try
                {ok, Rows} = drain(Op, []),
                {ok, column_names(Plan), Rows}
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
column_names(#p_agg{}) -> [];
column_names(#p_setop{left = L}) -> column_names(L);
column_names(#p_derived{schema = S}) -> S;
column_names(#p_nl_join{left = L, right = R}) -> column_names(L) ++ column_names(R);
column_names(#p_seq_scan{schema = Schema}) -> Schema.

drain(Op, Acc) ->
    case next(Op) of
        {row, Row, Op2} -> drain(Op2, [Row | Acc]);
        {eof, _Op2} -> {ok, lists:reverse(Acc)}
    end.

%%%===================================================================
%%% open
%%%===================================================================

%%----------------------------------------------------------------------
%% 演算子を作る。**env はここで一括して押す。**
%% 個々の節で付け忘れると、その演算子だけ外側の行を見られなくなる。
%%----------------------------------------------------------------------
open(Plan, Ctx) ->
    case open_1(Plan, Ctx) of
        {error, Reason} -> {error, Reason};
        #op{} = Op      -> Op#op{env = env(Ctx)}
    end.

%% 式の評価に渡す環境。副問い合わせを実行する関数もここに入れる。
%% 実行するとき、いまの行を1段外側として積む。
env(#ctx{outers = Outers} = Ctx) ->
    #{outers => Outers,
      run => fun(Plan, CurRow) ->
                     sub_lists(Plan, Ctx#ctx{outers = [CurRow | Outers]})
             end}.

open_1(#p_seq_scan{table = Table}, #ctx{scan_open = ScanOpen} = Ctx) ->
    case ScanOpen(Table) of
        {error, Reason} ->
            {error, Reason};
        {ok, Cursor} ->
            #op{kind = seq_scan, st = {Ctx, Cursor, []}}
    end;
%% 索引による等値検索。走査せずに一致する行だけを読む。
%%
%% 索引そのものは共有データしか知らないので、引くのは
%% query_exec が渡す関数に任せる。そちらが未コミットの
%% ローカル変更を重ねてから返す。ここで索引を直接引くと、
%% 自分がさっき入れた行が見えない。
open_1(#p_index_scan{table = Table, column = Col, value = Val},
     #ctx{index_lookup = Lookup}) when is_function(Lookup, 3) ->
    case Lookup(Table, Col, Val) of
        {error, Reason} ->
            {error, Reason};
        {ok, Rows} ->
            #op{kind = sorted, st = [list_to_tuple(R) || R <- Rows]}
    end;
open_1(#p_index_scan{}, _Ctx) ->
    {error, index_lookup_not_available};
open_1(#p_filter{pred = Pred, input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} -> {error, Reason};
        Child -> #op{kind = filter, st = {Pred, Child}}
    end;
open_1(#p_project{exprs = Exprs, input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} -> {error, Reason};
        Child -> #op{kind = project, st = {Exprs, Child}}
    end;
%% 並べ替えはブロッキング演算子。open の時点で入力を読み切る。
open_1(#p_sort{keys = Keys, limit = Limit, input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} ->
            {error, Reason};
        Child ->
            {ok, Rows} = collect(Child, []),
            #op{kind = sorted, st = sort_rows(Keys, Limit, Rows, env(Ctx))}
    end;
open_1(#p_limit{count = Count, offset = Offset, input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} -> {error, Reason};
        Child -> #op{kind = limit, st = {Count, Offset, Child}}
    end;
open_1(#p_distinct{input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} -> {error, Reason};
        Child -> #op{kind = distinct, st = {sets:new([{version, 2}]), Child}}
    end;
%% 入れ子ループ結合。
%% 右側は左の行ごとに読み直すので、開始時にメモリへ載せる
%% (走査カーソルは一度しか流せないため)。
open_1(#p_nl_join{type = Type, pred = Pred, left = L, right = R,
                left_width = LW, right_width = W}, Ctx) ->
    case open(L, Ctx) of
        {error, Reason} ->
            {error, Reason};
        Left ->
            case open(R, Ctx) of
                {error, Reason} ->
                    {error, Reason};
                Right ->
                    {ok, Rows} = collect(Right, []),
                    close(Right),
                    #op{kind = nl_join,
                        st = join_state(Type, Pred, Left, Rows, LW, W)}
            end
    end;
%% 集約もブロッキング演算子。入力を読み切ってグループごとにまとめる。
%% ハッシュ結合。右側でハッシュ表を作り、左側で引く。
open_1(#p_hash_join{type = Type, left_keys = LK, right_keys = RK, pred = Pred,
                  left = L, right = R, left_width = LW, right_width = W}, Ctx) ->
    case open(L, Ctx) of
        {error, Reason} ->
            {error, Reason};
        Left ->
            case open(R, Ctx) of
                {error, Reason} ->
                    {error, Reason};
                Right ->
                    {ok, Rows} = collect(Right, []),
                    close(Right),
                    St = join_state(Type, Pred, Left, Rows, LW, W),
                    #op{kind = hash_join,
                        st = St#{table => build_hash(maps:get(rows, St), RK, env(Ctx)),
                                 lkeys => LK}}
            end
    end;
%% 導出表。子が出すリストの行をタプルに直して流す。
open_1(#p_derived{input = In}, Ctx) ->
    case open(In, Ctx) of
        {error, Reason} -> {error, Reason};
        Child           -> #op{kind = derived, st = Child}
    end;
%% 集合演算。両側を読み切ってから突き合わせる。
open_1(#p_setop{op = Op, all = All, left = L, right = R}, Ctx) ->
    case open(L, Ctx) of
        {error, Reason} ->
            {error, Reason};
        Left ->
            case open(R, Ctx) of
                {error, Reason} ->
                    close(Left),
                    {error, Reason};
                Right ->
                    {ok, LRows} = collect(Left, []),
                    {ok, RRows} = collect(Right, []),
                    close(Left),
                    close(Right),
                    %% 上に並べ替えが載ることがあるのでタプルで出す。
                    %% 位置参照(element/2)がリストでは引けない。
                    #op{kind = sorted,
                        st = [to_tuple(Row) || Row <- set_op(Op, All, LRows, RRows)]}
            end
    end;
open_1(#p_agg{group_by = Keys, aggs = Aggs, having = Having, input = Input}, Ctx) ->
    case open(Input, Ctx) of
        {error, Reason} ->
            {error, Reason};
        Child ->
            {ok, Rows} = collect(Child, []),
            %% 上位の演算子(並べ替え・射影)は行をタプルとして扱う。
            %% 束縛済みの式が位置参照で、element/2 で引くため。
            Env = env(Ctx),
            Out = [list_to_tuple(R) || R <- aggregate(Keys, Aggs, Rows, Env)],
            #op{kind = sorted,
                st = [R || R <- Out, sql_expr:eval_pred(Having, R, Env)]}
    end.

%% 入力を読み切る。並べ替えのようなブロッキング演算子で使う。
collect(Op, Acc) ->
    case next(Op) of
        {row, Row, Op2} -> collect(Op2, [Row | Acc]);
        {eof, _Op2} -> {ok, lists:reverse(Acc)}
    end.

%% 並べ替え。Erlangの項順序ではなく sql_value:order_compare/4 を使う。
%% 素の `<` だと 100 < null が真になり、NULLを含む列で順序が壊れる。
sort_rows(Keys, Limit, Rows, Env) ->
    Sorted = lists:sort(fun(A, B) -> compare_rows(Keys, A, B, Env) =/= gt end, Rows),
    case Limit of
        undefined -> Sorted;
        N -> lists:sublist(Sorted, N)
    end.

compare_rows([], _A, _B, _Env) ->
    eq;
compare_rows([{Expr, Dir, Nulls} | T], A, B, Env) ->
    Va = sql_expr:eval(Expr, A, Env),
    Vb = sql_expr:eval(Expr, B, Env),
    case sql_value:order_compare(Va, Vb, Dir, Nulls) of
        eq -> compare_rows(T, A, B, Env);
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

next(#op{kind = filter, st = {Pred, Child}, env = Env} = Op) ->
    case next(Child) of
        {eof, Child2} ->
            {eof, Op#op{st = {Pred, Child2}}};
        {row, Row, Child2} ->
            case sql_expr:eval_pred(Pred, Row, Env) of
                true -> {row, Row, Op#op{st = {Pred, Child2}}};
                %% 3値論理はここに集約されている。null は通さない。
                false -> next(Op#op{st = {Pred, Child2}})
            end
    end;

%% 左の行を1つ取り、右の行を順に当てる。
next(#op{kind = nl_join, st = St, env = Env} = Op) ->
    nl_next(Op, St, Env);

next(#op{kind = hash_join, st = St, env = Env} = Op) ->
    hj_next(Op, St, Env);

next(#op{kind = derived, st = Child} = Op) ->
    case next(Child) of
        {eof, Child2}       -> {eof, Op#op{st = Child2}};
        {row, Row, Child2}  -> {row, to_tuple(Row), Op#op{st = Child2}}
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

next(#op{kind = project, st = {Exprs, Child}, env = Env} = Op) ->
    case next(Child) of
        {eof, Child2} ->
            {eof, Op#op{st = {Exprs, Child2}}};
        {row, Row, Child2} ->
            %% 出力はリストに戻す。既存APIの行の形に合わせるため。
            Out = [sql_expr:eval(E, Row, Env) || E <- Exprs],
            {row, Out, Op#op{st = {Exprs, Child2}}}
    end.

%%----------------------------------------------------------------------
%% 結合の状態。入れ子ループとハッシュ結合で共通。
%%
%% 右側の行に添字を振るのは、**どれが一致したかを覚えるため**である。
%% RIGHT / FULL では、左を全部見終わったあとに一致しなかった右の行を
%% 左をNULLで埋めて出す必要がある。
%%
%%   stage = probe  左を1行ずつ見て、右の候補と突き合わせる
%%   stage = drain  左を読み切った。未一致の右の行を出す(RIGHT/FULLのみ)
%%----------------------------------------------------------------------
join_state(Type, Pred, Left, Rows, LW, RW) ->
    #{type => Type, pred => Pred, left => Left,
      rows => lists:zip(lists:seq(1, length(Rows)), Rows),
      rest => [], cur => undefined, matched => false,
      seen => #{}, stage => probe, lwidth => LW, rwidth => RW}.

%% 未一致の右の行。
unmatched(#{rows := Rows, seen := Seen}) ->
    [P || {I, _} = P <- Rows, not maps:is_key(I, Seen)].

%% 左をNULLで埋める(RIGHT/FULL)。
pad_left(R, LW) ->
    list_to_tuple(lists:duplicate(LW, null) ++ tuple_to_list(R)).

%% 右をNULLで埋める(LEFT/FULL)。
pad_right(L, RW) ->
    list_to_tuple(tuple_to_list(L) ++ lists:duplicate(RW, null)).

outer_right(T) -> T =:= right orelse T =:= full.

%% 右側を出し切ったら次の左の行へ。
%% 1件も一致しなかった左の行は、LEFT/FULL なら右をNULLで埋めて返す。
%% 左を読み切ったら、RIGHT/FULL なら未一致の右の行を出す。
nl_next(Op, #{stage := drain, rest := []} = St, _Env) ->
    {eof, Op#op{st = St}};
nl_next(Op, #{stage := drain, rest := [{_I, R} | Rest], lwidth := LW} = St, _Env) ->
    {row, pad_left(R, LW), Op#op{st = St#{rest => Rest}}};
nl_next(Op, #{cur := undefined, left := Left, type := Type} = St, Env) ->
    case next(Left) of
        {eof, Left2} ->
            St1 = St#{left => Left2},
            case outer_right(Type) of
                true  -> nl_next(Op, St1#{stage => drain, rest => unmatched(St1)}, Env);
                false -> {eof, Op#op{st = St1}}
            end;
        {row, Row, Left2} ->
            nl_next(Op, St#{left => Left2, cur => Row,
                            rest => maps:get(rows, St), matched => false}, Env)
    end;
nl_next(Op, #{rest := [], type := Type, matched := false,
              cur := Cur, rwidth := RW} = St, _Env) when Type =:= left; Type =:= full ->
    {row, pad_right(Cur, RW), Op#op{st = St#{cur => undefined, matched => true}}};
nl_next(Op, #{rest := []} = St, Env) ->
    nl_next(Op, St#{cur => undefined}, Env);
nl_next(Op, #{rest := [{I, R} | Rest], cur := Cur, pred := Pred, seen := Seen} = St, Env) ->
    Joined = concat_rows(Cur, R),
    case sql_expr:eval_pred(Pred, Joined, Env) of
        true  -> {row, Joined, Op#op{st = St#{rest => Rest, matched => true,
                                              seen => Seen#{I => []}}}};
        false -> nl_next(Op, St#{rest => Rest}, Env)
    end.

concat_rows(A, B) ->
    list_to_tuple(tuple_to_list(A) ++ tuple_to_list(B)).

%%%===================================================================
%%% 副問い合わせの畳み込み
%%%
%%% 相関しない副問い合わせは本体の行に依存しないので、1回実行して
%%% 定数に置き換える。行ごとに実行し直す必要が無い。
%%%===================================================================

resolve_subqueries(Node, Ctx) ->
    map_node_exprs(fun(E) -> resolve_expr(E, Ctx) end, Node, Ctx).

%%----------------------------------------------------------------------
%% 副問い合わせを定数へ畳む。**相関するものは畳まない。**
%%
%% 相関しない副問い合わせは外側の行に依存しないので1回で済む。
%% 外側の列を参照している({outer, _, _} を含む)ものは行ごとに
%% 答えが変わるので、式のまま残して実行時に評価する。
%%----------------------------------------------------------------------
resolve_expr(E, Ctx) ->
    sql_expr:map_subqueries(
      fun({scalar_subquery, P} = Node) ->
              case correlated(P) of
                  true  -> Node;
                  false -> {const, scalar_value(P, Ctx)}
              end;
         ({exists_subquery, P} = Node) ->
              case correlated(P) of
                  true  -> Node;
                  false -> {const, sub_rows(P, Ctx) =/= []}
              end;
         ({in_subquery, A, P} = Node) ->
              case correlated(P) of
                  true  -> Node;
                  false -> {in, A, [{const, V} || [V] <- sub_lists(P, Ctx)]}
              end
      end, E).

%% そのプランが外側の列を参照しているか。
correlated(Plan) ->
    lists:any(fun has_outer/1, plan_exprs(Plan)).

%% プラン木の全ての式(子も含む)。
plan_exprs(Node) ->
    node_exprs(Node) ++ lists:append([plan_exprs(C) || C <- node_children(Node)]).

node_exprs(#p_filter{pred = P})            -> [P];
node_exprs(#p_nl_join{pred = P})           -> [P];
node_exprs(#p_hash_join{pred = P, left_keys = LK, right_keys = RK}) -> [P | LK ++ RK];
node_exprs(#p_agg{group_by = G, aggs = A, having = H}) ->
    G ++ [Ag#agg.arg || Ag <- A] ++ [H];
node_exprs(#p_sort{keys = K})              -> [E || {E, _, _} <- K];
node_exprs(#p_project{exprs = E})          -> E;
node_exprs(_Other)                         -> [].

node_children(#p_filter{input = In})            -> [In];
node_children(#p_nl_join{left = L, right = R})  -> [L, R];
node_children(#p_hash_join{left = L, right = R})-> [L, R];
node_children(#p_agg{input = In})               -> [In];
node_children(#p_sort{input = In})              -> [In];
node_children(#p_limit{input = In})             -> [In];
node_children(#p_distinct{input = In})          -> [In];
node_children(#p_project{input = In})           -> [In];
node_children(#p_derived{input = In})           -> [In];
node_children(#p_setop{left = L, right = R})    -> [L, R];
node_children(_Other)                           -> [].

%% 式が外側の列を参照しているか。
%% 副問い合わせの中まで見る(入れ子の内側から外側を参照することがある)。
has_outer({outer, _, _})        -> true;
has_outer({comp, _, L, R})      -> has_outer(L) orelse has_outer(R);
has_outer({arith, _, L, R})     -> has_outer(L) orelse has_outer(R);
has_outer({'and', Es})          -> lists:any(fun has_outer/1, Es);
has_outer({'or', Es})           -> lists:any(fun has_outer/1, Es);
has_outer({'not', E})           -> has_outer(E);
has_outer({neg, E})             -> has_outer(E);
has_outer({is_null, E})         -> has_outer(E);
has_outer({is_not_null, E})     -> has_outer(E);
has_outer({func, _, Args})      -> lists:any(fun has_outer/1, Args);
has_outer({like, A, P})         -> has_outer(A) orelse has_outer(P);
has_outer({in, A, Es})          -> has_outer(A) orelse lists:any(fun has_outer/1, Es);
has_outer({'case', Ws, E}) ->
    lists:any(fun({C, V}) -> has_outer(C) orelse has_outer(V) end, Ws)
        orelse (E =/= undefined andalso has_outer(E));
has_outer({scalar_subquery, P}) -> correlated(P);
has_outer({exists_subquery, P}) -> correlated(P);
has_outer({in_subquery, A, P})  -> has_outer(A) orelse correlated(P);
has_outer(_)                    -> false.

%% スカラー副問い合わせは1行1列でなければならない。
%% 0行なら NULL(標準SQL)。2行以上はエラー。
scalar_value(P, Ctx) ->
    case sub_lists(P, Ctx) of
        []       -> null;
        [[V]]    -> V;
        Rows     -> throw({subquery_error, {scalar_subquery_returned_rows, length(Rows)}})
    end.

sub_rows(P, Ctx) -> sub_lists(P, Ctx).

sub_lists(P, Ctx) ->
    %% 副問い合わせの中にも副問い合わせがありうる
    Plan = resolve_subqueries(P, Ctx),
    case open(Plan, Ctx) of
        {error, Reason} ->
            throw({subquery_error, Reason});
        Op ->
            try
                {ok, Rows} = drain(Op, []),
                [to_list(R) || R <- Rows]
            after
                close(Op)
            end
    end.

%% プランの各節点が持つ式に F を当てる。副問い合わせが現れうるのは
%% WHERE / ON / HAVING / GROUP BY / ORDER BY / 選択リスト。
map_node_exprs(F, #p_filter{pred = P, input = In} = N, Ctx) ->
    N#p_filter{pred = F(P), input = map_node_exprs(F, In, Ctx)};
map_node_exprs(F, #p_nl_join{pred = P, left = L, right = R} = N, Ctx) ->
    N#p_nl_join{pred = F(P), left = map_node_exprs(F, L, Ctx),
                right = map_node_exprs(F, R, Ctx)};
map_node_exprs(F, #p_hash_join{pred = P, left = L, right = R} = N, Ctx) ->
    N#p_hash_join{pred = F(P), left = map_node_exprs(F, L, Ctx),
                  right = map_node_exprs(F, R, Ctx)};
map_node_exprs(F, #p_agg{group_by = G, aggs = A, having = H, input = In} = N, Ctx) ->
    N#p_agg{group_by = [F(E) || E <- G],
            aggs = [Ag#agg{arg = F(Ag#agg.arg)} || Ag <- A],
            having = F(H), input = map_node_exprs(F, In, Ctx)};
map_node_exprs(F, #p_sort{keys = K, input = In} = N, Ctx) ->
    N#p_sort{keys = [{F(E), D, Nu} || {E, D, Nu} <- K],
             input = map_node_exprs(F, In, Ctx)};
map_node_exprs(F, #p_limit{input = In} = N, Ctx) ->
    N#p_limit{input = map_node_exprs(F, In, Ctx)};
map_node_exprs(F, #p_distinct{input = In} = N, Ctx) ->
    N#p_distinct{input = map_node_exprs(F, In, Ctx)};
map_node_exprs(F, #p_project{exprs = E, input = In} = N, Ctx) ->
    N#p_project{exprs = [F(X) || X <- E], input = map_node_exprs(F, In, Ctx)};
map_node_exprs(F, #p_derived{input = In} = N, Ctx) ->
    N#p_derived{input = map_node_exprs(F, In, Ctx)};
map_node_exprs(F, #p_setop{left = L, right = R} = N, Ctx) ->
    N#p_setop{left = map_node_exprs(F, L, Ctx), right = map_node_exprs(F, R, Ctx)};
map_node_exprs(_F, Leaf, _Ctx) ->
    Leaf.

%%%===================================================================
%%% 集合演算
%%%
%%% 重複の判定は sql_value:group_key/1 を通す。DISTINCT と同じで、
%%% NULL同士は同じとみなし、100 と 100.0 も同じ扱いにする。
%%% `=` の意味論(NULL = NULL は unknown)とは別であることに注意。
%%%===================================================================

set_op('union', true, L, R) ->
    L ++ R;
set_op('union', false, L, R) ->
    dedup(L ++ R);
set_op(intersect, All, L, R) ->
    match_op(L, counts(R), All, intersect, [], #{});
set_op(except, All, L, R) ->
    match_op(L, counts(R), All, except, [], #{}).

%% 右側の重複度を数える。
counts(Rows) ->
    lists:foldl(fun(Row, Acc) ->
                        maps:update_with(key(Row), fun(N) -> N + 1 end, 1, Acc)
                end, #{}, Rows).

%%----------------------------------------------------------------------
%% 左を順に見て、右にあるかどうかで通す/落とす。
%%
%% ALL のときは**左の1行が右の1行を打ち消す**。消費しないと重複度が狂う。
%%
%%   a = [1, 2, 2, 3]、b = [2] のとき
%%     INTERSECT ALL  → [2]        右の2は1つしか無いので1回だけ一致
%%     EXCEPT ALL     → [1, 2, 3]  右の2が左の2を1つだけ打ち消す
%%
%% ALL でなければ Seen で重複を落とす。
%%----------------------------------------------------------------------
match_op([], _Counts, _All, _Op, Acc, _Seen) ->
    lists:reverse(Acc);
match_op([Row | T], Counts, All, Op, Acc, Seen) ->
    K = key(Row),
    N = maps:get(K, Counts, 0),
    Matched = N > 0,
    Counts1 = case All andalso Matched of
                  true  -> Counts#{K => N - 1};
                  false -> Counts
              end,
    Emit = case Op of
               intersect -> Matched;
               except    -> not Matched
           end,
    case Emit andalso (All orelse not maps:is_key(K, Seen)) of
        true  -> match_op(T, Counts1, All, Op, [Row | Acc], Seen#{K => []});
        false -> match_op(T, Counts1, All, Op, Acc, Seen)
    end.

dedup(Rows) ->
    {Out, _} = lists:foldl(
                 fun(Row, {Acc, Seen}) ->
                         K = key(Row),
                         case maps:is_key(K, Seen) of
                             true  -> {Acc, Seen};
                             false -> {[Row | Acc], Seen#{K => []}}
                         end
                 end, {[], #{}}, Rows),
    lists:reverse(Out).

key(Row) -> [sql_value:group_key(V) || V <- to_list(Row)].

to_list(Row) when is_list(Row)  -> Row;
to_list(Row) when is_tuple(Row) -> tuple_to_list(Row).

to_tuple(Row) when is_list(Row)  -> list_to_tuple(Row);
to_tuple(Row) when is_tuple(Row) -> Row.

%%%===================================================================
%%% ハッシュ結合
%%%===================================================================

%% 右側の行を鍵で束ねる。
%%
%% **鍵にNULLを含む行は入れない。** NULL = NULL は unknown なので
%% 決して一致しない。ハッシュ表に入れると NULL 同士が衝突して
%% 一致してしまう。
build_hash(Rows, Keys, Env) ->
    Table = lists:foldl(
              fun({_I, Row} = P, Acc) ->
                      case join_key(Keys, Row, Env) of
                          null -> Acc;
                          K    -> maps:update_with(K, fun(L) -> [P | L] end,
                                                   [P], Acc)
                      end
              end, #{}, Rows),
    %% 入れ子ループと同じ順序で返すために積み直す
    maps:map(fun(_K, V) -> lists:reverse(V) end, Table).

%% 鍵の値。NULLが1つでもあれば null(一致しない)。
%%
%% 値は sql_value:group_key/1 で正規化する。素の項を鍵にすると
%% 100 と 100.0 が別の鍵になるが、SQLの `=` では等しい。
join_key(Exprs, Row, Env) -> join_key(Exprs, Row, Env, []).

join_key([], _Row, _Env, Acc) ->
    lists:reverse(Acc);
join_key([E | T], Row, Env, Acc) ->
    case sql_expr:eval(E, Row, Env) of
        null -> null;
        V    -> join_key(T, Row, Env, [sql_value:group_key(V) | Acc])
    end.

%% 状態遷移は入れ子ループと同じ。違うのは、右側の候補を
%% 全件ではなくハッシュ表から取ってくるところだけ。
hj_next(Op, #{stage := drain, rest := []} = St, _Env) ->
    {eof, Op#op{st = St}};
hj_next(Op, #{stage := drain, rest := [{_I, R} | Rest], lwidth := LW} = St, _Env) ->
    {row, pad_left(R, LW), Op#op{st = St#{rest => Rest}}};
hj_next(Op, #{cur := undefined, left := Left, lkeys := LK, table := Tab,
              type := Type} = St, Env) ->
    case next(Left) of
        {eof, Left2} ->
            St1 = St#{left => Left2},
            case outer_right(Type) of
                true  -> hj_next(Op, St1#{stage => drain, rest => unmatched(St1)}, Env);
                false -> {eof, Op#op{st = St1}}
            end;
        {row, Row, Left2} ->
            %% 鍵がNULLの左の行は決して一致しない。
            %% 右側も、鍵がNULLの行はハッシュ表に入っていないので
            %% 未一致として drain で出る
            Matches = case join_key(LK, Row, Env) of
                          null -> [];
                          K    -> maps:get(K, Tab, [])
                      end,
            hj_next(Op, St#{left => Left2, cur => Row,
                            rest => Matches, matched => false}, Env)
    end;
hj_next(Op, #{rest := [], type := Type, matched := false,
              cur := Cur, rwidth := RW} = St, _Env) when Type =:= left; Type =:= full ->
    {row, pad_right(Cur, RW), Op#op{st = St#{cur => undefined, matched => true}}};
hj_next(Op, #{rest := []} = St, Env) ->
    hj_next(Op, St#{cur => undefined}, Env);
hj_next(Op, #{rest := [{I, R} | Rest], cur := Cur, pred := Pred, seen := Seen} = St, Env) ->
    Joined = concat_rows(Cur, R),
    case sql_expr:eval_pred(Pred, Joined, Env) of
        true  -> {row, Joined, Op#op{st = St#{rest => Rest, matched => true,
                                              seen => Seen#{I => []}}}};
        false -> hj_next(Op, St#{rest => Rest}, Env)
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
%%% 集約
%%%===================================================================

%% グループごとにまとめて [キー..., 集約結果...] の行を返す。
%%
%% グループの出現順を保つ。SQLに順序の保証は無いが、決まっていないと
%% テストが書けないため。
aggregate([], Aggs, Rows, Env) ->
    %% GROUP BY が無い場合は全体で1グループ。
    %% 入力が空でも1行返る(COUNT(*) が 0 を返すため)。
    [[finish(A, acc_rows(A, Rows, Env)) || A <- Aggs]];
aggregate(Keys, Aggs, Rows, Env) ->
    Grouped = group_rows(Keys, Rows, Env),
    [KeyVals ++ [finish(A, acc_rows(A, GroupRows, Env)) || A <- Aggs]
     || {KeyVals, GroupRows} <- Grouped].

group_rows(Keys, Rows, Env) ->
    {Order, Map} =
        lists:foldl(
          fun(Row, {Ord, M}) ->
                  Vals = [sql_expr:eval(K, Row, Env) || K <- Keys],
                  %% グルーピングのキーは group_key/1 を通す。
                  %% NULL同士は同じ組、100 と 100.0 も同じ組にする。
                  GKey = [sql_value:group_key(V) || V <- Vals],
                  case maps:is_key(GKey, M) of
                      true -> {Ord, maps:update_with(GKey, fun({V, Rs}) -> {V, [Row | Rs]} end, M)};
                      false -> {[GKey | Ord], M#{GKey => {Vals, [Row]}}}
                  end
          end, {[], #{}}, Rows),
    [begin {Vals, Rs} = maps:get(K, Map), {Vals, lists:reverse(Rs)} end
     || K <- lists:reverse(Order)].

%% そのグループの行から、集約に使う値を取り出す。
acc_rows(#agg{func = count_star}, Rows, _Env) ->
    length(Rows);
acc_rows(#agg{arg = Arg, distinct = Dist}, Rows, Env) ->
    %% NULLは集約の対象から外す(COUNT(*) 以外はすべてこの規則)
    Vals = [V || Row <- Rows, (V = sql_expr:eval(Arg, Row, Env)) =/= null],
    case Dist of
        false -> Vals;
        true -> distinct_values(Vals)
    end.

distinct_values(Vals) ->
    {_, Out} = lists:foldl(fun(V, {Seen, Acc}) ->
                                   K = sql_value:group_key(V),
                                   case sets:is_element(K, Seen) of
                                       true -> {Seen, Acc};
                                       false -> {sets:add_element(K, Seen), [V | Acc]}
                                   end
                           end, {sets:new([{version, 2}]), []}, Vals),
    lists:reverse(Out).

%% 空集合の扱い: COUNT は 0、それ以外は NULL。
%% 「グループが空でないのに SUM が NULL」という非対称はSQLの規則。
finish(#agg{func = count_star}, N) -> N;
finish(#agg{func = count}, Vals) -> length(Vals);
finish(#agg{func = sum}, []) -> null;
finish(#agg{func = sum}, Vals) -> lists:foldl(fun(V, A) -> sql_value:arith('+', A, V) end,
                                              hd(Vals), tl(Vals));
finish(#agg{func = avg}, []) -> null;
finish(#agg{func = avg}, Vals) ->
    Sum = lists:foldl(fun(V, A) -> sql_value:arith('+', A, V) end, hd(Vals), tl(Vals)),
    sql_value:arith('/', Sum, length(Vals));
finish(#agg{func = min}, []) -> null;
finish(#agg{func = min}, Vals) -> extreme(lt, Vals);
finish(#agg{func = max}, []) -> null;
finish(#agg{func = max}, Vals) -> extreme(gt, Vals).

%% 比較は sql_value:compare/2 を通す。Erlangの項順序では型をまたぐと
%% SQLの順序と食い違う。
extreme(Want, [H | T]) ->
    lists:foldl(fun(V, Best) ->
                        case sql_value:compare(V, Best) of
                            Want -> V;
                            _ -> Best
                        end
                end, H, T).

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
close(#op{kind = nl_join, st = #{left := Left}}) ->
    close(Left);
close(#op{kind = hash_join, st = #{left := Left}}) ->
    close(Left);
close(#op{kind = derived, st = Child}) ->
    close(Child);
close({error, _}) ->
    ok.
