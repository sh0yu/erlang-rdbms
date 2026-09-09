%%%-------------------------------------------------------------------
%%% @doc
%%% B+treeインデックス。カラム値からオブジェクトIDを引く。
%%% simple_indexと違い、キーが順序を保って並ぶため範囲検索ができる。
%%%
%%% ノードはETS上に {Pointer, #node{}} として置く。
%%% 木の根は {ColName, root, Pointer} として同じETSに置く。
%%%
%%%   #node{type=leaf,     page=[#item{val=Key, pointer=[Oid, ...]}, ...]}
%%%   #node{type=non_leaf, page=[#item{val=nil, pointer=[C0]},
%%%                             #item{val=K1,  pointer=[C1]}, ...]}
%%%
%%% 内部ノードのitemのvalは、その子が持つキーの下限を表す。
%%% 先頭itemのvalは nil で、下限なし(-∞)を意味する。
%%% よって子Ciは Ki =< Key < K(i+1) のキーを持つ。
%%%
%%% 1ノードのitem数は最大 ?MAX (= 2 * ?E)、根以外は最小 ?E を保つ。
%%% 超えたら分割し、下回ったら隣から借りるか隣と併合する。
%%%
%%% 葉は left_node / right_node で双方向に繋がっており、
%%% 範囲検索(select_range/4)は葉を右にたどるだけで済む。
%%% @end
%%%-------------------------------------------------------------------
-module(index).

%% Public API
-export([init/0, create_table/2, drop_table/2, add_column/2]).
-export([create_index/2, drop_index/2, exist_index/2]).
-export([insert_index/3, delete_index/4, update_index/5, select_index/3,
         select_range/4]).
-export([get_index_ets/1, get_tree_top/2]).

%% デバッグ・検証用
-export([print_tree/2, to_list/2, validate/2]).

-include("../include/simple_db_server.hrl").

-record(item, {
    val,
    pointer
}).
-record(node, {
    type = non_leaf,
    page = [],
    left_node = nil,
    right_node = nil
}).

%% ノードあたりのitem数の下限。上限はその2倍。
-define(E, 2).
-define(MAX, (?E * 2)).

%%%===================================================================
%%% Public APIs
%%%===================================================================

init() ->
    case ets:info(ms_index) of
        undefined -> ets:new(ms_index, [set, named_table, public]);
        _ -> ms_index
    end,
    ok.

create_table(TableName, ColNameList) ->
    _ = add_table(TableName),
    lists:foreach(fun(ColName) -> add_column(TableName, ColName) end, ColNameList),
    ok.

%%----------------------------------------------------------------------
%% @doc カラム1つぶんの索引を作る / 落とす。simple_index と同じ口。
%%
%% drop_index は木の根を消すだけで、ぶら下がっていた節はテーブルの
%% ETS に残る。テーブルを落とすまで回収されない(既知の割り切り)。
%%----------------------------------------------------------------------
create_index(TableName, ColName) ->
    _ = add_table(TableName),
    _ = add_column(TableName, ColName),
    ok.

drop_index(TableName, ColName) ->
    case ets:lookup(ms_index, TableName) of
        [] -> ok;
        [{TableName, IndexName}] ->
            case ets:info(IndexName) of
                undefined -> ok;
                _ -> ets:delete(IndexName, ColName), ok
            end
    end.

exist_index(TableName, ColName) ->
    case ets:lookup(ms_index, TableName) of
        [] -> false;
        [{TableName, IndexName}] ->
            ets:info(IndexName) =/= undefined
                andalso get_tree_top(IndexName, ColName) =/= not_found
    end.

drop_table(TableName, _ColNameList) ->
    case ets:lookup(ms_index, TableName) of
        [] ->
            ok;
        [{TableName, IndexName}] ->
            case ets:info(IndexName) of
                undefined -> ok;
                _ -> ets:delete(IndexName)
            end,
            ets:delete(ms_index, TableName),
            ok
    end.

%%----------------------------------------------------------------------
%% @doc 1行分のインデックスを追加する。
%% ColNameVal : [{ColumnName, Val}, ...]
%%----------------------------------------------------------------------
insert_index(TableName, ColNameVal, Oid) ->
    IndexName = get_index_ets(TableName),
    lists:foreach(fun({ColName, Val}) -> insert(IndexName, ColName, Val, Oid) end, ColNameVal),
    ok.

delete_index(TableName, ColName, Val, Oid) ->
    IndexName = get_index_ets(TableName),
    delete(IndexName, ColName, Val, Oid),
    ok.

update_index(_TableName, _ColName, OldVal, NewVal, _Oid) when OldVal =:= NewVal ->
    ok;
update_index(TableName, ColName, OldVal, NewVal, Oid) ->
    IndexName = get_index_ets(TableName),
    delete(IndexName, ColName, OldVal, Oid),
    insert(IndexName, ColName, NewVal, Oid),
    ok.

%%----------------------------------------------------------------------
%% @doc キーに対応するオブジェクトIDのリストを返す。
%%----------------------------------------------------------------------
select_index(TableName, ColName, Val) ->
    IndexName = get_index_ets(TableName),
    case get_tree_top(IndexName, ColName) of
        not_found -> [];
        TreeTop -> search(IndexName, Val, TreeTop)
    end.

%%----------------------------------------------------------------------
%% @doc From =< Key =< To の範囲のオブジェクトIDをキー順に返す。
%% 葉を右にたどるだけなので、木を何度も下る必要がない。
%%----------------------------------------------------------------------
select_range(TableName, ColName, From, To) ->
    IndexName = get_index_ets(TableName),
    case get_tree_top(IndexName, ColName) of
        not_found ->
            [];
        TreeTop ->
            LeafPointer = descend_to_leaf(IndexName, From, TreeTop),
            scan_leaves(IndexName, LeafPointer, From, To, [])
    end.

%%%===================================================================
%%% テーブル・カラムの登録
%%%===================================================================

get_index_ets(TableName) ->
    case ets:lookup(ms_index, TableName) of
        [] -> add_table(TableName);
        [{TableName, IndexName}] -> IndexName
    end.

add_table(TableName) ->
    IndexName = list_to_atom(atom_to_list(TableName) ++ "_index"),
    case ets:info(IndexName) of
        undefined -> ets:new(IndexName, [set, named_table, public]);
        _ -> IndexName
    end,
    ets:insert(ms_index, {TableName, IndexName}),
    IndexName.

%% カラムごとに、空の葉1つだけの木を用意する。
add_column(TableName, ColName) ->
    IndexName = get_index_ets(TableName),
    TreeTop = new_pointer(),
    ets:insert(IndexName, {TreeTop, #node{type = leaf, page = []}}),
    change_tree_top(IndexName, ColName, TreeTop),
    TreeTop.

get_tree_top(IndexName, ColName) ->
    case ets:lookup(IndexName, ColName) of
        [] -> not_found;
        [{ColName, root, Pointer}] -> Pointer
    end.

change_tree_top(IndexName, ColName, Pointer) ->
    ets:insert(IndexName, {ColName, root, Pointer}).

%%%===================================================================
%%% 検索
%%%===================================================================

%% キーに一致する葉のitemのOidリストを返す。
search(IndexName, Search, Pointer) ->
    LeafPointer = descend_to_leaf(IndexName, Search, Pointer),
    #node{page = Page} = get_node(IndexName, LeafPointer),
    case lists:keyfind(Search, #item.val, Page) of
        false -> [];
        #item{pointer = OidList} -> OidList
    end.

%% 根から葉まで下り、Searchが入るべき葉のポインタを返す。
descend_to_leaf(IndexName, Search, Pointer) ->
    case get_node(IndexName, Pointer) of
        #node{type = leaf} ->
            Pointer;
        #node{type = non_leaf, page = Page} ->
            descend_to_leaf(IndexName, Search, child_for_key(Page, Search))
    end.

%% 内部ノードのpageから、Searchを含む子のポインタを選ぶ。
%% 「valがSearch以下である最後のitem」の子が答え。
child_for_key([#item{pointer = [Child]}], _Search) ->
    Child;
child_for_key([#item{pointer = [Child]}, #item{val = NextVal} = Next | Rest], Search) ->
    case NextVal =/= nil andalso key_le(NextVal, Search) of
        %% 次のitemの下限もSearch以下なので、さらに右を見る
        true -> child_for_key([Next | Rest], Search);
        %% Searchは次の下限に届かないので、この子が持ち主
        false -> Child
    end.

%% nil は下限なし(-∞)を表す。Erlangの項順序ではアトムが数値より
%% 大きいため、nilの比較は明示的に扱う必要がある。
key_le(nil, _Search) -> true;
key_le(Val, Search) -> Val =< Search.

%% 葉を右にたどって範囲内のOidを集める。
scan_leaves(_IndexName, nil, _From, _To, Acc) ->
    lists:append(lists:reverse(Acc));
scan_leaves(IndexName, Pointer, From, To, Acc) ->
    #node{page = Page, right_node = Right} = get_node(IndexName, Pointer),
    %% 1つのキーに複数のOidがぶら下がるので、キーごとのリストを平らにする
    InRange = lists:append([OidList || #item{val = V, pointer = OidList} <- Page,
                                       V >= From, V =< To]),
    case lists:any(fun(#item{val = V}) -> V > To end, Page) of
        %% Toを超えるキーが出たらこれ以上右を見る必要はない
        true -> lists:append(lists:reverse([InRange | Acc]));
        false -> scan_leaves(IndexName, Right, From, To, [InRange | Acc])
    end.

%%%===================================================================
%%% 挿入
%%%===================================================================

insert(IndexName, ColName, Val, Oid) ->
    TreeTop = case get_tree_top(IndexName, ColName) of
                  not_found -> add_column_for(IndexName, ColName);
                  Top -> Top
              end,
    Path = path_to_leaf(IndexName, Val, TreeTop, []),
    [Leaf | Parents] = Path,
    #node{page = Page} = Node = get_node(IndexName, Leaf),
    NewPage = insert_item(Page, Val, Oid),
    put_node(IndexName, Leaf, Node#node{page = NewPage}),
    maybe_split(IndexName, ColName, Leaf, Parents).

add_column_for(IndexName, ColName) ->
    TreeTop = new_pointer(),
    ets:insert(IndexName, {TreeTop, #node{type = leaf, page = []}}),
    change_tree_top(IndexName, ColName, TreeTop),
    TreeTop.

%% 根から葉までのポインタを、葉が先頭になる順で返す。
path_to_leaf(IndexName, Search, Pointer, Acc) ->
    case get_node(IndexName, Pointer) of
        #node{type = leaf} ->
            [Pointer | Acc];
        #node{type = non_leaf, page = Page} ->
            path_to_leaf(IndexName, Search, child_for_key(Page, Search), [Pointer | Acc])
    end.

%% 葉のitemをキー昇順に保ったまま挿入する。
%% 同じキーが既にあればOidを追加する。
insert_item([], Val, Oid) ->
    [#item{val = Val, pointer = [Oid]}];
insert_item([#item{val = V, pointer = O} = I | T], Val, Oid) when V =:= Val ->
    case lists:member(Oid, O) of
        true -> [I | T];
        false -> [I#item{pointer = [Oid | O]} | T]
    end;
insert_item([#item{val = V} = I | T], Val, Oid) when V < Val ->
    [I | insert_item(T, Val, Oid)];
insert_item(Page, Val, Oid) ->
    [#item{val = Val, pointer = [Oid]} | Page].

%% ノードが上限を超えていたら分割し、親へ繰り上げる。
maybe_split(IndexName, ColName, Pointer, Parents) ->
    #node{page = Page} = Node = get_node(IndexName, Pointer),
    case length(Page) > ?MAX of
        false ->
            ok;
        true ->
            {LeftPage, SepKey, RightPage} = split_page(Node#node.type, Page),
            RightPointer = new_pointer(),
            split_nodes(IndexName, Pointer, Node, LeftPage, RightPointer, RightPage),
            promote(IndexName, ColName, Pointer, SepKey, RightPointer, Parents)
    end.

%% 葉の分割: 右ノードの先頭キーを親へのセパレータにする。
%% 葉には全てのキーが残るので、セパレータは複製される。
split_page(leaf, Page) ->
    Mid = length(Page) div 2,
    {Left, Right} = lists:split(Mid, Page),
    [#item{val = SepKey} | _] = Right,
    {Left, SepKey, Right};
%% 内部ノードの分割: 中央itemのキーは親へ移動する。
%% そのitemの子は右ノードの先頭(下限なし)になる。
split_page(non_leaf, Page) ->
    Mid = length(Page) div 2,
    {Left, [#item{val = SepKey} = MidItem | Rest]} = lists:split(Mid, Page),
    {Left, SepKey, [MidItem#item{val = nil} | Rest]}.

%% 分割した2つのノードを書き戻し、葉なら兄弟リンクを繋ぎ直す。
split_nodes(IndexName, Pointer, #node{type = leaf, right_node = OldRight} = Node,
            LeftPage, RightPointer, RightPage) ->
    put_node(IndexName, Pointer, Node#node{page = LeftPage, right_node = RightPointer}),
    put_node(IndexName, RightPointer, #node{type = leaf, page = RightPage,
                                            left_node = Pointer, right_node = OldRight}),
    relink_left(IndexName, OldRight, RightPointer);
split_nodes(IndexName, Pointer, #node{type = non_leaf} = Node,
            LeftPage, RightPointer, RightPage) ->
    put_node(IndexName, Pointer, Node#node{page = LeftPage}),
    put_node(IndexName, RightPointer, #node{type = non_leaf, page = RightPage}).

relink_left(_IndexName, nil, _NewLeft) ->
    ok;
relink_left(IndexName, Pointer, NewLeft) ->
    Node = get_node(IndexName, Pointer),
    put_node(IndexName, Pointer, Node#node{left_node = NewLeft}),
    ok.

%% 分割で生まれた右ノードを親に登録する。親がなければ木が1段伸びる。
promote(IndexName, ColName, LeftPointer, SepKey, RightPointer, []) ->
    NewRoot = new_pointer(),
    put_node(IndexName, NewRoot,
             #node{type = non_leaf,
                   page = [#item{val = nil, pointer = [LeftPointer]},
                           #item{val = SepKey, pointer = [RightPointer]}]}),
    change_tree_top(IndexName, ColName, NewRoot);
promote(IndexName, ColName, LeftPointer, SepKey, RightPointer, [Parent | Grandparents]) ->
    #node{page = Page} = Node = get_node(IndexName, Parent),
    NewPage = insert_child(Page, LeftPointer, SepKey, RightPointer),
    put_node(IndexName, Parent, Node#node{page = NewPage}),
    maybe_split(IndexName, ColName, Parent, Grandparents).

%% 親のpageで、LeftPointerを指すitemの直後にRightPointerのitemを挿む。
insert_child([#item{pointer = [LeftPointer]} = I | T], LeftPointer, SepKey, RightPointer) ->
    [I, #item{val = SepKey, pointer = [RightPointer]} | T];
insert_child([H | T], LeftPointer, SepKey, RightPointer) ->
    [H | insert_child(T, LeftPointer, SepKey, RightPointer)].

%%%===================================================================
%%% 削除
%%%===================================================================

delete(IndexName, ColName, Val, Oid) ->
    case get_tree_top(IndexName, ColName) of
        not_found ->
            ok;
        TreeTop ->
            [Leaf | Parents] = path_to_leaf(IndexName, Val, TreeTop, []),
            #node{page = Page} = Node = get_node(IndexName, Leaf),
            case delete_item(Page, Val, Oid) of
                Page ->
                    %% 消すものがなかった
                    ok;
                NewPage ->
                    put_node(IndexName, Leaf, Node#node{page = NewPage}),
                    rebalance(IndexName, ColName, Leaf, Parents)
            end
    end.

%% 葉のitemからOidを外す。Oidがなくなったitemごと消す。
delete_item([], _Val, _Oid) ->
    [];
delete_item([#item{val = Val, pointer = OidList} = I | T], Val, Oid) ->
    case lists:filter(fun(X) -> X =/= Oid end, OidList) of
        [] -> T;
        OidList -> [I | T];
        Rest -> [I#item{pointer = Rest} | T]
    end;
delete_item([H | T], Val, Oid) ->
    [H | delete_item(T, Val, Oid)].

%% 下限を割ったノードを、隣から借りるか隣と併合して直す。
rebalance(IndexName, ColName, Pointer, []) ->
    %% 根の場合。内部ノードでitemが1つだけになったら木が1段縮む。
    #node{type = Type, page = Page} = get_node(IndexName, Pointer),
    case {Type, Page} of
        {non_leaf, [#item{pointer = [OnlyChild]}]} ->
            ets:delete(IndexName, Pointer),
            change_tree_top(IndexName, ColName, OnlyChild),
            ok;
        _ ->
            ok
    end;
rebalance(IndexName, ColName, Pointer, [Parent | Grandparents]) ->
    #node{page = Page} = get_node(IndexName, Pointer),
    case length(Page) >= ?E of
        true ->
            %% 下限を満たしているので、親のセパレータだけ直せばよい
            fix_separator(IndexName, Parent, Pointer),
            ok;
        false ->
            #node{page = ParentPage} = get_node(IndexName, Parent),
            case siblings(ParentPage, Pointer) of
                {Left, Right} ->
                    handle_underflow(IndexName, ColName, Pointer, Parent, Grandparents,
                                     Left, Right)
            end
    end.

handle_underflow(IndexName, ColName, Pointer, Parent, Grandparents, Left, Right) ->
    case can_lend(IndexName, Left) of
        true ->
            borrow_from_left(IndexName, Pointer, Parent, Left),
            ok;
        false ->
            case can_lend(IndexName, Right) of
                true ->
                    borrow_from_right(IndexName, Pointer, Parent, Right),
                    ok;
                false ->
                    %% 借りられないので併合する。併合すると親のitemが減るので、
                    %% 親も下限を割っていないか上へたどって確かめる。
                    case Left of
                        nil -> merge_nodes(IndexName, Pointer, Right, Parent);
                        _ -> merge_nodes(IndexName, Left, Pointer, Parent)
                    end,
                    rebalance(IndexName, ColName, Parent, Grandparents)
            end
    end.

can_lend(_IndexName, nil) ->
    false;
can_lend(IndexName, Pointer) ->
    #node{page = Page} = get_node(IndexName, Pointer),
    length(Page) > ?E.

%% 親のpageから、Pointerの左右の兄弟を返す。
siblings(ParentPage, Pointer) ->
    Children = [C || #item{pointer = [C]} <- ParentPage],
    siblings_1(Children, nil, Pointer).

siblings_1([], _Prev, _Pointer) ->
    {nil, nil};
siblings_1([Pointer], Prev, Pointer) ->
    {Prev, nil};
siblings_1([Pointer, Next | _], Prev, Pointer) ->
    {Prev, Next};
siblings_1([H | T], _Prev, Pointer) ->
    siblings_1(T, H, Pointer).

%% 左の兄弟から末尾のitemを1つもらう。
borrow_from_left(IndexName, Pointer, Parent, Left) ->
    #node{type = Type, page = LeftPage} = LeftNode = get_node(IndexName, Left),
    #node{page = Page} = Node = get_node(IndexName, Pointer),
    {LeftRest, [Moved]} = lists:split(length(LeftPage) - 1, LeftPage),
    put_node(IndexName, Left, LeftNode#node{page = LeftRest}),
    NewPage = case Type of
                  leaf ->
                      [Moved | Page];
                  non_leaf ->
                      %% 借りたitemが新しい先頭になるので下限なしにし、
                      %% 元の先頭には親のセパレータを書き戻す
                      [#item{val = FirstVal} = First | Rest] = Page,
                      SepKey = separator_of(IndexName, Parent, Pointer),
                      _ = FirstVal,
                      [Moved#item{val = nil}, First#item{val = SepKey} | Rest]
              end,
    put_node(IndexName, Pointer, Node#node{page = NewPage}),
    fix_separator(IndexName, Parent, Pointer),
    ok.

%% 右の兄弟から先頭のitemを1つもらう。
borrow_from_right(IndexName, Pointer, Parent, Right) ->
    #node{type = Type, page = RightPage} = RightNode = get_node(IndexName, Right),
    #node{page = Page} = Node = get_node(IndexName, Pointer),
    [Moved | RightRest] = RightPage,
    NewPage = case Type of
                  leaf ->
                      Page ++ [Moved];
                  non_leaf ->
                      %% 右の先頭は下限なしなので、親のセパレータを補って移す
                      SepKey = separator_of(IndexName, Parent, Right),
                      Page ++ [Moved#item{val = SepKey}]
              end,
    NewRightPage = case Type of
                       leaf ->
                           RightRest;
                       non_leaf ->
                           [Next | Others] = RightRest,
                           [Next#item{val = nil} | Others]
                   end,
    put_node(IndexName, Pointer, Node#node{page = NewPage}),
    put_node(IndexName, Right, RightNode#node{page = NewRightPage}),
    fix_separator(IndexName, Parent, Pointer),
    fix_separator(IndexName, Parent, Right),
    ok.

%% LeftにRightを取り込み、Rightを親から外して削除する。
merge_nodes(IndexName, Left, Right, Parent) ->
    #node{type = Type, page = LeftPage} = LeftNode = get_node(IndexName, Left),
    #node{page = RightPage, right_node = RightsRight} = get_node(IndexName, Right),
    MergedPage = case Type of
                     leaf ->
                         LeftPage ++ RightPage;
                     non_leaf ->
                         %% 右の先頭itemの下限は親のセパレータだった
                         SepKey = separator_of(IndexName, Parent, Right),
                         [First | Others] = RightPage,
                         LeftPage ++ [First#item{val = SepKey} | Others]
                 end,
    NewLeft = case Type of
                  leaf -> LeftNode#node{page = MergedPage, right_node = RightsRight};
                  non_leaf -> LeftNode#node{page = MergedPage}
              end,
    put_node(IndexName, Left, NewLeft),
    case Type of
        leaf -> relink_left(IndexName, RightsRight, Left);
        non_leaf -> ok
    end,
    ets:delete(IndexName, Right),
    remove_child(IndexName, Parent, Right),
    fix_separator(IndexName, Parent, Left),
    ok.

remove_child(IndexName, Parent, Child) ->
    #node{page = Page} = Node = get_node(IndexName, Parent),
    NewPage = [I || #item{pointer = [C]} = I <- Page, C =/= Child],
    %% 先頭になったitemは下限なしに直す
    put_node(IndexName, Parent, Node#node{page = normalize_first(NewPage)}),
    ok.

normalize_first([]) -> [];
normalize_first([First | Rest]) -> [First#item{val = nil} | Rest].

%% 親が持つ子Pointerのセパレータ(その子のキーの下限)を返す。
separator_of(IndexName, Parent, Pointer) ->
    #node{page = Page} = get_node(IndexName, Parent),
    case lists:keyfind([Pointer], #item.pointer, Page) of
        false -> nil;
        #item{val = Val} -> Val
    end.

%% 子の中身が変わった後、親が持つその子のセパレータを実際の最小キーに直す。
%% 先頭の子は常に下限なしなので触らない。
fix_separator(IndexName, Parent, Pointer) ->
    #node{page = Page} = Node = get_node(IndexName, Parent),
    case Page of
        [#item{pointer = [Pointer]} | _] ->
            %% 先頭の子は下限なしのまま
            ok;
        _ ->
            case min_key(IndexName, Pointer) of
                none ->
                    ok;
                MinKey ->
                    NewPage = [case I of
                                   #item{pointer = [P]} when P =:= Pointer ->
                                       I#item{val = MinKey};
                                   _ ->
                                       I
                               end || I <- Page],
                    put_node(IndexName, Parent, Node#node{page = NewPage}),
                    ok
            end
    end.

%% 部分木が持つ最小のキー。
min_key(IndexName, Pointer) ->
    case get_node(IndexName, Pointer) of
        #node{type = leaf, page = []} -> none;
        #node{type = leaf, page = [#item{val = Val} | _]} -> Val;
        #node{type = non_leaf, page = []} -> none;
        #node{type = non_leaf, page = [#item{pointer = [Child]} | _]} ->
            min_key(IndexName, Child)
    end.

%%%===================================================================
%%% ノードの入出力
%%%===================================================================

get_node(IndexName, Pointer) ->
    case ets:lookup(IndexName, Pointer) of
        [{Pointer, #node{} = Node}] -> Node;
        [] -> error({index_node_not_found, IndexName, Pointer})
    end.

put_node(IndexName, Pointer, #node{} = Node) ->
    ets:insert(IndexName, {Pointer, Node}).

new_pointer() ->
    erlang:unique_integer([monotonic, positive]).

%%%===================================================================
%%% デバッグ・検証用
%%%===================================================================

%% 木の全 {Key, OidList} をキー順に返す。
to_list(IndexName, ColName) ->
    case get_tree_top(IndexName, ColName) of
        not_found ->
            [];
        TreeTop ->
            Leaf = leftmost_leaf(IndexName, TreeTop),
            collect_leaves(IndexName, Leaf, [])
    end.

leftmost_leaf(IndexName, Pointer) ->
    case get_node(IndexName, Pointer) of
        #node{type = leaf} -> Pointer;
        #node{type = non_leaf, page = [#item{pointer = [Child]} | _]} ->
            leftmost_leaf(IndexName, Child)
    end.

collect_leaves(_IndexName, nil, Acc) ->
    lists:append(lists:reverse(Acc));
collect_leaves(IndexName, Pointer, Acc) ->
    #node{page = Page, right_node = Right} = get_node(IndexName, Pointer),
    Items = [{Val, lists:sort(OidList)} || #item{val = Val, pointer = OidList} <- Page],
    collect_leaves(IndexName, Right, [Items | Acc]).

%%----------------------------------------------------------------------
%% @doc 木の構造が不変条件を満たしているか確かめる。
%% Returns: ok | {error, Reason}
%%----------------------------------------------------------------------
validate(IndexName, ColName) ->
    case get_tree_top(IndexName, ColName) of
        not_found ->
            ok;
        TreeTop ->
            try
                {_Depth, _Min} = validate_node(IndexName, TreeTop, true),
                Keys = [K || {K, _} <- to_list(IndexName, ColName)],
                case Keys =:= lists:sort(Keys) of
                    true -> ok;
                    false -> {error, {keys_not_sorted, Keys}}
                end
            catch
                throw:Reason -> {error, Reason}
            end
    end.

validate_node(IndexName, Pointer, IsRoot) ->
    case get_node(IndexName, Pointer) of
        #node{type = leaf, page = Page} ->
            check_min_items(Pointer, Page, IsRoot, leaf),
            check_max_items(Pointer, Page),
            {1, first_key(Page)};
        #node{type = non_leaf, page = Page} ->
            check_min_items(Pointer, Page, IsRoot, non_leaf),
            check_max_items(Pointer, Page),
            case Page of
                [#item{val = nil} | _] -> ok;
                _ -> throw({first_item_must_be_nil, Pointer})
            end,
            Results = [validate_node(IndexName, C, false) || #item{pointer = [C]} <- Page],
            Depths = lists:usort([D || {D, _} <- Results]),
            case Depths of
                [D] -> {D + 1, element(2, hd(Results))};
                _ -> throw({unbalanced, Pointer, Depths})
            end
    end.

%% 根は下限を免除される。木全体が空になることがあるため。
check_min_items(_Pointer, _Page, true, _Type) ->
    ok;
check_min_items(Pointer, Page, false, _Type) ->
    case length(Page) >= ?E of
        true -> ok;
        false -> throw({underflow, Pointer, length(Page)})
    end.

check_max_items(Pointer, Page) ->
    case length(Page) =< ?MAX of
        true -> ok;
        false -> throw({overflow, Pointer, length(Page)})
    end.

first_key([]) -> none;
first_key([#item{val = Val} | _]) -> Val.

print_tree(IndexName, Pointer) ->
    print_tree(IndexName, Pointer, 0).

print_tree(IndexName, Pointer, Depth) ->
    Indent = lists:duplicate(Depth * 2, $\s),
    case get_node(IndexName, Pointer) of
        #node{type = leaf, page = Page} ->
            io:format("~sleaf ~p: ~p~n", [Indent, Pointer,
                                          [{V, O} || #item{val = V, pointer = O} <- Page]]);
        #node{type = non_leaf, page = Page} ->
            io:format("~snode ~p: ~p~n", [Indent, Pointer, [V || #item{val = V} <- Page]]),
            lists:foreach(fun(#item{pointer = [C]}) ->
                                  print_tree(IndexName, C, Depth + 1)
                          end, Page)
    end.
