-module(index).
-compile(export_all).

-include("../include/simple_db_server.hrl").
-record(item, {
    val,
    pointer
}).
-record(node, {
    type=non_leaf,
    page=[],
    left_node=nil,
    right_node=nil
}).
-define(E, 2).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public APIs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
init() ->
    ets:new(ms_index, [set, named_table, public]).

create_table(TableName, ColNameList) ->
    add_table(TableName),
    lists:map(fun(ColName) ->
        add_column(TableName, ColName) end, ColNameList),
    ok.

%% ColNameVal : [{ColumnaName, Val},,,]
insert_index(TableName, ColNameVal, Oid) ->
    IndexName = get_index_ets(TableName),
    lists:map(fun({ColName, Val}) ->
        insert(IndexName, ColName, Val, Oid) end, ColNameVal),
    ok.

delete_index(TableName, ColumnName, Val, Oid) ->
    IndexName = get_index_ets(TableName),
    delete(IndexName, ColumnName, Val, Oid).

select_index(TableName, ColName, Val) ->
    IndexName = get_index_ets(TableName),
    traverse_index_tree(IndexName, Val, get_tree_top(IndexName, ColName), oid).

disk_read() ->
    0.

disk_write() ->
    0.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% ColumnIndex mng functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% カラムインデックスを挿入する
insert_column_index(ColumnIndexId, Val, Oid) ->
    case ets:lookup(ColumnIndexId, Val) of
        %% カラムの値が新規登録されるパターン
        [] -> ets:insert(ColumnIndexId, {Val, [Oid]});
        %% すでにカラムの値が登録されているパターン
        %% 既存のオブジェクトIDのリストに新しいオブジェクトIDを追加する
        [{_Val, OidList}] ->
            case lists:member(Oid, OidList) of
                false ->
                    ets:insert(ColumnIndexId, {Val, [Oid | OidList]});
                %% ただし、すでに同じオブジェクトIDが登録されている場合は追加しない
                true -> nop
            end
    end.

%% カラムインデックスを更新する
update_column_index(ColumnIndexId, OldVal, NewVal, Oid) ->
    %% 更新前の値に紐づくオブジェクトIDを削除する
    delete_column_index(ColumnIndexId, OldVal, Oid),
    insert_column_index(ColumnIndexId, NewVal, Oid).

%% カラムインデックスからオブジェクトIDを削除する
delete_column_index(ColumnIndexId, Val, Oid) ->
    case ets:lookup(ColumnIndexId, Val) of
        [] -> nop;
        [{_Val, OidList}] ->
            NewOidList = lists:filter(fun(X) -> X =/= Oid end, OidList),
            case NewOidList of
                [] -> ets:delete(ColumnIndexId, Val);
                _ -> ets:insert(ColumnIndexId, {Val, NewOidList})
            end
    end.

%% カラムインデックスからオブジェクトIDを取得する
select_column_index(TableName, ColumnName, Val) ->
    case ets:lookup(ColumnName, Val) of
        [] -> [];
        [{_Val, OidList}] -> OidList
    end.

%% カラムインデックスを作成する
create_column_index(ColumnIndexName) ->
    ets:new(ColumnIndexName, [set, named_table, public]).

%% カラムインデックスを削除する
drop_column_index(ColumnIndexId) ->
    ets:delete(ColumnIndexId).

get_index_ets(TableName) ->
    case ets:lookup(ms_index, TableName) of
        [] ->
            add_table(TableName);
        [{TableName, IndexName}] ->
            IndexName
    end.

add_table(TableName) ->
    IndexName = list_to_atom(atom_to_list(TableName) ++ "_index"),
    ets:new(IndexName, [set, named_table, public]),
    ets:insert(ms_index, {TableName, IndexName}),
    IndexName.

add_column(TableName, ColName) ->
    IndexName = get_index_ets(TableName),
    TreeTop = new_pointer(),
    ets:insert(IndexName, {TreeTop, #node{type=leaf, page=[]}}),
    change_tree_top(IndexName, ColName, TreeTop).

get_tree_top(IndexName, ColName) ->
    case ets:lookup(IndexName, ColName) of
        [] -> err_5;
        [{ColName, root, Pointer}] ->
            io:format("TreeTop:[~p][~p]~n", [ColName, Pointer]),
            Pointer
    end.

change_tree_top(IndexName, ColName, Pointer) ->
    io:format("change_tree_top:~p,~p,~p~n", [IndexName, ColName, Pointer]),
    ets:insert(IndexName, {ColName, root, Pointer}).

%% B+tree Index
insert(IndexName, ColName, Val, Oid) ->
    LeafNodeList = lists:reverse(traverse_index_tree(IndexName, Val, get_tree_top(IndexName, ColName), node)),
    io:format("LeafNodeList:~p~n", [LeafNodeList]),
    sub_insert(IndexName, ColName, Val, Oid, LeafNodeList).

sub_insert(IndexName, ColName, Val, Oid, [LeafNode | ParentNodeList]) ->
    case ets:lookup(IndexName, LeafNode) of
        [] -> err_2;
        [{LeafNode, #node{type=Type, page=Page, left_node=LeftNodePointer, right_node=RightNodePointer}=Node}] ->
            NewPage = insert_item(Page, Val, Oid),
            io:format("NewPage:~p~n", [NewPage]),
            if (length(NewPage) / 2) <  ?E ->
                %% case: insert Oid to this page (not occur over flow)
                ets:insert(IndexName, {LeafNode, Node#node{page=NewPage}});
            true ->
                %% case: insert Oid to this page and split this page and insert middle item into parent page
                LeftPage = lists:sublist(NewPage, ?E),
                [#item{val=ParentVal} | _]=RightPage = lists:sublist(NewPage, ?E + 1, ?E * 2),
                RightNode = new_pointer(),
                io:format("LeftPage:~p~nRightPage:~p~n", [LeftPage, RightPage]),
                ets:insert(IndexName, {LeafNode, #node{type=Type, left_node=LeftNodePointer, page=LeftPage, right_node=RightNode}}),
                ets:insert(IndexName, {RightNode, #node{type=Type, left_node=LeafNode, page=RightPage, right_node=RightNodePointer}}),
                if ParentNodeList =:= [] ->
                    %% case: index tree get new top item, tree grows
                    ParentNode = new_pointer(),
                    ets:insert(IndexName, {ParentNode, #node{
                        type=non_leaf, page=[#item{val=nil, pointer=[LeafNode]}, #item{val=ParentVal, pointer=[RightNode]}]}}),
                    change_tree_top(IndexName, ColName, ParentNode);
                    true ->
                        sub_insert(IndexName, ColName, ParentVal, RightNode, ParentNodeList)
                end
            end
        end.

%% Insert Value into Leafnode in ascending order
insert_item([], Val, Oid) ->
    [#item{val=Val, pointer=[Oid]}];
insert_item([#item{val=nil}=I | T], Val, Oid) ->
    [I | insert_item(T, Val, Oid)];
insert_item([#item{val=V, pointer=O}=I | T], Val, Oid) when V =:= Val ->
    [I#item{pointer=[Oid | O]} | T];
insert_item([#item{val=V}=I | T], Val, Oid) when V < Val ->
    [I | insert_item(T, Val, Oid)];
insert_item([#item{val=V}=I | T], Val, Oid) when V > Val ->
    [#item{val=Val, pointer=[Oid]}, I | T].

delete(IndexName, ColName, Val, Oid) ->
    LeafNodeList = lists:reverse(traverse_index_tree(IndexName, Val, get_tree_top(IndexName, ColName), node)),
    io:format("LeafNodeList:~p~n", [LeafNodeList]),
    sub_delete(IndexName, ColName, Val, Oid, LeafNodeList).

sub_delete(IndexName, ColName, Val, Oid, [LeafNode | ParentNodeList]) ->
    case ets:lookup(IndexName, LeafNode) of
        [] -> err_2;
        [{LeafNode, #node{type=Type, page=Page, left_node=LeftNodePointer, right_node=RightNodePointer}=Node}] ->
            NewPage = delete_item(Page, Val, Oid),
            io:format("DelNewPage:~p~n", [NewPage]),
            if length(NewPage) <  ?E ->
                %% case: insert Oid to this page and split this page and insert middle item into parent page
                if ParentNodeList =:= [] ->
                    %% case: tree decline
                    io:format("decline tree~n"),
                    ets:insert(IndexName, {LeafNode, Node#node{page=NewPage}});
                true ->
                    io:format("merge tree~n"),
                    [ParentNode | _T] = ParentNodeList,
                    ets:insert(IndexName, {LeafNode, Node#node{page=NewPage}}),
                    case ets:lookup(IndexName, ParentNode) of
                        [] -> err;
                        [{ParentNode, #node{page=ParentPage}}] ->
                            io:format("ParentPage:~p~nVal:~p~n", [ParentPage, Val]),
                            LeftNode = get_left_node(ParentPage, Val),
                            RightNode = get_left_node(ParentPage, Val),
                            io:format("LeftNode:~p~nRightNode:~p~n", [LeftNode, RightNode]),
                        if (length(LeftNode) =:= ?E) and (length(RightNode) =:= ?E) ->
                            io:format("merge~n");
                        length(LeftNode) > ?E ->
                            %% TODO: implement
                            0;
                        true ->
                            %% TODO: implement
                            0
                        end
                    end,
                    %% case: merge left node or right node
                    %% TODO: implement
                    0
                end;
            true ->
                %% case: delete item and merge near node
                io:format("DeleteItemSingle:~p~n", [NewPage]),
                ets:insert(IndexName, {LeafNode, Node#node{page=NewPage}})
            end
        end.

get_left_node([_H], _Search) ->
    nil;
get_left_node([_H, #item{val=Val} | _T], Search) when Search < Val ->
    nil;
get_left_node([#item{pointer=[Pointer]}, #item{val=Val}], Search) when Val =< Search ->
    Pointer;
get_left_node([#item{pointer=[Pointer]}, #item{val=Val1}, #item{val=Val2} | _T], Search)
    when (Search >= Val1) and (Search < Val2) ->
    Pointer;
get_left_node([_H | T], Val) ->
    get_left_node(T, Val).

get_right_node([_H], _Val) ->
    nil;
get_right_node([#item{val=Val}, #item{pointer=[Pointer]} | _T], Val) ->
    Pointer;
get_right_node([_H | T], Val) ->
    get_right_node(T, Val).

delete_item([], _Val, _Oid) ->
    [];
delete_item([#item{val=Val, pointer=[Oid]} | T], Val, Oid) ->
    T;
delete_item([#item{val=Val, pointer=OidList} | T], Val, Oid) ->
    [#item{val=Val, pointer=lists:filter(fun(X) -> X =/= Oid end, OidList)} | T];
delete_item([H | T], Val, Oid) ->
    [H | delete_item(T, Val, Oid)].

%% when first call, Pointer is ColName
traverse_index_tree(IndexName, Search, Pointer, oid) ->
    %% to get oid of search value
    case ets:lookup(IndexName, Pointer) of
        [] -> [err_3];
        [{Pointer, #node{type=Type, page=Page}}] ->
            get_oid(IndexName, Search, Type, Page, oid)
    end;
traverse_index_tree(IndexName, Search, Pointer, node) ->
    %% to get target node to insert
    case ets:lookup(IndexName, Pointer) of
        [] -> [err_4];
        [{Pointer, #node{type=leaf}}] ->
            [Pointer];
        [{Pointer, #node{type=Type, page=Page}}] ->
            [Pointer | get_oid(IndexName, Search, Type, Page, node)]
    end.

get_oid(_IndexName, _Search, leaf, [#item{pointer=Oid}], _Method) ->
    %% case: reach leafnode
    Oid;
get_oid(_IndexName, Search, leaf, [#item{val=Search, pointer=Oid} | _T], _Method) ->
    Oid;
get_oid(IndexName, Search, _, [#item{pointer=[Pointer]}], Method) ->
    %% case: traverse right node
    traverse_index_tree(IndexName, Search, Pointer, Method);
get_oid(IndexName, Search, _, [#item{pointer=[Pntr1]}, #item{val=Val2} | _T], Method) when Search < Val2 ->
    %% case: traverse left node
    traverse_index_tree(IndexName, Search, Pntr1, Method);
get_oid(IndexName, Search, Type, [_H | T], Method) ->
    %% case: get_pointer in same page
    get_oid(IndexName, Search, Type, T, Method).

new_pointer() ->
    erlang:system_time(nanosecond).

print_tree(IndexName, Pointer) ->
    case ets:lookup(IndexName, Pointer) of
        [] -> err;
        [{Pointer, #node{type=Type, page=Page}=Node}] ->
            io:format("{~p, ~p}~n", [Pointer, Node]),
            print_page(IndexName, Type, Page)
    end.

print_page(IndexName, non_leaf, [#item{pointer=[Pointer]}]) ->
    print_tree(IndexName, Pointer);
print_page(IndexName, non_leaf, [#item{pointer=[Pointer]} | T]) ->
    print_tree(IndexName, Pointer),
    print_page(IndexName, non_leaf, T);
print_page(_IndexName, leaf, [#item{pointer=OidList}]) ->
    ok;
print_page(IndexName, leaf, [#item{pointer=OidList} | T]) ->
    print_page(IndexName, leaf, T).

test() ->
    Oid13 = #oid{table_name=fruit, page_id=1, slot=13},
    Oid25 = #oid{table_name=fruit, page_id=1, slot=25},
    Oid28 = #oid{table_name=fruit, page_id=1, slot=28},
    Oid50 = #oid{table_name=fruit, page_id=1, slot=50},
    Oid51 = #oid{table_name=fruit, page_id=1, slot=51},
    Oid75 = #oid{table_name=fruit, page_id=1, slot=75},
    ets:new(test, [set, named_table]),
    ets:insert(test, {col1, root, a}),
    ets:insert(test, {a, #node{type=non_leaf, page=[#item{val=nil, pointer=[b]}, #item{val=50, pointer=[c]}]}}),
    ets:insert(test, {b, #node{type=non_leaf, page=[#item{val=nil, pointer=[d]}, #item{val=25, pointer=[e]}]}}),
    ets:insert(test, {c, #node{type=non_leaf, page=[#item{val=50, pointer=[f]}, #item{val=75, pointer=[g]}]}}),
    ets:insert(test, {d, #node{type=leaf, page=[#item{val=13, pointer=[Oid13]}]}}),
    ets:insert(test, {e, #node{type=leaf, page=[#item{val=25, pointer=[Oid25]}, #item{val=28, pointer=[Oid28]}]}}),
    ets:insert(test, {f, #node{type=leaf, page=[#item{val=50, pointer=[Oid50]}, #item{val=51, pointer=[Oid51]}]}}),
    ets:insert(test, {g, #node{type=leaf, page=[#item{val=75, pointer=[Oid75]}]}}),
    io:format("Tree Top:~p~n", [get_tree_top(test, col1)]),
    Key = [13, 25, 28, 50, 51, 75, 0],
    lists:map(fun(K) ->
        io:format("Key:~p, Oid:~p~n", [K, traverse_index_tree(test, K, get_tree_top(test, col1), oid)])
    end, Key),
    InsOid18 = #oid{table_name=fruit, page_id=1, slot=18},
    InsOid15 = #oid{table_name=fruit, page_id=1, slot=15},
    InsOid76 = #oid{table_name=fruit, page_id=1, slot=76},
    InsOid77 = #oid{table_name=fruit, page_id=1, slot=77},
    InsOid78 = #oid{table_name=fruit, page_id=1, slot=78},
    InsOid79 = #oid{table_name=fruit, page_id=1, slot=79},
    InsOid80 = #oid{table_name=fruit, page_id=1, slot=80},
    InsOid81 = #oid{table_name=fruit, page_id=1, slot=81},
    InsOid82 = #oid{table_name=fruit, page_id=1, slot=82},
    InsOid83 = #oid{table_name=fruit, page_id=1, slot=83},
    InsOid84 = #oid{table_name=fruit, page_id=1, slot=84},
    InsOid85 = #oid{table_name=fruit, page_id=1, slot=85},
    InsOid85_2 = #oid{table_name=fruit, page_id=1, slot=852},
    insert(test, col1, 18, InsOid18),
    insert(test, col1, 15, InsOid15),
    insert(test, col1, 76, InsOid76),
    insert(test, col1, 77, InsOid77),
    insert(test, col1, 81, InsOid81),
    insert(test, col1, 82, InsOid82),
    insert(test, col1, 83, InsOid83),
    insert(test, col1, 84, InsOid84),
    insert(test, col1, 78, InsOid78),
    insert(test, col1, 79, InsOid79),
    insert(test, col1, 80, InsOid80),
    insert(test, col1, 85, InsOid85),
    insert(test, col1, 85, InsOid85_2),
    io:format("tree_top:~p~n", [get_tree_top(test,col1)]),
    lists:map(fun(K) ->
        io:format("Key:~p, Oid:~p~n", [K, traverse_index_tree(test, K, get_tree_top(test, col1), oid)])
    end, [15, 18, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85 | Key]),

    ets:insert(test, {col2, root, x}),
    ets:insert(test, {x, #node{type=leaf}}),
    insert(test, col2, 85, InsOid85),
    insert(test, col2, 84, InsOid84),
    insert(test, col2, 83, InsOid83),
    insert(test, col2, 82, InsOid82),
    insert(test, col2, 81, InsOid81),
    insert(test, col2, 80, InsOid80),
    insert(test, col2, 79, InsOid79),
    insert(test, col2, 78, InsOid78),
    insert(test, col2, 77, InsOid77),
    insert(test, col2, 18, InsOid18),
    insert(test, col2, 76, InsOid76),
    insert(test, col2, 15, InsOid15),
    insert(test, col2, 85, InsOid85_2),
    Key2 = [15, 18, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85],
    lists:map(fun(K) ->
        io:format("Key:~p, Oid:~p~n", [K, traverse_index_tree(test, K, get_tree_top(test, col2), oid)])
    end, Key2),
    delete(test, col2, 85, InsOid85),
    delete(test, col2, 85, InsOid85_2),
    delete(test, col2, 79, InsOid79),
    delete(test, col2, 78, InsOid78),
    delete(test, col2, 77, InsOid77),
    delete(test, col2, 76, InsOid76),
    io:format("Key:85, Oid:~p~n", [traverse_index_tree(test, 85, get_tree_top(test, col2), oid)]),
    io:format("Key:84, Oid:~p~n", [traverse_index_tree(test, 84, get_tree_top(test, col2), oid)]),
    % io:format("col1~n"),
    % print_tree(test, get_tree_top(test, col1)),
    % io:format("col2~n"),
    % print_tree(test, get_tree_top(test, col2)),
    ets:delete(test).

get_left_node_test() ->
    ItemNil = #item{val=nil, pointer=[item_nil]},
    Item0 = #item{val=0, pointer=[item_0]},
    Item10 = #item{val=10, pointer=[item_10]},
    Item20 = #item{val=20, pointer=[item_20]},
    Item30 = #item{val=30, pointer=[item_30]},

    OneListNil = [ItemNil],
    OneList10 = [Item0],
    TwoListNil = [ItemNil, Item10],
    TwoList10 = [Item0, Item10],
    ThreeListNil = [ItemNil, Item10, Item20],
    ThreeList10 = [Item0, Item10, Item20],
    FourListNil = [ItemNil, Item10, Item20, Item30],
    FourList10 = [Item0, Item10, Item20, Item30],

    %% case: List length = 1
    nil = get_left_node(OneListNil, 0),
    nil = get_left_node(OneListNil, 10),
    nil = get_left_node(OneList10, 0),
    nil = get_left_node(OneList10, 10),

    %% case: List length = 2
    nil = get_left_node(TwoListNil, -1),
    nil = get_left_node(TwoListNil, 0),
    nil = get_left_node(TwoListNil, 1),
    item_nil = get_left_node(TwoListNil, 10),
    item_nil = get_left_node(TwoListNil, 11),
    nil = get_left_node(TwoList10, -1),
    nil = get_left_node(TwoList10, 0),
    nil = get_left_node(TwoList10, 1),
    item_0 = get_left_node(TwoList10, 10),
    item_0 = get_left_node(TwoList10, 11),

    %% case: List length = 3
    nil = get_left_node(ThreeListNil, -1),
    nil = get_left_node(ThreeListNil, 0),
    nil = get_left_node(ThreeListNil, 1),
    item_nil = get_left_node(ThreeListNil, 10),
    item_nil = get_left_node(ThreeListNil, 11),
    item_10 = get_left_node(ThreeListNil, 20),
    item_10 = get_left_node(ThreeListNil, 21),
    nil = get_left_node(ThreeList10, -1),
    nil = get_left_node(ThreeList10, 0),
    nil = get_left_node(ThreeList10, 1),
    item_0 = get_left_node(ThreeList10, 10),
    item_0 = get_left_node(ThreeList10, 11),
    item_10 = get_left_node(ThreeList10, 20),
    item_10 = get_left_node(ThreeList10, 21),

    %% case: List length = 4
    nil = get_left_node(FourListNil, -1),
    nil = get_left_node(FourListNil, 0),
    nil = get_left_node(FourListNil, 1),
    item_nil = get_left_node(FourListNil, 10),
    item_nil = get_left_node(FourListNil, 11),
    item_10 = get_left_node(FourListNil, 20),
    item_10 = get_left_node(FourListNil, 21),
    item_20 = get_left_node(FourListNil, 30),
    item_20 = get_left_node(FourListNil, 31),
    nil = get_left_node(FourList10, -1),
    nil = get_left_node(FourList10, 0),
    nil = get_left_node(FourList10, 1),
    item_0 = get_left_node(FourList10, 10),
    item_0 = get_left_node(FourList10, 11),
    item_10 = get_left_node(FourList10, 20),
    item_10 = get_left_node(FourList10, 21),
    item_20 = get_left_node(FourList10, 30),
    item_20 = get_left_node(FourList10, 31).
