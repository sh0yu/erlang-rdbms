-module(index).
-compile(export_all).
-behaviour(gen_server).

-include("../include/simple_db_server.hrl").
-record(item, {
    val,
    pointer
}).
-define(E, 2).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop(Pid) ->
    gen_server:call(Pid, terminate).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public APIs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%% ColNameVal : [{ColumnaName, Val},,,]
insert_index(Pid, TableName, ColNameVal, Oid) ->
    gen_server:call(Pid, {insert_index, TableName, ColNameVal, Oid}).

delete_index(TableName, ColumnName, Val, Oid) ->
    0.

disk_read() ->
    0.

disk_write() ->
    0.

init([]) ->
    ets:new(ms_index, [set, named_table, public]),
    {ok, []}.
    
handle_call({insert_index, TableName, ColNameVal, Oid}, _From, State) ->
    IndexName = get_index_ets(TableName),
    [{ColName, Val} | _T] = ColNameVal,
    ets:insert(IndexName, {ColNameVal, Oid}),
    {reply, ok, State};
handle_call({}, _From, State) ->
    {reply, ok, State}.

handle_cast({}, State) ->
    {reply, ok, State}.

handle_info(Msg, State) ->
    io:format("Unexpected message: ~p~n", [Msg]),
    {noreply, State}.

terminate(normal, _State) ->
    io:format("Server teminated.~n"),
    ok.

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
            create_new_index(TableName);
        [TableName, IndexName] ->
            IndexName
    end.

create_new_index(TableName) ->
    IndexName = atom_to_list(TableName) ++ "_index",
    ets:new(IndexName, [set, named_table, public]),
    ets:insert(ms_index, {TableName, IndexName}),
    IndexName.

get_tree_top(IndexName, ColName) ->
    case ets:lookup(IndexName, ColName) of
        [] -> err_5;
        [{ColName, root, [#item{pointer=[Pointer]}]}] ->
            Pointer
    end.

change_tree_top(IndexName, ColName, Pointer) ->
    ets:insert(IndexName, {ColName, root, [#item{pointer=[Pointer]}]}).

%% B+tree Index
%% {pointer, leaf/non-leaf, [pointer, val, pointer,,,]}

insert(IndexName, ColName, Val, Oid) ->
    LeafNodeList = lists:reverse(traverse_index_tree(IndexName, Val, get_tree_top(IndexName, ColName), node)),
    io:format("LeafNodeList:~p~n", [LeafNodeList]),
    sub_insert(IndexName, ColName, Val, Oid, LeafNodeList).

sub_insert(IndexName, ColName, Val, Oid, [LeafNode]) ->
    case ets:lookup(IndexName, LeafNode) of
        [] -> err_2;
        [{LeafNode, Type, Page}] ->
            NewPage = insert_leaf_node(Page, Val, Oid),
            io:format("NewPage:~p~n", [NewPage]),
            if (length(NewPage) / 2) <  ?E ->
                %% case: insert Oid to this page (not occur over flow)
                ets:insert(IndexName, {LeafNode, Type, NewPage});
            true ->
                %% case: insert Oid to this page and split this page and insert middle item into parent page
                LeftPage = lists:sublist(NewPage, ?E),
                [#item{val=ParentVal} | _]=RightPage = lists:sublist(NewPage, ?E + 1, ?E * 2),
                RightNode = new_pointer(),
                io:format("LeftPage:~p~nRightPage:~p~n", [LeftPage, RightPage]),
                ets:insert(IndexName, {LeafNode, Type, LeftPage}),
                ets:insert(IndexName, {RightNode, Type, RightPage}),
                %% 木が高くなるケース
                ParentNode = new_pointer(),
                ets:insert(IndexName, {ParentNode, non_leaf, [#item{val=nil, pointer=[LeafNode]}, #item{val=ParentVal, pointer=[RightNode]}]}),
                change_tree_top(IndexName, ColName, ParentNode)
            end
        end;
sub_insert(IndexName, ColName, Val, Oid, [LeafNode | ParentNodeList]) ->
    case ets:lookup(IndexName, LeafNode) of
        [] -> err_2;
        [{LeafNode, Type, Page}] ->
            NewPage = insert_leaf_node(Page, Val, Oid),
            io:format("NewPage:~p~n", [NewPage]),
            if (length(NewPage) / 2) <  ?E ->
                %% case: insert Oid to this page (not occur over flow)
                ets:insert(IndexName, {LeafNode, Type, NewPage});
            true ->
                %% case: insert Oid to this page and split this page and insert middle item into parent page
                LeftPage = lists:sublist(NewPage, ?E),
                [#item{val=ParentVal} | _]=RightPage = lists:sublist(NewPage, ?E + 1, ?E * 2),
                NewNode = new_pointer(),
                io:format("LeftPage:~p~nRightPage:~p~n", [LeftPage, RightPage]),
                ets:insert(IndexName, {LeafNode, Type, LeftPage}),
                ets:insert(IndexName, {NewNode, Type, RightPage}),
                sub_insert(IndexName, ColName, ParentVal, NewNode, ParentNodeList)
            end
        end.
            
new_pointer() ->
    erlang:system_time(nanosecond).

%% Insert Value into Leafnode in ascending order
insert_leaf_node([], Val, Oid) ->
    [#item{val=Val, pointer=[Oid]}];
insert_leaf_node([#item{val=nil}=I | T], Val, Oid) ->
    [I | insert_leaf_node(T, Val, Oid)];
insert_leaf_node([#item{val=V, pointer=O}=I | T], Val, Oid) when V =:= Val ->
    [I#item{pointer=[Oid | O]} | T];
insert_leaf_node([#item{val=V}=I | T], Val, Oid) when V < Val ->
    [I | insert_leaf_node(T, Val, Oid)];
insert_leaf_node([#item{val=V}=I | T], Val, Oid) when V > Val ->
    [#item{val=Val, pointer=[Oid]}, I | T].

%% when first call, Pointer is ColName
traverse_index_tree(IndexName, Search, Pointer, oid) ->
    %% to get oid of search value
    case ets:lookup(IndexName, Pointer) of
        [] -> [err_3];
        [{Pointer, Type, Page}] ->
            get_oid(IndexName, Search, Type, Page, oid)
    end;
traverse_index_tree(IndexName, Search, Pointer, node) ->
    %% to get target node to insert
    case ets:lookup(IndexName, Pointer) of
        [] -> [err_4];
        [{Pointer, leaf, _Page}] ->
            [Pointer];
        [{Pointer, Type, Page}] ->
            [Pointer | get_oid(IndexName, Search, Type, Page, node)]
    end.

get_oid(_IndexName, _Search, leaf, [#item{pointer=Oid}], _Method) ->
    %% case: reach leafnode
    Oid;
get_oid(_IndexName, Search, leaf, [#item{val=Val, pointer=Oid} | _T], _Method) when Search =:= Val ->
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

test() ->
    Oid13 = #oid{table_name=fruit, page_id=1, slot=13},
    Oid25 = #oid{table_name=fruit, page_id=1, slot=25},
    Oid28 = #oid{table_name=fruit, page_id=1, slot=28},
    Oid50 = #oid{table_name=fruit, page_id=1, slot=50},
    Oid51 = #oid{table_name=fruit, page_id=1, slot=51},
    Oid75 = #oid{table_name=fruit, page_id=1, slot=75},
    ets:new(test, [set, named_table]),
    ets:insert(test, {col1, root, [#item{pointer=[a]}]}),
    ets:insert(test, {a, non_leaf, [#item{val=nil, pointer=[b]}, #item{val=50, pointer=[c]}]}),
    ets:insert(test, {b, non_leaf, [#item{val=nil, pointer=[d]}, #item{val=25, pointer=[e]}]}),
    ets:insert(test, {c, non_leaf, [#item{val=50, pointer=[f]}, #item{val=75, pointer=[g]}]}),
    ets:insert(test, {d, leaf, [#item{val=13, pointer=[Oid13]}]}),
    ets:insert(test, {e, leaf, [#item{val=25, pointer=[Oid25]}, #item{val=28, pointer=[Oid28]}]}),
    ets:insert(test, {f, leaf, [#item{val=50, pointer=[Oid50]}, #item{val=51, pointer=[Oid51]}]}),
    ets:insert(test, {g, leaf, [#item{val=75, pointer=[Oid75]}]}),
    io:format("Tree Top:~p~n", [get_tree_top(test, col1)]),
    Key = [13, 25, 28, 50, 51, 75, 0],
    lists:map(fun(K) ->
        io:format("Key:~p, Oid:~p~n", [K, traverse_index_tree(test, K, col1, oid)])
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

    ets:insert(test, {col2, root, [#item{pointer=[x]}]}),
    ets:insert(test, {x, leaf, []}),
    insert(test, col2, 18, InsOid18),
    insert(test, col2, 15, InsOid15),
    insert(test, col2, 76, InsOid76),
    insert(test, col2, 77, InsOid77),
    insert(test, col2, 81, InsOid81),
    insert(test, col2, 82, InsOid82),
    insert(test, col2, 83, InsOid83),
    insert(test, col2, 84, InsOid84),
    insert(test, col2, 78, InsOid78),
    insert(test, col2, 79, InsOid79),
    insert(test, col2, 80, InsOid80),
    insert(test, col2, 85, InsOid85),
    insert(test, col2, 85, InsOid85_2),
    Key2 = [15, 18, 76, 77, 78, 79, 80, 81, 82, 83, 84, 85],
    lists:map(fun(K) ->
        io:format("Key:~p, Oid:~p~n", [K, traverse_index_tree(test, K, get_tree_top(test, col2), oid)])
    end, Key2),
    ets:delete(test).
