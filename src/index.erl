-module(index).
-compile(export_all).
-behaviour(gen_server).

-include("../include/simple_db_server.hrl").
-record(pntr, {
    type,
    pointer
}).
-define(E, 20).

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

generate_pointer() ->
    erlang:system_time().

%% B+tree Index
%% {pointer, leaf/non-leaf, [pointer, val, pointer,,,]}
insert(IndexName, ColName, Val, Oid) ->
    LeafNodeList = lists:reverse(traverse_index_tree(IndexName, Val, ColName, node)),
    io:format("LeafNodeList:~p~n", [LeafNodeList]),
    [LeafNode | _T] = LeafNodeList,
    case ets:lookup(IndexName, LeafNode) of
        [] -> err;
        [{LeafNode, leaf, Page}] when (length(Page) / 2) < ?E ->
            %% case: insert Oid to this page (not occur over flow)
            NewPage = insert_leaf_node(Page, Val, Oid),
            io:format("NewPage:~p~n", [NewPage]),
            ets:insert(IndexName, {LeafNode, leaf, NewPage});
        [{LeafNode, leaf, Page}] ->
            RebalanceList = get_all_pointer(IndexName, LeafNodeList),
            0
            % create_new_page(Val, Oid);
    end.

get_all_pointer(IndexName, LeafNodeList) when LeafNodeList > 2 ->
    [_H, ParentPointer | T] = LeafNodeList,
    ParentList = ets:lookup(IndexName, ParentPointer).


extract_node(IndexName, #pntr{pointer=P}) ->
    case ets:lookup(IndexName, P) of
        [] -> [];
        [{P, leaf, List}] ->
            List
    end;
extract_node(IndexName, [P | T]) ->
    [extract_node(IndexName, P), extract_node(IndexName, T)];
extract_node(_IndexName, V) ->
    V.

%% Insert Value into Leafnode in ascending order
insert_leaf_node([], Val, Oid) ->
    [#pntr{}, Val, #pntr{pointer=[Oid]}];
insert_leaf_node([P0], Val, Oid) ->
    [P0, Val, #pntr{pointer=[Oid]}];
insert_leaf_node([P0, V1, #pntr{pointer=O}=P1 | T], Val, Oid) when V1 =:= Val ->
    [P0, V1, P1#pntr{pointer=[Oid | O]} | T];
insert_leaf_node([P0, V1, P1 | T], Val, Oid) when V1 < Val ->
    [P0, V1 | insert_leaf_node([P1 | T], Val, Oid)];
insert_leaf_node([P0, V1, P1 | T], Val, Oid) when V1 > Val ->
    [P0, Val, #pntr{pointer=[Oid]}, V1, P1 | T].

%% when first call, Pointer is ColName 
traverse_index_tree(IndexName, Search, Pointer, oid) ->
    %% to get oid of search value
    case ets:lookup(IndexName, Pointer) of
        [] -> err;
        [{Pointer, Type, Page}] ->
            get_oid(IndexName, Search, Type, Page, oid)
    end;
traverse_index_tree(IndexName, Search, Pointer, node) ->
    %% to get target node to insert
    case ets:lookup(IndexName, Pointer) of
        [] -> err;
        [{Pointer, leaf, _}] ->
            [Pointer];
        [{Pointer, Type, Page}] ->
            [Pointer | get_oid(IndexName, Search, Type, Page, node)]
    end.

get_oid(_IndexName, _Search, leaf, [#pntr{pointer=Oid}], _Method) ->
    %% case: reach leafnode
    Oid;
get_oid(IndexName, Search, non_leaf, [#pntr{pointer=Pointer}], Method) ->
    %% case: traverse right node
    traverse_index_tree(IndexName, Search, Pointer, Method);
get_oid(_IndexName, Search, leaf, [#pntr{pointer=Oid}, Val | _T], _Method) when Search < Val ->
    Oid;
get_oid(IndexName, Search, non_leaf, [#pntr{pointer=Pointer}, Val | _T], Method) when Search < Val ->
    %% case: traverse left node
    traverse_index_tree(IndexName, Search, Pointer, Method);
get_oid(IndexName, Search, Type, [_Pointer, _Val | T], Method) ->
    %% case: get_pointer in same page
    get_oid(IndexName, Search, Type, T, Method).

test() ->
    Oid13 = #oid{table_name=fruit, page_id=1, slot=1},
    Oid25 = #oid{table_name=fruit, page_id=1, slot=2},
    Oid28 = #oid{table_name=fruit, page_id=1, slot=3},
    Oid50 = #oid{table_name=fruit, page_id=1, slot=4},
    Oid51 = #oid{table_name=fruit, page_id=1, slot=5},
    Oid75 = #oid{table_name=fruit, page_id=1, slot=6},
    ets:new(test, [set, named_table]),
    ets:insert(test, {col1, non_leaf, [#pntr{pointer=a}]}),
    ets:insert(test, {a, non_leaf, [#pntr{pointer=b}, 50, #pntr{pointer=c}]}),
    ets:insert(test, {b, non_leaf, [#pntr{pointer=d}, 25, #pntr{pointer=e}]}),
    ets:insert(test, {c, non_leaf, [#pntr{pointer=f}, 75, #pntr{pointer=g}]}),
    ets:insert(test, {d, leaf, [#pntr{}, 13, #pntr{type=oid, pointer=[Oid13]}]}),
    ets:insert(test, {e, leaf, [#pntr{}, 25, #pntr{type=oid, pointer=[Oid25]}, 28, #pntr{type=oid, pointer=[Oid28]}]}),
    ets:insert(test, {f, leaf, [#pntr{}, 50, #pntr{type=oid, pointer=[Oid50]}, 51, #pntr{type=oid, pointer=[Oid51]}]}),
    ets:insert(test, {g, leaf, [#pntr{}, 75, #pntr{type=oid, pointer=[Oid75]}]}),
    Key = [13, 25, 28, 50, 51, 75, 0],
    lists:map(fun(K) ->
        io:format("Key:~p, Oid:~p~n", [K, traverse_index_tree(test, K, col1, oid)])
    end, Key),
    Ins = [10,15,23,29],
    lists:map(fun(K) ->
        io:format("Key:~p, TargetPointer:~p~n", [K, traverse_index_tree(test, K, col1, node)])
    end, Ins),
    InsOid18 = #oid{table_name=fruit, page_id=1, slot=7},
    InsOid15 = #oid{table_name=fruit, page_id=1, slot=8},
    insert(test, col1, 18, InsOid18),
    insert(test, col1, 15, InsOid15),
    lists:map(fun(K) ->
        io:format("Key:~p, Oid:~p~n", [K, traverse_index_tree(test, K, col1, oid)])
    end, [15, 18 | Key]),
    ets:delete(test).
