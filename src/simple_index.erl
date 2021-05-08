-module(simple_index).
-compile(export_all).

-include("../include/simple_db_server.hrl").

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Public APIs.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
create_table(TableName, ColNameList) ->
    lists:map(fun(ColName) ->
        ColIndexName = get_tab_column_key(TableName, ColName),
        create_column_index(ColIndexName) end, ColNameList),
    ok.

drop_table(TableName) ->
    {ok, ColNameList} = sys_tbl_mng:get_column_list(whereis(sys_tbl_mng), TableName),
    lists:map(fun(ColName) ->
        ColIndexName = get_tab_column_key(TableName, ColName),
        drop_column_index(ColIndexName) end, ColNameList),
    ok.

%% ColNameVal : [{ColumnName, Val},,,]
insert_index(TableName, ColNameVal, Oid) ->
    %% 各カラムごとにインデックスを追加する
    lists:map(fun({ColName, Val}) ->
        ColIndexName = get_tab_column_key(TableName, ColName),
        insert_column_index(ColIndexName, Val, Oid)
    end, ColNameVal),
    ok.

delete_index(TableName, ColName, Val, Oid) ->
    ColIndexName = get_tab_column_key(TableName, ColName),
    delete_column_index(ColIndexName, Val, Oid).

%% TODO: update_index実装
update_index(_TableName, _ColName, OldVal, NewVal, _Oid) when OldVal =:= NewVal->
    nop;
update_index(TableName, ColName, OldVal, NewVal, Oid) ->
    ColIndexName = get_tab_column_key(TableName, ColName),
    update_column_index(ColIndexName, OldVal, NewVal, Oid).

select_index(TableName, ColName, Val) ->
    ColIndexName = get_tab_column_key(TableName, ColName),
    case ets:lookup(ColIndexName, Val) of
        [] -> [];
        [{_Val, OidList}] -> OidList
    end.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% ColumnIndex mng functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% カラムインデックスを挿入する
insert_column_index(ColIndexName, Val, Oid) ->
    case ets:lookup(ColIndexName, Val) of
        %% カラムの値が新規登録されるパターン
        [] -> ets:insert(ColIndexName, {Val, [Oid]});
        %% すでにカラムの値が登録されているパターン
        %% 既存のオブジェクトIDのリストに新しいオブジェクトIDを追加する
        [{_Val, OidList}] ->
            case lists:member(Oid, OidList) of
                false ->
                    ets:insert(ColIndexName, {Val, [Oid | OidList]});
                %% ただし、すでに同じオブジェクトIDが登録されている場合は追加しない
                true -> nop
            end
    end.

%% カラムインデックスからオブジェクトIDを削除する
delete_column_index(ColIndexName, Val, Oid) ->
    case ets:lookup(ColIndexName, Val) of
        [] -> nop;
        [{_Val, OidList}] ->
            NewOidList = lists:filter(fun(X) -> X =/= Oid end, OidList),
            case NewOidList of
                [] -> ets:delete(ColIndexName, Val);
                _ -> ets:insert(ColIndexName, {Val, NewOidList})
            end
    end,
    ok.

%% カラムインデックスを更新する
update_column_index(ColIndexName, OldVal, NewVal, Oid) ->
    %% 更新前の値に紐づくオブジェクトIDを削除する
    delete_column_index(ColIndexName, OldVal, Oid),
    insert_column_index(ColIndexName, NewVal, Oid).

%% カラムインデックスを作成する
create_column_index(ColumnIndexName) ->
    ets:new(ColumnIndexName, [set, named_table, public]).

%% カラムインデックスを削除する
drop_column_index(ColIndexName) ->
    ets:delete(ColIndexName).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% util functions.
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% tab_column_keyを取得する
get_tab_column_key(TableName, ColumnNameList) when is_list(ColumnNameList) ->
    lists:map(fun(ColumnName) ->
        get_tab_column_key(TableName, ColumnName) end,
        ColumnNameList);
get_tab_column_key(TableName, ColumnName) ->
    TableNameStr = atom_to_list(TableName),
    ColumnNameStr = atom_to_list(ColumnName),
    list_to_atom(TableNameStr ++ "_" ++ ColumnNameStr).
