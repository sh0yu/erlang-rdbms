%%%-------------------------------------------------------------------
%%% @doc
%%% カラムインデックス。1テーブル1カラムにつき1つのETS(set)を持ち、
%%% カラム値からオブジェクトIDのリストを引く。
%%%
%%%   ets: <TableName>_<ColumnName> :: {Val, [Oid]}
%%%
%%% 等値検索のみを対象とした素朴な実装。範囲検索を含む用途向けには
%%% index.erl のB+treeを使う。
%%%
%%% 存在しないテーブル・カラムを引いた場合はクラッシュせず [] を返す。
%%% ロールバックやリカバリの途中で、まだ作られていない(あるいは
%%% すでに落とされた)インデックスに触れることがあるため。
%%% @end
%%%-------------------------------------------------------------------
-module(simple_index).

-export([init/0, create_table/2, drop_table/2, exist_index/2]).
-export([create_index/2, drop_index/2]).
-export([insert_index/3, delete_index/4, update_index/5, select_index/3]).
-export([get_tab_column_key/2]).

-include("../include/simple_db_server.hrl").

%%%===================================================================
%%% Public API
%%%===================================================================

%%----------------------------------------------------------------------
%% @doc 起動時の初期化。カラムごとのETSを都度作るので、
%% ここで用意しておくものはない(index.erlと同じ形にするために存在する)。
%%----------------------------------------------------------------------
init() ->
    ok.

%%----------------------------------------------------------------------
%% @doc テーブルの全カラムにインデックスを作る。
%%----------------------------------------------------------------------
create_table(TableName, ColNameList) ->
    lists:foreach(fun(ColName) ->
                          create_column_index(get_tab_column_key(TableName, ColName))
                  end, ColNameList),
    ok.

%%----------------------------------------------------------------------
%% @doc テーブルの全カラムのインデックスを破棄する。
%% カラム一覧は呼び出し側から渡す。システムカタログから引くと
%% drop_tableの処理順に依存してしまうため。
%%----------------------------------------------------------------------
drop_table(TableName, ColNameList) ->
    lists:foreach(fun(ColName) ->
                          drop_column_index(get_tab_column_key(TableName, ColName))
                  end, ColNameList),
    ok.

%%----------------------------------------------------------------------
%% @doc カラム1つぶんの索引を作る / 落とす。
%% CREATE INDEX / DROP INDEX の受け口。
%%----------------------------------------------------------------------
create_index(TableName, ColName) ->
    _ = create_column_index(get_tab_column_key(TableName, ColName)),
    ok.

drop_index(TableName, ColName) ->
    _ = drop_column_index(get_tab_column_key(TableName, ColName)),
    ok.

exist_index(TableName, ColName) ->
    ets:info(get_tab_column_key(TableName, ColName)) =/= undefined.

%%----------------------------------------------------------------------
%% @doc 1行分のインデックスを追加する。
%% ColNameVal : [{ColumnName, Val}, ...]
%%----------------------------------------------------------------------
insert_index(TableName, ColNameVal, Oid) ->
    lists:foreach(fun({ColName, Val}) ->
                          insert_column_index(get_tab_column_key(TableName, ColName), Val, Oid)
                  end, ColNameVal),
    ok.

delete_index(TableName, ColName, Val, Oid) ->
    delete_column_index(get_tab_column_key(TableName, ColName), Val, Oid),
    ok.

update_index(_TableName, _ColName, OldVal, NewVal, _Oid) when OldVal =:= NewVal ->
    ok;
update_index(TableName, ColName, OldVal, NewVal, Oid) ->
    IndexName = get_tab_column_key(TableName, ColName),
    delete_column_index(IndexName, OldVal, Oid),
    insert_column_index(IndexName, NewVal, Oid),
    ok.

%%----------------------------------------------------------------------
%% @doc 条件に一致する行のオブジェクトIDのリストを返す。
%%----------------------------------------------------------------------
select_index(TableName, ColName, Val) ->
    IndexName = get_tab_column_key(TableName, ColName),
    case ets:info(IndexName) of
        undefined ->
            [];
        _ ->
            case ets:lookup(IndexName, Val) of
                [] -> [];
                [{_Val, OidList}] -> OidList
            end
    end.

%%%===================================================================
%%% ColumnIndex mng functions
%%%===================================================================

insert_column_index(IndexName, Val, Oid) ->
    case ets:info(IndexName) of
        undefined ->
            ok;
        _ ->
            case ets:lookup(IndexName, Val) of
                [] ->
                    ets:insert(IndexName, {Val, [Oid]});
                [{_Val, OidList}] ->
                    %% 同じOidが二重に載らないようにする
                    case lists:member(Oid, OidList) of
                        false -> ets:insert(IndexName, {Val, [Oid | OidList]});
                        true -> true
                    end
            end,
            ok
    end.

delete_column_index(IndexName, Val, Oid) ->
    case ets:info(IndexName) of
        undefined ->
            ok;
        _ ->
            case ets:lookup(IndexName, Val) of
                [] ->
                    ok;
                [{_Val, OidList}] ->
                    case lists:filter(fun(X) -> X =/= Oid end, OidList) of
                        [] -> ets:delete(IndexName, Val);
                        NewOidList -> ets:insert(IndexName, {Val, NewOidList})
                    end,
                    ok
            end
    end.

%% 作成済みの場合は作り直す。テーブルを作り直した際に
%% 前のインデックスの中身が残らないようにするため。
create_column_index(IndexName) ->
    _ = drop_column_index(IndexName),
    ets:new(IndexName, [set, named_table, public]).

drop_column_index(IndexName) ->
    case ets:info(IndexName) of
        undefined -> ok;
        _ -> ets:delete(IndexName), ok
    end.

%%%===================================================================
%%% util functions
%%%===================================================================

%% テーブル名とカラム名からインデックスのETS名を作る。
get_tab_column_key(TableName, ColumnNameList) when is_list(ColumnNameList) ->
    [get_tab_column_key(TableName, ColumnName) || ColumnName <- ColumnNameList];
get_tab_column_key(TableName, ColumnName) ->
    list_to_atom(atom_to_list(TableName) ++ "_" ++ atom_to_list(ColumnName)).
