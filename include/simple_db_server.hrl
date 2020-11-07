-record(phys_loc, {
    table_name,
    page_id,
    slot
}).

%% スロット番号とそのデータ
-record(slot, {
    slot_n,
    data
}).

%% ディスク上のページ
%% 空き容量バイト数
%% スロット数
%% データ本体
-record(disk_data, {
    empty_size,
    slot_count,
    data_list
}).

%% REDOログ
%% タイムスタンプ
%% トランザクションID
%% アクション(ins,del)
%% オブジェクトID
%% 更新後値
-record(redo_log, {
    timestamp,
    txid,
    query_id,
    action,
    table_name,
    oid,
    val
}).