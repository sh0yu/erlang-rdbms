-record(oid, {
    table_name,
    page_id,
    slot
}).
-record(slot, {
    slot_n,
    data
}).
-record(disk_data, {
    empty_size,
    slot_count,
    data_list
}).