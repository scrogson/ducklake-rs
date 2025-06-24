UPDATE ducklake_table_stats SET
    record_count = record_count + $2,
    next_row_id = next_row_id + $2,
    file_size_bytes = file_size_bytes + $3
WHERE table_id = $1; 