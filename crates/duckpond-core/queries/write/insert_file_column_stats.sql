INSERT INTO ducklake_file_column_statistics (
    data_file_id,
    table_id,
    column_id,
    value_count,
    null_count,
    min_value,
    max_value,
    contains_nan
)
VALUES ($1, $2, $3, $4, $5, $6, $7, ($8 > 0)); 