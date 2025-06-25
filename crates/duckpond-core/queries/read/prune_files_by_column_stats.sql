SELECT DISTINCT data_file_id
FROM ducklake_file_column_statistics
WHERE
    table_id = $1 AND
    column_id = $2 AND
    ($3 >= min_value OR min_value IS NULL) AND
    ($3 <= max_value OR max_value IS NULL); 