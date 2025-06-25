SELECT table_id, table_name
FROM ducklake_table
WHERE
    schema_id = $1 AND
    $2 >= begin_snapshot AND
    ($2 < end_snapshot OR end_snapshot IS NULL); 