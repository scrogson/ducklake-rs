SELECT column_id, column_name, column_type, CAST(nulls_allowed AS INTEGER) as nulls_allowed
FROM ducklake_column
WHERE
    table_id = $1 AND
    parent_column IS NULL AND
    $2 >= begin_snapshot AND
    ($2 < end_snapshot OR end_snapshot IS NULL)
ORDER BY column_order; 