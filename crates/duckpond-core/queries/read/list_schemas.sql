SELECT schema_id, schema_name
FROM ducklake_schema
WHERE
    $1 >= begin_snapshot AND
    ($1 < end_snapshot OR end_snapshot IS NULL); 