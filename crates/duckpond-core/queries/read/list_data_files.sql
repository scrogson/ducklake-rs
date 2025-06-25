SELECT data.data_file_id, data.path AS data_file_path, CAST(data.path_is_relative AS INTEGER) as path_is_relative, 
       data.record_count, data.file_size_bytes, del.path AS delete_file_path
FROM ducklake_data_file AS data
LEFT JOIN (
    SELECT *
    FROM ducklake_delete_file
    WHERE
        $2 >= begin_snapshot AND
        ($2 < end_snapshot OR end_snapshot IS NULL)
    ) AS del
USING (data_file_id)
WHERE
    data.table_id = $1 AND
    $2 >= data.begin_snapshot AND
    ($2 < data.end_snapshot OR data.end_snapshot IS NULL)
ORDER BY data.file_order; 