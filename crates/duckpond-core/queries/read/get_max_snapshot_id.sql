SELECT COALESCE(MAX(snapshot_id), 0) as max_snapshot_id
FROM ducklake_snapshot; 