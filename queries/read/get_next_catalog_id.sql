SELECT next_catalog_id
FROM ducklake_snapshot
WHERE snapshot_id = (SELECT MAX(snapshot_id) FROM ducklake_snapshot); 