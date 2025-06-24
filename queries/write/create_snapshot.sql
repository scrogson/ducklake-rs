INSERT INTO ducklake_snapshot (
    snapshot_id,
    snapshot_time,
    schema_version,
    next_catalog_id,
    next_file_id
)
VALUES ($1, $2, $3, $4, $5); 