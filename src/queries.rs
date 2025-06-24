use crate::error::DuckLakeError;
use crate::models::*;
use chrono::{DateTime, Utc};
use sqlx::{Any, AnyPool, Executor, Row};
use uuid::Uuid;

/// Reading operations for DuckLake metadata
pub struct ReadQueries;

impl ReadQueries {
    /// Get the current (latest) snapshot ID
    pub async fn get_current_snapshot<'c, E>(executor: E) -> Result<Option<i64>, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/get_current_snapshot.sql");
        let row = sqlx::query(sql).fetch_optional(executor).await?;

        Ok(row.map(|r| r.try_get("snapshot_id").unwrap_or_default()))
    }

    /// Get the maximum snapshot ID (for generating new snapshots)
    pub async fn get_max_snapshot_id<'c, E>(executor: E) -> Result<i64, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/get_max_snapshot_id.sql");
        let row = sqlx::query(sql).fetch_one(executor).await?;

        Ok(row.try_get("max_snapshot_id").unwrap_or(0))
    }

    /// List schemas for a given snapshot
    pub async fn list_schemas<'c, E>(
        executor: E,
        snapshot_id: i64,
    ) -> Result<Vec<SchemaInfo>, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/list_schemas.sql");
        let rows = sqlx::query(sql)
            .bind(snapshot_id)
            .fetch_all(executor)
            .await?;

        let schemas = rows
            .into_iter()
            .map(|row| SchemaInfo {
                schema_id: row.try_get("schema_id").unwrap_or_default(),
                schema_name: row.try_get("schema_name").unwrap_or_default(),
            })
            .collect();

        Ok(schemas)
    }

    /// List tables in a schema for a given snapshot
    pub async fn list_tables<'c, E>(
        executor: E,
        schema_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<TableInfo>, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/list_tables.sql");
        let rows = sqlx::query(sql)
            .bind(schema_id)
            .bind(snapshot_id)
            .fetch_all(executor)
            .await?;

        let tables = rows
            .into_iter()
            .map(|row| TableInfo {
                table_id: row.try_get("table_id").unwrap_or_default(),
                table_name: row.try_get("table_name").unwrap_or_default(),
            })
            .collect();

        Ok(tables)
    }

    /// Show the structure of a table (top-level columns)
    pub async fn show_table_structure<'c, E>(
        executor: E,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<ColumnInfo>, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/show_table_structure.sql");
        let rows = sqlx::query(sql)
            .bind(table_id)
            .bind(snapshot_id)
            .fetch_all(executor)
            .await?;

        let columns = rows
            .into_iter()
            .map(|row| ColumnInfo {
                column_id: row.try_get("column_id").unwrap_or_default(),
                column_name: row.try_get("column_name").unwrap_or_default(),
                column_type: row.try_get("column_type").unwrap_or_default(),
                nulls_allowed: row.try_get::<i32, _>("nulls_allowed").unwrap_or_default() != 0,
            })
            .collect();

        Ok(columns)
    }

    /// List data files for a table
    pub async fn list_data_files<'c, E>(
        executor: E,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DataFileInfo>, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/list_data_files.sql");
        let rows = sqlx::query(sql)
            .bind(table_id)
            .bind(snapshot_id)
            .fetch_all(executor)
            .await?;

        let files = rows
            .into_iter()
            .map(|row| DataFileInfo {
                data_file_id: row.try_get("data_file_id").unwrap_or_default(),
                data_file_path: row.try_get("data_file_path").unwrap_or_default(),
                path_is_relative: row
                    .try_get::<i32, _>("path_is_relative")
                    .unwrap_or_default()
                    != 0,
                record_count: row.try_get("record_count").unwrap_or_default(),
                file_size_bytes: row.try_get("file_size_bytes").unwrap_or_default(),
                delete_file_path: row.try_get("delete_file_path").ok(),
            })
            .collect();

        Ok(files)
    }

    /// Prune files by column statistics
    pub async fn prune_files_by_column_stats<'c, E>(
        executor: E,
        table_id: i64,
        column_id: i64,
        value: &str,
    ) -> Result<Vec<i64>, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/prune_files_by_column_stats.sql");
        let rows = sqlx::query(sql)
            .bind(table_id)
            .bind(column_id)
            .bind(value)
            .fetch_all(executor)
            .await?;

        let file_ids = rows
            .into_iter()
            .map(|row| row.try_get("data_file_id").unwrap_or_default())
            .collect();
        Ok(file_ids)
    }

    /// Get next catalog ID from latest snapshot
    pub async fn get_next_catalog_id<'c, E>(executor: E) -> Result<Option<i64>, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/get_next_catalog_id.sql");
        let row = sqlx::query(sql).fetch_optional(executor).await?;

        Ok(row.map(|r| r.try_get("next_catalog_id").unwrap_or_default()))
    }

    /// Get next file ID from latest snapshot
    pub async fn get_next_file_id<'c, E>(executor: E) -> Result<Option<i64>, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/get_next_file_id.sql");
        let row = sqlx::query(sql).fetch_optional(executor).await?;

        Ok(row.map(|r| r.try_get("next_file_id").unwrap_or_default()))
    }

    /// Get next row ID for a table
    pub async fn get_table_next_row_id<'c, E>(
        executor: E,
        table_id: i64,
    ) -> Result<Option<i64>, DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/read/get_table_next_row_id.sql");
        let row = sqlx::query(sql)
            .bind(table_id)
            .fetch_optional(executor)
            .await?;

        Ok(row.map(|r| r.try_get("next_row_id").unwrap_or_default()))
    }
}

/// Writing operations for DuckLake metadata
pub struct WriteQueries;

impl WriteQueries {
    /// Create a new snapshot
    pub async fn create_snapshot<'c, E>(
        executor: E,
        snapshot_id: i64,
        timestamp: DateTime<Utc>,
        schema_version: i64,
        next_catalog_id: i64,
        next_file_id: i64,
    ) -> Result<(), DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/write/create_snapshot.sql");
        sqlx::query(sql)
            .bind(snapshot_id)
            .bind(timestamp.to_rfc3339())
            .bind(schema_version)
            .bind(next_catalog_id)
            .bind(next_file_id)
            .execute(executor)
            .await?;

        Ok(())
    }

    /// Log snapshot changes
    pub async fn log_snapshot_changes<'c, E>(
        executor: E,
        snapshot_id: i64,
        changes: &str,
    ) -> Result<(), DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/write/log_snapshot_changes.sql");
        sqlx::query(sql)
            .bind(snapshot_id)
            .bind(changes)
            .execute(executor)
            .await?;

        Ok(())
    }

    /// Create a new schema
    pub async fn create_schema<'c, E>(
        executor: E,
        schema_id: i64,
        schema_uuid: Uuid,
        begin_snapshot: i64,
        schema_name: &str,
    ) -> Result<(), DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/write/create_schema.sql");
        sqlx::query(sql)
            .bind(schema_id)
            .bind(schema_uuid.to_string())
            .bind(begin_snapshot)
            .bind(schema_name)
            .execute(executor)
            .await?;

        Ok(())
    }

    /// Create a new table
    pub async fn create_table<'c, E>(
        executor: E,
        table_id: i64,
        table_uuid: Uuid,
        begin_snapshot: i64,
        schema_id: i64,
        table_name: &str,
    ) -> Result<(), DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/write/create_table.sql");
        sqlx::query(sql)
            .bind(table_id)
            .bind(table_uuid.to_string())
            .bind(begin_snapshot)
            .bind(schema_id)
            .bind(table_name)
            .execute(executor)
            .await?;

        Ok(())
    }

    /// Create a new column
    pub async fn create_column<'c, E>(
        executor: E,
        column_id: i64,
        begin_snapshot: i64,
        table_id: i64,
        column_order: i64,
        column_name: &str,
        column_type: &str,
        nulls_allowed: bool,
    ) -> Result<(), DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/write/create_column.sql");
        sqlx::query(sql)
            .bind(column_id)
            .bind(begin_snapshot)
            .bind(table_id)
            .bind(column_order)
            .bind(column_name)
            .bind(column_type)
            .bind(nulls_allowed)
            .execute(executor)
            .await?;

        Ok(())
    }

    /// Insert a new data file
    pub async fn insert_data_file<'c, E>(
        executor: E,
        data_file_id: i64,
        table_id: i64,
        begin_snapshot: i64,
        path: &str,
        path_is_relative: bool,
        file_format: &str,
        record_count: i64,
        file_size_bytes: i64,
        footer_size: Option<i64>,
        row_id_start: i64,
    ) -> Result<(), DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/write/insert_data_file.sql");
        sqlx::query(sql)
            .bind(data_file_id)
            .bind(table_id)
            .bind(begin_snapshot)
            .bind(path)
            .bind(path_is_relative)
            .bind(file_format)
            .bind(record_count)
            .bind(file_size_bytes)
            .bind(footer_size)
            .bind(row_id_start)
            .execute(executor)
            .await?;

        Ok(())
    }

    /// Update table statistics
    pub async fn update_table_stats<'c, E>(
        executor: E,
        table_id: i64,
        record_count: i64,
        file_size_bytes: i64,
    ) -> Result<(), DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/write/update_table_stats.sql");
        sqlx::query(sql)
            .bind(table_id)
            .bind(record_count)
            .bind(file_size_bytes)
            .execute(executor)
            .await?;

        Ok(())
    }

    /// Update table column statistics
    pub async fn update_table_column_stats<'c, E>(
        executor: E,
        table_id: i64,
        column_id: i64,
        null_count: i64,
        nan_count: i64,
        min_value: Option<&str>,
        max_value: Option<&str>,
    ) -> Result<(), DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/write/update_table_column_stats.sql");
        sqlx::query(sql)
            .bind(table_id)
            .bind(column_id)
            .bind(null_count)
            .bind(nan_count)
            .bind(min_value)
            .bind(max_value)
            .execute(executor)
            .await?;

        Ok(())
    }

    /// Insert file column statistics
    pub async fn insert_file_column_stats<'c, E>(
        executor: E,
        data_file_id: i64,
        table_id: i64,
        column_id: i64,
        value_count: i64,
        null_count: i64,
        nan_count: i64,
        min_value: Option<&str>,
        max_value: Option<&str>,
    ) -> Result<(), DuckLakeError>
    where
        E: Executor<'c, Database = Any>,
    {
        let sql = include_str!("../queries/write/insert_file_column_stats.sql");
        sqlx::query(sql)
            .bind(data_file_id)
            .bind(table_id)
            .bind(column_id)
            .bind(value_count)
            .bind(null_count)
            .bind(min_value)
            .bind(max_value)
            .bind(nan_count)
            .execute(executor)
            .await?;

        Ok(())
    }
}
