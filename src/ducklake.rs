//! High-level DuckLake API for managing lakehouse operations
//!
//! This module provides a transaction-aware interface for DuckLake operations,
//! implementing the patterns described in the DuckLake specification.

use crate::error::Result;
use crate::models::*;
use crate::queries::{ReadQueries, WriteQueries};
use chrono::Utc;
use sqlx::{Any, AnyPool, Transaction};
use uuid::Uuid;

/// High-level DuckLake interface for managing lakehouse operations
#[derive(Clone)]
pub struct DuckLake {
    pool: AnyPool,
}

impl DuckLake {
    /// Create a new DuckLake instance with the given database pool
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }

    /// Get the current snapshot ID
    pub async fn current_snapshot(&self) -> Result<Option<i64>> {
        ReadQueries::get_current_snapshot(&self.pool).await
    }

    /// Get the maximum snapshot ID
    pub async fn max_snapshot_id(&self) -> Result<i64> {
        ReadQueries::get_max_snapshot_id(&self.pool).await
    }

    /// List all schemas at the current snapshot
    pub async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        let snapshot_id = self.current_snapshot().await?.unwrap_or(0);
        self.list_schemas_at_snapshot(snapshot_id).await
    }

    /// List all schemas at a specific snapshot
    pub async fn list_schemas_at_snapshot(&self, snapshot_id: i64) -> Result<Vec<SchemaInfo>> {
        ReadQueries::list_schemas(&self.pool, snapshot_id).await
    }

    /// List all tables in a schema at the current snapshot
    pub async fn list_tables(&self, schema_id: i64) -> Result<Vec<TableInfo>> {
        let snapshot_id = self.current_snapshot().await?.unwrap_or(0);
        self.list_tables_at_snapshot(schema_id, snapshot_id).await
    }

    /// List all tables in a schema at a specific snapshot
    pub async fn list_tables_at_snapshot(
        &self,
        schema_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<TableInfo>> {
        ReadQueries::list_tables(&self.pool, schema_id, snapshot_id).await
    }

    /// Get table structure (columns) at the current snapshot
    pub async fn table_structure(&self, table_id: i64) -> Result<Vec<ColumnInfo>> {
        let snapshot_id = self.current_snapshot().await?.unwrap_or(0);
        self.table_structure_at_snapshot(table_id, snapshot_id)
            .await
    }

    /// Get table structure (columns) at a specific snapshot
    pub async fn table_structure_at_snapshot(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<ColumnInfo>> {
        ReadQueries::show_table_structure(&self.pool, table_id, snapshot_id).await
    }

    /// List data files for a table at the current snapshot
    pub async fn list_data_files(&self, table_id: i64) -> Result<Vec<DataFileInfo>> {
        let snapshot_id = self.current_snapshot().await?.unwrap_or(0);
        self.list_data_files_at_snapshot(table_id, snapshot_id)
            .await
    }

    /// List data files for a table at a specific snapshot
    pub async fn list_data_files_at_snapshot(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<DataFileInfo>> {
        ReadQueries::list_data_files(&self.pool, table_id, snapshot_id).await
    }

    /// Prune files by column statistics for efficient querying
    pub async fn prune_files_by_column_stats(
        &self,
        table_id: i64,
        column_id: i64,
        value: &str,
    ) -> Result<Vec<i64>> {
        ReadQueries::prune_files_by_column_stats(&self.pool, table_id, column_id, value).await
    }

    /// Create a new schema in a transaction
    pub async fn create_schema(&self, schema_name: &str) -> Result<SchemaInfo> {
        let mut tx = self.pool.begin().await?;
        let result = self.create_schema_tx(&mut tx, schema_name).await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Create a new schema within an existing transaction
    pub async fn create_schema_tx(
        &self,
        tx: &mut Transaction<'_, Any>,
        schema_name: &str,
    ) -> Result<SchemaInfo> {
        let snapshot_context = SnapshotContext::new(&self.pool).await?;
        let schema_id = snapshot_context.next_catalog_id;

        WriteQueries::create_schema(
            &self.pool,
            schema_id,
            Uuid::new_v4(),
            snapshot_context.snapshot_id,
            schema_name,
        )
        .await?;

        snapshot_context
            .commit_with_changes(&self.pool, &format!("CREATE SCHEMA {}", schema_name))
            .await?;

        Ok(SchemaInfo {
            schema_id,
            schema_name: schema_name.to_string(),
        })
    }

    /// Create a new table in a transaction
    pub async fn create_table(
        &self,
        schema_id: i64,
        table_name: &str,
        columns: Vec<ColumnDefinition>,
    ) -> Result<TableInfo> {
        let mut tx = self.pool.begin().await?;
        let result = self
            .create_table_tx(&mut tx, schema_id, table_name, columns)
            .await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Create a new table within an existing transaction
    pub async fn create_table_tx(
        &self,
        tx: &mut Transaction<'_, Any>,
        schema_id: i64,
        table_name: &str,
        columns: Vec<ColumnDefinition>,
    ) -> Result<TableInfo> {
        let mut snapshot_context = SnapshotContext::new(&self.pool).await?;
        let table_id = snapshot_context.next_catalog_id;
        snapshot_context.next_catalog_id += 1;

        WriteQueries::create_table(
            &self.pool,
            table_id,
            Uuid::new_v4(),
            snapshot_context.snapshot_id,
            schema_id,
            table_name,
        )
        .await?;

        // Create columns
        for (index, column) in columns.into_iter().enumerate() {
            WriteQueries::create_column(
                &self.pool,
                column.column_id.unwrap_or(index as i64 + 1),
                snapshot_context.snapshot_id,
                table_id,
                index as i64,
                &column.name,
                &column.data_type,
                column.nullable,
            )
            .await?;
        }

        snapshot_context
            .commit_with_changes(&self.pool, &format!("CREATE TABLE {}", table_name))
            .await?;

        Ok(TableInfo {
            table_id,
            table_name: table_name.to_string(),
        })
    }

    /// Insert data file and update statistics in a transaction
    pub async fn insert_data_file(
        &self,
        table_id: i64,
        file_path: &str,
        record_count: i64,
        file_size_bytes: i64,
        column_statistics: Vec<FileColumnStatistics>,
    ) -> Result<i64> {
        let mut tx = self.pool.begin().await?;
        let result = self
            .insert_data_file_tx(
                &mut tx,
                table_id,
                file_path,
                record_count,
                file_size_bytes,
                column_statistics,
            )
            .await?;
        tx.commit().await?;
        Ok(result)
    }

    /// Insert data file within an existing transaction
    pub async fn insert_data_file_tx(
        &self,
        tx: &mut Transaction<'_, Any>,
        table_id: i64,
        file_path: &str,
        record_count: i64,
        file_size_bytes: i64,
        column_statistics: Vec<FileColumnStatistics>,
    ) -> Result<i64> {
        let mut snapshot_context = SnapshotContext::new(&self.pool).await?;
        let data_file_id = snapshot_context.next_file_id;
        snapshot_context.next_file_id += 1;

        // Get the next row ID for this table
        let row_id_start = ReadQueries::get_table_next_row_id(&self.pool, table_id)
            .await?
            .unwrap_or(0);

        // Insert the data file record
        WriteQueries::insert_data_file(
            &self.pool,
            data_file_id,
            table_id,
            snapshot_context.snapshot_id,
            file_path,
            true, // path_is_relative
            "parquet",
            record_count,
            file_size_bytes,
            Some(4096), // footer_size - default parquet footer size
            row_id_start,
        )
        .await?;

        // Update table statistics
        WriteQueries::update_table_stats(&self.pool, table_id, record_count, file_size_bytes)
            .await?;

        // Insert file-level column statistics
        for stat in column_statistics {
            WriteQueries::insert_file_column_stats(
                &self.pool,
                data_file_id,
                table_id,
                stat.column_id,
                stat.value_count,
                stat.null_count,
                stat.nan_count,
                stat.min_value.as_deref(),
                stat.max_value.as_deref(),
            )
            .await?;

            // Update table-level column statistics
            WriteQueries::update_table_column_stats(
                &self.pool,
                table_id,
                stat.column_id,
                stat.null_count,
                stat.nan_count,
                stat.min_value.as_deref(),
                stat.max_value.as_deref(),
            )
            .await?;
        }

        snapshot_context
            .commit_with_changes(&self.pool, &format!("INSERT DATA FILE {}", file_path))
            .await?;

        Ok(data_file_id)
    }
}

/// Column definition for table creation
#[derive(Debug, Clone)]
pub struct ColumnDefinition {
    pub column_id: Option<i64>,
    pub name: String,
    pub data_type: String,
    pub nullable: bool,
}

/// File-level column statistics for data insertion
#[derive(Debug, Clone)]
pub struct FileColumnStatistics {
    pub column_id: i64,
    pub value_count: i64,
    pub null_count: i64,
    pub nan_count: i64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
}

/// Internal helper for managing snapshot context within transactions
struct SnapshotContext {
    snapshot_id: i64,
    schema_version: i64,
    next_catalog_id: i64,
    next_file_id: i64,
}

impl SnapshotContext {
    /// Create a new snapshot context for a transaction
    async fn new(pool: &AnyPool) -> Result<Self> {
        let max_snapshot_id = ReadQueries::get_max_snapshot_id(pool).await?;
        let snapshot_id = max_snapshot_id + 1;

        // Get the next IDs from the current snapshot
        let next_catalog_id = ReadQueries::get_next_catalog_id(pool).await?.unwrap_or(1);
        let next_file_id = ReadQueries::get_next_file_id(pool).await?.unwrap_or(1);

        // For now, we'll use a simple schema version increment
        // In a real implementation, this should track actual schema changes
        let schema_version = max_snapshot_id + 1;

        Ok(Self {
            snapshot_id,
            schema_version,
            next_catalog_id,
            next_file_id,
        })
    }

    /// Commit the snapshot with changes
    async fn commit_with_changes(&self, pool: &AnyPool, changes: &str) -> Result<()> {
        // Create the snapshot
        WriteQueries::create_snapshot(
            pool,
            self.snapshot_id,
            Utc::now(),
            self.schema_version,
            self.next_catalog_id,
            self.next_file_id,
        )
        .await?;

        // Log the changes
        WriteQueries::log_snapshot_changes(pool, self.snapshot_id, changes).await?;

        Ok(())
    }
}

/// Time travel operations for querying historical data
pub struct TimeTravel<'a> {
    ducklake: &'a DuckLake,
    snapshot_id: i64,
}

impl<'a> TimeTravel<'a> {
    pub fn new(ducklake: &'a DuckLake, snapshot_id: i64) -> Self {
        Self {
            ducklake,
            snapshot_id,
        }
    }

    /// List schemas at this snapshot
    pub async fn list_schemas(&self) -> Result<Vec<SchemaInfo>> {
        self.ducklake
            .list_schemas_at_snapshot(self.snapshot_id)
            .await
    }

    /// List tables in a schema at this snapshot
    pub async fn list_tables(&self, schema_id: i64) -> Result<Vec<TableInfo>> {
        self.ducklake
            .list_tables_at_snapshot(schema_id, self.snapshot_id)
            .await
    }

    /// Get table structure at this snapshot
    pub async fn table_structure(&self, table_id: i64) -> Result<Vec<ColumnInfo>> {
        self.ducklake
            .table_structure_at_snapshot(table_id, self.snapshot_id)
            .await
    }

    /// List data files for a table at this snapshot
    pub async fn list_data_files(&self, table_id: i64) -> Result<Vec<DataFileInfo>> {
        self.ducklake
            .list_data_files_at_snapshot(table_id, self.snapshot_id)
            .await
    }
}

impl DuckLake {
    /// Create a time travel interface for querying at a specific snapshot
    pub fn at_snapshot(&self, snapshot_id: i64) -> TimeTravel<'_> {
        TimeTravel::new(self, snapshot_id)
    }
}
