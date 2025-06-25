//! High-level lakehouse operations that combine core, storage, and parquet functionality

use arrow::array::RecordBatch;
use duckpond_core::config::DuckPondConfig;
use duckpond_core::DuckPond;
use duckpond_parquet::{ParquetManager, ParquetReadConfig, ParquetWriteConfig};
use duckpond_storage::local::LocalFileSystem;
use duckpond_storage::FileSystem;

use uuid::Uuid;

/// High-level lakehouse interface that orchestrates all components
pub struct Lakehouse {
    core: DuckPond,
    #[allow(dead_code)] // TODO: May be used for advanced file operations
    filesystem: Box<dyn FileSystem + Send + Sync>,
    parquet_manager: ParquetManager,
}

impl Lakehouse {
    /// Create a new lakehouse instance with local storage
    pub async fn new_with_local_storage(
        database_url: String,
        storage_path: String,
    ) -> duckpond_core::Result<Self> {
        let config = DuckPondConfig {
            database_url,
            data_path: storage_path.clone(),
            max_connections: 10,
            connection_timeout_secs: 30,
        };

        let database = duckpond_core::database::Database::new(&config).await?;

        let core = DuckPond::new(database.pool().clone());
        let filesystem = Box::new(
            LocalFileSystem::new(storage_path)
                .map_err(|e| duckpond_core::error::DuckPondError::ConfigError(e.to_string()))?,
        );

        // Create ParquetManager with a clone of the filesystem
        let parquet_manager = ParquetManager::new(Box::new(
            LocalFileSystem::new(config.data_path)
                .map_err(|e| duckpond_core::error::DuckPondError::ConfigError(e.to_string()))?,
        ));

        Ok(Self {
            core,
            filesystem,
            parquet_manager,
        })
    }

    /// Write data to a table using Parquet files
    pub async fn write_to_table(
        &self,
        schema_name: &str,
        table_name: &str,
        data: Vec<RecordBatch>,
    ) -> duckpond_core::Result<()> {
        if data.is_empty() {
            return Err(duckpond_core::error::DuckPondError::ConfigError(
                "Cannot write empty data".to_string(),
            ));
        }

        // 1. Get table ID from schema and table names
        let table_id = self.get_table_id(schema_name, table_name).await?;

        // 2. Generate a unique file path
        let file_path = format!(
            "{}/{}/{}/data_{}.parquet",
            schema_name,
            table_name,
            Uuid::new_v4(),
            table_id
        );

        // 3. Write data to Parquet file using duckpond-parquet
        let write_config = ParquetWriteConfig::default();
        let file_stats = self
            .parquet_manager
            .write_file(&file_path, data.clone(), write_config)
            .await
            .map_err(|e| duckpond_core::error::DuckPondError::ConfigError(e.to_string()))?;

        // 4. Convert parquet column stats to core column stats
        let column_statistics: Vec<duckpond_core::FileColumnStatistics> = file_stats
            .column_stats
            .into_iter()
            .map(|col_stat| duckpond_core::FileColumnStatistics {
                column_id: col_stat.column_id,
                value_count: col_stat.value_count as i64,
                null_count: col_stat.null_count as i64,
                nan_count: col_stat.nan_count as i64,
                min_value: col_stat.min_value,
                max_value: col_stat.max_value,
            })
            .collect();

        // 5. Insert file record and statistics using core method
        self.core
            .insert_data_file(
                table_id,
                &file_path,
                file_stats.record_count as i64,
                file_stats.file_size_bytes as i64,
                column_statistics,
            )
            .await?;

        Ok(())
    }

    /// Read data from a table with optional time travel
    pub async fn read_from_table(
        &self,
        schema_name: &str,
        table_name: &str,
        snapshot_id: Option<Uuid>,
    ) -> duckpond_core::Result<Vec<RecordBatch>> {
        // 1. Get table metadata and file list from duckpond-core
        let table_id = self.get_table_id(schema_name, table_name).await?;
        let files = self.core.list_data_files(table_id).await?;

        if files.is_empty() {
            return Ok(Vec::new());
        }

        // 2. Read all Parquet files and combine the results
        let mut all_batches = Vec::new();
        let read_config = ParquetReadConfig::default();

        for file in files {
            // TODO: Apply snapshot filtering if snapshot_id is provided
            let _ = snapshot_id; // Silence unused warning for now

            match self
                .parquet_manager
                .read_file(&file.data_file_path, read_config.clone())
                .await
            {
                Ok(mut batches) => {
                    all_batches.append(&mut batches);
                }
                Err(e) => {
                    tracing::warn!("Failed to read file {}: {}", file.data_file_path, e);
                    // Continue reading other files rather than failing completely
                }
            }
        }

        // 3. TODO: Apply any necessary schema evolution
        // 4. TODO: Apply filters or projections if specified

        Ok(all_batches)
    }

    /// Get the underlying DuckPond core for advanced operations
    pub fn core(&self) -> &DuckPond {
        &self.core
    }

    /// Helper function to get table ID from schema and table names
    async fn get_table_id(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> duckpond_core::Result<i64> {
        // First get schema ID
        let schemas = self.core.list_schemas().await?;
        let schema_id = schemas
            .iter()
            .find(|s| s.schema_name == schema_name)
            .map(|s| s.schema_id)
            .ok_or_else(|| {
                duckpond_core::error::DuckPondError::ConfigError(format!(
                    "Schema '{}' not found",
                    schema_name
                ))
            })?;

        // Then get table ID
        let tables = self.core.list_tables(schema_id).await?;
        let table_id = tables
            .iter()
            .find(|t| t.table_name == table_name)
            .map(|t| t.table_id)
            .ok_or_else(|| {
                duckpond_core::error::DuckPondError::ConfigError(format!(
                    "Table '{}' not found in schema '{}'",
                    table_name, schema_name
                ))
            })?;

        Ok(table_id)
    }

    /// Helper function to get schema ID from schema name
    async fn get_schema_id(&self, schema_name: &str) -> duckpond_core::Result<i64> {
        let schemas = self.core.list_schemas().await?;
        schemas
            .iter()
            .find(|s| s.schema_name == schema_name)
            .map(|s| s.schema_id)
            .ok_or_else(|| {
                duckpond_core::error::DuckPondError::ConfigError(format!(
                    "Schema '{}' not found",
                    schema_name
                ))
            })
    }

    /// Create a schema
    pub async fn create_schema(&self, schema_name: &str) -> duckpond_core::Result<()> {
        self.core.create_schema(schema_name).await?;
        Ok(())
    }

    /// Create a table with the given schema
    pub async fn create_table(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: Vec<duckpond_core::ColumnDefinition>,
    ) -> duckpond_core::Result<()> {
        let schema_id = self.get_schema_id(schema_name).await?;
        self.core
            .create_table(schema_id, table_name, columns)
            .await?;
        Ok(())
    }

    /// List all schemas
    pub async fn list_schemas(&self) -> duckpond_core::Result<Vec<String>> {
        let schemas = self.core.list_schemas().await?;
        Ok(schemas.into_iter().map(|s| s.schema_name).collect())
    }

    /// List tables in a schema
    pub async fn list_tables(&self, schema_name: &str) -> duckpond_core::Result<Vec<String>> {
        let schema_id = self.get_schema_id(schema_name).await?;
        let tables = self.core.list_tables(schema_id).await?;
        Ok(tables.into_iter().map(|t| t.table_name).collect())
    }

    /// Get table structure
    pub async fn show_table_structure(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> duckpond_core::Result<Vec<duckpond_core::ColumnDefinition>> {
        let table_id = self.get_table_id(schema_name, table_name).await?;
        let columns = self.core.table_structure(table_id).await?;

        // Convert ColumnInfo to ColumnDefinition
        let column_defs = columns
            .into_iter()
            .map(|col| duckpond_core::ColumnDefinition {
                column_id: Some(col.column_id),
                name: col.column_name,
                data_type: col.column_type,
                nullable: col.nulls_allowed,
            })
            .collect();

        Ok(column_defs)
    }

    /// Compact files for a table (merge small files into larger ones)
    pub async fn compact_table(
        &self,
        schema_name: &str,
        table_name: &str,
    ) -> duckpond_core::Result<()> {
        // 1. Get list of all files for the table
        let table_id = self.get_table_id(schema_name, table_name).await?;
        let files = self.core.list_data_files(table_id).await?;

        if files.len() <= 1 {
            return Ok(()); // Nothing to compact
        }

        // 2. Read all files and merge them
        let file_paths: Vec<String> = files.iter().map(|f| f.data_file_path.clone()).collect();
        let output_path = format!(
            "{}/{}/compacted_{}.parquet",
            schema_name,
            table_name,
            Uuid::new_v4()
        );

        let write_config = ParquetWriteConfig::default();
        let _compacted_stats = self
            .parquet_manager
            .merge_files(&file_paths, &output_path, write_config)
            .await
            .map_err(|e| duckpond_core::error::DuckPondError::ConfigError(e.to_string()))?;

        // 3. TODO: Update metadata to reflect the compaction
        // - Create new file record for compacted file
        // - Mark old files as deleted or remove them
        // - Create new snapshot

        tracing::info!("Compacted {} files into {}", files.len(), output_path);
        Ok(())
    }
}
