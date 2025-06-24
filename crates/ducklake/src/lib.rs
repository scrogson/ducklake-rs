//! DuckLake - A lakehouse format implementation in Rust
//!
//! This crate implements the DuckLake specification for managing metadata
//! in a SQL database while storing data as Parquet files in object storage.

// Re-export core functionality
pub use ducklake_core::*;

// Re-export storage functionality
pub use ducklake_storage as storage;
pub use ducklake_storage::local::LocalFileSystem;
pub use ducklake_storage::FileSystem;

// Re-export parquet functionality
pub use ducklake_parquet as parquet;

// High-level lakehouse operations
pub mod lakehouse;
pub use lakehouse::Lakehouse;

// Re-export commonly used types
pub use ducklake_core::config::DuckLakeConfig;
pub use ducklake_core::{ColumnDefinition, DuckLake, FileColumnStatistics};
pub use sqlx::{Any, AnyPool};
pub use uuid::Uuid;

// Configuration and utility types
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub path: String,
    pub bucket: Option<String>,
    pub region: Option<String>,
}

#[derive(Debug, Clone)]
pub enum StorageBackend {
    Local,
    S3,
    GCS,
    Azure,
}

impl Lakehouse {
    /// Create a new lakehouse with the specified configuration
    pub async fn new(
        database_url: String,
        storage_config: StorageConfig,
    ) -> ducklake_core::Result<Self> {
        match storage_config.backend {
            StorageBackend::Local => {
                Self::new_with_local_storage(database_url, storage_config.path).await
            }
            _ => Err(ducklake_core::error::DuckLakeError::ConfigError(
                "Only local storage is currently implemented".to_string(),
            )),
        }
    }
}

// Example data structure for table operations
pub struct TableData {
    pub schema_name: String,
    pub table_name: String,
    pub data: Vec<arrow::array::RecordBatch>,
}

/// Simple table query interface
impl Lakehouse {
    /// Query a table and return results as RecordBatches
    pub async fn query_table(
        &self,
        schema_name: &str,
        table_name: &str,
        snapshot_id: Option<uuid::Uuid>,
    ) -> ducklake_core::Result<Vec<arrow::array::RecordBatch>> {
        self.read_from_table(schema_name, table_name, snapshot_id)
            .await
    }
}
