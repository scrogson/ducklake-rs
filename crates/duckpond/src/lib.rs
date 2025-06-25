//! DuckPond - A lakehouse format implementation in Rust
//!
//! This crate implements the DuckPond specification for managing metadata
//! in a SQL database while storing data as Parquet files in object storage.

// Re-export core functionality
pub use duckpond_core::*;

// Re-export storage functionality
pub use duckpond_storage as storage;
pub use duckpond_storage::local::LocalFileSystem;
pub use duckpond_storage::FileSystem;

// Re-export parquet functionality
pub use duckpond_parquet as parquet;

// High-level lakehouse operations
pub mod lakehouse;
pub use lakehouse::Lakehouse;

// Re-export commonly used types
pub use duckpond_core::config::DuckPondConfig;
pub use duckpond_core::{ColumnDefinition, DuckPond, FileColumnStatistics};
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
    ) -> duckpond_core::Result<Self> {
        match storage_config.backend {
            StorageBackend::Local => {
                Self::new_with_local_storage(database_url, storage_config.path).await
            }
            _ => Err(duckpond_core::error::DuckPondError::ConfigError(
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
    ) -> duckpond_core::Result<Vec<arrow::array::RecordBatch>> {
        self.read_from_table(schema_name, table_name, snapshot_id)
            .await
    }
}
