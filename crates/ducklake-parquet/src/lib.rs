//! Parquet I/O operations for DuckLake
//!
//! This crate provides functionality for reading and writing Parquet files,
//! collecting statistics, and managing schema evolution.

use arrow::array::RecordBatch;
use ducklake_storage::FileSystem;
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod reader;
pub mod schema;
pub mod statistics;
pub mod writer;

#[derive(Error, Debug)]
pub enum ParquetError {
    #[error("Schema mismatch: {message}")]
    SchemaMismatch { message: String },
    #[error("File corruption: {path}")]
    FileCorruption { path: String },
    #[error("Arrow error: {0}")]
    ArrowError(#[from] arrow::error::ArrowError),
    #[error("Parquet error: {0}")]
    ParquetError(#[from] parquet::errors::ParquetError),
    #[error("Storage error: {0}")]
    StorageError(String),
}

pub type Result<T> = std::result::Result<T, ParquetError>;

/// Configuration for reading Parquet files
#[derive(Debug, Clone)]
pub struct ParquetReadConfig {
    pub column_selection: Option<Vec<String>>,
    pub row_group_filter: Option<String>,
    pub batch_size: usize,
}

impl Default for ParquetReadConfig {
    fn default() -> Self {
        Self {
            column_selection: None,
            row_group_filter: None,
            batch_size: 8192,
        }
    }
}

/// Configuration for writing Parquet files
#[derive(Debug, Clone)]
pub struct ParquetWriteConfig {
    pub compression: CompressionType,
    pub row_group_size: usize,
    pub enable_statistics: bool,
}

impl Default for ParquetWriteConfig {
    fn default() -> Self {
        Self {
            compression: CompressionType::Snappy,
            row_group_size: 100_000,
            enable_statistics: true,
        }
    }
}

/// Compression types for Parquet files
#[derive(Debug, Clone)]
pub enum CompressionType {
    None,
    Snappy,
    Gzip,
    Lz4,
    Zstd,
}

/// Statistics collected from a Parquet file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetFileStats {
    pub file_path: String,
    pub file_size_bytes: u64,
    pub record_count: u64,
    pub row_group_count: u32,
    pub column_stats: Vec<ParquetColumnStats>,
}

/// Statistics for a single column in a Parquet file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParquetColumnStats {
    pub column_name: String,
    pub column_id: i64,
    pub value_count: u64,
    pub null_count: u64,
    pub nan_count: u64,
    pub min_value: Option<String>,
    pub max_value: Option<String>,
    pub distinct_count: Option<u64>,
}

/// High-level interface for Parquet operations
pub struct ParquetManager {
    filesystem: Box<dyn FileSystem>,
}

impl ParquetManager {
    pub fn new(filesystem: Box<dyn FileSystem>) -> Self {
        Self { filesystem }
    }

    /// Read a Parquet file into RecordBatches
    pub async fn read_file(
        &self,
        path: &str,
        config: ParquetReadConfig,
    ) -> Result<Vec<RecordBatch>> {
        reader::read_parquet_file(&*self.filesystem, path, config).await
    }

    /// Write RecordBatches to a Parquet file
    pub async fn write_file(
        &self,
        path: &str,
        batches: Vec<RecordBatch>,
        config: ParquetWriteConfig,
    ) -> Result<ParquetFileStats> {
        writer::write_parquet_file(&*self.filesystem, path, batches, config).await
    }

    /// Collect statistics from an existing Parquet file
    pub async fn collect_statistics(&self, path: &str) -> Result<ParquetFileStats> {
        statistics::collect_file_statistics(&*self.filesystem, path).await
    }

    /// Merge multiple Parquet files into one (for compaction)
    pub async fn merge_files(
        &self,
        input_paths: &[String],
        output_path: &str,
        config: ParquetWriteConfig,
    ) -> Result<ParquetFileStats> {
        // Read all input files
        let mut all_batches = Vec::new();
        for path in input_paths {
            let batches = self.read_file(path, ParquetReadConfig::default()).await?;
            all_batches.extend(batches);
        }

        // Write merged file
        self.write_file(output_path, all_batches, config).await
    }
}
