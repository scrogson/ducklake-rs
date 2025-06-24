//! File system abstraction for DuckLake
//!
//! This crate provides a unified interface for working with different storage backends:
//! - Local file system
//! - Amazon S3
//! - Google Cloud Storage  
//! - Azure Blob Storage

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use thiserror::Error;

pub mod local;
pub mod path;
#[cfg(feature = "s3")]
pub mod s3;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("File not found: {path}")]
    FileNotFound { path: String },
    #[error("Permission denied: {path}")]
    PermissionDenied { path: String },
    #[error("Storage backend error: {message}")]
    BackendError { message: String },
    #[error("Invalid path: {path}")]
    InvalidPath { path: String },
}

pub type Result<T> = std::result::Result<T, StorageError>;

/// Metadata about a file in storage
#[derive(Debug, Clone)]
pub struct FileMetadata {
    pub path: String,
    pub size: u64,
    pub modified: Option<DateTime<Utc>>,
    pub etag: Option<String>,
    pub content_type: Option<String>,
}

/// Configuration for storage backends
#[derive(Debug, Clone)]
pub struct StorageConfig {
    pub backend: StorageBackend,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub enum StorageBackend {
    Local {
        base_path: String,
    },
    #[cfg(feature = "s3")]
    S3 {
        bucket: String,
        region: String,
    },
}

/// Trait for file system operations
#[async_trait]
pub trait FileSystem {
    /// Read a file and return its contents
    async fn read_file(&self, path: &str) -> Result<Vec<u8>>;

    /// Write data to a file
    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()>;

    /// Delete a file
    async fn delete_file(&self, path: &str) -> Result<()>;

    /// Check if a file exists
    async fn file_exists(&self, path: &str) -> Result<bool>;

    /// Get file metadata
    async fn file_metadata(&self, path: &str) -> Result<FileMetadata>;

    /// List files matching a prefix
    async fn list_files(&self, prefix: &str) -> Result<Vec<FileMetadata>>;

    /// Copy a file from one location to another
    async fn copy_file(&self, from: &str, to: &str) -> Result<()>;
}

/// Create a file system instance from configuration
pub fn create_filesystem(config: StorageConfig) -> Result<Box<dyn FileSystem>> {
    match config.backend {
        StorageBackend::Local { base_path } => {
            Ok(Box::new(local::LocalFileSystem::new(base_path)?))
        }
        #[cfg(feature = "s3")]
        StorageBackend::S3 { bucket, region } => {
            Ok(Box::new(s3::S3FileSystem::new(bucket, region)?))
        }
    }
}
