//! Local file system implementation

use crate::{FileMetadata, FileSystem, Result, StorageError};
use async_trait::async_trait;
use std::path::PathBuf;
use tokio::fs;

pub struct LocalFileSystem {
    base_path: PathBuf,
}

impl LocalFileSystem {
    pub fn new<P: Into<PathBuf>>(base_path: P) -> Result<Self> {
        let base_path = base_path.into();
        Ok(Self { base_path })
    }

    fn resolve_path(&self, path: &str) -> PathBuf {
        if path.starts_with('/') || path.contains(':') {
            // Absolute path
            PathBuf::from(path)
        } else {
            // Relative to base path
            self.base_path.join(path)
        }
    }
}

#[async_trait]
impl FileSystem for LocalFileSystem {
    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let full_path = self.resolve_path(path);
        fs::read(&full_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::FileNotFound {
                    path: path.to_string(),
                }
            } else {
                StorageError::BackendError {
                    message: e.to_string(),
                }
            }
        })
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        let full_path = self.resolve_path(path);

        // Create parent directories if they don't exist
        if let Some(parent) = full_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::BackendError {
                    message: format!("Failed to create directories: {}", e),
                })?;
        }

        fs::write(&full_path, data)
            .await
            .map_err(|e| StorageError::BackendError {
                message: e.to_string(),
            })
    }

    async fn delete_file(&self, path: &str) -> Result<()> {
        let full_path = self.resolve_path(path);
        fs::remove_file(&full_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::FileNotFound {
                    path: path.to_string(),
                }
            } else {
                StorageError::BackendError {
                    message: e.to_string(),
                }
            }
        })
    }

    async fn file_exists(&self, path: &str) -> Result<bool> {
        let full_path = self.resolve_path(path);
        Ok(full_path.exists())
    }

    async fn file_metadata(&self, path: &str) -> Result<FileMetadata> {
        let full_path = self.resolve_path(path);
        let metadata = fs::metadata(&full_path).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::FileNotFound {
                    path: path.to_string(),
                }
            } else {
                StorageError::BackendError {
                    message: e.to_string(),
                }
            }
        })?;

        let modified = metadata
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| {
                chrono::DateTime::from_timestamp(d.as_secs() as i64, d.subsec_nanos()).unwrap()
            });

        Ok(FileMetadata {
            path: path.to_string(),
            size: metadata.len(),
            modified,
            etag: None, // Not applicable for local files
            content_type: None,
        })
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<FileMetadata>> {
        let full_path = self.resolve_path(prefix);
        let mut files = Vec::new();

        if full_path.is_file() {
            let metadata = self.file_metadata(prefix).await?;
            files.push(metadata);
        } else if full_path.is_dir() {
            let mut entries =
                fs::read_dir(&full_path)
                    .await
                    .map_err(|e| StorageError::BackendError {
                        message: e.to_string(),
                    })?;

            while let Some(entry) =
                entries
                    .next_entry()
                    .await
                    .map_err(|e| StorageError::BackendError {
                        message: e.to_string(),
                    })?
            {
                let file_path = entry.path();
                if file_path.is_file() {
                    let relative_path = file_path
                        .strip_prefix(&self.base_path)
                        .unwrap_or(&file_path)
                        .to_string_lossy()
                        .to_string();

                    if let Ok(metadata) = self.file_metadata(&relative_path).await {
                        files.push(metadata);
                    }
                }
            }
        }

        Ok(files)
    }

    async fn copy_file(&self, from: &str, to: &str) -> Result<()> {
        let from_path = self.resolve_path(from);
        let to_path = self.resolve_path(to);

        // Create parent directories if they don't exist
        if let Some(parent) = to_path.parent() {
            fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::BackendError {
                    message: format!("Failed to create directories: {}", e),
                })?;
        }

        fs::copy(&from_path, &to_path)
            .await
            .map_err(|e| StorageError::BackendError {
                message: e.to_string(),
            })?;

        Ok(())
    }
}
