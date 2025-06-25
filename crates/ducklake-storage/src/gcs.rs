//! Google Cloud Storage implementation

#[cfg(feature = "gcp")]
use crate::{FileMetadata, FileSystem, Result, StorageError};
#[cfg(feature = "gcp")]
use async_trait::async_trait;
#[cfg(feature = "gcp")]
use chrono::{DateTime, Utc};
#[cfg(feature = "gcp")]
use object_store::{gcp::GoogleCloudStorageBuilder, Error as ObjectStoreError, ObjectStore};
#[cfg(feature = "gcp")]
use std::sync::Arc;

#[cfg(feature = "gcp")]
pub struct GcsFileSystem {
    store: Arc<dyn ObjectStore>,
    bucket: String,
}

#[cfg(feature = "gcp")]
impl GcsFileSystem {
    pub fn new(bucket: String, service_account_path: Option<String>) -> Result<Self> {
        let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(&bucket);

        if let Some(path) = service_account_path {
            builder = builder.with_service_account_path(path);
        }

        let store = builder.build().map_err(|e| StorageError::BackendError {
            message: format!("Failed to create GCS client: {}", e),
        })?;

        Ok(Self {
            store: Arc::new(store),
            bucket,
        })
    }

    fn convert_object_store_error(err: ObjectStoreError, path: &str) -> StorageError {
        match err {
            ObjectStoreError::NotFound { .. } => StorageError::FileNotFound {
                path: path.to_string(),
            },
            ObjectStoreError::InvalidPath { .. } => StorageError::InvalidPath {
                path: path.to_string(),
            },
            _ => StorageError::BackendError {
                message: format!("GCS error: {}", err),
            },
        }
    }
}

#[cfg(feature = "gcp")]
#[async_trait]
impl FileSystem for GcsFileSystem {
    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        use object_store::path::Path;

        let path_obj = Path::from(path);
        let result = self
            .store
            .get(&path_obj)
            .await
            .map_err(|e| Self::convert_object_store_error(e, path))?;

        let bytes = result
            .bytes()
            .await
            .map_err(|e| StorageError::BackendError {
                message: format!("Failed to read GCS object body: {}", e),
            })?;

        Ok(bytes.to_vec())
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        use object_store::path::Path;

        let path_obj = Path::from(path);
        self.store
            .put(&path_obj, data.to_vec().into())
            .await
            .map_err(|e| Self::convert_object_store_error(e, path))?;

        Ok(())
    }

    async fn delete_file(&self, path: &str) -> Result<()> {
        use object_store::path::Path;

        let path_obj = Path::from(path);
        self.store
            .delete(&path_obj)
            .await
            .map_err(|e| Self::convert_object_store_error(e, path))?;

        Ok(())
    }

    async fn file_exists(&self, path: &str) -> Result<bool> {
        use object_store::path::Path;

        let path_obj = Path::from(path);
        match self.store.head(&path_obj).await {
            Ok(_) => Ok(true),
            Err(ObjectStoreError::NotFound { .. }) => Ok(false),
            Err(e) => Err(Self::convert_object_store_error(e, path)),
        }
    }

    async fn file_metadata(&self, path: &str) -> Result<FileMetadata> {
        use object_store::path::Path;

        let path_obj = Path::from(path);
        let meta = self
            .store
            .head(&path_obj)
            .await
            .map_err(|e| Self::convert_object_store_error(e, path))?;

        Ok(FileMetadata {
            path: path.to_string(),
            size: meta.size as u64,
            modified: Some(meta.last_modified),
            etag: meta.e_tag,
            content_type: None,
        })
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<FileMetadata>> {
        use object_store::path::Path;
        use tokio_stream::StreamExt;

        let prefix_path = if prefix.is_empty() {
            None
        } else {
            Some(Path::from(prefix))
        };

        let mut files = Vec::new();
        let mut stream = self.store.list(prefix_path.as_ref());

        while let Some(result) = stream.next().await {
            let meta = result.map_err(|e| StorageError::BackendError {
                message: format!("Failed to list GCS objects: {}", e),
            })?;

            files.push(FileMetadata {
                path: meta.location.to_string(),
                size: meta.size as u64,
                modified: Some(meta.last_modified),
                etag: meta.e_tag,
                content_type: None,
            });
        }

        Ok(files)
    }

    async fn copy_file(&self, from: &str, to: &str) -> Result<()> {
        use object_store::path::Path;

        let from_path = Path::from(from);
        let to_path = Path::from(to);

        self.store
            .copy(&from_path, &to_path)
            .await
            .map_err(|e| Self::convert_object_store_error(e, from))?;

        Ok(())
    }
}

#[cfg(not(feature = "gcp"))]
pub struct GcsFileSystem;

#[cfg(not(feature = "gcp"))]
impl GcsFileSystem {
    pub fn new(_bucket: String, _service_account_path: Option<String>) -> Result<Self> {
        Err(StorageError::BackendError {
            message: "GCP support not enabled. Enable the 'gcp' feature".to_string(),
        })
    }
}
