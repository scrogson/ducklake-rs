//! S3 storage implementation

#[cfg(feature = "s3")]
use crate::{FileMetadata, FileSystem, Result, StorageError};
#[cfg(feature = "s3")]
use async_trait::async_trait;
#[cfg(feature = "s3")]
use aws_sdk_s3::{Client, Config};
#[cfg(feature = "s3")]
use chrono::{DateTime, Utc};

#[cfg(feature = "s3")]
pub struct S3FileSystem {
    client: Client,
    bucket: String,
}

#[cfg(feature = "s3")]
impl S3FileSystem {
    pub fn new(bucket: String, region: String) -> Result<Self> {
        // Create AWS config with the specified region
        let config = Config::builder()
            .region(aws_sdk_s3::config::Region::new(region))
            .build();

        let client = Client::from_conf(config);

        Ok(Self { client, bucket })
    }

    /// Convert S3 error to StorageError
    fn convert_s3_error(
        err: aws_sdk_s3::error::SdkError<impl std::fmt::Debug>,
        path: &str,
    ) -> StorageError {
        match err {
            aws_sdk_s3::error::SdkError::ServiceError(service_err) => {
                let status_code = service_err.raw().status();
                if status_code.as_u16() == 404 {
                    StorageError::FileNotFound {
                        path: path.to_string(),
                    }
                } else if status_code.as_u16() == 403 {
                    StorageError::PermissionDenied {
                        path: path.to_string(),
                    }
                } else {
                    StorageError::BackendError {
                        message: format!("S3 service error: {:?}", service_err),
                    }
                }
            }
            _ => StorageError::BackendError {
                message: format!("S3 SDK error: {:?}", err),
            },
        }
    }
}

#[cfg(feature = "s3")]
#[async_trait]
impl FileSystem for S3FileSystem {
    async fn read_file(&self, path: &str) -> Result<Vec<u8>> {
        let result = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|e| Self::convert_s3_error(e, path))?;

        let body = result
            .body
            .collect()
            .await
            .map_err(|e| StorageError::BackendError {
                message: format!("Failed to read S3 object body: {}", e),
            })?;

        Ok(body.into_bytes().to_vec())
    }

    async fn write_file(&self, path: &str, data: &[u8]) -> Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(aws_sdk_s3::primitives::ByteStream::from(data.to_vec()))
            .send()
            .await
            .map_err(|e| Self::convert_s3_error(e, path))?;

        Ok(())
    }

    async fn delete_file(&self, path: &str) -> Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|e| Self::convert_s3_error(e, path))?;

        Ok(())
    }

    async fn file_exists(&self, path: &str) -> Result<bool> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
        {
            Ok(_) => Ok(true),
            Err(e) => match Self::convert_s3_error(e, path) {
                StorageError::FileNotFound { .. } => Ok(false),
                other => Err(other),
            },
        }
    }

    async fn file_metadata(&self, path: &str) -> Result<FileMetadata> {
        let result = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|e| Self::convert_s3_error(e, path))?;

        let size = result.content_length().unwrap_or(0) as u64;
        let modified = result.last_modified().map(|dt| {
            DateTime::<Utc>::from_timestamp(dt.secs(), dt.subsec_nanos()).unwrap_or_default()
        });
        let etag = result.e_tag().map(|s| s.to_string());
        let content_type = result.content_type().map(|s| s.to_string());

        Ok(FileMetadata {
            path: path.to_string(),
            size,
            modified,
            etag,
            content_type,
        })
    }

    async fn list_files(&self, prefix: &str) -> Result<Vec<FileMetadata>> {
        let mut files = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut request = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);

            if let Some(token) = &continuation_token {
                request = request.continuation_token(token);
            }

            let result = request
                .send()
                .await
                .map_err(|e| Self::convert_s3_error(e, prefix))?;

            for object in result.contents() {
                if let Some(key) = object.key() {
                    let size = object.size().unwrap_or(0) as u64;
                    let modified = object.last_modified().map(|dt| {
                        DateTime::<Utc>::from_timestamp(dt.secs(), dt.subsec_nanos())
                            .unwrap_or_default()
                    });
                    let etag = object.e_tag().map(|s| s.to_string());

                    files.push(FileMetadata {
                        path: key.to_string(),
                        size,
                        modified,
                        etag,
                        content_type: None,
                    });
                }
            }

            if result.is_truncated() == Some(true) {
                continuation_token = result.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }

        Ok(files)
    }

    async fn copy_file(&self, from: &str, to: &str) -> Result<()> {
        let copy_source = format!("{}/{}", self.bucket, from);

        self.client
            .copy_object()
            .bucket(&self.bucket)
            .key(to)
            .copy_source(&copy_source)
            .send()
            .await
            .map_err(|e| Self::convert_s3_error(e, from))?;

        Ok(())
    }
}

#[cfg(not(feature = "s3"))]
pub struct S3FileSystem;

#[cfg(not(feature = "s3"))]
impl S3FileSystem {
    pub fn new(_bucket: String, _region: String) -> Result<Self> {
        Err(StorageError::BackendError {
            message: "S3 support not enabled. Enable the 's3' feature".to_string(),
        })
    }
}
