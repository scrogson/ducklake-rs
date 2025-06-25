//! Parquet file reading functionality

use crate::{ParquetError, ParquetReadConfig, Result};
use arrow::array::RecordBatch;
use bytes::Bytes;
use duckpond_storage::FileSystem;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Read a Parquet file from storage and return RecordBatches
pub async fn read_parquet_file(
    filesystem: &dyn FileSystem,
    path: &str,
    config: ParquetReadConfig,
) -> Result<Vec<RecordBatch>> {
    // 1. Read file data from filesystem
    let data = filesystem
        .read_file(path)
        .await
        .map_err(|e| ParquetError::StorageError(e.to_string()))?;

    // 2. Create ParquetRecordBatchReaderBuilder from the data
    let bytes = Bytes::from(data);
    let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| ParquetError::ParquetError(e))?;

    // 3. Apply column selection if specified
    if let Some(columns) = &config.column_selection {
        let schema = builder.schema();
        let mut indices = Vec::new();
        for col_name in columns {
            if let Ok(field) = schema.field_with_name(col_name) {
                if let Ok(index) = schema.index_of(field.name()) {
                    indices.push(index);
                }
            }
        }
        if !indices.is_empty() {
            use parquet::arrow::ProjectionMask;
            let mask = ProjectionMask::roots(builder.parquet_schema(), indices);
            builder = builder.with_projection(mask);
        }
    }

    // 4. Set batch size and build the reader
    let reader = builder
        .with_batch_size(config.batch_size)
        .build()
        .map_err(|e| ParquetError::ParquetError(e))?;

    // 5. Read all batches
    let mut batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result.map_err(|e| ParquetError::ArrowError(e))?;
        batches.push(batch);
    }

    Ok(batches)
}
