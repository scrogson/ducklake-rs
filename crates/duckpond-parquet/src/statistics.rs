//! Parquet file statistics collection

use crate::{ParquetColumnStats, ParquetError, ParquetFileStats, Result};
use bytes::Bytes;
use duckpond_storage::FileSystem;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// Collect statistics from an existing Parquet file
pub async fn collect_file_statistics(
    filesystem: &dyn FileSystem,
    path: &str,
) -> Result<ParquetFileStats> {
    // 1. Read file data from filesystem
    let data = filesystem
        .read_file(path)
        .await
        .map_err(|e| ParquetError::StorageError(e.to_string()))?;

    // 2. Get file metadata first
    let file_metadata = filesystem
        .file_metadata(path)
        .await
        .map_err(|e| ParquetError::StorageError(e.to_string()))?;

    // 3. Create ParquetRecordBatchReaderBuilder from the data
    let bytes = Bytes::from(data);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .map_err(|e| ParquetError::ParquetError(e))?;

    // 4. Get the parquet metadata
    let parquet_metadata = builder.metadata();
    let schema = builder.schema();

    // 5. Collect basic file statistics
    let row_group_count = parquet_metadata.num_row_groups() as u32;
    let mut total_record_count = 0u64;

    for rg_index in 0..row_group_count {
        let row_group = parquet_metadata.row_group(rg_index as usize);
        total_record_count += row_group.num_rows() as u64;
    }

    // 6. Collect column statistics
    let mut column_stats = Vec::new();
    for (col_index, field) in schema.fields().iter().enumerate() {
        let mut value_count = 0u64;
        let mut null_count = 0u64;
        let min_value: Option<String> = None;
        let max_value: Option<String> = None;

        // Aggregate statistics across all row groups
        for rg_index in 0..row_group_count {
            let row_group_metadata = parquet_metadata.row_group(rg_index as usize);
            if col_index < row_group_metadata.num_columns() {
                let column_metadata = row_group_metadata.column(col_index);
                if let Some(_stats) = column_metadata.statistics() {
                    // TODO: Extract statistics when API is stable
                    value_count += total_record_count;
                    null_count += 0;

                    // TODO: Extract min/max values when API is stable
                    // For now, we'll skip detailed statistics collection
                }
            }
        }

        column_stats.push(ParquetColumnStats {
            column_name: field.name().clone(),
            column_id: col_index as i64,
            value_count,
            null_count,
            nan_count: 0, // TODO: Calculate NaN count for floating point types
            min_value,
            max_value,
            distinct_count: None, // TODO: Calculate distinct count if needed
        });
    }

    Ok(ParquetFileStats {
        file_path: path.to_string(),
        file_size_bytes: file_metadata.size,
        record_count: total_record_count,
        row_group_count,
        column_stats,
    })
}

/// Convert raw bytes to string representation based on physical type
#[allow(dead_code)] // TODO: Will be used when statistics extraction is implemented
fn bytes_to_string(bytes: &[u8], physical_type: parquet::basic::Type) -> String {
    use parquet::basic::Type;

    match physical_type {
        Type::BOOLEAN => {
            if !bytes.is_empty() {
                (bytes[0] != 0).to_string()
            } else {
                "null".to_string()
            }
        }
        Type::INT32 => {
            if bytes.len() >= 4 {
                let value = i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                value.to_string()
            } else {
                "null".to_string()
            }
        }
        Type::INT64 => {
            if bytes.len() >= 8 {
                let value = i64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                value.to_string()
            } else {
                "null".to_string()
            }
        }
        Type::FLOAT => {
            if bytes.len() >= 4 {
                let value = f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                value.to_string()
            } else {
                "null".to_string()
            }
        }
        Type::DOUBLE => {
            if bytes.len() >= 8 {
                let value = f64::from_le_bytes([
                    bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
                ]);
                value.to_string()
            } else {
                "null".to_string()
            }
        }
        Type::BYTE_ARRAY | Type::FIXED_LEN_BYTE_ARRAY => {
            // For string/binary data, convert to UTF-8 string if possible
            String::from_utf8_lossy(bytes).to_string()
        }
        Type::INT96 => {
            // INT96 is typically used for timestamps, convert to hex for now
            hex::encode(bytes)
        }
    }
}
