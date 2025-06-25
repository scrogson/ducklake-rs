//! Parquet file writing functionality

use crate::{
    CompressionType, ParquetColumnStats, ParquetError, ParquetFileStats, ParquetWriteConfig, Result,
};
use arrow::array::RecordBatch;
use ducklake_storage::FileSystem;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use std::io::Cursor;

/// Write RecordBatches to a Parquet file and return statistics
pub async fn write_parquet_file(
    filesystem: &dyn FileSystem,
    path: &str,
    batches: Vec<RecordBatch>,
    config: ParquetWriteConfig,
) -> Result<ParquetFileStats> {
    if batches.is_empty() {
        return Err(ParquetError::SchemaMismatch {
            message: "Cannot write empty batches".to_string(),
        });
    }

    // 1. Create WriterProperties with the specified configuration
    let compression = match config.compression {
        CompressionType::None => Compression::UNCOMPRESSED,
        CompressionType::Snappy => Compression::SNAPPY,
        CompressionType::Gzip => Compression::GZIP(Default::default()),
        CompressionType::Lz4 => Compression::LZ4,
        CompressionType::Zstd => Compression::ZSTD(Default::default()),
    };

    let props = WriterProperties::builder()
        .set_compression(compression)
        .set_max_row_group_size(config.row_group_size)
        .set_write_batch_size(1024)
        .set_statistics_enabled(if config.enable_statistics {
            parquet::file::properties::EnabledStatistics::Chunk
        } else {
            parquet::file::properties::EnabledStatistics::None
        })
        .build();

    // 2. Create an in-memory buffer to write to
    let mut buffer = Vec::new();
    let cursor = Cursor::new(&mut buffer);

    // 3. Create ArrowWriter and write batches
    let schema = batches[0].schema();
    let mut writer = ArrowWriter::try_new(cursor, schema.clone(), Some(props))
        .map_err(|e| ParquetError::ParquetError(e))?;

    let mut total_record_count = 0u64;
    for batch in &batches {
        // Validate schema consistency
        if batch.schema() != schema {
            return Err(ParquetError::SchemaMismatch {
                message: "All batches must have the same schema".to_string(),
            });
        }

        total_record_count += batch.num_rows() as u64;
        writer
            .write(&batch)
            .map_err(|e| ParquetError::ParquetError(e))?;
    }

    // 4. Close the writer to finalize the file
    let writer_metadata = writer.close().map_err(|e| ParquetError::ParquetError(e))?;

    // 5. Upload the file to storage
    filesystem
        .write_file(path, &buffer)
        .await
        .map_err(|e| ParquetError::StorageError(e.to_string()))?;

    // 6. Collect basic statistics
    let file_size_bytes = buffer.len() as u64;
    let row_group_count = 1; // Simplified for now

    let mut column_stats = Vec::new();
    if config.enable_statistics {
        for (col_index, field) in schema.fields().iter().enumerate() {
            // Create basic column statistics
            // TODO: Extract real statistics from the parquet metadata when the API is more stable
            column_stats.push(ParquetColumnStats {
                column_name: field.name().clone(),
                column_id: col_index as i64,
                value_count: total_record_count,
                null_count: 0, // TODO: Calculate from actual data
                nan_count: 0,
                min_value: None, // TODO: Calculate from actual data
                max_value: None, // TODO: Calculate from actual data
                distinct_count: None,
            });
        }
    }

    Ok(ParquetFileStats {
        file_path: path.to_string(),
        file_size_bytes,
        record_count: total_record_count,
        row_group_count,
        column_stats,
    })
}
