use arrow::array::*;
use duckpond::{Lakehouse, StorageBackend, StorageConfig};

/// Example showing how to read data from DuckPond tables
///
/// This demonstrates using the Lakehouse API to read Parquet data
/// programmatically using Apache Arrow.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🏠 DuckPond Data Reading Example");
    println!("=================================");

    // Create storage configuration
    let storage_config = StorageConfig {
        backend: StorageBackend::Local,
        path: "./example_data".to_string(),
        bucket: None,
        region: None,
    };

    // Connect to the lakehouse
    let lakehouse =
        Lakehouse::new("sqlite://duckpond.db?mode=rwc".to_string(), storage_config).await?;

    // Read data from the user_events table
    println!("📖 Reading data from analytics.user_events...");

    match lakehouse
        .read_from_table("analytics", "user_events", None)
        .await
    {
        Ok(batches) => {
            println!("✅ Successfully read {} batches", batches.len());

            for (i, batch) in batches.iter().enumerate() {
                println!(
                    "\n📦 Batch {}: {} rows, {} columns",
                    i,
                    batch.num_rows(),
                    batch.num_columns()
                );

                // Display column names and types
                println!("Columns:");
                for field in batch.schema().fields() {
                    println!("  - {} ({})", field.name(), field.data_type());
                }

                // Display sample data
                println!("\nData (first 5 rows):");
                let max_rows = std::cmp::min(5, batch.num_rows());

                // Print header
                for field in batch.schema().fields() {
                    print!("{:>12} ", field.name());
                }
                println!();

                // Print separator
                for _ in batch.schema().fields() {
                    print!("{:>12} ", "------------");
                }
                println!();

                // Print data rows
                for row_idx in 0..max_rows {
                    for col_idx in 0..batch.num_columns() {
                        let column = batch.column(col_idx);
                        let value = format_array_value(column, row_idx);
                        print!("{:>12} ", value);
                    }
                    println!();
                }

                if batch.num_rows() > max_rows {
                    println!("... and {} more rows", batch.num_rows() - max_rows);
                }
            }
        }
        Err(e) => {
            println!("❌ Error reading data: {}", e);
            println!("💡 Make sure to run the comprehensive example first to create data!");
        }
    }

    Ok(())
}

/// Format a single value from an Arrow array for display
fn format_array_value(array: &dyn Array, row_idx: usize) -> String {
    if array.is_null(row_idx) {
        return "NULL".to_string();
    }

    match array.data_type() {
        arrow::datatypes::DataType::Int64 => {
            let array = array.as_any().downcast_ref::<Int64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        arrow::datatypes::DataType::Utf8 => {
            let array = array.as_any().downcast_ref::<StringArray>().unwrap();
            array.value(row_idx).to_string()
        }
        arrow::datatypes::DataType::Boolean => {
            let array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
            array.value(row_idx).to_string()
        }
        arrow::datatypes::DataType::Float64 => {
            let array = array.as_any().downcast_ref::<Float64Array>().unwrap();
            array.value(row_idx).to_string()
        }
        _ => format!("{:?}", array.data_type()),
    }
}
