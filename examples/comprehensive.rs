//! Comprehensive example demonstrating DuckPond functionality
//!
//! This example shows:
//! - Creating schemas and tables
//! - Writing data using different storage backends
//! - Reading data with time travel
//! - Statistics collection
//! - Table compaction

use arrow::array::{Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use duckpond::{ColumnDefinition, Lakehouse, StorageBackend, StorageConfig};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    println!("🦆 DuckPond Comprehensive Example");
    println!("==================================");

    // 1. Setup lakehouse with local storage
    let database_url = "sqlite://duckpond.db".to_string();
    let storage_path = "./example_data".to_string();

    // Ensure data directory exists
    std::fs::create_dir_all(&storage_path)?;

    let lakehouse = Lakehouse::new_with_local_storage(database_url, storage_path).await?;
    println!("✅ Created lakehouse with local storage");

    // 2. Create a schema
    let schema_name = "analytics";
    lakehouse.create_schema(schema_name).await?;
    println!("✅ Created schema: {}", schema_name);

    // 3. Create a table
    let table_name = "user_events";
    let columns = vec![
        ColumnDefinition {
            column_id: Some(1),
            name: "user_id".to_string(),
            data_type: "INT64".to_string(),
            nullable: false,
        },
        ColumnDefinition {
            column_id: Some(2),
            name: "event_type".to_string(),
            data_type: "STRING".to_string(),
            nullable: false,
        },
        ColumnDefinition {
            column_id: Some(3),
            name: "timestamp".to_string(),
            data_type: "INT64".to_string(),
            nullable: false,
        },
    ];

    lakehouse
        .create_table(schema_name, table_name, columns)
        .await?;
    println!("✅ Created table: {}.{}", schema_name, table_name);

    // 4. Create sample data
    let user_ids = Int64Array::from(vec![1001, 1002, 1003, 1001, 1004]);
    let event_types = StringArray::from(vec![
        "page_view",
        "click",
        "purchase",
        "page_view",
        "signup",
    ]);
    let timestamps = Int64Array::from(vec![
        1640995200, 1640995260, 1640995320, 1640995380, 1640995440,
    ]);

    let arrow_schema = Arc::new(Schema::new(vec![
        Field::new("user_id", DataType::Int64, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("timestamp", DataType::Int64, false),
    ]));

    let batch = RecordBatch::try_new(
        arrow_schema,
        vec![
            Arc::new(user_ids),
            Arc::new(event_types),
            Arc::new(timestamps),
        ],
    )?;

    // 5. Write data to the table
    lakehouse
        .write_to_table(schema_name, table_name, vec![batch])
        .await?;
    println!("✅ Wrote data to {}.{}", schema_name, table_name);

    // 6. Read data back
    let batches = lakehouse
        .read_from_table(schema_name, table_name, None)
        .await?;
    println!("✅ Read {} batches from table", batches.len());

    for (i, batch) in batches.iter().enumerate() {
        println!(
            "   Batch {}: {} rows, {} columns",
            i,
            batch.num_rows(),
            batch.num_columns()
        );
    }

    // 7. Show table structure
    let structure = lakehouse
        .show_table_structure(schema_name, table_name)
        .await?;
    println!("✅ Table structure:");
    for column in structure {
        println!(
            "   Column: {} ({}, nullable: {})",
            column.name, column.data_type, column.nullable
        );
    }

    // 8. List schemas and tables
    let schemas = lakehouse.list_schemas().await?;
    println!("✅ Available schemas: {:?}", schemas);

    let tables = lakehouse.list_tables(schema_name).await?;
    println!("✅ Tables in {}: {:?}", schema_name, tables);

    // 9. Demonstrate storage backend configuration examples
    println!("\n📦 Storage Backend Examples:");

    // Local storage
    let local_config = StorageConfig {
        backend: StorageBackend::Local,
        path: "./local_data".to_string(),
        bucket: None,
        region: None,
    };
    println!("   Local: {:?}", local_config);

    Ok(())
}
