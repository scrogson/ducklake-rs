use duckpond::{Lakehouse, StorageBackend, StorageConfig};
use std::process::Command;

/// Example showing how to query DuckPond data using DuckDB
///
/// This demonstrates the practical approach for querying Parquet files
/// that are managed by the DuckPond catalog.
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🦆 DuckPond + DuckDB Query Example");
    println!("===================================");

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

    // 1. Use the catalog to discover tables and their file locations
    println!("📋 Discovering tables in the catalog...");
    let schemas = lakehouse.core().list_schemas().await?;

    for schema in &schemas {
        println!("Schema: {}", schema.schema_name);
        let tables = lakehouse.core().list_tables(schema.schema_id).await?;

        for table in &tables {
            println!("  Table: {}", table.table_name);

            // Get data files for this table
            let data_files = lakehouse.core().list_data_files(table.table_id).await?;
            println!("    Data files:");

            for file in &data_files {
                println!("      - {}", file.data_file_path);

                // 2. Query the Parquet file directly with DuckDB
                query_with_duckdb(&file.data_file_path, &schema.schema_name, &table.table_name)?;
            }
        }
    }

    Ok(())
}

/// Query a Parquet file using DuckDB command line
fn query_with_duckdb(
    file_path: &str,
    schema_name: &str,
    table_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("      🦆 Querying with DuckDB:");

    // Check if the file exists
    let full_path = format!("./example_data/{}", file_path);
    if !std::path::Path::new(&full_path).exists() {
        println!("        ❌ File not found: {}", full_path);
        return Ok(());
    }

    // Try to query with DuckDB CLI if available
    let output = Command::new("duckdb")
        .args(&["-c", &format!("SELECT * FROM '{}' LIMIT 3;", full_path)])
        .output();

    match output {
        Ok(result) => {
            if result.status.success() {
                let stdout = String::from_utf8_lossy(&result.stdout);
                println!(
                    "        ✅ Query result for {}.{}:",
                    schema_name, table_name
                );
                for line in stdout.lines() {
                    if !line.trim().is_empty() {
                        println!("           {}", line);
                    }
                }
            } else {
                let stderr = String::from_utf8_lossy(&result.stderr);
                println!("        ❌ DuckDB error: {}", stderr);
            }
        }
        Err(_) => {
            println!("        ℹ️  DuckDB CLI not available. Install with:");
            println!("           brew install duckdb");
            println!("           # or download from https://duckdb.org/");
            println!("        ℹ️  Example DuckDB SQL to query this file:");
            println!("           SELECT * FROM '{}' LIMIT 5;", full_path);
            println!("           DESCRIBE SELECT * FROM '{}';", full_path);
        }
    }

    Ok(())
}
