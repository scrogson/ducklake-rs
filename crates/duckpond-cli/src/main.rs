use clap::{Parser, Subcommand};
use duckpond::{Lakehouse, StorageBackend, StorageConfig};
use sqlx::any::{install_default_drivers, AnyPoolOptions};
use std::time::Duration;

#[derive(Parser)]
#[command(name = "duckpond")]
#[command(about = "DuckPond CLI - Lakehouse format administration")]
struct Cli {
    /// Database connection URL
    #[arg(long, env = "DUCKLAKE_DATABASE_URL")]
    database_url: String,

    /// Data storage path
    #[arg(long, default_value = "data", env = "DUCKLAKE_DATA_PATH")]
    data_path: String,

    /// Storage backend type (local, s3)
    #[arg(long, default_value = "local", env = "DUCKLAKE_STORAGE_BACKEND")]
    storage_backend: String,

    /// Storage base path or S3 bucket
    #[arg(long, env = "DUCKLAKE_STORAGE_PATH")]
    storage_path: Option<String>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Database migration commands
    #[command(subcommand)]
    Migrate(MigrateCommands),
    /// Initialize a new DuckPond catalog
    Init {
        /// Force initialization even if catalog exists
        #[arg(long)]
        force: bool,
    },
    /// Create a new schema
    CreateSchema {
        /// Schema name
        name: String,
    },
    /// Create a new table
    CreateTable {
        /// Schema name
        schema: String,
        /// Table name
        table: String,
        /// Column definitions as JSON
        #[arg(long)]
        columns: String,
    },
    /// List schemas
    ListSchemas,
    /// List tables in a schema
    ListTables {
        /// Schema name
        schema: String,
    },
    /// Show table structure
    ShowTable {
        /// Schema name
        schema: String,
        /// Table name
        table: String,
    },
    /// Query data from a table
    Query {
        /// Schema name
        schema: String,
        /// Table name
        table: String,
        /// Limit number of rows returned
        #[arg(long, default_value = "10")]
        limit: usize,
        /// Column names to select (comma-separated)
        #[arg(long)]
        columns: Option<String>,
        /// Output format (table, json, csv)
        #[arg(long, default_value = "table")]
        format: String,
    },
}

#[derive(Subcommand)]
enum MigrateCommands {
    /// Run pending migrations
    Run,
    /// Revert the last migration
    Revert,
    /// Show migration status
    Info,
    /// Add a new migration
    Add {
        /// Migration name
        #[arg(long)]
        name: String,
    },
}

#[derive(Subcommand)]
enum TimeTravelCommands {
    ListSchemas,
    ListTables {
        #[arg(long)]
        schema_id: i64,
    },
    ShowTable {
        #[arg(long)]
        table_id: i64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // Create storage configuration
    let storage_backend = match cli.storage_backend.as_str() {
        "local" => StorageBackend::Local,
        "s3" => StorageBackend::S3,
        _ => {
            eprintln!("Unsupported storage backend: {}", cli.storage_backend);
            std::process::exit(1);
        }
    };

    let storage_config = StorageConfig {
        backend: storage_backend,
        path: cli.storage_path.unwrap_or_else(|| cli.data_path.clone()),
        bucket: None,
        region: None,
    };

    match cli.command {
        Commands::Migrate(migrate_cmd) => {
            match migrate_cmd {
                MigrateCommands::Run => {
                    println!("Running database migrations...");

                    // Install drivers for AnyPool to work
                    install_default_drivers();

                    let pool = AnyPoolOptions::new()
                        .max_connections(10)
                        .acquire_timeout(Duration::from_secs(30))
                        .connect(&cli.database_url)
                        .await?;

                    sqlx::migrate!("./migrations").run(&pool).await?;
                    println!("Migrations completed successfully!");
                    Ok(())
                }
                MigrateCommands::Revert => {
                    println!("Migration revert not implemented yet");
                    Ok(())
                }
                MigrateCommands::Info => {
                    println!("Migration info not implemented yet");
                    Ok(())
                }
                MigrateCommands::Add { name } => {
                    println!("Migration add not implemented yet: {}", name);
                    Ok(())
                }
            }
        }
        Commands::Init { force } => {
            println!("Initializing DuckPond catalog...");
            // TODO: Implement initialization
            println!("Force: {}", force);
            Ok(())
        }
        Commands::CreateSchema { name } => {
            let lakehouse = Lakehouse::new(cli.database_url, storage_config).await?;

            lakehouse.core().create_schema(&name).await?;
            println!("Schema '{}' created successfully", name);
            Ok(())
        }
        Commands::CreateTable {
            schema,
            table,
            columns,
        } => {
            let lakehouse = Lakehouse::new(cli.database_url, storage_config).await?;

            // Parse column definitions from JSON
            let column_defs: Vec<serde_json::Value> = serde_json::from_str(&columns)?;
            let mut duckpond_columns = Vec::new();

            for col in column_defs.iter() {
                if let Some(name) = col.get("name").and_then(|v| v.as_str()) {
                    let data_type = col.get("type").and_then(|v| v.as_str()).unwrap_or("string");
                    let nullable = col
                        .get("nullable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(true);

                    duckpond_columns.push(duckpond::ColumnDefinition {
                        column_id: None,
                        name: name.to_string(),
                        data_type: data_type.to_string(),
                        nullable,
                    });
                }
            }

            // First get the schema to find its ID
            let schemas = lakehouse.core().list_schemas().await?;
            let schema_info = schemas
                .iter()
                .find(|s| s.schema_name == schema)
                .ok_or_else(|| format!("Schema '{}' not found", schema))?;

            lakehouse
                .core()
                .create_table(schema_info.schema_id, &table, duckpond_columns)
                .await?;
            println!("Table '{}.{}' created successfully", schema, table);
            Ok(())
        }
        Commands::ListSchemas => {
            let lakehouse = Lakehouse::new(cli.database_url, storage_config).await?;
            let schemas = lakehouse.core().list_schemas().await?;

            println!("Schemas:");
            for schema in schemas {
                println!("  - {}", schema.schema_name);
            }
            Ok(())
        }
        Commands::ListTables { schema } => {
            let lakehouse = Lakehouse::new(cli.database_url, storage_config).await?;

            // First get the schema to find its ID
            let schemas = lakehouse.core().list_schemas().await?;
            let schema_info = schemas
                .iter()
                .find(|s| s.schema_name == schema)
                .ok_or_else(|| format!("Schema '{}' not found", schema))?;

            let tables = lakehouse.core().list_tables(schema_info.schema_id).await?;

            println!("Tables in schema '{}':", schema);
            for table in tables {
                println!("  - {}", table.table_name);
            }
            Ok(())
        }
        Commands::ShowTable { schema, table } => {
            let lakehouse = Lakehouse::new(cli.database_url, storage_config).await?;

            // First get the schema to find its ID
            let schemas = lakehouse.core().list_schemas().await?;
            let schema_info = schemas
                .iter()
                .find(|s| s.schema_name == schema)
                .ok_or_else(|| format!("Schema '{}' not found", schema))?;

            // Then get the table to find its ID
            let tables = lakehouse.core().list_tables(schema_info.schema_id).await?;
            let table_info = tables
                .iter()
                .find(|t| t.table_name == table)
                .ok_or_else(|| format!("Table '{}' not found in schema '{}'", table, schema))?;

            let structure = lakehouse
                .core()
                .table_structure(table_info.table_id)
                .await?;

            println!("Table structure for '{}.{}':", schema, table);
            for column in structure {
                let nullable = if column.nulls_allowed {
                    "NULL"
                } else {
                    "NOT NULL"
                };
                println!(
                    "  {} {} {}",
                    column.column_name, column.column_type, nullable
                );
            }
            Ok(())
        }
        Commands::Query {
            schema,
            table,
            limit,
            columns,
            format,
        } => {
            let lakehouse = Lakehouse::new(cli.database_url, storage_config).await?;

            // First get the schema to find its ID
            let schemas = lakehouse.core().list_schemas().await?;
            let schema_info = schemas
                .iter()
                .find(|s| s.schema_name == schema)
                .ok_or_else(|| format!("Schema '{}' not found", schema))?;

            // Then get the table to find its ID
            let tables = lakehouse.core().list_tables(schema_info.schema_id).await?;
            let table_info = tables
                .iter()
                .find(|t| t.table_name == table)
                .ok_or_else(|| format!("Table '{}' not found in schema '{}'", table, schema))?;

            let query_result = lakehouse
                .core()
                .query_data(table_info.table_id, limit, columns, format)
                .await?;

            println!("Query result:");
            for row in query_result {
                println!("{}", row);
            }
            Ok(())
        }
    }
}
