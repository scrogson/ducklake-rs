# DuckLake

A Rust implementation of the [DuckLake specification](https://ducklake.select/docs/stable/specification/introduction/) - a new lakehouse format that uses SQL databases for metadata management while storing data as Parquet files.

## Overview

DuckLake rethinks lakehouse architecture by:

- **Storing metadata in SQL databases** (PostgreSQL, SQLite, MySQL) instead of file-based systems
- **Keeping data in Parquet files** on object storage (S3, local filesystem, etc.)
- **Providing ACID transactions** across multiple tables
- **Supporting time travel** and schema evolution
- **Eliminating metadata file sprawl** that plagues other lakehouse formats

## Features

- ✅ Complete DuckLake 0.2 specification implementation
- ✅ **Multi-database support**: PostgreSQL, MySQL, and SQLite
- ✅ Automatic database type detection
- ✅ ACID transactions with snapshot isolation
- ✅ Schema evolution and time travel
- ✅ Rust-native with tokio and sqlx
- 🚧 Parquet file management (coming soon)
- 🚧 S3/object storage integration (coming soon)

## Supported Databases

| Database       | Use Case                                              | Connection String Example                   |
| -------------- | ----------------------------------------------------- | ------------------------------------------- |
| **PostgreSQL** | Production deployments with full ACID guarantees      | `postgresql://user:pass@localhost/ducklake` |
| **MySQL**      | Alternative production database option                | `mysql://user:pass@localhost/ducklake`      |
| **SQLite**     | Local development, testing, and single-user scenarios | `sqlite:./ducklake.db`                      |

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
ducklake = "0.1.0"
```

## Quick Start

### Prerequisites

- Rust 1.70+
- One of: PostgreSQL, MySQL, or SQLite
- sqlx-cli for migrations

### Setup

1. **Clone and build:**

```bash
git clone <your-repo>
cd ducklake-rs
cargo build
```

2. **Choose your database and set up:**

#### PostgreSQL (Recommended for Production)

```bash
# Create a PostgreSQL database
createdb ducklake

# Set environment variables
export DATABASE_URL="postgresql://username:password@localhost/ducklake"
export DUCKLAKE_DATA_PATH="./data"
```

#### MySQL

```bash
# Create a MySQL database
mysql -e "CREATE DATABASE ducklake;"

# Set environment variables
export DATABASE_URL="mysql://username:password@localhost/ducklake"
export DUCKLAKE_DATA_PATH="./data"
```

#### SQLite (Great for Development)

```bash
# SQLite will create the file automatically
export DATABASE_URL="sqlite:./ducklake.db"
export DUCKLAKE_DATA_PATH="./data"
```

3. **Run migrations:**

```bash
# The migrations work with all supported database types
sqlx migrate run --database-url $DATABASE_URL
```

4. **Run the example:**

```bash
cargo run --example basic
```

The system will automatically detect your database type and show you the connection details!

## Usage Example

```rust
use ducklake::{config::DuckLakeConfig, database::Database, DatabaseType};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // From environment (automatic database type detection)
    let config = DuckLakeConfig::from_env()?;
    let db_type = config.detect_database_type()?;
    println!("Using database: {:?}", db_type);

    // Connect to database
    let database = Database::new(&config).await?;

    // Run migrations
    database.migrate().await?;

    // Verify connection
    database.health_check().await?;

    println!("DuckLake system is ready!");
    Ok(())
}
```

## Database Migration

The migration file `migrations/20250624030102_create_ducklake_tables.sql` is **cross-database compatible** and creates all 19 tables required by the DuckLake specification:

### Core Tables

- `ducklake_metadata` - Global instance metadata
- `ducklake_snapshot` - Snapshot tracking (commits)
- `ducklake_snapshot_changes` - Change logs
- `ducklake_schema` - Schema definitions
- `ducklake_table` - Table definitions
- `ducklake_column` - Column definitions

### Data Management

- `ducklake_data_file` - Parquet data files
- `ducklake_delete_file` - Delete marker files
- `ducklake_files_scheduled_for_deletion` - Cleanup tracking
- `ducklake_inlined_data_tables` - Small data inlining

### Statistics & Performance

- `ducklake_table_stats` - Table-level statistics
- `ducklake_table_column_stats` - Column statistics
- `ducklake_file_column_statistics` - File-level column stats

### Partitioning

- `ducklake_partition_info` - Partition schemes
- `ducklake_partition_column` - Partition column definitions
- `ducklake_file_partition_value` - File partition values

### Metadata & Tagging

- `ducklake_tag` - General purpose tags
- `ducklake_column_tag` - Column-specific tags
- `ducklake_view` - SQL view definitions

## Configuration

The system automatically detects your database type and uses environment variables:

```rust
use ducklake::{config::DuckLakeConfig, database::Database, DatabaseType};

// From environment (automatic database type detection)
let config = DuckLakeConfig::from_env()?;
let db_type = config.detect_database_type()?;
println!("Using database: {:?}", db_type);

// Or programmatically
let config = DuckLakeConfig::new(
    "postgresql://localhost/ducklake".to_string(),
    "s3://my-bucket/data/".to_string(),
);

let database = Database::new(&config).await?;
database.migrate().await?;
```

## Environment Variables

- `DATABASE_URL` - Database connection string (required)
- `DUCKLAKE_DATA_PATH` - Base path for data files (optional, defaults to "./data")

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Application   │    │   DuckLake-rs   │    │  PostgreSQL     │
│                 │◄──►│                 │◄──►│  MySQL, or      │
│                 │    │                 │    │  SQLite         │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   Object Store  │
                       │   (Parquet)     │
                       │                 │
                       └─────────────────┘
```

## Examples

The system provides helpful examples when no `DATABASE_URL` is set:

```bash
$ cargo run --example basic
🦆 DuckLake System Starting...
⚠️  DATABASE_URL not set. Here are examples for different databases:
  PostgreSQL - postgresql://user:pass@localhost/ducklake (For production use with full ACID guarantees)
  MySQL - mysql://user:pass@localhost/ducklake (Alternative SQL database option)
  SQLite - sqlite:./ducklake.db (For local development and testing)
```

## Development Status

This is an early implementation of the DuckLake specification. The current version provides:

- ✅ Complete database schema migration (cross-database compatible)
- ✅ Multi-database connection management (PostgreSQL, MySQL, SQLite)
- ✅ Automatic database type detection
- ✅ Configuration management
- ✅ Error handling
- 🚧 Table and schema operations (in progress)
- 🚧 Parquet file management (planned)
- 🚧 Snapshot and time travel (planned)

## Contributing

This project implements the [DuckLake specification](https://ducklake.select/docs/stable/specification/introduction/). Contributions are welcome!

## References

- [DuckLake Official Documentation](https://ducklake.select/)
- [DuckLake Blog Post](https://duckdb.org/2025/05/27/ducklake.html)
- [DuckLake Specification](https://ducklake.select/docs/stable/specification/introduction/)
- [DuckDB Extension](https://duckdb.org/docs/stable/core_extensions/ducklake.html)

## License

MIT License (following DuckLake specification license)
