# DuckPond

A Rust implementation of the [DuckLake specification](https://ducklake.select/docs/stable/specification/introduction/) - a new lakehouse format that uses SQL databases for metadata management while storing data as Parquet files.

## Overview

DuckLake rethinks lakehouse architecture by:

- **Storing metadata in SQL databases** (PostgreSQL, SQLite, MySQL) instead of file-based systems
- **Keeping data in Parquet files** on object storage (S3, local filesystem, etc.)
- **Providing ACID transactions** across multiple tables
- **Supporting time travel** and schema evolution
- **Eliminating metadata file sprawl** that plagues other lakehouse formats

## Features

- DuckLake 0.2 specification implementation
- **Multi-database support**: PostgreSQL, MySQL, and SQLite
- Automatic database type detection
- ACID transactions with snapshot isolation
- Schema evolution and time travel
- Rust-native with tokio and sqlx
- Parquet file management
- S3/object storage integration

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
duckpond = "0.0.1"
```

Install the CLI

```bash
cargo install duckpond
```

## Quick Start

### Prerequisites

- Rust 1.70+
- One of: PostgreSQL, MySQL, or SQLite
- `duckpond` CLI for migrations

### Setup

1. **Choose your database and set up:**

#### PostgreSQL (Recommended for Production)

```bash
# Create a PostgreSQL database
createdb duckpond

# Set environment variables
export DATABASE_URL="postgres://username:password@localhost/duckpond"
export DUCKLAKE_DATA_PATH="./data"
```

#### MySQL

```bash
# Create a MySQL database
mysql -e "CREATE DATABASE duckpond;"

# Set environment variables
export DATABASE_URL="mysql://username:password@localhost/duckpond"
export DUCKLAKE_DATA_PATH="./data"
```

#### SQLite (Great for Development)

```bash
# SQLite will create the file automatically
export DATABASE_URL="sqlite://duckpond.db"
export DUCKLAKE_DATA_PATH="./data"
```

2. **Run migrations:**

```bash
# The migrations work with all supported database types
duckpond migrate run --database-url $DATABASE_URL
```

3. **Run the example:**

```bash
cargo run --example comprehensive
cargo run --example read_data
```

## Database Migration

The migration file `crates/duckpond-cli/migrations/20250624030102_create_duckpond_tables.sql` is **cross-database compatible** and creates all 19 tables required by the DuckPond specification:

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

## Contributing

This project implements the [DuckLake specification](https://ducklake.select/docs/stable/specification/introduction/). Contributions are welcome!

## References

- [DuckLake Official Documentation](https://ducklake.select/)
- [DuckLake Blog Post](https://duckdb.org/2025/05/27/ducklake.html)
- [DuckLake Specification](https://ducklake.select/docs/stable/specification/introduction/)
- [DuckDB Extension](https://duckdb.org/docs/stable/core_extensions/ducklake.html)

## License

MIT License (following DuckLake specification license)
