# DuckLake CLI

Command-line tools for DuckLake administration.

## Installation

```bash
cargo build --release
```

## Usage

### Database Migrations

Before using DuckLake, you need to run database migrations to set up the catalog schema:

```bash
# Run all pending migrations
ducklake --database-url "sqlite://ducklake.db" migrate run

# Show migration status
ducklake --database-url "sqlite://ducklake.db" migrate info
```

### Managing Schemas and Tables

```bash
# List all schemas
ducklake --database-url "sqlite://ducklake.db" list-schemas

# Create a new schema
ducklake --database-url "sqlite://ducklake.db" create-schema my_schema

# Create a new table
ducklake --database-url "sqlite://ducklake.db" create-table \
  --columns '[{"name": "id", "type": "bigint", "nullable": false}, {"name": "name", "type": "string", "nullable": true}]' \
  my_schema my_table

# List tables in a schema
ducklake --database-url "sqlite://ducklake.db" list-tables my_schema

# Show table structure
ducklake --database-url "sqlite://ducklake.db" show-table my_schema my_table
```

### Environment Variables

You can set environment variables to avoid repeating common options:

```bash
export DUCKLAKE_DATABASE_URL="sqlite://ducklake.db"
export DUCKLAKE_DATA_PATH="./data"
export DUCKLAKE_STORAGE_BACKEND="local"

# Now you can run commands without specifying the database URL
ducklake migrate run
ducklake list-schemas
```

### Migration Workflow

1. **First time setup**: Run `ducklake migrate run` to initialize the database schema
2. **Regular usage**: Use other commands to manage schemas, tables, and data
3. **Updates**: When upgrading DuckLake, run `ducklake migrate run` again to apply any new migrations

### Supported Databases

- SQLite
- PostgreSQL
- MySQL

Example connection URLs:

- SQLite: `sqlite://path/to/database.db`
- PostgreSQL: `postgresql://user:password@localhost/dbname`
- MySQL: `mysql://user:password@localhost/dbname`
