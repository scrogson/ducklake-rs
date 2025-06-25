# DuckPond CLI

Command-line tools for DuckPond administration.

## Installation

```bash
cargo build --release
```

## Usage

### Database Migrations

Before using DuckPond, you need to run database migrations to set up the catalog schema:

```bash
# Run all pending migrations
duckpond --database-url "sqlite://duckpond.db" migrate run

# Show migration status
duckpond --database-url "sqlite://duckpond.db" migrate info
```

### Managing Schemas and Tables

```bash
# List all schemas
duckpond --database-url "sqlite://duckpond.db" list-schemas

# Create a new schema
duckpond --database-url "sqlite://duckpond.db" create-schema my_schema

# Create a new table
duckpond --database-url "sqlite://duckpond.db" create-table \
  --columns '[{"name": "id", "type": "bigint", "nullable": false}, {"name": "name", "type": "string", "nullable": true}]' \
  my_schema my_table

# List tables in a schema
duckpond --database-url "sqlite://duckpond.db" list-tables my_schema

# Show table structure
duckpond --database-url "sqlite://duckpond.db" show-table my_schema my_table
```

### Environment Variables

You can set environment variables to avoid repeating common options:

```bash
export DUCKLAKE_DATABASE_URL="sqlite://duckpond.db"
export DUCKLAKE_DATA_PATH="./data"
export DUCKLAKE_STORAGE_BACKEND="local"

# Now you can run commands without specifying the database URL
duckpond migrate run
duckpond list-schemas
```

### Migration Workflow

1. **First time setup**: Run `duckpond migrate run` to initialize the database schema
2. **Regular usage**: Use other commands to manage schemas, tables, and data
3. **Updates**: When upgrading DuckPond, run `duckpond migrate run` again to apply any new migrations

### Supported Databases

- SQLite
- PostgreSQL
- MySQL

Example connection URLs:

- SQLite: `sqlite://path/to/database.db`
- PostgreSQL: `postgresql://user:password@localhost/dbname`
- MySQL: `mysql://user:password@localhost/dbname`
