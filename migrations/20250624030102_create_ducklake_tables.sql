-- DuckLake 0.2 Specification - Complete Schema Migration
-- This migration creates all the catalog tables required for DuckLake format
-- Based on: https://ducklake.select/docs/stable/specification/tables/overview.html
-- Compatible with PostgreSQL, MySQL, and SQLite

-- Core metadata table for global DuckLake instance information
CREATE TABLE ducklake_metadata (
    key VARCHAR(255) NOT NULL,
    value TEXT NOT NULL
);

-- Snapshots table - tracks all snapshots (commits) in the DuckLake
CREATE TABLE ducklake_snapshot (
    snapshot_id BIGINT PRIMARY KEY,
    snapshot_time TIMESTAMP NOT NULL,
    schema_version BIGINT NOT NULL,
    next_catalog_id BIGINT NOT NULL,
    next_file_id BIGINT NOT NULL
);

-- Snapshot changes table - logs what changes were made in each snapshot
CREATE TABLE ducklake_snapshot_changes (
    snapshot_id BIGINT PRIMARY KEY,
    changes_made TEXT NOT NULL
);

-- Schema definitions table - defines schemas (collections of tables)
CREATE TABLE ducklake_schema (
    schema_id BIGINT PRIMARY KEY,
    schema_uuid VARCHAR(36) NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    schema_name VARCHAR(255) NOT NULL
);

-- Table definitions table - defines tables within schemas
CREATE TABLE ducklake_table (
    table_id BIGINT NOT NULL,
    table_uuid VARCHAR(36) NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    schema_id BIGINT NOT NULL,
    table_name VARCHAR(255) NOT NULL
);

-- View definitions table - defines SQL views
CREATE TABLE ducklake_view (
    view_id BIGINT NOT NULL,
    view_uuid VARCHAR(36) NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    schema_id BIGINT NOT NULL,
    view_name VARCHAR(255) NOT NULL,
    dialect VARCHAR(50) NOT NULL,
    sql TEXT NOT NULL,
    column_aliases TEXT
);

-- Column definitions table - defines columns for tables
CREATE TABLE ducklake_column (
    column_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    table_id BIGINT NOT NULL,
    column_order BIGINT NOT NULL,
    column_name VARCHAR(255) NOT NULL,
    column_type VARCHAR(255) NOT NULL,
    initial_default TEXT,
    default_value TEXT,
    nulls_allowed BOOLEAN NOT NULL,
    parent_column BIGINT
);

-- Data files table - tracks Parquet files containing table data
CREATE TABLE ducklake_data_file (
    data_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    file_order BIGINT,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    file_format VARCHAR(50) NOT NULL,
    record_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    footer_size BIGINT NOT NULL,
    row_id_start BIGINT NOT NULL,
    partition_id BIGINT,
    encryption_key TEXT,
    partial_file_info TEXT
);

-- Delete files table - tracks Parquet files marking deleted rows
CREATE TABLE ducklake_delete_file (
    delete_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    data_file_id BIGINT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    format VARCHAR(50) NOT NULL,
    delete_count BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    footer_size BIGINT NOT NULL,
    encryption_key TEXT
);

-- Files scheduled for deletion table - cleanup management
CREATE TABLE ducklake_files_scheduled_for_deletion (
    data_file_id BIGINT NOT NULL,
    path TEXT NOT NULL,
    path_is_relative BOOLEAN NOT NULL,
    schedule_start TIMESTAMP NOT NULL
);

-- Inlined data tables - stores small data changes directly in the catalog
CREATE TABLE ducklake_inlined_data_tables (
    table_id BIGINT NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    schema_snapshot BIGINT NOT NULL
);

-- Table-level statistics
CREATE TABLE ducklake_table_stats (
    table_id BIGINT NOT NULL,
    record_count BIGINT NOT NULL,
    next_row_id BIGINT NOT NULL,
    file_size_bytes BIGINT NOT NULL
);

-- Table column-level statistics
CREATE TABLE ducklake_table_column_stats (
    table_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    contains_null BOOLEAN NOT NULL,
    contains_nan BOOLEAN,
    min_value TEXT,
    max_value TEXT
);

-- File column-level statistics
CREATE TABLE ducklake_file_column_statistics (
    data_file_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    column_size_bytes BIGINT,
    value_count BIGINT NOT NULL,
    null_count BIGINT NOT NULL,
    min_value TEXT,
    max_value TEXT,
    contains_nan BOOLEAN
);

-- Partitioning information table
CREATE TABLE ducklake_partition_info (
    partition_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT
);

-- Partition column definitions
CREATE TABLE ducklake_partition_column (
    partition_id BIGINT NOT NULL,
    table_id BIGINT NOT NULL,
    partition_key_index BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    transform VARCHAR(255) NOT NULL
);

-- File partition values
CREATE TABLE ducklake_file_partition_value (
    data_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    partition_key_index BIGINT NOT NULL,
    partition_value TEXT NOT NULL
);

-- General-purpose tags for database objects
CREATE TABLE ducklake_tag (
    object_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    key VARCHAR(255) NOT NULL,
    value TEXT NOT NULL
);

-- Column-specific tags
CREATE TABLE ducklake_column_tag (
    table_id BIGINT NOT NULL,
    column_id BIGINT NOT NULL,
    begin_snapshot BIGINT NOT NULL,
    end_snapshot BIGINT,
    key VARCHAR(255) NOT NULL,
    value TEXT NOT NULL
);

-- Create indexes for performance on frequently queried columns
CREATE INDEX idx_ducklake_snapshot_time ON ducklake_snapshot(snapshot_time);
CREATE INDEX idx_ducklake_table_schema ON ducklake_table(schema_id);
CREATE INDEX idx_ducklake_column_table ON ducklake_column(table_id);
CREATE INDEX idx_ducklake_data_file_table ON ducklake_data_file(table_id);
CREATE INDEX idx_ducklake_data_file_snapshot ON ducklake_data_file(begin_snapshot);
CREATE INDEX idx_ducklake_delete_file_table ON ducklake_delete_file(table_id);
CREATE INDEX idx_ducklake_table_stats_table ON ducklake_table_stats(table_id);
CREATE INDEX idx_ducklake_partition_info_table ON ducklake_partition_info(table_id);
