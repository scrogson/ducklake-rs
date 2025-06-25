use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// DuckPond snapshot representing a commit/version
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Snapshot {
    pub snapshot_id: i64,
    pub snapshot_time: DateTime<Utc>,
    pub schema_version: i64,
    pub next_catalog_id: i64,
    pub next_file_id: i64,
}

/// DuckPond schema definition
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Schema {
    pub schema_id: i64,
    pub schema_uuid: Uuid,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub schema_name: String,
}

/// DuckPond table definition
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Table {
    pub table_id: i64,
    pub table_uuid: Uuid,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub schema_id: i64,
    pub table_name: String,
}

/// DuckPond column definition
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Column {
    pub column_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub table_id: i64,
    pub column_order: i64,
    pub column_name: String,
    pub column_type: String,
    pub initial_default: Option<String>,
    pub default_value: Option<String>,
    pub nulls_allowed: bool,
    pub parent_column: Option<i64>,
}

/// DuckPond data file tracking
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct DataFile {
    pub data_file_id: i64,
    pub table_id: i64,
    pub begin_snapshot: i64,
    pub end_snapshot: Option<i64>,
    pub file_order: Option<i64>,
    pub path: String,
    pub path_is_relative: bool,
    pub file_format: String,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub footer_size: i64,
    pub row_id_start: i64,
    pub partition_id: Option<i64>,
    pub encryption_key: Option<String>,
    pub partial_file_info: Option<String>,
}

/// DuckPond table statistics
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct TableStats {
    pub table_id: i64,
    pub record_count: i64,
    pub next_row_id: i64,
    pub file_size_bytes: i64,
}

/// Metadata key-value pairs
#[derive(Debug, Clone, Serialize, Deserialize, sqlx::FromRow)]
pub struct Metadata {
    pub key: String,
    pub value: String,
}

// Query result structs for simplified data retrieval

/// Simplified schema information for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaInfo {
    pub schema_id: i64,
    pub schema_name: String,
}

/// Simplified table information for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableInfo {
    pub table_id: i64,
    pub table_name: String,
}

/// Simplified column information for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub column_id: i64,
    pub column_name: String,
    pub column_type: String,
    pub nulls_allowed: bool,
}

/// Simplified data file information for queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataFileInfo {
    pub data_file_id: i64,
    pub data_file_path: String,
    pub path_is_relative: bool,
    pub record_count: i64,
    pub file_size_bytes: i64,
    pub delete_file_path: Option<String>,
}
