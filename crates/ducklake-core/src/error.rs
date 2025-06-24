use thiserror::Error;

/// Result type alias for DuckLake operations
pub type Result<T> = std::result::Result<T, DuckLakeError>;

/// DuckLake error types
#[derive(Error, Debug)]
pub enum DuckLakeError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    #[error("Migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("Configuration error: {message}")]
    Config { message: String },

    #[error("Configuration error: {0}")]
    ConfigError(String),

    #[error("Storage error: {0}")]
    StorageError(String),

    #[error("Invalid snapshot: {snapshot_id}")]
    InvalidSnapshot { snapshot_id: i64 },

    #[error("Table not found: {table_name}")]
    TableNotFound { table_name: String },

    #[error("Schema not found: {schema_name}")]
    SchemaNotFound { schema_name: String },

    #[error("File operation error: {0}")]
    FileOperation(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("UUID error: {0}")]
    Uuid(#[from] uuid::Error),

    #[error("Transaction conflict: {message}")]
    TransactionConflict { message: String },
}
