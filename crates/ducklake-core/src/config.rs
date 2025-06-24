use crate::database::DatabaseType;
use crate::error::{DuckLakeError, Result};
use serde::{Deserialize, Serialize};

/// Configuration for DuckLake catalog database and data storage
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuckLakeConfig {
    /// Database connection URL (PostgreSQL, SQLite, MySQL)
    pub database_url: String,
    /// Base path for data files (S3, local filesystem, etc.)
    pub data_path: String,
    /// Maximum number of database connections
    pub max_connections: u32,
    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,
}

impl DuckLakeConfig {
    /// Create a new configuration
    pub fn new(database_url: String, data_path: String) -> Self {
        Self {
            database_url,
            data_path,
            max_connections: 10,
            connection_timeout_secs: 30,
        }
    }

    /// Create configuration from environment variables
    pub fn from_env() -> Result<Self> {
        let database_url = std::env::var("DATABASE_URL").map_err(|_| DuckLakeError::Config {
            message: "DATABASE_URL environment variable not set".to_string(),
        })?;

        let data_path =
            std::env::var("DUCKLAKE_DATA_PATH").unwrap_or_else(|_| "./data".to_string());

        Ok(Self::new(database_url, data_path))
    }

    /// Detect database type from the URL
    pub fn detect_database_type(&self) -> Result<DatabaseType> {
        if self.database_url.starts_with("postgresql://")
            || self.database_url.starts_with("postgres://")
        {
            Ok(DatabaseType::PostgreSQL)
        } else if self.database_url.starts_with("mysql://") {
            Ok(DatabaseType::MySQL)
        } else if self.database_url.starts_with("sqlite://")
            || self.database_url.ends_with(".db")
            || self.database_url.ends_with(".sqlite")
        {
            Ok(DatabaseType::SQLite)
        } else {
            Err(DuckLakeError::Config {
                message: format!("Unsupported database URL format: {}", self.database_url),
            })
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<()> {
        if self.database_url.is_empty() {
            return Err(DuckLakeError::Config {
                message: "Database URL cannot be empty".to_string(),
            });
        }

        if self.data_path.is_empty() {
            return Err(DuckLakeError::Config {
                message: "Data path cannot be empty".to_string(),
            });
        }

        // Validate database URL format
        self.detect_database_type()?;

        Ok(())
    }

    /// Get example configurations for different database types
    pub fn examples() -> Vec<(&'static str, &'static str, &'static str)> {
        vec![
            (
                "PostgreSQL",
                "postgresql://user:pass@localhost/ducklake",
                "For production use with full ACID guarantees",
            ),
            (
                "MySQL",
                "mysql://user:pass@localhost/ducklake",
                "Alternative SQL database option",
            ),
            (
                "SQLite",
                "sqlite:./ducklake.db",
                "For local development and testing",
            ),
        ]
    }
}
