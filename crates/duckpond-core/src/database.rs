use crate::{config::DuckPondConfig, error::Result};
use sqlx::any::AnyPoolOptions;
use sqlx::{Any, AnyPool, Pool};
use std::time::Duration;

/// Database connection manager for DuckPond catalog
pub struct Database {
    pool: Pool<Any>,
}

impl Database {
    /// Create a new database connection from configuration
    pub async fn new(config: &DuckPondConfig) -> Result<Self> {
        config.validate()?;

        // Install drivers for AnyPool to work
        sqlx::any::install_default_drivers();

        let pool = AnyPoolOptions::new()
            .max_connections(config.max_connections)
            .acquire_timeout(Duration::from_secs(config.connection_timeout_secs))
            .connect(&config.database_url)
            .await?;

        Ok(Self { pool })
    }

    /// Get a reference to the connection pool
    pub fn pool(&self) -> &AnyPool {
        &self.pool
    }

    /// Check if the database is healthy
    pub async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pool).await?;
        Ok(())
    }

    /// Get the current database version/state
    pub async fn get_version(&self) -> Result<String> {
        // Use database-specific version query
        let version_query = match self.get_database_type().await? {
            DatabaseType::PostgreSQL => "SELECT version()",
            DatabaseType::Postgres => "SELECT version()",
            DatabaseType::MySQL => "SELECT VERSION()",
            DatabaseType::SQLite => "SELECT sqlite_version()",
        };

        let row = sqlx::query_scalar::<_, String>(version_query)
            .fetch_one(&self.pool)
            .await?;
        Ok(row)
    }

    /// Detect the database type from the connection
    pub async fn get_database_type(&self) -> Result<DatabaseType> {
        // Try to determine database type by attempting database-specific queries
        if sqlx::query("SELECT 1::INTEGER")
            .execute(&self.pool)
            .await
            .is_ok()
        {
            return Ok(DatabaseType::PostgreSQL);
        }

        if sqlx::query("SELECT 1 FROM DUAL")
            .execute(&self.pool)
            .await
            .is_ok()
        {
            return Ok(DatabaseType::MySQL);
        }

        // Default to SQLite if other checks fail
        Ok(DatabaseType::SQLite)
    }
}

/// Create a database connection pool (standalone function)
pub async fn create_pool(database_url: &str, db_type: DatabaseType) -> Result<AnyPool> {
    // Install drivers for AnyPool to work
    sqlx::any::install_default_drivers();

    let pool = AnyPoolOptions::new()
        .max_connections(10) // Default max connections
        .acquire_timeout(Duration::from_secs(30)) // Default timeout
        .connect(database_url)
        .await?;

    let _ = db_type; // Use the parameter to avoid unused variable warning
    Ok(pool)
}

/// Supported database types
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseType {
    PostgreSQL,
    Postgres, // Alias for PostgreSQL
    MySQL,
    SQLite,
}
