//! DuckLake - A lakehouse format implementation in Rust
//!
//! This crate implements the DuckLake specification for managing metadata
//! in a SQL database while storing data as Parquet files in object storage.

pub mod config;
pub mod database;
pub mod error;
pub mod models;

pub use database::DatabaseType;
pub use error::{DuckLakeError, Result};

/// Re-export commonly used types
pub use sqlx::{Any, AnyPool};
pub use uuid::Uuid;
