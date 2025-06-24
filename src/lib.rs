//! DuckLake - A lakehouse format implementation in Rust
//!
//! This crate implements the DuckLake specification for managing metadata
//! in a SQL database while storing data as Parquet files in object storage.

pub mod config;
pub mod database;
pub mod ducklake;
pub mod error;
pub mod models;
pub mod queries;

pub use database::DatabaseType;
pub use ducklake::{ColumnDefinition, DuckLake, FileColumnStatistics, TimeTravel};
pub use error::{DuckLakeError, Result};

/// Re-export commonly used types
pub use sqlx::{Any, AnyPool};
pub use uuid::Uuid;
