//! DuckPond - A lakehouse format implementation in Rust
//!
//! This crate implements the DuckPond specification for managing metadata
//! in a SQL database while storing data as Parquet files in object storage.

pub mod config;
pub mod database;
pub mod duckpond;
pub mod error;
pub mod models;
pub mod queries;

pub use database::DatabaseType;
pub use duckpond::{ColumnDefinition, DuckPond, FileColumnStatistics, TimeTravel};
pub use error::{DuckPondError, Result};

/// Re-export commonly used types
pub use sqlx::{Any, AnyPool};
pub use uuid::Uuid;
