#![forbid(unsafe_code)]

//! Command-line interface utilities and data import/export functionality.
//!
//! This module provides tools for CLI operations, particularly for importing
//! and exporting graph data in various formats.

/// Data import and export operations.
///
/// Handles loading graph data from external sources and exporting database
/// contents to various formats (CSV, Parquet, etc.).
pub mod import_export;
