//! Error handling for Sombra operations.
//!
//! This module defines the error types and handling utilities used
//! throughout the Sombra graph database. All public APIs return
//! `Result<T, GraphError>` for consistent error handling.
//!
//! # Error Types
//!
//! - [`GraphError`] - Main error enum with variants for different failure modes
//! - [`Result`] - Result type alias for convenience
//! - [`acquire_lock()`] - Helper for safe mutex locking
//!
//! # Error Handling Pattern
//!
//! ```rust
//! use sombra::{GraphDB, Result};
//!
//! fn safe_operation() -> Result<()> {
//!     let mut db = GraphDB::open("test.db")?;
//!     let mut tx = db.begin_transaction()?;
//!     // ... operations ...
//!     tx.commit()?;
//!     Ok(())
//! }
//! ```

use std::io;
use std::sync::{Mutex, MutexGuard};
use thiserror::Error;
use tracing::error;

/// Result type for Sombra operations.
///
/// All public APIs return `Result<T, GraphError>` for error handling.
pub type Result<T> = std::result::Result<T, GraphError>;

/// Errors that can occur during database operations.
///
/// Sombra uses this comprehensive error type to handle all failure modes
/// from I/O issues to data corruption to invalid usage.
#[derive(Debug, Error)]
pub enum GraphError {
    /// I/O error from the underlying filesystem.
    ///
    /// This can occur during file operations like reading, writing,
    /// or syncing data to disk.
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    /// Error during serialization or deserialization of data.
    ///
    /// This occurs when data cannot be properly encoded or decoded,
    /// often due to format issues or buffer size problems.
    #[error("serialization error: {0}")]
    Serialization(String),

    /// Data corruption detected.
    ///
    /// This is a critical error indicating that the database file
    /// or WAL has been corrupted. It may be recoverable with the
    /// repair tools, but data loss is possible.
    #[error("corruption detected: {0}")]
    Corruption(String),

    /// Requested resource was not found.
    ///
    /// This occurs when trying to access a node or edge that doesn't
    /// exist in the database.
    #[error("{0} not found")]
    NotFound(&'static str),

    /// Invalid argument or operation.
    ///
    /// This occurs for various reasons:
    /// - Invalid configuration parameters
    /// - Operations that violate database constraints
    /// - Transaction limits exceeded
    /// - Invalid property types for indexing
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// Feature is not yet implemented or supported.
    ///
    /// This indicates that the requested operation is not available
    /// in the current version of Sombra.
    #[error("unsupported feature: {0}")]
    UnsupportedFeature(&'static str),
}

/// Safely acquires a mutex lock with proper error handling.
///
/// This helper function handles mutex poisoning errors that can occur
/// when another thread panics while holding the lock. Instead of
/// panicking, it converts the error to a `GraphError::Corruption`.
///
/// # Arguments
/// * `mutex` - The mutex to lock
///
/// # Returns
/// A `MutexGuard` on success, or `GraphError::Corruption` if the lock is poisoned.
///
/// # Safety
/// This function is used throughout the codebase to ensure that lock
/// poisoning is handled gracefully rather than causing panics.
pub fn acquire_lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| {
        error!("Database lock poisoned - fatal error");
        GraphError::Corruption("Database lock poisoned - fatal error".into())
    })
}
