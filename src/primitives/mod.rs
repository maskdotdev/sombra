//! Low-level primitives for building the storage engine.
//!
//! Includes byte utilities, I/O operations, concurrency controls,
//! write-ahead logging (WAL), and paging abstractions.

/// Byte-level utilities and encoding/decoding.
///
/// Low-level utilities for working with raw bytes, buffers, and encoding operations.
pub mod bytes;

/// Concurrency primitives and synchronization.
///
/// Thread-safe data structures and coordination mechanisms for concurrent access.
pub mod concurrency;

/// I/O abstractions and utilities.
///
/// Interfaces for reading/writing data and file operations.
pub mod io;

/// Paging subsystem for efficient disk I/O.
///
/// Manages page-based storage, caching, and read/write coordination.
pub mod pager;

/// Write-ahead logging (WAL) for crash recovery.
///
/// Ensures durability through sequential logging of database operations.
pub mod wal;
