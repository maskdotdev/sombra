#![forbid(unsafe_code)]

//! Database administration and maintenance utilities.
//!
//! This module provides administrative functions for managing Sombra databases,
//! including checkpoint operations, statistics reporting, vacuum operations, and
//! verification tools.

mod checkpoint;
mod error;
mod options;
mod stats;
mod util;
mod vacuum;
mod verify;

/// Initiates a checkpoint of the write-ahead log into the database.
///
/// A checkpoint transfers committed WAL entries back into the main database file,
/// reducing the WAL size and improving read performance.
pub use checkpoint::{checkpoint, CheckpointReport};

/// Error types for administrative operations.
///
/// Defines error conditions that can occur during administrative tasks.
pub use error::{AdminError, Result};

/// Configuration options for opening a database in admin mode.
///
/// Controls how the database is opened for administrative operations.
pub use options::AdminOpenOptions;

/// Statistics collection and reporting.
///
/// Provides functions to gather and report detailed statistics about the database,
/// including pager, storage, and WAL statistics.
pub use stats::{
    stats, FilesystemStats, PagerStatsSection, StatsReport, StorageStatsSection, WalStatsSection,
};

/// Database vacuum (defragmentation) operations.
///
/// Vacuum reclaims unused space in the database file and can optimize data layout.
pub use vacuum::{vacuum_into, VacuumOptions, VacuumReport};

/// Database integrity verification.
///
/// Verifies the structural integrity of the database and reports any issues found.
pub use verify::{verify, VerifyCounts, VerifyFinding, VerifyLevel, VerifyReport, VerifySeverity};

pub use crate::primitives::pager::{CheckpointMode, PagerOptions};

/// Utility functions for opening database components.
pub use util::{open_graph, open_pager, GraphHandle};
