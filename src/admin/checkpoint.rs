use std::path::Path;
use std::time::Instant;

use crate::primitives::pager::{CheckpointMode, PageStore, MVCC_READER_WARN_THRESHOLD_MS};
use serde::Serialize;

use crate::admin::options::AdminOpenOptions;
use crate::admin::util::open_pager;
use crate::admin::Result;

/// Report generated after executing a checkpoint operation.
#[derive(Debug, Clone, Serialize)]
pub struct CheckpointReport {
    /// The checkpoint mode used (e.g., "force", "best_effort").
    pub mode: String,
    /// Duration of the checkpoint operation in milliseconds.
    pub duration_ms: f64,
    /// The LSN (log sequence number) of the last checkpoint.
    pub last_checkpoint_lsn: u64,
    /// Number of active MVCC readers when the checkpoint completed.
    pub mvcc_readers_active: u64,
    /// Oldest reader snapshot commit.
    pub mvcc_reader_oldest_snapshot: u64,
    /// Maximum reader age observed in milliseconds.
    pub mvcc_reader_max_age_ms: u64,
    /// Optional warning describing reader lag.
    pub mvcc_warning: Option<String>,
}

/// Executes a checkpoint operation on a database.
///
/// Opens the database at the given path, performs a checkpoint with the specified mode,
/// and returns a report with timing and checkpoint information.
///
/// # Errors
///
/// Returns an error if the database cannot be opened or the checkpoint fails.
pub fn checkpoint(
    path: impl AsRef<Path>,
    opts: &AdminOpenOptions,
    mode: CheckpointMode,
) -> Result<CheckpointReport> {
    let path = path.as_ref();
    let pager = open_pager(path, opts)?;
    let start = Instant::now();
    pager.checkpoint(mode)?;
    let elapsed = start.elapsed();
    let meta = pager.meta()?;
    let pager_stats = pager.stats();
    let mvcc_warning = if pager_stats.mvcc_readers_active > 0
        && pager_stats.mvcc_reader_max_age_ms >= MVCC_READER_WARN_THRESHOLD_MS
    {
        Some(format!(
            "slow readers max_age={} ms are blocking MVCC cleanup",
            pager_stats.mvcc_reader_max_age_ms
        ))
    } else {
        None
    };
    Ok(CheckpointReport {
        mode: mode_string(mode),
        duration_ms: elapsed.as_secs_f64() * 1_000.0,
        last_checkpoint_lsn: meta.last_checkpoint_lsn.0,
        mvcc_readers_active: pager_stats.mvcc_readers_active,
        mvcc_reader_oldest_snapshot: pager_stats.mvcc_reader_oldest_snapshot,
        mvcc_reader_max_age_ms: pager_stats.mvcc_reader_max_age_ms,
        mvcc_warning,
    })
}

fn mode_string(mode: CheckpointMode) -> String {
    match mode {
        CheckpointMode::Force => "force",
        CheckpointMode::BestEffort => "best_effort",
    }
    .to_string()
}
