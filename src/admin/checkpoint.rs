use std::path::Path;
use std::time::Instant;

use crate::primitives::pager::{CheckpointMode, PageStore};
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
    Ok(CheckpointReport {
        mode: mode_string(mode),
        duration_ms: elapsed.as_secs_f64() * 1_000.0,
        last_checkpoint_lsn: meta.last_checkpoint_lsn.0,
    })
}

fn mode_string(mode: CheckpointMode) -> String {
    match mode {
        CheckpointMode::Force => "force",
        CheckpointMode::BestEffort => "best_effort",
    }
    .to_string()
}
