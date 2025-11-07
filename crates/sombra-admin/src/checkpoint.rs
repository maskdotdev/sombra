use std::path::Path;
use std::time::Instant;

use serde::Serialize;
use sombra_pager::{CheckpointMode, PageStore};

use crate::options::AdminOpenOptions;
use crate::util::open_pager;
use crate::Result;

#[derive(Debug, Clone, Serialize)]
pub struct CheckpointReport {
    pub mode: String,
    pub duration_ms: f64,
    pub last_checkpoint_lsn: u64,
}

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
