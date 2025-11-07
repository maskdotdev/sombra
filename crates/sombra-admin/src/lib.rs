#![forbid(unsafe_code)]

mod checkpoint;
mod error;
mod options;
mod stats;
mod util;
mod vacuum;
mod verify;

pub use checkpoint::{checkpoint, CheckpointReport};
pub use error::{AdminError, Result};
pub use options::AdminOpenOptions;
pub use stats::{
    stats, FilesystemStats, PagerStatsSection, StatsReport, StorageStatsSection, WalStatsSection,
};
pub use vacuum::{vacuum_into, VacuumOptions, VacuumReport};
pub use verify::{verify, VerifyCounts, VerifyFinding, VerifyLevel, VerifyReport, VerifySeverity};

pub use sombra_pager::{CheckpointMode, PagerOptions};
pub use util::{open_graph, open_pager, GraphHandle};
