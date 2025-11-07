#![forbid(unsafe_code)]

mod adjacency;
mod edge;
mod graph;
mod metrics;
mod node;
mod options;
mod patch;
mod profile;
mod props;
mod types;

#[cfg(feature = "degree-cache")]
pub use adjacency::DegreeDir;
pub use adjacency::{Dir, ExpandOpts, Neighbor, NeighborCursor};
pub use graph::{
    Graph, DEFAULT_INLINE_PROP_BLOB, DEFAULT_INLINE_PROP_VALUE, STORAGE_FLAG_DEGREE_CACHE,
};
pub use metrics::{default_metrics, CounterMetrics, NoopMetrics, StorageMetrics};
pub use options::GraphOptions;
pub use patch::{PropPatch, PropPatchOp};
pub use profile::{
    profile_snapshot as storage_profile_snapshot, StorageProfileKind, StorageProfileSnapshot,
};
pub use sombra_index::{IndexDef, IndexKind, LabelScan, TypeTag};
pub use types::{
    DeleteMode, DeleteNodeOpts, EdgeData, EdgeSpec, NodeData, NodeSpec, PropEntry, PropValue,
    PropValueOwned,
};
