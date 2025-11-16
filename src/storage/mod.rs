//! Graph storage engine and core data structures.
//!
//! Implements persistent storage for nodes and edges, including B-tree based indices,
//! property storage, and various graph query operations.

/// B-tree data structure implementation.
///
/// Core B-tree indexes used for storing and retrieving graph data efficiently.
pub mod btree;

/// Data catalog and schema information.
///
/// Manages schema definitions, labels, property types, and other metadata.
pub mod catalog;

/// Core database and graph management.
///
/// Main graph storage engine and database operations.
pub mod core;

/// Index management and query operations.
///
/// Implements indices for efficient lookups by label, property, and other dimensions.
pub mod index;

/// Vertical storage (vstore) for columnar data.
///
/// Alternative storage format for efficient bulk queries and analytics.
pub mod vstore;

mod adjacency;
mod edge;
mod graph;
mod metrics;
mod mvcc;
mod node;
mod options;
mod patch;
mod profile;
mod props;
mod rowhash;
mod types;

/// Main database interface.
///
/// The core abstraction for database operations.
pub use core::Db;

#[cfg(feature = "degree-cache")]
/// Direction specification for neighbor queries with degree caching enabled.
pub use adjacency::DegreeDir;

/// Graph traversal and adjacency operations.
pub use adjacency::{Dir, ExpandOpts, Neighbor, NeighborCursor};

/// Core graph storage implementation.
pub use graph::{
    BfsOptions, BfsVisit, BulkEdgeValidator, CreateEdgeOptions, Graph, GraphWriter,
    GraphWriterStats, PropStats, DEFAULT_INLINE_PROP_BLOB, DEFAULT_INLINE_PROP_VALUE,
    STORAGE_FLAG_DEGREE_CACHE,
};

/// Index definitions and label scan operations.
pub use index::{IndexDef, IndexKind, LabelScan, TypeTag};
pub use mvcc::{
    flags as mvcc_flags, CommitId, VersionHeader, VersionedValue, COMMIT_MAX, VERSION_HEADER_LEN,
};

/// Metrics and profiling.
pub use metrics::{default_metrics, CounterMetrics, NoopMetrics, StorageMetrics};

/// Graph configuration options.
pub use options::GraphOptions;

/// Property patch operations for updates.
pub use patch::{PropPatch, PropPatchOp};

pub use profile::profile_snapshot as storage_profile_snapshot;
/// Storage layer profiling and statistics.
pub use profile::{
    profile_scope, profile_snapshot, record_btree_leaf_key_cmps, record_btree_leaf_key_decodes,
    record_btree_leaf_memcopy_bytes, record_pager_commit_borrowed_bytes, record_pager_fsync,
    record_pager_wal_bytes, record_pager_wal_frames, record_wal_coalesced_writes,
    record_wal_io_group_sample, StorageProfileKind, StorageProfileSnapshot,
};

/// Node, edge, and property data types and operations.
pub use types::{
    DeleteMode, DeleteNodeOpts, EdgeData, EdgeSpec, NodeData, NodeSpec, PropEntry, PropValue,
    PropValueOwned,
};
