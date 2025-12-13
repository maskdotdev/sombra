//! Index-Free Adjacency (IFA) storage layer.
//!
//! This module implements native graph adjacency storage using per-node type maps
//! and adjacency segments, eliminating B-tree lookups for neighbor traversal.
//!
//! # Design Overview
//!
//! For 99% of nodes, neighbor expansion follows this path:
//! 1. Node lookup -> in-cache type map -> direct jump to adjacency segment
//! 2. Zero extra seeks, no B-tree, minimal pointer chasing
//!
//! # Key Structures
//!
//! - [`NodeAdjHeader`]: Per-node type map with inline buckets for common cases
//! - [`AdjSegment`]: MVCC-aware adjacency list segment for a (node, dir, type)
//! - [`AdjEntry`]: Single neighbor entry within a segment
//! - [`IfaStore`]: Manages NodeAdjHeader B-trees and segment operations
//! - [`SegmentManager`]: Handles segment allocation and CoW operations
//!
//! # Inline Hybrid Design
//!
//! Each node has a `NodeAdjHeader` with K inline buckets (default K=6):
//! - For nodes with ≤K-1 distinct edge types: all mappings fit inline
//! - For nodes with >K-1 types: last slot points to overflow chain
//!
//! This keeps typical nodes entirely within a single cache line while
//! gracefully handling "API gateway" nodes with many relationship types.

mod adjacency;
mod node_adj_page;
mod segment;
mod segment_manager;
mod store;
mod types;

pub use adjacency::IfaAdjacency;
#[allow(unused_imports)]
pub use node_adj_page::{NodeAdjPage, NodeAdjPagePtr, NODE_ADJ_PAGE_DATA_LEN};
#[allow(unused_imports)]
pub use segment::{
    AdjEntry, AdjSegment, AdjSegmentHeader, ADJ_ENTRY_LEN, ADJ_SEGMENT_HEADER_LEN,
    max_entries_per_page,
};
#[allow(unused_imports)]
pub use segment_manager::SegmentManager;
#[allow(unused_imports)]
pub use store::{IfaRoots, IfaStore, TypeLookupResult};
#[allow(unused_imports)]
pub use types::{
    NodeAdjHeader, OverflowBlock, SegmentPtr, TypeBucket,
    INLINE_BUCKET_COUNT, NODE_ADJ_HEADER_LEN, OVERFLOW_BLOCK_ENTRIES, OVERFLOW_BLOCK_LEN,
    OVERFLOW_TAG, SEGMENT_PTR_LEN, TYPE_BUCKET_LEN,
};

use crate::storage::mvcc::CommitId;

/// Transaction ID type alias for MVCC operations.
pub type TxId = CommitId;

#[cfg(test)]
mod tests;
