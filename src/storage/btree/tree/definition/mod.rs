use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use super::super::cursor::Cursor;
use super::super::page;
use super::super::stats::{BTreeStats, BTreeStatsSnapshot};
use crate::primitives::pager::{PageMut, PageRef, PageStore, ReadGuard, WriteGuard};
use crate::storage::profile::{
    profile_scope, profile_timer, record_btree_leaf_allocator_cache,
    record_btree_leaf_binary_search, record_btree_leaf_in_place_success,
    record_btree_leaf_key_cmps, record_btree_leaf_key_decodes, record_btree_leaf_memcopy_bytes,
    record_btree_leaf_rebalance_in_place, record_btree_leaf_rebalance_rebuilds,
    record_btree_leaf_record_encode, record_btree_leaf_slot_alloc, record_btree_leaf_split,
    record_leaf_allocator_build, record_leaf_allocator_compaction, record_leaf_allocator_failure,
    record_leaf_allocator_snapshot_reuse, LeafAllocatorFailureKind, StorageProfileKind,
};
use crate::types::{
    page::PAGE_HDR_LEN,
    page::{PageHeader, PageKind},
    PageId, Result, SombraError,
};
use smallvec::SmallVec;

include!("types.rs");
include!("api.rs");
include!("leaf.rs");
include!("leaf_allocator.rs");
include!("leaf_allocator_cache.rs");
include!("internal.rs");
include!("maintenance.rs");

include!("free.rs");
