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
    profile_scope, record_btree_leaf_key_cmps, record_btree_leaf_key_decodes,
    record_btree_leaf_memcopy_bytes, record_btree_leaf_rebalance_in_place,
    record_btree_leaf_rebalance_rebuilds, StorageProfileKind,
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
