use std::cell::{Cell, RefCell};


use std::collections::BTreeMap;
use std::convert::TryFrom;


use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex as StdMutex};
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};


use parking_lot::Mutex;

use crate::primitives::pager::{
    AutockptContext, BackgroundMaintainer, PageStore, WriteGuard,
};
use crate::storage::btree::{BTree, ValCodec};
use crate::storage::index::{
    CatalogEpoch, DdlEpoch, GraphIndexCache, IndexDef, IndexRoots, IndexStore,
};
use crate::storage::vstore::VStore;

use crate::storage::PropValueOwned;
use crate::types::{EdgeId, LabelId, NodeId, PageId, PropId, Result, SombraError, TypeId};


use super::adjacency;
use super::edge;
use super::mvcc::{
    CommitId, CommitTable, ReaderTimeoutActions, VersionCodecConfig, VersionHeader, VersionedValue,
    VERSION_HEADER_LEN,
};
use super::mvcc_flags;
use super::node;
use super::options::{GraphOptions, VacuumCfg};


use super::props;

mod adjacency_ops;
mod deferred_ops;
mod edge_ops;
mod graph_types;
mod helpers;
mod index_ops;
mod mvcc_ops;
mod node_ops;
mod prop_ops;
mod snapshot;
mod tests;
mod vacuum;
mod version_cache;
mod writer;

pub use writer::{BulkEdgeValidator, CreateEdgeOptions, GraphWriter, GraphWriterStats};

#[allow(unused_imports)]
pub use graph_types::{
    AdjacencyVacuumStats, BfsOptions, BfsVisit, GraphMvccStatus, GraphVacuumStats, PropStats,
    SnapshotPoolStatus, VacuumBudget, VacuumMode, VacuumTrigger, VersionVacuumStats,
    DEFAULT_INLINE_PROP_BLOB, DEFAULT_INLINE_PROP_VALUE, MVCC_METRICS_PUBLISH_INTERVAL,
    STORAGE_FLAG_DEGREE_CACHE,
};

use graph_types::RootKind;
#[cfg(feature = "degree-cache")]
use helpers::open_degree_tree;
use helpers::{open_u64_vec_tree, open_unit_tree};

use snapshot::{SnapshotLease, SnapshotPool};
use vacuum::MicroGcTrigger;
use version_cache::VersionCache;

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct UnitValue;

impl ValCodec for UnitValue {
    fn encode_val(_: &Self, _: &mut Vec<u8>) {}

    fn decode_val(src: &[u8]) -> Result<Self> {
        if src.is_empty() {
            Ok(UnitValue)
        } else {
            Err(SombraError::Corruption("adjacency value payload not empty"))
        }
    }
}

/// Main graph storage structure managing nodes, edges, adjacency lists, and indexes.
#[allow(dead_code)]
pub struct Graph {
    store: Arc<dyn PageStore>,
    commit_table: Option<Arc<Mutex<CommitTable>>>,
    nodes: BTree<u64, Vec<u8>>,
    edges: BTree<u64, Vec<u8>>,
    adj_fwd: BTree<Vec<u8>, VersionedValue<UnitValue>>,
    adj_rev: BTree<Vec<u8>, VersionedValue<UnitValue>>,
    version_log: BTree<u64, Vec<u8>>,
    #[cfg(feature = "degree-cache")]
    degree: Option<BTree<Vec<u8>, u64>>,
    vstore: VStore,
    indexes: IndexStore,
    catalog_epoch: CatalogEpoch,
    inline_prop_blob: usize,
    inline_prop_value: usize,
    #[cfg(feature = "degree-cache")]
    degree_cache_enabled: bool,
    nodes_root: AtomicU64,
    edges_root: AtomicU64,
    adj_fwd_root: AtomicU64,
    adj_rev_root: AtomicU64,
    version_log_root: AtomicU64,
    #[cfg(feature = "degree-cache")]
    degree_root: AtomicU64,
    next_node_id: AtomicU64,
    next_edge_id: AtomicU64,
    next_version_ptr: AtomicU64,
    idx_cache_hits: AtomicU64,
    idx_cache_misses: AtomicU64,
    storage_flags: u32,
    metrics: Arc<dyn super::metrics::StorageMetrics>,
    mvcc_metrics_last: StdMutex<Option<Instant>>,
    distinct_neighbors_default: bool,
    row_hash_header: bool,
    vacuum_cfg: VacuumCfg,
    vacuum_sched: VacuumSched,
    inline_history: bool,
    inline_history_max_bytes: usize,
    defer_adjacency_flush: bool,
    defer_index_flush: bool,
    snapshot_pool: Option<SnapshotPool>,
    version_cache: Option<VersionCache>,
    version_codec_cfg: VersionCodecConfig,
    version_log_bytes: AtomicU64,
    version_log_entries: AtomicU64,
    version_cache_hits: AtomicU64,
    version_cache_misses: AtomicU64,
    version_codec_raw_bytes: AtomicU64,
    version_codec_encoded_bytes: AtomicU64,
    micro_gc_last_ms: AtomicU64,
    micro_gc_budget_hint: AtomicUsize,
    micro_gc_running: AtomicBool,
}

struct VacuumSched {
    running: Cell<bool>,
    next_deadline_ms: Cell<u128>,
    last_stats: RefCell<Option<GraphVacuumStats>>,
    owner_tid: ThreadId,
    pending_trigger: Cell<Option<VacuumTrigger>>,
    mode: Cell<VacuumMode>,
}

impl VacuumSched {
    fn new() -> Self {
        Self {
            running: Cell::new(false),
            next_deadline_ms: Cell::new(0),
            last_stats: RefCell::new(None),
            owner_tid: thread::current().id(),
            pending_trigger: Cell::new(None),
            mode: Cell::new(VacuumMode::Normal),
        }
    }
}

#[derive(Default)]
struct AdjacencyBuffer {
    inserts: Vec<(NodeId, NodeId, TypeId, EdgeId, CommitId)>,
    removals: Vec<(NodeId, NodeId, TypeId, EdgeId, CommitId)>,
}

#[derive(Default)]
struct IndexBuffer {
    label_inserts: Vec<(LabelId, NodeId, CommitId)>,
    label_removes: Vec<(LabelId, NodeId, CommitId)>,
    prop_inserts: Vec<(IndexDef, Vec<u8>, NodeId, CommitId)>,
    prop_removes: Vec<(IndexDef, Vec<u8>, NodeId, CommitId)>,
}

impl Graph {
    fn overwrite_encoded_header(bytes: &mut [u8], header: &VersionHeader) {
        let encoded = header.encode();
        bytes[..VERSION_HEADER_LEN].copy_from_slice(&encoded);
    }

    fn maybe_inline_history(&self, bytes: &[u8]) -> Option<Vec<u8>> {
        if !self.inline_history {
            return None;
        }
        if bytes.len() > self.inline_history_max_bytes {
            return None;
        }
        Some(bytes.to_vec())
    }

    fn lease_latest_snapshot(&self) -> Result<SnapshotLease<'_>> {
        self.drive_micro_gc(MicroGcTrigger::ReadPath);
        if let Some(pool) = &self.snapshot_pool {
            pool.lease()
        } else {
            let guard = self.begin_read_guard()?;
            Ok(SnapshotLease::direct(guard))
        }
    }

    /// Opens a graph storage instance with the specified configuration options.
    pub fn open(opts: GraphOptions) -> Result<Arc<Self>> {
        let store = Arc::clone(&opts.store);
        let meta = store.meta()?;

        let inline_blob_u32 = opts.inline_prop_blob.unwrap_or({
            if meta.storage_inline_prop_blob == 0 {
                DEFAULT_INLINE_PROP_BLOB
            } else {
                meta.storage_inline_prop_blob
            }
        });
        let inline_value_u32 = opts.inline_prop_value.unwrap_or({
            if meta.storage_inline_prop_value == 0 {
                DEFAULT_INLINE_PROP_VALUE
            } else {
                meta.storage_inline_prop_value
            }
        });
        let inline_prop_blob = inline_blob_u32 as usize;
        let inline_prop_value = inline_value_u32 as usize;

        let nodes = open_u64_vec_tree(&store, meta.storage_nodes_root)?;
        let edges = open_u64_vec_tree(&store, meta.storage_edges_root)?;
        let adj_fwd = open_unit_tree(&store, meta.storage_adj_fwd_root)?;
        let adj_rev = open_unit_tree(&store, meta.storage_adj_rev_root)?;
        let version_log = open_u64_vec_tree(&store, meta.storage_version_log_root)?;
        let index_roots = IndexRoots {
            catalog: meta.storage_index_catalog_root,
            label: meta.storage_label_index_root,
            prop_chunk: meta.storage_prop_chunk_root,
            prop_btree: meta.storage_prop_btree_root,
        };
        let (indexes, index_roots_actual) = IndexStore::open(Arc::clone(&store), index_roots)?;

        #[cfg(feature = "degree-cache")]
        let degree_cache_enabled = opts.degree_cache
            || (meta.storage_flags & STORAGE_FLAG_DEGREE_CACHE) != 0
            || meta.storage_degree_root.0 != 0;
        #[cfg(not(feature = "degree-cache"))]
        let _degree_cache_enabled = false;

        #[cfg(feature = "degree-cache")]
        let degree_tree = if degree_cache_enabled || meta.storage_degree_root.0 != 0 {
            let tree = open_degree_tree(&store, meta.storage_degree_root)?;
            Some(tree)
        } else {
            None
        };
        #[cfg(not(feature = "degree-cache"))]
        let _degree_tree: Option<BTree<Vec<u8>, u64>> = None;

        let nodes_root = nodes.root_page();
        let edges_root = edges.root_page();
        let adj_fwd_root = adj_fwd.root_page();
        let adj_rev_root = adj_rev.root_page();
        let version_log_root = version_log.root_page();
        let index_catalog_root = index_roots_actual.catalog;
        let index_label_root = index_roots_actual.label;
        let index_prop_chunk_root = index_roots_actual.prop_chunk;
        let index_prop_btree_root = index_roots_actual.prop_btree;
        #[cfg(feature = "degree-cache")]
        let degree_root = degree_tree
            .as_ref()
            .map(|tree| tree.root_page())
            .unwrap_or(PageId(0));
        #[cfg(not(feature = "degree-cache"))]
        let _degree_root = PageId(0);

        let mut storage_flags = meta.storage_flags;
        #[cfg(feature = "degree-cache")]
        {
            if degree_cache_enabled {
                storage_flags |= STORAGE_FLAG_DEGREE_CACHE;
            } else {
                storage_flags &= !STORAGE_FLAG_DEGREE_CACHE;
            }
        }
        #[cfg(not(feature = "degree-cache"))]
        {
            storage_flags &= !STORAGE_FLAG_DEGREE_CACHE;
        }

        let inline_blob_meta = u32::try_from(inline_prop_blob)
            .map_err(|_| SombraError::Invalid("inline_prop_blob exceeds u32::MAX"))?;
        let inline_value_meta = u32::try_from(inline_prop_value)
            .map_err(|_| SombraError::Invalid("inline_prop_value exceeds u32::MAX"))?;
        let next_node_id_init = meta.storage_next_node_id.max(1);
        let next_edge_id_init = meta.storage_next_edge_id.max(1);
        let next_version_ptr_init = meta.storage_next_version_ptr.max(1);

        let mut meta_update_needed = false;
        if storage_flags != meta.storage_flags {
            meta_update_needed = true;
        }
        if nodes_root != meta.storage_nodes_root
            || edges_root != meta.storage_edges_root
            || adj_fwd_root != meta.storage_adj_fwd_root
            || adj_rev_root != meta.storage_adj_rev_root
            || version_log_root != meta.storage_version_log_root
            || index_catalog_root != meta.storage_index_catalog_root
            || index_label_root != meta.storage_label_index_root
            || index_prop_chunk_root != meta.storage_prop_chunk_root
            || index_prop_btree_root != meta.storage_prop_btree_root
        {
            meta_update_needed = true;
        }
        if inline_blob_meta != meta.storage_inline_prop_blob
            || inline_value_meta != meta.storage_inline_prop_value
        {
            meta_update_needed = true;
        }
        if meta.storage_next_node_id != next_node_id_init
            || meta.storage_next_edge_id != next_edge_id_init
            || meta.storage_next_version_ptr != next_version_ptr_init
        {
            meta_update_needed = true;
        }
        #[cfg(feature = "degree-cache")]
        {
            if degree_root != meta.storage_degree_root {
                meta_update_needed = true;
            }
        }
        #[cfg(not(feature = "degree-cache"))]
        {
            if meta.storage_degree_root.0 != 0 {
                meta_update_needed = true;
            }
        }

        if meta_update_needed {
            let mut write = store.begin_write()?;
            write.update_meta(|meta| {
                meta.storage_flags = storage_flags;
                meta.storage_nodes_root = nodes_root;
                meta.storage_edges_root = edges_root;
                meta.storage_adj_fwd_root = adj_fwd_root;
                meta.storage_adj_rev_root = adj_rev_root;
                meta.storage_version_log_root = version_log_root;
                meta.storage_index_catalog_root = index_catalog_root;
                meta.storage_label_index_root = index_label_root;
                meta.storage_prop_chunk_root = index_prop_chunk_root;
                meta.storage_prop_btree_root = index_prop_btree_root;

                #[cfg(feature = "degree-cache")]
                {
                    meta.storage_degree_root = degree_root;
                }
                #[cfg(not(feature = "degree-cache"))]
                {
                    meta.storage_degree_root = PageId(0);
                }
                meta.storage_next_node_id = next_node_id_init;
                meta.storage_next_edge_id = next_edge_id_init;
                meta.storage_next_version_ptr = next_version_ptr_init;
                meta.storage_inline_prop_blob = inline_blob_meta;
                meta.storage_inline_prop_value = inline_value_meta;
            })?;
            store.commit(write)?;
        }

        let vstore = VStore::open(Arc::clone(&store))?;
        let catalog_epoch = CatalogEpoch::new(DdlEpoch(meta.storage_ddl_epoch));
        let next_node_id = AtomicU64::new(next_node_id_init);
        let next_edge_id = AtomicU64::new(next_edge_id_init);
        let idx_cache_hits = AtomicU64::new(0);
        let idx_cache_misses = AtomicU64::new(0);
        let row_hash_header = opts.row_hash_header;
        let nodes_root_id = nodes.root_page().0;
        let edges_root_id = edges.root_page().0;
        let adj_fwd_root_id = adj_fwd.root_page().0;
        let adj_rev_root_id = adj_rev.root_page().0;
        let version_log_root_id = version_log.root_page().0;
        #[cfg(feature = "degree-cache")]
        let degree_root_id = degree_tree
            .as_ref()
            .map(|tree| tree.root_page().0)
            .unwrap_or(0);

        let commit_table = store.commit_table();
        let metrics: Arc<dyn super::metrics::StorageMetrics> = opts
            .metrics
            .unwrap_or_else(|| super::metrics::default_metrics());
        let snapshot_pool = if opts.snapshot_pool_size > 0 {
            Some(SnapshotPool::new(
                Arc::clone(&store),
                Arc::clone(&metrics),
                opts.snapshot_pool_size,
                Duration::from_millis(opts.snapshot_pool_max_age_ms.max(1)),
            ))
        } else {
            None
        };
        let version_cache = if opts.version_cache_capacity > 0 {
            Some(VersionCache::new(
                opts.version_cache_shards,
                opts.version_cache_capacity,
            ))
        } else {
            None
        };

        let graph = Arc::new(Self {
            store,
            commit_table,
            nodes,
            edges,
            adj_fwd,
            adj_rev,
            version_log,
            #[cfg(feature = "degree-cache")]
            degree: degree_tree,
            vstore,
            indexes,
            catalog_epoch,
            inline_prop_blob,
            inline_prop_value,
            #[cfg(feature = "degree-cache")]
            degree_cache_enabled,
            nodes_root: AtomicU64::new(nodes_root_id),
            edges_root: AtomicU64::new(edges_root_id),
            adj_fwd_root: AtomicU64::new(adj_fwd_root_id),
            adj_rev_root: AtomicU64::new(adj_rev_root_id),
            version_log_root: AtomicU64::new(version_log_root_id),
            #[cfg(feature = "degree-cache")]
            degree_root: AtomicU64::new(degree_root_id),
            next_node_id,
            next_edge_id,
            next_version_ptr: AtomicU64::new(next_version_ptr_init),
            idx_cache_hits,
            idx_cache_misses,
            storage_flags,
            metrics: Arc::clone(&metrics),
            mvcc_metrics_last: StdMutex::new(None),
            distinct_neighbors_default: opts.distinct_neighbors_default,
            row_hash_header,
            vacuum_cfg: opts.vacuum.clone(),
            vacuum_sched: VacuumSched::new(),
            inline_history: opts.inline_history,
            inline_history_max_bytes: opts.inline_history_max_bytes,
            defer_adjacency_flush: opts.defer_adjacency_flush,
            defer_index_flush: opts.defer_index_flush,
            snapshot_pool,
            version_cache,
            version_codec_cfg: VersionCodecConfig {
                kind: opts.version_codec,
                min_payload_len: opts.version_codec_min_payload_len,
                min_savings_bytes: opts.version_codec_min_savings_bytes,
            },
            version_log_bytes: AtomicU64::new(0),
            version_log_entries: AtomicU64::new(0),
            version_cache_hits: AtomicU64::new(0),
            version_cache_misses: AtomicU64::new(0),
            version_codec_raw_bytes: AtomicU64::new(0),
            version_codec_encoded_bytes: AtomicU64::new(0),
            micro_gc_last_ms: AtomicU64::new(0),
            micro_gc_budget_hint: AtomicUsize::new(0),
            micro_gc_running: AtomicBool::new(false),
        });
        graph.recompute_version_log_bytes()?;
        graph.register_vacuum_hook();
        graph.vacuum_sched.next_deadline_ms.set(0);
        Ok(graph)
    }

    fn register_vacuum_hook(self: &Arc<Self>) {
        let maint_graph: Arc<Graph> = Arc::clone(self);
        let maint_trait: Arc<dyn BackgroundMaintainer> = maint_graph;
        self.store
            .register_background_maint(Arc::downgrade(&maint_trait));
    }
}

impl Drop for Graph {
    fn drop(&mut self) {}
}

impl BackgroundMaintainer for Graph {
    fn run_background_maint(&self, _ctx: &AutockptContext) {
        self.enforce_reader_timeouts();
        self.drive_micro_gc(MicroGcTrigger::PostCommit);
        self.maybe_background_vacuum(VacuumTrigger::Timer);
    }
}

impl Graph {
    /// Evicts readers that have exceeded the configured timeout and emits warnings when appropriate.
    fn enforce_reader_timeouts(&self) {
        let timeout = self.vacuum_cfg.reader_timeout;
        if timeout == Duration::MAX {
            return;
        }
        let warn_pct = self.vacuum_cfg.reader_timeout_warn_threshold_pct;
        let Some(table) = &self.commit_table else {
            return;
        };
        let actions: ReaderTimeoutActions = {
            let mut guard = table.lock();
            guard.collect_reader_timeouts(timeout, warn_pct, Instant::now())
        };
        for warning in actions.warnings {
            self.metrics.mvcc_reader_timeout_warning(
                warning.reader_id,
                warning.age_ms,
                warning.timeout_ms,
            );
        }
        for eviction in actions.evictions {
            if let Some(flag) = eviction.evicted_flag.upgrade() {
                flag.store(true, AtomicOrdering::Release);
            }
            self.metrics.mvcc_reader_evicted();
        }
    }
}

const TRUST_VALIDATOR_REQUIRED: &str = "trusted endpoints require validator";
const TRUST_BATCH_REQUIRED: &str = "trusted endpoints batch must be validated";

struct GraphTxnState {
    index_cache: GraphIndexCache,
    deferred_adj: Option<AdjacencyBuffer>,
    deferred_index: Option<IndexBuffer>,
}

impl GraphTxnState {
    fn new(epoch: DdlEpoch) -> Self {
        Self {
            index_cache: GraphIndexCache::new(epoch),
            deferred_adj: None,
            deferred_index: None,
        }
    }
}

pub(crate) struct PropDelta {
    old_map: BTreeMap<PropId, PropValueOwned>,
    new_map: BTreeMap<PropId, PropValueOwned>,
    encoded: props::PropEncodeResult,
}

/// Computes adjacency list bounds for a given node.
impl Graph {
    fn take_txn_state(&self, tx: &mut WriteGuard<'_>) -> GraphTxnState {
        tx.take_extension::<GraphTxnState>()
            .unwrap_or_else(|| GraphTxnState::new(self.catalog_epoch.current()))
    }

    fn store_txn_state(&self, tx: &mut WriteGuard<'_>, mut state: GraphTxnState) {
        let stats = state.index_cache.take_stats();
        self.idx_cache_hits
            .fetch_add(stats.hits, AtomicOrdering::Relaxed);
        self.idx_cache_misses
            .fetch_add(stats.misses, AtomicOrdering::Relaxed);
        tx.store_extension(state);
    }

    fn invalidate_txn_cache(&self, tx: &mut WriteGuard<'_>) {
        if let Some(mut state) = tx.take_extension::<GraphTxnState>() {
            let stats = state.index_cache.take_stats();
            self.idx_cache_hits
                .fetch_add(stats.hits, AtomicOrdering::Relaxed);
            self.idx_cache_misses
                .fetch_add(stats.misses, AtomicOrdering::Relaxed);
        }
    }

    fn persist_tree_root(&self, tx: &mut WriteGuard<'_>, kind: RootKind) -> Result<()> {
        match kind {
            RootKind::Nodes => self.persist_root_impl(
                tx,
                &self.nodes_root,
                self.nodes.root_page(),
                |meta, root| {
                    meta.storage_nodes_root = root;
                },
            ),
            RootKind::Edges => self.persist_root_impl(
                tx,
                &self.edges_root,
                self.edges.root_page(),
                |meta, root| {
                    meta.storage_edges_root = root;
                },
            ),
            RootKind::AdjFwd => self.persist_root_impl(
                tx,
                &self.adj_fwd_root,
                self.adj_fwd.root_page(),
                |meta, root| {
                    meta.storage_adj_fwd_root = root;
                },
            ),
            RootKind::AdjRev => self.persist_root_impl(
                tx,
                &self.adj_rev_root,
                self.adj_rev.root_page(),
                |meta, root| {
                    meta.storage_adj_rev_root = root;
                },
            ),
            RootKind::VersionLog => self.persist_root_impl(
                tx,
                &self.version_log_root,
                self.version_log.root_page(),
                |meta, root| {
                    meta.storage_version_log_root = root;
                },
            ),
            #[cfg(feature = "degree-cache")]
            RootKind::Degree => {
                if let Some(tree) = &self.degree {
                    self.persist_root_impl(tx, &self.degree_root, tree.root_page(), |meta, root| {
                        meta.storage_degree_root = root;
                    })
                } else {
                    Ok(())
                }
            }
        }
    }

    fn persist_root_impl<F>(
        &self,
        tx: &mut WriteGuard<'_>,
        cached: &AtomicU64,
        page_id: PageId,
        update: F,
    ) -> Result<()>
    where
        F: FnOnce(&mut crate::primitives::pager::Meta, PageId),
    {
        if cached.load(AtomicOrdering::SeqCst) == page_id.0 {
            return Ok(());
        }
        tx.update_meta(|meta| update(meta, page_id))?;
        cached.store(page_id.0, AtomicOrdering::SeqCst);
        Ok(())
    }

    #[cfg(feature = "degree-cache")]
    /// Inserts or removes a single degree cache entry while running tests.
    pub fn debug_set_degree_entry(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: adjacency::DegreeDir,
        ty: TypeId,
        value: u64,
    ) -> Result<()> {
        if let Some(tree) = &self.degree {
            let key = adjacency::encode_degree_key(node, dir, ty);
            if value == 0 {
                let _ = tree.delete(tx, &key)?;
            } else {
                tree.put(tx, &key, &value)?;
            }
        }
        Ok(())
    }
    /// Returns the current catalog epoch used for DDL invalidation.
    pub fn catalog_epoch(&self) -> u64 {
        self.catalog_epoch.current().0
    }
}
