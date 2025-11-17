use std::cmp::{Ordering as CmpOrdering, Ordering};
#[cfg(feature = "degree-cache")]
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::convert::TryFrom;
use std::num::NonZeroUsize;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::btree::{BTree, BTreeOptions, PutItem, ValCodec};
use crate::storage::index::{
    collect_all, CatalogEpoch, DdlEpoch, GraphIndexCache, GraphIndexCacheStats, IndexDef,
    IndexRoots, IndexStore, LabelScan, PostingStream, TypeTag,
};
use crate::storage::vstore::VStore;
use crate::types::{EdgeId, LabelId, NodeId, PageId, PropId, Result, SombraError, TypeId, VRef};

#[cfg(feature = "degree-cache")]
use super::adjacency::DegreeDir;
use super::adjacency::{self, Dir, ExpandOpts, Neighbor, NeighborCursor};
use super::edge::{
    self, EncodeOpts as EdgeEncodeOpts, PropPayload as EdgePropPayload,
    PropStorage as EdgePropStorage,
};
use super::mvcc::{
    CommitId, CommitTable, VersionHeader, VersionLog, VersionLogEntry, VersionPtr, VersionSpace,
    VersionedValue, COMMIT_MAX, VERSION_HEADER_LEN,
};
use super::mvcc_flags;
use super::node::{
    self, EncodeOpts as NodeEncodeOpts, PropPayload as NodePropPayload,
    PropStorage as NodePropStorage, VersionedNodeRow,
};
use super::options::GraphOptions;
use super::patch::{PropPatch, PropPatchOp};
use super::profile::{
    profile_timer as storage_profile_timer, profiling_enabled as storage_profiling_enabled,
    record_profile_timer as record_storage_profile_timer, StorageProfileKind,
};
use super::props;
use super::props::RawPropValue;
use super::types::{
    DeleteMode, DeleteNodeOpts, EdgeData, EdgeSpec, NodeData, NodeSpec, PropEntry, PropValue,
    PropValueOwned,
};

/// Default maximum size for inline property blob storage in bytes.
pub const DEFAULT_INLINE_PROP_BLOB: u32 = 128;
/// Default maximum size for inline property value storage in bytes.
pub const DEFAULT_INLINE_PROP_VALUE: u32 = 48;
/// Storage flag indicating that degree caching is enabled.
pub const STORAGE_FLAG_DEGREE_CACHE: u32 = 0x01;

#[derive(Clone, Copy, Debug, Default)]
struct UnitValue;

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
    version_log: Mutex<VersionLog>,
    nodes: BTree<u64, Vec<u8>>,
    edges: BTree<u64, Vec<u8>>,
    adj_fwd: BTree<Vec<u8>, VersionedValue<UnitValue>>,
    adj_rev: BTree<Vec<u8>, VersionedValue<UnitValue>>,
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
    #[cfg(feature = "degree-cache")]
    degree_root: AtomicU64,
    next_node_id: AtomicU64,
    next_edge_id: AtomicU64,
    idx_cache_hits: AtomicU64,
    idx_cache_misses: AtomicU64,
    storage_flags: u32,
    metrics: Arc<dyn super::metrics::StorageMetrics>,
    distinct_neighbors_default: bool,
    row_hash_header: bool,
}

/// Options for breadth-first traversal over the graph.
#[derive(Clone, Debug)]
pub struct BfsOptions {
    /// Maximum depth (inclusive) to explore starting from the origin node.
    pub max_depth: u32,
    /// Direction to follow for edge expansions.
    pub direction: Dir,
    /// Optional subset of edge types to consider (matches all when `None`).
    pub edge_types: Option<Vec<TypeId>>,
    /// Optional cap on the number of visited nodes returned (including the origin).
    pub max_results: Option<usize>,
}

impl Default for BfsOptions {
    fn default() -> Self {
        Self {
            max_depth: 1,
            direction: Dir::Out,
            edge_types: None,
            max_results: None,
        }
    }
}

/// Node visit captured during a breadth-first traversal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BfsVisit {
    /// Identifier of the visited node.
    pub node: NodeId,
    /// Depth (distance in hops) from the origin node.
    pub depth: u32,
}

#[derive(Copy, Clone)]
enum RootKind {
    Nodes,
    Edges,
    AdjFwd,
    AdjRev,
    #[cfg(feature = "degree-cache")]
    Degree,
}

/// Basic statistics for a (label, property) pair.
#[derive(Clone, Debug, Default)]
pub struct PropStats {
    /// Total number of rows (nodes with the label).
    pub row_count: u64,
    /// Number of rows with a non-null value for the property.
    pub non_null_count: u64,
    /// Number of rows where the property is missing or null.
    pub null_count: u64,
    /// Number of distinct non-null property values.
    pub distinct_count: u64,
    /// Minimum observed non-null property value.
    pub min: Option<PropValueOwned>,
    /// Maximum observed non-null property value.
    pub max: Option<PropValueOwned>,
}

impl Graph {
    fn overwrite_encoded_header(bytes: &mut [u8], header: &VersionHeader) {
        let encoded = header.encode();
        bytes[..VERSION_HEADER_LEN].copy_from_slice(&encoded);
    }

    fn log_version_entry(
        &self,
        space: VersionSpace,
        id: u64,
        header: VersionHeader,
        prev_ptr: VersionPtr,
        bytes: Vec<u8>,
    ) -> VersionPtr {
        let mut log = self.version_log.lock();
        log.append(VersionLogEntry {
            space,
            id,
            header,
            prev_ptr,
            bytes,
        })
    }

    /// Returns the commit table when the underlying pager provides one.
    pub fn commit_table(&self) -> Option<Arc<Mutex<CommitTable>>> {
        self.commit_table.as_ref().map(Arc::clone)
    }

    /// Opens a graph storage instance with the specified configuration options.
    pub fn open(opts: GraphOptions) -> Result<Self> {
        let store = Arc::clone(&opts.store);
        let meta = store.meta()?;

        let inline_blob_u32 = opts.inline_prop_blob.unwrap_or_else(|| {
            if meta.storage_inline_prop_blob == 0 {
                DEFAULT_INLINE_PROP_BLOB
            } else {
                meta.storage_inline_prop_blob
            }
        });
        let inline_value_u32 = opts.inline_prop_value.unwrap_or_else(|| {
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

        let mut meta_update_needed = false;
        if storage_flags != meta.storage_flags {
            meta_update_needed = true;
        }
        if nodes_root != meta.storage_nodes_root
            || edges_root != meta.storage_edges_root
            || adj_fwd_root != meta.storage_adj_fwd_root
            || adj_rev_root != meta.storage_adj_rev_root
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
        #[cfg(feature = "degree-cache")]
        let degree_root_id = degree_tree
            .as_ref()
            .map(|tree| tree.root_page().0)
            .unwrap_or(0);

        let commit_table = store.commit_table();

        Ok(Self {
            store,
            commit_table,
            version_log: Mutex::new(VersionLog::new()),
            nodes,
            edges,
            adj_fwd,
            adj_rev,
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
            #[cfg(feature = "degree-cache")]
            degree_root: AtomicU64::new(degree_root_id),
            next_node_id,
            next_edge_id,
            idx_cache_hits,
            idx_cache_misses,
            storage_flags,
            metrics: opts
                .metrics
                .unwrap_or_else(|| super::metrics::default_metrics()),
            distinct_neighbors_default: opts.distinct_neighbors_default,
            row_hash_header,
        })
    }

    /// Creates a new node in the graph with the given specification.
    pub fn create_node(&self, tx: &mut WriteGuard<'_>, spec: NodeSpec<'_>) -> Result<NodeId> {
        let labels = normalize_labels(spec.labels)?;
        let mut prop_owned: BTreeMap<PropId, PropValueOwned> = BTreeMap::new();
        for entry in spec.props {
            let owned = prop_value_to_owned(entry.value.clone());
            prop_owned.insert(entry.prop, owned);
        }
        let (prop_bytes, spill_vrefs) = self.encode_property_map(tx, spec.props)?;
        let mut map_vref: Option<VRef> = None;
        let payload = if prop_bytes.len() <= self.inline_prop_blob {
            NodePropPayload::Inline(&prop_bytes)
        } else {
            let vref = self.vstore.write(tx, &prop_bytes)?;
            map_vref = Some(vref);
            NodePropPayload::VRef(vref)
        };
        let root = self.nodes.root_page();
        debug_assert!(root.0 != 0, "nodes root page not initialized");
        let (_commit_id, version) = self.tx_version_header(tx);
        let row_bytes = match node::encode(
            &labels,
            payload,
            NodeEncodeOpts::new(self.row_hash_header),
            version,
            VersionPtr::null(),
        ) {
            Ok(encoded) => encoded.bytes,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &spill_vrefs);
                return Err(err);
            }
        };
        let id_raw = self.next_node_id.fetch_add(1, AtomicOrdering::SeqCst);
        let node_id = NodeId(id_raw);
        let next_id = node_id.0.saturating_add(1);
        tx.update_meta(|meta| {
            if meta.storage_next_node_id <= node_id.0 {
                meta.storage_next_node_id = next_id;
            }
        })?;
        if let Err(err) = self.nodes.put(tx, &node_id.0, &row_bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Nodes)?;
        if let Err(err) = self.indexes.insert_node_labels(tx, node_id, &labels) {
            let _ = self.nodes.delete(tx, &node_id.0);
            if let Err(root_err) = self.persist_tree_root(tx, RootKind::Nodes) {
                return Err(root_err);
            }
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        if let Err(err) = self.insert_indexed_props(tx, node_id, &labels, &prop_owned) {
            let _ = self.indexes.remove_node_labels(tx, node_id, &labels);
            let _ = self.nodes.delete(tx, &node_id.0);
            if let Err(root_err) = self.persist_tree_root(tx, RootKind::Nodes) {
                return Err(root_err);
            }
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.metrics.node_created();
        Ok(node_id)
    }

    /// Creates a label index for fast node lookups by label.
    pub fn create_label_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        if self.indexes.has_label_index_with_write(tx, label)? {
            return Ok(());
        }
        let mut nodes = Vec::new();
        self.nodes.for_each_with_write(tx, |id_raw, bytes| {
            let VersionedNodeRow { row, .. } = node::decode(&bytes)?;
            if row.labels.binary_search(&label).is_ok() {
                nodes.push(NodeId(id_raw));
            }
            Ok(())
        })?;
        self.indexes.create_label_index(tx, label, nodes)?;
        self.sync_index_roots(tx)?;
        self.bump_ddl_epoch(tx)
    }

    /// Drops an existing label index.
    pub fn drop_label_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        if !self.indexes.has_label_index_with_write(tx, label)? {
            return Ok(());
        }
        self.indexes.drop_label_index(tx, label)?;
        self.sync_index_roots(tx)?;
        self.bump_ddl_epoch(tx)
    }

    /// Checks if a label index exists for the given label.
    pub fn has_label_index(&self, label: LabelId) -> Result<bool> {
        self.indexes.has_label_index(label)
    }

    /// Creates a property index for fast property-based lookups.
    pub fn create_property_index(&self, tx: &mut WriteGuard<'_>, def: IndexDef) -> Result<()> {
        let existing = self
            .indexes
            .property_indexes_for_label_with_write(tx, def.label)?;
        if existing.iter().any(|entry| entry.prop == def.prop) {
            return Ok(());
        }
        let mut entries: Vec<(Vec<u8>, NodeId)> = Vec::new();
        self.nodes.for_each_with_write(tx, |id_raw, bytes| {
            let VersionedNodeRow { row, .. } = node::decode(&bytes)?;
            if row.labels.binary_search(&def.label).is_err() {
                return Ok(());
            }
            let prop_bytes = self.read_node_prop_bytes(&row.props)?;
            let props = self.materialize_props_owned(&prop_bytes)?;
            let map: BTreeMap<PropId, PropValueOwned> = props.into_iter().collect();
            if let Some(value) = map.get(&def.prop) {
                let key = encode_value_key_owned(def.ty, value)?;
                entries.push((key, NodeId(id_raw)));
            }
            Ok(())
        })?;
        entries.sort_by(|(a, node_a), (b, node_b)| match a.cmp(b) {
            CmpOrdering::Equal => node_a.0.cmp(&node_b.0),
            other => other,
        });
        self.indexes.create_property_index(tx, def, &entries)?;
        self.sync_index_roots(tx)?;
        self.bump_ddl_epoch(tx)
    }

    /// Drops a property index for the given label and property.
    pub fn drop_property_index(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
        prop: PropId,
    ) -> Result<()> {
        let defs = self
            .indexes
            .property_indexes_for_label_with_write(tx, label)?;
        let Some(def) = defs.into_iter().find(|d| d.prop == prop) else {
            return Ok(());
        };
        self.indexes.drop_property_index(tx, def)?;
        self.sync_index_roots(tx)?;
        self.bump_ddl_epoch(tx)
    }

    /// Checks if a property index exists for the given label and property.
    pub fn has_property_index(&self, label: LabelId, prop: PropId) -> Result<bool> {
        let read = self.store.begin_latest_committed_read()?;
        Ok(self
            .indexes
            .get_property_index(&read, label, prop)?
            .is_some())
    }

    /// Returns the root page ID of the index catalog.
    pub fn index_catalog_root(&self) -> PageId {
        self.indexes.catalog().tree().root_page()
    }

    /// Retrieves the property index definition for a given label and property.
    pub fn property_index(&self, label: LabelId, prop: PropId) -> Result<Option<IndexDef>> {
        let read = self.store.begin_latest_committed_read()?;
        self.indexes.get_property_index(&read, label, prop)
    }

    #[cfg(test)]
    /// Collects all entries from a property index for use in assertions.
    pub fn debug_collect_property_index(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
    ) -> Result<Vec<(Vec<u8>, NodeId)>> {
        use std::ops::Bound;
        let def = self
            .indexes
            .get_property_index(tx, label, prop)?
            .ok_or(SombraError::Invalid("property index not found"))?;
        self.indexes
            .scan_property_range(tx, &def, Bound::Unbounded, Bound::Unbounded)
    }

    /// Returns all property index definitions currently registered.
    pub fn all_property_indexes(&self) -> Result<Vec<IndexDef>> {
        let read = self.store.begin_latest_committed_read()?;
        self.indexes.all_property_indexes(&read)
    }

    /// Scans for nodes matching an exact property value using an index.
    pub fn property_scan_eq(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
        value: &PropValueOwned,
    ) -> Result<Vec<NodeId>> {
        let mut stream = self.property_scan_eq_stream(tx, label, prop, value)?;
        collect_posting_stream(&mut *stream)
    }

    /// Returns a stream of node IDs with the specified label and property value.
    pub fn property_scan_eq_stream<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
        prop: PropId,
        value: &PropValueOwned,
    ) -> Result<Box<dyn PostingStream + 'a>> {
        let lookup_timer = storage_profile_timer();
        let def = self
            .indexes
            .get_property_index(tx, label, prop)?
            .ok_or(SombraError::Invalid("property index not found"))?;
        record_storage_profile_timer(StorageProfileKind::PropIndexLookup, lookup_timer);

        let encode_timer = storage_profile_timer();
        let key = encode_value_key_owned(def.ty, value)?;
        record_storage_profile_timer(StorageProfileKind::PropIndexKeyEncode, encode_timer);

        let stream_timer = storage_profile_timer();
        let stream = self.indexes.scan_property_eq_stream(tx, &def, &key)?;
        record_storage_profile_timer(StorageProfileKind::PropIndexStreamBuild, stream_timer);
        Ok(instrument_posting_stream(stream))
    }

    /// Scans for nodes with property values in a range (inclusive bounds).
    pub fn property_scan_range(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
        start: &PropValueOwned,
        end: &PropValueOwned,
    ) -> Result<Vec<NodeId>> {
        self.property_scan_range_bounds(
            tx,
            label,
            prop,
            Bound::Included(start),
            Bound::Included(end),
        )
    }

    /// Returns aggregate cache statistics for label→index lookups.
    pub fn index_cache_stats(&self) -> GraphIndexCacheStats {
        GraphIndexCacheStats {
            hits: self.idx_cache_hits.load(AtomicOrdering::Relaxed),
            misses: self.idx_cache_misses.load(AtomicOrdering::Relaxed),
        }
    }

    /// Scans for nodes with property values in a range with custom bounds.
    pub fn property_scan_range_bounds(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
        start: Bound<&PropValueOwned>,
        end: Bound<&PropValueOwned>,
    ) -> Result<Vec<NodeId>> {
        let mut stream = self.property_scan_range_stream(tx, label, prop, start, end)?;
        collect_posting_stream(&mut *stream)
    }

    /// Returns a stream of node IDs with the specified label and property values in the given range.
    pub fn property_scan_range_stream<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
        prop: PropId,
        start: Bound<&PropValueOwned>,
        end: Bound<&PropValueOwned>,
    ) -> Result<Box<dyn PostingStream + 'a>> {
        let lookup_timer = storage_profile_timer();
        let def = self
            .indexes
            .get_property_index(tx, label, prop)?
            .ok_or(SombraError::Invalid("property index not found"))?;
        record_storage_profile_timer(StorageProfileKind::PropIndexLookup, lookup_timer);

        let encode_timer = storage_profile_timer();
        let start_key = encode_range_bound(def.ty, start)?;
        let end_key = encode_range_bound(def.ty, end)?;
        record_storage_profile_timer(StorageProfileKind::PropIndexKeyEncode, encode_timer);

        let stream_timer = storage_profile_timer();
        let stream = self
            .indexes
            .scan_property_range_stream(tx, &def, start_key, end_key)?;
        record_storage_profile_timer(StorageProfileKind::PropIndexStreamBuild, stream_timer);
        Ok(instrument_posting_stream(stream))
    }

    /// Returns a label scan iterator; this prefers real indexes.
    pub fn label_scan<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
    ) -> Result<Option<LabelScan<'a>>> {
        self.indexes.label_scan(tx, label)
    }

    /// Returns a label scan iterator and falls back to a full scan when no index exists.
    pub fn label_scan_stream<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
    ) -> Result<Box<dyn PostingStream + 'a>> {
        if let Some(scan) = self.indexes.label_scan(tx, label)? {
            return Ok(Box::new(scan));
        }
        let fallback = self.build_fallback_label_scan(tx, label)?;
        Ok(Box::new(fallback))
    }

    /// Retrieves node data by ID.
    pub fn get_node(&self, tx: &ReadGuard, id: NodeId) -> Result<Option<NodeData>> {
        let Some(bytes) = self.nodes.get(tx, &id.0)? else {
            return Ok(None);
        };
        let versioned = node::decode(&bytes)?;
        let snapshot = Self::reader_snapshot_commit(tx);
        if !Self::version_visible(&versioned.header, snapshot) {
            return Ok(None);
        }
        let row = versioned.row;
        let prop_bytes = match row.props {
            NodePropStorage::Inline(bytes) => bytes,
            NodePropStorage::VRef(vref) => self.vstore.read(tx, vref)?,
        };
        let raw = props::decode_raw(&prop_bytes)?;
        let props = props::materialize_props(&raw, &self.vstore, tx)?;
        Ok(Some(NodeData {
            labels: row.labels,
            props,
        }))
    }

    /// Scans and returns all nodes in the graph.
    pub fn scan_all_nodes(&self, tx: &ReadGuard) -> Result<Vec<(NodeId, NodeData)>> {
        let mut cursor = self.nodes.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut rows = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, bytes)) = cursor.next()? {
            let versioned = node::decode(&bytes)?;
            if !Self::version_visible(&versioned.header, snapshot) {
                continue;
            }
            let row = versioned.row;
            let prop_bytes = match row.props {
                NodePropStorage::Inline(bytes) => bytes,
                NodePropStorage::VRef(vref) => self.vstore.read(tx, vref)?,
            };
            let props = self.materialize_props_owned(&prop_bytes)?;
            rows.push((
                NodeId(key),
                NodeData {
                    labels: row.labels,
                    props,
                },
            ));
        }
        Ok(rows)
    }

    fn build_fallback_label_scan(
        &self,
        tx: &ReadGuard,
        label: LabelId,
    ) -> Result<FallbackLabelScan> {
        let mut cursor = self.nodes.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut nodes = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, bytes)) = cursor.next()? {
            let versioned = node::decode(&bytes)?;
            if !Self::version_visible(&versioned.header, snapshot) {
                continue;
            }
            let row = versioned.row;
            if row.labels.binary_search(&label).is_ok() {
                nodes.push(NodeId(key));
            }
        }
        Ok(FallbackLabelScan { nodes, pos: 0 })
    }

    /// Counts how many nodes exist with the specified label.
    pub fn count_nodes_with_label(&self, tx: &ReadGuard, label: LabelId) -> Result<u64> {
        let mut stream = self.label_scan_stream(tx, label)?;
        const BATCH: usize = 256;
        let mut buf = Vec::with_capacity(BATCH);
        let mut count = 0u64;
        loop {
            buf.clear();
            let has_more = stream.next_batch(&mut buf, BATCH)?;
            count += buf.len() as u64;
            if !has_more {
                break;
            }
        }
        Ok(count)
    }

    /// Returns all node identifiers that carry the provided label.
    pub fn nodes_with_label(&self, tx: &ReadGuard, label: LabelId) -> Result<Vec<NodeId>> {
        let mut stream = self.label_scan_stream(tx, label)?;
        let mut nodes = Vec::new();
        collect_all(&mut *stream, &mut nodes)?;
        Ok(nodes)
    }

    /// Counts the number of edges that have the provided type.
    pub fn count_edges_with_type(&self, tx: &ReadGuard, ty: TypeId) -> Result<u64> {
        let mut cursor = self.edges.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut count = 0u64;
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((_key, bytes)) = cursor.next()? {
            let versioned = edge::decode(&bytes)?;
            if !Self::version_visible(&versioned.header, snapshot) {
                continue;
            }
            let row = versioned.row;
            if row.ty == ty {
                count += 1;
            }
        }
        Ok(count)
    }

    /// Samples up to `limit` nodes from the B-Tree and returns their label lists.
    pub fn sample_node_labels(&self, tx: &ReadGuard, limit: usize) -> Result<Vec<Vec<LabelId>>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut cursor = self.nodes.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut labels = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((_key, bytes)) = cursor.next()? {
            let versioned = node::decode(&bytes)?;
            if !Self::version_visible(&versioned.header, snapshot) {
                continue;
            }
            labels.push(versioned.row.labels);
            if labels.len() >= limit {
                break;
            }
        }
        Ok(labels)
    }

    /// Deletes a node from the graph with the given options.
    pub fn delete_node(
        &self,
        tx: &mut WriteGuard<'_>,
        id: NodeId,
        opts: DeleteNodeOpts,
    ) -> Result<()> {
        let Some(bytes) = self.nodes.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let versioned = node::decode(&bytes)?;
        if versioned.header.is_tombstone() {
            return Err(SombraError::NotFound);
        }
        let row = versioned.row;
        let read = self.store.begin_latest_committed_read()?;
        let incident = self.collect_incident_edges(&read, id)?;
        drop(read);

        match opts.mode {
            DeleteMode::Restrict => {
                if !incident.is_empty() {
                    return Err(SombraError::Invalid("node has incident edges"));
                }
            }
            DeleteMode::Cascade => {
                let mut edges: Vec<EdgeId> = incident.into_iter().collect();
                edges.sort_by_key(|edge| edge.0);
                for edge_id in edges {
                    self.delete_edge(tx, edge_id)?;
                }
            }
        }

        let prop_bytes = self.read_node_prop_bytes_with_write(tx, &row.props)?;
        let prop_values = self.materialize_props_owned_with_write(tx, &prop_bytes)?;
        let prop_map: BTreeMap<PropId, PropValueOwned> = prop_values.into_iter().collect();
        self.remove_indexed_props(tx, id, &row.labels, &prop_map)?;
        self.indexes.remove_node_labels(tx, id, &row.labels)?;
        self.free_node_props(tx, row.props)?;
        let (commit_id, mut tombstone_header) = self.tx_version_header(tx);
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            VersionSpace::Node,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes,
        );
        tombstone_header.flags |= mvcc_flags::TOMBSTONE;
        let encoded = node::encode(
            &[],
            node::PropPayload::Inline(&[]),
            NodeEncodeOpts::new(false),
            tombstone_header,
            prev_ptr,
        )?;
        self.nodes.put(tx, &id.0, &encoded.bytes)?;
        self.persist_tree_root(tx, RootKind::Nodes)?;
        self.metrics.node_deleted();
        Ok(())
    }

    /// Updates the properties of an existing node by applying the given patch.
    pub fn update_node(
        &self,
        tx: &mut WriteGuard<'_>,
        id: NodeId,
        patch: PropPatch<'_>,
    ) -> Result<()> {
        if patch.is_empty() {
            return Ok(());
        }
        let Some(existing_bytes) = self.nodes.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let versioned = node::decode(&existing_bytes)?;
        if versioned.header.is_tombstone() {
            return Err(SombraError::NotFound);
        }
        let node::NodeRow {
            labels,
            props: storage,
            row_hash,
        } = versioned.row;
        let prop_bytes = self.read_node_prop_bytes_with_write(tx, &storage)?;
        let Some(delta) = self.build_prop_delta(tx, &prop_bytes, &patch)? else {
            return Ok(());
        };
        let (commit_id, new_header) = self.tx_version_header(tx);
        let mut map_vref: Option<VRef> = None;
        let payload = if delta.encoded.bytes.len() <= self.inline_prop_blob {
            NodePropPayload::Inline(&delta.encoded.bytes)
        } else {
            let vref = self.vstore.write(tx, &delta.encoded.bytes)?;
            map_vref = Some(vref);
            NodePropPayload::VRef(vref)
        };
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = existing_bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            VersionSpace::Node,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes,
        );
        let encoded_row = match node::encode(
            &labels,
            payload,
            NodeEncodeOpts::new(self.row_hash_header),
            new_header,
            prev_ptr,
        ) {
            Ok(encoded) => encoded,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
                return Err(err);
            }
        };
        if self.row_hash_header {
            if let (Some(old_hash), Some(new_hash)) = (row_hash, encoded_row.row_hash) {
                if old_hash == new_hash {
                    if let Some(vref) = map_vref.take() {
                        let _ = self.vstore.free(tx, vref);
                    }
                    props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
                    return Ok(());
                }
            }
        }
        if let Err(err) = self.nodes.put(tx, &id.0, &encoded_row.bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Nodes)?;
        self.update_indexed_props_for_node(tx, id, &labels, &delta.old_map, &delta.new_map)?;
        self.free_node_props(tx, storage)
    }

    /// Creates a new edge in the graph with the given specification.
    pub fn create_edge(&self, tx: &mut WriteGuard<'_>, spec: EdgeSpec<'_>) -> Result<EdgeId> {
        self.ensure_node_exists(tx, spec.src, "edge source node missing")?;
        self.ensure_node_exists(tx, spec.dst, "edge destination node missing")?;
        self.insert_edge_unchecked(tx, spec)
    }

    fn insert_edge_unchecked(&self, tx: &mut WriteGuard<'_>, spec: EdgeSpec<'_>) -> Result<EdgeId> {
        let (prop_bytes, spill_vrefs) = self.encode_property_map(tx, spec.props)?;
        let mut map_vref: Option<VRef> = None;
        let payload = if prop_bytes.len() <= self.inline_prop_blob {
            EdgePropPayload::Inline(&prop_bytes)
        } else {
            let vref = self.vstore.write(tx, &prop_bytes)?;
            map_vref = Some(vref);
            EdgePropPayload::VRef(vref)
        };
        let (_commit_id, version) = self.tx_version_header(tx);
        let row_bytes = match edge::encode(
            spec.src,
            spec.dst,
            spec.ty,
            payload,
            EdgeEncodeOpts::new(self.row_hash_header),
            version,
            VersionPtr::null(),
        ) {
            Ok(encoded) => encoded.bytes,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &spill_vrefs);
                return Err(err);
            }
        };
        let id_raw = self.next_edge_id.fetch_add(1, AtomicOrdering::SeqCst);
        let edge_id = EdgeId(id_raw);
        let next_id = edge_id.0.saturating_add(1);
        tx.update_meta(|meta| {
            if meta.storage_next_edge_id <= edge_id.0 {
                meta.storage_next_edge_id = next_id;
            }
        })?;
        if let Err(err) = self.edges.put(tx, &edge_id.0, &row_bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Edges)?;
        if let Err(err) = self.insert_adjacencies(tx, &[(spec.src, spec.dst, spec.ty, edge_id)]) {
            let _ = self.edges.delete(tx, &edge_id.0);
            if let Err(root_err) = self.persist_tree_root(tx, RootKind::Edges) {
                return Err(root_err);
            }
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.metrics.edge_created();
        Ok(edge_id)
    }

    /// Retrieves edge data by ID.
    pub fn get_edge(&self, tx: &ReadGuard, id: EdgeId) -> Result<Option<EdgeData>> {
        let Some(bytes) = self.edges.get(tx, &id.0)? else {
            return Ok(None);
        };
        let versioned = edge::decode(&bytes)?;
        let snapshot = Self::reader_snapshot_commit(tx);
        if !Self::version_visible(&versioned.header, snapshot) {
            return Ok(None);
        }
        let row = versioned.row;
        let prop_bytes = match row.props {
            EdgePropStorage::Inline(bytes) => bytes,
            EdgePropStorage::VRef(vref) => self.vstore.read(tx, vref)?,
        };
        let raw = props::decode_raw(&prop_bytes)?;
        let props = props::materialize_props(&raw, &self.vstore, tx)?;
        Ok(Some(EdgeData {
            src: row.src,
            dst: row.dst,
            ty: row.ty,
            props,
        }))
    }

    /// Scans and returns all edges in the graph.
    pub fn scan_all_edges(&self, tx: &ReadGuard) -> Result<Vec<(EdgeId, EdgeData)>> {
        let mut cursor = self.edges.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut rows = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, bytes)) = cursor.next()? {
            let versioned = edge::decode(&bytes)?;
            if !Self::version_visible(&versioned.header, snapshot) {
                continue;
            }
            let row = versioned.row;
            let prop_bytes = match row.props {
                EdgePropStorage::Inline(bytes) => bytes,
                EdgePropStorage::VRef(vref) => self.vstore.read(tx, vref)?,
            };
            let props = self.materialize_props_owned(&prop_bytes)?;
            rows.push((
                EdgeId(key),
                EdgeData {
                    src: row.src,
                    dst: row.dst,
                    ty: row.ty,
                    props,
                },
            ));
        }
        Ok(rows)
    }

    /// Updates edge properties with the given patch.
    pub fn update_edge(
        &self,
        tx: &mut WriteGuard<'_>,
        id: EdgeId,
        patch: PropPatch<'_>,
    ) -> Result<()> {
        if patch.is_empty() {
            return Ok(());
        }
        let Some(existing_bytes) = self.edges.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let versioned = edge::decode(&existing_bytes)?;
        if versioned.header.is_tombstone() {
            return Err(SombraError::NotFound);
        }
        let edge::EdgeRow {
            src,
            dst,
            ty,
            props: storage,
            row_hash: old_row_hash,
        } = versioned.row;
        let prop_bytes = self.read_edge_prop_bytes_with_write(tx, &storage)?;
        let Some(delta) = self.build_prop_delta(tx, &prop_bytes, &patch)? else {
            return Ok(());
        };
        let (commit_id, new_header) = self.tx_version_header(tx);
        let mut map_vref: Option<VRef> = None;
        let payload = if delta.encoded.bytes.len() <= self.inline_prop_blob {
            EdgePropPayload::Inline(&delta.encoded.bytes)
        } else {
            let vref = self.vstore.write(tx, &delta.encoded.bytes)?;
            map_vref = Some(vref);
            EdgePropPayload::VRef(vref)
        };
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = existing_bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            VersionSpace::Edge,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes,
        );
        let encoded_row = match edge::encode(
            src,
            dst,
            ty,
            payload,
            EdgeEncodeOpts::new(self.row_hash_header),
            new_header,
            prev_ptr,
        ) {
            Ok(encoded) => encoded,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
                return Err(err);
            }
        };

        if self.row_hash_header {
            if let (Some(old_hash), Some(new_hash)) = (old_row_hash, encoded_row.row_hash) {
                if old_hash == new_hash {
                    if let Some(vref) = map_vref.take() {
                        let _ = self.vstore.free(tx, vref);
                    }
                    props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
                    return Ok(());
                }
            }
        }

        if let Err(err) = self.edges.put(tx, &id.0, &encoded_row.bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &delta.encoded.spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Edges)?;
        self.free_edge_props(tx, storage)
    }

    /// Returns neighboring nodes of a given node based on direction and edge type.
    pub fn neighbors(
        &self,
        tx: &ReadGuard,
        id: NodeId,
        dir: Dir,
        ty: Option<TypeId>,
        opts: ExpandOpts,
    ) -> Result<NeighborCursor> {
        let mut neighbors: Vec<Neighbor> = Vec::new();
        let enable_distinct = opts.distinct_nodes || self.distinct_neighbors_default;
        let mut seen_set = enable_distinct.then(|| HashSet::new());
        if dir.includes_out() {
            self.metrics.adjacency_scan("out");
            self.collect_neighbors(tx, id, ty, true, seen_set.as_mut(), &mut neighbors)?;
        }
        if dir.includes_in() {
            self.metrics.adjacency_scan("in");
            self.collect_neighbors(tx, id, ty, false, seen_set.as_mut(), &mut neighbors)?;
        }
        Ok(NeighborCursor::new(neighbors))
    }

    /// Performs a breadth-first traversal from `start`, returning visited nodes up to `max_depth`.
    pub fn bfs(&self, tx: &ReadGuard, start: NodeId, opts: &BfsOptions) -> Result<Vec<BfsVisit>> {
        if !self.node_exists(tx, start)? {
            return Err(SombraError::NotFound);
        }
        let mut queue: VecDeque<(NodeId, u32)> = VecDeque::new();
        let mut seen: HashSet<NodeId> = HashSet::new();
        let mut visits: Vec<BfsVisit> = Vec::new();
        queue.push_back((start, 0));
        seen.insert(start);
        let type_filters = opts.edge_types.as_deref();
        while let Some((node, depth)) = queue.pop_front() {
            visits.push(BfsVisit { node, depth });
            if let Some(limit) = opts.max_results {
                if visits.len() >= limit {
                    break;
                }
            }
            if depth >= opts.max_depth {
                continue;
            }
            match type_filters {
                Some(types) if !types.is_empty() => {
                    for ty in types {
                        self.enqueue_bfs_neighbors(
                            tx,
                            node,
                            opts.direction,
                            Some(*ty),
                            depth + 1,
                            &mut seen,
                            &mut queue,
                        )?;
                    }
                }
                _ => {
                    self.enqueue_bfs_neighbors(
                        tx,
                        node,
                        opts.direction,
                        None,
                        depth + 1,
                        &mut seen,
                        &mut queue,
                    )?;
                }
            }
        }
        Ok(visits)
    }

    /// Computes the degree (number of edges) for a node in a given direction.
    pub fn degree(&self, tx: &ReadGuard, id: NodeId, dir: Dir, ty: Option<TypeId>) -> Result<u64> {
        let result = match dir {
            Dir::Out => self.degree_single(tx, id, true, ty)?,
            Dir::In => self.degree_single(tx, id, false, ty)?,
            Dir::Both => {
                let out = self.degree_single(tx, id, true, ty)?;
                let inn = self.degree_single(tx, id, false, ty)?;
                let loops = self.count_loop_edges(tx, id, ty)?;
                out + inn - loops
            }
        };
        let direction_str = match dir {
            Dir::Out => "out",
            Dir::In => "in",
            Dir::Both => "both",
        };
        let cached = self.degree_has_cache_entry(tx, id, dir, ty)?;
        self.metrics.degree_query(direction_str, cached);
        Ok(result)
    }

    /// Deletes an edge from the graph by ID.
    pub fn delete_edge(&self, tx: &mut WriteGuard<'_>, id: EdgeId) -> Result<()> {
        let Some(bytes) = self.edges.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let versioned = edge::decode(&bytes)?;
        if versioned.header.is_tombstone() {
            return Err(SombraError::NotFound);
        }
        let row = versioned.row;
        self.remove_adjacency(tx, row.src, row.dst, row.ty, id)?;
        let (commit_id, mut tombstone_header) = self.tx_version_header(tx);
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            VersionSpace::Edge,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes,
        );
        tombstone_header.flags |= mvcc_flags::TOMBSTONE;
        let encoded = edge::encode(
            row.src,
            row.dst,
            row.ty,
            EdgePropPayload::Inline(&[]),
            EdgeEncodeOpts::new(false),
            tombstone_header,
            prev_ptr,
        )?;
        self.edges.put(tx, &id.0, &encoded.bytes)?;
        self.persist_tree_root(tx, RootKind::Edges)?;
        self.free_edge_props(tx, row.props)
    }

    #[inline]
    fn tx_version_header(&self, tx: &mut WriteGuard<'_>) -> (CommitId, VersionHeader) {
        let commit_lsn = tx.reserve_commit_id();
        let commit_id = commit_lsn.0;
        (commit_id, VersionHeader::new(commit_id, COMMIT_MAX, 0, 0))
    }

    fn adjacency_value(
        &self,
        tx: &mut WriteGuard<'_>,
        tombstone: bool,
    ) -> VersionedValue<UnitValue> {
        let (_commit_id, mut header) = self.tx_version_header(tx);
        if tombstone {
            header.flags |= mvcc_flags::TOMBSTONE;
        }
        VersionedValue::new(header, UnitValue)
    }

    #[inline]
    fn reader_snapshot_commit(tx: &ReadGuard) -> CommitId {
        tx.snapshot_lsn().0
    }

    #[inline]
    fn version_visible(header: &VersionHeader, snapshot: CommitId) -> bool {
        header.visible_at(snapshot) && !header.is_tombstone()
    }

    fn visible_node_version<'a>(
        &self,
        tx: &ReadGuard,
        bytes: &'a [u8],
    ) -> Result<Option<node::VersionedNodeRow>> {
        let mut current = node::decode(bytes)?;
        let snapshot = Self::reader_snapshot_commit(tx);
        if Self::version_visible(&current.header, snapshot) {
            return Ok(Some(current));
        }
        let mut ptr = current.prev_ptr;
        while let Some(entry) = self.version_log.lock().get(ptr) {
            if entry.space != VersionSpace::Node {
                break;
            }
            if entry.id != current.row.labels.first().map(|_| 0).unwrap_or(0) {
                ptr = entry.prev_ptr;
                continue;
            }
            let decoded = node::decode(&entry.bytes)?;
            if Self::version_visible(&decoded.header, snapshot) {
                return Ok(Some(decoded));
            }
            ptr = decoded.prev_ptr;
        }
        Ok(None)
    }

    fn encode_property_map(
        &self,
        tx: &mut WriteGuard<'_>,
        props: &[PropEntry<'_>],
    ) -> Result<(Vec<u8>, Vec<VRef>)> {
        let result = props::encode_props(props, self.inline_prop_value, &self.vstore, tx)?;
        Ok((result.bytes, result.spill_vrefs))
    }

    fn ensure_node_exists(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        context: &'static str,
    ) -> Result<()> {
        if self.node_exists_with_write(tx, node)? {
            Ok(())
        } else {
            Err(SombraError::Invalid(context))
        }
    }

    /// Returns true if the node exists using a read guard.
    pub fn node_exists(&self, tx: &ReadGuard, node: NodeId) -> Result<bool> {
        Ok(self.nodes.get(tx, &node.0)?.is_some())
    }

    fn node_exists_with_write(&self, tx: &mut WriteGuard<'_>, node: NodeId) -> Result<bool> {
        Ok(self.nodes.get_with_write(tx, &node.0)?.is_some())
    }

    fn insert_adjacencies(
        &self,
        tx: &mut WriteGuard<'_>,
        entries: &[(NodeId, NodeId, TypeId, EdgeId)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let mut keys: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len());
        for (src, dst, ty, edge) in entries {
            let fwd_key = adjacency::encode_fwd_key(*src, *ty, *dst, *edge);
            let rev_key = adjacency::encode_rev_key(*dst, *ty, *src, *edge);
            keys.push((fwd_key, rev_key));
        }
        let versioned_unit = self.adjacency_value(tx, false);
        {
            let mut refs: Vec<&Vec<u8>> = keys.iter().map(|(fwd, _)| fwd).collect();
            refs.sort_unstable_by(|a, b| a.cmp(b));
            let value_ref = &versioned_unit;
            let iter = refs.into_iter().map(|key| PutItem {
                key,
                value: value_ref,
            });
            self.adj_fwd.put_many(tx, iter)?;
            self.persist_tree_root(tx, RootKind::AdjFwd)?;
        }
        {
            let mut refs: Vec<&Vec<u8>> = keys.iter().map(|(_, rev)| rev).collect();
            refs.sort_unstable_by(|a, b| a.cmp(b));
            let value_ref = &versioned_unit;
            let iter = refs.into_iter().map(|key| PutItem {
                key,
                value: value_ref,
            });
            if let Err(err) = self.adj_rev.put_many(tx, iter) {
                self.rollback_adjacency_batch(tx, &keys)?;
                return Err(err);
            }
            self.persist_tree_root(tx, RootKind::AdjRev)?;
        }
        #[cfg(feature = "degree-cache")]
        if self.degree_cache_enabled {
            let mut deltas: BTreeMap<(NodeId, DegreeDir, TypeId), i64> = BTreeMap::new();
            for (src, dst, ty, _edge) in entries {
                *deltas.entry((*src, DegreeDir::Out, *ty)).or_default() += 1;
                *deltas.entry((*dst, DegreeDir::In, *ty)).or_default() += 1;
            }
            if let Err(err) = self.apply_degree_batch(tx, &deltas) {
                self.rollback_adjacency_batch(tx, &keys)?;
                return Err(err);
            }
        }
        Ok(())
    }

    fn remove_adjacency(
        &self,
        tx: &mut WriteGuard<'_>,
        src: NodeId,
        dst: NodeId,
        ty: TypeId,
        edge: EdgeId,
    ) -> Result<()> {
        let fwd_key = adjacency::encode_fwd_key(src, ty, dst, edge);
        let rev_key = adjacency::encode_rev_key(dst, ty, src, edge);
        let versioned_tombstone = self.adjacency_value(tx, true);
        let mut ensure_existing =
            |tree: &BTree<Vec<u8>, VersionedValue<UnitValue>>, key: &Vec<u8>| -> Result<()> {
                if tree.get_with_write(tx, key)?.is_none() {
                    return Err(SombraError::Corruption(
                        "adjacency entry missing during delete",
                    ));
                }
                Ok(())
            };
        ensure_existing(&self.adj_fwd, &fwd_key)?;
        ensure_existing(&self.adj_rev, &rev_key)?;
        self.adj_fwd.put(tx, &fwd_key, &versioned_tombstone)?;
        self.adj_rev.put(tx, &rev_key, &versioned_tombstone)?;
        self.persist_tree_root(tx, RootKind::AdjFwd)?;
        self.persist_tree_root(tx, RootKind::AdjRev)?;
        #[cfg(feature = "degree-cache")]
        if self.degree_cache_enabled {
            self.bump_degree(tx, src, DegreeDir::Out, ty, -1)?;
            self.bump_degree(tx, dst, DegreeDir::In, ty, -1)?;
        }
        Ok(())
    }

    fn collect_incident_edges(&self, read: &ReadGuard, node: NodeId) -> Result<HashSet<EdgeId>> {
        let mut edges = HashSet::new();
        self.collect_adjacent_edges(read, node, true, &mut edges)?;
        self.collect_adjacent_edges(read, node, false, &mut edges)?;
        Ok(edges)
    }

    fn collect_adjacent_edges(
        &self,
        read: &ReadGuard,
        node: NodeId,
        forward: bool,
        edges: &mut HashSet<EdgeId>,
    ) -> Result<()> {
        let (lo, hi) = adjacency_bounds_for_node(node);
        let tree = if forward {
            &self.adj_fwd
        } else {
            &self.adj_rev
        };
        let snapshot = Self::reader_snapshot_commit(read);
        let mut cursor = tree.range(read, Bound::Included(lo), Bound::Included(hi))?;
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded = if forward {
                adjacency::decode_fwd_key(&key)
            } else {
                adjacency::decode_rev_key(&key)
            }
            .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
            edges.insert(decoded.3);
        }
        Ok(())
    }

    fn collect_neighbors(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        ty_filter: Option<TypeId>,
        forward: bool,
        seen: Option<&mut HashSet<NodeId>>,
        out: &mut Vec<Neighbor>,
    ) -> Result<()> {
        let (lo, hi) = if forward {
            adjacency::fwd_bounds(node, ty_filter)
        } else {
            adjacency::rev_bounds(node, ty_filter)
        };
        let tree = if forward {
            &self.adj_fwd
        } else {
            &self.adj_rev
        };
        let snapshot = Self::reader_snapshot_commit(tx);
        let mut cursor = tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut seen = seen;
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            if forward {
                let (src, ty, dst, edge) = adjacency::decode_fwd_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                debug_assert_eq!(src, node);
                match src.cmp(&node) {
                    CmpOrdering::Less => continue,
                    CmpOrdering::Greater => break,
                    CmpOrdering::Equal => {}
                }
                if let Some(filter) = ty_filter {
                    if ty != filter {
                        continue;
                    }
                }
                if let Some(set) = seen.as_deref_mut() {
                    if !set.insert(dst) {
                        continue;
                    }
                }
                out.push(Neighbor {
                    neighbor: dst,
                    edge,
                    ty,
                });
            } else {
                let (dst, ty, src, edge) = adjacency::decode_rev_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                debug_assert_eq!(dst, node);
                match dst.cmp(&node) {
                    CmpOrdering::Less => continue,
                    CmpOrdering::Greater => break,
                    CmpOrdering::Equal => {}
                }
                if let Some(filter) = ty_filter {
                    if ty != filter {
                        continue;
                    }
                }
                if let Some(set) = seen.as_deref_mut() {
                    if !set.insert(src) {
                        continue;
                    }
                }
                out.push(Neighbor {
                    neighbor: src,
                    edge,
                    ty,
                });
            }
        }
        Ok(())
    }

    fn enqueue_bfs_neighbors(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        ty_filter: Option<TypeId>,
        next_depth: u32,
        seen: &mut HashSet<NodeId>,
        queue: &mut VecDeque<(NodeId, u32)>,
    ) -> Result<()> {
        let mut cursor = self.neighbors(
            tx,
            node,
            dir,
            ty_filter,
            ExpandOpts {
                distinct_nodes: false,
            },
        )?;
        while let Some(neighbor) = cursor.next() {
            if seen.insert(neighbor.neighbor) {
                queue.push_back((neighbor.neighbor, next_depth));
            }
        }
        Ok(())
    }

    fn degree_single(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        forward: bool,
        ty: Option<TypeId>,
    ) -> Result<u64> {
        #[cfg(feature = "degree-cache")]
        {
            if self.degree_cache_enabled {
                if let Some(tree) = &self.degree {
                    let dir = if forward {
                        DegreeDir::Out
                    } else {
                        DegreeDir::In
                    };
                    match ty {
                        Some(ty) => {
                            let key = adjacency::encode_degree_key(node, dir, ty);
                            if let Some(value) = tree.get(tx, &key)? {
                                return Ok(value);
                            }
                        }
                        None => {
                            let (lo, hi) = adjacency::degree_bounds(node, dir, None);
                            let mut cursor =
                                tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
                            let mut total = 0u64;
                            while let Some((_key, value)) = cursor.next()? {
                                total = total.saturating_add(value);
                            }
                            return Ok(total);
                        }
                    }
                }
            }
        }
        self.count_adjacent_edges(tx, node, ty, forward)
    }

    fn degree_has_cache_entry(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        ty: Option<TypeId>,
    ) -> Result<bool> {
        #[cfg(feature = "degree-cache")]
        {
            if !self.degree_cache_enabled {
                return Ok(false);
            }
            let Some(tree) = &self.degree else {
                return Ok(false);
            };
            let result = match dir {
                Dir::Out => {
                    let tag = DegreeDir::Out;
                    match ty {
                        Some(ty) => {
                            let key = adjacency::encode_degree_key(node, tag, ty);
                            tree.get(tx, &key)?.is_some()
                        }
                        None => {
                            let (lo, hi) = adjacency::degree_bounds(node, tag, None);
                            let mut cursor =
                                tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
                            cursor.next()?.is_some()
                        }
                    }
                }
                Dir::In => {
                    let tag = DegreeDir::In;
                    match ty {
                        Some(ty) => {
                            let key = adjacency::encode_degree_key(node, tag, ty);
                            tree.get(tx, &key)?.is_some()
                        }
                        None => {
                            let (lo, hi) = adjacency::degree_bounds(node, tag, None);
                            let mut cursor =
                                tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
                            cursor.next()?.is_some()
                        }
                    }
                }
                Dir::Both => {
                    let out_has = self.degree_has_cache_entry(tx, node, Dir::Out, ty)?;
                    let in_has = self.degree_has_cache_entry(tx, node, Dir::In, ty)?;
                    out_has && in_has
                }
            };
            Ok(result)
        }
        #[cfg(not(feature = "degree-cache"))]
        {
            let _ = (tx, node, dir, ty);
            return Ok(false);
        }
    }
    fn count_adjacent_edges(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        ty: Option<TypeId>,
        forward: bool,
    ) -> Result<u64> {
        let (lo, hi) = if forward {
            adjacency::fwd_bounds(node, ty)
        } else {
            adjacency::rev_bounds(node, ty)
        };
        let tree = if forward {
            &self.adj_fwd
        } else {
            &self.adj_rev
        };
        let mut cursor = tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut count = 0u64;
        while cursor.next()?.is_some() {
            count = count.saturating_add(1);
        }
        Ok(count)
    }

    fn count_loop_edges(&self, tx: &ReadGuard, node: NodeId, ty: Option<TypeId>) -> Result<u64> {
        let (lo, hi) = adjacency::fwd_bounds(node, ty);
        let mut cursor = self
            .adj_fwd
            .range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut loops = 0u64;
        while let Some((key, _)) = cursor.next()? {
            let (src, ty_val, dst, _edge) = adjacency::decode_fwd_key(&key)
                .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
            debug_assert_eq!(src, node);
            if dst == node && ty.map(|t| t == ty_val).unwrap_or(true) {
                loops = loops.saturating_add(1);
            }
        }
        Ok(loops)
    }

    fn free_edge_props(&self, tx: &mut WriteGuard<'_>, props: EdgePropStorage) -> Result<()> {
        match props {
            EdgePropStorage::Inline(bytes) => self.free_prop_values_from_bytes(tx, &bytes),
            EdgePropStorage::VRef(vref) => {
                let bytes = self.vstore.read_with_write(tx, vref)?;
                self.free_prop_values_from_bytes(tx, &bytes)?;
                self.vstore.free(tx, vref)
            }
        }
    }

    fn free_prop_values_from_bytes(&self, tx: &mut WriteGuard<'_>, bytes: &[u8]) -> Result<()> {
        let raw = props::decode_raw(bytes)?;
        for entry in raw {
            match entry.value {
                RawPropValue::StrVRef(vref) | RawPropValue::BytesVRef(vref) => {
                    self.vstore.free(tx, vref)?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn free_node_props(&self, tx: &mut WriteGuard<'_>, props: NodePropStorage) -> Result<()> {
        match props {
            NodePropStorage::Inline(bytes) => self.free_prop_values_from_bytes(tx, &bytes),
            NodePropStorage::VRef(vref) => {
                let bytes = self.vstore.read_with_write(tx, vref)?;
                self.free_prop_values_from_bytes(tx, &bytes)?;
                self.vstore.free(tx, vref)
            }
        }
    }

    fn read_node_prop_bytes(&self, storage: &NodePropStorage) -> Result<Vec<u8>> {
        match storage {
            NodePropStorage::Inline(bytes) => Ok(bytes.clone()),
            NodePropStorage::VRef(vref) => {
                let read = self.store.begin_latest_committed_read()?;
                self.vstore.read(&read, *vref)
            }
        }
    }

    fn read_node_prop_bytes_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        storage: &NodePropStorage,
    ) -> Result<Vec<u8>> {
        match storage {
            NodePropStorage::Inline(bytes) => Ok(bytes.clone()),
            NodePropStorage::VRef(vref) => self.vstore.read_with_write(tx, *vref),
        }
    }

    fn read_edge_prop_bytes_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        storage: &EdgePropStorage,
    ) -> Result<Vec<u8>> {
        match storage {
            EdgePropStorage::Inline(bytes) => Ok(bytes.clone()),
            EdgePropStorage::VRef(vref) => self.vstore.read_with_write(tx, *vref),
        }
    }

    fn materialize_props_owned(&self, bytes: &[u8]) -> Result<Vec<(PropId, PropValueOwned)>> {
        let raw = props::decode_raw(bytes)?;
        let read = self.store.begin_latest_committed_read()?;
        let props = props::materialize_props(&raw, &self.vstore, &read)?;
        Ok(props)
    }

    fn materialize_props_owned_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        bytes: &[u8],
    ) -> Result<Vec<(PropId, PropValueOwned)>> {
        let raw = props::decode_raw(bytes)?;
        props::materialize_props_with_write(&raw, &self.vstore, tx)
    }

    #[cfg(feature = "degree-cache")]
    fn bump_degree(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: DegreeDir,
        ty: TypeId,
        delta: i64,
    ) -> Result<()> {
        if !self.degree_cache_enabled {
            return Ok(());
        }
        let Some(tree) = &self.degree else {
            return Ok(());
        };
        let key = adjacency::encode_degree_key(node, dir, ty);
        let current = tree.get_with_write(tx, &key)?;
        let current_val = current.unwrap_or(0);
        let new_val = if delta.is_negative() {
            let abs = delta.abs() as u64;
            if abs > current_val {
                return Err(SombraError::Corruption("degree underflow"));
            }
            current_val - abs
        } else {
            current_val.saturating_add(delta as u64)
        };
        if new_val == 0 {
            let removed = tree.delete(tx, &key)?;
            if delta.is_negative() && !removed {
                return Err(SombraError::Corruption(
                    "degree entry missing during delete",
                ));
            }
        } else {
            tree.put(tx, &key, &new_val)?;
        }
        self.persist_tree_root(tx, RootKind::Degree)?;
        Ok(())
    }
}

const TRUST_VALIDATOR_REQUIRED: &str = "trusted endpoints require validator";
const TRUST_BATCH_REQUIRED: &str = "trusted endpoints batch must be validated";

/// Options controlling how [`GraphWriter`] inserts edges.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CreateEdgeOptions {
    /// Whether edge endpoints have been validated externally and can skip lookups.
    pub trusted_endpoints: bool,
    /// Capacity of the node-existence cache when validation is required.
    pub exists_cache_capacity: usize,
}

impl Default for CreateEdgeOptions {
    fn default() -> Self {
        Self {
            trusted_endpoints: false,
            exists_cache_capacity: 1024,
        }
    }
}

/// Aggregate statistics captured by [`GraphWriter`].
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GraphWriterStats {
    /// Number of cache hits for endpoint existence checks.
    pub exists_cache_hits: u64,
    /// Number of cache misses for endpoint existence checks.
    pub exists_cache_misses: u64,
    /// Number of edges inserted using trusted endpoints.
    pub trusted_edges: u64,
}

/// Validator used by [`GraphWriter`] to confirm endpoints exist before trusting batches.
pub trait BulkEdgeValidator {
    /// Validates a batch of `(src, dst)` pairs before inserts begin.
    fn validate_batch(&self, edges: &[(NodeId, NodeId)]) -> Result<()>;
}

/// Batched edge writer that amortizes endpoint probes and supports trusted inserts.
pub struct GraphWriter<'a> {
    graph: &'a Graph,
    opts: CreateEdgeOptions,
    exists_cache: Option<LruCache<NodeId, bool>>,
    validator: Option<Box<dyn BulkEdgeValidator + 'a>>,
    stats: GraphWriterStats,
    trust_budget: usize,
}

impl<'a> GraphWriter<'a> {
    /// Constructs a new writer for the provided [`Graph`].
    pub fn try_new(
        graph: &'a Graph,
        opts: CreateEdgeOptions,
        validator: Option<Box<dyn BulkEdgeValidator + 'a>>,
    ) -> Result<Self> {
        if opts.trusted_endpoints && validator.is_none() {
            return Err(SombraError::Invalid(TRUST_VALIDATOR_REQUIRED));
        }
        let exists_cache = NonZeroUsize::new(opts.exists_cache_capacity).map(LruCache::new);
        Ok(Self {
            graph,
            opts,
            exists_cache,
            validator,
            stats: GraphWriterStats::default(),
            trust_budget: 0,
        })
    }

    /// Returns the options associated with this writer.
    pub fn options(&self) -> &CreateEdgeOptions {
        &self.opts
    }

    /// Returns current statistics collected by the writer.
    pub fn stats(&self) -> GraphWriterStats {
        self.stats
    }

    /// Validates a batch of edges before inserting them in trusted mode.
    pub fn validate_trusted_batch(&mut self, edges: &[(NodeId, NodeId)]) -> Result<()> {
        if !self.opts.trusted_endpoints {
            return Ok(());
        }
        let Some(validator) = self.validator.as_ref() else {
            return Err(SombraError::Invalid(TRUST_VALIDATOR_REQUIRED));
        };
        validator.validate_batch(edges)?;
        self.trust_budget = edges.len();
        Ok(())
    }

    /// Creates an edge with the configured validation strategy.
    pub fn create_edge(&mut self, tx: &mut WriteGuard<'_>, spec: EdgeSpec<'_>) -> Result<EdgeId> {
        if self.opts.trusted_endpoints {
            if self.trust_budget == 0 {
                return Err(SombraError::Invalid(TRUST_BATCH_REQUIRED));
            }
            self.trust_budget -= 1;
            self.stats.trusted_edges = self.stats.trusted_edges.saturating_add(1);
        } else {
            self.ensure_endpoint(tx, spec.src, "edge source node missing")?;
            self.ensure_endpoint(tx, spec.dst, "edge destination node missing")?;
        }
        self.graph.insert_edge_unchecked(tx, spec)
    }

    fn ensure_endpoint(
        &mut self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        context: &'static str,
    ) -> Result<()> {
        if let Some(cache) = self.exists_cache.as_mut() {
            if let Some(hit) = cache.get(&node).copied() {
                self.stats.exists_cache_hits = self.stats.exists_cache_hits.saturating_add(1);
                if hit {
                    return Ok(());
                }
                return Err(SombraError::Invalid(context));
            }
        }
        let exists = self.graph.node_exists_with_write(tx, node)?;
        if let Some(cache) = self.exists_cache.as_mut() {
            cache.put(node, exists);
        }
        self.stats.exists_cache_misses = self.stats.exists_cache_misses.saturating_add(1);
        if exists {
            Ok(())
        } else {
            Err(SombraError::Invalid(context))
        }
    }
}

fn open_u64_vec_tree(store: &Arc<dyn PageStore>, root: PageId) -> Result<BTree<u64, Vec<u8>>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

fn open_unit_tree(
    store: &Arc<dyn PageStore>,
    root: PageId,
) -> Result<BTree<Vec<u8>, VersionedValue<UnitValue>>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

#[cfg(feature = "degree-cache")]
fn open_degree_tree(store: &Arc<dyn PageStore>, root: PageId) -> Result<BTree<Vec<u8>, u64>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

struct GraphTxnState {
    index_cache: GraphIndexCache,
}

impl GraphTxnState {
    fn new(epoch: DdlEpoch) -> Self {
        Self {
            index_cache: GraphIndexCache::new(epoch),
        }
    }
}

struct PropDelta {
    old_map: BTreeMap<PropId, PropValueOwned>,
    new_map: BTreeMap<PropId, PropValueOwned>,
    encoded: props::PropEncodeResult,
}

fn apply_patch_ops(map: &mut BTreeMap<PropId, PropValueOwned>, ops: &[PropPatchOp<'_>]) {
    for op in ops {
        match op {
            PropPatchOp::Set(prop, value) => {
                map.insert(*prop, prop_value_to_owned(value.clone()));
            }
            PropPatchOp::Delete(prop) => {
                map.remove(prop);
            }
        }
    }
}

fn encode_value_key_owned(ty: TypeTag, value: &PropValueOwned) -> Result<Vec<u8>> {
    match (ty, value) {
        (TypeTag::Null, PropValueOwned::Null) => Ok(Vec::new()),
        (TypeTag::Bool, PropValueOwned::Bool(v)) => Ok(vec![u8::from(*v)]),
        (TypeTag::Int, PropValueOwned::Int(v)) => Ok(encode_i64_key(*v).to_vec()),
        (TypeTag::Float, PropValueOwned::Float(v)) => encode_f64_key(*v),
        (TypeTag::String, PropValueOwned::Str(s)) => encode_bytes_key(s.as_bytes()),
        (TypeTag::Bytes, PropValueOwned::Bytes(b)) => encode_bytes_key(b),
        (TypeTag::Date, PropValueOwned::Date(v)) => Ok(encode_i64_key(*v).to_vec()),
        (TypeTag::DateTime, PropValueOwned::DateTime(v)) => Ok(encode_i64_key(*v).to_vec()),
        _ => Err(SombraError::Invalid(
            "property value type mismatch for index",
        )),
    }
}

fn encode_i64_key(value: i64) -> [u8; 8] {
    ((value as u64) ^ 0x8000_0000_0000_0000).to_be_bytes()
}

fn encode_f64_key(value: f64) -> Result<Vec<u8>> {
    if value.is_nan() {
        return Err(SombraError::Invalid("NaN values cannot be indexed"));
    }
    let bits = value.to_bits();
    let normalized = if bits & 0x8000_0000_0000_0000 != 0 {
        !bits
    } else {
        bits ^ 0x8000_0000_0000_0000
    };
    Ok(normalized.to_be_bytes().to_vec())
}

fn encode_bytes_key(bytes: &[u8]) -> Result<Vec<u8>> {
    let len = u32::try_from(bytes.len())
        .map_err(|_| SombraError::Invalid("property value exceeds maximum length"))?;
    let mut out = Vec::with_capacity(4 + bytes.len());
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(bytes);
    Ok(out)
}

fn prop_stats_key(value: &PropValueOwned) -> Vec<u8> {
    use PropValueOwned::*;
    let mut out = Vec::new();
    match value {
        Null => out.push(0),
        Bool(v) => {
            out.push(1);
            out.push(u8::from(*v));
        }
        Int(v) => {
            out.push(2);
            out.extend_from_slice(&encode_i64_key(*v));
        }
        Float(v) => {
            out.push(3);
            out.extend_from_slice(&encode_f64_key(*v).unwrap_or_else(|_| vec![0; 8]));
        }
        Str(v) => {
            out.push(4);
            out.extend(
                encode_bytes_key(v.as_bytes())
                    .unwrap_or_else(|_| v.as_bytes().to_vec())
                    .into_iter(),
            );
        }
        Bytes(v) => {
            out.push(5);
            out.extend(
                encode_bytes_key(v)
                    .unwrap_or_else(|_| v.clone())
                    .into_iter(),
            );
        }
        Date(v) => {
            out.push(6);
            out.extend_from_slice(&encode_i64_key(*v));
        }
        DateTime(v) => {
            out.push(7);
            out.extend_from_slice(&encode_i64_key(*v));
        }
    }
    out
}

fn update_min_max(
    slot: &mut Option<PropValueOwned>,
    candidate: &PropValueOwned,
    desired: Ordering,
) -> Result<()> {
    match slot {
        Some(current) => {
            if compare_prop_values(candidate, current)? == desired {
                *slot = Some(candidate.clone());
            }
        }
        None => {
            *slot = Some(candidate.clone());
        }
    }
    Ok(())
}

fn compare_prop_values(a: &PropValueOwned, b: &PropValueOwned) -> Result<Ordering> {
    use PropValueOwned::*;
    Ok(match (a, b) {
        (Null, Null) => Ordering::Equal,
        (Null, _) => Ordering::Less,
        (_, Null) => Ordering::Greater,
        (Bool(a), Bool(b)) => a.cmp(b),
        (Int(a), Int(b)) => a.cmp(b),
        (Float(a), Float(b)) => a
            .partial_cmp(b)
            .ok_or(SombraError::Invalid("float comparison invalid"))?,
        (Str(a), Str(b)) => a.cmp(b),
        (Bytes(a), Bytes(b)) => a.cmp(b),
        (Date(a), Date(b)) => a.cmp(b),
        (DateTime(a), DateTime(b)) => a.cmp(b),
        (va, vb) => value_rank(va).cmp(&value_rank(vb)),
    })
}

fn value_rank(value: &PropValueOwned) -> u8 {
    use PropValueOwned::*;
    match value {
        Null => 0,
        Bool(_) => 1,
        Int(_) => 2,
        Float(_) => 3,
        Str(_) => 4,
        Bytes(_) => 5,
        Date(_) => 6,
        DateTime(_) => 7,
    }
}

fn prop_value_to_owned(value: PropValue<'_>) -> PropValueOwned {
    match value {
        PropValue::Null => PropValueOwned::Null,
        PropValue::Bool(v) => PropValueOwned::Bool(v),
        PropValue::Int(v) => PropValueOwned::Int(v),
        PropValue::Float(v) => PropValueOwned::Float(v),
        PropValue::Str(v) => PropValueOwned::Str(v.to_owned()),
        PropValue::Bytes(v) => PropValueOwned::Bytes(v.to_vec()),
        PropValue::Date(v) => PropValueOwned::Date(v),
        PropValue::DateTime(v) => PropValueOwned::DateTime(v),
    }
}

fn encode_range_bound(ty: TypeTag, bound: Bound<&PropValueOwned>) -> Result<Bound<Vec<u8>>> {
    match bound {
        Bound::Unbounded => Ok(Bound::Unbounded),
        Bound::Included(value) => encode_value_key_owned(ty, value).map(Bound::Included),
        Bound::Excluded(value) => encode_value_key_owned(ty, value).map(Bound::Excluded),
    }
}

fn collect_posting_stream(stream: &mut dyn PostingStream) -> Result<Vec<NodeId>> {
    let mut nodes = Vec::new();
    collect_all(stream, &mut nodes)?;
    nodes.sort_by_key(|node| node.0);
    nodes.dedup_by_key(|node| node.0);
    Ok(nodes)
}

fn instrument_posting_stream<'a>(
    stream: Box<dyn PostingStream + 'a>,
) -> Box<dyn PostingStream + 'a> {
    if storage_profiling_enabled() {
        Box::new(ProfilingPostingStream { inner: stream })
    } else {
        stream
    }
}

struct ProfilingPostingStream<'a> {
    inner: Box<dyn PostingStream + 'a>,
}

impl PostingStream for ProfilingPostingStream<'_> {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        let iter_timer = storage_profile_timer();
        let result = self.inner.next_batch(out, max);
        record_storage_profile_timer(StorageProfileKind::PropIndexStreamIter, iter_timer);
        result
    }
}

/// Computes adjacency list bounds for a given node.
fn adjacency_bounds_for_node(node: NodeId) -> (Vec<u8>, Vec<u8>) {
    const SUFFIX_LEN: usize = 4 + 8 + 8;
    let mut lower = Vec::with_capacity(8 + SUFFIX_LEN);
    lower.extend_from_slice(&node.0.to_be_bytes());
    lower.extend_from_slice(&[0u8; SUFFIX_LEN]);
    let mut upper = Vec::with_capacity(8 + SUFFIX_LEN);
    upper.extend_from_slice(&node.0.to_be_bytes());
    upper.extend_from_slice(&[0xFF; SUFFIX_LEN]);
    (lower, upper)
}

/// Normalizes and deduplicates a list of labels.
fn normalize_labels(labels: &[LabelId]) -> Result<Vec<LabelId>> {
    let mut result: Vec<LabelId> = labels.to_vec();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result.dedup_by(|a, b| a.0 == b.0);
    if result.len() > u8::MAX as usize {
        return Err(SombraError::Invalid("too many labels for node"));
    }
    Ok(result)
}

impl Graph {
    /// Collects all forward adjacency entries for debugging purposes.
    pub fn debug_collect_adj_fwd(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, TypeId, NodeId, EdgeId)>> {
        let mut cursor = self.adj_fwd.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut entries = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded =
                adjacency::decode_fwd_key(&key).ok_or(SombraError::Corruption("adj key decode"))?;
            entries.push(decoded);
        }
        Ok(entries)
    }

    /// Collects all reverse adjacency entries for debugging purposes.
    pub fn debug_collect_adj_rev(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, TypeId, NodeId, EdgeId)>> {
        let mut cursor = self.adj_rev.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut entries = Vec::new();
        let snapshot = Self::reader_snapshot_commit(tx);
        while let Some((key, value)) = cursor.next()? {
            if !Self::version_visible(&value.header, snapshot) {
                continue;
            }
            let decoded =
                adjacency::decode_rev_key(&key).ok_or(SombraError::Corruption("adj key decode"))?;
            entries.push(decoded);
        }
        Ok(entries)
    }

    fn insert_indexed_props(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        props: &BTreeMap<PropId, PropValueOwned>,
    ) -> Result<()> {
        for label in labels {
            let defs = self.index_defs_for_label(tx, *label)?;
            for def in defs.iter() {
                if let Some(value) = props.get(&def.prop) {
                    let key = encode_value_key_owned(def.ty, value)?;
                    self.indexes.insert_property_value(tx, &def, &key, node)?;
                }
            }
        }
        Ok(())
    }

    fn remove_indexed_props(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        props: &BTreeMap<PropId, PropValueOwned>,
    ) -> Result<()> {
        for label in labels {
            let defs = self.index_defs_for_label(tx, *label)?;
            for def in defs.iter() {
                if let Some(value) = props.get(&def.prop) {
                    let key = encode_value_key_owned(def.ty, value)?;
                    self.indexes.remove_property_value(tx, &def, &key, node)?;
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "degree-cache")]
    fn apply_degree_batch(
        &self,
        tx: &mut WriteGuard<'_>,
        deltas: &BTreeMap<(NodeId, DegreeDir, TypeId), i64>,
    ) -> Result<()> {
        if deltas.is_empty() {
            return Ok(());
        }
        let mut applied: Vec<(NodeId, DegreeDir, TypeId, i64)> = Vec::new();
        for ((node, dir, ty), delta) in deltas {
            if *delta == 0 {
                continue;
            }
            if let Err(err) = self.bump_degree(tx, *node, *dir, *ty, *delta) {
                for (node_applied, dir_applied, ty_applied, delta_applied) in
                    applied.into_iter().rev()
                {
                    let _ =
                        self.bump_degree(tx, node_applied, dir_applied, ty_applied, -delta_applied);
                }
                return Err(err);
            }
            applied.push((*node, *dir, *ty, *delta));
        }
        Ok(())
    }

    fn rollback_adjacency_batch(
        &self,
        tx: &mut WriteGuard<'_>,
        keys: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        for (fwd, rev) in keys {
            let _ = self.adj_fwd.delete(tx, fwd);
            let _ = self.adj_rev.delete(tx, rev);
        }
        self.persist_tree_root(tx, RootKind::AdjFwd)?;
        self.persist_tree_root(tx, RootKind::AdjRev)?;
        Ok(())
    }

    fn update_indexed_props_for_node(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        old_props: &BTreeMap<PropId, PropValueOwned>,
        new_props: &BTreeMap<PropId, PropValueOwned>,
    ) -> Result<()> {
        for label in labels {
            let defs = self.index_defs_for_label(tx, *label)?;
            for def in defs.iter() {
                let old = old_props.get(&def.prop);
                let new = new_props.get(&def.prop);
                if old == new {
                    continue;
                }
                if let Some(value) = old {
                    let key = encode_value_key_owned(def.ty, value)?;
                    self.indexes.remove_property_value(tx, &def, &key, node)?;
                }
                if let Some(value) = new {
                    let key = encode_value_key_owned(def.ty, value)?;
                    self.indexes.insert_property_value(tx, &def, &key, node)?;
                }
            }
        }
        Ok(())
    }

    fn build_prop_delta(
        &self,
        tx: &mut WriteGuard<'_>,
        prop_bytes: &[u8],
        patch: &PropPatch<'_>,
    ) -> Result<Option<PropDelta>> {
        if patch.is_empty() {
            return Ok(None);
        }
        let current = self.materialize_props_owned_with_write(tx, prop_bytes)?;
        let mut new_map: BTreeMap<PropId, PropValueOwned> = current.into_iter().collect();
        let old_map = new_map.clone();
        apply_patch_ops(&mut new_map, &patch.ops);
        if new_map == old_map {
            return Ok(None);
        }
        let ordered = new_map
            .iter()
            .map(|(prop, value)| (*prop, value.clone()))
            .collect::<Vec<_>>();
        let encoded =
            props::encode_props_owned(&ordered, self.inline_prop_value, &self.vstore, tx)?;
        Ok(Some(PropDelta {
            old_map,
            new_map,
            encoded,
        }))
    }

    fn index_defs_for_label(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
    ) -> Result<Arc<Vec<IndexDef>>> {
        let mut state = self.take_txn_state(tx);
        state.index_cache.sync_epoch(self.catalog_epoch.current());
        let result = state.index_cache.get_or_load(label, |label| {
            self.indexes
                .property_indexes_for_label_with_write(tx, label)
        });
        self.store_txn_state(tx, state);
        result
    }

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

    fn bump_ddl_epoch(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        self.catalog_epoch.bump_in_txn(tx)?;
        self.invalidate_txn_cache(tx);
        Ok(())
    }

    fn sync_index_roots(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        let roots = self.indexes.roots();
        tx.update_meta(|meta| {
            meta.storage_index_catalog_root = roots.catalog;
            meta.storage_label_index_root = roots.label;
            meta.storage_prop_chunk_root = roots.prop_chunk;
            meta.storage_prop_btree_root = roots.prop_btree;
        })
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
    /// Returns every stored degree cache entry for debugging purposes.
    pub fn debug_collect_degree(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, adjacency::DegreeDir, TypeId, u64)>> {
        let Some(tree) = &self.degree else {
            return Ok(Vec::new());
        };
        let mut cursor = tree.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut rows = Vec::new();
        while let Some((key, value)) = cursor.next()? {
            let (node, dir, ty) = adjacency::decode_degree_key(&key)
                .ok_or(SombraError::Corruption("degree key decode"))?;
            rows.push((node, dir, ty, value));
        }
        Ok(rows)
    }

    #[cfg(feature = "degree-cache")]
    /// Verifies that the degree cache matches the adjacency trees.
    pub fn validate_degree_cache(&self, tx: &ReadGuard) -> Result<()> {
        let Some(tree) = &self.degree else {
            return Ok(());
        };
        let mut actual: HashMap<(NodeId, adjacency::DegreeDir, TypeId), u64> = HashMap::new();
        {
            let mut cursor = self.adj_fwd.range(tx, Bound::Unbounded, Bound::Unbounded)?;
            let snapshot = Self::reader_snapshot_commit(tx);
            while let Some((key, value)) = cursor.next()? {
                if !Self::version_visible(&value.header, snapshot) {
                    continue;
                }
                let (src, ty, _, _) = adjacency::decode_fwd_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                *actual
                    .entry((src, adjacency::DegreeDir::Out, ty))
                    .or_insert(0) += 1;
            }
        }
        {
            let mut cursor = self.adj_rev.range(tx, Bound::Unbounded, Bound::Unbounded)?;
            let snapshot = Self::reader_snapshot_commit(tx);
            while let Some((key, value)) = cursor.next()? {
                if !Self::version_visible(&value.header, snapshot) {
                    continue;
                }
                let (dst, ty, _, _) = adjacency::decode_rev_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                *actual
                    .entry((dst, adjacency::DegreeDir::In, ty))
                    .or_insert(0) += 1;
            }
        }

        let mut degree_cursor = tree.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        while let Some((key, stored)) = degree_cursor.next()? {
            let (node, dir, ty) = adjacency::decode_degree_key(&key)
                .ok_or(SombraError::Corruption("degree key decode failed"))?;
            let actual_count = actual.remove(&(node, dir, ty)).unwrap_or(0);
            if actual_count != stored {
                return Err(SombraError::Corruption("degree cache mismatch"));
            }
        }
        if actual.values().any(|count| *count > 0) {
            return Err(SombraError::Corruption(
                "degree cache missing entry for adjacency",
            ));
        }
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
    /// Computes light-weight property statistics for a label/property pair.
    pub fn property_stats(&self, label: LabelId, prop: PropId) -> Result<Option<PropStats>> {
        let read = self.store.begin_latest_committed_read()?;
        let Some(mut scan) = self.indexes.label_scan(&read, label)? else {
            return Ok(None);
        };

        let mut stats = PropStats::default();
        let mut distinct_keys: HashSet<Vec<u8>> = HashSet::new();

        while let Some(node_id) = scan.next()? {
            stats.row_count += 1;
            let Some(node) = self.get_node(&read, node_id)? else {
                continue;
            };
            let value = node
                .props
                .iter()
                .find_map(|(prop_id, value)| (*prop_id == prop).then(|| value.clone()));
            match value {
                Some(PropValueOwned::Null) | None => {
                    stats.null_count += 1;
                }
                Some(value) => {
                    stats.non_null_count += 1;
                    distinct_keys.insert(prop_stats_key(&value));
                    update_min_max(&mut stats.min, &value, Ordering::Less)?;
                    update_min_max(&mut stats.max, &value, Ordering::Greater)?;
                }
            }
        }

        stats.distinct_count = distinct_keys.len() as u64;
        Ok(Some(stats))
    }

    /// Returns the current catalog epoch used for DDL invalidation.
    pub fn catalog_epoch(&self) -> u64 {
        self.catalog_epoch.current().0
    }
}

struct FallbackLabelScan {
    nodes: Vec<NodeId>,
    pos: usize,
}

impl PostingStream for FallbackLabelScan {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        if max == 0 {
            return Ok(self.pos >= self.nodes.len());
        }
        let mut produced = 0;
        while self.pos < self.nodes.len() && produced < max {
            out.push(self.nodes[self.pos]);
            self.pos += 1;
            produced += 1;
        }
        Ok(self.pos >= self.nodes.len())
    }
}
