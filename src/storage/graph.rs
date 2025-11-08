#[cfg(feature = "degree-cache")]
use std::collections::HashMap;
use std::collections::{BTreeMap, HashSet};
use std::convert::TryFrom;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::btree::{BTree, BTreeOptions, ValCodec};
use crate::storage::index::{
    collect_all, IndexDef, IndexRoots, IndexStore, LabelScan, PostingStream, TypeTag,
};
use crate::storage::vstore::VStore;
use crate::types::{EdgeId, LabelId, NodeId, PageId, PropId, Result, SombraError, TypeId, VRef};

#[cfg(feature = "degree-cache")]
use super::adjacency::DegreeDir;
use super::adjacency::{self, Dir, ExpandOpts, Neighbor, NeighborCursor};
use super::edge::{self, PropPayload as EdgePropPayload, PropStorage as EdgePropStorage};
use super::node::{self, PropPayload as NodePropPayload, PropStorage as NodePropStorage};
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

pub const DEFAULT_INLINE_PROP_BLOB: u32 = 128;
pub const DEFAULT_INLINE_PROP_VALUE: u32 = 48;
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

#[allow(dead_code)]
pub struct Graph {
    store: Arc<dyn PageStore>,
    nodes: BTree<u64, Vec<u8>>,
    edges: BTree<u64, Vec<u8>>,
    adj_fwd: BTree<Vec<u8>, UnitValue>,
    adj_rev: BTree<Vec<u8>, UnitValue>,
    #[cfg(feature = "degree-cache")]
    degree: Option<BTree<Vec<u8>, u64>>,
    vstore: VStore,
    indexes: IndexStore,
    inline_prop_blob: usize,
    inline_prop_value: usize,
    #[cfg(feature = "degree-cache")]
    degree_cache_enabled: bool,
    next_node_id: AtomicU64,
    next_edge_id: AtomicU64,
    storage_flags: u32,
    metrics: Arc<dyn super::metrics::StorageMetrics>,
    distinct_neighbors_default: bool,
}

impl Graph {
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
        let mut degree_cache_enabled = opts.degree_cache
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
        let next_node_id = AtomicU64::new(next_node_id_init);
        let next_edge_id = AtomicU64::new(next_edge_id_init);

        Ok(Self {
            store,
            nodes,
            edges,
            adj_fwd,
            adj_rev,
            #[cfg(feature = "degree-cache")]
            degree: degree_tree,
            vstore,
            indexes,
            inline_prop_blob,
            inline_prop_value,
            #[cfg(feature = "degree-cache")]
            degree_cache_enabled,
            next_node_id,
            next_edge_id,
            storage_flags,
            metrics: opts
                .metrics
                .unwrap_or_else(|| super::metrics::default_metrics()),
            distinct_neighbors_default: opts.distinct_neighbors_default,
        })
    }

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
        let row_bytes = match node::encode(&labels, payload) {
            Ok(bytes) => bytes,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &spill_vrefs);
                return Err(err);
            }
        };
        let id_raw = self.next_node_id.fetch_add(1, Ordering::SeqCst);
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
        if let Err(err) = self.indexes.insert_node_labels(tx, node_id, &labels) {
            let _ = self.nodes.delete(tx, &node_id.0);
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        if let Err(err) = self.insert_indexed_props(tx, node_id, &labels, &prop_owned) {
            let _ = self.indexes.remove_node_labels(tx, node_id, &labels);
            let _ = self.nodes.delete(tx, &node_id.0);
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.metrics.node_created();
        Ok(node_id)
    }

    pub fn create_label_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        if self.indexes.has_label_index_with_write(tx, label)? {
            return Ok(());
        }
        let mut nodes = Vec::new();
        self.nodes.for_each_with_write(tx, |id_raw, bytes| {
            let row = node::decode(&bytes)?;
            if row.labels.binary_search(&label).is_ok() {
                nodes.push(NodeId(id_raw));
            }
            Ok(())
        })?;
        self.indexes.create_label_index(tx, label, nodes)
    }

    pub fn drop_label_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        self.indexes.drop_label_index(tx, label)
    }

    pub fn has_label_index(&self, label: LabelId) -> Result<bool> {
        self.indexes.has_label_index(label)
    }

    pub fn create_property_index(&self, tx: &mut WriteGuard<'_>, def: IndexDef) -> Result<()> {
        let existing = self
            .indexes
            .property_indexes_for_label_with_write(tx, def.label)?;
        if existing.iter().any(|entry| entry.prop == def.prop) {
            return Ok(());
        }
        let mut entries: Vec<(Vec<u8>, NodeId)> = Vec::new();
        self.nodes.for_each_with_write(tx, |id_raw, bytes| {
            let row = node::decode(&bytes)?;
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
        self.indexes.create_property_index(tx, def, &entries)
    }

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
        self.indexes.drop_property_index(tx, def)
    }

    pub fn has_property_index(&self, label: LabelId, prop: PropId) -> Result<bool> {
        let read = self.store.begin_read()?;
        Ok(self
            .indexes
            .get_property_index(&read, label, prop)?
            .is_some())
    }

    pub fn index_catalog_root(&self) -> PageId {
        self.indexes.catalog().tree().root_page()
    }

    pub fn property_index(&self, label: LabelId, prop: PropId) -> Result<Option<IndexDef>> {
        let read = self.store.begin_read()?;
        self.indexes.get_property_index(&read, label, prop)
    }

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

    pub fn label_scan<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
    ) -> Result<Option<LabelScan<'a>>> {
        self.indexes.label_scan(tx, label)
    }

    pub fn get_node(&self, tx: &ReadGuard, id: NodeId) -> Result<Option<NodeData>> {
        let Some(bytes) = self.nodes.get(tx, &id.0)? else {
            return Ok(None);
        };
        let row = node::decode(&bytes)?;
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

    pub fn scan_all_nodes(&self, tx: &ReadGuard) -> Result<Vec<(NodeId, NodeData)>> {
        let mut cursor = self.nodes.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut rows = Vec::new();
        while let Some((key, bytes)) = cursor.next()? {
            let row = node::decode(&bytes)?;
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

    pub fn delete_node(
        &self,
        tx: &mut WriteGuard<'_>,
        id: NodeId,
        opts: DeleteNodeOpts,
    ) -> Result<()> {
        let Some(bytes) = self.nodes.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let row = node::decode(&bytes)?;
        let read = self.store.begin_read()?;
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

        let prop_bytes = self.read_node_prop_bytes(&row.props)?;
        let prop_values = self.materialize_props_owned(&prop_bytes)?;
        let prop_map: BTreeMap<PropId, PropValueOwned> = prop_values.into_iter().collect();
        self.remove_indexed_props(tx, id, &row.labels, &prop_map)?;
        self.indexes.remove_node_labels(tx, id, &row.labels)?;
        self.free_node_props(tx, row.props)?;
        let removed = self.nodes.delete(tx, &id.0)?;
        if !removed {
            return Err(SombraError::Corruption("node missing during delete"));
        }
        self.metrics.node_deleted();
        Ok(())
    }

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
        let node::NodeRow {
            labels,
            props: storage,
        } = node::decode(&existing_bytes)?;
        let prop_bytes = self.read_node_prop_bytes(&storage)?;
        let current = self.materialize_props_owned(&prop_bytes)?;
        let mut map: BTreeMap<PropId, PropValueOwned> = current.into_iter().collect();
        let old_map = map.clone();
        apply_patch_ops(&mut map, &patch.ops);

        let ordered: Vec<(PropId, PropValueOwned)> = map.into_iter().collect();
        let encoded =
            props::encode_props_owned(&ordered, self.inline_prop_value, &self.vstore, tx)?;
        let mut map_vref: Option<VRef> = None;
        let payload = if encoded.bytes.len() <= self.inline_prop_blob {
            NodePropPayload::Inline(&encoded.bytes)
        } else {
            let vref = self.vstore.write(tx, &encoded.bytes)?;
            map_vref = Some(vref);
            NodePropPayload::VRef(vref)
        };
        let new_bytes = match node::encode(&labels, payload) {
            Ok(bytes) => bytes,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &encoded.spill_vrefs);
                return Err(err);
            }
        };
        if let Err(err) = self.nodes.put(tx, &id.0, &new_bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &encoded.spill_vrefs);
            return Err(err);
        }
        let new_map: BTreeMap<PropId, PropValueOwned> = ordered.into_iter().collect();
        self.update_indexed_props_for_node(tx, id, &labels, &old_map, &new_map)?;
        self.free_node_props(tx, storage)
    }

    pub fn create_edge(&self, tx: &mut WriteGuard<'_>, spec: EdgeSpec<'_>) -> Result<EdgeId> {
        self.ensure_node_exists(tx, spec.src, "edge source node missing")?;
        self.ensure_node_exists(tx, spec.dst, "edge destination node missing")?;
        let (prop_bytes, spill_vrefs) = self.encode_property_map(tx, spec.props)?;
        let mut map_vref: Option<VRef> = None;
        let payload = if prop_bytes.len() <= self.inline_prop_blob {
            EdgePropPayload::Inline(&prop_bytes)
        } else {
            let vref = self.vstore.write(tx, &prop_bytes)?;
            map_vref = Some(vref);
            EdgePropPayload::VRef(vref)
        };
        let row_bytes = match edge::encode(spec.src, spec.dst, spec.ty, payload) {
            Ok(bytes) => bytes,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &spill_vrefs);
                return Err(err);
            }
        };
        let id_raw = self.next_edge_id.fetch_add(1, Ordering::SeqCst);
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
        if let Err(err) = self.insert_adjacency(tx, spec.src, spec.dst, spec.ty, edge_id) {
            let _ = self.edges.delete(tx, &edge_id.0);
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.metrics.edge_created();
        Ok(edge_id)
    }

    pub fn get_edge(&self, tx: &ReadGuard, id: EdgeId) -> Result<Option<EdgeData>> {
        let Some(bytes) = self.edges.get(tx, &id.0)? else {
            return Ok(None);
        };
        let row = edge::decode(&bytes)?;
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

    pub fn scan_all_edges(&self, tx: &ReadGuard) -> Result<Vec<(EdgeId, EdgeData)>> {
        let mut cursor = self.edges.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut rows = Vec::new();
        while let Some((key, bytes)) = cursor.next()? {
            let row = edge::decode(&bytes)?;
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
        let edge::EdgeRow {
            src,
            dst,
            ty,
            props: storage,
        } = edge::decode(&existing_bytes)?;
        let prop_bytes = self.read_edge_prop_bytes(&storage)?;
        let current = self.materialize_props_owned(&prop_bytes)?;
        let mut map: BTreeMap<PropId, PropValueOwned> = current.into_iter().collect();
        apply_patch_ops(&mut map, &patch.ops);

        let ordered: Vec<(PropId, PropValueOwned)> = map.into_iter().collect();
        let encoded =
            props::encode_props_owned(&ordered, self.inline_prop_value, &self.vstore, tx)?;
        let mut map_vref: Option<VRef> = None;
        let payload = if encoded.bytes.len() <= self.inline_prop_blob {
            EdgePropPayload::Inline(&encoded.bytes)
        } else {
            let vref = self.vstore.write(tx, &encoded.bytes)?;
            map_vref = Some(vref);
            EdgePropPayload::VRef(vref)
        };
        let new_bytes = match edge::encode(src, dst, ty, payload) {
            Ok(bytes) => bytes,
            Err(err) => {
                if let Some(vref) = map_vref.take() {
                    let _ = self.vstore.free(tx, vref);
                }
                props::free_vrefs(&self.vstore, tx, &encoded.spill_vrefs);
                return Err(err);
            }
        };

        if let Err(err) = self.edges.put(tx, &id.0, &new_bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &encoded.spill_vrefs);
            return Err(err);
        }
        self.free_edge_props(tx, storage)?;
        self.metrics.edge_deleted();
        Ok(())
    }

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

    pub fn delete_edge(&self, tx: &mut WriteGuard<'_>, id: EdgeId) -> Result<()> {
        let Some(bytes) = self.edges.get_with_write(tx, &id.0)? else {
            return Err(SombraError::NotFound);
        };
        let row = edge::decode(&bytes)?;
        self.remove_adjacency(tx, row.src, row.dst, row.ty, id)?;
        let removed = self.edges.delete(tx, &id.0)?;
        if !removed {
            return Err(SombraError::Corruption("edge missing during delete"));
        }
        self.free_edge_props(tx, row.props)
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
        if self.nodes.get_with_write(tx, &node.0)?.is_some() {
            Ok(())
        } else {
            Err(SombraError::Invalid(context))
        }
    }

    fn insert_adjacency(
        &self,
        tx: &mut WriteGuard<'_>,
        src: NodeId,
        dst: NodeId,
        ty: TypeId,
        edge: EdgeId,
    ) -> Result<()> {
        let fwd_key = adjacency::encode_fwd_key(src, ty, dst, edge);
        let rev_key = adjacency::encode_rev_key(dst, ty, src, edge);
        if let Err(err) = self.adj_fwd.put(tx, &fwd_key, &UnitValue) {
            return Err(err);
        }
        if let Err(err) = self.adj_rev.put(tx, &rev_key, &UnitValue) {
            let _ = self.adj_fwd.delete(tx, &fwd_key);
            return Err(err);
        }
        #[cfg(feature = "degree-cache")]
        if self.degree_cache_enabled {
            if let Err(err) = self.bump_degree(tx, src, DegreeDir::Out, ty, 1) {
                let _ = self.adj_rev.delete(tx, &rev_key);
                let _ = self.adj_fwd.delete(tx, &fwd_key);
                return Err(err);
            }
            if let Err(err) = self.bump_degree(tx, dst, DegreeDir::In, ty, 1) {
                let _ = self.bump_degree(tx, src, DegreeDir::Out, ty, -1);
                let _ = self.adj_rev.delete(tx, &rev_key);
                let _ = self.adj_fwd.delete(tx, &fwd_key);
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
        let removed_fwd = self.adj_fwd.delete(tx, &fwd_key)?;
        if !removed_fwd {
            return Err(SombraError::Corruption("missing forward adjacency entry"));
        }
        let removed_rev = self.adj_rev.delete(tx, &rev_key)?;
        if !removed_rev {
            return Err(SombraError::Corruption("missing reverse adjacency entry"));
        }
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
        let mut cursor = tree.range(read, Bound::Included(lo), Bound::Included(hi))?;
        while let Some((key, _)) = cursor.next()? {
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
        let mut cursor = tree.range(tx, Bound::Included(lo), Bound::Included(hi))?;
        let mut seen = seen;
        while let Some((key, _)) = cursor.next()? {
            if forward {
                let (src, ty, dst, edge) = adjacency::decode_fwd_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                debug_assert_eq!(src, node);
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
                let read = self.store.begin_read()?;
                let bytes = self.vstore.read(&read, vref)?;
                drop(read);
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
                let read = self.store.begin_read()?;
                let bytes = self.vstore.read(&read, vref)?;
                drop(read);
                self.free_prop_values_from_bytes(tx, &bytes)?;
                self.vstore.free(tx, vref)
            }
        }
    }

    fn read_node_prop_bytes(&self, storage: &NodePropStorage) -> Result<Vec<u8>> {
        match storage {
            NodePropStorage::Inline(bytes) => Ok(bytes.clone()),
            NodePropStorage::VRef(vref) => {
                let read = self.store.begin_read()?;
                self.vstore.read(&read, *vref)
            }
        }
    }

    fn read_edge_prop_bytes(&self, storage: &EdgePropStorage) -> Result<Vec<u8>> {
        match storage {
            EdgePropStorage::Inline(bytes) => Ok(bytes.clone()),
            EdgePropStorage::VRef(vref) => {
                let read = self.store.begin_read()?;
                self.vstore.read(&read, *vref)
            }
        }
    }

    fn materialize_props_owned(&self, bytes: &[u8]) -> Result<Vec<(PropId, PropValueOwned)>> {
        let raw = props::decode_raw(bytes)?;
        let read = self.store.begin_read()?;
        let props = props::materialize_props(&raw, &self.vstore, &read)?;
        Ok(props)
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
        Ok(())
    }
}

fn open_u64_vec_tree(store: &Arc<dyn PageStore>, root: PageId) -> Result<BTree<u64, Vec<u8>>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

fn open_unit_tree(store: &Arc<dyn PageStore>, root: PageId) -> Result<BTree<Vec<u8>, UnitValue>> {
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
    pub fn debug_collect_adj_fwd(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, TypeId, NodeId, EdgeId)>> {
        let mut cursor = self.adj_fwd.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut entries = Vec::new();
        while let Some((key, _)) = cursor.next()? {
            let decoded =
                adjacency::decode_fwd_key(&key).ok_or(SombraError::Corruption("adj key decode"))?;
            entries.push(decoded);
        }
        Ok(entries)
    }

    pub fn debug_collect_adj_rev(
        &self,
        tx: &ReadGuard,
    ) -> Result<Vec<(NodeId, TypeId, NodeId, EdgeId)>> {
        let mut cursor = self.adj_rev.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut entries = Vec::new();
        while let Some((key, _)) = cursor.next()? {
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
            let defs = self
                .indexes
                .property_indexes_for_label_with_write(tx, *label)?;
            for def in defs {
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
            let defs = self
                .indexes
                .property_indexes_for_label_with_write(tx, *label)?;
            for def in defs {
                if let Some(value) = props.get(&def.prop) {
                    let key = encode_value_key_owned(def.ty, value)?;
                    self.indexes.remove_property_value(tx, &def, &key, node)?;
                }
            }
        }
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
            let defs = self
                .indexes
                .property_indexes_for_label_with_write(tx, *label)?;
            for def in defs {
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

    #[cfg(feature = "degree-cache")]
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
    pub fn validate_degree_cache(&self, tx: &ReadGuard) -> Result<()> {
        let Some(tree) = &self.degree else {
            return Ok(());
        };
        let mut actual: HashMap<(NodeId, adjacency::DegreeDir, TypeId), u64> = HashMap::new();
        {
            let mut cursor = self.adj_fwd.range(tx, Bound::Unbounded, Bound::Unbounded)?;
            while let Some((key, _)) = cursor.next()? {
                let (src, ty, _, _) = adjacency::decode_fwd_key(&key)
                    .ok_or(SombraError::Corruption("adjacency key decode failed"))?;
                *actual
                    .entry((src, adjacency::DegreeDir::Out, ty))
                    .or_insert(0) += 1;
            }
        }
        {
            let mut cursor = self.adj_rev.range(tx, Bound::Unbounded, Bound::Unbounded)?;
            while let Some((key, _)) = cursor.next()? {
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

    #[cfg(all(test, feature = "degree-cache"))]
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
}
