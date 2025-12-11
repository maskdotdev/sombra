use std::cmp::{Ordering as CmpOrdering, Ordering};
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::sync::Arc;

use crate::primitives::pager::{ReadGuard, WriteGuard};
use crate::storage::btree::BTree;
use crate::storage::index::{
    collect_all, CatalogEpoch, GraphIndexCache, GraphIndexCacheStats, IndexDef, IndexKind,
    IndexRoots, IndexStore, LabelScan, PostingStream,
};
use crate::storage::mvcc::{CommitId, VersionHeader, VersionPtr, VersionSpace, VersionedValue};
use crate::storage::mvcc_flags;
use crate::storage::props;
use crate::types::{EdgeId, LabelId, NodeId, PageId, PropId, Result, SombraError, TypeId};
use crate::storage::PropValueOwned;

use super::adjacency;
use super::graph_types::{PropStats, RootKind};
use super::prop_ops::{
    clone_owned_bound, encode_range_bound, encode_value_key_owned, prop_stats_key,
    prop_value_to_owned, update_min_max,
};
use super::{Graph, GraphTxnState, UnitValue};

use crate::storage::{profile_timer, record_flush_adj_entries, record_flush_adj_fwd_put, record_flush_adj_fwd_sort, record_flush_adj_key_encode, record_flush_adj_rev_put, record_flush_adj_rev_sort};
use crate::storage::profile::{
    profile_timer as storage_profile_timer, profiling_enabled as storage_profiling_enabled,
    record_profile_timer as record_storage_profile_timer, StorageProfileKind,
};

impl Graph {
    pub fn create_label_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        if self.indexes.has_label_index_with_write(tx, label)? {
            return Ok(());
        }
        let mut nodes = Vec::new();
        self.nodes.for_each_with_write(tx, |id_raw, bytes| {
            let versioned = crate::storage::node::decode(&bytes)?;
            if versioned.header.is_tombstone() {
                return Ok(());
            }
            if versioned.row.labels.binary_search(&label).is_ok() {
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
            let versioned = crate::storage::node::decode(&bytes)?;
            if versioned.header.is_tombstone() {
                return Ok(());
            }
            if versioned.row.labels.binary_search(&def.label).is_err() {
                return Ok(());
            }
            let prop_bytes = self.read_node_prop_bytes(&versioned.row.props)?;
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
        let read = self.lease_latest_snapshot()?;
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
        let read = self.lease_latest_snapshot()?;
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
        let def = self
            .indexes
            .get_property_index(tx, label, prop)?
            .ok_or(SombraError::Invalid("property index not found"))?;
        self.indexes
            .scan_property_range(tx, &def, Bound::Unbounded, Bound::Unbounded)
    }

    /// Returns all property index definitions currently registered.
    pub fn all_property_indexes(&self) -> Result<Vec<IndexDef>> {
        let read = self.lease_latest_snapshot()?;
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
        let filtered = PropertyFilterStream::new_eq(self, tx, stream, label, prop, value.clone());
        Ok(instrument_posting_stream(filtered))
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
        let filtered = PropertyFilterStream::new_range(
            self,
            tx,
            stream,
            label,
            prop,
            clone_owned_bound(start),
            clone_owned_bound(end),
        );
        Ok(instrument_posting_stream(filtered))
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

    /// Counts how many nodes exist with the specified label.
    pub fn count_nodes_with_label(&self, tx: &ReadGuard, label: LabelId) -> Result<u64> {
        let mut stream = self.label_scan_stream(tx, label)?;
        const BATCH: usize = 256;
        let mut buf = Vec::with_capacity(BATCH);
        let mut count = 0u64;
        loop {
            buf.clear();
            let has_more = stream.next_batch(&mut buf, BATCH)?;
            for node in &buf {
                if self.node_has_label(tx, *node, label)? {
                    count = count.saturating_add(1);
                }
            }
            if !has_more {
                break;
            }
        }
        Ok(count)
    }

    /// Returns all node identifiers that carry the provided label.
    pub fn nodes_with_label(&self, tx: &ReadGuard, label: LabelId) -> Result<Vec<NodeId>> {
        let mut stream = self.label_scan_stream(tx, label)?;
        const BATCH: usize = 256;
        let mut buf = Vec::with_capacity(BATCH);
        let mut nodes = Vec::new();
        loop {
            buf.clear();
            let has_more = stream.next_batch(&mut buf, BATCH)?;
            for node in &buf {
                if self.node_has_label(tx, *node, label)? {
                    nodes.push(*node);
                }
            }
            if !has_more {
                break;
            }
        }
        Ok(nodes)
    }

    /// Counts the number of edges that have the provided type.
    pub fn count_edges_with_type(&self, tx: &ReadGuard, ty: TypeId) -> Result<u64> {
        let mut cursor = self.edges.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut count = 0u64;
        while let Some((key, bytes)) = cursor.next()? {
            let Some(versioned) = self.visible_edge_from_bytes(tx, EdgeId(key), &bytes)? else {
                continue;
            };
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
        while let Some((_key, bytes)) = cursor.next()? {
            if let Some(versioned) = self.visible_node_from_bytes(tx, NodeId(_key), &bytes)? {
                labels.push(versioned.row.labels);
            }
            if labels.len() >= limit {
                break;
            }
        }
        Ok(labels)
    }

    fn build_fallback_label_scan(
        &self,
        tx: &ReadGuard,
        label: LabelId,
    ) -> Result<FallbackLabelScan> {
        let mut cursor = self.nodes.range(tx, Bound::Unbounded, Bound::Unbounded)?;
        let mut nodes = Vec::new();
        while let Some((key, bytes)) = cursor.next()? {
            let Some(versioned) = self.visible_node_from_bytes(tx, NodeId(key), &bytes)? else {
                continue;
            };
            let row = versioned.row;
            if row.labels.binary_search(&label).is_ok() {
                nodes.push(NodeId(key));
            }
        }
        Ok(FallbackLabelScan { nodes, pos: 0 })
    }

    pub(crate) fn insert_indexed_props(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        props: &BTreeMap<PropId, PropValueOwned>,
        commit: CommitId,
    ) -> Result<()> {
        for label in labels {
            let defs = self.index_defs_for_label(tx, *label)?;
            for def in defs.iter() {
                if let Some(value) = props.get(&def.prop) {
                    let key = encode_value_key_owned(def.ty, value)?;
                    self.stage_prop_index_op(tx, *def, key, node, commit, true)?;
                }
            }
        }
        Ok(())
    }

    pub(crate) fn update_indexed_props_for_node(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        labels: &[LabelId],
        old_props: &BTreeMap<PropId, PropValueOwned>,
        new_props: &BTreeMap<PropId, PropValueOwned>,
        commit: CommitId,
    ) -> Result<()> {
        for label in labels {
            let defs = self.index_defs_for_label(tx, *label)?;
            if self.defer_index_flush {
                for def in defs.iter() {
                    let old = old_props.get(&def.prop);
                    let new = new_props.get(&def.prop);
                    match (old, new) {
                        (_, Some(value)) => {
                            let key = encode_value_key_owned(def.ty, value)?;
                            self.stage_prop_index_op(tx, *def, key, node, commit, true)?;
                        }
                        (Some(prev), None) => {
                            let key = encode_value_key_owned(def.ty, prev)?;
                            self.stage_prop_index_op(tx, *def, key, node, commit, false)?;
                        }
                        _ => {}
                    };
                }
                continue;
            }
            for def in defs.iter() {
                let old = old_props.get(&def.prop);
                let new = new_props.get(&def.prop);
                match (old, new) {
                    (_, Some(value)) => {
                        let key = encode_value_key_owned(def.ty, value)?;
                        self.indexes.insert_property_value_with_commit(
                            tx,
                            def,
                            &key,
                            node,
                            Some(commit),
                        )?;
                    }
                    (Some(prev), None) => {
                        let key = encode_value_key_owned(def.ty, prev)?;
                        self.indexes.remove_property_value_with_commit(
                            tx,
                            def,
                            &key,
                            node,
                            Some(commit),
                        )?;
                    }
                    _ => {}
                };
            }
        }
        Ok(())
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

enum PropertyPredicate {
    Eq(PropValueOwned),
    Range {
        start: Bound<PropValueOwned>,
        end: Bound<PropValueOwned>,
    },
}

struct PropertyFilterStream<'a> {
    graph: &'a Graph,
    tx: &'a ReadGuard,
    inner: Box<dyn PostingStream + 'a>,
    label: LabelId,
    prop: PropId,
    predicate: PropertyPredicate,
    pending: VecDeque<NodeId>,
    scratch: Vec<NodeId>,
    inner_exhausted: bool,
}

impl<'a> PropertyFilterStream<'a> {
    fn new_eq(
        graph: &'a Graph,
        tx: &'a ReadGuard,
        inner: Box<dyn PostingStream + 'a>,
        label: LabelId,
        prop: PropId,
        value: PropValueOwned,
    ) -> Box<dyn PostingStream + 'a> {
        Box::new(Self {
            graph,
            tx,
            inner,
            label,
            prop,
            predicate: PropertyPredicate::Eq(value),
            pending: VecDeque::new(),
            scratch: Vec::new(),
            inner_exhausted: false,
        })
    }

    fn new_range(
        graph: &'a Graph,
        tx: &'a ReadGuard,
        inner: Box<dyn PostingStream + 'a>,
        label: LabelId,
        prop: PropId,
        start: Bound<PropValueOwned>,
        end: Bound<PropValueOwned>,
    ) -> Box<dyn PostingStream + 'a> {
        Box::new(Self {
            graph,
            tx,
            inner,
            label,
            prop,
            predicate: PropertyPredicate::Range { start, end },
            pending: VecDeque::new(),
            scratch: Vec::new(),
            inner_exhausted: false,
        })
    }

    fn fill_pending(&mut self, max: usize) -> Result<()> {
        self.scratch.clear();
        let has_more = self.inner.next_batch(&mut self.scratch, max)?;
        self.inner_exhausted = !has_more;
        for node in &self.scratch {
            if self.node_matches(*node)? {
                self.pending.push_back(*node);
            }
        }
        Ok(())
    }

    fn node_matches(&self, node: NodeId) -> Result<bool> {
        match &self.predicate {
            PropertyPredicate::Eq(expected) => self
                .graph
                .node_matches_property_eq(self.tx, node, self.label, self.prop, expected),
            PropertyPredicate::Range { start, end } => self
                .graph
                .node_matches_property_range(self.tx, node, self.label, self.prop, start, end),
        }
    }
}

impl PostingStream for PropertyFilterStream<'_> {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        if max == 0 {
            if !self.pending.is_empty() {
                return Ok(true);
            }
            if self.inner_exhausted {
                return Ok(false);
            }
            self.fill_pending(0)?;
            return Ok(!self.pending.is_empty() || !self.inner_exhausted);
        }
        while out.len() < max {
            if let Some(node) = self.pending.pop_front() {
                out.push(node);
                continue;
            }
            if self.inner_exhausted {
                break;
            }
            self.fill_pending(max)?;
            if self.pending.is_empty() && self.inner_exhausted {
                break;
            }
        }
        Ok(!self.pending.is_empty() || !self.inner_exhausted)
    }
}

struct FallbackLabelScan {
    nodes: Vec<NodeId>,
    pos: usize,
}

impl PostingStream for FallbackLabelScan {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        if max == 0 {
            // Return true if more data remains, false if exhausted
            return Ok(self.pos < self.nodes.len());
        }
        let mut produced = 0;
        while self.pos < self.nodes.len() && produced < max {
            out.push(self.nodes[self.pos]);
            self.pos += 1;
            produced += 1;
        }
        // Return true if more data remains, false if exhausted
        Ok(self.pos < self.nodes.len())
    }
}
