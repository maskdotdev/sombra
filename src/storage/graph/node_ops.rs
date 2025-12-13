use std::collections::BTreeMap;

use super::node::{
    self, EncodeOpts as NodeEncodeOpts, PropPayload as NodePropPayload,
    PropStorage as NodePropStorage,
};
use super::Graph;
use crate::primitives::pager::{ReadGuard, WriteGuard};
use crate::storage::graph::RootKind;
use crate::storage::mvcc::{VersionHeader, VERSION_HEADER_LEN};
use crate::storage::patch;
use crate::storage::{props, DeleteMode, DeleteNodeOpts, NodeData, NodeSpec, PropValueOwned};
use crate::storage::{VersionPtr, VersionSpace};
use crate::types::{EdgeId, LabelId, NodeId, PropId, Result, SombraError, VRef};

use crate::storage::profile::{
    profile_timer as storage_profile_timer, record_profile_timer as record_storage_profile_timer,
    StorageProfileKind,
};

impl Graph {
    /// Creates a new node in the graph with the given specification.
    pub fn create_node(&self, tx: &mut WriteGuard<'_>, spec: NodeSpec<'_>) -> Result<NodeId> {
        let total_start = storage_profile_timer();
        let labels = super::helpers::normalize_labels(spec.labels)?;
        let mut prop_owned: BTreeMap<PropId, PropValueOwned> = BTreeMap::new();
        for entry in spec.props {
            let owned = super::prop_ops::prop_value_to_owned(entry.value.clone());
            prop_owned.insert(entry.prop, owned);
        }
        // Encode properties (profiled)
        let encode_start = storage_profile_timer();
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
        // When using deferred index flush, skip the pending flag since visibility
        // is already controlled by commit boundaries (readers can't see uncommitted data).
        // This avoids an expensive finalize_node_head re-write.
        let (commit_id, version) = if self.defer_index_flush {
            self.tx_version_header(tx)
        } else {
            self.tx_pending_version_header(tx)
        };
        let row_bytes = match node::encode(
            &labels,
            payload,
            NodeEncodeOpts::new(self.row_hash_header),
            version,
            VersionPtr::null(),
            None,
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
        record_storage_profile_timer(StorageProfileKind::CreateNodeEncodeProps, encode_start);
        let id_raw = self
            .next_node_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let node_id = NodeId(id_raw);
        let next_id = node_id.0.saturating_add(1);
        tx.update_meta(|meta| {
            if meta.storage_next_node_id <= node_id.0 {
                meta.storage_next_node_id = next_id;
            }
        })?;
        // BTree insert (profiled)
        let btree_start = storage_profile_timer();
        if let Err(err) = self.nodes.put(tx, &node_id.0, &row_bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Nodes)?;
        record_storage_profile_timer(StorageProfileKind::CreateNodeBTree, btree_start);
        // Label index update (profiled)
        let label_index_start = storage_profile_timer();
        if let Err(err) = self.stage_label_inserts(tx, node_id, &labels, commit_id) {
            let _ = self.nodes.delete(tx, &node_id.0);
            self.persist_tree_root(tx, RootKind::Nodes)?;
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        record_storage_profile_timer(StorageProfileKind::CreateNodeLabelIndex, label_index_start);
        // Property index update (profiled)
        let prop_index_start = storage_profile_timer();
        if let Err(err) = self.insert_indexed_props(tx, node_id, &labels, &prop_owned, commit_id) {
            let _ = self.indexes.remove_node_labels(tx, node_id, &labels);
            let _ = self.nodes.delete(tx, &node_id.0);
            self.persist_tree_root(tx, RootKind::Nodes)?;
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        record_storage_profile_timer(StorageProfileKind::CreateNodePropIndex, prop_index_start);
        // Skip finalize when using deferred index flush (we wrote without pending flag)
        if !self.defer_index_flush {
            self.finalize_node_head(tx, node_id)?;
        }
        self.metrics.node_created();
        record_storage_profile_timer(StorageProfileKind::CreateNode, total_start);
        Ok(node_id)
    }

    /// Retrieves node data by ID.
    pub fn get_node(&self, tx: &ReadGuard, id: NodeId) -> Result<Option<NodeData>> {
        let Some(bytes) = self.nodes.get(tx, &id.0)? else {
            return Ok(None);
        };
        let Some(versioned) = self.visible_node_from_bytes(tx, id, &bytes)? else {
            return Ok(None);
        };
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

    /// Retrieves the number of properties for a node without materializing values.
    pub fn get_node_prop_count(
        &self,
        tx: &ReadGuard,
        id: NodeId,
    ) -> Result<Option<usize>> {
        let Some(bytes) = self.nodes.get(tx, &id.0)? else {
            return Ok(None);
        };
        let Some(versioned) = self.visible_node_from_bytes(tx, id, &bytes)? else {
            return Ok(None);
        };
        let row = versioned.row;
        let prop_bytes = match row.props {
            NodePropStorage::Inline(bytes) => bytes,
            NodePropStorage::VRef(vref) => self.vstore.read(tx, vref)?,
        };
        let count = props::decode_prop_count(&prop_bytes)?;
        Ok(Some(count))
    }

    /// Retrieves node data using an active write transaction.
    ///
    /// This surfaces pending versions created by the current writer so that
    /// write transactions can read their own uncommitted changes.
    pub fn get_node_in_write(
        &self,
        tx: &mut WriteGuard<'_>,
        id: NodeId,
    ) -> Result<Option<NodeData>> {
        let Some(bytes) = self.nodes.get_with_write(tx, &id.0)? else {
            return Ok(None);
        };
        let versioned = node::decode(&bytes)?;
        if versioned.header.is_tombstone() {
            return Ok(None);
        }
        let row = versioned.row;
        let prop_bytes = self.read_node_prop_bytes_with_write(tx, &row.props)?;
        let props = self.materialize_props_owned_with_write(tx, &prop_bytes)?;
        Ok(Some(NodeData {
            labels: row.labels,
            props,
        }))
    }

    /// Scans and returns all nodes in the graph.
    pub fn scan_all_nodes(&self, tx: &ReadGuard) -> Result<Vec<(NodeId, NodeData)>> {
        let mut cursor =
            self.nodes
                .range(tx, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;
        let mut rows = Vec::new();
        while let Some((key, bytes)) = cursor.next()? {
            let Some(versioned) = self.visible_node_from_bytes(tx, NodeId(key), &bytes)? else {
                continue;
            };
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
        let prop_bytes = self.read_node_prop_bytes_with_write(tx, &row.props)?;
        let old_props_vec = self.materialize_props_owned_with_write(tx, &prop_bytes)?;
        let old_props: BTreeMap<PropId, PropValueOwned> = old_props_vec.into_iter().collect();
        let read = self.lease_latest_snapshot()?;
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

        let (commit_id, mut tombstone_header) = self.tx_pending_version_header(tx);
        self.stage_label_removals(tx, id, &row.labels, commit_id)?;
        let empty_props = BTreeMap::new();
        self.update_indexed_props_for_node(
            tx,
            id,
            &row.labels,
            &old_props,
            &empty_props,
            commit_id,
        )?;
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            tx,
            VersionSpace::Node,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes.clone(),
        )?;
        let inline_history = self.maybe_inline_history(&log_bytes);
        tombstone_header.flags |= crate::storage::mvcc_flags::TOMBSTONE;
        if inline_history.is_some() {
            tombstone_header.flags |= super::mvcc_flags::INLINE_HISTORY;
        }
        let encoded = node::encode(
            &[],
            node::PropPayload::Inline(&[]),
            NodeEncodeOpts::new(false),
            tombstone_header,
            prev_ptr,
            inline_history.as_deref(),
        )?;
        self.nodes.put(tx, &id.0, &encoded.bytes)?;
        self.persist_tree_root(tx, RootKind::Nodes)?;
        self.finalize_node_head(tx, id)?;
        self.metrics.node_deleted();
        Ok(())
    }

    /// Updates the properties of an existing node by applying the given patch.
    pub fn update_node(
        &self,
        tx: &mut WriteGuard<'_>,
        id: NodeId,
        patch: patch::PropPatch<'_>,
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
            adj_page,
            inline_adj,
        } = versioned.row;
        let prop_bytes = self.read_node_prop_bytes_with_write(tx, &storage)?;
        let Some(delta) = self.build_prop_delta(tx, &prop_bytes, &patch)? else {
            return Ok(());
        };
        let (commit_id, new_header) = self.tx_pending_version_header(tx);
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
            tx,
            VersionSpace::Node,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes.clone(),
        )?;
        let inline_history = self.maybe_inline_history(&log_bytes);
        let mut new_header = new_header;
        if inline_history.is_some() {
            new_header.flags |= crate::storage::mvcc_flags::INLINE_HISTORY;
        }
        let mut encode_opts = NodeEncodeOpts::new(self.row_hash_header);
        if let Some(adj) = adj_page {
            encode_opts = encode_opts.with_adj_page(adj);
        }
        if let Some(inline) = inline_adj.as_ref() {
            encode_opts = encode_opts.with_inline_adj(inline);
        }
        let encoded_row = match node::encode(
            &labels,
            payload,
            encode_opts,
            new_header,
            prev_ptr,
            inline_history.as_deref(),
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
        self.update_indexed_props_for_node(
            tx,
            id,
            &labels,
            &delta.old_map,
            &delta.new_map,
            commit_id,
        )?;
        self.finalize_node_head(tx, id)?;
        Ok(())
    }

    fn finalize_node_head(&self, tx: &mut WriteGuard<'_>, id: NodeId) -> Result<()> {
        let Some(mut bytes) = self.nodes.get_with_write(tx, &id.0)? else {
            return Err(SombraError::Corruption("node head missing during finalize"));
        };
        let mut header = VersionHeader::decode(&bytes[..VERSION_HEADER_LEN])?;
        if !self.finalize_version_header(&mut header) {
            return Ok(());
        }
        Self::overwrite_encoded_header(&mut bytes, &header);
        self.nodes.put(tx, &id.0, &bytes)?;
        Ok(())
    }

    pub(crate) fn node_has_label(
        &self,
        tx: &ReadGuard,
        id: NodeId,
        label: LabelId,
    ) -> Result<bool> {
        if let Some(versioned) = self.visible_node(tx, id)? {
            Ok(versioned.row.labels.binary_search(&label).is_ok())
        } else {
            Ok(false)
        }
    }

    pub(crate) fn ensure_node_exists(
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
        Ok(self.visible_node(tx, node)?.is_some())
    }

    pub(crate) fn node_exists_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
    ) -> Result<bool> {
        let Some(bytes) = self.nodes.get_with_write(tx, &node.0)? else {
            return Ok(false);
        };
        let versioned = node::decode(&bytes)?;
        Ok(!versioned.header.is_tombstone() && !versioned.header.is_pending())
    }
}
