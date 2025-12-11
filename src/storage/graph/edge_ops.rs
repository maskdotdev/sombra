use crate::primitives::pager::{ReadGuard, WriteGuard};
use crate::storage::graph::RootKind;
use crate::storage::mvcc::{VersionHeader, VERSION_HEADER_LEN};
use crate::storage::mvcc_flags;
use crate::storage::patch;
use crate::storage::profile::{
    profile_timer as storage_profile_timer, record_profile_timer as record_storage_profile_timer,
    StorageProfileKind,
};
use crate::storage::{props, EdgeData, EdgeSpec};
use crate::storage::{VersionPtr, VersionSpace};
use crate::types::{EdgeId, Result, SombraError, VRef};

use super::edge::{
    self, EncodeOpts as EdgeEncodeOpts, PropPayload as EdgePropPayload,
    PropStorage as EdgePropStorage,
};
use super::Graph;

impl Graph {
    /// Creates a new edge in the graph with the given specification.
    pub fn create_edge(&self, tx: &mut WriteGuard<'_>, spec: EdgeSpec<'_>) -> Result<EdgeId> {
        let total_start = storage_profile_timer();
        self.ensure_node_exists(tx, spec.src, "edge source node missing")?;
        self.ensure_node_exists(tx, spec.dst, "edge destination node missing")?;
        let result = self.insert_edge_unchecked_inner(tx, spec);
        record_storage_profile_timer(StorageProfileKind::CreateEdge, total_start);
        result
    }

    pub(crate) fn insert_edge_unchecked(
        &self,
        tx: &mut WriteGuard<'_>,
        spec: EdgeSpec<'_>,
    ) -> Result<EdgeId> {
        let total_start = storage_profile_timer();
        let result = self.insert_edge_unchecked_inner(tx, spec);
        record_storage_profile_timer(StorageProfileKind::CreateEdge, total_start);
        result
    }

    fn insert_edge_unchecked_inner(
        &self,
        tx: &mut WriteGuard<'_>,
        spec: EdgeSpec<'_>,
    ) -> Result<EdgeId> {
        // Encode properties (profiled)
        let encode_start = storage_profile_timer();
        let (prop_bytes, spill_vrefs) = self.encode_property_map(tx, spec.props)?;
        let mut map_vref: Option<VRef> = None;
        let payload = if prop_bytes.len() <= self.inline_prop_blob {
            EdgePropPayload::Inline(&prop_bytes)
        } else {
            let vref = self.vstore.write(tx, &prop_bytes)?;
            map_vref = Some(vref);
            EdgePropPayload::VRef(vref)
        };
        // When using deferred index flush, skip the pending flag since visibility
        // is already controlled by commit boundaries.
        let (commit_id, version) = if self.defer_index_flush {
            self.tx_version_header(tx)
        } else {
            self.tx_pending_version_header(tx)
        };
        let row_bytes = match edge::encode(
            spec.src,
            spec.dst,
            spec.ty,
            payload,
            EdgeEncodeOpts::new(self.row_hash_header),
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
        record_storage_profile_timer(StorageProfileKind::CreateEdgeEncodeProps, encode_start);
        let id_raw = self
            .next_edge_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let edge_id = EdgeId(id_raw);
        let next_id = edge_id.0.saturating_add(1);
        tx.update_meta(|meta| {
            if meta.storage_next_edge_id <= edge_id.0 {
                meta.storage_next_edge_id = next_id;
            }
        })?;
        // BTree insert (profiled)
        let btree_start = storage_profile_timer();
        if let Err(err) = self.edges.put(tx, &edge_id.0, &row_bytes) {
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        self.persist_tree_root(tx, RootKind::Edges)?;
        record_storage_profile_timer(StorageProfileKind::CreateEdgeBTree, btree_start);
        // Adjacency index update (profiled)
        let adjacency_start = storage_profile_timer();
        if let Err(err) =
            self.stage_adjacency_inserts(tx, &[(spec.src, spec.dst, spec.ty, edge_id)], commit_id)
        {
            let _ = self.edges.delete(tx, &edge_id.0);
            self.persist_tree_root(tx, RootKind::Edges)?;
            if let Some(vref) = map_vref.take() {
                let _ = self.vstore.free(tx, vref);
            }
            props::free_vrefs(&self.vstore, tx, &spill_vrefs);
            return Err(err);
        }
        record_storage_profile_timer(StorageProfileKind::CreateEdgeAdjacency, adjacency_start);
        // Skip finalize when using deferred index flush (we wrote without pending flag)
        if !self.defer_index_flush {
            self.finalize_edge_head(tx, edge_id)?;
        }
        self.metrics.edge_created();
        Ok(edge_id)
    }

    /// Retrieves edge data by ID.
    pub fn get_edge(&self, tx: &ReadGuard, id: EdgeId) -> Result<Option<EdgeData>> {
        let Some(bytes) = self.edges.get(tx, &id.0)? else {
            return Ok(None);
        };
        let Some(versioned) = self.visible_edge_from_bytes(tx, id, &bytes)? else {
            return Ok(None);
        };
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
        let mut cursor =
            self.edges
                .range(tx, std::ops::Bound::Unbounded, std::ops::Bound::Unbounded)?;
        let mut rows = Vec::new();
        while let Some((key, bytes)) = cursor.next()? {
            let Some(versioned) = self.visible_edge_from_bytes(tx, EdgeId(key), &bytes)? else {
                continue;
            };
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
        patch: patch::PropPatch<'_>,
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
        let (commit_id, new_header) = self.tx_pending_version_header(tx);
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
            tx,
            VersionSpace::Edge,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes.clone(),
        )?;
        let inline_history = self.maybe_inline_history(&log_bytes);
        let mut new_header = new_header;
        if inline_history.is_some() {
            new_header.flags |= mvcc_flags::INLINE_HISTORY;
        }
        let encoded_row = match edge::encode(
            src,
            dst,
            ty,
            payload,
            EdgeEncodeOpts::new(self.row_hash_header),
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
        self.finalize_edge_head(tx, id)?;
        Ok(())
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
        let (commit_id, mut tombstone_header) = self.tx_pending_version_header(tx);
        self.stage_adjacency_removals(tx, &[(row.src, row.dst, row.ty, id)], commit_id)?;
        let mut old_header = versioned.header;
        old_header.end = commit_id;
        let mut log_bytes = bytes.clone();
        Self::overwrite_encoded_header(&mut log_bytes, &old_header);
        let prev_ptr = self.log_version_entry(
            tx,
            VersionSpace::Edge,
            id.0,
            old_header,
            versioned.prev_ptr,
            log_bytes.clone(),
        )?;
        tombstone_header.flags |= mvcc_flags::TOMBSTONE;
        let inline_history = self.maybe_inline_history(&log_bytes);
        if inline_history.is_some() {
            tombstone_header.flags |= mvcc_flags::INLINE_HISTORY;
        }
        let encoded = edge::encode(
            row.src,
            row.dst,
            row.ty,
            EdgePropPayload::Inline(&[]),
            EdgeEncodeOpts::new(false),
            tombstone_header,
            prev_ptr,
            inline_history.as_deref(),
        )?;
        self.edges.put(tx, &id.0, &encoded.bytes)?;
        self.persist_tree_root(tx, RootKind::Edges)?;
        self.finalize_edge_head(tx, id)?;
        Ok(())
    }

    fn finalize_edge_head(&self, tx: &mut WriteGuard<'_>, id: EdgeId) -> Result<()> {
        let Some(mut bytes) = self.edges.get_with_write(tx, &id.0)? else {
            return Err(SombraError::Corruption("edge head missing during finalize"));
        };
        let mut header = VersionHeader::decode(&bytes[..VERSION_HEADER_LEN])?;
        if !self.finalize_version_header(&mut header) {
            return Ok(());
        }
        Self::overwrite_encoded_header(&mut bytes, &header);
        self.edges.put(tx, &id.0, &bytes)?;
        Ok(())
    }
}
