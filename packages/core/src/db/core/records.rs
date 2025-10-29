use super::graphdb::GraphDB;
use super::pointer_kind::{EdgePointerKind, PointerKind};
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeId, Node, NULL_EDGE_ID};
use crate::pager::PageId;
use crate::storage::page::RecordPage;
use crate::storage::record::{RecordHeader, RecordKind, RECORD_HEADER_SIZE};
use crate::storage::version_chain::VersionChainReader;
use crate::storage::{
    deserialize_edge, deserialize_node, serialize_edge, serialize_node, RecordPointer, RecordStore,
};
use std::convert::TryFrom;

impl GraphDB {
    pub fn load_edge(&self, edge_id: EdgeId) -> Result<Edge> {
        if let Some(edge) = self.edge_cache.get(&edge_id) {
            return Ok(edge.clone());
        }

        // Phase 4A: Use get_latest() to get the most recent version
        let pointer = self
            .edge_index
            .get_latest(&edge_id)
            .ok_or(GraphError::NotFound("edge"))?;
        let page = self.pager.fetch_page(pointer.page_id)?;
        let mut page_data = page.data.clone();
        let record_page = RecordPage::from_bytes(&mut page_data)?;
        let record = record_page.record_slice(pointer.slot_index as usize)?;
        let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
        if header.kind != RecordKind::Edge {
            return Err(GraphError::Corruption(
                "expected edge record, found other kind".into(),
            ));
        }

        // Check if this is a versioned record by looking at the actual kind byte
        let kind_byte = record[0];
        let is_versioned = kind_byte == 0x03 || kind_byte == 0x04; // VersionedNode or VersionedEdge

        let payload_len = header.payload_length as usize;

        let edge = if is_versioned {
            // For versioned records, skip the 25-byte metadata header
            // Layout: [RecordHeader: 8][VersionMetadata: 25][Payload: N]
            // payload_len includes metadata, so actual data starts at offset 33
            const VERSION_METADATA_SIZE: usize = 25;
            if payload_len < VERSION_METADATA_SIZE {
                return Err(GraphError::Corruption("versioned record too small".into()));
            }
            let data_start = RECORD_HEADER_SIZE + VERSION_METADATA_SIZE;
            let data_end = RECORD_HEADER_SIZE + payload_len;
            let payload = &record[data_start..data_end];
            deserialize_edge(payload)?
        } else {
            // Legacy non-versioned record
            let payload = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
            deserialize_edge(payload)?
        };

        self.edge_cache.put(edge_id, edge.clone());
        Ok(edge)
    }

    /// Load an edge with MVCC snapshot isolation
    ///
    /// This method reads an edge using the provided snapshot timestamp,
    /// ensuring the correct version is returned based on MVCC visibility rules.
    ///
    /// # Arguments
    /// * `edge_id` - The ID of the edge to load
    /// * `snapshot_ts` - Snapshot timestamp for visibility checking
    /// * `current_tx_id` - Optional transaction ID for read-your-own-writes
    ///
    /// # Returns
    /// * `Ok(Edge)` - Edge visible at the snapshot timestamp
    /// * `Err(GraphError::NotFound)` - Edge doesn't exist or is not visible
    /// * `Err(_)` - Error reading the edge
    pub fn load_edge_with_snapshot(
        &self,
        edge_id: EdgeId,
        snapshot_ts: u64,
        current_tx_id: Option<crate::db::TxId>,
    ) -> Result<Edge> {
        // If MVCC is not enabled, fall back to regular load_edge
        if !self.config.mvcc_enabled {
            return self.load_edge(edge_id);
        }

        // Phase 4A: Use get_latest() for head pointer
        let head_pointer = self
            .edge_index
            .get_latest(&edge_id)
            .ok_or(GraphError::NotFound("edge"))?;

        // Use VersionChainReader to find the visible version
        let versioned_record = self.pager.with_pager_write(|pager| {
            let mut record_store = RecordStore::new(pager);
            VersionChainReader::read_version_for_snapshot(
                &mut record_store,
                head_pointer,
                snapshot_ts,
                current_tx_id,
            )
        })?;

        match versioned_record {
            Some(vr) => {
                // Deserialize the edge from the versioned record data
                deserialize_edge(&vr.data)
            }
            None => Err(GraphError::NotFound("edge not visible at snapshot")),
        }
    }

    pub fn load_edges_batch(&self, edge_ids: &[EdgeId]) -> Result<Vec<Edge>> {
        use std::collections::HashMap;

        let mut edges_to_load: HashMap<PageId, Vec<(EdgeId, RecordPointer)>> = HashMap::new();
        let mut loaded_edges: HashMap<EdgeId, Edge> = HashMap::new();

        for &edge_id in edge_ids {
            if let Some(edge) = self.edge_cache.get(&edge_id) {
                loaded_edges.insert(edge_id, edge.clone());
            } else {
                // Phase 4A: Use get_latest() to get most recent version
                let pointer = self
                    .edge_index
                    .get_latest(&edge_id)
                    .ok_or(GraphError::NotFound("edge"))?;
                edges_to_load
                    .entry(pointer.page_id)
                    .or_default()
                    .push((edge_id, pointer));
            }
        }

        for (page_id, edges_on_page) in edges_to_load {
            let page = self.pager.fetch_page(page_id)?;
            let mut page_data = page.data.clone();
            let record_page = RecordPage::from_bytes(&mut page_data)?;

            for (edge_id, pointer) in edges_on_page {
                let record = record_page.record_slice(pointer.slot_index as usize)?;
                let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
                if header.kind != RecordKind::Edge {
                    return Err(GraphError::Corruption(
                        "expected edge record, found other kind".into(),
                    ));
                }

                // Check if this is a versioned record by looking at the actual kind byte
                let kind_byte = record[0];
                let is_versioned = kind_byte == 0x03 || kind_byte == 0x04;

                let payload_len = header.payload_length as usize;

                let edge = if is_versioned {
                    // For versioned records, skip the 25-byte metadata header
                    const VERSION_METADATA_SIZE: usize = 25;
                    if payload_len < VERSION_METADATA_SIZE {
                        return Err(GraphError::Corruption("versioned record too small".into()));
                    }
                    let data_start = RECORD_HEADER_SIZE + VERSION_METADATA_SIZE;
                    let data_end = RECORD_HEADER_SIZE + payload_len;
                    let payload = &record[data_start..data_end];
                    deserialize_edge(payload)?
                } else {
                    // Legacy non-versioned record
                    let payload = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
                    deserialize_edge(payload)?
                };

                self.edge_cache.put(edge_id, edge.clone());
                loaded_edges.insert(edge_id, edge);
            }
        }

        edge_ids
            .iter()
            .map(|&edge_id| {
                loaded_edges
                    .get(&edge_id)
                    .cloned()
                    .ok_or(GraphError::NotFound("edge"))
            })
            .collect()
    }

    pub(crate) fn read_node_at(&self, pointer: RecordPointer) -> Result<Node> {
        let page = self.pager.fetch_page(pointer.page_id)?;
        let mut page_data = page.data.clone();
        let record_page = RecordPage::from_bytes(&mut page_data)?;
        let record = record_page.record_slice(pointer.slot_index as usize)?;
        let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
        if header.kind != RecordKind::Node {
            return Err(GraphError::Corruption(
                "expected node record, found other kind".into(),
            ));
        }

        // Check if this is a versioned record by looking at the actual kind byte
        let kind_byte = record[0];
        let is_versioned = kind_byte == 0x03 || kind_byte == 0x04; // VersionedNode or VersionedEdge

        let payload_len = header.payload_length as usize;

        if is_versioned {
            // For versioned records, skip the 25-byte metadata header
            // Layout: [RecordHeader: 8][VersionMetadata: 25][Payload: N]
            // payload_len includes metadata, so actual data starts at offset 33
            const VERSION_METADATA_SIZE: usize = 25;
            if payload_len < VERSION_METADATA_SIZE {
                return Err(GraphError::Corruption("versioned record too small".into()));
            }
            let data_start = RECORD_HEADER_SIZE + VERSION_METADATA_SIZE;
            let data_end = RECORD_HEADER_SIZE + payload_len;
            let payload = &record[data_start..data_end];
            deserialize_node(payload)
        } else {
            // Legacy non-versioned record
            let payload = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
            deserialize_node(payload)
        }
    }

    // Helper removed - RecordStore creation inlined at call sites due to Mutex

    pub(crate) fn insert_record(
        &self,
        record: &[u8],
        preferred_page: Option<PageId>,
    ) -> Result<RecordPointer> {
        if let Some(page_id) = preferred_page {
            let (pointer_opt, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
                let mut store = RecordStore::new(pager);
                let result = store.try_insert_into_page(page_id, record)?;
                let dirty = if let Some(ptr) = result {
                    vec![ptr.page_id]
                } else {
                    vec![]
                };
                Ok((result, dirty))
            })?;

            if let Some(pointer) = pointer_opt {
                if self.tracking_enabled.load(std::sync::atomic::Ordering::Relaxed) {
                    self.recent_dirty_pages.lock().unwrap().extend(dirty_pages);
                }
                return Ok(pointer);
            }
        }

        // Try to reuse slots from pages with free slots
        let free_pages: Vec<u32> = self.pages_with_free_slots.iter().map(|r| *r).collect();
        for &page_id in &free_pages {
            let (pointer_opt, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
                let mut store = RecordStore::new(pager);
                let result = store.try_insert_into_page(page_id, record)?;
                let dirty = if let Some(ptr) = result {
                    vec![ptr.page_id]
                } else {
                    vec![]
                };
                Ok((result, dirty))
            })?;

            if let Some(pointer) = pointer_opt {
                if self.tracking_enabled.load(std::sync::atomic::Ordering::Relaxed) {
                    self.recent_dirty_pages.lock().unwrap().extend(dirty_pages);
                }
                let page = self.pager.fetch_page(page_id)?;
                let mut page_data = page.data.clone();
                let record_page = RecordPage::from_bytes(&mut page_data)?;
                if !record_page.has_free_slots()? {
                    self.pages_with_free_slots.remove(&page_id);
                }
                return Ok(pointer);
            }
        }

        if let Some(page_id) = self.take_free_page()? {
            let (pointer_opt, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
                let mut store = RecordStore::new(pager);
                let result = store.try_insert_into_page(page_id, record)?;
                let dirty = if let Some(ptr) = result {
                    vec![ptr.page_id]
                } else {
                    vec![]
                };
                Ok((result, dirty))
            })?;

            if let Some(pointer) = pointer_opt {
                if self.tracking_enabled.load(std::sync::atomic::Ordering::Relaxed) {
                    self.recent_dirty_pages.lock().unwrap().extend(dirty_pages);
                }
                return Ok(pointer);
            }

            self.push_free_page(page_id)?;
        }

        let ((page_id, slot, byte_offset), dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
            let page_id = pager.allocate_page()?;
            let page = pager.fetch_page(page_id)?;
            let mut record_page = RecordPage::from_bytes(&mut page.data)?;
            record_page.initialize()?;
            if !record_page.can_fit(record.len())? {
                return Err(GraphError::InvalidArgument(
                    "record larger than available page space".into(),
                ));
            }
            let slot = record_page.append_record(record)?;
            let byte_offset = record_page.record_offset(slot as usize)?;
            page.dirty = true;
            Ok(((page_id, slot, byte_offset), vec![page_id]))
        })?;
        if self.tracking_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            self.recent_dirty_pages.lock().unwrap().extend(dirty_pages);
        }
        Ok(RecordPointer {
            page_id,
            slot_index: slot,
            byte_offset,
        })
    }

    pub(crate) fn free_record(&self, pointer: RecordPointer) -> Result<()> {
        let page_id = pointer.page_id;
        let (page_empty, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
            let mut store = RecordStore::new(pager);
            let result = store.mark_free(pointer)?;
            Ok((result, vec![page_id]))
        })?;

        if self.tracking_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            self.recent_dirty_pages.lock().unwrap().extend(dirty_pages);
        }

        if page_empty {
            self.push_free_page(page_id)?;
            self.pages_with_free_slots.remove(&page_id);
            if self.header.lock().unwrap().last_record_page == Some(page_id) {
                self.recompute_last_record_page()?;
            }
        } else {
            self.pages_with_free_slots.insert(page_id);
        }
        Ok(())
    }

    pub(crate) fn take_free_page(&self) -> Result<Option<PageId>> {
        let Some(head) = self.header.lock().unwrap().free_page_head else {
            return Ok(None);
        };
        let next = self.pager.with_pager_write(|pager| {
            let page = pager.fetch_page(head)?;
            let mut record_page = RecordPage::from_bytes(&mut page.data)?;
            let next = record_page.free_list_next()?;
            record_page.clear()?;
            record_page.initialize()?;
            page.dirty = true;
            Ok(next)
        })?;
        self.header.lock().unwrap().free_page_head = if next == 0 { None } else { Some(next) };
        self.record_page_write(head);
        Ok(Some(head))
    }

    pub(crate) fn push_free_page(&self, page_id: PageId) -> Result<()> {
        let next = self.header.lock().unwrap().free_page_head.unwrap_or(0);
        self.pager.with_pager_write(|pager| {
            let page = pager.fetch_page(page_id)?;
            // Clear the page data first to remove any existing magic bytes (e.g., BIDX, PIDX)
            // that would cause RecordPage::from_bytes() to fail
            page.data.fill(0);
            let mut record_page = RecordPage::from_bytes(&mut page.data)?;
            record_page.clear()?;
            record_page.set_free_list_next(next);
            page.dirty = true;
            Ok(())
        })?;
        self.header.lock().unwrap().free_page_head = Some(page_id);
        self.record_page_write(page_id);
        Ok(())
    }

    pub(crate) fn update_node_pointer(
        &self,
        pointer: RecordPointer,
        kind: PointerKind,
        new_edge_id: EdgeId,
    ) -> Result<()> {
        self.pager.with_pager_write(|pager| {
            let page = pager.fetch_page(pointer.page_id)?;
            let mut record_page = RecordPage::from_bytes(&mut page.data)?;
            let record = record_page.record_slice_mut(pointer.slot_index as usize)?;
            let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
            if header.kind != RecordKind::Node {
                return Err(GraphError::Corruption(
                    "expected node record, found other kind".into(),
                ));
            }

            // Check if this is a versioned record by looking at the actual kind byte
            let kind_byte = record[0];
            let is_versioned = kind_byte == 0x03 || kind_byte == 0x04; // VersionedNode or VersionedEdge

            let payload_len = header.payload_length as usize;

            let (node, data_start) = if is_versioned {
                // For versioned records, skip the 25-byte metadata header
                const VERSION_METADATA_SIZE: usize = 25;
                if payload_len < VERSION_METADATA_SIZE {
                    return Err(GraphError::Corruption("versioned record too small".into()));
                }
                let data_start = RECORD_HEADER_SIZE + VERSION_METADATA_SIZE;
                let data_end = RECORD_HEADER_SIZE + payload_len;
                let payload_slice = &record[data_start..data_end];
                (deserialize_node(payload_slice)?, data_start)
            } else {
                // Legacy non-versioned record
                let payload_slice = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
                (deserialize_node(payload_slice)?, RECORD_HEADER_SIZE)
            };

            let mut node = node;
            match kind {
                PointerKind::Outgoing => node.first_outgoing_edge_id = new_edge_id,
                PointerKind::Incoming => node.first_incoming_edge_id = new_edge_id,
            }
            let new_payload = serialize_node(&node)?;
            let expected_payload_len = if is_versioned {
                payload_len - 25 // Subtract VERSION_METADATA_SIZE
            } else {
                payload_len
            };
            if new_payload.len() != expected_payload_len {
                return Err(GraphError::Serialization(
                    "node payload size changed during pointer update".into(),
                ));
            }
            let data_end = data_start + new_payload.len();
            record[data_start..data_end].copy_from_slice(&new_payload);
            page.dirty = true;
            Ok(())
        })?;
        self.record_page_write(pointer.page_id);
        Ok(())
    }

    fn update_edge_pointer(
        &self,
        pointer: RecordPointer,
        kind: EdgePointerKind,
        new_edge_id: EdgeId,
    ) -> Result<()> {
        let edge_id = self.pager.with_pager_write(|pager| {
            let page = pager.fetch_page(pointer.page_id)?;
            let mut record_page = RecordPage::from_bytes(&mut page.data)?;
            let record = record_page.record_slice_mut(pointer.slot_index as usize)?;
            let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
            if header.kind != RecordKind::Edge {
                return Err(GraphError::Corruption(
                    "expected edge record, found other kind".into(),
                ));
            }

            // Check if this is a versioned record by looking at the actual kind byte
            let kind_byte = record[0];
            let is_versioned = kind_byte == 0x03 || kind_byte == 0x04; // VersionedNode or VersionedEdge

            let payload_len = header.payload_length as usize;

            let (edge, data_start) = if is_versioned {
                // For versioned records, skip the 25-byte metadata header
                const VERSION_METADATA_SIZE: usize = 25;
                if payload_len < VERSION_METADATA_SIZE {
                    return Err(GraphError::Corruption("versioned record too small".into()));
                }
                let data_start = RECORD_HEADER_SIZE + VERSION_METADATA_SIZE;
                let data_end = RECORD_HEADER_SIZE + payload_len;
                let payload_slice = &record[data_start..data_end];
                (deserialize_edge(payload_slice)?, data_start)
            } else {
                // Legacy non-versioned record
                let payload_slice = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
                (deserialize_edge(payload_slice)?, RECORD_HEADER_SIZE)
            };

            let mut edge = edge;
            let edge_id = edge.id;
            match kind {
                EdgePointerKind::Outgoing => edge.next_outgoing_edge_id = new_edge_id,
                EdgePointerKind::Incoming => edge.next_incoming_edge_id = new_edge_id,
            }
            let new_payload = serialize_edge(&edge)?;
            let expected_payload_len = if is_versioned {
                payload_len - 25 // Subtract VERSION_METADATA_SIZE
            } else {
                payload_len
            };
            if new_payload.len() != expected_payload_len {
                return Err(GraphError::Serialization(
                    "edge payload size changed during pointer update".into(),
                ));
            }
            let data_end = data_start + new_payload.len();
            record[data_start..data_end].copy_from_slice(&new_payload);
            page.dirty = true;
            Ok(edge_id)
        })?;
        self.record_page_write(pointer.page_id);
        self.edge_cache.pop(&edge_id);
        Ok(())
    }

    pub(crate) fn remove_edge_from_node_chain(
        &self,
        node_pointer: RecordPointer,
        pointer_kind: PointerKind,
        removed_edge_id: EdgeId,
        successor_edge_id: EdgeId,
    ) -> Result<()> {
        let node = self.read_node_at(node_pointer)?;
        let first_edge_id = match pointer_kind {
            PointerKind::Outgoing => node.first_outgoing_edge_id,
            PointerKind::Incoming => node.first_incoming_edge_id,
        };

        if first_edge_id == removed_edge_id {
            return self.update_node_pointer(node_pointer, pointer_kind, successor_edge_id);
        }

        let mut current_edge_id = first_edge_id;
        while current_edge_id != NULL_EDGE_ID {
            let current_edge = self.load_edge(current_edge_id)?;
            let next_id = match pointer_kind {
                PointerKind::Outgoing => current_edge.next_outgoing_edge_id,
                PointerKind::Incoming => current_edge.next_incoming_edge_id,
            };
            if next_id == removed_edge_id {
                // Phase 4A: Use get_latest() to get most recent version
                let predecessor_ptr = self
                    .edge_index
                    .get_latest(&current_edge_id)
                    .ok_or(GraphError::Corruption(
                        "edge predecessor missing during deletion".into(),
                    ))?;
                let edge_kind = match pointer_kind {
                    PointerKind::Outgoing => EdgePointerKind::Outgoing,
                    PointerKind::Incoming => EdgePointerKind::Incoming,
                };
                return self.update_edge_pointer(predecessor_ptr, edge_kind, successor_edge_id);
            }
            current_edge_id = next_id;
        }

        Err(GraphError::Corruption(
            "edge not present in node adjacency list".into(),
        ))
    }

    fn recompute_last_record_page(&self) -> Result<()> {
        let mut last = None;
        let page_count = self.pager.page_count();
        if page_count <= 1 {
            self.header.lock().unwrap().last_record_page = None;
            return Ok(());
        }
        for page_idx in (1..page_count).rev() {
            let page_id = PageId::try_from(page_idx)
                .map_err(|_| GraphError::Corruption("page index exceeds u32::MAX".into()))?;
            let live_count = self.pager.with_pager_write(|pager| {
                let page = pager.fetch_page(page_id)?;
                let record_page = RecordPage::from_bytes(&mut page.data)?;
                record_page.live_record_count()
            })?;
            if live_count > 0 {
                last = Some(page_id);
                break;
            }
        }
        self.header.lock().unwrap().last_record_page = last;
        Ok(())
    }
}
