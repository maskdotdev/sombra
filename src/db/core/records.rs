use super::graphdb::GraphDB;
use super::pointer_kind::{EdgePointerKind, PointerKind};
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeId, Node, NULL_EDGE_ID};
use crate::pager::PageId;
use crate::storage::page::RecordPage;
use crate::storage::record::{RecordHeader, RecordKind, RECORD_HEADER_SIZE};
use crate::storage::{
    deserialize_edge, deserialize_node, serialize_edge, serialize_node, RecordPointer, RecordStore,
};
use std::convert::TryFrom;

impl GraphDB {
    pub fn load_edge(&mut self, edge_id: EdgeId) -> Result<Edge> {
        if let Some(edge) = self.edge_cache.get(&edge_id) {
            return Ok(edge.clone());
        }

        let pointer = *self
            .edge_index
            .get(&edge_id)
            .ok_or(GraphError::NotFound("edge"))?;
        let page = self.pager.fetch_page(pointer.page_id)?;
        let record_page = RecordPage::from_bytes(&mut page.data)?;
        let record = record_page.record_slice(pointer.slot_index as usize)?;
        let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
        if header.kind != RecordKind::Edge {
            return Err(GraphError::Corruption(
                "expected edge record, found other kind".into(),
            ));
        }
        let payload_len = header.payload_length as usize;
        let payload = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
        let edge = deserialize_edge(payload)?;

        self.edge_cache.put(edge_id, edge.clone());
        Ok(edge)
    }

    pub fn load_edges_batch(&mut self, edge_ids: &[EdgeId]) -> Result<Vec<Edge>> {
        use std::collections::HashMap;

        let mut edges_to_load: HashMap<PageId, Vec<(EdgeId, RecordPointer)>> = HashMap::new();
        let mut loaded_edges: HashMap<EdgeId, Edge> = HashMap::new();

        for &edge_id in edge_ids {
            if let Some(edge) = self.edge_cache.get(&edge_id) {
                loaded_edges.insert(edge_id, edge.clone());
            } else {
                let pointer = *self
                    .edge_index
                    .get(&edge_id)
                    .ok_or(GraphError::NotFound("edge"))?;
                edges_to_load
                    .entry(pointer.page_id)
                    .or_default()
                    .push((edge_id, pointer));
            }
        }

        for (page_id, edges_on_page) in edges_to_load {
            let page = self.pager.fetch_page(page_id)?;
            let record_page = RecordPage::from_bytes(&mut page.data)?;

            for (edge_id, pointer) in edges_on_page {
                let record = record_page.record_slice(pointer.slot_index as usize)?;
                let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
                if header.kind != RecordKind::Edge {
                    return Err(GraphError::Corruption(
                        "expected edge record, found other kind".into(),
                    ));
                }
                let payload_len = header.payload_length as usize;
                let payload = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
                let edge = deserialize_edge(payload)?;

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

    pub(crate) fn read_node_at(&mut self, pointer: RecordPointer) -> Result<Node> {
        let page = self.pager.fetch_page(pointer.page_id)?;
        let record_page = RecordPage::from_bytes(&mut page.data)?;
        let record = record_page.record_slice(pointer.slot_index as usize)?;
        let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
        if header.kind != RecordKind::Node {
            return Err(GraphError::Corruption(
                "expected node record, found other kind".into(),
            ));
        }
        let payload_len = header.payload_length as usize;
        let payload = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
        deserialize_node(payload)
    }

    pub(crate) fn record_store(&mut self) -> RecordStore<'_> {
        RecordStore::new(&mut self.pager)
    }

    pub(crate) fn insert_record(
        &mut self,
        record: &[u8],
        preferred_page: Option<PageId>,
    ) -> Result<RecordPointer> {
        if let Some(page_id) = preferred_page {
            let mut store = self.record_store();
            if let Some(pointer) = store.try_insert_into_page(page_id, record)? {
                self.record_page_write(pointer.page_id);
                return Ok(pointer);
            }
        }

        // Try to reuse slots from pages with free slots
        for &page_id in self.pages_with_free_slots.clone().iter() {
            let mut store = self.record_store();
            if let Some(pointer) = store.try_insert_into_page(page_id, record)? {
                self.record_page_write(pointer.page_id);
                let page = self.pager.fetch_page(page_id)?;
                let record_page = RecordPage::from_bytes(&mut page.data)?;
                if !record_page.has_free_slots()? {
                    self.pages_with_free_slots.remove(&page_id);
                }
                return Ok(pointer);
            }
        }

        if let Some(page_id) = self.take_free_page()? {
            {
                let mut store = self.record_store();
                if let Some(pointer) = store.try_insert_into_page(page_id, record)? {
                    self.record_page_write(pointer.page_id);
                    return Ok(pointer);
                }
            }
            self.push_free_page(page_id)?;
        }

        let page_id = self.pager.allocate_page()?;
        let page = self.pager.fetch_page(page_id)?;
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
        self.record_page_write(page_id);
        Ok(RecordPointer {
            page_id,
            slot_index: slot,
            byte_offset,
        })
    }

    pub(crate) fn free_record(&mut self, pointer: RecordPointer) -> Result<()> {
        let page_id = pointer.page_id;
        let mut store = self.record_store();
        let page_empty = store.mark_free(pointer)?;
        drop(store);

        if page_empty {
            self.push_free_page(page_id)?;
            self.pages_with_free_slots.remove(&page_id);
            if self.header.last_record_page == Some(page_id) {
                self.recompute_last_record_page()?;
            }
        } else {
            self.pages_with_free_slots.insert(page_id);
        }
        Ok(())
    }

    pub(crate) fn take_free_page(&mut self) -> Result<Option<PageId>> {
        let Some(head) = self.header.free_page_head else {
            return Ok(None);
        };
        let page = self.pager.fetch_page(head)?;
        let mut record_page = RecordPage::from_bytes(&mut page.data)?;
        let next = record_page.free_list_next()?;
        self.header.free_page_head = if next == 0 { None } else { Some(next) };
        record_page.clear()?;
        record_page.initialize()?;
        page.dirty = true;
        self.record_page_write(head);
        Ok(Some(head))
    }

    pub(crate) fn push_free_page(&mut self, page_id: PageId) -> Result<()> {
        let page = self.pager.fetch_page(page_id)?;
        let mut record_page = RecordPage::from_bytes(&mut page.data)?;
        record_page.clear()?;
        let next = self.header.free_page_head.unwrap_or(0);
        record_page.set_free_list_next(next);
        self.header.free_page_head = Some(page_id);
        page.dirty = true;
        self.record_page_write(page_id);
        Ok(())
    }

    pub(crate) fn update_node_pointer(
        &mut self,
        pointer: RecordPointer,
        kind: PointerKind,
        new_edge_id: EdgeId,
    ) -> Result<()> {
        let page = self.pager.fetch_page(pointer.page_id)?;
        let mut record_page = RecordPage::from_bytes(&mut page.data)?;
        let record = record_page.record_slice_mut(pointer.slot_index as usize)?;
        let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
        if header.kind != RecordKind::Node {
            return Err(GraphError::Corruption(
                "expected node record, found other kind".into(),
            ));
        }
        let payload_len = header.payload_length as usize;
        let payload_slice = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
        let mut node = deserialize_node(payload_slice)?;
        match kind {
            PointerKind::Outgoing => node.first_outgoing_edge_id = new_edge_id,
            PointerKind::Incoming => node.first_incoming_edge_id = new_edge_id,
        }
        let new_payload = serialize_node(&node)?;
        if new_payload.len() != payload_len {
            return Err(GraphError::Serialization(
                "node payload size changed during pointer update".into(),
            ));
        }
        record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len].copy_from_slice(&new_payload);
        page.dirty = true;
        self.record_page_write(pointer.page_id);
        Ok(())
    }

    fn update_edge_pointer(
        &mut self,
        pointer: RecordPointer,
        kind: EdgePointerKind,
        new_edge_id: EdgeId,
    ) -> Result<()> {
        let page = self.pager.fetch_page(pointer.page_id)?;
        let mut record_page = RecordPage::from_bytes(&mut page.data)?;
        let record = record_page.record_slice_mut(pointer.slot_index as usize)?;
        let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
        if header.kind != RecordKind::Edge {
            return Err(GraphError::Corruption(
                "expected edge record, found other kind".into(),
            ));
        }
        let payload_len = header.payload_length as usize;
        let payload_slice = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
        let mut edge = deserialize_edge(payload_slice)?;
        let edge_id = edge.id;
        match kind {
            EdgePointerKind::Outgoing => edge.next_outgoing_edge_id = new_edge_id,
            EdgePointerKind::Incoming => edge.next_incoming_edge_id = new_edge_id,
        }
        let new_payload = serialize_edge(&edge)?;
        if new_payload.len() != payload_len {
            return Err(GraphError::Serialization(
                "edge payload size changed during pointer update".into(),
            ));
        }
        record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len].copy_from_slice(&new_payload);
        page.dirty = true;
        self.record_page_write(pointer.page_id);
        self.edge_cache.pop(&edge_id);
        Ok(())
    }

    pub(crate) fn remove_edge_from_node_chain(
        &mut self,
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
                let predecessor_ptr =
                    *self
                        .edge_index
                        .get(&current_edge_id)
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

    fn recompute_last_record_page(&mut self) -> Result<()> {
        let mut last = None;
        let page_count = self.pager.page_count();
        if page_count <= 1 {
            self.header.last_record_page = None;
            return Ok(());
        }
        for page_idx in (1..page_count).rev() {
            let page_id = PageId::try_from(page_idx)
                .map_err(|_| GraphError::Corruption("page index exceeds u32::MAX".into()))?;
            let page = self.pager.fetch_page(page_id)?;
            let record_page = RecordPage::from_bytes(&mut page.data)?;
            if record_page.live_record_count()? > 0 {
                last = Some(page_id);
                break;
            }
        }
        self.header.last_record_page = last;
        Ok(())
    }
}
