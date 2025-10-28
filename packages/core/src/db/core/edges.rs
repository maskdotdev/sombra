use super::graphdb::GraphDB;
use super::pointer_kind::PointerKind;
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeId, NodeId, NULL_EDGE_ID};
use crate::storage::record::{encode_record, RecordKind};
use crate::storage::serialize_edge;
use crate::storage::heap::RecordStore;

impl GraphDB {
    pub fn add_edge(&mut self, edge: Edge) -> Result<EdgeId> {
        let tx_id = self.allocate_tx_id()?;
        self.start_tracking();

        // Allocate commit timestamp for MVCC
        let commit_ts = if self.config.mvcc_enabled {
            self.timestamp_oracle.as_ref()
                .map(|oracle| oracle.allocate_commit_timestamp())
                .unwrap_or(0)
        } else {
            0
        };

        let (edge_id, _version_ptr) = self.add_edge_internal(edge, tx_id, commit_ts)?;

        self.header.last_committed_tx_id = tx_id;
        self.write_header()?;

        let dirty_pages = self.take_recent_dirty_pages();
        self.commit_to_wal(tx_id, &dirty_pages)?;
        self.stop_tracking();

        Ok(edge_id)
    }

    pub fn add_edge_internal(&mut self, mut edge: Edge, tx_id: crate::db::TxId, commit_ts: u64) -> Result<(EdgeId, Option<crate::storage::heap::RecordPointer>)> {
        let edge_id = self.header.next_edge_id;
        self.header.next_edge_id += 1;

        let source_ptr = self
            .node_index
            .get(&edge.source_node_id)
            .ok_or(GraphError::NotFound("source node"))?;
        let target_ptr = self
            .node_index
            .get(&edge.target_node_id)
            .ok_or(GraphError::NotFound("target node"))?;

        let source_node = self.read_node_at(source_ptr)?;
        let target_node = self.read_node_at(target_ptr)?;

        edge.id = edge_id;
        edge.next_outgoing_edge_id = source_node.first_outgoing_edge_id;
        edge.next_incoming_edge_id = target_node.first_incoming_edge_id;

        let payload = serialize_edge(&edge)?;
        
        // Use store_new_version if MVCC enabled, otherwise legacy record format
        let (pointer, version_pointer) = if self.config.mvcc_enabled {
            use crate::storage::version_chain::store_new_version;
            
            let dirty_pages = {
                let mut pager_guard = self.pager.lock().unwrap();
                let mut record_store = RecordStore::new(&mut *pager_guard);
                let pointer = store_new_version(
                    &mut record_store,
                    None,  // No previous version for new edges
                    edge_id,
                    RecordKind::Edge,
                    &payload,
                    tx_id,
                    commit_ts,
                )?;
                
                // Collect dirty pages before dropping guards
                let dirty_pages = record_store.take_dirty_pages();
                drop(record_store);
                drop(pager_guard);
                Ok::<_, GraphError>((pointer, dirty_pages))
            }?;
            
            // Register dirty pages with GraphDB
            for page_id in dirty_pages.1 {
                self.record_page_write(page_id);
            }
            
            (dirty_pages.0, Some(dirty_pages.0))
        } else {
            // Legacy non-versioned record
            let record = encode_record(RecordKind::Edge, &payload)?;
            let preferred = self.header.last_record_page;
            let pointer = self.insert_record(&record, preferred)?;
            (pointer, None)
        };
        
        self.edge_index.insert(edge_id, pointer);
        self.header.last_record_page = Some(pointer.page_id);

        self.update_node_pointer(source_ptr, PointerKind::Outgoing, edge_id)?;
        self.update_node_pointer(target_ptr, PointerKind::Incoming, edge_id)?;

        self.outgoing_adjacency
            .entry(edge.source_node_id)
            .or_default()
            .push(edge_id);
        self.incoming_adjacency
            .entry(edge.target_node_id)
            .or_default()
            .push(edge_id);

        self.outgoing_neighbors_cache.remove(&edge.source_node_id);
        self.incoming_neighbors_cache.remove(&edge.target_node_id);

        self.node_cache.lock().unwrap().pop(&edge.source_node_id);
        self.node_cache.lock().unwrap().pop(&edge.target_node_id);
        self.edge_cache.lock().unwrap().put(edge_id, edge);

        Ok((edge_id, version_pointer))
    }

    pub fn delete_edge(&mut self, edge_id: EdgeId) -> Result<()> {
        self.delete_edge_internal(edge_id)
    }

    pub fn delete_edge_internal(&mut self, edge_id: EdgeId) -> Result<()> {
        let pointer = *self
            .edge_index
            .get(&edge_id)
            .ok_or(GraphError::NotFound("edge"))?;
        let edge = self.load_edge(edge_id)?;

        let source_ptr = self
            .node_index
            .get(&edge.source_node_id)
            .ok_or(GraphError::NotFound("source node"))?;
        let target_ptr = self
            .node_index
            .get(&edge.target_node_id)
            .ok_or(GraphError::NotFound("target node"))?;

        self.remove_edge_from_node_chain(
            source_ptr,
            PointerKind::Outgoing,
            edge_id,
            edge.next_outgoing_edge_id,
        )?;
        self.remove_edge_from_node_chain(
            target_ptr,
            PointerKind::Incoming,
            edge_id,
            edge.next_incoming_edge_id,
        )?;

        if let Some(edges) = self.outgoing_adjacency.get_mut(&edge.source_node_id) {
            edges.retain(|&e| e != edge_id);
        }
        if let Some(edges) = self.incoming_adjacency.get_mut(&edge.target_node_id) {
            edges.retain(|&e| e != edge_id);
        }

        self.outgoing_neighbors_cache.remove(&edge.source_node_id);
        self.incoming_neighbors_cache.remove(&edge.target_node_id);

        self.node_cache.lock().unwrap().pop(&edge.source_node_id);
        self.node_cache.lock().unwrap().pop(&edge.target_node_id);
        self.edge_cache.lock().unwrap().pop(&edge_id);

        self.edge_index.remove(&edge_id);
        self.free_record(pointer)?;
        Ok(())
    }

    pub fn count_outgoing_edges(&mut self, node_id: NodeId) -> Result<usize> {
        if let Some(edges) = self.outgoing_adjacency.get(&node_id) {
            return Ok(edges.len());
        }

        let node = self
            .get_node(node_id)?
            .ok_or(GraphError::NotFound("node"))?;
        let mut count = 0;
        let mut edge_list = Vec::new();
        let mut edge_id = node.first_outgoing_edge_id;
        while edge_id != NULL_EDGE_ID {
            let edge = self.load_edge(edge_id)?;
            count += 1;
            edge_list.push(edge_id);
            edge_id = edge.next_outgoing_edge_id;
        }
        self.outgoing_adjacency.insert(node_id, edge_list);
        Ok(count)
    }

    pub fn count_incoming_edges(&mut self, node_id: NodeId) -> Result<usize> {
        if let Some(edges) = self.incoming_adjacency.get(&node_id) {
            return Ok(edges.len());
        }

        let node = self
            .get_node(node_id)?
            .ok_or(GraphError::NotFound("node"))?;
        let mut count = 0;
        let mut edge_list = Vec::new();
        let mut edge_id = node.first_incoming_edge_id;
        while edge_id != NULL_EDGE_ID {
            let edge = self.load_edge(edge_id)?;
            count += 1;
            edge_list.push(edge_id);
            edge_id = edge.next_incoming_edge_id;
        }
        self.incoming_adjacency.insert(node_id, edge_list);
        Ok(count)
    }
}
