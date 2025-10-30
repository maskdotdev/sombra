use super::graphdb::GraphDB;
use super::pointer_kind::PointerKind;
use crate::error::{GraphError, Result};
use crate::model::{Edge, EdgeId, NodeId, NULL_EDGE_ID};
use crate::storage::heap::RecordStore;
use crate::storage::record::RecordKind;
use crate::storage::serialize_edge;

impl GraphDB {
    pub fn add_edge(&mut self, edge: Edge) -> Result<EdgeId> {
        let tx_id = self.allocate_tx_id()?;
        self.start_tracking();

        // Allocate commit timestamp for MVCC
        let commit_ts = self.timestamp_oracle
            .allocate_commit_timestamp();

        let (edge_id, _version_ptr) = self.add_edge_internal(edge, tx_id, commit_ts)?;

        self.header.lock().unwrap().last_committed_tx_id = tx_id;
        self.write_header()?;

        let dirty_pages = self.take_recent_dirty_pages();
        self.commit_to_wal(tx_id, &dirty_pages)?;
        self.stop_tracking();

        Ok(edge_id)
    }

    pub fn add_edge_internal(
        &self,
        mut edge: Edge,
        tx_id: crate::db::TxId,
        commit_ts: u64,
    ) -> Result<(EdgeId, Option<crate::storage::heap::RecordPointer>)> {
        let edge_id = {
            let mut header = self.header.lock().unwrap();
            let edge_id = header.next_edge_id;
            header.next_edge_id += 1;
            edge_id
        };

        let source_ptr = self
            .node_index
            .get_latest(&edge.source_node_id)
            .ok_or(GraphError::NotFound("source node"))?;
        let target_ptr = self
            .node_index
            .get_latest(&edge.target_node_id)
            .ok_or(GraphError::NotFound("target node"))?;

        let source_node = self.read_node_at(source_ptr)?;
        let target_node = self.read_node_at(target_ptr)?;

        edge.id = edge_id;
        edge.next_outgoing_edge_id = source_node.first_outgoing_edge_id;
        edge.next_incoming_edge_id = target_node.first_incoming_edge_id;

        let payload = serialize_edge(&edge)?;

        // Create new version with MVCC support
        use crate::storage::version_chain::store_new_version;

        let (pointer, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
            let mut record_store = RecordStore::new(pager);
            let pointer = store_new_version(
                &mut record_store,
                None, // No previous version for new edges
                edge_id,
                RecordKind::Edge,
                &payload,
                tx_id,
                commit_ts,
                false, // is_deleted: false for new edge creation
            )?;

            // Collect dirty pages before dropping guards
            let dirty_pages = record_store.take_dirty_pages();
            Ok((pointer, dirty_pages.clone()))
        })?;

        // Track dirty pages for transaction if needed
        if self.tracking_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            self.recent_dirty_pages.lock().unwrap().extend(dirty_pages);
        }

        let version_pointer = Some(pointer);

        // Phase 4A: Insert into BTreeIndex (supports version chains)
        self.edge_index.insert(edge_id, pointer);
        self.header.lock().unwrap().last_record_page = Some(pointer.page_id);

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

        self.node_cache.pop(&edge.source_node_id);
        self.node_cache.pop(&edge.target_node_id);
        self.edge_cache.put(edge_id, edge);

        Ok((edge_id, version_pointer))
    }

    pub fn delete_edge(&mut self, edge_id: EdgeId) -> Result<()> {
        self.delete_edge_internal(edge_id)
    }

    pub fn delete_edge_internal(&self, edge_id: EdgeId) -> Result<()> {
        // Phase 4B: MVCC-aware deletion
        // Get the latest version of the edge
        let pointer = self
            .edge_index
            .get_latest(&edge_id)
            .ok_or(GraphError::NotFound("edge"))?;
        let edge = self.load_edge(edge_id)?;

        let source_ptr = self
            .node_index
            .get_latest(&edge.source_node_id)
            .ok_or(GraphError::NotFound("source node"))?;
        let target_ptr = self
            .node_index
            .get_latest(&edge.target_node_id)
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

        if let Some(mut edges) = self.outgoing_adjacency.get_mut(&edge.source_node_id) {
            edges.retain(|&e| e != edge_id);
        }
        if let Some(mut edges) = self.incoming_adjacency.get_mut(&edge.target_node_id) {
            edges.retain(|&e| e != edge_id);
        }

        self.outgoing_neighbors_cache.remove(&edge.source_node_id);
        self.incoming_neighbors_cache.remove(&edge.target_node_id);

        self.node_cache.pop(&edge.source_node_id);
        self.node_cache.pop(&edge.target_node_id);
        self.edge_cache.pop(&edge_id);

        // Create tombstone version for MVCC snapshot isolation
        use crate::storage::version_chain::store_new_version;
        
        let tx_id = self.allocate_tx_id().unwrap_or(1);
        let commit_ts = self.timestamp_oracle.allocate_commit_timestamp();
        
        let payload = serialize_edge(&edge)?;
        
        let (tombstone_ptr, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
            let mut record_store = RecordStore::new(pager);
            
            // store_new_version with is_deleted=true creates a tombstone
            let tombstone_ptr = store_new_version(
                &mut record_store,
                Some(pointer), // Link to previous version
                edge_id,
                RecordKind::Edge,
                &payload,
                tx_id,
                commit_ts,
                true, // is_deleted: true to create tombstone
            )?;
            
            let dirty_pages = record_store.take_dirty_pages();
            Ok((tombstone_ptr, dirty_pages.clone()))
        })?;
        
        // Track dirty pages for transaction if needed
        if self.tracking_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            self.recent_dirty_pages.lock().unwrap().extend(dirty_pages);
        }
        
        // Update index to point to tombstone (keeps edge in index for snapshot isolation)
        self.edge_index.insert(edge_id, tombstone_ptr);
        
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
