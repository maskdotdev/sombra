use super::graphdb::GraphDB;
use crate::error::{GraphError, Result};
use crate::model::{Node, NodeId, NULL_EDGE_ID};
use crate::storage::record::{encode_record, RecordKind};
use crate::storage::serialize_node;
use crate::storage::version_chain::VersionChainReader;
use crate::storage::deserialize_node;
use crate::storage::heap::RecordStore;
use std::collections::HashSet;

impl GraphDB {
    pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
        let tx_id = self.allocate_tx_id()?;
        self.start_tracking();

        // Call add_node_internal with tx_id and commit_ts
        // Note: commit_ts is allocated here since this is auto-committed
        let commit_ts = if self.config.mvcc_enabled {
            self.timestamp_oracle.as_ref()
                .map(|oracle| oracle.allocate_commit_timestamp())
                .unwrap_or(0)
        } else {
            0
        };

        let (node_id, _version_ptr) = self.add_node_internal(node, tx_id, commit_ts)?;

        self.header.last_committed_tx_id = tx_id;
        self.write_header()?;

        let dirty_pages = self.take_recent_dirty_pages();
        self.commit_to_wal(tx_id, &dirty_pages)?;
        self.stop_tracking();

        Ok(node_id)
    }

    pub fn add_node_internal(&mut self, node: Node, tx_id: crate::db::TxId, commit_ts: u64) -> Result<(NodeId, Option<crate::storage::heap::RecordPointer>)> {
        // Detect if this is an update (node has ID and exists) or new node creation
        let is_update = node.id != 0 && self.node_index.get(&node.id).is_some();
        
        if is_update {
            // Update existing node - create new version in version chain
            self.update_node_version(node, tx_id, commit_ts)
        } else {
            // Create new node - allocate new ID
            self.create_new_node(node, tx_id, commit_ts)
        }
    }

    /// Create a new node with a new ID (not an update)
    fn create_new_node(&mut self, mut node: Node, tx_id: crate::db::TxId, commit_ts: u64) -> Result<(NodeId, Option<crate::storage::heap::RecordPointer>)> {
        let node_id = self.header.next_node_id;
        self.header.next_node_id += 1;

        node.id = node_id;
        node.first_outgoing_edge_id = NULL_EDGE_ID;
        node.first_incoming_edge_id = NULL_EDGE_ID;

        let payload = serialize_node(&node)?;
        
        // Use store_new_version if MVCC enabled, otherwise legacy record format
        let (pointer, version_pointer) = if self.config.mvcc_enabled {
            use crate::storage::version_chain::store_new_version;
            
            let dirty_pages = {
                let mut pager_guard = self.pager.write().unwrap();
                let mut record_store = RecordStore::new(&mut *pager_guard);
                let pointer = store_new_version(
                    &mut record_store,
                    None,  // No previous version
                    node_id,
                    RecordKind::Node,
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
            let record = encode_record(RecordKind::Node, &payload)?;
            let preferred = self.header.last_record_page;
            let pointer = self.insert_record(&record, preferred)?;
            (pointer, None)
        };

        self.node_index.insert(node_id, pointer);

        for label in &node.labels {
            self.label_index
                .entry(label.clone())
                .or_default()
                .insert(node_id);
        }

        self.update_property_indexes_on_node_add(node_id)?;
        self.node_cache.lock().unwrap().put(node_id, node.clone());
        self.header.last_record_page = Some(pointer.page_id);

        Ok((node_id, version_pointer))
    }

    /// Update an existing node by creating a new version in the version chain
    fn update_node_version(&mut self, node: Node, tx_id: crate::db::TxId, commit_ts: u64) -> Result<(NodeId, Option<crate::storage::heap::RecordPointer>)> {
        let node_id = node.id;
        
        // Get pointer to current version (head of version chain)
        let prev_pointer = self.node_index.get(&node_id)
            .ok_or_else(|| GraphError::NotFound("node"))?;
        
        // Read the old node version to compute diffs for index updates
        let old_node = self.read_node_at(prev_pointer)?;
        
        let payload = serialize_node(&node)?;
        
        // Create new version in version chain
        use crate::storage::version_chain::store_new_version;
        
        let new_pointer = {
            let mut pager_guard = self.pager.write().unwrap();
            let mut record_store = RecordStore::new(&mut *pager_guard);
            let new_pointer = store_new_version(
                &mut record_store,
                Some(prev_pointer),  // Link to previous version
                node_id,
                RecordKind::Node,
                &payload,
                tx_id,
                commit_ts,
            )?;
            
            // Collect dirty pages before dropping guards
            let dirty_pages = record_store.take_dirty_pages();
            drop(record_store);
            drop(pager_guard);
            
            // Register dirty pages with GraphDB
            for page_id in dirty_pages {
                self.record_page_write(page_id);
            }
            
            Ok::<_, GraphError>(new_pointer)
        }?;
        
        // Update index to point to NEW head of version chain
        self.node_index.insert(node_id, new_pointer);
        
        // Update label indexes: remove old labels, add new labels
        use std::collections::HashSet;
        let old_labels: HashSet<_> = old_node.labels.iter().collect();
        let new_labels: HashSet<_> = node.labels.iter().collect();
        
        // Remove labels that are no longer present
        for label in old_labels.difference(&new_labels) {
            if let Some(node_set) = self.label_index.get_mut(*label) {
                node_set.remove(&node_id);
                if node_set.is_empty() {
                    self.label_index.remove(*label);
                }
            }
        }
        
        // Add new labels
        for label in &node.labels {
            self.label_index
                .entry(label.clone())
                .or_default()
                .insert(node_id);
        }
        
        // Update property indexes: remove old properties, add new properties
        for label in &old_node.labels {
            for (property_key, property_value) in &old_node.properties {
                // Only remove if the property changed or label changed
                let should_remove = !node.labels.contains(label) 
                    || node.properties.get(property_key) != Some(property_value);
                
                if should_remove {
                    self.update_property_index_on_remove(node_id, label, property_key, property_value);
                }
            }
        }
        
        // Add new property index entries
        for label in &node.labels {
            for (property_key, property_value) in &node.properties {
                self.update_property_index_on_add(node_id, label, property_key, property_value);
            }
        }
        
        // Update cache with new version
        self.node_cache.lock().unwrap().put(node_id, node.clone());
        self.header.last_record_page = Some(new_pointer.page_id);
        
        Ok((node_id, Some(new_pointer)))
    }

    pub fn add_nodes_bulk(&mut self, nodes: Vec<Node>) -> Result<Vec<NodeId>> {
        let mut node_ids = Vec::with_capacity(nodes.len());
        
        // Allocate a single transaction ID for the bulk operation
        let tx_id = self.allocate_tx_id()?;
        
        // Allocate commit timestamp if MVCC enabled
        let commit_ts = if self.config.mvcc_enabled {
            self.timestamp_oracle.as_ref()
                .map(|oracle| oracle.allocate_commit_timestamp())
                .unwrap_or(0)
        } else {
            0
        };

        for node in nodes {
            // Use add_node_internal which handles both create and update
            let (node_id, _version_ptr) = self.add_node_internal(node, tx_id, commit_ts)?;
            node_ids.push(node_id);
        }

        Ok(node_ids)
    }

    pub fn delete_nodes_bulk(&mut self, node_ids: &[NodeId]) -> Result<()> {
        for &node_id in node_ids {
            self.delete_node_internal(node_id)?;
        }

        self.node_index.batch_remove(node_ids);
        Ok(())
    }

    pub fn delete_node(&mut self, node_id: NodeId) -> Result<()> {
        self.delete_node_internal(node_id)
    }

    pub fn delete_node_internal(&mut self, node_id: NodeId) -> Result<()> {
        let pointer = self
            .node_index
            .get(&node_id)
            .ok_or(GraphError::NotFound("node"))?;
        let node = self.read_node_at(pointer)?;

        let mut visited = HashSet::new();
        let mut edges_to_delete = Vec::new();

        let mut next_out = node.first_outgoing_edge_id;
        while next_out != NULL_EDGE_ID {
            let edge = self.load_edge(next_out)?;
            if visited.insert(edge.id) {
                edges_to_delete.push(edge.id);
            }
            next_out = edge.next_outgoing_edge_id;
        }

        let mut next_in = node.first_incoming_edge_id;
        while next_in != NULL_EDGE_ID {
            let edge = self.load_edge(next_in)?;
            if visited.insert(edge.id) {
                edges_to_delete.push(edge.id);
            }
            next_in = edge.next_incoming_edge_id;
        }

        for edge_id in edges_to_delete {
            self.delete_edge_internal(edge_id)?;
        }

        for label in &node.labels {
            if let Some(node_set) = self.label_index.get_mut(label) {
                node_set.remove(&node_id);
                if node_set.is_empty() {
                    self.label_index.remove(label);
                }
            }
        }

        self.update_property_indexes_on_node_delete(node_id)?;

        self.node_cache.lock().unwrap().pop(&node_id);

        self.node_index.remove(&node_id);
        self.free_record(pointer)?;
        Ok(())
    }

    pub fn get_node(&mut self, node_id: NodeId) -> Result<Option<Node>> {
        self.metrics.node_lookups += 1;

        if let Some(node) = self.node_cache.lock().unwrap().get(&node_id) {
            self.metrics.cache_hits += 1;
            return Ok(Some(node.clone()));
        }

        self.metrics.cache_misses += 1;

        let pointer = match self.node_index.get(&node_id) {
            Some(p) => p,
            None => return Ok(None),
        };
        let node = self.read_node_at(pointer)?;
        self.node_cache.lock().unwrap().put(node_id, node.clone());
        Ok(Some(node))
    }

    /// Get a node with MVCC snapshot isolation
    ///
    /// This method reads a node using the provided snapshot timestamp,
    /// ensuring the correct version is returned based on MVCC visibility rules.
    ///
    /// # Arguments
    /// * `node_id` - The ID of the node to retrieve
    /// * `snapshot_ts` - Snapshot timestamp for visibility checking
    /// * `current_tx_id` - Optional transaction ID for read-your-own-writes
    ///
    /// # Returns
    /// * `Ok(Some(Node))` - Node visible at the snapshot timestamp
    /// * `Ok(None)` - Node doesn't exist or is not visible at snapshot
    /// * `Err(_)` - Error reading the node
    pub fn get_node_with_snapshot(
        &mut self,
        node_id: NodeId,
        snapshot_ts: u64,
        current_tx_id: Option<crate::db::TxId>,
    ) -> Result<Option<Node>> {
        self.metrics.node_lookups += 1;

        // If MVCC is not enabled, fall back to regular get_node
        if !self.config.mvcc_enabled {
            return self.get_node(node_id);
        }

        // Get the head pointer from the index
        let head_pointer = match self.node_index.get(&node_id) {
            Some(p) => p.clone(),
            None => return Ok(None),
        };

        // Use VersionChainReader to find the visible version
        let mut pager_guard = self.pager.write().unwrap();
        let mut record_store = RecordStore::new(&mut *pager_guard);
        let versioned_record = VersionChainReader::read_version_for_snapshot(
            &mut record_store,
            head_pointer,
            snapshot_ts,
            current_tx_id,
        )?;

        match versioned_record {
            Some(vr) => {
                // Deserialize the node from the versioned record data
                let node = deserialize_node(&vr.data)?;
                Ok(Some(node))
            }
            None => Ok(None),
        }
    }

    pub fn get_nodes_by_label(&mut self, label: &str) -> Result<Vec<NodeId>> {
        self.metrics.label_index_queries += 1;
        Ok(self
            .label_index
            .get(label)
            .map(|nodes| nodes.iter().cloned().collect())
            .unwrap_or_default())
    }

    pub fn get_nodes_in_range(&self, start: NodeId, end: NodeId) -> Vec<NodeId> {
        self.node_index
            .range(start, end)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    pub fn get_nodes_from(&self, start: NodeId) -> Vec<NodeId> {
        self.node_index
            .range_from(start)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    pub fn get_nodes_to(&self, end: NodeId) -> Vec<NodeId> {
        self.node_index
            .range_to(end)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    pub fn get_first_node(&self) -> Option<NodeId> {
        self.node_index.first().map(|(id, _)| id)
    }

    pub fn get_last_node(&self) -> Option<NodeId> {
        self.node_index.last().map(|(id, _)| id)
    }

    pub fn get_first_n_nodes(&self, n: usize) -> Vec<NodeId> {
        self.node_index
            .first_n(n)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    pub fn get_last_n_nodes(&self, n: usize) -> Vec<NodeId> {
        self.node_index
            .last_n(n)
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    pub fn get_all_node_ids_ordered(&self) -> Vec<NodeId> {
        self.node_index
            .iter()
            .into_iter()
            .map(|(id, _)| id)
            .collect()
    }

    pub fn set_node_property(
        &mut self,
        node_id: NodeId,
        key: String,
        value: crate::model::PropertyValue,
    ) -> Result<()> {
        let tx_id = self.allocate_tx_id()?;
        self.start_tracking();

        let pointer = self
            .node_index
            .get(&node_id)
            .ok_or(GraphError::NotFound("node"))?;
        let mut node = self.read_node_at(pointer)?;

        let old_value = node.properties.get(&key).cloned();

        node.properties.insert(key.clone(), value.clone());

        let payload = crate::storage::serialize_node(&node)?;
        let record = crate::storage::record::encode_record(
            crate::storage::record::RecordKind::Node,
            &payload,
        )?;

        let update_result = {
            let mut pager_guard = self.pager.write().unwrap();
            let mut store = RecordStore::new(&mut *pager_guard);
            let result = store.update_in_place(pointer, &record)?;
            drop(store);
            drop(pager_guard);
            result
        };

        if let Some(new_pointer) = update_result {
            self.record_page_write(new_pointer.page_id);

            if let Some(old_val) = old_value {
                for label in &node.labels {
                    self.update_property_index_on_remove(node_id, label, &key, &old_val);
                }
            }

            for label in &node.labels {
                self.update_property_index_on_add(node_id, label, &key, &value);
            }

            self.node_cache.lock().unwrap().put(node_id, node.clone());

            self.header.last_committed_tx_id = tx_id;
            self.write_header()?;

            let dirty_pages = self.take_recent_dirty_pages();
            self.commit_to_wal(tx_id, &dirty_pages)?;
            self.stop_tracking();

            Ok(())
        } else {
            self.free_record(pointer)?;

            let preferred = self.header.last_record_page;
            let new_pointer = self.insert_record(&record, preferred)?;

            self.node_index.insert(node_id, new_pointer);

            if let Some(old_val) = old_value {
                for label in &node.labels {
                    self.update_property_index_on_remove(node_id, label, &key, &old_val);
                }
            }

            for label in &node.labels {
                self.update_property_index_on_add(node_id, label, &key, &value);
            }

            self.node_cache.lock().unwrap().put(node_id, node.clone());

            self.header.last_record_page = Some(new_pointer.page_id);
            self.header.last_committed_tx_id = tx_id;
            self.write_header()?;

            let dirty_pages = self.take_recent_dirty_pages();
            self.commit_to_wal(tx_id, &dirty_pages)?;
            self.stop_tracking();

            Ok(())
        }
    }

    pub fn remove_node_property(&mut self, node_id: NodeId, key: &str) -> Result<()> {
        let tx_id = self.allocate_tx_id()?;
        self.start_tracking();

        let pointer = self
            .node_index
            .get(&node_id)
            .ok_or(GraphError::NotFound("node"))?;
        let mut node = self.read_node_at(pointer)?;

        let old_value = match node.properties.remove(key) {
            Some(v) => v,
            None => {
                self.stop_tracking();
                return Ok(());
            }
        };

        let payload = crate::storage::serialize_node(&node)?;
        let record = crate::storage::record::encode_record(
            crate::storage::record::RecordKind::Node,
            &payload,
        )?;

        let update_result = {
            let mut pager_guard = self.pager.write().unwrap();
            let mut store = RecordStore::new(&mut *pager_guard);
            let result = store.update_in_place(pointer, &record)?;
            drop(store);
            drop(pager_guard);
            result
        };

        if let Some(new_pointer) = update_result {
            self.record_page_write(new_pointer.page_id);

            for label in &node.labels {
                self.update_property_index_on_remove(node_id, label, key, &old_value);
            }

            self.node_cache.lock().unwrap().put(node_id, node.clone());

            self.header.last_committed_tx_id = tx_id;
            self.write_header()?;

            let dirty_pages = self.take_recent_dirty_pages();
            self.commit_to_wal(tx_id, &dirty_pages)?;
            self.stop_tracking();

            Ok(())
        } else {
            self.free_record(pointer)?;

            let preferred = self.header.last_record_page;
            let new_pointer = self.insert_record(&record, preferred)?;

            self.node_index.insert(node_id, new_pointer);

            for label in &node.labels {
                self.update_property_index_on_remove(node_id, label, key, &old_value);
            }

            self.node_cache.lock().unwrap().put(node_id, node.clone());

            self.header.last_record_page = Some(new_pointer.page_id);
            self.header.last_committed_tx_id = tx_id;
            self.write_header()?;

            let dirty_pages = self.take_recent_dirty_pages();
            self.commit_to_wal(tx_id, &dirty_pages)?;
            self.stop_tracking();

            Ok(())
        }
    }

    #[allow(dead_code)]
    pub fn set_node_property_internal(
        &mut self,
        node_id: NodeId,
        key: String,
        value: crate::model::PropertyValue,
    ) -> Result<()> {
        if !self.is_in_transaction() {
            return Err(GraphError::InvalidArgument(
                "set_node_property_internal must be called within a transaction".into(),
            ));
        }

        let pointer = self
            .node_index
            .get(&node_id)
            .ok_or(GraphError::NotFound("node"))?;
        let mut node = self.read_node_at(pointer)?;

        let old_value = node.properties.get(&key).cloned();
        node.properties.insert(key.clone(), value.clone());

        let payload = crate::storage::serialize_node(&node)?;
        let record = crate::storage::record::encode_record(
            crate::storage::record::RecordKind::Node,
            &payload,
        )?;

        let update_result = {
            let mut pager_guard = self.pager.write().unwrap();
            let mut store = RecordStore::new(&mut *pager_guard);
            let result = store.update_in_place(pointer, &record)?;
            drop(store);
            drop(pager_guard);
            result
        };

        if let Some(new_pointer) = update_result {
            self.record_page_write(new_pointer.page_id);
        } else {
            self.free_record(pointer)?;

            let preferred = self.header.last_record_page;
            let new_pointer = self.insert_record(&record, preferred)?;
            self.node_index.insert(node_id, new_pointer);
            self.header.last_record_page = Some(new_pointer.page_id);
        }

        if let Some(old_val) = old_value {
            for label in &node.labels {
                self.update_property_index_on_remove(node_id, label, &key, &old_val);
            }
        }

        for label in &node.labels {
            self.update_property_index_on_add(node_id, label, &key, &value);
        }

        self.node_cache.lock().unwrap().put(node_id, node);

        Ok(())
    }

    #[allow(dead_code)]
    pub fn remove_node_property_internal(&mut self, node_id: NodeId, key: &str) -> Result<()> {
        if !self.is_in_transaction() {
            return Err(GraphError::InvalidArgument(
                "remove_node_property_internal must be called within a transaction".into(),
            ));
        }

        let pointer = self
            .node_index
            .get(&node_id)
            .ok_or(GraphError::NotFound("node"))?;
        let mut node = self.read_node_at(pointer)?;

        let old_value = match node.properties.remove(key) {
            Some(v) => v,
            None => return Ok(()),
        };

        let payload = crate::storage::serialize_node(&node)?;
        let record = crate::storage::record::encode_record(
            crate::storage::record::RecordKind::Node,
            &payload,
        )?;

        let update_result = {
            let mut pager_guard = self.pager.write().unwrap();
            let mut store = RecordStore::new(&mut *pager_guard);
            let result = store.update_in_place(pointer, &record)?;
            drop(store);
            drop(pager_guard);
            result
        };

        if let Some(new_pointer) = update_result {
            self.record_page_write(new_pointer.page_id);
        } else {
            self.free_record(pointer)?;

            let preferred = self.header.last_record_page;
            let new_pointer = self.insert_record(&record, preferred)?;
            self.node_index.insert(node_id, new_pointer);
            self.header.last_record_page = Some(new_pointer.page_id);
        }

        for label in &node.labels {
            self.update_property_index_on_remove(node_id, label, key, &old_value);
        }

        self.node_cache.lock().unwrap().put(node_id, node);

        Ok(())
    }
}
