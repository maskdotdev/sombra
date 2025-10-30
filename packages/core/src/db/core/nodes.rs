use super::graphdb::GraphDB;
use crate::error::{GraphError, Result};
use crate::index::VersionedIndexEntries;
use crate::model::{Node, NodeId, NULL_EDGE_ID};
use crate::storage::deserialize_node;
use crate::storage::heap::RecordStore;
use crate::storage::page::RecordPage;
use crate::storage::record::RecordKind;
use crate::storage::serialize_node;
use crate::storage::version_chain::VersionChainReader;
use dashmap::DashMap;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

impl GraphDB {
    pub fn add_node(&mut self, node: Node) -> Result<NodeId> {
        let tx_id = self.allocate_tx_id()?;
        self.start_tracking();

        // Call add_node_internal with tx_id and commit_ts
        // Note: commit_ts is allocated here since this is auto-committed
        let commit_ts = self.timestamp_oracle
            .allocate_commit_timestamp();

        let (node_id, _version_ptr) = self.add_node_internal(node, tx_id, commit_ts)?;

        self.header.lock().unwrap().last_committed_tx_id = tx_id;
        self.write_header()?;

        let dirty_pages = self.take_recent_dirty_pages();
        self.commit_to_wal(tx_id, &dirty_pages)?;
        self.stop_tracking();

        Ok(node_id)
    }

    pub fn add_node_internal(
        &self,
        node: Node,
        tx_id: crate::db::TxId,
        commit_ts: u64,
    ) -> Result<(NodeId, Option<crate::storage::heap::RecordPointer>)> {
        // Detect if this is an update (node has ID and exists) or new node creation
        let is_update = node.id != 0 && self.node_index.get_latest(&node.id).is_some();

        if is_update {
            // Update existing node - create new version in version chain
            self.update_node_version(node, tx_id, commit_ts)
        } else {
            // Create new node - allocate new ID
            self.create_new_node(node, tx_id, commit_ts)
        }
    }

    /// Create a new node with a new ID (not an update)
    fn create_new_node(
        &self,
        mut node: Node,
        tx_id: crate::db::TxId,
        commit_ts: u64,
    ) -> Result<(NodeId, Option<crate::storage::heap::RecordPointer>)> {
        let node_id = {
            let mut header = self.header.lock().unwrap();
            let node_id = header.next_node_id;
            header.next_node_id += 1;
            node_id
        };

        node.id = node_id;
        node.first_outgoing_edge_id = NULL_EDGE_ID;
        node.first_incoming_edge_id = NULL_EDGE_ID;

        let payload = serialize_node(&node)?;

        // Create new version with MVCC support
        use crate::storage::version_chain::store_new_version;

        let (pointer, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
            let mut record_store = RecordStore::new(pager);
            let pointer = store_new_version(
                &mut record_store,
                None, // No previous version
                node_id,
                RecordKind::Node,
                &payload,
                tx_id,
                commit_ts,
                false, // Not deleted
            )?;

            // Collect dirty pages and return them for atomic invalidation
            // Cache invalidation happens atomically while holding the pager write lock,
            // preventing other threads from caching stale data between write and invalidation
            let dirty_pages = record_store.take_dirty_pages();
            Ok((pointer, dirty_pages))
        })?;

        // Track dirty pages for transaction support (cache already invalidated atomically)
        if self.tracking_enabled.load(std::sync::atomic::Ordering::Acquire) {
            let mut recent = self.recent_dirty_pages.lock().unwrap();
            recent.extend(dirty_pages);
        }

        let version_pointer = Some(pointer);

        self.node_index.insert(node_id, pointer);

        // Update label index with versioning support
        for label in &node.labels {
            let label_map = self
                .label_index
                .entry(label.clone())
                .or_insert_with(DashMap::new);
            
            let entries = label_map
                .entry(node_id)
                .or_insert_with(|| Arc::new(Mutex::new(VersionedIndexEntries::new())));
            
            entries.lock().unwrap().add_entry(pointer, commit_ts);
        }

        // This call reads the node via get_node(), which uses fetch_page() and the DashMap cache
        // Cache invalidation MUST happen before this point (done above)
        self.update_property_indexes_on_node_add(node_id)?;
        self.node_cache.put(node_id, node.clone());
        self.header.lock().unwrap().last_record_page = Some(pointer.page_id);

        Ok((node_id, version_pointer))
    }

    /// Update an existing node by creating a new version in the version chain
    fn update_node_version(
        &self,
        node: Node,
        tx_id: crate::db::TxId,
        commit_ts: u64,
    ) -> Result<(NodeId, Option<crate::storage::heap::RecordPointer>)> {
        let node_id = node.id;

        // Get pointer to current version (head of version chain)
        let prev_pointer = self
            .node_index
            .get_latest(&node_id)
            .ok_or(GraphError::NotFound("node"))?;

        // Read the old node version to compute diffs for index updates
        let old_node = self.read_node_at(prev_pointer)?;

        let payload = serialize_node(&node)?;

        // Create new version in version chain
        use crate::storage::version_chain::store_new_version;

        let (new_pointer, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
            let mut record_store = RecordStore::new(pager);
            let new_pointer = store_new_version(
                &mut record_store,
                Some(prev_pointer), // Link to previous version
                node_id,
                RecordKind::Node,
                &payload,
                tx_id,
                commit_ts,
                false, // Not deleted
            )?;

            // Collect dirty pages for atomic cache invalidation
            let dirty_pages = record_store.take_dirty_pages();

            Ok((new_pointer, dirty_pages))
        })?;

        // Track dirty pages for transaction support (cache already invalidated atomically)
        if self.tracking_enabled.load(std::sync::atomic::Ordering::Acquire) {
            let mut recent = self.recent_dirty_pages.lock().unwrap();
            recent.extend(dirty_pages);
        }

        // Update index to point to NEW head of version chain
        self.node_index.insert(node_id, new_pointer);

        // Update label indexes with version-aware logic
        use std::collections::HashSet;
        let old_labels: HashSet<_> = old_node.labels.iter().collect();
        let new_labels: HashSet<_> = node.labels.iter().collect();

        // Mark deleted for labels that are no longer present
        for label in old_labels.difference(&new_labels) {
            if let Some(label_map) = self.label_index.get(*label) {
                if let Some(entries) = label_map.get(&node_id) {
                    entries.lock().unwrap().mark_deleted(commit_ts);
                }
            }
        }

        // Add new entries for labels that were added
        for label in new_labels.difference(&old_labels) {
            let label_map = self
                .label_index
                .entry(label.to_string())
                .or_insert_with(DashMap::new);
            
            let entries = label_map
                .entry(node_id)
                .or_insert_with(|| Arc::new(Mutex::new(VersionedIndexEntries::new())));
            
            entries.lock().unwrap().add_entry(new_pointer, commit_ts);
        }
        
        // For labels that remain, add a new entry (node properties might have changed)
        for label in old_labels.intersection(&new_labels) {
            if let Some(label_map) = self.label_index.get(*label) {
                if let Some(entries) = label_map.get(&node_id) {
                    entries.lock().unwrap().add_entry(new_pointer, commit_ts);
                }
            }
        }

        // Update property indexes: remove old properties, add new properties
        // Use prev_pointer (the old version) when marking property entries as deleted
        for label in &old_node.labels {
            for (property_key, property_value) in &old_node.properties {
                // Only remove if the property changed or label changed
                let should_remove = !node.labels.contains(label)
                    || node.properties.get(property_key) != Some(property_value);

                if should_remove {
                    self.update_property_index_on_remove_with_pointer(
                        node_id,
                        label,
                        property_key,
                        property_value,
                        Some(prev_pointer),
                    );
                }
            }
        }

        // Add new property index entries with the new version's pointer
        for label in &node.labels {
            for (property_key, property_value) in &node.properties {
                self.update_property_index_on_add_with_pointer(
                    node_id,
                    label,
                    property_key,
                    property_value,
                    Some(new_pointer),
                );
            }
        }

        // Update cache with new version
        self.node_cache.put(node_id, node.clone());
        self.header.lock().unwrap().last_record_page = Some(new_pointer.page_id);

        Ok((node_id, Some(new_pointer)))
    }

    pub fn add_nodes_bulk(&mut self, nodes: Vec<Node>) -> Result<Vec<NodeId>> {
        let mut node_ids = Vec::with_capacity(nodes.len());

        // Allocate a single transaction ID for the bulk operation
        let tx_id = self.allocate_tx_id()?;

        // Allocate commit timestamp for MVCC
        let commit_ts = self.timestamp_oracle
            .allocate_commit_timestamp();

        for node in nodes {
            // Use add_node_internal which handles both create and update
            let (node_id, _version_ptr) = self.add_node_internal(node, tx_id, commit_ts)?;
            node_ids.push(node_id);
        }

        Ok(node_ids)
    }

    pub fn delete_nodes_bulk(&mut self, node_ids: &[NodeId]) -> Result<()> {
        let tx_id = self.allocate_tx_id()?;
        let commit_ts = self.timestamp_oracle.allocate_commit_timestamp();
        
        for &node_id in node_ids {
            self.delete_node_internal(node_id, tx_id, commit_ts)?;
        }

        self.node_index.batch_remove(node_ids);
        Ok(())
    }

    pub fn delete_node(&mut self, node_id: NodeId) -> Result<()> {
        let tx_id = self.allocate_tx_id()?;
        let commit_ts = self.timestamp_oracle.allocate_commit_timestamp();
        
        self.delete_node_internal(node_id, tx_id, commit_ts)?;
        Ok(())
    }

    pub fn delete_node_internal(
        &self,
        node_id: NodeId,
        tx_id: crate::db::TxId,
        commit_ts: u64,
    ) -> Result<Option<crate::storage::RecordPointer>> {
        let pointer = self
            .node_index
            .get_latest(&node_id)
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

        // Mark label index entries as deleted (MVCC tombstone approach)
        for label in &node.labels {
            if let Some(label_map) = self.label_index.get(label) {
                // Mark as deleted at current timestamp
                if let Some(entries) = label_map.get(&node_id) {
                    let delete_ts = self
                        .timestamp_oracle
                        .allocate_commit_timestamp();
                    entries.lock().unwrap().mark_deleted(delete_ts);
                }
            }
        }

        self.update_property_indexes_on_node_delete(node_id)?;

        self.node_cache.pop(&node_id);

        // Create a tombstone version to mark the node as deleted (MVCC)
        use crate::storage::version_chain::store_new_version;
        
        let payload = serialize_node(&node)?;
        
        let (tombstone_ptr, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
            let mut record_store = RecordStore::new(pager);
            
            // store_new_version with is_deleted=true creates a tombstone
            // Use tx_id and commit_ts from parameters (commit_ts will be 0 for pending)
            let tombstone_ptr = store_new_version(
                &mut record_store,
                Some(pointer), // Link to previous version
                node_id,
                RecordKind::Node,
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
        
        // Update index to point to tombstone (keeps node in index for snapshot isolation)
        self.node_index.insert(node_id, tombstone_ptr);
        
        Ok(Some(tombstone_ptr))
    }

    pub fn get_node(&self, node_id: NodeId) -> Result<Option<Node>> {
        // TODO: Re-enable metrics with interior mutability (AtomicU64)
        // self.metrics.node_lookups += 1;

        if let Some(node) = self.node_cache.get(&node_id) {
            // TODO: Re-enable metrics with interior mutability (AtomicU64)
            // self.metrics.cache_hits += 1;
            return Ok(Some(node.clone()));
        }

        // TODO: Re-enable metrics with interior mutability (AtomicU64)
        // self.metrics.cache_misses += 1;

        let pointer = match self.node_index.get_latest(&node_id) {
            Some(p) => p,
            None => return Ok(None),
        };
        
        // With MVCC, check if the latest version is a tombstone (deleted)
        let page = self.pager.fetch_page(pointer.page_id)?;
        let mut page_data = page.data.clone();
        let record_page = RecordPage::from_bytes(&mut page_data)?;
        let record = record_page.record_slice(pointer.slot_index as usize)?;
        
        // Check if this is a versioned record
        let kind_byte = record[0];
        let is_versioned = kind_byte == 0x03 || kind_byte == 0x04; // VersionedNode or VersionedEdge
        
        if is_versioned {
            // Read version metadata to check for deletion flag
            use crate::storage::version::VersionMetadata;
            const RECORD_HEADER_SIZE: usize = 8;
            if record.len() < RECORD_HEADER_SIZE + 25 {
                return Err(crate::error::GraphError::Corruption(
                    "versioned record too small".into(),
                ));
            }
            let metadata = VersionMetadata::from_bytes(&record[RECORD_HEADER_SIZE..])?;
            use crate::storage::version::VersionFlags;
            if metadata.flags == VersionFlags::Deleted {
                // Latest version is deleted
                return Ok(None);
            }
        }
        
        let node = self.read_node_at(pointer)?;
        self.node_cache.put(node_id, node.clone());
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
        &self,
        node_id: NodeId,
        snapshot_ts: u64,
        current_tx_id: Option<crate::db::TxId>,
    ) -> Result<Option<Node>> {
        // TODO: Re-enable metrics with interior mutability (AtomicU64)
        // self.metrics.node_lookups += 1;

        // Get all version pointers from the index (latest first)
        let version_pointers = match self.node_index.get(&node_id) {
            Some(pointers) => pointers,
            None => return Ok(None),
        };

        // Use VersionChainReader to find the visible version
        // Try each version in order (latest first) until we find a visible one
        for head_pointer in version_pointers {
            let versioned_record = self.pager.with_pager_write(|pager| {
                let mut record_store = RecordStore::new(pager);
                VersionChainReader::read_version_for_snapshot(
                    &mut record_store,
                    head_pointer,
                    snapshot_ts,
                    current_tx_id,
                )
            })?;

            if let Some(vr) = versioned_record {
                // Check if this version is a tombstone (deleted)
                use crate::storage::version::VersionFlags;
                if vr.metadata.flags == VersionFlags::Deleted {
                    // Record is deleted at this snapshot
                    return Ok(None);
                }
                
                // Deserialize the node from the versioned record data
                let node = deserialize_node(&vr.data)?;
                return Ok(Some(node));
            }
        }

        Ok(None)
    }

    pub fn get_nodes_by_label(&self, label: &str) -> Result<Vec<NodeId>> {
        // TODO: Re-enable metrics with interior mutability (AtomicU64)
        // self.metrics.label_index_queries += 1;
        
        // For non-MVCC mode or when snapshot is not needed, return all node IDs
        // that have at least one visible entry (latest snapshot)
        Ok(self
            .label_index
            .get(label)
            .map(|label_map| {
                label_map
                    .iter()
                    .filter_map(|entry| {
                        let node_id = *entry.key();
                        let entries = entry.value();
                        
                        // Check if there's any active entry (delete_ts = None)
                        let has_active = {
                            let entries_guard = entries.lock().unwrap();
                            // For backward compatibility, use current timestamp
                            let snapshot_ts = self.timestamp_oracle.current_timestamp();
                            entries_guard.is_visible_at(snapshot_ts)
                        };
                        
                        if has_active {
                            Some(node_id)
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default())
    }

    /// Get nodes by label with MVCC snapshot isolation
    ///
    /// This method queries the label index using snapshot isolation,
    /// returning only nodes that had the specified label at the given
    /// snapshot timestamp.
    ///
    /// # Arguments
    /// * `label` - The label to search for
    /// * `snapshot_ts` - Snapshot timestamp for visibility checking
    ///
    /// # Returns
    /// * `Ok(Vec<NodeId>)` - List of node IDs with the label at snapshot time
    /// * `Err(_)` - Error querying the index
    pub fn get_nodes_by_label_with_snapshot(
        &self,
        label: &str,
        snapshot_ts: u64,
    ) -> Result<Vec<NodeId>> {
        Ok(self
            .label_index
            .get(label)
            .map(|label_map| {
                label_map
                    .iter()
                    .filter_map(|entry| {
                        let node_id = *entry.key();
                        let entries = entry.value();
                        
                        // Check if the entry is visible at the snapshot timestamp
                        let is_visible = {
                            let entries_guard = entries.lock().unwrap();
                            entries_guard.is_visible_at(snapshot_ts)
                        };
                        
                        if is_visible {
                            Some(node_id)
                        } else {
                            None
                        }
                    })
                    .collect()
            })
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
        let commit_ts = self.timestamp_oracle.allocate_commit_timestamp();
        self.start_tracking();

        let pointer = self
            .node_index
            .get_latest(&node_id)
            .ok_or(GraphError::NotFound("node"))?;
        let mut node = self.read_node_at(pointer)?;

        let old_value = node.properties.get(&key).cloned();
        node.properties.insert(key.clone(), value.clone());

        let payload = crate::storage::serialize_node(&node)?;

        // Use MVCC version chain instead of update_in_place
        use crate::storage::version_chain::store_new_version;
        
        let (new_pointer, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
            let mut store = RecordStore::new(pager);
            let new_pointer = store_new_version(
                &mut store,
                Some(pointer), // Link to previous version
                node_id,
                crate::storage::record::RecordKind::Node,
                &payload,
                tx_id,
                commit_ts,
                false, // Not deleted
            )?;
            let dirty = store.take_dirty_pages();
            Ok((new_pointer, dirty))
        })?;

        // Update node index to point to new version
        self.node_index.insert(node_id, new_pointer);

        // Cache already invalidated atomically during write
        if self.tracking_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            self.recent_dirty_pages.lock().unwrap().extend(dirty_pages);
        }

        // Update property indexes
        if let Some(old) = old_value {
            for label in &node.labels {
                self.update_property_index_on_remove(node_id, label, &key, &old);
                self.update_property_index_on_add(node_id, label, &key, &value);
            }
        } else {
            for label in &node.labels {
                self.update_property_index_on_add(node_id, label, &key, &value);
            }
        }

        self.node_cache.put(node_id, node);

        // Auto-commit: write header, commit to WAL, stop tracking
        self.header.lock().unwrap().last_committed_tx_id = tx_id;
        self.write_header()?;

        let dirty_pages = self.take_recent_dirty_pages();
        self.commit_to_wal(tx_id, &dirty_pages)?;
        self.stop_tracking();

        Ok(())
    }

    #[allow(dead_code)]
    pub fn remove_node_property_internal(&self, node_id: NodeId, key: &str) -> Result<()> {
        let tx_id = self.allocate_tx_id()?;
        let commit_ts = self.timestamp_oracle.allocate_commit_timestamp();
        
        let pointer = self
            .node_index
            .get_latest(&node_id)
            .ok_or(GraphError::NotFound("node"))?;
        let mut node = self.read_node_at(pointer)?;

        let old_value = match node.properties.remove(key) {
            Some(v) => v,
            None => return Ok(()),
        };

        let payload = crate::storage::serialize_node(&node)?;

        // Use MVCC version chain instead of update_in_place
        use crate::storage::version_chain::store_new_version;
        
        let (new_pointer, dirty_pages) = self.pager.with_pager_write_and_invalidate(|pager| {
            let mut store = RecordStore::new(pager);
            let new_pointer = store_new_version(
                &mut store,
                Some(pointer), // Link to previous version
                node_id,
                crate::storage::record::RecordKind::Node,
                &payload,
                tx_id,
                commit_ts,
                false, // Not deleted
            )?;
            let dirty = store.take_dirty_pages();
            Ok((new_pointer, dirty))
        })?;

        // Update node index to point to new version
        self.node_index.insert(node_id, new_pointer);

        // Cache already invalidated atomically during write
        if self.tracking_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            self.recent_dirty_pages.lock().unwrap().extend(dirty_pages);
        }

        // Remove from property index
        for label in &node.labels {
            self.update_property_index_on_remove(node_id, label, key, &old_value);
        }

        self.node_cache.put(node_id, node);

        Ok(())
    }
}
