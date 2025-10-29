use super::graphdb::{GraphDB, IndexableValue};
use crate::error::{GraphError, Result};
use crate::index::VersionedIndexEntries;
use crate::model::{NodeId, PropertyValue};
use crate::storage::RecordPointer;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

impl GraphDB {
    pub fn create_property_index(&mut self, label: &str, property_key: &str) -> Result<()> {
        let key = (label.to_string(), property_key.to_string());

        if self.property_indexes.contains_key(&key) {
            return Ok(());
        }

        let index = dashmap::DashMap::new();

        let node_ids: Vec<NodeId> = self
            .label_index
            .get(label)
            .map(|label_map| {
                label_map
                    .iter()
                    .map(|entry| *entry.key())
                    .collect()
            })
            .unwrap_or_default();

        for node_id in node_ids {
            if let Some(node) = self.get_node(node_id)? {
                if let Some(prop_value) = node.properties.get(property_key) {
                    if let Some(indexable_value) = Option::<IndexableValue>::from(prop_value) {
                        // Get the node's record pointer for version tracking (latest version)
                        if let Some(pointer) = self.node_index.get_latest(&node_id) {
                            let mut entries = VersionedIndexEntries::new();
                            // For existing nodes, we use commit_ts=1 (bootstrap)
                            entries.add_entry(pointer, 1);
                            index
                                .entry(indexable_value)
                                .or_insert_with(|| Arc::new(Mutex::new(VersionedIndexEntries::new())))
                                .lock()
                                .unwrap()
                                .add_entry(pointer, 1);
                        }
                    }
                }
            }
        }

        self.property_indexes.insert(key, Arc::new(index));
        Ok(())
    }

    /// Creates a property index using interior mutability (for concurrent access).
    ///
    /// This version doesn't require &mut self and can be called from ConcurrentGraphDB.
    pub fn create_property_index_concurrent(&self, label: &str, property_key: &str) -> Result<()> {
        let key = (label.to_string(), property_key.to_string());

        if self.property_indexes.contains_key(&key) {
            return Ok(());
        }

        let index = dashmap::DashMap::new();

        let node_ids: Vec<NodeId> = self
            .label_index
            .get(label)
            .map(|label_map| {
                label_map
                    .iter()
                    .map(|entry| *entry.key())
                    .collect()
            })
            .unwrap_or_default();

        for node_id in node_ids {
            if let Some(node) = self.get_node(node_id)? {
                if let Some(prop_value) = node.properties.get(property_key) {
                    if let Some(indexable_value) = Option::<IndexableValue>::from(prop_value) {
                        // Get the node's record pointer for version tracking (latest version)
                        if let Some(pointer) = self.node_index.get_latest(&node_id) {
                            let mut entries = VersionedIndexEntries::new();
                            // For existing nodes, we use commit_ts=1 (bootstrap)
                            entries.add_entry(pointer, 1);
                            index
                                .entry(indexable_value)
                                .or_insert_with(|| Arc::new(Mutex::new(VersionedIndexEntries::new())))
                                .lock()
                                .unwrap()
                                .add_entry(pointer, 1);
                        }
                    }
                }
            }
        }

        self.property_indexes.insert(key, Arc::new(index));
        Ok(())
    }

    pub fn drop_property_index(&mut self, label: &str, property_key: &str) -> Result<()> {
        let key = (label.to_string(), property_key.to_string());
        self.property_indexes.remove(&key);
        Ok(())
    }

    pub fn find_nodes_by_property(
        &self,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
    ) -> Result<Vec<NodeId>> {
        let key = (label.to_string(), property_key.to_string());

        if let Some(index) = self.property_indexes.get(&key) {
            if let Some(indexable_value) = Option::<IndexableValue>::from(value) {
                if let Some(entries_arc) = index.get(&indexable_value) {
                    // TODO: Re-enable metrics with interior mutability (AtomicU64)
                    // self.metrics.record_property_index_hit();
                    
                    // Lock the VersionedIndexEntries and get all node IDs
                    let entries = entries_arc.lock().unwrap();
                    let node_ids: Vec<NodeId> = entries
                        .entries()
                        .iter()
                        .filter_map(|entry| {
                            // Convert RecordPointer to NodeId via reverse lookup
                            self.node_index.find_by_pointer(entry.pointer)
                        })
                        .collect();
                    return Ok(node_ids);
                } else {
                    // TODO: Re-enable metrics with interior mutability (AtomicU64)
                    // self.metrics.record_property_index_hit();
                    return Ok(Vec::new());
                }
            }
        }

        // TODO: Re-enable metrics with interior mutability (AtomicU64)
        // self.metrics.record_property_index_miss();
        self.scan_nodes_by_property(label, property_key, value)
    }

    /// Find nodes by property with snapshot isolation.
    ///
    /// This method filters results based on the snapshot timestamp, ensuring
    /// that only versions visible at the given snapshot are returned.
    ///
    /// # Arguments
    /// * `label` - The node label
    /// * `property_key` - The property key
    /// * `value` - The property value to match
    /// * `snapshot_ts` - The snapshot timestamp for visibility checking
    ///
    /// # Returns
    /// * `Ok(Vec<NodeId>)` - List of node IDs with the property at snapshot time
    /// * `Err(_)` - Error querying the index
    pub fn find_nodes_by_property_with_snapshot(
        &self,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
        snapshot_ts: u64,
    ) -> Result<Vec<NodeId>> {
        
        // If MVCC is not enabled, fall back to regular find_nodes_by_property
        if !self.config.mvcc_enabled {
            return self.find_nodes_by_property(label, property_key, value);
        }

        let key = (label.to_string(), property_key.to_string());

        if let Some(index) = self.property_indexes.get(&key) {
            if let Some(indexable_value) = Option::<IndexableValue>::from(value) {
                if let Some(entries_arc) = index.get(&indexable_value) {
                    // TODO: Re-enable metrics with interior mutability (AtomicU64)
                    // self.metrics.record_property_index_hit();
                    
                    let entries = entries_arc.lock().unwrap();
                    
                    // Filter by snapshot visibility
                    let mut node_ids: Vec<NodeId> = entries
                        .entries()
                        .iter()
                        .filter(|entry| {
                            // Check if entry is visible at snapshot_ts
                            entry.commit_ts <= snapshot_ts && 
                                (entry.delete_ts.is_none() || entry.delete_ts.unwrap() > snapshot_ts)
                        })
                        .filter_map(|entry| {
                            // Convert RecordPointer to NodeId via reverse lookup
                            self.node_index.find_by_pointer(entry.pointer)
                        })
                        .collect();
                    
                    // Deduplicate node IDs - multiple version pointers may map to same node
                    node_ids.sort_unstable();
                    node_ids.dedup();
                    
                    return Ok(node_ids);
                } else {
                    // TODO: Re-enable metrics with interior mutability (AtomicU64)
                    // self.metrics.record_property_index_hit();
                    return Ok(Vec::new());
                }
            }
        }

        // TODO: Re-enable metrics with interior mutability (AtomicU64)
        // self.metrics.record_property_index_miss();
        self.scan_nodes_by_property(label, property_key, value)
    }

    pub fn find_nodes_by_property_range<R>(
        &mut self,
        label: &str,
        property_key: &str,
        range: R,
    ) -> Result<Vec<NodeId>>
    where
        R: RangeBounds<IndexableValue>,
    {
        let key = (label.to_string(), property_key.to_string());

        if let Some(index) = self.property_indexes.get(&key) {
            let mut results = Vec::new();
            // DashMap doesn't support range queries, so we need to iterate all entries and filter
            for entry in index.iter() {
                if range.contains(entry.key()) {
                    // Collect all node IDs from the versioned entries
                    let entries = entry.value().lock().unwrap();
                    let node_ids: Vec<NodeId> = entries
                        .entries()
                        .iter()
                        .filter_map(|entry| {
                            // Convert RecordPointer to NodeId via reverse lookup
                            self.node_index.find_by_pointer(entry.pointer)
                        })
                        .collect();
                    results.extend(node_ids);
                }
            }
            self.metrics.record_property_index_hit();
            return Ok(results);
        }

        self.metrics.record_property_index_miss();
        Err(GraphError::InvalidArgument(format!(
            "No property index exists for {label}.{property_key}"
        )))
    }

    fn scan_nodes_by_property(
        &self,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
    ) -> Result<Vec<NodeId>> {
        let mut results = Vec::new();

        let node_ids: Vec<NodeId> = self
            .label_index
            .get(label)
            .map(|label_map| {
                label_map
                    .iter()
                    .map(|entry| *entry.key())
                    .collect()
            })
            .unwrap_or_default();

        for node_id in node_ids {
            if let Some(node) = self.get_node(node_id)? {
                if let Some(prop_value) = node.properties.get(property_key) {
                    if prop_value == value {
                        results.push(node_id);
                    }
                }
            }
        }

        Ok(results)
    }

    pub(crate) fn update_property_index_on_add(
        &self,
        node_id: NodeId,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
    ) {
        self.update_property_index_on_add_with_pointer(node_id, label, property_key, value, None);
    }

    pub(crate) fn update_property_index_on_add_with_pointer(
        &self,
        node_id: NodeId,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
        pointer_opt: Option<crate::storage::heap::RecordPointer>,
    ) {
        let key = (label.to_string(), property_key.to_string());

        if let Some(index) = self.property_indexes.get(&key) {
            if let Some(indexable_value) = Option::<IndexableValue>::from(value) {
                // Use provided pointer or get the node's latest record pointer
                let pointer = pointer_opt.or_else(|| self.node_index.get_latest(&node_id));
                
                if let Some(ptr) = pointer {
                    index
                        .entry(indexable_value)
                        .or_insert_with(|| Arc::new(Mutex::new(VersionedIndexEntries::new())))
                        .lock()
                        .unwrap()
                        .add_entry(ptr, 0); // commit_ts=0 will be updated at commit time
                }
            }
        }
    }

    pub(crate) fn update_property_index_on_remove(
        &self,
        node_id: NodeId,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
    ) {
        self.update_property_index_on_remove_with_pointer(node_id, label, property_key, value, None);
    }

    pub(crate) fn update_property_index_on_remove_with_pointer(
        &self,
        node_id: NodeId,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
        pointer_opt: Option<crate::storage::heap::RecordPointer>,
    ) {
        let key = (label.to_string(), property_key.to_string());

        if let Some(index) = self.property_indexes.get(&key) {
            if let Some(indexable_value) = Option::<IndexableValue>::from(value) {
                // Use provided pointer or get the node's latest record pointer
                let pointer = pointer_opt.or_else(|| self.node_index.get_latest(&node_id));
                
                if let Some(ptr) = pointer {
                    if let Some(entries_arc) = index.get(&indexable_value) {
                        let mut entries = entries_arc.lock().unwrap();
                        
                        // Mark as deleted with delete_ts=0 (will be updated at commit time)
                        let _updated = entries.update_delete_ts_for_pointer(ptr, 0);
                    }
                }
            }
        }
    }

    pub(crate) fn update_property_indexes_on_node_add(&self, node_id: NodeId) -> Result<()> {
        let node = self
            .get_node(node_id)?
            .ok_or(GraphError::NotFound("node"))?;

        for label in &node.labels {
            for (property_key, property_value) in &node.properties {
                self.update_property_index_on_add(node_id, label, property_key, property_value);
            }
        }

        Ok(())
    }

    pub(crate) fn update_property_indexes_on_node_delete(&self, node_id: NodeId) -> Result<()> {
        let node = self
            .get_node(node_id)?
            .ok_or(GraphError::NotFound("node"))?;

        // Get ALL version pointers for this node
        // When a node is deleted, we need to mark ALL its version pointers as deleted
        // in the property indexes, not just the latest one
        let all_pointers = self.node_index.get(&node_id).unwrap_or_default();

        for label in &node.labels {
            for (property_key, property_value) in &node.properties {
                // Mark all version pointers as deleted
                for pointer in &all_pointers {
                    self.update_property_index_on_remove_with_pointer(
                        node_id, 
                        label, 
                        property_key, 
                        property_value, 
                        Some(*pointer)
                    );
                }
            }
        }

        Ok(())
    }

    /// Updates property index commit timestamps for a node.
    /// This should be called at transaction commit time to update entries from commit_ts=0 to actual commit_ts.
    pub(crate) fn update_property_index_commit_ts(&self, node_id: NodeId, commit_ts: u64) -> Result<()> {
        // Get the node's pointer
        let pointer = self
            .node_index
            .get_latest(&node_id)
            .ok_or(GraphError::NotFound("node"))?;

        // Iterate through all property indexes and update commit_ts for matching pointers
        for entry in self.property_indexes.iter() {
            let value_map = entry.value();
            for value_entry in value_map.iter() {
                let entries_arc = value_entry.value();
                let mut entries = entries_arc.lock().unwrap();
                // Update commit_ts for any entry with this pointer that has commit_ts=0
                entries.update_commit_ts_for_pointer(pointer, commit_ts);
            }
        }

        Ok(())
    }

    /// Updates property index delete timestamps for a deleted node using the node's pointer.
    /// This should be called at transaction commit time with the actual commit timestamp.
    /// Uses the pointer directly since the node may have been removed from node_index.
    pub(crate) fn update_property_index_delete_ts_by_pointer(&self, pointer: RecordPointer, delete_ts: u64) -> Result<()> {
        // Iterate through all property indexes and update matching pointers
        for entry in self.property_indexes.iter() {
            let value_map = entry.value();
            for value_entry in value_map.iter() {
                let entries_arc = value_entry.value();
                let mut entries = entries_arc.lock().unwrap();
                // Update delete_ts for any entry with this pointer that has delete_ts=0
                entries.update_delete_ts_for_pointer(pointer, delete_ts);
            }
        }

        Ok(())
    }

    /// Updates property index delete timestamps for a deleted node.
    /// This should be called at transaction commit time with the actual commit timestamp.
    pub(crate) fn update_property_index_delete_ts(&self, node_id: NodeId, delete_ts: u64) -> Result<()> {
        // Get the node's pointer - it might still be in the index even though marked as deleted
        let pointer = self
            .node_index
            .get_latest(&node_id)
            .ok_or(GraphError::NotFound("node"))?;

        // Try to get the node to access its labels and properties
        // The node might already be deleted from the heap, so we need to handle that
        // For now, we iterate through all property indexes and update matching pointers
        for entry in self.property_indexes.iter() {
            let value_map = entry.value();
            for value_entry in value_map.iter() {
                let entries_arc = value_entry.value();
                let mut entries = entries_arc.lock().unwrap();
                // Update delete_ts for any entry with this pointer that has delete_ts=0
                entries.update_delete_ts_for_pointer(pointer, delete_ts);
            }
        }

        Ok(())
    }
}
