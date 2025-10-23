use super::graphdb::GraphDB;
use crate::error::{GraphError, Result};
use crate::model::{Node, NodeId, NULL_EDGE_ID};
use crate::storage::record::{encode_record, RecordKind};
use crate::storage::serialize_node;
use std::collections::HashSet;

impl GraphDB {
    pub fn add_node(&mut self, mut node: Node) -> Result<NodeId> {
        if self.is_in_transaction() {
            return Err(GraphError::InvalidArgument(
                "add_node must be called through a transaction when in transaction context".into(),
            ));
        }

        let tx_id = self.allocate_tx_id()?;
        self.start_tracking();

        let node_id = self.header.next_node_id;
        self.header.next_node_id += 1;

        node.id = node_id;
        node.first_outgoing_edge_id = NULL_EDGE_ID;
        node.first_incoming_edge_id = NULL_EDGE_ID;

        let payload = serialize_node(&node)?;
        let record = encode_record(RecordKind::Node, &payload)?;

        let preferred = self.header.last_record_page;
        let pointer = self.insert_record(&record, preferred)?;

        self.node_index.insert(node_id, pointer);

        for label in &node.labels {
            self.label_index
                .entry(label.clone())
                .or_default()
                .insert(node_id);
        }

        self.update_property_indexes_on_node_add(node_id)?;

        self.node_cache.put(node_id, node.clone());

        self.header.last_record_page = Some(pointer.page_id);
        self.header.last_committed_tx_id = tx_id;
        self.write_header()?;

        let dirty_pages = self.take_recent_dirty_pages();
        self.commit_to_wal(tx_id, &dirty_pages)?;
        self.stop_tracking();

        Ok(node_id)
    }

    pub fn add_node_internal(&mut self, mut node: Node) -> Result<NodeId> {
        let node_id = self.header.next_node_id;
        self.header.next_node_id += 1;

        node.id = node_id;
        node.first_outgoing_edge_id = NULL_EDGE_ID;
        node.first_incoming_edge_id = NULL_EDGE_ID;

        let payload = serialize_node(&node)?;
        let record = encode_record(RecordKind::Node, &payload)?;

        let preferred = self.header.last_record_page;
        let pointer = self.insert_record(&record, preferred)?;

        self.node_index.insert(node_id, pointer);

        for label in &node.labels {
            self.label_index
                .entry(label.clone())
                .or_default()
                .insert(node_id);
        }

        self.update_property_indexes_on_node_add(node_id)?;

        self.node_cache.put(node_id, node.clone());

        self.header.last_record_page = Some(pointer.page_id);
        Ok(node_id)
    }

    pub fn add_nodes_bulk(&mut self, nodes: Vec<Node>) -> Result<Vec<NodeId>> {
        let mut node_ids = Vec::with_capacity(nodes.len());
        let mut index_entries = Vec::with_capacity(nodes.len());

        for mut node in nodes {
            let node_id = self.header.next_node_id;
            self.header.next_node_id += 1;

            node.id = node_id;
            node.first_outgoing_edge_id = NULL_EDGE_ID;
            node.first_incoming_edge_id = NULL_EDGE_ID;

            let payload = serialize_node(&node)?;
            let record = encode_record(RecordKind::Node, &payload)?;

            let preferred = self.header.last_record_page;
            let pointer = self.insert_record(&record, preferred)?;

            index_entries.push((node_id, pointer));

            for label in &node.labels {
                self.label_index
                    .entry(label.clone())
                    .or_default()
                    .insert(node_id);
            }

            self.update_property_indexes_on_node_add(node_id)?;

            self.node_cache.put(node_id, node.clone());
            self.header.last_record_page = Some(pointer.page_id);
            node_ids.push(node_id);
        }

        self.node_index.batch_insert(index_entries);
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
        if self.is_in_transaction() {
            return Err(GraphError::InvalidArgument(
                "delete_node must be called through a transaction when in transaction context"
                    .into(),
            ));
        }

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

        self.node_cache.pop(&node_id);

        self.node_index.remove(&node_id);
        self.free_record(pointer)?;
        Ok(())
    }

    pub fn get_node(&mut self, node_id: NodeId) -> Result<Node> {
        self.metrics.node_lookups += 1;

        if let Some(node) = self.node_cache.get(&node_id) {
            self.metrics.cache_hits += 1;
            return Ok(node.clone());
        }

        self.metrics.cache_misses += 1;

        let pointer = self
            .node_index
            .get(&node_id)
            .ok_or(GraphError::NotFound("node"))?;
        let node = self.read_node_at(pointer)?;
        self.node_cache.put(node_id, node.clone());
        Ok(node)
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
        if self.is_in_transaction() {
            return Err(GraphError::InvalidArgument(
                "set_node_property must be called through a transaction when in transaction context"
                    .into(),
            ));
        }

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

        let mut store = self.record_store();
        if let Some(new_pointer) = store.update_in_place(pointer, &record)? {
            self.record_page_write(new_pointer.page_id);

            if let Some(old_val) = old_value {
                for label in &node.labels {
                    self.update_property_index_on_remove(node_id, label, &key, &old_val);
                }
            }

            for label in &node.labels {
                self.update_property_index_on_add(node_id, label, &key, &value);
            }

            self.node_cache.put(node_id, node.clone());

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

            self.node_cache.put(node_id, node.clone());

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
        if self.is_in_transaction() {
            return Err(GraphError::InvalidArgument(
                "remove_node_property must be called through a transaction when in transaction context"
                    .into(),
            ));
        }

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

        let mut store = self.record_store();
        if let Some(new_pointer) = store.update_in_place(pointer, &record)? {
            self.record_page_write(new_pointer.page_id);

            for label in &node.labels {
                self.update_property_index_on_remove(node_id, label, key, &old_value);
            }

            self.node_cache.put(node_id, node.clone());

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

            self.node_cache.put(node_id, node.clone());

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

        let mut store = self.record_store();
        if let Some(new_pointer) = store.update_in_place(pointer, &record)? {
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

        self.node_cache.put(node_id, node);

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

        let mut store = self.record_store();
        if let Some(new_pointer) = store.update_in_place(pointer, &record)? {
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

        self.node_cache.put(node_id, node);

        Ok(())
    }
}
