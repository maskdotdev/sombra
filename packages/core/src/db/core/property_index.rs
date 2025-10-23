use super::graphdb::{GraphDB, IndexableValue};
use crate::error::{GraphError, Result};
use crate::model::{NodeId, PropertyValue};
use std::collections::BTreeSet;
use std::ops::RangeBounds;

impl GraphDB {
    pub fn create_property_index(&mut self, label: &str, property_key: &str) -> Result<()> {
        if self.is_in_transaction() {
            return Err(GraphError::InvalidArgument(
                "create_property_index must be called through a transaction when in transaction context".into(),
            ));
        }

        let key = (label.to_string(), property_key.to_string());

        if self.property_indexes.contains_key(&key) {
            return Ok(());
        }

        let mut index = std::collections::BTreeMap::new();

        let node_ids: Vec<NodeId> = self
            .label_index
            .get(label)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();

        for node_id in node_ids {
            let node = self.get_node(node_id)?;
            if let Some(prop_value) = node.properties.get(property_key) {
                if let Some(indexable_value) = Option::<IndexableValue>::from(prop_value) {
                    index
                        .entry(indexable_value)
                        .or_insert_with(BTreeSet::new)
                        .insert(node_id);
                }
            }
        }

        self.property_indexes.insert(key, index);
        Ok(())
    }

    pub fn drop_property_index(&mut self, label: &str, property_key: &str) -> Result<()> {
        if self.is_in_transaction() {
            return Err(GraphError::InvalidArgument(
                "drop_property_index must be called through a transaction when in transaction context".into(),
            ));
        }

        let key = (label.to_string(), property_key.to_string());
        self.property_indexes.remove(&key);
        Ok(())
    }

    pub fn find_nodes_by_property(
        &mut self,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
    ) -> Result<Vec<NodeId>> {
        let key = (label.to_string(), property_key.to_string());

        if let Some(index) = self.property_indexes.get(&key) {
            if let Some(indexable_value) = Option::<IndexableValue>::from(value) {
                if let Some(node_ids) = index.get(&indexable_value) {
                    self.metrics.record_property_index_hit();
                    return Ok(node_ids.iter().copied().collect());
                } else {
                    self.metrics.record_property_index_hit();
                    return Ok(Vec::new());
                }
            }
        }

        self.metrics.record_property_index_miss();
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
            for (_, node_ids) in index.range(range) {
                results.extend(node_ids.iter().copied());
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
        &mut self,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
    ) -> Result<Vec<NodeId>> {
        let mut results = Vec::new();

        let node_ids: Vec<NodeId> = self
            .label_index
            .get(label)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();

        for node_id in node_ids {
            let node = self.get_node(node_id)?;
            if let Some(prop_value) = node.properties.get(property_key) {
                if prop_value == value {
                    results.push(node_id);
                }
            }
        }

        Ok(results)
    }

    pub(crate) fn update_property_index_on_add(
        &mut self,
        node_id: NodeId,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
    ) {
        let key = (label.to_string(), property_key.to_string());

        if let Some(index) = self.property_indexes.get_mut(&key) {
            if let Some(indexable_value) = Option::<IndexableValue>::from(value) {
                index
                    .entry(indexable_value)
                    .or_insert_with(BTreeSet::new)
                    .insert(node_id);
            }
        }
    }

    pub(crate) fn update_property_index_on_remove(
        &mut self,
        node_id: NodeId,
        label: &str,
        property_key: &str,
        value: &PropertyValue,
    ) {
        let key = (label.to_string(), property_key.to_string());

        if let Some(index) = self.property_indexes.get_mut(&key) {
            if let Some(indexable_value) = Option::<IndexableValue>::from(value) {
                if let Some(node_set) = index.get_mut(&indexable_value) {
                    node_set.remove(&node_id);
                    if node_set.is_empty() {
                        index.remove(&indexable_value);
                    }
                }
            }
        }
    }

    pub(crate) fn update_property_indexes_on_node_add(&mut self, node_id: NodeId) -> Result<()> {
        let node = self.get_node(node_id)?;

        for label in &node.labels {
            for (property_key, property_value) in &node.properties {
                self.update_property_index_on_add(node_id, label, property_key, property_value);
            }
        }

        Ok(())
    }

    pub(crate) fn update_property_indexes_on_node_delete(&mut self, node_id: NodeId) -> Result<()> {
        let node = self.get_node(node_id)?;

        for label in &node.labels {
            for (property_key, property_value) in &node.properties {
                self.update_property_index_on_remove(node_id, label, property_key, property_value);
            }
        }

        Ok(())
    }
}
