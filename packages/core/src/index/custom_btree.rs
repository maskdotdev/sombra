use crate::error::{GraphError, Result};
use crate::model::NodeId;
use crate::storage::RecordPointer;
use std::convert::TryInto;

const NODE_SIZE: usize = 256;
const MIN_KEYS: usize = NODE_SIZE / 2;

#[derive(Debug, Clone)]
struct BTreeNode {
    keys: Vec<NodeId>,
    values: Vec<RecordPointer>,
    #[allow(clippy::vec_box)]
    children: Vec<Box<BTreeNode>>,
    is_leaf: bool,
}

impl BTreeNode {
    fn new(is_leaf: bool) -> Self {
        Self {
            keys: Vec::with_capacity(NODE_SIZE),
            values: Vec::with_capacity(NODE_SIZE),
            children: if is_leaf {
                Vec::new()
            } else {
                Vec::with_capacity(NODE_SIZE + 1)
            },
            is_leaf,
        }
    }

    fn is_full(&self) -> bool {
        self.keys.len() >= NODE_SIZE
    }

    fn search(&self, key: &NodeId) -> Option<&RecordPointer> {
        match self.keys.binary_search(key) {
            Ok(idx) => Some(&self.values[idx]),
            Err(idx) => {
                if self.is_leaf {
                    None
                } else if idx < self.children.len() {
                    self.children[idx].search(key)
                } else {
                    None
                }
            }
        }
    }

    fn insert_non_full(&mut self, key: NodeId, value: RecordPointer) {
        if self.is_leaf {
            match self.keys.binary_search(&key) {
                Ok(idx) => {
                    self.values[idx] = value;
                }
                Err(idx) => {
                    self.keys.insert(idx, key);
                    self.values.insert(idx, value);
                }
            }
        } else {
            let idx = match self.keys.binary_search(&key) {
                Ok(idx) => {
                    self.values[idx] = value;
                    return;
                }
                Err(idx) => idx,
            };

            if self.children[idx].is_full() {
                self.split_child(idx);
                if key > self.keys[idx] {
                    self.children[idx + 1].insert_non_full(key, value);
                } else if key == self.keys[idx] {
                    self.values[idx] = value;
                } else {
                    self.children[idx].insert_non_full(key, value);
                }
            } else {
                self.children[idx].insert_non_full(key, value);
            }
        }
    }

    fn split_child(&mut self, idx: usize) {
        let full_child = &mut self.children[idx];
        let mut new_child = BTreeNode::new(full_child.is_leaf);

        let mid = MIN_KEYS;
        let mid_key = full_child.keys[mid];
        let mid_value = full_child.values[mid];

        new_child.keys = full_child.keys.split_off(mid + 1);
        new_child.values = full_child.values.split_off(mid + 1);
        full_child.keys.pop();
        full_child.values.pop();

        if !full_child.is_leaf {
            new_child.children = full_child.children.split_off(mid + 1);
        }

        self.keys.insert(idx, mid_key);
        self.values.insert(idx, mid_value);
        self.children.insert(idx + 1, Box::new(new_child));
    }

    fn remove(&mut self, key: &NodeId) -> Option<RecordPointer> {
        match self.keys.binary_search(key) {
            Ok(idx) => {
                if self.is_leaf {
                    self.keys.remove(idx);
                    Some(self.values.remove(idx))
                } else {
                    let left_child = &self.children[idx];
                    let right_child = &self.children[idx + 1];

                    if left_child.keys.len() > MIN_KEYS {
                        let pred = self.get_predecessor(idx);
                        let old_value = self.values[idx];
                        self.keys[idx] = pred.0;
                        self.values[idx] = pred.1;
                        self.children[idx].remove(&pred.0);
                        Some(old_value)
                    } else if right_child.keys.len() > MIN_KEYS {
                        let succ = self.get_successor(idx);
                        let old_value = self.values[idx];
                        self.keys[idx] = succ.0;
                        self.values[idx] = succ.1;
                        self.children[idx + 1].remove(&succ.0);
                        Some(old_value)
                    } else {
                        self.merge_children(idx);
                        self.children[idx].remove(key)
                    }
                }
            }
            Err(idx) => {
                if self.is_leaf {
                    None
                } else if idx < self.children.len() {
                    if self.children[idx].keys.len() <= MIN_KEYS {
                        self.fix_child(idx);
                    }

                    let search_idx = match self.keys.binary_search(key) {
                        Ok(i) => i + 1,
                        Err(i) => i,
                    };

                    if search_idx < self.children.len() {
                        self.children[search_idx].remove(key)
                    } else {
                        None
                    }
                } else {
                    None
                }
            }
        }
    }

    fn get_predecessor(&self, idx: usize) -> (NodeId, RecordPointer) {
        let mut current = &self.children[idx];
        while !current.is_leaf {
            current = &current.children[current.children.len() - 1];
        }
        let last_idx = current.keys.len() - 1;
        (current.keys[last_idx], current.values[last_idx])
    }

    fn get_successor(&self, idx: usize) -> (NodeId, RecordPointer) {
        let mut current = &self.children[idx + 1];
        while !current.is_leaf {
            current = &current.children[0];
        }
        (current.keys[0], current.values[0])
    }

    fn merge_children(&mut self, idx: usize) {
        let key = self.keys.remove(idx);
        let value = self.values.remove(idx);

        let right_child = self.children.remove(idx + 1);
        let left_child = &mut self.children[idx];

        left_child.keys.push(key);
        left_child.values.push(value);
        left_child.keys.extend_from_slice(&right_child.keys);
        left_child.values.extend_from_slice(&right_child.values);

        if !left_child.is_leaf {
            left_child.children.extend(right_child.children);
        }
    }

    fn fix_child(&mut self, idx: usize) {
        if idx > 0 && self.children[idx - 1].keys.len() > MIN_KEYS {
            self.borrow_from_left(idx);
        } else if idx < self.children.len() - 1 && self.children[idx + 1].keys.len() > MIN_KEYS {
            self.borrow_from_right(idx);
        } else if idx > 0 {
            self.merge_children(idx - 1);
        } else {
            self.merge_children(idx);
        }
    }

    fn borrow_from_left(&mut self, idx: usize) {
        if idx == 0 {
            return;
        }

        let parent_key = match self.keys.get(idx - 1).copied() {
            Some(key) => key,
            None => return,
        };
        let parent_value = match self.values.get(idx - 1).copied() {
            Some(value) => value,
            None => return,
        };

        let (left_part, right_part) = self.children.split_at_mut(idx);
        let left_sibling = match left_part.get_mut(idx - 1) {
            Some(node) => node,
            None => return,
        };
        let child = match right_part.get_mut(0) {
            Some(node) => node,
            None => return,
        };

        if left_sibling.keys.is_empty() || left_sibling.values.is_empty() {
            return;
        }

        let borrowed_key = match left_sibling.keys.pop() {
            Some(key) => key,
            None => return,
        };
        let borrowed_value = match left_sibling.values.pop() {
            Some(value) => value,
            None => return,
        };
        let borrowed_child = if !child.is_leaf {
            left_sibling.children.pop()
        } else {
            None
        };

        child.keys.insert(0, parent_key);
        child.values.insert(0, parent_value);

        if let Some(parent_key_slot) = self.keys.get_mut(idx - 1) {
            *parent_key_slot = borrowed_key;
        }
        if let Some(parent_value_slot) = self.values.get_mut(idx - 1) {
            *parent_value_slot = borrowed_value;
        }

        if let Some(borrowed_child) = borrowed_child {
            child.children.insert(0, borrowed_child);
        }
    }

    fn borrow_from_right(&mut self, idx: usize) {
        if idx >= self.keys.len() {
            return;
        }

        let parent_key = match self.keys.get(idx).copied() {
            Some(key) => key,
            None => return,
        };
        let parent_value = match self.values.get(idx).copied() {
            Some(value) => value,
            None => return,
        };

        let (left_part, right_part) = self.children.split_at_mut(idx + 1);
        let child = match left_part.get_mut(idx) {
            Some(node) => node,
            None => return,
        };
        let right_sibling = match right_part.get_mut(0) {
            Some(node) => node,
            None => return,
        };

        if right_sibling.keys.is_empty() || right_sibling.values.is_empty() {
            return;
        }

        let borrowed_key = right_sibling.keys.remove(0);
        let borrowed_value = right_sibling.values.remove(0);
        let borrowed_child = if !child.is_leaf && !right_sibling.children.is_empty() {
            Some(right_sibling.children.remove(0))
        } else {
            None
        };

        child.keys.push(parent_key);
        child.values.push(parent_value);

        if let Some(parent_key_slot) = self.keys.get_mut(idx) {
            *parent_key_slot = borrowed_key;
        }
        if let Some(parent_value_slot) = self.values.get_mut(idx) {
            *parent_value_slot = borrowed_value;
        }

        if let Some(borrowed_child) = borrowed_child {
            child.children.push(borrowed_child);
        }
    }

    fn collect_entries(&self, result: &mut Vec<(NodeId, RecordPointer)>) {
        for i in 0..self.keys.len() {
            if !self.is_leaf {
                self.children[i].collect_entries(result);
            }
            result.push((self.keys[i], self.values[i]));
        }
        if !self.is_leaf && !self.children.is_empty() {
            self.children[self.children.len() - 1].collect_entries(result);
        }
    }
}

#[derive(Debug, Clone)]
pub struct CustomBTree {
    root: Option<Box<BTreeNode>>,
    size: usize,
}

impl CustomBTree {
    pub fn new() -> Self {
        Self {
            root: Some(Box::new(BTreeNode::new(true))),
            size: 0,
        }
    }

    pub fn insert(&mut self, key: NodeId, value: RecordPointer) {
        let root_is_full = self.root.as_ref().is_some_and(|root| root.is_full());

        if root_is_full {
            if let Some(old_root) = self.root.take() {
                let mut new_root = BTreeNode::new(false);
                new_root.children.push(old_root);
                new_root.split_child(0);
                new_root.insert_non_full(key, value);
                self.root = Some(Box::new(new_root));
            } else {
                self.root = Some(Box::new(BTreeNode::new(true)));
                if let Some(root) = self.root.as_mut() {
                    root.insert_non_full(key, value);
                }
            }
        } else if let Some(root) = self.root.as_mut() {
            root.insert_non_full(key, value);
        }

        self.size = self.size.saturating_add(1);
    }

    pub fn get(&self, key: &NodeId) -> Option<&RecordPointer> {
        self.root.as_ref().and_then(|root| root.search(key))
    }

    pub fn remove(&mut self, key: &NodeId) -> Option<RecordPointer> {
        let result = self.root.as_mut().and_then(|root| root.remove(key));
        if result.is_some() {
            self.size = self.size.saturating_sub(1);

            if let Some(root) = &self.root {
                if root.keys.is_empty() && !root.is_leaf && !root.children.is_empty() {
                    self.root = Some(root.children[0].clone());
                }
            }
        }
        result
    }

    pub fn clear(&mut self) {
        self.root = Some(Box::new(BTreeNode::new(true)));
        self.size = 0;
    }

    pub fn len(&self) -> usize {
        self.size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn iter(&self) -> CustomBTreeIter {
        let mut entries = Vec::new();
        if let Some(root) = &self.root {
            root.collect_entries(&mut entries);
        }
        CustomBTreeIter { entries, index: 0 }
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.size as u64).to_le_bytes());

        let entries: Vec<_> = self.iter().collect();
        for (node_id, pointer) in entries {
            buf.extend_from_slice(&node_id.to_le_bytes());
            buf.extend_from_slice(&pointer.page_id.to_le_bytes());
            buf.extend_from_slice(&pointer.slot_index.to_le_bytes());
        }

        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Ok(Self::new());
        }

        let len = Self::read_u64(data, 0)? as usize;
        let entry_size = 8 + 4 + 2;
        let required = len
            .checked_mul(entry_size)
            .ok_or_else(|| GraphError::Corruption("CustomBTree entry overflow".into()))?;
        if data.len() < 8 + required {
            return Err(GraphError::Corruption("CustomBTree data truncated".into()));
        }

        let mut tree = Self::new();

        for i in 0..len {
            let offset = 8 + i * entry_size;
            let entry = data
                .get(offset..offset + entry_size)
                .ok_or_else(|| GraphError::Corruption("CustomBTree entry exceeds buffer".into()))?;

            let node_id = Self::read_u64(entry, 0)?;
            let page_id = Self::read_u32(entry, 8)?;
            let slot_index = Self::read_u16(entry, 12)?;

            tree.insert(
                node_id,
                RecordPointer {
                    page_id,
                    slot_index,
                    byte_offset: 0,
                },
            );
        }

        Ok(tree)
    }

    pub fn range(
        &self,
        start: NodeId,
        end: NodeId,
    ) -> impl Iterator<Item = (NodeId, RecordPointer)> {
        self.iter().filter(move |(k, _)| *k >= start && *k <= end)
    }

    pub fn range_from(&self, start: NodeId) -> impl Iterator<Item = (NodeId, RecordPointer)> {
        self.iter().filter(move |(k, _)| *k >= start)
    }

    pub fn range_to(&self, end: NodeId) -> impl Iterator<Item = (NodeId, RecordPointer)> {
        self.iter().filter(move |(k, _)| *k <= end)
    }

    pub fn batch_insert(&mut self, entries: Vec<(NodeId, RecordPointer)>) {
        for (key, value) in entries {
            self.insert(key, value);
        }
    }

    pub fn batch_remove(&mut self, keys: &[NodeId]) -> Vec<(NodeId, RecordPointer)> {
        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.remove(key) {
                removed.push((*key, value));
            }
        }
        removed
    }
}

impl CustomBTree {
    fn read_u16(buf: &[u8], offset: usize) -> Result<u16> {
        let end = offset
            .checked_add(2)
            .ok_or_else(|| GraphError::Corruption("u16 offset overflow".into()))?;
        let slice = buf
            .get(offset..end)
            .ok_or_else(|| GraphError::Corruption("Invalid u16 in CustomBTree data".into()))?;
        let bytes: [u8; 2] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to read u16 from CustomBTree data".into())
        })?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32(buf: &[u8], offset: usize) -> Result<u32> {
        let end = offset
            .checked_add(4)
            .ok_or_else(|| GraphError::Corruption("u32 offset overflow".into()))?;
        let slice = buf
            .get(offset..end)
            .ok_or_else(|| GraphError::Corruption("Invalid u32 in CustomBTree data".into()))?;
        let bytes: [u8; 4] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to read u32 from CustomBTree data".into())
        })?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64(buf: &[u8], offset: usize) -> Result<u64> {
        let end = offset
            .checked_add(8)
            .ok_or_else(|| GraphError::Corruption("u64 offset overflow".into()))?;
        let slice = buf
            .get(offset..end)
            .ok_or_else(|| GraphError::Corruption("Invalid u64 in CustomBTree data".into()))?;
        let bytes: [u8; 8] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to read u64 from CustomBTree data".into())
        })?;
        Ok(u64::from_le_bytes(bytes))
    }
}

impl Default for CustomBTree {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CustomBTreeIter {
    entries: Vec<(NodeId, RecordPointer)>,
    index: usize,
}

impl Iterator for CustomBTreeIter {
    type Item = (NodeId, RecordPointer);

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.entries.len() {
            let item = self.entries[self.index];
            self.index += 1;
            Some(item)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut tree = CustomBTree::new();

        let ptr1 = RecordPointer {
            page_id: 1,
            slot_index: 0,
            byte_offset: 0,
        };
        let ptr2 = RecordPointer {
            page_id: 2,
            slot_index: 1,
            byte_offset: 0,
        };

        tree.insert(1, ptr1);
        tree.insert(2, ptr2);

        assert_eq!(tree.get(&1), Some(&ptr1));
        assert_eq!(tree.get(&2), Some(&ptr2));
        assert_eq!(tree.get(&3), None);

        assert_eq!(tree.len(), 2);

        let removed = tree.remove(&1);
        assert_eq!(removed, Some(ptr1));
        assert_eq!(tree.get(&1), None);
        assert_eq!(tree.len(), 1);
    }

    #[test]
    fn test_large_dataset() {
        let mut tree = CustomBTree::new();

        for i in 0..1000 {
            tree.insert(
                i,
                RecordPointer {
                    page_id: i as u32,
                    slot_index: (i % 100) as u16,
                    byte_offset: 0,
                },
            );
        }

        assert_eq!(tree.len(), 1000);

        for i in 0..1000 {
            assert!(tree.get(&i).is_some());
        }

        for i in (0..1000).step_by(2) {
            tree.remove(&i);
        }

        assert_eq!(tree.len(), 500);

        for i in 0..1000 {
            if i % 2 == 0 {
                assert!(tree.get(&i).is_none());
            } else {
                assert!(tree.get(&i).is_some());
            }
        }
    }

    #[test]
    fn test_serialization() {
        let mut tree = CustomBTree::new();

        tree.insert(
            1,
            RecordPointer {
                page_id: 10,
                slot_index: 5,
                byte_offset: 0,
            },
        );
        tree.insert(
            2,
            RecordPointer {
                page_id: 20,
                slot_index: 15,
                byte_offset: 0,
            },
        );
        tree.insert(
            100,
            RecordPointer {
                page_id: 30,
                slot_index: 25,
                byte_offset: 0,
            },
        );

        let serialized = tree.serialize().unwrap();
        let deserialized = CustomBTree::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.get(&1), tree.get(&1));
        assert_eq!(deserialized.get(&2), tree.get(&2));
        assert_eq!(deserialized.get(&100), tree.get(&100));
        assert_eq!(deserialized.len(), tree.len());
    }

    #[test]
    fn test_range_queries() {
        let mut tree = CustomBTree::new();

        for i in 0..100 {
            tree.insert(
                i,
                RecordPointer {
                    page_id: i as u32,
                    slot_index: 0,
                    byte_offset: 0,
                },
            );
        }

        let range_results: Vec<_> = tree.range(10, 20).collect();
        assert_eq!(range_results.len(), 11);
        assert_eq!(range_results[0].0, 10);
        assert_eq!(range_results[10].0, 20);

        let from_results: Vec<_> = tree.range_from(90).collect();
        assert_eq!(from_results.len(), 10);

        let to_results: Vec<_> = tree.range_to(10).collect();
        assert_eq!(to_results.len(), 11);
    }

    #[test]
    fn test_bulk_operations() {
        let mut tree = CustomBTree::new();

        let entries: Vec<_> = (0..100)
            .map(|i| {
                (
                    i,
                    RecordPointer {
                        page_id: i as u32,
                        slot_index: 0,
                        byte_offset: 0,
                    },
                )
            })
            .collect();

        tree.batch_insert(entries);
        assert_eq!(tree.len(), 100);

        let keys_to_remove: Vec<_> = (0..50).collect();
        let removed = tree.batch_remove(&keys_to_remove);
        assert_eq!(removed.len(), 50);
        assert_eq!(tree.len(), 50);
    }
}
