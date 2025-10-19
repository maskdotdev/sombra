use std::collections::HashMap;
use crate::model::NodeId;
use crate::storage::RecordPointer;
use crate::error::Result;

#[derive(Debug, Clone)]
pub struct BTreeIndex {
    root: HashMap<NodeId, RecordPointer>,
}

impl BTreeIndex {
    pub fn new() -> Self {
        Self {
            root: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: NodeId, value: RecordPointer) {
        self.root.insert(key, value);
    }

    pub fn get(&self, key: &NodeId) -> Option<&RecordPointer> {
        self.root.get(key)
    }

    pub fn remove(&mut self, key: &NodeId) -> Option<RecordPointer> {
        self.root.remove(key)
    }

    pub fn clear(&mut self) {
        self.root.clear();
    }

    pub fn iter(&self) -> impl Iterator<Item = (&NodeId, &RecordPointer)> {
        self.root.iter()
    }

    pub fn len(&self) -> usize {
        self.root.len()
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_empty()
    }

    pub fn range(&self, start: NodeId, end: NodeId) -> Vec<(&NodeId, &RecordPointer)> {
        let mut items: Vec<_> = self.root.iter()
            .filter(|(&k, _)| k >= start && k <= end)
            .collect();
        items.sort_by_key(|(k, _)| *k);
        items
    }

    pub fn range_from(&self, start: NodeId) -> Vec<(&NodeId, &RecordPointer)> {
        let mut items: Vec<_> = self.root.iter()
            .filter(|(&k, _)| k >= start)
            .collect();
        items.sort_by_key(|(k, _)| *k);
        items
    }

    pub fn range_to(&self, end: NodeId) -> Vec<(&NodeId, &RecordPointer)> {
        let mut items: Vec<_> = self.root.iter()
            .filter(|(&k, _)| k <= end)
            .collect();
        items.sort_by_key(|(k, _)| *k);
        items
    }

    pub fn batch_insert(&mut self, entries: Vec<(NodeId, RecordPointer)>) {
        for (key, value) in entries {
            self.root.insert(key, value);
        }
    }

    pub fn batch_remove(&mut self, keys: &[NodeId]) -> Vec<(NodeId, RecordPointer)> {
        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some(value) = self.root.remove(key) {
                removed.push((*key, value));
            }
        }
        removed
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        let len = self.root.len() as u64;
        buf.extend_from_slice(&len.to_le_bytes());
        
        for (&node_id, &pointer) in &self.root {
            buf.extend_from_slice(&node_id.to_le_bytes());
            buf.extend_from_slice(&pointer.page_id.to_le_bytes());
            buf.extend_from_slice(&pointer.slot_index.to_le_bytes());
            buf.extend_from_slice(&pointer.byte_offset.to_le_bytes());
        }
        
        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.len() < 8 {
            return Ok(Self::new());
        }
        
        let len = u64::from_le_bytes(data[0..8].try_into().unwrap()) as usize;
        let mut root = HashMap::with_capacity(len);
        let entry_size = 8 + 4 + 2 + 2;
        let old_entry_size = 8 + 4 + 2;
        
        for i in 0..len {
            let offset = 8 + i * entry_size;
            if offset + entry_size > data.len() {
                let old_offset = 8 + i * old_entry_size;
                if old_offset + old_entry_size <= data.len() {
                    let node_id = u64::from_le_bytes(data[old_offset..old_offset+8].try_into().unwrap());
                    let page_id = u32::from_le_bytes(data[old_offset+8..old_offset+12].try_into().unwrap());
                    let slot_index = u16::from_le_bytes(data[old_offset+12..old_offset+14].try_into().unwrap());
                    root.insert(node_id, RecordPointer { page_id, slot_index, byte_offset: 0 });
                }
                break;
            }
            
            let node_id = u64::from_le_bytes(data[offset..offset+8].try_into().unwrap());
            let page_id = u32::from_le_bytes(data[offset+8..offset+12].try_into().unwrap());
            let slot_index = u16::from_le_bytes(data[offset+12..offset+14].try_into().unwrap());
            let byte_offset = u16::from_le_bytes(data[offset+14..offset+16].try_into().unwrap());
            
            root.insert(node_id, RecordPointer { page_id, slot_index, byte_offset });
        }
        
        Ok(Self { root })
    }
}

impl Default for BTreeIndex {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_operations() {
        let mut index = BTreeIndex::new();
        
        let ptr1 = RecordPointer { page_id: 1, slot_index: 0, byte_offset: 0 };
        let ptr2 = RecordPointer { page_id: 2, slot_index: 1, byte_offset: 0 };
        
        index.insert(1, ptr1);
        index.insert(2, ptr2);
        
        assert_eq!(index.get(&1), Some(&ptr1));
        assert_eq!(index.get(&2), Some(&ptr2));
        assert_eq!(index.get(&3), None);
        
        assert_eq!(index.len(), 2);
        
        let removed = index.remove(&1);
        assert_eq!(removed, Some(ptr1));
        assert_eq!(index.get(&1), None);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_serialization() {
        let mut index = BTreeIndex::new();
        
        index.insert(1, RecordPointer { page_id: 10, slot_index: 5, byte_offset: 0 });
        index.insert(2, RecordPointer { page_id: 20, slot_index: 15, byte_offset: 0 });
        index.insert(100, RecordPointer { page_id: 30, slot_index: 25, byte_offset: 0 });
        
        let serialized = index.serialize().unwrap();
        let deserialized = BTreeIndex::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.get(&1), index.get(&1));
        assert_eq!(deserialized.get(&2), index.get(&2));
        assert_eq!(deserialized.get(&100), index.get(&100));
        assert_eq!(deserialized.len(), index.len());
    }

    #[test]
    fn test_clear() {
        let mut index = BTreeIndex::new();
        index.insert(1, RecordPointer { page_id: 1, slot_index: 0, byte_offset: 0 });
        index.insert(2, RecordPointer { page_id: 2, slot_index: 1, byte_offset: 0 });
        
        assert_eq!(index.len(), 2);
        
        index.clear();
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_iteration() {
        let mut index = BTreeIndex::new();
        index.insert(3, RecordPointer { page_id: 30, slot_index: 3, byte_offset: 0 });
        index.insert(1, RecordPointer { page_id: 10, slot_index: 1, byte_offset: 0 });
        index.insert(2, RecordPointer { page_id: 20, slot_index: 2, byte_offset: 0 });
        
        let mut keys: Vec<NodeId> = index.iter().map(|(k, _)| *k).collect();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn test_large_dataset() {
        let mut index = BTreeIndex::new();
        
        for i in 0..10000 {
            index.insert(i, RecordPointer { page_id: i as u32, slot_index: (i % 100) as u16, byte_offset: 0 });
        }
        
        assert_eq!(index.len(), 10000);
        
        for i in 0..10000 {
            assert!(index.get(&i).is_some());
        }
        
        for i in (0..10000).step_by(2) {
            index.remove(&i);
        }
        
        assert_eq!(index.len(), 5000);
        
        for i in 0..10000 {
            if i % 2 == 0 {
                assert!(index.get(&i).is_none());
            } else {
                assert!(index.get(&i).is_some());
            }
        }
    }

    #[test]
    fn test_empty_serialization() {
        let index = BTreeIndex::new();
        let serialized = index.serialize().unwrap();
        let deserialized = BTreeIndex::deserialize(&serialized).unwrap();
        assert!(deserialized.is_empty());
    }

    #[test]
    fn test_large_serialization() {
        let mut index = BTreeIndex::new();
        
        for i in 0..1000 {
            index.insert(i, RecordPointer { page_id: i as u32 * 2, slot_index: i as u16 % 50, byte_offset: 0 });
        }
        
        let serialized = index.serialize().unwrap();
        let deserialized = BTreeIndex::deserialize(&serialized).unwrap();
        
        assert_eq!(deserialized.len(), 1000);
        for i in 0..1000 {
            assert_eq!(deserialized.get(&i), index.get(&i));
        }
    }
}
