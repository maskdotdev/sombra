use crate::error::{GraphError, Result};
use crate::model::NodeId;
use crate::storage::RecordPointer;
use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

const BTREE_MAGIC: &[u8; 4] = b"BIDX";
const BTREE_VERSION: u16 = 1;
const BTREE_HEADER_SIZE: usize = 8; // magic (4) + version (2) + reserved (2)
const ENTRY_SIZE: usize = 8 + 4 + 2 + 2;

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
        let mut items: Vec<_> = self
            .root
            .iter()
            .filter(|(&k, _)| k >= start && k <= end)
            .collect();
        items.sort_by_key(|(k, _)| *k);
        items
    }

    pub fn range_from(&self, start: NodeId) -> Vec<(&NodeId, &RecordPointer)> {
        let mut items: Vec<_> = self.root.iter().filter(|(&k, _)| k >= start).collect();
        items.sort_by_key(|(k, _)| *k);
        items
    }

    pub fn range_to(&self, end: NodeId) -> Vec<(&NodeId, &RecordPointer)> {
        let mut items: Vec<_> = self.root.iter().filter(|(&k, _)| k <= end).collect();
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
        let len = u64::try_from(self.root.len()).map_err(|_| {
            GraphError::Corruption("Too many entries to serialize BTree index".into())
        })?;
        let mut buf = Vec::with_capacity(BTREE_HEADER_SIZE + 8 + (ENTRY_SIZE * self.root.len()));
        buf.extend_from_slice(BTREE_MAGIC);
        buf.extend_from_slice(&BTREE_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
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
        if data.is_empty() {
            return Ok(Self::new());
        }

        if data.len() < BTREE_HEADER_SIZE + 8 || &data[..BTREE_MAGIC.len()] != BTREE_MAGIC {
            return Err(GraphError::Corruption(
                "BTree index missing magic header".into(),
            ));
        }

        let version = Self::read_u16_le(data, 4)?;
        if version != BTREE_VERSION {
            return Err(GraphError::Corruption(format!(
                "Unsupported BTree index format version {version}"
            )));
        }

        let mut cursor = BTREE_HEADER_SIZE;
        let len_u64 = Self::read_u64_le(data, cursor)?;
        let len: usize = usize::try_from(len_u64).map_err(|_| {
            GraphError::Corruption("BTree index length exceeds platform limits".into())
        })?;
        cursor += 8;

        let remaining = data.len().saturating_sub(cursor);
        let required = len
            .checked_mul(ENTRY_SIZE)
            .ok_or_else(|| GraphError::Corruption("BTree index entry size overflow".into()))?;
        if remaining < required {
            return Err(GraphError::Corruption("BTree index data truncated".into()));
        }

        let mut root = HashMap::with_capacity(len);

        for i in 0..len {
            let offset = cursor
                .checked_add(i.saturating_mul(ENTRY_SIZE))
                .ok_or_else(|| GraphError::Corruption("BTree index offset overflow".into()))?;
            if offset + ENTRY_SIZE > data.len() {
                return Err(GraphError::Corruption(
                    "BTree index entry extends beyond buffer".into(),
                ));
            }

            let node_id = Self::read_u64_le(data, offset)?;
            let page_id = Self::read_u32_le(data, offset + 8)?;
            let slot_index = Self::read_u16_le(data, offset + 12)?;
            let byte_offset = Self::read_u16_le(data, offset + 14)?;

            root.insert(
                node_id,
                RecordPointer {
                    page_id,
                    slot_index,
                    byte_offset,
                },
            );
        }

        Ok(Self { root })
    }

    fn read_u16_le(buf: &[u8], offset: usize) -> Result<u16> {
        let end = offset
            .checked_add(2)
            .ok_or_else(|| GraphError::Corruption("u16 read offset overflow".into()))?;
        let slice = buf.get(offset..end).ok_or_else(|| {
            GraphError::Corruption(format!("Invalid u16 at BTree offset {offset}"))
        })?;
        let bytes: [u8; 2] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to copy u16 bytes from BTree data".into())
        })?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
        let end = offset
            .checked_add(4)
            .ok_or_else(|| GraphError::Corruption("u32 read offset overflow".into()))?;
        let slice = buf.get(offset..end).ok_or_else(|| {
            GraphError::Corruption(format!("Invalid u32 at BTree offset {offset}"))
        })?;
        let bytes: [u8; 4] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to copy u32 bytes from BTree data".into())
        })?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
        let end = offset
            .checked_add(8)
            .ok_or_else(|| GraphError::Corruption("u64 read offset overflow".into()))?;
        let slice = buf.get(offset..end).ok_or_else(|| {
            GraphError::Corruption(format!("Invalid u64 at BTree offset {offset}"))
        })?;
        let bytes: [u8; 8] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to copy u64 bytes from BTree data".into())
        })?;
        Ok(u64::from_le_bytes(bytes))
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

        index.insert(
            1,
            RecordPointer {
                page_id: 10,
                slot_index: 5,
                byte_offset: 0,
            },
        );
        index.insert(
            2,
            RecordPointer {
                page_id: 20,
                slot_index: 15,
                byte_offset: 0,
            },
        );
        index.insert(
            100,
            RecordPointer {
                page_id: 30,
                slot_index: 25,
                byte_offset: 0,
            },
        );

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
        index.insert(
            1,
            RecordPointer {
                page_id: 1,
                slot_index: 0,
                byte_offset: 0,
            },
        );
        index.insert(
            2,
            RecordPointer {
                page_id: 2,
                slot_index: 1,
                byte_offset: 0,
            },
        );

        assert_eq!(index.len(), 2);

        index.clear();
        assert_eq!(index.len(), 0);
        assert!(index.is_empty());
    }

    #[test]
    fn test_iteration() {
        let mut index = BTreeIndex::new();
        index.insert(
            3,
            RecordPointer {
                page_id: 30,
                slot_index: 3,
                byte_offset: 0,
            },
        );
        index.insert(
            1,
            RecordPointer {
                page_id: 10,
                slot_index: 1,
                byte_offset: 0,
            },
        );
        index.insert(
            2,
            RecordPointer {
                page_id: 20,
                slot_index: 2,
                byte_offset: 0,
            },
        );

        let mut keys: Vec<NodeId> = index.iter().map(|(k, _)| *k).collect();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn test_large_dataset() {
        let mut index = BTreeIndex::new();

        for i in 0..10000 {
            index.insert(
                i,
                RecordPointer {
                    page_id: i as u32,
                    slot_index: (i % 100) as u16,
                    byte_offset: 0,
                },
            );
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
            index.insert(
                i,
                RecordPointer {
                    page_id: i as u32 * 2,
                    slot_index: i as u16 % 50,
                    byte_offset: 0,
                },
            );
        }

        let serialized = index.serialize().unwrap();
        let deserialized = BTreeIndex::deserialize(&serialized).unwrap();

        assert_eq!(deserialized.len(), 1000);
        for i in 0..1000 {
            assert_eq!(deserialized.get(&i), index.get(&i));
        }
    }

    #[test]
    fn deserialize_rejects_unknown_version() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BTREE_MAGIC);
        bytes.extend_from_slice(&(BTREE_VERSION + 1).to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());

        let err = BTreeIndex::deserialize(&bytes).expect_err("unsupported version should error");
        match err {
            GraphError::Corruption(message) => {
                assert!(
                    message.contains("Unsupported BTree index format version"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected corruption error, got {other:?}"),
        }
    }

    #[test]
    fn deserialize_detects_truncated_payload() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(BTREE_MAGIC);
        bytes.extend_from_slice(&BTREE_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u16.to_le_bytes());
        bytes.extend_from_slice(&1u64.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes()); // Only partial entry (8 bytes)

        let err = BTreeIndex::deserialize(&bytes).expect_err("truncated data should error");
        match err {
            GraphError::Corruption(message) => {
                assert!(
                    message.contains("truncated") || message.contains("extends beyond"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected corruption error, got {other:?}"),
        }
    }
}
