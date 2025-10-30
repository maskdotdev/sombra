use crate::error::{GraphError, Result};
use crate::model::NodeId;
use crate::storage::RecordPointer;
use dashmap::DashMap;
use std::convert::{TryFrom, TryInto};
use std::sync::Arc;

const BTREE_MAGIC: &[u8; 4] = b"BIDX";
const BTREE_VERSION: u16 = 1;
const BTREE_HEADER_SIZE: usize = 8; // magic (4) + version (2) + reserved (2)
#[allow(dead_code)]
const ENTRY_SIZE: usize = 8 + 4 + 2 + 2;

/// Lock-free BTreeIndex using DashMap for concurrent reads with MVCC support
///
/// This implementation uses DashMap for O(1) lock-free lookups on the hot path (get/insert/remove)
/// while maintaining compatibility with range queries by collecting entries on-demand.
///
/// For MVCC support, each NodeId maps to a Vec<RecordPointer> representing the version chain.
/// The first element in the vector is always the latest (most recent) version.
#[derive(Debug, Clone)]
pub struct BTreeIndex {
    // Primary storage: lock-free hash map for fast concurrent access
    // Each key maps to a vector of version pointers (latest first)
    map: Arc<DashMap<NodeId, Vec<RecordPointer>>>,
}

impl BTreeIndex {
    pub fn new() -> Self {
        Self {
            map: Arc::new(DashMap::new()),
        }
    }

    /// Insert a new version for a key (lock-free)
    /// The new version is added to the front of the version chain (most recent)
    pub fn insert(&self, key: NodeId, value: RecordPointer) {
        self.map
            .entry(key)
            .or_insert_with(Vec::new)
            .insert(0, value);
    }

    /// Get all versions for a key (lock-free read)
    /// Returns a vector with the latest version first
    pub fn get(&self, key: &NodeId) -> Option<Vec<RecordPointer>> {
        self.map.get(key).map(|r| r.value().clone())
    }

    /// Get the latest (most recent) version for a key (lock-free read)
    pub fn get_latest(&self, key: &NodeId) -> Option<RecordPointer> {
        self.map.get(key).and_then(|r| r.value().first().copied())
    }
    
    /// Find a node ID by its record pointer (reverse lookup)
    /// This is O(n*m) where n is number of nodes and m is average version chain length
    pub fn find_by_pointer(&self, pointer: RecordPointer) -> Option<NodeId> {
        self.map
            .iter()
            .find(|entry| entry.value().contains(&pointer))
            .map(|entry| *entry.key())
    }

    /// Remove a key and all its versions (lock-free)
    /// Returns the removed version chain
    pub fn remove(&self, key: &NodeId) -> Option<Vec<RecordPointer>> {
        self.map.remove(key).map(|(_, v)| v)
    }

    /// Clear all entries
    pub fn clear(&self) {
        self.map.clear();
    }

    /// Replace the entire contents of this index with another index
    pub fn replace_with(&self, other: BTreeIndex) {
        self.clear();
        // Clone all entries from other into self
        for entry in other.map.iter() {
            self.map.insert(*entry.key(), entry.value().clone());
        }
    }

    /// Get all entries as a sorted vector (for compatibility)
    /// Returns only the latest version for each node
    pub fn iter(&self) -> Vec<(NodeId, RecordPointer)> {
        let mut entries: Vec<_> = self
            .map
            .iter()
            .filter_map(|r| {
                r.value().first().map(|ptr| (*r.key(), *ptr))
            })
            .collect();
        entries.sort_by_key(|(k, _)| *k);
        entries
    }

    /// Get all entries with all versions as a sorted vector
    pub fn iter_all_versions(&self) -> Vec<(NodeId, Vec<RecordPointer>)> {
        let mut entries: Vec<_> = self
            .map
            .iter()
            .map(|r| (*r.key(), r.value().clone()))
            .collect();
        entries.sort_by_key(|(k, _)| *k);
        entries
    }

    /// Get the number of entries
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Check if the index is empty
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Get entries in a range [start, end] (sorted)
    /// Returns only the latest version for each node
    pub fn range(&self, start: NodeId, end: NodeId) -> Vec<(NodeId, RecordPointer)> {
        if start > end {
            return Vec::new();
        }
        let mut entries: Vec<_> = self
            .map
            .iter()
            .filter(|r| {
                let key = *r.key();
                key >= start && key <= end
            })
            .filter_map(|r| {
                r.value().first().map(|ptr| (*r.key(), *ptr))
            })
            .collect();
        entries.sort_by_key(|(k, _)| *k);
        entries
    }

    /// Get entries from start onwards (sorted)
    /// Returns only the latest version for each node
    pub fn range_from(&self, start: NodeId) -> Vec<(NodeId, RecordPointer)> {
        let mut entries: Vec<_> = self
            .map
            .iter()
            .filter(|r| *r.key() >= start)
            .filter_map(|r| {
                r.value().first().map(|ptr| (*r.key(), *ptr))
            })
            .collect();
        entries.sort_by_key(|(k, _)| *k);
        entries
    }

    /// Get entries up to end (sorted)
    /// Returns only the latest version for each node
    pub fn range_to(&self, end: NodeId) -> Vec<(NodeId, RecordPointer)> {
        let mut entries: Vec<_> = self
            .map
            .iter()
            .filter(|r| *r.key() <= end)
            .filter_map(|r| {
                r.value().first().map(|ptr| (*r.key(), *ptr))
            })
            .collect();
        entries.sort_by_key(|(k, _)| *k);
        entries
    }

    /// Get the first entry (smallest key)
    /// Returns only the latest version
    pub fn first(&self) -> Option<(NodeId, RecordPointer)> {
        self.map
            .iter()
            .filter_map(|r| {
                r.value().first().map(|ptr| (*r.key(), *ptr))
            })
            .min_by_key(|(k, _)| *k)
    }

    /// Get the last entry (largest key)
    /// Returns only the latest version
    pub fn last(&self) -> Option<(NodeId, RecordPointer)> {
        self.map
            .iter()
            .filter_map(|r| {
                r.value().first().map(|ptr| (*r.key(), *ptr))
            })
            .max_by_key(|(k, _)| *k)
    }

    /// Get the first N entries (sorted by key)
    /// Returns only the latest version for each node
    pub fn first_n(&self, n: usize) -> Vec<(NodeId, RecordPointer)> {
        let mut entries: Vec<_> = self
            .map
            .iter()
            .filter_map(|r| {
                r.value().first().map(|ptr| (*r.key(), *ptr))
            })
            .collect();
        entries.sort_by_key(|(k, _)| *k);
        entries.truncate(n);
        entries
    }

    /// Get the last N entries (sorted by key descending)
    /// Returns only the latest version for each node
    pub fn last_n(&self, n: usize) -> Vec<(NodeId, RecordPointer)> {
        let mut entries: Vec<_> = self
            .map
            .iter()
            .filter_map(|r| {
                r.value().first().map(|ptr| (*r.key(), *ptr))
            })
            .collect();
        entries.sort_by_key(|(k, _)| std::cmp::Reverse(*k));
        entries.truncate(n);
        entries
    }

    /// Batch insert entries (lock-free)
    /// Each entry creates a new version in the version chain
    pub fn batch_insert(&self, entries: Vec<(NodeId, RecordPointer)>) {
        for (key, value) in entries {
            self.insert(key, value);
        }
    }

    /// Batch remove entries (lock-free)
    /// Removes entire version chains for each key
    pub fn batch_remove(&self, keys: &[NodeId]) -> Vec<(NodeId, Vec<RecordPointer>)> {
        let mut removed = Vec::with_capacity(keys.len());
        for key in keys {
            if let Some((_, value)) = self.map.remove(key) {
                removed.push((*key, value));
            }
        }
        removed
    }

    pub fn serialize(&self) -> Result<Vec<u8>> {
        // Convert DashMap to sorted vector for serialization
        let entries = self.iter_all_versions(); // Get all versions
        let num_nodes = u64::try_from(entries.len()).map_err(|_| {
            GraphError::Corruption("Too many nodes to serialize BTree index".into())
        })?;

        // Calculate total number of version entries
        let total_versions: usize = entries.iter().map(|(_, versions)| versions.len()).sum();
        let total_versions_u64 = u64::try_from(total_versions).map_err(|_| {
            GraphError::Corruption("Too many versions to serialize BTree index".into())
        })?;

        // Header: magic (4) + version (2) + reserved (2) + num_nodes (8) + total_versions (8)
        const EXTENDED_HEADER_SIZE: usize = BTREE_HEADER_SIZE + 8 + 8;
        // Each node entry: node_id (8) + version_count (4)
        // Each version entry: page_id (4) + slot_index (2) + byte_offset (2)
        const VERSION_ENTRY_SIZE: usize = 4 + 2 + 2;
        let capacity = EXTENDED_HEADER_SIZE 
            + (entries.len() * 12) // node_id + version_count
            + (total_versions * VERSION_ENTRY_SIZE);

        let mut buf = Vec::with_capacity(capacity);
        buf.extend_from_slice(BTREE_MAGIC);
        buf.extend_from_slice(&BTREE_VERSION.to_le_bytes());
        buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
        buf.extend_from_slice(&num_nodes.to_le_bytes());
        buf.extend_from_slice(&total_versions_u64.to_le_bytes());

        for (node_id, versions) in entries {
            buf.extend_from_slice(&node_id.to_le_bytes());
            let version_count = u32::try_from(versions.len()).map_err(|_| {
                GraphError::Corruption(format!("Too many versions for node {node_id}"))
            })?;
            buf.extend_from_slice(&version_count.to_le_bytes());
            
            for pointer in versions {
                buf.extend_from_slice(&pointer.page_id.to_le_bytes());
                buf.extend_from_slice(&pointer.slot_index.to_le_bytes());
                buf.extend_from_slice(&pointer.byte_offset.to_le_bytes());
            }
        }

        // Debug assertion: Verify the magic bytes were written correctly
        debug_assert_eq!(
            &buf[..BTREE_MAGIC.len()],
            BTREE_MAGIC,
            "BTree serialization must start with BIDX magic bytes"
        );

        Ok(buf)
    }

    pub fn deserialize(data: &[u8]) -> Result<Self> {
        if data.is_empty() {
            return Ok(Self::new());
        }

        const EXTENDED_HEADER_SIZE: usize = BTREE_HEADER_SIZE + 8 + 8;
        if data.len() < EXTENDED_HEADER_SIZE || &data[..BTREE_MAGIC.len()] != BTREE_MAGIC {
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
        let num_nodes = Self::read_u64_le(data, cursor)?;
        cursor += 8;
        let _total_versions = Self::read_u64_le(data, cursor)?;
        cursor += 8;

        let index = Self::new();

        for _node_idx in 0..num_nodes {
            if cursor + 12 > data.len() {
                return Err(GraphError::Corruption(
                    "BTree index node entry truncated".into(),
                ));
            }

            let node_id = Self::read_u64_le(data, cursor)?;
            cursor += 8;
            let version_count = Self::read_u32_le(data, cursor)?;
            cursor += 4;

            let mut versions = Vec::with_capacity(version_count as usize);
            for _ver_idx in 0..version_count {
                if cursor + 8 > data.len() {
                    return Err(GraphError::Corruption(
                        "BTree index version entry truncated".into(),
                    ));
                }

                let page_id = Self::read_u32_le(data, cursor)?;
                cursor += 4;
                let slot_index = Self::read_u16_le(data, cursor)?;
                cursor += 2;
                let byte_offset = Self::read_u16_le(data, cursor)?;
                cursor += 2;

                versions.push(RecordPointer {
                    page_id,
                    slot_index,
                    byte_offset,
                });
            }

            index.map.insert(node_id, versions);
        }

        Ok(index)
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
        let index = BTreeIndex::new();

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

        assert_eq!(index.get(&1), Some(vec![ptr1]));
        assert_eq!(index.get(&2), Some(vec![ptr2]));
        assert_eq!(index.get(&3), None);

        assert_eq!(index.len(), 2);

        let removed = index.remove(&1);
        assert_eq!(removed, Some(vec![ptr1]));
        assert_eq!(index.get(&1), None);
        assert_eq!(index.len(), 1);
    }

    #[test]
    fn test_serialization() {
        let index = BTreeIndex::new();

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
        let index = BTreeIndex::new();
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
        let index = BTreeIndex::new();
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

        let mut keys: Vec<NodeId> = index.iter().into_iter().map(|(k, _)| k).collect();
        keys.sort();
        assert_eq!(keys, vec![1, 2, 3]);
    }

    #[test]
    fn test_large_dataset() {
        let index = BTreeIndex::new();

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
        let index = BTreeIndex::new();

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
        bytes.extend_from_slice(&0u64.to_le_bytes()); // num_nodes
        bytes.extend_from_slice(&0u64.to_le_bytes()); // total_versions

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

    #[test]
    fn test_range_queries() {
        let index = BTreeIndex::new();

        for i in 0..100 {
            index.insert(
                i,
                RecordPointer {
                    page_id: i as u32,
                    slot_index: 0,
                    byte_offset: 0,
                },
            );
        }

        let range_result = index.range(10, 20);
        assert_eq!(range_result.len(), 11);
        for (i, (id, _)) in range_result.iter().enumerate() {
            assert_eq!(*id, 10 + i as u64);
        }

        let range_from_result = index.range_from(90);
        assert_eq!(range_from_result.len(), 10);
        assert_eq!(range_from_result[0].0, 90);
        assert_eq!(range_from_result[9].0, 99);

        let range_to_result = index.range_to(10);
        assert_eq!(range_to_result.len(), 11);
        assert_eq!(range_to_result[0].0, 0);
        assert_eq!(range_to_result[10].0, 10);
    }

    #[test]
    fn test_first_last_operations() {
        let index = BTreeIndex::new();

        assert_eq!(index.first(), None);
        assert_eq!(index.last(), None);

        for i in 0..100 {
            index.insert(
                i,
                RecordPointer {
                    page_id: i as u32,
                    slot_index: 0,
                    byte_offset: 0,
                },
            );
        }

        assert_eq!(index.first().map(|(id, _)| id), Some(0));
        assert_eq!(index.last().map(|(id, _)| id), Some(99));

        let first_10 = index.first_n(10);
        assert_eq!(first_10.len(), 10);
        assert_eq!(first_10[0].0, 0);
        assert_eq!(first_10[9].0, 9);

        let last_10 = index.last_n(10);
        assert_eq!(last_10.len(), 10);
        assert_eq!(last_10[0].0, 99);
        assert_eq!(last_10[9].0, 90);
    }

    #[test]
    fn test_ordered_iteration() {
        let index = BTreeIndex::new();

        let test_ids = vec![50, 10, 90, 30, 70, 20, 60, 40, 80, 100];
        for id in test_ids {
            index.insert(
                id,
                RecordPointer {
                    page_id: id as u32,
                    slot_index: 0,
                    byte_offset: 0,
                },
            );
        }

        let all_items = index.iter();
        assert_eq!(all_items.len(), 10);

        let ids: Vec<_> = all_items.iter().map(|(id, _)| *id).collect();
        let mut sorted_ids = ids.clone();
        sorted_ids.sort();

        assert_eq!(ids, sorted_ids);
    }
}
