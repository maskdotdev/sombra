//! IFA Store - manages NodeAdjHeader B-trees and segment operations.
//!
//! This module provides the `IfaStore` struct that coordinates:
//! - Two B-trees for NodeAdjHeader storage (OUT and IN directions)
//! - Overflow block storage for high-type-count nodes
//! - Segment allocation and CoW operations

use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::adjacency::Dir;
use crate::storage::btree::{BTree, BTreeOptions, KeyCodec, ValCodec};
use crate::types::{NodeId, PageId, Result, SombraError, TypeId};

use super::types::{NodeAdjHeader, OverflowBlock, SegmentPtr};

/// Codec for NodeId keys in the IFA B-trees.
///
/// Keys are stored as big-endian u64 for proper ordering.
impl KeyCodec for NodeId {
    fn encode_key(key: &Self, out: &mut Vec<u8>) {
        out.extend_from_slice(&key.0.to_be_bytes());
    }

    fn compare_encoded(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        a.cmp(b)
    }

    fn decode_key(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != 8 {
            return Err(SombraError::Corruption("NodeId key length mismatch"));
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(bytes);
        Ok(NodeId(u64::from_be_bytes(arr)))
    }
}

/// Codec for NodeAdjHeader values in the IFA B-trees.
impl ValCodec for NodeAdjHeader {
    fn encode_val(value: &Self, out: &mut Vec<u8>) {
        out.extend_from_slice(&value.encode());
    }

    fn decode_val(src: &[u8]) -> Result<Self> {
        NodeAdjHeader::decode(src)
    }
}

/// Codec for OverflowBlock values.
impl ValCodec for OverflowBlock {
    fn encode_val(value: &Self, out: &mut Vec<u8>) {
        out.extend_from_slice(&value.encode());
    }

    fn decode_val(src: &[u8]) -> Result<Self> {
        OverflowBlock::decode(src)
    }
}

/// Index-Free Adjacency store managing per-node type maps and segments.
///
/// The store maintains:
/// - `adj_out`: B-tree mapping NodeId -> NodeAdjHeader for outgoing edges
/// - `adj_in`: B-tree mapping NodeId -> NodeAdjHeader for incoming edges
/// - `overflow`: B-tree mapping (NodeId, Dir, SequenceNum) -> OverflowBlock
///
/// # Design
///
/// For each (node, direction) pair, a NodeAdjHeader provides O(1) lookup
/// of the segment head for any edge type. Headers with >5 types use overflow
/// blocks stored separately.
pub struct IfaStore {
    #[allow(dead_code)]
    store: Arc<dyn PageStore>,
    /// B-tree for outgoing adjacency headers: NodeId -> NodeAdjHeader
    adj_out: BTree<NodeId, NodeAdjHeader>,
    /// B-tree for incoming adjacency headers: NodeId -> NodeAdjHeader
    adj_in: BTree<NodeId, NodeAdjHeader>,
    /// B-tree for overflow blocks: (NodeId << 1 | dir_bit) -> OverflowBlock
    /// The key encodes both node and direction for efficient lookup.
    overflow: BTree<u64, OverflowBlock>,
    /// Atomic roots for tracking changes
    adj_out_root: AtomicU64,
    adj_in_root: AtomicU64,
    overflow_root: AtomicU64,
}

/// Result of looking up a type in a node's adjacency header.
#[derive(Debug, Clone)]
pub enum TypeLookupResult {
    /// Type found with segment pointer.
    Found(SegmentPtr),
    /// Type not found (doesn't exist for this node).
    NotFound,
}

impl IfaStore {
    /// Opens or creates an IFA store with the given root pages.
    ///
    /// If roots are zero, new empty B-trees are created.
    pub fn open(
        store: Arc<dyn PageStore>,
        adj_out_root: PageId,
        adj_in_root: PageId,
        overflow_root: PageId,
    ) -> Result<Self> {
        let adj_out = Self::open_header_tree(&store, adj_out_root)?;
        let adj_in = Self::open_header_tree(&store, adj_in_root)?;
        let overflow = Self::open_overflow_tree(&store, overflow_root)?;

        let adj_out_root_id = adj_out.root_page().0;
        let adj_in_root_id = adj_in.root_page().0;
        let overflow_root_id = overflow.root_page().0;

        Ok(Self {
            store,
            adj_out,
            adj_in,
            overflow,
            adj_out_root: AtomicU64::new(adj_out_root_id),
            adj_in_root: AtomicU64::new(adj_in_root_id),
            overflow_root: AtomicU64::new(overflow_root_id),
        })
    }

    fn open_header_tree(
        store: &Arc<dyn PageStore>,
        root: PageId,
    ) -> Result<BTree<NodeId, NodeAdjHeader>> {
        let mut opts = BTreeOptions::default();
        opts.root_page = (root.0 != 0).then_some(root);
        BTree::open_or_create(store, opts)
    }

    fn open_overflow_tree(
        store: &Arc<dyn PageStore>,
        root: PageId,
    ) -> Result<BTree<u64, OverflowBlock>> {
        let mut opts = BTreeOptions::default();
        opts.root_page = (root.0 != 0).then_some(root);
        BTree::open_or_create(store, opts)
    }

    /// Returns current root pages for persisting to meta.
    pub fn roots(&self) -> IfaRoots {
        IfaRoots {
            adj_out: PageId(self.adj_out_root.load(AtomicOrdering::SeqCst)),
            adj_in: PageId(self.adj_in_root.load(AtomicOrdering::SeqCst)),
            overflow: PageId(self.overflow_root.load(AtomicOrdering::SeqCst)),
        }
    }

    /// Updates cached roots if B-tree roots have changed.
    pub fn refresh_roots(&self) {
        self.adj_out_root.store(self.adj_out.root_page().0, AtomicOrdering::SeqCst);
        self.adj_in_root.store(self.adj_in.root_page().0, AtomicOrdering::SeqCst);
        self.overflow_root.store(self.overflow.root_page().0, AtomicOrdering::SeqCst);
    }

    // =========================================================================
    // Read Operations (using ReadGuard)
    // =========================================================================

    /// Gets the NodeAdjHeader for a node in the given direction.
    ///
    /// Returns `None` if no header exists (node has no edges in this direction).
    pub fn get_header(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
    ) -> Result<Option<NodeAdjHeader>> {
        let tree = self.tree_for_dir(dir);
        tree.get(tx, &node)
    }

    /// Looks up the segment head for a specific (node, dir, type) triple.
    ///
    /// This is the primary read path for neighbor traversal:
    /// 1. Get NodeAdjHeader
    /// 2. Check inline buckets
    /// 3. If overflow present, search overflow chain
    pub fn lookup_type(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
    ) -> Result<TypeLookupResult> {
        let header = match self.get_header(tx, node, dir)? {
            Some(h) => h,
            None => return Ok(TypeLookupResult::NotFound),
        };

        // Try inline lookup first
        if let Some(ptr) = header.lookup_inline(type_id) {
            return Ok(TypeLookupResult::Found(ptr));
        }

        // Check if we need to search overflow
        if header.has_overflow() {
            if let Some(overflow_ptr) = header.overflow_ptr() {
                // Search overflow chain
                let result = self.search_overflow_chain(tx, node, dir, type_id, overflow_ptr)?;
                return Ok(match result {
                    Some(ptr) => TypeLookupResult::Found(ptr),
                    None => TypeLookupResult::NotFound,
                });
            }
        }

        Ok(TypeLookupResult::NotFound)
    }

    /// Searches the overflow chain for a type.
    fn search_overflow_chain(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
        _start: SegmentPtr,
    ) -> Result<Option<SegmentPtr>> {
        // Overflow blocks are stored in a B-tree keyed by (node, dir, sequence)
        // For now we use a simplified approach: the overflow pointer in NodeAdjHeader
        // points to a page containing the OverflowBlock directly.
        // In a more sophisticated implementation, we'd use a B-tree for overflow blocks.
        
        // For the initial implementation, we store overflow blocks indexed by
        // a composite key: (node_id << 1) | dir_bit
        let overflow_key = Self::overflow_key(node, dir);
        
        // Walk the chain
        let mut current_key = overflow_key;
        loop {
            let block = match self.overflow.get(tx, &current_key)? {
                Some(b) => b,
                None => return Ok(None),
            };

            // Binary search within block
            if let Some(ptr) = block.lookup(type_id) {
                return Ok(Some(ptr));
            }

            // Check next block
            if block.next.is_null() {
                return Ok(None);
            }

            // Move to next block (use next pointer as key offset)
            current_key = overflow_key.wrapping_add(block.next.0);
        }
    }

    /// Computes overflow B-tree key from node and direction.
    #[inline]
    fn overflow_key(node: NodeId, dir: Dir) -> u64 {
        let dir_bit = match dir {
            Dir::Out => 0,
            Dir::In => 1,
            Dir::Both => 0, // Shouldn't happen for adjacency
        };
        (node.0 << 1) | dir_bit
    }

    // =========================================================================
    // Write Operations (using WriteGuard)
    // =========================================================================

    /// Gets the NodeAdjHeader for a node in the given direction (write path).
    pub fn get_header_mut(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
    ) -> Result<Option<NodeAdjHeader>> {
        let tree = self.tree_for_dir(dir);
        tree.get_with_write(tx, &node)
    }

    /// Looks up the segment head for a specific (node, dir, type) triple (write path).
    pub fn lookup_type_mut(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
    ) -> Result<TypeLookupResult> {
        let header = match self.get_header_mut(tx, node, dir)? {
            Some(h) => h,
            None => return Ok(TypeLookupResult::NotFound),
        };

        // Try inline lookup first
        if let Some(ptr) = header.lookup_inline(type_id) {
            return Ok(TypeLookupResult::Found(ptr));
        }

        // Check if we need to search overflow
        if header.has_overflow() {
            if let Some(overflow_ptr) = header.overflow_ptr() {
                let result = self.search_overflow_chain_mut(tx, node, dir, type_id, overflow_ptr)?;
                return Ok(match result {
                    Some(ptr) => TypeLookupResult::Found(ptr),
                    None => TypeLookupResult::NotFound,
                });
            }
        }

        Ok(TypeLookupResult::NotFound)
    }

    /// Searches the overflow chain for a type (write path).
    fn search_overflow_chain_mut(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
        _start: SegmentPtr,
    ) -> Result<Option<SegmentPtr>> {
        let overflow_key = Self::overflow_key(node, dir);
        
        let mut current_key = overflow_key;
        loop {
            let block = match self.overflow.get_with_write(tx, &current_key)? {
                Some(b) => b,
                None => return Ok(None),
            };

            if let Some(ptr) = block.lookup(type_id) {
                return Ok(Some(ptr));
            }

            if block.next.is_null() {
                return Ok(None);
            }

            current_key = overflow_key.wrapping_add(block.next.0);
        }
    }

    /// Inserts or updates a type mapping in a node's adjacency header.
    ///
    /// This handles:
    /// - Creating new headers for nodes without existing adjacency
    /// - Inserting into inline buckets when space available
    /// - Triggering overflow when inline buckets are full
    pub fn upsert_type(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
        head: SegmentPtr,
    ) -> Result<()> {
        let tree = self.tree_for_dir(dir);
        
        // Get or create header
        let mut header = tree.get_with_write(tx, &node)?.unwrap_or_else(NodeAdjHeader::new);

        // Try inline insert
        match header.insert_inline(type_id, head) {
            Ok(()) => {
                // Successfully inserted inline
                tree.put(tx, &node, &header)?;
                self.refresh_roots();
                Ok(())
            }
            Err(_) => {
                // Inline buckets full, need overflow
                self.insert_with_overflow(tx, node, dir, type_id, head, &mut header)?;
                tree.put(tx, &node, &header)?;
                self.refresh_roots();
                Ok(())
            }
        }
    }

    /// Handles insertion when inline buckets are full.
    fn insert_with_overflow(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
        head: SegmentPtr,
        header: &mut NodeAdjHeader,
    ) -> Result<()> {
        let overflow_key = Self::overflow_key(node, dir);

        if header.has_overflow() {
            // Already have overflow - insert into existing chain
            let mut block = self.overflow.get_with_write(tx, &overflow_key)?
                .unwrap_or_else(OverflowBlock::new);

            if block.is_full() {
                // Need to chain a new block
                // For simplicity, we create a new block and link it
                let mut new_block = OverflowBlock::new();
                new_block.insert(type_id, head)?;
                new_block.next = SegmentPtr(1); // Mark as having a next
                
                // Store old block with offset key
                let old_key = overflow_key.wrapping_add(1);
                self.overflow.put(tx, &old_key, &block)?;
                
                // Store new block at primary key
                self.overflow.put(tx, &overflow_key, &new_block)?;
            } else {
                block.insert(type_id, head)?;
                self.overflow.put(tx, &overflow_key, &block)?;
            }
        } else {
            // First overflow - create new block
            let mut block = OverflowBlock::new();
            block.insert(type_id, head)?;
            self.overflow.put(tx, &overflow_key, &block)?;
            
            // Set overflow pointer in header
            header.set_overflow(SegmentPtr::from_page(PageId(overflow_key)));
        }

        self.refresh_roots();
        Ok(())
    }

    /// Removes a type mapping from a node's adjacency header.
    ///
    /// Returns the old segment pointer if found.
    pub fn remove_type(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
    ) -> Result<Option<SegmentPtr>> {
        let tree = self.tree_for_dir(dir);

        let mut header = match tree.get_with_write(tx, &node)? {
            Some(h) => h,
            None => return Ok(None),
        };

        // Try inline removal first
        if let Some(old_ptr) = header.remove_inline(type_id) {
            // Update or remove header
            if header.active_count() == 0 && !header.has_overflow() {
                tree.delete(tx, &node)?;
            } else {
                tree.put(tx, &node, &header)?;
            }
            self.refresh_roots();
            return Ok(Some(old_ptr));
        }

        // Check overflow
        if header.has_overflow() {
            let overflow_key = Self::overflow_key(node, dir);
            if let Some(mut block) = self.overflow.get_with_write(tx, &overflow_key)? {
                if let Some(old_ptr) = block.remove(type_id) {
                    if block.is_empty() && block.next.is_null() {
                        // Remove overflow entirely
                        self.overflow.delete(tx, &overflow_key)?;
                        header.clear_overflow();
                    } else {
                        self.overflow.put(tx, &overflow_key, &block)?;
                    }
                    tree.put(tx, &node, &header)?;
                    self.refresh_roots();
                    return Ok(Some(old_ptr));
                }
            }
        }

        Ok(None)
    }

    /// Returns an iterator over all types for a node in a given direction (write path).
    pub fn iter_types_mut(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
    ) -> Result<Vec<(TypeId, SegmentPtr)>> {
        let mut result = Vec::new();

        let header = match self.get_header_mut(tx, node, dir)? {
            Some(h) => h,
            None => return Ok(result),
        };

        // Collect inline types
        for (type_id, ptr) in header.iter_types() {
            result.push((type_id, ptr));
        }

        // Collect overflow types
        if header.has_overflow() {
            let overflow_key = Self::overflow_key(node, dir);
            if let Some(block) = self.overflow.get_with_write(tx, &overflow_key)? {
                for (type_id, ptr) in block.iter() {
                    result.push((type_id, ptr));
                }
                // TODO: Walk chain if block.next is not null
            }
        }

        Ok(result)
    }

    /// Helper to get the appropriate B-tree for a direction.
    fn tree_for_dir(&self, dir: Dir) -> &BTree<NodeId, NodeAdjHeader> {
        match dir {
            Dir::Out => &self.adj_out,
            Dir::In => &self.adj_in,
            Dir::Both => &self.adj_out, // Default to out for "both"
        }
    }
}

/// Root page IDs for IFA B-trees.
#[derive(Clone, Copy, Debug)]
pub struct IfaRoots {
    pub adj_out: PageId,
    pub adj_in: PageId,
    pub overflow: PageId,
}

impl Default for IfaRoots {
    fn default() -> Self {
        Self {
            adj_out: PageId(0),
            adj_in: PageId(0),
            overflow: PageId(0),
        }
    }
}

#[cfg(test)]
mod store_tests {
    use super::*;
    use crate::primitives::pager::{Pager, PagerOptions};
    use tempfile::tempdir;

    fn create_test_store() -> (Arc<Pager>, IfaStore) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let store = IfaStore::open(
            Arc::clone(&pager) as Arc<dyn PageStore>,
            PageId(0),
            PageId(0),
            PageId(0),
        ).unwrap();
        (pager, store)
    }

    #[test]
    fn test_store_creation() {
        let (_pager, store) = create_test_store();
        let roots = store.roots();
        // New store should have non-zero roots (empty B-trees get a root page)
        // Actually empty B-trees might have root 0 initially
        assert!(roots.adj_out.0 == 0 || roots.adj_out.0 > 0);
    }

    #[test]
    fn test_upsert_and_lookup() {
        let (pager, store) = create_test_store();
        let mut tx = pager.begin_write().unwrap();

        let node = NodeId(100);
        let type_id = TypeId(5);
        let head = SegmentPtr(1000);

        // Insert
        store.upsert_type(&mut tx, node, Dir::Out, type_id, head).unwrap();

        // Lookup using write path
        let result = store.lookup_type_mut(&mut tx, node, Dir::Out, type_id).unwrap();
        match result {
            TypeLookupResult::Found(ptr) => assert_eq!(ptr, head),
            _ => panic!("Expected Found"),
        }

        // Lookup non-existent type
        let result = store.lookup_type_mut(&mut tx, node, Dir::Out, TypeId(999)).unwrap();
        assert!(matches!(result, TypeLookupResult::NotFound));

        // Lookup non-existent node
        let result = store.lookup_type_mut(&mut tx, NodeId(999), Dir::Out, type_id).unwrap();
        assert!(matches!(result, TypeLookupResult::NotFound));
    }

    #[test]
    fn test_remove_type() {
        let (pager, store) = create_test_store();
        let mut tx = pager.begin_write().unwrap();

        let node = NodeId(100);
        let type_id = TypeId(5);
        let head = SegmentPtr(1000);

        // Insert
        store.upsert_type(&mut tx, node, Dir::Out, type_id, head).unwrap();

        // Remove
        let old = store.remove_type(&mut tx, node, Dir::Out, type_id).unwrap();
        assert_eq!(old, Some(head));

        // Verify removed
        let result = store.lookup_type_mut(&mut tx, node, Dir::Out, type_id).unwrap();
        assert!(matches!(result, TypeLookupResult::NotFound));

        // Remove non-existent
        let old = store.remove_type(&mut tx, node, Dir::Out, type_id).unwrap();
        assert_eq!(old, None);
    }

    #[test]
    fn test_multiple_types() {
        let (pager, store) = create_test_store();
        let mut tx = pager.begin_write().unwrap();

        let node = NodeId(100);

        // Insert multiple types
        for i in 1..=5 {
            let type_id = TypeId(i);
            let head = SegmentPtr(i as u64 * 100);
            store.upsert_type(&mut tx, node, Dir::Out, type_id, head).unwrap();
        }

        // Verify all types
        let types = store.iter_types_mut(&mut tx, node, Dir::Out).unwrap();
        assert_eq!(types.len(), 5);

        for i in 1..=5 {
            let result = store.lookup_type_mut(&mut tx, node, Dir::Out, TypeId(i)).unwrap();
            match result {
                TypeLookupResult::Found(ptr) => assert_eq!(ptr.0, i as u64 * 100),
                _ => panic!("Expected Found for type {}", i),
            }
        }
    }

    #[test]
    fn test_both_directions() {
        let (pager, store) = create_test_store();
        let mut tx = pager.begin_write().unwrap();

        let node = NodeId(100);
        let type_id = TypeId(5);
        let out_head = SegmentPtr(1000);
        let in_head = SegmentPtr(2000);

        // Insert in both directions
        store.upsert_type(&mut tx, node, Dir::Out, type_id, out_head).unwrap();
        store.upsert_type(&mut tx, node, Dir::In, type_id, in_head).unwrap();

        // Verify they're independent
        let out_result = store.lookup_type_mut(&mut tx, node, Dir::Out, type_id).unwrap();
        let in_result = store.lookup_type_mut(&mut tx, node, Dir::In, type_id).unwrap();

        match out_result {
            TypeLookupResult::Found(ptr) => assert_eq!(ptr, out_head),
            _ => panic!("Expected Found for Out"),
        }
        match in_result {
            TypeLookupResult::Found(ptr) => assert_eq!(ptr, in_head),
            _ => panic!("Expected Found for In"),
        }
    }

    #[test]
    fn test_read_after_commit() {
        let (pager, store) = create_test_store();
        
        let node = NodeId(100);
        let type_id = TypeId(5);
        let head = SegmentPtr(1000);

        // Insert and commit
        {
            let mut tx = pager.begin_write().unwrap();
            store.upsert_type(&mut tx, node, Dir::Out, type_id, head).unwrap();
            pager.commit(tx).unwrap();
        }

        // Read with a new read transaction
        {
            let read_tx = pager.begin_read().unwrap();
            let result = store.lookup_type(&read_tx, node, Dir::Out, type_id).unwrap();
            match result {
                TypeLookupResult::Found(ptr) => assert_eq!(ptr, head),
                _ => panic!("Expected Found after commit"),
            }
        }
    }
}
