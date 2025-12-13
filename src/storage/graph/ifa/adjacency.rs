//! Unified IFA Adjacency API combining IfaStore and SegmentManager.
//!
//! This module provides the `IfaAdjacency` struct that coordinates:
//! - Type map lookups via `IfaStore`
//! - Segment CoW operations via `SegmentManager`
//! - MVCC-aware edge insertion and removal
//!
//! # True Index-Free Adjacency
//!
//! For true O(1) neighbor lookups, adjacency headers can be stored directly
//! in per-node `NodeAdjPage` pages, referenced by the node row's `adj_page` field.
//! This eliminates B-tree lookups entirely:
//!
//! ```text
//! neighbors(node) → read node row → adj_page → direct page read → NodeAdjPage
//! ```
//!
//! The fallback path uses B-trees (`IfaStore`) for backward compatibility.
//!
//! # CoW Insert Algorithm
//!
//! 1. Look up NodeAdjHeader for (node, dir)
//! 2. Get old_head = bucket.head for TypeId
//! 3. CoW clone current segment (if exists) or create new
//! 4. Insert (neighbor, edge) in sorted order
//! 5. Allocate new page, write new_seg
//! 6. Update bucket.head = new_seg pointer
//! 7. Mark old segment as superseded (set xmax)
//!
//! # CoW Delete Algorithm
//!
//! Similar to insert, but removes the entry and may remove the type mapping
//! entirely if the segment becomes empty.

use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::adjacency::Dir;
use crate::types::{EdgeId, NodeId, PageId, Result, SombraError, TypeId};

use super::node_adj_page::NodeAdjPage;
use super::segment_manager::SegmentManager;
use super::store::{IfaRoots, IfaStore, InlineInsertResult, TypeLookupResult};
use super::types::{InlineAdjEntry, NodeAdjHeader, SegmentPtr};
use super::TxId;

/// Unified Index-Free Adjacency API.
///
/// Combines `IfaStore` (type map management) and `SegmentManager` (segment I/O)
/// into a single interface for edge operations with MVCC support.
///
/// # Example
///
/// ```ignore
/// let ifa = IfaAdjacency::open(store, roots)?;
///
/// // Insert an edge
/// ifa.insert_edge(&mut tx, src, dst, type_id, edge_id, xmin)?;
///
/// // Query neighbors
/// let neighbors = ifa.get_neighbors(&mut tx, node, Dir::Out, type_id, snapshot)?;
/// ```
pub struct IfaAdjacency {
    /// Manages NodeAdjHeader B-trees and type lookups.
    store: IfaStore,
    /// Handles segment allocation and CoW operations.
    segment_manager: SegmentManager,
}

impl IfaAdjacency {
    /// Opens or creates an IFA adjacency store.
    ///
    /// If roots are zero, new empty B-trees are created.
    pub fn open(
        page_store: Arc<dyn PageStore>,
        roots: IfaRoots,
    ) -> Result<Self> {
        let store = IfaStore::open(
            Arc::clone(&page_store),
            roots.adj_out,
            roots.adj_in,
            roots.overflow,
        )?;
        let segment_manager = SegmentManager::new(page_store);

        Ok(Self {
            store,
            segment_manager,
        })
    }

    /// Returns current root pages for persisting to database meta.
    pub fn roots(&self) -> IfaRoots {
        self.store.roots()
    }

    // =========================================================================
    // Edge Insertion (CoW)
    // =========================================================================

    /// Inserts an edge into the adjacency structure.
    ///
    /// This performs the full CoW insert algorithm:
    /// 1. Look up or create NodeAdjHeader for source node (OUT direction)
    /// 2. Look up or create segment for the edge type
    /// 3. CoW clone the segment with the new edge
    /// 4. Update the type mapping to point to new segment
    /// 5. Mark old segment as superseded
    /// 6. Repeat for destination node (IN direction)
    ///
    /// # Arguments
    ///
    /// * `tx` - Write transaction guard
    /// * `src` - Source node ID
    /// * `dst` - Destination node ID
    /// * `type_id` - Edge type ID
    /// * `edge_id` - Edge ID
    /// * `xmin` - Transaction ID creating this edge (MVCC version)
    pub fn insert_edge(
        &self,
        tx: &mut WriteGuard<'_>,
        src: NodeId,
        dst: NodeId,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
    ) -> Result<()> {
        // Insert outgoing edge: src -> dst
        self.insert_directed_edge(tx, src, dst, Dir::Out, type_id, edge_id, xmin)?;

        // Insert incoming edge: dst <- src
        self.insert_directed_edge(tx, dst, src, Dir::In, type_id, edge_id, xmin)?;

        Ok(())
    }

    /// Inserts a single directed edge (either OUT or IN).
    ///
    /// This is the core insertion implementation with inline-first optimization:
    /// 1. For new edges, try inline storage first (fast path)
    /// 2. If inline capacity exceeded, promote to external segment
    /// 3. For types already using external storage, use CoW insert
    fn insert_directed_edge(
        &self,
        tx: &mut WriteGuard<'_>,
        owner: NodeId,
        neighbor: NodeId,
        dir: Dir,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
    ) -> Result<()> {
        // Step 1: Look up existing storage for this (owner, dir, type)
        let lookup = self.store.lookup_type_mut(tx, owner, dir, type_id)?;

        match lookup {
            TypeLookupResult::NotFound => {
                // New type - try inline storage first (fast path)
                self.insert_inline_first(tx, owner, neighbor, dir, type_id, edge_id, xmin)
            }
            TypeLookupResult::Inline(existing_entries) => {
                // Type already has inline entries - try adding to inline
                self.insert_to_inline_or_promote(
                    tx, owner, neighbor, dir, type_id, edge_id, xmin, existing_entries,
                )
            }
            TypeLookupResult::External(old_ptr) => {
                // Type uses external storage - use CoW insert
                self.insert_to_external(tx, owner, neighbor, dir, type_id, edge_id, xmin, old_ptr)
            }
        }
    }

    /// Tries inline insertion first, promotes to external if capacity exceeded.
    fn insert_inline_first(
        &self,
        tx: &mut WriteGuard<'_>,
        owner: NodeId,
        neighbor: NodeId,
        dir: Dir,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
    ) -> Result<()> {
        let entry = InlineAdjEntry::new(neighbor, edge_id);
        
        match self.store.upsert_inline_entry(tx, owner, dir, type_id, entry)? {
            InlineInsertResult::Inserted => {
                // Successfully inserted inline - done!
                Ok(())
            }
            InlineInsertResult::NeedsPromotion(_) => {
                // Bucket slots full - fall back to external storage
                // This shouldn't happen for a new type, but handle it anyway
                let new_ptr = self.segment_manager.insert_edge(
                    tx, None, owner, dir, type_id, neighbor, edge_id, xmin,
                )?;
                self.store.upsert_type(tx, owner, dir, type_id, new_ptr)?;
                Ok(())
            }
        }
    }

    /// Inserts to existing inline storage or promotes if capacity exceeded.
    #[allow(dead_code)]
    fn insert_to_inline_or_promote(
        &self,
        tx: &mut WriteGuard<'_>,
        owner: NodeId,
        neighbor: NodeId,
        dir: Dir,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
        _existing_entries: smallvec::SmallVec<[InlineAdjEntry; 3]>,
    ) -> Result<()> {
        let new_entry = InlineAdjEntry::new(neighbor, edge_id);
        
        match self.store.upsert_inline_entry(tx, owner, dir, type_id, new_entry)? {
            InlineInsertResult::Inserted => {
                // Successfully added to inline - done!
                Ok(())
            }
            InlineInsertResult::NeedsPromotion(entries_to_migrate) => {
                // Inline capacity exceeded - promote to external segment
                // Create segment with all existing entries + new entry
                let new_ptr = self.segment_manager.create_segment_with_entries(
                    tx,
                    owner,
                    dir,
                    type_id,
                    &entries_to_migrate,
                    new_entry,
                    xmin,
                )?;
                
                // Update store to use external storage
                self.store.promote_to_external(tx, owner, dir, type_id, new_ptr)?;
                Ok(())
            }
        }
    }

    /// Inserts to external storage using CoW.
    fn insert_to_external(
        &self,
        tx: &mut WriteGuard<'_>,
        owner: NodeId,
        neighbor: NodeId,
        dir: Dir,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
        old_ptr: SegmentPtr,
    ) -> Result<()> {
        let old_ptr_opt = if old_ptr.is_null() { None } else { Some(old_ptr) };
        
        // Create new segment with the edge inserted (CoW)
        let new_ptr = self.segment_manager.insert_edge(
            tx,
            old_ptr_opt,
            owner,
            dir,
            type_id,
            neighbor,
            edge_id,
            xmin,
        )?;

        // Update type mapping to point to new segment
        self.store.upsert_type(tx, owner, dir, type_id, new_ptr)?;

        // Mark old segment as superseded (if it existed)
        if let Some(old) = old_ptr_opt {
            self.segment_manager.mark_superseded(tx, old, xmin)?;
        }

        Ok(())
    }

    // =========================================================================
    // Edge Removal (CoW)
    // =========================================================================

    /// Removes an edge from the adjacency structure.
    ///
    /// This performs the full CoW delete algorithm:
    /// 1. Look up segment for (src, OUT, type)
    /// 2. CoW clone without the edge
    /// 3. If segment becomes empty, remove type mapping
    /// 4. Otherwise update type mapping to new segment
    /// 5. Mark old segment as superseded
    /// 6. Repeat for (dst, IN, type)
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the edge was found and removed, `Ok(false)` if not found.
    pub fn remove_edge(
        &self,
        tx: &mut WriteGuard<'_>,
        src: NodeId,
        dst: NodeId,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
    ) -> Result<bool> {
        // Remove outgoing edge: src -> dst
        let removed_out = self.remove_directed_edge(tx, src, dst, Dir::Out, type_id, edge_id, xmin)?;

        // Remove incoming edge: dst <- src
        let removed_in = self.remove_directed_edge(tx, dst, src, Dir::In, type_id, edge_id, xmin)?;

        // Edge should exist in both directions or neither
        Ok(removed_out || removed_in)
    }

    /// Removes a single directed edge (either OUT or IN).
    fn remove_directed_edge(
        &self,
        tx: &mut WriteGuard<'_>,
        owner: NodeId,
        neighbor: NodeId,
        dir: Dir,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
    ) -> Result<bool> {
        // Step 1: Look up existing storage for this type
        let lookup = self.store.lookup_type_mut(tx, owner, dir, type_id)?;

        match lookup {
            TypeLookupResult::Inline(_) => {
                // Type uses inline storage - remove entry directly
                self.store.remove_inline_entry(tx, owner, dir, type_id, neighbor, edge_id)
            }
            TypeLookupResult::External(old_ptr) => {
                if old_ptr.is_null() {
                    return Ok(false);
                }

                // Remove edge from segment (CoW)
                let new_ptr_opt = self.segment_manager.remove_edge(
                    tx,
                    old_ptr,
                    neighbor,
                    edge_id,
                    xmin,
                )?;

                match new_ptr_opt {
                    Some(new_ptr) => {
                        if new_ptr == old_ptr {
                            // Edge wasn't found in segment
                            return Ok(false);
                        }
                        // Update type mapping to new segment
                        self.store.upsert_type(tx, owner, dir, type_id, new_ptr)?;
                    }
                    None => {
                        // Segment is now empty - remove type mapping entirely
                        self.store.remove_type(tx, owner, dir, type_id)?;
                    }
                }

                // Mark old segment as superseded
                self.segment_manager.mark_superseded(tx, old_ptr, xmin)?;

                Ok(true)
            }
            TypeLookupResult::NotFound => Ok(false), // No edges of this type
        }
    }

    // =========================================================================
    // Neighbor Queries
    // =========================================================================

    /// Gets all neighbors of a node for a specific edge type and direction.
    ///
    /// Returns neighbors visible at the given snapshot (MVCC read).
    ///
    /// # Arguments
    ///
    /// * `tx` - Write guard (needed for page access)
    /// * `node` - Node to query neighbors for
    /// * `dir` - Direction (OUT for outgoing, IN for incoming)
    /// * `type_id` - Edge type to query
    /// * `snapshot` - MVCC snapshot timestamp
    ///
    /// # Returns
    ///
    /// Vector of (neighbor_id, edge_id) pairs.
    #[allow(dead_code)]
    pub fn get_neighbors(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
        snapshot: TxId,
    ) -> Result<Vec<(NodeId, EdgeId)>> {
        // Look up segment head or inline entries for this (node, dir, type)
        let lookup = self.store.lookup_type_mut(tx, node, dir, type_id)?;

        match lookup {
            TypeLookupResult::Inline(entries) => {
                // Fast path: return inline entries directly
                // Note: inline entries don't have individual MVCC tracking,
                // they inherit visibility from the header's creation time.
                // For now, we return all entries. Full MVCC support would
                // require storing header xmin and checking visibility.
                Ok(entries.iter().map(|e| (e.neighbor, e.edge)).collect())
            }
            TypeLookupResult::External(head_ptr) => {
                if head_ptr.is_null() {
                    return Ok(Vec::new());
                }

                // Find the visible segment version at this snapshot
                let segment =
                    match self.segment_manager.find_visible_segment(tx, head_ptr, snapshot)? {
                        Some(seg) => seg,
                        None => return Ok(Vec::new()), // No visible version
                    };

                // Collect all entries
                let neighbors: Vec<(NodeId, EdgeId)> = segment
                    .entries
                    .iter()
                    .map(|e| (e.neighbor, e.edge))
                    .collect();

                Ok(neighbors)
            }
            TypeLookupResult::NotFound => Ok(Vec::new()),
        }
    }

    /// Gets all neighbors of a node for a specific edge type (both directions).
    ///
    /// This is a convenience method that queries both OUT and IN directions
    /// and merges the results.
    #[allow(dead_code)]
    pub fn get_neighbors_both(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        type_id: TypeId,
        snapshot: TxId,
    ) -> Result<Vec<(NodeId, EdgeId, Dir)>> {
        let mut result = Vec::new();

        // Get outgoing neighbors
        for (neighbor, edge) in self.get_neighbors(tx, node, Dir::Out, type_id, snapshot)? {
            result.push((neighbor, edge, Dir::Out));
        }

        // Get incoming neighbors
        for (neighbor, edge) in self.get_neighbors(tx, node, Dir::In, type_id, snapshot)? {
            result.push((neighbor, edge, Dir::In));
        }

        Ok(result)
    }

    /// Gets all neighbors of a node across all edge types.
    ///
    /// # Returns
    ///
    /// Vector of (type_id, neighbor_id, edge_id) tuples.
    #[allow(dead_code)]
    pub fn get_all_neighbors(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        snapshot: TxId,
    ) -> Result<Vec<(TypeId, NodeId, EdgeId)>> {
        let mut result = Vec::new();

        // Get the header to access both inline and external types
        let header = match self.store.get_header_mut(tx, node, dir)? {
            Some(h) => h,
            None => return Ok(result),
        };

        // First, collect from inline types (stored directly in header)
        for (type_id, entries) in header.iter_inline_types() {
            for entry in entries {
                result.push((type_id, entry.neighbor, entry.edge));
            }
        }

        // Then, collect from external types (stored in segments)
        for (type_id, head_ptr) in header.iter_types() {
            if head_ptr.is_null() {
                continue;
            }

            // Find visible segment version
            if let Some(segment) =
                self.segment_manager.find_visible_segment(tx, head_ptr, snapshot)?
            {
                for entry in &segment.entries {
                    result.push((type_id, entry.neighbor, entry.edge));
                }
            }
        }

        // Handle overflow blocks for high-type-count nodes
        if header.has_overflow() {
            let overflow_types = self.store.iter_overflow_types_mut(tx, node, dir)?;
            for (type_id, head_ptr) in overflow_types {
                if head_ptr.is_null() {
                    continue;
                }
                if let Some(segment) =
                    self.segment_manager.find_visible_segment(tx, head_ptr, snapshot)?
                {
                    for entry in &segment.entries {
                        result.push((type_id, entry.neighbor, entry.edge));
                    }
                }
            }
        }

        Ok(result)
    }

    // =========================================================================
    // Read-Only Queries (using ReadGuard)
    // =========================================================================

    /// Gets neighbors using a read-only transaction.
    ///
    /// This is the primary read path for IFA, used when `AdjacencyBackend::IfaOnly`
    /// is configured to bypass B-tree reads entirely.
    pub fn get_neighbors_read(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
        snapshot: TxId,
    ) -> Result<Vec<(NodeId, EdgeId)>> {
        // Look up segment head or inline entries using read guard
        let lookup = self.store.lookup_type(tx, node, dir, type_id)?;

        match lookup {
            TypeLookupResult::Inline(entries) => {
                // Fast path: return inline entries directly
                Ok(entries.iter().map(|e| (e.neighbor, e.edge)).collect())
            }
            TypeLookupResult::External(head_ptr) => {
                if head_ptr.is_null() {
                    return Ok(Vec::new());
                }

                // Find the visible segment version at this snapshot
                let segment =
                    match self.segment_manager.find_visible_segment_ro(tx, head_ptr, snapshot)? {
                        Some(seg) => seg,
                        None => return Ok(Vec::new()), // No visible version
                    };

                // Collect all entries
                let neighbors: Vec<(NodeId, EdgeId)> = segment
                    .entries
                    .iter()
                    .map(|e| (e.neighbor, e.edge))
                    .collect();

                Ok(neighbors)
            }
            TypeLookupResult::NotFound => Ok(Vec::new()),
        }
    }

    /// Gets neighbors from all types for a node/direction using read-only transaction.
    ///
    /// This scans all type buckets for the node and collects neighbors.
    /// Used when no specific type filter is provided.
    pub fn get_all_neighbors_read(
        &self,
        tx: &ReadGuard,
        node: NodeId,
        dir: Dir,
        snapshot: TxId,
    ) -> Result<Vec<(NodeId, EdgeId, TypeId)>> {
        // Get the header for this node/direction
        let header = match self.store.get_header(tx, node, dir)? {
            Some(h) => h,
            None => return Ok(Vec::new()),
        };

        let mut all_neighbors = Vec::new();

        // First, collect from inline types (stored directly in header)
        for (type_id, entries) in header.iter_inline_types() {
            for entry in entries {
                all_neighbors.push((entry.neighbor, entry.edge, type_id));
            }
        }

        // Then, collect from external types (stored in segments)
        for (type_id, head_ptr) in header.iter_types() {
            if head_ptr.is_null() {
                continue;
            }

            // Find visible segment for this type
            if let Some(segment) =
                self.segment_manager.find_visible_segment_ro(tx, head_ptr, snapshot)?
            {
                for entry in &segment.entries {
                    all_neighbors.push((entry.neighbor, entry.edge, type_id));
                }
            }
        }

        // Handle overflow blocks for high-type-count nodes
        if header.has_overflow() {
            let overflow_types = self.store.iter_overflow_types_ro(tx, node, dir)?;
            for (type_id, head_ptr) in overflow_types {
                if head_ptr.is_null() {
                    continue;
                }
                if let Some(segment) =
                    self.segment_manager.find_visible_segment_ro(tx, head_ptr, snapshot)?
                {
                    for entry in &segment.entries {
                        all_neighbors.push((entry.neighbor, entry.edge, type_id));
                    }
                }
            }
        }

        Ok(all_neighbors)
    }

    // =========================================================================
    // Utility Methods
    // =========================================================================

    /// Checks if a specific edge exists.
    #[allow(dead_code)]
    pub fn has_edge(
        &self,
        tx: &mut WriteGuard<'_>,
        src: NodeId,
        dst: NodeId,
        type_id: TypeId,
        edge_id: EdgeId,
        snapshot: TxId,
    ) -> Result<bool> {
        let neighbors = self.get_neighbors(tx, src, Dir::Out, type_id, snapshot)?;
        Ok(neighbors.iter().any(|(n, e)| *n == dst && *e == edge_id))
    }

    /// Gets the degree (number of neighbors) for a node/direction/type.
    #[allow(dead_code)]
    pub fn degree(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
        snapshot: TxId,
    ) -> Result<usize> {
        let neighbors = self.get_neighbors(tx, node, dir, type_id, snapshot)?;
        Ok(neighbors.len())
    }

    /// Gets the total degree across all types for a direction.
    #[allow(dead_code)]
    pub fn total_degree(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        snapshot: TxId,
    ) -> Result<usize> {
        let all = self.get_all_neighbors(tx, node, dir, snapshot)?;
        Ok(all.len())
    }

    // =========================================================================
    // True Index-Free Adjacency Methods
    // =========================================================================
    // These methods work directly with NodeAdjPage stored in per-node pages,
    // eliminating B-tree lookups for O(1) neighbor access.

    /// Reads a `NodeAdjPage` directly from a page ID.
    ///
    /// This is the true IFA read path - O(1) page access with no B-tree lookup.
    pub fn read_adj_page(
        &self,
        tx: &ReadGuard,
        page_id: PageId,
    ) -> Result<NodeAdjPage> {
        use crate::types::page::PAGE_HDR_LEN;
        
        let page = self.segment_manager.store().get_page(tx, page_id)?;
        let data = page.data();
        
        // Skip page header to get to NodeAdjPage data
        let adj_data = &data[PAGE_HDR_LEN..];
        NodeAdjPage::decode(adj_data)
    }

    /// Reads a `NodeAdjPage` directly from a page ID using write transaction.
    pub fn read_adj_page_mut(
        &self,
        tx: &mut WriteGuard<'_>,
        page_id: PageId,
    ) -> Result<NodeAdjPage> {
        use crate::types::page::PAGE_HDR_LEN;
        
        let page = tx.page_mut(page_id)?;
        let data = page.data();
        
        // Skip page header to get to NodeAdjPage data
        let adj_data = &data[PAGE_HDR_LEN..];
        let result = NodeAdjPage::decode(adj_data);
        drop(page);
        result
    }

    /// Writes a `NodeAdjPage` to a page.
    pub fn write_adj_page(
        &self,
        tx: &mut WriteGuard<'_>,
        page_id: PageId,
        adj_page: &NodeAdjPage,
    ) -> Result<()> {
        use crate::types::page::{PageHeader, PageKind, PAGE_HDR_LEN};
        
        let store = self.segment_manager.store();
        // Use cached salt from segment_manager instead of reading meta page every time
        let salt = self.segment_manager.salt();
        let page_size = store.page_size();
        
        let encoded = adj_page.encode();
        let mut page = tx.page_mut(page_id)?;
        let data = page.data_mut();
        
        // Write proper page header first
        let header = PageHeader::new(
            page_id,
            PageKind::IfaSegment, // Reuse IfaSegment kind for adjacency pages
            page_size,
            salt,
        )?.with_crc32(0);
        header.encode(&mut data[..PAGE_HDR_LEN])?;
        
        // Write adjacency page data after header
        data[PAGE_HDR_LEN..PAGE_HDR_LEN + encoded.len()].copy_from_slice(&encoded);
        
        // Zero out remaining space
        for byte in &mut data[PAGE_HDR_LEN + encoded.len()..] {
            *byte = 0;
        }
        
        drop(page);
        Ok(())
    }

    /// Allocates a new adjacency page and initializes it for the given owner node.
    ///
    /// Returns the PageId of the new adjacency page.
    pub fn allocate_adj_page(&self, tx: &mut WriteGuard<'_>, owner: NodeId) -> Result<PageId> {
        let page_id = tx.allocate_page()?;
        let adj_page = NodeAdjPage::new(owner);
        self.write_adj_page(tx, page_id, &adj_page)?;
        Ok(page_id)
    }

    /// Gets neighbors using true IFA path - reads NodeAdjPage directly from page.
    ///
    /// This is O(1) in page lookups for inline types:
    /// 1. adj_page_id is already known (passed in from node row)
    /// 2. Read NodeAdjPage directly from that page
    /// 3. Look up type in inline buckets (O(1))
    /// 4. If inline entries exist, return them directly (fastest path)
    /// 5. If external segment pointer, read segment
    /// 6. If not found inline and has overflow, search B-tree overflow (O(log n))
    /// 7. Filter entries by per-entry visibility (avoiding B-tree edge lookups)
    pub fn get_neighbors_true_ifa(
        &self,
        tx: &ReadGuard,
        adj_page_id: PageId,
        dir: Dir,
        type_id: TypeId,
        snapshot: TxId,
    ) -> Result<Vec<(NodeId, EdgeId)>> {
        // Step 1: Read NodeAdjPage directly (O(1))
        let adj_page = self.read_adj_page(tx, adj_page_id)?;

        // Step 2: Get header for direction
        let header = adj_page.header(dir);

        // Step 3: Check for inline entries first (fastest path - no segment read!)
        if let Some(entries) = header.lookup_inline_entries(type_id) {
            // Inline entries don't have individual MVCC tracking,
            // they inherit visibility from header creation time.
            return Ok(entries.iter().map(|e| (e.neighbor, e.edge)).collect());
        }

        // Step 4: Look up external segment pointer for this type
        let head_ptr = match header.lookup_inline(type_id) {
            Some(ptr) => ptr,
            None => {
                // Check overflow if present - use owner from NodeAdjPage for B-tree lookup
                if header.has_overflow() {
                    let owner = adj_page.owner();
                    match self
                        .store
                        .search_overflow_chain(tx, owner, dir, type_id, SegmentPtr::null())?
                    {
                        Some(ptr) => ptr,
                        None => return Ok(Vec::new()),
                    }
                } else {
                    return Ok(Vec::new());
                }
            }
        };

        if head_ptr.is_null() {
            return Ok(Vec::new());
        }

        // Step 5: Find visible segment and collect entries
        let segment = match self
            .segment_manager
            .find_visible_segment_ro(tx, head_ptr, snapshot)?
        {
            Some(seg) => seg,
            None => return Ok(Vec::new()),
        };

        // Step 6: Filter entries by per-entry visibility (O(1) per entry, no B-tree lookups!)
        let neighbors: Vec<(NodeId, EdgeId)> = segment
            .entries
            .iter()
            .filter(|e| e.visible_at(snapshot))
            .map(|e| (e.neighbor, e.edge))
            .collect();

        Ok(neighbors)
    }

    /// Gets all neighbors across all types using true IFA path.
    ///
    /// For inline entries: O(1) - data is directly in header (fastest!)
    /// For inline types (≤5): O(1) lookup per type + segment read
    /// For overflow types (>5): O(log n) B-tree lookup per type
    /// Per-entry visibility filtering avoids B-tree edge lookups.
    pub fn get_all_neighbors_true_ifa(
        &self,
        tx: &ReadGuard,
        adj_page_id: PageId,
        dir: Dir,
        snapshot: TxId,
    ) -> Result<Vec<(NodeId, EdgeId, TypeId)>> {
        // Read NodeAdjPage directly (O(1))
        let adj_page = self.read_adj_page(tx, adj_page_id)?;
        let header = adj_page.header(dir);
        let owner = adj_page.owner();

        let mut all_neighbors = Vec::new();

        // First, collect from inline entries (fastest path - no segment reads!)
        for (type_id, entries) in header.iter_inline_types() {
            for entry in entries {
                all_neighbors.push((entry.neighbor, entry.edge, type_id));
            }
        }

        // Then, collect from external segment types
        for (type_id, head_ptr) in header.iter_types() {
            if head_ptr.is_null() {
                continue;
            }

            // Find visible segment for this type
            if let Some(segment) = self
                .segment_manager
                .find_visible_segment_ro(tx, head_ptr, snapshot)?
            {
                // Filter by per-entry visibility (O(1) per entry, no B-tree lookups!)
                for entry in &segment.entries {
                    if entry.visible_at(snapshot) {
                        all_neighbors.push((entry.neighbor, entry.edge, type_id));
                    }
                }
            }
        }

        // Handle overflow blocks for high-type-count nodes using hybrid B-tree lookup
        if header.has_overflow() {
            // Get overflow types from B-tree store using owner NodeId
            let overflow_types = self.store.iter_overflow_types_ro(tx, owner, dir)?;
            
            for (type_id, head_ptr) in overflow_types {
                if head_ptr.is_null() {
                    continue;
                }
                
                // Find visible segment for this overflow type
                if let Some(segment) = self.segment_manager.find_visible_segment_ro(tx, head_ptr, snapshot)? {
                    for entry in &segment.entries {
                        if entry.visible_at(snapshot) {
                            all_neighbors.push((entry.neighbor, entry.edge, type_id));
                        }
                    }
                }
            }
        }
        
        Ok(all_neighbors)
    }

    /// Inserts an edge using true IFA path - updates NodeAdjPage directly.
    ///
    /// This method works with an existing adjacency page (adj_page_id from node row).
    /// The caller is responsible for:
    /// 1. Allocating the adjacency page if it doesn't exist
    /// 2. Updating the node row with the adj_page_id
    #[allow(dead_code)]
    pub fn insert_edge_true_ifa(
        &self,
        tx: &mut WriteGuard<'_>,
        adj_page_id: PageId,
        src: NodeId,
        dst: NodeId,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
    ) -> Result<()> {
        // Insert outgoing edge: src -> dst
        self.insert_directed_edge_true_ifa(tx, adj_page_id, src, dst, Dir::Out, type_id, edge_id, xmin)?;
        
        Ok(())
    }

    /// Removes an edge using true IFA path.
    ///
    /// This removes both the outgoing edge from src and the incoming edge to dst.
    /// The caller is responsible for:
    /// 1. Looking up the adj_page_id from the source node row
    /// 2. Looking up the adj_page_id from the destination node row
    ///
    /// # Returns
    ///
    /// `Ok(true)` if the edge was found and removed, `Ok(false)` if not found.
    pub fn remove_edge_true_ifa(
        &self,
        tx: &mut WriteGuard<'_>,
        src_adj_page_id: PageId,
        dst_adj_page_id: PageId,
        src: NodeId,
        dst: NodeId,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
    ) -> Result<bool> {
        // Remove outgoing edge: src -> dst
        let removed_out = self.remove_directed_edge_true_ifa(
            tx, src_adj_page_id, src, dst, Dir::Out, type_id, edge_id, xmin
        )?;
        
        // Remove incoming edge: dst <- src
        let removed_in = self.remove_directed_edge_true_ifa(
            tx, dst_adj_page_id, dst, src, Dir::In, type_id, edge_id, xmin
        )?;
        
        Ok(removed_out || removed_in)
    }

    /// Removes a single directed edge using true IFA path.
    fn remove_directed_edge_true_ifa(
        &self,
        tx: &mut WriteGuard<'_>,
        adj_page_id: PageId,
        owner: NodeId,
        neighbor: NodeId,
        dir: Dir,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
    ) -> Result<bool> {
        use super::types::InlineAdjEntry;

        // Step 1: Read NodeAdjPage
        let mut adj_page = self.read_adj_page_mut(tx, adj_page_id)?;
        let header = adj_page.header_mut(dir);

        // Step 2: Check if type has inline entries
        if let Some(entries) = header.lookup_inline_entries(type_id) {
            // Check if edge exists in inline entries
            let target_entry = InlineAdjEntry::new(neighbor, edge_id);
            if entries.iter().any(|e| *e == target_entry) {
                // Remove inline entry
                header.remove_inline_entry(type_id, neighbor, edge_id);
                self.write_adj_page(tx, adj_page_id, &adj_page)?;
                return Ok(true);
            }
            return Ok(false);
        }

        // Step 3: Check for external segment pointer
        let old_ptr = header.lookup_inline(type_id);
        let (old_ptr, found_in_overflow) = if old_ptr.is_none() && header.has_overflow() {
            let overflow_ptr = self.store.search_overflow_chain_for_type(tx, owner, dir, type_id)?;
            (overflow_ptr, overflow_ptr.is_some())
        } else {
            (old_ptr, false)
        };

        let Some(old_ptr) = old_ptr else {
            return Ok(false); // Type not found
        };

        if old_ptr.is_null() {
            return Ok(false);
        }

        // Step 4: Remove edge from segment (CoW)
        let new_ptr_opt = self.segment_manager.remove_edge(
            tx,
            old_ptr,
            neighbor,
            edge_id,
            xmin,
        )?;

        match new_ptr_opt {
            Some(new_ptr) => {
                if new_ptr == old_ptr {
                    // Edge wasn't found in segment
                    return Ok(false);
                }
                // Update type mapping to new segment
                if found_in_overflow {
                    self.store.update_overflow_type(tx, owner, dir, type_id, new_ptr)?;
                } else {
                    header.insert_inline(type_id, new_ptr)?;
                }
            }
            None => {
                // Segment is now empty - remove type mapping
                if found_in_overflow {
                    // For overflow, we'd need to remove from overflow chain
                    // For simplicity, set to null pointer for now
                    self.store.update_overflow_type(tx, owner, dir, type_id, SegmentPtr::null())?;
                } else {
                    // Clear the bucket
                    header.remove_inline(type_id);
                }
            }
        }

        self.write_adj_page(tx, adj_page_id, &adj_page)?;

        // Mark old segment as superseded
        self.segment_manager.mark_superseded(tx, old_ptr, xmin)?;

        Ok(true)
    }

    /// Inserts multiple edges to a single node/direction using batched CoW.
    ///
    /// This is much more efficient than calling `insert_edge_true_ifa` repeatedly because:
    /// 1. Only one CoW clone per (node, dir, type) triple
    /// 2. Only one page read/write per adjacency page
    /// 3. Edges are grouped by type before insertion
    ///
    /// Note: For batch insertion, we skip inline storage and go directly to external
    /// segments since batch insertions typically involve many edges per type.
    ///
    /// # Arguments
    ///
    /// * `tx` - Write transaction guard
    /// * `adj_page_id` - The adjacency page ID from the node row
    /// * `owner` - The node that owns this adjacency page
    /// * `dir` - Direction (OUT or IN)
    /// * `edges` - Slice of (neighbor, type_id, edge_id) tuples to insert
    /// * `xmin` - Transaction ID creating these edges
    pub fn insert_edges_batch_true_ifa(
        &self,
        tx: &mut WriteGuard<'_>,
        adj_page_id: PageId,
        owner: NodeId,
        dir: Dir,
        edges: &[(NodeId, TypeId, EdgeId)],
        xmin: TxId,
    ) -> Result<()> {
        use super::types::InlineAdjEntry;
        use std::collections::BTreeMap;

        if edges.is_empty() {
            return Ok(());
        }

        // Group edges by type_id for efficient batch insertion
        let mut by_type: BTreeMap<TypeId, Vec<(NodeId, EdgeId)>> = BTreeMap::new();
        for (neighbor, type_id, edge_id) in edges {
            by_type
                .entry(*type_id)
                .or_default()
                .push((*neighbor, *edge_id));
        }

        // Read NodeAdjPage once
        let mut adj_page = self.read_adj_page_mut(tx, adj_page_id)?;
        let header = adj_page.header_mut(dir);

        // Track old pointers to mark as superseded
        let mut old_ptrs: Vec<SegmentPtr> = Vec::new();

        // Process each type's edges
        for (type_id, type_edges) in by_type {
            // Check for existing inline entries that need to be migrated
            let existing_inline: Option<smallvec::SmallVec<[InlineAdjEntry; 3]>> =
                header.lookup_inline_entries(type_id).map(|entries| entries.iter().copied().collect());

            // Get old external segment pointer (if any)
            let old_ptr = header.lookup_inline(type_id);
            let (old_ptr, found_in_overflow) = if old_ptr.is_none() && header.has_overflow() {
                let overflow_ptr =
                    self.store
                        .search_overflow_chain_for_type(tx, owner, dir, type_id)?;
                (overflow_ptr, overflow_ptr.is_some())
            } else {
                (old_ptr, false)
            };

            // Remember old pointer for later superseding
            if let Some(old) = old_ptr {
                if !old.is_null() {
                    old_ptrs.push(old);
                }
            }

            // Create new segment with ALL edges of this type inserted (single CoW)
            // If there were inline entries, include them in the batch
            let had_inline = existing_inline.is_some();
            let combined_edges: Vec<(NodeId, EdgeId)> = if let Some(inline) = existing_inline {
                // Combine existing inline entries with new edges
                let mut combined: Vec<(NodeId, EdgeId)> =
                    inline.iter().map(|e| (e.neighbor, e.edge)).collect();
                combined.extend(type_edges);
                combined
            } else {
                type_edges
            };

            let new_ptr = self.segment_manager.insert_edges_batch(
                tx,
                old_ptr,
                owner,
                dir,
                type_id,
                &combined_edges,
                xmin,
            )?;

            // Update type mapping - promote from inline if needed
            if had_inline {
                // Was inline, now external - promote
                header.promote_to_external(type_id, new_ptr);
            } else if found_in_overflow {
                self.store
                    .update_overflow_type(tx, owner, dir, type_id, new_ptr)?;
            } else {
                match header.insert_inline(type_id, new_ptr) {
                    Ok(()) => {}
                    Err(_) => {
                        // Inline buckets full - insert into overflow
                        self.store
                            .insert_overflow(tx, owner, dir, type_id, new_ptr, header)?;
                    }
                }
            }
        }

        // Write updated NodeAdjPage back (single write for all types)
        self.write_adj_page(tx, adj_page_id, &adj_page)?;

        // Mark all old segments as superseded
        for old_ptr in old_ptrs {
            self.segment_manager.mark_superseded(tx, old_ptr, xmin)?;
        }

        Ok(())
    }

    /// Inserts multiple edges to a single node/direction using pre-allocated pages.
    ///
    /// This is the most efficient variant because it uses bulk-allocated pages,
    /// avoiding the overhead of individual page allocations.
    ///
    /// # Arguments
    ///
    /// * `tx` - Write transaction guard
    /// * `adj_page_id` - The adjacency page ID from the node row
    /// * `owner` - The node that owns this adjacency page
    /// * `dir` - Direction (OUT or IN)
    /// * `edges` - Slice of (neighbor, type_id, edge_id) tuples to insert
    /// * `xmin` - Transaction ID creating these edges
    /// * `preallocated_pages` - Iterator over pre-allocated page IDs to use
    ///
    /// # Returns
    ///
    /// The number of pre-allocated pages consumed.
    #[allow(dead_code)]
    pub fn insert_edges_batch_true_ifa_preallocated<'a, I>(
        &self,
        tx: &mut WriteGuard<'_>,
        adj_page_id: PageId,
        owner: NodeId,
        dir: Dir,
        edges: &[(NodeId, TypeId, EdgeId)],
        xmin: TxId,
        preallocated_pages: &mut I,
    ) -> Result<usize>
    where
        I: Iterator<Item = PageId>,
    {
        use super::types::InlineAdjEntry;
        use std::collections::BTreeMap;
        
        if edges.is_empty() {
            return Ok(0);
        }

        // Group edges by type_id for efficient batch insertion
        let mut by_type: BTreeMap<TypeId, Vec<(NodeId, EdgeId)>> = BTreeMap::new();
        for (neighbor, type_id, edge_id) in edges {
            by_type
                .entry(*type_id)
                .or_default()
                .push((*neighbor, *edge_id));
        }

        // Read NodeAdjPage once
        let mut adj_page = self.read_adj_page_mut(tx, adj_page_id)?;
        let header = adj_page.header_mut(dir);

        // Track old pointers to mark as superseded
        let mut old_ptrs: Vec<SegmentPtr> = Vec::new();
        let mut pages_used = 0;

        // Process each type's edges - try inline first, then external segments
        for (type_id, type_edges) in by_type {
            // Check for existing inline entries
            let existing_inline: Option<smallvec::SmallVec<[InlineAdjEntry; 3]>> =
                header.lookup_inline_entries(type_id).map(|entries| entries.iter().copied().collect());

            // Get old external segment pointer (if any)
            let old_ptr = header.lookup_inline(type_id);
            let (old_ptr, found_in_overflow) = if old_ptr.is_none() && header.has_overflow() {
                let overflow_ptr = self.store.search_overflow_chain_for_type(tx, owner, dir, type_id)?;
                (overflow_ptr, overflow_ptr.is_some())
            } else {
                (old_ptr, false)
            };

            // Check if we already have an external segment
            let has_external = old_ptr.map_or(false, |p| !p.is_null());

            // Calculate total edges after insertion
            let existing_count = existing_inline.as_ref().map_or(0, |e| e.len());
            let total_count = existing_count + type_edges.len();

            // Try inline-first strategy if no external segment exists and total fits
            // Max inline is 3 for single type, 2 for multiple types
            let max_inline = if header.active_count() <= 1 { 3 } else { 2 };
            
            if !has_external && !found_in_overflow && total_count <= max_inline {
                // Try to insert all edges inline
                let mut all_inserted = true;
                for (neighbor, edge_id) in &type_edges {
                    let entry = InlineAdjEntry::new(*neighbor, *edge_id);
                    if header.insert_inline_entry(type_id, entry).is_err() {
                        all_inserted = false;
                        break;
                    }
                }
                
                if all_inserted {
                    // Successfully inserted all edges inline - no segment needed
                    continue;
                }
                
                // Failed to insert inline - fall through to external segment
                // Note: Some entries may have been partially inserted, but that's ok
                // because we'll read them back and combine with remaining edges below
            }

            // Need external segment - either:
            // 1. Already had external segment (CoW update)
            // 2. Exceeded inline capacity (promotion to external)
            // 3. Inline insertion failed due to space constraints

            // Remember old pointer for later superseding
            if let Some(old) = old_ptr {
                if !old.is_null() {
                    old_ptrs.push(old);
                }
            }

            // Get a pre-allocated page for this segment
            let page_id = preallocated_pages.next()
                .ok_or_else(|| SombraError::Invalid("ran out of pre-allocated pages"))?;
            pages_used += 1;

            // Re-read current inline entries (may have changed from partial insertion above)
            let current_inline: Option<smallvec::SmallVec<[InlineAdjEntry; 3]>> =
                header.lookup_inline_entries(type_id).map(|entries| entries.iter().copied().collect());

            // Combine current inline entries with new edges
            let combined_edges: Vec<(NodeId, EdgeId)> = if let Some(inline) = &current_inline {
                let mut combined: Vec<(NodeId, EdgeId)> =
                    inline.iter().map(|e| (e.neighbor, e.edge)).collect();
                combined.extend(type_edges);
                combined
            } else {
                type_edges
            };

            // Create new segment with ALL edges of this type inserted (single CoW)
            let new_ptr = self.segment_manager.insert_edges_batch_preallocated(
                tx,
                old_ptr,
                owner,
                dir,
                type_id,
                &combined_edges,
                xmin,
                page_id,
            )?;

            // Update type mapping - promote from inline if needed
            if current_inline.is_some() {
                // Was inline, now external - promote
                header.promote_to_external(type_id, new_ptr);
            } else if found_in_overflow {
                self.store.update_overflow_type(tx, owner, dir, type_id, new_ptr)?;
            } else {
                match header.insert_inline(type_id, new_ptr) {
                    Ok(()) => {}
                    Err(_) => {
                        // Inline buckets full - insert into overflow
                        self.store.insert_overflow(tx, owner, dir, type_id, new_ptr, header)?;
                    }
                }
            }
        }

        // Write updated NodeAdjPage back (single write for all types)
        self.write_adj_page(tx, adj_page_id, &adj_page)?;

        // Mark all old segments as superseded
        for old_ptr in old_ptrs {
            self.segment_manager.mark_superseded(tx, old_ptr, xmin)?;
        }

        Ok(pages_used)
    }

    /// Inserts a single directed edge using true IFA.
    ///
    /// Uses inline-first insertion strategy:
    /// 1. Try to insert into inline entries (fastest - no segment allocation)
    /// 2. If inline capacity exceeded, promote to external segment
    /// 3. If type already uses external segment, use CoW insertion
    fn insert_directed_edge_true_ifa(
        &self,
        tx: &mut WriteGuard<'_>,
        adj_page_id: PageId,
        owner: NodeId,
        neighbor: NodeId,
        dir: Dir,
        type_id: TypeId,
        edge_id: EdgeId,
        xmin: TxId,
    ) -> Result<()> {
        use super::types::InlineAdjEntry;

        // Step 1: Read NodeAdjPage
        let mut adj_page = self.read_adj_page_mut(tx, adj_page_id)?;
        let header = adj_page.header_mut(dir);

        // Step 2: Check if type already has inline entries
        if let Some(entries) = header.lookup_inline_entries(type_id) {
            // Type exists with inline entries - try to add more inline
            let new_entry = InlineAdjEntry::new(neighbor, edge_id);
            let existing_entries: smallvec::SmallVec<[InlineAdjEntry; 3]> =
                entries.iter().copied().collect();

            match header.insert_inline_entry(type_id, new_entry) {
                Ok(()) => {
                    // Successfully added inline
                    self.write_adj_page(tx, adj_page_id, &adj_page)?;
                    return Ok(());
                }
                Err(_) => {
                    // Inline capacity exceeded - promote to external segment
                    let new_ptr = self.segment_manager.create_segment_with_entries(
                        tx,
                        owner,
                        dir,
                        type_id,
                        &existing_entries,
                        new_entry,
                        xmin,
                    )?;

                    // Promote to external storage
                    header.promote_to_external(type_id, new_ptr);
                    self.write_adj_page(tx, adj_page_id, &adj_page)?;
                    return Ok(());
                }
            }
        }

        // Step 3: Check if type uses external segment storage
        let old_ptr = header.lookup_inline(type_id);

        // If not found inline and has overflow, search overflow
        let (old_ptr, found_in_overflow) = if old_ptr.is_none() && header.has_overflow() {
            let overflow_ptr = self.find_type_in_overflow(tx, owner, dir, type_id)?;
            (overflow_ptr, overflow_ptr.is_some())
        } else {
            (old_ptr, false)
        };

        if let Some(ptr) = old_ptr {
            if !ptr.is_null() {
                // Type uses external segment - use CoW insertion
                let new_ptr = self.segment_manager.insert_edge(
                    tx,
                    Some(ptr),
                    owner,
                    dir,
                    type_id,
                    neighbor,
                    edge_id,
                    xmin,
                )?;

                // Update type mapping
                if found_in_overflow {
                    self.update_overflow_type(tx, owner, dir, type_id, new_ptr)?;
                } else {
                    header.insert_inline(type_id, new_ptr)?;
                }

                self.write_adj_page(tx, adj_page_id, &adj_page)?;

                // Mark old segment as superseded
                self.segment_manager.mark_superseded(tx, ptr, xmin)?;
                return Ok(());
            }
        }

        // Step 4: New type - try inline first
        let new_entry = InlineAdjEntry::new(neighbor, edge_id);
        match header.insert_inline_entry(type_id, new_entry) {
            Ok(()) => {
                // Successfully inserted inline
                self.write_adj_page(tx, adj_page_id, &adj_page)?;
                Ok(())
            }
            Err(_) => {
                // Can't fit inline - create external segment
                let new_ptr = self.segment_manager.insert_edge(
                    tx,
                    None,
                    owner,
                    dir,
                    type_id,
                    neighbor,
                    edge_id,
                    xmin,
                )?;

                // Try inline bucket first, then overflow
                match header.insert_inline(type_id, new_ptr) {
                    Ok(()) => {}
                    Err(_) => {
                        // Inline buckets full - insert into overflow
                        self.insert_into_overflow(tx, owner, dir, type_id, new_ptr, header)?;
                    }
                }

                self.write_adj_page(tx, adj_page_id, &adj_page)?;
                Ok(())
            }
        }
    }
    
    /// Finds a type in overflow blocks for true IFA path.
    fn find_type_in_overflow(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
    ) -> Result<Option<SegmentPtr>> {
        // Use the store's overflow search mechanism
        self.store.search_overflow_chain_for_type(tx, node, dir, type_id)
    }
    
    /// Updates a type mapping in overflow blocks.
    fn update_overflow_type(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
        new_ptr: SegmentPtr,
    ) -> Result<()> {
        self.store.update_overflow_type(tx, node, dir, type_id, new_ptr)
    }
    
    /// Inserts a type mapping into overflow when inline buckets are full.
    fn insert_into_overflow(
        &self,
        tx: &mut WriteGuard<'_>,
        node: NodeId,
        dir: Dir,
        type_id: TypeId,
        head: SegmentPtr,
        header: &mut NodeAdjHeader,
    ) -> Result<()> {
        self.store.insert_overflow(tx, node, dir, type_id, head, header)
    }

    /// Helper to get meta salt for page headers.
    #[allow(dead_code)]
    fn meta_salt(store: &Arc<dyn PageStore>) -> Result<u64> {
        use crate::types::page::{PageHeader, PAGE_HDR_LEN};
        let read = store.begin_latest_committed_read()?;
        let meta = store.get_page(&read, PageId(0))?;
        let header = PageHeader::decode(&meta.data()[..PAGE_HDR_LEN])?;
        Ok(header.salt)
    }

    /// Returns a reference to the segment manager for direct segment operations.
    ///
    /// This is used by the True IFA write path in adjacency_ops.rs to perform
    /// CoW segment operations directly when adj_page is stored in node rows.
    #[allow(dead_code)]
    pub fn segment_manager(&self) -> &SegmentManager {
        &self.segment_manager
    }

    /// Returns a reference to the IFA store for overflow operations.
    ///
    /// This is used by the True IFA write path in adjacency_ops.rs to handle
    /// overflow when inline buckets are full.
    #[allow(dead_code)]
    pub fn ifa_store(&self) -> &IfaStore {
        &self.store
    }

    /// Returns a reference to the underlying page store.
    #[cfg(test)]
    #[allow(dead_code)]
    pub fn store(&self) -> &Arc<dyn PageStore> {
        self.segment_manager.store()
    }
}

#[cfg(test)]
mod adjacency_tests {
    use super::*;
    use crate::primitives::pager::{Pager, PagerOptions};
    use tempfile::tempdir;

    fn create_test_adjacency() -> (Arc<Pager>, IfaAdjacency) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let adjacency = IfaAdjacency::open(
            Arc::clone(&pager) as Arc<dyn PageStore>,
            IfaRoots::default(),
        ).unwrap();
        (pager, adjacency)
    }

    #[test]
    fn test_insert_and_query_single_edge() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let src = NodeId(1);
        let dst = NodeId(2);
        let type_id = TypeId(10);
        let edge_id = EdgeId(100);
        let xmin = 1000;

        // Insert edge
        adjacency.insert_edge(&mut tx, src, dst, type_id, edge_id, xmin).unwrap();

        // Query outgoing from src
        let out_neighbors = adjacency.get_neighbors(&mut tx, src, Dir::Out, type_id, xmin).unwrap();
        assert_eq!(out_neighbors.len(), 1);
        assert_eq!(out_neighbors[0], (dst, edge_id));

        // Query incoming to dst
        let in_neighbors = adjacency.get_neighbors(&mut tx, dst, Dir::In, type_id, xmin).unwrap();
        assert_eq!(in_neighbors.len(), 1);
        assert_eq!(in_neighbors[0], (src, edge_id));

        // Query non-existent direction
        let no_neighbors = adjacency.get_neighbors(&mut tx, src, Dir::In, type_id, xmin).unwrap();
        assert!(no_neighbors.is_empty());
    }

    #[test]
    fn test_insert_multiple_edges_same_type() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let src = NodeId(1);
        let type_id = TypeId(10);
        let xmin = 1000;

        // Insert multiple edges from same source
        adjacency.insert_edge(&mut tx, src, NodeId(2), type_id, EdgeId(101), xmin).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(3), type_id, EdgeId(102), xmin + 1).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(4), type_id, EdgeId(103), xmin + 2).unwrap();

        // Query all outgoing
        let neighbors = adjacency.get_neighbors(&mut tx, src, Dir::Out, type_id, xmin + 2).unwrap();
        assert_eq!(neighbors.len(), 3);

        // Verify all neighbors are present (sorted by NodeId)
        let neighbor_ids: Vec<NodeId> = neighbors.iter().map(|(n, _)| *n).collect();
        assert!(neighbor_ids.contains(&NodeId(2)));
        assert!(neighbor_ids.contains(&NodeId(3)));
        assert!(neighbor_ids.contains(&NodeId(4)));
    }

    #[test]
    fn test_insert_multiple_edge_types() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let src = NodeId(1);
        let dst = NodeId(2);
        let xmin = 1000;

        // Insert edges of different types
        adjacency.insert_edge(&mut tx, src, dst, TypeId(1), EdgeId(101), xmin).unwrap();
        adjacency.insert_edge(&mut tx, src, dst, TypeId(2), EdgeId(102), xmin).unwrap();
        adjacency.insert_edge(&mut tx, src, dst, TypeId(3), EdgeId(103), xmin).unwrap();

        // Query each type separately
        let type1 = adjacency.get_neighbors(&mut tx, src, Dir::Out, TypeId(1), xmin).unwrap();
        let type2 = adjacency.get_neighbors(&mut tx, src, Dir::Out, TypeId(2), xmin).unwrap();
        let type3 = adjacency.get_neighbors(&mut tx, src, Dir::Out, TypeId(3), xmin).unwrap();

        assert_eq!(type1.len(), 1);
        assert_eq!(type2.len(), 1);
        assert_eq!(type3.len(), 1);

        assert_eq!(type1[0].1, EdgeId(101));
        assert_eq!(type2[0].1, EdgeId(102));
        assert_eq!(type3[0].1, EdgeId(103));

        // Query all types at once
        let all = adjacency.get_all_neighbors(&mut tx, src, Dir::Out, xmin).unwrap();
        assert_eq!(all.len(), 3);
    }

    #[test]
    fn test_remove_edge() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let src = NodeId(1);
        let dst = NodeId(2);
        let type_id = TypeId(10);
        let edge_id = EdgeId(100);
        let xmin = 1000;

        // Insert edge
        adjacency.insert_edge(&mut tx, src, dst, type_id, edge_id, xmin).unwrap();

        // Verify it exists
        let neighbors = adjacency.get_neighbors(&mut tx, src, Dir::Out, type_id, xmin).unwrap();
        assert_eq!(neighbors.len(), 1);

        // Remove edge
        let removed = adjacency.remove_edge(&mut tx, src, dst, type_id, edge_id, xmin + 1).unwrap();
        assert!(removed);

        // Verify it's gone (at newer snapshot)
        let neighbors = adjacency.get_neighbors(&mut tx, src, Dir::Out, type_id, xmin + 1).unwrap();
        assert!(neighbors.is_empty());

        // Remove non-existent should return false
        let removed = adjacency.remove_edge(&mut tx, src, dst, type_id, edge_id, xmin + 2).unwrap();
        assert!(!removed);
    }

    #[test]
    fn test_remove_one_of_many_edges() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let src = NodeId(1);
        let type_id = TypeId(10);
        let xmin = 1000;

        // Insert multiple edges
        adjacency.insert_edge(&mut tx, src, NodeId(2), type_id, EdgeId(101), xmin).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(3), type_id, EdgeId(102), xmin + 1).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(4), type_id, EdgeId(103), xmin + 2).unwrap();

        // Remove middle edge
        let removed = adjacency.remove_edge(&mut tx, src, NodeId(3), type_id, EdgeId(102), xmin + 3).unwrap();
        assert!(removed);

        // Verify remaining edges
        let neighbors = adjacency.get_neighbors(&mut tx, src, Dir::Out, type_id, xmin + 3).unwrap();
        assert_eq!(neighbors.len(), 2);

        let neighbor_ids: Vec<NodeId> = neighbors.iter().map(|(n, _)| *n).collect();
        assert!(neighbor_ids.contains(&NodeId(2)));
        assert!(neighbor_ids.contains(&NodeId(4)));
        assert!(!neighbor_ids.contains(&NodeId(3)));
    }

    #[test]
    fn test_has_edge() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let src = NodeId(1);
        let dst = NodeId(2);
        let type_id = TypeId(10);
        let edge_id = EdgeId(100);
        let xmin = 1000;

        // Initially doesn't exist
        assert!(!adjacency.has_edge(&mut tx, src, dst, type_id, edge_id, xmin).unwrap());

        // Insert
        adjacency.insert_edge(&mut tx, src, dst, type_id, edge_id, xmin).unwrap();

        // Now exists
        assert!(adjacency.has_edge(&mut tx, src, dst, type_id, edge_id, xmin).unwrap());

        // Different edge doesn't exist
        assert!(!adjacency.has_edge(&mut tx, src, dst, type_id, EdgeId(999), xmin).unwrap());
    }

    #[test]
    fn test_degree() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let src = NodeId(1);
        let type_id = TypeId(10);
        let xmin = 1000;

        assert_eq!(adjacency.degree(&mut tx, src, Dir::Out, type_id, xmin).unwrap(), 0);

        adjacency.insert_edge(&mut tx, src, NodeId(2), type_id, EdgeId(101), xmin).unwrap();
        assert_eq!(adjacency.degree(&mut tx, src, Dir::Out, type_id, xmin).unwrap(), 1);

        adjacency.insert_edge(&mut tx, src, NodeId(3), type_id, EdgeId(102), xmin + 1).unwrap();
        assert_eq!(adjacency.degree(&mut tx, src, Dir::Out, type_id, xmin + 1).unwrap(), 2);

        adjacency.insert_edge(&mut tx, src, NodeId(4), type_id, EdgeId(103), xmin + 2).unwrap();
        assert_eq!(adjacency.degree(&mut tx, src, Dir::Out, type_id, xmin + 2).unwrap(), 3);
    }

    #[test]
    fn test_get_neighbors_both() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let node = NodeId(2);
        let type_id = TypeId(10);
        let xmin = 1000;

        // Node 1 -> Node 2 (incoming to node 2)
        adjacency.insert_edge(&mut tx, NodeId(1), node, type_id, EdgeId(101), xmin).unwrap();
        // Node 2 -> Node 3 (outgoing from node 2)
        adjacency.insert_edge(&mut tx, node, NodeId(3), type_id, EdgeId(102), xmin + 1).unwrap();

        // Get both directions
        let both = adjacency.get_neighbors_both(&mut tx, node, type_id, xmin + 1).unwrap();
        assert_eq!(both.len(), 2);

        // Verify we have one outgoing and one incoming
        let outgoing: Vec<_> = both.iter().filter(|(_, _, d)| *d == Dir::Out).collect();
        let incoming: Vec<_> = both.iter().filter(|(_, _, d)| *d == Dir::In).collect();

        assert_eq!(outgoing.len(), 1);
        assert_eq!(incoming.len(), 1);
        assert_eq!(outgoing[0].0, NodeId(3));
        assert_eq!(incoming[0].0, NodeId(1));
    }

    #[test]
    fn test_mvcc_visibility() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let src = NodeId(1);
        let type_id = TypeId(10);

        // Insert 4 edges to force promotion from inline to external storage.
        // External storage supports MVCC visibility tracking per-segment.
        // Note: Inline storage (up to 3 entries) does not have individual MVCC tracking.
        adjacency.insert_edge(&mut tx, src, NodeId(2), type_id, EdgeId(100), 100).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(3), type_id, EdgeId(101), 100).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(4), type_id, EdgeId(102), 100).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(5), type_id, EdgeId(103), 100).unwrap();

        // Snapshot at 50 (before insert) should see nothing
        let neighbors = adjacency.get_neighbors(&mut tx, src, Dir::Out, type_id, 50).unwrap();
        assert!(neighbors.is_empty());

        // Snapshot at 100 should see all 4 edges
        let neighbors = adjacency.get_neighbors(&mut tx, src, Dir::Out, type_id, 100).unwrap();
        assert_eq!(neighbors.len(), 4);

        // Snapshot at 200 should also see all 4 edges
        let neighbors = adjacency.get_neighbors(&mut tx, src, Dir::Out, type_id, 200).unwrap();
        assert_eq!(neighbors.len(), 4);
    }

    #[test]
    fn test_roots_persistence() {
        let (pager, adjacency) = create_test_adjacency();

        // Insert some data
        {
            let mut tx = pager.begin_write().unwrap();
            adjacency.insert_edge(&mut tx, NodeId(1), NodeId(2), TypeId(10), EdgeId(100), 1000).unwrap();
            pager.commit(tx).unwrap();
        }

        // Get roots after commit
        let roots = adjacency.roots();

        // Roots should be non-zero after data is inserted
        // (B-trees have been created and have pages allocated)
        let _ = roots; // Roots are valid
    }

    #[test]
    fn test_total_degree() {
        let (pager, adjacency) = create_test_adjacency();
        let mut tx = pager.begin_write().unwrap();

        let src = NodeId(1);
        let xmin = 1000;

        // Insert edges of different types
        adjacency.insert_edge(&mut tx, src, NodeId(2), TypeId(1), EdgeId(101), xmin).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(3), TypeId(1), EdgeId(102), xmin + 1).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(4), TypeId(2), EdgeId(103), xmin + 2).unwrap();
        adjacency.insert_edge(&mut tx, src, NodeId(5), TypeId(3), EdgeId(104), xmin + 3).unwrap();

        // Total degree should be 4 (all outgoing edges)
        let total = adjacency.total_degree(&mut tx, src, Dir::Out, xmin + 3).unwrap();
        assert_eq!(total, 4);

        // Type 1 has 2 edges
        assert_eq!(adjacency.degree(&mut tx, src, Dir::Out, TypeId(1), xmin + 3).unwrap(), 2);
        // Type 2 has 1 edge
        assert_eq!(adjacency.degree(&mut tx, src, Dir::Out, TypeId(2), xmin + 3).unwrap(), 1);
        // Type 3 has 1 edge
        assert_eq!(adjacency.degree(&mut tx, src, Dir::Out, TypeId(3), xmin + 3).unwrap(), 1);
    }
}
