//! Segment Manager - handles AdjSegment allocation and CoW operations.
//!
//! This module provides the `SegmentManager` that coordinates:
//! - Allocation of pages for new AdjSegments
//! - Copy-on-Write segment creation
//! - Segment serialization to/from pages

use std::convert::TryInto;
use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::adjacency::Dir;
use crate::types::page::{PageHeader, PageKind, PAGE_HDR_LEN};
use crate::types::{EdgeId, NodeId, PageId, Result, SombraError, TypeId};

use super::segment::{AdjEntry, AdjSegment, AdjSegmentHeader, ADJ_SEGMENT_HEADER_LEN, ADJ_ENTRY_LEN};
use super::types::{InlineAdjEntry, SegmentPtr};
use super::TxId;

/// Manager for AdjSegment allocation and CoW operations.
///
/// The SegmentManager is responsible for:
/// - Allocating new pages for AdjSegments
/// - Reading segments from pages
/// - Writing segments to pages
/// - Creating CoW clones of segments
pub struct SegmentManager {
    store: Arc<dyn PageStore>,
    page_size: usize,
    /// Salt from meta page, used for page header checksums.
    salt: u64,
}

impl SegmentManager {
    /// Creates a new segment manager.
    pub fn new(store: Arc<dyn PageStore>) -> Self {
        let page_size = store.page_size() as usize;
        let salt = Self::meta_salt(&store).unwrap_or(0);
        Self { store, page_size, salt }
    }

    /// Returns a reference to the underlying page store.
    pub fn store(&self) -> &Arc<dyn PageStore> {
        &self.store
    }

    /// Returns the cached salt value for page headers.
    pub fn salt(&self) -> u64 {
        self.salt
    }

    /// Gets the salt from the meta page.
    fn meta_salt(store: &Arc<dyn PageStore>) -> Result<u64> {
        let read = store.begin_latest_committed_read()?;
        let meta = store.get_page(&read, PageId(0))?;
        let header = PageHeader::decode(&meta.data()[..PAGE_HDR_LEN])?;
        Ok(header.salt)
    }

    /// Returns the maximum number of entries that can fit in a single page.
    pub fn max_entries_per_page(&self) -> usize {
        if self.page_size <= ADJ_SEGMENT_HEADER_LEN {
            return 0;
        }
        (self.page_size - ADJ_SEGMENT_HEADER_LEN) / ADJ_ENTRY_LEN
    }

    /// Reads an AdjSegment from a page.
    ///
    /// Returns `None` if the page doesn't contain a valid segment.
    pub fn read_segment(&self, tx: &mut WriteGuard<'_>, ptr: SegmentPtr) -> Result<Option<AdjSegment>> {
        if ptr.is_null() {
            return Ok(None);
        }

        let page_id = ptr.to_page();
        let page = tx.page_mut(page_id)?;
        let data = page.data();

        // Skip page header to get to segment data
        let segment_data = &data[PAGE_HDR_LEN..];
        
        if segment_data.len() < ADJ_SEGMENT_HEADER_LEN {
            return Err(SombraError::Corruption("segment page too small"));
        }

        let segment = AdjSegment::decode(segment_data)?;
        drop(page);
        Ok(Some(segment))
    }

    /// Reads an AdjSegment from a page using a read-only transaction.
    ///
    /// Returns `None` if the page doesn't contain a valid segment.
    pub fn read_segment_ro(&self, tx: &ReadGuard, ptr: SegmentPtr) -> Result<Option<AdjSegment>> {
        if ptr.is_null() {
            return Ok(None);
        }

        let page_id = ptr.to_page();
        let page = self.store.get_page(tx, page_id)?;
        let data = page.data();

        // Skip page header to get to segment data
        let segment_data = &data[PAGE_HDR_LEN..];
        
        if segment_data.len() < ADJ_SEGMENT_HEADER_LEN {
            return Err(SombraError::Corruption("segment page too small"));
        }

        let segment = AdjSegment::decode(segment_data)?;
        Ok(Some(segment))
    }

    /// Writes an AdjSegment to a page with a proper page header.
    ///
    /// The segment must fit within a single page. For larger segments,
    /// use extent chaining (not yet implemented).
    pub fn write_segment(
        &self,
        tx: &mut WriteGuard<'_>,
        page_id: PageId,
        segment: &AdjSegment,
    ) -> Result<()> {
        let encoded = segment.encode();
        let total_len = PAGE_HDR_LEN + encoded.len();

        if total_len > self.page_size {
            return Err(SombraError::Invalid("segment too large for page"));
        }

        let mut page = tx.page_mut(page_id)?;
        let data = page.data_mut();

        // Write proper page header first
        let header = PageHeader::new(
            page_id,
            PageKind::IfaSegment,
            self.store.page_size(),
            self.salt,
        )?.with_crc32(0);
        header.encode(&mut data[..PAGE_HDR_LEN])?;

        // Write segment data after page header
        data[PAGE_HDR_LEN..PAGE_HDR_LEN + encoded.len()].copy_from_slice(&encoded);

        // Zero out remaining space
        for byte in &mut data[PAGE_HDR_LEN + encoded.len()..] {
            *byte = 0;
        }

        drop(page);
        Ok(())
    }

    /// Allocates a new page and writes a segment to it.
    ///
    /// Returns the SegmentPtr for the new page.
    pub fn allocate_segment(
        &self,
        tx: &mut WriteGuard<'_>,
        segment: &AdjSegment,
    ) -> Result<SegmentPtr> {
        let page_id = tx.allocate_page()?;
        self.write_segment(tx, page_id, segment)?;
        Ok(SegmentPtr::from_page(page_id))
    }

    /// Creates a new segment as a CoW clone of an existing segment.
    ///
    /// This is the core operation for MVCC adjacency updates:
    #[allow(dead_code)]
    /// 1. Read the current segment
    /// 2. Clone it with new MVCC metadata
    /// 3. Allocate a new page for the clone
    /// 4. Write the clone
    ///
    /// Returns the new segment pointer.
    pub fn cow_clone(
        &self,
        tx: &mut WriteGuard<'_>,
        old_ptr: SegmentPtr,
        new_xmin: TxId,
    ) -> Result<(SegmentPtr, AdjSegment)> {
        let old_segment = self.read_segment(tx, old_ptr)?
            .ok_or(SombraError::Corruption("cannot clone null segment"))?;

        let new_segment = AdjSegment::cow_clone(&old_segment, old_ptr, new_xmin);
        let new_ptr = self.allocate_segment(tx, &new_segment)?;

        Ok((new_ptr, new_segment))
    }

    /// Creates a new empty segment for a (node, dir, type) triple.
    ///
    /// This is used when a node doesn't have any adjacency for a given type yet.
    #[allow(dead_code)]
    pub fn create_segment(
        &self,
        tx: &mut WriteGuard<'_>,
        owner: NodeId,
        dir: Dir,
        type_id: TypeId,
        xmin: TxId,
    ) -> Result<SegmentPtr> {
        let segment = AdjSegment::new(owner, dir, type_id, xmin);
        self.allocate_segment(tx, &segment)
    }

    /// Creates a new segment from inline entries during promotion.
    ///
    /// This is called when inline storage capacity is exceeded and entries
    /// need to be migrated to an external segment. The method:
    /// 1. Creates a new segment with the given owner/dir/type
    /// 2. Converts existing inline entries to full AdjEntry format
    /// 3. Adds the new entry that triggered promotion
    /// 4. Allocates a page and writes the segment
    ///
    /// # Arguments
    ///
    /// * `tx` - Write transaction guard
    /// * `owner` - Node that owns this segment
    /// * `dir` - Direction (OUT or IN)
    /// * `type_id` - Edge type ID
    /// * `existing_entries` - Inline entries to migrate (without MVCC fields)
    /// * `new_entry` - New entry that triggered the promotion
    /// * `xmin` - Transaction ID creating these entries
    ///
    /// # Returns
    ///
    /// The SegmentPtr to the newly allocated segment.
    pub fn create_segment_with_entries(
        &self,
        tx: &mut WriteGuard<'_>,
        owner: NodeId,
        dir: Dir,
        type_id: TypeId,
        existing_entries: &[InlineAdjEntry],
        new_entry: InlineAdjEntry,
        xmin: TxId,
    ) -> Result<SegmentPtr> {
        let mut segment = AdjSegment::new(owner, dir, type_id, xmin);

        // Convert existing inline entries to full AdjEntry format
        // Inline entries don't have individual MVCC fields, so we use xmin
        // for all entries being promoted (they inherit visibility from header)
        for inline_entry in existing_entries {
            segment.insert(AdjEntry::new(inline_entry.neighbor, inline_entry.edge, xmin));
        }

        // Add the new entry that triggered promotion
        segment.insert(AdjEntry::new(new_entry.neighbor, new_entry.edge, xmin));

        // Check if segment fits in a page
        let max_space = self.page_size - PAGE_HDR_LEN;
        if segment.encoded_size() <= max_space {
            return self.allocate_segment(tx, &segment);
        }

        // Need extent chaining for high-degree nodes (unlikely for promotion)
        self.allocate_chained_segment(tx, &segment)
    }

    /// Inserts an edge into a segment, creating a CoW clone.
    ///
    /// This is the main insert operation:
    /// 1. If no existing segment, create a new one
    /// 2. If existing segment, CoW clone it
    /// 3. Insert the new entry
    /// 4. If segment exceeds page size, use extent chaining
    /// 5. Return the new segment pointer
    pub fn insert_edge(
        &self,
        tx: &mut WriteGuard<'_>,
        old_ptr: Option<SegmentPtr>,
        owner: NodeId,
        dir: Dir,
        type_id: TypeId,
        neighbor: NodeId,
        edge: EdgeId,
        xmin: TxId,
    ) -> Result<SegmentPtr> {
        let mut segment = if let Some(ptr) = old_ptr {
            if ptr.is_null() {
                AdjSegment::new(owner, dir, type_id, xmin)
            } else {
                let old = self.read_segment(tx, ptr)?
                    .ok_or(SombraError::Corruption("invalid segment pointer"))?;
                AdjSegment::cow_clone(&old, ptr, xmin)
            }
        } else {
            AdjSegment::new(owner, dir, type_id, xmin)
        };

        segment.insert(AdjEntry::new(neighbor, edge, xmin));

        // Check if segment fits in a page
        let max_space = self.page_size - PAGE_HDR_LEN;
        if segment.encoded_size() <= max_space {
            // Fits in a single page
            return self.allocate_segment(tx, &segment);
        }
        
        // Need extent chaining for high-degree nodes
        self.allocate_chained_segment(tx, &segment)
    }
    
    /// Allocates a segment that spans multiple pages via extent chaining.
    ///
    /// This handles high-degree nodes where entries exceed a single page.
    /// The segment is split into:
    /// - Primary page: header + as many entries as fit
    /// - Extent pages: remaining entries with minimal headers
    fn allocate_chained_segment(
        &self,
        tx: &mut WriteGuard<'_>,
        segment: &AdjSegment,
    ) -> Result<SegmentPtr> {
        let max_entries_primary = self.max_entries_per_page();
        let max_entries_extent = self.max_entries_per_extent_page();
        
        if segment.entries.len() <= max_entries_primary {
            // Actually fits - shouldn't happen but handle gracefully
            return self.allocate_segment(tx, segment);
        }
        
        // Split entries into chunks
        let entries = &segment.entries;
        let mut pages: Vec<PageId> = Vec::new();
        
        // First page has full header, so fewer entries
        let first_chunk_size = max_entries_primary.min(entries.len());
        
        // Allocate all pages first, then link them
        // This ensures we have all page IDs before writing
        let total_entries = entries.len();
        let remaining_after_first = total_entries.saturating_sub(first_chunk_size);
        let extent_pages_needed = if remaining_after_first == 0 {
            0
        } else {
            (remaining_after_first + max_entries_extent - 1) / max_entries_extent
        };
        
        // Allocate primary page
        let primary_page_id = tx.allocate_page()?;
        pages.push(primary_page_id);
        
        // Allocate extent pages
        for _ in 0..extent_pages_needed {
            let extent_page_id = tx.allocate_page()?;
            pages.push(extent_page_id);
        }
        
        // Now write the pages in reverse order so we know the next_extent pointers
        let mut next_extent = SegmentPtr::null();
        
        // Write extent pages (from last to first)
        for (i, &page_id) in pages.iter().skip(1).enumerate().rev() {
            let chunk_start = first_chunk_size + i * max_entries_extent;
            let chunk_end = (chunk_start + max_entries_extent).min(total_entries);
            let chunk_entries: Vec<_> = entries[chunk_start..chunk_end].to_vec();
            
            self.write_extent_page(tx, page_id, &chunk_entries, next_extent)?;
            next_extent = SegmentPtr::from_page(page_id);
        }
        
        // Write primary page with full header
        let first_entries: Vec<_> = entries[0..first_chunk_size].to_vec();
        let primary_segment = AdjSegment {
            header: AdjSegmentHeader {
                next_extent,
                entry_count: total_entries as u32, // Total count across all extents
                ..segment.header
            },
            entries: first_entries,
        };
        
        self.write_segment(tx, primary_page_id, &primary_segment)?;
        
        Ok(SegmentPtr::from_page(primary_page_id))
    }
    
    /// Maximum entries that fit in an extent page (no full header).
    /// Extent pages only have: next_extent (8) + entry_count (4) = 12 bytes header
    fn max_entries_per_extent_page(&self) -> usize {
        const EXTENT_HEADER_LEN: usize = 12; // next_extent + entry_count
        if self.page_size <= PAGE_HDR_LEN + EXTENT_HEADER_LEN {
            return 0;
        }
        (self.page_size - PAGE_HDR_LEN - EXTENT_HEADER_LEN) / ADJ_ENTRY_LEN
    }
    
    /// Writes an extent page with entries.
    fn write_extent_page(
        &self,
        tx: &mut WriteGuard<'_>,
        page_id: PageId,
        entries: &[AdjEntry],
        next_extent: SegmentPtr,
    ) -> Result<()> {
        const EXTENT_HEADER_LEN: usize = 12;
        let encoded_len = EXTENT_HEADER_LEN + entries.len() * ADJ_ENTRY_LEN;
        let total_len = PAGE_HDR_LEN + encoded_len;
        
        if total_len > self.page_size {
            return Err(SombraError::Invalid("extent page too large"));
        }
        
        let mut page = tx.page_mut(page_id)?;
        let data = page.data_mut();
        
        // Write page header
        let header = PageHeader::new(
            page_id,
            PageKind::IfaSegment,
            self.store.page_size(),
            self.salt,
        )?.with_crc32(0);
        header.encode(&mut data[..PAGE_HDR_LEN])?;
        
        let mut offset = PAGE_HDR_LEN;
        
        // Write extent header: next_extent (8) + entry_count (4)
        data[offset..offset + 8].copy_from_slice(&next_extent.to_bytes());
        offset += 8;
        data[offset..offset + 4].copy_from_slice(&(entries.len() as u32).to_be_bytes());
        offset += 4;
        
        // Write entries
        for entry in entries {
            data[offset..offset + ADJ_ENTRY_LEN].copy_from_slice(&entry.encode());
            offset += ADJ_ENTRY_LEN;
        }
        
        // Zero remaining
        for byte in &mut data[offset..] {
            *byte = 0;
        }
        
        drop(page);
        Ok(())
    }
    
    /// Reads all entries from a segment, including chained extents.
    #[allow(dead_code)]
    pub fn read_segment_with_extents(
        &self,
        tx: &mut WriteGuard<'_>,
        ptr: SegmentPtr,
    ) -> Result<Option<AdjSegment>> {
        if ptr.is_null() {
            return Ok(None);
        }
        
        // Read primary segment
        let mut segment = match self.read_segment(tx, ptr)? {
            Some(s) => s,
            None => return Ok(None),
        };
        
        // Follow extent chain to collect all entries
        let mut next_extent = segment.header.next_extent;
        let mut chain_depth = 0;
        const MAX_CHAIN_DEPTH: u32 = 1000; // Prevent infinite loops
        
        while !next_extent.is_null() {
            chain_depth += 1;
            if chain_depth >= MAX_CHAIN_DEPTH {
                return Err(SombraError::Corruption("extent chain too deep"));
            }
            
            let extent_entries = self.read_extent_page(tx, next_extent)?;
            let (entries, next) = extent_entries;
            segment.entries.extend(entries);
            next_extent = next;
        }
        
        Ok(Some(segment))
    }
    
    /// Reads entries from an extent page.
    /// Returns (entries, next_extent_ptr).
    fn read_extent_page(
        &self,
        tx: &mut WriteGuard<'_>,
        ptr: SegmentPtr,
    ) -> Result<(Vec<AdjEntry>, SegmentPtr)> {
        let page_id = ptr.to_page();
        let page = tx.page_mut(page_id)?;
        let data = page.data();
        
        let extent_data = &data[PAGE_HDR_LEN..];
        if extent_data.len() < 12 {
            return Err(SombraError::Corruption("extent page too small"));
        }
        
        // Read extent header
        let next_extent = SegmentPtr::from_bytes(&extent_data[0..8])?;
        let entry_count = u32::from_be_bytes(extent_data[8..12].try_into().unwrap()) as usize;
        
        let expected_len = 12 + entry_count * ADJ_ENTRY_LEN;
        if extent_data.len() < expected_len {
            return Err(SombraError::Corruption("extent entries truncated"));
        }
        
        let mut entries = Vec::with_capacity(entry_count);
        let mut offset = 12;
        for _ in 0..entry_count {
            entries.push(AdjEntry::decode(&extent_data[offset..offset + ADJ_ENTRY_LEN])?);
            offset += ADJ_ENTRY_LEN;
        }
        
        drop(page);
        Ok((entries, next_extent))
    }
    
    /// Reads entries from an extent page (read-only path).
    fn read_extent_page_ro(
        &self,
        tx: &ReadGuard,
        ptr: SegmentPtr,
    ) -> Result<(Vec<AdjEntry>, SegmentPtr)> {
        let page_id = ptr.to_page();
        let page = self.store.get_page(tx, page_id)?;
        let data = page.data();
        
        let extent_data = &data[PAGE_HDR_LEN..];
        if extent_data.len() < 12 {
            return Err(SombraError::Corruption("extent page too small"));
        }
        
        // Read extent header
        let next_extent = SegmentPtr::from_bytes(&extent_data[0..8])?;
        let entry_count = u32::from_be_bytes(extent_data[8..12].try_into().unwrap()) as usize;
        
        let expected_len = 12 + entry_count * ADJ_ENTRY_LEN;
        if extent_data.len() < expected_len {
            return Err(SombraError::Corruption("extent entries truncated"));
        }
        
        let mut entries = Vec::with_capacity(entry_count);
        let mut offset = 12;
        for _ in 0..entry_count {
            entries.push(AdjEntry::decode(&extent_data[offset..offset + ADJ_ENTRY_LEN])?);
            offset += ADJ_ENTRY_LEN;
        }
        
        Ok((entries, next_extent))
    }

    /// Inserts multiple edges into a segment in a single CoW operation.
    ///
    /// This is much more efficient than calling `insert_edge` repeatedly because:
    #[allow(dead_code)]
    /// 1. Only one CoW clone is created for all edges
    /// 2. Only one page allocation is needed
    /// 3. All entries are inserted in bulk
    ///
    /// # Arguments
    ///
    /// * `tx` - Write transaction guard
    /// * `old_ptr` - Optional existing segment pointer
    /// * `owner` - Node that owns this segment
    /// * `dir` - Direction (OUT or IN)
    /// * `type_id` - Edge type ID
    /// * `edges` - Slice of (neighbor, edge_id) pairs to insert
    /// * `xmin` - Transaction ID creating these edges
    ///
    /// # Returns
    ///
    /// The new segment pointer containing all edges.
    pub fn insert_edges_batch(
        &self,
        tx: &mut WriteGuard<'_>,
        old_ptr: Option<SegmentPtr>,
        owner: NodeId,
        dir: Dir,
        type_id: TypeId,
        edges: &[(NodeId, EdgeId)],
        xmin: TxId,
    ) -> Result<SegmentPtr> {
        if edges.is_empty() {
            // No edges to insert - return old pointer or null
            return Ok(old_ptr.unwrap_or(SegmentPtr::null()));
        }

        let mut segment = if let Some(ptr) = old_ptr {
            if ptr.is_null() {
                AdjSegment::new(owner, dir, type_id, xmin)
            } else {
                let old = self.read_segment(tx, ptr)?
                    .ok_or(SombraError::Corruption("invalid segment pointer"))?;
                AdjSegment::cow_clone(&old, ptr, xmin)
            }
        } else {
            AdjSegment::new(owner, dir, type_id, xmin)
        };

        // Insert all edges in bulk
        for (neighbor, edge_id) in edges {
            segment.insert(AdjEntry::new(*neighbor, *edge_id, xmin));
        }

        // Check if segment fits in a page
        let max_space = self.page_size - PAGE_HDR_LEN;
        if segment.encoded_size() <= max_space {
            // Fits in a single page
            return self.allocate_segment(tx, &segment);
        }

        // Need extent chaining for high-degree nodes
        self.allocate_chained_segment(tx, &segment)
    }

    /// Inserts multiple edges into a segment using a pre-allocated page.
    ///
    /// This variant avoids page allocation overhead by using a page that was
    /// already allocated in bulk. Use this with `allocate_pages_batch` for
    /// maximum performance when inserting many edges.
    ///
    /// # Arguments
    ///
    /// * `tx` - Write transaction guard
    /// * `old_ptr` - Optional existing segment pointer
    /// * `owner` - Node that owns this segment
    /// * `dir` - Direction (OUT or IN)
    /// * `type_id` - Edge type ID
    /// * `edges` - Slice of (neighbor, edge_id) pairs to insert
    /// * `xmin` - Transaction ID creating these edges
    /// * `preallocated_page` - A page ID that was pre-allocated for this segment
    ///
    /// # Returns
    ///
    /// The new segment pointer (using the pre-allocated page).
    #[allow(dead_code)]
    pub fn insert_edges_batch_preallocated(
        &self,
        tx: &mut WriteGuard<'_>,
        old_ptr: Option<SegmentPtr>,
        owner: NodeId,
        dir: Dir,
        type_id: TypeId,
        edges: &[(NodeId, EdgeId)],
        xmin: TxId,
        preallocated_page: PageId,
    ) -> Result<SegmentPtr> {
        if edges.is_empty() {
            // No edges to insert - return old pointer or null
            // Note: The pre-allocated page will be wasted in this case
            return Ok(old_ptr.unwrap_or(SegmentPtr::null()));
        }

        let mut segment = if let Some(ptr) = old_ptr {
            if ptr.is_null() {
                AdjSegment::new(owner, dir, type_id, xmin)
            } else {
                let old = self.read_segment(tx, ptr)?
                    .ok_or(SombraError::Corruption("invalid segment pointer"))?;
                AdjSegment::cow_clone(&old, ptr, xmin)
            }
        } else {
            AdjSegment::new(owner, dir, type_id, xmin)
        };

        // Insert all edges in bulk
        for (neighbor, edge_id) in edges {
            segment.insert(AdjEntry::new(*neighbor, *edge_id, xmin));
        }

        // Check if segment fits in a page
        let max_space = self.page_size - PAGE_HDR_LEN;
        if segment.encoded_size() <= max_space {
            // Fits in a single page - use pre-allocated page
            self.write_segment(tx, preallocated_page, &segment)?;
            return Ok(SegmentPtr::from_page(preallocated_page));
        }

        // Need extent chaining for high-degree nodes
        // For simplicity, fall back to regular allocation for chained segments
        // (this is rare for typical graphs)
        self.allocate_chained_segment(tx, &segment)
    }

    /// Allocates multiple pages at once for batch segment creation.
    ///
    /// This is much more efficient than calling `allocate_page` repeatedly
    /// because it uses extent allocation which takes the lock once and
    /// allocates contiguous pages.
    ///
    /// # Arguments
    ///
    /// * `tx` - Write transaction guard
    /// * `count` - Number of pages to allocate
    ///
    /// # Returns
    ///
    /// A vector of allocated page IDs.
    #[allow(dead_code)]
    pub fn allocate_pages_batch(
        &self,
        tx: &mut WriteGuard<'_>,
        count: usize,
    ) -> Result<Vec<PageId>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut pages = Vec::with_capacity(count);
        let mut remaining = count;

        // Allocate in batches using extent allocation
        while remaining > 0 {
            // Request up to remaining pages (capped at u32::MAX)
            let request_len = remaining.min(u32::MAX as usize) as u32;
            let extent = tx.allocate_extent(request_len)?;
            
            // Collect pages from extent
            for page_id in extent.iter_pages() {
                pages.push(page_id);
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }

        Ok(pages)
    }

    /// Removes an edge from a segment, creating a CoW clone.
    ///
    /// Returns the new segment pointer, or None if the segment is now empty.
    pub fn remove_edge(
        &self,
        tx: &mut WriteGuard<'_>,
        old_ptr: SegmentPtr,
        neighbor: NodeId,
        edge: EdgeId,
        xmin: TxId,
    ) -> Result<Option<SegmentPtr>> {
        if old_ptr.is_null() {
            return Ok(None);
        }

        let old = self.read_segment(tx, old_ptr)?
            .ok_or(SombraError::Corruption("invalid segment pointer"))?;

        let mut segment = AdjSegment::cow_clone(&old, old_ptr, xmin);
        let removed = segment.remove(neighbor, edge);

        if !removed {
            // Entry not found - return old pointer unchanged
            // (In a real implementation, we'd skip the CoW clone)
            return Ok(Some(old_ptr));
        }

        if segment.is_empty() {
            // Segment is now empty - caller should remove the type mapping
            return Ok(None);
        }

        let new_ptr = self.allocate_segment(tx, &segment)?;
        Ok(Some(new_ptr))
    }

    /// Marks an old segment as superseded by setting its xmax.
    ///
    /// This is called after a successful CoW operation to mark the old
    /// version as no longer the latest.
    pub fn mark_superseded(
        &self,
        tx: &mut WriteGuard<'_>,
        ptr: SegmentPtr,
        xmax: TxId,
    ) -> Result<()> {
        if ptr.is_null() {
            return Ok(());
        }

        let mut segment = self.read_segment(tx, ptr)?
            .ok_or(SombraError::Corruption("invalid segment pointer"))?;

        segment.header.xmax = xmax;
        self.write_segment(tx, ptr.to_page(), &segment)
    }

    /// Finds the visible segment version for a given snapshot.
    ///
    /// Walks the prev_version chain to find the first segment visible at the snapshot.
    /// Also collects entries from extent chains for high-degree nodes.
    pub fn find_visible_segment(
        &self,
        tx: &mut WriteGuard<'_>,
        head_ptr: SegmentPtr,
        snapshot: TxId,
    ) -> Result<Option<AdjSegment>> {
        let mut current_ptr = head_ptr;

        while !current_ptr.is_null() {
            let mut segment = self.read_segment(tx, current_ptr)?
                .ok_or(SombraError::Corruption("broken version chain"))?;

            if segment.header.visible_at(snapshot) {
                // Collect entries from extent chain if present
                if !segment.header.next_extent.is_null() {
                    let mut next_extent = segment.header.next_extent;
                    let mut chain_depth = 0;
                    const MAX_CHAIN_DEPTH: u32 = 1000;
                    
                    while !next_extent.is_null() {
                        chain_depth += 1;
                        if chain_depth >= MAX_CHAIN_DEPTH {
                            return Err(SombraError::Corruption("extent chain too deep"));
                        }
                        
                        let (entries, next) = self.read_extent_page(tx, next_extent)?;
                        segment.entries.extend(entries);
                        next_extent = next;
                    }
                }
                return Ok(Some(segment));
            }

            current_ptr = segment.header.prev_version;
        }

        Ok(None)
    }

    /// Finds the visible segment version for a given snapshot using a read-only transaction.
    ///
    /// Walks the prev_version chain to find the first segment visible at the snapshot.
    /// Also collects entries from extent chains for high-degree nodes.
    pub fn find_visible_segment_ro(
        &self,
        tx: &ReadGuard,
        head_ptr: SegmentPtr,
        snapshot: TxId,
    ) -> Result<Option<AdjSegment>> {
        let mut current_ptr = head_ptr;

        while !current_ptr.is_null() {
            let mut segment = self.read_segment_ro(tx, current_ptr)?
                .ok_or(SombraError::Corruption("broken version chain"))?;

            if segment.header.visible_at(snapshot) {
                // Collect entries from extent chain if present
                if !segment.header.next_extent.is_null() {
                    let mut next_extent = segment.header.next_extent;
                    let mut chain_depth = 0;
                    const MAX_CHAIN_DEPTH: u32 = 1000;
                    
                    while !next_extent.is_null() {
                        chain_depth += 1;
                        if chain_depth >= MAX_CHAIN_DEPTH {
                            return Err(SombraError::Corruption("extent chain too deep"));
                        }
                        
                        let (entries, next) = self.read_extent_page_ro(tx, next_extent)?;
                        segment.entries.extend(entries);
                        next_extent = next;
                    }
                }
                return Ok(Some(segment));
            }

            current_ptr = segment.header.prev_version;
        }

        Ok(None)
    }
}

#[cfg(test)]
mod segment_manager_tests {
    use super::*;
    use crate::primitives::pager::{Pager, PagerOptions};
    use tempfile::tempdir;

    fn create_test_manager() -> (Arc<Pager>, SegmentManager) {
        let dir = tempdir().unwrap();
        let path = dir.path().join("test.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).unwrap());
        let manager = SegmentManager::new(Arc::clone(&pager) as Arc<dyn PageStore>);
        (pager, manager)
    }

    #[test]
    fn test_create_and_read_segment() {
        let (pager, manager) = create_test_manager();
        let mut tx = pager.begin_write().unwrap();

        let ptr = manager.create_segment(
            &mut tx,
            NodeId(100),
            Dir::Out,
            TypeId(5),
            1000,
        ).unwrap();

        let segment = manager.read_segment(&mut tx, ptr).unwrap().unwrap();
        assert_eq!(segment.header.owner, NodeId(100));
        assert_eq!(segment.header.dir, Dir::Out);
        assert_eq!(segment.header.type_id, TypeId(5));
        assert_eq!(segment.header.xmin, 1000);
        assert_eq!(segment.header.xmax, 0);
        assert!(segment.entries.is_empty());
    }

    #[test]
    fn test_insert_edge() {
        let (pager, manager) = create_test_manager();
        let mut tx = pager.begin_write().unwrap();

        // Insert first edge (no existing segment)
        let ptr1 = manager.insert_edge(
            &mut tx,
            None,
            NodeId(100),
            Dir::Out,
            TypeId(5),
            NodeId(200),
            EdgeId(1),
            1000,
        ).unwrap();

        let segment = manager.read_segment(&mut tx, ptr1).unwrap().unwrap();
        assert_eq!(segment.len(), 1);
        assert_eq!(segment.entries[0].neighbor, NodeId(200));
        assert_eq!(segment.entries[0].edge, EdgeId(1));

        // Insert second edge (CoW clone)
        let ptr2 = manager.insert_edge(
            &mut tx,
            Some(ptr1),
            NodeId(100),
            Dir::Out,
            TypeId(5),
            NodeId(300),
            EdgeId(2),
            1001,
        ).unwrap();

        let segment2 = manager.read_segment(&mut tx, ptr2).unwrap().unwrap();
        assert_eq!(segment2.len(), 2);
        assert_eq!(segment2.header.xmin, 1001);
        assert_eq!(segment2.header.prev_version, ptr1);

        // Original segment should still exist with original data
        let segment1 = manager.read_segment(&mut tx, ptr1).unwrap().unwrap();
        assert_eq!(segment1.len(), 1);
    }

    #[test]
    fn test_remove_edge() {
        let (pager, manager) = create_test_manager();
        let mut tx = pager.begin_write().unwrap();

        // Create segment with two edges
        let ptr1 = manager.insert_edge(
            &mut tx,
            None,
            NodeId(100),
            Dir::Out,
            TypeId(5),
            NodeId(200),
            EdgeId(1),
            1000,
        ).unwrap();

        let ptr2 = manager.insert_edge(
            &mut tx,
            Some(ptr1),
            NodeId(100),
            Dir::Out,
            TypeId(5),
            NodeId(300),
            EdgeId(2),
            1001,
        ).unwrap();

        // Remove one edge
        let ptr3 = manager.remove_edge(&mut tx, ptr2, NodeId(200), EdgeId(1), 1002)
            .unwrap()
            .unwrap();

        let segment = manager.read_segment(&mut tx, ptr3).unwrap().unwrap();
        assert_eq!(segment.len(), 1);
        assert_eq!(segment.entries[0].neighbor, NodeId(300));
        assert_eq!(segment.header.xmin, 1002);

        // Remove last edge - should return None
        let ptr4 = manager.remove_edge(&mut tx, ptr3, NodeId(300), EdgeId(2), 1003).unwrap();
        assert!(ptr4.is_none());
    }

    #[test]
    fn test_cow_clone() {
        let (pager, manager) = create_test_manager();
        let mut tx = pager.begin_write().unwrap();

        let ptr1 = manager.create_segment(
            &mut tx,
            NodeId(100),
            Dir::Out,
            TypeId(5),
            1000,
        ).unwrap();

        let (ptr2, segment2) = manager.cow_clone(&mut tx, ptr1, 1001).unwrap();

        assert_ne!(ptr1, ptr2);
        assert_eq!(segment2.header.xmin, 1001);
        assert_eq!(segment2.header.prev_version, ptr1);
    }

    #[test]
    fn test_mark_superseded() {
        let (pager, manager) = create_test_manager();
        let mut tx = pager.begin_write().unwrap();

        let ptr = manager.create_segment(
            &mut tx,
            NodeId(100),
            Dir::Out,
            TypeId(5),
            1000,
        ).unwrap();

        // Segment should be active initially
        let segment = manager.read_segment(&mut tx, ptr).unwrap().unwrap();
        assert_eq!(segment.header.xmax, 0);

        // Mark as superseded
        manager.mark_superseded(&mut tx, ptr, 2000).unwrap();

        let segment = manager.read_segment(&mut tx, ptr).unwrap().unwrap();
        assert_eq!(segment.header.xmax, 2000);
    }

    #[test]
    fn test_find_visible_segment() {
        let (pager, manager) = create_test_manager();
        let mut tx = pager.begin_write().unwrap();

        // Create a chain of versions: v1 -> v2 -> v3 (head)
        let v1_ptr = manager.create_segment(
            &mut tx,
            NodeId(100),
            Dir::Out,
            TypeId(5),
            100,  // xmin = 100
        ).unwrap();
        manager.mark_superseded(&mut tx, v1_ptr, 200).unwrap();

        let (v2_ptr, _) = manager.cow_clone(&mut tx, v1_ptr, 200).unwrap();
        manager.mark_superseded(&mut tx, v2_ptr, 300).unwrap();

        let (v3_ptr, _) = manager.cow_clone(&mut tx, v2_ptr, 300).unwrap();
        // v3 is the current head, xmax = 0

        // Snapshot at 150 should see v1 (100 <= 150, 200 > 150)
        let visible = manager.find_visible_segment(&mut tx, v3_ptr, 150).unwrap().unwrap();
        assert_eq!(visible.header.xmin, 100);

        // Snapshot at 250 should see v2 (200 <= 250, 300 > 250)
        let visible = manager.find_visible_segment(&mut tx, v3_ptr, 250).unwrap().unwrap();
        assert_eq!(visible.header.xmin, 200);

        // Snapshot at 350 should see v3 (300 <= 350, xmax = 0)
        let visible = manager.find_visible_segment(&mut tx, v3_ptr, 350).unwrap().unwrap();
        assert_eq!(visible.header.xmin, 300);

        // Snapshot at 50 should see nothing (too old)
        let visible = manager.find_visible_segment(&mut tx, v3_ptr, 50).unwrap();
        assert!(visible.is_none());
    }

    #[test]
    fn test_max_entries() {
        let (_pager, manager) = create_test_manager();
        let max = manager.max_entries_per_page();
        // With 8KB pages and 32-byte entries: (8192 - PAGE_HDR_LEN - 50) / 32 ≈ 250+
        assert!(max > 200, "max_entries={} should be > 200", max);
    }
}
