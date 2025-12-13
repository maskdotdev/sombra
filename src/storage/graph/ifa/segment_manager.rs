//! Segment Manager - handles AdjSegment allocation and CoW operations.
//!
//! This module provides the `SegmentManager` that coordinates:
//! - Allocation of pages for new AdjSegments
//! - Copy-on-Write segment creation
//! - Segment serialization to/from pages

use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::adjacency::Dir;
use crate::types::page::{PageHeader, PageKind, PAGE_HDR_LEN};
use crate::types::{EdgeId, NodeId, PageId, Result, SombraError, TypeId};

use super::segment::{AdjEntry, AdjSegment, ADJ_SEGMENT_HEADER_LEN, ADJ_ENTRY_LEN};
use super::types::SegmentPtr;
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

    /// Inserts an edge into a segment, creating a CoW clone.
    ///
    /// This is the main insert operation:
    /// 1. If no existing segment, create a new one
    /// 2. If existing segment, CoW clone it
    /// 3. Insert the new entry
    /// 4. Return the new segment pointer
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
        if segment.encoded_size() > self.page_size - PAGE_HDR_LEN {
            // TODO: Implement extent chaining for high-degree nodes
            return Err(SombraError::Invalid("segment too large, extent chaining not implemented"));
        }

        self.allocate_segment(tx, &segment)
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
    pub fn find_visible_segment(
        &self,
        tx: &mut WriteGuard<'_>,
        head_ptr: SegmentPtr,
        snapshot: TxId,
    ) -> Result<Option<AdjSegment>> {
        let mut current_ptr = head_ptr;

        while !current_ptr.is_null() {
            let segment = self.read_segment(tx, current_ptr)?
                .ok_or(SombraError::Corruption("broken version chain"))?;

            if segment.header.visible_at(snapshot) {
                return Ok(Some(segment));
            }

            current_ptr = segment.header.prev_version;
        }

        Ok(None)
    }

    /// Finds the visible segment version for a given snapshot using a read-only transaction.
    ///
    /// Walks the prev_version chain to find the first segment visible at the snapshot.
    pub fn find_visible_segment_ro(
        &self,
        tx: &ReadGuard,
        head_ptr: SegmentPtr,
        snapshot: TxId,
    ) -> Result<Option<AdjSegment>> {
        let mut current_ptr = head_ptr;

        while !current_ptr.is_null() {
            let segment = self.read_segment_ro(tx, current_ptr)?
                .ok_or(SombraError::Corruption("broken version chain"))?;

            if segment.header.visible_at(snapshot) {
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
