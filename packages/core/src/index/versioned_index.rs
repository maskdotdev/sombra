//! Multi-Version Index Support for MVCC
//!
//! This module provides version-aware index entries that track when
//! index entries are added and removed, enabling snapshot isolation
//! for secondary indexes (label_index, property_indexes).
//!
//! # Design
//!
//! For MVCC correctness, secondary indexes must track the version history
//! of index entries. When a node's label or property changes:
//! - The old entry is marked with a delete_ts (deletion timestamp)
//! - A new entry is added with the new commit_ts
//!
//! This allows queries at different snapshot timestamps to see the
//! correct index state as it existed at that point in time.

use crate::storage::RecordPointer;

/// Represents a versioned entry in a secondary index
///
/// Each IndexEntry tracks when it was added to the index (commit_ts)
/// and optionally when it was removed (delete_ts). This enables
/// snapshot-based filtering during index lookups.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexEntry {
    /// Pointer to the version of the record this entry represents
    pub pointer: RecordPointer,
    
    /// Commit timestamp when this index entry was created
    pub commit_ts: u64,
    
    /// Optional delete timestamp when this entry was removed from the index
    /// None means the entry is still active
    pub delete_ts: Option<u64>,
}

impl IndexEntry {
    /// Create a new active index entry
    pub fn new(pointer: RecordPointer, commit_ts: u64) -> Self {
        Self {
            pointer,
            commit_ts,
            delete_ts: None,
        }
    }
    
    /// Create a deleted index entry (for testing)
    pub fn new_deleted(pointer: RecordPointer, commit_ts: u64, delete_ts: u64) -> Self {
        Self {
            pointer,
            commit_ts,
            delete_ts: Some(delete_ts),
        }
    }
    
    /// Mark this entry as deleted at the given timestamp
    pub fn mark_deleted(&mut self, delete_ts: u64) {
        self.delete_ts = Some(delete_ts);
    }
    
    /// Check if this entry is visible at the given snapshot timestamp
    ///
    /// An entry is visible if:
    /// - It was committed before or at the snapshot (commit_ts <= snapshot_ts)
    /// - AND it was not deleted, or was deleted after the snapshot
    pub fn is_visible_at(&self, snapshot_ts: u64) -> bool {
        // Entry must be committed before or at snapshot time
        if self.commit_ts > snapshot_ts {
            return false;
        }
        
        // Entry must not be deleted, or deleted after snapshot time
        match self.delete_ts {
            None => true, // Still active
            Some(dt) => dt > snapshot_ts, // Deleted after our snapshot
        }
    }
    
    /// Check if this entry can be garbage collected
    ///
    /// An entry can be GC'd if it was deleted and the delete timestamp
    /// is older than the GC horizon (no active snapshots need it)
    pub fn can_gc(&self, gc_horizon: u64) -> bool {
        match self.delete_ts {
            Some(dt) => dt < gc_horizon,
            None => false, // Active entries can't be GC'd
        }
    }
}

/// Version-aware list of index entries for a single key
///
/// This wraps a vector of IndexEntry and provides snapshot-aware operations.
#[derive(Debug, Clone)]
pub struct VersionedIndexEntries {
    entries: Vec<IndexEntry>,
}

impl VersionedIndexEntries {
    /// Create a new empty versioned index entry list
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
    
    /// Add a new entry to the index
    pub fn add_entry(&mut self, pointer: RecordPointer, commit_ts: u64) {
        self.entries.push(IndexEntry::new(pointer, commit_ts));
    }
    
    /// Add an entry that was deleted (used during deserialization)
    pub fn add_deleted_entry(&mut self, pointer: RecordPointer, commit_ts: u64, delete_ts: u64) {
        self.entries.push(IndexEntry::new_deleted(pointer, commit_ts, delete_ts));
    }
    
    /// Mark an entry as deleted at the given timestamp
    ///
    /// This finds the most recent active entry and marks it deleted.
    /// If no active entry exists, this is a no-op.
    pub fn mark_deleted(&mut self, delete_ts: u64) -> bool {
        // Find the most recent active entry (one with delete_ts = None)
        // and mark it deleted
        for entry in self.entries.iter_mut().rev() {
            if entry.delete_ts.is_none() {
                entry.mark_deleted(delete_ts);
                return true;
            }
        }
        false
    }
    
    /// Update the commit timestamp of the most recent uncommitted entry
    ///
    /// This is used during transaction commit to update entries that were
    /// created with commit_ts=0 to their actual commit timestamp.
    /// Updates the most recent entry with commit_ts=0.
    pub fn update_latest_commit_ts(&mut self, new_commit_ts: u64) -> bool {
        // Find the most recent entry with commit_ts=0 (uncommitted)
        for entry in self.entries.iter_mut().rev() {
            if entry.commit_ts == 0 {
                entry.commit_ts = new_commit_ts;
                return true;
            }
        }
        false
    }
    
    /// Update the delete timestamp of the most recent entry marked deleted at ts=0
    ///
    /// This is used during transaction commit to update entries that were
    /// marked as deleted with delete_ts=0 to their actual commit timestamp.
    /// Updates the most recent entry with delete_ts=Some(0).
    pub fn update_latest_delete_ts(&mut self, new_delete_ts: u64) -> bool {
        // Find the most recent entry with delete_ts=Some(0) (uncommitted deletion)
        for entry in self.entries.iter_mut().rev() {
            if entry.delete_ts == Some(0) {
                entry.delete_ts = Some(new_delete_ts);
                return true;
            }
        }
        false
    }
    
    /// Update the commit timestamp for a specific pointer
    /// Update the delete timestamp for a specific pointer
    ///
    /// This is used during transaction commit for property indexes where
    /// multiple nodes can share the same property value. We need to update
    /// the specific entry for the given pointer.
    pub fn update_commit_ts_for_pointer(&mut self, pointer: RecordPointer, new_commit_ts: u64) -> bool {
        
        // Find the most recent entry with this pointer and commit_ts=0
        for entry in self.entries.iter_mut().rev() {
            if entry.pointer == pointer && entry.commit_ts == 0 {
                entry.commit_ts = new_commit_ts;
                return true;
            }
        }
        
        false
    }
    
    pub fn update_delete_ts_for_pointer(&mut self, pointer: RecordPointer, new_delete_ts: u64) -> bool {
        // Find the most recent entry with this pointer
        // If new_delete_ts is 0, we're setting the initial delete timestamp (find entries with delete_ts=None)
        // If new_delete_ts > 0, we're updating from placeholder 0 to actual commit_ts (find entries with delete_ts=Some(0))
        for entry in self.entries.iter_mut().rev() {
            if entry.pointer == pointer {
                if new_delete_ts == 0 && entry.delete_ts.is_none() {
                    // Initial marking as deleted
                    entry.delete_ts = Some(0);
                    return true;
                } else if new_delete_ts > 0 && entry.delete_ts == Some(0) {
                    // Update placeholder to actual commit timestamp
                    entry.delete_ts = Some(new_delete_ts);
                    return true;
                }
            }
        }
        
        false
    }
    
    /// Check if any entry is visible at the given snapshot timestamp
    pub fn is_visible_at(&self, snapshot_ts: u64) -> bool {
        self.entries.iter().any(|e| e.is_visible_at(snapshot_ts))
    }
    
    /// Get all visible entries at the given snapshot timestamp
    pub fn get_visible_entries(&self, snapshot_ts: u64) -> Vec<&IndexEntry> {
        self.entries
            .iter()
            .filter(|e| e.is_visible_at(snapshot_ts))
            .collect()
    }
    
    /// Remove entries that can be garbage collected
    ///
    /// Returns the number of entries removed
    pub fn gc(&mut self, gc_horizon: u64) -> usize {
        let before_len = self.entries.len();
        self.entries.retain(|e| !e.can_gc(gc_horizon));
        before_len - self.entries.len()
    }
    
    /// Check if all entries have been deleted and GC'd (can remove the key)
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    
    /// Get the number of entries (including deleted ones)
    pub fn len(&self) -> usize {
        self.entries.len()
    }
    
    /// Get a reference to the entries vector
    pub fn entries(&self) -> &Vec<IndexEntry> {
        &self.entries
    }
}

impl Default for VersionedIndexEntries {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_index_entry_visibility() {
        let entry = IndexEntry::new(
            RecordPointer { page_id: 1, slot_index: 0, byte_offset: 100 },
            100, // commit_ts
        );
        
        // Visible at and after commit time
        assert!(!entry.is_visible_at(99));
        assert!(entry.is_visible_at(100));
        assert!(entry.is_visible_at(200));
    }
    
    #[test]
    fn test_index_entry_deletion() {
        let mut entry = IndexEntry::new(
            RecordPointer { page_id: 1, slot_index: 0, byte_offset: 100 },
            100, // commit_ts
        );
        
        entry.mark_deleted(200);
        
        // Not visible before commit
        assert!(!entry.is_visible_at(99));
        
        // Visible between commit and deletion
        assert!(entry.is_visible_at(100));
        assert!(entry.is_visible_at(150));
        assert!(entry.is_visible_at(199));
        
        // Not visible at or after deletion
        assert!(!entry.is_visible_at(200));
        assert!(!entry.is_visible_at(300));
    }
    
    #[test]
    fn test_versioned_entries_add_and_visibility() {
        let mut entries = VersionedIndexEntries::new();
        
        // Add first version at ts=100
        entries.add_entry(
            RecordPointer { page_id: 1, slot_index: 0, byte_offset: 100 },
            100,
        );
        
        // Visible at ts=150
        assert!(entries.is_visible_at(150));
        
        // Mark deleted at ts=200
        assert!(entries.mark_deleted(200));
        
        // Still visible at ts=150
        assert!(entries.is_visible_at(150));
        
        // Not visible at ts=250
        assert!(!entries.is_visible_at(250));
        
        // Add new version at ts=300
        entries.add_entry(
            RecordPointer { page_id: 1, slot_index: 1, byte_offset: 200 },
            300,
        );
        
        // Now visible at ts=350
        assert!(entries.is_visible_at(350));
        
        // Old snapshot still sees the old version
        assert!(entries.is_visible_at(150));
    }
    
    #[test]
    fn test_gc_cleanup() {
        let mut entries = VersionedIndexEntries::new();
        
        // Add entry at ts=100, delete at ts=200
        entries.add_entry(
            RecordPointer { page_id: 1, slot_index: 0, byte_offset: 100 },
            100,
        );
        entries.mark_deleted(200);
        
        // Add new entry at ts=300
        entries.add_entry(
            RecordPointer { page_id: 1, slot_index: 1, byte_offset: 200 },
            300,
        );
        
        assert_eq!(entries.len(), 2);
        
        // GC with horizon=250 should remove the first entry (deleted at 200 < 250)
        let removed = entries.gc(250);
        assert_eq!(removed, 1);
        assert_eq!(entries.len(), 1);
        
        // The remaining entry should be the one committed at 300
        assert!(entries.is_visible_at(350));
        assert!(!entries.is_visible_at(250)); // Old one is gone
    }
    
    #[test]
    fn test_multiple_versions() {
        let mut entries = VersionedIndexEntries::new();
        
        // Timeline:
        // ts=100: add entry A
        // ts=200: delete entry A, add entry B
        // ts=300: delete entry B, add entry C
        
        entries.add_entry(
            RecordPointer { page_id: 1, slot_index: 0, byte_offset: 100 },
            100,
        );
        entries.mark_deleted(200);
        
        entries.add_entry(
            RecordPointer { page_id: 1, slot_index: 1, byte_offset: 200 },
            200,
        );
        entries.mark_deleted(300);
        
        entries.add_entry(
            RecordPointer { page_id: 1, slot_index: 2, byte_offset: 300 },
            300,
        );
        
        // Check visibility at different snapshots
        let visible_at_150 = entries.get_visible_entries(150);
        assert_eq!(visible_at_150.len(), 1);
        assert_eq!(visible_at_150[0].pointer.byte_offset, 100);
        
        let visible_at_250 = entries.get_visible_entries(250);
        assert_eq!(visible_at_250.len(), 1);
        assert_eq!(visible_at_250[0].pointer.byte_offset, 200);
        
        let visible_at_350 = entries.get_visible_entries(350);
        assert_eq!(visible_at_350.len(), 1);
        assert_eq!(visible_at_350[0].pointer.byte_offset, 300);
    }
}
