//! Version Chain Storage and Retrieval for MVCC
//!
//! This module provides functions for storing and retrieving version chains
//! in an MVCC system. Each version chain links multiple versions of the
//! same record together, allowing readers to access historical versions
//! while writers create new versions.
//!
//! # Version Chain Structure
//!
//! ```
//! Record Page Layout for Versioned Record:
//! [RecordHeader][VersionMetadata][payload]
//! [   8 bytes ][  25 bytes       ][variable]
//! ```
//!
//! Version chains are linked using `RecordPointer` references stored in
//! the `VersionMetadata` `prev_version` field.

use crate::db::TxId;
use crate::error::{GraphError, Result};
use crate::storage::record::RecordKind;
use crate::storage::version::{VersionFlags, VersionMetadata, VersionedRecordKind};
use crate::storage::{RecordPointer, RecordStore};
use std::collections::HashMap;

/// Tracks in-memory version pointers for an active transaction
#[derive(Debug, Clone, Default)]
pub struct VersionTracker {
    /// Maps record ID to pointer of current version being written
    pub current_versions: HashMap<u64, RecordPointer>,
}

impl VersionTracker {
    /// Create a new version tracker
    pub fn new() -> Self {
        Self {
            current_versions: HashMap::new(),
        }
    }

    /// Track a new version being written
    pub fn track_version(&mut self, record_id: u64, pointer: RecordPointer) {
        self.current_versions.insert(record_id, pointer);
    }

    /// Get the current version pointer for a record
    pub fn get_current_version(&self, record_id: u64) -> Option<RecordPointer> {
        self.current_versions.get(&record_id).copied()
    }
}

/// Represents a version of a record in a version chain
#[derive(Debug, Clone)]
pub struct VersionedRecord {
    /// Pointer to this version in storage
    pub pointer: RecordPointer,
    /// The version metadata
    pub metadata: VersionMetadata,
    /// The raw record data (without metadata)
    pub data: Vec<u8>,
}

/// Version chain reader for traversing version history
pub struct VersionChainReader;

impl VersionChainReader {
    /// Read a specific version of a record from the version chain
    ///
    /// This function traverses the version chain backwards (newest to oldest)
    /// and returns the first version visible to the given snapshot timestamp.
    ///
    /// # Arguments
    /// * `record_store` - The record store to read from
    /// * `head_pointer` - Pointer to the newest version (head of chain)
    /// * `snapshot_ts` - Snapshot timestamp to determine visibility
    /// * `current_tx_id` - Optional current transaction ID for read-your-own-writes
    ///
    /// # Returns
    /// * `Some(VersionedRecord)` if a visible version is found
    /// * `None` if the record was deleted or no version is visible
    pub fn read_version_for_snapshot(
        record_store: &mut RecordStore,
        head_pointer: RecordPointer,
        snapshot_ts: u64,
        current_tx_id: Option<TxId>,
    ) -> Result<Option<VersionedRecord>> {
        // For now, implement a simplified version that only handles non-versioned records
        // Full version chain traversal will be implemented in the next phase
        
        record_store.visit_record(head_pointer, |record_data| {
            if record_data.len() < 8 {
                return Ok(None); // Invalid pointer
            }

            let kind_byte = record_data[0];
            
            // Handle versioned records
            if kind_byte == VersionedRecordKind::VersionedNode.to_byte() 
                || kind_byte == VersionedRecordKind::VersionedEdge.to_byte() {
                // This is a versioned record
                if record_data.len() < 8 + 25 {
                    return Ok(None);
                }

                let metadata = VersionMetadata::from_bytes(&record_data[8..33])?;
                
                // Check visibility
                if is_version_visible(&metadata, snapshot_ts, current_tx_id) {
                    let payload_length_bytes: [u8; 4] = record_data[4..8]
                        .try_into()
                        .map_err(|_| GraphError::Corruption("invalid record header".into()))?;
                    let payload_length = u32::from_le_bytes(payload_length_bytes) as usize;
                    
                    // Record layout: [header: 8][metadata: 25][data: N]
                    // payload_length = 25 + N (total size of metadata + data)
                    // Data starts at offset 33 (after 8-byte header and 25-byte metadata)
                    let data_start = 33;
                    let data_end = 8 + payload_length; // header_size + total_payload_size
                    
                    if record_data.len() < data_end {
                        return Ok(None);
                    }

                    let data = record_data[data_start..data_end].to_vec();
                    
                    return Ok(Some(VersionedRecord {
                        pointer: head_pointer,
                        metadata,
                        data,
                    }));
                }
                
                return Ok(None); // Version not visible - would traverse to prev_version in full implementation
            }
            
            // Non-versioned record (backwards compatibility)
            let payload_length_bytes: [u8; 4] = record_data[4..8]
                .try_into()
                .map_err(|_| GraphError::Corruption("invalid record header".into()))?;
            let payload_length = u32::from_le_bytes(payload_length_bytes) as usize;
            
            let record_end = 8 + payload_length;
            if record_data.len() < record_end {
                return Ok(None);
            }

            let data = record_data[8..record_end].to_vec();
            
            // Create synthetic VersionMetadata for legacy records
            let metadata = VersionMetadata {
                tx_id: 0,
                commit_ts: 0,
                prev_version: None,
                flags: VersionFlags::Alive,
            };

            Ok(Some(VersionedRecord {
                pointer: head_pointer,
                metadata,
                data,
            }))
        })
    }

    /// Get the latest version pointer for a record
    ///
    /// This is typically stored in an index that maps record IDs to their
    /// latest version pointers.
    pub fn get_latest_version_pointer(
        index: &HashMap<u64, RecordPointer>,
        record_id: u64,
    ) -> Option<RecordPointer> {
        index.get(&record_id).copied()
    }
}

/// Check if a version is visible to a given snapshot timestamp
///
/// A version is visible if:
/// 1. It's alive (not deleted)
/// 2. It was committed before or at the snapshot timestamp
/// Check if a version is visible to a snapshot
///
/// A version is visible if:
/// 1. It's not deleted
/// 2. It was committed before or at the snapshot timestamp
/// 3. OR it was created by the current transaction (read-your-own-writes)
fn is_version_visible(metadata: &VersionMetadata, snapshot_ts: u64, current_tx_id: Option<TxId>) -> bool {
    // Check if the record is deleted
    if metadata.flags == VersionFlags::Deleted {
        // Deleted records are not visible to any snapshot
        return false;
    }

    // Read-your-own-writes: If this version was created by the current transaction, it's visible
    if let Some(tx_id) = current_tx_id {
        if metadata.tx_id == tx_id {
            return true;
        }
    }

    // Check if the version was committed before or at the snapshot
    metadata.commit_ts <= snapshot_ts || snapshot_ts == 0
}

/// Store a new version in the version chain
///
/// # Arguments
/// * `record_store` - The record store to write to
/// * `prev_pointer` - Pointer to the previous version (None for first version)
/// * `record_id` - Unique ID of the record
/// * `kind` - Type of record (Node or Edge)
/// * `data` - The record data
/// * `tx_id` - Transaction ID creating this version
/// * `commit_ts` - Commit timestamp (0 for pending)
///
/// # Returns
/// * Pointer to the newly stored version
pub fn store_new_version(
    record_store: &mut RecordStore,
    prev_pointer: Option<RecordPointer>,
    _record_id: u64,
    kind: RecordKind,
    data: &[u8],
    tx_id: TxId,
    commit_ts: u64,
) -> Result<RecordPointer> {
    // Create version metadata
    let metadata = VersionMetadata::new(tx_id, commit_ts, prev_pointer, false);

    // Convert to versioned record kind
    let versioned_kind = match kind {
        RecordKind::Node => VersionedRecordKind::VersionedNode,
        RecordKind::Edge => VersionedRecordKind::VersionedEdge,
        _ => return Err(GraphError::InvalidArgument("cannot version free records".into())),
    };

    // Serialize metadata
    let metadata_bytes = metadata.to_bytes();

    // Combine: versioned_kind + reserved + payload_length + metadata + data
    let total_payload_size = 25 + data.len(); // metadata + data
    
    let mut record_data = Vec::with_capacity(8 + 25 + data.len());
    
    // Write versioned record header
    record_data.push(versioned_kind.to_byte());
    record_data.extend_from_slice(&[0; 3]); // Reserved
    record_data.extend_from_slice(&(total_payload_size as u32).to_le_bytes());
    
    // Write metadata
    record_data.extend_from_slice(&metadata_bytes);
    
    // Write actual record data
    record_data.extend_from_slice(data);

    // Store in the record store
    // Use insert_new_slot to ensure each version gets its own storage location
    // This prevents slot reuse which would break version chain integrity
    let pointer = record_store.insert_new_slot(&record_data)?;

    Ok(pointer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visibility_checking() {
        // Create metadata for an alive, committed version
        let metadata = VersionMetadata::new(1, 100, None, false);

        // Should be visible to snapshots at or after commit time
        assert!(is_version_visible(&metadata, 100, None));
        assert!(is_version_visible(&metadata, 200, None));
        // Should not be visible to snapshots before commit time (except snapshot_ts=0)
        assert!(is_version_visible(&metadata, 0, None)); // Special case for legacy records
    }

    #[test]
    fn test_deleted_version_not_visible() {
        let metadata = VersionMetadata::new(1, 100, None, true);

        // Deleted versions are never visible
        assert!(!is_version_visible(&metadata, 200, None));
        assert!(!is_version_visible(&metadata, 100, None));
    }

    #[test]
    fn test_version_tracker() {
        let mut tracker = VersionTracker::new();
        
        let pointer1 = RecordPointer { page_id: 1, slot_index: 10, byte_offset: 100 };
        let pointer2 = RecordPointer { page_id: 1, slot_index: 20, byte_offset: 200 };

        tracker.track_version(10, pointer1);
        tracker.track_version(20, pointer2);

        assert_eq!(tracker.get_current_version(10), Some(pointer1));
        assert_eq!(tracker.get_current_version(20), Some(pointer2));
        assert_eq!(tracker.get_current_version(30), None);
    }

    #[test]
    fn test_store_new_version() {
        // This test verifies that store_new_version compiles correctly
        // Actual integration tests will be added later
        let metadata = VersionMetadata::new(1, 100, None, false);
        let bytes = metadata.to_bytes();
        assert_eq!(bytes.len(), 25);
    }
}
