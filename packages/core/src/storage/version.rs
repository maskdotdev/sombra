//! Version Metadata for MVCC
//!
//! This module provides structures for tracking version metadata in
//! a Multi-Version Concurrency Control (MVCC) system.
//!
//! # Overview
//!
//! In MVCC, each write to a record creates a new version. Versions are
//! linked together in a chain, with each version containing:
//! - The transaction ID that created it
//! - The commit timestamp when it became visible
//! - A pointer to the previous version in the chain
//! - A deletion marker (tombstone)

use crate::db::TxId;
use crate::error::{GraphError, Result};
use crate::storage::RecordPointer;

/// Size of version metadata header (tx_id + commit_ts + prev_version + flags)
pub const VERSION_METADATA_SIZE: usize = 8 + 8 + 8 + 1; // 25 bytes

/// Flags for version metadata
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum VersionFlags {
    /// Version is alive (not deleted)
    Alive = 0x00,
    /// Version is a tombstone (deleted)
    Deleted = 0x01,
}

impl VersionFlags {
    pub fn from_byte(byte: u8) -> VersionFlags {
        if byte & 0x01 != 0 {
            VersionFlags::Deleted
        } else {
            VersionFlags::Alive
        }
    }

    pub fn to_byte(self) -> u8 {
        match self {
            VersionFlags::Alive => 0x00,
            VersionFlags::Deleted => 0x01,
        }
    }

    pub fn is_deleted(self) -> bool {
        self == VersionFlags::Deleted
    }
}

/// Metadata for a versioned record
///
/// This structure is prepended to versioned records to track:
/// - Which transaction created this version
/// - When it was committed
/// - Link to previous version in the chain
/// - Whether this version is deleted
#[derive(Debug, Clone, Copy)]
pub struct VersionMetadata {
    /// Transaction ID that created this version
    pub tx_id: TxId,
    /// Commit timestamp (0 if not yet committed)
    pub commit_ts: u64,
    /// Pointer to previous version in the chain (None if no previous version)
    pub prev_version: Option<RecordPointer>,
    /// Version flags
    pub flags: VersionFlags,
}

impl VersionMetadata {
    /// Creates new version metadata
    ///
    /// # Arguments
    /// * `tx_id` - Transaction ID
    /// * `commit_ts` - Commit timestamp (0 for uncommitted)
    /// * `prev_version` - Previous version pointer
    /// * `is_deleted` - Whether this is a deletion tombstone
    pub fn new(
        tx_id: TxId,
        commit_ts: u64,
        prev_version: Option<RecordPointer>,
        is_deleted: bool,
    ) -> Self {
        Self {
            tx_id,
            commit_ts,
            prev_version,
            flags: if is_deleted {
                VersionFlags::Deleted
            } else {
                VersionFlags::Alive
            },
        }
    }

    /// Serializes version metadata to bytes
    ///
    /// Format: [tx_id: 8 bytes][commit_ts: 8 bytes][prev_version: 8 bytes (0xFFFFFFFFFFFFFFFF if None)][flags: 1 byte]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = vec![0u8; VERSION_METADATA_SIZE];
        buf[0..8].copy_from_slice(&self.tx_id.to_le_bytes());
        buf[8..16].copy_from_slice(&self.commit_ts.to_le_bytes());

        // Encode prev_version: u64::MAX if None, otherwise serialize pointer
        let prev_ts = self.prev_version.map_or(u64::MAX, |ptr| {
            // Encode as: page_id as u32 << 32 | slot_index as u32
            ((ptr.page_id as u64) << 32) | (ptr.slot_index as u64)
        });
        buf[16..24].copy_from_slice(&prev_ts.to_le_bytes());

        buf[24] = self.flags.to_byte();

        buf
    }

    /// Deserializes version metadata from bytes
    ///
    /// # Errors
    /// Returns an error if the buffer is too short
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < VERSION_METADATA_SIZE {
            return Err(GraphError::Corruption(
                "version metadata buffer too short".into(),
            ));
        }

        let tx_id = u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]);

        let commit_ts = u64::from_le_bytes([
            bytes[8], bytes[9], bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15],
        ]);

        let prev_ts = u64::from_le_bytes([
            bytes[16], bytes[17], bytes[18], bytes[19], bytes[20], bytes[21], bytes[22], bytes[23],
        ]);

        let flags = VersionFlags::from_byte(bytes[24]);

        let prev_version = if prev_ts == u64::MAX {
            None
        } else {
            Some(RecordPointer {
                page_id: (prev_ts >> 32) as u32,
                slot_index: (prev_ts & 0xFFFFFFFF) as u16,
                byte_offset: 0, // Not stored in version metadata
            })
        };

        Ok(Self {
            tx_id,
            commit_ts,
            prev_version,
            flags,
        })
    }

    /// Checks if this version is deleted
    pub fn is_deleted(&self) -> bool {
        self.flags.is_deleted()
    }

    /// Checks if this version is committed
    pub fn is_committed(&self) -> bool {
        self.commit_ts != 0
    }
}

/// Extends RecordKind with version types
#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum VersionedRecordKind {
    /// Free slot
    Free = 0x00,
    /// Node record (non-versioned, for backwards compatibility)
    Node = 0x01,
    /// Edge record (non-versioned, for backwards compatibility)
    Edge = 0x02,
    /// Versioned node record
    VersionedNode = 0x03,
    /// Versioned edge record
    VersionedEdge = 0x04,
}

impl VersionedRecordKind {
    pub fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0x00 => Ok(Self::Free),
            0x01 => Ok(Self::Node),
            0x02 => Ok(Self::Edge),
            0x03 => Ok(Self::VersionedNode),
            0x04 => Ok(Self::VersionedEdge),
            other => Err(GraphError::Corruption(format!(
                "unknown versioned record kind: 0x{other:02X}"
            ))),
        }
    }

    pub fn to_byte(self) -> u8 {
        self as u8
    }

    /// Checks if this record kind is versioned
    pub fn is_versioned(self) -> bool {
        matches!(self, Self::VersionedNode | Self::VersionedEdge)
    }

    /// Gets the corresponding base record kind
    pub fn base_kind(self) -> crate::storage::record::RecordKind {
        use crate::storage::record::RecordKind;
        match self {
            Self::Free => RecordKind::Free,
            Self::Node | Self::VersionedNode => RecordKind::Node,
            Self::Edge | Self::VersionedEdge => RecordKind::Edge,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::RecordPointer;

    #[test]
    fn test_version_metadata_serialization() {
        let metadata = VersionMetadata::new(100, 200, None, false);
        let bytes = metadata.to_bytes();

        let deserialized = VersionMetadata::from_bytes(&bytes).unwrap();
        assert_eq!(deserialized.tx_id, 100);
        assert_eq!(deserialized.commit_ts, 200);
        assert_eq!(deserialized.prev_version, None);
        assert!(!deserialized.is_deleted());
    }

    #[test]
    fn test_version_metadata_with_prev_pointer() {
        let prev_ptr = RecordPointer {
            page_id: 5,
            slot_index: 10,
            byte_offset: 20,
        };
        let metadata = VersionMetadata::new(100, 200, Some(prev_ptr), false);
        let bytes = metadata.to_bytes();

        let deserialized = VersionMetadata::from_bytes(&bytes).unwrap();
        assert_eq!(
            deserialized.prev_version,
            Some(RecordPointer {
                page_id: 5,
                slot_index: 10,
                byte_offset: 0, // Not restored
            })
        );
    }

    #[test]
    fn test_version_metadata_deleted() {
        let metadata = VersionMetadata::new(100, 200, None, true);
        assert!(metadata.is_deleted());

        let bytes = metadata.to_bytes();
        let deserialized = VersionMetadata::from_bytes(&bytes).unwrap();
        assert!(deserialized.is_deleted());
    }

    #[test]
    fn test_versioned_record_kind() {
        assert!(VersionedRecordKind::VersionedNode.is_versioned());
        assert!(VersionedRecordKind::VersionedEdge.is_versioned());
        assert!(!VersionedRecordKind::Node.is_versioned());
        assert!(!VersionedRecordKind::Edge.is_versioned());
    }
}
