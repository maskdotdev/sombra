use crate::storage::btree::ValCodec;
use crate::types::{Result, SombraError};
use std::collections::VecDeque;

/// Opaque identifier assigned to every committed write transaction.
///
/// Currently identical to the pager LSN space, but separated so future
/// refactors can evolve independently.
pub type CommitId = u64;

/// Sentinel commit ID meaning "visible forever".
pub const COMMIT_MAX: CommitId = 0;

/// Length of the encoded [`VersionHeader`] in bytes.
pub const VERSION_HEADER_LEN: usize = 20;

/// MVCC record flags.
pub mod flags {
    /// Record represents a logical delete (tombstone).
    pub const TOMBSTONE: u16 = 0x0001;
    /// Payload is stored externally (e.g., via `VRef` or implicit unit value).
    pub const PAYLOAD_EXTERNAL: u16 = 0x0002;
}

/// Fixed-size header prepended to every MVCC-aware payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct VersionHeader {
    /// First commit ID where this version becomes visible.
    pub begin: CommitId,
    /// Exclusive commit ID where the version stops being visible. `COMMIT_MAX`
    /// (zero) means unbounded.
    pub end: CommitId,
    /// Bitflags captured in [`flags`].
    pub flags: u16,
    /// Inline payload length. Zero implies an external payload reference.
    pub payload_len: u16,
}

impl VersionHeader {
    /// Builds a new header.
    pub const fn new(begin: CommitId, end: CommitId, flags: u16, payload_len: u16) -> Self {
        Self {
            begin,
            end,
            flags,
            payload_len,
        }
    }

    /// Encodes the header into a fixed `[u8; VERSION_HEADER_LEN]` array.
    pub fn encode(&self) -> [u8; VERSION_HEADER_LEN] {
        let mut buf = [0u8; VERSION_HEADER_LEN];
        buf[0..8].copy_from_slice(&self.begin.to_be_bytes());
        buf[8..16].copy_from_slice(&self.end.to_be_bytes());
        buf[16..18].copy_from_slice(&self.flags.to_be_bytes());
        buf[18..20].copy_from_slice(&self.payload_len.to_be_bytes());
        buf
    }

    /// Appends the encoded header to an output buffer.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.encode());
    }

    /// Decodes a header from the provided byte slice.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < VERSION_HEADER_LEN {
            return Err(SombraError::Corruption("version header truncated"));
        }
        let begin = CommitId::from_be_bytes(bytes[0..8].try_into().unwrap());
        let end = CommitId::from_be_bytes(bytes[8..16].try_into().unwrap());
        let flags = u16::from_be_bytes(bytes[16..18].try_into().unwrap());
        let payload_len = u16::from_be_bytes(bytes[18..20].try_into().unwrap());
        Ok(Self {
            begin,
            end,
            flags,
            payload_len,
        })
    }

    /// Returns `true` when the version is visible for the given snapshot commit.
    pub fn visible_at(&self, snapshot: CommitId) -> bool {
        if snapshot < self.begin {
            return false;
        }
        if self.end == COMMIT_MAX {
            return true;
        }
        snapshot < self.end
    }

    /// Indicates whether the payload is stored externally.
    pub fn payload_external(&self) -> bool {
        (self.flags & flags::PAYLOAD_EXTERNAL) != 0
    }

    /// Indicates whether the record encodes a tombstone.
    pub fn is_tombstone(&self) -> bool {
        (self.flags & flags::TOMBSTONE) != 0
    }
}

/// Value wrapper that prefixes the encoded payload with a [`VersionHeader`].
#[derive(Clone, Debug)]
pub struct VersionedValue<V> {
    /// MVCC metadata for the value.
    pub header: VersionHeader,
    /// Inner value being stored.
    pub value: V,
}

impl<V> VersionedValue<V> {
    /// Creates a new versioned value.
    pub fn new(header: VersionHeader, value: V) -> Self {
        Self { header, value }
    }
}

impl<V: ValCodec> ValCodec for VersionedValue<V> {
    fn encode_val(value: &Self, out: &mut Vec<u8>) {
        let mut payload = Vec::new();
        V::encode_val(&value.value, &mut payload);
        let mut header = value.header;
        let payload_len = payload.len().min(u16::MAX as usize) as u16;
        header.payload_len = payload_len;
        header.encode_into(out);
        out.extend_from_slice(&payload);
    }

    fn decode_val(src: &[u8]) -> Result<Self> {
        if src.len() < VERSION_HEADER_LEN {
            return Err(SombraError::Corruption("versioned value truncated"));
        }
        let header = VersionHeader::decode(&src[..VERSION_HEADER_LEN])?;
        let payload = &src[VERSION_HEADER_LEN..];
        if payload.len() < header.payload_len as usize {
            return Err(SombraError::Corruption(
                "versioned value payload shorter than expected",
            ));
        }
        let value = V::decode_val(payload)?;
        Ok(Self { header, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_roundtrip() {
        let header = VersionHeader::new(42, 0, flags::TOMBSTONE | flags::PAYLOAD_EXTERNAL, 0);
        let encoded = header.encode();
        let decoded = VersionHeader::decode(&encoded).expect("decode succeeds");
        assert_eq!(decoded, header);
    }

    #[test]
    fn visible_at_checks_bounds() {
        let header = VersionHeader::new(5, 10, 0, 16);
        assert!(!header.visible_at(4));
        assert!(header.visible_at(5));
        assert!(header.visible_at(9));
        assert!(!header.visible_at(10));
    }

    #[test]
    fn visible_at_infinite_end() {
        let header = VersionHeader::new(3, COMMIT_MAX, 0, 12);
        assert!(header.visible_at(100));
    }
}

/// Lifecycle state for a commit tracked by [`CommitTable`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitStatus {
    /// Commit ID has been handed out but not finished.
    Pending,
    /// Commit finished and is safe for readers to observe.
    Committed,
}

#[derive(Clone, Debug)]
struct CommitEntry {
    id: CommitId,
    status: CommitStatus,
}

/// In-memory commit table backed by WAL redo in future stages.
///
/// The pager hands out monotonically increasing IDs via [`reserve`], storage records
/// visibility transitions with [`mark_committed`], and checkpoints/vacuum reclaim
/// entries via [`release_committed`].  Once MVCC metadata is persisted, this table
/// becomes the authoritative map from commit IDs to visibility states.
#[derive(Clone, Debug)]
pub struct CommitTable {
    released_up_to: CommitId,
    entries: VecDeque<CommitEntry>,
}

impl CommitTable {
    /// Creates a new table beginning after the provided `start_id`.
    pub fn new(start_id: CommitId) -> Self {
        Self {
            released_up_to: start_id,
            entries: VecDeque::new(),
        }
    }

    /// Registers a reserved commit ID, marking it pending.
    pub fn reserve(&mut self, id: CommitId) -> Result<()> {
        if id == COMMIT_MAX {
            return Err(SombraError::Invalid("commit id zero reserved"));
        }
        if id <= self.released_up_to {
            return Err(SombraError::Invalid("commit id already released"));
        }
        if let Some(last) = self.entries.back() {
            if id <= last.id {
                return Err(SombraError::Invalid("commit id must increase"));
            }
        }
        self.entries.push_back(CommitEntry {
            id,
            status: CommitStatus::Pending,
        });
        Ok(())
    }

    /// Marks a previously reserved commit as committed.
    pub fn mark_committed(&mut self, id: CommitId) -> Result<()> {
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.id == id)
            .ok_or_else(|| SombraError::Invalid("unknown commit id"))?;
        if entry.status == CommitStatus::Committed {
            return Err(SombraError::Invalid("commit already finalized"));
        }
        entry.status = CommitStatus::Committed;
        Ok(())
    }

    /// Releases committed entries up to (and including) `upto_id`.
    ///
    /// Callers should advance this after checkpoints or GC reclaim version chains.
    pub fn release_committed(&mut self, upto_id: CommitId) {
        while let Some(front) = self.entries.front() {
            if front.id > upto_id || front.status != CommitStatus::Committed {
                break;
            }
            self.released_up_to = front.id;
            self.entries.pop_front();
        }
    }

    /// Returns the smallest commit ID that must remain visible to readers.
    pub fn oldest_visible(&self) -> CommitId {
        self.entries
            .front()
            .map(|entry| entry.id)
            .unwrap_or(self.released_up_to)
    }
}

#[cfg(test)]
mod commit_table_tests {
    use super::*;

    #[test]
    fn reserve_and_commit_flow() {
        let mut table = CommitTable::new(0);
        table.reserve(1).unwrap();
        table.reserve(2).unwrap();
        assert_eq!(table.oldest_visible(), 1);
        table.mark_committed(1).unwrap();
        table.release_committed(1);
        assert_eq!(table.oldest_visible(), 2);
        table.mark_committed(2).unwrap();
        table.release_committed(2);
        assert_eq!(table.oldest_visible(), 2);
    }

    #[test]
    fn reject_unknown_ids() {
        let mut table = CommitTable::new(10);
        assert!(table.reserve(5).is_err());
        table.reserve(11).unwrap();
        assert!(table.reserve(11).is_err());
        assert!(table.mark_committed(5).is_err());
    }
}
