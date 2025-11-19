use crate::storage::btree::ValCodec;
use crate::types::{Result, SombraError};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::convert::TryFrom;
use std::thread::ThreadId;
use std::time::{Duration, Instant};

/// Opaque identifier assigned to every committed write transaction.
///
/// Currently identical to the pager LSN space, but separated so future
/// refactors can evolve independently.
pub type CommitId = u64;

/// Sentinel commit ID meaning "visible forever".
pub const COMMIT_MAX: CommitId = 0;

/// Length of the encoded [`VersionHeader`] in bytes.
pub const VERSION_HEADER_LEN: usize = 20;
/// Length of the encoded [`VersionPtr`] value in bytes.
pub const VERSION_PTR_LEN: usize = 8;

/// Pointer into the version log; zero means "null".
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct VersionPtr(u64);

impl VersionPtr {
    /// Returns a null pointer that references no historical version.
    pub const fn null() -> Self {
        Self(0)
    }

    /// Returns `true` when the pointer references no entry.
    pub const fn is_null(self) -> bool {
        self.0 == 0
    }

    /// Creates a pointer from its raw integer representation.
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// Returns the underlying raw integer.
    pub const fn raw(self) -> u64 {
        self.0
    }

    /// Encodes the pointer into big-endian bytes.
    pub fn to_bytes(self) -> [u8; VERSION_PTR_LEN] {
        self.0.to_be_bytes()
    }

    /// Decodes a pointer from a big-endian byte slice.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < VERSION_PTR_LEN {
            return Err(SombraError::Corruption("version pointer truncated"));
        }
        let mut buf = [0u8; VERSION_PTR_LEN];
        buf.copy_from_slice(&bytes[..VERSION_PTR_LEN]);
        Ok(Self(u64::from_be_bytes(buf)))
    }
}

impl Default for VersionPtr {
    fn default() -> Self {
        Self::null()
    }
}

/// MVCC record flags.
pub mod flags {
    /// Record represents a logical delete (tombstone).
    pub const TOMBSTONE: u16 = 0x0001;
    /// Payload is stored externally (e.g., via `VRef` or implicit unit value).
    pub const PAYLOAD_EXTERNAL: u16 = 0x0002;
    /// Record is pending (not yet visible).
    pub const PENDING: u16 = 0x0004;
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

    /// Indicates whether the record is pending visibility.
    pub fn is_pending(&self) -> bool {
        (self.flags & flags::PENDING) != 0
    }

    /// Clears the pending flag.
    pub fn clear_pending(&mut self) {
        self.flags &= !flags::PENDING;
    }

    /// Marks the record as pending.
    pub fn set_pending(&mut self) {
        self.flags |= flags::PENDING;
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
    reader_refs: u32,
    committed_at: Option<Instant>,
}

#[derive(Clone, Debug)]
struct ActiveReader {
    snapshot: CommitId,
    begin_instant: Instant,
    thread_id: ThreadId,
}

const MAX_SLOW_READER_SAMPLES: usize = 4;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CommitReaderSource {
    /// Reader pinned a snapshot that no longer has an explicit table entry.
    Floor,
    /// Reader pinned a snapshot that still has a table entry.
    Entry,
}

/// Unique identifier assigned to active readers for diagnostics.
pub type ReaderId = u32;

/// Token returned when a reader registers with the [`CommitTable`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CommitReader {
    id: ReaderId,
    commit: CommitId,
    source: CommitReaderSource,
}

impl CommitReader {
    fn new(id: ReaderId, commit: CommitId, source: CommitReaderSource) -> Self {
        Self { id, commit, source }
    }

    /// Returns the commit ID this reader pins.
    pub fn commit(&self) -> CommitId {
        self.commit
    }

    fn id(&self) -> ReaderId {
        self.id
    }

    fn source(&self) -> CommitReaderSource {
        self.source
    }
}

/// Summary of reader activity captured from the [`CommitTable`].
#[derive(Clone, Debug, Default)]
pub struct ReaderSnapshot {
    /// Total active readers currently registered.
    pub active: u64,
    /// Oldest snapshot commit held by any reader.
    pub oldest_snapshot: Option<CommitId>,
    /// Newest snapshot commit held by any reader.
    pub newest_snapshot: Option<CommitId>,
    /// Maximum observed reader age in milliseconds.
    pub max_age_ms: u64,
    /// Sample of the slowest readers for diagnostics.
    pub slow_readers: Vec<ReaderSnapshotEntry>,
}

/// Detailed information about an individual reader captured in a [`ReaderSnapshot`].
#[derive(Clone, Debug)]
pub struct ReaderSnapshotEntry {
    /// Unique reader identifier.
    pub reader_id: ReaderId,
    /// Snapshot commit pinned by the reader.
    pub snapshot_commit: CommitId,
    /// Approximate reader age in milliseconds.
    pub age_ms: u64,
    /// Thread identifier associated with the reader, when available.
    pub thread_id: ThreadId,
}

/// Summary of a single commit tracked inside the commit table.
#[derive(Clone, Debug)]
pub struct CommitEntrySnapshot {
    /// Commit identifier.
    pub id: CommitId,
    /// Lifecycle state for the commit.
    pub status: CommitStatus,
    /// Number of readers currently referencing this commit entry.
    pub reader_refs: u32,
    /// Age of the commit in milliseconds (if committed).
    pub committed_ms_ago: Option<u64>,
}

/// Snapshot of the commit table state used for diagnostics.
#[derive(Clone, Debug)]
pub struct CommitTableSnapshot {
    /// Oldest commit released back to the free list.
    pub released_up_to: CommitId,
    /// Smallest commit that must remain visible to readers.
    pub oldest_visible: CommitId,
    /// Outstanding commit entries that have not been released yet.
    pub entries: Vec<CommitEntrySnapshot>,
    /// Live reader statistics captured during the snapshot.
    pub reader_snapshot: ReaderSnapshot,
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
    reader_floor: BTreeMap<CommitId, u32>,
    readers: HashMap<ReaderId, ActiveReader>,
    next_reader_id: ReaderId,
}

impl CommitTable {
    /// Creates a new table beginning after the provided `start_id`.
    pub fn new(start_id: CommitId) -> Self {
        Self {
            released_up_to: start_id,
            entries: VecDeque::new(),
            reader_floor: BTreeMap::new(),
            readers: HashMap::new(),
            next_reader_id: 1,
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
            reader_refs: 0,
            committed_at: None,
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
        entry.committed_at = Some(Instant::now());
        Ok(())
    }

    /// Registers a reader pinned to `snapshot` and returns a token for release.
    pub fn register_reader(
        &mut self,
        snapshot: CommitId,
        now: Instant,
        thread_id: ThreadId,
    ) -> Result<CommitReader> {
        if snapshot <= self.released_up_to {
            let counter = self.reader_floor.entry(snapshot).or_insert(0);
            if *counter == u32::MAX {
                return Err(SombraError::Invalid("reader floor overflow"));
            }
            *counter += 1;
            let id = self.allocate_reader_id()?;
            self.readers.insert(
                id,
                ActiveReader {
                    snapshot,
                    begin_instant: now,
                    thread_id,
                },
            );
            return Ok(CommitReader::new(id, snapshot, CommitReaderSource::Floor));
        }
        let entry = self
            .entries
            .iter_mut()
            .find(|entry| entry.id == snapshot)
            .ok_or_else(|| SombraError::Invalid("reader snapshot unknown"))?;
        if entry.reader_refs == u32::MAX {
            return Err(SombraError::Invalid("reader ref overflow"));
        }
        entry.reader_refs += 1;
        let id = self.allocate_reader_id()?;
        self.readers.insert(
            id,
            ActiveReader {
                snapshot,
                begin_instant: now,
                thread_id,
            },
        );
        Ok(CommitReader::new(id, snapshot, CommitReaderSource::Entry))
    }

    /// Releases a previously registered reader token.
    pub fn release_reader(&mut self, reader: CommitReader) {
        match reader.source() {
            CommitReaderSource::Floor => {
                if let Some(counter) = self.reader_floor.get_mut(&reader.commit()) {
                    if *counter > 1 {
                        *counter -= 1;
                    } else {
                        self.reader_floor.remove(&reader.commit());
                    }
                }
            }
            CommitReaderSource::Entry => {
                if let Some(entry) = self.entries.iter_mut().find(|e| e.id == reader.commit()) {
                    if entry.reader_refs > 0 {
                        entry.reader_refs -= 1;
                    }
                }
            }
        }
        self.readers.remove(&reader.id());
    }

    fn allocate_reader_id(&mut self) -> Result<ReaderId> {
        let id = self.next_reader_id;
        if id == ReaderId::MAX {
            return Err(SombraError::Invalid("reader id overflow"));
        }
        self.next_reader_id = self.next_reader_id.saturating_add(1);
        Ok(id)
    }

    /// Releases committed entries up to (and including) `upto_id`.
    ///
    /// Callers should advance this after checkpoints or GC reclaim version chains.
    pub fn release_committed(&mut self, upto_id: CommitId) {
        while let Some(front) = self.entries.front() {
            if front.id > upto_id
                || front.status != CommitStatus::Committed
                || front.reader_refs > 0
            {
                break;
            }
            self.released_up_to = front.id;
            self.entries.pop_front();
        }
    }

    /// Returns the smallest commit ID that must remain visible to readers.
    pub fn oldest_visible(&self) -> CommitId {
        if let Some((&commit, _)) = self.reader_floor.iter().next() {
            return commit;
        }
        self.entries
            .front()
            .map(|entry| entry.id)
            .unwrap_or(self.released_up_to)
    }

    /// Returns the maximum commit ID eligible for cleanup given `retention`.
    pub fn vacuum_horizon(&self, retention: Duration) -> CommitId {
        let cutoff = Instant::now()
            .checked_sub(retention)
            .unwrap_or_else(Instant::now);
        let mut floor = self.released_up_to;
        for entry in self.entries.iter().rev() {
            if entry.status != CommitStatus::Committed {
                continue;
            }
            if let Some(committed_at) = entry.committed_at {
                if committed_at <= cutoff {
                    floor = floor.max(entry.id);
                    break;
                }
            }
        }
        let oldest = self.oldest_visible();
        floor.min(oldest)
    }

    /// Returns a snapshot describing currently active readers.
    pub fn reader_snapshot(&self, now: Instant) -> ReaderSnapshot {
        let mut snapshot = ReaderSnapshot::default();
        if self.readers.is_empty() {
            return snapshot;
        }
        let mut oldest: Option<CommitId> = None;
        let mut newest: Option<CommitId> = None;
        let mut slow = Vec::with_capacity(self.readers.len().min(MAX_SLOW_READER_SAMPLES));
        for (&reader_id, info) in &self.readers {
            snapshot.active = snapshot.active.saturating_add(1);
            let current_oldest = oldest.map_or(info.snapshot, |current| current.min(info.snapshot));
            oldest = Some(current_oldest);
            let current_newest = newest.map_or(info.snapshot, |current| current.max(info.snapshot));
            newest = Some(current_newest);
            let age_ms = now
                .saturating_duration_since(info.begin_instant)
                .as_millis()
                .min(u64::MAX as u128) as u64;
            if age_ms > snapshot.max_age_ms {
                snapshot.max_age_ms = age_ms;
            }
            slow.push(ReaderSnapshotEntry {
                reader_id,
                snapshot_commit: info.snapshot,
                age_ms,
                thread_id: info.thread_id,
            });
        }
        slow.sort_by(|a, b| b.age_ms.cmp(&a.age_ms));
        if slow.len() > MAX_SLOW_READER_SAMPLES {
            slow.truncate(MAX_SLOW_READER_SAMPLES);
        }
        snapshot.oldest_snapshot = oldest;
        snapshot.newest_snapshot = newest;
        snapshot.slow_readers = slow;
        snapshot
    }

    /// Captures a diagnostic snapshot of the commit table state.
    pub fn snapshot(&self, now: Instant) -> CommitTableSnapshot {
        let entries = self
            .entries
            .iter()
            .map(|entry| CommitEntrySnapshot {
                id: entry.id,
                status: entry.status,
                reader_refs: entry.reader_refs,
                committed_ms_ago: entry.committed_at.map(|instant| {
                    now.saturating_duration_since(instant)
                        .as_millis()
                        .min(u64::MAX as u128) as u64
                }),
            })
            .collect();
        CommitTableSnapshot {
            released_up_to: self.released_up_to,
            oldest_visible: self.oldest_visible(),
            entries,
            reader_snapshot: self.reader_snapshot(now),
        }
    }
}

#[cfg(test)]
mod commit_table_tests {
    use super::*;
    use std::thread;
    use std::time::{Duration, Instant};

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

    #[test]
    fn reader_floor_tracks_oldest() {
        let mut table = CommitTable::new(0);
        // Snapshot at released commit pins floor.
        let reader = table
            .register_reader(0, Instant::now(), thread::current().id())
            .unwrap();
        assert_eq!(table.oldest_visible(), 0);
        // New commits still see oldest floor commit.
        table.reserve(1).unwrap();
        table.mark_committed(1).unwrap();
        assert_eq!(table.oldest_visible(), 0);
        table.release_reader(reader);
        assert_eq!(table.oldest_visible(), 1);
    }

    #[test]
    fn reader_blocks_release_until_drop() {
        let mut table = CommitTable::new(0);
        table.reserve(1).unwrap();
        table.mark_committed(1).unwrap();
        let reader = table
            .register_reader(1, Instant::now(), thread::current().id())
            .unwrap();
        table.release_committed(1);
        // Entry remains because reader is active.
        assert_eq!(table.oldest_visible(), 1);
        table.release_reader(reader);
        table.release_committed(1);
        assert_eq!(table.oldest_visible(), 1);
    }

    #[test]
    fn multiple_floor_readers_share_same_commit() {
        let mut table = CommitTable::new(5);
        let r1 = table
            .register_reader(5, Instant::now(), thread::current().id())
            .unwrap();
        let r2 = table
            .register_reader(5, Instant::now(), thread::current().id())
            .unwrap();
        assert_eq!(table.oldest_visible(), 5);
        table.release_reader(r1);
        assert_eq!(table.oldest_visible(), 5);
        table.release_reader(r2);
        assert_eq!(table.oldest_visible(), 5);
    }

    #[test]
    fn vacuum_horizon_respects_active_readers() {
        let mut table = CommitTable::new(0);
        table.reserve(1).unwrap();
        table.mark_committed(1).unwrap();
        table.reserve(2).unwrap();
        table.mark_committed(2).unwrap();
        let reader = table
            .register_reader(1, Instant::now(), thread::current().id())
            .unwrap();
        assert_eq!(table.vacuum_horizon(Duration::from_millis(0)), 1);
        table.release_reader(reader);
        table.release_committed(1);
        assert_eq!(table.vacuum_horizon(Duration::from_millis(0)), 2);
    }

    #[test]
    fn vacuum_horizon_respects_retention_window() {
        let mut table = CommitTable::new(0);
        table.reserve(1).unwrap();
        table.mark_committed(1).unwrap();
        table.reserve(2).unwrap();
        table.mark_committed(2).unwrap();
        let long_retention = table.vacuum_horizon(Duration::from_secs(60));
        assert_eq!(long_retention, 0);
        table.release_committed(2);
        let immediate = table.vacuum_horizon(Duration::from_millis(0));
        assert_eq!(immediate, 2);
    }

    #[test]
    fn reader_snapshot_reports_activity() {
        let mut table = CommitTable::new(0);
        table.reserve(1).unwrap();
        table.mark_committed(1).unwrap();
        let now = Instant::now();
        let old_start = now - Duration::from_millis(25);
        let reader = table
            .register_reader(1, old_start, thread::current().id())
            .unwrap();
        let snapshot = table.reader_snapshot(now);
        assert_eq!(snapshot.active, 1);
        assert_eq!(snapshot.oldest_snapshot, Some(1));
        assert_eq!(snapshot.newest_snapshot, Some(1));
        assert!(
            snapshot.max_age_ms >= 25,
            "max_age_ms {} insufficient",
            snapshot.max_age_ms
        );
        assert_eq!(snapshot.slow_readers.len(), 1);
        assert_eq!(snapshot.slow_readers[0].snapshot_commit, 1);
        table.release_reader(reader);
        let empty = table.reader_snapshot(now);
        assert_eq!(empty.active, 0);
        assert!(empty.oldest_snapshot.is_none());
    }

    #[test]
    fn snapshot_reports_entries() {
        let mut table = CommitTable::new(0);
        table.reserve(1).unwrap();
        table.mark_committed(1).unwrap();
        table.reserve(2).unwrap();
        let reader = table
            .register_reader(2, Instant::now(), thread::current().id())
            .unwrap();
        let now = Instant::now();
        let snapshot = table.snapshot(now);
        assert_eq!(snapshot.released_up_to, 0);
        assert_eq!(snapshot.oldest_visible, 1);
        assert_eq!(snapshot.entries.len(), 2);
        let committed = snapshot
            .entries
            .iter()
            .find(|entry| entry.id == 1)
            .expect("committed entry present");
        assert_eq!(committed.status, CommitStatus::Committed);
        assert!(committed.committed_ms_ago.is_some());
        let pending = snapshot
            .entries
            .iter()
            .find(|entry| entry.id == 2)
            .expect("pending entry present");
        assert_eq!(pending.status, CommitStatus::Pending);
        assert_eq!(pending.reader_refs, 1);
        assert!(pending.committed_ms_ago.is_none());
        assert_eq!(snapshot.reader_snapshot.active, 1);
        table.release_reader(reader);
    }

    #[test]
    fn version_log_entry_roundtrip() {
        let entry = VersionLogEntry {
            space: VersionSpace::Node,
            id: 42,
            header: VersionHeader::new(7, 11, 0x10, 32),
            prev_ptr: VersionPtr::from_raw(9),
            bytes: vec![1, 2, 3, 4, 5],
        };
        let encoded = entry.encode().expect("encode succeeds");
        let decoded = VersionLogEntry::decode(&encoded).expect("decode succeeds");
        assert_eq!(decoded.space, entry.space);
        assert_eq!(decoded.id, entry.id);
        assert_eq!(decoded.header, entry.header);
        assert_eq!(decoded.prev_ptr.raw(), entry.prev_ptr.raw());
        assert_eq!(decoded.bytes, entry.bytes);
    }
}

/// Logical collection owning a version.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VersionSpace {
    /// Node row stored in the primary node tree.
    Node,
    /// Edge row stored in the edge tree.
    Edge,
}

impl VersionSpace {
    fn encode(self) -> u8 {
        match self {
            VersionSpace::Node => 0,
            VersionSpace::Edge => 1,
        }
    }

    fn decode(tag: u8) -> Result<Self> {
        match tag {
            0 => Ok(VersionSpace::Node),
            1 => Ok(VersionSpace::Edge),
            _ => Err(SombraError::Corruption("unknown version space tag")),
        }
    }
}

/// Historical version held outside the primary B-tree.
#[derive(Clone, Debug)]
pub struct VersionLogEntry {
    /// Collection the version belongs to.
    pub space: VersionSpace,
    /// Logical identifier (e.g., node or edge id).
    pub id: u64,
    /// MVCC metadata describing the version.
    pub header: VersionHeader,
    /// Pointer to the next older version.
    pub prev_ptr: VersionPtr,
    /// Encoded record bytes (header + ptr + payload).
    pub bytes: Vec<u8>,
}

impl VersionLogEntry {
    /// Encodes the entry into an owned byte buffer.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let len = u32::try_from(self.bytes.len())
            .map_err(|_| SombraError::Invalid("version log payload too large"))?;
        let mut out =
            Vec::with_capacity(1 + 8 + VERSION_HEADER_LEN + VERSION_PTR_LEN + 4 + self.bytes.len());
        out.push(self.space.encode());
        out.extend_from_slice(&self.id.to_be_bytes());
        self.header.encode_into(&mut out);
        out.extend_from_slice(&self.prev_ptr.to_bytes());
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(&self.bytes);
        Ok(out)
    }

    /// Decodes a log entry from the provided bytes.
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < 1 + 8 + VERSION_HEADER_LEN + VERSION_PTR_LEN + 4 {
            return Err(SombraError::Corruption("version log entry truncated"));
        }
        let space = VersionSpace::decode(data[0])?;
        let mut offset = 1;
        let mut id_bytes = [0u8; 8];
        id_bytes.copy_from_slice(&data[offset..offset + 8]);
        let id = u64::from_be_bytes(id_bytes);
        offset += 8;
        let header = VersionHeader::decode(&data[offset..offset + VERSION_HEADER_LEN])?;
        offset += VERSION_HEADER_LEN;
        let prev_ptr = VersionPtr::from_bytes(&data[offset..offset + VERSION_PTR_LEN])?;
        offset += VERSION_PTR_LEN;
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&data[offset..offset + 4]);
        offset += 4;
        let payload_len = u32::from_be_bytes(len_bytes) as usize;
        if data.len() < offset + payload_len {
            return Err(SombraError::Corruption("version log payload truncated"));
        }
        let bytes = data[offset..offset + payload_len].to_vec();
        Ok(Self {
            space,
            id,
            header,
            prev_ptr,
            bytes,
        })
    }
}
