//! Adjacency segment structures for Index-Free Adjacency.
//!
//! Defines `AdjSegment` - the MVCC-aware adjacency list for a specific
//! (node, direction, type) combination, and `AdjEntry` - individual
//! neighbor references within a segment.

use super::types::SegmentPtr;
use super::TxId;
use crate::storage::adjacency::Dir;
use crate::types::{EdgeId, NodeId, Result, SombraError, TypeId};
use std::convert::TryInto;

/// Size of encoded AdjEntry in bytes.
/// neighbor (8) + edge (8) + xmin (8) + xmax (8) = 32 bytes
pub const ADJ_ENTRY_LEN: usize = 32;

/// Size of encoded AdjSegmentHeader in bytes.
/// owner (8) + dir (1) + reserved (1) + type (4) + xmin (8) + xmax (8) +
/// prev_version (8) + next_extent (8) + entry_count (4) = 50 bytes
pub const ADJ_SEGMENT_HEADER_LEN: usize = 50;

/// A single entry in an adjacency segment.
///
/// Represents one neighbor relationship: the neighbor node, the edge
/// connecting them, and MVCC visibility info.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdjEntry {
    /// The neighboring node ID.
    /// For OUT direction: this is the destination node.
    /// For IN direction: this is the source node.
    pub neighbor: NodeId,
    /// The edge ID connecting this node to the neighbor.
    pub edge: EdgeId,
    /// Transaction ID that created this edge (edge's xmin).
    pub xmin: TxId,
    /// Transaction ID that deleted this edge (edge's xmax), 0 if not deleted.
    pub xmax: TxId,
}

impl AdjEntry {
    /// Creates a new adjacency entry.
    #[inline]
    pub const fn new(neighbor: NodeId, edge: EdgeId, xmin: TxId) -> Self {
        Self { neighbor, edge, xmin, xmax: 0 }
    }

    /// Creates a new adjacency entry with full visibility info.
    #[inline]
    pub const fn with_visibility(neighbor: NodeId, edge: EdgeId, xmin: TxId, xmax: TxId) -> Self {
        Self { neighbor, edge, xmin, xmax }
    }

    /// Returns true if this entry is visible at the given snapshot.
    #[inline]
    pub fn visible_at(&self, snapshot: TxId) -> bool {
        if self.xmin > snapshot {
            return false;
        }
        self.xmax == 0 || self.xmax > snapshot
    }

    /// Encodes the entry as big-endian bytes.
    pub fn encode(&self) -> [u8; ADJ_ENTRY_LEN] {
        let mut buf = [0u8; ADJ_ENTRY_LEN];
        buf[0..8].copy_from_slice(&self.neighbor.0.to_be_bytes());
        buf[8..16].copy_from_slice(&self.edge.0.to_be_bytes());
        buf[16..24].copy_from_slice(&self.xmin.to_be_bytes());
        buf[24..32].copy_from_slice(&self.xmax.to_be_bytes());
        buf
    }

    /// Decodes an entry from big-endian bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < ADJ_ENTRY_LEN {
            return Err(SombraError::Corruption("adj entry truncated"));
        }
        let neighbor = NodeId(u64::from_be_bytes(bytes[0..8].try_into().unwrap()));
        let edge = EdgeId(u64::from_be_bytes(bytes[8..16].try_into().unwrap()));
        let xmin = u64::from_be_bytes(bytes[16..24].try_into().unwrap());
        let xmax = u64::from_be_bytes(bytes[24..32].try_into().unwrap());
        Ok(Self { neighbor, edge, xmin, xmax })
    }
}

/// Header for an adjacency segment.
///
/// Contains identity, MVCC version info, and chaining pointers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AdjSegmentHeader {
    /// Node that owns this adjacency list.
    pub owner: NodeId,
    /// Direction of edges (OUT or IN).
    pub dir: Dir,
    /// Edge type for this segment.
    pub type_id: TypeId,
    /// Transaction ID that created this version.
    pub xmin: TxId,
    /// Transaction ID that superseded this version (0 = still active).
    pub xmax: TxId,
    /// Pointer to the previous version of this (node, dir, type) segment.
    pub prev_version: SegmentPtr,
    /// Pointer to next extent page if entries overflow one page.
    pub next_extent: SegmentPtr,
    /// Number of entries in this segment (across all extents if chained).
    pub entry_count: u32,
}

impl AdjSegmentHeader {
    /// Creates a new segment header.
    pub const fn new(owner: NodeId, dir: Dir, type_id: TypeId, xmin: TxId) -> Self {
        Self {
            owner,
            dir,
            type_id,
            xmin,
            xmax: 0, // Not superseded
            prev_version: SegmentPtr::null(),
            next_extent: SegmentPtr::null(),
            entry_count: 0,
        }
    }

    /// Returns true if this version is still active (not superseded).
    #[inline]
    pub fn is_active(&self) -> bool {
        self.xmax == 0
    }

    /// Returns true if this version is visible at the given snapshot.
    ///
    /// A version is visible if:
    /// - xmin <= snapshot (created before or at snapshot)
    /// - xmax == 0 OR xmax > snapshot (not yet superseded at snapshot)
    #[inline]
    pub fn visible_at(&self, snapshot: TxId) -> bool {
        if self.xmin > snapshot {
            return false;
        }
        self.xmax == 0 || self.xmax > snapshot
    }

    /// Encodes direction as a byte.
    fn dir_to_u8(dir: Dir) -> u8 {
        match dir {
            Dir::Out => 0,
            Dir::In => 1,
            Dir::Both => 2,
        }
    }

    /// Decodes direction from a byte.
    fn dir_from_u8(byte: u8) -> Result<Dir> {
        match byte {
            0 => Ok(Dir::Out),
            1 => Ok(Dir::In),
            2 => Ok(Dir::Both),
            _ => Err(SombraError::Corruption("invalid direction byte")),
        }
    }

    /// Encodes the header as big-endian bytes.
    pub fn encode(&self) -> [u8; ADJ_SEGMENT_HEADER_LEN] {
        let mut buf = [0u8; ADJ_SEGMENT_HEADER_LEN];
        let mut offset = 0;

        // owner: 8 bytes
        buf[offset..offset + 8].copy_from_slice(&self.owner.0.to_be_bytes());
        offset += 8;

        // dir: 1 byte
        buf[offset] = Self::dir_to_u8(self.dir);
        offset += 1;

        // reserved: 1 byte
        buf[offset] = 0;
        offset += 1;

        // type_id: 4 bytes
        buf[offset..offset + 4].copy_from_slice(&self.type_id.0.to_be_bytes());
        offset += 4;

        // xmin: 8 bytes
        buf[offset..offset + 8].copy_from_slice(&self.xmin.to_be_bytes());
        offset += 8;

        // xmax: 8 bytes
        buf[offset..offset + 8].copy_from_slice(&self.xmax.to_be_bytes());
        offset += 8;

        // prev_version: 8 bytes
        buf[offset..offset + 8].copy_from_slice(&self.prev_version.to_bytes());
        offset += 8;

        // next_extent: 8 bytes
        buf[offset..offset + 8].copy_from_slice(&self.next_extent.to_bytes());
        offset += 8;

        // entry_count: 4 bytes
        buf[offset..offset + 4].copy_from_slice(&self.entry_count.to_be_bytes());

        buf
    }

    /// Decodes a header from big-endian bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < ADJ_SEGMENT_HEADER_LEN {
            return Err(SombraError::Corruption("adj segment header truncated"));
        }

        let mut offset = 0;

        let owner = NodeId(u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap()));
        offset += 8;

        let dir = Self::dir_from_u8(bytes[offset])?;
        offset += 1;

        // Skip reserved byte
        offset += 1;

        let type_id = TypeId(u32::from_be_bytes(
            bytes[offset..offset + 4].try_into().unwrap(),
        ));
        offset += 4;

        let xmin = u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let xmax = u64::from_be_bytes(bytes[offset..offset + 8].try_into().unwrap());
        offset += 8;

        let prev_version = SegmentPtr::from_bytes(&bytes[offset..offset + 8])?;
        offset += 8;

        let next_extent = SegmentPtr::from_bytes(&bytes[offset..offset + 8])?;
        offset += 8;

        let entry_count = u32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap());

        Ok(Self {
            owner,
            dir,
            type_id,
            xmin,
            xmax,
            prev_version,
            next_extent,
            entry_count,
        })
    }
}

/// An MVCC-aware adjacency segment for a (node, direction, type) combination.
///
/// Segments store sorted neighbor entries and support version chaining for
/// snapshot isolation. Each write creates a new segment version, linking
/// back to the previous version via `prev_version`.
///
/// # Layout
///
/// ```text
/// +-------------------+
/// | AdjSegmentHeader  |  50 bytes
/// +-------------------+
/// | AdjEntry[0]       |  16 bytes
/// | AdjEntry[1]       |  16 bytes
/// | ...               |
/// | AdjEntry[N-1]     |  16 bytes
/// +-------------------+
/// ```
///
/// For high-degree nodes that exceed one page, `next_extent` chains to
/// additional pages holding more entries.
#[derive(Clone, Debug)]
pub struct AdjSegment {
    /// Segment header with identity and MVCC info.
    pub header: AdjSegmentHeader,
    /// Sorted list of neighbor entries.
    pub entries: Vec<AdjEntry>,
}

impl AdjSegment {
    /// Creates a new empty segment.
    pub fn new(owner: NodeId, dir: Dir, type_id: TypeId, xmin: TxId) -> Self {
        Self {
            header: AdjSegmentHeader::new(owner, dir, type_id, xmin),
            entries: Vec::new(),
        }
    }

    /// Creates a new segment as a CoW clone of an existing segment.
    ///
    /// The new segment has:
    /// - Same owner, dir, type
    /// - New xmin, xmax=0
    /// - prev_version pointing to the old segment
    /// - Cloned entries
    pub fn cow_clone(old: &AdjSegment, old_ptr: SegmentPtr, new_xmin: TxId) -> Self {
        Self {
            header: AdjSegmentHeader {
                owner: old.header.owner,
                dir: old.header.dir,
                type_id: old.header.type_id,
                xmin: new_xmin,
                xmax: 0,
                prev_version: old_ptr,
                next_extent: SegmentPtr::null(), // Will be set if needed
                entry_count: old.header.entry_count,
            },
            entries: old.entries.clone(),
        }
    }

    /// Returns the number of entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns true if the segment has no entries.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Inserts a new entry, maintaining sorted order by neighbor ID.
    ///
    /// If the neighbor already exists with the same edge, this is a no-op.
    /// If the neighbor exists with a different edge, the new entry is added.
    pub fn insert(&mut self, entry: AdjEntry) {
        // Find insertion point
        match self
            .entries
            .binary_search_by_key(&entry.neighbor.0, |e| e.neighbor.0)
        {
            Ok(mut idx) => {
                // Neighbor exists - check if this exact entry exists
                // (same neighbor can have multiple edges of same type)
                while idx < self.entries.len() && self.entries[idx].neighbor == entry.neighbor {
                    if self.entries[idx].edge == entry.edge {
                        // Exact duplicate - no-op
                        return;
                    }
                    idx += 1;
                }
                // Insert after existing entries for this neighbor
                self.entries.insert(idx, entry);
            }
            Err(idx) => {
                self.entries.insert(idx, entry);
            }
        }
        self.header.entry_count = self.entries.len() as u32;
    }

    /// Removes an entry by neighbor and edge ID.
    ///
    /// Returns true if the entry was found and removed.
    pub fn remove(&mut self, neighbor: NodeId, edge: EdgeId) -> bool {
        if let Ok(start_idx) = self
            .entries
            .binary_search_by_key(&neighbor.0, |e| e.neighbor.0)
        {
            // Find the exact entry
            let mut idx = start_idx;
            while idx < self.entries.len() && self.entries[idx].neighbor == neighbor {
                if self.entries[idx].edge == edge {
                    self.entries.remove(idx);
                    self.header.entry_count = self.entries.len() as u32;
                    return true;
                }
                idx += 1;
            }
            // Also check backwards (binary_search might land in the middle)
            if start_idx > 0 {
                let mut idx = start_idx - 1;
                while self.entries[idx].neighbor == neighbor {
                    if self.entries[idx].edge == edge {
                        self.entries.remove(idx);
                        self.header.entry_count = self.entries.len() as u32;
                        return true;
                    }
                    if idx == 0 {
                        break;
                    }
                    idx -= 1;
                }
            }
        }
        false
    }

    /// Looks up all entries for a specific neighbor.
    pub fn lookup_neighbor(&self, neighbor: NodeId) -> Vec<AdjEntry> {
        let mut result = Vec::new();
        if let Ok(start_idx) = self
            .entries
            .binary_search_by_key(&neighbor.0, |e| e.neighbor.0)
        {
            // Scan backwards to find first entry for this neighbor
            let mut idx = start_idx;
            while idx > 0 && self.entries[idx - 1].neighbor == neighbor {
                idx -= 1;
            }
            // Collect all entries for this neighbor
            while idx < self.entries.len() && self.entries[idx].neighbor == neighbor {
                result.push(self.entries[idx]);
                idx += 1;
            }
        }
        result
    }

    /// Returns an iterator over all entries.
    pub fn iter(&self) -> impl Iterator<Item = &AdjEntry> {
        self.entries.iter()
    }

    /// Calculates the encoded size of this segment.
    pub fn encoded_size(&self) -> usize {
        ADJ_SEGMENT_HEADER_LEN + self.entries.len() * ADJ_ENTRY_LEN
    }

    /// Encodes the segment into bytes.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_size());
        buf.extend_from_slice(&self.header.encode());
        for entry in &self.entries {
            buf.extend_from_slice(&entry.encode());
        }
        buf
    }

    /// Decodes a segment from bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < ADJ_SEGMENT_HEADER_LEN {
            return Err(SombraError::Corruption("adj segment too short"));
        }

        let header = AdjSegmentHeader::decode(&bytes[..ADJ_SEGMENT_HEADER_LEN])?;
        let entry_count = header.entry_count as usize;
        let expected_len = ADJ_SEGMENT_HEADER_LEN + entry_count * ADJ_ENTRY_LEN;

        if bytes.len() < expected_len {
            return Err(SombraError::Corruption("adj segment entries truncated"));
        }

        let mut entries = Vec::with_capacity(entry_count);
        let mut offset = ADJ_SEGMENT_HEADER_LEN;
        for _ in 0..entry_count {
            entries.push(AdjEntry::decode(&bytes[offset..offset + ADJ_ENTRY_LEN])?);
            offset += ADJ_ENTRY_LEN;
        }

        Ok(Self { header, entries })
    }
}

/// Calculates how many entries can fit in a page after the header.
///
/// Given a page size (e.g., 8192), returns the maximum number of AdjEntry
/// that can fit alongside the header.
pub const fn max_entries_per_page(page_size: usize) -> usize {
    if page_size <= ADJ_SEGMENT_HEADER_LEN {
        return 0;
    }
    (page_size - ADJ_SEGMENT_HEADER_LEN) / ADJ_ENTRY_LEN
}

#[cfg(test)]
mod segment_tests {
    use super::*;

    #[test]
    fn adj_entry_roundtrip() {
        let entry = AdjEntry::new(NodeId(42), EdgeId(123), 1000);
        let encoded = entry.encode();
        let decoded = AdjEntry::decode(&encoded).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn adj_entry_visibility() {
        let entry = AdjEntry::with_visibility(NodeId(1), EdgeId(1), 100, 200);
        
        // Before xmin - not visible
        assert!(!entry.visible_at(50));
        
        // At xmin - visible
        assert!(entry.visible_at(100));
        
        // Between xmin and xmax - visible
        assert!(entry.visible_at(150));
        
        // At xmax - not visible (deleted)
        assert!(!entry.visible_at(200));
        
        // After xmax - not visible
        assert!(!entry.visible_at(300));
        
        // Entry with xmax=0 (not deleted) is always visible after xmin
        let active_entry = AdjEntry::new(NodeId(1), EdgeId(1), 100);
        assert!(active_entry.visible_at(100));
        assert!(active_entry.visible_at(u64::MAX));
    }

    #[test]
    fn adj_segment_header_roundtrip() {
        let header = AdjSegmentHeader {
            owner: NodeId(100),
            dir: Dir::Out,
            type_id: TypeId(5),
            xmin: 1000,
            xmax: 2000,
            prev_version: SegmentPtr(50),
            next_extent: SegmentPtr(60),
            entry_count: 42,
        };
        let encoded = header.encode();
        let decoded = AdjSegmentHeader::decode(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn adj_segment_insert_sorted() {
        let mut seg = AdjSegment::new(NodeId(1), Dir::Out, TypeId(1), 100);

        seg.insert(AdjEntry::new(NodeId(30), EdgeId(3), 100));
        seg.insert(AdjEntry::new(NodeId(10), EdgeId(1), 100));
        seg.insert(AdjEntry::new(NodeId(20), EdgeId(2), 100));

        assert_eq!(seg.entries[0].neighbor, NodeId(10));
        assert_eq!(seg.entries[1].neighbor, NodeId(20));
        assert_eq!(seg.entries[2].neighbor, NodeId(30));
    }

    #[test]
    fn adj_segment_remove() {
        let mut seg = AdjSegment::new(NodeId(1), Dir::Out, TypeId(1), 100);

        seg.insert(AdjEntry::new(NodeId(10), EdgeId(1), 100));
        seg.insert(AdjEntry::new(NodeId(20), EdgeId(2), 100));
        seg.insert(AdjEntry::new(NodeId(30), EdgeId(3), 100));

        assert!(seg.remove(NodeId(20), EdgeId(2)));
        assert_eq!(seg.len(), 2);
        assert_eq!(seg.entries[0].neighbor, NodeId(10));
        assert_eq!(seg.entries[1].neighbor, NodeId(30));

        // Remove non-existent
        assert!(!seg.remove(NodeId(20), EdgeId(2)));
    }

    #[test]
    fn adj_segment_roundtrip() {
        let mut seg = AdjSegment::new(NodeId(1), Dir::In, TypeId(5), 100);
        seg.insert(AdjEntry::new(NodeId(10), EdgeId(1), 100));
        seg.insert(AdjEntry::new(NodeId(20), EdgeId(2), 100));

        let encoded = seg.encode();
        let decoded = AdjSegment::decode(&encoded).unwrap();

        assert_eq!(seg.header, decoded.header);
        assert_eq!(seg.entries, decoded.entries);
    }

    #[test]
    fn max_entries_calculation() {
        // 8KB page
        let max = max_entries_per_page(8192);
        // (8192 - 50) / 32 = 254
        assert_eq!(max, 254);
    }

    #[test]
    fn visibility_check() {
        let header = AdjSegmentHeader {
            owner: NodeId(1),
            dir: Dir::Out,
            type_id: TypeId(1),
            xmin: 100,
            xmax: 200,
            prev_version: SegmentPtr::null(),
            next_extent: SegmentPtr::null(),
            entry_count: 0,
        };

        // Before creation
        assert!(!header.visible_at(50));
        // At creation
        assert!(header.visible_at(100));
        // During lifetime
        assert!(header.visible_at(150));
        // At supersession
        assert!(!header.visible_at(200));
        // After supersession
        assert!(!header.visible_at(250));

        // Active version (xmax = 0)
        let active_header = AdjSegmentHeader {
            xmax: 0,
            ..header
        };
        assert!(active_header.visible_at(100));
        assert!(active_header.visible_at(1000));
    }
}
