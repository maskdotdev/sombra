//! Core types for Index-Free Adjacency storage.
//!
//! Defines the fundamental structures: `SegmentPtr`, `NodeAdjHeader`, `TypeBucket`,
//! and `OverflowBlock` that form the per-node type map infrastructure.

use crate::types::{PageId, Result, SombraError, TypeId};
use std::convert::TryInto;

/// Number of inline buckets in a NodeAdjHeader.
///
/// Chosen to fit typical nodes (1-4 edge types) within a cache line while
/// leaving room for the overflow pointer in the last slot.
pub const INLINE_BUCKET_COUNT: usize = 6;

/// Sentinel TypeId indicating the bucket points to an overflow chain.
pub const OVERFLOW_TAG: TypeId = TypeId(u32::MAX);

/// Size of encoded SegmentPtr in bytes.
pub const SEGMENT_PTR_LEN: usize = 8;

/// Size of encoded TypeBucket in bytes (TypeId + SegmentPtr).
pub const TYPE_BUCKET_LEN: usize = 4 + SEGMENT_PTR_LEN; // 12 bytes

/// Size of encoded NodeAdjHeader in bytes.
/// K buckets * 12 bytes each = 72 bytes for K=6
pub const NODE_ADJ_HEADER_LEN: usize = INLINE_BUCKET_COUNT * TYPE_BUCKET_LEN;

/// Pointer to an adjacency segment.
///
/// Encodes a PageId where the segment starts. A zero value indicates null/empty.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Default)]
pub struct SegmentPtr(pub u64);

impl SegmentPtr {
    /// Returns a null pointer indicating no segment.
    #[inline]
    pub const fn null() -> Self {
        Self(0)
    }

    /// Returns true if this pointer is null (no segment).
    #[inline]
    pub const fn is_null(self) -> bool {
        self.0 == 0
    }

    /// Creates a SegmentPtr from a PageId.
    #[inline]
    pub const fn from_page(page: PageId) -> Self {
        Self(page.0)
    }

    /// Returns the PageId this pointer references.
    #[inline]
    pub const fn to_page(self) -> PageId {
        PageId(self.0)
    }

    /// Encodes the pointer as big-endian bytes.
    #[inline]
    pub fn to_bytes(self) -> [u8; SEGMENT_PTR_LEN] {
        self.0.to_be_bytes()
    }

    /// Decodes a pointer from big-endian bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < SEGMENT_PTR_LEN {
            return Err(SombraError::Corruption("segment pointer truncated"));
        }
        let arr: [u8; SEGMENT_PTR_LEN] = bytes[..SEGMENT_PTR_LEN].try_into().unwrap();
        Ok(Self(u64::from_be_bytes(arr)))
    }
}

/// A single bucket mapping TypeId to segment head pointer.
///
/// Used both in inline buckets and overflow blocks.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TypeBucket {
    /// Edge type ID, or OVERFLOW_TAG for overflow pointer.
    pub type_id: TypeId,
    /// Head of the adjacency segment chain for this type.
    pub head: SegmentPtr,
}

impl Default for TypeBucket {
    fn default() -> Self {
        Self::empty()
    }
}

impl TypeBucket {
    /// Creates a new empty bucket.
    #[inline]
    pub const fn empty() -> Self {
        Self {
            type_id: TypeId(0),
            head: SegmentPtr::null(),
        }
    }

    /// Creates a bucket for a specific type.
    #[inline]
    pub const fn new(type_id: TypeId, head: SegmentPtr) -> Self {
        Self { type_id, head }
    }

    /// Creates an overflow bucket pointing to an overflow block.
    #[inline]
    pub const fn overflow(overflow_ptr: SegmentPtr) -> Self {
        Self {
            type_id: OVERFLOW_TAG,
            head: overflow_ptr,
        }
    }

    /// Returns true if this bucket is empty (unused slot).
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.type_id.0 == 0 && self.head.is_null()
    }

    /// Returns true if this bucket is an overflow pointer.
    #[inline]
    pub fn is_overflow(&self) -> bool {
        self.type_id == OVERFLOW_TAG
    }

    /// Encodes the bucket as big-endian bytes.
    pub fn encode(&self) -> [u8; TYPE_BUCKET_LEN] {
        let mut buf = [0u8; TYPE_BUCKET_LEN];
        buf[0..4].copy_from_slice(&self.type_id.0.to_be_bytes());
        buf[4..12].copy_from_slice(&self.head.to_bytes());
        buf
    }

    /// Decodes a bucket from big-endian bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < TYPE_BUCKET_LEN {
            return Err(SombraError::Corruption("type bucket truncated"));
        }
        let type_id = TypeId(u32::from_be_bytes(bytes[0..4].try_into().unwrap()));
        let head = SegmentPtr::from_bytes(&bytes[4..12])?;
        Ok(Self { type_id, head })
    }
}

/// Per-node adjacency header with inline type map buckets.
///
/// Each node has one header per direction (OUT/IN) containing K inline buckets.
/// For nodes with ≤K-1 distinct edge types, all mappings fit inline.
/// For nodes with >K-1 types, the last bucket points to an overflow chain.
///
/// # Layout
///
/// ```text
/// +------------------+------------------+-----+------------------+
/// | Bucket 0         | Bucket 1         | ... | Bucket K-1       |
/// | type | head_ptr  | type | head_ptr  |     | type | head_ptr  |
/// +------------------+------------------+-----+------------------+
///                                             ^
///                                             |
///                     If type == OVERFLOW_TAG, head_ptr -> OverflowBlock
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeAdjHeader {
    /// Inline buckets for type -> segment mappings.
    pub buckets: [TypeBucket; INLINE_BUCKET_COUNT],
}

impl Default for NodeAdjHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeAdjHeader {
    /// Creates a new empty header with all buckets unused.
    pub const fn new() -> Self {
        Self {
            buckets: [TypeBucket::empty(); INLINE_BUCKET_COUNT],
        }
    }

    /// Returns the number of active (non-empty) type mappings in inline slots.
    /// Only counts first K-1 buckets (last is reserved for overflow).
    pub fn active_count(&self) -> usize {
        self.buckets[..INLINE_BUCKET_COUNT - 1]
            .iter()
            .filter(|b| !b.is_empty())
            .count()
    }

    /// Returns true if an overflow block is present.
    pub fn has_overflow(&self) -> bool {
        self.buckets
            .last()
            .map(|b| b.is_overflow())
            .unwrap_or(false)
    }

    /// Gets the overflow pointer if present.
    pub fn overflow_ptr(&self) -> Option<SegmentPtr> {
        self.buckets.last().and_then(|b| {
            if b.is_overflow() {
                Some(b.head)
            } else {
                None
            }
        })
    }

    /// Looks up the segment head for a given type ID in inline buckets only.
    ///
    /// Returns `None` if the type is not found inline. Caller must check
    /// overflow if this returns `None` and `has_overflow()` is true.
    pub fn lookup_inline(&self, type_id: TypeId) -> Option<SegmentPtr> {
        // Only search first K-1 buckets (last is reserved for overflow)
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            let bucket = &self.buckets[i];
            if bucket.is_empty() {
                continue;
            }
            if bucket.type_id == type_id {
                return Some(bucket.head);
            }
        }
        None
    }

    /// Inserts or updates a type mapping in inline buckets.
    ///
    /// Returns `Ok(())` if successful, or `Err` if no space and overflow needed.
    /// Note: The last bucket slot (index K-1) is reserved for overflow pointer,
    /// so only K-1 types can be stored inline.
    pub fn insert_inline(&mut self, type_id: TypeId, head: SegmentPtr) -> Result<()> {
        // First, check if type already exists (in slots 0..K-1, not including last)
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            let bucket = &mut self.buckets[i];
            if bucket.type_id == type_id {
                bucket.head = head;
                return Ok(());
            }
        }

        // Find first empty slot (only in slots 0..K-1, reserving last for overflow)
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            if self.buckets[i].is_empty() {
                self.buckets[i] = TypeBucket::new(type_id, head);
                return Ok(());
            }
        }

        // No space - need overflow
        Err(SombraError::Invalid("inline buckets full, need overflow"))
    }

    /// Removes a type mapping from inline buckets.
    ///
    /// Returns the old head pointer if found.
    pub fn remove_inline(&mut self, type_id: TypeId) -> Option<SegmentPtr> {
        // Only search first K-1 buckets
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            let bucket = &mut self.buckets[i];
            if bucket.type_id == type_id {
                let old_head = bucket.head;
                *bucket = TypeBucket::empty();
                return Some(old_head);
            }
        }
        None
    }

    /// Sets the overflow pointer in the last bucket slot.
    pub fn set_overflow(&mut self, overflow_ptr: SegmentPtr) {
        self.buckets[INLINE_BUCKET_COUNT - 1] = TypeBucket::overflow(overflow_ptr);
    }

    /// Clears the overflow pointer.
    pub fn clear_overflow(&mut self) {
        if self.has_overflow() {
            self.buckets[INLINE_BUCKET_COUNT - 1] = TypeBucket::empty();
        }
    }

    /// Encodes the header as big-endian bytes.
    pub fn encode(&self) -> [u8; NODE_ADJ_HEADER_LEN] {
        let mut buf = [0u8; NODE_ADJ_HEADER_LEN];
        for (i, bucket) in self.buckets.iter().enumerate() {
            let start = i * TYPE_BUCKET_LEN;
            buf[start..start + TYPE_BUCKET_LEN].copy_from_slice(&bucket.encode());
        }
        buf
    }

    /// Decodes a header from big-endian bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < NODE_ADJ_HEADER_LEN {
            return Err(SombraError::Corruption("node adj header truncated"));
        }
        let mut buckets = [TypeBucket::empty(); INLINE_BUCKET_COUNT];
        for i in 0..INLINE_BUCKET_COUNT {
            let start = i * TYPE_BUCKET_LEN;
            buckets[i] = TypeBucket::decode(&bytes[start..start + TYPE_BUCKET_LEN])?;
        }
        Ok(Self { buckets })
    }

    /// Returns an iterator over all inline type mappings (excluding overflow slot).
    pub fn iter_types(&self) -> impl Iterator<Item = (TypeId, SegmentPtr)> + '_ {
        // Only iterate first K-1 buckets
        self.buckets[..INLINE_BUCKET_COUNT - 1].iter().filter_map(|b| {
            if b.is_empty() {
                None
            } else {
                Some((b.type_id, b.head))
            }
        })
    }
}

/// Maximum entries per overflow block.
pub const OVERFLOW_BLOCK_ENTRIES: usize = 16;

/// Size of encoded OverflowBlock in bytes.
/// next_ptr (8) + entry_count (2) + entries (16 * 12) = 202 bytes
pub const OVERFLOW_BLOCK_LEN: usize = 8 + 2 + (OVERFLOW_BLOCK_ENTRIES * TYPE_BUCKET_LEN);

/// Overflow block for nodes with many edge types.
///
/// When a node has more than K-1 distinct edge types, the excess mappings
/// are stored in a linked list of overflow blocks. Entries within each block
/// are sorted by TypeId for efficient binary search.
///
/// # Layout
///
/// ```text
/// OverflowBlock {
///     next: SegmentPtr,              // 8 bytes - next block or null
///     entry_count: u16,              // 2 bytes - number of valid entries
///     entries: [TypeBucket; M],      // M * 12 bytes - sorted by TypeId
/// }
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OverflowBlock {
    /// Pointer to next overflow block, or null if last.
    pub next: SegmentPtr,
    /// Number of valid entries in this block.
    pub entry_count: u16,
    /// Type buckets sorted by TypeId.
    pub entries: [TypeBucket; OVERFLOW_BLOCK_ENTRIES],
}

impl Default for OverflowBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl OverflowBlock {
    /// Creates a new empty overflow block.
    pub const fn new() -> Self {
        Self {
            next: SegmentPtr::null(),
            entry_count: 0,
            entries: [TypeBucket::empty(); OVERFLOW_BLOCK_ENTRIES],
        }
    }

    /// Returns true if the block has no entries.
    pub fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Returns true if the block is full.
    pub fn is_full(&self) -> bool {
        self.entry_count as usize >= OVERFLOW_BLOCK_ENTRIES
    }

    /// Looks up a type in this block using binary search.
    pub fn lookup(&self, type_id: TypeId) -> Option<SegmentPtr> {
        let count = self.entry_count as usize;
        if count == 0 {
            return None;
        }

        // Binary search over sorted entries
        let entries = &self.entries[..count];
        match entries.binary_search_by_key(&type_id.0, |e| e.type_id.0) {
            Ok(idx) => Some(entries[idx].head),
            Err(_) => None,
        }
    }

    /// Inserts or updates a type mapping, maintaining sorted order.
    ///
    /// Returns `Err` if the block is full and the type doesn't already exist.
    pub fn insert(&mut self, type_id: TypeId, head: SegmentPtr) -> Result<()> {
        let count = self.entry_count as usize;

        // Check if type already exists
        if count > 0 {
            let entries = &mut self.entries[..count];
            match entries.binary_search_by_key(&type_id.0, |e| e.type_id.0) {
                Ok(idx) => {
                    // Update existing
                    entries[idx].head = head;
                    return Ok(());
                }
                Err(insert_pos) => {
                    if count >= OVERFLOW_BLOCK_ENTRIES {
                        return Err(SombraError::Invalid("overflow block full"));
                    }
                    // Shift entries to make room
                    for i in (insert_pos..count).rev() {
                        self.entries[i + 1] = self.entries[i];
                    }
                    self.entries[insert_pos] = TypeBucket::new(type_id, head);
                    self.entry_count += 1;
                    return Ok(());
                }
            }
        }

        // Empty block - just insert at start
        if count >= OVERFLOW_BLOCK_ENTRIES {
            return Err(SombraError::Invalid("overflow block full"));
        }
        self.entries[0] = TypeBucket::new(type_id, head);
        self.entry_count = 1;
        Ok(())
    }

    /// Removes a type mapping.
    ///
    /// Returns the old head pointer if found.
    pub fn remove(&mut self, type_id: TypeId) -> Option<SegmentPtr> {
        let count = self.entry_count as usize;
        if count == 0 {
            return None;
        }

        let entries = &self.entries[..count];
        match entries.binary_search_by_key(&type_id.0, |e| e.type_id.0) {
            Ok(idx) => {
                let old_head = self.entries[idx].head;
                // Shift entries down
                for i in idx..count - 1 {
                    self.entries[i] = self.entries[i + 1];
                }
                self.entries[count - 1] = TypeBucket::empty();
                self.entry_count -= 1;
                Some(old_head)
            }
            Err(_) => None,
        }
    }

    /// Encodes the block as big-endian bytes.
    pub fn encode(&self) -> [u8; OVERFLOW_BLOCK_LEN] {
        let mut buf = [0u8; OVERFLOW_BLOCK_LEN];
        buf[0..8].copy_from_slice(&self.next.to_bytes());
        buf[8..10].copy_from_slice(&self.entry_count.to_be_bytes());
        for i in 0..OVERFLOW_BLOCK_ENTRIES {
            let start = 10 + i * TYPE_BUCKET_LEN;
            buf[start..start + TYPE_BUCKET_LEN].copy_from_slice(&self.entries[i].encode());
        }
        buf
    }

    /// Decodes a block from big-endian bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < OVERFLOW_BLOCK_LEN {
            return Err(SombraError::Corruption("overflow block truncated"));
        }
        let next = SegmentPtr::from_bytes(&bytes[0..8])?;
        let entry_count = u16::from_be_bytes(bytes[8..10].try_into().unwrap());
        if entry_count as usize > OVERFLOW_BLOCK_ENTRIES {
            return Err(SombraError::Corruption("overflow block entry count too high"));
        }
        let mut entries = [TypeBucket::empty(); OVERFLOW_BLOCK_ENTRIES];
        for i in 0..OVERFLOW_BLOCK_ENTRIES {
            let start = 10 + i * TYPE_BUCKET_LEN;
            entries[i] = TypeBucket::decode(&bytes[start..start + TYPE_BUCKET_LEN])?;
        }
        Ok(Self {
            next,
            entry_count,
            entries,
        })
    }

    /// Returns an iterator over valid entries.
    pub fn iter(&self) -> impl Iterator<Item = (TypeId, SegmentPtr)> + '_ {
        self.entries[..self.entry_count as usize]
            .iter()
            .map(|e| (e.type_id, e.head))
    }
}
