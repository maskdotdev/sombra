//! Core types for Index-Free Adjacency storage.
//!
//! Defines the fundamental structures: `SegmentPtr`, `NodeAdjHeader`, `TypeBucket`,
//! and `OverflowBlock` that form the per-node type map infrastructure.
//!
//! # Inline Segment Storage
//!
//! For low-degree nodes, adjacency entries can be stored directly in the `NodeAdjHeader`
//! instead of requiring a separate segment page. This optimization:
//! - Reduces low-degree node lookup from 2 page reads to 1
//! - Eliminates segment allocation for nodes with 1-3 edges per type
//! - Saves ~60% space for single-edge nodes
//!
//! The inline storage uses a compact 16-byte entry format (vs 32 bytes for full MVCC entries)
//! since inline entries inherit visibility from the header's creation time.

use crate::types::{EdgeId, NodeId, PageId, Result, SombraError, TypeId};
use smallvec::SmallVec;
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

// ============================================================================
// Inline Segment Storage Constants
// ============================================================================

/// Marker bit in TypeId indicating inline storage.
///
/// When bit 31 is set, the bucket contains inline adjacency entries
/// instead of a SegmentPtr to an external segment.
pub const INLINE_STORAGE_FLAG: u32 = 1 << 31;

/// Maximum inline entries per type for single-type nodes.
///
/// Single-type nodes have 5 spare buckets (60 bytes) which can hold
/// 3 inline entries (48 bytes) plus a count byte.
pub const MAX_INLINE_ENTRIES_SINGLE: usize = 3;

/// Maximum inline entries per type for multi-type nodes.
///
/// Multi-type nodes have fewer spare buckets, so we limit to 2 entries
/// per type to ensure all types can have inline storage.
pub const MAX_INLINE_ENTRIES_MULTI: usize = 2;

/// Size of compact inline entry in bytes (no MVCC fields).
pub const INLINE_ADJ_ENTRY_LEN: usize = 16;

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

// ============================================================================
// Inline Adjacency Entry
// ============================================================================

/// Compact inline adjacency entry without MVCC fields.
///
/// Used for inline storage in `TypeBucket` slots. This 16-byte format
/// omits `xmin`/`xmax` fields since inline entries inherit visibility
/// from the header's creation time.
///
/// # Layout
///
/// ```text
/// +------------+----------+
/// | neighbor   | edge     |
/// | 8 bytes    | 8 bytes  |
/// +------------+----------+
/// ```
///
/// When an entry is deleted or updated, it must be promoted to an external
/// segment with full MVCC tracking.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub struct InlineAdjEntry {
    /// The neighbor node ID.
    pub neighbor: NodeId,
    /// The edge ID connecting to the neighbor.
    pub edge: EdgeId,
}

impl InlineAdjEntry {
    /// Creates a new inline entry.
    #[inline]
    pub const fn new(neighbor: NodeId, edge: EdgeId) -> Self {
        Self { neighbor, edge }
    }

    /// Encodes the entry as big-endian bytes.
    #[inline]
    pub fn encode(&self) -> [u8; INLINE_ADJ_ENTRY_LEN] {
        let mut buf = [0u8; INLINE_ADJ_ENTRY_LEN];
        buf[0..8].copy_from_slice(&self.neighbor.0.to_be_bytes());
        buf[8..16].copy_from_slice(&self.edge.0.to_be_bytes());
        buf
    }

    /// Decodes an entry from big-endian bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < INLINE_ADJ_ENTRY_LEN {
            return Err(SombraError::Corruption("inline adj entry truncated"));
        }
        let neighbor = NodeId(u64::from_be_bytes(bytes[0..8].try_into().unwrap()));
        let edge = EdgeId(u64::from_be_bytes(bytes[8..16].try_into().unwrap()));
        Ok(Self { neighbor, edge })
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

    // ========================================================================
    // Inline Storage Methods
    // ========================================================================

    /// Returns true if this bucket uses inline storage.
    ///
    /// When bit 31 of the type_id is set, the bucket stores adjacency entries
    /// inline rather than pointing to an external segment. The `head` field
    /// is repurposed to store the entry count in its high byte.
    #[inline]
    pub fn is_inline(&self) -> bool {
        (self.type_id.0 & INLINE_STORAGE_FLAG) != 0
    }

    /// Gets the actual type ID without the inline storage flag.
    #[inline]
    pub fn actual_type_id(&self) -> TypeId {
        TypeId(self.type_id.0 & !INLINE_STORAGE_FLAG)
    }

    /// Gets the inline entry count from the head field.
    ///
    /// Only valid when `is_inline()` returns true. The count is stored
    /// in the high byte of the head field.
    #[inline]
    pub fn inline_count(&self) -> u8 {
        debug_assert!(self.is_inline());
        (self.head.0 >> 56) as u8
    }

    /// Creates an inline bucket with the given type and entry count.
    ///
    /// The type_id gets the INLINE_STORAGE_FLAG set, and the count
    /// is stored in the high byte of the head field.
    #[inline]
    pub fn new_inline(type_id: TypeId, count: u8) -> Self {
        Self {
            type_id: TypeId(type_id.0 | INLINE_STORAGE_FLAG),
            head: SegmentPtr((count as u64) << 56),
        }
    }

    /// Sets this bucket to use inline storage with the given count.
    #[inline]
    pub fn set_inline(&mut self, type_id: TypeId, count: u8) {
        self.type_id = TypeId(type_id.0 | INLINE_STORAGE_FLAG);
        self.head = SegmentPtr((count as u64) << 56);
    }

    /// Sets this bucket to use external (non-inline) storage.
    #[inline]
    pub fn set_external(&mut self, type_id: TypeId, head: SegmentPtr) {
        self.type_id = TypeId(type_id.0 & !INLINE_STORAGE_FLAG);
        self.head = head;
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
/// # Layout (Standard Format)
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
///
/// # Layout (Inline Storage Format)
///
/// When a bucket has the INLINE_STORAGE_FLAG set, it uses inline entry storage:
///
/// ```text
/// +----------------------+------------------+------------------+
/// | Bucket 0 (inline)    | Entry data...    | Entry data...    |
/// | type|FLAG | count    | neighbor | edge  | neighbor | edge  |
/// +----------------------+------------------+------------------+
/// ```
///
/// Inline entries are stored in the raw bytes following the bucket header.
/// The `count` field (stored in high byte of `head`) indicates how many
/// 16-byte entries follow.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NodeAdjHeader {
    /// Inline buckets for type -> segment mappings.
    /// When a bucket is marked inline, subsequent slots may contain entry data.
    pub buckets: [TypeBucket; INLINE_BUCKET_COUNT],
    /// Raw inline entry storage for inline types.
    /// Maps bucket index -> inline entries for that type.
    pub inline_entries: SmallVec<[(usize, SmallVec<[InlineAdjEntry; 3]>); 2]>,
}

impl Default for NodeAdjHeader {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeAdjHeader {
    /// Creates a new empty header with all buckets unused.
    pub fn new() -> Self {
        Self {
            buckets: [TypeBucket::empty(); INLINE_BUCKET_COUNT],
            inline_entries: SmallVec::new(),
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

    /// Calculates total bytes needed to encode all non-empty buckets and their inline entries.
    ///
    /// This is used to check if there's space before inserting a new inline entry.
    fn total_inline_bytes(&self) -> usize {
        let mut bytes = 0;
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            let bucket = &self.buckets[i];
            if bucket.is_empty() {
                continue;
            }
            bytes += TYPE_BUCKET_LEN;
            if bucket.is_inline() {
                // Find inline entries for this bucket
                for (idx, entries) in &self.inline_entries {
                    if *idx == i {
                        bytes += entries.len() * INLINE_ADJ_ENTRY_LEN;
                        break;
                    }
                }
            }
        }
        bytes
    }

    /// Maximum bytes available for buckets and inline entries before overflow slot.
    const fn max_inline_bytes() -> usize {
        (INLINE_BUCKET_COUNT - 1) * TYPE_BUCKET_LEN // 60 bytes
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
    /// Returns `None` if the type is not found inline or uses inline storage.
    /// Caller must check overflow if this returns `None` and `has_overflow()` is true.
    /// For inline storage types, use `lookup_inline_entries()` instead.
    pub fn lookup_inline(&self, type_id: TypeId) -> Option<SegmentPtr> {
        // Only search first K-1 buckets (last is reserved for overflow)
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            let bucket = &self.buckets[i];
            if bucket.is_empty() {
                continue;
            }
            // Check actual type (with flag masked out)
            if bucket.actual_type_id() == type_id {
                // Return None if this is inline storage (caller should use lookup_inline_entries)
                if bucket.is_inline() {
                    return None;
                }
                return Some(bucket.head);
            }
        }
        None
    }

    /// Looks up inline entries for a given type ID.
    ///
    /// Returns `Some` with the entries if the type uses inline storage,
    /// `None` if the type is not found or uses external storage.
    pub fn lookup_inline_entries(&self, type_id: TypeId) -> Option<&[InlineAdjEntry]> {
        // Find the bucket for this type
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            let bucket = &self.buckets[i];
            if bucket.is_empty() {
                continue;
            }
            if bucket.actual_type_id() == type_id && bucket.is_inline() {
                // Look up entries in inline_entries by bucket index
                for (idx, entries) in &self.inline_entries {
                    if *idx == i {
                        return Some(entries.as_slice());
                    }
                }
                // Bucket marked inline but no entries found - shouldn't happen
                return Some(&[]);
            }
        }
        None
    }

    /// Checks if a type uses inline storage.
    #[allow(dead_code)]
    pub fn is_type_inline(&self, type_id: TypeId) -> bool {
        for bucket in &self.buckets[..INLINE_BUCKET_COUNT - 1] {
            if bucket.actual_type_id() == type_id {
                return bucket.is_inline();
            }
        }
        false
    }

    /// Inserts or updates a type mapping in inline buckets (external storage).
    ///
    /// Returns `Ok(())` if successful, or `Err` if no space and overflow needed.
    /// Note: The last bucket slot (index K-1) is reserved for overflow pointer,
    /// so only K-1 types can be stored inline.
    ///
    /// This method is for external segment storage. For inline entry storage,
    /// use `insert_inline_entry()` instead.
    pub fn insert_inline(&mut self, type_id: TypeId, head: SegmentPtr) -> Result<()> {
        // First, check if type already exists (in slots 0..K-1, not including last)
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            let bucket = &mut self.buckets[i];
            if bucket.actual_type_id() == type_id {
                // Clear any inline entries for this type
                self.inline_entries.retain(|(idx, _)| *idx != i);
                bucket.set_external(type_id, head);
                return Ok(());
            }
        }

        // Check if there's space for a new bucket (12 bytes)
        let current_bytes = self.total_inline_bytes();
        if current_bytes + TYPE_BUCKET_LEN > Self::max_inline_bytes() {
            return Err(SombraError::Invalid(
                "not enough space for external type bucket, need overflow",
            ));
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

    /// Inserts an inline entry for a type.
    ///
    /// This creates or updates the type bucket to use inline storage and adds
    /// the entry to the inline entries list.
    ///
    /// Returns `Ok(())` if successful, `Err` if the type needs external storage
    /// (too many entries, bucket slots full, or total size exceeds available space).
    pub fn insert_inline_entry(&mut self, type_id: TypeId, entry: InlineAdjEntry) -> Result<()> {
        let active = self.active_count();
        let max_entries = if active <= 1 {
            MAX_INLINE_ENTRIES_SINGLE
        } else {
            MAX_INLINE_ENTRIES_MULTI
        };

        // Calculate current space usage upfront
        let current_bytes = self.total_inline_bytes();

        // First pass: find if type already exists and get its bucket index
        let mut existing_bucket_idx: Option<usize> = None;
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            if self.buckets[i].actual_type_id() == type_id && !self.buckets[i].is_empty() {
                existing_bucket_idx = Some(i);
                break;
            }
        }

        if let Some(bucket_idx) = existing_bucket_idx {
            // Type exists - try to add entry to existing inline entries
            for (idx, entries) in &mut self.inline_entries {
                if *idx == bucket_idx {
                    if entries.len() >= max_entries {
                        return Err(SombraError::Invalid("inline entries full, need external"));
                    }
                    // Check if adding this entry would exceed available space
                    if current_bytes + INLINE_ADJ_ENTRY_LEN > Self::max_inline_bytes() {
                        return Err(SombraError::Invalid(
                            "not enough space for inline entry, need external",
                        ));
                    }
                    entries.push(entry);
                    self.buckets[bucket_idx].set_inline(type_id, entries.len() as u8);
                    return Ok(());
                }
            }
            // Bucket exists but no entries yet - this is first inline entry
            // Check space for the new entry
            if current_bytes + INLINE_ADJ_ENTRY_LEN > Self::max_inline_bytes() {
                return Err(SombraError::Invalid(
                    "not enough space for inline entry, need external",
                ));
            }
            let entries: SmallVec<[InlineAdjEntry; 3]> = smallvec::smallvec![entry];
            self.inline_entries.push((bucket_idx, entries));
            self.buckets[bucket_idx].set_inline(type_id, 1);
            return Ok(());
        }

        // Type doesn't exist - find empty slot
        // Check if there's space for a new bucket + entry
        let needed = TYPE_BUCKET_LEN + INLINE_ADJ_ENTRY_LEN;
        if current_bytes + needed > Self::max_inline_bytes() {
            return Err(SombraError::Invalid(
                "not enough space for new type bucket, need external",
            ));
        }

        for i in 0..INLINE_BUCKET_COUNT - 1 {
            if self.buckets[i].is_empty() {
                self.buckets[i] = TypeBucket::new_inline(type_id, 1);
                let entries: SmallVec<[InlineAdjEntry; 3]> = smallvec::smallvec![entry];
                self.inline_entries.push((i, entries));
                return Ok(());
            }
        }

        // No space - need overflow
        Err(SombraError::Invalid("inline buckets full, need overflow"))
    }

    /// Gets mutable access to inline entries for a type, if it exists.
    #[allow(dead_code)]
    pub fn get_inline_entries_mut(
        &mut self,
        type_id: TypeId,
    ) -> Option<&mut SmallVec<[InlineAdjEntry; 3]>> {
        // Find bucket index for this type
        let mut bucket_idx = None;
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            if self.buckets[i].actual_type_id() == type_id && self.buckets[i].is_inline() {
                bucket_idx = Some(i);
                break;
            }
        }

        let idx = bucket_idx?;
        for (i, entries) in &mut self.inline_entries {
            if *i == idx {
                return Some(entries);
            }
        }
        None
    }

    /// Promotes a type from inline storage to external segment storage.
    ///
    /// Returns the inline entries that were stored, which should be migrated
    /// to the new external segment.
    pub fn promote_to_external(
        &mut self,
        type_id: TypeId,
        head: SegmentPtr,
    ) -> Option<SmallVec<[InlineAdjEntry; 3]>> {
        // Find bucket for this type
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            if self.buckets[i].actual_type_id() == type_id {
                // Set to external storage
                self.buckets[i].set_external(type_id, head);

                // Remove and return inline entries
                let mut removed = None;
                self.inline_entries.retain(|(idx, entries)| {
                    if *idx == i {
                        removed = Some(entries.clone());
                        false
                    } else {
                        true
                    }
                });
                return removed;
            }
        }
        None
    }

    /// Removes a single inline entry from a type's inline storage.
    ///
    /// Returns `Some(true)` if the entry was found and removed,
    /// `Some(false)` if the entry wasn't found,
    /// `None` if the type doesn't exist or uses external storage.
    ///
    /// If this was the last entry, the bucket is cleared entirely.
    pub fn remove_inline_entry(
        &mut self,
        type_id: TypeId,
        neighbor: NodeId,
        edge: EdgeId,
    ) -> Option<bool> {
        // Find bucket index for this type
        let mut bucket_idx = None;
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            if self.buckets[i].actual_type_id() == type_id && self.buckets[i].is_inline() {
                bucket_idx = Some(i);
                break;
            }
        }

        let idx = bucket_idx?;

        // Find and remove the entry
        let mut removed = false;
        let mut remaining_count = 0;
        for (bucket_i, entries) in &mut self.inline_entries {
            if *bucket_i == idx {
                let original_len = entries.len();
                entries.retain(|e| !(e.neighbor == neighbor && e.edge == edge));
                removed = entries.len() < original_len;
                remaining_count = entries.len();
                break;
            }
        }

        if removed {
            if remaining_count == 0 {
                // Last entry removed - clear the bucket entirely
                self.buckets[idx] = TypeBucket::empty();
                self.inline_entries.retain(|(i, _)| *i != idx);
            } else {
                // Update count in bucket
                self.buckets[idx].set_inline(type_id, remaining_count as u8);
            }
        }

        Some(removed)
    }

    /// Removes a type mapping from inline buckets.
    ///
    /// Returns the old head pointer if found and the type used external storage.
    /// For inline storage types, this removes the entries and returns `None`.
    pub fn remove_inline(&mut self, type_id: TypeId) -> Option<SegmentPtr> {
        // Only search first K-1 buckets
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            let bucket = &mut self.buckets[i];
            if bucket.actual_type_id() == type_id {
                let was_inline = bucket.is_inline();
                let old_head = if was_inline { None } else { Some(bucket.head) };

                // Clear the bucket
                *bucket = TypeBucket::empty();

                // Remove inline entries if any
                self.inline_entries.retain(|(idx, _)| *idx != i);

                return old_head;
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
    ///
    /// The encoding format:
    /// - Bytes 0-59: Bucket data area (for buckets 0-4, leaving space for entries)
    /// - Bytes 60-71: Overflow slot (bucket 5, always at fixed position)
    ///
    /// For inline storage, entries are packed into the bucket data area after the
    /// inline bucket header. Each inline bucket uses 12 bytes for its header, and
    /// entries follow immediately.
    ///
    /// Example with 1 inline type having 2 entries:
    /// - Bytes 0-11: Bucket 0 header (TypeId|FLAG + count)
    /// - Bytes 12-27: Entry 0 (16 bytes)
    /// - Bytes 28-43: Entry 1 (16 bytes)
    /// - Bytes 44-59: Unused
    /// - Bytes 60-71: Overflow slot
    pub fn encode(&self) -> [u8; NODE_ADJ_HEADER_LEN] {
        let mut buf = [0u8; NODE_ADJ_HEADER_LEN];

        // Reserve overflow slot at the end (bucket 5)
        let overflow_start = (INLINE_BUCKET_COUNT - 1) * TYPE_BUCKET_LEN; // 60
        let overflow_bucket = &self.buckets[INLINE_BUCKET_COUNT - 1];
        buf[overflow_start..overflow_start + TYPE_BUCKET_LEN].copy_from_slice(&overflow_bucket.encode());

        // Encode buckets 0-4 and their inline entries
        let mut write_pos = 0;
        for i in 0..INLINE_BUCKET_COUNT - 1 {
            let bucket = &self.buckets[i];

            if bucket.is_empty() {
                continue;
            }

            // Encode bucket header
            if write_pos + TYPE_BUCKET_LEN > overflow_start {
                // No more space before overflow slot
                break;
            }
            buf[write_pos..write_pos + TYPE_BUCKET_LEN].copy_from_slice(&bucket.encode());
            write_pos += TYPE_BUCKET_LEN;

            // If inline, encode entries immediately after
            if bucket.is_inline() {
                for (idx, entries) in &self.inline_entries {
                    if *idx == i {
                        for entry in entries {
                            if write_pos + INLINE_ADJ_ENTRY_LEN <= overflow_start {
                                buf[write_pos..write_pos + INLINE_ADJ_ENTRY_LEN]
                                    .copy_from_slice(&entry.encode());
                                write_pos += INLINE_ADJ_ENTRY_LEN;
                            }
                        }
                        break;
                    }
                }
            }
        }

        buf
    }

    /// Decodes a header from big-endian bytes.
    ///
    /// This handles both legacy headers (all external storage) and new headers
    /// with inline entry storage.
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < NODE_ADJ_HEADER_LEN {
            return Err(SombraError::Corruption("node adj header truncated"));
        }

        let mut buckets = [TypeBucket::empty(); INLINE_BUCKET_COUNT];
        let mut inline_entries: SmallVec<[(usize, SmallVec<[InlineAdjEntry; 3]>); 2]> =
            SmallVec::new();

        // Decode overflow slot first (always at fixed position)
        let overflow_start = (INLINE_BUCKET_COUNT - 1) * TYPE_BUCKET_LEN;
        buckets[INLINE_BUCKET_COUNT - 1] = TypeBucket::decode(&bytes[overflow_start..overflow_start + TYPE_BUCKET_LEN])?;

        // Decode buckets 0-4 and their inline entries
        let mut read_pos = 0;
        let mut bucket_idx = 0;

        while read_pos + TYPE_BUCKET_LEN <= overflow_start && bucket_idx < INLINE_BUCKET_COUNT - 1 {
            let bucket = TypeBucket::decode(&bytes[read_pos..read_pos + TYPE_BUCKET_LEN])?;
            read_pos += TYPE_BUCKET_LEN;

            if bucket.is_empty() {
                // Empty bucket at this position - check if this is end of data
                // In legacy format, all buckets are at fixed positions
                // In new format, empty = end of bucket sequence
                // To distinguish, check if remaining bytes are all zero
                let remaining = &bytes[read_pos - TYPE_BUCKET_LEN..overflow_start];
                let is_legacy = remaining.chunks(TYPE_BUCKET_LEN).enumerate().any(|(offset, chunk)| {
                    let pos = (bucket_idx + offset) * TYPE_BUCKET_LEN;
                    pos < overflow_start && TypeBucket::decode(chunk).map(|b| !b.is_empty()).unwrap_or(false)
                });

                if is_legacy {
                    // Legacy format - decode from fixed positions
                    buckets[bucket_idx] = bucket;
                    for j in bucket_idx + 1..INLINE_BUCKET_COUNT - 1 {
                        let pos = j * TYPE_BUCKET_LEN;
                        buckets[j] = TypeBucket::decode(&bytes[pos..pos + TYPE_BUCKET_LEN])?;
                    }
                    return Ok(Self { buckets, inline_entries });
                } else {
                    // New format - no more buckets
                    break;
                }
            }

            buckets[bucket_idx] = bucket;

            // If inline, decode entries immediately after
            if bucket.is_inline() {
                let count = bucket.inline_count() as usize;
                if count > MAX_INLINE_ENTRIES_SINGLE {
                    return Err(SombraError::Corruption("inline entry count too high"));
                }

                let mut entries: SmallVec<[InlineAdjEntry; 3]> = SmallVec::new();
                for _ in 0..count {
                    if read_pos + INLINE_ADJ_ENTRY_LEN <= overflow_start {
                        let entry = InlineAdjEntry::decode(&bytes[read_pos..read_pos + INLINE_ADJ_ENTRY_LEN])?;
                        entries.push(entry);
                        read_pos += INLINE_ADJ_ENTRY_LEN;
                    } else {
                        return Err(SombraError::Corruption("inline entries truncated"));
                    }
                }

                if !entries.is_empty() {
                    inline_entries.push((bucket_idx, entries));
                }
            }

            bucket_idx += 1;
        }

        Ok(Self {
            buckets,
            inline_entries,
        })
    }

    /// Returns an iterator over all external type mappings (excluding overflow slot and inline types).
    ///
    /// This only returns types that use external segment storage.
    /// For inline storage types, use `iter_inline_types()`.
    pub fn iter_types(&self) -> impl Iterator<Item = (TypeId, SegmentPtr)> + '_ {
        // Only iterate first K-1 buckets, skip inline storage types
        self.buckets[..INLINE_BUCKET_COUNT - 1].iter().filter_map(|b| {
            if b.is_empty() || b.is_inline() {
                None
            } else {
                Some((b.actual_type_id(), b.head))
            }
        })
    }

    /// Returns an iterator over all type IDs (both external and inline storage).
    #[allow(dead_code)]
    pub fn iter_all_type_ids(&self) -> impl Iterator<Item = TypeId> + '_ {
        self.buckets[..INLINE_BUCKET_COUNT - 1].iter().filter_map(|b| {
            if b.is_empty() {
                None
            } else {
                Some(b.actual_type_id())
            }
        })
    }

    /// Returns an iterator over inline storage types and their entries.
    pub fn iter_inline_types(&self) -> impl Iterator<Item = (TypeId, &[InlineAdjEntry])> + '_ {
        self.inline_entries.iter().map(|(idx, entries)| {
            let type_id = self.buckets[*idx].actual_type_id();
            (type_id, entries.as_slice())
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

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // InlineAdjEntry Tests
    // ========================================================================

    #[test]
    fn test_inline_adj_entry_encode_decode() {
        let entry = InlineAdjEntry::new(NodeId(123456), EdgeId(789012));
        let encoded = entry.encode();
        let decoded = InlineAdjEntry::decode(&encoded).unwrap();
        assert_eq!(entry, decoded);
    }

    #[test]
    fn test_inline_adj_entry_roundtrip_edge_cases() {
        // Test with max values
        let entry = InlineAdjEntry::new(NodeId(u64::MAX), EdgeId(u64::MAX));
        let encoded = entry.encode();
        let decoded = InlineAdjEntry::decode(&encoded).unwrap();
        assert_eq!(entry, decoded);

        // Test with zero values
        let entry = InlineAdjEntry::new(NodeId(0), EdgeId(0));
        let encoded = entry.encode();
        let decoded = InlineAdjEntry::decode(&encoded).unwrap();
        assert_eq!(entry, decoded);
    }

    // ========================================================================
    // TypeBucket Inline Storage Tests
    // ========================================================================

    #[test]
    fn test_type_bucket_inline_flag() {
        let bucket = TypeBucket::new_inline(TypeId(42), 2);
        assert!(bucket.is_inline());
        assert_eq!(bucket.actual_type_id(), TypeId(42));
        assert_eq!(bucket.inline_count(), 2);
    }

    #[test]
    fn test_type_bucket_external_no_flag() {
        let bucket = TypeBucket::new(TypeId(42), SegmentPtr::from_page(PageId(100)));
        assert!(!bucket.is_inline());
        assert_eq!(bucket.actual_type_id(), TypeId(42));
    }

    #[test]
    fn test_type_bucket_set_inline_external() {
        let mut bucket = TypeBucket::empty();

        // Set to inline
        bucket.set_inline(TypeId(5), 3);
        assert!(bucket.is_inline());
        assert_eq!(bucket.actual_type_id(), TypeId(5));
        assert_eq!(bucket.inline_count(), 3);

        // Switch to external
        bucket.set_external(TypeId(5), SegmentPtr::from_page(PageId(200)));
        assert!(!bucket.is_inline());
        assert_eq!(bucket.actual_type_id(), TypeId(5));
        assert_eq!(bucket.head.to_page(), PageId(200));
    }

    #[test]
    fn test_type_bucket_inline_flag_bit() {
        // Verify the flag doesn't interfere with valid TypeIds
        let type_id = TypeId(0x7FFFFFFF); // Max without flag
        let bucket = TypeBucket::new_inline(type_id, 1);
        assert!(bucket.is_inline());
        assert_eq!(bucket.actual_type_id(), type_id);
    }

    // ========================================================================
    // NodeAdjHeader Inline Entry Tests
    // ========================================================================

    #[test]
    fn test_node_adj_header_insert_inline_entry() {
        let mut header = NodeAdjHeader::new();

        // Insert first entry
        let entry1 = InlineAdjEntry::new(NodeId(100), EdgeId(200));
        header.insert_inline_entry(TypeId(1), entry1).unwrap();

        assert!(header.is_type_inline(TypeId(1)));
        let entries = header.lookup_inline_entries(TypeId(1)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], entry1);
    }

    #[test]
    fn test_node_adj_header_insert_multiple_inline_entries() {
        let mut header = NodeAdjHeader::new();

        // Insert multiple entries for same type
        let entry1 = InlineAdjEntry::new(NodeId(100), EdgeId(200));
        let entry2 = InlineAdjEntry::new(NodeId(101), EdgeId(201));
        let entry3 = InlineAdjEntry::new(NodeId(102), EdgeId(202));

        header.insert_inline_entry(TypeId(1), entry1).unwrap();
        header.insert_inline_entry(TypeId(1), entry2).unwrap();
        header.insert_inline_entry(TypeId(1), entry3).unwrap();

        let entries = header.lookup_inline_entries(TypeId(1)).unwrap();
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], entry1);
        assert_eq!(entries[1], entry2);
        assert_eq!(entries[2], entry3);
    }

    #[test]
    fn test_node_adj_header_inline_capacity_limit() {
        let mut header = NodeAdjHeader::new();

        // Fill to capacity (3 entries for single type)
        for i in 0..MAX_INLINE_ENTRIES_SINGLE {
            let entry = InlineAdjEntry::new(NodeId(i as u64), EdgeId(i as u64));
            header.insert_inline_entry(TypeId(1), entry).unwrap();
        }

        // Fourth entry should fail
        let entry = InlineAdjEntry::new(NodeId(100), EdgeId(100));
        assert!(header.insert_inline_entry(TypeId(1), entry).is_err());
    }

    #[test]
    fn test_node_adj_header_promote_to_external() {
        let mut header = NodeAdjHeader::new();

        // Insert inline entries
        let entry1 = InlineAdjEntry::new(NodeId(100), EdgeId(200));
        let entry2 = InlineAdjEntry::new(NodeId(101), EdgeId(201));
        header.insert_inline_entry(TypeId(1), entry1).unwrap();
        header.insert_inline_entry(TypeId(1), entry2).unwrap();

        // Promote to external
        let old_entries = header
            .promote_to_external(TypeId(1), SegmentPtr::from_page(PageId(500)))
            .unwrap();

        assert_eq!(old_entries.len(), 2);
        assert_eq!(old_entries[0], entry1);
        assert_eq!(old_entries[1], entry2);

        // Verify now using external storage
        assert!(!header.is_type_inline(TypeId(1)));
        assert_eq!(
            header.lookup_inline(TypeId(1)),
            Some(SegmentPtr::from_page(PageId(500)))
        );
    }

    #[test]
    fn test_node_adj_header_encode_decode_no_inline() {
        let mut header = NodeAdjHeader::new();
        header.insert_inline(TypeId(1), SegmentPtr::from_page(PageId(100))).unwrap();
        header.insert_inline(TypeId(2), SegmentPtr::from_page(PageId(200))).unwrap();

        let encoded = header.encode();
        let decoded = NodeAdjHeader::decode(&encoded).unwrap();

        assert_eq!(
            decoded.lookup_inline(TypeId(1)),
            Some(SegmentPtr::from_page(PageId(100)))
        );
        assert_eq!(
            decoded.lookup_inline(TypeId(2)),
            Some(SegmentPtr::from_page(PageId(200)))
        );
    }

    #[test]
    fn test_node_adj_header_encode_decode_with_inline_entries() {
        let mut header = NodeAdjHeader::new();

        // Insert inline entries
        let entry1 = InlineAdjEntry::new(NodeId(100), EdgeId(200));
        let entry2 = InlineAdjEntry::new(NodeId(101), EdgeId(201));
        header.insert_inline_entry(TypeId(1), entry1).unwrap();
        header.insert_inline_entry(TypeId(1), entry2).unwrap();

        let encoded = header.encode();
        let decoded = NodeAdjHeader::decode(&encoded).unwrap();

        // Verify inline entries preserved
        assert!(decoded.is_type_inline(TypeId(1)));
        let entries = decoded.lookup_inline_entries(TypeId(1)).unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0], entry1);
        assert_eq!(entries[1], entry2);
    }

    #[test]
    fn test_node_adj_header_mixed_inline_external() {
        let mut header = NodeAdjHeader::new();

        // Type 1: inline storage
        let entry = InlineAdjEntry::new(NodeId(100), EdgeId(200));
        header.insert_inline_entry(TypeId(1), entry).unwrap();

        // Type 2: external storage
        header.insert_inline(TypeId(2), SegmentPtr::from_page(PageId(300))).unwrap();

        let encoded = header.encode();
        let decoded = NodeAdjHeader::decode(&encoded).unwrap();

        // Verify inline type
        assert!(decoded.is_type_inline(TypeId(1)));
        let entries = decoded.lookup_inline_entries(TypeId(1)).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], entry);

        // Verify external type
        assert!(!decoded.is_type_inline(TypeId(2)));
        assert_eq!(
            decoded.lookup_inline(TypeId(2)),
            Some(SegmentPtr::from_page(PageId(300)))
        );
    }

    #[test]
    fn test_node_adj_header_iter_all_type_ids() {
        let mut header = NodeAdjHeader::new();

        // Mix of inline and external types
        // Space constraints: 60 bytes available before overflow slot
        // - Inline entry: 12 bytes (bucket) + 16 bytes (entry) = 28 bytes
        // - External: 12 bytes (bucket only)
        //
        // Using 1 inline (28 bytes) + 2 external (24 bytes) = 52 bytes fits
        header
            .insert_inline_entry(TypeId(1), InlineAdjEntry::new(NodeId(1), EdgeId(1)))
            .unwrap();
        header
            .insert_inline(TypeId(2), SegmentPtr::from_page(PageId(100)))
            .unwrap();
        header
            .insert_inline(TypeId(3), SegmentPtr::from_page(PageId(200)))
            .unwrap();

        let type_ids: Vec<_> = header.iter_all_type_ids().collect();
        assert_eq!(type_ids.len(), 3);
        assert!(type_ids.contains(&TypeId(1)));
        assert!(type_ids.contains(&TypeId(2)));
        assert!(type_ids.contains(&TypeId(3)));
    }

    #[test]
    fn test_node_adj_header_iter_inline_types() {
        let mut header = NodeAdjHeader::new();

        let entry1 = InlineAdjEntry::new(NodeId(100), EdgeId(200));
        let entry2 = InlineAdjEntry::new(NodeId(101), EdgeId(201));
        header.insert_inline_entry(TypeId(1), entry1).unwrap();
        header.insert_inline_entry(TypeId(1), entry2).unwrap();
        header.insert_inline(TypeId(2), SegmentPtr::from_page(PageId(100))).unwrap();

        let inline_types: Vec<_> = header.iter_inline_types().collect();
        assert_eq!(inline_types.len(), 1);
        assert_eq!(inline_types[0].0, TypeId(1));
        assert_eq!(inline_types[0].1.len(), 2);
    }
}
