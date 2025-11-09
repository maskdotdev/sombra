use std::convert::{TryFrom, TryInto};

use crate::primitives::bytes::var;
use crate::storage::btree::KeyCursor;
use crate::types::{page::PAGE_HDR_LEN, PageId, Result, SombraError};
use smallvec::SmallVec;

/// Number of bytes used by the B+ tree payload header (excluding fences and slot directory).
pub const PAYLOAD_HEADER_LEN: usize = 48;

const KIND_OFFSET: usize = 0;
const FLAGS_OFFSET: usize = 1;
const NSLOTS_OFFSET: usize = 2;
const FREE_START_OFFSET: usize = 4;
const FREE_END_OFFSET: usize = 6;
const PARENT_OFFSET: usize = 8;
const RIGHT_SIB_OFFSET: usize = 16;
const LEFT_SIB_OFFSET: usize = 24;
const LOW_FENCE_LEN_OFFSET: usize = 32;
const HIGH_FENCE_LEN_OFFSET: usize = 40;
const FENCE_DATA_OFFSET: usize = PAYLOAD_HEADER_LEN;

/// Internal record header length (`child:u64` + `sep_len:u16`).
pub const INTERNAL_RECORD_HEADER_LEN: usize = 10;

/// Size in bytes of a single slot directory entry (offset + length).
pub const SLOT_ENTRY_LEN: usize = 4;

/// Logical kind for a B+ tree page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BTreePageKind {
    /// Leaf page containing actual data records
    Leaf = 1,
    /// Internal page containing separators and child pointers
    Internal = 2,
}

impl BTreePageKind {
    /// Converts a byte value to a BTreePageKind.
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Self::Leaf),
            2 => Ok(Self::Internal),
            _ => Err(SombraError::Corruption("unknown btree page kind")),
        }
    }
}

/// Header metadata decoded from the payload region of a B+ tree page.
#[derive(Clone, Debug)]
pub struct Header {
    /// The type of B+ tree page (leaf or internal)
    pub kind: BTreePageKind,
    /// Page-level flags for future use
    pub flags: u8,
    /// Number of records in the slot directory
    pub slot_count: u16,
    /// Offset to the start of free space in the payload
    pub free_start: u16,
    /// Offset to the end of free space in the payload
    pub free_end: u16,
    /// Page ID of the parent page, if any
    pub parent: Option<PageId>,
    /// Page ID of the right sibling page, if any
    pub right_sibling: Option<PageId>,
    /// Page ID of the left sibling page, if any
    pub left_sibling: Option<PageId>,
    /// Length of the low fence key in bytes
    pub low_fence_len: usize,
    /// Length of the high fence key in bytes
    pub high_fence_len: usize,
}

impl Header {
    /// Decode the header from `page` (which must include the Stage-1 header).
    pub fn parse(page: &[u8]) -> Result<Self> {
        let payload = payload_slice(page)?;
        if payload.len() < PAYLOAD_HEADER_LEN {
            return Err(SombraError::Corruption("page payload shorter than header"));
        }
        let kind = BTreePageKind::from_u8(payload[KIND_OFFSET])?;
        let flags = payload[FLAGS_OFFSET];
        let slot_count = read_u16(payload, NSLOTS_OFFSET);
        let free_start = read_u16(payload, FREE_START_OFFSET);
        let free_end = read_u16(payload, FREE_END_OFFSET);
        let parent = decode_page_id(&payload[PARENT_OFFSET..PARENT_OFFSET + 8]);
        let right_sibling = decode_page_id(&payload[RIGHT_SIB_OFFSET..RIGHT_SIB_OFFSET + 8]);
        let left_sibling = decode_page_id(&payload[LEFT_SIB_OFFSET..LEFT_SIB_OFFSET + 8]);
        let low_fence_len = decode_len(&payload[LOW_FENCE_LEN_OFFSET..LOW_FENCE_LEN_OFFSET + 8])?;
        let high_fence_len =
            decode_len(&payload[HIGH_FENCE_LEN_OFFSET..HIGH_FENCE_LEN_OFFSET + 8])?;

        if free_start as usize > payload.len()
            || free_end as usize > payload.len()
            || free_start > free_end
        {
            return Err(SombraError::Corruption(
                "btree page free space pointers out of range",
            ));
        }
        let fences_end = PAYLOAD_HEADER_LEN + low_fence_len + high_fence_len;
        if fences_end > payload.len() {
            return Err(SombraError::Corruption(
                "btree page fence keys exceed payload",
            ));
        }
        if (free_start as usize) < fences_end {
            return Err(SombraError::Corruption(
                "btree page free_start overlaps fences",
            ));
        }
        if free_end as usize > payload.len() {
            return Err(SombraError::Corruption(
                "btree page free_end beyond payload",
            ));
        }
        let slot_bytes = slot_count as usize * SLOT_ENTRY_LEN;
        if slot_bytes > payload.len() {
            return Err(SombraError::Corruption(
                "btree slot directory larger than payload",
            ));
        }
        let slot_start = payload.len().saturating_sub(slot_bytes);
        if slot_start < free_end as usize {
            return Err(SombraError::Corruption(
                "btree free_end overlaps slot directory",
            ));
        }

        Ok(Self {
            kind,
            flags,
            slot_count,
            free_start,
            free_end,
            parent,
            right_sibling,
            left_sibling,
            low_fence_len,
            high_fence_len,
        })
    }

    /// Return the low and high fence slices stored in the page.
    pub fn fence_slices<'a>(&self, page: &'a [u8]) -> Result<(&'a [u8], &'a [u8])> {
        let payload = payload_slice(page)?;
        let low_start = FENCE_DATA_OFFSET;
        let low_end = low_start + self.low_fence_len;
        let high_end = low_end + self.high_fence_len;
        if high_end > payload.len() {
            return Err(SombraError::Corruption("fence keys exceed payload"));
        }
        Ok((&payload[low_start..low_end], &payload[low_end..high_end]))
    }

    /// Access the slot directory for the page.
    pub fn slot_directory<'a>(&self, page: &'a [u8]) -> Result<SlotDirectory<'a>> {
        let payload = payload_slice(page)?;
        let slot_bytes = self.slot_count as usize * SLOT_ENTRY_LEN;
        if slot_bytes == 0 {
            return Ok(SlotDirectory { slots: &[] });
        }
        let start = payload.len() - slot_bytes;
        Ok(SlotDirectory {
            slots: &payload[start..],
        })
    }
}

/// Update helpers for callers that operate on a mutable page payload.

/// Sets the slot count in the page header.
pub fn set_slot_count(payload: &mut [u8], value: u16) {
    write_u16(payload, NSLOTS_OFFSET, value);
}

/// Writes a single slot entry at `pos` (relative to the payload start).
#[inline]
pub fn write_slot_entry(payload: &mut [u8], pos: usize, offset: u16, len: u16) {
    debug_assert!(pos + SLOT_ENTRY_LEN <= payload.len());
    let offset_bytes = offset.to_be_bytes();
    let len_bytes = len.to_be_bytes();
    payload[pos..pos + 2].copy_from_slice(&offset_bytes);
    payload[pos + 2..pos + 4].copy_from_slice(&len_bytes);
}

/// Sets the free space start offset in the page header.
pub fn set_free_start(payload: &mut [u8], value: u16) {
    write_u16(payload, FREE_START_OFFSET, value);
}

/// Sets the free space end offset in the page header.
pub fn set_free_end(payload: &mut [u8], value: u16) {
    write_u16(payload, FREE_END_OFFSET, value);
}

/// Sets the parent page ID in the page header.
pub fn set_parent(payload: &mut [u8], page: Option<PageId>) {
    write_page_id(payload, PARENT_OFFSET, page);
}

/// Sets the right sibling page ID in the page header.
pub fn set_right_sibling(payload: &mut [u8], page: Option<PageId>) {
    write_page_id(payload, RIGHT_SIB_OFFSET, page);
}

/// Sets the left sibling page ID in the page header.
pub fn set_left_sibling(payload: &mut [u8], page: Option<PageId>) {
    write_page_id(payload, LEFT_SIB_OFFSET, page);
}

/// Sets the low fence key in the page.
pub fn set_low_fence(payload: &mut [u8], fence: &[u8]) -> Result<()> {
    write_fence(payload, LOW_FENCE_LEN_OFFSET, fence)
}

/// Sets the high fence key in the page.
pub fn set_high_fence(payload: &mut [u8], fence: &[u8]) -> Result<()> {
    write_fence(payload, HIGH_FENCE_LEN_OFFSET, fence)
}

/// View over the slot directory at the tail of a page.
pub struct SlotDirectory<'a> {
    slots: &'a [u8],
}

impl<'a> SlotDirectory<'a> {
    /// Returns the number of slots in the directory.
    pub fn len(&self) -> usize {
        if SLOT_ENTRY_LEN == 0 {
            0
        } else {
            self.slots.len() / SLOT_ENTRY_LEN
        }
    }

    /// Retrieves the offset value at the given slot index.
    pub fn get(&self, idx: usize) -> Result<u16> {
        let entry = self.entry_bytes(idx)?;
        Ok(u16::from_be_bytes(entry[0..2].try_into().unwrap()))
    }

    /// Returns an iterator over all slot offsets.
    pub fn iter(&self) -> SlotIter<'a> {
        SlotIter {
            slots: self.slots,
            pos: 0,
        }
    }

    /// Returns the (start, length) tuple for `idx`.
    pub fn extent(&self, idx: usize) -> Result<(u16, u16)> {
        let entry = self.entry_bytes(idx)?;
        let start = u16::from_be_bytes(entry[0..2].try_into().unwrap());
        let len = u16::from_be_bytes(entry[2..4].try_into().unwrap());
        Ok((start, len))
    }

    fn entry_bytes(&self, idx: usize) -> Result<&'a [u8]> {
        if idx >= self.len() {
            return Err(SombraError::Invalid("slot index out of range"));
        }
        let off = idx * SLOT_ENTRY_LEN;
        Ok(&self.slots[off..off + SLOT_ENTRY_LEN])
    }
}

/// Iterator over slot directory entries (offset-only view).
pub struct SlotIter<'a> {
    slots: &'a [u8],
    pos: usize,
}

impl<'a> Iterator for SlotIter<'a> {
    type Item = u16;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.slots.len() {
            return None;
        }
        let value = u16::from_be_bytes(
            self.slots[self.pos..self.pos + 2]
                .try_into()
                .expect("slot slice always 2 bytes"),
        );
        self.pos += SLOT_ENTRY_LEN;
        Some(value)
    }
}

const INLINE_SLOT_EXTENTS: usize = 32;

#[derive(Clone, Copy, Debug, Default)]
struct SlotExtent {
    start: u16,
    end: u16,
}

/// Precomputed extents for all slots on a page to make record lookups O(1).
pub struct SlotExtents {
    extents: SmallVec<[SlotExtent; INLINE_SLOT_EXTENTS]>,
}

impl SlotExtents {
    /// Builds the extent table for `slots` on a single page visit.
    pub fn build(header: &Header, payload: &[u8], slots: &SlotDirectory<'_>) -> Result<Self> {
        if slots.len() == 0 {
            return Ok(Self {
                extents: SmallVec::new(),
            });
        }
        let payload_len = payload.len();
        if payload_len > u16::MAX as usize {
            return Err(SombraError::Corruption("btree payload exceeds u16"));
        }
        let fences_end = FENCE_DATA_OFFSET + header.low_fence_len + header.high_fence_len;
        let free_start = header.free_start as usize;
        if free_start > payload_len {
            return Err(SombraError::Corruption("record extent beyond payload"));
        }
        let mut extents = SmallVec::with_capacity(slots.len());
        extents.resize(slots.len(), SlotExtent::default());
        let mut ordered: SmallVec<[(usize, usize); INLINE_SLOT_EXTENTS]> =
            SmallVec::with_capacity(slots.len());
        for idx in 0..slots.len() {
            let (start_u16, len_u16) = slots.extent(idx)?;
            let start = start_u16 as usize;
            let len = len_u16 as usize;
            if len == 0 {
                return Err(SombraError::Corruption("record length zero"));
            }
            if start < fences_end {
                return Err(SombraError::Corruption("record overlaps fence keys"));
            }
            let end = start
                .checked_add(len)
                .ok_or(SombraError::Corruption("record extent overflow"))?;
            if end > payload_len {
                return Err(SombraError::Corruption("record extent beyond payload"));
            }
            if end > free_start {
                return Err(SombraError::Corruption("record extent beyond free_start"));
            }
            let end_u16 = u16::try_from(end)
                .map_err(|_| SombraError::Corruption("record extent beyond u16"))?;
            extents[idx] = SlotExtent {
                start: start_u16,
                end: end_u16,
            };
            ordered.push((start, end));
        }
        ordered.sort_unstable_by_key(|entry| entry.0);
        let mut prev_end = fences_end;
        for (start, end) in ordered {
            if start < prev_end {
                return Err(SombraError::Corruption("record extents overlap"));
            }
            prev_end = end;
        }
        Ok(Self { extents })
    }

    /// Returns the raw bytes for `slot_idx` using the precomputed extents.
    pub fn record_slice<'a>(&self, payload: &'a [u8], slot_idx: usize) -> Result<&'a [u8]> {
        let extent = self
            .extents
            .get(slot_idx)
            .ok_or(SombraError::Invalid("slot index out of bounds"))?;
        let start = extent.start as usize;
        let end = extent.end as usize;
        if end > payload.len() {
            return Err(SombraError::Corruption("record extent beyond payload"));
        }
        if start > end {
            return Err(SombraError::Corruption("record extent inverted"));
        }
        Ok(&payload[start..end])
    }
}

/// Reference to a plain leaf record stored on-page (`varint key_len | varint val_len | key | value`).
#[derive(Clone, Copy, Debug)]
pub struct LeafRecordRef<'a> {
    /// Full key bytes stored in the record.
    pub key: &'a [u8],
    /// Value data stored in the record.
    pub value: &'a [u8],
}

/// Reference to an internal record stored on-page.
#[derive(Clone, Copy, Debug)]
pub struct InternalRecordRef<'a> {
    /// The separator key stored in the internal record
    pub separator: &'a [u8],
    /// The child page ID pointed to by this record
    pub child: PageId,
}

/// Decodes an internal record from the given buffer.
pub fn decode_internal_record(buf: &[u8]) -> Result<InternalRecordRef<'_>> {
    if buf.len() < INTERNAL_RECORD_HEADER_LEN {
        return Err(SombraError::Corruption(
            "internal record shorter than header",
        ));
    }
    let child = PageId(u64::from_be_bytes(buf[0..8].try_into().unwrap()));
    let key_len = u16::from_be_bytes(buf[8..10].try_into().unwrap()) as usize;
    let end = INTERNAL_RECORD_HEADER_LEN + key_len;
    if buf.len() < end {
        return Err(SombraError::Corruption("internal record truncated"));
    }
    let separator = &buf[INTERNAL_RECORD_HEADER_LEN..end];
    Ok(InternalRecordRef { separator, child })
}

/// Encodes a plain (varint) leaf record into the destination vector.
pub fn encode_leaf_record(key: &[u8], value: &[u8], dst: &mut Vec<u8>) -> Result<()> {
    if key.is_empty() {
        return Err(SombraError::Invalid(
            "plain leaf key length must be non-zero",
        ));
    }
    let key_len = u64::try_from(key.len())
        .map_err(|_| SombraError::Invalid("plain leaf key length exceeds u64"))?;
    let val_len = u64::try_from(value.len())
        .map_err(|_| SombraError::Invalid("plain leaf value length exceeds u64"))?;
    var::encode_u64(key_len, dst);
    var::encode_u64(val_len, dst);
    dst.extend_from_slice(key);
    dst.extend_from_slice(value);
    Ok(())
}

/// Decodes a plain leaf record from the given buffer.
pub fn decode_leaf_record(buf: &[u8]) -> Result<LeafRecordRef<'_>> {
    let mut cursor = KeyCursor::new(buf);
    let key_len = cursor.read_var_u64("plain leaf key length truncated")?;
    if key_len == 0 {
        return Err(SombraError::Corruption("plain leaf key length zero"));
    }
    let val_len = cursor.read_var_u64("plain leaf value length truncated")?;
    let key_len_usize = usize::try_from(key_len)
        .map_err(|_| SombraError::Corruption("plain leaf key length exceeds usize"))?;
    let val_len_usize = usize::try_from(val_len)
        .map_err(|_| SombraError::Corruption("plain leaf value length exceeds usize"))?;
    let key = cursor.take(key_len_usize)?;
    let value = cursor.take(val_len_usize)?;
    Ok(LeafRecordRef { key, value })
}

/// Computes the encoded length of a plain leaf record without writing it.
pub fn plain_leaf_record_encoded_len(key_len: usize, value_len: usize) -> Result<usize> {
    if key_len == 0 {
        return Err(SombraError::Invalid(
            "plain leaf key length must be non-zero",
        ));
    }
    let key_var = varint_len_usize(key_len)?;
    let val_var = varint_len_usize(value_len)?;
    key_var
        .checked_add(val_var)
        .and_then(|total| total.checked_add(key_len))
        .and_then(|total| total.checked_add(value_len))
        .ok_or(SombraError::Invalid("plain leaf record length overflow"))
}

/// Encode an internal record as `[child_page_id:u64][sep_len:u16][sep bytes]`.
pub fn encode_internal_record(separator: &[u8], child: PageId, dst: &mut Vec<u8>) {
    let key_len = u16::try_from(separator.len()).expect("separator longer than u16");
    dst.extend_from_slice(&child.0.to_be_bytes());
    dst.extend_from_slice(&key_len.to_be_bytes());
    dst.extend_from_slice(separator);
}

/// Return the raw bytes of the record referenced by `slot_idx`.
pub fn record_slice<'a>(header: &Header, page: &'a [u8], slot_idx: usize) -> Result<&'a [u8]> {
    let payload = payload_slice(page)?;
    let slots = header.slot_directory(page)?;
    record_slice_from_parts(header, payload, &slots, slot_idx)
}

/// Internal helper that avoids recomputing the slot directory on repeated calls.
pub fn record_slice_from_parts<'a>(
    header: &Header,
    payload: &'a [u8],
    slots: &SlotDirectory<'a>,
    slot_idx: usize,
) -> Result<&'a [u8]> {
    let extents = SlotExtents::build(header, payload, slots)?;
    extents.record_slice(payload, slot_idx)
}

fn varint_len_usize(value: usize) -> Result<usize> {
    let raw =
        u64::try_from(value).map_err(|_| SombraError::Invalid("plain leaf length exceeds u64"))?;
    Ok(varint_len_u64(raw))
}

fn varint_len_u64(mut value: u64) -> usize {
    let mut len = 1;
    while value >= 0x80 {
        value >>= 7;
        len += 1;
    }
    len
}

fn decode_page_id(bytes: &[u8]) -> Option<PageId> {
    let raw = u64::from_be_bytes(bytes.try_into().unwrap());
    if raw == 0 {
        None
    } else {
        Some(PageId(raw))
    }
}

fn decode_len(bytes: &[u8]) -> Result<usize> {
    let raw = u64::from_be_bytes(bytes.try_into().unwrap());
    usize::try_from(raw).map_err(|_| SombraError::Corruption("btree length overflow"))
}

fn payload_slice(page: &[u8]) -> Result<&[u8]> {
    if page.len() < PAGE_HDR_LEN {
        return Err(SombraError::Corruption("page shorter than header"));
    }
    Ok(&page[PAGE_HDR_LEN..])
}

fn payload_slice_mut(page: &mut [u8]) -> Result<&mut [u8]> {
    if page.len() < PAGE_HDR_LEN {
        return Err(SombraError::Corruption("page shorter than header"));
    }
    Ok(&mut page[PAGE_HDR_LEN..])
}

/// Expose the payload slice (after the Stage-1 header).
pub fn payload(page: &[u8]) -> Result<&[u8]> {
    payload_slice(page)
}

/// Mutable variant of [`payload`].
pub fn payload_mut(page: &mut [u8]) -> Result<&mut [u8]> {
    payload_slice_mut(page)
}

fn read_u16(payload: &[u8], offset: usize) -> u16 {
    u16::from_be_bytes(payload[offset..offset + 2].try_into().unwrap())
}

fn write_u16(payload: &mut [u8], offset: usize, value: u16) {
    payload[offset..offset + 2].copy_from_slice(&value.to_be_bytes());
}

fn write_page_id(payload: &mut [u8], offset: usize, value: Option<PageId>) {
    let raw = value.map(|p| p.0).unwrap_or(0);
    payload[offset..offset + 8].copy_from_slice(&raw.to_be_bytes());
}

fn write_fence(payload: &mut [u8], offset: usize, fence: &[u8]) -> Result<()> {
    let len =
        u64::try_from(fence.len()).map_err(|_| SombraError::Invalid("fence longer than u64"))?;
    payload[offset..offset + 8].copy_from_slice(&len.to_be_bytes());
    let low_len = if offset == LOW_FENCE_LEN_OFFSET {
        len as usize
    } else {
        read_u64(payload, LOW_FENCE_LEN_OFFSET) as usize
    };
    let start = if offset == LOW_FENCE_LEN_OFFSET {
        FENCE_DATA_OFFSET
    } else {
        FENCE_DATA_OFFSET + low_len
    };
    let end = start + fence.len();
    if end > payload.len() {
        return Err(SombraError::Invalid("fence does not fit in payload"));
    }
    payload[start..end].copy_from_slice(fence);
    Ok(())
}

fn read_u64(payload: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(payload[offset..offset + 8].try_into().unwrap())
}

/// Write the default header for a freshly-initialized page.
pub fn write_initial_header(buf: &mut [u8], kind: BTreePageKind) -> Result<()> {
    if buf.len() < PAYLOAD_HEADER_LEN {
        return Err(SombraError::Invalid("payload buffer too small for header"));
    }
    buf.fill(0);
    buf[KIND_OFFSET] = kind as u8;
    buf[FLAGS_OFFSET] = 0;
    write_u16(buf, NSLOTS_OFFSET, 0);

    let free_start = u16::try_from(PAYLOAD_HEADER_LEN)
        .map_err(|_| SombraError::Invalid("payload header exceeds u16"))?;
    let free_end = u16::try_from(buf.len())
        .map_err(|_| SombraError::Invalid("page payload larger than u16"))?;
    write_u16(buf, FREE_START_OFFSET, free_start);
    write_u16(buf, FREE_END_OFFSET, free_end);
    Ok(())
}

/// Mutable view over the payload to help callers derive offsets for further updates.
#[allow(dead_code)]
pub struct PayloadMut<'a> {
    buf: &'a mut [u8],
}

impl<'a> PayloadMut<'a> {
    /// Creates a new PayloadMut from a page buffer.
    pub fn new(page: &'a mut [u8]) -> Result<Self> {
        let buf = payload_slice_mut(page)?;
        Ok(Self { buf })
    }

    /// Returns an immutable slice of the payload.
    pub fn as_slice(&self) -> &[u8] {
        self.buf
    }

    /// Returns a mutable slice of the payload.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        self.buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::page::{PageHeader, PageKind};

    #[test]
    fn write_and_parse_initial_header() -> Result<()> {
        let page_size = 4096;
        let mut buf = vec![0u8; page_size];
        let header =
            PageHeader::new(PageId(1), PageKind::BTreeLeaf, page_size as u32, 777)?.with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        write_initial_header(&mut buf[PAGE_HDR_LEN..], BTreePageKind::Leaf)?;
        let parsed = Header::parse(&buf)?;
        assert_eq!(parsed.kind, BTreePageKind::Leaf);
        assert_eq!(parsed.slot_count, 0);
        assert_eq!(parsed.free_start as usize, PAYLOAD_HEADER_LEN);
        assert_eq!(parsed.free_end as usize, page_size - PAGE_HDR_LEN);
        let (low, high) = parsed.fence_slices(&buf)?;
        assert!(low.is_empty());
        assert!(high.is_empty());
        let slots = parsed.slot_directory(&buf)?;
        assert_eq!(slots.len(), 0);
        Ok(())
    }

    #[test]
    fn encode_decode_plain_leaf_record_roundtrip() -> Result<()> {
        let mut buf = Vec::new();
        encode_leaf_record(b"plain-key", b"value", &mut buf)?;
        let rec = decode_leaf_record(&buf)?;
        assert_eq!(rec.key, b"plain-key");
        assert_eq!(rec.value, b"value");
        Ok(())
    }

    #[test]
    fn encode_decode_internal_record_roundtrip() -> Result<()> {
        let mut buf = Vec::new();
        encode_internal_record(b"sep", PageId(77), &mut buf);
        let rec = decode_internal_record(&buf)?;
        assert_eq!(rec.separator, b"sep");
        assert_eq!(rec.child, PageId(77));
        Ok(())
    }

    #[test]
    fn record_slice_tracks_free_space() -> Result<()> {
        let page_size = 512;
        let mut buf = vec![0u8; page_size];
        let header =
            PageHeader::new(PageId(5), PageKind::BTreeLeaf, page_size as u32, 1234)?.with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        write_initial_header(&mut buf[PAGE_HDR_LEN..], BTreePageKind::Leaf)?;

        let payload_len = page_size - PAGE_HDR_LEN;
        let mut rec = Vec::new();
        encode_leaf_record(b"key", b"value", &mut rec)?;
        assert!(rec.len() < payload_len);
        let record_start = PAYLOAD_HEADER_LEN;
        let record_end = record_start + rec.len();
        {
            let payload = &mut buf[PAGE_HDR_LEN..];
            payload[record_start..record_end].copy_from_slice(&rec);
            set_free_start(payload, record_end as u16);
            set_free_end(payload, (payload_len - SLOT_ENTRY_LEN) as u16);
            set_slot_count(payload, 1);
            let rec_len_u16 =
                u16::try_from(rec.len()).expect("test record length fits into u16 slot entry");
            write_slot_entry(
                payload,
                payload_len - SLOT_ENTRY_LEN,
                record_start as u16,
                rec_len_u16,
            );
        }
        let payload = &buf[PAGE_HDR_LEN..];
        let hdr = Header::parse(&buf)?;
        let slice = record_slice(&hdr, &buf, 0)?;
        assert_eq!(slice, &payload[record_start..record_end]);
        let rec_decoded = decode_leaf_record(slice)?;
        assert_eq!(rec_decoded.key, b"key");
        assert_eq!(rec_decoded.value, b"value");
        Ok(())
    }

    #[test]
    fn encode_plain_leaf_record_rejects_empty_key() {
        let mut buf = Vec::new();
        let err = encode_leaf_record(b"", b"value", &mut buf).unwrap_err();
        assert!(matches!(err, SombraError::Invalid(_)));
    }

    #[test]
    fn decode_plain_leaf_record_rejects_truncated_payload() {
        let mut buf = Vec::new();
        encode_leaf_record(b"k", b"v", &mut buf).expect("encode plain record");
        let truncated = &buf[..buf.len() - 1];
        let err = decode_leaf_record(truncated).unwrap_err();
        assert!(matches!(err, SombraError::Corruption(_)));
    }

    #[test]
    fn slot_extents_precompute_record_boundaries() -> Result<()> {
        let page_size = 512;
        let mut buf = vec![0u8; page_size];
        let header =
            PageHeader::new(PageId(6), PageKind::BTreeLeaf, page_size as u32, 421)?.with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        write_initial_header(&mut buf[PAGE_HDR_LEN..], BTreePageKind::Leaf)?;

        let mut rec1 = Vec::new();
        let mut rec2 = Vec::new();
        encode_leaf_record(b"key-1", b"value-1", &mut rec1)?;
        encode_leaf_record(b"key-2", b"value-2", &mut rec2)?;
        let payload_len = page_size - PAGE_HDR_LEN;
        let payload = payload_slice_mut(&mut buf)?;
        let rec1_start = PAYLOAD_HEADER_LEN;
        let rec2_start = rec1_start + rec1.len();
        let rec2_end = rec2_start + rec2.len();
        payload[rec1_start..rec1_start + rec1.len()].copy_from_slice(&rec1);
        payload[rec2_start..rec2_end].copy_from_slice(&rec2);
        set_free_start(payload, rec2_end as u16);
        set_free_end(payload, (payload_len - SLOT_ENTRY_LEN * 2) as u16);
        set_slot_count(payload, 2);
        let rec1_len_u16 =
            u16::try_from(rec1.len()).expect("test record length fits into u16 slot entry");
        let rec2_len_u16 =
            u16::try_from(rec2.len()).expect("test record length fits into u16 slot entry");
        let slots_start = payload_len - SLOT_ENTRY_LEN * 2;
        write_slot_entry(payload, slots_start, rec1_start as u16, rec1_len_u16);
        write_slot_entry(
            payload,
            slots_start + SLOT_ENTRY_LEN,
            rec2_start as u16,
            rec2_len_u16,
        );

        let parsed = Header::parse(&buf)?;
        let payload = payload_slice(&buf)?;
        let slots = parsed.slot_directory(&buf)?;
        let extents = SlotExtents::build(&parsed, payload, &slots)?;
        let first = extents.record_slice(payload, 0)?;
        let second = extents.record_slice(payload, 1)?;
        assert_eq!(first, &rec1[..]);
        assert_eq!(second, &rec2[..]);
        Ok(())
    }
}
