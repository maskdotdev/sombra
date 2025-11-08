use std::convert::TryInto;

use crate::types::{page::PAGE_HDR_LEN, PageId, Result, SombraError};

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
const SLOT_ENTRY_LEN: usize = 2;

/// Leaf record header length in bytes (`prefix_len:u16` + `suffix_len:u16`).
pub const LEAF_RECORD_HEADER_LEN: usize = 4;

/// Internal record header length (`child:u64` + `sep_len:u16`).
pub const INTERNAL_RECORD_HEADER_LEN: usize = 10;

/// Logical kind for a B+ tree page.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BTreePageKind {
    Leaf = 1,
    Internal = 2,
}

impl BTreePageKind {
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
    pub kind: BTreePageKind,
    pub flags: u8,
    pub slot_count: u16,
    pub free_start: u16,
    pub free_end: u16,
    pub parent: Option<PageId>,
    pub right_sibling: Option<PageId>,
    pub left_sibling: Option<PageId>,
    pub low_fence_len: usize,
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
pub fn set_slot_count(payload: &mut [u8], value: u16) {
    write_u16(payload, NSLOTS_OFFSET, value);
}

pub fn set_free_start(payload: &mut [u8], value: u16) {
    write_u16(payload, FREE_START_OFFSET, value);
}

pub fn set_free_end(payload: &mut [u8], value: u16) {
    write_u16(payload, FREE_END_OFFSET, value);
}

pub fn set_parent(payload: &mut [u8], page: Option<PageId>) {
    write_page_id(payload, PARENT_OFFSET, page);
}

pub fn set_right_sibling(payload: &mut [u8], page: Option<PageId>) {
    write_page_id(payload, RIGHT_SIB_OFFSET, page);
}

pub fn set_left_sibling(payload: &mut [u8], page: Option<PageId>) {
    write_page_id(payload, LEFT_SIB_OFFSET, page);
}

pub fn set_low_fence(payload: &mut [u8], fence: &[u8]) -> Result<()> {
    write_fence(payload, LOW_FENCE_LEN_OFFSET, fence)
}

pub fn set_high_fence(payload: &mut [u8], fence: &[u8]) -> Result<()> {
    write_fence(payload, HIGH_FENCE_LEN_OFFSET, fence)
}

/// View over the slot directory at the tail of a page.
pub struct SlotDirectory<'a> {
    slots: &'a [u8],
}

impl<'a> SlotDirectory<'a> {
    pub fn len(&self) -> usize {
        self.slots.len() / SLOT_ENTRY_LEN
    }

    pub fn get(&self, idx: usize) -> Result<u16> {
        if idx >= self.len() {
            return Err(SombraError::Invalid("slot index out of range"));
        }
        let off = idx * SLOT_ENTRY_LEN;
        Ok(u16::from_be_bytes(
            self.slots[off..off + SLOT_ENTRY_LEN].try_into().unwrap(),
        ))
    }

    pub fn iter(&self) -> SlotIter<'a> {
        SlotIter {
            slots: self.slots,
            pos: 0,
        }
    }
}

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
            self.slots[self.pos..self.pos + SLOT_ENTRY_LEN]
                .try_into()
                .expect("slot slice always 2 bytes"),
        );
        self.pos += SLOT_ENTRY_LEN;
        Some(value)
    }
}

/// Reference to a leaf record stored on-page.
#[derive(Clone, Copy, Debug)]
pub struct LeafRecordRef<'a> {
    pub prefix_len: u16,
    pub key_suffix: &'a [u8],
    pub value: &'a [u8],
}

impl<'a> LeafRecordRef<'a> {
    pub fn total_len(&self) -> usize {
        LEAF_RECORD_HEADER_LEN + self.key_suffix.len() + self.value.len()
    }
}

/// Reference to an internal record stored on-page.
#[derive(Clone, Copy, Debug)]
pub struct InternalRecordRef<'a> {
    pub separator: &'a [u8],
    pub child: PageId,
}

pub fn decode_leaf_record(buf: &[u8]) -> Result<LeafRecordRef<'_>> {
    if buf.len() < LEAF_RECORD_HEADER_LEN {
        return Err(SombraError::Corruption("leaf record shorter than header"));
    }
    let prefix_len = u16::from_be_bytes(buf[0..2].try_into().unwrap());
    let suffix_len = u16::from_be_bytes(buf[2..4].try_into().unwrap()) as usize;
    if buf.len() < LEAF_RECORD_HEADER_LEN + suffix_len {
        return Err(SombraError::Corruption("leaf record key truncated"));
    }
    let key_suffix = &buf[LEAF_RECORD_HEADER_LEN..LEAF_RECORD_HEADER_LEN + suffix_len];
    let value = &buf[LEAF_RECORD_HEADER_LEN + suffix_len..];
    Ok(LeafRecordRef {
        prefix_len,
        key_suffix,
        value,
    })
}

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

pub fn encode_leaf_record(prefix_len: u16, key_suffix: &[u8], value: &[u8], dst: &mut Vec<u8>) {
    let suffix_len = u16::try_from(key_suffix.len()).expect("key suffix longer than u16");
    dst.extend_from_slice(&prefix_len.to_be_bytes());
    dst.extend_from_slice(&suffix_len.to_be_bytes());
    dst.extend_from_slice(key_suffix);
    dst.extend_from_slice(value);
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
    if slot_idx >= slots.len() {
        return Err(SombraError::Invalid("slot index out of bounds"));
    }
    let start = slots.get(slot_idx)? as usize;
    let fences_end = FENCE_DATA_OFFSET + header.low_fence_len + header.high_fence_len;
    if start < fences_end {
        return Err(SombraError::Corruption("record overlaps fence keys"));
    }
    if start >= payload.len() {
        return Err(SombraError::Corruption("record offset beyond payload"));
    }
    let mut end = header.free_start as usize;
    for offset in slots.iter() {
        let offset = offset as usize;
        if offset > start && offset < end {
            end = offset;
        }
    }
    if end > payload.len() {
        return Err(SombraError::Corruption("record extent beyond payload"));
    }
    if end < start {
        return Err(SombraError::Corruption("record extent inverted"));
    }
    Ok(&payload[start..end])
}

/// Compute the length of the shared prefix for two encoded keys.
pub fn shared_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let max = a.len().min(b.len());
    for i in 0..max {
        if a[i] != b[i] {
            return i;
        }
    }
    max
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
    pub fn new(page: &'a mut [u8]) -> Result<Self> {
        let buf = payload_slice_mut(page)?;
        Ok(Self { buf })
    }

    pub fn as_slice(&self) -> &[u8] {
        self.buf
    }

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
    fn encode_decode_leaf_record_roundtrip() -> Result<()> {
        let mut buf = Vec::new();
        encode_leaf_record(3, b"suffix", b"value", &mut buf);
        let rec = decode_leaf_record(&buf)?;
        assert_eq!(rec.prefix_len, 3);
        assert_eq!(rec.key_suffix, b"suffix");
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
    fn shared_prefix_len_handles_mismatch() {
        assert_eq!(shared_prefix_len(b"abcdef", b"abcXYZ"), 3);
        assert_eq!(shared_prefix_len(b"", b"foo"), 0);
        assert_eq!(shared_prefix_len(b"same", b"same"), 4);
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
        encode_leaf_record(0, b"key", b"value", &mut rec);
        assert!(rec.len() < payload_len);
        let record_start = PAYLOAD_HEADER_LEN;
        let record_end = record_start + rec.len();
        {
            let payload = &mut buf[PAGE_HDR_LEN..];
            payload[record_start..record_end].copy_from_slice(&rec);
            set_free_start(payload, record_end as u16);
            set_free_end(payload, (payload_len - SLOT_ENTRY_LEN) as u16);
            set_slot_count(payload, 1);
            write_u16(payload, payload_len - SLOT_ENTRY_LEN, record_start as u16);
        }
        let payload = &buf[PAGE_HDR_LEN..];
        let hdr = Header::parse(&buf)?;
        let slice = record_slice(&hdr, &buf, 0)?;
        assert_eq!(slice, &payload[record_start..record_end]);
        let rec_decoded = decode_leaf_record(slice)?;
        assert_eq!(rec_decoded.key_suffix, b"key");
        assert_eq!(rec_decoded.value, b"value");
        Ok(())
    }
}
