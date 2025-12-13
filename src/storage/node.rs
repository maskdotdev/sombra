#[cfg(test)]
use super::mvcc::COMMIT_MAX;
use super::mvcc::{flags, VersionHeader, VersionPtr, VERSION_HEADER_LEN, VERSION_PTR_LEN};
use crate::types::{EdgeId, LabelId, NodeId, PageId, Result, SombraError, VRef};

use super::rowhash::row_hash64;

const ROW_STORAGE_MASK: u8 = 0x7F;
const ROW_FLAG_HASH: u8 = 0x80;
const ROW_STORAGE_INLINE: u8 = 0;
const ROW_STORAGE_VREF: u8 = 1;

pub enum PropPayload<'a> {
    Inline(&'a [u8]),
    VRef(VRef),
}

#[derive(Clone, Debug)]
pub enum PropStorage {
    Inline(Vec<u8>),
    VRef(VRef),
}

pub const DIR_OUT: u8 = 0;
pub const DIR_IN: u8 = 1;

/// Single inline adjacency entry stored in a node row.
/// Layout: [dir:1][type_id:3][neighbor:8][edge:8] (20 bytes)
#[derive(Clone, Debug, PartialEq)]
pub struct InlineAdjEntry {
    pub direction: u8,   // 0 = OUT, 1 = IN
    pub type_id: u32,    // lower 24 bits encoded
    pub neighbor: NodeId,
    pub edge: EdgeId,
}

impl InlineAdjEntry {
    pub const ENCODED_LEN: usize = 20;

    pub fn encode(&self, buf: &mut [u8]) {
        debug_assert!(buf.len() >= Self::ENCODED_LEN);
        buf[0] = self.direction;
        let type_bytes = self.type_id.to_be_bytes();
        buf[1..4].copy_from_slice(&type_bytes[1..4]);
        buf[4..12].copy_from_slice(&self.neighbor.0.to_be_bytes());
        buf[12..20].copy_from_slice(&self.edge.0.to_be_bytes());
    }

    pub fn decode(buf: &[u8]) -> Self {
        debug_assert!(buf.len() >= Self::ENCODED_LEN);
        let direction = buf[0];
        let type_id = u32::from_be_bytes([0, buf[1], buf[2], buf[3]]);
        let neighbor = NodeId(u64::from_be_bytes(buf[4..12].try_into().unwrap()));
        let edge = EdgeId(u64::from_be_bytes(buf[12..20].try_into().unwrap()));
        Self {
            direction,
            type_id,
            neighbor,
            edge,
        }
    }
}

pub const MAX_INLINE_ADJ_ENTRIES: usize = 8;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct InlineNodeAdj {
    pub entries: Vec<InlineAdjEntry>,
}

impl InlineNodeAdj {
    pub fn new() -> Self {
        Self { entries: Vec::new() }
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn needs_promotion(&self, additional: usize) -> bool {
        self.entries.len() + additional > MAX_INLINE_ADJ_ENTRIES
    }

    pub fn add(&mut self, entry: InlineAdjEntry) {
        self.entries.push(entry);
    }
}

#[derive(Clone, Debug)]
pub struct NodeRow {
    pub labels: Vec<LabelId>,
    pub props: PropStorage,
    #[cfg_attr(not(test), allow(dead_code))]
    pub row_hash: Option<u64>,
    /// Optional adjacency page for IFA (Index-Free Adjacency).
    /// When present, this page contains both OUT and IN adjacency headers.
    pub adj_page: Option<PageId>,
    /// Optional inline adjacency stored directly in the node row.
    pub inline_adj: Option<InlineNodeAdj>,
}

/// Node payload paired with its MVCC metadata.
#[derive(Clone, Debug)]
pub struct VersionedNodeRow {
    pub header: VersionHeader,
    pub prev_ptr: VersionPtr,
    pub row: NodeRow,
    pub inline_history: Option<Vec<u8>>,
}

/// Encoding options for node rows.
#[derive(Clone, Copy, Debug, Default)]
pub struct EncodeOpts<'a> {
    /// Whether to append an 8-byte SipHash64 footer.
    pub append_row_hash: bool,
    /// Optional IFA adjacency page ID to store with the node.
    pub adj_page: Option<PageId>,
    /// Optional inline adjacency to encode into the payload.
    pub inline_adj: Option<&'a InlineNodeAdj>,
}

impl<'a> EncodeOpts<'a> {
    pub const fn new(append_row_hash: bool) -> Self {
        Self {
            append_row_hash,
            adj_page: None,
            inline_adj: None,
        }
    }

    /// Sets the adjacency page for IFA.
    pub const fn with_adj_page(mut self, page: PageId) -> Self {
        self.adj_page = Some(page);
        self
    }

    /// Sets inline adjacency to be encoded.
    pub fn with_inline_adj(mut self, adj: &'a InlineNodeAdj) -> Self {
        self.inline_adj = Some(adj);
        self
    }
}

/// Result of node row encoding, exposing the encoded bytes and optional hash.
#[derive(Clone, Debug)]
pub struct EncodedNodeRow {
    pub bytes: Vec<u8>,
    #[allow(dead_code)]
    pub header: VersionHeader,
    #[allow(dead_code)]
    pub prev_ptr: VersionPtr,
    #[allow(dead_code)]
    pub row_hash: Option<u64>,
}

pub fn encode<'a>(
    labels: &[LabelId],
    props: PropPayload<'_>,
    opts: EncodeOpts<'a>,
    mut version: VersionHeader,
    prev_ptr: VersionPtr,
    inline_history: Option<&[u8]>,
) -> Result<EncodedNodeRow> {
    // Clear inline-adjacency flag; it will be recomputed below based on opts.
    version.flags &= !flags::HAS_INLINE_ADJ;
    if labels.len() > u8::MAX as usize {
        return Err(SombraError::Invalid(
            "too many labels for inline node encoding",
        ));
    }
    let mut payload = Vec::new();
    payload.push(labels.len() as u8);
    for label in labels {
        payload.extend_from_slice(&label.0.to_be_bytes());
    }
    match props {
        PropPayload::Inline(bytes) => {
            if bytes.len() > u16::MAX as usize {
                return Err(SombraError::Invalid(
                    "inline property blob exceeds u16 length",
                ));
            }
            let mut tag = ROW_STORAGE_INLINE;
            if opts.append_row_hash {
                tag |= ROW_FLAG_HASH;
            }
            payload.push(tag);
            payload.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            payload.extend_from_slice(bytes);
        }
        PropPayload::VRef(vref) => {
            let mut tag = ROW_STORAGE_VREF;
            if opts.append_row_hash {
                tag |= ROW_FLAG_HASH;
            }
            payload.push(tag);
            version.flags |= flags::PAYLOAD_EXTERNAL;
            payload.extend_from_slice(&vref.start_page.0.to_be_bytes());
            payload.extend_from_slice(&vref.n_pages.to_be_bytes());
            payload.extend_from_slice(&vref.len.to_be_bytes());
            payload.extend_from_slice(&vref.checksum.to_be_bytes());
            payload.extend_from_slice(&vref.owner_commit.to_be_bytes());
        }
    }
    let row_hash = if opts.append_row_hash {
        let hash = row_hash64(&payload);
        payload.extend_from_slice(&hash.to_be_bytes());
        Some(hash)
    } else {
        None
    };
    // Append adjacency page pointer if present (for IFA)
    if let Some(adj_page) = opts.adj_page {
        version.flags |= flags::HAS_ADJ_PAGE;
        payload.extend_from_slice(&adj_page.0.to_be_bytes());
    }
    // Append inline adjacency if present (for true IFA inline mode)
    if let Some(inline_adj) = opts.inline_adj {
        if !inline_adj.is_empty() {
            if inline_adj.len() > u8::MAX as usize {
                return Err(SombraError::Invalid("too many inline adjacency entries"));
            }
            let needed = payload
                .len()
                .saturating_add(1 + inline_adj.len() * InlineAdjEntry::ENCODED_LEN);
            if needed > u16::MAX as usize {
                return Err(SombraError::Invalid("inline adjacency exceeds payload limit"));
            }
            version.flags |= flags::HAS_INLINE_ADJ;
            payload.push(inline_adj.len() as u8);
            for entry in &inline_adj.entries {
                let mut buf = [0u8; InlineAdjEntry::ENCODED_LEN];
                entry.encode(&mut buf);
                payload.extend_from_slice(&buf);
            }
        }
    }
    if let Some(history) = inline_history {
        let history_len = u16::try_from(history.len())
            .map_err(|_| SombraError::Invalid("inline history too large"))?;
        let needed = payload.len().saturating_add(2 + history.len());
        if needed > u16::MAX as usize {
            return Err(SombraError::Invalid("inline history exceeds payload limit"));
        }
        version.flags |= flags::INLINE_HISTORY;
        payload.extend_from_slice(&history_len.to_be_bytes());
        payload.extend_from_slice(history);
    }
    let payload_len =
        u16::try_from(payload.len()).map_err(|_| SombraError::Invalid("node payload too large"))?;
    version.payload_len = payload_len;
    let mut bytes = Vec::with_capacity(VERSION_HEADER_LEN + VERSION_PTR_LEN + payload.len());
    version.encode_into(&mut bytes);
    bytes.extend_from_slice(&prev_ptr.to_bytes());
    bytes.extend_from_slice(&payload);
    Ok(EncodedNodeRow {
        bytes,
        header: version,
        prev_ptr,
        row_hash,
    })
}

pub fn decode(data: &[u8]) -> Result<VersionedNodeRow> {
    if data.len() < VERSION_HEADER_LEN {
        return Err(SombraError::Corruption("node row truncated"));
    }
    let header = VersionHeader::decode(data)?;
    if header.payload_len == 0 {
        return Err(SombraError::Corruption("node row missing payload"));
    }
    let ptr_offset = VERSION_HEADER_LEN;
    let payload_offset = ptr_offset + VERSION_PTR_LEN;
    if data.len() < payload_offset {
        return Err(SombraError::Corruption("node row missing version pointer"));
    }
    let prev_ptr = VersionPtr::from_bytes(&data[ptr_offset..payload_offset])?;
    let payload_end = payload_offset + header.payload_len as usize;
    if data.len() < payload_end {
        return Err(SombraError::Corruption("node row payload truncated"));
    }
    let payload = &data[payload_offset..payload_end];
    if payload.is_empty() {
        return Err(SombraError::Corruption("node row payload empty"));
    }
    let label_count = payload[0] as usize;
    let mut offset = 1usize;
    if payload.len() < offset + label_count * 4 + 1 {
        return Err(SombraError::Corruption("node labels truncated"));
    }
    let mut labels = Vec::with_capacity(label_count);
    for _ in 0..label_count {
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&payload[offset..offset + 4]);
        offset += 4;
        labels.push(LabelId(u32::from_be_bytes(arr)));
    }
    if offset >= payload.len() {
        return Err(SombraError::Corruption("node property tag missing"));
    }
    let tag = payload[offset];
    offset += 1;
    let has_hash = (tag & ROW_FLAG_HASH) != 0;
    let storage_kind = tag & ROW_STORAGE_MASK;
    let props = match storage_kind {
        ROW_STORAGE_INLINE => {
            if offset + 2 > payload.len() {
                return Err(SombraError::Corruption(
                    "node inline property length missing",
                ));
            }
            let len = u16::from_be_bytes(payload[offset..offset + 2].try_into().unwrap()) as usize;
            offset += 2;
            if offset + len > payload.len() {
                return Err(SombraError::Corruption(
                    "node inline property payload truncated",
                ));
            }
            let value = payload[offset..offset + len].to_vec();
            offset += len;
            PropStorage::Inline(value)
        }
        ROW_STORAGE_VREF => {
            if offset + 28 > payload.len() {
                return Err(SombraError::Corruption("node vref payload truncated"));
            }
            let start_page = u64_from_be(&payload[offset..offset + 8]);
            offset += 8;
            let n_pages = u32_from_be(&payload[offset..offset + 4]);
            offset += 4;
            let len = u32_from_be(&payload[offset..offset + 4]);
            offset += 4;
            let checksum = u32_from_be(&payload[offset..offset + 4]);
            offset += 4;
            let owner_commit = u64_from_be(&payload[offset..offset + 8]);
            offset += 8;
            PropStorage::VRef(VRef {
                start_page: crate::types::PageId(start_page),
                n_pages,
                len,
                checksum,
                owner_commit,
            })
        }
        _ => {
            return Err(SombraError::Corruption(
                "unknown node property representation tag",
            ))
        }
    };
    let row_hash = if has_hash {
        if offset + 8 > payload.len() {
            return Err(SombraError::Corruption("node row hash truncated"));
        }
        let mut hash_bytes = [0u8; 8];
        hash_bytes.copy_from_slice(&payload[offset..offset + 8]);
        offset += 8;
        Some(u64::from_be_bytes(hash_bytes))
    } else {
        None
    };
    // Read adjacency page pointer if present (for IFA)
    let adj_page = if (header.flags & flags::HAS_ADJ_PAGE) != 0 {
        if offset + 8 > payload.len() {
            return Err(SombraError::Corruption("node adj page ptr truncated"));
        }
        let page_id = u64_from_be(&payload[offset..offset + 8]);
        offset += 8;
        Some(PageId(page_id))
    } else {
        None
    };
    // Read inline adjacency entries if present.
    let inline_adj = if (header.flags & flags::HAS_INLINE_ADJ) != 0 {
        if offset >= payload.len() {
            return Err(SombraError::Corruption("node inline adj length missing"));
        }
        let count = payload[offset] as usize;
        offset += 1;
        let needed = count
            .checked_mul(InlineAdjEntry::ENCODED_LEN)
            .ok_or(SombraError::Corruption("node inline adj entry count overflow"))?;
        if offset + needed > payload.len() {
            return Err(SombraError::Corruption("node inline adj entries truncated"));
        }
        let mut entries = Vec::with_capacity(count);
        for _ in 0..count {
            let start = offset;
            let end = start + InlineAdjEntry::ENCODED_LEN;
            entries.push(InlineAdjEntry::decode(&payload[start..end]));
            offset += InlineAdjEntry::ENCODED_LEN;
        }
        Some(InlineNodeAdj { entries })
    } else {
        None
    };
    let mut inline_history: Option<Vec<u8>> = None;
    if (header.flags & flags::INLINE_HISTORY) != 0 {
        if payload.len() - offset < 2 {
            return Err(SombraError::Corruption("inline history length missing"));
        }
        let len = u16::from_be_bytes(payload[offset..offset + 2].try_into().unwrap()) as usize;
        offset += 2;
        if offset + len > payload.len() {
            return Err(SombraError::Corruption("inline history payload truncated"));
        }
        inline_history = Some(payload[offset..offset + len].to_vec());
        offset += len;
        if offset != payload.len() {
            return Err(SombraError::Corruption(
                "node row trailing bytes after inline history",
            ));
        }
    }
    if (header.flags & flags::INLINE_HISTORY) != 0 && inline_history.is_none() {
        return Err(SombraError::Corruption(
            "inline history flag set but payload missing",
        ));
    }
    Ok(VersionedNodeRow {
        header,
        prev_ptr,
        row: NodeRow {
            labels,
            props,
            row_hash,
            adj_page,
            inline_adj,
        },
        inline_history,
    })
}

fn u64_from_be(bytes: &[u8]) -> u64 {
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(arr)
}

fn u32_from_be(bytes: &[u8]) -> u32 {
    let mut arr = [0u8; 4];
    arr.copy_from_slice(&bytes[..4]);
    u32::from_be_bytes(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    #[test]
    fn encode_decode_without_hash_roundtrip() -> Result<()> {
        let version = VersionHeader::new(7, COMMIT_MAX, 0, 0);
        let labels = [LabelId(7)];
        let props = PropPayload::Inline(b"inline-bytes");
        let encoded = encode(
            &labels,
            props,
            EncodeOpts::new(false),
            version,
            VersionPtr::null(),
            None,
        )?;
        assert!(encoded.row_hash.is_none());

        let decoded = decode(&encoded.bytes)?;
        assert_eq!(decoded.header.begin, 7);
        assert!(decoded.row.row_hash.is_none());
        assert_eq!(decoded.row.labels, labels);
        match decoded.row.props {
            PropStorage::Inline(bytes) => assert_eq!(bytes, b"inline-bytes"),
            _ => panic!("expected inline payload"),
        }
        Ok(())
    }

    #[test]
    fn encode_decode_with_hash_roundtrip() -> Result<()> {
        let version = VersionHeader::new(9, COMMIT_MAX, 0, 0);
        let labels = [LabelId(1), LabelId(2)];
        let props = PropPayload::Inline(b"hash-me");
        let encoded = encode(
            &labels,
            props,
            EncodeOpts::new(true),
            version,
            VersionPtr::null(),
            None,
        )?;
        let expected_hash = encoded.row_hash.expect("row hash present");

        let decoded = decode(&encoded.bytes)?;
        assert_eq!(decoded.header.begin, 9);
        assert_eq!(decoded.row.row_hash, Some(expected_hash));
        match decoded.row.props {
            PropStorage::Inline(bytes) => assert_eq!(bytes, b"hash-me"),
            _ => panic!("expected inline payload"),
        }
        Ok(())
    }

    #[test]
    fn header_reports_payload_length() -> Result<()> {
        let version = VersionHeader::new(4, COMMIT_MAX, 0, 0);
        let labels = [];
        let props = PropPayload::Inline(b"");
        let encoded = encode(
            &labels,
            props,
            EncodeOpts::new(false),
            version,
            VersionPtr::null(),
            None,
        )?;
        assert_eq!(
            encoded.header.payload_len as usize,
            encoded.bytes.len() - VERSION_HEADER_LEN - VERSION_PTR_LEN
        );
        Ok(())
    }

    proptest! {
        #[test]
        fn proptest_versioned_node_roundtrip(
            label_values in vec(0u32..200, 0..16),
            inline_bytes in vec(any::<u8>(), 0..512),
            append_hash in any::<bool>(),
            use_vref in any::<bool>(),
            (start_page, n_pages, len, checksum) in (any::<u64>(), 1u32..32, 1u32..4096, any::<u32>()),
            begin in any::<u64>(),
            lifetime in 0u32..2048,
            prev_raw in any::<u64>(),
            tombstone in any::<bool>(),
            pending in any::<bool>(),
        ) {
            let labels: Vec<LabelId> = label_values.into_iter().map(LabelId).collect();
            let end = if lifetime == 0 {
                COMMIT_MAX
            } else {
                begin.saturating_add(lifetime as u64 + 1)
            };
            let mut base_flags = 0;
            if tombstone {
                base_flags |= flags::TOMBSTONE;
            }
            if pending {
                base_flags |= flags::PENDING;
            }
            let version = VersionHeader::new(begin, end, base_flags, 0);
            let prev_ptr = VersionPtr::from_raw(prev_raw);
            let vref = VRef {
                start_page: crate::types::PageId(start_page),
                n_pages,
                len,
                checksum,
                owner_commit: begin,
            };
            let payload = if use_vref {
                PropPayload::VRef(vref)
            } else {
                PropPayload::Inline(&inline_bytes)
            };

            let encoded = encode(
                &labels,
                payload,
                EncodeOpts::new(append_hash),
                version,
                prev_ptr,
                None,
            ).expect("encode succeeds");
            let decoded = decode(&encoded.bytes).expect("decode succeeds");

            prop_assert_eq!(decoded.header, encoded.header);
            prop_assert_eq!(decoded.prev_ptr, prev_ptr);
            prop_assert_eq!(decoded.row.labels, labels);

            match (use_vref, decoded.row.props) {
                (false, PropStorage::Inline(bytes)) => prop_assert_eq!(bytes, inline_bytes),
                (true, PropStorage::VRef(observed)) => {
                    prop_assert_eq!(observed.start_page, vref.start_page);
                    prop_assert_eq!(observed.n_pages, vref.n_pages);
                    prop_assert_eq!(observed.len, vref.len);
                    prop_assert_eq!(observed.checksum, vref.checksum);
                }
                (false, PropStorage::VRef(_)) => prop_assert!(false, "expected inline props"),
                (true, PropStorage::Inline(_)) => prop_assert!(false, "expected vref props"),
            }

            prop_assert_eq!(decoded.row.row_hash, encoded.row_hash);
            prop_assert_eq!(decoded.row.row_hash.is_some(), append_hash);
        }
    }
}
