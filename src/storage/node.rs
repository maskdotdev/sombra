use crate::types::{LabelId, Result, SombraError, VRef};

use super::mvcc::{
    flags, VersionHeader, VersionPtr, COMMIT_MAX, VERSION_HEADER_LEN, VERSION_PTR_LEN,
};

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

#[derive(Clone, Debug)]
pub struct NodeRow {
    pub labels: Vec<LabelId>,
    pub props: PropStorage,
    #[cfg_attr(not(test), allow(dead_code))]
    pub row_hash: Option<u64>,
}

/// Node payload paired with its MVCC metadata.
#[derive(Clone, Debug)]
pub struct VersionedNodeRow {
    pub header: VersionHeader,
    pub prev_ptr: VersionPtr,
    pub row: NodeRow,
}

/// Encoding options for node rows.
#[derive(Clone, Copy, Debug, Default)]
pub struct EncodeOpts {
    /// Whether to append an 8-byte SipHash64 footer.
    pub append_row_hash: bool,
}

impl EncodeOpts {
    pub const fn new(append_row_hash: bool) -> Self {
        Self { append_row_hash }
    }
}

/// Result of node row encoding, exposing the encoded bytes and optional hash.
#[derive(Clone, Debug)]
pub struct EncodedNodeRow {
    pub bytes: Vec<u8>,
    pub header: VersionHeader,
    pub prev_ptr: VersionPtr,
    #[cfg_attr(not(test), allow(dead_code))]
    pub row_hash: Option<u64>,
}

pub fn encode(
    labels: &[LabelId],
    props: PropPayload<'_>,
    opts: EncodeOpts,
    mut version: VersionHeader,
    prev_ptr: VersionPtr,
) -> Result<EncodedNodeRow> {
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
        }
    }
    let row_hash = if opts.append_row_hash {
        let hash = row_hash64(&payload);
        payload.extend_from_slice(&hash.to_be_bytes());
        Some(hash)
    } else {
        None
    };
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
            if offset + 20 > payload.len() {
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
            PropStorage::VRef(VRef {
                start_page: crate::types::PageId(start_page),
                n_pages,
                len,
                checksum,
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
        Some(u64::from_be_bytes(hash_bytes))
    } else {
        None
    };
    Ok(VersionedNodeRow {
        header,
        prev_ptr,
        row: NodeRow {
            labels,
            props,
            row_hash,
        },
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
        )?;
        assert_eq!(
            encoded.header.payload_len as usize,
            encoded.bytes.len() - VERSION_HEADER_LEN - VERSION_PTR_LEN
        );
        Ok(())
    }
}
