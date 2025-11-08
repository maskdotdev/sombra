use crate::types::{LabelId, Result, SombraError, VRef};

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
    #[cfg_attr(not(test), allow(dead_code))]
    pub row_hash: Option<u64>,
}

pub fn encode(
    labels: &[LabelId],
    props: PropPayload<'_>,
    opts: EncodeOpts,
) -> Result<EncodedNodeRow> {
    if labels.len() > u8::MAX as usize {
        return Err(SombraError::Invalid(
            "too many labels for inline node encoding",
        ));
    }
    let mut buf = Vec::new();
    buf.push(labels.len() as u8);
    for label in labels {
        buf.extend_from_slice(&label.0.to_be_bytes());
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
            buf.push(tag);
            buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        PropPayload::VRef(vref) => {
            let mut tag = ROW_STORAGE_VREF;
            if opts.append_row_hash {
                tag |= ROW_FLAG_HASH;
            }
            buf.push(tag);
            buf.extend_from_slice(&vref.start_page.0.to_be_bytes());
            buf.extend_from_slice(&vref.n_pages.to_be_bytes());
            buf.extend_from_slice(&vref.len.to_be_bytes());
            buf.extend_from_slice(&vref.checksum.to_be_bytes());
        }
    }
    let row_hash = if opts.append_row_hash {
        let hash = row_hash64(&buf);
        buf.extend_from_slice(&hash.to_be_bytes());
        Some(hash)
    } else {
        None
    };
    Ok(EncodedNodeRow {
        bytes: buf,
        row_hash,
    })
}

pub fn decode(data: &[u8]) -> Result<NodeRow> {
    if data.is_empty() {
        return Err(SombraError::Corruption("node row truncated"));
    }
    let label_count = data[0] as usize;
    let mut offset = 1usize;
    if data.len() < offset + label_count * 4 + 1 {
        return Err(SombraError::Corruption("node labels truncated"));
    }
    let mut labels = Vec::with_capacity(label_count);
    for _ in 0..label_count {
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&data[offset..offset + 4]);
        offset += 4;
        labels.push(LabelId(u32::from_be_bytes(arr)));
    }
    if offset >= data.len() {
        return Err(SombraError::Corruption("node property tag missing"));
    }
    let tag = data[offset];
    offset += 1;
    let has_hash = (tag & ROW_FLAG_HASH) != 0;
    let storage_kind = tag & ROW_STORAGE_MASK;
    let props = match storage_kind {
        ROW_STORAGE_INLINE => {
            if offset + 2 > data.len() {
                return Err(SombraError::Corruption(
                    "node inline property length missing",
                ));
            }
            let mut len_buf = [0u8; 2];
            len_buf.copy_from_slice(&data[offset..offset + 2]);
            let len = u16::from_be_bytes(len_buf) as usize;
            offset += 2;
            if offset + len > data.len() {
                return Err(SombraError::Corruption(
                    "node inline property payload truncated",
                ));
            }
            PropStorage::Inline(data[offset..offset + len].to_vec())
        }
        ROW_STORAGE_VREF => {
            if offset + 20 > data.len() {
                return Err(SombraError::Corruption("node vref payload truncated"));
            }
            let start_page = u64_from_be(&data[offset..offset + 8]);
            offset += 8;
            let n_pages = u32_from_be(&data[offset..offset + 4]);
            offset += 4;
            let len = u32_from_be(&data[offset..offset + 4]);
            offset += 4;
            let checksum = u32_from_be(&data[offset..offset + 4]);
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
        if offset + 8 > data.len() {
            return Err(SombraError::Corruption("node row hash truncated"));
        }
        let mut hash_bytes = [0u8; 8];
        hash_bytes.copy_from_slice(&data[offset..offset + 8]);
        Some(u64::from_be_bytes(hash_bytes))
    } else {
        None
    };
    Ok(NodeRow {
        labels,
        props,
        row_hash,
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
        let labels = [LabelId(7)];
        let props = PropPayload::Inline(b"inline-bytes");
        let encoded = encode(&labels, props, EncodeOpts::new(false))?;
        assert!(encoded.row_hash.is_none());

        let row = decode(&encoded.bytes)?;
        assert_eq!(row.labels, labels);
        assert!(row.row_hash.is_none());
        match row.props {
            PropStorage::Inline(bytes) => assert_eq!(bytes, b"inline-bytes"),
            _ => panic!("expected inline payload"),
        }
        Ok(())
    }

    #[test]
    fn encode_decode_with_hash_roundtrip() -> Result<()> {
        let labels = [LabelId(1), LabelId(2)];
        let props = PropPayload::Inline(b"hash-me");
        let encoded = encode(&labels, props, EncodeOpts::new(true))?;
        let expected_hash = encoded.row_hash.expect("row hash present");

        let row = decode(&encoded.bytes)?;
        assert_eq!(row.row_hash, Some(expected_hash));
        match row.props {
            PropStorage::Inline(bytes) => assert_eq!(bytes, b"hash-me"),
            _ => panic!("expected inline payload"),
        }
        Ok(())
    }
}
