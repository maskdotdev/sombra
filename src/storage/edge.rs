use crate::types::{NodeId, Result, SombraError, TypeId, VRef};

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
pub struct EdgeRow {
    pub src: NodeId,
    pub dst: NodeId,
    pub ty: TypeId,
    pub props: PropStorage,
    #[cfg_attr(not(test), allow(dead_code))]
    pub row_hash: Option<u64>,
}

/// Encoding options for edge rows.
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

/// Result of encoding an edge row.
#[derive(Clone, Debug)]
pub struct EncodedEdgeRow {
    pub bytes: Vec<u8>,
    #[cfg_attr(not(test), allow(dead_code))]
    pub row_hash: Option<u64>,
}

pub fn encode(
    src: NodeId,
    dst: NodeId,
    ty: TypeId,
    props: PropPayload<'_>,
    opts: EncodeOpts,
) -> Result<EncodedEdgeRow> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&src.0.to_be_bytes());
    buf.extend_from_slice(&dst.0.to_be_bytes());
    buf.extend_from_slice(&ty.0.to_be_bytes());
    match props {
        PropPayload::Inline(bytes) => {
            if bytes.len() > u16::MAX as usize {
                return Err(SombraError::Invalid(
                    "inline edge property blob exceeds u16 length",
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
    Ok(EncodedEdgeRow {
        bytes: buf,
        row_hash,
    })
}

pub fn decode(data: &[u8]) -> Result<EdgeRow> {
    if data.len() < 8 + 8 + 4 + 1 {
        return Err(SombraError::Corruption("edge row truncated"));
    }
    let mut offset = 0usize;
    let src = NodeId(u64_from_be(&data[offset..offset + 8]));
    offset += 8;
    let dst = NodeId(u64_from_be(&data[offset..offset + 8]));
    offset += 8;
    let ty = TypeId(u32_from_be(&data[offset..offset + 4]));
    offset += 4;
    let tag = data[offset];
    offset += 1;
    let has_hash = (tag & ROW_FLAG_HASH) != 0;
    let storage_kind = tag & ROW_STORAGE_MASK;
    let props = match storage_kind {
        ROW_STORAGE_INLINE => {
            if offset + 2 > data.len() {
                return Err(SombraError::Corruption(
                    "edge inline property length missing",
                ));
            }
            let len = u16_from_be(&data[offset..offset + 2]) as usize;
            offset += 2;
            if offset + len > data.len() {
                return Err(SombraError::Corruption("edge inline property truncated"));
            }
            PropStorage::Inline(data[offset..offset + len].to_vec())
        }
        ROW_STORAGE_VREF => {
            if offset + 20 > data.len() {
                return Err(SombraError::Corruption("edge vref payload truncated"));
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
                "unknown edge property representation tag",
            ))
        }
    };
    let row_hash = if has_hash {
        if offset + 8 > data.len() {
            return Err(SombraError::Corruption("edge row hash truncated"));
        }
        let mut hash_bytes = [0u8; 8];
        hash_bytes.copy_from_slice(&data[offset..offset + 8]);
        Some(u64::from_be_bytes(hash_bytes))
    } else {
        None
    };
    Ok(EdgeRow {
        src,
        dst,
        ty,
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

fn u16_from_be(bytes: &[u8]) -> u16 {
    let mut arr = [0u8; 2];
    arr.copy_from_slice(&bytes[..2]);
    u16::from_be_bytes(arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_without_hash_roundtrip() -> Result<()> {
        let encoded = encode(
            NodeId(1),
            NodeId(2),
            TypeId(3),
            PropPayload::Inline(b"edge-bytes"),
            EncodeOpts::new(false),
        )?;
        assert!(encoded.row_hash.is_none());

        let row = decode(&encoded.bytes)?;
        assert!(row.row_hash.is_none());
        match row.props {
            PropStorage::Inline(bytes) => assert_eq!(bytes, b"edge-bytes"),
            _ => panic!("expected inline payload"),
        }
        Ok(())
    }

    #[test]
    fn encode_decode_with_hash_roundtrip() -> Result<()> {
        let encoded = encode(
            NodeId(5),
            NodeId(6),
            TypeId(7),
            PropPayload::Inline(b"hashed-edge"),
            EncodeOpts::new(true),
        )?;
        let hash = encoded.row_hash.expect("row hash present");
        let row = decode(&encoded.bytes)?;
        assert_eq!(row.row_hash, Some(hash));
        match row.props {
            PropStorage::Inline(bytes) => assert_eq!(bytes, b"hashed-edge"),
            _ => panic!("expected inline payload"),
        }
        Ok(())
    }
}
