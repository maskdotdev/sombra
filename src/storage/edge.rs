use crate::types::{NodeId, Result, SombraError, TypeId, VRef};
use super::mvcc::{flags, VersionHeader, VersionPtr, VERSION_HEADER_LEN, VERSION_PTR_LEN};
#[cfg(test)]
use super::mvcc::COMMIT_MAX;

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

/// Edge payload plus MVCC metadata.
#[derive(Clone, Debug)]
pub struct VersionedEdgeRow {
    pub header: VersionHeader,
    pub prev_ptr: VersionPtr,
    pub row: EdgeRow,
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
    pub header: VersionHeader,
    pub prev_ptr: VersionPtr,
    #[cfg_attr(not(test), allow(dead_code))]
    pub row_hash: Option<u64>,
}

pub fn encode(
    src: NodeId,
    dst: NodeId,
    ty: TypeId,
    props: PropPayload<'_>,
    opts: EncodeOpts,
    mut version: VersionHeader,
    prev_ptr: VersionPtr,
) -> Result<EncodedEdgeRow> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&src.0.to_be_bytes());
    payload.extend_from_slice(&dst.0.to_be_bytes());
    payload.extend_from_slice(&ty.0.to_be_bytes());
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
        u16::try_from(payload.len()).map_err(|_| SombraError::Invalid("edge payload too large"))?;
    version.payload_len = payload_len;
    let mut bytes = Vec::with_capacity(VERSION_HEADER_LEN + VERSION_PTR_LEN + payload.len());
    version.encode_into(&mut bytes);
    bytes.extend_from_slice(&prev_ptr.to_bytes());
    bytes.extend_from_slice(&payload);
    Ok(EncodedEdgeRow {
        bytes,
        header: version,
        prev_ptr,
        row_hash,
    })
}

pub fn decode(data: &[u8]) -> Result<VersionedEdgeRow> {
    if data.len() < VERSION_HEADER_LEN + VERSION_PTR_LEN + 8 + 8 + 4 + 1 {
        return Err(SombraError::Corruption("edge row truncated"));
    }
    let header = VersionHeader::decode(data)?;
    if header.payload_len == 0 {
        return Err(SombraError::Corruption("edge row missing payload"));
    }
    let ptr_offset = VERSION_HEADER_LEN;
    let payload_offset = ptr_offset + VERSION_PTR_LEN;
    if data.len() < payload_offset {
        return Err(SombraError::Corruption("edge row missing version pointer"));
    }
    let prev_ptr = VersionPtr::from_bytes(&data[ptr_offset..payload_offset])?;
    let payload_end = payload_offset + header.payload_len as usize;
    if data.len() < payload_end {
        return Err(SombraError::Corruption("edge payload truncated"));
    }
    let payload = &data[payload_offset..payload_end];
    let mut offset = 0usize;
    let src = NodeId(u64_from_be(&payload[offset..offset + 8]));
    offset += 8;
    let dst = NodeId(u64_from_be(&payload[offset..offset + 8]));
    offset += 8;
    let ty = TypeId(u32_from_be(&payload[offset..offset + 4]));
    offset += 4;
    let tag = payload[offset];
    offset += 1;
    let has_hash = (tag & ROW_FLAG_HASH) != 0;
    let storage_kind = tag & ROW_STORAGE_MASK;
    let props = match storage_kind {
        ROW_STORAGE_INLINE => {
            if offset + 2 > payload.len() {
                return Err(SombraError::Corruption(
                    "edge inline property length missing",
                ));
            }
            let len = u16_from_be(&payload[offset..offset + 2]) as usize;
            offset += 2;
            if offset + len > payload.len() {
                return Err(SombraError::Corruption("edge inline property truncated"));
            }
            let value = payload[offset..offset + len].to_vec();
            offset += len;
            PropStorage::Inline(value)
        }
        ROW_STORAGE_VREF => {
            if offset + 20 > payload.len() {
                return Err(SombraError::Corruption("edge vref payload truncated"));
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
                "unknown edge property representation tag",
            ))
        }
    };
    let row_hash = if has_hash {
        if offset + 8 > payload.len() {
            return Err(SombraError::Corruption("edge row hash truncated"));
        }
        let mut hash_bytes = [0u8; 8];
        hash_bytes.copy_from_slice(&payload[offset..offset + 8]);
        Some(u64::from_be_bytes(hash_bytes))
    } else {
        None
    };
    Ok(VersionedEdgeRow {
        header,
        prev_ptr,
        row: EdgeRow {
            src,
            dst,
            ty,
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

fn u16_from_be(bytes: &[u8]) -> u16 {
    let mut arr = [0u8; 2];
    arr.copy_from_slice(&bytes[..2]);
    u16::from_be_bytes(arr)
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::collection::vec;
    use proptest::prelude::*;

    #[test]
    fn encode_decode_without_hash_roundtrip() -> Result<()> {
        let version = VersionHeader::new(11, COMMIT_MAX, 0, 0);
        let encoded = encode(
            NodeId(1),
            NodeId(2),
            TypeId(3),
            PropPayload::Inline(b"edge-bytes"),
            EncodeOpts::new(false),
            version,
            VersionPtr::null(),
        )?;
        assert!(encoded.row_hash.is_none());

        let decoded = decode(&encoded.bytes)?;
        assert_eq!(decoded.header.begin, 11);
        assert!(decoded.row.row_hash.is_none());
        match decoded.row.props {
            PropStorage::Inline(bytes) => assert_eq!(bytes, b"edge-bytes"),
            _ => panic!("expected inline payload"),
        }
        Ok(())
    }

    #[test]
    fn encode_decode_with_hash_roundtrip() -> Result<()> {
        let version = VersionHeader::new(13, COMMIT_MAX, 0, 0);
        let encoded = encode(
            NodeId(5),
            NodeId(6),
            TypeId(7),
            PropPayload::Inline(b"hashed-edge"),
            EncodeOpts::new(true),
            version,
            VersionPtr::null(),
        )?;
        let hash = encoded.row_hash.expect("row hash present");
        let decoded = decode(&encoded.bytes)?;
        assert_eq!(decoded.header.begin, 13);
        assert_eq!(decoded.row.row_hash, Some(hash));
        match decoded.row.props {
            PropStorage::Inline(bytes) => assert_eq!(bytes, b"hashed-edge"),
            _ => panic!("expected inline payload"),
        }
        Ok(())
    }

    #[test]
    fn edge_header_tracks_payload_length() -> Result<()> {
        let version = VersionHeader::new(21, COMMIT_MAX, 0, 0);
        let encoded = encode(
            NodeId(1),
            NodeId(1),
            TypeId(1),
            PropPayload::Inline(&[]),
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

    proptest! {
        #[test]
        fn proptest_versioned_edge_roundtrip(
            src_raw in any::<u64>(),
            dst_raw in any::<u64>(),
            ty_raw in any::<u32>(),
            inline_bytes in vec(any::<u8>(), 0..512),
            append_hash in any::<bool>(),
            use_vref in any::<bool>(),
            (start_page, n_pages, len, checksum) in (any::<u64>(), 1u32..32, 1u32..4096, any::<u32>()),
            begin in any::<u64>(),
            lifetime in 0u32..1024,
            prev_raw in any::<u64>(),
            tombstone in any::<bool>(),
            pending in any::<bool>(),
        ) {
            let src = NodeId(src_raw);
            let dst = NodeId(dst_raw);
            let ty = TypeId(ty_raw);
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
            };
            let payload = if use_vref {
                PropPayload::VRef(vref)
            } else {
                PropPayload::Inline(&inline_bytes)
            };

            let encoded = encode(
                src,
                dst,
                ty,
                payload,
                EncodeOpts::new(append_hash),
                version,
                prev_ptr,
            ).expect("encode succeeds");
            let decoded = decode(&encoded.bytes).expect("decode succeeds");

            prop_assert_eq!(decoded.header, encoded.header);
            prop_assert_eq!(decoded.prev_ptr, prev_ptr);
            prop_assert_eq!(decoded.row.src, src);
            prop_assert_eq!(decoded.row.dst, dst);
            prop_assert_eq!(decoded.row.ty, ty);

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
