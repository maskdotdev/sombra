use sombra_types::{NodeId, Result, SombraError, TypeId, VRef};

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
}

pub fn encode(src: NodeId, dst: NodeId, ty: TypeId, props: PropPayload<'_>) -> Result<Vec<u8>> {
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
            buf.push(0);
            buf.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
            buf.extend_from_slice(bytes);
        }
        PropPayload::VRef(vref) => {
            buf.push(1);
            buf.extend_from_slice(&vref.start_page.0.to_be_bytes());
            buf.extend_from_slice(&vref.n_pages.to_be_bytes());
            buf.extend_from_slice(&vref.len.to_be_bytes());
            buf.extend_from_slice(&vref.checksum.to_be_bytes());
        }
    }
    Ok(buf)
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
    let props = match tag {
        0 => {
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
        1 => {
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
                start_page: sombra_types::PageId(start_page),
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
    Ok(EdgeRow {
        src,
        dst,
        ty,
        props,
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
