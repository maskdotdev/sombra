use sombra_types::{LabelId, Result, SombraError, VRef};

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
}

pub fn encode(labels: &[LabelId], props: PropPayload<'_>) -> Result<Vec<u8>> {
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
    let props = match tag {
        0 => {
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
        1 => {
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
                start_page: sombra_types::PageId(start_page),
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
    Ok(NodeRow { labels, props })
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
