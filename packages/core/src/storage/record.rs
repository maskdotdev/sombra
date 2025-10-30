use crate::error::{GraphError, Result};
use std::convert::TryInto;

pub const RECORD_HEADER_SIZE: usize = 8;
pub const MAX_RECORD_SIZE: usize = 16 * 1024 * 1024;

#[repr(u8)]
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum RecordKind {
    Free = 0x00,
    Node = 0x01,
    Edge = 0x02,
}

impl RecordKind {
    pub fn from_byte(byte: u8) -> Result<Self> {
        match byte {
            0x00 => Ok(Self::Free),
            0x01 => Ok(Self::Node),
            0x02 => Ok(Self::Edge),
            other => Err(GraphError::Corruption(format!(
                "unknown record kind: 0x{other:02X}"
            ))),
        }
    }

    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct RecordHeader {
    pub kind: RecordKind,
    pub payload_length: u32,
}

impl RecordHeader {
    pub fn new(kind: RecordKind, payload_length: u32) -> Self {
        Self {
            kind,
            payload_length,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < RECORD_HEADER_SIZE {
            return Err(GraphError::Corruption("record header truncated".into()));
        }

        // Use lenient parsing: accept versioned kinds (0x03, 0x04) and map them to base kinds
        // This allows RecordHeader to work with both legacy and versioned records
        let kind = match bytes[0] {
            0x00 => RecordKind::Free,
            0x01 | 0x03 => RecordKind::Node, // 0x03 = VersionedNode
            0x02 | 0x04 => RecordKind::Edge, // 0x04 = VersionedEdge
            other => {
                return Err(GraphError::Corruption(format!(
                    "unknown record kind: 0x{other:02X}"
                )))
            }
        };

        let payload_length_bytes: [u8; 4] = bytes[4..8]
            .try_into()
            .map_err(|_| GraphError::Corruption("record header length slice invalid".into()))?;
        let payload_length = u32::from_le_bytes(payload_length_bytes);
        let payload_length_usize = usize::try_from(payload_length).map_err(|_| {
            GraphError::Corruption("record payload length exceeds platform limits".into())
        })?;
        if payload_length_usize > MAX_RECORD_SIZE {
            return Err(GraphError::Corruption(format!(
                "record payload length {payload_length_usize} exceeds maximum {MAX_RECORD_SIZE}"
            )));
        }
        Ok(Self {
            kind,
            payload_length,
        })
    }

    pub fn write_to(&self, bytes: &mut [u8]) -> Result<()> {
        if bytes.len() < RECORD_HEADER_SIZE {
            return Err(GraphError::Corruption(
                "destination slice shorter than header".into(),
            ));
        }
        bytes[0] = self.kind.to_byte();
        bytes[1..4].fill(0);
        bytes[4..8].copy_from_slice(&self.payload_length.to_le_bytes());
        Ok(())
    }
}

pub fn encode_record(kind: RecordKind, payload: &[u8]) -> Result<Vec<u8>> {
    if payload.len() > MAX_RECORD_SIZE {
        return Err(GraphError::InvalidArgument(format!(
            "record payload exceeds maximum size of {MAX_RECORD_SIZE} bytes"
        )));
    }
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        GraphError::InvalidArgument("record payload length does not fit into u32".into())
    })?;

    let mut buffer = Vec::with_capacity(RECORD_HEADER_SIZE + payload.len());
    buffer.push(kind.to_byte());
    buffer.extend_from_slice(&[0; 3]);
    buffer.extend_from_slice(&payload_len.to_le_bytes());
    buffer.extend_from_slice(payload);
    Ok(buffer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_rejects_excessive_payload_length() {
        let mut bytes = vec![0u8; RECORD_HEADER_SIZE];
        bytes[0] = RecordKind::Node.to_byte();
        let too_large = (MAX_RECORD_SIZE as u32).saturating_add(1);
        bytes[4..8].copy_from_slice(&too_large.to_le_bytes());
        let err =
            RecordHeader::from_bytes(&bytes).expect_err("header should reject oversized payload");
        match err {
            GraphError::Corruption(message) => {
                assert!(
                    message.contains("exceeds maximum"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected corruption error, got {other:?}"),
        }
    }

    #[test]
    fn encode_record_rejects_large_payload() {
        let payload = vec![0u8; MAX_RECORD_SIZE + 1];
        let err = encode_record(RecordKind::Node, &payload)
            .expect_err("encode should reject oversized payload");
        match err {
            GraphError::InvalidArgument(message) => {
                assert!(
                    message.contains("exceeds maximum size"),
                    "unexpected message: {message}"
                );
            }
            other => panic!("expected invalid argument error, got {other:?}"),
        }
    }
}
