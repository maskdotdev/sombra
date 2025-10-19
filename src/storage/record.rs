use crate::error::{GraphError, Result};

pub const RECORD_HEADER_SIZE: usize = 8;

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
        let kind = RecordKind::from_byte(bytes[0])?;
        let payload_length =
            u32::from_le_bytes(bytes[4..8].try_into().expect("slice has exactly 4 bytes"));
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

pub fn encode_record(kind: RecordKind, payload: &[u8]) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(RECORD_HEADER_SIZE + payload.len());
    buffer.push(kind.to_byte());
    buffer.extend_from_slice(&[0; 3]);
    buffer.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    buffer.extend_from_slice(payload);
    buffer
}
