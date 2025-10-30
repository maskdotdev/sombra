//! MVCC WAL Frame Support
//!
//! This module extends the standard WAL format with MVCC-specific frame
//! metadata including snapshot and commit timestamps.
//!
//! # WAL Frame Layout
//!
//! Standard frame (24 bytes):
//! ```
//! [page_id: 4][frame_number: 4][checksum: 4][tx_id: 8][flags: 4]
//! ```
//!
//! MVCC-extended frame (40 bytes):
//! ```
//! [page_id: 4][frame_number: 4][checksum: 4][tx_id: 8][flags: 4]
//! [snapshot_ts: 8][commit_ts: 8]
//! ```

use crate::error::{GraphError, Result};
use crate::pager::PageId;

/// Size of MVCC-extended frame header (24 + 16 = 40 bytes)
pub const MVCC_FRAME_HEADER_SIZE: usize = 40;

/// Extended WAL frame header with MVCC metadata
#[derive(Debug, Clone, Copy)]
pub struct MvccWalFrameHeader {
    /// Page ID (4 bytes)
    pub page_id: PageId,
    /// Frame number (4 bytes)
    pub frame_number: u32,
    /// Checksum (4 bytes)
    pub checksum: u32,
    /// Transaction ID (8 bytes)
    pub tx_id: u64,
    /// Flags (4 bytes)
    pub flags: u32,
    /// Snapshot timestamp (8 bytes)
    pub snapshot_ts: u64,
    /// Commit timestamp (8 bytes, 0 if not committed)
    pub commit_ts: u64,
}

impl MvccWalFrameHeader {
    /// Creates a new MVCC frame header
    pub fn new(
        page_id: PageId,
        frame_number: u32,
        checksum: u32,
        tx_id: u64,
        flags: u32,
        snapshot_ts: u64,
        commit_ts: u64,
    ) -> Self {
        Self {
            page_id,
            frame_number,
            checksum,
            tx_id,
            flags,
            snapshot_ts,
            commit_ts,
        }
    }

    /// Encodes the frame header into bytes
    pub fn encode(&self) -> [u8; MVCC_FRAME_HEADER_SIZE] {
        let mut buf = [0u8; MVCC_FRAME_HEADER_SIZE];
        
        // Standard header (24 bytes)
        buf[0..4].copy_from_slice(&self.page_id.to_le_bytes());
        buf[4..8].copy_from_slice(&self.frame_number.to_le_bytes());
        buf[8..12].copy_from_slice(&self.checksum.to_le_bytes());
        buf[12..20].copy_from_slice(&self.tx_id.to_le_bytes());
        buf[20..24].copy_from_slice(&self.flags.to_le_bytes());
        
        // MVCC extension (16 bytes)
        buf[24..32].copy_from_slice(&self.snapshot_ts.to_le_bytes());
        buf[32..40].copy_from_slice(&self.commit_ts.to_le_bytes());
        
        buf
    }

    /// Decodes a frame header from bytes
    pub fn decode(data: &[u8]) -> Result<Self> {
        if data.len() < MVCC_FRAME_HEADER_SIZE {
            return Err(GraphError::Corruption(
                "WAL frame header too short for MVCC extension".into(),
            ));
        }

        let page_id = Self::read_u32_le(data, 0)?;
        let frame_number = Self::read_u32_le(data, 4)?;
        let checksum = Self::read_u32_le(data, 8)?;
        let tx_id = Self::read_u64_le(data, 12)?;
        let flags = Self::read_u32_le(data, 20)?;
        let snapshot_ts = Self::read_u64_le(data, 24)?;
        let commit_ts = Self::read_u64_le(data, 32)?;

        Ok(Self {
            page_id,
            frame_number,
            checksum,
            tx_id,
            flags,
            snapshot_ts,
            commit_ts,
        })
    }

    fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
        let end = offset
            .checked_add(4)
            .ok_or_else(|| GraphError::Corruption("u32 read offset overflow".into()))?;
        let slice = buf.get(offset..end).ok_or_else(|| {
            GraphError::Corruption(format!("Invalid u32 at MVCC WAL header offset {offset}"))
        })?;
        let bytes: [u8; 4] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to copy u32 bytes from MVCC WAL header".into())
        })?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
        let end = offset
            .checked_add(8)
            .ok_or_else(|| GraphError::Corruption("u64 read offset overflow".into()))?;
        let slice = buf.get(offset..end).ok_or_else(|| {
            GraphError::Corruption(format!("Invalid u64 at MVCC WAL header offset {offset}"))
        })?;
        let bytes: [u8; 8] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to copy u64 bytes from MVCC WAL header".into())
        })?;
        Ok(u64::from_le_bytes(bytes))
    }
}

/// Flag indicating this WAL file uses MVCC frame format
pub const WAL_FLAG_MVCC: u32 = 0x2;

/// Checks if a frame flag indicates MVCC format
pub fn is_mvcc_frame(flags: u32) -> bool {
    (flags & WAL_FLAG_MVCC) != 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_frame_header_encoding() {
        let header = MvccWalFrameHeader::new(
            1,      // page_id
            2,      // frame_number
            0x12345678,  // checksum
            100,    // tx_id
            0x1,    // flags
            200,    // snapshot_ts
            201,    // commit_ts
        );

        let encoded = header.encode();
        let decoded = MvccWalFrameHeader::decode(&encoded).unwrap();

        assert_eq!(decoded.page_id, 1);
        assert_eq!(decoded.frame_number, 2);
        assert_eq!(decoded.checksum, 0x12345678);
        assert_eq!(decoded.tx_id, 100);
        assert_eq!(decoded.flags, 0x1);
        assert_eq!(decoded.snapshot_ts, 200);
        assert_eq!(decoded.commit_ts, 201);
    }

    #[test]
    fn test_is_mvcc_frame() {
        assert!(is_mvcc_frame(0x2));
        assert!(is_mvcc_frame(0x3)); // COMMIT + MVCC
        assert!(!is_mvcc_frame(0x1)); // Just COMMIT
        assert!(!is_mvcc_frame(0x0));
    }
}
