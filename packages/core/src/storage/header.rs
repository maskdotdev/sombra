use std::convert::TryInto;

use crate::db::TxId;
use crate::error::{GraphError, Result};
use crate::model::{EdgeId, NodeId};
use crate::pager::PageId;

const MAGIC: &[u8; 8] = b"GRPHITE\0";
const HEADER_REGION_SIZE: usize = 96;
const VERSION_MAJOR: u16 = 1;
const VERSION_MINOR: u16 = 3;

#[derive(Debug, Clone)]
pub struct Header {
    pub page_size: u32,
    pub next_node_id: NodeId,
    pub next_edge_id: EdgeId,
    pub free_page_head: Option<PageId>,
    pub last_record_page: Option<PageId>,
    pub last_committed_tx_id: TxId,
    pub btree_index_page: Option<PageId>,
    pub btree_index_size: u32,
    pub property_index_root_page: Option<PageId>,
    pub property_index_count: u32,
    pub property_index_version: u16,
    // MVCC fields (version 1.3+) - always enabled
    pub max_timestamp: u64,
    pub oldest_snapshot_ts: u64,
}

impl Header {
    pub fn new(page_size: usize) -> Result<Self> {
        let page_size_u32 = u32::try_from(page_size)
            .map_err(|_| GraphError::InvalidArgument("page size exceeds u32::MAX".into()))?;
        Ok(Self {
            page_size: page_size_u32,
            next_node_id: 1,
            next_edge_id: 1,
            free_page_head: None,
            last_record_page: None,
            last_committed_tx_id: 0,
            btree_index_page: None,
            btree_index_size: 0,
            property_index_root_page: None,
            property_index_count: 0,
            property_index_version: 1,
            max_timestamp: 0,
            oldest_snapshot_ts: 0,
        })
    }

    pub fn read(data: &[u8]) -> Result<Option<Self>> {
        if data.len() < HEADER_REGION_SIZE {
            return Err(GraphError::Corruption(
                "header page shorter than expected".into(),
            ));
        }

        if data[..MAGIC.len()].iter().all(|&b| b == 0) {
            return Ok(None);
        }

        if &data[..MAGIC.len()] != MAGIC {
            return Err(GraphError::Corruption(
                "invalid graphite header magic".into(),
            ));
        }

        let major = u16::from_le_bytes([data[8], data[9]]);
        let _minor = u16::from_le_bytes([data[10], data[11]]);
        if major != VERSION_MAJOR {
            return Err(GraphError::Corruption(format!(
                "unsupported header major version {major} (expected {VERSION_MAJOR})"
            )));
        }

        let page_size = u32::from_le_bytes([data[12], data[13], data[14], data[15]]);
        let next_node_id = Self::read_u64(data, 16, 24)?;
        let next_edge_id = Self::read_u64(data, 24, 32)?;
        let free_page_head = u32::from_le_bytes([data[32], data[33], data[34], data[35]]);
        let last_record_page = u32::from_le_bytes([data[36], data[37], data[38], data[39]]);
        let last_committed_tx_id = Self::read_u64(data, 40, 48)?;

        let btree_index_page = if data.len() >= 56 {
            let page = u32::from_le_bytes([data[48], data[49], data[50], data[51]]);
            if page == 0 {
                None
            } else {
                Some(page)
            }
        } else {
            None
        };
        let btree_index_size = if data.len() >= 56 {
            u32::from_le_bytes([data[52], data[53], data[54], data[55]])
        } else {
            0
        };

        let property_index_root_page = if data.len() >= 64 {
            let page = u32::from_le_bytes([data[56], data[57], data[58], data[59]]);
            if page == 0 {
                None
            } else {
                Some(page)
            }
        } else {
            None
        };

        let property_index_count = if data.len() >= 64 {
            u32::from_le_bytes([data[60], data[61], data[62], data[63]])
        } else {
            0
        };

        let property_index_version = if data.len() >= 66 {
            u16::from_le_bytes([data[64], data[65]])
        } else {
            0
        };

        // MVCC fields (version 1.3+) - always enabled now
        let max_timestamp = if data.len() >= 75 {
            Self::read_u64(data, 67, 75)?
        } else {
            0
        };

        let oldest_snapshot_ts = if data.len() >= 83 {
            Self::read_u64(data, 75, 83)?
        } else {
            0
        };

        Ok(Some(Self {
            page_size,
            next_node_id,
            next_edge_id,
            free_page_head: if free_page_head == 0 {
                None
            } else {
                Some(free_page_head)
            },
            last_record_page: if last_record_page == 0 {
                None
            } else {
                Some(last_record_page)
            },
            last_committed_tx_id,
            btree_index_page,
            btree_index_size,
            property_index_root_page,
            property_index_count,
            property_index_version,
            max_timestamp,
            oldest_snapshot_ts,
        }))
    }

    pub fn write(&self, data: &mut [u8]) -> Result<()> {
        if data.len() < HEADER_REGION_SIZE {
            return Err(GraphError::Corruption(
                "header page shorter than expected".into(),
            ));
        }

        data.fill(0);
        data[..MAGIC.len()].copy_from_slice(MAGIC);
        data[8..10].copy_from_slice(&VERSION_MAJOR.to_le_bytes());
        data[10..12].copy_from_slice(&VERSION_MINOR.to_le_bytes());
        data[12..16].copy_from_slice(&self.page_size.to_le_bytes());
        data[16..24].copy_from_slice(&self.next_node_id.to_le_bytes());
        data[24..32].copy_from_slice(&self.next_edge_id.to_le_bytes());
        data[32..36].copy_from_slice(&self.free_page_head.unwrap_or(0).to_le_bytes());
        data[36..40].copy_from_slice(&self.last_record_page.unwrap_or(0).to_le_bytes());
        data[40..48].copy_from_slice(&self.last_committed_tx_id.to_le_bytes());
        data[48..52].copy_from_slice(&self.btree_index_page.unwrap_or(0).to_le_bytes());
        data[52..56].copy_from_slice(&self.btree_index_size.to_le_bytes());
        data[56..60].copy_from_slice(&self.property_index_root_page.unwrap_or(0).to_le_bytes());
        data[60..64].copy_from_slice(&self.property_index_count.to_le_bytes());
        data[64..66].copy_from_slice(&self.property_index_version.to_le_bytes());

        // MVCC fields (version 1.3+) - always enabled
        // Reserve byte 66 for future use (was mvcc_enabled flag)
        data[66] = 1; // Always 1 to indicate MVCC is enabled
        data[67..75].copy_from_slice(&self.max_timestamp.to_le_bytes());
        data[75..83].copy_from_slice(&self.oldest_snapshot_ts.to_le_bytes());

        Ok(())
    }

    fn read_u64(data: &[u8], start: usize, end: usize) -> Result<u64> {
        let slice = data
            .get(start..end)
            .ok_or_else(|| GraphError::Corruption("header u64 field truncated".into()))?;
        let bytes: [u8; 8] = slice
            .try_into()
            .map_err(|_| GraphError::Corruption("failed to parse u64 from header".into()))?;
        Ok(u64::from_le_bytes(bytes))
    }
}
