use crate::db::group_commit::TxId;
use crate::error::Result;
use crate::model::{EdgeId, NodeId};
use crate::pager::PageId;
use crate::storage::header::Header;

#[derive(Debug, Clone)]
pub struct HeaderState {
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
    pub max_timestamp: u64,
    pub oldest_snapshot_ts: u64,
}

impl From<Header> for HeaderState {
    fn from(header: Header) -> Self {
        Self {
            next_node_id: header.next_node_id,
            next_edge_id: header.next_edge_id,
            free_page_head: header.free_page_head,
            last_record_page: header.last_record_page,
            last_committed_tx_id: header.last_committed_tx_id,
            btree_index_page: header.btree_index_page,
            btree_index_size: header.btree_index_size,
            property_index_root_page: header.property_index_root_page,
            property_index_count: header.property_index_count,
            property_index_version: header.property_index_version,
            max_timestamp: header.max_timestamp,
            oldest_snapshot_ts: header.oldest_snapshot_ts,
        }
    }
}

impl HeaderState {
    pub fn to_header(&self, page_size: usize) -> Result<Header> {
        let mut header = Header::new(page_size)?;
        header.next_node_id = self.next_node_id;
        header.next_edge_id = self.next_edge_id;
        header.free_page_head = self.free_page_head;
        header.last_record_page = self.last_record_page;
        header.last_committed_tx_id = self.last_committed_tx_id;
        header.btree_index_page = self.btree_index_page;
        header.btree_index_size = self.btree_index_size;
        header.property_index_root_page = self.property_index_root_page;
        header.property_index_count = self.property_index_count;
        header.property_index_version = self.property_index_version;
        header.max_timestamp = self.max_timestamp;
        header.oldest_snapshot_ts = self.oldest_snapshot_ts;
        Ok(header)
    }
}
