
use std::convert::TryFrom;
use crate::error::{GraphError, Result};
use crate::index::BTreeIndex;
use crate::pager::PageId;
use crate::storage::page::RecordPage;
use crate::storage::record::{RECORD_HEADER_SIZE, RecordHeader, RecordKind};
use crate::storage::{deserialize_edge, deserialize_node, RecordPointer};
use super::graphdb::GraphDB;

impl GraphDB {
    pub(crate) fn load_btree_index(&mut self) -> Result<bool> {
        let (index_page, index_size) = match (self.header.btree_index_page, self.header.btree_index_size) {
            (Some(page), size) if size > 0 => (page, size as usize),
            _ => return Ok(false),
        };

        let mut data = Vec::new();
        let page_size = self.pager.page_size();
        let mut current_page = index_page;
        let mut bytes_read = 0;

        while bytes_read < index_size {
            let page = self.pager.fetch_page(current_page)?;
            let to_read = (index_size - bytes_read).min(page_size);
            data.extend_from_slice(&page.data[..to_read]);
            bytes_read += to_read;
            current_page += 1;
        }

        self.node_index = BTreeIndex::deserialize(&data)?;
        Ok(true)
    }

    pub(crate) fn persist_btree_index(&mut self) -> Result<()> {
        let data = self.node_index.serialize()?;
        let data_size = data.len();
        
        if data_size == 0 {
            self.header.btree_index_page = None;
            self.header.btree_index_size = 0;
            return Ok(());
        }

        let page_size = self.pager.page_size();
        let pages_needed = data_size.div_ceil(page_size);
        
        let start_page = if let Some(old_page) = self.header.btree_index_page {
            let old_size = self.header.btree_index_size as usize;
            let old_pages = old_size.div_ceil(page_size);
            
            if pages_needed <= old_pages {
                old_page
            } else {
                for i in 0..old_pages {
                    self.push_free_page(old_page + i as u32)?;
                }
                self.pager.allocate_page()?
            }
        } else {
            self.pager.allocate_page()?
        };

        let mut offset = 0;
        for i in 0..pages_needed {
            let page_id = if i == 0 {
                start_page
            } else {
                self.pager.allocate_page()?
            };
            
            let page = self.pager.fetch_page(page_id)?;
            let to_write = (data_size - offset).min(page_size);
            page.data[..to_write].copy_from_slice(&data[offset..offset + to_write]);
            if to_write < page_size {
                page.data[to_write..].fill(0);
            }
            page.dirty = true;
            self.record_page_write(page_id);
            offset += to_write;
        }

        self.header.btree_index_page = Some(start_page);
        self.header.btree_index_size = data_size as u32;
        Ok(())
    }

    pub(crate) fn rebuild_indexes(&mut self) -> Result<()> {
        if let Some(index_page) = self.header.btree_index_page {
            if self.try_load_btree_index(index_page, self.header.btree_index_size as usize).is_ok() {
                self.rebuild_remaining_indexes()?;
                return Ok(());
            }
        }

        self.node_index.clear();
        self.edge_index.clear();
        self.label_index.clear();
        self.node_cache.clear();
        self.outgoing_adjacency.clear();
        self.incoming_adjacency.clear();
        self.outgoing_neighbors_cache.clear();
        self.incoming_neighbors_cache.clear();

        let mut last_record_page: Option<PageId> = None;
        let mut max_node_id = 0;
        let mut max_edge_id = 0;
        let page_count = self.pager.page_count();
        for page_idx in 1..page_count {
            let page_id = PageId::try_from(page_idx)
                .map_err(|_| GraphError::Corruption("page index exceeds u32::MAX".into()))?;
            let page = self.pager.fetch_page(page_id)?;
            let record_page = RecordPage::from_bytes(&mut page.data)?;
            let record_count = record_page.record_count()? as usize;
            if record_count == 0 {
                continue;
            }
            let mut live_on_page = 0usize;
            for slot in 0..record_count {
                let byte_offset = record_page.record_offset(slot)?;
                let pointer = RecordPointer {
                    page_id,
                    slot_index: slot as u16,
                    byte_offset,
                };
                let record = record_page.record_slice(slot)?;
                let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
                let payload_len = header.payload_length as usize;
                let payload = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
                match header.kind {
                    RecordKind::Free => continue,
                    RecordKind::Node => {
                        let node = deserialize_node(payload)?;
                        max_node_id = max_node_id.max(node.id);
                        self.node_index.insert(node.id, pointer);
                        
                        for label in &node.labels {
                            self.label_index
                                .entry(label.clone())
                                .or_default()
                                .insert(node.id);
                        }
                        
                        live_on_page += 1;
                    }
                    RecordKind::Edge => {
                        let edge = deserialize_edge(payload)?;
                        max_edge_id = max_edge_id.max(edge.id);
                        self.edge_index.insert(edge.id, pointer);
                        
                        self.outgoing_adjacency
                            .entry(edge.source_node_id)
                            .or_default()
                            .push(edge.id);
                        self.incoming_adjacency
                            .entry(edge.target_node_id)
                            .or_default()
                            .push(edge.id);
                        
                        live_on_page += 1;
                    }
                }
            }
            if live_on_page > 0 {
                last_record_page = Some(page_id);
            }
        }

        if max_node_id >= self.header.next_node_id {
            self.header.next_node_id = max_node_id + 1;
        }
        if max_edge_id >= self.header.next_edge_id {
            self.header.next_edge_id = max_edge_id + 1;
        }
        self.header.last_record_page = last_record_page;
        
        self.populate_neighbors_cache()?;
        Ok(())
    }

    fn try_load_btree_index(&mut self, start_page: PageId, size: usize) -> Result<()> {
        let mut data = Vec::with_capacity(size);
        let page_size = self.pager.page_size();
        let pages_needed = size.div_ceil(page_size);
        
        for i in 0..pages_needed {
            let page_id = start_page + i as u32;
            let page = self.pager.fetch_page(page_id)?;
            let bytes_to_copy = (size - data.len()).min(page_size);
            data.extend_from_slice(&page.data[..bytes_to_copy]);
        }
        
        self.node_index = BTreeIndex::deserialize(&data)?;
        Ok(())
    }

    fn rebuild_remaining_indexes(&mut self) -> Result<()> {
        self.edge_index.clear();
        self.label_index.clear();
        self.node_cache.clear();
        self.outgoing_adjacency.clear();
        self.incoming_adjacency.clear();
        self.outgoing_neighbors_cache.clear();
        self.incoming_neighbors_cache.clear();

        let mut last_record_page: Option<PageId> = None;
        let mut max_edge_id = 0;
        let page_count = self.pager.page_count();
        for page_idx in 1..page_count {
            let page_id = PageId::try_from(page_idx)
                .map_err(|_| GraphError::Corruption("page index exceeds u32::MAX".into()))?;
            let page = self.pager.fetch_page(page_id)?;
            let record_page = RecordPage::from_bytes(&mut page.data)?;
            let record_count = record_page.record_count()? as usize;
            if record_count == 0 {
                continue;
            }
            let mut live_on_page = 0usize;
            for slot in 0..record_count {
                let byte_offset = record_page.record_offset(slot)?;
                let pointer = RecordPointer {
                    page_id,
                    slot_index: slot as u16,
                    byte_offset,
                };
                let record = record_page.record_slice(slot)?;
                let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
                let payload_len = header.payload_length as usize;
                let payload = &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len];
                match header.kind {
                    RecordKind::Free => continue,
                    RecordKind::Node => {
                        let node = deserialize_node(payload)?;
                        
                        for label in &node.labels {
                            self.label_index
                                .entry(label.clone())
                                .or_default()
                                .insert(node.id);
                        }
                        
                        live_on_page += 1;
                    }
                    RecordKind::Edge => {
                        let edge = deserialize_edge(payload)?;
                        max_edge_id = max_edge_id.max(edge.id);
                        self.edge_index.insert(edge.id, pointer);
                        
                        self.outgoing_adjacency
                            .entry(edge.source_node_id)
                            .or_default()
                            .push(edge.id);
                        self.incoming_adjacency
                            .entry(edge.target_node_id)
                            .or_default()
                            .push(edge.id);
                        
                        live_on_page += 1;
                    }
                }
            }
            if live_on_page > 0 {
                last_record_page = Some(page_id);
            }
        }

        if max_edge_id >= self.header.next_edge_id {
            self.header.next_edge_id = max_edge_id + 1;
        }
        self.header.last_record_page = last_record_page;
        
        self.populate_neighbors_cache()?;
        Ok(())
    }
    
    fn populate_neighbors_cache(&mut self) -> Result<()> {
        let outgoing_clone = self.outgoing_adjacency.clone();
        for (node_id, edge_ids) in outgoing_clone {
            let mut neighbors = Vec::with_capacity(edge_ids.len());
            for edge_id in edge_ids {
                let edge = self.load_edge(edge_id)?;
                neighbors.push(edge.target_node_id);
            }
            self.outgoing_neighbors_cache.insert(node_id, neighbors);
        }
        
        let incoming_clone = self.incoming_adjacency.clone();
        for (node_id, edge_ids) in incoming_clone {
            let mut neighbors = Vec::with_capacity(edge_ids.len());
            for edge_id in edge_ids {
                let edge = self.load_edge(edge_id)?;
                neighbors.push(edge.source_node_id);
            }
            self.incoming_neighbors_cache.insert(node_id, neighbors);
        }
        
        Ok(())
    }
}
