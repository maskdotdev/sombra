use super::graphdb::GraphDB;
use super::property_index_persistence::PropertyIndexSerializer;
use crate::error::{GraphError, Result};
use crate::index::{BTreeIndex, VersionedIndexEntries};
use crate::pager::{PageId, PAGE_CHECKSUM_SIZE};
use crate::storage::page::RecordPage;
use crate::storage::record::{RecordHeader, RecordKind, RECORD_HEADER_SIZE};
use crate::storage::{deserialize_edge, deserialize_node, RecordPointer};
use std::sync::{Arc, Mutex};
use tracing::{info, warn};

impl GraphDB {
    pub(crate) fn load_btree_index(&self) -> Result<bool> {
        let (index_page, index_size) = {
            let header = self.header.lock().unwrap();
            match (header.btree_index_page, header.btree_index_size) {
                (Some(page), size) if size > 0 => (page, size as usize),
                _ => return Ok(false),
            }
        };

        let mut data = Vec::new();
        let page_size = self.pager.page_size();
        // Reserve space for page checksum (last 4 bytes of each page)
        let usable_page_size = page_size - PAGE_CHECKSUM_SIZE;
        let mut current_page = index_page;
        let mut bytes_read = 0;

        while bytes_read < index_size {
            let to_read = self.pager.with_pager_write(|pager| {
                let page = pager.fetch_page(current_page)?;
                // Read only from the usable area, not the checksum
                let to_read = (index_size - bytes_read).min(usable_page_size);
                data.extend_from_slice(&page.data[..to_read]);
                Ok(to_read)
            })?;
            bytes_read += to_read;
            current_page += 1;
        }

        self.node_index
            .replace_with(BTreeIndex::deserialize(&data)?);
        
        // After loading the node index, rebuild edges and other indices
        self.rebuild_remaining_indexes()?;
        Ok(true)
    }

    pub(crate) fn persist_btree_index(&self) -> Result<()> {
        let data = self.node_index.serialize()?;
        let data_size = data.len();

        if data_size == 0 {
            let mut header = self.header.lock().unwrap();
            header.btree_index_page = None;
            header.btree_index_size = 0;
            return Ok(());
        }

        let page_size = self.pager.page_size();
        // Reserve space for page checksum (last 4 bytes of each page)
        let usable_page_size = page_size - PAGE_CHECKSUM_SIZE;
        let pages_needed = data_size.div_ceil(usable_page_size);

        let start_page = {
            let header = self.header.lock().unwrap();
            if let Some(old_page) = header.btree_index_page {
                let old_size = header.btree_index_size as usize;
                let old_pages = old_size.div_ceil(usable_page_size);

                if pages_needed <= old_pages {
                    // Reuse existing pages, but clear any pages we won't be using
                    if pages_needed < old_pages {
                        drop(header); // Release lock before clearing pages
                        for i in pages_needed..old_pages {
                            let page_to_clear = old_page + i as u32;
                            self.pager.with_pager_write(|pager| {
                                let page = pager.fetch_page(page_to_clear)?;
                                page.data.fill(0);
                                page.dirty = true;
                                Ok(())
                            })?;
                            self.push_free_page(page_to_clear)?;
                        }
                    }
                    old_page
                } else {
                    drop(header); // Release lock before allocation
                    for i in 0..old_pages {
                        self.push_free_page(old_page + i as u32)?;
                    }
                    let start = self.pager.with_pager_write(|pager| pager.allocate_page())?;
                    for i in 1..pages_needed {
                        let expected_page = start + i as u32;
                        let allocated =
                            self.pager.with_pager_write(|pager| pager.allocate_page())?;
                        if allocated != expected_page {
                            return Err(GraphError::Corruption(format!(
                                "Expected contiguous page allocation: got {allocated}, expected {expected_page}"
                            )));
                        }
                    }
                    start
                }
            } else {
                drop(header); // Release lock before allocation
                let new_page = self.pager.with_pager_write(|pager| pager.allocate_page())?;
                for i in 1..pages_needed {
                    let expected_page = new_page + i as u32;
                    let allocated = self.pager.with_pager_write(|pager| pager.allocate_page())?;
                    if allocated != expected_page {
                        return Err(GraphError::Corruption(format!(
                            "Expected contiguous page allocation: got {allocated}, expected {expected_page}"
                        )));
                    }
                }
                new_page
            }
        };

        let mut offset = 0;
        for i in 0..pages_needed {
            let page_id = start_page + i as u32;
            // Use usable_page_size to avoid overwriting the checksum area
            let to_write = (data_size - offset).min(usable_page_size);

            self.pager.with_pager_write(|pager| {
                let page = pager.fetch_page(page_id)?;
                page.data[..to_write].copy_from_slice(&data[offset..offset + to_write]);
                if to_write < usable_page_size {
                    // Zero out the rest of the usable area (checksum will be added later)
                    page.data[to_write..usable_page_size].fill(0);
                }
                page.dirty = true;
                Ok(())
            })?;
            self.record_page_write(page_id);
            offset += to_write;
        }

        let mut header = self.header.lock().unwrap();
        header.btree_index_page = Some(start_page);
        header.btree_index_size = data_size as u32;
        Ok(())
    }

    pub(crate) fn persist_property_indexes(&self) -> Result<()> {
        // Convert nested DashMap structure to HashMap/BTreeMap for serialization
        // Structure: DashMap<(label, key), Arc<DashMap<IndexableValue, Arc<Mutex<VersionedIndexEntries>>>>>
        let indexes_snapshot: std::collections::HashMap<_, _> = self
            .property_indexes
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let inner_map = entry.value();

                // Convert to BTreeMap for ordered serialization
                let converted_map: std::collections::BTreeMap<_, Vec<(RecordPointer, u64, Option<u64>)>> =
                    inner_map
                        .iter()
                        .map(|inner_entry| {
                            let idx_val = inner_entry.key().clone();
                            let entries_arc = inner_entry.value();
                            let entries = entries_arc.lock().unwrap();
                            
                            // Serialize each IndexEntry as (pointer, commit_ts, delete_ts)
                            let entry_tuples: Vec<(RecordPointer, u64, Option<u64>)> = entries
                                .entries()
                                .iter()
                                .map(|e| (e.pointer, e.commit_ts, e.delete_ts))
                                .collect();
                            
                            (idx_val, entry_tuples)
                        })
                        .collect();

                (key, converted_map)
            })
            .collect();

        let (root_page, count, written_pages, old_pages) =
            self.pager.with_pager_write(|pager| {
                let mut serializer = PropertyIndexSerializer::new(pager);
                let (root_page, count, written_pages) =
                    serializer.serialize_indexes(&indexes_snapshot)?;

                if root_page == 0 {
                    return Ok((0, 0, Vec::new(), Vec::new()));
                }

                let old_pages = {
                    let header = self.header.lock().unwrap();
                    if let Some(old_root) = header.property_index_root_page {
                        serializer.collect_old_pages(old_root)?
                    } else {
                        Vec::new()
                    }
                };

                Ok((root_page, count, written_pages, old_pages))
            })?;

        if root_page == 0 {
            let mut header = self.header.lock().unwrap();
            header.property_index_root_page = None;
            header.property_index_count = 0;
            return Ok(());
        }

        // Free old pages if any
        for page_id in old_pages {
            self.push_free_page(page_id)?;
        }

        for page_id in written_pages {
            self.record_page_write(page_id);
        }

        let mut header = self.header.lock().unwrap();
        header.property_index_root_page = Some(root_page);
        header.property_index_count = count;
        header.property_index_version = 1;

        info!(root_page, count, "Persisted property indexes");

        Ok(())
    }

    pub(crate) fn load_property_indexes(&self) -> Result<bool> {
        let root_page = {
            let header = self.header.lock().unwrap();
            match header.property_index_root_page {
                Some(page) if page > 0 => page,
                _ => return Ok(false),
            }
        };

        match self.pager.with_pager_write(|pager| {
            let mut serializer = PropertyIndexSerializer::new(pager);
            serializer.deserialize_indexes(root_page)
        }) {
            Ok(indexes) => {
                // Convert HashMap to DashMap
                self.property_indexes.clear();
                for (key, value) in indexes {
                    // Convert HashMap<IndexableValue, Vec<(pointer, commit_ts, delete_ts)>> to 
                    // DashMap<IndexableValue, Arc<Mutex<VersionedIndexEntries>>>
                    let dash_value = Arc::new(dashmap::DashMap::new());
                    for (idx_val, entry_tuples) in value {
                        let mut versioned_entries = VersionedIndexEntries::new();
                        for (pointer, commit_ts, delete_ts) in entry_tuples {
                            // Reconstruct IndexEntry - need to create with correct delete_ts
                            // Can't use add_entry + update_delete_ts_for_pointer because that only works
                            // when updating from Some(0) to actual timestamp, not from None
                            if let Some(dt) = delete_ts {
                                // Entry was deleted - use add_deleted_entry
                                versioned_entries.add_deleted_entry(pointer, commit_ts, dt);
                            } else {
                                // Entry is still active
                                versioned_entries.add_entry(pointer, commit_ts);
                            }
                        }
                        dash_value.insert(idx_val, Arc::new(Mutex::new(versioned_entries)));
                    }
                    self.property_indexes.insert(key, dash_value);
                }
                info!(
                    count = self.property_indexes.len(),
                    "Loaded property indexes from disk"
                );
                Ok(true)
            }
            Err(e) => {
                warn!(
                    error = ?e,
                    "Failed to load property indexes, will rebuild"
                );
                Ok(false)
            }
        }
    }

    pub(crate) fn rebuild_indexes(&self) -> Result<()> {
        let (index_page, index_size) = {
            let header = self.header.lock().unwrap();
            (header.btree_index_page, header.btree_index_size as usize)
        };

        if let Some(index_page) = index_page {
            if self.try_load_btree_index(index_page, index_size).is_ok() {
                self.rebuild_remaining_indexes()?;
                return Ok(());
            }
        }

        self.node_index.clear();
        self.edge_index.clear();
        self.label_index.clear();
        self.property_indexes.clear();
        self.node_cache.clear();
        self.outgoing_adjacency.clear();
        self.incoming_adjacency.clear();
        self.outgoing_neighbors_cache.clear();
        self.incoming_neighbors_cache.clear();

        let mut last_record_page: Option<PageId> = None;
        let mut max_node_id = 0;
        let mut max_edge_id = 0;
        let page_count = self.pager.page_count();

        let btree_pages: std::collections::HashSet<PageId> = {
            let header = self.header.lock().unwrap();
            if let Some(btree_start) = header.btree_index_page {
                let btree_size = header.btree_index_size as usize;
                let page_size = self.pager.page_size();
                let btree_page_count = btree_size.div_ceil(page_size);
                (btree_start..btree_start + btree_page_count as u32).collect()
            } else {
                std::collections::HashSet::new()
            }
        };

        // Collect property index pages to skip
        let property_index_pages: std::collections::HashSet<PageId> = {
            let header = self.header.lock().unwrap();
            if let Some(root_page) = header.property_index_root_page {
                match self.pager.with_pager_write(|pager| {
                    let mut serializer = PropertyIndexSerializer::new(pager);
                    serializer.collect_old_pages(root_page)
                }) {
                    Ok(pages) => pages.into_iter().collect(),
                    Err(_) => std::collections::HashSet::new(),
                }
            } else {
                std::collections::HashSet::new()
            }
        };

        for page_idx in 1..page_count {
            let page_id = PageId::try_from(page_idx)
                .map_err(|_| GraphError::Corruption("page index exceeds u32::MAX".into()))?;

            if btree_pages.contains(&page_id) || property_index_pages.contains(&page_id) {
                continue;
            }

            let (record_count, records_data) = self.pager.with_pager_write(|pager| {
                let page = pager.fetch_page(page_id)?;
                
                // Try to parse as RecordPage; skip if it's an index page
                let record_page = match RecordPage::from_bytes(&mut page.data) {
                    Ok(rp) => rp,
                    Err(GraphError::InvalidArgument(_)) => {
                        // This is an index page with magic bytes, skip it
                        return Ok((0, Vec::new()));
                    }
                    Err(e) => return Err(e),
                };
                
                let record_count = record_page.record_count()? as usize;
                if record_count == 0 {
                    return Ok((0, Vec::new()));
                }

                // Collect all record data we need
                let mut records = Vec::new();
                for slot in 0..record_count {
                    let byte_offset = record_page.record_offset(slot)?;
                    let record = record_page.record_slice(slot)?.to_vec();
                    records.push((slot, byte_offset, record));
                }
                Ok((record_count, records))
            })?;

            if record_count == 0 {
                continue;
            }

            let mut live_on_page = 0usize;
            for (slot, byte_offset, record) in records_data {
                let pointer = RecordPointer {
                    page_id,
                    slot_index: slot as u16,
                    byte_offset,
                };
                
                // Skip records that don't have enough data for a header
                if record.len() < RECORD_HEADER_SIZE {
                    continue;
                }
                
                // Try to parse record header; skip malformed records during rebuild
                let header = match RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE]) {
                    Ok(h) => h,
                    Err(_) => continue, // Skip malformed records
                };
                let payload_len = header.payload_length as usize;

                // Check if this is a versioned record by looking at the actual kind byte
                let kind_byte = record[0];
                let is_versioned = kind_byte == 0x03 || kind_byte == 0x04; // VersionedNode or VersionedEdge

                let payload = if is_versioned {
                    // For versioned records, skip the 25-byte metadata header
                    const VERSION_METADATA_SIZE: usize = 25;
                    if payload_len < VERSION_METADATA_SIZE {
                        continue; // Skip malformed versioned records
                    }
                    let data_start = RECORD_HEADER_SIZE + VERSION_METADATA_SIZE;
                    let data_end = RECORD_HEADER_SIZE + payload_len;
                    &record[data_start..data_end]
                } else {
                    // Legacy non-versioned record
                    &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len]
                };

                match header.kind {
                    RecordKind::Free => continue,
                    RecordKind::Node => {
                        let node = deserialize_node(payload)?;
                        max_node_id = max_node_id.max(node.id);
                        self.node_index.insert(node.id, pointer);

                        for label in &node.labels {
                            let label_map = self.label_index
                                .entry(label.clone())
                                .or_insert_with(dashmap::DashMap::new);
                            let entries = label_map
                                .entry(node.id)
                                .or_insert_with(|| Arc::new(Mutex::new(VersionedIndexEntries::new())));
                            entries.lock().unwrap().add_entry(pointer, 0); // commit_ts=0 for rebuild
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

        {
            let mut header = self.header.lock().unwrap();
            if max_node_id >= header.next_node_id {
                header.next_node_id = max_node_id + 1;
            }
            if max_edge_id >= header.next_edge_id {
                header.next_edge_id = max_edge_id + 1;
            }
            header.last_record_page = last_record_page;
        }

        self.populate_neighbors_cache()?;
        Ok(())
    }

    fn try_load_btree_index(&self, start_page: PageId, size: usize) -> Result<()> {
        let mut data = Vec::with_capacity(size);
        let page_size = self.pager.page_size();
        let pages_needed = size.div_ceil(page_size);

        for i in 0..pages_needed {
            let page_id = start_page + i as u32;
            self.pager.with_pager_write(|pager| {
                let page = pager.fetch_page(page_id)?;
                let bytes_to_copy = (size - data.len()).min(page_size);
                data.extend_from_slice(&page.data[..bytes_to_copy]);
                Ok(())
            })?;
        }

        self.node_index
            .replace_with(BTreeIndex::deserialize(&data)?);
        Ok(())
    }

    fn rebuild_remaining_indexes(&self) -> Result<()> {
        self.edge_index.clear();
        self.label_index.clear();
        self.property_indexes.clear();
        self.node_cache.clear();
        self.outgoing_adjacency.clear();
        self.incoming_adjacency.clear();
        self.outgoing_neighbors_cache.clear();
        self.incoming_neighbors_cache.clear();

        let mut last_record_page: Option<PageId> = None;
        let mut max_edge_id = 0;
        let page_count = self.pager.page_count();

        let btree_pages: std::collections::HashSet<PageId> = {
            let header = self.header.lock().unwrap();
            if let Some(btree_start) = header.btree_index_page {
                let btree_size = header.btree_index_size as usize;
                let page_size = self.pager.page_size();
                let btree_page_count = btree_size.div_ceil(page_size);
                (btree_start..btree_start + btree_page_count as u32).collect()
            } else {
                std::collections::HashSet::new()
            }
        };

        // Collect property index pages to skip
        let property_index_pages: std::collections::HashSet<PageId> = {
            let header = self.header.lock().unwrap();
            if let Some(root_page) = header.property_index_root_page {
                match self.pager.with_pager_write(|pager| {
                    let mut serializer = PropertyIndexSerializer::new(pager);
                    serializer.collect_old_pages(root_page)
                }) {
                    Ok(pages) => pages.into_iter().collect(),
                    Err(_) => std::collections::HashSet::new(),
                }
            } else {
                std::collections::HashSet::new()
            }
        };

        for page_idx in 1..page_count {
            let page_id = PageId::try_from(page_idx)
                .map_err(|_| GraphError::Corruption("page index exceeds u32::MAX".into()))?;

            if btree_pages.contains(&page_id) || property_index_pages.contains(&page_id) {
                continue;
            }

            let (record_count, records_data) = self.pager.with_pager_write(|pager| {
                let page = pager.fetch_page(page_id)?;
                
                // Try to parse as RecordPage; skip if it's an index page
                let record_page = match RecordPage::from_bytes(&mut page.data) {
                    Ok(rp) => rp,
                    Err(GraphError::InvalidArgument(_)) => {
                        // This is an index page with magic bytes, skip it
                        return Ok((0, Vec::new()));
                    }
                    Err(e) => return Err(e),
                };
                
                let record_count = record_page.record_count()? as usize;
                if record_count == 0 {
                    return Ok((0, Vec::new()));
                }

                // Collect all record data we need
                let mut records = Vec::new();
                for slot in 0..record_count {
                    let byte_offset = record_page.record_offset(slot)?;
                    let record = record_page.record_slice(slot)?.to_vec();
                    records.push((slot, byte_offset, record));
                }
                Ok((record_count, records))
            })?;

            if record_count == 0 {
                continue;
            }

            let mut live_on_page = 0usize;
            for (slot, byte_offset, record) in records_data {
                let pointer = RecordPointer {
                    page_id,
                    slot_index: slot as u16,
                    byte_offset,
                };
                
                // Skip records that don't have enough data for a header
                if record.len() < RECORD_HEADER_SIZE {
                    continue;
                }
                
                // Try to parse record header; skip malformed records during rebuild
                let header = match RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE]) {
                    Ok(h) => h,
                    Err(_) => continue, // Skip malformed records
                };
                let payload_len = header.payload_length as usize;

                // Check if this is a versioned record by looking at the actual kind byte
                let kind_byte = record[0];
                let is_versioned = kind_byte == 0x03 || kind_byte == 0x04; // VersionedNode or VersionedEdge

                let payload = if is_versioned {
                    // For versioned records, skip the 25-byte metadata header
                    const VERSION_METADATA_SIZE: usize = 25;
                    if payload_len < VERSION_METADATA_SIZE {
                        continue; // Skip malformed versioned records
                    }
                    let data_start = RECORD_HEADER_SIZE + VERSION_METADATA_SIZE;
                    let data_end = RECORD_HEADER_SIZE + payload_len;
                    &record[data_start..data_end]
                } else {
                    // Legacy non-versioned record
                    &record[RECORD_HEADER_SIZE..RECORD_HEADER_SIZE + payload_len]
                };

                match header.kind {
                    RecordKind::Free => continue,
                    RecordKind::Node => {
                        let node = deserialize_node(payload)?;

                        for label in &node.labels {
                            let label_map = self.label_index
                                .entry(label.clone())
                                .or_insert_with(dashmap::DashMap::new);
                            let entries = label_map
                                .entry(node.id)
                                .or_insert_with(|| Arc::new(Mutex::new(VersionedIndexEntries::new())));
                            entries.lock().unwrap().add_entry(pointer, 0); // commit_ts=0 for rebuild
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

        {
            let mut header = self.header.lock().unwrap();
            if max_edge_id >= header.next_edge_id {
                header.next_edge_id = max_edge_id + 1;
            }
            header.last_record_page = last_record_page;
        }

        self.populate_neighbors_cache()?;
        Ok(())
    }

    fn populate_neighbors_cache(&self) -> Result<()> {
        for entry in self.outgoing_adjacency.iter() {
            let node_id = *entry.key();
            let edge_ids = entry.value();
            let mut neighbors = Vec::with_capacity(edge_ids.len());
            for &edge_id in edge_ids.iter() {
                let edge = self.load_edge(edge_id)?;
                neighbors.push(edge.target_node_id);
            }
            self.outgoing_neighbors_cache.insert(node_id, neighbors);
        }

        for entry in self.incoming_adjacency.iter() {
            let node_id = *entry.key();
            let edge_ids = entry.value();
            let mut neighbors = Vec::with_capacity(edge_ids.len());
            for &edge_id in edge_ids.iter() {
                let edge = self.load_edge(edge_id)?;
                neighbors.push(edge.source_node_id);
            }
            self.incoming_neighbors_cache.insert(node_id, neighbors);
        }

        Ok(())
    }
}
