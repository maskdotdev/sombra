use crate::error::{GraphError, Result};
use crate::pager::{PageId, Pager, PAGE_CHECKSUM_SIZE};
use crate::storage::page::RecordPage;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RecordPointer {
    pub page_id: PageId,
    pub slot_index: u16,
    pub byte_offset: u16,
}

pub struct RecordStore<'a> {
    pager: &'a mut Pager,
}

impl<'a> RecordStore<'a> {
    pub fn new(pager: &'a mut Pager) -> Self {
        Self { pager }
    }

    pub fn insert(
        &mut self,
        record: &[u8],
        preferred_page: Option<PageId>,
    ) -> Result<RecordPointer> {
        if let Some(page_id) = preferred_page {
            if let Some(pointer) = self.try_insert_into_page(page_id, record)? {
                return Ok(pointer);
            }
        }

        let page_id = self.pager.allocate_page()?;
        let page = self.pager.fetch_page(page_id)?;
        let mut record_page = RecordPage::from_bytes(&mut page.data)?;
        record_page.initialize()?;
        if !record_page.can_fit(record.len())? {
            return Err(GraphError::InvalidArgument(
                "newly allocated page cannot fit record".into(),
            ));
        }
        let slot = record_page.append_record(record)?;
        let byte_offset = record_page.record_offset(slot as usize)?;
        page.dirty = true;
        Ok(RecordPointer {
            page_id,
            slot_index: slot,
            byte_offset,
        })
    }

    pub fn visit_record<F, T>(&mut self, pointer: RecordPointer, mut f: F) -> Result<T>
    where
        F: FnMut(&[u8]) -> Result<T>,
    {
        let page = self.pager.fetch_page(pointer.page_id)?;
        let offset = pointer.byte_offset as usize;
        let payload_end = page
            .data
            .len()
            .checked_sub(PAGE_CHECKSUM_SIZE)
            .ok_or_else(|| GraphError::Corruption("page too small for checksum".into()))?;
        if offset >= payload_end {
            return Err(GraphError::Corruption(
                "record offset beyond payload".into(),
            ));
        }
        let slice = &page.data[offset..payload_end];
        if slice.len() < 8 {
            return Err(GraphError::Corruption("record header truncated".into()));
        }
        let payload_len = u32::from_le_bytes([slice[4], slice[5], slice[6], slice[7]]) as usize;
        let record_len = 8 + payload_len;
        if slice.len() < record_len {
            return Err(GraphError::Corruption("record extends past page".into()));
        }
        f(&slice[..record_len])
    }

    pub fn visit_record_mut<F, T>(&mut self, pointer: RecordPointer, mut f: F) -> Result<T>
    where
        F: FnMut(&mut [u8]) -> Result<T>,
    {
        let page = self.pager.fetch_page(pointer.page_id)?;
        let offset = pointer.byte_offset as usize;
        let payload_end = page
            .data
            .len()
            .checked_sub(PAGE_CHECKSUM_SIZE)
            .ok_or_else(|| GraphError::Corruption("page too small for checksum".into()))?;
        if offset >= payload_end {
            return Err(GraphError::Corruption(
                "record offset beyond payload".into(),
            ));
        }
        let slice = &mut page.data[offset..payload_end];
        if slice.len() < 8 {
            return Err(GraphError::Corruption("record header truncated".into()));
        }
        let payload_len = u32::from_le_bytes([slice[4], slice[5], slice[6], slice[7]]) as usize;
        let record_len = 8 + payload_len;
        if record_len > slice.len() {
            return Err(GraphError::Corruption("record extends past page".into()));
        }
        let result = f(&mut slice[..record_len])?;
        page.dirty = true;
        Ok(result)
    }

    pub fn try_insert_into_page(
        &mut self,
        page_id: PageId,
        record: &[u8],
    ) -> Result<Option<RecordPointer>> {
        let page = self.pager.fetch_page(page_id)?;
        let mut record_page = RecordPage::from_bytes(&mut page.data)?;
        record_page.initialize()?;
        let slot_count = record_page.record_count()? as usize;
        for slot in 0..slot_count {
            if record_page.try_reuse_slot(slot, record)? {
                let byte_offset = record_page.record_offset(slot)?;
                page.dirty = true;
                return Ok(Some(RecordPointer {
                    page_id,
                    slot_index: slot as u16,
                    byte_offset,
                }));
            }
        }
        if record_page.can_fit(record.len())? {
            let slot = record_page.append_record(record)?;
            let byte_offset = record_page.record_offset(slot as usize)?;
            page.dirty = true;
            Ok(Some(RecordPointer {
                page_id,
                slot_index: slot,
                byte_offset,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn mark_free(&mut self, pointer: RecordPointer) -> Result<bool> {
        let page = self.pager.fetch_page(pointer.page_id)?;
        let mut record_page = RecordPage::from_bytes(&mut page.data)?;
        record_page.mark_slot_free(pointer.slot_index as usize)?;
        let live = record_page.live_record_count()?;
        page.dirty = true;
        Ok(live == 0)
    }

    pub fn update_in_place(
        &mut self,
        pointer: RecordPointer,
        new_record: &[u8],
    ) -> Result<Option<RecordPointer>> {
        let page = self.pager.fetch_page(pointer.page_id)?;
        let mut record_page = RecordPage::from_bytes(&mut page.data)?;
        
        if record_page.try_update_slot(pointer.slot_index as usize, new_record)? {
            page.dirty = true;
            let byte_offset = record_page.record_offset(pointer.slot_index as usize)?;
            Ok(Some(RecordPointer {
                page_id: pointer.page_id,
                slot_index: pointer.slot_index,
                byte_offset,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_page_fragmentation(&mut self, page_id: PageId) -> Result<f64> {
        let page = self.pager.fetch_page(page_id)?;
        let record_page = RecordPage::from_bytes(&mut page.data)?;
        
        let record_count = record_page.record_count()? as usize;
        if record_count == 0 {
            return Ok(0.0);
        }
        
        let mut free_records = 0;
        let mut total_wasted_space = 0;
        
        for idx in 0..record_count {
            let header = record_page.record_header_at(idx)?;
            if header.kind == crate::storage::record::RecordKind::Free {
                free_records += 1;
                // Wasted space includes the header and payload
                total_wasted_space += crate::storage::record::RECORD_HEADER_SIZE + header.payload_length as usize;
            }
        }
        
        if free_records == 0 {
            return Ok(0.0);
        }
        
        // Calculate fragmentation as percentage of wasted space vs page capacity
        let page_size = record_page.page_size()? as usize;
        let fragmentation = (total_wasted_space as f64 / page_size as f64) * 100.0;
        
        Ok(fragmentation)
    }

    pub fn compact_page(&mut self, page_id: PageId) -> Result<usize> {
        // Collect all live records from the page
        let live_records: Vec<Vec<u8>> = {
            let page = self.pager.fetch_page(page_id)?;
            let record_page = RecordPage::from_bytes(&mut page.data)?;
            let record_count = record_page.record_count()? as usize;
            
            let mut records = Vec::new();
            for idx in 0..record_count {
                let header = record_page.record_header_at(idx)?;
                if header.kind != crate::storage::record::RecordKind::Free {
                    let slice = record_page.record_slice(idx)?;
                    records.push(slice.to_vec());
                }
            }
            records
        };
        
        // If no live records, the page can be cleared
        if live_records.is_empty() {
            let page = self.pager.fetch_page(page_id)?;
            let mut record_page = RecordPage::from_bytes(&mut page.data)?;
            let bytes_before = record_page.available_space()?;
            record_page.clear()?;
            record_page.initialize()?;
            page.dirty = true;
            let bytes_after = record_page.available_space()?;
            return Ok(bytes_after.saturating_sub(bytes_before));
        }
        
        // Calculate space before compaction
        let bytes_before = {
            let page = self.pager.fetch_page(page_id)?;
            let record_page = RecordPage::from_bytes(&mut page.data)?;
            record_page.available_space()?
        };
        
        // Clear the page and rewrite all live records
        {
            let page = self.pager.fetch_page(page_id)?;
            let mut record_page = RecordPage::from_bytes(&mut page.data)?;
            record_page.clear()?;
            record_page.initialize()?;
            
            for record in &live_records {
                if !record_page.can_fit(record.len())? {
                    return Err(GraphError::Corruption(
                        "compacted page cannot fit original live records".into(),
                    ));
                }
                record_page.append_record(record)?;
            }
            
            page.dirty = true;
        }
        
        // Calculate space after compaction
        let bytes_after = {
            let page = self.pager.fetch_page(page_id)?;
            let record_page = RecordPage::from_bytes(&mut page.data)?;
            record_page.available_space()?
        };
        
        Ok(bytes_after.saturating_sub(bytes_before))
    }

    pub fn identify_compaction_candidates(
        &mut self,
        threshold_percent: u8,
        max_candidates: usize,
    ) -> Result<Vec<PageId>> {
        let mut candidates = Vec::new();
        let page_count = self.pager.page_count();
        
        // Start from page 1 (skip header page 0)
        for page_id in 1..page_count as u32 {
            if candidates.len() >= max_candidates {
                break;
            }
            
            let fragmentation = self.get_page_fragmentation(page_id)?;
            if fragmentation >= threshold_percent as f64 {
                candidates.push(page_id);
            }
        }
        
        Ok(candidates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{encode_record, RecordKind};
    use tempfile::NamedTempFile;

    fn build_record(payload: &[u8]) -> Vec<u8> {
        encode_record(RecordKind::Node, payload).expect("encode record")
    }

    #[test]
    fn insert_and_read_round_trip() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let record = build_record(b"payload");
        let pointer = {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            let pointer = store.insert(&record, None).expect("insert");
            pager.flush().expect("flush");
            pointer
        };

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);
        store
            .visit_record(pointer, |slice| {
                assert_eq!(slice[..record.len()], record);
                Ok(())
            })
            .expect("read");
    }

    #[test]
    fn get_page_fragmentation_empty_page() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let mut pager = Pager::open(&path).expect("open pager");
        let page_id = pager.allocate_page().expect("allocate page");
        let mut store = RecordStore::new(&mut pager);

        let fragmentation = store.get_page_fragmentation(page_id).expect("get fragmentation");
        assert_eq!(fragmentation, 0.0);
    }

    #[test]
    fn get_page_fragmentation_with_free_records() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let record = build_record(b"test payload");
        let (page_id, _pointer1, _pointer2) = {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            
            let pointer1 = store.insert(&record, None).expect("insert 1");
            let pointer2 = store.insert(&record, Some(pointer1.page_id)).expect("insert 2");
            let pointer3 = store.insert(&record, Some(pointer1.page_id)).expect("insert 3");
            
            store.mark_free(pointer2).expect("mark free");
            
            pager.flush().expect("flush");
            (pointer1.page_id, pointer1, pointer3)
        };

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);

        let fragmentation = store.get_page_fragmentation(page_id).expect("get fragmentation");
        assert!(fragmentation > 0.0);
    }

    #[test]
    fn compact_page_with_free_records() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let record = build_record(b"test data");
        let page_id = {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            
            let p1 = store.insert(&record, None).expect("insert 1");
            let p2 = store.insert(&record, Some(p1.page_id)).expect("insert 2");
            let _p3 = store.insert(&record, Some(p1.page_id)).expect("insert 3");
            let p4 = store.insert(&record, Some(p1.page_id)).expect("insert 4");
            
            store.mark_free(p2).expect("mark p2 free");
            store.mark_free(p4).expect("mark p4 free");
            
            pager.flush().expect("flush");
            p1.page_id
        };

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);

        let bytes_reclaimed = store.compact_page(page_id).expect("compact page");
        assert!(bytes_reclaimed > 0);

        let fragmentation = store.get_page_fragmentation(page_id).expect("get fragmentation");
        assert_eq!(fragmentation, 0.0);
    }

    #[test]
    fn compact_page_empty() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let record = build_record(b"temp");
        let page_id = {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            
            let p1 = store.insert(&record, None).expect("insert 1");
            let p2 = store.insert(&record, Some(p1.page_id)).expect("insert 2");
            
            store.mark_free(p1).expect("mark p1 free");
            store.mark_free(p2).expect("mark p2 free");
            
            pager.flush().expect("flush");
            p1.page_id
        };

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);

        let bytes_reclaimed = store.compact_page(page_id).expect("compact page");
        assert!(bytes_reclaimed > 0);
    }

    #[test]
    fn identify_compaction_candidates_finds_fragmented_pages() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        // Use larger records to ensure they don't all fit on one page
        let record = build_record(&vec![0u8; 200]);
        {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            
            // Create fragmentation on multiple pages by inserting and then freeing records
            for _ in 0..3 {
                let p1 = store.insert(&record, None).expect("insert 1");
                let p2 = store.insert(&record, Some(p1.page_id)).expect("insert 2");
                let _p3 = store.insert(&record, Some(p1.page_id)).expect("insert 3");
                // Free the middle record to create fragmentation
                store.mark_free(p2).expect("mark free");
            }
            
            pager.flush().expect("flush");
        };

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);

        let candidates = store.identify_compaction_candidates(1, 10).expect("identify candidates");
        assert!(candidates.len() >= 2, "found {} candidates, page_count={}", candidates.len(), pager.page_count());
    }

    #[test]
    fn identify_compaction_candidates_respects_max_limit() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        // Use larger records to ensure they span multiple pages
        let record = build_record(&vec![0u8; 200]);
        {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            
            for _ in 0..5 {
                let p1 = store.insert(&record, None).expect("insert");
                let p2 = store.insert(&record, Some(p1.page_id)).expect("insert");
                let _p3 = store.insert(&record, Some(p1.page_id)).expect("insert");
                store.mark_free(p2).expect("mark free");
            }
            
            pager.flush().expect("flush");
        };

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);

        // With max_candidates=2, we should get at most 2 candidates
        let candidates = store.identify_compaction_candidates(1, 2).expect("identify candidates");
        assert!(candidates.len() <= 2, "found {} candidates, expected <= 2", candidates.len());
        
        // We should have at least some fragmented pages
        let all_candidates = store.identify_compaction_candidates(1, 100).expect("identify all");
        assert!(all_candidates.len() >= 2, "found {} total fragmented pages", all_candidates.len());
    }

    #[test]
    fn update_in_place_when_smaller_record() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let original_record = build_record(b"original data that is fairly long");
        let smaller_record = build_record(b"shorter");

        let pointer = {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            let pointer = store.insert(&original_record, None).expect("insert original");
            pager.flush().expect("flush");
            pointer
        };

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);

        let result = store.update_in_place(pointer, &smaller_record).expect("update in place");
        assert!(result.is_some(), "smaller record should fit in place");

        let updated_pointer = result.unwrap();
        assert_eq!(updated_pointer.page_id, pointer.page_id);
        assert_eq!(updated_pointer.slot_index, pointer.slot_index);

        store.visit_record(updated_pointer, |slice| {
            assert_eq!(&slice[..smaller_record.len()], &smaller_record[..]);
            Ok(())
        }).expect("read updated record");
    }

    #[test]
    fn update_in_place_when_same_size_record() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let original_record = build_record(b"same size data");
        let updated_record = build_record(b"updated values");

        let pointer = {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            let pointer = store.insert(&original_record, None).expect("insert original");
            pager.flush().expect("flush");
            pointer
        };

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);

        let result = store.update_in_place(pointer, &updated_record).expect("update in place");
        assert!(result.is_some(), "same size record should fit in place");

        let updated_pointer = result.unwrap();
        assert_eq!(updated_pointer.page_id, pointer.page_id);
        assert_eq!(updated_pointer.slot_index, pointer.slot_index);

        store.visit_record(updated_pointer, |slice| {
            assert_eq!(&slice[..updated_record.len()], &updated_record[..]);
            Ok(())
        }).expect("read updated record");
    }

    #[test]
    fn update_in_place_fails_when_larger_record() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let original_record = build_record(b"short");
        let larger_record = build_record(b"this is a much longer record that won't fit in the same slot");

        let pointer = {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            let pointer = store.insert(&original_record, None).expect("insert original");
            pager.flush().expect("flush");
            pointer
        };

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);

        let result = store.update_in_place(pointer, &larger_record).expect("update in place");
        assert!(result.is_none(), "larger record should not fit in place and return None");

        store.visit_record(pointer, |slice| {
            assert_eq!(&slice[..original_record.len()], &original_record[..]);
            Ok(())
        }).expect("original record should still be intact");
    }

    #[test]
    fn update_in_place_multiple_times() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let record1 = build_record(b"version 1 data content");
        let record2 = build_record(b"version 2 modified");
        let record3 = build_record(b"version 3 final");

        let mut pointer = {
            let mut pager = Pager::open(&path).expect("open pager");
            let mut store = RecordStore::new(&mut pager);
            let pointer = store.insert(&record1, None).expect("insert v1");
            pager.flush().expect("flush");
            pointer
        };

        {
            let mut pager = Pager::open(&path).expect("reopen pager");
            let mut store = RecordStore::new(&mut pager);

            let result = store.update_in_place(pointer, &record2).expect("update to v2");
            assert!(result.is_some());
            pointer = result.unwrap();

            let result = store.update_in_place(pointer, &record3).expect("update to v3");
            assert!(result.is_some());
            pointer = result.unwrap();

            pager.flush().expect("flush");
        }

        let mut pager = Pager::open(&path).expect("reopen pager");
        let mut store = RecordStore::new(&mut pager);

        store.visit_record(pointer, |slice| {
            assert_eq!(&slice[..record3.len()], &record3[..]);
            Ok(())
        }).expect("final version should be v3");
    }
}
