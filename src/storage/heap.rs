use crate::error::{GraphError, Result};
use crate::pager::{PageId, Pager};
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
        let slice = &page.data[offset..];
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
        let slice = &page.data[offset..];
        if slice.len() < 8 {
            return Err(GraphError::Corruption("record header truncated".into()));
        }
        let payload_len = u32::from_le_bytes([slice[4], slice[5], slice[6], slice[7]]) as usize;
        let record_len = 8 + payload_len;
        if page.data.len() < offset + record_len {
            return Err(GraphError::Corruption("record extends past page".into()));
        }
        let result = f(&mut page.data[offset..offset + record_len])?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{encode_record, RecordKind};
    use tempfile::NamedTempFile;

    fn build_record(payload: &[u8]) -> Vec<u8> {
        encode_record(RecordKind::Node, payload)
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
}
