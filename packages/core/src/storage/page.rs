use crate::error::{GraphError, Result};
use crate::pager::PAGE_CHECKSUM_SIZE;
use crate::storage::record::{RecordHeader, RecordKind, RECORD_HEADER_SIZE};
use std::convert::TryInto;

const PAGE_HEADER_SIZE: usize = 16;
const RECORD_COUNT_OFFSET: usize = 0;
const FREE_SPACE_OFFSET_OFFSET: usize = 2;
const FREE_LIST_NEXT_OFFSET: usize = 4;

// Known page type magic bytes - these are NOT RecordPages
const BTREE_INDEX_MAGIC: &[u8; 4] = b"BIDX";
const PROPERTY_INDEX_MAGIC: &[u8; 4] = b"PIDX";

#[derive(Debug)]
pub struct RecordPage<'a> {
    data: &'a mut [u8],
}

impl<'a> RecordPage<'a> {
    pub fn from_bytes(data: &'a mut [u8]) -> Result<Self> {
        if data.len() < PAGE_CHECKSUM_SIZE {
            return Err(GraphError::Corruption(
                "page smaller than checksum metadata region".into(),
            ));
        }
        let payload_len = data.len() - PAGE_CHECKSUM_SIZE;
        if payload_len < PAGE_HEADER_SIZE {
            return Err(GraphError::Corruption(
                "page smaller than header size".into(),
            ));
        }

        // Check if this page has a known non-RecordPage magic signature
        // BTree and Property index pages start with 4-byte magic, which would
        // be misinterpreted as record_count/free_offset if we tried to parse them
        if data.len() >= 4 {
            let maybe_magic = &data[0..4];
            if maybe_magic == BTREE_INDEX_MAGIC || maybe_magic == PROPERTY_INDEX_MAGIC {
                return Err(GraphError::InvalidArgument(
                    format!("not a record page (magic: {:?})", 
                        std::str::from_utf8(maybe_magic).unwrap_or("???")).into()
                ));
            }
        }

        Ok(Self { data })
    }

    fn payload_limit(&self) -> Result<usize> {
        self.data
            .len()
            .checked_sub(PAGE_CHECKSUM_SIZE)
            .ok_or_else(|| GraphError::Corruption("page smaller than checksum metadata".into()))
    }

    pub fn initialize(&mut self) -> Result<()> {
        if self.record_count()? == 0 && self.free_space_offset()? == 0 {
            let page_size = self.page_size()?;
            self.set_free_space_offset(page_size)?;
        }
        Ok(())
    }

    pub fn page_size(&self) -> Result<u16> {
        let len = self.payload_limit()?;
        if len > u16::MAX as usize {
            return Err(GraphError::InvalidArgument(
                "page size exceeds u16::MAX".into(),
            ));
        }
        Ok(len as u16)
    }

    pub fn record_count(&self) -> Result<u16> {
        self.read_u16_at(RECORD_COUNT_OFFSET)
    }

    fn set_record_count(&mut self, value: u16) {
        self.data[RECORD_COUNT_OFFSET..RECORD_COUNT_OFFSET + 2]
            .copy_from_slice(&value.to_le_bytes());
    }

    pub fn free_space_offset(&self) -> Result<u16> {
        self.read_u16_at(FREE_SPACE_OFFSET_OFFSET)
    }

    fn set_free_space_offset(&mut self, value: u16) -> Result<()> {
        let limit = self.payload_limit()?;
        if value as usize > limit {
            return Err(GraphError::InvalidArgument(
                "free space offset beyond page size".into(),
            ));
        }
        self.data[FREE_SPACE_OFFSET_OFFSET..FREE_SPACE_OFFSET_OFFSET + 2]
            .copy_from_slice(&value.to_le_bytes());
        Ok(())
    }

    pub fn free_list_next(&self) -> Result<u32> {
        self.read_u32_at(FREE_LIST_NEXT_OFFSET)
    }

    pub fn set_free_list_next(&mut self, page_id: u32) {
        self.data[FREE_LIST_NEXT_OFFSET..FREE_LIST_NEXT_OFFSET + 4]
            .copy_from_slice(&page_id.to_le_bytes());
    }

    fn directory_start() -> usize {
        PAGE_HEADER_SIZE
    }

    fn directory_len(&self) -> Result<usize> {
        let count = self.record_count()? as usize;
        Ok(count * 2)
    }

    fn directory_end(&self) -> Result<usize> {
        Ok(Self::directory_start() + self.directory_len()?)
    }

    pub fn available_space(&self) -> Result<usize> {
        let free_offset = self.free_space_offset()? as usize;
        let dir_end = self.directory_end()?;
        let limit = self.payload_limit()?;
        if free_offset < dir_end {
            return Err(GraphError::Corruption(
                "free space offset precedes directory".into(),
            ));
        }
        if free_offset > limit {
            return Err(GraphError::Corruption(
                "free space offset beyond payload region".into(),
            ));
        }
        Ok(free_offset - dir_end)
    }

    pub fn record_offset(&self, index: usize) -> Result<u16> {
        let count = self.record_count()? as usize;
        if index >= count {
            return Err(GraphError::InvalidArgument(
                "record index out of bounds".into(),
            ));
        }
        let dir_pos = Self::directory_start() + index * 2;
        self.read_u16_at(dir_pos)
    }

    fn set_record_offset(&mut self, index: usize, offset: u16) -> Result<()> {
        let count = self.record_count()? as usize;
        if index > count {
            return Err(GraphError::InvalidArgument(
                "record index beyond current count".into(),
            ));
        }
        let dir_pos = Self::directory_start() + index * 2;
        let limit = self.payload_limit()?;
        if dir_pos + 2 > limit {
            return Err(GraphError::InvalidArgument(
                "directory position outside page".into(),
            ));
        }
        self.data[dir_pos..dir_pos + 2].copy_from_slice(&offset.to_le_bytes());
        Ok(())
    }

    fn read_u16_at(&self, offset: usize) -> Result<u16> {
        let end = offset
            .checked_add(2)
            .ok_or_else(|| GraphError::Corruption("u16 read offset overflow".into()))?;
        let limit = self.payload_limit()?;
        if end > limit {
            return Err(GraphError::Corruption(
                "record page short read for u16".into(),
            ));
        }
        let slice = &self.data[offset..end];
        let bytes: [u8; 2] = slice
            .try_into()
            .map_err(|_| GraphError::Corruption("failed to read u16 from record page".into()))?;
        Ok(u16::from_le_bytes(bytes))
    }

    fn read_u32_at(&self, offset: usize) -> Result<u32> {
        let end = offset
            .checked_add(4)
            .ok_or_else(|| GraphError::Corruption("u32 read offset overflow".into()))?;
        let limit = self.payload_limit()?;
        if end > limit {
            return Err(GraphError::Corruption(
                "record page short read for u32".into(),
            ));
        }
        let slice = &self.data[offset..end];
        let bytes: [u8; 4] = slice
            .try_into()
            .map_err(|_| GraphError::Corruption("failed to read u32 from record page".into()))?;
        Ok(u32::from_le_bytes(bytes))
    }

    pub fn append_record(&mut self, record: &[u8]) -> Result<u16> {
        let padded_len = align_to_eight(record.len());
        let space_needed = required_space(record.len());

        // Log state before append
        if self.available_space()? < space_needed {
            return Err(GraphError::InvalidArgument(
                "insufficient space for record".into(),
            ));
        }

        let free_offset = self.free_space_offset()? as usize;
        let new_offset = free_offset - padded_len;
        if new_offset > free_offset {
            return Err(GraphError::Corruption(
                "free space calculation underflowed".into(),
            ));
        }

        self.data[new_offset..new_offset + record.len()].copy_from_slice(record);
        if padded_len > record.len() {
            self.data[new_offset + record.len()..free_offset].fill(0);
        }

        let record_idx = self.record_count()? as usize;
        if record_idx >= u16::MAX as usize {
            return Err(GraphError::InvalidArgument(
                "record count would overflow u16".into(),
            ));
        }
        self.set_record_offset(record_idx, new_offset as u16)?;
        self.set_record_count((record_idx + 1) as u16);
        self.set_free_space_offset(new_offset as u16)?;
        
        Ok(record_idx as u16)
    }

    pub fn record_slice(&self, index: usize) -> Result<&[u8]> {
        let offset = self.record_offset(index)? as usize;
        let (start, end) = self.record_bounds(offset)?;
        Ok(&self.data[start..end])
    }

    pub fn record_slice_mut(&mut self, index: usize) -> Result<&mut [u8]> {
        let offset = self.record_offset(index)? as usize;
        let (start, end) = self.record_bounds(offset)?;
        Ok(&mut self.data[start..end])
    }

    pub fn can_fit(&self, record_len: usize) -> Result<bool> {
        Ok(self.available_space()? >= required_space(record_len))
    }

    pub fn try_reuse_slot(&mut self, index: usize, record: &[u8]) -> Result<bool> {
        if record.is_empty() {
            return Err(GraphError::InvalidArgument(
                "record payload cannot be empty".into(),
            ));
        }
        let header = self.record_header_at(index)?;
        if header.kind != RecordKind::Free {
            return Ok(false);
        }
        let offset = self.record_offset(index)? as usize;
        let (start, end) = self.record_bounds(offset)?;
        let capacity = end - start;
        let needed = align_to_eight(record.len());
        if needed > capacity {
            return Ok(false);
        }
        self.data[start..start + record.len()].copy_from_slice(record);
        if needed > record.len() {
            self.data[start + record.len()..start + needed].fill(0);
        }
        if capacity > needed {
            self.data[start + needed..end].fill(0);
        }
        Ok(true)
    }

    pub fn try_update_slot(&mut self, index: usize, record: &[u8]) -> Result<bool> {
        if record.is_empty() {
            return Err(GraphError::InvalidArgument(
                "record payload cannot be empty".into(),
            ));
        }
        let offset = self.record_offset(index)? as usize;
        let (start, end) = self.record_bounds(offset)?;
        let capacity = end - start;
        let needed = align_to_eight(record.len());
        if needed > capacity {
            return Ok(false);
        }
        self.data[start..start + record.len()].copy_from_slice(record);
        if needed > record.len() {
            self.data[start + record.len()..start + needed].fill(0);
        }
        if capacity > needed {
            self.data[start + needed..end].fill(0);
        }
        Ok(true)
    }

    pub fn mark_slot_free(&mut self, index: usize) -> Result<()> {
        let offset = self.record_offset(index)? as usize;
        let (start, end) = self.record_bounds(offset)?;
        let header = self.record_header_at(index)?;
        if header.kind == RecordKind::Free {
            return Ok(());
        }
        let capacity = end - start;
        if capacity < RECORD_HEADER_SIZE {
            return Err(GraphError::Corruption(
                "record slot smaller than header".into(),
            ));
        }
        let free_payload = capacity - RECORD_HEADER_SIZE;
        let free_header = RecordHeader::new(RecordKind::Free, free_payload as u32);
        free_header.write_to(&mut self.data[start..start + RECORD_HEADER_SIZE])?;
        self.data[start + RECORD_HEADER_SIZE..end].fill(0);
        Ok(())
    }

    pub fn live_record_count(&self) -> Result<usize> {
        let count = self.record_count()? as usize;
        let mut live = 0;
        for idx in 0..count {
            if self.record_header_at(idx)?.kind != RecordKind::Free {
                live += 1;
            }
        }
        Ok(live)
    }

    pub fn clear(&mut self) -> Result<()> {
        self.data.fill(0);
        Ok(())
    }

    fn record_bounds(&self, offset: usize) -> Result<(usize, usize)> {
        let limit = self.payload_limit()?;
        if offset >= limit {
            return Err(GraphError::Corruption("record offset outside page".into()));
        }
        if offset + RECORD_HEADER_SIZE > limit {
            return Err(GraphError::Corruption("record header truncated".into()));
        }
        let header_slice = &self.data[offset..offset + RECORD_HEADER_SIZE];
        let header = RecordHeader::from_bytes(header_slice)?;
        let payload_len = header.payload_length as usize;
        let record_len = RECORD_HEADER_SIZE + payload_len;
        let padded_len = align_to_eight(record_len);
        let end = offset + padded_len;
        if end > limit {
            return Err(GraphError::Corruption(
                "record extends past end of page".into(),
            ));
        }
        Ok((offset, end))
    }

    pub fn record_header_at(&self, index: usize) -> Result<RecordHeader> {
        let offset = self.record_offset(index)? as usize;
        let limit = self.payload_limit()?;
        if offset + RECORD_HEADER_SIZE > limit {
            return Err(GraphError::Corruption(
                "record header extends beyond page".into(),
            ));
        }
        RecordHeader::from_bytes(&self.data[offset..offset + RECORD_HEADER_SIZE])
    }

    /// Returns true if this page has any free slots (either genuinely free or
    /// slots marked as RecordKind::Free that can be reused).
    pub fn has_free_slots(&self) -> Result<bool> {
        let record_count = self.record_count()? as usize;
        for slot in 0..record_count {
            let record = self.record_slice(slot)?;
            let header = RecordHeader::from_bytes(&record[..RECORD_HEADER_SIZE])?;
            if header.kind == RecordKind::Free {
                return Ok(true);
            }
        }
        Ok(false)
    }
}

fn align_to_eight(value: usize) -> usize {
    let remainder = value % 8;
    if remainder == 0 {
        value
    } else {
        value + (8 - remainder)
    }
}

fn required_space(record_len: usize) -> usize {
    align_to_eight(record_len) + 2
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::record::{encode_record, RecordKind};

    struct PageBuffer {
        data: Vec<u8>,
    }

    impl PageBuffer {
        fn new(size: usize) -> Self {
            Self {
                data: vec![0; size],
            }
        }

        fn with_page<F>(&mut self, mut f: F)
        where
            F: FnMut(&mut RecordPage<'_>),
        {
            let mut page = RecordPage::from_bytes(self.data.as_mut_slice()).expect("create page");
            f(&mut page);
        }
    }

    fn build_record(payload: &[u8]) -> Vec<u8> {
        encode_record(RecordKind::Node, payload).expect("encode record")
    }

    #[test]
    fn initialize_sets_free_space() {
        let mut buf = PageBuffer::new(256);
        buf.with_page(|page| {
            page.initialize().expect("initialize");
            let expected_offset = (256 - PAGE_CHECKSUM_SIZE) as u16;
            assert_eq!(page.free_space_offset().unwrap(), expected_offset);
            assert_eq!(page.record_count().unwrap(), 0);
        });
    }

    #[test]
    fn append_record_stores_data() {
        let payload = b"hello";
        let record = build_record(payload);

        let mut buf = PageBuffer::new(128);
        let mut stored_record = Vec::new();

        buf.with_page(|page| {
            page.initialize().expect("initialize");
            let initial_space = page.available_space().unwrap();

            let slot = page.append_record(&record).expect("append record");
            assert_eq!(page.record_count().unwrap(), 1);
            assert_eq!(slot, 0);
            let offset = page.record_offset(slot as usize).unwrap();
            assert!(offset as usize <= 120); // header + directory occupy 16 + 2 bytes

            let slice = page.record_slice(0).expect("record slice");
            stored_record = slice[..record.len()].to_vec();

            let expected_space = initial_space - align_to_eight(record.len()) - 2;
            assert_eq!(page.available_space().unwrap(), expected_space);
        });

        assert_eq!(stored_record, record);
    }

    #[test]
    fn insufficient_space_errors() {
        let payload = vec![1u8; 120];
        let record = build_record(&payload);
        let mut buf = PageBuffer::new(128);
        buf.with_page(|page| {
            page.initialize().expect("initialize");
            assert!(!page.can_fit(record.len()).unwrap());
            let err = page.append_record(&record).unwrap_err();
            assert!(matches!(err, GraphError::InvalidArgument(_)));
        });
    }

    #[test]
    fn can_fit_tracks_free_space() {
        let payload = b"abc";
        let record = build_record(payload);
        let mut buf = PageBuffer::new(64);
        buf.with_page(|page| {
            page.initialize().expect("initialize");
            assert!(page.can_fit(record.len()).unwrap());
            while page.can_fit(record.len()).unwrap() {
                page.append_record(&record).expect("append");
            }
            assert!(!page.can_fit(record.len()).unwrap());
        });
    }
}
