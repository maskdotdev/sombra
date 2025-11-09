use crate::storage::profile::{
    profile_timer, record_leaf_allocator_build, record_leaf_allocator_compaction,
    record_leaf_allocator_failure, record_leaf_allocator_snapshot_reuse,
    LeafAllocatorFailureKind,
};

#[allow(dead_code)]
const INLINE_FREE_REGIONS: usize = 32;
#[allow(dead_code)]
const INLINE_SLOT_EXTENTS: usize = 128;

/// Incremental allocator for B-tree leaf payloads.
#[allow(dead_code)]
pub(super) struct LeafAllocator<'page> {
    page_bytes: &'page mut [u8],
    header: page::Header,
    payload_len: usize,
    arena_start: usize,
    slot_meta: SmallVec<[SlotMeta; INLINE_SLOT_EXTENTS]>,
    free_regions: SmallVec<[FreeRegion; INLINE_FREE_REGIONS]>,
}

#[allow(dead_code)]
impl<'page> LeafAllocator<'page> {
    /// Builds a new allocator for `page_bytes` using the parsed `header`.
    pub fn new(page_bytes: &'page mut [u8], header: page::Header) -> Result<Self> {
        let timer = profile_timer();
        let data = &*page_bytes;
        let slot_view = SlotView::new(&header, data)?;
        let payload_len = slot_view.payload().len();
        let mut slot_meta = SmallVec::with_capacity(slot_view.len());
        for idx in 0..slot_view.len() {
            let (offset, len) = slot_view.slots().extent(idx)?;
            slot_meta.push(SlotMeta { offset, len });
        }
        let arena_start = page::PAYLOAD_HEADER_LEN + header.low_fence_len + header.high_fence_len;
        let mut allocator = Self {
            page_bytes,
            header,
            payload_len,
            arena_start,
            slot_meta,
            free_regions: SmallVec::new(),
        };
        allocator.rebuild_free_regions()?;
        let duration = timer
            .map(|start| start.elapsed().as_nanos().min(u64::MAX as u128) as u64)
            .unwrap_or(0);
        record_leaf_allocator_build(duration, allocator.free_regions.len() as u64);
        Ok(allocator)
    }

    pub fn from_snapshot(
        page_bytes: &'page mut [u8],
        header: page::Header,
        snapshot: LeafAllocatorSnapshot,
    ) -> Result<Self> {
        let payload_len = page::payload(page_bytes)?.len();
        if payload_len != snapshot.payload_len {
            return Err(SombraError::Invalid("leaf allocator snapshot payload mismatch"));
        }
        if snapshot.slot_meta.len() != header.slot_count as usize {
            return Err(SombraError::Invalid("leaf allocator snapshot slot mismatch"));
        }
        let arena_start = snapshot.arena_start;
        let allocator = Self {
            page_bytes,
            header,
            payload_len,
            arena_start,
            slot_meta: SmallVec::from_vec(snapshot.slot_meta),
            free_regions: SmallVec::from_vec(snapshot.free_regions),
        };
        record_leaf_allocator_snapshot_reuse(allocator.free_regions.len() as u64);
        Ok(allocator)
    }

    /// Returns the number of slots currently tracked.
    pub fn slot_count(&self) -> usize {
        self.slot_meta.len()
    }

    /// Inserts a new record at `insert_idx` using `record_bytes`.
    pub fn insert_slot(&mut self, insert_idx: usize, record_bytes: &[u8]) -> Result<()> {
        if insert_idx > self.slot_meta.len() {
            return Err(SombraError::Invalid("leaf slot insert out of range"));
        }
        if record_bytes.is_empty() {
            return Err(SombraError::Invalid("leaf record length zero"));
        }
        let len_u16 = u16::try_from(record_bytes.len())
            .map_err(|_| SombraError::Invalid("leaf record longer than u16::MAX"))?;
        let offset = self.reserve_for_insert(insert_idx, record_bytes.len())?;
        self.write_record_bytes(offset, record_bytes)?;
        self.slot_meta.insert(
            insert_idx,
            SlotMeta {
                offset,
                len: len_u16,
            },
        );
        self.persist_slot_directory()
    }

    /// Overwrites the record stored at `slot_idx`.
    pub fn replace_slot(&mut self, slot_idx: usize, record_bytes: &[u8]) -> Result<()> {
        if slot_idx >= self.slot_meta.len() {
            return Err(SombraError::Invalid("leaf slot replace out of range"));
        }
        if record_bytes.is_empty() {
            return Err(SombraError::Invalid("leaf record length zero"));
        }
        let new_len_u16 = u16::try_from(record_bytes.len())
            .map_err(|_| SombraError::Invalid("leaf record longer than u16::MAX"))?;
        let current = self.slot_meta[slot_idx];
        if record_bytes.len() <= current.len as usize {
            self.write_record_bytes(current.offset, record_bytes)?;
            if record_bytes.len() < current.len as usize {
                let freed_start = current.offset as usize + record_bytes.len();
                let freed_end = current.end() as usize;
                let freed_len = freed_end - freed_start;
                let freed_start_u16 = u16::try_from(freed_start)
                    .map_err(|_| SombraError::Invalid("leaf free region overflow"))?;
                let freed_len_u16 = u16::try_from(freed_len)
                    .map_err(|_| SombraError::Invalid("leaf free region overflow"))?;
                self.zero_range(freed_start_u16, freed_len_u16)?;
                let freed_end_u16 = u16::try_from(freed_end)
                    .map_err(|_| SombraError::Invalid("leaf free region overflow"))?;
                self.insert_free_region(freed_start_u16, freed_end_u16);
            }
            self.slot_meta[slot_idx].len = new_len_u16;
            return self.persist_slot_directory();
        }

        // Remove the slot temporarily so we can reuse the insertion helpers.
        let removed = self.slot_meta.remove(slot_idx);
        match self.insert_slot(slot_idx, record_bytes) {
            Ok(()) => {
                self.insert_free_region(removed.offset, removed.end());
                Ok(())
            }
            Err(err) => {
                // Restore the previous metadata so the caller can retry via rebuild.
                self.slot_meta.insert(slot_idx, removed);
                Err(err)
            }
        }
    }

    /// Deletes the record at `slot_idx` and recovers its space.
    pub fn delete_slot(&mut self, slot_idx: usize) -> Result<()> {
        if slot_idx >= self.slot_meta.len() {
            return Err(SombraError::Invalid("leaf slot delete out of range"));
        }
        let removed = self.slot_meta.remove(slot_idx);
        self.zero_range(removed.offset, removed.len)?;
        self.insert_free_region(removed.offset, removed.end());
        self.try_shrink_free_start();
        self.persist_slot_directory()
    }

    /// Updates the low fence to match `new_low`.
    pub fn update_low_fence(&mut self, new_low: &[u8]) -> Result<()> {
        let new_arena_start = page::PAYLOAD_HEADER_LEN + new_low.len() + self.header.high_fence_len;
        let used_bytes = self.total_used_bytes();
        self.ensure_capacity_state(self.slot_meta.len(), used_bytes, new_arena_start)?;
        self.compact_all_from(new_arena_start)?;
        {
            let payload = page::payload_mut(self.page_bytes)?;
            page::set_low_fence(payload, new_low)?;
        }
        self.header.low_fence_len = new_low.len();
        self.arena_start = new_arena_start;
        self.persist_slot_directory()?;
        Ok(())
    }

    /// Updates the high fence to match `new_high`.
    pub fn update_high_fence(&mut self, new_high: &[u8]) -> Result<()> {
        let new_arena_start = page::PAYLOAD_HEADER_LEN + self.header.low_fence_len + new_high.len();
        let used_bytes = self.total_used_bytes();
        self.ensure_capacity_state(self.slot_meta.len(), used_bytes, new_arena_start)?;
        self.compact_all_from(new_arena_start)?;
        {
            let payload = page::payload_mut(self.page_bytes)?;
            page::set_high_fence(payload, new_high)?;
        }
        self.header.high_fence_len = new_high.len();
        self.arena_start = new_arena_start;
        self.persist_slot_directory()?;
        Ok(())
    }

    /// Clears slot metadata and reinitializes fences/free space pointers.
    pub fn reset(&mut self, low: &[u8], high: &[u8]) -> Result<()> {
        let new_arena_start = page::PAYLOAD_HEADER_LEN + low.len() + high.len();
        if new_arena_start > self.payload_len {
            return Err(SombraError::Invalid("leaf payload exhausted"));
        }
        self.slot_meta.clear();
        self.free_regions.clear();
        self.arena_start = new_arena_start;
        self.header.low_fence_len = low.len();
        self.header.high_fence_len = high.len();
        {
            let payload = page::payload_mut(self.page_bytes)?;
            page::set_low_fence(payload, low)?;
            page::set_high_fence(payload, high)?;
            payload[self.arena_start..self.payload_len].fill(0);
        }
        self.header.free_start = u16::try_from(self.arena_start)
            .map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
        self.header.free_end = u16::try_from(self.payload_len)
            .map_err(|_| SombraError::Invalid("leaf free_end overflow"))?;
        self.persist_slot_directory()?;
        Ok(())
    }

    /// Rewrites the page contents with the provided entries.
    pub fn rebuild_from_entries(
        &mut self,
        low: &[u8],
        high: &[u8],
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        self.reset(low, high)?;
        let mut record = Vec::new();
        for (idx, (key, value)) in entries.iter().enumerate() {
            record.clear();
            page::encode_leaf_record(key.as_slice(), value.as_slice(), &mut record)?;
            self.insert_slot(idx, &record)?;
        }
        Ok(())
    }

    /// Renders the current header value (for testing/debugging).
    pub fn header(&self) -> &page::Header {
        &self.header
    }

    /// Returns the raw record slice for `slot_idx`.
    pub fn record_slice(&self, slot_idx: usize) -> Result<&[u8]> {
        let payload = page::payload(&*self.page_bytes)?;
        let meta = self
            .slot_meta
            .get(slot_idx)
            .ok_or(SombraError::Invalid("slot index out of bounds"))?;
        let start = meta.offset as usize;
        let end = start
            .checked_add(meta.len as usize)
            .ok_or_else(|| SombraError::Invalid("record extent overflow"))?;
        if end > payload.len() {
            return Err(SombraError::Invalid("record extent beyond payload"));
        }
        Ok(&payload[start..end])
    }

    /// Decodes and returns the leaf record stored at `slot_idx`.
    pub fn leaf_record(&self, slot_idx: usize) -> Result<page::LeafRecordRef<'_>> {
        let slice = self.record_slice(slot_idx)?;
        page::decode_leaf_record(slice)
    }

    /// Persists the allocator metadata for reuse by future edits.
    pub fn into_snapshot(self) -> LeafAllocatorSnapshot {
        LeafAllocatorSnapshot {
            slot_meta: self.slot_meta.into_vec(),
            free_regions: self.free_regions.into_vec(),
            arena_start: self.arena_start,
            payload_len: self.payload_len,
        }
    }

    fn total_used_bytes(&self) -> usize {
        self.slot_meta.iter().map(|meta| meta.len as usize).sum()
    }

    fn reserve_for_insert(&mut self, insert_idx: usize, len: usize) -> Result<u16> {
        let used_bytes = self
            .total_used_bytes()
            .checked_add(len)
            .ok_or_else(|| SombraError::Invalid("leaf insert size overflow"))?;
        self.ensure_capacity_state(self.slot_meta.len() + 1, used_bytes, self.arena_start)?;
        self.reserve_with_gap(insert_idx, len)
    }

    fn ensure_capacity_state(
        &self,
        future_slots: usize,
        used_bytes: usize,
        arena_start: usize,
    ) -> Result<()> {
        let slot_bytes = future_slots
            .checked_mul(page::SLOT_ENTRY_LEN)
            .ok_or_else(|| SombraError::Invalid("slot directory overflow"))?;
        if slot_bytes > self.payload_len {
            record_leaf_allocator_failure(LeafAllocatorFailureKind::SlotOverflow);
            return Err(SombraError::Invalid("slot directory exceeds payload"));
        }
        let usable = self
            .payload_len
            .checked_sub(slot_bytes)
            .ok_or_else(|| SombraError::Invalid("slot directory exceeds payload"))?;
        if arena_start > usable {
            record_leaf_allocator_failure(LeafAllocatorFailureKind::PayloadExhausted);
            return Err(SombraError::Invalid("leaf payload exhausted"));
        }
        if used_bytes > usable - arena_start {
            record_leaf_allocator_failure(LeafAllocatorFailureKind::PageFull);
            return Err(SombraError::Invalid("leaf page full"));
        }
        Ok(())
    }

    fn reserve_with_gap(&mut self, insert_idx: usize, len: usize) -> Result<u16> {
        if let Some(offset) = self.allocate_from_free_regions(len) {
            return Ok(offset);
        }
        if self.has_contiguous_headroom(len) {
            let offset = self.header.free_start;
            let new_free_start = (self.header.free_start as usize)
                .checked_add(len)
                .ok_or_else(|| SombraError::Invalid("leaf free_start overflow"))?;
            self.header.free_start = u16::try_from(new_free_start)
                .map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
            return Ok(offset);
        }
        self.compact_with_gap(insert_idx, len)
    }

    fn allocate_from_free_regions(&mut self, len: usize) -> Option<u16> {
        for idx in 0..self.free_regions.len() {
            if let Some(offset) = self.free_regions[idx].alloc(len) {
                if self.free_regions[idx].is_empty() {
                    self.free_regions.remove(idx);
                }
                return Some(offset);
            }
        }
        None
    }

    fn has_contiguous_headroom(&self, len: usize) -> bool {
        let available =
            (self.header.free_end as usize).saturating_sub(self.header.free_start as usize);
        available >= len
    }

    fn compact_with_gap(&mut self, insert_idx: usize, gap_len: usize) -> Result<u16> {
        let payload = page::payload_mut(self.page_bytes)?;
        let mut cursor = self.arena_start;
        let mut bytes_moved = 0u64;
        let mut gap_offset = None;
        for idx in 0..=self.slot_meta.len() {
            if idx == insert_idx {
                gap_offset = Some(cursor as u16);
                cursor += gap_len;
                continue;
            }
            let slot_idx = if idx < insert_idx { idx } else { idx - 1 };
            let meta = &mut self.slot_meta[slot_idx];
            let start = meta.offset as usize;
            let end = start + meta.len as usize;
            if start != cursor {
                payload.copy_within(start..end, cursor);
                bytes_moved += (end - start) as u64;
                meta.offset = u16::try_from(cursor)
                    .map_err(|_| SombraError::Invalid("leaf slot offset overflow"))?;
            }
            cursor += meta.len as usize;
        }
        let new_free_start = cursor;
        self.header.free_start = u16::try_from(new_free_start)
            .map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
        self.free_regions.clear();
        record_leaf_allocator_compaction(bytes_moved);
        gap_offset.ok_or_else(|| SombraError::Invalid("gap allocation failed"))
    }

    fn compact_all_from(&mut self, new_start: usize) -> Result<()> {
        if self.slot_meta.is_empty() {
            self.header.free_start = u16::try_from(new_start)
                .map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
            self.free_regions.clear();
            return Ok(());
        }
        let payload = page::payload_mut(self.page_bytes)?;
        let mut cursor = new_start;
        let mut bytes_moved = 0u64;
        for meta in self.slot_meta.iter_mut() {
            let start = meta.offset as usize;
            let end = start + meta.len as usize;
            if start != cursor {
                payload.copy_within(start..end, cursor);
                bytes_moved += (end - start) as u64;
                meta.offset = u16::try_from(cursor)
                    .map_err(|_| SombraError::Invalid("leaf slot offset overflow"))?;
            }
            cursor += meta.len as usize;
        }
        self.header.free_start = u16::try_from(cursor)
            .map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
        self.free_regions.clear();
        if bytes_moved > 0 {
            record_leaf_allocator_compaction(bytes_moved);
        }
        Ok(())
    }

    fn persist_slot_directory(&mut self) -> Result<()> {
        let slot_count = self.slot_meta.len();
        let slot_bytes = slot_count
            .checked_mul(page::SLOT_ENTRY_LEN)
            .ok_or_else(|| SombraError::Invalid("slot directory overflow"))?;
        if slot_bytes > self.payload_len {
            return Err(SombraError::Invalid("slot directory exceeds payload"));
        }
        let new_free_end = self
            .payload_len
            .checked_sub(slot_bytes)
            .ok_or_else(|| SombraError::Invalid("slot directory exceeds payload"))?;
        if new_free_end < self.header.free_start as usize {
            return Err(SombraError::Invalid("leaf payload exhausted"));
        }
        self.header.free_end = u16::try_from(new_free_end)
            .map_err(|_| SombraError::Invalid("leaf free_end overflow"))?;
        let payload = page::payload_mut(self.page_bytes)?;
        page::set_slot_count(
            payload,
            u16::try_from(slot_count).map_err(|_| SombraError::Invalid("slot count overflow"))?,
        );
        page::set_free_start(payload, self.header.free_start);
        page::set_free_end(payload, self.header.free_end);
        let slot_dir_start = new_free_end;
        for (idx, meta) in self.slot_meta.iter().enumerate() {
            let pos = slot_dir_start + idx * page::SLOT_ENTRY_LEN;
            page::write_slot_entry(payload, pos, meta.offset, meta.len);
        }
        Ok(())
    }

    fn write_record_bytes(&mut self, offset: u16, record_bytes: &[u8]) -> Result<()> {
        let payload = page::payload_mut(self.page_bytes)?;
        let start = offset as usize;
        let end = start
            .checked_add(record_bytes.len())
            .ok_or_else(|| SombraError::Invalid("record extent overflow"))?;
        if end > self.payload_len {
            return Err(SombraError::Invalid("record extent beyond payload"));
        }
        payload[start..end].copy_from_slice(record_bytes);
        Ok(())
    }

    fn zero_range(&mut self, offset: u16, len: u16) -> Result<()> {
        if len == 0 {
            return Ok(());
        }
        let payload = page::payload_mut(self.page_bytes)?;
        let start = offset as usize;
        let end = start + len as usize;
        if end > self.payload_len {
            return Err(SombraError::Invalid("record extent beyond payload"));
        }
        payload[start..end].fill(0);
        Ok(())
    }

    fn rebuild_free_regions(&mut self) -> Result<()> {
        self.free_regions.clear();
        if self.slot_meta.is_empty() {
            if self.header.free_start as usize > self.arena_start {
                self.push_free_region(self.arena_start as u16, self.header.free_start);
            }
            return Ok(());
        }
        let mut ordered: SmallVec<[(usize, usize); INLINE_SLOT_EXTENTS]> =
            SmallVec::with_capacity(self.slot_meta.len());
        for meta in &self.slot_meta {
            ordered.push((meta.offset as usize, meta.end() as usize));
        }
        ordered.sort_unstable_by_key(|entry| entry.0);
        let mut cursor = self.arena_start;
        for (start, end) in ordered {
            if start < cursor {
                return Err(SombraError::Corruption("leaf record extents overlap"));
            }
            if start > cursor {
                self.push_free_region(cursor as u16, start as u16);
            }
            cursor = end;
        }
        if cursor < self.header.free_start as usize {
            self.push_free_region(cursor as u16, self.header.free_start);
        }
        Ok(())
    }

    fn push_free_region(&mut self, start: u16, end: u16) {
        if start >= end {
            return;
        }
        let mut insert_at = 0;
        while insert_at < self.free_regions.len()
            && self.free_regions[insert_at].start < start
        {
            insert_at += 1;
        }
        self.free_regions
            .insert(insert_at, FreeRegion { start, end });
        self.coalesce_free_regions();
    }

    fn insert_free_region(&mut self, start: u16, end: u16) {
        self.push_free_region(start, end);
    }

    fn coalesce_free_regions(&mut self) {
        let mut idx = 0;
        while idx + 1 < self.free_regions.len() {
            if self.free_regions[idx].end >= self.free_regions[idx + 1].start {
                let new_end = self.free_regions[idx + 1].end;
                self.free_regions[idx].end = new_end;
                self.free_regions.remove(idx + 1);
            } else {
                idx += 1;
            }
        }
    }

    fn try_shrink_free_start(&mut self) {
        loop {
            let mut shrunk = false;
            if let Some(pos) = self
                .free_regions
                .iter()
                .position(|region| region.end == self.header.free_start)
            {
                self.header.free_start = self.free_regions[pos].start;
                self.free_regions.remove(pos);
                shrunk = true;
            }
            if !shrunk {
                break;
            }
        }
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
struct SlotMeta {
    offset: u16,
    len: u16,
}

#[allow(dead_code)]
impl SlotMeta {
    fn end(&self) -> u16 {
        self.offset.saturating_add(self.len)
    }
}

#[allow(dead_code)]
#[derive(Clone, Copy, Debug)]
struct FreeRegion {
    start: u16,
    end: u16,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub(super) struct LeafAllocatorSnapshot {
    slot_meta: Vec<SlotMeta>,
    free_regions: Vec<FreeRegion>,
    arena_start: usize,
    payload_len: usize,
}

#[allow(dead_code)]
impl FreeRegion {
    fn len(&self) -> usize {
        (self.end as usize).saturating_sub(self.start as usize)
    }

    fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    fn alloc(&mut self, len: usize) -> Option<u16> {
        if len == 0 || len > self.len() {
            return None;
        }
        let offset = self.start;
        self.start = self
            .start
            .checked_add(len as u16)?;
        Some(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::btree::page;
    use crate::types::page::{PageHeader, PageKind, PAGE_HDR_LEN, DEFAULT_PAGE_SIZE};
    use crate::types::{PageId, Result as TestResult, SombraError};
    use proptest::prelude::*;

    #[allow(dead_code)]
    #[derive(Clone, Debug)]
    enum TestOp {
        Insert(Vec<u8>, Vec<u8>),
        Delete,
    }

    #[allow(dead_code)]
    fn op_strategy() -> impl Strategy<Value = TestOp> {
        let key_strategy = prop::collection::vec(any::<u8>(), 1..6);
        let val_strategy = prop::collection::vec(any::<u8>(), 0..24);
        prop_oneof![
            (key_strategy, val_strategy).prop_map(|(k, v)| TestOp::Insert(k, v)),
            Just(TestOp::Delete),
        ]
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(24))]
        fn allocator_handles_random_ops(ops in prop::collection::vec(op_strategy(), 1..48)) {
            run_sequence(ops).expect("allocator sequence should not corrupt page");
        }
    }

    #[test]
    fn allocator_recovers_space_after_deletes() -> TestResult<()> {
        let mut buf = blank_leaf(DEFAULT_PAGE_SIZE as usize)?;
        let mut entries = Vec::new();
        for i in 0..32u32 {
            apply_insert(&mut buf, &mut entries, format!("k{i}").into_bytes(), vec![i as u8])?;
        }
        for _ in 0..16 {
            apply_delete(&mut buf, &mut entries)?;
        }
        assert_page_matches(&buf, &entries)?;
        Ok(())
    }

    #[allow(dead_code)]
    fn run_sequence(ops: Vec<TestOp>) -> TestResult<()> {
        let mut buf = blank_leaf(DEFAULT_PAGE_SIZE as usize)?;
        let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for op in ops {
            match op {
                TestOp::Insert(k, v) => {
                    apply_insert(&mut buf, &mut entries, k, v)?;
                }
                TestOp::Delete => {
                    if !entries.is_empty() {
                        apply_delete(&mut buf, &mut entries)?;
                    }
                }
            }
            assert_page_matches(&buf, &entries)?;
        }
        Ok(())
    }

    fn apply_insert(
        buf: &mut [u8],
        entries: &mut Vec<(Vec<u8>, Vec<u8>)>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> TestResult<()> {
        let header = page::Header::parse(buf)?;
        let mut allocator = LeafAllocator::new(buf, header)?;
        let mut record = Vec::new();
        page::encode_leaf_record(&key, &value, &mut record)?;
        let idx = match entries.binary_search_by(|(existing, _)| existing.cmp(&key)) {
            Ok(_) => return Ok(()),
            Err(pos) => pos,
        };
        match allocator.insert_slot(idx, &record) {
            Ok(()) => {
                entries.insert(idx, (key, value));
                Ok(())
            }
            Err(err) if is_full(&err) => Ok(()),
            Err(err) => Err(err),
        }
    }

    fn apply_delete(buf: &mut [u8], entries: &mut Vec<(Vec<u8>, Vec<u8>)>) -> TestResult<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let header = page::Header::parse(buf)?;
        let mut allocator = LeafAllocator::new(buf, header)?;
        let idx = entries.len() / 2;
        allocator.delete_slot(idx)?;
        entries.remove(idx);
        Ok(())
    }

    fn assert_page_matches(page_bytes: &[u8], entries: &[(Vec<u8>, Vec<u8>)]) -> TestResult<()> {
        let header = page::Header::parse(page_bytes)?;
        let slot_view = SlotView::new(&header, page_bytes)?;
        assert_eq!(slot_view.len(), entries.len());
        for (idx, (expected_key, expected_val)) in entries.iter().enumerate() {
            let rec_slice = slot_view.slice(idx)?;
            let record = page::decode_leaf_record(rec_slice)?;
            assert_eq!(record.key, expected_key.as_slice());
            assert_eq!(record.value, expected_val.as_slice());
        }
        Ok(())
    }

    fn blank_leaf(page_size: usize) -> TestResult<Vec<u8>> {
        let mut buf = vec![0u8; PAGE_HDR_LEN + page_size];
        let header = PageHeader::new(PageId(1), PageKind::BTreeLeaf, page_size as u32, 0)?
            .with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        page::write_initial_header(
            &mut buf[PAGE_HDR_LEN..],
            page::BTreePageKind::Leaf,
        )?;
        Ok(buf)
    }

    fn is_full(err: &SombraError) -> bool {
        matches!(
            err,
            SombraError::Invalid("leaf page full")
                | SombraError::Invalid("slot directory exceeds payload")
        )
    }
}
