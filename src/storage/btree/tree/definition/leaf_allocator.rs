// Note: profile imports come from mod.rs via the include! macro

const INLINE_FREE_REGIONS: usize = 32;
const INLINE_SLOT_EXTENTS: usize = 128;

/// Incremental allocator for B-tree leaf payloads.
pub(super) struct LeafAllocator<'page> {
    page_bytes: &'page mut [u8],
    header: page::Header,
    payload_len: usize,
    arena_start: usize,
    slot_meta: SmallVec<[SlotMeta; INLINE_SLOT_EXTENTS]>,
    free_regions: SmallVec<[FreeRegion; INLINE_FREE_REGIONS]>,
}

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
            return Err(SombraError::Invalid(
                "leaf allocator snapshot payload mismatch",
            ));
        }
        if snapshot.slot_meta.len() != header.slot_count as usize {
            return Err(SombraError::Invalid(
                "leaf allocator snapshot slot mismatch",
            ));
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
        self.persist_slot_directory()?;
        self.debug_assert_ordered();
        Ok(())
    }

    /// Overwrites the record stored at `slot_idx`.
    #[allow(dead_code)]
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
            self.persist_slot_directory()?;
            self.debug_assert_ordered();
            return Ok(());
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
        self.persist_slot_directory()?;
        self.debug_assert_ordered();
        Ok(())
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
        self.debug_assert_ordered();
        Ok(())
    }

    /// Updates the high fence to match `new_high`.
    #[allow(dead_code)]
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

    pub fn rebuild_from_records<'a, I>(&mut self, low: &[u8], high: &[u8], records: I) -> Result<()>
    where
        I: IntoIterator<Item = &'a [u8]>,
    {
        self.reset(low, high)?;
        for (idx, record) in records.into_iter().enumerate() {
            self.insert_slot(idx, record)?;
        }
        Ok(())
    }

    pub fn split_into(
        &mut self,
        right: &mut LeafAllocator<'page>,
        original_low_fence: &[u8],
        original_high_fence: &[u8],
        split_idx: usize,
        pending_insert: &LeafPendingInsert,
    ) -> Result<LeafSplitOutcome> {
        let existing_slots = self.slot_meta.len();
        let slots = build_record_slots(existing_slots, pending_insert)?;
        let total_slots = slots.len();
        if total_slots < 2 {
            return Err(SombraError::Invalid(
                "cannot split leaf with fewer than 2 entries",
            ));
        }
        if split_idx == 0 || split_idx >= total_slots {
            return Err(SombraError::Invalid("leaf split index out of range"));
        }

        let payload_copy = {
            let payload = page::payload(&*self.page_bytes)?;
            payload.to_vec()
        };
        if payload_copy.len() != self.payload_len {
            return Err(SombraError::Invalid(
                "leaf payload snapshot length mismatch",
            ));
        }
        let payload_slice = payload_copy.as_slice();
        let pending_bytes = pending_insert.record.as_slice();
        if pending_bytes.is_empty() {
            return Err(SombraError::Invalid("pending leaf record empty"));
        }

        let mut slices = Vec::with_capacity(slots.len());
        for slot in &slots {
            match slot {
                RecordSlot::Existing(idx) => {
                    let meta = self
                        .slot_meta
                        .get(*idx)
                        .ok_or(SombraError::Invalid("missing slot metadata"))?;
                    let start = meta.offset as usize;
                    let end = start
                        .checked_add(meta.len as usize)
                        .ok_or(SombraError::Invalid("leaf record extent overflow"))?;
                    if end > payload_slice.len() {
                        return Err(SombraError::Invalid("leaf record beyond payload"));
                    }
                    slices.push(RecordSlice::Existing { start, end });
                }
                RecordSlot::Pending => slices.push(RecordSlice::Pending),
            }
        }
        let (left_slices, right_slices) = slices.split_at(split_idx);
        let left_first = left_slices
            .first()
            .ok_or(SombraError::Invalid("left split missing entries"))?;
        let right_first = right_slices
            .first()
            .ok_or(SombraError::Invalid("right split missing entries"))?;
        let left_min = decode_split_key(left_first, payload_slice, pending_bytes)?;
        let right_min = decode_split_key(right_first, payload_slice, pending_bytes)?;

        let left_low_fence = if pending_insert.requires_low_fence_update {
            left_min.as_slice()
        } else {
            original_low_fence
        };
        let left_iter = SliceIter::new(left_slices, payload_slice, pending_bytes);
        self.rebuild_from_records(left_low_fence, right_min.as_slice(), left_iter)?;
        let right_iter = SliceIter::new(right_slices, payload_slice, pending_bytes);
        right.rebuild_from_records(right_min.as_slice(), original_high_fence, right_iter)?;

        Ok(LeafSplitOutcome {
            left_min,
            right_min,
        })
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
        meta.slice(payload)
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
            .ok_or(SombraError::Invalid("leaf insert size overflow"))?;
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
            .ok_or(SombraError::Invalid("slot directory overflow"))?;
        if slot_bytes > self.payload_len {
            record_leaf_allocator_failure(LeafAllocatorFailureKind::SlotOverflow);
            return Err(SombraError::Invalid("slot directory exceeds payload"));
        }
        let usable = self
            .payload_len
            .checked_sub(slot_bytes)
            .ok_or(SombraError::Invalid("slot directory exceeds payload"))?;
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
                .ok_or(SombraError::Invalid("leaf free_start overflow"))?;
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
        gap_offset.ok_or(SombraError::Invalid("gap allocation failed"))
    }

    fn compact_all_from(&mut self, new_start: usize) -> Result<()> {
        if self.slot_meta.is_empty() {
            self.header.free_start = u16::try_from(new_start)
                .map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
            self.free_regions.clear();
            return Ok(());
        }
        let total_bytes = self
            .slot_meta
            .iter()
            .map(|meta| meta.len as usize)
            .sum::<usize>();
        if total_bytes == 0 {
            self.header.free_start = u16::try_from(new_start)
                .map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
            self.free_regions.clear();
            return Ok(());
        }
        let mut scratch = Vec::with_capacity(total_bytes);
        {
            let payload = page::payload(&*self.page_bytes)?;
            for meta in &self.slot_meta {
                let start = meta.offset as usize;
                let end = start + meta.len as usize;
                if end > payload.len() {
                    return Err(SombraError::Invalid("record extent beyond payload"));
                }
                scratch.extend_from_slice(&payload[start..end]);
            }
        }
        let payload = page::payload_mut(self.page_bytes)?;
        let mut cursor = new_start;
        let mut copied = 0usize;
        for meta in self.slot_meta.iter_mut() {
            let len = meta.len as usize;
            let end = cursor
                .checked_add(len)
                .ok_or(SombraError::Invalid("record extent overflow"))?;
            if end > payload.len() {
                return Err(SombraError::Invalid("record extent beyond payload"));
            }
            payload[cursor..end].copy_from_slice(&scratch[copied..copied + len]);
            meta.offset = u16::try_from(cursor)
                .map_err(|_| SombraError::Invalid("leaf slot offset overflow"))?;
            cursor = end;
            copied += len;
        }
        self.header.free_start =
            u16::try_from(cursor).map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
        self.free_regions.clear();
        record_leaf_allocator_compaction(total_bytes as u64);
        Ok(())
    }

    fn persist_slot_directory(&mut self) -> Result<()> {
        let slot_count = self.slot_meta.len();
        let slot_bytes = slot_count
            .checked_mul(page::SLOT_ENTRY_LEN)
            .ok_or(SombraError::Invalid("slot directory overflow"))?;
        if slot_bytes > self.payload_len {
            return Err(SombraError::Invalid("slot directory exceeds payload"));
        }
        let new_free_end = self
            .payload_len
            .checked_sub(slot_bytes)
            .ok_or(SombraError::Invalid("slot directory exceeds payload"))?;
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
            .ok_or(SombraError::Invalid("record extent overflow"))?;
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
        while insert_at < self.free_regions.len() && self.free_regions[insert_at].start < start {
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

    #[cfg(debug_assertions)]
    fn debug_assert_ordered(&self) {
        if let Err(err) = self.validate_key_order() {
            panic!("leaf allocator key order invariant violated: {err:?}");
        }
    }

    #[cfg(debug_assertions)]
    fn validate_key_order(&self) -> Result<()> {
        if self.slot_meta.is_empty() {
            return Ok(());
        }
        let payload = page::payload(&*self.page_bytes)?;
        let mut prev: Option<Vec<u8>> = None;
        for meta in &self.slot_meta {
            let slice = meta.slice(payload)?;
            let record = page::decode_leaf_record(slice)?;
            if let Some(prev_key) = prev.as_ref() {
                if prev_key.as_slice() >= record.key {
                    return Err(SombraError::Corruption(
                        "leaf allocator produced non-increasing keys",
                    ));
                }
            }
            prev = Some(record.key.to_vec());
        }
        Ok(())
    }

    #[cfg(not(debug_assertions))]
    fn debug_assert_ordered(&self) {}

    #[cfg(not(debug_assertions))]
    #[allow(dead_code)]
    fn validate_key_order(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
struct SlotMeta {
    offset: u16,
    len: u16,
}

impl SlotMeta {
    fn end(&self) -> u16 {
        self.offset.saturating_add(self.len)
    }

    fn slice<'a>(&self, payload: &'a [u8]) -> Result<&'a [u8]> {
        let start = self.offset as usize;
        let end = start
            .checked_add(self.len as usize)
            .ok_or(SombraError::Invalid("record extent overflow"))?;
        if end > payload.len() {
            return Err(SombraError::Invalid("record extent beyond payload"));
        }
        Ok(&payload[start..end])
    }
}

#[derive(Clone, Copy, Debug)]
enum RecordSlot {
    Existing(usize),
    Pending,
}

fn build_record_slots(
    slot_count: usize,
    pending_insert: &LeafPendingInsert,
) -> Result<Vec<RecordSlot>> {
    if pending_insert.insert_idx > slot_count {
        return Err(SombraError::Invalid(
            "pending insert slot index out of range",
        ));
    }
    if pending_insert.replaces_existing && pending_insert.insert_idx >= slot_count {
        return Err(SombraError::Invalid(
            "pending insert replace index out of range",
        ));
    }
    let total_slots = if pending_insert.replaces_existing {
        slot_count
    } else {
        slot_count
            .checked_add(1)
            .ok_or(SombraError::Invalid("leaf slot count overflow"))?
    };
    let mut slots = Vec::with_capacity(total_slots);
    for idx in 0..=slot_count {
        if idx == pending_insert.insert_idx {
            slots.push(RecordSlot::Pending);
        }
        if idx < slot_count {
            if pending_insert.replaces_existing && idx == pending_insert.insert_idx {
                continue;
            }
            slots.push(RecordSlot::Existing(idx));
        }
    }
    debug_assert_eq!(slots.len(), total_slots);
    Ok(slots)
}

#[derive(Clone, Copy, Debug)]
enum RecordSlice {
    Existing { start: usize, end: usize },
    Pending,
}

struct SliceIter<'a> {
    payload: &'a [u8],
    pending: &'a [u8],
    slices: &'a [RecordSlice],
    pos: usize,
}

impl<'a> SliceIter<'a> {
    fn new(slices: &'a [RecordSlice], payload: &'a [u8], pending: &'a [u8]) -> Self {
        Self {
            payload,
            pending,
            slices,
            pos: 0,
        }
    }
}

impl<'a> Iterator for SliceIter<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        let slice = self.slices.get(self.pos)?;
        self.pos += 1;
        match slice {
            RecordSlice::Existing { start, end } => Some(&self.payload[*start..*end]),
            RecordSlice::Pending => Some(self.pending),
        }
    }
}

fn decode_split_key(slice: &RecordSlice, payload: &[u8], pending: &[u8]) -> Result<Vec<u8>> {
    let record_bytes = match slice {
        RecordSlice::Existing { start, end } => &payload[*start..*end],
        RecordSlice::Pending => pending,
    };
    record_btree_leaf_key_decodes(1);
    let record = page::decode_leaf_record(record_bytes)?;
    record_btree_leaf_memcopy_bytes(record.key.len() as u64);
    Ok(record.key.to_vec())
}

#[derive(Clone, Copy, Debug)]
struct FreeRegion {
    start: u16,
    end: u16,
}

#[derive(Clone, Debug)]
pub(super) struct LeafAllocatorSnapshot {
    slot_meta: Vec<SlotMeta>,
    free_regions: Vec<FreeRegion>,
    arena_start: usize,
    payload_len: usize,
}

impl LeafAllocatorSnapshot {
    pub(super) fn decode_entries(&self, page_bytes: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let payload = page::payload(page_bytes)?;
        let mut entries = Vec::with_capacity(self.slot_meta.len());
        for record in self.record_iter(payload)? {
            let record = record?;
            entries.push((record.key.to_vec(), record.value.to_vec()));
        }
        Ok(entries)
    }

    pub(super) fn record_iter<'a>(&'a self, payload: &'a [u8]) -> Result<LeafSnapshotIter<'a>> {
        if payload.len() != self.payload_len {
            return Err(SombraError::Invalid(
                "leaf allocator snapshot payload mismatch",
            ));
        }
        Ok(LeafSnapshotIter {
            payload,
            slot_meta: &self.slot_meta,
            idx: 0,
        })
    }

    pub(super) fn record_slice<'a>(&'a self, payload: &'a [u8], idx: usize) -> Result<&'a [u8]> {
        let meta = self
            .slot_meta
            .get(idx)
            .ok_or(SombraError::Invalid("snapshot slot index out of bounds"))?;
        meta.slice(payload)
    }

    pub(super) fn record_count(&self) -> usize {
        self.slot_meta.len()
    }

    pub(super) fn encoded_len(&self, idx: usize) -> Result<usize> {
        let meta = self
            .slot_meta
            .get(idx)
            .ok_or(SombraError::Invalid("snapshot slot index out of bounds"))?;
        Ok(meta.len as usize)
    }
}

pub(super) struct LeafSnapshotIter<'a> {
    payload: &'a [u8],
    slot_meta: &'a [SlotMeta],
    idx: usize,
}

impl<'a> Iterator for LeafSnapshotIter<'a> {
    type Item = Result<page::LeafRecordRef<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        let meta = self.slot_meta.get(self.idx)?;
        self.idx += 1;
        let slice = match meta.slice(self.payload) {
            Ok(slice) => slice,
            Err(err) => return Some(Err(err)),
        };
        record_btree_leaf_key_decodes(1);
        match page::decode_leaf_record(slice) {
            Ok(record) => {
                record_btree_leaf_memcopy_bytes(record.key.len() as u64);
                Some(Ok(record))
            }
            Err(err) => Some(Err(err)),
        }
    }
}

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
        self.start = self.start.checked_add(len as u16)?;
        Some(offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::btree::page;
    use crate::types::page::{PageHeader, PageKind, DEFAULT_PAGE_SIZE, PAGE_HDR_LEN};
    use crate::types::{PageId, Result as TestResult, SombraError};
    use proptest::prelude::*;

    #[derive(Clone, Debug)]
    #[allow(dead_code)]
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
            apply_insert(
                &mut buf,
                &mut entries,
                format!("k{i}").into_bytes(),
                vec![i as u8],
            )?;
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
        let header =
            PageHeader::new(PageId(1), PageKind::BTreeLeaf, page_size as u32, 0)?.with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        page::write_initial_header(&mut buf[PAGE_HDR_LEN..], page::BTreePageKind::Leaf)?;
        Ok(buf)
    }

    fn is_full(err: &SombraError) -> bool {
        matches!(
            err,
            SombraError::Invalid("leaf page full")
                | SombraError::Invalid("slot directory exceeds payload")
        )
    }

    #[test]
    fn low_fence_update_preserves_existing_records() -> TestResult<()> {
        let mut buf = blank_leaf(DEFAULT_PAGE_SIZE as usize)?;
        let header = page::Header::parse(&buf)?;
        let mut allocator = LeafAllocator::new(&mut buf, header)?;
        let value = vec![1u8];
        let key_a = vec![10, 20, 30, 40];
        let key_b = vec![5, 15, 25, 35];
        let mut record = Vec::new();
        page::encode_leaf_record(&key_a, &value, &mut record)?;
        allocator.insert_slot(0, &record)?;
        allocator.update_low_fence(&key_a)?;
        record.clear();
        page::encode_leaf_record(&key_b, &value, &mut record)?;
        allocator.insert_slot(0, &record)?;
        allocator.update_low_fence(&key_b)?;
        let first = allocator.leaf_record(0)?;
        let second = allocator.leaf_record(1)?;
        assert_eq!(first.key, key_b.as_slice());
        assert_eq!(second.key, key_a.as_slice());
        Ok(())
    }
}
