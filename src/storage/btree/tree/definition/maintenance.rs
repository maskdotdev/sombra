impl<K: KeyCodec, V: ValCodec> BTree<K, V> {
    fn build_leaf_layout(
        &self,
        payload_len: usize,
        low_fence: &[u8],
        high_fence: &[u8],
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<Option<LeafLayout>> {
        let fences_end = page::PAYLOAD_HEADER_LEN + low_fence.len() + high_fence.len();
        let slot_bytes = entries
            .len()
            .checked_mul(page::SLOT_ENTRY_LEN)
            .ok_or_else(|| SombraError::Invalid("slot directory overflow"))?;
        if fences_end > payload_len {
            return Err(SombraError::Invalid("fence data exceeds payload"));
        }
        if slot_bytes > payload_len {
            return Err(SombraError::Invalid("slot directory exceeds payload"));
        }
        let new_free_end = payload_len
            .checked_sub(slot_bytes)
            .ok_or_else(|| SombraError::Invalid("slot directory larger than payload"))?;
        if new_free_end < fences_end {
            return Ok(None);
        }
        let max_records_bytes = new_free_end - fences_end;
        let mut records = Vec::new();
        let mut offsets = Vec::with_capacity(entries.len());
        let mut lengths = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let record_len = page::plain_leaf_record_encoded_len(key.len(), value.len())?;
            if records.len() + record_len > max_records_bytes {
                return Ok(None);
            }
            let offset = fences_end + records.len();
            let offset_u16 = u16::try_from(offset)
                .map_err(|_| SombraError::Invalid("record offset exceeds u16"))?;
            offsets.push(offset_u16);
            let record_len_u16 = u16::try_from(record_len)
                .map_err(|_| SombraError::Invalid("record length exceeds u16"))?;
            lengths.push(record_len_u16);
            page::encode_leaf_record(key, value, &mut records)?;
        }
        let free_start = fences_end + records.len();
        if free_start > new_free_end {
            return Ok(None);
        }
        let free_start_u16 = u16::try_from(free_start)
            .map_err(|_| SombraError::Invalid("free_start exceeds u16"))?;
        let free_end_u16 = u16::try_from(new_free_end)
            .map_err(|_| SombraError::Invalid("free_end exceeds u16"))?;
        Ok(Some(LeafLayout {
            records,
            offsets,
            lengths,
            free_start: free_start_u16,
            free_end: free_end_u16,
        }))
    }

    fn apply_leaf_layout(
        &self,
        page: &mut PageMut<'_>,
        header: &page::Header,
        fences_end: usize,
        layout: &LeafLayout,
    ) -> Result<()> {
        let payload = page::payload_mut(page.data_mut())?;
        let new_free_end = layout.free_end as usize;
        if fences_end > payload.len() || new_free_end > payload.len() {
            return Err(SombraError::Invalid("leaf layout exceeds payload"));
        }
        payload[fences_end..new_free_end].fill(0);
        let record_end = fences_end + layout.records.len();
        if record_end > new_free_end {
            return Err(SombraError::Invalid("leaf layout overflows payload"));
        }
        payload[fences_end..record_end].copy_from_slice(&layout.records);
        let slot_count_u16 = u16::try_from(layout.offsets.len())
            .map_err(|_| SombraError::Invalid("leaf slot count exceeds u16"))?;
        page::set_slot_count(payload, slot_count_u16);
        page::set_free_start(payload, layout.free_start);
        page::set_free_end(payload, layout.free_end);
        debug_assert_eq!(layout.offsets.len(), layout.lengths.len());
        for i in 0..layout.offsets.len() {
            let pos = new_free_end + i * page::SLOT_ENTRY_LEN;
            page::write_slot_entry(payload, pos, layout.offsets[i], layout.lengths[i]);
        }
        // Preserve parent/sibling metadata.
        if let Some(parent) = header.parent {
            page::set_parent(payload, Some(parent));
        }
        page::set_left_sibling(payload, header.left_sibling);
        page::set_right_sibling(payload, header.right_sibling);
        Ok(())
    }

    fn apply_leaf_fences(
        &self,
        page: &mut PageMut<'_>,
        low: &[u8],
        high: Option<&[u8]>,
    ) -> Result<()> {
        let payload = page::payload_mut(page.data_mut())?;
        page::set_low_fence(payload, low)?;
        if let Some(high_bytes) = high {
            page::set_high_fence(payload, high_bytes)?;
        } else {
            page::set_high_fence(payload, &[])?;
        }
        Ok(())
    }
}

impl<K: KeyCodec, V: ValCodec> BTree<K, V> {
    fn init_leaf_page(&self, page_id: PageId, page: &mut PageMut<'_>) -> Result<()> {
        let buf = page.data_mut();
        if buf.len() < self.page_size {
            return Err(SombraError::Invalid(
                "page buffer shorter than configured size",
            ));
        }
        buf[..self.page_size].fill(0);
        let header = PageHeader::new(
            page_id,
            PageKind::BTreeLeaf,
            self.page_size as u32,
            self.salt,
        )?
        .with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        page::write_initial_header(
            &mut buf[PAGE_HDR_LEN..self.page_size],
            page::BTreePageKind::Leaf,
        )?;
        Ok(())
    }

    fn init_internal_page(&self, page_id: PageId, page: &mut PageMut<'_>) -> Result<()> {
        let buf = page.data_mut();
        if buf.len() < self.page_size {
            return Err(SombraError::Invalid(
                "page buffer shorter than configured size",
            ));
        }
        buf[..self.page_size].fill(0);
        let header = PageHeader::new(
            page_id,
            PageKind::BTreeInternal,
            self.page_size as u32,
            self.salt,
        )?
        .with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        page::write_initial_header(
            &mut buf[PAGE_HDR_LEN..self.page_size],
            page::BTreePageKind::Internal,
        )
    }

    fn build_internal_layout(
        &self,
        payload_len: usize,
        low_fence: &[u8],
        high_fence: &[u8],
        entries: &[(Vec<u8>, PageId)],
    ) -> Result<Option<InternalLayout>> {
        let fences_end = page::PAYLOAD_HEADER_LEN + low_fence.len() + high_fence.len();
        let slot_bytes = entries
            .len()
            .checked_mul(page::SLOT_ENTRY_LEN)
            .ok_or_else(|| SombraError::Invalid("slot directory overflow"))?;
        if fences_end > payload_len {
            return Err(SombraError::Invalid("fence data exceeds payload"));
        }
        if slot_bytes > payload_len {
            return Err(SombraError::Invalid("slot directory exceeds payload"));
        }
        let new_free_end = payload_len
            .checked_sub(slot_bytes)
            .ok_or_else(|| SombraError::Invalid("slot directory larger than payload"))?;
        if new_free_end < fences_end {
            return Ok(None);
        }
        let max_records_bytes = new_free_end - fences_end;
        let mut records = Vec::new();
        let mut offsets = Vec::with_capacity(entries.len());
        let mut lengths = Vec::with_capacity(entries.len());
        for (key, child) in entries {
            let record_len = page::INTERNAL_RECORD_HEADER_LEN + key.len();
            if records.len() + record_len > max_records_bytes {
                return Ok(None);
            }
            let offset = fences_end + records.len();
            let offset_u16 = u16::try_from(offset)
                .map_err(|_| SombraError::Invalid("record offset exceeds u16"))?;
            offsets.push(offset_u16);
            let record_len_u16 = u16::try_from(record_len)
                .map_err(|_| SombraError::Invalid("internal record length exceeds u16"))?;
            lengths.push(record_len_u16);
            page::encode_internal_record(key, *child, &mut records);
        }
        let free_start = fences_end + records.len();
        if free_start > new_free_end {
            return Ok(None);
        }
        let free_start_u16 = u16::try_from(free_start)
            .map_err(|_| SombraError::Invalid("free_start exceeds u16"))?;
        let free_end_u16 = u16::try_from(new_free_end)
            .map_err(|_| SombraError::Invalid("free_end exceeds u16"))?;
        Ok(Some(InternalLayout {
            records,
            offsets,
            lengths,
            free_start: free_start_u16,
            free_end: free_end_u16,
        }))
    }

    fn apply_internal_layout(
        &self,
        page: &mut PageMut<'_>,
        header: &page::Header,
        fences_end: usize,
        layout: &InternalLayout,
    ) -> Result<()> {
        let payload = page::payload_mut(page.data_mut())?;
        let new_free_end = layout.free_end as usize;
        if fences_end > payload.len() || new_free_end > payload.len() {
            return Err(SombraError::Invalid("internal layout exceeds payload"));
        }
        payload[fences_end..new_free_end].fill(0);
        let record_end = fences_end + layout.records.len();
        if record_end > new_free_end {
            return Err(SombraError::Invalid("internal layout overflows payload"));
        }
        payload[fences_end..record_end].copy_from_slice(&layout.records);
        let slot_count_u16 = u16::try_from(layout.offsets.len())
            .map_err(|_| SombraError::Invalid("internal slot count exceeds u16"))?;
        page::set_slot_count(payload, slot_count_u16);
        page::set_free_start(payload, layout.free_start);
        page::set_free_end(payload, layout.free_end);
        for i in 0..layout.offsets.len() {
            let pos = new_free_end + i * page::SLOT_ENTRY_LEN;
            page::write_slot_entry(payload, pos, layout.offsets[i], layout.lengths[i]);
        }
        if let Some(parent) = header.parent {
            page::set_parent(payload, Some(parent));
        }
        page::set_left_sibling(payload, header.left_sibling);
        page::set_right_sibling(payload, header.right_sibling);
        Ok(())
    }

    fn apply_internal_fences(
        &self,
        page: &mut PageMut<'_>,
        low: &[u8],
        high: Option<&[u8]>,
    ) -> Result<()> {
        let payload = page::payload_mut(page.data_mut())?;
        page::set_low_fence(payload, low)?;
        if let Some(high_bytes) = high {
            page::set_high_fence(payload, high_bytes)?;
        } else {
            page::set_high_fence(payload, &[])?;
        }
        Ok(())
    }

    fn snapshot_leaf(&self, header: &page::Header, data: &[u8]) -> Result<LeafSnapshot> {
        let slot_view = SlotView::new(header, data)?;
        let (low_fence_bytes, high_fence_bytes) = header.fence_slices(data)?;
        let low_vec = low_fence_bytes.to_vec();
        let high_vec = high_fence_bytes.to_vec();
        let mut entries = Vec::with_capacity(slot_view.len());
        for idx in 0..slot_view.len() {
            let rec_slice = slot_view.slice(idx)?;
            let record = page::decode_leaf_record(rec_slice)?;
            entries.push((record.key.to_vec(), record.value.to_vec()));
        }
        Ok(LeafSnapshot {
            entries,
            low_fence: low_vec,
            high_fence: high_vec,
        })
    }

    fn snapshot_internal(&self, header: &page::Header, data: &[u8]) -> Result<InternalSnapshot> {
        let slot_view = SlotView::new(header, data)?;
        let (low_fence_bytes, high_fence_bytes) = header.fence_slices(data)?;
        let low_vec = low_fence_bytes.to_vec();
        let high_vec = high_fence_bytes.to_vec();
        let mut entries = Vec::with_capacity(slot_view.len());
        for idx in 0..slot_view.len() {
            let rec_slice = slot_view.slice(idx)?;
            let record = page::decode_internal_record(rec_slice)?;
            entries.push((record.separator.to_vec(), record.child));
        }
        Ok(InternalSnapshot {
            entries,
            low_fence: low_vec,
            high_fence: high_vec,
        })
    }

    fn fill_percent(payload_len: usize, free_start: u16, free_end: u16) -> u8 {
        if payload_len == 0 {
            return 0;
        }
        let free_start = free_start as usize;
        let free_end = free_end as usize;
        let free_bytes = free_end.saturating_sub(free_start);
        let used = payload_len.saturating_sub(free_bytes);
        ((used * 100) / payload_len) as u8
    }

    fn write_leaf_empty(
        &self,
        page: &mut PageMut<'_>,
        header: &page::Header,
        low_fence: &[u8],
        high_fence: &[u8],
    ) -> Result<()> {
        let payload = page::payload_mut(page.data_mut())?;
        let payload_len = payload.len();
        let fences_end = page::PAYLOAD_HEADER_LEN + low_fence.len() + high_fence.len();
        if fences_end > payload_len {
            return Err(SombraError::Invalid("fence data exceeds payload"));
        }
        if fences_end < payload_len {
            payload[fences_end..payload_len].fill(0);
        }
        let free_start = u16::try_from(fences_end)
            .map_err(|_| SombraError::Invalid("free_start exceeds u16"))?;
        let free_end = u16::try_from(payload_len)
            .map_err(|_| SombraError::Invalid("payload length exceeds u16"))?;
        page::set_slot_count(payload, 0);
        page::set_free_start(payload, free_start);
        page::set_free_end(payload, free_end);
        if let Some(parent) = header.parent {
            page::set_parent(payload, Some(parent));
        } else {
            page::set_parent(payload, None);
        }
        page::set_left_sibling(payload, header.left_sibling);
        page::set_right_sibling(payload, header.right_sibling);
        let high_opt = if high_fence.is_empty() {
            None
        } else {
            Some(high_fence)
        };
        self.apply_leaf_fences(page, low_fence, high_opt)?;
        Ok(())
    }

    fn internal_layout_or_err(
        &self,
        payload_len: usize,
        low_fence: &[u8],
        high_fence: &[u8],
        entries: &[(Vec<u8>, PageId)],
    ) -> Result<InternalLayout> {
        self.build_internal_layout(payload_len, low_fence, high_fence, entries)?
            .ok_or_else(|| SombraError::Invalid("internal layout exceeds capacity"))
    }

    fn update_parent_separator(
        &self,
        tx: &mut WriteGuard<'_>,
        parent_frame: &PathEntry,
        new_key: &[u8],
    ) -> Result<()> {
        self.update_parent_separator_at_index(
            tx,
            parent_frame.page_id,
            parent_frame.slot_index,
            new_key,
        )
    }

    fn update_parent_separator_at_index(
        &self,
        tx: &mut WriteGuard<'_>,
        parent_id: PageId,
        slot_index: usize,
        new_key: &[u8],
    ) -> Result<()> {
        let mut page = tx.page_mut(parent_id)?;
        let header = page::Header::parse(page.data())?;
        let payload_len = page::payload(page.data())?.len();
        let InternalSnapshot {
            mut entries,
            low_fence: _,
            high_fence,
        } = self.snapshot_internal(&header, page.data())?;
        if slot_index >= entries.len() {
            return Err(SombraError::Corruption("parent slot index out of range"));
        }
        entries[slot_index].0 = new_key.to_vec();
        let low_slice = entries
            .first()
            .map(|(k, _)| k.as_slice())
            .ok_or_else(|| SombraError::Corruption("internal node has no entries"))?;
        let fences_end = page::PAYLOAD_HEADER_LEN + low_slice.len() + high_fence.len();
        let layout = self
            .build_internal_layout(payload_len, low_slice, high_fence.as_slice(), &entries)?
            .ok_or_else(|| SombraError::Invalid("internal layout after delete exceeds capacity"))?;
        self.apply_internal_layout(&mut page, &header, fences_end, &layout)?;
        let high_opt = if high_fence.is_empty() {
            None
        } else {
            Some(high_fence.as_slice())
        };
        self.apply_internal_fences(&mut page, low_slice, high_opt)?;
        Ok(())
    }

    fn try_borrow_from_left(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_payload_len: usize,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        left_id: PageId,
    ) -> Result<BorrowResult> {
        if self.options.in_place_leaf_edits
            && self.borrow_from_left_in_place(tx, leaf_id, leaf_header, parent_frame, left_id)?
        {
            self.stats.inc_leaf_rebalance_in_place();
            record_btree_leaf_rebalance_in_place(1);
            return Ok(BorrowResult::Borrowed);
        }

        let result = self.borrow_from_left_rebuild(
            tx,
            leaf_id,
            leaf_payload_len,
            leaf_header,
            leaf_snapshot,
            parent_frame,
            left_id,
        )?;
        if matches!(result, BorrowResult::Borrowed) {
            self.stats.inc_leaf_rebalance_rebuilds();
            record_btree_leaf_rebalance_rebuilds(1);
        }
        Ok(result)
    }

    fn borrow_from_left_rebuild(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_payload_len: usize,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        left_id: PageId,
    ) -> Result<BorrowResult> {
        let left_page = tx.page_mut(left_id)?;
        let left_header = page::Header::parse(left_page.data())?;
        if left_header.parent != leaf_header.parent {
            return Ok(BorrowResult::LayoutOverflow);
        }
        let left_payload_len = page::payload(left_page.data())?.len();
        let left_snapshot = self.snapshot_leaf(&left_header, left_page.data())?;
        drop(left_page);

        if left_snapshot.entries.len() <= 1 {
            return Ok(BorrowResult::InsufficientDonor);
        }

        let mut left_entries = left_snapshot.entries.clone();
        let borrowed = left_entries
            .pop()
            .ok_or_else(|| SombraError::Corruption("left leaf empty during borrow"))?;
        let mut leaf_entries = leaf_snapshot.entries.clone();
        leaf_entries.insert(0, borrowed.clone());

        let left_low = left_entries
            .first()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| SombraError::Corruption("left leaf lost first key"))?;
        let new_leaf_first = leaf_entries
            .first()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| SombraError::Corruption("borrowed leaf has no keys"))?;

        let left_layout = match self.build_leaf_layout(
            left_payload_len,
            left_low.as_slice(),
            new_leaf_first.as_slice(),
            &left_entries,
        )? {
            Some(layout) => layout,
            None => return Ok(BorrowResult::LayoutOverflow),
        };
        let leaf_layout = match self.build_leaf_layout(
            leaf_payload_len,
            new_leaf_first.as_slice(),
            leaf_snapshot.high_fence.as_slice(),
            &leaf_entries,
        )? {
            Some(layout) => layout,
            None => return Ok(BorrowResult::LayoutOverflow),
        };

        {
            let mut page = tx.page_mut(left_id)?;
            let fences_end = page::PAYLOAD_HEADER_LEN + left_low.len() + new_leaf_first.len();
            self.apply_leaf_layout(&mut page, &left_header, fences_end, &left_layout)?;
            self.apply_leaf_fences(
                &mut page,
                left_low.as_slice(),
                Some(new_leaf_first.as_slice()),
            )?;
        }
        {
            let mut page = tx.page_mut(leaf_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + new_leaf_first.len() + leaf_snapshot.high_fence.len();
            self.apply_leaf_layout(&mut page, leaf_header, fences_end, &leaf_layout)?;
            let high_opt = if leaf_snapshot.high_fence.is_empty() {
                None
            } else {
                Some(leaf_snapshot.high_fence.as_slice())
            };
            self.apply_leaf_fences(&mut page, new_leaf_first.as_slice(), high_opt)?;
        }
        self.update_parent_separator(tx, parent_frame, new_leaf_first.as_slice())?;
        Ok(BorrowResult::Borrowed)
    }

    fn borrow_from_left_in_place(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_header: &page::Header,
        parent_frame: &PathEntry,
        left_id: PageId,
    ) -> Result<bool> {
        let mut left_page = tx.page_mut(left_id)?;
        let left_header = page::Header::parse(left_page.data())?;
        if left_header.parent != leaf_header.parent {
            return Ok(false);
        }
        if left_header.slot_count <= 1 {
            return Ok(false);
        }

        let left_data = left_page.data();
        let slot_view = SlotView::new(&left_header, left_data)?;
        if slot_view.len() == 0 {
            return Ok(false);
        }
        let donor_idx = slot_view.len() - 1;
        let rec_slice = slot_view.slice(donor_idx)?;
        let record = page::decode_leaf_record(rec_slice)?;
        let borrowed_key = record.key.to_vec();
        let borrowed_val = record.value.to_vec();
        let record_len =
            page::plain_leaf_record_encoded_len(borrowed_key.len(), borrowed_val.len())?;

        let free_gap =
            (leaf_header.free_end as usize).saturating_sub(leaf_header.free_start as usize);
        let needed = record_len
            .checked_add(page::SLOT_ENTRY_LEN)
            .ok_or_else(|| SombraError::Invalid("leaf borrow size overflow"))?;
        if free_gap < needed {
            return Ok(false);
        }
        if leaf_header.low_fence_len != borrowed_key.len()
            || left_header.high_fence_len != borrowed_key.len()
        {
            return Ok(false);
        }

        // Delete from the donor first to ensure we can fall back by reinserting.
        if self
            .try_delete_leaf_in_place(
                &mut left_page,
                &left_header,
                borrowed_key.as_slice(),
                false,
                None,
            )?
            .is_none()
        {
            return Ok(false);
        }
        drop(left_page);

        let mut applied = false;
        {
            let mut page = tx.page_mut(leaf_id)?;
            if self.try_insert_leaf_in_place(
                &mut page,
                leaf_header,
                borrowed_key.as_slice(),
                borrowed_val.as_slice(),
            )? {
                applied = true;
            }
        }
        if !applied {
            // Reinsert into the left sibling to restore original state.
            let mut left_page = tx.page_mut(left_id)?;
            let refreshed_header = page::Header::parse(left_page.data())?;
            let _ = self.try_insert_leaf_in_place(
                &mut left_page,
                &refreshed_header,
                borrowed_key.as_slice(),
                borrowed_val.as_slice(),
            )?;
            return Ok(false);
        }

        let mut left_page = tx.page_mut(left_id)?;
        {
            let payload = page::payload_mut(left_page.data_mut())?;
            page::set_high_fence(payload, borrowed_key.as_slice())?;
        }
        drop(left_page);

        self.update_parent_separator(tx, parent_frame, borrowed_key.as_slice())?;
        Ok(true)
    }

    fn leaf_append_cost(entries: &[(Vec<u8>, Vec<u8>)]) -> Result<usize> {
        let mut total = 0usize;
        for (key, value) in entries {
            let record_len = page::plain_leaf_record_encoded_len(key.len(), value.len())?;
            total = total
                .checked_add(record_len)
                .and_then(|acc| acc.checked_add(page::SLOT_ENTRY_LEN))
                .ok_or_else(|| SombraError::Invalid("leaf append size overflow"))?;
        }
        Ok(total)
    }

    fn borrow_from_right_rebuild(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_payload_len: usize,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        right_id: PageId,
    ) -> Result<bool> {
        let right_page = tx.page_mut(right_id)?;
        let right_header = page::Header::parse(right_page.data())?;
        let right_payload_len = page::payload(right_page.data())?.len();
        if right_header.parent != leaf_header.parent {
            return Ok(false);
        }
        let right_snapshot = self.snapshot_leaf(&right_header, right_page.data())?;
        drop(right_page);

        if right_snapshot.entries.len() <= 1 {
            return Ok(false);
        }

        let mut right_entries = right_snapshot.entries.clone();
        let borrowed = right_entries.remove(0);
        let mut leaf_entries = leaf_snapshot.entries.clone();
        leaf_entries.push(borrowed.clone());

        let new_leaf_first = leaf_entries
            .first()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| SombraError::Corruption("leaf empty after borrowing from right"))?;
        let right_new_first = right_entries
            .first()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| SombraError::Corruption("right leaf empty after lending"))?;

        let right_layout = match self.build_leaf_layout(
            right_payload_len,
            right_new_first.as_slice(),
            right_snapshot.high_fence.as_slice(),
            &right_entries,
        )? {
            Some(layout) => layout,
            None => return Ok(false),
        };
        let leaf_layout = match self.build_leaf_layout(
            leaf_payload_len,
            new_leaf_first.as_slice(),
            right_new_first.as_slice(),
            &leaf_entries,
        )? {
            Some(layout) => layout,
            None => return Ok(false),
        };

        {
            let mut page = tx.page_mut(right_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + right_new_first.len() + right_snapshot.high_fence.len();
            self.apply_leaf_layout(&mut page, &right_header, fences_end, &right_layout)?;
            let high_opt = if right_snapshot.high_fence.is_empty() {
                None
            } else {
                Some(right_snapshot.high_fence.as_slice())
            };
            self.apply_leaf_fences(&mut page, right_new_first.as_slice(), high_opt)?;
        }
        {
            let mut page = tx.page_mut(leaf_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + new_leaf_first.len() + right_new_first.len();
            self.apply_leaf_layout(&mut page, leaf_header, fences_end, &leaf_layout)?;
            self.apply_leaf_fences(
                &mut page,
                new_leaf_first.as_slice(),
                Some(right_new_first.as_slice()),
            )?;
        }

        if K::compare_encoded(
            new_leaf_first.as_slice(),
            leaf_snapshot.low_fence.as_slice(),
        ) != Ordering::Equal
        {
            self.update_parent_separator(tx, parent_frame, new_leaf_first.as_slice())?;
        }
        self.update_parent_separator_at_index(
            tx,
            parent_frame.page_id,
            parent_frame.slot_index + 1,
            right_new_first.as_slice(),
        )?;

        Ok(true)
    }

    fn borrow_from_right_in_place(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        right_id: PageId,
    ) -> Result<bool> {
        let mut right_page = tx.page_mut(right_id)?;
        let right_header = page::Header::parse(right_page.data())?;
        if right_header.parent != leaf_header.parent {
            return Ok(false);
        }
        if right_header.slot_count <= 1 {
            return Ok(false);
        }

        let right_data = right_page.data();
        let slot_view = SlotView::new(&right_header, right_data)?;
        if slot_view.len() < 2 {
            return Ok(false);
        }
        let first_slice = slot_view.slice(0)?;
        let record = page::decode_leaf_record(first_slice)?;
        let borrowed_key = record.key.to_vec();
        let borrowed_val = record.value.to_vec();
        let record_len =
            page::plain_leaf_record_encoded_len(borrowed_key.len(), borrowed_val.len())?;

        let second_slice = slot_view.slice(1)?;
        let next_record = page::decode_leaf_record(second_slice)?;
        let right_new_first = next_record.key.to_vec();

        if right_header.low_fence_len != right_new_first.len()
            || leaf_header.high_fence_len != right_new_first.len()
        {
            return Ok(false);
        }

        let free_gap =
            (leaf_header.free_end as usize).saturating_sub(leaf_header.free_start as usize);
        let needed = record_len
            .checked_add(page::SLOT_ENTRY_LEN)
            .ok_or_else(|| SombraError::Invalid("leaf borrow size overflow"))?;
        if free_gap < needed {
            return Ok(false);
        }

        // Insert into the recipient before deleting from the donor so we can bail out cleanly.
        let mut appended = false;
        {
            let mut page = tx.page_mut(leaf_id)?;
            if self.try_insert_leaf_in_place(
                &mut page,
                leaf_header,
                borrowed_key.as_slice(),
                borrowed_val.as_slice(),
            )? {
                appended = true;
            }
        }
        if !appended {
            return Ok(false);
        }

        let delete_result = self.try_delete_leaf_in_place(
            &mut right_page,
            &right_header,
            borrowed_key.as_slice(),
            true,
            Some(right_new_first.as_slice()),
        )?;
        if delete_result.is_none() {
            // Remove the appended key to keep state consistent.
            let mut page = tx.page_mut(leaf_id)?;
            let refreshed = page::Header::parse(page.data())?;
            let _ = self.try_delete_leaf_in_place(
                &mut page,
                &refreshed,
                borrowed_key.as_slice(),
                false,
                None,
            )?;
            return Ok(false);
        }
        drop(right_page);

        {
            let mut page = tx.page_mut(leaf_id)?;
            let payload = page::payload_mut(page.data_mut())?;
            page::set_high_fence(payload, right_new_first.as_slice())?;
        }

        let new_leaf_first = if let Some((key, _)) = leaf_snapshot.entries.first() {
            key.clone()
        } else {
            borrowed_key.clone()
        };
        if leaf_snapshot.entries.is_empty()
            || K::compare_encoded(
                new_leaf_first.as_slice(),
                leaf_snapshot.low_fence.as_slice(),
            ) != Ordering::Equal
        {
            self.update_parent_separator(tx, parent_frame, new_leaf_first.as_slice())?;
        }
        self.update_parent_separator_at_index(
            tx,
            parent_frame.page_id,
            parent_frame.slot_index + 1,
            right_new_first.as_slice(),
        )?;
        Ok(true)
    }

    fn try_borrow_from_right(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_payload_len: usize,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        right_id: PageId,
    ) -> Result<bool> {
        if self.options.in_place_leaf_edits
            && self.borrow_from_right_in_place(
                tx,
                leaf_id,
                leaf_header,
                leaf_snapshot,
                parent_frame,
                right_id,
            )?
        {
            self.stats.inc_leaf_rebalance_in_place();
            record_btree_leaf_rebalance_in_place(1);
            return Ok(true);
        }

        let result = self.borrow_from_right_rebuild(
            tx,
            leaf_id,
            leaf_payload_len,
            leaf_header,
            leaf_snapshot,
            parent_frame,
            right_id,
        )?;
        if result {
            self.stats.inc_leaf_rebalance_rebuilds();
            record_btree_leaf_rebalance_rebuilds(1);
        }
        Ok(result)
    }

    fn merge_leaf_with_left(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        path: &[PathEntry],
        left_id: PageId,
    ) -> Result<bool> {
        let left_page = tx.page_mut(left_id)?;
        let left_header = page::Header::parse(left_page.data())?;
        if left_header.parent != leaf_header.parent {
            return Err(SombraError::Corruption(
                "left sibling parent mismatch during merge",
            ));
        }
        let left_payload_len = page::payload(left_page.data())?.len();
        let left_snapshot = self.snapshot_leaf(&left_header, left_page.data())?;
        drop(left_page);

        if self.options.in_place_leaf_edits
            && self.merge_leaf_with_left_in_place(
                tx,
                leaf_id,
                leaf_header,
                leaf_snapshot,
                parent_frame,
                path,
                left_id,
                &left_snapshot,
            )?
        {
            self.stats.inc_leaf_merges();
            self.stats.inc_leaf_rebalance_in_place();
            record_btree_leaf_rebalance_in_place(1);
            tracing::trace!(
                target: "sombra_btree::merge",
                survivor = left_id.0,
                removed = leaf_id.0,
                direction = "left",
                "merged leaf into left sibling (in-place)"
            );
            return Ok(true);
        }

        let merged = self.merge_leaf_with_left_rebuild(
            tx,
            leaf_id,
            leaf_header,
            leaf_snapshot,
            parent_frame,
            path,
            left_id,
            left_payload_len,
            &left_header,
            &left_snapshot,
        )?;
        if merged {
            self.stats.inc_leaf_rebalance_rebuilds();
            record_btree_leaf_rebalance_rebuilds(1);
        }
        Ok(merged)
    }

    fn merge_leaf_with_left_rebuild(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        path: &[PathEntry],
        left_id: PageId,
        left_payload_len: usize,
        left_header: &page::Header,
        left_snapshot: &LeafSnapshot,
    ) -> Result<bool> {
        let removal_index = parent_frame.slot_index;
        let mut combined = left_snapshot.entries.clone();
        combined.extend_from_slice(&leaf_snapshot.entries);
        if combined.is_empty() {
            tx.free_page(leaf_id)?;
            self.stats.inc_leaf_merges();
            tracing::trace!(
                target: "sombra_btree::merge",
                survivor = left_id.0,
                removed = leaf_id.0,
                direction = "left",
                "merged empty leaf into left sibling"
            );
            self.remove_child_entry(tx, path.to_vec(), parent_frame.clone(), removal_index)?;
            return Ok(true);
        }
        let new_low = combined[0].0.clone();
        let primary_layout = self.build_leaf_layout(
            left_payload_len,
            new_low.as_slice(),
            leaf_snapshot.high_fence.as_slice(),
            &combined,
        )?;
        let layout = match primary_layout {
            Some(layout) => layout,
            None => {
                let fallback = self.build_leaf_layout(
                    left_payload_len,
                    left_snapshot.low_fence.as_slice(),
                    leaf_snapshot.high_fence.as_slice(),
                    &combined,
                )?;
                match fallback {
                    Some(layout) => layout,
                    None => {
                        return Ok(false);
                    }
                }
            }
        };
        {
            let mut page = tx.page_mut(left_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + new_low.len() + leaf_snapshot.high_fence.len();
            self.apply_leaf_layout(&mut page, left_header, fences_end, &layout)?;
            let high_opt = if leaf_snapshot.high_fence.is_empty() {
                None
            } else {
                Some(leaf_snapshot.high_fence.as_slice())
            };
            self.apply_leaf_fences(&mut page, new_low.as_slice(), high_opt)?;
            let payload = page::payload_mut(page.data_mut())?;
            page::set_right_sibling(payload, leaf_header.right_sibling);
        }
        if let Some(right_id) = leaf_header.right_sibling {
            let mut right_page = tx.page_mut(right_id)?;
            let payload = page::payload_mut(right_page.data_mut())?;
            page::set_left_sibling(payload, Some(left_id));
        }
        tx.free_page(leaf_id)?;

        if removal_index == 0 {
            return Err(SombraError::Corruption(
                "expected left sibling to precede current child",
            ));
        }
        let left_index = removal_index - 1;
        if K::compare_encoded(new_low.as_slice(), left_snapshot.low_fence.as_slice())
            != Ordering::Equal
        {
            self.update_parent_separator_at_index(
                tx,
                parent_frame.page_id,
                left_index,
                new_low.as_slice(),
            )?;
        }
        self.stats.inc_leaf_merges();
        tracing::trace!(
            target: "sombra_btree::merge",
            survivor = left_id.0,
            removed = leaf_id.0,
            direction = "left",
            "merged leaf into left sibling"
        );
        self.remove_child_entry(tx, path.to_vec(), parent_frame.clone(), removal_index)?;
        Ok(true)
    }

    fn merge_leaf_with_left_in_place(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        path: &[PathEntry],
        left_id: PageId,
        left_snapshot: &LeafSnapshot,
    ) -> Result<bool> {
        let mut left_page = tx.page_mut(left_id)?;
        let left_header = page::Header::parse(left_page.data())?;
        if left_header.parent != leaf_header.parent {
            return Err(SombraError::Corruption(
                "left sibling parent mismatch during merge",
            ));
        }

        let total_cost = Self::leaf_append_cost(&leaf_snapshot.entries)?;
        let free_gap =
            (left_header.free_end as usize).saturating_sub(left_header.free_start as usize);
        if free_gap < total_cost {
            return Ok(false);
        }
        if left_header.high_fence_len != leaf_snapshot.high_fence.len() {
            return Ok(false);
        }

        for (key, value) in &leaf_snapshot.entries {
            let header = page::Header::parse(left_page.data())?;
            if !self.try_insert_leaf_in_place(
                &mut left_page,
                &header,
                key.as_slice(),
                value.as_slice(),
            )? {
                return Ok(false);
            }
        }
        {
            let payload = page::payload_mut(left_page.data_mut())?;
            page::set_right_sibling(payload, leaf_header.right_sibling);
            page::set_high_fence(payload, leaf_snapshot.high_fence.as_slice())?;
        }
        drop(left_page);

        if let Some(right_id) = leaf_header.right_sibling {
            let mut right_page = tx.page_mut(right_id)?;
            let payload = page::payload_mut(right_page.data_mut())?;
            page::set_left_sibling(payload, Some(left_id));
        }
        tx.free_page(leaf_id)?;

        let removal_index = parent_frame.slot_index;
        if removal_index == 0 {
            return Err(SombraError::Corruption(
                "expected left sibling to precede current child",
            ));
        }
        let left_index = removal_index - 1;
        let new_low = if let Some((key, _)) = left_snapshot.entries.first() {
            key.clone()
        } else if let Some((key, _)) = leaf_snapshot.entries.first() {
            key.clone()
        } else {
            return Ok(false);
        };
        if K::compare_encoded(new_low.as_slice(), left_snapshot.low_fence.as_slice())
            != Ordering::Equal
        {
            self.update_parent_separator_at_index(
                tx,
                parent_frame.page_id,
                left_index,
                new_low.as_slice(),
            )?;
        }
        self.remove_child_entry(tx, path.to_vec(), parent_frame.clone(), removal_index)?;
        Ok(true)
    }

    fn merge_leaf_with_right(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_payload_len: usize,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        path: &[PathEntry],
        right_id: PageId,
    ) -> Result<bool> {
        let right_page = tx.page_mut(right_id)?;
        let right_header = page::Header::parse(right_page.data())?;
        if right_header.parent != leaf_header.parent {
            return Err(SombraError::Corruption(
                "right sibling parent mismatch during merge",
            ));
        }
        let right_snapshot = self.snapshot_leaf(&right_header, right_page.data())?;
        drop(right_page);

        if self.options.in_place_leaf_edits
            && self.merge_leaf_with_right_in_place(
                tx,
                leaf_id,
                leaf_header,
                leaf_snapshot,
                parent_frame,
                path,
                right_id,
                &right_header,
                &right_snapshot,
            )?
        {
            self.stats.inc_leaf_merges();
            self.stats.inc_leaf_rebalance_in_place();
            record_btree_leaf_rebalance_in_place(1);
            tracing::trace!(
                target: "sombra_btree::merge",
                survivor = leaf_id.0,
                removed = right_id.0,
                direction = "right",
                "merged right leaf into current leaf (in-place)"
            );
            return Ok(true);
        }

        let merged = self.merge_leaf_with_right_rebuild(
            tx,
            leaf_id,
            leaf_payload_len,
            leaf_header,
            leaf_snapshot,
            parent_frame,
            path,
            right_id,
            &right_header,
            &right_snapshot,
        )?;
        if merged {
            self.stats.inc_leaf_rebalance_rebuilds();
            record_btree_leaf_rebalance_rebuilds(1);
        }
        Ok(merged)
    }

    fn merge_leaf_with_right_rebuild(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_payload_len: usize,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        path: &[PathEntry],
        right_id: PageId,
        right_header: &page::Header,
        right_snapshot: &LeafSnapshot,
    ) -> Result<bool> {
        let removal_index = parent_frame.slot_index + 1;
        let mut combined = leaf_snapshot.entries.clone();
        combined.extend_from_slice(&right_snapshot.entries);
        if combined.is_empty() {
            tx.free_page(right_id)?;
            self.stats.inc_leaf_merges();
            tracing::trace!(
                target: "sombra_btree::merge",
                survivor = leaf_id.0,
                removed = right_id.0,
                direction = "right",
                "merged empty right leaf into current leaf"
            );
            self.remove_child_entry(tx, path.to_vec(), parent_frame.clone(), removal_index)?;
            return Ok(true);
        }
        let new_low = combined[0].0.clone();
        let primary_layout = self.build_leaf_layout(
            leaf_payload_len,
            new_low.as_slice(),
            right_snapshot.high_fence.as_slice(),
            &combined,
        )?;
        let layout = match primary_layout {
            Some(layout) => layout,
            None => {
                let fallback = self.build_leaf_layout(
                    leaf_payload_len,
                    leaf_snapshot.low_fence.as_slice(),
                    right_snapshot.high_fence.as_slice(),
                    &combined,
                )?;
                match fallback {
                    Some(layout) => layout,
                    None => {
                        return Ok(false);
                    }
                }
            }
        };
        {
            let mut page = tx.page_mut(leaf_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + new_low.len() + right_snapshot.high_fence.len();
            self.apply_leaf_layout(&mut page, leaf_header, fences_end, &layout)?;
            let high_opt = if right_snapshot.high_fence.is_empty() {
                None
            } else {
                Some(right_snapshot.high_fence.as_slice())
            };
            self.apply_leaf_fences(&mut page, new_low.as_slice(), high_opt)?;
            let payload = page::payload_mut(page.data_mut())?;
            page::set_right_sibling(payload, right_header.right_sibling);
        }
        if let Some(next_id) = right_header.right_sibling {
            let mut next_page = tx.page_mut(next_id)?;
            let payload = page::payload_mut(next_page.data_mut())?;
            page::set_left_sibling(payload, Some(leaf_id));
        }
        tx.free_page(right_id)?;

        if K::compare_encoded(new_low.as_slice(), leaf_snapshot.low_fence.as_slice())
            != Ordering::Equal
        {
            self.update_parent_separator(tx, parent_frame, new_low.as_slice())?;
        }
        self.stats.inc_leaf_merges();
        tracing::trace!(
            target: "sombra_btree::merge",
            survivor = leaf_id.0,
            removed = right_id.0,
            direction = "right",
            "merged right leaf into current leaf"
        );
        self.remove_child_entry(tx, path.to_vec(), parent_frame.clone(), removal_index)?;
        Ok(true)
    }

    fn merge_leaf_with_right_in_place(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        leaf_header: &page::Header,
        leaf_snapshot: &LeafSnapshot,
        parent_frame: &PathEntry,
        path: &[PathEntry],
        right_id: PageId,
        right_header: &page::Header,
        right_snapshot: &LeafSnapshot,
    ) -> Result<bool> {
        let mut leaf_page = tx.page_mut(leaf_id)?;
        let total_cost = Self::leaf_append_cost(&right_snapshot.entries)?;
        let free_gap =
            (leaf_header.free_end as usize).saturating_sub(leaf_header.free_start as usize);
        if free_gap < total_cost {
            return Ok(false);
        }
        if leaf_header.high_fence_len != right_snapshot.high_fence.len() {
            return Ok(false);
        }

        for (key, value) in &right_snapshot.entries {
            let header = page::Header::parse(leaf_page.data())?;
            if !self.try_insert_leaf_in_place(
                &mut leaf_page,
                &header,
                key.as_slice(),
                value.as_slice(),
            )? {
                return Ok(false);
            }
        }
        {
            let payload = page::payload_mut(leaf_page.data_mut())?;
            page::set_right_sibling(payload, right_header.right_sibling);
            page::set_high_fence(payload, right_snapshot.high_fence.as_slice())?;
        }
        drop(leaf_page);

        if let Some(next_id) = right_header.right_sibling {
            let mut next_page = tx.page_mut(next_id)?;
            let payload = page::payload_mut(next_page.data_mut())?;
            page::set_left_sibling(payload, Some(leaf_id));
        }
        tx.free_page(right_id)?;

        let removal_index = parent_frame.slot_index + 1;
        self.remove_child_entry(tx, path.to_vec(), parent_frame.clone(), removal_index)?;

        let new_low = if let Some((key, _)) = leaf_snapshot.entries.first() {
            key.clone()
        } else if let Some((key, _)) = right_snapshot.entries.first() {
            key.clone()
        } else {
            return Ok(false);
        };
        if leaf_snapshot.entries.is_empty()
            || K::compare_encoded(new_low.as_slice(), leaf_snapshot.low_fence.as_slice())
                != Ordering::Equal
        {
            self.update_parent_separator(tx, parent_frame, new_low.as_slice())?;
        }
        Ok(true)
    }

    fn remove_child_entry(
        &self,
        tx: &mut WriteGuard<'_>,
        path: Vec<PathEntry>,
        parent_frame: PathEntry,
        remove_index: usize,
    ) -> Result<()> {
        let parent_id = parent_frame.page_id;
        let page = tx.page_mut(parent_id)?;
        let header = page::Header::parse(page.data())?;
        let payload_len = page::payload(page.data())?.len();
        let snapshot = self.snapshot_internal(&header, page.data())?;
        drop(page);

        if remove_index >= snapshot.entries.len() {
            return Err(SombraError::Corruption(
                "internal remove index out of range",
            ));
        }
        let mut entries = snapshot.entries.clone();
        entries.remove(remove_index);

        if entries.is_empty() {
            if header.parent.is_none() {
                return Ok(());
            } else {
                return Err(SombraError::Corruption("internal node lost all children"));
            }
        }

        let new_low = entries[0].0.clone();
        let layout = self.internal_layout_or_err(
            payload_len,
            new_low.as_slice(),
            snapshot.high_fence.as_slice(),
            &entries,
        )?;
        {
            let mut page = tx.page_mut(parent_id)?;
            let fences_end = page::PAYLOAD_HEADER_LEN + new_low.len() + snapshot.high_fence.len();
            self.apply_internal_layout(&mut page, &header, fences_end, &layout)?;
            let high_opt = if snapshot.high_fence.is_empty() {
                None
            } else {
                Some(snapshot.high_fence.as_slice())
            };
            self.apply_internal_fences(&mut page, new_low.as_slice(), high_opt)?;
        }

        if header.parent.is_none() && entries.len() == 1 {
            self.promote_child_to_root(tx, parent_id, entries[0].1)?;
            return Ok(());
        }

        for (_, child) in entries.iter() {
            self.set_parent_pointer(tx, *child, Some(parent_id))?;
        }

        if let Some(grandparent_frame) = path.last() {
            if K::compare_encoded(new_low.as_slice(), snapshot.low_fence.as_slice())
                != Ordering::Equal
            {
                self.update_parent_separator(tx, grandparent_frame, new_low.as_slice())?;
            }
        }

        let fill = Self::fill_percent(payload_len, layout.free_start, layout.free_end);
        if header.parent.is_some() && fill < self.options.internal_min_fill {
            self.rebalance_internal(tx, parent_id, path)
        } else {
            Ok(())
        }
    }

    fn rebalance_internal(
        &self,
        tx: &mut WriteGuard<'_>,
        node_id: PageId,
        mut path: Vec<PathEntry>,
    ) -> Result<()> {
        let grandparent_frame = match path.pop() {
            Some(frame) => frame,
            None => return Ok(()),
        };
        let node_page = tx.page_mut(node_id)?;
        let node_header = page::Header::parse(node_page.data())?;
        let payload_len = page::payload(node_page.data())?.len();
        let snapshot = self.snapshot_internal(&node_header, node_page.data())?;
        drop(node_page);

        if node_header.parent != Some(grandparent_frame.page_id) {
            return Err(SombraError::Corruption(
                "internal node parent mismatch during rebalance",
            ));
        }

        if let Some(left_id) = node_header.left_sibling {
            if self.try_borrow_internal_from_left(
                tx,
                node_id,
                payload_len,
                &node_header,
                &snapshot,
                &grandparent_frame,
                left_id,
            )? {
                return Ok(());
            }
        }
        if let Some(right_id) = node_header.right_sibling {
            if self.try_borrow_internal_from_right(
                tx,
                node_id,
                payload_len,
                &node_header,
                &snapshot,
                &grandparent_frame,
                right_id,
            )? {
                return Ok(());
            }
        }

        if let Some(left_id) = node_header.left_sibling {
            return self.merge_internal_with_left(
                tx,
                node_id,
                payload_len,
                node_header,
                &snapshot,
                grandparent_frame,
                path,
                left_id,
            );
        }
        if let Some(right_id) = node_header.right_sibling {
            return self.merge_internal_with_right(
                tx,
                node_id,
                payload_len,
                node_header,
                &snapshot,
                grandparent_frame,
                path,
                right_id,
            );
        }

        Err(SombraError::Invalid(
            "no siblings available for internal rebalance",
        ))
    }

    fn try_borrow_internal_from_left(
        &self,
        tx: &mut WriteGuard<'_>,
        node_id: PageId,
        node_payload_len: usize,
        node_header: &page::Header,
        node_snapshot: &InternalSnapshot,
        grandparent_frame: &PathEntry,
        left_id: PageId,
    ) -> Result<bool> {
        let left_page = tx.page_mut(left_id)?;
        let left_header = page::Header::parse(left_page.data())?;
        if left_header.parent != node_header.parent {
            return Ok(false);
        }
        let left_payload_len = page::payload(left_page.data())?.len();
        let left_snapshot = self.snapshot_internal(&left_header, left_page.data())?;
        drop(left_page);

        if left_snapshot.entries.len() <= 1 {
            return Ok(false);
        }

        let mut left_entries = left_snapshot.entries.clone();
        let borrowed = left_entries
            .pop()
            .ok_or_else(|| SombraError::Corruption("left internal empty during borrow"))?;
        let mut node_entries = node_snapshot.entries.clone();
        node_entries.insert(0, borrowed.clone());

        let left_low = left_entries
            .first()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| SombraError::Corruption("left internal lost first key"))?;
        let new_node_first = node_entries
            .first()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| SombraError::Corruption("node internal empty after borrow"))?;

        let left_layout = self.internal_layout_or_err(
            left_payload_len,
            left_low.as_slice(),
            new_node_first.as_slice(),
            &left_entries,
        )?;
        let node_layout = self.internal_layout_or_err(
            node_payload_len,
            new_node_first.as_slice(),
            node_snapshot.high_fence.as_slice(),
            &node_entries,
        )?;

        let left_fill = Self::fill_percent(
            left_payload_len,
            left_layout.free_start,
            left_layout.free_end,
        );
        if left_fill < self.options.internal_min_fill {
            return Ok(false);
        }

        {
            let mut page = tx.page_mut(left_id)?;
            let fences_end = page::PAYLOAD_HEADER_LEN + left_low.len() + new_node_first.len();
            self.apply_internal_layout(&mut page, &left_header, fences_end, &left_layout)?;
            self.apply_internal_fences(
                &mut page,
                left_low.as_slice(),
                Some(new_node_first.as_slice()),
            )?;
            let payload = page::payload_mut(page.data_mut())?;
            page::set_right_sibling(payload, Some(node_id));
        }
        {
            let mut page = tx.page_mut(node_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + new_node_first.len() + node_snapshot.high_fence.len();
            self.apply_internal_layout(&mut page, node_header, fences_end, &node_layout)?;
            let high_opt = if node_snapshot.high_fence.is_empty() {
                None
            } else {
                Some(node_snapshot.high_fence.as_slice())
            };
            self.apply_internal_fences(&mut page, new_node_first.as_slice(), high_opt)?;
        }
        self.update_parent_separator(tx, grandparent_frame, new_node_first.as_slice())?;
        self.set_parent_pointer(tx, borrowed.1, Some(node_id))?;
        Ok(true)
    }

    fn merge_internal_with_left(
        &self,
        tx: &mut WriteGuard<'_>,
        node_id: PageId,
        _node_payload_len: usize,
        node_header: page::Header,
        node_snapshot: &InternalSnapshot,
        grandparent_frame: PathEntry,
        path: Vec<PathEntry>,
        left_id: PageId,
    ) -> Result<()> {
        let left_page = tx.page_mut(left_id)?;
        let left_header = page::Header::parse(left_page.data())?;
        if left_header.parent != node_header.parent {
            return Err(SombraError::Corruption(
                "left internal parent mismatch during merge",
            ));
        }
        let left_payload_len = page::payload(left_page.data())?.len();
        let left_snapshot = self.snapshot_internal(&left_header, left_page.data())?;
        drop(left_page);

        let removal_index = grandparent_frame.slot_index;
        let mut combined = left_snapshot.entries.clone();
        combined.extend_from_slice(&node_snapshot.entries);
        if combined.is_empty() {
            tx.free_page(node_id)?;
            self.stats.inc_internal_merges();
            tracing::trace!(
                target: "sombra_btree::merge",
                survivor = left_id.0,
                removed = node_id.0,
                kind = "internal",
                direction = "left",
                "merged empty internal node into left sibling"
            );
            return self.remove_child_entry(tx, path, grandparent_frame, removal_index);
        }
        let new_low = combined[0].0.clone();
        let layout = self.internal_layout_or_err(
            left_payload_len,
            new_low.as_slice(),
            node_snapshot.high_fence.as_slice(),
            &combined,
        )?;
        {
            let mut page = tx.page_mut(left_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + new_low.len() + node_snapshot.high_fence.len();
            self.apply_internal_layout(&mut page, &left_header, fences_end, &layout)?;
            let high_opt = if node_snapshot.high_fence.is_empty() {
                None
            } else {
                Some(node_snapshot.high_fence.as_slice())
            };
            self.apply_internal_fences(&mut page, new_low.as_slice(), high_opt)?;
            let payload = page::payload_mut(page.data_mut())?;
            page::set_right_sibling(payload, node_header.right_sibling);
        }
        if let Some(right_id) = node_header.right_sibling {
            let mut right_page = tx.page_mut(right_id)?;
            let payload = page::payload_mut(right_page.data_mut())?;
            page::set_left_sibling(payload, Some(left_id));
        }
        for (_, child) in combined.iter() {
            self.set_parent_pointer(tx, *child, Some(left_id))?;
        }
        tx.free_page(node_id)?;

        if removal_index == 0 {
            return Err(SombraError::Corruption(
                "expected left internal sibling to precede node",
            ));
        }
        let left_index = removal_index - 1;
        if K::compare_encoded(new_low.as_slice(), left_snapshot.low_fence.as_slice())
            != Ordering::Equal
        {
            self.update_parent_separator_at_index(
                tx,
                grandparent_frame.page_id,
                left_index,
                new_low.as_slice(),
            )?;
        }
        self.stats.inc_internal_merges();
        tracing::trace!(
            target: "sombra_btree::merge",
            survivor = left_id.0,
            removed = node_id.0,
            kind = "internal",
            direction = "left",
            "merged internal node into left sibling"
        );
        self.remove_child_entry(tx, path, grandparent_frame, removal_index)
    }

    fn merge_internal_with_right(
        &self,
        tx: &mut WriteGuard<'_>,
        node_id: PageId,
        node_payload_len: usize,
        node_header: page::Header,
        node_snapshot: &InternalSnapshot,
        grandparent_frame: PathEntry,
        path: Vec<PathEntry>,
        right_id: PageId,
    ) -> Result<()> {
        let right_page = tx.page_mut(right_id)?;
        let right_header = page::Header::parse(right_page.data())?;
        if right_header.parent != node_header.parent {
            return Err(SombraError::Corruption(
                "right internal parent mismatch during merge",
            ));
        }
        let right_snapshot = self.snapshot_internal(&right_header, right_page.data())?;
        drop(right_page);

        let removal_index = grandparent_frame.slot_index + 1;
        let mut combined = node_snapshot.entries.clone();
        combined.extend_from_slice(&right_snapshot.entries);
        if combined.is_empty() {
            tx.free_page(right_id)?;
            self.stats.inc_internal_merges();
            tracing::trace!(
                target: "sombra_btree::merge",
                survivor = node_id.0,
                removed = right_id.0,
                kind = "internal",
                direction = "right",
                "merged empty right internal node"
            );
            return self.remove_child_entry(tx, path, grandparent_frame, removal_index);
        }
        let new_low = combined[0].0.clone();
        let layout = self.internal_layout_or_err(
            node_payload_len,
            new_low.as_slice(),
            right_snapshot.high_fence.as_slice(),
            &combined,
        )?;
        {
            let mut page = tx.page_mut(node_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + new_low.len() + right_snapshot.high_fence.len();
            self.apply_internal_layout(&mut page, &node_header, fences_end, &layout)?;
            let high_opt = if right_snapshot.high_fence.is_empty() {
                None
            } else {
                Some(right_snapshot.high_fence.as_slice())
            };
            self.apply_internal_fences(&mut page, new_low.as_slice(), high_opt)?;
            let payload = page::payload_mut(page.data_mut())?;
            page::set_right_sibling(payload, right_header.right_sibling);
        }
        if let Some(next_id) = right_header.right_sibling {
            let mut next_page = tx.page_mut(next_id)?;
            let payload = page::payload_mut(next_page.data_mut())?;
            page::set_left_sibling(payload, Some(node_id));
        }
        for (_, child) in combined.iter() {
            self.set_parent_pointer(tx, *child, Some(node_id))?;
        }
        tx.free_page(right_id)?;

        if K::compare_encoded(new_low.as_slice(), node_snapshot.low_fence.as_slice())
            != Ordering::Equal
        {
            self.update_parent_separator(tx, &grandparent_frame, new_low.as_slice())?;
        }
        self.stats.inc_internal_merges();
        tracing::trace!(
            target: "sombra_btree::merge",
            survivor = node_id.0,
            removed = right_id.0,
            kind = "internal",
            direction = "right",
            "merged internal node into right sibling"
        );
        self.remove_child_entry(tx, path, grandparent_frame, removal_index)
    }

    fn promote_child_to_root(
        &self,
        tx: &mut WriteGuard<'_>,
        parent_id: PageId,
        child_id: PageId,
    ) -> Result<()> {
        self.set_parent_pointer(tx, child_id, None)?;
        self.root.store(child_id.0, AtomicOrdering::SeqCst);
        tx.free_page(parent_id)?;
        Ok(())
    }

    fn try_borrow_internal_from_right(
        &self,
        tx: &mut WriteGuard<'_>,
        node_id: PageId,
        node_payload_len: usize,
        node_header: &page::Header,
        node_snapshot: &InternalSnapshot,
        grandparent_frame: &PathEntry,
        right_id: PageId,
    ) -> Result<bool> {
        let right_page = tx.page_mut(right_id)?;
        let right_header = page::Header::parse(right_page.data())?;
        if right_header.parent != node_header.parent {
            return Ok(false);
        }
        let right_payload_len = page::payload(right_page.data())?.len();
        let right_snapshot = self.snapshot_internal(&right_header, right_page.data())?;
        drop(right_page);

        if right_snapshot.entries.len() <= 1 {
            return Ok(false);
        }

        let mut right_entries = right_snapshot.entries.clone();
        let borrowed = right_entries.remove(0);
        let mut node_entries = node_snapshot.entries.clone();
        node_entries.push(borrowed.clone());

        let new_node_first = node_entries
            .first()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| SombraError::Corruption("node internal empty after borrow"))?;
        let right_new_first = right_entries
            .first()
            .map(|(k, _)| k.clone())
            .ok_or_else(|| SombraError::Corruption("right internal empty after lend"))?;

        let right_layout = self.internal_layout_or_err(
            right_payload_len,
            right_new_first.as_slice(),
            right_snapshot.high_fence.as_slice(),
            &right_entries,
        )?;
        let node_layout = self.internal_layout_or_err(
            node_payload_len,
            new_node_first.as_slice(),
            right_new_first.as_slice(),
            &node_entries,
        )?;

        let right_fill = Self::fill_percent(
            right_payload_len,
            right_layout.free_start,
            right_layout.free_end,
        );
        if right_fill < self.options.internal_min_fill {
            return Ok(false);
        }

        {
            let mut page = tx.page_mut(right_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + right_new_first.len() + right_snapshot.high_fence.len();
            self.apply_internal_layout(&mut page, &right_header, fences_end, &right_layout)?;
            let high_opt = if right_snapshot.high_fence.is_empty() {
                None
            } else {
                Some(right_snapshot.high_fence.as_slice())
            };
            self.apply_internal_fences(&mut page, right_new_first.as_slice(), high_opt)?;
            let payload = page::payload_mut(page.data_mut())?;
            page::set_left_sibling(payload, Some(node_id));
        }
        {
            let mut page = tx.page_mut(node_id)?;
            let fences_end =
                page::PAYLOAD_HEADER_LEN + new_node_first.len() + right_new_first.len();
            self.apply_internal_layout(&mut page, node_header, fences_end, &node_layout)?;
            self.apply_internal_fences(
                &mut page,
                new_node_first.as_slice(),
                Some(right_new_first.as_slice()),
            )?;
        }

        if K::compare_encoded(
            new_node_first.as_slice(),
            node_snapshot.low_fence.as_slice(),
        ) != Ordering::Equal
        {
            self.update_parent_separator(tx, grandparent_frame, new_node_first.as_slice())?;
        }
        self.update_parent_separator_at_index(
            tx,
            grandparent_frame.page_id,
            grandparent_frame.slot_index + 1,
            right_new_first.as_slice(),
        )?;
        self.set_parent_pointer(tx, borrowed.1, Some(node_id))?;
        Ok(true)
    }
    fn rebalance_leaf(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        mut path: Vec<PathEntry>,
        mut snapshot_override: Option<LeafSnapshot>,
        mut new_first_key: Option<Vec<u8>>,
    ) -> Result<()> {
        let parent_frame = match path.pop() {
            Some(frame) => frame,
            None => return Ok(()),
        };
        let leaf_page = tx.page_mut(leaf_id)?;
        let leaf_header = page::Header::parse(leaf_page.data())?;
        let leaf_payload_len = page::payload(leaf_page.data())?.len();
        let leaf_snapshot = if let Some(snapshot) = snapshot_override.take() {
            drop(leaf_page);
            snapshot
        } else {
            let snapshot = self.snapshot_leaf(&leaf_header, leaf_page.data())?;
            drop(leaf_page);
            snapshot
        };

        if leaf_header.parent != Some(parent_frame.page_id) {
            return Err(SombraError::Corruption(
                "leaf parent mismatch during rebalance",
            ));
        }
        let has_left = leaf_header.left_sibling.is_some();
        let has_right = leaf_header.right_sibling.is_some();

        let mut left_insufficient = false;
        if let Some(left_id) = leaf_header.left_sibling {
            match self.try_borrow_from_left(
                tx,
                leaf_id,
                leaf_payload_len,
                &leaf_header,
                &leaf_snapshot,
                &parent_frame,
                left_id,
            )? {
                BorrowResult::Borrowed => return Ok(()),
                BorrowResult::InsufficientDonor => left_insufficient = true,
                BorrowResult::LayoutOverflow => {}
            }
        }

        if let Some(right_id) = leaf_header.right_sibling {
            if self.try_borrow_from_right(
                tx,
                leaf_id,
                leaf_payload_len,
                &leaf_header,
                &leaf_snapshot,
                &parent_frame,
                right_id,
            )? {
                return Ok(());
            }
        }

        let force_merge = self.options.page_fill_target >= 100;
        if let Some(left_id) = leaf_header.left_sibling {
            if leaf_snapshot.entries.is_empty() || !left_insufficient || force_merge {
                if self.merge_leaf_with_left(
                    tx,
                    leaf_id,
                    &leaf_header,
                    &leaf_snapshot,
                    &parent_frame,
                    &path,
                    left_id,
                )? {
                    return Ok(());
                }
            }
        }
        if let Some(right_id) = leaf_header.right_sibling {
            if leaf_snapshot.entries.is_empty() || force_merge {
                if self.merge_leaf_with_right(
                    tx,
                    leaf_id,
                    leaf_payload_len,
                    &leaf_header,
                    &leaf_snapshot,
                    &parent_frame,
                    &path,
                    right_id,
                )? {
                    return Ok(());
                }
            }
        }

        if let Some(first_key) = new_first_key.take() {
            self.update_parent_separator(tx, &parent_frame, first_key.as_slice())?;
        }

        if !has_left && !has_right {
            return Err(SombraError::Invalid(
                "no siblings available for leaf rebalance",
            ));
        }

        Ok(())
    }

    fn create_new_root(
        &self,
        tx: &mut WriteGuard<'_>,
        left: PageId,
        right: PageId,
        left_min: Vec<u8>,
        right_min: Vec<u8>,
    ) -> Result<()> {
        let payload_len = self
            .page_size
            .checked_sub(PAGE_HDR_LEN)
            .ok_or_else(|| SombraError::Invalid("page size smaller than header"))?;
        let entries = vec![(left_min.clone(), left), (right_min.clone(), right)];
        let layout = self
            .build_internal_layout(payload_len, left_min.as_slice(), &[], &entries)?
            .ok_or_else(|| SombraError::Invalid("internal root layout too large"))?;
        let new_root_id = tx.allocate_page()?;
        {
            let mut root_page = tx.page_mut(new_root_id)?;
            self.init_internal_page(new_root_id, &mut root_page)?;
            let header = page::Header::parse(root_page.data())?;
            let root_low = entries[0].0.as_slice();
            let fences_end = page::PAYLOAD_HEADER_LEN + root_low.len();
            self.apply_internal_layout(&mut root_page, &header, fences_end, &layout)?;
            self.apply_internal_fences(&mut root_page, root_low, None)?;
        }
        {
            let mut left_page = tx.page_mut(left)?;
            let payload = page::payload_mut(left_page.data_mut())?;
            page::set_parent(payload, Some(new_root_id));
        }
        {
            let mut right_page = tx.page_mut(right)?;
            let payload = page::payload_mut(right_page.data_mut())?;
            page::set_parent(payload, Some(new_root_id));
        }
        tracing::trace!(
            target: "sombra_btree::split",
            new_root = new_root_id.0,
            left = left.0,
            right = right.0,
            "created new root after split"
        );
        self.root.store(new_root_id.0, AtomicOrdering::SeqCst);
        Ok(())
    }

    fn leftmost_leaf_id_with_write(&self, tx: &mut WriteGuard<'_>) -> Result<PageId> {
        let mut current = PageId(self.root.load(AtomicOrdering::SeqCst));
        loop {
            let page = tx.page_mut(current)?;
            let header = page::Header::parse(page.data())?;
            match header.kind {
                page::BTreePageKind::Leaf => {
                    drop(page);
                    return Ok(current);
                }
                page::BTreePageKind::Internal => {
                    let slot_view = SlotView::new(&header, page.data())?;
                    if slot_view.len() == 0 {
                        return Err(SombraError::Corruption("internal node without slots"));
                    }
                    let rec_slice = slot_view.slice(0)?;
                    let record = page::decode_internal_record(rec_slice)?;
                    let next = record.child;
                    drop(page);
                    current = next;
                }
            }
        }
    }
}
