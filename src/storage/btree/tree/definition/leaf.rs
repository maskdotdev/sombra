impl<K: KeyCodec, V: ValCodec> BTree<K, V> {
    fn find_leaf_mut(
        &self,
        tx: &mut WriteGuard<'_>,
        key: &[u8],
    ) -> Result<(PageId, page::Header, Vec<PathEntry>)> {
        let mut current = PageId(self.root.load(AtomicOrdering::SeqCst));
        let mut path = Vec::new();
        loop {
            let page = tx.page_mut(current)?;
            let header = page::Header::parse(page.data())?;
            if header.kind == page::BTreePageKind::Leaf {
                self.stats.inc_leaf_searches();
                tracing::trace!(
                    target: "sombra_btree::search",
                    page = current.0,
                    kind = "leaf_mut",
                    "located mutable leaf"
                );
                drop(page);
                return Ok((current, header, path));
            }
            self.stats.inc_internal_searches();
            let (next, slot_index) = self.choose_child_with_slot(page.data(), &header, key)?;
            let entry = PathEntry {
                page_id: current,
                slot_index,
            };
            drop(page);
            path.push(entry);
            current = next;
        }
    }

    fn try_reuse_leaf(
        &self,
        tx: &mut WriteGuard<'_>,
        cache: LeafCache,
        key: &[u8],
    ) -> Result<Option<(PageId, page::Header, Vec<PathEntry>)>> {
        match self.leaf_header_for_key(tx, cache.leaf_id, key)? {
            Some(header) => Ok(Some((cache.leaf_id, header, cache.path))),
            None => Ok(None),
        }
    }

    fn leaf_header_for_key(
        &self,
        tx: &mut WriteGuard<'_>,
        leaf_id: PageId,
        key: &[u8],
    ) -> Result<Option<page::Header>> {
        let page = self.store.get_page_with_write(tx, leaf_id)?;
        let header = page::Header::parse(page.data())?;
        let (low, high) = header.fence_slices(page.data())?;
        if K::compare_encoded(key, low) == Ordering::Less {
            return Ok(None);
        }
        if !high.is_empty() && K::compare_encoded(key, high) != Ordering::Less {
            return Ok(None);
        }
        Ok(Some(header))
    }

    fn search_leaf(&self, page: &PageRef, header: &page::Header, key: &[u8]) -> Result<Option<V>> {
        self.search_leaf_bytes(page.data(), header, key)
    }

    fn choose_child_from_bytes(
        &self,
        data: &[u8],
        header: &page::Header,
        key: &[u8],
    ) -> Result<PageId> {
        let (child, _) = self.choose_child_with_slot(data, header, key)?;
        Ok(child)
    }

    fn choose_child_with_slot(
        &self,
        data: &[u8],
        header: &page::Header,
        key: &[u8],
    ) -> Result<(PageId, usize)> {
        if header.slot_count == 0 {
            return Err(SombraError::Corruption("internal node without slots"));
        }
        let slot_view = SlotView::new(header, data)?;
        let slot_len = slot_view.len();
        let mut lo = 0usize;
        let mut hi = slot_len;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let rec_slice = slot_view.slice(mid)?;
            let record = page::decode_internal_record(rec_slice)?;
            match K::compare_encoded(key, record.separator) {
                Ordering::Less => hi = mid,
                _ => lo = mid + 1,
            }
        }
        let idx = if lo == 0 {
            0
        } else {
            (lo - 1).min(slot_len - 1)
        };
        let rec_slice = slot_view.slice(idx)?;
        let record = page::decode_internal_record(rec_slice)?;
        Ok((record.child, idx))
    }

    fn search_leaf_bytes(
        &self,
        data: &[u8],
        header: &page::Header,
        key: &[u8],
    ) -> Result<Option<V>> {
        let _scope = profile_scope(StorageProfileKind::BTreeLeafSearch);
        let slot_view = SlotView::new(header, data)?;
        if slot_view.len() == 0 {
            return Ok(None);
        }
        let mut lo = 0usize;
        let mut hi = slot_view.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let rec_slice = slot_view.slice(mid)?;
            let record = page::decode_leaf_record(rec_slice)?;
            record_btree_leaf_key_decodes(1);
            record_btree_leaf_key_cmps(1);
            match K::compare_encoded(record.key, key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => {
                    let value = V::decode_val(record.value)?;
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }

    fn insert_into_leaf(
        &self,
        tx: &mut WriteGuard<'_>,
        mut page: PageMut<'_>,
        header: page::Header,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<LeafInsert> {
        let _scope = profile_scope(StorageProfileKind::BTreeLeafInsert);
        if self.options.in_place_leaf_edits {
            if self.try_insert_leaf_in_place(
                &mut page,
                &header,
                key.as_slice(),
                value.as_slice(),
            )? {
                self.stats.inc_leaf_in_place_edits();
                return Ok(LeafInsert::Done);
            }
        }

        let data = page.data();
        let slot_view = SlotView::new(&header, data)?;
        let payload = slot_view.payload();
        let (low_fence, high_fence) = header.fence_slices(data)?;
        let low_fence_vec = low_fence.to_vec();
        let high_fence_vec = high_fence.to_vec();

        let mut entries = Vec::with_capacity(slot_view.len());
        for idx in 0..slot_view.len() {
            let rec_slice = slot_view.slice(idx)?;
            record_btree_leaf_key_decodes(1);
            let record = page::decode_leaf_record(rec_slice)?;
            record_btree_leaf_memcopy_bytes(record.key.len() as u64);
            entries.push((record.key.to_vec(), record.value.to_vec()));
        }

        match entries.binary_search_by(|(existing, _)| {
            record_btree_leaf_key_cmps(1);
            K::compare_encoded(existing, key.as_slice())
        }) {
            Ok(idx) => {
                entries[idx].1 = value;
            }
            Err(idx) => {
                entries.insert(idx, (key, value));
            }
        }

        let payload_len = payload.len();
        let high_slice_existing = high_fence_vec.as_slice();
        let new_low_slice = entries[0].0.as_slice();
        let fences_end_inline =
            page::PAYLOAD_HEADER_LEN + new_low_slice.len() + high_slice_existing.len();
        if let Some(layout) =
            self.build_leaf_layout(payload_len, new_low_slice, high_slice_existing, &entries)?
        {
            self.apply_leaf_layout(&mut page, &header, fences_end_inline, &layout)?;
            let new_low = entries[0].0.clone();
            let high_opt = if high_fence_vec.is_empty() {
                None
            } else {
                Some(high_fence_vec.as_slice())
            };
            self.apply_leaf_fences(&mut page, new_low.as_slice(), high_opt)?;
            self.stats.inc_leaf_rebuilds();
            return Ok(LeafInsert::Done);
        }

        // Need to split this leaf.
        let len = entries.len();
        if len < 2 {
            return Err(SombraError::Invalid(
                "cannot split leaf with fewer than 2 entries",
            ));
        }
        let mut candidates: Vec<usize> = (1..len).collect();
        let mid = len / 2;
        candidates.sort_by_key(|idx| idx.abs_diff(mid));
        let mut left_layout = None;
        let mut right_layout = None;
        let mut split_at = None;
        for idx in candidates {
            let left_slice = &entries[..idx];
            let right_slice = &entries[idx..];
            if left_slice.is_empty() || right_slice.is_empty() {
                continue;
            }
            let left_high = right_slice.first().map(|(k, _)| k.as_slice()).unwrap();
            let right_low = right_slice.first().map(|(k, _)| k.as_slice()).unwrap();
            let left_try = self.build_leaf_layout(
                payload_len,
                low_fence_vec.as_slice(),
                left_high,
                left_slice,
            )?;
            let right_try = self.build_leaf_layout(
                payload_len,
                right_low,
                high_fence_vec.as_slice(),
                right_slice,
            )?;
            if let (Some(l), Some(r)) = (left_try, right_try) {
                split_at = Some(idx);
                left_layout = Some(l);
                right_layout = Some(r);
                break;
            }
        }
        let split_at = split_at
            .ok_or_else(|| SombraError::Invalid("unable to split leaf into fitting halves"))?;
        let left_layout = left_layout.expect("left layout");
        let right_layout = right_layout.expect("right layout");
        let left_min = entries[0].0.clone();
        let right_min = entries[split_at].0.clone();

        let left_fences_end = page::PAYLOAD_HEADER_LEN + low_fence_vec.len() + right_min.len();
        self.apply_leaf_layout(&mut page, &header, left_fences_end, &left_layout)?;

        let page_id = page.id;
        drop(page);

        let new_page_id = tx.allocate_page()?;
        {
            let mut right_page = tx.page_mut(new_page_id)?;
            self.init_leaf_page(new_page_id, &mut right_page)?;
            let right_header = page::Header::parse(right_page.data())?;
            self.apply_leaf_layout(
                &mut right_page,
                &right_header,
                page::PAYLOAD_HEADER_LEN + right_min.len() + high_fence_vec.len(),
                &right_layout,
            )?;
        }

        {
            let mut left_page = tx.page_mut(page_id)?;
            {
                let payload = page::payload_mut(left_page.data_mut())?;
                page::set_right_sibling(payload, Some(new_page_id));
            }
            self.apply_leaf_fences(
                &mut left_page,
                left_min.as_slice(),
                Some(right_min.as_slice()),
            )?;
        }
        {
            let mut right_page = tx.page_mut(new_page_id)?;
            {
                let payload = page::payload_mut(right_page.data_mut())?;
                page::set_left_sibling(payload, Some(page_id));
                // Preserve existing right sibling from original header.
                page::set_right_sibling(payload, header.right_sibling);
                page::set_parent(payload, header.parent);
            }
            let high_opt = if high_fence_vec.is_empty() {
                None
            } else {
                Some(high_fence_vec.as_slice())
            };
            self.apply_leaf_fences(&mut right_page, right_min.as_slice(), high_opt)?;
        }
        if let Some(rsib) = header.right_sibling {
            let mut sibling = tx.page_mut(rsib)?;
            let payload = page::payload_mut(sibling.data_mut())?;
            page::set_left_sibling(payload, Some(new_page_id));
        }

        self.stats.inc_leaf_splits();
        tracing::trace!(
            target: "sombra_btree::split",
            left = page_id.0,
            right = new_page_id.0,
            "split leaf page"
        );

        Ok(LeafInsert::Split {
            left_min,
            right_min,
            right_page: new_page_id,
        })
    }

    fn try_insert_leaf_in_place(
        &self,
        page: &mut PageMut<'_>,
        header: &page::Header,
        key: &[u8],
        value: &[u8],
    ) -> Result<bool> {
        if header.kind != page::BTreePageKind::Leaf {
            return Ok(false);
        }
        let free_start = header.free_start as usize;
        let free_end = header.free_end as usize;
        if free_end < free_start {
            return Ok(false);
        }
        let record_len = page::plain_leaf_record_encoded_len(key.len(), value.len())?;
        let total_needed = record_len
            .checked_add(page::SLOT_ENTRY_LEN)
            .ok_or_else(|| SombraError::Invalid("leaf insert size overflow"))?;
        if free_end - free_start < total_needed {
            return Ok(false);
        }

        let (slot_entries, insert_idx, has_existing) = {
            let data = page.data();
            let slot_view = SlotView::new(&header, data)?;
            let mut entries = Vec::with_capacity(slot_view.len());
            for idx in 0..slot_view.len() {
                let (start, len) = slot_view.slots().extent(idx)?;
                entries.push((start as usize, len as usize));
            }
            let mut lo = 0usize;
            let mut hi = slot_view.len();
            let mut existing = None;
            while lo < hi {
                let mid = (lo + hi) / 2;
                let rec_slice = slot_view.slice(mid)?;
                record_btree_leaf_key_decodes(1);
                let record = page::decode_leaf_record(rec_slice)?;
                record_btree_leaf_key_cmps(1);
                match K::compare_encoded(record.key, key) {
                    Ordering::Less => lo = mid + 1,
                    Ordering::Greater => hi = mid,
                    Ordering::Equal => {
                        existing = Some(mid);
                        break;
                    }
                }
            }
            (entries, existing.unwrap_or(lo), existing.is_some())
        };

        if has_existing {
            return Ok(false);
        }

        if insert_idx == 0 {
            if header.low_fence_len == 0 || header.low_fence_len != key.len() {
                return Ok(false);
            }
        }

        let insert_offset = if insert_idx == slot_entries.len() {
            free_start
        } else {
            slot_entries[insert_idx].0
        };

        let mut record = Vec::with_capacity(record_len);
        page::encode_leaf_record(key, value, &mut record)?;
        debug_assert_eq!(record.len(), record_len);

        {
            let payload = page::payload_mut(page.data_mut())?;
            let moved = free_start - insert_offset;
            payload.copy_within(insert_offset..free_start, insert_offset + record_len);
            record_btree_leaf_memcopy_bytes(moved as u64);
            payload[insert_offset..insert_offset + record_len].copy_from_slice(&record);
            record_btree_leaf_memcopy_bytes(record_len as u64);

            let new_free_start = free_start + record_len;
            let new_free_end = free_end - page::SLOT_ENTRY_LEN;
            let new_free_start_u16 = u16::try_from(new_free_start)
                .map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
            let new_free_end_u16 = u16::try_from(new_free_end)
                .map_err(|_| SombraError::Invalid("leaf free_end overflow"))?;
            page::set_free_start(payload, new_free_start_u16);
            page::set_free_end(payload, new_free_end_u16);

            let new_slot_count = slot_entries.len() + 1;
            let new_slot_count_u16 = u16::try_from(new_slot_count)
                .map_err(|_| SombraError::Invalid("leaf slot count overflow"))?;
            page::set_slot_count(payload, new_slot_count_u16);

            let record_len_u16 = u16::try_from(record_len)
                .map_err(|_| SombraError::Invalid("leaf slot length overflow"))?;
            let mut new_entries = Vec::with_capacity(new_slot_count);
            for i in 0..new_slot_count {
                if i == insert_idx {
                    let offset_u16 = u16::try_from(insert_offset)
                        .map_err(|_| SombraError::Invalid("leaf slot offset overflow"))?;
                    new_entries.push((offset_u16, record_len_u16));
                } else {
                    let old_idx = if i < insert_idx { i } else { i - 1 };
                    let (old_offset, old_len) = slot_entries[old_idx];
                    let mut offset = old_offset;
                    if old_idx >= insert_idx {
                        offset = offset
                            .checked_add(record_len)
                            .ok_or_else(|| SombraError::Invalid("leaf slot offset overflow"))?;
                    }
                    let offset_u16 = u16::try_from(offset)
                        .map_err(|_| SombraError::Invalid("leaf slot offset overflow"))?;
                    let len_u16 = u16::try_from(old_len)
                        .map_err(|_| SombraError::Invalid("leaf slot length overflow"))?;
                    new_entries.push((offset_u16, len_u16));
                }
            }
            let payload_len = payload.len();
            let new_slot_bytes = new_entries.len() * page::SLOT_ENTRY_LEN;
            let slot_dir_start = payload_len
                .checked_sub(new_slot_bytes)
                .ok_or_else(|| SombraError::Invalid("slot directory exceeds payload"))?;
            for (idx, (offset, len)) in new_entries.iter().enumerate() {
                let pos = slot_dir_start + idx * page::SLOT_ENTRY_LEN;
                page::write_slot_entry(payload, pos, *offset, *len);
            }

            if insert_idx == 0 {
                page::set_low_fence(payload, key)?;
            }
        }

        Ok(true)
    }

    fn try_delete_leaf_in_place(
        &self,
        page: &mut PageMut<'_>,
        header: &page::Header,
        key: &[u8],
        removed_first_key: bool,
        new_first_key: Option<&[u8]>,
    ) -> Result<Option<InPlaceDeleteResult>> {
        if header.kind != page::BTreePageKind::Leaf {
            return Ok(None);
        }
        if header.slot_count == 0 {
            return Ok(None);
        }
        if removed_first_key {
            let Some(new_key) = new_first_key else {
                return Ok(None);
            };
            if header.low_fence_len != new_key.len() {
                return Ok(None);
            }
        }
        let (slot_entries, target_idx, payload_len) = {
            let data = page.data();
            let slot_view = SlotView::new(header, data)?;
            let mut entries = Vec::with_capacity(slot_view.len());
            for idx in 0..slot_view.len() {
                let (start, len) = slot_view.slots().extent(idx)?;
                entries.push((start as usize, len as usize));
            }
            let mut lo = 0usize;
            let mut hi = slot_view.len();
            let mut found = None;
            while lo < hi {
                let mid = (lo + hi) / 2;
                let rec_slice = slot_view.slice(mid)?;
                let record = page::decode_leaf_record(rec_slice)?;
                match K::compare_encoded(record.key, key) {
                    Ordering::Less => lo = mid + 1,
                    Ordering::Greater => hi = mid,
                    Ordering::Equal => {
                        found = Some(mid);
                        break;
                    }
                }
            }
            let Some(idx) = found else {
                return Ok(None);
            };
            (entries, idx, slot_view.payload().len())
        };
        if slot_entries.len() <= 1 {
            return Ok(None);
        }
        let (record_start, record_len) = slot_entries[target_idx];
        let record_end = record_start
            .checked_add(record_len)
            .ok_or_else(|| SombraError::Corruption("leaf record extent overflow"))?;
        if record_end < record_start {
            return Err(SombraError::Corruption("leaf record extent inverted"));
        }
        if record_len == 0 {
            return Ok(None);
        }
        let payload = page::payload_mut(page.data_mut())?;
        let free_start = header.free_start as usize;
        let free_end = header.free_end as usize;
        if record_end > free_start || free_start > payload.len() || free_end > payload.len() {
            return Err(SombraError::Corruption("leaf free space pointers invalid"));
        }
        let bytes_to_move = free_start - record_end;
        if bytes_to_move > 0 {
            payload.copy_within(record_end..free_start, record_start);
            record_btree_leaf_memcopy_bytes(bytes_to_move as u64);
        }
        if record_len > 0 {
            let clear_start = free_start - record_len;
            payload[clear_start..free_start].fill(0);
        }
        let new_free_start = free_start
            .checked_sub(record_len)
            .ok_or_else(|| SombraError::Corruption("leaf free_start underflow"))?;
        let new_free_end = free_end
            .checked_add(page::SLOT_ENTRY_LEN)
            .ok_or_else(|| SombraError::Corruption("leaf free_end overflow"))?;
        let new_free_start_u16 = u16::try_from(new_free_start)
            .map_err(|_| SombraError::Invalid("leaf free_start overflow"))?;
        let new_free_end_u16 = u16::try_from(new_free_end)
            .map_err(|_| SombraError::Invalid("leaf free_end overflow"))?;
        page::set_free_start(payload, new_free_start_u16);
        page::set_free_end(payload, new_free_end_u16);

        let mut new_entries = Vec::with_capacity(slot_entries.len() - 1);
        for (idx, (offset, len)) in slot_entries.iter().enumerate() {
            if idx == target_idx {
                continue;
            }
            let mut adjusted = *offset;
            if idx > target_idx {
                adjusted = adjusted
                    .checked_sub(record_len)
                    .ok_or_else(|| SombraError::Corruption("slot offset underflow"))?;
            }
            let offset_u16 = u16::try_from(adjusted)
                .map_err(|_| SombraError::Invalid("leaf slot offset overflow"))?;
            let len_u16 = u16::try_from(*len)
                .map_err(|_| SombraError::Invalid("leaf slot length overflow"))?;
            new_entries.push((offset_u16, len_u16));
        }
        let new_slot_count_u16 = u16::try_from(new_entries.len())
            .map_err(|_| SombraError::Invalid("leaf slot count overflow"))?;
        page::set_slot_count(payload, new_slot_count_u16);
        let new_slot_bytes = new_entries.len() * page::SLOT_ENTRY_LEN;
        let new_slot_start = payload_len
            .checked_sub(new_slot_bytes)
            .ok_or_else(|| SombraError::Invalid("slot directory exceeds payload"))?;
        let old_slot_start = payload_len
            .checked_sub(slot_entries.len() * page::SLOT_ENTRY_LEN)
            .ok_or_else(|| SombraError::Invalid("slot directory exceeds payload"))?;
        if new_slot_start > old_slot_start {
            payload[old_slot_start..new_slot_start].fill(0);
        }
        for (idx, (offset, len)) in new_entries.iter().enumerate() {
            let pos = new_slot_start + idx * page::SLOT_ENTRY_LEN;
            page::write_slot_entry(payload, pos, *offset, *len);
        }
        if removed_first_key {
            if let Some(first) = new_first_key {
                page::set_low_fence(payload, first)?;
            }
        }
        Ok(Some(InPlaceDeleteResult {
            free_start: new_free_start_u16,
            free_end: new_free_end_u16,
        }))
    }
}
