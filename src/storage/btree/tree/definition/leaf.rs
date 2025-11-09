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
            match self.try_insert_leaf_in_place(
                &mut page,
                &header,
                key.as_slice(),
                value.as_slice(),
            )? {
                InPlaceInsertResult::Applied { new_first_key } => {
                    self.stats.inc_leaf_in_place_edits();
                    return Ok(LeafInsert::Done { new_first_key });
                }
                InPlaceInsertResult::NotApplied => {}
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
            let first_key_changed = K::compare_encoded(
                new_low.as_slice(),
                low_fence_vec.as_slice(),
            ) != Ordering::Equal;
            let new_first_key = first_key_changed.then_some(new_low);
            return Ok(LeafInsert::Done { new_first_key });
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
    ) -> Result<InPlaceInsertResult> {
        if header.kind != page::BTreePageKind::Leaf {
            return Ok(InPlaceInsertResult::NotApplied);
        }
        let low_fence_bytes = {
            let fences = page.data();
            header.fence_slices(fences)?.0.to_vec()
        };

        let mut allocator = LeafAllocator::new(page.data_mut(), header.clone())?;
        let mut lo = 0usize;
        let mut hi = allocator.slot_count();
        let mut existing = None;
        while lo < hi {
            let mid = (lo + hi) / 2;
            record_btree_leaf_key_decodes(1);
            let record = allocator.leaf_record(mid)?;
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
        let insert_idx = existing.unwrap_or(lo);
        let requires_fence_update = existing.is_none()
            && insert_idx == 0
            && (header.low_fence_len != key.len()
                || K::compare_encoded(low_fence_bytes.as_slice(), key) != Ordering::Equal);

        let mut record = Vec::new();
        page::encode_leaf_record(key, value, &mut record)?;

        let result = if let Some(idx) = existing {
            allocator.replace_slot(idx, &record)
        } else {
            allocator.insert_slot(insert_idx, &record)
        };
        match result {
            Ok(()) => {
                let mut new_first_key = None;
                if requires_fence_update {
                    allocator.update_low_fence(key)?;
                    new_first_key = Some(key.to_vec());
                }
                Ok(InPlaceInsertResult::Applied { new_first_key })
            }
            Err(err) if allocator_capacity_error(&err) => {
                Ok(InPlaceInsertResult::NotApplied)
            }
            Err(err) => Err(err),
        }
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
        if removed_first_key && new_first_key.is_none() {
            return Ok(None);
        }
        if header.slot_count <= 1 {
            return Ok(None);
        }
        let mut allocator = LeafAllocator::new(page.data_mut(), header.clone())?;
        if allocator.slot_count() <= 1 {
            return Ok(None);
        }
        let mut lo = 0usize;
        let mut hi = allocator.slot_count();
        let mut found = None;
        while lo < hi {
            let mid = (lo + hi) / 2;
            record_btree_leaf_key_decodes(1);
            let record = allocator.leaf_record(mid)?;
            match K::compare_encoded(record.key, key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => {
                    found = Some(mid);
                    break;
                }
            }
        }
        let Some(target_idx) = found else {
            return Ok(None);
        };

        allocator.delete_slot(target_idx)?;
        if removed_first_key {
            if let Some(first) = new_first_key {
                allocator.update_low_fence(first)?;
            } else {
                return Ok(None);
            }
        }
        let updated = allocator.header();
        Ok(Some(InPlaceDeleteResult {
            free_start: updated.free_start,
            free_end: updated.free_end,
        }))
    }
}

fn allocator_capacity_error(err: &SombraError) -> bool {
    matches!(
        err,
        SombraError::Invalid("slot directory exceeds payload")
            | SombraError::Invalid("leaf payload exhausted")
            | SombraError::Invalid("leaf page full")
    )
}
