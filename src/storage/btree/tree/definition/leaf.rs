impl<K: KeyCodec, V: ValCodec> BTree<K, V> {
    fn slice_fits(
        &self,
        payload_len: usize,
        low_fence: &[u8],
        high_fence: &[u8],
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<bool> {
        let mut encoded_lengths = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let len = page::plain_leaf_record_encoded_len(key.len(), value.len())?;
            encoded_lengths.push(len);
        }
        self.slice_fits_encoded(
            payload_len,
            low_fence.len(),
            high_fence.len(),
            &encoded_lengths,
        )
    }

    fn slice_fits_encoded(
        &self,
        payload_len: usize,
        low_len: usize,
        high_len: usize,
        record_lengths: &[usize],
    ) -> Result<bool> {
        let fences_end = page::PAYLOAD_HEADER_LEN + low_len + high_len;
        if fences_end > payload_len {
            return Ok(false);
        }
        let slot_bytes = record_lengths
            .len()
            .checked_mul(page::SLOT_ENTRY_LEN)
            .ok_or(SombraError::Invalid("slot directory overflow"))?;
        if slot_bytes > payload_len {
            return Ok(false);
        }
        let new_free_end = payload_len
            .checked_sub(slot_bytes)
            .ok_or(SombraError::Invalid("slot directory exceeds payload"))?;
        if new_free_end < fences_end {
            return Ok(false);
        }
        let mut total_records = 0usize;
        for len in record_lengths {
            total_records = total_records
                .checked_add(*len)
                .ok_or(SombraError::Invalid("leaf records overflow payload"))?;
            if fences_end + total_records > new_free_end {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn split_with_allocator(
        &self,
        tx: &mut WriteGuard<'_>,
        mut page: PageMut<'_>,
        header: page::Header,
        snapshot: LeafAllocatorSnapshot,
        pending_insert: LeafPendingInsert,
        payload_len: usize,
        low_fence: Vec<u8>,
        high_fence: Vec<u8>,
    ) -> Result<LeafInsert> {
        record_btree_leaf_split();
        let payload = page::payload(page.data())?;
        let split_idx = self.choose_allocator_split_index(
            payload_len,
            low_fence.as_slice(),
            high_fence.as_slice(),
            &snapshot,
            &pending_insert,
            payload,
        )?;
        let page_id = page.id;
        let mut left_allocator =
            LeafAllocator::from_snapshot(page.data_mut(), header.clone(), snapshot)?;
        let new_page_id = tx.allocate_page()?;
        let mut right_page = tx.page_mut(new_page_id)?;
        self.init_leaf_page(new_page_id, &mut right_page)?;
        let right_header = page::Header::parse(right_page.data())?;
        let mut right_allocator = LeafAllocator::new(right_page.data_mut(), right_header.clone())?;
        let outcome = left_allocator.split_into(
            &mut right_allocator,
            low_fence.as_slice(),
            high_fence.as_slice(),
            split_idx,
            &pending_insert,
        )?;
        let left_snapshot = left_allocator.into_snapshot();
        self.leaf_allocator_cache(tx).insert(page_id, left_snapshot);
        let right_snapshot = right_allocator.into_snapshot();
        self.leaf_allocator_cache(tx)
            .insert(new_page_id, right_snapshot);
        drop(right_page);
        drop(page);
        self.finalize_leaf_split(
            tx,
            page_id,
            new_page_id,
            &header,
            outcome.left_min.as_slice(),
            outcome.right_min.as_slice(),
            high_fence.as_slice(),
        )?;
        self.stats.inc_leaf_splits();
        tracing::trace!(
            target: "sombra_btree::split",
            left = page_id.0,
            right = new_page_id.0,
            "split leaf page"
        );
        Ok(LeafInsert::Split {
            left_min: outcome.left_min,
            right_min: outcome.right_min,
            right_page: new_page_id,
        })
    }

    fn choose_allocator_split_index(
        &self,
        payload_len: usize,
        low_fence: &[u8],
        high_fence: &[u8],
        snapshot: &LeafAllocatorSnapshot,
        pending_insert: &LeafPendingInsert,
        payload: &[u8],
    ) -> Result<usize> {
        let slots = build_record_slots(snapshot.record_count(), pending_insert)?;
        if slots.len() < 2 {
            return Err(SombraError::Invalid(
                "cannot split leaf with fewer than 2 entries",
            ));
        }
        let mut lengths = Vec::with_capacity(slots.len());
        for slot in &slots {
            let len = match slot {
                RecordSlot::Existing(idx) => snapshot.encoded_len(*idx)?,
                RecordSlot::Pending => pending_insert.record.len(),
            };
            lengths.push(len);
        }
        let pending_record = page::decode_leaf_record(pending_insert.record.as_slice())?;
        let mut candidates: Vec<usize> = (1..lengths.len()).collect();
        let mid = lengths.len() / 2;
        candidates.sort_by_key(|idx| idx.abs_diff(mid));
        let left_low_len = if pending_insert.requires_low_fence_update {
            pending_record.key.len()
        } else {
            low_fence.len()
        };
        for idx in candidates {
            let left_slice = &lengths[..idx];
            let right_slice = &lengths[idx..];
            if left_slice.is_empty() || right_slice.is_empty() {
                continue;
            }
            let right_slot = &slots[idx];
            let right_key_len = record_slot_key_len(snapshot, right_slot, payload, pending_record)?;
            let left_fits =
                self.slice_fits_encoded(payload_len, left_low_len, right_key_len, left_slice)?;
            let right_fits =
                self.slice_fits_encoded(payload_len, right_key_len, high_fence.len(), right_slice)?;
            if left_fits && right_fits {
                return Ok(idx);
            }
        }
        Err(SombraError::Invalid(
            "unable to split leaf into fitting halves",
        ))
    }

    fn finalize_leaf_split(
        &self,
        tx: &mut WriteGuard<'_>,
        left_page: PageId,
        right_page: PageId,
        header: &page::Header,
        left_min: &[u8],
        right_min: &[u8],
        high_fence: &[u8],
    ) -> Result<()> {
        {
            let mut page = tx.page_mut(left_page)?;
            {
                let payload = page::payload_mut(page.data_mut())?;
                page::set_right_sibling(payload, Some(right_page));
            }
            self.apply_leaf_fences(&mut page, left_min, Some(right_min))?;
        }
        {
            let mut page = tx.page_mut(right_page)?;
            {
                let payload = page::payload_mut(page.data_mut())?;
                page::set_left_sibling(payload, Some(left_page));
                page::set_right_sibling(payload, header.right_sibling);
                page::set_parent(payload, header.parent);
            }
            let high_opt = if high_fence.is_empty() {
                None
            } else {
                Some(high_fence)
            };
            self.apply_leaf_fences(&mut page, right_min, high_opt)?;
        }
        if let Some(rsib) = header.right_sibling {
            let mut sibling = tx.page_mut(rsib)?;
            let payload = page::payload_mut(sibling.data_mut())?;
            page::set_left_sibling(payload, Some(right_page));
        }
        Ok(())
    }

    fn rebuild_leaf_payload(
        &self,
        tx: &mut WriteGuard<'_>,
        page: &mut PageMut<'_>,
        header: &page::Header,
        low_fence: &[u8],
        high_fence: &[u8],
        entries: &[(Vec<u8>, Vec<u8>)],
    ) -> Result<()> {
        let cache = self.leaf_allocator_cache(tx);
        let snapshot = cache.take(page.id);
        let page_id = page.id;
        let mut allocator = if let Some(snapshot) = snapshot {
            LeafAllocator::from_snapshot(page.data_mut(), header.clone(), snapshot)?
        } else {
            LeafAllocator::new(page.data_mut(), header.clone())?
        };
        allocator.rebuild_from_entries(low_fence, high_fence, entries)?;
        cache.insert(page_id, allocator.into_snapshot());
        Ok(())
    }

    fn leaf_allocator_cache<'a>(&self, tx: &'a mut WriteGuard<'_>) -> &'a mut LeafAllocatorCache {
        if tx.extension_mut::<LeafAllocatorCache>().is_none() {
            tx.store_extension(LeafAllocatorCache::default());
        }
        tx.extension_mut::<LeafAllocatorCache>()
            .expect("allocator cache extension")
    }

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
        let mut fallback_snapshot = None;
        let mut pending_insert = None;
        match self.try_insert_leaf_in_place(
            tx,
            &mut page,
            &header,
            key.as_slice(),
            value.as_slice(),
        )? {
            InPlaceInsertResult::Applied { new_first_key } => {
                self.stats.inc_leaf_in_place_edits();
                return Ok(LeafInsert::Done { new_first_key });
            }
            InPlaceInsertResult::NotApplied {
                snapshot,
                pending_insert: pending,
            } => {
                if let Some(snapshot) = snapshot {
                    fallback_snapshot = Some(snapshot);
                }
                if let Some(pending) = pending {
                    pending_insert = Some(pending);
                }
            }
        }

        let data = page.data();
        let payload_len = page::payload(data)?.len();
        let (low_fence, high_fence) = header.fence_slices(data)?;
        let low_fence_vec = low_fence.to_vec();
        let high_fence_vec = high_fence.to_vec();

        if let Some(pending) = pending_insert.take() {
            if let Some(snapshot) = fallback_snapshot.take() {
                return self.split_with_allocator(
                    tx,
                    page,
                    header,
                    snapshot,
                    pending,
                    payload_len,
                    low_fence_vec,
                    high_fence_vec,
                );
            }
        }

        let mut entries = if let Some(snapshot) = fallback_snapshot.take() {
            snapshot.decode_entries(data)?
        } else {
            let slot_view = SlotView::new(&header, data)?;
            let mut rows = Vec::with_capacity(slot_view.len());
            for idx in 0..slot_view.len() {
                let rec_slice = slot_view.slice(idx)?;
                record_btree_leaf_key_decodes(1);
                let record = page::decode_leaf_record(rec_slice)?;
                record_btree_leaf_memcopy_bytes(record.key.len() as u64);
                rows.push((record.key.to_vec(), record.value.to_vec()));
            }
            rows
        };

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

        let high_slice_existing = high_fence_vec.as_slice();
        let new_low_slice = entries[0].0.as_slice();
        if self.slice_fits(payload_len, new_low_slice, high_slice_existing, &entries)? {
            self.rebuild_leaf_payload(
                tx,
                &mut page,
                &header,
                new_low_slice,
                high_slice_existing,
                &entries,
            )?;
            let new_low = entries[0].0.clone();
            self.stats.inc_leaf_rebuilds();
            let first_key_changed =
                K::compare_encoded(new_low.as_slice(), low_fence_vec.as_slice()) != Ordering::Equal;
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
        let mut split_at = None;
        for idx in candidates {
            let left_slice = &entries[..idx];
            let right_slice = &entries[idx..];
            if left_slice.is_empty() || right_slice.is_empty() {
                continue;
            }
            let left_high = right_slice.first().map(|(k, _)| k.as_slice()).unwrap();
            let right_low = right_slice.first().map(|(k, _)| k.as_slice()).unwrap();
            let left_fits =
                self.slice_fits(payload_len, low_fence_vec.as_slice(), left_high, left_slice)?;
            let right_fits = self.slice_fits(
                payload_len,
                right_low,
                high_fence_vec.as_slice(),
                right_slice,
            )?;
            if left_fits && right_fits {
                split_at = Some(idx);
                break;
            }
        }
        let split_at = split_at
            .ok_or(SombraError::Invalid("unable to split leaf into fitting halves"))?;
        let left_min = entries[0].0.clone();
        let right_min = entries[split_at].0.clone();

        {
            let snapshot = self.leaf_allocator_cache(tx).take(page.id);
            let mut allocator = if let Some(snapshot) = snapshot {
                LeafAllocator::from_snapshot(page.data_mut(), header.clone(), snapshot)?
            } else {
                LeafAllocator::new(page.data_mut(), header.clone())?
            };
            allocator.rebuild_from_entries(
                low_fence_vec.as_slice(),
                right_min.as_slice(),
                &entries[..split_at],
            )?;
            let snapshot = allocator.into_snapshot();
            self.leaf_allocator_cache(tx).insert(page.id, snapshot);
        }

        let page_id = page.id;
        drop(page);

        let new_page_id = tx.allocate_page()?;
        {
            let mut right_page = tx.page_mut(new_page_id)?;
            self.init_leaf_page(new_page_id, &mut right_page)?;
            let right_header = page::Header::parse(right_page.data())?;
            let mut allocator = LeafAllocator::new(right_page.data_mut(), right_header.clone())?;
            allocator.rebuild_from_entries(
                right_min.as_slice(),
                high_fence_vec.as_slice(),
                &entries[split_at..],
            )?;
            let snapshot = allocator.into_snapshot();
            self.leaf_allocator_cache(tx).insert(new_page_id, snapshot);
        }

        self.finalize_leaf_split(
            tx,
            page_id,
            new_page_id,
            &header,
            left_min.as_slice(),
            right_min.as_slice(),
            high_fence_vec.as_slice(),
        )?;
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
        tx: &mut WriteGuard<'_>,
        page: &mut PageMut<'_>,
        header: &page::Header,
        key: &[u8],
        value: &[u8],
    ) -> Result<InPlaceInsertResult> {
        if header.kind != page::BTreePageKind::Leaf {
            return Ok(InPlaceInsertResult::NotApplied {
                snapshot: None,
                pending_insert: None,
            });
        }
        let (low_fence_slice, _) = header.fence_slices(page.data())?;
        let low_fence: Vec<u8> = low_fence_slice.to_vec();

        // Profile allocator cache access
        let cache_start = profile_timer();
        let snapshot = self.leaf_allocator_cache(tx).take(page.id);
        let mut allocator = if let Some(snapshot) = snapshot {
            LeafAllocator::from_snapshot(page.data_mut(), header.clone(), snapshot)?
        } else {
            LeafAllocator::new(page.data_mut(), header.clone())?
        };
        if let Some(start) = cache_start {
            record_btree_leaf_allocator_cache(start.elapsed().as_nanos() as u64);
        }

        // Profile binary search
        let search_start = profile_timer();
        let mut lo = 0usize;
        let mut hi = allocator.slot_count();
        let mut insert_idx = 0usize;
        let mut replaces_existing = false;
        while lo < hi {
            let mid = (lo + hi) / 2;
            record_btree_leaf_key_decodes(1);
            let record = allocator.leaf_record(mid)?;
            record_btree_leaf_key_cmps(1);
            match K::compare_encoded(record.key, key) {
                Ordering::Less => lo = mid + 1,
                Ordering::Greater => hi = mid,
                Ordering::Equal => {
                    insert_idx = mid;
                    replaces_existing = true;
                    break;
                }
            }
        }
        if !replaces_existing {
            insert_idx = lo;
        }
        if let Some(start) = search_start {
            record_btree_leaf_binary_search(start.elapsed().as_nanos() as u64);
        }

        // Profile record encoding
        let encode_start = profile_timer();
        let record_len = page::plain_leaf_record_encoded_len(key.len(), value.len())?;
        let mut record = Vec::with_capacity(record_len);
        page::encode_leaf_record(key, value, &mut record)?;
        if let Some(start) = encode_start {
            record_btree_leaf_record_encode(start.elapsed().as_nanos() as u64);
        }
        debug_assert!(
            record.first().copied().unwrap_or(0) != 0,
            "leaf record encoded with zero key length byte (key_len={}, value_len={}, record_len={})",
            key.len(),
            value.len(),
            record_len
        );

        let requires_low_fence_update = insert_idx == 0
            && !replaces_existing
            && (low_fence.is_empty()
                || K::compare_encoded(key, low_fence.as_slice()) == Ordering::Less);

        if replaces_existing {
            let snapshot = allocator.into_snapshot();
            return Ok(InPlaceInsertResult::NotApplied {
                snapshot: Some(snapshot),
                pending_insert: None,
            });
        }

        // Profile slot allocation
        let slot_start = profile_timer();
        let insert_result = allocator.insert_slot(insert_idx, &record);
        if let Some(start) = slot_start {
            record_btree_leaf_slot_alloc(start.elapsed().as_nanos() as u64);
        }

        match insert_result {
            Ok(()) => {
                if requires_low_fence_update {
                    if let Err(err) = allocator.update_low_fence(key) {
                        if allocator_capacity_error(&err) {
                            allocator.delete_slot(insert_idx)?;
                            let snapshot = allocator.into_snapshot();
                            return Ok(InPlaceInsertResult::NotApplied {
                                snapshot: Some(snapshot),
                                pending_insert: Some(LeafPendingInsert {
                                    insert_idx,
                                    replaces_existing,
                                    requires_low_fence_update,
                                    record,
                                }),
                            });
                        } else {
                            return Err(err);
                        }
                    }
                }
                let new_first_key = requires_low_fence_update.then(|| key.to_vec());
                let snapshot = allocator.into_snapshot();
                self.leaf_allocator_cache(tx).insert(page.id, snapshot);
                record_btree_leaf_in_place_success();
                Ok(InPlaceInsertResult::Applied { new_first_key })
            }
            Err(err) if allocator_capacity_error(&err) => {
                let snapshot = allocator.into_snapshot();
                Ok(InPlaceInsertResult::NotApplied {
                    snapshot: Some(snapshot),
                    pending_insert: Some(LeafPendingInsert {
                        insert_idx,
                        replaces_existing,
                        requires_low_fence_update,
                        record,
                    }),
                })
            }
            Err(err) => Err(err),
        }
    }

    fn try_delete_leaf_in_place(
        &self,
        tx: &mut WriteGuard<'_>,
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
        let snapshot = self.leaf_allocator_cache(tx).take(page.id);
        let mut allocator = if let Some(snapshot) = snapshot {
            LeafAllocator::from_snapshot(page.data_mut(), header.clone(), snapshot)?
        } else {
            LeafAllocator::new(page.data_mut(), header.clone())?
        };
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
        let free_start = updated.free_start;
        let free_end = updated.free_end;
        let snapshot = allocator.into_snapshot();
        self.leaf_allocator_cache(tx).insert(page.id, snapshot);
        Ok(Some(InPlaceDeleteResult {
            free_start,
            free_end,
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

fn record_slot_key_len(
    snapshot: &LeafAllocatorSnapshot,
    slot: &RecordSlot,
    payload: &[u8],
    pending_record: page::LeafRecordRef<'_>,
) -> Result<usize> {
    match slot {
        RecordSlot::Existing(idx) => {
            let slice = snapshot.record_slice(payload, *idx)?;
            record_btree_leaf_key_decodes(1);
            let record = page::decode_leaf_record(slice)?;
            record_btree_leaf_memcopy_bytes(record.key.len() as u64);
            Ok(record.key.len())
        }
        RecordSlot::Pending => Ok(pending_record.key.len()),
    }
}
