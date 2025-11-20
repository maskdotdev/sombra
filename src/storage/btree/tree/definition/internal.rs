impl<K: KeyCodec, V: ValCodec> BTree<K, V> {
    fn propagate_split(
        &self,
        tx: &mut WriteGuard<'_>,
        path: Vec<PathEntry>,
        mut left_page: PageId,
        mut left_min: Vec<u8>,
        mut right_min: Vec<u8>,
        mut right_page: PageId,
    ) -> Result<()> {
        let mut path = path;
        while let Some(frame) = path.pop() {
            let parent_id = frame.page_id;
            self.set_parent_pointer(tx, right_page, Some(parent_id))?;
            match self.insert_into_internal(tx, parent_id, right_min.clone(), right_page)? {
                InternalInsert::Done => return Ok(()),
                InternalInsert::Split {
                    left_min: new_left_min,
                    right_min: new_right_min,
                    right_page: new_right_page,
                } => {
                    left_page = parent_id;
                    left_min = new_left_min;
                    right_min = new_right_min;
                    right_page = new_right_page;
                    continue;
                }
            }
        }

        self.create_new_root(tx, left_page, right_page, left_min, right_min)
    }

    fn set_parent_pointer(
        &self,
        tx: &mut WriteGuard<'_>,
        page_id: PageId,
        parent: Option<PageId>,
    ) -> Result<()> {
        let mut page = tx.page_mut(page_id)?;
        let payload = page::payload_mut(page.data_mut())?;
        page::set_parent(payload, parent);
        Ok(())
    }

    fn insert_into_internal(
        &self,
        tx: &mut WriteGuard<'_>,
        page_id: PageId,
        separator: Vec<u8>,
        right_child: PageId,
    ) -> Result<InternalInsert> {
        let mut page = tx.page_mut(page_id)?;
        let header = page::Header::parse(page.data())?;
        let data = page.data();
        let slot_view = SlotView::new(&header, data)?;
        let payload = slot_view.payload();
        let (low_fence_bytes, high_fence_bytes) = header.fence_slices(data)?;
        let old_low_fence = low_fence_bytes.to_vec();
        let old_high_fence = high_fence_bytes.to_vec();
        let mut entries = Vec::with_capacity(slot_view.len() + 1);
        for idx in 0..slot_view.len() {
            let rec_slice = slot_view.slice(idx)?;
            let record = page::decode_internal_record(rec_slice)?;
            entries.push((record.separator.to_vec(), record.child));
        }

        match entries
            .binary_search_by(|(existing, _)| K::compare_encoded(existing, separator.as_slice()))
        {
            Ok(idx) => entries[idx] = (separator.clone(), right_child),
            Err(idx) => entries.insert(idx, (separator.clone(), right_child)),
        }

        let payload_len = payload.len();
        let high_slice_existing = old_high_fence.as_slice();
        let new_low_slice = entries[0].0.as_slice();
        let fences_end_current =
            page::PAYLOAD_HEADER_LEN + new_low_slice.len() + high_slice_existing.len();
        if let Some(layout) =
            self.build_internal_layout(payload_len, new_low_slice, high_slice_existing, &entries)?
        {
            self.apply_internal_layout(&mut page, &header, fences_end_current, &layout)?;
            let new_low = entries[0].0.clone();
            let high_opt = if old_high_fence.is_empty() {
                None
            } else {
                Some(old_high_fence.as_slice())
            };
            self.apply_internal_fences(&mut page, new_low.as_slice(), high_opt)?;
            drop(page);
            self.set_parent_pointer(tx, right_child, Some(page_id))?;
            return Ok(InternalInsert::Done);
        }

        let len = entries.len();
        if len < 2 {
            return Err(SombraError::Invalid(
                "cannot split internal node with fewer than 2 entries",
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
            let left_try = self.build_internal_layout(
                payload_len,
                old_low_fence.as_slice(),
                left_high,
                left_slice,
            )?;
            let right_try = self.build_internal_layout(
                payload_len,
                right_low,
                old_high_fence.as_slice(),
                right_slice,
            )?;
            if let (Some(l), Some(r)) = (left_try, right_try) {
                left_layout = Some(l);
                right_layout = Some(r);
                split_at = Some(idx);
                break;
            }
        }
        let split_at = split_at.ok_or({
            SombraError::Invalid("unable to split internal node into fitting halves")
        })?;
        let left_layout = left_layout.expect("left layout");
        let right_layout = right_layout.expect("right layout");
        let left_min = entries[0].0.clone();
        let right_min = entries[split_at].0.clone();
        let header_parent = header.parent;
        let right_sibling = header.right_sibling;

        let left_fences_end = page::PAYLOAD_HEADER_LEN + old_low_fence.len() + right_min.len();
        self.apply_internal_layout(&mut page, &header, left_fences_end, &left_layout)?;
        self.apply_internal_fences(
            &mut page,
            old_low_fence.as_slice(),
            Some(right_min.as_slice()),
        )?;
        drop(page);

        let new_page_id = tx.allocate_page()?;
        {
            let mut right_page = tx.page_mut(new_page_id)?;
            self.init_internal_page(new_page_id, &mut right_page)?;
            let right_header = page::Header::parse(right_page.data())?;
            self.apply_internal_layout(
                &mut right_page,
                &right_header,
                page::PAYLOAD_HEADER_LEN + right_min.len() + old_high_fence.len(),
                &right_layout,
            )?;
            {
                let payload = page::payload_mut(right_page.data_mut())?;
                page::set_parent(payload, header_parent);
            }
            let high_opt = if old_high_fence.is_empty() {
                None
            } else {
                Some(old_high_fence.as_slice())
            };
            self.apply_internal_fences(&mut right_page, right_min.as_slice(), high_opt)?;
        }

        {
            let mut left_page = tx.page_mut(page_id)?;
            let payload = page::payload_mut(left_page.data_mut())?;
            page::set_right_sibling(payload, Some(new_page_id));
        }
        {
            let mut right_page = tx.page_mut(new_page_id)?;
            let payload = page::payload_mut(right_page.data_mut())?;
            page::set_left_sibling(payload, Some(page_id));
            page::set_right_sibling(payload, right_sibling);
        }
        if let Some(rsib) = right_sibling {
            let mut sibling = tx.page_mut(rsib)?;
            let payload = page::payload_mut(sibling.data_mut())?;
            page::set_left_sibling(payload, Some(new_page_id));
        }

        self.stats.inc_internal_splits();
        tracing::trace!(
            target: "sombra_btree::split",
            left = page_id.0,
            right = new_page_id.0,
            "split internal page"
        );

        let right_slice = entries[split_at..].to_vec();
        for (_, child) in right_slice.iter() {
            self.set_parent_pointer(tx, *child, Some(new_page_id))?;
        }
        let left_slice = entries[..split_at].to_vec();
        for (_, child) in left_slice.iter() {
            self.set_parent_pointer(tx, *child, Some(page_id))?;
        }

        Ok(InternalInsert::Split {
            left_min,
            right_min,
            right_page: new_page_id,
        })
    }
}
