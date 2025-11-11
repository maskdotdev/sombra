impl<K: KeyCodec, V: ValCodec> BTree<K, V> {
    /// Open an existing tree or create a brand-new one if the root page has not been allocated.
    pub fn open_or_create(ps: &Arc<dyn PageStore>, mut opts: BTreeOptions) -> Result<Self> {
        ps.set_checksum_verification(opts.checksum_verify_on_read);
        let store = Arc::clone(ps);
        let page_size = store.page_size() as usize;
        let salt = meta_salt(&store)?;
        let stats = Arc::new(BTreeStats::default());
        let root = match opts.root_page {
            Some(root) => root,
            None => {
                let mut write = store.begin_write()?;
                let root_page = write.allocate_page()?;
                init_leaf_root(&store, &mut write, root_page, page_size, salt)?;
                store.commit(write)?;
                opts.root_page = Some(root_page);
                root_page
            }
        };
        Ok(Self {
            store,
            root: AtomicU64::new(root.0),
            page_size,
            salt,
            options: opts,
            stats,
            _marker: PhantomData,
        })
    }

    /// Return the root page identifier.
    pub fn root_page(&self) -> PageId {
        PageId(self.root.load(AtomicOrdering::SeqCst))
    }

    /// Access the live statistics counters for this tree.
    pub fn stats(&self) -> Arc<BTreeStats> {
        Arc::clone(&self.stats)
    }
    /// Snapshot the current statistics counters.
    pub fn stats_snapshot(&self) -> BTreeStatsSnapshot {
        self.stats.snapshot()
    }

    /// Emit the current statistics to the tracing sink.
    pub fn emit_stats(&self) {
        self.stats.emit_tracing();
    }

    /// Iterates through all key-value pairs in the tree using a write transaction,
    /// calling the visitor function for each pair.
    pub fn for_each_with_write<F>(&self, tx: &mut WriteGuard<'_>, mut visit: F) -> Result<()>
    where
        F: FnMut(K, V) -> Result<()>,
    {
        let root = self.root.load(AtomicOrdering::SeqCst);
        if root == 0 {
            return Ok(());
        }
        let mut current = self.leftmost_leaf_id_with_write(tx)?;
        loop {
            let page = tx.page_mut(current)?;
            let header = page::Header::parse(page.data())?;
            let slot_view = SlotView::new(&header, page.data())?;
            for idx in 0..slot_view.len() {
                let rec_slice = slot_view.slice(idx)?;
                record_btree_leaf_key_decodes(1);
                let record = page::decode_leaf_record(rec_slice)?;
                let value = V::decode_val(record.value)?;
                let key = K::decode_key(record.key)?;
                visit(key, value)?;
            }
            let next = header.right_sibling;
            drop(page);
            match next {
                Some(id) => current = id,
                None => break,
            }
        }
        Ok(())
    }

    /// Retrieves the value associated with the given key, if it exists, using a read transaction.
    pub fn get(&self, tx: &ReadGuard, key: &K) -> Result<Option<V>> {
        let mut encoded_key = Vec::new();
        K::encode_key(key, &mut encoded_key);
        let (leaf, header) = self.find_leaf(tx, &encoded_key)?;
        self.search_leaf(&leaf, &header, &encoded_key)
    }

    /// Retrieves the value associated with the given key, if it exists, using a write transaction.
    pub fn get_with_write(&self, tx: &mut WriteGuard<'_>, key: &K) -> Result<Option<V>> {
        let mut encoded_key = Vec::new();
        K::encode_key(key, &mut encoded_key);
        let (leaf_id, header, _) = self.find_leaf_mut(tx, &encoded_key)?;
        let page = tx.page_mut(leaf_id)?;
        let result = self.search_leaf_bytes(page.data(), &header, &encoded_key)?;
        drop(page);
        Ok(result)
    }

    /// Inserts or updates a key-value pair in the tree.
    pub fn put(&self, tx: &mut WriteGuard<'_>, key: &K, val: &V) -> Result<()> {
        let mut key_buf = Vec::new();
        K::encode_key(key, &mut key_buf);
        let mut val_buf = Vec::new();
        V::encode_val(val, &mut val_buf);
        let (leaf_id, header, path) = self.find_leaf_mut(tx, &key_buf)?;
        let leaf = tx.page_mut(leaf_id)?;
        match self.insert_into_leaf(tx, leaf, header, key_buf, val_buf)? {
            LeafInsert::Done { new_first_key } => {
                if let (Some(first_key), Some(parent_frame)) = (new_first_key.as_ref(), path.last())
                {
                    self.update_parent_separator(tx, parent_frame, first_key)?;
                }
                Ok(())
            }
            LeafInsert::Split {
                left_min,
                right_min,
                right_page,
            } => self.propagate_split(tx, path, leaf_id, left_min, right_min, right_page),
        }
    }

    /// Inserts many key-value pairs assuming the iterator is sorted by key.
    ///
    /// Keys must be provided in ascending order (duplicates allowed). In debug builds this is
    /// asserted, while release builds assume the caller upholds the contract.
    pub fn put_many<'a, I>(&self, tx: &mut WriteGuard<'_>, items: I) -> Result<()>
    where
        I: IntoIterator<Item = PutItem<'a, K, V>>,
        K: 'a,
        V: 'a,
    {
        let mut cache: Option<LeafCache> = None;
        let mut prev_key: Option<Vec<u8>> = None;
        for item in items.into_iter() {
            let mut key_buf = Vec::new();
            K::encode_key(item.key, &mut key_buf);
            if let Some(prev) = &prev_key {
                debug_assert!(
                    K::compare_encoded(prev, &key_buf) != Ordering::Greater,
                    "put_many keys must be sorted"
                );
            }
            let mut val_buf = Vec::new();
            V::encode_val(item.value, &mut val_buf);
            let (leaf_id, header, path) = match cache.take() {
                Some(cached) => match self.try_reuse_leaf(tx, cached, &key_buf)? {
                    Some(result) => result,
                    None => self.find_leaf_mut(tx, &key_buf)?,
                },
                None => self.find_leaf_mut(tx, &key_buf)?,
            };
            let leaf_page = tx.page_mut(leaf_id)?;
            let key_for_insert = key_buf.clone();
            match self.insert_into_leaf(tx, leaf_page, header, key_for_insert, val_buf)? {
                LeafInsert::Done { new_first_key } => {
                    if let (Some(first), Some(parent_frame)) =
                        (new_first_key.as_ref(), path.last())
                    {
                        self.update_parent_separator(tx, parent_frame, first)?;
                    }
                    cache = Some(LeafCache {
                        leaf_id,
                        path: path.clone(),
                    });
                }
                LeafInsert::Split {
                    left_min,
                    right_min,
                    right_page,
                } => {
                    self.propagate_split(tx, path, leaf_id, left_min, right_min, right_page)?;
                    cache = None;
                }
            }
            prev_key = Some(key_buf);
        }
        Ok(())
    }

    /// Deletes the key-value pair associated with the given key.
    /// Returns true if the key was found and deleted, false otherwise.
    pub fn delete(&self, tx: &mut WriteGuard<'_>, key: &K) -> Result<bool> {
        let mut key_buf = Vec::new();
        K::encode_key(key, &mut key_buf);
        let (leaf_id, header, path) = self.find_leaf_mut(tx, &key_buf)?;
        let leaf_page = tx.page_mut(leaf_id)?;
        let snapshot = self.snapshot_leaf(&header, leaf_page.data())?;
        let payload_len = page::payload(leaf_page.data())?.len();
        drop(leaf_page);

        let LeafSnapshot {
            mut entries,
            low_fence,
            high_fence,
        } = snapshot;

        let position = match entries
            .binary_search_by(|(existing, _)| K::compare_encoded(existing, key_buf.as_slice()))
        {
            Ok(idx) => idx,
            Err(_) => return Ok(false),
        };
        entries.remove(position);

        let path = path;
        let has_parent = header.parent.is_some();
        let mut parent_update_key: Option<Vec<u8>> = None;
        let mut first_key_changed = false;
        let mut rebalance_snapshot: Option<LeafSnapshot> = None;

        let needs_rebalance = if entries.is_empty() {
            let mut page = tx.page_mut(leaf_id)?;
            self.write_leaf_empty(&mut page, &header, &[], high_fence.as_slice())?;
            self.stats.inc_leaf_rebuilds();
            drop(page);
            true
        } else {
            let new_low = entries[0].0.clone();
            first_key_changed =
                K::compare_encoded(new_low.as_slice(), low_fence.as_slice()) != Ordering::Equal;
            let mut local_rebalance = false;
            let removed_first = position == 0;
            let mut applied_in_place = false;
            let can_update_fence =
                !removed_first || !first_key_changed || header.low_fence_len == new_low.len();
            if can_update_fence {
                let mut page = tx.page_mut(leaf_id)?;
                if let Some(result) = self.try_delete_leaf_in_place(
                    tx,
                    &mut page,
                    &header,
                    key_buf.as_slice(),
                    removed_first && first_key_changed,
                    removed_first.then_some(new_low.as_slice()),
                )? {
                    applied_in_place = true;
                    self.stats.inc_leaf_in_place_edits();
                    if first_key_changed {
                        parent_update_key = Some(new_low.clone());
                    }
                    let fill =
                        Self::fill_percent(payload_len, result.free_start, result.free_end);
                    local_rebalance = has_parent && fill < self.options.page_fill_target;
                    if local_rebalance {
                        rebalance_snapshot = Some(LeafSnapshot {
                            entries: entries.clone(),
                            low_fence: new_low.clone(),
                            high_fence: high_fence.clone(),
                        });
                    }
                }
            }

            if !applied_in_place {
                if !has_parent {
                    return Err(SombraError::Invalid(
                        "leaf layout after delete exceeds capacity",
                    ));
                }
                rebalance_snapshot = Some(LeafSnapshot {
                    entries: entries.clone(),
                    low_fence: new_low.clone(),
                    high_fence: high_fence.clone(),
                });
                local_rebalance = true;
                if first_key_changed {
                    parent_update_key = Some(new_low);
                }
            }
            local_rebalance
        };

        if !has_parent {
            if rebalance_snapshot.is_some() {
                return Err(SombraError::Invalid(
                    "leaf layout after delete exceeds capacity",
                ));
            }
            return Ok(true);
        }

        if needs_rebalance {
            self.rebalance_leaf(
                tx,
                leaf_id,
                path,
                rebalance_snapshot,
                parent_update_key.clone(),
            )?;
        } else if first_key_changed {
            if let (Some(first_key), Some(parent_frame)) = (parent_update_key.as_ref(), path.last())
            {
                self.update_parent_separator(tx, parent_frame, first_key)?;
            }
        }
        Ok(true)
    }

    /// Returns a cursor for iterating over key-value pairs within the specified range bounds.
    pub fn range<'a>(
        &'a self,
        tx: &'a ReadGuard,
        lo: Bound<K>,
        hi: Bound<K>,
    ) -> Result<Cursor<'a, K, V>> {
        Cursor::new(self, tx, lo, hi)
    }

    pub(crate) fn find_leaf(&self, tx: &ReadGuard, key: &[u8]) -> Result<(PageRef, page::Header)> {
        let mut current = PageId(self.root.load(AtomicOrdering::SeqCst));
        loop {
            let page = self.store.get_page(tx, current)?;
            let header = page::Header::parse(page.data())?;
            match header.kind {
                page::BTreePageKind::Leaf => {
                    self.stats.inc_leaf_searches();
                    tracing::trace!(
                        target: "sombra_btree::search",
                        page = current.0,
                        kind = "leaf",
                        "located target leaf"
                    );
                    return Ok((page, header));
                }
                page::BTreePageKind::Internal => {
                    self.stats.inc_internal_searches();
                    tracing::trace!(
                        target: "sombra_btree::search",
                        page = current.0,
                        kind = "internal",
                        "descending through internal node"
                    );
                    current = self.choose_child_from_bytes(page.data(), &header, key)?;
                }
            }
        }
    }

    pub(crate) fn find_leftmost_leaf(&self, tx: &ReadGuard) -> Result<(PageRef, page::Header)> {
        let mut current = PageId(self.root.load(AtomicOrdering::SeqCst));
        loop {
            let page = self.store.get_page(tx, current)?;
            let header = page::Header::parse(page.data())?;
            match header.kind {
                page::BTreePageKind::Leaf => {
                    self.stats.inc_leaf_searches();
                    tracing::trace!(
                        target: "sombra_btree::search",
                        page = current.0,
                        kind = "leaf",
                        "found leftmost leaf"
                    );
                    return Ok((page, header));
                }
                page::BTreePageKind::Internal => {
                    self.stats.inc_internal_searches();
                    tracing::trace!(
                        target: "sombra_btree::search",
                        page = current.0,
                        kind = "internal",
                        "descending to leftmost child"
                    );
                    let slot_view = SlotView::new(&header, page.data())?;
                    if slot_view.len() == 0 {
                        return Err(SombraError::Corruption("internal node without slots"));
                    }
                    let child = {
                        let rec_slice = slot_view.slice(0)?;
                        let record = page::decode_internal_record(rec_slice)?;
                        record.child
                    };
                    drop(page);
                    current = child;
                }
            }
        }
    }

    pub(crate) fn load_leaf_page(
        &self,
        tx: &ReadGuard,
        page_id: PageId,
    ) -> Result<(PageRef, page::Header)> {
        let page = self.store.get_page(tx, page_id)?;
        let header = page::Header::parse(page.data())?;
        if header.kind != page::BTreePageKind::Leaf {
            return Err(SombraError::Corruption("expected leaf page"));
        }
        Ok((page, header))
    }
}
