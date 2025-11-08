use std::cmp::Ordering;
use std::marker::PhantomData;
use std::ops::Bound;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;

use super::super::cursor::Cursor;
use super::super::page;
use super::super::stats::{BTreeStats, BTreeStatsSnapshot};
use crate::primitives::pager::{PageMut, PageRef, PageStore, ReadGuard, WriteGuard};
use crate::storage::profile::{
    profile_scope, record_btree_leaf_key_cmps, record_btree_leaf_key_decodes,
    record_btree_leaf_memcopy_bytes, StorageProfileKind,
};
use crate::types::{
    page::PAGE_HDR_LEN,
    page::{PageHeader, PageKind},
    PageId, Result, SombraError,
};

/// Trait implemented by key types that can be encoded for storage in the B+ tree.
pub trait KeyCodec: Sized {
    /// Encode `key` into `out` using the order-preserving representation.
    fn encode_key(key: &Self, out: &mut Vec<u8>);

    /// Compare two encoded keys.
    fn compare_encoded(a: &[u8], b: &[u8]) -> Ordering;

    /// Decode a key from its encoded representation.
    fn decode_key(bytes: &[u8]) -> Result<Self>;
}

/// Trait implemented by value types that can be stored in the B+ tree.
pub trait ValCodec: Sized {
    /// Encode `value` into `out`.
    fn encode_val(value: &Self, out: &mut Vec<u8>);

    /// Decode a value from `src`.
    fn decode_val(src: &[u8]) -> Result<Self>;
}

/// Configuration knobs for the B+ tree.
#[derive(Clone, Debug)]
pub struct BTreeOptions {
    /// Target fill percentage for pages (0-100)
    pub page_fill_target: u8,
    /// Minimum fill percentage for internal pages before merging (0-100)
    pub internal_min_fill: u8,
    /// Whether to verify checksums when reading pages
    pub checksum_verify_on_read: bool,
    /// Optional root page ID for an existing tree
    pub root_page: Option<PageId>,
}

impl Default for BTreeOptions {
    fn default() -> Self {
        Self {
            page_fill_target: 85,
            internal_min_fill: 40,
            checksum_verify_on_read: true,
            root_page: None,
        }
    }
}

/// Minimal internal state for a B+ tree instance.
pub struct BTree<K: KeyCodec, V: ValCodec> {
    store: Arc<dyn PageStore>,
    root: AtomicU64,
    page_size: usize,
    salt: u64,
    options: BTreeOptions,
    stats: Arc<BTreeStats>,
    _marker: PhantomData<(K, V)>,
}

#[derive(Clone)]
struct PathEntry {
    page_id: PageId,
    slot_index: usize,
}

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
            let payload = page::payload(page.data())?;
            let slots = header.slot_directory(page.data())?;
            for idx in 0..slots.len() {
                let rec_slice = page::record_slice_from_parts(&header, payload, &slots, idx)?;
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
            LeafInsert::Done => Ok(()),
            LeafInsert::Split {
                left_min,
                right_min,
                right_page,
            } => self.propagate_split(tx, path, leaf_id, left_min, right_min, right_page),
        }
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
            drop(page);
            true
        } else {
            let new_low = entries[0].0.clone();
            first_key_changed =
                K::compare_encoded(new_low.as_slice(), low_fence.as_slice()) != Ordering::Equal;
            let high_slice = high_fence.as_slice();
            let primary_layout =
                self.build_leaf_layout(payload_len, new_low.as_slice(), high_slice, &entries)?;
            let local_rebalance = match primary_layout {
                Some(layout) => {
                    let fences_end = page::PAYLOAD_HEADER_LEN + new_low.len() + high_fence.len();
                    {
                        let mut page = tx.page_mut(leaf_id)?;
                        self.apply_leaf_layout(&mut page, &header, fences_end, &layout)?;
                        let high_opt = if high_fence.is_empty() {
                            None
                        } else {
                            Some(high_slice)
                        };
                        self.apply_leaf_fences(&mut page, new_low.as_slice(), high_opt)?;
                    }
                    let fill = Self::fill_percent(payload_len, layout.free_start, layout.free_end);
                    has_parent && fill < self.options.page_fill_target
                }
                None => {
                    let fallback_layout = self.build_leaf_layout(
                        payload_len,
                        low_fence.as_slice(),
                        high_slice,
                        &entries,
                    )?;
                    match fallback_layout {
                        Some(layout) => {
                            {
                                let mut page = tx.page_mut(leaf_id)?;
                                let fences_end =
                                    page::PAYLOAD_HEADER_LEN + low_fence.len() + high_fence.len();
                                self.apply_leaf_layout(&mut page, &header, fences_end, &layout)?;
                                let high_opt = if high_fence.is_empty() {
                                    None
                                } else {
                                    Some(high_slice)
                                };
                                self.apply_leaf_fences(&mut page, low_fence.as_slice(), high_opt)?;
                            }
                            let fill =
                                Self::fill_percent(payload_len, layout.free_start, layout.free_end);
                            first_key_changed = false;
                            has_parent && fill < self.options.page_fill_target
                        }
                        None => {
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
                            true
                        }
                    }
                }
            };

            if first_key_changed {
                parent_update_key = Some(new_low);
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
                    let payload = page::payload(page.data())?;
                    let slots = header.slot_directory(page.data())?;
                    if slots.len() == 0 {
                        return Err(SombraError::Corruption("internal node without slots"));
                    }
                    let child = {
                        let rec_slice = page::record_slice_from_parts(&header, payload, &slots, 0)?;
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
        let payload = page::payload(data)?;
        let slots = header.slot_directory(data)?;
        let mut lo = 0usize;
        let mut hi = slots.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let rec_slice = page::record_slice_from_parts(header, payload, &slots, mid)?;
            let record = page::decode_internal_record(rec_slice)?;
            match K::compare_encoded(key, record.separator) {
                Ordering::Less => hi = mid,
                _ => lo = mid + 1,
            }
        }
        let idx = if lo == 0 {
            0
        } else {
            (lo - 1).min(slots.len() - 1)
        };
        let rec_slice = page::record_slice_from_parts(header, payload, &slots, idx)?;
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
        let payload = page::payload(data)?;
        let slots = header.slot_directory(data)?;
        if slots.len() == 0 {
            return Ok(None);
        }
        let mut lo = 0usize;
        let mut hi = slots.len();
        while lo < hi {
            let mid = (lo + hi) / 2;
            let rec_slice = page::record_slice_from_parts(&header, payload, &slots, mid)?;
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
        let data = page.data();
        let payload = page::payload(data)?;
        let slots = header.slot_directory(data)?;
        let (low_fence, high_fence) = header.fence_slices(data)?;
        let low_fence_vec = low_fence.to_vec();
        let high_fence_vec = high_fence.to_vec();
        let mut entries = Vec::with_capacity(slots.len());
        for idx in 0..slots.len() {
            let rec_slice = page::record_slice_from_parts(&header, payload, &slots, idx)?;
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
        let payload = page::payload(data)?;
        let slots = header.slot_directory(data)?;
        let (low_fence_bytes, high_fence_bytes) = header.fence_slices(data)?;
        let old_low_fence = low_fence_bytes.to_vec();
        let old_high_fence = high_fence_bytes.to_vec();
        let mut entries = Vec::with_capacity(slots.len() + 1);
        for idx in 0..slots.len() {
            let rec_slice = page::record_slice_from_parts(&header, payload, &slots, idx)?;
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
        let split_at = split_at.ok_or_else(|| {
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
            .checked_mul(2)
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
        for (key, value) in entries {
            let record_len = page::plain_leaf_record_encoded_len(key.len(), value.len())?;
            if records.len() + record_len > max_records_bytes {
                return Ok(None);
            }
            let offset = fences_end + records.len();
            let offset_u16 = u16::try_from(offset)
                .map_err(|_| SombraError::Invalid("record offset exceeds u16"))?;
            offsets.push(offset_u16);
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
        for (i, offset) in layout.offsets.iter().enumerate() {
            let pos = new_free_end + i * 2;
            payload[pos..pos + 2].copy_from_slice(&offset.to_be_bytes());
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

enum LeafInsert {
    Done,
    Split {
        left_min: Vec<u8>,
        right_min: Vec<u8>,
        right_page: PageId,
    },
}

enum BorrowResult {
    Borrowed,
    InsufficientDonor,
    LayoutOverflow,
}

enum InternalInsert {
    Done,
    Split {
        left_min: Vec<u8>,
        right_min: Vec<u8>,
        right_page: PageId,
    },
}

struct LeafLayout {
    records: Vec<u8>,
    offsets: Vec<u16>,
    free_start: u16,
    free_end: u16,
}

struct InternalLayout {
    records: Vec<u8>,
    offsets: Vec<u16>,
    free_start: u16,
    free_end: u16,
}

struct LeafSnapshot {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    low_fence: Vec<u8>,
    high_fence: Vec<u8>,
}

struct InternalSnapshot {
    entries: Vec<(Vec<u8>, PageId)>,
    low_fence: Vec<u8>,
    high_fence: Vec<u8>,
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
            .checked_mul(2)
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
        for (key, child) in entries {
            let record_len = page::INTERNAL_RECORD_HEADER_LEN + key.len();
            if records.len() + record_len > max_records_bytes {
                return Ok(None);
            }
            let offset = fences_end + records.len();
            let offset_u16 = u16::try_from(offset)
                .map_err(|_| SombraError::Invalid("record offset exceeds u16"))?;
            offsets.push(offset_u16);
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
        for (i, offset) in layout.offsets.iter().enumerate() {
            let pos = new_free_end + i * 2;
            payload[pos..pos + 2].copy_from_slice(&offset.to_be_bytes());
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
        let payload = page::payload(data)?;
        let slots = header.slot_directory(data)?;
        let (low_fence_bytes, high_fence_bytes) = header.fence_slices(data)?;
        let low_vec = low_fence_bytes.to_vec();
        let high_vec = high_fence_bytes.to_vec();
        let mut entries = Vec::with_capacity(slots.len());
        for idx in 0..slots.len() {
            let rec_slice = page::record_slice_from_parts(header, payload, &slots, idx)?;
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
        let payload = page::payload(data)?;
        let slots = header.slot_directory(data)?;
        let (low_fence_bytes, high_fence_bytes) = header.fence_slices(data)?;
        let low_vec = low_fence_bytes.to_vec();
        let high_vec = high_fence_bytes.to_vec();
        let mut entries = Vec::with_capacity(slots.len());
        for idx in 0..slots.len() {
            let rec_slice = page::record_slice_from_parts(header, payload, &slots, idx)?;
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
            self.apply_leaf_layout(&mut page, &left_header, fences_end, &layout)?;
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
                    let payload = page::payload(page.data())?;
                    let slots = header.slot_directory(page.data())?;
                    if slots.len() == 0 {
                        return Err(SombraError::Corruption("internal node without slots"));
                    }
                    let rec_slice = page::record_slice_from_parts(&header, payload, &slots, 0)?;
                    let record = page::decode_internal_record(rec_slice)?;
                    let next = record.child;
                    drop(page);
                    current = next;
                }
            }
        }
    }
}

fn init_leaf_root(
    store: &Arc<dyn PageStore>,
    write: &mut WriteGuard<'_>,
    page_id: PageId,
    page_size: usize,
    salt: u64,
) -> Result<()> {
    let mut page = write.page_mut(page_id)?;
    let buf = page.data_mut();
    if buf.len() < page_size {
        return Err(SombraError::Invalid("page buffer shorter than page size"));
    }
    buf[..page_size].fill(0);
    let header = PageHeader::new(
        page_id,
        crate::types::page::PageKind::BTreeLeaf,
        store.page_size(),
        salt,
    )?
    .with_crc32(0);
    header.encode(&mut buf[..PAGE_HDR_LEN])?;
    page::write_initial_header(&mut buf[PAGE_HDR_LEN..page_size], page::BTreePageKind::Leaf)
}

fn meta_salt(store: &Arc<dyn PageStore>) -> Result<u64> {
    let read = store.begin_read()?;
    let meta = store.get_page(&read, PageId(0))?;
    let header = PageHeader::decode(&meta.data()[..PAGE_HDR_LEN])?;
    Ok(header.salt)
}
