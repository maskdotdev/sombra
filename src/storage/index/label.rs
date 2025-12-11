use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::btree::{
    page::{self, BTreePageKind},
    BTree, BTreeOptions, Cursor, PutItem, ValCodec,
};
use crate::storage::{mvcc_flags, CommitId, VersionHeader, VersionedValue, COMMIT_MAX};
use crate::types::{
    page::{PageHeader, PageKind, PAGE_HDR_LEN},
    LabelId, NodeId, PageId, Result, SombraError,
};
use parking_lot::RwLock;

use super::types::PostingStream;

#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyValue;

impl ValCodec for EmptyValue {
    fn encode_val(_: &Self, _: &mut Vec<u8>) {}

    fn decode_val(src: &[u8]) -> Result<Self> {
        if src.is_empty() {
            Ok(EmptyValue)
        } else {
            Err(SombraError::Corruption(
                "label index value payload not empty",
            ))
        }
    }
}

/// Sentinel node ID used as a marker for label index existence.
pub const LABEL_SENTINEL_NODE: NodeId = NodeId(0);
const GLOBAL_SENTINEL_LABEL: LabelId = LabelId(u32::MAX);

pub struct LabelIndex {
    store: Arc<dyn PageStore>,
    tree: BTree<Vec<u8>, VersionedValue<EmptyValue>>,
    indexed_labels: RwLock<HashSet<LabelId>>,
    /// Cache of labels that are NOT indexed (negative cache for fast skip).
    not_indexed_labels: RwLock<HashSet<LabelId>>,
}

impl LabelIndex {
    pub fn open(store: &Arc<dyn PageStore>, root: PageId) -> Result<(Self, PageId)> {
        let mut opts = BTreeOptions::default();
        opts.root_page = (root.0 != 0).then_some(root);
        let tree = BTree::open_or_create(store, opts)?;
        let root_page = tree.root_page();
        let index = Self {
            store: Arc::clone(store),
            tree,
            indexed_labels: RwLock::new(HashSet::new()),
            not_indexed_labels: RwLock::new(HashSet::new()),
        };
        {
            let mut write = store.begin_write()?;
            index.ensure_root_initialized(&mut write)?;
            let sentinel_key = encode_key(GLOBAL_SENTINEL_LABEL, LABEL_SENTINEL_NODE);
            if index
                .tree
                .get_with_write(&mut write, &sentinel_key)?
                .is_none()
            {
                let value = index.versioned_empty_value(&mut write, false, None);
                index.tree.put(&mut write, &sentinel_key, &value)?;
            }
            store.commit(write)?;
        }
        Ok((index, root_page))
    }

    pub fn is_indexed_read(&self, label: LabelId) -> Result<bool> {
        if self.indexed_labels.read().contains(&label) {
            return Ok(true);
        }
        // Fast path: check negative cache
        if self.not_indexed_labels.read().contains(&label) {
            return Ok(false);
        }
        let read = self.store.begin_latest_committed_read()?;
        let key = encode_key(label, LABEL_SENTINEL_NODE);
        let snapshot = snapshot_commit(&read);
        let present = match self.tree.get(&read, &key) {
            Ok(Some(value)) => {
                value.header.visible_at(snapshot)
                    && (value.header.flags & mvcc_flags::TOMBSTONE) == 0
            }
            Ok(None) => false,
            // Defensive fallback: treat btree corruption as "index not present".
            // The primary fix (cache invalidation on rollback) should prevent this,
            // but we keep this as defense-in-depth for any remaining edge cases.
            Err(SombraError::Corruption(msg))
                if msg == "unknown btree page kind" || msg == "invalid page magic" =>
            {
                false
            }
            Err(err) => return Err(err),
        };
        drop(read);
        if present {
            self.indexed_labels.write().insert(label);
            Ok(true)
        } else {
            // Cache negative result
            self.not_indexed_labels.write().insert(label);
            Ok(false)
        }
    }

    pub fn is_indexed_with_write(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<bool> {
        if self.indexed_labels.read().contains(&label) {
            return Ok(true);
        }
        // Fast path: check negative cache
        if self.not_indexed_labels.read().contains(&label) {
            return Ok(false);
        }
        self.ensure_root_initialized(tx)?;
        let key = encode_key(label, LABEL_SENTINEL_NODE);
        let present = match self.tree.get_with_write(tx, &key) {
            Ok(Some(value)) => value.header.flags & mvcc_flags::TOMBSTONE == 0,
            Ok(None) => false,
            // Defensive fallback: treat btree corruption as "index not present".
            // The primary fix (cache invalidation on rollback) should prevent this,
            // but we keep this as defense-in-depth for any remaining edge cases.
            Err(SombraError::Corruption(msg))
                if msg == "unknown btree page kind" || msg == "invalid page magic" =>
            {
                false
            }
            Err(err) => return Err(err),
        };
        if present {
            self.indexed_labels.write().insert(label);
        } else {
            // Cache negative result
            self.not_indexed_labels.write().insert(label);
        }
        Ok(present)
    }

    pub fn root_page(&self) -> PageId {
        self.tree.root_page()
    }

    pub fn create_index(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
        mut existing_nodes: Vec<NodeId>,
    ) -> Result<()> {
        if label == GLOBAL_SENTINEL_LABEL {
            return Err(SombraError::Invalid("label id reserved for sentinel"));
        }
        self.ensure_root_initialized(tx)?;
        if self.is_indexed_with_write(tx, label)? {
            return Ok(());
        }
        existing_nodes.sort_by_key(|node| node.0);
        existing_nodes.dedup_by_key(|node| node.0);

        let sentinel_key = encode_key(label, LABEL_SENTINEL_NODE);
        let empty_value = self.versioned_empty_value(tx, false, None);
        self.tree.put(tx, &sentinel_key, &empty_value)?;
        let filtered: Vec<_> = existing_nodes
            .into_iter()
            .filter(|node| *node != LABEL_SENTINEL_NODE)
            .collect();
        if !filtered.is_empty() {
            let mut key_bufs = Vec::with_capacity(filtered.len());
            for node in &filtered {
                key_bufs.push(encode_key(label, *node));
            }
            let iter = key_bufs.iter().map(|key| PutItem {
                key,
                value: &empty_value,
            });
            self.tree.put_many(tx, iter)?;
        }
        // Invalidate negative cache and add to positive cache
        self.not_indexed_labels.write().remove(&label);
        self.indexed_labels.write().insert(label);
        Ok(())
    }

    pub fn drop_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        self.ensure_root_initialized(tx)?;
        if !self.is_indexed_with_write(tx, label)? {
            return Ok(());
        }
        let (lower, upper) = label_bounds(label);
        let read = self.store.begin_latest_committed_read()?;
        let mut cursor = self.tree.range(
            &read,
            Bound::Included(lower.clone()),
            Bound::Included(upper.clone()),
        )?;
        let mut keys = Vec::new();
        while let Some((key, _)) = cursor.next()? {
            keys.push(key);
        }
        drop(read);

        for key in keys {
            let removed = self.tree.delete(tx, &key)?;
            if !removed {
                return Err(SombraError::Corruption(
                    "label index entry missing during drop",
                ));
            }
        }
        // Remove from positive cache and add to negative cache
        self.indexed_labels.write().remove(&label);
        self.not_indexed_labels.write().insert(label);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn insert_node(&self, tx: &mut WriteGuard<'_>, label: LabelId, node: NodeId) -> Result<()> {
        self.insert_node_with_commit(tx, label, node, None)
    }

    pub fn insert_node_with_commit(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
        node: NodeId,
        commit: Option<CommitId>,
    ) -> Result<()> {
        self.ensure_root_initialized(tx)?;
        if !self.is_indexed_with_write(tx, label)? {
            return Ok(());
        }
        let key = encode_key(label, node);
        let value = self.versioned_empty_value(tx, false, commit);
        self.tree.put(tx, &key, &value)
    }

    #[allow(dead_code)]
    pub fn remove_node(&self, tx: &mut WriteGuard<'_>, label: LabelId, node: NodeId) -> Result<()> {
        self.remove_node_with_commit(tx, label, node, None)
    }

    pub fn remove_node_with_commit(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
        node: NodeId,
        commit: Option<CommitId>,
    ) -> Result<()> {
        self.ensure_root_initialized(tx)?;
        if !self.is_indexed_with_write(tx, label)? {
            return Ok(());
        }
        let key = encode_key(label, node);
        if self.tree.get_with_write(tx, &key)?.is_none() {
            return Err(SombraError::Corruption(
                "label index entry missing during removal",
            ));
        }
        let tombstone = self.versioned_empty_value(tx, true, commit);
        self.tree.put(tx, &key, &tombstone)
    }

    /// Batch insert multiple (label, node) pairs into the label index.
    /// This is much more efficient than calling `insert_node_with_commit` in a loop
    /// because it uses `put_many` which handles BTree rebalancing efficiently.
    ///
    /// The entries are grouped by label, filtered to only indexed labels, sorted,
    /// and inserted in bulk.
    pub fn insert_nodes_batch_with_commit(
        &self,
        tx: &mut WriteGuard<'_>,
        entries: Vec<(LabelId, NodeId, CommitId)>,
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.ensure_root_initialized(tx)?;

        // Group entries by label for efficient index checking
        let mut by_label: std::collections::BTreeMap<LabelId, Vec<(NodeId, CommitId)>> =
            std::collections::BTreeMap::new();
        for (label, node, commit) in entries {
            by_label.entry(label).or_default().push((node, commit));
        }

        // Filter to only indexed labels and build key-value pairs
        let mut all_entries: Vec<(Vec<u8>, VersionedValue<EmptyValue>)> = Vec::new();

        for (label, nodes) in by_label {
            if !self.is_indexed_with_write(tx, label)? {
                continue;
            }
            for (node, commit) in nodes {
                if node == LABEL_SENTINEL_NODE {
                    continue;
                }
                let key = encode_key(label, node);
                let value = self.versioned_empty_value(tx, false, Some(commit));
                all_entries.push((key, value));
            }
        }

        if all_entries.is_empty() {
            return Ok(());
        }

        // Sort by key for put_many requirement (skip if already sorted)
        if !crate::storage::util::is_sorted_by(&all_entries, |a, b| a.0.cmp(&b.0)) {
            all_entries.sort_by(|a, b| a.0.cmp(&b.0));
        }

        // Use put_many for efficient bulk insertion
        let iter = all_entries
            .iter()
            .map(|(key, value)| PutItem { key, value });
        self.tree.put_many(tx, iter)
    }

    pub fn vacuum(&self, tx: &mut WriteGuard<'_>, horizon: CommitId) -> Result<u64> {
        self.ensure_root_initialized(tx)?;
        prune_versioned_tree(&self.tree, tx, horizon)
    }

    pub fn scan<'a>(&'a self, tx: &'a ReadGuard, label: LabelId) -> Result<LabelScan<'a>> {
        let (lower, upper) = label_bounds(label);
        let snapshot = snapshot_commit(tx);
        let cursor = self
            .tree
            .range(tx, Bound::Included(lower), Bound::Included(upper))?;
        Ok(LabelScan {
            target_label: label,
            cursor,
            snapshot,
        })
    }

    fn ensure_root_initialized(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        let root = self.tree.root_page();
        if root.0 == 0 {
            return Ok(());
        }
        let read = self.store.begin_latest_committed_read()?;
        let page_ref = self.store.get_page(&read, root)?;
        let data = page_ref.data();
        if data.len() < PAGE_HDR_LEN {
            return Err(SombraError::Corruption("btree page shorter than header"));
        }
        let initialized = if &data[..4] == b"SOMB" {
            page::Header::parse(data).is_ok()
        } else {
            false
        };
        drop(read);
        if initialized {
            return Ok(());
        }
        let mut page = tx.page_mut(root)?;
        let data = page.data_mut();
        let meta = self.store.meta()?;
        let page_size = self.store.page_size() as usize;
        if data.len() < page_size {
            return Err(SombraError::Corruption(
                "page buffer smaller than page size",
            ));
        }
        data[..page_size].fill(0);
        let header = PageHeader::new(root, PageKind::BTreeLeaf, self.store.page_size(), meta.salt)?
            .with_crc32(0);
        header.encode(&mut data[..PAGE_HDR_LEN])?;
        page::write_initial_header(&mut data[PAGE_HDR_LEN..page_size], BTreePageKind::Leaf)
    }
}

fn prune_versioned_tree<V: ValCodec>(
    tree: &BTree<Vec<u8>, VersionedValue<V>>,
    tx: &mut WriteGuard<'_>,
    horizon: CommitId,
) -> Result<u64> {
    let mut keys = Vec::new();
    tree.for_each_with_write(tx, |key, value| {
        if value.header.end != COMMIT_MAX && value.header.end <= horizon {
            keys.push(key);
        }
        Ok(())
    })?;
    let mut pruned = 0u64;
    for key in keys {
        if tree.delete(tx, &key)? {
            pruned = pruned.saturating_add(1);
        }
    }
    Ok(pruned)
}

/// Iterator for scanning nodes with a specific label.
pub struct LabelScan<'a> {
    target_label: LabelId,
    cursor: Cursor<'a, Vec<u8>, VersionedValue<EmptyValue>>,
    snapshot: CommitId,
}

impl<'a> LabelScan<'a> {
    /// Retrieves the next node ID matching the target label, if available.
    pub fn next(&mut self) -> Result<Option<NodeId>> {
        while let Some((key, value)) = self.cursor.next()? {
            if !value.header.visible_at(self.snapshot)
                || (value.header.flags & mvcc_flags::TOMBSTONE) != 0
            {
                continue;
            }
            let (label, node) = decode_key(&key)?;
            if label != self.target_label {
                continue;
            }
            if node == LABEL_SENTINEL_NODE {
                continue;
            }
            return Ok(Some(node));
        }
        Ok(None)
    }
}

impl LabelIndex {
    fn versioned_empty_value(
        &self,
        tx: &mut WriteGuard<'_>,
        tombstone: bool,
        commit: Option<CommitId>,
    ) -> VersionedValue<EmptyValue> {
        let commit_id = commit.unwrap_or_else(|| tx.reserve_commit_id().0);
        let mut header = VersionHeader::new(commit_id, COMMIT_MAX, 0, 0);
        if tombstone {
            header.flags |= mvcc_flags::TOMBSTONE;
        }
        VersionedValue::new(header, EmptyValue)
    }
}

fn snapshot_commit(tx: &ReadGuard) -> CommitId {
    tx.snapshot_lsn().0
}

impl<'a> PostingStream for LabelScan<'a> {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        if max == 0 {
            return Ok(true);
        }
        let mut produced = 0;
        while produced < max {
            match self.next()? {
                Some(node) => {
                    out.push(node);
                    produced += 1;
                }
                None => return Ok(false),
            }
        }
        Ok(true)
    }
}

fn encode_key(label: LabelId, node: NodeId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(12);
    buf.extend_from_slice(&label.0.to_be_bytes());
    buf.extend_from_slice(&node.0.to_be_bytes());
    buf
}

fn decode_key(bytes: &[u8]) -> Result<(LabelId, NodeId)> {
    if bytes.len() != 12 {
        return Err(SombraError::Corruption("label index key length invalid"));
    }
    let mut label_bytes = [0u8; 4];
    label_bytes.copy_from_slice(&bytes[..4]);
    let mut node_bytes = [0u8; 8];
    node_bytes.copy_from_slice(&bytes[4..12]);
    Ok((
        LabelId(u32::from_be_bytes(label_bytes)),
        NodeId(u64::from_be_bytes(node_bytes)),
    ))
}

fn label_bounds(label: LabelId) -> (Vec<u8>, Vec<u8>) {
    let lower = encode_key(label, LABEL_SENTINEL_NODE);
    let upper = encode_key(label, NodeId(u64::MAX));
    (lower, upper)
}
