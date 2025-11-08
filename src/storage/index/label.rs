use std::collections::HashSet;
use std::ops::Bound;
use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::btree::{
    page::{self, BTreePageKind},
    BTree, BTreeOptions, Cursor, ValCodec,
};
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
    tree: BTree<Vec<u8>, EmptyValue>,
    indexed_labels: RwLock<HashSet<LabelId>>,
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
                index.tree.put(&mut write, &sentinel_key, &EmptyValue)?;
            }
            store.commit(write)?;
        }
        Ok((index, root_page))
    }

    pub fn is_indexed_read(&self, label: LabelId) -> Result<bool> {
        if self.indexed_labels.read().contains(&label) {
            return Ok(true);
        }
        let read = self.store.begin_read()?;
        let key = encode_key(label, LABEL_SENTINEL_NODE);
        let present = match self.tree.get(&read, &key) {
            Ok(value) => value.is_some(),
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
            Ok(false)
        }
    }

    pub fn is_indexed_with_write(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<bool> {
        if self.indexed_labels.read().contains(&label) {
            return Ok(true);
        }
        self.ensure_root_initialized(tx)?;
        let key = encode_key(label, LABEL_SENTINEL_NODE);
        let present = match self.tree.get_with_write(tx, &key) {
            Ok(value) => value.is_some(),
            Err(SombraError::Corruption(msg))
                if msg == "unknown btree page kind" || msg == "invalid page magic" =>
            {
                false
            }
            Err(err) => return Err(err),
        };
        if present {
            self.indexed_labels.write().insert(label);
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
        self.tree.put(tx, &sentinel_key, &EmptyValue)?;
        for node in existing_nodes {
            if node == LABEL_SENTINEL_NODE {
                continue;
            }
            let key = encode_key(label, node);
            self.tree.put(tx, &key, &EmptyValue)?;
        }
        self.indexed_labels.write().insert(label);
        Ok(())
    }

    pub fn drop_index(&self, tx: &mut WriteGuard<'_>, label: LabelId) -> Result<()> {
        self.ensure_root_initialized(tx)?;
        if !self.is_indexed_with_write(tx, label)? {
            return Ok(());
        }
        let (lower, upper) = label_bounds(label);
        let read = self.store.begin_read()?;
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
        self.indexed_labels.write().remove(&label);
        Ok(())
    }

    pub fn insert_node(&self, tx: &mut WriteGuard<'_>, label: LabelId, node: NodeId) -> Result<()> {
        self.ensure_root_initialized(tx)?;
        if !self.is_indexed_with_write(tx, label)? {
            return Ok(());
        }
        let key = encode_key(label, node);
        self.tree.put(tx, &key, &EmptyValue)
    }

    pub fn remove_node(&self, tx: &mut WriteGuard<'_>, label: LabelId, node: NodeId) -> Result<()> {
        self.ensure_root_initialized(tx)?;
        if !self.is_indexed_with_write(tx, label)? {
            return Ok(());
        }
        let key = encode_key(label, node);
        let removed = self.tree.delete(tx, &key)?;
        if removed {
            Ok(())
        } else {
            Err(SombraError::Corruption(
                "label index entry missing during removal",
            ))
        }
    }

    pub fn scan<'a>(&'a self, tx: &'a ReadGuard, label: LabelId) -> Result<LabelScan<'a>> {
        let (lower, upper) = label_bounds(label);
        let cursor = self
            .tree
            .range(tx, Bound::Included(lower), Bound::Included(upper))?;
        Ok(LabelScan {
            target_label: label,
            cursor,
        })
    }

    fn ensure_root_initialized(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        let root = self.tree.root_page();
        if root.0 == 0 {
            return Ok(());
        }
        let read = self.store.begin_read()?;
        let page_ref = self.store.get_page(&read, root)?;
        let data = page_ref.data();
        if data.len() < PAGE_HDR_LEN {
            return Err(SombraError::Corruption("btree page shorter than header"));
        }
        let initialized = if &data[..4] == b"SOMB" {
            page::Header::parse(&data[..]).is_ok()
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

/// Iterator for scanning nodes with a specific label.
pub struct LabelScan<'a> {
    target_label: LabelId,
    cursor: Cursor<'a, Vec<u8>, EmptyValue>,
}

impl<'a> LabelScan<'a> {
    /// Retrieves the next node ID matching the target label, if available.
    pub fn next(&mut self) -> Result<Option<NodeId>> {
        while let Some((key, _)) = self.cursor.next()? {
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
