use std::cell::{Cell, Ref, RefCell};
use std::ops::Bound;
use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::btree::{page, BTree, BTreeOptions, PutItem, ValCodec};
use crate::types::{
    page::{PageHeader, PageKind, PAGE_HDR_LEN},
    LabelId, NodeId, PageId, PropId, Result, SombraError,
};

use super::types::{EmptyPostingStream, PostingStream};

pub struct BTreePostings {
    store: Arc<dyn PageStore>,
    root: Cell<PageId>,
    tree: RefCell<Option<BTree<Vec<u8>, Unit>>>,
    btree_inplace: bool,
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct Unit;

impl ValCodec for Unit {
    fn encode_val(value: &Self, _out: &mut Vec<u8>) {
        let _ = value;
    }

    fn decode_val(_: &[u8]) -> Result<Self> {
        Ok(Unit)
    }
}

impl BTreePostings {
    pub fn open(
        store: &Arc<dyn PageStore>,
        root: PageId,
        in_place: bool,
    ) -> Result<(Self, PageId)> {
        let index = Self {
            store: Arc::clone(store),
            root: Cell::new(root),
            tree: RefCell::new(None),
            btree_inplace: in_place,
        };
        Ok((index, root))
    }

    pub fn root_page(&self) -> PageId {
        self.root.get()
    }

    pub fn put(&self, tx: &mut WriteGuard<'_>, prefix: &[u8], node: NodeId) -> Result<()> {
        self.ensure_tree_with_write(tx)?;
        let tree_ref = self.tree.borrow();
        let tree = tree_ref.as_ref().expect("btree postings initialised");
        let key = Self::make_key(prefix, node);
        tree.put(tx, &key, &Unit)
    }

    pub fn put_many<'a, I>(&self, tx: &mut WriteGuard<'_>, items: I) -> Result<()>
    where
        I: IntoIterator<Item = PutItem<'a, Vec<u8>, Unit>>,
    {
        self.ensure_tree_with_write(tx)?;
        let tree_ref = self.tree.borrow();
        let tree = tree_ref.as_ref().expect("btree postings initialised");
        tree.put_many(tx, items)
    }

    pub fn remove(&self, tx: &mut WriteGuard<'_>, prefix: &[u8], node: NodeId) -> Result<()> {
        if self.root_page().0 == 0 {
            return Err(SombraError::Corruption(
                "btree postings entry missing during remove",
            ));
        }
        self.ensure_tree_with_write(tx)?;
        let tree_ref = self.tree.borrow();
        let tree = tree_ref.as_ref().expect("btree postings initialised");
        let key = Self::make_key(prefix, node);
        let removed = tree.delete(tx, &key)?;
        if !removed {
            return Err(SombraError::Corruption(
                "btree postings entry missing during remove",
            ));
        }
        Ok(())
    }

    pub fn scan_eq(&self, tx: &ReadGuard, prefix: &[u8]) -> Result<Vec<NodeId>> {
        if self.root_page().0 == 0 {
            return Ok(Vec::new());
        }
        self.ensure_tree_read()?;
        let tree_ref = self.tree.borrow();
        let tree = tree_ref.as_ref().expect("btree postings initialised");
        let mut lower = prefix.to_vec();
        lower.extend_from_slice(&0u64.to_be_bytes());
        let mut upper = prefix.to_vec();
        upper.extend_from_slice(&u64::MAX.to_be_bytes());
        let mut cursor = tree.range(
            tx,
            std::ops::Bound::Included(lower),
            std::ops::Bound::Included(upper),
        )?;
        let mut out = Vec::new();
        while let Some((key, _)) = cursor.next()? {
            let node = Self::parse_node_id(&key)?;
            out.push(node);
        }
        Ok(out)
    }

    pub fn scan_range_bounds(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<Vec<(Vec<u8>, NodeId)>> {
        if self.root_page().0 == 0 {
            return Ok(Vec::new());
        }
        self.ensure_tree_read()?;
        let tree_ref = self.tree.borrow();
        let tree = tree_ref.as_ref().expect("btree postings initialised");
        let lower = make_btree_lower_bound(label, prop, start);
        let upper = make_btree_upper_bound(label, prop, end);
        let mut cursor = tree.range(tx, lower, upper)?;
        let mut out = Vec::new();
        while let Some((key, _)) = cursor.next()? {
            if key.len() < 8 {
                return Err(SombraError::Corruption("btree postings key too short"));
            }
            let mut key_label_bytes = [0u8; 4];
            key_label_bytes.copy_from_slice(&key[0..4]);
            let mut key_prop_bytes = [0u8; 4];
            key_prop_bytes.copy_from_slice(&key[4..8]);
            let key_label = u32::from_be_bytes(key_label_bytes);
            let key_prop = u32::from_be_bytes(key_prop_bytes);
            if key_label != label.0 || key_prop != prop.0 {
                if key_label > label.0 || (key_label == label.0 && key_prop > prop.0) {
                    break;
                }
                continue;
            }
            let value_prefix = key[..key.len() - 8].to_vec();
            let node = Self::parse_node_id(&key)?;
            out.push((value_prefix, node));
        }
        Ok(out)
    }

    pub fn stream_eq<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
        prop: PropId,
        value_key: &[u8],
    ) -> Result<Box<dyn PostingStream + 'a>> {
        let key = value_key.to_vec();
        self.stream_range_bounds(
            tx,
            label,
            prop,
            Bound::Included(key.clone()),
            Bound::Included(key),
        )
    }

    pub fn stream_range_bounds<'a>(
        &'a self,
        tx: &'a ReadGuard,
        label: LabelId,
        prop: PropId,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
    ) -> Result<Box<dyn PostingStream + 'a>> {
        if self.root_page().0 == 0 {
            return Ok(Box::new(EmptyPostingStream::new()));
        }
        let lower = make_btree_lower_bound(label, prop, start);
        let upper = make_btree_upper_bound(label, prop, end);
        let keys = self.collect_stream_keys(tx, label, prop, lower, upper)?;
        let stream = BTreePostingStream::new(self, tx, keys);
        Ok(Box::new(stream))
    }

    pub fn drop_entries(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
        prop: PropId,
    ) -> Result<()> {
        if self.root_page().0 == 0 {
            return Ok(());
        }
        self.ensure_tree_with_write(tx)?;
        let tree_ref = self.tree.borrow();
        let tree = tree_ref.as_ref().expect("btree postings initialised");
        let label_bytes = label.0.to_be_bytes();
        let prop_bytes = prop.0.to_be_bytes();
        let mut keys = Vec::new();
        tree.for_each_with_write(tx, |key, _| {
            if key.len() >= 8 && key[0..4] == label_bytes && key[4..8] == prop_bytes {
                keys.push(key);
            }
            Ok(())
        })?;
        for key in keys {
            let _ = tree.delete(tx, &key)?;
        }
        Ok(())
    }

    fn ensure_tree_with_write(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        if self.tree.borrow().is_some() {
            return Ok(());
        }
        if self.root.get().0 == 0 {
            let root = self.init_root_page(tx)?;
            self.root.set(root);
        }
        let mut opts = BTreeOptions::default();
        opts.root_page = Some(self.root.get());
        opts.in_place_leaf_edits = self.btree_inplace;
        let tree = BTree::open_or_create(&self.store, opts)?;
        self.tree.replace(Some(tree));
        Ok(())
    }

    fn ensure_tree_read(&self) -> Result<()> {
        if self.tree.borrow().is_some() || self.root.get().0 == 0 {
            return Ok(());
        }
        let mut opts = BTreeOptions::default();
        opts.root_page = Some(self.root.get());
        opts.in_place_leaf_edits = self.btree_inplace;
        let tree = BTree::open_or_create(&self.store, opts)?;
        self.tree.replace(Some(tree));
        Ok(())
    }

    fn borrow_tree(&self) -> Result<Ref<'_, BTree<Vec<u8>, Unit>>> {
        self.ensure_tree_read()?;
        Ok(Ref::map(self.tree.borrow(), |opt| {
            opt.as_ref().expect("btree postings initialised")
        }))
    }

    fn collect_stream_keys(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        {
            let tree = self.borrow_tree()?;
            let mut cursor = tree.range(tx, lower, upper)?;
            while let Some((key, _)) = cursor.next()? {
                if key.len() < 8 {
                    return Err(SombraError::Corruption("btree postings key too short"));
                }
                let mut key_label_bytes = [0u8; 4];
                key_label_bytes.copy_from_slice(&key[0..4]);
                let mut key_prop_bytes = [0u8; 4];
                key_prop_bytes.copy_from_slice(&key[4..8]);
                let key_label = u32::from_be_bytes(key_label_bytes);
                let key_prop = u32::from_be_bytes(key_prop_bytes);
                if key_label != label.0 || key_prop != prop.0 {
                    if key_label > label.0 || (key_label == label.0 && key_prop > prop.0) {
                        break;
                    }
                    continue;
                }
                keys.push(key);
            }
        }
        Ok(keys)
    }

    fn init_root_page(&self, tx: &mut WriteGuard<'_>) -> Result<PageId> {
        let page_id = tx.allocate_page()?;
        let mut page = tx.page_mut(page_id)?;
        let page_size = self.store.page_size() as usize;
        let meta = self.store.meta()?;
        let buf = page.data_mut();
        buf[..page_size].fill(0);
        let header = PageHeader::new(
            page_id,
            PageKind::BTreeLeaf,
            self.store.page_size(),
            meta.salt,
        )?
        .with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        page::write_initial_header(&mut buf[PAGE_HDR_LEN..page_size], page::BTreePageKind::Leaf)?;
        Ok(page_id)
    }

    pub fn make_prefix(label: LabelId, prop: PropId, value_key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8 + value_key.len());
        buf.extend_from_slice(&label.0.to_be_bytes());
        buf.extend_from_slice(&prop.0.to_be_bytes());
        buf.extend_from_slice(value_key);
        buf
    }

    pub(crate) fn make_key(prefix: &[u8], node: NodeId) -> Vec<u8> {
        let mut buf = Vec::with_capacity(prefix.len() + 8);
        buf.extend_from_slice(prefix);
        buf.extend_from_slice(&node.0.to_be_bytes());
        buf
    }

    fn parse_node_id(key: &[u8]) -> Result<NodeId> {
        if key.len() < 8 {
            return Err(SombraError::Corruption("btree postings key too short"));
        }
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&key[key.len() - 8..]);
        Ok(NodeId(u64::from_be_bytes(bytes)))
    }
}

struct BTreePostingStream<'a> {
    #[allow(dead_code)]
    index: &'a BTreePostings,
    #[allow(dead_code)]
    guard: &'a ReadGuard,
    keys: Vec<Vec<u8>>,
    key_pos: usize,
    last: Option<NodeId>,
}

impl<'a> BTreePostingStream<'a> {
    fn new(index: &'a BTreePostings, guard: &'a ReadGuard, keys: Vec<Vec<u8>>) -> Self {
        Self {
            index,
            guard,
            keys,
            key_pos: 0,
            last: None,
        }
    }

    fn next_node(&mut self) -> Result<Option<NodeId>> {
        while self.key_pos < self.keys.len() {
            let key = &self.keys[self.key_pos];
            self.key_pos += 1;
            let node = BTreePostings::parse_node_id(key)?;
            if let Some(last) = self.last {
                if node.0 == last.0 {
                    continue;
                }
            }
            self.last = Some(node);
            return Ok(Some(node));
        }
        Ok(None)
    }
}

impl PostingStream for BTreePostingStream<'_> {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        if max == 0 {
            return Ok(self.key_pos < self.keys.len());
        }
        let mut produced = 0;
        while produced < max {
            match self.next_node()? {
                Some(node) => {
                    out.push(node);
                    produced += 1;
                }
                None => return Ok(false),
            }
        }
        Ok(self.key_pos < self.keys.len())
    }
}

fn base_prefix(label: LabelId, prop: PropId) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8);
    buf.extend_from_slice(&label.0.to_be_bytes());
    buf.extend_from_slice(&prop.0.to_be_bytes());
    buf
}

fn next_prefix(label: LabelId, prop: PropId) -> Option<Vec<u8>> {
    if prop.0 < u32::MAX {
        let mut buf = Vec::with_capacity(8);
        buf.extend_from_slice(&label.0.to_be_bytes());
        buf.extend_from_slice(&(prop.0 + 1).to_be_bytes());
        Some(buf)
    } else if label.0 < u32::MAX {
        let mut buf = Vec::with_capacity(8);
        buf.extend_from_slice(&(label.0 + 1).to_be_bytes());
        buf.extend_from_slice(&0u32.to_be_bytes());
        Some(buf)
    } else {
        None
    }
}

fn make_btree_lower_bound(label: LabelId, prop: PropId, bound: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Unbounded => Bound::Included(base_prefix(label, prop)),
        Bound::Included(value) => {
            let mut key = BTreePostings::make_prefix(label, prop, &value);
            key.extend_from_slice(&0u64.to_be_bytes());
            Bound::Included(key)
        }
        Bound::Excluded(value) => {
            let mut key = BTreePostings::make_prefix(label, prop, &value);
            key.extend_from_slice(&u64::MAX.to_be_bytes());
            Bound::Excluded(key)
        }
    }
}

fn make_btree_upper_bound(label: LabelId, prop: PropId, bound: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Unbounded => match next_prefix(label, prop) {
            Some(next) => Bound::Excluded(next),
            None => Bound::Unbounded,
        },
        Bound::Included(value) => {
            let mut key = BTreePostings::make_prefix(label, prop, &value);
            key.extend_from_slice(&u64::MAX.to_be_bytes());
            Bound::Included(key)
        }
        Bound::Excluded(value) => {
            let mut key = BTreePostings::make_prefix(label, prop, &value);
            key.extend_from_slice(&0u64.to_be_bytes());
            Bound::Excluded(key)
        }
    }
}
