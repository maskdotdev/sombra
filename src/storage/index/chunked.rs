use std::cell::{Cell, Ref, RefCell};
use std::collections::BTreeMap;
use std::ops::Bound;
use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::btree::{page, BTree, BTreeOptions};
use crate::storage::{mvcc_flags, CommitId, VersionHeader, VersionedValue, COMMIT_MAX};
use crate::types::{
    page::{PageHeader, PageKind, PAGE_HDR_LEN},
    LabelId, NodeId, PageId, PropId, Result, SombraError,
};

use super::types::{EmptyPostingStream, PostingStream};

const SEGMENT_PRIMARY: u32 = 0;

pub struct ChunkedIndex {
    store: Arc<dyn PageStore>,
    root: Cell<PageId>,
    tree: RefCell<Option<BTree<Vec<u8>, VersionedValue<Vec<u8>>>>>,
}

impl ChunkedIndex {
    pub fn open(store: &Arc<dyn PageStore>, root: PageId) -> Result<(Self, PageId)> {
        let index = Self {
            store: Arc::clone(store),
            root: Cell::new(root),
            tree: RefCell::new(None),
        };
        Ok((index, root))
    }

    pub fn root_page(&self) -> PageId {
        self.root.get()
    }

    pub fn put(&self, tx: &mut WriteGuard<'_>, prefix: &[u8], node: NodeId) -> Result<()> {
        self.ensure_tree_with_write(tx)?;
        let tree_ref = self.tree.borrow();
        let tree = tree_ref.as_ref().expect("chunked index tree initialised");
        let key = Self::make_key(prefix, SEGMENT_PRIMARY);
        let mut segment = match tree.get_with_write(tx, &key)? {
            Some(bytes) => Segment::decode(&bytes.value)?,
            None => Segment::new(),
        };
        segment.insert(node);
        let encoded = segment.encode();
        let value = self.versioned_bytes(tx, encoded);
        tree.put(tx, &key, &value)
    }

    pub fn remove(&self, tx: &mut WriteGuard<'_>, prefix: &[u8], node: NodeId) -> Result<()> {
        if self.root_page().0 == 0 {
            return Err(SombraError::Corruption("chunked postings segment missing"));
        }
        self.ensure_tree_with_write(tx)?;
        let tree_ref = self.tree.borrow();
        let tree = tree_ref.as_ref().expect("chunked index tree initialised");
        let key = Self::make_key(prefix, SEGMENT_PRIMARY);
        let Some(bytes) = tree.get_with_write(tx, &key)? else {
            return Err(SombraError::Corruption("chunked postings segment missing"));
        };
        let mut segment = Segment::decode(&bytes.value)?;
        if !segment.remove(node) {
            return Err(SombraError::Corruption("chunked postings entry missing"));
        }
        if segment.is_empty() {
            let removed = tree.delete(tx, &key)?;
            debug_assert!(removed, "expected postings segment to exist");
            Ok(())
        } else {
            let encoded = segment.encode();
            let value = self.versioned_bytes(tx, encoded);
            tree.put(tx, &key, &value)
        }
    }

    pub fn scan(&self, tx: &ReadGuard, prefix: &[u8]) -> Result<Vec<NodeId>> {
        if self.root_page().0 == 0 {
            return Ok(Vec::new());
        }
        self.ensure_tree_read()?;
        let tree_ref = self.tree.borrow();
        let tree = tree_ref.as_ref().expect("chunked index tree initialised");
        let mut out = Vec::new();
        let mut lower = prefix.to_vec();
        lower.extend_from_slice(&SEGMENT_PRIMARY.to_be_bytes());
        let mut upper = prefix.to_vec();
        upper.extend_from_slice(&u32::MAX.to_be_bytes());
        let mut cursor = tree.range(
            tx,
            std::ops::Bound::Included(lower),
            std::ops::Bound::Included(upper),
        )?;
        let snapshot = snapshot_commit(tx);
        while let Some((_, bytes)) = cursor.next()? {
            if !bytes.header.visible_at(snapshot)
                || (bytes.header.flags & mvcc_flags::TOMBSTONE) != 0
            {
                continue;
            }
            let segment = Segment::decode(&bytes.value)?;
            out.extend(segment.nodes.iter().copied());
        }
        out.sort_by_key(|node| node.0);
        out.dedup_by_key(|node| node.0);
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
        let tree = tree_ref.as_ref().expect("chunked index tree initialised");

        let lower = make_chunk_lower_bound(label, prop, start);
        let upper = make_chunk_upper_bound(label, prop, end);

        let mut cursor = tree.range(tx, lower, upper)?;
        let mut grouped: BTreeMap<Vec<u8>, Vec<NodeId>> = BTreeMap::new();
        let snapshot = snapshot_commit(tx);
        while let Some((key, bytes)) = cursor.next()? {
            if !bytes.header.visible_at(snapshot)
                || (bytes.header.flags & mvcc_flags::TOMBSTONE) != 0
            {
                continue;
            }
            if key.len() < 12 {
                return Err(SombraError::Corruption("chunked postings key too short"));
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
            let value_len = key.len() - 4;
            let value_key = key[8..value_len].to_vec();
            let segment = Segment::decode(&bytes.value)?;
            let entry = grouped.entry(value_key).or_default();
            entry.extend(segment.nodes.iter().copied());
        }

        let mut out = Vec::new();
        for (value_key, mut nodes) in grouped {
            nodes.sort_by_key(|node| node.0);
            nodes.dedup_by_key(|node| node.0);
            for node in nodes {
                out.push((value_key.clone(), node));
            }
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
        let lower = make_chunk_lower_bound(label, prop, start);
        let upper = make_chunk_upper_bound(label, prop, end);
        let snapshot = snapshot_commit(tx);
        let keys = self.collect_stream_keys(tx, label, prop, lower, upper, snapshot)?;
        let stream = ChunkedPostingStream::new(self, tx, keys, snapshot);
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
        let tree = tree_ref.as_ref().expect("chunked index tree initialised");
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
        let tree = BTree::open_or_create(&self.store, opts)?;
        self.tree.replace(Some(tree));
        Ok(())
    }

    fn borrow_tree(&self) -> Result<Ref<'_, BTree<Vec<u8>, VersionedValue<Vec<u8>>>>> {
        self.ensure_tree_read()?;
        Ok(Ref::map(self.tree.borrow(), |opt| {
            opt.as_ref().expect("chunked index tree initialised")
        }))
    }

    fn collect_stream_keys(
        &self,
        tx: &ReadGuard,
        label: LabelId,
        prop: PropId,
        lower: Bound<Vec<u8>>,
        upper: Bound<Vec<u8>>,
        snapshot: CommitId,
    ) -> Result<Vec<Vec<u8>>> {
        let mut keys = Vec::new();
        {
            let tree = self.borrow_tree()?;
            let mut cursor = tree.range(tx, lower, upper)?;
            while let Some((key, value)) = cursor.next()? {
                if !value.header.visible_at(snapshot)
                    || (value.header.flags & mvcc_flags::TOMBSTONE) != 0
                {
                    continue;
                }
                if key.len() < 12 {
                    return Err(SombraError::Corruption("chunked postings key too short"));
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

    fn versioned_bytes(&self, tx: &mut WriteGuard<'_>, data: Vec<u8>) -> VersionedValue<Vec<u8>> {
        let commit = tx.reserve_commit_id().0;
        VersionedValue::new(VersionHeader::new(commit, COMMIT_MAX, 0, 0), data)
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

    fn make_key(prefix: &[u8], segment: u32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(prefix.len() + 4);
        buf.extend_from_slice(prefix);
        buf.extend_from_slice(&segment.to_be_bytes());
        buf
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

fn snapshot_commit(tx: &ReadGuard) -> CommitId {
    tx.snapshot_lsn().0
}

fn make_chunk_lower_bound(label: LabelId, prop: PropId, bound: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Unbounded => Bound::Included(base_prefix(label, prop)),
        Bound::Included(value) => {
            let mut key = ChunkedIndex::make_prefix(label, prop, &value);
            key.extend_from_slice(&SEGMENT_PRIMARY.to_be_bytes());
            Bound::Included(key)
        }
        Bound::Excluded(value) => {
            let mut key = ChunkedIndex::make_prefix(label, prop, &value);
            key.extend_from_slice(&u32::MAX.to_be_bytes());
            Bound::Excluded(key)
        }
    }
}

fn make_chunk_upper_bound(label: LabelId, prop: PropId, bound: Bound<Vec<u8>>) -> Bound<Vec<u8>> {
    match bound {
        Bound::Unbounded => match next_prefix(label, prop) {
            Some(next) => Bound::Excluded(next),
            None => Bound::Unbounded,
        },
        Bound::Included(value) => {
            let mut key = ChunkedIndex::make_prefix(label, prop, &value);
            key.extend_from_slice(&u32::MAX.to_be_bytes());
            Bound::Included(key)
        }
        Bound::Excluded(value) => {
            let mut key = ChunkedIndex::make_prefix(label, prop, &value);
            key.extend_from_slice(&SEGMENT_PRIMARY.to_be_bytes());
            Bound::Excluded(key)
        }
    }
}

struct ChunkedPostingStream<'a> {
    index: &'a ChunkedIndex,
    guard: &'a ReadGuard,
    keys: Vec<Vec<u8>>,
    key_pos: usize,
    buffer: Vec<NodeId>,
    buf_pos: usize,
    last: Option<NodeId>,
    done: bool,
    snapshot: CommitId,
}

impl<'a> ChunkedPostingStream<'a> {
    fn new(
        index: &'a ChunkedIndex,
        guard: &'a ReadGuard,
        keys: Vec<Vec<u8>>,
        snapshot: CommitId,
    ) -> Self {
        Self {
            index,
            guard,
            keys,
            key_pos: 0,
            buffer: Vec::new(),
            buf_pos: 0,
            last: None,
            done: false,
            snapshot,
        }
    }

    fn load_next_segment(&mut self) -> Result<bool> {
        if self.done {
            return Ok(false);
        }
        while self.key_pos < self.keys.len() {
            let key = &self.keys[self.key_pos];
            self.key_pos += 1;
            let value = {
                let tree = self.index.borrow_tree()?;
                let result = tree.get(self.guard, key)?;
                result
            };
            let value = match value {
                Some(bytes) => bytes,
                None => {
                    return Err(SombraError::Corruption(
                        "chunked postings segment missing during stream",
                    ))
                }
            };
            if !value.header.visible_at(self.snapshot)
                || (value.header.flags & mvcc_flags::TOMBSTONE) != 0
            {
                continue;
            }
            let nodes = Segment::decode(&value.value)?.into_nodes();
            if nodes.is_empty() {
                continue;
            }
            let start_idx = if let Some(last) = self.last {
                match nodes.binary_search_by_key(&last.0, |n| n.0) {
                    Ok(pos) => pos + 1,
                    Err(pos) => pos,
                }
            } else {
                0
            };
            if start_idx >= nodes.len() {
                continue;
            }
            self.buffer = nodes;
            self.buf_pos = start_idx;
            return Ok(true);
        }
        self.buffer.clear();
        self.buf_pos = 0;
        self.done = true;
        Ok(false)
    }
}

impl PostingStream for ChunkedPostingStream<'_> {
    fn next_batch(&mut self, out: &mut Vec<NodeId>, max: usize) -> Result<bool> {
        if max == 0 {
            return Ok(!(self.done && self.buf_pos >= self.buffer.len()));
        }
        let mut produced = 0;
        while produced < max {
            if self.buf_pos >= self.buffer.len() {
                if !self.load_next_segment()? {
                    return Ok(false);
                }
            }
            let node = self.buffer[self.buf_pos];
            self.buf_pos += 1;
            if let Some(last) = self.last {
                if node.0 == last.0 {
                    continue;
                }
            }
            self.last = Some(node);
            out.push(node);
            produced += 1;
        }
        if self.buf_pos < self.buffer.len() {
            Ok(true)
        } else if self.done {
            Ok(false)
        } else {
            Ok(true)
        }
    }
}

#[derive(Clone, Debug)]
struct Segment {
    nodes: Vec<NodeId>,
}

impl Segment {
    fn new() -> Self {
        Self { nodes: Vec::new() }
    }

    fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 4 {
            return Err(SombraError::Corruption(
                "chunked postings segment truncated",
            ));
        }
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&bytes[..4]);
        let len = u32::from_be_bytes(len_bytes) as usize;
        let expected = 4 + len * 8;
        if bytes.len() != expected {
            return Err(SombraError::Corruption(
                "chunked postings segment length mismatch",
            ));
        }
        let mut nodes = Vec::with_capacity(len);
        let mut offset = 4;
        for _ in 0..len {
            let mut id_bytes = [0u8; 8];
            id_bytes.copy_from_slice(&bytes[offset..offset + 8]);
            nodes.push(NodeId(u64::from_be_bytes(id_bytes)));
            offset += 8;
        }
        Ok(Self { nodes })
    }

    fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + self.nodes.len() * 8);
        let len =
            u32::try_from(self.nodes.len()).expect("chunked postings segment length exceeds u32");
        buf.extend_from_slice(&len.to_be_bytes());
        for node in &self.nodes {
            buf.extend_from_slice(&node.0.to_be_bytes());
        }
        buf
    }

    fn insert(&mut self, node: NodeId) {
        match self.nodes.binary_search_by_key(&node.0, |entry| entry.0) {
            Ok(_) => {}
            Err(pos) => self.nodes.insert(pos, node),
        }
    }

    fn remove(&mut self, node: NodeId) -> bool {
        match self.nodes.binary_search_by_key(&node.0, |entry| entry.0) {
            Ok(pos) => {
                self.nodes.remove(pos);
                true
            }
            Err(_) => false,
        }
    }

    fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    fn into_nodes(self) -> Vec<NodeId> {
        self.nodes
    }
}
