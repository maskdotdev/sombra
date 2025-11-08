use std::cmp::Ordering;
use std::ops::Bound;

use crate::primitives::pager::{PageRef, ReadGuard};
use crate::types::{Result, SombraError};

use super::page;
use super::tree::{BTree, KeyCodec, ValCodec};

pub struct Cursor<'a, K: KeyCodec, V: ValCodec> {
    tree: &'a BTree<K, V>,
    tx: &'a ReadGuard,
    lower: EncodedBound,
    upper: EncodedBound,
    current_page: Option<PageRef>,
    current_header: Option<page::Header>,
    slot_index: usize,
    prev_key: Vec<u8>,
    done: bool,
}

impl<'a, K: KeyCodec, V: ValCodec> Cursor<'a, K, V> {
    pub(crate) fn new(
        tree: &'a BTree<K, V>,
        tx: &'a ReadGuard,
        lo: Bound<K>,
        hi: Bound<K>,
    ) -> Result<Self> {
        let lower = EncodedBound::from_bound::<K>(lo);
        let upper = EncodedBound::from_bound::<K>(hi);
        if EncodedBound::range_is_empty::<K>(&lower, &upper) {
            return Ok(Self {
                tree,
                tx,
                lower,
                upper,
                current_page: None,
                current_header: None,
                slot_index: 0,
                prev_key: Vec::new(),
                done: true,
            });
        }

        let mut cursor = Self {
            tree,
            tx,
            lower,
            upper,
            current_page: None,
            current_header: None,
            slot_index: 0,
            prev_key: Vec::new(),
            done: false,
        };
        cursor.initialize()?;
        Ok(cursor)
    }

    pub fn next(&mut self) -> Result<Option<(K, V)>> {
        if self.done {
            return Ok(None);
        }
        loop {
            let (page, header) = match self.current_pair() {
                Some(pair) => pair,
                None => {
                    self.finish();
                    return Ok(None);
                }
            };
            let payload = page::payload(page.data())?;
            let slots = header.slot_directory(page.data())?;
            if self.slot_index >= slots.len() {
                if !self.advance_to_next_leaf()? {
                    return Ok(None);
                }
                continue;
            }
            let rec_slice =
                page::record_slice_from_parts(header, payload, &slots, self.slot_index)?;
            let record = page::decode_leaf_record(rec_slice)?;
            let key = Self::build_key(
                self.tree.prefix_compression_enabled(),
                self.prev_key.as_slice(),
                &record,
            )?;
            if self.is_past_upper(key.as_slice()) {
                self.finish();
                return Ok(None);
            }
            let value = V::decode_val(record.value)?;
            let typed_key = K::decode_key(key.as_slice())?;
            self.prev_key = key.clone();
            self.slot_index += 1;
            return Ok(Some((typed_key, value)));
        }
    }

    fn initialize(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        let (page, header) = match self.lower.key_bytes() {
            Some(key) => self.tree.find_leaf(self.tx, key)?,
            None => self.tree.find_leftmost_leaf(self.tx)?,
        };
        self.current_page = Some(page);
        self.current_header = Some(header);
        self.slot_index = 0;
        self.seek_to_lower_bound()
    }

    fn seek_to_lower_bound(&mut self) -> Result<()> {
        if self.done {
            return Ok(());
        }
        loop {
            let (page, header) = match self.current_pair() {
                Some(pair) => pair,
                None => {
                    self.finish();
                    return Ok(());
                }
            };
            let payload = page::payload(page.data())?;
            let slots = header.slot_directory(page.data())?;
            if slots.len() == 0 {
                if !self.advance_to_next_leaf()? {
                    return Ok(());
                }
                continue;
            }
            let (low_fence, _) = header.fence_slices(page.data())?;
            let mut prev_key = if self.tree.prefix_compression_enabled() {
                low_fence.to_vec()
            } else {
                Vec::new()
            };
            for idx in 0..slots.len() {
                let rec_slice = page::record_slice_from_parts(header, payload, &slots, idx)?;
                let record = page::decode_leaf_record(rec_slice)?;
                let key = Self::build_key(
                    self.tree.prefix_compression_enabled(),
                    prev_key.as_slice(),
                    &record,
                )?;
                if self.is_past_upper(key.as_slice()) {
                    self.finish();
                    return Ok(());
                }
                if self.lower_allows(key.as_slice()) {
                    self.slot_index = idx;
                    self.prev_key = prev_key;
                    return Ok(());
                }
                prev_key = key;
            }
            if !self.advance_to_next_leaf()? {
                return Ok(());
            }
        }
    }

    fn advance_to_next_leaf(&mut self) -> Result<bool> {
        let next_id = match self
            .current_header
            .as_ref()
            .and_then(|header| header.right_sibling)
        {
            Some(id) => id,
            None => {
                self.finish();
                return Ok(false);
            }
        };
        let (page, header) = self.tree.load_leaf_page(self.tx, next_id)?;
        self.current_page = Some(page);
        self.current_header = Some(header);
        self.slot_index = 0;
        self.set_prev_key_to_low_fence()?;
        Ok(true)
    }

    fn set_prev_key_to_low_fence(&mut self) -> Result<()> {
        if !self.tree.prefix_compression_enabled() {
            self.prev_key.clear();
            return Ok(());
        }
        if let Some((page, header)) = self.current_pair() {
            let (low_fence, _) = header.fence_slices(page.data())?;
            self.prev_key = low_fence.to_vec();
        } else {
            self.prev_key.clear();
        }
        Ok(())
    }

    fn current_pair(&self) -> Option<(&PageRef, &page::Header)> {
        self.current_page
            .as_ref()
            .and_then(|page| self.current_header.as_ref().map(|header| (page, header)))
    }

    fn finish(&mut self) {
        self.done = true;
        self.current_page = None;
        self.current_header = None;
        self.slot_index = 0;
        self.prev_key.clear();
    }

    fn lower_allows(&self, key: &[u8]) -> bool {
        match &self.lower {
            EncodedBound::Unbounded => true,
            EncodedBound::Included(bound) => {
                matches!(
                    K::compare_encoded(key, bound),
                    Ordering::Equal | Ordering::Greater
                )
            }
            EncodedBound::Excluded(bound) => {
                matches!(K::compare_encoded(key, bound), Ordering::Greater)
            }
        }
    }

    fn is_past_upper(&self, key: &[u8]) -> bool {
        match &self.upper {
            EncodedBound::Unbounded => false,
            EncodedBound::Included(bound) => {
                matches!(K::compare_encoded(key, bound), Ordering::Greater)
            }
            EncodedBound::Excluded(bound) => {
                !matches!(K::compare_encoded(key, bound), Ordering::Less)
            }
        }
    }

    fn build_key(
        prefix_compress: bool,
        base: &[u8],
        record: &page::LeafRecordRef<'_>,
    ) -> Result<Vec<u8>> {
        let mut key = Vec::new();
        if prefix_compress {
            let prefix_len = record.prefix_len as usize;
            if prefix_len > base.len() {
                return Err(SombraError::Corruption("leaf prefix exceeds base key"));
            }
            key.extend_from_slice(&base[..prefix_len]);
        } else if record.prefix_len != 0 {
            return Err(SombraError::Corruption(
                "non-zero prefix length with compression disabled",
            ));
        }
        key.extend_from_slice(record.key_suffix);
        Ok(key)
    }
}

#[derive(Clone)]
enum EncodedBound {
    Unbounded,
    Included(Vec<u8>),
    Excluded(Vec<u8>),
}

impl EncodedBound {
    fn from_bound<K: KeyCodec>(bound: Bound<K>) -> Self {
        match bound {
            Bound::Unbounded => EncodedBound::Unbounded,
            Bound::Included(key) => {
                let mut buf = Vec::new();
                K::encode_key(&key, &mut buf);
                EncodedBound::Included(buf)
            }
            Bound::Excluded(key) => {
                let mut buf = Vec::new();
                K::encode_key(&key, &mut buf);
                EncodedBound::Excluded(buf)
            }
        }
    }

    fn key_bytes(&self) -> Option<&[u8]> {
        match self {
            EncodedBound::Unbounded => None,
            EncodedBound::Included(bytes) | EncodedBound::Excluded(bytes) => Some(bytes.as_slice()),
        }
    }

    fn range_is_empty<K: KeyCodec>(lower: &EncodedBound, upper: &EncodedBound) -> bool {
        let Some(lo) = lower.key_bytes() else {
            return false;
        };
        let Some(hi) = upper.key_bytes() else {
            return false;
        };
        match K::compare_encoded(lo, hi) {
            Ordering::Greater => true,
            Ordering::Equal => !matches!(
                (lower, upper),
                (EncodedBound::Included(_), EncodedBound::Included(_))
            ),
            Ordering::Less => false,
        }
    }
}
