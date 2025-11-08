use std::ops::Bound;
use std::sync::Arc;

use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::btree::{BTree, BTreeOptions};
use crate::types::{LabelId, PageId, PropId, Result, SombraError};

use super::types::{IndexDef, IndexKind, TypeTag};

/// Persistent catalog of property index definitions.
pub struct IndexCatalog {
    store: Arc<dyn PageStore>,
    tree: BTree<Vec<u8>, Vec<u8>>,
}

impl IndexCatalog {
    pub fn open(store: &Arc<dyn PageStore>, root: PageId) -> Result<(Self, PageId)> {
        let mut opts = BTreeOptions::default();
        opts.root_page = (root.0 != 0).then_some(root);
        let tree = BTree::open_or_create(store, opts)?;
        let root_page = tree.root_page();
        let catalog = Self {
            store: Arc::clone(store),
            tree,
        };
        Ok((catalog, root_page))
    }

    pub fn store(&self) -> &Arc<dyn PageStore> {
        &self.store
    }

    pub fn tree(&self) -> &BTree<Vec<u8>, Vec<u8>> {
        &self.tree
    }

    fn encode_key(label: LabelId, prop: PropId) -> Vec<u8> {
        let mut buf = Vec::with_capacity(8);
        buf.extend_from_slice(&label.0.to_be_bytes());
        buf.extend_from_slice(&prop.0.to_be_bytes());
        buf
    }

    fn encode_value(kind: IndexKind, ty: TypeTag) -> Vec<u8> {
        vec![Self::encode_kind(kind), Self::encode_type(ty)]
    }

    fn encode_kind(kind: IndexKind) -> u8 {
        match kind {
            IndexKind::Chunked => 1,
            IndexKind::BTree => 2,
        }
    }

    fn decode_kind(byte: u8) -> Result<IndexKind> {
        match byte {
            1 => Ok(IndexKind::Chunked),
            2 => Ok(IndexKind::BTree),
            _ => Err(SombraError::Corruption("unknown property index kind")),
        }
    }

    fn encode_type(tag: TypeTag) -> u8 {
        match tag {
            TypeTag::Null => 0,
            TypeTag::Bool => 1,
            TypeTag::Int => 2,
            TypeTag::Float => 3,
            TypeTag::String => 4,
            TypeTag::Bytes => 5,
            TypeTag::Date => 6,
            TypeTag::DateTime => 7,
        }
    }

    fn decode_type(byte: u8) -> Result<TypeTag> {
        match byte {
            0 => Ok(TypeTag::Null),
            1 => Ok(TypeTag::Bool),
            2 => Ok(TypeTag::Int),
            3 => Ok(TypeTag::Float),
            4 => Ok(TypeTag::String),
            5 => Ok(TypeTag::Bytes),
            6 => Ok(TypeTag::Date),
            7 => Ok(TypeTag::DateTime),
            _ => Err(SombraError::Corruption("unknown property index type tag")),
        }
    }

    fn decode_value(bytes: &[u8]) -> Result<(IndexKind, TypeTag)> {
        if bytes.len() != 2 {
            return Err(SombraError::Corruption(
                "property catalog payload length invalid",
            ));
        }
        let kind = Self::decode_kind(bytes[0])?;
        let ty = Self::decode_type(bytes[1])?;
        Ok((kind, ty))
    }

    pub fn has_property_index(&self, tx: &ReadGuard, label: LabelId, prop: PropId) -> Result<bool> {
        let key = Self::encode_key(label, prop);
        Ok(self.tree.get(tx, &key)?.is_some())
    }

    pub fn get(&self, tx: &ReadGuard, label: LabelId, prop: PropId) -> Result<Option<IndexDef>> {
        let key = Self::encode_key(label, prop);
        let Some(value) = self.tree.get(tx, &key)? else {
            return Ok(None);
        };
        let (kind, ty) = Self::decode_value(&value)?;
        Ok(Some(IndexDef {
            label,
            prop,
            kind,
            ty,
        }))
    }

    pub fn insert(&self, tx: &mut WriteGuard<'_>, def: IndexDef) -> Result<()> {
        let key = Self::encode_key(def.label, def.prop);
        if self.tree.get_with_write(tx, &key)?.is_some() {
            return Err(SombraError::Invalid("property index already exists"));
        }
        let value = Self::encode_value(def.kind, def.ty);
        self.tree.put(tx, &key, &value)
    }

    pub fn remove(&self, tx: &mut WriteGuard<'_>, label: LabelId, prop: PropId) -> Result<bool> {
        let key = Self::encode_key(label, prop);
        self.tree.delete(tx, &key)
    }

    pub fn iter_label<'a>(&'a self, tx: &'a ReadGuard, label: LabelId) -> Result<Vec<IndexDef>> {
        let mut results = Vec::new();
        let mut lower = Vec::with_capacity(8);
        lower.extend_from_slice(&label.0.to_be_bytes());
        lower.extend_from_slice(&[0u8; 4]);
        let mut upper = Vec::with_capacity(8);
        upper.extend_from_slice(&label.0.to_be_bytes());
        upper.extend_from_slice(&[0xFF; 4]);
        let mut cursor = self
            .tree
            .range(tx, Bound::Included(lower), Bound::Included(upper))?;
        while let Some((key, value)) = cursor.next()? {
            if key.len() != 8 {
                return Err(SombraError::Corruption("catalog key length invalid"));
            }
            let mut prop_bytes = [0u8; 4];
            prop_bytes.copy_from_slice(&key[4..8]);
            let prop = PropId(u32::from_be_bytes(prop_bytes));
            let (kind, ty) = Self::decode_value(&value)?;
            results.push(IndexDef {
                label,
                prop,
                kind,
                ty,
            });
        }
        Ok(results)
    }

    pub fn iter_label_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        label: LabelId,
    ) -> Result<Vec<IndexDef>> {
        let mut results = Vec::new();
        self.tree.for_each_with_write(tx, |key, value| {
            if key.len() != 8 {
                return Err(SombraError::Corruption("catalog key length invalid"));
            }
            let mut label_bytes = [0u8; 4];
            label_bytes.copy_from_slice(&key[..4]);
            let entry_label = LabelId(u32::from_be_bytes(label_bytes));
            if entry_label != label {
                return Ok(());
            }
            let mut prop_bytes = [0u8; 4];
            prop_bytes.copy_from_slice(&key[4..8]);
            let prop = PropId(u32::from_be_bytes(prop_bytes));
            let (kind, ty) = Self::decode_value(&value)?;
            results.push(IndexDef {
                label,
                prop,
                kind,
                ty,
            });
            Ok(())
        })?;
        Ok(results)
    }
}
