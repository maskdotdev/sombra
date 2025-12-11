use std::sync::Arc;
use std::time::SystemTime;

use crate::primitives::pager::PageStore;
use crate::storage::btree::{BTree, BTreeOptions};
use crate::storage::VersionedValue;
use crate::types::{LabelId, PageId, Result, SombraError};

use super::UnitValue;

pub(crate) fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

pub(crate) fn open_u64_vec_tree(
    store: &Arc<dyn PageStore>,
    root: PageId,
) -> Result<BTree<u64, Vec<u8>>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

pub(crate) fn open_unit_tree(
    store: &Arc<dyn PageStore>,
    root: PageId,
) -> Result<BTree<Vec<u8>, VersionedValue<UnitValue>>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

#[cfg(feature = "degree-cache")]
pub(crate) fn open_degree_tree(
    store: &Arc<dyn PageStore>,
    root: PageId,
) -> Result<BTree<Vec<u8>, u64>> {
    let mut opts = BTreeOptions::default();
    opts.root_page = (root.0 != 0).then_some(root);
    BTree::open_or_create(store, opts)
}

pub(crate) fn normalize_labels(labels: &[LabelId]) -> Result<Vec<LabelId>> {
    let mut result: Vec<LabelId> = labels.to_vec();
    result.sort_by(|a, b| a.0.cmp(&b.0));
    result.dedup_by(|a, b| a.0 == b.0);
    if result.len() > u8::MAX as usize {
        return Err(SombraError::Invalid("too many labels for node"));
    }
    Ok(result)
}
