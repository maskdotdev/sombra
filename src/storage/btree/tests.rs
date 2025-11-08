#![cfg(test)]

use super::{page, BTree, BTreeOptions};
use crate::primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions, ReadGuard};
use crate::types::{PageId, Result, SombraError};
use proptest::prelude::*;
use std::collections::{BTreeMap, HashSet, VecDeque};
use std::ops::Bound;
use std::sync::Arc;
use tempfile::tempdir;

fn assert_tree_matches_reference(
    tree: &BTree<u64, u64>,
    pager: &Pager,
    reference: &BTreeMap<u64, u64>,
    max_key: u64,
) -> Result<()> {
    let read = pager.begin_read()?;
    for key in 0..=max_key {
        let expected = reference.get(&key).copied();
        assert_eq!(tree.get(&read, &key)?, expected);
    }
    Ok(())
}

fn decode_leaf_keys(
    pager: &Pager,
    read: &ReadGuard,
    leaf_id: PageId,
    prefix_compress: bool,
) -> Result<Vec<u64>> {
    let page = pager.get_page(read, leaf_id)?;
    let header = page::Header::parse(page.data())?;
    assert_eq!(header.kind, page::BTreePageKind::Leaf, "expected leaf page");
    let payload = page::payload(page.data())?;
    let slots = header.slot_directory(page.data())?;
    let mut keys = Vec::with_capacity(slots.len());
    let mut prev_key: Vec<u8> = Vec::new();
    for idx in 0..slots.len() {
        let record_bytes = page::record_slice_from_parts(&header, payload, &slots, idx)?;
        let record = page::decode_leaf_record(record_bytes)?;
        let full_key = if prefix_compress {
            let prefix_len = record.prefix_len as usize;
            if prefix_len > prev_key.len() {
                return Err(SombraError::Corruption("leaf prefix longer than base key"));
            }
            let mut materialized = Vec::with_capacity(prefix_len + record.key_suffix.len());
            materialized.extend_from_slice(&prev_key[..prefix_len]);
            materialized.extend_from_slice(record.key_suffix);
            materialized
        } else {
            if record.prefix_len != 0 {
                return Err(SombraError::Corruption(
                    "unexpected prefix on uncompressed key",
                ));
            }
            record.key_suffix.to_vec()
        };
        if full_key.len() != 8 {
            return Err(SombraError::Corruption("unexpected key length"));
        }
        let mut arr = [0u8; 8];
        arr.copy_from_slice(&full_key);
        keys.push(u64::from_be_bytes(arr));
        prev_key = full_key;
    }
    Ok(keys)
}

fn choose_child_for_key(header: &page::Header, data: &[u8], key: &[u8]) -> Result<PageId> {
    let payload = page::payload(data)?;
    let slots = header.slot_directory(data)?;
    if slots.len() == 0 {
        return Err(SombraError::Corruption("internal node without slots"));
    }
    let mut lo = 0usize;
    let mut hi = slots.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        let rec_slice = page::record_slice_from_parts(header, payload, &slots, mid)?;
        let record = page::decode_internal_record(rec_slice)?;
        if key < record.separator {
            hi = mid;
        } else {
            lo = mid + 1;
        }
    }
    let idx = if lo == 0 {
        0
    } else {
        (lo - 1).min(slots.len() - 1)
    };
    let rec_slice = page::record_slice_from_parts(header, payload, &slots, idx)?;
    let record = page::decode_internal_record(rec_slice)?;
    Ok(record.child)
}

fn find_leaf_for_key(pager: &Pager, read: &ReadGuard, root: PageId, key: &[u8]) -> Result<PageId> {
    let mut current = root;
    loop {
        let page = pager.get_page(read, current)?;
        let header = page::Header::parse(page.data())?;
        match header.kind {
            page::BTreePageKind::Leaf => return Ok(current),
            page::BTreePageKind::Internal => {
                let child = choose_child_for_key(&header, page.data(), key)?;
                drop(page);
                current = child;
            }
        }
    }
}

fn collect_leaf_snapshots(
    pager: &Pager,
    read: &ReadGuard,
    root: PageId,
    prefix_compress: bool,
) -> Result<Vec<(PageId, page::Header, Vec<u64>)>> {
    let mut queue = VecDeque::new();
    let mut visited = HashSet::new();
    let mut leaves = Vec::new();
    queue.push_back(root);
    while let Some(page_id) = queue.pop_front() {
        if !visited.insert(page_id) {
            continue;
        }
        let page = pager.get_page(read, page_id)?;
        let header = page::Header::parse(page.data())?;
        match header.kind {
            page::BTreePageKind::Leaf => {
                let keys = decode_leaf_keys(pager, read, page_id, prefix_compress)?;
                leaves.push((page_id, header, keys));
            }
            page::BTreePageKind::Internal => {
                let payload = page::payload(page.data())?;
                let slots = header.slot_directory(page.data())?;
                for idx in 0..slots.len() {
                    let rec_slice = page::record_slice_from_parts(&header, payload, &slots, idx)?;
                    let record = page::decode_internal_record(rec_slice)?;
                    queue.push_back(record.child);
                }
            }
        }
    }
    leaves.sort_by_key(|(_, _, keys)| keys.first().copied().unwrap_or(u64::MAX));
    Ok(leaves)
}

#[test]
fn empty_tree_get_returns_none() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree.db");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let mut tree_opts = BTreeOptions::default();
    tree_opts.prefix_compress = false;
    let tree = BTree::<u64, u64>::open_or_create(&store, tree_opts)?;
    pager.checkpoint(CheckpointMode::Force)?;
    let read = pager.begin_read()?;
    assert!(tree.get(&read, &42)?.is_none());
    Ok(())
}

#[test]
fn checksum_option_updates_pager() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_checksum.db");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    assert!(store.checksum_verification_enabled());

    let mut tree_opts = BTreeOptions::default();
    tree_opts.checksum_verify_on_read = false;
    let _tree = BTree::<u64, u64>::open_or_create(&store, tree_opts)?;
    assert!(!store.checksum_verification_enabled());

    store.set_checksum_verification(true);
    assert!(store.checksum_verification_enabled());
    Ok(())
}

#[test]
fn insert_and_get_roundtrip() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_insert.db");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())?;

    {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &5, &123)?;
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;
    {
        let read = pager.begin_read()?;
        assert_eq!(tree.get(&read, &5)?, Some(123));
    }

    {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &5, &456)?;
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;
    let read = pager.begin_read()?;
    assert_eq!(tree.get(&read, &5)?, Some(456));
    Ok(())
}

#[test]
fn range_iterates_with_bounds() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_range.db");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())?;

    for key in 0u64..50 {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &key, &(key + 10_000))?;
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let mut cursor = tree.range(&read, Bound::Unbounded, Bound::Unbounded)?;
    let mut collected = Vec::new();
    while let Some((key, value)) = cursor.next()? {
        collected.push((key, value));
    }
    assert_eq!(collected.len(), 50);
    for (idx, (key, value)) in collected.iter().enumerate() {
        assert_eq!(*key, idx as u64);
        assert_eq!(*value, key + 10_000);
    }

    let mut bounded = tree.range(&read, Bound::Included(10), Bound::Excluded(20))?;
    let mut bounded_keys = Vec::new();
    while let Some((key, value)) = bounded.next()? {
        bounded_keys.push((key, value));
    }
    assert_eq!(bounded_keys.len(), 10);
    assert!(bounded_keys
        .iter()
        .all(|(key, value)| { *key >= 10 && *key < 20 && *value == *key + 10_000 }));

    let mut single = tree.range(&read, Bound::Included(25), Bound::Included(25))?;
    let entry = single.next()?;
    assert_eq!(entry, Some((25, 25 + 10_000)));
    assert!(single.next()?.is_none());

    let mut empty = tree.range(&read, Bound::Excluded(30), Bound::Included(30))?;
    assert!(empty.next()?.is_none());

    Ok(())
}

#[test]
fn root_split_creates_new_internal() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_split.db");
    let mut options = PagerOptions::default();
    options.page_size = 512;
    let pager = Arc::new(Pager::create(&path, options)?);
    let store: Arc<dyn PageStore> = pager.clone();
    let mut tree_opts = BTreeOptions::default();
    tree_opts.prefix_compress = false;
    let tree = BTree::<u64, u64>::open_or_create(&store, tree_opts)?;
    let initial_root = tree.root_page();

    let mut inserted_keys = Vec::new();
    let mut root_changed = false;
    for key in 0u64..100 {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &key, &(key + 100))?;
        pager.commit(write)?;
        inserted_keys.push(key);
        if tree.root_page() != initial_root {
            root_changed = true;
            break;
        }
    }
    assert!(root_changed, "root page did not change after split");
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    for key in inserted_keys {
        assert_eq!(tree.get(&read, &key)?, Some(key + 100));
    }
    Ok(())
}

#[test]
fn delete_nonexistent_key_returns_false() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_delete_missing.db");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())?;

    pager.checkpoint(CheckpointMode::Force)?;
    let mut write = pager.begin_write()?;
    let removed = tree.delete(&mut write, &1234)?;
    pager.commit(write)?;
    assert!(!removed);
    pager.checkpoint(CheckpointMode::Force)?;
    Ok(())
}

#[test]
fn delete_existing_key_removes_entry() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_delete_basic.db");
    let pager = Arc::new(Pager::create(&path, PagerOptions::default())?);
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())?;

    pager.checkpoint(CheckpointMode::Force)?;
    {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &1, &111)?;
        tree.put(&mut write, &2, &222)?;
        tree.put(&mut write, &3, &333)?;
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;

    {
        let mut write = pager.begin_write()?;
        let removed = tree.delete(&mut write, &2)?;
        assert!(removed);
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    assert_eq!(tree.get(&read, &1)?, Some(111));
    assert_eq!(tree.get(&read, &2)?, None);
    assert_eq!(tree.get(&read, &3)?, Some(333));
    Ok(())
}

#[test]
fn delete_rebalances_via_left_sibling_borrow() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_delete_borrow_left.db");
    let mut pager_opts = PagerOptions::default();
    pager_opts.page_size = 256;
    let pager = Arc::new(Pager::create(&path, pager_opts)?);
    let store: Arc<dyn PageStore> = pager.clone();
    let mut tree_opts = BTreeOptions::default();
    tree_opts.prefix_compress = false;
    tree_opts.page_fill_target = 99;
    let tree = BTree::<u64, u64>::open_or_create(&store, tree_opts)?;

    let mut reference = BTreeMap::new();
    for key in 0u64..80 {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &key, &(key + 1_000))?;
        pager.commit(write)?;
        reference.insert(key, key + 1_000);
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let leaves = collect_leaf_snapshots(
        &pager,
        &read,
        tree.root_page(),
        tree.prefix_compression_enabled(),
    )?;
    let (left_info, right_info) = leaves
        .windows(2)
        .filter_map(|window| {
            let left = window[0].clone();
            let right = window[1].clone();
            if left.1.parent == right.1.parent
                && left.1.parent.is_some()
                && left.2.len() >= 3
                && right.2.len() >= 2
                && right.1.right_sibling.is_none()
            {
                Some((left, right))
            } else {
                None
            }
        })
        .next()
        .expect("expected adjacent leaves with common parent");
    let (left_id, left_header, _left_keys) = left_info;
    let (right_id, _right_header, right_keys) = right_info;
    let parent_id = left_header
        .parent
        .expect("selected leaves should have a parent");
    let parent_page = pager.get_page(&read, parent_id)?;
    let parent_slots_before = page::Header::parse(parent_page.data())?.slot_count;
    drop(parent_page);
    let mut observed_borrow = false;
    let mut current_right_len = right_keys.len();
    drop(read);

    for key in right_keys.iter() {
        let mut write = pager.begin_write()?;
        let removed = tree.delete(&mut write, key)?;
        pager.commit(write)?;
        if !removed {
            continue;
        }
        reference.remove(key);

        let read = pager.begin_read()?;
        let leaves_after = collect_leaf_snapshots(
            &pager,
            &read,
            tree.root_page(),
            tree.prefix_compression_enabled(),
        )?;
        leaves_after
            .iter()
            .find(|(id, _, _)| *id == left_id)
            .expect("left sibling should persist after borrow");
        let right_after = leaves_after
            .iter()
            .find(|(id, _, _)| *id == right_id)
            .expect("right sibling should persist after borrow");
        let parent_page_after = pager.get_page(&read, parent_id)?;
        let parent_header_after = page::Header::parse(parent_page_after.data())?;
        drop(parent_page_after);
        assert_eq!(
            parent_header_after.slot_count, parent_slots_before,
            "borrow should not change parent slot count",
        );
        if right_after.2.len() == current_right_len {
            observed_borrow = true;
        }
        current_right_len = right_after.2.len();
        drop(read);
    }

    assert!(
        observed_borrow,
        "expected delete to trigger borrow from left sibling"
    );

    pager.checkpoint(CheckpointMode::Force)?;
    assert_tree_matches_reference(&tree, &pager, &reference, 79)?;
    Ok(())
}

#[test]
fn delete_merges_with_left_sibling_when_borrow_forbidden() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_delete_merge_left.db");
    let mut pager_opts = PagerOptions::default();
    pager_opts.page_size = 512;
    let pager = Arc::new(Pager::create(&path, pager_opts)?);
    let store: Arc<dyn PageStore> = pager.clone();
    let mut tree_opts = BTreeOptions::default();
    tree_opts.prefix_compress = false;
    tree_opts.page_fill_target = 100;
    let tree = BTree::<u64, u64>::open_or_create(&store, tree_opts)?;

    let mut reference = BTreeMap::new();
    for key in 0u64..90 {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &key, &(key + 500))?;
        pager.commit(write)?;
        reference.insert(key, key + 500);
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let root_page = pager.get_page(&read, tree.root_page())?;
    let root_header = page::Header::parse(root_page.data())?;
    assert_eq!(root_header.kind, page::BTreePageKind::Internal);
    let initial_slots = root_header.slot_count;
    drop(root_page);

    let target_key = 89u64;
    let right_leaf_id =
        find_leaf_for_key(&pager, &read, tree.root_page(), &target_key.to_be_bytes())?;
    let right_page = pager.get_page(&read, right_leaf_id)?;
    let right_header = page::Header::parse(right_page.data())?;
    let left_id = right_header
        .left_sibling
        .expect("expected left sibling for merge");
    drop(right_page);

    let keys_to_remove = decode_leaf_keys(
        &pager,
        &read,
        right_leaf_id,
        tree.prefix_compression_enabled(),
    )?;
    assert!(
        !keys_to_remove.is_empty(),
        "target leaf should contain keys"
    );
    drop(read);

    for key in &keys_to_remove {
        let mut write = pager.begin_write()?;
        assert!(tree.delete(&mut write, key)?);
        pager.commit(write)?;
        reference.remove(key);
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let root_page = pager.get_page(&read, tree.root_page())?;
    let root_header = page::Header::parse(root_page.data())?;
    if root_header.kind == page::BTreePageKind::Internal {
        assert!(
            root_header.slot_count < initial_slots,
            "merge should reduce parent fan-out"
        );
    }
    drop(root_page);
    if let Some((&max_key, _)) = reference.iter().next_back() {
        let survivor_leaf =
            find_leaf_for_key(&pager, &read, tree.root_page(), &max_key.to_be_bytes())?;
        assert_eq!(
            survivor_leaf, left_id,
            "left sibling should retain merged contents"
        );
    }
    drop(read);

    assert_tree_matches_reference(&tree, &pager, &reference, 89)?;
    Ok(())
}

#[test]
fn delete_merges_leftmost_leaf_with_right_sibling() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_delete_merge_right.db");
    let mut pager_opts = PagerOptions::default();
    pager_opts.page_size = 512;
    let pager = Arc::new(Pager::create(&path, pager_opts)?);
    let store: Arc<dyn PageStore> = pager.clone();
    let mut tree_opts = BTreeOptions::default();
    tree_opts.prefix_compress = false;
    tree_opts.page_fill_target = 100;
    let tree = BTree::<u64, u64>::open_or_create(&store, tree_opts)?;

    let mut reference = BTreeMap::new();
    for key in 0u64..90 {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &key, &(key + 10_000))?;
        pager.commit(write)?;
        reference.insert(key, key + 10_000);
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let root_page = pager.get_page(&read, tree.root_page())?;
    let root_header = page::Header::parse(root_page.data())?;
    assert_eq!(root_header.kind, page::BTreePageKind::Internal);
    let initial_slots = root_header.slot_count;
    drop(root_page);

    let target_key = 0u64;
    let left_leaf_id =
        find_leaf_for_key(&pager, &read, tree.root_page(), &target_key.to_be_bytes())?;
    let left_page = pager.get_page(&read, left_leaf_id)?;
    let left_header = page::Header::parse(left_page.data())?;
    let _right_id = left_header
        .right_sibling
        .expect("leftmost leaf should have right sibling");
    drop(left_page);

    let keys_to_remove = decode_leaf_keys(
        &pager,
        &read,
        left_leaf_id,
        tree.prefix_compression_enabled(),
    )?;
    assert!(
        !keys_to_remove.is_empty(),
        "leftmost leaf should contain keys"
    );
    drop(read);

    for key in &keys_to_remove {
        let mut write = pager.begin_write()?;
        assert!(tree.delete(&mut write, key)?);
        pager.commit(write)?;
        reference.remove(key);
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    let root_page = pager.get_page(&read, tree.root_page())?;
    let root_header = page::Header::parse(root_page.data())?;
    if root_header.kind == page::BTreePageKind::Internal {
        assert!(
            root_header.slot_count < initial_slots,
            "merge should reduce parent fan-out"
        );
    }
    drop(root_page);

    if let Some((&min_key, _)) = reference.iter().next() {
        let survivor_leaf =
            find_leaf_for_key(&pager, &read, tree.root_page(), &min_key.to_be_bytes())?;
        assert_eq!(
            survivor_leaf, left_leaf_id,
            "current leaf should retain merged contents"
        );
    }
    drop(read);

    assert_tree_matches_reference(&tree, &pager, &reference, 89)?;
    Ok(())
}

#[test]
fn delete_sequence_preserves_map_equivalence() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_delete_sequence.db");
    let mut options = PagerOptions::default();
    options.page_size = 512;
    let pager = Arc::new(Pager::create(&path, options)?);
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())?;
    let mut reference = BTreeMap::new();

    pager.checkpoint(CheckpointMode::Force)?;
    for key in 0u64..40 {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &key, &(key + 1000))?;
        pager.commit(write)?;
        reference.insert(key, key + 1000);
    }
    pager.checkpoint(CheckpointMode::Force)?;

    for key in (0u64..40).step_by(2) {
        let mut write = pager.begin_write()?;
        let removed = match tree.delete(&mut write, &key) {
            Ok(value) => value,
            Err(err) => {
                panic!("delete failed for key {key}: {:?}", err);
            }
        };
        pager.commit(write)?;
        let expected_removed = reference.remove(&key).is_some();
        assert_eq!(removed, expected_removed);
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    for key in 0u64..40 {
        let expected = reference.get(&key).copied();
        assert_eq!(tree.get(&read, &key)?, expected);
    }
    Ok(())
}

#[test]
fn delete_collapse_root_to_leaf() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_delete_collapse.db");
    let mut options = PagerOptions::default();
    options.page_size = 512;
    let pager = Arc::new(Pager::create(&path, options)?);
    let store: Arc<dyn PageStore> = pager.clone();
    let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default())?;

    pager.checkpoint(CheckpointMode::Force)?;
    for key in 0u64..20 {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &key, &(key + 5))?;
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;

    for key in 0u64..20 {
        let mut write = pager.begin_write()?;
        let removed = tree.delete(&mut write, &key)?;
        assert!(removed);
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let read = pager.begin_read()?;
    for key in 0u64..20 {
        assert_eq!(tree.get(&read, &key)?, None);
    }
    let root_page = store.get_page(&read, tree.root_page())?;
    let header = page::Header::parse(root_page.data())?;
    assert_eq!(header.kind, page::BTreePageKind::Leaf);
    Ok(())
}

#[test]
fn cascading_splits_build_multi_level_tree() -> Result<()> {
    let dir = tempdir().map_err(SombraError::Io)?;
    let path = dir.path().join("btree_cascade.db");
    let mut pager_opts = PagerOptions::default();
    pager_opts.page_size = 512;
    let pager = Arc::new(Pager::create(&path, pager_opts)?);
    let store: Arc<dyn PageStore> = pager.clone();
    let mut tree_opts = BTreeOptions::default();
    tree_opts.prefix_compress = false;
    let tree = BTree::<u64, u64>::open_or_create(&store, tree_opts)?;
    let initial_root = tree.root_page();

    for key in 0u64..200 {
        let mut write = pager.begin_write()?;
        tree.put(&mut write, &key, &(key + 1_000))?;
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let new_root = tree.root_page();
    assert_ne!(
        new_root, initial_root,
        "root page should change after cascading splits"
    );

    let read = pager.begin_read()?;
    let root_page = pager.get_page(&read, new_root)?;
    let header = page::Header::parse(root_page.data())?;
    assert_eq!(header.kind, page::BTreePageKind::Internal);
    assert!(
        header.slot_count as usize >= 3,
        "expected root to have at least 3 children"
    );

    for key in 0u64..200 {
        assert_eq!(tree.get(&read, &key)?, Some(key + 1_000));
    }
    Ok(())
}

#[derive(Clone, Debug)]
enum PropOp {
    Put(u64, u64),
    Delete(u64),
}

fn op_strategy() -> impl Strategy<Value = PropOp> {
    let key_range = 0u64..64;
    let value_range = 0u64..512;
    prop_oneof![
        (key_range.clone(), value_range).prop_map(|(k, v)| PropOp::Put(k, v)),
        key_range.prop_map(PropOp::Delete),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(48))]
    #[test]
    fn btree_matches_btreemap_random_ops(ops in prop::collection::vec(op_strategy(), 1..20)) {
        let dir = tempdir().expect("tempdir");
        let path = dir.path().join("btree_prop.db");
        let pager = Arc::new(Pager::create(&path, PagerOptions::default()).expect("create pager"));
        let store: Arc<dyn PageStore> = pager.clone();
        let tree = BTree::<u64, u64>::open_or_create(&store, BTreeOptions::default()).expect("open tree");
        let mut reference = BTreeMap::new();

        for op in ops {
            match op {
                PropOp::Put(key, value) => {
                    let mut write = pager.begin_write().expect("begin write");
                    tree.put(&mut write, &key, &value).expect("put");
                    pager.commit(write).expect("commit");
                    reference.insert(key, value);
                }
                PropOp::Delete(key) => {
                    let expected = reference.remove(&key).is_some();
                    let mut write = pager.begin_write().expect("begin write");
                    let removed = tree.delete(&mut write, &key).expect("delete");
                    pager.commit(write).expect("commit");
                    prop_assert_eq!(removed, expected);
                }
            }
        }

        pager.checkpoint(CheckpointMode::Force).expect("checkpoint");
        let read = pager.begin_read().expect("begin read");
        let mut cursor = tree
            .range(&read, Bound::Unbounded, Bound::Unbounded)
            .expect("range");
        let mut actual = BTreeMap::new();
        while let Some((key, value)) = cursor.next().expect("cursor next") {
            actual.insert(key, value);
        }
        drop(read);
        prop_assert_eq!(actual, reference);
    }
}
