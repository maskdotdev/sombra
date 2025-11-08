#![allow(clippy::all)]

use std::sync::Arc;

use rand::{distributions::Alphanumeric, Rng};
use sombra::{
    primitives::pager::{CheckpointMode, PageStore, Pager, PagerOptions, Synchronous},
    storage::catalog::{Dict, DictOptions},
    types::Result,
};

fn create_pager(path: &std::path::Path) -> Result<Arc<Pager>> {
    let mut opts = PagerOptions::default();
    opts.page_size = 8192;
    opts.synchronous = Synchronous::Full;
    let pager = Pager::create(path, opts)?;
    Ok(Arc::new(pager))
}

fn random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .map(char::from)
        .take(len)
        .collect()
}

#[test]
fn intern_and_resolve_roundtrip() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("dict.db");
    let pager = create_pager(&path)?;
    let store: Arc<dyn PageStore> = pager.clone();
    let dict = Dict::open(store.clone(), DictOptions::default())?;

    let long_value = "x".repeat(512);

    let mut write = store.begin_write()?;
    let id_inline = dict.intern(&mut write, "alpha")?;
    let id_long = dict.intern(&mut write, &long_value)?;
    let repeat = dict.intern(&mut write, "alpha")?;
    assert_eq!(id_inline, repeat, "intern should deduplicate strings");
    store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;

    let read = store.begin_read()?;
    assert_eq!(dict.resolve(&read, id_inline)?, "alpha");
    assert_eq!(dict.resolve(&read, id_long)?, long_value);
    drop(read);
    let snapshot = dict.metrics_snapshot();
    assert_eq!(snapshot.intern_calls, 3);
    assert_eq!(snapshot.intern_hits, 1);
    assert_eq!(snapshot.intern_misses, 2);
    assert_eq!(snapshot.resolve_calls, 2);
    assert_eq!(snapshot.resolve_misses, 0);
    Ok(())
}

#[test]
fn ids_are_monotonic_and_persist() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("dict_ids.db");
    let pager = create_pager(&path)?;
    let store: Arc<dyn PageStore> = pager.clone();
    let dict = Dict::open(store.clone(), DictOptions::default())?;

    let mut write = store.begin_write()?;
    let mut ids = Vec::new();
    for i in 0..50 {
        let key = format!("key-{i}");
        ids.push(dict.intern(&mut write, &key)?);
    }
    store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;
    assert_eq!(ids.len(), 50);
    for (idx, id) in ids.iter().enumerate() {
        assert_eq!(id.0, (idx as u32) + 1);
    }
    let first_metrics = dict.metrics_snapshot();
    assert_eq!(first_metrics.intern_calls, 50);
    assert_eq!(first_metrics.intern_hits, 0);
    assert_eq!(first_metrics.intern_misses, 50);
    assert_eq!(first_metrics.resolve_calls, 0);

    drop(dict);

    let dict_reopen = Dict::open(store.clone(), DictOptions::default())?;
    let mut write = store.begin_write()?;
    let next_id = dict_reopen.intern(&mut write, "after-reopen")?;
    store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;
    assert_eq!(next_id.0, ids.len() as u32 + 1);

    let read = store.begin_read()?;
    assert_eq!(dict_reopen.resolve(&read, next_id)?, "after-reopen");
    drop(read);
    let reopen_metrics = dict_reopen.metrics_snapshot();
    assert_eq!(reopen_metrics.intern_calls, 1);
    assert_eq!(reopen_metrics.intern_misses, 1);
    assert_eq!(reopen_metrics.intern_hits, 0);
    assert_eq!(reopen_metrics.resolve_calls, 1);
    assert_eq!(reopen_metrics.resolve_misses, 0);
    Ok(())
}

#[test]
fn resolves_many_entries() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("dict_many.db");
    let pager = create_pager(&path)?;
    let store: Arc<dyn PageStore> = pager.clone();
    let dict = Dict::open(store.clone(), DictOptions::default())?;

    let entries: Vec<String> = (0..125)
        .map(|i| {
            if i % 3 == 0 {
                random_string(300)
            } else {
                format!("short-{i}")
            }
        })
        .collect();

    let mut write = store.begin_write()?;
    let mut ids = Vec::new();
    for s in &entries {
        ids.push(dict.intern(&mut write, s)?);
    }
    store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;

    let read = store.begin_read()?;
    for (expected, id) in entries.iter().zip(ids.iter()) {
        assert_eq!(dict.resolve(&read, *id)?, *expected);
    }
    drop(read);
    let snapshot = dict.metrics_snapshot();
    assert_eq!(snapshot.intern_calls, entries.len() as u64);
    assert_eq!(snapshot.intern_hits, 0);
    assert_eq!(snapshot.intern_misses, entries.len() as u64);
    assert_eq!(snapshot.resolve_calls, entries.len() as u64);
    assert_eq!(snapshot.resolve_misses, 0);
    Ok(())
}
