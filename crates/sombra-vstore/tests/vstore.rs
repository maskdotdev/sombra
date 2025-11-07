#![allow(clippy::all)]

use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

use rand::{rngs::StdRng, Rng, SeedableRng};
use sombra_pager::{CheckpointMode, PageStore, Pager, PagerOptions, Synchronous};
use sombra_types::{page::PAGE_HDR_LEN, Result, SombraError};
use sombra_vstore::VStore;

const PAGE_SIZE: u32 = 8192;
const OVERFLOW_HEADER_LEN: usize = 16;

fn create_pager(path: &std::path::Path) -> Result<Arc<Pager>> {
    let mut options = PagerOptions::default();
    options.page_size = PAGE_SIZE;
    options.synchronous = Synchronous::Full;
    let pager = Pager::create(path, options)?;
    Ok(Arc::new(pager))
}

#[test]
fn write_read_roundtrip_various_sizes() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("roundtrip.db");
    let pager = create_pager(&path)?;
    let store: Arc<dyn PageStore> = pager.clone();
    let vstore = VStore::open(store.clone())?;
    let sizes = [0usize, 1, 17, 4096, 8192, 12_345, 65_000];
    for size in sizes {
        let data: Vec<u8> = (0..size).map(|i| (i % 251) as u8).collect();
        let mut write = store.begin_write()?;
        let vref = vstore.write(&mut write, &data)?;
        let _ = store.commit(write)?;
        store.checkpoint(CheckpointMode::Force)?;

        let read = store.begin_read()?;
        let fetched = vstore.read(&read, vref)?;
        #[cfg(debug_assertions)]
        vstore.dump_vref(&read, vref)?;
        drop(read);
        assert_eq!(fetched, data, "roundtrip mismatch at size {}", size);

        let mut write = store.begin_write()?;
        vstore.free(&mut write, vref)?;
        let _ = store.commit(write)?;
        store.checkpoint(CheckpointMode::Force)?;
    }
    let snapshot = vstore.metrics_snapshot();
    assert_eq!(snapshot.live_pages(), 0);
    assert!(snapshot.bytes_read > 0);
    Ok(())
}

#[test]
fn update_in_place_and_reallocate() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("update.db");
    let pager = create_pager(&path)?;
    let store: Arc<dyn PageStore> = pager.clone();
    let vstore = VStore::open(store.clone())?;

    // Start with a buffer that spans multiple pages.
    let mut rng = StdRng::seed_from_u64(42);
    let make_payload =
        |len: usize, rng: &mut StdRng| -> Vec<u8> { (0..len).map(|_| rng.gen()).collect() };
    let initial = make_payload((PAGE_SIZE as usize * 2) + 123, &mut rng);
    let mut write = store.begin_write()?;
    let mut vref = vstore.write(&mut write, &initial)?;
    let _ = store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;

    let smaller = make_payload(128, &mut rng);
    let mut write = store.begin_write()?;
    vstore.update(&mut write, &mut vref, &smaller)?;
    let _ = store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;
    let read = store.begin_read()?;
    let fetched = vstore.read(&read, vref)?;
    drop(read);
    assert_eq!(fetched, smaller);

    let larger = make_payload(initial.len() * 3, &mut rng);
    let mut write = store.begin_write()?;
    vstore.update(&mut write, &mut vref, &larger)?;
    let _ = store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;
    let read = store.begin_read()?;
    let fetched = vstore.read(&read, vref)?;
    drop(read);
    assert_eq!(fetched, larger);
    let mut write = store.begin_write()?;
    vstore.free(&mut write, vref)?;
    let _ = store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;
    let snapshot = vstore.metrics_snapshot();
    assert_eq!(snapshot.live_pages(), 0);
    Ok(())
}

#[test]
fn checksum_detects_corruption() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("corrupt.db");
    let pager = create_pager(&path)?;
    let store: Arc<dyn PageStore> = pager.clone();
    let vstore = VStore::open(store.clone())?;

    let payload: Vec<u8> = (0..10_000).map(|i| (i % 251) as u8).collect();
    let mut write = store.begin_write()?;
    let vref = vstore.write(&mut write, &payload)?;
    let _ = store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;

    // Corrupt the first page payload.
    let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
    let offset =
        (vref.start_page.0 as usize * PAGE_SIZE as usize) + PAGE_HDR_LEN + OVERFLOW_HEADER_LEN;
    file.seek(SeekFrom::Start(offset as u64))?;
    file.write_all(&[0xFF])?;
    file.flush()?;

    let read = store.begin_read()?;
    let err = vstore
        .read(&read, vref)
        .expect_err("corruption should be detected");
    drop(read);
    match err {
        SombraError::Corruption(_) => {}
        other => panic!("expected corruption error, got {:?}", other),
    }
    let mut write = store.begin_write()?;
    vstore.free(&mut write, vref)?;
    let _ = store.commit(write)?;
    store.checkpoint(CheckpointMode::Force)?;
    let snapshot = vstore.metrics_snapshot();
    assert_eq!(snapshot.live_pages(), 0);
    Ok(())
}
