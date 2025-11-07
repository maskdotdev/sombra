use std::collections::HashSet;
use std::time::Duration;

use sombra_pager::{CheckpointMode, PageStore, Pager, PagerOptions, ReadGuard, Synchronous};
use sombra_types::page::{PageHeader, PageKind, PAGE_HDR_LEN};
use sombra_types::{PageId, Result};
use tempfile::tempdir;

fn init_page(
    pager: &Pager,
    write: &mut sombra_pager::WriteGuard<'_>,
    page: PageId,
    seed: &[u8],
) -> Result<()> {
    let meta = pager.meta()?;
    let mut frame = write.page_mut(page)?;
    let buf = frame.data_mut();
    let header =
        PageHeader::new(page, PageKind::BTreeLeaf, meta.page_size, meta.salt)?.with_crc32(0);
    header.encode(&mut buf[..PAGE_HDR_LEN])?;
    buf[PAGE_HDR_LEN..PAGE_HDR_LEN + seed.len()].copy_from_slice(seed);
    Ok(())
}

fn overwrite_payload(
    write: &mut sombra_pager::WriteGuard<'_>,
    page: PageId,
    data: &[u8],
) -> Result<()> {
    let mut frame = write.page_mut(page)?;
    let buf = frame.data_mut();
    buf[PAGE_HDR_LEN..PAGE_HDR_LEN + data.len()].copy_from_slice(data);
    Ok(())
}

fn read_payload(pager: &Pager, guard: &ReadGuard, page: PageId) -> Result<Vec<u8>> {
    let snapshot = pager.get_page(guard, page)?;
    Ok(snapshot.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4].to_vec())
}

#[test]
fn stage3_end_to_end_snapshot_and_recovery() -> Result<()> {
    let dir = tempdir().unwrap();
    let path = dir.path().join("stage3_e2e.db");
    let options = PagerOptions {
        synchronous: Synchronous::Normal,
        autocheckpoint_pages: 8,
        autocheckpoint_ms: Some(10),
        ..PagerOptions::default()
    };

    let pager = Pager::create(&path, options.clone())?;
    let mut pages = Vec::new();
    for i in 0..8u8 {
        let mut write = pager.begin_write()?;
        let page = write.allocate_page()?;
        init_page(&pager, &mut write, page, &[i; 4])?;
        pager.commit(write)?;
        pages.push(page);
    }
    pager.checkpoint(CheckpointMode::Force)?;

    let target = pages[0];
    let read_guard = pager.begin_read()?;
    assert_eq!(read_payload(&pager, &read_guard, target)?, vec![0; 4]);

    let mut write = pager.begin_write()?;
    overwrite_payload(&mut write, target, b"EDIT")?;
    let lsn = pager.commit(write)?;
    std::thread::sleep(Duration::from_millis(25));

    assert_eq!(read_payload(&pager, &read_guard, target)?, vec![0; 4]);
    drop(read_guard);

    let pre_checkpoint = pager.begin_read()?;
    assert_eq!(read_payload(&pager, &pre_checkpoint, target)?, vec![0; 4]);
    drop(pre_checkpoint);

    pager.checkpoint(CheckpointMode::Force)?;
    assert!(pager.last_checkpoint_lsn() >= lsn);

    let post_checkpoint = pager.begin_read()?;
    assert_eq!(read_payload(&pager, &post_checkpoint, target)?, b"EDIT");
    drop(post_checkpoint);

    for page in pages.iter().take(4) {
        let mut write = pager.begin_write()?;
        write.free_page(*page)?;
        pager.commit(write)?;
    }
    pager.checkpoint(CheckpointMode::Force)?;
    drop(pager);

    let pager = Pager::open(&path, options)?;
    let mut ids = Vec::new();
    for _ in 0..8 {
        let mut write = pager.begin_write()?;
        let page = write.allocate_page()?;
        init_page(&pager, &mut write, page, b"NEW!")?;
        pager.commit(write)?;
        ids.push(page);
    }
    let unique: HashSet<_> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "allocated pages should be unique");
    assert!(
        ids.iter().any(|p| pages.contains(p)),
        "should reuse freed pages"
    );
    Ok(())
}
