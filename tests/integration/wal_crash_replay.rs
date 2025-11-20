#![allow(missing_docs)]

use sombra::primitives::pager::{PageStore, Pager, PagerOptions, Synchronous};
use sombra::types::page::{PageHeader, PageKind, PAGE_HDR_LEN};
use sombra::types::Result;
use tempfile::tempdir;

#[test]
fn wal_replays_uncheckpointed_commit_after_crash() -> Result<()> {
    let dir = tempdir().expect("tmpdir");
    let path = dir.path().join("wal_replay.db");
    let mut options = PagerOptions::default();
    options.synchronous = Synchronous::Normal;

    let pager = Pager::create(&path, options.clone())?;
    let meta = pager.meta()?;
    let mut write = pager.begin_write()?;
    let page = write.allocate_page()?;
    let pattern = 0xABCD_1234_5566_7788u64.to_le_bytes();
    {
        let mut frame = write.page_mut(page)?;
        let buf = frame.data_mut();
        let header =
            PageHeader::new(page, PageKind::BTreeLeaf, meta.page_size, meta.salt)?.with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        let payload_start = PAGE_HDR_LEN;
        buf[payload_start..payload_start + pattern.len()].copy_from_slice(&pattern);
        buf[payload_start + pattern.len()..payload_start + pattern.len() + 8]
            .copy_from_slice(&[0x5A; 8]);
    }
    let committed_lsn = pager.commit(write)?;
    drop(pager); // simulate crash before checkpoint

    let reopened = Pager::open(&path, options)?;
    let read_guard = reopened.begin_read()?;
    let page_ref = reopened.get_page(&read_guard, page)?;
    let payload = &page_ref.data()[PAGE_HDR_LEN..];
    assert_eq!(&payload[..pattern.len()], &pattern);
    assert_eq!(&payload[pattern.len()..pattern.len() + 8], &[0x5A; 8]);
    drop(read_guard);

    let meta = reopened.meta()?;
    assert!(
        meta.last_checkpoint_lsn.0 >= committed_lsn.0,
        "recovery should advance checkpoint to the committed LSN"
    );
    Ok(())
}
