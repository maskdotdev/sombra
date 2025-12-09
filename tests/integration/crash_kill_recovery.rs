#![allow(missing_docs)]

use std::process::{abort, Command};

use sombra::primitives::pager::{PageStore, Pager, PagerOptions, Synchronous};
use sombra::types::page::{PageHeader, PageKind, PAGE_HDR_LEN};
use sombra::types::{PageId, Result};
use tempfile::tempdir;

#[test]
fn kill_during_commit_recovers() -> Result<()> {
    let dir = tempdir()?;
    let db_path = dir.path().join("crash-harness.db");

    // Spawn the same test binary in a special mode that will write once and then SIGKILL itself.
    let child_status = Command::new(std::env::current_exe()?)
        .env("SOMBRA_CRASH_DB_PATH", &db_path)
        .arg("--nocapture")
        .arg("--ignored")
        .arg("crash_child_kill_after_commit")
        .status()
        .expect("spawn child");

    assert!(!child_status.success(), "child should abort");

    // Recovery: open the database and verify the page contents written by the child.
    let opts = PagerOptions {
        synchronous: Synchronous::Normal,
        ..PagerOptions::default()
    };
    let pager = Pager::open(&db_path, opts)?;
    let read_guard = pager.begin_read()?;
    let page_ref = pager.get_page(&read_guard, PageId(1))?;
    let payload = &page_ref.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 8];
    assert_eq!(payload, &[0xC0, 0xFF, 0xEE, 0x00, 0xBA, 0xBE, 0x00, 0x01]);
    Ok(())
}

#[test]
#[ignore]
fn crash_child_kill_after_commit() -> Result<()> {
    let path = std::env::var("SOMBRA_CRASH_DB_PATH").expect("missing SOMBRA_CRASH_DB_PATH");
    let db_path = std::path::PathBuf::from(path);
    let opts = PagerOptions {
        synchronous: Synchronous::Normal,
        ..PagerOptions::default()
    };
    let pager = Pager::create(&db_path, opts)?;
    let meta = pager.meta()?;
    let mut write = pager.begin_write()?;
    let page = write.allocate_page()?;
    {
        let mut frame = write.page_mut(page)?;
        let buf = frame.data_mut();
        let header =
            PageHeader::new(page, PageKind::BTreeLeaf, meta.page_size, meta.salt)?.with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        buf[PAGE_HDR_LEN..PAGE_HDR_LEN + 8]
            .copy_from_slice(&[0xC0, 0xFF, 0xEE, 0x00, 0xBA, 0xBE, 0x00, 0x01]);
    }
    pager.commit(write)?;

    // Simulate an abrupt crash before any checkpoint/cleanup.
    abort();
}
