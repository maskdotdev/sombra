#![allow(missing_docs)]

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use sombra::primitives::pager::{PageStore, Pager, PagerOptions, Synchronous};
use sombra::primitives::wal::{
    set_wal_batch_delay_ms_for_tests, Wal, WalCommitConfig, WalCommitter, WalFrameOwned,
    WalOptions, WalSyncMode,
};
use sombra::types::{Lsn, PageId, Result};
use tempfile::tempdir;

#[test]
fn wal_group_commit_backlog_surfaces_pending_work() -> Result<()> {
    let dir = tempdir()?;
    let wal_path = dir.path().join("group_backlog");
    let wal = Arc::new(Wal::open(&wal_path, WalOptions::new(512, 7, Lsn(1)))?);
    let config = WalCommitConfig {
        max_batch_commits: 16,
        max_batch_frames: 128,
        max_batch_wait: Duration::from_millis(10),
    };
    let committer = Arc::new(WalCommitter::new(Arc::clone(&wal), config));

    // Delay apply_batch to let the backlog build up before frames are flushed.
    set_wal_batch_delay_ms_for_tests(50);
    let mut handles = Vec::new();
    for i in 0..4 {
        let committer = Arc::clone(&committer);
        handles.push(thread::spawn(move || -> Result<()> {
            let frame = WalFrameOwned {
                lsn: Lsn(i + 1),
                page_id: PageId(i + 1),
                payload: vec![0u8; 512],
            };
            let _ = committer.commit(vec![frame], WalSyncMode::Deferred)?;
            Ok(())
        }));
    }

    let deadline = Instant::now() + Duration::from_secs(1);
    let mut observed = None;
    while Instant::now() < deadline {
        let backlog = committer.backlog();
        if backlog.pending_commits >= 2 {
            observed = Some(backlog);
            break;
        }
        thread::sleep(Duration::from_millis(5));
    }
    set_wal_batch_delay_ms_for_tests(0);
    for handle in handles {
        handle.join().expect("thread join").unwrap()?;
    }
    let backlog = observed.expect("expected backlog to appear");
    assert!(
        backlog.pending_commits >= 2,
        "should have seen multiple pending commits, got {backlog:?}"
    );
    Ok(())
}

#[test]
fn async_fsync_coalesce_creates_and_drains_backlog() -> Result<()> {
    let dir = tempdir()?;
    let path = dir.path().join("async_fsync.db");
    let mut opts = PagerOptions::default();
    opts.synchronous = Synchronous::Full;
    opts.async_fsync = true;
    opts.async_fsync_max_wait_ms = 200;

    let pager = Pager::create(&path, opts)?;
    let mut write = pager.begin_write()?;
    let page = write.allocate_page()?;
    {
        let mut frame = write.page_mut(page)?;
        frame.data_mut()[0..4].copy_from_slice(b"ping");
    }
    pager.commit(write)?;

    // Backlog should be visible before coalesce delay elapses.
    let backlog = pager
        .async_fsync_backlog()
        .expect("async fsync backlog available");
    assert!(
        backlog.pending_lag > 0,
        "expected pending lag to be non-zero, got {backlog:?}"
    );

    thread::sleep(Duration::from_millis(350));
    let backlog_after = pager.async_fsync_backlog().unwrap();
    assert_eq!(
        backlog_after.pending_lag, 0,
        "pending lag should drain after coalesce window"
    );
    Ok(())
}
