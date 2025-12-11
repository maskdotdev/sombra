use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use crate::primitives::pager::{PageStore, ReadGuard};
use crate::storage::metrics;
use crate::types::Result;

use super::graph_types::SnapshotPoolStatus;

pub(crate) struct PooledSnapshot {
    guard: ReadGuard,
    acquired_at: std::time::Instant,
}

pub(crate) struct SnapshotPool {
    store: Arc<dyn PageStore>,
    metrics: Arc<dyn metrics::StorageMetrics>,
    retention: Duration,
    inner: Mutex<Vec<PooledSnapshot>>,
    capacity: usize,
}

impl SnapshotPool {
    pub(crate) fn new(
        store: Arc<dyn PageStore>,
        metrics: Arc<dyn metrics::StorageMetrics>,
        capacity: usize,
        retention: Duration,
    ) -> Self {
        Self {
            store,
            metrics,
            retention,
            inner: Mutex::new(Vec::new()),
            capacity: capacity.max(1),
        }
    }

    pub(crate) fn lease(&self) -> Result<SnapshotLease<'_>> {
        let now = std::time::Instant::now();
        let durable = self.store.durable_lsn().map(|lsn| lsn.0).unwrap_or(0);
        let mut pool = self.inner.lock();
        while let Some(snapshot) = pool.pop() {
            let age = now.saturating_duration_since(snapshot.acquired_at);
            if age > self.retention {
                continue;
            }
            if durable > snapshot.guard.snapshot_lsn().0 {
                continue;
            }
            self.metrics.snapshot_pool_hit();
            return Ok(SnapshotLease {
                pool: Some(self),
                snapshot: Some(snapshot),
            });
        }
        drop(pool);
        self.metrics.snapshot_pool_miss();
        let guard = self.store.begin_read()?;
        Ok(SnapshotLease {
            pool: Some(self),
            snapshot: Some(PooledSnapshot {
                guard,
                acquired_at: now,
            }),
        })
    }

    pub(crate) fn return_snapshot(&self, snapshot: PooledSnapshot) {
        let now = std::time::Instant::now();
        let durable = self.store.durable_lsn().map(|lsn| lsn.0).unwrap_or(0);
        if now.saturating_duration_since(snapshot.acquired_at) > self.retention {
            return;
        }
        if durable > snapshot.guard.snapshot_lsn().0 {
            return;
        }
        let mut pool = self.inner.lock();
        if pool.len() >= self.capacity {
            return;
        }
        pool.push(snapshot);
    }

    pub(crate) fn status(&self) -> SnapshotPoolStatus {
        let pool = self.inner.lock();
        SnapshotPoolStatus {
            capacity: self.capacity,
            available: pool.len(),
        }
    }
}

pub(crate) struct SnapshotLease<'a> {
    pool: Option<&'a SnapshotPool>,
    snapshot: Option<PooledSnapshot>,
}

impl<'a> SnapshotLease<'a> {
    pub(crate) fn inner(&self) -> &ReadGuard {
        &self.snapshot.as_ref().expect("snapshot present").guard
    }

    pub(crate) fn direct(guard: ReadGuard) -> Self {
        Self {
            pool: None,
            snapshot: Some(PooledSnapshot {
                guard,
                acquired_at: std::time::Instant::now(),
            }),
        }
    }
}

impl<'a> std::ops::Deref for SnapshotLease<'a> {
    type Target = ReadGuard;

    fn deref(&self) -> &Self::Target {
        self.inner()
    }
}

impl<'a> Drop for SnapshotLease<'a> {
    fn drop(&mut self) {
        if let (Some(pool), Some(snapshot)) = (self.pool, self.snapshot.take()) {
            pool.return_snapshot(snapshot);
        }
    }
}
