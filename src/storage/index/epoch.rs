use std::sync::atomic::{AtomicU64, Ordering};

use crate::primitives::pager::WriteGuard;
use crate::types::Result;

/// Monotonic epoch tracking catalog DDL changes for cache invalidation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub struct DdlEpoch(pub u64);

/// Thread-safe epoch mirror backed by the persisted catalog epoch in pager meta.
pub struct CatalogEpoch {
    value: AtomicU64,
}

impl CatalogEpoch {
    /// Creates a new epoch tracker seeded with the provided starting value.
    pub fn new(initial: DdlEpoch) -> Self {
        Self {
            value: AtomicU64::new(initial.0),
        }
    }

    /// Returns the current epoch value.
    pub fn current(&self) -> DdlEpoch {
        DdlEpoch(self.value.load(Ordering::SeqCst))
    }

    /// Resets the in-memory epoch to the provided value.
    pub fn sync_from_meta(&self, epoch: DdlEpoch) {
        self.value.store(epoch.0, Ordering::SeqCst);
    }

    /// Increments the persisted epoch inside the provided transaction and returns the new value.
    pub fn bump_in_txn(&self, tx: &mut WriteGuard<'_>) -> Result<DdlEpoch> {
        let mut next = None;
        tx.update_meta(|meta| {
            let candidate = meta.storage_ddl_epoch.saturating_add(1);
            meta.storage_ddl_epoch = candidate;
            next = Some(candidate);
        })?;
        let epoch = DdlEpoch(next.expect("catalog epoch must be recorded"));
        self.value.store(epoch.0, Ordering::SeqCst);
        Ok(epoch)
    }
}
