use std::sync::Arc;

use rustc_hash::FxHashMap;

use crate::types::{LabelId, Result};

use super::epoch::DdlEpoch;
use super::types::IndexDef;

/// Cache hit/miss counters aggregated per transaction.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct GraphIndexCacheStats {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
}

/// Transaction-local cache that stores property index definitions per label.
pub struct GraphIndexCache {
    epoch: DdlEpoch,
    entries: FxHashMap<LabelId, Arc<Vec<IndexDef>>>,
    stats: GraphIndexCacheStats,
}

impl GraphIndexCache {
    /// Creates a new cache seeded with the provided epoch.
    pub fn new(epoch: DdlEpoch) -> Self {
        Self {
            epoch,
            entries: FxHashMap::default(),
            stats: GraphIndexCacheStats::default(),
        }
    }

    /// Resets the cache if the observed epoch differs from the tracked epoch.
    pub fn sync_epoch(&mut self, epoch: DdlEpoch) {
        if self.epoch != epoch {
            self.epoch = epoch;
            self.entries.clear();
            self.stats = GraphIndexCacheStats::default();
        }
    }

    /// Clears all cached entries without modifying the epoch.
    pub fn clear(&mut self) {
        self.entries.clear();
        self.stats = GraphIndexCacheStats::default();
    }

    /// Records current stats and resets the internal counters.
    pub fn take_stats(&mut self) -> GraphIndexCacheStats {
        let stats = self.stats;
        self.stats = GraphIndexCacheStats::default();
        stats
    }

    /// Retrieves cached definitions for `label`, loading them via `loader` on miss.
    pub fn get_or_load<F>(&mut self, label: LabelId, loader: F) -> Result<Arc<Vec<IndexDef>>>
    where
        F: FnOnce(LabelId) -> Result<Vec<IndexDef>>,
    {
        if let Some(entry) = self.entries.get(&label) {
            self.stats.hits += 1;
            return Ok(Arc::clone(entry));
        }
        let defs = loader(label)?;
        let arc = Arc::new(defs);
        self.entries.insert(label, Arc::clone(&arc));
        self.stats.misses += 1;
        Ok(arc)
    }
}
