#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    Full,
    Normal,
    Checkpoint,
    GroupCommit,
    Off,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub wal_sync_mode: SyncMode,
    pub sync_interval: usize,
    pub checkpoint_threshold: usize,
    pub page_cache_size: usize,
    pub group_commit_timeout_ms: u64,
    pub use_mmap: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            wal_sync_mode: SyncMode::Full,
            sync_interval: 1,
            checkpoint_threshold: 1000,
            page_cache_size: 10000,
            group_commit_timeout_ms: 1,
            use_mmap: true,
        }
    }
}

impl Config {
    pub fn production() -> Self {
        Self {
            wal_sync_mode: SyncMode::GroupCommit,
            sync_interval: 1,
            checkpoint_threshold: 1000,
            page_cache_size: 10000,
            group_commit_timeout_ms: 1,
            use_mmap: true,
        }
    }

    pub fn balanced() -> Self {
        Self {
            wal_sync_mode: SyncMode::Normal,
            sync_interval: 100,
            checkpoint_threshold: 5000,
            page_cache_size: 20000,
            group_commit_timeout_ms: 10,
            use_mmap: true,
        }
    }

    pub fn benchmark() -> Self {
        Self {
            wal_sync_mode: SyncMode::GroupCommit,
            sync_interval: 1,
            checkpoint_threshold: 10000,
            page_cache_size: 50000,
            group_commit_timeout_ms: 1,
            use_mmap: true,
        }
    }

    pub fn fully_durable() -> Self {
        Self {
            wal_sync_mode: SyncMode::Full,
            sync_interval: 1,
            checkpoint_threshold: 1000,
            page_cache_size: 10000,
            group_commit_timeout_ms: 10,
            use_mmap: true,
        }
    }
}
