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
    pub checksum_enabled: bool,
    pub max_database_size_mb: Option<u64>,
    pub max_wal_size_mb: u64,
    pub max_transaction_pages: usize,
    pub transaction_timeout_ms: Option<u64>,
    pub auto_checkpoint_interval_ms: Option<u64>,
    pub wal_size_warning_threshold_mb: u64,
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
            checksum_enabled: true,
            max_database_size_mb: None,
            max_wal_size_mb: 100,
            max_transaction_pages: 10000,
            transaction_timeout_ms: None,
            auto_checkpoint_interval_ms: Some(30000),
            wal_size_warning_threshold_mb: 80,
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
            checksum_enabled: true,
            max_database_size_mb: None,
            max_wal_size_mb: 100,
            max_transaction_pages: 10000,
            transaction_timeout_ms: Some(300000),
            auto_checkpoint_interval_ms: Some(30000),
            wal_size_warning_threshold_mb: 80,
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
            checksum_enabled: true,
            max_database_size_mb: None,
            max_wal_size_mb: 200,
            max_transaction_pages: 20000,
            transaction_timeout_ms: Some(600000),
            auto_checkpoint_interval_ms: Some(60000),
            wal_size_warning_threshold_mb: 160,
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
            checksum_enabled: false,
            max_database_size_mb: None,
            max_wal_size_mb: 500,
            max_transaction_pages: 50000,
            transaction_timeout_ms: None,
            auto_checkpoint_interval_ms: None,
            wal_size_warning_threshold_mb: 400,
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
            checksum_enabled: true,
            max_database_size_mb: None,
            max_wal_size_mb: 100,
            max_transaction_pages: 10000,
            transaction_timeout_ms: Some(300000),
            auto_checkpoint_interval_ms: Some(30000),
            wal_size_warning_threshold_mb: 80,
        }
    }
}
