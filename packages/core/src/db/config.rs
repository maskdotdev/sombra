//! Database configuration options.
//!
//! This module provides configuration structures for controlling
//! Sombra's behavior, performance characteristics, and resource usage.
//!
//! # Configuration Presets
//!
//! - [`Config::production()`] - Optimized for production safety
//! - [`Config::balanced()`] - Balanced performance and durability
//! - [`Config::benchmark()`] - Maximum performance for testing
//! - [`Config::fully_durable()`] - Maximum durability guarantees
//!
//! # Example
//!
//! ```rust
//! use sombra::Config;
//!
//! // Use a preset configuration
//! let config = Config::production();
//!
//! // Or customize specific options
//! let mut config = Config::default();
//! config.page_cache_size = 20000;
//! ```

/// WAL synchronization modes controlling durability vs. performance trade-offs.
///
/// Different modes provide different guarantees about when data is safely
/// stored on disk versus kept in memory for better performance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Full synchronization after every write operation.
    ///
    /// Provides the highest durability but lowest performance.
    /// Every WAL write is immediately synced to disk using fsync.
    Full,

    /// Normal synchronization with periodic fsync.
    ///
    /// Balances durability and performance by syncing after a
    /// configurable number of operations.
    Normal,

    /// Sync only during checkpoints.
    ///
    /// Data is written to the WAL but not synced until a checkpoint
    /// occurs. Better performance but risk of losing recent writes.
    Checkpoint,

    /// Group commit mode for high throughput.
    ///
    /// Multiple transactions are batched together and synced as a group.
    /// Provides excellent throughput with reasonable durability.
    GroupCommit,

    /// No synchronization.
    ///
    /// Maximum performance but highest risk of data loss.
    /// Only suitable for testing or temporary data.
    Off,
}

/// Configuration options for Sombra database behavior.
///
/// Config controls performance, durability, and resource usage characteristics.
/// Use the provided presets (`production()`, `balanced()`, `benchmark()`)
/// or customize individual options.
///
/// # Example
///
/// ```rust
/// use sombra::{Config, SyncMode};
///
/// // Use a preset
/// let config = Config::production();
///
/// // Or customize
/// let mut config = Config::default();
/// config.wal_sync_mode = SyncMode::GroupCommit;
/// config.page_cache_size = 20000;
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    /// WAL synchronization mode controlling durability guarantees.
    pub wal_sync_mode: SyncMode,

    /// Number of operations between automatic syncs in Normal mode.
    pub sync_interval: usize,

    /// Number of WAL frames before triggering automatic checkpoint.
    pub checkpoint_threshold: usize,

    /// Number of pages to cache in memory for faster access.
    pub page_cache_size: usize,

    /// Timeout in milliseconds for group commit batching.
    pub group_commit_timeout_ms: u64,

    /// Whether to use memory-mapped I/O for file access.
    pub use_mmap: bool,

    /// Whether to enable page checksums for corruption detection.
    pub checksum_enabled: bool,

    /// Maximum database size in megabytes (None = unlimited).
    pub max_database_size_mb: Option<u64>,

    /// Maximum WAL size in megabytes before auto-checkpoint.
    pub max_wal_size_mb: u64,

    /// Maximum number of dirty pages a transaction can modify.
    pub max_transaction_pages: usize,

    /// Transaction timeout in milliseconds (None = no timeout).
    pub transaction_timeout_ms: Option<u64>,

    /// Auto-checkpoint interval in milliseconds (None = disabled).
    pub auto_checkpoint_interval_ms: Option<u64>,

    /// WAL size threshold for warning logs in megabytes.
    pub wal_size_warning_threshold_mb: u64,

    /// Optional override for Rayon thread pool size used by parallel traversals.
    pub rayon_thread_pool_size: Option<usize>,

    /// Minimum workload size before enabling parallel traversal algorithms.
    pub parallel_traversal_threshold: usize,

    /// Enable background compaction to reclaim disk space.
    pub enable_background_compaction: bool,

    /// Interval in seconds between compaction runs (None = disabled).
    pub compaction_interval_secs: Option<u64>,

    /// Minimum percentage of dead space in a page to trigger compaction (0-100).
    pub compaction_threshold_percent: u8,

    /// Maximum number of pages to compact in a single run.
    pub compaction_batch_size: usize,

    /// Enable Multi-Version Concurrency Control (MVCC) for transactions.
    ///
    /// When enabled, transactions use snapshot isolation with version chains.
    /// This allows multiple readers and writers to work concurrently without blocking.
    pub mvcc_enabled: bool,

    /// Maximum number of concurrent transactions allowed when MVCC is enabled.
    ///
    /// Only applies when MVCC is enabled. Controls how many transactions can
    /// be active simultaneously. None = use default (100).
    pub max_concurrent_transactions: Option<usize>,

    /// Interval in seconds between garbage collection runs (None = disabled).
    ///
    /// Only applies when MVCC is enabled. GC reclaims old versions that are
    /// no longer visible to any active transaction.
    pub gc_interval_secs: Option<u64>,
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
            rayon_thread_pool_size: None,
            parallel_traversal_threshold: 1024,
            enable_background_compaction: false,
            compaction_interval_secs: Some(300),
            compaction_threshold_percent: 50,
            compaction_batch_size: 100,
            mvcc_enabled: false,
            max_concurrent_transactions: None,
            gc_interval_secs: None,
        }
    }
}

impl Config {
    /// Creates a configuration optimized for production use.
    ///
    /// This configuration prioritizes data safety and reliability:
    /// - Group commit for good performance with durability
    /// - Checksums enabled for corruption detection
    /// - Reasonable timeouts and limits
    /// - Auto-checkpointing enabled
    ///
    /// # Returns
    /// A `Config` instance with production-safe settings.
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
            rayon_thread_pool_size: None,
            parallel_traversal_threshold: 2048,
            enable_background_compaction: true,
            compaction_interval_secs: Some(300),
            compaction_threshold_percent: 50,
            compaction_batch_size: 100,
            mvcc_enabled: false,
            max_concurrent_transactions: None,
            gc_interval_secs: None,
        }
    }

    /// Creates a configuration balancing performance and durability.
    ///
    /// This configuration provides a good middle ground:
    /// - Normal sync mode for periodic durability
    /// - Larger cache for better performance
    /// - Higher limits for larger workloads
    /// - Longer intervals for less frequent I/O
    ///
    /// # Returns
    /// A `Config` instance with balanced settings.
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
            rayon_thread_pool_size: None,
            parallel_traversal_threshold: 2048,
            enable_background_compaction: true,
            compaction_interval_secs: Some(600),
            compaction_threshold_percent: 40,
            compaction_batch_size: 200,
            mvcc_enabled: false,
            max_concurrent_transactions: None,
            gc_interval_secs: None,
        }
    }

    /// Creates a configuration optimized for benchmarking.
    ///
    /// This configuration maximizes performance at the cost of durability:
    /// - Group commit with minimal timeout
    /// - Checksums disabled for speed
    /// - Large cache sizes
    /// - No auto-checkpointing or timeouts
    ///
    /// **Warning**: Do not use this configuration for production data
    /// as it provides minimal durability guarantees.
    ///
    /// # Returns
    /// A `Config` instance with benchmark-optimized settings.
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
            rayon_thread_pool_size: None,
            parallel_traversal_threshold: 512,
            enable_background_compaction: false,
            compaction_interval_secs: None,
            compaction_threshold_percent: 50,
            compaction_batch_size: 100,
            mvcc_enabled: false,
            max_concurrent_transactions: None,
            gc_interval_secs: None,
        }
    }

    /// Creates a configuration with maximum durability guarantees.
    ///
    /// This configuration prioritizes data safety above all else:
    /// - Full sync mode for immediate durability
    /// - Checksums enabled
    /// - Conservative limits and timeouts
    /// - Regular auto-checkpointing
    ///
    /// Use this when data loss is unacceptable and performance
    /// is a secondary concern.
    ///
    /// # Returns
    /// A `Config` instance with maximum durability settings.
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
            rayon_thread_pool_size: None,
            parallel_traversal_threshold: 2048,
            enable_background_compaction: true,
            compaction_interval_secs: Some(180),
            compaction_threshold_percent: 60,
            compaction_batch_size: 50,
            mvcc_enabled: false,
            max_concurrent_transactions: None,
            gc_interval_secs: None,
        }
    }
}
