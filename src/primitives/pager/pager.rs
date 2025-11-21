use std::any::{Any, TypeId};
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{ErrorKind, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Weak};
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{
    lock_api::{ArcRwLockReadGuard, ArcRwLockWriteGuard},
    Mutex, RawRwLock,
};

use super::frame::{Frame, FrameState};
use super::freelist::{free_page_capacity, read_free_page, write_free_page, Extent, FreeCache};
use super::meta::{create_meta, load_meta, write_meta_page, Meta};
use crate::primitives::{
    concurrency::{ReaderGuard as LockReaderGuard, SingleWriter, WriterGuard as LockWriterGuard},
    io::{FileIo, StdFileIo},
    wal::{
        Wal, WalAllocatorStats, WalCommitBacklog, WalCommitConfig, WalCommitter, WalFrame,
        WalFrameOwned, WalFramePtr, WalOptions, WalSyncMode,
    },
};
use crate::storage::{
    profile_scope, record_pager_commit_borrowed_bytes, record_pager_wal_bytes,
    record_pager_wal_frames, CommitId, CommitReader, CommitTable, IntentId, ReaderSnapshot,
    StorageProfileKind,
};
use crate::types::{
    page::{self, PageHeader, PAGE_HDR_LEN},
    page_crc32, Lsn, PageId, Result, SombraError,
};
use tracing::{debug, info, warn};

#[cfg(test)]
macro_rules! pager_test_log {
    ($($arg:tt)*) => {
        eprintln!($($arg)*);
    };
}

#[cfg(not(test))]
macro_rules! pager_test_log {
    ($($arg:tt)*) => {
        if false {
            let _ = format_args!($($arg)*);
        }
    };
}

/// Configuration options for the pager.
///
/// These options control page size, caching behavior, durability guarantees,
/// and automatic checkpoint triggers.
#[derive(Clone, Debug)]
pub struct PagerOptions {
    /// Size of each page in bytes (e.g., 4096).
    pub page_size: u32,
    /// Number of pages to cache in memory.
    pub cache_pages: usize,
    /// Whether to prefetch adjacent pages on cache miss.
    pub prefetch_on_miss: bool,
    /// Durability mode for write-ahead log synchronization.
    pub synchronous: Synchronous,
    /// Number of WAL pages before triggering automatic checkpoint.
    pub autocheckpoint_pages: usize,
    /// Time interval in milliseconds before triggering automatic checkpoint.
    pub autocheckpoint_ms: Option<u64>,
    /// Maximum number of writers to batch inside the WAL committer.
    pub group_commit_max_writers: usize,
    /// Maximum number of frames to batch per WAL write group.
    pub group_commit_max_frames: usize,
    /// Maximum time in milliseconds to wait for extra writers before flushing.
    pub group_commit_max_wait_ms: u64,
    /// Whether commits should return before fsync completes.
    pub async_fsync: bool,
    /// Maximum time to defer async fsync to coalesce multiple commits.
    pub async_fsync_max_wait_ms: u64,
    /// Preferred WAL segment size when preallocation is enabled.
    pub wal_segment_size_bytes: u64,
    /// Number of WAL segments to preallocate ahead of time.
    pub wal_preallocate_segments: u32,
}

struct PendingWalFrame {
    lsn: Lsn,
    page_id: PageId,
    payload: PendingPayload,
}

enum PendingPayload {
    Owned(Vec<u8>),
    Borrowed(PageImageLease),
}

impl PendingPayload {
    fn is_borrowed(&self) -> bool {
        matches!(self, PendingPayload::Borrowed(_))
    }

    fn as_slice(&self) -> &[u8] {
        match self {
            PendingPayload::Owned(buf) => buf.as_slice(),
            PendingPayload::Borrowed(lease) => lease.as_slice(),
        }
    }

    fn into_owned(self) -> Vec<u8> {
        match self {
            PendingPayload::Owned(buf) => buf,
            PendingPayload::Borrowed(lease) => lease.into_vec(),
        }
    }
}

struct PageImageLease {
    guard: ArcRwLockReadGuard<RawRwLock, Box<[u8]>>,
}

impl PageImageLease {
    fn as_slice(&self) -> &[u8] {
        &self.guard
    }

    fn into_vec(self) -> Vec<u8> {
        self.guard.to_vec()
    }
}

struct OverlayEntry {
    lsn: Lsn,
    data: Arc<[u8]>,
}

struct VersionChainEntry {
    lsn: Lsn,
    wal_offset: Option<WalFramePtr>,
    data: Option<Arc<[u8]>>,
}

struct ReaderMetrics {
    active: AtomicU64,
    begin_total: AtomicU64,
    end_total: AtomicU64,
}

impl ReaderMetrics {
    fn new() -> Self {
        Self {
            active: AtomicU64::new(0),
            begin_total: AtomicU64::new(0),
            end_total: AtomicU64::new(0),
        }
    }

    fn on_begin(&self) {
        self.active.fetch_add(1, AtomicOrdering::Relaxed);
        self.begin_total.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn on_end(&self) {
        self.active.fetch_sub(1, AtomicOrdering::Relaxed);
        self.end_total.fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn snapshot(&self) -> (u64, u64, u64) {
        (
            self.active.load(AtomicOrdering::Relaxed),
            self.begin_total.load(AtomicOrdering::Relaxed),
            self.end_total.load(AtomicOrdering::Relaxed),
        )
    }
}

#[derive(Clone)]
struct ReaderMetricsHandle {
    metrics: Arc<ReaderMetrics>,
}

impl ReaderMetricsHandle {
    fn new(metrics: Arc<ReaderMetrics>) -> Self {
        metrics.on_begin();
        Self { metrics }
    }
}

impl Drop for ReaderMetricsHandle {
    fn drop(&mut self) {
        self.metrics.on_end();
    }
}

impl Default for PagerOptions {
    /// Creates default pager options with sensible values.
    fn default() -> Self {
        Self {
            page_size: page::DEFAULT_PAGE_SIZE,
            cache_pages: 128,
            prefetch_on_miss: false,
            synchronous: Synchronous::Full,
            autocheckpoint_pages: 1024,
            autocheckpoint_ms: None,
            group_commit_max_writers: 32,
            group_commit_max_frames: 512,
            group_commit_max_wait_ms: 2,
            async_fsync: false,
            async_fsync_max_wait_ms: 0,
            wal_segment_size_bytes: 64 * 1024 * 1024,
            wal_preallocate_segments: 0,
        }
    }
}

/// Durability mode for write-ahead log synchronization.
///
/// Controls when the WAL is synchronized to disk, trading performance for durability.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
pub enum Synchronous {
    /// Sync to disk immediately after each commit (most durable).
    #[default]
    Full,
    /// Batch syncs with short delay (balanced).
    Normal,
    /// No explicit syncs (fastest but least durable).
    Off,
}

impl Synchronous {
    /// Returns the string representation of the synchronous mode.
    pub fn as_str(self) -> &'static str {
        match self {
            Synchronous::Full => "full",
            Synchronous::Normal => "normal",
            Synchronous::Off => "off",
        }
    }

    /// Parses a synchronous mode from a string (case-insensitive).
    pub fn from_str(value: &str) -> Option<Self> {
        match value.to_ascii_lowercase().as_str() {
            "full" => Some(Synchronous::Full),
            "normal" => Some(Synchronous::Normal),
            "off" => Some(Synchronous::Off),
            _ => None,
        }
    }
}

/// Mode for checkpoint execution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CheckpointMode {
    /// Block until checkpoint completes.
    Force,
    /// Attempt checkpoint without blocking if lock unavailable.
    BestEffort,
}

/// Desired read snapshot semantics.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ReadConsistency {
    /// Only observe data durable in the last checkpoint.
    Checkpoint,
    /// Observe the latest committed pages, replaying WAL if needed.
    LatestCommitted,
}

fn wal_path(path: &Path) -> PathBuf {
    append_suffix(path, "-wal")
}

fn wal_cookie_path(path: &Path) -> PathBuf {
    let mut dir = wal_path(path);
    dir.push("wal.dwm");
    dir
}

fn lock_path(path: &Path) -> PathBuf {
    append_suffix(path, "-lock")
}

fn append_suffix(path: &Path, suffix: &str) -> PathBuf {
    let mut name = path
        .file_name()
        .map(OsString::from)
        .unwrap_or_else(|| OsString::from("sombra"));
    name.push(suffix);
    let mut new_path = path.to_path_buf();
    new_path.set_file_name(name);
    new_path
}

#[derive(Debug, Clone)]
struct WalDurableCookie {
    path: PathBuf,
}

impl WalDurableCookie {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }

    fn read(&self) -> Result<Option<Lsn>> {
        match File::open(&self.path) {
            Ok(mut file) => {
                let mut buf = [0u8; 8];
                file.read_exact(&mut buf)?;
                Ok(Some(Lsn(u64::from_be_bytes(buf))))
            }
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(SombraError::from(err)),
        }
    }

    fn persist(&self, lsn: Lsn) -> Result<()> {
        let mut tmp = self.path.clone();
        tmp.set_extension("tmp");
        if let Some(parent) = tmp.parent() {
            fs::create_dir_all(parent)?;
        }
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)?;
            file.write_all(&lsn.0.to_be_bytes())?;
            file.sync_all()?;
        }
        fs::rename(&tmp, &self.path)?;
        Ok(())
    }
}

fn recover_database(
    wal: &Wal,
    db_io: &dyn FileIo,
    meta: &mut Meta,
    page_size: usize,
    replay_limit: Option<Lsn>,
) -> Result<Lsn> {
    let mut iter = wal.iter()?;
    let mut frames = Vec::new();
    let mut max_lsn = meta.last_checkpoint_lsn;
    while let Some(frame) = iter.next_frame()? {
        if let Some(limit) = replay_limit {
            if frame.lsn.0 > limit.0 {
                break;
            }
        }
        if frame.payload.as_slice().len() != page_size {
            return Err(SombraError::Corruption("wal frame payload length mismatch"));
        }
        if frame.lsn.0 <= meta.last_checkpoint_lsn.0 {
            continue;
        }
        if frame.lsn.0 > max_lsn.0 {
            max_lsn = frame.lsn;
        }
        frames.push(frame);
    }
    if frames.is_empty() {
        let _ = wal.recycle_active_segments()?;
        wal.reset(Lsn(meta.last_checkpoint_lsn.0 + 1))?;
        return Ok(Lsn(meta.last_checkpoint_lsn.0 + 1));
    }
    for frame in &frames {
        let offset = page_offset(frame.page_id, page_size);
        db_io.write_at(offset, frame.payload.as_slice())?;
    }
    db_io.sync_all()?;
    meta.last_checkpoint_lsn = max_lsn;
    let mut meta_buf = vec![0u8; page_size];
    write_meta_page(&mut meta_buf, meta)?;
    db_io.write_at(0, &meta_buf)?;
    db_io.sync_all()?;
    let refreshed = load_meta(db_io, meta.page_size)?;
    *meta = refreshed;
    let _ = wal.recycle_active_segments()?;
    wal.reset(Lsn(meta.last_checkpoint_lsn.0 + 1))?;
    Ok(Lsn(meta.last_checkpoint_lsn.0 + 1))
}

/// Reader age threshold (ms) that triggers MVCC lag warnings.
pub const MVCC_READER_WARN_THRESHOLD_MS: u64 = 600_000;

/// Statistics tracking pager operations.
#[derive(Default, Clone, Debug)]
pub struct PagerStats {
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Number of page evictions from cache.
    pub evictions: u64,
    /// Number of dirty pages written back.
    pub dirty_writebacks: u64,
    /// Total number of MVCC page versions retained.
    pub mvcc_page_versions_total: u64,
    /// Number of pages currently tracking historical versions.
    pub mvcc_pages_with_versions: u64,
    /// Total active read guards.
    pub mvcc_readers_active: u64,
    /// Total reader begin events since start.
    pub mvcc_reader_begin_total: u64,
    /// Total reader end events since start.
    pub mvcc_reader_end_total: u64,
    /// Oldest snapshot commit across active readers.
    pub mvcc_reader_oldest_snapshot: CommitId,
    /// Newest snapshot commit across active readers.
    pub mvcc_reader_newest_snapshot: CommitId,
    /// Maximum observed reader age in milliseconds.
    pub mvcc_reader_max_age_ms: u64,
    /// Maximum observed MVCC version chain length for any page.
    pub mvcc_max_chain_len: u64,
    /// Pages with uncheckpointed overlays present.
    pub mvcc_overlay_pages: u64,
    /// Total overlay entries across pages.
    pub mvcc_overlay_entries: u64,
    /// Active reader lock count (file lock coordinator).
    pub lock_readers: u32,
    /// Whether the writer lock is held.
    pub lock_writer: bool,
    /// Whether checkpoint lock is held.
    pub lock_checkpoint: bool,
}

/// Context provided to background maintenance hooks after auto-checkpoints.
#[derive(Clone, Copy, Debug)]
pub struct AutockptContext {
    /// LSN of the last completed checkpoint.
    pub last_checkpoint_lsn: Lsn,
    /// Time elapsed since the last checkpoint finished.
    pub elapsed_since_last: Duration,
}

/// Hook trait for background maintenance tasks that run on the pager thread.
pub trait BackgroundMaintainer: 'static {
    /// Invoked after `maybe_autocheckpoint` completes its work.
    fn run_background_maint(&self, ctx: &AutockptContext);
}

/// Trait for page-oriented storage with transactional support.
#[allow(dead_code)]
pub trait PageStore: 'static {
    /// Returns the page size in bytes.
    fn page_size(&self) -> u32;
    /// Retrieves a page within a read transaction.
    fn get_page(&self, guard: &ReadGuard, id: PageId) -> Result<PageRef>;
    /// Retrieves a page while holding a write transaction.
    fn get_page_with_write(&self, guard: &mut WriteGuard<'_>, id: PageId) -> Result<PageRef>;
    /// Begins a read transaction.
    fn begin_read(&self) -> Result<ReadGuard>;
    /// Begins a read transaction targeting the latest committed snapshot.
    fn begin_latest_committed_read(&self) -> Result<ReadGuard> {
        self.begin_read()
    }
    /// Begins a write transaction.
    fn begin_write(&self) -> Result<WriteGuard<'_>>;
    /// Commits a write transaction, returning the LSN.
    fn commit(&self, guard: WriteGuard<'_>) -> Result<Lsn>;
    /// Triggers a checkpoint operation.
    fn checkpoint(&self, mode: CheckpointMode) -> Result<()>;
    /// Returns the LSN of the last completed checkpoint.
    fn last_checkpoint_lsn(&self) -> Lsn;
    /// Returns the current metadata.
    fn meta(&self) -> Result<Meta>;
    /// Returns a snapshot of pager statistics.
    fn stats(&self) -> PagerStats;

    /// Returns the latest committed LSN when provided by the pager.
    fn latest_committed_lsn(&self) -> Option<Lsn> {
        None
    }

    /// Returns the durable watermark LSN when available.
    fn durable_lsn(&self) -> Option<Lsn> {
        None
    }

    /// Returns the current WAL commit backlog when available.
    fn wal_commit_backlog(&self) -> Option<WalCommitBacklog> {
        None
    }

    /// Returns allocator/preallocation stats for the WAL when available.
    fn wal_allocator_stats(&self) -> Option<WalAllocatorStats> {
        None
    }

    /// Returns async fsync backlog details, including pending cookie LSN.
    fn async_fsync_backlog(&self) -> Option<AsyncFsyncBacklog> {
        None
    }

    /// Enables or disables checksum verification on page reads.
    fn set_checksum_verification(&self, enabled: bool) {
        let _ = enabled;
    }

    /// Returns whether checksum verification is enabled.
    fn checksum_verification_enabled(&self) -> bool {
        true
    }

    /// Returns the pager commit table when available.
    fn commit_table(&self) -> Option<Arc<Mutex<CommitTable>>> {
        None
    }

    /// Hook invoked after the pager finishes its auto-checkpoint pass.
    fn maybe_background_maint(&self, _ctx: &AutockptContext) {}

    /// Registers a background maintenance callback.
    fn register_background_maint(&self, _hook: Weak<dyn BackgroundMaintainer>) {}
}

/// An immutable reference to a page.
pub struct PageRef {
    /// Page identifier.
    pub id: PageId,
    data: Arc<[u8]>,
}

impl PageRef {
    /// Returns the page data as a byte slice.
    pub fn data(&self) -> &[u8] {
        &self.data
    }
}

/// A mutable reference to a page within a write transaction.
pub struct PageMut<'a> {
    /// Page identifier.
    pub id: PageId,
    pager: &'a Pager,
    frame_idx: usize,
    guard: Option<ArcRwLockWriteGuard<parking_lot::RawRwLock, Box<[u8]>>>,
}

impl<'a> PageMut<'a> {
    /// Returns the page data as an immutable byte slice.
    pub fn data(&self) -> &[u8] {
        self.guard
            .as_ref()
            .map(|guard| &guard[..])
            .expect("page guard missing")
    }

    /// Returns the page data as a mutable byte slice.
    pub fn data_mut(&mut self) -> &mut [u8] {
        self.guard
            .as_mut()
            .map(|guard| &mut guard[..])
            .expect("page guard missing")
    }
}

impl<'a> Drop for PageMut<'a> {
    fn drop(&mut self) {
        if let Some(guard) = self.guard.take() {
            drop(guard);
        }
        self.pager.release_frame(self.frame_idx);
    }
}

/// Guard for a read transaction, holding a snapshot at a specific LSN.
pub struct ReadGuard {
    _lock: LockReaderGuard,
    snapshot_lsn: Lsn,
    consistency: ReadConsistency,
    commit_table: Arc<Mutex<CommitTable>>,
    commit_reader: CommitReader,
    _metrics: ReaderMetricsHandle,
}

/// Guard for a write transaction, tracking modifications and state for rollback.
pub struct WriteGuard<'a> {
    pager: &'a Pager,
    lock: Option<LockWriterGuard>,
    dirty_pages: HashSet<PageId>,
    original_pages: HashMap<PageId, Vec<u8>>,
    allocated_pages: Vec<PageId>,
    freed_pages: Vec<PageId>,
    meta_snapshot: Meta,
    free_cache_snapshot: FreeCache,
    freelist_pages_snapshot: Vec<PageId>,
    pending_free_snapshot: Vec<PageId>,
    meta_dirty_snapshot: bool,
    committed: bool,
    commit_lsn: Option<Lsn>,
    extensions: TxnExtensions,
    intent_id: IntentId,
}

#[derive(Default)]
struct TxnExtensions {
    map: HashMap<TypeId, Box<dyn Any>>,
}

impl TxnExtensions {
    fn get_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.map
            .get_mut(&TypeId::of::<T>())
            .and_then(|value| value.downcast_mut::<T>())
    }

    fn insert<T: Any>(&mut self, value: T) {
        self.map.insert(TypeId::of::<T>(), Box::new(value));
    }

    fn remove<T: Any>(&mut self) -> Option<T> {
        self.map
            .remove(&TypeId::of::<T>())
            .map(|boxed| *boxed.downcast::<T>().expect("extension type mismatch"))
    }
}

impl ReadGuard {
    /// Returns the LSN of the snapshot this read guard observes.
    pub fn snapshot_lsn(&self) -> Lsn {
        self.snapshot_lsn
    }

    /// Indicates which consistency mode was requested.
    pub fn consistency(&self) -> ReadConsistency {
        self.consistency
    }
}

impl Drop for ReadGuard {
    fn drop(&mut self) {
        let mut table = self.commit_table.lock();
        table.release_reader(self.commit_reader);
    }
}

impl<'a> WriteGuard<'a> {
    /// Acquires a mutable reference to a page within this write transaction.
    pub fn page_mut(&mut self, id: PageId) -> Result<PageMut<'a>> {
        self.pager.get_page_mut_for_write(self, id)
    }

    /// Allocates a new page within this write transaction.
    pub fn allocate_page(&mut self) -> Result<PageId> {
        self.pager.allocate_page_in_txn(self)
    }

    /// Marks a page as free within this write transaction.
    pub fn free_page(&mut self, id: PageId) -> Result<()> {
        self.pager.free_page_in_txn(self, id)
    }

    /// Allocates a contiguous extent containing up to `len` pages.
    ///
    /// The returned extent may be shorter than `len` if the free cache cannot
    /// satisfy the entire request; callers should continue requesting extents
    /// until the desired page count is reached.
    pub fn allocate_extent(&mut self, len: u32) -> Result<Extent> {
        self.pager.allocate_extent_in_txn(self, len)
    }

    /// Reserves the commit LSN that will be used when this transaction commits.
    ///
    /// Subsequent calls return the same LSN. Reserving early may leave gaps in
    /// the LSN sequence if the transaction aborts, which is acceptable.
    pub fn reserve_commit_id(&mut self) -> Lsn {
        if let Some(lsn) = self.commit_lsn {
            return lsn;
        }
        let mut inner = self.pager.inner.lock();
        let lsn = inner.next_lsn;
        inner.next_lsn = Lsn(lsn.0.saturating_add(1));
        drop(inner);
        self.pager
            .promote_intent(self.intent_id, lsn)
            .expect("commit table promote intent");
        self.commit_lsn = Some(lsn);
        lsn
    }

    /// Updates the metadata within this write transaction using a closure.
    pub fn update_meta<F>(&mut self, f: F) -> Result<()>
    where
        F: FnOnce(&mut Meta),
    {
        self.pager.update_meta_in_txn(self, f)
    }

    /// Retrieves a mutable reference to a previously stored extension of type `T`.
    pub fn extension_mut<T: Any>(&mut self) -> Option<&mut T> {
        self.extensions.get_mut::<T>()
    }

    /// Inserts or replaces the extension of type `T`.
    pub fn store_extension<T: Any>(&mut self, value: T) {
        self.extensions.insert(value);
    }

    /// Removes and returns the extension of type `T`, if present.
    pub fn take_extension<T: Any>(&mut self) -> Option<T> {
        self.extensions.remove::<T>()
    }
    fn release_writer_lock(&mut self) {
        if let Some(lock) = self.lock.take() {
            drop(lock);
        }
    }

    fn reacquire_writer_lock(&mut self) -> Result<()> {
        if self.lock.is_some() {
            return Ok(());
        }
        loop {
            match self.pager.locks.acquire_writer() {
                Ok(lock) => {
                    self.lock = Some(lock);
                    return Ok(());
                }
                Err(SombraError::Invalid(msg)) if msg == "writer lock already held" => {
                    thread::sleep(Duration::from_millis(1));
                }
                Err(err) => return Err(err),
            }
        }
    }
}

impl Drop for WriteGuard<'_> {
    fn drop(&mut self) {
        if !self.committed {
            if self.lock.is_none() {
                let _ = self.reacquire_writer_lock();
            }
            let _ = self.pager.rollback_transaction(self);
        }
        if let Some(lock) = self.lock.take() {
            drop(lock);
        }
    }
}

const NORMAL_SYNC_DELAY_MS: u64 = 10;

struct WalSyncState {
    scheduled: bool,
    last_error: Option<SombraError>,
    pending_lsn: Lsn,
}

impl WalSyncState {
    fn new() -> Self {
        Self {
            scheduled: false,
            last_error: None,
            pending_lsn: Lsn(0),
        }
    }
}

/// Describes the outstanding async fsync backlog.
#[derive(Clone, Debug)]
pub struct AsyncFsyncBacklog {
    /// Highest LSN queued for async fsync.
    pub pending_lsn: Lsn,
    /// Last LSN synced and persisted to the durable cookie.
    pub durable_lsn: Lsn,
    /// Difference between pending and durable LSNs.
    pub pending_lag: u64,
    /// Last error observed by the async fsync worker, if any.
    pub last_error: Option<String>,
}

struct AsyncFsyncState {
    scheduled: bool,
    pending_lsn: Lsn,
    durable_lsn: Lsn,
    last_error: Option<SombraError>,
}

impl AsyncFsyncState {
    fn new(initial: Lsn) -> Self {
        Self {
            scheduled: false,
            pending_lsn: initial,
            durable_lsn: initial,
            last_error: None,
        }
    }
}

fn async_fsync_worker(
    wal: Arc<Wal>,
    cookie: Option<Arc<WalDurableCookie>>,
    commit_table: Arc<Mutex<CommitTable>>,
    state_arc: Arc<Mutex<AsyncFsyncState>>,
    durable: Arc<AtomicU64>,
    coalesce_wait: Duration,
) {
    loop {
        let mut target = {
            let mut state = state_arc.lock();
            if state.pending_lsn.0 <= state.durable_lsn.0 {
                state.scheduled = false;
                return;
            }
            state.pending_lsn
        };
        if !coalesce_wait.is_zero() {
            thread::sleep(coalesce_wait);
            let mut state = state_arc.lock();
            if state.pending_lsn.0 <= state.durable_lsn.0 {
                state.scheduled = false;
                return;
            }
            target = state.pending_lsn;
        }
        if let Err(err) = wal.sync() {
            let mut state = state_arc.lock();
            state.last_error = Some(err);
            state.scheduled = false;
            return;
        }
        if let Some(cookie) = cookie.as_ref() {
            if let Err(err) = cookie.persist(target) {
                let mut state = state_arc.lock();
                state.last_error = Some(err);
                state.scheduled = false;
                return;
            }
        }
        if let Err(err) = commit_table.lock().mark_durable_up_to(target.0) {
            let mut state = state_arc.lock();
            state.last_error = Some(err);
            state.scheduled = false;
            return;
        }
        durable.fetch_max(target.0, AtomicOrdering::Release);
        let mut state = state_arc.lock();
        state.durable_lsn = target;
        if state.pending_lsn.0 <= state.durable_lsn.0 {
            state.scheduled = false;
            return;
        }
    }
}

struct PagerInner {
    frames: Vec<Frame>,
    page_table: HashMap<PageId, usize>,
    free_cache: FreeCache,
    freelist_pages: Vec<PageId>,
    pending_free: Vec<PageId>,
    meta: Meta,
    meta_dirty: bool,
    next_lsn: Lsn,
    stats: PagerStats,
    clock_hand_hot: usize,
    clock_hand_cold: usize,
    target_cold: usize,
    hot_count: usize,
    cold_count: usize,
    test_pages: Vec<PageId>,
    test_lookup: HashSet<PageId>,
}

impl PagerInner {
    fn new(meta: Meta, cache_pages: usize, page_size: usize, next_lsn: Lsn) -> Self {
        let capacity = cache_pages.max(1);
        let mut frames = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            frames.push(Frame::new(page_size));
        }
        Self {
            frames,
            page_table: HashMap::new(),
            free_cache: FreeCache::default(),
            freelist_pages: Vec::new(),
            pending_free: Vec::new(),
            meta,
            meta_dirty: false,
            next_lsn,
            stats: PagerStats::default(),
            clock_hand_hot: 0,
            clock_hand_cold: 0,
            target_cold: max(1, capacity / 2),
            hot_count: 0,
            cold_count: 0,
            test_pages: Vec::new(),
            test_lookup: HashSet::new(),
        }
    }

    fn set_frame_state(&mut self, idx: usize, new_state: FrameState) {
        let frame = &mut self.frames[idx];
        if frame.state == new_state {
            return;
        }
        match frame.state {
            FrameState::Hot => self.hot_count = self.hot_count.saturating_sub(1),
            FrameState::Cold => self.cold_count = self.cold_count.saturating_sub(1),
            FrameState::Test => {}
        }
        match new_state {
            FrameState::Hot => self.hot_count += 1,
            FrameState::Cold => self.cold_count += 1,
            FrameState::Test => {}
        }
        frame.state = new_state;
    }

    fn add_test_page(&mut self, id: PageId) {
        if self.test_lookup.insert(id) {
            self.test_pages.push(id);
            while self.test_pages.len() > self.frames.len() {
                let removed = self.test_pages.remove(0);
                self.test_lookup.remove(&removed);
            }
        }
    }

    fn remove_test_page(&mut self, id: &PageId) -> bool {
        if self.test_lookup.remove(id) {
            if let Some(pos) = self.test_pages.iter().position(|p| p == id) {
                self.test_pages.swap_remove(pos);
            }
            true
        } else {
            false
        }
    }
}

/// Page-oriented storage manager with write-ahead logging and caching.
///
/// The pager manages pages on disk with transactional semantics, including:
/// - Buffer cache for frequently accessed pages
/// - Write-ahead logging for durability
/// - Automatic checkpointing
/// - MVCC-style read snapshots
pub struct Pager {
    db_io: Arc<dyn FileIo>,
    wal: Arc<Wal>,
    wal_committer: WalCommitter,
    locks: SingleWriter,
    page_size: usize,
    options: Mutex<PagerOptions>,
    inner: Mutex<PagerInner>,
    last_autocheckpoint: Mutex<Option<Instant>>,
    wal_sync_state: Arc<Mutex<WalSyncState>>,
    wal_cookie: Option<Arc<WalDurableCookie>>,
    async_fsync_state: Option<Arc<Mutex<AsyncFsyncState>>>,
    checksum_verify_on_read: AtomicBool,
    latest_visible_lsn: AtomicU64,
    durable_lsn: Arc<AtomicU64>,
    commit_table: Arc<Mutex<CommitTable>>,
    overlays: Mutex<HashMap<PageId, VecDeque<OverlayEntry>>>,
    version_chains: Mutex<HashMap<PageId, VecDeque<VersionChainEntry>>>,
    mvcc_version_count: AtomicU64,
    mvcc_version_pages: AtomicU64,
    reader_metrics: Arc<ReaderMetrics>,
    background_hooks: Mutex<Vec<Weak<dyn BackgroundMaintainer>>>,
}

impl Pager {
    /// Creates a new pager database at the specified path.
    ///
    /// This initializes a fresh database with metadata page and WAL.
    pub fn create(path: impl AsRef<Path>, options: PagerOptions) -> Result<Self> {
        let path = path.as_ref();
        let db = Arc::new(StdFileIo::open(path)?);
        let mut meta = create_meta(db.as_ref(), options.page_size)?;
        Self::open_internal(path, db, &mut meta, options, true)
    }

    /// Opens an existing pager database at the specified path.
    ///
    /// This loads metadata and performs WAL recovery if needed.
    pub fn open(path: impl AsRef<Path>, options: PagerOptions) -> Result<Self> {
        let path = path.as_ref();
        let db = Arc::new(StdFileIo::open(path)?);
        let mut meta = load_meta(db.as_ref(), options.page_size)?;
        Self::open_internal(path, db, &mut meta, options, false)
    }

    /// Sets the synchronous mode at runtime.
    pub fn set_synchronous(&self, mode: Synchronous) {
        let mut options = self.options.lock();
        options.synchronous = mode;
    }

    /// Returns the current synchronous mode.
    pub fn synchronous(&self) -> Synchronous {
        let options = self.options.lock();
        options.synchronous
    }

    /// Sets the WAL commit coalesce time in milliseconds at runtime.
    pub fn set_wal_coalesce_ms(&self, ms: u64) {
        let config = {
            let mut options = self.options.lock();
            options.group_commit_max_wait_ms = ms;
            Self::wal_commit_config_from_options(&options)
        };
        self.wal_committer.set_config(config);
    }

    /// Returns the current WAL commit coalesce time in milliseconds.
    pub fn wal_coalesce_ms(&self) -> u64 {
        let options = self.options.lock();
        options.group_commit_max_wait_ms
    }

    /// Sets the autocheckpoint interval in milliseconds at runtime.
    pub fn set_autocheckpoint_ms(&self, ms: Option<u64>) {
        let mut options = self.options.lock();
        options.autocheckpoint_ms = ms;
        if ms.is_some() {
            *self.last_autocheckpoint.lock() = Some(Instant::now());
        } else {
            *self.last_autocheckpoint.lock() = None;
        }
    }

    /// Returns the current autocheckpoint interval in milliseconds.
    pub fn autocheckpoint_ms(&self) -> Option<u64> {
        let options = self.options.lock();
        options.autocheckpoint_ms
    }

    fn open_internal(
        path: &Path,
        db_io: Arc<dyn FileIo>,
        meta: &mut Meta,
        options: PagerOptions,
        is_create: bool,
    ) -> Result<Self> {
        let wal_dir = wal_path(path);
        let mut wal_options = WalOptions::new(
            meta.page_size,
            meta.wal_salt,
            Lsn(meta.last_checkpoint_lsn.0 + 1),
        );
        wal_options.segment_size_bytes = options.wal_segment_size_bytes;
        wal_options.preallocate_segments = options.wal_preallocate_segments;
        let wal = Wal::open(&wal_dir, wal_options)?;
        let wal_cookie = if options.async_fsync {
            Some(Arc::new(WalDurableCookie::new(wal_cookie_path(path))))
        } else {
            None
        };
        let cookie_floor = if let Some(cookie) = wal_cookie.as_ref() {
            cookie.read()?.unwrap_or(meta.last_checkpoint_lsn)
        } else {
            meta.last_checkpoint_lsn
        };
        let next_lsn = if is_create {
            wal.reset(Lsn(meta.last_checkpoint_lsn.0 + 1))?;
            Lsn(meta.last_checkpoint_lsn.0 + 1)
        } else {
            recover_database(
                wal.as_ref(),
                db_io.as_ref(),
                meta,
                meta.page_size as usize,
                if options.async_fsync {
                    Some(cookie_floor)
                } else {
                    None
                },
            )?
        };
        if let Some(cookie) = wal_cookie.as_ref() {
            cookie.persist(meta.last_checkpoint_lsn)?;
        }
        let locks = SingleWriter::open(lock_path(path))?;
        let page_size = meta.page_size as usize;
        let cache_pages = options.cache_pages;
        let inner = PagerInner::new(meta.clone(), cache_pages, page_size, next_lsn);
        let wal_commit_config = Self::wal_commit_config_from_options(&options);
        let wal_committer = WalCommitter::new(Arc::clone(&wal), wal_commit_config);
        let commit_table = Arc::new(Mutex::new(CommitTable::new(meta.last_checkpoint_lsn.0)));
        let durable_lsn = Arc::new(AtomicU64::new(meta.last_checkpoint_lsn.0));
        let async_fsync_state = if options.async_fsync {
            Some(Arc::new(Mutex::new(AsyncFsyncState::new(
                meta.last_checkpoint_lsn,
            ))))
        } else {
            None
        };
        let pager = Self {
            db_io,
            wal,
            wal_committer,
            locks,
            page_size,
            options: Mutex::new(options),
            inner: Mutex::new(inner),
            last_autocheckpoint: Mutex::new(None),
            wal_sync_state: Arc::new(Mutex::new(WalSyncState::new())),
            wal_cookie,
            async_fsync_state,
            checksum_verify_on_read: AtomicBool::new(true),
            latest_visible_lsn: AtomicU64::new(meta.last_checkpoint_lsn.0),
            durable_lsn,
            commit_table,
            overlays: Mutex::new(HashMap::new()),
            version_chains: Mutex::new(HashMap::new()),
            mvcc_version_count: AtomicU64::new(0),
            mvcc_version_pages: AtomicU64::new(0),
            reader_metrics: Arc::new(ReaderMetrics::new()),
            background_hooks: Mutex::new(Vec::new()),
        };
        pager.load_freelist()?;
        Ok(pager)
    }

    fn load_freelist(&self) -> Result<()> {
        let mut inner = self.inner.lock();
        self.load_freelist_locked(&mut inner)
    }

    fn load_freelist_locked(&self, inner: &mut PagerInner) -> Result<()> {
        inner.free_cache = FreeCache::default();
        inner.freelist_pages.clear();
        let mut next = inner.meta.free_head;
        let max_page = inner.meta.next_page.0;
        let mut filtered = false;
        while next.0 != 0 {
            let mut buf = vec![0u8; self.page_size];
            self.db_io
                .read_at(page_offset(next, self.page_size), &mut buf)?;
            let free_page = read_free_page(&buf, self.page_size, &inner.meta)?;
            for extent in free_page.extents {
                if extent.start.0 >= max_page {
                    filtered = true;
                    continue;
                }
                let end = extent.start.0 + extent.len as u64;
                let clamped_len = if end > max_page {
                    filtered = true;
                    (max_page - extent.start.0) as u32
                } else {
                    extent.len
                };
                if clamped_len == 0 {
                    continue;
                }
                inner
                    .free_cache
                    .extend(vec![Extent::new(extent.start, clamped_len)]);
            }
            inner.freelist_pages.push(next);
            next = free_page.next;
        }
        if filtered {
            inner.meta_dirty = true;
        }
        Ok(())
    }

    /// Evicts a frame from the cache, writing it back if dirty.
    fn evict_frame(&self, inner: &mut PagerInner, idx: usize) -> Result<()> {
        if inner.frames[idx].dirty {
            self.flush_frame(inner, idx)?;
        }
        if let Some(old) = inner.frames[idx].id {
            inner.page_table.remove(&old);
            if old.0 != 0 {
                inner.add_test_page(old);
            }
        }
        inner.set_frame_state(idx, FrameState::Test);
        let frame = &mut inner.frames[idx];
        frame.id = None;
        frame.reference = false;
        frame.dirty = false;
        frame.pin_count = 0;
        frame.pending_checkpoint = false;
        frame.newly_allocated = false;
        frame.needs_refresh = false;
        inner.stats.evictions += 1;
        Ok(())
    }

    /// Returns the page size in bytes.
    pub fn page_size(&self) -> u32 {
        self.page_size as u32
    }

    /// Returns allocator state for the backing WAL.
    pub fn wal_allocator_stats(&self) -> WalAllocatorStats {
        self.wal.allocator_stats()
    }

    /// Returns a snapshot of current pager statistics.
    pub fn stats(&self) -> PagerStats {
        let (active_total, begin_total, end_total) = self.reader_metrics.snapshot();
        let now = Instant::now();
        let reader_snapshot = {
            let table = self.commit_table.lock();
            table.reader_snapshot(now)
        };
        let newest_snapshot = self.latest_committed_lsn().0;
        self.maybe_warn_slow_readers(newest_snapshot, &reader_snapshot);
        let state = self.inner.lock();
        let mut stats = state.stats.clone();
        let lock_snapshot = self.locks.snapshot();
        let (overlay_pages, overlay_entries) = {
            let overlays = self.overlays.lock();
            let pages = overlays.len() as u64;
            let entries = overlays.values().map(|q| q.len() as u64).sum::<u64>();
            (pages, entries)
        };
        let max_chain_len = {
            let chains = self.version_chains.lock();
            chains
                .values()
                .map(|entries| entries.len() as u64)
                .max()
                .unwrap_or(0)
        };
        stats.mvcc_page_versions_total = self.mvcc_version_count.load(AtomicOrdering::Relaxed);
        stats.mvcc_pages_with_versions = self.mvcc_version_pages.load(AtomicOrdering::Relaxed);
        stats.mvcc_readers_active = reader_snapshot.active.max(active_total);
        stats.mvcc_reader_begin_total = begin_total;
        stats.mvcc_reader_end_total = end_total;
        stats.mvcc_reader_oldest_snapshot =
            reader_snapshot.oldest_snapshot.unwrap_or(newest_snapshot);
        stats.mvcc_reader_newest_snapshot =
            reader_snapshot.newest_snapshot.unwrap_or(newest_snapshot);
        stats.mvcc_reader_max_age_ms = reader_snapshot.max_age_ms;
        stats.mvcc_max_chain_len = max_chain_len;
        stats.mvcc_overlay_pages = overlay_pages;
        stats.mvcc_overlay_entries = overlay_entries;
        stats.lock_readers = lock_snapshot.readers;
        stats.lock_writer = lock_snapshot.writer;
        stats.lock_checkpoint = lock_snapshot.checkpoint;
        stats
    }

    fn maybe_warn_slow_readers(&self, newest_snapshot: CommitId, snapshot: &ReaderSnapshot) {
        if snapshot.active == 0 || snapshot.max_age_ms < MVCC_READER_WARN_THRESHOLD_MS {
            return;
        }
        warn!(
            reader_count = snapshot.active,
            max_age_ms = snapshot.max_age_ms,
            threshold_ms = MVCC_READER_WARN_THRESHOLD_MS,
            oldest_snapshot = snapshot
                .oldest_snapshot
                .unwrap_or(newest_snapshot),
            latest_committed = newest_snapshot,
            slow_readers = ?snapshot.slow_readers,
            "pager.mvcc.reader_lag"
        );
    }

    fn lookup_or_load_frame(
        &self,
        inner: &mut PagerInner,
        page_id: PageId,
    ) -> Result<(usize, bool)> {
        if let Some(&idx) = inner.page_table.get(&page_id) {
            return Ok((idx, true));
        }
        if page_id.0 >= inner.meta.next_page.0 {
            return Err(SombraError::Invalid("page not allocated"));
        }
        let idx = self.obtain_available_frame(inner)?;
        self.load_page_into_frame(inner, idx, page_id)?;
        inner.page_table.insert(page_id, idx);
        Ok((idx, false))
    }

    fn obtain_available_frame(&self, inner: &mut PagerInner) -> Result<usize> {
        if let Some(idx) = inner.frames.iter().enumerate().find_map(|(idx, frame)| {
            if frame.id.is_none() && frame.pin_count == 0 {
                Some(idx)
            } else {
                None
            }
        }) {
            return Ok(idx);
        }
        self.run_clock(inner)?;
        inner
            .frames
            .iter()
            .enumerate()
            .find_map(|(idx, frame)| {
                if frame.id.is_none() && frame.pin_count == 0 {
                    Some(idx)
                } else {
                    None
                }
            })
            .ok_or(SombraError::Invalid("no eviction candidate available"))
    }

    fn load_page_into_frame(
        &self,
        inner: &mut PagerInner,
        idx: usize,
        page_id: PageId,
    ) -> Result<()> {
        {
            let frame = &mut inner.frames[idx];
            debug_assert!(frame.id.is_none());
            frame.id = Some(page_id);
            frame.reference = true;
            frame.dirty = false;
            frame.pin_count = 0;
            frame.pending_checkpoint = false;
            frame.newly_allocated = false;
            frame.needs_refresh = false;
        }
        let was_test = inner.remove_test_page(&page_id);
        if was_test {
            inner.target_cold = min(inner.frames.len(), inner.target_cold + 1);
            inner.set_frame_state(idx, FrameState::Hot);
        } else {
            if inner.target_cold > 1 {
                inner.target_cold -= 1;
            }
            inner.set_frame_state(idx, FrameState::Cold);
        }
        self.adjust_cold_balance(inner);
        let mut guard = inner.frames[idx].buf.write();
        guard.fill(0);
        let result = self
            .db_io
            .read_at(page_offset(page_id, self.page_size), &mut guard);
        let fresh = match result {
            Ok(()) => false,
            Err(SombraError::Io(err)) if err.kind() == ErrorKind::UnexpectedEof => true,
            Err(err) => return Err(err),
        };
        let verify_crc = self.checksum_verify_on_read.load(AtomicOrdering::Relaxed);
        if !fresh && page_id.0 != 0 {
            let header = PageHeader::decode(&guard[..PAGE_HDR_LEN])?;
            if header.page_no != page_id {
                return Err(SombraError::Corruption("page number mismatch"));
            }
            if header.page_size != inner.meta.page_size {
                return Err(SombraError::Corruption("page size mismatch"));
            }
            if verify_crc {
                let mut scratch = guard.clone();
                page::clear_crc32(&mut scratch[..PAGE_HDR_LEN])?;
                let crc = page_crc32(page_id.0, inner.meta.salt, &scratch);
                if crc != header.crc32 {
                    return Err(SombraError::Corruption("page crc mismatch"));
                }
            }
        }
        Ok(())
    }

    fn flush_frame(&self, inner: &mut PagerInner, idx: usize) -> Result<()> {
        let frame = &mut inner.frames[idx];
        if !frame.dirty {
            return Ok(());
        }
        if frame.pin_count != 0 {
            return Err(SombraError::Invalid("cannot flush pinned page"));
        }
        let page_id = frame.id.expect("dirty frame must have id");
        let mut guard = frame.buf.write();
        page::clear_crc32(&mut guard[..PAGE_HDR_LEN])?;
        let crc = page_crc32(page_id.0, inner.meta.salt, &guard);
        guard[page::header::CRC32].copy_from_slice(&crc.to_be_bytes());
        self.db_io
            .write_at(page_offset(page_id, self.page_size), &guard)?;
        frame.dirty = false;
        inner.stats.dirty_writebacks += 1;
        Ok(())
    }
}

impl Pager {
    fn rebuild_freelist(&self, inner: &mut PagerInner) -> Result<()> {
        let original_next_page = inner.meta.next_page;
        let mut truncated = false;
        pager_test_log!(
            "[pager.freelist] rebuild start free_cache_extents={} pending_free={} freelist_pages={}",
            inner.free_cache.extents().len(),
            inner.pending_free.len(),
            inner.freelist_pages.len()
        );
        // absorb pending frees
        let mut all_pages: Vec<PageId> = inner
            .free_cache
            .extents()
            .iter()
            .flat_map(|e| e.iter_pages())
            .collect();
        all_pages.append(&mut inner.pending_free);
        // released freelist pages become regular pages before reassigning
        all_pages.append(&mut inner.freelist_pages);
        if all_pages.is_empty() {
            inner.free_cache = FreeCache::default();
            inner.meta.free_head = PageId(0);
            inner.freelist_pages.clear();
            return Ok(());
        }
        all_pages.sort_by_key(|p| p.0);
        all_pages.dedup();
        pager_test_log!("[pager.freelist] unique free pages={}", all_pages.len());

        {
            let mut shrink_target = inner.meta.next_page;
            while shrink_target.0 > 1 {
                match all_pages.last() {
                    Some(last) if last.0 == shrink_target.0 - 1 => {
                        all_pages.pop();
                        shrink_target = PageId(shrink_target.0 - 1);
                    }
                    _ => break,
                }
            }
            if shrink_target != inner.meta.next_page {
                truncated = true;
                pager_test_log!(
                    "[pager.freelist] shrink next_page from {} to {}",
                    inner.meta.next_page.0,
                    shrink_target.0
                );
                let mut to_clear = Vec::new();
                for (idx, frame) in inner.frames.iter().enumerate() {
                    if let Some(id) = frame.id {
                        if id.0 >= shrink_target.0 {
                            to_clear.push((idx, id));
                        }
                    }
                }
                for (idx, id) in to_clear {
                    inner.page_table.remove(&id);
                    inner.set_frame_state(idx, FrameState::Test);
                    let frame = &mut inner.frames[idx];
                    frame.id = None;
                    frame.dirty = false;
                    frame.reference = false;
                    frame.pin_count = 0;
                    inner.remove_test_page(&id);
                }
                inner.meta.next_page = shrink_target;
                inner.meta_dirty = true;
            }
        }
        all_pages.retain(|p| p.0 < inner.meta.next_page.0);
        pager_test_log!(
            "[pager.freelist] pages after truncation={}",
            all_pages.len()
        );

        if all_pages.is_empty() {
            inner.free_cache = FreeCache::default();
            inner.meta.free_head = PageId(0);
            inner.freelist_pages.clear();
            debug_assert!(inner
                .free_cache
                .extents()
                .iter()
                .all(|extent| extent.start.0 + extent.len as u64 <= inner.meta.next_page.0));
            if truncated && inner.meta.next_page.0 < original_next_page.0 {
                let new_len = inner.meta.next_page.0 * self.page_size as u64;
                self.db_io.truncate(new_len)?;
            }
            return Ok(());
        }

        // We'll take pages needed for freelist metadata from the end
        let capacity = free_page_capacity(self.page_size);
        pager_test_log!(
            "[pager.freelist] free page capacity per freelist page={}",
            capacity
        );
        inner.free_cache = FreeCache::from_extents(pages_to_extents(&all_pages));

        if capacity == 0 {
            return Err(SombraError::Invalid("page size too small for freelist"));
        }
        let total_extents = pages_to_extents(&all_pages);
        let needed_pages = if total_extents.is_empty() {
            0
        } else {
            total_extents.len().div_ceil(capacity)
        };
        pager_test_log!(
            "[pager.freelist] total_extents={} needed_pages={}",
            total_extents.len(),
            needed_pages
        );
        if needed_pages == 0 {
            inner.meta.free_head = PageId(0);
            inner.freelist_pages.clear();
            debug_assert!(inner
                .free_cache
                .extents()
                .iter()
                .all(|extent| extent.start.0 + extent.len as u64 <= inner.meta.next_page.0));
            if truncated && inner.meta.next_page.0 < original_next_page.0 {
                let new_len = inner.meta.next_page.0 * self.page_size as u64;
                self.db_io.truncate(new_len)?;
            }
            return Ok(());
        }

        let mut freelist_pages = Vec::new();
        let mut remaining_pages = all_pages;

        while freelist_pages.len() < needed_pages {
            let remaining_needed = needed_pages - freelist_pages.len();
            if let Some(page) = remaining_pages.pop() {
                if remaining_pages.len() < remaining_needed || page.0 <= 1 {
                    remaining_pages.push(page);
                    let next = inner.meta.next_page;
                    inner.meta.next_page = PageId(next.0 + 1);
                    freelist_pages.push(next);
                    inner.meta_dirty = true;
                } else {
                    freelist_pages.push(page);
                }
            } else {
                let next = inner.meta.next_page;
                inner.meta.next_page = PageId(next.0 + 1);
                freelist_pages.push(next);
                inner.meta_dirty = true;
            }
        }

        freelist_pages.sort_by_key(|p| p.0);
        pager_test_log!("[pager.freelist] freelist_pages={:?}", freelist_pages);
        inner.meta.free_head = freelist_pages[0];
        inner.freelist_pages = freelist_pages.clone();

        // remove freelist pages from free cache
        let remaining: Vec<PageId> = remaining_pages;
        let remaining_extents = pages_to_extents(&remaining);
        inner.free_cache = FreeCache::from_extents(remaining_extents.clone());
        debug_assert!(inner
            .free_cache
            .extents()
            .iter()
            .all(|extent| extent.start.0 + extent.len as u64 <= inner.meta.next_page.0));

        let mut extent_iter = remaining_extents.into_iter();
        for (idx, page_id) in freelist_pages.iter().enumerate() {
            pager_test_log!(
                "[pager.freelist] writing freelist page {:?} (idx={})",
                page_id,
                idx
            );
            let mut slot = Vec::new();
            for _ in 0..capacity {
                if let Some(extent) = extent_iter.next() {
                    slot.push(extent);
                } else {
                    break;
                }
            }
            let next = if idx + 1 < freelist_pages.len() {
                freelist_pages[idx + 1]
            } else {
                PageId(0)
            };
            let mut buf = vec![0u8; self.page_size];
            write_free_page(&mut buf, *page_id, &inner.meta, next, &slot)?;
            let offset = page_offset(*page_id, self.page_size);
            pager_test_log!(
                "[pager.freelist] write start page {:?} offset {}",
                *page_id,
                offset
            );
            self.db_io.write_at(offset, &buf)?;
            pager_test_log!("[pager.freelist] write complete page {:?}", *page_id);
        }
        pager_test_log!("[pager.freelist] freelist pages written");

        if truncated && inner.meta.next_page.0 < original_next_page.0 {
            let new_len = inner.meta.next_page.0 * self.page_size as u64;
            self.db_io.truncate(new_len)?;
        }

        pager_test_log!("[pager.freelist] rebuild complete");
        Ok(())
    }

    fn release_frame(&self, frame_idx: usize) {
        let mut inner = self.inner.lock();
        if let Some(frame) = inner.frames.get_mut(frame_idx) {
            if frame.pin_count > 0 {
                frame.pin_count -= 1;
            }
        }
    }

    fn get_page_mut_for_write(
        &self,
        guard: &mut WriteGuard<'_>,
        id: PageId,
    ) -> Result<PageMut<'_>> {
        let (idx, buf_arc) = {
            let mut inner = self.inner.lock();
            let (idx, hit) = self.lookup_or_load_frame(&mut inner, id)?;
            if hit {
                inner.stats.hits += 1;
            } else {
                inner.stats.misses += 1;
            }
            {
                let frame = &mut inner.frames[idx];
                frame.reference = true;
                frame.pin_count += 1;
                frame.dirty = true;
                frame.needs_refresh = false;
                if guard.allocated_pages.contains(&id) {
                    frame.newly_allocated = true;
                }
            }
            guard.original_pages.entry(id).or_insert_with(|| {
                let snapshot = inner.frames[idx].buf.read_arc();
                snapshot.as_ref().to_vec()
            });
            guard.dirty_pages.insert(id);
            let arc = inner.frames[idx].buf.clone();
            (idx, arc)
        };
        let guard_buf = buf_arc.write_arc();
        Ok(PageMut {
            id,
            pager: self,
            frame_idx: idx,
            guard: Some(guard_buf),
        })
    }

    fn allocate_page_in_txn(&self, guard: &mut WriteGuard<'_>) -> Result<PageId> {
        let wal_frames = self.wal.stats().frames_appended;
        let mut inner = self.inner.lock();
        if let Some(page) = inner.free_cache.pop() {
            if page.0 >= inner.meta.next_page.0 {
                inner.meta.next_page = PageId(page.0 + 1);
            }
            inner.meta_dirty = true;
            guard.allocated_pages.push(page);
            return Ok(page);
        }
        let should_reload = !inner.meta_dirty
            && wal_frames == 0
            && (inner.meta.free_head.0 != 0 || !inner.freelist_pages.is_empty());
        if should_reload {
            self.load_freelist_locked(&mut inner)?;
            if let Some(page) = inner.free_cache.pop() {
                if page.0 >= inner.meta.next_page.0 {
                    inner.meta.next_page = PageId(page.0 + 1);
                }
                inner.meta_dirty = true;
                guard.allocated_pages.push(page);
                return Ok(page);
            }
        }
        let page = inner.meta.next_page;
        inner.meta.next_page = PageId(page.0 + 1);
        inner.meta_dirty = true;
        guard.allocated_pages.push(page);
        Ok(page)
    }

    fn allocate_extent_in_txn(&self, guard: &mut WriteGuard<'_>, len: u32) -> Result<Extent> {
        if len == 0 {
            return Err(SombraError::Invalid("extent length must be non-zero"));
        }
        let wal_frames = self.wal.stats().frames_appended;
        let mut inner = self.inner.lock();
        if let Some(extent) = inner.free_cache.pop_extent(len) {
            self.record_extent_allocation(&mut inner, guard, extent);
            return Ok(extent);
        }
        let should_reload = !inner.meta_dirty
            && wal_frames == 0
            && (inner.meta.free_head.0 != 0 || !inner.freelist_pages.is_empty());
        if should_reload {
            self.load_freelist_locked(&mut inner)?;
            if let Some(extent) = inner.free_cache.pop_extent(len) {
                self.record_extent_allocation(&mut inner, guard, extent);
                return Ok(extent);
            }
        }
        let start = inner.meta.next_page;
        let extent = Extent::new(start, len);
        let end = start
            .0
            .checked_add(len as u64)
            .ok_or(SombraError::Invalid("extent allocation overflow"))?;
        inner.meta.next_page = PageId(end);
        inner.meta_dirty = true;
        self.record_extent_allocation(&mut inner, guard, extent);
        Ok(extent)
    }

    fn record_extent_allocation(
        &self,
        inner: &mut PagerInner,
        guard: &mut WriteGuard<'_>,
        extent: Extent,
    ) {
        let end = extent
            .start
            .0
            .checked_add(extent.len as u64)
            .expect("extent length overflow");
        if end > inner.meta.next_page.0 {
            inner.meta.next_page = PageId(end);
        }
        inner.meta_dirty = true;
        for page in extent.iter_pages() {
            guard.allocated_pages.push(page);
        }
    }

    fn free_page_in_txn(&self, guard: &mut WriteGuard<'_>, id: PageId) -> Result<()> {
        if id.0 == 0 {
            return Err(SombraError::Invalid("cannot free meta page"));
        }
        let mut inner = self.inner.lock();
        if let Some(&idx) = inner.page_table.get(&id) {
            let frame = &inner.frames[idx];
            if frame.pin_count != 0 {
                return Err(SombraError::Invalid("cannot free pinned page"));
            }
        }
        inner.page_table.remove(&id);
        inner.pending_free.push(id);
        inner.meta_dirty = true;
        guard.freed_pages.push(id);
        guard.dirty_pages.remove(&id);
        guard.original_pages.remove(&id);
        Ok(())
    }

    fn update_meta_in_txn<F>(&self, guard: &mut WriteGuard<'_>, f: F) -> Result<()>
    where
        F: FnOnce(&mut Meta),
    {
        let mut inner = self.inner.lock();
        f(&mut inner.meta);
        inner.meta_dirty = true;
        drop(inner);
        guard.dirty_pages.insert(PageId(0));
        Ok(())
    }

    fn rollback_transaction(&self, guard: &mut WriteGuard<'_>) -> Result<()> {
        let mut inner = self.inner.lock();
        inner.meta = guard.meta_snapshot.clone();
        inner.free_cache = guard.free_cache_snapshot.clone();
        inner.freelist_pages = guard.freelist_pages_snapshot.clone();
        inner.pending_free = guard.pending_free_snapshot.clone();
        inner.meta_dirty = guard.meta_dirty_snapshot;
        for (page_id, data) in guard.original_pages.iter() {
            if let Some(&idx) = inner.page_table.get(page_id) {
                {
                    let mut buf = inner.frames[idx].buf.write();
                    buf.copy_from_slice(data);
                }
                inner.frames[idx].dirty = false;
                inner.frames[idx].pending_checkpoint = false;
                inner.frames[idx].newly_allocated = false;
                inner.frames[idx].needs_refresh = true;
            }
        }
        Ok(())
    }

    fn wal_commit_config_from_options(options: &PagerOptions) -> WalCommitConfig {
        WalCommitConfig {
            max_batch_commits: options.group_commit_max_writers,
            max_batch_frames: options.group_commit_max_frames,
            max_batch_wait: Duration::from_millis(options.group_commit_max_wait_ms),
        }
    }

    fn run_clock(&self, inner: &mut PagerInner) -> Result<()> {
        let len = inner.frames.len();
        for _ in 0..len * 4 {
            let idx = inner.clock_hand_cold;
            inner.clock_hand_cold = (inner.clock_hand_cold + 1) % len;
            let mut promote_to_hot = false;
            let mut convert_to_cold = false;
            let mut evict = false;
            {
                let frame = &mut inner.frames[idx];
                if frame.id.is_none() || frame.pin_count > 0 {
                    continue;
                }
                match frame.state {
                    FrameState::Cold => {
                        if frame.reference {
                            frame.reference = false;
                            promote_to_hot = true;
                        } else {
                            evict = true;
                        }
                    }
                    FrameState::Hot => {
                        if frame.reference {
                            frame.reference = false;
                        } else if inner.cold_count < inner.target_cold {
                            convert_to_cold = true;
                        }
                    }
                    FrameState::Test => {}
                }
            }
            if promote_to_hot {
                inner.set_frame_state(idx, FrameState::Hot);
                continue;
            }
            if convert_to_cold {
                inner.set_frame_state(idx, FrameState::Cold);
                continue;
            }
            if evict {
                self.evict_frame(inner, idx)?;
                return Ok(());
            }
        }
        Err(SombraError::Invalid("no eviction candidate available"))
    }

    fn adjust_cold_balance(&self, inner: &mut PagerInner) {
        while inner.cold_count > inner.target_cold {
            if !self.promote_cold_to_hot(inner) {
                break;
            }
        }
        while inner.cold_count < inner.target_cold {
            if !self.demote_hot_to_cold(inner) {
                break;
            }
        }
    }

    fn promote_cold_to_hot(&self, inner: &mut PagerInner) -> bool {
        if inner.cold_count == 0 {
            return false;
        }
        let len = inner.frames.len();
        for _ in 0..len {
            let idx = inner.clock_hand_hot;
            inner.clock_hand_hot = (inner.clock_hand_hot + 1) % len;
            let should_promote = {
                let frame = &inner.frames[idx];
                frame.id.is_some() && frame.pin_count == 0 && frame.state == FrameState::Cold
            };
            if should_promote {
                inner.set_frame_state(idx, FrameState::Hot);
                return true;
            }
        }
        false
    }

    fn demote_hot_to_cold(&self, inner: &mut PagerInner) -> bool {
        if inner.hot_count == 0 {
            return false;
        }
        let len = inner.frames.len();
        for _ in 0..len * 2 {
            let idx = inner.clock_hand_hot;
            inner.clock_hand_hot = (inner.clock_hand_hot + 1) % len;
            let should_demote = {
                let frame = &mut inner.frames[idx];
                if frame.id.is_none() || frame.pin_count > 0 {
                    continue;
                }
                match frame.state {
                    FrameState::Hot => {
                        if frame.reference {
                            frame.reference = false;
                            continue;
                        }
                        true
                    }
                    _ => continue,
                }
            };
            if should_demote {
                inner.set_frame_state(idx, FrameState::Cold);
                return true;
            }
        }
        false
    }

    fn commit_txn(&self, mut guard: WriteGuard<'_>) -> Result<Lsn> {
        let _scope = profile_scope(StorageProfileKind::PagerCommit);
        let mut inner = self.inner.lock();
        let (lsn, newly_reserved) = match guard.commit_lsn {
            Some(lsn) => (lsn, false),
            None => {
                let lsn = inner.next_lsn;
                inner.next_lsn = Lsn(lsn.0.saturating_add(1));
                guard.commit_lsn = Some(lsn);
                (lsn, true)
            }
        };
        if newly_reserved {
            self.promote_intent(guard.intent_id, lsn)?;
        }
        pager_test_log!(
            "[pager.commit] start lsn={} dirty_pages={} meta_dirty={}",
            lsn.0,
            guard.dirty_pages.len(),
            inner.meta_dirty
        );
        debug!(
            lsn = lsn.0,
            dirty_pages = guard.dirty_pages.len(),
            meta_dirty = inner.meta_dirty,
            "pager.commit_txn.start"
        );
        let mut dirty_pages: Vec<PageId> = guard.dirty_pages.iter().copied().collect();
        if inner.meta_dirty && !dirty_pages.iter().any(|p| p.0 == 0) {
            dirty_pages.push(PageId(0));
        }
        dirty_pages.sort_by_key(|p| p.0);
        dirty_pages.dedup();

        let mut wal_frames: Vec<PendingWalFrame> = Vec::with_capacity(dirty_pages.len());
        let dirty_page_count = dirty_pages.len();
        let mut borrowed_bytes = 0u64;
        for page_id in dirty_pages {
            if page_id.0 == 0 {
                let mut payload = vec![0u8; self.page_size];
                write_meta_page(&mut payload, &inner.meta)?;
                wal_frames.push(PendingWalFrame {
                    lsn,
                    page_id,
                    payload: PendingPayload::Owned(payload),
                });
                continue;
            }
            let idx = match inner.page_table.get(&page_id) {
                Some(&idx) => idx,
                None => {
                    return Err(SombraError::Invalid("page not cached"));
                }
            };
            let buf_guard = inner.frames[idx].buf.write_arc();
            let mut buf_guard = buf_guard;
            page::clear_crc32(&mut buf_guard[..PAGE_HDR_LEN])?;
            let crc = page_crc32(page_id.0, inner.meta.salt, &buf_guard);
            buf_guard[page::header::CRC32].copy_from_slice(&crc.to_be_bytes());
            let lease_guard = ArcRwLockWriteGuard::downgrade(buf_guard);
            inner.frames[idx].pending_checkpoint = true;
            wal_frames.push(PendingWalFrame {
                lsn,
                page_id,
                payload: PendingPayload::Borrowed(PageImageLease { guard: lease_guard }),
            });
            inner.frames[idx].dirty = false;
            inner.stats.dirty_writebacks += 1;
            borrowed_bytes = borrowed_bytes.saturating_add(self.page_size as u64);
        }
        let borrowed_frames = wal_frames
            .iter()
            .filter(|frame| frame.payload.is_borrowed())
            .count();
        pager_test_log!(
            "[pager.commit] frames built lsn={} frames={} borrowed_frames={}",
            lsn.0,
            wal_frames.len(),
            borrowed_frames
        );
        debug!(
            lsn = lsn.0,
            wal_frames = wal_frames.len(),
            borrowed_frames,
            dirty_pages = dirty_page_count,
            "pager.commit_txn.frames_built"
        );
        let version_targets: Vec<Option<(PageId, Lsn)>> = wal_frames
            .iter()
            .map(|frame| {
                if frame.page_id.0 == 0 {
                    None
                } else {
                    Some((frame.page_id, frame.lsn))
                }
            })
            .collect();
        self.cache_overlays(&wal_frames);
        drop(inner);
        let wal_frame_count = wal_frames.len() as u64;
        if wal_frame_count > 0 {
            record_pager_wal_frames(wal_frame_count);
            let payload_bytes = wal_frame_count.saturating_mul(self.page_size as u64);
            record_pager_wal_bytes(payload_bytes);
            if borrowed_bytes > 0 {
                record_pager_commit_borrowed_bytes(borrowed_bytes.min(payload_bytes));
            }
        }

        let (synchronous, async_fsync) = {
            let options = self.options.lock();
            (options.synchronous, options.async_fsync)
        };
        let sync_mode = match (synchronous, async_fsync) {
            (Synchronous::Full, true) => WalSyncMode::Deferred,
            (Synchronous::Full, false) => WalSyncMode::Immediate,
            (Synchronous::Normal, _) => WalSyncMode::Deferred,
            (Synchronous::Off, _) => WalSyncMode::Off,
        };
        let has_borrowed = wal_frames.iter().any(|frame| frame.payload.is_borrowed());
        if has_borrowed {
            pager_test_log!(
                "[pager.commit] flushing borrowed frames lsn={} frames={}",
                lsn.0,
                wal_frames.len()
            );
            debug!(
                lsn = lsn.0,
                frames = wal_frames.len(),
                "pager.commit_txn.borrowed_flush_start"
            );
            let offsets = self.flush_pending_wal_frames(&wal_frames, sync_mode)?;
            if !offsets.is_empty() {
                self.attach_version_offsets(&version_targets, &offsets);
                self.release_version_payloads_for_floor();
            }
            pager_test_log!(
                "[pager.commit] flush complete lsn={} frames={}",
                lsn.0,
                wal_frames.len()
            );
            debug!(
                lsn = lsn.0,
                frames = wal_frames.len(),
                "pager.commit_txn.borrowed_flush_complete"
            );
            guard.release_writer_lock();
            pager_test_log!(
                "[pager.commit] writer lock released (borrowed) lsn={}",
                lsn.0
            );
            debug!(lsn = lsn.0, "pager.commit_txn.writer_lock_released");
            self.finalize_commit(lsn)?;
            self.record_committed_lsn(lsn);
            match synchronous {
                Synchronous::Full if async_fsync => self.schedule_async_fsync(lsn)?,
                Synchronous::Full => self.mark_commit_durable(lsn)?,
                Synchronous::Normal => self.schedule_normal_sync(lsn)?,
                Synchronous::Off => self.record_durable_state(lsn)?,
            }
            guard.committed = true;
            drop(guard);
            pager_test_log!("[pager.commit] borrowed path done lsn={}", lsn.0);
            debug!(lsn = lsn.0, "pager.commit_txn.borrowed_commit_done");
            drop(wal_frames);
            self.maybe_autocheckpoint()?;
            return Ok(lsn);
        }
        let owned_frames: Vec<WalFrameOwned> = wal_frames
            .into_iter()
            .map(|frame| WalFrameOwned {
                lsn: frame.lsn,
                page_id: frame.page_id,
                payload: frame.payload.into_owned(),
            })
            .collect();
        pager_test_log!(
            "[pager.commit] enqueue owned frames lsn={} frames={}",
            lsn.0,
            owned_frames.len()
        );
        debug!(
            lsn = lsn.0,
            frames = owned_frames.len(),
            "pager.commit_txn.enqueue_start"
        );
        let ticket = self.wal_committer.enqueue(owned_frames, sync_mode);
        guard.release_writer_lock();
        pager_test_log!(
            "[pager.commit] writer lock released lsn={} ticket_present={}",
            lsn.0,
            ticket.is_some()
        );
        debug!(
            lsn = lsn.0,
            has_ticket = ticket.is_some(),
            "pager.commit_txn.writer_lock_released"
        );
        let (commit_result, offsets) = match ticket {
            Some(waiter) => {
                pager_test_log!("[pager.commit] waiting on WAL ticket lsn={}", lsn.0);
                debug!(lsn = lsn.0, "pager.commit_txn.waiting_on_ticket");
                let result = waiter.wait();
                if let Err(err) = &result {
                    pager_test_log!("[pager.commit] ticket error lsn={} err={}", lsn.0, err);
                    debug!(lsn = lsn.0, error = %err, "pager.commit_txn.ticket_error");
                } else {
                    pager_test_log!("[pager.commit] ticket done lsn={}", lsn.0);
                    debug!(lsn = lsn.0, "pager.commit_txn.ticket_done");
                }
                match result {
                    Ok(offsets) => (Ok(()), offsets),
                    Err(err) => (Err(err), Vec::new()),
                }
            }
            None => {
                pager_test_log!("[pager.commit] no ticket needed lsn={}", lsn.0);
                debug!(lsn = lsn.0, "pager.commit_txn.no_ticket_needed");
                (Ok(()), Vec::new())
            }
        };
        if let Err(err) = commit_result {
            guard.reacquire_writer_lock()?;
            return Err(err);
        }
        self.finalize_commit(lsn)?;
        self.record_committed_lsn(lsn);
        match synchronous {
            Synchronous::Full if async_fsync => self.schedule_async_fsync(lsn)?,
            Synchronous::Full => self.mark_commit_durable(lsn)?,
            Synchronous::Normal => self.schedule_normal_sync(lsn)?,
            Synchronous::Off => self.record_durable_state(lsn)?,
        }
        if !offsets.is_empty() {
            self.attach_version_offsets(&version_targets, &offsets);
            self.release_version_payloads_for_floor();
        }
        guard.committed = true;
        drop(guard);
        pager_test_log!("[pager.commit] commit complete lsn={}", lsn.0);
        debug!(lsn = lsn.0, "pager.commit_txn.commit_complete");
        self.maybe_autocheckpoint()?;
        Ok(lsn)
    }

    fn flush_pending_wal_frames(
        &self,
        frames: &[PendingWalFrame],
        sync_mode: WalSyncMode,
    ) -> Result<Vec<WalFramePtr>> {
        if frames.is_empty() {
            return Ok(Vec::new());
        }
        let mut refs: Vec<WalFrame<'_>> = Vec::with_capacity(frames.len());
        for frame in frames {
            refs.push(WalFrame {
                lsn: frame.lsn,
                page_id: frame.page_id,
                payload: frame.payload.as_slice(),
            });
        }
        let offsets = self.wal.append_frame_batch(&refs)?;
        if matches!(sync_mode, WalSyncMode::Immediate) {
            self.wal.sync()?;
        }
        Ok(offsets)
    }

    fn run_checkpoint(&self, mode: CheckpointMode) -> Result<()> {
        pager_test_log!("[pager.checkpoint] start mode={:?}", mode);
        debug!(mode = ?mode, "pager.run_checkpoint.start");
        let checkpoint_guard = match mode {
            CheckpointMode::Force => loop {
                if let Some(guard) = self.locks.try_acquire_checkpoint()? {
                    pager_test_log!("[pager.checkpoint] force guard acquired mode={:?}", mode);
                    debug!(mode = ?mode, "pager.run_checkpoint.force_acquired");
                    break guard;
                }
                pager_test_log!("[pager.checkpoint] waiting for force guard mode={:?}", mode);
                debug!(
                    mode = ?mode,
                    "pager.run_checkpoint.force_waiting_for_guard"
                );
                std::thread::sleep(Duration::from_millis(10));
            },
            CheckpointMode::BestEffort => match self.locks.try_acquire_checkpoint()? {
                Some(guard) => {
                    pager_test_log!(
                        "[pager.checkpoint] best-effort guard acquired mode={:?}",
                        mode
                    );
                    debug!(mode = ?mode, "pager.run_checkpoint.best_effort_acquired");
                    guard
                }
                None => {
                    pager_test_log!(
                        "[pager.checkpoint] best-effort guard unavailable mode={:?}",
                        mode
                    );
                    debug!(
                        mode = ?mode,
                        "pager.run_checkpoint.best_effort_guard_unavailable"
                    );
                    info!(
                        mode = ?mode,
                        "pager.checkpoint.skip_guard_unavailable"
                    );
                    return Ok(());
                }
            },
        };
        let result = self.perform_checkpoint();
        drop(checkpoint_guard);
        pager_test_log!(
            "[pager.checkpoint] guard released mode={:?} success={}",
            mode,
            result.is_ok()
        );
        match &result {
            Ok(()) => debug!(mode = ?mode, "pager.run_checkpoint.complete"),
            Err(err) => debug!(mode = ?mode, error = %err, "pager.run_checkpoint.error"),
        }
        result
    }

    fn perform_checkpoint(&self) -> Result<()> {
        pager_test_log!("[pager.checkpoint] perform begin");
        let reader_snapshot = {
            let table = self.commit_table.lock();
            table.reader_snapshot(Instant::now())
        };
        let mut inner = self.inner.lock();
        pager_test_log!(
            "[pager.checkpoint] meta before checkpoint last_lsn={}",
            inner.meta.last_checkpoint_lsn.0
        );
        let mut iter = self.wal.iter()?;
        let mut frames = Vec::new();
        let mut max_lsn = inner.meta.last_checkpoint_lsn;
        while let Some(frame) = iter.next_frame()? {
            if frame.lsn.0 <= inner.meta.last_checkpoint_lsn.0 {
                continue;
            }
            if frame.lsn.0 > max_lsn.0 {
                max_lsn = frame.lsn;
            }
            frames.push(frame);
        }
        pager_test_log!("[pager.checkpoint] frames collected={}", frames.len());
        debug!(
            pending_frames = frames.len(),
            last_checkpoint_lsn = inner.meta.last_checkpoint_lsn.0,
            "pager.perform_checkpoint.frames_collected"
        );
        info!(
            pending_frames = frames.len(),
            last_checkpoint_lsn = inner.meta.last_checkpoint_lsn.0,
            reader_count = reader_snapshot.active,
            reader_oldest_commit = reader_snapshot
                .oldest_snapshot
                .unwrap_or(inner.meta.last_checkpoint_lsn.0),
            reader_max_age_ms = reader_snapshot.max_age_ms,
            "pager.checkpoint.plan"
        );
        if frames.is_empty() {
            let _ = self.wal.recycle_active_segments()?;
            self.wal.reset(Lsn(inner.meta.last_checkpoint_lsn.0 + 1))?;
            inner.next_lsn = Lsn(inner.meta.last_checkpoint_lsn.0 + 1);
            pager_test_log!("[pager.checkpoint] no frames; reset wal");
            debug!("pager.perform_checkpoint.no_frames");
            info!(
                last_checkpoint_lsn = inner.meta.last_checkpoint_lsn.0,
                "pager.checkpoint.skip_no_frames"
            );
            return Ok(());
        }
        for (idx, frame) in frames.iter().enumerate() {
            let payload = frame.payload.as_slice();
            let offset = page_offset(frame.page_id, self.page_size);
            pager_test_log!(
                "[pager.checkpoint] applying frame {:?} ({}/{})",
                frame.page_id,
                idx + 1,
                frames.len()
            );
            pager_test_log!(
                "[pager.checkpoint] writing page {:?} at offset {}",
                frame.page_id,
                offset
            );
            self.db_io.write_at(offset, payload)?;
            pager_test_log!("[pager.checkpoint] write complete page {:?}", frame.page_id);
            if let Some(&frame_idx) = inner.page_table.get(&frame.page_id) {
                pager_test_log!(
                    "[pager.checkpoint] refreshing cached frame {:?} idx={}",
                    frame.page_id,
                    frame_idx
                );
                {
                    let mut buf = inner.frames[frame_idx].buf.write();
                    buf.copy_from_slice(payload);
                }
                pager_test_log!(
                    "[pager.checkpoint] cached frame refreshed {:?}",
                    frame.page_id
                );
                inner.frames[frame_idx].dirty = false;
                inner.frames[frame_idx].pending_checkpoint = false;
                inner.frames[frame_idx].newly_allocated = false;
                inner.frames[frame_idx].needs_refresh = true;
            }
            pager_test_log!(
                "[pager.checkpoint] frame {:?} applied ({}/{})",
                frame.page_id,
                idx + 1,
                frames.len()
            );
        }
        self.rebuild_freelist(&mut inner)?;
        pager_test_log!("[pager.checkpoint] freelist rebuilt");
        pager_test_log!("[pager.checkpoint] syncing db file (data pages)");
        self.db_io.sync_all()?;
        pager_test_log!("[pager.checkpoint] db file synced (data pages)");
        inner.meta.last_checkpoint_lsn = max_lsn;
        let mut meta_buf = vec![0u8; self.page_size];
        write_meta_page(&mut meta_buf, &inner.meta)?;
        self.db_io.write_at(0, &meta_buf)?;
        self.db_io.sync_all()?;
        pager_test_log!("[pager.checkpoint] meta page written+synced");
        let _ = self.wal.recycle_active_segments()?;
        self.wal.reset(Lsn(inner.meta.last_checkpoint_lsn.0 + 1))?;
        inner.next_lsn = Lsn(inner.meta.last_checkpoint_lsn.0 + 1);
        inner.meta_dirty = false;
        let prune_lsn =
            self.reader_prune_threshold(inner.meta.last_checkpoint_lsn, &reader_snapshot);
        self.release_commits_up_to(prune_lsn);
        self.prune_overlays(prune_lsn);
        self.prune_version_chains(prune_lsn);
        *self.last_autocheckpoint.lock() = Some(Instant::now());
        pager_test_log!(
            "[pager.checkpoint] complete applied_frames={} new_last_lsn={}",
            frames.len(),
            inner.meta.last_checkpoint_lsn.0
        );
        debug!(
            applied_frames = frames.len(),
            new_last_checkpoint_lsn = inner.meta.last_checkpoint_lsn.0,
            "pager.perform_checkpoint.applied"
        );
        info!(
            applied_frames = frames.len(),
            new_last_checkpoint_lsn = inner.meta.last_checkpoint_lsn.0,
            reader_count = reader_snapshot.active,
            reader_max_age_ms = reader_snapshot.max_age_ms,
            "pager.checkpoint.applied"
        );
        Ok(())
    }

    fn schedule_normal_sync(&self, target: Lsn) -> Result<()> {
        let state_arc = Arc::clone(&self.wal_sync_state);
        {
            let mut state = state_arc.lock();
            if let Some(err) = state.last_error.take() {
                return Err(err);
            }
            if target.0 > state.pending_lsn.0 {
                state.pending_lsn = target;
            }
            if state.scheduled {
                return Ok(());
            }
            state.scheduled = true;
        }
        let wal = Arc::clone(&self.wal);
        let durable = Arc::clone(&self.durable_lsn);
        let wal_cookie = self.wal_cookie.clone();
        let commit_table = Arc::clone(&self.commit_table);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(NORMAL_SYNC_DELAY_MS));
            let result = wal.sync().and_then(|_| {
                let lsn = {
                    let state = state_arc.lock();
                    state.pending_lsn
                };
                if let Some(cookie) = wal_cookie.as_ref() {
                    cookie.persist(lsn)?;
                }
                {
                    let mut table = commit_table.lock();
                    table.mark_durable_up_to(lsn.0)?;
                }
                durable.fetch_max(lsn.0, AtomicOrdering::Release);
                Ok(())
            });
            let mut state = state_arc.lock();
            state.scheduled = false;
            match result {
                Ok(()) => {}
                Err(err) => {
                    state.last_error = Some(err);
                }
            }
        });
        Ok(())
    }

    fn schedule_async_fsync(&self, target: Lsn) -> Result<()> {
        let Some(state_arc) = self.async_fsync_state.as_ref() else {
            self.durable_lsn
                .fetch_max(target.0, AtomicOrdering::Release);
            if let Some(cookie) = &self.wal_cookie {
                cookie.persist(target)?;
            }
            return Ok(());
        };
        let coalesce_wait = {
            let opts = self.options.lock();
            Duration::from_millis(opts.async_fsync_max_wait_ms.min(10_000))
        };
        let wal = Arc::clone(&self.wal);
        let cookie = self.wal_cookie.clone();
        let durable = Arc::clone(&self.durable_lsn);
        let state_clone = Arc::clone(state_arc);
        let mut spawn_worker = false;
        {
            let mut state = state_arc.lock();
            if let Some(err) = state.last_error.take() {
                return Err(err);
            }
            if target.0 > state.pending_lsn.0 {
                state.pending_lsn = target;
            }
            if !state.scheduled {
                state.scheduled = true;
                spawn_worker = true;
            }
        }
        if spawn_worker {
            let commit_table = Arc::clone(&self.commit_table);
            thread::spawn(move || {
                async_fsync_worker(
                    wal,
                    cookie,
                    commit_table,
                    state_clone,
                    durable,
                    coalesce_wait,
                )
            });
        }
        Ok(())
    }

    fn maybe_autocheckpoint(&self) -> Result<()> {
        let (autocheckpoint_pages, autocheckpoint_ms) = {
            let options = self.options.lock();
            (options.autocheckpoint_pages, options.autocheckpoint_ms)
        };
        let mut should_checkpoint = false;
        let mut pages_triggered = false;
        let mut timer_triggered = false;
        if autocheckpoint_pages > 0 {
            let wal_len = self.wal.len()?;
            let threshold = (autocheckpoint_pages as u64).saturating_mul(self.page_size as u64);
            if wal_len >= threshold {
                should_checkpoint = true;
                pages_triggered = true;
                pager_test_log!(
                    "[pager.autockpt] wal_len {} >= threshold {} (pages)",
                    wal_len,
                    threshold
                );
                debug!(
                    wal_len,
                    threshold, "pager.autocheckpoint.pages_threshold_met"
                );
            } else {
                pager_test_log!(
                    "[pager.autockpt] wal_len {} < threshold {} (pages)",
                    wal_len,
                    threshold
                );
                debug!(
                    wal_len,
                    threshold, "pager.autocheckpoint.pages_below_threshold"
                );
            }
        }
        if let Some(ms) = autocheckpoint_ms {
            let mut last = self.last_autocheckpoint.lock();
            match *last {
                Some(prev) if prev.elapsed() >= Duration::from_millis(ms) => {
                    should_checkpoint = true;
                    timer_triggered = true;
                    pager_test_log!(
                        "[pager.autockpt] timer expired elapsed={}ms threshold={}ms",
                        prev.elapsed().as_millis(),
                        ms
                    );
                    debug!(
                        elapsed_ms = prev.elapsed().as_millis() as u64,
                        ms, "pager.autocheckpoint.timer_triggered"
                    );
                }
                None => {
                    *last = Some(Instant::now());
                    pager_test_log!("[pager.autockpt] timer armed for {}ms", ms);
                    debug!(ms, "pager.autocheckpoint.timer_armed");
                }
                _ => {}
            }
        }
        if should_checkpoint {
            pager_test_log!(
                "[pager.autockpt] requesting checkpoint pages_triggered={} timer_triggered={}",
                pages_triggered,
                timer_triggered
            );
            debug!(
                pages_triggered,
                timer_triggered, "pager.autocheckpoint.requesting_checkpoint"
            );
            let _ = self.run_checkpoint(CheckpointMode::BestEffort);
            *self.last_autocheckpoint.lock() = Some(Instant::now());
        } else {
            pager_test_log!(
                "[pager.autockpt] no checkpoint needed pages_triggered={} timer_triggered={}",
                pages_triggered,
                timer_triggered
            );
            debug!(
                pages_triggered,
                timer_triggered, "pager.autocheckpoint.no_checkpoint_needed"
            );
        }
        let elapsed_since_last = {
            let last = self.last_autocheckpoint.lock();
            last.as_ref().map(|inst| inst.elapsed()).unwrap_or_default()
        };
        let ctx = AutockptContext {
            last_checkpoint_lsn: self.last_checkpoint_lsn(),
            elapsed_since_last,
        };
        self.notify_background_hooks(&ctx);
        Ok(())
    }

    fn record_committed_lsn(&self, lsn: Lsn) {
        self.latest_visible_lsn
            .fetch_max(lsn.0, AtomicOrdering::Release);
    }

    /// Returns the most recent LSN whose commit finished.
    pub fn latest_committed_lsn(&self) -> Lsn {
        Lsn(self.latest_visible_lsn.load(AtomicOrdering::Acquire))
    }

    /// Returns the most recent LSN known to be durable on disk.
    pub fn durable_lsn(&self) -> Lsn {
        Lsn(self.durable_lsn.load(AtomicOrdering::Acquire))
    }

    fn record_durable_state(&self, lsn: Lsn) -> Result<()> {
        self.durable_lsn.fetch_max(lsn.0, AtomicOrdering::Release);
        let mut table = self.commit_table.lock();
        table.mark_durable_up_to(lsn.0)
    }

    fn mark_commit_durable(&self, lsn: Lsn) -> Result<()> {
        self.record_durable_state(lsn)?;
        if let Some(cookie) = &self.wal_cookie {
            cookie.persist(lsn)?;
        }
        Ok(())
    }

    fn promote_intent(&self, intent: IntentId, lsn: Lsn) -> Result<()> {
        let mut table = self.commit_table.lock();
        table.promote_intent(intent, lsn.0)
    }

    fn finalize_commit(&self, lsn: Lsn) -> Result<()> {
        let mut table = self.commit_table.lock();
        table.mark_committed(lsn.0)
    }

    fn release_commits_up_to(&self, lsn: Lsn) {
        let mut table = self.commit_table.lock();
        table.release_committed(lsn.0);
    }

    fn notify_background_hooks(&self, ctx: &AutockptContext) {
        let mut hooks = self.background_hooks.lock();
        let mut callbacks: Vec<Arc<dyn BackgroundMaintainer>> = Vec::new();
        hooks.retain(|weak| {
            if let Some(hook) = weak.upgrade() {
                callbacks.push(hook);
                true
            } else {
                false
            }
        });
        drop(hooks);
        for hook in callbacks {
            hook.run_background_maint(ctx);
        }
    }

    fn cache_overlays(&self, frames: &[PendingWalFrame]) {
        if frames.is_empty() {
            return;
        }
        let mut overlays = self.overlays.lock();
        let mut chains = self.version_chains.lock();
        for frame in frames {
            if frame.page_id.0 == 0 {
                continue;
            }
            let data = Arc::<[u8]>::from(frame.payload.as_slice());
            overlays
                .entry(frame.page_id)
                .or_default()
                .push_back(OverlayEntry {
                    lsn: frame.lsn,
                    data: Arc::clone(&data),
                });
            self.insert_version_chain_locked(
                &mut chains,
                frame.page_id,
                frame.lsn,
                None,
                Some(data),
            );
        }
    }

    fn insert_version_chain_locked(
        &self,
        chains: &mut HashMap<PageId, VecDeque<VersionChainEntry>>,
        page_id: PageId,
        lsn: Lsn,
        wal_offset: Option<WalFramePtr>,
        data: Option<Arc<[u8]>>,
    ) {
        let chain = chains.entry(page_id).or_default();
        let was_empty = chain.is_empty();
        if let Some(last) = chain.back() {
            if last.lsn.0 > lsn.0 {
                // Ignore out-of-order versions.
                return;
            }
            if last.lsn.0 == lsn.0 {
                chain.pop_back();
                self.mvcc_version_count
                    .fetch_sub(1, AtomicOrdering::Relaxed);
            }
        }
        if was_empty {
            self.mvcc_version_pages
                .fetch_add(1, AtomicOrdering::Relaxed);
        }
        chain.push_back(VersionChainEntry {
            lsn,
            wal_offset,
            data,
        });
        self.mvcc_version_count
            .fetch_add(1, AtomicOrdering::Relaxed);
    }

    fn attach_version_offsets(&self, targets: &[Option<(PageId, Lsn)>], offsets: &[WalFramePtr]) {
        let mut chains = self.version_chains.lock();
        for (meta, offset) in targets.iter().zip(offsets.iter()) {
            let Some((page_id, lsn)) = meta else {
                continue;
            };
            if let Some(entries) = chains.get_mut(page_id) {
                if let Some(entry) = entries.iter_mut().rev().find(|entry| entry.lsn == *lsn) {
                    entry.wal_offset = Some(*offset);
                }
            }
        }
    }

    fn release_version_payloads_for_floor(&self) {
        let floor_commit = {
            let table = self.commit_table.lock();
            table.oldest_visible()
        };
        if floor_commit == 0 {
            return;
        }
        let upto = Lsn(floor_commit.saturating_sub(1));
        self.release_version_payloads(upto);
    }

    fn release_version_payloads(&self, upto: Lsn) {
        let mut chains = self.version_chains.lock();
        for entries in chains.values_mut() {
            for entry in entries.iter_mut() {
                if entry.lsn.0 <= upto.0 && entry.wal_offset.is_some() {
                    entry.data = None;
                }
            }
        }
    }

    fn overlay_from_cache(&self, page_id: PageId, snapshot_lsn: Lsn) -> Option<Arc<[u8]>> {
        let overlays = self.overlays.lock();
        overlays.get(&page_id).and_then(|queue| {
            queue
                .iter()
                .rev()
                .find(|entry| entry.lsn.0 <= snapshot_lsn.0)
                .map(|entry| Arc::clone(&entry.data))
        })
    }

    fn version_page_for_snapshot(&self, page_id: PageId, snapshot_lsn: Lsn) -> Option<Arc<[u8]>> {
        let candidate = {
            let chains = self.version_chains.lock();
            chains.get(&page_id).and_then(|entries| {
                entries
                    .iter()
                    .rev()
                    .find(|entry| entry.lsn.0 <= snapshot_lsn.0)
                    .map(|entry| {
                        (
                            entry.lsn,
                            entry.wal_offset,
                            entry.data.as_ref().map(Arc::clone),
                        )
                    })
            })
        };
        let Some((lsn, wal_offset, data)) = candidate else {
            return None;
        };
        if let Some(cached) = data {
            return Some(cached);
        }
        let ptr = wal_offset?;
        match self.read_wal_version(page_id, lsn, ptr) {
            Ok(data) => {
                self.backfill_version_data(page_id, lsn, Arc::clone(&data));
                Some(data)
            }
            Err(err) => {
                warn!(
                    page_id = page_id.0,
                    lsn = lsn.0,
                    segment_id = ptr.segment_id,
                    offset = ptr.offset,
                    error = %err,
                    "pager.version_chain.rehydrate_failed"
                );
                None
            }
        }
    }

    fn reader_prune_threshold(&self, checkpoint_lsn: Lsn, snapshot: &ReaderSnapshot) -> Lsn {
        let cutoff = snapshot
            .oldest_snapshot
            .map(|commit| {
                let floor = commit.saturating_sub(1);
                floor.min(checkpoint_lsn.0)
            })
            .unwrap_or(checkpoint_lsn.0);
        Lsn(cutoff)
    }

    fn read_wal_version(&self, page_id: PageId, lsn: Lsn, ptr: WalFramePtr) -> Result<Arc<[u8]>> {
        let Some(frame) = self.wal.read_frame_at(ptr)? else {
            return Err(SombraError::Corruption(
                "wal frame missing during version lookup",
            ));
        };
        if frame.page_id != page_id || frame.lsn != lsn {
            return Err(SombraError::Corruption(
                "wal frame metadata mismatch during version lookup",
            ));
        }
        Ok(Arc::<[u8]>::from(frame.payload))
    }

    fn backfill_version_data(&self, page_id: PageId, lsn: Lsn, data: Arc<[u8]>) {
        let mut chains = self.version_chains.lock();
        if let Some(entries) = chains.get_mut(&page_id) {
            if let Some(entry) = entries.iter_mut().rev().find(|entry| entry.lsn == lsn) {
                if entry.data.is_none() {
                    entry.data = Some(data);
                }
            }
        }
    }

    fn prune_overlays(&self, upto: Lsn) {
        let mut overlays = self.overlays.lock();
        overlays.retain(|_, queue| {
            while let Some(front) = queue.front() {
                if front.lsn.0 <= upto.0 {
                    queue.pop_front();
                } else {
                    break;
                }
            }
            !queue.is_empty()
        });
    }

    fn prune_version_chains(&self, upto: Lsn) {
        let mut chains = self.version_chains.lock();
        let mut removed_entries = 0u64;
        let mut removed_pages = 0u64;
        chains.retain(|_, entries| {
            while let Some(front) = entries.front() {
                if front.lsn.0 <= upto.0 {
                    entries.pop_front();
                    removed_entries += 1;
                } else {
                    break;
                }
            }
            if entries.is_empty() {
                removed_pages += 1;
                false
            } else {
                true
            }
        });
        if removed_entries > 0 {
            self.mvcc_version_count
                .fetch_sub(removed_entries, AtomicOrdering::Relaxed);
        }
        if removed_pages > 0 {
            self.mvcc_version_pages
                .fetch_sub(removed_pages, AtomicOrdering::Relaxed);
        }
    }

    fn overlay_page_for_snapshot(
        &self,
        page_id: PageId,
        checkpoint_lsn: Lsn,
        snapshot_lsn: Lsn,
    ) -> Result<Option<Arc<[u8]>>> {
        if snapshot_lsn.0 <= checkpoint_lsn.0 {
            return Ok(None);
        }
        if let Some(chain) = self.version_page_for_snapshot(page_id, snapshot_lsn) {
            return Ok(Some(chain));
        }
        if let Some(overlay) = self.overlay_from_cache(page_id, snapshot_lsn) {
            return Ok(Some(overlay));
        }
        let mut iter = self.wal.iter()?;
        let mut overlay: Option<Vec<u8>> = None;
        while let Some(frame) = iter.next_frame()? {
            if frame.lsn.0 <= checkpoint_lsn.0 || frame.lsn.0 > snapshot_lsn.0 {
                continue;
            }
            if frame.page_id == page_id {
                overlay = Some(frame.payload);
            }
        }
        Ok(overlay.map(Arc::<[u8]>::from))
    }

    fn begin_read_consistency(&self, consistency: ReadConsistency) -> Result<ReadGuard> {
        let lock = self.locks.acquire_reader()?;
        let snapshot_lsn = match consistency {
            ReadConsistency::Checkpoint => {
                let inner = self.inner.lock();
                inner.meta.last_checkpoint_lsn
            }
            ReadConsistency::LatestCommitted => self.latest_committed_lsn(),
        };
        let commit_reader = {
            let mut table = self.commit_table.lock();
            match table.register_reader(snapshot_lsn.0, Instant::now(), thread::current().id()) {
                Ok(reader) => reader,
                Err(err) => {
                    drop(lock);
                    return Err(err);
                }
            }
        };
        let metrics_handle = ReaderMetricsHandle::new(Arc::clone(&self.reader_metrics));
        Ok(ReadGuard {
            _lock: lock,
            snapshot_lsn,
            consistency,
            commit_table: Arc::clone(&self.commit_table),
            commit_reader,
            _metrics: metrics_handle,
        })
    }

    /// Begins a read transaction targeting the latest committed snapshot.
    pub fn begin_latest_committed_read(&self) -> Result<ReadGuard> {
        self.begin_read_consistency(ReadConsistency::LatestCommitted)
    }

    /// Begins a read transaction restricted to checkpoint durability.
    pub fn begin_checkpoint_read(&self) -> Result<ReadGuard> {
        self.begin_read_consistency(ReadConsistency::Checkpoint)
    }

    /// Returns the current metadata.
    pub fn meta(&self) -> Result<Meta> {
        let inner = self.inner.lock();
        Ok(inner.meta.clone())
    }

    #[cfg(test)]
    fn drop_version_payloads_for_test(&self) {
        let mut chains = self.version_chains.lock();
        for entries in chains.values_mut() {
            for entry in entries.iter_mut() {
                if entry.wal_offset.is_some() {
                    entry.data = None;
                }
            }
        }
    }

    #[cfg(test)]
    fn test_version_page_for_snapshot(
        &self,
        page_id: PageId,
        snapshot_lsn: Lsn,
    ) -> Option<Arc<[u8]>> {
        self.version_page_for_snapshot(page_id, snapshot_lsn)
    }
}

impl PageStore for Pager {
    fn page_size(&self) -> u32 {
        self.page_size as u32
    }

    fn get_page(&self, guard: &ReadGuard, id: PageId) -> Result<PageRef> {
        let mut cached: Option<Arc<[u8]>> = None;
        let mut refresh_idx: Option<usize> = None;
        let snapshot_lsn = guard.snapshot_lsn;
        let (salt, page_size, last_checkpoint_lsn) = {
            let mut inner = self.inner.lock();
            if let Some(&idx) = inner.page_table.get(&id) {
                let frame = &inner.frames[idx];
                let has_uncommitted = frame.dirty && !frame.newly_allocated;
                let needs_refresh = frame.needs_refresh;
                let snapshot_is_checkpoint = snapshot_lsn == inner.meta.last_checkpoint_lsn;
                let pending_checkpoint = frame.pending_checkpoint;
                if has_uncommitted {
                    inner.stats.misses += 1;
                } else if needs_refresh {
                    inner.stats.misses += 1;
                    refresh_idx = Some(idx);
                } else if pending_checkpoint && snapshot_is_checkpoint {
                    inner.stats.misses += 1;
                } else {
                    inner.stats.hits += 1;
                    let buf = inner.frames[idx].buf.read();
                    let mut copy = vec![0u8; self.page_size];
                    copy.copy_from_slice(&buf[..]);
                    cached = Some(Arc::<[u8]>::from(copy));
                }
            } else {
                inner.stats.misses += 1;
            }
            (
                inner.meta.salt,
                inner.meta.page_size,
                inner.meta.last_checkpoint_lsn,
            )
        };
        debug_assert!(
            snapshot_lsn.0 >= last_checkpoint_lsn.0,
            "snapshot regressed while reader active"
        );
        let mut buf = vec![0u8; self.page_size];
        let verify_crc = self.checksum_verify_on_read.load(AtomicOrdering::Relaxed);
        let read_result = self
            .db_io
            .read_at(page_offset(id, self.page_size), &mut buf);
        let fresh = match read_result {
            Ok(()) => false,
            Err(SombraError::Io(err)) if err.kind() == ErrorKind::UnexpectedEof => {
                buf.fill(0);
                true
            }
            Err(err) => return Err(err),
        };
        if let Some(idx) = refresh_idx {
            let mut inner = self.inner.lock();
            if let Some(frame) = inner.frames.get_mut(idx) {
                let mut guard_buf = frame.buf.write();
                guard_buf.copy_from_slice(&buf);
                frame.dirty = false;
                frame.pending_checkpoint = false;
                frame.newly_allocated = false;
                frame.needs_refresh = false;
            }
        }
        if !fresh {
            let header = PageHeader::decode(&buf[..PAGE_HDR_LEN])?;
            if header.page_no != id {
                return Err(SombraError::Corruption("page number mismatch"));
            }
            if header.page_size != page_size {
                return Err(SombraError::Corruption("page size mismatch"));
            }
            if verify_crc {
                let mut scratch = buf.clone();
                page::clear_crc32(&mut scratch[..PAGE_HDR_LEN])?;
                let crc = page_crc32(id.0, salt, &scratch);
                if crc != header.crc32 {
                    return Err(SombraError::Corruption("page crc mismatch"));
                }
            }
        }
        let latest_committed = self.latest_committed_lsn();
        if snapshot_lsn.0 < latest_committed.0 && snapshot_lsn.0 > last_checkpoint_lsn.0 {
            if let Some(overlay) =
                self.overlay_page_for_snapshot(id, last_checkpoint_lsn, snapshot_lsn)?
            {
                return Ok(PageRef { id, data: overlay });
            }
        }
        if let Some(data) = cached {
            return Ok(PageRef { id, data });
        }
        Ok(PageRef {
            id,
            data: Arc::from(buf),
        })
    }

    fn get_page_with_write(&self, _guard: &mut WriteGuard<'_>, id: PageId) -> Result<PageRef> {
        let data = {
            let mut inner = self.inner.lock();
            let (idx, hit) = self.lookup_or_load_frame(&mut inner, id)?;
            if hit {
                inner.stats.hits += 1;
            } else {
                inner.stats.misses += 1;
            }
            let buf = inner.frames[idx].buf.read();
            let mut copy = vec![0u8; self.page_size];
            copy.copy_from_slice(&buf[..]);
            Arc::<[u8]>::from(copy)
        };
        Ok(PageRef { id, data })
    }

    fn begin_read(&self) -> Result<ReadGuard> {
        self.begin_read_consistency(ReadConsistency::Checkpoint)
    }

    fn begin_latest_committed_read(&self) -> Result<ReadGuard> {
        self.begin_read_consistency(ReadConsistency::LatestCommitted)
    }

    fn begin_write(&self) -> Result<WriteGuard<'_>> {
        let lock = self.locks.acquire_writer()?;
        let inner = self.inner.lock();
        let intent_id = {
            let mut table = self.commit_table.lock();
            table.reserve_intent()
        };
        let guard = WriteGuard {
            pager: self,
            lock: Some(lock),
            dirty_pages: HashSet::new(),
            original_pages: HashMap::new(),
            allocated_pages: Vec::new(),
            freed_pages: Vec::new(),
            meta_snapshot: inner.meta.clone(),
            free_cache_snapshot: inner.free_cache.clone(),
            freelist_pages_snapshot: inner.freelist_pages.clone(),
            pending_free_snapshot: inner.pending_free.clone(),
            meta_dirty_snapshot: inner.meta_dirty,
            committed: false,
            commit_lsn: None,
            extensions: TxnExtensions::default(),
            intent_id,
        };
        drop(inner);
        Ok(guard)
    }

    fn commit(&self, guard: WriteGuard<'_>) -> Result<Lsn> {
        self.commit_txn(guard)
    }

    fn checkpoint(&self, mode: CheckpointMode) -> Result<()> {
        self.run_checkpoint(mode)
    }

    fn last_checkpoint_lsn(&self) -> Lsn {
        let inner = self.inner.lock();
        inner.meta.last_checkpoint_lsn
    }

    fn meta(&self) -> Result<Meta> {
        let inner = self.inner.lock();
        Ok(inner.meta.clone())
    }

    fn stats(&self) -> PagerStats {
        Pager::stats(self)
    }

    fn latest_committed_lsn(&self) -> Option<Lsn> {
        Some(Pager::latest_committed_lsn(self))
    }

    fn durable_lsn(&self) -> Option<Lsn> {
        Some(self.durable_lsn())
    }

    fn wal_commit_backlog(&self) -> Option<WalCommitBacklog> {
        Some(self.wal_committer.backlog())
    }

    fn wal_allocator_stats(&self) -> Option<WalAllocatorStats> {
        Some(Pager::wal_allocator_stats(self))
    }

    fn async_fsync_backlog(&self) -> Option<AsyncFsyncBacklog> {
        self.async_fsync_state.as_ref().map(|state| {
            let guard = state.lock();
            let pending_lag = guard.pending_lsn.0.saturating_sub(guard.durable_lsn.0);
            AsyncFsyncBacklog {
                pending_lsn: guard.pending_lsn,
                durable_lsn: guard.durable_lsn,
                pending_lag,
                last_error: guard.last_error.as_ref().map(|err| err.to_string()),
            }
        })
    }

    fn set_checksum_verification(&self, enabled: bool) {
        self.checksum_verify_on_read
            .store(enabled, AtomicOrdering::Relaxed);
    }

    fn checksum_verification_enabled(&self) -> bool {
        self.checksum_verify_on_read.load(AtomicOrdering::Relaxed)
    }

    fn commit_table(&self) -> Option<Arc<Mutex<CommitTable>>> {
        Some(Arc::clone(&self.commit_table))
    }

    fn maybe_background_maint(&self, ctx: &AutockptContext) {
        self.notify_background_hooks(ctx);
    }

    fn register_background_maint(&self, hook: Weak<dyn BackgroundMaintainer>) {
        self.background_hooks.lock().push(hook);
    }
}

fn page_offset(page: PageId, page_size: usize) -> u64 {
    page.0 * page_size as u64
}

fn pages_to_extents(pages: &[PageId]) -> Vec<Extent> {
    if pages.is_empty() {
        return Vec::new();
    }
    let mut sorted = pages.to_vec();
    sorted.sort_by_key(|p| p.0);
    let mut extents = Vec::new();
    let mut iter = sorted.into_iter();
    if let Some(first) = iter.next() {
        let mut current = Extent::new(first, 1);
        for page in iter {
            if page.0 == current.start.0 + current.len as u64 {
                current.len += 1;
            } else {
                extents.push(current);
                current = Extent::new(page, 1);
            }
        }
        extents.push(current);
    }
    extents
}

impl fmt::Display for PagerStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "hits={} misses={} evictions={} dirty_writebacks={}",
            self.hits, self.misses, self.evictions, self.dirty_writebacks
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::page::PageKind;
    use rand::Rng;
    use std::collections::HashSet;
    use std::fs::metadata;
    use std::sync::Once;
    use tempfile::tempdir;
    use tracing_subscriber::EnvFilter;

    fn write_test_payload(pager: &Pager, guard: &mut WriteGuard<'_>, id: PageId) -> Result<()> {
        let meta = pager.meta()?;
        let mut page = guard.page_mut(id)?;
        let buf = page.data_mut();
        let header =
            PageHeader::new(id, PageKind::BTreeLeaf, meta.page_size, meta.salt)?.with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        buf[PAGE_HDR_LEN..PAGE_HDR_LEN + 4].copy_from_slice(b"DATA");
        Ok(())
    }

    fn init_tracing() {
        static INIT: Once = Once::new();
        INIT.call_once(|| {
            let filter = EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| EnvFilter::new("pager=debug,sombra::primitives::wal=debug"));
            let _ = tracing_subscriber::fmt()
                .with_env_filter(filter)
                .with_writer(std::io::stderr)
                .with_ansi(false)
                .try_init();
        });
    }

    #[test]
    fn pager_create_open_roundtrip() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("stage2_roundtrip.db");
        let options = PagerOptions {
            page_size: 4096,
            cache_pages: 32,
            prefetch_on_miss: false,
            synchronous: Synchronous::Full,
            autocheckpoint_pages: 16,
            autocheckpoint_ms: None,
            ..PagerOptions::default()
        };
        let pager = Pager::create(&path, options.clone())?;
        let page = {
            let mut write = pager.begin_write()?;
            let page = write.allocate_page()?;
            write_test_payload(&pager, &mut write, page)?;
            pager.commit(write)?;
            page
        };
        pager.checkpoint(CheckpointMode::Force)?;
        assert!(metadata(&path).unwrap().len() >= pager.page_size() as u64);
        drop(pager);

        let pager = Pager::open(&path, options.clone())?;
        let read = pager.begin_read()?;
        let data = pager.get_page(&read, page)?;
        assert_eq!(&data.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4], b"DATA");
        Ok(())
    }

    #[test]
    fn version_chain_rehydrates_from_wal() -> Result<()> {
        init_tracing();
        let dir = tempdir().unwrap();
        let path = dir.path().join("rehydrate_wal.db");
        let options = PagerOptions {
            page_size: 4096,
            cache_pages: 8,
            prefetch_on_miss: false,
            synchronous: Synchronous::Full,
            autocheckpoint_pages: 32,
            autocheckpoint_ms: None,
            ..PagerOptions::default()
        };
        let pager = Pager::create(&path, options)?;
        let page_id = {
            let mut write = pager.begin_write()?;
            let page = write.allocate_page()?;
            write_test_payload(&pager, &mut write, page)?;
            pager.commit(write)?;
            page
        };
        let read_snapshot = pager.begin_latest_committed_read()?;
        let mut write = pager.begin_write()?;
        {
            let mut page = write.page_mut(page_id)?;
            page.data_mut()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4].copy_from_slice(b"NEWW");
        }
        pager.commit(write)?;
        pager.drop_version_payloads_for_test();
        {
            let chains = pager.version_chains.lock();
            let entries = chains.get(&page_id).expect("version chain present");
            assert!(
                entries
                    .iter()
                    .any(|entry| entry.lsn.0 == 1 && entry.wal_offset.is_some()),
                "expected wal offsets recorded for historical versions"
            );
        }
        assert!(
            pager
                .test_version_page_for_snapshot(page_id, Lsn(1))
                .is_some(),
            "version chain should produce historical page"
        );
        let versioned = pager.get_page(&read_snapshot, page_id)?;
        assert_eq!(&versioned.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4], b"DATA");

        let latest = pager.begin_latest_committed_read()?;
        let head = pager.get_page(&latest, page_id)?;
        assert_eq!(&head.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4], b"NEWW");
        Ok(())
    }

    #[test]
    fn read_guard_observes_checkpoint_snapshot() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("snapshot_read.db");
        let options = PagerOptions {
            page_size: 4096,
            cache_pages: 4,
            prefetch_on_miss: false,
            synchronous: Synchronous::Full,
            autocheckpoint_pages: 1024,
            autocheckpoint_ms: None,
            ..PagerOptions::default()
        };
        let pager = Pager::create(&path, options)?;
        let page = {
            let mut write = pager.begin_write()?;
            let page = write.allocate_page()?;
            write_test_payload(&pager, &mut write, page)?;
            pager.commit(write)?;
            page
        };
        pager.checkpoint(CheckpointMode::Force)?;

        let mut write = pager.begin_write()?;
        {
            let mut page_mut = write.page_mut(page)?;
            page_mut.data_mut()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4].copy_from_slice(b"NEW1");
        }

        let read_during_txn = pager.begin_read()?;
        let snapshot = pager.get_page(&read_during_txn, page)?;
        assert_eq!(
            &snapshot.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4],
            b"DATA",
            "reader should see checkpointed image during write txn"
        );
        drop(read_during_txn);

        pager.commit(write)?;

        let read_after_commit = pager.begin_read()?;
        let snapshot_after_commit = pager.get_page(&read_after_commit, page)?;
        assert_eq!(
            &snapshot_after_commit.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4],
            b"DATA",
            "reader should still see checkpoint snapshot until checkpoint runs"
        );
        drop(read_after_commit);

        pager.checkpoint(CheckpointMode::Force)?;

        let read_post_checkpoint = pager.begin_read()?;
        let snapshot_after_checkpoint = pager.get_page(&read_post_checkpoint, page)?;
        assert_eq!(
            &snapshot_after_checkpoint.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4],
            b"NEW1",
            "reader should observe new data after checkpoint"
        );
        Ok(())
    }

    #[test]
    fn latest_committed_read_includes_wal_overlay() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("latest_committed_overlay.db");
        let options = PagerOptions {
            page_size: 4096,
            cache_pages: 8,
            prefetch_on_miss: false,
            synchronous: Synchronous::Full,
            autocheckpoint_pages: usize::MAX,
            autocheckpoint_ms: None,
            ..PagerOptions::default()
        };
        let pager = Pager::create(&path, options)?;
        let page = {
            let mut write = pager.begin_write()?;
            let page = write.allocate_page()?;
            write_test_payload(&pager, &mut write, page)?;
            pager.commit(write)?;
            page
        };
        pager.checkpoint(CheckpointMode::Force)?;

        // Apply new data and commit without checkpointing.
        {
            let mut write = pager.begin_write()?;
            {
                let mut frame = write.page_mut(page)?;
                frame.data_mut()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4].copy_from_slice(b"WAL1");
            }
            pager.commit(write)?;
        }

        // Checkpoint-scoped read should still see the checkpoint image.
        let checkpoint_read = pager.begin_read()?;
        let checkpoint_page = pager.get_page(&checkpoint_read, page)?;
        assert_eq!(
            &checkpoint_page.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4],
            b"DATA"
        );
        drop(checkpoint_read);

        // Latest-committed reader must observe the WAL overlay.
        let latest_read = pager.begin_latest_committed_read()?;
        let latest_page = pager.get_page(&latest_read, page)?;
        assert_eq!(&latest_page.data()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4], b"WAL1");
        Ok(())
    }

    #[test]
    fn synchronous_normal_batches_syncs() -> Result<()> {
        let dir = tempdir().unwrap();
        let base_options = PagerOptions {
            cache_pages: 8,
            autocheckpoint_pages: usize::MAX,
            autocheckpoint_ms: None,
            ..PagerOptions::default()
        };

        let path_full = dir.path().join("sync_full.db");
        let mut options_full = base_options.clone();
        options_full.synchronous = Synchronous::Full;
        let pager_full = Pager::create(&path_full, options_full)?;
        for _ in 0..2 {
            let mut write = pager_full.begin_write()?;
            let _ = write.allocate_page()?;
            pager_full.commit(write)?;
        }
        assert_eq!(pager_full.wal.stats().syncs, 2);
        drop(pager_full);

        let path_normal = dir.path().join("sync_normal.db");
        let mut options_normal = base_options;
        options_normal.synchronous = Synchronous::Normal;
        let pager_normal = Pager::create(&path_normal, options_normal)?;
        for _ in 0..2 {
            let mut write = pager_normal.begin_write()?;
            let _ = write.allocate_page()?;
            pager_normal.commit(write)?;
        }
        for _ in 0..50 {
            if pager_normal.wal.stats().syncs >= 1 {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(pager_normal.wal.stats().syncs, 1);
        Ok(())
    }

    #[test]
    fn autocheckpoint_ms_triggers_checkpoint() -> Result<()> {
        init_tracing();
        let dir = tempdir().unwrap();
        let path = dir.path().join("auto_ms.db");
        let options = PagerOptions {
            cache_pages: 8,
            autocheckpoint_pages: usize::MAX,
            autocheckpoint_ms: Some(5),
            synchronous: Synchronous::Full,
            ..PagerOptions::default()
        };
        eprintln!(
            "[autockpt_ms] creating pager at {:?} with autocheckpoint_ms={:?}",
            path, options.autocheckpoint_ms
        );
        let pager = Pager::create(&path, options)?;
        eprintln!(
            "[autockpt_ms] pager created; last_checkpoint_lsn={:?}",
            pager.last_checkpoint_lsn()
        );
        let page = {
            eprintln!("[autockpt_ms] begin initial seed write");
            let mut write = pager.begin_write()?;
            eprintln!("[autockpt_ms] begin_write acquired (seed)");
            let page = write.allocate_page()?;
            eprintln!("[autockpt_ms] allocate_page returned {page:?}");
            write_test_payload(&pager, &mut write, page)?;
            eprintln!("[autockpt_ms] payload written to {page:?}");
            eprintln!("[autockpt_ms] committing seed transaction");
            pager.commit(write)?;
            eprintln!("[autockpt_ms] seed commit complete");
            eprintln!(
                "[autockpt_ms] seeded page {:?}; wal_stats={:?}",
                page,
                pager.wal.stats()
            );
            page
        };
        assert_eq!(pager.last_checkpoint_lsn(), Lsn(0));
        eprintln!("[autockpt_ms] sleeping to let timer elapse");
        thread::sleep(Duration::from_millis(15));
        eprintln!("[autockpt_ms] starting timed write");
        let mut write = pager.begin_write()?;
        eprintln!("[autockpt_ms] begin_write acquired (timer)");
        eprintln!("[autockpt_ms] modifying tracked page {page:?}");
        {
            let mut page_mut = write.page_mut(page)?;
            page_mut.data_mut()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4].copy_from_slice(b"TIME");
        }
        eprintln!("[autockpt_ms] page modification complete, committing");
        let lsn = pager.commit(write)?;
        eprintln!(
            "[autockpt_ms] commit complete lsn={:?}; last_checkpoint_lsn={:?}; wal_stats={:?}",
            lsn,
            pager.last_checkpoint_lsn(),
            pager.wal.stats()
        );
        assert_eq!(pager.last_checkpoint_lsn(), lsn);
        assert_eq!(pager.wal.stats().frames_appended, 0);
        Ok(())
    }

    #[test]
    fn pager_freelist_reuse() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("stage2_freelist.db");
        let options = PagerOptions {
            page_size: 4096,
            cache_pages: 32,
            prefetch_on_miss: false,
            synchronous: Synchronous::Full,
            autocheckpoint_pages: 16,
            autocheckpoint_ms: None,
            ..PagerOptions::default()
        };
        let pager = Pager::create(&path, options.clone())?;
        let (a, b, c) = {
            let mut write = pager.begin_write()?;
            let a = write.allocate_page()?;
            let b = write.allocate_page()?;
            let c = write.allocate_page()?;
            pager.commit(write)?;
            (a, b, c)
        };
        {
            let mut write = pager.begin_write()?;
            write.free_page(b)?;
            pager.commit(write)?;
        }
        pager.checkpoint(CheckpointMode::Force)?;
        let reused = {
            let mut write = pager.begin_write()?;
            let reused = write.allocate_page()?;
            pager.commit(write)?;
            reused
        };
        assert_eq!(reused, b);
        {
            let mut write = pager.begin_write()?;
            write.free_page(a)?;
            write.free_page(c)?;
            write.free_page(reused)?;
            pager.commit(write)?;
        }
        pager.checkpoint(CheckpointMode::Force)?;
        Ok(())
    }

    #[test]
    fn pager_random_workload() -> Result<()> {
        init_tracing();
        let dir = tempdir().unwrap();
        let path = dir.path().join("stage2_random.db");
        let options = PagerOptions {
            page_size: 4096,
            cache_pages: 32,
            prefetch_on_miss: false,
            synchronous: Synchronous::Full,
            autocheckpoint_pages: 16,
            autocheckpoint_ms: None,
            ..PagerOptions::default()
        };
        eprintln!(
            "[random_workload] starting test at {:?} with cache_pages={}",
            path, options.cache_pages
        );
        let pager = Pager::create(&path, options.clone())?;
        let mut rng = rand::thread_rng();
        let mut allocated = Vec::new();
        let mut max_page = PageId(0);

        for step in 0..500 {
            let do_alloc = allocated.is_empty() || rng.gen_bool(0.6);
            if do_alloc {
                eprintln!(
                    "[random_workload] step {step}: allocating (allocated_before={})",
                    allocated.len()
                );
                eprintln!("[random_workload] step {step}: begin_write (alloc path)");
                let mut write = pager.begin_write()?;
                eprintln!("[random_workload] step {step}: begin_write acquired (alloc path)");
                eprintln!("[random_workload] step {step}: calling allocate_page (alloc path)");
                let page = write.allocate_page()?;
                eprintln!("[random_workload] step {step}: allocate_page returned {page:?}");
                eprintln!("[random_workload] step {step}: writing payload to {page:?}");
                write_test_payload(&pager, &mut write, page)?;
                eprintln!("[random_workload] step {step}: payload write complete");
                eprintln!("[random_workload] step {step}: committing transaction (alloc path)");
                pager.commit(write)?;
                eprintln!("[random_workload] step {step}: commit complete (alloc path)");
                allocated.push(page);
                eprintln!(
                    "[random_workload] step {step}: allocated page {:?} (allocated_after={})",
                    page,
                    allocated.len()
                );
                if page.0 > max_page.0 {
                    max_page = page;
                }
            } else {
                let before = allocated.len();
                let idx = rng.gen_range(0..allocated.len());
                let page = allocated.swap_remove(idx);
                eprintln!(
                    "[random_workload] step {step}: freeing page {:?} (allocated_before={before}, remaining_after={})",
                    page,
                    allocated.len()
                );
                eprintln!("[random_workload] step {step}: begin_write (free path)");
                let mut write = pager.begin_write()?;
                eprintln!("[random_workload] step {step}: begin_write acquired (free path)");
                eprintln!("[random_workload] step {step}: freeing {page:?} inside txn");
                write.free_page(page)?;
                eprintln!("[random_workload] step {step}: page {page:?} marked free");
                eprintln!("[random_workload] step {step}: committing transaction (free path)");
                pager.commit(write)?;
                eprintln!("[random_workload] step {step}: commit complete (free path)");
                match pager.checkpoint(CheckpointMode::BestEffort) {
                    Ok(()) => eprintln!(
                        "[random_workload] step {step}: free-triggered checkpoint completed"
                    ),
                    Err(err) => eprintln!(
                        "[random_workload] step {step}: free-triggered checkpoint error: {err}"
                    ),
                }
            }
            if step % 50 == 0 {
                eprintln!(
                    "[random_workload] step {step}: running periodic checkpoint (allocated={})",
                    allocated.len()
                );
                match pager.checkpoint(CheckpointMode::BestEffort) {
                    Ok(()) => {
                        eprintln!("[random_workload] step {step}: periodic checkpoint completed")
                    }
                    Err(err) => {
                        eprintln!("[random_workload] step {step}: periodic checkpoint error: {err}")
                    }
                }
            }
        }

        for page in allocated.drain(..) {
            eprintln!("[random_workload] draining page {page:?}");
            let mut write = pager.begin_write()?;
            write.free_page(page)?;
            pager.commit(write)?;
        }
        pager.checkpoint(CheckpointMode::Force)?;
        eprintln!(
            "[random_workload] force checkpoint complete; wal_stats={:?}",
            pager.wal.stats()
        );
        assert!(metadata(&path).unwrap().len() >= pager.page_size() as u64);
        drop(pager);

        eprintln!("[random_workload] reopening pager");
        let pager = Pager::open(&path, options)?;
        let mut ids = Vec::new();
        for _ in 0..16 {
            let mut write = pager.begin_write()?;
            let page = write.allocate_page()?;
            pager.commit(write)?;
            ids.push(page);
            eprintln!("[random_workload] reopen allocated page {page:?}");
        }
        let set: HashSet<_> = ids.iter().copied().collect();
        assert_eq!(set.len(), ids.len());
        Ok(())
    }

    #[test]
    fn pager_runtime_synchronous_toggle() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime_sync.db");
        let mut options = PagerOptions::default();
        options.autocheckpoint_pages = usize::MAX;
        options.autocheckpoint_ms = None;
        let pager = Pager::create(&path, options)?;
        pager.set_synchronous(Synchronous::Off);
        let mut write = pager.begin_write()?;
        let page = write.allocate_page()?;
        write_test_payload(&pager, &mut write, page)?;
        pager.commit(write)?;
        assert_eq!(pager.wal.stats().syncs, 0);
        Ok(())
    }

    #[test]
    fn pager_runtime_wal_coalesce_toggle() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("runtime_coalesce.db");
        let pager = Pager::create(&path, PagerOptions::default())?;
        assert_eq!(pager.wal_coalesce_ms(), 2);
        pager.set_wal_coalesce_ms(10);
        assert_eq!(pager.wal_coalesce_ms(), 10);
        Ok(())
    }

    #[test]
    fn pager_allocate_extent_reuses_free_cache() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("extent_reuse.db");
        let mut options = PagerOptions::default();
        options.autocheckpoint_pages = usize::MAX;
        let pager = Pager::create(&path, options)?;
        let (page_a, page_b) = {
            let mut write = pager.begin_write()?;
            let a = write.allocate_page()?;
            let b = write.allocate_page()?;
            pager.commit(write)?;
            (a, b)
        };
        {
            let mut write = pager.begin_write()?;
            write.free_page(page_a)?;
            write.free_page(page_b)?;
            pager.commit(write)?;
        }
        pager.checkpoint(CheckpointMode::Force)?;
        let mut write = pager.begin_write()?;
        let extent = write.allocate_extent(2)?;
        assert_eq!(extent.start, page_a);
        assert_eq!(extent.len, 2);
        pager.commit(write)?;
        Ok(())
    }

    #[test]
    fn page_image_lease_blocks_writes_until_dropped() {
        use parking_lot::RwLock;

        let buf = Arc::new(RwLock::new(vec![0u8; 64].into_boxed_slice()));
        let write_guard = buf.write_arc();
        let lease = PageImageLease {
            guard: ArcRwLockWriteGuard::downgrade(write_guard),
        };
        assert!(
            buf.try_write_arc().is_none(),
            "write arc should be blocked while lease holds read lock"
        );
        drop(lease);
        assert!(
            buf.try_write_arc().is_some(),
            "write arc should succeed after lease drop"
        );
    }
}
