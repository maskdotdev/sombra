use std::any::{Any, TypeId};
use std::cmp::{max, min};
use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{lock_api::ArcRwLockWriteGuard, Mutex};
use std::io::ErrorKind;

use super::frame::{Frame, FrameState};
use super::freelist::{free_page_capacity, read_free_page, write_free_page, Extent, FreeCache};
use super::meta::{create_meta, load_meta, write_meta_page, Meta};
use crate::primitives::{
    concurrency::{ReaderGuard as LockReaderGuard, SingleWriter, WriterGuard as LockWriterGuard},
    io::{FileIo, StdFileIo},
    wal::{Wal, WalCommitConfig, WalCommitter, WalFrameOwned, WalOptions, WalSyncMode},
};
use crate::storage::{
    profile_scope, record_pager_wal_bytes, record_pager_wal_frames, StorageProfileKind,
};
use crate::types::{
    page::{self, PageHeader, PAGE_HDR_LEN},
    page_crc32, Lsn, PageId, Result, SombraError,
};

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
    /// Maximum number of commits to batch in WAL committer.
    pub wal_commit_max_commits: usize,
    /// Maximum number of frames to batch in WAL committer.
    pub wal_commit_max_frames: usize,
    /// Time in milliseconds to coalesce WAL commits.
    pub wal_commit_coalesce_ms: u64,
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
            wal_commit_max_commits: 32,
            wal_commit_max_frames: 512,
            wal_commit_coalesce_ms: 2,
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

fn wal_path(path: &Path) -> PathBuf {
    append_suffix(path, "-wal")
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

fn recover_database(
    wal: &Wal,
    db_io: &dyn FileIo,
    meta: &mut Meta,
    page_size: usize,
) -> Result<Lsn> {
    let mut iter = wal.iter()?;
    let mut frames = Vec::new();
    let mut max_lsn = meta.last_checkpoint_lsn;
    while let Some(frame) = iter.next_frame()? {
        if frame.payload.len() != page_size {
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
        wal.reset(Lsn(meta.last_checkpoint_lsn.0 + 1))?;
        return Ok(Lsn(meta.last_checkpoint_lsn.0 + 1));
    }
    for frame in &frames {
        let offset = page_offset(frame.page_id, page_size);
        db_io.write_at(offset, &frame.payload)?;
    }
    db_io.sync_all()?;
    meta.last_checkpoint_lsn = max_lsn;
    let mut meta_buf = vec![0u8; page_size];
    write_meta_page(&mut meta_buf, meta)?;
    db_io.write_at(0, &meta_buf)?;
    db_io.sync_all()?;
    let refreshed = load_meta(db_io, meta.page_size)?;
    *meta = refreshed;
    wal.reset(Lsn(meta.last_checkpoint_lsn.0 + 1))?;
    Ok(Lsn(meta.last_checkpoint_lsn.0 + 1))
}

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
}

/// Trait for page-oriented storage with transactional support.
#[allow(dead_code)]
pub trait PageStore: Send + Sync {
    /// Returns the page size in bytes.
    fn page_size(&self) -> u32;
    /// Retrieves a page within a read transaction.
    fn get_page(&self, guard: &ReadGuard, id: PageId) -> Result<PageRef>;
    /// Retrieves a page while holding a write transaction.
    fn get_page_with_write(&self, guard: &mut WriteGuard<'_>, id: PageId) -> Result<PageRef>;
    /// Begins a read transaction.
    fn begin_read(&self) -> Result<ReadGuard>;
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

    /// Enables or disables checksum verification on page reads.
    fn set_checksum_verification(&self, enabled: bool) {
        let _ = enabled;
    }

    /// Returns whether checksum verification is enabled.
    fn checksum_verification_enabled(&self) -> bool {
        true
    }
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
    guard: ArcRwLockWriteGuard<parking_lot::RawRwLock, Box<[u8]>>,
}

impl<'a> PageMut<'a> {
    /// Returns the page data as an immutable byte slice.
    pub fn data(&self) -> &[u8] {
        &self.guard
    }

    /// Returns the page data as a mutable byte slice.
    pub fn data_mut(&mut self) -> &mut [u8] {
        &mut self.guard
    }
}

impl<'a> Drop for PageMut<'a> {
    fn drop(&mut self) {
        self.pager.release_frame(self.frame_idx);
    }
}

/// Guard for a read transaction, holding a snapshot at a specific LSN.
pub struct ReadGuard {
    _lock: LockReaderGuard,
    snapshot_lsn: Lsn,
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
    extensions: TxnExtensions,
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
}

impl WalSyncState {
    fn new() -> Self {
        Self {
            scheduled: false,
            last_error: None,
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
    checksum_verify_on_read: AtomicBool,
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
            options.wal_commit_coalesce_ms = ms;
            Self::wal_commit_config_from_options(&*options)
        };
        self.wal_committer.set_config(config);
    }

    /// Returns the current WAL commit coalesce time in milliseconds.
    pub fn wal_coalesce_ms(&self) -> u64 {
        let options = self.options.lock();
        options.wal_commit_coalesce_ms
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
        let wal_path = wal_path(path);
        let wal_io = Arc::new(StdFileIo::open(&wal_path)?);
        let wal = Arc::new(Wal::open(
            wal_io,
            WalOptions::new(
                meta.page_size,
                meta.wal_salt,
                Lsn(meta.last_checkpoint_lsn.0 + 1),
            ),
        )?);
        let next_lsn = if is_create {
            wal.reset(Lsn(meta.last_checkpoint_lsn.0 + 1))?;
            Lsn(meta.last_checkpoint_lsn.0 + 1)
        } else {
            recover_database(wal.as_ref(), db_io.as_ref(), meta, meta.page_size as usize)?
        };
        let locks = SingleWriter::open(lock_path(path))?;
        let page_size = meta.page_size as usize;
        let cache_pages = options.cache_pages;
        let inner = PagerInner::new(meta.clone(), cache_pages, page_size, next_lsn);
        let wal_commit_config = Self::wal_commit_config_from_options(&options);
        let wal_committer = WalCommitter::new(Arc::clone(&wal), wal_commit_config);
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
            checksum_verify_on_read: AtomicBool::new(true),
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

    /// Returns a snapshot of current pager statistics.
    pub fn stats(&self) -> PagerStats {
        let state = self.inner.lock();
        state.stats.clone()
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
            .ok_or_else(|| SombraError::Invalid("no eviction candidate available"))
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
        inner.free_cache = FreeCache::from_extents(pages_to_extents(&all_pages));

        if capacity == 0 {
            return Err(SombraError::Invalid("page size too small for freelist"));
        }
        let total_extents = pages_to_extents(&all_pages);
        let needed_pages = if total_extents.is_empty() {
            0
        } else {
            (total_extents.len() + capacity - 1) / capacity
        };
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
            self.db_io
                .write_at(page_offset(*page_id, self.page_size), &buf)?;
        }

        if truncated && inner.meta.next_page.0 < original_next_page.0 {
            let new_len = inner.meta.next_page.0 * self.page_size as u64;
            self.db_io.truncate(new_len)?;
        }

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
            guard: guard_buf,
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
            max_batch_commits: options.wal_commit_max_commits,
            max_batch_frames: options.wal_commit_max_frames,
            max_batch_wait: Duration::from_millis(options.wal_commit_coalesce_ms),
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
        let lsn = inner.next_lsn;
        let mut dirty_pages: Vec<PageId> = guard.dirty_pages.iter().copied().collect();
        if inner.meta_dirty && !dirty_pages.iter().any(|p| p.0 == 0) {
            dirty_pages.push(PageId(0));
        }
        dirty_pages.sort_by_key(|p| p.0);
        dirty_pages.dedup();

        let mut wal_frames = Vec::with_capacity(dirty_pages.len());
        for page_id in dirty_pages {
            if page_id.0 == 0 {
                let mut payload = vec![0u8; self.page_size];
                write_meta_page(&mut payload, &inner.meta)?;
                wal_frames.push(WalFrameOwned {
                    lsn,
                    page_id,
                    payload,
                });
                continue;
            }
            let idx = match inner.page_table.get(&page_id) {
                Some(&idx) => idx,
                None => {
                    return Err(SombraError::Invalid("page not cached"));
                }
            };
            let mut payload = vec![0u8; self.page_size];
            {
                let mut buf_guard = inner.frames[idx].buf.write();
                page::clear_crc32(&mut buf_guard[..PAGE_HDR_LEN])?;
                let crc = page_crc32(page_id.0, inner.meta.salt, &buf_guard);
                buf_guard[page::header::CRC32].copy_from_slice(&crc.to_be_bytes());
                payload.copy_from_slice(&buf_guard[..]);
            }
            inner.frames[idx].pending_checkpoint = true;
            wal_frames.push(WalFrameOwned {
                lsn,
                page_id,
                payload,
            });
            inner.frames[idx].dirty = false;
            inner.stats.dirty_writebacks += 1;
        }
        inner.next_lsn = Lsn(lsn.0 + 1);
        drop(inner);
        let wal_frame_count = wal_frames.len() as u64;
        if wal_frame_count > 0 {
            record_pager_wal_frames(wal_frame_count);
            let payload_bytes = wal_frame_count.saturating_mul(self.page_size as u64);
            record_pager_wal_bytes(payload_bytes);
        }

        let synchronous = {
            let options = self.options.lock();
            options.synchronous
        };
        let sync_mode = match synchronous {
            Synchronous::Full => WalSyncMode::Immediate,
            Synchronous::Normal => WalSyncMode::Deferred,
            Synchronous::Off => WalSyncMode::Off,
        };
        let ticket = self.wal_committer.enqueue(wal_frames, sync_mode);
        guard.release_writer_lock();
        let commit_result = match ticket {
            Some(waiter) => waiter.wait(),
            None => Ok(()),
        };
        if let Err(err) = commit_result {
            guard.reacquire_writer_lock()?;
            return Err(err);
        }
        if matches!(synchronous, Synchronous::Normal) {
            self.schedule_normal_sync()?;
        }
        guard.committed = true;
        drop(guard);
        self.maybe_autocheckpoint()?;
        Ok(lsn)
    }

    fn run_checkpoint(&self, mode: CheckpointMode) -> Result<()> {
        let checkpoint_guard = match mode {
            CheckpointMode::Force => loop {
                if let Some(guard) = self.locks.try_acquire_checkpoint()? {
                    break guard;
                }
                std::thread::sleep(Duration::from_millis(10));
            },
            CheckpointMode::BestEffort => match self.locks.try_acquire_checkpoint()? {
                Some(guard) => guard,
                None => return Ok(()),
            },
        };
        let result = self.perform_checkpoint();
        drop(checkpoint_guard);
        result
    }

    fn perform_checkpoint(&self) -> Result<()> {
        let mut inner = self.inner.lock();
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
        if frames.is_empty() {
            self.wal.reset(Lsn(inner.meta.last_checkpoint_lsn.0 + 1))?;
            inner.next_lsn = Lsn(inner.meta.last_checkpoint_lsn.0 + 1);
            return Ok(());
        }
        for frame in &frames {
            let offset = page_offset(frame.page_id, self.page_size);
            self.db_io.write_at(offset, &frame.payload)?;
            if let Some(&idx) = inner.page_table.get(&frame.page_id) {
                {
                    let mut buf = inner.frames[idx].buf.write();
                    buf.copy_from_slice(&frame.payload);
                }
                inner.frames[idx].dirty = false;
                inner.frames[idx].pending_checkpoint = false;
                inner.frames[idx].newly_allocated = false;
                inner.frames[idx].needs_refresh = true;
            }
        }
        self.rebuild_freelist(&mut inner)?;
        self.db_io.sync_all()?;
        inner.meta.last_checkpoint_lsn = max_lsn;
        let mut meta_buf = vec![0u8; self.page_size];
        write_meta_page(&mut meta_buf, &inner.meta)?;
        self.db_io.write_at(0, &meta_buf)?;
        self.db_io.sync_all()?;
        self.wal.reset(Lsn(inner.meta.last_checkpoint_lsn.0 + 1))?;
        inner.next_lsn = Lsn(inner.meta.last_checkpoint_lsn.0 + 1);
        inner.meta_dirty = false;
        *self.last_autocheckpoint.lock() = Some(Instant::now());
        Ok(())
    }

    fn schedule_normal_sync(&self) -> Result<()> {
        let state_arc = Arc::clone(&self.wal_sync_state);
        {
            let mut state = state_arc.lock();
            if let Some(err) = state.last_error.take() {
                return Err(err);
            }
            if state.scheduled {
                return Ok(());
            }
            state.scheduled = true;
        }
        let wal = Arc::clone(&self.wal);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(NORMAL_SYNC_DELAY_MS));
            let result = wal.sync();
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

    fn maybe_autocheckpoint(&self) -> Result<()> {
        let (autocheckpoint_pages, autocheckpoint_ms) = {
            let options = self.options.lock();
            (options.autocheckpoint_pages, options.autocheckpoint_ms)
        };
        let mut should_checkpoint = false;
        if autocheckpoint_pages > 0 {
            let wal_len = self.wal.len()?;
            let threshold = (autocheckpoint_pages as u64).saturating_mul(self.page_size as u64);
            if wal_len >= threshold {
                should_checkpoint = true;
            }
        }
        if let Some(ms) = autocheckpoint_ms {
            let mut last = self.last_autocheckpoint.lock();
            match *last {
                Some(prev) if prev.elapsed() >= Duration::from_millis(ms) => {
                    should_checkpoint = true;
                }
                None => {
                    *last = Some(Instant::now());
                }
                _ => {}
            }
        }
        if should_checkpoint {
            let _ = self.run_checkpoint(CheckpointMode::BestEffort);
        }
        Ok(())
    }

    /// Returns the current metadata.
    pub fn meta(&self) -> Result<Meta> {
        let inner = self.inner.lock();
        Ok(inner.meta.clone())
    }
}

impl PageStore for Pager {
    fn page_size(&self) -> u32 {
        self.page_size as u32
    }

    fn get_page(&self, guard: &ReadGuard, id: PageId) -> Result<PageRef> {
        let mut cached: Option<Arc<[u8]>> = None;
        let mut refresh_idx: Option<usize> = None;
        let (salt, page_size, last_checkpoint_lsn) = {
            let mut inner = self.inner.lock();
            if let Some(&idx) = inner.page_table.get(&id) {
                let frame_flags = {
                    let frame = &inner.frames[idx];
                    (
                        !frame.newly_allocated
                            && (frame.dirty || frame.pending_checkpoint)
                            && guard.snapshot_lsn == inner.meta.last_checkpoint_lsn,
                        frame.needs_refresh,
                    )
                };
                let needs_snapshot = frame_flags.0;
                let needs_refresh = frame_flags.1;
                if needs_snapshot {
                    inner.stats.misses += 1;
                } else if needs_refresh {
                    inner.stats.misses += 1;
                    refresh_idx = Some(idx);
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
        if let Some(data) = cached {
            debug_assert_eq!(
                guard.snapshot_lsn, last_checkpoint_lsn,
                "snapshot advanced while reader active"
            );
            return Ok(PageRef { id, data });
        }
        debug_assert_eq!(
            guard.snapshot_lsn, last_checkpoint_lsn,
            "snapshot advanced while reader active"
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
        let lock = self.locks.acquire_reader()?;
        let snapshot_lsn = {
            let inner = self.inner.lock();
            inner.meta.last_checkpoint_lsn
        };
        Ok(ReadGuard {
            _lock: lock,
            snapshot_lsn,
        })
    }

    fn begin_write(&self) -> Result<WriteGuard<'_>> {
        let lock = self.locks.acquire_writer()?;
        let inner = self.inner.lock();
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
            extensions: TxnExtensions::default(),
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

    fn set_checksum_verification(&self, enabled: bool) {
        self.checksum_verify_on_read
            .store(enabled, AtomicOrdering::Relaxed);
    }

    fn checksum_verification_enabled(&self) -> bool {
        self.checksum_verify_on_read.load(AtomicOrdering::Relaxed)
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
    use tempfile::tempdir;

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
        let dir = tempdir().unwrap();
        let path = dir.path().join("auto_ms.db");
        let options = PagerOptions {
            cache_pages: 8,
            autocheckpoint_pages: usize::MAX,
            autocheckpoint_ms: Some(5),
            synchronous: Synchronous::Full,
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
        assert_eq!(pager.last_checkpoint_lsn(), Lsn(0));
        thread::sleep(Duration::from_millis(15));
        let mut write = pager.begin_write()?;
        {
            let mut page_mut = write.page_mut(page)?;
            page_mut.data_mut()[PAGE_HDR_LEN..PAGE_HDR_LEN + 4].copy_from_slice(b"TIME");
        }
        let lsn = pager.commit(write)?;
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
        let pager = Pager::create(&path, options.clone())?;
        let mut rng = rand::thread_rng();
        let mut allocated = Vec::new();
        let mut max_page = PageId(0);

        for step in 0..500 {
            let do_alloc = allocated.is_empty() || rng.gen_bool(0.6);
            if do_alloc {
                let mut write = pager.begin_write()?;
                let page = write.allocate_page()?;
                write_test_payload(&pager, &mut write, page)?;
                pager.commit(write)?;
                allocated.push(page);
                if page.0 > max_page.0 {
                    max_page = page;
                }
            } else {
                let idx = rng.gen_range(0..allocated.len());
                let page = allocated.swap_remove(idx);
                let mut write = pager.begin_write()?;
                write.free_page(page)?;
                pager.commit(write)?;
                let _ = pager.checkpoint(CheckpointMode::BestEffort);
            }
            if step % 50 == 0 {
                let _ = pager.checkpoint(CheckpointMode::BestEffort);
            }
        }

        for page in allocated.drain(..) {
            let mut write = pager.begin_write()?;
            write.free_page(page)?;
            pager.commit(write)?;
        }
        pager.checkpoint(CheckpointMode::Force)?;
        assert!(metadata(&path).unwrap().len() >= pager.page_size() as u64);
        drop(pager);

        let pager = Pager::open(&path, options)?;
        let mut ids = Vec::new();
        for _ in 0..16 {
            let mut write = pager.begin_write()?;
            let page = write.allocate_page()?;
            pager.commit(write)?;
            ids.push(page);
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
}
