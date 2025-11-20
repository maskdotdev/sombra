#![forbid(unsafe_code)]

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, IoSlice, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant, SystemTime};
use tracing::{debug, warn};

use crate::primitives::io::{FileIo, StdFileIo};
use crate::storage::{
    record_pager_fsync, record_wal_coalesced_writes, record_wal_io_group_sample,
    record_wal_reused_segments,
};
use crate::types::{Checksum, Crc32Fast, Lsn, PageId, Result, SombraError};
use parking_lot::{Condvar, Mutex};

const WAL_MAGIC: [u8; 4] = *b"SOMW";
const WAL_FORMAT_VERSION: u16 = 1;
const FILE_HEADER_LEN: usize = 32;
const FRAME_HEADER_LEN: usize = 32;
const WAL_MAX_IO_SLICES: usize = 512;
const WAL_SEGMENT_PREFIX: &str = "wal-";
const WAL_LAYOUT_VERSION: u32 = 1;
const WAL_LAYOUT_KIND: &str = "segmented_v1";
/// Test-only delay for WAL batch application to surface backlogs.
static APPLY_BATCH_DELAY_MS: AtomicU64 = AtomicU64::new(0);
/// Configuration options for opening a write-ahead log.
#[derive(Clone, Debug)]
pub struct WalOptions {
    /// Size of each page in bytes
    pub page_size: u32,
    /// Random salt value to distinguish different database instances
    pub wal_salt: u64,
    /// Starting LSN for the log sequence
    pub start_lsn: Lsn,
    /// Size of each WAL segment in bytes (0 disables segmentation)
    pub segment_size_bytes: u64,
    /// Number of segments to preallocate ahead of the append pointer
    pub preallocate_segments: u32,
}

impl WalOptions {
    /// Creates a new WalOptions with the specified configuration.
    pub fn new(page_size: u32, wal_salt: u64, start_lsn: Lsn) -> Self {
        Self {
            page_size,
            wal_salt,
            start_lsn,
            segment_size_bytes: 64 * 1024 * 1024,
            preallocate_segments: 0,
        }
    }
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            page_size: 0,
            wal_salt: 0,
            start_lsn: Lsn(0),
            segment_size_bytes: 64 * 1024 * 1024,
            preallocate_segments: 0,
        }
    }
}

/// Logical pointer to a WAL frame.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WalFramePtr {
    /// Segment identifier when segmented WAL is enabled (0 for the legacy single file layout).
    pub segment_id: u64,
    /// Byte offset of the frame header within the selected segment.
    pub offset: u64,
}

impl WalFramePtr {}

/// Statistics tracking WAL operations.
#[derive(Clone, Debug, Default)]
pub struct WalStats {
    /// Number of frames appended to the log
    pub frames_appended: u64,
    /// Total bytes written to the log
    pub bytes_appended: u64,
    /// Number of sync operations performed
    pub syncs: u64,
    /// Number of coalesced write batches executed
    pub coalesced_writes: u64,
}

/// Snapshot of WAL segment allocator health.
#[derive(Clone, Debug, Serialize)]
pub struct WalAllocatorStats {
    /// Configured segment size in bytes.
    pub segment_size_bytes: u64,
    /// Target number of preallocated segments.
    pub preallocate_segments: u32,
    /// Segments ready for activation.
    pub ready_segments: usize,
    /// Segments queued for recycling.
    pub recycle_segments: usize,
    /// Total segments prepared from the recycle queue.
    pub reused_segments_total: u64,
    /// Total segments freshly created for readiness.
    pub created_segments_total: u64,
    /// Last allocator error (e.g., ENOSPC) if any.
    pub allocation_error: Option<String>,
}

#[derive(Clone, Debug)]
struct FileHeader {
    page_size: u32,
    wal_salt: u64,
    start_lsn: Lsn,
}

impl FileHeader {
    fn new(page_size: u32, wal_salt: u64, start_lsn: Lsn) -> Self {
        Self {
            page_size,
            wal_salt,
            start_lsn,
        }
    }

    fn encode(&self) -> [u8; FILE_HEADER_LEN] {
        let mut buf = [0u8; FILE_HEADER_LEN];
        buf[0..4].copy_from_slice(&WAL_MAGIC);
        buf[4..6].copy_from_slice(&WAL_FORMAT_VERSION.to_be_bytes());
        buf[6..8].fill(0);
        buf[8..12].copy_from_slice(&self.page_size.to_be_bytes());
        buf[12..20].copy_from_slice(&self.wal_salt.to_be_bytes());
        buf[20..28].copy_from_slice(&self.start_lsn.0.to_be_bytes());
        let mut crc_buf = buf;
        crc_buf[28..32].fill(0);
        let crc = compute_crc32(&[&crc_buf]);
        buf[28..32].copy_from_slice(&crc.to_be_bytes());
        buf
    }

    fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < FILE_HEADER_LEN {
            return Err(SombraError::Corruption("wal header truncated"));
        }
        let mut header = [0u8; FILE_HEADER_LEN];
        header.copy_from_slice(&src[..FILE_HEADER_LEN]);
        if header[0..4] != WAL_MAGIC {
            return Err(SombraError::Corruption("wal magic mismatch"));
        }
        let version = u16::from_be_bytes(header[4..6].try_into().unwrap());
        if version != WAL_FORMAT_VERSION {
            return Err(SombraError::Corruption("wal format version mismatch"));
        }
        if header[6..8] != [0, 0] {
            return Err(SombraError::Corruption(
                "wal reserved header bytes non-zero",
            ));
        }
        let stored_crc = u32::from_be_bytes(header[28..32].try_into().unwrap());
        header[28..32].fill(0);
        let crc = compute_crc32(&[&header]);
        if crc != stored_crc {
            return Err(SombraError::Corruption("wal header crc mismatch"));
        }
        let page_size = u32::from_be_bytes(src[8..12].try_into().unwrap());
        let wal_salt = u64::from_be_bytes(src[12..20].try_into().unwrap());
        let start_lsn = Lsn(u64::from_be_bytes(src[20..28].try_into().unwrap()));
        Ok(Self {
            page_size,
            wal_salt,
            start_lsn,
        })
    }
}

#[derive(Clone, Debug)]
struct FrameHeader {
    frame_lsn: Lsn,
    page_id: PageId,
    prev_crc32_chain: u64,
    payload_crc32: u32,
    header_crc32: u32,
}

impl FrameHeader {
    fn new(frame_lsn: Lsn, page_id: PageId, prev_crc32_chain: u64, payload_crc32: u32) -> Self {
        Self {
            frame_lsn,
            page_id,
            prev_crc32_chain,
            payload_crc32,
            header_crc32: 0,
        }
    }

    fn encode(&self) -> [u8; FRAME_HEADER_LEN] {
        let mut buf = [0u8; FRAME_HEADER_LEN];
        buf[0..8].copy_from_slice(&self.frame_lsn.0.to_be_bytes());
        buf[8..16].copy_from_slice(&self.page_id.0.to_be_bytes());
        buf[16..24].copy_from_slice(&self.prev_crc32_chain.to_be_bytes());
        buf[24..28].copy_from_slice(&self.payload_crc32.to_be_bytes());
        buf[28..32].copy_from_slice(&self.header_crc32.to_be_bytes());
        buf
    }

    fn encode_with_crc(&self) -> [u8; FRAME_HEADER_LEN] {
        let mut buf = self.encode();
        let mut crc_buf = buf;
        crc_buf[28..32].fill(0);
        let crc = compute_crc32(&[&crc_buf]);
        buf[28..32].copy_from_slice(&crc.to_be_bytes());
        buf
    }

    fn decode(src: &[u8]) -> Result<Self> {
        if src.len() < FRAME_HEADER_LEN {
            return Err(SombraError::Corruption("wal frame header truncated"));
        }
        let mut header = [0u8; FRAME_HEADER_LEN];
        header.copy_from_slice(&src[..FRAME_HEADER_LEN]);
        let stored_crc = u32::from_be_bytes(header[28..32].try_into().unwrap());
        header[28..32].fill(0);
        let crc = compute_crc32(&[&header]);
        if crc != stored_crc {
            return Err(SombraError::Corruption("wal frame header crc mismatch"));
        }
        let frame_lsn = Lsn(u64::from_be_bytes(src[0..8].try_into().unwrap()));
        let page_id = PageId(u64::from_be_bytes(src[8..16].try_into().unwrap()));
        let prev_crc32_chain = u64::from_be_bytes(src[16..24].try_into().unwrap());
        let payload_crc32 = u32::from_be_bytes(src[24..28].try_into().unwrap());
        Ok(Self {
            frame_lsn,
            page_id,
            prev_crc32_chain,
            payload_crc32,
            header_crc32: stored_crc,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct WalDirectoryManifest {
    layout: String,
    version: u32,
    page_size: u32,
    wal_salt: u64,
    start_lsn: u64,
    segment_size_bytes: u64,
    preallocate_segments: u32,
    next_segment_id: u64,
    created_unix_ms: u64,
}

impl WalDirectoryManifest {
    fn new(options: &WalOptions) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|dur| dur.as_millis() as u64)
            .unwrap_or(0);
        Self {
            layout: WAL_LAYOUT_KIND.to_string(),
            version: WAL_LAYOUT_VERSION,
            page_size: options.page_size,
            wal_salt: options.wal_salt,
            start_lsn: options.start_lsn.0,
            segment_size_bytes: options.segment_size_bytes,
            preallocate_segments: options.preallocate_segments,
            next_segment_id: 1,
            created_unix_ms: now,
        }
    }

    fn load(dir: &Path) -> Result<Self> {
        let manifest_path = dir.join("manifest");
        let mut file = File::open(&manifest_path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        let manifest: WalDirectoryManifest = serde_json::from_slice(&buf)
            .map_err(|err| SombraError::InvalidOwned(format!("wal manifest parse error: {err}")))?;
        Ok(manifest)
    }

    fn persist(&self, dir: &Path) -> Result<()> {
        let manifest_path = dir.join("manifest");
        let tmp_path = manifest_path.with_extension("tmp");
        let data = serde_json::to_vec_pretty(self).map_err(|err| {
            SombraError::InvalidOwned(format!("failed to serialize wal manifest: {err}"))
        })?;
        {
            let mut file = File::create(&tmp_path)?;
            file.write_all(&data)?;
            file.sync_all()?;
        }
        fs::rename(&tmp_path, &manifest_path)?;
        Ok(())
    }

    fn bootstrap(dir: &Path, options: &WalOptions) -> Result<Self> {
        if options.segment_size_bytes == 0 {
            return Err(SombraError::Invalid(
                "wal_segment_size_bytes must be greater than zero",
            ));
        }
        match fs::metadata(dir) {
            Ok(meta) if meta.is_file() => {
                return Err(SombraError::Invalid(
                    "wal path must be a directory named db-wal/",
                ));
            }
            Ok(_) => {}
            Err(err) if err.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(dir)?;
            }
            Err(err) => return Err(SombraError::from(err)),
        }
        fs::create_dir_all(active_dir(dir))?;
        fs::create_dir_all(recycle_dir(dir))?;
        let manifest_path = dir.join("manifest");
        if manifest_path.exists() {
            return Self::load(dir);
        }
        let manifest = WalDirectoryManifest::new(options);
        manifest.persist(dir)?;
        Ok(manifest)
    }
}

fn active_dir(dir: &Path) -> PathBuf {
    dir.join("active")
}

fn recycle_dir(dir: &Path) -> PathBuf {
    dir.join("recycle")
}

fn segment_path(dir: &Path, id: u64) -> PathBuf {
    active_dir(dir).join(format!("{}{:06}", WAL_SEGMENT_PREFIX, id))
}

fn recycle_segment_path(dir: &Path, id: u64) -> PathBuf {
    recycle_dir(dir).join(format!("{}{:06}", WAL_SEGMENT_PREFIX, id))
}

fn parse_segment_id(name: &str) -> Option<u64> {
    name.strip_prefix(WAL_SEGMENT_PREFIX)?.parse().ok()
}

fn list_segments(dir: &Path) -> Result<Vec<u64>> {
    let mut ids = Vec::new();
    if !active_dir(dir).exists() {
        return Ok(ids);
    }
    for entry in fs::read_dir(active_dir(dir))? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            if let Some(id) = parse_segment_id(name) {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn create_segment_file(
    dir: &Path,
    id: u64,
    page_size: u32,
    wal_salt: u64,
    start_lsn: Lsn,
    capacity: u64,
) -> Result<Arc<StdFileIo>> {
    let path = segment_path(dir, id);
    debug!(
        segment_id = id,
        ?path,
        capacity_bytes = capacity,
        "wal.segment.create"
    );
    let header = FileHeader::new(page_size, wal_salt, start_lsn);
    let io = initialize_segment_file(&path, &header, capacity)?;
    Ok(Arc::new(io))
}

fn list_recycle_segments(dir: &Path) -> Result<Vec<u64>> {
    let recycle = recycle_dir(dir);
    if !recycle.exists() {
        return Ok(Vec::new());
    }
    let mut ids = Vec::new();
    for entry in fs::read_dir(recycle)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        if let Some(name) = entry.file_name().to_str() {
            if let Some(id) = parse_segment_id(name) {
                ids.push(id);
            }
        }
    }
    ids.sort_unstable();
    Ok(ids)
}

fn open_segment_file(dir: &Path, id: u64, page_size: u32, wal_salt: u64) -> Result<Arc<StdFileIo>> {
    let path = segment_path(dir, id);
    let io = Arc::new(StdFileIo::open(&path)?);
    let mut buf = [0u8; FILE_HEADER_LEN];
    io.read_at(0, &mut buf)?;
    let header = FileHeader::decode(&buf)?;
    if header.page_size != page_size {
        return Err(SombraError::Corruption("wal segment page size mismatch"));
    }
    if header.wal_salt != wal_salt {
        return Err(SombraError::Corruption("wal segment salt mismatch"));
    }
    Ok(io)
}

struct SegmentWriter {
    id: u64,
    io: Arc<StdFileIo>,
    offset: u64,
    max_len: u64,
}

impl SegmentWriter {
    fn new(id: u64, io: Arc<StdFileIo>, offset: u64, max_len: u64) -> Self {
        Self {
            id,
            io,
            offset,
            max_len,
        }
    }

    fn remaining(&self) -> u64 {
        if self.offset >= self.max_len {
            0
        } else {
            self.max_len - self.offset
        }
    }
}

#[derive(Clone, Copy)]
struct SegmentMeta {
    len: u64,
}

struct WalState {
    header: FileHeader,
    prev_chain: u64,
    stats: WalStats,
    segment_writer: SegmentWriter,
    segment_capacity: u64,
}

impl WalState {
    fn new(header: FileHeader, writer: SegmentWriter, segment_capacity: u64) -> Self {
        Self {
            header,
            prev_chain: 0,
            stats: WalStats::default(),
            segment_writer: writer,
            segment_capacity,
        }
    }
}

struct PreallocQueues {
    state: Mutex<PreallocState>,
    cv: Condvar,
}

impl PreallocQueues {
    fn new() -> Self {
        Self {
            state: Mutex::new(PreallocState::default()),
            cv: Condvar::new(),
        }
    }
}

#[derive(Default)]
struct PreallocState {
    ready: VecDeque<u64>,
    recycle: VecDeque<u64>,
    reused_total: u64,
    created_total: u64,
    enospc_error: Option<String>,
    shutdown: bool,
}

enum PreallocOp {
    Reuse(u64),
    Create,
}

/// Write-ahead log that provides durability and crash recovery.
///
/// The WAL stores page modifications as a sequence of frames, each containing
/// a page image along with metadata. Frames are checksummed and chained together
/// to detect corruption.
pub struct Wal {
    dir: PathBuf,
    page_size: usize,
    state: Mutex<WalState>,
    segments: Mutex<BTreeMap<u64, SegmentMeta>>,
    segment_cache: Mutex<HashMap<u64, Arc<StdFileIo>>>,
    manifest: Mutex<WalDirectoryManifest>,
    prealloc: Arc<PreallocQueues>,
    prealloc_thread: Mutex<Option<thread::JoinHandle<()>>>,
    prealloc_target: u32,
    pending_recycle: Mutex<Option<Vec<u64>>>,
}

impl Wal {
    fn initialize_ready_segments(&self) -> Result<()> {
        let recycle_ids = list_recycle_segments(&self.dir)?;
        if recycle_ids.is_empty() {
            return Ok(());
        }
        let mut state = self.prealloc.state.lock();
        for id in recycle_ids {
            state.recycle.push_back(id);
        }
        self.prealloc.cv.notify_all();
        Ok(())
    }

    fn start_preallocator(self: &Arc<Self>) {
        if self.prealloc_target == 0 {
            return;
        }
        let mut guard = self.prealloc_thread.lock();
        if guard.is_some() {
            return;
        }
        let wal = Arc::clone(self);
        let handle = thread::spawn(move || wal.preallocator_loop());
        *guard = Some(handle);
    }

    fn preallocator_loop(self: Arc<Self>) {
        loop {
            let op = {
                let mut guard = self.prealloc.state.lock();
                loop {
                    if guard.shutdown {
                        return;
                    }
                    if let Some(id) = guard.recycle.pop_front() {
                        break PreallocOp::Reuse(id);
                    }
                    if (guard.ready.len() as u32) < self.prealloc_target {
                        break PreallocOp::Create;
                    }
                    self.prealloc.cv.wait(&mut guard);
                }
            };
            match op {
                PreallocOp::Reuse(id) => {
                    debug!(segment_id = id, "wal.preallocator.reuse_start");
                    match self.prepare_recycled_segment(id) {
                        Ok(new_id) => {
                            debug!(segment_id = new_id, "wal.preallocator.reuse_ready");
                            {
                                let mut guard = self.prealloc.state.lock();
                                guard.reused_total = guard.reused_total.saturating_add(1);
                            }
                            self.push_ready_segment(new_id)
                        }
                        Err(err) => self.record_prealloc_error(&err),
                    }
                }
                PreallocOp::Create => {
                    debug!("wal.preallocator.create_start");
                    match self.create_ready_segment() {
                        Ok(id) => {
                            debug!(segment_id = id, "wal.preallocator.create_ready");
                            {
                                let mut guard = self.prealloc.state.lock();
                                guard.created_total = guard.created_total.saturating_add(1);
                            }
                            self.push_ready_segment(id)
                        }
                        Err(err) => self.record_prealloc_error(&err),
                    }
                }
            }
        }
    }

    fn record_prealloc_error(&self, err: &SombraError) {
        let mut guard = self.prealloc.state.lock();
        guard.enospc_error = Some(err.to_string());
        self.prealloc.cv.notify_all();
        warn!(error = %err, "wal.preallocator.error");
    }

    fn push_ready_segment(&self, id: u64) {
        let mut guard = self.prealloc.state.lock();
        guard.ready.push_back(id);
        guard.enospc_error = None;
        self.prealloc.cv.notify_all();
    }

    fn create_ready_segment(&self) -> Result<u64> {
        let (header, capacity) = self.segment_template();
        self.create_ready_segment_with_template(&header, capacity)
    }

    fn create_ready_segment_with_template(
        &self,
        header: &FileHeader,
        capacity: u64,
    ) -> Result<u64> {
        let mut manifest = self.manifest.lock();
        let id = manifest.next_segment_id;
        manifest.next_segment_id += 1;
        manifest.persist(&self.dir)?;
        drop(manifest);
        let path = recycle_segment_path(&self.dir, id);
        initialize_segment_file(&path, &header, capacity)?;
        Ok(id)
    }

    fn segment_template(&self) -> (FileHeader, u64) {
        let state = self.state.lock();
        (state.header.clone(), state.segment_capacity)
    }

    fn prepare_recycled_segment(&self, old_id: u64) -> Result<u64> {
        let src = recycle_segment_path(&self.dir, old_id);
        if !src.exists() {
            return self.create_ready_segment();
        }
        let (header, capacity) = self.segment_template();
        self.prepare_recycled_segment_with_template(old_id, &header, capacity)
    }

    fn prepare_recycled_segment_with_template(
        &self,
        old_id: u64,
        header: &FileHeader,
        capacity: u64,
    ) -> Result<u64> {
        let src = recycle_segment_path(&self.dir, old_id);
        if !src.exists() {
            return self.create_ready_segment_with_template(header, capacity);
        }
        let mut manifest = self.manifest.lock();
        let new_id = manifest.next_segment_id;
        manifest.next_segment_id += 1;
        manifest.persist(&self.dir)?;
        drop(manifest);
        let dst = recycle_segment_path(&self.dir, new_id);
        fs::rename(&src, &dst)?;
        initialize_segment_file(&dst, &header, capacity)?;
        Ok(new_id)
    }

    fn try_prepare_recycle_immediate(
        &self,
        header: &FileHeader,
        capacity: u64,
    ) -> Result<Option<u64>> {
        let recycled = {
            let mut guard = self.prealloc.state.lock();
            guard.recycle.pop_front()
        };
        if let Some(old_id) = recycled {
            let id = self.prepare_recycled_segment_with_template(old_id, header, capacity)?;
            {
                let mut guard = self.prealloc.state.lock();
                guard.reused_total = guard.reused_total.saturating_add(1);
            }
            return Ok(Some(id));
        }
        Ok(None)
    }

    fn take_ready_segment(&self, header: &FileHeader, capacity: u64) -> Result<u64> {
        if self.prealloc_target == 0 {
            if let Some(id) = self.try_prepare_recycle_immediate(header, capacity)? {
                return Ok(id);
            }
            let id = self.create_ready_segment_with_template(header, capacity)?;
            {
                let mut guard = self.prealloc.state.lock();
                guard.created_total = guard.created_total.saturating_add(1);
            }
            return Ok(id);
        }
        loop {
            let mut guard = self.prealloc.state.lock();
            if let Some(id) = guard.ready.pop_front() {
                self.prealloc.cv.notify_all();
                return Ok(id);
            }
            if let Some(err) = guard.enospc_error.clone() {
                return Err(SombraError::InvalidOwned(format!(
                    "wal segment allocation blocked: {err}"
                )));
            }
            debug!("wal.take_ready_segment.waiting_for_segment");
            self.prealloc.cv.wait(&mut guard);
        }
    }
    /// Opens or creates a write-ahead log using the segmented directory layout.
    pub fn open(dir: impl AsRef<Path>, options: WalOptions) -> Result<Arc<Self>> {
        if options.page_size == 0 {
            return Err(SombraError::Invalid("wal page size must be non-zero"));
        }
        if options.segment_size_bytes <= FILE_HEADER_LEN as u64 {
            return Err(SombraError::Invalid(
                "wal_segment_size_bytes must exceed the header length",
            ));
        }
        let dir = dir.as_ref().to_path_buf();
        debug!(
            wal_dir = ?dir,
            page_size = options.page_size,
            segment_size_bytes = options.segment_size_bytes,
            preallocate_segments = options.preallocate_segments,
            start_lsn = options.start_lsn.0,
            "wal.open.start"
        );
        let manifest = WalDirectoryManifest::bootstrap(&dir, &options)?;
        if manifest.layout != WAL_LAYOUT_KIND || manifest.version != WAL_LAYOUT_VERSION {
            return Err(SombraError::Corruption("wal manifest layout mismatch"));
        }
        if manifest.page_size != options.page_size {
            return Err(SombraError::Corruption("wal manifest page size mismatch"));
        }
        if manifest.wal_salt != options.wal_salt {
            return Err(SombraError::Corruption("wal manifest salt mismatch"));
        }
        let mut manifest_state = manifest;
        let mut manifest_dirty = false;
        if manifest_state.start_lsn != options.start_lsn.0 {
            manifest_state.start_lsn = options.start_lsn.0;
            manifest_dirty = true;
        }
        if manifest_state.segment_size_bytes != options.segment_size_bytes {
            manifest_state.segment_size_bytes = options.segment_size_bytes;
            manifest_dirty = true;
        }
        if manifest_state.preallocate_segments != options.preallocate_segments {
            manifest_state.preallocate_segments = options.preallocate_segments;
            manifest_dirty = true;
        }
        if manifest_dirty {
            manifest_state.persist(&dir)?;
        }
        let mut segment_ids = list_segments(&dir)?;
        if segment_ids.is_empty() {
            let id = manifest_state.next_segment_id;
            create_segment_file(
                &dir,
                id,
                options.page_size,
                options.wal_salt,
                options.start_lsn,
                options.segment_size_bytes,
            )?;
            manifest_state.next_segment_id += 1;
            manifest_state.persist(&dir)?;
            segment_ids.push(id);
        }
        segment_ids.sort_unstable();
        if let Some(&max_id) = segment_ids.last() {
            if manifest_state.next_segment_id <= max_id {
                manifest_state.next_segment_id = max_id
                    .checked_add(1)
                    .ok_or_else(|| SombraError::Invalid("wal segment id overflow"))?;
                manifest_dirty = true;
            }
        }
        if manifest_dirty {
            manifest_state.persist(&dir)?;
        }
        let active_id = *segment_ids.last().expect("at least one segment");
        let active_io = open_segment_file(&dir, active_id, options.page_size, options.wal_salt)?;
        let header = FileHeader::new(options.page_size, options.wal_salt, options.start_lsn);
        let mut segment_cache = HashMap::new();
        segment_cache.insert(active_id, Arc::clone(&active_io));
        let mut metadata = BTreeMap::new();
        for id in &segment_ids {
            let path = segment_path(&dir, *id);
            let raw_len = fs::metadata(&path)?.len().max(FILE_HEADER_LEN as u64);
            let io = if *id == active_id {
                Arc::clone(&active_io)
            } else {
                open_segment_file(&dir, *id, options.page_size, options.wal_salt)?
            };
            let valid_len = detect_valid_prefix(&io, raw_len, options.page_size as usize, &header)?;
            metadata.insert(*id, SegmentMeta { len: valid_len });
            if *id != active_id {
                segment_cache.insert(*id, Arc::clone(&io));
            }
        }
        let active_len = metadata
            .get(&active_id)
            .map(|meta| meta.len)
            .unwrap_or(FILE_HEADER_LEN as u64);
        if active_len > options.segment_size_bytes {
            return Err(SombraError::Corruption(
                "wal segment exceeds configured size",
            ));
        }
        let writer =
            SegmentWriter::new(active_id, active_io, active_len, options.segment_size_bytes);
        let state = WalState::new(header, writer, options.segment_size_bytes);
        let wal = Arc::new(Self {
            dir,
            page_size: options.page_size as usize,
            state: Mutex::new(state),
            segments: Mutex::new(metadata),
            segment_cache: Mutex::new(segment_cache),
            manifest: Mutex::new(manifest_state),
            prealloc: Arc::new(PreallocQueues::new()),
            prealloc_thread: Mutex::new(None),
            prealloc_target: options.preallocate_segments,
            pending_recycle: Mutex::new(None),
        });
        wal.initialize_ready_segments()?;
        wal.start_preallocator();
        Ok(wal)
    }

    /// Resets the WAL to a new starting LSN, truncating all existing frames.
    pub fn reset(&self, start_lsn: Lsn) -> Result<()> {
        debug!(start_lsn = start_lsn.0, "wal.reset.start");
        let mut state = self.state.lock();
        let recycled_ids = {
            let mut pending = self.pending_recycle.lock();
            match pending.take() {
                Some(ids) => ids,
                None => self.recycle_segments_internal()?,
            }
        };
        state.header = FileHeader::new(state.header.page_size, state.header.wal_salt, start_lsn);
        state.prev_chain = 0;
        state.stats = WalStats::default();
        let new_id = self.take_ready_segment(&state.header, state.segment_capacity)?;
        let segment_io =
            self.activate_ready_segment(new_id, &state.header, state.segment_capacity)?;
        state.segment_writer = SegmentWriter::new(
            new_id,
            segment_io,
            FILE_HEADER_LEN as u64,
            state.segment_capacity,
        );
        {
            let mut manifest = self.manifest.lock();
            manifest.start_lsn = start_lsn.0;
            manifest.persist(&self.dir)?;
        }
        if !recycled_ids.is_empty() {
            record_wal_reused_segments(recycled_ids.len() as u64);
        }
        debug!(
            start_lsn = start_lsn.0,
            recycled_segments = recycled_ids.len(),
            new_segment_id = new_id,
            "wal.reset.complete"
        );
        Ok(())
    }

    /// Moves active segments into the recycle queue ahead of a reset.
    ///
    /// This allows the background preallocator to prepare fresh segments while
    /// checkpoint/replay work proceeds, reducing stalls when `reset` is
    /// ultimately invoked. Callers must ensure no further WAL appends occur
    /// before the reset completes.
    pub fn recycle_active_segments(&self) -> Result<usize> {
        {
            let pending = self.pending_recycle.lock();
            if let Some(ids) = pending.as_ref() {
                return Ok(ids.len());
            }
        }
        let _state_guard = self.state.lock();
        let recycled = self.recycle_segments_internal()?;
        let len = recycled.len();
        let mut pending = self.pending_recycle.lock();
        *pending = Some(recycled);
        Ok(len)
    }

    fn recycle_segments_internal(&self) -> Result<Vec<u64>> {
        let old_ids: Vec<u64> = {
            let segments = self.segments.lock();
            segments.keys().copied().collect()
        };
        if old_ids.is_empty() {
            return Ok(old_ids);
        }
        {
            let mut segments = self.segments.lock();
            let mut cache = self.segment_cache.lock();
            for id in &old_ids {
                segments.remove(id);
                cache.remove(id);
            }
        }
        for id in &old_ids {
            let _ = self.enqueue_recycle(*id);
        }
        Ok(old_ids)
    }

    /// Appends a single frame to the WAL.
    ///
    /// The frame payload must match the configured page size. This method does not
    /// sync to disk; call `sync()` to ensure durability.
    pub fn append_frame(&self, frame: WalFrame<'_>) -> Result<Vec<WalFramePtr>> {
        let frames = [frame];
        self.append_frame_batch(&frames)
    }

    /// Appends a batch of frames, coalescing writes when possible.
    pub fn append_frame_batch(&self, frames: &[WalFrame<'_>]) -> Result<Vec<WalFramePtr>> {
        if frames.is_empty() {
            return Ok(Vec::new());
        }
        let mut state = self.state.lock();
        for frame in frames {
            if frame.payload.len() != self.page_size {
                return Err(SombraError::Invalid("wal frame payload size mismatch"));
            }
            if frame.lsn.0 < state.header.start_lsn.0 {
                return Err(SombraError::Invalid("wal frame lsn below start_lsn"));
            }
        }
        if state.stats.frames_appended == 0 && frames[0].lsn.0 > state.header.start_lsn.0 {
            state.header.start_lsn = frames[0].lsn;
            state
                .segment_writer
                .io
                .write_at(0, &state.header.encode())?;
        }
        let frame_size = FRAME_HEADER_LEN + self.page_size;
        let mut offsets = Vec::with_capacity(frames.len());
        let mut index = 0usize;
        while index < frames.len() {
            let remaining = frames.len() - index;
            let chunk_frames = remaining.min(WAL_MAX_IO_SLICES / 2).max(1);
            let slice_end = index + chunk_frames;
            let chunk = &frames[index..slice_end];
            let mut header_bufs: Vec<[u8; FRAME_HEADER_LEN]> = Vec::with_capacity(chunk.len());
            for frame in chunk {
                let payload_crc32 = compute_crc32(&[frame.payload]);
                let header =
                    FrameHeader::new(frame.lsn, frame.page_id, state.prev_chain, payload_crc32);
                let encoded_header = header.encode_with_crc();
                let mut chain_hasher = Crc32Fast::default();
                chain_hasher.update(&state.prev_chain.to_be_bytes());
                chain_hasher.update(&encoded_header);
                chain_hasher.update(frame.payload);
                let chain_crc = chain_hasher.finalize();
                state.prev_chain = ((frame_size as u64) << 32) | u64::from(chain_crc);
                header_bufs.push(encoded_header);
            }
            let mut slices: Vec<IoSlice<'_>> = Vec::with_capacity(chunk.len() * 2);
            for (idx, frame) in chunk.iter().enumerate() {
                slices.push(IoSlice::new(&header_bufs[idx]));
                slices.push(IoSlice::new(frame.payload));
            }
            let chunk_bytes = chunk.len() * frame_size;
            let chunk_bytes_u64 = chunk_bytes as u64;
            self.ensure_segment_capacity(&mut state, chunk_bytes_u64)?;
            let chunk_start = state.segment_writer.offset;
            state.segment_writer.io.writev_at(chunk_start, &slices)?;
            state.segment_writer.offset += chunk_bytes_u64;
            state.stats.frames_appended += chunk.len() as u64;
            state.stats.bytes_appended += chunk_bytes_u64;
            state.stats.coalesced_writes += 1;
            record_wal_coalesced_writes(1);
            record_wal_io_group_sample(chunk.len() as u64);
            self.update_segment_len(state.segment_writer.id, state.segment_writer.offset);
            for frame_idx in 0..chunk.len() {
                offsets.push(WalFramePtr {
                    segment_id: state.segment_writer.id,
                    offset: chunk_start + (frame_idx * frame_size) as u64,
                });
            }
            index = slice_end;
        }
        Ok(offsets)
    }

    /// Syncs all pending writes to persistent storage.
    pub fn sync(&self) -> Result<()> {
        let io = {
            let state = self.state.lock();
            Arc::clone(&state.segment_writer.io)
        };
        io.sync_all()?;
        record_pager_fsync();
        let mut state = self.state.lock();
        state.stats.syncs += 1;
        Ok(())
    }

    /// Creates an iterator to read frames from the WAL.
    pub fn iter(&self) -> Result<WalIterator> {
        let header = {
            let state = self.state.lock();
            state.header.clone()
        };
        let segments_snapshot = self.segments.lock().clone();
        let mut segments = Vec::with_capacity(segments_snapshot.len());
        let mut base = 0u64;
        for (id, meta) in segments_snapshot {
            let io = self.open_segment_cached(id)?;
            segments.push(SegmentIterState {
                io,
                offset: FILE_HEADER_LEN as u64,
                end: meta.len,
                base,
            });
            base += meta.len;
        }
        Ok(WalIterator {
            segments,
            segment_index: 0,
            page_size: self.page_size,
            prev_chain: 0,
            valid_up_to: FILE_HEADER_LEN as u64,
            header,
        })
    }

    /// Reads a WAL frame located at the provided byte `offset`.
    ///
    /// Returns `Ok(None)` when the offset lies beyond the end of the current WAL.
    pub fn read_frame_at(&self, ptr: WalFramePtr) -> Result<Option<WalFrameOwned>> {
        if ptr.offset < FILE_HEADER_LEN as u64 {
            return Err(SombraError::Invalid("wal frame offset before header"));
        }
        let segment_len = {
            let segments = self.segments.lock();
            match segments.get(&ptr.segment_id) {
                Some(meta) => meta.len,
                None => return Ok(None),
            }
        };
        if ptr.offset + FRAME_HEADER_LEN as u64 > segment_len {
            return Ok(None);
        }
        let io = self.open_segment_cached(ptr.segment_id)?;
        let mut header_buf = [0u8; FRAME_HEADER_LEN];
        io.read_at(ptr.offset, &mut header_buf)?;
        let header = FrameHeader::decode(&header_buf)?;
        {
            let state = self.state.lock();
            if header.frame_lsn.0 < state.header.start_lsn.0 {
                return Err(SombraError::Corruption("wal frame lsn below start_lsn"));
            }
        }
        let payload_off = ptr.offset + FRAME_HEADER_LEN as u64;
        if payload_off + self.page_size as u64 > segment_len {
            return Ok(None);
        }
        let mut payload = vec![0u8; self.page_size];
        io.read_at(payload_off, &mut payload)?;
        let payload_crc = compute_crc32(&[&payload]);
        if payload_crc != header.payload_crc32 {
            return Err(SombraError::Corruption("wal frame payload crc mismatch"));
        }
        Ok(Some(WalFrameOwned {
            lsn: header.frame_lsn,
            page_id: header.page_id,
            payload,
        }))
    }

    /// Returns current statistics for this WAL instance.
    pub fn stats(&self) -> WalStats {
        let state = self.state.lock();
        state.stats.clone()
    }

    /// Returns allocator/preallocation state for observability.
    pub fn allocator_stats(&self) -> WalAllocatorStats {
        let segment_size_bytes = {
            let state = self.state.lock();
            state.segment_capacity
        };
        let queues = self.prealloc.state.lock();
        WalAllocatorStats {
            segment_size_bytes,
            preallocate_segments: self.prealloc_target,
            ready_segments: queues.ready.len(),
            recycle_segments: queues.recycle.len(),
            reused_segments_total: queues.reused_total,
            created_segments_total: queues.created_total,
            allocation_error: queues.enospc_error.clone(),
        }
    }

    /// Returns the total size of the WAL file in bytes.
    pub fn len(&self) -> Result<u64> {
        let segments = self.segments.lock();
        Ok(segments.values().map(|meta| meta.len).sum())
    }

    /// Returns true if the WAL contains no frames.
    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? <= FILE_HEADER_LEN as u64)
    }

    fn ensure_segment_capacity(&self, state: &mut WalState, required: u64) -> Result<()> {
        let max_payload = state
            .segment_capacity
            .saturating_sub(FILE_HEADER_LEN as u64);
        if required > max_payload {
            return Err(SombraError::Invalid(
                "wal frame batch exceeds segment capacity",
            ));
        }
        while state.segment_writer.remaining() < required {
            self.rotate_segment(state)?;
        }
        Ok(())
    }

    fn rotate_segment(&self, state: &mut WalState) -> Result<()> {
        self.update_segment_len(state.segment_writer.id, state.segment_writer.offset);
        let new_id = self.take_ready_segment(&state.header, state.segment_capacity)?;
        let segment_io =
            self.activate_ready_segment(new_id, &state.header, state.segment_capacity)?;
        state.segment_writer = SegmentWriter::new(
            new_id,
            segment_io,
            FILE_HEADER_LEN as u64,
            state.segment_capacity,
        );
        Ok(())
    }

    fn update_segment_len(&self, id: u64, len: u64) {
        let mut segments = self.segments.lock();
        if let Some(meta) = segments.get_mut(&id) {
            meta.len = len;
        }
    }

    fn activate_ready_segment(
        &self,
        id: u64,
        header: &FileHeader,
        capacity: u64,
    ) -> Result<Arc<StdFileIo>> {
        let src = recycle_segment_path(&self.dir, id);
        let dst = segment_path(&self.dir, id);
        debug!(
            segment_id = id,
            ?dst,
            capacity_bytes = capacity,
            recycled = src.exists(),
            "wal.segment.activate"
        );
        if src.exists() {
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::rename(&src, &dst)?;
        }
        let io = Arc::new(StdFileIo::open(&dst)?);
        io.truncate(capacity)?;
        io.write_at(0, &header.encode())?;
        {
            let mut segments = self.segments.lock();
            segments.insert(
                id,
                SegmentMeta {
                    len: FILE_HEADER_LEN as u64,
                },
            );
        }
        {
            let mut cache = self.segment_cache.lock();
            cache.insert(id, Arc::clone(&io));
        }
        Ok(io)
    }

    fn enqueue_recycle(&self, id: u64) -> Result<()> {
        let src = segment_path(&self.dir, id);
        let dst = recycle_segment_path(&self.dir, id);
        if Path::new(&src).exists() {
            if let Some(parent) = dst.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::rename(src, dst)?;
        }
        let mut guard = self.prealloc.state.lock();
        guard.recycle.push_back(id);
        self.prealloc.cv.notify_all();
        Ok(())
    }

    fn open_segment_cached(&self, id: u64) -> Result<Arc<StdFileIo>> {
        if let Some(io) = self.segment_cache.lock().get(&id) {
            return Ok(Arc::clone(io));
        }
        let (page_size, wal_salt) = {
            let state = self.state.lock();
            (state.header.page_size, state.header.wal_salt)
        };
        let io = open_segment_file(&self.dir, id, page_size, wal_salt)?;
        let mut cache = self.segment_cache.lock();
        Ok(Arc::clone(cache.entry(id).or_insert(io)))
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        {
            let mut state = self.prealloc.state.lock();
            state.shutdown = true;
        }
        self.prealloc.cv.notify_all();
        if let Some(handle) = self.prealloc_thread.lock().take() {
            let _ = handle.join();
        }
    }
}

/// A WAL frame containing a page image and metadata.
pub struct WalFrame<'a> {
    /// Log sequence number for this frame
    pub lsn: Lsn,
    /// Page identifier being updated
    pub page_id: PageId,
    /// Page data contents
    pub payload: &'a [u8],
}

/// Owned version of WalFrame with owned payload data.
pub struct WalFrameOwned {
    /// Log sequence number for this frame
    pub lsn: Lsn,
    /// Page identifier being updated
    pub page_id: PageId,
    /// Owned page data contents
    pub payload: Vec<u8>,
}

impl fmt::Debug for WalFrameOwned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WalFrameOwned")
            .field("lsn", &self.lsn)
            .field("page_id", &self.page_id)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

/// Synchronization mode for WAL commits.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalSyncMode {
    /// Sync immediately after writing frames
    Immediate,
    /// Defer sync to a later time
    Deferred,
    /// Do not sync (unsafe, for testing only)
    Off,
}

/// Configuration for batch commit behavior in WalCommitter.
#[derive(Clone, Copy, Debug)]
pub struct WalCommitConfig {
    /// Maximum number of commit requests to batch together
    pub max_batch_commits: usize,
    /// Maximum total frames across all batched commits
    pub max_batch_frames: usize,
    /// Maximum time to wait for additional commits before flushing batch
    pub max_batch_wait: Duration,
}

impl Default for WalCommitConfig {
    fn default() -> Self {
        Self {
            max_batch_commits: 32,
            max_batch_frames: 512,
            max_batch_wait: Duration::from_millis(2),
        }
    }
}

impl WalCommitConfig {
    fn normalize(mut self) -> Self {
        if self.max_batch_commits == 0 {
            self.max_batch_commits = 1;
        }
        if self.max_batch_frames == 0 {
            self.max_batch_frames = 1;
        }
        if self.max_batch_wait.is_zero() {
            self.max_batch_wait = Duration::from_micros(100);
        }
        self
    }
}

/// Injects a fixed delay (in milliseconds) before applying each WAL batch.
///
/// Intended for testing/chaos scenarios to surface backlog handling.
pub fn set_wal_batch_delay_ms_for_tests(ms: u64) {
    APPLY_BATCH_DELAY_MS.store(ms, AtomicOrdering::Relaxed);
}

/// Snapshot of the WAL committer backlog.
#[derive(Clone, Debug, Default, Serialize)]
pub struct WalCommitBacklog {
    /// Pending commit requests waiting to be flushed.
    pub pending_commits: usize,
    /// Total WAL frames queued across pending commits.
    pub pending_frames: usize,
    /// Whether the worker thread is currently running.
    pub worker_running: bool,
}

/// Asynchronous WAL committer that batches writes for improved throughput.
///
/// Spawns a background worker thread that coalesces multiple commit requests
/// into larger batches before writing to the WAL.
pub struct WalCommitter {
    wal: Arc<Wal>,
    state: Arc<Mutex<CommitState>>,
    wakeup: Arc<Condvar>,
    config: Arc<Mutex<WalCommitConfig>>,
}

/// Ticket representing a pending commit operation.
///
/// Call `wait()` to block until the commit completes or fails.
pub struct WalCommitTicket {
    request: Arc<CommitRequest>,
}

impl WalCommitTicket {
    /// Blocks until the associated commit operation completes.
    ///
    /// Returns an error if the commit failed.
    pub fn wait(self) -> Result<Vec<WalFramePtr>> {
        self.request.wait()
    }
}

impl WalCommitter {
    /// Creates a new WalCommitter with the specified configuration.
    pub fn new(wal: Arc<Wal>, config: WalCommitConfig) -> Self {
        let cfg = config.normalize();
        Self {
            wal,
            state: Arc::new(Mutex::new(CommitState::default())),
            wakeup: Arc::new(Condvar::new()),
            config: Arc::new(Mutex::new(cfg)),
        }
    }

    /// Enqueues a commit request without blocking.
    ///
    /// Returns a ticket that can be used to wait for completion, or None if
    /// the request was empty and no sync was needed.
    pub fn enqueue(
        &self,
        frames: Vec<WalFrameOwned>,
        sync_mode: WalSyncMode,
    ) -> Option<WalCommitTicket> {
        if frames.is_empty() && !matches!(sync_mode, WalSyncMode::Immediate) {
            return None;
        }
        let request = Arc::new(CommitRequest::new(frames, sync_mode));
        {
            let mut state = self.state.lock();
            state.pending.push_back(Arc::clone(&request));
            debug!(
                frames = request.frames.len(),
                sync_mode = ?sync_mode,
                pending = state.pending.len(),
                worker_running = state.worker_running,
                "wal.committer.enqueue"
            );
            if !state.worker_running {
                state.worker_running = true;
                Self::spawn_worker(
                    Arc::clone(&self.wal),
                    Arc::clone(&self.state),
                    Arc::clone(&self.wakeup),
                    Arc::clone(&self.config),
                );
            } else {
                self.wakeup.notify_one();
            }
        }
        Some(WalCommitTicket { request })
    }

    /// Enqueues a commit request and blocks until it completes.
    pub fn commit(
        &self,
        frames: Vec<WalFrameOwned>,
        sync_mode: WalSyncMode,
    ) -> Result<Vec<WalFramePtr>> {
        match self.enqueue(frames, sync_mode) {
            Some(ticket) => ticket.wait(),
            None => Ok(Vec::new()),
        }
    }

    /// Updates the commit batching configuration at runtime.
    pub fn set_config(&self, config: WalCommitConfig) {
        {
            let mut guard = self.config.lock();
            *guard = config.normalize();
        }
        self.wakeup.notify_one();
    }

    /// Returns a snapshot of the pending commit backlog for observability.
    pub fn backlog(&self) -> WalCommitBacklog {
        let guard = self.state.lock();
        let pending_frames = guard.pending.iter().map(|req| req.frames.len()).sum();
        WalCommitBacklog {
            pending_commits: guard.pending.len(),
            pending_frames,
            worker_running: guard.worker_running,
        }
    }

    fn spawn_worker(
        wal: Arc<Wal>,
        state: Arc<Mutex<CommitState>>,
        wakeup: Arc<Condvar>,
        config: Arc<Mutex<WalCommitConfig>>,
    ) {
        thread::spawn(move || Self::worker_loop(wal, state, wakeup, config));
    }

    fn worker_loop(
        wal: Arc<Wal>,
        state: Arc<Mutex<CommitState>>,
        wakeup: Arc<Condvar>,
        config: Arc<Mutex<WalCommitConfig>>,
    ) {
        let mut batch = Vec::new();
        loop {
            batch.clear();
            {
                let mut guard = state.lock();
                let Some(first) = guard.pending.pop_front() else {
                    guard.worker_running = false;
                    debug!("wal.committer.worker_exit");
                    break;
                };
                debug!(
                    pending_remaining = guard.pending.len(),
                    "wal.committer.worker_batch_head"
                );
                batch.push(first);
            }
            let config_snapshot = *config.lock();
            Self::coalesce_batch(&state, &wakeup, &mut batch, config_snapshot);
            let total_frames: usize = batch.iter().map(|r| r.frames.len()).sum();
            debug!(
                batch_commits = batch.len(),
                total_frames, "wal.committer.worker_batch_ready"
            );
            if let Err(err) = Self::apply_batch(&wal, &mut batch) {
                Self::fail_batch(&batch, &err);
                Self::fail_pending(&state, &err);
                let mut guard = state.lock();
                guard.pending.clear();
                guard.worker_running = false;
                break;
            }
        }
    }

    fn coalesce_batch(
        state: &Arc<Mutex<CommitState>>,
        wakeup: &Arc<Condvar>,
        batch: &mut Vec<Arc<CommitRequest>>,
        config: WalCommitConfig,
    ) {
        let start = Instant::now();
        let mut total_frames: usize = batch.iter().map(|r| r.frames.len()).sum();
        while batch.len() < config.max_batch_commits && total_frames < config.max_batch_frames {
            let remaining = match config.max_batch_wait.checked_sub(start.elapsed()) {
                Some(dur) if !dur.is_zero() => dur,
                _ => break,
            };
            let mut guard = state.lock();
            if guard.pending.is_empty() {
                let wait_result = wakeup.wait_for(&mut guard, remaining);
                if wait_result.timed_out() && guard.pending.is_empty() {
                    break;
                }
            }
            if let Some(req) = guard.pending.pop_front() {
                total_frames += req.frames.len();
                batch.push(req);
            } else {
                drop(guard);
                continue;
            }
        }
    }

    fn apply_batch(wal: &Wal, batch: &mut Vec<Arc<CommitRequest>>) -> Result<()> {
        let total_frames: usize = batch.iter().map(|req| req.frames.len()).sum();
        debug!(
            batch_commits = batch.len(),
            total_frames, "wal.committer.apply_batch.start"
        );
        let delay_ms = APPLY_BATCH_DELAY_MS.load(AtomicOrdering::Relaxed);
        if delay_ms > 0 {
            thread::sleep(Duration::from_millis(delay_ms));
        }
        let mut offsets_all: Vec<WalFramePtr> = Vec::new();
        if total_frames > 0 {
            let mut flat: Vec<WalFrame<'_>> = Vec::with_capacity(total_frames);
            for req in batch.iter() {
                for frame in &req.frames {
                    flat.push(WalFrame {
                        lsn: frame.lsn,
                        page_id: frame.page_id,
                        payload: frame.payload.as_slice(),
                    });
                }
            }
            offsets_all = wal.append_frame_batch(&flat)?;
            debug!(
                frames = flat.len(),
                "wal.committer.apply_batch.appended_frames"
            );
        }
        if batch
            .iter()
            .any(|req| matches!(req.sync_mode, WalSyncMode::Immediate))
        {
            debug!("wal.committer.apply_batch.sync_start");
            wal.sync()?;
            debug!("wal.committer.apply_batch.sync_complete");
        }
        let mut offset_index = 0usize;
        for req in batch.drain(..) {
            let frame_count = req.frames.len();
            let mut per_req_offsets = Vec::with_capacity(frame_count);
            for _ in 0..frame_count {
                if let Some(offset) = offsets_all.get(offset_index).copied() {
                    per_req_offsets.push(offset);
                }
                offset_index += 1;
            }
            req.finish(Ok(per_req_offsets));
        }
        Ok(())
    }

    fn fail_batch(batch: &[Arc<CommitRequest>], err: &SombraError) {
        for req in batch {
            req.finish(Err(clone_error(err)));
        }
    }

    fn fail_pending(state: &Arc<Mutex<CommitState>>, err: &SombraError) {
        let mut guard = state.lock();
        while let Some(req) = guard.pending.pop_front() {
            req.finish(Err(clone_error(err)));
        }
    }
}

#[derive(Default)]
struct CommitState {
    pending: VecDeque<Arc<CommitRequest>>,
    worker_running: bool,
}

struct CommitRequest {
    frames: Vec<WalFrameOwned>,
    sync_mode: WalSyncMode,
    result: Mutex<Option<Result<Vec<WalFramePtr>>>>,
    cv: Condvar,
}

impl CommitRequest {
    fn new(frames: Vec<WalFrameOwned>, sync_mode: WalSyncMode) -> Self {
        Self {
            frames,
            sync_mode,
            result: Mutex::new(None),
            cv: Condvar::new(),
        }
    }

    fn finish(&self, outcome: Result<Vec<WalFramePtr>>) {
        let mut result = self.result.lock();
        if result.is_none() {
            *result = Some(outcome);
            self.cv.notify_all();
        }
    }

    fn wait(&self) -> Result<Vec<WalFramePtr>> {
        let mut guard = self.result.lock();
        loop {
            if let Some(result) = guard.take() {
                return result;
            }
            self.cv.wait(&mut guard);
        }
    }
}

/// Iterator for reading frames from a WAL file.
///
/// Stops iteration when corruption is detected or end of valid frames is reached.
pub struct WalIterator {
    segments: Vec<SegmentIterState>,
    segment_index: usize,
    page_size: usize,
    prev_chain: u64,
    valid_up_to: u64,
    header: FileHeader,
}

struct SegmentIterState {
    io: Arc<StdFileIo>,
    offset: u64,
    end: u64,
    base: u64,
}

impl WalIterator {
    /// Reads the next frame from the WAL.
    ///
    /// Returns None when reaching the end of valid frames or detecting corruption.
    pub fn next_frame(&mut self) -> Result<Option<WalFrameOwned>> {
        while self.segment_index < self.segments.len() {
            let segment = &mut self.segments[self.segment_index];
            if segment.offset + FRAME_HEADER_LEN as u64 > segment.end {
                self.segment_index += 1;
                continue;
            }
            let mut header_buf = [0u8; FRAME_HEADER_LEN];
            let read = segment.io.read_at(segment.offset, &mut header_buf);
            if let Err(err) = read {
                if matches!(err, SombraError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof)
                {
                    self.segment_index = self.segments.len();
                    return Ok(None);
                }
                return Err(err);
            }
            let header = match FrameHeader::decode(&header_buf) {
                Ok(header) => header,
                Err(_) => {
                    debug!(
                        segment_id = segment.base,
                        offset = segment.offset,
                        "wal.iterator.header_decode_failed"
                    );
                    #[cfg(test)]
                    eprintln!(
                        "[wal.iter] header decode failed segment_base={} offset={}",
                        segment.base, segment.offset
                    );
                    self.segment_index = self.segments.len();
                    return Ok(None);
                }
            };
            if header.frame_lsn.0 < self.header.start_lsn.0 {
                return Err(SombraError::Corruption("wal frame lsn below start_lsn"));
            }
            if header.prev_crc32_chain != self.prev_chain {
                debug!(
                    expected_prev_chain = self.prev_chain,
                    observed_prev_chain = header.prev_crc32_chain,
                    segment_offset = segment.offset,
                    "wal.iterator.prev_chain_mismatch"
                );
                #[cfg(test)]
                eprintln!(
                    "[wal.iter] prev_chain mismatch expected={} observed={} offset={}",
                    self.prev_chain, header.prev_crc32_chain, segment.offset
                );
                self.segment_index = self.segments.len();
                return Ok(None);
            }
            let mut payload = vec![0u8; self.page_size];
            let payload_off = segment.offset + FRAME_HEADER_LEN as u64;
            let payload_res = segment.io.read_at(payload_off, &mut payload);
            if let Err(err) = payload_res {
                if matches!(err, SombraError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof)
                {
                    debug!(
                        segment_offset = segment.offset,
                        "wal.iterator.payload_truncated"
                    );
                    #[cfg(test)]
                    eprintln!("[wal.iter] payload truncated offset={}", segment.offset);
                    self.segment_index = self.segments.len();
                    return Ok(None);
                }
                return Err(err);
            }
            let payload_crc = compute_crc32(&[&payload]);
            if payload_crc != header.payload_crc32 {
                debug!(
                    expected_crc = header.payload_crc32,
                    observed_crc = payload_crc,
                    segment_offset = segment.offset,
                    "wal.iterator.payload_crc_mismatch"
                );
                #[cfg(test)]
                eprintln!(
                    "[wal.iter] payload crc mismatch expected={} observed={} offset={}",
                    header.payload_crc32, payload_crc, segment.offset
                );
                self.segment_index = self.segments.len();
                return Ok(None);
            }
            let mut encoded_header = header.encode();
            encoded_header[28..32].copy_from_slice(&header.header_crc32.to_be_bytes());
            let frame_size = FRAME_HEADER_LEN + self.page_size;
            let mut chain_hasher = Crc32Fast::default();
            chain_hasher.update(&self.prev_chain.to_be_bytes());
            chain_hasher.update(&encoded_header);
            chain_hasher.update(&payload);
            let chain_crc = chain_hasher.finalize();
            let new_chain = ((frame_size as u64) << 32) | u64::from(chain_crc);
            self.prev_chain = new_chain;
            segment.offset += frame_size as u64;
            self.valid_up_to = segment.base + segment.offset;
            return Ok(Some(WalFrameOwned {
                lsn: header.frame_lsn,
                page_id: header.page_id,
                payload,
            }));
        }
        Ok(None)
    }

    /// Returns the file offset up to which frames have been validated.
    pub fn valid_up_to(&self) -> u64 {
        self.valid_up_to
    }
}

fn detect_valid_prefix(
    io: &Arc<StdFileIo>,
    segment_len: u64,
    page_size: usize,
    header: &FileHeader,
) -> Result<u64> {
    let mut offset = FILE_HEADER_LEN as u64;
    let frame_size = FRAME_HEADER_LEN as u64 + page_size as u64;
    let mut prev_chain = 0u64;
    while offset + FRAME_HEADER_LEN as u64 <= segment_len {
        let mut header_buf = [0u8; FRAME_HEADER_LEN];
        if let Err(err) = io.read_at(offset, &mut header_buf) {
            if matches!(err, SombraError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof)
            {
                break;
            }
            return Err(err);
        }
        let frame_header = match FrameHeader::decode(&header_buf) {
            Ok(hdr) => hdr,
            Err(_) => break,
        };
        if frame_header.frame_lsn.0 < header.start_lsn.0 {
            break;
        }
        if frame_header.prev_crc32_chain != prev_chain {
            break;
        }
        let payload_off = offset + FRAME_HEADER_LEN as u64;
        if payload_off + page_size as u64 > segment_len {
            break;
        }
        let mut payload = vec![0u8; page_size];
        if let Err(err) = io.read_at(payload_off, &mut payload) {
            if matches!(err, SombraError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof)
            {
                break;
            }
            return Err(err);
        }
        let payload_crc = compute_crc32(&[&payload]);
        if payload_crc != frame_header.payload_crc32 {
            break;
        }
        let mut encoded_header = frame_header.encode();
        encoded_header[28..32].copy_from_slice(&frame_header.header_crc32.to_be_bytes());
        let mut chain_hasher = Crc32Fast::default();
        chain_hasher.update(&prev_chain.to_be_bytes());
        chain_hasher.update(&encoded_header);
        chain_hasher.update(&payload);
        let chain_crc = chain_hasher.finalize();
        prev_chain = (frame_size << 32) | u64::from(chain_crc);
        offset += frame_size;
    }
    Ok(offset)
}

fn clone_error(err: &SombraError) -> SombraError {
    match err {
        SombraError::Io(io_err) => {
            let kind = io_err.kind();
            let message = io_err.to_string();
            SombraError::Io(io::Error::new(kind, message))
        }
        SombraError::Corruption(msg) => SombraError::Corruption(msg),
        SombraError::Invalid(msg) => SombraError::Invalid(msg),
        SombraError::InvalidOwned(msg) => SombraError::InvalidOwned(msg.clone()),
        SombraError::NotFound => SombraError::NotFound,
        SombraError::Cancelled => SombraError::Cancelled,
    }
}

fn compute_crc32(chunks: &[&[u8]]) -> u32 {
    let mut hasher = Crc32Fast::default();
    for chunk in chunks {
        hasher.update(chunk);
    }
    hasher.finalize()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn wal_append_and_iterate_roundtrip() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_roundtrip");
        let wal = Wal::open(&path, WalOptions::new(4096, 42, Lsn(1)))?;
        let payload_a = vec![1u8; 4096];
        let _ = wal.append_frame(WalFrame {
            lsn: Lsn(1),
            page_id: PageId(1),
            payload: &payload_a,
        })?;
        let payload_b = vec![2u8; 4096];
        let _ = wal.append_frame(WalFrame {
            lsn: Lsn(2),
            page_id: PageId(2),
            payload: &payload_b,
        })?;
        wal.sync()?;

        let mut iter = wal.iter()?;
        let first = iter.next_frame()?.expect("first frame");
        assert_eq!(first.lsn, Lsn(1));
        assert_eq!(first.page_id, PageId(1));
        assert_eq!(first.payload, payload_a);
        let second = iter.next_frame()?.expect("second frame");
        assert_eq!(second.lsn, Lsn(2));
        assert_eq!(second.page_id, PageId(2));
        assert_eq!(second.payload, payload_b);
        assert!(iter.next_frame()?.is_none());
        Ok(())
    }

    #[test]
    fn wal_detects_corruption() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_corruption");
        let wal = Wal::open(&path, WalOptions::new(4096, 777, Lsn(5)))?;
        let payload = vec![3u8; 4096];
        let _ = wal.append_frame(WalFrame {
            lsn: Lsn(5),
            page_id: PageId(7),
            payload: &payload,
        })?;
        wal.sync()?;

        // Corrupt a byte in the payload.
        let segment = path.join("active").join("wal-000001");
        let io = StdFileIo::open(&segment)?;
        let mut buf = vec![0u8; FRAME_HEADER_LEN + 4096];
        io.read_at(FILE_HEADER_LEN as u64, &mut buf)?;
        buf[FRAME_HEADER_LEN + 10] ^= 0xFF;
        io.write_at(FILE_HEADER_LEN as u64, &buf)?;

        let mut iter = wal.iter()?;
        assert!(iter.next_frame()?.is_none());
        assert_eq!(iter.valid_up_to(), FILE_HEADER_LEN as u64);
        Ok(())
    }

    #[test]
    fn wal_committer_appends_and_syncs() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_committer_basic");
        let wal = Wal::open(&path, WalOptions::new(4096, 555, Lsn(10)))?;
        let committer = WalCommitter::new(Arc::clone(&wal), WalCommitConfig::default());
        let mut payload = vec![0u8; 4096];
        payload[0] = 42;
        let frame = WalFrameOwned {
            lsn: Lsn(10),
            page_id: PageId(3),
            payload,
        };
        let _ = committer.commit(vec![frame], WalSyncMode::Immediate)?;
        assert_eq!(wal.stats().frames_appended, 1);
        assert_eq!(wal.stats().coalesced_writes, 1);
        assert_eq!(wal.stats().syncs, 1);
        let mut iter = wal.iter()?;
        let frame = iter.next_frame()?.expect("frame available");
        assert_eq!(frame.page_id, PageId(3));
        assert_eq!(frame.payload[0], 42);
        Ok(())
    }

    #[test]
    fn wal_committer_empty_batch_syncs() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_committer_empty");
        let wal = Wal::open(&path, WalOptions::new(4096, 111, Lsn(1)))?;
        let committer = WalCommitter::new(Arc::clone(&wal), WalCommitConfig::default());
        let _ = committer.commit(Vec::new(), WalSyncMode::Immediate)?;
        assert_eq!(wal.stats().syncs, 1);
        Ok(())
    }

    #[test]
    fn wal_committer_update_config_runtime() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_committer_update");
        let wal = Wal::open(&path, WalOptions::new(4096, 222, Lsn(1)))?;
        let committer = WalCommitter::new(Arc::clone(&wal), WalCommitConfig::default());
        committer.set_config(WalCommitConfig {
            max_batch_commits: 0,
            max_batch_frames: 0,
            max_batch_wait: Duration::from_millis(0),
        });
        let payload = vec![9u8; 4096];
        let _ = committer.commit(
            vec![WalFrameOwned {
                lsn: Lsn(1),
                page_id: PageId(1),
                payload,
            }],
            WalSyncMode::Immediate,
        )?;
        assert_eq!(wal.stats().frames_appended, 1);
        assert_eq!(wal.stats().coalesced_writes, 1);
        Ok(())
    }

    #[test]
    fn wal_rejects_file_path() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("walfile");
        File::create(&path).unwrap();
        let err = match Wal::open(&path, WalOptions::new(4096, 7, Lsn(1))) {
            Ok(_) => panic!("expected wal open to fail on file path"),
            Err(err) => err,
        };
        let message = match err {
            SombraError::Invalid(msg) => msg.to_string(),
            SombraError::InvalidOwned(msg) => msg,
            other => panic!("unexpected error: {other:?}"),
        };
        assert!(
            message.contains("wal path must be a directory"),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn wal_rotates_and_iterates_across_segments() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_rotate");
        let mut opts = WalOptions::new(512, 9, Lsn(1));
        // Force each segment to accommodate exactly one frame to trigger rotation.
        opts.segment_size_bytes = (FILE_HEADER_LEN + FRAME_HEADER_LEN + 512) as u64 + 1;
        opts.preallocate_segments = 0;
        let wal = Wal::open(&path, opts)?;
        let a = vec![1u8; 512];
        wal.append_frame(WalFrame {
            lsn: Lsn(1),
            page_id: PageId(1),
            payload: &a,
        })?;
        let b = vec![2u8; 512];
        wal.append_frame(WalFrame {
            lsn: Lsn(2),
            page_id: PageId(2),
            payload: &b,
        })?;
        let mut iter = wal.iter()?;
        let first = iter.next_frame()?.expect("first frame");
        assert_eq!(first.lsn, Lsn(1));
        assert_eq!(first.payload, a);
        let second = iter.next_frame()?.expect("second frame");
        assert_eq!(second.lsn, Lsn(2));
        assert_eq!(second.payload, b);
        assert!(iter.next_frame()?.is_none());
        Ok(())
    }

    #[test]
    fn wal_recycles_segments_on_reset() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_recycle_reset");
        let mut opts = WalOptions::new(512, 5, Lsn(1));
        opts.segment_size_bytes = (FILE_HEADER_LEN + FRAME_HEADER_LEN + 512) as u64 + 1;
        opts.preallocate_segments = 0;
        let wal = Wal::open(&path, opts)?;
        let payload = vec![3u8; 512];
        wal.append_frame(WalFrame {
            lsn: Lsn(1),
            page_id: PageId(1),
            payload: &payload,
        })?;
        let payload_b = vec![4u8; 512];
        wal.append_frame(WalFrame {
            lsn: Lsn(2),
            page_id: PageId(2),
            payload: &payload_b,
        })?;
        wal.reset(Lsn(5))?;
        let allocator = wal.allocator_stats();
        assert_eq!(allocator.preallocate_segments, 0);
        assert!(
            allocator.recycle_segments >= 1,
            "expected recycled segments after reset, got {:?}",
            allocator
        );
        assert_eq!(wal.stats().frames_appended, 0);
        Ok(())
    }
}
fn initialize_segment_file(path: &Path, header: &FileHeader, capacity: u64) -> Result<StdFileIo> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    let io = StdFileIo::new(file);
    io.truncate(capacity)?;
    io.write_at(0, &header.encode())?;
    Ok(io)
}
