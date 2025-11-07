#![forbid(unsafe_code)]

use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use parking_lot::{Condvar, Mutex};
use sombra_checksum::{Checksum, Crc32Fast};
use sombra_io::FileIo;
use sombra_types::{Lsn, PageId, Result, SombraError};

const WAL_MAGIC: [u8; 4] = *b"SOMW";
const WAL_FORMAT_VERSION: u16 = 1;
const FILE_HEADER_LEN: usize = 32;
const FRAME_HEADER_LEN: usize = 32;

#[derive(Clone, Debug)]
pub struct WalOptions {
    pub page_size: u32,
    pub wal_salt: u64,
    pub start_lsn: Lsn,
}

impl WalOptions {
    pub fn new(page_size: u32, wal_salt: u64, start_lsn: Lsn) -> Self {
        Self {
            page_size,
            wal_salt,
            start_lsn,
        }
    }
}

impl Default for WalOptions {
    fn default() -> Self {
        Self {
            page_size: 0,
            wal_salt: 0,
            start_lsn: Lsn(0),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct WalStats {
    pub frames_appended: u64,
    pub bytes_appended: u64,
    pub syncs: u64,
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

struct WalState {
    header: FileHeader,
    append_offset: u64,
    prev_chain: u64,
    stats: WalStats,
}

impl WalState {
    fn new(header: FileHeader, append_offset: u64) -> Self {
        Self {
            header,
            append_offset,
            prev_chain: 0,
            stats: WalStats::default(),
        }
    }
}

pub struct Wal {
    io: Arc<dyn FileIo>,
    page_size: usize,
    state: Mutex<WalState>,
}

impl Wal {
    pub fn open(io: Arc<dyn FileIo>, options: WalOptions) -> Result<Self> {
        if options.page_size == 0 {
            return Err(SombraError::Invalid("wal page size must be non-zero"));
        }
        let len = io.len()?;
        let header = if len < FILE_HEADER_LEN as u64 {
            let header = FileHeader::new(options.page_size, options.wal_salt, options.start_lsn);
            io.write_at(0, &header.encode())?;
            io.truncate(FILE_HEADER_LEN as u64)?;
            header
        } else {
            let mut buf = [0u8; FILE_HEADER_LEN];
            io.read_at(0, &mut buf)?;
            let header = FileHeader::decode(&buf)?;
            if header.page_size != options.page_size {
                return Err(SombraError::Corruption("wal page size mismatch"));
            }
            if header.wal_salt != options.wal_salt {
                return Err(SombraError::Corruption("wal salt mismatch"));
            }
            header
        };
        let append_offset = io.len()?.max(FILE_HEADER_LEN as u64);
        let wal = Self {
            io,
            page_size: options.page_size as usize,
            state: Mutex::new(WalState::new(header, append_offset)),
        };
        Ok(wal)
    }

    pub fn reset(&self, start_lsn: Lsn) -> Result<()> {
        let mut state = self.state.lock();
        state.header = FileHeader::new(state.header.page_size, state.header.wal_salt, start_lsn);
        state.prev_chain = 0;
        state.stats = WalStats::default();
        self.io.write_at(0, &state.header.encode())?;
        self.io.truncate(FILE_HEADER_LEN as u64)?;
        state.append_offset = FILE_HEADER_LEN as u64;
        Ok(())
    }

    pub fn append_frame(&self, frame: WalFrame<'_>) -> Result<()> {
        if frame.payload.len() != self.page_size {
            return Err(SombraError::Invalid("wal frame payload size mismatch"));
        }
        let mut state = self.state.lock();
        if frame.lsn.0 < state.header.start_lsn.0 {
            return Err(SombraError::Invalid("wal frame lsn below start_lsn"));
        }
        if state.stats.frames_appended == 0 && frame.lsn.0 > state.header.start_lsn.0 {
            state.header.start_lsn = frame.lsn;
            self.io.write_at(0, &state.header.encode())?;
        }
        let payload_crc32 = compute_crc32(&[frame.payload]);
        let mut header =
            FrameHeader::new(frame.lsn, frame.page_id, state.prev_chain, payload_crc32);
        let encoded_header = header.encode_with_crc();
        header.header_crc32 = u32::from_be_bytes(encoded_header[28..32].try_into().unwrap());
        let frame_size = FRAME_HEADER_LEN + self.page_size;
        let mut chain_hasher = Crc32Fast::default();
        chain_hasher.update(&state.prev_chain.to_be_bytes());
        chain_hasher.update(&encoded_header);
        chain_hasher.update(frame.payload);
        let chain_crc = chain_hasher.finalize();
        let new_chain = ((frame_size as u64) << 32) | u64::from(chain_crc);
        self.io.write_at(state.append_offset, &encoded_header)?;
        self.io
            .write_at(state.append_offset + FRAME_HEADER_LEN as u64, frame.payload)?;
        state.append_offset += frame_size as u64;
        state.prev_chain = new_chain;
        state.stats.frames_appended += 1;
        state.stats.bytes_appended += frame_size as u64;
        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        self.io.sync_all()?;
        let mut state = self.state.lock();
        state.stats.syncs += 1;
        Ok(())
    }

    pub fn iter(&self) -> Result<WalIterator> {
        let len = self.io.len()?;
        if len < FILE_HEADER_LEN as u64 {
            return Err(SombraError::Corruption("wal truncated header"));
        }
        let mut header_buf = [0u8; FILE_HEADER_LEN];
        self.io.read_at(0, &mut header_buf)?;
        let header = FileHeader::decode(&header_buf)?;
        Ok(WalIterator {
            io: Arc::clone(&self.io),
            page_size: self.page_size,
            offset: FILE_HEADER_LEN as u64,
            end: len,
            prev_chain: 0,
            valid_up_to: FILE_HEADER_LEN as u64,
            header,
        })
    }

    pub fn stats(&self) -> WalStats {
        let state = self.state.lock();
        state.stats.clone()
    }

    pub fn len(&self) -> Result<u64> {
        self.io.len()
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? <= FILE_HEADER_LEN as u64)
    }
}

pub struct WalFrame<'a> {
    pub lsn: Lsn,
    pub page_id: PageId,
    pub payload: &'a [u8],
}

pub struct WalFrameOwned {
    pub lsn: Lsn,
    pub page_id: PageId,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalSyncMode {
    Immediate,
    Deferred,
    Off,
}

#[derive(Clone, Copy, Debug)]
pub struct WalCommitConfig {
    pub max_batch_commits: usize,
    pub max_batch_frames: usize,
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

pub struct WalCommitter {
    wal: Arc<Wal>,
    state: Arc<Mutex<CommitState>>,
    wakeup: Arc<Condvar>,
    config: Arc<Mutex<WalCommitConfig>>,
}

pub struct WalCommitTicket {
    request: Arc<CommitRequest>,
}

impl WalCommitTicket {
    pub fn wait(self) -> Result<()> {
        self.request.wait()
    }
}

impl WalCommitter {
    pub fn new(wal: Arc<Wal>, config: WalCommitConfig) -> Self {
        let cfg = config.normalize();
        Self {
            wal,
            state: Arc::new(Mutex::new(CommitState::default())),
            wakeup: Arc::new(Condvar::new()),
            config: Arc::new(Mutex::new(cfg)),
        }
    }

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

    pub fn commit(&self, frames: Vec<WalFrameOwned>, sync_mode: WalSyncMode) -> Result<()> {
        match self.enqueue(frames, sync_mode) {
            Some(ticket) => ticket.wait(),
            None => Ok(()),
        }
    }

    pub fn set_config(&self, config: WalCommitConfig) {
        {
            let mut guard = self.config.lock();
            *guard = config.normalize();
        }
        self.wakeup.notify_one();
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
                    break;
                };
                batch.push(first);
            }
            let config_snapshot = *config.lock();
            Self::coalesce_batch(&state, &wakeup, &mut batch, config_snapshot);
            if let Err(err) = Self::apply_batch(&wal, &batch) {
                Self::fail_batch(&batch, &err);
                Self::fail_pending(&state, &err);
                let mut guard = state.lock();
                guard.pending.clear();
                guard.worker_running = false;
                break;
            }
            for req in batch.drain(..) {
                req.finish(Ok(()));
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

    fn apply_batch(wal: &Wal, batch: &[Arc<CommitRequest>]) -> Result<()> {
        for req in batch {
            for frame in &req.frames {
                wal.append_frame(WalFrame {
                    lsn: frame.lsn,
                    page_id: frame.page_id,
                    payload: &frame.payload,
                })?;
            }
        }
        if batch
            .iter()
            .any(|req| matches!(req.sync_mode, WalSyncMode::Immediate))
        {
            wal.sync()?;
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
    result: Mutex<Option<Result<()>>>,
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

    fn finish(&self, outcome: Result<()>) {
        let mut result = self.result.lock();
        if result.is_none() {
            *result = Some(outcome);
            self.cv.notify_all();
        }
    }

    fn wait(&self) -> Result<()> {
        let mut guard = self.result.lock();
        loop {
            if let Some(result) = guard.take() {
                return result;
            }
            self.cv.wait(&mut guard);
        }
    }
}

pub struct WalIterator {
    io: Arc<dyn FileIo>,
    page_size: usize,
    offset: u64,
    end: u64,
    prev_chain: u64,
    valid_up_to: u64,
    header: FileHeader,
}

impl WalIterator {
    pub fn next_frame(&mut self) -> Result<Option<WalFrameOwned>> {
        if self.offset + FRAME_HEADER_LEN as u64 > self.end {
            self.offset = self.end;
            return Ok(None);
        }
        let mut header_buf = [0u8; FRAME_HEADER_LEN];
        let read = self.io.read_at(self.offset, &mut header_buf);
        if let Err(err) = read {
            if matches!(err, SombraError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof)
            {
                self.offset = self.end;
                return Ok(None);
            }
            return Err(err);
        }
        let header = match FrameHeader::decode(&header_buf) {
            Ok(header) => header,
            Err(_) => {
                self.offset = self.end;
                return Ok(None);
            }
        };
        if header.frame_lsn.0 < self.header.start_lsn.0 {
            return Err(SombraError::Corruption("wal frame lsn below start_lsn"));
        }
        if header.prev_crc32_chain != self.prev_chain {
            self.offset = self.end;
            return Ok(None);
        }
        let mut payload = vec![0u8; self.page_size];
        let payload_off = self.offset + FRAME_HEADER_LEN as u64;
        let payload_res = self.io.read_at(payload_off, &mut payload);
        if let Err(err) = payload_res {
            if matches!(err, SombraError::Io(ref io_err) if io_err.kind() == std::io::ErrorKind::UnexpectedEof)
            {
                self.offset = self.end;
                return Ok(None);
            }
            return Err(err);
        }
        let payload_crc = compute_crc32(&[&payload]);
        if payload_crc != header.payload_crc32 {
            self.offset = self.end;
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
        self.offset += frame_size as u64;
        self.valid_up_to = self.offset;
        Ok(Some(WalFrameOwned {
            lsn: header.frame_lsn,
            page_id: header.page_id,
            payload,
        }))
    }

    pub fn valid_up_to(&self) -> u64 {
        self.valid_up_to
    }
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
        SombraError::NotFound => SombraError::NotFound,
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
    use sombra_io::StdFileIo;
    use tempfile::tempdir;

    #[test]
    fn wal_append_and_iterate_roundtrip() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_roundtrip");
        let io = StdFileIo::open(&path)?;
        let wal = Wal::open(Arc::new(io), WalOptions::new(4096, 42, Lsn(1)))?;
        let payload_a = vec![1u8; 4096];
        wal.append_frame(WalFrame {
            lsn: Lsn(1),
            page_id: PageId(1),
            payload: &payload_a,
        })?;
        let payload_b = vec![2u8; 4096];
        wal.append_frame(WalFrame {
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
        let io = StdFileIo::open(&path)?;
        let wal = Wal::open(Arc::new(io.clone()), WalOptions::new(4096, 777, Lsn(5)))?;
        let payload = vec![3u8; 4096];
        wal.append_frame(WalFrame {
            lsn: Lsn(5),
            page_id: PageId(7),
            payload: &payload,
        })?;
        wal.sync()?;

        // Corrupt a byte in the payload.
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
        let io = StdFileIo::open(&path)?;
        let wal = Arc::new(Wal::open(
            Arc::new(io),
            WalOptions::new(4096, 555, Lsn(10)),
        )?);
        let committer = WalCommitter::new(Arc::clone(&wal), WalCommitConfig::default());
        let mut payload = vec![0u8; 4096];
        payload[0] = 42;
        let frame = WalFrameOwned {
            lsn: Lsn(10),
            page_id: PageId(3),
            payload,
        };
        committer.commit(vec![frame], WalSyncMode::Immediate)?;
        assert_eq!(wal.stats().frames_appended, 1);
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
        let io = StdFileIo::open(&path)?;
        let wal = Arc::new(Wal::open(Arc::new(io), WalOptions::new(4096, 111, Lsn(1)))?);
        let committer = WalCommitter::new(Arc::clone(&wal), WalCommitConfig::default());
        committer.commit(Vec::new(), WalSyncMode::Immediate)?;
        assert_eq!(wal.stats().syncs, 1);
        Ok(())
    }

    #[test]
    fn wal_committer_update_config_runtime() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("wal_committer_update");
        let io = StdFileIo::open(&path)?;
        let wal = Arc::new(Wal::open(Arc::new(io), WalOptions::new(4096, 222, Lsn(1)))?);
        let committer = WalCommitter::new(Arc::clone(&wal), WalCommitConfig::default());
        committer.set_config(WalCommitConfig {
            max_batch_commits: 0,
            max_batch_frames: 0,
            max_batch_wait: Duration::from_millis(0),
        });
        let payload = vec![9u8; 4096];
        committer.commit(
            vec![WalFrameOwned {
                lsn: Lsn(1),
                page_id: PageId(1),
                payload,
            }],
            WalSyncMode::Immediate,
        )?;
        assert_eq!(wal.stats().frames_appended, 1);
        Ok(())
    }
}
