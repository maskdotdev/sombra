#![allow(unsafe_code)]

use std::fs::{File, OpenOptions};
use std::io;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::types::{Result, SombraError};
use parking_lot::Mutex;

/// Single-writer, multiple-reader lock coordinator using file-based locking.
#[derive(Clone)]
pub struct SingleWriter {
    inner: Arc<Inner>,
}

struct Inner {
    file: Arc<File>,
    state: Mutex<LockState>,
}

#[derive(Default, Debug)]
struct LockState {
    readers: u32,
    writer: bool,
    checkpoint: bool,
}

/// Snapshot of lock state for observability.
#[derive(Default, Debug, Clone, Copy)]
pub struct LockSnapshot {
    /// Number of active readers.
    pub readers: u32,
    /// Whether the writer lock is held.
    pub writer: bool,
    /// Whether a checkpoint lock is held.
    pub checkpoint: bool,
}

/// Guard representing a held reader lock.
pub struct ReaderGuard {
    _guard: SlotGuard,
}

/// Guard representing a held writer lock.
pub struct WriterGuard {
    _guard: SlotGuard,
}

/// Guard representing a held checkpoint lock that blocks readers.
pub struct CheckpointGuard {
    checkpoint_guard: Option<SlotGuard>,
    reader_block: Option<RangeGuard>,
}

impl Drop for CheckpointGuard {
    fn drop(&mut self) {
        if let Some(guard) = self.checkpoint_guard.take() {
            drop(guard);
        }
        if let Some(reader_block) = self.reader_block.take() {
            drop(reader_block);
        }
    }
}

impl SingleWriter {
    /// Opens or creates a lock file at the specified path for coordinating access.
    ///
    /// Creates a new lock file if it doesn't exist, ensuring it has the correct size
    /// for managing reader, writer, and checkpoint lock slots.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(SombraError::from)?;
        ensure_lock_file_size(&file)?;
        Ok(Self {
            inner: Arc::new(Inner {
                file: Arc::new(file),
                state: Mutex::new(LockState::default()),
            }),
        })
    }

    /// Acquires a reader lock, blocking until available.
    ///
    /// Multiple readers can hold locks concurrently. This method will block if a
    /// checkpoint operation is in progress, waiting for it to complete.
    pub fn acquire_reader(&self) -> Result<ReaderGuard> {
        loop {
            {
                let state = self.inner.state.lock();
                if state.checkpoint {
                    drop(state);
                    std::thread::sleep(Duration::from_millis(5));
                    continue;
                }
            }
            lock_slot_blocking(&self.inner.file, Slot::Reader)?;
            let mut state = self.inner.state.lock();
            if state.checkpoint {
                drop(state);
                unlock_range(&self.inner.file, READER_SLOT.start, READER_SLOT.len)?;
                std::thread::sleep(Duration::from_millis(5));
                continue;
            }
            state.readers = state.readers.saturating_add(1);
            break;
        }
        Ok(ReaderGuard {
            _guard: SlotGuard::new(self.inner.clone(), Slot::Reader),
        })
    }

    /// Acquires the writer lock, blocking until available.
    ///
    /// Only one writer can hold the lock at a time. Returns an error if the writer
    /// lock is already held by the current process.
    pub fn acquire_writer(&self) -> Result<WriterGuard> {
        loop {
            let mut state = self.inner.state.lock();
            if state.checkpoint {
                drop(state);
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
            if state.writer {
                return Err(SombraError::Invalid("writer lock already held"));
            }
            state.writer = true;
            break;
        }
        let result = lock_slot_blocking(&self.inner.file, Slot::Writer);
        if let Err(err) = result {
            self.inner.state.lock().writer = false;
            return Err(err);
        }
        Ok(WriterGuard {
            _guard: SlotGuard::new(self.inner.clone(), Slot::Writer),
        })
    }

    /// Attempts to acquire a checkpoint lock without blocking.
    ///
    /// Checkpoint locks are exclusive - they prevent both readers and writers
    /// from acquiring locks. Returns `None` if any locks are currently held.
    pub fn try_acquire_checkpoint(&self) -> Result<Option<CheckpointGuard>> {
        {
            let state = self.inner.state.lock();
            if state.readers > 0 || state.writer || state.checkpoint {
                return Ok(None);
            }
        }
        if !try_lock_range(&self.inner.file, READER_SLOT.start, READER_SLOT.len, true)? {
            return Ok(None);
        }
        let reader_guard = RangeGuard::new(self.inner.clone(), READER_SLOT.start, READER_SLOT.len);
        if !try_lock_slot(&self.inner.file, Slot::Checkpoint)? {
            drop(reader_guard);
            return Ok(None);
        }
        let checkpoint_guard = SlotGuard::new(self.inner.clone(), Slot::Checkpoint);
        {
            let mut state = self.inner.state.lock();
            if state.readers > 0 || state.writer || state.checkpoint {
                drop(state);
                drop(checkpoint_guard);
                drop(reader_guard);
                return Ok(None);
            }
            state.checkpoint = true;
        }
        Ok(Some(CheckpointGuard {
            checkpoint_guard: Some(checkpoint_guard),
            reader_block: Some(reader_guard),
        }))
    }

    /// Returns a snapshot of the current lock state.
    pub fn snapshot(&self) -> LockSnapshot {
        let state = self.inner.state.lock();
        LockSnapshot {
            readers: state.readers,
            writer: state.writer,
            checkpoint: state.checkpoint,
        }
    }
}

struct SlotGuard {
    inner: Arc<Inner>,
    slot: Slot,
}

impl SlotGuard {
    fn new(inner: Arc<Inner>, slot: Slot) -> Self {
        Self { inner, slot }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        {
            let mut state = self.inner.state.lock();
            match self.slot {
                Slot::Reader => {
                    state.readers = state.readers.saturating_sub(1);
                }
                Slot::Writer => {
                    state.writer = false;
                }
                Slot::Checkpoint => {
                    state.checkpoint = false;
                }
            }
        }
        if let Err(_err) = unlock_range(&self.inner.file, self.slot.start(), self.slot.len()) {
            #[cfg(debug_assertions)]
            eprintln!("failed to unlock {:?}: {:?}", self.slot, _err);
        }
    }
}

struct RangeGuard {
    inner: Arc<Inner>,
    start: u64,
    len: u64,
}

impl RangeGuard {
    fn new(inner: Arc<Inner>, start: u64, len: u64) -> Self {
        Self { inner, start, len }
    }
}

impl Drop for RangeGuard {
    fn drop(&mut self) {
        if let Err(_err) = unlock_range(&self.inner.file, self.start, self.len) {
            #[cfg(debug_assertions)]
            eprintln!(
                "failed to unlock range [{}, {}): {:?}",
                self.start,
                self.start + self.len,
                _err
            );
        }
    }
}

#[derive(Copy, Clone, Debug)]
enum Slot {
    Reader,
    Writer,
    Checkpoint,
}

impl Slot {
    fn start(self) -> u64 {
        match self {
            Slot::Reader => READER_SLOT.start,
            Slot::Writer => WRITER_SLOT.start,
            Slot::Checkpoint => CHECKPOINT_SLOT.start,
        }
    }

    fn len(self) -> u64 {
        1
    }

    fn exclusive(self) -> bool {
        !matches!(self, Slot::Reader)
    }
}

struct SlotSpec {
    start: u64,
    len: u64,
}

const READER_SLOT: SlotSpec = SlotSpec { start: 0, len: 1 };
const WRITER_SLOT: SlotSpec = SlotSpec { start: 1, len: 1 };
const CHECKPOINT_SLOT: SlotSpec = SlotSpec { start: 2, len: 1 };

fn ensure_lock_file_size(file: &File) -> Result<()> {
    let metadata = file.metadata().map_err(SombraError::from)?;
    if metadata.len() < 3 {
        file.set_len(3).map_err(SombraError::from)?;
    }
    Ok(())
}

fn lock_slot_blocking(file: &Arc<File>, slot: Slot) -> Result<()> {
    lock_range_impl(file, slot.start(), slot.len(), slot.exclusive(), true)?;
    Ok(())
}

fn try_lock_slot(file: &Arc<File>, slot: Slot) -> Result<bool> {
    lock_range_impl(file, slot.start(), slot.len(), slot.exclusive(), false)
}

fn try_lock_range(file: &Arc<File>, start: u64, len: u64, exclusive: bool) -> Result<bool> {
    lock_range_impl(file, start, len, exclusive, false)
}

fn lock_range_impl(
    file: &Arc<File>,
    start: u64,
    len: u64,
    exclusive: bool,
    blocking: bool,
) -> Result<bool> {
    lock_range_inner(file, start, len, exclusive, blocking).map_err(SombraError::from)
}

fn lock_range_inner(
    file: &Arc<File>,
    start: u64,
    len: u64,
    exclusive: bool,
    blocking: bool,
) -> io::Result<bool> {
    #[cfg(unix)]
    {
        unix::lock_region(file, start, len, exclusive, blocking)
    }
    #[cfg(windows)]
    {
        windows::lock_region(file, start, len, exclusive, blocking)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, start, len, exclusive, blocking);
        Err(io::Error::new(
            io::ErrorKind::Other,
            "file locking unsupported on this platform",
        ))
    }
}

fn unlock_range(file: &Arc<File>, start: u64, len: u64) -> Result<()> {
    #[cfg(unix)]
    {
        unix::unlock_region(file, start, len).map_err(SombraError::from)
    }
    #[cfg(windows)]
    {
        windows::unlock_region(file, start, len).map_err(SombraError::from)
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, start, len);
        Err(SombraError::Invalid(
            "file locking unsupported on this platform",
        ))
    }
}

#[cfg(unix)]
mod unix {
    use super::*;
    use std::os::unix::io::AsRawFd;

    pub fn lock_region(
        file: &Arc<File>,
        start: u64,
        len: u64,
        exclusive: bool,
        blocking: bool,
    ) -> io::Result<bool> {
        let fd = file.as_raw_fd();
        let mut flock = libc::flock {
            l_type: if exclusive {
                libc::F_WRLCK
            } else {
                libc::F_RDLCK
            },
            l_whence: libc::SEEK_SET as _,
            l_start: start as libc::off_t,
            l_len: len as libc::off_t,
            l_pid: 0,
        };
        let cmd = if blocking {
            libc::F_SETLKW
        } else {
            libc::F_SETLK
        };
        loop {
            let res = unsafe { libc::fcntl(fd, cmd, &mut flock) };
            if res == 0 {
                return Ok(true);
            }
            let err = io::Error::last_os_error();
            match err.raw_os_error() {
                Some(libc::EINTR) if blocking => continue,
                Some(libc::EAGAIN) | Some(libc::EACCES) if !blocking => return Ok(false),
                _ => return Err(err),
            }
        }
    }

    pub fn unlock_region(file: &Arc<File>, start: u64, len: u64) -> io::Result<()> {
        let fd = file.as_raw_fd();
        let mut flock = libc::flock {
            l_type: libc::F_UNLCK,
            l_whence: libc::SEEK_SET as _,
            l_start: start as libc::off_t,
            l_len: len as libc::off_t,
            l_pid: 0,
        };
        let res = unsafe { libc::fcntl(fd, libc::F_SETLK, &mut flock) };
        if res == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

#[cfg(windows)]
mod windows {
    use super::*;
    use std::mem::zeroed;
    use std::os::windows::io::AsRawHandle;
    use windows_sys::Win32::Foundation::ERROR_LOCK_VIOLATION;
    use windows_sys::Win32::Storage::FileSystem::{
        LockFileEx, UnlockFileEx, LOCKFILE_EXCLUSIVE_LOCK, LOCKFILE_FAIL_IMMEDIATELY,
    };
    use windows_sys::Win32::System::IO::OVERLAPPED;

    pub fn lock_region(
        file: &Arc<File>,
        start: u64,
        len: u64,
        exclusive: bool,
        blocking: bool,
    ) -> io::Result<bool> {
        unsafe {
            let handle = file.as_raw_handle();
            let mut overlapped: OVERLAPPED = zeroed();
            overlapped.Offset = start as u32;
            overlapped.OffsetHigh = (start >> 32) as u32;
            let mut flags = 0;
            if exclusive {
                flags |= LOCKFILE_EXCLUSIVE_LOCK;
            }
            if !blocking {
                flags |= LOCKFILE_FAIL_IMMEDIATELY;
            }
            let low = len as u32;
            let high = (len >> 32) as u32;
            let res = LockFileEx(handle as isize, flags, 0, low, high, &mut overlapped);
            if res != 0 {
                Ok(true)
            } else {
                let err = io::Error::last_os_error();
                if !blocking && matches!(err.raw_os_error(), Some(ERROR_LOCK_VIOLATION)) {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }

    pub fn unlock_region(file: &Arc<File>, start: u64, len: u64) -> io::Result<()> {
        unsafe {
            let handle = file.as_raw_handle();
            let mut overlapped: OVERLAPPED = zeroed();
            overlapped.Offset = start as u32;
            overlapped.OffsetHigh = (start >> 32) as u32;
            let low = len as u32;
            let high = (len >> 32) as u32;
            let res = UnlockFileEx(handle as isize, 0, low, high, &mut overlapped);
            if res != 0 {
                Ok(())
            } else {
                Err(io::Error::last_os_error())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SombraError;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::thread;
    use std::time::Duration;
    use tempfile::tempdir;

    #[test]
    fn reader_locks_stack() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("reader_stack.lock");
        let manager = SingleWriter::open(&path)?;
        let _a = manager.acquire_reader()?;
        let _b = manager.acquire_reader()?;
        Ok(())
    }

    #[test]
    fn writer_blocks_until_release() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("writer_block.lock");
        let manager = SingleWriter::open(&path)?;
        let guard = manager.acquire_writer()?;
        let manager_clone = manager.clone();
        let handle = thread::spawn(move || loop {
            match manager_clone.acquire_writer() {
                Ok(inner) => return inner,
                Err(SombraError::Invalid("writer lock already held")) => {
                    thread::sleep(Duration::from_millis(10));
                    continue;
                }
                Err(err) => panic!("unexpected error: {err:?}"),
            }
        });
        thread::sleep(Duration::from_millis(50));
        drop(guard);
        let new_guard = handle.join().unwrap();
        drop(new_guard);
        let final_guard = manager.acquire_writer()?;
        drop(final_guard);
        Ok(())
    }

    #[test]
    fn checkpoint_blocks_new_readers_until_release() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint_block.lock");
        let manager = SingleWriter::open(&path)?;
        let guard = manager
            .try_acquire_checkpoint()?
            .expect("checkpoint lock acquired");
        let flag = Arc::new(AtomicBool::new(false));
        let waiter_flag = Arc::clone(&flag);
        let manager_clone = manager.clone();
        let handle = thread::spawn(move || {
            let _reader = manager_clone.acquire_reader().expect("reader lock");
            waiter_flag.store(true, Ordering::SeqCst);
        });
        thread::sleep(Duration::from_millis(50));
        assert!(
            !flag.load(Ordering::SeqCst),
            "reader should block while checkpoint holds reader slot"
        );
        drop(guard);
        handle.join().unwrap();
        assert!(
            flag.load(Ordering::SeqCst),
            "reader should acquire once checkpoint released"
        );
        Ok(())
    }

    #[test]
    fn checkpoint_skips_when_reader_active() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("checkpoint_skip.lock");
        let manager = SingleWriter::open(&path)?;
        let reader = manager.acquire_reader()?;
        assert!(manager.try_acquire_checkpoint()?.is_none());
        drop(reader);
        assert!(manager.try_acquire_checkpoint()?.is_some());
        Ok(())
    }
}
