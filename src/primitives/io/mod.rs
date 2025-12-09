#![forbid(unsafe_code)]

use std::{
    fs::File,
    io::{self, IoSlice},
    path::Path,
    sync::Arc,
};

use crate::types::{Result, SombraError};

#[cfg(test)]
macro_rules! io_test_log {
    ($($arg:tt)*) => {
        eprintln!($($arg)*);
    };
}

#[cfg(not(test))]
macro_rules! io_test_log {
    ($($arg:tt)*) => {
        if false {
            let _ = format_args!($($arg)*);
        }
    };
}

/// Trait for performing positioned file I/O operations.
pub trait FileIo: Send + Sync + 'static {
    /// Reads bytes from the file at the specified offset into the buffer.
    fn read_at(&self, off: u64, dst: &mut [u8]) -> Result<()>;
    /// Writes bytes to the file at the specified offset from the buffer.
    fn write_at(&self, off: u64, src: &[u8]) -> Result<()>;
    /// Writes multiple buffers to the file at the specified offset.
    fn writev_at(&self, mut off: u64, bufs: &[IoSlice<'_>]) -> Result<()> {
        for slice in bufs {
            if slice.is_empty() {
                continue;
            }
            self.write_at(off, slice)?;
            off = off
                .checked_add(slice.len() as u64)
                .ok_or(SombraError::Invalid("writev offset overflow"))?;
        }
        Ok(())
    }
    /// Synchronizes all file data and metadata to disk.
    fn sync_all(&self) -> Result<()>;
    /// Returns the current length of the file in bytes.
    fn len(&self) -> Result<u64>;
    /// Returns true if the file is empty.
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
    /// Truncates or extends the file to the specified length.
    fn truncate(&self, len: u64) -> Result<()>;
}

#[cfg(unix)]
/// Unix-specific file I/O operations using POSIX APIs.
pub mod stdio_unix {
    use std::{
        fs::{File, OpenOptions},
        io::{self, ErrorKind},
        os::unix::fs::FileExt,
        path::Path,
    };

    use crate::types::{Result, SombraError};

    use super::StdFileIo;

    /// Opens a file in read-write mode with creation support (Unix).
    pub fn open_rw(path: impl AsRef<Path>) -> Result<StdFileIo> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(SombraError::from)?;
        Ok(StdFileIo::new(file))
    }

    /// Reads exact number of bytes at offset using Unix pread semantics.
    pub fn read_exact(file: &File, mut off: u64, mut dst: &mut [u8]) -> io::Result<()> {
        io_test_log!("[io.read_exact] start off={} len={}", off, dst.len());
        while !dst.is_empty() {
            let read = file.read_at(dst, off)?;
            if read == 0 {
                io_test_log!(
                    "[io.read_exact] zero bytes read off={} remaining={}",
                    off,
                    dst.len()
                );
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "read_at reached EOF",
                ));
            }
            let (_, tail) = dst.split_at_mut(read);
            dst = tail;
            off += read as u64;
        }
        io_test_log!("[io.read_exact] complete");
        Ok(())
    }

    /// Writes all bytes at offset using Unix pwrite semantics.
    pub fn write_all(file: &File, mut off: u64, mut src: &[u8]) -> io::Result<()> {
        let start_off = off;
        let total = src.len();
        io_test_log!("[io.write_all] start off={} len={}", off, total);
        while !src.is_empty() {
            io_test_log!("[io.write_all] chunk off={} remaining={}", off, src.len());
            let written = file.write_at(src, off)?;
            if written == 0 {
                io_test_log!(
                    "[io.write_all] zero bytes written off={} remaining={}",
                    off,
                    src.len()
                );
                return Err(io::Error::new(
                    ErrorKind::WriteZero,
                    "write_at wrote zero bytes",
                ));
            }
            src = &src[written..];
            off += written as u64;
        }
        io_test_log!(
            "[io.write_all] complete start_off={} bytes={}",
            start_off,
            total
        );
        Ok(())
    }
}

#[cfg(windows)]
/// Windows-specific file I/O operations using Windows APIs.
pub mod stdio_win {
    use std::{
        fs::{File, OpenOptions},
        io::{self, ErrorKind},
        os::windows::fs::FileExt,
        path::Path,
    };

    use crate::types::{Result, SombraError};

    use super::StdFileIo;

    /// Opens a file in read-write mode with creation support (Windows).
    pub fn open_rw(path: impl AsRef<Path>) -> Result<StdFileIo> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)
            .map_err(SombraError::from)?;
        Ok(StdFileIo::new(file))
    }

    /// Reads exact number of bytes at offset using Windows seek_read semantics.
    pub fn read_exact(file: &File, mut off: u64, mut dst: &mut [u8]) -> io::Result<()> {
        while !dst.is_empty() {
            let read = file.seek_read(dst, off)?;
            if read == 0 {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "seek_read reached EOF",
                ));
            }
            let (_, tail) = dst.split_at_mut(read);
            dst = tail;
            off += read as u64;
        }
        Ok(())
    }

    /// Writes all bytes at offset using Windows seek_write semantics.
    pub fn write_all(file: &File, mut off: u64, mut src: &[u8]) -> io::Result<()> {
        while !src.is_empty() {
            let written = file.seek_write(src, off)?;
            if written == 0 {
                return Err(io::Error::new(
                    ErrorKind::WriteZero,
                    "seek_write wrote zero bytes",
                ));
            }
            src = &src[written..];
            off += written as u64;
        }
        Ok(())
    }
}

/// Standard file I/O implementation using `Arc<File>`.
#[derive(Clone)]
pub struct StdFileIo {
    inner: Arc<File>,
}

impl StdFileIo {
    /// Creates a new StdFileIo from an existing File handle.
    pub fn new(file: File) -> Self {
        Self {
            inner: Arc::new(file),
        }
    }

    /// Opens or creates a file for read-write access.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        #[cfg(unix)]
        {
            return stdio_unix::open_rw(path);
        }
        #[cfg(windows)]
        {
            return stdio_win::open_rw(path);
        }
        #[allow(unreachable_code)]
        Err(SombraError::Invalid(
            "StdFileIo unsupported on this platform",
        ))
    }

    fn file(&self) -> &File {
        &self.inner
    }

    #[cfg(unix)]
    fn read_exact(&self, off: u64, dst: &mut [u8]) -> io::Result<()> {
        stdio_unix::read_exact(self.file(), off, dst)
    }

    #[cfg(windows)]
    fn read_exact(&self, off: u64, dst: &mut [u8]) -> io::Result<()> {
        stdio_win::read_exact(self.file(), off, dst)
    }

    #[cfg(unix)]
    fn write_all(&self, off: u64, src: &[u8]) -> io::Result<()> {
        stdio_unix::write_all(self.file(), off, src)
    }

    #[cfg(windows)]
    fn write_all(&self, off: u64, src: &[u8]) -> io::Result<()> {
        stdio_win::write_all(self.file(), off, src)
    }

    #[cfg(not(any(unix, windows)))]
    fn read_exact(&self, _off: u64, _dst: &mut [u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "StdFileIo unsupported on this platform",
        ))
    }

    #[cfg(not(any(unix, windows)))]
    fn write_all(&self, _off: u64, _src: &[u8]) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "StdFileIo unsupported on this platform",
        ))
    }
}

impl FileIo for StdFileIo {
    fn read_at(&self, off: u64, dst: &mut [u8]) -> Result<()> {
        self.read_exact(off, dst).map_err(SombraError::from)
    }

    fn write_at(&self, off: u64, src: &[u8]) -> Result<()> {
        self.write_all(off, src).map_err(SombraError::from)
    }

    fn sync_all(&self) -> Result<()> {
        io_test_log!("[io.sync_all] start");
        let result = self.file().sync_all().map_err(SombraError::from);
        match &result {
            Ok(()) => {
                io_test_log!("[io.sync_all] complete");
            }
            Err(err) => {
                io_test_log!("[io.sync_all] error: {}", err);
            }
        }
        result
    }

    fn len(&self) -> Result<u64> {
        Ok(self.file().metadata().map_err(SombraError::from)?.len())
    }

    fn truncate(&self, len: u64) -> Result<()> {
        self.file().set_len(len).map_err(SombraError::from)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::OpenOptions;
    use std::io::ErrorKind;
    use tempfile::tempdir;

    #[test]
    fn write_read_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("io.bin");
        let io = StdFileIo::open(&path).unwrap();

        let payload = b"hello mundo";
        io.write_at(0, payload).unwrap();
        io.sync_all().unwrap();

        let mut buf = vec![0u8; payload.len()];
        io.read_at(0, &mut buf).unwrap();
        assert_eq!(&buf, payload);
        assert!(io.len().unwrap() >= payload.len() as u64);
    }

    #[test]
    fn read_past_eof_returns_error() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("io.bin");
        let io = StdFileIo::open(&path).unwrap();
        let mut buf = [0u8; 8];
        let err = io.read_at(0, &mut buf).unwrap_err();
        match err {
            SombraError::Io(inner) => assert_eq!(inner.kind(), ErrorKind::UnexpectedEof),
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn reopen_and_read_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("io_roundtrip.bin");
        {
            let io = StdFileIo::open(&path).unwrap();
            let buf = vec![42u8; 8192];
            io.write_at(0, &buf).unwrap();
            io.sync_all().unwrap();
        }
        let reopen = StdFileIo::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)
                .unwrap(),
        );
        let mut buf = vec![0u8; 8192];
        reopen.read_at(0, &mut buf).unwrap();
        assert!(buf.iter().all(|&b| b == 42));
    }
}
