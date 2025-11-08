#![forbid(unsafe_code)]

use std::{fs::File, io, path::Path, sync::Arc};

use crate::types::{Result, SombraError};

pub trait FileIo: Send + Sync + 'static {
    fn read_at(&self, off: u64, dst: &mut [u8]) -> Result<()>;
    fn write_at(&self, off: u64, src: &[u8]) -> Result<()>;
    fn sync_all(&self) -> Result<()>;
    fn len(&self) -> Result<u64>;
    fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }
    fn truncate(&self, len: u64) -> Result<()>;
}

#[cfg(unix)]
pub mod stdio_unix {
    use std::{
        fs::{File, OpenOptions},
        io::{self, ErrorKind},
        os::unix::fs::FileExt,
        path::Path,
    };

    use crate::types::{Result, SombraError};

    use super::StdFileIo;

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

    pub fn read_exact(file: &File, mut off: u64, mut dst: &mut [u8]) -> io::Result<()> {
        while !dst.is_empty() {
            let read = file.read_at(dst, off)?;
            if read == 0 {
                return Err(io::Error::new(
                    ErrorKind::UnexpectedEof,
                    "read_at reached EOF",
                ));
            }
            let (_, tail) = dst.split_at_mut(read);
            dst = tail;
            off += read as u64;
        }
        Ok(())
    }

    pub fn write_all(file: &File, mut off: u64, mut src: &[u8]) -> io::Result<()> {
        while !src.is_empty() {
            let written = file.write_at(src, off)?;
            if written == 0 {
                return Err(io::Error::new(
                    ErrorKind::WriteZero,
                    "write_at wrote zero bytes",
                ));
            }
            src = &src[written..];
            off += written as u64;
        }
        Ok(())
    }
}

#[cfg(windows)]
pub mod stdio_win {
    use std::{
        fs::{File, OpenOptions},
        io::{self, ErrorKind},
        os::windows::fs::FileExt,
        path::Path,
    };

    use crate::types::{Result, SombraError};

    use super::StdFileIo;

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

#[derive(Clone)]
pub struct StdFileIo {
    inner: Arc<File>,
}

impl StdFileIo {
    pub fn new(file: File) -> Self {
        Self {
            inner: Arc::new(file),
        }
    }

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
        self.file().sync_all().map_err(SombraError::from)
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
