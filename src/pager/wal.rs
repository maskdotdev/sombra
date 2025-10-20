use std::collections::HashMap;
use std::convert::TryInto;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crc32fast::Hasher;

use crate::error::{GraphError, Result};

use super::PageId;

const WAL_MAGIC: &[u8; 8] = b"GRPHWAL\0";
const WAL_VERSION_MAJOR: u16 = 1;
const WAL_VERSION_MINOR: u16 = 0;
pub(crate) const WAL_HEADER_SIZE: usize = 32;
const WAL_FRAME_HEADER_SIZE: usize = 24;

const FRAME_FLAG_COMMIT: u32 = 0x1;

#[derive(Debug)]
pub(crate) struct Wal {
    file: File,
    page_size: usize,
    next_frame_number: u32,
    sync_enabled: bool,
}

impl Wal {
    #[allow(dead_code)]
    pub(crate) fn open(db_path: &Path, page_size: usize) -> Result<Self> {
        Self::open_with_config(db_path, page_size, true)
    }

    pub(crate) fn open_with_config(
        db_path: &Path,
        page_size: usize,
        sync_enabled: bool,
    ) -> Result<Self> {
        let path = wal_path(db_path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut wal = Self {
            file,
            page_size,
            next_frame_number: 1,
            sync_enabled,
        };

        let len = wal.file.metadata()?.len();
        if len == 0 {
            wal.write_header()?;
        } else {
            wal.validate_header()?;
            wal.next_frame_number = wal.scan_frame_count()? + 1;
        }

        Ok(wal)
    }

    pub(crate) fn append_page_frame(
        &mut self,
        page_id: PageId,
        tx_id: u64,
        page_bytes: &[u8],
    ) -> Result<()> {
        self.append_frame_inner(page_id, tx_id, 0, page_bytes)
    }

    pub(crate) fn append_commit_frame(&mut self, tx_id: u64) -> Result<()> {
        let zeros = vec![0u8; self.page_size];
        self.append_frame_inner(0, tx_id, FRAME_FLAG_COMMIT, &zeros)
    }

    fn append_frame_inner(
        &mut self,
        page_id: PageId,
        tx_id: u64,
        flags: u32,
        page_bytes: &[u8],
    ) -> Result<()> {
        if page_bytes.len() != self.page_size {
            return Err(GraphError::InvalidArgument(
                "WAL frame size does not match pager page size".into(),
            ));
        }

        let mut header = [0u8; WAL_FRAME_HEADER_SIZE];
        let checksum = checksum_for(page_bytes);
        Self::encode_frame_header(
            &mut header,
            page_id,
            self.next_frame_number,
            checksum,
            tx_id,
            flags,
        );

        self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&header)?;
        self.file.write_all(page_bytes)?;
        self.next_frame_number = self
            .next_frame_number
            .checked_add(1)
            .ok_or_else(|| GraphError::Corruption("WAL frame number overflow".into()))?;
        Ok(())
    }

    pub(crate) fn sync(&mut self) -> Result<()> {
        if self.sync_enabled {
            self.file.sync_data()?;
        }
        Ok(())
    }

    pub(crate) fn reset(&mut self) -> Result<()> {
        self.file.set_len(WAL_HEADER_SIZE as u64)?;
        self.file.seek(SeekFrom::End(0))?;
        self.next_frame_number = 1;
        self.file.sync_data()?;
        Ok(())
    }

    pub(crate) fn size(&self) -> Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Replays frames into the provided closure. Returns the number of frames applied.
    pub(crate) fn replay<F>(&mut self, mut apply: F) -> Result<u32>
    where
        F: FnMut(PageId, &[u8]) -> Result<()>,
    {
        let mut frames_applied = 0u32;
        let mut expected_frame = 1u32;

        self.file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;
        let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
        let mut page_buf = vec![0u8; self.page_size];

        let mut pending: HashMap<u64, Vec<(PageId, Vec<u8>)>> = HashMap::new();

        loop {
            if !self.read_exact_or_eof(&mut header_buf)? {
                break;
            }

            let (page_id, frame_number, checksum, tx_id, flags) =
                Self::decode_frame_header(&header_buf)?;
            if frame_number != expected_frame {
                return Err(GraphError::Corruption(
                    "WAL frame numbers out of sequence during recovery".into(),
                ));
            }

            expected_frame = expected_frame
                .checked_add(1)
                .ok_or_else(|| GraphError::Corruption("WAL frame number overflow".into()))?;

            if !self.read_exact_or_eof(&mut page_buf)? {
                return Err(GraphError::Corruption(
                    "WAL contains partial frame payload".into(),
                ));
            }
            let computed = checksum_for(&page_buf);
            if computed != checksum {
                return Err(GraphError::Corruption("WAL frame checksum mismatch".into()));
            }

            if (flags & FRAME_FLAG_COMMIT) != 0 {
                if let Some(frames) = pending.remove(&tx_id) {
                    for (page_id, data) in frames {
                        apply(page_id, &data)?;
                        frames_applied = frames_applied.checked_add(1).ok_or_else(|| {
                            GraphError::Corruption("WAL frame count overflow".into())
                        })?;
                    }
                }
                continue;
            }

            let mut data = vec![0u8; self.page_size];
            data.copy_from_slice(&page_buf);
            pending.entry(tx_id).or_default().push((page_id, data));
        }

        self.next_frame_number = expected_frame;
        Ok(frames_applied)
    }

    fn write_header(&mut self) -> Result<()> {
        let mut header = [0u8; WAL_HEADER_SIZE];
        header[..WAL_MAGIC.len()].copy_from_slice(WAL_MAGIC);
        header[8..10].copy_from_slice(&WAL_VERSION_MAJOR.to_le_bytes());
        header[10..12].copy_from_slice(&WAL_VERSION_MINOR.to_le_bytes());
        header[12..16].copy_from_slice(&(self.page_size as u32).to_le_bytes());
        self.file.seek(SeekFrom::Start(0))?;
        self.file.write_all(&header)?;
        self.file.sync_data()?;
        Ok(())
    }

    fn validate_header(&mut self) -> Result<()> {
        let mut header = [0u8; WAL_HEADER_SIZE];
        self.file.seek(SeekFrom::Start(0))?;
        self.file.read_exact(&mut header)?;

        if &header[..WAL_MAGIC.len()] != WAL_MAGIC {
            return Err(GraphError::Corruption("invalid WAL magic".into()));
        }

        let major = u16::from_le_bytes([header[8], header[9]]);
        let minor = u16::from_le_bytes([header[10], header[11]]);
        if major != WAL_VERSION_MAJOR || minor != WAL_VERSION_MINOR {
            return Err(GraphError::Corruption(
                "unsupported WAL version detected".into(),
            ));
        }

        let stored_page_size = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        if stored_page_size as usize != self.page_size {
            return Err(GraphError::Corruption(
                "WAL page size differs from pager configuration".into(),
            ));
        }
        Ok(())
    }

    fn scan_frame_count(&mut self) -> Result<u32> {
        self.file.seek(SeekFrom::Start(WAL_HEADER_SIZE as u64))?;
        let mut header_buf = [0u8; WAL_FRAME_HEADER_SIZE];
        let mut page_buf = vec![0u8; self.page_size];

        let mut expected_frame = 1u32;
        let mut count = 0u32;

        loop {
            if !self.read_exact_or_eof(&mut header_buf)? {
                break;
            }

            let (_, frame_number, checksum, _, _) = Self::decode_frame_header(&header_buf)?;
            if frame_number != expected_frame {
                return Err(GraphError::Corruption(
                    "WAL frame numbers out of sequence during scan".into(),
                ));
            }
            expected_frame = expected_frame
                .checked_add(1)
                .ok_or_else(|| GraphError::Corruption("WAL frame number overflow".into()))?;

            if !self.read_exact_or_eof(&mut page_buf)? {
                return Err(GraphError::Corruption(
                    "WAL contains partial frame payload".into(),
                ));
            }
            let computed = checksum_for(&page_buf);
            if computed != checksum {
                return Err(GraphError::Corruption(
                    "WAL frame checksum mismatch during scan".into(),
                ));
            }

            count = count
                .checked_add(1)
                .ok_or_else(|| GraphError::Corruption("WAL frame count overflow".into()))?;
        }

        Ok(count)
    }

    fn encode_frame_header(
        buf: &mut [u8; WAL_FRAME_HEADER_SIZE],
        page_id: PageId,
        frame_number: u32,
        checksum: u32,
        tx_id: u64,
        flags: u32,
    ) {
        buf[0..4].copy_from_slice(&page_id.to_le_bytes());
        buf[4..8].copy_from_slice(&frame_number.to_le_bytes());
        buf[8..12].copy_from_slice(&checksum.to_le_bytes());
        buf[12..20].copy_from_slice(&tx_id.to_le_bytes());
        buf[20..24].copy_from_slice(&flags.to_le_bytes());
    }

    fn decode_frame_header(
        buf: &[u8; WAL_FRAME_HEADER_SIZE],
    ) -> Result<(PageId, u32, u32, u64, u32)> {
        let page_id = Self::read_u32_le(buf, 0)?;
        let frame_number = Self::read_u32_le(buf, 4)?;
        let checksum = Self::read_u32_le(buf, 8)?;
        let tx_id = Self::read_u64_le(buf, 12)?;
        let flags = Self::read_u32_le(buf, 20)?;
        Ok((page_id, frame_number, checksum, tx_id, flags))
    }

    fn read_exact_or_eof(&mut self, buf: &mut [u8]) -> Result<bool> {
        let mut read = 0usize;
        while read < buf.len() {
            let bytes = self.file.read(&mut buf[read..])?;
            if bytes == 0 {
                if read == 0 {
                    return Ok(false);
                }
                return Err(GraphError::Corruption(
                    "WAL contains partial frame data".into(),
                ));
            }
            read += bytes;
        }
        Ok(true)
    }
}

impl Wal {
    fn read_u32_le(buf: &[u8], offset: usize) -> Result<u32> {
        let end = offset
            .checked_add(4)
            .ok_or_else(|| GraphError::Corruption("u32 read offset overflow".into()))?;
        let slice = buf.get(offset..end).ok_or_else(|| {
            GraphError::Corruption(format!("Invalid u32 at WAL header offset {offset}"))
        })?;
        let bytes: [u8; 4] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to copy u32 bytes from WAL header".into())
        })?;
        Ok(u32::from_le_bytes(bytes))
    }

    fn read_u64_le(buf: &[u8], offset: usize) -> Result<u64> {
        let end = offset
            .checked_add(8)
            .ok_or_else(|| GraphError::Corruption("u64 read offset overflow".into()))?;
        let slice = buf.get(offset..end).ok_or_else(|| {
            GraphError::Corruption(format!("Invalid u64 at WAL header offset {offset}"))
        })?;
        let bytes: [u8; 8] = slice.try_into().map_err(|_| {
            GraphError::Corruption("Failed to copy u64 bytes from WAL header".into())
        })?;
        Ok(u64::from_le_bytes(bytes))
    }
}

fn checksum_for(page_bytes: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(page_bytes);
    hasher.finalize()
}

fn wal_path(db_path: &Path) -> PathBuf {
    let mut os_string = db_path.as_os_str().to_owned();
    os_string.push(".wal");
    PathBuf::from(os_string)
}
