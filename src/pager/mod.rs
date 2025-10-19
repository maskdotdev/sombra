use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::num::NonZeroUsize;

use lru::LruCache;
use memmap2::MmapMut;
use crate::error::{GraphError, Result};

mod wal;

use wal::Wal;

pub const DEFAULT_PAGE_SIZE: usize = 8192;
pub const DEFAULT_CACHE_SIZE: usize = 1024;

pub type PageId = u32;

#[derive(Debug)]
pub struct Page {
    pub id: PageId,
    pub data: Vec<u8>,
    pub dirty: bool,
}

impl Page {
    pub fn new(id: PageId, page_size: usize) -> Self {
        Self {
            id,
            data: vec![0; page_size],
            dirty: false,
        }
    }
}

pub struct Pager {
    file: File,
    page_size: usize,
    cache: LruCache<PageId, Page>,
    file_len: u64,
    wal: Wal,
    mmap: Option<MmapMut>,
    use_mmap: bool,
}

impl Pager {
    pub fn open(path: &Path) -> Result<Self> {
        Self::open_with_config(path, true)
    }

    pub fn open_with_config(path: &Path, wal_sync_enabled: bool) -> Result<Self> {
        Self::open_with_full_config(path, wal_sync_enabled, true, DEFAULT_CACHE_SIZE)
    }

    pub fn open_with_full_config(path: &Path, wal_sync_enabled: bool, use_mmap: bool, cache_size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(path)?;

        let file_len = file.metadata()?.len();

        let mmap = if use_mmap && file_len > 0 {
            unsafe { MmapMut::map_mut(&file).ok() }
        } else {
            None
        };

        let cache_size_nonzero = NonZeroUsize::new(cache_size).unwrap_or(NonZeroUsize::new(DEFAULT_CACHE_SIZE).unwrap());
        let mut pager = Self {
            file,
            page_size: DEFAULT_PAGE_SIZE,
            cache: LruCache::new(cache_size_nonzero),
            file_len,
            wal: Wal::open_with_config(path, DEFAULT_PAGE_SIZE, wal_sync_enabled)?,
            mmap,
            use_mmap,
        };
        
        pager.recover_wal()?;
        
        if pager.page_count() > 0 {
            pager.fetch_page(0)?;
        }

        Ok(pager)
    }

    pub fn page_size(&self) -> usize {
        self.page_size
    }

    pub fn page_count(&self) -> usize {
        if self.file_len == 0 {
            0
        } else {
            ((self.file_len - 1) as usize / self.page_size) + 1
        }
    }

    pub fn fetch_page(&mut self, page_id: PageId) -> Result<&mut Page> {
        if !self.cache.contains(&page_id) {
            let mut page = Page::new(page_id, self.page_size);
            self.read_page_from_disk(&mut page)?;
            if let Some((evicted_id, evicted_page)) = self.cache.push(page_id, page) {
                if evicted_page.dirty {
                    self.write_page_to_disk(evicted_id, &evicted_page.data)?;
                    self.invalidate_mmap();
                }
            }
        }
        Ok(self.cache.get_mut(&page_id).expect("page must exist"))
    }

    fn invalidate_mmap(&mut self) {
        if self.mmap.is_some() {
            self.mmap = None;
        }
    }

    fn ensure_mmap(&mut self) -> Result<()> {
        if self.use_mmap && self.mmap.is_none() && self.file_len > 0 {
            self.file.sync_data()?;
            self.mmap = unsafe { MmapMut::map_mut(&self.file).ok() };
        }
        Ok(())
    }

    pub fn allocate_page(&mut self) -> Result<PageId> {
        if self.file_len % self.page_size as u64 != 0 {
            return Err(GraphError::Corruption(
                "underlying file length is not page aligned".into(),
            ));
        }

        let next_page_id = (self.file_len / self.page_size as u64) as PageId;
        let mut page = Page::new(next_page_id, self.page_size);
        page.dirty = true;
        if let Some((evicted_id, evicted_page)) = self.cache.push(next_page_id, page) {
            if evicted_page.dirty {
                self.write_page_to_disk(evicted_id, &evicted_page.data)?;
                self.invalidate_mmap();
            }
        }
        self.file_len = (u64::from(next_page_id) + 1) * self.page_size as u64;
        self.invalidate_mmap();
        Ok(next_page_id)
    }

    pub fn with_page<F, T>(&mut self, page_id: PageId, mut f: F) -> Result<T>
    where
        F: FnMut(&[u8]) -> Result<T>,
    {
        let page = self.fetch_page(page_id)?;
        f(&page.data)
    }

    pub fn with_page_mut<F, T>(&mut self, page_id: PageId, mut f: F) -> Result<T>
    where
        F: FnMut(&mut [u8]) -> Result<T>,
    {
        let page = self.fetch_page(page_id)?;
        let result = f(&mut page.data)?;
        page.dirty = true;
        Ok(result)
    }

    pub fn flush(&mut self) -> Result<()> {
        let dirty_pages: Vec<PageId> = self
            .cache
            .iter()
            .filter_map(|(&id, page)| if page.dirty { Some(id) } else { None })
            .collect();

        if dirty_pages.is_empty() {
            return Ok(());
        }

        self.flush_pages_internal(dirty_pages, 0)?;
        self.invalidate_mmap();
        Ok(())
    }

    pub fn flush_pages(&mut self, page_ids: &[PageId], tx_id: u64) -> Result<()> {
        if page_ids.is_empty() {
            return Ok(());
        }
        self.flush_pages_internal(page_ids.to_vec(), tx_id)
    }

    pub fn append_to_wal(&mut self, page_id: PageId, tx_id: u64, page_bytes: &[u8]) -> Result<()> {
        self.wal.append_page_frame(page_id, tx_id, page_bytes)
    }

    pub fn append_page_to_wal(&mut self, page_id: PageId, tx_id: u64) -> Result<()> {
        if let Some(page) = self.cache.get(&page_id) {
            self.wal.append_page_frame(page_id, tx_id, &page.data)?;
        } else {
            let mut page = Page::new(page_id, self.page_size);
            self.read_page_from_disk(&mut page)?;
            self.wal.append_page_frame(page_id, tx_id, &page.data)?;
        }
        Ok(())
    }

    pub fn append_commit_to_wal(&mut self, tx_id: u64) -> Result<()> {
        self.wal.append_commit_frame(tx_id)
    }

    pub fn sync_wal(&mut self) -> Result<()> {
        self.wal.sync()
    }

    pub fn checkpoint(&mut self) -> Result<()> {
        // Collect all the page updates first
        let mut page_updates = Vec::new();
        let frames_applied = self.wal.replay(|page_id, data| {
            page_updates.push((page_id, data.to_vec()));
            Ok(())
        })?;

        // Apply the updates to disk
        for (page_id, data) in page_updates {
            self.write_page_to_disk(page_id, &data)?;
            // Clear dirty flag for this page if it's in cache
            if let Some(page) = self.cache.get_mut(&page_id) {
                page.dirty = false;
            }
        }

        if frames_applied > 0 {
            self.file.sync_data()?;
            self.wal.reset()?;
        }

        Ok(())
    }

    fn flush_pages_internal(&mut self, mut pages: Vec<PageId>, tx_id: u64) -> Result<()> {
        pages.sort_unstable();
        pages.dedup();

        let mut frames = Vec::with_capacity(pages.len());
        for &page_id in &pages {
            let page = self
                .cache
                .get(&page_id)
                .ok_or_else(|| GraphError::Corruption("dirty page missing from cache".into()))?;
            frames.push((page_id, page.data.clone()));
        }

        for (page_id, data) in &frames {
            self.wal.append_page_frame(*page_id, tx_id, data)?;
        }
        self.wal.append_commit_frame(tx_id)?;
        self.wal.sync()?;

        for (page_id, data) in &frames {
            self.write_page_to_disk(*page_id, data)?;
        }
        self.file.sync_data()?;

        for &page_id in &pages {
            if let Some(page) = self.cache.get_mut(&page_id) {
                page.dirty = false;
            }
        }

        self.wal.reset()?;
        Ok(())
    }

    pub fn restore_pages(&mut self, page_ids: &[PageId]) -> Result<()> {
        if page_ids.is_empty() {
            return Ok(());
        }

        let mut pages = page_ids.to_vec();
        pages.sort_unstable();
        pages.dedup();

        for &page_id in &pages {
            let data = self.load_page_bytes(page_id)?;
            if let Some(page) = self.cache.get_mut(&page_id) {
                page.data = data;
                page.dirty = false;
            } else {
                let mut page = Page::new(page_id, self.page_size);
                page.data = data;
                page.dirty = false;
                if let Some((evicted_id, evicted_page)) = self.cache.push(page_id, page) {
                    if evicted_page.dirty {
                        self.write_page_to_disk(evicted_id, &evicted_page.data)?;
                    }
                }
            }
        }

        let file_len = self.file.metadata()?.len();
        let max_pages = if file_len == 0 {
            1
        } else {
            ((file_len - 1) / self.page_size as u64) + 1
        };
        self.file_len = max_pages * self.page_size as u64;
        
        let to_remove: Vec<PageId> = self.cache
            .iter()
            .filter_map(|(&id, _)| {
                if id != 0 && u64::from(id) >= max_pages {
                    Some(id)
                } else {
                    None
                }
            })
            .collect();
        
        for id in to_remove {
            self.cache.pop(&id);
        }

        Ok(())
    }

    fn recover_wal(&mut self) -> Result<()> {
        let page_size = self.page_size;
        let file = &mut self.file;
        let file_len = &mut self.file_len;
        let frames = self
            .wal
            .replay(|page_id, data| write_page_image(file, file_len, page_size, page_id, data))?;
        if frames > 0 {
            self.file.sync_data()?;
            self.wal.reset()?;
        }
        Ok(())
    }

    fn read_page_from_disk(&mut self, page: &mut Page) -> Result<()> {
        let data = self.load_page_bytes(page.id)?;
        page.data = data;
        Ok(())
    }

    fn write_page_to_disk(&mut self, page_id: PageId, data: &[u8]) -> Result<()> {
        write_page_image(
            &mut self.file,
            &mut self.file_len,
            self.page_size,
            page_id,
            data,
        )
    }

    fn load_page_bytes(&mut self, page_id: PageId) -> Result<Vec<u8>> {
        let offset = page_offset(page_id, self.page_size)? as usize;
        let mut buf = vec![0u8; self.page_size];
        
        if offset < self.file_len as usize {
            self.ensure_mmap()?;
            
            if let Some(ref mmap) = self.mmap {
                let end = (offset + self.page_size).min(mmap.len());
                if offset < mmap.len() {
                    let copy_len = end - offset;
                    buf[..copy_len].copy_from_slice(&mmap[offset..end]);
                }
            } else {
                self.file.seek(SeekFrom::Start(offset as u64))?;
                let bytes_read = self.file.read(&mut buf)?;
                if bytes_read < buf.len() {
                    buf[bytes_read..].fill(0);
                }
            }
        }
        Ok(buf)
    }
}

fn page_offset(page_id: PageId, page_size: usize) -> Result<u64> {
    let page_size_u64 = page_size as u64;
    let id_u64 = u64::from(page_id);
    id_u64
        .checked_mul(page_size_u64)
        .ok_or_else(|| GraphError::InvalidArgument("page offset overflow".into()))
}

fn write_page_image(
    file: &mut File,
    file_len: &mut u64,
    page_size: usize,
    page_id: PageId,
    data: &[u8],
) -> Result<()> {
    if data.len() != page_size {
        return Err(GraphError::InvalidArgument(
            "page size mismatch during flush".into(),
        ));
    }
    let offset = page_offset(page_id, page_size)?;
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(data)?;
    let end = offset + data.len() as u64;
    if end > *file_len {
        *file_len = end;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::wal::{WAL_HEADER_SIZE, Wal};
    use super::*;
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::NamedTempFile;

    #[test]
    fn allocate_and_reopen_page() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        {
            let mut pager = Pager::open(&path).expect("open pager");
            let page_id = pager.allocate_page().expect("allocate page");
            assert_eq!(page_id, 0);

            pager
                .with_page_mut(page_id, |data| {
                    data[0..4].copy_from_slice(&[1, 2, 3, 4]);
                    Ok(())
                })
                .expect("write page");

            pager.flush().expect("flush");
        }

        {
            let mut pager = Pager::open(&path).expect("reopen pager");
            pager
                .with_page(0, |data| {
                    assert_eq!(&data[0..4], &[1, 2, 3, 4]);
                    Ok(())
                })
                .expect("read page");
        }
    }

    #[test]
    fn flush_truncates_wal_after_checkpoint() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        {
            let mut pager = Pager::open(&path).expect("open pager");
            let page_id = pager.allocate_page().expect("allocate page");
            pager
                .with_page_mut(page_id, |data| {
                    data[0] = 42;
                    Ok(())
                })
                .expect("write page");
            pager.flush().expect("flush");
        }

        let wal_path = wal_path_for(&path);
        let metadata = fs::metadata(&wal_path).expect("wal metadata");
        assert_eq!(
            metadata.len(),
            WAL_HEADER_SIZE as u64,
            "checkpoint should truncate WAL to header"
        );
    }

    #[test]
    fn wal_recovery_applies_pending_frames() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        {
            let mut pager = Pager::open(&path).expect("open pager");
            let page_id = pager.allocate_page().expect("allocate page");
            pager
                .with_page_mut(page_id, |data| {
                    data.fill(0);
                    Ok(())
                })
                .expect("zero page");
            pager.flush().expect("flush base state");
        }

        {
            let mut wal = Wal::open(&path, DEFAULT_PAGE_SIZE).expect("open wal");
            let mut frame = vec![0u8; DEFAULT_PAGE_SIZE];
            frame[0..4].copy_from_slice(&[9, 8, 7, 6]);
            wal.append_page_frame(0, 1, &frame).expect("append frame");
            wal.append_commit_frame(1).expect("append commit");
            wal.sync().expect("sync wal");
        }

        {
            let mut pager = Pager::open(&path).expect("reopen pager with wal recovery");
            pager
                .with_page(0, |data| {
                    assert_eq!(&data[0..4], &[9, 8, 7, 6]);
                    Ok(())
                })
                .expect("verify recovered page");

            let wal_path = wal_path_for(&path);
            let metadata = fs::metadata(&wal_path).expect("wal metadata after recovery");
            assert_eq!(
                metadata.len(),
                WAL_HEADER_SIZE as u64,
                "recovery should checkpoint and truncate WAL"
            );
        }
    }

    #[test]
    fn wal_recovery_skips_uncommitted_frames() {
        let tmp = NamedTempFile::new().expect("temp file");
        let path = tmp.path().to_path_buf();

        let page_id = {
            let mut pager = Pager::open(&path).expect("open pager");
            let page_id = pager.allocate_page().expect("allocate page");
            pager
                .with_page_mut(page_id, |data| {
                    data.fill(0);
                    data[0..4].copy_from_slice(&[1, 2, 3, 4]);
                    Ok(())
                })
                .expect("write base page");
            pager.flush().expect("flush base state");
            page_id
        };

        {
            let mut wal = Wal::open(&path, DEFAULT_PAGE_SIZE).expect("open wal");
            let mut frame = vec![0u8; DEFAULT_PAGE_SIZE];
            frame[0..4].copy_from_slice(&[9, 9, 9, 9]);
            wal.append_page_frame(page_id, 42, &frame)
                .expect("append uncommitted frame");
            wal.sync().expect("sync wal without commit");
        }

        {
            let mut pager = Pager::open(&path).expect("reopen pager with pending wal");
            pager
                .with_page(page_id, |data| {
                    assert_eq!(&data[0..4], &[1, 2, 3, 4]);
                    Ok(())
                })
                .expect("page should retain original data");
        }
    }

    fn wal_path_for(path: &Path) -> PathBuf {
        let mut os_str = path.as_os_str().to_owned();
        os_str.push(".wal");
        PathBuf::from(os_str)
    }
}
