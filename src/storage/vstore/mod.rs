#![forbid(unsafe_code)]

use std::cmp::min;
use std::convert::TryInto;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::primitives::pager::{PageMut, PageStore, ReadGuard, WriteGuard};
use crate::types::page::{PageHeader, PageKind, PAGE_HDR_LEN};
use crate::types::{Checksum, Crc32Fast, PageId, Result, SombraError, VRef};
#[cfg(debug_assertions)]
use tracing::debug;
use tracing::trace;

const OVERFLOW_HEADER_LEN: usize = 16;

/// Metrics tracking for variable-length value storage operations.
#[derive(Default)]
pub struct VStoreMetrics {
    pages_allocated: AtomicU64,
    pages_freed: AtomicU64,
    bytes_written: AtomicU64,
    bytes_read: AtomicU64,
    extent_writes: AtomicU64,
    extent_segments: AtomicU64,
    extent_pages: AtomicU64,
}

/// Snapshot of variable-length value storage metrics at a point in time.
#[derive(Clone, Copy, Debug, Default)]
pub struct VStoreMetricsSnapshot {
    /// Total number of overflow pages allocated
    pub pages_allocated: u64,
    /// Total number of overflow pages freed
    pub pages_freed: u64,
    /// Total bytes written to overflow pages
    pub bytes_written: u64,
    /// Total bytes read from overflow pages
    pub bytes_read: u64,
    /// Number of writes that used at least one extent
    pub extent_writes: u64,
    /// Total number of extent segments allocated
    pub extent_segments: u64,
    /// Total number of pages covered by extent allocations
    pub extent_pages: u64,
}

impl VStoreMetricsSnapshot {
    /// Returns the current number of live overflow pages.
    pub fn live_pages(&self) -> i64 {
        self.pages_allocated as i64 - self.pages_freed as i64
    }
}

#[cfg(debug_assertions)]
impl VStore {
    /// Dumps detailed information about a VRef chain for debugging purposes.
    pub fn dump_vref(&self, tx: &ReadGuard, vref: VRef) -> Result<()> {
        debug!(
            start_page = vref.start_page.0,
            pages = vref.n_pages,
            len = vref.len,
            checksum = vref.checksum,
            "vstore.dump_vref.start"
        );
        let mut current = vref.start_page;
        let mut remaining = vref.n_pages;
        let mut index = 0u32;
        while remaining > 0 {
            if current.0 == 0 {
                return Err(SombraError::Corruption("dump_vref truncated chain"));
            }
            let page = self.store.get_page(tx, current)?;
            let (next, used, _) = self.decode_page(page.data())?;
            debug!(
                page_index = index,
                page_id = current.0,
                used_bytes = used,
                next_page = next.0,
                "vstore.dump_vref.page"
            );
            current = next;
            remaining -= 1;
            index += 1;
        }
        if current.0 != 0 {
            return Err(SombraError::Corruption(
                "dump_vref encountered extra pages beyond n_pages",
            ));
        }
        debug!("vstore.dump_vref.end");
        Ok(())
    }
}

impl VStoreMetrics {
    /// Returns the total number of pages allocated.
    pub fn pages_allocated(&self) -> u64 {
        self.pages_allocated.load(Ordering::Relaxed)
    }

    /// Returns the total number of pages freed.
    pub fn pages_freed(&self) -> u64 {
        self.pages_freed.load(Ordering::Relaxed)
    }

    /// Returns the total number of bytes written.
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written.load(Ordering::Relaxed)
    }

    /// Returns the total number of bytes read.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    /// Returns the number of writes that used at least one extent segment.
    pub fn extent_writes(&self) -> u64 {
        self.extent_writes.load(Ordering::Relaxed)
    }

    /// Returns the total extent segment count.
    pub fn extent_segments(&self) -> u64 {
        self.extent_segments.load(Ordering::Relaxed)
    }

    /// Returns the total number of pages covered by extent allocations.
    pub fn extent_pages(&self) -> u64 {
        self.extent_pages.load(Ordering::Relaxed)
    }

    /// Creates a snapshot of the current metrics.
    pub fn snapshot(&self) -> VStoreMetricsSnapshot {
        VStoreMetricsSnapshot {
            pages_allocated: self.pages_allocated(),
            pages_freed: self.pages_freed(),
            bytes_written: self.bytes_written(),
            bytes_read: self.bytes_read(),
            extent_writes: self.extent_writes(),
            extent_segments: self.extent_segments(),
            extent_pages: self.extent_pages(),
        }
    }

    fn add_pages_allocated(&self, delta: u64) {
        if delta != 0 {
            self.pages_allocated.fetch_add(delta, Ordering::Relaxed);
        }
    }

    fn add_pages_freed(&self, delta: u64) {
        if delta != 0 {
            self.pages_freed.fetch_add(delta, Ordering::Relaxed);
        }
    }

    fn add_bytes_written(&self, delta: u64) {
        if delta != 0 {
            self.bytes_written.fetch_add(delta, Ordering::Relaxed);
        }
    }

    fn add_bytes_read(&self, delta: u64) {
        if delta != 0 {
            self.bytes_read.fetch_add(delta, Ordering::Relaxed);
        }
    }

    fn add_extent_stats(&self, segments: u64, pages: u64) {
        if segments == 0 || pages == 0 {
            return;
        }
        self.extent_writes.fetch_add(1, Ordering::Relaxed);
        self.extent_segments.fetch_add(segments, Ordering::Relaxed);
        self.extent_pages.fetch_add(pages, Ordering::Relaxed);
    }
}

/// Variable-length data storage manager using overflow pages.
pub struct VStore {
    store: Arc<dyn PageStore>,
    page_size: usize,
    salt: u64,
    data_capacity: usize,
    metrics: Arc<VStoreMetrics>,
}

impl VStore {
    /// Opens a VStore instance using the provided page store.
    pub fn open(store: Arc<dyn PageStore>) -> Result<Self> {
        let meta = store.meta()?;
        let page_size = store.page_size() as usize;
        if page_size < PAGE_HDR_LEN + OVERFLOW_HEADER_LEN {
            return Err(SombraError::Invalid(
                "page size too small for overflow payload",
            ));
        }
        let data_capacity = page_size - PAGE_HDR_LEN - OVERFLOW_HEADER_LEN;
        if data_capacity == 0 {
            return Err(SombraError::Invalid("overflow data capacity is zero"));
        }
        Ok(Self {
            store,
            page_size,
            salt: meta.salt,
            data_capacity,
            metrics: Arc::new(VStoreMetrics::default()),
        })
    }

    /// Returns a reference to the VStore metrics.
    pub fn metrics(&self) -> Arc<VStoreMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Returns a snapshot of current VStore metrics.
    pub fn metrics_snapshot(&self) -> VStoreMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Writes variable-length data and returns a reference to it.
    pub fn write(&self, tx: &mut WriteGuard<'_>, bytes: &[u8]) -> Result<VRef> {
        if bytes.len() > u32::MAX as usize {
            return Err(SombraError::Invalid("value larger than 4GB not supported"));
        }
        let needed_pages = if bytes.is_empty() {
            1
        } else {
            let capacity = self.data_capacity;
            let required = (bytes.len() + capacity - 1) / capacity;
            if required > u32::MAX as usize {
                return Err(SombraError::Invalid("page count exceeds u32::MAX"));
            }
            required.max(1)
        };
        let mut remaining = u32::try_from(needed_pages)
            .map_err(|_| SombraError::Invalid("page count exceeds u32::MAX"))?;
        let mut extents = Vec::new();
        while remaining > 0 {
            let extent = tx.allocate_extent(remaining)?;
            if extent.len == 0 {
                return Err(SombraError::Invalid("allocator returned empty extent"));
            }
            remaining = remaining.saturating_sub(extent.len);
            extents.push(extent);
        }
        let mut pages = Vec::with_capacity(needed_pages);
        for extent in &extents {
            pages.extend(extent.iter_pages());
        }
        debug_assert_eq!(pages.len(), needed_pages);
        let mut checksum = Crc32Fast::default();
        let mut offset = 0usize;
        for (idx, page_id) in pages.iter().enumerate() {
            let next = if idx + 1 < pages.len() {
                pages[idx + 1]
            } else {
                PageId(0)
            };
            let remaining = bytes.len().saturating_sub(offset);
            let chunk_len = min(remaining, self.data_capacity);
            let chunk = &bytes[offset..offset + chunk_len];
            checksum.update(chunk);
            let mut page = tx.page_mut(*page_id)?;
            self.init_overflow_page(&mut page, *page_id, next, chunk)?;
            offset += chunk_len;
        }
        debug_assert_eq!(offset, bytes.len());
        self.metrics
            .add_extent_stats(extents.len() as u64, pages.len() as u64);
        self.metrics.add_pages_allocated(pages.len() as u64);
        self.metrics.add_bytes_written(bytes.len() as u64);
        trace!(pages = pages.len(), len = bytes.len(), "vstore.write");
        Ok(VRef {
            start_page: pages[0],
            n_pages: pages.len() as u32,
            len: bytes.len() as u32,
            checksum: checksum.finalize(),
        })
    }

    /// Reads variable-length data from a VRef into a new vector.
    pub fn read(&self, tx: &ReadGuard, vref: VRef) -> Result<Vec<u8>> {
        let mut dst = Vec::with_capacity(vref.len as usize);
        self.read_into(tx, vref, &mut dst)?;
        Ok(dst)
    }

    /// Reads variable-length data from a VRef into an existing vector.
    pub fn read_into(&self, tx: &ReadGuard, vref: VRef, dst: &mut Vec<u8>) -> Result<()> {
        if vref.n_pages == 0 {
            dst.clear();
            return Ok(());
        }
        let mut current = vref.start_page;
        let mut pages_left = vref.n_pages;
        let mut remaining = vref.len as usize;
        dst.clear();
        dst.reserve(remaining);
        let mut checksum = Crc32Fast::default();
        while pages_left > 0 {
            if current.0 == 0 {
                return Err(SombraError::Corruption("overflow chain terminated early"));
            }
            let page = self.store.get_page(tx, current)?;
            let (next, used, data) = self.decode_page(page.data())?;
            let used_usize = used as usize;
            if used_usize > remaining {
                return Err(SombraError::Corruption(
                    "overflow chain exceeded reported length",
                ));
            }
            dst.extend_from_slice(data);
            checksum.update(data);
            remaining -= used_usize;
            pages_left -= 1;
            current = next;
        }
        if current.0 != 0 {
            return Err(SombraError::Corruption(
                "overflow chain longer than n_pages",
            ));
        }
        if remaining != 0 {
            return Err(SombraError::Corruption(
                "overflow chain shorter than reported length",
            ));
        }
        let computed = checksum.finalize();
        if computed != vref.checksum {
            return Err(SombraError::Corruption("overflow checksum mismatch"));
        }
        self.metrics.add_bytes_read(vref.len as u64);
        trace!(pages = vref.n_pages, len = vref.len, "vstore.read");
        Ok(())
    }

    /// Reads variable-length data using a write transaction without opening a read guard.
    pub fn read_with_write(&self, tx: &mut WriteGuard<'_>, vref: VRef) -> Result<Vec<u8>> {
        let mut dst = Vec::with_capacity(vref.len as usize);
        self.read_into_with_write(tx, vref, &mut dst)?;
        Ok(dst)
    }

    /// Reads variable-length data into an existing buffer using a write transaction.
    pub fn read_into_with_write(
        &self,
        tx: &mut WriteGuard<'_>,
        vref: VRef,
        dst: &mut Vec<u8>,
    ) -> Result<()> {
        if vref.n_pages == 0 {
            dst.clear();
            return Ok(());
        }
        let mut current = vref.start_page;
        let mut pages_left = vref.n_pages;
        let mut remaining = vref.len as usize;
        dst.clear();
        dst.reserve(remaining);
        let mut checksum = Crc32Fast::default();
        while pages_left > 0 {
            if current.0 == 0 {
                return Err(SombraError::Corruption("overflow chain terminated early"));
            }
            let page = self.store.get_page_with_write(tx, current)?;
            let (next, used, data) = self.decode_page(page.data())?;
            let used_usize = used as usize;
            if used_usize > remaining {
                return Err(SombraError::Corruption(
                    "overflow chain exceeded reported length",
                ));
            }
            dst.extend_from_slice(data);
            checksum.update(data);
            remaining -= used_usize;
            pages_left -= 1;
            current = next;
        }
        if current.0 != 0 {
            return Err(SombraError::Corruption(
                "overflow chain longer than n_pages",
            ));
        }
        if remaining != 0 {
            return Err(SombraError::Corruption(
                "overflow chain shorter than reported length",
            ));
        }
        let computed = checksum.finalize();
        if computed != vref.checksum {
            return Err(SombraError::Corruption("overflow checksum mismatch"));
        }
        self.metrics.add_bytes_read(vref.len as u64);
        trace!(
            pages = vref.n_pages,
            len = vref.len,
            "vstore.read_with_write"
        );
        Ok(())
    }

    /// Frees the overflow pages associated with a VRef.
    pub fn free(&self, tx: &mut WriteGuard<'_>, vref: VRef) -> Result<()> {
        if vref.n_pages == 0 {
            return Ok(());
        }
        let mut current = vref.start_page;
        let mut remaining = vref.n_pages;
        while remaining > 0 {
            if current.0 == 0 {
                return Err(SombraError::Corruption(
                    "overflow free chain shorter than expected",
                ));
            }
            let page = tx.page_mut(current)?;
            let (next, _, _) = self.decode_page(page.data())?;
            drop(page);
            tx.free_page(current)?;
            current = next;
            remaining -= 1;
        }
        if current.0 != 0 {
            return Err(SombraError::Corruption(
                "overflow free chain longer than expected",
            ));
        }
        self.metrics.add_pages_freed(vref.n_pages as u64);
        trace!(pages = vref.n_pages, len = vref.len, "vstore.free");
        Ok(())
    }

    /// Updates variable-length data in place if possible, otherwise reallocates.
    pub fn update(&self, tx: &mut WriteGuard<'_>, vref: &mut VRef, new: &[u8]) -> Result<()> {
        if new.len() > u32::MAX as usize {
            return Err(SombraError::Invalid("value larger than 4GB not supported"));
        }
        let total_capacity = self.data_capacity * vref.n_pages as usize;
        if new.len() <= total_capacity {
            let mut current = vref.start_page;
            let mut pages_left = vref.n_pages;
            let mut remaining = new;
            let mut checksum = Crc32Fast::default();
            while pages_left > 0 {
                if current.0 == 0 {
                    return Err(SombraError::Corruption("overflow chain terminated early"));
                }
                let mut page = tx.page_mut(current)?;
                let (next, _, _) = self.decode_page(page.data())?;
                let chunk_len = min(remaining.len(), self.data_capacity);
                let (chunk, rest) = remaining.split_at(chunk_len);
                checksum.update(chunk);
                self.write_payload(&mut page, current, next, chunk)?;
                remaining = rest;
                current = next;
                pages_left -= 1;
            }
            if !remaining.is_empty() {
                return Err(SombraError::Corruption(
                    "overflow chain shorter than expected during update",
                ));
            }
            vref.len = new.len() as u32;
            vref.checksum = checksum.finalize();
            self.metrics.add_bytes_written(new.len() as u64);
            trace!(pages = vref.n_pages, len = vref.len, "vstore.update.inline");
            return Ok(());
        }

        let replacement = self.write(tx, new)?;
        self.free(tx, *vref)?;
        *vref = replacement;
        trace!(
            pages = vref.n_pages,
            len = vref.len,
            "vstore.update.reallocate"
        );
        Ok(())
    }

    fn init_overflow_page(
        &self,
        page: &mut PageMut<'_>,
        page_id: PageId,
        next: PageId,
        payload: &[u8],
    ) -> Result<()> {
        let buf = page.data_mut();
        if buf.len() < self.page_size {
            return Err(SombraError::Invalid(
                "overflow page buffer shorter than configured size",
            ));
        }
        buf[..self.page_size].fill(0);
        let header = PageHeader::new(
            page_id,
            PageKind::Overflow,
            self.page_size as u32,
            self.salt,
        )?
        .with_crc32(0);
        header.encode(&mut buf[..PAGE_HDR_LEN])?;
        self.write_payload_raw(&mut buf[PAGE_HDR_LEN..self.page_size], next, payload)?;
        Ok(())
    }

    fn write_payload(
        &self,
        page: &mut PageMut<'_>,
        page_id: PageId,
        next: PageId,
        payload: &[u8],
    ) -> Result<()> {
        let buf = page.data_mut();
        if buf.len() < self.page_size {
            return Err(SombraError::Invalid(
                "overflow page buffer shorter than configured size",
            ));
        }
        self.ensure_overflow_header(buf, page_id)?;
        self.write_payload_raw(&mut buf[PAGE_HDR_LEN..self.page_size], next, payload)?;
        Ok(())
    }

    fn ensure_overflow_header(&self, buf: &[u8], expected_id: PageId) -> Result<()> {
        let header = PageHeader::decode(&buf[..PAGE_HDR_LEN])?;
        if header.kind != PageKind::Overflow {
            return Err(SombraError::Corruption("page is not overflow kind"));
        }
        if header.page_no != expected_id {
            return Err(SombraError::Corruption("overflow page id mismatch"));
        }
        if header.page_size as usize != self.page_size {
            return Err(SombraError::Corruption("overflow page size mismatch"));
        }
        Ok(())
    }

    fn write_payload_raw(
        &self,
        payload_buf: &mut [u8],
        next: PageId,
        payload: &[u8],
    ) -> Result<()> {
        if payload_buf.len() < OVERFLOW_HEADER_LEN {
            return Err(SombraError::Invalid("overflow payload buffer too small"));
        }
        if payload.len() > self.data_capacity {
            return Err(SombraError::Invalid("payload exceeds overflow capacity"));
        }
        payload_buf.fill(0);
        payload_buf[..8].copy_from_slice(&next.0.to_be_bytes());
        payload_buf[8..12].copy_from_slice(&(payload.len() as u32).to_be_bytes());
        payload_buf[12..16].fill(0);
        let data_end = OVERFLOW_HEADER_LEN + payload.len();
        payload_buf[OVERFLOW_HEADER_LEN..data_end].copy_from_slice(payload);
        Ok(())
    }

    fn decode_page<'a>(&self, data: &'a [u8]) -> Result<(PageId, u32, &'a [u8])> {
        if data.len() < self.page_size {
            return Err(SombraError::Corruption("overflow page truncated"));
        }
        let header = PageHeader::decode(&data[..PAGE_HDR_LEN])?;
        if header.kind != PageKind::Overflow {
            return Err(SombraError::Corruption("page kind mismatch"));
        }
        if header.page_size as usize != self.page_size {
            return Err(SombraError::Corruption("overflow page size mismatch"));
        }
        let payload = &data[PAGE_HDR_LEN..self.page_size];
        let next = PageId(u64::from_be_bytes(payload[..8].try_into().map_err(
            |_| SombraError::Corruption("overflow next pointer truncated"),
        )?));
        let used = u32::from_be_bytes(
            payload[8..12]
                .try_into()
                .map_err(|_| SombraError::Corruption("overflow used bytes truncated"))?,
        );
        if payload[12..16] != [0; 4] {
            return Err(SombraError::Corruption("overflow reserved bytes not zero"));
        }
        if used as usize > self.data_capacity {
            return Err(SombraError::Corruption(
                "overflow used bytes exceed capacity",
            ));
        }
        let data_end = OVERFLOW_HEADER_LEN + used as usize;
        if data_end > payload.len() {
            return Err(SombraError::Corruption("overflow payload truncated"));
        }
        Ok((next, used, &payload[OVERFLOW_HEADER_LEN..data_end]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitives::pager::{PageStore, Pager, PagerOptions};
    use std::sync::Arc;
    use tempfile::tempdir;

    #[test]
    fn vstore_large_write_tracks_extent_metrics() -> Result<()> {
        let dir = tempdir().unwrap();
        let path = dir.path().join("vstore_extents.db");
        let pager: Arc<dyn PageStore> = Arc::new(Pager::create(&path, PagerOptions::default())?);
        let vstore = VStore::open(Arc::clone(&pager))?;
        let mut write = pager.begin_write()?;
        let payload = vec![7u8; (pager.page_size() as usize * 2) + 256];
        let vref = vstore.write(&mut write, &payload)?;
        pager.commit(write)?;
        let snapshot = vstore.metrics_snapshot();
        assert_eq!(snapshot.extent_writes, 1);
        assert!(snapshot.extent_segments >= 1);
        assert_eq!(snapshot.extent_pages, vref.n_pages as u64);
        Ok(())
    }
}
