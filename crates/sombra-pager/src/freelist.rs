use std::cmp::Ordering;
use std::collections::BinaryHeap;

use sombra_checksum::page_crc32;
use sombra_types::page::{self, PageHeader, PageKind, PAGE_HDR_LEN};
use sombra_types::{PageId, Result, SombraError};

use crate::meta::Meta;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Extent {
    pub start: PageId,
    pub len: u32,
}

impl Extent {
    pub fn new(start: PageId, len: u32) -> Self {
        Self { start, len }
    }

    pub fn coalesce_with(&mut self, other: &Extent) -> bool {
        if self.start.0 + self.len as u64 == other.start.0 {
            self.len += other.len;
            true
        } else {
            false
        }
    }

    pub fn iter_pages(&self) -> impl Iterator<Item = PageId> + '_ {
        (0..self.len).map(move |off| PageId(self.start.0 + off as u64))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct HeapExtent {
    start: PageId,
    len: u32,
}

impl Ord for HeapExtent {
    fn cmp(&self, other: &Self) -> Ordering {
        self.len
            .cmp(&other.len)
            .then_with(|| other.start.0.cmp(&self.start.0))
    }
}

impl PartialOrd for HeapExtent {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl From<HeapExtent> for Extent {
    fn from(extent: HeapExtent) -> Self {
        Extent::new(extent.start, extent.len)
    }
}

impl From<Extent> for HeapExtent {
    fn from(extent: Extent) -> Self {
        HeapExtent {
            start: extent.start,
            len: extent.len,
        }
    }
}

#[derive(Clone, Default, Debug)]
pub struct FreeCache {
    extents: Vec<Extent>,
    heap: BinaryHeap<HeapExtent>,
}

impl FreeCache {
    pub fn from_extents(extents: Vec<Extent>) -> Self {
        let mut cache = Self {
            extents,
            heap: BinaryHeap::new(),
        };
        cache.rebuild();
        cache
    }

    pub fn extents(&self) -> &[Extent] {
        &self.extents
    }

    pub fn pop(&mut self) -> Option<PageId> {
        let mut extent = self.heap.pop()?;
        let page = extent.start;
        if let Ok(pos) = self
            .extents
            .binary_search_by_key(&extent.start.0, |e| e.start.0)
        {
            if extent.len > 1 {
                self.extents[pos].start.0 += 1;
                self.extents[pos].len -= 1;
                extent.start.0 += 1;
                extent.len -= 1;
                self.heap.push(extent);
            } else {
                self.extents.remove(pos);
            }
        } else {
            self.heap.push(extent);
            self.rebuild();
            return self.pop();
        }
        Some(page)
    }

    pub fn extend(&mut self, mut extents: Vec<Extent>) {
        if extents.is_empty() {
            return;
        }
        self.extents.append(&mut extents);
        self.rebuild();
    }

    fn rebuild(&mut self) {
        if self.extents.is_empty() {
            self.heap.clear();
            return;
        }
        self.extents.sort_by(|a, b| a.start.0.cmp(&b.start.0));
        let mut merged: Vec<Extent> = Vec::with_capacity(self.extents.len());
        for extent in self.extents.drain(..) {
            if let Some(last) = merged.last_mut() {
                if last.coalesce_with(&extent) {
                    continue;
                }
            }
            merged.push(extent);
        }
        self.heap = BinaryHeap::from(
            merged
                .iter()
                .copied()
                .map(HeapExtent::from)
                .collect::<Vec<_>>(),
        );
        self.extents = merged;
    }
}

pub fn free_page_capacity(page_size: usize) -> usize {
    let payload = page_size
        .checked_sub(PAGE_HDR_LEN)
        .expect("page size smaller than header");
    payload.saturating_sub(16) / 16
}

pub struct FreePage {
    pub next: PageId,
    pub extents: Vec<Extent>,
}

pub fn read_free_page(buf: &[u8], page_size: usize, meta: &Meta) -> Result<FreePage> {
    if buf.len() < PAGE_HDR_LEN {
        return Err(SombraError::Corruption("free page truncated"));
    }
    let header = PageHeader::decode(&buf[..PAGE_HDR_LEN])?;
    if header.kind != PageKind::FreeList {
        return Err(SombraError::Corruption("free page kind mismatch"));
    }
    if header.page_size != meta.page_size {
        return Err(SombraError::Corruption("free page size mismatch"));
    }
    if header.page_no.0 == 0 {
        return Err(SombraError::Corruption("free page cannot be page 0"));
    }
    if buf.len() < page_size {
        return Err(SombraError::Corruption("free page truncated"));
    }
    let mut scratch = buf[..page_size].to_vec();
    page::clear_crc32(&mut scratch[..PAGE_HDR_LEN])?;
    let crc = page_crc32(header.page_no.0, meta.salt, &scratch);
    if crc != header.crc32 {
        return Err(SombraError::Corruption("free page crc mismatch"));
    }

    let payload = &buf[PAGE_HDR_LEN..page_size];
    let next = PageId(u64::from_be_bytes(payload[0..8].try_into().unwrap()));
    let count = u32::from_be_bytes(payload[8..12].try_into().unwrap()) as usize;
    if payload[12..16] != [0; 4] {
        return Err(SombraError::Corruption("free page reserved non-zero"));
    }
    let capacity = free_page_capacity(page_size);
    if count > capacity {
        return Err(SombraError::Corruption("free page count exceeds capacity"));
    }
    let mut extents = Vec::with_capacity(count);
    for i in 0..count {
        let off = 16 + i * 16;
        let start = PageId(u64::from_be_bytes(
            payload[off..off + 8].try_into().unwrap(),
        ));
        let len = u32::from_be_bytes(payload[off + 8..off + 12].try_into().unwrap());
        extents.push(Extent::new(start, len));
    }
    Ok(FreePage { next, extents })
}

pub fn write_free_page(
    buf: &mut [u8],
    page_id: PageId,
    meta: &Meta,
    next: PageId,
    extents: &[Extent],
) -> Result<()> {
    let page_size = meta.page_size as usize;
    if buf.len() < page_size {
        return Err(SombraError::Invalid("free page buffer too small"));
    }
    buf[..page_size].fill(0);
    let header =
        PageHeader::new(page_id, PageKind::FreeList, meta.page_size, meta.salt)?.with_crc32(0);
    header.encode(&mut buf[..PAGE_HDR_LEN])?;
    let payload = &mut buf[PAGE_HDR_LEN..page_size];
    payload[..8].copy_from_slice(&next.0.to_be_bytes());
    payload[8..12].copy_from_slice(&(extents.len() as u32).to_be_bytes());
    // reserved already zeroed
    for (idx, extent) in extents.iter().enumerate() {
        let off = 16 + idx * 16;
        payload[off..off + 8].copy_from_slice(&extent.start.0.to_be_bytes());
        payload[off + 8..off + 12].copy_from_slice(&extent.len.to_be_bytes());
    }
    page::clear_crc32(&mut buf[..PAGE_HDR_LEN])?;
    let crc = page_crc32(page_id.0, meta.salt, &buf[..page_size]);
    buf[page::header::CRC32].copy_from_slice(&crc.to_be_bytes());
    Ok(())
}
