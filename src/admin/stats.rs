use std::fs;
use std::path::Path;

use crate::primitives::pager::PagerStats;
use serde::Serialize;

use crate::admin::options::AdminOpenOptions;
use crate::admin::util::{open_pager, wal_path};
use crate::admin::Result;

#[derive(Debug, Clone, Serialize)]
pub struct StatsReport {
    pub pager: PagerStatsSection,
    pub wal: WalStatsSection,
    pub storage: StorageStatsSection,
    pub filesystem: FilesystemStats,
}

#[derive(Debug, Clone, Serialize)]
pub struct PagerStatsSection {
    pub page_size: u32,
    pub cache_pages: usize,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub dirty_writebacks: u64,
    pub last_checkpoint_lsn: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct WalStatsSection {
    pub path: String,
    pub exists: bool,
    pub size_bytes: u64,
    pub last_checkpoint_lsn: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StorageStatsSection {
    pub next_node_id: u64,
    pub next_edge_id: u64,
    pub estimated_node_count: u64,
    pub estimated_edge_count: u64,
    pub inline_prop_blob: u32,
    pub inline_prop_value: u32,
    pub storage_flags: u32,
    pub distinct_neighbors_default: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct FilesystemStats {
    pub db_path: String,
    pub db_size_bytes: u64,
    pub wal_path: String,
    pub wal_size_bytes: u64,
}

pub fn stats(path: impl AsRef<Path>, opts: &AdminOpenOptions) -> Result<StatsReport> {
    let path = path.as_ref();
    let pager = open_pager(path, opts)?;
    let meta = pager.meta()?;
    let pager_counters: PagerStats = pager.stats();
    let db_meta = fs::metadata(path)?;
    let wal_path = wal_path(path);
    let wal_meta = fs::metadata(&wal_path).ok();
    let wal_size = wal_meta.as_ref().map(|m| m.len()).unwrap_or(0);
    let wal_exists = wal_meta.is_some();

    let pager_section = PagerStatsSection {
        page_size: meta.page_size,
        cache_pages: opts.pager.cache_pages,
        hits: pager_counters.hits,
        misses: pager_counters.misses,
        evictions: pager_counters.evictions,
        dirty_writebacks: pager_counters.dirty_writebacks,
        last_checkpoint_lsn: meta.last_checkpoint_lsn.0,
    };

    let wal_section = WalStatsSection {
        path: wal_path.display().to_string(),
        exists: wal_exists,
        size_bytes: wal_size,
        last_checkpoint_lsn: meta.last_checkpoint_lsn.0,
    };

    let estimated_node_count = meta.storage_next_node_id.saturating_sub(1);
    let estimated_edge_count = meta.storage_next_edge_id.saturating_sub(1);
    let storage_section = StorageStatsSection {
        next_node_id: meta.storage_next_node_id,
        next_edge_id: meta.storage_next_edge_id,
        estimated_node_count,
        estimated_edge_count,
        inline_prop_blob: meta.storage_inline_prop_blob,
        inline_prop_value: meta.storage_inline_prop_value,
        storage_flags: meta.storage_flags,
        distinct_neighbors_default: opts.distinct_neighbors_default,
    };

    let filesystem_section = FilesystemStats {
        db_path: path.display().to_string(),
        db_size_bytes: db_meta.len(),
        wal_path: wal_section.path.clone(),
        wal_size_bytes: wal_size,
    };

    Ok(StatsReport {
        pager: pager_section,
        wal: wal_section,
        storage: storage_section,
        filesystem: filesystem_section,
    })
}
