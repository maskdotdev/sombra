use std::fs;
use std::io;
use std::path::Path;

use crate::primitives::pager::PagerStats;
use serde::Serialize;

use crate::admin::options::AdminOpenOptions;
use crate::admin::util::{open_graph, open_pager, wal_path};
use crate::admin::Result;

/// Comprehensive statistics report for a database instance.
///
/// Contains detailed information about the pager, WAL, storage, and filesystem
/// aspects of the database.
#[derive(Debug, Clone, Serialize)]
pub struct StatsReport {
    /// Statistics about the pager component.
    pub pager: PagerStatsSection,
    /// Statistics about the write-ahead log (WAL).
    pub wal: WalStatsSection,
    /// Statistics about the storage layer.
    pub storage: StorageStatsSection,
    /// Statistics about filesystem usage.
    pub filesystem: FilesystemStats,
    /// Detailed storage space breakdown across components.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub storage_space: Option<StorageSpaceStats>,
}

/// Pager-related statistics and configuration.
///
/// Provides information about the page cache, hit/miss rates, and checkpoint state.
#[derive(Debug, Clone, Serialize)]
pub struct PagerStatsSection {
    /// Size of each page in bytes.
    pub page_size: u32,
    /// Number of pages in the cache.
    pub cache_pages: usize,
    /// Number of cache hits.
    pub hits: u64,
    /// Number of cache misses.
    pub misses: u64,
    /// Number of pages evicted from the cache.
    pub evictions: u64,
    /// Number of dirty pages written back to disk.
    pub dirty_writebacks: u64,
    /// Log sequence number of the last checkpoint.
    pub last_checkpoint_lsn: u64,
    /// Total MVCC page versions retained in memory.
    pub mvcc_page_versions_total: u64,
    /// Number of pages currently tracking historical versions.
    pub mvcc_pages_with_versions: u64,
    /// Active mvcc readers.
    pub mvcc_readers_active: u64,
    /// Total reader begin events observed.
    pub mvcc_reader_begin_total: u64,
    /// Total reader end events observed.
    pub mvcc_reader_end_total: u64,
    /// Oldest reader snapshot commit ID.
    pub mvcc_reader_oldest_snapshot: u64,
    /// Newest reader snapshot commit ID.
    pub mvcc_reader_newest_snapshot: u64,
    /// Maximum reader age in milliseconds.
    pub mvcc_reader_max_age_ms: u64,
    /// Maximum MVCC version chain length.
    pub mvcc_max_chain_len: u64,
    /// Pages with uncheckpointed overlays present.
    pub mvcc_overlay_pages: u64,
    /// Total overlay entries across pages.
    pub mvcc_overlay_entries: u64,
    /// Active reader locks in the file lock coordinator.
    pub lock_readers: u32,
    /// Whether the writer lock is held.
    pub lock_writer: bool,
    /// Whether the checkpoint lock is held.
    pub lock_checkpoint: bool,
}

/// Write-ahead log (WAL) statistics.
///
/// Contains information about the WAL directory location, size, and state.
#[derive(Debug, Clone, Serialize)]
pub struct WalStatsSection {
    /// Path to the WAL directory.
    pub path: String,
    /// Whether the WAL directory exists on disk.
    pub exists: bool,
    /// Total size of WAL files in bytes.
    pub size_bytes: u64,
    /// Log sequence number of the last checkpoint.
    pub last_checkpoint_lsn: u64,
    /// WAL segment size in bytes.
    pub segment_size_bytes: u64,
    /// Target number of preallocated segments.
    pub preallocate_segments: u32,
    /// Segments ready for activation.
    pub ready_segments: usize,
    /// Segments queued for recycling.
    pub recycle_segments: usize,
    /// Last allocator error (e.g., ENOSPC) if any.
    pub allocation_error: Option<String>,
}

/// Storage layer statistics.
///
/// Provides information about nodes, edges, and storage configuration.
#[derive(Debug, Clone, Serialize)]
pub struct StorageStatsSection {
    /// Next available node ID.
    pub next_node_id: u64,
    /// Next available edge ID.
    pub next_edge_id: u64,
    /// Estimated number of nodes in the database.
    pub estimated_node_count: u64,
    /// Estimated number of edges in the database.
    pub estimated_edge_count: u64,
    /// Maximum size for inline property blobs in bytes.
    pub inline_prop_blob: u32,
    /// Maximum size for inline property values in bytes.
    pub inline_prop_value: u32,
    /// Storage configuration flags.
    pub storage_flags: u32,
    /// Whether distinct neighbors are enforced by default.
    pub distinct_neighbors_default: bool,
}

/// Filesystem statistics for database files.
///
/// Contains information about file paths and sizes for the database and WAL.
#[derive(Debug, Clone, Serialize)]
pub struct FilesystemStats {
    /// Path to the database file.
    pub db_path: String,
    /// Size of the database file in bytes.
    pub db_size_bytes: u64,
    /// Path to the WAL file.
    pub wal_path: String,
    /// Size of the WAL file in bytes.
    pub wal_size_bytes: u64,
}

/// Detailed storage space statistics for core graph components.
#[derive(Debug, Clone, Serialize)]
pub struct StorageSpaceStats {
    /// Bytes retained inside the version log B-tree.
    pub version_log_bytes: u64,
    /// Number of entries stored in the version log.
    pub version_log_entries: u64,
    /// Overflow pages allocated by the VStore.
    pub vstore_pages_allocated: u64,
    /// Overflow pages freed by the VStore.
    pub vstore_pages_freed: u64,
    /// Total bytes written into VStore overflow pages.
    pub vstore_bytes_written: u64,
    /// Total bytes read from VStore overflow pages.
    pub vstore_bytes_read: u64,
    /// Number of writes that used at least one extent.
    pub vstore_extent_writes: u64,
    /// Total number of extent segments allocated.
    pub vstore_extent_segments: u64,
    /// Total number of pages covered by extent allocations.
    pub vstore_extent_pages: u64,
    /// B-tree pages used by the nodes tree.
    pub nodes_tree_pages: u64,
    /// Approximate bytes used in the nodes tree payloads.
    pub nodes_tree_bytes: u64,
    /// B-tree pages used by the edges tree.
    pub edges_tree_pages: u64,
    /// Approximate bytes used in the edges tree payloads.
    pub edges_tree_bytes: u64,
    /// B-tree pages used by the forward adjacency tree.
    pub adj_fwd_tree_pages: u64,
    /// Approximate bytes used in the forward adjacency tree payloads.
    pub adj_fwd_tree_bytes: u64,
    /// B-tree pages used by the reverse adjacency tree.
    pub adj_rev_tree_pages: u64,
    /// Approximate bytes used in the reverse adjacency tree payloads.
    pub adj_rev_tree_bytes: u64,
    /// B-tree pages used by all index trees combined.
    pub index_tree_pages: u64,
    /// Approximate bytes used across all index trees.
    pub index_tree_bytes: u64,
    /// B-tree pages used by the version log heap.
    pub version_log_pages: u64,
}

/// Collects comprehensive statistics about a database.
///
/// This function gathers statistics from the pager, WAL, storage layer, and filesystem
/// for the database at the specified path.
///
/// # Arguments
///
/// * `path` - Path to the database file
/// * `opts` - Admin options for opening the database
///
/// # Returns
///
/// Returns a `StatsReport` containing all collected statistics, or an error if the
/// database cannot be opened or read.
///
/// # Errors
///
/// Returns an error if:
/// - The database file cannot be opened
/// - The pager metadata cannot be read
/// - File metadata cannot be retrieved
pub fn stats(path: impl AsRef<Path>, opts: &AdminOpenOptions) -> Result<StatsReport> {
    let path = path.as_ref();
    let pager = open_pager(path, opts)?;
    let meta = pager.meta()?;
    let pager_counters: PagerStats = pager.stats();
    let wal_allocator = pager.wal_allocator_stats();
    let db_meta = fs::metadata(path)?;
    let wal_path = wal_path(path);
    let wal_meta = fs::metadata(&wal_path).ok();
    let wal_size = wal_meta
        .as_ref()
        .map(|_| directory_size(&wal_path).unwrap_or(0))
        .unwrap_or(0);
    let wal_exists = wal_meta.is_some();

    let storage_space = collect_storage_space_stats(path, opts)?;

    let pager_section = PagerStatsSection {
        page_size: meta.page_size,
        cache_pages: opts.pager.cache_pages,
        hits: pager_counters.hits,
        misses: pager_counters.misses,
        evictions: pager_counters.evictions,
        dirty_writebacks: pager_counters.dirty_writebacks,
        last_checkpoint_lsn: meta.last_checkpoint_lsn.0,
        mvcc_page_versions_total: pager_counters.mvcc_page_versions_total,
        mvcc_pages_with_versions: pager_counters.mvcc_pages_with_versions,
        mvcc_readers_active: pager_counters.mvcc_readers_active,
        mvcc_reader_begin_total: pager_counters.mvcc_reader_begin_total,
        mvcc_reader_end_total: pager_counters.mvcc_reader_end_total,
        mvcc_reader_oldest_snapshot: pager_counters.mvcc_reader_oldest_snapshot,
        mvcc_reader_newest_snapshot: pager_counters.mvcc_reader_newest_snapshot,
        mvcc_reader_max_age_ms: pager_counters.mvcc_reader_max_age_ms,
        mvcc_max_chain_len: pager_counters.mvcc_max_chain_len,
        mvcc_overlay_pages: pager_counters.mvcc_overlay_pages,
        mvcc_overlay_entries: pager_counters.mvcc_overlay_entries,
        lock_readers: pager_counters.lock_readers,
        lock_writer: pager_counters.lock_writer,
        lock_checkpoint: pager_counters.lock_checkpoint,
    };

    let wal_section = WalStatsSection {
        path: wal_path.display().to_string(),
        exists: wal_exists,
        size_bytes: wal_size,
        last_checkpoint_lsn: meta.last_checkpoint_lsn.0,
        segment_size_bytes: wal_allocator.segment_size_bytes,
        preallocate_segments: wal_allocator.preallocate_segments,
        ready_segments: wal_allocator.ready_segments,
        recycle_segments: wal_allocator.recycle_segments,
        allocation_error: wal_allocator.allocation_error,
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
        storage_space: Some(storage_space),
    })
}

fn collect_storage_space_stats(
    path: &Path,
    opts: &AdminOpenOptions,
) -> Result<StorageSpaceStats> {
    let handle = open_graph(path, opts)?;
    let mvcc = handle.graph.mvcc_status();
    let vstore = handle.graph.vstore_metrics_snapshot();
    let usage = handle.graph.space_usage()?;

    Ok(StorageSpaceStats {
        version_log_bytes: mvcc.version_log_bytes,
        version_log_entries: mvcc.version_log_entries,
        vstore_pages_allocated: vstore.pages_allocated,
        vstore_pages_freed: vstore.pages_freed,
        vstore_bytes_written: vstore.bytes_written,
        vstore_bytes_read: vstore.bytes_read,
        vstore_extent_writes: vstore.extent_writes,
        vstore_extent_segments: vstore.extent_segments,
        vstore_extent_pages: vstore.extent_pages,
        nodes_tree_pages: usage.nodes_pages,
        nodes_tree_bytes: usage.nodes_bytes,
        edges_tree_pages: usage.edges_pages,
        edges_tree_bytes: usage.edges_bytes,
        adj_fwd_tree_pages: usage.adj_fwd_pages,
        adj_fwd_tree_bytes: usage.adj_fwd_bytes,
        adj_rev_tree_pages: usage.adj_rev_pages,
        adj_rev_tree_bytes: usage.adj_rev_bytes,
        index_tree_pages: usage.index_pages,
        index_tree_bytes: usage.index_bytes,
        version_log_pages: usage.version_log_pages,
    })
}

fn directory_size(path: &Path) -> io::Result<u64> {
    let meta = fs::metadata(path)?;
    if meta.is_file() {
        return Ok(meta.len());
    }
    if meta.is_dir() {
        let mut total = 0;
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            total += directory_size(&entry.path())?;
        }
        return Ok(total);
    }
    Ok(0)
}
