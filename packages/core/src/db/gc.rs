//! Garbage Collection for MVCC Version Chains
//!
//! This module implements garbage collection (GC) for multi-version concurrency
//! control (MVCC) version chains. As transactions create new versions of records,
//! old versions accumulate and must be periodically cleaned up to prevent
//! unbounded memory and disk growth.
//!
//! # Overview
//!
//! The GC system identifies and removes old versions that are no longer needed
//! by any active transaction. It works by:
//!
//! 1. Calculating a GC watermark (minimum active snapshot timestamp)
//! 2. Scanning version chains to find reclaimable versions
//! 3. Compacting version chains by removing old versions
//! 4. Reclaiming freed pages back to the storage system
//!
//! # Safety
//!
//! GC must be careful to never remove versions that might still be visible
//! to active transactions. The watermark calculation ensures that only
//! versions older than all active snapshots are eligible for collection.
//!
//! # Usage
//!
//! ```rust
//! use sombra::db::gc::GarbageCollector;
//!
//! let mut gc = GarbageCollector::new();
//! let stats = gc.run_gc(&mut db)?;
//! println!("Reclaimed {} versions", stats.versions_reclaimed);
//! ```

use crate::db::timestamp_oracle::TimestampOracle;
use crate::error::{GraphError, Result};
use crate::storage::version::{VersionMetadata, VersionedRecordKind};
use crate::storage::{RecordPointer, RecordStore};
use std::sync::Arc;
use tracing::{debug, info, trace, warn};

/// Statistics collected during a garbage collection run
#[derive(Debug, Clone, Default)]
pub struct GcStats {
    /// Number of version chains scanned
    pub chains_scanned: usize,
    /// Number of versions examined
    pub versions_examined: usize,
    /// Number of versions marked as reclaimable
    pub versions_reclaimable: usize,
    /// Number of versions actually reclaimed
    pub versions_reclaimed: usize,
    /// Number of pages freed
    pub pages_freed: usize,
    /// Duration of the GC run in milliseconds
    pub duration_ms: u64,
    /// GC watermark timestamp used
    pub gc_watermark: u64,
}

impl GcStats {
    /// Creates new empty GC statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns true if any versions were reclaimed
    pub fn any_reclaimed(&self) -> bool {
        self.versions_reclaimed > 0
    }
}

/// Information about a reclaimable version
#[derive(Debug, Clone)]
pub struct ReclaimableVersion {
    /// Pointer to the version
    pub pointer: RecordPointer,
    /// Record ID this version belongs to
    pub record_id: u64,
    /// Commit timestamp of this version
    pub commit_ts: u64,
}

/// Garbage collector for MVCC version chains
///
/// The garbage collector scans version chains and removes old versions
/// that are no longer needed by any active transaction.
pub struct GarbageCollector {
    /// Minimum number of versions to keep per record (default: 1)
    min_versions_per_record: usize,
    /// Maximum number of versions to scan in a single batch
    scan_batch_size: usize,
}

impl GarbageCollector {
    /// Creates a new garbage collector with default settings
    pub fn new() -> Self {
        Self {
            min_versions_per_record: 1,
            scan_batch_size: 1000,
        }
    }

    /// Creates a garbage collector with custom settings
    ///
    /// # Arguments
    /// * `min_versions_per_record` - Minimum versions to keep (must be >= 1)
    /// * `scan_batch_size` - Number of versions to scan per batch
    pub fn with_settings(min_versions_per_record: usize, scan_batch_size: usize) -> Result<Self> {
        if min_versions_per_record == 0 {
            return Err(GraphError::InvalidArgument(
                "min_versions_per_record must be >= 1".into(),
            ));
        }

        Ok(Self {
            min_versions_per_record,
            scan_batch_size,
        })
    }

    /// Checks if a record is versioned (has MVCC metadata)
    ///
    /// # Arguments
    /// * `record_store` - The record store to read from
    /// * `pointer` - Pointer to the record
    ///
    /// # Returns
    /// True if the record is versioned (VersionedNode or VersionedEdge)
    /// False if the record is non-versioned, free, or invalid
    fn is_record_versioned(
        &self,
        record_store: &mut RecordStore,
        pointer: RecordPointer,
    ) -> Result<bool> {
        // Try to visit the record - if it fails, consider it non-versioned
        let result = record_store.visit_record(pointer, |record_data| {
            eprintln!("GC: is_record_versioned - record_len={}, first_bytes={:?}", 
                      record_data.len(), 
                      &record_data[0..std::cmp::min(10, record_data.len())]);
            
            if record_data.len() < 1 {
                debug!(
                    page_id = pointer.page_id,
                    slot_index = pointer.slot_index,
                    "Record too short, skipping"
                );
                return Ok(false);
            }
            let kind_byte = record_data[0];
            
            eprintln!("GC: is_record_versioned - kind_byte={} (0x{:02X})", kind_byte, kind_byte);
            
            // Try to parse the kind - if it fails, the record is invalid
            match VersionedRecordKind::from_byte(kind_byte) {
                Ok(kind) => {
                    let is_versioned = kind.is_versioned();
                    eprintln!("GC: is_record_versioned - kind={:?}, is_versioned={}", kind, is_versioned);
                    trace!(
                        page_id = pointer.page_id,
                        slot_index = pointer.slot_index,
                        kind_byte = kind_byte,
                        is_versioned = is_versioned,
                        "Checked record version status"
                    );
                    Ok(is_versioned)
                }
                Err(e) => {
                    // Invalid kind byte - treat as non-versioned
                    eprintln!("GC: is_record_versioned - ERROR parsing kind: {:?}", e);
                    debug!(
                        page_id = pointer.page_id,
                        slot_index = pointer.slot_index,
                        kind_byte = kind_byte,
                        "Invalid record kind, treating as non-versioned"
                    );
                    Ok(false)
                }
            }
        });
        
        // If visit_record itself fails (e.g., freed slot), consider it non-versioned
        match result {
            Ok(is_versioned) => {
                eprintln!("GC: is_record_versioned - returning {}", is_versioned);
                Ok(is_versioned)
            }
            Err(e) => {
                eprintln!("GC: is_record_versioned - visit_record failed: {:?}", e);
                debug!(
                    page_id = pointer.page_id,
                    slot_index = pointer.slot_index,
                    error = %e,
                    "Failed to visit record, treating as non-versioned"
                );
                Ok(false)
            }
        }
    }

    /// Scans a single version chain and identifies reclaimable versions
    ///
    /// This function traverses a version chain from head (newest) to tail (oldest)
    /// and identifies versions that are safe to reclaim based on the GC watermark.
    ///
    /// # Arguments
    /// * `record_store` - The record store to read from
    /// * `head_pointer` - Pointer to the newest version (head of chain)
    /// * `record_id` - The record ID for this chain
    /// * `gc_watermark` - Versions with commit_ts < watermark are candidates
    ///
    /// # Returns
    /// A vector of reclaimable version information
    pub fn scan_version_chain(
        &self,
        record_store: &mut RecordStore,
        head_pointer: RecordPointer,
        record_id: u64,
        gc_watermark: u64,
    ) -> Result<Vec<ReclaimableVersion>> {
        // First check if this is a versioned record
        // Non-versioned records (legacy or non-MVCC databases) should be skipped
        eprintln!("GC: scan_version_chain called for record_id={}, page={}, slot={}", 
                  record_id, head_pointer.page_id, head_pointer.slot_index);
        
        let is_versioned = self.is_record_versioned(record_store, head_pointer)?;
        eprintln!("GC: is_versioned={} for record_id={}", is_versioned, record_id);
        
        debug!(
            record_id = record_id,
            is_versioned = is_versioned,
            page_id = head_pointer.page_id,
            slot_index = head_pointer.slot_index,
            "Checked record version status in scan_version_chain"
        );
        
        if !is_versioned {
            eprintln!("GC: Skipping non-versioned record_id={}", record_id);
            debug!(
                record_id = record_id,
                "Skipping non-versioned record in scan_version_chain"
            );
            return Ok(Vec::new());
        }

        eprintln!("GC: About to scan versioned chain for record_id={}", record_id);

        let mut reclaimable = Vec::new();
        let mut current_pointer = Some(head_pointer);
        let mut version_count = 0;

        trace!(
            record_id = record_id,
            gc_watermark = gc_watermark,
            "Scanning version chain (after version check)"
        );

        // Traverse the version chain from head to tail
        while let Some(pointer) = current_pointer {
            version_count += 1;

            debug!(
                record_id = record_id,
                version = version_count,
                page_id = pointer.page_id,
                slot_index = pointer.slot_index,
                "About to read_version_metadata"
            );

            // Read the version metadata
            let metadata = self.read_version_metadata(record_store, pointer)?;
            
            debug!(
                record_id = record_id,
                version = version_count,
                commit_ts = metadata.commit_ts,
                "Successfully read version metadata"
            );

            // Check if this version is reclaimable
            if self.is_version_reclaimable(&metadata, version_count, gc_watermark) {
                reclaimable.push(ReclaimableVersion {
                    pointer,
                    record_id,
                    commit_ts: metadata.commit_ts,
                });

                trace!(
                    record_id = record_id,
                    version = version_count,
                    commit_ts = metadata.commit_ts,
                    "Found reclaimable version"
                );
            }

            // Move to the previous version in the chain
            current_pointer = metadata.prev_version;

            // Safety check: prevent infinite loops
            if version_count > 10000 {
                warn!(
                    record_id = record_id,
                    version_count = version_count,
                    "Version chain unexpectedly long, stopping scan"
                );
                break;
            }
        }

        debug!(
            record_id = record_id,
            total_versions = version_count,
            reclaimable_versions = reclaimable.len(),
            "Completed version chain scan"
        );

        Ok(reclaimable)
    }

    /// Checks if a specific version is safe to reclaim
    ///
    /// A version is reclaimable if:
    /// 1. It was committed before the GC watermark (no active txn can see it)
    /// 2. It's not one of the minimum required versions per record
    /// 3. It's actually committed (commit_ts != 0)
    ///
    /// # Arguments
    /// * `metadata` - The version metadata to check
    /// * `version_position` - Position in chain (1 = head/newest, higher = older)
    /// * `gc_watermark` - The GC watermark timestamp
    ///
    /// # Returns
    /// True if this version can be safely reclaimed
    fn is_version_reclaimable(
        &self,
        metadata: &VersionMetadata,
        version_position: usize,
        gc_watermark: u64,
    ) -> bool {
        // Never reclaim uncommitted versions
        if metadata.commit_ts == 0 {
            return false;
        }

        // Always preserve the minimum number of versions
        if version_position <= self.min_versions_per_record {
            return false;
        }

        // Only reclaim versions older than the GC watermark
        // (no active transaction can see them)
        metadata.commit_ts < gc_watermark
    }

    /// Reads version metadata from a record pointer
    ///
    /// # Arguments
    /// * `record_store` - The record store to read from
    /// * `pointer` - Pointer to the versioned record
    ///
    /// # Returns
    /// The version metadata, or an error if the record is invalid
    fn read_version_metadata(
        &self,
        record_store: &mut RecordStore,
        pointer: RecordPointer,
    ) -> Result<VersionMetadata> {
        record_store.visit_record(pointer, |record_data| {
            eprintln!("GC: read_version_metadata - record_len={}, first_bytes={:?}", 
                      record_data.len(), 
                      &record_data[0..std::cmp::min(10, record_data.len())]);
            
            debug!(
                page_id = pointer.page_id,
                slot_index = pointer.slot_index,
                record_len = record_data.len(),
                "read_version_metadata: visiting record"
            );
            
            if record_data.len() < 8 {
                eprintln!("GC: read_version_metadata - ERROR: record too short");
                return Err(GraphError::Corruption("record too short".into()));
            }

            let kind_byte = record_data[0];
            eprintln!("GC: read_version_metadata - kind_byte={} (0x{:02X})", kind_byte, kind_byte);
            
            debug!(
                page_id = pointer.page_id,
                slot_index = pointer.slot_index,
                kind_byte = kind_byte,
                "read_version_metadata: got kind byte"
            );
            
            let kind = VersionedRecordKind::from_byte(kind_byte)?;
            eprintln!("GC: read_version_metadata - kind={:?}, is_versioned={}", kind, kind.is_versioned());
            
            debug!(
                page_id = pointer.page_id,
                slot_index = pointer.slot_index,
                kind = ?kind,
                is_versioned = kind.is_versioned(),
                "read_version_metadata: parsed kind"
            );

            // Only versioned records have metadata
            if !kind.is_versioned() {
                eprintln!("GC: read_version_metadata - ERROR: not versioned!");
                return Err(GraphError::Corruption(
                    "attempted to read metadata from non-versioned record".into(),
                ));
            }

            // Metadata starts at offset 8 (after RecordHeader)
            if record_data.len() < 8 + 25 {
                eprintln!("GC: read_version_metadata - ERROR: versioned record too short");
                return Err(GraphError::Corruption("versioned record too short".into()));
            }

            let metadata = VersionMetadata::from_bytes(&record_data[8..33])?;
            eprintln!("GC: read_version_metadata - SUCCESS: commit_ts={}", metadata.commit_ts);
            Ok(metadata)
        })
    }

    /// Scans all version chains in a collection and identifies reclaimable versions
    ///
    /// This is the main entry point for the GC scanner. It iterates through
    /// all records and scans their version chains.
    ///
    /// # Arguments
    /// * `record_store` - The record store to scan
    /// * `record_ids` - Iterator of (record_id, head_pointer) pairs
    /// * `timestamp_oracle` - Oracle to get the GC watermark
    ///
    /// # Returns
    /// A vector of all reclaimable versions found
    pub fn scan_for_reclaimable_versions(
        &self,
        record_store: &mut RecordStore,
        record_ids: impl Iterator<Item = (u64, RecordPointer)>,
        timestamp_oracle: &Arc<TimestampOracle>,
    ) -> Result<Vec<ReclaimableVersion>> {
        let gc_watermark = timestamp_oracle.gc_eligible_before();
        let mut all_reclaimable = Vec::new();
        let mut chains_scanned = 0;

        info!(
            gc_watermark = gc_watermark,
            "Starting GC scan for reclaimable versions"
        );

        for (record_id, head_pointer) in record_ids {
            let reclaimable = self.scan_version_chain(
                record_store,
                head_pointer,
                record_id,
                gc_watermark,
            )?;

            all_reclaimable.extend(reclaimable);
            chains_scanned += 1;

            // Batch processing to avoid scanning everything at once
            if chains_scanned % self.scan_batch_size == 0 {
                debug!(
                    chains_scanned = chains_scanned,
                    reclaimable_found = all_reclaimable.len(),
                    "GC scan progress"
                );
            }
        }

        info!(
            chains_scanned = chains_scanned,
            total_reclaimable = all_reclaimable.len(),
            "Completed GC scan"
        );

        Ok(all_reclaimable)
    }

    /// Compacts version chains by removing reclaimable versions
    ///
    /// This function takes a list of reclaimable versions and frees them
    /// from the record store. This reclaims disk space but doesn't update
    /// the version chain pointers (they'll simply point to freed slots).
    ///
    /// # Arguments
    /// * `record_store` - The record store to modify
    /// * `reclaimable` - List of versions to reclaim
    ///
    /// # Returns
    /// The number of versions actually freed
    pub fn compact_version_chains(
        &self,
        record_store: &mut RecordStore,
        reclaimable: Vec<ReclaimableVersion>,
    ) -> Result<usize> {
        let mut freed_count = 0;

        for version in reclaimable {
            // Mark the old version as free in the record store
            // This reclaims the storage space
            match record_store.mark_free(version.pointer) {
                Ok(true) => {
                    freed_count += 1;
                    trace!(
                        record_id = version.record_id,
                        commit_ts = version.commit_ts,
                        "Freed old version"
                    );
                }
                Ok(false) => {
                    // Record was already freed or invalid
                    trace!(
                        record_id = version.record_id,
                        "Version already freed or invalid"
                    );
                }
                Err(e) => {
                    // Log error but continue with other versions
                    warn!(
                        record_id = version.record_id,
                        error = %e,
                        "Failed to free version"
                    );
                }
            }
        }

        info!(freed_count = freed_count, "Compacted version chains");
        Ok(freed_count)
    }

    /// Performs a complete GC cycle: scan and compact
    ///
    /// This is the main entry point for garbage collection. It:
    /// 1. Calculates the GC watermark
    /// 2. Scans all version chains for reclaimable versions
    /// 3. Compacts chains by freeing old versions
    ///
    /// # Arguments
    /// * `record_store` - The record store to operate on
    /// * `record_ids` - Iterator of (record_id, head_pointer) pairs
    /// * `timestamp_oracle` - Oracle for watermark calculation
    ///
    /// # Returns
    /// Statistics about the GC run
    pub fn run_gc(
        &self,
        record_store: &mut RecordStore,
        record_ids: impl Iterator<Item = (u64, RecordPointer)>,
        timestamp_oracle: &Arc<TimestampOracle>,
    ) -> Result<GcStats> {
        use std::time::Instant;

        let start = Instant::now();
        let gc_watermark = timestamp_oracle.gc_eligible_before();

        info!(gc_watermark = gc_watermark, "Starting GC cycle");

        // Phase 1: Scan for reclaimable versions
        let reclaimable = self.scan_for_reclaimable_versions(
            record_store,
            record_ids,
            timestamp_oracle,
        )?;

        let versions_reclaimable = reclaimable.len();

        // Phase 2: Compact version chains
        let versions_reclaimed = self.compact_version_chains(record_store, reclaimable)?;

        let duration = start.elapsed();

        let stats = GcStats {
            chains_scanned: 0, // TODO: Track this in scan_for_reclaimable_versions
            versions_examined: 0, // TODO: Track this in scan_version_chain
            versions_reclaimable,
            versions_reclaimed,
            pages_freed: 0, // TODO: Track freed pages
            duration_ms: duration.as_millis() as u64,
            gc_watermark,
        };

        info!(
            versions_reclaimed = stats.versions_reclaimed,
            duration_ms = stats.duration_ms,
            "GC cycle completed"
        );

        Ok(stats)
    }
}

impl Default for GarbageCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Messages for controlling background GC
#[derive(Debug)]
pub enum GcMessage {
    /// Trigger an immediate GC run
    Trigger,
    /// Shutdown the background GC thread
    Shutdown,
}

/// Configuration for background garbage collection
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Whether background GC is enabled
    pub enabled: bool,
    /// Interval between GC runs in seconds (None = disabled)
    pub interval_secs: Option<u64>,
    /// Minimum versions to keep per record
    pub min_versions_per_record: usize,
    /// Batch size for scanning version chains
    pub scan_batch_size: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            interval_secs: Some(60), // Run every minute by default
            min_versions_per_record: 1,
            scan_batch_size: 1000,
        }
    }
}

/// State for managing background garbage collection
///
/// This manages a background thread that periodically runs GC to clean up
/// old MVCC versions that are no longer needed by any active transaction.
pub struct BackgroundGcState {
    /// Channel sender to communicate with the GC thread
    pub sender: std::sync::mpsc::Sender<GcMessage>,
    /// Handle to the background GC thread (if running)
    pub _gc_thread: Option<std::thread::JoinHandle<()>>,
}

impl BackgroundGcState {
    /// Spawns a new background GC thread
    ///
    /// If GC is disabled in the config, this creates a non-functional state
    /// with no background thread.
    ///
    /// # Arguments
    /// * `db_path` - Path to the database
    /// * `config` - GC configuration
    /// * `timestamp_oracle` - Shared timestamp oracle for watermark calculation
    ///
    /// # Returns
    /// An Arc<Mutex<BackgroundGcState>> that can be shared across threads
    pub fn spawn(
        _db_path: std::path::PathBuf,
        config: GcConfig,
        timestamp_oracle: Arc<TimestampOracle>,
    ) -> Result<Arc<std::sync::Mutex<Self>>> {
        use std::sync::mpsc;
        use std::thread;

        if !config.enabled {
            let (sender, _receiver) = mpsc::channel();
            return Ok(Arc::new(std::sync::Mutex::new(BackgroundGcState {
                sender,
                _gc_thread: None,
            })));
        }

        let (sender, receiver) = mpsc::channel();

        let gc_thread = thread::spawn(move || {
            Self::gc_loop(receiver, config, timestamp_oracle);
        });

        Ok(Arc::new(std::sync::Mutex::new(BackgroundGcState {
            sender,
            _gc_thread: Some(gc_thread),
        })))
    }

    /// Triggers an immediate GC run
    pub fn trigger_gc(&self) -> Result<()> {
        self.sender
            .send(GcMessage::Trigger)
            .map_err(|_| GraphError::Corruption("gc channel closed".into()))
    }

    /// Shuts down the background GC thread gracefully
    pub fn shutdown(&self) -> Result<()> {
        self.sender
            .send(GcMessage::Shutdown)
            .map_err(|_| GraphError::Corruption("gc channel closed".into()))
    }

    /// Main GC loop running in the background thread
    ///
    /// This loop waits for either:
    /// - A timeout (interval_secs) to trigger periodic GC
    /// - An explicit Trigger message to run GC immediately
    /// - A Shutdown message to stop the thread
    fn gc_loop(
        receiver: std::sync::mpsc::Receiver<GcMessage>,
        config: GcConfig,
        timestamp_oracle: Arc<TimestampOracle>,
    ) {
        use std::sync::mpsc::RecvTimeoutError;
        use std::time::Duration;

        let interval = Duration::from_secs(config.interval_secs.unwrap_or(60));

        loop {
            match receiver.recv_timeout(interval) {
                Ok(GcMessage::Trigger) => {
                    let _ = Self::perform_gc(&config, &timestamp_oracle);
                }
                Ok(GcMessage::Shutdown) => {
                    info!("Background GC thread shutting down");
                    break;
                }
                Err(RecvTimeoutError::Timeout) => {
                    // Periodic GC run
                    let _ = Self::perform_gc(&config, &timestamp_oracle);
                }
                Err(RecvTimeoutError::Disconnected) => {
                    warn!("GC channel disconnected, shutting down");
                    break;
                }
            }
        }
    }

    /// Performs a single GC run
    ///
    /// This is a placeholder for now. In Task 16, we'll implement the actual
    /// version chain compaction logic here.
    fn perform_gc(_config: &GcConfig, timestamp_oracle: &Arc<TimestampOracle>) -> Result<()> {
        let gc_watermark = timestamp_oracle.gc_eligible_before();

        debug!(
            gc_watermark = gc_watermark,
            "Background GC run (compaction not yet implemented)"
        );

        // TODO (Task 16): Implement actual version chain compaction
        // For now, we just log that GC would run here
        
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::version::VersionMetadata;

    #[test]
    fn test_is_version_reclaimable_basic() {
        let gc = GarbageCollector::new();
        let gc_watermark = 100;

        // Version 1 (newest): commit_ts=150, position=1 -> NOT reclaimable (too new)
        let metadata = VersionMetadata::new(1, 150, None, false);
        assert!(!gc.is_version_reclaimable(&metadata, 1, gc_watermark));

        // Version 2: commit_ts=50, position=2 -> reclaimable (old enough and > min)
        let metadata = VersionMetadata::new(2, 50, None, false);
        assert!(gc.is_version_reclaimable(&metadata, 2, gc_watermark));
    }

    #[test]
    fn test_is_version_reclaimable_preserves_minimum() {
        let gc = GarbageCollector::new(); // min_versions = 1
        let gc_watermark = 100;

        // Position 1 (head): Never reclaimable (within minimum)
        let metadata = VersionMetadata::new(1, 50, None, false);
        assert!(!gc.is_version_reclaimable(&metadata, 1, gc_watermark));

        // Position 2: Reclaimable if old enough
        let metadata = VersionMetadata::new(2, 50, None, false);
        assert!(gc.is_version_reclaimable(&metadata, 2, gc_watermark));
    }

    #[test]
    fn test_is_version_reclaimable_uncommitted() {
        let gc = GarbageCollector::new();
        let gc_watermark = 100;

        // Uncommitted version (commit_ts=0): Never reclaimable
        let metadata = VersionMetadata::new(1, 0, None, false);
        assert!(!gc.is_version_reclaimable(&metadata, 2, gc_watermark));
    }

    #[test]
    fn test_is_version_reclaimable_custom_min_versions() {
        let gc = GarbageCollector::with_settings(3, 1000).unwrap();
        let gc_watermark = 100;

        // Position 1, 2, 3: Not reclaimable (within minimum of 3)
        let metadata = VersionMetadata::new(1, 50, None, false);
        assert!(!gc.is_version_reclaimable(&metadata, 1, gc_watermark));
        assert!(!gc.is_version_reclaimable(&metadata, 2, gc_watermark));
        assert!(!gc.is_version_reclaimable(&metadata, 3, gc_watermark));

        // Position 4: Reclaimable (beyond minimum)
        assert!(gc.is_version_reclaimable(&metadata, 4, gc_watermark));
    }

    #[test]
    fn test_gc_collector_creation() {
        let gc = GarbageCollector::new();
        assert_eq!(gc.min_versions_per_record, 1);
        assert_eq!(gc.scan_batch_size, 1000);
    }

    #[test]
    fn test_gc_collector_custom_settings() {
        let gc = GarbageCollector::with_settings(2, 500).unwrap();
        assert_eq!(gc.min_versions_per_record, 2);
        assert_eq!(gc.scan_batch_size, 500);
    }

    #[test]
    fn test_gc_collector_invalid_min_versions() {
        let result = GarbageCollector::with_settings(0, 500);
        assert!(result.is_err());
    }

    #[test]
    fn test_gc_stats_default() {
        let stats = GcStats::new();
        assert_eq!(stats.chains_scanned, 0);
        assert_eq!(stats.versions_examined, 0);
        assert_eq!(stats.versions_reclaimed, 0);
        assert!(!stats.any_reclaimed());
    }

    #[test]
    fn test_gc_stats_any_reclaimed() {
        let mut stats = GcStats::new();
        assert!(!stats.any_reclaimed());

        stats.versions_reclaimed = 5;
        assert!(stats.any_reclaimed());
    }
}
