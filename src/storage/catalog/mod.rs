#![forbid(unsafe_code)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::primitives::bytes::ord;
use crate::primitives::pager::{PageStore, ReadGuard, WriteGuard};
use crate::storage::btree::{BTree, BTreeOptions, ValCodec};
use crate::storage::vstore::VStore;
use crate::types::{PageId, Result, SombraError, StrId, VRef};
use tracing::trace;

/// Configuration options for the dictionary (string catalog).
///
/// The dictionary manages the mapping between strings and their integer identifiers,
/// optimizing storage by inlining short strings and storing longer strings in the
/// variable-length store (VStore).
#[derive(Clone, Debug)]
pub struct DictOptions {
    /// Maximum string length (in bytes) to store inline. Strings longer than this
    /// will be stored in the VStore and referenced via [`VRef`].
    pub inline_limit: usize,
    
    /// Whether to verify checksums when reading data from disk.
    pub checksum_verify_on_read: bool,
}

impl Default for DictOptions {
    fn default() -> Self {
        Self {
            inline_limit: 60,
            checksum_verify_on_read: true,
        }
    }
}

/// Represents how a string is stored in the dictionary.
///
/// Strings can be stored in two ways:
/// - Short strings are stored inline within the BTree for efficiency.
/// - Long strings are stored in the variable-length store (VStore) and referenced via [`VRef`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StrEntry {
    /// A string stored inline as a byte vector. Used for strings up to the `inline_limit`.
    Inline(Vec<u8>),
    
    /// A reference to a string stored in the VStore. Used for strings exceeding the `inline_limit`.
    VRef(VRef),
}

/// Thread-safe metrics collector for dictionary operations.
///
/// Tracks statistics about string interning and resolution operations,
/// including cache hit rates and operation counts.
#[derive(Default)]
pub struct DictMetrics {
    intern_calls: AtomicU64,
    intern_hits: AtomicU64,
    intern_misses: AtomicU64,
    resolve_calls: AtomicU64,
    resolve_misses: AtomicU64,
}

/// A point-in-time snapshot of dictionary metrics.
///
/// This structure provides a consistent view of dictionary performance metrics
/// at a specific moment, useful for monitoring and debugging.
#[derive(Clone, Copy, Debug, Default)]
pub struct DictMetricsSnapshot {
    /// Total number of intern operations called.
    pub intern_calls: u64,
    
    /// Number of intern operations that found an existing entry.
    pub intern_hits: u64,
    
    /// Number of intern operations that had to create a new entry.
    pub intern_misses: u64,
    
    /// Total number of resolve operations called.
    pub resolve_calls: u64,
    
    /// Number of resolve operations that failed to find an entry.
    pub resolve_misses: u64,
}

impl DictMetricsSnapshot {
    /// Calculates the cache hit rate for intern operations.
    ///
    /// # Returns
    /// A value between 0.0 and 1.0 representing the percentage of intern operations
    /// that found an existing entry. Returns 0.0 if no intern operations have been performed.
    pub fn intern_hit_rate(&self) -> f64 {
        if self.intern_calls == 0 {
            return 0.0;
        }
        self.intern_hits as f64 / self.intern_calls as f64
    }
}

impl DictMetrics {
    /// Creates a snapshot of the current metrics.
    ///
    /// # Returns
    /// A [`DictMetricsSnapshot`] containing the current values of all counters.
    pub fn snapshot(&self) -> DictMetricsSnapshot {
        DictMetricsSnapshot {
            intern_calls: self.intern_calls.load(Ordering::Relaxed),
            intern_hits: self.intern_hits.load(Ordering::Relaxed),
            intern_misses: self.intern_misses.load(Ordering::Relaxed),
            resolve_calls: self.resolve_calls.load(Ordering::Relaxed),
            resolve_misses: self.resolve_misses.load(Ordering::Relaxed),
        }
    }

    fn inc(&self, counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    fn intern_call(&self) {
        self.inc(&self.intern_calls);
    }

    fn intern_hit(&self) {
        self.inc(&self.intern_hits);
    }

    fn intern_miss(&self) {
        self.inc(&self.intern_misses);
    }

    fn resolve_call(&self) {
        self.inc(&self.resolve_calls);
    }

    fn resolve_miss(&self) {
        self.inc(&self.resolve_misses);
    }
}

impl StrEntry {
    fn from_string(
        opts: &DictOptions,
        vstore: &VStore,
        tx: &mut WriteGuard<'_>,
        s: &str,
    ) -> Result<Self> {
        if s.len() <= opts.inline_limit {
            Ok(StrEntry::Inline(s.as_bytes().to_vec()))
        } else {
            let vref = vstore.write(tx, s.as_bytes())?;
            Ok(StrEntry::VRef(vref))
        }
    }
}

impl ValCodec for StrEntry {
    fn encode_val(value: &Self, out: &mut Vec<u8>) {
        match value {
            StrEntry::Inline(bytes) => {
                debug_assert!(
                    bytes.len() <= u8::MAX as usize,
                    "inline string exceeds 255 bytes"
                );
                out.push(0);
                out.push(bytes.len() as u8);
                out.extend_from_slice(bytes);
            }
            StrEntry::VRef(vref) => {
                out.push(1);
                out.extend_from_slice(&vref.start_page.0.to_be_bytes());
                out.extend_from_slice(&vref.n_pages.to_be_bytes());
                out.extend_from_slice(&vref.len.to_be_bytes());
                out.extend_from_slice(&vref.checksum.to_be_bytes());
            }
        }
    }

    fn decode_val(src: &[u8]) -> Result<Self> {
        if src.is_empty() {
            return Err(SombraError::Corruption("str entry payload truncated"));
        }
        match src[0] {
            0 => {
                if src.len() < 2 {
                    return Err(SombraError::Corruption("inline string missing length"));
                }
                let len = src[1] as usize;
                if src.len() < 2 + len {
                    return Err(SombraError::Corruption("inline string truncated"));
                }
                let data = src[2..2 + len].to_vec();
                Ok(StrEntry::Inline(data))
            }
            1 => {
                if src.len() != 1 + 8 + 4 + 4 + 4 {
                    return Err(SombraError::Corruption("vref payload length mismatch"));
                }
                let mut buf8 = [0u8; 8];
                buf8.copy_from_slice(&src[1..9]);
                let start_page = PageId(u64::from_be_bytes(buf8));
                let mut buf4 = [0u8; 4];
                buf4.copy_from_slice(&src[9..13]);
                let n_pages = u32::from_be_bytes(buf4);
                buf4.copy_from_slice(&src[13..17]);
                let len = u32::from_be_bytes(buf4);
                buf4.copy_from_slice(&src[17..21]);
                let checksum = u32::from_be_bytes(buf4);
                Ok(StrEntry::VRef(VRef {
                    start_page,
                    n_pages,
                    len,
                    checksum,
                }))
            }
            _ => Err(SombraError::Corruption("unknown string entry tag")),
        }
    }
}

/// String dictionary providing bidirectional mapping between strings and unique identifiers.
///
/// The [`Dict`] manages a persistent mapping between strings and compact integer identifiers
/// ([`StrId`]). It uses two B-Trees for efficient lookups in both directions:
/// - String-to-ID mapping for interning strings
/// - ID-to-String mapping for resolving identifiers
///
/// Short strings (up to `inline_limit` bytes) are stored inline in the B-Tree,
/// while longer strings are stored in the variable-length store (VStore) to avoid
/// excessive B-Tree node bloat.
pub struct Dict {
    store: Arc<dyn PageStore>,
    s2i: BTree<Vec<u8>, u64>,
    i2s: BTree<u64, StrEntry>,
    vstore: VStore,
    opts: DictOptions,
    metrics: Arc<DictMetrics>,
}

impl Dict {
    /// Opens or creates a dictionary using the provided page store and options.
    ///
    /// This method initializes the dictionary's B-Trees and VStore, loading existing
    /// data from the page store if available or creating new structures if needed.
    ///
    /// # Parameters
    /// * `store` - The page store to use for persistent storage.
    /// * `opts` - Configuration options for the dictionary.
    ///
    /// # Returns
    /// A new [`Dict`] instance on success, or an error if initialization fails.
    pub fn open(store: Arc<dyn PageStore>, opts: DictOptions) -> Result<Self> {
        let vstore = VStore::open(Arc::clone(&store))?;
        let meta = store.meta()?;

        let mut s2i_opts = BTreeOptions::default();
        s2i_opts.checksum_verify_on_read = opts.checksum_verify_on_read;
        s2i_opts.root_page = (meta.dict_str_to_id_root.0 != 0).then_some(meta.dict_str_to_id_root);
        let s2i = BTree::<Vec<u8>, u64>::open_or_create(&store, s2i_opts)?;

        let mut i2s_opts = BTreeOptions::default();
        i2s_opts.checksum_verify_on_read = opts.checksum_verify_on_read;
        i2s_opts.root_page = (meta.dict_id_to_str_root.0 != 0).then_some(meta.dict_id_to_str_root);
        let i2s = BTree::<u64, StrEntry>::open_or_create(&store, i2s_opts)?;

        let dict = Dict {
            store,
            s2i,
            i2s,
            vstore,
            opts,
            metrics: Arc::new(DictMetrics::default()),
        };
        dict.initialize_meta(&meta)?;
        Ok(dict)
    }

    /// Returns a shared reference to the dictionary's metrics collector.
    ///
    /// # Returns
    /// An [`Arc`] to the [`DictMetrics`] instance used by this dictionary.
    pub fn metrics(&self) -> Arc<DictMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Creates a snapshot of the current dictionary metrics.
    ///
    /// # Returns
    /// A [`DictMetricsSnapshot`] containing the current metric values.
    pub fn metrics_snapshot(&self) -> DictMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Looks up the identifier for the provided string without mutating the dictionary.
    ///
    /// This is a read-only operation that returns the existing identifier if the string
    /// is already in the dictionary, or `None` if it hasn't been interned yet.
    ///
    /// # Parameters
    /// * `s` - The string to look up.
    ///
    /// # Returns
    /// * `Ok(Some(id))` - The string identifier if found.
    /// * `Ok(None)` - The string is not in the dictionary.
    /// * `Err(_)` - An error occurred during the lookup.
    pub fn lookup(&self, s: &str) -> Result<Option<StrId>> {
        let key = encode_string_key(s);
        let read = self.store.begin_read()?;
        let raw = self.s2i.get(&read, &key)?;
        drop(read);
        if let Some(id) = raw {
            if id > u32::MAX as u64 {
                return Err(SombraError::Corruption("string id exceeds u32 range"));
            }
            Ok(Some(StrId(id as u32)))
        } else {
            Ok(None)
        }
    }

    /// Interns a string, returning its unique identifier.
    ///
    /// If the string already exists in the dictionary, returns its existing identifier.
    /// Otherwise, creates a new entry and assigns a new unique identifier.
    ///
    /// # Parameters
    /// * `tx` - The write transaction to use for modifications.
    /// * `s` - The string to intern.
    ///
    /// # Returns
    /// The unique identifier for the string, either existing or newly created.
    ///
    /// # Errors
    /// Returns an error if:
    /// * The dictionary runs out of available identifiers (u32::MAX reached).
    /// * A storage operation fails.
    pub fn intern(&self, tx: &mut WriteGuard<'_>, s: &str) -> Result<StrId> {
        let key = encode_string_key(s);
        self.metrics.intern_call();
        if let Some(existing) = self.s2i.get_with_write(tx, &key)? {
            if existing > u32::MAX as u64 {
                return Err(SombraError::Corruption("string id exceeds u32 range"));
            }
            self.metrics.intern_hit();
            trace!(len = s.len(), id = existing, "dict.intern.hit");
            return Ok(StrId(existing as u32));
        }
        self.metrics.intern_miss();
        let entry = StrEntry::from_string(&self.opts, &self.vstore, tx, s)?;
        let id = self.reserve_str_id(tx)?;
        let raw = u64::from(id.0);
        self.s2i.put(tx, &key, &raw)?;
        self.i2s.put(tx, &raw, &entry)?;
        self.sync_roots(tx)?;
        trace!(len = s.len(), id = id.0, "dict.intern.insert");
        Ok(id)
    }

    /// Resolves a string identifier back to its original string.
    ///
    /// # Parameters
    /// * `tx` - The read transaction to use for the lookup.
    /// * `id` - The string identifier to resolve.
    ///
    /// # Returns
    /// The original string associated with the identifier.
    ///
    /// # Errors
    /// Returns an error if:
    /// * The identifier is not found in the dictionary ([`SombraError::NotFound`]).
    /// * The stored data is not valid UTF-8 ([`SombraError::Corruption`]).
    /// * A storage operation fails.
    pub fn resolve(&self, tx: &ReadGuard, id: StrId) -> Result<String> {
        self.metrics.resolve_call();
        match self.i2s.get(tx, &u64::from(id.0))? {
            Some(StrEntry::Inline(bytes)) => {
                trace!(id = id.0, len = bytes.len(), "dict.resolve.inline");
                String::from_utf8(bytes)
                    .map_err(|_| SombraError::Corruption("dictionary entry not valid UTF-8"))
            }
            Some(StrEntry::VRef(vref)) => {
                let bytes = self.vstore.read(tx, vref)?;
                trace!(id = id.0, len = bytes.len(), "dict.resolve.vref");
                String::from_utf8(bytes)
                    .map_err(|_| SombraError::Corruption("dictionary entry not valid UTF-8"))
            }
            None => {
                self.metrics.resolve_miss();
                trace!(id = id.0, "dict.resolve.miss");
                Err(SombraError::NotFound)
            }
        }
    }

    fn reserve_str_id(&self, tx: &mut WriteGuard<'_>) -> Result<StrId> {
        let mut allocated: Option<u32> = None;
        tx.update_meta(|meta| {
            let next = meta.dict_next_str_id.max(1);
            if next == u32::MAX {
                return;
            }
            meta.dict_next_str_id = next + 1;
            allocated = Some(next);
        })?;
        let raw = allocated.ok_or(SombraError::Invalid("string id overflow"))?;
        Ok(StrId(raw))
    }

    fn sync_roots(&self, tx: &mut WriteGuard<'_>) -> Result<()> {
        let s2i_root = self.s2i.root_page();
        let i2s_root = self.i2s.root_page();
        let meta = self.store.meta()?;
        if meta.dict_str_to_id_root == s2i_root && meta.dict_id_to_str_root == i2s_root {
            return Ok(());
        }
        tx.update_meta(|meta| {
            meta.dict_str_to_id_root = s2i_root;
            meta.dict_id_to_str_root = i2s_root;
        })
    }

    fn initialize_meta(&self, meta: &crate::primitives::pager::Meta) -> Result<()> {
        let s2i_root = self.s2i.root_page();
        let i2s_root = self.i2s.root_page();
        let needs_update = meta.dict_str_to_id_root != s2i_root
            || meta.dict_id_to_str_root != i2s_root
            || meta.dict_next_str_id == 0;
        if !needs_update {
            return Ok(());
        }
        let mut write = self.store.begin_write()?;
        write.update_meta(|meta| {
            meta.dict_str_to_id_root = s2i_root;
            meta.dict_id_to_str_root = i2s_root;
            if meta.dict_next_str_id == 0 {
                meta.dict_next_str_id = 1;
            }
        })?;
        let _ = self.store.commit(write)?;
        Ok(())
    }
}

fn encode_string_key(s: &str) -> Vec<u8> {
    let mut buf = Vec::with_capacity(4 + s.len());
    ord::put_str_key(&mut buf, s);
    buf
}
