use crate::primitives::pager::PagerOptions;
use crate::storage::VersionCodecKind;

/// Common options used when opening a database for administrative commands.
#[derive(Clone, Debug)]
pub struct AdminOpenOptions {
    /// Pager configuration options.
    pub pager: PagerOptions,
    /// Default value for distinct neighbors in graph traversal queries.
    pub distinct_neighbors_default: bool,
    /// Whether to create the database if it doesn't exist.
    pub create_if_missing: bool,
    /// Whether to embed newest historical version inline on page heads.
    pub inline_history: bool,
    /// Maximum inline history payload size in bytes.
    pub inline_history_max_bytes: usize,
    /// Compression strategy applied to historical version payloads.
    pub version_codec: VersionCodecKind,
    /// Minimum payload size before compression is attempted.
    pub version_codec_min_payload_len: usize,
    /// Minimum bytes that must be saved for compression to be kept.
    pub version_codec_min_savings_bytes: usize,
    /// Maximum cached snapshots to retain for reuse.
    pub snapshot_pool_size: usize,
    /// Maximum age in milliseconds for cached snapshots.
    pub snapshot_pool_max_age_ms: u64,
}

impl Default for AdminOpenOptions {
    fn default() -> Self {
        Self {
            pager: PagerOptions::default(),
            distinct_neighbors_default: false,
            create_if_missing: false,
            inline_history: true,
            inline_history_max_bytes: 1024,
            version_codec: VersionCodecKind::None,
            version_codec_min_payload_len: 64,
            version_codec_min_savings_bytes: 8,
            snapshot_pool_size: 0,
            snapshot_pool_max_age_ms: 200,
        }
    }
}
