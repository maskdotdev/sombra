#![forbid(unsafe_code)]

use crate::primitives::pager::{PageStore, PagerStats};
use crate::types::{Result, SombraError};

/// Canonical error message emitted when a legacy prefix-compressed leaf layout is encountered.
pub const LEGACY_LEAF_LAYOUT_ERR: &str =
    "plain leaf record layout required (prefix-compressed leaves detected; rebuild or re-import your data)";

/// Core database instance that manages page storage.
pub struct Db<P: PageStore> {
    pager: P,
}

impl<P: PageStore> Db<P> {
    /// Creates a new database instance with the given page store.
    pub fn new(pager: P) -> Self {
        Self { pager }
    }

    /// Returns a reference to the underlying page store.
    pub fn pager(&self) -> &P {
        &self.pager
    }

    /// Returns statistics about page storage operations.
    pub fn stats(&self) -> PagerStats {
        PagerStats::default()
    }

    /// Returns a typed error when a legacy prefix-compressed leaf layout is detected.
    pub fn bail_legacy_leaf_layout<T>(&self) -> Result<T> {
        Err(SombraError::Invalid(LEGACY_LEAF_LAYOUT_ERR))
    }
}
