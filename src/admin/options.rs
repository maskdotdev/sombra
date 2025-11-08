use crate::primitives::pager::PagerOptions;

/// Common options used when opening a database for administrative commands.
#[derive(Clone, Debug)]
pub struct AdminOpenOptions {
    /// Pager configuration options.
    pub pager: PagerOptions,
    /// Default value for distinct neighbors in graph traversal queries.
    pub distinct_neighbors_default: bool,
    /// Whether to create the database if it doesn't exist.
    pub create_if_missing: bool,
}

impl Default for AdminOpenOptions {
    fn default() -> Self {
        Self {
            pager: PagerOptions::default(),
            distinct_neighbors_default: false,
            create_if_missing: false,
        }
    }
}
