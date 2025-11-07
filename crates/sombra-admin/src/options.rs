use sombra_pager::PagerOptions;

/// Common options used when opening a database for administrative commands.
#[derive(Clone, Debug)]
pub struct AdminOpenOptions {
    pub pager: PagerOptions,
    pub distinct_neighbors_default: bool,
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
