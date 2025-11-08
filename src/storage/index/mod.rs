#![forbid(unsafe_code)]

mod btree_postings;
mod cache;
mod catalog;
mod chunked;
mod epoch;
mod label;
mod store;
mod types;

pub use cache::{GraphIndexCache, GraphIndexCacheStats};
pub use catalog::IndexCatalog;
pub use epoch::{CatalogEpoch, DdlEpoch};
pub use label::{LabelScan, LABEL_SENTINEL_NODE};
pub use store::{IndexRoots, IndexStore};
pub use types::{
    collect_all, intersect_k, intersect_sorted, IndexDef, IndexKind, PostingStream, TypeTag,
    VecPostingStream,
};
