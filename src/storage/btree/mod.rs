#![forbid(unsafe_code)]

//! Lightweight building blocks for the Stage 4 B+ tree.

/// B+ tree page format and operations.
pub mod page;

/// Encoding and decoding utilities for keys and values.
pub mod codecs;
mod cursor;
mod key_cursor;
mod stats;
mod tree;

pub use cursor::Cursor;
pub(crate) use key_cursor::KeyCursor;
pub use stats::BTreeStats;
pub use tree::{BTree, BTreeOptions, KeyCodec, ValCodec};

#[cfg(test)]
mod tests;
