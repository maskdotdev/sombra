#![forbid(unsafe_code)]

//! Lightweight building blocks for the Stage 4 B+ tree.

pub mod page;

pub mod codecs;
mod cursor;
mod stats;
mod tree;

pub use cursor::Cursor;
pub use stats::BTreeStats;
pub use tree::{BTree, BTreeOptions, KeyCodec, ValCodec};

#[cfg(test)]
mod tests;
