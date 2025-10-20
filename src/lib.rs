//! # Sombra - High Performance Graph Database
//!
//! Sombra is a fast, embedded graph database written in Rust with ACID transactions,
//! WAL-based durability, and comprehensive indexing support.
//!
//! ## Quick Start
//!
//! ```rust
//! use sombra::{GraphDB, Node, Edge};
//!
//! // Open a database (creates if it doesn't exist)
//! let mut db = GraphDB::open("my_graph.db")?;
//!
//! // Start a transaction
//! let mut tx = db.begin_transaction()?;
//!
//! // Create nodes
//! let alice = tx.add_node(Node::new(1))?;
//! let bob = tx.add_node(Node::new(2))?;
//!
//! // Create an edge between nodes
//! let edge = Edge::new(1, alice, bob, "KNOWS");
//! tx.add_edge(edge)?;
//!
//! // Commit the transaction
//! tx.commit()?;
//! # Ok::<(), sombra::GraphError>(())
//! ```
//!
//! ## Features
//!
//! - **ACID Transactions**: Full atomicity, consistency, isolation, and durability
//! - **WAL-based Durability**: Write-Ahead Logging for crash recovery
//! - **Multiple Index Types**: B-tree indexes for fast lookups
//! - **Property Indexes**: Index nodes by property values
//! - **Language Bindings**: Python and Node.js support
//! - **Configurable Sync Modes**: Trade off performance vs. durability
//! - **Health Monitoring**: Built-in metrics and health checks
//!
//! ## Architecture
//!
//! Sombra uses a layered architecture:
//! - **Storage Layer**: Page-based storage with checksums
//! - **Pager Layer**: Page caching and WAL management
//! - **Database Layer**: Graph operations and transactions
//! - **API Layer**: Public interface and language bindings
//!
//! See the [architecture documentation](docs/architecture.md) for more details.

pub mod db;
pub mod error;
pub mod index;
pub mod logging;
pub mod model;
pub mod pager;
pub mod storage;

#[cfg(feature = "benchmarks")]
pub mod benchmark_suite;
#[cfg(feature = "napi")]
pub mod bindings;
#[cfg(feature = "benchmarks")]
pub mod data_generator;
#[cfg(feature = "benchmarks")]
pub mod performance_utils;
#[cfg(feature = "python")]
pub mod python;
#[cfg(feature = "benchmarks")]
pub mod sqlite_adapter;

// Re-export the main public API
pub use crate::db::{
    Config, GraphDB, IntegrityOptions, IntegrityReport, SyncMode, Transaction, TxId, TxState,
};
pub use crate::error::{GraphError, Result};
pub use crate::model::{Edge, EdgeId, Node, NodeId, PropertyValue};
