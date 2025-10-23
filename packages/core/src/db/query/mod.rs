//! High-level query APIs for navigating graph hierarchies and subgraphs.
//!
//! This module groups facilities for ancestor/descendant lookups,
//! subgraph extraction, analytical helpers, and the fluent query builder.

pub mod analytics;
pub mod builder;
pub mod hierarchy;
pub mod pattern;
pub mod subgraph;
