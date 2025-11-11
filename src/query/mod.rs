#![forbid(unsafe_code)]

//! Query planning and execution engine (Stage 8).
//!
//! This module provides the core infrastructure for query planning and execution,
//! including AST representation, logical planning, physical execution, and profiling.

/// Abstract syntax tree (AST) for graph queries.
///
/// Defines the high-level query structure with match clauses, edges, and predicates.
pub mod ast;

/// Query builder for programmatic query construction.
///
/// Provides a fluent API for building complex queries without writing raw AST.
pub mod builder;

/// Query normalization utilities (Phase 3).
pub mod analyze;

/// Query error types surfaced across planner/analyzer boundaries.
pub mod errors;

/// Canonical scalar value representation shared across the query stack.
pub mod value;

/// Query execution engine.
///
/// Executes physical plans and streams result rows back to clients.
pub mod executor;

/// Logical query plan representation.
///
/// Intermediate representation for query optimization and analysis.
pub mod logical;

/// Query metadata and catalog information.
///
/// Manages schema information and metadata required for planning and execution.
pub mod metadata;

/// Physical query plan representation.
///
/// Executable plan containing specific operators and their configurations.
pub mod physical;

/// Query plan generation and optimization.
///
/// Converts AST to optimized physical plans for execution.
pub mod planner;

/// Performance profiling for query operations.
///
/// Collects timing and count statistics to identify performance bottlenecks.
pub mod profile;

pub use builder::QueryBuilder;
pub use value::Value;

/// Execution plan output with explanation capabilities.
pub use planner::{PlanExplain, PlannerOutput};
