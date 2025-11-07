#![forbid(unsafe_code)]

//! Stage 8 query planning and execution scaffolding.
//!
//! This crate currently exposes the high-level builder, logical plan
//! structures, and operator enums needed by the future planner. The
//! executor and planner wiring are placeholders that will be completed
//! in subsequent stages.

pub mod ast;
pub mod builder;
pub mod executor;
pub mod logical;
pub mod metadata;
pub mod physical;
pub mod planner;
pub mod profile;

pub use builder::QueryBuilder;
pub use planner::{PlanExplain, PlannerOutput};
