//! Sombra monolithic crate stub created during migration.
//! Actual implementations will be migrated from the multi-crate workspace.

#![warn(missing_docs)]
#![allow(
    clippy::arc_with_non_send_sync,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::match_like_matches_macro,
    clippy::redundant_guards,
    clippy::manual_clamp,
    clippy::question_mark,
    clippy::should_implement_trait,
    clippy::module_inception,
    clippy::len_without_is_empty,
    clippy::ptr_arg,
    clippy::field_reassign_with_default,
    clippy::only_used_in_recursion,
    clippy::collapsible_match,
    clippy::doc_overindented_list_items,
    clippy::redundant_locals
)]

pub mod admin;
pub mod cli;
#[path = "../packages/api-server/mod.rs"]
pub mod dashboard;
pub mod ffi;
pub mod primitives;
pub mod query;
pub mod storage;
pub mod types;
