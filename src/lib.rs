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

use std::{backtrace::Backtrace, panic, sync::OnceLock};

pub mod admin;
pub mod cli;
#[path = "../packages/api-server/mod.rs"]
pub mod dashboard;
pub mod ffi;
pub mod primitives;
pub mod query;
pub mod storage;
pub mod types;

/// Installs a panic hook that logs the panic payload, location, thread name, and backtrace.
///
/// The hook is idempotent and safe to call from multiple entry points.
pub fn install_panic_hook() {
    static HOOK: OnceLock<()> = OnceLock::new();
    HOOK.get_or_init(|| {
        panic::set_hook(Box::new(|info| {
            let thread_name = std::thread::current()
                .name()
                .unwrap_or("unnamed thread")
                .to_string();
            let location = info
                .location()
                .map(|loc| format!("{}:{}:{}", loc.file(), loc.line(), loc.column()))
                .unwrap_or_else(|| "<unknown>".to_string());
            let message = info
                .payload()
                .downcast_ref::<&str>()
                .map(|s| (*s).to_string())
                .or_else(|| info.payload().downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "panic without message".to_string());
            let backtrace = Backtrace::force_capture();

            eprintln!("panic: {message}\nthread: {thread_name}\nlocation: {location}\nbacktrace:\n{backtrace}");
            tracing::error!(
                target: "panic",
                thread = thread_name,
                %location,
                %backtrace,
                "panic: {message}"
            );
        }));
    });
}
