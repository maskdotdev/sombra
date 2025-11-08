use std::path::{Path, PathBuf};

use crate::types::SombraError;
use thiserror::Error;

/// Error type for administrative operations.
#[derive(Debug, Error)]
pub enum AdminError {
    /// Database file not found at the specified path.
    #[error("database not found: {0}")]
    MissingDatabase(PathBuf),
    /// Custom error message.
    #[error("{0}")]
    Message(String),
    /// Core database error.
    #[error(transparent)]
    Core(#[from] SombraError),
    /// I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

/// Result type alias for administrative operations.
pub type Result<T> = std::result::Result<T, AdminError>;

impl AdminError {
    pub(crate) fn missing_database(path: impl AsRef<Path>) -> Self {
        AdminError::MissingDatabase(path.as_ref().to_path_buf())
    }
}
