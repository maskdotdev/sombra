use std::path::{Path, PathBuf};

use crate::types::SombraError;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AdminError {
    #[error("database not found: {0}")]
    MissingDatabase(PathBuf),
    #[error("{0}")]
    Message(String),
    #[error(transparent)]
    Core(#[from] SombraError),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

pub type Result<T> = std::result::Result<T, AdminError>;

impl AdminError {
    pub(crate) fn missing_database(path: impl AsRef<Path>) -> Self {
        AdminError::MissingDatabase(path.as_ref().to_path_buf())
    }
}
