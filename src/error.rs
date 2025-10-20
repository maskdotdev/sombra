use std::io;
use std::sync::{Mutex, MutexGuard};
use thiserror::Error;
use tracing::error;

pub type Result<T> = std::result::Result<T, GraphError>;

#[derive(Debug, Error)]
pub enum GraphError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    #[error("serialization error: {0}")]
    Serialization(String),
    #[error("corruption detected: {0}")]
    Corruption(String),
    #[error("{0} not found")]
    NotFound(&'static str),
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    #[error("unsupported feature: {0}")]
    UnsupportedFeature(&'static str),
}

pub fn acquire_lock<T>(mutex: &Mutex<T>) -> Result<MutexGuard<'_, T>> {
    mutex.lock().map_err(|_| {
        error!("Database lock poisoned - fatal error");
        GraphError::Corruption("Database lock poisoned - fatal error".into())
    })
}
