use crate::error::{GraphError, Result};
use tracing_subscriber::{fmt, EnvFilter};

pub fn init_logging(level: &str) -> Result<()> {
    fmt()
        .with_env_filter(
            EnvFilter::try_new(level)
                .map_err(|e| GraphError::InvalidArgument(format!("Invalid log level: {e}")))?,
        )
        .with_target(true)
        .with_thread_ids(true)
        .try_init()
        .map_err(|_| GraphError::InvalidArgument("Logging already initialized".into()))
}
