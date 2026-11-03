use std::fmt;

use crate::plugin::PluginError;
use crate::service::ServiceKey;

/// Errors raised by the kernel.
#[derive(Debug, Clone)]
pub enum CordisError {
    /// The current fiber was already disposed.
    InactiveFiber,
    /// A service name is already provided in this scope.
    ServiceAlreadyProvided(&'static str),
    /// A required service is missing.
    MissingService(&'static str),
    /// The stored service has a different Rust type than requested.
    ServiceTypeMismatch {
        /// Service name.
        key: &'static str,
        /// The type the caller asked for.
        expected: &'static str,
    },
    /// Plugin startup failed.
    PluginFailed(PluginError),
}

impl CordisError {
    pub(crate) fn missing(key: ServiceKey) -> Self {
        CordisError::MissingService(key.as_str())
    }
}

impl fmt::Display for CordisError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CordisError::InactiveFiber => f.write_str("cannot create effect on inactive context"),
            CordisError::ServiceAlreadyProvided(key) => {
                write!(f, "service `{key}` is already provided in this scope")
            }
            CordisError::MissingService(key) => write!(f, "service `{key}` is not available"),
            CordisError::ServiceTypeMismatch { key, expected } => {
                write!(f, "service `{key}` is not a `{expected}`")
            }
            CordisError::PluginFailed(err) => write!(f, "plugin failed: {err}"),
        }
    }
}

impl std::error::Error for CordisError {}

impl From<PluginError> for CordisError {
    fn from(value: PluginError) -> Self {
        CordisError::PluginFailed(value)
    }
}
