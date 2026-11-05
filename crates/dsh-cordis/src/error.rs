use std::fmt;

use crate::plugin::PluginError;
use crate::service::ServiceKeyId;

/// Errors raised by the kernel.
#[derive(Debug, Clone)]
pub enum CordisError {
    /// The current fiber was already disposed.
    InactiveFiber,
    /// A service name is already provided in this scope.
    ServiceAlreadyProvided(&'static str),
    /// A required service is missing.
    MissingService(&'static str),
    /// A plugin config was rejected by its [`ConfigValidator`].
    ///
    /// The payload is the validator's error message. Validation happens
    /// before mounting, so no fiber is created for an invalid config.
    ///
    /// [`ConfigValidator`]: crate::ConfigValidator
    InvalidConfig(String),
    /// Plugin startup failed.
    PluginFailed(PluginError),
}

impl CordisError {
    pub(crate) fn missing(key: ServiceKeyId) -> Self {
        CordisError::MissingService(key.name())
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
            CordisError::InvalidConfig(message) => {
                write!(f, "invalid plugin config: {message}")
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
