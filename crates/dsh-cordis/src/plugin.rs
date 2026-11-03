use std::fmt;
use std::sync::Arc;

use dsh_runtime::LocalBoxFuture;

use crate::context::Context;
use crate::service::ServiceKey;

/// Plugin configuration. Config validation lands with the profile/loader crate;
/// the kernel treats it as an opaque JSON value.
pub type Config = serde_json::Value;

/// Static metadata a plugin declares.
#[derive(Debug, Clone)]
pub struct PluginMeta {
    /// Display name used by diagnostics.
    pub name: &'static str,
    /// Services this plugin requires before `apply` runs.
    pub inject: Vec<ServiceKey>,
}

impl PluginMeta {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            inject: Vec::new(),
        }
    }
}

/// Error raised by a plugin's `apply`.
#[derive(Clone)]
pub struct PluginError {
    message: Arc<str>,
}

impl PluginError {
    pub fn new(message: impl Into<Arc<str>>) -> Self {
        Self {
            message: message.into(),
        }
    }

    pub fn disposed() -> Self {
        Self::new("fiber was disposed before the plugin finished loading")
    }

    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Debug for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PluginError({:?})", self.message)
    }
}

impl fmt::Display for PluginError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for PluginError {}

/// A Cordis plugin: metadata plus one async `apply`.
pub trait Plugin: 'static {
    fn meta(&self) -> &PluginMeta;

    fn apply(
        &self,
        ctx: Context,
        config: Config,
    ) -> LocalBoxFuture<'static, Result<(), PluginError>>;
}

/// Closure-backed plugin, mirroring `ctx.plugin(function (ctx, config) {})`.
pub struct FnPlugin<F> {
    meta: PluginMeta,
    apply: F,
}

impl<F> FnPlugin<F>
where
    F: Fn(Context, Config) -> LocalBoxFuture<'static, Result<(), PluginError>> + 'static,
{
    pub fn new(name: &'static str, apply: F) -> Self {
        Self {
            meta: PluginMeta::new(name),
            apply,
        }
    }

    /// Declare required services.
    pub fn inject<I>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = ServiceKey>,
    {
        self.meta.inject.extend(keys);
        self
    }
}

impl<F> Plugin for FnPlugin<F>
where
    F: Fn(Context, Config) -> LocalBoxFuture<'static, Result<(), PluginError>> + 'static,
{
    fn meta(&self) -> &PluginMeta {
        &self.meta
    }

    fn apply(
        &self,
        ctx: Context,
        config: Config,
    ) -> LocalBoxFuture<'static, Result<(), PluginError>> {
        (self.apply)(ctx, config)
    }
}
