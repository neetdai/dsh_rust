use std::fmt;
use std::sync::Arc;

use dsh_runtime::LocalBoxFuture;

use crate::context::Context;
use crate::service::ServiceKeyId;

/// Plugin configuration. The kernel keeps it as an opaque JSON value;
/// [`ConfigValidator`] hooks normalize and reject it before `apply` runs.
pub type Config = serde_json::Value;

/// Validates and normalizes a plugin config before its fiber is mounted.
///
/// The kernel calls [`ConfigValidator::validate`] in
/// [`crate::Context::plugin_with_config`] **before** creating a fiber, so an
/// invalid config never reaches `apply` and leaves no half-mounted plugin.
///
/// The trait is object-safe and `Send + Sync`, so validators can be shared
/// through an `Arc<dyn ConfigValidator>`. Closures of the shape
/// `Fn(&Config) -> Result<Config, String>` implement it automatically:
///
/// ```
/// use std::sync::Arc;
///
/// use dsh_cordis::{Config, FnPlugin, Plugin};
///
/// let validator = Arc::new(|raw: &Config| -> Result<Config, String> {
///     if raw.get("token").is_some() {
///         Ok(raw.clone())
///     } else {
///         Err("missing `token`".to_owned())
///     }
/// });
///
/// let plugin = FnPlugin::new("http", |_ctx, _config| Box::pin(async { Ok(()) }))
///     .validate_with(validator);
/// assert!(plugin.meta().validator.is_some());
/// ```
pub trait ConfigValidator: Send + Sync {
    /// Check `raw` and return the normalized config passed to `apply`.
    ///
    /// Returning `Err` aborts mounting with
    /// [`crate::CordisError::InvalidConfig`].
    fn validate(&self, raw: &Config) -> Result<Config, String>;
}

impl<F> ConfigValidator for F
where
    F: Fn(&Config) -> Result<Config, String> + Send + Sync,
{
    fn validate(&self, raw: &Config) -> Result<Config, String> {
        (self)(raw)
    }
}

impl<W> ConfigValidator for Arc<W>
where
    W: ConfigValidator + ?Sized,
{
    fn validate(&self, raw: &Config) -> Result<Config, String> {
        (**self).validate(raw)
    }
}

/// Static metadata a plugin declares.
#[derive(Clone)]
pub struct PluginMeta {
    /// Display name used by diagnostics.
    pub name: &'static str,
    /// Services this plugin requires before `apply` runs.
    pub inject: Vec<ServiceKeyId>,
    /// Optional config validation/normalization hook (see [`ConfigValidator`]).
    pub validator: Option<Arc<dyn ConfigValidator>>,
}

impl PluginMeta {
    pub fn new(name: &'static str) -> Self {
        Self {
            name,
            inject: Vec::new(),
            validator: None,
        }
    }

    /// Attach a config validator; it runs before every mount of this plugin.
    pub fn with_validator<V>(mut self, validator: V) -> Self
    where
        V: ConfigValidator + 'static,
    {
        self.validator = Some(Arc::new(validator));
        self
    }

    /// Run the configured validator (or pass `raw` through when there is
    /// none), returning the config that `apply` should receive.
    pub(crate) fn validate_config(&self, raw: Config) -> Result<Config, String> {
        match self.validator.as_deref() {
            Some(validator) => validator.validate(&raw),
            None => Ok(raw),
        }
    }
}

impl fmt::Debug for PluginMeta {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PluginMeta")
            .field("name", &self.name)
            .field("inject", &self.inject)
            .field("validator", &self.validator.is_some())
            .finish()
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
    ///
    /// Accepts typed keys or erased [`ServiceKeyId`]s, so both
    /// `inject([GREETER])` and `inject([GREETER.id()])` work.
    pub fn inject<I, K>(mut self, keys: I) -> Self
    where
        I: IntoIterator<Item = K>,
        K: Into<ServiceKeyId>,
    {
        self.meta.inject.extend(keys.into_iter().map(Into::into));
        self
    }

    /// Attach a config validator that runs before `apply`.
    ///
    /// Accepts a [`ConfigValidator`] or a plain
    /// `Fn(&Config) -> Result<Config, String>` closure.
    pub fn validate_with<V>(mut self, validator: V) -> Self
    where
        V: ConfigValidator + 'static,
    {
        self.meta = self.meta.with_validator(validator);
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
