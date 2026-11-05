//! Cordis-compatible plugin kernel.
//!
//! The kernel mirrors the five Cordis concepts:
//!
//! * **services** — keyed values owned by a fiber and visible to its scope;
//! * **events** — five dispatch modes (`emit`, `parallel`, `serial`, `bail`,
//!   `waterfall`);
//! * **fiber** — one plugin instance with a lifecycle and reverse-order effects;
//! * **inject** — a plugin waits until all required services are active;
//! * **scope** — child contexts inherit services and can isolate a name.
//!
//! The kernel is single-threaded by design: it runs on the thread that owns
//! the [`dsh_runtime::Runtime`] (compio runtimes are thread-local). Use
//! [`dsh_runtime::Runtime::spawn_blocking`] for CPU-bound or blocking work.

mod context;
mod error;
mod event;
mod fiber;
mod plugin;
mod registry;
mod service;

pub use context::Context;
pub use error::CordisError;
pub use event::{
    BailEvent, EmitEvent, Event, EventBus, EventOptions, ListenerHandle, Next, ParallelEvent,
    SerialEvent, WaterfallEvent,
};
pub use fiber::{Disposer, EffectHandle, FiberHandle, FiberId, FiberStatus};
pub use plugin::{Config, ConfigValidator, FnPlugin, Plugin, PluginError, PluginMeta};
pub use service::{ServiceEntry, ServiceHandle, ServiceKey, ServiceKeyId};
