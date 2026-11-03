//! Tokio backend.
//!
//! Kernels are pinned to a [`tokio::task::LocalSet`] because the shared
//! [`Runtime`] trait uses non-`Send` futures. `LocalSet` keeps tasks alive
//! across `block_on` calls until the runtime is dropped.

use std::time::Instant;

use crate::{LocalBoxFuture, Runtime};

/// A Tokio runtime with its own `LocalSet`.
pub struct TokioRuntime {
    rt: tokio::runtime::Runtime,
    local: tokio::task::LocalSet,
}

impl TokioRuntime {
    /// Build a multi-threaded Tokio runtime with all drivers enabled.
    pub fn new() -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            rt,
            local: tokio::task::LocalSet::new(),
        })
    }

    /// Build a current-thread Tokio runtime.
    pub fn current_thread() -> std::io::Result<Self> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        Ok(Self {
            rt,
            local: tokio::task::LocalSet::new(),
        })
    }

    /// Access the underlying Tokio handle (for Send-only integrations).
    pub fn handle(&self) -> tokio::runtime::Handle {
        self.rt.handle().clone()
    }
}

impl Runtime for TokioRuntime {
    fn name(&self) -> &'static str {
        "tokio"
    }

    fn spawn_raw(&self, fut: LocalBoxFuture<'static, ()>) {
        self.local.spawn_local(fut);
    }

    fn spawn_blocking_raw(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        // Dropping a Tokio JoinHandle detaches the blocking task.
        let handle = self.rt.handle().spawn_blocking(f);
        drop(handle);
    }

    fn sleep_until(&self, at: Instant) -> LocalBoxFuture<'static, ()> {
        Box::pin(async move {
            tokio::time::sleep_until(tokio::time::Instant::from_std(at)).await;
        })
    }

    fn block_on(&self, fut: LocalBoxFuture<'static, ()>) {
        self.local.block_on(&self.rt, fut);
    }
}
