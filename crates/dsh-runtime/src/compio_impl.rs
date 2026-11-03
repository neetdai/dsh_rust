//! Compio backend.
//!
//! Compio's runtime is thread-local (`!Send`/`!Sync`) and its `JoinHandle`
//! cancels the task when dropped, so every handle is detached immediately.
//! The kernel must therefore call [`Runtime::block_on`] (or spawn) on the
//! thread that owns this runtime.

use std::time::Instant;

use crate::{LocalBoxFuture, Runtime};

/// A thread-local compio runtime.
pub struct CompioRuntime {
    rt: compio::runtime::Runtime,
}

impl CompioRuntime {
    /// Build a compio runtime with default configuration.
    pub fn new() -> std::io::Result<Self> {
        Ok(Self {
            rt: compio::runtime::Runtime::new()?,
        })
    }
}

impl Runtime for CompioRuntime {
    fn name(&self) -> &'static str {
        "compio"
    }

    fn spawn_raw(&self, fut: LocalBoxFuture<'static, ()>) {
        // Dropping a compio JoinHandle cancels the task; detach it instead.
        self.rt.spawn(fut).detach();
    }

    fn spawn_blocking_raw(&self, f: Box<dyn FnOnce() + Send + 'static>) {
        self.rt.spawn_blocking(move || f()).detach();
    }

    fn sleep_until(&self, at: Instant) -> LocalBoxFuture<'static, ()> {
        Box::pin(async move {
            compio::time::sleep_until(at).await;
        })
    }

    fn block_on(&self, fut: LocalBoxFuture<'static, ()>) {
        self.rt.block_on(fut);
    }
}
