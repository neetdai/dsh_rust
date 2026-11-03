//! Runtime abstraction shared by the Rust harness crates.
//!
//! The Cordis kernel is deliberately executor-agnostic: it only needs to
//! * spawn a local task,
//! * spawn a blocking task,
//! * sleep until an instant,
//! * run a future to completion.
//!
//! Two backends are provided behind Cargo features:
//!
//! * `tokio`  — multi-threaded Tokio, tasks pinned to a [`LocalSet`].
//! * `compio` — thread-local completion-based runtime.
//!
//! All futures are **non-`Send`** on purpose. `compio::runtime::Runtime` is
//! itself thread-local and `!Send`/`!Sync`, so requiring `Send` would exclude
//! the backend entirely. The kernel therefore runs on one engine thread, which
//! is also the natural execution model for a Cordis/QuickJS plugin host.
//! Blocking work escapes through [`Runtime::spawn_blocking`].

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context as TaskContext, Poll, Waker};
use std::time::{Duration, Instant};

use futures::channel::oneshot;

/// A boxed, single-threaded future.
pub type LocalBoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + 'a>>;

/// Why a [`TaskHandle`] could not produce a value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskError {
    /// The task was aborted before producing a value.
    Aborted,
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TaskError::Aborted => f.write_str("task aborted"),
        }
    }
}

impl std::error::Error for TaskError {}

/// Shared cancellation flag for one task.
#[derive(Default)]
struct CancelState {
    cancelled: AtomicBool,
    finished: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl CancelState {
    fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
        if let Some(waker) = self.waker.lock().expect("cancel waker lock").take() {
            waker.wake();
        }
    }

    fn is_finished(&self) -> bool {
        self.finished.load(Ordering::SeqCst) || self.cancelled.load(Ordering::SeqCst)
    }
}

struct Cancelled {
    state: Arc<CancelState>,
}

impl Future for Cancelled {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<()> {
        if self.state.cancelled.load(Ordering::SeqCst) {
            return Poll::Ready(());
        }
        *self.state.waker.lock().expect("cancel waker lock") = Some(cx.waker().clone());
        // Close the race between the first load and storing the waker.
        if self.state.cancelled.load(Ordering::SeqCst) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// Handle to one spawned task.
///
/// Dropping the handle detaches the task; call [`TaskHandle::abort`] to cancel
/// it cooperatively at its next await point.
pub struct TaskHandle<T> {
    rx: Option<oneshot::Receiver<T>>,
    state: Arc<CancelState>,
}

impl<T> TaskHandle<T> {
    fn from_parts(rx: oneshot::Receiver<T>, state: Arc<CancelState>) -> Self {
        Self {
            rx: Some(rx),
            state,
        }
    }

    /// Abort the task. Cancellation is cooperative: it takes effect at the
    /// next await point of the task future.
    pub fn abort(&self) {
        self.state.cancel();
    }

    /// A cheap, cloneable abort handle for this task.
    pub fn abort_handle(&self) -> AbortHandle {
        AbortHandle(self.state.clone())
    }

    /// Whether the task already completed or was aborted.
    pub fn is_finished(&self) -> bool {
        self.state.is_finished()
    }

    /// Wait for the task result.
    ///
    /// Returns [`TaskError::Aborted`] when the task was aborted or the task
    /// future was dropped before sending a value.
    pub async fn join(mut self) -> Result<T, TaskError> {
        let rx = self
            .rx
            .take()
            .expect("TaskHandle::join may only be called once");
        rx.await.map_err(|_| TaskError::Aborted)
    }
}

/// Cloneable abort handle kept by the owning fiber.
#[derive(Clone)]
pub struct AbortHandle(Arc<CancelState>);

impl AbortHandle {
    /// Abort the associated task.
    pub fn abort(&self) {
        self.0.cancel();
    }

    /// Whether the associated task already completed or was aborted.
    pub fn is_finished(&self) -> bool {
        self.0.is_finished()
    }
}

/// The executor surface the harness depends on.
///
/// Implementations are thread-confined handles; the trait deliberately does
/// not require `Send`/`Sync`.
pub trait Runtime {
    /// Backend name, for diagnostics.
    fn name(&self) -> &'static str;

    /// Spawn a task without the cancellation wrapper. Low-level hook used by
    /// the provided [`Runtime::spawn`] implementation.
    fn spawn_raw(&self, fut: LocalBoxFuture<'static, ()>);

    /// Spawn blocking work without the cancellation wrapper.
    fn spawn_blocking_raw(&self, f: Box<dyn FnOnce() + Send + 'static>);

    /// A future that resolves at `at`.
    fn sleep_until(&self, at: Instant) -> LocalBoxFuture<'static, ()>;

    /// Drive a future to completion on the current thread.
    fn block_on(&self, fut: LocalBoxFuture<'static, ()>);

    /// Spawn an abortable task.
    fn spawn(&self, fut: LocalBoxFuture<'static, ()>) -> TaskHandle<()> {
        let state = Arc::new(CancelState::default());
        let (tx, rx) = oneshot::channel();
        let cancelled = Cancelled {
            state: state.clone(),
        };
        let task_state = state.clone();
        let task: LocalBoxFuture<'static, ()> = Box::pin(async move {
            match futures::future::select(fut, cancelled).await {
                futures::future::Either::Left(((), _)) => {
                    task_state.finished.store(true, Ordering::SeqCst);
                    let _ = tx.send(());
                }
                futures::future::Either::Right(((), _)) => {
                    task_state.finished.store(true, Ordering::SeqCst);
                }
            }
        });
        self.spawn_raw(task);
        TaskHandle::from_parts(rx, state)
    }

    /// Spawn abortable blocking work.
    ///
    /// Blocking work cannot be interrupted once it started; [`TaskHandle::abort`]
    /// suppresses the join result and skips queued work.
    fn spawn_blocking(&self, f: Box<dyn FnOnce() + Send + 'static>) -> TaskHandle<()> {
        let state = Arc::new(CancelState::default());
        let (tx, rx) = oneshot::channel();
        let task_state = state.clone();
        self.spawn_blocking_raw(Box::new(move || {
            if task_state.cancelled.load(Ordering::SeqCst) {
                task_state.finished.store(true, Ordering::SeqCst);
                return;
            }
            f();
            task_state.finished.store(true, Ordering::SeqCst);
            let _ = tx.send(());
        }));
        TaskHandle::from_parts(rx, state)
    }
}

/// Convenience methods layered on top of [`Runtime`].
pub trait RuntimeExt: Runtime {
    /// Spawn a task that returns a typed value.
    fn spawn_typed<F, T>(&self, fut: F) -> TaskHandle<T>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        let (tx, rx) = oneshot::channel();
        let handle = self.spawn(Box::pin(async move {
            let value = fut.await;
            let _ = tx.send(value);
        }));
        TaskHandle::from_parts(rx, handle.state.clone())
    }

    /// Spawn blocking work that returns a typed value.
    fn spawn_blocking_typed<F, T>(&self, f: F) -> TaskHandle<T>
    where
        F: FnOnce() -> T + Send + 'static,
        T: Send + 'static,
    {
        let (tx, rx) = oneshot::channel();
        let handle = self.spawn_blocking(Box::new(move || {
            let value = f();
            let _ = tx.send(value);
        }));
        TaskHandle::from_parts(rx, handle.state.clone())
    }

    /// Sleep for `duration`.
    fn sleep(&self, duration: Duration) -> LocalBoxFuture<'static, ()> {
        self.sleep_until(Instant::now() + duration)
    }

    /// Run `fut` with a deadline.
    fn timeout(
        &self,
        after: Duration,
        fut: LocalBoxFuture<'static, ()>,
    ) -> LocalBoxFuture<'static, Result<(), Elapsed>> {
        let deadline = self.sleep_until(Instant::now() + after);
        Box::pin(async move {
            match futures::future::select(fut, deadline).await {
                futures::future::Either::Left(((), _)) => Ok(()),
                futures::future::Either::Right(((), _)) => Err(Elapsed { after }),
            }
        })
    }
}

impl<R: Runtime + ?Sized> RuntimeExt for R {}

/// Marker returned by [`RuntimeExt::timeout`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed {
    /// The configured timeout.
    pub after: Duration,
}

impl std::fmt::Display for Elapsed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "operation timed out after {:?}", self.after)
    }
}

impl std::error::Error for Elapsed {}

#[cfg(feature = "tokio")]
pub mod tokio_impl;
#[cfg(feature = "tokio")]
pub use tokio_impl::TokioRuntime;

#[cfg(feature = "compio")]
pub mod compio_impl;
#[cfg(feature = "compio")]
pub use compio_impl::CompioRuntime;
