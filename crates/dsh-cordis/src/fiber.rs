use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, Ordering};

use dsh_runtime::AbortHandle;
use futures::channel::oneshot;

use crate::plugin::PluginError;
use crate::registry::Registry;
use crate::service::ServiceKeyId;

/// Unique fiber id inside one registry. The root fiber is `0`.
pub type FiberId = u64;

static NEXT_EFFECT_ID: AtomicU64 = AtomicU64::new(1);

/// Lifecycle state of one plugin instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FiberStatus {
    /// Waiting for injected services.
    Pending,
    /// `apply` is running.
    Loading,
    /// `apply` finished; services and listeners are visible.
    Active,
    /// `apply` failed; effects were rolled back.
    Failed,
    /// Unload is running.
    Disposing,
    /// Unloaded.
    Disposed,
}

/// A cleanup callback owned by a fiber.
pub type Disposer = Box<dyn FnOnce()>;

/// One registered effect and its disposers.
pub(crate) struct EffectEntry {
    pub(crate) id: u64,
    pub(crate) disposers: RefCell<Vec<Disposer>>,
    pub(crate) disposed: Cell<bool>,
}

impl EffectEntry {
    fn new(disposers: Vec<Disposer>) -> Self {
        Self {
            id: NEXT_EFFECT_ID.fetch_add(1, Ordering::Relaxed),
            disposers: RefCell::new(disposers),
            disposed: Cell::new(false),
        }
    }
}

/// Handle returned by `ctx.effect`.
pub struct EffectHandle {
    pub(crate) fiber: Rc<FiberInner>,
    pub(crate) entry: Rc<EffectEntry>,
}

impl EffectHandle {
    /// Run this effect's disposers (reverse registration order). Idempotent.
    pub fn dispose(&self) {
        dispose_entry(&self.fiber, &self.entry);
    }

    /// The effect's diagnostic id.
    pub fn id(&self) -> u64 {
        self.entry.id
    }
}

pub(crate) fn dispose_entry(fiber: &FiberInner, entry: &Rc<EffectEntry>) {
    if entry.disposed.replace(true) {
        return;
    }
    let disposers = std::mem::take(&mut *entry.disposers.borrow_mut());
    for disposer in disposers.into_iter().rev() {
        disposer();
    }
    fiber
        .effects
        .borrow_mut()
        .retain(|candidate| !Rc::ptr_eq(candidate, entry));
}

/// Runtime state of one plugin instance.
pub(crate) struct FiberInner {
    pub(crate) id: FiberId,
    pub(crate) parent: Option<FiberId>,
    pub(crate) status: Cell<FiberStatus>,
    pub(crate) inject: Vec<ServiceKeyId>,
    pub(crate) effects: RefCell<Vec<Rc<EffectEntry>>>,
    pub(crate) children: RefCell<Vec<FiberId>>,
    pub(crate) tasks: RefCell<Vec<AbortHandle>>,
    pub(crate) waiters: RefCell<Vec<oneshot::Sender<Result<(), PluginError>>>>,
    pub(crate) error: RefCell<Option<PluginError>>,
}

impl FiberInner {
    pub(crate) fn new(id: FiberId, parent: Option<FiberId>, inject: Vec<ServiceKeyId>) -> Self {
        Self {
            id,
            parent,
            status: Cell::new(FiberStatus::Pending),
            inject,
            effects: RefCell::new(Vec::new()),
            children: RefCell::new(Vec::new()),
            tasks: RefCell::new(Vec::new()),
            waiters: RefCell::new(Vec::new()),
            error: RefCell::new(None),
        }
    }

    pub(crate) fn register_effect(&self, disposers: Vec<Disposer>) -> Rc<EffectEntry> {
        let entry = Rc::new(EffectEntry::new(disposers));
        self.effects.borrow_mut().push(entry.clone());
        entry
    }

    pub(crate) fn abort_tasks(&self) {
        let tasks = std::mem::take(&mut *self.tasks.borrow_mut());
        for task in tasks {
            task.abort();
        }
    }

    pub(crate) fn dispose_effects(&self) {
        let effects: Vec<Rc<EffectEntry>> = self.effects.borrow().iter().cloned().collect();
        for entry in effects.into_iter().rev() {
            dispose_entry(self, &entry);
        }
    }

    pub(crate) fn notify_waiters(&self, result: &Result<(), PluginError>) {
        let waiters = std::mem::take(&mut *self.waiters.borrow_mut());
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }
}

/// Handle to one mounted plugin (one fiber).
#[derive(Clone)]
pub struct FiberHandle {
    pub(crate) inner: Rc<FiberInner>,
    pub(crate) registry: Rc<Registry>,
}

impl FiberHandle {
    pub fn id(&self) -> FiberId {
        self.inner.id
    }

    /// Parent fiber id, or `None` for the root.
    pub fn parent_id(&self) -> Option<FiberId> {
        self.inner.parent
    }

    pub fn status(&self) -> FiberStatus {
        self.inner.status.get()
    }

    /// Wait until the plugin finishes loading (or fails).
    pub async fn join(&self) -> Result<(), PluginError> {
        match self.status() {
            FiberStatus::Active => Ok(()),
            FiberStatus::Failed => Err(self
                .inner
                .error
                .borrow()
                .clone()
                .unwrap_or_else(PluginError::disposed)),
            FiberStatus::Disposed => Err(PluginError::disposed()),
            _ => {
                let (tx, rx) = oneshot::channel();
                self.inner.waiters.borrow_mut().push(tx);
                match rx.await {
                    Ok(result) => result,
                    Err(_) => Err(PluginError::disposed()),
                }
            }
        }
    }

    /// Unload this fiber, its children, effects and services.
    pub async fn dispose(&self) {
        self.registry.dispose(self.inner.id).await;
    }
}
