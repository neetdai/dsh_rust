use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use dsh_runtime::{Runtime, RuntimeExt};

use crate::context::Context;
use crate::fiber::{FiberHandle, FiberId, FiberInner, FiberStatus};
use crate::plugin::{Config, Plugin, PluginError};
use crate::service::ServiceKeyId;

#[derive(Clone)]
struct Mounted {
    fiber: Rc<FiberInner>,
    plugin: Rc<dyn Plugin>,
    ctx: Context,
    config: Config,
    /// Apply generation shared by the mount entry and every spawned apply task.
    /// Bumping it invalidates results of earlier (now unloaded) applies.
    generation: Rc<Cell<u64>>,
}

pub(crate) struct Registry {
    runtime: Rc<dyn Runtime>,
    fibers: RefCell<HashMap<FiberId, Mounted>>,
    pending: RefCell<Vec<FiberId>>,
    next_fiber: Cell<u64>,
    /// Re-entrancy guard for [`Registry::cascade`].
    cascading: Cell<bool>,
}

impl Registry {
    pub(crate) fn new(runtime: Rc<dyn Runtime>) -> Self {
        Self {
            runtime,
            fibers: RefCell::new(HashMap::new()),
            pending: RefCell::new(Vec::new()),
            next_fiber: Cell::new(1),
            cascading: Cell::new(false),
        }
    }

    pub(crate) fn status_of(&self, id: FiberId) -> FiberStatus {
        if id == 0 {
            return FiberStatus::Active;
        }
        self.fibers
            .borrow()
            .get(&id)
            .map(|mounted| mounted.fiber.status.get())
            .unwrap_or(FiberStatus::Disposed)
    }

    fn can_resolve(ctx: &Context, inject: &[ServiceKeyId]) -> bool {
        inject.iter().all(|key| ctx.has_active_service(*key))
    }

    pub(crate) fn mount(
        self: &Rc<Self>,
        parent: &Context,
        plugin: Rc<dyn Plugin>,
        config: Config,
    ) -> FiberHandle {
        let id = self.next_fiber.get();
        self.next_fiber.set(id + 1);

        let inject = plugin.meta().inject.clone();
        let fiber = Rc::new(FiberInner::new(id, Some(parent.fiber_id()), inject));
        let ctx = parent.child(fiber.clone());
        parent.register_child(id);

        let handle = FiberHandle {
            inner: fiber.clone(),
            registry: self.clone(),
        };

        self.fibers.borrow_mut().insert(
            id,
            Mounted {
                fiber: fiber.clone(),
                plugin,
                ctx: ctx.clone(),
                config,
                generation: Rc::new(Cell::new(0)),
            },
        );

        if Self::can_resolve(&ctx, &fiber.inject) {
            self.start(id);
        } else {
            self.pending.borrow_mut().push(id);
        }

        handle
    }

    fn start(self: &Rc<Self>, id: FiberId) {
        let Some(mounted) = self.fibers.borrow().get(&id).cloned() else {
            return;
        };
        if mounted.fiber.status.get() != FiberStatus::Pending {
            return;
        }
        let generation = mounted.generation.get() + 1;
        mounted.generation.set(generation);
        mounted.fiber.status.set(FiberStatus::Loading);

        let mut apply_future = mounted
            .plugin
            .apply(mounted.ctx.clone(), mounted.config.clone());
        // Gate every poll on the apply generation. Once an unload/dispose
        // bumps it, the old future is dropped without polling user code
        // again, so it cannot register listeners/services/tasks for a newer
        // generation that now owns the fiber.
        let gate_registry = self.clone();
        let guarded = futures::future::poll_fn(move |cx| {
            let stale = {
                let fibers = gate_registry.fibers.borrow();
                fibers.get(&id).map(|mounted| mounted.generation.get()) != Some(generation)
            };
            if stale {
                std::task::Poll::Ready(None)
            } else {
                apply_future.as_mut().poll(cx).map(Some)
            }
        });
        let registry = self.clone();
        let task = RuntimeExt::spawn_typed(self.runtime.as_ref(), async move {
            match guarded.await {
                Some(result) => {
                    registry.finish_apply(id, generation, result.clone());
                    result
                }
                // A stale apply was never polled after the generation bump;
                // the unload rollback already cleaned up its registrations.
                None => Ok(()),
            }
        });
        mounted.fiber.tasks.borrow_mut().push(task.abort_handle());
    }

    fn finish_apply(
        self: &Rc<Self>,
        id: FiberId,
        generation: u64,
        result: Result<(), PluginError>,
    ) {
        let Some(mounted) = self.fibers.borrow().get(&id).cloned() else {
            return;
        };
        // A newer apply started, or this fiber was unloaded/disposed while the
        // old apply was running: drop the stale result.
        if mounted.generation.get() != generation {
            // Cancellation can lose a race against an already-ready apply
            // future, so a stale apply may still have registered effects after
            // its generation was invalidated. Tear them down while the fiber is
            // still Pending; if a newer apply already owns the fiber
            // (Loading/Active) its effects must not be touched.
            if mounted.fiber.status.get() == FiberStatus::Pending {
                mounted.fiber.status.set(FiberStatus::Disposing);
                self.rollback_to_pending(&mounted);
            }
            return;
        }
        if !matches!(
            mounted.fiber.status.get(),
            FiberStatus::Pending | FiberStatus::Loading
        ) {
            return;
        }
        match result {
            Ok(()) => {
                // A provider may have disappeared while `apply` was running.
                // Do not activate a fiber whose inject no longer resolves;
                // unload it through the same rollback path instead.
                if !Self::can_resolve(&mounted.ctx, &mounted.fiber.inject) {
                    mounted.fiber.status.set(FiberStatus::Disposing);
                    self.rollback_to_pending(&mounted);
                    return;
                }
                mounted.fiber.status.set(FiberStatus::Active);
                mounted.fiber.notify_waiters(&Ok(()));
                self.wake_pending();
            }
            Err(error) => {
                // Roll back registrations made before the failure, including
                // child plugins mounted by the failed `apply`.
                let children = mounted.fiber.children.borrow().clone();
                for child in children {
                    let order = self.collect_subtree(child);
                    for fiber_id in order {
                        self.dispose_one(fiber_id);
                    }
                }
                mounted.fiber.dispose_effects();
                mounted.fiber.error.borrow_mut().replace(error.clone());
                mounted.fiber.status.set(FiberStatus::Failed);
                mounted.fiber.notify_waiters(&Err(error));
            }
        }
    }

    fn wake_pending(self: &Rc<Self>) {
        loop {
            let ready: Vec<FiberId> = {
                let mut pending = self.pending.borrow_mut();
                let mut ready = Vec::new();
                let mut index = 0;
                while index < pending.len() {
                    let id = pending[index];
                    let is_ready = self
                        .fibers
                        .borrow()
                        .get(&id)
                        .map(|mounted| {
                            mounted.fiber.status.get() == FiberStatus::Pending
                                && Self::can_resolve(&mounted.ctx, &mounted.fiber.inject)
                        })
                        .unwrap_or(false);
                    if is_ready {
                        ready.push(pending.remove(index));
                    } else {
                        index += 1;
                    }
                }
                ready
            };
            if ready.is_empty() {
                break;
            }
            for id in ready {
                self.start(id);
            }
        }
    }

    pub(crate) fn service_added(self: &Rc<Self>) {
        self.wake_pending();
    }

    pub(crate) fn service_removed(self: &Rc<Self>) {
        self.cascade();
    }

    pub(crate) async fn dispose(self: &Rc<Self>, id: FiberId) {
        let order = self.collect_subtree(id);
        for fiber_id in order {
            self.dispose_one(fiber_id);
        }
        self.cascade();
    }

    /// Unload an `Active` or `Loading` fiber back to `Pending` so it can be
    /// re-applied when its injected services become available again.
    ///
    /// For a `Loading` fiber the in-flight apply task is aborted and the apply
    /// generation is bumped, so a result that arrives late is discarded. The
    /// mount entry (plugin instance, context and the already normalized
    /// config) is kept; effects and child plugins from the previous `apply`
    /// are torn down. Unlike explicit [`Registry::dispose`] this never
    /// notifies join waiters: a `join()` on this fiber keeps waiting for the
    /// next reload and only fails if the fiber is explicitly disposed.
    fn unload_to_pending(self: &Rc<Self>, id: FiberId) {
        let Some(mounted) = self.fibers.borrow().get(&id).cloned() else {
            return;
        };
        if !matches!(
            mounted.fiber.status.get(),
            FiberStatus::Active | FiberStatus::Loading
        ) {
            return;
        }

        mounted.fiber.status.set(FiberStatus::Disposing);
        // Abort a Loading fiber's apply before tearing down its partial
        // effects; the generation bump also discards any late result.
        mounted.fiber.abort_tasks();
        self.rollback_to_pending(&mounted);
    }

    /// Shared rollback used by cascade and by a late `finish_apply`.
    ///
    /// The caller must set `Disposing` first. This invalidates the current
    /// apply generation, unloads child plugins, runs effects in reverse order,
    /// and queues the fiber for a future reload without notifying join
    /// waiters.
    fn rollback_to_pending(self: &Rc<Self>, mounted: &Mounted) {
        mounted.generation.set(mounted.generation.get() + 1);

        // Child plugins mounted by the previous apply are unloaded with it;
        // forgetting them lets the re-apply build a fresh subtree instead of
        // accumulating stale child ids.
        let children = mounted.fiber.children.borrow().clone();
        for child in children {
            for fiber_id in self.collect_subtree(child) {
                self.dispose_one(fiber_id);
            }
        }
        mounted.fiber.children.borrow_mut().clear();

        // Reverse-order effect teardown removes listeners and provided
        // services. Disposers may call `service_removed` re-entrantly; the
        // cascade guard makes that a no-op and the outer loop re-scans.
        mounted.fiber.dispose_effects();
        mounted.fiber.error.borrow_mut().take();

        mounted.fiber.status.set(FiberStatus::Pending);
        let mut pending = self.pending.borrow_mut();
        if !pending.contains(&mounted.fiber.id) {
            pending.push(mounted.fiber.id);
        }
    }

    fn collect_subtree(&self, id: FiberId) -> Vec<FiberId> {
        let mut out = Vec::new();
        self.collect_into(id, &mut out);
        out
    }

    fn collect_into(&self, id: FiberId, out: &mut Vec<FiberId>) {
        let children = self
            .fibers
            .borrow()
            .get(&id)
            .map(|mounted| mounted.fiber.children.borrow().clone())
            .unwrap_or_default();
        for child in children {
            self.collect_into(child, out);
        }
        out.push(id);
    }

    fn dispose_one(self: &Rc<Self>, id: FiberId) {
        let Some(mounted) = self.fibers.borrow().get(&id).cloned() else {
            return;
        };
        if mounted.fiber.status.get() == FiberStatus::Disposed {
            return;
        }
        mounted.fiber.status.set(FiberStatus::Disposing);
        // Discard a result from an apply task that is being cancelled now.
        mounted.generation.set(mounted.generation.get() + 1);
        mounted.fiber.abort_tasks();
        mounted.fiber.dispose_effects();
        self.pending.borrow_mut().retain(|pending| *pending != id);
        mounted.fiber.status.set(FiberStatus::Disposed);
        mounted.fiber.notify_waiters(&Err(PluginError::disposed()));
        self.fibers.borrow_mut().remove(&id);
    }

    /// Unload plugins whose injected services disappeared with a provider.
    ///
    /// Dependents are moved back to `Pending` instead of being disposed, so a
    /// replacement provider makes them re-apply automatically. Both `Active`
    /// fibers and `Loading` fibers whose `apply` is still in flight are
    /// victims. A cascade triggered while one is already running (e.g. from an
    /// effect disposer) returns immediately; the running loop re-scans until
    /// stable.
    fn cascade(self: &Rc<Self>) {
        if self.cascading.replace(true) {
            return;
        }
        let _guard = CascadeGuard(&self.cascading);
        loop {
            let victims: Vec<FiberId> = self
                .fibers
                .borrow()
                .iter()
                .filter_map(|(id, mounted)| {
                    let unloadable = matches!(
                        mounted.fiber.status.get(),
                        FiberStatus::Active | FiberStatus::Loading
                    );
                    let broken = !mounted.fiber.inject.is_empty()
                        && !Self::can_resolve(&mounted.ctx, &mounted.fiber.inject);
                    (unloadable && broken).then_some(*id)
                })
                .collect();
            if victims.is_empty() {
                break;
            }
            for id in victims {
                self.unload_to_pending(id);
            }
        }
    }
}

/// Resets [`Registry::cascading`] even when a user disposer panics in the
/// middle of a cascade, so later service events still unload dependents.
struct CascadeGuard<'a>(&'a Cell<bool>);

impl Drop for CascadeGuard<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}
