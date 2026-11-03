use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::Rc;

use dsh_runtime::{Runtime, RuntimeExt};

use crate::context::Context;
use crate::fiber::{FiberHandle, FiberId, FiberInner, FiberStatus};
use crate::plugin::{Config, Plugin, PluginError};
use crate::service::ServiceKey;

#[derive(Clone)]
struct Mounted {
    fiber: Rc<FiberInner>,
    plugin: Rc<dyn Plugin>,
    ctx: Context,
    config: Config,
}

pub(crate) struct Registry {
    runtime: Rc<dyn Runtime>,
    fibers: RefCell<HashMap<FiberId, Mounted>>,
    pending: RefCell<Vec<FiberId>>,
    next_fiber: Cell<u64>,
}

impl Registry {
    pub(crate) fn new(runtime: Rc<dyn Runtime>) -> Self {
        Self {
            runtime,
            fibers: RefCell::new(HashMap::new()),
            pending: RefCell::new(Vec::new()),
            next_fiber: Cell::new(1),
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

    fn can_resolve(ctx: &Context, inject: &[ServiceKey]) -> bool {
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
        mounted.fiber.status.set(FiberStatus::Loading);

        let fut = mounted
            .plugin
            .apply(mounted.ctx.clone(), mounted.config.clone());
        let registry = self.clone();
        let task = RuntimeExt::spawn_typed(self.runtime.as_ref(), async move {
            let result = fut.await;
            registry.finish_apply(id, result.clone());
            result
        });
        mounted.fiber.tasks.borrow_mut().push(task.abort_handle());
    }

    fn finish_apply(self: &Rc<Self>, id: FiberId, result: Result<(), PluginError>) {
        let Some(mounted) = self.fibers.borrow().get(&id).cloned() else {
            return;
        };
        if !matches!(
            mounted.fiber.status.get(),
            FiberStatus::Pending | FiberStatus::Loading
        ) {
            return;
        }
        match result {
            Ok(()) => {
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
        mounted.fiber.abort_tasks();
        mounted.fiber.dispose_effects();
        self.pending.borrow_mut().retain(|pending| *pending != id);
        mounted.fiber.status.set(FiberStatus::Disposed);
        mounted.fiber.notify_waiters(&Err(PluginError::disposed()));
        self.fibers.borrow_mut().remove(&id);
    }

    /// Unload plugins whose injected services disappeared with a provider.
    fn cascade(self: &Rc<Self>) {
        loop {
            let victims: Vec<FiberId> = self
                .fibers
                .borrow()
                .iter()
                .filter_map(|(id, mounted)| {
                    let active = mounted.fiber.status.get() == FiberStatus::Active;
                    let broken = !mounted.fiber.inject.is_empty()
                        && !Self::can_resolve(&mounted.ctx, &mounted.fiber.inject);
                    (active && broken).then_some(*id)
                })
                .collect();
            if victims.is_empty() {
                break;
            }
            for id in victims {
                self.dispose_one(id);
            }
        }
    }
}
