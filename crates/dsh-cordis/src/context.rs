use std::any::{Any, TypeId};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use dsh_runtime::{LocalBoxFuture, Runtime, RuntimeExt, TaskHandle};

use crate::error::CordisError;
use crate::event::{
    BailEvent, EmitEvent, EmitFn, EventBus, ListenerHandle, ParallelEvent, SerialEvent,
    WaterfallEvent,
};
use crate::fiber::{Disposer, EffectHandle, FiberHandle, FiberId, FiberInner, FiberStatus};
use crate::plugin::{Config, FnPlugin, Plugin, PluginError};
use crate::registry::Registry;
use crate::service::{ServiceEntry, ServiceHandle, ServiceKey, ServiceStore};

pub(crate) struct ContextInner {
    parent: Option<Context>,
    store: Rc<ServiceStore>,
    events: Rc<EventBus>,
    fiber: Rc<FiberInner>,
    registry: Rc<Registry>,
    runtime: Rc<dyn Runtime>,
}

/// A Cordis context: the handle through which plugins reach services, events
/// and their fiber.
///
/// Cloning a context is cheap (one `Rc` bump). Child contexts created by
/// [`Context::scope`] and [`Context::isolate`] share the parent's fiber and
/// event bus but resolve services through their own scope first.
#[derive(Clone)]
pub struct Context {
    inner: Rc<ContextInner>,
}

impl Context {
    /// Create the root context for `runtime`.
    ///
    /// The root fiber is always active; the registry assigns plugin fibers
    /// ids starting at `1`.
    pub fn root(runtime: Rc<dyn Runtime>) -> Self {
        let registry = Rc::new(Registry::new(runtime.clone()));
        let fiber = Rc::new(FiberInner::new(0, None, Vec::new()));
        fiber.status.set(FiberStatus::Active);
        Self {
            inner: Rc::new(ContextInner {
                parent: None,
                store: ServiceStore::root(),
                events: Rc::new(EventBus::new()),
                fiber,
                registry,
                runtime,
            }),
        }
    }

    pub(crate) fn child(&self, fiber: Rc<FiberInner>) -> Self {
        self.child_with_store(fiber, self.inner.store.clone())
    }

    fn child_with_store(&self, fiber: Rc<FiberInner>, store: Rc<ServiceStore>) -> Self {
        Self {
            inner: Rc::new(ContextInner {
                parent: Some(self.clone()),
                store,
                events: self.inner.events.clone(),
                fiber,
                registry: self.inner.registry.clone(),
                runtime: self.inner.runtime.clone(),
            }),
        }
    }

    /// A child context in the same service scope and fiber.
    pub fn scope(&self) -> Self {
        self.child(self.inner.fiber.clone())
    }

    /// A child context where `key` is resolved independently from the parent
    /// (Cordis `ctx.isolate(name)`).
    pub fn isolate(&self, key: ServiceKey) -> Self {
        let store = ServiceStore::isolate(self.inner.store.clone(), key);
        self.child_with_store(self.inner.fiber.clone(), store)
    }

    /// Walk to the root context.
    pub fn root_context(&self) -> Self {
        let mut current = self.clone();
        while let Some(parent) = current.inner.parent.clone() {
            current = parent;
        }
        current
    }

    pub fn fiber_id(&self) -> FiberId {
        self.inner.fiber.id
    }

    pub(crate) fn register_child(&self, id: FiberId) {
        self.inner.fiber.children.borrow_mut().push(id);
    }

    pub fn status(&self) -> FiberStatus {
        self.inner.fiber.status.get()
    }

    pub fn runtime(&self) -> Rc<dyn Runtime> {
        self.inner.runtime.clone()
    }

    fn assert_active(&self) -> Result<(), CordisError> {
        match self.status() {
            FiberStatus::Disposed | FiberStatus::Disposing | FiberStatus::Failed => {
                Err(CordisError::InactiveFiber)
            }
            _ => Ok(()),
        }
    }

    fn provider_visible(&self, entry: &ServiceEntry) -> bool {
        entry.provider == self.inner.fiber.id
            || self.inner.registry.status_of(entry.provider) == FiberStatus::Active
    }

    /// Read a service by key, walking the scope chain.
    ///
    /// Services provided by a fiber that has not finished loading are not
    /// visible to other fibers (Cordis activates a service when its provider
    /// fiber becomes active).
    pub fn get<T>(&self, key: ServiceKey) -> Option<Arc<T>>
    where
        T: Any + Send + Sync,
    {
        let mut store = self.inner.store.clone();
        loop {
            let entry = store.services.borrow().get(&key).cloned();
            if let Some(entry) = entry {
                if entry.type_id != TypeId::of::<T>() {
                    return None;
                }
                if !self.provider_visible(&entry) {
                    return None;
                }
                return entry.downcast::<T>();
            }
            if store.isolated.borrow().contains(&key) {
                return None;
            }
            store = store.parent.clone()?;
        }
    }

    /// Read a required service or fail with [`CordisError::MissingService`].
    pub fn require<T>(&self, key: ServiceKey) -> Result<Arc<T>, CordisError>
    where
        T: Any + Send + Sync,
    {
        self.get::<T>(key).ok_or_else(|| CordisError::missing(key))
    }

    pub(crate) fn has_active_service(&self, key: ServiceKey) -> bool {
        let mut store = self.inner.store.clone();
        loop {
            let entry = store.services.borrow().get(&key).cloned();
            if let Some(entry) = entry {
                return self.provider_visible(&entry);
            }
            if store.isolated.borrow().contains(&key) {
                return false;
            }
            match store.parent.clone() {
                Some(parent) => store = parent,
                None => return false,
            }
        }
    }

    /// Register a service implementation owned by the current fiber.
    ///
    /// The service is removed when its disposer runs or when the fiber
    /// unloads. Providing an already-present name in the same scope fails;
    /// use [`Context::isolate`] to override an inherited service.
    pub fn provide<T>(&self, key: ServiceKey, value: Arc<T>) -> Result<ServiceHandle, CordisError>
    where
        T: Send + Sync + 'static,
    {
        self.assert_active()?;
        {
            let mut services = self.inner.store.services.borrow_mut();
            if services.contains_key(&key) {
                return Err(CordisError::ServiceAlreadyProvided(key.as_str()));
            }
            services.insert(key, ServiceEntry::new(value, self.fiber_id()));
        }

        let store = self.inner.store.clone();
        let registry = self.inner.registry.clone();
        let effect = self.effect(move || {
            let store = store.clone();
            let registry = registry.clone();
            Some(Box::new(move || {
                store.services.borrow_mut().remove(&key);
                registry.service_removed();
            }))
        })?;

        // A provisioned service may satisfy pending `inject` declarations.
        self.inner.registry.service_added();
        Ok(ServiceHandle { effect, key })
    }

    /// Register a cleanup-aware effect on this fiber.
    ///
    /// `execute` runs immediately and returns its disposers. Disposers run in
    /// reverse order when the effect is disposed or the fiber unloads.
    pub fn effect<F>(&self, execute: F) -> Result<EffectHandle, CordisError>
    where
        F: FnOnce() -> Option<Disposer> + 'static,
    {
        self.assert_active()?;
        let disposers = execute().into_iter().collect::<Vec<_>>();
        let entry = self.inner.fiber.register_effect(disposers);
        Ok(EffectHandle {
            fiber: self.inner.fiber.clone(),
            entry,
        })
    }

    fn add_emit<E: EmitEvent>(
        &self,
        f: EmitFn<E>,
        prepend: bool,
        once: bool,
    ) -> Result<ListenerHandle, CordisError> {
        self.assert_active()?;
        let (key, id) = self.inner.events.add_emit::<E>(f, prepend, once);
        let bus = self.inner.events.clone();
        let effect = self.effect({
            let bus = bus.clone();
            move || {
                let bus = bus.clone();
                Some(Box::new(move || {
                    bus.remove(key, id);
                }))
            }
        });
        if let Err(error) = effect {
            bus.remove(key, id);
            return Err(error);
        }
        Ok(ListenerHandle::new(bus, key, id))
    }

    /// Register an `emit` listener.
    pub fn on<E: EmitEvent>(
        &self,
        f: impl Fn(E::Args) + 'static,
    ) -> Result<ListenerHandle, CordisError> {
        self.add_emit::<E>(Rc::new(f), false, false)
    }

    /// Register an `emit` listener that disposes itself after its first call.
    pub fn once<E: EmitEvent>(
        &self,
        f: impl Fn(E::Args) + 'static,
    ) -> Result<ListenerHandle, CordisError> {
        self.add_emit::<E>(Rc::new(f), false, true)
    }

    /// Register a `waterfall` listener. The listener may call
    /// [`crate::Next::next`] or return without it to veto the chain.
    pub fn on_waterfall<E: WaterfallEvent>(
        &self,
        f: impl Fn(E::Args, crate::Next<E>) -> LocalBoxFuture<'static, E::Out> + 'static,
    ) -> Result<ListenerHandle, CordisError> {
        self.assert_active()?;
        let (key, id) = self.inner.events.add_waterfall::<E>(Rc::new(f), false);
        let bus = self.inner.events.clone();
        let effect = self.effect({
            let bus = bus.clone();
            move || {
                let bus = bus.clone();
                Some(Box::new(move || {
                    bus.remove(key, id);
                }))
            }
        });
        if let Err(error) = effect {
            bus.remove(key, id);
            return Err(error);
        }
        Ok(ListenerHandle::new(bus, key, id))
    }

    /// Register a `serial` listener. Return `Some(value)` to bail.
    pub fn on_serial<E: SerialEvent>(
        &self,
        f: impl Fn(E::Args) -> LocalBoxFuture<'static, Option<E::Out>> + 'static,
    ) -> Result<ListenerHandle, CordisError> {
        self.assert_active()?;
        let (key, id) = self.inner.events.add_serial::<E>(Rc::new(f), false, false);
        let bus = self.inner.events.clone();
        let effect = self.effect({
            let bus = bus.clone();
            move || {
                let bus = bus.clone();
                Some(Box::new(move || {
                    bus.remove(key, id);
                }))
            }
        });
        if let Err(error) = effect {
            bus.remove(key, id);
            return Err(error);
        }
        Ok(ListenerHandle::new(bus, key, id))
    }

    /// Register a `bail` listener. Return `Some(value)` to bail.
    pub fn on_bail<E: BailEvent>(
        &self,
        f: impl Fn(E::Args) -> Option<E::Out> + 'static,
    ) -> Result<ListenerHandle, CordisError> {
        self.assert_active()?;
        let (key, id) = self.inner.events.add_bail::<E>(Rc::new(f), false, false);
        let bus = self.inner.events.clone();
        let effect = self.effect({
            let bus = bus.clone();
            move || {
                let bus = bus.clone();
                Some(Box::new(move || {
                    bus.remove(key, id);
                }))
            }
        });
        if let Err(error) = effect {
            bus.remove(key, id);
            return Err(error);
        }
        Ok(ListenerHandle::new(bus, key, id))
    }

    /// Register a `parallel` listener.
    pub fn on_parallel<E: ParallelEvent>(
        &self,
        f: impl Fn(E::Args) -> LocalBoxFuture<'static, ()> + 'static,
    ) -> Result<ListenerHandle, CordisError> {
        self.assert_active()?;
        let (key, id) = self
            .inner
            .events
            .add_parallel::<E>(Rc::new(f), false, false);
        let bus = self.inner.events.clone();
        let effect = self.effect({
            let bus = bus.clone();
            move || {
                let bus = bus.clone();
                Some(Box::new(move || {
                    bus.remove(key, id);
                }))
            }
        });
        if let Err(error) = effect {
            bus.remove(key, id);
            return Err(error);
        }
        Ok(ListenerHandle::new(bus, key, id))
    }

    /// Dispatch an `emit` event.
    pub fn emit<E: EmitEvent>(&self, args: E::Args) {
        self.inner.events.emit::<E>(args);
    }

    /// Dispatch a `parallel` event and await every listener.
    pub async fn parallel<E: ParallelEvent>(&self, args: E::Args) {
        self.inner.events.parallel::<E>(args).await;
    }

    /// Dispatch a `serial` event and await listeners until one bails.
    pub async fn serial<E: SerialEvent>(&self, args: E::Args) -> Option<E::Out> {
        self.inner.events.serial::<E>(args).await
    }

    /// Dispatch a `bail` event synchronously.
    pub fn bail<E: BailEvent>(&self, args: E::Args) -> Option<E::Out> {
        self.inner.events.bail::<E>(args)
    }

    /// Dispatch a `waterfall` event around `default`.
    pub fn waterfall<E: WaterfallEvent>(
        &self,
        args: E::Args,
        default: impl Fn(E::Args) -> LocalBoxFuture<'static, E::Out> + 'static,
    ) -> LocalBoxFuture<'static, E::Out> {
        self.inner.events.waterfall::<E>(args, default)
    }

    /// Spawn a task tracked by this fiber; unloading the fiber aborts it.
    pub fn spawn(&self, fut: LocalBoxFuture<'static, ()>) -> Result<TaskHandle<()>, CordisError> {
        self.assert_active()?;
        let handle = Runtime::spawn(self.inner.runtime.as_ref(), fut);
        self.track(handle.abort_handle());
        Ok(handle)
    }

    /// Spawn a typed task tracked by this fiber.
    pub fn spawn_typed<F, T>(&self, fut: F) -> Result<TaskHandle<T>, CordisError>
    where
        F: Future<Output = T> + 'static,
        T: 'static,
    {
        self.assert_active()?;
        let handle = RuntimeExt::spawn_typed(self.inner.runtime.as_ref(), fut);
        self.track(handle.abort_handle());
        Ok(handle)
    }

    /// Spawn blocking work owned by this fiber.
    pub fn spawn_blocking(
        &self,
        f: Box<dyn FnOnce() + Send + 'static>,
    ) -> Result<TaskHandle<()>, CordisError> {
        self.assert_active()?;
        let handle = Runtime::spawn_blocking(self.inner.runtime.as_ref(), f);
        self.track(handle.abort_handle());
        Ok(handle)
    }

    fn track(&self, handle: dsh_runtime::AbortHandle) {
        let mut tasks = self.inner.fiber.tasks.borrow_mut();
        tasks.retain(|task| !task.is_finished());
        tasks.push(handle);
    }

    /// Mount a plugin as a child fiber.
    pub fn plugin<P>(&self, plugin: P) -> Result<FiberHandle, CordisError>
    where
        P: Plugin + 'static,
    {
        self.plugin_with_config(plugin, Config::Null)
    }

    /// Mount a plugin with a config value.
    pub fn plugin_with_config<P>(
        &self,
        plugin: P,
        config: Config,
    ) -> Result<FiberHandle, CordisError>
    where
        P: Plugin + 'static,
    {
        self.assert_active()?;
        Ok(self.inner.registry.mount(self, Rc::new(plugin), config))
    }

    /// Cordis `ctx.inject(deps, callback)` shorthand.
    pub fn inject<F>(&self, deps: &[ServiceKey], callback: F) -> Result<FiberHandle, CordisError>
    where
        F: Fn(Context) -> LocalBoxFuture<'static, Result<(), PluginError>> + 'static,
    {
        let plugin =
            FnPlugin::new("inject", move |ctx, _config| callback(ctx)).inject(deps.iter().copied());
        self.plugin(plugin)
    }
}
