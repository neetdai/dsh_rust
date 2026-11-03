use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::rc::Rc;
use std::sync::Arc;

use crate::fiber::FiberId;

/// Service container shared by a context scope.
///
/// Every child context shares the nearest scope store, so a provider's
/// `provide` is visible to siblings. `ctx.isolate(key)` creates a nested
/// store whose `key` lookup stops there.
pub(crate) struct ServiceStore {
    pub(crate) services: RefCell<HashMap<ServiceKey, ServiceEntry>>,
    pub(crate) parent: Option<Rc<ServiceStore>>,
    pub(crate) isolated: RefCell<HashSet<ServiceKey>>,
}

impl ServiceStore {
    pub(crate) fn root() -> Rc<Self> {
        Rc::new(Self {
            services: RefCell::new(HashMap::new()),
            parent: None,
            isolated: RefCell::new(HashSet::new()),
        })
    }

    pub(crate) fn isolate(parent: Rc<Self>, key: ServiceKey) -> Rc<Self> {
        let mut isolated = HashSet::new();
        isolated.insert(key);
        Rc::new(Self {
            services: RefCell::new(HashMap::new()),
            parent: Some(parent),
            isolated: RefCell::new(isolated),
        })
    }
}

/// Stable service name used by `provide`, `get`, `require` and `inject`.
///
/// Cordis services are addressed by a string name (`ctx.tools`, `ctx.llm`).
/// The key keeps that name for diagnostics and configuration, while the
/// Rust-side lookup additionally checks the concrete service type.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ServiceKey(&'static str);

impl ServiceKey {
    /// Define a service key.
    pub const fn new(name: &'static str) -> Self {
        Self(name)
    }

    /// The Cordis service name.
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Debug for ServiceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServiceKey({:?})", self.0)
    }
}

impl fmt::Display for ServiceKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// One provided service implementation.
#[derive(Clone)]
pub struct ServiceEntry {
    pub(crate) value: Arc<dyn Any + Send + Sync>,
    pub(crate) type_id: TypeId,
    pub(crate) provider: FiberId,
}

impl ServiceEntry {
    pub(crate) fn new<T: Any + Send + Sync>(value: Arc<T>, provider: FiberId) -> Self {
        Self {
            value,
            type_id: TypeId::of::<T>(),
            provider,
        }
    }

    pub(crate) fn downcast<T: Any + Send + Sync>(&self) -> Option<Arc<T>> {
        if self.type_id != TypeId::of::<T>() {
            return None;
        }
        self.value.clone().downcast::<T>().ok()
    }
}

/// Handle returned by [`crate::Context::provide`].
pub struct ServiceHandle {
    pub(crate) effect: crate::fiber::EffectHandle,
    pub(crate) key: ServiceKey,
}

impl ServiceHandle {
    /// The provided service name.
    pub fn key(&self) -> ServiceKey {
        self.key
    }

    /// Withdraw the service immediately.
    pub fn dispose(&self) {
        self.effect.dispose();
    }
}
