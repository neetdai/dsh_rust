use std::any::{Any, TypeId};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fmt;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use std::rc::Rc;
use std::sync::Arc;

use crate::fiber::FiberId;

/// Service container shared by a context scope.
///
/// Every child context shares the nearest scope store, so a provider's
/// `provide` is visible to siblings. `ctx.isolate(key)` creates a nested
/// store whose `key` lookup stops there.
pub(crate) struct ServiceStore {
    pub(crate) services: RefCell<HashMap<ServiceKeyId, ServiceEntry>>,
    pub(crate) parent: Option<Rc<ServiceStore>>,
    pub(crate) isolated: RefCell<HashSet<ServiceKeyId>>,
}

impl ServiceStore {
    pub(crate) fn root() -> Rc<Self> {
        Rc::new(Self {
            services: RefCell::new(HashMap::new()),
            parent: None,
            isolated: RefCell::new(HashSet::new()),
        })
    }

    pub(crate) fn isolate(parent: Rc<Self>, key: ServiceKeyId) -> Rc<Self> {
        let mut isolated = HashSet::new();
        isolated.insert(key);
        Rc::new(Self {
            services: RefCell::new(HashMap::new()),
            parent: Some(parent),
            isolated: RefCell::new(isolated),
        })
    }
}

/// Type-erased identity of a service: its Cordis name plus the concrete Rust
/// type stored under it.
///
/// `ServiceKeyId` is what the service store, `inject` declarations and
/// diagnostics use; it can be compared, hashed and passed across code that is
/// not generic over the service type. Use [`ServiceKey::id`] to obtain one.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct ServiceKeyId {
    name: &'static str,
    type_id: TypeId,
}

impl ServiceKeyId {
    /// The Cordis service name (`ctx.tools`, `ctx.llm`, ...).
    pub const fn name(self) -> &'static str {
        self.name
    }

    /// The concrete Rust type provided under this name.
    pub const fn type_id(self) -> TypeId {
        self.type_id
    }
}

impl fmt::Debug for ServiceKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServiceKeyId({:?}, {:?})", self.name, self.type_id)
    }
}

impl fmt::Display for ServiceKeyId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

/// Compile-time-typed service key used by `provide`, `get`, `require` and
/// `inject`.
///
/// A `ServiceKey<T>` couples the Cordis service name with the concrete Rust
/// type `T` behind it, so a lookup for `ServiceKey<T>` can only ever return a
/// `T`. The key is a small `Copy` value (the name plus a marker for `T`) and
/// can be defined in `const` items.
///
/// ```
/// use dsh_cordis::ServiceKey;
///
/// struct Greeter;
/// const GREETER: ServiceKey<Greeter> = ServiceKey::new("greeter");
///
/// assert_eq!(GREETER.name(), "greeter");
/// assert_eq!(GREETER.id(), GREETER.id());
/// ```
pub struct ServiceKey<T: Any + Send + Sync> {
    name: &'static str,
    service: PhantomData<fn() -> T>,
}

impl<T: Any + Send + Sync> ServiceKey<T> {
    /// Define a service key for the concrete service type `T`.
    pub const fn new(name: &'static str) -> Self {
        Self {
            name,
            service: PhantomData,
        }
    }

    /// The type-erased storage key (`name` + `TypeId`) for this service.
    pub const fn id(self) -> ServiceKeyId {
        ServiceKeyId {
            name: self.name,
            type_id: TypeId::of::<T>(),
        }
    }

    /// The Cordis service name.
    pub const fn name(self) -> &'static str {
        self.name
    }
}

impl<T: Any + Send + Sync> Copy for ServiceKey<T> {}

impl<T: Any + Send + Sync> Clone for ServiceKey<T> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<T: Any + Send + Sync> PartialEq for ServiceKey<T> {
    fn eq(&self, other: &Self) -> bool {
        self.id() == other.id()
    }
}

impl<T: Any + Send + Sync> Eq for ServiceKey<T> {}

impl<T: Any + Send + Sync> Hash for ServiceKey<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.id().hash(state);
    }
}

impl<T: Any + Send + Sync> fmt::Debug for ServiceKey<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ServiceKey({:?})", self.name)
    }
}

impl<T: Any + Send + Sync> fmt::Display for ServiceKey<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name)
    }
}

/// Erase the service type, e.g. for `PluginMeta::inject` and `ctx.inject`.
impl<T: Any + Send + Sync> From<ServiceKey<T>> for ServiceKeyId {
    fn from(key: ServiceKey<T>) -> Self {
        key.id()
    }
}

/// Erase a borrowed key, so `inject(&[GREETER])` works as well.
impl<T: Any + Send + Sync> From<&ServiceKey<T>> for ServiceKeyId {
    fn from(key: &ServiceKey<T>) -> Self {
        key.id()
    }
}

impl From<&ServiceKeyId> for ServiceKeyId {
    fn from(key: &ServiceKeyId) -> Self {
        *key
    }
}

/// One provided service implementation.
#[derive(Clone)]
pub struct ServiceEntry {
    pub(crate) value: Arc<dyn Any + Send + Sync>,
    pub(crate) provider: FiberId,
}

impl ServiceEntry {
    pub(crate) fn new<T: Any + Send + Sync>(value: Arc<T>, provider: FiberId) -> Self {
        Self { value, provider }
    }

    /// Recover the concrete service value.
    ///
    /// The store is keyed by [`ServiceKeyId`], which embeds `TypeId::of::<T>()`,
    /// so a lookup through `ServiceKey<T>` can only reach an entry holding a
    /// `T`; this is an internal invariant, not a runtime type check.
    pub(crate) fn downcast<T: Any + Send + Sync>(&self) -> Arc<T> {
        self.value
            .clone()
            .downcast::<T>()
            .unwrap_or_else(|_| unreachable!("service store key/type invariant violated"))
    }
}

/// Handle returned by [`crate::Context::provide`].
pub struct ServiceHandle {
    pub(crate) effect: crate::fiber::EffectHandle,
    pub(crate) key: ServiceKeyId,
}

impl ServiceHandle {
    /// The provided service's erased key (name + concrete type).
    pub fn key(&self) -> ServiceKeyId {
        self.key
    }

    /// Withdraw the service immediately.
    pub fn dispose(&self) {
        self.effect.dispose();
    }
}
