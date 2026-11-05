//! Typed event bus with the five Cordis dispatch modes.
//!
//! | mode | awaited | order | return value |
//! |---|---|---|---|
//! | `emit` | no | registration order | no |
//! | `waterfall` | no (futures) | registration order, around middleware | yes |
//! | `parallel` | yes | concurrent | no |
//! | `serial` | yes | registration order, bails | first non-`None` |
//! | `bail` | no | registration order, bails | first non-`None` |
//!
//! Listeners are owned by a fiber through an effect, so unloading a plugin
//! removes its listeners. `once` listeners are removed from the bus when their
//! first dispatch starts, before any callback runs; that dispatch's snapshot
//! still runs them at most once, and later or re-entrant dispatches never see
//! them. This applies to all five modes, including `waterfall`.

use std::any::{Any, TypeId};
use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;

use dsh_runtime::LocalBoxFuture;

/// An event declaration.
pub trait Event: 'static {
    /// Arguments passed to every listener.
    type Args: Clone + 'static;
    /// Stable event name, for diagnostics.
    const NAME: &'static str;
}

/// Event dispatched synchronously with `ctx.emit`; listener results are ignored.
pub trait EmitEvent: Event {}

/// Event dispatched with `ctx.waterfall`; listeners wrap a final default.
pub trait WaterfallEvent: Event {
    /// Value threaded through the listener chain.
    type Out: 'static;
}

/// Event dispatched with `ctx.serial`; the first non-`None` value bails.
pub trait SerialEvent: Event {
    /// Bail value.
    type Out: 'static;
}

/// Event dispatched with `ctx.bail`; the first non-`None` value bails.
pub trait BailEvent: Event {
    /// Bail value.
    type Out: 'static;
}

/// Event dispatched with `ctx.parallel`; all listeners run concurrently.
pub trait ParallelEvent: Event {}

/// Registration options shared by all five event modes.
///
/// `EventOptions::default()` registers at the tail and keeps the listener
/// until it is disposed. See [`crate::Context::on_with`],
/// [`crate::Context::on_waterfall_with`], [`crate::Context::on_serial_with`],
/// [`crate::Context::on_bail_with`] and [`crate::Context::on_parallel_with`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct EventOptions {
    /// Insert the listener at the head of the chain instead of the tail.
    ///
    /// With several prepended listeners the most recently prepended one runs
    /// first (same as `Vec::insert(0, ..)`).
    pub prepend: bool,
    /// Remove the listener when its first dispatch starts.
    ///
    /// It still takes part in that dispatch, at most once, but never in a later
    /// or re-entrant dispatch.
    pub once: bool,
}

pub(crate) type EmitFn<E> = Rc<dyn Fn(<E as Event>::Args)>;
pub(crate) type WaterfallFn<E> =
    Rc<dyn Fn(<E as Event>::Args, Next<E>) -> LocalBoxFuture<'static, <E as WaterfallEvent>::Out>>;
pub(crate) type SerialFn<E> =
    Rc<dyn Fn(<E as Event>::Args) -> LocalBoxFuture<'static, Option<<E as SerialEvent>::Out>>>;
pub(crate) type BailFn<E> = Rc<dyn Fn(<E as Event>::Args) -> Option<<E as BailEvent>::Out>>;
pub(crate) type ParallelFn<E> = Rc<dyn Fn(<E as Event>::Args) -> LocalBoxFuture<'static, ()>>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum EventKind {
    Emit,
    Waterfall,
    Serial,
    Bail,
    Parallel,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct EventKey {
    event: TypeId,
    kind: EventKind,
}

impl EventKey {
    fn new<E: Event>(kind: EventKind) -> Self {
        Self {
            event: TypeId::of::<E>(),
            kind,
        }
    }
}

impl fmt::Display for EventKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "event {:?}/{:?}", self.event, self.kind)
    }
}

struct ListenerSlot<F> {
    id: u64,
    once: Cell<bool>,
    f: F,
}

struct ErasedListener {
    id: u64,
    slot: Rc<dyn Any>,
}

/// The shared event bus. One instance is shared by a root context and all of
/// its scopes.
pub struct EventBus {
    listeners: RefCell<HashMap<EventKey, Vec<ErasedListener>>>,
    next_id: Cell<u64>,
}

impl Default for EventBus {
    fn default() -> Self {
        Self::new()
    }
}

impl EventBus {
    pub(crate) fn new() -> Self {
        Self {
            listeners: RefCell::new(HashMap::new()),
            next_id: Cell::new(1),
        }
    }

    fn insert<F: 'static>(&self, key: EventKey, f: F, prepend: bool, once: bool) -> u64 {
        let id = self.next_id.get();
        self.next_id.set(id + 1);
        let slot = Rc::new(ListenerSlot {
            id,
            once: Cell::new(once),
            f,
        });
        let erased_slot: Rc<dyn Any> = slot;
        let erased = ErasedListener {
            id,
            slot: erased_slot,
        };
        let mut listeners = self.listeners.borrow_mut();
        let list = listeners.entry(key).or_default();
        if prepend {
            list.insert(0, erased);
        } else {
            list.push(erased);
        }
        id
    }

    pub(crate) fn remove(&self, key: EventKey, id: u64) -> bool {
        let mut listeners = self.listeners.borrow_mut();
        let Some(list) = listeners.get_mut(&key) else {
            return false;
        };
        let before = list.len();
        list.retain(|entry| entry.id != id);
        let removed = list.len() != before;
        if list.is_empty() {
            listeners.remove(&key);
        }
        removed
    }

    fn snapshot<F: 'static>(&self, key: EventKey) -> Vec<Rc<ListenerSlot<F>>> {
        let listeners = self.listeners.borrow();
        listeners
            .get(&key)
            .map(|list| {
                list.iter()
                    .filter_map(|entry| entry.slot.clone().downcast::<ListenerSlot<F>>().ok())
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn add_emit<E: EmitEvent>(
        &self,
        f: EmitFn<E>,
        prepend: bool,
        once: bool,
    ) -> (EventKey, u64) {
        let key = EventKey::new::<E>(EventKind::Emit);
        (key, self.insert(key, f, prepend, once))
    }

    pub(crate) fn add_waterfall<E: WaterfallEvent>(
        &self,
        f: WaterfallFn<E>,
        prepend: bool,
        once: bool,
    ) -> (EventKey, u64) {
        let key = EventKey::new::<E>(EventKind::Waterfall);
        (key, self.insert(key, f, prepend, once))
    }

    pub(crate) fn add_serial<E: SerialEvent>(
        &self,
        f: SerialFn<E>,
        prepend: bool,
        once: bool,
    ) -> (EventKey, u64) {
        let key = EventKey::new::<E>(EventKind::Serial);
        (key, self.insert(key, f, prepend, once))
    }

    pub(crate) fn add_bail<E: BailEvent>(
        &self,
        f: BailFn<E>,
        prepend: bool,
        once: bool,
    ) -> (EventKey, u64) {
        let key = EventKey::new::<E>(EventKind::Bail);
        (key, self.insert(key, f, prepend, once))
    }

    pub(crate) fn add_parallel<E: ParallelEvent>(
        &self,
        f: ParallelFn<E>,
        prepend: bool,
        once: bool,
    ) -> (EventKey, u64) {
        let key = EventKey::new::<E>(EventKind::Parallel);
        (key, self.insert(key, f, prepend, once))
    }

    /// Remove `once` listeners from the bus before their dispatch starts.
    ///
    /// The dispatch's snapshot still holds an `Rc` to each listener, so those
    /// listeners run in this dispatch (at most once) but re-entrant or later
    /// dispatches cannot see them again.
    fn remove_once<F: 'static>(&self, key: EventKey, slots: &[Rc<ListenerSlot<F>>]) {
        for slot in slots {
            if slot.once.get() {
                self.remove(key, slot.id);
            }
        }
    }

    /// Synchronously call every listener; return values are ignored.
    pub fn emit<E: EmitEvent>(&self, args: E::Args) {
        let key = EventKey::new::<E>(EventKind::Emit);
        let slots = self.snapshot::<EmitFn<E>>(key);
        self.remove_once(key, &slots);
        for slot in slots {
            (slot.f)(args.clone());
        }
    }

    /// Call every listener concurrently and wait for all of them.
    pub async fn parallel<E: ParallelEvent>(&self, args: E::Args) {
        let key = EventKey::new::<E>(EventKind::Parallel);
        let slots = self.snapshot::<ParallelFn<E>>(key);
        self.remove_once(key, &slots);
        let futures = slots.iter().map(|slot| (slot.f)(args.clone()));
        futures::future::join_all(futures).await;
    }

    /// Await listeners in registration order, stopping at the first bail.
    pub async fn serial<E: SerialEvent>(&self, args: E::Args) -> Option<E::Out> {
        let key = EventKey::new::<E>(EventKind::Serial);
        let slots = self.snapshot::<SerialFn<E>>(key);
        self.remove_once(key, &slots);
        for slot in slots {
            let value = (slot.f)(args.clone()).await;
            if value.is_some() {
                return value;
            }
        }
        None
    }

    /// Call listeners in registration order, stopping at the first bail.
    pub fn bail<E: BailEvent>(&self, args: E::Args) -> Option<E::Out> {
        let key = EventKey::new::<E>(EventKind::Bail);
        let slots = self.snapshot::<BailFn<E>>(key);
        self.remove_once(key, &slots);
        for slot in slots {
            let value = (slot.f)(args.clone());
            if value.is_some() {
                return value;
            }
        }
        None
    }

    /// Compose listeners around `default` as around-middleware.
    ///
    /// The caller passes the built-in behavior; every listener receives a
    /// [`Next`] that runs the rest of the chain. A listener that returns
    /// without calling `next` vetoes the default behavior.
    ///
    /// `once` listeners are removed when the dispatch starts, so they take
    /// part in this chain (its snapshot keeps them alive) but never in a later
    /// dispatch. Within one dispatch a `once` link runs at most once, even if
    /// an upstream listener calls [`Next::next`] more than once.
    pub fn waterfall<E: WaterfallEvent>(
        &self,
        args: E::Args,
        default: impl Fn(E::Args) -> LocalBoxFuture<'static, E::Out> + 'static,
    ) -> LocalBoxFuture<'static, E::Out> {
        let key = EventKey::new::<E>(EventKind::Waterfall);
        let slots = self.snapshot::<WaterfallFn<E>>(key);

        // Remove once listeners up front; the chain below owns its own clones
        // so they still run in this dispatch. This also covers the veto case:
        // once removed, the listener never runs on a later dispatch.
        self.remove_once(key, &slots);

        let chain: Rc<Vec<WaterfallLink<E>>> = Rc::new(
            slots
                .into_iter()
                .map(|slot| {
                    Rc::new(WaterfallLinkInner {
                        once: slot.once.get(),
                        fired: Cell::new(false),
                        f: slot.f.clone(),
                    })
                })
                .collect(),
        );
        let default: Rc<dyn Fn(E::Args) -> LocalBoxFuture<'static, E::Out>> = Rc::new(default);
        run_waterfall_chain(chain, 0, args, default)
    }
}

type WaterfallLink<E> = Rc<WaterfallLinkInner<E>>;

struct WaterfallLinkInner<E: WaterfallEvent> {
    once: bool,
    /// Per-dispatch flag; the chain is rebuilt for every dispatch.
    fired: Cell<bool>,
    f: WaterfallFn<E>,
}

fn run_waterfall_chain<E: WaterfallEvent>(
    chain: Rc<Vec<WaterfallLink<E>>>,
    start: usize,
    args: E::Args,
    default: Rc<dyn Fn(E::Args) -> LocalBoxFuture<'static, E::Out>>,
) -> LocalBoxFuture<'static, E::Out> {
    Box::pin(async move {
        let mut index = start;
        while let Some(link) = chain.get(index) {
            if link.once && link.fired.replace(true) {
                index += 1;
                continue;
            }
            let next = Next {
                chain: chain.clone(),
                index: index + 1,
                default: default.clone(),
            };
            return (link.f)(args, next).await;
        }
        default(args).await
    })
}

/// Continuation passed to a `waterfall` listener.
pub struct Next<E: WaterfallEvent> {
    chain: Rc<Vec<WaterfallLink<E>>>,
    index: usize,
    default: Rc<dyn Fn(E::Args) -> LocalBoxFuture<'static, E::Out>>,
}

impl<E: WaterfallEvent> Next<E> {
    /// Run the next listener (or the built-in default at the end of the chain).
    pub fn next(&self, args: E::Args) -> LocalBoxFuture<'static, E::Out> {
        run_waterfall_chain(self.chain.clone(), self.index, args, self.default.clone())
    }
}

/// Handle returned by `ctx.on*`; removes the listener when disposed.
pub struct ListenerHandle {
    bus: Rc<EventBus>,
    key: EventKey,
    id: u64,
    disposed: Cell<bool>,
}

impl ListenerHandle {
    pub(crate) fn new(bus: Rc<EventBus>, key: EventKey, id: u64) -> Self {
        Self {
            bus,
            key,
            id,
            disposed: Cell::new(false),
        }
    }

    /// Remove the listener. Returns whether it was still registered.
    pub fn dispose(&self) -> bool {
        if self.disposed.replace(true) {
            return false;
        }
        self.bus.remove(self.key, self.id)
    }
}

impl fmt::Debug for ListenerHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ListenerHandle")
            .field("key", &self.key)
            .field("id", &self.id)
            .finish()
    }
}
