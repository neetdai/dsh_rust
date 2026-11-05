//! Independent conformance suite for the Cordis kernel (A1–A4).
//!
//! These tests are written against the public API contract only and do not
//! reuse helpers from `tests/kernel.rs`. The same file is compiled against
//! both runtimes through the local `Backend` alias:
//!
//! * Tokio (default): `cargo test -p dsh-cordis --test conformance --offline`
//! * compio: `cargo test -p dsh-cordis --features compio --test conformance --offline`
//!
//! Service-type errors are unrepresentable at compile time: `provide`, `get`
//! and `require` only accept `ServiceKey<T>`, and the storage key embeds
//! `TypeId::of::<T>()`. There is no way to name a service as the wrong Rust
//! type, so no runtime-mismatch test (and no `trybuild`) is needed.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use dsh_cordis::{
    Config, Context, CordisError, EmitEvent, Event, EventOptions, FiberHandle, FiberStatus,
    FnPlugin, PluginError, SerialEvent, ServiceKey, ServiceKeyId, WaterfallEvent,
};
use dsh_runtime::{Runtime, RuntimeExt};

#[cfg(feature = "compio")]
use dsh_runtime::CompioRuntime as Backend;
#[cfg(not(feature = "compio"))]
use dsh_runtime::TokioRuntime as Backend;

/// Run one scenario on a fresh [Backend] runtime and root context.
fn run<F, Fut>(scenario: F)
where
    F: FnOnce(Context) -> Fut + 'static,
    Fut: Future<Output = ()> + 'static,
{
    let runtime: Rc<dyn Runtime> = Rc::new(Backend::new().expect("runtime"));
    let root = Context::root(runtime.clone());
    runtime.block_on(Box::pin(async move {
        scenario(root).await;
    }));
}

fn plugin_error(error: CordisError) -> PluginError {
    PluginError::new(error.to_string())
}

/// Yield once so tasks cancelled by `dispose` get polled and dropped.
async fn yield_once() {
    let mut yielded = false;
    futures::future::poll_fn(move |cx| {
        if yielded {
            std::task::Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            std::task::Poll::Pending
        }
    })
    .await;
}

// ---------------------------------------------------------------------------
// 1. Typed `ServiceKey<T>`
// ---------------------------------------------------------------------------

struct Counter {
    value: u32,
}

const COUNTER: ServiceKey<Counter> = ServiceKey::new("conf-counter");
/// Same Cordis name as [`COUNTER`] but another Rust type: a distinct key.
const COUNTER_TEXT: ServiceKey<String> = ServiceKey::new("conf-counter");
const MISSING: ServiceKey<u64> = ServiceKey::new("conf-missing");

#[test]
fn typed_service_keys_carry_name_and_type() {
    run(|ctx| async move {
        let handle = ctx
            .provide(COUNTER, Arc::new(Counter { value: 3 }))
            .expect("provide");
        assert_eq!(handle.key(), COUNTER.id());
        assert_eq!(handle.key().name(), "conf-counter");
        assert_eq!(
            handle.key().type_id(),
            std::any::TypeId::of::<Counter>(),
            "the erased id must retain the concrete Rust type"
        );

        assert_eq!(ctx.get(COUNTER).expect("get").value, 3);
        assert_eq!(ctx.require(COUNTER).expect("require").value, 3);

        // A lookup through the same name but a different type never aliases.
        assert_eq!(COUNTER.name(), COUNTER_TEXT.name());
        assert_ne!(COUNTER.id(), COUNTER_TEXT.id());
        assert!(ctx.get(COUNTER_TEXT).is_none());
        assert!(matches!(
            ctx.require(COUNTER_TEXT),
            Err(CordisError::MissingService("conf-counter"))
        ));
        assert!(matches!(
            ctx.require(MISSING),
            Err(CordisError::MissingService("conf-missing"))
        ));

        // Erased keys are constructible from typed keys (used by `inject`).
        let erased: ServiceKeyId = COUNTER.into();
        assert_eq!(erased, COUNTER.id());
        assert_eq!(ServiceKeyId::from(&COUNTER), COUNTER.id());
        assert_eq!(ServiceKeyId::from(&erased), erased);

        // The same name under another type is a separate service.
        ctx.provide(COUNTER_TEXT, Arc::new("text".to_owned()))
            .expect("same name, different type");
        assert_eq!(ctx.get(COUNTER_TEXT).expect("text").as_str(), "text");
        assert_eq!(ctx.get(COUNTER).expect("counter").value, 3);
    });
}

struct Value(u32);
struct ScopedMarker(u32);

const VALUE: ServiceKey<Value> = ServiceKey::new("conf-value");
const SCOPED_MARKER: ServiceKey<ScopedMarker> = ServiceKey::new("conf-scoped-marker");
const UNRELATED: ServiceKey<u32> = ServiceKey::new("conf-unrelated");

#[test]
fn scope_shares_store_and_isolate_hides_parent_per_key() {
    run(|ctx| async move {
        ctx.provide(VALUE, Arc::new(Value(1))).expect("provide");

        // `scope()` shares the nearest store: a service provided through a
        // scoped context is visible to the root and to sibling scopes.
        let scoped = ctx.scope();
        assert_eq!(scoped.get(VALUE).expect("inherited").0, 1);
        scoped
            .provide(SCOPED_MARKER, Arc::new(ScopedMarker(7)))
            .expect("provide through scope");
        assert_eq!(ctx.get(SCOPED_MARKER).expect("shared with root").0, 7);
        assert_eq!(ctx.scope().get(SCOPED_MARKER).expect("sibling").0, 7);
        assert_eq!(ctx.scope().get(VALUE).expect("root service").0, 1);

        // `isolate()` hides the parent's service for that key only.
        let isolated = ctx.isolate(VALUE);
        assert!(isolated.get(VALUE).is_none());
        assert!(matches!(
            isolated.require(VALUE),
            Err(CordisError::MissingService("conf-value"))
        ));

        // A local implementation shadows the parent without touching it.
        let override_handle = isolated
            .provide(VALUE, Arc::new(Value(2)))
            .expect("isolated override");
        assert_eq!(isolated.get(VALUE).expect("override").0, 2);
        assert_eq!(isolated.scope().get(VALUE).expect("scope of isolate").0, 2);
        assert_eq!(ctx.get(VALUE).expect("root untouched").0, 1);

        // Withdrawing the override does not fall back to the parent service.
        override_handle.dispose();
        assert!(isolated.get(VALUE).is_none());

        // Isolation is per key: unrelated keys still resolve through the parent.
        let other_isolated = ctx.isolate(UNRELATED);
        assert_eq!(other_isolated.get(VALUE).expect("parent path").0, 1);
        assert!(other_isolated.get(UNRELATED).is_none());
    });
}

// ---------------------------------------------------------------------------
// 2. Config validation
// ---------------------------------------------------------------------------

#[test]
fn invalid_config_is_rejected_before_any_fiber_exists() {
    run(|ctx| async move {
        let applies = Rc::new(Cell::new(0));
        let applies_in_plugin = applies.clone();
        let plugin = FnPlugin::new("conf-invalid-config", move |_ctx, _config| {
            let applies = applies_in_plugin.clone();
            Box::pin(async move {
                applies.set(applies.get() + 1);
                Ok(())
            })
        })
        .validate_with(|raw: &Config| -> Result<Config, String> {
            if raw.get("mode").and_then(|value| value.as_str()) == Some("valid") {
                Ok(raw.clone())
            } else {
                Err("`mode` must be \"valid\"".to_owned())
            }
        });

        let error = match ctx.plugin_with_config(plugin, serde_json::json!({ "mode": "invalid" })) {
            Ok(_) => panic!("invalid config must be rejected"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, CordisError::InvalidConfig(message) if message.contains("mode")),
            "unexpected error: {error:?}"
        );
        assert_eq!(applies.get(), 0, "apply must not run for an invalid config");

        // `ctx.plugin` funnels `Config::Null` through the same validator.
        let null_applies = Rc::new(Cell::new(0));
        let null_applies_in_plugin = null_applies.clone();
        let null_plugin = FnPlugin::new("conf-null-config", move |_ctx, _config| {
            let applies = null_applies_in_plugin.clone();
            Box::pin(async move {
                applies.set(applies.get() + 1);
                Ok(())
            })
        })
        .validate_with(|raw: &Config| -> Result<Config, String> {
            raw.as_object()
                .map(|_| raw.clone())
                .ok_or_else(|| "config must be an object".to_owned())
        });
        let null_error = match ctx.plugin(null_plugin) {
            Ok(_) => panic!("null config must be rejected"),
            Err(error) => error,
        };
        assert!(matches!(null_error, CordisError::InvalidConfig(_)));
        assert_eq!(null_applies.get(), 0);

        // Nothing was mounted: the first accepted plugin gets fiber id 1 and
        // there is no orphan fiber left to join.
        let next = ctx
            .plugin(FnPlugin::new("conf-after-reject", |_, _| {
                Box::pin(async { Ok(()) })
            }))
            .expect("mount after rejection");
        assert_eq!(next.id(), 1, "a rejected config must not allocate a fiber");
        next.join().await.expect("join");
        assert_eq!(next.status(), FiberStatus::Active);
    });
}

#[test]
fn valid_config_is_normalized_before_apply() {
    run(|ctx| async move {
        let validations = Arc::new(AtomicUsize::new(0));
        let seen = Rc::new(RefCell::new(Vec::<Config>::new()));

        let validation_count = validations.clone();
        let seen_in_plugin = seen.clone();
        let plugin = FnPlugin::new("conf-normalized", move |_ctx, config| {
            seen_in_plugin.borrow_mut().push(config);
            Box::pin(async { Ok(()) })
        })
        .validate_with(Arc::new(move |raw: &Config| {
            validation_count.fetch_add(1, Ordering::Relaxed);
            let retries = raw
                .get("retries")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| "`retries` must be an integer".to_owned())?;
            let mut normalized = raw.clone();
            normalized["limit"] = (retries * 2).into();
            Ok(normalized)
        }));

        let handle = ctx
            .plugin_with_config(plugin, serde_json::json!({ "retries": 3 }))
            .expect("valid config");
        handle.join().await.expect("join");

        assert_eq!(validations.load(Ordering::Relaxed), 1);
        assert_eq!(seen.borrow().len(), 1);
        assert_eq!(seen.borrow()[0]["retries"], serde_json::json!(3));
        assert_eq!(
            seen.borrow()[0]["limit"],
            serde_json::json!(6),
            "apply must receive the validator's normalized config"
        );
    });
}

// ---------------------------------------------------------------------------
// 3. Event registration options
// ---------------------------------------------------------------------------

struct ConfPing;
impl Event for ConfPing {
    type Args = u32;
    const NAME: &'static str = "conf-ping";
}
impl EmitEvent for ConfPing {}

#[test]
fn event_default_options_and_prepend_order() {
    run(|ctx| async move {
        let log = Rc::new(RefCell::new(Vec::<(&'static str, u32)>::new()));

        for entry in ["tail-a", "tail-b"] {
            let log = log.clone();
            ctx.on::<ConfPing>(move |value| log.borrow_mut().push((entry, value)))
                .expect("tail listener");
        }
        for entry in ["head-a", "head-b"] {
            let log = log.clone();
            ctx.on_with::<ConfPing>(
                EventOptions {
                    prepend: true,
                    once: false,
                },
                move |value| log.borrow_mut().push((entry, value)),
            )
            .expect("prepend listener");
        }

        ctx.emit::<ConfPing>(1);
        assert_eq!(
            &*log.borrow(),
            &[("head-b", 1), ("head-a", 1), ("tail-a", 1), ("tail-b", 1)],
            "prepend goes to the head, later prepends run first; `on` stays at the tail"
        );

        // Default options are not `once`: every listener survives to the next
        // dispatch (no regression of the plain `on` semantics).
        ctx.emit::<ConfPing>(2);
        assert_eq!(log.borrow().len(), 8);
        assert_eq!(
            &log.borrow()[4..],
            &[("head-b", 2), ("head-a", 2), ("tail-a", 2), ("tail-b", 2)]
        );
    });
}

struct ConfStep;
impl Event for ConfStep {
    type Args = u32;
    const NAME: &'static str = "conf-step";
}
impl WaterfallEvent for ConfStep {
    type Out = u32;
}

#[test]
fn waterfall_once_listener_runs_at_most_once() {
    run(|ctx| async move {
        let hits = Rc::new(Cell::new(0));
        let hits_in_once = hits.clone();

        // Upstream link calls `next` twice per dispatch; the `once` link sits
        // between it and the default.
        ctx.on_waterfall::<ConfStep>(|args, next| {
            Box::pin(async move {
                let first = next.next(args).await;
                let second = next.next(args).await;
                first + second
            })
        })
        .expect("upstream listener");

        ctx.on_waterfall_with::<ConfStep>(
            EventOptions {
                prepend: false,
                once: true,
            },
            move |args, next| {
                let hits = hits_in_once.clone();
                Box::pin(async move {
                    hits.set(hits.get() + 1);
                    next.next(args).await + 1
                })
            },
        )
        .expect("once listener");

        let first = ctx
            .waterfall::<ConfStep>(1, |args| Box::pin(async move { args + 10 }))
            .await;
        assert_eq!(
            first, 23,
            "first next: 12, second next: 11 (the once link is skipped)"
        );
        assert_eq!(
            hits.get(),
            1,
            "a once listener runs at most once inside one dispatch"
        );

        let second = ctx
            .waterfall::<ConfStep>(1, |args| Box::pin(async move { args + 10 }))
            .await;
        assert_eq!(
            second, 22,
            "after removal both next calls reach the default directly"
        );
        assert_eq!(
            hits.get(),
            1,
            "a once listener must not survive into the next dispatch"
        );
    });
}

#[test]
fn waterfall_once_listener_is_removed_even_when_upstream_vetoes() {
    run(|ctx| async move {
        let veto = Rc::new(Cell::new(true));
        let hits = Rc::new(Cell::new(0));
        let veto_in_upstream = veto.clone();
        let hits_in_once = hits.clone();

        ctx.on_waterfall::<ConfStep>(move |_args, next| {
            let veto = veto_in_upstream.clone();
            Box::pin(async move { if veto.get() { 7 } else { next.next(1).await } })
        })
        .expect("upstream listener");

        ctx.on_waterfall_with::<ConfStep>(
            EventOptions {
                prepend: false,
                once: true,
            },
            move |args, next| {
                let hits = hits_in_once.clone();
                Box::pin(async move {
                    hits.set(hits.get() + 1);
                    next.next(args).await + 1
                })
            },
        )
        .expect("once listener");

        // Upstream vetoes: the default and the once listener are not reached,
        // but the once listener must be evicted when the dispatch starts.
        let first = ctx
            .waterfall::<ConfStep>(1, |args| Box::pin(async move { args + 10 }))
            .await;
        assert_eq!(first, 7);
        assert_eq!(hits.get(), 0);

        veto.set(false);
        let second = ctx
            .waterfall::<ConfStep>(1, |args| Box::pin(async move { args + 10 }))
            .await;
        assert_eq!(
            second, 11,
            "the vetoed once listener must not reappear in a later dispatch"
        );
        assert_eq!(hits.get(), 0);
    });
}

struct ConfSerial;
impl Event for ConfSerial {
    type Args = u32;
    const NAME: &'static str = "conf-serial";
}
impl SerialEvent for ConfSerial {
    type Out = u32;
}

#[test]
fn serial_prepend_and_once_options_apply_to_awaitable_events() {
    run(|ctx| async move {
        let order = Rc::new(RefCell::new(Vec::<&'static str>::new()));
        let once_hits = Rc::new(Cell::new(0));

        for entry in ["tail-a", "tail-b"] {
            let order = order.clone();
            ctx.on_serial::<ConfSerial>(move |_| {
                let order = order.clone();
                Box::pin(async move {
                    order.borrow_mut().push(entry);
                    None
                })
            })
            .expect("tail listener");
        }
        for entry in ["head-a", "head-b"] {
            let order = order.clone();
            ctx.on_serial_with::<ConfSerial>(
                EventOptions {
                    prepend: true,
                    once: false,
                },
                move |_| {
                    let order = order.clone();
                    Box::pin(async move {
                        order.borrow_mut().push(entry);
                        None
                    })
                },
            )
            .expect("prepend listener");
        }
        let once = once_hits.clone();
        ctx.on_serial_with::<ConfSerial>(
            EventOptions {
                prepend: false,
                once: true,
            },
            move |_| {
                let once = once.clone();
                Box::pin(async move {
                    once.set(once.get() + 1);
                    None
                })
            },
        )
        .expect("once listener");

        assert_eq!(ctx.serial::<ConfSerial>(1).await, None);
        assert_eq!(&*order.borrow(), &["head-b", "head-a", "tail-a", "tail-b"]);
        assert_eq!(once_hits.get(), 1);

        order.borrow_mut().clear();
        assert_eq!(ctx.serial::<ConfSerial>(2).await, None);
        assert_eq!(
            &*order.borrow(),
            &["head-b", "head-a", "tail-a", "tail-b"],
            "the once listener must not run in the second dispatch"
        );
        assert_eq!(once_hits.get(), 1);
    });
}

// ---------------------------------------------------------------------------
// 4. `inject` reload lifecycle
// ---------------------------------------------------------------------------

struct ConfTools;
const CONF_TOOLS: ServiceKey<ConfTools> = ServiceKey::new("conf-tools");

struct ConfTick;
impl Event for ConfTick {
    type Args = ();
    const NAME: &'static str = "conf-tick";
}
impl EmitEvent for ConfTick {}

#[test]
fn inject_reload_after_provider_replacement_and_dispose_is_terminal() {
    run(|ctx| async move {
        let applies = Rc::new(Cell::new(0));
        let hits = Rc::new(Cell::new(0));
        let cleaned = Rc::new(Cell::new(0));
        let validations = Arc::new(AtomicUsize::new(0));

        let applies_in_plugin = applies.clone();
        let hits_in_plugin = hits.clone();
        let cleaned_in_plugin = cleaned.clone();
        let validation_count = validations.clone();

        let consumer = FnPlugin::new("conf-consumer", move |ctx, _config| {
            let applies = applies_in_plugin.clone();
            let hits = hits_in_plugin.clone();
            let cleaned = cleaned_in_plugin.clone();
            Box::pin(async move {
                applies.set(applies.get() + 1);
                ctx.on::<ConfTick>(move |_| hits.set(hits.get() + 1))
                    .map_err(plugin_error)?;
                ctx.effect(move || Some(Box::new(move || cleaned.set(cleaned.get() + 1))))
                    .map_err(plugin_error)?;
                Ok(())
            })
        })
        .inject([CONF_TOOLS])
        .validate_with(Arc::new(move |raw: &Config| {
            validation_count.fetch_add(1, Ordering::Relaxed);
            Ok(raw.clone())
        }));

        let consumer_handle = ctx
            .plugin_with_config(consumer, serde_json::json!({ "label": "consumer" }))
            .expect("mount consumer");
        assert_eq!(consumer_handle.status(), FiberStatus::Pending);

        let provider_one = ctx
            .plugin(FnPlugin::new("conf-provider-one", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(CONF_TOOLS, Arc::new(ConfTools))
                        .map_err(plugin_error)?;
                    Ok(())
                })
            }))
            .expect("provider one");
        provider_one.join().await.expect("provider one join");
        consumer_handle.join().await.expect("consumer join");

        assert_eq!(consumer_handle.status(), FiberStatus::Active);
        assert_eq!(applies.get(), 1);
        assert_eq!(validations.load(Ordering::Relaxed), 1);
        ctx.emit::<ConfTick>(());
        assert_eq!(hits.get(), 1);

        // Removing the provider unloads the dependent to Pending; its effects
        // and listeners from the first `apply` are torn down.
        provider_one.dispose().await;
        assert_eq!(consumer_handle.status(), FiberStatus::Pending);
        assert_eq!(
            cleaned.get(),
            1,
            "unload must run the first apply's disposers"
        );
        ctx.emit::<ConfTick>(());
        assert_eq!(hits.get(), 1, "the old listener must be gone after unload");
        assert_eq!(
            validations.load(Ordering::Relaxed),
            1,
            "unload must not re-run the validator"
        );

        // A replacement provider wakes the pending fiber automatically.
        let provider_two = ctx
            .plugin(FnPlugin::new("conf-provider-two", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(CONF_TOOLS, Arc::new(ConfTools))
                        .map_err(plugin_error)?;
                    Ok(())
                })
            }))
            .expect("provider two");
        provider_two.join().await.expect("provider two join");
        consumer_handle.join().await.expect("consumer reload join");

        assert_eq!(consumer_handle.status(), FiberStatus::Active);
        assert_eq!(applies.get(), 2, "the dependent must re-apply");
        assert_eq!(
            validations.load(Ordering::Relaxed),
            1,
            "reload reuses the normalized config"
        );
        ctx.emit::<ConfTick>(());
        assert_eq!(hits.get(), 2, "only the new apply's listener may fire");

        // Explicit dispose stays terminal: later providers cannot resurrect it.
        consumer_handle.dispose().await;
        assert_eq!(consumer_handle.status(), FiberStatus::Disposed);
        assert!(consumer_handle.join().await.is_err());
        assert_eq!(cleaned.get(), 2);

        provider_two.dispose().await;

        let provider_three = ctx
            .plugin(FnPlugin::new("conf-provider-three", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(CONF_TOOLS, Arc::new(ConfTools))
                        .map_err(plugin_error)?;
                    Ok(())
                })
            }))
            .expect("provider three");
        provider_three.join().await.expect("provider three join");
        assert_eq!(
            consumer_handle.status(),
            FiberStatus::Disposed,
            "explicit dispose is terminal"
        );
        ctx.emit::<ConfTick>(());
        assert_eq!(hits.get(), 2);
        assert_eq!(applies.get(), 2);
    });
}

// ---------------------------------------------------------------------------
// 5. Lifecycle boundaries
// ---------------------------------------------------------------------------

struct ConfSlow;
const CONF_SLOW: ServiceKey<ConfSlow> = ServiceKey::new("conf-slow");

struct ConfLoadTick;
impl Event for ConfLoadTick {
    type Args = ();
    const NAME: &'static str = "conf-load-tick";
}
impl EmitEvent for ConfLoadTick {}

#[test]
fn dispose_while_loading_is_terminal_and_rolls_back() {
    run(|ctx| async move {
        let hits = Rc::new(Cell::new(0));
        let hits_in_plugin = hits.clone();
        let (entered_tx, entered_rx) = futures::channel::oneshot::channel::<()>();
        let entered_tx = Rc::new(RefCell::new(Some(entered_tx)));

        let plugin = FnPlugin::new("conf-loading", move |ctx, _config| {
            let hits = hits_in_plugin.clone();
            let entered_tx = entered_tx.clone();
            Box::pin(async move {
                ctx.provide(CONF_SLOW, Arc::new(ConfSlow))
                    .map_err(plugin_error)?;
                ctx.on::<ConfLoadTick>(move |_| hits.set(hits.get() + 1))
                    .map_err(plugin_error)?;
                if let Some(tx) = entered_tx.borrow_mut().take() {
                    let _ = tx.send(());
                }
                std::future::pending::<()>().await;
                Ok(())
            })
        });

        let handle = ctx.plugin(plugin).expect("mount");
        // `start` moves the fiber to Loading synchronously; the spawned apply
        // future then parks on `pending` with its effects already registered.
        assert_eq!(handle.status(), FiberStatus::Loading);
        entered_rx.await.expect("apply must be entered");
        assert_eq!(handle.status(), FiberStatus::Loading);

        ctx.emit::<ConfLoadTick>(());
        assert_eq!(hits.get(), 1, "the listener is live before apply completes");

        handle.dispose().await;
        assert_eq!(handle.status(), FiberStatus::Disposed);
        assert!(handle.join().await.is_err());

        // Let the aborted task settle; its rolled-back effects must be gone.
        for _ in 0..3 {
            yield_once().await;
        }
        ctx.emit::<ConfLoadTick>(());
        assert_eq!(hits.get(), 1, "dispose must remove the listener");
        assert_eq!(handle.status(), FiberStatus::Disposed);

        // The service entry was removed as well: a fresh provider can claim
        // the same key (a leaked entry would fail with ServiceAlreadyProvided).
        let replacement = ctx
            .plugin(FnPlugin::new("conf-loading-replacement", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(CONF_SLOW, Arc::new(ConfSlow))
                        .map_err(plugin_error)?;
                    Ok(())
                })
            }))
            .expect("replacement provider");
        replacement.join().await.expect("replacement join");
        assert_eq!(replacement.status(), FiberStatus::Active);
        assert!(ctx.get(CONF_SLOW).is_some());
    });
}

struct ConfFailSvc;
const CONF_FAIL_SVC: ServiceKey<ConfFailSvc> = ServiceKey::new("conf-fail-svc");

struct ConfFailTick;
impl Event for ConfFailTick {
    type Args = ();
    const NAME: &'static str = "conf-fail-tick";
}
impl EmitEvent for ConfFailTick {}

struct ConfChildTick;
impl Event for ConfChildTick {
    type Args = ();
    const NAME: &'static str = "conf-child-tick";
}
impl EmitEvent for ConfChildTick {}

#[test]
fn failed_apply_rolls_back_services_listeners_children_and_effects() {
    run(|ctx| async move {
        let hits = Rc::new(Cell::new(0));
        let cleaned = Rc::new(Cell::new(false));
        let child_hits = Rc::new(Cell::new(0));
        let child_slot: Rc<RefCell<Option<FiberHandle>>> = Rc::new(RefCell::new(None));

        let hits_in_plugin = hits.clone();
        let cleaned_in_plugin = cleaned.clone();
        let child_hits_in_plugin = child_hits.clone();
        let child_slot_in_plugin = child_slot.clone();

        let plugin = FnPlugin::new("conf-failing", move |ctx, _config| {
            let hits = hits_in_plugin.clone();
            let cleaned = cleaned_in_plugin.clone();
            let child_hits = child_hits_in_plugin.clone();
            let child_slot = child_slot_in_plugin.clone();
            Box::pin(async move {
                ctx.provide(CONF_FAIL_SVC, Arc::new(ConfFailSvc))
                    .map_err(plugin_error)?;
                ctx.on::<ConfFailTick>(move |_| hits.set(hits.get() + 1))
                    .map_err(plugin_error)?;
                ctx.effect(move || Some(Box::new(move || cleaned.set(true))))
                    .map_err(plugin_error)?;
                let child = ctx
                    .plugin(FnPlugin::new("conf-failing-child", move |ctx, _| {
                        let child_hits = child_hits.clone();
                        Box::pin(async move {
                            ctx.on::<ConfChildTick>(move |_| child_hits.set(child_hits.get() + 1))
                                .map_err(plugin_error)?;
                            Ok(())
                        })
                    }))
                    .map_err(plugin_error)?;
                *child_slot.borrow_mut() = Some(child);
                Err(PluginError::new("intentional failure"))
            })
        });

        let handle = ctx.plugin(plugin).expect("mount");
        let error = handle.join().await.expect_err("apply must fail");
        assert!(error.message().contains("intentional"));
        assert_eq!(handle.status(), FiberStatus::Failed);
        assert!(cleaned.get(), "failed apply must run its effect disposers");

        ctx.emit::<ConfFailTick>(());
        assert_eq!(hits.get(), 0, "failed apply must remove its listener");

        let child = child_slot
            .borrow()
            .clone()
            .expect("the failing apply mounted a child");
        assert_eq!(
            child.status(),
            FiberStatus::Disposed,
            "children mounted by a failed apply must be disposed"
        );
        ctx.emit::<ConfChildTick>(());
        assert_eq!(child_hits.get(), 0);

        // Rolled-back service entry: a fresh provider can install the key.
        let replacement = ctx
            .plugin(FnPlugin::new("conf-failing-replacement", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(CONF_FAIL_SVC, Arc::new(ConfFailSvc))
                        .map_err(plugin_error)?;
                    Ok(())
                })
            }))
            .expect("replacement provider");
        replacement.join().await.expect("replacement join");
        assert_eq!(replacement.status(), FiberStatus::Active);
        assert!(ctx.get(CONF_FAIL_SVC).is_some());
    });
}

struct ConfParentTick;
impl Event for ConfParentTick {
    type Args = ();
    const NAME: &'static str = "conf-parent-tick";
}
impl EmitEvent for ConfParentTick {}

#[test]
fn child_plugin_is_disposed_with_parent() {
    run(|ctx| async move {
        let parent_hits = Rc::new(Cell::new(0));
        let child_hits = Rc::new(Cell::new(0));
        let child_slot: Rc<RefCell<Option<FiberHandle>>> = Rc::new(RefCell::new(None));

        let parent_hits_in_plugin = parent_hits.clone();
        let child_hits_in_plugin = child_hits.clone();
        let child_slot_in_plugin = child_slot.clone();

        let parent_plugin = FnPlugin::new("conf-parent", move |ctx, _config| {
            let parent_hits = parent_hits_in_plugin.clone();
            let child_hits = child_hits_in_plugin.clone();
            let child_slot = child_slot_in_plugin.clone();
            Box::pin(async move {
                ctx.on::<ConfParentTick>(move |_| parent_hits.set(parent_hits.get() + 1))
                    .map_err(plugin_error)?;
                let child = ctx
                    .plugin(FnPlugin::new("conf-child", move |ctx, _| {
                        let child_hits = child_hits.clone();
                        Box::pin(async move {
                            ctx.on::<ConfChildTick>(move |_| child_hits.set(child_hits.get() + 1))
                                .map_err(plugin_error)?;
                            Ok(())
                        })
                    }))
                    .map_err(plugin_error)?;
                *child_slot.borrow_mut() = Some(child);
                Ok(())
            })
        });

        let parent = ctx.plugin(parent_plugin).expect("mount parent");
        parent.join().await.expect("parent join");
        let child = child_slot.borrow().clone().expect("child mounted");
        child.join().await.expect("child join");
        assert_eq!(child.status(), FiberStatus::Active);

        ctx.emit::<ConfParentTick>(());
        ctx.emit::<ConfChildTick>(());
        assert_eq!(parent_hits.get(), 1);
        assert_eq!(child_hits.get(), 1);

        parent.dispose().await;
        assert_eq!(parent.status(), FiberStatus::Disposed);
        assert_eq!(
            child.status(),
            FiberStatus::Disposed,
            "disposing a parent must dispose its children"
        );

        ctx.emit::<ConfParentTick>(());
        ctx.emit::<ConfChildTick>(());
        assert_eq!(parent_hits.get(), 1);
        assert_eq!(child_hits.get(), 1);
    });
}

// ---------------------------------------------------------------------------
// 7. Repair-round regressions (R1-1 Loading races, R1-3 once re-entrancy)
// ---------------------------------------------------------------------------

struct ConfSleepSvc;
const CONF_SLEEP_SVC: ServiceKey<ConfSleepSvc> = ServiceKey::new("conf-sleep-svc");

struct ConfSleepTick;
impl Event for ConfSleepTick {
    type Args = ();
    const NAME: &'static str = "conf-sleep-tick";
}
impl EmitEvent for ConfSleepTick {}

/// R1-1 variant: the dependent's `apply` is suspended on a runtime sleep when
/// its provider disappears. The Loading fiber must be unloaded to Pending and
/// the stale apply must never activate it afterwards.
#[test]
fn loading_dependent_sleeping_apply_unloads_to_pending() {
    run(|ctx| async move {
        let applies = Rc::new(Cell::new(0));
        let hits = Rc::new(Cell::new(0));
        let (entered_tx, entered_rx) = futures::channel::oneshot::channel::<()>();
        let entered_tx = Rc::new(RefCell::new(Some(entered_tx)));

        let applies_in_plugin = applies.clone();
        let hits_in_plugin = hits.clone();
        let consumer = FnPlugin::new("conf-sleeping-consumer", move |ctx, _config| {
            let applies = applies_in_plugin.clone();
            let hits = hits_in_plugin.clone();
            let entered_tx = entered_tx.clone();
            Box::pin(async move {
                applies.set(applies.get() + 1);
                ctx.on::<ConfSleepTick>(move |_| hits.set(hits.get() + 1))
                    .map_err(plugin_error)?;
                if let Some(tx) = entered_tx.borrow_mut().take() {
                    let _ = tx.send(());
                }
                ctx.runtime().sleep(Duration::from_millis(50)).await;
                Ok(())
            })
        })
        .inject([CONF_SLEEP_SVC]);

        let provider_one = ctx
            .plugin(FnPlugin::new("conf-sleep-provider-one", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(CONF_SLEEP_SVC, Arc::new(ConfSleepSvc))
                        .map_err(plugin_error)?;
                    Ok(())
                })
            }))
            .expect("provider one");
        provider_one.join().await.expect("provider one join");

        let consumer_handle = ctx.plugin(consumer).expect("mount consumer");
        assert_eq!(consumer_handle.status(), FiberStatus::Loading);
        entered_rx.await.expect("apply entered its sleep");
        assert_eq!(consumer_handle.status(), FiberStatus::Loading);

        // The provider disappears while the dependent's apply is sleeping.
        provider_one.dispose().await;
        assert_eq!(
            consumer_handle.status(),
            FiberStatus::Pending,
            "a Loading dependent must be unloaded when its dependency disappears"
        );

        // Outwait the original sleep: the stale apply must not activate the
        // fiber even though its future would have completed by now.
        ctx.runtime().sleep(Duration::from_millis(80)).await;
        for _ in 0..3 {
            yield_once().await;
        }
        assert_eq!(
            consumer_handle.status(),
            FiberStatus::Pending,
            "a stale sleeping apply must not flip the fiber back to Active"
        );
        assert_eq!(applies.get(), 1);
        ctx.emit::<ConfSleepTick>(());
        assert_eq!(
            hits.get(),
            0,
            "the unloaded apply's listener must be torn down"
        );

        // A replacement provider reloads the consumer; this apply completes
        // after its sleep and activates normally.
        let provider_two = ctx
            .plugin(FnPlugin::new("conf-sleep-provider-two", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(CONF_SLEEP_SVC, Arc::new(ConfSleepSvc))
                        .map_err(plugin_error)?;
                    Ok(())
                })
            }))
            .expect("provider two");
        provider_two.join().await.expect("provider two join");
        consumer_handle.join().await.expect("consumer reload join");
        assert_eq!(consumer_handle.status(), FiberStatus::Active);
        assert_eq!(applies.get(), 2);
        ctx.emit::<ConfSleepTick>(());
        assert_eq!(hits.get(), 1);

        provider_two.dispose().await;
        assert_eq!(consumer_handle.status(), FiberStatus::Pending);
    });
}

struct ConfStuckSvc;
const CONF_STUCK_SVC: ServiceKey<ConfStuckSvc> = ServiceKey::new("conf-stuck-svc");

struct ConfStuckTick;
impl Event for ConfStuckTick {
    type Args = ();
    const NAME: &'static str = "conf-stuck-tick";
}
impl EmitEvent for ConfStuckTick {}

/// R1-1 variant: the dependent's `apply` never returns (`pending`). Removing
/// the provider must not leave the fiber Loading forever; an explicit dispose
/// afterwards is terminal.
#[test]
fn loading_dependent_never_returning_apply_unloads_and_dispose_is_terminal() {
    run(|ctx| async move {
        let applies = Rc::new(Cell::new(0));
        let hits = Rc::new(Cell::new(0));
        let (entered_tx, entered_rx) = futures::channel::oneshot::channel::<()>();
        let entered_tx = Rc::new(RefCell::new(Some(entered_tx)));

        let applies_in_plugin = applies.clone();
        let hits_in_plugin = hits.clone();
        let consumer = FnPlugin::new("conf-stuck-consumer", move |ctx, _config| {
            let applies = applies_in_plugin.clone();
            let hits = hits_in_plugin.clone();
            let entered_tx = entered_tx.clone();
            Box::pin(async move {
                applies.set(applies.get() + 1);
                ctx.on::<ConfStuckTick>(move |_| hits.set(hits.get() + 1))
                    .map_err(plugin_error)?;
                if let Some(tx) = entered_tx.borrow_mut().take() {
                    let _ = tx.send(());
                }
                std::future::pending::<()>().await;
                Ok(())
            })
        })
        .inject([CONF_STUCK_SVC]);

        let provider_one = ctx
            .plugin(FnPlugin::new("conf-stuck-provider-one", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(CONF_STUCK_SVC, Arc::new(ConfStuckSvc))
                        .map_err(plugin_error)?;
                    Ok(())
                })
            }))
            .expect("provider one");
        provider_one.join().await.expect("provider one join");

        let consumer_handle = ctx.plugin(consumer).expect("mount consumer");
        assert_eq!(consumer_handle.status(), FiberStatus::Loading);
        entered_rx.await.expect("apply entered its pending await");

        provider_one.dispose().await;
        assert_eq!(
            consumer_handle.status(),
            FiberStatus::Pending,
            "a Loading dependent must not stay stuck in Loading"
        );
        for _ in 0..5 {
            yield_once().await;
        }
        assert_eq!(consumer_handle.status(), FiberStatus::Pending);
        assert_eq!(applies.get(), 1);
        ctx.emit::<ConfStuckTick>(());
        assert_eq!(hits.get(), 0, "the aborted apply's listener must be gone");

        // Explicit dispose is terminal even though the apply never finished.
        consumer_handle.dispose().await;
        assert_eq!(consumer_handle.status(), FiberStatus::Disposed);
        assert!(consumer_handle.join().await.is_err());
        for _ in 0..3 {
            yield_once().await;
        }
        assert_eq!(consumer_handle.status(), FiberStatus::Disposed);

        // A later provider must not resurrect an explicitly disposed fiber.
        let provider_two = ctx
            .plugin(FnPlugin::new("conf-stuck-provider-two", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(CONF_STUCK_SVC, Arc::new(ConfStuckSvc))
                        .map_err(plugin_error)?;
                    Ok(())
                })
            }))
            .expect("provider two");
        provider_two.join().await.expect("provider two join");
        assert_eq!(consumer_handle.status(), FiberStatus::Disposed);
        assert_eq!(applies.get(), 1);
        ctx.emit::<ConfStuckTick>(());
        assert_eq!(hits.get(), 0);
    });
}

struct ConfEcho;
impl Event for ConfEcho {
    type Args = u32;
    const NAME: &'static str = "conf-once-echo";
}
impl EmitEvent for ConfEcho {}

/// R1-3: a `once` emit listener is removed before it is invoked, so a
/// re-entrant `emit` from inside its own callback cannot run it twice.
#[test]
fn once_emit_listener_is_removed_before_reentrant_emit() {
    run(|ctx| async move {
        let once_hits = Rc::new(Cell::new(0));
        let tail_hits = Rc::new(Cell::new(0));
        let inner_ctx = ctx.clone();

        // A persistent tail listener counts every dispatch, including the
        // re-entrant one, proving the nested emit really ran.
        let tail = tail_hits.clone();
        ctx.on::<ConfEcho>(move |_| tail.set(tail.get() + 1))
            .expect("tail listener");

        let once = once_hits.clone();
        ctx.once::<ConfEcho>(move |value| {
            once.set(once.get() + 1);
            if value == 0 {
                inner_ctx.emit::<ConfEcho>(1);
            }
        })
        .expect("once listener");

        ctx.emit::<ConfEcho>(0);
        assert_eq!(
            once_hits.get(),
            1,
            "a re-entrant emit must not run the once listener a second time"
        );
        assert_eq!(
            tail_hits.get(),
            2,
            "the re-entrant emit must still reach persistent listeners"
        );

        ctx.emit::<ConfEcho>(2);
        assert_eq!(once_hits.get(), 1);
        assert_eq!(tail_hits.get(), 3);
    });
}

// ---------------------------------------------------------------------------
// 8. Repair-round regressions (R2-1 stale apply after reload starts)
// ---------------------------------------------------------------------------

struct ConfStaleSvc;
const CONF_STALE_SVC: ServiceKey<ConfStaleSvc> = ServiceKey::new("conf-stale-svc");

struct ConfStaleCheck(u32);
const CONF_STALE_CHECK: ServiceKey<ConfStaleCheck> = ServiceKey::new("conf-stale-check");

struct ConfStaleTick;
impl Event for ConfStaleTick {
    type Args = ();
    const NAME: &'static str = "conf-stale-tick";
}
impl EmitEvent for ConfStaleTick {}

/// R2-1: releasing a parked (aborted) apply after the reload generation owns
/// the fiber must not let it register listeners/services into the reload.
///
/// The first apply parks on a oneshot before registering anything. The test
/// unloads the dependent, releases the stale apply and starts the reload in one
/// uninterrupted synchronous step, so the stale task can only be polled after
/// the replacement generation already owns the fiber.
#[test]
fn stale_apply_does_not_pollute_reloaded_fiber() {
    run(|ctx| async move {
        let applies = Rc::new(Cell::new(0));
        let hits = Rc::new(Cell::new(0));
        let (entered_tx, entered_rx) = futures::channel::oneshot::channel::<()>();
        let (release_tx, release_rx) = futures::channel::oneshot::channel::<()>();
        let entered_tx = Rc::new(RefCell::new(Some(entered_tx)));
        let release_rx = Rc::new(RefCell::new(Some(release_rx)));

        let applies_in_plugin = applies.clone();
        let hits_in_plugin = hits.clone();
        let consumer = FnPlugin::new("conf-stale-consumer", move |ctx, _config| {
            let applies = applies_in_plugin.clone();
            let hits = hits_in_plugin.clone();
            let entered_tx = entered_tx.clone();
            let release_rx = release_rx.clone();
            Box::pin(async move {
                let attempt = applies.get() + 1;
                applies.set(attempt);
                if attempt == 1 {
                    if let Some(tx) = entered_tx.borrow_mut().take() {
                        let _ = tx.send(());
                    }
                    // Drop the `RefMut` before the await point.
                    let release = release_rx.borrow_mut().take();
                    if let Some(rx) = release {
                        let _ = rx.await;
                    }
                }
                // These registrations are the payload of the stale apply. The
                // generation gate must keep them out of the reloaded fiber.
                ctx.on::<ConfStaleTick>(move |_| hits.set(hits.get() + 1))
                    .map_err(plugin_error)?;
                ctx.provide(CONF_STALE_CHECK, Arc::new(ConfStaleCheck(attempt)))
                    .map_err(plugin_error)?;
                Ok(())
            })
        })
        .inject([CONF_STALE_SVC]);

        let provider_one = ctx
            .provide(CONF_STALE_SVC, Arc::new(ConfStaleSvc))
            .expect("provider one");
        let consumer_handle = ctx.plugin(consumer).expect("mount consumer");
        assert_eq!(consumer_handle.status(), FiberStatus::Loading);
        entered_rx.await.expect("first apply entered its park");

        // Remove the provider while the first apply is parked: the dependent is
        // unloaded back to Pending and its apply generation is invalidated.
        provider_one.dispose();
        assert_eq!(consumer_handle.status(), FiberStatus::Pending);
        assert_eq!(applies.get(), 1);

        // Release the stale apply, then start the reload without yielding in
        // between. Both calls are synchronous, so the replacement generation
        // owns the fiber before the stale task can run.
        let _ = release_tx.send(());
        let provider_two = ctx
            .provide(CONF_STALE_SVC, Arc::new(ConfStaleSvc))
            .expect("provider two");
        assert_eq!(consumer_handle.status(), FiberStatus::Loading);

        consumer_handle.join().await.expect("reload join");
        for _ in 0..3 {
            yield_once().await;
        }

        assert_eq!(consumer_handle.status(), FiberStatus::Active);
        assert_eq!(applies.get(), 2);
        assert_eq!(
            ctx.get(CONF_STALE_CHECK).map(|check| check.0),
            Some(2),
            "only the reloaded apply's service may be visible"
        );
        ctx.emit::<ConfStaleTick>(());
        assert_eq!(
            hits.get(),
            1,
            "the stale apply's listener must not survive into the reloaded fiber"
        );

        provider_two.dispose();
        assert_eq!(consumer_handle.status(), FiberStatus::Pending);
    });
}
