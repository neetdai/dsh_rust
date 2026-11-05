use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dsh_cordis::{
    BailEvent, Config, Context, CordisError, EmitEvent, Event, EventOptions, FnPlugin,
    ParallelEvent, Plugin, PluginMeta, SerialEvent, ServiceKey, WaterfallEvent,
};
use dsh_runtime::Runtime;

#[cfg(feature = "compio")]
use dsh_runtime::CompioRuntime as Backend;
#[cfg(not(feature = "compio"))]
use dsh_runtime::TokioRuntime as Backend;

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

/// Yield to the executor once so abort/cleanup tasks get a chance to settle.
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

struct Counter {
    value: u32,
}

const COUNTER: ServiceKey<Counter> = ServiceKey::new("counter");
/// Same Cordis name as [`COUNTER`] but a different Rust type: a distinct key.
const COUNTER_AS_STRING: ServiceKey<String> = ServiceKey::new("counter");

#[test]
fn service_provide_get_and_require() {
    run(|ctx| async move {
        ctx.provide(COUNTER, Arc::new(Counter { value: 3 }))
            .unwrap();
        assert_eq!(ctx.get(COUNTER).unwrap().value, 3);
        // The erased key includes the type id, so the same name with another
        // type never aliases an existing service.
        assert!(ctx.get(COUNTER_AS_STRING).is_none());
        assert!(matches!(
            ctx.require(COUNTER),
            Ok(counter) if counter.value == 3
        ));
        assert!(matches!(
            ctx.require(COUNTER_AS_STRING),
            Err(CordisError::MissingService("counter"))
        ));
    });
}

#[test]
fn duplicate_provide_is_rejected() {
    run(|ctx| async move {
        ctx.provide(COUNTER, Arc::new(Counter { value: 1 }))
            .unwrap();
        assert!(
            ctx.provide(COUNTER, Arc::new(Counter { value: 2 }))
                .is_err()
        );
    });
}

#[test]
fn effect_disposes_reverse_order_and_is_idempotent() {
    run(|ctx| async move {
        let log = Rc::new(RefCell::new(Vec::new()));

        let first = log.clone();
        let handle = ctx
            .effect(move || Some(Box::new(move || first.borrow_mut().push("first"))))
            .unwrap();
        let second = log.clone();
        ctx.effect(move || Some(Box::new(move || second.borrow_mut().push("second"))))
            .unwrap();

        handle.dispose();
        handle.dispose();

        assert_eq!(&*log.borrow(), &["first"]);
    });
}

#[test]
fn fiber_unload_runs_effects_in_reverse_order() {
    run(|ctx| async move {
        let log = Rc::new(RefCell::new(Vec::new()));
        let plugin_log = log.clone();

        let plugin = FnPlugin::new("effects", move |ctx, _| {
            let log = plugin_log.clone();
            Box::pin(async move {
                let first = log.clone();
                ctx.effect(move || Some(Box::new(move || first.borrow_mut().push("a"))))
                    .unwrap();
                let second = log.clone();
                ctx.effect(move || Some(Box::new(move || second.borrow_mut().push("b"))))
                    .unwrap();
                Ok(())
            })
        });

        let handle = ctx.plugin(plugin).unwrap();
        handle.join().await.unwrap();
        handle.dispose().await;

        assert_eq!(&*log.borrow(), &["b", "a"]);
        assert_eq!(handle.status(), dsh_cordis::FiberStatus::Disposed);
    });
}

struct Tools;

const TOOLS: ServiceKey<Tools> = ServiceKey::new("tools");

#[test]
fn inject_waits_for_service_and_reacts_to_removal() {
    run(|ctx| async move {
        let started = Rc::new(Cell::new(false));
        let consumer_started = started.clone();

        let consumer = FnPlugin::new("consumer", move |_ctx, _| {
            consumer_started.set(true);
            Box::pin(async { Ok(()) })
        })
        .inject([TOOLS]);

        let consumer_handle = ctx.plugin(consumer).unwrap();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Pending);

        let provider = FnPlugin::new("provider", |ctx, _| {
            Box::pin(async move {
                ctx.provide(TOOLS, Arc::new(Tools)).unwrap();
                Ok(())
            })
        });
        let provider_handle = ctx.plugin(provider).unwrap();

        provider_handle.join().await.unwrap();
        consumer_handle.join().await.unwrap();
        assert!(started.get());

        provider_handle.dispose().await;
        assert_eq!(
            consumer_handle.status(),
            dsh_cordis::FiberStatus::Pending,
            "a dependent must unload back to Pending, not Disposed"
        );

        // A pending fiber can still be disposed explicitly; it stays terminal.
        consumer_handle.dispose().await;
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Disposed);

        // A later provider must not resurrect an explicitly disposed fiber.
        let replacement = ctx
            .plugin(FnPlugin::new("replacement", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(TOOLS, Arc::new(Tools)).unwrap();
                    Ok(())
                })
            }))
            .unwrap();
        replacement.join().await.unwrap();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Disposed);
    });
}

#[test]
fn waterfall_runs_in_order_and_wraps_default() {
    run(|ctx| async move {
        struct Wf;
        impl Event for Wf {
            type Args = u32;
            const NAME: &'static str = "wf";
        }
        impl WaterfallEvent for Wf {
            type Out = u32;
        }

        let log = Rc::new(RefCell::new(Vec::new()));

        let first = log.clone();
        ctx.on_waterfall::<Wf>(move |args, next| {
            let log = first.clone();
            Box::pin(async move {
                log.borrow_mut().push("a");
                next.next(args).await + 1
            })
        })
        .unwrap();

        let second = log.clone();
        ctx.on_waterfall::<Wf>(move |args, next| {
            let log = second.clone();
            Box::pin(async move {
                log.borrow_mut().push("b");
                next.next(args).await * 2
            })
        })
        .unwrap();

        let out = ctx
            .waterfall::<Wf>(1, |args| Box::pin(async move { args + 10 }))
            .await;

        // a(b(default(1))) = (1 + 10) * 2 + 1
        assert_eq!(out, 23);
        assert_eq!(&*log.borrow(), &["a", "b"]);
    });
}

#[test]
fn waterfall_listener_can_veto_the_default() {
    run(|ctx| async move {
        struct Wf;
        impl Event for Wf {
            type Args = u32;
            const NAME: &'static str = "wf-veto";
        }
        impl WaterfallEvent for Wf {
            type Out = u32;
        }

        ctx.on_waterfall::<Wf>(|_args, _next| Box::pin(async { 99 }))
            .unwrap();

        let out = ctx
            .waterfall::<Wf>(1, |_args| {
                Box::pin(async { panic!("default must not run when vetoed") })
            })
            .await;
        assert_eq!(out, 99);
    });
}

#[test]
fn serial_bails_at_first_value() {
    run(|ctx| async move {
        struct Ser;
        impl Event for Ser {
            type Args = u32;
            const NAME: &'static str = "ser";
        }
        impl SerialEvent for Ser {
            type Out = u32;
        }

        ctx.on_serial::<Ser>(|_| Box::pin(async { None })).unwrap();
        ctx.on_serial::<Ser>(|_| Box::pin(async { Some(7) }))
            .unwrap();
        ctx.on_serial::<Ser>(|_| Box::pin(async { Some(9) }))
            .unwrap();

        assert_eq!(ctx.serial::<Ser>(0).await, Some(7));
    });
}

#[test]
fn bail_stops_at_first_value() {
    run(|ctx| async move {
        struct Stop;
        impl Event for Stop {
            type Args = u32;
            const NAME: &'static str = "stop";
        }
        impl BailEvent for Stop {
            type Out = u32;
        }

        ctx.on_bail::<Stop>(|_| None).unwrap();
        ctx.on_bail::<Stop>(|_| Some(3)).unwrap();
        ctx.on_bail::<Stop>(|_| Some(4)).unwrap();

        assert_eq!(ctx.bail::<Stop>(0), Some(3));
    });
}

#[test]
fn emit_calls_every_listener_in_registration_order() {
    run(|ctx| async move {
        struct Ping;
        impl Event for Ping {
            type Args = u32;
            const NAME: &'static str = "ping";
        }
        impl EmitEvent for Ping {}

        let log = Rc::new(RefCell::new(Vec::new()));
        let first = log.clone();
        ctx.on::<Ping>(move |value| first.borrow_mut().push(("a", value)))
            .unwrap();
        let second = log.clone();
        ctx.on::<Ping>(move |value| second.borrow_mut().push(("b", value)))
            .unwrap();

        ctx.emit::<Ping>(7);
        assert_eq!(&*log.borrow(), &[("a", 7), ("b", 7)]);
    });
}

#[test]
fn once_listener_fires_once() {
    run(|ctx| async move {
        struct Once;
        impl Event for Once {
            type Args = ();
            const NAME: &'static str = "once";
        }
        impl EmitEvent for Once {}

        let hits = Rc::new(Cell::new(0));
        let counter = hits.clone();
        ctx.once::<Once>(move |_| counter.set(counter.get() + 1))
            .unwrap();

        ctx.emit::<Once>(());
        ctx.emit::<Once>(());
        assert_eq!(hits.get(), 1);
    });
}

#[test]
fn parallel_calls_every_listener() {
    run(|ctx| async move {
        struct Par;
        impl Event for Par {
            type Args = u32;
            const NAME: &'static str = "par";
        }
        impl ParallelEvent for Par {}

        let hits = Rc::new(Cell::new(0));
        for _ in 0..3 {
            let counter = hits.clone();
            ctx.on_parallel::<Par>(move |_| {
                counter.set(counter.get() + 1);
                Box::pin(async {})
            })
            .unwrap();
        }

        ctx.parallel::<Par>(0).await;
        assert_eq!(hits.get(), 3);
    });
}

#[test]
fn unloading_a_plugin_removes_its_listeners() {
    run(|ctx| async move {
        struct Tick;
        impl Event for Tick {
            type Args = ();
            const NAME: &'static str = "tick";
        }
        impl EmitEvent for Tick {}

        let hits = Rc::new(Cell::new(0));
        let listener_hits = hits.clone();
        let plugin = FnPlugin::new("listener", move |ctx, _| {
            let hits = listener_hits.clone();
            Box::pin(async move {
                ctx.on::<Tick>(move |_| hits.set(hits.get() + 1)).unwrap();
                Ok(())
            })
        });

        let handle = ctx.plugin(plugin).unwrap();
        handle.join().await.unwrap();
        ctx.emit::<Tick>(());
        assert_eq!(hits.get(), 1);

        handle.dispose().await;
        ctx.emit::<Tick>(());
        assert_eq!(hits.get(), 1);
    });
}

#[test]
fn scope_inherits_and_isolate_overrides() {
    run(|ctx| async move {
        struct Value(u32);
        const VALUE: ServiceKey<Value> = ServiceKey::new("value");

        ctx.provide(VALUE, Arc::new(Value(1))).unwrap();

        let scoped = ctx.scope();
        assert_eq!(scoped.get::<Value>(VALUE).unwrap().0, 1);

        let isolated = ctx.isolate(VALUE);
        assert!(isolated.get::<Value>(VALUE).is_none());
        isolated.provide(VALUE, Arc::new(Value(2))).unwrap();
        assert_eq!(isolated.get::<Value>(VALUE).unwrap().0, 2);
        assert_eq!(ctx.get::<Value>(VALUE).unwrap().0, 1);
    });
}

#[test]
fn failing_plugin_rolls_back_its_effects() {
    run(|ctx| async move {
        let cleaned = Rc::new(Cell::new(false));
        let cleanup = cleaned.clone();

        let plugin = FnPlugin::new("failing", move |ctx, _| {
            let cleanup = cleanup.clone();
            Box::pin(async move {
                ctx.effect(move || Some(Box::new(move || cleanup.set(true))))
                    .unwrap();
                Err(dsh_cordis::PluginError::new("boom"))
            })
        });

        let handle = ctx.plugin(plugin).unwrap();
        assert!(handle.join().await.is_err());
        assert_eq!(handle.status(), dsh_cordis::FiberStatus::Failed);
        assert!(cleaned.get());
    });
}

#[test]
fn config_is_passed_to_plugin() {
    run(|ctx| async move {
        let seen = Rc::new(RefCell::new(Config::Null));
        let sink = seen.clone();
        let plugin = FnPlugin::new("config", move |_ctx, config| {
            *sink.borrow_mut() = config.clone();
            Box::pin(async { Ok(()) })
        });

        let handle = ctx
            .plugin_with_config(plugin, serde_json::json!({ "name": "rust" }))
            .unwrap();
        handle.join().await.unwrap();
        assert_eq!(seen.borrow()["name"], "rust");
    });
}

#[test]
fn invalid_config_is_rejected_before_apply() {
    run(|ctx| async move {
        let applied = Rc::new(Cell::new(false));
        let applied_by_plugin = applied.clone();

        let plugin = FnPlugin::new("validated", move |_ctx, _config| {
            applied_by_plugin.set(true);
            Box::pin(async { Ok(()) })
        })
        .validate_with(Arc::new(|raw: &Config| {
            if raw.get("enabled").and_then(|value| value.as_bool()) == Some(true) {
                Ok(raw.clone())
            } else {
                Err("`enabled` must be true".to_owned())
            }
        }));

        // `PluginMeta` keeps a working `Debug` impl with a validator attached.
        let meta = PluginMeta::new("meta-check")
            .with_validator(|_raw: &Config| -> Result<Config, String> { Ok(Config::Null) });
        assert!(format!("{meta:?}").contains("meta-check"));
        assert!(format!("{:?}", plugin.meta()).contains("validator: true"));

        let result = ctx.plugin_with_config(plugin, serde_json::json!({ "enabled": false }));
        let error = match result {
            Ok(_) => panic!("invalid config must be rejected"),
            Err(error) => error,
        };
        assert!(
            matches!(error, CordisError::InvalidConfig(ref message) if message.contains("enabled")),
            "unexpected error: {error:?}"
        );

        // The validator ran before mounting: apply was never called and the
        // rejected plugin did not allocate a fiber (the next one gets id 1).
        assert!(!applied.get());

        let next = ctx
            .plugin(FnPlugin::new("next", |_, _| Box::pin(async { Ok(()) })))
            .unwrap();
        assert_eq!(next.id(), 1);
        next.join().await.unwrap();
    });
}

#[test]
fn config_validator_normalizes_before_apply() {
    run(|ctx| async move {
        let seen = Rc::new(RefCell::new(Config::Null));
        let sink = seen.clone();

        let plugin = FnPlugin::new("normalized", move |_ctx, config| {
            *sink.borrow_mut() = config;
            Box::pin(async { Ok(()) })
        })
        .validate_with(Arc::new(|raw: &Config| -> Result<Config, String> {
            let mut normalized = raw.clone();
            let seconds = normalized
                .get("timeout_s")
                .and_then(|value| value.as_u64())
                .ok_or_else(|| "`timeout_s` must be a positive integer".to_owned())?;
            normalized["timeout_ms"] = (seconds * 1000).into();
            Ok(normalized)
        }));

        let handle = ctx
            .plugin_with_config(plugin, serde_json::json!({ "timeout_s": 2 }))
            .unwrap();
        handle.join().await.unwrap();

        // apply received the normalized config, not the raw one.
        assert_eq!(seen.borrow()["timeout_ms"], serde_json::json!(2000));
        assert_eq!(seen.borrow()["timeout_s"], serde_json::json!(2));
    });
}

#[test]
fn event_options_api_covers_all_five_modes() {
    run(|ctx| async move {
        struct Emit;
        impl Event for Emit {
            type Args = u32;
            const NAME: &'static str = "opt-emit";
        }
        impl EmitEvent for Emit {}

        struct Wf;
        impl Event for Wf {
            type Args = u32;
            const NAME: &'static str = "opt-wf";
        }
        impl WaterfallEvent for Wf {
            type Out = u32;
        }

        struct Ser;
        impl Event for Ser {
            type Args = u32;
            const NAME: &'static str = "opt-ser";
        }
        impl SerialEvent for Ser {
            type Out = u32;
        }

        struct Stop;
        impl Event for Stop {
            type Args = u32;
            const NAME: &'static str = "opt-bail";
        }
        impl BailEvent for Stop {
            type Out = u32;
        }

        struct Par;
        impl Event for Par {
            type Args = u32;
            const NAME: &'static str = "opt-par";
        }
        impl ParallelEvent for Par {}

        let options = EventOptions::default();
        ctx.on_with::<Emit>(options, |_| {}).unwrap();
        ctx.on_emit_with::<Emit>(options, |_| {}).unwrap();
        ctx.on_waterfall_with::<Wf>(options, |args, next| {
            Box::pin(async move { next.next(args).await + 1 })
        })
        .unwrap();
        ctx.on_serial_with::<Ser>(options, |_| Box::pin(async { None }))
            .unwrap();
        ctx.on_bail_with::<Stop>(options, |_| None).unwrap();
        ctx.on_parallel_with::<Par>(options, |_| Box::pin(async {}))
            .unwrap();

        ctx.emit::<Emit>(1);
        assert_eq!(
            ctx.waterfall::<Wf>(1, |args| Box::pin(async move { args }))
                .await,
            2
        );
        assert_eq!(ctx.serial::<Ser>(1).await, None);
        assert_eq!(ctx.bail::<Stop>(1), None);
        ctx.parallel::<Par>(1).await;
    });
}

#[test]
fn prepend_listeners_run_first_in_reverse_registration_order() {
    run(|ctx| async move {
        struct Ping;
        impl Event for Ping {
            type Args = ();
            const NAME: &'static str = "prepend-ping";
        }
        impl EmitEvent for Ping {}

        let log = Rc::new(RefCell::new(Vec::new()));
        for entry in ["a", "b"] {
            let log = log.clone();
            ctx.on::<Ping>(move |_| log.borrow_mut().push(entry))
                .unwrap();
        }
        for entry in ["c", "d"] {
            let log = log.clone();
            ctx.on_with::<Ping>(
                EventOptions {
                    prepend: true,
                    once: false,
                },
                move |_| log.borrow_mut().push(entry),
            )
            .unwrap();
        }
        ctx.emit::<Ping>(());
        assert_eq!(&*log.borrow(), &["d", "c", "a", "b"]);

        struct Wf;
        impl Event for Wf {
            type Args = u32;
            const NAME: &'static str = "prepend-wf";
        }
        impl WaterfallEvent for Wf {
            type Out = u32;
        }

        let order = Rc::new(RefCell::new(Vec::new()));
        for entry in ["a", "b"] {
            let order = order.clone();
            ctx.on_waterfall::<Wf>(move |args, next| {
                let order = order.clone();
                Box::pin(async move {
                    order.borrow_mut().push(entry);
                    next.next(args).await + 1
                })
            })
            .unwrap();
        }
        for entry in ["c", "d"] {
            let order = order.clone();
            ctx.on_waterfall_with::<Wf>(
                EventOptions {
                    prepend: true,
                    once: false,
                },
                move |args, next| {
                    let order = order.clone();
                    Box::pin(async move {
                        order.borrow_mut().push(entry);
                        next.next(args).await + 1
                    })
                },
            )
            .unwrap();
        }

        let out = ctx
            .waterfall::<Wf>(0, |args| Box::pin(async move { args + 10 }))
            .await;
        assert_eq!(out, 14);
        assert_eq!(&*order.borrow(), &["d", "c", "a", "b"]);
    });
}

#[test]
fn waterfall_once_listener_runs_at_most_once() {
    run(|ctx| async move {
        struct Step;
        impl Event for Step {
            type Args = u32;
            const NAME: &'static str = "once-wf";
        }
        impl WaterfallEvent for Step {
            type Out = u32;
        }

        let hits = Rc::new(Cell::new(0));

        // An upstream listener calls `next` twice per dispatch; the `once`
        // listener downstream must still run at most once in that dispatch.
        ctx.on_waterfall::<Step>(|args, next| {
            Box::pin(async move {
                let first = next.next(args).await;
                let second = next.next(args).await;
                first + second
            })
        })
        .unwrap();

        let once_hits = hits.clone();
        ctx.on_waterfall_with::<Step>(
            EventOptions {
                prepend: false,
                once: true,
            },
            move |args, next| {
                let once_hits = once_hits.clone();
                Box::pin(async move {
                    once_hits.set(once_hits.get() + 1);
                    next.next(args).await + 1
                })
            },
        )
        .unwrap();

        // First dispatch: 11 (default) + 1 from the once listener = 12 for the
        // first `next`, then the once listener is skipped: 11. Upstream sums
        // both calls.
        let first = ctx
            .waterfall::<Step>(1, |args| Box::pin(async move { args + 10 }))
            .await;
        assert_eq!(first, 23);
        assert_eq!(hits.get(), 1);

        // Second dispatch: the once listener is gone; both `next` calls hit the
        // default directly.
        let second = ctx
            .waterfall::<Step>(1, |args| Box::pin(async move { args + 10 }))
            .await;
        assert_eq!(second, 22);
        assert_eq!(hits.get(), 1);
    });
}

#[test]
fn inject_reloads_after_provider_replacement() {
    run(|ctx| async move {
        struct Tick;
        impl Event for Tick {
            type Args = ();
            const NAME: &'static str = "reload-tick";
        }
        impl EmitEvent for Tick {}

        let applies = Rc::new(Cell::new(0));
        let hits = Rc::new(Cell::new(0));
        let cleaned = Rc::new(Cell::new(0));
        let applies_in_plugin = applies.clone();
        let hits_in_plugin = hits.clone();
        let cleaned_in_plugin = cleaned.clone();

        let consumer = FnPlugin::new("reload-consumer", move |ctx, _config| {
            let applies = applies_in_plugin.clone();
            let hits = hits_in_plugin.clone();
            let cleaned = cleaned_in_plugin.clone();
            Box::pin(async move {
                applies.set(applies.get() + 1);
                ctx.on::<Tick>(move |_| hits.set(hits.get() + 1))
                    .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                ctx.effect(move || Some(Box::new(move || cleaned.set(cleaned.get() + 1))))
                    .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                Ok(())
            })
        })
        .inject([TOOLS]);

        let consumer_handle = ctx.plugin(consumer).unwrap();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Pending);

        let provider_one = ctx
            .plugin(FnPlugin::new("reload-provider-one", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(TOOLS, Arc::new(Tools))
                        .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                    Ok(())
                })
            }))
            .unwrap();
        provider_one.join().await.unwrap();
        consumer_handle.join().await.unwrap();
        assert_eq!(applies.get(), 1);
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Active);

        ctx.emit::<Tick>(());
        assert_eq!(hits.get(), 1, "the first apply's listener must be live");

        // Removing the provider unloads the consumer: back to Pending and no
        // old listener remains on the bus.
        provider_one.dispose().await;
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Pending);
        assert_eq!(cleaned.get(), 1, "unload must run the old effects");
        ctx.emit::<Tick>(());
        assert_eq!(hits.get(), 1, "unload must remove the old listener");

        // A replacement provider makes the consumer re-apply automatically.
        let provider_two = ctx
            .plugin(FnPlugin::new("reload-provider-two", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(TOOLS, Arc::new(Tools))
                        .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                    Ok(())
                })
            }))
            .unwrap();
        provider_two.join().await.unwrap();
        consumer_handle.join().await.unwrap();
        assert_eq!(applies.get(), 2, "the consumer must re-apply");
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Active);

        ctx.emit::<Tick>(());
        assert_eq!(hits.get(), 2, "only the new listener may fire");

        // The second provider's removal unloads the second apply's effect too.
        provider_two.dispose().await;
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Pending);
        assert_eq!(cleaned.get(), 2);
    });
}

#[test]
fn reload_reuses_normalized_config_without_revalidation() {
    run(|ctx| async move {
        let validations = Arc::new(AtomicUsize::new(0));
        let applies = Rc::new(Cell::new(0));
        let seen = Rc::new(RefCell::new(Vec::<Config>::new()));

        let validation_count = validations.clone();
        let apply_count = applies.clone();
        let seen_configs = seen.clone();

        let consumer = FnPlugin::new("config-reload-consumer", move |_ctx, config| {
            apply_count.set(apply_count.get() + 1);
            seen_configs.borrow_mut().push(config);
            Box::pin(async { Ok(()) })
        })
        .inject([TOOLS])
        .validate_with(Arc::new(move |raw: &Config| {
            validation_count.fetch_add(1, Ordering::Relaxed);
            let mut normalized = raw.clone();
            normalized["normalized"] = true.into();
            Ok(normalized)
        }));

        let consumer_handle = ctx
            .plugin_with_config(consumer, serde_json::json!({ "value": 1 }))
            .unwrap();
        assert_eq!(validations.load(Ordering::Relaxed), 1);
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Pending);

        let provider_one = ctx
            .plugin(FnPlugin::new("config-provider-one", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(TOOLS, Arc::new(Tools))
                        .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                    Ok(())
                })
            }))
            .unwrap();
        provider_one.join().await.unwrap();
        consumer_handle.join().await.unwrap();
        assert_eq!(applies.get(), 1);
        assert_eq!(seen.borrow()[0]["normalized"], serde_json::json!(true));

        provider_one.dispose().await;
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Pending);

        let provider_two = ctx
            .plugin(FnPlugin::new("config-provider-two", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(TOOLS, Arc::new(Tools))
                        .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                    Ok(())
                })
            }))
            .unwrap();
        provider_two.join().await.unwrap();
        consumer_handle.join().await.unwrap();
        assert_eq!(applies.get(), 2);
        assert_eq!(
            validations.load(Ordering::Relaxed),
            1,
            "the normalized config must be reused, not revalidated"
        );
        assert_eq!(seen.borrow().len(), 2);
        assert_eq!(seen.borrow()[0], seen.borrow()[1]);
    });
}

#[test]
fn cascade_unload_is_reentrant_and_reloads_dependency_chain_once() {
    run(|ctx| async move {
        struct S1;
        struct S2;
        const S1_KEY: ServiceKey<S1> = ServiceKey::new("chain-s1");
        const S2_KEY: ServiceKey<S2> = ServiceKey::new("chain-s2");

        let b_applies = Rc::new(Cell::new(0));
        let c_applies = Rc::new(Cell::new(0));

        let b_count = b_applies.clone();
        let b = FnPlugin::new("chain-b", move |ctx, _| {
            let b_count = b_count.clone();
            Box::pin(async move {
                b_count.set(b_count.get() + 1);
                ctx.provide(S2_KEY, Arc::new(S2))
                    .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                Ok(())
            })
        })
        .inject([S1_KEY]);

        let c_count = c_applies.clone();
        let c = FnPlugin::new("chain-c", move |_ctx, _| {
            let c_count = c_count.clone();
            Box::pin(async move {
                c_count.set(c_count.get() + 1);
                Ok(())
            })
        })
        .inject([S2_KEY]);

        let b_handle = ctx.plugin(b).unwrap();
        let c_handle = ctx.plugin(c).unwrap();

        let provider_one = ctx
            .plugin(FnPlugin::new("chain-provider-one", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(S1_KEY, Arc::new(S1))
                        .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                    Ok(())
                })
            }))
            .unwrap();
        provider_one.join().await.unwrap();
        b_handle.join().await.unwrap();
        c_handle.join().await.unwrap();
        assert_eq!(b_applies.get(), 1);
        assert_eq!(c_applies.get(), 1);
        assert_eq!(c_handle.status(), dsh_cordis::FiberStatus::Active);

        // Unloading the S1 provider unloads B; removing B's S2 service then
        // unloads C from the outer cascade loop, not from a nested cascade.
        provider_one.dispose().await;
        assert_eq!(b_handle.status(), dsh_cordis::FiberStatus::Pending);
        assert_eq!(c_handle.status(), dsh_cordis::FiberStatus::Pending);

        let provider_two = ctx
            .plugin(FnPlugin::new("chain-provider-two", |ctx, _| {
                Box::pin(async move {
                    ctx.provide(S1_KEY, Arc::new(S1))
                        .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                    Ok(())
                })
            }))
            .unwrap();
        provider_two.join().await.unwrap();
        b_handle.join().await.unwrap();
        c_handle.join().await.unwrap();

        // Every dependent re-applies exactly once; duplicate pending entries
        // would show up as extra applies here.
        assert_eq!(b_applies.get(), 2);
        assert_eq!(c_applies.get(), 2);
        assert_eq!(b_handle.status(), dsh_cordis::FiberStatus::Active);
        assert_eq!(c_handle.status(), dsh_cordis::FiberStatus::Active);
    });
}

#[test]
fn inject_loading_dependent_unloads_and_discards_stale_apply() {
    run(|ctx| async move {
        struct Slow;
        const SLOW: ServiceKey<Slow> = ServiceKey::new("loading-slow");

        struct Check(u32);
        const CHECK: ServiceKey<Check> = ServiceKey::new("loading-check");

        struct LoadingTick;
        impl Event for LoadingTick {
            type Args = ();
            const NAME: &'static str = "loading-tick";
        }
        impl EmitEvent for LoadingTick {}

        let applies = Rc::new(Cell::new(0));
        let hits = Rc::new(Cell::new(0));
        let entered_tx = Rc::new(RefCell::new(None::<futures::channel::oneshot::Sender<()>>));
        let release_rx = Rc::new(RefCell::new(
            None::<futures::channel::oneshot::Receiver<()>>,
        ));

        let (entered_sender, entered_receiver) = futures::channel::oneshot::channel();
        let (release_sender, release_receiver) = futures::channel::oneshot::channel();
        *entered_tx.borrow_mut() = Some(entered_sender);
        *release_rx.borrow_mut() = Some(release_receiver);

        let applies_in_plugin = applies.clone();
        let hits_in_plugin = hits.clone();
        let entered_in_plugin = entered_tx.clone();
        let release_in_plugin = release_rx.clone();

        let consumer = FnPlugin::new("loading-consumer", move |ctx, _config| {
            let applies = applies_in_plugin.clone();
            let hits = hits_in_plugin.clone();
            let entered = entered_in_plugin.clone();
            let release = release_in_plugin.clone();
            Box::pin(async move {
                let attempt = applies.get() + 1;
                applies.set(attempt);
                if let Some(sender) = entered.borrow_mut().take() {
                    let _ = sender.send(());
                }
                if attempt == 1 {
                    let receiver = release.borrow_mut().take();
                    if let Some(receiver) = receiver {
                        let _ = receiver.await;
                    }
                }
                ctx.on::<LoadingTick>(move |_| hits.set(hits.get() + 1))
                    .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                ctx.provide(CHECK, Arc::new(Check(attempt)))
                    .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                Ok(())
            })
        })
        .inject([SLOW]);

        let provider_one = ctx.provide(SLOW, Arc::new(Slow)).unwrap();
        let consumer_handle = ctx.plugin(consumer).unwrap();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Loading);

        entered_receiver.await.unwrap();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Loading);

        // Provider disappears while the dependent's apply is parked. The
        // Loading victim is unloaded and its apply task is aborted.
        provider_one.dispose();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Pending);
        assert_eq!(applies.get(), 1);

        // Start the reload before releasing the aborted apply: its next poll
        // would otherwise run while the new generation already owns the fiber
        // (Loading/Active) and leak a listener/service into it.
        let provider_two = ctx.provide(SLOW, Arc::new(Slow)).unwrap();
        let _ = release_sender.send(());
        consumer_handle.join().await.unwrap();
        for _ in 0..3 {
            yield_once().await;
        }

        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Active);
        assert_eq!(applies.get(), 2);
        assert_eq!(
            ctx.get(CHECK).map(|check| check.0),
            Some(2),
            "no old-generation service may be visible"
        );
        ctx.emit::<LoadingTick>(());
        assert_eq!(hits.get(), 1, "only the new apply's listener may fire");
        provider_two.dispose();
    });
}

#[test]
fn inject_loading_dependent_unloads_when_apply_never_finishes() {
    run(|ctx| async move {
        struct Stuck;
        const STUCK: ServiceKey<Stuck> = ServiceKey::new("loading-stuck");

        let applies = Rc::new(Cell::new(0));
        let entered_tx = Rc::new(RefCell::new(None::<futures::channel::oneshot::Sender<()>>));
        let (entered_sender, entered_receiver) = futures::channel::oneshot::channel();
        *entered_tx.borrow_mut() = Some(entered_sender);

        let applies_in_plugin = applies.clone();
        let entered_in_plugin = entered_tx.clone();
        let consumer = FnPlugin::new("stuck-consumer", move |_ctx, _config| {
            let applies = applies_in_plugin.clone();
            let entered = entered_in_plugin.clone();
            Box::pin(async move {
                let attempt = applies.get() + 1;
                applies.set(attempt);
                if attempt == 1 {
                    if let Some(sender) = entered.borrow_mut().take() {
                        let _ = sender.send(());
                    }
                    std::future::pending::<()>().await;
                }
                Ok(())
            })
        })
        .inject([STUCK]);

        let provider_one = ctx.provide(STUCK, Arc::new(Stuck)).unwrap();
        let consumer_handle = ctx.plugin(consumer).unwrap();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Loading);

        entered_receiver.await.unwrap();
        provider_one.dispose();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Pending);

        // The aborted apply stays aborted; it cannot complete or resurrect.
        for _ in 0..3 {
            yield_once().await;
        }
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Pending);
        assert_eq!(applies.get(), 1);

        let _provider_two = ctx.provide(STUCK, Arc::new(Stuck)).unwrap();
        consumer_handle.join().await.unwrap();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Active);
        assert_eq!(applies.get(), 2);
    });
}

#[test]
fn cascade_guard_recovers_after_disposer_panic() {
    run(|ctx| async move {
        struct PanicSvc;
        const PANIC_SVC: ServiceKey<PanicSvc> = ServiceKey::new("panic-svc");

        let panicked = Rc::new(Cell::new(false));
        let panicked_in_plugin = panicked.clone();
        let consumer = FnPlugin::new("panic-consumer", move |ctx, _config| {
            let panicked = panicked_in_plugin.clone();
            Box::pin(async move {
                ctx.effect(move || {
                    Some(Box::new(move || {
                        if !panicked.replace(true) {
                            panic!("disposer panic in cascade test");
                        }
                    }))
                })
                .map_err(|error| dsh_cordis::PluginError::new(error.to_string()))?;
                Ok(())
            })
        })
        .inject([PANIC_SVC]);

        let consumer_handle = ctx.plugin(consumer).unwrap();
        let provider_one = ctx.provide(PANIC_SVC, Arc::new(PanicSvc)).unwrap();
        consumer_handle.join().await.unwrap();
        assert_eq!(consumer_handle.status(), dsh_cordis::FiberStatus::Active);

        // The disposer runs inside the cascade guard; catch the unwind here so
        // the test can verify the guard was reset.
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            provider_one.dispose();
        }));
        assert!(outcome.is_err(), "the disposer panic must propagate");

        // A fresh dependency pair must still cascade after the panic unwound.
        struct LaterSvc;
        const LATER_SVC: ServiceKey<LaterSvc> = ServiceKey::new("later-svc");
        let later_consumer = FnPlugin::new("later-consumer", |_ctx, _| Box::pin(async { Ok(()) }))
            .inject([LATER_SVC]);
        let later_handle = ctx.plugin(later_consumer).unwrap();
        let provider_two = ctx.provide(LATER_SVC, Arc::new(LaterSvc)).unwrap();
        later_handle.join().await.unwrap();
        assert_eq!(later_handle.status(), dsh_cordis::FiberStatus::Active);

        provider_two.dispose();
        assert_eq!(
            later_handle.status(),
            dsh_cordis::FiberStatus::Pending,
            "cascade must keep working after a disposer panic unwound"
        );
    });
}

#[test]
fn once_emit_listener_is_removed_before_invocation() {
    run(|ctx| async move {
        struct Echo;
        impl Event for Echo {
            type Args = u32;
            const NAME: &'static str = "once-echo";
        }
        impl EmitEvent for Echo {}

        let hits = Rc::new(Cell::new(0));
        let hits_in_plugin = hits.clone();
        let inner_ctx = ctx.clone();
        ctx.once::<Echo>(move |value| {
            hits_in_plugin.set(hits_in_plugin.get() + 1);
            if value == 0 {
                // Re-enter the same dispatch from inside the once listener.
                inner_ctx.emit::<Echo>(1);
            }
        })
        .unwrap();

        ctx.emit::<Echo>(0);
        assert_eq!(
            hits.get(),
            1,
            "a re-entrant emit must not run the once listener twice"
        );
    });
}
