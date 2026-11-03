use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;

use dsh_cordis::{
    BailEvent, Config, Context, CordisError, EmitEvent, Event, FnPlugin, ParallelEvent,
    SerialEvent, ServiceKey, WaterfallEvent,
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

struct Counter {
    value: u32,
}

const COUNTER: ServiceKey = ServiceKey::new("counter");

#[test]
fn service_provide_get_and_require() {
    run(|ctx| async move {
        ctx.provide(COUNTER, Arc::new(Counter { value: 3 }))
            .unwrap();
        assert_eq!(ctx.get::<Counter>(COUNTER).unwrap().value, 3);
        assert!(ctx.get::<String>(COUNTER).is_none());
        assert!(matches!(
            ctx.require::<Counter>(COUNTER),
            Ok(counter) if counter.value == 3
        ));
        assert!(matches!(
            ctx.require::<String>(COUNTER),
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

const TOOLS: ServiceKey = ServiceKey::new("tools");

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
        const VALUE: ServiceKey = ServiceKey::new("value");

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
