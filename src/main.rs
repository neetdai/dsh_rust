use std::rc::Rc;
use std::sync::Arc;

use dsh_cordis::{Context, FnPlugin, PluginError, ServiceKey};
use dsh_runtime::{Runtime, TokioRuntime};

struct Greeter {
    name: String,
}

const GREETER: ServiceKey<Greeter> = ServiceKey::new("greeter");

fn main() {
    let runtime: Rc<dyn Runtime> = Rc::new(TokioRuntime::new().expect("build tokio runtime"));
    let root = Context::root(runtime.clone());

    runtime.block_on(Box::pin(async move {
        let provider = FnPlugin::new("provider", |ctx, _config| {
            Box::pin(async move {
                ctx.provide(GREETER, Arc::new(Greeter { name: "dsh".into() }))
                    .map_err(|error| PluginError::new(error.to_string()))?;
                println!("[provider] registered `greeter`");
                Ok(())
            })
        });

        let consumer = FnPlugin::new("consumer", |ctx, _config| {
            Box::pin(async move {
                let greeter = ctx
                    .require(GREETER)
                    .map_err(|error| PluginError::new(error.to_string()))?;
                println!("[consumer] hello, {}!", greeter.name);
                Ok(())
            })
        })
        .inject([GREETER]);

        let provider = root.plugin(provider).expect("mount provider");
        let consumer = root.plugin(consumer).expect("mount consumer");

        provider.join().await.expect("provider ready");
        consumer.join().await.expect("consumer ready");

        consumer.dispose().await;
        provider.dispose().await;
        println!("[main] unloaded cleanly");
    }));
}
