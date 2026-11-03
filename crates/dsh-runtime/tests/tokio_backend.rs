#![cfg(feature = "tokio")]

use std::rc::Rc;
use std::time::{Duration, Instant};

use dsh_runtime::{Runtime, RuntimeExt, TokioRuntime};

fn runtime() -> Rc<TokioRuntime> {
    Rc::new(TokioRuntime::new().expect("tokio runtime"))
}

#[test]
fn spawn_typed_returns_value() {
    let rt = runtime();
    let task_rt = rt.clone();
    rt.block_on(Box::pin(async move {
        let handle = task_rt.spawn_typed(async { 21 * 2 });
        assert_eq!(handle.join().await.unwrap(), 42);
    }));
}

#[test]
fn nested_spawn_runs_on_local_set() {
    let rt = runtime();
    let task_rt = rt.clone();
    rt.block_on(Box::pin(async move {
        let inner_rt = task_rt.clone();
        let outer = task_rt.spawn_typed(async move {
            let inner = inner_rt.spawn_typed(async { 5 });
            inner.join().await.unwrap() + 1
        });
        assert_eq!(outer.join().await.unwrap(), 6);
    }));
}

#[test]
fn sleep_waits() {
    let rt = runtime();
    let task_rt = rt.clone();
    let start = Instant::now();
    rt.block_on(Box::pin(async move {
        task_rt
            .sleep_until(Instant::now() + Duration::from_millis(30))
            .await;
    }));
    assert!(start.elapsed() >= Duration::from_millis(25));
}

#[test]
fn timeout_expires() {
    let rt = runtime();
    let task_rt = rt.clone();
    rt.block_on(Box::pin(async move {
        let result = task_rt
            .timeout(
                Duration::from_millis(15),
                Box::pin(std::future::pending::<()>()),
            )
            .await;
        assert!(result.is_err());
    }));
}

#[test]
fn abort_cancels_task() {
    let rt = runtime();
    let task_rt = rt.clone();
    rt.block_on(Box::pin(async move {
        let handle = task_rt.spawn_typed(async {
            std::future::pending::<()>().await;
            1u8
        });
        handle.abort();
        assert!(handle.join().await.is_err());
    }));
}

#[test]
fn blocking_returns_value() {
    let rt = runtime();
    let task_rt = rt.clone();
    rt.block_on(Box::pin(async move {
        let handle = task_rt.spawn_blocking_typed(|| 7u32);
        assert_eq!(handle.join().await.unwrap(), 7);
    }));
}
