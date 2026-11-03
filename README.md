# deepseek-harness-rust

Rust 复刻 DeepSeek Harness 的工作区。当前阶段完成了两件事：

1. **运行时可替换层**：`crates/dsh-runtime`，同时提供 Tokio 与 compio 后端；
2. **Cordis 最小内核**：`crates/dsh-cordis`，服务、事件五种派发、fiber/effect、inject、scope/isolate。

## 目录

```
crates/
  dsh-runtime/     # Runtime trait + TokioRuntime + CompioRuntime
  dsh-cordis/      # Cordis 兼容内核
    src/context.rs
    src/event.rs
    src/fiber.rs
    src/plugin.rs
    src/registry.rs
    src/service.rs
    tests/kernel.rs
```

## 快速开始

```bash
cargo test --workspace
cargo test -p dsh-runtime --features compio --test compio_backend
cargo test -p dsh-cordis --features compio --test kernel
```

> 本机若只能离线构建，可把 cargo registry 缓存放进工作区并设置
> `CARGO_HOME=.cargo-home`；该目录已加入 `.gitignore`，不要提交。

## Runtime 抽象

```rust
pub trait Runtime {
    fn name(&self) -> &'static str;
    fn spawn_raw(&self, fut: LocalBoxFuture<'static, ()>);
    fn spawn_blocking_raw(&self, f: Box<dyn FnOnce() + Send + 'static>);
    fn sleep_until(&self, at: Instant) -> LocalBoxFuture<'static, ()>;
    fn block_on(&self, fut: LocalBoxFuture<'static, ()>);
}
```

`RuntimeExt` 在其上提供 `spawn`、`spawn_typed`、`spawn_blocking`、
`timeout`、`sleep`。所有任务都带一个协作式取消标记，`TaskHandle::abort`
会在下一个 await 点生效；`TaskHandle::join` 返回 `TaskError::Aborted`。

### 为什么 future 不是 `Send`

`compio::runtime::Runtime` 本身是 **`!Send`/`!Sync` 的线程本地运行时**（官方文档明确写
"cannot be sent to other threads"）。要同时支持 Tokio 和 compio，trait 不能要求
`Send`，否则 compio 直接被排除。

因此内核是**单线程引擎**模型：

- 内核任务在引擎线程上执行，和 Cordis/QuickJS 的单线程语义一致；
- 阻塞或 CPU 密集工作通过 `Runtime::spawn_blocking` 逃逸到线程池；
- 真实 I/O（HTTP、SQLite、进程）由各接缝按后端实现，内核只依赖 spawn/sleep。

这个取舍换来的是：**同一份内核代码在 Tokio 和 compio 上都能编译、运行、通过同一套测试**。

### 后端差异

| | Tokio | compio |
|---|---|---|
| 任务模型 | `LocalSet` + `spawn_local` | 线程本地 executor |
| 取消 | 协作式 `select` + 取消标记 | 同左 |
| 句柄 | `JoinHandle` 丢弃即 detach | **`JoinHandle` 丢弃会取消任务，必须 `detach()`** |
| I/O | 生态成熟（reqwest/hyper/sqlx） | io_uring（Linux）/ IOCP（Windows），生态较新 |
| 适用 | Phase 1 默认 | 低内存、移动端、后续压测 |

**当前建议**：Phase 1 用 Tokio（HTTP/TLS/MCP/SQLite 生态成熟），compio 后端保留为低内存目标；
trait 不泄漏任何后端类型，切换只需改 `Backend` 别名。

## Cordis 内核

### 已覆盖

| Cordis 概念 | Rust 实现 |
|---|---|
| `ctx.provide / get / require` | `ServiceStore`（按 scope 共享，fiber 拥有） |
| `ctx.effect` | `EffectHandle`；fiber 卸载时**反序**执行，幂等 |
| `ctx.on / once` | 监听器注册为 effect，随 fiber 卸载自动移除 |
| `ctx.emit` | 同步、忽略返回值 |
| `ctx.parallel` | 并发 await 全部监听器 |
| `ctx.serial` | 顺序 await，遇到 `Some` 短路 |
| `ctx.bail` | 同步顺序，遇到 `Some` 短路 |
| `ctx.waterfall` | 监听器环绕 `next()`；不调用即否决默认行为 |
| `ctx.plugin` | 子 fiber，`apply` 异步执行；失败回滚已注册 effect |
| `ctx.inject` | 缺服务时 Pending，服务激活后自动启动；提供者卸载时级联卸载依赖者 |
| `ctx.scope / isolate` | 共享 scope store；isolate 为指定 key 新建嵌套 store |
| fiber 生命周期 | Pending → Loading → Active / Failed → Disposing → Disposed |

### 尚未实现（刻意留给后续）

- 配置校验（schemastery / JSON Schema）与 `Config` 的类型化；
- Loader / profile / bundle / patch 组合；
- `ctx.intercept`、`ctx.accessor`、`ctx.mixin`、context filter、global listener；
- 细粒度 fiber 热重载（当前用子树重启近似）；
- 异步 disposer（当前 disposer 是同步 `FnOnce`）；
- `getEffects()` 诊断树。

## 内核示例

```rust
use std::rc::Rc;
use std::sync::Arc;

use dsh_cordis::{Context, FnPlugin, PluginError, ServiceKey};
use dsh_runtime::{Runtime, TokioRuntime};

struct Greeter { name: String }
const GREETER: ServiceKey = ServiceKey::new("greeter");

let runtime: Rc<dyn Runtime> = Rc::new(TokioRuntime::new()?);
let ctx = Context::root(runtime.clone());

runtime.block_on(Box::pin(async move {
    let provider = FnPlugin::new("provider", |ctx, _| Box::pin(async move {
        ctx.provide(GREETER, Arc::new(Greeter { name: "dsh".into() }))
            .map_err(|e| PluginError::new(e.to_string()))?;
        Ok(())
    }));

    let consumer = FnPlugin::new("consumer", |ctx, _| Box::pin(async move {
        let greeter = ctx.require::<Greeter>(GREETER)
            .map_err(|e| PluginError::new(e.to_string()))?;
        println!("hello {}", greeter.name);
        Ok(())
    })).inject([GREETER]);

    let _ = ctx.plugin(provider).unwrap();
    let consumer = ctx.plugin(consumer).unwrap();
    consumer.join().await.unwrap();
}));
```

## 测试

- `tests/kernel.rs` 16 个用例，同一份代码通过 `Backend` 别名在 Tokio / compio 上各跑一遍；
- `tests/tokio_backend.rs` / `tests/compio_backend.rs` 覆盖 spawn、嵌套 spawn、sleep、
  timeout、abort、blocking。

## 下一步

1. 把 `ServiceKey` 升级为接口 marker + 类型化服务（替换目前“具体类型查找”的简化）；
2. 配置校验与 Loader（`cordis.patch.yml` 的 row 模型）；
3. `dsh-plugin-host`：QuickJS realm + Cordis JS 内核 + 能力桥；
4. 用同一套 kernel 跑 agent loop 的最小回合，验证 `agent/pre-step` 这类 waterfall 事件。
