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
| `ctx.provide / get / require` | `ServiceStore`（按 scope 共享，fiber 拥有）；键是编译期类型化的 `ServiceKey<T>` |
| `ctx.effect` | `EffectHandle`；fiber 卸载时**反序**执行，幂等 |
| `ctx.on / once` | 监听器注册为 effect，随 fiber 卸载自动移除 |
| `ctx.on_*_with` | 五种事件统一 `EventOptions { prepend, once }`；once 在派发开始时移除，本次派发仍参与 |
| `ctx.emit` | 同步、忽略返回值 |
| `ctx.parallel` | 并发 await 全部监听器 |
| `ctx.serial` | 顺序 await，遇到 `Some` 短路 |
| `ctx.bail` | 同步顺序，遇到 `Some` 短路 |
| `ctx.waterfall` | 监听器环绕 `next()`；不调用即否决默认行为 |
| `ctx.plugin` | 子 fiber，`apply` 异步执行；失败回滚已注册 effect |
| `ctx.inject` | 缺服务时 Pending；供者卸载时 Active/Loading 依赖者 unload 回 Pending，新 provider 激活后自动 reload |
| `ctx.scope / isolate` | 共享 scope store；isolate 为指定 key 新建嵌套 store |
| fiber 生命周期 | Pending → Loading → Active / Failed → Disposing → Disposed；Active/Loading 依赖消失时回到 Pending |

### 配置校验

`PluginMeta` 可以挂一个对象安全的 `ConfigValidator`，在创建 fiber **之前**校验并
归一化配置：

```rust
use std::sync::Arc;

use dsh_cordis::{Config, FnPlugin};

let plugin = FnPlugin::new("http", |_ctx, config| Box::pin(async move {
    // `apply` 看到的是校验器归一化后的配置。
    let _timeout_ms = config["timeout_ms"].as_u64().unwrap();
    Ok(())
}))
.validate_with(Arc::new(|raw: &Config| -> Result<Config, String> {
    let mut config = raw.clone();
    if config.get("timeout_ms").is_none() {
        config["timeout_ms"] = 5_000.into();
    }
    Ok(config)
}));
```

- 校验失败返回 `CordisError::InvalidConfig(String)`，**不会创建 fiber**，也不会执行
  `apply`，因此不会留下半挂载的插件；
- 校验成功时，传给 `apply` 的是校验器返回的**归一化**配置；
- `Fn(&Config) -> Result<Config, String>` 自动实现 `ConfigValidator`，也可手写实现；
- `ctx.plugin_with_config(plugin, config)` 是执行校验的入口；`ctx.plugin(plugin)`
  等价于传入 `Config::Null`；
- 完整 JSON Schema / schemastery 校验仍留给 Loader 层，这里只提供可插拔钩子。

### 事件注册选项

五种事件都提供带 `EventOptions` 的注册入口；默认 `on` / `on_*` 等价于
`EventOptions::default()`（尾部插入、常驻），因此现有行为不变：

| 事件 | 带选项注册 | 默认注册 |
|---|---|---|
| emit | `on_with` / `on_emit_with` | `on` / `once` |
| waterfall | `on_waterfall_with` | `on_waterfall` |
| serial | `on_serial_with` | `on_serial` |
| bail | `on_bail_with` | `on_bail` |
| parallel | `on_parallel_with` | `on_parallel` |

```rust
use dsh_cordis::EventOptions;

// prepend 插到链头；多次 prepend 时后注册者先执行（等价 Vec::insert(0)）。
ctx.on_with::<Tick>(
    EventOptions { prepend: true, once: false },
    |args| { /* ... */ },
)?;

// 五种事件的 once：派发开始时从总线移除，本次派发仍参与且最多执行一次；
// 后续或重入派发不再执行（emit 监听器在回调里再 emit 同一事件也安全）。
ctx.on_waterfall_with::<Step>(
    EventOptions { prepend: false, once: true },
    |args, next| Box::pin(async move { next.next(args).await }),
)?;
```

`emit` / `parallel` / `serial` / `bail` / `waterfall` 的 `once` 统一为先移除后派发：
即使监听器从未被上游调用（例如 serial/bail 短路或 waterfall 否决），它也不会在后续
派发中再次出现。

### inject 与可重载生命周期

`inject([SERVICE])` 声明的 fiber 在依赖不可解析时保持 `Pending`。当依赖提供者被卸载
时，**Active 或 Loading（apply 尚未完成）**的依赖者都会 unload 回 `Pending`，而不是
进入终态 `Disposed`：

- Loading 依赖者的 apply task 会被 abort，apply generation 递增；apply future 外层有
  **poll 闸门**：每次 poll 前比对 generation，不匹配后绝不再 poll 旧 future。因此旧代
  apply 不可能在新代 Loading/Active 期间注册 listener/服务/task；registry 的 Pending
  回滚保留作为兜底；
- 反序执行 effects：监听器、已 `provide` 的服务、子插件全部清理；
- 保留挂载时的插件实例、`Context` 与**已归一化**的 config，reload 不会再次调用
  validator；
- 新 provider 激活后 `wake_pending` 自动重新 `apply`，listener/服务重新注册；
  `finish_apply` 在置 `Active` 前会再次检查 inject，不满足则回到 `Pending`；
- unload 不通知 join waiter：`join()` 会继续等待下一次 reload 或显式 dispose；
- 显式 `FiberHandle::dispose()` 仍是终态 `Disposed`；Pending / Active fiber 被显式
  dispose 后，新 provider 不会让它复活。

恢复路径与首次挂载相同（`Pending → Loading → Active`）。相关测试见
`inject_reloads_after_provider_replacement`、
`inject_loading_dependent_unloads_and_discards_stale_apply`、
`inject_loading_dependent_unloads_when_apply_never_finishes` 与
`cascade_unload_is_reentrant_and_reloads_dependency_chain_once`。

### 尚未实现（刻意留给后续）

- 完整配置 schema（schemastery / JSON Schema）与 `Config` 的类型化；
- Loader / profile / bundle / patch 组合；
- `ctx.intercept`、`ctx.accessor`、`ctx.mixin`、context filter、global listener；
- 细粒度代码/配置热重载（当前只有 inject 依赖恢复的自动重载与子树重启近似）；
- 异步 disposer（当前 disposer 是同步 `FnOnce`）；
- `getEffects()` 诊断树。

## 内核示例

服务键是**编译期类型化**的：`ServiceKey<T>` 同时携带 Cordis 服务名和 Rust 类型
`T`，`ctx.get` / `ctx.require` / `ctx.provide` 只接受 `ServiceKey<T>`，
拿错类型在编译期就失败。需要跨泛型传递（如 `PluginMeta::inject`）时用
`ServiceKeyId`（`{name, type_id}` 的擦除键），`inject([GREETER])` 会自动转换。

```rust
use std::rc::Rc;
use std::sync::Arc;

use dsh_cordis::{Context, FnPlugin, PluginError, ServiceKey};
use dsh_runtime::{Runtime, TokioRuntime};

struct Greeter { name: String }
const GREETER: ServiceKey<Greeter> = ServiceKey::new("greeter");

let runtime: Rc<dyn Runtime> = Rc::new(TokioRuntime::new()?);
let ctx = Context::root(runtime.clone());

runtime.block_on(Box::pin(async move {
    let provider = FnPlugin::new("provider", |ctx, _| Box::pin(async move {
        ctx.provide(GREETER, Arc::new(Greeter { name: "dsh".into() }))
            .map_err(|e| PluginError::new(e.to_string()))?;
        Ok(())
    }));

    let consumer = FnPlugin::new("consumer", |ctx, _| Box::pin(async move {
        let greeter = ctx.require(GREETER)
            .map_err(|e| PluginError::new(e.to_string()))?;
        println!("hello {}", greeter.name);
        Ok(())
    })).inject([GREETER]);

    let _ = ctx.plugin(provider).unwrap();
    let consumer = ctx.plugin(consumer).unwrap();
    consumer.join().await.unwrap();
}));
```

`ctx.isolate(key)` 与 `ctx.scope()` 的语义不变：isolate 按擦除后的键为该层新建嵌套
store，查找在该层终止。

## 测试

- `tests/kernel.rs` 28 个用例（含配置校验、事件注册选项、inject Active/Loading 自动重载、
  disposer panic 后的 cascade 守卫、reentrant emit），同一份代码通过 `Backend` 别名在
  Tokio / compio 上各跑一遍；
- `tests/conformance.rs` 16 个独立一致性用例（验证者维护，不复用 kernel 辅助代码）：
  - Tokio：`cargo test -p dsh-cordis --test conformance --offline` → 16 passed；
  - compio：`cargo test -p dsh-cordis --features compio --test conformance --offline`
    → 16 passed；
- `tests/tokio_backend.rs` / `tests/compio_backend.rs` 覆盖 spawn、嵌套 spawn、sleep、
  timeout、abort、blocking。

完整验证（2026-11-04）：

```powershell
$env:CARGO_HOME='D:\rust\deepseek_harness_rust\.cargo-home'
cargo test --workspace --offline                      # kernel 28 + conformance 16 + runtime(tokio) 6 + doctest 2 = 52 passed（命名套件合计）
cargo test -p dsh-cordis --features compio --test kernel --offline       # 28 passed
cargo test -p dsh-cordis --test conformance --offline                    # 16 passed
cargo test -p dsh-cordis --features compio --test conformance --offline  # 16 passed
cargo test --workspace --features compio --offline    # conformance 16 + kernel 28 + compio_backend 6 + tokio_backend 6 + doctest 2 = 58 passed（命名套件合计）
cargo clippy --workspace --offline -- -D warnings
```

## 下一步

1. 服务接口化：支持 `ServiceKey<dyn Trait>` / 接口 marker，使不同 provider 可互换
   （本期已完成具体类型化的 `ServiceKey<T>`，trait 对象暂需用具体句柄包装）；
2. Loader / profile / bundle / patch（含完整配置 schema 校验）；
3. `dsh-plugin-host`：QuickJS realm + Cordis JS 内核 + 能力桥；
4. 用同一套 kernel 跑 agent loop 的最小回合，验证 `agent/pre-step` 这类 waterfall 事件。
