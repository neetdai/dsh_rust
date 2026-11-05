# agent.md — Rust 复刻 DeepSeek Harness 工作手册

> 本文件面向在此仓库工作的 AI agent 与新成员，是**决策记录 + 工作规约 + 当前进度**。
> 用户向说明见 [README.md](README.md)。上游参考实现位于 `D:\deepseek-harness\`，**只读**，不要修改。
> 代码与本文冲突时以代码为准，并立即修正本文。

---

## 0. 项目目标

用 Rust 行为级复刻 DeepSeek Harness：**单线程 Cordis 兼容内核 + 原生 UI + TS/QuickJS 插件系统**，全程不引入 Node、Chromium、WebView。

当前仓库只完成内核地基（runtime 抽象 + Cordis 最小内核），**不是可用产品**。不要按“已经能替代原版”的前提写代码或做承诺。

### 范围基线

| 项目 | 基线 |
|---|---|
| 核心复刻范围 | P0+P1 约 70 个包，Rust 约 9–12 万行 |
| 全量 355 个包 | 约 20+ 人年，**不作为目标** |
| 总工期 | 3–4 名高级工程师 16–20 个月，6–8 人年 |
| MVP | P1+P2，约 7–8 个月 / 3 人 |
| 当前阶段 | P0 五项 spike 中，Cordis 内核已落地；TS 插件桥、wasm、UI 原型、差分测试骨架未开始。P1 未开始 |

---

## 1. 硬约束（不可自行放宽）

1. **核心必须行为级复刻**，不是“做一个类似的 agent”。
2. **插件系统必须能跑 TypeScript**；wasm 是可选的隔离后端。
3. **UI 必须多平台**：Windows、Linux、macOS/其他 Unix 完整支持；Android 有原生 UI；iOS 做远程客户端。
4. **不使用浏览器内核 / WebView / Tauri / Electron / Node 嵌入**做 UI 或插件运行时。
5. **低内存优先**：低端设备要能跑，不能靠“加内存解决”。
6. **能打开用户现有的 dsh 会话**（session v4），这是迁移的核心卖点。

### 兼容性分级（已拍板）

| 级别 | 含义 | 目标 |
|---|---|---|
| **L0 行为/数据兼容** | session JSONL v4、CLI 语义、事件流、配置分层、profile/bundle/patch | ✅ 必须 |
| **L1 API 兼容** | Cordis 插件 API + dsh 服务接口 | ✅ 必须 |
| **L2 源码兼容** | 现有插件源码经 `dshc` 打包/少量修改可运行 | ✅ 目标：官方常用插件 + 第三方纯 JS 插件 |
| **L3 二进制兼容** | 直接 `import` npm 上的 dsh 插件包 | ❌ 不做 |

UI 侧：

| 级别 | 含义 | 目标 |
|---|---|---|
| **U1** | 第一方 UI 用 Rust 原生实现 | ✅ |
| **U2** | 新 UI 插件 ABI（声明式视图树 + 插槽） | ✅ Phase 2 |
| **U3** | React 桥 + CSS 子集引擎 | 🔬 仅研究，v1 不做 |
| **U4** | 默认关闭的 WebView 孤岛 | ⚠️ 可选逃生舱，不用于主界面 |

---

## 2. 已定架构决策（不要轻易推翻）

| # | 决策 | 理由 | 不要做什么 |
|---|---|---|---|
| 1 | 内核 **单线程引擎**，future **不是 `Send`** | compio 的 runtime 是 `!Send`/`!Sync`；Cordis/QuickJS 也是单线程语义 | 不要给内核 future 加 `Send` 约束，不要为了“并行”把内核改成多线程 |
| 2 | 执行器抽象为 `Runtime` trait，**Tokio 默认，compio 可选** | 两者 API 差异被 trait 吸收；Tokio 生态成熟 | 不要在 kernel 公共 API 暴露 tokio/compio 类型；切换只改 feature 与测试 `Backend` 别名 |
| 3 | Rust 内核负责服务/事件/fiber/生命周期；**JS 可见的监听器链未来放 JS 侧** | 跨语言实现 `waterfall` 的 `next()` 续延复杂度会爆炸 | 不要试图用 Rust 复刻 `ctx.on` 的 JS 闭包链；事件无 JS 监听器时走 Rust 快路径 |
| 4 | **Session log 是唯一事实源** | 可恢复、可回放、可审计 | 不要维护一套“内存消息”与日志双写；`deriveMessages()` 只能从日志派生 |
| 5 | **Host/Client 分离**：桌面单进程双角色，`dsh serve` 暴露 WS；iOS 纯客户端 | iOS 无子进程/PTY/LSP、禁 JIT、后台受限 | 不要承诺 iOS 本地跑完整 core；不要为移动端把 host 硬塞进 app |
| 6 | 插件分三档：纯 JS 打包 → QuickJS realm → wasm 隔离 | L3 不可行（官方包依赖原生模块与 React） | 不要承诺 npm 插件原样运行；不要支持 native addon；QuickJS realm **不是安全边界** |
| 7 | UI 用**原生视图树 + 插槽**，第一方 Rust 与插件共用同一棵树 | 无 WebView、低内存、可控 | 不要引入 HTML/CSS 引擎；不要接受任意 CSS；v1 不移植 React UI |
| 8 | 不可信插件必须进 wasm 或独立进程 | 能力隔离不能靠 QuickJS | 不要把 QuickJS 的 heap limit 当成安全沙箱 |

### 2.1 Runtime 层

- 内核只依赖 `spawn` / `spawn_blocking` / `sleep` / `block_on`，不直接依赖 I/O 库。
- 阻塞或 CPU 密集工作必须走 `Runtime::spawn_blocking`；**禁止**在 async 任务里 `std::thread::sleep` 或同步 I/O。
- 取消是**协作式**的：`TaskHandle::abort()` 在下一个 await 点生效；`spawn_blocking` 开始后不可中断。
- compio 的 `JoinHandle` **drop 会取消任务**，实现里必须显式 `detach()`；Tokio 用 `LocalSet + spawn_local`。
- 不要在持有 `RefCell` 借用时 `.await`；借用必须收敛在同步作用域内。

### 2.2 Cordis 内核语义

以下行为已有测试覆盖，修改时必须保持：

- 服务只在**提供者 fiber 处于 `Active`** 时对其他人可见；同 scope 重复 `provide` 报错；`isolate(key)` 让该 key 的查找在本层终止。
- `effect` 立即执行，fiber 卸载时**反序**执行 disposer，且幂等。
- 监听器注册本身是一个 effect，随 fiber 卸载自动移除；事件注册支持 `EventOptions { prepend, once }`。五种事件的 `once` 都在派发开始时从总线移除：本次派发仍参与且最多执行一次，后续/重入派发不再看到它（emit 回调内重入 emit 同一事件也安全）。
- `apply` 失败要回滚：已注册 effect、已 provide 服务、已挂载的子插件全部清理，fiber 进入 `Failed`。
- 提供者卸载时，`Active` 或 `Loading`（apply 未完成）且 inject 不可解析的依赖者会 **unload 回 `Pending`**：Loading 的 apply task 被 abort，apply generation 递增；apply future 外层 poll 闸门每次 poll 前比对 generation，不匹配后绝不再 poll 旧 future，因此旧代不会在新代 Loading/Active 期间注册 listener/服务/task（Pending 回滚保留作兜底）。反序清理 effects、服务、子插件，保留插件实例/ctx/已归一化 config；依赖恢复后自动重新 `apply`，`finish_apply` 在置 Active 前会复查 inject。unload 不通知 join waiter（join 继续等待 reload/显式 dispose）。显式 `dispose()` 才是终态 `Disposed`。
- `waterfall` 监听器环绕 `next()`；不调用 `next` 即否决默认行为。
- fiber 状态机：`Pending → Loading → Active / Failed → Disposing → Disposed`，其中 `Active` 或 `Loading` 可因 inject 依赖消失回到 `Pending`（可重载）。新增状态转换必须配测试。

已知简化（技术债，不是“完成”）：

- `ServiceKey` 已按**具体 Rust 类型**编译期类型化（`ServiceKey<T>` / `ServiceKeyId`）；但尚未接口化，不支持 `ServiceKey<dyn Trait>`，替换 provider 仍需同具体类型。
- `Config` 仍是 `serde_json::Value`；只有可插拔 `ConfigValidator` 校验/归一化钩子，没有 schema 与类型化配置。
- disposer 只能是同步 `FnOnce`；disposer panic 会中断当前 fiber 的 unload（该 fiber 停在 `Disposing`，不会自动恢复）。cascade 重入守卫使用 RAII，unwind 后仍会复位，其他 fiber 的 cascade 继续生效（有回归测试）。
- 没有 `intercept` / `accessor` / `mixin` / context filter / global listener。
- 没有细粒度代码/配置热重载（只有 inject 依赖恢复的自动重载与子树重启近似）。
- 没有 `getEffects()` 诊断树。

### 2.3 插件系统（后续实现，先守住边界）

- `dshc` 随产品分发，用户不需要装 Node：
  1. `oxc` 去类型/降级；
  2. 打包纯 JS 依赖（`rolldown` 或自研 ESM bundler）；
  3. 重写 `@deepseek-ai/cordis` / `@deepseek-ai/dsh-*` 导入；
  4. `node:*` 映射到 shim；`fs` / `child_process` 走能力接口；
  5. 静态能力扫描，生成 manifest；
  6. 产出签名 `.dshp`（tar+zstd）。
- 运行时：一个 profile 一个根 realm，每个 agent scope 一个子 realm；realm 在专用线程上，宿主调用同步阻塞等待内部异步完成；设 heap / 指令超时；卸载先跑 JS disposer，再回收工具、监听器、能力。
- 插件 API 兼容清单：`inject`、`Config`、`ctx.effect`、`ctx.on/once`、五种事件、`ctx.inject(deps, cb)`、`ctx.plugin()`、`ctx.set/get`、`ctx.scope/isolate`、`ctx.logger`；服务：`tools`、`llm`、`sessions`、`systemPrompt`、`agents`、`commands`、`settings`、`credentials`、`fs`、`shell`、`subprocess`、`web`、`mcpResources`、`skills`、`sessionProjections`、`approval`。
- wasm 路线是**可选**：桌面 wasmtime，iOS 用 wasmi 或 wasmtime Pulley 解释器（禁 JIT）。Javy 现有导出 ABI 不支持参数/返回值，不能直接当插件 ABI；先 spike 再定。

### 2.4 Session 与 agent loop

实现时必须复刻 `D:\deepseek-harness\docs\architecture.md` 的 turn flow：

```text
turn/start → claim → assemble → agent/pre-step → step/start
→ agent/request → freeze → stream → tool/* → step/end
→ … → turn/end
```

不变量：

- **Model-visible ⟺ logged**：模型看不到未写入日志的内容，日志里有的才能被模型看到。
- `deriveMessages()` 只从日志派生，不另存一份消息状态。
- `assistant/message` 内嵌完整流；失败/取消不提交半成品。
- 重试不重复执行 `agent/pre-step`；取消时 system/user 消息都不提交。
- 工具欠账继续下一步；inbox 与 turn boundary 语义按上游。
- session 格式为 **v4 JSONL**（可 `.zstd`），实现相邻迁移链（v0→v1→…→v4），必须能读写用户现有会话。

### 2.5 UI

- 视图树节点：布局（Column/Row/Grid/Scroll/List）、文本（Text/RichText/Markdown/CodeBlock/Diff）、控件（Button/Input/Select/Toggle/…）、`Slot(slotId)`、`Terminal/Canvas`。
- 样式只接受设计 token（间距/颜色/圆角/字号/字重/flex），**不接受任意 CSS**。
- 事件带 `NodeId` 回传插件；插件返回新子树或 patch；UI 插件超时用上一帧缓存，不得阻塞帧。
- 插槽：`sidebar.left/right`、`chat.transcript`、`chat.composer`、`chat.toolCard`、`settings.section`、`command.palette`、`status.bar`、`conversation.node`。
- 长列表虚拟化；markdown 增量解析缓存；字体按需/系统回退，不内嵌大 CJK 字体。

### 2.6 内存与平台

目标（Phase 0 实测校准，不是当前实测值）：

| 组件 | 目标 RSS |
|---|---|
| Host 空闲 | 20–35 MB |
| Host 活跃会话（2k 消息窗口） | 60–100 MB |
| QuickJS realm（5 个纯 JS 插件） | +4–8 MB |
| 桌面 UI（egui+wgpu） | 50–100 MB + 显存 30–80 MB |
| 移动客户端 | 60–120 MB |

| 平台 | Host | UI | 备注 |
|---|---|---|---|
| Windows | ✅ | ✅ | PowerShell、Job Object/AppContainer |
| Linux | ✅ | ✅ | bwrap/Landlock |
| macOS | ✅ | ✅ | Seatbelt、公证 |
| BSD/其他 Unix | ⚠️ best effort | ✅ | X11/Wayland |
| Android | ⚠️ lite 可选 | ✅ | 无 bash/PTY/LSP 承诺 |
| iOS | ❌ | ✅ | 远程客户端；无 JIT/子进程 |

---

## 3. 仓库现状

```
crates/
  dsh-runtime/
    src/lib.rs          # Runtime / RuntimeExt / TaskHandle / 协作式取消
    src/tokio_impl.rs   # TokioRuntime（LocalSet）
    src/compio_impl.rs  # CompioRuntime
    tests/              # tokio_backend.rs / compio_backend.rs 各 6 个用例
  dsh-cordis/
    src/context.rs      # Context 公共 API：provide/get/effect/on*_with/plugin/inject/scope
    src/event.rs        # 五种事件 + EventBus + Next + EventOptions
    src/fiber.rs        # fiber 生命周期、effect 反序回收、waiters、任务
    src/plugin.rs       # Plugin / FnPlugin / Config / ConfigValidator / PluginError
    src/registry.rs     # mount/start/wake_pending/cascade/unload_to_pending/dispose + apply generation/poll gate
    src/service.rs      # ServiceKey<T> / ServiceKeyId / ServiceStore / ServiceEntry
    src/error.rs        # CordisError
    tests/kernel.rs     # 28 个内核用例，Tokio/compio 各跑一遍
    tests/conformance.rs # 16 个独立一致性用例（验证者维护，Tokio/compio 各跑一遍）
src/main.rs             # provider → consumer 的 inject 演示
README.md               # 用户向说明
```

`Cargo.lock` 按规约 10 应纳入版本控制；当前仍未 tracked，提交前需执行
`git add Cargo.lock`。`.cargo-home/` 与 `target/` 仍被忽略。

已验证（2026-11-04）：

- `cargo fmt --all -- --check`：通过
- `cargo clippy --workspace --all-targets --offline -- -D warnings`：通过
- `cargo test --workspace --offline`：kernel 28 + conformance 16 + runtime(tokio) 6 + doctest 2 = 52 通过（命名套件合计）
- `cargo test -p dsh-cordis --features compio --test kernel --offline`：28 通过
- `cargo test -p dsh-cordis --test conformance --offline`：16 通过
- `cargo test -p dsh-cordis --features compio --test conformance --offline`：16 通过
- `cargo test --workspace --features compio --offline`：conformance 16 + kernel 28 + compio_backend 6 + tokio_backend 6 + doctest 2 = 58 通过（命名套件合计）
- `cargo test -p dsh-runtime --features compio --test compio_backend --offline`：6 通过

### 快速导航

| 要改什么 | 先读 |
|---|---|
| 服务解析 / scope / isolate | `service.rs`、`context.rs` |
| 事件派发 / waterfall / once | `event.rs`、`tests/kernel.rs` |
| 生命周期 / 回滚 / 级联卸载 / inject reload | `fiber.rs`、`registry.rs` |
| 执行器 / 取消 / 超时 | `dsh-runtime/src/lib.rs` + 后端实现 |
| 插件 API 形状 | `plugin.rs`、`context.rs` |

---

## 4. 编码规约

1. **Rust 2024，稳定版工具链**（当前 rustc 1.99）。不要用 nightly 特性。
2. **内核类型用 `Rc`/`RefCell`/`Cell`**；只有跨线程的服务负载用 `Arc<T: Send + Sync>`。不要为了“以后可能并行”提前引入 `Arc<Mutex<...>>`。
3. **不要给内核 future 加 `Send` 约束**，不要引入 `async-trait` 作为捷径；手写 `LocalBoxFuture` 是刻意的。
4. **内核尚未发布（0.x）**，允许必要的破坏性变更，但必须一次完成、改完所有调用点，并同步 README / 本文件；进入 P1 后公共 API 原则上只增不改。
5. **错误用 `Result`，别 panic**。`expect` 只用于内部不变量；disposer 里禁止 `unwrap`。
6. **借用不跨 await**。跨 await 的 `RefCell` 借用会导致运行时 panic，编译器不会拦。
7. 命名：crate 用 `dsh-*`；`ServiceKey` 常量用 `SCREAMING_SNAKE_CASE`；事件名用 `domain/action`（如 `agent/pre-step`）。
8. 代码与 rustdoc 用英文；`README.md`、`agent.md`、设计文档用中文。
9. **依赖最小化**：内核算子不引入网络/UI/数据库依赖；新增依赖要在提交说明里写理由。`dsh-runtime` 的后端依赖必须 feature-gated。
10. `Cargo.lock` 属于应用型 workspace，应纳入版本控制，不要加进 `.gitignore`。
11. 每次行为变更同步更新 README / agent.md；架构决策变更必须先征得用户同意。

---

## 5. 构建与测试

```powershell
$env:CARGO_HOME='D:\rust\deepseek_harness_rust\.cargo-home'

cargo fmt --all -- --check
cargo clippy --workspace --all-targets --offline -- -D warnings
cargo test --workspace --offline

# compio 后端（同一份内核用例）
cargo test -p dsh-runtime --features compio --test compio_backend --offline
cargo test -p dsh-cordis --features compio --test kernel --offline

# 独立一致性验证（验证者维护；实现任务不要改 conformance.rs）
cargo test -p dsh-cordis --test conformance --offline
cargo test -p dsh-cordis --features compio --test conformance --offline

# 演示
cargo run --offline
```

环境注意：

- `.cargo-home` 是本地离线 registry 缓存（约 500 MB，已 gitignore）。不存在时去掉 `--offline` 走网络。
- PowerShell 里 `cargo ... 2>&1 | ...` 会把 stderr 变成 ErrorRecord，可能让 `$LASTEXITCODE`/`$?` 显示失败，即使 `test result: ok`。判断结果看输出，不要只看退出码。
- Windows 下若工作区写入/读取被拒，先按 ACL 问题排查（本环境有 `diagnose-windows-sandbox-acl` skill），不要改路径或放弃写入。

### 测试规则

- 新内核行为加到 `crates/dsh-cordis/tests/kernel.rs`，**必须同时在 Tokio 和 compio 下通过**（用 `Backend` 别名，不要为后端写两套逻辑）。
- `tests/conformance.rs` 由独立验证者维护，实现任务只读、不修改；实现回归加到 `kernel.rs`。
- 后端专属行为才写 `tests/tokio_backend.rs` / `tests/compio_backend.rs`。
- 测公共行为，不测私有实现；用例必须确定性，不依赖真实网络/时间；需要时间用 runtime 的 `sleep/timeout`。
- 每个 bug 修复补回归用例；失败路径（回滚、取消、级联卸载、重复 provide）必须有覆盖。
- 不写“只跑一次看看”的临时测试进仓库。

### 完成定义（DoD）

一个任务算完成，至少满足：

1. `cargo fmt --check` 通过；
2. `cargo clippy --workspace --all-targets --offline -- -D warnings` 通过；
3. 默认 feature 与 compio feature 的相关测试通过；
4. 新增/变更的公共 API 有 rustdoc；
5. README / agent.md 同步；
6. 没有无关重构、没有把 `.cargo-home`/`target`/临时日志提交进去。

---

## 6. 路线图与当前待办

### 里程碑

| 阶段 | 时长 | 退出标准 |
|---|---|---|
| **P0 可行性 spike** | 4 周 | Cordis 内核、TS 插件桥、wasm guest、egui/Slint 原型、差分测试骨架各有 go/no-go |
| **P1 内核 + headless** | 10–12 周 | 读写 v4 session；`dsh -p` 可用；20 个 golden 场景通过 |
| **P2 插件 + UI MVP** | 12–14 周 | `dshc` + 插件运行时 + 聊天/审批/设置；10+ 插件跑通 |
| **P3 对齐 + 移动** | 16–18 周 | MCP/终端/jobs/LSP/skills/plan/goal/schedule/compaction；Android/iOS 客户端；`dsh serve` |
| **P4 加固 + 兼容** | 16–20 周 | 三平台沙箱、可访问性、i18n、兼容清单、性能与内存、签名分发 |

### 当前待办（按依赖顺序）

1. **`ServiceKey` 接口化**
   - 目标：消费者依赖 `Arc<dyn Trait>`，provider 可替换而不改消费者；`isolate` 语义不变。
   - 现状：具体类型化的 `ServiceKey<T>` / `ServiceKeyId` 已完成；接口 marker 与 `ServiceKey<dyn Trait>` 尚未做。
   - 验收：两个同接口 provider 的替换用例；现有内核用例（当前 28 个 kernel + 16 个 conformance）不回归。
2. **Session v4 核心**
   - 事件模型、log、`deriveMessages`、projection、v0→v4 迁移链。
   - 验收：读取真实 v4 会话 fixture，派生消息与原版一致；round-trip；损坏日志有明确错误。
3. **Config / Loader / profile / bundle / patch**
   - 类型化配置 + schema 校验；YAML 分层；按 row id 整段替换；`--dump-config` 确定性输出。
   - 验收：分层/覆盖/非法字段/未知字段用例；patch 语义测试。
4. **最小 agent turn**
   - 事件定义 + turn flow + replay LLM adapter；工具调用；取消与重试。
   - 验收：固定场景事件序列与 golden 一致；取消不提交；重试不重复 pre-step。
5. **`dsh-plugin-host`（QuickJS）**
   - 根 realm + scope 子 realm、JS Cordis 内核、能力桥、工具注册端到端、heap/超时限制。
   - 验收：示例 TS 插件经 `dshc` 打包后加载；`ctx.tools.register` 可调用；卸载清理干净；超限被终止。
6. **`dshc` 工具链**
   - TS 去类型、打包纯 JS 依赖、`node:*` shim、能力扫描、`.dshp` 签名产物。
   - 验收：含 `zod` 等纯 JS 依赖的插件可构建并运行；产物可复现。
7. **UI 选型 spike**
   - `dsh-ui-model` + egui / Slint 双原型；Windows + Android 真机 + iOS 模拟器测启动、RSS、IME、CJK、触摸。
   - 验收：数据记录成表，给出主后端 go/no-go。

> 顺序原则：**P1（1–4）先做到 headless 可用，再进 P2（5–7）**。不要在 agent loop 还不存在时先做 UI。

---

## 7. 不要做（硬性）

- ❌ 引入 WebView / Tauri / Electron / Chromium / Node 嵌入 / 每插件一个 V8 isolate。
- ❌ 追求 npm 插件二进制兼容（L3）。
- ❌ 支持 native addon（koffi、better-sqlite3、sharp 等）。
- ❌ 把 355 个包全量重写；核心范围按约 70 个包控制。
- ❌ v1 移植 React UI 或自研 CSS 引擎。
- ❌ 把内核改成多线程 / 给 future 加 `Send` / 用 `async-trait` 绕过单线程模型。
- ❌ 在引擎线程上做阻塞 I/O、`thread::sleep`、CPU 密集计算。
- ❌ 破坏 session v4 格式；未写迁移就改字段。
- ❌ 在 kernel crate 引入 UI、HTTP、数据库依赖。
- ❌ 提交 `.cargo-home/`、`target/`、测试临时日志。
- ❌ 擅自改变第 2 节的架构决策；要改先写清理由，征得用户确认。

---

## 8. 风险清单（做取舍时先看这里）

| 风险 | 后果 | 对策 |
|---|---|---|
| 插件兼容预期过高（想要 npm 原样跑） | 目标崩 | 守住 L1/L2；兼容矩阵公开；P0 先 spike |
| Cordis / loop 语义偏差 | 行为不一致 | 原版当 oracle 的差分测试；移植上游行为测试；golden session |
| 跨语言事件/异步桥 | 卡住核心 | JS 侧持有监听器链；同步桥 + 专用线程；先最小原型 |
| wasm guest / iOS 解释器 | 安全隔离缺失 | TS 直接跑 QuickJS 作主路径；wasm 仅可选，不阻塞主线 |
| UI 工作量（上游 66 个客户端包） | 延期 | 只做 MVP 屏幕；不追像素级；U2 新 ABI |
| egui 移动端/IME 不达标 | 体验差 | P0 真机对比 Slint；保留渲染器抽象 |
| iOS/Android 平台限制 | 功能缺失 | Host/Client 分离；iOS 远程；能力裁剪 |
| 沙箱跨平台 | 安全/工期 | 先策略 + 审批，后内核隔离；分平台推进 |
| 许可/品牌 | 法务 | 保留 MIT 归属与 THIRD_PARTY_NOTICES；不要用 DeepSeek 官方品牌；Slint 是 GPLv3/royalty-free/commercial 三选一，采用前先评估 |

---

## 9. 汇报格式

完成任务后按以下结构汇报，便于用户快速判断：

1. **做了什么**（一句话）与为什么；
2. **改动文件**（路径 + 作用）；
3. **验证结果**（实际跑过的命令 + 结果，不要只说“应该可以”）；
4. **已知限制 / 未做**；
5. **下一步建议**；
6. 若改了架构决策或公共 API，单独标注，并确认 README / agent.md 已更新。

---

## 10. 参考

- 本仓库用户向说明：[README.md](README.md)
- 上游架构：`D:\deepseek-harness\docs\architecture.md`
- 上游 Cordis 入门：`D:\deepseek-harness\docs\cordis-primer.md`
- 上游插件管理：`D:\deepseek-harness\packages\boot\plugin-manager\README.md`
- 上游连接层：`D:\deepseek-harness\packages\client\connection\README.md`
- 外部：[Slint FAQ](https://slint.dev/faqs)、[Javy](https://github.com/bytecodealliance/javy)、[wasmtime iOS/Pulley 讨论](https://github.com/bytecodealliance/wasmtime/issues/12251)
