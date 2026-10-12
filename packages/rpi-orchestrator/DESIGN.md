# ISSUE: 编排层 DAG 子代理编排（subagent orchestration）

> **归属（重要）**：本文件是**个人开发凭据 / 本地规格**，不代表已向上游登记。
> - 上游公共仓库（`bigfish1913/pi-rust`、`pi-rust/rpi-package`）**未建**任何 issue/PR/评论。
> - 未来若要正式贡献：按 `pi-rust/rpi-package` 的 feature request 模板建**一个**批次 issue，并附本文件与验证结果。
> - 归属路由依据：`pi-rust/CONTRIBUTING.md` —— “Extensions — publish `cdylib` extensions in `pi-rust/rpi-package`；ABI/loader 的问题才归 pi-rust”。
>
> 状态：阶段1 + 阶段2 已实现，单节点与 DAG 两波 e2e 均通过 · 负责人：gqf2008 · worktree：`rpi-package-wt-orchestrator` · 跟踪：https://github.com/gqf2008/rpi-package/issues/1

---

## 1. 现状（已实核，非外推）

| 事实 | 位置 | 结论 |
| --- | --- | --- |
| 同一轮多个 tool call 已并行 | `crates/rpi-agent/src/agent_loop.rs:740` `execute_tool_calls_parallel`（每个 call 一个 `tokio::spawn` 并 race） | 扇出**框架已就绪** |
| tool 结果自动回流并驱动下一轮 | `crates/rpi-agent/src/agent_loop.rs:225-330`（结果按序 append 回 context，`while has_more_tool_calls` 继续） | 回流**框架已就绪** |
| 插件工具的同步 `poll` 由 blocking driver 驱动 | `crates/rpi-extensions/src/tool.rs:444`（`spawn_blocking` 里循环 poll，转 `Pending` 为 partial） | 扩展里起子进程**不会饿死** tokio worker |
| 扩展可在自身进程内起子进程 | `rpi-package/packages/rpi-lens/src/lib.rs:25-70`（`Command::new` + `wait_timeout` + 40k 截断） | 有**现成先例** |
| 状态型工具可把状态存 tool result `details`、按分支重放 | `rpi-package/packages/rpi-todo`（README：“reconstructed by replaying tool results on the current branch”） | DAG 状态的**存储范式** |
| 轮数/时长预算 + 超限自动 pause + 注入 context | `rpi-package/packages/rpi-goal`（README “Budgets”：`maxTurns` / `deadlineMs`） | 预算的**现成范式** |
| 单次 run 的轮数硬上限 | `crates/rpi-harness/src/run_budget.rs`（`RPI_MAX_TURNS_PER_RUN`） | 子进程轮数预算落点 |
| `rpi-subagents` 的 `delegate_task` 只产信封、无消费者 | `rpi-package/packages/rpi-subagents/src/lib.rs:8-31`；README：“scheduling remains a host responsibility” | **不复用该信封**，改做真执行 |

**唯一缺口**：没有任何工具/代码真的去“起子代理并收集结果”。编排层因此只能靠人类在各 agent 间搬运结果。

## 2. 目标

让编排层（主 agent）能：
1. **生成大任务的 DAG**（节点=子任务，边=依赖），并在每波结果回来后**随时改图**。
2. **按拓扑顺序执行**：只跑依赖已完成的节点；独立节点**并行**。
3. **双层硬预算**：编排层自身（轮数 + 墙钟）+ 每个子代理（轮数 + 时长），超限即停。
4. 子代理产出自动回到编排层 context，**无需人类搬运**。

## 3. 非目标（本期不做）

- 不改 `pi-rust` 仓库、不动 ABI、不动 `active_run`/lane 内部。
- 不做 in-process 多 lane 并发（那是另一条优化路线；本方案子代理=**子进程**）。
- 不做“边跑边改图”的动态句柄模型（见 §7 阶段二）。
- 不复活 `delegate_task` 信封（无消费者，且不是必需）。

## 4. 架构

**形态**：out-of-tree 扩展（cdylib），与 `rpi-goal` / `rpi-todo` / `rpi-lens` 同构。

**分工（核心决策）**：
- **模型**：决定图长什么样（拆节点、连依赖）；看结果后**改图**。
- **代码**：拓扑排序、就绪判定、并发起子进程、回填结果。**顺序不交给模型判断**（确定性逻辑；`run_budget.rs` 已记录模型 runaway 案例）。

**子代理 = 子进程**：
```text
rpi -p "<node.task>" --exclude-tools subagent  -e ...   # 递归守卫
env: RPI_MAX_TURNS_PER_RUN=<node.maxTurns>              # 轮数预算
host: wait_timeout / tokio timeout -> kill              # 时长预算
```
- 用 `std::env::current_exe()` 拿宿主二进制（**不要**写死 `"rpi"`）。
- 子进程继承 cwd + `~/.rpi/auth.json`；⚠️ 父进程若用 `--provider/--model` 启动，这些**不在环境变量里**，需显式透传或落盘。

## 5. 接口

### 5.1 `plan`（改形；模型用）

| action | 参数 | 效果 |
| --- | --- | --- |
| `set` | `nodes[]` | 建立/替换整张图（校验 id 唯一、依赖存在、无环） |
| `update` | `addNodes[]` / `removeIds[]` / `setDeps[]` / `reset[]` | 局部改图；**没有** `status` 写入 |
| `show` | — | 渲染当前图 + 状态 |

节点：`{ id, task, deps: [id], role?, provider?, model?, maxTurns?, timeoutSeconds?, expectFile?, verify? }`
`status ∈ {pending, running, done, failed, skipped}`（**只由引擎写**）

**完成只由 `dispatch` 写**：模型无法手动把节点标成 `done`（早期 `status` 动作会让模型绕过引擎“谎报完成”，已删除）；模型最多只能 `reset` 重排。

**提交前守卫**：查环，有环直接拒绝（附环路径）。

### 5.2 `dispatch`（执行；代码调度）

- 输入：无（读当前图）或 `{ maxParallel? }`。
- 行为：算出 **ready set**（`status=pending && deps 全 done`）→ 在并发上限内**并行**起子进程 → 收集 → **逐节点验收**（`expectFile`/`verify`）→ 标记 `done`/`failed` → 返回本波总结。
- **一次调用推进一波**：让每波结果回到编排层，模型才有机会改图。

## 6. 状态与预算

**状态存储**：单次进程内存（`OnceLock<Mutex<Graph>>`），**不落文件、不做分支重放**。**会话切换/重启即清空**——是继续还是重来由人决定。`plan`/`dispatch` 的结果仍带 `details` 供查看。

**双层预算**：
- 编排层：照 `rpi-goal` 的 `maxTurns` + `deadlineMs`，超限 auto-pause + 注入提示。
- 子代理：`RPI_MAX_TURNS_PER_RUN` + 宿主超时 kill。

**硬性要求**：预算必须在**代码**里 enforce，不能只写在 prompt 里。

## 7. 阶段

- **阶段一（已实现）**：`plan` + `dispatch`（一波一同步）+ 环/无进展/递归守卫 + 子代理级预算（`RPI_MAX_TURNS_PER_RUN` + 宿主超时 kill）。已跑通“拆图 → 并行一波 → 结果回流 → 改图 → 下一波”。
  - **编排层自身预算（轮数/deadline）未在本扩展实现**：由 `rpi-goal` 的 `maxTurns`/`deadlineMs` 承担（同进程内可叠加使用）。
- **阶段二（未做，可选）**：动态图——`dispatch` 返回进行中句柄，后续轮询并随时改图（`StepResult::pending` 已支持进度流式）。

## 8. 守卫清单（必须，否则一定出事）

- [x] **环检测**：提交图前拒绝有环。
- [x] **无进展检测**：剩余节点全因依赖 `failed` 而 blocked → 报错，不死循环。
- [x] **并发上限**：`maxParallel`（默认 4、上限 16）。
- [x] **递归守卫**：子进程 `--exclude-tools subagent,plan,dispatch`，并打 `RPI_ORCHESTRATOR_SUBAGENT=1` 标记。
- [x] **输出截断**：子进程 stdout 截断后再回填（40KB），节点结果摘要截到 2KB。
- [x] **取消贯通**：工具 builder 在工作线程（`poll` 非阻塞）；`cancel`/`destroy` 置位 `Control`；`spawn` 每 100ms 检查并**递归杀整棵进程树**（孙进程可能被宿主的 bash 工具重新分组，单靠组 kill 会漏），`destroy` 不持锁 join。**e2e**：`rpi --mode rpc` 发 `abort` → 子 rpi 与孙 `sleep` 均被杀，无孤儿（单测 `cancellation_kills_a_running_child` / `cancellation_kills_the_whole_process_group`）。**已知宿主限制**：print 模式 SIGINT 不触发 run 取消（`ctrl_c` 只在 server / plugin-stdio 分支安装，`rpi-cli/src/app.rs:464`），只有 TUI Ctrl+C / RPC abort 会走到这条链路。
- [x] **配置透传**：子进程 `provider`/`model` 通过工具入参透传（父进程的 `--provider/--model` 不会被自动继承；宿主未暴露当前 model，扩展拿不到，故由调用方给出）。
- [x] **完成只由引擎写**：`plan` 无 `status` 写入，`done`/`failed` 只能由 `dispatch` 写 → 堵住“模型谎报完成”。
- [x] **逐节点验收**：子代理退出后检查 `expectFile`（文件须存在）/`verify`（命令须 exit 0），不满足标 `failed` → 堵住“子代理撞轮数上限仍 exit 0 被记为 done”的假成功。
- [x] **可观测**：`poll` 非阻塞返回 `StepResult::pending(progress)`；`subagent` 报 “running”，`dispatch` 每并发块报 “running N node(s): …”。

## 9. 引导编排层生成 DAG

- **已实现（opt-in）**：`before_agent_start` 往 system prompt 追加一段“多步/可并行任务 → 先 plan 建图、dispatch 一波波推进、按结果 update 改图”的引导。**默认关闭**，设 `RPI_ORCHESTRATOR_GUIDE=1` 才注入（本扩展全局安装，无条件注入会影响所有会话）。子代理不注入（`RPI_ORCHESTRATOR_SUBAGENT=1` 标记）。
- 未做：每轮把**当前图状态**单独注入 context——目前依赖工具结果本身留在 context 里（`plan`/`dispatch` 的输出含图状态），实测够用。

实测（引导开启、无人职守）：模型自建 5 节点图（4 独立 + 1 依赖），先后两次 `dispatch`（4 并行 → 1），全部完成。**不加引导时它根本不用这套工具**。

## 10. 验收用例

- [x] AC1：多独立节点一次 `dispatch` 并行执行、结果全部回填 —— e2e：4 节点一波并行（big test）+ 2 节点一波（scripted）。
- [x] AC2：依赖门控、分波执行、不早跑 —— 单测 `ready_set_respects_dependencies`；e2e 三波 `set(1)→dispatch→addNodes(2)→dispatch→addNodes(1)→dispatch`。
- [x] AC3：节点 `failed` → 下游 `skipped`、不空转 —— 单测 `failed_dependency_skips_downstream_transitively` + `dispatch` 的 no-progress 守卫。
- [x] AC4：含环的图被拒 —— 单测 `rejects_cycles`。
- [x] AC5：超 `timeoutSeconds` 被 kill 标 `failed` —— 与取消同一条 `kill_and_reap` 路径（单测覆盖 kill）。
- [x] AC6：`RPI_MAX_TURNS_PER_RUN` 生效 —— 实测子代理撞 `maxTurns=2` 提前停（也因此暴露“假成功”，已用 `expectFile` 验收修复）。
- [ ] AC7：编排层 `deadlineMs` 到期 auto-pause —— **不在本扩展**，由 `rpi-goal` 承担。
- [x] AC8：取消 → 在跑子进程全被 kill、无孤儿 —— **e2e（RPC abort）**：子 rpi 与孙 `sleep` 均被杀。
- [x] AC9（**砍掉**）：不做分支重放——会话切换即清空图，恢复还是重来由人决定（设计决定，非缺陷）。
- [x] AC10：子进程默认无编排工具（无递归）—— 启动参数 `--exclude-tools subagent,plan,dispatch`（单测）+ 引导跳过 `RPI_ORCHESTRATOR_SUBAGENT=1`。

## 11. 参考实现锚点

- 子进程 + 超时 + 截断：`rpi-package/packages/rpi-lens/src/lib.rs:25-70`
- 状态存 `details` + 分支重放：`rpi-package/packages/rpi-todo`
- 轮数/时长预算 + auto-pause + context 注入：`rpi-package/packages/rpi-goal`
- 插件工具 poll 驱动（非阻塞/进度）：`crates/rpi-extensions/src/tool.rs:444`
- 轮数上限环境变量：`crates/rpi-harness/src/run_budget.rs`

---

## 待确认

- [ ] 扩展命名（如 `rpi-orchestrator` / `rpi-dag`）与是否独立 repo。
- [ ] 是否要先做一次“单节点同步 `dispatch`”最小冒烟，再上 DAG。
- [ ] `role` 语义（本期是否只记录、不做 per-role prompt）。
