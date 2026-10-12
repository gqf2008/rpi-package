# rpi-orchestrator

Bounded subagent orchestration for the `rpi` Rust agent. A subagent is a child
`rpi` process; the orchestrator's fan-out and fan-in already exist in the host
(parallel tool calls, results returned to the context), so this extension only
adds the delegation primitive and, later, the DAG scheduler on top of it.

Design and staged plan: [`DESIGN.md`](DESIGN.md). How it collaborates with the
host, `rpi-goal` and `rpi-mcp-adapter`: [`ARCHITECTURE.md`](ARCHITECTURE.md).

## Status

Three tools share one in-memory task graph. It lives for the life of a run and
is cleared when the session is switched; deciding whether to resume from history
or start over is left to the human. `plan`/`dispatch` results still carry the
graph in `details` for inspection.

### `subagent` — one bounded delegation

Run one task in a fresh `rpi -p` subprocess and return its final answer.

```json
{"task":"review packages/rpi-lens for bugs","role":"reviewer","maxTurns":6,"timeoutSeconds":120}
```

| param | required | default | bounds |
| --- | --- | --- | --- |
| `task` | yes | — | 1–20000 bytes, non-empty |
| `role` | no | `worker` | recorded only today |
| `provider` | no | child default | passed to the child as `--provider` |
| `model` | no | child default | passed to the child as `--model` |
| `maxTurns` | no | 8 | 1–32 |
| `timeoutSeconds` | no | 300 | 10–1800 |

### `plan` — author/edit the DAG

| action | params | effect |
| --- | --- | --- |
| `set` | `nodes` | replace the graph (validates ids, deps, acyclicity) |
| `update` | `addNodes` / `removeIds` / `setDeps` / `reset` | edit the graph in place |
| `show` | — | render the graph |

A node is `{id, task, deps[], role?, provider?, model?, maxTurns?, timeoutSeconds?, expectFile?, verify?}`.

**Completion is written only by `dispatch`** — there is no way to hand-mark a node
`done`, so a plan cannot be "completed" by the model. `reset` re-arms a node.

### `dispatch` — run one wave

Runs every pending node whose dependencies are all `done`, up to `maxParallel`
children at once (default 4, cap 16), then reports each node's result summary and
the updated graph. One call advances **one wave**, so the orchestrator sees the
results and can reshape the graph before the next call.

Guards (all enforced in code, not asked of the model):

- a cycle is rejected by `plan` before anything runs;
- a wave with nothing ready and pending nodes left reports "no progress" instead
  of spinning;
- a failed node makes its dependents `skipped` transitively;
- **per-node acceptance**: after a child exits successfully, a node that declared
  `expectFile` must have produced it, and one that declared `verify` must pass
  its shell command — otherwise the node is `failed`. Without this a child that
  hit its turn cap still exits 0 and would be recorded as `done`;
- **non-blocking and cancellable**: each tool's builder runs on a worker thread,
  so `poll` never blocks. Progress streams as a pending partial (per wave for
  `dispatch`), and cancelling the run kills the in-flight child **and its
  descendants** within ~100 ms — the tree is walked because nested shells
  (rpi's own `bash` tool) put themselves in a new process group;
- **no session litter**: children run with `--no-session`, so a wave does not
  leave a ~2 MB session file per node in the parent's `.rpi/sessions/` directory.

### Provider / model passthrough

The child shares the orchestrator's agent dir, so with no `provider`/`model` it
uses that dir's **default** provider. If the orchestrator was started with an
explicit `--provider`/`--model`, pass the same values here so the child does not
silently use a different (or misconfigured) default.

## How it stays bounded

- **Turn budget** — `maxTurns` is passed as `RPI_MAX_TURNS_PER_RUN`, which the
  harness enforces regardless of what the child model wants.
- **Wall-clock budget** — the host kills the child after `timeoutSeconds`.
- **Recursion guard** — the child is launched with `--exclude-tools subagent`,
  so a subagent cannot delegate again.
- **Output bound** — child output is truncated before it re-enters the
  orchestrator's context.

## Task transport

The task is written to the child's **stdin** rather than passed positionally:
`rpi -p` treats the next non-flag token as its prompt, so a task beginning with
`-` would otherwise be parsed as a CLI flag. Piped stdin becomes the initial
prompt (`rpi-cli::app::build_initial_message`).

## Autonomy (opt-in guidance)

Left alone, a capable orchestrator model just does the work itself (it solved a
test goal with `bash` and never touched `plan`/`dispatch`). To make it actually
orchestrate, this extension can append a short workflow section to the system
prompt — **only when opted in**, because it is installed globally:

```bash
RPI_ORCHESTRATOR_GUIDE=1 rpi -p "..."
```

The guidance is never injected into a subagent (children carry
`RPI_ORCHESTRATOR_SUBAGENT=1` and get no `plan`/`dispatch` anyway).

With it on, an unattended run of “four independent crate summaries plus a
dependent index” self-planned a 5-node graph, ran the four summaries in one
parallel wave, then the index node after them, and finished — no human relaying.

## Recursion guard

The child is launched with `--exclude-tools subagent,plan,dispatch`, so a
subagent cannot orchestrate again.

## Build

Built against the local `rpi-plugin-sdk` (see the workspace `Cargo.toml`).

```bash
cargo test -p rpi-orchestrator
```
