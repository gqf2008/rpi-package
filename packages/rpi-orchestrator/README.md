# rpi-orchestrator

Bounded subagent orchestration for the `rpi` Rust agent. A subagent is a child
`rpi` process; the orchestrator's fan-out and fan-in already exist in the host
(parallel tool calls, results returned to the context), so this extension only
adds the delegation primitive and, later, the DAG scheduler on top of it.

Design and staged plan: [`DESIGN.md`](DESIGN.md).

## Status

**Stage 1 — single-node smoke (this crate).** Registers one tool:

- `subagent` — run one bounded task in a fresh `rpi -p` subprocess and return its
  final answer.

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

### Provider / model passthrough

The child shares the orchestrator's agent dir, so with no `provider`/`model` it
uses that dir's **default** provider. If the orchestrator was started with an
explicit `--provider`/`--model`, pass the same values here so the child does not
silently use a different (or misconfigured) default.

**Stage 2 — DAG orchestration.** Adds `plan` (author/edit a task graph) and
`dispatch` (run the ready set, one wave per call so the model can reshape the
graph from the results). Not implemented yet.

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

## Build

Built against the local `rpi-plugin-sdk` (see the workspace `Cargo.toml`).

```bash
cargo test -p rpi-orchestrator
```
