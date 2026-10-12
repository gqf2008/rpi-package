# Architecture — how the pieces collaborate

`rpi-orchestrator` is one of several extensions loaded into a single `rpi`
process. This file shows how it works with the host, with `rpi-goal` (budgets)
and with `rpi-mcp-adapter` (external tools).

## Static layout

```
┌────────────────────────────────────────────────────────────────────────────────┐
│ rpi main process  =  the orchestrator / "foreman"  (one LLM loop)              │
│                                                                                │
│   main agent ──► tool calls                                                    │
│                                                                                │
│   ┌─ loaded extensions (~/.rpi/agent/extensions/, manifest-managed) ────────┐  │
│   │                                                                        │  │
│   │  rpi-orchestrator            rpi-goal                 rpi-mcp-adapter   │  │
│   │  ├ tool subagent             ├ tool goal              ├ mcp_list       │  │
│   │  ├ tool plan                 ├ /goal command          ├ mcp_call       │  │
│   │  ├ tool dispatch             └ TurnEnd hook:          └ mcp_request    │  │
│   │  └ before_agent_start guide*   counts turns /                         │  │
│   │     (only RPI_ORCHESTRATOR_GUIDE=1)  expiry → pause                   │  │
│   └────────────────────────────────────────────────────────────────────────┘  │
│                                                                               │
│   built-in tools: read / write / edit / bash / grep / find / ls (parent+child) │
└───────────────────────────────────┬───────────────────────────────────────────┘
                                    │ dispatch: one child process per ready node
                                    ▼
   ┌────────────────────────────────────────────────────────────────────────┐
   │ child rpi #1   child rpi #2   child rpi #3   child rpi #4   (parallel)  │
   │                                                                        │
   │  · fresh context: cannot see the parent's conversation, only its task  │
   │  · env: RPI_MAX_TURNS_PER_RUN=<node maxTurns>      (hard turn ceiling) │
   │         RPI_ORCHESTRATOR_SUBAGENT=1                (no orchestration   │
   │                                                     guidance injected) │
   │  · args: --exclude-tools subagent,plan,dispatch     (recursion guard)   │
   │  · can use: built-in tools + MCP tools (if configured)                 │
   │  · stdout (final answer) ──► back to dispatch ──► parent context       │
   └────────────────────────────────────────────────────────────────────────┘
```

\* The guide is appended to the system prompt only when
`RPI_ORCHESTRATOR_GUIDE=1`, and never in a child.

## Example — goal: "summarise 4 crates, then build an index"

```
 human         rpi-goal           main agent(LLM)        orchestrator tools      child rpi procs
 │                │                    │                      │                       │
 │ /goal start …  │                    │                      │                       │
 │ /goal turns 20 ├─► active goal      │                      │                       │
 │ /goal time 30m │   (TurnEnd counts) │                      │                       │
 │                │                    │                      │                       │
 │  RPI_ORCHESTRATOR_GUIDE=1 ─► adds "use plan/dispatch for multi-step work"
 │                │                    │                      │                       │
 │ "finish this"  │                    │                      │                       │
 ├────────────────────────────────────► plan                 │                       │
 │                │                    ├ plan(set 5 nodes) ──►│ build graph + acyclic │
 │                │                    │   a,b,c,d free;  e deps=[a,b,c,d]           │
 │                │                    │                      │                       │
 │                │                    ├ dispatch(maxPar=4) ─► ready = {a,b,c,d}      │
 │                │                    │                      ├── spawn 4 children ──►│ ×4
 │                │                    │                      │     each read+write   │
 │                │                    │                      │◄── result / exit ─────┤
 │                │  TurnEnd #1 ◄──────┤                      │ verify expectFile     │
 │                │  turns 1/20        │◄── results fed back ─┤ mark done / failed    │
 │                │                    │                      │                       │
 │                │                    ├ dispatch(maxPar=1) ─► ready = {e}             │
 │                │                    │                      ├── spawn 1 child ─────►│
 │                │                    │                      │◄── index.md written ──┤
 │                │                    │◄── 5/5 done ─────────┤                       │
 │                │                    │ "goal met" / /goal stop                      │
 │◄───────────────┴────────────────────┘                      │                       │

 budget exhausted (20 turns / 30 min) ─► rpi-goal: paused + notice ─► agent stops
 hard turn ceiling                    ─► RPI_MAX_TURNS_PER_RUN=N (host stops the run)
 cancel (TUI Ctrl+C / RPC abort)      ─► orchestrator cancel ─► kill whole child tree
```

## Who does what

| Component | Role | Key point |
| --- | --- | --- |
| **rpi host** | Runs the agent loop, loads extensions, ships built-in tools | Also offers the hard turn ceiling `RPI_MAX_TURNS_PER_RUN` |
| **rpi-orchestrator** (this crate) | The foreman's scheduler: `plan` authors the DAG, `dispatch` runs a wave, `subagent` runs one task | Order, parallelism, acceptance and cancellation live in code; `done` is written only by `dispatch` |
| **rpi-goal** | The foreman's watch: one objective with turn/time budgets; auto-pauses + injects a notice | Bounds *how long*, not *how the work is split* |
| **rpi-mcp-adapter** | External toolbox: exposes MCP server tools | Available to parent and children |
| **child rpi process** | The worker: one node per process, fresh context | Has no orchestrator tools (recursion guard); its output returns to the foreman |

In one line: **`rpi-goal` bounds *how long*, `rpi-orchestrator` decides *how the
work is split and run*, `rpi-mcp-adapter` decides *which external tools are
available*.**
