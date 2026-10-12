#![forbid(unsafe_code)]

//! `rpi-orchestrator` — DAG subagent orchestration for the rpi Rust agent.
//!
//! A subagent is a child `rpi` process. Three tools share one session-scoped
//! task graph:
//!
//! - `subagent` — run one bounded task (the delegation primitive),
//! - `plan`     — author/edit the DAG,
//! - `dispatch` — run the ready set (one wave per call).
//!
//! The graph lives in this process's memory for the life of a run. Switching
//! sessions clears it — a human decides whether to resume or start over, rather
//! than the extension silently replaying an old branch. `plan`/`dispatch`
//! results still carry the graph in `details` for inspection. Design and
//! stages: `DESIGN.md`.

mod control;
mod dispatch;
mod graph;
mod kit;
mod plan;
mod spawn;
mod subagent;

use serde_json::Value;
use std::sync::{Mutex, OnceLock};

static GRAPH: OnceLock<Mutex<graph::Graph>> = OnceLock::new();

/// The one in-memory graph. A poisoned lock is recovered rather than panicking,
/// mirroring the SDK's tool-handle convention.
pub(crate) fn graph_state() -> &'static Mutex<graph::Graph> {
    GRAPH.get_or_init(|| Mutex::new(graph::Graph::default()))
}

crate::tool_entry!(subagent_tool, crate::subagent::subagent);
crate::tool_entry!(plan_tool, crate::plan::plan);
crate::tool_entry!(dispatch_tool, crate::dispatch::dispatch);

const SUBAGENT_PARAMS: &str = r#"{"type":"object","properties":{"task":{"type":"string","maxLength":20000},"role":{"type":"string"},"provider":{"type":"string"},"model":{"type":"string"},"maxTurns":{"type":"integer","minimum":1,"maximum":32},"timeoutSeconds":{"type":"integer","minimum":10,"maximum":1800}},"required":["task"]}"#;

const PLAN_PARAMS: &str = r#"{"type":"object","properties":{"action":{"type":"string","enum":["set","update","show"]},"nodes":{"type":"array","description":"action=set: full node list. Each node: {id, task, deps?, role?, provider?, model?, maxTurns?, timeoutSeconds?, expectFile?, verify?}"},"addNodes":{"type":"array","description":"action=update: nodes to add (same node shape)"},"removeIds":{"type":"array","items":{"type":"string"},"description":"action=update: ids to remove"},"setDeps":{"type":"array","description":"action=update: [{id, deps}]"},"reset":{"type":"array","items":{"type":"string"},"description":"action=update: re-arm these ids to pending for another attempt"}},"required":["action"]}"#;

const DISPATCH_PARAMS: &str = r#"{"type":"object","properties":{"maxParallel":{"type":"integer","minimum":1,"maximum":16,"description":"max concurrent subagents for this wave (default 4)"}}}"#;

/// Opt-in switch for the orchestration guidance injected into the system prompt.
/// Off by default: this extension is installed globally, so silently rewriting
/// every session's prompt would be intrusive.
pub const GUIDE_ENV: &str = "RPI_ORCHESTRATOR_GUIDE";

const GUIDE_HEADING: &str = "## Subagent orchestration";

const GUIDE_SECTION: &str = r#"## Subagent orchestration

For work that has several steps, or steps that can run in parallel, use the
`plan` and `dispatch` tools instead of doing everything inline:

1. `plan` with action=set: write the task graph as nodes, each with an `id`, a
   self-contained `task`, and its `deps`.
2. `dispatch`: run the current ready wave. Nodes whose dependencies are done run
   in parallel (up to `maxParallel`).
3. Read the results, then `plan` with action=update (add/remove/re-dep/reset
   nodes) and `dispatch` again until the goal is met.

A subagent starts fresh and cannot see this conversation, so each node's `task`
must contain everything it needs. Prefer this over hand-rolling the whole
workflow with `bash`."#;

fn guide_enabled_from(value: Option<&str>) -> bool {
    matches!(
        value.map(|v| v.trim().to_ascii_lowercase()).as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn guide_enabled() -> bool {
    // Never inject into a subagent: it has no `plan`/`dispatch`, and telling it to
    // use them would just waste a turn.
    if std::env::var(crate::spawn::SUBAGENT_ENV).is_ok() {
        return false;
    }
    guide_enabled_from(std::env::var(GUIDE_ENV).ok().as_deref())
}

fn write_out(out: &mut rpi_plugin_sdk::callbacks::Output<'_>, value: Value) {
    if out.is_available() {
        out.write(value.to_string());
    }
}

rpi_plugin_sdk::string_handler! {
/// Append the orchestration guidance to the system prompt, only when opted in.
///
/// Replacement contract: the handler reads the base prompt from the event's
/// `systemPrompt` and writes `{"systemPrompt": "..."}` when it changed it, or
/// `{}` (no change) otherwise. A malformed envelope declines rather than
/// replacing the host's prompt with our section alone.
fn on_before_agent_start(event_json, out, _user_data) {
    if !guide_enabled() {
        write_out(out, serde_json::json!({}));
        return 0;
    }
    let parsed: Value = match serde_json::from_str(event_json) {
        Ok(value) => value,
        Err(_) => {
            write_out(out, serde_json::json!({}));
            return 0;
        }
    };
    let base = parsed
        .get("systemPrompt")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if base.contains(GUIDE_HEADING) {
        write_out(out, serde_json::json!({}));
        return 0;
    }
    let next = format!("{base}\n\n{GUIDE_SECTION}");
    write_out(out, serde_json::json!({"systemPrompt": next}));
    0
}
}

rpi_plugin_sdk::export_plugin!(|api| {
    if let Some(register_prompt) = api.register_before_agent_start {
        let _ = register_prompt(
            on_before_agent_start,
            rpi_plugin_sdk::callbacks::free_string,
            api.user_data,
        );
    }

    let Some(register) = api.register_tool else {
        return 1;
    };

    {
        let schema = Box::new(crate::kit::schema(
            "subagent",
            "Delegate one bounded task to a fresh rpi subprocess and return its final answer.",
            SUBAGENT_PARAMS,
        ));
        let rc = register(
            &*schema,
            subagent_tool::execute,
            subagent_tool::poll,
            subagent_tool::cancel,
            subagent_tool::destroy,
            crate::kit::plugin_free_string,
        );
        drop(schema);
        if rc != 0 {
            return 1;
        }
    }

    {
        let schema = Box::new(crate::kit::schema(
            "plan",
            "Author or edit the task DAG. action=set (replace with `nodes`), action=update (addNodes/removeIds/setDeps/reset), action=show. A node may declare `expectFile` (file that must exist after its child runs) and `verify` (shell command that must exit 0); `dispatch` enforces both. Completion is written only by `dispatch`, never by `plan`.",
            PLAN_PARAMS,
        ));
        let rc = register(
            &*schema,
            plan_tool::execute,
            plan_tool::poll,
            plan_tool::cancel,
            plan_tool::destroy,
            crate::kit::plugin_free_string,
        );
        drop(schema);
        if rc != 0 {
            return 1;
        }
    }

    {
        let schema = Box::new(crate::kit::schema(
            "dispatch",
            "Run one wave of the plan: every pending node whose dependencies are done, up to `maxParallel` subagents at once. Returns each node's result summary and the updated graph.",
            DISPATCH_PARAMS,
        ));
        let rc = register(
            &*schema,
            dispatch_tool::execute,
            dispatch_tool::poll,
            dispatch_tool::cancel,
            dispatch_tool::destroy,
            crate::kit::plugin_free_string,
        );
        drop(schema);
        if rc != 0 {
            return 1;
        }
    }
    0
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guide_switch_accepts_common_truthy_values() {
        for value in ["1", "true", "TRUE", "yes", "on", " on "] {
            assert!(guide_enabled_from(Some(value)), "{value} should enable");
        }
        for value in [None, Some(""), Some("0"), Some("false"), Some("nope")] {
            assert!(!guide_enabled_from(value), "{value:?} should not enable");
        }
    }
}
