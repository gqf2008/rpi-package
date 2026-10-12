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

/// Optional override for the orchestration guidance injected into the system
/// prompt. Guidance is **on by default whenever a goal is active** — `/goal`
/// implies orchestration, so no variable is needed. This only forces the decision:
/// `0/false/no/off` disables it even with a goal, `1/true/yes/on` enables it even
/// without one.
pub const GUIDE_ENV: &str = "RPI_ORCHESTRATOR_GUIDE";

/// Project-relative path of `rpi-goal`'s state. An active goal there is what turns
/// the guidance on — a deliberate coupling to `/goal` mode.
const GOALS_FILE: &str = ".rpi/goals.json";

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

/// Tri-state read of [`GUIDE_ENV`]: `Some(true)` force on, `Some(false)` force
/// off, `None` means "decide from the goal state".
fn guide_override(value: Option<&str>) -> Option<bool> {
    match value.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        Some("1" | "true" | "yes" | "on") => Some(true),
        Some("0" | "false" | "no" | "off") => Some(false),
        _ => None,
    }
}

/// Whether `rpi-goal` has an active goal in this project. Parsed leniently: any
/// read or parse problem simply means "no goal".
fn active_goal_from(text: &str) -> bool {
    let Ok(value) = serde_json::from_str::<Value>(text) else {
        return false;
    };
    let Some(active) = value.get("active") else {
        return false;
    };
    !active.is_null() && active.get("status").and_then(Value::as_str) == Some("active")
}

fn active_goal_in_project() -> bool {
    let Ok(cwd) = std::env::current_dir() else {
        return false;
    };
    match std::fs::read_to_string(cwd.join(GOALS_FILE)) {
        Ok(text) => active_goal_from(&text),
        Err(_) => false,
    }
}

fn guide_enabled() -> bool {
    // Never inject into a subagent: it has no `plan`/`dispatch`, and telling it to
    // use them would just waste a turn.
    if std::env::var(crate::spawn::SUBAGENT_ENV).is_ok() {
        return false;
    }
    // `/goal` implies orchestration: an active goal turns the guidance on unless
    // the variable explicitly overrides the decision.
    guide_override(std::env::var(GUIDE_ENV).ok().as_deref()).unwrap_or_else(active_goal_in_project)
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
    fn guide_env_override_is_tri_state() {
        for value in ["1", "true", "TRUE", "yes", "on", " on "] {
            assert_eq!(guide_override(Some(value)), Some(true), "{value}");
        }
        for value in ["0", "false", "no", "off", " OFF "] {
            assert_eq!(guide_override(Some(value)), Some(false), "{value}");
        }
        assert_eq!(guide_override(None), None);
        assert_eq!(guide_override(Some("")), None);
        assert_eq!(guide_override(Some("whatever")), None);
    }

    #[test]
    fn active_goal_is_detected_from_the_project_state() {
        assert!(active_goal_from(
            r#"{"active":{"status":"active"},"archived":[]}"#
        ));
        assert!(!active_goal_from(r#"{"active":null,"archived":[]}"#));
        assert!(!active_goal_from(r#"{"active":{"status":"paused"}}"#));
        assert!(!active_goal_from(r#"{"active":{"status":"complete"}}"#));
        assert!(!active_goal_from("{}"));
        assert!(!active_goal_from("not json"));
    }
}
