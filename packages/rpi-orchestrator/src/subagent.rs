//! The `subagent` tool: one bounded delegation to a child `rpi` process.
//!
//! This is the primitive the `dispatch` scheduler fans out over, and it is also
//! usable on its own — the host already parallelises tool calls, so a turn that
//! issues several `subagent` calls already runs them concurrently.

use crate::graph::{
    DEFAULT_MAX_TURNS, DEFAULT_TIMEOUT_SECS, MAX_MAX_TURNS, MAX_TASK_BYTES, MAX_TIMEOUT_SECS,
    MIN_TIMEOUT_SECS,
};
use crate::kit::{content_only, optional_string, optional_trimmed, string_param};
use crate::spawn;
use serde_json::Value;

pub fn subagent(params: &Value) -> Result<String, String> {
    let task = string_param(params, "task")?;
    if task.trim().is_empty() {
        return Err("task must not be empty".into());
    }
    if task.len() > MAX_TASK_BYTES {
        return Err(format!("task must be at most {MAX_TASK_BYTES} bytes"));
    }

    // `role` is recorded but does not yet change behaviour (design §7 "只记录").
    let role = optional_string(params, "role", "worker");
    let max_turns = params
        .get("maxTurns")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_TURNS)
        .clamp(1, MAX_MAX_TURNS);
    let timeout_secs = params
        .get("timeoutSeconds")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_TIMEOUT_SECS)
        .clamp(MIN_TIMEOUT_SECS, MAX_TIMEOUT_SECS);
    // Optional provider/model passthrough: the child shares the agent dir and
    // would otherwise fall back to its *default* provider, which is not
    // necessarily the one the orchestrator was started with.
    let provider = optional_trimmed(params, "provider");
    let model = optional_trimmed(params, "model");

    let output = spawn::run(
        &task,
        max_turns,
        timeout_secs,
        provider.as_deref(),
        model.as_deref(),
    )?;
    Ok(content_only(&format!(
        "[subagent role={role} turns<={max_turns} timeout={timeout_secs}s provider={} model={}]\n{output}",
        provider.as_deref().unwrap_or("-"),
        model.as_deref().unwrap_or("-"),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_missing_or_empty_task() {
        assert!(subagent(&serde_json::json!({})).is_err());
        assert!(subagent(&serde_json::json!({ "task": "   " })).is_err());
    }

    #[test]
    fn rejects_oversized_task() {
        let task = "x".repeat(MAX_TASK_BYTES + 1);
        assert!(subagent(&serde_json::json!({ "task": task })).is_err());
    }
}
