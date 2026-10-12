#![forbid(unsafe_code)]

//! `rpi-orchestrator` — bounded subagent delegation for the rpi Rust agent.
//!
//! Stage 1 of the design in `DESIGN.md`: the **single-node smoke**. One
//! `subagent` tool call = one child `rpi -p` process. The orchestrator's
//! fan-out (parallel tool calls) and fan-in (tool results returned to the
//! context) already exist in the host, so nothing else is needed to turn a
//! batch of `subagent` calls into parallel delegation.
//!
//! Later stages add the `plan` / `dispatch` DAG tools on top of this primitive.

mod kit;
mod spawn;

use serde_json::Value;

/// Defaults mirror `rpi-subagents`' envelope limits so callers see one contract.
const DEFAULT_MAX_TURNS: u64 = 8;
const MAX_MAX_TURNS: u64 = 32;
const DEFAULT_TIMEOUT_SECS: u64 = 300;
const MIN_TIMEOUT_SECS: u64 = 10;
const MAX_TIMEOUT_SECS: u64 = 1800;
/// Matches the schema's `maxLength`; guards against a runaway prompt.
const MAX_TASK_BYTES: usize = 20_000;

fn clamp_u64(value: Option<u64>, default: u64, min: u64, max: u64) -> u64 {
    value.unwrap_or(default).clamp(min, max)
}

/// Read an optional string param, treating blank/whitespace as absent so an empty
/// `provider` never becomes a bare `--provider` flag.
fn optional_trimmed(params: &Value, name: &str) -> Option<String> {
    params
        .get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn subagent(params: &Value) -> Result<String, String> {
    let task = kit::string_param(params, "task")?;
    if task.trim().is_empty() {
        return Err("task must not be empty".into());
    }
    if task.len() > MAX_TASK_BYTES {
        return Err(format!("task must be at most {MAX_TASK_BYTES} bytes"));
    }

    // `role` is recorded but does not yet change behaviour (design §7 "只记录").
    let role = kit::optional_string(params, "role", "worker");
    let max_turns = clamp_u64(
        params.get("maxTurns").and_then(Value::as_u64),
        DEFAULT_MAX_TURNS,
        1,
        MAX_MAX_TURNS,
    );
    let timeout_secs = clamp_u64(
        params.get("timeoutSeconds").and_then(Value::as_u64),
        DEFAULT_TIMEOUT_SECS,
        MIN_TIMEOUT_SECS,
        MAX_TIMEOUT_SECS,
    );

    // Optional provider/model passthrough. Without them the child falls back to
    // the agent dir's *default* provider, which is not necessarily the one the
    // orchestrator is using (and is broken outright on a machine whose default is
    // misconfigured).
    let provider = optional_trimmed(params, "provider");
    let model = optional_trimmed(params, "model");

    let output = spawn::run(
        &task,
        max_turns,
        timeout_secs,
        provider.as_deref(),
        model.as_deref(),
    )?;
    Ok(format!(
        "[subagent role={role} turns<={max_turns} timeout={timeout_secs}s provider={} model={}]\n{output}",
        provider.as_deref().unwrap_or("-"),
        model.as_deref().unwrap_or("-"),
    ))
}

export_single_tool_plugin!(
    subagent,
    "subagent",
    "Delegate one bounded task to a fresh rpi subprocess and return its final answer.",
    r#"{"type":"object","properties":{"task":{"type":"string","maxLength":20000},"role":{"type":"string"},"provider":{"type":"string"},"model":{"type":"string"},"maxTurns":{"type":"integer","minimum":1,"maximum":32},"timeoutSeconds":{"type":"integer","minimum":10,"maximum":1800}},"required":["task"]}"#
);

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

    #[test]
    fn clamps_limits_to_the_schema_bounds() {
        assert_eq!(
            clamp_u64(Some(999), DEFAULT_MAX_TURNS, 1, MAX_MAX_TURNS),
            MAX_MAX_TURNS
        );
        assert_eq!(
            clamp_u64(
                Some(0),
                DEFAULT_TIMEOUT_SECS,
                MIN_TIMEOUT_SECS,
                MAX_TIMEOUT_SECS
            ),
            MIN_TIMEOUT_SECS
        );
        assert_eq!(
            clamp_u64(
                None,
                DEFAULT_TIMEOUT_SECS,
                MIN_TIMEOUT_SECS,
                MAX_TIMEOUT_SECS
            ),
            DEFAULT_TIMEOUT_SECS
        );
    }

    #[test]
    fn optional_trimmed_drops_blank_values() {
        assert_eq!(
            optional_trimmed(&serde_json::json!({ "provider": "deepseek" }), "provider"),
            Some("deepseek".to_string())
        );
        assert_eq!(
            optional_trimmed(&serde_json::json!({ "provider": "   " }), "provider"),
            None
        );
        assert_eq!(optional_trimmed(&serde_json::json!({}), "model"), None);
    }
}
