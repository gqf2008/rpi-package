//! The `dispatch` tool: run one wave of the DAG.
//!
//! Scheduling is deterministic code, not a model decision: the ready set is
//! "pending nodes whose dependencies are all done", and it is executed with at
//! most `maxParallel` concurrent child processes. One call advances **one wave**,
//! so the orchestrator sees the results and can reshape the graph before the
//! next call.

use crate::graph::Graph;
use crate::graph_state;
use crate::kit::result_with_details;
use crate::spawn;
use serde_json::Value;

const DEFAULT_MAX_PARALLEL: usize = 4;
const MAX_PARALLEL_CAP: usize = 16;
const RESULT_SUMMARY_BYTES: usize = 2_000;

pub fn dispatch(params: &Value) -> Result<String, String> {
    let max_parallel = params
        .get("maxParallel")
        .and_then(Value::as_u64)
        .unwrap_or(DEFAULT_MAX_PARALLEL as u64)
        .clamp(1, MAX_PARALLEL_CAP as u64) as usize;

    let mut guard = graph_state().lock().unwrap_or_else(|p| p.into_inner());
    if guard.nodes.is_empty() {
        return Err("no plan yet: call plan(action=set, nodes=[...]) first".into());
    }

    let ready = guard.ready();
    if ready.is_empty() {
        if guard.has_pending() {
            let blocked = guard.pending_ids().join(", ");
            return Err(format!(
                "no progress: no pending node is ready (blocked by unmet dependencies): {blocked}"
            ));
        }
        return Ok(result_with_details(
            &format!("nothing left to dispatch\n\n{}", guard.render()),
            guard.to_details(),
        ));
    }

    for id in &ready {
        if let Some(node) = guard.get_mut(id) {
            node.status = crate::graph::Status::Running;
        }
    }

    // Snapshot before releasing the lock into the wave: the children run for a
    // long time and must not hold the state lock.
    let snapshot = guard.clone();
    let outcomes = run_wave(&snapshot, &ready, max_parallel);

    for (id, outcome) in outcomes {
        if let Some(node) = guard.get_mut(&id) {
            match outcome {
                Ok(text) => {
                    node.status = crate::graph::Status::Done;
                    node.result = Some(summarize(&text));
                }
                Err(error) => {
                    node.status = crate::graph::Status::Failed;
                    node.result = Some(summarize(&error));
                }
            }
        }
    }
    guard.propagate_skips();

    let text = format!("dispatched {} node(s)\n\n{}", ready.len(), guard.render());
    Ok(result_with_details(&text, guard.to_details()))
}

/// Run `ids` with at most `max_parallel` children at a time, preserving input
/// order in the returned pairs.
fn run_wave(
    graph: &Graph,
    ids: &[String],
    max_parallel: usize,
) -> Vec<(String, Result<String, String>)> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(max_parallel.max(1)) {
        let chunk_results = std::thread::scope(|scope| {
            let handles: Vec<(String, _)> = chunk
                .iter()
                .map(|id| {
                    let node = graph.get(id).cloned().expect("ready node exists");
                    let id = id.clone();
                    let handle = scope.spawn(move || {
                        spawn::run(
                            &node.task,
                            node.max_turns,
                            node.timeout_secs,
                            node.provider.as_deref(),
                            node.model.as_deref(),
                        )
                    });
                    (id, handle)
                })
                .collect();
            handles
                .into_iter()
                .map(|(id, handle)| {
                    let outcome = handle
                        .join()
                        .unwrap_or_else(|_| Err("subagent thread panicked".into()));
                    (id, outcome)
                })
                .collect::<Vec<_>>()
        });
        out.extend(chunk_results);
    }
    out
}

fn summarize(text: &str) -> String {
    if text.len() <= RESULT_SUMMARY_BYTES {
        return text.to_string();
    }
    let mut end = RESULT_SUMMARY_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summarize_bounds_and_stays_char_safe() {
        assert_eq!(summarize("ok"), "ok");
        let long = "あ".repeat(RESULT_SUMMARY_BYTES);
        let text = summarize(&long);
        assert!(text.ends_with('…'));
        assert!(text.len() <= RESULT_SUMMARY_BYTES + '…'.len_utf8());
    }
}
