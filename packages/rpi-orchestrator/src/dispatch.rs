//! The `dispatch` tool: run one wave of the DAG.
//!
//! Scheduling is deterministic code, not a model decision: the ready set is
//! "pending nodes whose dependencies are all done", and it is executed with at
//! most `maxParallel` concurrent child processes. One call advances **one wave**,
//! so the orchestrator sees the results and can reshape the graph before the
//! next call.

use crate::control::Control;
use crate::graph::{Graph, Status};
use crate::graph_state;
use crate::kit::{progress, result_with_details, Progress};
use crate::spawn;
use serde_json::Value;

const DEFAULT_MAX_PARALLEL: usize = 4;
const MAX_PARALLEL_CAP: usize = 16;
const RESULT_SUMMARY_BYTES: usize = 2_000;

pub fn dispatch(
    params: &Value,
    control: &Control,
    progress_tx: &Progress,
) -> Result<String, String> {
    control.check()?;
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
            node.status = Status::Running;
        }
    }

    // Snapshot before releasing the lock into the wave: the children run for a
    // long time and must not hold the state lock.
    let snapshot = guard.clone();
    let outcomes = run_wave(&snapshot, &ready, max_parallel, control, progress_tx);

    for (id, outcome) in outcomes {
        // Acceptance is checked only when the child reported success: a child that
        // hit its turn cap still exits 0, so without this a no-op would be `done`.
        let spec = snapshot.get(&id);
        let verified = match &outcome {
            Ok(_) => verify_node(
                spec.and_then(|node| node.expect_file.as_deref()),
                spec.and_then(|node| node.verify.as_deref()),
            ),
            Err(_) => Ok(()),
        };
        if let Some(node) = guard.get_mut(&id) {
            match (outcome, verified) {
                (Ok(text), Ok(())) => {
                    node.status = Status::Done;
                    node.result = Some(summarize(&text));
                }
                (Ok(text), Err(reason)) => {
                    node.status = Status::Failed;
                    node.result = Some(summarize(&format!("{reason}\n---\n{text}")));
                }
                (Err(error), _) => {
                    node.status = Status::Failed;
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
    control: &Control,
    progress_tx: &Progress,
) -> Vec<(String, Result<String, String>)> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(max_parallel.max(1)) {
        progress(
            progress_tx,
            &format!(
                "[dispatch] running {} node(s): {}",
                chunk.len(),
                chunk.join(", ")
            ),
        );
        let chunk_results = std::thread::scope(|scope| {
            let handles: Vec<(String, _)> = chunk
                .iter()
                .map(|id| {
                    let node = graph.get(id).cloned().expect("ready node exists");
                    let id = id.clone();
                    let control = control.clone();
                    let handle = scope.spawn(move || {
                        spawn::run(
                            &node.task,
                            node.max_turns,
                            node.timeout_secs,
                            node.provider.as_deref(),
                            node.model.as_deref(),
                            &control,
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
        // A cancelled wave must not start the next chunk.
        if control.is_cancelled() {
            break;
        }
    }
    out
}

/// Check a node's declared acceptance criteria after its child exits.
fn verify_node(expect_file: Option<&str>, verify: Option<&str>) -> Result<(), String> {
    if let Some(file) = expect_file {
        let path = std::path::Path::new(file);
        let resolved = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()
                .map(|dir| dir.join(path))
                .unwrap_or_else(|_| path.to_path_buf())
        };
        if !resolved.exists() {
            return Err(format!("expected file was not produced: {file}"));
        }
    }
    if let Some(command) = verify {
        if !run_verify_command(command)? {
            return Err(format!("verification command failed: {command}"));
        }
    }
    Ok(())
}

fn run_verify_command(command: &str) -> Result<bool, String> {
    let status = if cfg!(windows) {
        std::process::Command::new("cmd")
            .arg("/C")
            .arg(command)
            .stdin(std::process::Stdio::null())
            .status()
    } else {
        std::process::Command::new("sh")
            .arg("-c")
            .arg(command)
            .stdin(std::process::Stdio::null())
            .status()
    }
    .map_err(|err| format!("could not run verification command: {err}"))?;
    Ok(status.success())
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

    #[test]
    fn verify_node_checks_declared_files() {
        assert!(verify_node(None, None).is_ok());
        assert!(verify_node(Some("Cargo.toml"), None).is_ok());
        assert!(verify_node(Some("no-such-file-orchestrator.txt"), None).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn verify_node_runs_declared_commands() {
        assert!(verify_node(None, Some("true")).is_ok());
        assert!(verify_node(None, Some("false")).is_err());
    }
}
