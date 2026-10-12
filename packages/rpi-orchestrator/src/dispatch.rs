//! The `dispatch` tool: schedule the DAG.
//!
//! Scheduling is deterministic code: the ready set is "pending nodes whose
//! dependencies are all done", started within a global concurrency budget. Nodes
//! run on **detached** workers, so a call can return before they finish — the
//! orchestrator can reshape the graph (`plan` `addNodes` / `cancel`) while a wave
//! is in flight, then call `dispatch` again to start newly-ready nodes and
//! collect finished ones.
//!
//! `waitSeconds` bounds how long a call blocks:
//! - absent  → wait until the running set drains (one call per wave, classic);
//! - `0`     → start and return immediately (the dynamic schedule);
//! - `N`     → start, then wait up to N seconds.

use std::time::{Duration, Instant};

use crate::control::Control;
use crate::graph::{Graph, Node, Status};
use crate::kit::{progress, result_with_details, Progress};
use crate::spawn;
use crate::{graph_state, running};
use serde_json::Value;

const DEFAULT_MAX_PARALLEL: usize = 4;
const MAX_PARALLEL_CAP: usize = 16;
const RESULT_SUMMARY_BYTES: usize = 2_000;
const WAIT_POLL: Duration = Duration::from_millis(100);

enum Wait {
    /// Return as soon as the ready set is started.
    None,
    /// Block up to this long.
    Fixed(Duration),
    /// Block until the running set drains.
    Drain,
}

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
    let wait = match params.get("waitSeconds").and_then(Value::as_u64) {
        Some(0) => Wait::None,
        Some(secs) => Wait::Fixed(Duration::from_secs(secs)),
        None => Wait::Drain,
    };

    // ---- Start the ready set within the global concurrency budget. ----
    let started = {
        let mut graph = graph_state().lock().unwrap_or_else(|p| p.into_inner());
        if graph.nodes.is_empty() {
            return Err("no plan yet: call plan(action=set, nodes=[...]) first".into());
        }
        let running_now = running().lock().unwrap_or_else(|p| p.into_inner()).len();
        let budget = max_parallel.saturating_sub(running_now);

        let mut ready = graph.ready();
        if ready.is_empty() && running_now == 0 && graph.has_pending() {
            let blocked = graph.pending_ids().join(", ");
            return Err(format!(
                "no progress: no pending node is ready (blocked by unmet dependencies): {blocked}"
            ));
        }
        ready.truncate(budget);

        let mut started = Vec::with_capacity(ready.len());
        for id in ready {
            let Some(node) = graph.get(&id).cloned() else {
                continue;
            };
            if let Some(slot) = graph.get_mut(&id) {
                slot.status = Status::Running;
            }
            let node_control = crate::node_control();
            running()
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .insert(id.clone(), node_control.clone());
            spawn_node_worker(id.clone(), node, node_control, progress_tx.clone());
            started.push(id);
        }
        started
    };

    // ---- Wait, if asked. ----
    match wait {
        Wait::None => {}
        Wait::Fixed(max) => wait_for_drain(Some(max)),
        Wait::Drain => wait_for_drain(None),
    }

    // ---- Collect. ----
    let mut graph = graph_state().lock().unwrap_or_else(|p| p.into_inner());
    let finished = take_finished(&mut graph);
    let running_ids: Vec<String> = {
        let mut ids: Vec<String> = running()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .keys()
            .cloned()
            .collect();
        ids.sort();
        ids
    };

    let mut text = format!(
        "started {} · running {} · finished now {}\n",
        started.len(),
        running_ids.len(),
        finished.len()
    );
    if !started.is_empty() {
        text.push_str(&format!("started: {}\n", started.join(", ")));
    }
    if !running_ids.is_empty() {
        text.push_str(&format!("running: {}\n", running_ids.join(", ")));
    }
    for node in &finished {
        text.push_str(&format!(
            "\n--- {} ({})\n{}\n",
            node.id,
            node.status.as_str(),
            node.result.as_deref().unwrap_or("(no output)")
        ));
    }
    text.push('\n');
    text.push_str(&graph.render());
    Ok(result_with_details(&text, graph.to_details()))
}

/// Spawn a detached worker for one node. It runs the child, applies acceptance,
/// commits the node's terminal state, and unregisters itself — guaranteed, even
/// if the child logic panics, so a `dispatch` waiting for the running set to
/// drain cannot hang forever.
fn spawn_node_worker(id: String, node: Node, control: Control, progress_tx: Progress) {
    std::thread::spawn(move || {
        progress(&progress_tx, &format!("[dispatch] {id} running"));
        let cleanup_id = id.clone();
        let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let outcome = spawn::run(
                &node.task,
                node.max_turns,
                node.timeout_secs,
                node.provider.as_deref(),
                node.model.as_deref(),
                &control,
            );
            // Acceptance is only checked when the child reported success: a child
            // that hit its turn cap still exits 0, so without this a no-op would
            // be `done`.
            let verified = match &outcome {
                Ok(_) => verify_node(node.expect_file.as_deref(), node.verify.as_deref()),
                Err(_) => Ok(()),
            };
            let mut graph = graph_state().lock().unwrap_or_else(|p| p.into_inner());
            if let Some(slot) = graph.get_mut(&id) {
                match (outcome, verified) {
                    (Ok(text), Ok(())) => {
                        slot.status = Status::Done;
                        slot.result = Some(summarize(&text));
                    }
                    (Ok(text), Err(reason)) => {
                        slot.status = Status::Failed;
                        slot.result = Some(summarize(&format!("{reason}\n---\n{text}")));
                    }
                    (Err(error), _) => {
                        slot.status = Status::Failed;
                        slot.result = Some(summarize(&error));
                    }
                }
            }
            graph.propagate_skips();
        }));
        if ran.is_err() {
            // A panicked worker must still leave the node terminal, or it is stuck
            // `running` and any draining wait never returns.
            let mut graph = graph_state().lock().unwrap_or_else(|p| p.into_inner());
            if let Some(slot) = graph.get_mut(&cleanup_id) {
                if slot.status == Status::Running {
                    slot.status = Status::Failed;
                    slot.result = Some("node worker panicked".into());
                }
            }
            graph.propagate_skips();
        }
        running()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&cleanup_id);
        progress(&progress_tx, &format!("[dispatch] {cleanup_id} finished"));
    });
}

fn wait_for_drain(max: Option<Duration>) {
    let start = Instant::now();
    loop {
        if running()
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_empty()
        {
            return;
        }
        if max.is_some_and(|max| start.elapsed() >= max) {
            return;
        }
        std::thread::sleep(WAIT_POLL);
    }
}

/// Terminal nodes whose result has not been returned yet, marking them reported.
fn take_finished(graph: &mut Graph) -> Vec<Node> {
    let mut out = Vec::new();
    for node in &mut graph.nodes {
        if node.status.is_terminal() && !node.reported {
            node.reported = true;
            out.push(node.clone());
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

    #[test]
    fn wait_returns_immediately_when_nothing_runs() {
        // Must not block when the running set is empty.
        let start = Instant::now();
        wait_for_drain(None);
        assert!(start.elapsed() < Duration::from_secs(1));
    }

    #[test]
    fn a_result_is_reported_once_until_the_node_is_reset() {
        // Guards the cursor: a fresh `plan set` / `reset` must be able to report a
        // node again, so a reused node id cannot hide its result.
        let mut graph =
            Graph::from_nodes_value(&serde_json::json!([{"id": "a", "task": "x"}])).unwrap();
        {
            let node = graph.get_mut("a").unwrap();
            node.status = Status::Done;
            node.result = Some("ok".into());
        }
        assert_eq!(take_finished(&mut graph).len(), 1);
        assert_eq!(take_finished(&mut graph).len(), 0, "reported once");

        graph.get_mut("a").unwrap().reported = false;
        assert_eq!(take_finished(&mut graph).len(), 1, "reset re-reports");
    }
}
