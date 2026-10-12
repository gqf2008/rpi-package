//! Subprocess runner for a single delegation.
//!
//! The runtime already parallelises tool calls (`rpi-agent`'s
//! `execute_tool_calls_parallel`) and feeds every tool result back into the
//! orchestrator's context, so this module only has to launch, bound, and collect
//! one child `rpi`.
//!
//! The task travels on **stdin**, not as a positional argument: `rpi -p`
//! consumes the next non-flag token as its prompt, so a task beginning with `-`
//! would be parsed as a flag. Piped stdin becomes the initial prompt
//! (`rpi-cli::app::build_initial_message`).

use std::io::{Read, Write};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::Duration;

use wait_timeout::ChildExt;

/// Tool this package registers. Excluded from the child's tool set so a
/// subagent cannot recursively delegate (design §8 "递归守卫").
pub const TOOL_NAME: &str = "subagent";

/// Upper bound on the text fed back to the orchestrator (design §8 "输出截断").
pub const MAX_OUTPUT_BYTES: usize = 40_000;

/// Turn budget env the host already enforces (`rpi-harness::run_budget`).
pub const MAX_TURNS_ENV: &str = "RPI_MAX_TURNS_PER_RUN";

/// Build the child command. Separate from `run` so it is unit-testable without
/// spawning anything.
pub fn child_command(exe: &Path, max_turns: u64) -> Command {
    let mut command = Command::new(exe);
    command
        .arg("-p")
        // Recursion guard: the child must not offer `subagent` again.
        .arg("--exclude-tools")
        .arg(TOOL_NAME)
        // Hard turn ceiling enforced by the harness, not the model.
        .env(MAX_TURNS_ENV, max_turns.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command
}

/// Truncate to [`MAX_OUTPUT_BYTES`] on a char boundary, appending a marker.
pub fn truncate_output(mut text: String) -> String {
    const MARKER: &str = "\n[subagent output truncated]";
    if text.len() <= MAX_OUTPUT_BYTES {
        return text;
    }
    let mut end = MAX_OUTPUT_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    text.truncate(end);
    text.push_str(MARKER);
    text
}

/// Run one delegation using the current executable as the child `rpi`.
pub fn run(task: &str, max_turns: u64, timeout_secs: u64) -> Result<String, String> {
    let exe = std::env::current_exe()
        .map_err(|err| format!("cannot resolve the rpi executable: {err}"))?;
    run_with_exe(&exe, task, max_turns, timeout_secs)
}

/// Run one delegation with an explicit executable (test seam).
pub fn run_with_exe(
    exe: &Path,
    task: &str,
    max_turns: u64,
    timeout_secs: u64,
) -> Result<String, String> {
    let mut child = child_command(exe, max_turns)
        .spawn()
        .map_err(|err| format!("failed to start subagent `{}`: {err}", exe.display()))?;

    // Drain stdout/stderr on their own threads so a chatty child cannot block on
    // a full pipe while we wait for it to exit.
    let out_reader = child.stdout.take().map(spawn_reader);
    let err_reader = child.stderr.take().map(spawn_reader);

    write_stdin(&mut child, task);

    match child.wait_timeout(Duration::from_secs(timeout_secs)) {
        Ok(Some(_)) => {}
        Ok(None) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("subagent timed out after {timeout_secs}s"));
        }
        Err(err) => {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!("failed while waiting for subagent: {err}"));
        }
    }

    let status = child
        .wait()
        .map_err(|err| format!("failed to reap subagent: {err}"))?;
    let mut text = join_reader(out_reader);
    let stderr_text = join_reader(err_reader);
    if !status.success() && !stderr_text.trim().is_empty() {
        text.push_str("\n[stderr]\n");
        text.push_str(stderr_text.trim_end());
    }
    let text = truncate_output(text);
    if !status.success() {
        return Err(format!(
            "subagent exited with {}:\n{}",
            status.code().unwrap_or(-1),
            text
        ));
    }
    Ok(text)
}

fn write_stdin(child: &mut Child, task: &str) {
    if let Some(mut stdin) = child.stdin.take() {
        // A child that exits before reading yields EPIPE; not fatal — the exit
        // status is what decides success.
        let _ = stdin.write_all(task.as_bytes());
        let _ = stdin.flush();
        // Dropping `stdin` closes the pipe so the child observes EOF.
    }
}

fn spawn_reader<R: Read + Send + 'static>(mut reader: R) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = reader.read_to_end(&mut buf);
        String::from_utf8_lossy(&buf).into_owned()
    })
}

fn join_reader(handle: Option<std::thread::JoinHandle<String>>) -> String {
    handle
        .map(|handle| handle.join().unwrap_or_default())
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn child_command_sets_recursion_guard_and_turn_budget() {
        let command = child_command(Path::new("/usr/bin/rpi"), 7);
        let args: Vec<String> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args, vec!["-p", "--exclude-tools", "subagent"]);

        let envs: Vec<(String, String)> = command
            .get_envs()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value
                        .map(|value| value.to_string_lossy().into_owned())
                        .unwrap_or_default(),
                )
            })
            .collect();
        assert_eq!(envs, vec![(MAX_TURNS_ENV.to_string(), "7".to_string())]);
    }

    #[test]
    fn truncate_output_is_bounded_and_char_safe() {
        let short = "ok".to_string();
        assert_eq!(truncate_output(short.clone()), short);

        // Multi-byte chars: the cut must land on a boundary, not split a char.
        let long = "あ".repeat(MAX_OUTPUT_BYTES);
        let out = truncate_output(long);
        assert!(out.starts_with('あ'));
        assert!(out.ends_with("[subagent output truncated]"));
        assert!(out.len() <= MAX_OUTPUT_BYTES + "\n[subagent output truncated]".len());
    }
}
