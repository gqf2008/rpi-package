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
use std::time::{Duration, Instant};

use wait_timeout::ChildExt;

use crate::control::Control;

/// Tools this package registers. Excluded from the child's tool set so a
/// subagent cannot recursively orchestrate (design §8 "递归守卫").
pub const EXCLUDED_TOOLS: &str = "subagent,plan,dispatch";

/// Upper bound on the text fed back to the orchestrator (design §8 "输出截断").
pub const MAX_OUTPUT_BYTES: usize = 40_000;

/// Turn budget env the host already enforces (`rpi-harness::run_budget`).
pub const MAX_TURNS_ENV: &str = "RPI_MAX_TURNS_PER_RUN";

/// Marker set on the child so the orchestrator guidance is not injected into a
/// subagent (which does not have `plan`/`dispatch`).
pub const SUBAGENT_ENV: &str = "RPI_ORCHESTRATOR_SUBAGENT";

/// How often the wait loop re-checks cancellation/timeout. Bounds how long a
/// cancelled call can keep a child alive.
const POLL_SLICE: Duration = Duration::from_millis(100);

/// Build the child command. Separate from `run` so it is unit-testable without
/// spawning anything.
///
/// `provider` / `model` are passed through as `--provider` / `--model`. The child
/// shares the orchestrator's agent dir, so without them it silently falls back to
/// the *default* provider — which is wrong (or broken) whenever the orchestrator
/// itself was started with an explicit `--provider` / `--model`.
pub fn child_command(
    exe: &Path,
    max_turns: u64,
    provider: Option<&str>,
    model: Option<&str>,
) -> Command {
    let mut command = Command::new(exe);
    command
        .arg("-p")
        // Recursion guard: the child must not offer `subagent`/`plan`/`dispatch`.
        .arg("--exclude-tools")
        .arg(EXCLUDED_TOOLS);
    if let Some(provider) = provider.filter(|value| !value.is_empty()) {
        command.arg("--provider").arg(provider);
    }
    if let Some(model) = model.filter(|value| !value.is_empty()) {
        command.arg("--model").arg(model);
    }
    command
        // Hard turn ceiling enforced by the harness, not the model.
        .env(MAX_TURNS_ENV, max_turns.to_string())
        .env(SUBAGENT_ENV, "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // Own process group, so cancelling can kill the child *and its descendants*
    // instead of orphaning whatever it spawned.
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        command.process_group(0);
    }
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
#[allow(clippy::too_many_arguments)]
pub fn run(
    task: &str,
    max_turns: u64,
    timeout_secs: u64,
    provider: Option<&str>,
    model: Option<&str>,
    control: &Control,
) -> Result<String, String> {
    let exe = std::env::current_exe()
        .map_err(|err| format!("cannot resolve the rpi executable: {err}"))?;
    run_with_exe(
        &exe,
        task,
        max_turns,
        timeout_secs,
        provider,
        model,
        control,
    )
}

/// Run one delegation with an explicit executable (test seam).
#[allow(clippy::too_many_arguments)]
pub fn run_with_exe(
    exe: &Path,
    task: &str,
    max_turns: u64,
    timeout_secs: u64,
    provider: Option<&str>,
    model: Option<&str>,
    control: &Control,
) -> Result<String, String> {
    // Fail before launching anything if already cancelled.
    control.check()?;
    let mut child = child_command(exe, max_turns, provider, model)
        .spawn()
        .map_err(|err| format!("failed to start subagent `{}`: {err}", exe.display()))?;

    // Drain stdout/stderr on their own threads so a chatty child cannot block on
    // a full pipe while we wait for it to exit.
    let out_reader = child.stdout.take().map(spawn_reader);
    let err_reader = child.stderr.take().map(spawn_reader);

    write_stdin(&mut child, task);

    // Wait in slices so cancellation and the wall-clock deadline are both observed
    // while the child is still running.
    let deadline = Instant::now() + Duration::from_secs(timeout_secs);
    loop {
        if control.is_cancelled() {
            kill_and_reap(&mut child);
            return Err("subagent cancelled".into());
        }
        if Instant::now() >= deadline {
            kill_and_reap(&mut child);
            return Err(format!("subagent timed out after {timeout_secs}s"));
        }
        let slice = deadline
            .saturating_duration_since(Instant::now())
            .min(POLL_SLICE);
        match child.wait_timeout(slice) {
            Ok(Some(_)) => break,
            Ok(None) => continue,
            Err(err) => {
                kill_and_reap(&mut child);
                return Err(format!("failed while waiting for subagent: {err}"));
            }
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

fn kill_and_reap(child: &mut Child) {
    #[cfg(unix)]
    {
        // Descendants can re-group: rpi's own `bash` tool puts each shell in a new
        // process group, so a group kill from here would miss them. Walk the tree
        // first (while it is still attached), then kill the group and the child.
        kill_descendants(child.id());
        use nix::sys::signal::{kill, Signal};
        use nix::unistd::Pid;
        let _ = kill(Pid::from_raw(-(child.id() as i32)), Signal::SIGKILL);
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// Best-effort recursive SIGKILL of `pid` and everything below it (unix).
#[cfg(unix)]
fn kill_descendants(pid: u32) {
    if let Ok(output) = std::process::Command::new("pgrep")
        .arg("-P")
        .arg(pid.to_string())
        .output()
    {
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            if let Ok(child) = line.trim().parse::<u32>() {
                kill_descendants(child);
            }
        }
    }
    let _ = std::process::Command::new("kill")
        .arg("-KILL")
        .arg(pid.to_string())
        .status();
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
        let command = child_command(Path::new("/usr/bin/rpi"), 7, None, None);
        let args: Vec<String> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(args, vec!["-p", "--exclude-tools", EXCLUDED_TOOLS]);

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
        assert_eq!(
            envs,
            vec![
                (MAX_TURNS_ENV.to_string(), "7".to_string()),
                (SUBAGENT_ENV.to_string(), "1".to_string()),
            ]
        );
    }

    #[test]
    fn child_command_passes_provider_and_model_through() {
        let command = child_command(
            Path::new("/usr/bin/rpi"),
            8,
            Some("deepseek"),
            Some("deepseek-flash"),
        );
        let args: Vec<String> = command
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec![
                "-p",
                "--exclude-tools",
                EXCLUDED_TOOLS,
                "--provider",
                "deepseek",
                "--model",
                "deepseek-flash",
            ]
        );

        // Empty strings must be dropped rather than emitted as bare flags.
        let bare = child_command(Path::new("/usr/bin/rpi"), 8, Some(""), None);
        let bare_args: Vec<String> = bare
            .get_args()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect();
        assert_eq!(bare_args, vec!["-p", "--exclude-tools", EXCLUDED_TOOLS]);
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

    #[cfg(unix)]
    #[test]
    fn cancellation_kills_the_whole_process_group() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        // The child backgrounds a `sleep` (a grandchild) and records its pid, then
        // waits. Cancelling must kill the group, not just the direct child.
        let dir = std::env::temp_dir();
        let script = dir.join(format!("orch-group-{}.sh", std::process::id()));
        let pidfile = dir.join(format!("orch-group-{}.pid", std::process::id()));
        {
            let mut file = std::fs::File::create(&script).unwrap();
            writeln!(file, "#!/bin/sh").unwrap();
            writeln!(file, "sleep 30 &").unwrap();
            writeln!(file, "echo $! > {}", pidfile.display()).unwrap();
            writeln!(file, "wait").unwrap();
        }
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).unwrap();

        let control = Control::default();
        let canceller = control.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(800));
            canceller.cancel();
        });
        let _ = run_with_exe(&script, "ignored", 8, 30, None, None, &control);
        let _ = handle.join();

        let grandchild = std::fs::read_to_string(&pidfile)
            .ok()
            .and_then(|text| text.trim().parse::<i32>().ok());
        std::thread::sleep(Duration::from_millis(300));
        if let Some(pid) = grandchild {
            let alive = std::process::Command::new("kill")
                .arg("-0")
                .arg(pid.to_string())
                .status()
                .map(|status| status.success())
                .unwrap_or(false);
            assert!(!alive, "grandchild {pid} survived the process-group kill");
        }
        let _ = std::fs::remove_file(&script);
        let _ = std::fs::remove_file(&pidfile);
    }

    #[test]
    fn cancelled_control_aborts_the_call() {
        // A pre-cancelled control aborts before any child is launched, so this
        // holds even where the fake executable path does not exist.
        let control = Control::default();
        control.cancel();
        let err = run_with_exe(
            Path::new("/nonexistent/orchestrator-child"),
            "ignored",
            8,
            5,
            None,
            None,
            &control,
        )
        .unwrap_err();
        assert!(err.contains("cancelled"), "got: {err}");
    }

    #[cfg(unix)]
    #[test]
    fn cancellation_kills_a_running_child() {
        use std::io::Write;
        use std::os::unix::fs::PermissionsExt;

        // A stand-in child that ignores our args and blocks, so the wait loop has
        // something to kill. `exec` makes the script process become the sleeper,
        // so killing the direct child leaves no orphan.
        let script = std::env::temp_dir().join(format!("orch-cancel-{}.sh", std::process::id()));
        {
            let mut file = std::fs::File::create(&script).unwrap();
            writeln!(file, "#!/bin/sh").unwrap();
            writeln!(file, "exec sleep 30").unwrap();
        }
        let mut perms = std::fs::metadata(&script).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&script, perms).unwrap();

        let control = Control::default();
        let canceller = control.clone();
        let handle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(300));
            canceller.cancel();
        });
        let start = Instant::now();
        let err = run_with_exe(&script, "ignored", 8, 30, None, None, &control).unwrap_err();
        let elapsed = start.elapsed();
        let _ = handle.join();
        let _ = std::fs::remove_file(&script);

        assert!(err.contains("cancelled"), "got: {err}");
        assert!(
            elapsed < Duration::from_secs(10),
            "cancel took too long: {elapsed:?}"
        );
    }
}
