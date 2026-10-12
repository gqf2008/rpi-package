//! Package-local ABI helpers and the per-tool entry boilerplate.
//!
//! Each tool runs its builder on a **worker thread** so `poll` stays
//! non-blocking: the host can then stream progress and, crucially, cancel a call
//! whose worker is waiting on a child process (see [`crate::control`]). This
//! mirrors `rpi-mcp-adapter`'s non-blocking bridge.
#![allow(dead_code)]

use rpi_plugin_sdk::StbString;
use serde_json::Value;
use std::sync::mpsc::Sender;

pub use rpi_plugin_sdk::callbacks::free_string as plugin_free_string;

/// One message from a worker to `poll`.
pub enum Update {
    /// A partial result, shaped like a tool result (`{"content":[...]}`).
    Progress(String),
    /// The final result (or error message).
    Done(Result<String, String>),
}

/// Half of a worker's channel, handed to the builder so it can report progress.
pub type Progress = Sender<Update>;

/// A tool builder: parameters, cancellation token, progress channel → result JSON.
pub type Builder = fn(&Value, &crate::control::Control, &Progress) -> Result<String, String>;

/// Send a progress partial (best-effort; a closed channel is ignored).
pub fn progress(sender: &Progress, text: &str) {
    let _ = sender.send(Update::Progress(content_only(text)));
}

pub fn schema(name: &str, description: &str, parameters: &str) -> rpi_plugin_sdk::StableToolSchema {
    rpi_plugin_sdk::StableToolSchema {
        name: StbString::from_string(name.to_string()),
        description: StbString::from_string(description.to_string()),
        parameters: StbString::from_string(parameters.to_string()),
    }
}

pub fn string_param(params: &Value, name: &str) -> Result<String, String> {
    params
        .get(name)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("missing string parameter `{name}`"))
}

pub fn optional_string(params: &Value, name: &str, default: &str) -> String {
    params
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .to_string()
}

/// Read an optional string param, treating blank/whitespace as absent so an empty
/// value never becomes a bare CLI flag.
pub fn optional_trimmed(params: &Value, name: &str) -> Option<String> {
    params
        .get(name)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

/// A result with only text content (no state to replay).
pub fn content_only(text: &str) -> String {
    serde_json::json!({"content":[{"type":"text","text":text}]}).to_string()
}

/// A result that also carries `details`, which the host persists on the tool
/// result so the extension can reconstruct its state by replaying the branch.
pub fn result_with_details(text: &str, details: Value) -> String {
    serde_json::json!({"content":[{"type":"text","text":text}],"details":details}).to_string()
}

/// Generate a tool's `extern "C"` entry points over a private `ToolHandles`
/// registry, driving the builder on a worker thread.
#[macro_export]
macro_rules! tool_entry {
    ($module:ident, $builder:path) => {
        mod $module {
            use rpi_plugin_sdk::handles::ToolHandles;
            use rpi_plugin_sdk::{
                FreeStringFn, StbString, StbStringRef, StepHandle, StepResult, ToolPartialCb,
            };
            use serde_json::Value;
            use std::ffi::c_void;
            use std::sync::mpsc::{self, Receiver};
            use std::sync::Mutex;
            use std::thread::JoinHandle;

            use $crate::control::Control;
            use $crate::kit::Update;

            struct State {
                receiver: Receiver<Update>,
                worker: Option<JoinHandle<()>>,
                completed: bool,
            }

            struct Drive {
                control: Control,
                state: Mutex<State>,
            }

            static HANDLES: ToolHandles<Drive> = ToolHandles::new();

            pub extern "C" fn execute(
                _: StbStringRef,
                params: StbString,
                free: Option<FreeStringFn>,
            ) -> StepHandle {
                let text = params.to_string_lossy();
                params.free_with(free);
                let control = Control::default();
                let worker_control = control.clone();
                let (sender, receiver) = mpsc::channel();
                let worker = std::thread::spawn(move || {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        worker_control.check()?;
                        let params: Value = serde_json::from_str(&text).unwrap_or(Value::Null);
                        $builder(&params, &worker_control, &sender)
                    }))
                    .unwrap_or_else(|_| Err("tool worker panicked".into()));
                    let _ = sender.send(Update::Done(result));
                });
                HANDLES.insert(Drive {
                    control,
                    state: Mutex::new(State {
                        receiver,
                        worker: Some(worker),
                        completed: false,
                    }),
                })
            }

            pub extern "C" fn poll(
                handle: StepHandle,
                _: Option<ToolPartialCb>,
                _: *mut c_void,
            ) -> StepResult {
                if handle.is_null() {
                    return StepResult::err(StbString::from_string("null package handle".into()));
                }
                let Some(entry) = HANDLES.get(handle) else {
                    return StepResult::err(StbString::from_string(
                        "invalid or destroyed tool handle".into(),
                    ));
                };
                let drive = entry.lock();
                let mut state = match drive.state.lock() {
                    Ok(state) => state,
                    Err(_) => {
                        return StepResult::err(StbString::from_string(
                            "package tool state poisoned".into(),
                        ))
                    }
                };
                if state.completed {
                    return StepResult::err(StbString::from_string(
                        "package tool polled after completion".into(),
                    ));
                }
                let mut latest: Option<String> = None;
                loop {
                    match state.receiver.try_recv() {
                        Ok(Update::Progress(json)) => latest = Some(json),
                        Ok(Update::Done(result)) => {
                            // The worker has only sender destruction left; joining
                            // belongs in `destroy`, not poll (which must not block).
                            state.completed = true;
                            return match result {
                                Ok(json) => StepResult::done(StbString::from_string(json)),
                                Err(message) => StepResult::err(StbString::from_string(message)),
                            };
                        }
                        Err(mpsc::TryRecvError::Empty) => break,
                        Err(mpsc::TryRecvError::Disconnected) => {
                            state.completed = true;
                            return StepResult::err(StbString::from_string(
                                "package tool worker disconnected".into(),
                            ));
                        }
                    }
                }
                match latest {
                    Some(json) => StepResult::pending(StbString::from_string(json)),
                    None => StepResult::pending(StbString::empty()),
                }
            }

            pub extern "C" fn cancel(handle: StepHandle) {
                if let Some(entry) = HANDLES.get(handle) {
                    entry.cancel();
                    entry.lock().control.cancel();
                }
            }

            pub extern "C" fn destroy(handle: StepHandle) {
                if let Some(entry) = HANDLES.remove(handle) {
                    // Take the worker out and drop every lock before joining, so a
                    // blocked worker is never joined while a lock is held.
                    let worker = {
                        let drive = entry.lock();
                        drive.control.cancel();
                        // Bind before the block ends so the guard's temporary drops
                        // while `drive` is still alive.
                        let taken = match drive.state.lock() {
                            Ok(mut state) => state.worker.take(),
                            Err(_) => None,
                        };
                        taken
                    };
                    if let Some(worker) = worker {
                        let _ = worker.join();
                    }
                }
            }
        }
    };
}
