//! Package-local ABI helpers and the per-tool entry boilerplate.
//!
//! Every tool needs the same `execute` / `poll` / `cancel` / `destroy` quartet
//! plus its own `ToolHandles` registry, so this crate keeps that plumbing in one
//! macro ([`tool_entry!`]) and one module instead of vendoring it per tool.
#![allow(dead_code)]

use rpi_plugin_sdk::StbString;
use serde_json::Value;

pub use rpi_plugin_sdk::callbacks::free_string as plugin_free_string;

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
/// registry. The builder returns the **full** result JSON (content + details).
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
            use std::sync::atomic::Ordering;

            pub struct Drive {
                pub params: Value,
                pub builder: fn(&Value) -> Result<String, String>,
                pub completed: bool,
            }

            static HANDLES: ToolHandles<Drive> = ToolHandles::new();

            pub extern "C" fn execute(
                _: StbStringRef,
                params: StbString,
                free: Option<FreeStringFn>,
            ) -> StepHandle {
                let text = params.to_string_lossy();
                params.free_with(free);
                HANDLES.insert(Drive {
                    params: serde_json::from_str(&text).unwrap_or(Value::Null),
                    builder: $builder,
                    completed: false,
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
                let mut drive = entry.lock();
                if entry.cancelled.load(Ordering::SeqCst) {
                    return StepResult::err(StbString::from_string(
                        "package tool cancelled".into(),
                    ));
                }
                if drive.completed {
                    return StepResult::err(StbString::from_string(
                        "package tool polled after completion".into(),
                    ));
                }
                drive.completed = true;
                match (drive.builder)(&drive.params) {
                    Ok(text) => StepResult::done(StbString::from_string(text)),
                    Err(message) => StepResult::err(StbString::from_string(message)),
                }
            }

            pub extern "C" fn cancel(handle: StepHandle) {
                HANDLES.cancel(handle);
            }

            pub extern "C" fn destroy(handle: StepHandle) {
                drop(HANDLES.remove(handle));
            }
        }
    };
}
