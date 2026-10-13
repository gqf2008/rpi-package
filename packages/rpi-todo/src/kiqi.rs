//! The compile-time (`kiqi-plugin`) face of rpi-todo.
//!
//! This is the same plugin the cdylib ABI loads, expressed as a Rust trait:
//! no FFI, no `StepHandle`, no `PluginApi`. Everything below delegates to the
//! helpers in the crate root, so both faces share one implementation of the
//! task-list semantics.

use crate::{parse_todos, state, state_json, todo, TOOL};
use kiqi_plugin::{
    Block, CommandSpec, Event, EventAction, HostCaps, Plugin, PluginError, ToolOutcome, ToolSpec,
};
use serde_json::Value;

const DESCRIPTION: &str = "Manage one session task list. Actions: list, add (text), update (id, status: pending/in_progress/completed), toggle (id), clear. Mark the task you are working on in_progress and completed when finished.";
const SCHEMA: &str = r#"{"type":"object","properties":{"action":{"type":"string","enum":["list","add","update","toggle","clear"]},"text":{"type":"string"},"id":{"type":"integer"},"status":{"type":"string","enum":["pending","in_progress","completed"]},"includeDone":{"type":"boolean"}},"required":["action"]}"#;
const COMMAND: &str = "todos";

#[derive(Default)]
pub struct TodoPlugin;

impl TodoPlugin {
    pub fn new() -> Self {
        Self
    }

    /// Rebuild the list from the session branch, the same walk the native
    /// `restore_from_branch` does - but the branch now arrives through
    /// [`HostCaps`] instead of a `RuntimeActionId::GetSessionBranch` FFI call.
    fn restore(&self, caps: &HostCaps<'_>) {
        let Some(session) = caps.session else {
            return;
        };
        let mut restored = Default::default();
        if let Some(entries) = session.session_branch().get("entries").and_then(Value::as_array) {
            for entry in entries {
                let Some(message) = entry.get("message") else {
                    continue;
                };
                if message.get("role").and_then(Value::as_str) != Some("toolResult")
                    || message.get("toolName").and_then(Value::as_str) != Some(TOOL)
                {
                    continue;
                }
                if let Some(next) = parse_todos(message.get("details").unwrap_or(&Value::Null)) {
                    restored = next;
                }
            }
        }
        *state().lock().unwrap_or_else(|p| p.into_inner()) = restored;
    }
}

impl Plugin for TodoPlugin {
    fn id(&self) -> &str {
        "rpi-todo"
    }

    fn tools(&self) -> Vec<ToolSpec> {
        vec![ToolSpec {
            name: TOOL.to_string(),
            description: DESCRIPTION.to_string(),
            parameters: serde_json::from_str(SCHEMA).expect("todo schema is valid JSON"),
        }]
    }

    fn commands(&self) -> Vec<CommandSpec> {
        vec![CommandSpec {
            name: COMMAND.to_string(),
            description: "Show todos on the current session branch".to_string(),
        }]
    }

    fn call(
        &self,
        name: &str,
        arguments: &Value,
        _caps: &HostCaps<'_>,
        _sink: &dyn kiqi_plugin::ProgressSink,
    ) -> Result<ToolOutcome, PluginError> {
        if name != TOOL {
            return Err(PluginError::UnknownTool(name.to_string()));
        }
        // `todo` already returns the host-shaped result; reuse it verbatim
        // rather than re-deriving the details payload.
        let raw = todo(arguments).map_err(PluginError::InvalidArguments)?;
        let parsed: Value =
            serde_json::from_str(&raw).map_err(|e| PluginError::Failed(e.to_string()))?;
        let text = parsed
            .get("content")
            .and_then(|c| c.get(0))
            .and_then(|c| c.get("text"))
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        let details = parsed.get("details").cloned().unwrap_or(Value::Null);
        let is_error = details.get("error").is_some();
        Ok(ToolOutcome {
            blocks: vec![Block::Text(text)],
            is_error,
            details,
        })
    }

    fn on_event(&self, event: &Event, caps: &HostCaps<'_>) -> EventAction {
        match event {
            Event::SessionStart | Event::SessionTree => self.restore(caps),
            Event::ToolResult { tool, details, .. } if tool == TOOL => {
                if let Some(next) = parse_todos(details) {
                    *state().lock().unwrap_or_else(|p| p.into_inner()) = next;
                }
            }
            _ => {}
        }
        EventAction::Continue
    }

    fn run_command(
        &self,
        name: &str,
        _arguments: &str,
        _caps: &HostCaps<'_>,
    ) -> Result<String, PluginError> {
        if name != COMMAND {
            return Err(PluginError::UnknownTool(name.to_string()));
        }
        let _ = state_json(&state().lock().unwrap_or_else(|p| p.into_inner()));
        Ok(crate::render(&state().lock().unwrap_or_else(|p| p.into_inner()), true))
    }
}

/// The factory the host registry calls.
pub fn factory() -> Box<dyn Plugin> {
    Box::new(TodoPlugin::new())
}
