//! Persistent connections scoped to the host session and configuration identity.
use crate::connection::{
    initialize_request, initialized_notification, parse_initialize, response_result,
    tools_call_request,
};
use crate::control::{Control, Deadline};
use crate::http::HttpTransport;
use crate::{McpConfig, McpServerConfig, Request, Response, ServerInfo, StdioTransport, Tool};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, SyncSender};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, TryLockError};

#[derive(Debug, Clone, PartialEq)]
pub struct DiscoveredServer {
    pub name: String,
    pub info: ServerInfo,
    pub tools: Vec<Tool>,
}

#[derive(Debug)]
enum Transport {
    Stdio(StdioTransport),
    Http(HttpTransport),
}
impl Transport {
    fn request(&mut self, request: &Request, control: &Control) -> Result<Response, String> {
        match self {
            Self::Stdio(t) => t.request_control(request, control),
            Self::Http(t) => t.request(request, control),
        }
    }
    fn notify(&mut self, control: &Control) -> Result<(), String> {
        match self {
            Self::Stdio(t) => t.notify_control(&initialized_notification(), control),
            Self::Http(t) => t.notify(&initialized_notification(), control),
        }
    }
    fn timeout(&mut self, timeout: u64) {
        match self {
            Self::Stdio(t) => t.set_timeout(timeout),
            Self::Http(t) => t.set_timeout(timeout),
        }
    }
}
#[derive(Debug)]
struct Session {
    config: McpServerConfig,
    transport: Transport,
    info: ServerInfo,
    next_id: u64,
    failed: bool,
}
impl Session {
    fn start(
        config: &McpServerConfig,
        timeout: u64,
        control: &Control,
        notifications: &SyncSender<Value>,
    ) -> Result<Self, String> {
        control.check()?;
        let mut transport = match config {
            McpServerConfig::Stdio(s) => Transport::Stdio(StdioTransport::start(
                &s.command,
                &s.args,
                &s.env,
                s.cwd.as_deref(),
                Some(notifications.clone()),
            )?),
            McpServerConfig::Http(s) => Transport::Http(HttpTransport::new(s)?),
        };
        transport.timeout(timeout);
        let info = parse_initialize(transport.request(
            &initialize_request(1, "rpi-mcp-adapter", env!("CARGO_PKG_VERSION")),
            control,
        )?)?;
        if let Transport::Http(http) = &mut transport {
            http.version = Some(info.protocol_version.clone());
        }
        transport.notify(control)?;
        Ok(Self {
            config: config.clone(),
            transport,
            info,
            next_id: 2,
            failed: false,
        })
    }
    fn request(
        &mut self,
        method: &str,
        params: Value,
        control: &Control,
    ) -> Result<Response, String> {
        let id = self.next_id;
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or("MCP request ID exhausted")?;
        let response = self
            .transport
            .request(&Request::new(id, method, Some(params)), control);
        if response.is_err() {
            self.failed = true;
        }
        response
    }
    fn tools(&mut self, control: &Control) -> Result<Vec<Tool>, String> {
        let mut tools = Vec::new();
        let mut cursor = None;
        let mut seen = HashSet::new();
        for _ in 0..100 {
            let params = cursor
                .as_ref()
                .map(|v| json!({"cursor":v}))
                .unwrap_or_else(|| json!({}));
            let result = response_result(self.request("tools/list", params, control)?)?;
            tools.extend(crate::tool_list(&result)?);
            if tools.len() > 16_384 {
                return Err("MCP tool list exceeded 16384 tools".into());
            }
            match result.get("nextCursor") {
                None => return Ok(tools),
                Some(Value::String(next)) => {
                    if !seen.insert(next.clone()) {
                        return Err("MCP tools/list returned a repeated pagination cursor".into());
                    }
                    cursor = Some(next.clone());
                }
                _ => return Err("MCP tools/list nextCursor must be a string".into()),
            }
        }
        Err("MCP tools/list exceeded 100 pages".into())
    }
}
struct Entry {
    session: Mutex<Option<Session>>,
    stop: Control,
    /// Notifications from this connection's reader, consumed by its dispatcher.
    /// Owned by the entry so a re-opened session reuses the same dispatcher.
    notifications: SyncSender<Value>,
}
type Sessions = BTreeMap<String, Arc<Entry>>;
static SESSIONS: OnceLock<Mutex<Sessions>> = OnceLock::new();
fn sessions() -> &'static Mutex<Sessions> {
    SESSIONS.get_or_init(Default::default)
}

// ---------------------------------------------------------------------------
// Resource subscriptions and pushed updates
// ---------------------------------------------------------------------------

/// Subscribed resource URIs, per connection key.
static SUBSCRIPTIONS: OnceLock<Mutex<std::collections::HashMap<String, HashSet<String>>>> =
    OnceLock::new();
fn subscriptions() -> &'static Mutex<std::collections::HashMap<String, HashSet<String>>> {
    SUBSCRIPTIONS.get_or_init(Default::default)
}

/// A resource read in response to `notifications/resources/updated`, waiting to
/// be shown to the model. Drained at the turn boundary (`TurnEnd`) so injection
/// never splits an assistant `tool_calls` from its `toolResult`s.
#[derive(Debug, Clone)]
pub(crate) struct ResourceUpdate {
    pub key: String,
    pub uri: String,
    pub text: String,
}
static PENDING: OnceLock<Mutex<Vec<ResourceUpdate>>> = OnceLock::new();
fn pending() -> &'static Mutex<Vec<ResourceUpdate>> {
    PENDING.get_or_init(Default::default)
}
const MAX_PENDING: usize = 64;
const MAX_RESOURCE_TEXT: usize = 20_000;

pub(crate) fn take_pending_updates() -> Vec<ResourceUpdate> {
    std::mem::take(&mut *pending().lock().unwrap_or_else(|p| p.into_inner()))
}

fn add_subscription(key: &str, uri: &str) {
    if let Ok(mut map) = subscriptions().lock() {
        map.entry(key.to_string())
            .or_default()
            .insert(uri.to_string());
    }
}
fn remove_subscription(key: &str, uri: &str) {
    if let Ok(mut map) = subscriptions().lock() {
        if let Some(set) = map.get_mut(key) {
            set.remove(uri);
            if set.is_empty() {
                map.remove(key);
            }
        }
    }
}
fn clear_subscriptions(key: &str) {
    if let Ok(mut map) = subscriptions().lock() {
        map.remove(key);
    }
}
fn is_subscribed(key: &str, uri: &str) -> bool {
    subscriptions()
        .lock()
        .map(|map| map.get(key).is_some_and(|set| set.contains(uri)))
        .unwrap_or(false)
}

/// One dispatcher per connection. It ends when every sender is gone (entry
/// removed and session dropped).
fn spawn_dispatcher(key: String, receiver: Receiver<Value>) {
    let _ = std::thread::Builder::new()
        .name("mcp-notify".into())
        .spawn(move || {
            while let Ok(value) = receiver.recv() {
                dispatch_notification(&key, &value);
            }
        });
}

fn dispatch_notification(key: &str, value: &Value) {
    let method = value
        .get("method")
        .and_then(Value::as_str)
        .unwrap_or_default();
    if method != "notifications/resources/updated" {
        return;
    }
    let Some(uri) = value
        .get("params")
        .and_then(|params| params.get("uri"))
        .and_then(Value::as_str)
        .map(str::to_string)
    else {
        return;
    };
    if !is_subscribed(key, &uri) {
        return;
    }
    if let Ok(text) = read_resource_by_key(key, &uri) {
        let mut pending = pending().lock().unwrap_or_else(|p| p.into_inner());
        if pending.len() < MAX_PENDING {
            pending.push(ResourceUpdate {
                key: key.to_string(),
                uri,
                text,
            });
        }
    }
}

/// Read a resource on an existing connection (used by the dispatcher). Never
/// creates a session: if the connection is gone, the update is dropped. Runs on
/// the dispatcher thread, not the reader, so it can await the reply.
fn read_resource_by_key(key: &str, uri: &str) -> Result<String, String> {
    let entry = sessions()
        .lock()
        .map_err(|_| "MCP session registry poisoned")?
        .get(key)
        .cloned()
        .ok_or("MCP connection is gone")?;
    let control = Control::default();
    let deadline = Deadline::new(120, "resources/read", &control);
    let mut state = lock_until(&entry.session, &deadline)?;
    let session = state.as_mut().ok_or("MCP connection is closed")?;
    let result =
        response_result(session.request("resources/read", json!({"uri": uri}), &control)?)?;
    Ok(resource_text(&result))
}

/// Flatten a `resources/read` result's `contents` into text (blobs are noted by
/// size, not inlined).
fn resource_text(result: &Value) -> String {
    let Some(contents) = result.get("contents").and_then(Value::as_array) else {
        return result.to_string();
    };
    let mut out = String::new();
    for item in contents {
        let uri = item.get("uri").and_then(Value::as_str).unwrap_or("");
        let body = if let Some(text) = item.get("text").and_then(Value::as_str) {
            text.to_string()
        } else if let Some(blob) = item.get("blob").and_then(Value::as_str) {
            format!("[blob, {} base64 chars]", blob.len())
        } else {
            continue;
        };
        if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&format!("{uri}\n{body}"));
    }
    if out.len() > MAX_RESOURCE_TEXT {
        let mut end = MAX_RESOURCE_TEXT;
        while end > 0 && !out.is_char_boundary(end) {
            end -= 1;
        }
        out.truncate(end);
        out.push_str("\n[truncated]");
    }
    out
}

/// `resources/list` | `read` | `subscribe` | `unsubscribe`.
pub(crate) fn resources(params: &Value, control: &Control) -> Result<Value, String> {
    control.check()?;
    let action = params
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("list");
    if !matches!(action, "list" | "read" | "subscribe" | "unsubscribe") {
        return Err("mcp_resource action must be list, read, subscribe or unsubscribe".into());
    }
    let (key, name, config, timeout) = target(params)?;

    if action == "list" {
        return with_session(key, config, timeout, control, |session, control| {
            let mut resources = Vec::new();
            let mut cursor = None;
            let mut seen = HashSet::new();
            for _ in 0..100 {
                let params = cursor
                    .as_ref()
                    .map(|c| json!({"cursor": c}))
                    .unwrap_or_else(|| json!({}));
                let result =
                    response_result(session.request("resources/list", params, control)?)?;
                if let Some(items) = result.get("resources").and_then(Value::as_array) {
                    resources.extend(items.iter().cloned());
                }
                if resources.len() > 16_384 {
                    return Err("MCP resource list exceeded 16384 resources".into());
                }
                match result.get("nextCursor") {
                    None => break,
                    Some(Value::String(next)) => {
                        if !seen.insert(next.clone()) {
                            return Err(
                                "MCP resources/list returned a repeated pagination cursor".into()
                            );
                        }
                        cursor = Some(next.clone());
                    }
                    _ => return Err("MCP resources/list nextCursor must be a string".into()),
                }
            }
            Ok(json!({"server": name, "resources": resources}))
        });
    }

    let uri = crate::kit::string_param(params, "uri")?;
    if action == "unsubscribe" {
        let _ = with_session(key.clone(), config, timeout, control, |session, control| {
            response_result(session.request(
                "resources/unsubscribe",
                json!({"uri": uri}),
                control,
            )?)
        });
        remove_subscription(&key, &uri);
        return Ok(json!({"server": name, "uri": uri, "unsubscribed": true}));
    }
    if action == "subscribe" {
        with_session(key.clone(), config, timeout, control, |session, control| {
            response_result(session.request("resources/subscribe", json!({"uri": uri}), control)?)
        })?;
        add_subscription(&key, &uri);
        return Ok(json!({"server": name, "uri": uri, "subscribed": true}));
    }
    // read
    with_session(key, config, timeout, control, |session, control| {
        let result =
            response_result(session.request("resources/read", json!({"uri": uri}), control)?)?;
        Ok(json!({
            "server": name,
            "uri": uri,
            "contents": result.get("contents").cloned().unwrap_or(Value::Null),
        }))
    })
}

fn lock_until<'a, T>(
    mutex: &'a Mutex<T>,
    deadline: &Deadline,
) -> Result<MutexGuard<'a, T>, String> {
    loop {
        deadline.check()?;
        match mutex.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(TryLockError::Poisoned(_)) => return Err("MCP connection lock poisoned".into()),
            Err(TryLockError::WouldBlock) => std::thread::sleep(deadline.slice()?),
        }
    }
}
fn with_session<T>(
    key: String,
    config: McpServerConfig,
    timeout: u64,
    control: &Control,
    run: impl FnOnce(&mut Session, &Control) -> Result<T, String>,
) -> Result<T, String> {
    let entry = {
        let mut sessions = sessions()
            .lock()
            .map_err(|_| "MCP session registry poisoned")?;
        if !sessions.contains_key(&key) && sessions.len() >= 64 {
            return Err("MCP connection limit (64) reached; close unused connections with mcp_list action=close".into());
        }
        match sessions.get(&key) {
            Some(entry) => entry.clone(),
            None => {
                let (notifications, receiver) = mpsc::sync_channel(64);
                spawn_dispatcher(key.clone(), receiver);
                let entry = Arc::new(Entry {
                    session: Mutex::new(None),
                    stop: Control::default(),
                    notifications,
                });
                sessions.insert(key.clone(), Arc::clone(&entry));
                entry
            }
        }
    };
    let control = control.linked(&entry.stop);
    let deadline = Deadline::new(timeout, "connection lock", &control);
    let mut state = lock_until(&entry.session, &deadline)?;
    if state.as_ref().is_some_and(|s| s.config != config) {
        state.take();
    }
    if state.is_none() {
        *state = Some(Session::start(
            &config,
            timeout,
            &control,
            &entry.notifications,
        )?);
    }
    let session = state.as_mut().ok_or("MCP session unavailable")?;
    session.transport.timeout(timeout);
    let result = run(session, &control);
    if session.failed {
        state.take();
    } // Do not retry tool calls: they may have side effects.
    result
}

pub fn shutdown_connections() {
    if let Ok(mut subs) = subscriptions().lock() {
        subs.clear();
    }
    let entries = if let Ok(mut map) = sessions().lock() {
        std::mem::take(&mut *map)
    } else {
        return;
    };
    for entry in entries.values() {
        entry.stop.cancel();
    }
    for entry in entries.into_values() {
        if let Ok(mut state) = entry.session.lock() {
            state.take();
        }
    }
}
fn close_key(key: &str) -> Result<(), String> {
    clear_subscriptions(key);
    let entry = sessions()
        .lock()
        .map_err(|_| "MCP session registry poisoned")?
        .remove(key);
    if let Some(entry) = entry {
        entry.stop.cancel();
        entry
            .session
            .lock()
            .map_err(|_| "MCP connection lock poisoned")?
            .take();
    }
    Ok(())
}

fn host_scope(params: &Value) -> String {
    let host = params.get(rpi_plugin_sdk::HOST_CONTEXT_KEY);
    if let Some(id) = host
        .and_then(|v| v.get("sessionId"))
        .and_then(Value::as_str)
    {
        return format!("session:{id}");
    }
    if let Some(cwd) = host.and_then(|v| v.get("cwd")).and_then(Value::as_str) {
        return format!(
            "project:{}",
            std::fs::canonicalize(cwd)
                .unwrap_or_else(|_| PathBuf::from(cwd))
                .to_string_lossy()
        );
    }
    "library".into()
}

fn host_cwd(params: &Value) -> PathBuf {
    params
        .get(rpi_plugin_sdk::HOST_CONTEXT_KEY)
        .and_then(|v| v.get("cwd"))
        .and_then(Value::as_str)
        .map(PathBuf::from)
        .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")))
}
fn config_file(params: &Value) -> Result<PathBuf, String> {
    let path = params
        .get("configPath")
        .and_then(Value::as_str)
        .ok_or("missing configPath")?;
    std::fs::canonicalize(host_cwd(params).join(path))
        .map_err(|e| format!("read MCP config {path:?}: {e}"))
}
fn load_config(params: &Value) -> Result<(PathBuf, McpConfig), String> {
    let path = config_file(params)?;
    let text = std::fs::read_to_string(&path).map_err(|e| format!("read MCP config: {e}"))?;
    Ok((path, McpConfig::parse(&text)?))
}
fn target(params: &Value) -> Result<(String, String, McpServerConfig, u64), String> {
    let scope = host_scope(params);
    let (name, identity, mut config) = if params.get("configPath").is_some() {
        if params.get("url").is_some() {
            return Err("specify configPath/server or url, not both".into());
        }
        let name = crate::kit::string_param(params, "server")?;
        let (path, config) = load_config(params)?;
        let server = config
            .servers
            .get(&name)
            .ok_or_else(|| format!("MCP server {name:?} is not configured"))?
            .clone();
        (name, path.to_string_lossy().into_owned(), server)
    } else {
        if params.get("server").is_some() {
            return Err("server requires configPath".into());
        }
        let url =
            crate::http::validate_mcp_url(&crate::kit::string_param(params, "url")?)?.to_string();
        let config = McpServerConfig::Http(crate::HttpServer {
            url: url.clone(),
            headers: Default::default(),
            enabled: true,
            timeout: 60,
            exposure: Default::default(),
        });
        ("http".into(), url, config)
    };
    let key = serde_json::to_string(&(scope, identity, &name)).map_err(|e| e.to_string())?;
    let (enabled, default_timeout) = match &config {
        McpServerConfig::Stdio(s) => (s.enabled, s.timeout),
        McpServerConfig::Http(s) => (s.enabled, s.timeout),
    };
    if !enabled {
        close_key(&key)?;
        return Err(format!("MCP server {name:?} is disabled"));
    }
    if let McpServerConfig::Stdio(server) = &mut config {
        // A configured cwd is relative to the config file; otherwise use host project cwd.
        if let Some(cwd) = &server.cwd {
            if Path::new(cwd).is_relative() {
                let file = config_file(params)?;
                server.cwd = Some(
                    file.parent()
                        .unwrap_or(Path::new("."))
                        .join(cwd)
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        } else {
            server.cwd = Some(host_cwd(params).to_string_lossy().into_owned());
        }
    }
    let timeout = match params.get("timeoutSeconds") {
        None => default_timeout.clamp(1, 300),
        Some(value) => value
            .as_u64()
            .filter(|v| (1..=300).contains(v))
            .ok_or("timeoutSeconds must be an integer from 1 to 300")?,
    };
    Ok((key, name, config, timeout))
}

pub(crate) fn call(params: &Value, control: &Control) -> Result<Value, String> {
    let tool = crate::kit::string_param(params, "tool")?;
    if tool.trim().is_empty() || tool.len() > 200 {
        return Err("MCP tool must contain 1-200 characters".into());
    }
    let arguments = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    if !arguments.is_object() {
        return Err("MCP arguments must be an object".into());
    }
    let (key, _, config, timeout) = target(params)?;
    with_session(key, config, timeout, control, |session, control| {
        let tools = session.tools(control)?;
        if !tools.iter().any(|t| t.name == tool) {
            return Err(format!("MCP server does not expose tool {tool:?}"));
        }
        let request = tools_call_request(0, &tool, arguments);
        response_result(session.request(
            "tools/call",
            request.params.unwrap_or_else(|| json!({})),
            control,
        )?)
    })
}
pub(crate) fn list(params: &Value, control: &Control) -> Result<Value, String> {
    control.check()?;
    let action = params
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("list");
    if !matches!(action, "list" | "close") {
        return Err("mcp_list action must be list or close".into());
    }
    if params.get("configPath").is_some() && params.get("server").is_none() {
        if action != "list" {
            return Err("close requires a server or url".into());
        }
        let (_, config) = load_config(params)?;
        let servers: Vec<Value> = config
            .servers
            .iter()
            .map(|(name, config)| {
                let (transport, enabled) = match config {
                    McpServerConfig::Stdio(s) => ("stdio", s.enabled),
                    McpServerConfig::Http(s) => ("http", s.enabled),
                };
                json!({"server":name, "transport":transport, "enabled":enabled})
            })
            .collect();
        return Ok(json!({"servers":servers}));
    }
    let (key, name, config, timeout) = target(params)?;
    if action == "close" {
        close_key(&key)?;
        return Ok(json!({"server":name,"closed":true}));
    }
    with_session(key, config, timeout, control, |session, control| {
        let tools = session.tools(control)?;
        Ok(
            json!({"server":name, "serverInfo":{"name":session.info.name,"version":session.info.version},"protocolVersion":session.info.protocol_version,"tools":tools}),
        )
    })
}

pub fn call_from_config(
    config_path: &str,
    server_name: &str,
    tool_name: &str,
    arguments: Value,
) -> Result<String, String> {
    call_from_config_with_timeout(config_path, server_name, tool_name, arguments, None)
}
pub fn call_from_config_with_timeout(
    config_path: &str,
    server_name: &str,
    tool_name: &str,
    arguments: Value,
    timeout: Option<u64>,
) -> Result<String, String> {
    let mut params = json!({"configPath":config_path,"server":server_name,"tool":tool_name,"arguments":arguments});
    if let Some(timeout) = timeout {
        params["timeoutSeconds"] = timeout.into();
    }
    let result = call(&params, &Control::default())?;
    crate::connection::check_tool_result(&result)?;
    Ok(crate::call_result_text(&result))
}
pub fn discover_from_config(
    config_path: &str,
    server_name: &str,
) -> Result<DiscoveredServer, String> {
    let value = list(
        &json!({"configPath":config_path,"server":server_name}),
        &Control::default(),
    )?;
    Ok(DiscoveredServer {
        name: server_name.into(),
        info: ServerInfo {
            name: value["serverInfo"]["name"]
                .as_str()
                .unwrap_or("unknown")
                .into(),
            version: value["serverInfo"]["version"]
                .as_str()
                .unwrap_or("unknown")
                .into(),
            protocol_version: value["protocolVersion"].as_str().unwrap_or("").into(),
        },
        tools: serde_json::from_value(value["tools"].clone()).map_err(|e| e.to_string())?,
    })
}
pub fn close_from_config(config_path: &str, server_name: &str) -> Result<(), String> {
    list(
        &json!({"configPath":config_path,"server":server_name,"action":"close"}),
        &Control::default(),
    )
    .map(|_| ())
}

#[cfg(test)]
mod tests {
    use super::resource_text;
    use serde_json::json;

    #[test]
    fn resource_text_flattens_text_and_notes_blobs() {
        let text = resource_text(&json!({"contents": [
            {"uri": "res://a", "mimeType": "text/plain", "text": "hello"},
            {"uri": "res://b", "blob": "QUJD"}]}));
        assert!(text.contains("res://a"));
        assert!(text.contains("hello"));
        assert!(text.contains("[blob, 4 base64 chars]"), "got: {text}");
    }
}
