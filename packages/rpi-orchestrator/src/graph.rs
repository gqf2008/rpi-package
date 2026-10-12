//! The orchestration DAG: nodes, dependencies, statuses, plus the ready-set /
//! no-progress logic the scheduler is built on.
//!
//! Pure data and algorithms (no session, no child process), so every rule that
//! decides *what runs next* is unit-testable on its own.

use serde_json::{json, Value};

pub const DEFAULT_MAX_TURNS: u64 = 8;
pub const MAX_MAX_TURNS: u64 = 32;
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;
pub const MIN_TIMEOUT_SECS: u64 = 10;
pub const MAX_TIMEOUT_SECS: u64 = 1800;
pub const MAX_TASK_BYTES: usize = 20_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Pending,
    Running,
    Done,
    Failed,
    Skipped,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Status::Pending => "pending",
            Status::Running => "running",
            Status::Done => "done",
            Status::Failed => "failed",
            Status::Skipped => "skipped",
        }
    }

    pub fn parse(value: &str) -> Option<Status> {
        match value {
            "pending" => Some(Status::Pending),
            "running" => Some(Status::Running),
            "done" | "completed" => Some(Status::Done),
            "failed" => Some(Status::Failed),
            "skipped" => Some(Status::Skipped),
            _ => None,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub id: String,
    pub task: String,
    pub deps: Vec<String>,
    pub role: Option<String>,
    pub provider: Option<String>,
    pub model: Option<String>,
    pub max_turns: u64,
    pub timeout_secs: u64,
    pub status: Status,
    pub result: Option<String>,
}

impl Node {
    pub fn from_value(value: &Value) -> Result<Node, String> {
        let id = value
            .get("id")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|id| !id.is_empty())
            .ok_or("every node needs a non-empty `id`")?
            .to_string();
        let task = value
            .get("task")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|task| !task.is_empty())
            .ok_or_else(|| format!("node `{id}` needs a non-empty `task`"))?
            .to_string();
        if task.len() > MAX_TASK_BYTES {
            return Err(format!("node `{id}` task exceeds {MAX_TASK_BYTES} bytes"));
        }
        let mut deps = parse_string_list(value.get("deps"), &format!("node `{id}` deps"))?;
        // Duplicate dependency edges are meaningless and would break the
        // indegree count in `check_acyclic`.
        let mut seen_deps: Vec<String> = Vec::with_capacity(deps.len());
        deps.retain(|dep| {
            if seen_deps.contains(dep) {
                false
            } else {
                seen_deps.push(dep.clone());
                true
            }
        });
        let role = string_field(value, "role");
        let provider = string_field(value, "provider");
        let model = string_field(value, "model");
        let max_turns = clamp(
            value.get("maxTurns").and_then(Value::as_u64),
            DEFAULT_MAX_TURNS,
            1,
            MAX_MAX_TURNS,
        );
        let timeout_secs = clamp(
            value.get("timeoutSeconds").and_then(Value::as_u64),
            DEFAULT_TIMEOUT_SECS,
            MIN_TIMEOUT_SECS,
            MAX_TIMEOUT_SECS,
        );
        Ok(Node {
            id,
            task,
            deps,
            role,
            provider,
            model,
            max_turns,
            timeout_secs,
            status: Status::Pending,
            result: None,
        })
    }

    fn to_value(&self) -> Value {
        json!({
            "id": self.id,
            "task": self.task,
            "deps": self.deps,
            "role": self.role,
            "provider": self.provider,
            "model": self.model,
            "maxTurns": self.max_turns,
            "timeoutSeconds": self.timeout_secs,
            "status": self.status.as_str(),
            "result": self.result,
        })
    }
}

#[derive(Clone, Debug, Default)]
pub struct Graph {
    pub nodes: Vec<Node>,
}

impl Graph {
    pub fn from_nodes_value(value: &Value) -> Result<Graph, String> {
        let array = value.as_array().ok_or("`nodes` must be an array")?;
        if array.is_empty() {
            return Err("`nodes` must contain at least one node".into());
        }
        let mut nodes = Vec::with_capacity(array.len());
        for item in array {
            nodes.push(Node::from_value(item)?);
        }
        let graph = Graph { nodes };
        graph.validate()?;
        Ok(graph)
    }

    pub fn from_details(value: &Value) -> Option<Graph> {
        let array = value.get("nodes")?.as_array()?;
        let mut nodes = Vec::with_capacity(array.len());
        for item in array {
            let node = Node::from_value(item).ok()?;
            let status = item
                .get("status")
                .and_then(Value::as_str)
                .and_then(Status::parse)
                .unwrap_or(Status::Pending);
            let result = item
                .get("result")
                .and_then(Value::as_str)
                .map(str::to_string);
            nodes.push(Node {
                status,
                result,
                ..node
            });
        }
        Some(Graph { nodes })
    }

    pub fn to_details(&self) -> Value {
        json!({"nodes": self.nodes.iter().map(Node::to_value).collect::<Vec<_>>()})
    }

    pub fn get(&self, id: &str) -> Option<&Node> {
        self.nodes.iter().find(|node| node.id == id)
    }

    pub fn get_mut(&mut self, id: &str) -> Option<&mut Node> {
        self.nodes.iter_mut().find(|node| node.id == id)
    }

    pub fn has_pending(&self) -> bool {
        self.nodes.iter().any(|node| node.status == Status::Pending)
    }

    /// Ids of `pending` nodes whose dependencies are all `done`.
    pub fn ready(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|node| node.status == Status::Pending)
            .filter(|node| {
                node.deps
                    .iter()
                    .all(|dep| self.get(dep).map(Node::is_done).unwrap_or(false))
            })
            .map(|node| node.id.clone())
            .collect()
    }

    /// Every remaining `pending` node — the caller reports these when nothing is
    /// ready, which is the "no progress" condition.
    pub fn pending_ids(&self) -> Vec<String> {
        self.nodes
            .iter()
            .filter(|node| node.status == Status::Pending)
            .map(|node| node.id.clone())
            .collect()
    }

    /// Mark every pending node that depends (transitively) on a failed/skipped
    /// node as skipped, so a dead branch cannot look dispatchable forever.
    pub fn propagate_skips(&mut self) {
        loop {
            let mut changed = false;
            let dead: Vec<String> = self
                .nodes
                .iter()
                .filter(|node| matches!(node.status, Status::Failed | Status::Skipped))
                .map(|node| node.id.clone())
                .collect();
            for node in &mut self.nodes {
                if node.status != Status::Pending {
                    continue;
                }
                if node.deps.iter().any(|dep| dead.contains(dep)) {
                    node.status = Status::Skipped;
                    changed = true;
                }
            }
            if !changed {
                break;
            }
        }
    }

    /// Unique ids, deps that exist, and no cycles.
    pub fn validate(&self) -> Result<(), String> {
        let mut seen: Vec<&str> = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            if seen.contains(&node.id.as_str()) {
                return Err(format!("duplicate node id `{}`", node.id));
            }
            seen.push(&node.id);
        }
        for node in &self.nodes {
            for dep in &node.deps {
                if !seen.contains(&dep.as_str()) {
                    return Err(format!("node `{}` depends on unknown `{dep}`", node.id));
                }
            }
        }
        self.check_acyclic()
    }

    fn check_acyclic(&self) -> Result<(), String> {
        // Kahn's algorithm over the dependency edges.
        let mut indegree: Vec<usize> = self.nodes.iter().map(|node| node.deps.len()).collect();
        let mut queue: Vec<usize> = Vec::new();
        for (index, degree) in indegree.iter().enumerate() {
            if *degree == 0 {
                queue.push(index);
            }
        }
        let mut visited = 0usize;
        while let Some(index) = queue.pop() {
            visited += 1;
            let id = self.nodes[index].id.clone();
            for (other, node) in self.nodes.iter().enumerate() {
                if node.deps.iter().any(|dep| *dep == id) {
                    indegree[other] -= 1;
                    if indegree[other] == 0 {
                        queue.push(other);
                    }
                }
            }
        }
        if visited == self.nodes.len() {
            Ok(())
        } else {
            let cyclic: Vec<String> = self
                .nodes
                .iter()
                .enumerate()
                .filter(|(index, _)| indegree[*index] > 0)
                .map(|(_, node)| node.id.clone())
                .collect();
            Err(format!("cycle detected among: {}", cyclic.join(", ")))
        }
    }

    pub fn render(&self) -> String {
        if self.nodes.is_empty() {
            return "plan is empty".into();
        }
        let done = self
            .nodes
            .iter()
            .filter(|node| node.status == Status::Done)
            .count();
        let mut lines = vec![format!("{done}/{} done", self.nodes.len())];
        for node in &self.nodes {
            let mark = match node.status {
                Status::Done => "x",
                Status::Failed => "!",
                Status::Skipped => "-",
                Status::Running => "~",
                Status::Pending => " ",
            };
            let deps = if node.deps.is_empty() {
                String::new()
            } else {
                format!(" deps=[{}]", node.deps.join(","))
            };
            lines.push(format!(
                "[{mark}] {} {}{deps}",
                node.id,
                node.status.as_str()
            ));
        }
        lines.join("\n")
    }
}

impl Node {
    pub fn is_done(&self) -> bool {
        self.status == Status::Done
    }
}

fn clamp(value: Option<u64>, default: u64, min: u64, max: u64) -> u64 {
    value.unwrap_or(default).clamp(min, max)
}

fn string_field(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_string)
}

fn parse_string_list(value: Option<&Value>, what: &str) -> Result<Vec<String>, String> {
    let Some(value) = value else {
        return Ok(Vec::new());
    };
    let array = value
        .as_array()
        .ok_or_else(|| format!("{what} must be an array of strings"))?;
    let mut out = Vec::with_capacity(array.len());
    for item in array {
        let text = item
            .as_str()
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .ok_or_else(|| format!("{what} must contain non-empty strings"))?;
        out.push(text.to_string());
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn graph(nodes: Value) -> Graph {
        Graph::from_nodes_value(&nodes).unwrap()
    }

    #[test]
    fn rejects_duplicate_ids_and_unknown_deps() {
        let dup = Graph::from_nodes_value(&json!([
            {"id": "a", "task": "x"},
            {"id": "a", "task": "y"}
        ]));
        assert!(dup.unwrap_err().contains("duplicate"));

        let unknown = Graph::from_nodes_value(&json!([
            {"id": "a", "task": "x", "deps": ["ghost"]}
        ]));
        assert!(unknown.unwrap_err().contains("unknown"));
    }

    #[test]
    fn rejects_cycles() {
        let err = Graph::from_nodes_value(&json!([
            {"id": "a", "task": "x", "deps": ["b"]},
            {"id": "b", "task": "y", "deps": ["a"]}
        ]))
        .unwrap_err();
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn ready_set_respects_dependencies() {
        let mut g = graph(json!([
            {"id": "a", "task": "x"},
            {"id": "b", "task": "y"},
            {"id": "c", "task": "z", "deps": ["a", "b"]}
        ]));
        assert_eq!(g.ready(), vec!["a".to_string(), "b".to_string()]);
        g.get_mut("a").unwrap().status = Status::Done;
        assert_eq!(g.ready(), vec!["b".to_string()]);
        g.get_mut("b").unwrap().status = Status::Done;
        assert_eq!(g.ready(), vec!["c".to_string()]);
    }

    #[test]
    fn failed_dependency_skips_downstream_transitively() {
        let mut g = graph(json!([
            {"id": "a", "task": "x"},
            {"id": "b", "task": "y", "deps": ["a"]},
            {"id": "c", "task": "z", "deps": ["b"]}
        ]));
        g.get_mut("a").unwrap().status = Status::Failed;
        g.propagate_skips();
        assert_eq!(g.get("b").unwrap().status, Status::Skipped);
        assert_eq!(g.get("c").unwrap().status, Status::Skipped);
        assert!(!g.has_pending());
    }

    #[test]
    fn details_round_trip_preserves_status() {
        let mut g = graph(json!([{"id": "a", "task": "x"}]));
        g.get_mut("a").unwrap().status = Status::Done;
        g.get_mut("a").unwrap().result = Some("ok".into());
        let restored = Graph::from_details(&g.to_details()).unwrap();
        assert_eq!(restored.get("a").unwrap().status, Status::Done);
        assert_eq!(restored.get("a").unwrap().result.as_deref(), Some("ok"));
    }
}
