//! The `plan` tool: author and edit the task DAG.
//!
//! Scheduling lives in `dispatch`; this tool only changes the graph's *shape*
//! (and can re-arm node state). Every result carries the graph in `details` so
//! it can be replayed from the session branch.
//!
//! There is deliberately **no** way for the model to mark a node `done`/`failed`:
//! completion is only ever written by `dispatch`, so the plan cannot be
//! "completed" by hand. `reset` is the one state edit the model keeps.

use crate::control::Control;
use crate::graph::{Graph, Node, Status};
use crate::graph_state;
use crate::kit::{result_with_details, Progress};
use serde_json::Value;

pub fn plan(params: &Value, control: &Control, _progress: &Progress) -> Result<String, String> {
    control.check()?;
    let action = params
        .get("action")
        .and_then(Value::as_str)
        .unwrap_or("show");
    let mut guard = graph_state().lock().unwrap_or_else(|p| p.into_inner());
    match action {
        "set" => {
            let nodes = params
                .get("nodes")
                .ok_or("`nodes` is required for action=set")?;
            *guard = Graph::from_nodes_value(nodes)?;
        }
        "update" => {
            let mut next = guard.clone();
            apply_update(&mut next, params)?;
            next.validate()?;
            *guard = next;
        }
        "show" => {}
        other => {
            return Err(format!(
                "unknown plan action `{other}` (expected set, update, show)"
            ))
        }
    }
    Ok(result_with_details(&guard.render(), guard.to_details()))
}

fn apply_update(graph: &mut Graph, params: &Value) -> Result<(), String> {
    if let Some(add) = params.get("addNodes") {
        let array = add.as_array().ok_or("`addNodes` must be an array")?;
        for item in array {
            let node = Node::from_value(item)?;
            if graph.get(&node.id).is_some() {
                return Err(format!("node `{}` already exists", node.id));
            }
            graph.nodes.push(node);
        }
    }
    if let Some(remove) = params.get("removeIds") {
        let array = remove.as_array().ok_or("`removeIds` must be an array")?;
        for item in array {
            let id = item
                .as_str()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .ok_or("`removeIds` must contain non-empty strings")?;
            graph.nodes.retain(|node| node.id != id);
        }
    }
    if let Some(set_deps) = params.get("setDeps") {
        let array = set_deps.as_array().ok_or("`setDeps` must be an array")?;
        for item in array {
            let id = item
                .get("id")
                .and_then(Value::as_str)
                .ok_or("each `setDeps` entry needs an `id`")?;
            let deps = item
                .get("deps")
                .and_then(Value::as_array)
                .ok_or("each `setDeps` entry needs a `deps` array")?;
            let mut parsed = Vec::with_capacity(deps.len());
            for dep in deps {
                parsed.push(
                    dep.as_str()
                        .map(str::trim)
                        .filter(|dep| !dep.is_empty())
                        .ok_or("`deps` must contain non-empty strings")?
                        .to_string(),
                );
            }
            let node = graph
                .get_mut(id)
                .ok_or_else(|| format!("node `{id}` not found"))?;
            node.deps = parsed;
        }
    }
    if let Some(reset) = params.get("reset") {
        let array = reset.as_array().ok_or("`reset` must be an array")?;
        for item in array {
            let id = item
                .as_str()
                .map(str::trim)
                .filter(|id| !id.is_empty())
                .ok_or("`reset` must contain non-empty strings")?;
            let node = graph
                .get_mut(id)
                .ok_or_else(|| format!("node `{id}` not found"))?;
            node.status = Status::Pending;
            node.result = None;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn seeded() -> Graph {
        Graph::from_nodes_value(&json!([
            {"id": "a", "task": "x"},
            {"id": "b", "task": "y", "deps": ["a"]}
        ]))
        .unwrap()
    }

    #[test]
    fn update_can_add_remove_and_reset() {
        let mut g = seeded();
        apply_update(
            &mut g,
            &json!({"addNodes": [{"id": "c", "task": "z", "deps": ["b"]}]}),
        )
        .unwrap();
        assert_eq!(g.nodes.len(), 3);

        apply_update(&mut g, &json!({"setDeps": [{"id": "b", "deps": []}]})).unwrap();
        assert!(g.get("b").unwrap().deps.is_empty());

        apply_update(&mut g, &json!({"reset": ["a"]})).unwrap();
        assert_eq!(g.get("a").unwrap().status, Status::Pending);

        apply_update(&mut g, &json!({"removeIds": ["c"]})).unwrap();
        assert!(g.get("c").is_none());
    }

    #[test]
    fn update_cannot_mark_nodes_done() {
        // Completion is dispatch-only: there is no `status` action, so a stray
        // `status` field is ignored and cannot hand-mark a node done.
        let mut g = seeded();
        apply_update(&mut g, &json!({"status": [{"id": "a", "status": "done"}]})).unwrap();
        assert_eq!(g.get("a").unwrap().status, Status::Pending);
    }

    #[test]
    fn update_rejects_unknown_dep_and_cycle() {
        let mut g = seeded();
        // A missing dep is caught by validate(), not apply_update().
        apply_update(
            &mut g,
            &json!({"setDeps": [{"id": "b", "deps": ["ghost"]}]}),
        )
        .unwrap();
        assert!(g.validate().unwrap_err().contains("unknown"));

        let mut g = seeded();
        apply_update(&mut g, &json!({"setDeps": [{"id": "a", "deps": ["b"]}]})).unwrap();
        assert!(g.validate().unwrap_err().contains("cycle"));
    }
}
