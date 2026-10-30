//! The read-only views the tabs draw, folded from node-admin's typed answers over Node RPC: the fabric
//! header, the topology with its connection edges, and the Builds.

use crate::chaos::NodeFacts;
use crate::admin::Admin;
use rdm_client::{BuildId, CallEnd};
use serde_json::{json, Value};

fn text(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_string)
}

/// The primary role a node holds, as the tabs label it. "mesh primary" is the node-admin node-type primary
/// only; the fabric-primary is that node-type primary of its own mesh as well.
pub fn primary_label(kind: &str, is_primary: bool, is_fabric_primary: bool) -> &'static str {
    match (kind, is_primary, is_fabric_primary) {
        (_, _, true) => "fabric primary · mesh primary",
        ("node_admin", true, false) => "mesh primary",
        _ => "",
    }
}

/// mesh.nodes.get of every mesh the entry admin's view holds, as JSON objects (the client's `NodeView` serialized).
pub async fn nodes(admin: &Admin) -> Result<Vec<Value>, CallEnd> {
    Ok(admin.nodes().await?.iter().map(|n| serde_json::to_value(n).unwrap_or(Value::Null)).collect())
}

/// What a fault needs of each node.
pub fn facts(nodes: &[Value]) -> Vec<NodeFacts> {
    nodes
        .iter()
        .filter_map(|n| {
            Some(NodeFacts {
                name: text(n, "name")?,
                mesh: text(n, "mesh")?,
                is_fabric_primary: n["is_fabric_primary"].as_bool().unwrap_or(false),
                data_dir: text(n, "data_dir"),
                transport_port: text(n, "transport_addr").and_then(|a| a.rsplit(':').next().and_then(|p| p.parse().ok())),
            })
        })
        .collect()
}

/// The fabric header: state, current Build, meshes with node counts.
pub async fn overview(admin: &Admin) -> Value {
    let fabric = match admin.fabric().await {
        Ok(f) => serde_json::to_value(f).unwrap_or(Value::Null),
        Err(e) => return json!({"fabric": null, "meshes": [], "nodes_total": 0, "build": null, "error": format!("node-admin unreadable: {e}")}),
    };
    let nodes = nodes(admin).await.unwrap_or_default();
    let meshes: Vec<Value> = fabric["meshes"]
        .as_array()
        .map(|ms| {
            ms.iter()
                .map(|m| {
                    let name = text(m, "name").unwrap_or_default();
                    let count = nodes.iter().filter(|n| text(n, "mesh").as_deref() == Some(name.as_str())).count();
                    json!({"name": name, "status": m["status"], "primary_admin": m["primary_admin"], "nodes": count})
                })
                .collect()
        })
        .unwrap_or_default();
    let build = match text(&fabric, "build_id") {
        Some(id) => match BuildId::parse(&id) {
            Ok(id) => admin.build(&id).await.ok().map(|b| brief(&receipts(&b))),
            Err(_) => None,
        },
        None => None,
    };
    json!({
        "fabric": {"name": fabric["name"], "status": fabric["status"], "provider": fabric["provider"], "build_id": fabric["build_id"], "fabric_primary": fabric["fabric_primary"]},
        "meshes": meshes,
        "nodes_total": nodes.len(),
        "build": build,
        "error": null,
    })
}

/// A Build's header as the tabs draw it.
fn receipts(b: &rdm_client::Build) -> Value {
    json!({ "build_id": b.build_id.to_string(), "phase": format!("{:?}", b.phase), "attempt": b.attempt, "reason": b.reason, "executor": b.executor, "last_failure": b.last_failure, "steps": b.steps.len() })
}

fn brief(v: &Value) -> Value {
    json!({ "build_id": v["build_id"], "state": v["phase"], "attempt": v["attempt"], "reason": v["reason"], "executor": v["executor"] })
}

/// The nodes with their role labels, and every connection fact each node answers for its own exact
/// birth (node.connection.get, served by every node): the data plane's Direct and Proxy rows live on
/// the role nodes that hold them, not only on the node-admins.
pub async fn topology(admin: &Admin) -> Result<Value, CallEnd> {
    let views = admin.nodes().await?;
    let view: Vec<Value> = views
        .iter()
        .map(|n| {
            let n = serde_json::to_value(n).unwrap_or(Value::Null);
            let kind = text(&n, "kind").unwrap_or_default();
            let (p, fp) = (n["is_primary"].as_bool().unwrap_or(false), n["is_fabric_primary"].as_bool().unwrap_or(false));
            json!({
                "name": n["name"], "kind": kind, "mesh": n["mesh"], "status": n["status"], "role": primary_label(&kind, p, fp),
                "is_primary": p, "is_fabric_primary": fp, "declared": n["declared"], "incarnation_id": n["incarnation_id"], "node_id": n["node_id"],
                "has_runtime": n["data_dir"].is_string(),
            })
        })
        .collect();
    let mut edges: Vec<Value> = Vec::new();
    let mut errors = Vec::new();
    for n in views.iter() {
        match admin.connections(n).await {
            Some(Ok(c)) => {
                let by = n.name.to_string();
                for e in serde_json::to_value(&c).unwrap_or(Value::Null)["connections"].as_array().into_iter().flatten() {
                    let mut e = e.clone();
                    e["reported_by"] = json!(by);
                    // One fact per (source, destination, kind): the latest any admin reports.
                    let same = |x: &Value| x["source"] == e["source"] && x["destination"] == e["destination"] && x["kind"] == e["kind"];
                    match edges.iter().position(|x| same(x)) {
                        Some(i) if edges[i]["logged_at_ms"].as_u64() >= e["logged_at_ms"].as_u64() => {}
                        Some(i) => edges[i] = e,
                        None => edges.push(e),
                    }
                }
            }
            Some(Err(e)) => errors.push(format!("{}: {e}", n.name)),
            None => {}
        }
    }
    Ok(json!({"nodes": view, "edges": edges, "edge_errors": errors}))
}

/// The current accepted Build and the Builds this UI submitted, newest first.
pub async fn builds(admin: &Admin, submitted: &[String]) -> Value {
    let current = admin.fabric().await.ok().and_then(|f| f.build_id);
    let mut ids: Vec<String> = Vec::new();
    for id in current.iter().chain(submitted.iter().rev()) {
        if !ids.contains(id) {
            ids.push(id.clone());
        }
    }
    let mut out = Vec::new();
    for id in ids.iter().take(20) {
        let Ok(parsed) = BuildId::parse(id) else { continue };
        if let Ok(b) = admin.build(&parsed).await {
            out.push(receipts(&b));
        }
    }
    json!({"current": current, "builds": out})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_node_admins_node_type_primary_is_labelled_mesh_primary() {
        assert_eq!(primary_label("rpc_node", true, false), "", "an rpc node first in its cohort is not a mesh primary");
        assert_eq!(primary_label("node_admin", true, false), "mesh primary");
        assert_eq!(primary_label("node_admin", false, false), "");
        assert_eq!(primary_label("node_admin", true, true), "fabric primary · mesh primary");
    }
}
