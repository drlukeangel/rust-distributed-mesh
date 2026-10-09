//! The read-only views the tabs draw, folded from node-admin's REST answers: the fabric header, the
//! topology with its connection edges, and the Builds.

use crate::chaos::NodeFacts;
use rafka_node_admin_client::{BuildId, ClientError, NodeAdminClient};
use serde_json::{json, Value};

fn text(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_string)
}

/// The seat a node holds, as the tabs label it. "mesh primary" is the node-admin cohort's primary
/// only; the fabric-primary is that cohort's primary of its own mesh as well.
pub fn seat_label(kind: &str, is_primary: bool, is_fabric_primary: bool) -> &'static str {
    match (kind, is_fabric_primary, is_primary) {
        (_, true, _) => "fabric primary · mesh primary",
        ("node_admin", false, true) => "mesh primary",
        _ => "",
    }
}

/// Node-admin's `GET /api/nodes`, as JSON objects (the client's `NodeView` serialized).
pub async fn nodes(admin: &NodeAdminClient) -> Result<Vec<Value>, ClientError> {
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

async fn get_json(http: &reqwest::Client, url: &str) -> Result<Value, String> {
    let res = http.get(url).send().await.map_err(|e| format!("{url}: {e}"))?;
    let status = res.status();
    let body: Value = res.json().await.map_err(|e| format!("{url}: undecodable answer: {e}"))?;
    if !status.is_success() {
        return Err(format!("{url}: {status} {}", body));
    }
    Ok(body)
}

/// The fabric header: state, current Build, meshes with node counts.
pub async fn overview(admin: &NodeAdminClient, http: &reqwest::Client) -> Value {
    let fabric = match get_json(http, &format!("{}/api/fabric", admin.base())).await {
        Ok(f) => f,
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
        Some(id) => admin.build_view(&BuildId(id)).await.ok().map(|b| brief(&b)),
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

fn brief(b: &rafka_node_admin_client::BuildView) -> Value {
    let v = serde_json::to_value(b).unwrap_or(Value::Null);
    json!({
        "build_id": v["build_id"], "state": v["state"], "attempt": v["attempt"], "reason": v["reason"], "executor": v["executor"],
        "change": v["submitted_change"]["kind"],
    })
}

/// The nodes with their seat labels, and every connection fact node-admin's own view of the
/// fabric can read: asked of each node-admin that publishes an API.
pub async fn topology(admin: &NodeAdminClient, http: &reqwest::Client) -> Result<Value, ClientError> {
    let nodes = nodes(admin).await?;
    let view: Vec<Value> = nodes
        .iter()
        .map(|n| {
            let kind = text(n, "kind").unwrap_or_default();
            let (p, fp) = (n["is_primary"].as_bool().unwrap_or(false), n["is_fabric_primary"].as_bool().unwrap_or(false));
            json!({
                "name": n["name"], "kind": kind, "mesh": n["mesh"], "status": n["status"], "seat": seat_label(&kind, p, fp),
                "is_primary": p, "is_fabric_primary": fp, "declared": n["declared"], "incarnation_id": n["incarnation_id"], "node_id": n["node_id"],
                "has_runtime": n["data_dir"].is_string(),
            })
        })
        .collect();
    let mut edges: Vec<Value> = Vec::new();
    let mut errors = Vec::new();
    for n in &nodes {
        let Some(base) = text(n, "admin_api_base") else { continue };
        match get_json(http, &format!("{}/api/connections", base.trim_end_matches('/'))).await {
            Ok(c) => {
                let by = text(n, "name").unwrap_or_default();
                for e in c["connections"].as_array().into_iter().flatten() {
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
            Err(e) => errors.push(e),
        }
    }
    Ok(json!({"nodes": view, "edges": edges, "edge_errors": errors}))
}

/// The current accepted Build and the Builds this UI submitted, newest first, each with its steps.
pub async fn builds(admin: &NodeAdminClient, http: &reqwest::Client, submitted: &[String]) -> Value {
    let current = get_json(http, &format!("{}/api/fabric", admin.base())).await.ok().and_then(|f| text(&f, "build_id"));
    let mut ids: Vec<String> = Vec::new();
    for id in current.iter().chain(submitted.iter().rev()) {
        if !ids.contains(id) {
            ids.push(id.clone());
        }
    }
    let mut out = Vec::new();
    for id in ids.iter().take(20) {
        if let Ok(b) = admin.build_view(&BuildId(id.clone())).await {
            out.push(serde_json::to_value(&b).unwrap_or(Value::Null));
        }
    }
    out.sort_by(|a, b| b["submitted_at_ms"].as_u64().cmp(&a["submitted_at_ms"].as_u64()));
    json!({"current": current, "builds": out})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_node_admin_cohorts_primary_is_labelled_mesh_primary() {
        assert_eq!(seat_label("rpc_node", true, false), "", "an rpc node first in its cohort is not a mesh primary");
        assert_eq!(seat_label("node_admin", true, false), "mesh primary");
        assert_eq!(seat_label("node_admin", false, false), "");
        assert_eq!(seat_label("node_admin", true, true), "fabric primary · mesh primary");
    }
}
