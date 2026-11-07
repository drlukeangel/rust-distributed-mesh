//! The read-only views the tabs draw, folded from node-admin's typed answers over Node RPC: the fabric
//! header, the topology with its connections, and the Builds.

use crate::chaos::NodeFacts;
use crate::admin::Admin;
use rdm_client::{BuildId, CallEnd, NodeView};
use serde_json::{json, Value};

fn text(v: &Value, k: &str) -> Option<String> {
    v.get(k).and_then(Value::as_str).map(str::to_string)
}

/// The primary role a node holds, as the tabs label it. "mesh primary" is the node-admin node-type primary
/// only; the fabric-primary is that node-type primary of its own mesh as well.
pub fn primary_label(r#type: &str, is_primary: bool, is_fabric_primary: bool) -> &'static str {
    match (r#type, is_primary, is_fabric_primary) {
        (_, _, true) => "fabric primary · mesh primary",
        ("node-admin", true, false) => "mesh primary",
        _ => "",
    }
}

/// mesh.nodes.get of every mesh the entry admin's view holds, as JSON objects (the client's `NodeView` serialized).
pub async fn nodes(admin: &Admin) -> Result<Vec<Value>, CallEnd> {
    Ok(admin.nodes().await?.iter().map(|n| serde_json::to_value(n).unwrap_or(Value::Null)).collect())
}

/// What a fault needs of each node, from its record as `node.get` answers it: its mesh is its path's
/// first location (Q366) and its data dir is its storage's.
pub fn facts(nodes: &[NodeView]) -> Vec<NodeFacts> {
    nodes
        .iter()
        .map(|n| NodeFacts {
            name: n.location(),
            mesh: n.mesh(),
            is_fabric_primary: n.is_fabric_primary,
            data_dir: n.storage.data_dir.clone(),
            transport_port: n.transport_addr.map(|a| a.port()),
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
            Ok(id) => admin.build(&id).await.ok().map(|b| brief(&build_view(&b))),
            Err(_) => None,
        },
        None => None,
    };
    // `fabric.get` names the fabric-primary by its node id; the header shows its path and name.
    let fabric_primary = nodes.iter().find(|n| !fabric["fabric_primary"].is_null() && n["node_id"] == fabric["fabric_primary"]).map_or(Value::Null, |n| json!({"path": n["path"], "name": n["name"]}));
    json!({
        "fabric": {"name": fabric["name"], "status": fabric["status"], "provider": fabric["provider"], "build_id": fabric["build_id"], "fabric_primary": fabric_primary, "fabric_primary_id": fabric["fabric_primary"]},
        "meshes": meshes,
        "nodes_total": nodes.len(),
        "build": build,
        "error": null,
    })
}

/// A Build as the tabs draw it, from what `build.get` carries (build.md §5): its state, its record's
/// change and acceptance time (ms since the Unix epoch), its topology's node count, its events in
/// event.id order and its end. `sections` are the meshes the fabric-primary dispatched a section to;
/// `steps` are its `rdm.build.step` events; `ended` is the `state` of its last `rdm.build.ended`
/// (`complete`, `incomplete { sections }` or `failed { reason, code }`, Q346).
fn build_view(b: &rdm_client::Build) -> Value {
    let of = |t: &'static str| events_of(b, t);
    let steps: Vec<Value> = of("rdm.build.step").map(|e| json!({ "mesh": e["data"]["mesh"], "op": e["data"]["op"], "step": e["data"]["step"], "state": e["data"]["state"], "subject": e["subject"], "time": e["time"] })).collect();
    let sections: Vec<Value> = of("rdm.build.dispatched").map(|e| e["data"]["mesh"].clone()).collect();
    json!({
        "build_id": b.build_id.to_string(),
        "state": format!("{:?}", b.phase).to_lowercase(),
        "current": b.current,
        "nodes": b.nodes.len(),
        "change": serde_json::to_value(&b.change).unwrap_or(Value::Null),
        "time": b.time,
        "runs": of("rdm.build.started").count(),
        "sections": sections,
        "steps": steps,
        "ended": of("rdm.build.ended").last().map_or(Value::Null, |e| e["data"]["state"].clone()),
    })
}

/// The events of `b` of type `t`, in event.id order.
fn events_of<'a>(b: &'a rdm_client::Build, t: &'static str) -> impl Iterator<Item = &'a Value> + 'a {
    b.events.iter().filter(move |e| e["type"] == t)
}

/// The header's line of the current Build: its id, state and end.
fn brief(v: &Value) -> Value {
    json!({ "build_id": v["build_id"], "state": v["state"], "change": v["change"], "time": v["time"], "ended": v["ended"] })
}

/// The nodes with their role labels, and every connection fact each node answers for its own exact
/// birth (node.connection.get, served by every node): the data plane's Direct and Proxy rows live on
/// the role nodes that hold them, not only on the node-admins.
pub async fn topology(admin: &Admin) -> Result<Value, CallEnd> {
    let views = admin.nodes().await?;
    let view: Vec<Value> = views
        .iter()
        .map(|v| {
            let n = serde_json::to_value(v).unwrap_or(Value::Null);
            let r#type = text(&n, "type").unwrap_or_default();
            let (p, fp) = (n["is_primary"].as_bool().unwrap_or(false), n["is_fabric_primary"].as_bool().unwrap_or(false));
            // A node's mesh is its path's first location (Q366); the rest under the record's names.
            json!({
                "path": n["path"], "name": n["name"], "type": r#type, "mesh": v.mesh(), "state": n["state"], "role": primary_label(&r#type, p, fp),
                "is_primary": p, "is_fabric_primary": fp, "incarnation": n["incarnation"], "node_id": n["node_id"],
                "has_runtime": v.storage.data_dir.is_some(),
            })
        })
        .collect();
    let mut connections: Vec<Value> = Vec::new();
    let mut errors = Vec::new();
    for n in views.iter() {
        match admin.connections(n).await {
            Some(Ok(c)) => {
                let by = json!(n.node_id);
                for e in serde_json::to_value(&c).unwrap_or(Value::Null)["connections"].as_array().into_iter().flatten() {
                    let mut e = e.clone();
                    e["reported_by"] = by.clone();
                    // One fact per (source, destination, kind): the latest any admin reports.
                    let same = |x: &Value| x["source"] == e["source"] && x["destination"] == e["destination"] && x["kind"] == e["kind"];
                    match connections.iter().position(|x| same(x)) {
                        Some(i) if connections[i]["logged_at_ms"].as_u64() >= e["logged_at_ms"].as_u64() => {}
                        Some(i) => connections[i] = e,
                        None => connections.push(e),
                    }
                }
            }
            Some(Err(e)) => errors.push(format!("path {:?}, name {:?}: {e}", n.path, n.name)),
            None => {}
        }
    }
    Ok(json!({"nodes": view, "connections": connections, "connection_errors": errors}))
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
            out.push(build_view(&b));
        }
    }
    json!({"current": current, "builds": out})
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_node_admins_node_type_primary_is_labelled_mesh_primary() {
        assert_eq!(primary_label("broker", true, false), "", "a broker first of its node type is not a mesh primary");
        assert_eq!(primary_label("node-admin", true, false), "mesh primary");
        assert_eq!(primary_label("node-admin", false, false), "");
        assert_eq!(primary_label("node-admin", true, true), "fabric primary · mesh primary");
    }
}
