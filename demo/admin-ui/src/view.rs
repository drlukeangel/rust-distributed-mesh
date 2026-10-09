//! The read-only views the tabs draw, folded from node-admin's REST answers: the fabric header, the
//! topology with its connection edges, and the Builds.

use crate::chaos::NodeFacts;
use crate::timeline::ConnFact;
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

/// Each node's CPU and RAM load as the mesh digest published it, read from every node-admin's
/// `GET /api/nodes` (a peer mesh's members come without load in this admin's view, so the admin of
/// that mesh is asked for them). `None` for a node no admin reports a load for.
pub async fn loads(nodes: &[Value], http: &reqwest::Client) -> std::collections::HashMap<String, Value> {
    let mut bases: Vec<String> = nodes.iter().filter_map(|n| text(n, "admin_api_base")).collect();
    bases.sort();
    bases.dedup();
    let mut out = std::collections::HashMap::new();
    for base in bases {
        if let Ok(v) = get_json(http, &format!("{}/api/nodes", base.trim_end_matches('/'))).await {
            for n in v["nodes"].as_array().into_iter().flatten() {
                if let (Some(name), false) = (text(n, "name"), n["load"].is_null()) {
                    out.entry(name).or_insert_with(|| n["load"].clone());
                }
            }
        }
    }
    out
}

/// What the topology view reads besides node-admin: the nodes' own connection facts, the network
/// cuts in force, and who is on the backbone (from the nodes' own span records).
pub struct TopoCtx<'a> {
    /// Every node's latest connection fact.
    pub facts: &'a [ConnFact],
    /// The members on the cut side of each cut in force.
    pub cuts: &'a [Vec<String>],
    /// Nodes that joined the backbone.
    pub listeners: &'a [String],
    /// Nodes that publish their mesh's aggregate on the backbone.
    pub publishers: &'a [String],
}

/// The nodes with their seat labels and load, and the connection edges of the whole estate:
/// every node's own facts (from the span records every node writes, see
/// [`crate::timeline::CONNECTION_FACT_SPAN`]) joined with the facts each node-admin holds
/// (`GET /api/connections`), each judged by [`effective_edges`]. `cuts` lists the members on the
/// cut side of every network cut in force.
pub async fn topology(admin: &NodeAdminClient, http: &reqwest::Client, ctx: &TopoCtx<'_>) -> Result<Value, ClientError> {
    let (facts, cuts) = (ctx.facts, ctx.cuts);
    let nodes = nodes(admin).await?;
    let load = loads(&nodes, http).await;
    let view: Vec<Value> = nodes
        .iter()
        .map(|n| {
            let kind = text(n, "kind").unwrap_or_default();
            let (p, fp) = (n["is_primary"].as_bool().unwrap_or(false), n["is_fabric_primary"].as_bool().unwrap_or(false));
            let name = text(n, "name").unwrap_or_default();
            json!({
                "name": n["name"], "kind": kind, "mesh": n["mesh"], "status": n["status"], "seat": seat_label(&kind, p, fp),
                "is_primary": p, "is_fabric_primary": fp, "declared": n["declared"], "incarnation_id": n["incarnation_id"], "node_id": n["node_id"],
                "backbone": if ctx.publishers.contains(&name) { "publisher" } else if ctx.listeners.contains(&name) { "listener" } else { "" },
                "has_runtime": n["data_dir"].is_string(), "load": load.get(&name).cloned().or_else(|| n.get("load").filter(|l| !l.is_null()).cloned()).unwrap_or(Value::Null),
            })
        })
        .collect();
    let mut all: Vec<ConnFact> = facts.to_vec();
    let mut errors = Vec::new();
    let mut seen_bases = std::collections::HashSet::new();
    for n in &nodes {
        let Some(base) = text(n, "admin_api_base") else { continue };
        if !seen_bases.insert(base.clone()) {
            continue;
        }
        match get_json(http, &format!("{}/api/connections", base.trim_end_matches('/'))).await {
            Ok(c) => {
                for e in c["connections"].as_array().into_iter().flatten() {
                    let s = |k: &str| e[k].as_str().unwrap_or_default().to_string();
                    all.push(ConnFact {
                        source: s("source"), destination: s("destination"), kind: s("kind"), state: s("state"), reason: s("reason"), carrier: s("carrier"),
                        source_incarnation: String::new(), destination_incarnation: String::new(), logged_at_ms: e["logged_at_ms"].as_u64().unwrap_or(0),
                    });
                }
            }
            Err(e) => errors.push(e),
        }
    }
    let edges = effective_edges(&all, &view, cuts, ctx.listeners);
    Ok(json!({"nodes": view, "edges": edges, "edge_errors": errors, "fact_sources": {"node_spans": facts.len(), "admin_api": all.len().saturating_sub(facts.len())}}))
}

/// The edges to draw: the CURRENT state of each pair, decided from the facts held, never from the
/// last fact alone. A fact is dropped when it names a process birth that is not the node's current
/// one (it is about a birth that is gone) or a node the fabric no longer holds. Of what is left,
/// per `(source, destination, kind)` the newest fact stands, and then:
///
/// - `connected`: drawn connected, unless an end is not `ready-for-traffic` (a held pool entry of a
///   node that has gone quiet): then `unheard`, basis naming the end and its status.
/// - `failed` (a Direct attempt that did not connect; never proof the destination is dead):
///   `failed` while a network cut in force separates the two ends, or while either end is not
///   `ready-for-traffic`; otherwise it is superseded and drawn `recovered`, basis
///   `later-connected-fact` (the destination or source wrote a later connected fact on this pair)
///   or `both-ends-ready` (both ends are ready for traffic and heard, so the pair is healthy even
///   though neither has dialled again).
/// - `disconnected`: dropped when a later connected fact on the pair exists, else drawn `disconnected`.
///
/// Every pair of backbone listeners in different meshes is drawn: from the facts when either holds
/// one, and otherwise from the backbone membership itself (basis `backbone-membership`), judged by
/// the same cut and readiness rule.
pub fn effective_edges(facts: &[ConnFact], nodes: &[Value], cuts: &[Vec<String>], backbone: &[String]) -> Vec<Value> {
    use std::collections::HashMap;
    let by_name: HashMap<&str, &Value> = nodes.iter().filter_map(|n| n["name"].as_str().map(|s| (s, n))).collect();
    let current = |name: &str, inc: &str| -> bool {
        match by_name.get(name) {
            None => false,
            Some(n) => inc.is_empty() || n["incarnation_id"].as_str().is_none_or(|c| c == inc),
        }
    };
    let mut latest: HashMap<(&str, &str, &str), &ConnFact> = HashMap::new();
    for f in facts.iter().filter(|f| current(&f.source, &f.source_incarnation) && current(&f.destination, &f.destination_incarnation)) {
        let k = (f.source.as_str(), f.destination.as_str(), f.kind.as_str());
        if latest.get(&k).is_none_or(|o| o.logged_at_ms <= f.logged_at_ms) {
            latest.insert(k, f);
        }
    }
    // The newest connected fact on each unordered pair, either direction.
    let mut connected_at: HashMap<(String, String), (u64, String)> = HashMap::new();
    for f in latest.values().filter(|f| f.state == "connected") {
        let key = if f.source <= f.destination { (f.source.clone(), f.destination.clone()) } else { (f.destination.clone(), f.source.clone()) };
        if connected_at.get(&key).is_none_or(|(t, _)| *t < f.logged_at_ms) {
            connected_at.insert(key, (f.logged_at_ms, f.source.clone()));
        }
    }
    let ready = |name: &str| by_name.get(name).is_some_and(|n| n["status"] == "ready-for-traffic");
    let separated = |a: &str, b: &str| cuts.iter().any(|side| side.iter().any(|m| m == a) != side.iter().any(|m| m == b));
    let mut out = Vec::new();
    for f in latest.values() {
        let key = if f.source <= f.destination { (f.source.clone(), f.destination.clone()) } else { (f.destination.clone(), f.source.clone()) };
        let later = connected_at.get(&key).filter(|(t, _)| *t > f.logged_at_ms);
        let (state, basis) = match f.state.as_str() {
            "connected" if !ready(&f.source) || !ready(&f.destination) => {
                let end = if !ready(&f.source) { &f.source } else { &f.destination };
                ("unheard", format!("connected-fact-held-but-{end}-is-{}", by_name[end.as_str()]["status"].as_str().unwrap_or("unknown")))
            }
            "connected" => ("connected", "latest-fact-connected".to_string()),
            "disconnected" => match later {
                Some(_) => continue,
                None => ("disconnected", "latest-fact-disconnected".to_string()),
            },
            _ if separated(&f.source, &f.destination) => ("failed", "a-network-cut-in-force-separates-the-ends".to_string()),
            _ if !ready(&f.source) => ("failed", format!("source-{}", by_name[f.source.as_str()]["status"].as_str().unwrap_or("unknown"))),
            _ if !ready(&f.destination) => ("failed", format!("destination-{}", by_name[f.destination.as_str()]["status"].as_str().unwrap_or("unknown"))),
            _ => match later {
                Some((_, by)) => ("recovered", format!("later-connected-fact-from-{by}")),
                None => ("recovered", "both-ends-ready".to_string()),
            },
        };
        out.push(json!({
            "source": f.source, "destination": f.destination, "kind": f.kind, "state": state, "basis": basis, "last_fact": f.state,
            "carrier": if f.carrier.is_empty() { Value::Null } else { json!(f.carrier) }, "reason": f.reason, "logged_at_ms": f.logged_at_ms,
        }));
    }
    let mesh_of = |n: &str| by_name.get(n).and_then(|v| v["mesh"].as_str()).unwrap_or_default().to_string();
    let listeners: Vec<&String> = backbone.iter().filter(|b| by_name.contains_key(b.as_str())).collect();
    for (i, a) in listeners.iter().enumerate() {
        for b in listeners.iter().skip(i + 1).filter(|b| mesh_of(b) != mesh_of(a)) {
            let has = out.iter().any(|e| e["kind"] == "direct" && ((e["source"] == a.as_str() && e["destination"] == b.as_str()) || (e["source"] == b.as_str() && e["destination"] == a.as_str())));
            if has {
                continue;
            }
            let (state, basis) = if separated(a, b) {
                ("failed", "a-network-cut-in-force-separates-the-ends".to_string())
            } else if !ready(a) || !ready(b) {
                ("failed", "an-end-is-not-ready-for-traffic".to_string())
            } else {
                ("connected", "backbone-membership".to_string())
            };
            out.push(json!({"source": a, "destination": b, "kind": "direct", "state": state, "basis": basis, "last_fact": "none", "carrier": Value::Null, "reason": "", "logged_at_ms": 0}));
        }
    }
    let listener_set: std::collections::HashSet<&str> = listeners.iter().map(|s| s.as_str()).collect();
    for e in out.iter_mut() {
        let (s, d) = (e["source"].as_str().unwrap_or_default().to_string(), e["destination"].as_str().unwrap_or_default().to_string());
        e["cross_mesh"] = json!(mesh_of(&s) != mesh_of(&d));
        e["backbone"] = json!(listener_set.contains(s.as_str()) && listener_set.contains(d.as_str()));
    }
    out.sort_by(|a, b| (a["source"].as_str(), a["destination"].as_str(), a["kind"].as_str()).cmp(&(b["source"].as_str(), b["destination"].as_str(), b["kind"].as_str())));
    out
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

    fn fact(src: &str, dst: &str, state: &str, at: u64) -> ConnFact {
        ConnFact { source: src.into(), destination: dst.into(), kind: "direct".into(), state: state.into(), reason: String::new(), carrier: String::new(), source_incarnation: String::new(), destination_incarnation: String::new(), logged_at_ms: at }
    }
    fn nd(name: &str, status: &str) -> Value {
        json!({"name": name, "status": status, "incarnation_id": format!("inc-{name}")})
    }

    #[test]
    fn a_failed_fact_stays_red_while_an_end_is_not_ready_or_a_cut_separates_the_pair_and_recovers_after() {
        let ready = [nd("a", "ready-for-traffic"), nd("b", "ready-for-traffic")];
        let failed = [fact("a", "b", "failed", 10)];
        assert_eq!(effective_edges(&failed, &ready, &[], &[])[0]["state"], "recovered", "both ends ready and heard");
        assert_eq!(effective_edges(&failed, &ready, &[], &[])[0]["basis"], "both-ends-ready");
        assert_eq!(effective_edges(&failed, &ready, &[vec!["b".into()]], &[])[0]["state"], "failed", "a cut in force separates the ends");
        let pending = [nd("a", "ready-for-traffic"), nd("b", "pending-reconnect")];
        assert_eq!(effective_edges(&failed, &pending, &[], &[])[0]["state"], "failed");
        assert_eq!(effective_edges(&failed, &pending, &[], &[])[0]["basis"], "destination-pending-reconnect");
        let healed = [fact("a", "b", "failed", 10), fact("b", "a", "connected", 20)];
        let e = effective_edges(&healed, &ready, &[], &[]);
        assert_eq!(e.iter().find(|x| x["source"] == "a").unwrap()["basis"], "later-connected-fact-from-b");
    }

    #[test]
    fn a_fact_naming_a_dead_birth_or_a_departed_node_is_not_drawn() {
        let nodes = [nd("a", "ready-for-traffic"), nd("b", "ready-for-traffic")];
        let mut old = fact("a", "b", "connected", 5);
        old.destination_incarnation = "inc-b-old".into();
        assert!(effective_edges(&[old], &nodes, &[], &[]).is_empty());
        assert!(effective_edges(&[fact("a", "gone", "connected", 5)], &nodes, &[], &[]).is_empty());
        let quiet = [nd("a", "ready-for-traffic"), nd("b", "pending-reconnect")];
        assert_eq!(effective_edges(&[fact("a", "b", "connected", 5)], &quiet, &[], &[])[0]["state"], "unheard", "a connected fact of a quiet node is not drawn as healthy");
        let disc = [fact("a", "b", "disconnected", 5), fact("b", "a", "connected", 9)];
        assert_eq!(effective_edges(&disc, &nodes, &[], &[]).len(), 1, "a disconnected fact superseded by a later connected one is dropped");
    }

    #[test]
    fn backbone_listeners_in_two_meshes_are_always_linked_and_the_link_follows_the_cut() {
        let mk = |n: &str, mesh: &str| json!({"name": n, "mesh": mesh, "status": "ready-for-traffic", "incarnation_id": format!("inc-{n}")});
        let nodes = [mk("mesh1.admin.1", "mesh1"), mk("mesh2.admin.1", "mesh2"), mk("mesh1.admin.2", "mesh1")];
        let bb = ["mesh1.admin.1".to_string(), "mesh2.admin.1".to_string(), "mesh1.admin.2".to_string()];
        let e = effective_edges(&[], &nodes, &[], &bb);
        assert_eq!(e.len(), 2, "mesh1.admin.1<->mesh2.admin.1 and mesh1.admin.2<->mesh2.admin.1, never the same-mesh pair: {e:?}");
        assert!(e.iter().all(|x| x["state"] == "connected" && x["basis"] == "backbone-membership" && x["cross_mesh"] == true && x["backbone"] == true));
        let cut = effective_edges(&[], &nodes, &[vec!["mesh2.admin.1".into()]], &bb);
        assert!(cut.iter().all(|x| x["state"] == "failed"));
        let with_fact = effective_edges(&[fact("mesh1.admin.1", "mesh2.admin.1", "connected", 5)], &nodes, &[], &bb);
        assert_eq!(with_fact.iter().filter(|x| x["basis"] == "latest-fact-connected").count(), 1, "a held fact is used, not the inference");
    }
}
