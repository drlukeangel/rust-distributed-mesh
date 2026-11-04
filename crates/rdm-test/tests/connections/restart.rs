//! Connections across a restart, end to end on the process provider (`connections.md` §13 B, C,
//! Q227): the cell kills the source's runtime and its mesh-primary recovers it at the same separate path and name
//! (node-recovery.md); the recovered source holds no connection and reads none back. A release-only
//! stem (`connections_restart`). The map of every connections acceptance test to its cell is
//! `connections/acceptance.rs`.

use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use rdm_core::{NodeId, PathName};
use std::time::Duration;

fn source() -> PathName { PathName::new("mesh1.node", "1").unwrap() }
fn carrier() -> PathName { PathName::new("mesh1.node", "2").unwrap() }
fn destination() -> PathName { PathName::new("mesh1.node", "3").unwrap() }

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "connections".into(), subfeature: "connections-restart".into(), rung: "MN".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    match v {
        Value::String(x) => x.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// As `connections_acceptance`'s estate: one mesh of 2 node-admins and 3 nodes.
async fn born(cell: &str) -> Estate {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let root = estate.bootstrap_data_dir("mesh1").parent().expect("the estate root holds the data dirs").to_path_buf();
    std::fs::write(root.join(rdm_test::node::APP_PROTOCOL_PROXY_KNOB), b"").unwrap();
    estate.resize_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    for (witness, peer) in [(source(), carrier()), (source(), destination()), (carrier(), destination())] {
        estate.witness_holds_birth(&witness, &peer).await;
    }
    estate
}

fn snapshot(estate: &Estate, node: &NodeId) -> Value {
    let out = estate.probe(&["snapshot", "--target", &format!("exact:{node}"), "--key", "x"]);
    assert_eq!(out["outcome"], "Reply", "{node} answers the originate service's snapshot: {out}");
    out
}

fn rows<'a>(history: &'a [Value], to: &NodeId, kind: &str) -> Vec<&'a Value> {
    history.iter().filter(|r| r["destination"]["node_id"] == to.as_str() && r["kind"] == kind).collect()
}

/// Kill `node`'s runtime and wait for its mesh-primary's recovery: a new node, ready, at its separate path and name.
async fn recovered(estate: &Estate, node: &PathName) -> Value {
    let before = estate.node_value(node).await;
    estate.kill_node(node).await;
    estate
        .until_node(node, &format!("{node:?} is recovered as a new node at its separate path and name"), crate::node_recovery_due(), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["state"] == "ready" && n["node_id"] != before["node_id"]).then_some(n)
        })
        .await
}

/// CONTRACT (Q227; connections.md §13 B and C: a restart closes every connection the node held, so
/// the recovered source holds none, no Failed Direct among them, and it reads no connection fact
/// back): the source records a Proxy to the destination after five Direct Failed facts, then its
/// runtime is killed and its mesh-primary recovers it over the inherited
/// data dir. The recovered source holds no Direct and no Proxy, its boot opens its connections store
/// and reads nothing back, and the store keeps the first birth's rows as written. Must NOT happen: the
/// recovered source holding the fifth Failed row, or the Proxy, it read back from the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_source_holds_no_connection_and_reads_none_back() {
    let cell = "a_recovered_source_holds_no_connection_and_reads_none_back";
    let mut estate = born(cell).await;
    let source_id = estate.node_view(&source()).await.node_id;
    let destination_id = estate.node_view(&destination()).await.node_id;
    let carrier_id = estate.node_view(&carrier()).await.node_id;
    let seeded = estate.probe(&["record-proxy", "--target", &format!("exact:{source_id}"), "--destination", &format!("exact:{destination_id}"), "--carrier", &format!("exact:{carrier_id}"), "--failed-attempts", "5", "--key", "x"]);
    assert_eq!(seeded["proxy_recorded"], true, "{seeded}");
    let control = serde_json::json!(rdm_core::connections::ALPN);
    let failed: Vec<Value> = rows(&estate.connection_history(&source_id), &destination_id, "Direct").into_iter().filter(|r| r["state"] == "Failed" && r["alpn"] == control).cloned().collect();
    assert_eq!(failed.len(), 5, "{failed:?}");
    let fifth = failed[4].clone();

    let after = recovered(&estate, &source()).await;
    let recovered_id: NodeId = serde_json::from_value(after["node_id"].clone()).expect("recovered ID");
    let held = snapshot(&estate, &recovered_id);
    assert_eq!((&held["by"], held["by_node_id"].as_str()), (&serde_json::json!(source()), Some(recovered_id.as_str())), "the recovered node answers: {held}");
    assert!(held["own_latest_directs"].as_array().expect("snapshot includes Direct array").is_empty(), "the recovered source holds no Direct: {held}");
    assert!(held["own_active_proxies"].as_array().expect("snapshot includes Proxy array").is_empty(), "nor any Proxy: {held}");
    assert!(estate.connection_history(&source_id).iter().any(|r| r["id"] == fifth["id"]), "the store keeps the first birth's rows as written");

    estate.stop().await;
    let spans = estate.spans();
    let boot = named(&spans, "rdm.mesh.node.create.via-boot").into_iter().find(|sp| sp["attributes"]["node_id"] == after["node_id"] && sp["attributes"]["incarnation_id"] == after["incarnation"]).expect("recovered exact birth boot");
    let opened = named(&spans, "rdm.mesh.node.create.via-connections-opened").into_iter().filter(|sp| sp["attributes"]["node_path"] == source().path && sp["attributes"]["node_name"] == source().name && sp["parent_span_id"] == boot["span_id"] && sp["trace_id"] == boot["trace_id"]).last().cloned().expect("the recovered source opened its connections store");
    assert!(named(&spans, "rdm.node_admin.connection.update.via-hydrate").is_empty(), "no node reads its connections back");
    assert_ne!(after["node_id"], serde_json::Value::Null, "{after}");
    estate.record_trace_url(&s(&opened["trace_id"]));
}
