//! Connections across a restart, end to end on the process provider (`connections.md` §13 B, C,
//! Q227): the cell kills the source's runtime and its mesh-primary recovers it at the same path.name
//! (node-recovery.md); the recovered source holds no connection and reads none back. A release-only
//! stem (`connections_restart`). The map of every connections acceptance test to its cell is
//! `connections/acceptance.rs`.

use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

const SOURCE: &str = "mesh1.rpc.1";
const CARRIER: &str = "mesh1.rpc.2";
const DEST: &str = "mesh1.rpc.3";

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

/// As `connections_acceptance`'s estate: one mesh of 2 node-admins and 3 rpc nodes.
async fn born(cell: &str) -> Estate {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let root = estate.bootstrap_data_dir("mesh1").parent().expect("the estate root holds the data dirs").to_path_buf();
    std::fs::write(root.join(rdm_test::node::APP_PROTOCOL_PROXY_KNOB), b"").unwrap();
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settled_views(&[("mesh1", 2, 3)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    for (witness, peer) in [(SOURCE, CARRIER), (SOURCE, DEST), (CARRIER, DEST)] {
        estate.witness_holds_birth(witness, peer).await;
    }
    estate
}

fn snapshot(estate: &Estate, node: &str) -> Value {
    let out = estate.probe(&["snapshot", "--target", &format!("path:{node}"), "--key", "x"]);
    assert_eq!(out["outcome"], "Reply", "{node} answers the snapshot door: {out}");
    out
}

fn rows<'a>(history: &'a [Value], to: &str, kind: &str) -> Vec<&'a Value> {
    history.iter().filter(|r| r["destination"]["name"] == to && r["kind"] == kind).collect()
}

/// Kill `node`'s runtime and wait for its mesh-primary's recovery: a new node, ready, at its path.name.
async fn recovered(estate: &Estate, node: &str) -> Value {
    let before = estate.node_value(node).await;
    estate.kill_node(node).await;
    estate
        .until_node(node, &format!("{node} is recovered as a new node at its path.name"), crate::node_recovery_due(), |n| {
            let n = serde_json::to_value(n?).expect("a view serializes");
            (n["status"] == "ready" && n["node_id"] != before["node_id"]).then_some(n)
        })
        .await
}

/// CONTRACT (Q227; connections.md §13 B and C: a restart closes every connection the node held, so
/// the recovered source holds none, its reconnect backoff starts fresh, and it reads no connection
/// fact back): the source records a Proxy to the destination after five Direct Failed facts (one
/// round, ordinals 1-5), then its runtime is killed and its mesh-primary recovers it over the inherited
/// data dir. The recovered source holds no Direct and no Proxy, its boot opens its connections store
/// and reads nothing back, and the store keeps the first birth's rows as written. Must NOT happen: the
/// recovered source holding the fifth Failed row, or the Proxy, it read back from the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_source_holds_no_connection_and_reads_none_back() {
    let cell = "a_recovered_source_holds_no_connection_and_reads_none_back";
    let mut estate = born(cell).await;
    let seeded = estate.probe(&["record-proxy", "--target", &format!("path:{SOURCE}"), "--destination", &format!("path:{DEST}"), "--carrier", &format!("path:{CARRIER}"), "--failed-attempts", "5", "--key", "x"]);
    assert_eq!(seeded["proxy_recorded"], true, "{seeded}");
    let control = serde_json::json!(rdm_core::connections::ALPN);
    let fifth = rows(&estate.connection_history(SOURCE), DEST, "Direct").into_iter().rev().find(|r| r["state"] == "Failed" && r["alpn"] == control).cloned().expect("the fifth Direct Failed");
    assert_eq!(fifth["recovery"]["attempt_ordinal"], 5, "{fifth}");

    let after = recovered(&estate, SOURCE).await;
    let held = snapshot(&estate, SOURCE);
    assert!(held["own_latest_directs"].as_array().is_none_or(|d| d.is_empty()), "the recovered source holds no Direct: {held}");
    assert!(held["own_active_proxies"].as_array().is_none_or(|p| p.is_empty()), "nor any Proxy: {held}");
    assert!(estate.connection_history(SOURCE).iter().any(|r| r["id"] == fifth["id"]), "the store keeps the first birth's rows as written");

    estate.stop().await;
    let spans = estate.spans();
    let opened = named(&spans, "rdm.mesh.node.create.via-connections-opened").into_iter().filter(|sp| sp["attributes"]["node"] == SOURCE).last().cloned().expect("the recovered source opened its connections store");
    assert!(named(&spans, "rdm.node_admin.connection.update.via-hydrate").is_empty(), "no node reads its connections back");
    assert_ne!(after["node_id"], serde_json::Value::Null, "{after}");
    estate.record_trace_url(&s(&opened["trace_id"]));
}
