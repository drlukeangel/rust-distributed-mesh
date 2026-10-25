//! i143.e4.s15 acceptance (rafka-v2 #2941, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2941-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose commands set `RDM_ARTIFACTS_DIR` (the
//! estate's own manifest, rpc ledger and every process's spans land under it, feature
//! `i143-2941`, test the cell's name) at the test cadence (staleness 3 s, gossip 500 ms).
//!
//! The estate: one mesh, one node-admin, two rpc nodes. One rpc node is restarted through Build
//! by the admin that launched it; the reborn birth's own entry pull and its mesh-channel join are
//! read from its spans, against the admin's serve span, by the birth's own records.

use rdm_client::{NodeStatus, NodeView};
use rdm_test::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

const NODE: &str = "mesh1.rpc.1";

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2941".into(),
        subfeature: "entry-pull".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2941/process").join(cell),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn now_nano() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

/// The shared flow: bring the estate up, restart `NODE` through Build, wait for its new birth to
/// be ready, stop, and hand back (the launching admin's node row, the old row, the new row, the
/// restart's wall time, every span).
async fn restart_one_member(test: &str) -> (Value, Value, Value, u64, Vec<Value>) {
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 1, 2)]).await;
    let nodes = estate.settled_views(&[("mesh1", 1, 2)]).await;
    // The initial fabric refuses a node restart until it has first reached ready-for-traffic (states.md "The initial fabric before ready-for-traffic").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let admin = nodes.iter().find(|n| n.name.to_string() == "mesh1.admin.1").cloned().expect("the launching admin");
    let before = nodes.iter().find(|n| n.name.to_string() == NODE).cloned().expect("the member to restart");
    let restart_at = now_nano();
    let mut restart = estate.nodes_obj().await.restart(&NODE.parse().unwrap()).await.expect("the fabric-primary accepts the restart");
    Estate::complete(&mut restart).await;
    let after = estate.node_view(NODE).await;
    assert_eq!(after.status, NodeStatus::ReadyForTraffic, "the restarted birth is ready: {after:?}");
    assert_eq!((&after.node_id, &after.incarnation_id), (&before.node_id, &before.incarnation_id), "a restart is stop then start on the stopped process: it keeps the NodeId and the incarnation");
    estate.stop().await;
    let spans = estate.spans();
    let json = |n: &NodeView| serde_json::to_value(n).expect("a view serializes");
    (json(&admin), json(&before), json(&after), restart_at, spans)
}

/// The reborn birth's entry pull is answered within one staleness window by the admin its launch
/// names, and the pull that answers is the one that admin logs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_member_pulls_current_admin_within_staleness_window() {
    let cell = "restarted_member_pulls_current_admin_within_staleness_window";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let (admin, before, after, restart_at, spans) = restart_one_member(cell).await;
    let staleness_ms: u64 = std::env::var("RDM_STALENESS_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(3000);

    // The reborn birth's pull attempts, in order: the first is answered, by the launching admin,
    // within the staleness window; nothing failed.
    let mut attempts: Vec<&Value> = named(&spans, "rdm.mesh.entry.update.via-pull-attempt").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).collect();
    attempts.sort_by_key(|sp| at(sp));
    assert!(!attempts.is_empty(), "the reborn birth pulled its entry");
    let first = attempts[0];
    let elapsed: u64 = first["attributes"]["elapsed_ms"].as_str().and_then(|v| v.parse().ok()).unwrap_or(u64::MAX);
    assert_eq!(first["attributes"]["step"], "answered", "the first attempt was answered: {first}");
    assert!(elapsed <= staleness_ms, "the first attempt was answered within the staleness window ({elapsed} ms of {staleness_ms})");
    let admin_endpoint_short: String = s(&admin["endpoint_id"]).chars().take(10).collect();
    assert_eq!(s(&first["attributes"]["anchor"]), admin_endpoint_short, "the pull went to the admin that launched the birth");
    let failed: Vec<&Value> = named(&spans, "rdm.mesh.entry.reject.via-topology-pull-failed").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).collect();
    assert!(failed.is_empty(), "no pull of the reborn birth failed: {failed:?}");
    let pulled: Vec<&Value> = named(&spans, "rdm.mesh.entry.update.via-topology-pulled").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).collect();
    assert_eq!(pulled.len(), 1, "one completed pull after the restart: {pulled:?}");
    assert_eq!(pulled[0]["attributes"]["served_by"], "mesh1.admin.1");
    // The admin logs the request it answered, after the restart, for this node.
    let served: Vec<&Value> = named(&spans, "rdm.mesh.entry.serve.via-pull").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && sp["attributes"]["served_by"] == "mesh1.admin.1" && at(sp) > restart_at).collect();
    assert!(!served.is_empty(), "the launching admin served the reborn birth's pull");

    let result = json!({
        "cell": cell,
        "launching_admin": { "name": admin["name"], "node_id": admin["node_id"], "endpoint_id": admin["endpoint_id"], "transport_addr": admin["transport_addr"], "incarnation_id": admin["incarnation_id"] },
        "node": { "name": NODE, "node_id": before["node_id"], "old_incarnation_id": before["incarnation_id"], "new_incarnation_id": after["incarnation_id"], "transport_addr": after["transport_addr"] },
        "restart_at_unix_nano": restart_at,
        "staleness_ms": staleness_ms,
        "pull_attempts": attempts.iter().map(|sp| json!({ "start_unix_nano": at(sp), "attempt": sp["attributes"]["attempt"], "anchor": sp["attributes"]["anchor"], "step": sp["attributes"]["step"], "elapsed_ms": sp["attributes"]["elapsed_ms"], "outcome": sp["attributes"]["outcome"], "trace_id": sp["trace_id"], "span_id": sp["span_id"] })).collect::<Vec<_>>(),
        "served": served.iter().map(|sp| json!({ "start_unix_nano": at(sp), "served_by": sp["attributes"]["served_by"], "members": sp["attributes"]["members"], "trace_id": sp["trace_id"], "span_id": sp["span_id"] })).collect::<Vec<_>>(),
        "pulled": pulled.iter().map(|sp| json!({ "start_unix_nano": at(sp), "served_by": sp["attributes"]["served_by"], "attempt": sp["attributes"]["attempt"] })).collect::<Vec<_>>(),
        "failed_pulls": failed.len(),
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// The reborn birth resubscribes its mesh channel and its entry pull completes once; no pull of that
/// birth fails. When gossip brings it a neighbour is gossip's: the cell asserts no time for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_member_resubscribes_its_mesh_channel_and_its_entry_completes_once() {
    let cell = "restarted_member_resubscribes_its_mesh_channel_and_its_entry_completes_once";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let (admin, before, after, restart_at, spans) = restart_one_member(cell).await;

    // A restarted node rejoins on the process it stopped on: its entry pull completes (`topology_view-joined`)
    // and its mesh channel is resubscribed at once after (the resubscribe span names no node: the first
    // one on the channel after the rejoin is the node's own).
    let rejoin = named(&spans, "rdm.mesh.node.update.via-topology-joined").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).min_by_key(|sp| at(sp)).cloned().expect("the restarted node rejoined its mesh");
    let joined = named(&spans, "rdm.mesh.topology.update.via-resubscribe")
        .into_iter()
        .filter(|sp| sp["attributes"]["channel"] == "mesh:mesh1" && at(sp) >= at(&rejoin))
        .min_by_key(|sp| at(sp))
        .cloned()
        .expect("the restarted node resubscribed its mesh channel");
    let pulled: Vec<&Value> = named(&spans, "rdm.mesh.entry.update.via-topology-pulled").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).collect();
    assert_eq!(pulled.len(), 1, "the entry completed once after the restart: {pulled:?}");
    let failed: Vec<&Value> = named(&spans, "rdm.mesh.entry.reject.via-topology-pull-failed").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).collect();
    assert!(failed.is_empty(), "no pull of the reborn birth failed: {failed:?}");

    let result = json!({
        "cell": cell,
        "launching_admin": { "name": admin["name"], "node_id": admin["node_id"], "endpoint_id": admin["endpoint_id"] },
        "node": { "name": NODE, "node_id": before["node_id"], "old_incarnation_id": before["incarnation_id"], "new_incarnation_id": after["incarnation_id"] },
        "restart_at_unix_nano": restart_at,
        "join": { "start_unix_nano": at(&joined), "peers": joined["attributes"]["peers"], "trace_id": joined["trace_id"], "span_id": joined["span_id"] },
        "pulled": pulled.iter().map(|sp| json!({ "start_unix_nano": at(sp), "served_by": sp["attributes"]["served_by"], "attempt": sp["attributes"]["attempt"] })).collect::<Vec<_>>(),
        "failed_pulls": failed.len(),
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
