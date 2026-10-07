//! i143.e4.s15 acceptance (rafka-v2 #2941, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2941-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose commands set `RAFKA_ARTIFACTS_DIR` (the
//! estate's own manifest, rpc ledger and every process's spans land under it, feature
//! `i143-2941`, test the cell's name) at the test cadence (staleness 3 s, gossip 500 ms).
//!
//! The estate: one mesh, one node-admin, two rpc nodes. One rpc node is restarted through Build
//! by the admin that launched it; the reborn birth's own entry pull and its mesh-channel join are
//! read from its spans, against the admin's serve span, by the birth's own records.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
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
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let nodes = estate.settled_shape(&[("mesh1", 1, 2)], Duration::from_secs(30)).await;
    let admin = nodes.iter().find(|n| n["name"] == "mesh1.admin.1").cloned().expect("the launching admin");
    let before = nodes.iter().find(|n| n["name"] == NODE).cloned().expect("the member to restart");
    let restart_at = now_nano();
    let (status, r) = estate.post(&format!("/api/nodes/{NODE}/restart"), &Value::Null).await;
    assert_eq!(status, 202, "{r}");
    estate.await_build(r["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let after = wait_for("the restarted birth is ready under a new incarnation", Duration::from_secs(30), || async {
        let n = estate.node_opt(NODE).await?;
        (n["status"] == "ready-for-traffic" && n["incarnation_id"] != before["incarnation_id"]).then_some(n)
    })
    .await;
    assert_eq!(after["node_id"], before["node_id"], "a restart keeps the NodeId");
    estate.stop().await;
    let spans = estate.spans();
    (admin, before, after, restart_at, spans)
}

/// The reborn birth's entry pull is answered within one staleness window by the admin its launch
/// names, and the pull that answers is the one that admin logs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_member_pulls_current_admin_within_staleness_window() {
    let cell = "restarted_member_pulls_current_admin_within_staleness_window";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let (admin, before, after, restart_at, spans) = restart_one_member(cell).await;
    let staleness_ms: u64 = std::env::var("RAFKA_STALENESS_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(3000);

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
    let failed: Vec<&Value> = named(&spans, "rdm.mesh.entry.reject.via-membership-pull-failed").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).collect();
    assert!(failed.is_empty(), "no pull of the reborn birth failed: {failed:?}");
    let pulled: Vec<&Value> = named(&spans, "rdm.mesh.entry.update.via-membership-pulled").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).collect();
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

/// The reborn birth's mesh channel reports a neighbour within one gossip interval of its join,
/// then its entry completes; no pull of that birth fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarted_member_joins_mesh_channel_within_gossip_interval() {
    let cell = "restarted_member_joins_mesh_channel_within_gossip_interval";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let (admin, before, after, restart_at, spans) = restart_one_member(cell).await;
    let gossip_ms: u64 = std::env::var("RAFKA_GOSSIP_INTERVAL_MS").ok().and_then(|v| v.parse().ok()).unwrap_or(500);

    let joined = named(&spans, "rdm.mesh.membership.update.via-subscribe")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == NODE && sp["attributes"]["channel"] == "mesh:mesh1" && at(sp) > restart_at)
        .min_by_key(|sp| at(sp))
        .cloned()
        .expect("the reborn birth joined its mesh channel");
    let up = named(&spans, "rdm.mesh.connection.update.via-neighbour-up")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == NODE && sp["attributes"]["channel"] == "mesh:mesh1" && at(sp) >= at(&joined))
        .min_by_key(|sp| at(sp))
        .cloned()
        .expect("the reborn birth's mesh channel found a neighbour");
    let join_to_neighbour_ms = (at(&up) - at(&joined)) / 1_000_000;
    assert!(join_to_neighbour_ms <= gossip_ms, "a neighbour within one gossip interval of the join ({join_to_neighbour_ms} ms of {gossip_ms})");
    let pulled: Vec<&Value> = named(&spans, "rdm.mesh.entry.update.via-membership-pulled").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).collect();
    assert_eq!(pulled.len(), 1, "the entry completed once after the restart: {pulled:?}");
    let failed: Vec<&Value> = named(&spans, "rdm.mesh.entry.reject.via-membership-pull-failed").into_iter().filter(|sp| sp["attributes"]["node"] == NODE && at(sp) > restart_at).collect();
    assert!(failed.is_empty(), "no pull of the reborn birth failed: {failed:?}");

    let result = json!({
        "cell": cell,
        "launching_admin": { "name": admin["name"], "node_id": admin["node_id"], "endpoint_id": admin["endpoint_id"] },
        "node": { "name": NODE, "node_id": before["node_id"], "old_incarnation_id": before["incarnation_id"], "new_incarnation_id": after["incarnation_id"] },
        "restart_at_unix_nano": restart_at,
        "gossip_interval_ms": gossip_ms,
        "join": { "start_unix_nano": at(&joined), "peers": joined["attributes"]["peers"], "trace_id": joined["trace_id"], "span_id": joined["span_id"] },
        "first_neighbour": { "start_unix_nano": at(&up), "peer": up["attributes"]["peer"], "neighbours": up["attributes"]["neighbours"], "trace_id": up["trace_id"], "span_id": up["span_id"] },
        "join_to_neighbour_ms": join_to_neighbour_ms,
        "pulled": pulled.iter().map(|sp| json!({ "start_unix_nano": at(sp), "served_by": sp["attributes"]["served_by"], "attempt": sp["attributes"]["attempt"] })).collect::<Vec<_>>(),
        "failed_pulls": failed.len(),
        "spans_read": spans.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
