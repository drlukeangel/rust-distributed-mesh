//! Connections across a restart, end to end on the process provider (`connections.md` §13 C): the
//! cell kills the source's runtime, its mesh-primary recovers it at the same path.name
//! (node-recovery.md), and the cell waits out the persisted reconnect backoff (64 s at the fifth
//! attempt), so it is a release-only stem (`connections_restart`). The map of every connections
//! acceptance test to its cell is `connections/acceptance.rs`.

use rdm_test::estate::{named, wait_for, Estate, Owner};
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

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

/// As `connections_acceptance`'s estate: one mesh of 2 node-admins and 3 rpc nodes.
async fn born(cell: &str) -> Estate {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let root = estate.bootstrap_data_dir("mesh1").parent().expect("the estate root holds the data dirs").to_path_buf();
    std::fs::write(root.join(rdm_test::node::APP_PROTOCOL_FORWARDABLE_KNOB), b"").unwrap();
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
            (n["status"] == "ready-for-traffic" && n["node_id"] != before["node_id"]).then_some(n)
        })
        .await
}

/// CONTRACT (connections.md §13 C: "Write `Direct Failed(E1, attempt=N, event_time=T)` such that
/// `next_due` is in the future, restart the source, and assert no attempt occurs before the
/// reconstructed due time. Forbidden: restart at attempt 1"; §9.3: "A process restart therefore does not
/// reset exponential backoff ... A restart before `next_due` must not trigger an early attempt";
/// §15.14-15): the source writes five Direct Failed facts toward the destination through its own
/// writer (one round, ordinals 1-5; the fifth's backoff is 64 s and its jitter under 16 s), then its
/// runtime is killed and its mesh-primary recovers it over the inherited data dir. The recovered
/// source hydrates the fifth Failed as its latest Direct toward the destination, and its reconnect
/// dial toward it starts no earlier than the due time the fifth row sets. Must NOT happen: a dial
/// before that due time, or a hydrated latest Direct at ordinal 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recovered_source_dials_no_earlier_than_its_failed_rows_due_time() {
    let cell = "a_recovered_source_dials_no_earlier_than_its_failed_rows_due_time";
    let mut estate = born(cell).await;
    let seeded = estate.probe(&["record-proxy", "--target", &format!("path:{SOURCE}"), "--destination", &format!("path:{DEST}"), "--carrier", &format!("path:{CARRIER}"), "--failed-attempts", "5", "--key", "x"]);
    assert_eq!(seeded["proxy_recorded"], true, "{seeded}");
    let control = serde_json::json!(rdm_core::connections::ALPN);
    let fifth = rows(&estate.connection_history(SOURCE), DEST, "Direct").into_iter().rev().find(|r| r["state"] == "Failed" && r["alpn"] == control).cloned().expect("the fifth Direct Failed");
    assert_eq!(fifth["recovery"]["attempt_ordinal"], 5, "{fifth}");
    let row: rdm_core::connections::NodeConnection = serde_json::from_value(fifth.clone()).expect("a connections row decodes");
    let due_ms = rdm_core::reconnect::next_due(&row).expect("a Failed row carries its due time");

    let after = recovered(&estate, SOURCE).await;
    let recovered_at = now_ms();
    assert!(recovered_at < due_ms, "the cell restarts the source before the due time ({recovered_at} < {due_ms}): the backoff under test is still running");
    let hydrated = snapshot(&estate, SOURCE);
    let latest: Vec<&Value> = hydrated["own_latest_directs"].as_array().into_iter().flatten().filter(|d| d["destination"]["name"] == DEST && d["alpn"] == control).collect();
    assert_eq!(latest.len(), 1, "{hydrated}");
    assert_eq!((s(&latest[0]["state"]), latest[0]["recovery"]["attempt_ordinal"].as_u64()), ("Failed".to_string(), Some(5)), "the series continues from its persisted ordinal, never reset to 1: {hydrated}");

    // The recovered source's own dial toward the destination, no earlier than the due time.
    let dial = wait_for("the recovered source's reconnect dial toward the destination", Duration::from_millis(due_ms.saturating_sub(now_ms()) + 30_000), || async {
        let spans = estate.spans();
        named(&spans, "rdm.node_admin.connection.create.via-recovery-dial").into_iter().find(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST).cloned()
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();
    let dials: Vec<&Value> = named(&spans, "rdm.node_admin.connection.create.via-recovery-dial").into_iter().filter(|sp| sp["attributes"]["source"] == SOURCE && sp["attributes"]["destination"] == DEST).collect();
    assert!(!dials.is_empty(), "{dial}");
    assert!(dials.iter().all(|sp| start(sp) >= due_ms * 1_000_000), "no reconnect dial toward the destination before the due time {due_ms} ms: {dials:#?}");
    assert_eq!(s(&dials[0]["attributes"]["destination_incarnation"]), s(&row.destination.incarnation.as_ref().map(ToString::to_string).map(Value::String).unwrap_or_default()), "the dial is the persisted series' own, toward the birth its Failed row names");
    let _ = after;
    estate.record_trace_url(&s(&dials[0]["trace_id"]));
}
