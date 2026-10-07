//! i143.e11.s9 process E2E: observability pass-through on the role shape (PRD §13.1).
//!
//! `{node_admin: 1, broker: 1, gateway: 1}`. One probe call carried by the gateway to the broker:
//! one trace, three processes. The probe's call span is the root; the carrier hands the original
//! context through verbatim, so the gateway's carried inner invocation and the broker's serve span
//! both descend from the probe's call, and the broker's serve carries the originating
//! `caller_system` (`rdm`) unchanged.
//! The malformed-value arm (a bad traceparent is dropped on a `via-context-dropped` span and the
//! outcome is unchanged) is `rafka-node-base/tests/role_process.rs` on the gateway's own client.

use rafka_test_scenario::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-rpc".into(),
        subfeature: "role-context".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "one_call_through_a_gateway_to_a_broker_is_one_trace_across_three_processes".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_call_through_a_gateway_to_a_broker_is_one_trace_across_three_processes() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "broker": 1, "gateway": 1}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let want = ["mesh1.admin.1", "mesh1.broker.1", "mesh1.gateway.1"].iter().map(|n| n.to_string()).collect();
    estate.settled(&want, Duration::from_secs(30)).await;
    let broker = estate.node("mesh1.broker.1").await;
    let broker_id = broker["node_id"].as_str().unwrap().to_string();
    let exact = format!("exact:{broker_id}");

    let put = estate.probe(&["put", "--target", &exact, "--via", "path:mesh1.gateway.1", "--key", "t1", "--value", "traced"]);
    assert_eq!((put["outcome"].as_str(), put["route"].as_str()), (Some("Reply"), Some("via-peer")), "{put}");
    assert_eq!(put["reply"]["executing_node"], broker_id, "{put}");
    estate.stop().await;

    let spans = estate.spans();
    let root = named(&spans, "rdm.node_rpc.proof_store.resolve.via-probe").into_iter().find(|s| s["attributes"]["op"] == "put").cloned().expect("the probe's call span");
    let carried = named(&spans, "rdm.node_rpc.request.serve.via-carried-inner").into_iter().find(|s| s["attributes"]["target"] == broker_id.as_str()).cloned().expect("the gateway's carried inner invocation");
    let served = named(&spans, "rdm.node_rpc.proof_store.serve.via-request").into_iter().find(|s| s["attributes"]["incarnation_id"] == broker["incarnation_id"]).cloned().expect("the broker's serve span");
    let op_is = |v: &Value, op: u64| v.as_u64() == Some(op) || v.as_str() == Some(&op.to_string());
    let broker_direct: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.request.serve.via-direct" && s["service"] == "rafka-broker" && s["trace_id"] == root["trace_id"] && op_is(&s["attributes"]["op"], 0x70)).collect();
    assert_eq!(broker_direct.len(), 1, "the broker served the one carried request in the probe's trace: {broker_direct:?}");
    assert_eq!(broker_direct[0]["attributes"]["caller_system"], "rdm", "the originating system rode through the carrier unchanged: {}", broker_direct[0]);
    assert_eq!(carried["trace_id"], root["trace_id"], "one trace");
    assert_eq!(served["trace_id"], root["trace_id"], "one trace");
    assert!(descends_from(&spans, &carried, &root), "the gateway's inner invocation descends from the probe's call");
    assert!(descends_from(&spans, broker_direct[0], &root), "the broker's serve descends from the probe's call: the carrier handed the original context through");
    assert!(descends_from(&spans, &served, broker_direct[0]), "the proof store served under the broker's serve");
    estate.artifact("trace.json", &json!({"trace_id": root["trace_id"], "root": root, "carried": carried, "served": served}));
    estate.record_trace_url(root["trace_id"].as_str().unwrap_or(""));
}
