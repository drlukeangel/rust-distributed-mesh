//! i143.e11.s5 process E2E: the exact target on the role shape (PRD §13.1).
//!
//! `{node_admin: 1, broker: 1, gateway: 1}`. The gateway is the long-lived caller: the probe
//! invokes the broker through it by the gateway’s captured node ID, so the gateway's one
//! `NodeRpcClient` holds the pooled connection to the broker's birth.
//! 1. By `ExactNode` and by `CurrentPath`, the gateway reaches the broker: it served, nobody else.
//! 2. The broker restarts (stop, then start, on the stopped process: same NodeId, incarnation and port).
//! 3. The next call through the gateway reaches the broker: its stop cut the connection the gateway
//!    held (`rdm.node_rpc.connection.delete.via-hard-cut`, an accepted connection of the broker's), so
//!    the gateway dialed afresh; the value written before the restart is read back from the broker's
//!    own data dir. Every call was the gateway's one inner invocation
//!    of exactly the broker (`rdm.node_rpc.request.serve.via-carried-inner`).
//! The gateway learns a birth by the topology view, not from the admin's view: the cell calls through the
//! gateway only once the gateway's own resolver holds the birth it is about to call (the probe's
//! `resolve` asks the gateway what it holds).
//! The in-flight arm — a dial to a birth that moves before it is pooled ends as
//! `RejectedStale`, never dispatched — is `crates/rafka-node-rpc/tests/pool.rs`.

use rdm_client::{FabricDesired, MeshDesired};
use rdm_test::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use rdm_core::PathName;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-rpc".into(),
        subfeature: "restart-target".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_gateway_reaches_the_brokers_same_birth_after_its_restart".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_reaches_the_brokers_same_birth_after_its_restart() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.resize(&FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 1), ("broker", 2), ("gateway", 1)])] }).await;
    let want = [("mesh1.node-admin", "1"), ("mesh1.broker", "1"), ("mesh1.broker", "2"), ("mesh1.gateway", "1")].into_iter().map(|(path, name)| PathName::new(path, name).unwrap()).collect();
    let broker_location = PathName::new("mesh1.broker", "1").unwrap();
    let gateway_location = PathName::new("mesh1.gateway", "1").unwrap();
    estate.settles_to_named(&want, Duration::from_secs(30)).await;
    let broker = estate.node_value(&broker_location).await;
    let (broker_id, old_birth, addr) = (s(&broker["node_id"]), s(&broker["incarnation"]), s(&broker["transport_addr"]));
    let exact = format!("exact:{broker_id}");
    let gateway_id = estate.node_view(&gateway_location).await.node_id;
    let via = format!("exact:{gateway_id}");
    let by_location = format!("path:{}", serde_json::to_string(&broker_location).unwrap());
    estate.witness_holds_birth(&gateway_location, &broker_location).await;

    // 1. ExactNode and CurrentPath, through the gateway: the broker served.
    let put = estate.probe(&["put", "--target", &exact, "--via", &via, "--key", "f1", "--value", "before-restart"]);
    assert_eq!((put["outcome"].as_str(), put["route"].as_str()), (Some("Reply"), Some("via-peer")), "{put}");
    assert_eq!(put["reply"]["executing_node"], broker_id, "{put}");
    let by_path = estate.probe(&["get", "--target", &by_location, "--via", &via, "--key", "f1"]);
    assert_eq!((by_path["outcome"].as_str(), by_path["route"].as_str()), (Some("Reply"), Some("via-peer")), "{by_path}");
    assert_eq!(by_path["reply"]["executing_node"], broker_id, "{by_path}");
    assert_eq!(by_path["reply"]["result"], json!({"found": true, "value": "before-restart"}));

    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // 2. The broker restarts, node.stop then node.start (Q183): the same node, birth and port.
    estate.stop_then_start(&broker_location).await;
    let restarted = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(30)).await).into_iter().find(|n| n["node_id"] == broker["node_id"]).unwrap();
    assert_eq!(s(&restarted["node_id"]), broker_id, "a restart keeps the NodeId: {restarted}");
    assert_eq!(s(&restarted["incarnation"]), old_birth, "a restart keeps the birth: {restarted}");
    assert_eq!(s(&restarted["transport_addr"]), addr, "a restart keeps the port: {restarted}");

    estate.witness_holds_birth(&gateway_location, &broker_location).await;

    // 3. Through the gateway again: the same birth serves, and the value survived in its data dir.
    let after = estate.probe(&["get", "--target", &exact, "--via", &via, "--key", "f1"]);
    assert_eq!((after["outcome"].as_str(), after["route"].as_str()), (Some("Reply"), Some("via-peer")), "{after}");
    assert_eq!(after["reply"]["executing_node"], broker_id, "{after}");
    assert_eq!(after["reply"]["result"], json!({"found": true, "value": "before-restart"}));
    estate.artifact("calls.json", &json!({"put": put, "by_path": by_path, "restarted": restarted, "after": after}));

    estate.stop().await;
    let spans = estate.spans();
    let cut = named(&spans, "rdm.node_admin.node.update.via-stop-transport-cut").into_iter().find(|sp| sp["attributes"]["node"] == broker_id && sp["attributes"]["operation"] == "node.stop").expect("broker stop cuts transport");
    let evicted = named(&spans, "rdm.node_rpc.connection.delete.via-hard-cut")
        .into_iter()
        .find(|sp| sp["attributes"]["direction"] == "accepted" && descends_from(&spans, sp, cut) && sp["trace_id"] == cut["trace_id"])
        .cloned()
        .unwrap_or_else(|| panic!("the broker never cut a connection it had accepted"));
    let carried: Vec<&Value> = named(&spans, "rdm.node_rpc.request.serve.via-carried-inner").into_iter().filter(|sp| sp["attributes"]["target"] == broker_id.as_str()).collect();
    assert_eq!(carried.len(), 3, "the gateway carried the three calls to the broker: {carried:?}");
    estate.record_trace_url(evicted["trace_id"].as_str().unwrap_or(""));
}
