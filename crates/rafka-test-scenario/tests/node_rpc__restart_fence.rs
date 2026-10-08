//! i143.e11.s5 process E2E: exact target + fence on the role shape (PRD §13.1).
//!
//! `{node_admin: 1, broker: 1, gateway: 1}`. The gateway is the long-lived caller: the probe
//! invokes the broker through it (`--via path:mesh1.gateway.1`), so the gateway's one
//! `NodeRpcClient` holds the pooled connection to the broker's birth.
//! 1. By `ExactNode` and by `CurrentPath`, the gateway reaches the broker: it served, nobody else.
//! 2. The broker restarts (same NodeId, new incarnation, fresh ports).
//! 3. The next call through the gateway reaches the new birth: the gateway evicted the old
//!    incarnation's connection (`rdm.node_rpc.connection.evict.via-incarnation-superseded`,
//!    naming the old incarnation) and dialed the new one; the value written before the restart is
//!    read back from the broker's own data dir. Every call was the gateway's one inner invocation
//!    of exactly the broker (`rdm.node_rpc.request.serve.via-carried-inner`).
//! The in-flight arm — a dial to a birth that moves before it is pooled ends as
//! `RejectedStale`, never dispatched — is `crates/rafka-node-rpc/tests/pool.rs`.

use rafka_test_scenario::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-rpc".into(),
        subfeature: "restart-fence".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_gateway_reaches_the_brokers_new_birth_after_its_restart".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_reaches_the_brokers_new_birth_after_its_restart() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "broker": 1, "gateway": 1}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let want = ["mesh1.admin.1", "mesh1.broker.1", "mesh1.gateway.1"].iter().map(|n| n.to_string()).collect();
    estate.settled(&want, Duration::from_secs(30)).await;
    let broker = estate.node("mesh1.broker.1").await;
    let (broker_id, old_birth, addr) = (s(&broker["node_id"]), s(&broker["incarnation_id"]), s(&broker["transport_addr"]));
    let exact = format!("exact:{broker_id}");
    let via = "path:mesh1.gateway.1";

    // 1. ExactNode and CurrentPath, through the gateway: the broker served.
    let put = estate.probe(&["put", "--target", &exact, "--via", via, "--key", "f1", "--value", "before-restart"]);
    assert_eq!((put["outcome"].as_str(), put["route"].as_str()), (Some("Reply"), Some("via-peer")), "{put}");
    assert_eq!(put["reply"]["executing_node"], broker_id, "{put}");
    let by_path = estate.probe(&["get", "--target", "path:mesh1.broker.1", "--via", via, "--key", "f1"]);
    assert_eq!((by_path["outcome"].as_str(), by_path["route"].as_str()), (Some("Reply"), Some("via-peer")), "{by_path}");
    assert_eq!(by_path["reply"]["executing_node"], broker_id, "{by_path}");
    assert_eq!(by_path["reply"]["result"], json!({"found": true, "value": "before-restart"}));

    // 2. The broker restarts: same NodeId, new incarnation, fresh ports.
    let (status, restart) = estate.post("/api/nodes/mesh1.broker.1/restart", &json!({})).await;
    assert_eq!(status, 202, "{restart}");
    estate.await_attempt(restart["build_id"].as_str().unwrap(), Estate::attempt_of(&restart), Duration::from_secs(120)).await;
    let reborn = estate.settled(&want, Duration::from_secs(30)).await.into_iter().find(|n| n["name"] == "mesh1.broker.1").unwrap();
    assert_eq!(s(&reborn["node_id"]), broker_id, "a restart keeps the NodeId: {reborn}");
    assert_ne!(s(&reborn["incarnation_id"]), old_birth, "a restart is a new birth: {reborn}");
    assert_ne!(s(&reborn["transport_addr"]), addr, "a restart binds a fresh port: {reborn}");

    // 3. Through the gateway again: the new birth serves, and the value survived in its data dir.
    let after = estate.probe(&["get", "--target", &exact, "--via", via, "--key", "f1"]);
    assert_eq!((after["outcome"].as_str(), after["route"].as_str()), (Some("Reply"), Some("via-peer")), "{after}");
    assert_eq!(after["reply"]["executing_node"], broker_id, "{after}");
    assert_eq!(after["reply"]["result"], json!({"found": true, "value": "before-restart"}));
    estate.artifact("calls.json", &json!({"put": put, "by_path": by_path, "restart": restart, "reborn": reborn, "after": after}));

    estate.stop().await;
    let spans = estate.spans();
    let evicted = named(&spans, "rdm.node_rpc.connection.evict.via-incarnation-superseded")
        .into_iter()
        .find(|sp| sp["attributes"]["incarnation_id"] == old_birth.as_str())
        .cloned()
        .unwrap_or_else(|| panic!("the gateway never evicted the old birth {old_birth}"));
    let carried: Vec<&Value> = named(&spans, "rdm.node_rpc.request.serve.via-carried-inner").into_iter().filter(|sp| sp["attributes"]["target"] == broker_id.as_str()).collect();
    assert_eq!(carried.len(), 3, "the gateway carried the three calls to the broker: {carried:?}");
    estate.record_trace_url(evicted["trace_id"].as_str().unwrap_or(""));
}
