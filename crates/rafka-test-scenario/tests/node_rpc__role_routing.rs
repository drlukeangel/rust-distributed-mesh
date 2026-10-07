//! i143.e11.s7 process E2E: routing/proxy, lite, on the role shape (PRD §13.1).
//!
//! `{node_admin: 1, broker: 2, gateway: 2}`. The probe stands in for a gateway's domain: `--target`
//! is the exact broker it selected, and the leg is the operator's (`Direct`, `--via <gateway>` =
//! `ViaPeer` through another gateway, `--no-route` = `NoActiveRoute`). From public surfaces and
//! span evidence only:
//! 1. Direct: the selected broker served, nobody else.
//! 2. ViaPeer through `mesh1.gateway.2`: the carrier made exactly one inner invocation to that same
//!    broker (`rafka.node_rpc.request.serve.via-carried-inner`) and never changed the final target.
//! 3. NoActiveRoute: `NotSent`, no connection opened, nothing served.
//! 4. Selecting the other broker executes that broker; the first broker's store is untouched.

use rafka_test_scenario::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-rpc".into(),
        subfeature: "role-routing".into(),
        rung: "SN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_gateway_reaches_the_selected_broker_direct_via_a_peer_gateway_or_not_at_all".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gateway_reaches_the_selected_broker_direct_via_a_peer_gateway_or_not_at_all() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "broker": 2, "gateway": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let want = ["mesh1.admin.1", "mesh1.broker.1", "mesh1.broker.2", "mesh1.gateway.1", "mesh1.gateway.2"].iter().map(|n| n.to_string()).collect();
    estate.settled(&want, Duration::from_secs(30)).await;
    let (b1, b2) = (estate.node("mesh1.broker.1").await, estate.node("mesh1.broker.2").await);
    let (b1_id, b2_id) = (b1["node_id"].as_str().unwrap().to_string(), b2["node_id"].as_str().unwrap().to_string());
    let exact_b1 = format!("exact:{b1_id}");

    // 1. Direct to broker.1.
    let put = estate.probe(&["put", "--target", &exact_b1, "--key", "r1", "--value", "one"]);
    assert_eq!(put["outcome"], "Reply", "{put}");
    let direct = estate.probe(&["get", "--target", &exact_b1, "--key", "r1"]);
    assert_eq!((direct["outcome"].as_str(), direct["route"].as_str()), (Some("Reply"), Some("direct")), "{direct}");
    assert_eq!(direct["reply"]["executing_node"], b1_id, "broker.1 served: {direct}");
    assert_eq!(direct["reply"]["result"], json!({"found": true, "value": "one"}));

    // 2. ViaPeer through gateway.2 to the same broker.
    let carried = estate.probe(&["get", "--target", &exact_b1, "--via", "path:mesh1.gateway.2", "--key", "r1"]);
    assert_eq!((carried["outcome"].as_str(), carried["route"].as_str()), (Some("Reply"), Some("via-peer")), "{carried}");
    assert_eq!(carried["reply"]["executing_node"], b1_id, "the carrier reached exactly broker.1: {carried}");
    assert_eq!(carried["reply"]["result"], json!({"found": true, "value": "one"}));

    // 3. NoActiveRoute: nothing sent.
    let none = estate.probe(&["get", "--target", &exact_b1, "--no-route", "--key", "r1"]);
    assert_eq!((none["outcome"].as_str(), none["route"].as_str()), (Some("NotSent"), Some("no-active-route")), "{none}");
    assert_eq!(none["reason"], "NoActiveRoute", "{none}");

    // 4. The other broker is selected: it executes; broker.1's store is as it was.
    let exact_b2 = format!("exact:{b2_id}");
    let changed = estate.probe(&["get", "--target", &exact_b2, "--key", "r1"]);
    assert_eq!((changed["outcome"].as_str(), changed["route"].as_str()), (Some("Reply"), Some("direct")), "{changed}");
    assert_eq!(changed["reply"]["executing_node"], b2_id, "broker.2 served: {changed}");
    assert_eq!(changed["reply"]["result"], json!({"found": false}), "broker.2 never saw broker.1's write: {changed}");
    estate.artifact("routing.json", &json!({"direct": direct, "carried": carried, "none": none, "changed": changed}));
    estate.stop().await;

    let spans = estate.spans();
    let served = named(&spans, "rafka.node_rpc.proof_store.serve.via-request");
    let served_in = |t: &[&Value]| served.iter().filter(|s| t.iter().any(|x| x["trace_id"] == s["trace_id"])).cloned().collect::<Vec<_>>();
    let direct_b1: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rafka.node_rpc.route.resolve.via-connections" && s["attributes"]["route"] == "direct" && s["attributes"]["target"] == b1_id).collect();
    assert!(!direct_b1.is_empty() && direct_b1.iter().all(|r| r["attributes"]["outcome"] == "Reply"), "{direct_b1:?}");
    let direct_served = served_in(&direct_b1);
    assert!(!direct_served.is_empty() && direct_served.iter().all(|s| s["attributes"]["incarnation_id"] == b1["incarnation_id"]), "the direct calls were served by broker.1's birth only: {direct_served:?}");
    let carried_t: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rafka.node_rpc.request.serve.via-carried-inner").collect();
    assert_eq!(carried_t.len(), 1, "exactly one carried inner call in the whole run: {carried_t:?}");
    assert_eq!(carried_t[0]["attributes"]["target"], b1_id, "the carrier was handed exactly broker.1");
    let via: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rafka.node_rpc.route.resolve.via-connections" && s["attributes"]["route"] == "via-peer").collect();
    assert_eq!(via.len(), 1, "{via:?}");
    assert_eq!((via[0]["attributes"]["target"].as_str(), via[0]["attributes"]["carrier"].as_str(), via[0]["attributes"]["outcome"].as_str()), (Some(b1_id.as_str()), Some("mesh1.gateway.2"), Some("Reply")));
    assert!(served_in(&via).iter().all(|s| s["attributes"]["incarnation_id"] == b1["incarnation_id"]), "the carried call was served by broker.1's birth only");
    let none_t: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rafka.node_rpc.route.resolve.via-connections" && s["attributes"]["route"] == "no-active-route").collect();
    assert_eq!(none_t.len(), 1, "{none_t:?}");
    assert_eq!(none_t[0]["attributes"]["outcome"], "NotSent");
    let none_trace = &none_t[0]["trace_id"];
    assert!(!spans.iter().any(|s| s["trace_id"] == *none_trace && s["name"].as_str().unwrap_or("").starts_with("rafka.node_rpc.connection")), "no route: no connection was opened");
    assert!(served_in(&none_t).is_empty(), "no route: nothing served");
    let b2_served: Vec<_> = served.iter().filter(|s| s["attributes"]["incarnation_id"] == b2["incarnation_id"]).collect();
    assert_eq!(b2_served.len(), 1, "broker.2 served exactly the one call selected for it: {b2_served:?}");
    estate.record_trace_url(via[0]["trace_id"].as_str().unwrap_or(""));
}
