//! i143.e6.s6 process E2E: the routing composition seam, from public surfaces and span
//! evidence only.
//!
//! The probe stands in for a domain: `--target` is the exact final node it selected. The leg is
//! the operator's here (`Direct`, `--via <carrier>` = `ViaPeer`, `--no-route` = `NoActiveRoute`),
//! standing in for connections' answer; that the seam takes its leg from a resolved
//! `EffectiveRoute` and never moves the selection is proven in `rafka-node-rpc/tests/routing.rs`.
//! The evidence names the same final target at the selector's output (the probe's argument),
//! at route resolution (`rdm.node_rpc.route.resolve.via-connections`) and at execution (the
//! carrier's one inner call and the target's own serve span):
//! 1. Direct: the selected node served, nobody else.
//! 2. ViaPeer: the carrier made exactly one inner call to that same node, which served.
//! 3. NoActiveRoute: `NotSent`, and the probe opened no connection at all.
//! 4. A selection change (another target) executes that target; the first target's leg is
//!    untouched and nothing was sent anywhere else.

use rafka_test_scenario::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-rpc".into(),
        subfeature: "routing".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "the_seam_executes_the_selected_target_over_the_chosen_route_and_nothing_else".into(),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_seam_executes_the_selected_target_over_the_chosen_route_and_nothing_else() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 3}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 1, 3)], Duration::from_secs(30)).await;
    let (b, c, p) = (estate.node("mesh1.rpc.1").await, estate.node("mesh1.rpc.2").await, estate.node("mesh1.rpc.3").await);
    let (b_id, c_id) = (b["node_id"].as_str().unwrap().to_string(), c["node_id"].as_str().unwrap().to_string());
    let exact_b = format!("exact:{b_id}");

    // 1. Direct to B.
    let put = estate.probe(&["put", "--target", &exact_b, "--key", "d1", "--value", "one"]);
    assert_eq!(put["outcome"], "Reply", "{put}");
    let direct = estate.probe(&["get", "--target", &exact_b, "--key", "d1"]);
    assert_eq!((direct["outcome"].as_str(), direct["route"].as_str()), (Some("Reply"), Some("direct")), "{direct}");
    assert_eq!(direct["reply"]["executing_node"], b["node_id"], "B served: {direct}");
    assert_eq!(direct["reply"]["result"], json!({"found": true, "value": "one"}));

    // 2. ViaPeer through P to the same B: P's one inner call reaches B, which served.
    estate.witness_holds("path:mesh1.rpc.3", "mesh1.rpc.1").await;
    let carried = estate.probe(&["get", "--target", &exact_b, "--via", "path:mesh1.rpc.3", "--key", "d1"]);
    assert_eq!((carried["outcome"].as_str(), carried["route"].as_str()), (Some("Reply"), Some("via-peer")), "{carried}");
    assert_eq!(carried["reply"]["executing_node"], b["node_id"], "the carrier reached exactly B: {carried}");
    assert_eq!(carried["reply"]["result"], json!({"found": true, "value": "one"}));

    // 3. NoActiveRoute to B: nothing sent.
    let none = estate.probe(&["get", "--target", &exact_b, "--no-route", "--key", "d1"]);
    assert_eq!((none["outcome"].as_str(), none["route"].as_str()), (Some("NotSent"), Some("no-active-route")), "{none}");
    assert_eq!(none["reason"], "NoActiveRoute", "{none}");

    // 4. The domain selects C instead: C executes, B's store is untouched, nothing went elsewhere.
    let exact_c = format!("exact:{c_id}");
    let changed = estate.probe(&["get", "--target", &exact_c, "--key", "d1"]);
    assert_eq!((changed["outcome"].as_str(), changed["route"].as_str()), (Some("Reply"), Some("direct")), "{changed}");
    assert_eq!(changed["reply"]["executing_node"], c["node_id"], "C served: {changed}");
    assert_eq!(changed["reply"]["result"], json!({"found": false}), "C's store never saw B's write: {changed}");
    let still = estate.probe(&["get", "--target", &exact_b, "--key", "d1"]);
    assert_eq!(still["reply"]["result"], json!({"found": true, "value": "one"}), "B's leg is as it was: {still}");

    estate.artifact("routing.json", &json!({"direct": direct, "carried": carried, "none": none, "changed": changed}));
    estate.stop().await;

    // Evidence: one final target at selection, resolution and execution.
    let spans = estate.spans();
    let served = named(&spans, "rdm.node_rpc.proof_store.serve.via-request");
    let served_in = |t: &[&Value]| served.iter().filter(|s| t.iter().any(|x| x["trace_id"] == s["trace_id"])).cloned().collect::<Vec<_>>();

    let direct_b: Vec<&Value> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node_rpc.route.resolve.via-connections" && s["attributes"]["route"] == "direct" && s["attributes"]["target"] == b_id)
        .collect();
    assert!(!direct_b.is_empty() && direct_b.iter().all(|r| r["attributes"]["outcome"] == "Reply"), "{direct_b:?}");
    let direct_served = served_in(&direct_b);
    assert!(!direct_served.is_empty() && direct_served.iter().all(|s| s["attributes"]["incarnation_id"] == b["incarnation_id"]), "the direct calls were served by B's birth only: {direct_served:?}");

    let carried_t: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.request.serve.via-carried-inner").collect();
    assert_eq!(carried_t.len(), 1, "exactly one carried inner call in the whole run: {carried_t:?}");
    assert_eq!(carried_t[0]["attributes"]["target"], b_id, "the carrier was handed exactly B");
    let via = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.route.resolve.via-connections" && s["attributes"]["route"] == "via-peer").collect::<Vec<_>>();
    assert_eq!(via.len(), 1, "{via:?}");
    assert_eq!((via[0]["attributes"]["target"].as_str(), via[0]["attributes"]["carrier"].as_str(), via[0]["attributes"]["outcome"].as_str()), (Some(b_id.as_str()), Some("mesh1.rpc.3"), Some("Reply")));
    let via_served = served_in(&via);
    assert!(via_served.iter().all(|s| s["attributes"]["incarnation_id"] == b["incarnation_id"]), "the carried call was served by B's birth only: {via_served:?}");

    let none_t: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.route.resolve.via-connections" && s["attributes"]["route"] == "no-active-route").collect();
    assert_eq!(none_t.len(), 1, "{none_t:?}");
    assert_eq!(none_t[0]["attributes"]["outcome"], "NotSent");
    let none_trace = &none_t[0]["trace_id"];
    assert!(!spans.iter().any(|s| s["trace_id"] == *none_trace && s["name"].as_str().unwrap_or("").starts_with("rdm.node_rpc.connection")), "no route: no connection was opened");
    assert!(served_in(&none_t).is_empty(), "no route: nothing served");

    let c_served: Vec<_> = served.iter().filter(|s| s["attributes"]["incarnation_id"] == c["incarnation_id"]).collect();
    assert_eq!(c_served.len(), 1, "C served exactly the one call the domain selected it for: {c_served:?}");
    let _ = p;
}
