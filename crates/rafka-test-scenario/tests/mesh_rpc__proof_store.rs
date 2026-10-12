//! i143.e7.s3 process E2E: the testkit proof store on op `0x70`.
//!
//! From public surfaces (node-admin's control API and `rafka-rpc-probe` over
//! real Node RPC) and the estate's span evidence only:
//! 1. Every op answers its typed result and names where it ran: node id, path,
//!    mesh and incarnation.
//! 2. A same-node restart keeps the store: the new incarnation of the same
//!    logical node serves the value written before it.
//! 3. A killed node is recovered by its mesh-primary (node-recovery.md): a new logical node at
//!    the same path.name over the inherited data dir and endpoint key, so it keeps the store.
//! 4. A planned replacement (node-replace.md) does not inherit it: `path:` reaches the successor,
//!    whose store is empty; the predecessor is parked at `<path>.old`, and once node.delete of it
//!    publishes its departure, `exact:<old>` resolves Gone and never follows the replacement.
//! 5. A key or value over its limit is refused by name, and writes nothing.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

const NODE: &str = "mesh1.rpc.1";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-rpc".into(),
        subfeature: "proof-store".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "the_proof_store_survives_a_restart_and_a_recovery_and_never_a_replacement".into(),
    }
}

/// The reply's provenance names exactly `node` (as the view holds it).
fn landed_on(out: &Value, node: &Value, op: &str) {
    assert_eq!(out["outcome"], "Reply", "{out}");
    let r = &out["reply"];
    assert_eq!(r["executing_node"], node["node_id"], "{out}");
    assert_eq!(r["node"], node["name"], "{out}");
    assert_eq!(r["mesh"], "mesh1", "{out}");
    assert_eq!(r["incarnation_id"], node["incarnation_id"], "{out}");
    assert_eq!(r["op"], op, "{out}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_proof_store_survives_a_restart_and_a_recovery_and_never_a_replacement() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 1, "rpc_node": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 1, 2)], Duration::from_secs(30)).await;
    let path = format!("path:{NODE}");

    // 1. Typed results, each naming where it ran.
    let first = estate.node(NODE).await;
    let exact_first = format!("exact:{}", first["node_id"].as_str().unwrap());
    landed_on(&estate.probe(&["put", "--target", &path, "--key", "41", "--value", "before-restart"]), &first, "put");
    let got = estate.probe(&["get", "--target", &exact_first, "--key", "41"]);
    landed_on(&got, &first, "get");
    assert_eq!(got["reply"]["result"], json!({"found": true, "value": "before-restart"}));
    let miss = estate.probe(&["cas", "--target", &path, "--key", "41", "--expected", "nope", "--value", "x"]);
    landed_on(&miss, &first, "compare-and-swap");
    assert_eq!(miss["reply"]["result"], json!({"swapped": false, "current": "before-restart"}));
    estate.probe(&["put", "--target", &path, "--key", "42", "--value", "doomed"]);
    let swapped = estate.probe(&["cas", "--target", &path, "--key", "42", "--expected", "doomed"]);
    assert_eq!(swapped["reply"]["result"], json!({"swapped": true}), "cas without --value deletes: {swapped}");
    let gone = estate.probe(&["delete", "--target", &path, "--key", "42"]);
    landed_on(&gone, &first, "delete");
    assert_eq!(gone["reply"]["result"], json!({"found": false}), "the key was already deleted: {gone}");
    // The other rpc node holds none of it.
    let other = estate.probe(&["get", "--target", "path:mesh1.rpc.2", "--key", "41"]);
    landed_on(&other, &estate.node("mesh1.rpc.2").await, "get");
    assert_eq!(other["reply"]["result"], json!({"found": false}));

    // 5. Limits, refused by name; nothing is written.
    let long_key = "k".repeat(257);
    let refused = estate.probe(&["put", "--target", &path, "--key", &long_key, "--value", "v"]);
    assert_eq!(refused["reply"]["result"], json!({"refused": "too-large", "field": "key", "limit": 256, "got": 257}), "{refused}");
    let absent = estate.probe(&["get", "--target", &path, "--key", &"k".repeat(256)]);
    assert_eq!(absent["reply"]["result"], json!({"found": false}));

    // 2. A same-node restart keeps the store.
    let (status, r) = estate.post(&format!("/api/nodes/{NODE}/restart"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    estate.await_attempt(r["build_id"].as_str().unwrap(), Estate::attempt_of(&r), Duration::from_secs(120)).await;
    let restarted = wait_for("the restarted birth is ready", Duration::from_secs(60), || async {
        let n = estate.node_opt(NODE).await?;
        (n["status"] == "ready-for-traffic" && n["parked"] == false).then_some(n)
    })
    .await;
    assert_eq!(restarted["node_id"], first["node_id"], "a restart keeps the logical node");
    assert_eq!(restarted["incarnation_id"], first["incarnation_id"], "and the birth: the process was stopped and started, never replaced");
    let kept = estate.probe(&["get", "--target", &exact_first, "--key", "41"]);
    landed_on(&kept, &restarted, "get");
    assert_eq!(kept["reply"]["result"], json!({"found": true, "value": "before-restart"}), "the restarted node serves the old value: {kept}");

    // 3. A killed node is recovered by its mesh-primary over its inherited data dir: a new logical
    //    node at the same path.name, with the same endpoint key, that keeps the store.
    let data_dir = estate.data_dir_of(NODE).await;
    estate.kill_node(NODE).await;
    let recovered = wait_for("the recovered node holds the path", crate::node_recovery_due(), || async {
        let n = estate.node_opt(NODE).await?;
        (n["status"] == "ready-for-traffic" && n["node_id"] != first["node_id"]).then_some(n)
    })
    .await;
    assert_ne!(recovered["incarnation_id"], first["incarnation_id"], "recovery creates a new node.id and incarnation");
    assert_eq!(recovered["endpoint_id"], first["endpoint_id"], "the same endpoint key, loaded from the inherited data dir");
    assert_eq!(estate.data_dir_of(NODE).await, data_dir, "the inherited data dir");
    let inherited = estate.probe(&["get", "--target", &path, "--key", "41"]);
    landed_on(&inherited, &recovered, "get");
    assert_eq!(inherited["reply"]["result"], json!({"found": true, "value": "before-restart"}), "the recovered node serves the store it inherited: {inherited}");

    // 4. A planned replacement does not inherit it: the successor's data dir is its own.
    let exact_recovered = format!("exact:{}", recovered["node_id"].as_str().unwrap());
    let (status, r) = estate.post(&format!("/api/nodes/{NODE}/replace"), &json!({})).await;
    assert_eq!(status, 202, "{r}");
    estate.await_attempt(r["build_id"].as_str().unwrap(), Estate::attempt_of(&r), Duration::from_secs(120)).await;
    let replacement = wait_for("a new logical node holds the path", Duration::from_secs(60), || async {
        let n = estate.node_opt(NODE).await?;
        (n["status"] == "ready-for-traffic" && n["node_id"] != recovered["node_id"]).then_some(n)
    })
    .await;
    assert_ne!(replacement["endpoint_id"], recovered["endpoint_id"], "a planned replacement is a new node with a new endpoint key");
    let fresh = estate.probe(&["get", "--target", &path, "--key", "41"]);
    landed_on(&fresh, &replacement, "get");
    assert_eq!(fresh["reply"]["result"], json!({"found": false}), "a replacement starts with an empty store: {fresh}");
    // The replace parks the predecessor at `<path>.old`, stopped, with its data (node-replace.md
    // step 3); node.delete of that parked birth is the recipe's step that releases its identity,
    // and its NodeDeleted is the departure (node-rpc.md: Gone comes only from NodeDeleted).
    let old_name = format!("{NODE}.old");
    let (status, d) = estate.delete(&format!("/api/nodes/{old_name}")).await;
    assert_eq!(status, 202, "node.delete of the parked {old_name} is accepted: {d}");
    estate.await_attempt(d["build_id"].as_str().unwrap(), Estate::attempt_of(&d), Duration::from_secs(120)).await;
    // A node's own live resolver knows more: the parked predecessor's delete published the old
    // birth's departure, so the other rpc node answers Gone for the old id, the new holder for
    // the path, and Unknown for an id nobody ever saw.
    let other = "path:mesh1.rpc.2";
    let gone = wait_for("mesh1.rpc.2 holds the old birth's departure", Duration::from_secs(30), || async {
        let r = estate.probe(&["resolve", "--target", other, "--query", &exact_recovered]);
        (r["reply"]["resolution"] == "gone").then_some(r)
    })
    .await;
    assert_eq!(gone["outcome"], "Reply", "{gone}");
    // The admin holds the old birth as the replacement's predecessor until the departure is
    // published, and it is published after the replacement is ready: the admin's view is
    // current-only once the departure has been heard, so the probe's own resolver can only say
    // the old id is nothing it knows.
    let old = estate.probe(&["get", "--target", &exact_recovered, "--key", "41"]);
    // The probe also prints its own traceparent; the verdict is the outcome, reason and route.
    assert_eq!((&old["outcome"], &old["reason"], &old["route"]), (&json!("NotSent"), &json!("Resolve(Unknown)"), &json!("direct")), "exact:<old> never follows a replacement: {old}");
    let holder = estate.probe(&["resolve", "--target", other, "--query", &format!("path:{NODE}")]);
    assert_eq!(holder["reply"]["resolution"], "found", "{holder}");
    assert_eq!(holder["reply"]["node_id"], replacement["node_id"], "the path's current holder is the replacement: {holder}");
    let never = estate.probe(&["resolve", "--target", other, "--query", &format!("exact:{}", rafka_mesh_entity::NodeId::mint())]);
    assert_eq!(never["reply"]["resolution"], "unknown", "{never}");
    estate.artifact("nodes-after.json", &json!(estate.nodes().await));
    estate.artifact("exact-old.json", &json!({"probe": old, "node": gone, "holder": holder, "never": never}));

    estate.stop().await;
    // Every served call is a span on the executing birth, naming the op and its outcome.
    let spans = estate.spans();
    let served = named(&spans, "rdm.node_rpc.proof_store.serve.via-request");
    let on = |inc: &Value, op: &str, outcome: &str| {
        served.iter().any(|s| s["attributes"]["incarnation_id"] == *inc && s["attributes"]["op"] == op && s["attributes"]["outcome"] == outcome)
    };
    assert!(on(&first["incarnation_id"], "put", "stored"));
    assert!(on(&first["incarnation_id"], "compare-and-swap", "mismatch"));
    assert!(on(&first["incarnation_id"], "put", "too-large"));
    assert!(on(&restarted["incarnation_id"], "get", "value"), "the restarted birth served the kept value");
    assert!(on(&recovered["incarnation_id"], "get", "value"), "the recovered birth served the inherited value");
    // 3: the mesh-primary proved the killed birth's exit and submitted its recovery, keyed on that
    // exact birth, and followed it to its terminal.
    let recovery = named(&spans, "rdm.node_admin.build.update.via-node-recovery")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == NODE && sp["attributes"]["node_id"] == first["node_id"])
        .expect("the mesh-primary's recovery submit for the killed birth");
    assert_eq!(recovery["attributes"]["incarnation_id"], first["incarnation_id"], "keyed on the exact exited birth: {recovery}");
    assert_eq!(recovery["attributes"]["outcome"], format!("complete: attempt {}", recovery["attributes"]["attempt"].as_str().unwrap_or_default()), "{recovery}");
    estate.record_trace_url(recovery["trace_id"].as_str().unwrap_or(""));
    assert!(on(&replacement["incarnation_id"], "get", "absent"), "the replacement served an empty store");
}
