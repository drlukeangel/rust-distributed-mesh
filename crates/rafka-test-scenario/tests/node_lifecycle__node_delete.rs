//! i143 process E2E: removing one node is a lifecycle op with two events (i143.e6.s10).
//!
//! From public surfaces and span evidence only, on an MM estate: mesh1 (two admins, two rpc
//! nodes) and mesh2 (one admin, two rpc nodes). `DELETE /api/nodes/mesh2.rpc.2` reaches the
//! fabric-primary, which accepts a Build without the node; mesh2's primary claims the attempt
//! and executes the retirement.
//!
//! 1. Before the executor starts, the node is routable everywhere. Once it holds the operation
//!    it publishes `NodeDeleting`: the node stays found and live in every view and stops being
//!    routable. Only after the provider proved the runtime terminal does it publish
//!    `NodeDeleted`: a peer mesh's rpc node then answers `Gone` for the old id, `Unknown` for
//!    the path (no holder) and for an id nobody saw, and still `Found` for the surviving node.
//! 2. A node born after the events learns the departure from the retained aggregate, never
//!    having heard an event.
//! 3. The pre-notice is never emitted before the executor's claim, the departure never before
//!    the terminal proof, and the departure is not drift: nothing is reborn.

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

const VICTIM: &str = "mesh2.rpc.2";
const WITNESS: &str = "path:mesh1.rpc.1";

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-lifecycle".into(),
        subfeature: "node-delete".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn resolution(r: &Value) -> String {
    s(&r["reply"]["resolution"])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_a_node_is_a_pre_notice_then_a_proven_departure_every_mesh_hears() {
    let mut estate = Estate::bootstrap(owner("deleting_a_node_is_a_pre_notice_then_a_proven_departure"), "fabric1", "mesh1").await;
    let (status, a) = estate
        .post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 2}, {"name": "mesh2", "node_admin": 1, "rpc_node": 2}]}))
        .await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(180)).await;
    let before = estate.settled_shape(&[("mesh1", 2, 2), ("mesh2", 1, 2)], Duration::from_secs(60)).await;
    let victim = before.iter().find(|n| n["name"] == VICTIM).cloned().unwrap();
    let (victim_id, victim_inc) = (s(&victim["node_id"]), s(&victim["incarnation_id"]));
    let exact_victim = format!("exact:{victim_id}");
    estate.artifact("nodes-before.json", &json!(before));

    // The peer mesh's rpc node holds mesh2's nodes through the forwarded aggregate.
    let found = wait_for("mesh1.rpc.1 resolves mesh2.rpc.2", Duration::from_secs(30), || async {
        let r = estate.probe(&["resolve", "--target", WITNESS, "--query", &exact_victim]);
        (resolution(&r) == "found").then_some(r)
    })
    .await;
    assert_eq!(found["reply"]["incarnation_id"], victim["incarnation_id"], "{found}");
    assert!(before.iter().all(|n| n["routable"] == true), "every live node is routable before the delete: {before:#?}");

    // 1. The removal.
    let (status, d) = estate.delete(&format!("/api/nodes/{VICTIM}")).await;
    assert_eq!(status, 202, "{d}");
    let build_id = s(&d["build_id"]);
    // While the executor holds the operation and the runtime still runs, every view shows the
    // node found, live and not routable.
    let overlay = wait_for("the pre-notice overlay is visible while the node still lives", Duration::from_secs(30), || async {
        let n = estate.nodes().await.into_iter().find(|n| n["name"] == VICTIM)?;
        (n["routable"] == false && !matches!(n["status"].as_str(), Some("dead" | "pending-reconnect")) && n["node_id"] == victim["node_id"]).then_some(n)
    })
    .await;
    estate.artifact("overlay.json", &overlay);
    estate.await_build(&build_id, Duration::from_secs(120)).await;
    let after = estate.settled_shape(&[("mesh1", 2, 2), ("mesh2", 1, 1)], Duration::from_secs(60)).await;
    assert!(after.iter().all(|n| n["name"] != VICTIM), "the current topology no longer lists the node: {after:#?}");
    estate.artifact("nodes-after.json", &json!(after));

    let gone = wait_for("mesh1.rpc.1 holds the departure", Duration::from_secs(30), || async {
        let r = estate.probe(&["resolve", "--target", WITNESS, "--query", &exact_victim]);
        (resolution(&r) == "gone").then_some(r)
    })
    .await;
    let path = estate.probe(&["resolve", "--target", WITNESS, "--query", &format!("path:{VICTIM}")]);
    assert_eq!(resolution(&path), "unknown", "the path has no holder: {path}");
    let survivor = estate.probe(&["resolve", "--target", WITNESS, "--query", "path:mesh2.rpc.1"]);
    assert_eq!(resolution(&survivor), "found", "the surviving node is untouched: {survivor}");
    let never = estate.probe(&["resolve", "--target", WITNESS, "--query", &format!("exact:{}", rafka_mesh_entity::NodeId::mint())]);
    assert_eq!(resolution(&never), "unknown", "{never}");
    estate.artifact("resolutions.json", &json!({"gone": gone, "path": path, "survivor": survivor, "never": never}));

    // 2. A node born after the events learns the departure from the retained aggregate.
    let (status, sp) = estate.post("/api/nodes/spawn", &json!({"mesh": "mesh1", "kind": "rpc_node"})).await;
    assert_eq!(status, 202, "{sp}");
    estate.await_build(sp["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let late = estate.settled_shape(&[("mesh1", 2, 3), ("mesh2", 1, 1)], Duration::from_secs(60)).await;
    let newcomer = late.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node" && before.iter().all(|b| b["node_id"] != n["node_id"])).cloned().unwrap();
    let late_gone = wait_for("the newcomer holds the departure it never heard as an event", Duration::from_secs(45), || async {
        let r = estate.probe(&["resolve", "--target", &format!("path:{}", s(&newcomer["name"])), "--query", &exact_victim]);
        (resolution(&r) == "gone").then_some(r)
    })
    .await;
    // And one born into the executor's own mesh, which learns it from its own primary's
    // overlays rather than a forwarded aggregate.
    let (status, sp2) = estate.post("/api/nodes/spawn", &json!({"mesh": "mesh2", "kind": "rpc_node"})).await;
    assert_eq!(status, 202, "{sp2}");
    estate.await_build(sp2["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let late2 = estate.settled_shape(&[("mesh1", 2, 3), ("mesh2", 1, 2)], Duration::from_secs(60)).await;
    let newcomer2 = late2.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "rpc_node" && before.iter().all(|b| b["node_id"] != n["node_id"])).cloned().unwrap();
    let own_mesh_gone = wait_for("the newcomer in the executor's own mesh holds the departure", Duration::from_secs(45), || async {
        let r = estate.probe(&["resolve", "--target", &format!("path:{}", s(&newcomer2["name"])), "--query", &exact_victim]);
        (resolution(&r) == "gone").then_some(r)
    })
    .await;
    estate.artifact("late-joiner.json", &json!({"peer_mesh": late_gone, "own_mesh": own_mesh_gone}));

    estate.stop().await;
    // 3. Cut points and the absence of drift.
    let spans = estate.spans();
    let start = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let deleting: Vec<&Value> = named(&spans, "rafka.node_admin.node.update.via-node-deleting").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).collect();
    let deleted: Vec<&Value> = named(&spans, "rafka.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).collect();
    assert_eq!(deleting.len(), 1, "one pre-notice for one retirement: {deleting:?}");
    assert_eq!(deleted.len(), 1, "one departure for one retirement: {deleted:?}");
    assert_eq!(deleting[0]["attributes"]["build_id"], build_id);
    assert_eq!(deleted[0]["attributes"]["operation"], format!("retire-node:{VICTIM}"));
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], victim_inc, "the departure names the exact birth");
    let executor_claim = named(&spans, "rafka.node_admin.build.update.via-reconcile")
        .into_iter()
        .filter(|sp| sp["attributes"]["build_id"] == build_id && sp["attributes"]["executor"] == "mesh2.admin.1")
        .map(start)
        .min()
        .expect("mesh2's primary claimed the removal");
    assert!(start(deleting[0]) > executor_claim, "the pre-notice follows the executor's claim");
    let terminal = named(&spans, "rafka.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["node"] == VICTIM && sp["attributes"]["outcome"] == "complete")
        .map(start)
        .max()
        .expect("the runtime was terminated and inspected");
    assert!(start(deleted[0]) > terminal, "the departure follows the terminal proof");
    assert!(start(deleting[0]) < start(deleted[0]));
    let heard_deleting = named(&spans, "rafka.mesh.membership.update.via-node-deleting").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).count();
    let heard_deleted = named(&spans, "rafka.mesh.membership.remove.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).count();
    assert!(heard_deleting >= 5, "every other node heard the pre-notice: {heard_deleting}");
    assert!(heard_deleted >= 5, "every other node heard the departure: {heard_deleted}");
    assert!(named(&spans, "rafka.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| start(sp) > start(deleting[0])).next().is_none(), "a departure is not drift: nothing is repaired");
    let mut births: Vec<String> = named(&spans, "rafka.node_admin.node.create.via-build").into_iter().filter(|sp| start(sp) > start(deleting[0])).map(|sp| s(&sp["attributes"]["node"])).collect();
    births.sort();
    let mut asked = vec![s(&newcomer["name"]), s(&newcomer2["name"])];
    asked.sort();
    assert_eq!(births, asked, "the only births after the delete are the two asked for");
}
