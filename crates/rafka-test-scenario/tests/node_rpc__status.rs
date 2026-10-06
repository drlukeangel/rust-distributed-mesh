//! i143.e6.s7 process E2E: a node declares its lifecycle state to its authority over Node RPC,
//! from public surfaces (node-admin's control API, `rafka-rpc-probe`'s `declare` oracle on the
//! node) and the estate's span evidence only.
//!
//! 1. The node declares `ReadyForTraffic` to its mesh-primary: `Applied`; the authority's view
//!    shows it as `declared` (membership's `status` is untouched); again: `AlreadyApplied`.
//! 2. `Pending` after `ReadyForTraffic`: `RejectedInvalidTransition`.
//! 3. A declaration under another node's id: `RejectedNotAuthority` (sender-not-subject); under
//!    a wrong incarnation: `RejectedStaleBirth`.
//! 4. A declaration to the non-primary admin: `RejectedNotAuthority` (receiver-not-primary).
//! 5. `Draining`, then `Leaving`: `Applied`, and the view follows.
//! Every decision is one `rafka.node_admin.status.update.via-declaration` span on the authority.

use rafka_test_scenario::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-rpc".into(),
        subfeature: "status".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_node_declares_its_state_to_its_authority_and_the_authority_decides".into(),
    }
}

fn declared(out: &Value) -> (String, String) {
    assert_eq!(out["outcome"], "Reply", "the oracle answered: {out}");
    let d = &out["declared"];
    assert_eq!(d["outcome"], "Reply", "the authority answered: {out}");
    (d["reply"].as_str().unwrap_or("").to_string(), d["detail"].as_str().unwrap_or("").to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_declares_its_state_to_its_authority_and_the_authority_decides() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", 2, 2)], Duration::from_secs(30)).await;
    let nodes = estate.nodes().await;
    let primary = nodes.iter().find(|n| n["kind"] == "node_admin" && n["is_primary"] == true).expect("a mesh primary").clone();
    let other_admin = nodes.iter().find(|n| n["kind"] == "node_admin" && n["is_primary"] != true).expect("a non-primary admin").clone();
    let (rpc1, rpc2) = (estate.node("mesh1.rpc.1").await, estate.node("mesh1.rpc.2").await);
    let node = format!("exact:{}", rpc1["node_id"].as_str().unwrap());
    let to = format!("exact:{}", primary["node_id"].as_str().unwrap());
    // What the authority applied is the authority's view: read it there.
    estate.admin = primary["admin_api_base"].as_str().expect("the primary advertises its control API").to_string();

    // 1. Applied, visible as `declared`, idempotent.
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "ready-for-traffic"]);
    assert_eq!(declared(&r).0, "applied", "{r}");
    let view = estate.node("mesh1.rpc.1").await;
    assert_eq!(view["declared"], "ReadyForTraffic", "the authority's view shows what it applied: {view}");
    assert_eq!(view["status"], "ready-for-traffic", "membership's status is untouched");
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "ready-for-traffic"]);
    assert_eq!(declared(&r).0, "already-applied", "{r}");

    // 2. Backward is refused by name.
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "pending"]);
    let (reply, detail) = declared(&r);
    assert_eq!(reply, "rejected-invalid-transition", "{r}");
    assert!(detail.contains("ReadyForTraffic"), "{r}");

    // 3. Not the subject; a stale birth.
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "draining", "--as-node-id", rpc2["node_id"].as_str().unwrap()]);
    let (reply, detail) = declared(&r);
    assert_eq!(reply, "rejected-not-authority", "{r}");
    assert!(detail.contains("SenderNotSubject"), "{r}");
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "draining", "--as-incarnation", "00000000000000000000000000000000"]);
    assert_eq!(declared(&r).0, "rejected-stale-birth", "{r}");

    // 4. The non-primary admin is not the authority.
    let not_primary = format!("exact:{}", other_admin["node_id"].as_str().unwrap());
    let r = estate.probe(&["declare", "--target", &node, "--to", &not_primary, "--state", "draining"]);
    let (reply, detail) = declared(&r);
    assert_eq!(reply, "rejected-not-authority", "{r}");
    assert!(detail.contains("ReceiverNotPrimary"), "{r}");
    assert_eq!(estate.node("mesh1.rpc.1").await["declared"], "ReadyForTraffic", "a refusal applies nothing");

    // 5. Forward moves apply and the view follows.
    assert_eq!(declared(&estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "draining"])).0, "applied");
    assert_eq!(declared(&estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "leaving"])).0, "applied");
    assert_eq!(estate.node("mesh1.rpc.1").await["declared"], "Leaving");
    assert_eq!(estate.node("mesh1.rpc.2").await["declared"], Value::Null, "rpc.2 declared nothing");

    estate.artifact("declarations.json", &json!({"primary": primary["name"], "other_admin": other_admin["name"], "node": rpc1["name"]}));
    estate.stop().await;

    // Evidence: every decision is one span on the authority, naming op, sender and outcome.
    let spans = estate.spans();
    let decided = named(&spans, "rafka.node_admin.status.update.via-declaration");
    let on_primary = |outcome: &str| decided.iter().filter(|s| s["attributes"]["node"] == primary["name"] && s["attributes"]["outcome"] == outcome && s["attributes"]["sender"] == "mesh1.rpc.1").count();
    assert_eq!(on_primary("applied"), 3, "RFT, Draining, Leaving: {decided:?}");
    assert_eq!(on_primary("already-applied"), 1);
    assert_eq!(on_primary("rejected-invalid-transition"), 1);
    assert_eq!(on_primary("rejected-stale-birth"), 1);
    assert_eq!(on_primary("rejected-not-authority"), 1, "sender-not-subject on the primary");
    assert_eq!(decided.iter().filter(|s| s["attributes"]["node"] == other_admin["name"] && s["attributes"]["outcome"] == "rejected-not-authority").count(), 1, "receiver-not-primary on the other admin");
    assert!(decided.iter().all(|s| s["attributes"]["op"] == "declare-node-state"));
}
