//! i143.e6.s7 process E2E: a node declares its lifecycle state to its authority over Node RPC,
//! from public surfaces (node-admin's control API, `rafka-rpc-probe`'s `declare` oracle on the
//! node) and the estate's span evidence only.
//!
//! 0. Born ready, every rpc node declares `ReadyForTraffic` to its mesh-primary by itself
//!    (i143.e4.s11), and the non-primary admin declares its own to the fabric-primary: the
//!    authority's view shows them as `declared` before any probe asks.
//! 1. The node's `ReadyForTraffic` asked again through the oracle: `AlreadyApplied` (one logical
//!    event); membership's `status` is untouched.
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

    // 0. Born ready, the nodes and the other admin declared by themselves.
    let declared_by_birth = rafka_test_scenario::estate::wait_for("every birth's own ReadyForTraffic declared at the authority", Duration::from_secs(20), || async {
        let nodes = estate.nodes().await;
        let of = |name: &str| nodes.iter().find(|n| n["name"] == name).map(|n| n["declared"].clone()).unwrap_or(Value::Null);
        (of("mesh1.rpc.1") == "ReadyForTraffic" && of("mesh1.rpc.2") == "ReadyForTraffic" && of(other_admin["name"].as_str().unwrap()) == "ReadyForTraffic").then_some(nodes)
    })
    .await;
    let view = declared_by_birth.iter().find(|n| n["name"] == "mesh1.rpc.1").unwrap().clone();
    assert_eq!(view["status"], "ready-for-traffic", "membership's status is untouched");

    // 1. The same declaration asked again: one logical event.
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
    assert_eq!(estate.node("mesh1.rpc.2").await["declared"], "ReadyForTraffic", "rpc.2 declared only its birth");

    estate.artifact("declarations.json", &json!({"primary": primary["name"], "other_admin": other_admin["name"], "node": rpc1["name"]}));
    estate.stop().await;

    // Evidence: every decision is one span on the authority, naming op, sender and outcome.
    let spans = estate.spans();
    let decided = named(&spans, "rafka.node_admin.status.update.via-declaration");
    let on_primary = |outcome: &str| decided.iter().filter(|s| s["attributes"]["node"] == primary["name"] && s["attributes"]["outcome"] == outcome && s["attributes"]["sender"] == "mesh1.rpc.1").count();
    assert_eq!(on_primary("applied"), 3, "RFT (by the node at birth), Draining, Leaving: {decided:?}");
    assert!(on_primary("already-applied") >= 1, "the oracle's repeat, and the node's own Draining/Leaving at stop: {decided:?}");
    // The oracle's Pending after ReadyForTraffic; and, once the oracle moved the node to Leaving, the
    // node's own Draining at stop is a backward move too: refused by name, never regressing.
    let backward: Vec<_> = decided.iter().filter(|s| s["attributes"]["node"] == primary["name"] && s["attributes"]["outcome"] == "rejected-invalid-transition" && s["attributes"]["sender"] == "mesh1.rpc.1").collect();
    assert!(backward.iter().any(|s| s["attributes"]["detail"] == "ReadyForTraffic"), "Pending after ReadyForTraffic: {backward:?}");
    assert!(backward.iter().all(|s| s["attributes"]["detail"] == "ReadyForTraffic" || s["attributes"]["detail"] == "Leaving"), "{backward:?}");
    assert_eq!(on_primary("rejected-stale-birth"), 1);
    assert_eq!(on_primary("rejected-not-authority"), 1, "sender-not-subject on the primary");
    // Births try their mesh's admins in path order, so the non-primary admin refuses each once
    // (receiver-not-primary) besides the oracle's probe.
    let not_primary: Vec<_> = decided.iter().filter(|s| s["attributes"]["node"] == other_admin["name"] && s["attributes"]["outcome"] == "rejected-not-authority").collect();
    assert!(not_primary.iter().any(|s| s["attributes"]["sender"] == "mesh1.rpc.1" && s["attributes"]["detail"] == "receiver-not-primary"), "{not_primary:?}");
    assert!(not_primary.iter().all(|s| s["attributes"]["detail"] == "receiver-not-primary" || s["attributes"]["detail"] == "sender-not-subject"), "{not_primary:?}");
    // Nodes and admins declare their own state; the mesh primary declares its Mesh's (e4.s11).
    assert!(decided.iter().all(|s| ["declare-node-state", "declare-mesh-state", "apply-fabric-event"].contains(&s["attributes"]["op"].as_str().unwrap_or(""))), "{decided:?}");
    // The other admin's own birth was applied by the fabric-primary (this primary) too.
    assert!(decided.iter().any(|s| s["attributes"]["sender"] == other_admin["name"] && s["attributes"]["outcome"] == "applied"), "{decided:?}");
}
