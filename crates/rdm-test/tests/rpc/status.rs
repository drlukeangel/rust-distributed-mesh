//! i143.e6.s7 process E2E: a node declares its lifecycle state to its authority over Node RPC,
//! from public surfaces (`node.get` at the authority, `rdm-rpc-probe`'s `declare` oracle on the
//! node) and the estate's span evidence only.
//!
//! 0. Born ready, every rpc node declares `Ready` to its mesh-primary by itself
//!    (i143.e4.s11), and the non-primary admin declares its own to its mesh-primary: the
//!    authority's view shows them as `declared` before any probe asks.
//! 1. The node's `Ready` asked again through the oracle: `AlreadyApplied` (one logical
//!    event); topology view's `status` is untouched.
//! 2. `Pending` after `Ready`: `RejectedInvalidNodeTransition`.
//! 3. A declaration naming a node the authority does not hold: `RejectedNotPrimary`
//!    (subject-unknown; decided for the subject, never the caller); under a wrong incarnation:
//!    `RejectedStaleIncarnation`.
//! 4. A declaration to the non-primary admin: `RejectedNotPrimary` (receiver-not-primary).
//! 5. `ConnectionsDeleting`, then `Stopped`: `Applied`, and the view follows.
//! Every decision is one `rdm.node_admin.status.update.via-declaration` span on the authority.

use rdm_test::estate::{named, Estate, Owner};
use serde_json::{json, Value};

fn actor_is(span: &Value, node: &Value) -> bool { span["attributes"]["node_path"] == node["name"]["path"] && span["attributes"]["node_name"] == node["name"]["name"] }
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-rpc".into(),
        subfeature: "status".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn declared(out: &Value) -> (String, String) {
    assert_eq!(out["outcome"], "Reply", "the oracle answered: {out}");
    let d = &out["declared"];
    assert_eq!(d["outcome"], "Reply", "the authority answered: {out}");
    (d["reply"].as_str().unwrap_or("").to_string(), d["detail"].as_str().unwrap_or("").to_string())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_declares_its_state_to_its_mesh_primary_and_the_mesh_primary_decides() {
    let mut estate = Estate::bootstrap(owner("a_node_declares_its_state_to_its_mesh_primary_and_the_mesh_primary_decides"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.settles_to(&[("mesh1", 2, 2)], Duration::from_secs(30)).await;
    let nodes = estate.node_values().await;
    let primary = nodes.iter().find(|n| n["kind"] == "node_admin" && n["is_primary"] == true).expect("a mesh primary").clone();
    let other_admin = nodes.iter().find(|n| n["kind"] == "node_admin" && n["is_primary"] != true).expect("a non-primary admin").clone();
    let rpc1 = estate.node_value(&rdm_core::PathName::new("mesh1.rpc", "1").unwrap()).await;
    let node = format!("exact:{}", rpc1["node_id"].as_str().unwrap());
    let to = format!("exact:{}", primary["node_id"].as_str().unwrap());
    // What the authority applied is the authority's view: read it there.
    let primary_id = serde_json::from_value(primary["node_id"].clone()).expect("primary ID");
    estate.enter_by_id(&primary_id).await;

    // 0. Born ready, the nodes and the other admin declared by themselves.
    let declared_by_birth = Estate::values(
        &estate
            .until_view("every birth's own Ready declared at the authority", Duration::from_secs(20), |views| {
                nodes.iter().all(|before| views.iter().any(|now| now.node_id.as_str() == before["node_id"].as_str().unwrap() && serde_json::to_value(&now.incarnation_id).unwrap() == before["incarnation_id"] && now.declared.as_deref() == Some("Ready"))).then(|| views.to_vec())
            })
            .await,
    );
    let view = declared_by_birth.iter().find(|n| n["node_id"] == rpc1["node_id"]).unwrap().clone();
    assert_eq!(view["status"], "ready", "topology view's status is untouched");
    // The authority itself: its own Ready goes through the same door, applied by itself.
    let self_declared = declared_by_birth.iter().find(|n| n["node_id"] == primary["node_id"]).unwrap();
    assert_eq!(self_declared["declared"], "Ready", "the mesh primary declared its own birth to itself: {self_declared}");

    // 1. The same declaration asked again: one logical event.
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "ready"]);
    assert_eq!(declared(&r).0, "already-applied", "{r}");

    // 2. Backward is refused by name.
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "pending"]);
    let (reply, detail) = declared(&r);
    assert_eq!(reply, "rejected-invalid-node-transition", "{r}");
    assert!(detail.contains("Ready"), "{r}");

    // 3. A subject the authority does not hold (decided for the subject, never the caller:
    // node-rpc.md §14); a stale birth.
    let never_born = rdm_core::NodeId::mint();
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "connections-deleting", "--as-node-id", never_born.as_str()]);
    let (reply, detail) = declared(&r);
    assert_eq!(reply, "rejected-not-primary", "{r}");
    assert!(detail.contains("SubjectUnknown"), "{r}");
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "connections-deleting", "--as-incarnation", rdm_core::IncarnationId::mint().as_str()]);
    assert_eq!(declared(&r).0, "rejected-stale-incarnation", "{r}");

    // 4. The non-primary admin is not the authority.
    let not_primary = format!("exact:{}", other_admin["node_id"].as_str().unwrap());
    let r = estate.probe(&["declare", "--target", &node, "--to", &not_primary, "--state", "connections-deleting"]);
    let (reply, detail) = declared(&r);
    assert_eq!(reply, "rejected-not-primary", "{r}");
    assert!(detail.contains("ReceiverNotPrimary"), "{r}");
    assert_eq!(estate.node_value(&rdm_core::PathName::new("mesh1.rpc", "1").unwrap()).await["declared"], "Ready", "a refusal applies nothing");

    // 5. Forward moves apply and the view follows.
    assert_eq!(declared(&estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "connections-deleting"])).0, "applied");
    assert_eq!(declared(&estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "stopped"])).0, "applied");
    assert_eq!(estate.node_value(&rdm_core::PathName::new("mesh1.rpc", "1").unwrap()).await["declared"], "Stopped");
    assert_eq!(estate.node_value(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await["declared"], "Ready", "rpc.2 declared only its birth");

    estate.artifact("declarations.json", &json!({"primary": primary["name"], "other_admin": other_admin["name"], "node": rpc1["name"]}));
    estate.stop().await;

    // Evidence: every decision is one span on the authority, naming op, sender and outcome.
    let spans = estate.spans();
    let decided = named(&spans, "rdm.node_admin.status.update.via-declaration");
    let on_primary = |outcome: &str| decided.iter().filter(|s| actor_is(s, &primary) && s["attributes"]["outcome"] == outcome && s["attributes"]["sender"] == rpc1["node_id"]).count();
    assert_eq!(on_primary("applied"), 3, "RFT (by the node at birth), ConnectionsDeleting, Stopped: {decided:?}");
    assert!(on_primary("already-applied") >= 1, "the oracle's repeat, and the node's own ConnectionsDeleting/Stopped at stop: {decided:?}");
    // The oracle's Pending after Ready; and, once the oracle moved the node to Stopped, the
    // node's own ConnectionsDeleting at stop is a backward move too: refused by name, never regressing.
    let backward: Vec<_> = decided.iter().filter(|s| actor_is(s, &primary) && s["attributes"]["outcome"] == "rejected-invalid-node-transition" && s["attributes"]["sender"] == rpc1["node_id"]).collect();
    assert!(backward.iter().any(|s| s["attributes"]["detail"] == "Ready"), "Pending after Ready: {backward:?}");
    assert!(backward.iter().all(|s| s["attributes"]["detail"] == "Ready" || s["attributes"]["detail"] == "Stopped"), "{backward:?}");
    assert_eq!(on_primary("rejected-stale-incarnation"), 1);
    assert_eq!(on_primary("rejected-not-primary"), 1, "subject-unknown on the primary");
    assert!(decided.iter().filter(|s| actor_is(s, &primary) && s["attributes"]["outcome"] == "rejected-not-primary").all(|s| s["attributes"]["detail"] == "subject-unknown"), "the primary refuses no caller: {decided:?}");
    // The explicit probe sent to the non-primary admin receives that admin’s own role decision.
    let not_primary: Vec<_> = decided.iter().filter(|s| actor_is(s, &other_admin) && s["attributes"]["outcome"] == "rejected-not-primary").collect();
    assert!(not_primary.iter().any(|s| s["attributes"]["sender"] == rpc1["node_id"] && s["attributes"]["detail"] == "receiver-not-primary"), "{not_primary:?}");
    // A birth's declaration that reaches the non-primary admin before its view holds the birth names a
    // subject it does not hold yet.
    assert!(not_primary.iter().all(|s| s["attributes"]["detail"] == "receiver-not-primary" || s["attributes"]["detail"] == "subject-unknown"), "{not_primary:?}");
    // Nodes and admins declare their own state; the mesh primary declares its Mesh's; the Day-0
    // root applies its own mesh's Pending, having no upstream authority (e4.s11).
    assert!(decided.iter().all(|s| ["declare-node-state", "declare-mesh-state", "apply-fabric-event", "apply-mesh-state"].contains(&s["attributes"]["op"].as_str().unwrap_or(""))), "{decided:?}");
    let root_pending: Vec<_> = decided.iter().filter(|s| s["attributes"]["op"] == "apply-mesh-state").collect();
    assert_eq!(root_pending.len(), 1, "exactly one Pending, self-applied by the Day-0 root: {root_pending:?}");
    let bootstrap = nodes.iter().find(|n| n["name"]["path"] == "mesh1.admin" && n["name"]["name"] == "1").expect("bootstrap fixture");
    assert_eq!(root_pending[0]["attributes"]["sender_node_id"], bootstrap["node_id"], "{root_pending:?}");
    assert!(actor_is(root_pending[0], bootstrap), "the bootstrap applies its own Pending: {root_pending:?}");
    assert_eq!(root_pending[0]["attributes"]["receiver_is_primary"], "false", "applied before any election: {root_pending:?}");
    // The other admin's own birth was applied by its mesh's primary (this primary) too.
    assert!(decided.iter().any(|s| s["attributes"]["sender"] == other_admin["node_id"] && s["attributes"]["outcome"] == "applied"), "{decided:?}");
    // And the primary's own birth: the sender is the authority, through the same door.
    assert!(
        decided.iter().any(|s| actor_is(s, &primary) && s["attributes"]["sender"] == primary["node_id"] && s["attributes"]["op"] == "declare-node-state" && s["attributes"]["outcome"] == "applied"),
        "the authority applied its own declaration to itself: {decided:?}"
    );
}

/// CONTRACT (R24, Luke Q11: "dispatch the request to its implementation; if this endpoint has no
/// implementation for it, answer request not handled here"): an rpc node's Status door has no handler
/// for an upward declaration. The declare probe makes `mesh1.rpc.2` declare its state straight to
/// `mesh1.rpc.1`, which answers `RequestNotHandled` naming the request (`declare-node-state`), decided by
/// dispatch alone, on its reject span `rdm.node.status.reject.via-request-not-handled` (`request`, and
/// `node` the receiver's held name) in the probe's own trace. Must NOT happen: `RejectedNotPrimary {
/// ReceiverNotPrimary }`, a missing handler answered as an authority refusal.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_reaching_an_rpc_node_is_answered_not_handled_with_its_reject_span() {
    let mut estate = Estate::bootstrap(owner("a_declaration_reaching_an_rpc_node_is_answered_not_handled_with_its_reject_span"), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.settles_to(&[("mesh1", 2, 2)], Duration::from_secs(30)).await;
    let (rpc1, rpc2) = (estate.node_value(&rdm_core::PathName::new("mesh1.rpc", "1").unwrap()).await, estate.node_value(&rdm_core::PathName::new("mesh1.rpc", "2").unwrap()).await);
    let node = format!("exact:{}", rpc2["node_id"].as_str().unwrap());
    let to = format!("exact:{}", rpc1["node_id"].as_str().unwrap());
    let r = estate.probe(&["declare", "--target", &node, "--to", &to, "--state", "connections-deleting"]);
    assert_eq!(declared(&r).0, "request-not-handled", "an rpc node answers a declaration it has no handler for not handled here: {r}");
    estate.stop().await;
    let spans = estate.spans();
    let rejected: Vec<&Value> = named(&spans, "rdm.node.status.reject.via-request-not-handled").into_iter().filter(|sp| sp["attributes"]["node"] == rpc1["node_id"]).collect();
    assert_eq!(rejected.len(), 1, "mesh1.rpc.1 answers the one declaration on its reject span: {rejected:#?}");
    assert_eq!(rejected[0]["attributes"]["request"], "declare-node-state", "{}", rejected[0]);
    let trace = rejected[0]["trace_id"].as_str().unwrap_or_default().to_string();
    assert!(r["traceparent"].as_str().is_some_and(|tp| tp.contains(&trace)), "the reject span is in the probe's trace: {r} {}", rejected[0]);
    println!("request not handled at an rpc node: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}

/// CONTRACT (i146 Q39: "A node's declaration is owed to the primary of its mesh's primary role,
/// resolved from the role, never to an admin its view happens to hear"; R24, Luke Q11: an admin holding
/// no role answers a declaration with its own role decision): `mesh1.admin.1` yields both roles to
/// `mesh1.admin.2` (`node.stop` runs `node.primary.update` first) and starts again as itself, holding no
/// role. `mesh1.rpc.1`, restarted, declares its state to `mesh1.admin.2`, the role's primary, which takes
/// it (`definitive=true`), and to no other admin. A declaration sent straight to `mesh1.admin.1` (the
/// declare probe) is answered `RejectedNotPrimary { ReceiverNotPrimary { needed: "mesh-primary of
/// mesh1" } }`, its own role decision (`status_rpc.rs`'s `decide`). Must NOT happen: a declaration
/// addressed to the admin holding no role, or `RequestNotHandled` from a node-admin for a declaration.
/// The restarted node's start declares Ready as an owed message naming the start (node-start.md step 5);
/// Q93 supersedes Q47's line "the open start sends no second Ready declaration".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_declares_to_its_mesh_primary_and_a_secondary_admin_answers_its_own_decision() {
    let test = "a_node_declares_to_its_mesh_primary_and_a_secondary_admin_answers_its_own_decision";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    estate.settles_to(&[("mesh1", 2, 2)], Duration::from_secs(30)).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let (a1, a2, rpc1) = (Estate::path("mesh1.admin", "1"), Estate::path("mesh1.admin", "2"), Estate::path("mesh1.rpc", "1"));
    estate.enter(&a2).await;
    let mut stop = estate.nodes_obj().await.stop(&a1).await.expect("node.stop of mesh1.admin.1");
    let accepted = stop.accepted().clone();
    if matches!(Estate::frames_of(&mut stop).await.last(), Some(rdm_client::Frame::FabricPrimaryMoved { .. })) {
        estate.resume(&accepted.build_id, accepted.attempt).await;
    }
    estate.until_node(&rdm_core::PathName::new("mesh1.admin", "2").unwrap(), "mesh1.admin.2 holds the roles", Duration::from_secs(60), |n| n.filter(|n| n.is_primary && n.is_fabric_primary).map(|_| ())).await;
    Estate::workflow("node.start of mesh1.admin.1", estate.nodes_obj().await.start(&a1).await).await;
    estate.until_node(&rdm_core::PathName::new("mesh1.admin", "1").unwrap(), "mesh1.admin.1 runs again holding no primary role", Duration::from_secs(60), |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready && !n.stopped && !n.is_primary).map(|_| ())).await;
    let before_rpc = estate.node_view(&rpc1).await;
    let restart_at = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    estate.stop_then_start(&rpc1).await;
    estate.until_node(&rdm_core::PathName::new("mesh1.rpc", "1").unwrap(), "mesh1.rpc.1 is ready again", Duration::from_secs(60), |n| n.filter(|n| n.status == rdm_client::NodeStatus::Ready && !n.stopped).map(|_| ())).await;

    // A declaration straight to the admin holding no role: its own role decision.
    let (rpc1v, a1v, a2v) = (estate.node_value(&rdm_core::PathName::new("mesh1.rpc", "1").unwrap()).await, estate.node_value(&rdm_core::PathName::new("mesh1.admin", "1").unwrap()).await, estate.node_value(&rdm_core::PathName::new("mesh1.admin", "2").unwrap()).await);
    assert_eq!(rpc1v["node_id"], before_rpc.node_id.as_str(), "stop/start keeps the node ID");
    assert_eq!(rpc1v["incarnation_id"], serde_json::to_value(&before_rpc.incarnation_id).unwrap(), "stop/start keeps the incarnation");
    let primary_id = a2v["node_id"].as_str().unwrap().to_string();
    let node = format!("exact:{}", rpc1v["node_id"].as_str().unwrap());
    let non_primary = format!("exact:{}", a1v["node_id"].as_str().unwrap());
    let r = estate.probe(&["declare", "--target", &node, "--to", &non_primary, "--state", "ready"]);
    let (reply, detail) = declared(&r);
    assert_eq!(reply, "rejected-not-primary", "the admin holding no primary role answers by its role decision: {r}");
    assert!(detail.contains("ReceiverNotPrimary") && detail.contains("mesh-primary of mesh1"), "{r}");
    estate.stop().await;

    let spans = estate.spans();
    // An open start reports on its command stream to the primary, and also declares Ready to it as an
    // owed message naming the start (node-start.md step 5; Q93 supersedes Q47's line "the open start
    // sends no second Ready declaration"); frame and declaration apply once (Q49).
    let started: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-command-frame")
        .into_iter()
        .filter(|sp| sp["attributes"]["node_id"] == rpc1v["node_id"] && sp["attributes"]["incarnation_id"] == rpc1v["incarnation_id"] && sp["attributes"]["frame"] == "node-started" && sp["attributes"]["operation"] == "node.start" && sp["start_unix_nano"].as_u64().unwrap_or(0) >= restart_at)
        .collect();
    assert!(!started.is_empty(), "the primary records the start's terminal: {started:#?}");
    assert!(started.iter().all(|sp| sp["attributes"]["recorder_node_id"] == primary_id), "only the primary records it: {started:#?}; holder {primary_id}");
    let declared_own: Vec<&Value> = named(&spans, "rdm.node_rpc.status.update.via-declare-own")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == rpc1v["node_id"] && sp["attributes"]["operation"] == "node.start" && sp["start_unix_nano"].as_u64().unwrap_or(0) >= restart_at)
        .collect();
    assert!(!declared_own.is_empty(), "the start declares its Ready naming the start: {declared_own:#?}");
    assert!(declared_own.iter().all(|sp| sp["attributes"]["to"] == primary_id), "the start's declaration goes to the role's primary and to no other admin: {declared_own:#?}; holder {primary_id}");
    assert!(declared_own.iter().any(|sp| sp["attributes"]["definitive"] == "true" && ["applied", "already-applied"].contains(&sp["attributes"]["outcome"].as_str().unwrap_or_default())), "the primary takes it, applied or already applied beside the frame: {declared_own:#?}");
    let trace = r["traceparent"].as_str().and_then(|tp| tp.split('-').nth(1)).unwrap_or_default().to_string();
    println!("a declaration at the admin holding no primary role: http://192.168.68.99:16687/trace/{trace}");
    estate.record_trace_url(&trace);
}
