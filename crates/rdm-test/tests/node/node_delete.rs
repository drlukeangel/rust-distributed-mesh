//! i143 process E2E: removing one node is a lifecycle op with two events (i143.e6.s10).
//!
//! From public surfaces and span evidence only, on an MM estate: mesh1 (two admins, two rpc
//! nodes) and mesh2 (one admin, two rpc nodes). `node.delete` of mesh2.rpc.2 reaches the
//! fabric-primary, which accepts a Build without the node; mesh2's primary claims the attempt
//! and executes the deletion.
//!
//! 1. Before the executor starts, the node is routable everywhere. Once it holds the operation
//!    it publishes `NodeDeleting`: the node stays found and live in every view and stops being
//!    routable. Only after the provider proved the runtime terminal does it publish
//!    `NodeDeleted`: a peer mesh's rpc node then answers `Gone` for the old id, `Unknown` for
//!    the path (no holder) and for an id nobody saw, and still `Found` for the surviving node.
//! 2. A node born after the events learns the departure from the retained mesh topology, never
//!    having heard an event.
//! 3. The pre-notice is never emitted before the executor's claim, the departure never before
//!    the terminal proof, and the departure is not drift: nothing is re-born.
//! 4. The delete releases identity, not storage (node-delete.md "Storage": "Retain data dir and
//!    endpoint key; a later spec can inherit them"): the victim's data dir and its endpoint key
//!    stay, and no step of the delete releases storage.

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, ScopeStatus};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::time::Duration;

const VICTIM: &str = "mesh2.rpc.2";
const WITNESS_NODE: &str = "mesh1.rpc.1";
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
    let shape = |a1: u32, r1: u32, a2: u32, r2: u32| FabricDesired {
        fabric: "fabric1".into(),
        meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, a1), (NodeKind::RpcNode, r1)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, a2), (NodeKind::RpcNode, r2)])],
    };
    estate.reconcile(&shape(2, 2, 1, 2)).await;
    let before = Estate::values(&estate.settled_views(&[("mesh1", 2, 2), ("mesh2", 1, 2)]).await);
    let victim = before.iter().find(|n| n["name"] == VICTIM).cloned().unwrap();
    let (victim_id, victim_inc) = (s(&victim["node_id"]), s(&victim["incarnation_id"]));
    let exact_victim = format!("exact:{victim_id}");
    estate.artifact("nodes-before.json", &json!(before));

    // The peer mesh's rpc node holds mesh2's nodes through the forwarded mesh topology.
    estate.witness_holds_birth(WITNESS_NODE, VICTIM).await;
    let found = estate.probe(&["resolve", "--target", WITNESS, "--query", &exact_victim]);
    assert_eq!(resolution(&found), "found", "{found}");
    assert_eq!(found["reply"]["incarnation_id"], victim["incarnation_id"], "{found}");
    // States.md "Routing": application routing selects a birth only while it is ready and its
    // mesh is ready.
    let meshes = estate.fabric_view().await.meshes;
    assert!(before.iter().all(|n| n["status"] == "ready") && meshes.iter().all(|m| m.status == ScopeStatus::Ready), "every live node is routable before the delete: {before:#?} {meshes:?}");
    // The victim's data dir, read while an admin still advertises it.
    let victim_dir = std::path::PathBuf::from(estate.data_dir(VICTIM).await);
    assert!(victim_dir.join("node-key").is_file(), "the victim's data dir holds its endpoint key before the delete: {victim_dir:?}");

    // The initial fabric refuses the removal of a node until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // 1. The removal.
    let mut removal = estate.nodes_obj().await.delete(&VICTIM.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of {VICTIM}: {e}"));
    // The pre-notice is read from each receiver's overlay span below, not from a poll: the window
    // in which the node is live and not routable is the stop leg, and a poll's cadence
    // is not evidence of it.
    let build_id = Estate::complete(&mut removal).await.build_id.to_string();
    let after = Estate::values(&estate.settled_views(&[("mesh1", 2, 2), ("mesh2", 1, 1)]).await);
    assert!(after.iter().all(|n| n["name"] != VICTIM), "the current topology no longer lists the node: {after:#?}");
    estate.artifact("nodes-after.json", &json!(after));
    // 4. node-delete.md step 3: "Retain the data dir, node-identity.json and endpoint key."
    assert!(victim_dir.is_dir(), "the delete keeps the victim's data dir: {victim_dir:?}");
    assert!(victim_dir.join("node-key").is_file(), "the delete keeps the victim's endpoint key in its data dir: {victim_dir:?}");

    // Every ordinary node of the peer mesh, not only its primary, holds mesh2 at the version that
    // carries the departure (gossip.md §3.3, Q130): a node whose pull came back older than a delta it
    // dropped while the pull was in flight pulls again until it does.
    for node in after.iter().filter(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node").map(|n| s(&n["name"])) {
        estate.holds_departure(&node, "mesh2", &victim_id).await;
        let held = estate.probe(&["resolve", "--target", &format!("path:{node}"), "--query", &exact_victim]);
        assert_eq!(resolution(&held), "gone", "{node} holds the departure of {VICTIM}: {held}");
    }
    let gone = estate.probe(&["resolve", "--target", WITNESS, "--query", &exact_victim]);
    assert_eq!(resolution(&gone), "gone", "{gone}");
    let path = estate.probe(&["resolve", "--target", WITNESS, "--query", &format!("path:{VICTIM}")]);
    assert_eq!(resolution(&path), "unknown", "the path has no holder: {path}");
    let survivor = estate.probe(&["resolve", "--target", WITNESS, "--query", "path:mesh2.rpc.1"]);
    assert_eq!(resolution(&survivor), "found", "the surviving node is untouched: {survivor}");
    let never = estate.probe(&["resolve", "--target", WITNESS, "--query", &format!("exact:{}", rdm_core::NodeId::mint())]);
    assert_eq!(resolution(&never), "unknown", "{never}");
    estate.artifact("resolutions.json", &json!({"gone": gone, "path": path, "survivor": survivor, "never": never}));

    // 2. A node born after the events learns the departure from the retained mesh topology.
    let mut spawn = estate.create_node("mesh1", NodeKind::RpcNode).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of an rpc node in mesh1: {e}"));
    Estate::complete(&mut spawn).await;
    let late = Estate::values(&estate.settled_views(&[("mesh1", 2, 3), ("mesh2", 1, 1)]).await);
    let newcomer = late.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node" && before.iter().all(|b| b["node_id"] != n["node_id"])).cloned().unwrap();
    estate.holds_departure(&s(&newcomer["name"]), "mesh2", &victim_id).await;
    let late_gone = estate.probe(&["resolve", "--target", &format!("path:{}", s(&newcomer["name"])), "--query", &exact_victim]);
    assert_eq!(resolution(&late_gone), "gone", "the newcomer holds the departure it never heard as an event: {late_gone}");
    // And one born into the executor's own mesh, which learns it from its own primary's
    // overlays rather than a forwarded mesh topology.
    let mut spawn2 = estate.create_node("mesh2", NodeKind::RpcNode).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of an rpc node in mesh2: {e}"));
    Estate::complete(&mut spawn2).await;
    let late2 = Estate::values(&estate.settled_views(&[("mesh1", 2, 3), ("mesh2", 1, 2)]).await);
    let newcomer2 = late2.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "rpc_node" && before.iter().all(|b| b["node_id"] != n["node_id"])).cloned().unwrap();
    estate.holds_departure(&s(&newcomer2["name"]), "mesh2", &victim_id).await;
    let own_mesh_gone = estate.probe(&["resolve", "--target", &format!("path:{}", s(&newcomer2["name"])), "--query", &exact_victim]);
    assert_eq!(resolution(&own_mesh_gone), "gone", "the newcomer in the executor's own mesh holds the departure: {own_mesh_gone}");
    estate.artifact("late-joiner.json", &json!({"peer_mesh": late_gone, "own_mesh": own_mesh_gone}));

    estate.stop().await;
    // 3. Cut points and the absence of drift.
    let spans = estate.spans();
    let start = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let deleting: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-deleting").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).collect();
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).collect();
    assert_eq!(deleting.len(), 1, "one pre-notice for one deletion: {deleting:?}");
    assert_eq!(deleted.len(), 1, "one departure for one deletion: {deleted:?}");
    // The node's row (ops-naming "Node data", the delete point; R13): the mesh's primary, which
    // executes the delete, writes it with status Deleted at its NodeDeleted from the exact record it
    // acted on; the node born after it at the same path.name claims the row there at its create,
    // seeded from the Deleted row.
    let row_deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == VICTIM).collect();
    assert_eq!(row_deleted.len(), 1, "one Deleted row for one delete: {row_deleted:?}");
    let a = &row_deleted[0]["attributes"];
    assert_eq!((s(&a["outcome"]), s(&a["node_id"]), s(&a["node"])), ("row-deleted".to_string(), victim_id.clone(), "mesh2.admin.1".to_string()), "{a}");
    let row_seeded: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-node-row").into_iter().filter(|sp| sp["attributes"]["subject"] == VICTIM && sp["attributes"]["node_id"] == newcomer2["node_id"] && sp["attributes"]["moment"] == "claim").collect();
    assert_eq!(row_seeded.len(), 1, "the newcomer at {VICTIM} claims its row once: {row_seeded:?}");
    let a = &row_seeded[0]["attributes"];
    assert_eq!((s(&a["outcome"]), s(&a["seeded_from"])), ("row-seeded".to_string(), victim_id.clone()), "{a}");
    estate.artifact("node-rows.json", &json!({"deleted": row_deleted[0], "seeded": row_seeded[0]}));
    assert_eq!(deleting[0]["attributes"]["build_id"], build_id);
    assert_eq!(deleted[0]["attributes"]["operation"], format!("node.delete:{VICTIM}"));
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], victim_inc, "the departure names the exact birth");
    let executor_claim = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .filter(|sp| sp["attributes"]["build_id"] == build_id && sp["attributes"]["executor"] == "mesh2.admin.1")
        .map(start)
        .min()
        .expect("mesh2's primary claimed the removal");
    assert!(start(deleting[0]) > executor_claim, "the pre-notice follows the executor's claim");
    let terminal = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["node"] == VICTIM && sp["attributes"]["outcome"] == "complete")
        .map(start)
        .max()
        .expect("the runtime was terminated and inspected");
    assert!(start(deleted[0]) > terminal, "the departure follows the terminal proof");
    assert!(start(deleting[0]) < start(deleted[0]));
    // The stop leg (node-stop.md steps 2-3): after the pre-notice the executor's stop-node reached the
    // exact birth (the victim's own `via-stop-node`); inside it the victim ran its whole
    // node.connection.delete { All }, then its own transport cut; its reply `stopped` is the
    // stop's completion (no node-left call is made), all before the terminal proof.
    let stop_span = named(&spans, "rdm.node_admin.status.update.via-stop-node")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == VICTIM)
        .min_by_key(|sp| start(sp))
        .cloned()
        .expect("the victim served stop-node");
    let stopped_at = start(&stop_span);
    assert_eq!(s(&stop_span["attributes"]["receipt"]), "established", "the victim's All let its in-flight work reach zero: {stop_span}");
    // The victim's stop, never the signal stop the estate's teardown runs on a node born later at its path.
    let all = named(&spans, "rdm.node_admin.connection.delete.via-stop")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == VICTIM && sp["attributes"]["operation"] != "signal")
        .cloned()
        .collect::<Vec<Value>>();
    assert_eq!(all.len(), 1, "the victim's stop ran its node.connection.delete {{ All }} once: {all:#?}");
    assert_eq!(all[0]["parent_span_id"], stop_span["span_id"], "the All is the stop's own step, inside its via-node.stop: {}", all[0]);
    let all_at = start(&all[0]);
    let all_end = all[0]["end_unix_nano"].as_u64().unwrap_or(u64::MAX);
    assert!(all_at > start(deleting[0]) && all_at < terminal, "the All lands between the pre-notice and the terminal proof");
    let cut_at = named(&spans, "rdm.node_admin.node.update.via-stop-transport-cut")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == VICTIM && sp["attributes"]["operation"] != "signal")
        .map(start)
        .min()
        .expect("the victim's stop cut its own transport");
    // `left` is the reply of the stop-node call itself: the command-sent span records `stopped` when the
    // reply arrived, and no node-left call is made.
    let left_at = named(&spans, "rdm.node_admin.node.update.via-command-sent")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == VICTIM && sp["attributes"]["command"] == "stop-node" && sp["attributes"]["admission"] == "stopped")
        .map(start)
        .min()
        .expect("the victim answered stop-node stopped");
    assert!(named(&spans, "rdm.node_admin.status.update.via-completion-accepted").into_iter().all(|sp| sp["attributes"]["op"] != "node-left"), "no node-left call is made: left rides the stop reply");
    assert!(stopped_at < all_at && all_end <= cut_at, "the transport cut follows the whole All: stop {stopped_at} All {all_at}..{all_end} cut {cut_at}");
    assert!(left_at < terminal, "left is answered before the terminal proof: left {left_at} terminal {terminal}");
    // The mesh primary publishes the departure once, from the victim's declaration of its stop, which
    // the victim sends after its transport cut and before its reply (node-stop.md steps 4-5, i146 Q36),
    // and before the runtime exits.
    let published: Vec<u64> = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).map(start).collect();
    assert_eq!(published.len(), 1, "NodeStopped is published once for the victim: {published:?}");
    assert!(cut_at <= published[0] && published[0] < terminal, "NodeStopped follows the victim's transport cut and precedes the terminal proof: cut {cut_at} published {} terminal {terminal}", published[0]);
    let victim_steps: Vec<String> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM && sp["attributes"]["build_id"] == build_id).map(|sp| s(&sp["attributes"]["step"])).collect();
    assert!(!victim_steps.iter().any(|st| st == "ReleaseStorage"), "no step of the delete releases storage: {victim_steps:?}");
    // The All is the node's own, inside its stop: the delete composes no separate DeleteConnections step.
    assert!(!victim_steps.iter().any(|st| st == "DeleteConnections"), "the delete composes no DeleteConnections step beside the stop: {victim_steps:?}");
    for step in ["StopNode", "AwaitNodeLeft"] {
        let done = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .find(|sp| sp["attributes"]["step"] == step && sp["attributes"]["node"] == VICTIM && sp["attributes"]["outcome"] == "complete")
            .unwrap_or_else(|| panic!("{step} completed on the executor"));
        assert!(start(done) < terminal, "{step} precedes termination");
    }
    // Stopping reaches peers as the victim's status in its mesh-primary's projection, set on the stop
    // call's admission (states.md "Who sets each state"); node-leaving and node-left are reserved frame
    // positions (gossip.md frame catalog): nobody publishes or hears either, and nothing publishes
    // node-connections-deleting or node-connections-deleted.
    for kind in ["node-left", "node-leaving"] {
        assert_eq!(named(&spans, "rdm.mesh.topology.update.via-lifecycle-frame").into_iter().filter(|sp| sp["attributes"]["kind"] == kind && sp["attributes"]["subject"] == VICTIM).count(), 0, "no {kind} frame of the victim is heard by anyone");
    }
    assert!(named(&spans, "rdm.mesh.topology.reject.via-node-leaving").is_empty(), "no node-leaving frame reached any receiver");
    for kind in ["node-connections-deleting", "node-connections-deleted"] {
        let reserved = named(&spans, "rdm.mesh.topology.update.via-lifecycle-frame").into_iter().filter(|sp| sp["attributes"]["kind"] == kind && sp["attributes"]["subject"] == VICTIM).count();
        assert_eq!(reserved, 0, "no {kind} frame of the victim is published or heard");
    }
    let stop_calls: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-completion-call").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM && ["node-left", "node-connections-deleted"].iter().any(|c| sp["attributes"]["completion"] == *c)).collect();
    assert!(stop_calls.is_empty(), "the victim makes no completion call for its stop, its reply is the completion: {stop_calls:#?}");
    let heard_deleting = named(&spans, "rdm.mesh.topology.update.via-node-deleting").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).count();
    let heard_deleted = named(&spans, "rdm.mesh.topology.remove.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).count();
    // The victim's own mesh hears the pre-notice on its channel. A peer mesh gets it only inside a backbone
    // mesh topology while the operation is open (`in_flight`: "while the operation is claimed and has no
    // completed receipt", gossip.md MeshTopology): a deletion that completes within one mesh topology interval
    // reaches it as the retained departure alone, which `heard_deleted` below covers for every node.
    assert!(heard_deleting >= 2, "the victim's own mesh heard the pre-notice: {heard_deleting}");
    // Each receiver's overlay install shows the victim found, live and not routable as the overlay
    // opens in its view (so before that receiver's own removal of it).
    let overlays: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-node-deleting").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).collect();
    let held: Vec<&&Value> = overlays.iter().filter(|sp| sp["attributes"]["held"] == "true").collect();
    assert!(held.len() >= 2, "receivers that held the victim when the overlay opened: {}: {overlays:#?}", held.len());
    for sp in &held {
        assert_eq!(sp["attributes"]["routable"], "false", "the overlay made the held victim not routable: {sp}");
        assert!(!matches!(sp["attributes"]["held_status"].as_str(), Some("Dead" | "PendingReconnect")), "the victim is not dead or unheard while the overlay opens: {sp}");
    }
    assert!(heard_deleted >= 5, "every other node heard the departure: {heard_deleted}");
    assert!(named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| start(sp) > start(deleting[0])).next().is_none(), "a departure is not drift: nothing is repaired");
    let mut births: Vec<String> = named(&spans, "rdm.node_admin.node.create.via-build").into_iter().filter(|sp| start(sp) > start(deleting[0])).map(|sp| s(&sp["attributes"]["node"])).collect();
    births.sort();
    let mut asked = vec![s(&newcomer["name"]), s(&newcomer2["name"])];
    asked.sort();
    assert_eq!(births, asked, "the only births after the delete are the two asked for");
}

fn unix_nanos() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).expect("after the Unix epoch").as_nanos() as u64
}

/// CONTRACT (node-delete.md step 4: "The surviving executor emits node.deleted on the caller stream
/// from the Exited evidence and records the complete delete receipt. The exited node cannot publish
/// its own terminal proof or serve that stream"; "NodeDeleting / NodeDeleted are peer
/// dissemination"): mesh1 (one admin, one rpc node) and mesh2 (two admins, two rpc nodes), every
/// node-admin the testkit's faulted node-admin. `node.delete` of mesh2.rpc.2 is executed by mesh2's
/// primary P. The fault door holds P at the `deleted` event cut: the victim's runtime is proven
/// terminal and the NodeDeleted receipt (its row Deleted inside) is durable, and NodeDeleted is not
/// yet published. Once mesh2's other admin NA2 holds that receipt in its own Build log, P is killed
/// at the hold. The surviving executor publishes NodeDeleted for the victim's exact birth, the peer
/// mesh's rpc node retains the departure, and the removal's caller stream ends Complete. Must NOT
/// happen: the deleted birth held in a view for good because the one executor that would have
/// published its departure exited between the receipt and the publish.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_delete_whose_executor_exits_between_its_deleted_receipt_and_the_publish_is_published_by_the_surviving_executor() {
    const SETTLE: Duration = Duration::from_secs(120);
    let test = "a_delete_whose_executor_exits_between_its_deleted_receipt_and_the_publish";
    let sha = candidate_sha();
    let estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let shape = [("mesh1", 1, 1), ("mesh2", 2, 2)];
    estate.reconcile_shape("fabric1", &shape).await;
    estate.settles_to(&shape, Duration::from_secs(90)).await;
    // The initial fabric refuses the removal of a node until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(SETTLE).await;
    let nodes = estate.node_values().await;
    let fp = s(&nodes.iter().find(|n| n["is_fabric_primary"] == true).expect("a fabric-primary")["name"]);
    assert!(!fp.starts_with("mesh2."), "the fabric-primary is not in mesh2: {fp}");
    let p = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().expect("mesh2's primary");
    let p_name = s(&p["name"]);
    let na2 = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["name"] != p["name"]).expect("mesh2's other node-admin")["name"]);
    let victim = nodes.iter().find(|n| n["name"] == VICTIM).cloned().expect("the victim");
    let (victim_id, victim_inc) = (s(&victim["node_id"]), s(&victim["incarnation_id"]));
    estate.witness_holds_birth(WITNESS_NODE, VICTIM).await;

    let door = Door::at(&estate, &p_name).await;
    door.arm("deleted-unpublished", json!({"kind": "event", "event": "deleted", "node": VICTIM})).await;
    let mut removal = estate.nodes_obj().await.delete(&VICTIM.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of {VICTIM}: {e}"));
    let held = door.wait_held("deleted-unpublished").await;
    let (build, attempt) = (s(&held["hit"]["build_id"]), held["hit"]["attempt"].as_u64().expect("the held event names its attempt"));
    assert_eq!((s(&held["hit"]["operation"]), s(&held["hit"]["incarnation_id"])), (format!("node.delete:{VICTIM}"), victim_inc.clone()), "P holds the victim's NodeDeleted publish: {held}");
    // NA2 heard the NodeDeleted receipt on the Build topic: in its own log the receipt is durable.
    let deleted_receipt = |b: &Value| {
        let mut steps = b["steps"].as_array().cloned().unwrap_or_default();
        for lane in b["lanes"].as_object().map(|l| l.values().cloned().collect::<Vec<_>>()).unwrap_or_default() {
            steps.extend(lane["steps"].as_array().cloned().unwrap_or_default());
        }
        steps.iter().any(|r| r["operation"] == format!("node.delete:{VICTIM}").as_str() && r["step"] == "NodeDeleted" && r["attempt"] == json!(attempt) && r["outcome"] == "complete")
    };
    crate::build_log::build_fact(&estate.carrier_at(&Estate::path(&na2)).await, &build, &format!("the NodeDeleted receipt of {VICTIM} at attempt {attempt}"), SETTLE, |pr| deleted_receipt(&serde_json::to_value(pr).expect("a Build view serializes")))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    let killed_at = unix_nanos();
    estate.kill_node(&p_name).await;

    // The surviving executor publishes the departure of the exact birth.
    let published = wait_for(&format!("a surviving executor publishes NodeDeleted for {VICTIM} ({victim_id}) after {p_name} exited"), SETTLE, || async {
        named(&estate.spans(), "rdm.node_admin.node.delete.via-node-deleted")
            .into_iter()
            .find(|sp| sp["attributes"]["node_id"] == victim_id.as_str() && sp["start_unix_nano"].as_u64().unwrap_or(0) > killed_at)
            .cloned()
    })
    .await;
    assert_eq!((s(&published["attributes"]["incarnation_id"]), s(&published["attributes"]["build_id"])), (victim_inc.clone(), build.clone()), "the departure names the exact birth and the removal's Build: {published}");
    estate.holds_departure(WITNESS_NODE, "mesh2", &victim_id).await;
    let end = Estate::complete(&mut removal).await;
    eprintln!(
        "s78 B: {p_name} held at the deleted cut of {VICTIM} (attempt {attempt} of {build}) and killed; {na2} survives; NodeDeleted published trace {} span {} attempt {}; {WITNESS_NODE} holds the departure; removal complete {:?}",
        s(&published["trace_id"]), s(&published["span_id"]), s(&published["attributes"]["attempt"]), end.build_id,
    );
    estate.record_trace_url(published["trace_id"].as_str().unwrap_or(""));
    estate.shutdown().await;
}
