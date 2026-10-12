//! i143 process E2E: removing one node is a lifecycle op with two events (i143.e6.s10).
//!
//! From public surfaces and span evidence only, on an MM estate: mesh1 (two admins, two rpc
//! nodes) and mesh2 (one admin, two rpc nodes). `node.delete` of mesh2.rpc.2 reaches the
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
//! 4. The delete releases identity, not storage (node-delete.md "Storage": "Retain data dir and
//!    endpoint key; a later spec can inherit them"): the victim's data dir and its endpoint key
//!    stay, and no step of the delete releases storage.

use rafka_node_admin_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, NodeSpec, ScopeStatus};
use rafka_test_scenario::estate::{named, Estate, Owner};
use serde_json::{json, Value};

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

    // The peer mesh's rpc node holds mesh2's nodes through the forwarded aggregate.
    estate.witness_holds_birth(WITNESS_NODE, VICTIM).await;
    let found = estate.probe(&["resolve", "--target", WITNESS, "--query", &exact_victim]);
    assert_eq!(resolution(&found), "found", "{found}");
    assert_eq!(found["reply"]["incarnation_id"], victim["incarnation_id"], "{found}");
    // States.md "Routing": application routing selects a birth only while it is ready-for-traffic and its
    // mesh is ready-for-traffic.
    let meshes = estate.fabric_view().await.meshes;
    assert!(before.iter().all(|n| n["status"] == "ready-for-traffic") && meshes.iter().all(|m| m.status == ScopeStatus::ReadyForTraffic), "every live node is routable before the delete: {before:#?} {meshes:?}");
    // The victim's data dir, read while an admin still advertises it.
    let victim_dir = std::path::PathBuf::from(estate.data_dir(VICTIM).await);
    assert!(victim_dir.join("node-key").is_file(), "the victim's data dir holds its endpoint key before the delete: {victim_dir:?}");

    // 1. The removal.
    let mut removal = estate.nodes_obj().await.delete(&VICTIM.parse().unwrap()).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of {VICTIM}: {e}"));
    // The pre-notice is read from each receiver's overlay span below, not from a poll: the window
    // in which the node is live and not routable is the drain and stop legs, and a poll's cadence
    // is not evidence of it.
    let build_id = Estate::complete(&mut removal).await.build_id.0;
    let after = Estate::values(&estate.settled_views(&[("mesh1", 2, 2), ("mesh2", 1, 1)]).await);
    assert!(after.iter().all(|n| n["name"] != VICTIM), "the current topology no longer lists the node: {after:#?}");
    estate.artifact("nodes-after.json", &json!(after));
    // 4. node-delete.md step 3: "Retain the data dir, node-identity.json and endpoint key."
    assert!(victim_dir.is_dir(), "the delete keeps the victim's data dir: {victim_dir:?}");
    assert!(victim_dir.join("node-key").is_file(), "the delete keeps the victim's endpoint key in its data dir: {victim_dir:?}");

    estate.holds_departure(WITNESS_NODE, "mesh2", &victim_id).await;
    let gone = estate.probe(&["resolve", "--target", WITNESS, "--query", &exact_victim]);
    assert_eq!(resolution(&gone), "gone", "{gone}");
    let path = estate.probe(&["resolve", "--target", WITNESS, "--query", &format!("path:{VICTIM}")]);
    assert_eq!(resolution(&path), "unknown", "the path has no holder: {path}");
    let survivor = estate.probe(&["resolve", "--target", WITNESS, "--query", "path:mesh2.rpc.1"]);
    assert_eq!(resolution(&survivor), "found", "the surviving node is untouched: {survivor}");
    let never = estate.probe(&["resolve", "--target", WITNESS, "--query", &format!("exact:{}", rafka_mesh_entity::NodeId::mint())]);
    assert_eq!(resolution(&never), "unknown", "{never}");
    estate.artifact("resolutions.json", &json!({"gone": gone, "path": path, "survivor": survivor, "never": never}));

    // 2. A node born after the events learns the departure from the retained aggregate.
    let mut spawn = estate.nodes_obj().await.create(&NodeSpec { mesh: "mesh1".into(), kind: NodeKind::RpcNode }).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of an rpc node in mesh1: {e}"));
    Estate::complete(&mut spawn).await;
    let late = Estate::values(&estate.settled_views(&[("mesh1", 2, 3), ("mesh2", 1, 1)]).await);
    let newcomer = late.iter().find(|n| n["mesh"] == "mesh1" && n["kind"] == "rpc_node" && before.iter().all(|b| b["node_id"] != n["node_id"])).cloned().unwrap();
    estate.holds_departure(&s(&newcomer["name"]), "mesh2", &victim_id).await;
    let late_gone = estate.probe(&["resolve", "--target", &format!("path:{}", s(&newcomer["name"])), "--query", &exact_victim]);
    assert_eq!(resolution(&late_gone), "gone", "the newcomer holds the departure it never heard as an event: {late_gone}");
    // And one born into the executor's own mesh, which learns it from its own primary's
    // overlays rather than a forwarded aggregate.
    let mut spawn2 = estate.nodes_obj().await.create(&NodeSpec { mesh: "mesh2".into(), kind: NodeKind::RpcNode }).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.create of an rpc node in mesh2: {e}"));
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
    assert_eq!(deleting.len(), 1, "one pre-notice for one retirement: {deleting:?}");
    assert_eq!(deleted.len(), 1, "one departure for one retirement: {deleted:?}");
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
    assert_eq!(deleted[0]["attributes"]["operation"], format!("retire-node:{VICTIM}"));
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
    // The drain and stop legs (node-drain.md, node-stop.md): the executor's drain-node reached the
    // exact birth (the victim's own `via-drain-node`), after the pre-notice; the victim called
    // node-drained back and the executor accepted it; then stop-node, the victim's `via-stop-node`,
    // its node-left call accepted by the executor, all before the terminal proof.
    let drained_at = named(&spans, "rdm.node_admin.status.update.via-drain-node")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == VICTIM)
        .map(start)
        .min()
        .expect("the victim served drain-node");
    assert!(drained_at > start(deleting[0]) && drained_at < terminal, "the drain lands between the pre-notice and the terminal proof");
    let stopped_at = named(&spans, "rdm.node_admin.status.update.via-stop-node")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == VICTIM)
        .map(start)
        .min()
        .expect("the victim served stop-node");
    let node_drained_at = named(&spans, "rdm.node_admin.status.update.via-completion-accepted")
        .into_iter()
        .filter(|sp| sp["attributes"]["op"] == "node-drained" && sp["attributes"]["subject"] == victim_id.as_str() && sp["attributes"]["outcome"] == "applied")
        .map(start)
        .min()
        .expect("the executor accepted no node-drained from the victim");
    // `left` is the reply of the stop-node call itself: the command-sent span records `left` when the
    // reply arrived, and no node-left call is made.
    let left_at = named(&spans, "rdm.node_admin.node.update.via-command-sent")
        .into_iter()
        .filter(|sp| sp["attributes"]["node"] == VICTIM && sp["attributes"]["command"] == "stop-node" && sp["attributes"]["admission"] == "stopped")
        .map(start)
        .min()
        .expect("the victim answered stop-node left");
    assert!(named(&spans, "rdm.node_admin.status.update.via-completion-accepted").into_iter().all(|sp| sp["attributes"]["op"] != "node-left"), "no node-left call is made: left rides the stop reply");
    assert!(drained_at < node_drained_at && node_drained_at < stopped_at, "stop-node follows the accepted node-drained: drain {drained_at} drained {node_drained_at} stop {stopped_at}");
    assert!(stopped_at < left_at && left_at < terminal, "left is answered before the terminal proof: stop {stopped_at} left {left_at} terminal {terminal}");
    // The mesh primary publishes the departure from that reply, before the runtime exits.
    let published = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).map(start).min().expect("the executor published NodeStopped from the left reply");
    assert!(left_at <= published && published < terminal, "NodeStopped follows left and precedes the terminal proof");
    let victim_steps: Vec<String> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM && sp["attributes"]["build_id"] == build_id).map(|sp| s(&sp["attributes"]["step"])).collect();
    assert!(!victim_steps.iter().any(|st| st == "ReleaseStorage"), "no step of the delete releases storage: {victim_steps:?}");
    for step in ["DrainNode", "AwaitNodeDrained", "StopNode", "AwaitNodeLeft"] {
        let done = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .find(|sp| sp["attributes"]["step"] == step && sp["attributes"]["node"] == VICTIM && sp["attributes"]["outcome"] == "complete")
            .unwrap_or_else(|| panic!("{step} completed on the executor"));
        assert!(start(done) < terminal, "{step} precedes termination");
    }
    // The command hooks: node-draining and node-leaving, which the owning mesh-admin authors, are heard
    // by the other nodes of the fabric; the node authors no node-left frame (it gossips nothing for its
    // stop).
    assert_eq!(named(&spans, "rdm.mesh.membership.update.via-lifecycle-frame").into_iter().filter(|sp| sp["attributes"]["kind"] == "node-left" && sp["attributes"]["subject"] == VICTIM).count(), 0, "no node-left frame of the victim is heard by anyone");
    for kind in ["node-draining", "node-leaving"] {
        let heard = named(&spans, "rdm.mesh.membership.update.via-lifecycle-frame").into_iter().filter(|sp| sp["attributes"]["kind"] == kind && sp["attributes"]["subject"] == VICTIM && sp["attributes"]["node"] != VICTIM).count();
        assert!(heard >= 1, "{kind} for the victim was heard by another node: {heard}");
    }
    // The completion hook is the victim's own (gossip.md "Node-draining and node-leaving events": "The
    // node-drained upward call runs NodeDrained in the sending node after eligible in-flight work
    // reaches zero. The node publishes the event on its own mesh channel."). What the doc makes certain
    // is the publication and the call, not a peer's receipt: "the directed calls are the certainty"
    // (gossip.md §10), and NodeDrained rides no aggregate (§7: only NodeDeleting and NodeDeleted are
    // retained, in `in_flight` and `departed`; "No event is retried by the application and no second
    // delivery mechanism exists"). The victim's stop cuts its gossip neighbours right after the drain
    // (node-stop.md step 3), so a peer holds the frame only if iroh-gossip pushed it before the cut.
    // The victim takes its own frame on its mesh channel, hands it to gossip, and calls node-drained at
    // the executor, which accepted it (asserted above).
    let own_hook: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-lifecycle-frame")
        .into_iter()
        .filter(|sp| sp["attributes"]["kind"] == "node-drained" && sp["attributes"]["subject"] == VICTIM && sp["attributes"]["node"] == VICTIM)
        .collect();
    assert_eq!(own_hook.len(), 1, "the victim published its node-drained once: {own_hook:#?}");
    assert_eq!((s(&own_hook[0]["attributes"]["via"]), s(&own_hook[0]["attributes"]["forwarded_by"])), ("mesh-channel".to_string(), String::new()), "on its own mesh channel, as its author: {}", own_hook[0]);
    let call: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-completion-call").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM && sp["attributes"]["completion"] == "node-drained").collect();
    assert_eq!(call.len(), 1, "the victim made one node-drained completion call: {call:#?}");
    assert_eq!((s(&call[0]["attributes"]["hook"]), s(&call[0]["attributes"]["answer"])), ("published".to_string(), "applied".to_string()), "the hook went to gossip before the call, which the executor applied: {}", call[0]);
    let heard_deleting = named(&spans, "rdm.mesh.membership.update.via-node-deleting").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).count();
    let heard_deleted = named(&spans, "rdm.mesh.membership.remove.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).count();
    // The victim's own mesh hears the pre-notice on its channel. A peer mesh gets it only inside a backbone
    // aggregate while the operation is open (`in_flight`: "while the operation is claimed and has no
    // completed receipt", gossip.md Members): a retirement that completes within one aggregate interval
    // reaches it as the retained departure alone, which `heard_deleted` below covers for every node.
    assert!(heard_deleting >= 2, "the victim's own mesh heard the pre-notice: {heard_deleting}");
    // Each receiver's overlay install shows the victim found, live and not routable as the overlay
    // opens in its book (so before that receiver's own removal of it).
    let overlays: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-node-deleting").into_iter().filter(|sp| sp["attributes"]["node"] == VICTIM).collect();
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
