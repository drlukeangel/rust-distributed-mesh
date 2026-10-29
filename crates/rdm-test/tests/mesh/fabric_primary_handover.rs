//! i143.e4.s8, i143.e12.s49 process E2E: deleting the fabric-primary's own mesh hands the
//! fabric-primary seat to a surviving mesh first, then deletes the mesh. The fabric does not move;
//! its primary does. mesh.delete ends at mesh.deleted with the mesh-primary alive; its end is a
//! separate node.delete, with the fabric-primary as executor.
//!
//! mesh-delete.md, steps 5-7: "`mesh.deleted`: when every member has its exact exited verdict, the
//! mesh-primary, still alive, writes the mesh row deleted and reports mesh.deleted to the
//! fabric-primary ... After mesh.deleted the mesh-primary keeps publishing its mesh topology, itself the
//! only member and the mesh deleted ... A separate node.delete deletes the mesh-primary. The
//! fabric-primary is its executor, proves its exit and, as acting primary of a mesh that has no
//! primary, writes its row deleted; from that admin's stop on it publishes the deleted mesh's
//! mesh topology." R-MD1 items 2-3: mesh-left requires complete exact-birth terminal coverage, and
//! "Peers retain the Dead mesh and its final birth while that birth lives": the fabric-primary
//! publishes MeshLeft only once the final birth's node.delete has its exact exited verdict. R-RX4 (a
//! live mesh with no primary goes unheard) does not cover a deleted mesh.
//!
//! desired {mesh1, mesh2, mesh3} -> {mesh2, mesh3} through one Build, mesh1 holding the fabric seat.
//! A desired change, never a recovery: no proven-drift Build names mesh1 and nothing recreates a
//! mesh1 node (subfeature `fabric-primary-handover`). Then `node.delete` of mesh1's last admin.
//!
//! R-NS1 (gossip.md, "Mesh delete"): after mesh.deleted the last admin, still its mesh's primary,
//! keeps publishing its mesh topology with itself the only member and MeshStatus Dead, and every peer
//! holds the deleted mesh with that one member while it lives. "From the stop on the fabric-primary
//! is that mesh's publisher of record ... it publishes the deleted mesh's own mesh topology on
//! the backbone, publisher itself, `digests` empty, the stopped admin in `in_flight` as NodeStopped
//! ... until ... a delete moves the birth to `deleted` as NodeDeleted." The
//! seat lands on the lowest NodeId among mesh2's and mesh3's primaries (the election's, never
//! chosen here). Every receiver read is made at BOTH surviving meshes, the fabric-primary's own and
//! the other, so every run covers both placements.

use crate::build_log;
use rdm_test::estate::{descends_from, named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "fabric-primary-handover".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "deleting_the_fabric_primarys_mesh_hands_the_fabric_primary_over_first_then_deletes_it".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn names(meshes: &[&str]) -> BTreeSet<String> {
    meshes
        .iter()
        .flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1"), format!("{m}.rpc.2")])
        .collect()
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn end_ns(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}

fn attr_u(sp: &Value, k: &str) -> u64 {
    sp["attributes"][k].as_str().and_then(|v| v.parse().ok()).or_else(|| sp["attributes"][k].as_u64()).unwrap_or(0)
}

fn alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

/// CONTRACT: deleting the mesh that holds the fabric seat hands the seat to a surviving mesh's
/// primary first, then deletes the mesh. The mesh.delete ends at mesh.deleted: the fabric-primary
/// validates the handoff and records its completion receipt, and stops nothing of mesh1's final
/// primary, which stays alive, publishing mesh1's topology with itself the only member and
/// MeshStatus Dead, while every peer holds mesh1 with that one member. Its end is a separate
/// node.delete the fabric-primary executes: node.stop, exact exited verdict, its row Deleted,
/// NodeDeleted in `deleted`, then the fabric-primary's own mesh topology for mesh1; only then, with
/// every birth proven exited, mesh-left. No MeshLeft is published between the handoff and the final
/// birth's exited verdict.
///
/// Adversarial (R-NS2: "Final mesh topologies and leadership announcements are best-effort dissemination.
/// A stop does not wait for their delivery. Committed lifecycle facts remain recoverable after the
/// sender cuts gossip, and the executing authority updates its own view from the matching completion
/// and publishes that fact as its author: ... the fabric-primary for a mesh's last admin"): the last
/// admin's final mesh topology before its stop, itself stopping, is withheld (the fault door's
/// `announcement` cut) and its stop cuts its transport right behind it. The deleted mesh's final state
/// reaches every surviving mesh from the fabric-primary, which executed the delete and publishes it as
/// the author.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_the_fabric_primarys_mesh_hands_the_fabric_primary_over_first_then_deletes_it() {
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2), ("mesh2", 2, 2), ("mesh3", 2, 2)]).await;
    let before = Estate::values(&estate.settles_to_named(&names(&["mesh1", "mesh2", "mesh3"]), Duration::from_secs(30)).await);
    // A mesh delete waits for the Build in progress (Q170): the fabric is ready under this
    // Build before mesh1 goes.
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let fabric_id = estate.fabric_view().await.id.to_string();
    let (mesh1_id, mesh2_id, mesh3_id) = (estate.mesh_view("mesh1").await.id.expect("mesh1 carries its id").to_string(), estate.mesh_view("mesh2").await.id.expect("mesh2 carries its id").to_string(), estate.mesh_view("mesh3").await.id.expect("mesh3 carries its id").to_string());
    let mesh1_pids: Vec<(String, u64)> = {
        let mut v = Vec::new();
        for n in before.iter().filter(|n| n["mesh"] == "mesh1") {
            let name = s(&n["name"]);
            // The bootstrap admin was adopted on day 0: its pid is the estate's own child.
            let pid = match estate.bootstrap_pid() {
                Some(p) if name == "mesh1.admin.1" => u64::from(p),
                _ => estate.pid_of(&name).await,
            };
            v.push((name, pid));
        }
        v
    };

    // The Node rows mesh1's admin holds, read from its data dir while it runs.
    let mesh1_ids: Vec<(String, String)> = before.iter().filter(|n| n["mesh"] == "mesh1").map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
    let mesh1_dir = estate.data_dir("mesh1.admin.1").await;
    let mesh1_bare: String = mesh1_id.split_once('_').map(|(_, r)| r).unwrap_or(&mesh1_id).replace('-', "").to_lowercase();

    // Control moves to mesh2 before mesh1 goes: its admins advertise it in the topology.
    let mesh2_admin = before
        .iter()
        .find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["status"] == "ready")
        .map(|n| Estate::path(&s(&n["name"])))
        .expect("a ready mesh2 admin");
    estate.enter(&mesh2_admin).await;

    // {mesh1, mesh2, mesh3} -> {mesh2, mesh3}: one Build. It completes at mesh.deleted.
    let b = estate.reconcile_shape("fabric1", &[("mesh2", 2, 2), ("mesh3", 2, 2)]).await.build_id.to_string();
    let survivors_named = names(&["mesh2", "mesh3"]);
    // Every member of mesh1 but its final primary is deleted by mesh1's primary (mesh-delete.md step
    // 3, the whole node.delete: NodeDeleted from its exact exited verdict), so the view settles on mesh2,
    // mesh3 and at most mesh1's final primary.
    let settle_by = std::time::Instant::now() + Duration::from_secs(60);
    let after_b = loop {
        let read = estate.try_node_views().await;
        let nodes = read.as_ref().map(|views| Estate::values(views)).unwrap_or_default();
        let ours: BTreeSet<String> = nodes.iter().filter(|n| n["mesh"] != "mesh1").filter_map(|n| n["name"].as_str().map(str::to_string)).collect();
        let ready = nodes.iter().filter(|n| n["mesh"] != "mesh1").all(|n| n["status"] == "ready");
        if read.is_ok() && ours == survivors_named && ready && nodes.iter().filter(|n| n["mesh"] == "mesh1").count() <= 1 {
            break nodes;
        }
        if std::time::Instant::now() >= settle_by {
            let held: Vec<String> = match &read {
                Ok(_) => nodes.iter().map(|n| format!("{}={}", s(&n["name"]), s(&n["status"]))).collect(),
                Err(e) => vec![format!("the read ended: {e}")],
            };
            estate.stop().await;
            let spans = estate.spans();
            let deleted: Vec<String> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["build_id"] == b.as_str()).map(|sp| s(&sp["attributes"]["node"])).collect();
            let recoveries: Vec<String> = named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().map(|sp| format!("{} by {}: {}", s(&sp["attributes"]["node"]), s(&sp["attributes"]["submitter"]), s(&sp["attributes"]["outcome"]))).collect();
            panic!("the view never settled on mesh2, mesh3 and at most mesh1's final primary within 60 s after build {b}: {held:?}; NodeDeleted published under {b}: {deleted:?}; recovery submissions ({}): {:?}", recoveries.len(), recoveries.iter().take(3).collect::<Vec<_>>());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // mesh.deleted leaves mesh1's final primary alive: the one mesh1 node the view still holds.
    let Some(last) = after_b.iter().find(|n| n["mesh"] == "mesh1").cloned() else {
        estate.stop().await;
        let spans = estate.spans();
        let stopped: Vec<String> = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .filter(|sp| sp["attributes"]["build_id"] == b.as_str() && s(&sp["attributes"]["node"]).starts_with("mesh1.admin.") && sp["attributes"]["step"] == "TerminateRuntime")
            .map(|sp| format!("{} TerminateRuntime trace {}", s(&sp["attributes"]["node"]), s(&sp["trace_id"])))
            .collect();
        let inside: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().filter(|sp| sp["attributes"]["build_id"] == b.as_str()).map(|sp| s(&sp["attributes"]["node"])).collect();
        panic!("mesh.delete ends at mesh.deleted with the mesh-primary alive, but no mesh1 node is left after build {b}: the fabric-primary stopped the final primary inside the mesh.delete ({stopped:?}; shutdown-member workflows under {b}: {inside:?})");
    };
    let final_name = s(&last["name"]);
    assert!(final_name.starts_with("mesh1.admin."), "the mesh1 node left alive is a node-admin, the final primary: {last}");
    let final_pid = mesh1_pids.iter().find(|(n, _)| *n == final_name).map(|(_, p)| *p).unwrap();
    assert!(alive(final_pid), "mesh1's final primary {final_name} still runs after mesh.deleted");
    let fabric_primary_path = estate.fabric_view().await.fabric_primary.expect("the fabric names its primary");
    let fabric_primary = fabric_primary_path.to_string();
    estate.enter(&fabric_primary_path).await;

    // Peers hold the deleted mesh with its last admin while it lives (R-NS1 item 1, R-MD1 item 3):
    // an admin of each surviving mesh, in what it holds itself (query.md §2), holds mesh1 deleted with
    // that one node: the mesh status it took from the gossiped MeshStatus, and the mesh1 births its
    // topology holds (`node.topology.get`). fabric.get at it answers a primary's atlas, never its own
    // view (query.md §1; Q243). The admin lives on here until its node.delete, so this interval is held,
    // not raced.
    let survivors = ["mesh2", "mesh3"];
    for m in survivors {
        for admin in after_b.iter().filter(|n| n["mesh"] == m && n["kind"] == "node_admin") {
            let who = s(&admin["name"]);
            wait_for(&format!("{who} holds mesh1 dead with {final_name} its only node"), Duration::from_secs(30), || async {
                let dead = named(&estate.spans(), "rdm.mesh.topology.update.via-mesh-status").into_iter().any(|sp| s(&sp["attributes"]["node"]) == who && sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["status"] == "dead" && s(&sp["attributes"]["taken"]) == "true");
                let held = estate.held_topology_at(&Estate::path(&who)).await?;
                let mesh1: Vec<String> = held.members.iter().filter(|d| d.node.name.mesh == "mesh1").map(|d| d.node.name.to_string()).collect();
                (dead && mesh1 == [final_name.clone()]).then_some(())
            })
            .await;
        }
    }
    assert!(alive(final_pid), "mesh1's final primary {final_name} still runs while every peer holds it");

    // The completion receipt of mesh.deleted, read from the live Build projection: every birth of mesh1
    // but its final primary, which is not deleted inside its own mesh.delete; no MeshLeft yet.
    let steps_of = |build: &Value, step: &str| -> Vec<Value> { build["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["step"] == step).collect() };
    let build_b = build_log::build_value(&estate.carrier().await, &b).await;
    let deleted_receipts = steps_of(&build_b, "MeshDeleted");
    assert_eq!(deleted_receipts.len(), 1, "one MeshDeleted completion receipt under {b}: {build_b}");
    assert_eq!(deleted_receipts[0]["operation"], format!("mesh.delete:{mesh1_bare}"), "keyed by the mesh's minted id: {}", deleted_receipts[0]);
    assert_eq!(deleted_receipts[0]["output"]["receipts"].as_array().map(Vec::len), Some(3), "a terminal receipt for every birth of mesh1 but its final primary: {}", deleted_receipts[0]);
    assert_eq!(s(&deleted_receipts[0]["output"]["final_primary"]["node"]), final_name, "{}", deleted_receipts[0]);
    assert!(steps_of(&build_b, "MeshLeft").is_empty(), "no MeshLeft while the final primary lives: {build_b}");

    // The final primary's end: an ordinary node.delete, a separate call, the fabric-primary its
    // executor. The topology names no mesh1, so the call opens its own targeted attempt of the accepted
    // Build (as a delete of a stopped `.old` does), with its own trace.
    let mut removal = estate.nodes_obj().await.delete(&Estate::path(&final_name)).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of {final_name}: {e}"));
    let accepted = Estate::complete(&mut removal).await;
    let (c, c_attempt) = (accepted.build_id.to_string(), u64::from(accepted.attempt));
    let after = Estate::values(&estate.settles_to_named(&names(&["mesh2", "mesh3"]), Duration::from_secs(60)).await);

    // Identities: the Fabric, mesh2 and mesh3 keep theirs; mesh1 is gone.
    assert_eq!(estate.fabric_view().await.id.to_string(), fabric_id, "the Fabric keeps its id");
    assert_eq!(estate.mesh_view("mesh2").await.id.expect("mesh2 carries its id").to_string(), mesh2_id, "an untouched mesh keeps its id");
    assert_eq!(estate.mesh_view("mesh3").await.id.expect("mesh3 carries its id").to_string(), mesh3_id, "an untouched mesh keeps its id");
    estate.fabrics_obj().await.fact("mesh1 is no longer a mesh of the fabric", Duration::from_secs(30), |f| !f.meshes.iter().any(|m| m.name == "mesh1")).await.unwrap_or_else(|end| panic!("{end}"));
    assert!(!after.iter().any(|n| n["mesh"] == "mesh1"), "no mesh1 node remains: {after:#?}");
    for (name, pid) in &mesh1_pids {
        wait_for(&format!("{name}'s runtime is gone"), Duration::from_secs(30), || async { (!alive(*pid)).then_some(()) }).await;
    }

    // Deleted is final and its members resolve Gone: an ordinary node of each surviving mesh answers
    // `gone` from its own live resolver for every deleted mesh1 birth, never `unknown`.
    for rx in ["mesh2.rpc.1", "mesh3.rpc.1"] {
        for (name, id) in mesh1_ids.iter() {
            let query = format!("exact:{id}");
            let target = format!("path:{rx}");
            wait_for(&format!("{rx}'s resolver answers Gone for deleted {name}"), Duration::from_secs(30), || async {
                let r = estate.probe(&["resolve", "--target", &target, "--query", &query]);
                (r["reply"]["resolution"] == "gone").then_some(())
            })
            .await;
        }
    }
    // R-MN1 (mesh-delete.md "Rows", steps 3 and 7): every birth of mesh1 is deleted and no row is
    // removed. Each mesh1 birth's Node record stays on the fabric's store root at its node id
    // (its lines in `nodes.jsonl` at its node id, R-SR1; storage-whole-records.md), written Deleted: the members' by
    // mesh1's primary, the final primary's by the fabric-primary in its node.delete. A deleted birth
    // never answers, so mesh-left takes the address of every birth its proof set names off its record.
    assert!(std::path::Path::new(&format!("{mesh1_dir}/nodes/self.json")).exists(), "mesh1.admin.1's own Node row remains in {mesh1_dir}");
    for (name, id) in mesh1_ids.iter() {
        let row: Value = estate.node_record(id).unwrap_or_else(|| panic!("the Node row of {name} ({id}) remains in nodes.jsonl"));
        assert_eq!((s(&row["record"]["node_id"]), s(&row["record"]["status"])), (id.clone(), "deleted".to_string()), "{name}'s Node row is written Deleted: {row}");
        let newest = row["record"]["births"].as_array().and_then(|b| b.last()).cloned().unwrap_or_default();
        assert!(newest["transport_addr"].is_null() && newest["is_primary"].is_null(), "mesh-left took {name}'s ({id}) address and primaries off its record: {row}");
    }
    // states.md: the mesh's `deleting` is entered by the delete-mesh down op, and "one commit stores
    // the row, applies it, gossips it and answers the op": mesh1's primary stored its own mesh row
    // Leaving when it admitted delete-mesh, on mesh1's record in mesh.storage on the fabric's store root
    // (R14, R-SR1; storage-whole-records.md: the record's lines in `meshes.jsonl` at the mesh id, every
    // state it entered on the record).
    let record_at = estate.fabric_store_root().join(rdm_admin::store_root::MESHES_FILE);
    let row: Value = rdm_admin::store_log::read_record(&record_at, "mesh-record", Some(&mesh1_bare)).unwrap_or_else(|| panic!("mesh1's record is not in {}", record_at.display()));
    assert_eq!(s(&row["record"]["mesh_id"]), mesh1_bare.clone(), "{row}");
    assert!(row["record"]["states"].as_array().is_some_and(|states| states.iter().any(|st| st == "Leaving")), "mesh1's primary stored Leaving on its mesh's record: {row}");
    // mesh-left, once the final birth's exit was proven: one Complete MeshLeft receipt under the
    // mesh.delete's operation, covering every birth of the mesh, the final primary included.
    let build_b = build_log::build_value(&estate.carrier().await, &b).await;
    let left_receipts = steps_of(&build_b, "MeshLeft");
    let other_receipts = steps_of(&build_b, "OtherMembersExited");
    estate.stop().await;
    let spans = estate.spans();
    assert_eq!(other_receipts.len(), 1, "one OtherMembersExited receipt, recorded by mesh1's primary: {build_b}");
    assert_eq!(other_receipts[0]["output"]["receipts"].as_array().map(|r| r.len()), Some(3), "a terminal receipt for every birth of mesh1 but its final primary");
    assert_eq!(left_receipts.len(), 1, "one MeshLeft receipt under {b}: {build_b}");
    assert_eq!(left_receipts[0]["outcome"], json!("complete"), "{}", left_receipts[0]);
    assert_eq!(left_receipts[0]["operation"], format!("mesh.delete:{mesh1_bare}"), "keyed by the mesh's minted id");
    assert_eq!(left_receipts[0]["output"]["receipts"].as_array().map(|r| r.len()), Some(4), "a terminal receipt for every birth of mesh1, the final primary included: {}", left_receipts[0]);

    // The mesh.delete Build's executor is the fabric-primary; so is the node.delete's. The run that ran
    // `op` ends converged, or, its operations all done while the fabric it put back to pending has not
    // changed, handed back to the fabric-primary, which completes the Build after the change (Q154).
    let reconciles = named(&spans, "rdm.node_admin.build.update.via-reconcile");
    let converged = |build: &str, op: &str| -> String {
        let r = reconciles
            .iter()
            .find(|sp| sp["attributes"]["build_id"] == build && (sp["attributes"]["outcome"] == "converged" || sp["attributes"]["outcome"] == "handed-off") && sp["attributes"]["operations"].as_str().is_some_and(|o| o.contains(op)))
            .cloned()
            .unwrap_or_else(|| panic!("a reconcile ran {op} under {build} to its end"));
        s(&r["attributes"]["executor"])
    };
    let owner_name = converged(&b, "mesh.delete:");
    let delete_executor = converged(&c, &format!("node.delete:{final_name}"));
    assert_eq!(delete_executor, fabric_primary, "the fabric-primary executes the final primary's node.delete (mesh1 has no other admin)");

    // mesh1's members left through the member workflows under B: every member stopped and
    // proven exited in the documented order, and none of the node-removal steps ran.
    let steps = named(&spans, "rdm.node_admin.deployment.update.via-step");
    // The node.delete's run: Build `c` from attempt `c_attempt`; the mesh.delete's: every other
    // attempt of Build `b`.
    let node_delete_run = |sp: &Value| sp["attributes"]["build_id"] == c.as_str() && attr_u(sp, "attempt") >= c_attempt;
    let in_run = |sp: &Value, node_delete: bool| if node_delete { node_delete_run(sp) } else { sp["attributes"]["build_id"] == b.as_str() && !node_delete_run(sp) };
    let order_in = |name: &str, node_delete: bool| -> Vec<(String, u64, u64)> {
        let mut v: Vec<&Value> = steps.iter().copied().filter(|sp| sp["attributes"]["node"] == name && in_run(sp, node_delete)).collect();
        v.sort_by_key(|sp| sp["start_unix_nano"].as_u64());
        v.iter().map(|sp| (s(&sp["attributes"]["step"]), sp["start_unix_nano"].as_u64().unwrap_or(0), sp["end_unix_nano"].as_u64().unwrap_or(0))).collect()
    };
    let members: Vec<String> = mesh1_pids.iter().map(|(n, _)| n.clone()).filter(|n| *n != final_name).collect();
    // mesh-delete.md step 3: each member's end is the whole node.delete, a node-admin's the same as
    // any member's: it holds no seat, so no node.primary.update precedes it (R-MT1).
    for name in &members {
        let order = order_in(name, false);
        let got: Vec<&str> = order.iter().map(|(n, _, _)| n.as_str()).collect();
        let want: &[&str] = &["NodeDeleting", "StopNode", "AwaitNodeLeft", "TerminateRuntime", "NodeDeleted", "RemoveTopologyMembership", "Complete"];
        assert_eq!(got, want, "{name}: the ordinary node.delete under {b}, node.stop first, then the exited verdict and NodeDeleted");
    }
    // Each member's NodeDeleted is mesh1's primary's, before mesh.deleted; the deleted mesh's last
    // admin then holds no exited member to recover (node-recovery.md: recovery is for a member that
    // exits outside any workflow).
    let deleted_member: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["build_id"] == b.as_str()).collect();
    for name in &members {
        assert!(deleted_member.iter().any(|sp| sp["attributes"]["node"] == name.as_str() && s(&sp["attributes"]["operation"]) == format!("node.delete:{name}")), "mesh1's primary published NodeDeleted for {name} under {b}: {deleted_member:?}");
    }
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| s(&sp["attributes"]["submitter"]) == final_name).collect();
    assert!(recoveries.is_empty(), "the deleted mesh's last admin {final_name} submits no recovery of its deleted members: {} submissions, first {:?}", recoveries.len(), recoveries.first());
    // 1. mesh.deleted ends the mesh.delete: nothing of the final primary is stopped under B.
    assert!(order_in(&final_name, false).is_empty(), "the mesh.delete runs no step on its final primary {final_name} (its node.delete is attempt {c_attempt}): {:?}", order_in(&final_name, false));
    let inside: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().filter(|sp| sp["attributes"]["node"] == final_name.as_str()).collect();
    assert!(inside.is_empty(), "no shutdown-member workflow of the final primary {final_name}: {inside:?}");
    // Tiers: the ordinary nodes' workflows overlap in time; the other node-admin starts only after the
    // last ordinary node's exit is proven.
    let member = |name: &str| -> (u64, u64) {
        let sp = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().find(|sp| sp["attributes"]["node"] == name && sp["attributes"]["operation"].as_str().is_some_and(|o| o.starts_with("mesh.delete:"))).cloned().unwrap_or_else(|| panic!("no shutdown-member span for {name}"));
        (start_ns(&sp), end_ns(&sp))
    };
    let terminal_end = |name: &str, node_delete: bool| order_in(name, node_delete).iter().find(|(n, _, _)| n == "TerminateRuntime").map(|(_, _, e)| *e).unwrap_or_else(|| panic!("{name} has a TerminateRuntime step in the {} run", if node_delete { "node.delete" } else { "mesh.delete" }));
    let stop_start = |name: &str, node_delete: bool| order_in(name, node_delete).iter().find(|(n, _, _)| n == "StopNode").map(|(_, st, _)| *st).unwrap_or_else(|| panic!("{name} has a StopNode step in the {} run", if node_delete { "node.delete" } else { "mesh.delete" }));
    let (r1, r2) = (member("mesh1.rpc.1"), member("mesh1.rpc.2"));
    assert!(r1.0 < r2.1 && r2.0 < r1.1, "the ordinary nodes' workflows overlap in time: {r1:?} {r2:?}");
    let tier1_last = terminal_end("mesh1.rpc.1", false).max(terminal_end("mesh1.rpc.2", false));
    let tier2: Vec<&String> = members.iter().filter(|n| n.contains(".admin.")).collect();
    assert_eq!(tier2.len(), 1, "one other node-admin of mesh1: {members:?}");
    for n in &tier2 {
        assert!(stop_start(n, false) > tier1_last, "{n} (a node-admin) stopped only after every ordinary node's exit was proven");
    }
    let handoff = named(&spans, "rdm.node_admin.mesh.update.via-handoff-validated").into_iter().find(|sp| sp["attributes"]["operation"].as_str().is_some_and(|o| o.starts_with("mesh.delete:"))).cloned().expect("the handoff was validated");
    assert_eq!(handoff["attributes"]["outcome"], "applied", "the owner validated the mesh-deleted handoff: {handoff}");
    // The completion follows the validated handoff, at the fabric-primary that owns the mesh.delete.
    let completion = named(&spans, "rdm.node_admin.mesh.update.via-mesh-deleted-receipt").into_iter().find(|sp| s(&sp["attributes"]["mesh_id"]) == mesh1_bare).cloned().unwrap_or_else(|| panic!("the fabric-primary recorded mesh.deleted's completion receipt for mesh1"));
    assert!(start_ns(&completion) >= end_ns(&handoff), "the completion receipt follows the validated handoff: {completion}");
    assert_eq!((attr_u(&completion, "receipts"), attr_u(&completion, "roster"), s(&completion["attributes"]["final_primary"])), (3, 4, final_name.clone()), "{completion}");
    assert_eq!(s(&completion["attributes"]["node"]), owner_name, "the mesh.delete's owner records its completion: {completion}");
    eprintln!("bullet 1: mesh.deleted completion trace {} by {owner_name}, handoff trace {}", s(&completion["trace_id"]), s(&handoff["trace_id"]));

    let recorded = named(&spans, "rdm.node_admin.mesh.update.via-exit-manifest-recorded");
    assert!(recorded.iter().any(|sp| attr_u(sp, "receipts") == 3 && attr_u(sp, "roster") == 4), "the mesh primary recorded the exit manifest of its three other members: {recorded:?}");
    let heard = |m: &str, name: &str| -> Vec<&Value> { named(&spans, &format!("rdm.mesh.topology.update.{name}")).into_iter().filter(|sp| s(&sp["attributes"]["node"]).starts_with(&format!("{m}."))).collect() };
    for m in survivors {
        // R4: no mesh-level hold at a peer. A node learns mesh1's members are leaving from each
        // member's own ConnectionsDeleting and Leaving in what it installs, and routes none of them meanwhile.
        let states: Vec<&Value> = heard(m, "via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]).starts_with("mesh1.")).collect();
        let leaving_states: Vec<&Value> = states.iter().copied().filter(|sp| matches!(s(&sp["attributes"]["status"]).as_str(), "ConnectionsDeleting" | "Leaving")).collect();
        assert!(!leaving_states.is_empty(), "a {m} node held a mesh1 member's own connections-deleting or Leaving: {states:?}");
        assert!(leaving_states.iter().all(|sp| s(&sp["attributes"]["routable"]) == "false" || sp["attributes"]["routable"] == json!(false)), "a mesh1 member saying connections-deleting or Leaving is not routable at {m}: {leaving_states:?}");
        // Each ordinary node holds a member's own state from its mesh channel or the read a gap on that
        // channel pulls, or it skipped those versions: gossip.md §3.3 "On a mismatch it marks that source's
        // projection desynced, stops applying its deltas, and pulls the current full fabric topology from
        // its own current mesh-primary", "A node that installs a pulled copy older than a delta it dropped
        // while the pull was in flight pulls again (Q130)", and "A node that missed a lifecycle event
        // recovers it from `MeshTopology.in_flight` or `MeshTopology.deleted`, through the next delta, full
        // or top-up". A node desynced on mesh1 while its members left installs, from its top-up, mesh1 with
        // every one of them deleted, and holds none of their transient states.
        for ordinary in [format!("{m}.rpc.1"), format!("{m}.rpc.2")] {
            let held_here = leaving_states.iter().any(|sp| s(&sp["attributes"]["node"]) == ordinary && matches!(sp["attributes"]["channel"].as_str(), Some("mesh-channel" | "topology-read")));
            let gap_at = named(&spans, "rdm.mesh.topology.update.via-gap").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == ordinary && sp["attributes"]["mesh"] == "mesh1").map(|sp| start_ns(sp)).min();
            let topped_up: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-snapshot-installed")
                .into_iter()
                .filter(|sp| s(&sp["attributes"]["node"]) == ordinary && sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["channel"] == "topology-read" && attr_u(sp, "deleted") >= members.len() as u64 && gap_at.is_some_and(|g| start_ns(sp) >= g))
                .collect();
            assert!(
                held_here || !topped_up.is_empty(),
                "{ordinary} held a mesh1 member's own connections-deleting or Leaving from its mesh channel or a topology read, or, desynced on mesh1 (first gap {gap_at:?}), installed from its top-up mesh1 with all {} of its other members deleted: {leaving_states:?}",
                members.len()
            );
        }
    }
    let routable = |sp: &Value| s(&sp["attributes"]["routable"]) == "true" || sp["attributes"]["routable"] == json!(true);
    let installed = named(&spans, "rdm.mesh.topology.update.via-snapshot-installed");

    // 1. While it lives: after mesh.deleted the last admin keeps publishing
    // mesh1's topology with itself the only member and MeshStatus Dead. Every peer held mesh1 dead
    // with that one member while it lived (read above, on each surviving admin's own view); which
    // frame brought it there is best-effort dissemination (R-NS2), so no particular one is expected.
    let final_exit = terminal_end(&final_name, true);
    let lone: Vec<&Value> = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && s(&sp["attributes"]["node"]) == final_name && attr_u(sp, "members") == 1 && start_ns(sp) < final_exit).collect();
    assert!(!lone.is_empty(), "the last admin {final_name} published mesh1's topology with itself the only member while it lived");
    let dead_row: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-dead-row").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name && s(&sp["attributes"]["mesh_id"]) == mesh1_bare).collect();
    assert!(dead_row.iter().any(|sp| s(&sp["attributes"]["outcome"]) == "applied" && start_ns(sp) <= start_ns(&completion)), "the last admin {final_name} committed mesh1's row Dead at mesh.deleted: {dead_row:?}");
    let dead_sent: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name && sp["attributes"]["scope"] == "mesh:mesh1" && sp["attributes"]["status"] == "dead").collect();
    assert!(dead_sent.iter().any(|sp| start_ns(sp) < final_exit), "the last admin {final_name} sent MeshStatus Dead for mesh1 while it lived: {dead_sent:?}");
    eprintln!("bullet 1: {final_name} lone mesh topology trace {}, MeshStatus Dead trace {}", s(&lone[0]["trace_id"]), dead_sent.first().map(|sp| s(&sp["trace_id"])).unwrap_or_default());

    // 2. The final primary's node.delete under C, executed by the fabric-primary: its whole node.stop
    // (the delete of a mesh's only node skips node.primary.update, R-MT1: "delete is skipped if it's
    // the only node on the mesh"; its seat stays with it), the provider's exact exited verdict, its row
    // Deleted, NodeDeleted, its removal.
    let order = order_in(&final_name, true);
    let got: Vec<&str> = order.iter().map(|(n, _, _)| n.as_str()).collect();
    let moved: Vec<&Value> = ["rdm.node_admin.primary.update.via-command", "rdm.node_admin.primary.update.via-update-primary"]
        .iter()
        .flat_map(|n| named(&spans, n))
        .filter(|sp| (s(&sp["attributes"]["primary"]) == final_name || s(&sp["attributes"]["node"]) == final_name) && s(&sp["attributes"]["build_id"]) == c && attr_u(sp, "attempt") == c_attempt)
        .collect();
    assert!(moved.is_empty(), "{final_name} is its mesh's only node: its delete runs no node.primary.update: {moved:#?}");
    assert_eq!(got, ["NodeDeleting", "StopNode", "AwaitNodeLeft", "TerminateRuntime", "NodeDeleted", "RemoveTopologyMembership", "Complete"], "{final_name}: the ordinary node.delete, node.stop first, then the exited verdict");
    let c_steps: Vec<&Value> = steps.iter().copied().filter(|sp| sp["attributes"]["node"] == final_name.as_str() && node_delete_run(sp)).collect();
    assert!(c_steps.iter().all(|sp| sp["attributes"]["outcome"] == "complete"), "{final_name}: every step of its node.delete completed: {c_steps:?}");
    let row_deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| s(&sp["attributes"]["subject"]) == final_name && s(&sp["attributes"]["node"]) == fabric_primary).collect();
    assert!(row_deleted.iter().any(|sp| sp["attributes"]["outcome"] == "row-deleted" && start_ns(sp) >= final_exit), "the fabric-primary {fabric_primary} wrote {final_name}'s row Deleted after its exited verdict: {row_deleted:?}");
    let node_deleted = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().find(|sp| s(&sp["attributes"]["node"]) == final_name && s(&sp["attributes"]["build_id"]) == c).cloned().unwrap_or_else(|| panic!("NodeDeleted was published for {final_name} under {c}"));
    assert!(start_ns(&node_deleted) >= final_exit, "NodeDeleted follows the exited verdict: {node_deleted}");

    // mesh-left once every birth is proven exited: the receipt over all four births at the
    // fabric-primary, after the final birth's exited verdict.
    let left_recorded: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-mesh-left-receipt").into_iter().filter(|sp| attr_u(sp, "receipts") == 4 && attr_u(sp, "roster") == 4).collect();
    assert!(left_recorded.iter().any(|sp| start_ns(sp) >= final_exit), "the fabric-primary recorded MeshLeft over all four births after the final birth's exited verdict: {left_recorded:?}");
    // mesh-left takes the addresses of the births its proof set names off their records, the final primary included.
    let dropped = named(&spans, "rdm.node_admin.node.remove.via-mesh-left-contact")
        .into_iter()
        .find(|sp| sp["attributes"]["operation"] == format!("mesh.delete:{mesh1_bare}").as_str() && sp["attributes"]["error"].is_null())
        .cloned()
        .unwrap_or_else(|| panic!("mesh-left took mesh1's births' addresses off their records under mesh.delete:{mesh1_bare}"));
    for (name, id) in mesh1_ids.iter() {
        assert!(s(&dropped["attributes"]["dropped"]).split(", ").any(|d| d == format!("{name} {id}")), "mesh-left took the address of {name} ({id}) off its record: {dropped}");
    }
    // e12.s72: the deleted mesh's last admin publishes the final mesh topology (`published == "true"`,
    // `status == "Leaving"`, carrying `node.delete:<final_name>` in `in_flight`) after its stop's
    // `node.connections.delete { All }` and before its transport cut.
    let final_cut = named(&spans, "rdm.node_admin.node.update.via-stop-transport-cut").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name).map(|sp| start_ns(sp)).min().expect("the final primary's transport was cut by its stop");
    let final_all_end = named(&spans, "rdm.node_admin.connection.delete.via-stop").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name).map(|sp| end_ns(sp)).min().expect("the final primary's stop ran its node.connections.delete { All }");
    let leaving_aggregate: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-leaving-aggregate").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == final_name).collect();
    assert!(
        leaving_aggregate.iter().any(|sp| sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["status"] == "Leaving" && attr_u(sp, "members") == 1 && sp["attributes"]["published"] == "true" && start_ns(sp) >= final_all_end && end_ns(sp) <= final_cut),
        "the last admin {final_name} published mesh1's final mesh topology carrying itself Leaving after its All and before its stop's cut: {leaving_aggregate:?}"
    );
    let final_stopped_at = order.iter().find(|(n, _, _)| n == "StopNode").map(|(_, _, e)| *e).expect("the final primary's stop was answered");
    let final_after_stop: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]) == final_name && start_ns(sp) > final_stopped_at).collect();
    assert!(final_after_stop.iter().all(|sp| !routable(sp)), "no node holds the final primary {final_name} routable after its stop: {final_after_stop:?}");
    let _ = installed;

    // gossip.md, node-stopped lifecycle overlay: mesh1's primary publishes NodeStopped from each
    // member's matching stopped reply, for every member it stops, and a new mesh1 version carries it.
    let published_stopped = named(&spans, "rdm.node_admin.node.update.via-node-stopped");
    let stopped_at = |m: &str| -> u64 {
        published_stopped.iter().find(|sp| s(&sp["attributes"]["node"]) == m && s(&sp["attributes"]["operation"]) == format!("node.stop:{m}")).map(|sp| start_ns(sp)).unwrap_or_else(|| panic!("mesh1's primary published NodeStopped for {m}: {published_stopped:?}"))
    };
    let mesh1_publications: Vec<u64> = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1").map(|sp| start_ns(sp)).collect();
    let mesh1_wakes: Vec<u64> = named(&spans, "rdm.mesh.backbone.update.via-material-change").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1").map(|sp| start_ns(sp)).collect();
    assert!(!mesh1_wakes.is_empty(), "mesh1's primary's checklist was woken by its own projection's move");
    for m in &members {
        let at = stopped_at(m);
        assert!(mesh1_publications.iter().any(|p| *p >= at), "mesh1's primary published a new version after {m}'s NodeStopped at {at}: publications {mesh1_publications:?}");
        let after: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member"]) == *m && start_ns(sp) > at).collect();
        assert!(after.iter().all(|sp| !routable(sp)), "no node holds {m} routable after its NodeStopped: {after:?}");
    }
    // The Leaving row is the delete-mesh admission's commit, before the first member stops.
    let leaving_commit: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-leaving-row").into_iter().filter(|sp| s(&sp["attributes"]["mesh_id"]) == mesh1_bare).collect();
    assert!(leaving_commit.iter().any(|sp| s(&sp["attributes"]["node"]) == final_name && s(&sp["attributes"]["outcome"]) == "applied"), "mesh1's primary {final_name} applied its mesh row Leaving at delete-mesh: {leaving_commit:?}");
    let first_member_stop = ["mesh1.rpc.1", "mesh1.rpc.2"].iter().map(|n| stop_start(n, false)).min().unwrap();
    assert!(leaving_commit.iter().all(|sp| end_ns(sp) < first_member_stop), "the Leaving row was committed before any member stopped: {leaving_commit:?}");

    // lifecycles/node-primary-update.md "The fabric-primary" and mesh-delete.md step 1: when the mesh is
    // the fabric-primary's, node.fabric-primary.update runs first with this mesh excluded from the
    // successor search; the incumbent stays mesh-primary until its own node.delete. In order at mesh1's
    // primary: its issued writes finished, then the outside successor installs the fabric-primary alone
    // and answers node.primary.updated under the primary's call; the leave's owner finds the
    // fabric-primary no node-admin of mesh1 (`via-fabric-primary-left`) before DeleteMesh and every stop.
    let attr_s = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let operation = format!("node.fabric-primary.update:{b}:1:mesh.delete:{mesh1_bare}");
    let one = |name: &str| -> Value {
        let v: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| attr_s(sp, "operation") == operation).collect();
        assert_eq!(v.len(), 1, "exactly one {name} span of {operation}: {v:?}");
        v[0].clone()
    };
    // mesh1's last admin stays its mesh's primary to its own stop: it reports its stop to no one,
    // itself included, since after its cut it publishes nothing (node-stop.md step 4; i146 Q37), and
    // its NodeStopped is published once, by its commander.
    let final_stop = format!("node.stop:{final_name}");
    let own_primary: Vec<&Value> = named(&spans, "rdm.node_admin.status.reject.via-own-mesh-primary").into_iter().filter(|sp| attr_s(sp, "node") == final_name && attr_s(sp, "operation") == final_stop).collect();
    assert_eq!(own_primary.len(), 1, "{final_name}'s stop is not reported to itself: {own_primary:#?}");
    let final_stopped: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| attr_s(sp, "node") == final_name).collect();
    assert_eq!(final_stopped.len(), 1, "{final_name}'s NodeStopped is published once: {final_stopped:#?}");
    let updating = one("rdm.node_admin.primary.update.via-update-primary");
    assert_eq!((attr_s(&updating, "node"), attr_s(&updating, "moving"), attr_s(&updating, "successor"), attr_s(&updating, "outcome")), (final_name.clone(), "fabric-primary".to_string(), fabric_primary.clone(), "updated".to_string()), "mesh1's primary moved the fabric-primary alone to the outside successor: {updating}");
    let finished = one("rdm.node_admin.primary.update.via-issued-writes-finished");
    let installed: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| attr_s(sp, "operation") == operation && attr_s(sp, "node") == fabric_primary && attr_s(sp, "outcome") == "held").collect();
    assert_eq!(installed.len(), 1, "the outside successor installed the fabric-primary once and answered node.primary.updated: {installed:?}");
    let installed = installed[0].clone();
    assert!(attr_s(&installed, "primaries").starts_with("fabric ") && !attr_s(&installed, "primaries").contains("node-admin "), "the fabric-primary alone moves; mesh1's mesh-primary stays: {installed}");
    assert!(descends_from(&spans, &installed, &updating), "the successor installed under the primary's call: {installed}");
    let left = one("rdm.node_admin.primary.update.via-fabric-primary-left");
    assert_eq!(attr_s(&left, "departing"), "mesh1", "the leave's owner found the fabric-primary no node-admin of mesh1: {left}");
    assert!(end_ns(&finished) <= start_ns(&installed), "the issued writes finished, then the successor's install");
    assert!(end_ns(&installed) <= start_ns(&left) && end_ns(&left) < first_member_stop, "the fabric-primary left mesh1 before DeleteMesh and every stop");
    assert!(named(&spans, "rdm.node_admin.primary.reject.via-primary-update").is_empty(), "no node.primary.update was refused");
    // Commands go down: every 0x21 call is the holder's node.primary.update on its successor.
    let calls: Vec<&Value> = named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| attr_u(sp, "op") == 0x21).collect();
    assert!(!calls.is_empty() && calls.iter().all(|sp| descends_from(&spans, sp, &updating)), "every 0x21 call is the holder's call on its successor: {calls:?}");

    // A desired change, not a recovery.
    assert!(!named(&spans, "rdm.node_admin.build.create.via-proven-drift").iter().any(|sp| s(&sp["attributes"]["scope"]).contains("mesh1")), "no recovery Build names mesh1");
    let created = named(&spans, "rdm.node_admin.node.create.via-build");
    assert!(!created.iter().any(|sp| (sp["attributes"]["build_id"] == b.as_str() || sp["attributes"]["build_id"] == c.as_str()) && s(&sp["attributes"]["node"]).starts_with("mesh1.")), "nothing recreates a mesh1 node");

    // rafka-time: the fabric seat moved from the deleted mesh1 to the lowest NodeId among the
    // remaining mesh primaries (R-A2). The other remaining mesh primary, which does not hold the seat,
    // pulled once from the new fabric-primary and adopted its time under the mesh-primary rule.
    let holder = fabric_primary.clone();
    let holder_id = after.iter().find(|n| s(&n["name"]) == holder).map(|n| s(&n["node_id"])).unwrap_or_else(|| panic!("the fabric primary {holder} is in the settled view"));
    assert!(["mesh2.admin.1", "mesh3.admin.1"].contains(&holder.as_str()), "the seat left the deleted mesh1 for a remaining mesh primary: {holder}");
    let other = if holder == "mesh2.admin.1" { "mesh3.admin.1" } else { "mesh2.admin.1" };
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let moved: Vec<&Value> = named(&spans, "rdm.mesh.entry.resolve.via-fabric-primary-changed").into_iter().filter(|sp| attr(sp, "node") == other && attr(sp, "to") == holder_id).collect();
    assert_eq!(moved.len(), 1, "{other} pulled once from {holder} when the fabric seat moved to it: {moved:?}");
    assert_eq!(attr(moved[0], "outcome"), "adopted", "{:?}", moved[0]);
    let adopted = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted");
    let child = adopted.iter().find(|a| descends_from(&spans, a, moved[0])).expect("the adoption is a child of the pull");
    assert_eq!(
        (attr(child, "node").as_str(), attr(child, "source").as_str(), attr(child, "via").as_str(), attr(child, "kind").as_str(), attr(child, "served_by").as_str()),
        (other, "pull", "get-topology", "fabric-primary", holder.as_str())
    );

    // The successor is the holder's exact birth (gossip.md "A primary change on gossip": its digest then
    // carries `is_fabric_primary`, and the other remaining mesh-primary pulled from it above).
    let h = after.iter().find(|n| s(&n["name"]) == holder).unwrap();
    assert!(attr_s(&installed, "primaries").contains(&format!("-> {}/{} @", s(&h["node_id"]), s(&h["incarnation_id"]))), "the successor is the holder's exact birth: {installed} {h}");
    estate.record_trace_url(&s(&installed["trace_id"]));
}
