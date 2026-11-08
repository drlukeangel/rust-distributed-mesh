//! i143.e4.s8, i143.e12.s49 process E2E: deleting the fabric-primary's own mesh hands the
//! fabric-primary role to a surviving mesh first, then deletes the mesh. The fabric does not move;
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
//! "Peers retain the deleted mesh and its final birth while that birth lives": the fabric-primary
//! publishes MeshLeft only once the final birth's node.delete has its exact exited verdict. R-RX4 (a
//! live mesh with no primary goes unheard) does not cover a deleted mesh.
//!
//! desired {mesh1, mesh2, mesh3} -> {mesh2, mesh3} through one Build, mesh1 holding the fabric-primary role.
//! A desired change, never a recovery: no proven-drift Build names mesh1 and nothing recreates a
//! mesh1 node (subfeature `fabric-primary-handover`). Then `node.delete` of mesh1's last admin.
//!
//! R-NS1 (gossip.md, "Mesh delete"): after mesh.deleted the last admin, still its mesh's primary,
//! keeps publishing its mesh topology with itself the only node not listed `deleted` and MeshStatus
//! Dead, and every peer holds the deleted mesh with that one node while it lives. "The separate
//! node.delete of that admin, the fabric-primary as executor, ends its publishing: when the delete
//! reaches it, its final mesh topology, the mesh deleted and itself deleting, is its last gossip." The
//! role lands on the lowest NodeId among mesh2's and mesh3's primaries (the election's, never
//! chosen here). Every receiver read is made at BOTH surviving meshes, the fabric-primary's own and
//! the other, so every run covers both placements.

use rdm_core::PathName;
use std::collections::BTreeMap;
use rdm_test::estate::location;
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

/// Whether span `sp` is one of the birth at `at`: by its location, or by its node id (`ids`).
fn on(sp: &Value, ids: &BTreeMap<PathName, String>, at: &PathName) -> bool {
    rdm_test::estate::of_node(sp, at, ids.get(at).map(String::as_str).unwrap_or_default())
}

/// Whether span `sp` keys `action` on a subject that names `id`.
fn keyed(sp: &Value, action: &str, id: &str) -> bool {
    sp["attributes"]["operation"] == action && sp["attributes"]["operation_subject"].as_str().is_some_and(|o| o.contains(id))
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn names(meshes: &[&str]) -> BTreeSet<PathName> {
    meshes
        .iter()
        .flat_map(|m| [Estate::path(&format!("{m}.node-admin"), "1"), Estate::path(&format!("{m}.node-admin"), "2"), Estate::path(&format!("{m}.node"), "1"), Estate::path(&format!("{m}.node"), "2")])
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

/// CONTRACT: deleting the mesh that holds the fabric-primary role hands the role to a surviving mesh's
/// primary first, then deletes the mesh. The mesh.delete ends at mesh.deleted: the fabric-primary
/// validates the handoff and writes the MeshDeleted step event, and stops nothing of mesh1's final
/// primary, which stays alive, publishing mesh1's topology with itself the only node not listed
/// `deleted` and MeshStatus Dead, while every peer holds mesh1 with that one node. Its end is a separate
/// node.delete the fabric-primary executes: node.stop, exact exited verdict, its row Deleted,
/// NodeDeleted in `deleted`, then the fabric-primary's own mesh topology for mesh1; only then, with
/// every birth proven exited, mesh-left. No MeshLeft is published between the handoff and the final
/// birth's exited verdict.
///
/// Adversarial (R-NS2: "Final mesh topologies and leadership announcements are best-effort dissemination.
/// A stop does not wait for their delivery. Committed lifecycle facts remain recoverable after the
/// sender cuts gossip, and the executing authority updates its own view from the matching completion
/// and publishes that fact as its author: ... the fabric-primary for a mesh's last admin"): the last
/// admin's final mesh topology before its stop, itself stopping, is withheld (the fault service's
/// `announcement` cut) and its stop cuts its transport right behind it. The deleted mesh's final state
/// reaches every surviving mesh from the fabric-primary, which executed the delete and publishes it as
/// the author.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deleting_the_fabric_primarys_mesh_hands_the_fabric_primary_over_first_then_deletes_it() {
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    estate.resize_shape("fabric1", &[("mesh1", 2, 2), ("mesh2", 2, 2), ("mesh3", 2, 2)]).await;
    let before = Estate::values(&estate.settles_to_named(&names(&["mesh1", "mesh2", "mesh3"]), Duration::from_secs(30)).await);
    // A mesh delete waits for the Build in progress (Q170): the fabric is ready under this
    // Build before mesh1 goes.
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let fabric_id = estate.fabric_view().await.id.to_string();
    let (mesh1_id, mesh2_id, mesh3_id) = (estate.mesh_view("mesh1").await.id.expect("mesh1 carries its id").to_string(), estate.mesh_view("mesh2").await.id.expect("mesh2 carries its id").to_string(), estate.mesh_view("mesh3").await.id.expect("mesh3 carries its id").to_string());
    // Every birth's node id by location: a node's own spans name it by its id.
    let mut ids: BTreeMap<PathName, String> = before.iter().map(|n| (location(n), s(&n["node_id"]))).collect();
    let mesh1_pids: Vec<(PathName, u64)> = {
        let mut v = Vec::new();
        for n in before.iter().filter(|n| rdm_test::estate::mesh_of(&n) == "mesh1") {
            let name = location(&n);
            // The bootstrap admin was adopted on day 0: its pid is the estate's own child.
            let pid = match estate.bootstrap_pid() {
                Some(p) if name == Estate::path("mesh1.node-admin", "1") => u64::from(p),
                _ => estate.pid_of(&name).await,
            };
            v.push((name, pid));
        }
        v
    };

    // The Node rows mesh1's admin holds, read from its data dir while it runs.
    let mesh1_ids: Vec<(PathName, String)> = before.iter().filter(|n| rdm_test::estate::mesh_of(&n) == "mesh1").map(|n| (location(&n), s(&n["node_id"]))).collect();
    let mesh1_dir = estate.data_dir(&rdm_core::PathName::new("mesh1.node-admin", "1").unwrap()).await;
    let mesh1_bare: String = mesh1_id.split_once('_').map(|(_, r)| r).unwrap_or(&mesh1_id).replace('-', "").to_lowercase();

    // Control moves to mesh2 before mesh1 goes: its admins advertise it in the topology.
    let mesh2_admin = before
        .iter()
        .find(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin" && n["state"] == "ready")
        .map(|n| location(n))
        .expect("a ready mesh2 admin");
    estate.enter(&mesh2_admin).await;

    // {mesh1, mesh2, mesh3} -> {mesh2, mesh3}: one Build. It completes at mesh.deleted.
    let b = estate.resize_shape("fabric1", &[("mesh2", 2, 2), ("mesh3", 2, 2)]).await.build_id.to_string();
    let survivors_named = names(&["mesh2", "mesh3"]);
    // Every member of mesh1 but its final primary is deleted by mesh1's primary (mesh-delete.md step
    // 3, the whole node.delete: NodeDeleted from its exact exited verdict), so the view settles on mesh2,
    // mesh3 and at most mesh1's final primary.
    let settle_by = std::time::Instant::now() + Duration::from_secs(60);
    let after_b = loop {
        let read = estate.try_node_views().await;
        let nodes = read.as_ref().map(|views| Estate::values(views)).unwrap_or_default();
        let ours: BTreeSet<PathName> = nodes.iter().filter(|n| rdm_test::estate::mesh_of(&n) != "mesh1").map(location).collect();
        let ready = nodes.iter().filter(|n| rdm_test::estate::mesh_of(&n) != "mesh1").all(|n| n["state"] == "ready");
        if read.is_ok() && ours == survivors_named && ready && nodes.iter().filter(|n| rdm_test::estate::mesh_of(&n) == "mesh1").count() <= 1 {
            break nodes;
        }
        if std::time::Instant::now() >= settle_by {
            let held: Vec<String> = match &read {
                Ok(_) => nodes.iter().map(|n| format!("{:?}={}", location(n), s(&n["state"]))).collect(),
                Err(e) => vec![format!("the read ended: {e}")],
            };
            estate.stop().await;
            let spans = estate.spans();
            let deleted: Vec<String> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["build_id"] == b.as_str()).map(|sp| s(&sp["attributes"]["node"])).collect();
            let recoveries: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().map(|sp| format!("{} by {}: {}", s(&sp["attributes"]["node"]), s(&sp["attributes"]["submitter"]), s(&sp["attributes"]["outcome"]))).collect();
            panic!("the view never settled on mesh2, mesh3 and at most mesh1's final primary within 60 s after build {b}: {held:?}; NodeDeleted published under {b}: {deleted:?}; recovery submissions ({}): {:?}", recoveries.len(), recoveries.iter().take(3).collect::<Vec<_>>());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    // mesh.deleted leaves mesh1's final primary alive: the one mesh1 node the view still holds.
    let Some(last) = after_b.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh1").cloned() else {
        estate.stop().await;
        let spans = estate.spans();
        let stopped: Vec<String> = named(&spans, "rdm.node_admin.deployment.update.via-step")
            .into_iter()
            .filter(|sp| sp["attributes"]["build_id"] == b.as_str() && s(&sp["attributes"]["node_path"]) == "mesh1.node-admin" && sp["attributes"]["step"] == "TerminateRuntime")
            .map(|sp| format!("{}/{} TerminateRuntime trace {}", s(&sp["attributes"]["node_path"]), s(&sp["attributes"]["node_name"]), s(&sp["trace_id"])))
            .collect();
        let inside: Vec<String> = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().filter(|sp| sp["attributes"]["build_id"] == b.as_str()).map(|sp| s(&sp["attributes"]["node"])).collect();
        panic!("mesh.delete ends at mesh.deleted with the mesh-primary alive, but no mesh1 node is left after build {b}: the fabric-primary stopped the final primary inside the mesh.delete ({stopped:?}; shutdown-member workflows under {b}: {inside:?})");
    };
    let (final_name, final_id) = (location(&last), s(&last["node_id"]));
    assert!(final_name.path == "mesh1.node-admin", "the mesh1 node left alive is a node-admin, the final primary: {last}");
    let final_pid = mesh1_pids.iter().find(|(n, _)| *n == final_name).map(|(_, p)| *p).unwrap();
    assert!(alive(final_pid), "mesh1's final primary {final_name:?} still runs after mesh.deleted");
    let fabric_primary_id = estate.fabric_view().await.fabric_primary.expect("the fabric names its primary").to_string();
    let fabric_primary = after_b.iter().find(|n| n["node_id"] == fabric_primary_id.as_str()).map(location).expect("the fabric-primary is in the settled view");
    estate.enter(&fabric_primary).await;

    // Peers hold the deleted mesh with its last admin while it lives (R-NS1 item 1, R-MD1 item 3):
    // an admin of each surviving mesh, in what it holds itself (query.md §2), holds mesh1 deleted with
    // that one node: the mesh status it took from the gossiped MeshStatus, and the mesh1 births its
    // topology holds (`node.topology.get`), its deleted nodes listed `deleted` for the retention (Q390),
    // not held. fabric.get at it answers a primary's atlas, never its own
    // view (query.md §1; Q243). The admin lives on here until its node.delete, so this interval is held,
    // not raced.
    let survivors = ["mesh2", "mesh3"];
    for m in survivors {
        for admin in after_b.iter().filter(|n| rdm_test::estate::mesh_of(&n) == m && n["type"] == "node-admin") {
            let who = location(&admin);
            wait_for(&format!("{who:?} holds mesh1 dead with {final_name:?} its only node"), Duration::from_secs(30), || async {
                let dead = named(&estate.spans(), "rdm.mesh.topology.update.via-mesh-status").into_iter().any(|sp| on(sp, &ids, &who) && sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["status"] == "dead" && s(&sp["attributes"]["taken"]) == "true");
                let held = estate.held_topology_at(&who).await?;
                let mesh1: Vec<PathName> = held.members.iter().filter(|d| d.node.mesh == "mesh1" && d.status != rdm_core::model::NodeStatus::Deleted).map(|d| d.node.location()).collect();
                (dead && mesh1 == [final_name.clone()]).then_some(())
            })
            .await;
        }
    }
    assert!(alive(final_pid), "mesh1's final primary {final_name:?} still runs while every peer holds it");

    // The MeshDeleted step event, read from the live Build projection, its operation
    // `mesh.delete:<mesh_id>` keyed by the mesh's minted id; no MeshLeft yet.
    let mesh_delete = format!("mesh.delete:{mesh1_id}");
    let steps_of = |build: &Value, step: &str| -> Vec<Value> { build["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["step"] == step).collect() };
    let build_b = build_log::build_value(&estate, &b).await;
    let deleted_events = steps_of(&build_b, "MeshDeleted");
    assert_eq!(deleted_events.len(), 1, "one MeshDeleted step event under {b}: {build_b}");
    assert!(deleted_events[0]["operation"] == mesh_delete.as_str() && deleted_events[0]["outcome"] == "complete", "keyed by the mesh's minted id, complete: {}", deleted_events[0]);
    assert!(steps_of(&build_b, "MeshLeft").is_empty(), "no MeshLeft while the final primary lives: {build_b}");

    // The final primary's end: an ordinary node.delete, a separate call, the fabric-primary its
    // executor. The topology names no mesh1, so the call opens its own targeted run of the accepted
    // Build (as a delete of a stopped node does), with its own trace.
    let c_from = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let mut removal = estate.delete_node(&final_name).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.delete of {final_name:?}: {e}"));
    let accepted = Estate::complete(&mut removal).await.expect("the call opened a Build");
    let c = accepted.build_id.to_string();
    let after = Estate::values(&estate.settles_to_named(&names(&["mesh2", "mesh3"]), Duration::from_secs(60)).await);
    ids.extend(after.iter().map(|n| (location(n), s(&n["node_id"]))));

    // Identities: the Fabric, mesh2 and mesh3 keep theirs; mesh1 is gone.
    assert_eq!(estate.fabric_view().await.id.to_string(), fabric_id, "the Fabric keeps its id");
    assert_eq!(estate.mesh_view("mesh2").await.id.expect("mesh2 carries its id").to_string(), mesh2_id, "an untouched mesh keeps its id");
    assert_eq!(estate.mesh_view("mesh3").await.id.expect("mesh3 carries its id").to_string(), mesh3_id, "an untouched mesh keeps its id");
    estate.fabrics_obj().await.fact("mesh1 is no longer a mesh of the fabric", Duration::from_secs(30), |f| !f.meshes.iter().any(|m| m.name == "mesh1")).await.unwrap_or_else(|end| panic!("{end}"));
    assert!(!after.iter().any(|n| rdm_test::estate::mesh_of(&n) == "mesh1"), "no mesh1 node remains: {after:#?}");
    for (name, pid) in &mesh1_pids {
        wait_for(&format!("{name:?}'s runtime is gone"), Duration::from_secs(30), || async { (!alive(*pid)).then_some(()) }).await;
    }

    // Deleted is final and its members resolve Gone: an ordinary node of each surviving mesh answers
    // `gone` from its own live resolver for every deleted mesh1 birth, never `unknown`.
    for rx in [Estate::path("mesh2.node", "1"), Estate::path("mesh3.node", "1")] {
        for (name, id) in mesh1_ids.iter() {
            let query = format!("exact:{id}");
            let target = rdm_test::estate::path_target(&rx);
            wait_for(&format!("{rx:?}'s resolver answers Gone for deleted {name:?}"), Duration::from_secs(30), || async {
                let r = estate.probe(&["resolve", "--target", &target, "--query", &query]);
                (r["reply"]["resolution"] == "gone").then_some(())
            })
            .await;
        }
    }
    // mesh-left, once the final birth's exit was proven: one complete MeshLeft step event under the
    // mesh.delete's operation. The proof set it covers, every birth of the mesh with the final
    // primary included, is the records' and the events' (D67: a step event carries no output), read
    // below from the spans of the manifest and of mesh-left.
    let build_b = build_log::build_value(&estate, &b).await;
    let left_events = steps_of(&build_b, "MeshLeft");
    let other_events = steps_of(&build_b, "OtherMembersExited");
    estate.stop().await;
    let spans = estate.spans();
    assert_eq!(other_events.len(), 1, "one OtherMembersExited step event, written by mesh1's primary: {build_b}");
    assert!(other_events[0]["operation"] == mesh_delete.as_str() && other_events[0]["outcome"] == "complete", "{}", other_events[0]);
    assert_eq!(left_events.len(), 1, "one MeshLeft step event under {b}: {build_b}");
    assert!(left_events[0]["operation"] == mesh_delete.as_str() && left_events[0]["outcome"] == "complete", "keyed by the mesh's minted id, complete: {}", left_events[0]);
    // The records are read from the fabric's store root once the estate is stopped: its files stand,
    // and a red here is read from every process's spans.
    // R-MN1 (mesh-delete.md "Rows", steps 3 and 7): every birth of mesh1 is deleted and no row is
    // removed. Each mesh1 birth's Node record stays on the fabric's store root at its node id
    // (its lines in `nodes.jsonl` at its node id, R-SR1; storage-whole-records.md), written Deleted: the members' by
    // mesh1's primary, the final primary's by the fabric-primary in its node.delete. A deleted birth
    // never answers, so mesh-left takes the primaries of every birth its proof set names off its record
    // (`clear_primaries`), written from the node's latest line (Q356: the node cannot write it).
    assert!(std::path::Path::new(&format!("{mesh1_dir}/nodes/self.json")).exists(), "mesh1.node-admin.1's own Node row remains in {mesh1_dir}");
    for (name, id) in mesh1_ids.iter() {
        let row: Value = estate.node_record(id).unwrap_or_else(|| panic!("the Node row of {name:?} ({id}) remains in nodes.jsonl"));
        assert_eq!((s(&row["data"]["node_id"]), s(&row["data"]["state"])), (id.clone(), "deleted".to_string()), "{name:?}'s Node row is written Deleted: {row}");
        assert!(row["data"]["is_primary"].is_null() && row["data"]["is_fabric_primary"].is_null(), "mesh-left took {name:?}'s ({id}) primaries off its record: {row}");
    }
    // states.md: the mesh's `deleting` is entered by the delete-mesh down op, and "one commit stores
    // the row, applies it, gossips it and answers the op": mesh1's primary stored its own mesh row
    // deleting when it admitted delete-mesh, on mesh1's record in mesh.storage on the fabric's store root
    // (R14, R-SR1; storage-whole-records.md: the record's lines in `meshes.jsonl` at the mesh id, a line
    // for each state it entered).
    let record_at = estate.fabric_store_root().join(rdm_admin::store_root::MESHES_FILE);
    let row: Value = rdm_admin::store_log::read_record(&record_at, "mesh-record", Some(&mesh1_bare)).unwrap_or_else(|| panic!("mesh1's record is not in {}", record_at.display()));
    assert_eq!(s(&row["data"]["mesh_id"]), mesh1_bare.clone(), "{row}");
    let lines = rdm_admin::store_log::read_file(&record_at).unwrap_or_default();
    assert!(lines.iter().any(|l| l.kind() == "mesh-record" && l.subject() == mesh1_bare && l.data["state"] == "deleting"), "mesh1's primary stored deleting on its mesh's record: {row}");

    // The mesh.delete Build's executor is the fabric-primary; so is the node.delete's: the section that
    // ran `op` ended complete (Q346).
    let reconciles = named(&spans, "rdm.node_admin.build.update.via-reconcile");
    // A section's operations are named by action and subject.
    let converged = |build: &str, action: &str, subject: &str| -> String {
        let r = reconciles
            .iter()
            .find(|sp| sp["attributes"]["build_id"] == build && sp["attributes"]["outcome"] == "complete" && rdm_test::estate::planned_operations(sp).iter().any(|o| o.contains(&format!("action: {action:?}")) && o.contains(subject)))
            .cloned()
            .unwrap_or_else(|| panic!("a section ran {action} on {subject} under {build} to its end"));
        s(&r["attributes"]["executor"])
    };
    let owner_name = converged(&b, "mesh.delete", &mesh1_id);
    let delete_executor = converged(&c, "node.delete", &final_id);
    assert!(delete_executor.starts_with(&format!("{fabric_primary_id}/")), "the fabric-primary executes the final primary's node.delete (mesh1 has no other admin)");

    // mesh1's members left through the member workflows under B: every member stopped and
    // proven exited in the documented order, and none of the node-removal steps ran.
    let steps = named(&spans, "rdm.node_admin.deployment.update.via-step");
    // The node.delete's run: Build `c`'s steps from the call on; the mesh.delete's: every other step of
    // Build `b`.
    let node_delete_run = |sp: &Value| sp["attributes"]["build_id"] == c.as_str() && sp["start_unix_nano"].as_u64().is_some_and(|t| t >= c_from);
    let in_run = |sp: &Value, node_delete: bool| if node_delete { node_delete_run(sp) } else { sp["attributes"]["build_id"] == b.as_str() && !node_delete_run(sp) };
    let order_in = |name: &PathName, node_delete: bool| -> Vec<(String, u64, u64)> {
        let mut v: Vec<&Value> = steps.iter().copied().filter(|sp| on(sp, &ids, name) && in_run(sp, node_delete)).collect();
        v.sort_by_key(|sp| sp["start_unix_nano"].as_u64());
        v.iter().map(|sp| (s(&sp["attributes"]["step"]), sp["start_unix_nano"].as_u64().unwrap_or(0), sp["end_unix_nano"].as_u64().unwrap_or(0))).collect()
    };
    let members: Vec<PathName> = mesh1_pids.iter().map(|(n, _)| n.clone()).filter(|n| *n != final_name).collect();
    // mesh-delete.md step 3: each member's end is the whole node.delete, a node-admin's the same as
    // any member's: it holds no role, so no node.primary.update precedes it (R-MT1).
    for name in &members {
        let order = order_in(name, false);
        let got: Vec<&str> = order.iter().map(|(n, _, _)| n.as_str()).collect();
        let want: &[&str] = &["NodeDeleting", "StopNode", "AwaitNodeLeft", "TerminateRuntime", "NodeDeleted", "RemoveTopologyMembership", "Complete"];
        assert_eq!(got, want, "{name:?}: the ordinary node.delete under {b}, node.stop first, then the exited verdict and NodeDeleted");
    }
    // Each member's NodeDeleted is mesh1's primary's, before mesh.deleted; the deleted mesh's last
    // admin then holds no exited member to recover (node-recovery.md: recovery is for a member that
    // exits outside any workflow).
    let deleted_member: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["build_id"] == b.as_str()).collect();
    for name in &members {
        assert!(deleted_member.iter().any(|sp| on(sp, &ids, name) && keyed(sp, "node.delete", &ids[name])), "mesh1's primary published NodeDeleted for {name:?} under {b}: {deleted_member:?}");
    }
    let recoveries: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| s(&sp["attributes"]["submitter"]) == final_id).collect();
    assert!(recoveries.is_empty(), "the deleted mesh's last admin {final_name:?} submits no recovery of its deleted members: {} submissions, first {:?}", recoveries.len(), recoveries.first());
    // 1. mesh.deleted ends the mesh.delete: nothing of the final primary is stopped under B.
    assert!(order_in(&final_name, false).is_empty(), "the mesh.delete runs no step on its final primary {final_name:?} (its node.delete is Build {c}'s run from the call): {:?}", order_in(&final_name, false));
    let inside: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().filter(|sp| on(sp, &ids, &final_name)).collect();
    assert!(inside.is_empty(), "no shutdown-member workflow of the final primary {final_name:?}: {inside:?}");
    // Tiers: the ordinary nodes' workflows overlap in time; the other node-admin starts only after the
    // last ordinary node's exit is proven.
    let member = |name: &PathName| -> (u64, u64) {
        let sp = named(&spans, "rdm.node_admin.node.update.via-shutdown-member").into_iter().find(|sp| on(sp, &ids, name) && sp["attributes"]["operation"] == "mesh.delete").cloned().unwrap_or_else(|| panic!("no shutdown-member span for {name:?}"));
        (start_ns(&sp), end_ns(&sp))
    };
    let terminal_end = |name: &PathName, node_delete: bool| order_in(name, node_delete).iter().find(|(n, _, _)| n == "TerminateRuntime").map(|(_, _, e)| *e).unwrap_or_else(|| panic!("{name:?} has a TerminateRuntime step in the {} run", if node_delete { "node.delete" } else { "mesh.delete" }));
    let stop_start = |name: &PathName, node_delete: bool| order_in(name, node_delete).iter().find(|(n, _, _)| n == "StopNode").map(|(_, st, _)| *st).unwrap_or_else(|| panic!("{name:?} has a StopNode step in the {} run", if node_delete { "node.delete" } else { "mesh.delete" }));
    let (rpc1, rpc2) = (Estate::path("mesh1.node", "1"), Estate::path("mesh1.node", "2"));
    let (r1, r2) = (member(&rpc1), member(&rpc2));
    assert!(r1.0 < r2.1 && r2.0 < r1.1, "the ordinary nodes' workflows overlap in time: {r1:?} {r2:?}");
    let tier1_last = terminal_end(&rpc1, false).max(terminal_end(&rpc2, false));
    let tier2: Vec<&PathName> = members.iter().filter(|n| n.path == "mesh1.node-admin").collect();
    assert_eq!(tier2.len(), 1, "one other node-admin of mesh1: {members:?}");
    for n in &tier2 {
        assert!(stop_start(n, false) > tier1_last, "{n:?} (a node-admin) stopped only after every ordinary node's exit was proven");
    }
    let handoff = named(&spans, "rdm.node_admin.mesh.update.via-handoff-validated").into_iter().find(|sp| sp["attributes"]["operation"] == "mesh.delete").cloned().expect("the handoff was validated");
    assert_eq!(handoff["attributes"]["outcome"], "applied", "the owner validated the mesh-deleted handoff: {handoff}");
    // The completion follows the validated handoff, at the fabric-primary that owns the mesh.delete.
    let completion = named(&spans, "rdm.node_admin.mesh.update.via-mesh-deleted-receipt").into_iter().find(|sp| s(&sp["attributes"]["mesh_id"]) == mesh1_bare).cloned().unwrap_or_else(|| panic!("the fabric-primary recorded mesh.deleted's completion receipt for mesh1"));
    assert!(start_ns(&completion) >= end_ns(&handoff), "the MeshDeleted step event follows the validated handoff: {completion}");
    assert_eq!((attr_u(&completion, "receipts"), attr_u(&completion, "roster"), s(&completion["attributes"]["final_primary_node_id"])), (3, 4, final_id.clone()), "{completion}");
    // The reconcile names the owner's birth (`<node id>/<incarnation>`); the completion its node id.
    assert_eq!(Some(s(&completion["attributes"]["executor"]).as_str()), owner_name.split('/').next(), "the mesh.delete's owner records its completion: {completion}");
    eprintln!("bullet 1: mesh.deleted completion trace {} by {owner_name}, handoff trace {}", s(&completion["trace_id"]), s(&handoff["trace_id"]));

    let recorded = named(&spans, "rdm.node_admin.mesh.update.via-exit-manifest-recorded");
    assert!(recorded.iter().any(|sp| attr_u(sp, "receipts") == 3 && attr_u(sp, "roster") == 4), "the mesh primary recorded the exit manifest of its three other members: {recorded:?}");
    // A topology span names its node by its id: the ids of `m`'s births.
    let heard = |m: &str, name: &str| -> Vec<&Value> {
        let of_m: BTreeSet<&String> = ids.iter().filter(|(at, _)| at.path.starts_with(&format!("{m}."))).map(|(_, id)| id).collect();
        named(&spans, &format!("rdm.mesh.topology.update.{name}")).into_iter().filter(|sp| of_m.contains(&s(&sp["attributes"]["node"]))).collect()
    };
    for m in survivors {
        // R4: no mesh-level hold at a peer. A node learns mesh1's members are leaving from each
        // member's own ConnectionsDeleting and Leaving in what it installs, and routes none of them meanwhile.
        let states: Vec<&Value> = heard(m, "via-member-state").into_iter().filter(|sp| s(&sp["attributes"]["member_path"]).starts_with("mesh1.")).collect();
        let leaving_states: Vec<&Value> = states.iter().copied().filter(|sp| matches!(s(&sp["attributes"]["status"]).as_str(), "ConnectionsDeleting" | "Leaving")).collect();
        assert!(leaving_states.iter().all(|sp| s(&sp["attributes"]["routable"]) == "false" || sp["attributes"]["routable"] == json!(false)), "a mesh1 member saying connections-deleting or Leaving is not routable at {m}: {leaving_states:?}");
        // Each ordinary node holds a member's own state from what it installs, or it skipped those
        // versions and recovered the members' ends from mesh1's `deleted`: gossip.md "A node that missed a
        // lifecycle event recovers it from `TopologyFull.in_flight` or `TopologyFull.deleted`, through the
        // next delta, full or top-up". A delete that ends between two of mesh1's versions it installs
        // reaches it as the next one, mesh1 with every other member deleted, and it holds none of their
        // transient states; which frame brought it is best-effort dissemination (R-NS2).
        for ordinary in [Estate::path(&format!("{m}.node"), "1"), Estate::path(&format!("{m}.node"), "2")] {
            let held_here = leaving_states.iter().any(|sp| on(sp, &ids, &ordinary));
            let recovered: Vec<&Value> = ["rdm.mesh.topology.update.via-full", "rdm.mesh.topology.update.via-delta"]
                .iter()
                .flat_map(|n| named(&spans, n))
                .filter(|sp| on(sp, &ids, &ordinary) && sp["attributes"]["mesh"] == "mesh1" && attr_u(sp, "deleted") >= members.len() as u64)
                .collect();
            assert!(
                held_here || !recovered.is_empty(),
                "{ordinary:?} held a mesh1 member's own connections-deleting or Leaving, or installed mesh1 with all {} of its other members deleted through a delta, full or top-up: {leaving_states:?}",
                members.len()
            );
        }
    }
    let routable = |sp: &Value| s(&sp["attributes"]["routable"]) == "true" || sp["attributes"]["routable"] == json!(true);
    let installed = named(&spans, "rdm.mesh.topology.update.via-full");

    // 1. While it lives: after mesh.deleted the last admin keeps publishing
    // mesh1's topology with itself the only node not listed `deleted` and MeshStatus Dead (Q390: the
    // aggregate lists the deleted nodes for the retention). Every peer held mesh1 dead with that one
    // node while it lived (read above, on each surviving admin's own view); which frame brought it
    // there is best-effort dissemination (R-NS2), so no particular one is expected.
    let final_exit = terminal_end(&final_name, true);
    let lone: Vec<&Value> = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1" && on(sp, &ids, &final_name) && attr_u(sp, "members").saturating_sub(attr_u(sp, "deleted")) == 1 && start_ns(sp) < final_exit).collect();
    assert!(!lone.is_empty(), "the last admin {final_name:?} published mesh1's topology with itself the only node not listed deleted while it lived");
    let dead_row: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-deleted-row").into_iter().filter(|sp| on(sp, &ids, &final_name) && s(&sp["attributes"]["mesh_id"]) == mesh1_bare).collect();
    assert!(dead_row.iter().any(|sp| s(&sp["attributes"]["outcome"]) == "applied" && start_ns(sp) <= start_ns(&completion)), "the last admin {final_name:?} committed mesh1's row deleted at mesh.deleted: {dead_row:?}");
    let dead_sent: Vec<&Value> = named(&spans, "rdm.mesh.fabric.update.via-status-send").into_iter().filter(|sp| on(sp, &ids, &final_name) && sp["attributes"]["scope"] == "mesh:mesh1" && sp["attributes"]["status"] == "dead").collect();
    assert!(dead_sent.iter().any(|sp| start_ns(sp) < final_exit), "the last admin {final_name:?} sent MeshStatus Dead for mesh1 while it lived: {dead_sent:?}");
    eprintln!("bullet 1: {final_name:?} lone mesh topology trace {}, MeshStatus Dead trace {}", s(&lone[0]["trace_id"]), dead_sent.first().map(|sp| s(&sp["trace_id"])).unwrap_or_default());

    // 2. The final primary's node.delete under C, executed by the fabric-primary: its whole node.stop
    // (the delete of a mesh's only node skips node.primary.update, R-MT1: "delete is skipped if it's
    // the only node on the mesh"; its role stays with it), the provider's exact exited verdict, its row
    // Deleted, NodeDeleted, its removal.
    let order = order_in(&final_name, true);
    let got: Vec<&str> = order.iter().map(|(n, _, _)| n.as_str()).collect();
    let moved: Vec<&Value> = ["rdm.node_admin.primary.update.via-command", "rdm.node_admin.primary.update.via-update-primary"]
        .iter()
        .flat_map(|n| named(&spans, n))
        .filter(|sp| (s(&sp["attributes"]["primary_node_id"]) == final_id || on(sp, &ids, &final_name)) && s(&sp["attributes"]["build_id"]) == c && sp["start_unix_nano"].as_u64().is_some_and(|t| t >= c_from))
        .collect();
    assert!(moved.is_empty(), "{final_name:?} is its mesh's only node: its delete runs no node.primary.update: {moved:#?}");
    assert_eq!(got, ["NodeDeleting", "StopNode", "AwaitNodeLeft", "TerminateRuntime", "NodeDeleted", "RemoveTopologyMembership", "Complete"], "{final_name:?}: the ordinary node.delete, node.stop first, then the exited verdict");
    let c_steps: Vec<&Value> = steps.iter().copied().filter(|sp| on(sp, &ids, &final_name) && node_delete_run(sp)).collect();
    assert!(c_steps.iter().all(|sp| sp["attributes"]["outcome"] == "complete"), "{final_name:?}: every step of its node.delete completed: {c_steps:?}");
    let row_deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| rdm_test::estate::names_location(sp, "subject", &final_name) && on(sp, &ids, &fabric_primary)).collect();
    assert!(row_deleted.iter().any(|sp| sp["attributes"]["outcome"] == "row-deleted" && start_ns(sp) >= final_exit), "the fabric-primary {fabric_primary:?} wrote {final_name:?}'s row Deleted after its exited verdict: {row_deleted:?}");
    let node_deleted = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().find(|sp| on(sp, &ids, &final_name) && s(&sp["attributes"]["build_id"]) == c).cloned().unwrap_or_else(|| panic!("NodeDeleted was published for {final_name:?} under {c}"));
    assert!(start_ns(&node_deleted) >= final_exit, "NodeDeleted follows the exited verdict: {node_deleted}");

    // mesh-left once every birth is proven exited: MeshLeft over all four births at the
    // fabric-primary, after the final birth's exited verdict.
    let left_recorded: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-mesh-left-receipt").into_iter().filter(|sp| attr_u(sp, "receipts") == 4 && attr_u(sp, "roster") == 4).collect();
    assert!(left_recorded.iter().any(|sp| start_ns(sp) >= final_exit), "the fabric-primary recorded MeshLeft over all four births after the final birth's exited verdict: {left_recorded:?}");
    // mesh-left takes the primaries of the births its proof set names off their records, the final primary included.
    let dropped = named(&spans, "rdm.node_admin.node.remove.via-mesh-left-contact")
        .into_iter()
        .find(|sp| keyed(sp, "mesh.delete", &mesh1_id) && sp["attributes"]["error"].is_null())
        .cloned()
        .unwrap_or_else(|| panic!("mesh-left took mesh1's births' primaries off their records under mesh.delete:{mesh1_bare}"));
    for (name, id) in mesh1_ids.iter() {
        assert!(s(&dropped["attributes"]["dropped"]).split(", ").any(|d| d.contains(id.as_str())), "mesh-left took the primaries of {name:?} ({id}) off its record: {dropped}");
    }
    // e12.s72: the deleted mesh's last admin publishes the final mesh topology (`published == "true"`,
    // `status == "Leaving"`, carrying `node.delete:<final_name>` in `in_flight`) after its stop's
    // `node.connection.delete { All }` and before its transport cut.
    let final_cut = named(&spans, "rdm.node_admin.node.update.via-stop-transport-cut").into_iter().filter(|sp| on(sp, &ids, &final_name)).map(|sp| start_ns(sp)).min().expect("the final primary's transport was cut by its stop");
    let final_all_end = named(&spans, "rdm.node_admin.connection.delete.via-stop").into_iter().filter(|sp| on(sp, &ids, &final_name)).map(|sp| end_ns(sp)).min().expect("the final primary's stop ran its node.connection.delete { All }");
    let leaving_aggregate: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-leaving-aggregate").into_iter().filter(|sp| on(sp, &ids, &final_name)).collect();
    assert!(
        leaving_aggregate.iter().any(|sp| sp["attributes"]["mesh"] == "mesh1" && sp["attributes"]["status"] == "Leaving" && attr_u(sp, "members") == 1 && sp["attributes"]["published"] == "true" && start_ns(sp) >= final_all_end && end_ns(sp) <= final_cut),
        "the last admin {final_name:?} published mesh1's final mesh topology carrying itself Leaving after its All and before its stop's cut: {leaving_aggregate:?}"
    );
    let final_stopped_at = order.iter().find(|(n, _, _)| n == "StopNode").map(|(_, _, e)| *e).expect("the final primary's stop was answered");
    let final_after_stop: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-member-state").into_iter().filter(|sp| (rdm_test::estate::names_location(sp, "member", &final_name) || s(&sp["attributes"]["member_id"]) == final_id) && start_ns(sp) > final_stopped_at).collect();
    assert!(final_after_stop.iter().all(|sp| !routable(sp)), "no node holds the final primary {final_name:?} routable after its stop: {final_after_stop:?}");
    let _ = installed;

    // gossip.md, node-stopped lifecycle overlay: mesh1's primary publishes NodeStopped from each
    // member's matching stopped reply, for every member it stops, and a new mesh1 version carries it.
    let published_stopped = named(&spans, "rdm.node_admin.node.update.via-node-stopped");
    let stopped_at = |m: &PathName| -> u64 {
        published_stopped.iter().find(|sp| on(sp, &ids, m) && keyed(sp, "node.stop", &ids[m])).map(|sp| start_ns(sp)).unwrap_or_else(|| panic!("mesh1's primary published NodeStopped for {m:?}: {published_stopped:?}"))
    };
    let mesh1_publications: Vec<u64> = named(&spans, "rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1").map(|sp| start_ns(sp)).collect();
    let mesh1_wakes: Vec<u64> = named(&spans, "rdm.mesh.backbone.update.via-material-change").into_iter().filter(|sp| sp["attributes"]["mesh"] == "mesh1").map(|sp| start_ns(sp)).collect();
    assert!(!mesh1_wakes.is_empty(), "mesh1's primary's checklist was woken by its own projection's move");
    for m in &members {
        let at = stopped_at(m);
        assert!(mesh1_publications.iter().any(|p| *p >= at), "mesh1's primary published a new version after {m:?}'s NodeStopped at {at}: publications {mesh1_publications:?}");
        let after: Vec<&Value> = named(&spans, "rdm.mesh.topology.update.via-member-state").into_iter().filter(|sp| (rdm_test::estate::names_location(sp, "member", m) || s(&sp["attributes"]["member_id"]) == ids[m]) && start_ns(sp) > at).collect();
        assert!(after.iter().all(|sp| !routable(sp)), "no node holds {m:?} routable after its NodeStopped: {after:?}");
    }
    // The Leaving row is the delete-mesh admission's commit, before the first member stops.
    let leaving_commit: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-leaving-row").into_iter().filter(|sp| s(&sp["attributes"]["mesh_id"]) == mesh1_bare).collect();
    assert!(leaving_commit.iter().any(|sp| on(sp, &ids, &final_name) && s(&sp["attributes"]["outcome"]) == "applied"), "mesh1's primary {final_name:?} applied its mesh row Leaving at delete-mesh: {leaving_commit:?}");
    let first_member_stop = [&rpc1, &rpc2].iter().map(|n| stop_start(n, false)).min().unwrap();
    assert!(leaving_commit.iter().all(|sp| end_ns(sp) < first_member_stop), "the Leaving row was committed before any member stopped: {leaving_commit:?}");

    // lifecycles/node-primary-update.md "The fabric-primary" and mesh-delete.md step 1: when the mesh is
    // the fabric-primary's, node.fabric-primary.update runs first with this mesh excluded from the
    // successor search; the incumbent stays mesh-primary until its own node.delete. In order at mesh1's
    // primary: its issued writes finished, then the outside successor installs the fabric-primary alone
    // and answers node.primary.updated under the primary's call; the leave's owner finds the
    // fabric-primary no node-admin of mesh1 (`via-fabric-primary-left`) before DeleteMesh and every stop.
    let attr_s = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let operation = "node.fabric-primary.update";
    let one = |name: &str| -> Value {
        let v: Vec<&Value> = named(&spans, name).into_iter().filter(|sp| attr_s(sp, "operation") == operation).collect();
        assert_eq!(v.len(), 1, "exactly one {name} span of {operation}: {v:?}");
        v[0].clone()
    };
    // mesh1's last admin stays its mesh's primary to its own stop: it reports its stop to no one,
    // itself included, since after its cut it publishes nothing (node-stop.md step 4; i146 Q37), and
    // its NodeStopped is published once, by its commander.
    let own_primary: Vec<&Value> = named(&spans, "rdm.node_admin.status.reject.via-own-mesh-primary").into_iter().filter(|sp| on(sp, &ids, &final_name) && attr_s(sp, "operation") == "node.stop").collect();
    assert_eq!(own_primary.len(), 1, "{final_name:?}'s stop is not reported to itself: {own_primary:#?}");
    let final_stopped: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-stopped").into_iter().filter(|sp| on(sp, &ids, &final_name)).collect();
    assert_eq!(final_stopped.len(), 1, "{final_name:?}'s NodeStopped is published once: {final_stopped:#?}");
    let updating = one("rdm.node_admin.primary.update.via-update-primary");
    assert_eq!((on(&updating, &ids, &final_name), attr_s(&updating, "moving"), attr_s(&updating, "successor"), attr_s(&updating, "outcome")), (true, "fabric-primary".to_string(), fabric_primary_id.clone(), "updated".to_string()), "mesh1's primary moved the fabric-primary alone to the outside successor: {updating}");
    let finished = one("rdm.node_admin.primary.update.via-issued-writes-finished");
    let installed: Vec<&Value> = named(&spans, "rdm.node_admin.primary.update.via-primary-updated").into_iter().filter(|sp| attr_s(sp, "operation") == operation && on(sp, &ids, &fabric_primary) && attr_s(sp, "outcome") == "held").collect();
    assert_eq!(installed.len(), 1, "the outside successor installed the fabric-primary once and answered node.primary.updated: {installed:?}");
    let installed = installed[0].clone();
    assert!(attr_s(&installed, "primaries").starts_with("fabric ") && !attr_s(&installed, "primaries").contains("node-admin "), "the fabric-primary alone moves; mesh1's mesh-primary stays: {installed}");
    assert!(descends_from(&spans, &installed, &updating), "the successor installed under the primary's call: {installed}");
    let left = one("rdm.node_admin.primary.update.via-fabric-primary-left");
    assert_eq!(attr_s(&left, "departing"), "mesh1", "the leave's owner found the fabric-primary no node-admin of mesh1: {left}");
    assert!(end_ns(&finished) <= start_ns(&installed), "the issued writes finished, then the successor's install");
    assert!(end_ns(&installed) <= start_ns(&left) && end_ns(&left) < first_member_stop, "the fabric-primary left mesh1 before DeleteMesh and every stop");
    assert!(named(&spans, "rdm.node_admin.primary.reject.via-primary-update").is_empty(), "no node.primary.update was refused");
    // Commands go down: every 0x21 call is the primary's node.primary.update on its successor, but the
    // mesh-primary's create commanding a type's first node in its mesh (Q407, `via-create`).
    let first_primaries = named(&spans, "rdm.node_admin.primary.update.via-create");
    let calls: Vec<&Value> = named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| attr_u(sp, "op") == 0x21).filter(|sp| !first_primaries.iter().any(|create| descends_from(&spans, sp, create))).collect();
    assert!(!calls.is_empty() && calls.iter().all(|sp| descends_from(&spans, sp, &updating)), "every 0x21 call is the primary's call on its successor: {calls:?}");

    // A desired change, not a recovery: no node recovery of a mesh1 birth (node recovery is the
    // mesh-primary's own workflow, Q342).
    let mesh1_recovered: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| mesh1_ids.iter().any(|(_, id)| s(&sp["attributes"]["node_id"]) == *id)).collect();
    assert!(mesh1_recovered.is_empty(), "no node recovery of a mesh1 birth: {mesh1_recovered:?}");
    let created = named(&spans, "rdm.node_admin.node.create.via-build");
    assert!(!created.iter().any(|sp| (sp["attributes"]["build_id"] == b.as_str() || sp["attributes"]["build_id"] == c.as_str()) && s(&sp["attributes"]["node_path"]).starts_with("mesh1.")), "nothing recreates a mesh1 node");

    // rafka-time: the fabric-primary role moved from the deleted mesh1 to the lowest NodeId among the
    // remaining mesh primaries (R-A2). The other remaining mesh primary, which does not hold the role,
    // pulled once from the new fabric-primary and adopted its time under the mesh-primary rule.
    let primary = fabric_primary.clone();
    let primary_id = after.iter().find(|n| location(n) == primary).map(|n| s(&n["node_id"])).unwrap_or_else(|| panic!("the fabric primary {primary:?} is in the settled view"));
    let (m2, m3) = (Estate::path("mesh2.node-admin", "1"), Estate::path("mesh3.node-admin", "1"));
    assert!([&m2, &m3].contains(&&primary), "the role left the deleted mesh1 for a remaining mesh primary: {primary:?}");
    let other = if primary == m2 { m3.clone() } else { m2.clone() };
    let other_id = ids[&other].clone();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let moved: Vec<&Value> = named(&spans, "rdm.mesh.entry.resolve.via-fabric-primary-changed").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &other) && attr(sp, "to") == primary_id).collect();
    assert_eq!(moved.len(), 1, "{other:?} pulled once from {primary:?} when the fabric-primary role moved to it: {moved:?}");
    assert_eq!(attr(moved[0], "outcome"), "adopted", "{:?}", moved[0]);
    let adopted = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted");
    let child = adopted.iter().find(|a| descends_from(&spans, a, moved[0])).expect("the adoption is a child of the pull");
    assert_eq!(
        (attr(child, "node").as_str(), attr(child, "source").as_str(), attr(child, "via").as_str(), attr(child, "kind").as_str(), attr(child, "served_by").as_str()),
        (other_id.as_str(), "pull", "get-topology", "fabric-primary", primary_id.as_str())
    );

    // The successor is the primary's exact birth (gossip.md "A primary change on gossip": its digest then
    // carries `is_fabric_primary`, and the other remaining mesh-primary pulled from it above).
    let h = after.iter().find(|n| location(n) == primary).unwrap();
    assert!(attr_s(&installed, "primaries").contains(&format!("-> {}/{} @", s(&h["node_id"]), s(&h["incarnation"]))), "the successor is the primary's exact birth: {installed} {h}");
    estate.record_trace_url(&s(&installed["trace_id"]));
}
