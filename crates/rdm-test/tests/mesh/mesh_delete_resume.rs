//! i143.e12.s39 (#3066) process E2E: a mesh leave resumed after the leaving mesh's primary is lost
//! mid-leave (R11; mesh-delete.md, "Resume after the primary's loss").
//!
//! desired {mesh1, mesh2} -> {mesh1}: the fabric-primary in mesh1 owns `mesh.delete:<mesh2 id>`.
//! mesh2's primary is held by its fault door in tier 1 (rpc.2's node.delete StopNode receipt) and
//! SIGKILLed there by exact pid; the door dies with it. The owner's attempt ends the moment its view
//! holds the lost primary unheard (`via-final-primary-lost`), never on the hand-off bound, and the next
//! attempts delete the lost primary by its node.delete, amend the roster with the new final
//! primary once it is seated (or created, when none survives), and finish at mesh.deleted: its
//! completion covers the accepted roster plus the amendment but the new final primary, which stays
//! alive. Its end is its own node.delete (mesh-delete.md step 7), after which MeshLeft covers every
//! birth, both primary births included.
//!
//! This stem is form (a): a surviving admin of mesh2 the election seats. Form (b), no admin of mesh2
//! survives and the fabric-primary creates the one node-admin the leaving mesh may gain, is
//! `mesh_lifecycle__mesh_delete_no_survivor`, over the shared run and assertions here. Form (c) is
//! form (a) with a member's delete cut right after its exited verdict: the primary is killed holding
//! rpc.1 at its NodeDeleted receipt, so the Build holds rpc.1's exact Exited (TerminateRuntime) and
//! no NodeDeleted.

use crate::build_log;
use rdm_client::{FabricDesired, Frame, LaunchKind as NodeKind, MeshDesired, PathName};
use rdm_test::typed::BuildSpec;
use rdm_test::estate::{named, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha, Door};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::time::Duration;

pub(crate) fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-delete-resume".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

pub(crate) fn s(v: &Value) -> String {
    v.as_str().map(str::to_string).unwrap_or_else(|| if v.is_null() { String::new() } else { v.to_string() })
}

pub(crate) fn attr_u(sp: &Value, k: &str) -> u64 {
    sp["attributes"][k].as_str().and_then(|v| v.parse().ok()).or_else(|| sp["attributes"][k].as_u64()).unwrap_or(0)
}

pub(crate) fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

/// Where mesh2's primary holds rpc.1 when it is killed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rpc1 {
    /// Not held: rpc.1's delete runs on whatever the kill finds it at.
    Free,
    /// Stopped (its stop-node answered), its process alive: held at its StopNode receipt, with no
    /// Exited receipt and no NodeStopped published.
    Stopped,
    /// Exited and its exact Exited receipted (TerminateRuntime), the delete cut after NodeDeleted's
    /// work and before NodeDeleted's receipt is durable: the Build holds the exited verdict and no
    /// NodeDeleted, and none is published. The fault door's `Receipt` cut holds BEFORE the named
    /// receipt is durable (`FaultedBuilds::append_step_receipt`), so the cut that leaves
    /// TerminateRuntime durable is the next step's: NodeDeleted.
    ExitProven,
}

/// Build `id` as the node-admin the estate asks holds it now (its own Build log over `FetchBuildFacts`), as
/// the JSON its view serializes to; a read that ends, or a log that holds no acceptance of it, is recorded
/// as such: nothing here panics before the estate is stopped.
async fn build_now(estate: &Estate, id: &str) -> Value {
    match build_log::log_read(&estate.carrier().await, id).await {
        Ok(facts) => build_log::projection(&facts, id).map(|p| serde_json::to_value(p).expect("a Build view serializes")).unwrap_or_else(|| json!({"unaccepted": facts.len()})),
        Err(end) => json!({"read_ended": end.to_string()}),
    }
}

/// What one form's run leaves to assert on.
pub(crate) struct Run {
    pub(crate) build: Value,
    pub(crate) spans: Vec<Value>,
    pub(crate) mesh2_id: String,
    pub(crate) lost: (String, String),
    pub(crate) b: String,
    /// mesh2's final primary after the amendment, alive after mesh.deleted until its node.delete.
    pub(crate) last: String,
}

/// Build {mesh1, mesh2 with `mesh2_admins` admins}, arm mesh2's primary's door at rpc.2's StopNode
/// receipt (and, by `rpc1`, at rpc.1's StopNode receipt: rpc.1 stopped, its process alive, no Exited
/// receipt, and its NodeStopped not yet published; or at rpc.1's NodeDeleted receipt: rpc.1's exit
/// proof receipted and no NodeDeleted), submit {mesh1}, kill the held primary by exact pid once every
/// cut holds, and wait for the Build to complete on a later attempt.
pub(crate) async fn primary_lost_mid_leave(test: &str, mesh2_admins: u32, rpc1: Rpc1) -> Run {
    let sha = candidate_sha();
    // Every path is named its executable; the one node-admin the fabric-primary creates for a deleting
    // mesh no admin of which survives (mesh-delete.md resume step 2) runs the executable its lost
    // predecessor's row records (node-recovery.md "Retained"; `admin/mesh_delete.rs`
    // `create_deleting_mesh_admin`).
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let mesh1 = MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2)]);
    let mesh2 = MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, mesh2_admins), (NodeKind::RpcNode, 2)]);
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1.clone(), mesh2] }).await;
    let mut want: BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh2.rpc.1", "mesh2.rpc.2"].iter().map(|n| n.to_string()).collect();
    want.extend((1..=mesh2_admins).map(|i| format!("mesh2.admin.{i}")));
    let before = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(60)).await);
    // A mesh delete waits for the Build in progress (Q170): the fabric is ready before mesh2
    // goes, so the admin seated after the primary's loss reads mesh2 deleting from its mesh's own rows.
    estate.until_fabric_ready(Duration::from_secs(90)).await;
    let mesh2_id = estate.mesh_view("mesh2").await.id.map(|i| i.to_string()).unwrap_or_else(|| panic!("mesh2 has a minted id"));
    let primary = before.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).cloned().unwrap_or_else(|| panic!("mesh2 has an admin primary: {before:#?}"));
    let lost = (s(&primary["name"]), s(&primary["incarnation_id"]));
    let door = Door::at(&estate, &lost.0).await;

    // Held in tier 1: rpc.2's stop answered `stopped` (its node.connection.delete { All } and its
    // transport cut done, its process alive) and its StopNode receipt is not yet durable, so a resumed
    // attempt sends its stop-node again and the stopped birth answers the same receipt; the other
    // admin (form a) is still ready.
    let cut = "leave:rpc2-StopNode";
    // A member's end is its node.delete, under node.delete's own operation (mesh-delete.md step 3).
    door.arm(cut, json!({"kind": "receipt", "step": "StopNode", "operation": "node.delete:mesh2.rpc.2"})).await;
    // rpc.1's stop completed (its stop-node answered stopped: its connections deleted, its transport
    // cut, its process alive) and its workflow held at that receipt: its NodeStopped, published at the
    // next step, is never published, and it has no Exited receipt. Nothing any admin holds names it.
    let stopped = "leave:rpc1-StopNode";
    if rpc1 == Rpc1::Stopped {
        door.arm(stopped, json!({"kind": "receipt", "step": "StopNode", "operation": "node.delete:mesh2.rpc.1"})).await;
    }
    // rpc.1's whole delete ran to its exited verdict: the runtime is terminated, the provider's exact
    // Exited is receipted, and the executor is held in NodeDeleted before its receipt: the Build
    // holds the exited verdict and no NodeDeleted receipt, and `NodeDeleted` is never published.
    let exit_proven = "leave:rpc1-NodeDeleted";
    if rpc1 == Rpc1::ExitProven {
        door.arm(exit_proven, json!({"kind": "receipt", "step": "NodeDeleted", "operation": "node.delete:mesh2.rpc.1"})).await;
    }
    // The leave's stream is held to the end of the run: the Build is read as the admin the estate asks
    // holds it, and the stream's attempt ends when the primary is lost.
    let leave = estate.submit(&BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1] })).await;
    let b = leave.build_id().to_string();
    let held = door.wait_held(cut).await;
    assert_eq!(held["hit"]["build_id"], b.as_str(), "the held receipt is the leave's: {held}");
    if rpc1 == Rpc1::Stopped {
        let held = door.wait_held(stopped).await;
        assert_eq!(held["hit"]["build_id"], b.as_str(), "the held receipt is the leave's: {held}");
    }
    if rpc1 == Rpc1::ExitProven {
        let held = door.wait_held(exit_proven).await;
        assert_eq!((&held["hit"]["build_id"], &held["hit"]["step"], &held["hit"]["operation"]), (&json!(b), &json!("NodeDeleted"), &json!("node.delete:mesh2.rpc.1")), "the held receipt is rpc.1's NodeDeleted of the leave: {held}");
    }
    // The door dies with the process: it is never released.
    let pid = estate.pid_of(&lost.0).await;
    estate.kill_pid(u32::try_from(pid).unwrap());

    // Attempt 1 ends once the owner's view holds the primary proven exited; a later attempt
    // completes. The bound is below the owner's 120 s hand-off bound: a resume that waited on it is
    // red here. Nothing here panics before the estate is stopped: every process flushes its spans on
    // exit, and a red run is read from them.
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    let mut build = Value::Null;
    while std::time::Instant::now() < deadline {
        build = build_now(&estate, &b).await;
        if build["state"] == "complete" && build["attempt"].as_u64().unwrap_or(0) >= 2 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // mesh.delete ends at mesh.deleted: the view settles on mesh1 and mesh2's new final primary,
    // alive; its end is its own node.delete (mesh-delete.md step 7), then the view converges on
    // mesh1 alone within gossip delay of mesh-left.
    let mut view: Vec<String> = Vec::new();
    let mut mesh2_gone = false;
    let mut last: Option<String> = None;
    let mut deleted = Value::Null;
    if build["state"] == "complete" {
        let settle = std::time::Instant::now() + Duration::from_secs(60);
        while std::time::Instant::now() < settle && last.is_none() {
            // The lost primary's exit is recorded, not published deleted: it may still be held
            // unheard until mesh-left. The final primary is the one mesh2 birth that is alive.
            let nodes = estate.try_node_views().await.map(|v| Estate::values(&v)).unwrap_or_default();
            let mesh2: Vec<String> = nodes.iter().filter(|n| n["mesh"] == "mesh2" && n["status"] == "ready").map(|n| s(&n["name"])).collect();
            let deleting: Vec<&Value> = nodes.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "rpc_node").collect();
            if mesh2.len() == 1 && deleting.is_empty() {
                last = mesh2.into_iter().next();
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    if let Some(last) = &last {
        let path: PathName = last.parse().unwrap_or_else(|e| panic!("{last} is not a path.name: {e:?}"));
        deleted = match estate.nodes_obj().await.delete(&path).await {
            Ok(mut stream) => {
                let accepted = stream.accepted().clone();
                let (mut frames, mut end) = (Vec::new(), None);
                while let Some(f) = stream.next().await {
                    match f {
                        Ok(f) => frames.push(f),
                        Err(e) => {
                            end = Some(e.to_string());
                            break;
                        }
                    }
                }
                let state = match frames.last() {
                    Some(Frame::Complete) => "complete",
                    Some(Frame::Failed { .. }) => "failed",
                    _ => "ended",
                };
                json!({"build_id": accepted.build_id.to_string(), "attempt": accepted.attempt, "state": state, "frames": format!("{frames:?}"), "end": end})
            }
            Err(e) => json!({"refused": e.to_string()}),
        };
        let settle = std::time::Instant::now() + Duration::from_secs(60);
        while std::time::Instant::now() < settle {
            view = match estate.try_node_views().await {
                Ok(views) => views.iter().map(|n| format!("{}={:?}", n.name, n.status)).collect(),
                Err(e) => vec![format!("the read ended: {e}")],
            };
            mesh2_gone = matches!(rdm_test::typed::one_mesh(&estate.meshes_obj().await, "mesh2").await, Ok(None));
            if mesh2_gone && view.iter().all(|n| n.starts_with("mesh1.")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
    let build = if build["state"] == "complete" { build_now(&estate, &b).await } else { build };
    drop(leave);
    estate.stop().await;
    let spans = estate.spans();
    assert!(build["state"] == "complete" && build["attempt"].as_u64().unwrap_or(0) >= 2, "build {b} completes on an attempt after the first within 90 s: {build}");
    let last = last.unwrap_or_else(|| panic!("mesh.deleted leaves mesh2's final primary alive, the one mesh2 node the view holds after build {b}"));
    assert_eq!(deleted["state"], "complete", "the node.delete of mesh2's final primary {last} completes: {deleted}");
    assert!(view.iter().all(|n| n.starts_with("mesh1.")), "the view holds no mesh2 birth after mesh-left: {view:?}");
    assert!(mesh2_gone, "mesh2 is no longer a mesh of the fabric: mesh.get [name: mesh2] answers the empty list");
    Run { build, spans, mesh2_id, lost, b, last }
}

/// The receipts and spans both forms share: the lost primary's exit recorded before the
/// amendment, one amendment admitting `next`, mesh.deleted's completion over every birth but `next`,
/// and, after `next`'s own node.delete, a MeshLeft covering the accepted roster plus it.
pub(crate) fn assert_resumed(run: &Run, next: &str, roster: usize) -> Value {
    let Run { build, spans, mesh2_id, lost, b, last } = run;
    assert_eq!(last, next, "mesh2's final primary alive after mesh.deleted is the amended one");
    let outer = format!("mesh.delete:{mesh2_id}");
    let steps: Vec<Value> = build["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["operation"] == outer.as_str()).collect();
    let step = |name: &str| -> Vec<&Value> { steps.iter().filter(|r| r["step"] == name).collect() };
    let amended = step("RosterAmended");
    assert_eq!(amended.len(), 1, "one RosterAmended receipt under {outer}: {build}");
    assert_eq!(s(&amended[0]["output"]["predecessor"]["node"]), lost.0, "the amendment names the lost primary: {}", amended[0]);
    assert_eq!(s(&amended[0]["output"]["predecessor"]["incarnation_id"]), lost.1, "the amendment names the lost primary's exact birth: {}", amended[0]);
    assert_eq!(s(&amended[0]["output"]["final_primary"]["node"]), next, "the amendment admits {next}: {}", amended[0]);
    let completion = step("MeshDeleted");
    assert_eq!(completion.len(), 1, "one MeshDeleted completion under {outer}: {build}");
    assert_eq!(s(&completion[0]["output"]["final_primary"]["node"]), next, "mesh.deleted names the amended final primary: {}", completion[0]);
    let completed: BTreeSet<String> = completion[0]["output"]["receipts"].as_array().cloned().unwrap_or_default().iter().map(|r| s(&r["node"])).collect();
    assert!(completed.contains(&lost.0) && !completed.contains(next), "mesh.deleted covers the lost primary and every other birth but the alive final primary {next}: {completed:?}");
    assert_eq!(completion[0]["output"]["receipts"].as_array().map(Vec::len), Some(roster - 1), "{}", completion[0]);
    let left = step("MeshLeft");
    assert_eq!(left.len(), 1, "one MeshLeft receipt under {outer}, after {next}'s own node.delete: {build}");
    assert_eq!(left[0]["outcome"], json!("complete"), "{}", left[0]);
    let births: BTreeSet<String> = left[0]["output"]["receipts"].as_array().cloned().unwrap_or_default().iter().map(|r| s(&r["node"])).collect();
    assert!(births.contains(&lost.0) && births.contains(next), "MeshLeft covers both primary births ({} and {next}): {births:?}", lost.0);
    assert_eq!(left[0]["output"]["receipts"].as_array().map(Vec::len), Some(roster), "one exact Exited per birth of the accepted roster plus the amendment: {}", left[0]);

    let one = |name: &str| -> Value {
        let v: Vec<&Value> = named(spans, name).into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str()).collect();
        assert!(!v.is_empty(), "a {name} span for {outer}");
        v[0].clone()
    };
    // Attempt 1 ends on the lost primary's loss in the owner's view, never on the timer.
    let ended = one("rdm.node_admin.mesh.update.via-final-primary-lost");
    assert_eq!((s(&ended["attributes"]["final_primary"]), attr_u(&ended, "attempt")), (format!("{} {}", lost.0, lost.1), 1), "attempt 1 ends on the lost primary's loss: {ended}");
    // The lost primary's roster receipt is its own node.delete's (mesh-delete.md "Resume after the
    // primary's loss" step 1): the fabric-primary deletes the exact unreached birth under the leave's
    // Build, its TerminateRuntime the receipt; no receipt of it is written outside node.delete.
    let lost_terminate: Vec<&Value> = named(spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == lost.0 && sp["attributes"]["step"] == "TerminateRuntime" && s(&sp["attributes"]["build_id"]) == *b && sp["attributes"]["outcome"] == "complete").collect();
    assert_eq!(lost_terminate.len(), 1, "the lost primary {}'s node.delete runs its TerminateRuntime once under {b}: {lost_terminate:?}", lost.0);
    let recorded = lost_terminate[0];
    assert!(start_ns(&ended) <= start_ns(recorded), "the wait ends on the loss before the resume deletes it");
    let applied: Vec<&Value> = named(spans, "rdm.node_admin.mesh.update.via-roster-amended").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str() && sp["attributes"]["outcome"] == "applied").collect();
    assert_eq!(applied.len(), 1, "the amendment was applied once: {applied:?}");
    assert!(start_ns(recorded) <= start_ns(applied[0]), "the lost primary is deleted before the amendment");
    assert!(named(spans, "rdm.node_admin.mesh.update.via-predecessor-exit-recorded").is_empty(), "no exit receipt is recorded outside node.delete");
    // NodeDeleted at most once per roster birth the leave deleted (node-delete.md step 4): by the
    // executor of its node.delete, never again by a later primary that keeps its receipt (a killed
    // executor's publication may never reach the export; a peer that never heard it keeps the birth
    // until mesh-left); the final primary's is its own separate node.delete's.
    for name in births.iter().filter(|n| *n != next) {
        let published: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == *name && s(&sp["attributes"]["build_id"]) == *b).collect();
        assert!(published.len() <= 1, "{name}'s NodeDeleted is published by its node.delete's executor alone, never again by a later primary, under {b}: {published:?}");
    }
    let lost_published: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == lost.0 && s(&sp["attributes"]["build_id"]) == *b).collect();
    assert_eq!(lost_published.len(), 1, "the lost primary's NodeDeleted is published once, by the executor that writes its Deleted row: {lost_published:?}");
    // The lost primary's Deleted row has one writer (node-identity-and-state.md §4.3, R-OW1): the
    // fabric-primary, acting primary of mesh2 while it holds no primary; once mesh2's election names
    // the survivor, the new mesh-primary, which runs on the delete from the fabric-primary's exact Exited
    // receipt (mesh-delete.md "Resume after the primary's loss" steps 1 and 3). It is never refused.
    let lost_rows: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| s(&sp["attributes"]["subject"]) == lost.0).collect();
    assert!(lost_rows.iter().all(|sp| sp["attributes"]["outcome"] == "row-deleted"), "the lost primary's Deleted row is written by its mesh's one writer and never refused: {lost_rows:?}");
    let handed: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-exit-for-writer").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == lost.0).collect();
    let writer = if handed.is_empty() { "mesh1.admin.1" } else { next };
    assert!(lost_rows.len() == 1 && s(&lost_rows[0]["attributes"]["node"]) == writer, "the lost primary's Deleted row is written once, by {writer}: {lost_rows:?}");
    assert!(s(&applied[0]["attributes"]["final_primary"]).starts_with(&format!("{next} ")), "{}", applied[0]);
    assert!(attr_u(applied[0], "attempt") >= 2, "the amendment belongs to a resumed attempt: {}", applied[0]);
    let mesh_left = one("rdm.node_admin.mesh.update.via-mesh-left-receipt");
    assert_eq!((attr_u(&mesh_left, "receipts"), attr_u(&mesh_left, "roster")), (roster as u64, roster as u64), "{mesh_left}");
    // An exit inside the mesh.delete belongs to it (node-recovery.md "Failure and receipts"): the
    // deleted mesh's final primary submits no recovery of a mesh2 birth, the lost primary included.
    let recoveries: Vec<String> = named(spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().filter(|sp| s(&sp["attributes"]["submitter"]).starts_with("mesh2.")).map(|sp| format!("{} by {}: {}", s(&sp["attributes"]["node"]), s(&sp["attributes"]["submitter"]), s(&sp["attributes"]["outcome"]))).collect();
    assert!(recoveries.is_empty(), "no recovery of a mesh2 birth is submitted by mesh2's primary: {} submissions, first {:?}", recoveries.len(), recoveries.first());
    // A desired change, not a recovery: no drift attempt replaces or restarts a mesh2 birth.
    let drift: Vec<&Value> = named(spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| s(&sp["attributes"]["scope"]).contains("mesh2.") && sp["attributes"]["action"] != "none").collect();
    assert!(drift.is_empty(), "no drift attempt acts on a mesh2 birth while the leave resumes: {drift:?}");
    assert!(named(spans, "rdm.node_admin.mesh.reject.via-second-amendment").is_empty());
    eprintln!("{test} trace {} build {b}", s(&applied[0]["trace_id"]), test = "mesh_delete_resume");
    applied[0].clone()
}

/// CONTRACT: mesh2's primary, killed while it holds rpc.2's StopNode receipt, is replaced as the
/// leave's final primary by the surviving admin the election seats: the fabric-primary records
/// the lost primary's exact Exited, then one RosterAmended naming the predecessor and the
/// survivor, and the resumed attempt finishes; MeshLeft covers the four births of the accepted
/// roster, which already holds the survivor, both primaries included.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_whose_primary_is_lost_mid_tier_finishes_under_the_surviving_admin() {
    let run = primary_lost_mid_leave("a_leave_whose_primary_is_lost_mid_tier_finishes_under_the_surviving_admin", 2, Rpc1::Free).await;
    let survivor = if run.lost.0 == "mesh2.admin.1" { "mesh2.admin.2" } else { "mesh2.admin.1" };
    assert_resumed(&run, survivor, 4);
    // The survivor admits the leave from the accepted roster plus the amendment, from its own
    // facts or the owner's, and runs only what has no terminal receipt.
    let outer = format!("mesh.delete:{}", run.mesh2_id);
    let resumed: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-retained-receipts").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str() && attr_u(sp, "attempt") >= 2).collect();
    assert!(!resumed.is_empty(), "the survivor resumed the leave's tiers on a later attempt");
    assert!(named(&run.spans, "rdm.node_admin.node.create.via-leaving-mesh").is_empty(), "a surviving admin needs no created one");
}

/// CONTRACT: a member's delete is done only when every step of it has its receipt, and the resumed
/// leave runs the steps that have none, in order (mesh-delete.md, "Resume after the primary's loss"
/// step 3: the new mesh-primary "runs only the tiers without a terminal receipt"; node-delete.md
/// step 4: the executor records the complete delete receipt and emits node.deleted; the Build
/// carrier: "Completed steps are not rerun"). mesh2's primary is killed holding rpc.1 at its
/// NodeDeleted receipt (rpc.1 exited and its exact Exited receipted, no NodeDeleted) and rpc.2 at
/// its StopNode receipt. The surviving admin the election seats finishes rpc.1's delete: its
/// receipted TerminateRuntime is reused (the runtime is not terminated again), NodeDeleted is
/// executed and published, and the delete ends; the leave then finishes as in form (a): mesh2 holds
/// no ordinary node in any view once mesh.deleted ends the mesh.delete, and MeshLeft covers the four
/// births of the accepted roster. A peer that never hears NodeDeleted keeps the stopped birth until
/// mesh-left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_whose_primary_is_lost_after_a_members_terminate_runtime_receipt_finishes_that_members_delete_under_the_surviving_admin() {
    let run = primary_lost_mid_leave("a_leave_whose_primary_is_lost_after_a_members_terminate_runtime_receipt_finishes_that_members_delete_under_the_surviving_admin", 2, Rpc1::ExitProven).await;
    let survivor = if run.lost.0 == "mesh2.admin.1" { "mesh2.admin.2" } else { "mesh2.admin.1" };
    let eprint_trace = assert_resumed(&run, survivor, 4);
    let outer = format!("mesh.delete:{}", run.mesh2_id);
    let rpc1 = "mesh2.rpc.1";
    // The survivor's resumed attempt runs the two ordinary members: rpc.1 holds an exited verdict and is
    // still owed its delete. The lost primary's delete is finished when the fabric-primary wrote its row
    // (mesh2 held no primary), else owed from its exact Exited to the survivor, mesh2's one writer.
    let resumed: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-retained-receipts").into_iter().filter(|sp| sp["attributes"]["operation"] == outer.as_str() && attr_u(sp, "attempt") >= 2).collect();
    let handed = !named(&run.spans, "rdm.node_admin.node.delete.via-exit-for-writer").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == run.lost.0).collect::<Vec<_>>().is_empty();
    let (retained, owed, remaining) = if handed { (String::new(), format!("{},{rpc1}", run.lost.0), 3) } else { (run.lost.0.clone(), rpc1.to_string(), 2) };
    assert!(
        resumed.iter().any(|sp| attr_u(sp, "roster") == 4 && s(&sp["attributes"]["retained_births"]) == retained && attr_u(sp, "remaining") == remaining && s(&sp["attributes"]["delete_owed"]).split(',').collect::<BTreeSet<_>>() == owed.split(',').collect::<BTreeSet<_>>()),
        "the resumed attempt names rpc.1 as holding an exited verdict and owed the rest of its delete (the lost primary's delete {}): {resumed:?}",
        if handed { "owed to the survivor" } else { "finished" }
    );
    // rpc.1's delete in the resumed attempt: its receipted TerminateRuntime is reused, never run
    // again; its NodeDeleted is executed once.
    let steps = |step: &str| -> Vec<&Value> {
        named(&run.spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == rpc1 && s(&sp["attributes"]["step"]) == step && s(&sp["attributes"]["build_id"]) == run.b && attr_u(sp, "attempt") >= 2).collect()
    };
    let terminate = steps("TerminateRuntime");
    assert!(!terminate.is_empty() && terminate.iter().all(|sp| sp["attributes"]["outcome"] == "reused"), "the resumed attempt reuses rpc.1's receipted TerminateRuntime and terminates nothing: {terminate:?}");
    let deleted = steps("NodeDeleted");
    assert_eq!(deleted.len(), 1, "the resumed attempt executes rpc.1's NodeDeleted once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["outcome"], json!("complete"), "{}", deleted[0]);
    // NodeDeleted is published, from the same pipeline run, and heard by a peer.
    let published: Vec<&Value> = named(&run.spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == rpc1 && s(&sp["attributes"]["build_id"]) == run.b).collect();
    assert_eq!(published.len(), 1, "rpc.1's NodeDeleted is published once, by the executor that finished its delete: {published:?}");
    assert_eq!(s(&published[0]["trace_id"]), s(&deleted[0]["trace_id"]), "the publication belongs to the resumed delete's own trace");
    let heard: Vec<&Value> = named(&run.spans, "rdm.mesh.topology.remove.via-node-deleted").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == rpc1 || s(&sp["attributes"]["name"]) == rpc1).collect();
    assert!(!heard.is_empty(), "a peer applies rpc.1's NodeDeleted");
    eprintln!("trace {} rpc.1 TerminateRuntime reused, NodeDeleted complete + published; amendment trace {}", s(&deleted[0]["trace_id"]), s(&eprint_trace["trace_id"]));
}
