//! i143.e12.s39 (#3066) process E2E: a mesh delete resumed after the deleting mesh's primary is lost
//! mid-delete (mesh-delete.md, "Resume after the primary's loss").
//!
//! desired {mesh1, mesh2} -> {mesh1}: the fabric-primary in mesh1 runs `mesh.delete:<mesh2 id>`.
//! mesh2's primary is held by its fault service in tier 1 (the `StopNode` step event of rpc.2's
//! node.delete) and SIGKILLed there by exact pid; the fault service dies with it. The fabric-primary's wait
//! ends the moment its view holds the lost primary unheard (`via-final-primary-lost`), never on the
//! handoff bound: the section ends short and the Build ends `incomplete`, what ran standing (Q346).
//! The caller resubmits the Build by its build_id (build.md §4). The fabric-primary deletes the lost
//! primary by its node.delete and hands the mesh's section to the new mesh-primary, which runs only
//! the deletes without a terminal event over the mesh's records (D67: the roster is the mesh's node
//! records at each step, and a birth this Build's node.delete deleted keeps its `Deleted` record); the
//! Build ends `complete` at mesh.deleted, the new mesh-primary alive. Its end is its own node.delete
//! (mesh-delete.md step 7), after which mesh-left covers every birth of the mesh's records, both
//! primary births included.
//!
//! This stem is form (a): a surviving admin of mesh2 the election names. Form (b), no admin of mesh2
//! survives and the fabric-primary creates the one node-admin the deleting mesh may gain, is
//! `mesh_lifecycle__mesh_delete_no_survivor`, over the shared run and assertions here. Form (c) is
//! form (a) with a member's delete cut right after its exited verdict: the primary is killed holding
//! rpc.1 at its `NodeDeleted` step event, so the Build holds rpc.1's `TerminateRuntime` event complete
//! and no `NodeDeleted`.

use rdm_test::estate::location;
use crate::build_log;
use rdm_client::{FabricDesired, Frame, MeshDesired, PathName};
use rdm_test::typed::BuildSpec;
use rdm_test::estate::{at_location, names_location, named, Estate, Owner};
use std::collections::BTreeMap;
use rdm_test::faults::{binding_set, candidate_sha, FaultClient};
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

fn end_ns(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// Where mesh2's primary holds rpc.1 when it is killed.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Rpc1 {
    /// Not held: rpc.1's delete runs on whatever the kill finds it at.
    Free,
    /// Stopped (its stop-node answered), its process alive: held at its `StopNode` step event, with
    /// no `TerminateRuntime` event and no NodeStopped published.
    Stopped,
    /// Exited, its `TerminateRuntime` step event complete, the delete cut after NodeDeleted's work and
    /// before NodeDeleted's step event is durable: the Build holds the exited verdict and no
    /// NodeDeleted, and none is published. The fault service's `step-event` cut holds BEFORE the named
    /// step's event is durable (`FaultedBuilds::write_step`), so the cut that leaves TerminateRuntime
    /// durable is the next step's: NodeDeleted.
    ExitProven,
}

/// Build `id` as the fabric's Build records hold it now (read from its store root, Q292), as
/// the JSON its view serializes to; a read that ends, or a log that holds no acceptance of it, is recorded
/// as such: nothing here panics before the estate is stopped.
async fn build_now(estate: &Estate, id: &str) -> Value {
    match build_log::log_read(&estate, id).await {
        Ok(facts) => build_log::projection(estate, &facts, id).map(|p| serde_json::to_value(p).expect("a Build view serializes")).unwrap_or_else(|| json!({"unaccepted": facts.len()})),
        Err(end) => json!({"read_ended": end.to_string()}),
    }
}

/// What one form's run leaves to assert on.
pub(crate) struct Run {
    /// The Build as the first run left it, ended short by the loss.
    pub(crate) first: Value,
    /// The Build after the resubmit.
    pub(crate) build: Value,
    /// The resubmit's stream's last frame.
    pub(crate) resumed: String,
    /// How many runs the Build started (`rdm.build.started`).
    pub(crate) starts: usize,
    /// When the cell resubmitted the Build.
    pub(crate) resubmitted_ns: u64,
    pub(crate) spans: Vec<Value>,
    pub(crate) mesh2_id: String,
    /// The lost primary: its location, node id and incarnation.
    pub(crate) lost: (PathName, String, String),
    pub(crate) b: String,
    /// mesh2's mesh-primary after the resubmit, alive after mesh.deleted until its node.delete, and
    /// its node id.
    pub(crate) last: PathName,
    pub(crate) last_id: String,
    /// The node id of every birth settled before the delete, by location.
    pub(crate) ids: BTreeMap<PathName, String>,
}

impl Run {
    /// The node id settled at `at` before the delete.
    pub(crate) fn id(&self, at: &PathName) -> String {
        self.ids.get(at).cloned().unwrap_or_else(|| panic!("no birth settled at {at:?}"))
    }
}

/// Build {mesh1, mesh2 with `mesh2_admins` admins}, arm mesh2's primary's `NodeCommandService` at the `StopNode` step
/// event of rpc.2's node.delete (and, by `rpc1`, at rpc.1's `StopNode` event: rpc.1 stopped, its
/// process alive, no `TerminateRuntime` event, and its NodeStopped not yet published; or at rpc.1's
/// `NodeDeleted` event: rpc.1's exited verdict durable and no NodeDeleted), submit {mesh1}, kill the
/// held primary by exact pid once every cut holds, wait for the Build to end `incomplete`, and, once
/// a surviving admin is mesh2's primary (`mesh2_admins` > 1), resubmit it and follow it to its
/// terminal.
pub(crate) async fn primary_lost_mid_leave(test: &str, mesh2_admins: u32, rpc1: Rpc1) -> Run {
    let sha = candidate_sha();
    // Every path is named its executable; the one node-admin the fabric-primary creates for a deleting
    // mesh no admin of which survives (mesh-delete.md resume step 2) runs the executable its lost
    // predecessor's row records (node-recovery.md "Retained"; `admin/mesh_delete.rs`
    // `create_deleting_mesh_admin`).
    let mut estate = Estate::bootstrap_on(owner(test), "fabric1", "mesh1", &binding_set(&sha), &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    let mesh1 = MeshDesired::of("mesh1", [("node-admin", 2)]);
    let mesh2 = MeshDesired::of("mesh2", [("node-admin", mesh2_admins), ("node", 2)]);
    estate.resize(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1.clone(), mesh2] }).await;
    let mut want: BTreeSet<PathName> = [("mesh1.node-admin", "1"), ("mesh1.node-admin", "2"), ("mesh2.node", "1"), ("mesh2.node", "2")].iter().map(|(path, name)| Estate::path(path, name)).collect();
    want.extend((1..=mesh2_admins).map(|i| Estate::path("mesh2.node-admin", &i.to_string())));
    let before = Estate::values(&estate.settles_to_named(&want, Duration::from_secs(60)).await);
    // A mesh delete waits for the Build in progress (Q170): the fabric is ready before mesh2
    // goes, so the admin in its role after the primary's loss reads mesh2 deleting from its mesh's own rows.
    estate.until_fabric_ready(Duration::from_secs(90)).await;
    let mesh2_id = estate.mesh_view("mesh2").await.id.map(|i| i.to_string()).unwrap_or_else(|| panic!("mesh2 has a minted id"));
    let primary = before.iter().find(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true).cloned().unwrap_or_else(|| panic!("mesh2 has an admin primary: {before:#?}"));
    let lost = (location(&primary), s(&primary["node_id"]), s(&primary["incarnation"]));
    let ids: BTreeMap<PathName, String> = before.iter().map(|n| (location(n), s(&n["node_id"]))).collect();
    // A member's delete is keyed by its node id (mesh-delete.md step 3).
    let member = |name: &str| json!({"Node": ids[&Estate::path("mesh2.node", name)]});
    let faults = FaultClient::at(&estate, &lost.0).await;

    // Held in tier 1: rpc.2's stop answered `stopped` (its node.connection.delete { All } and its
    // transport cut done, its process alive) and its StopNode step event is not yet durable, so the
    // resubmit's delete sends its stop-node again and the stopped birth answers the same.
    let cut = "leave:rpc2-StopNode";
    // A member's end is its node.delete, under node.delete's own operation (mesh-delete.md step 3).
    faults.arm(cut, json!({"kind": "step-event", "step": "StopNode", "operation": "node.delete", "node": member("2")})).await;
    // rpc.1's stop completed (its stop-node answered stopped: its connections deleted, its transport
    // cut, its process alive) and its delete held at that step's event: its NodeStopped, published at
    // the next step, is never published, and it has no TerminateRuntime event. Nothing any admin holds
    // names it.
    let stopped = "leave:rpc1-StopNode";
    if rpc1 == Rpc1::Stopped {
        faults.arm(stopped, json!({"kind": "step-event", "step": "StopNode", "operation": "node.delete", "node": member("1")})).await;
    }
    // rpc.1's whole delete ran to its exited verdict: the runtime is terminated and its TerminateRuntime
    // event is complete, and the executor is held in NodeDeleted before its event: the Build holds the
    // exited verdict and no NodeDeleted, and `NodeDeleted` is never published.
    let exit_proven = "leave:rpc1-NodeDeleted";
    if rpc1 == Rpc1::ExitProven {
        faults.arm(exit_proven, json!({"kind": "step-event", "step": "NodeDeleted", "operation": "node.delete", "node": member("1")})).await;
    }
    let leave = estate.submit(&BuildSpec::Fabric(FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1] })).await;
    let build_id = leave.build_id().clone();
    let b = build_id.to_string();
    let held = faults.wait_held(cut).await;
    assert_eq!(held["hit"]["build_id"], b.as_str(), "the held step event is the delete's: {held}");
    if rpc1 == Rpc1::Stopped {
        let held = faults.wait_held(stopped).await;
        assert_eq!(held["hit"]["build_id"], b.as_str(), "the held step event is the delete's: {held}");
    }
    if rpc1 == Rpc1::ExitProven {
        let held = faults.wait_held(exit_proven).await;
        assert_eq!((&held["hit"]["build_id"], &held["hit"]["step"], &held["hit"]["operation"]["action"], &held["hit"]["operation"]["subject"]), (&json!(b), &json!("NodeDeleted"), &json!("node.delete"), &member("1")), "the held step event is rpc.1's NodeDeleted of the delete: {held}");
    }
    // The fault service dies with the process: it is never released.
    let pid = estate.pid_of(&lost.0).await;
    estate.kill_pid(u32::try_from(pid).unwrap());
    drop(leave);

    // The section ends short once the fabric-primary's view holds the primary unheard, and the Build
    // ends `incomplete`. The bound is below the fabric-primary's 120 s handoff bound: a section that
    // waited on it is red here. Nothing here panics before the estate is stopped: every process
    // flushes its spans on exit, and a red run is read from them.
    let first = match build_log::build_fact(&estate, &b, "the Build ended short of complete", Duration::from_secs(90), |p| p.state.ended_short()).await {
        Ok(p) => serde_json::to_value(p).expect("a Build view serializes"),
        Err(end) => json!({"read_ended": end.to_string()}),
    };
    // A surviving admin takes mesh2's primary role by its election before the resubmit hands it the
    // mesh's section (mesh-delete.md resume step 2: "the surviving admin the election named").
    let mut survivor_primary = mesh2_admins == 1;
    let elect_by = std::time::Instant::now() + Duration::from_secs(60);
    while !survivor_primary && std::time::Instant::now() < elect_by {
        let nodes = estate.try_node_views().await.map(|v| Estate::values(&v)).unwrap_or_default();
        survivor_primary = nodes.iter().any(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node-admin" && n["is_primary"] == true && n["state"] == "ready" && s(&n["node_id"]) != lost.1);
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    let resubmitted_ns = now_ns();
    let resumed = if first["state"] == "incomplete" && survivor_primary {
        match tokio::time::timeout(Duration::from_secs(150), async { estate.follow_to_terminal(estate.resubmit(&build_id).await).await }).await {
            Ok(done) => format!("{:?}", done.frames.last()),
            Err(_) => "no terminal within 150 s".to_string(),
        }
    } else {
        format!("not resubmitted: the first run ended {}, a surviving admin primary: {survivor_primary}", s(&first["state"]))
    };
    let mut build = build_now(&estate, &b).await;
    // mesh.delete ends at mesh.deleted: the view settles on mesh1 and mesh2's new mesh-primary,
    // alive; its end is its own node.delete (mesh-delete.md step 7), then the view converges on
    // mesh1 alone within gossip delay of mesh-left.
    // Each view row as (mesh, the row named), read until mesh2 is gone.
    let mut view: Vec<(String, String)> = Vec::new();
    let mut mesh2_gone = false;
    let mut last: Option<(PathName, String)> = None;
    let mut deleted = Value::Null;
    let mut starts = 0;
    if build["state"] == "complete" {
        starts = estate.starts_of_build(&b).await;
        let settle = std::time::Instant::now() + Duration::from_secs(60);
        while std::time::Instant::now() < settle && last.is_none() {
            // The lost primary's Deleted record may still be held unheard until mesh-left. The new
            // mesh-primary is the one mesh2 birth that is alive.
            let nodes = estate.try_node_views().await.map(|v| Estate::values(&v)).unwrap_or_default();
            let mesh2: Vec<(PathName, String)> = nodes.iter().filter(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["state"] == "ready").map(|n| (location(n), s(&n["node_id"]))).collect();
            let deleting: Vec<&Value> = nodes.iter().filter(|n| rdm_test::estate::mesh_of(&n) == "mesh2" && n["type"] == "node").collect();
            if mesh2.len() == 1 && deleting.is_empty() {
                last = mesh2.into_iter().next();
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    if let Some((path, _)) = &last {
        deleted = match estate.delete_node(&path).await {
            Ok(mut stream) => {
                let accepted = stream.accepted().cloned().expect("the call opened a Build");
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
                json!({"build_id": accepted.build_id.to_string(), "state": state, "frames": format!("{frames:?}"), "end": end})
            }
            Err(e) => json!({"refused": e.to_string()}),
        };
        let settle = std::time::Instant::now() + Duration::from_secs(60);
        while std::time::Instant::now() < settle {
            view = match estate.try_node_views().await {
                Ok(views) => views.iter().map(|n| (n.mesh().clone(), format!("{:?}={:?}", n.name, n.state))).collect(),
                Err(e) => vec![(String::new(), format!("the read ended: {e}"))],
            };
            mesh2_gone = matches!(rdm_test::typed::one_mesh(&estate.meshes_obj().await, "mesh2").await, Ok(None));
            if mesh2_gone && view.iter().all(|(mesh, _)| mesh == "mesh1") {
                break;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        build = build_now(&estate, &b).await;
    }
    estate.stop().await;
    let spans = estate.spans();
    assert_eq!(first["state"], "incomplete", "build {b}'s first run ends incomplete on the primary's loss within 90 s, never failed: {first}");
    assert!(survivor_primary, "a surviving admin of mesh2 is its primary within 60 s of the loss");
    assert_eq!(build["state"], "complete", "the resubmit of build {b} completes it ({resumed}): {build}");
    assert_eq!(starts, 2, "the submission and the one resubmit each started a run of build {b}");
    let (last, last_id) = last.unwrap_or_else(|| panic!("mesh.deleted leaves mesh2's new mesh-primary alive, the one mesh2 node the view holds after build {b}"));
    assert_eq!(deleted["state"], "complete", "the node.delete of mesh2's new mesh-primary {last:?} completes: {deleted}");
    assert!(view.iter().all(|(mesh, _)| mesh == "mesh1"), "the view holds no mesh2 birth after mesh-left: {view:?}");
    assert!(mesh2_gone, "mesh2 is no longer a mesh of the fabric: mesh.get [name: mesh2] answers the empty list");
    Run { first, build, resumed, starts, resubmitted_ns, spans, mesh2_id, lost, b, last, last_id, ids }
}

/// The events and spans every form shares (mesh-delete.md, "Resume after the primary's loss"): the
/// first run ends on the loss; in the resubmit the lost primary is deleted by its node.delete before
/// the mesh's section goes to `next`, the new mesh-primary, which runs the deletes without a terminal
/// event over the mesh's records (`roster` births, both primaries included); mesh.deleted's
/// manifest covers every birth but `next`, the lost primary's exit included; and, after `next`'s own
/// node.delete, mesh-left covers all `roster`. Answers the mesh.deleted span.
pub(crate) fn assert_resumed(run: &Run, next: &PathName, roster: usize) -> Value {
    let Run { first, build, spans, lost, b, last, last_id, ids, mesh2_id, resubmitted_ns, .. } = run;
    let resubmitted_ns = *resubmitted_ns;
    assert_eq!(last, next, "mesh2's mesh-primary alive after mesh.deleted is {next:?}");
    let outer = format!("mesh.delete:{mesh2_id}");
    // The first run's step events stand, and the resubmit writes none of them again (build.md §1).
    let events = |build: &Value, name: &str| -> Vec<Value> { build["steps"].as_array().cloned().unwrap_or_default().into_iter().filter(|r| r["operation"] == outer.as_str() && r["step"] == name).collect() };
    assert_eq!(events(first, "AcceptedRoster").len(), 1, "the first run wrote the AcceptedRoster event of {outer}: {first}");
    assert!(events(first, "MeshDeleted").is_empty(), "the first run wrote no MeshDeleted: {first}");
    assert_eq!(events(build, "AcceptedRoster").len(), 1, "the resubmit writes no second AcceptedRoster event: {build}");
    for name in ["MeshDeleted", "MeshLeft"] {
        let e = events(build, name);
        assert!(e.len() == 1 && e[0]["outcome"] == "complete", "one complete {name} event of {outer}: {build}");
    }
    let of_mesh2 = |name: &str| -> Vec<&Value> { named(spans, name).into_iter().filter(|sp| s(&sp["attributes"]["mesh_id"]) == *mesh2_id).collect() };

    // The first run ends on the lost primary's loss in the fabric-primary's view, never on the timer.
    let ended = of_mesh2("rdm.node_admin.mesh.update.via-final-primary-lost");
    assert!(ended.len() == 1 && s(&ended[0]["attributes"]["final_primary"]) == format!("{} {}", lost.1, lost.2) && start_ns(ended[0]) < resubmitted_ns, "the first run ends once, on the lost primary's loss, before the resubmit: {ended:?}");
    // Resume step 2: the lost primary is deleted by its own node.delete under the Build in the
    // resubmit, its TerminateRuntime once.
    let lost_terminate: Vec<&Value> = named(spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| at_location(sp, &lost.0) && sp["attributes"]["step"] == "TerminateRuntime" && s(&sp["attributes"]["build_id"]) == *b && sp["attributes"]["outcome"] == "complete").collect();
    assert_eq!(lost_terminate.len(), 1, "the lost primary {:?}'s node.delete runs its TerminateRuntime once under {b}: {lost_terminate:?}", lost.0);
    let terminated = lost_terminate[0];
    assert!(start_ns(terminated) >= resubmitted_ns, "the lost primary is deleted in the resubmit: {terminated}");
    // Resume step 3: the mesh's section then goes to the new mesh-primary, which runs only the deletes
    // without a terminal event, over the mesh's records.
    let resumed: Vec<&Value> = of_mesh2("rdm.node_admin.mesh.resolve.via-retained-receipts").into_iter().filter(|sp| start_ns(sp) >= resubmitted_ns).collect();
    assert!(!resumed.is_empty() && resumed.iter().all(|sp| attr_u(sp, "roster") == roster as u64 && start_ns(sp) >= end_ns(terminated)), "the new mesh-primary runs the deletes over the mesh's {roster} records only after the lost primary's TerminateRuntime: {resumed:?}");
    let recaptured: Vec<&Value> = of_mesh2("rdm.node_admin.mesh.resolve.via-accepted-roster").into_iter().filter(|sp| start_ns(sp) >= resubmitted_ns && sp["attributes"]["step"] == "AcceptedRoster").collect();
    assert!(recaptured.iter().all(|sp| sp["attributes"]["source"] == "records-resumed" && attr_u(sp, "roster") == roster as u64), "the resubmit reads the roster from the mesh's records, its event already written: {recaptured:?}");
    // Resume step 4: mesh.deleted, validated at the fabric-primary: its manifest covers the mesh's
    // records, every birth but the alive new mesh-primary, the lost primary's exit included.
    let completion = of_mesh2("rdm.node_admin.mesh.update.via-mesh-deleted-receipt");
    assert_eq!(completion.len(), 1, "mesh.deleted is recorded once: {completion:?}");
    let completion = completion[0];
    assert_eq!((attr_u(completion, "receipts"), attr_u(completion, "roster"), s(&completion["attributes"]["final_primary_node_id"])), ((roster - 1) as u64, roster as u64, last_id.clone()), "mesh.deleted covers every birth of the records but the new mesh-primary {next:?}: {completion}");
    // mesh-left, after the new mesh-primary's own node.delete: every birth, both primaries included.
    let mesh_left = of_mesh2("rdm.node_admin.mesh.update.via-mesh-left-receipt");
    assert!(mesh_left.len() == 1 && (attr_u(mesh_left[0], "receipts"), attr_u(mesh_left[0], "roster")) == (roster as u64, roster as u64), "mesh-left covers all {roster} births once: {mesh_left:?}");
    // NodeDeleted at most once per birth the delete deleted (node-delete.md step 4): by the executor of
    // its node.delete, never again by a later primary (a killed executor's publication may never reach
    // the export; a peer that never heard it keeps the birth until mesh-left); the new mesh-primary's is
    // its own separate node.delete's.
    for name in ids.keys().filter(|n| n.path.starts_with("mesh2.") && *n != next) {
        let published: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| at_location(sp, name) && s(&sp["attributes"]["build_id"]) == *b).collect();
        assert!(published.len() <= 1, "{name:?}'s NodeDeleted is published by its node.delete's executor alone, never again by a later primary, under {b}: {published:?}");
    }
    let lost_published: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| at_location(sp, &lost.0) && s(&sp["attributes"]["build_id"]) == *b).collect();
    assert_eq!(lost_published.len(), 1, "the lost primary's NodeDeleted is published once, by the executor that writes its Deleted row: {lost_published:?}");
    // The lost primary's Deleted row has one writer (node-identity-and-state.md §4.3, R-OW1): the
    // fabric-primary, acting primary of mesh2 while it holds no primary; once mesh2's election names
    // the survivor, the new mesh-primary, which runs the delete on from its TerminateRuntime event
    // (mesh-delete.md "Resume after the primary's loss" steps 2 and 3). It is never refused.
    let lost_rows: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-node-row").into_iter().filter(|sp| names_location(sp, "subject", &lost.0)).collect();
    assert!(lost_rows.iter().all(|sp| sp["attributes"]["outcome"] == "row-deleted"), "the lost primary's Deleted row is written by its mesh's one writer and never refused: {lost_rows:?}");
    let handed: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-exit-for-writer").into_iter().filter(|sp| at_location(sp, &lost.0)).collect();
    let writer = if handed.is_empty() { Estate::path("mesh1.node-admin", "1") } else { next.clone() };
    assert!(lost_rows.len() == 1 && at_location(lost_rows[0], &writer), "the lost primary's Deleted row is written once, by {writer:?}: {lost_rows:?}");
    // An exit inside the mesh.delete belongs to it (node-recovery.md "Failure"): the
    // deleted mesh's mesh-primary submits no recovery of a mesh2 birth, the lost primary included.
    let mesh2_ids: BTreeSet<String> = ids.iter().filter(|(at, _)| at.path.starts_with("mesh2.")).map(|(_, id)| id.clone()).chain([last_id.clone()]).collect();
    let recoveries: Vec<String> = named(spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().filter(|sp| mesh2_ids.contains(&s(&sp["attributes"]["submitter"])) || mesh2_ids.contains(&s(&sp["attributes"]["node_id"]))).map(|sp| format!("{} by {}: {}", s(&sp["attributes"]["node_id"]), s(&sp["attributes"]["submitter"]), s(&sp["attributes"]["outcome"]))).collect();
    assert!(recoveries.is_empty(), "no recovery of a mesh2 birth is submitted: {} submissions, first {:?}", recoveries.len(), recoveries.first());
    eprintln!("{test} build {b}: mesh.deleted trace {}, the new mesh-primary's run trace {}", s(&completion["trace_id"]), s(&resumed[0]["trace_id"]), test = "mesh_delete_resume");
    completion.clone()
}

/// CONTRACT: mesh2's primary, killed while it holds rpc.2's StopNode step event, ends the first run
/// incomplete. On the resubmit the fabric-primary deletes the lost primary by its node.delete and hands
/// mesh2's section to the surviving admin the election named, which runs the deletes without a
/// terminal event; the Build completes, and mesh-left covers the four births of mesh2's records, both
/// primaries included. Must NOT happen: an admin created for mesh2, or the Build failed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_whose_primary_is_lost_mid_tier_finishes_under_the_surviving_admin() {
    let run = primary_lost_mid_leave("a_leave_whose_primary_is_lost_mid_tier_finishes_under_the_surviving_admin", 2, Rpc1::Free).await;
    let survivor = Estate::path("mesh2.node-admin", if run.lost.0.name == "1" { "2" } else { "1" });
    assert_resumed(&run, &survivor, 4);
    assert!(named(&run.spans, "rdm.node_admin.node.create.via-leaving-mesh").is_empty(), "a surviving admin needs no created one");
}

/// CONTRACT: a member's delete is done only when every step of it has its event, and the resubmitted
/// delete runs the steps that have none, in order (mesh-delete.md, "Resume after the primary's loss"
/// step 3: the new mesh-primary "runs only the tiers without a terminal event"; node-delete.md step 4;
/// build.md §1: a step whose event ended `complete` is never run again). mesh2's primary is killed
/// holding rpc.1 at its NodeDeleted step event (rpc.1 exited, its TerminateRuntime event complete, no
/// NodeDeleted) and rpc.2 at its StopNode event. On the resubmit the surviving admin the election names
/// finishes rpc.1's delete: its TerminateRuntime is reused (the runtime is not terminated again),
/// NodeDeleted is executed and published, and the delete ends; the delete then finishes as in form (a):
/// mesh2 holds no ordinary node in any view once mesh.deleted ends the mesh.delete, and mesh-left covers
/// the four births of mesh2's records. A peer that never hears NodeDeleted keeps the stopped birth until
/// mesh-left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leave_whose_primary_is_lost_after_a_members_terminate_runtime_receipt_finishes_that_members_delete_under_the_surviving_admin() {
    let run = primary_lost_mid_leave("a_leave_whose_primary_is_lost_after_a_members_terminate_runtime_receipt_finishes_that_members_delete_under_the_surviving_admin", 2, Rpc1::ExitProven).await;
    let survivor = Estate::path("mesh2.node-admin", if run.lost.0.name == "1" { "2" } else { "1" });
    let completion = assert_resumed(&run, &survivor, 4);
    let rpc1 = Estate::path("mesh2.node", "1");
    let rpc1_id = run.id(&rpc1);
    // The survivor's run in the resubmit runs the two ordinary members: rpc.1 holds an exited verdict
    // and is still owed its delete. The lost primary's delete is finished when the fabric-primary wrote
    // its row (mesh2 held no primary), else owed from its TerminateRuntime event to the survivor, mesh2's
    // one writer.
    let resumed: Vec<&Value> = named(&run.spans, "rdm.node_admin.mesh.resolve.via-retained-receipts").into_iter().filter(|sp| s(&sp["attributes"]["mesh_id"]) == run.mesh2_id && start_ns(sp) >= run.resubmitted_ns).collect();
    let handed = !named(&run.spans, "rdm.node_admin.node.delete.via-exit-for-writer").into_iter().filter(|sp| at_location(sp, &run.lost.0)).collect::<Vec<_>>().is_empty();
    // The spans name births by node id.
    let (retained, owed, remaining) = if handed { (String::new(), format!("{},{rpc1_id}", run.lost.1), 3) } else { (run.lost.1.clone(), rpc1_id.clone(), 2) };
    assert!(
        resumed.iter().any(|sp| attr_u(sp, "roster") == 4 && s(&sp["attributes"]["retained_births"]) == retained && attr_u(sp, "remaining") == remaining && s(&sp["attributes"]["delete_owed"]).split(',').collect::<BTreeSet<_>>() == owed.split(',').collect::<BTreeSet<_>>()),
        "the resubmit's run names rpc.1 as holding an exited verdict and owed the rest of its delete (the lost primary's delete {}): {resumed:?}",
        if handed { "owed to the survivor" } else { "finished" }
    );
    // rpc.1's delete in the resubmit: its TerminateRuntime, whose event ended complete, is reused, never
    // run again; its NodeDeleted is executed once.
    let steps = |step: &str| -> Vec<&Value> {
        named(&run.spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| at_location(sp, &rpc1) && s(&sp["attributes"]["step"]) == step && s(&sp["attributes"]["build_id"]) == run.b && start_ns(sp) >= run.resubmitted_ns).collect()
    };
    let terminate = steps("TerminateRuntime");
    assert!(!terminate.is_empty() && terminate.iter().all(|sp| sp["attributes"]["outcome"] == "reused"), "the resubmit reuses rpc.1's TerminateRuntime and terminates nothing: {terminate:?}");
    let deleted = steps("NodeDeleted");
    assert_eq!(deleted.len(), 1, "the resubmit executes rpc.1's NodeDeleted once: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["outcome"], json!("complete"), "{}", deleted[0]);
    // NodeDeleted is published, from the same delete, and heard by a peer.
    let published: Vec<&Value> = named(&run.spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| at_location(sp, &rpc1) && s(&sp["attributes"]["build_id"]) == run.b).collect();
    assert_eq!(published.len(), 1, "rpc.1's NodeDeleted is published once, by the executor that finished its delete: {published:?}");
    assert_eq!(s(&published[0]["trace_id"]), s(&deleted[0]["trace_id"]), "the publication belongs to the resubmitted delete's own trace");
    let heard: Vec<&Value> = named(&run.spans, "rdm.mesh.topology.remove.via-node-deleted").into_iter().filter(|sp| at_location(sp, &rpc1) || s(&sp["attributes"]["node_id"]) == rpc1_id).collect();
    assert!(!heard.is_empty(), "a peer applies rpc.1's NodeDeleted");
    eprintln!("trace {} rpc.1 TerminateRuntime reused, NodeDeleted complete + published; mesh.deleted trace {}", s(&deleted[0]["trace_id"]), s(&completion["trace_id"]));
}
