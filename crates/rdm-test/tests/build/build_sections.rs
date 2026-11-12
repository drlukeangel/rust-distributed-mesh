//! product=mesh, feature=fabric-build, subfeature=build-sections, rung=MM, provider=process.
//!
//! A Build's progress is its `rdm.build.step` events (build.md §5); a section is one call to a
//! mesh's primary, `build.section.run`; a failed step fails its mesh's section, never the Build; and a
//! Build ends `complete` when every section finished, `incomplete` when it ran and not every section
//! finished (what ran stands), `failed` only when it could not start (Q346). An incomplete Build is
//! resubmitted by its build_id, never a second Build (build.md §4).
//!
//! The estate: `mesh1` (two node-admins, one node) holds the fabric-primary role and `mesh2` (two
//! node-admins, two nodes) is run by its own primary, so every section of `mesh2` crosses a real
//! `build.section.run`. The spans an estate exports are each cell's flame.

use rdm_client::{BuildFrame, BuildId, BuildStream, Change, NodeStatus, NodeView};
use rdm_admin::build_state::{BuildState, StepOutcome};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::process_faults::{ExactRuntime, Fault};
use serde_json::Value;
use std::path::Path;
use std::time::Duration;

const FABRIC: &str = "fabric1";
const SHAPE: [(&str, u32, u32); 2] = [("mesh1", 2, 1), ("mesh2", 2, 2)];
const SETTLE: Duration = Duration::from_secs(120);

fn owner(test: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "fabric-build".into(), subfeature: "build-sections".into(), rung: "MM".into(), provider: "process".into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn attr<'a>(sp: &'a Value, k: &str) -> &'a str {
    sp["attributes"][k].as_str().unwrap_or_default()
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64
}

/// The estate born to [`SHAPE`], its fabric ready.
async fn born(test: &str) -> Estate {
    let estate = Estate::bootstrap(owner(test), FABRIC, "mesh1").await;
    estate.resize_shape(FABRIC, &SHAPE).await;
    estate.settled_views(&SHAPE).await;
    estate.until_fabric_ready(SETTLE).await;
    estate
}

/// `mesh2`'s primary node-admin in the view now.
async fn mesh2_primary(estate: &Estate) -> NodeView {
    estate.node_views().await.into_iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary).expect("mesh2's primary")
}

/// The frames of `stream` up to and including the first `Pending` frame of `operation`; a terminal
/// before it is a defect of the cell.
async fn until_pending(stream: &mut BuildStream, operation: &str) -> Vec<BuildFrame> {
    let mut frames = Vec::new();
    loop {
        let frame = tokio::time::timeout(SETTLE, stream.next()).await.expect("a frame within the bound").expect("the stream goes on").expect("the stream does not break");
        frames.push(frame.clone());
        match frame {
            BuildFrame::Pending { operation: o, .. } if o == operation => return frames,
            BuildFrame::Complete | BuildFrame::Failed { .. } | BuildFrame::Incomplete { .. } => panic!("the Build ended before {operation} was pending: {frames:?}"),
            _ => {}
        }
    }
}

/// CONTRACT (Q346: "A Build ends `complete` when every section finished, `incomplete` when it ran but
/// not every section finished (what ran stands)"; build.md §4: an incomplete Build is resubmitted by
/// its build_id; build.md §1: a step whose event ended `complete` is never run again; Q344: no
/// attempts, no claims, no next-pass pickup): a Build grows `mesh2` by two nodes. The first is created
/// and the second launched and held at its `pre_node_ready` when `mesh2`'s primary is killed: the section call ends
/// short, the Build ends `incomplete` naming `mesh2`, and nothing runs it on its own. `mesh2`'s other
/// node-admin takes the mesh's primary role, and the held node is released once its own view names
/// that primary: its start's one topology read goes to its own current mesh-primary (node-topology-get.md,
/// "exact member → own mesh-primary"), and a start finishes on its direct calls (Q90), so one released
/// while its view still names its lost launcher ends at that admin (#373). The resubmit by the build_id
/// dispatches `mesh2`'s section to that new primary, which reads the Build and its step events from
/// storage and runs only the steps whose event did not end `complete`; the Build ends `complete`. Must
/// NOT happen: a second Build, a run of the section before the resubmit, a step that ended `complete`
/// run again, or the released node's start ended at a step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resubmitted_incomplete_build_is_run_by_the_meshs_new_primary_from_the_step_events() {
    let test = "a_resubmitted_incomplete_build_is_run_by_the_meshs_new_primary_from_the_step_events";
    let mut estate = born(test).await;
    let old = mesh2_primary(&estate).await;
    let held_at = Estate::path("mesh2.node", "4");
    rdm_test::write_pre_node_ready_knob(&estate.root, &held_at, "held by the cell").unwrap();

    let mut stream = estate.submit_shape(FABRIC, &[("mesh1", 2, 1), ("mesh2", 2, 4)]).await;
    let build_id: BuildId = stream.build_id().clone();
    let b = build_id.to_string();
    let held_id = crate::build_log::created_id(&estate, &b, &held_at).await.to_string();
    let held_op = format!("node.create:{held_id}");
    until_pending(&mut stream, &held_op).await;
    drop(stream);

    // The section stands where it is: its mesh-primary is lost.
    let killed_ns = now_ns();
    estate.kill_node(&old.location()).await;
    let short = crate::build_log::build_fact(&estate, &b, "the Build ended short of complete", SETTLE, |p| p.state.ended_short()).await.unwrap_or_else(|end| panic!("{end}"));
    assert_eq!(short.state, BuildState::Incomplete, "the Build ran, so it is incomplete, never failed: {short:#?}");
    assert!(short.unfinished.iter().any(|u| u.mesh == "mesh2"), "the end names mesh2's section: {:?}", short.unfinished);
    let stood: Vec<(String, String)> = short.steps.iter().filter(|st| st.mesh == "mesh2" && st.outcome == StepOutcome::Complete).map(|st| (st.operation.clone(), st.step.clone())).collect();
    assert!(!stood.is_empty(), "mesh2's section completed steps before the loss: {:?}", short.steps);

    // mesh2's other node-admin takes the mesh's primary role.
    let new = estate
        .until_view("mesh2's other node-admin is its primary", SETTLE, |views| views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary && n.state == NodeStatus::Ready && n.node_id != old.node_id).cloned())
        .await;
    // The held node's own view names that primary before it is released: the entered admin's view
    // naming it is not yet the held node's (#373, as #369).
    let new_id = new.node_id.to_string();
    wait_for("the held node's own view names mesh2's new primary", SETTLE, || {
        let spans = estate.spans();
        let (held_id, new_id) = (held_id.clone(), new_id.clone());
        async move {
            named(&spans, "rdm.node_rpc.primary.update.via-topology")
                .into_iter()
                .any(|sp| attr(sp, "node") == held_id && attr(sp, "mesh") == "mesh2" && attr(sp, "primary_node_id") == new_id)
                .then_some(())
        }
    })
    .await;
    rdm_test::write_pre_node_ready_knob(&estate.root, &held_at, "pass").unwrap();
    let resubmitted_ns = now_ns();
    let resumed = tokio::time::timeout(SETTLE * 2, async { estate.follow_to_terminal(estate.resubmit(&build_id).await).await }).await.unwrap_or_else(|_| panic!("the resubmitted Build {b} has no terminal within {:?}", SETTLE * 2));
    assert_eq!(resumed.build_id, build_id, "never a second Build");
    assert!(matches!(resumed.frames.last(), Some(BuildFrame::Complete)), "the resubmitted Build completes: {:?}", resumed.frames);
    let done = crate::build_log::complete(&estate, &b, SETTLE).await;
    for (op, step) in &stood {
        let n = done.steps.iter().filter(|st| st.mesh == "mesh2" && st.operation == *op && st.step == *step && st.outcome == StepOutcome::Complete).count();
        assert_eq!(n, 1, "a step that ended complete before the loss is never run again: {op} {step}");
    }
    assert_eq!(estate.starts_of_build(&b).await, 2, "the submission and the one resubmit each started a run");

    estate.stop().await;
    let spans = estate.spans();
    let unfinished = named(&spans, "rdm.node_admin.build.update.via-section-unfinished").into_iter().find(|sp| attr(sp, "build_id") == b && attr(sp, "mesh") == "mesh2" && start(sp) >= killed_ns).cloned().unwrap_or_else(|| panic!("the drive named mesh2's section unfinished after the loss"));
    let sections: Vec<&Value> = named(&spans, "rdm.node_admin.build.serve.via-section-run").into_iter().filter(|sp| attr(sp, "build_id") == b && attr(sp, "mesh") == "mesh2").collect();
    let between: Vec<&&Value> = sections.iter().filter(|sp| start(sp) > killed_ns && start(sp) < resubmitted_ns).collect();
    assert!(between.is_empty(), "nothing ran mesh2's section between the loss and the resubmit: {between:?}");
    let picked = sections.iter().find(|sp| start(sp) >= resubmitted_ns).copied().cloned().unwrap_or_else(|| panic!("the resubmit's section call to mesh2"));
    assert_eq!((attr(&picked, "node_path"), attr(&picked, "node_name")), (new.path.as_str(), new.name.as_str()), "mesh2's new primary ran the resubmitted section: {picked}");
    let step_events: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-step-event").into_iter().filter(|sp| attr(sp, "build_id") == b && attr(sp, "mesh") == "mesh2" && sp["trace_id"] == picked["trace_id"]).collect();
    assert!(!step_events.is_empty(), "the new primary wrote the remaining steps' events in the resubmitted section's trace");
    assert!(step_events.iter().all(|sp| !stood.iter().any(|(op, step)| attr(sp, "op") == op && attr(sp, "step") == step)), "the resubmitted section wrote no event of a step that stood: {step_events:?}");
    let failed: Vec<&Value> = named(&spans, "rdm.mesh.node.reject.via-start-step-failed").into_iter().filter(|sp| attr(sp, "node_id") == held_id).collect();
    assert!(failed.is_empty(), "the released node's start ended at no step: {failed:?}");
    // mesh2's new primary may command the held node back through node.mesh.join while its first start
    // is held at its hook (node-mesh-join.md step 10: it "runs the whole sequence again on the commanding
    // call"): that cycle ends the first start's hook, so one start reads the node's topology, never two.
    let taken: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-topology-taken").into_iter().filter(|sp| attr(sp, "node_id") == held_id).collect();
    assert_eq!(taken.len(), 1, "the released node read its topology once, in the one start that ran past its hook: {taken:#?}");
    assert_eq!(attr(taken[0], "served_by"), new_id, "the released node read its topology from mesh2's new primary: {}", taken[0]);
    if let Some(cancelled) = named(&spans, "rdm.mesh.node.update.via-pre-node-ready-cancelled").into_iter().find(|sp| attr(sp, "node_id") == held_id) {
        println!("flame (the held node's first start ended Cancelled by its commanded cycle): http://192.168.68.99:16687/trace/{}", s(&cancelled["trace_id"]));
    }

    println!("flame (the released node's one topology read): http://192.168.68.99:16687/trace/{}", s(&taken[0]["trace_id"]));
    println!("flame (resubmitted section at mesh2's new primary): http://192.168.68.99:16687/trace/{}", s(&picked["trace_id"]));
    println!("flame (mesh2's section unfinished at the loss): http://192.168.68.99:16687/trace/{}", s(&unfinished["trace_id"]));
    estate.record_trace_url(picked["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-topology-get.md, "exact member → own mesh-primary"; node-start.md step 4: "obtain
/// topology once through node.topology.get"; Q90: a start finishes on its direct calls): a Build grows
/// `mesh2` by one node, held at its `pre_node_ready`. `mesh2`'s primary, which launched it, is stopped by
/// the OS (alive, answering nothing) and the hold is released at once, so the node's one topology read
/// goes to that primary while its own view still names it, and is under way there, unanswered.
/// `mesh2`'s other node-admin takes the mesh's primary role; once the node's own view names it, the
/// read under way is made again to it, which answers, and the node is Ready, its one topology step
/// served by mesh2's new primary. Must NOT happen (rdm#400): the read held at the lost launcher until
/// the start fails, the start ended at a step, or a second topology step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_starts_read_under_way_at_its_lost_launcher_is_made_again_to_the_meshs_new_primary() {
    let test = "a_first_starts_read_under_way_at_its_lost_launcher_is_made_again_to_the_meshs_new_primary";
    let mut estate = born(test).await;
    let old = mesh2_primary(&estate).await;
    let (old_id, held_at) = (old.node_id.to_string(), Estate::path("mesh2.node", "3"));
    rdm_test::write_pre_node_ready_knob(&estate.root, &held_at, "held by the cell").unwrap();
    let mut stream = estate.submit_shape(FABRIC, &[("mesh1", 2, 1), ("mesh2", 2, 3)]).await;
    let b = stream.build_id().to_string();
    let held_id = crate::build_log::created_id(&estate, &b, &held_at).await.to_string();
    until_pending(&mut stream, &format!("node.create:{held_id}")).await;
    drop(stream);

    // The launcher answers nothing from here, and the held node's hook passes at its next attempt.
    let rt = ExactRuntime::published(Path::new(&estate.data_dir(&old.location()).await)).expect("mesh2's primary published its runtime");
    let stopped = rt.apply(Fault::Stop).expect("mesh2's primary is stopped by the OS");
    assert_eq!(stopped.state_after, Some('T'), "the OS holds mesh2's primary: {stopped:?}");
    rdm_test::write_pre_node_ready_knob(&estate.root, &held_at, "pass").unwrap();
    let new = estate
        .until_view("mesh2's other node-admin is its primary", SETTLE, |views| views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary && n.state == NodeStatus::Ready && n.node_id != old.node_id).cloned())
        .await;
    let new_id = new.node_id.to_string();
    estate.until_node(&held_at, "the held node is Ready", SETTLE, |v| v.filter(|n| n.state == NodeStatus::Ready).cloned()).await;
    // The cell's own cleanup: the stopped launcher is no estate's.
    rt.apply(Fault::Kill(estate.fabric_ready())).expect("the cell kills the launcher it stopped");

    estate.stop().await;
    let spans = estate.spans();
    let failed: Vec<&Value> = named(&spans, "rdm.mesh.node.reject.via-start-step-failed").into_iter().filter(|sp| attr(sp, "node_id") == held_id).collect();
    assert!(failed.is_empty(), "the held node's start ended at no step: {failed:?}");
    let taken: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-topology-taken").into_iter().filter(|sp| attr(sp, "node_id") == held_id).collect();
    assert_eq!(taken.len(), 1, "the held node took its topology in one step: {taken:#?}");
    let taken = taken[0];
    assert_eq!((attr(taken, "launcher"), attr(taken, "served_by")), (old_id.as_str(), new_id.as_str()), "launched by the lost primary, its topology served by mesh2's new primary: {taken}");
    // The read went to the launcher first, under way there before the node's own view named the new
    // primary: the shape this cell holds, never a read made only after the election.
    let calls: Vec<&Value> = named(&spans, "rdm.node_rpc.request.update.via-call").into_iter().filter(|sp| sp["trace_id"] == taken["trace_id"] && attr(sp, "op") == "30" && start(sp) >= start(taken)).collect();
    let at_old = calls.iter().find(|sp| attr(sp, "target").contains(&old_id)).unwrap_or_else(|| panic!("the read went to the launcher first: {calls:#?}"));
    assert!(calls.iter().any(|sp| attr(sp, "target").contains(&new_id)), "the read was made again to mesh2's new primary: {calls:#?}");
    let named_new = named(&spans, "rdm.node_rpc.primary.update.via-topology").into_iter().filter(|sp| attr(sp, "node") == held_id && attr(sp, "mesh") == "mesh2" && attr(sp, "primary_node_id") == new_id).map(start).min().unwrap_or_else(|| panic!("the held node's own view named mesh2's new primary"));
    assert!(start(at_old) < named_new, "the read was under way at the launcher before the held node's view named the new primary: {at_old}");
    println!("flame (the read under way at the lost launcher made again to mesh2's new primary): http://192.168.68.99:16687/trace/{}", s(&taken["trace_id"]));
    estate.record_trace_url(taken["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (node-mesh-join.md step 10: "a birth commanded back leaves its channels, runs the whole
/// sequence again on the commanding call and publishes ready only once its hook has passed (Q87)";
/// node-recovery.md step 3: a held birth heard again "is commanded back through node.mesh.join by the
/// primary above it"; fabric-node-hooks.md "Cancellation and lifecycle commands": "Ending the in-flight
/// start cancels the token", and "A cancelled hook names the cancellation"): a Build grows `mesh2` by
/// one node, held at its `pre_node_ready`. `mesh2`'s primary, which launched it, is killed and `mesh2`'s
/// other node-admin takes the role. The held node is then hung past the staleness floor and released,
/// so its new primary hears it again and commands it back through node.mesh.join while its first start
/// is still held at its hook. That command begins the birth's next cycle, which ends the first start's
/// hook: it ends Cancelled, named on its span in the first start's trace, and runs nothing after it.
/// Once the hold is released the commanded sequence's hook passes, it reads its topology at mesh2's new
/// primary, and the node is Ready once. Must NOT happen (rdm#400): the first start's hook passing later
/// and reading its topology or reporting beside the commanded sequence, a second topology step or
/// report, or a start ended at a step.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_first_start_held_at_its_hook_ends_cancelled_when_its_new_primary_commands_it_back() {
    let test = "a_first_start_held_at_its_hook_ends_cancelled_when_its_new_primary_commands_it_back";
    let mut estate = born(test).await;
    let old = mesh2_primary(&estate).await;
    let held_at = Estate::path("mesh2.node", "3");
    rdm_test::write_pre_node_ready_knob(&estate.root, &held_at, "held by the cell").unwrap();
    let mut stream = estate.submit_shape(FABRIC, &[("mesh1", 2, 1), ("mesh2", 2, 3)]).await;
    let b = stream.build_id().to_string();
    let held_id = crate::build_log::created_id(&estate, &b, &held_at).await.to_string();
    until_pending(&mut stream, &format!("node.create:{held_id}")).await;
    drop(stream);
    estate.kill_node(&old.location()).await;
    let new = estate
        .until_view("mesh2's other node-admin is its primary", SETTLE, |views| views.iter().find(|n| n.mesh() == "mesh2" && n.r#type == "node-admin" && n.is_primary && n.state == NodeStatus::Ready && n.node_id != old.node_id).cloned())
        .await;
    let new_id = new.node_id.to_string();

    // The held node goes unheard past the floor, then is heard again: its new primary commands it back.
    // The OS stops it (its launcher is gone, so it is no child of this process to trace) and continues it.
    let floor = rdm_core::cadence::staleness_floor();
    let rt = ExactRuntime::published(Path::new(&estate.data_dir(&held_at).await)).expect("the held node published its runtime");
    let stopped = rt.apply(Fault::Stop).expect("the held node is stopped by the OS");
    assert_eq!(stopped.state_after, Some('T'), "the OS holds the held node: {stopped:?}");
    tokio::time::sleep(floor * 2).await;
    let released_ns = now_ns();
    rt.apply(Fault::Continue).expect("the held node is continued by the OS");
    wait_for("mesh2's new primary commands the held node back through node.mesh.join", SETTLE, || {
        let spans = estate.spans();
        let held_id = held_id.clone();
        async move { named(&spans, "rdm.node_admin.node.update.via-mesh-join-commanded").into_iter().any(|sp| attr(sp, "node_id") == held_id && start(sp) >= released_ns).then_some(()) }
    })
    .await;
    rdm_test::write_pre_node_ready_knob(&estate.root, &held_at, "pass").unwrap();
    estate.until_node(&held_at, "the held node is Ready", SETTLE, |v| v.filter(|n| n.state == NodeStatus::Ready).cloned()).await;

    estate.stop().await;
    let spans = estate.spans();
    let of_held = |name: &str| -> Vec<&Value> { named(&spans, name).into_iter().filter(|sp| attr(sp, "node_id") == held_id).collect() };
    let boot = of_held("rdm.mesh.node.create.via-boot").into_iter().next().unwrap_or_else(|| panic!("the held node's first start")).clone();
    let cancelled = of_held("rdm.mesh.node.update.via-pre-node-ready-cancelled");
    assert!(cancelled.iter().any(|sp| sp["trace_id"] == boot["trace_id"]), "the first start's hook ended Cancelled, named in its own trace: {cancelled:#?}");
    let passed_first: Vec<&Value> = of_held("rdm.mesh.node.update.via-pre-node-ready-attempt").into_iter().filter(|sp| sp["trace_id"] == boot["trace_id"] && attr(sp, "outcome") == "ok").collect();
    assert!(passed_first.is_empty(), "the first start's hook never passed after its cycle ended: {passed_first:#?}");
    let failed = of_held("rdm.mesh.node.reject.via-start-step-failed");
    assert!(failed.is_empty(), "no start of the held node ended at a step: {failed:#?}");
    let taken = of_held("rdm.mesh.node.update.via-topology-taken");
    assert_eq!(taken.len(), 1, "one topology step, the commanded sequence's: {taken:#?}");
    assert_ne!(taken[0]["trace_id"], boot["trace_id"], "the topology step is the commanded sequence's, never the first start's: {}", taken[0]);
    assert_eq!(attr(taken[0], "served_by"), new_id, "the commanded sequence read its topology at mesh2's new primary: {}", taken[0]);
    let ready = of_held("rdm.mesh.node.update.via-ready");
    assert_eq!(ready.len(), 1, "the held node reported ready once: {ready:#?}");
    let first = cancelled.iter().find(|sp| sp["trace_id"] == boot["trace_id"]).unwrap();
    println!("flame (the held node's first start ended Cancelled by its commanded cycle): http://192.168.68.99:16687/trace/{}", s(&first["trace_id"]));
    println!("flame (the commanded sequence's one topology read and report): http://192.168.68.99:16687/trace/{}", s(&taken[0]["trace_id"]));
    estate.record_trace_url(first["trace_id"].as_str().unwrap_or(""));
}

/// CONTRACT (Q346: "A failed step fails its mesh's section, never the Build, and a Build is never
/// reverted"; build.md §5: each step's outcome is its `rdm.build.step` event): one Build creates a node
/// in `mesh1` and one in `mesh2`, the `mesh2` node named on a copy of the node executable. `mesh2`'s
/// primary is held by its fault service at that create's `PrepareStorage` while the copy is removed; once
/// released, a later step of it fails by name (the executable is gone and nothing is launched),
/// which ends `mesh2`'s section there; `mesh1`'s section finishes and its node serves. The Build ends
/// `incomplete`, its end naming `mesh2`'s section with the failed step, and the stream's terminal is
/// `Incomplete`. Must NOT happen: the Build `failed` (it ran), `mesh1`'s create undone, a step of
/// `mesh2`'s section after the failed one, or the Build reverted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_step_ends_its_section_and_the_build_ends_incomplete() {
    let test = "a_failed_step_ends_its_section_and_the_build_ends_incomplete";
    let sha = rdm_test::faults::candidate_sha();
    let set = rdm_test::faults::binding_set(&sha);
    let mut estate = Estate::bootstrap_on(owner(test), FABRIC, "mesh1", &set, &sha, &["node"]).await.expect("the faulted-admin binding set is accepted");
    estate.resize_shape(FABRIC, &SHAPE).await;
    estate.settled_views(&SHAPE).await;
    estate.until_fabric_ready(SETTLE).await;
    let current = estate.current().await;
    let good = current.one_more("mesh1", "node", &estate.executables).unwrap_or_else(|e| panic!("{e}"));
    let mut bad = current.one_more("mesh2", "node", &estate.executables).unwrap_or_else(|e| panic!("{e}"));
    let binding = bad.executable.as_mut().expect("a node create names its executable");
    let doomed = estate.root.join("doomed-node-executable");
    std::fs::copy(&binding.executable, &doomed).expect("the executable is copied");
    binding.executable = doomed.clone();
    let (good_at, bad_at) = (rdm_core::PathName::new(&good.path, &good.name).unwrap(), rdm_core::PathName::new(&bad.path, &bad.name).unwrap());
    let faults = rdm_test::faults::FaultClient::at(&estate, &mesh2_primary(&estate).await.location()).await;
    faults.arm("held", serde_json::json!({"kind": "step-event", "step": "PrepareStorage", "operation": "node.create", "at": bad_at})).await;

    let stream = estate.submit_change(Change::Create { meshes: Vec::new(), nodes: vec![good, bad] }).await;
    let build_id = stream.build_id().clone();
    let b = build_id.to_string();
    let held = faults.wait_held("held").await;
    assert_eq!(s(&held["hit"]["build_id"]), b, "mesh2's primary is held in the Build's create: {held}");
    std::fs::remove_file(&doomed).expect("the copy is removed while the create is held");
    faults.release("held").await;
    let ended = estate.follow_to_terminal(stream).await;
    let Some(BuildFrame::Incomplete { sections }) = ended.frames.last().cloned() else { panic!("the Build ran, so its terminal is Incomplete: {:?}", ended.frames) };
    assert_eq!(sections.iter().map(|u| u.mesh.as_str()).collect::<Vec<_>>(), vec!["mesh2"], "only mesh2's section ended short: {sections:?}");

    let p = crate::build_log::build_fact(&estate, &b, "the Build ended", SETTLE, |p| p.state.ended_short() || p.state == BuildState::Complete).await.unwrap_or_else(|end| panic!("{end}"));
    assert_eq!(p.state, BuildState::Incomplete, "a failed step fails its section, never the Build: {p:#?}");
    let bad_op = format!("node.create:{}", crate::build_log::created_id(&estate, &b, &bad_at).await);
    let good_op = format!("node.create:{}", crate::build_log::created_id(&estate, &b, &good_at).await);
    let failed: Vec<_> = p.steps.iter().filter(|st| st.mesh == "mesh2" && matches!(st.outcome, StepOutcome::Failed { .. })).collect();
    assert_eq!(failed.len(), 1, "one failed step ended mesh2's section: {:?}", p.steps);
    assert_eq!(failed[0].operation, bad_op, "the failed step is the bad node's create");
    let unfinished = p.unfinished.iter().find(|u| u.mesh == "mesh2").expect("the end names mesh2's section");
    assert_eq!((unfinished.operation.as_str(), unfinished.step.as_str()), (bad_op.as_str(), failed[0].step.as_str()), "the end names the failed step: {unfinished:?}");
    let after: Vec<_> = p.steps.iter().skip_while(|st| !(st.mesh == "mesh2" && matches!(st.outcome, StepOutcome::Failed { .. }))).skip(1).filter(|st| st.mesh == "mesh2").collect();
    assert!(after.is_empty(), "no step of mesh2's section ran after the failed one: {after:?}");
    assert!(p.operation_completed(&rdm_core::OperationId::node("node.create", &crate::build_log::created_id(&estate, &b, &good_at).await)), "mesh1's create finished: {:?}", p.steps);
    let view = estate.until_node(&good_at, "mesh1's new node ready", SETTLE, |v| v.filter(|n| n.state == NodeStatus::Ready).cloned()).await;
    assert_eq!(view.mesh(), "mesh1", "mesh1's node serves: what ran stands");

    estate.stop().await;
    let spans = estate.spans();
    let failed_event = named(&spans, "rdm.node_admin.build.update.via-step-event").into_iter().find(|sp| attr(sp, "build_id") == b && attr(sp, "op") == bad_op && attr(sp, "state").contains("Failed")).cloned().unwrap_or_else(|| panic!("the failed step's event was written"));
    let unfinished_span = named(&spans, "rdm.node_admin.build.update.via-section-unfinished").into_iter().find(|sp| attr(sp, "build_id") == b && attr(sp, "mesh") == "mesh2").cloned().unwrap_or_else(|| panic!("the drive named mesh2's section unfinished"));
    let ended_span = named(&spans, "rdm.node_admin.build.update.via-on-build-ended").into_iter().find(|sp| attr(sp, "build_id") == b).cloned().unwrap_or_else(|| panic!("the Build's end was written"));
    assert_eq!(attr(&ended_span, "state"), "incomplete", "the Build's end is incomplete: {ended_span}");
    let good_event = named(&spans, "rdm.node_admin.build.update.via-step-event").into_iter().any(|sp| attr(sp, "build_id") == b && attr(sp, "op") == good_op && attr(sp, "step") == "Complete");
    assert!(good_event, "mesh1's create wrote its Complete step event");
    println!("flame (failed step): http://192.168.68.99:16687/trace/{}", s(&failed_event["trace_id"]));
    println!("flame (section unfinished, Build incomplete): http://192.168.68.99:16687/trace/{}", s(&unfinished_span["trace_id"]));
    estate.record_trace_url(failed_event["trace_id"].as_str().unwrap_or(""));
}
