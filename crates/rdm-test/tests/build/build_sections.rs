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
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
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
    estate.node_views().await.into_iter().find(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.is_primary).expect("mesh2's primary")
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
/// node-admin takes the mesh's primary role. The resubmit by the build_id dispatches `mesh2`'s section
/// to that new primary, which reads the Build and its step events from storage and runs only the steps
/// whose event did not end `complete`; the Build ends `complete`. Must NOT happen: a second Build, a
/// run of the section before the resubmit, or a step that ended `complete` run again.
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
    let held_op = format!("node.create:{}", crate::build_log::created_id(&estate, &b, &held_at).await);
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
        .until_view("mesh2's other node-admin is its primary", SETTLE, |views| views.iter().find(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.is_primary && n.status == NodeStatus::Ready && n.node_id != old.node_id).cloned())
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

    println!("flame (resubmitted section at mesh2's new primary): http://192.168.68.99:16687/trace/{}", s(&picked["trace_id"]));
    println!("flame (mesh2's section unfinished at the loss): http://192.168.68.99:16687/trace/{}", s(&unfinished["trace_id"]));
    estate.record_trace_url(picked["trace_id"].as_str().unwrap_or(""));
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
    let view = estate.until_node(&good_at, "mesh1's new node ready", SETTLE, |v| v.filter(|n| n.status == NodeStatus::Ready).cloned()).await;
    assert_eq!(view.mesh, "mesh1", "mesh1's node serves: what ran stands");

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
