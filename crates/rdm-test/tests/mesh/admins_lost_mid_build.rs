//! i143.e4.s7 process E2E: node investigation and node recovery of the lost mesh-primary of a mesh
//! whose node-admins are lost while a Build runs (node-recovery.md step 2, Q335; node-create.md "A
//! node-admin created into a live mesh"; PRD §12.2).
//!
//! desired {mesh1, mesh2}; a Build B is running (growing mesh2, mesh2's section run by mesh2's
//! mesh-primary) when both of mesh2's node-admins and every birth B launched are killed; the three
//! members born before B stay alive, so mesh2 is a live mesh with no node-admin. From public
//! surfaces only:
//! - control is found again through the advertised endpoints;
//! - B ends incomplete naming mesh2's section (Q346), and its resubmit by its build_id completes it:
//!   mesh2's re-created mesh-primary runs the section from B's step events;
//! - mesh2 keeps its MeshId and its node names;
//! - every node-admin of mesh2 is a new birth, created into the live mesh; the members that stayed
//!   alive keep theirs;
//! - mesh1 was never touched (every mesh1 node keeps its incarnation).
//!
//! Evidence: the fabric-primary's primary watch runs node investigation on mesh2's lost mesh-primary
//! (`rdm.node_admin.primary.update.via-investigation`), and on its verdict submits that birth's node
//! recovery (`rdm.node_admin.node.update.via-node-recovery`): no node-admin of mesh2 is left for the
//! election to name, so the fabric-primary runs it; the resubmitted B's section call to mesh2 is served
//! by mesh2's re-created mesh-primary (`rdm.node_admin.build.serve.via-attempt-run`).

use rdm_core::PathName;
use rdm_test::estate::location;
use crate::build_log;
use rdm_client::{FabricDesired, MeshDesired, Meshes};
use rdm_test::typed::BuildSpec;
use rdm_admin::build_state::BuildState;
use rdm_client::BuildFrame;
use rdm_test::estate::{named, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::process::Command;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-lifecycle".into(),
        subfeature: "mesh-investigation".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_mesh_whose_admins_are_lost_mid_build_has_its_mesh_primary_recovered_by_the_fabric_primary_and_the_build_completes".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn names(meshes: &[(&str, u32, u32)]) -> BTreeSet<PathName> {
    let mut out = BTreeSet::new();
    for (m, a, r) in meshes {
        out.extend((1..=*a).map(|i| Estate::path(&format!("{m}.node-admin"), &i.to_string())));
        out.extend((1..=*r).map(|i| Estate::path(&format!("{m}.node"), &i.to_string())));
    }
    out
}

/// location -> incarnation
fn births(nodes: &[rdm_client::NodeView]) -> BTreeMap<PathName, String> {
    nodes.iter().map(|n| (n.location(), n.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default())).collect()
}

/// The Build that reconciles the fabric to `mesh2` with `r2` nodes beside `mesh1`.
fn shape(r2: u32) -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 2), ("node", 3)]), MeshDesired::of("mesh2", [("node-admin", 2), ("node", r2)])] }
}

/// CONTRACT: docs/architecture/lifecycles/node-recovery.md
/// "(A mesh none of whose node-admins can be reached is this page's case with no successor named: the
/// fabric-primary investigates the lost mesh-primary and re-creates it at its path.name, Q335.)"
/// CONTRACT (Q346): "A failed step fails its mesh's section, never the Build ... A Build ends
/// `complete` when every section finished, `incomplete` when it ran but not every section finished
/// (what ran stands)"; build.md §4: an incomplete Build is resubmitted by its build_id.
/// CONTRACT (R-D1): mesh2 loses both node-admins mid-Build while three of its members live on. B's
/// section call to mesh2 ends short and B ends incomplete naming mesh2. The fabric-primary (mesh1's
/// primary) runs node investigation on mesh2's lost mesh-primary and, on its verdict, recovers it at
/// its path.name; the re-created node-admin is mesh2's primary and recovers the rest of its mesh. The
/// resubmit of B by its build_id dispatches mesh2's section to that re-created mesh-primary, which runs
/// only the steps whose event did not end `complete`, and B completes. Must NOT happen: a node of
/// mesh2 created by B between the loss and the resubmit, a new MeshId, a member that stayed alive born
/// again, mesh1 touched, or a step of B that ended `complete` run again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_whose_admins_are_lost_mid_build_has_its_mesh_primary_recovered_by_the_fabric_primary_and_the_build_completes() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.resize(&shape(3)).await;
    let before = estate.settles_to_named(&names(&[("mesh1", 2, 3), ("mesh2", 2, 3)]), Duration::from_secs(15)).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let mesh2_id = estate.mesh_view("mesh2").await.id.expect("mesh2 carries its id").to_string();
    // mesh2's members are grown by mesh2's admin primary (the lowest NodeId of its admins).
    let mesh2_primary = before.iter().find(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.is_primary).map(|n| n.node_id.clone()).unwrap();
    let mesh1_primary_id = estate.mesh_view("mesh1").await.primary_admin.expect("mesh1 roles a primary admin");
    let mesh1_primary = before.iter().find(|n| n.node_id == mesh1_primary_id).map(|n| n.location()).expect("mesh1's primary admin is settled");


    // B: grow mesh2. Once mesh2's section is dispatched, lose mesh2's node-admins. Its births run one
    // after another (~55 ms each on the process provider), so six of them keep the section running
    // across several polls; two finished inside one 100 ms poll.
    let stream = estate.submit(&BuildSpec::Fabric(shape(9))).await;
    let b = stream.build_id().to_string();
    let b_id = stream.build_id().clone();
    // Running fabric-wide: the fabric-primary dispatched mesh2's section (`rdm.build.dispatched`).
    build_log::build_fact(&estate, &b, "B running with mesh2's section dispatched", Duration::from_secs(30), |p| p.state == BuildState::Running && p.dispatched.contains("mesh2"))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    // mesh2's node-admins and every birth B launched; the members born before B
    // (rpc.1..3) stay alive, so a member of mesh2 can say it cannot reach its own node-admin.
    let kept: BTreeSet<PathName> = (1..=3).map(|i| Estate::path("mesh2.node", &i.to_string())).collect();
    // A runtime's data dir is named by its node id: every birth B launched is mesh2's, so a runtime
    // whose id is neither mesh1's nor a kept member's is lost.
    let survivors: BTreeSet<String> = before.iter().filter(|n| n.mesh == "mesh1" || kept.contains(&n.location())).map(|n| n.node_id.to_string()).collect();
    estate.fabric_ready();
    for (dir, pid) in estate.live_runtimes() {
        let id = dir.file_name().unwrap().to_string_lossy().to_string();
        if !survivors.contains(&id) {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
    }
    drop(stream);
    let killed_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // Control stays at the surviving fabric-primary mesh's admin.
    estate.until_view_at(&mesh1_primary, "a surviving admin answers", Duration::from_secs(30), |_| Some(())).await;
    estate.enter(&mesh1_primary).await;

    // B ends incomplete naming mesh2's section: its section call ended short with its mesh-primary.
    let short = build_log::build_fact(&estate, &b, "B ended incomplete naming mesh2", Duration::from_secs(120), |p| p.state.ended_short()).await.unwrap_or_else(|end| panic!("{end}"));
    assert_eq!(short.state, BuildState::Incomplete, "B ran, so it is incomplete, never failed: {short:#?}");
    assert!(short.unfinished.iter().any(|u| u.mesh == "mesh2"), "B's end names mesh2's section: {:?}", short.unfinished);
    let stood: Vec<(String, String, String)> = short.steps.iter().filter(|s| s.outcome == rdm_admin::build_state::StepOutcome::Complete).map(|s| (s.mesh.clone(), s.operation.clone(), s.step.clone())).collect();
    // mesh2's mesh-primary is recovered at its path.name: both its node-admins are back.
    estate
        .until_view("both of mesh2's node-admins recovered and ready", Duration::from_secs(180), |views| {
            let admins: Vec<&rdm_client::NodeView> = views.iter().filter(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.status == rdm_client::NodeStatus::Ready).collect();
            (admins.len() == 2 && admins.iter().all(|n| n.node_id != mesh2_primary)).then_some(())
        })
        .await;
    let resubmitted_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // The resubmit by B's build_id completes it, and mesh2 keeps its MeshId and names.
    let resumed = estate.follow_to_terminal(estate.resubmit(&b_id).await).await;
    assert!(matches!(resumed.frames.last(), Some(BuildFrame::Complete)), "the resubmitted B completes: {:?}", resumed.frames);
    let done = build_log::complete(&estate, &b, Duration::from_secs(60)).await;
    for (mesh, op, step) in &stood {
        let n = done.steps.iter().filter(|s| s.mesh == *mesh && s.operation == *op && s.step == *step && s.outcome == rdm_admin::build_state::StepOutcome::Complete).count();
        assert_eq!(n, 1, "a step that ended complete before the loss is never run again: {mesh} {op} {step}");
    }
    let after = estate.settles_to_named(&names(&[("mesh1", 2, 3), ("mesh2", 2, 9)]), Duration::from_secs(60)).await;
    let mesh2 = estate.mesh_view("mesh2").await;
    assert_eq!(mesh2.id.as_ref().map(|i| i.to_string()), Some(mesh2_id.clone()), "mesh2 keeps its MeshId: {mesh2:#?}");
    // ...in mesh2's created admins too: they carry its identity.
    for admin in after.iter().filter(|n| n.mesh == "mesh2" && n.r#type == "node-admin") {
        let own = rdm_test::typed::one_mesh(&Meshes::new(estate.carrier_at(&admin.location()).await), "mesh2").await.unwrap_or_else(|e| panic!("mesh.get [name: mesh2] at {:?}: {e}", admin.name)).unwrap_or_else(|| panic!("{:?} holds mesh2", admin.name));
        assert_eq!(own.id.as_ref().map(|i| i.to_string()), Some(mesh2_id.clone()), "{:?} serves mesh2 under its own identity: {own:#?}", admin.name);
    }
    let (old, new) = (births(&before), births(&after));
    for (name, incarnation) in &old {
        let now_inc = &new[name];
        if name.path == "mesh2.node-admin" {
            assert_ne!(now_inc, incarnation, "{name:?} is a new birth");
        } else if kept.contains(name) {
            assert_eq!(now_inc, incarnation, "{name:?} stayed alive through the loss and keeps its birth");
        } else {
            assert_eq!(now_inc, incarnation, "{name:?} was never touched");
        }
    }
    let (before, after) = (Estate::values(&before), Estate::values(&after));

    // The estate stops through whoever holds the fabric now.
    estate.artifact("nodes.json", &json!(after));
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    // The verdict: the fabric-primary's node investigation of mesh2's lost mesh-primary, then that birth's
    // node recovery on it (Q335).
    let look = named(&spans, "rdm.node_admin.primary.update.via-investigation").into_iter().find(|sp| attr(sp, "suspect_node_id") == mesh2_primary.as_str() && attr(sp, "node_path") == "mesh1.node-admin" && at(sp) > killed_ns).cloned().unwrap_or_else(|| panic!("a mesh1 admin ran node investigation on mesh2's lost mesh-primary {mesh2_primary}"));
    assert_ne!(attr(&look, "finding"), "reached", "the killed mesh-primary is not reached: {look}");
    let create = named(&spans, "rdm.node_admin.node.update.via-node-recovery").into_iter().find(|sp| attr(sp, "node_id") == mesh2_primary.as_str() && at(sp) >= at(&look)).cloned().unwrap_or_else(|| panic!("the fabric-primary submitted the node recovery of mesh2's lost mesh-primary on the verdict"));
    // Nothing of mesh2 was created by B between the loss and the resubmit: B had ended.
    let early: Vec<String> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| attr(c, "build_id") == b && attr(c, "node_path").starts_with("mesh2.") && at(c) > killed_ns && at(c) < resubmitted_ns)
        .map(|c| format!("{}/{} at {}", attr(c, "node_path"), attr(c, "node_name"), at(c)))
        .collect();
    assert!(early.is_empty(), "nothing of mesh2 is created by B between the loss and its resubmit: {early:?}");
    assert!(at(&create) < resubmitted_ns, "mesh2's mesh-primary was recovered before the resubmit");
    // B's section ended short at the loss, named on the fabric-primary's drive.
    let unfinished = named(&spans, "rdm.node_admin.build.update.via-section-unfinished").into_iter().find(|sp| attr(sp, "build_id") == b && attr(sp, "mesh") == "mesh2").cloned().unwrap_or_else(|| panic!("B's drive named mesh2's section unfinished"));
    // The resubmitted B's section call to mesh2 is served by mesh2's re-created admin, not by a mesh1 admin.
    let section = named(&spans, "rdm.node_admin.build.serve.via-attempt-run")
        .into_iter()
        .find(|sp| attr(sp, "build_id") == b && attr(sp, "mesh") == "mesh2" && at(sp) > resubmitted_ns)
        .cloned()
        .unwrap_or_else(|| panic!("the resubmitted B's section call to mesh2 was served"));
    assert_eq!(attr(&section, "node_path"), "mesh2.node-admin", "mesh2's section runs on mesh2's own mesh-primary: {section}");

    // Each created admin held every surviving member's runtime before it
    // committed Ready (i143.e4.s16): mesh1's five births at least.
    for admin in after.iter().filter(|n| n["mesh"] == "mesh2" && n["type"] == "node-admin") {
        let ready = named(&spans, "rdm.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| rdm_test::estate::of_node(sp, &location(admin), admin["node_id"].as_str().unwrap_or_default()) && sp["attributes"]["incarnation_id"] == admin["incarnation_id"])
            .unwrap_or_else(|| panic!("{} reports ready", admin["name"]))
            .clone();
        let held = ready["attributes"]["runtime_facts_held"].as_u64().or_else(|| ready["attributes"]["runtime_facts_held"].as_str().and_then(|v| v.parse().ok())).unwrap_or(0);
        assert!(held >= 5, "{} held {held} runtime facts before Ready: {ready}", admin["name"]);
    }
    eprintln!("admins_lost_mid_build: investigation trace {} | recovery trace {} | unfinished trace {} | resubmitted section trace {}", s(&look["trace_id"]), s(&create["trace_id"]), s(&unfinished["trace_id"]), s(&section["trace_id"]));
    estate.record_trace_url(section["trace_id"].as_str().unwrap_or(""));
}
