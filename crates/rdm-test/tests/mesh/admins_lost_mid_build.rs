//! i143.e4.s7 process E2E: node investigation and node recovery of the lost mesh-primary of a mesh
//! whose node-admins are lost while a Build runs (node-recovery.md step 2, Q335; node-create.md "A
//! node-admin created into a live mesh"; PRD §12.2).
//!
//! desired {mesh1, mesh2}; a Build B is active (growing mesh2, executed by mesh2's admin primary)
//! when both of mesh2's node-admins and every birth B's first attempt launched are killed; the three
//! members born before B stay alive, so mesh2 is a live mesh with no node-admin. From public
//! surfaces only:
//! - control is found again through the advertised endpoints;
//! - B completes under its own id: an admin of mesh1 takes it over;
//! - mesh2 keeps its MeshId and its node names;
//! - every node-admin of mesh2 is a new birth, created into the live mesh; the members that stayed
//!   alive keep theirs;
//! - mesh1 was never touched (every mesh1 node keeps its incarnation).
//!
//! Evidence: the fabric-primary's primary watch runs node investigation on mesh2's lost mesh-primary
//! (`rdm.node_admin.primary.update.via-investigation`), and on its verdict submits that birth's node
//! recovery (`rdm.node_admin.build.update.via-node-recovery`): no node-admin of mesh2 is left for the
//! election to name, so the fabric-primary executes it; a reconcile of B by a mesh1 admin names mesh2's
//! admin primary as the previous executor.

use rdm_core::PathName;
use rdm_test::estate::location;
use crate::build_log;
use rdm_client::{FabricDesired, MeshDesired, Meshes};
use rdm_test::typed::BuildSpec;
use rdm_admin::build_state::BuildState;
use rdm_test::estate::{claim_decider, named, Estate, Owner};
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
    nodes.iter().map(|n| (n.name.clone(), n.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default())).collect()
}

/// The Build that reconciles the fabric to `mesh2` with `r2` rpc nodes beside `mesh1`.
fn shape(r2: u32) -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [("node-admin", 2), ("node", 3)]), MeshDesired::of("mesh2", [("node-admin", 2), ("node", r2)])] }
}

/// CONTRACT: docs/architecture/lifecycles/node-recovery.md
/// "(A mesh none of whose node-admins can be reached is this page's case with no successor named: the
/// fabric-primary investigates the lost mesh-primary and re-creates it at its path.name, Q335.)"
/// CONTRACT: docs/architecture/lifecycles/node-create.md "A node-admin created into a live mesh"
/// "A node-admin that node recovery re-creates into a mesh that lost its node-admins ... joins that mesh
/// under the mesh's own MeshId and never mints another. The mesh is still in the accepted topology, so
/// the attempt belongs to the SAME accepted Build."
/// CONTRACT (R-D1): mesh2 loses both node-admins mid-Build while three of its members live on. The
/// fabric-primary (mesh1's primary) runs node investigation on mesh2's lost mesh-primary and, on its
/// verdict, recovers it at its path.name; the re-created node-admin is mesh2's primary, recovers the
/// rest of its mesh, and B completes. Must NOT happen: a node of mesh2 created by B before the
/// recovery (the target holds), a new MeshId, a member that stayed alive born again, or mesh1 touched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_whose_admins_are_lost_mid_build_has_its_mesh_primary_recovered_by_the_fabric_primary_and_the_build_completes() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.reconcile(&shape(3)).await;
    let before = estate.settles_to_named(&names(&[("mesh1", 2, 3), ("mesh2", 2, 3)]), Duration::from_secs(15)).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let mesh2_id = estate.mesh_view("mesh2").await.id.expect("mesh2 carries its id").to_string();
    // mesh2's members are grown by mesh2's admin primary (the lowest NodeId of its admins).
    let mesh2_primary = before.iter().find(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.is_primary).map(|n| n.node_id.clone()).unwrap();
    let mesh1_primary_id = estate.mesh_view("mesh1").await.primary_admin.expect("mesh1 roles a primary admin");
    let mesh1_primary = before.iter().find(|n| n.node_id == mesh1_primary_id).map(|n| n.name.clone()).expect("mesh1's primary admin is settled");
    let mesh1_admin_ids: BTreeSet<String> = before.iter().filter(|n| n.mesh == "mesh1" && n.r#type == "node-admin").map(|n| n.node_id.to_string()).collect();

    // B: grow mesh2. Once its first attempt is claimed, lose mesh2's node-admins. Its births run one
    // after another (~55 ms each on the process provider), so six of them keep the first attempt
    // running across several polls; two finished inside one 100 ms poll.
    let stream = estate.submit(&BuildSpec::Fabric(shape(9))).await;
    let b = stream.build_id().to_string();
    // Active fabric-wide: the fabric-primary's mesh already sees mesh2's admin holding B's first attempt.
    build_log::build_fact(&estate, &b, "mesh1's admin sees B's first attempt held by mesh2's admin", Duration::from_secs(30), |p| p.attempt == 1 && p.state == BuildState::Running && p.executor.as_ref().is_some_and(|e| e.node_id == mesh2_primary))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    // mesh2's node-admins and every birth B's first attempt launched; the members born before B
    // (rpc.1..3) stay alive, so a member of mesh2 can say it cannot reach its own node-admin.
    let kept: BTreeSet<PathName> = (1..=3).map(|i| Estate::path("mesh2.node", &i.to_string())).collect();
    // A runtime's data dir is named by its node id: every birth B launched is mesh2's, so a runtime
    // whose id is neither mesh1's nor a kept member's is lost.
    let survivors: BTreeSet<String> = before.iter().filter(|n| n.mesh == "mesh1" || kept.contains(&n.name)).map(|n| n.node_id.to_string()).collect();
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

    // B completes under its own id, and mesh2 keeps its MeshId and names.
    let done = build_log::attempt_complete(&estate, &b, 2, Duration::from_secs(180)).await;
    assert!(done.attempt > 1, "a successor continued B: {done:#?}");
    let after = estate.settles_to_named(&names(&[("mesh1", 2, 3), ("mesh2", 2, 9)]), Duration::from_secs(60)).await;
    let mesh2 = estate.mesh_view("mesh2").await;
    assert_eq!(mesh2.id.as_ref().map(|i| i.to_string()), Some(mesh2_id.clone()), "mesh2 keeps its MeshId: {mesh2:#?}");
    // ...in mesh2's created admins too: they carry its identity.
    for admin in after.iter().filter(|n| n.mesh == "mesh2" && n.r#type == "node-admin") {
        let own = rdm_test::typed::one_mesh(&Meshes::new(estate.carrier_at(&admin.name).await), "mesh2").await.unwrap_or_else(|e| panic!("mesh.get [name: mesh2] at {:?}: {e}", admin.name)).unwrap_or_else(|| panic!("{:?} holds mesh2", admin.name));
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
    let create = named(&spans, "rdm.node_admin.build.update.via-node-recovery").into_iter().find(|sp| attr(sp, "node_id") == mesh2_primary.as_str() && at(sp) >= at(&look)).cloned().unwrap_or_else(|| panic!("the fabric-primary submitted the node recovery of mesh2's lost mesh-primary on the verdict"));
    // The target held: no node of mesh2 was created by B before the recovery.
    let early: Vec<String> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .chain(named(&spans, "rdm.node_admin.node.update.via-build"))
        .filter(|c| attr(c, "build_id") == b && attr(c, "node_path").starts_with("mesh2.") && at(c) > killed_ns && at(c) < at(&create))
        .map(|c| format!("{}/{} attempt {} at {}", attr(c, "node_path"), attr(c, "node_name"), attr(c, "attempt"), at(c)))
        .collect();
    assert!(early.is_empty(), "nothing of mesh2 is created by B before its mesh-primary's recovery (at {}): {early:?}", at(&create));
    let accepting = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| location(&n)).expect("a fabric-primary before the loss");
    let takeover = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|r| {
            let a = &r["attributes"];
            a["build_id"] == b.as_str() && a["previous_executor"].as_str().is_some_and(|p| p.contains(mesh2_primary.as_str())) && a["executor_node_id"].as_str().is_some_and(|e| mesh1_admin_ids.contains(e))
        })
        .unwrap_or_else(|| panic!("no takeover of {b} by a mesh1 admin"))
        .clone();
    let attempt = takeover["attributes"]["attempt"].as_str().map(String::from).unwrap_or_else(|| takeover["attributes"]["attempt"].to_string());
    let decider = claim_decider(&spans, b.as_str(), &attempt).unwrap_or_else(|| panic!("the claim of takeover attempt {attempt} of {b} has its decision span"));
    if takeover["attributes"]["reason"] == "recovery" {
        // The takeover is the lost mesh-primary's node recovery (Q335): its attempt is that recovery's story.
        assert_eq!(takeover["trace_id"], create["trace_id"], "the recovery attempt that took B's mesh over shares the recovery submit's trace: {takeover}");
    } else if decider == accepting {
        let accepted = named(&spans, "rdm.node_admin.build.create.via-accepted").into_iter().find(|sp| sp["attributes"]["build_id"] == b.as_str()).expect("B's request").clone();
        assert_eq!(takeover["trace_id"], accepted["trace_id"], "a takeover the accepting fabric-primary decided shares B's request trace");
    } else {
        assert_eq!(takeover["parent_span_id"].as_str().unwrap_or(""), "", "a takeover decided after the role moved starts its own trace: {takeover}");
    }
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
    eprintln!("admins_lost_mid_build: investigation trace {} | recovery trace {} | takeover trace {}", s(&look["trace_id"]), s(&create["trace_id"]), s(&takeover["trace_id"]));
    estate.record_trace_url(takeover["trace_id"].as_str().unwrap_or(""));
}
