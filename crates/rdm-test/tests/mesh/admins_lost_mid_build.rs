//! i143.e4.s7 process E2E: mesh investigation of a mesh whose node-admins are lost while a Build runs
//! (mesh-investigation.md steps 1-7, "A node-admin joins the live mesh"; PRD §12.2).
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
//! Evidence: the fabric-primary's investigation of mesh2 finds, through a member of mesh2 that
//! answers, that mesh2's node-admins cannot be reached (`carrier-edge-lost`), records the verdict,
//! and only then runs node.create of mesh2's first node-admin (span `via-recovery`); a reconcile of
//! B by a mesh1 admin names mesh2's admin primary as the previous executor.

use crate::build_log;
use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, Meshes};
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
        test: "a_mesh_whose_admins_are_lost_mid_build_gets_a_node_admin_on_the_carrier_edge_lost_verdict_and_the_build_completes".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn names(meshes: &[(&str, u32, u32)]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (m, a, r) in meshes {
        out.extend((1..=*a).map(|i| format!("{m}.admin.{i}")));
        out.extend((1..=*r).map(|i| format!("{m}.rpc.{i}")));
    }
    out
}

/// name -> incarnation
fn births(nodes: &[rdm_client::NodeView]) -> BTreeMap<String, String> {
    nodes.iter().map(|n| (n.name.to_string(), n.incarnation_id.as_ref().map(|i| i.as_str().to_string()).unwrap_or_default())).collect()
}

/// The Build that reconciles the fabric to `mesh2` with `r2` rpc nodes beside `mesh1`.
fn shape(r2: u32) -> FabricDesired {
    FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 3)]), MeshDesired::of("mesh2", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, r2)])] }
}

/// CONTRACT (mesh-investigation.md step 5: "A node-admin is created into mesh-x in one of two cases
/// only: Members live. The latest probe answers `carrier-edge-lost`: a mesh-x member reports that it
/// cannot reach its own node-admin ... Record the verdict before opening the node.create attempt";
/// step 6: "A carrier-edge-lost verdict runs node.create of a node-admin into mesh-x, on the SAME
/// accepted Build and mesh-x's own MeshId: a node-admin joining a live mesh"; R-D1): mesh2 loses both
/// node-admins mid-Build while three of its members live on. The fabric-primary (mesh1's primary)
/// probes mesh2 through a member that answers, hears `carrier-edge-lost` for each node-admin,
/// records the verdict, and only then runs node.create of mesh2's first node-admin under B; B
/// completes. Must NOT happen: a node of mesh2 created by B before the verdict (the target holds), a
/// new MeshId, a member that stayed alive born again, or mesh1 touched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mesh_whose_admins_are_lost_mid_build_gets_a_node_admin_on_the_carrier_edge_lost_verdict_and_the_build_completes() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.reconcile(&shape(3)).await;
    let before = estate.settles_to_named(&names(&[("mesh1", 2, 3), ("mesh2", 2, 3)]), Duration::from_secs(15)).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let mesh2_id = estate.mesh_view("mesh2").await.id.expect("mesh2 carries its id").to_string();
    // mesh2's members are grown by mesh2's admin primary (the lowest NodeId of its admins).
    let mesh2_primary = before.iter().find(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin && n.is_primary).map(|n| n.name.to_string()).unwrap();
    let mesh1_primary = estate.mesh_view("mesh1").await.primary_admin.expect("mesh1 roles a primary admin");

    // B: grow mesh2. Once its first attempt is claimed, lose mesh2's node-admins. Its births run one
    // after another (~55 ms each on the process provider), so six of them keep the first attempt
    // running across several polls; two finished inside one 100 ms poll.
    let stream = estate.submit(&BuildSpec::Fabric(shape(9))).await;
    let b = stream.build_id().to_string();
    // Active fabric-wide: the fabric-primary's mesh already sees mesh2's admin holding B's first attempt.
    build_log::build_fact(&estate.carrier_at(&mesh1_primary).await, &b, "mesh1's admin sees B's first attempt held by mesh2's admin", Duration::from_secs(30), |p| p.attempt == 1 && p.state == BuildState::Running && p.executor.as_deref() == Some(mesh2_primary.as_str()))
        .await
        .unwrap_or_else(|end| panic!("{end}"));
    // mesh2's node-admins and every birth B's first attempt launched; the members born before B
    // (rpc.1..3) stay alive, so a member of mesh2 can say it cannot reach its own node-admin.
    let kept: BTreeSet<String> = (1..=3).map(|i| format!("mesh2.rpc.{i}")).collect();
    estate.fabric_ready();
    for (dir, pid) in estate.live_runtimes() {
        let name = dir.file_name().unwrap().to_string_lossy().to_string();
        let path = name.split('-').next().unwrap_or_default().to_string();
        if path.starts_with("mesh2.") && !kept.contains(&path) {
            let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
    }
    drop(stream);
    let killed_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // Control stays at the surviving fabric-primary mesh's admin.
    estate.until_view_at(&mesh1_primary, "a surviving admin answers", Duration::from_secs(30), |_| Some(())).await;
    estate.enter(&mesh1_primary).await;

    // B completes under its own id, and mesh2 keeps its MeshId and names.
    let done = build_log::attempt_complete(&estate.carrier().await, &b, 2, Duration::from_secs(180)).await;
    assert!(done.attempt > 1, "a successor continued B: {done:#?}");
    let after = estate.settles_to_named(&names(&[("mesh1", 2, 3), ("mesh2", 2, 9)]), Duration::from_secs(60)).await;
    let mesh2 = estate.mesh_view("mesh2").await;
    assert_eq!(mesh2.id.as_ref().map(|i| i.to_string()), Some(mesh2_id.clone()), "mesh2 keeps its MeshId: {mesh2:#?}");
    // ...in mesh2's created admins too: they carry its identity.
    for admin in after.iter().filter(|n| n.mesh == "mesh2" && n.kind == NodeKind::NodeAdmin) {
        let own = rdm_test::typed::one_mesh(&Meshes::new(estate.carrier_at(&admin.name).await), "mesh2").await.unwrap_or_else(|e| panic!("mesh.get [name: mesh2] at {}: {e}", admin.name)).unwrap_or_else(|| panic!("{} holds mesh2", admin.name));
        assert_eq!(own.id.as_ref().map(|i| i.to_string()), Some(mesh2_id.clone()), "{} serves mesh2 under its own identity: {own:#?}", admin.name);
    }
    let (old, new) = (births(&before), births(&after));
    for (name, incarnation) in &old {
        let now_inc = &new[name];
        if name.starts_with("mesh2.admin.") {
            assert_ne!(now_inc, incarnation, "{name} is a new birth");
        } else if kept.contains(name) {
            assert_eq!(now_inc, incarnation, "{name} stayed alive through the loss and keeps its birth");
        } else {
            assert_eq!(now_inc, incarnation, "{name} was never touched");
        }
    }
    let (before, after) = (Estate::values(&before), Estate::values(&after));

    // The estate stops through whoever holds the fabric now.
    estate.artifact("nodes.json", &json!(after));
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    // The verdict: the fabric-primary's probes of mesh2 went through a member that answered and
    // reported mesh2's node-admins lost, the latest probe decided, and the node.create opened.
    let create = named(&spans, "rdm.node_admin.mesh.create.via-recovery").into_iter().find(|r| attr(r, "mesh") == "mesh2" && attr(r, "node").starts_with("mesh1.")).unwrap_or_else(|| panic!("the node.create of mesh2's node-admin opened (via-recovery)")).clone();
    let probes: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-probe").into_iter().filter(|p| attr(p, "mesh") == "mesh2" && attr(p, "node") == attr(&create, "node") && at(p) > killed_ns && at(p) < at(&create)).collect();
    let last = probes.iter().max_by_key(|p| at(p)).unwrap_or_else(|| panic!("mesh2 was probed by the fabric-primary before its verdict"));
    assert_eq!(attr(last, "outcome"), "carrier-edge-lost", "the latest probe: a member of mesh2 cannot reach its node-admins: {probes:#?}");
    assert!(kept.contains(&attr(last, "carrier")), "the carrier is a member that stayed alive: {last}");
    assert!(
        named(&spans, "rdm.node_admin.mesh.update.via-probe-verdict").iter().any(|v| attr(v, "mesh") == "mesh2" && attr(v, "outcome") == "carrier-edge-lost" && at(v) <= at(&create)),
        "the verdict was recorded before the node.create attempt opened"
    );
    // The target held: no node of mesh2 was created by B before the verdict.
    let early: Vec<String> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .chain(named(&spans, "rdm.node_admin.node.update.via-build"))
        .filter(|c| attr(c, "build_id") == b && attr(c, "node").starts_with("mesh2.") && at(c) > killed_ns && at(c) < at(&create))
        .map(|c| format!("{} attempt {} at {}", attr(c, "node"), attr(c, "attempt"), at(c)))
        .collect();
    assert!(early.is_empty(), "nothing of mesh2 is created before its verdict (via-recovery at {}): {early:?}", at(&create));
    let accepting = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("a fabric-primary before the loss");
    let takeover = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|r| {
            let a = &r["attributes"];
            a["build_id"] == b.as_str() && a["previous_executor"] == mesh2_primary.as_str() && a["executor"].as_str().is_some_and(|e| e.starts_with("mesh1."))
        })
        .unwrap_or_else(|| panic!("no takeover of {b} by a mesh1 admin"))
        .clone();
    let attempt = takeover["attributes"]["attempt"].as_str().map(String::from).unwrap_or_else(|| takeover["attributes"]["attempt"].to_string());
    let decider = claim_decider(&spans, b.as_str(), &attempt).unwrap_or_else(|| panic!("the claim of takeover attempt {attempt} of {b} has its decision span"));
    if decider == accepting {
        let accepted = named(&spans, "rdm.node_admin.build.create.via-accepted").into_iter().find(|sp| sp["attributes"]["build_id"] == b.as_str()).expect("B's request").clone();
        assert_eq!(takeover["trace_id"], accepted["trace_id"], "a takeover the accepting fabric-primary decided shares B's request trace");
    } else {
        assert_eq!(takeover["parent_span_id"].as_str().unwrap_or(""), "", "a takeover decided after the role moved starts its own trace: {takeover}");
    }
    // Each created admin held every surviving member's runtime before it
    // committed Ready (i143.e4.s16): mesh1's five births at least.
    for admin in after.iter().filter(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin") {
        let ready = named(&spans, "rdm.mesh.node.update.via-ready")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == admin["name"] && sp["attributes"]["incarnation_id"] == admin["incarnation_id"])
            .unwrap_or_else(|| panic!("{} reports ready", admin["name"]))
            .clone();
        let held = ready["attributes"]["runtime_facts_held"].as_u64().or_else(|| ready["attributes"]["runtime_facts_held"].as_str().and_then(|v| v.parse().ok())).unwrap_or(0);
        assert!(held >= 5, "{} held {held} runtime facts before Ready: {ready}", admin["name"]);
    }
    eprintln!("admins_lost_mid_build: node-admin create trace {} | latest probe trace {} | takeover trace {}", s(&create["trace_id"]), s(&last["trace_id"]), s(&takeover["trace_id"]));
    estate.record_trace_url(takeover["trace_id"].as_str().unwrap_or(""));
}
