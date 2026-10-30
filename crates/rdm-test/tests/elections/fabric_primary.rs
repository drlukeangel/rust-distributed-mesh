//! i143.e4.s5 process E2E: fabric-primary election and the advertised
//! control endpoint (PRD §1.14, §1.16, §11).
//!
//! The fabric-primary role stays with its primary (ruling R-A2): a lower NodeId never displaces a living
//! primary, and a fabric's first election is filled by the lowest Ready NodeId, which on Day 0 is
//! the Day-0 admin alone. The role leaves the primary's mesh only when every node-admin birth of
//! that mesh is gone; the lowest NodeId among the remaining mesh primaries then wins.
//!
//! An MM fabric loses its fabric-primary mesh (every process of the mesh
//! that holds the fabric killed). From public surfaces only:
//! - before the loss, `fabric.get` names one fabric primary, the one
//!   computed from the advertised NodeIds, and every mesh advertises its
//!   control API;
//! - after it, the test finds control again through the meshes' primary
//!   admins the fabric view named (no hidden map): the surviving mesh's admin answers, its
//!   fabric view names one new fabric primary (the surviving mesh's primary)
//!   and advertises that admin's control API;
//! - control has moved: that admin accepts a change and drives it to completion, the lost mesh
//!   made whole again on the way, and the fabric then shuts down from the new primary; no runtime
//!   of the estate is left running.
//!
//! A three-mesh fabric with shuffled names keeps the Day-0 primary whatever the other mesh
//! primaries' NodeIds are; every mesh primary reports the same primary; losing every node-admin
//! of the primary's mesh moves the role to the lowest NodeId among the remaining mesh primaries.
//!
//! Evidence: the new fabric primary's election is announced as
//! `rdm.mesh.election.resolve.via-fabric-recompute` (`election_level =
//! fabric_primary`, the winner's NodeId, path and mesh) by a surviving mesh
//! primary. The lost mesh is fully crashed (mesh-investigation.md step 5, "Fully crashed"): no member
//! of it answers either probe of the new primary's peer-mesh investigation, so its
//! `rdm.node_admin.mesh.update.via-probe-verdict` is `outcome = unreachable` and its drive stops at
//! `rdm.node_admin.build.reject.via-unheard-mesh-drive` (`recovery_decided = false`) until then; on that
//! decision (mesh-investigation.md step 5, "No member answers") the new fabric primary runs the create
//! of that mesh's first node-admin in process, and nothing else of that mesh.

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, NodeStatus, PathName};
use rdm_test::elections::{advertised_fabric_primaries, primaries_as_expected};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "fabric-primary".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "losing_the_fabric_primary_mesh_moves_control".into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// CONTRACT (Q122; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" step 2,
/// "Outside its mesh"; mesh-investigation.md step 5): every process of the fabric-primary's mesh is
/// killed. No member of it answers mesh investigation's probes, so at the decision step the lowest
/// Ready mesh-primary on the backbone, mesh2's, runs fabric recovery, exactly as a node-admin
/// of the lost mesh would: it takes no primary role, node investigation reads the lost birth (not
/// `reached`), node recovery re-creates the node-admin at mesh1.admin.1, and when that node is
/// Ready the workflow restores it as mesh-primary and fabric-primary. The restored mesh-primary
/// recovers the rest of its mesh, and the fabric accepts and completes a change from it. Must NOT happen:
/// mesh2's primary, or any other node, named fabric-primary while the workflow runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_fabric_primarys_whole_mesh_is_fabric_recovery_run_from_the_backbone() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    estate.settles_to(&[("mesh1", 2, 3), ("mesh2", 2, 3)], Duration::from_secs(30)).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let nodes = estate.node_values().await;
    let fp = "mesh1.admin.1";
    assert_eq!(nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).as_deref(), Some(fp), "the Day-0 admin is fabric-primary: {nodes:#?}");
    let old_node_id = s(&nodes.iter().find(|n| n["name"] == fp).unwrap()["node_id"]);
    let backbone = s(&nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).expect("mesh2 has a primary")["name"]);
    estate.enter(&backbone.parse().unwrap()).await;

    // Lose the fabric-primary's mesh: every one of its processes.
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == "mesh1" && n["name"] != fp) {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    for pid in pids {
        estate.kill_pid(pid as u32);
    }
    estate.kill_bootstrap();
    let killed_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // Nobody but the node at mesh1.admin.1 is ever fabric-primary; the workflow restores a new birth there.
    let floor = rdm_transport::topology::staleness_floor();
    estate
        .until_view("the node re-created at mesh1.admin.1 is restored as mesh-primary and fabric-primary", floor * 20 + Duration::from_secs(120), |views| {
            let ns = Estate::values(views);
            let primaries: Vec<String> = ns.iter().filter(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).collect();
            assert!(primaries.iter().all(|h| h == fp), "only the node at {fp} is ever fabric-primary: {primaries:?}");
            ns.iter().find(|n| n["name"] == fp && s(&n["node_id"]) != old_node_id && n["status"] == "ready" && n["is_fabric_primary"] == true && n["is_primary"] == true).map(|_| ())
        })
        .await;
    // The restored mesh-primary recovers the rest of its mesh; the fabric is ready again.
    estate.settles_to(&[("mesh1", 2, 3), ("mesh2", 2, 3)], Duration::from_secs(120)).await;
    estate.enter(&fp.parse().unwrap()).await;
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    // Control is back at the restored fabric-primary: it accepts a change and completes it.
    let mut spawn = estate.create_node("mesh2", NodeKind::RpcNode).await.unwrap_or_else(|e| panic!("the restored fabric-primary refused node.create of an rpc node in mesh2: {e}"));
    Estate::complete(&mut spawn).await;
    estate.settles_to(&[("mesh1", 2, 3), ("mesh2", 2, 4)], Duration::from_secs(60)).await;

    estate.stop().await;
    assert_eq!(estate.live_runtimes(), vec![], "no runtime of the estate is left running");
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    // No member answered: mesh2's primary decided the lost mesh unreachable.
    let verdict = named(&spans, "rdm.node_admin.mesh.update.via-probe-verdict")
        .into_iter()
        .find(|sp| attr(sp, "node") == backbone && attr(sp, "mesh") == "mesh1" && attr(sp, "outcome") == "unreachable" && at(sp) >= killed_ns)
        .cloned()
        .expect("mesh2's primary decided the lost mesh unreachable");
    // At that decision it ran fabric recovery from the backbone and restored the node at mesh1.admin.1.
    let ran: Vec<Value> = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| at(sp) >= killed_ns).cloned().collect();
    let run = ran.iter().find(|sp| attr(sp, "node") == backbone && attr(sp, "arm") == "backbone" && attr(sp, "outcome").starts_with("restored")).unwrap_or_else(|| panic!("{backbone} ran fabric recovery from the backbone and restored {fp}: {ran:#?}"));
    assert!(at(run) >= at(&verdict), "fabric recovery started at the decision: {run} {verdict}");
    assert!(ran.iter().all(|sp| attr(sp, "node") == backbone), "nobody else ran it: {ran:#?}");
    let restore = named(&spans, "rdm.node_admin.primary.update.via-fabric-recovery").into_iter().find(|sp| attr(sp, "node") == backbone && attr(sp, "to") == fp && attr(sp, "outcome") == "restored").cloned();
    assert!(restore.is_some(), "the workflow's call restored the node at {fp}");
    // mesh2's primary never took a primary role beyond its own mesh's.
    let recomputes = named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute");
    assert!(!recomputes.iter().any(|sp| at(sp) >= killed_ns && attr(sp, "winner_path") == backbone), "{backbone} was never elected fabric-primary");
}

/// The positive arm: a member whose exact runtime the provider inspects as `Exited` is
/// canonically dead, so the same desired topology restores the count with a new birth at the
/// lowest free path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_whose_runtime_exited_is_replaced() {
    let mut estate = Estate::bootstrap(
        Owner { subfeature: "path-target".into(), rung: "MN".into(), test: "a_member_whose_runtime_exited_is_replaced".into(), ..owner() },
        "fabric1",
        "mesh1",
    )
    .await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    let gone = estate.until_node("mesh1.rpc.2", "this admin hears mesh1.rpc.2", Duration::from_secs(30), |n| n.cloned()).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.kill_node("mesh1.rpc.2").await;
    estate
        .until_view("a new birth restores two ready rpc nodes", rdm_transport::topology::staleness_floor() + Duration::from_secs(60), |views| {
            let ready: Vec<_> = views.iter().filter(|n| n.kind == NodeKind::RpcNode && n.status == NodeStatus::Ready).collect();
            (ready.len() == 2 && !ready.iter().any(|n| n.incarnation_id == gone.incarnation_id)).then_some(())
        })
        .await;
    estate.stop().await;
}

/// CONTRACT: three meshes with shuffled names. The fabric primary is the Day-0 admin, even when
/// another mesh primary has a lower NodeId (R-A2: a lower NodeId never displaces a living
/// primary); every mesh primary's own view advertises that same primary. Losing every node-admin of
/// the primary's mesh moves the role to the lowest NodeId among the remaining mesh primaries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_mesh_primaries_keep_the_day0_fabric_primary_and_elect_the_lowest_remaining_when_its_mesh_is_lost() {
    let mut estate = Estate::bootstrap(
        Owner { subfeature: "fabric-primary".into(), rung: "MMM".into(), test: "three_mesh_primaries_keep_the_day0_fabric_primary".into(), ..owner() },
        "fabric1",
        "mesh1",
    )
    .await;
    let desired = FabricDesired { fabric: "fabric1".into(), meshes: ["mesh3", "mesh1", "mesh2"].map(|m| MeshDesired::of(m, [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)])).to_vec() };
    estate.reconcile(&desired).await;
    let all_ready = |nodes: &[Value]| nodes.len() == 9 && nodes.iter().all(|n| n["status"] == "ready");
    let nodes = estate
        .until_view("three meshes settle on the computed roles", Duration::from_secs(30), |views| {
            let nodes = Estate::values(views);
            (all_ready(&nodes) && primaries_as_expected(&nodes).is_ok()).then_some(nodes)
        })
        .await;
    let winner = "mesh1.admin.1".to_string();
    assert_eq!(advertised_fabric_primaries(&nodes), vec![winner.clone()], "the Day-0 admin holds the fabric-primary role: {nodes:#?}");
    let winner_node = nodes.iter().find(|n| n["name"] == winner.as_str()).unwrap().clone();
    // Every mesh primary's own view advertises the same primary.
    let mesh_primaries: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "node_admin" && n["is_primary"] == true).cloned().collect();
    assert_eq!(mesh_primaries.len(), 3);
    for mp in &mesh_primaries {
        let view = estate
            .until_view_at(&Estate::path(&s(&mp["name"])), &format!("{} agrees", mp["name"]), Duration::from_secs(30), |views| {
                let v = Estate::values(views);
                (all_ready(&v) && primaries_as_expected(&v).is_ok()).then_some(v)
            })
            .await;
        assert_eq!(advertised_fabric_primaries(&view), vec![winner.clone()], "{} resolves the same fabric primary", mp["name"]);
    }

    // Delete the primary's mesh (`mesh.delete` runs `node.fabric-primary.update` first and takes
    // `mesh1` down only after the confirmed transfer): the role leaves it for the lowest remaining mesh primary.
    let survivors: Vec<&Value> = mesh_primaries.iter().filter(|m| m["mesh"] != "mesh1").collect();
    let survivor_admin = Estate::path(&s(&survivors[0]["name"]));
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter(&survivor_admin).await;
    estate.reconcile_shape("fabric1", &[("mesh3", 2, 1), ("mesh2", 2, 1)]).await;
    estate.kill_bootstrap();
    let next_id = survivors.iter().map(|m| s(&m["node_id"])).min().unwrap();
    let winner_id = s(&winner_node["node_id"]);
    let after = estate
        .until_view_at(&survivor_admin, "the fabric-primary role moves to the lowest remaining mesh primary", Duration::from_secs(30), |views| {
            let v = Estate::values(views);
            let fp: Vec<&Value> = v.iter().filter(|n| n["is_fabric_primary"] == true).collect();
            (fp.len() == 1 && s(&fp[0]["node_id"]) != winner_id).then(|| v.clone())
        })
        .await;
    let primary: Vec<&Value> = after.iter().filter(|n| n["is_fabric_primary"] == true).collect();
    assert_eq!(s(&primary[0]["node_id"]), next_id, "the lowest NodeId among the remaining mesh primaries: {survivors:#?}");

    estate.stop().await;
    let spans = estate.spans();
    // Each mesh primary that observed the first resolution reported the same winner.
    let first: Vec<&Value> = named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute")
        .into_iter()
        .filter(|sp| sp["attributes"]["winner_node_id"] == winner_node["node_id"])
        .collect();
    let observers: std::collections::BTreeSet<String> = first.iter().map(|sp| s(&sp["attributes"]["observer"])).collect();
    assert!(observers.len() >= 2, "more than one mesh primary resolved the same winner: {observers:?}");
    assert!(first.iter().all(|sp| sp["attributes"]["election_level"] == "fabric_primary" && sp["attributes"]["election_key"] == "node_id_crockford"));
    // The role is vacant while the primary's mesh is only silent (`winner_node_id` empty, previous
    // the primary) and filled by the lowest remaining mesh primary once every admin of that mesh is gone.
    let recomputes = named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute");
    assert!(recomputes.iter().any(|sp| sp["attributes"]["winner_node_id"] == next_id.as_str()), "the lowest remaining mesh primary {next_id} was announced as the winner");
    assert!(
        recomputes.iter().any(|sp| sp["attributes"]["previous_node_id"] == winner_node["node_id"] && sp["attributes"]["winner_node_id"] != winner_node["node_id"]),
        "the role left the primary {winner_id} only after its mesh was gone"
    );
}
