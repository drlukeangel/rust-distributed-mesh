//! i143.e4.s5 process E2E: fabric-primary election and the advertised
//! control endpoint (PRD §1.14, §1.16, §11).
//!
//! The fabric seat stays with its holder (ruling R-A2): a lower NodeId never displaces a living
//! holder, and a fabric's first election is filled by the lowest Ready NodeId, which on Day 0 is
//! the Day-0 admin alone. The seat leaves the holder's mesh only when every node-admin birth of
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
//! A three-mesh fabric with shuffled names keeps the Day-0 holder whatever the other mesh
//! primaries' NodeIds are; every mesh primary reports the same holder; losing every node-admin
//! of the holder's mesh moves the seat to the lowest NodeId among the remaining mesh primaries.
//!
//! Evidence: the new fabric primary's election is announced as
//! `rdm.mesh.election.resolve.via-fabric-recompute` (`election_level =
//! fabric_primary`, the winner's NodeId, path and mesh) by a surviving mesh
//! primary. The lost mesh is fully crashed (mesh-investigation.md step 5, "Fully crashed"): no member
//! of it answers either probe of the new primary's peer-mesh investigation, so its
//! `rdm.node_admin.mesh.update.via-probe-verdict` is `outcome = unreachable` and its drive stops at
//! `rdm.node_admin.build.reject.via-unheard-mesh-drive` (`recovery_decided = false`) until then; the
//! provider's exit proof of every node-admin of the lost mesh is
//! `rdm.node_admin.mesh.update.via-crash-proof` (`outcome = proven`), on which the new fabric primary
//! runs the create of that mesh's first node-admin in process, and nothing else of that mesh.

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired, NodeSpec, NodeStatus, PathName};
use rafka_test_scenario::elections::{advertised_fabric_primaries, seats_as_expected};
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::Value;
use std::process::Command;
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

fn kill(pid: u64) {
    assert!(Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success(), "kill -9 {pid}");
}

/// CONTRACT (R-A2; R-D1: loss of peer-mesh control-plane visibility fences lifecycle mutation for
/// every node in that mesh; mesh-investigation.md step 5, "Fully crashed": "Every node of mesh-x, its
/// node-admins included, has crashed while the fabric is alive. No member answers, so
/// carrier-edge-lost cannot arise. The fabric-primary acts on the provider's exact exit proof, never
/// on silence, and creates mesh-x's node-admin in process ..., on the same accepted Build and mesh-x's
/// own MeshId"): every process of the fabric-primary's mesh is killed. Control moves to the surviving
/// mesh's primary, found through the advertised endpoints alone; it accepts a change and drives it.
/// No member of the lost mesh answers: both probes and the verdict are `unreachable`, and the drive
/// stops at the fence on the lost mesh's first node-admin until that decision. The provider then
/// proves every node-admin of the lost mesh exited, and on that proof the new fabric primary runs the
/// create of the lost mesh's first node-admin in process, on the change's Build; the lost mesh's new
/// primary makes the rest of it, and the change completes. Must NOT happen: a carrier-edge-lost
/// recovery of a mesh no member of which answers, the create before the provider's proof, or any
/// other node of the lost mesh made by the new fabric primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn losing_the_fabric_primary_mesh_moves_control() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3), ("mesh2", 2, 3)]).await;
    // The Build may complete on another admin: this view hears every birth first.
    estate.settles_to(&[("mesh1", 2, 3), ("mesh2", 2, 3)], Duration::from_secs(30)).await;

    // The advertised topology: one fabric primary, every mesh's control API.
    let fabric = estate.fabric_view().await;
    let old_primary = fabric.fabric_primary.clone().expect("the fabric names its primary").to_string();
    let nodes = estate.node_values().await;
    assert_eq!(old_primary, "mesh1.admin.1", "the Day-0 admin keeps the seat whatever mesh2's primary NodeId is (R-A2): {nodes:#?}");
    let old_base = s(&nodes.iter().find(|n| n["name"] == old_primary.as_str()).unwrap()["admin_api_base"]);
    assert_eq!(fabric.admin_api_base.as_deref(), Some(old_base.as_str()), "the fabric advertises its primary's control API");
    let lost = old_primary.split('.').next().unwrap().to_string();
    let survivor = if lost == "mesh1" { "mesh2" } else { "mesh1" }.to_string();
    let advertised: Vec<PathName> = fabric.meshes.iter().filter_map(|m| m.primary_admin.clone()).collect();
    assert!(fabric.meshes.iter().all(|m| m.admin_api_base.as_deref().is_some_and(|b| !b.is_empty())), "every mesh names its application contact: {fabric:#?}");

    // Lose the fabric primary's mesh: every one of its processes.
    // Every pid first: an admin's view goes with it.
    let mut pids = Vec::new();
    for n in nodes.iter().filter(|n| n["mesh"] == lost.as_str() && n["name"] != "mesh1.admin.1") {
        pids.push(estate.pid_of(&s(&n["name"])).await);
    }
    for pid in pids {
        kill(pid);
    }
    if lost == "mesh1" {
        estate.kill_bootstrap();
    }
    let killed_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;

    // Find control again through the mesh primaries the fabric named: ask each
    // for the fabric, and take the primary it names once that primary answers
    // for the fabric too and agrees.
    let control = wait_for("an advertised admin leads to a live fabric primary", (rdm_transport::membership::staleness_floor() * 2 + rdm_transport::membership::backbone_gossip_interval() * 2) + Duration::from_secs(30), || async {
        for admin in &advertised {
            // A killed admin's port is soon another estate's: only this Fabric's admins count.
            let Some(f) = estate.fabric_view_at(admin).await else { continue };
            let Some(primary) = f.fabric_primary.clone().filter(|p| p.to_string() != old_primary) else { continue };
            let Some(own) = estate.fabric_view_at(&primary).await else { continue };
            if own.fabric_primary.as_ref() == Some(&primary) {
                return Some((primary, own));
            }
        }
        None
    })
    .await;
    let (new_primary_path, fabric) = control;
    let new_primary = new_primary_path.to_string();
    estate.enter(&new_primary_path).await;
    // The surviving mesh's view settles: every one of its members ready again.
    // Losing half the fabric costs gossip a failure-detection window (the
    // dead peers' connections time out) before mesh2's members are heard.
    estate
        .until_view("the surviving mesh settles in the new primary's view", Duration::from_secs(90), |views| {
            let alive: Vec<_> = views.iter().filter(|n| n.mesh == survivor).collect();
            (alive.len() == 5 && alive.iter().all(|n| n.status == NodeStatus::ReadyForTraffic)).then_some(())
        })
        .await;
    let nodes = estate.node_values().await;
    let holder = nodes.iter().find(|n| n["name"] == new_primary.as_str()).unwrap().clone();
    assert_eq!(holder["is_primary"], true, "the fabric primary is its own mesh's admin primary");
    assert_eq!(nodes.iter().filter(|n| n["is_fabric_primary"] == true).count(), 1, "exactly one fabric primary");
    assert_eq!(new_primary.split('.').next(), Some(survivor.as_str()), "the surviving mesh's primary holds the fabric");
    let held = fabric.meshes.iter().find(|m| m.name == survivor).unwrap();
    assert_eq!(held.admin_api_base, fabric.admin_api_base, "the surviving mesh advertises its live owning admin");

    // Control moved: the new primary accepts a change and drives it. What is left of the accepted
    // topology begins with the lost mesh's first node-admin, of which no member answers either probe:
    // the drive stops at the fence until the decision, then runs that create on the provider's proof,
    // and the lost mesh's own new primary makes the rest of it.
    let mut spawn = estate.nodes_obj().await.create(&NodeSpec { mesh: survivor.clone(), kind: NodeKind::RpcNode }).await.unwrap_or_else(|e| panic!("the new fabric-primary refused node.create of an rpc node in {survivor}: {e}"));
    let change = spawn.accepted().build_id.to_string();
    Estate::complete(&mut spawn).await;
    estate.settles_to(&[(lost.as_str(), 2, 3), (survivor.as_str(), 2, 4)], Duration::from_secs(60)).await;

    // And the whole fabric stops from the new primary.
    estate.stop().await;
    assert_eq!(estate.live_runtimes(), vec![], "no runtime of the estate is left running");
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    // No member answered: the verdict and every probe of the investigation that reached it are
    // unreachable, and no carrier-edge-lost recovery opened. (The fabric's own stop later makes the
    // recovered mesh unheard again; that investigation is the stop's.)
    let verdict = named(&spans, "rdm.node_admin.mesh.update.via-probe-verdict")
        .into_iter()
        .find(|sp| attr(sp, "node") == new_primary && attr(sp, "mesh") == lost && attr(sp, "outcome") == "unreachable")
        .cloned()
        .expect("the new primary's investigation decided the lost mesh unreachable");
    let probes: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-probe").into_iter().filter(|p| attr(p, "node") == new_primary && attr(p, "mesh") == lost && at(p) < at(&verdict)).collect();
    assert!(!probes.is_empty() && probes.iter().all(|p| attr(p, "outcome") == "unreachable"), "every probe of the lost mesh is unreachable: {probes:#?}");
    let rebirths: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.create.via-recovery").into_iter().filter(|r| attr(r, "mesh") == lost && attr(r, "verdict") == "carrier-edge-lost").collect();
    assert!(rebirths.is_empty(), "no carrier-edge-lost recovery of a mesh no member of which answers: {rebirths:#?}");
    // Until the decision the drive stopped at the fence on the lost mesh's first node-admin.
    let fence = named(&spans, "rdm.node_admin.build.reject.via-unheard-mesh-drive")
        .into_iter()
        .find(|sp| attr(sp, "build_id") == change && attr(sp, "node") == new_primary && attr(sp, "mesh") == lost)
        .cloned()
        .expect("the drive stopped at the fence on the lost mesh");
    assert_eq!((attr(&fence, "recovery_decided").as_str(), attr(&fence, "target")), ("false", format!("{lost}.admin.1")), "the fence held the lost mesh's first node-admin: {fence}");
    assert!(at(&fence) < at(&verdict), "the fence stood before the decision: {fence} {verdict}");
    // The provider proved every node of the lost mesh exited ("Every node of mesh-x, its node-admins included,
    // has crashed"), read by the drive of the change.
    let proof = named(&spans, "rdm.node_admin.mesh.update.via-crash-proof")
        .into_iter()
        .find(|sp| attr(sp, "node") == new_primary && attr(sp, "mesh") == lost && attr(sp, "build_id") == change && attr(sp, "outcome") == "proven")
        .cloned()
        .expect("the provider's exit proof of every node of the lost mesh was read");
    assert_eq!(
        (attr(&proof, "proven"), attr(&proof, "unproven")),
        (format!("{lost}.admin.1,{lost}.admin.2,{lost}.rpc.1,{lost}.rpc.2,{lost}.rpc.3"), String::new()),
        "every node of the lost mesh proven exited, its node-admins included: {proof}"
    );
    // On that proof the new fabric primary ran the create of the lost mesh's first node-admin in
    // process; every other node of the lost mesh was made by the lost mesh's new primary.
    let executor_of = |attempt: &str| {
        named(&spans, "rdm.node_admin.build.update.via-claim-decision")
            .into_iter()
            .find(|sp| attr(sp, "build_id") == change && attr(sp, "attempt") == attempt && attr(sp, "outcome") == "won")
            .map(|sp| attr(sp, "executor"))
            .unwrap_or_default()
    };
    let creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build").into_iter().filter(|sp| attr(sp, "build_id") == change && attr(sp, "node").starts_with(&format!("{lost}.")) && at(sp) > killed_ns).collect();
    let first_admin = format!("{lost}.admin.1");
    let first = creates.iter().find(|sp| attr(sp, "node") == first_admin).unwrap_or_else(|| panic!("the lost mesh's first node-admin was created: {creates:#?}"));
    assert_eq!(executor_of(&attr(first, "attempt")), new_primary, "the new fabric primary ran the first node-admin's create in process: {first}");
    assert!(at(first) > at(&proof), "the create followed the provider's proof: {first} {proof}");
    let others: Vec<(String, String)> = creates.iter().filter(|sp| attr(sp, "node") != first_admin).map(|sp| (attr(sp, "node"), executor_of(&attr(sp, "attempt")))).collect();
    assert!(!others.is_empty() && others.iter().all(|(_, executor)| *executor == first_admin), "the rest of the lost mesh was made by its new primary {first_admin}: {others:?}");
    // R-DG1 (mesh-investigation.md steps 5-7): when it gave the lost mesh its node-admin the new fabric primary authored
    // the fabric degraded, and it cleared it on the report of the lost mesh's new primary, after that.
    let opening = named(&spans, "rdm.node_admin.mesh.create.via-recovery")
        .into_iter()
        .find(|sp| attr(sp, "node") == new_primary && attr(sp, "mesh") == lost && attr(sp, "verdict") == "fully-crashed")
        .cloned()
        .unwrap_or_else(|| panic!("the new fabric primary authored degraded when it claimed the create of {first_admin} on the proof"));
    assert_eq!(attr(&opening, "fabric_status"), "degraded", "the claim authored the fabric degraded: {opening}");
    assert!(at(&opening) < at(first), "degraded was authored when the create was claimed, before the node-admin was made: {opening} {first}");
    let recovered = named(&spans, "rdm.node_admin.mesh.update.via-recovered")
        .into_iter()
        .find(|sp| attr(sp, "node") == new_primary && attr(sp, "mesh") == lost)
        .cloned()
        .unwrap_or_else(|| panic!("the lost mesh's new primary reported: the fabric is ready again"));
    assert!(at(&recovered) > at(&opening), "the clear followed the opening: {recovered} {opening}");
    // The new holder is announced by a surviving admin: a vacancy first (the lost mesh's holder is
    // silent, then proven gone), then the lowest remaining mesh primary takes the seat.
    let recomputes = named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute");
    assert!(
        recomputes.iter().any(|sp| {
            let a = &sp["attributes"];
            a["election_level"] == "fabric_primary"
                && a["winner_path"] == new_primary.as_str()
                && a["winner_node_id"] == holder["node_id"]
                && a["winner_mesh"] == survivor.as_str()
                && a["election_key"] == "node_id_crockford"
                && a["observer"] == new_primary.as_str()
        }),
        "a surviving admin announced the new fabric primary"
    );
    assert!(
        recomputes.iter().any(|sp| sp["attributes"]["previous"].as_str().is_some_and(|p| p.starts_with(&format!("{lost}.admin.")))),
        "the lost mesh's holder was announced gone before the seat moved"
    );
    eprintln!("fully crashed: verdict trace {} | fence trace {} | proof trace {} | create trace {}", s(&verdict["trace_id"]), s(&fence["trace_id"]), s(&proof["trace_id"]), s(&first["trace_id"]));
    estate.record_trace_url(&s(&proof["trace_id"]));
}

/// A member that is unheard (frozen: its process runs but says nothing) is dead in the view, but
/// silence is never death proof (Luke 2026-10-05): the desired Build resubmitted holds it. No new
/// birth takes the path, nothing is terminated, and the fence names the hold (`held-running`).
/// Must NOT happen: a replacement birth at the path, or `via-fence outcome=terminated`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_silent_member_whose_runtime_still_runs_is_held_not_replaced() {
    let mut estate = Estate::bootstrap(
        Owner { subfeature: "path-fence".into(), rung: "MN".into(), test: "a_silent_member_whose_runtime_still_runs_is_held".into(), ..owner() },
        "fabric1",
        "mesh1",
    )
    .await;
    let desired = FabricDesired { fabric: "fabric1".into(), meshes: vec![MeshDesired::of("mesh1", [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])] };
    estate.reconcile(&desired).await;
    let frozen = estate.until_node("mesh1.rpc.2", "this admin hears mesh1.rpc.2", Duration::from_secs(30), |n| n.cloned()).await;
    let pid = estate.pid_of("mesh1.rpc.2").await;
    assert!(Command::new("kill").args(["-STOP", &pid.to_string()]).status().unwrap().success());
    estate
        .until_node("mesh1.rpc.2", "the frozen member is unheard in the view", rdm_transport::membership::staleness_floor() + Duration::from_secs(30), |n| n.filter(|n| matches!(n.status, NodeStatus::Dead | NodeStatus::PendingReconnect)).map(|_| ()))
        .await;

    estate.reconcile(&desired).await;
    let at_path: Vec<_> = estate.live_runtimes().into_iter().filter(|(d, _)| d.file_name().unwrap().to_string_lossy().starts_with("mesh1.rpc.2-")).collect();
    assert_eq!(at_path.len(), 1, "one runtime at the path, the frozen one: {at_path:?}");
    assert_eq!(u64::from(at_path[0].1), pid, "the frozen predecessor is held, never terminated");
    assert_eq!(estate.node_view("mesh1.rpc.2").await.incarnation_id, frozen.incarnation_id, "no new birth at the path");

    assert!(Command::new("kill").args(["-CONT", &pid.to_string()]).status().unwrap().success());
    estate.stop().await;
    let spans = estate.spans();
    let fences: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.delete.via-fence").into_iter().filter(|sp| sp["attributes"]["node"] == "mesh1.rpc.2").collect();
    assert!(fences.iter().any(|sp| sp["attributes"]["outcome"] == "held-running"), "the hold is in the evidence: {fences:?}");
    assert!(!fences.iter().any(|sp| sp["attributes"]["outcome"] == "terminated"), "nothing was terminated on silence: {fences:?}");
}

/// The positive arm: a member whose exact runtime the provider inspects as `Exited` is
/// canonically dead, so the same desired topology restores the count with a new birth at the
/// lowest free path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_whose_runtime_exited_is_replaced() {
    let mut estate = Estate::bootstrap(
        Owner { subfeature: "path-fence".into(), rung: "MN".into(), test: "a_member_whose_runtime_exited_is_replaced".into(), ..owner() },
        "fabric1",
        "mesh1",
    )
    .await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    let gone = estate.until_node("mesh1.rpc.2", "this admin hears mesh1.rpc.2", Duration::from_secs(30), |n| n.cloned()).await;
    kill(estate.pid_of("mesh1.rpc.2").await);
    estate
        .until_view("a new birth restores two ready rpc nodes", rdm_transport::membership::staleness_floor() + Duration::from_secs(60), |views| {
            let ready: Vec<_> = views.iter().filter(|n| n.kind == NodeKind::RpcNode && n.status == NodeStatus::ReadyForTraffic).collect();
            (ready.len() == 2 && !ready.iter().any(|n| n.incarnation_id == gone.incarnation_id)).then_some(())
        })
        .await;
    estate.stop().await;
}

/// CONTRACT: three meshes with shuffled names. The fabric primary is the Day-0 admin, even when
/// another mesh primary has a lower NodeId (R-A2: a lower NodeId never displaces a living
/// holder); every mesh primary's own view advertises that same holder. Losing every node-admin of
/// the holder's mesh moves the seat to the lowest NodeId among the remaining mesh primaries.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_mesh_primaries_keep_the_day0_holder_and_elect_the_lowest_remaining_when_its_mesh_is_lost() {
    let mut estate = Estate::bootstrap(
        Owner { subfeature: "fabric-primary".into(), rung: "MMM".into(), test: "three_mesh_primaries_keep_the_day0_holder".into(), ..owner() },
        "fabric1",
        "mesh1",
    )
    .await;
    let desired = FabricDesired { fabric: "fabric1".into(), meshes: ["mesh3", "mesh1", "mesh2"].map(|m| MeshDesired::of(m, [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 1)])).to_vec() };
    estate.reconcile(&desired).await;
    let all_ready = |nodes: &[Value]| nodes.len() == 9 && nodes.iter().all(|n| n["status"] == "ready-for-traffic");
    let nodes = estate
        .until_view("three meshes settle on the computed seats", Duration::from_secs(30), |views| {
            let nodes = Estate::values(views);
            (all_ready(&nodes) && seats_as_expected(&nodes).is_ok()).then_some(nodes)
        })
        .await;
    let winner = "mesh1.admin.1".to_string();
    assert_eq!(advertised_fabric_primaries(&nodes), vec![winner.clone()], "the Day-0 admin holds the fabric seat: {nodes:#?}");
    let winner_node = nodes.iter().find(|n| n["name"] == winner.as_str()).unwrap().clone();
    // Every mesh primary's own view advertises the same holder.
    let mesh_primaries: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "node_admin" && n["is_primary"] == true).cloned().collect();
    assert_eq!(mesh_primaries.len(), 3);
    for mp in &mesh_primaries {
        let view = estate
            .until_view_at(&Estate::path(&s(&mp["name"])), &format!("{} agrees", mp["name"]), Duration::from_secs(30), |views| {
                let v = Estate::values(views);
                (all_ready(&v) && seats_as_expected(&v).is_ok()).then_some(v)
            })
            .await;
        assert_eq!(advertised_fabric_primaries(&view), vec![winner.clone()], "{} resolves the same fabric primary", mp["name"]);
    }

    // Retire the holder's mesh (`mesh.delete` runs `node.fabric-primary.update` first and takes
    // `mesh1` down only after the confirmed transfer): the seat leaves it for the lowest remaining mesh primary.
    let survivors: Vec<&Value> = mesh_primaries.iter().filter(|m| m["mesh"] != "mesh1").collect();
    let survivor_admin = Estate::path(&s(&survivors[0]["name"]));
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter(&survivor_admin).await;
    estate.reconcile_shape("fabric1", &[("mesh3", 2, 1), ("mesh2", 2, 1)]).await;
    estate.kill_bootstrap();
    let next_id = survivors.iter().map(|m| s(&m["node_id"])).min().unwrap();
    let winner_id = s(&winner_node["node_id"]);
    let after = estate
        .until_view_at(&survivor_admin, "the fabric seat moves to the lowest remaining mesh primary", Duration::from_secs(30), |views| {
            let v = Estate::values(views);
            let fp: Vec<&Value> = v.iter().filter(|n| n["is_fabric_primary"] == true).collect();
            (fp.len() == 1 && s(&fp[0]["node_id"]) != winner_id).then(|| v.clone())
        })
        .await;
    let holder: Vec<&Value> = after.iter().filter(|n| n["is_fabric_primary"] == true).collect();
    assert_eq!(s(&holder[0]["node_id"]), next_id, "the lowest NodeId among the remaining mesh primaries: {survivors:#?}");

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
    // The seat is vacant while the holder's mesh is only silent (`winner_node_id` empty, previous
    // the holder) and filled by the lowest remaining mesh primary once every admin of that mesh is gone.
    let recomputes = named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute");
    assert!(recomputes.iter().any(|sp| sp["attributes"]["winner_node_id"] == next_id.as_str()), "the lowest remaining mesh primary {next_id} was announced as the winner");
    assert!(
        recomputes.iter().any(|sp| sp["attributes"]["previous_node_id"] == winner_node["node_id"] && sp["attributes"]["winner_node_id"] != winner_node["node_id"]),
        "the seat left the holder {winner_id} only after its mesh was gone"
    );
}
