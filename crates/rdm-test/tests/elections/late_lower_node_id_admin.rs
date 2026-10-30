//! i143.e4.s14 process E2E: a lower-NodeId admin born after a Build
//! completed hydrates, leaves the roles with their primary, and manages that Build's births once
//! the primary is lost (rafka-v2#2840 acceptance 3–4; ruling R-A2).
//!
//! Build A grows mesh1 and completes; its history is forgotten. Admins are
//! then born one at a time until one draws a NodeId lower than the incumbent
//! admin's (NodeIds are random, so a birth wins with the incumbent's share of the
//! id space; a loser is removed again). The incumbent is drawn first from the
//! upper 70% of the space (an estate whose bootstrap id is lower is stopped and
//! bootstrapped again), so each birth wins at least 30% of the time. That late
//! admin:
//! - holds the current desired topology and every held birth's runtime
//!   before it commits Ready;
//! - does not displace the living primary, which keeps every role it held;
//! - fills the roles when the primary is lost (the mesh's lowest ready NodeId), and restarts and
//!   deletes births A launched, through its own `node.stop` then `node.start` and `node.delete`, from their published
//!   runtimes, with A's receipts nowhere.

use rdm_client::{CallEnd, LaunchKind as NodeKind, NodeStatus};
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "late-authority".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births".into(),
    }
}

/// The share of the 60-bit id space below `id` (canonical Crockford, most significant first),
/// read from its first two characters.
fn share_below(id: &str) -> f64 {
    const ALPHABET: &str = "0123456789abcdefghjkmnpqrstvwxyz";
    let digit = |c: char| ALPHABET.find(c).unwrap_or(0) as f64;
    let mut chars = id.chars();
    let (a, b) = (chars.next().map_or(0.0, digit), chars.next().map_or(0.0, digit));
    (a * 32.0 + b) / 1024.0
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lower_node_id_admin_born_after_a_build_wins_and_manages_its_births() {
    // An incumbent near the bottom of the id space can never be beaten by a later birth: draw it
    // from the upper 70%.
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    for _ in 0..5 {
        let me = estate.until_node("mesh1.admin.1", "the bootstrap admin lists itself", Duration::from_secs(30), |n| n.cloned()).await;
        if share_below(&me.node_id.to_string()) >= 0.3 {
            break;
        }
        estate.stop().await;
        estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    }
    let incumbent_admin = Estate::path("mesh1.admin.1");
    // Build A: three rpc nodes, launched by the bootstrap admin. The accepted Build is never
    // history: it is forgotten only once a later Build is current.
    let a = estate.reconcile_shape("fabric1", &[("mesh1", 1, 3)]).await.build_id;
    let nodes = estate.settles_to(&[("mesh1", 1, 3)], Duration::from_secs(30)).await;
    let incumbent = nodes.iter().find(|n| n.name == incumbent_admin).cloned().unwrap();
    let a_births: Vec<_> = nodes.iter().filter(|n| n.kind == NodeKind::RpcNode).cloned().collect();
    let refused = estate.builds_obj().await.delete(&a).await;
    assert!(matches!(&refused, Err(CallEnd::Rejected { reason, .. }) if reason == "cannot-delete"), "the accepted Build is never history: {refused:?}");

    // The initial fabric refuses the removal of a node until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // Births until one draws a lower NodeId than the incumbent's.
    let mut late = None;
    for _ in 0..30 {
        Estate::workflow("node.create of a node-admin in mesh1", estate.create_node("mesh1", NodeKind::NodeAdmin).await).await;
        let nodes = estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(30)).await;
        let born = nodes.iter().find(|n| n.kind == NodeKind::NodeAdmin && n.name != incumbent_admin).cloned().unwrap();
        if born.node_id.to_string() < incumbent.node_id.to_string() {
            late = Some(born);
            break;
        }
        Estate::workflow(&format!("node.delete of {}", born.name), estate.nodes_obj().await.delete(&born.name).await).await;
        estate.settles_to(&[("mesh1", 1, 3)], Duration::from_secs(30)).await;
    }
    let late = late.unwrap_or_else(|| {
        panic!(
            "none of 30 births drew a NodeId below the incumbent {} (it holds {:.0}% of the space below it)",
            incumbent.node_id,
            share_below(&incumbent.node_id.to_string()) * 100.0
        )
    });
    let late_name = late.name.clone();

    // A lower NodeId never displaces a living primary: the incumbent keeps every role, seen from
    // both admins, for several gossip intervals: what each admin holds itself (query.md §2), never
    // node.get, which the late admin, not a primary, forwards to its mesh-primary (Q243).
    for _ in 0..12 {
        for admin in [&incumbent_admin, &late_name] {
            let held = estate.held_topology_at(admin).await.unwrap_or_else(|| panic!("{admin} does not answer node.topology.get"));
            for n in [&incumbent, &late] {
                assert!(held.members.iter().any(|d| d.node.node_id == n.node_id), "{admin} holds {}: {held:#?}", n.name);
            }
            let roles = |id: &rdm_core::NodeId| (held.mesh_primary("mesh1") == Some(id), held.fabric_primary() == Some(id));
            assert_eq!(roles(&incumbent.node_id), (true, true), "the incumbent keeps both roles ({admin}): {held:#?}");
            assert_eq!(roles(&late.node_id), (false, false), "the lower NodeId holds none ({admin}): {held:#?}");
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    // A is history on the admin that ran it, and leaves; the late admin never held it.
    estate.builds_obj().await.delete(&a).await.unwrap_or_else(|e| panic!("A leaves history on the admin that ran it: {e}"));
    // The primary yields (`node.stop` runs `node.primary.update` first): the mesh's lowest ready
    // NodeId fills both roles, and the late admin is it.
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter(&late_name).await;
    let mut stop = estate.nodes_obj().await.stop(&incumbent_admin).await.expect("node.stop of incumbent_admin");
    let accepted = stop.accepted().clone();
    if matches!(Estate::frames_of(&mut stop).await.last(), Some(rdm_client::Frame::FabricPrimaryMoved { .. })) {
        estate.resume(&accepted.build_id, accepted.attempt).await;
    }
    estate.kill_bootstrap();
    estate.until_node(&late_name.to_string(), "the late admin fills the mesh and the fabric roles", Duration::from_secs(60), |n| n.filter(|n| n.is_primary && n.is_fabric_primary).map(|_| ())).await;
    let unknown = estate.carrier().await.build(&a).await;
    assert!(matches!(&unknown, Err(CallEnd::Rejected { reason, .. }) if reason == "unknown-build"), "A's history is nowhere: {unknown:?}");

    // It stops and starts one of A's births and deletes the other.
    let (restarted, deleted) = (a_births[0].clone(), a_births[1].clone());
    estate.stop_then_start(&restarted.name).await;
    let after = estate.until_node(&restarted.name.to_string(), "the restarted node is ready again after its restart", Duration::from_secs(30), |n| n.filter(|n| n.status == NodeStatus::Ready && !n.stopped).cloned()).await;
    assert_eq!(after.node_id, restarted.node_id, "a restart keeps the NodeId");
    Estate::workflow(&format!("node.delete of {}", deleted.name), estate.nodes_obj().await.delete(&deleted.name).await).await;
    estate
        .until_view("the deleted birth leaves the view", Duration::from_secs(30), |views| views.iter().all(|n| n.name != deleted.name || n.incarnation_id != deleted.incarnation_id).then_some(()))
        .await;

    estate.stop().await;
    let (late, restarted, deleted) = (serde_json::to_value(&late).unwrap(), serde_json::to_value(&restarted).unwrap(), serde_json::to_value(&deleted).unwrap());
    let late_name = late_name.to_string();
    let spans = estate.spans();
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap();
    // Before Ready: the current Build and every held birth's runtime.
    let ready = named(&spans, "rdm.mesh.node.update.via-ready")
        .into_iter()
        .find(|sp| sp["attributes"]["node"] == late_name.as_str() && sp["attributes"]["incarnation_id"] == late["incarnation_id"])
        .cloned()
        .expect("the late admin is ready");
    let current = ready["attributes"]["build_id"].as_str().unwrap_or_default().to_string();
    assert!(
        !current.is_empty()
            && named(&spans, "rdm.node_admin.build.create.via-accepted")
                .into_iter()
                .any(|sp| sp["attributes"]["node"] == "mesh1.admin.1" && sp["attributes"]["build_id"] == current.as_str() && at(sp) < at(&ready)),
        "it held the current build_id before Ready: {ready}"
    );
    let held = ready["attributes"]["runtime_facts_held"].as_str().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);
    assert!(held >= 4, "it held the incumbent's and A's three births' runtimes before Ready: {held}");
    // The vacancy was filled by the mesh's lowest ready NodeId, at every level.
    assert!(named(&spans, "rdm.mesh.election.resolve.via-recompute")
        .iter()
        .any(|sp| sp["attributes"]["kind"] == "node_admin" && sp["attributes"]["winner_node_id"] == late["node_id"] && sp["attributes"]["election_key"] == "node_id_crockford"));
    assert!(named(&spans, "rdm.mesh.election.resolve.via-fabric-recompute").iter().any(|sp| sp["attributes"]["winner_node_id"] == late["node_id"]));
    // It managed A's births from their published runtimes.
    for birth in [&restarted, &deleted] {
        let adopted = named(&spans, "rdm.node_admin.runtime.update.via-adopt")
            .into_iter()
            .find(|sp| sp["attributes"]["node"] == birth["name"] && sp["attributes"]["incarnation_id"] == birth["incarnation_id"])
            .unwrap_or_else(|| panic!("{} adopted by the late admin", birth["name"]));
        assert_eq!(adopted["attributes"]["adopter"], late_name.as_str());
        assert_eq!(adopted["attributes"]["source"], "self-published-topology");
    }
}
