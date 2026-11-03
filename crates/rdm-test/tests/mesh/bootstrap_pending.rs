//! i143.e4.s11 acceptance (rafka-v2 #2805, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2805-process`, which exports `I143_ACCEPTANCE_DIR` (this
//! cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans land under it, feature `i143-2805`).
//!
//! A real two-mesh birth through the Build rectifier: `build.create` adds mesh2; the fabric
//! primary births mesh2's bootstrap admin, which hydrates the Fabric identity and the current Build, then
//! receives MeshStatus::Pending from the fabric primary, and only then does any of mesh2's own
//! membership get created; election alone supplies mesh2's primary afterwards.

use rdm_core::PathName;

use rdm_client::{NodeStatus, NodeView};
use rdm_test::estate::{descends_from, named, Estate, Owner};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2805".into(),
        subfeature: "pending-handoff".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2805/process").join(cell),
    }
}

const DECLARATION: &str = "rdm.node_admin.status.update.via-declaration";

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn start(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn end(sp: &Value) -> u64 {
    sp["end_unix_nano"].as_u64().unwrap_or(0)
}

/// CONTRACT (#2805): mesh2's bootstrap admin is born, hydrates the Fabric identity and the complete
/// accepted Build (its entry join), and only then receives MeshStatus::Pending from the fabric
/// primary, applied once through its status door without making it the elected primary; no create
/// of mesh2's own membership starts before that Pending is applied; mesh2's primary is supplied by
/// election afterwards; the creation Build's section calls are in its request's trace. What
/// must NOT happen: a member of mesh2 created before Pending, a second Pending applied, or the
/// bootstrap admin taking a role by accepting Pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_admin_receives_pending_before_own_mesh_attempt() {
    pending_contract("bootstrap_admin_receives_pending_before_own_mesh_attempt").await;
}

/// CONTRACT (#2805): the same Pending contract as the PROCESS cell, on a real same-Docker-domain
/// estate: every node runs in a container of the fabric's one Docker domain, and the boundaries
/// hold exactly as on processes. Refused by name unless the estate really runs on containers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_admin_repeats_status_contract_in_container_domain() {
    assert_eq!(std::env::var("MESH_SPAWN_TYPE").as_deref(), Ok("container"), "this cell runs only on the container provider (MESH_SPAWN_TYPE=container); a process run never stands in for it");
    pending_contract("bootstrap_admin_repeats_status_contract_in_container_domain").await;
}

async fn pending_contract(cell: &str) {
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let mesh = |m: &'static str| (m, 2, 2);
    estate.resize_shape("fabric1", &[mesh("mesh1")]).await;
    // {mesh1} -> {mesh1, mesh2}, through the rectifier.
    let created = estate.resize_shape("fabric1", &[mesh("mesh1"), mesh("mesh2")]).await;
    let create = created.build_id.to_string();
    let runs = estate.starts_of_build(&create).await;
    let nodes = estate.settled_views(&[mesh("mesh1"), mesh("mesh2")]).await;
    let mesh2_primary = nodes.iter().find(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && n.is_primary).cloned().expect("mesh2 elects a primary");
    let fabric_primary = nodes.iter().find(|n| n.is_fabric_primary).cloned().expect("one fabric primary");
    let bootstrap2 = nodes.iter().find(|n| n.location() == Estate::path("mesh2.node-admin", "1")).cloned().expect("mesh2's bootstrap admin");
    let of = |sp: &Value, n: &NodeView| rdm_test::estate::of_node(sp, &n.location(), n.node_id.as_str());
    // The provider every mesh2 member actually ran on, from the view (and, on containers, its
    // immutable container id).
    let provider = estate.owner.provider.clone();
    let mut mesh2_runtimes: Vec<Value> = Vec::new();
    for n in nodes.iter().filter(|n| n.mesh == "mesh2" && n.r#type != "node-admin") {
        mesh2_runtimes.push(json!({"node": n.name, "provider": n.provider, "container": estate.container_of(&n.location()).await}));
    }
    for r in &mesh2_runtimes {
        assert_eq!(r["provider"], provider.as_str(), "{r}");
        if provider == "container" {
            assert_eq!(r["container"].as_str().map(str::len), Some(64), "an immutable container id: {r}");
        }
    }
    // The estate stops once the fabric is ready under the creation Build (Q174): every mesh,
    // mesh2's included, is ready then.
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    estate.stop().await;
    let spans = estate.spans();

    // The bootstrap admin hydrated first: its entry join and the accepted Build it then holds.
    let joined = named(&spans, "rdm.node_admin.fabric.update.via-join").into_iter().find(|j| of(j, &bootstrap2)).cloned().expect("mesh2.node-admin.1 joined by entry pull");
    // The Pending hand-off, applied exactly once at that birth.
    let handoffs: Vec<Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|h| h["attributes"]["mesh"] == "mesh2").cloned().collect();
    let applied: Vec<&Value> = handoffs.iter().filter(|h| h["attributes"]["outcome"] == "applied").collect();
    assert_eq!(applied.len(), 1, "one Pending applied at mesh2's bootstrap admin: {handoffs:?}");
    let handoff = applied[0];
    assert!(rdm_test::estate::names_location(handoff, "target", &bootstrap2.location()), "{handoff}");
    assert!(end(&joined) <= start(handoff), "the target hydrated ({}) before Pending was handed to it ({})", end(&joined), start(handoff));
    // Decided at the target through its status door, not as an election.
    let decided: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|d| of(d, &bootstrap2) && d["attributes"]["op"] == "apply-mesh-state" && d["attributes"]["outcome"] == "applied")
        .collect();
    assert_eq!(decided.len(), 1, "{decided:?}");
    assert_eq!(decided[0]["attributes"]["receiver_is_primary"], "false", "accepting Pending is not an election");
    // Nothing of mesh2's own membership was created before Pending was applied.
    let members = [Estate::path("mesh2.node-admin", "2"), Estate::path("mesh2.node", "1"), Estate::path("mesh2.node", "2")];
    let member_creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| c["attributes"]["build_id"] == create.as_str() && members.iter().any(|m| rdm_test::estate::at_location(c, m)))
        .collect();
    assert_eq!(member_creates.len(), members.len(), "each member created once under the creation Build: {member_creates:?}");
    let first_member_create = member_creates.iter().map(|c| start(c)).min().unwrap();
    assert!(end(handoff) <= first_member_create, "Pending applied ({}) before mesh2's first own create ({first_member_create})", end(handoff));
    // The bootstrap admin's Ready follows its hydration; mesh2's primary is elected afterwards.
    let ready = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|r| of(r, &bootstrap2)).cloned().expect("mesh2.node-admin.1 committed Ready");
    assert!(end(&joined) <= start(&ready), "Ready after hydration");
    // The elected mesh primary owns the Mesh's later lifecycle: it declares the Mesh's state to the
    // fabric primary, which applies it; and the fabric primary applies the fabric's readiness at
    // the mesh primary. Both come after the elected primary's own Ready.
    let elected_ready = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|r| of(r, &mesh2_primary)).cloned().expect("the elected primary committed Ready");
    let mesh_declared: Vec<&Value> = named(&spans, DECLARATION)
        .into_iter()
        .filter(|d| d["attributes"]["op"] == "declare-mesh-state" && d["attributes"]["sender"] == mesh2_primary.node_id.as_str() && of(d, &fabric_primary))
        .collect();
    assert!(mesh_declared.iter().any(|d| matches!(d["attributes"]["outcome"].as_str(), Some("applied" | "already-applied"))), "the elected primary's Mesh declaration was certain at the fabric primary: {mesh_declared:?}");
    assert!(mesh_declared.iter().all(|d| start(d) >= end(&elected_ready)), "a Mesh declaration comes from a primary that is already Ready: {mesh_declared:?}");
    // A declaration made before the fabric primary holds the Mesh it names is refused by name for
    // that subject and is the only refusal there is; the Mesh is applied once.
    assert!(mesh_declared.iter().all(|d| matches!(d["attributes"]["outcome"].as_str(), Some("applied" | "already-applied")) || (d["attributes"]["outcome"] == "rejected-not-primary" && d["attributes"]["detail"] == "subject-unknown")), "{mesh_declared:?}");
    let mesh2_applied: Vec<&Value> = named(&spans, DECLARATION)
        .into_iter()
        .filter(|d| d["attributes"]["op"] == "declare-mesh-state" && of(d, &fabric_primary) && nodes.iter().any(|n| n.mesh == "mesh2" && n.r#type == "node-admin" && d["attributes"]["sender"] == n.node_id.as_str()) && d["attributes"]["outcome"] == "applied")
        .collect();
    assert!(mesh2_applied.len() <= 1, "mesh2's Ready is applied at most once by one fabric primary, whoever declared it: {mesh2_applied:?}");
    // The creation Build ran once, and its section call to mesh2 is in its request's trace: the
    // fabric-primary that accepted it drove it and dispatched the section.
    assert_eq!(runs, 1, "the creation Build ran once");
    let accepted = named(&spans, "rdm.node_admin.build.create.via-node-rpc").into_iter().find(|sp| sp["attributes"]["build_id"] == create.as_str()).cloned().expect("the creation Build's build.create span");
    let sections: Vec<&Value> = named(&spans, "rdm.node_admin.build.serve.via-attempt-run").into_iter().filter(|r| r["attributes"]["build_id"] == create.as_str() && r["attributes"]["mesh"] == "mesh2").collect();
    assert!(!sections.is_empty(), "the creation Build's section of mesh2 was run");
    // The section call runs under a pass of the creation Build's drive at the fabric-primary that
    // accepted it, joined to the request by `build_id` (telemetry.md §4: each pass of the Build drive
    // is a root per pass, joined by `build_id`).
    let drives: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-drive").into_iter().filter(|d| d["attributes"]["build_id"] == create.as_str() && of(d, &fabric_primary)).collect();
    assert!(sections.iter().any(|r| drives.iter().any(|d| descends_from(&spans, r, d))), "mesh2's section call runs under the fabric-primary's drive of the creation Build: {sections:?}");

    let result = json!({
        "cell": cell,
        "build_id": create,
        "runs": runs,
        "mesh2_primary": mesh2_primary.name,
        "provider": provider,
        "mesh2_runtimes": mesh2_runtimes,
        "joined": {"span_id": joined["span_id"], "end": end(&joined), "served_by": joined["attributes"]["joined"]},
        "handoff": {"span_id": handoff["span_id"], "trace_id": handoff["trace_id"], "start": start(handoff), "end": end(handoff), "attributes": handoff["attributes"]},
        "handoffs_seen": handoffs.len(),
        "decided": decided[0]["attributes"],
        "first_member_create": first_member_create,
        "mesh_declarations": mesh_declared.iter().map(|d| json!({"span_id": d["span_id"], "outcome": d["attributes"]["outcome"], "start": start(d)})).collect::<Vec<_>>(),
        "fabric_primary": fabric_primary,
        "mesh_applied_span": mesh2_applied.first().map(|d| d["span_id"].clone()),
        "elected_ready_end": end(&elected_ready),
        "ready": {"span_id": ready["span_id"], "start": start(&ready)},
        "sections": sections.len(),
        "trace_id": accepted["trace_id"],
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}


/// The bootstrap admin (the one handed Pending) of mesh2 or mesh3 that does not hold the fabric-primary role.
fn bootstrap_admin_not_fabric_primary(settled: &[NodeView]) -> PathName {
    [Estate::path("mesh2.node-admin", "1"), Estate::path("mesh3.node-admin", "1")]
        .into_iter()
        .find(|a| settled.iter().any(|n| n.location() == *a && !n.is_fabric_primary))
        .expect("at most one admin holds the primary role, so one of the two bootstrap admins is off it")
}

/// CONTRACT (#2805, acceptance 8; R-SB1: a restart keeps node.id and incarnation, and a restart of a
/// mesh's only admin is refused; node-stop.md Primary; node.restart is node.stop then node.start, Q183): the bootstrap node-admin of a peer mesh
/// of two admins (mesh2 or mesh3, whichever does not hold the fabric-primary role), the admin Pending was handed
/// to, is restarted through the Build rectifier (never the fabric-primary), its mesh's role moving to the
/// other admin first when it holds it: it comes back as the same birth, the same node and incarnation
/// (node-start.md: a start introduces no new incarnation), and reaches Ready without a fabric-primary
/// hand-off, because the Pending it was applied before the restart is its own durable status row, folded
/// at boot. Ready is never granted by election authority it lacks; its mesh's primary is the restarted
/// admin only through the ordinary election afterwards. What must NOT happen: the restarted admin stuck
/// holding its Ready for a Pending nobody will re-apply, or a second Pending hand-off applied for the
/// same MeshId, or a replacement MeshId.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_meshs_bootstrap_admin_restarts_ready_from_its_durable_pending() {
    let cell = "a_peer_meshs_bootstrap_admin_restarts_ready_from_its_durable_pending";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let mesh = |m: &'static str, admins: u32| (m, admins, 1);
    estate.resize_shape("fabric1", &[mesh("mesh1", 2)]).await;
    let shape = [mesh("mesh1", 2), mesh("mesh2", 2), mesh("mesh3", 2)];
    estate.resize_shape("fabric1", &shape).await;
    let settled = estate.settled_views(&shape).await;
    // The bootstrap admin restarted is one that does not hold the fabric-primary role (the role goes to the
    // lowest node id fabric-wide, and the fabric primary is never restarted by this cell).
    let target = bootstrap_admin_not_fabric_primary(&settled);
    let before = estate.node_view(&target).await;

    // The initial fabric refuses a node stop until it has first reached ready (states.md "The initial fabric before ready").
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // The restart, node.stop then node.start (Q183): node commands, which belong to no Build (Q342).
    estate.stop_then_start(&target).await;
    let restart_build = String::new();
    let after = estate.settled_views(&shape).await;
    let after = after.iter().find(|n| n.location() == target).cloned().expect("the restarted admin in the view");
    let mesh_name = after.mesh.clone();
    estate.stop().await;
    let spans = estate.spans();

    assert_eq!(after.node_id, before.node_id, "the same node");
    assert_eq!(after.incarnation_id, before.incarnation_id, "the same incarnation: a stop then a start keep the birth (node-stop.md, Kept: same node.id, incarnation)");
    assert_eq!(after.status, NodeStatus::Ready, "{after:?}");
    let readies: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().filter(|r| rdm_test::estate::of_node(r, &target, before.node_id.as_str())).collect();
    assert_eq!(readies.len(), 2, "Ready at birth and again after the restart: {readies:?}");
    let handoffs: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|h| h["attributes"]["mesh"] == mesh_name.as_str()).collect();
    let mesh_ids: std::collections::BTreeSet<String> = handoffs.iter().map(|h| s(&h["attributes"]["mesh_id"])).collect();
    assert_eq!(mesh_ids.len(), 1, "one MeshId for {mesh_name} in every hand-off, none minted: {mesh_ids:?}");
    let blocked: Vec<&Value> = named(&spans, "rdm.node_admin.runtime.reject.via-not-ready")
        .into_iter()
        .filter(|b| rdm_test::estate::of_node(b, &target, before.node_id.as_str()) && s(&b["attributes"]["detail"]).contains("Pending has not been applied"))
        .collect();
    let result = json!({
        "cell": cell,
        "target": target,
        "restart_build": restart_build,
        "before_incarnation": before.incarnation_id,
        "after_incarnation": after.incarnation_id,
        "ready_spans": readies.len(),
        "handoff_spans": handoffs.len(),
        "mesh_ids": mesh_ids,
        "held_for_pending_spans": blocked.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (#2805, acceptance 2; node-create.md "A node-admin created into a live mesh" step 4: "as the
/// mesh's successor primary it adopts the mesh's held state"; node-recovery.md step 2, Q335; states.md
/// "mesh Pending"): a peer mesh's only node-admin is lost (its runtime killed exactly; its mesh holds no
/// fabric-primary role) while its member lives. The fabric primary runs node investigation on the lost
/// mesh-primary and recovers it on the verdict, and the node-admin it re-creates joins the live mesh
/// under the mesh's EXISTING MeshId, with no Build. As the mesh's successor primary it adopts the
/// mesh's held state from the maker's topology and republishes the held MeshStatus with its changed_at
/// preserved, after its checklist. Pending is handed once, to the admin that launched the mesh's first
/// admin: the recovery admin is handed no state at all (D65). What must NOT happen: a second certain
/// Pending hand-off, any state hand-off after the fault, a republish under a new changed_at or before the
/// checklist completed, a `degraded` status, a second MeshId for the mesh, or a new Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_admin_adopts_the_held_mesh_status_under_the_existing_mesh_id() {
    let cell = "recovery_admin_adopts_the_held_mesh_status_under_the_existing_mesh_id";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let mesh = |m: &'static str, admins: u32| (m, admins, 1);
    let shape = [mesh("mesh1", 2), mesh("mesh2", 1), mesh("mesh3", 1)];
    let built = estate.resize_shape("fabric1", &shape).await;
    let accepted = built.build_id.to_string();
    let before = estate.settled_views(&shape).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let fabric_primary_view = before.iter().find(|n| n.is_fabric_primary).cloned().expect("one fabric primary");
    let fabric_primary = fabric_primary_view.location();
    // The lost mesh is a sole-admin mesh that does not hold the fabric-primary role.
    let lost_admin = bootstrap_admin_not_fabric_primary(&before);
    let lost = before.iter().find(|n| n.location() == lost_admin).map(|n| n.mesh.clone()).expect("the lost admin's mesh");
    let mesh_id = estate.mesh_view(&lost).await.id.expect("the lost mesh has its id").to_string();
    let admin_before = before.iter().find(|n| n.location() == lost_admin).cloned().unwrap();
    let runs_before = estate.starts_of_build(&built.build_id.to_string()).await;
    estate.enter(&fabric_primary).await;

    // The fault: the lost mesh's only admin runtime, exactly.
    let fault_ns = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos() as u64;
    let mut killed = 0;
    for (path, pid) in estate.live_runtimes() {
        if path.file_name().unwrap().to_string_lossy().starts_with(admin_before.node_id.as_str()) {
            estate.kill_pid(pid);
            killed += 1;
        }
    }
    assert_eq!(killed, 1, "the lost mesh's only admin was killed");

    let admin_inc = serde_json::to_value(&admin_before.incarnation_id).expect("an incarnation serializes");
    let recovered = estate
        .until_node(&lost_admin, "the lost mesh's admin is created again and ready", Duration::from_secs(240), |n| {
            let n = n?;
            (n.incarnation_id != admin_before.incarnation_id && n.status == NodeStatus::Ready).then(|| serde_json::to_value(n).expect("a view serializes"))
        })
        .await;
    assert_eq!(estate.mesh_view(&lost).await.id.expect("the lost mesh still has its id").to_string(), mesh_id, "the lost mesh recovered under its own MeshId");
    assert_eq!(estate.accepted_build_id().await, accepted, "no new topology Build");
    // The fabric is ready before the estate stops.
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    // The successor adopts the mesh's held state on its own clock: the fabric can be ready before it, so
    // the estate stops only once it has.
    rdm_test::estate::wait_for("the successor adopts the mesh's held state", Duration::from_secs(60), || async {
        named(&estate.spans(), "rdm.node_admin.mesh.update.via-held-state-adopted").into_iter().find(|sp| sp["attributes"]["mesh"] == lost.as_str() && rdm_test::estate::at_location(sp, &lost_admin) && start(sp) >= fault_ns).map(|_| ())
    })
    .await;
    // The republish follows the successor's checklist, which completes when every member is heard
    // since it opened: on its own clock, after the hand-off, so the estate stops only once it is sent.
    let lost_scope = format!("mesh:{lost}");
    let successor = s(&recovered["node_id"]);
    rdm_test::estate::wait_for("the successor primary republishes the held MeshStatus", Duration::from_secs(60), || async {
        named(&estate.spans(), "rdm.mesh.fabric.update.via-status-send").into_iter().find(|sp| sp["attributes"]["scope"] == lost_scope.as_str() && s(&sp["attributes"]["node"]) == successor && start(sp) >= fault_ns).map(|_| ())
    })
    .await;
    estate.stop().await;
    let spans = estate.spans();

    // Pending: once, at the mesh's first birth.
    let handoffs: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|h| h["attributes"]["mesh"] == lost.as_str()).collect();
    let ids: std::collections::BTreeSet<String> = handoffs.iter().map(|h| s(&h["attributes"]["mesh_id"])).collect();
    assert_eq!(ids, [mesh_id.clone()].into_iter().collect(), "every hand-off names the one MeshId, none minted: {handoffs:?}");
    let certain_handoffs: Vec<&&Value> = handoffs.iter().filter(|h| matches!(h["attributes"]["outcome"].as_str(), Some("applied" | "already-applied"))).collect();
    assert_eq!(certain_handoffs.len(), 1, "Pending is made certain once, at the mesh's first birth; the recovery admin is never handed Pending again: {handoffs:?}");
    assert!(start(certain_handoffs[0]) < fault_ns, "the one certain hand-off is the first birth's, before the fault: {handoffs:?}");
    let pending_after: Vec<&&Value> = handoffs.iter().filter(|h| start(h) >= fault_ns).collect();
    assert!(pending_after.is_empty(), "the recovery admin is never handed Pending: {pending_after:?}");
    let state_after: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-state-handoff").into_iter().filter(|h| h["attributes"]["mesh"] == lost.as_str() && start(h) >= fault_ns).collect();
    assert!(state_after.is_empty(), "the recovery admin is handed no state; it adopts the held state (D65): {state_after:?}");

    // The decision: the fabric primary's node recovery of the lost mesh-primary, on its node investigation's
    // verdict (Q335).
    let decision = named(&spans, "rdm.node_admin.node.update.via-node-recovery")
        .into_iter()
        .filter(|sp| sp["attributes"]["node_id"] == admin_before.node_id.as_str() && start(sp) >= fault_ns)
        .min_by_key(|sp| start(sp))
        .cloned()
        .expect("the fabric primary submitted the node recovery of the lost mesh's node-admin");
    assert_eq!(decision["attributes"]["submitter"], fabric_primary_view.node_id.as_str(), "the fabric primary submitted it: {decision}");

    // Step 7: the successor primary republishes the held MeshStatus, changed_at preserved, after its checklist.
    let scope = format!("mesh:{lost}");
    let sends = named(&spans, "rdm.mesh.fabric.update.via-status-send");
    // The held MeshStatus is the latest the lost admin published before the fault, as its own send or
    // as the fabric heard it (a via-mesh-status with the lost admin as publisher): the SIGKILL loses
    // the killed process's spans not yet exported, so its last send may live only in its receivers'.
    let heard = named(&spans, "rdm.mesh.topology.update.via-mesh-status")
        .into_iter()
        .filter(|sp| sp["attributes"]["mesh"] == lost.as_str() && sp["attributes"]["publisher"] == admin_before.node_id.as_str());
    let held = sends
        .iter()
        .copied()
        .filter(|sp| sp["attributes"]["scope"] == scope.as_str() && sp["attributes"]["node"] == admin_before.node_id.as_str())
        .chain(heard)
        .filter(|sp| start(sp) < fault_ns)
        .map(|sp| (s(&sp["attributes"]["status"]), s(&sp["attributes"]["changed_at_rafka_ms"])))
        .max_by_key(|(_, at)| at.parse::<u64>().expect("a changed_at_rafka_ms"))
        .expect("the lost mesh published its MeshStatus before the fault");
    // What the successor adopted: the mesh's held state, from its maker's topology (node-create.md "A
    // node-admin created into a live mesh" step 4). It is never behind what the lost admin published before the fault.
    let rank = |st: &str| ["pending", "ready", "leaving", "dead"].iter().position(|x| *x == st).unwrap_or(0);
    let adopted = named(&spans, "rdm.node_admin.mesh.update.via-held-state-adopted")
        .into_iter()
        .filter(|sp| sp["attributes"]["mesh"] == lost.as_str() && rdm_test::estate::at_location(sp, &lost_admin) && start(sp) >= start(&decision))
        .map(|sp| s(&sp["attributes"]["state"]).to_lowercase())
        .last()
        .expect("the successor adopted the mesh's held state after the decision");
    assert!(rank(&adopted) >= rank(&held.0), "the adopted state is never behind the held MeshStatus {held:?}: {adopted:?}");
    let expected = held.clone();
    let degraded_sends: Vec<&&Value> = sends.iter().filter(|sp| sp["attributes"]["status"] == "degraded").collect();
    assert!(degraded_sends.is_empty(), "no degraded status is sent: {degraded_sends:?}");
    let republished: Vec<&&Value> = sends
        .iter()
        .filter(|sp| sp["attributes"]["scope"] == scope.as_str() && start(sp) >= start(&decision))
        .collect();
    assert!(!republished.is_empty(), "the successor primary republishes the held MeshStatus (a via-status-send for {scope} after the decision): none");
    for sp in &republished {
        assert_eq!(s(&sp["attributes"]["node"]), s(&recovered["node_id"]), "the mesh's status is its own primary's, the recovered birth at {lost_admin:?}: {sp}");
        assert_eq!((s(&sp["attributes"]["status"]), s(&sp["attributes"]["changed_at_rafka_ms"])), expected, "the republish is the held MeshStatus, changed_at preserved: {sp}");
    }
    let checklist_complete = named(&spans, "rdm.node_admin.mesh.update.via-checklist-complete")
        .into_iter()
        .filter(|sp| sp["attributes"]["mesh"] == lost.as_str() && rdm_test::estate::at_location(sp, &lost_admin) && start(sp) >= start(&decision))
        .map(start)
        .min()
        .expect("the successor primary's checklist completed");
    let first_republish = republished.iter().map(|sp| start(sp)).min().unwrap();
    assert!(first_republish >= checklist_complete, "the held MeshStatus is republished only after the checklist completed: {first_republish} vs {checklist_complete}");

    let result = json!({
        "cell": cell,
        "lost_admin": lost_admin,
        "fabric_primary": fabric_primary,
        "mesh_id": mesh_id,
        "accepted_build": accepted,
        "runs_before": runs_before,
        "admin_before": admin_inc,
        "admin_after": recovered["incarnation_id"],
        "handoffs": handoffs.len(),
        "decision": {"span_id": decision["span_id"], "start": start(&decision)},
        "held_mesh_status": {"status": held.0, "changed_at_rafka_ms": held.1},
        "republished": republished.len(),
        "checklist_complete": checklist_complete,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    estate.record_trace_url(decision["trace_id"].as_str().unwrap_or(""));
}
