//! i143.e4.s10 acceptance (rafka-v2 #2803, hardened 2026-10-07), CHAOS-PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2803-chaos-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest, rpc ledger and every process's spans, feature `i143-2803`).
//!
//! Loss of every node-admin of a mesh and recovery through the Build rectifier. The fabric-primary role is never in
//! the lost mesh: the mesh lost is the one whose admins do not hold it (a fabric-primary's mesh is
//! only ever lost after its authority was handed off; that is a separate cell).

use rdm_core::PathName;
use rdm_test::estate::location;
use rdm_client::NodeView;
use rdm_test::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2803".into(),
        subfeature: "mesh-recovery".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn acceptance_dir(layer: &str, cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2803").join(layer).join(cell),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// The views as the JSON the cell records.
fn values(views: &[NodeView]) -> Vec<Value> {
    views.iter().map(|n| serde_json::to_value(n).expect("a view serializes")).collect()
}

/// location -> incarnation
fn births(nodes: &[Value]) -> BTreeMap<PathName, String> {
    nodes.iter().map(|n| (location(&n), s(&n["incarnation_id"]))).collect()
}

/// The fabric-primary role handed over before any loss (Luke 2026-10-12, i143-prd.md e12.s57): a node.stop of
/// the fabric-primary's admin `former`, whose first step moves its roles to its same-mesh successor
/// (node-primary-update.md step 1). The move ends the caller's stream with the new role, and the
/// Build is followed there to its end: the former primary is down only once the stop completes, and
/// the successor's view names it the fabric-primary. Returns the successor.
async fn hand_fabric_primary_over(estate: &mut Estate, former: &PathName) -> PathName {
    let successor = former.with_name(if former.name == "1" { "2" } else { "1" }).expect("a sibling admin's location");
    estate.enter(&successor).await;
    let mut stream = estate.nodes_obj().await.stop(former).await.unwrap_or_else(|e| panic!("the fabric-primary refused node.stop of {former:?}: {e}"));
    let accepted = stream.accepted().clone();
    if matches!(Estate::frames_of(&mut stream).await.last(), Some(rdm_client::Frame::FabricPrimaryMoved { .. })) {
        estate.resume(&accepted.build_id, accepted.attempt).await;
    }
    estate.until_node(&successor, &format!("{successor:?} holds the fabric-primary role {former:?} handed over"), Duration::from_secs(60), |n| n.filter(|n| n.is_fabric_primary).map(|_| ())).await;
    successor
}

/// CONTRACT (#2803 acceptance 2, 3, 10, 13, 14, 15, 16, 19; node-admin-lifecycle.md 4.1-4.3): a peer
/// mesh loses every node-admin (exact SIGKILL of both admin runtimes) and, while it has
/// no admin, one ordinary member dies as well (`rpc.3`). The fabric primary's verdict on the lost
/// mesh opens an attempt of the SAME accepted Build that runs node.create of a node-admin into the
/// live mesh (mesh-investigation.md steps 6-7): it joins under the mesh's EXISTING MeshId (no id is
/// minted) through its maker (JoinNode), adopts the mesh's held state as its successor primary (no
/// Pending is handed: the mesh is not created again), and sweeps its own mesh once with a Ping: the
/// two members that live answer, the one that died does not. The unreached member goes to
/// node recovery (mesh-recovery.md step 6): the new mesh-primary's tickle, then the provider's exact
/// exited verdict, then one recovery under the same Build (node.delete of the exited instance with no
/// RPC to it, node.create at the same path.name, node.update); a missing Ping reply is the
/// trigger, never the proof. The second admin is created by the NEW mesh primary. What must NOT
/// happen: a new MeshId or Build id, a Pending handed to the joining node-admin, a surviving member
/// re-created, a member that answered recovered, the fabric primary creating the second admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_admin_restores_lost_node_admins_preserves_mesh_and_build() {
    let cell = "recovery_admin_restores_lost_node_admins_preserves_mesh_and_build";
    let dir = acceptance_dir("chaos-process", cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = [("mesh1", 2, 3), ("mesh2", 2, 3)];
    let accepted = estate.reconcile_shape("fabric1", &shape).await.build_id.to_string();
    let before = values(&estate.settled_views(&shape).await);
    // The node-admins are lost from a running fabric: the fault falls after its first ready.
    estate.until_fabric_ready(Duration::from_secs(120)).await;

    // The lost mesh: the one whose admins do not hold the fabric-primary role.
    let fabric_primary_row = before.iter().find(|n| n["is_fabric_primary"] == true).cloned().expect("one fabric primary");
    let (fabric_primary, fabric_primary_id) = (location(&fabric_primary_row), s(&fabric_primary_row["node_id"]));
    let lost = if fabric_primary.path.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let lost_mesh_id = estate.mesh_view(lost).await.id.expect("the lost mesh has its id").to_string();
    let window_victim = Estate::path(&format!("{lost}.rpc"), "3");
    let survivors: Vec<PathName> = (1..=2).map(|i| Estate::path(&format!("{lost}.rpc"), &i.to_string())).collect();
    assert_eq!(estate.accepted_build_id().await, accepted, "the accepted Build names the topology");
    let attempt_before = estate.attempt_of_build(&accepted).await;
    let lost_births: BTreeMap<PathName, String> = births(&before).into_iter().filter(|(n, _)| n.path == format!("{lost}.admin") || *n == window_victim).collect();
    // A runtime's data dir is named by its birth's node id.
    let lost_admin_ids: Vec<String> = before.iter().filter(|n| n["mesh"] == lost && n["kind"] == "node_admin").map(|n| s(&n["node_id"])).collect();
    let victim_id = before.iter().find(|n| location(&n) == window_victim).map(|n| s(&n["node_id"])).expect("the member's node id");

    // Control goes through the fabric primary's advertised API, which the fault never touches.
    estate.enter(&fabric_primary).await;

    // The fault: both admins at once, then, with the mesh admin-less, one ordinary member.
    let mut killed = Vec::new();
    for (path, pid) in estate.live_runtimes() {
        let id = path.file_name().unwrap().to_string_lossy().to_string();
        if lost_admin_ids.contains(&id) {
            estate.kill_pid(pid);
            killed.push(json!({"node_id": id, "pid": pid}));
        }
    }
    if lost == "mesh1" {
        estate.kill_bootstrap();
        killed.push(json!({"node": "mesh1.admin.1", "pid": "bootstrap"}));
    }
    assert_eq!(killed.len(), 2, "both admins of {lost} were lost: {killed:?}");
    // The member dies too (SIGKILL): its exact runtime exits while the mesh has no admin.
    let mut victim_pid = 0u32;
    for (path, pid) in estate.live_runtimes() {
        if path.file_name().unwrap().to_string_lossy() == victim_id.as_str() {
            estate.kill_pid(pid);
            victim_pid = pid;
            killed.push(json!({"node": window_victim, "node_id": victim_id, "pid": pid, "signal": "KILL"}));
        }
    }
    assert_eq!(killed.len(), 3, "the member died in the admin-less window: {killed:?}");

    // Recovery through the rectifier: the same Build, later attempts, the node-admins and the member back.
    let done = wait_for("the accepted Build is complete past the fault and the lost births are re-born", Duration::from_secs(240), || {
        let estate = &estate;
        let (accepted, lost_births) = (accepted.clone(), lost_births.clone());
        async move {
            let b = estate.carrier().await.build(&rdm_client::BuildId::parse(&accepted).unwrap()).await.expect("build.get [id] of the accepted Build");
            let later = b.phase == rdm_core::build::BuildPhase::Complete && u64::from(b.attempt) > attempt_before;
            let re_born = births(&estate.node_values().await).iter().filter(|(n, i)| lost_births.get(*n).is_some_and(|old| old != *i)).count() == lost_births.len();
            (later && re_born).then_some(b)
        }
    })
    .await;
    let after = values(&estate.settled_views(&shape).await);
    assert_eq!(estate.accepted_build_id().await, accepted, "no new topology Build: the current build_id is unchanged");
    assert_eq!(estate.mesh_view(lost).await.id.expect("the lost mesh still has its id").to_string(), lost_mesh_id, "{lost} recovered under its own MeshId");
    let (old, new) = (births(&before), births(&after));
    for (name, inc) in &old {
        if lost_births.contains_key(name) {
            assert_ne!(&new[name], inc, "{name:?}: a new birth (its runtime was lost)");
        } else {
            assert_eq!(&new[name], inc, "{name:?}: preserved, never recreated");
        }
    }

    // The member's recovery runs to its end before the estate stops. Since Q273 its verdict is the
    // inspect's (`exited`, no tickle), so its first submit can come before the recovery admin holds
    // mesh2's role and is answered not-claimant and sent again, and the settled view above can be read
    // while that create still waits for the node's Ready.
    wait_for("the dead member's recovery completes", Duration::from_secs(60), || {
        let estate = &estate;
        let (victim, id) = (window_victim.clone(), victim_id.clone());
        async move {
            let spans = estate.spans();
            named(&spans, "rdm.node_admin.build.update.via-node-recovery")
                .into_iter()
                .any(|sp| rdm_test::estate::at_location(sp, &victim) && s(&sp["attributes"]["node_id"]) == id && s(&sp["attributes"]["outcome"]).starts_with("complete"))
                .then_some(())
        }
    })
    .await;
    estate.enter_fabric_primary().await;
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);

    // The fabric primary's verdict on the lost mesh runs node.create of a node-admin into the live
    // mesh (mesh-investigation.md step 6): an attempt of the accepted Build past the fault, which the
    // fabric primary executes. The node-admin joins under the mesh's EXISTING MeshId and adopts the
    // mesh's held state (step 7); no Pending is handed to it.
    // The admin it created: the create in that attempt's own trace, at a location of the lost mesh's admins.
    let lost_admins = format!("{lost}.admin");
    let creates_admin = |r: &Value| named(&spans, "rdm.node_admin.node.create.via-build").into_iter().find(|c| c["trace_id"] == r["trace_id"] && attr(c, "node_path") == lost_admins).cloned();
    let first = named(&spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .filter(|r| attr(r, "build_id") == accepted && attr(r, "executor_node_id") == fabric_primary_id && attr(r, "attempt").parse::<u64>().is_ok_and(|a| a > attempt_before) && creates_admin(r).is_some())
        .min_by_key(|r| at(r))
        .expect("the fabric primary created a node-admin into the lost mesh under the accepted Build");
    let created = creates_admin(first).expect("the first attempt's create");
    let (recovery_admin, recovery_admin_id) = (PathName::new(attr(&created, "node_path"), attr(&created, "node_name")).expect("a create names its location"), attr(&created, "node_id"));
    let joined: Vec<&Value> = named(&spans, "rdm.node.mesh.update.via-joining").into_iter().filter(|j| rdm_test::estate::of_node(j, &recovery_admin, &recovery_admin_id) && at(j) > at(first)).collect();
    assert!(!joined.is_empty() && joined.iter().all(|j| attr(j, "mesh_id") == lost_mesh_id), "{recovery_admin:?} joined under {lost}'s own MeshId: it never minted: {joined:?}");
    let adopted = named(&spans, "rdm.node_admin.mesh.update.via-held-state-adopted").into_iter().filter(|a| attr(a, "publisher") == recovery_admin_id && attr(a, "mesh_id") == lost_mesh_id && at(a) > at(first)).count();
    assert!(adopted > 0, "{recovery_admin:?} adopted {lost}'s held state as its successor primary");
    let handed: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|h| attr(h, "mesh") == lost && at(h) > at(first)).collect();
    assert!(handed.is_empty(), "no Pending is handed to a node-admin joining a live mesh: {handed:?}");

    // Its own-mesh sweep: one, once, from the recovery admin; the members that live answered, the
    // one that died did not.
    let sweeps: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-entry-sweep").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &recovery_admin)).collect();
    assert_eq!(sweeps.len(), 1, "one sweep on entering the mesh: {sweeps:?}");
    let swept = |sp: &Value| PathName::new(attr(sp, "node_path"), attr(sp, "node_name")).expect("a swept node's location");
    let silent: Vec<PathName> = named(&spans, "rdm.node_admin.node.reject.via-entry-sweep-no-reply").into_iter().filter(|sp| attr(sp, "sweeper_node_id") == recovery_admin_id).map(swept).collect();
    let sibling = recovery_admin.with_name(if recovery_admin.name == "1" { "2" } else { "1" }).expect("a sibling admin's location");
    for dead in [&window_victim, &sibling] {
        assert!(silent.contains(dead), "{dead:?} died and did not answer the sweep: {silent:?}");
    }
    for m in &survivors {
        assert!(!silent.contains(m), "{m:?} answered the sweep: {silent:?}");
    }
    // Not reached goes to node recovery under the new mesh-primary (mesh-recovery.md step 6): its tickle,
    // the provider's exact exited verdict, one recovery of the exited birth under the same Build. No
    // decommission queue exists.
    assert!(named(&spans, "rdm.node_admin.node.update.via-sweep-decommission").is_empty(), "no sweep decommission: the unreached member is node-recovered");
    let recovery: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-node-recovery")
        .into_iter()
        .filter(|sp| rdm_test::estate::at_location(sp, &window_victim) && attr(sp, "node_id") == victim_id && attr(sp, "outcome").starts_with("complete"))
        .collect();
    assert_eq!(recovery.len(), 1, "the dead member is recovered once, keyed on its exited birth: {recovery:?}");
    let lost_admins_after: Vec<String> = after.iter().filter(|n| n["mesh"] == lost && n["kind"] == "node_admin").map(|n| s(&n["node_id"])).collect();
    assert!(lost_admins_after.contains(&attr(recovery[0], "submitter")), "by its own mesh's new primary: {recovery:?}");
    assert_eq!(attr(recovery[0], "build_id"), accepted, "under the same Build: {recovery:?}");
    // The recovery's steps are the ones in its submit's own trace (R-BI7: the mesh-primary runs it in
    // its mesh's drift lane, whose attempt number also names an attempt of the fabric lane).
    let victim_steps: Vec<String> = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| sp["trace_id"] == recovery[0]["trace_id"] && rdm_test::estate::at_location(sp, &window_victim) && attr(sp, "build_id") == accepted && attr(sp, "attempt") == attr(recovery[0], "attempt") && attr(sp, "outcome") == "complete")
        .map(|sp| attr(sp, "step"))
        .collect();
    for step in ["NodeDeleting", "NodeDeleted", "DeployRuntime", "WaitForNodeReady", "Complete"] {
        assert!(victim_steps.iter().any(|n| n == step), "the recovery ran {step} for {window_victim:?}: {victim_steps:?}");
    }
    for step in ["DeleteConnections", "StopNode"] {
        assert!(!victim_steps.iter().any(|n| n == step), "no RPC to the exited process: {step} ran for {window_victim:?}: {victim_steps:?}");
    }
    let gone = !std::path::Path::new(&format!("/proc/{victim_pid}")).exists() || std::fs::read_to_string(format!("/proc/{victim_pid}/stat")).unwrap_or_default().contains(") Z ");
    assert!(gone, "the member's exact runtime (pid {victim_pid}) exited");
    // The members that answered are never deleted or created again.
    let touched: Vec<PathName> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .chain(named(&spans, "rdm.node_admin.node.delete.via-build"))
        .filter(|sp| attr(sp, "build_id") == accepted && survivors.iter().any(|m| rdm_test::estate::at_location(sp, m)) && at(sp) > at(first))
        .map(|sp| swept(sp))
        .collect();
    assert!(touched.is_empty(), "members that answered are neither deleted nor created again: {touched:?}");

    // The second admin was created by an attempt the NEW mesh primary executed, not the fabric primary.
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| attr(r, "build_id") == accepted).collect();
    // The birth is an operation of the accepted Build: the fabric-primary's drift check replaces the
    // proven-exited admin (`node.update.via-build`, node-replace.md), or the shape creates the missing
    // one (`node.create.via-build`).
    let creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .chain(named(&spans, "rdm.node_admin.node.update.via-build"))
        .filter(|c| attr(c, "build_id") == accepted && attr(c, "node_path") == lost_admins && !rdm_test::estate::at_location(c, &recovery_admin) && at(c) > at(first))
        .collect();
    assert!(!creates.is_empty(), "the second admin of {lost} was created after the first: {creates:?}");
    for c in &creates {
        // The reconcile the create ran in is the one in its own trace: a drift lane's attempt number
        // also names an attempt of the fabric lane (R-BI7).
        let executor = reconciles.iter().find(|r| r["trace_id"] == c["trace_id"] && attr(r, "attempt") == attr(c, "attempt")).map(|r| attr(r, "executor_node_id")).expect("the create ran inside a reconcile");
        assert_eq!(executor, recovery_admin_id, "{:?} was created by the new mesh primary, not by {fabric_primary:?}", swept(c));
    }

    let result = json!({
        "cell": cell,
        "fabric_id": estate.fabric_id,
        "mesh_id": lost_mesh_id,
        "mesh_name": lost,
        "accepted_build_id": accepted,
        "attempt_before": attempt_before,
        "attempt": done.attempt,
        "fabric_primary": fabric_primary,
        "killed": killed,
        "recovery_admin": recovery_admin,
        "recovery_attempt": attr(first, "attempt"),
        "provider": estate.owner.provider,
        "desired_shape": shape,
        "live_before": old.len(),
        "preserved_count": survivors.len(),
        "not_reached": silent,
        "tombstoned_count": silent.len(),
        "created_count": creates.len(),
        "live_after": new.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (#2803 acceptance 4, 5, 8, 9, 10; node-admin-lifecycle.md 3, 4.4): every node-admin of the
/// fabric is lost at once (exact SIGKILL of all four admin runtimes) and, with no admin running,
/// one ordinary member of the fabric primary's mesh dies as well. Before the loss the fabric-primary role is
/// handed over: node.stop of the fabric-primary's admin moves its roles to its same-mesh successor
/// first and takes it down only once that transfer is confirmed. A person starts ONE node-admin on
/// the current fabric primary's data dir with both primary flags. It is not Day 0: it comes back as
/// the same node (same NodeId, a new incarnation) with no maker, takes its durable map as the
/// topology, connects to a local node of its own mesh, sweeps its own mesh once with a Ping (the
/// dead sibling and the dead member do not answer), and sends them to the standard decommission.
/// The other mesh's admins are restored by the fabric under the same Build with their existing
/// MeshIds. What must NOT happen: a Day-0 start or a new Build or MeshId, a member that answered
/// created or deleted again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fabric_recovery_restarts_one_admin_and_restores_every_missing_admin() {
    let cell = "fabric_recovery_restarts_one_admin_and_restores_every_missing_admin";
    let dir = acceptance_dir("chaos-process", cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = [("mesh1", 2, 3), ("mesh2", 2, 3)];
    let accepted = estate.reconcile_shape("fabric1", &shape).await.build_id.to_string();
    let before = values(&estate.settled_views(&shape).await);
    // The fault falls on an established fabric: the person's admin restarts from the Fabric record the
    // fabric-primary writes at the state commit, so the fabric is ready first.
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let former = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| location(&n)).expect("one fabric primary");
    let primary = hand_fabric_primary_over(&mut estate, &former).await;
    let primary_mesh = primary.path.split('.').next().unwrap().to_string();
    let primary_dir = if primary == Estate::path("mesh1.admin", "1") { estate.bootstrap_data_dir("mesh1").display().to_string() } else { estate.data_dir(&primary).await };
    let mesh_ids: BTreeMap<String, String> = {
        let mut m = BTreeMap::new();
        for mesh in ["mesh1", "mesh2"] {
            m.insert(mesh.to_string(), estate.mesh_view(mesh).await.id.expect("the mesh has its id").to_string());
        }
        m
    };
    let window_victim = Estate::path(&format!("{primary_mesh}.rpc"), "3");
    let own_members: Vec<PathName> = (1..=2).map(|i| Estate::path(&format!("{primary_mesh}.rpc"), &i.to_string())).collect();
    let old = births(&before);
    let old_ids: BTreeMap<PathName, String> = before.iter().map(|n| (location(&n), s(&n["node_id"]))).collect();
    let victim_id = old_ids[&window_victim].clone();
    // A runtime's data dir is named by its birth's node id.
    let admin_ids: Vec<String> = before.iter().filter(|n| n["kind"] == "node_admin").map(|n| s(&n["node_id"])).collect();

    // The fault: every node-admin, at once, the stopped former primary's process with them; then, with
    // the fabric admin-less, one ordinary member.
    let mut killed = Vec::new();
    for (path, pid) in estate.live_runtimes() {
        let id = path.file_name().unwrap().to_string_lossy().to_string();
        if admin_ids.contains(&id) {
            estate.kill_pid(pid);
            killed.push(json!({"node_id": id, "pid": pid}));
        }
    }
    estate.kill_bootstrap();
    killed.push(json!({"node": "mesh1.admin.1", "pid": "bootstrap"}));
    assert_eq!(killed.len(), 4, "all four node-admins were lost: {killed:?}");
    for (path, pid) in estate.live_runtimes() {
        if path.file_name().unwrap().to_string_lossy() == victim_id.as_str() {
            estate.kill_pid(pid);
            killed.push(json!({"node": window_victim, "node_id": victim_id, "pid": pid}));
        }
    }
    assert_eq!(killed.len(), 5, "the member died with no admin running: {killed:?}");

    // The person starts one node-admin on the current fabric primary's data dir.
    let restarted = estate.restart_admin_with(std::path::Path::new(&primary_dir), &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    estate.enter(&restarted).await;
    let after = values(&estate.settles_to(&shape, Duration::from_secs(240)).await);

    assert_eq!(estate.accepted_build_id().await, accepted, "no new Build: the current build_id is unchanged");
    for (mesh, id) in &mesh_ids {
        assert_eq!(&estate.mesh_view(mesh).await.id.expect("the mesh still has its id").to_string(), id, "{mesh} recovered under its own MeshId");
    }
    let new = births(&after);
    let new_ids: BTreeMap<PathName, String> = after.iter().map(|n| (location(&n), s(&n["node_id"]))).collect();
    for (name, inc) in &old {
        if name.path.ends_with(".admin") || *name == window_victim {
            assert_ne!(&new[name], inc, "{name:?}: a new birth");
            if *name == primary {
                assert_ne!(new_ids[name], old_ids[name], "{name:?}: the admin started on the fabric-primary's data dir is a new node over it (R-SB1; fabric-recovery.md)");
            }
        } else {
            assert_eq!(&new[name], inc, "{name:?}: an ordinary member that lives is preserved, never re-created");
        }
    }

    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let primary_id = new_ids[&primary].clone();
    let started: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-recovery-start").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &primary)).collect();
    assert!(
        started.iter().any(|sp| sp["attributes"]["mesh_primary"] == "true" && sp["attributes"]["fabric_primary"] == "true"),
        "the person-started admin recorded both recovery flags: {started:?}"
    );
    let t0 = started.iter().map(|sp| at(sp)).min().unwrap_or(0);
    // Living members of its own mesh hold rafka-time: it pulls rafka-time from one of them rather
    // than adopting its own clock.
    let adopted: Vec<&Value> = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted")
        .into_iter()
        .filter(|sp| attr(sp, "node") == primary_id && at(sp) >= t0)
        .collect();
    assert_eq!(adopted.len(), 1, "the re-born fabric primary adopted rafka-time once: {adopted:?}");
    assert_eq!(attr(adopted[0], "source"), "pull");
    assert_eq!(attr(adopted[0], "reason"), "recovery-pull");
    let pulls: Vec<&Value> = named(&spans, "rdm.mesh.entry.resolve.via-recovery-pull").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &primary) && at(sp) >= t0).collect();
    assert_eq!(pulls.len(), 1, "the re-born fabric primary pulled rafka-time from a live node in its durable map: {pulls:?}");
    assert_eq!(attr(pulls[0], "outcome"), "adopted");
    // No maker: its topology is its durable map; one sweep of its own mesh, once.
    let sweeps: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-entry-sweep").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &primary) && at(sp) >= t0).collect();
    assert_eq!(sweeps.len(), 1, "one sweep on entering its mesh: {sweeps:?}");
    assert_eq!(attr(sweeps[0], "source"), "durable-map", "a re-born fabric primary has no maker");
    let swept = |sp: &Value| PathName::new(attr(sp, "node_path"), attr(sp, "node_name")).expect("a swept node's location");
    let silent: Vec<PathName> = named(&spans, "rdm.node_admin.node.reject.via-entry-sweep-no-reply").into_iter().filter(|sp| attr(sp, "sweeper_node_id") == primary_id && at(sp) >= t0).map(swept).collect();
    let sibling = primary.with_name(if primary.name == "1" { "2" } else { "1" }).expect("a sibling admin's location");
    for dead in [&window_victim, &sibling] {
        assert!(silent.contains(dead), "{dead:?} died and did not answer the sweep: {silent:?}");
    }
    for m in &own_members {
        assert!(!silent.contains(m), "{m:?} answered the sweep: {silent:?}");
    }
    let recreated: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| at(c) >= t0 && own_members.iter().any(|m| rdm_test::estate::at_location(c, m)))
        .collect();
    assert!(recreated.is_empty(), "no member that answered is created again: {recreated:?}");

    let result = json!({
        "cell": cell,
        "fabric_id": estate.fabric_id,
        "accepted_build_id": accepted,
        "provider": estate.owner.provider,
        "desired_shape": shape,
        "fabric_primary": primary,
        "killed": killed,
        "live_before": old.len(),
        "live_after": new.len(),
        "preserved_count": own_members.len() + 3,
        "not_reached": silent,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (node-identity-and-state.md 2.5: "a node-admin that holds the fabric-primary role and finds no
/// live node in its durable map adopts its own clock"): every runtime of the fabric is lost at once,
/// the admins and the ordinary members alike, so no node is left that holds rafka-time. Before the
/// loss the fabric-primary role is handed over as the cell above hands it. A person starts ONE node-admin on
/// the current fabric primary's data dir with both primary flags. It asks every node in its durable
/// map at once for rafka-time (fabric-recovery.md "Nothing alive": "If any node answers, it is not this
/// case"); none answers, so it adopts its own clock
/// (`rdm.mesh.entry.update.via-rafka-time-adopted{source=own-clock, reason=nothing-alive}`) within one
/// read's budget. The births the fabric then recreates adopt rafka-time from it.
/// What must NOT happen: a refusal to start, a node that adopts from anyone but the primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fabric_recovery_with_no_running_node_adopts_the_fabric_primarys_own_clock() {
    let cell = "fabric_recovery_with_no_running_node_adopts_the_fabric_primarys_own_clock";
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = [("mesh1", 2, 1), ("mesh2", 2, 1)];
    estate.reconcile_shape("fabric1", &shape).await;
    let before = values(&estate.settled_views(&shape).await);
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let former = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| location(&n)).expect("one fabric primary");
    let primary = hand_fabric_primary_over(&mut estate, &former).await;
    let primary_dir = if primary == Estate::path("mesh1.admin", "1") { estate.bootstrap_data_dir("mesh1").display().to_string() } else { estate.data_dir(&primary).await };

    // The fault: every runtime of the fabric, at once.
    for (_, pid) in estate.live_runtimes() {
        estate.kill_pid(pid);
    }
    estate.kill_bootstrap();
    assert!(estate.live_runtimes().is_empty(), "no runtime of the fabric is left: {:?}", estate.live_runtimes());

    let restarted = estate.restart_admin_with(std::path::Path::new(&primary_dir), &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    estate.enter(&restarted).await;
    let after = estate.settles_to(&shape, Duration::from_secs(240)).await;
    assert_eq!(after.len(), 6);

    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let primary_id = after.iter().find(|n| n.name == primary).map(|n| n.node_id.to_string()).expect("the restarted admin in the view");
    let started: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-recovery-start").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &primary)).collect();
    let t0 = started.iter().map(|sp| at(sp)).min().expect("the person-started admin recorded its recovery start");
    let own: Vec<&Value> = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted")
        .into_iter()
        .filter(|sp| attr(sp, "node") == primary_id && at(sp) >= t0 && attr(sp, "source") == "own-clock")
        .collect();
    assert_eq!(own.len(), 1, "the re-born fabric primary adopted its own clock once: {own:?}");
    assert_eq!(attr(own[0], "reason"), "nothing-alive");
    let pulls: Vec<&Value> = named(&spans, "rdm.mesh.entry.resolve.via-recovery-pull").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &primary) && at(sp) >= t0).collect();
    assert!(pulls.len() == 1 && attr(pulls[0], "outcome") == "no-live-node", "one read of every node the store names, none answering, before the own clock: {pulls:?}");
    let others: Vec<String> = named(&spans, "rdm.mesh.entry.update.via-rafka-time-adopted")
        .into_iter()
        .filter(|sp| at(sp) >= t0 && attr(sp, "source") == "pull" && attr(sp, "node") != primary_id)
        .map(|sp| attr(sp, "served_by"))
        .collect();
    assert!(!others.is_empty(), "the births the fabric recreated adopted rafka-time");
}

/// CONTRACT (node-stop.md, Entry: "Live exact `(node.id, incarnation)`", Status: "stopped with a live
/// process"; node-rpc-envelope.md, Workflow: "A later attempt resumes from the recorded receipts, never
/// from step 1, and validates those receipts against the accepted operation and exact births before it
/// reuses them"): a Build of steps stops `mesh1.rpc.1` (its `Complete` receipt durable: stopped, its
/// process alive) and then creates `mesh1.rpc.3`; every runtime of the fabric is lost inside that
/// Build, the create's first receipt held by the faulted fabric-primary's door. The fabric-primary,
/// restarted on its own data dir as both primaries, resumes the Build and completes it. The stopped
/// node's runtime exited with the fault: the completed stop's receipt is not taken into any view after
/// the restart (no NodeStopped of it), and it is recovered (node-recovery.md step 3): node.delete of the
/// exited birth, then node.create at its path.name under a new node id, and the view settles.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_stopped_inside_a_build_the_fabric_lost_is_recovered_never_recorded_stopped() {
    let cell = "a_node_stopped_inside_a_build_the_fabric_lost_is_recovered_never_recorded_stopped";
    let sha = rdm_test::faults::candidate_sha();
    let mut estate = Estate::bootstrap_on(owner(cell), "fabric1", "mesh1", &rdm_test::faults::binding_set(&sha), &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2)]).await;
    let before = values(&estate.settled_views(&[("mesh1", 2, 2)]).await);
    estate.until_fabric_ready(Duration::from_secs(60)).await;
    let fp = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| location(&n)).expect("one fabric primary");
    assert_eq!(fp, Estate::path("mesh1.admin", "1"), "the bootstrap admin holds the fabric-primary role: {before:#?}");
    let stopped = before.iter().find(|n| location(n) == Estate::path("mesh1.rpc", "1")).cloned().expect("mesh1.rpc.1");
    let stopped_id = s(&stopped["node_id"]);

    // The fabric-primary's door holds the create's first receipt: the stop before it is complete and
    // durable, and the Build's attempt is open.
    let door = rdm_test::faults::Door::at(&estate, &fp).await;
    let cut = "steps:create";
    door.arm(cut, json!({"kind": "receipt", "step": "PrepareStorage", "operation": "node.create:mesh1.rpc.3"})).await;
    let rpc1: rdm_client::PathName = Estate::path("mesh1.rpc", "1");
    let stream = estate.nodes_obj().await.steps(vec![rdm_core::build::Step::Stop { node: rpc1 }, rdm_core::build::Step::Create { mesh: "mesh1".into(), kind: rdm_core::NodeKind::RpcNode, node: Estate::path("mesh1.rpc", "3"), executable: Some(estate.executables.of(rdm_client::LaunchKind::RpcNode)) }]).await.unwrap_or_else(|e| panic!("the Build is accepted: {e}"));
    let held = door.wait_held(cut).await;

    // The fault: every runtime of the fabric, at once, inside the Build.
    for (_, pid) in estate.live_runtimes() {
        estate.kill_pid(pid);
    }
    estate.kill_bootstrap();
    drop(stream);
    assert!(estate.live_runtimes().is_empty(), "no runtime of the fabric is left: {:?}", estate.live_runtimes());

    let fp_dir = estate.bootstrap_data_dir("mesh1");
    let restarted = estate.restart_admin_with(&fp_dir, &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    estate.enter(&restarted).await;
    let after = values(&estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(240)).await);
    let back = after.iter().find(|n| location(n) == Estate::path("mesh1.rpc", "1")).cloned().unwrap_or_else(|| panic!("mesh1.rpc.1 is in the settled view: {after:#?}"));
    assert_ne!(s(&back["node_id"]), stopped_id, "mesh1.rpc.1 is re-created by recovery under a new node id: {back}");

    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let t0 = named(&spans, "rdm.node_admin.node.update.via-recovery-start").into_iter().filter(|sp| rdm_test::estate::at_location(sp, &fp)).map(|sp| at(sp)).min().expect("the restarted fabric-primary recorded its recovery start");
    let taken: Vec<&Value> = named(&spans, "rdm.mesh.topology.remove.via-node-stopped").into_iter().filter(|sp| at(sp) >= t0 && attr(sp, "node_id") == stopped_id).collect();
    assert!(taken.is_empty(), "no view takes a NodeStopped of the exited birth after the restart: {taken:#?}; the cut held {held}");
    let deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| at(sp) >= t0 && attr(sp, "node_id") == stopped_id).collect();
    assert_eq!(deleted.len(), 1, "the exited birth is recovered: node.delete of it once, then node.create at its path.name: {deleted:#?}");
}
