//! i143.e4.s10 acceptance (rafka-v2 #2803, hardened 2026-10-07), CHAOS-PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2803-chaos-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest, rpc ledger and every process's spans, feature `i143-2803`).
//!
//! Node-admin cohort loss and recovery through the Build rectifier. The fabric seat is never in
//! the lost mesh: the cohort lost is the one whose admins do not hold it (a fabric-primary's mesh is
//! only ever lost after its authority was handed off; that is a separate cell).

use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
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

fn names(meshes: &[(&str, u32, u32)]) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    for (m, a, r) in meshes {
        out.extend((1..=*a).map(|i| format!("{m}.admin.{i}")));
        out.extend((1..=*r).map(|i| format!("{m}.rpc.{i}")));
    }
    out
}

/// name -> incarnation
fn births(nodes: &[Value]) -> BTreeMap<String, String> {
    nodes.iter().map(|n| (s(&n["name"]), s(&n["incarnation_id"]))).collect()
}

/// CONTRACT (#2803 acceptance 2, 3, 10, 13, 14, 15, 16, 19; node-admin-lifecycle.md 4.1-4.3): a peer
/// mesh loses its entire node-admin cohort (exact SIGKILL of both admin runtimes) and, while it has
/// no admin, one ordinary member dies as well (`rpc.3`). The fabric primary proves the admins exited
/// and opens an attempt of the SAME accepted Build that runs the mesh-create flow for the lost
/// mesh's first admin with its EXISTING MeshId (no id is minted). That admin calls its maker
/// (JoinNode), reads a local member's topology, becomes Ready and sweeps its own mesh once with a
/// Ping: the two members that live answer, the one that died does not. The unreached member goes
/// through the standard decommission (its runtime inspected as exited, `NodeDeleted` on that proof)
/// and is replaced under the same Build; the second admin is created by the NEW mesh primary. What
/// must NOT happen: a new MeshId or Build id, a surviving member re-created, a member that answered
/// decommissioned, the fabric primary creating the second admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_admin_restores_lost_cohort_preserves_mesh_and_build() {
    let cell = "recovery_admin_restores_lost_cohort_preserves_mesh_and_build";
    let dir = acceptance_dir("chaos-process", cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 3},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 3},
    ]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    let accepted = s(&a["build_id"]);
    estate.await_attempt(&accepted, Estate::attempt_of(&a), Duration::from_secs(120)).await;
    let want = names(&[("mesh1", 2, 3), ("mesh2", 2, 3)]);
    let before = estate.settled(&want, Duration::from_secs(30)).await;

    // The lost mesh: the one whose admins do not hold the fabric seat.
    let fabric_holder = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let lost = if fabric_holder.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let (_, lost_view) = estate.get(&format!("/api/meshes/{lost}")).await;
    let lost_mesh_id = s(&lost_view["id"]);
    let window_victim = format!("{lost}.rpc.3");
    let survivors: Vec<String> = (1..=2).map(|i| format!("{lost}.rpc.{i}")).collect();
    let (_, fabric) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric["build_id"]), accepted, "the accepted Build names the topology");
    let (_, b0) = estate.get(&format!("/api/builds?id={accepted}")).await;
    let attempt_before = b0["attempt"].as_u64().unwrap_or(0);
    let lost_births: BTreeMap<String, String> = births(&before).into_iter().filter(|(n, _)| n.starts_with(&format!("{lost}.admin.")) || *n == window_victim).collect();

    // Control goes through the fabric primary's advertised API, which the fault never touches.
    estate.admin = before.iter().find(|n| s(&n["name"]) == fabric_holder).map(|n| s(&n["admin_api_base"])).filter(|b| !b.is_empty()).expect("the fabric primary advertises its control API");

    // The fault: both admins at once, then, with the mesh admin-less, one ordinary member.
    let mut killed = Vec::new();
    for (path, pid) in estate.live_runtimes() {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.starts_with(&format!("{lost}.admin.")) {
            estate.kill_pid(pid);
            killed.push(json!({"node": name, "pid": pid}));
        }
    }
    if lost == "mesh1" {
        estate.kill_bootstrap();
        killed.push(json!({"node": "mesh1.admin.1", "pid": "bootstrap"}));
    }
    assert_eq!(killed.len(), 2, "both admins of {lost} were lost: {killed:?}");
    for (path, pid) in estate.live_runtimes() {
        if path.file_name().unwrap().to_string_lossy().starts_with(&format!("{window_victim}-")) {
            estate.kill_pid(pid);
            killed.push(json!({"node": window_victim, "pid": pid}));
        }
    }
    assert_eq!(killed.len(), 3, "the member died in the admin-less window: {killed:?}");

    // Recovery through the rectifier: the same Build, later attempts, the cohort and the member back.
    let done = wait_for("the accepted Build is complete past the fault and the lost births are reborn", Duration::from_secs(240), || {
        let estate = &estate;
        let (accepted, lost_births) = (accepted.clone(), lost_births.clone());
        async move {
            let (_, b) = estate.get(&format!("/api/builds?id={accepted}")).await;
            let later = b["state"] == "complete" && b["attempt"].as_u64().unwrap_or(0) > attempt_before;
            let reborn = births(&estate.nodes().await).iter().filter(|(n, i)| lost_births.get(*n).is_some_and(|old| old != *i)).count() == lost_births.len();
            (later && reborn).then_some(b)
        }
    })
    .await;
    let after = estate.settled(&want, Duration::from_secs(60)).await;
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["build_id"]), accepted, "no new topology Build: Fabric.build_id is unchanged");
    let (_, lost_after) = estate.get(&format!("/api/meshes/{lost}")).await;
    assert_eq!(s(&lost_after["id"]), lost_mesh_id, "{lost} recovered under its own MeshId");
    let (old, new) = (births(&before), births(&after));
    for (name, inc) in &old {
        if lost_births.contains_key(name) {
            assert_ne!(&new[name], inc, "{name}: a new birth (its runtime was lost)");
        } else {
            assert_eq!(&new[name], inc, "{name}: preserved, never recreated");
        }
    }

    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);

    // The fabric primary ran the mesh-create flow for the lost mesh's first admin: Pending handed
    // to it with the EXISTING MeshId, before any member work.
    let handoffs: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff")
        .into_iter()
        .filter(|h| attr(h, "mesh") == lost && (attr(h, "outcome") == "applied" || attr(h, "outcome") == "already-applied"))
        .collect();
    let first = handoffs.iter().max_by_key(|h| at(h)).expect("Pending handed to the recovery admin");
    let recovery_admin = attr(first, "target");
    assert_eq!(attr(first, "mesh_id"), lost_mesh_id, "Pending names the same MeshId: it joined, it never minted");
    assert_eq!(attr(first, "node"), fabric_holder, "the fabric primary ran the first admin's birth");

    // Its own-mesh sweep: one, once, from the recovery admin; the members that live answered, the
    // one that died did not.
    let sweeps: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-entry-sweep").into_iter().filter(|sp| attr(sp, "node") == recovery_admin).collect();
    assert_eq!(sweeps.len(), 1, "one sweep on entering the mesh: {sweeps:?}");
    let silent: Vec<String> = named(&spans, "rdm.node_admin.node.reject.via-entry-sweep-no-reply").into_iter().filter(|sp| attr(sp, "sweeper") == recovery_admin).map(|sp| attr(sp, "node")).collect();
    let sibling = format!("{lost}.admin.{}", if recovery_admin.ends_with(".1") { 2 } else { 1 });
    for dead in [&window_victim, &sibling] {
        assert!(silent.contains(dead), "{dead} died and did not answer the sweep: {silent:?}");
    }
    for m in &survivors {
        assert!(!silent.contains(m), "{m} answered the sweep: {silent:?}");
    }
    // Not reached starts the standard decommission, one attempt per node in path order. The
    // sibling admin is retired by the pipeline (drain, terminate and inspect, NodeDeleted on the
    // proven exit) and created again; the member is replaced under the same Build, by this queue or
    // by the drift check that proves the same exit, whichever opened its attempt first.
    let decommissions: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-sweep-decommission").into_iter().filter(|sp| attr(sp, "sweeper") == recovery_admin).collect();
    for dead in [&window_victim, &sibling] {
        assert!(decommissions.iter().any(|sp| attr(sp, "node") == *dead), "{dead} entered the decommission queue: {decommissions:?}");
    }
    let sibling_steps: Vec<String> = named(&spans, "rdm.node_admin.deployment.update.via-step")
        .into_iter()
        .filter(|sp| attr(sp, "node") == sibling && attr(sp, "build_id") == accepted && attr(sp, "outcome") == "complete")
        .map(|sp| attr(sp, "step"))
        .collect();
    for step in ["MarkDraining", "TerminateRuntime", "NodeDeleted"] {
        assert!(sibling_steps.iter().any(|n| n == step), "the standard retire ran {step} for {sibling}: {sibling_steps:?}");
    }
    // The members that answered are never retired or created again.
    let touched: Vec<String> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .chain(named(&spans, "rdm.node_admin.node.delete.via-build"))
        .filter(|sp| attr(sp, "build_id") == accepted && survivors.contains(&attr(sp, "node")) && at(sp) > at(first))
        .map(|sp| attr(sp, "node"))
        .collect();
    assert!(touched.is_empty(), "members that answered are neither retired nor created again: {touched:?}");

    // The second admin was created by an attempt the NEW mesh primary executed, not the fabric primary.
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| attr(r, "build_id") == accepted).collect();
    let creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| attr(c, "build_id") == accepted && attr(c, "node").starts_with(&format!("{lost}.admin.")) && attr(c, "node") != recovery_admin && at(c) > at(first))
        .collect();
    assert!(!creates.is_empty(), "the second admin of {lost} was created after the first: {creates:?}");
    for c in &creates {
        let executor = reconciles.iter().find(|r| attr(r, "attempt") == attr(c, "attempt")).map(|r| attr(r, "executor")).expect("the create ran inside a reconcile");
        assert_eq!(executor, recovery_admin, "{} was created by the new mesh primary, not by {fabric_holder}", attr(c, "node"));
    }

    let result = json!({
        "cell": cell,
        "fabric_id": estate.fabric_id,
        "mesh_id": lost_mesh_id,
        "mesh_name": lost,
        "accepted_build_id": accepted,
        "attempt_before": attempt_before,
        "attempt": done["attempt"],
        "fabric_primary": fabric_holder,
        "killed": killed,
        "recovery_admin": recovery_admin,
        "pending_natural_key": first["attributes"]["key"],
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
/// one ordinary member of the fabric primary's mesh dies as well. A person starts ONE node-admin on
/// the former fabric primary's data dir with both primary flags. It is not Day 0: it comes back as
/// the same node (same NodeId, a new incarnation) with no maker, takes its durable map as the
/// topology, connects to a local node of its own mesh, sweeps its own mesh once with a Ping (the
/// dead sibling and the dead member do not answer), and sends them to the standard decommission.
/// The other mesh's admins are restored by the fabric under the same Build with their existing
/// MeshIds. What must NOT happen: a Day-0 start or a new Build or MeshId, a member that answered
/// created or retired again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fabric_recovery_restarts_one_admin_and_restores_every_missing_admin() {
    let cell = "fabric_recovery_restarts_one_admin_and_restores_every_missing_admin";
    let dir = acceptance_dir("chaos-process", cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 3},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 3},
    ]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    let accepted = s(&a["build_id"]);
    estate.await_attempt(&accepted, Estate::attempt_of(&a), Duration::from_secs(120)).await;
    let want = names(&[("mesh1", 2, 3), ("mesh2", 2, 3)]);
    let before = estate.settled(&want, Duration::from_secs(30)).await;
    let holder = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let holder_mesh = holder.split('.').next().unwrap().to_string();
    let holder_dir = if holder == "mesh1.admin.1" { estate.bootstrap_data_dir("mesh1").display().to_string() } else { estate.data_dir_of(&holder).await };
    let mesh_ids: BTreeMap<String, String> = {
        let mut m = BTreeMap::new();
        for mesh in ["mesh1", "mesh2"] {
            let (_, v) = estate.get(&format!("/api/meshes/{mesh}")).await;
            m.insert(mesh.to_string(), s(&v["id"]));
        }
        m
    };
    let window_victim = format!("{holder_mesh}.rpc.3");
    let own_members: Vec<String> = (1..=2).map(|i| format!("{holder_mesh}.rpc.{i}")).collect();
    let old = births(&before);
    let old_ids: BTreeMap<String, String> = before.iter().map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();

    // The fault: every node-admin, at once; then, with the fabric admin-less, one ordinary member.
    let mut killed = Vec::new();
    for (path, pid) in estate.live_runtimes() {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.contains(".admin.") {
            estate.kill_pid(pid);
            killed.push(json!({"node": name, "pid": pid}));
        }
    }
    estate.kill_bootstrap();
    killed.push(json!({"node": "mesh1.admin.1", "pid": "bootstrap"}));
    assert_eq!(killed.len(), 4, "all four node-admins were lost: {killed:?}");
    for (path, pid) in estate.live_runtimes() {
        if path.file_name().unwrap().to_string_lossy().starts_with(&format!("{window_victim}-")) {
            estate.kill_pid(pid);
            killed.push(json!({"node": window_victim, "pid": pid}));
        }
    }
    assert_eq!(killed.len(), 5, "the member died with no admin running: {killed:?}");

    // The person starts one node-admin on the former fabric primary's data dir.
    let base = estate.restart_admin_with(std::path::Path::new(&holder_dir), &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    estate.admin = base;
    let after = estate.settled(&want, Duration::from_secs(240)).await;

    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["build_id"]), accepted, "no new Build: Fabric.build_id is unchanged");
    for (mesh, id) in &mesh_ids {
        let (_, v) = estate.get(&format!("/api/meshes/{mesh}")).await;
        assert_eq!(&s(&v["id"]), id, "{mesh} recovered under its own MeshId");
    }
    let new = births(&after);
    let new_ids: BTreeMap<String, String> = after.iter().map(|n| (s(&n["name"]), s(&n["node_id"]))).collect();
    for (name, inc) in &old {
        if name.contains(".admin.") || *name == window_victim {
            assert_ne!(&new[name], inc, "{name}: a new birth");
            if *name == holder {
                assert_eq!(new_ids[name], old_ids[name], "{name}: the restarted admin is the same node");
            }
        } else {
            assert_eq!(&new[name], inc, "{name}: an ordinary member that lives is preserved, never re-created");
        }
    }

    estate.stop().await;
    let spans = estate.spans();
    let attr = |sp: &Value, k: &str| s(&sp["attributes"][k]);
    let at = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let started: Vec<&Value> = named(&spans, "rdm.node_admin.node.update.via-recovery-start").into_iter().filter(|sp| attr(sp, "node") == holder).collect();
    assert!(
        started.iter().any(|sp| sp["attributes"]["mesh_primary"] == "true" && sp["attributes"]["fabric_primary"] == "true"),
        "the person-started admin recorded both recovery flags: {started:?}"
    );
    let t0 = started.iter().map(|sp| at(sp)).min().unwrap_or(0);
    // No maker: its topology is its durable map; one sweep of its own mesh, once.
    let sweeps: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-entry-sweep").into_iter().filter(|sp| attr(sp, "node") == holder && at(sp) >= t0).collect();
    assert_eq!(sweeps.len(), 1, "one sweep on entering its mesh: {sweeps:?}");
    assert_eq!(attr(sweeps[0], "source"), "durable-map", "a reborn fabric primary has no maker");
    let silent: Vec<String> = named(&spans, "rdm.node_admin.node.reject.via-entry-sweep-no-reply").into_iter().filter(|sp| attr(sp, "sweeper") == holder && at(sp) >= t0).map(|sp| attr(sp, "node")).collect();
    let sibling = format!("{holder_mesh}.admin.{}", if holder.ends_with(".1") { 2 } else { 1 });
    for dead in [&window_victim, &sibling] {
        assert!(silent.contains(dead), "{dead} died and did not answer the sweep: {silent:?}");
    }
    for m in &own_members {
        assert!(!silent.contains(m), "{m} answered the sweep: {silent:?}");
    }
    let recreated: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| at(c) >= t0 && own_members.contains(&attr(c, "node")))
        .collect();
    assert!(recreated.is_empty(), "no member that answered is created again: {recreated:?}");

    let result = json!({
        "cell": cell,
        "fabric_id": estate.fabric_id,
        "accepted_build_id": accepted,
        "provider": estate.owner.provider,
        "desired_shape": shape,
        "fabric_primary": holder,
        "killed": killed,
        "live_before": old.len(),
        "live_after": new.len(),
        "preserved_count": own_members.len() + 3,
        "not_reached": silent,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
