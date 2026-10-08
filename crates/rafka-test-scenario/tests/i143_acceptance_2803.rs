//! i143.e4.s10 acceptance (rafka-v2 #2803, hardened 2026-10-07), CHAOS-PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2803-chaos-process`, which exports `I143_ACCEPTANCE_DIR`
//! (this cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the
//! estate's manifest, rpc ledger and every process's spans, feature `i143-2803`).
//!
//! Node-admin cohort loss and recovery through the Build rectifier. The fabric seat is never in
//! the lost mesh: the cohort lost is the one whose admins do not hold it (a fabric-primary's mesh is
//! only ever lost after its authority was handed off; that is a separate cell).

use rafka_test_scenario::estate::{claim_decider, descends_from, named, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::process::Command;
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

/// CONTRACT (#2803 acceptance 2, 13, 14, 15, 16, 19): a mesh's entire node-admin cohort dies (exact
/// SIGKILL of both admin runtimes, the members untouched). The fabric primary proves the exact
/// runtimes exited and opens the next attempt of the SAME accepted Build (no new topology Build);
/// one recovery admin is born under the same MeshId, hydrates, receives Pending (Applied) before
/// any reconciliation of its mesh, creates only the missing capacity (the sibling admin) and
/// preserves every surviving member birth (same incarnation, never recreated); the recovery admin
/// is not elected primary by executing Pending; the other mesh is never touched. What must NOT
/// happen: a new MeshId, a new Build id, a surviving member re-created, or a member create before
/// Pending.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_admin_restores_lost_cohort_preserves_mesh_and_build() {
    let cell = "recovery_admin_restores_lost_cohort_preserves_mesh_and_build";
    let dir = acceptance_dir("chaos-process", cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let shape = json!({"fabric": "fabric1", "meshes": [
        {"name": "mesh1", "node_admin": 2, "rpc_node": 2},
        {"name": "mesh2", "node_admin": 2, "rpc_node": 2},
    ]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    let accepted = s(&a["build_id"]);
    estate.await_build(&accepted, Duration::from_secs(120)).await;
    let want = names(&[("mesh1", 2, 2), ("mesh2", 2, 2)]);
    let before = estate.settled(&want, Duration::from_secs(30)).await;

    // The lost mesh: the one whose admins do not hold the fabric seat (never the bootstrap's own
    // host process, which is a mesh1 admin).
    let fabric_holder = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let lost = if fabric_holder.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    if lost == "mesh1" {
        // mesh1's first admin is the bootstrap host process; its loss is the estate's to make.
        assert!(!fabric_holder.starts_with("mesh1."), "the fabric seat is never in the lost mesh");
    }
    let (_, lost_view) = estate.get(&format!("/api/meshes/{lost}")).await;
    let lost_mesh_id = s(&lost_view["id"]);
    let members: Vec<String> = (1..=2).map(|i| format!("{lost}.rpc.{i}")).collect();
    let (_, fabric) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric["build_id"]), accepted, "the accepted Build names the topology");
    let (_, b0) = estate.get(&format!("/api/builds?id={accepted}")).await;
    let attempt_before = b0["attempt"].as_u64().unwrap_or(0);
    let lost_births: BTreeMap<String, String> = births(&before).into_iter().filter(|(n, _)| n.starts_with(&format!("{lost}.admin."))).collect();

    // Control goes through the fabric primary's advertised API, which the fault never touches.
    estate.admin = before.iter().find(|n| s(&n["name"]) == fabric_holder).map(|n| s(&n["admin_api_base"])).filter(|b| !b.is_empty()).expect("the fabric primary advertises its control API");

    // The fault: exact SIGKILL of the lost mesh's admin runtimes, its members untouched.
    let mut killed = Vec::new();
    for (path, pid) in estate.live_runtimes() {
        let name = path.file_name().unwrap().to_string_lossy().to_string();
        if name.starts_with(&format!("{lost}.admin.")) {
            assert!(Command::new("kill").args(["-9", &pid.to_string()]).status().unwrap().success());
            killed.push(json!({"node": name, "pid": pid}));
        }
    }
    if lost == "mesh1" {
        estate.kill_bootstrap();
        killed.push(json!({"node": "mesh1.admin.1", "pid": "bootstrap"}));
    }
    assert_eq!(killed.len(), 2, "both admins of {lost} were lost: {killed:?}");

    // Recovery through the rectifier: the same Build, a later attempt, the cohort back.
    // Recovered only when the accepted Build completed a LATER attempt than before the fault and
    // both lost admins are new births in the fabric primary's view.
    let done = wait_for("the accepted Build completes a later attempt and the lost cohort is reborn", Duration::from_secs(180), || {
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
        if name.starts_with(&format!("{lost}.admin.")) {
            assert_ne!(&new[name], inc, "{name}: a new birth (its runtime was lost)");
        } else {
            assert_eq!(&new[name], inc, "{name}: preserved, never recreated");
        }
    }
    let lost_primary = after.iter().find(|n| s(&n["mesh"]) == lost && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("the recovered cohort elects a primary");

    let (_, fabric) = estate.get("/api/fabric").await;
    estate.admin = s(&fabric["admin_api_base"]);
    estate.stop().await;
    let spans = estate.spans();
    // Pending applied at the recovery admin before any reconciliation of its mesh's members.
    let handoffs: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|h| h["attributes"]["mesh"] == lost).collect();
    let applied: Vec<&&Value> = handoffs.iter().filter(|h| h["attributes"]["outcome"] == "applied" || h["attributes"]["outcome"] == "already-applied").collect();
    let recovery_pending = applied.iter().max_by_key(|h| h["start_unix_nano"].as_u64().unwrap_or(0)).expect("Pending handed to the recovery admin");
    let recovery_admin = s(&recovery_pending["attributes"]["target"]);
    assert_eq!(s(&recovery_pending["attributes"]["mesh_id"]), lost_mesh_id, "Pending names the same MeshId");
    let pending_end = recovery_pending["end_unix_nano"].as_u64().unwrap_or(0);
    // No surviving member was created again under the accepted Build after the loss.
    let recreated: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| c["attributes"]["build_id"] == accepted.as_str() && members.contains(&s(&c["attributes"]["node"])) && c["start_unix_nano"].as_u64().unwrap_or(0) >= pending_end)
        .collect();
    assert!(recreated.is_empty(), "surviving members are never recreated: {recreated:?}");
    // The sibling admin is created only after Pending, by the recovery attempt.
    let admin_creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| c["attributes"]["build_id"] == accepted.as_str() && s(&c["attributes"]["node"]).starts_with(&format!("{lost}.admin.")) && c["start_unix_nano"].as_u64().unwrap_or(0) > 0)
        .collect();
    let after_pending: Vec<&&Value> = admin_creates.iter().filter(|c| c["start_unix_nano"].as_u64().unwrap_or(0) >= pending_end).collect();
    assert!(!after_pending.is_empty(), "the missing sibling admin is created after Pending: {admin_creates:?}");
    // Every reconcile of the accepted Build descends from its REST request.
    let rest = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == accepted.as_str()).cloned().expect("the accepted Build's request");
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| r["attributes"]["build_id"] == accepted.as_str()).collect();
    // The attempt context lives with the fabric primary that accepted the Build (R-X1): an attempt
    // that primary decided descends from the request; one decided after the seat moved starts its
    // own trace.
    let accepting = claim_decider(&spans, accepted.as_str(), "1").expect("attempt 1 of the accepted Build was claimed");
    for r in &reconciles {
        let attempt = s(&r["attributes"]["attempt"]);
        if r["attributes"]["reason"] != "requested" {
            continue; // a proven-drift attempt is rooted at the drift span that opened it
        }
        if claim_decider(&spans, accepted.as_str(), &attempt).is_none_or(|d| d == accepting) {
            assert!(descends_from(&spans, r, &rest), "an attempt the accepting fabric primary decided descends from the request: {r}");
        } else {
            assert_eq!(r["parent_span_id"].as_str().unwrap_or(""), "", "an attempt decided after the seat moved starts its own trace: {r}");
        }
    }
    // Accepting Pending is not an election.
    let decided: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|d| s(&d["attributes"]["node"]) == recovery_admin && d["attributes"]["op"] == "apply-mesh-state")
        .collect();
    assert!(decided.iter().all(|d| d["attributes"]["receiver_is_primary"] == "false"), "{decided:?}");

    // The recovery admin is launched with a live member of its mesh among its seeds (the launcher
    // alone is a node of another mesh): it joins its mesh channel through more than the launcher.
    let subscribed: Vec<&Value> = named(&spans, "rdm.mesh.membership.update.via-subscribe")
        .into_iter()
        .filter(|sp| s(&sp["attributes"]["node"]) == recovery_admin && s(&sp["attributes"]["channel"]) == format!("mesh:{lost}"))
        .collect();
    assert!(
        subscribed.iter().any(|sp| sp["attributes"]["peers"].as_str().and_then(|p| p.parse::<u64>().ok()).unwrap_or(0) >= 2),
        "{recovery_admin} joined mesh:{lost} through its launcher alone: {subscribed:?}"
    );

    let preserved = old.keys().filter(|n| !n.starts_with(&format!("{lost}.admin.")) && n.starts_with(&format!("{lost}."))).count();
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
        "pending_natural_key": recovery_pending["attributes"]["key"],
        "provider": estate.owner.provider,
        "desired_shape": shape,
        "live_before": old.len(),
        "preserved_count": preserved,
        "created_count": after_pending.len(),
        "recreated_members": recreated.len(),
        "live_after": new.len(),
        "lost_mesh_primary_after": lost_primary,
        "reconciles": reconciles.len(),
        "trace_id": rest["trace_id"],
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
