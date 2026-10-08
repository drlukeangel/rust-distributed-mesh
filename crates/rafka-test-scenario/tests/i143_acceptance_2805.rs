//! i143.e4.s11 acceptance (rafka-v2 #2805, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2805-process`, which exports `I143_ACCEPTANCE_DIR` (this
//! cell's `result.json` goes there) and whose command sets `RDM_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans land under it, feature `i143-2805`).
//!
//! A real two-mesh birth through the Build rectifier: `POST /api/build` adds mesh2; the fabric
//! primary births mesh2's bootstrap admin, which hydrates the Fabric pointer and its Build, then
//! receives MeshStatus::Pending from the fabric primary, and only then does any of mesh2's own
//! membership get created; election alone supplies mesh2's primary afterwards.

use rafka_test_scenario::estate::{claim_decider, descends_from, named, Estate, Owner};
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

/// CONTRACT (#2805): mesh2's bootstrap admin is born, hydrates the Fabric pointer and the complete
/// accepted Build (its entry join), and only then receives MeshStatus::Pending from the fabric
/// primary, applied once through its status door without making it the elected primary; no create
/// of mesh2's own membership starts before that Pending is applied; mesh2's primary is supplied by
/// election afterwards; every attempt of the creation Build descends from its REST request. What
/// must NOT happen: a member of mesh2 created before Pending, a second Pending applied, or the
/// bootstrap admin taking a seat by accepting Pending.
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
    let mesh = |m: &str| json!({"name": m, "node_admin": 2, "rpc_node": 2});
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1")]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(&s(&a["build_id"]), Duration::from_secs(120)).await;
    // {mesh1} -> {mesh1, mesh2}, through the rectifier.
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1"), mesh("mesh2")]})).await;
    assert_eq!(status, 202, "{a}");
    let create = s(&a["build_id"]);
    let build = estate.await_build(&create, Duration::from_secs(120)).await;
    let want: std::collections::BTreeSet<String> =
        ["mesh1", "mesh2"].iter().flat_map(|m| (1..=2).map(move |i| format!("{m}.admin.{i}")).chain((1..=2).map(move |i| format!("{m}.rpc.{i}")))).collect();
    let nodes = estate.settled(&want, Duration::from_secs(30)).await;
    let mesh2_primary = nodes.iter().find(|n| n["mesh"] == "mesh2" && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("mesh2 elects a primary");
    let fabric_primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    // The provider every mesh2 member actually ran on, from the view (and, on containers, its
    // immutable container id).
    let provider = estate.owner.provider.clone();
    let mesh2_runtimes: Vec<Value> = nodes
        .iter()
        .filter(|n| n["mesh"] == "mesh2" && n["kind"] != "node_admin")
        .map(|n| json!({"node": n["name"], "provider": n["provider"], "container": estate.container_of(&s(&n["name"]))}))
        .collect();
    for r in &mesh2_runtimes {
        assert_eq!(r["provider"], provider.as_str(), "{r}");
        if provider == "container" {
            assert_eq!(r["container"].as_str().map(str::len), Some(64), "an immutable container id: {r}");
        }
    }
    estate.stop().await;
    let spans = estate.spans();

    // The bootstrap admin hydrated first: its entry join and the accepted Build it then holds.
    let joined = named(&spans, "rdm.node_admin.fabric.update.via-join").into_iter().find(|j| j["attributes"]["node"] == "mesh2.admin.1").cloned().expect("mesh2.admin.1 joined by entry pull");
    // The Pending hand-off, applied exactly once at that birth.
    let handoffs: Vec<Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|h| h["attributes"]["mesh"] == "mesh2").cloned().collect();
    let applied: Vec<&Value> = handoffs.iter().filter(|h| h["attributes"]["outcome"] == "applied").collect();
    assert_eq!(applied.len(), 1, "one Pending applied at mesh2's bootstrap admin: {handoffs:?}");
    let handoff = applied[0];
    assert_eq!(handoff["attributes"]["target"], "mesh2.admin.1");
    assert!(end(&joined) <= start(handoff), "the target hydrated ({}) before Pending was handed to it ({})", end(&joined), start(handoff));
    // Decided at the target through its status door, not as an election.
    let decided: Vec<&Value> = named(&spans, "rdm.node_admin.status.update.via-declaration")
        .into_iter()
        .filter(|d| d["attributes"]["node"] == "mesh2.admin.1" && d["attributes"]["op"] == "apply-mesh-state" && d["attributes"]["outcome"] == "applied")
        .collect();
    assert_eq!(decided.len(), 1, "{decided:?}");
    assert_eq!(decided[0]["attributes"]["receiver_is_primary"], "false", "accepting Pending is not an election");
    // Nothing of mesh2's own membership was created before Pending was applied.
    let members = ["mesh2.admin.2", "mesh2.rpc.1", "mesh2.rpc.2"];
    let member_creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| c["attributes"]["build_id"] == create.as_str() && members.contains(&c["attributes"]["node"].as_str().unwrap_or("")))
        .collect();
    assert_eq!(member_creates.len(), members.len(), "each member created once under the creation Build: {member_creates:?}");
    let first_member_create = member_creates.iter().map(|c| start(c)).min().unwrap();
    assert!(end(handoff) <= first_member_create, "Pending applied ({}) before mesh2's first own create ({first_member_create})", end(handoff));
    // The bootstrap admin's Ready follows its hydration; mesh2's primary is elected afterwards.
    let ready = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|r| r["attributes"]["node"] == "mesh2.admin.1").cloned().expect("mesh2.admin.1 committed Ready");
    assert!(end(&joined) <= start(&ready), "Ready after hydration");
    // The elected mesh primary owns the Mesh's later lifecycle: it declares the Mesh's state to the
    // fabric primary, which applies it; and the fabric primary applies the fabric's readiness at
    // the mesh primary. Both come after the elected primary's own Ready.
    let elected_ready = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|r| r["attributes"]["node"] == mesh2_primary.as_str()).cloned().expect("the elected primary committed Ready");
    let mesh_declared: Vec<&Value> = named(&spans, DECLARATION)
        .into_iter()
        .filter(|d| d["attributes"]["op"] == "declare-mesh-state" && d["attributes"]["sender"] == mesh2_primary.as_str() && d["attributes"]["node"] == fabric_primary.as_str())
        .collect();
    assert!(mesh_declared.iter().any(|d| d["attributes"]["outcome"] == "applied"), "the elected primary's Mesh declaration was applied by the fabric primary: {mesh_declared:?}");
    assert!(mesh_declared.iter().all(|d| start(d) >= end(&elected_ready)), "a Mesh declaration comes from a primary that is already Ready: {mesh_declared:?}");
    assert!(mesh_declared.iter().all(|d| matches!(d["attributes"]["outcome"].as_str(), Some("applied" | "already-applied"))), "{mesh_declared:?}");
    let fabric_event: Vec<&Value> = named(&spans, DECLARATION)
        .into_iter()
        .filter(|d| d["attributes"]["op"] == "apply-fabric-event" && d["attributes"]["sender"] == fabric_primary.as_str() && d["attributes"]["node"] == mesh2_primary.as_str() && d["attributes"]["outcome"] == "applied")
        .collect();
    assert_eq!(fabric_event.len(), 1, "the fabric's readiness applied once at mesh2's elected primary: {fabric_event:?}");
    // Every attempt the accepting fabric-primary decides descends from the REST request (the
    // rectifier); an attempt decided after the seat moved starts its own trace.
    let accepted = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == create.as_str()).cloned().expect("the creation Build's REST span");
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| r["attributes"]["build_id"] == create.as_str()).collect();
    assert!(!reconciles.is_empty(), "the creation Build was reconciled");
    let accepting = claim_decider(&spans, create.as_str(), "1").expect("attempt 1 of the creation Build was claimed");
    for r in &reconciles {
        if claim_decider(&spans, create.as_str(), r["attributes"]["attempt"].as_str().unwrap_or_default()).is_none_or(|d| d == accepting) {
            assert!(descends_from(&spans, r, &accepted), "every attempt the accepting fabric-primary decided descends from its request: {r}");
        } else {
            assert_eq!(r["parent_span_id"].as_str().unwrap_or(""), "", "an attempt decided after the seat moved starts its own trace: {r}");
        }
    }

    let result = json!({
        "cell": cell,
        "build_id": create,
        "attempt": build["attempt"],
        "executor": build["executor"],
        "mesh2_primary": mesh2_primary,
        "provider": provider,
        "mesh2_runtimes": mesh2_runtimes,
        "joined": {"span_id": joined["span_id"], "end": end(&joined), "served_by": joined["attributes"]["joined"]},
        "handoff": {"span_id": handoff["span_id"], "trace_id": handoff["trace_id"], "start": start(handoff), "end": end(handoff), "attributes": handoff["attributes"]},
        "handoffs_seen": handoffs.len(),
        "decided": decided[0]["attributes"],
        "first_member_create": first_member_create,
        "mesh_declarations": mesh_declared.iter().map(|d| json!({"span_id": d["span_id"], "outcome": d["attributes"]["outcome"], "start": start(d)})).collect::<Vec<_>>(),
        "fabric_event": fabric_event[0]["span_id"],
        "elected_ready_end": end(&elected_ready),
        "ready": {"span_id": ready["span_id"], "start": start(&ready)},
        "reconciles": reconciles.len(),
        "trace_id": accepted["trace_id"],
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (#2805, acceptance 8): the sole node-admin of a peer mesh is restarted through the
/// Build rectifier (never the fabric-primary): it comes back as the same node with a new
/// incarnation and reaches Ready without a fabric-primary hand-off, because the Pending it was
/// applied before the restart is its own durable status row, folded at boot. Ready is never
/// granted by election authority it lacks; mesh2's primary is the restarted admin only through
/// the ordinary election afterwards. What must NOT happen: the restarted admin stuck holding its
/// Ready for a Pending nobody will re-apply, or a second Pending hand-off applied for the same
/// MeshId, or a replacement MeshId.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sole_peer_admin_restarts_ready_from_its_durable_pending() {
    let cell = "sole_peer_admin_restarts_ready_from_its_durable_pending";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let mesh = |m: &str, admins: u32| json!({"name": m, "node_admin": admins, "rpc_node": 1});
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1", 2)]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(&s(&a["build_id"]), Duration::from_secs(120)).await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [mesh("mesh1", 2), mesh("mesh2", 1)]})).await;
    assert_eq!(status, 202, "{a}");
    let create = s(&a["build_id"]);
    estate.await_build(&create, Duration::from_secs(120)).await;
    let want: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh2.admin.1", "mesh2.rpc.1"].iter().map(|s| s.to_string()).collect();
    estate.settled(&want, Duration::from_secs(30)).await;
    let before = estate.node("mesh2.admin.1").await;

    // The restart, through the rectifier: an attempt of the accepted Build.
    let (status, restart) = estate.post("/api/nodes/mesh2.admin.1/restart", &json!({})).await;
    assert_eq!(status, 202, "restart route: {restart}");
    let restart_build = s(&restart["build_id"]);
    estate.await_attempt(&restart_build, Estate::attempt_of(&restart), Duration::from_secs(120)).await;
    let after = estate.settled(&want, Duration::from_secs(60)).await;
    let after = after.iter().find(|n| n["name"] == "mesh2.admin.1").cloned().expect("mesh2.admin.1 in the view");
    estate.stop().await;
    let spans = estate.spans();

    assert_eq!(after["node_id"], before["node_id"], "the same node");
    assert_ne!(after["incarnation_id"], before["incarnation_id"], "a new incarnation");
    assert_eq!(after["status"], "ready-for-traffic", "{after}");
    let readies: Vec<&Value> = named(&spans, "rdm.mesh.node.update.via-ready").into_iter().filter(|r| r["attributes"]["node"] == "mesh2.admin.1").collect();
    assert_eq!(readies.len(), 2, "Ready at birth and again after the restart: {readies:?}");
    let handoffs: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|h| h["attributes"]["mesh"] == "mesh2").collect();
    let mesh_ids: std::collections::BTreeSet<String> = handoffs.iter().map(|h| s(&h["attributes"]["mesh_id"])).collect();
    assert_eq!(mesh_ids.len(), 1, "one MeshId for mesh2 in every hand-off, none minted: {mesh_ids:?}");
    let blocked: Vec<&Value> = named(&spans, "rdm.node_admin.runtime.reject.via-not-authority-capable")
        .into_iter()
        .filter(|b| b["attributes"]["node"] == "mesh2.admin.1" && s(&b["attributes"]["detail"]).contains("Pending has not been applied"))
        .collect();
    let result = json!({
        "cell": cell,
        "restart_build": restart_build,
        "before_incarnation": before["incarnation_id"],
        "after_incarnation": after["incarnation_id"],
        "ready_spans": readies.len(),
        "handoff_spans": handoffs.len(),
        "mesh_ids": mesh_ids,
        "held_for_pending_spans": blocked.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (#2805, acceptance 2): a peer mesh's only node-admin is lost (its runtime killed
/// exactly; mesh2 holds no fabric seat) and the Build rectifier recovers it as an attempt of the
/// same Build: the fabric primary hands Pending to the recovery admin with the mesh's EXISTING
/// MeshId, applied or already-applied, before that admin's Ready, and the Mesh still has the id
/// it was born with. What must NOT happen: a second MeshId for mesh2 in any hand-off or in the
/// mesh's own view, the recovery admin Ready before Pending was certain, or a new Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_admin_receives_pending_under_the_existing_mesh_id() {
    let cell = "recovery_admin_receives_pending_under_the_existing_mesh_id";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let mut estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let mesh = |m: &str, admins: u32| json!({"name": m, "node_admin": admins, "rpc_node": 1});
    let shape = json!({"fabric": "fabric1", "meshes": [mesh("mesh1", 2), mesh("mesh2", 1)]});
    let (status, a) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{a}");
    let accepted = s(&a["build_id"]);
    estate.await_attempt(&accepted, Estate::attempt_of(&a), Duration::from_secs(120)).await;
    let want: std::collections::BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.rpc.1", "mesh2.admin.1", "mesh2.rpc.1"].iter().map(|s| s.to_string()).collect();
    let before = estate.settled(&want, Duration::from_secs(30)).await;
    let fabric_holder = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    assert!(fabric_holder.starts_with("mesh1."), "mesh2 holds no fabric seat: {fabric_holder}");
    let (_, mesh_before) = estate.get("/api/meshes/mesh2").await;
    let mesh_id = s(&mesh_before["id"]);
    let admin_before = before.iter().find(|n| n["name"] == "mesh2.admin.1").cloned().unwrap();
    let (_, b0) = estate.get(&format!("/api/builds?id={accepted}")).await;
    let attempt_before = b0["attempt"].as_u64().unwrap_or(0);
    estate.admin = before.iter().find(|n| s(&n["name"]) == fabric_holder).map(|n| s(&n["admin_api_base"])).filter(|b| !b.is_empty()).expect("the fabric primary advertises its control API");

    // The fault: mesh2's only admin runtime, exactly.
    let mut killed = 0;
    for (path, pid) in estate.live_runtimes() {
        if path.file_name().unwrap().to_string_lossy().starts_with("mesh2.admin.1") {
            estate.kill_pid(pid);
            killed += 1;
        }
    }
    assert_eq!(killed, 1, "mesh2's only admin was killed");

    let admin_inc = admin_before["incarnation_id"].clone();
    let recovered = rafka_test_scenario::estate::wait_for("mesh2's admin is reborn and ready", Duration::from_secs(240), || {
        let estate = &estate;
        let admin_inc = admin_inc.clone();
        async move {
            let n = estate.node_opt("mesh2.admin.1").await?;
            (n["incarnation_id"] != admin_inc && n["status"] == "ready-for-traffic").then_some(n)
        }
    })
    .await;
    let (_, mesh_after) = estate.get("/api/meshes/mesh2").await;
    assert_eq!(s(&mesh_after["id"]), mesh_id, "mesh2 recovered under its own MeshId");
    let (_, fabric_after) = estate.get("/api/fabric").await;
    assert_eq!(s(&fabric_after["build_id"]), accepted, "no new topology Build");
    estate.stop().await;
    let spans = estate.spans();

    let handoffs: Vec<&Value> = named(&spans, "rdm.node_admin.mesh.update.via-pending-handoff").into_iter().filter(|h| h["attributes"]["mesh"] == "mesh2").collect();
    let ids: std::collections::BTreeSet<String> = handoffs.iter().map(|h| s(&h["attributes"]["mesh_id"])).collect();
    assert_eq!(ids, [mesh_id.clone()].into_iter().collect(), "every hand-off names the one MeshId, none minted: {handoffs:?}");
    let certain = |h: &&&Value| matches!(h["attributes"]["outcome"].as_str(), Some("applied" | "already-applied"));
    let certain_handoffs: Vec<&&Value> = handoffs.iter().filter(certain).collect();
    assert_eq!(certain_handoffs.len(), 2, "Pending was made certain at the first birth and at the recovery: {handoffs:?}");
    let recovery = certain_handoffs.iter().max_by_key(|h| start(h)).unwrap();
    assert_eq!(recovery["attributes"]["target"], "mesh2.admin.1");
    assert_ne!(recovery["attributes"]["target_incarnation"], admin_inc, "the hand-off is to the new birth");
    assert_eq!(recovery["attributes"]["node"], fabric_holder.as_str(), "the fabric primary ran the recovery birth");
    let ready = named(&spans, "rdm.mesh.node.update.via-ready")
        .into_iter()
        .filter(|r| r["attributes"]["node"] == "mesh2.admin.1" && r["attributes"]["incarnation_id"] == recovered["incarnation_id"])
        .next_back()
        .cloned()
        .expect("the recovery admin committed Ready");
    assert!(end(recovery) <= start(&ready), "Pending certain ({}) before the recovery admin's Ready ({})", end(recovery), start(&ready));
    let decided: Vec<&Value> = named(&spans, DECLARATION)
        .into_iter()
        .filter(|d| d["attributes"]["node"] == "mesh2.admin.1" && d["attributes"]["op"] == "apply-mesh-state" && matches!(d["attributes"]["outcome"].as_str(), Some("applied" | "already-applied")))
        .collect();
    assert!(decided.len() >= 2, "the new birth's status door decided Pending too: {decided:?}");
    let result = json!({
        "cell": cell,
        "mesh_id": mesh_id,
        "accepted_build": accepted,
        "attempt_before": attempt_before,
        "admin_before": admin_inc,
        "admin_after": recovered["incarnation_id"],
        "handoffs": handoffs.len(),
        "recovery_handoff": {"span_id": recovery["span_id"], "start": start(recovery), "end": end(recovery), "outcome": recovery["attributes"]["outcome"]},
        "ready": {"span_id": ready["span_id"], "start": start(&ready)},
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
