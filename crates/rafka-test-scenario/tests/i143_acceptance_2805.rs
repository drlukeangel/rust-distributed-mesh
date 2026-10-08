//! i143.e4.s11 acceptance (rafka-v2 #2805, hardened 2026-10-07), PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2805-process`, which exports `I143_ACCEPTANCE_DIR` (this
//! cell's `result.json` goes there) and whose command sets `RAFKA_ARTIFACTS_DIR` (the estate's
//! manifest, rpc ledger and every process's spans land under it, feature `i143-2805`).
//!
//! A real two-mesh birth through the Build rectifier: `POST /api/build` adds mesh2; the fabric
//! primary births mesh2's bootstrap admin, which hydrates the Fabric pointer and its Build, then
//! receives MeshStatus::Pending from the fabric primary, and only then does any of mesh2's own
//! membership get created; election alone supplies mesh2's primary afterwards.

use rafka_test_scenario::estate::{descends_from, named, Estate, Owner};
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
    // Every attempt of the creation Build descends from its REST request (the rectifier).
    let accepted = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == create.as_str()).cloned().expect("the creation Build's REST span");
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| r["attributes"]["build_id"] == create.as_str()).collect();
    assert!(!reconciles.is_empty(), "the creation Build was reconciled");
    for r in &reconciles {
        assert!(descends_from(&spans, r, &accepted), "every attempt of the creation Build descends from its request: {r}");
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
        "ready": {"span_id": ready["span_id"], "start": start(&ready)},
        "reconciles": reconciles.len(),
        "trace_id": accepted["trace_id"],
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
