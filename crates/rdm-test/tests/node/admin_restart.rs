//! Node-admin restart: product=mesh, feature=node-lifecycle, subfeature=admin-restart, rung=MM.
//! A restart is node.stop then node.start (Q183: node.restart is removed). Spec:
//! `docs/architecture/lifecycles/node-stop.md`, `docs/architecture/lifecycles/node-start.md`.

use rdm_client::{FabricDesired, LaunchKind as NodeKind, MeshDesired};
use rdm_test::estate::{named, wait_for, Estate, Owner};
use rdm_test::faults::{binding_set, candidate_sha};
use serde_json::Value;
use std::collections::BTreeSet;
use std::time::Duration;

fn owner_of(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "node-lifecycle".into(),
        subfeature: "admin-restart".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn mesh(name: &str) -> MeshDesired {
    MeshDesired::of(name, [(NodeKind::NodeAdmin, 2), (NodeKind::RpcNode, 2)])
}

/// Every span the process `pid` wrote.
fn spans_of(estate: &Estate, pid: u64) -> Vec<Value> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(&estate.evidence).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with(".spans.jsonl") && name.contains(&format!(".{pid}-")) {
            out.extend(std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()));
        }
    }
    out
}

/// CONTRACT (node-primary-update.md failure contract, "The successor is unheard after the update": the
/// ordinary election then names whoever it names, the old primary included, and the successor's node
/// goes through node recovery like any unheard member): a mesh-primary node-admin restarted by
/// `node.stop` then `node.start` first hands the mesh-primary role to its mesh's other admin by
/// node.primary.update at its stop.
/// When that successor's process is then gone, the election names the restarted admin again, and the
/// successor's recovery completes under it: a new node stands ready at the successor's path.name.
///
/// What must NOT happen (the twelve soaks at 768dfaee, `mock_soak_primaries_preserve_continuous_invariants`
/// round 17, mesh2.admin.2-n88v5zcrvdkd): the restarted primary's mesh-row writes refused, and no
/// re-birth in 150 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_mesh_primary_elected_again_after_its_successor_dies_writes_its_mesh_rows() {
    let sha = candidate_sha();
    let mut estate = Estate::bootstrap_on(owner_of("a_restarted_mesh_primary_elected_again_after_its_successor_dies_writes_its_mesh_rows"), "fabric1", "mesh1", &binding_set(&sha), &sha, &["rpc_node"])
        .await
        .expect("the faulted-admin binding set is accepted");
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await;
    let names: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| [format!("{m}.admin.1"), format!("{m}.admin.2"), format!("{m}.rpc.1"), format!("{m}.rpc.2")]).collect();
    let before = Estate::values(&estate.settles_to_named(&names, Duration::from_secs(60)).await);
    // The initial fabric refuses a restart until it has first reached ready; a kill before
    // that is refused by the estate (Q74).
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    // The target mesh is the one that does not hold the fabric-primary role: its primary (the primary) and its
    // other admin (the successor the primary's restart hands the role to).
    let fabric_primary = before.iter().find(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).expect("one fabric primary");
    let away = if fabric_primary.starts_with("mesh1.") { "mesh2" } else { "mesh1" };
    let primary = before.iter().find(|n| n["mesh"] == away && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).expect("a mesh primary");
    let successor = before.iter().find(|n| n["mesh"] == away && n["kind"] == "node_admin" && s(&n["name"]) != primary).map(|n| s(&n["name"])).expect("the other admin");
    let successor_birth = before.iter().find(|n| s(&n["name"]) == successor).map(|n| (s(&n["node_id"]), s(&n["incarnation_id"]))).expect("the successor's birth");
    let primary_birth = before.iter().find(|n| s(&n["name"]) == primary).map(|n| (s(&n["node_id"]), s(&n["incarnation_id"]))).expect("the holder's birth");

    // 1. The primary's stop then start: its role goes to the successor by node.primary.update at the
    //    stop, then the same birth starts again.
    estate.stop_then_start(&primary.parse().unwrap_or_else(|e| panic!("{primary} is not a path.name: {e:?}"))).await;
    wait_for(&format!("{successor} holds {away}'s mesh-primary role and {primary} is back as the same birth, not the primary"), Duration::from_secs(60), || async {
        let nodes = estate.node_values().await;
        let s_ok = nodes.iter().any(|n| s(&n["name"]) == successor && n["is_primary"] == true && n["status"] == "ready");
        let h_ok = nodes.iter().any(|n| s(&n["name"]) == primary && n["is_primary"] == false && n["status"] == "ready" && (s(&n["node_id"]), s(&n["incarnation_id"])) == primary_birth);
        (s_ok && h_ok).then_some(())
    })
    .await;
    let primary_pid = estate.pid_of(&primary).await;
    let handed = wait_for(&format!("{primary}'s evidence records its restart's node.primary.update"), Duration::from_secs(10), || {
        let spans = spans_of(&estate, primary_pid);
        let found = named(&spans, "rdm.node_admin.primary.update.via-update-primary").into_iter().find(|x| s(&x["attributes"]["operation"]).ends_with(&format!("node.stop:{primary}")) && x["attributes"]["outcome"] == "updated").cloned();
        async move { found }
    })
    .await;

    // 2. The successor's process is gone: SIGKILL of its exact runtime, provider-confirmed on the next
    //    inspection.
    let successor_pid = estate.pid_of(&successor).await;
    estate.kill_pid(successor_pid as u32);
    let killed_at = std::time::Instant::now();

    // 3. The ordinary rule names the primary again, and the successor's recovery completes under it: a
    //    new node ready at the successor's path.name (a new node.id). The bound is the ladder's (a floor
    //    to mark it silent, the tickle, a floor to the second no-path, the provider's proof, the create).
    let recovered = wait_for(&format!("{primary} is {away}'s mesh primary again and a new node stands ready at {successor}"), Duration::from_secs(120), || async {
        let nodes = estate.node_values().await;
        let h_ok = nodes.iter().any(|n| s(&n["name"]) == primary && n["is_primary"] == true && n["status"] == "ready");
        let re_born = nodes.iter().find(|n| s(&n["name"]) == successor && n["status"] == "ready" && s(&n["node_id"]) != successor_birth.0).cloned();
        if h_ok { re_born } else { None }
    })
    .await;
    let recovered_ms = killed_at.elapsed().as_millis();
    estate.stop().await;

    // The primary's own record: the election named it mesh-primary again after its restart's update.
    let spans = spans_of(&estate, primary_pid);
    let start = |x: &Value| x["start_unix_nano"].as_u64().unwrap_or_default();
    let election = named(&spans, "rdm.mesh.primary.update.via-announcement").into_iter().filter(|x| start(x) > start(&handed) && x["attributes"]["role"] == "mesh-primary" && x["attributes"]["via"] == "own" && x["attributes"]["outcome"] == "held").cloned().collect::<Vec<_>>();
    assert!(!election.is_empty(), "{primary} holds itself {away}'s mesh-primary again by the election after its restart's update: {}", serde_json::to_string(&named(&spans, "rdm.mesh.primary.update.via-announcement")).unwrap_or_default());
    let trace_id = s(&election[0]["trace_id"]);
    estate.record_trace_url(&trace_id);
    eprintln!("PRIMARY-AGAIN-TRACE {trace_id} holder={primary} successor={successor} re-born={} recovered_ms={recovered_ms}", s(&recovered["node_id"]));
}
