//! Q122 (Luke, 2026-10-22): the fabric-primary unheard past the staleness floor is taken by nobody
//! (fabric-recovery.md "The loss of the fabric-primary while a node-admin lives"; elections §3, §4.1).
//! The lowest ReadyForTraffic node-admin of its mesh runs fabric recovery and takes no primary role;
//! node investigation decides what becomes of the lost birth, and node recovery re-creates the
//! node-admin at its path.name; when that node is ReadyForTraffic the workflow restores it as
//! mesh-primary and fabric-primary, and the node records both on its own row.
use rdm_test::estate::{named, Estate, Owner};
use serde_json::Value;
use std::time::Duration;

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "mesh-elections".into(),
        subfeature: "fabric-primary-loss".into(),
        rung: "MM".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}
fn at(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}
fn now_ns() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}
fn signal(pid: u32, sig: &str) {
    let ok = std::process::Command::new("kill").args([sig, &pid.to_string()]).status().expect("kill runs").success();
    assert!(ok, "kill {sig} {pid}");
}
/// The fabric-primary's name and whether it also holds its mesh's seat, from a view.
fn seats(nodes: &[Value]) -> Option<(String, bool)> {
    nodes.iter().find(|n| n["is_fabric_primary"] == true).map(|n| (s(&n["name"]), n["is_primary"] == true))
}

/// CONTRACT (Q122; fabric-recovery.md "The loss of the fabric-primary while a node-admin lives" steps 1-4;
/// elections §3, §4.1): a two-mesh estate; mesh1.admin.1, the fabric-primary and mesh1's primary, has its
/// exact runtime held (SIGSTOP) past the staleness floor. From the mark until the workflow restores the
/// fabric-primary, no view names a fabric-primary or a primary node-admin of mesh1: mesh1.admin.2, the
/// lowest ReadyForTraffic node-admin of the mesh, runs fabric recovery and takes no primary role. Node
/// investigation reads the held runtime (not `reached`), so node recovery re-creates the node-admin at
/// mesh1.admin.1 with a new node id; once it is ReadyForTraffic the workflow restores it as mesh-primary
/// and fabric-primary. What must NOT happen: mesh1.admin.2 or a node of mesh2 named fabric-primary, or
/// mesh1.admin.2 named mesh1's primary, while the workflow runs. The held birth stays held for the whole
/// cell (what a returning old birth meets is not this cell's); the teardown kills it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fabric_primary_held_past_the_floor_is_restored_at_its_path_name_by_fabric_recovery_its_mesh_runs() {
    let test = "a_fabric_primary_held_past_the_floor_is_restored_at_its_path_name_by_fabric_recovery_its_mesh_runs";
    let mut estate = Estate::bootstrap(owner(test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 2), ("mesh2", 2, 2)]).await;
    estate.settled_views(&[("mesh1", 2, 2), ("mesh2", 2, 2)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    let floor = rdm_transport::membership::staleness_floor();
    let fp = "mesh1.admin.1";
    let standby = "mesh1.admin.2";
    let before = estate.node_view(fp).await;
    let nodes0 = estate.node_values().await;
    assert_eq!(seats(&nodes0), Some((fp.to_string(), true)), "the Day-0 admin holds both roles: {nodes0:#?}");
    // Control is read through the standby from here: a held admin answers nothing.
    estate.enter(&Estate::path(standby)).await;
    let pid = estate.bootstrap_pid().unwrap_or_else(|| panic!("{fp} is the bootstrap admin and the estate holds its pid"));

    let held_at = now_ns();
    signal(pid, "-STOP");
    // Past the mark: nobody holds either role, in every sampled view, until a node at mesh1.admin.1 with a
    // new node id is ReadyForTraffic and holds both.
    let mut sampled_vacant = false;
    let restored = estate
        .until_view("the node re-created at mesh1.admin.1 is ReadyForTraffic, mesh-primary and fabric-primary", floor * 12 + Duration::from_secs(120), |views| {
            let ns = Estate::values(views);
            let holders: Vec<String> = ns.iter().filter(|n| n["is_fabric_primary"] == true).map(|n| s(&n["name"])).collect();
            let mesh1_primary: Vec<String> = ns.iter().filter(|n| n["kind"] == "node_admin" && n["is_primary"] == true && s(&n["name"]).starts_with("mesh1.")).map(|n| s(&n["name"])).collect();
            assert!(holders.iter().all(|h| h == fp), "only the node at {fp} is ever fabric-primary: {holders:?}");
            assert!(mesh1_primary.iter().all(|h| h == fp), "only the node at {fp} is ever mesh1's primary: {mesh1_primary:?}");
            if holders.is_empty() && mesh1_primary.is_empty() {
                sampled_vacant = true;
            }
            ns.iter()
                .find(|n| n["name"] == fp && s(&n["node_id"]) != before.node_id.to_string() && n["status"] == "ready-for-traffic" && n["is_fabric_primary"] == true && n["is_primary"] == true)
                .cloned()
        })
        .await;
    assert!(sampled_vacant, "a view was sampled with neither role held while fabric recovery ran");
    // The held birth is the test's to end; nothing in the workflow terminates an os-stopped runtime.
    signal(pid, "-KILL");
    estate.stop().await;
    let spans = estate.spans();
    let ran: Vec<Value> = named(&spans, "rdm.node_admin.fabric.update.via-fabric-recovery").into_iter().filter(|sp| at(sp) >= held_at).cloned().collect();
    assert!(ran.iter().any(|sp| s(&sp["attributes"]["node"]) == standby && s(&sp["attributes"]["arm"]) == "own-mesh" && s(&sp["attributes"]["outcome"]).starts_with("restored")), "{standby} ran fabric recovery in its own mesh and restored the node at {fp}: {ran:#?}");
    assert!(ran.iter().all(|sp| s(&sp["attributes"]["node"]) == standby), "nobody else ran it: {ran:#?}");
    let verdict = ran.iter().find_map(|sp| sp["attributes"]["verdict"].as_str().map(str::to_string)).unwrap_or_default();
    assert!(!verdict.is_empty() && verdict != "reached", "node investigation decided the re-create: verdict {verdict:?}");
    let created = named(&spans, "rdm.node_admin.node.create.via-build").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == fp && at(sp) >= held_at).count();
    assert!(created >= 1, "node recovery re-created the node-admin at {fp}");
    let installed = named(&spans, "rdm.node_admin.seat.update.via-primary-updated").into_iter().filter(|sp| s(&sp["attributes"]["node"]) == fp && at(sp) >= held_at).count();
    assert!(installed >= 1, "the restored node installed both roles on the workflow's call");
    assert_ne!(s(&restored["node_id"]), before.node_id.to_string(), "a new node id at the same path.name");
}
