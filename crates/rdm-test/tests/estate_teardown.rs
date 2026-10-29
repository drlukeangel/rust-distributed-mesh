//! The estate harness's teardown: an estate that ends, by a passing run or a panicking one, leaves no
//! process of its own behind.

use rdm_test::estate::{environ_names_estate, wait_for, Estate, Owner};
use serde_json::{json, Value};
use std::path::Path;
use std::time::Duration;

fn owner(subfeature: &str, test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "estate-teardown".into(),
        subfeature: subfeature.into(),
        rung: "M".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

/// The processes whose environment names `root` as their estate, the nodes an admin launched included.
fn processes_of(root: &Path) -> Vec<u32> {
    std::fs::read_dir("/proc")
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|e| e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()))
        .filter(|pid| std::fs::read(format!("/proc/{pid}/environ")).is_ok_and(|env| environ_names_estate(&env, root)))
        .collect()
}

/// Whether the fabric's store root holds the durable Fabric row (its line in `fabric.jsonl`), which the
/// fabric-primary writes at Day 0.
fn holds_fabric_identity_row(store_root: &Path) -> bool {
    rdm_admin::store_log::read_record(&store_root.join(rdm_admin::store_root::FABRIC_FILE), "fabric-record", None).is_some()
}

/// CONTRACT: the node-admin an estate restarted on its own data dir is the estate's process like the
/// bootstrap admin; dropping the estate (as a panicking cell does) stops it and whatever it launched,
/// at once, not when the test binary exits. The bootstrap admin hands off its seats via `node.stop`
/// (`node.primary.update` to `mesh1.admin.2`) once the Fabric row is durable; every node-admin is then
/// lost and it is restarted on its data dir (a launch while a node-admin lives is refused, Q156, Q199).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_estate_leaves_no_process_of_its_root() {
    let mut estate = Estate::bootstrap(owner("drop", "a_dropped_estate_leaves_no_process_of_its_root"), "fabric1", "mesh1").await;
    let root = estate.root.clone();
    let data_dir = estate.bootstrap_data_dir("mesh1");
    wait_for("the bootstrap admin's Fabric row is durable", Duration::from_secs(30), || async { holds_fabric_identity_row(&estate.fabric_store_root()).then_some(()) }).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 0)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter(&Estate::path("mesh1.admin.2")).await;
    let mut stop = estate.nodes_obj().await.stop(&Estate::path("mesh1.admin.1")).await.expect("node.stop of mesh1.admin.1");
    let accepted = stop.accepted().clone();
    if matches!(Estate::frames_of(&mut stop).await.last(), Some(rdm_client::Frame::FabricPrimaryMoved { .. })) {
        estate.resume(&accepted.build_id, accepted.attempt).await;
    }
    estate.kill_bootstrap();
    for (path, pid) in estate.live_runtimes() {
        if path.file_name().is_some_and(|n| n.to_string_lossy().contains(".admin.")) {
            estate.kill_pid(pid);
        }
    }
    estate.restart_admin_with(&data_dir, &[]);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!processes_of(&root).is_empty(), "the restarted node-admin runs under the estate's root");
    drop(estate);
    assert_eq!(processes_of(&root), Vec::<u32>::new(), "nothing of the estate is left running after it is dropped");
}

/// CONTRACT (i146 Q55, Luke 2026-10-17: "an estate stop with SIGTERM to every process at once ends
/// every rpc node before the SIGKILL, each having reported connections-deleting, left its mesh and
/// sent Stopped, nothing gossiped by the stopping node"): a fabric of one mesh, two node-admins and
/// three rpc nodes is stopped with SIGTERM to every process at once. Each rpc node runs node-stop.md
/// steps 2-4 as its own subject and waits on no answer: it owes its connections-deleting to its
/// mesh-primary, leaves its mesh (its gossip neighbours left, its Node RPC pool cut), then owes its
/// stopped state, in that order, and its process ends before the estate's bound. Must NOT happen: a
/// runtime the bound kills, a heartbeat of a stopping node after its leave, or the deleted connections delete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_estate_stopped_by_sigterm_ends_every_rpc_node_by_its_own_stop_before_the_kill() {
    let test = "an_estate_stopped_by_sigterm_ends_every_rpc_node_by_its_own_stop_before_the_kill";
    let mut estate = Estate::bootstrap(owner("signal-stop", test), "fabric1", "mesh1").await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 3)]).await;
    estate.settles_to(&[("mesh1", 2, 3)], Duration::from_secs(60)).await;
    let rpc: Vec<String> = estate.node_values().await.iter().filter(|n| n["kind"] == "rpc_node").filter_map(|n| n["name"].as_str().map(str::to_string)).collect();
    assert_eq!(rpc.len(), 3, "three rpc nodes: {rpc:?}");

    let killed = estate.stop().await;
    assert_eq!(killed, Vec::<u32>::new(), "every runtime ended by its own stop before the bound's SIGKILL");

    let spans = estate.spans();
    let start = |sp: &Value| sp["start_unix_nano"].as_u64().unwrap_or(0);
    let of = |name: &str, node: &str| -> Vec<&Value> { spans.iter().filter(|s| s["name"] == name && s["attributes"]["node"] == node).collect() };
    let mut evidence = Vec::new();
    for node in &rpc {
        let owed = of("rdm.node_rpc.status.update.via-owed-on-signal", node);
        let owed_state = |state: &str| owed.iter().find(|s| s["attributes"]["state"] == state).copied();
        let deleting = owed_state("ConnectionsDeleting").unwrap_or_else(|| panic!("{node} owed its connections-deleting to its mesh-primary: {owed:?}"));
        let stopped = owed_state("Stopped").unwrap_or_else(|| panic!("{node} owed its stopped state to its mesh-primary: {owed:?}"));
        let leaving = of("rdm.node.mesh.update.via-leaving", node);
        assert_eq!(leaving.len(), 1, "{node} left its mesh once: {leaving:?}");
        assert_eq!(leaving[0]["attributes"]["operation"], "signal", "{node}'s leave is its own stop's: {leaving:?}");
        let left = of("rdm.node.mesh.update.via-left", node);
        assert!(!left.is_empty(), "{node} left its mesh channel");
        assert!(start(deleting) <= start(leaving[0]) && start(leaving[0]) <= start(stopped), "{node}: connections-deleting owed, then the leave, then stopped owed");
        let published_after: Vec<&Value> = of("rdm.mesh.node.update.via-heartbeat", node).into_iter().filter(|h| start(h) > start(left[0])).collect();
        assert!(published_after.is_empty(), "{node} published nothing after it left its mesh: {published_after:?}");
        evidence.push(json!({"node": node, "connections_deleting_owed": deleting["trace_id"], "left": leaving[0]["trace_id"], "stopped_owed": stopped["trace_id"]}));
    }
    let connections_deleted: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.mesh.node.update.via-connection-delete").collect();
    assert!(connections_deleted.is_empty(), "no node runs the deleted connections delete: {connections_deleted:?}");
    estate.artifact("signal-stop.json", &json!({"rpc_nodes": evidence}));
}
