//! The estate harness's teardown: an estate that ends, by a passing run or a panicking one, leaves no
//! process of its own behind.

use rafka_test_scenario::estate::{environ_names_estate, wait_for, Estate, Owner};
use std::path::Path;
use std::time::Duration;

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "estate-teardown".into(),
        subfeature: "drop".into(),
        rung: "M".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: "a_dropped_estate_leaves_no_process_of_its_root".into(),
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

/// Whether the fabric's store root holds a durable fabric-primary seat row (`fabric/seats/`).
fn holds_fabric_seat_row(store_root: &Path) -> bool {
    let seat = rafka_mesh_entity::Seat::FabricPrimary.name();
    std::fs::read_dir(store_root.join("fabric/seats")).into_iter().flatten().flatten().any(|e| e.file_name().to_str().is_some_and(|n| n.starts_with(seat) && n.ends_with(".json")))
}

/// CONTRACT: the node-admin an estate restarted on its own data dir is the estate's process like the
/// bootstrap admin; dropping the estate (as a panicking cell does) stops it and whatever it launched,
/// at once, not when the test binary exits. The bootstrap admin is killed once its fabric seat row is
/// durable on the fabric's store root: a restart on its data dir is the fabric's root and adopts its own clock
/// (fabric-node-lifecycle.md 2.5); with no seat row it is no root and waits for a node to take
/// rafka-time from, which a sole admin never finds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_estate_leaves_no_process_of_its_root() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let root = estate.root.clone();
    let data_dir = estate.bootstrap_data_dir("mesh1");
    wait_for("the bootstrap admin's fabric seat row is durable", Duration::from_secs(30), || async { holds_fabric_seat_row(&estate.fabric_store_root()).then_some(()) }).await;
    estate.kill_bootstrap();
    let base = estate.restart_admin_with(&data_dir, &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    estate.admin = base;
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!processes_of(&root).is_empty(), "the restarted node-admin runs under the estate's root");
    drop(estate);
    assert_eq!(processes_of(&root), Vec::<u32>::new(), "nothing of the estate is left running after it is dropped");
}
