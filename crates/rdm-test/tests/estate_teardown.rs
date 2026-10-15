//! The estate harness's teardown: an estate that ends, by a passing run or a panicking one, leaves no
//! process of its own behind.

use rdm_test::estate::{environ_names_estate, wait_for, Estate, Owner};
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

/// Whether the fabric's store root holds the durable Fabric row (`fabric/identity.json`), which the
/// fabric-primary publishes at the fabric's first state-commit.
fn holds_fabric_identity_row(store_root: &Path) -> bool {
    store_root.join("fabric/identity.json").is_file()
}

/// CONTRACT: the node-admin an estate restarted on its own data dir is the estate's process like the
/// bootstrap admin; dropping the estate (as a panicking cell does) stops it and whatever it launched,
/// at once, not when the test binary exits. The bootstrap admin hands off its seats via `node.stop`
/// (`node.primary.update` to `mesh1.admin.2`) once the Fabric row is durable, and is restarted on its data dir.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_estate_leaves_no_process_of_its_root() {
    let mut estate = Estate::bootstrap(owner(), "fabric1", "mesh1").await;
    let root = estate.root.clone();
    let data_dir = estate.bootstrap_data_dir("mesh1");
    wait_for("the bootstrap admin's Fabric row is durable", Duration::from_secs(30), || async { holds_fabric_identity_row(&estate.fabric_store_root()).then_some(()) }).await;
    estate.reconcile_shape("fabric1", &[("mesh1", 2, 0)]).await;
    estate.until_fabric_ready(Duration::from_secs(120)).await;
    estate.enter(&Estate::path("mesh1.admin.2")).await;
    let mut stop = estate.nodes_obj().await.stop(&Estate::path("mesh1.admin.1")).await.expect("node.stop of mesh1.admin.1");
    let accepted = stop.accepted().clone();
    if matches!(Estate::frames_of(&mut stop).await.last(), Some(rdm_client::Frame::SeatMoved { .. })) {
        estate.resume(&accepted.build_id, accepted.attempt).await;
    }
    estate.kill_bootstrap();
    estate.restart_admin_with(&data_dir, &[]);
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert!(!processes_of(&root).is_empty(), "the restarted node-admin runs under the estate's root");
    drop(estate);
    assert_eq!(processes_of(&root), Vec::<u32>::new(), "nothing of the estate is left running after it is dropped");
}
