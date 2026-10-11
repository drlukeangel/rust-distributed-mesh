//! A parked node-admin always serves its start: node-start.md makes start "the only exit from
//! stopped Leaving", and node-stop parks the admin's tasks. A task held while it waits for, or
//! holds, the view's lock keeps that lock from every handler of the parked admin, its start's among
//! them. Every task that can take the view lock is therefore held only at its own wait, outside
//! every lock (`ParkGate::opened`).
//!
//! The admin under test is a real Day-0 admin (`admin::start_with`) whose gossip and backbone rounds
//! are stretched far past the cell, in a process of its own: only the wakes the cell gives move its
//! loops, as the stop's own status change moves them.

use rafka_node_admin_core::admin::{start_with, AdminConfig};
use rafka_node_admin_core::wiring::{FabricHooks, Wiring};
use rafka_mesh_entity::NodeId;
use std::time::Duration;

/// Far past any round this cell could see.
const STRETCHED_MS: &str = "600000";
/// What a parked admin's handler is given to take the view lock before the cell fails. Taking an
/// uncontended lock is immediate; a lock a held task keeps is never given.
const NEVER: Duration = Duration::from_secs(5);

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" => Some(dir.clone()),
        "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// CONTRACT (node-start.md: start is the only exit from stopped Leaving; node-stop: the parked
/// admin's tasks are held): the loops that take the view lock, woken while the lock is taken (the
/// hierarchy on a view change, the declarer on its wake, the executor on a submitted Build) and the
/// admin parked while they wait on it, leave the lock free once its holder lets go: a handler of the
/// parked admin takes it at once. Must NOT happen: a parked task keeping the view lock it was
/// waiting on, so the parked admin never serves the start that would open it.
#[test]
fn a_parked_admin_whose_loops_were_woken_on_a_taken_view_lock_still_gives_the_lock_to_its_handlers() {
    let cell = "a_parked_admin_whose_loops_were_woken_on_a_taken_view_lock_still_gives_the_lock_to_its_handlers";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    std::env::set_var("RDM_GOSSIP_INTERVAL_MS", STRETCHED_MS);
    std::env::set_var("RDM_BACKBONE_INTERVAL_MS", STRETCHED_MS);
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("park-gate-view-lock-{}", NodeId::mint()));
        let wiring = Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() };
        let running = start_with(day0_config(&dir), wiring).await.expect("the Day-0 admin comes up");
        let (topology, records, submitted) = (running.control.topology.clone(), running.runner.records.clone(), running.control.build_submitted.clone());

        // The view lock is taken; every loop that takes it is woken, as the stop's own status
        // change wakes them, and reaches the lock behind the holder.
        let holder = topology.write().await;
        records.view_changed.notify_one();
        records.wake.poke();
        submitted.notify_one();
        tokio::time::sleep(Duration::from_millis(500)).await;
        // The stop parks the admin while they wait; then the holder lets go.
        running.gate().park();
        drop(holder);

        // A handler of the parked admin (its start's) takes the view lock.
        let taken = tokio::time::timeout(NEVER, topology.write()).await;
        assert!(taken.is_ok(), "a parked task kept the view lock it was waiting on: no handler of the parked admin can take it, so it never serves its start");
        drop(taken);

        running.gate().resume();
        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    drop(rt);
}
