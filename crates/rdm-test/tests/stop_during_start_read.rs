//! A node-admin's start runs its steps after the hook (topology, Build floor, rafka-time) in a task
//! its stop gate holds wherever it stands (`boot.rs`, `HookRuns::Spawned` through `StopGate::spawn`),
//! and a stop waits for every turn in progress at it (`StopGate::quiesce`). The gate's own rule
//! (stop_gate.rs): a task it can hold takes no lock another task needs. The start's Build-floor step
//! reads the Build log (`FileJournal::current_build` → `StoreLog::advance`), and so does the
//! hierarchy's turn (`adapter.current()`): a stop that holds the start inside that read must still
//! end, the hierarchy's turn with it.
//!
//! The admin under test is a real Day-0 admin (`admin::start_with`) in a process of its own (the stop
//! flag is process-wide). The testkit's store door (`rdm_node::store_log::read_door`, feature
//! `testkit-store`) holds the start's Build-floor read inside the read itself; nothing waits on a
//! clock. The start's own steps read the Build log too (its derivation turn, `derive_on_every_fact`,
//! awaited before `start_with` returns) and may reach it after the Build-floor read: the door admits
//! that read into the Build log's read only once the start has returned, so the read it holds is
//! never one the start's return waits on.

use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::NodeId;
use std::collections::BTreeMap;
use std::time::Duration;

/// What a step that is immediate on a correct admin is given before the cell fails by name.
const NEVER: Duration = Duration::from_secs(20);
/// The span the start's Build-floor step runs under (node_start.rs).
const BUILD_FLOOR: &str = "rdm.mesh.node.update.via-build-floor";
/// A tail read that waits for the one in progress on its file (store_log.rs).
const READ_WAITING: &str = "rdm.node.store.update.via-tail-read-waiting";
/// A stop's quiesce beginning to wait for the turns in progress at it (stop_gate.rs).
const QUIESCE_WAITING: &str = "rdm.node_admin.node.update.via-stop-quiesce-waiting";

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

/// Every span named `name` the cell's capture holds, by its fields.
fn named(spans: &crate::common::Spans, name: &str) -> Vec<BTreeMap<String, String>> {
    spans.0.lock().unwrap().values().filter(|(n, _, _)| n == name).map(|(_, _, f)| f.clone()).collect()
}

// @feature: node-lifecycle
/// CONTRACT (node-stop: a stop holds the admin's tasks, then ends; stop_gate.rs: a task the gate can
/// hold takes no lock another task needs): the Day-0 admin's start is inside its Build-floor read of
/// the Build log, and its hierarchy is in a turn whose read of the same log waits for that one, when
/// a signal stops the admin. The stop holds the start where it stands; the start's read then ends on
/// its own. The stop's quiesce names the hierarchy's turn among those it waits for, the hierarchy's
/// read names the Build floor's as the one it waited for, and the stop ends within the bound: the
/// held start keeps nothing the hierarchy's turn needs. Must NOT happen: the held start keeping the
/// Build log's read, so the hierarchy's turn never ends and the stop never does (#3210: the leave of
/// `member_cert_admin::` and `app_protocol_dials::` to the 300 s kill).
#[test]
fn an_admin_stopped_while_its_start_reads_the_build_log_ends_its_stop() {
    let cell = "an_admin_stopped_while_its_start_reads_the_build_log_ends_its_stop";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = crate::common::capture(cell);
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let dir = std::env::temp_dir().join(format!("stop-during-start-read-{}", NodeId::mint()));
        let door = rdm_node::store_log::read_door::arm(std::path::Path::new(rdm_node::store_root::BUILDS_FILE), BUILD_FLOOR);
        let running = match tokio::time::timeout(NEVER, start_with(day0_config(&dir), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() })).await {
            Ok(running) => running.expect("the Day-0 admin comes up"),
            Err(_) => {
                door.release();
                panic!("the Day-0 admin's start did not return within {NEVER:?}; reads that waited: {:?}", named(&spans, READ_WAITING));
            }
        };
        // The start has returned: the Build-floor read may now take the Build log's read, and is held there.
        door.admit();
        let gate = running.gate().clone();
        let mut state = gate.state();

        // The start is inside its Build-floor read.
        tokio::time::timeout(NEVER, door.entered()).await.expect("the start never read the Build log at its Build floor");
        // The hierarchy is in a turn whose read of the Build log waits for the start's.
        running.runner.records.view_changed.notify_one();
        tokio::time::timeout(NEVER, state.wait_for(|s| s.turns.contains_key("hierarchy"))).await.expect("the hierarchy began no turn").unwrap();
        tokio::time::timeout(NEVER, spans.recorded(READ_WAITING, |f| f.get("held_by").map(String::as_str) == Some(BUILD_FLOOR)))
            .await
            .expect("no read of the Build log waited for the start's Build-floor read");

        // A signal stops the admin, as a cell's end does; once its gate is stopped, the start's read ends.
        let leave = tokio::spawn(running.leave());
        tokio::time::timeout(NEVER, state.wait_for(|s| s.stopped)).await.expect("the stop never stopped the gate").unwrap();
        door.release();
        let ended = tokio::time::timeout(NEVER, leave).await;
        let stop_waits = named(&spans, QUIESCE_WAITING);
        let read_waits = named(&spans, READ_WAITING);
        if ended.is_err() {
            // Let the held start go, so the process ends and its trace is flushed.
            gate.resume();
            panic!(
                "the stop did not end within {NEVER:?}: the held start kept what the hierarchy's turn needs. stop waited for: {stop_waits:?}; reads that waited: {read_waits:?}; gate now: {:?}",
                state.borrow().clone()
            );
        }
        ended.unwrap().expect("the leave task ended");
        assert!(
            stop_waits.iter().any(|f| f.get("waiting_for").is_some_and(|w| w.split(',').any(|t| t.split(':').next() == Some("hierarchy")))),
            "the stop's quiesce waited for the hierarchy's turn: {stop_waits:?}"
        );
        let _ = std::fs::remove_dir_all(&dir);
    })
}
