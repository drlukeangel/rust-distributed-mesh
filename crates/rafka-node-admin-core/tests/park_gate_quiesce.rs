//! A stop ends every turn already past its wait point before it cuts (node-stop.md steps 3-4: the
//! node gossips nothing for its stop; R-ON1; park_gate.rs: a turn already past its wait point runs
//! to the end of that turn). The stop parks the admin's gate, so no turn begins, and waits until no
//! gated turn is in progress; only then are its connections cut. A turn that ran on past the cut
//! would publish (the hierarchy's aggregate, its statuses) and call out (the executor, the
//! declarer) from a stopped node.
//!
//! The admin under test is a real Day-0 admin (`admin::start_with`) at the gate's cadence
//! (`tools/test-runner` CADENCE), in a process of its own (the parking flag is process-wide).

use rafka_mesh_entity::NodeId;
use rafka_node_admin_core::admin::{start_with, AdminConfig};
use rafka_node_admin_core::wiring::{FabricHooks, Wiring};
use rafka_node_rpc_contract::status::{StatusReply, StatusRequest};
use serde_json::{json, Value};
use std::time::{Duration, UNIX_EPOCH};
use tracing::Instrument;

/// The gate's cadence (`tools/test-runner/src/lib.rs` CADENCE): a digest heartbeat is never slower
/// than 2 s (gossip.md §3.2), and the Day-0 admin's own Ready reaches its book on that heartbeat.
const CADENCE: [(&str, &str); 3] = [("RDM_STALENESS_MS", "3000"), ("RDM_GOSSIP_INTERVAL_MS", "500"), ("RDM_BACKBONE_INTERVAL_MS", "500")];
/// What a step that is immediate on a correct admin is given before the cell fails by name.
const NEVER: Duration = Duration::from_secs(20);

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

/// The cell's own span capture: the product span pipeline in front of an in-memory exporter, behind
/// a dispatcher installed on every thread of the cell's runtime. Each span keeps its start and end.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rafka_mesh_telemetry::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture("mesh1.admin.1", exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    fn spans(&self) -> Vec<Value> {
        self.capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let ns = |t: std::time::SystemTime| t.duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0);
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": s.name,
                    "trace_id": s.span_context.trace_id().to_string(),
                    "start_ns": ns(s.start_time),
                    "end_ns": ns(s.end_time),
                    "attributes": attributes,
                })
            })
            .collect()
    }
}

/// CONTRACT (node-stop.md step 3-4, R-ON1; park_gate.rs): the admin's hierarchy is in a turn, past
/// its wait point and held where it takes the view lock, when its mesh-admin's `stop-node` reaches
/// it; the cell lets the lock go only once the stop has parked the gate. The stop is answered
/// `Stopped`, and the aggregate that turn published (its `topology_version` bump) began before the
/// stop's connection cut did: the stop waited for the turn to end. Its cut found no turn in
/// progress. Must NOT happen: a turn that was in progress at the park running on past the cut, so
/// a stopped admin publishes its aggregate (and calls out) after it was cut.
#[test]
fn a_stop_cuts_only_once_no_turn_that_was_in_progress_at_the_park_is_still_running() {
    let cell = "a_stop_cuts_only_once_no_turn_that_was_in_progress_at_the_park_is_still_running";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    for (k, v) in CADENCE {
        std::env::set_var(k, v);
    }
    let cap = capture();
    let (stop_trace, lock_taken_ns) = cap.run(async {
        let dir = std::env::temp_dir().join(format!("park-gate-quiesce-{}", NodeId::mint()));
        let wiring = Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() };
        let running = start_with(day0_config(&dir), wiring).await.expect("the Day-0 admin comes up");
        let gate = running.gate().clone();
        let mut state = gate.state();

        // The admin is its mesh's publisher: its hierarchy decided so in a turn that has ended.
        tokio::time::timeout(NEVER, async {
            loop {
                if running.publishes_its_mesh() && !state.borrow_and_update().turns.contains_key("hierarchy") {
                    return;
                }
                state.changed().await.expect("the gate lives as long as the admin");
            }
        })
        .await
        .expect("the Day-0 admin never became its mesh's publisher with its hierarchy idle");

        // The view lock is taken; the hierarchy's next round begins its turn and holds where it
        // takes the lock.
        let topology = running.control.topology.clone();
        let holder = topology.write().await;
        let lock_taken_ns = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64;
        let own = running.digest.lock().unwrap().clone();
        tokio::time::timeout(NEVER, state.wait_for(|s| s.turns.contains_key("hierarchy"))).await.expect("the hierarchy began no turn within its round").unwrap();

        // Its mesh-admin's stop reaches it while that turn is in progress.
        let root = tracing::info_span!("cell.park-gate-quiesce", node = %own.node.name);
        let stop_trace = root.in_scope(rafka_mesh_telemetry::current_traceparent).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context");
        let node_self = running.node_self();
        let commander = NodeId::mint();
        let req = StatusRequest::StopNode { node_id: own.node.node_id.clone(), incarnation: own.node.incarnation.clone(), build_id: "bld_quiesce".into(), attempt: 1, operation: format!("stop-node:{}", own.node.name) };
        let stop = tokio::spawn(async move { node_self.serve(&commander, None, &req).await }.instrument(root));
        // Once the stop has parked the gate, the lock goes.
        tokio::time::timeout(NEVER, state.wait_for(|s| s.parked)).await.expect("the stop never parked the gate").unwrap();
        drop(holder);
        let reply = tokio::time::timeout(NEVER, stop).await.expect("the stop was not answered").unwrap();
        assert!(matches!(reply, Some(StatusReply::Stopped { .. })), "the stop is answered Stopped: {reply:?}");
        // The held turn runs to its end before the admin is ended.
        tokio::time::timeout(NEVER, state.wait_for(|s| !s.turns.contains_key("hierarchy"))).await.expect("the held hierarchy turn never ended").unwrap();

        running.leave_after_stop().await;
        let _ = std::fs::remove_dir_all(&dir);
        (stop_trace, lock_taken_ns)
    });

    let spans = cap.spans();
    let me = "mesh1.admin.1";
    let named = |name: &str| -> Vec<&Value> { spans.iter().filter(|s| s["name"] == name && s["attributes"]["node"] == me).collect() };
    let cut = named("rdm.node_admin.node.connections.delete.via-stop");
    assert_eq!(cut.len(), 1, "one connection cut for the stop: {cut:?}");
    let cut_began = cut[0]["start_ns"].as_u64().unwrap();
    // The aggregate the held turn published: no hierarchy turn publishes while the cell holds the
    // view lock, and none begins once the gate is parked, so it is the one bump after the lock.
    let held: Vec<&Value> = named("rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|s| s["start_ns"].as_u64().unwrap() > lock_taken_ns).collect();
    assert_eq!(held.len(), 1, "the held hierarchy turn published its mesh's aggregate once (its own Draining moved it): {held:?}");
    let bump = held[0];
    let bumped_at = bump["start_ns"].as_u64().unwrap();
    assert!(
        bumped_at < cut_began,
        "the hierarchy turn that was in progress at the park published its aggregate (topology_version {}) {} µs after the stop's connection cut began: the stop cut while a turn was still running (stop trace {stop_trace})",
        bump["attributes"]["topology_version"],
        (bumped_at - cut_began) / 1000
    );
    let cut_report = named("rdm.node_admin.node.connections.delete.via-stop-cut");
    assert_eq!(cut_report.len(), 1, "{cut_report:?}");
    assert_eq!(cut_report[0]["attributes"]["turns_in_progress"], "0", "the cut found a turn in progress: {}", cut_report[0]);
    assert_eq!(cut[0]["trace_id"], stop_trace.as_str(), "the cut is in the stop's own trace");
    println!("stop trace {stop_trace}; aggregate bumped {} µs before the cut began", (cut_began - bumped_at) / 1000);
}
