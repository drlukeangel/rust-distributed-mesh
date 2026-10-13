//! A stop ends every turn already past its wait point before it cuts (node-stop.md steps 3-4: the
//! node gossips nothing for its stop; R-ON1; park_gate.rs: a turn already past its wait point runs
//! to the end of that turn). The stop parks the admin's gate, so no turn begins, and waits until no
//! gated turn is in progress; only then are its connections cut. A turn that ran on past the cut
//! would publish (the hierarchy's aggregate, its statuses) and call out (the executor, the
//! declarer) from a stopped node.
//!
//! The admin under test is a real Day-0 admin (`admin::start_with`) at the gate's cadence
//! (`tools/test-runner` CADENCE, set by the binary's ctor in `tests/main.rs`: a digest heartbeat is
//! never slower than 2 s, gossip.md §3.2), in a process of its own (the parking flag is
//! process-wide).

use rafka_mesh_entity::NodeId;
use rafka_node_admin_core::admin::{start_with, AdminConfig};
use rafka_node_admin_core::wiring::{FabricHooks, Wiring};
use rafka_node_rpc_contract::status::{StatusReply, StatusRequest};
use serde_json::{json, Value};
use std::time::{Duration, UNIX_EPOCH};
use tracing::Instrument;

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

/// CONTRACT (node-stop.md steps 3-4, R-ON1; park_gate.rs): the admin's executor is in a turn, past
/// its wait point and held where it takes the view lock, when its mesh-admin's `stop-node` reaches
/// it; the cell lets the lock go only once the stop has parked the gate. The stop is answered
/// `Stopped`; its quiesce step (in the stop's own trace) names the executor among the turns it
/// waited for, and the stop's connection cut begins only after the quiesce ended: the cut finds no
/// turn in progress, and no aggregate of the admin's mesh is published once the cut began. Between
/// the quiesce and the cut the stop publishes the admin's last aggregate, carrying itself Leaving
/// (gossip.md, "Mesh delete"), in the stop's own trace. Nothing of the admin's Build drive (its
/// claim, its dispatch, the run it called, the facts it broadcast) runs on past the cut: a drive
/// whose turn was in progress at the park ended it before the cut, and one that would begin after
/// the park waits at the gate. Must NOT happen: a turn that was in progress at the park running on
/// past the cut, so a stopped admin plans, publishes or calls out after it was cut.
///
/// The executor is the turn held because its idle wait (`select!` on a submitted Build or its
/// 300 ms poll, admin.rs) touches no lock: it always reaches its turn and holds there, at the
/// cell's writer. The hierarchy's idle wait reads the view on a view change (admin.rs, its
/// `view_changed` branch), so a hierarchy woken by the install the cell's writer queued behind
/// would hold at the writer outside its turn. The writer takes the view lock only while no
/// executor turn is in progress: a turn already in progress when the writer took it may be past
/// the one place a turn takes the lock (`install_read`, admin.rs), and would end without holding
/// at the writer.
#[test]
fn a_stop_cuts_only_once_no_turn_that_was_in_progress_at_the_park_is_still_running() {
    let cell = "a_stop_cuts_only_once_no_turn_that_was_in_progress_at_the_park_is_still_running";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = capture();
    let stop_trace = cap.run(async {
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

        // The view lock is taken while no executor turn is in progress; the executor's next poll
        // begins its turn after it and holds where it takes the lock.
        let topology = running.control.topology.clone();
        let holder = tokio::time::timeout(NEVER, async {
            loop {
                state.wait_for(|s| !s.turns.contains_key("executor")).await.expect("the gate lives as long as the admin");
                let holder = topology.write().await;
                if !state.borrow_and_update().turns.contains_key("executor") {
                    return holder;
                }
            }
        })
        .await
        .expect("the executor was never between turns");
        let own = running.digest.lock().unwrap().clone();
        tokio::time::timeout(NEVER, state.wait_for(|s| s.turns.contains_key("executor"))).await.expect("the executor began no turn within its poll").unwrap();

        // Its mesh-admin's stop reaches it while that turn is in progress.
        let root = tracing::info_span!("cell.park-gate-quiesce", node = %own.node.name);
        let stop_trace = root.in_scope(rafka_mesh_telemetry::current_traceparent).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context");
        let node_self = running.node_self();
        let commander = NodeId::mint();
        let req = StatusRequest::StopNode { node_id: own.node.node_id.clone(), incarnation: own.node.incarnation.clone(), build_id: "8pe1w1tk1f0n".into(), attempt: 1, operation: format!("stop-node:{}", own.node.name) };
        let stop = tokio::spawn(async move { node_self.serve(Some(&commander), None, &req).await }.instrument(root));
        // Once the stop has parked the gate, the lock goes.
        tokio::time::timeout(NEVER, state.wait_for(|s| s.parked)).await.expect("the stop never parked the gate").unwrap();
        drop(holder);
        let reply = tokio::time::timeout(NEVER, stop).await.expect("the stop was not answered").unwrap();
        assert!(matches!(reply, Some(StatusReply::Stopped { .. })), "the stop is answered Stopped: {reply:?}");
        // The held turn runs to its end before the admin is ended.
        tokio::time::timeout(NEVER, state.wait_for(|s| !s.turns.contains_key("executor"))).await.expect("the held executor turn never ended").unwrap();

        running.leave_after_stop().await;
        let _ = std::fs::remove_dir_all(&dir);
        stop_trace
    });

    let spans = cap.spans();
    let artifact = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit").join(cell);
    std::fs::create_dir_all(&artifact).unwrap();
    std::fs::write(artifact.join("spans.jsonl"), spans.iter().map(|s| s.to_string() + "\n").collect::<String>()).unwrap();
    let me = "mesh1.admin.1";
    let named = |name: &str| -> Vec<&Value> { spans.iter().filter(|s| s["name"] == name && s["attributes"]["node"] == me).collect() };
    // node-stop.md steps 2-3: the whole node.connections.delete { All } first, then the hold, then the
    // stop's own transport cut, whose span wraps the cut (its start is when the cut began).
    let all = named("rdm.node_admin.node.connections.delete.via-stop");
    assert_eq!(all.len(), 1, "one node.connections.delete {{ All }} for the stop: {all:?}");
    let cut = named("rdm.node_admin.node.update.via-stop-transport-cut");
    assert_eq!(cut.len(), 1, "one transport cut for the stop: {cut:?}");
    assert_eq!(cut[0]["trace_id"], stop_trace.as_str(), "the cut is in the stop's own trace");
    let cut_began = cut[0]["start_ns"].as_u64().unwrap();
    let quiesce = named("rdm.node_admin.node.update.via-stop-quiesce");
    assert_eq!(quiesce.len(), 1, "one quiesce step for the stop (stop trace {stop_trace}): {quiesce:?}");
    let quiesce = quiesce[0];
    assert_eq!(quiesce["trace_id"], stop_trace.as_str(), "the quiesce is in the stop's own trace");
    let waited_for = quiesce["attributes"]["waited_for"].as_str().unwrap_or_default();
    assert!(waited_for.split(',').any(|t| t.split(':').next() == Some("executor")), "the quiesce waited for the executor's held turn (stop trace {stop_trace}): {quiesce}");
    assert!(quiesce["end_ns"].as_u64().unwrap() <= cut_began, "the cut began before the quiesce ended: {quiesce}");
    assert!(all[0]["end_ns"].as_u64().unwrap() <= quiesce["start_ns"].as_u64().unwrap(), "the hold began before the All ended: {}", all[0]);
    // The admin publishes its mesh's aggregate, so its last one, carrying itself Leaving, goes out
    // after the hold and before the cut (gossip.md, "Mesh delete": "Its last aggregate before a stop
    // carries itself Leaving"): no gated turn publishes after it.
    let leaving = named("rdm.node_admin.node.update.via-leaving-aggregate");
    assert_eq!(leaving.len(), 1, "one last aggregate carrying the admin Leaving for the stop (stop trace {stop_trace}): {leaving:?}");
    let leaving = leaving[0];
    assert_eq!(leaving["trace_id"], stop_trace.as_str(), "the last aggregate is in the stop's own trace");
    assert_eq!(leaving["attributes"]["published"], "true", "the admin's last aggregate went out on the backbone: {leaving}");
    assert!(quiesce["end_ns"].as_u64().unwrap() <= leaving["start_ns"].as_u64().unwrap(), "the last aggregate went out before the hold ended (stop trace {stop_trace}): {leaving}");
    assert!(leaving["end_ns"].as_u64().unwrap() <= cut_began, "the cut began before the last aggregate went out (stop trace {stop_trace}): {leaving}");
    let cut_report = named("rdm.node_admin.node.update.via-stop-transport-cut");
    assert_eq!(cut_report.len(), 1, "{cut_report:?}");
    assert_eq!(cut_report[0]["attributes"]["turns_in_progress"], "0", "the cut found a turn in progress (stop trace {stop_trace}): {}", cut_report[0]);
    // Nothing of a turn follows the cut: no aggregate of the admin's mesh is published after it began.
    let late: Vec<&Value> = named("rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|s| s["start_ns"].as_u64().unwrap() >= cut_began).collect();
    assert!(late.is_empty(), "the stopped admin published its mesh's aggregate after its cut began: {late:?}");
    // Nothing of the Build drive runs on past the cut: every span of a drive's trace, and of a run
    // it called (its own trace), ended before the cut began. The one exception is the span of a
    // drive held at the gate's wait point (begun after the park, so it ran no pass): it ends when
    // the cell's runtime drops it, with no outcome recorded, and nothing of it ran.
    let build_traces: std::collections::BTreeSet<&str> =
        spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-drive" || s["name"] == "rdm.node_admin.build.update.via-reconcile").filter_map(|s| s["trace_id"].as_str()).collect();
    let held_at_the_gate = |s: &Value| s["name"] == "rdm.node_admin.build.update.via-drive" && s["attributes"].get("outcome").is_none();
    let after_cut: Vec<&Value> = spans.iter().filter(|s| s["trace_id"].as_str().is_some_and(|t| build_traces.contains(t)) && s["end_ns"].as_u64().unwrap() > cut_began && !held_at_the_gate(s)).collect();
    assert!(after_cut.is_empty(), "the stopped admin's Build drive ran on past its cut (stop trace {stop_trace}): {after_cut:?}");
    println!(
        "stop trace {stop_trace}; quiesce waited for [{waited_for}] {} us, ended {} us before the cut began; spans in {}",
        (quiesce["end_ns"].as_u64().unwrap() - quiesce["start_ns"].as_u64().unwrap()) / 1000,
        (cut_began - quiesce["end_ns"].as_u64().unwrap()) / 1000,
        artifact.display()
    );
}
