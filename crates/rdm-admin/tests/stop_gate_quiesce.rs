//! A stop ends every turn already past its wait point before it cuts (node-stop.md steps 3-4: the
//! node gossips nothing for its stop; R-ON1; stop_gate.rs: a turn already past its wait point runs
//! to the end of that turn). The stop holds the admin's gate, so no turn begins, and waits until no
//! gated turn is in progress; only then are its connections cut. A turn that ran on past the cut
//! would publish (the hierarchy's mesh topology, its statuses) and call out (the executor, the
//! declarer) from a stopped node.
//!
//! The admin under test is a real Day-0 admin (`admin::start_with`) at the gate's cadence
//! (`tools/test-runner` CADENCE, set by the binary's ctor in `tests/main.rs`: a digest heartbeat is
//! never slower than 2 s, gossip.md §3.2), in a process of its own (the stop flag is
//! process-wide).

use rdm_core::NodeId;
use rdm_admin::admin::{start_with, AdminConfig};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::status::{StatusReply, StatusRequest};
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
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("mesh1.admin.1", exporter.clone());
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

/// CONTRACT (node-stop.md steps 3-4, R-ON1; stop_gate.rs): the admin's executor is in a turn, past
/// its wait point and held where it takes the view lock, when its mesh-admin's `stop-node` reaches
/// it; the cell lets the lock go only once the stop has stopped the gate. The stop is answered
/// `Stopped`; its quiesce step (in the stop's own trace) names the executor among the turns it
/// waited for, and the stop's connection cut begins only after the quiesce ended: the cut finds no
/// turn in progress, and no mesh topology of the admin's mesh is published once the cut began. The stop
/// gossips nothing of its own (node-stop.md step 4, R-ON1, Q95): no stopping mesh topology. Nothing of
/// the admin's Build drive (its
/// claim, its dispatch, the run it called, the facts it broadcast) runs on past the cut: a drive
/// whose turn was in progress at the stop ended it before the cut, and one that would begin after
/// the stop waits at the gate. Must NOT happen: a turn that was in progress at the stop running on
/// past the cut, so a stopped admin plans, publishes or calls out after it was cut.
///
/// The executor is the turn held because its idle wait (`ReconcileWake::next`: a submitted Build, a
/// view installed, a fabric state change, the peer-mesh investigation's move, a drive's end) touches no
/// lock: woken by the cell's submit notification, it always reaches its turn and holds there, at the
/// cell's writer. The hierarchy's idle wait reads the view on a view change (admin.rs, its
/// `view_changed` branch), so a hierarchy woken by the install the cell's writer queued behind
/// would hold at the writer outside its turn. The writer takes the view lock only while no
/// executor turn is in progress: a turn already in progress when the writer took it may be past
/// the one place a turn takes the lock (`install_read`, admin.rs), and would end without holding
/// at the writer.
#[test]
fn a_stop_cuts_only_once_no_turn_that_was_in_progress_at_the_stop_is_still_running() {
    let cell = "a_stop_cuts_only_once_no_turn_that_was_in_progress_at_the_stop_is_still_running";
    if crate::own_process::delegated(module_path!(), cell) {
        return;
    }
    let cap = capture();
    let stop_trace = cap.run(async {
        let dir = std::env::temp_dir().join(format!("stop-gate-quiesce-{}", NodeId::mint()));
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

        // The view lock is taken while no executor turn is in progress; the executor's next pass,
        // woken by a submit's notification (Q253 amended: each pass runs on an event, never a tick),
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
        running.control.build_submitted.notify_waiters();
        tokio::time::timeout(NEVER, state.wait_for(|s| s.turns.contains_key("executor"))).await.expect("the executor began no turn on the submit's notification").unwrap();

        // Its mesh-admin's stop reaches it while that turn is in progress.
        let root = tracing::info_span!("cell.stop-gate-quiesce", node = %own.node.name);
        let stop_trace = root.in_scope(rdm_transport::current_traceparent).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).expect("the cell's root span has a trace context");
        let node_self = running.node_self();
        let commander = NodeId::mint();
        let req = rdm_core::node_command::NodeCommandRequest::StopNode { node_id: own.node.node_id.clone(), incarnation: own.node.incarnation.clone(), build_id: "8pe1w1tk1f0n".into(), attempt: 1, operation: format!("node.stop:{}", own.node.name) };
        let stop = tokio::spawn(async move {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            node_self.serve_node_command(Some(&commander), &req, Box::new(rdm_admin::node_self::ChannelFrames(tx))).await
        }.instrument(root));
        // Once the stop has stopped the gate, the lock goes.
        tokio::time::timeout(NEVER, state.wait_for(|s| s.stopped)).await.expect("the stop never stopped the gate").unwrap();
        drop(holder);
        let reply = tokio::time::timeout(NEVER, stop).await.expect("the stop was not answered").unwrap();
        assert!(matches!(reply, rdm_core::node_command::NodeCommandReply::NodeStopped), "the stop is answered Stopped: {reply:?}");
        // The held turn runs to its end before the admin is ended.
        tokio::time::timeout(NEVER, state.wait_for(|s| !s.turns.contains_key("executor"))).await.expect("the held executor turn never ended").unwrap();

        running.leave_after_stop().await;
        let _ = std::fs::remove_dir_all(&dir);
        stop_trace
    });

    let spans = cap.spans();
    let artifact = std::env::temp_dir().join("i143-testkit").join(cell);
    std::fs::create_dir_all(&artifact).unwrap();
    std::fs::write(artifact.join("spans.jsonl"), spans.iter().map(|s| s.to_string() + "\n").collect::<String>()).unwrap();
    let me = "mesh1.admin.1";
    let named = |name: &str| -> Vec<&Value> { spans.iter().filter(|s| s["name"] == name && s["attributes"]["node"] == me).collect() };
    // node-stop.md steps 2-3: the whole node.connection.delete { All } first, then the hold, then the
    // stop's own transport cut, whose span wraps the cut (its start is when the cut began).
    let all = named("rdm.node_admin.connection.delete.via-stop");
    assert_eq!(all.len(), 1, "one node.connection.delete {{ All }} for the stop: {all:?}");
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
    // A stop gossips nothing of its own (Q95): the one final mesh topology is R-NS1 item 2's, a deleted
    // mesh's last admin's on its node.delete.
    let leaving = named("rdm.node_admin.node.update.via-leaving-aggregate");
    assert!(leaving.is_empty(), "a stop publishes no stopping mesh topology (stop trace {stop_trace}): {leaving:?}");
    let cut_report = named("rdm.node_admin.node.update.via-stop-transport-cut");
    assert_eq!(cut_report.len(), 1, "{cut_report:?}");
    assert_eq!(cut_report[0]["attributes"]["turns_in_progress"], "0", "the cut found a turn in progress (stop trace {stop_trace}): {}", cut_report[0]);
    // Nothing of a turn follows the cut: no mesh topology of the admin's mesh is published after it began.
    let late: Vec<&Value> = named("rdm.mesh.backbone.update.via-topology-version").into_iter().filter(|s| s["start_ns"].as_u64().unwrap() >= cut_began).collect();
    assert!(late.is_empty(), "the stopped admin published its mesh's topology after its cut began: {late:?}");
    // Nothing of the Build drive runs on past the cut: every span of a drive's trace, and of a run
    // it called (its own trace), ended before the cut began. The one exception is the span of a
    // drive held at the gate's wait point (begun after the stop, so it ran no pass): it ends when
    // the cell's runtime drops it, with no outcome recorded, and nothing of it ran.
    let build_traces: std::collections::BTreeSet<&str> =
        spans.iter().filter(|s| s["name"] == "rdm.node_admin.build.update.via-drive" || s["name"] == "rdm.node_admin.build.update.via-reconcile").filter_map(|s| s["trace_id"].as_str()).collect();
    let held_at_the_gate = |s: &Value| s["name"] == "rdm.node_admin.build.update.via-drive" && s["attributes"].get("outcome").is_none();
    // A Build notice to the application is fire and forget, on a task of its own, and gates nothing
    // (fabric-node-hooks.md): sent as the drive's attempt began or ended before the cut, its own span may
    // end after it. It is no work of the drive.
    let notice = |s: &Value| matches!(s["name"].as_str(), Some("rdm.node_admin.build.update.via-on-build-started" | "rdm.node_admin.build.update.via-on-build-ended"));
    let after_cut: Vec<&Value> = spans.iter().filter(|s| s["trace_id"].as_str().is_some_and(|t| build_traces.contains(t)) && s["end_ns"].as_u64().unwrap() > cut_began && !held_at_the_gate(s) && !notice(s)).collect();
    assert!(after_cut.is_empty(), "the stopped admin's Build drive ran on past its cut (stop trace {stop_trace}): {after_cut:?}");
    println!(
        "stop trace {stop_trace}; quiesce waited for [{waited_for}] {} us, ended {} us before the cut began; spans in {}",
        (quiesce["end_ns"].as_u64().unwrap() - quiesce["start_ns"].as_u64().unwrap()) / 1000,
        (cut_began - quiesce["end_ns"].as_u64().unwrap()) / 1000,
        artifact.display()
    );
}
