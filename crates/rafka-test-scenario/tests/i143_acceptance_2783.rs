//! i143.e8.s5 acceptance (rafka-v2 #2783, hardened 2026-10-07), CHAOS-PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2783-chaos-process`, which exports `I143_ACCEPTANCE_DIR`
//! (each cell's `result.json` goes there) and whose commands set `RDM_ARTIFACTS_DIR` (the
//! estate's manifest, rpc ledger and every process's spans land under it, feature `i143-2783`).
//!
//! - `process_fault_backend_kills_exact_runtime_recovers_current_birth`: the process fault backend
//!   (`rafka_test_scenario::process_faults`) on a real estate.
//! The cell does not touch the fabric-primary node-admin, and no mesh loses every node-admin.

use opentelemetry::trace::TracerProvider as _;
use rafka_test_scenario::elections::{advertised_fabric_primaries, seats_as_expected};
use rafka_test_scenario::estate::{descends_from, named, wait_for, Estate, Owner};
use rafka_test_scenario::process_faults::{cpu_ticks, proc_state, ExactRuntime, Fault, Refusal as FaultRefusal};
use rafka_test_scenario::wedge::*;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer as _;

const KILL_CELL: &str = "process_fault_backend_kills_exact_runtime_recovers_current_birth";

fn owner(test: &str) -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2783".into(),
        subfeature: "process-faults".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: test.into(),
    }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    let dir = match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2783/chaos-process").join(cell),
    };
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

fn num(v: &Value) -> u64 {
    v.as_u64().or_else(|| v.as_str().and_then(|a| a.parse().ok())).unwrap_or(0)
}

/// This process's own spans: an in-memory OTel exporter behind the global subscriber, so the
/// fault backend's and the detector's spans are exactly what this cell emitted.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    service: String,
}

fn capture(cell: &str) -> Capture {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let service = format!("i143-2783-{cell}");
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", service.clone())]))
        .build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-2783")).with_filter(tracing_subscriber::filter::filter_fn(|m| m.name().starts_with("rdm.")));
    tracing::subscriber::set_global_default(tracing_subscriber::registry().with(layer)).expect("one global subscriber per cell process");
    Capture { exporter, provider, service }
}

impl Capture {
    fn spans(&self) -> Vec<Value> {
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|sp| {
                let attributes: serde_json::Map<String, Value> = sp.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": sp.name,
                    "trace_id": sp.span_context.trace_id().to_string(),
                    "span_id": sp.span_context.span_id().to_string(),
                    "parent_span_id": sp.parent_span_id.to_string(),
                    "resource": {"service.name": self.service},
                    "attributes": attributes,
                })
            })
            .collect()
    }
}

fn attr<'a>(sp: &'a Value, k: &str) -> &'a str {
    sp["attributes"][k].as_str().unwrap_or_default()
}

/// mesh1 with `admins` node-admins and `rpcs` rpc nodes, through the rectifier.
async fn estate(cell: &str, admins: u32, rpcs: u32) -> Estate {
    let estate = Estate::bootstrap(owner(cell), "fabric1", "mesh1").await;
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": admins, "rpc_node": rpcs}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(&s(&a["build_id"]), Duration::from_secs(120)).await;
    estate.settled_shape(&[("mesh1", admins, rpcs)], Duration::from_secs(60)).await;
    estate
}

async fn published(estate: &Estate, node: &str) -> ExactRuntime {
    let dir = estate.data_dir_of(node).await;
    ExactRuntime::published(std::path::Path::new(&dir)).unwrap_or_else(|r| panic!("{node}: no exact runtime published in {dir}: {r:?}"))
}

fn seats(nodes: &[Value]) -> (bool, String) {
    match seats_as_expected(nodes) {
        Ok(()) => (true, String::new()),
        Err(e) => (false, e),
    }
}

fn fabric_primary(nodes: &[Value]) -> String {
    advertised_fabric_primaries(nodes).join(",")
}

/// The highest attempt of Build `id` as the admin at the estate's control API holds it.
async fn attempt_of(estate: &Estate, id: &str) -> u64 {
    num(&estate.get(&format!("/api/builds?id={id}")).await.1["attempt"])
}

/// CONTRACT (#2783): the process fault backend acts on the exact runtime a birth published
/// (provider control domain + pid + start token), never on a pid alone, and the rectifier recovers
/// the current birth. A forged start token (a recycled pid) is refused by name with the process
/// untouched. A held (SIGSTOP) rpc node is alive and silent: the OS acknowledges the hold, the
/// view stops calling it ready, calls to it never complete, and no replacement, re-attempt or new
/// birth follows; the thaw returns the SAME birth, which serves the value stored before the hold
/// (the semantic wedge detector judges it). A killed rpc node and a killed non-fabric-primary
/// node-admin are terminal by observed exit only: the Fabric's authority opens the next attempt of
/// the SAME Build (reason proven drift, no Build minted), the path is re-created as a new
/// NodeId/incarnation at the same path.name through the deployment pipeline (every step a child of
/// its pipeline), the old NodeId never follows the path, and exactly one runtime serves each path.
/// What must NOT happen: a signal reaching a process the fact does not name, a held runtime
/// replaced, a Build minted for drift, the fabric-primary node-admin signalled, or a mesh left
/// without a node-admin.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_fault_backend_kills_exact_runtime_recovers_current_birth() {
    let dir = acceptance_dir(KILL_CELL);
    let cap = capture(KILL_CELL);
    // Three node-admins: the bootstrap admin is the harness's own control address and is never signalled,
    // and a launched admin that is neither primary is always left to kill.
    let mut estate = estate(KILL_CELL, 3, 3).await;
    let floor = rafka_mesh_transport::membership::staleness_floor();
    let build_id = s(&estate.get("/api/fabric").await.1["build_id"]);
    let nodes0 = estate.nodes().await;
    let rpcs: Vec<Value> = nodes0.iter().filter(|n| n["kind"] == "rpc_node").cloned().collect();
    assert_eq!(rpcs.len(), 3);
    let (hold_n, kill_n) = (&rpcs[0], &rpcs[1]);
    let (hold, kill) = (s(&hold_n["name"]), s(&kill_n["name"]));
    let attempt_before = attempt_of(&estate, &build_id).await;

    // Survivor evidence, complete before any authority disappears: the view, the Build and a value
    // stored on each victim.
    let hold_exact = format!("exact:{}", s(&hold_n["node_id"]));
    let kill_exact = format!("exact:{}", s(&kill_n["node_id"]));
    for (exact, inc) in [(&hold_exact, &hold_n["incarnation_id"]), (&kill_exact, &kill_n["incarnation_id"])] {
        let put = estate.probe(&["put", "--target", exact, "--key", "83", "--value", "before-fault"]);
        assert_eq!(put["outcome"], "Reply", "{put}");
        assert_eq!(put["reply"]["incarnation_id"], *inc, "{put}");
    }
    let survivors_before: BTreeSet<String> = nodes0.iter().map(|n| s(&n["node_id"])).collect();

    // ---- A forged start token: refused by name, nothing signalled.
    let hold_rt = published(&estate, &hold).await;
    let forged = hold_rt.with_start(hold_rt.start + 1).apply(Fault::Stop);
    assert!(matches!(forged, Err(FaultRefusal::NotThisRuntime { .. })), "a recycled pid is refused: {forged:?}");
    assert_ne!(proc_state(hold_rt.pid), Some('T'), "the process was not stopped");
    let foreign = ExactRuntime { control_domain: "process:another-host:1".into(), ..hold_rt.clone() }.apply(Fault::Kill);
    assert!(matches!(foreign, Err(FaultRefusal::ForeignDomain { .. })), "{foreign:?}");
    assert!(hold_rt.check().is_ok(), "the exact runtime is untouched");

    // ---- Hold: SIGSTOP of the exact runtime, judged by the semantic detector.
    let mut ev = Evidence::new(Family::SilentRuntime, format!("silent-runtime:stop-{hold}"));
    let (seats_before, detail_before) = seats(&nodes0);
    let fp_before = fabric_primary(&nodes0);
    let stopped = hold_rt.apply(Fault::Stop).expect("the exact runtime is held");
    assert_eq!(stopped.state_after, Some('T'));
    ev.primitive = Some(Primitive { armed: true, ack: serde_json::to_value(&stopped).unwrap() });
    let silent_view = wait_for(&format!("{hold} no longer ready in the public view"), floor * 2 + Duration::from_secs(30), || async { estate.node_opt(&hold).await.filter(|n| n["status"] != "ready-for-traffic") }).await;
    let mut reads = Vec::new();
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        let probe = estate.probe(&["get", "--target", &hold_exact, "--key", "83"]);
        reads.push(json!({"proc_state": proc_state(hold_rt.pid).map(String::from), "cpu_ticks": cpu_ticks(hold_rt.pid), "outcome": probe["outcome"]}).to_string());
        outcomes.push(probe);
    }
    let held_at_last_read = proc_state(hold_rt.pid) == Some('T');
    ev.fault = Some(FaultAck { held: true, names: Some(json!({"pid": hold_rt.pid, "start": hold_rt.start, "control_domain": hold_rt.control_domain, "view_status": silent_view["status"]})), held_at_last_read });
    ev.progress = Some(Progress { reads, complete_while_held: outcomes.iter().any(|o| o["outcome"] == "Reply") });
    ev.routing = Some(Routing { expected_routable: false, observed_routable: outcomes.iter().any(|o| o["outcome"] == "Reply"), observed: format!("view {}; calls {}", silent_view["status"], outcomes.iter().map(|o| s(&o["outcome"])).collect::<Vec<_>>().join(",")) });
    let nodes_during = estate.nodes().await;
    let (seats_during, detail_during) = seats(&nodes_during);
    let during = nodes_during.iter().find(|n| n["name"] == hold.as_str()).cloned();
    let attempt_during = attempt_of(&estate, &build_id).await;
    let alive = hold_rt.check().is_ok();
    let replaced = during.as_ref().is_none_or(|n| n["node_id"] != hold_n["node_id"]);
    let thawed = hold_rt.apply(Fault::Continue).expect("the hold is released");
    ev.release = Some(Release { acked: true, hold_ended: thawed.state_after != Some('T') });
    let back = wait_for(&format!("{hold} back as its own birth"), floor * 2 + Duration::from_secs(30), || async {
        estate.node_opt(&hold).await.filter(|n| n["incarnation_id"] == hold_n["incarnation_id"] && n["status"] == "ready-for-traffic")
    })
    .await;
    let kept = estate.probe(&["get", "--target", &hold_exact, "--key", "83"]);
    ev.recovery = Some(Recovery { work_complete: kept["outcome"] == "Reply", marker_after: json!({"proc_state": proc_state(hold_rt.pid).map(String::from), "outcome": kept["outcome"]}).to_string() });
    let nodes_after = estate.nodes().await;
    let (seats_after, detail_after) = seats(&nodes_after);
    ev.control = Some(Control {
        seats_as_expected: [seats_before, seats_during, seats_after],
        seats_detail: [detail_before, detail_during, detail_after.clone()].join(" | "),
        fabric_primary: [fp_before, fabric_primary(&nodes_during), fabric_primary(&nodes_after)],
        authority_may_move: false,
        incarnation: [s(&hold_n["incarnation_id"]), during.as_ref().map(|n| s(&n["incarnation_id"])).unwrap_or_default(), s(&back["incarnation_id"])],
        attempts: [attempt_before, attempt_during],
        exact_runtime_alive: alive,
        replaced_during: replaced,
    });
    let mut rec = Reconciliation::default();
    rec.check("the value stored before the hold is served after it", kept["reply"]["result"] == json!({"found": true, "value": "before-fault"}), kept.to_string());
    rec.check("the same birth serves it", kept["reply"]["incarnation_id"] == hold_n["incarnation_id"] && kept["reply"]["executing_node"] == hold_n["node_id"], kept.to_string());
    rec.check("the exact runtime is the one that was held (same domain, pid, start)", published(&estate, &hold).await == hold_rt, format!("{hold_rt:?}"));
    rec.check("the Build gained no attempt", attempt_of(&estate, &build_id).await == attempt_before, format!("attempt {attempt_before}"));
    rec.check("the seats equal the public candidates' after the thaw", seats_after, detail_after);
    ev.reconciliation = Some(rec);

    // ---- Kill: the exact runtime of another rpc node exits; the rectifier recovers the path.
    let kill_rt = published(&estate, &kill).await;
    let kill_dir = estate.data_dir_of(&kill).await;
    let killed = kill_rt.apply(Fault::Kill).expect("the exact runtime is killed");
    assert!(killed.exited, "the OS observed the exit: {killed:?}");
    assert!(matches!(kill_rt.apply(Fault::Kill), Err(FaultRefusal::AlreadyExited { .. })), "a second kill finds nothing");
    let replacement = wait_for("a new logical node holds the path", Duration::from_secs(120), || async {
        let n = estate.node_opt(&kill).await?;
        (n["status"] == "ready-for-traffic" && n["node_id"] != kill_n["node_id"]).then_some(n)
    })
    .await;
    let replacement_rt = published(&estate, &kill).await;
    assert_ne!((replacement_rt.pid, replacement_rt.start), (kill_rt.pid, kill_rt.start));
    let fresh = estate.probe(&["get", "--target", &format!("path:{kill}"), "--key", "83"]);
    assert_eq!(fresh["reply"]["result"], json!({"found": false}), "a replacement starts with an empty store: {fresh}");
    let old = estate.probe(&["get", "--target", &kill_exact, "--key", "83"]);
    assert_eq!(old, json!({"outcome": "NotSent", "reason": "Resolve(Unknown)", "route": "direct"}), "exact:<old> never follows a replacement: {old}");
    assert_eq!(estate.live_runtimes().iter().filter(|(d, _)| d.file_name().is_some_and(|f| f.to_string_lossy().starts_with(&format!("{kill}-")))).count(), 1, "one runtime serves the path");

    // ---- A node-admin that is neither mesh primary nor fabric primary: killed by exact runtime.
    let nodes = estate.nodes().await;
    let admin_n = nodes.iter().find(|n| n["kind"] == "node_admin" && n["is_primary"] == false && n["is_fabric_primary"] == false && n["name"] != "mesh1.admin.1").cloned().unwrap_or_else(|| panic!("a non-primary node-admin: {nodes:?}"));
    let admin = s(&admin_n["name"]);
    let admin_rt = published(&estate, &admin).await;
    let admin_killed = admin_rt.apply(Fault::Kill).expect("the admin's exact runtime is killed");
    assert!(admin_killed.exited);
    let admin_new = wait_for("a new logical node holds the admin's path", Duration::from_secs(120), || async {
        let n = estate.node_opt(&admin).await?;
        (n["status"] == "ready-for-traffic" && n["node_id"] != admin_n["node_id"]).then_some(n)
    })
    .await;
    estate.settled_shape(&[("mesh1", 3, 3)], Duration::from_secs(120)).await;
    let nodes_end = estate.nodes().await;
    let survivors_after: BTreeSet<String> = nodes_end.iter().map(|n| s(&n["node_id"])).collect();
    let untouched: Vec<&String> = survivors_before.iter().filter(|id| ![s(&kill_n["node_id"]), s(&admin_n["node_id"])].contains(id)).collect();
    assert!(untouched.iter().all(|id| survivors_after.contains(*id)), "no other node was replaced: before {survivors_before:?} after {survivors_after:?}");
    let seats_end = seats(&nodes_end);
    assert!(seats_end.0, "{}", seats_end.1);
    let build_end = s(&estate.get("/api/fabric").await.1["build_id"]);
    assert_eq!(build_end, build_id, "Fabric.build_id is the Build it was");
    estate.stop().await;

    // ---- Evidence: the estate's exported spans and this process's own.
    let spans = estate.spans();
    let reconciles: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| attr(r, "build_id") == build_id && num(&r["attributes"]["attempt"]) > attempt_before).collect();
    let recovery_for = |path: &str| -> Value {
        reconciles
            .iter()
            .find(|r| attr(r, "reason") == "proven-drift" && attr(r, "operations").contains(&format!("create-node:{path}")))
            .cloned()
            .cloned()
            .unwrap_or_else(|| panic!("a proven-drift attempt of {build_id} re-creating {path}: {reconciles:?}"))
    };
    let (rpc_rec, admin_rec) = (recovery_for(&kill), recovery_for(&admin));
    assert_eq!(attr(&rpc_rec, "build_id"), build_id);
    assert!(named(&spans, "rdm.node_admin.build.create.via-proven-drift").is_empty(), "no Build is minted for drift");
    let pipeline_of = |path: &str, attempt: u64| -> Value {
        named(&spans, "rdm.node_admin.deployment.update.via-pipeline")
            .into_iter()
            .find(|p| attr(p, "pipeline") == "create" && attr(p, "node") == path && attr(p, "build_id") == build_id && num(&p["attributes"]["attempt"]) == attempt)
            .cloned()
            .unwrap_or_else(|| panic!("a create pipeline span of {path} at attempt {attempt}"))
    };
    let mut recoveries = Vec::new();
    for (path, rec) in [(&kill, &rpc_rec), (&admin, &admin_rec)] {
        let attempt = num(&rec["attributes"]["attempt"]);
        let pipe = pipeline_of(path, attempt);
        let steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|st| st["parent_span_id"] == pipe["span_id"]).collect();
        assert!(!steps.is_empty(), "{path}: step spans are children of the create pipeline");
        assert!(steps.iter().all(|st| st["trace_id"] == pipe["trace_id"] && attr(st, "node") == path.as_str()), "{path}: every step shares the pipeline's trace");
        assert!(steps.iter().any(|st| attr(st, "step") == "WaitForBind" || attr(st, "step").to_lowercase().contains("bind")), "{path}: the birth waited for its bind: {:?}", steps.iter().map(|st| attr(st, "step")).collect::<Vec<_>>());
        recoveries.push(json!({
            "path": path, "attempt": rec["attributes"]["attempt"], "reason": rec["attributes"]["reason"], "action": rec["attributes"]["action"], "operations": rec["attributes"]["operations"],
            "reconcile": {"trace_id": rec["trace_id"], "span_id": rec["span_id"], "parent_span_id": rec["parent_span_id"], "service": rec["service"]},
            "pipeline": {"trace_id": pipe["trace_id"], "span_id": pipe["span_id"], "parent_span_id": pipe["parent_span_id"], "provider": pipe["attributes"]["provider"]},
            "steps": steps.iter().map(|st| json!({"step": st["attributes"]["step"], "outcome": st["attributes"]["outcome"], "span_id": st["span_id"], "parent_span_id": st["parent_span_id"], "trace_id": st["trace_id"]})).collect::<Vec<_>>(),
            "reconcile_is_in_a_trace_with_the_pipeline": descends_from(&spans, &pipe, rec) || pipe["trace_id"] == rec["trace_id"],
        }));
    }
    // The dispatches to the held node's births.
    for sp in named(&spans, "rdm.node_rpc.proof_store.serve.via-request").into_iter().filter(|sp| attr(sp, "node_id") == s(&hold_n["node_id"])) {
        ev.dispatches.push(Dispatch { birth: attr(sp, "incarnation_id").to_string(), current_birth: s(&hold_n["incarnation_id"]), after_supersession: false });
    }
    assert!(!ev.dispatches.is_empty(), "the held node's birth exported serve spans for the calls that reached it");
    let verdict = judge(&ev).unwrap_or_else(|r| panic!("the detector refused the held runtime: {r}"));
    let own = cap.spans();
    let fault_spans: Vec<&Value> = named(&own, "rdm.testkit.fault.update.via-process-signal");
    for f in ["stop", "continue", "kill"] {
        assert!(fault_spans.iter().any(|sp| attr(sp, "fault") == f), "a `{f}` fault span was emitted by the backend");
    }
    let refused_spans = fault_spans.iter().filter(|sp| attr(sp, "outcome").contains("not_this_runtime") || attr(sp, "outcome").contains("foreign_domain")).count();
    assert!(refused_spans >= 2, "the refusals left spans");
    let verdict_span = named(&own, "rdm.scenario.wedge.resolve.via-detector");
    assert!(!verdict_span.is_empty(), "the detector's verdict span was emitted");

    let result = json!({
        "cell": KILL_CELL,
        "build_id": build_id,
        "attempt_before": attempt_before,
        "forged_start": format!("{forged:?}"),
        "foreign_domain": format!("{foreign:?}"),
        "hold": {"node": hold, "runtime": hold_rt, "stopped": stopped, "thawed": thawed, "view_while_held": silent_view, "calls_while_held": outcomes, "after_thaw": kept, "verdict": serde_json::to_value(&verdict).unwrap(), "evidence": serde_json::to_value(&ev).unwrap()},
        "kill": {"node": kill, "killed": killed, "old": {"node_id": kill_n["node_id"], "incarnation_id": kill_n["incarnation_id"], "data_dir": kill_dir}, "replacement": {"node_id": replacement["node_id"], "incarnation_id": replacement["incarnation_id"], "runtime": replacement_rt}, "fresh": fresh["reply"], "old_exact_probe": old},
        "admin_kill": {"node": admin, "killed": admin_killed, "old_node_id": admin_n["node_id"], "new_node_id": admin_new["node_id"], "was_fabric_primary": admin_n["is_fabric_primary"], "was_mesh_primary": admin_n["is_primary"]},
        "recoveries": recoveries,
        "backend_spans": fault_spans.iter().map(|sp| json!({"fault": attr(sp, "fault"), "pid": attr(sp, "pid"), "outcome": attr(sp, "outcome"), "trace_id": sp["trace_id"], "span_id": sp["span_id"], "service": sp["resource"]["service.name"]})).collect::<Vec<_>>(),
        "detector_spans": verdict_span.iter().map(|sp| json!({"trace_id": sp["trace_id"], "span_id": sp["span_id"], "parent_span_id": sp["parent_span_id"]})).collect::<Vec<_>>(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&own).unwrap()).unwrap();
}
