//! i143.e8.s5 acceptance (rafka-v2 #2783, hardened 2026-10-07), CHAOS-PROCESS layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2783-chaos-process`, which exports `I143_ACCEPTANCE_DIR`
//! (each cell's `result.json` goes there) and whose commands set `RDM_ARTIFACTS_DIR` (the
//! estate's manifest, rpc ledger and every process's spans land under it, feature `i143-2783`).
//!
//! - `process_fault_backend_kills_exact_runtime_recovers_current_birth`: the process fault backend
//!   (`rafka_test_scenario::process_faults`) on a real estate.
//! - `process_port_allocator_survives_seeded_collisions`: seeded foreign processes squat ports of
//!   the allocator's range while births, restarts, kills and deletions run through the rectifier.
//!
//! Neither cell touches the fabric-primary node-admin, and no mesh loses every node-admin.

use opentelemetry::trace::TracerProvider as _;
use rafka_node_admin_core::deployment::endpoint::{port_holders, port_range_from_env, SlotTransport};
use rafka_test_scenario::elections::{advertised_fabric_primaries, seats_as_expected};
use rafka_test_scenario::estate::{binary, descends_from, named, wait_for, Estate, Owner};
use rafka_test_scenario::process_faults::{cpu_ticks, proc_state, ExactRuntime, Fault, Refusal as FaultRefusal};
use rafka_test_scenario::sim::Scheduler;
use rafka_test_scenario::wedge::*;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::io::BufRead;
use std::path::PathBuf;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer as _;

const KILL_CELL: &str = "process_fault_backend_kills_exact_runtime_recovers_current_birth";
const SOAK_CELL: &str = "process_port_allocator_survives_seeded_collisions";

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

// ---------------------------------------------------------------------------------------------
// The port-collision soak.
// ---------------------------------------------------------------------------------------------

/// A foreign process holding ports (`rafka-port-squatter`), alive until its stdin closes.
struct Squatter {
    child: std::process::Child,
    pid: u32,
    held: Vec<(String, u16)>,
    refused: Vec<Value>,
}

fn squat(tcp: &[u16], udp: &[u16]) -> Squatter {
    let list = |v: &[u16]| v.iter().map(u16::to_string).collect::<Vec<_>>().join(",");
    let mut child = std::process::Command::new(binary("rafka-port-squatter"))
        .args(["--tcp", &list(tcp), "--udp", &list(udp)])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .expect("spawn rafka-port-squatter");
    let mut line = String::new();
    std::io::BufReader::new(child.stdout.take().unwrap()).read_line(&mut line).expect("the squatter reports what it holds");
    let v: Value = serde_json::from_str(line.trim()).unwrap_or_else(|e| panic!("squatter printed no JSON ({e}): {line}"));
    let held = v["held"].as_array().into_iter().flatten().map(|h| (s(&h["proto"]), h["port"].as_u64().unwrap() as u16)).collect();
    Squatter { pid: child.id(), child, held, refused: v["refused"].as_array().cloned().unwrap_or_default() }
}

impl Squatter {
    fn ports(&self) -> BTreeSet<u16> {
        self.held.iter().map(|(_, p)| *p).collect()
    }
    fn end(mut self) {
        drop(self.child.stdin.take());
        let _ = self.child.wait();
    }
}

fn claims_dir() -> PathBuf {
    std::env::temp_dir().join("rafka-endpoint-ports")
}

fn claim_path(port: u16) -> PathBuf {
    claims_dir().join(format!("127.0.0.1-{port}"))
}

fn claim_owner(port: u16) -> Option<u32> {
    std::fs::read_to_string(claim_path(port)).ok().and_then(|t| t.split_whitespace().next().and_then(|p| p.parse().ok()))
}

fn live_claim(port: u16) -> bool {
    claim_owner(port).is_some_and(|p| std::path::Path::new(&format!("/proc/{p}")).exists())
}

/// Every socket port an estate's nodes advertise, by node.
fn assigned(nodes: &[Value]) -> BTreeMap<String, Vec<u16>> {
    nodes
        .iter()
        .map(|n| {
            let mut ports: Vec<u16> = n["transport_addr"].as_str().and_then(|a| a.rsplit(':').next()).and_then(|p| p.parse().ok()).into_iter().collect();
            for l in n["listeners"].as_array().into_iter().flatten() {
                if let Some(p) = l[1].as_str().and_then(|a| a.rsplit(':').next()).and_then(|p| p.parse().ok()) {
                    ports.push(p);
                }
            }
            (s(&n["name"]), ports)
        })
        .collect()
}

/// One round's raw draws. A fixed number of draws per round: the schedule never depends on what
/// the estate answered, so one seed is one schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Draws {
    op: u64,
    squats: u64,
    offsets: Vec<u64>,
    protos: Vec<u64>,
    stale: u64,
    pick: u64,
    probe: u64,
}

const WINDOW: u64 = 24;
const MAX_SQUATS: usize = 12;

fn draw_round(sch: &mut Scheduler) -> Draws {
    let op = sch.draw("op", 4);
    let squats = sch.draw("squats", 10) + 3;
    let offsets = (0..MAX_SQUATS).map(|_| sch.draw("squat-offset", WINDOW)).collect();
    let protos = (0..MAX_SQUATS).map(|_| sch.draw("squat-proto", 3)).collect();
    let stale = sch.draw("stale-claims", 3);
    let pick = sch.draw("pick", 1000);
    let probe = sch.draw("probe", 1000);
    Draws { op, squats, offsets, protos, stale, pick, probe }
}

fn schedule(seed: u64, rounds: u64) -> (Vec<Draws>, Vec<Value>) {
    let mut sch = Scheduler::new(seed);
    let all = (0..rounds).map(|_| draw_round(&mut sch)).collect();
    (all, sch.events_json())
}

/// CONTRACT (#2783): seeded foreign processes squat ports of the allocator's range while births,
/// restarts (a restart binds fresh ports, like a birth), exact-runtime kills and deletions run through the Build rectifier on the process
/// provider. Each round a seeded set of ports (always the very next port the allocator would hand
/// out, a seeded few ahead of it in TCP-only, UDP-only or both, every free port below it, and a
/// few stale claim files of a dead process) is held by a real foreign process; then one seeded
/// operation runs. After it: every node is ready; no two nodes advertise one socket; no advertised
/// socket is a squatted port; every advertised socket is held by its own runtime and by no other
/// process; every squatter still holds its ports; each live runtime is a node's (no orphan); a
/// restart keeps its addresses; and a second foreign bind of a port a live runtime holds is
/// refused by the OS (EADDRINUSE). A stale claim of a dead process is taken over, never honoured.
/// The schedule is drawn from one seed with a fixed number of draws per round, so the same seed
/// is the same schedule; the classified outcomes are recorded for comparison. What must NOT
/// happen: a handed-out port that something holds, a duplicated allocation, a birth that does not
/// bind, an orphan runtime, a failed replay. The story names no soak length: `RDM_COLLISION_ROUNDS`
/// (default 60) sets the rounds, `RDM_COLLISION_SEED` (default 2783) the seed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn process_port_allocator_survives_seeded_collisions() {
    let dir = acceptance_dir(SOAK_CELL);
    let rounds: u64 = std::env::var("RDM_COLLISION_ROUNDS").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    let seed: u64 = std::env::var("RDM_COLLISION_SEED").ok().and_then(|v| v.parse().ok()).unwrap_or(2783);
    let rerun = format!("RDM_COLLISION_SEED={seed} RDM_COLLISION_ROUNDS={rounds} cargo test -p rafka-test-scenario --test i143_acceptance_2783 {SOAK_CELL} -- --exact");
    eprintln!("COLLISION seed={seed} rounds={rounds}  (rerun: {rerun})");
    let (draws, events) = schedule(seed, rounds);
    assert_eq!((draws.clone(), events.clone()), schedule(seed, rounds), "one seed draws one schedule");
    assert_ne!(events, schedule(seed + 1, rounds).1, "the seed decides the schedule");
    let (first, last) = port_range_from_env();
    let cap = capture(SOAK_CELL);
    let mut estate = estate(SOAK_CELL, 2, 2).await;
    estate.set_seed(seed);

    let bootstrap_pid = u64::from(estate.bootstrap_pid().expect("the bootstrap admin runs"));
    let mut names: BTreeSet<String> = estate.nodes().await.iter().map(|n| s(&n["name"])).collect();
    let in_range = |p: &u16| *p >= first && *p <= last;
    let mut hwm: u16 = assigned(&estate.nodes().await).values().flatten().copied().filter(in_range).max().unwrap_or(first - 1);
    let mut squatters: Vec<Squatter> = Vec::new();
    let mut stale_files: BTreeSet<u16> = BTreeSet::new();
    let dead_pid = {
        let mut c = std::process::Command::new("true").spawn().unwrap();
        c.wait().unwrap();
        c.id()
    };
    let mut classes: Vec<String> = Vec::new();
    let mut rows: Vec<Value> = Vec::new();
    let (mut skipped_total, mut birth_rounds, mut stale_taken_total, mut refused_total) = (0usize, 0usize, 0usize, 0usize);
    let mut violations: Vec<String> = Vec::new();

    for (round, d) in draws.iter().enumerate() {
        let before = estate.nodes().await;
        let before_assigned = assigned(&before);
        let in_use: BTreeSet<u16> = before_assigned.values().flatten().copied().filter(in_range).collect();
        let all_squatted = |sq: &[Squatter]| sq.iter().flat_map(|q| q.ports()).collect::<BTreeSet<u16>>();
        let already = all_squatted(&squatters);

        // The squats: the next port the allocator would hand out, a seeded few ahead of it, every
        // free port below it.
        let mut ahead: BTreeMap<u16, u64> = BTreeMap::new();
        if !already.contains(&(hwm + 1)) {
            ahead.insert(hwm + 1, d.protos[0]);
        }
        for i in 1..(d.squats as usize).min(MAX_SQUATS) {
            let p = hwm + 1 + (d.offsets[i] as u16);
            if p <= last && !in_use.contains(&p) && !already.contains(&p) {
                ahead.entry(p).or_insert(d.protos[i]);
            }
        }
        let gaps: Vec<u16> = (first..=hwm).filter(|p| !in_use.contains(p) && !already.contains(p) && !live_claim(*p)).collect();
        let (mut tcp, mut udp) = (Vec::new(), Vec::new());
        for (p, proto) in ahead.iter() {
            match proto {
                0 => tcp.push(*p),
                1 => udp.push(*p),
                _ => {
                    tcp.push(*p);
                    udp.push(*p);
                }
            }
        }
        for p in &gaps {
            tcp.push(*p);
            udp.push(*p);
        }
        let sq = squat(&tcp, &udp);
        refused_total += sq.refused.len();
        let squatted_now = sq.ports();
        squatters.push(sq);
        // Stale claim files of a dead process, ahead of the allocator.
        let mut stale: BTreeSet<u16> = BTreeSet::new();
        // The lowest free ports above the allocator's frontier carry the stale claims: the next
        // birth reaches them first and must take them over.
        let mut p = hwm + 1;
        while stale.len() < d.stale as usize && p <= last {
            if !in_use.contains(&p) && !already.contains(&p) && !squatted_now.contains(&p) && claim_owner(p).is_none() {
                std::fs::create_dir_all(claims_dir()).unwrap();
                std::fs::write(claim_path(p), dead_pid.to_string()).unwrap();
                stale.insert(p);
                stale_files.insert(p);
            }
            p += 1;
        }
        let squatted_all = all_squatted(&squatters);

        // The operation, through the rectifier (or the exact fault backend).
        let rpcs: Vec<String> = names.iter().filter(|n| n.contains(".rpc.")).cloned().collect();
        let pick = |k: u64| rpcs[(k as usize) % rpcs.len()].clone();
        let op = match d.op {
            0 if rpcs.len() < 5 => "spawn",
            0 | 3 if rpcs.len() > 2 => "delete",
            3 => "spawn",
            1 => "restart",
            _ => "kill",
        };
        let target = if op == "spawn" { String::new() } else { pick(d.pick) };
        let old = before.iter().find(|n| n["name"] == target.as_str()).cloned();
        let new_node: Option<Value> = match op {
            "spawn" => {
                let (status, a) = estate.post("/api/nodes/spawn", &json!({"mesh": "mesh1", "kind": "rpc_node"})).await;
                assert_eq!(status, 202, "round {round}: spawn: {a}");
                estate.await_build(&s(&a["build_id"]), Duration::from_secs(120)).await;
                let n = wait_for("the spawned node is ready", Duration::from_secs(60), || async {
                    estate.nodes().await.into_iter().find(|n| !names.contains(&s(&n["name"])) && n["status"] == "ready-for-traffic")
                })
                .await;
                names.insert(s(&n["name"]));
                Some(n)
            }
            "restart" => {
                let old = old.clone().unwrap();
                let (status, r) = estate.post(&format!("/api/nodes/{target}/restart"), &json!({})).await;
                assert_eq!(status, 202, "round {round}: restart {target}: {r}");
                estate.await_build(&s(&r["build_id"]), Duration::from_secs(120)).await;
                Some(wait_for("the restarted birth is ready", Duration::from_secs(60), || async {
                    estate.node_opt(&target).await.filter(|n| n["status"] == "ready-for-traffic" && n["incarnation_id"] != old["incarnation_id"])
                })
                .await)
            }
            "delete" => {
                let (status, r) = estate.delete(&format!("/api/nodes/{target}")).await;
                assert_eq!(status, 202, "round {round}: delete {target}: {r}");
                estate.await_build(&s(&r["build_id"]), Duration::from_secs(120)).await;
                wait_for(&format!("{target} left the view"), Duration::from_secs(60), || async { estate.node_opt(&target).await.is_none().then_some(()) }).await;
                names.remove(&target);
                None
            }
            _ => {
                let old = old.clone().unwrap();
                let rt = published(&estate, &target).await;
                let killed = rt.apply(Fault::Kill).unwrap_or_else(|r| panic!("round {round}: kill {target}: {r:?}"));
                assert!(killed.exited);
                Some(wait_for("the path is re-created", Duration::from_secs(120), || async {
                    estate.node_opt(&target).await.filter(|n| n["status"] == "ready-for-traffic" && n["node_id"] != old["node_id"])
                })
                .await)
            }
        };
        estate.settled(&names, Duration::from_secs(120)).await;

        // The invariants.
        let nodes = estate.nodes().await;
        let now = assigned(&nodes);
        let mut v: Vec<String> = Vec::new();
        let mut seen: BTreeMap<u16, String> = BTreeMap::new();
        for (node, ports) in &now {
            for p in ports {
                if let Some(other) = seen.insert(*p, node.clone()) {
                    v.push(format!("port {p} is advertised by {other} and {node}"));
                }
                if squatted_all.contains(p) {
                    v.push(format!("{node} was handed squatted port {p}"));
                }
            }
        }
        let mut runtimes: BTreeMap<String, u64> = BTreeMap::new();
        for n in &nodes {
            let name = s(&n["name"]);
            let pid = if name == "mesh1.admin.1" { bootstrap_pid } else { u64::from(published(&estate, &name).await.pid) };
            runtimes.insert(name.clone(), pid);
            let addr = n["transport_addr"].as_str().and_then(|a| a.parse::<std::net::SocketAddr>().ok());
            let Some(addr) = addr else { v.push(format!("{name} advertises no transport")); continue };
            let holders = port_holders(addr, SlotTransport::Udp);
            if holders.is_empty() || holders.iter().any(|h| h.pid != Some(pid as u32)) {
                v.push(format!("{name}: transport {addr} is held by {holders:?}, not only by its runtime pid {pid}; {}", socket_diagnosis(pid as u32, addr.port())));
            }
            for l in n["listeners"].as_array().into_iter().flatten() {
                if let Some(la) = l[1].as_str().and_then(|a| a.parse::<std::net::SocketAddr>().ok()) {
                    let h = port_holders(la, SlotTransport::Tcp);
                    if h.iter().filter(|x| x.state == "listen").any(|x| x.pid != Some(pid as u32)) || !h.iter().any(|x| x.pid == Some(pid as u32)) {
                        v.push(format!("{name}: listener {la} is held by {h:?}, not only by its runtime pid {pid}"));
                    }
                }
            }
        }
        let live: BTreeSet<u64> = estate.live_runtimes().iter().map(|(_, p)| u64::from(*p)).collect();
        let expected: BTreeSet<u64> = runtimes.values().copied().filter(|p| *p != bootstrap_pid).collect();
        if live != expected {
            v.push(format!("live runtimes {live:?} are not the nodes' {expected:?}"));
        }
        // Every squatter still holds what it took (a sample).
        for q in &squatters {
            if !std::path::Path::new(&format!("/proc/{}", q.pid)).exists() {
                v.push(format!("squatter {} died", q.pid));
                continue;
            }
            for (proto, p) in q.held.iter().take(3) {
                let t = if proto == "tcp" { SlotTransport::Tcp } else { SlotTransport::Udp };
                let h = port_holders(std::net::SocketAddr::from(([127, 0, 0, 1], *p)), t);
                if !h.iter().any(|x| x.pid == Some(q.pid)) {
                    v.push(format!("squatter {} lost {proto} {p}: {h:?}", q.pid));
                }
            }
        }
        // A second foreign bind of a port a live runtime holds: the OS refuses it.
        let victim = {
            let names_now: Vec<&String> = now.keys().collect();
            names_now[(d.probe as usize) % names_now.len()].clone()
        };
        let vp = now[&victim][0];
        let probe = squat(&[], &[vp]);
        let eaddrinuse = probe.refused.len() == 1 && probe.refused[0]["os_error"] == 98 && probe.held.is_empty();
        if !eaddrinuse {
            v.push(format!("a foreign bind of {victim}'s port {vp} was not refused with EADDRINUSE: held {:?} refused {:?}", probe.held, probe.refused));
        }
        probe.end();

        // What the round did to the allocator, classified.
        let new_ports: Vec<u16> = match (&new_node, op) {
            (Some(n), "spawn" | "kill" | "restart") => now.get(&s(&n["name"])).cloned().unwrap_or_default(),
            _ => Vec::new(),
        };
        let skipped: Vec<u16> = match new_ports.iter().copied().max() {
            Some(top) => squatted_all.iter().copied().filter(|p| *p > hwm && *p < top).collect(),
            None => Vec::new(),
        };
        let stale_taken: Vec<u16> = stale.iter().copied().filter(|p| new_ports.contains(p)).collect();
        let class = match op {
            "restart" => if skipped.is_empty() { "restart_bound_fresh_no_squat_in_path" } else { "restart_skipped_squats_and_bound_fresh" },
            "delete" => "delete_released_addresses",
            "spawn" => if skipped.is_empty() { "spawn_bound_no_squat_in_path" } else { "spawn_skipped_squats_and_bound" },
            _ => if skipped.is_empty() { "replacement_bound_no_squat_in_path" } else { "replacement_skipped_squats_and_bound" },
        };
        if matches!(op, "spawn" | "kill" | "restart") {
            birth_rounds += 1;
            skipped_total += skipped.len();
        }
        stale_taken_total += stale_taken.len();
        classes.push(class.to_string());
        hwm = hwm.max(now.values().flatten().copied().filter(in_range).max().unwrap_or(hwm));
        rows.push(json!({
            "round": round, "op": op, "target": target, "class": class,
            "squatted_ahead": ahead.keys().collect::<Vec<_>>(), "squatted_gaps": gaps.len(), "stale_claims": stale, "stale_taken": stale_taken,
            "new_ports": new_ports, "squats_skipped_by_the_allocator": skipped, "hwm": hwm, "eaddrinuse_probe": {"node": victim, "port": vp, "refused": eaddrinuse},
            "squatter_refused_binds": squatters.last().map(|q| q.refused.clone()).unwrap_or_default(),
        }));
        eprintln!("COLLISION round {round} op={op} class={class} skipped={} violations={}", skipped.len(), v.len());
        violations.extend(v.into_iter().map(|x| format!("round {round} ({op} {target}): {x}")));
        if !violations.is_empty() {
            break;
        }
    }

    // The runs' end: nothing bound twice, no refusal in any node's own logs.
    let held_by_squatters: BTreeSet<u16> = squatters.iter().flat_map(|q| q.ports()).collect();
    let squatter_count = squatters.len();
    for q in squatters {
        q.end();
    }
    for p in &stale_files {
        if claim_owner(*p) == Some(dead_pid) {
            let _ = std::fs::remove_file(claim_path(*p));
        }
    }
    let root = estate.root.clone();
    let mut bind_errors = Vec::new();
    for e in walk_logs(&root) {
        for line in std::fs::read_to_string(&e).unwrap_or_default().lines().filter(|l| l.contains("Address already in use") || l.contains("AddrInUse")) {
            bind_errors.push(format!("{}: {line}", e.display()));
        }
    }
    if !bind_errors.is_empty() {
        violations.push(format!("a runtime hit EADDRINUSE: {bind_errors:?}"));
    }
    // The iroh `UnknownIssuer` mechanism (a caller keeping a restarted birth's old address that a
    // foreign process now holds): every log and every exported span is scanned for it.
    let mut unknown_issuer: Vec<String> = Vec::new();
    for e in walk_logs(&root) {
        for line in std::fs::read_to_string(&e).unwrap_or_default().lines().filter(|l| l.contains("UnknownIssuer")) {
            unknown_issuer.push(format!("{}: {}", e.display(), line.chars().take(400).collect::<String>()));
        }
    }
    estate.stop().await;
    let spans = estate.spans();
    for sp in &spans {
        if sp.to_string().contains("UnknownIssuer") {
            unknown_issuer.push(format!("span {} {}", sp["name"], sp["span_id"]));
        }
    }
    for e in walk_logs(&root) {
        for line in std::fs::read_to_string(&e).unwrap_or_default().lines().filter(|l| l.contains("UnknownIssuer")) {
            let l = format!("{}: {}", e.display(), line.chars().take(400).collect::<String>());
            if !unknown_issuer.contains(&l) {
                unknown_issuer.push(l);
            }
        }
    }
    let bind_steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|st| attr(st, "step") == "WaitForBind").collect();
    let failed_binds: Vec<&&Value> = bind_steps.iter().filter(|st| attr(st, "outcome") != "complete").collect();
    if !failed_binds.is_empty() {
        violations.push(format!("WaitForBind did not complete: {:?}", failed_binds.iter().map(|st| json!({"node": attr(st, "node"), "outcome": attr(st, "outcome")})).collect::<Vec<_>>()));
    }
    let own = cap.spans();
    let result = json!({
        "cell": SOAK_CELL, "seed": seed, "rounds_run": rows.len(), "rounds_planned": rounds, "rerun": rerun,
        "port_range": [first, last], "story_names_no_length": true,
        "schedule": {"events": events, "draws": draws.len()},
        "classified_outcomes": classes,
        "totals": {"birth_rounds": birth_rounds, "squats_skipped_by_the_allocator": skipped_total, "stale_claims_taken_over": stale_taken_total, "squatter_binds_refused": refused_total, "squatter_processes": squatter_count, "distinct_ports_squatted": held_by_squatters.len(), "wait_for_bind_steps": bind_steps.len()},
        "rows": rows, "violations": violations,
        "backend_spans": named(&own, "rdm.testkit.fault.update.via-process-signal").len(),
        "unknown_issuer": {"scanned": "every .log under the estate root (before and after the estate stopped) and every exported span", "matches": unknown_issuer.len(), "lines": unknown_issuer},
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&own).unwrap()).unwrap();
    assert!(violations.is_empty(), "the allocator collided:\n{}\nreplay: {rerun}", violations.join("\n"));
    assert!(skipped_total > 0, "no squat was ever in the allocator's path: the soak proved nothing (replay: {rerun})");
}

/// What the OS says about a runtime and a port: the runtime's state, its own socket inodes, and
/// every table line of the port (the evidence behind a holder mismatch).
fn socket_diagnosis(pid: u32, port: u16) -> String {
    let inodes: Vec<String> = std::fs::read_dir(format!("/proc/{pid}/fd"))
        .map(|d| d.flatten().filter_map(|e| std::fs::read_link(e.path()).ok()).map(|t| t.to_string_lossy().to_string()).filter(|t| t.starts_with("socket:")).collect())
        .unwrap_or_default();
    let hex = format!("{port:04X}");
    let lines: Vec<String> = ["/proc/net/udp", "/proc/net/udp6", "/proc/net/tcp", "/proc/net/tcp6"]
        .iter()
        .flat_map(|t| std::fs::read_to_string(t).unwrap_or_default().lines().skip(1).filter(|l| l.split_whitespace().nth(1).is_some_and(|a| a.ends_with(&format!(":{hex}")))).map(|l| format!("{t}: {}", l.split_whitespace().take(10).collect::<Vec<_>>().join(" "))).collect::<Vec<_>>())
        .collect();
    format!("runtime pid {pid} state {:?}, its sockets {inodes:?}, table lines {lines:?}", proc_state(pid))
}

fn walk_logs(root: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else if p.extension().is_some_and(|x| x == "log") {
                out.push(p);
            }
        }
    }
    out
}
