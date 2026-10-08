//! i143.e8.s4 acceptance (rafka-v2 #2782, hardened 2026-10-07): the semantic wedge detectors.
//!
//! - UNIT `wedge_detector_rejects_fault_without_progress_consequence`: run by
//!   `scripts/i143-acceptance-gate.sh i143-2782-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//!   cell leaves `result.json` (its direct observations) and `spans.json` (every span the
//!   detector emitted, captured in process by the evidence exporter) there.
//! - CHAOS-PROCESS `wedge_detector_proves_stall_and_stateful_recovery`: run by
//!   `scripts/i143-acceptance-gate.sh i143-2782-process`, whose command sets `RDM_ARTIFACTS_DIR`
//!   (the estate's manifest and every process's spans land under it, feature `i143-2782`).
//!
//! The detectors are `rafka_test_scenario::wedge`: they judge a cut by the consequences it leaves,
//! never by the injector's success and never by a duration.

use opentelemetry::trace::TracerProvider as _;
use rafka_node_rpc_contract::outcome::{IndeterminateReason, NotSentReason, PreCommit, RequestFinished};
use rafka_node_rpc_contract::ping::Ping;
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_test_scenario::ledger::{Ledger, MutationIntent};
use rafka_test_scenario::wedge::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::Layer as _;

fn acceptance_dir(layer: &str, cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2782").join(layer).join(cell),
    }
}

// ---------------------------------------------------------------------------------------------
// UNIT: the detector refuses every wedge that lacks a consequence, by name.
// ---------------------------------------------------------------------------------------------

/// This cell's own span capture: an in-memory OTel exporter behind a dispatcher, so the spans are
/// exactly the ones the detector emitted for this cell.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
    service: String,
}

fn capture(cell: &str) -> Capture {
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let service = format!("i143-2782-{cell}");
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", service.clone())]))
        .build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-2782")).with_filter(tracing_subscriber::filter::filter_fn(|m| m.name().starts_with("rdm.")));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)), service }
}

impl Capture {
    /// Every finished span, as exported: name, TraceId, SpanId, ParentSpanId, resource, attributes.
    fn spans(&self) -> Vec<Value> {
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": s.name,
                    "trace_id": s.span_context.trace_id().to_string(),
                    "span_id": s.span_context.span_id().to_string(),
                    "parent_span_id": s.parent_span_id.to_string(),
                    "resource": {"service.name": self.service},
                    "attributes": attributes,
                })
            })
            .collect()
    }
}

/// A request-family ledger: one acknowledged put (the held proof mutation) and its final state.
fn request_ledger(lost: bool) -> (Ledger, BTreeMap<String, Vec<u8>>) {
    let committed = || PreCommit::begin(Ping::OP).commit(RequestFinished::after_clean_finish(8, 8, true).unwrap());
    let mut ledger = Ledger::new();
    let put = ledger.issue(Ping::OP, "node-held", Some(MutationIntent { key: "41".into(), value: b"held-put".to_vec() }));
    let cut = ledger.issue(Ping::OP, "node-held", Some(MutationIntent { key: "42".into(), value: b"never-sent".to_vec() }));
    let lost_reply = ledger.issue(Ping::OP, "node-held", Some(MutationIntent { key: "43".into(), value: b"lost-reply".to_vec() }));
    ledger.classify(put, &committed().reply::<Ping>(&Ping::encode_reply(&rafka_node_rpc_contract::ping::PingReply::Pong { payload: vec![1] }).unwrap())).unwrap();
    ledger.classify(cut, &PreCommit::begin(Ping::OP).not_sent::<rafka_node_rpc_contract::ping::PingReply>(NotSentReason::Deadline)).unwrap();
    ledger.classify(lost_reply, &committed().indeterminate::<rafka_node_rpc_contract::ping::PingReply>(IndeterminateReason::ReplyLost("held".into()))).unwrap();
    let mut state = BTreeMap::new();
    if !lost {
        state.insert("41".to_string(), b"held-put".to_vec());
    }
    state.insert("43".to_string(), b"lost-reply".to_vec());
    (ledger, state)
}

/// The evidence of a wedge that proved: every consequence present and holding.
fn healthy(family: Family) -> Evidence {
    let cut = format!("{}:planted", family.name());
    let mut e = Evidence::new(family, cut);
    e.primitive = Some(Primitive { armed: true, ack: json!({"armed": e.cut}) });
    e.fault = Some(FaultAck { held: true, names: Some(json!({"call": "the held call"})), held_at_last_read: true });
    let marker = build_marker("running", 1, &["AllocateIdentity".to_string()]);
    e.progress = Some(Progress { reads: vec![marker.clone(), marker.clone(), marker], complete_while_held: false });
    e.routing = Some(Routing { expected_routable: false, observed_routable: false, observed: "absent from the view".into() });
    e.control = Some(Control {
        seats_as_expected: [true; 3],
        seats_detail: String::new(),
        fabric_primary: ["mesh1.admin.1".into(), "mesh1.admin.1".into(), "mesh1.admin.1".into()],
        authority_may_move: false,
        incarnation: ["inc-a".into(), "inc-a".into(), "inc-a".into()],
        attempts: [1, 1],
        exact_runtime_alive: true,
        replaced_during: false,
    });
    e.release = Some(Release { acked: true, hold_ended: true });
    e.recovery = Some(Recovery { work_complete: true, marker_after: build_marker("complete", 1, &["AllocateIdentity".to_string(), "Complete".to_string()]) });
    let mut r = Reconciliation::default();
    r.check("each step receipt once", true, "one receipt per step");
    r.check("one runtime serves the node", true, "1");
    if matches!(family, Family::RequestPreSend | Family::RequestPostApply) {
        let (ledger, state) = request_ledger(false);
        r.ledger(&ledger, &state);
    }
    e.reconciliation = Some(r);
    e
}

/// Planted wedges: each takes healthy evidence and removes or falsifies exactly one consequence.
type Plant = fn(&mut Evidence);

fn plants() -> Vec<(&'static str, &'static str, Plant)> {
    vec![
        ("primitive success alone", PRIMITIVE_SUCCESS_ALONE, |e| {
            e.fault = None;
            e.progress = None;
            e.routing = None;
            e.control = None;
            e.release = None;
            e.recovery = None;
            e.reconciliation = None;
        }),
        ("no active-fault ack", NO_ACTIVE_FAULT_ACK, |e| e.fault = None),
        ("fault reported not held", NO_ACTIVE_FAULT_ACK, |e| e.fault.as_mut().unwrap().held = false),
        ("fault held without naming the call", NO_ACTIVE_FAULT_ACK, |e| e.fault.as_mut().unwrap().names = None),
        ("fault no longer held at the last read", NO_ACTIVE_FAULT_ACK, |e| e.fault.as_mut().unwrap().held_at_last_read = false),
        // The planted false positive: the injector reports an active hold and the work advances anyway.
        ("false-positive fault: progress advances", NO_PROGRESS_CONSEQUENCE, |e| {
            let p = e.progress.as_mut().unwrap();
            p.reads[2] = build_marker("running", 1, &["AllocateIdentity".to_string(), "PrepareStorage".to_string()]);
        }),
        ("false-positive fault: work completes while held", NO_PROGRESS_CONSEQUENCE, |e| e.progress.as_mut().unwrap().complete_while_held = true),
        ("no progress read", NO_PROGRESS_CONSEQUENCE, |e| e.progress = None),
        ("a single progress read", NO_PROGRESS_CONSEQUENCE, |e| e.progress.as_mut().unwrap().reads.truncate(1)),
        ("no routing effect", NO_ROUTING_CONSEQUENCE, |e| e.routing = None),
        ("routing effect contradicts the cut", NO_ROUTING_CONSEQUENCE, |e| e.routing.as_mut().unwrap().observed_routable = true),
        ("no control effect", NO_CONTROL_CONSEQUENCE, |e| e.control = None),
        ("seats diverge from the public candidates", NO_CONTROL_CONSEQUENCE, |e| {
            let c = e.control.as_mut().unwrap();
            c.seats_as_expected[1] = false;
            c.seats_detail = "fabric primary: advertised [\"mesh1.admin.2\"], expected Some(\"mesh1.admin.1\")".into();
        }),
        ("authority moves across a stall of a live admin", NO_CONTROL_CONSEQUENCE, |e| e.control.as_mut().unwrap().fabric_primary[2] = "mesh1.admin.2".into()),
        ("a stalled membership change moves authority and the public candidates disagree", NO_CONTROL_CONSEQUENCE, |e| {
            let c = e.control.as_mut().unwrap();
            c.authority_may_move = true;
            c.fabric_primary[2] = "mesh2.admin.1".into();
            c.seats_as_expected[2] = false;
            c.seats_detail = "fabric primary: advertised [\"mesh2.admin.1\"], expected Some(\"mesh1.admin.1\")".into();
        }),
        ("a running silent runtime is replaced", SILENT_RUNTIME_JUDGED_DEAD, |e| e.control.as_mut().unwrap().replaced_during = true),
        ("a running silent runtime is re-born", SILENT_RUNTIME_JUDGED_DEAD, |e| e.control.as_mut().unwrap().incarnation[2] = "inc-b".into()),
        ("a running silent runtime gets a new attempt", SILENT_RUNTIME_JUDGED_DEAD, |e| e.control.as_mut().unwrap().attempts[1] = 2),
        ("never released", NO_RELEASE, |e| e.release = None),
        ("release not acknowledged", NO_RELEASE, |e| e.release.as_mut().unwrap().acked = false),
        ("hold does not end", NO_RELEASE, |e| e.release.as_mut().unwrap().hold_ended = false),
        ("no recovery observed", NO_RECOVERY, |e| e.recovery = None),
        ("held work never completes", NO_RECOVERY, |e| e.recovery.as_mut().unwrap().work_complete = false),
        ("release changes nothing", NO_RECOVERY, |e| {
            let held = e.progress.as_ref().unwrap().reads[0].clone();
            e.recovery.as_mut().unwrap().marker_after = held;
        }),
        ("no reconciliation", NO_RECONCILIATION, |e| e.reconciliation = None),
        ("empty reconciliation", NO_RECONCILIATION, |e| e.reconciliation = Some(Reconciliation::default())),
        ("a reconciliation check fails", NO_RECONCILIATION, |e| e.reconciliation.as_mut().unwrap().check("fabric pointer names the Build", false, "names build-0, expected build-1")),
        ("dispatch to a superseded birth", SUPERSEDED_BIRTH_DISPATCHED, |e| {
            e.dispatches.push(Dispatch { birth: "inc-old".into(), current_birth: "inc-a".into(), after_supersession: true });
        }),
    ]
}

/// CONTRACT (#2782): the wedge detector judges a cut by the consequences it leaves. For every wedge
/// family of PRD §12 (and the silent runtime and the fabric-primary stall), evidence in which every
/// consequence holds (the injector's acknowledgement that the fault is active, progress stalled
/// across two or more reads while held, the typed routing effect, the public control effect, the
/// release, the recovery, the reconciled final state) passes; and for each planted wedge that lacks
/// one consequence the detector refuses with exactly that invariant, by name, and no other: the
/// injector's success alone, a missing or false active-fault acknowledgement, a false-positive fault
/// report whose work advances or finishes while reported held, a wrong routing effect, diverging
/// seats or a moved fabric primary, a running silent runtime judged dead, a missing release or
/// recovery, a failed reconciliation (including the RPC ledger's lost applied mutation), and a
/// dispatch to a superseded birth. What must NOT happen: a refusal that names another invariant, a
/// pass over a missing consequence, or a verdict that depends on a duration.
#[test]
fn wedge_detector_rejects_fault_without_progress_consequence() {
    let cell = "wedge_detector_rejects_fault_without_progress_consequence";
    let cap = capture(cell);
    let _guard = tracing::dispatcher::set_default(&cap.dispatch);

    let mut passes = Vec::new();
    let mut refusals = Vec::new();
    for family in Family::ALL {
        // The valid completed control passes, proving every consequence.
        let v = judge(&healthy(family)).unwrap_or_else(|r| panic!("a wedge that proved was refused: {r}"));
        assert_eq!(
            v.proved,
            vec!["fault-active", "progress-stalled", "routing", "control", "silent-runtime-not-dead", "released", "recovered", "reconciled", "no-superseded-dispatch"],
            "{}: the control proves every consequence",
            family.name()
        );
        passes.push(json!({"family": family.name(), "cut": v.cut, "proved": v.proved}));
        for (plant, want, apply) in plants() {
            let mut e = healthy(family);
            apply(&mut e);
            let refusal = judge(&e).expect_err(&format!("{}: `{plant}` must be refused", family.name()));
            assert_eq!(refusal.invariants(), vec![want], "{}: `{plant}` is refused by exactly `{want}`: {refusal}", family.name());
            refusals.push(json!({"family": family.name(), "planted": plant, "invariant": want, "detail": refusal.rejections[0].detail}));
        }
    }

    // A held proof mutation never reconciled: the RPC ledger's own algebra, inside the detector.
    let (ledger, state) = request_ledger(true);
    let mut e = healthy(Family::RequestPostApply);
    let mut r = Reconciliation::default();
    r.ledger(&ledger, &state);
    e.reconciliation = Some(r);
    let refusal = judge(&e).expect_err("a lost applied mutation is refused");
    assert_eq!(refusal.invariants(), vec![NO_RECONCILIATION]);
    assert!(refusal.rejections[0].detail.contains("operation_reconciler_detects_lost_applied_mutation") && refusal.rejections[0].detail.contains("op#0"), "{refusal}");
    let ledger_refusal = refusal.rejections[0].detail.clone();

    // Every consequence missing at once is named, every one of them, not the first.
    let mut bare = Evidence::new(Family::BuildStep, "bare");
    bare.fault = Some(FaultAck { held: true, names: Some(json!("x")), held_at_last_read: true });
    let all = judge(&bare).expect_err("a fault ack alone is refused");
    assert_eq!(all.invariants(), vec![NO_PROGRESS_CONSEQUENCE, NO_ROUTING_CONSEQUENCE, NO_CONTROL_CONSEQUENCE, NO_RELEASE, NO_RECOVERY, NO_RECONCILIATION]);

    // The invariants are the detector's whole vocabulary: each is exercised.
    let exercised: std::collections::BTreeSet<&str> = plants().iter().map(|p| p.1).collect();
    assert_eq!(exercised, INVARIANTS.iter().copied().collect(), "every named invariant has a planted wedge");

    // Spans: one resolve per pass, one reject per rejection, each rejection naming its invariant.
    let spans = cap.spans();
    let named = |n: &str| spans.iter().filter(|s| s["name"] == n).count();
    let resolves = named("rdm.scenario.wedge.resolve.via-detector");
    let rejects = spans.iter().filter(|s| s["name"].as_str().unwrap().starts_with("rdm.scenario.wedge.reject.via-")).count();
    let planted = Family::ALL.len() * plants().len();
    assert_eq!(resolves, Family::ALL.len(), "one resolve span per passing control");
    assert_eq!(rejects, planted + 1 + all.rejections.len(), "one reject span per rejection");
    for s in &spans {
        let n = s["name"].as_str().unwrap();
        if n.starts_with("rdm.scenario.wedge.reject.via-") {
            assert!(INVARIANTS.contains(&s["attributes"]["invariant"].as_str().unwrap()), "the reject span names its invariant: {s}");
        }
    }
    assert_eq!(named("rdm.scenario.wedge.reject.via-missing-progress-consequence") as usize, Family::ALL.len() * 4 + 1);

    let result = json!({
        "cell": cell,
        "families": Family::ALL.iter().map(|f| f.name()).collect::<Vec<_>>(),
        "passes": passes,
        "planted_wedges": plants().len(),
        "refusals": refusals,
        "ledger_refusal": ledger_refusal,
        "all_missing": all.rejections,
        "spans": {"resolve": resolves, "reject": rejects},
    });
    let dir = acceptance_dir("unit", cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

// ---------------------------------------------------------------------------------------------
// CHAOS-PROCESS: real stalls of a real estate, judged by the same detector.
// ---------------------------------------------------------------------------------------------

use rafka_node_admin_core::deployment::pipeline::{CreateStep, RetireStep};
use rafka_node_admin_core::lifecycle::HookPhase;
use rafka_test_scenario::elections::{advertised_fabric_primaries, seats_as_expected};
use rafka_test_scenario::estate::{named, wait_for, Estate, Owner};
use rafka_test_scenario::faults::{binding_set, candidate_sha, Door};
use std::sync::Mutex;
use std::time::Duration;

const CHAOS_CELL: &str = "wedge_detector_proves_stall_and_stateful_recovery";

fn owner() -> Owner {
    Owner {
        product: "mesh".into(),
        feature: "i143-2782".into(),
        subfeature: "wedge-detector".into(),
        rung: "MN".into(),
        provider: std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into()),
        test: CHAOS_CELL.into(),
    }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// The steps of `operation` at `attempt` the Build holds a `complete` receipt for, in order.
fn done_steps(build: &Value, operation: &str, attempt: u64) -> Vec<String> {
    build["steps"].as_array().into_iter().flatten().filter(|r| r["operation"] == operation && r["outcome"] == "complete" && r["attempt"] == attempt).map(|r| s(&r["step"])).collect()
}

/// How many receipts each step of `operation` holds at `attempt`.
fn step_counts(build: &Value, operation: &str, attempt: u64) -> BTreeMap<String, usize> {
    let mut m = BTreeMap::new();
    for r in build["steps"].as_array().into_iter().flatten().filter(|r| r["operation"] == operation && r["attempt"] == attempt) {
        *m.entry(s(&r["step"])).or_default() += 1;
    }
    m
}

/// `Ok` when the advertised seats equal the ones the public candidates compute.
fn seats(nodes: &[Value]) -> (bool, String) {
    match seats_as_expected(nodes) {
        Ok(()) => (true, String::new()),
        Err(e) => (false, e),
    }
}

fn fabric_primary(nodes: &[Value]) -> String {
    advertised_fabric_primaries(nodes).join(",")
}

/// The OS process state letter of `pid` (`R`, `S`, `T` stopped, `Z`...), or `None` when it is gone.
fn proc_state(pid: u64) -> Option<char> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    stat.rsplit(')').next()?.split_whitespace().next()?.chars().next()
}

/// utime + stime ticks the process has used: a stopped process uses none.
fn cpu_ticks(pid: u64) -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).unwrap_or_default();
    let f: Vec<&str> = stat.rsplit(')').next().unwrap_or("").split_whitespace().collect();
    f.get(11).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) + f.get(12).and_then(|v| v.parse::<u64>().ok()).unwrap_or(0)
}

fn signal(pid: u64, sig: &str) -> bool {
    std::process::Command::new("kill").args([sig, &pid.to_string()]).status().map(|s| s.success()).unwrap_or(false)
}

/// What a cut is expected to leave in the public view, and where it looks.
enum Watch {
    /// Any node not in the view when the cut was armed: it must not be Ready (or must be).
    NewNodes(Vec<String>),
    Node(String),
}

enum Trigger {
    Spawn,
    Delete(String),
    /// The accepting request itself holds until release; it is made from a task.
    SpawnRequest,
    /// A new mesh with one node-admin: a membership change, so the fabric primary may move.
    CreateMesh(String),
}

/// Which pipeline's documented receipts the Build holds.
enum Shape {
    Create { node: Option<String> },
    Retire { node: String },
}

struct Run {
    estate: Estate,
    root: PathBuf,
    doors: Mutex<BTreeMap<String, Door>>,
    evidence: Mutex<Vec<Evidence>>,
    rows: Mutex<Vec<Value>>,
    /// Cuts whose hold span the exported spans must show: (cut id, door admin).
    holds: Mutex<Vec<String>>,
}

impl Run {
    async fn build(&self, door: &Door, id: &str) -> Value {
        let (status, b) = self.estate.http_get(&door.api, &format!("/api/builds?id={id}")).await;
        assert_eq!(status, 200, "GET {}/api/builds?id={id}: {b}", door.api);
        b
    }

    async fn fabric(&self) -> Value {
        self.estate.get("/api/fabric").await.1
    }

    /// The highest attempt of Build `id` any admin holds: an unaccepted Build has none.
    async fn attempt_anywhere(&self, id: &str) -> u64 {
        let mut max = 0;
        for api in self.estate.nodes().await.iter().filter(|n| n["kind"] == "node_admin").filter_map(|n| n["admin_api_base"].as_str().map(String::from)) {
            let (st, b) = self.estate.http_get(&api, &format!("/api/builds?id={id}")).await;
            if st == 200 {
                max = max.max(b["attempt"].as_u64().unwrap_or(0));
            }
        }
        max
    }

    async fn door_of(&self, name: &str) -> Door {
        if let Some(d) = self.doors.lock().unwrap().get(name) {
            return d.clone();
        }
        let api = self.estate.nodes().await.iter().find(|n| n["name"] == name).and_then(|n| n["admin_api_base"].as_str().map(String::from)).unwrap_or_else(|| panic!("{name} advertises its control API"));
        let d = Door::open(&self.root, name, &api).await;
        self.doors.lock().unwrap().insert(name.to_string(), d.clone());
        d
    }

    async fn member_door(&self, mesh: &str) -> Door {
        let nodes = self.estate.nodes().await;
        let primary = nodes.iter().find(|n| n["kind"] == "node_admin" && n["mesh"] == mesh && n["is_primary"] == true).unwrap_or_else(|| panic!("{mesh} has an admin primary: {nodes:?}"));
        self.door_of(primary["name"].as_str().unwrap()).await
    }

    async fn fabric_door(&self) -> Door {
        let nodes = self.estate.nodes().await;
        let primary = nodes.iter().find(|n| n["is_fabric_primary"] == true).unwrap_or_else(|| panic!("the fabric has a primary: {nodes:?}"));
        self.door_of(primary["name"].as_str().unwrap()).await
    }

    async fn complete(&self, build_id: &str) -> Value {
        self.estate.await_build(build_id, Duration::from_secs(120)).await
    }

    async fn spawn(&self, kind: &str) -> String {
        let (status, a) = self.estate.post("/api/nodes/spawn", &json!({"mesh": "mesh1", "kind": kind})).await;
        assert_eq!(status, 202, "spawn {kind}: {a}");
        s(&a["build_id"])
    }

    async fn delete(&self, node: &str) -> String {
        let (status, a) = self.estate.delete(&format!("/api/nodes/{node}")).await;
        assert_eq!(status, 202, "delete {node}: {a}");
        s(&a["build_id"])
    }

    async fn names(&self) -> Vec<String> {
        self.estate.nodes().await.iter().map(|n| s(&n["name"])).collect()
    }

    async fn node_ready(&self, node: &str) -> Value {
        wait_for(&format!("{node} ready for traffic"), Duration::from_secs(60), || async { self.estate.node_opt(node).await.filter(|n| n["status"] == "ready-for-traffic") }).await
    }

    async fn node_gone(&self, node: &str) {
        wait_for(&format!("{node} left the view"), Duration::from_secs(60), || async { self.estate.node_opt(node).await.is_none().then_some(()) }).await
    }

    /// An unarmed create: the node it makes, once ready.
    async fn plain_create(&self, kind: &str) -> String {
        let before = self.names().await;
        let id = self.spawn(kind).await;
        self.complete(&id).await;
        wait_for("the created node is ready", Duration::from_secs(60), || async {
            self.estate.nodes().await.iter().find(|n| !before.contains(&s(&n["name"])) && n["status"] == "ready-for-traffic").map(|n| s(&n["name"]))
        })
        .await
    }

    fn runtimes(&self, node: &str) -> usize {
        self.estate.live_runtimes().iter().filter(|(dir, _)| dir.file_name().is_some_and(|n| n.to_string_lossy().starts_with(&format!("{node}-")))).count()
    }

    fn admin_pid(&self, name: &str) -> Option<u64> {
        if name == "mesh1.admin.1" {
            return self.estate.bootstrap_pid().map(u64::from);
        }
        self.estate.live_runtimes().iter().find(|(dir, _)| dir.file_name().is_some_and(|n| n.to_string_lossy().starts_with(&format!("{name}-")))).map(|(_, pid)| u64::from(*pid))
    }

    /// Whether the nodes `watch` names are routable in the public view now, and what it showed.
    async fn routable(&self, watch: &Watch) -> (bool, String) {
        let nodes = self.estate.nodes().await;
        match watch {
            Watch::NewNodes(before) => {
                let new: Vec<&Value> = nodes.iter().filter(|n| !before.contains(&s(&n["name"]))).collect();
                (new.iter().any(|n| n["status"] == "ready-for-traffic"), if new.is_empty() { "absent from the view".into() } else { new.iter().map(|n| format!("{}={}", n["name"], n["status"])).collect::<Vec<_>>().join(",") })
            }
            Watch::Node(name) => match nodes.iter().find(|n| n["name"] == name.as_str()) {
                Some(n) => (n["status"] == "ready-for-traffic", format!("{name}={}", n["status"])),
                None => (false, format!("{name} absent from the view")),
            },
        }
    }
}

/// One progress read of the Build a cut holds.
async fn read(run: &Run, door: &Door, build_id: &str, op: &Option<String>) -> (String, bool, u64) {
    let b = run.build(door, build_id).await;
    let attempt = b["attempt"].as_u64().unwrap_or(0);
    let done = op.as_ref().map(|o| done_steps(&b, o, attempt)).unwrap_or_default();
    let marker = json!({"state": b["state"], "attempt": attempt, "done": done, "pointer": s(&run.fabric().await["build_id"]), "attempt_anywhere": run.attempt_anywhere(build_id).await}).to_string();
    (marker, b["state"] == "complete", attempt)
}

/// Arm `id`, make the Build that reaches it, hold, observe, release, complete: one wedge, judged.
#[allow(clippy::too_many_arguments)]
async fn door_case(run: &Run, family: Family, id: &str, spec: Value, door: &Door, trigger: Trigger, shape: Shape, watch: Watch, expect_routable: bool) {
    let mut ev = Evidence::new(family, id);
    let nodes_before = run.estate.nodes().await;
    let (seats_before, seats_detail_before) = seats(&nodes_before);
    let admin_view = |nodes: &[Value]| nodes.iter().find(|n| n["name"] == door.name.as_str()).map(|n| s(&n["incarnation_id"])).unwrap_or_default();
    let (fp_before, inc_before) = (fabric_primary(&nodes_before), admin_view(&nodes_before));
    let pointer_before = s(&run.fabric().await["build_id"]);
    let pid_before = run.admin_pid(&door.name);
    let may_move = matches!(trigger, Trigger::CreateMesh(_));

    // Arm: the injector's own success, which proves nothing alone.
    let ack = door.arm(id, spec.clone()).await;
    ev.primitive = Some(Primitive { armed: true, ack: ack.clone() });
    let (build_id, request) = match &trigger {
        Trigger::Spawn => (run.spawn("rpc_node").await, None),
        Trigger::Delete(node) => (run.delete(node).await, None),
        Trigger::CreateMesh(mesh) => {
            let (status, a) = run.estate.post("/api/meshes", &json!({"name": mesh, "node_admin": 1})).await;
            assert_eq!(status, 202, "create {mesh}: {a}");
            (s(&a["build_id"]), None)
        }
        Trigger::SpawnRequest => {
            let (url, http) = (format!("{}/api/nodes/spawn", door.api), reqwest::Client::new());
            let task = tokio::spawn(async move {
                let r = http.post(url).json(&json!({"mesh": "mesh1", "kind": "rpc_node"})).send().await.expect("the accepting request is sent");
                (r.status().as_u16(), r.json::<Value>().await.unwrap_or(Value::Null))
            });
            (String::new(), Some(task))
        }
    };

    // The acknowledgement that the fault is active, naming the exact call it holds.
    let held = door.wait_held(id).await;
    let build_id = if build_id.is_empty() { held["hit"]["build_id"].as_str().or(held["hit"]["pointer_to_build_id"].as_str()).expect("the cut names the Build").to_string() } else { build_id };
    if !held["hit"]["build_id"].is_null() {
        assert_eq!(held["hit"]["build_id"], build_id.as_str(), "`{id}` holds a call of the Build that triggered it: {held}");
    }
    let op = match &shape {
        Shape::Create { node: Some(n) } => Some(format!("create-node:{n}")),
        Shape::Retire { node } => Some(format!("retire-node:{node}")),
        Shape::Create { node: None } => None,
    };

    // Progress reads taken while the fault stays active: one executor round (300 ms) apart.
    let mut reads = Vec::new();
    let mut complete_while_held = false;
    let (mut attempts, mut last_attempt) = ([0u64; 2], 0);
    for i in 0..3 {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let (marker, complete, attempt) = read(run, door, &build_id, &op).await;
        if i == 0 {
            attempts[0] = attempt;
        }
        last_attempt = attempt;
        complete_while_held |= complete;
        reads.push(marker);
    }
    attempts[1] = last_attempt;
    let still = door.cut(id).await;
    ev.fault = Some(FaultAck { held: held["held"] == true, names: (!held["hit"].is_null()).then(|| held["hit"].clone()), held_at_last_read: still["held"] == true });
    ev.progress = Some(Progress { reads: reads.clone(), complete_while_held });

    let (routable, shown) = run.routable(&watch).await;
    ev.routing = Some(Routing { expected_routable: expect_routable, observed_routable: routable, observed: shown });
    let nodes_during = run.estate.nodes().await;
    let (seats_during, seats_detail_during) = seats(&nodes_during);
    let (fp_during, inc_during) = (fabric_primary(&nodes_during), admin_view(&nodes_during));
    let held_pid = run.admin_pid(&door.name);
    let alive = held_pid.and_then(proc_state).is_some_and(|c| c != 'Z');
    let replaced = pid_before != held_pid || inc_before != inc_during || attempts[0] != attempts[1];
    let during_marker_attempt = attempts[1];

    // Release.
    let rel = door.release(id).await;
    let ended = wait_for(&format!("`{id}` hold ended"), Duration::from_secs(30), || async { (door.cut(id).await["held"] == false).then_some(true) }).await;
    ev.release = Some(Release { acked: rel["released"] == id, hold_ended: ended });
    if let Some(r) = request {
        let (status, body) = r.await.unwrap();
        assert_eq!((status, body["build_id"].as_str()), (202, Some(build_id.as_str())), "release answers the accepting request: {body}");
    }
    let done = run.complete(&build_id).await;

    // Recovery and the reconciled final state.
    let (after_marker, complete_after, _) = read(run, door, &build_id, &op).await;
    ev.recovery = Some(Recovery { work_complete: complete_after, marker_after: after_marker });
    let nodes_after = run.estate.nodes().await;
    let (seats_after, seats_detail_after) = seats(&nodes_after);
    ev.control = Some(Control {
        seats_as_expected: [seats_before, seats_during, seats_after],
        seats_detail: [seats_detail_before, seats_detail_during, seats_detail_after].join(" | "),
        fabric_primary: [fp_before, fp_during, fabric_primary(&nodes_after)],
        authority_may_move: may_move,
        incarnation: [inc_before, inc_during, admin_view(&nodes_after)],
        attempts: [attempts[0], during_marker_attempt],
        exact_runtime_alive: alive,
        replaced_during: replaced,
    });
    let mut rec = Reconciliation::default();
    let attempt = done["attempt"].as_u64().unwrap_or(0);
    rec.check("the Build completed at one attempt", done["state"] == "complete" && attempt == 1, format!("state {} attempt {attempt}", done["state"]));
    let fabric = run.fabric().await;
    rec.check("Fabric.build_id names the Build", fabric["build_id"] == build_id.as_str(), format!("{} vs {}", fabric["build_id"], build_id));
    rec.check("the pointer moved off the previous Build", pointer_before != build_id, format!("previous {pointer_before}"));
    match &shape {
        Shape::Create { .. } => create_checks(run, &mut rec, &done).await,
        Shape::Retire { node } => {
            let order: Vec<String> = RetireStep::ORDER.iter().map(|x| x.name().to_string()).collect();
            let op = format!("retire-node:{node}");
            let counts = step_counts(&done, &op, 1);
            rec.check("every documented retire step has one receipt, in order", done_steps(&done, &op, 1) == order && counts.values().all(|n| *n == 1), format!("{counts:?}"));
            run.node_gone(node).await;
            rec.check("no runtime of the node is left", run.runtimes(node) == 0, format!("{}", run.runtimes(node)));
        }
    }
    rec.check("the advertised seats equal the public candidates' after the release", seats_after, seats_detail_after_str(&nodes_after));
    ev.reconciliation = Some(rec);

    run.holds.lock().unwrap().push(id.to_string());
    run.rows.lock().unwrap().push(json!({"cut": id, "family": family.name(), "spec": spec, "arm_ack": ack, "held_ack": held, "build_id": build_id, "door_admin": door.name}));
    run.evidence.lock().unwrap().push(ev);
}

/// The reconciled final state of a create: every documented step once, the node serving, one runtime.
async fn create_checks(run: &Run, rec: &mut Reconciliation, done: &Value) {
    let node = s(&done["steps"].as_array().and_then(|a| a.iter().find(|r| r["step"] == "AllocateIdentity")).map(|r| r["operation"].clone()).unwrap_or(Value::Null)).trim_start_matches("create-node:").to_string();
    let order: Vec<String> = CreateStep::ORDER.iter().map(|x| x.name().to_string()).collect();
    let op = format!("create-node:{node}");
    let counts = step_counts(done, &op, 1);
    rec.check("every documented create step has one receipt, in order", done_steps(done, &op, 1) == order && counts.values().all(|n| *n == 1), format!("{counts:?}"));
    let view = run.node_ready(&node).await;
    rec.check("the node serves", view["status"] == "ready-for-traffic", s(&view["status"]));
    rec.check("exactly one runtime serves the node", run.runtimes(&node) == 1, format!("{}", run.runtimes(&node)));
}

/// A joining node-admin held at a boot cut (armed from `<root>/faults/<name>.boot.json`, before its
/// first line runs): the create Build waits on it, and it is not Ready until the release.
#[allow(clippy::too_many_arguments)]
async fn join_case(run: &Run, family: Family, id: &str, boot: Value, admin: &str, receipts_before: usize, exact: bool) {
    let mut ev = Evidence::new(family, id);
    let create_order: Vec<String> = CreateStep::ORDER.iter().map(|x| x.name().to_string()).collect();
    let faults = run.root.join("faults");
    std::fs::create_dir_all(&faults).unwrap();
    let _ = std::fs::remove_file(faults.join(format!("{admin}.door")));
    let mut spec = boot.clone();
    spec["id"] = json!(id);
    std::fs::write(faults.join(format!("{admin}.boot.json")), serde_json::to_vec(&json!([spec])).unwrap()).unwrap();
    let exec = run.fabric_door().await;
    let nodes_before = run.estate.nodes().await;
    let (seats_before, detail_before) = seats(&nodes_before);
    let (fp_before, inc_before) = (fabric_primary(&nodes_before), nodes_before.iter().find(|n| n["name"] == exec.name.as_str()).map(|n| s(&n["incarnation_id"])).unwrap_or_default());
    let pointer_before = s(&run.fabric().await["build_id"]);
    let ack = json!({"armed": id, "node": admin, "spec": boot, "armed_by": "boot file before the birth's first line"});
    ev.primitive = Some(Primitive { armed: true, ack: ack.clone() });
    let build_id = run.spawn("node_admin").await;
    let door = Door::open(&run.root, admin, "").await;
    let held = door.wait_held(id).await;
    let op = format!("create-node:{admin}");
    let want = create_order[..receipts_before].to_vec();
    // The pipeline goes on while the admin boots, and stops where it needs the admin Ready.
    wait_for(&format!("`{id}`: the Build reaches the held admin"), Duration::from_secs(60), || async {
        let b = run.build(&exec, &build_id).await;
        let d = done_steps(&b, &op, 1);
        (if exact { d == want } else { d.len() >= create_order.iter().position(|x| x == CreateStep::WaitForBind.name()).unwrap() }).then_some(())
    })
    .await;
    let mut reads = Vec::new();
    let mut complete_while_held = false;
    let mut attempts = [0u64; 2];
    for i in 0..3 {
        if i > 0 {
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        let (marker, complete, attempt) = read(run, &exec, &build_id, &Some(op.clone())).await;
        if i == 0 {
            attempts[0] = attempt;
        }
        attempts[1] = attempt;
        complete_while_held |= complete;
        reads.push(marker);
    }
    ev.fault = Some(FaultAck { held: held["held"] == true, names: (!held["hit"].is_null()).then(|| held["hit"].clone()), held_at_last_read: door.cut(id).await["held"] == true });
    ev.progress = Some(Progress { reads, complete_while_held });
    let (routable, shown) = run.routable(&Watch::Node(admin.into())).await;
    ev.routing = Some(Routing { expected_routable: false, observed_routable: routable, observed: shown });
    let nodes_during = run.estate.nodes().await;
    let (seats_during, detail_during) = seats(&nodes_during);
    let alive = run.admin_pid(admin).and_then(proc_state).is_some_and(|c| c != 'Z');
    let inc_during = nodes_during.iter().find(|n| n["name"] == exec.name.as_str()).map(|n| s(&n["incarnation_id"])).unwrap_or_default();
    let fp_during = fabric_primary(&nodes_during);

    let rel = door.release(id).await;
    let ended = wait_for(&format!("`{id}` hold ended"), Duration::from_secs(30), || async { (door.cut(id).await["held"] == false).then_some(true) }).await;
    ev.release = Some(Release { acked: rel["released"] == id, hold_ended: ended });
    let done = run.complete(&build_id).await;
    let (after_marker, complete_after, _) = read(run, &exec, &build_id, &Some(op.clone())).await;
    ev.recovery = Some(Recovery { work_complete: complete_after, marker_after: after_marker });
    let nodes_after = run.estate.nodes().await;
    let (seats_after, detail_after) = seats(&nodes_after);
    ev.control = Some(Control {
        seats_as_expected: [seats_before, seats_during, seats_after],
        seats_detail: [detail_before, detail_during, detail_after.clone()].join(" | "),
        fabric_primary: [fp_before, fp_during, fabric_primary(&nodes_after)],
        authority_may_move: true,
        incarnation: [inc_before.clone(), inc_during.clone(), nodes_after.iter().find(|n| n["name"] == exec.name.as_str()).map(|n| s(&n["incarnation_id"])).unwrap_or_default()],
        attempts,
        exact_runtime_alive: alive,
        replaced_during: inc_before != inc_during || attempts[0] != attempts[1],
    });
    let mut rec = Reconciliation::default();
    let attempt = done["attempt"].as_u64().unwrap_or(0);
    rec.check("the Build completed at one attempt", done["state"] == "complete" && attempt == 1, format!("state {} attempt {attempt}", done["state"]));
    rec.check("Fabric.build_id names the Build", run.fabric().await["build_id"] == build_id.as_str() && pointer_before != build_id, format!("previous {pointer_before}"));
    create_checks(run, &mut rec, &done).await;
    rec.check("the advertised seats equal the public candidates' after the release", seats_after, detail_after);
    ev.reconciliation = Some(rec);
    run.holds.lock().unwrap().push(id.to_string());
    run.rows.lock().unwrap().push(json!({"cut": id, "family": family.name(), "spec": boot, "arm_ack": ack, "held_ack": held, "build_id": build_id, "door_admin": admin}));
    run.evidence.lock().unwrap().push(ev);
}

fn seats_detail_after_str(nodes: &[Value]) -> String {
    seats(nodes).1
}

/// A runtime frozen in place (SIGSTOP) is alive and silent: it is judged by the typed outcome of
/// calls to it, by its process state and CPU use, and by what the control plane does about it,
/// never by how long it has been quiet.
async fn silent_case(run: &Run) {
    let floor = rafka_mesh_transport::membership::staleness_floor();
    let id = "silent-runtime:sigstop-rpc-node";
    let mut ev = Evidence::new(Family::SilentRuntime, id);
    let nodes = run.estate.nodes().await;
    let target = nodes.iter().find(|n| n["kind"] == "rpc_node" && n["is_primary"] == false && n["status"] == "ready-for-traffic").cloned().unwrap_or_else(|| panic!("a ready non-primary rpc node: {nodes:?}"));
    let (name, node_id, incarnation) = (s(&target["name"]), s(&target["node_id"]), s(&target["incarnation_id"]));
    let exact = format!("exact:{node_id}");
    let pid = run.estate.pid_of(&name).await;
    let data_dir = run.estate.data_dir_of(&name).await;
    let fabric_build = s(&run.fabric().await["build_id"]);
    let attempt_of = |b: &Value| b["attempt"].as_u64().unwrap_or(0);
    let attempt_before = attempt_of(&run.estate.get(&format!("/api/builds?id={fabric_build}")).await.1);

    // The held proof mutation: stored by the live node before it is frozen.
    let put = run.estate.probe(&["put", "--target", &exact, "--key", "77", "--value", "before-freeze"]);
    assert_eq!(put["outcome"], "Reply", "the put reaches the live node: {put}");
    assert_eq!(put["reply"]["incarnation_id"], incarnation.as_str(), "{put}");
    let (seats_before, detail_before) = seats(&nodes);
    let (fp_before, inc_before) = (fabric_primary(&nodes), incarnation.clone());

    // The fault: SIGSTOP, acknowledged by the OS process state.
    assert!(signal(pid, "-STOP"), "SIGSTOP {pid} ({name})");
    let state_after_stop = wait_for(&format!("{name} (pid {pid}) is stopped"), Duration::from_secs(10), || async { (proc_state(pid) == Some('T')).then_some('T') }).await;
    let primitive = json!({"signal": "SIGSTOP", "pid": pid, "node": name});
    ev.primitive = Some(Primitive { armed: true, ack: primitive.clone() });

    // The membership consequence: the view stops calling it ready, within the staleness window.
    let silent_view = wait_for(&format!("{name} no longer ready in the public view"), floor * 2 + Duration::from_secs(30), || async {
        run.estate.node_opt(&name).await.filter(|n| n["status"] != "ready-for-traffic")
    })
    .await;

    // Reads while stopped: process state, CPU use and the typed outcome of a call to the exact node.
    let mut reads = Vec::new();
    let mut outcomes = Vec::new();
    for _ in 0..2 {
        let probe = run.estate.probe(&["get", "--target", &exact, "--key", "77"]);
        reads.push(json!({"proc_state": proc_state(pid).map(String::from), "cpu_ticks": cpu_ticks(pid), "outcome": probe["outcome"]}).to_string());
        outcomes.push(probe);
    }
    let held_at_last_read = proc_state(pid) == Some('T');
    ev.fault = Some(FaultAck { held: state_after_stop == 'T', names: Some(json!({"pid": pid, "proc_state": "T", "view_status": silent_view["status"]})), held_at_last_read });
    ev.progress = Some(Progress { reads, complete_while_held: outcomes.iter().any(|o| o["outcome"] == "Reply") });
    ev.routing = Some(Routing { expected_routable: false, observed_routable: outcomes.iter().any(|o| o["outcome"] == "Reply"), observed: format!("view {}; calls {}", silent_view["status"], outcomes.iter().map(|o| s(&o["outcome"])).collect::<Vec<_>>().join(",")) });
    let nodes_during = run.estate.nodes().await;
    let (seats_during, detail_during) = seats(&nodes_during);
    let during_node = nodes_during.iter().find(|n| n["name"] == name.as_str()).cloned();
    let attempt_during = attempt_of(&run.estate.get(&format!("/api/builds?id={fabric_build}")).await.1);
    let alive = proc_state(pid).is_some_and(|c| c != 'Z');
    let inc_during = during_node.as_ref().map(|n| s(&n["incarnation_id"])).unwrap_or_default();
    let replaced = during_node.as_ref().is_none_or(|n| s(&n["node_id"]) != node_id) || s(&run.fabric().await["build_id"]) != fabric_build;

    // Thaw.
    let acked = signal(pid, "-CONT");
    let ended = wait_for(&format!("{name} (pid {pid}) runs again"), Duration::from_secs(10), || async { proc_state(pid).filter(|c| *c != 'T').map(|_| true) }).await;
    ev.release = Some(Release { acked, hold_ended: ended });

    // Recovery: the same birth is ready again and serves the value stored before the freeze.
    let back = wait_for(&format!("{name} back as {incarnation}"), floor * 2 + Duration::from_secs(30), || async {
        run.estate.node_opt(&name).await.filter(|n| n["incarnation_id"] == incarnation.as_str() && n["status"] == "ready-for-traffic")
    })
    .await;
    let kept = run.estate.probe(&["get", "--target", &exact, "--key", "77"]);
    ev.recovery = Some(Recovery { work_complete: kept["outcome"] == "Reply", marker_after: json!({"proc_state": proc_state(pid).map(String::from), "outcome": kept["outcome"]}).to_string() });
    let nodes_after = run.estate.nodes().await;
    let (seats_after, detail_after) = seats(&nodes_after);
    ev.control = Some(Control {
        seats_as_expected: [seats_before, seats_during, seats_after],
        seats_detail: [detail_before, detail_during, detail_after.clone()].join(" | "),
        fabric_primary: [fp_before, fabric_primary(&nodes_during), fabric_primary(&nodes_after)],
        authority_may_move: false,
        incarnation: [inc_before, inc_during, s(&back["incarnation_id"])],
        attempts: [attempt_before, attempt_during],
        exact_runtime_alive: alive,
        replaced_during: replaced,
    });
    let mut rec = Reconciliation::default();
    rec.check("the value stored before the freeze is served after it", kept["reply"]["result"] == json!({"found": true, "value": "before-freeze"}), kept.to_string());
    rec.check("the same birth serves it", kept["reply"]["incarnation_id"] == incarnation.as_str() && kept["reply"]["executing_node"] == node_id.as_str(), kept.to_string());
    rec.check("the node keeps its process and its data dir", run.estate.pid_of(&name).await == pid && run.estate.data_dir_of(&name).await == data_dir, format!("pid {pid}, {data_dir}"));
    rec.check("exactly one runtime serves the node", run.runtimes(&name) == 1, format!("{}", run.runtimes(&name)));
    rec.check("Fabric.build_id is the Build it was and that Build gained no attempt", s(&run.fabric().await["build_id"]) == fabric_build && attempt_of(&run.estate.get(&format!("/api/builds?id={fabric_build}")).await.1) == attempt_before, format!("{fabric_build} attempts {attempt_before}"));
    rec.check("the advertised seats equal the public candidates' after the thaw", seats_after, detail_after);
    ev.reconciliation = Some(rec);
    run.rows.lock().unwrap().push(json!({
        "cut": id, "family": Family::SilentRuntime.name(), "primitive": primitive, "node": {"name": name, "node_id": node_id, "incarnation_id": incarnation, "pid": pid},
        "put": put, "view_while_silent": silent_view, "calls_while_silent": outcomes, "after_thaw": kept,
    }));
    run.evidence.lock().unwrap().push(ev);
}

/// CONTRACT (#2782): real stalls of a real estate (mesh1 with two node-admins, both the testkit's
/// faulted admin, and rpc nodes), each held and released through its own door or signal, are judged
/// by the semantic detector, never by a timeout. Cuts: a deployment step (create `WaitForBind`), a
/// removal step on a serving node (retire `NodeDeleting`), a lifecycle hook (`BeforeEligibility`),
/// the accepted Build durable before `Fabric.build_id` names it and the pointer's own write (the
/// fabric-primary admin held), and a non-primary rpc node frozen in place with SIGSTOP. Each: the
/// injector acknowledged the active fault and named it; progress read three times (a Build's
/// receipts and attempt on every admin, the pointer; a frozen node's process state, CPU use and the
/// typed outcome of calls to it) stood still while held; the routing effect the cut's position
/// implies; the seats equal what the public candidates compute and the held, running admin or
/// frozen, running node was neither replaced, re-attempted nor re-born; the release; the work
/// completed with its receipts once, one runtime, the pointer on the Build, and for the frozen node
/// the value stored before the freeze served by the same birth afterwards. What must NOT happen: an
/// election move or a new birth caused by silence, a Build executed before the pointer named it, a
/// step receipt twice, a stalled-progress reading that moves while the fault is held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wedge_detector_proves_stall_and_stateful_recovery() {
    let dir = acceptance_dir("process", CHAOS_CELL);
    std::fs::create_dir_all(&dir).unwrap();
    let sha = candidate_sha();
    let set = binding_set(&sha);
    let estate = Estate::bootstrap_external(owner(), "fabric1", "mesh1", &set, &sha, &["rpc_node"]).await.expect("the faulted-admin binding set is accepted");
    let root = estate.root.clone();
    let run = Run { estate, root, doors: Default::default(), evidence: Default::default(), rows: Default::default(), holds: Default::default() };
    // A lone admin hears no other member and is cut off, so it executes nothing: two admins first.
    let (status, a) = run.estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2}]})).await;
    assert_eq!(status, 202, "{a}");
    run.complete(&s(&a["build_id"])).await;
    run.estate.settled(&["mesh1.admin.1", "mesh1.admin.2"].iter().map(|n| n.to_string()).collect(), Duration::from_secs(60)).await;

    let node = "mesh1.rpc.1";
    // The healthy control, in the same capture: an unarmed create and removal of the same node
    // complete with every documented receipt once. Every cut below is judged against what they did.
    let control_create = run.spawn("rpc_node").await;
    let control_door = run.member_door("mesh1").await;
    let b = run.complete(&control_create).await;
    let create_order: Vec<String> = CreateStep::ORDER.iter().map(|x| x.name().to_string()).collect();
    assert_eq!(done_steps(&b, &format!("create-node:{node}"), 1), create_order, "the healthy create runs every documented step once: {b}");
    run.node_ready(node).await;
    let control_delete = run.delete(node).await;
    let b = run.complete(&control_delete).await;
    let retire_order: Vec<String> = RetireStep::ORDER.iter().map(|x| x.name().to_string()).collect();
    assert_eq!(done_steps(&b, &format!("retire-node:{node}"), 1), retire_order, "the healthy removal runs every documented step once: {b}");
    run.node_gone(node).await;
    run.rows.lock().unwrap().push(json!({"cut": "control", "group": "healthy-control", "create_build_id": control_create, "create_steps": create_order, "retire_build_id": control_delete, "retire_steps": retire_order, "door_admin": control_door.name}));

    // A deployment step: the runtime is bound and the node declares itself Ready on its own, so the
    // stalled Build bookkeeping does not unroute it (the public view shows it ready-for-traffic).
    let door = run.member_door("mesh1").await;
    let before = run.names().await;
    let step = CreateStep::WaitForBind.name();
    door_case(
        &run,
        Family::DeploymentStep,
        &format!("create:{step}"),
        json!({"kind": "receipt", "step": step, "operation": "create-node", "node": node}),
        &door,
        Trigger::Spawn,
        Shape::Create { node: Some(node.into()) },
        Watch::NewNodes(before),
        true,
    )
    .await;
    // A removal step on a node that keeps serving until the retire reaches it.
    let step = RetireStep::NodeDeleting.name();
    door_case(
        &run,
        Family::BuildStep,
        &format!("retire:{step}"),
        json!({"kind": "receipt", "step": step, "operation": "retire-node", "node": node}),
        &door,
        Trigger::Delete(node.into()),
        Shape::Retire { node: node.into() },
        Watch::Node(node.into()),
        true,
    )
    .await;
    // A lifecycle hook of the Pending -> ReadyForTraffic transition: the rpc node's declared status is
    // independent of the admin-side hook, so the node stays routable while the hook holds.
    let before = run.names().await;
    let phase = HookPhase::BeforeEligibility.as_str();
    door_case(
        &run,
        Family::LifecycleHook,
        &format!("hook:{phase}"),
        json!({"kind": "hook", "phase": phase, "node": node}),
        &door,
        Trigger::Spawn,
        Shape::Create { node: Some(node.into()) },
        Watch::NewNodes(before),
        true,
    )
    .await;

    // A second rpc node, so the frozen one is not the cohort's only member; then the frozen node.
    run.plain_create("rpc_node").await;
    silent_case(&run).await;

    // The fabric-primary admin held while it accepts a Build: durable before the pointer, then in the pointer's write.
    let fdoor = run.fabric_door().await;
    let before = run.names().await;
    door_case(
        &run,
        Family::AcceptedBuildPersistence,
        "accept:build-durable-before-pointer",
        json!({"kind": "accepted-build"}),
        &fdoor,
        Trigger::SpawnRequest,
        Shape::Create { node: None },
        Watch::NewNodes(before),
        false,
    )
    .await;
    let before = run.names().await;
    door_case(
        &run,
        Family::FabricPrimaryStall,
        "pointer:fabric-record-write",
        json!({"kind": "pointer-write", "moves_pointer": true}),
        &fdoor,
        Trigger::SpawnRequest,
        Shape::Create { node: None },
        Watch::NewNodes(before),
        false,
    )
    .await;

    // A mesh's first admin, born by the fabric primary: stalled before the step that publishes it,
    // so its mesh's Pending has not been applied. The admin is not routable until the release.
    let step = CreateStep::PublishTopologyAndRuntimeFactAndCurrentRuntimeMetadata.name();
    let admin = "mesh2.admin.1";
    let before = run.names().await;
    door_case(
        &run,
        Family::PendingBeforeBuild,
        "pending:mesh-first-admin",
        json!({"kind": "receipt", "step": step, "operation": "create-node", "node": admin}),
        &fdoor,
        Trigger::CreateMesh("mesh2".into()),
        Shape::Create { node: Some(admin.into()) },
        Watch::NewNodes(before),
        false,
    )
    .await;

    // A joining admin held at its hydration of the Fabric record, and at its Ready gate on the provider domain.
    join_case(&run, Family::RuntimeMetadataHydration, "hydration:fabric-record-write", json!({"kind": "pointer-write", "moves_pointer": true}), "mesh1.admin.3", 12, false).await;
    join_case(&run, Family::ProviderDomain, "pending:provider-domain", json!({"kind": "provider-domain"}), "mesh1.admin.4", 12, true).await;

    finish(run, &dir).await;
}

fn attr<'a>(sp: &'a Value, k: &str) -> &'a str {
    sp["attributes"][k].as_str().unwrap_or_default()
}

async fn finish(run: Run, dir: &std::path::Path) {
    let Run { mut estate, evidence, rows, holds, .. } = run;
    estate.stop().await;
    let spans = estate.spans();
    let mut evidence = evidence.into_inner().unwrap();
    // The dispatches to the frozen node's births, from the serve spans its births exported.
    for ev in evidence.iter_mut().filter(|e| e.family == Family::SilentRuntime) {
        let row = rows.lock().unwrap().iter().find(|r| r["cut"] == ev.cut.as_str()).cloned().unwrap();
        let (node_id, current) = (s(&row["node"]["node_id"]), s(&row["node"]["incarnation_id"]));
        for sp in named(&spans, "rdm.node_rpc.proof_store.serve.via-request").into_iter().filter(|sp| attr(sp, "node_id") == node_id) {
            ev.dispatches.push(Dispatch { birth: attr(sp, "incarnation_id").to_string(), current_birth: current.clone(), after_supersession: false });
        }
        assert!(!ev.dispatches.is_empty(), "the frozen node's births exported serve spans for the calls that reached them");
    }
    let mut verdicts = Vec::new();
    let mut refusals = Vec::new();
    for ev in &evidence {
        match judge(ev) {
            Ok(v) => verdicts.push(serde_json::to_value(v).unwrap()),
            Err(r) => refusals.push(r.to_string()),
        }
    }
    // Each held door cut left its hold span in the exported spans: the hold ran, and ended.
    let mut hold_spans = Vec::new();
    for cut in holds.into_inner().unwrap() {
        let h: Vec<&Value> = named(&spans, "rdm.testkit.fault.update.via-hold").into_iter().filter(|sp| attr(sp, "cut") == cut).collect();
        assert!(!h.is_empty(), "`{cut}`: a hold span was exported");
        hold_spans.push(json!({"cut": cut, "trace_id": h[0]["trace_id"], "span_id": h[0]["span_id"], "parent_span_id": h[0]["parent_span_id"], "service": h[0]["service"]}));
    }
    let covered: Vec<&str> = evidence.iter().map(|e| e.family.name()).collect();
    let not_run: Vec<Value> = Family::ALL
        .iter()
        .filter(|f| !covered.contains(&f.name()))
        .map(|f| json!({"family": f.name(), "judged_by": "wedge_detector_rejects_fault_without_progress_consequence (planted evidence)", "run_here": false}))
        .collect();
    let result = json!({
        "cell": CHAOS_CELL,
        "cuts": evidence.iter().map(|e| json!({"cut": e.cut, "family": e.family.name()})).collect::<Vec<_>>(),
        "evidence": evidence,
        "verdicts": verdicts,
        "refusals": refusals,
        "hold_spans": hold_spans,
        "rows": rows.into_inner().unwrap(),
        "families_not_exercised_on_the_estate": not_run,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    assert!(refusals.is_empty(), "the detector refused a wedge of the estate:\n{}", refusals.join("\n"));
}
