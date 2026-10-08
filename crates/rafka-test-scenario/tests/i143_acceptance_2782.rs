//! i143.e8.s4 acceptance (rafka-v2 #2782, hardened 2026-10-07): the semantic wedge detectors.
//!
//! - UNIT `wedge_detector_rejects_fault_without_progress_consequence`: run by
//!   `scripts/i143-acceptance-gate.sh i143-2782-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//!   cell leaves `result.json` (its direct observations) and `spans.json` (every span the
//!   detector emitted, captured in process by the evidence exporter) there.
//! - CHAOS-PROCESS `wedge_detector_proves_stall_and_stateful_recovery`: run by
//!   `scripts/i143-acceptance-gate.sh i143-2782-process`, whose command sets `RAFKA_ARTIFACTS_DIR`
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
    let marker = build_marker("running", 1, &["AllocateIdentity".to_string(), "AllocateEndpoints".to_string()]);
    e.progress = Some(Progress { reads: vec![marker.clone(), marker.clone(), marker], complete_while_held: false });
    e.routing = Some(Routing { expected_routable: false, observed_routable: false, observed: "absent from the view".into() });
    e.control = Some(Control {
        seats_as_expected: [true; 3],
        seats_detail: String::new(),
        fabric_primary: ["mesh1.admin.1".into(), "mesh1.admin.1".into(), "mesh1.admin.1".into()],
        incarnation: ["inc-a".into(), "inc-a".into(), "inc-a".into()],
        attempts: [1, 1],
        exact_runtime_alive: true,
        replaced_during: false,
    });
    e.release = Some(Release { acked: true, hold_ended: true });
    e.recovery = Some(Recovery { work_complete: true, marker_after: build_marker("complete", 1, &["AllocateIdentity".to_string(), "AllocateEndpoints".to_string(), "Complete".to_string()]) });
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
            p.reads[2] = build_marker("running", 1, &["AllocateIdentity".to_string(), "AllocateEndpoints".to_string(), "PrepareStorage".to_string()]);
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
