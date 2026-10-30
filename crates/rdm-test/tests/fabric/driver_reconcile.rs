//! i143.e9.s2 acceptance (rafka-v2 #2787, hardened 2026-10-07), SOAK layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2787-soak-process` (and `-soak-container`), which exports
//! `I143_ACCEPTANCE_DIR` (each cell's `result.json` goes there) and whose commands set
//! `RDM_ARTIFACTS_DIR` (the estate's manifest, rpc ledger and every process's spans land under
//! it, feature `i143-2787`), `RDM_SOAK_SECS` and `RDM_SOAK_SEED`.
//!
//! One driver (`rdm_test::soak`) serves both providers; the cells differ only in the
//! provider the estate runs on.

use rdm_test::estate::{named, Estate, Owner};
use rdm_test::model::NodeClass;
use rdm_test::soak::{self, Config, Driver, Profile};
use serde_json::{json, Value};
use std::path::{Path, PathBuf};

const PROCESS_CELL: &str = "soak_driver_reconciles_fixed_seed_thirty_minute_process_run";
const CONTAINER_CELL: &str = "soak_driver_reconciles_fixed_seed_thirty_minute_container_run";
const SEED: u64 = 1_432_787;
/// Per mesh, of two meshes: the canonical R-shape (two node-admins, two compute, three gateway and
/// three broker nodes), twenty nodes in all.
const PER_MESH: [(NodeClass, u32); 4] = [(NodeClass::NodeAdmin, 2), (NodeClass::Compute, 2), (NodeClass::Gateway, 3), (NodeClass::Broker, 3)];

fn owner(test: &str, provider: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "i143-2787".into(), subfeature: "soak".into(), rung: "MM".into(), provider: provider.into(), test: test.into() }
}

fn acceptance_dir(cell: &str, layer: &str) -> PathBuf {
    let dir = match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2787").join(layer).join(cell),
    };
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// This process's own spans: the product span pipeline (`install_span_capture`: its span filter and
/// `BatchSpanProcessor`) in front of an in-memory exporter behind the global subscriber, so the
/// fault backend's and the detector's spans are exactly what this cell emitted.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    service: String,
}

fn capture(cell: &str) -> Capture {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let service = format!("i143-2787-{cell}");
    let capture = rdm_transport::install_span_capture(&service, exporter.clone()).expect("one global subscriber per cell process");
    Capture { exporter, capture, service }
}

impl Capture {
    fn spans(&self) -> Vec<Value> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let finished = self.exporter.get_finished_spans().unwrap();
        crate::assert_own_story(finished.iter().map(|s| s.name.as_ref()));
        finished
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

fn walk_logs(root: &Path) -> Vec<PathBuf> {
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

/// One offline-tickle ladder at its longest: the direct connect, then every via-peer carrier at once,
/// each one call of the default budget.
fn tickle_ladder() -> std::time::Duration {
    rdm_admin::offline::tickle_ladder_at_default_budget()
}

fn attr<'a>(sp: &'a Value, k: &str) -> &'a str {
    sp["attributes"][k].as_str().unwrap_or_default()
}

/// The soak of one cell: the driver runs `RDM_SOAK_SECS` seconds (default 60) of seed
/// `RDM_SOAK_SEED` (default the story's) on the provider, then the estate stops and the exported
/// spans are read.
async fn soak_cell(cell: &'static str, provider: &'static str, layer: &'static str) {
    let secs: u64 = std::env::var("RDM_SOAK_SECS").ok().and_then(|v| v.parse().ok()).unwrap_or(60);
    let seed: u64 = std::env::var("RDM_SOAK_SEED").ok().and_then(|v| v.parse().ok()).unwrap_or(SEED);
    let dir = acceptance_dir(cell, layer);
    let cap = capture(cell);
    eprintln!("SOAK seed={seed} secs={secs} provider={provider}  (rerun: RDM_SOAK_SEED={seed} RDM_SOAK_SECS={secs})");
    let estate = Estate::bootstrap(owner(cell, provider), "fabric1", "mesh1").await;
    assert_eq!(estate.owner.provider, provider, "the cell runs on its provider");
    let root = estate.root.clone();
    let cfg = Config::roles(seed, secs, PER_MESH, Profile::All, false, rdm_transport::topology::staleness_floor(), rdm_transport::topology::backbone_gossip_interval(), tickle_ladder());
    let driver = Driver::new(estate, cfg, crate::build_log::build_read).await;
    let progress = driver.progress();
    // The run is its own task: a panic inside it still leaves the seed and the executed sequence.
    let joined = tokio::spawn(driver.run()).await;
    let (report, mut estate) = match joined {
        Ok(r) => r,
        Err(e) => {
            let message = if e.is_panic() { format!("{:?}", e.into_panic().downcast_ref::<String>().cloned().or_else(|| Some("a non-string panic".into()))) } else { e.to_string() };
            let repro = soak::panicked(seed, &progress.lock().unwrap(), &message);
            let result = json!({"cell": cell, "seed": seed, "secs": secs, "driver_panic": message, "repro": repro});
            std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
            panic!("seed {seed}: the soak driver panicked: {message}\nminimized reproduction ({} of {} actions): {:#?}", repro.minimized.len(), repro.original_len, repro.minimized);
        }
    };
    eprintln!("{}", soak::summary(&report));
    estate.stop().await;
    let left = estate.live_runtimes();
    let containers = estate.live_containers();

    // The estate's own evidence: every log and every exported span.
    let mut unknown_issuer: Vec<String> = Vec::new();
    for e in walk_logs(&root) {
        for line in std::fs::read_to_string(&e).unwrap_or_default().lines().filter(|l| l.contains("UnknownIssuer")) {
            unknown_issuer.push(format!("{}: {}", e.display(), line.chars().take(400).collect::<String>()));
        }
    }
    let spans = estate.spans();
    for sp in &spans {
        if sp.to_string().contains("UnknownIssuer") {
            unknown_issuer.push(format!("span {} {}", sp["name"], sp["span_id"]));
        }
    }
    let mut violations: Vec<String> = Vec::new();
    // The path RAN: the operations reached handlers, the topology actions ran as Build attempts.
    let served = named(&spans, "rdm.node_rpc.proof_store.serve.via-request").len();
    let reconciles = named(&spans, "rdm.node_admin.build.update.via-reconcile").len();
    if report.ledger.issued > 0 && served == 0 {
        violations.push(format!("{} operations were issued and no proof-store handler span was exported", report.ledger.issued));
    }
    if reconciles == 0 {
        violations.push("no Build attempt span (rdm.node_admin.build.update.via-reconcile) was exported".into());
    }
    let own = cap.spans();
    let fault_spans = named(&own, "rdm.testkit.fault.update.via-process-signal");
    let verdicts = named(&own, "rdm.testkit.wedge.resolve.via-detector");
    if provider == "process" {
        for (action, faults) in [("kill", vec!["kill"]), ("wedge", vec!["stop", "continue"])] {
            if report.actions.get(action).copied().unwrap_or(0) > 0 {
                for f in faults {
                    if !fault_spans.iter().any(|sp| attr(sp, "fault") == f) {
                        violations.push(format!("{action} ran and the process fault backend emitted no `{f}` span"));
                    }
                }
            }
        }
    }
    if report.actions.get("wedge").copied().unwrap_or(0) > 0 && verdicts.is_empty() {
        violations.push("a wedge ran and the detector emitted no verdict span".into());
    }
    let result = json!({
        "cell": cell,
        "seed": seed,
        "secs": secs,
        "provider": provider,
        "report": report,
        "evidence": {
            "estate_manifest": estate.artifacts.join("manifest.json"),
            "rpc_ledger": estate.artifacts.join("rpc-ledger.jsonl"),
            "spans_dir": estate.kept_evidence(),
            "exported_spans": spans.len(),
            "proof_store_serve_spans": served,
            "build_attempt_spans": reconciles,
            "fault_backend_spans": fault_spans.len(),
            "wedge_verdict_spans": verdicts.len(),
        },
        "unknown_issuer": {"scanned": "every .log under the estate root and every exported span", "matches": unknown_issuer.len(), "lines": unknown_issuer},
        "cell_violations": violations,
        "runtimes_left": left,
        "containers_left": containers,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&own).unwrap()).unwrap();
    if let Some(repro) = &report.repro {
        eprintln!("SOAK FAILED seed={seed}: rule {} at step {}\nminimized reproduction ({} of {} actions): {:#?}", repro.rule, repro.failing_step, repro.minimized.len(), repro.original_len, repro.minimized);
    }
    assert!(report.ok(), "seed {seed}: violations {:#?}; ledger {:#?}; rerun RDM_SOAK_SEED={seed} RDM_SOAK_SECS={secs}", report.violations, report.ledger.violations);
    assert!(violations.is_empty(), "seed {seed}: {violations:#?}");
    assert!(left.is_empty(), "seed {seed}: no runtime of the estate is left running: {left:?}");
    assert!(containers.is_empty(), "seed {seed}: no container of the estate is left running: {containers:?}");
    assert!(report.rounds >= 3, "seed {seed}: only {} rounds in {secs}s", report.rounds);
}

/// CONTRACT (#2787): on the canonical R-shape estate (two meshes, each of two node-admins, two
/// compute, three gateway and three broker nodes: twenty nodes), the
/// soak driver issues continuous proof-store operations (puts, compare-and-swaps, gets, deletes,
/// each recorded in the operation ledger before it is sent and classified once from its typed
/// outcome) for `RDM_SOAK_SECS` seconds of seed `RDM_SOAK_SEED` (1432787, 1800 s in the
/// registered job) while the action model's legal random actions run: grow, shrink, restart,
/// replace and hand-off as Builds the node-admin rectifier executes, and kills and holds of exact
/// runtimes on the process provider. After every action the estate converges (the Build's receipts
/// name the new birth, every live admin holds the same births) and the invariants hold: one current
/// birth per path, one fabric-primary, the current build_id held, no peer mesh unheard, the roles
/// equal the public candidates', every role node reachable on its current birth, no Build minted
/// for drift, a held runtime comes back as the same birth, a deleted node is never dispatched.
/// At the end every issued operation is accounted: issued = Reply + NotSent + Unserved +
/// RejectedStale + Indeterminate with none unclassified, every successful write is in the final
/// store of each surviving node, nothing was applied that no operation explains. The
/// fabric-primary node-admin is never signalled. On a violation the run stops at once and reports
/// the seed, the executed sequence and a minimized legal reproduction. What must NOT happen: an
/// operation without an outcome, a lost applied write, a violated invariant, an unexplained
/// timeout or hang, a runtime left running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_driver_reconciles_fixed_seed_thirty_minute_process_run() {
    if crate::own_process::delegated(module_path!(), "soak_driver_reconciles_fixed_seed_thirty_minute_process_run") {
        return;
    }
    soak_cell(PROCESS_CELL, "process", "soak-process").await;
}

/// CONTRACT (#2787): the same driver, seed and schedule on the container provider: kills are
/// `docker kill` of the exact container (SIGKILL to its init, acknowledged by the daemon; an
/// interface removal is no death: the runtime keeps running and is held under R-FM1), holds are
/// `docker pause`, and a mesh is made unheard
/// by a packet filter in the namespaces of its containers until its heal row. The fabric-primary's
/// container is in no silenced set. Every container of the estate is gone when the estate stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn soak_driver_reconciles_fixed_seed_thirty_minute_container_run() {
    if crate::own_process::delegated(module_path!(), "soak_driver_reconciles_fixed_seed_thirty_minute_container_run") {
        return;
    }
    soak_cell(CONTAINER_CELL, "container", "soak-container").await;
}
