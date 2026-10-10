//! i143.e7.s5 acceptance (rafka-v2 #2777): the restart canary and the exact seed on the process and
//! the container providers. Run by `scripts/i143-acceptance-gate.sh i143-2777-process` /
//! `i143-2777-container`, which export `I143_ACCEPTANCE_DIR` (this cell's `result.json` goes there)
//! and whose command sets `RDM_ARTIFACTS_DIR` (the estate's manifest and every process's spans
//! land under it, feature `i143-2777`, test the cell's name) and `MESH_SPAWN_TYPE`.
//!
//! The canary body is `rafka_test_scenario::canary::restart_canary`; the seed is
//! `scenarios/i143-node-rpc-seed.yaml` through `runner::run`. Both reach their nodes through
//! node-admin Build only.

use rafka_test_scenario::canary::restart_canary;
use rafka_test_scenario::estate::{artifacts_root, wait_for, Estate, Owner};
use rafka_test_scenario::{runner, scenario::Scenario};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

const FEATURE: &str = "i143-2777";

fn provider() -> String {
    std::env::var("MESH_SPAWN_TYPE").unwrap_or_default()
}

/// A cell runs only on the provider it is named for: a process run never stands in for a container cell.
fn require_provider(want: &str) {
    assert_eq!(provider(), want, "this cell runs only with MESH_SPAWN_TYPE={want}; another provider never stands in for it");
}

fn owner(test: &str, subfeature: &str) -> Owner {
    Owner { product: "mesh".into(), feature: FEATURE.into(), subfeature: subfeature.into(), rung: "multi-node".into(), provider: provider(), test: test.into() }
}

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2777").join(provider()).join(cell),
    }
}

/// Where the estate of `cell` left its manifest, ledger and every process's spans.
fn estate_dir(cell: &str) -> PathBuf {
    artifacts_root().join(FEATURE).join(cell)
}

fn spans_of(cell: &str) -> Vec<Value> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(estate_dir(cell).join("spans")).into_iter().flatten().flatten() {
        if e.path().to_string_lossy().ends_with(".spans.jsonl") {
            out.extend(std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()));
        }
    }
    out
}

fn trace_url(trace_id: &str) -> String {
    format!("{}/trace/{trace_id}", std::env::var("JAEGER_QUERY_URL").unwrap_or_else(|_| "http://localhost:16686".into()))
}

fn write_result(cell: &str, result: &Value) {
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_string_pretty(result).unwrap()).unwrap();
}

/// The canary on `provider`: run it, then record what it observed and where its evidence is.
async fn canary_cell(cell: &str, provider: &str) -> Value {
    require_provider(provider);
    let facts = restart_canary(owner(cell, "restart-canary")).await;
    assert!(estate_dir(cell).join("manifest.json").is_file(), "the estate manifest of {cell} exists under {}", estate_dir(cell).display());
    let spans = spans_of(cell);
    assert!(!spans.is_empty(), "the estate exported spans under {}", estate_dir(cell).display());
    let trace = facts["trace_id"].as_str().expect("the canary names its restart trace").to_string();
    assert!(spans.iter().any(|s| s["trace_id"] == trace.as_str()), "the restart trace {trace} is in the exported spans");
    write_result(
        cell,
        &json!({
            "cell": cell, "provider": provider, "facts": facts, "trace_id": trace, "trace_url": trace_url(&trace),
            "estate_dir": estate_dir(cell), "spans": spans.len(),
        }),
    );
    facts
}

/// CONTRACT: on real processes, an RPC node restarted through Build is the same node, in the same
/// process, under the same incarnation, endpoint and port (stop then start on the parked process);
/// the value written before the restart is read back from its own data dir; a request cut before it
/// finished is NotSent and never applied; the restart's Build -> reconcile -> node -> step -> start
/// spans are parented in one trace.
#[tokio::test(flavor = "multi_thread")]
async fn proof_canary_restarts_same_node_preserves_state() {
    let facts = canary_cell("proof_canary_restarts_same_node_preserves_state", "process").await;
    assert!(facts["process_before"]["pid"].is_u64() && facts["process_before"] == facts["process_after"], "a restart is stop then start on the same process: {facts}");
}

/// CONTRACT: the same canary on real containers: the restarted node keeps its container, and
/// everything the process cell proves holds.
#[tokio::test(flavor = "multi_thread")]
async fn proof_canary_restarts_container_preserves_state() {
    let facts = canary_cell("proof_canary_restarts_container_preserves_state", "container").await;
    assert!(facts["process_before"]["container"].is_string() && facts["process_before"] == facts["process_after"], "a restart is stop then start in the same container: {facts}");
    assert_eq!(facts["process_after"]["provider"], "container", "{facts}");
}

/// The seed on `provider`: every operation applied and every assertion read back, with the
/// estate's evidence under this issue's feature.
async fn seed_cell(cell: &str, provider: &str) {
    require_provider(provider);
    let mut scenario = Scenario::parse(include_str!("../scenarios/i143-node-rpc-seed.yaml")).expect("seed parses");
    scenario.feature = FEATURE.into();
    let report = runner::run(&scenario, Some(provider), cell).await;
    assert!(report.failures.is_empty(), "seed failures: {:#?}", report.failures);
    assert!(estate_dir(cell).join("manifest.json").is_file(), "the estate manifest of {cell} exists");
    let spans = spans_of(cell);
    let create = spans
        .iter()
        .find(|s| s["name"] == "rdm.node_admin.build.create.via-rest" && s["attributes"]["build_id"] == report.build_id.as_str())
        .expect("the accepted seed Build's create span is exported");
    let trace = create["trace_id"].as_str().unwrap().to_string();
    let after = estate_dir(cell).join("nodes-after.json");
    let nodes: Value = serde_json::from_str(&std::fs::read_to_string(&after).expect("nodes-after.json")).unwrap();
    let ready = |kind: &str| nodes.as_array().unwrap().iter().filter(|n| n["kind"] == kind && n["status"] == "ready-for-traffic").count();
    assert_eq!((ready("node_admin"), ready("rpc_node")), (2, 3), "the seed shape is MN = 2 admins + 3 rpc nodes: {nodes}");
    write_result(
        cell,
        &json!({
            "cell": cell, "provider": provider, "build_id": report.build_id, "operations": scenario.operations.len(),
            "assertions": scenario.assert.len(), "failures": [], "trace_id": trace, "trace_url": trace_url(&trace),
            "estate_dir": estate_dir(cell), "spans": spans.len(),
        }),
    );
}

/// CONTRACT: the checked-in seed, brought up as MN through Build on real processes, applies its eight
/// operations in order over real Node RPC and reads back exactly its three asserted values.
#[tokio::test(flavor = "multi_thread")]
async fn seed_runner_completes_every_operation_on_process() {
    seed_cell("seed_runner_completes_every_operation_on_process", "process").await;
}

/// CONTRACT: the identical seed on real containers reads back the identical values.
#[tokio::test(flavor = "multi_thread")]
async fn seed_runner_completes_every_operation_on_container() {
    seed_cell("seed_runner_completes_every_operation_on_container", "container").await;
}

/// The control base of the fabric-primary as the entry admin advertises it now.
async fn primary_base(estate: &Estate) -> String {
    let (_, fabric) = estate.get("/api/fabric").await;
    fabric["admin_api_base"].as_str().filter(|b| !b.is_empty()).expect("the fabric advertises its primary").to_string()
}

/// One MN estate whose Build is accepted; returns it with the Build id and the base of the admin that accepted it.
async fn estate_with_build_in_flight(cell: &str) -> (Estate, String, String) {
    let estate = Estate::bootstrap(owner(cell, "restart-refusals"), "fabric1", "mesh1").await;
    let shape = json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "rpc_node": 3}]});
    let (status, accepted) = estate.post("/api/build", &shape).await;
    assert_eq!(status, 202, "{accepted}");
    let build = accepted["build_id"].as_str().expect("build_id").to_string();
    let base = primary_base(&estate).await;
    (estate, build, base)
}

/// CONTRACT: while a Build is still reconciling, a restart is refused `409 build-in-progress` naming
/// that Build and opens no attempt; once the Build settles the same restart is accepted as the next
/// attempt of that Build.
#[tokio::test(flavor = "multi_thread")]
async fn restart_during_an_inflight_build_is_refused_and_opens_no_attempt() {
    require_provider("process");
    let cell = "restart_during_an_inflight_build_is_refused_and_opens_no_attempt";
    let (estate, build, base) = estate_with_build_in_flight(cell).await;
    let (status, refused) = estate.http_post(&base, "/api/nodes/mesh1.rpc.2/restart", &json!({})).await;
    assert_eq!(status, 409, "{refused}");
    assert_eq!((refused["error"].as_str(), refused["current_build_id"].as_str()), (Some("build-in-progress"), Some(build.as_str())), "{refused}");
    let (_, view) = estate.http_get(&base, &format!("/api/builds?id={build}")).await;
    assert!(view["attempt"].as_u64().unwrap_or(0) <= 1 && view["reason"] != "restart", "the refusal opened no attempt: {view}");
    estate.await_build(&build, Duration::from_secs(120)).await;
    wait_for("MN settled", Duration::from_secs(120), || async {
        let nodes = estate.nodes().await;
        (nodes.iter().filter(|n| n["kind"] == "rpc_node" && n["status"] == "ready-for-traffic").count() == 3).then_some(())
    })
    .await;
    let base = primary_base(&estate).await;
    let (status, accepted) = estate.http_post(&base, "/api/nodes/mesh1.rpc.2/restart", &json!({})).await;
    assert_eq!(status, 202, "{accepted}");
    assert_eq!(accepted["build_id"], build.as_str(), "the restart is an attempt of the accepted Build: {accepted}");
    estate.await_attempt(&build, Estate::attempt_of(&accepted), Duration::from_secs(120)).await;
    write_result(cell, &json!({"cell": cell, "provider": "process", "build_id": build, "refused": refused, "restart_accepted": accepted}));
    estate.shutdown().await;
}

/// CONTRACT: a restart naming a path the Build does not hold is refused `404 unknown-node` and
/// opens no attempt; the accepted Build's attempt count is what it was.
#[tokio::test(flavor = "multi_thread")]
async fn restart_of_an_unknown_node_is_refused_by_name_and_opens_no_attempt() {
    require_provider("process");
    let cell = "restart_of_an_unknown_node_is_refused_by_name_and_opens_no_attempt";
    let (estate, build, _) = estate_with_build_in_flight(cell).await;
    estate.await_build(&build, Duration::from_secs(120)).await;
    let base = primary_base(&estate).await;
    let (_, before) = estate.http_get(&base, &format!("/api/builds?id={build}")).await;
    let (status, refused) = estate.http_post(&base, "/api/nodes/mesh1.rpc.9/restart", &json!({})).await;
    assert_eq!((status, refused["error"].as_str()), (404, Some("unknown-node")), "{refused}");
    let (_, after) = estate.http_get(&base, &format!("/api/builds?id={build}")).await;
    assert_eq!(before["attempt"], after["attempt"], "the refusal opened no attempt: {before} / {after}");
    write_result(cell, &json!({"cell": cell, "provider": "process", "build_id": build, "refused": refused, "attempt_before": before["attempt"], "attempt_after": after["attempt"]}));
    estate.shutdown().await;
}
