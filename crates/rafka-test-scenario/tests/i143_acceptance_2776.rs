//! i143.e7.s4 acceptance (rafka-v2 #2776, hardened 2026-10-07): the scenario runner, its
//! explicit executable-binding seam, its replay manifests and the acceptance runner's receipts.
//! Run by `scripts/i143-acceptance-gate.sh i143-2776-{unit,process,container}`, which exports
//! `I143_ACCEPTANCE_DIR` (each cell's `result.json` goes there) and whose estate commands set
//! `RDM_ARTIFACTS_DIR` (feature `i143-2776`, test the cell's name).

use rafka_node_admin_client::binding::{sha256_file, Binding, BindingError, BindingSet, Candidate};
use rafka_test_scenario::estate::{descends_from, named, Estate, Owner, RUNTIME_IMAGE};
use serde_json::{json, Value};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

fn provider() -> String {
    std::env::var("MESH_SPAWN_TYPE").unwrap_or_else(|_| "process".into())
}

fn owner(test: &str, subfeature: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "i143-2776".into(), subfeature: subfeature.into(), rung: "MN".into(), provider: provider(), test: test.into() }
}

fn acceptance_dir(layer: &str, cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2776").join(layer).join(cell),
    }
}

fn write_result(dir: &Path, v: &Value) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(v).unwrap()).unwrap();
}

/// The exact source this run is of.
fn candidate_sha() -> String {
    if let Ok(s) = std::env::var("I143_CANDIDATE_SHA") {
        return s;
    }
    let out = std::process::Command::new("git").args(["rev-parse", "HEAD"]).current_dir(env!("CARGO_MANIFEST_DIR")).output().expect("git rev-parse HEAD");
    String::from_utf8(out.stdout).unwrap().trim().to_string()
}

// ---- the explicit external binary set ----------------------------------------------------------

/// The four consumer executables, by launch id (built by `-p rafka-consumer-fixture`).
fn consumer_executables() -> [(&'static str, PathBuf); 4] {
    let exe = |name: &str| {
        let p = rafka_test_scenario::estate::bin_dir().join(name);
        assert!(p.exists(), "RED: consumer executable `{name}` is not built at {} (cargo build -p rafka-consumer-fixture)", p.display());
        p
    };
    [("node_admin", exe("consumer-node-admin")), ("broker", exe("consumer-broker")), ("gateway", exe("consumer-gateway")), ("compute", exe("consumer-compute"))]
}

/// The complete, true binding set of the consumer executables for the run's provider.
fn consumer_set(sha: &str) -> BindingSet {
    let container = provider() == "container";
    BindingSet {
        candidate: Candidate { sha: sha.into(), build: "consumer-fixture".into() },
        launch_ids: consumer_executables().iter().map(|(id, _)| id.to_string()).collect(),
        bindings: consumer_executables()
            .iter()
            .map(|(id, path)| Binding {
                launch_id: id.to_string(),
                executable: path.canonicalize().unwrap(),
                sha256: sha256_file(path).unwrap(),
                image: container.then(|| RUNTIME_IMAGE.to_string()),
            })
            .collect(),
    }
}

const REQUIRED: [&str; 3] = ["broker", "gateway", "compute"];

/// Host processes running any of `paths` (by `/proc/<pid>/exe`).
fn processes_running(paths: &[PathBuf]) -> Vec<(u32, PathBuf)> {
    let mut out = Vec::new();
    for e in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
        let Some(pid) = e.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else { continue };
        if let Ok(exe) = std::fs::read_link(format!("/proc/{pid}/exe")) {
            if paths.contains(&exe) {
                out.push((pid, exe));
            }
        }
    }
    out
}

fn docker(args: &[&str]) -> String {
    let out = std::process::Command::new("docker").args(args).output().unwrap_or_else(|e| panic!("docker {args:?}: {e}"));
    assert!(out.status.success(), "docker {args:?}: {}", String::from_utf8_lossy(&out.stderr));
    String::from_utf8_lossy(&out.stdout).trim().to_string()
}

/// Containers (any state) with a bind mount whose source is one of `paths`.
fn containers_mounting(paths: &[PathBuf]) -> Vec<String> {
    let ids = docker(&["ps", "-a", "-q", "--no-trunc"]);
    let mut out = Vec::new();
    for id in ids.lines().filter(|l| !l.is_empty()) {
        let Ok(m) = std::process::Command::new("docker").args(["inspect", "--format", "{{range .Mounts}}{{.Source}}\n{{end}}", id]).output() else { continue };
        if String::from_utf8_lossy(&m.stdout).lines().any(|s| paths.iter().any(|p| p.as_os_str() == s)) {
            out.push(id.to_string());
        }
    }
    out
}

/// What is running the bound executables right now: host processes and containers.
fn provider_actions(paths: &[PathBuf]) -> Value {
    let procs = processes_running(paths);
    let containers = if provider() == "container" { containers_mounting(paths) } else { Vec::new() };
    json!({ "processes": procs.iter().map(|(p, e)| json!({"pid": p, "exe": e})).collect::<Vec<_>>(), "containers": containers })
}

/// One planted defect of the healthy set, the rule that must refuse it and the run's inputs.
struct Planted {
    name: &'static str,
    rule: &'static str,
    set: BindingSet,
    candidate: String,
    required: Vec<&'static str>,
}

fn planted(sha: &str, scratch: &Path) -> Vec<Planted> {
    let good = || consumer_set(sha);
    let p = |name, rule, set, candidate: &str, required: Vec<&'static str>| Planted { name, rule, set, candidate: candidate.to_string(), required };
    let mut out = Vec::new();

    let mut s = good();
    s.bindings[1].executable = scratch.join("absent-executable");
    out.push(p("missing_file", "ExecutableAbsent", s, sha, REQUIRED.to_vec()));

    let mut s = good();
    let plain = scratch.join("not-executable");
    std::fs::copy(&s.bindings[1].executable, &plain).unwrap();
    std::fs::set_permissions(&plain, std::os::unix::fs::PermissionsExt::from_mode(0o644)).unwrap();
    s.bindings[1].executable = plain;
    out.push(p("non_executable", "NotExecutable", s, sha, REQUIRED.to_vec()));

    let mut s = good();
    s.bindings[1].sha256 = "0".repeat(64);
    out.push(p("hash_mismatch", "HashMismatch", s, sha, REQUIRED.to_vec()));

    let mut s = good();
    s.bindings.retain(|b| b.launch_id != "gateway");
    out.push(p("incomplete_map_declared_launch_id_unbound", "MissingBinding", s, sha, REQUIRED.to_vec()));

    let mut s = good();
    s.bindings.retain(|b| b.launch_id != "gateway");
    s.launch_ids.retain(|i| i != "gateway");
    out.push(p("incomplete_map_required_launch_id_undeclared", "NotRequiredByTheSet", s, sha, REQUIRED.to_vec()));

    let mut s = good();
    s.bindings.push(s.bindings[2].clone());
    out.push(p("duplicate_binding", "DuplicateBinding", s, sha, REQUIRED.to_vec()));

    out.push(p("candidate_mismatch", "CandidateMismatch", good(), "0123456789abcdef0123456789abcdef01234567", REQUIRED.to_vec()));

    let mut s = good();
    s.bindings[2].image = Some("another-image:1".into());
    let rule = if provider() == "container" { "ImageMismatch" } else { "ImageUnderProcessProvider" };
    out.push(p("wrong_image", rule, s, sha, REQUIRED.to_vec()));
    out
}

fn variant(e: &BindingError) -> String {
    format!("{e:?}").split(|c: char| !c.is_alphanumeric()).next().unwrap_or_default().to_string()
}

/// The exact executable and image a node of the estate runs, observed from the provider (the
/// process's `/proc/<pid>/exe`, the container's entry point and bind mount), never from a span.
async fn observe_launch(estate: &Estate, node: &str) -> Value {
    let n = estate.node(node).await;
    if provider() == "container" {
        let id = estate.container_of(node).unwrap_or_else(|| panic!("{node}: no running container"));
        let inspect: Value = serde_json::from_str(&docker(&["inspect", "--format", "{{json .}}", &id])).unwrap();
        let path = inspect["Path"].as_str().unwrap().to_string();
        // The mount that supplies the entry point: the executable itself (a node), or its directory
        // (the Day-0 admin, which also reads the bound files).
        let mount = inspect["Mounts"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|m| Path::new(&path).starts_with(m["Destination"].as_str().unwrap()))
            .max_by_key(|m| m["Destination"].as_str().unwrap().len())
            .unwrap_or_else(|| panic!("{node}: no mount supplies its entry point {path}: {}", inspect["Mounts"]))
            .clone();
        let rest = Path::new(&path).strip_prefix(mount["Destination"].as_str().unwrap()).unwrap();
        let source = if rest.as_os_str().is_empty() { mount["Source"].as_str().unwrap().to_string() } else { Path::new(mount["Source"].as_str().unwrap()).join(rest).display().to_string() };
        json!({
            "node": node, "node_id": n["node_id"], "incarnation_id": n["incarnation_id"], "kind": n["kind"],
            "container": id, "image": inspect["Config"]["Image"], "entry_point": path, "mount_source": source, "mount_read_only": mount["RW"] == false,
            "observed_sha256": sha256_file(Path::new(&source)).unwrap(),
        })
    } else {
        // The Day-0 admin is the estate's own child: adopted, so it has no provider deployment record.
        let pid = match (node == "mesh1.admin.1", estate.bootstrap_pid()) {
            (true, Some(p)) => p as u64,
            _ => estate.pid_of(node).await,
        };
        let exe = std::fs::read_link(format!("/proc/{pid}/exe")).unwrap_or_else(|e| panic!("{node}: /proc/{pid}/exe: {e}"));
        json!({
            "node": node, "node_id": n["node_id"], "incarnation_id": n["incarnation_id"], "kind": n["kind"],
            "pid": pid, "exe": exe, "observed_sha256": sha256_file(Path::new(&format!("/proc/{pid}/exe"))).unwrap(),
        })
    }
}

fn launched_path(o: &Value) -> PathBuf {
    PathBuf::from(o["exe"].as_str().or(o["mount_source"].as_str()).unwrap())
}

/// CONTRACT: a scenario runner given an explicit external binary set launches EXACTLY those
/// executables and no built-in. A set with a missing or non-executable file, a hash, candidate or
/// image mismatch, a duplicate or an incomplete map is refused by its own named rule before any
/// file or process is made, and the provider then holds no runtime of the set. The complete set
/// starts the Day-0 admin, a second admin and the broker, gateway and compute the admin launches;
/// each runs the bound path with the bound hash (read from the provider, not a span); a proof
/// round-trip through the launched nodes succeeds; restarting a node and the second admin launches
/// the same binding under a new incarnation and the same NodeId; the estate's spans carry the
/// consumer admin's service and never the built-in admin's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_runner_launches_explicit_external_binary_set() {
    let cell = "scenario_runner_launches_explicit_external_binary_set";
    let layer = provider();
    let dir = acceptance_dir(&layer, cell);
    let sha = candidate_sha();
    let bound: Vec<PathBuf> = consumer_set(&sha).bindings.iter().map(|b| b.executable.clone()).collect();
    let scratch = std::env::temp_dir().join(format!("i143-2776-planted-{}", std::process::id()));
    std::fs::create_dir_all(&scratch).unwrap();

    // 1. Every planted defect is refused by name before anything is launched.
    let estate_root = std::env::temp_dir().join(format!("i143-{cell}-{}", std::process::id()));
    let before = provider_actions(&bound);
    assert_eq!(before, json!({"processes": [], "containers": []}), "nothing runs a bound executable before the cell starts");
    let mut refusals = Vec::new();
    for pl in planted(&sha, &scratch) {
        let outcome = Estate::bootstrap_external(owner(cell, "external-binding"), "fabric1", "mesh1", &pl.set, &pl.candidate, &pl.required).await;
        let err = match outcome {
            Ok(_) => panic!("planted `{}` was accepted; the rule `{}` must refuse it", pl.name, pl.rule),
            Err(e) => e,
        };
        assert_eq!(variant(&err), pl.rule, "planted `{}` is refused by `{}`, got: {err}", pl.name, pl.rule);
        let after = provider_actions(&bound);
        assert_eq!(after, before, "planted `{}`: a refusal makes no provider action", pl.name);
        assert!(!estate_root.exists(), "planted `{}`: a refusal makes no estate root", pl.name);
        refusals.push(json!({ "planted": pl.name, "rule": pl.rule, "refusal": err.to_string(), "provider_actions_after": after }));
    }
    let _ = std::fs::remove_dir_all(&scratch);

    // 2. The healthy set: the presence control. It starts, serves and restarts through the binding.
    let set = consumer_set(&sha);
    let mut estate = Estate::bootstrap_external(owner(cell, "external-binding"), "fabric1", "mesh1", &set, &sha, &REQUIRED).await.unwrap_or_else(|e| panic!("the healthy set is accepted: {e}"));
    let (status, a) = estate.post("/api/build", &json!({"fabric": "fabric1", "meshes": [{"name": "mesh1", "node_admin": 2, "broker": 1, "gateway": 1, "compute": 1}]})).await;
    assert_eq!(status, 202, "{a}");
    estate.await_build(a["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
    let names: BTreeSet<String> = ["mesh1.admin.1", "mesh1.admin.2", "mesh1.broker.1", "mesh1.gateway.1", "mesh1.compute.1"].iter().map(|n| n.to_string()).collect();
    estate.settled(&names, Duration::from_secs(30)).await;

    let by_kind = |kind: &str| set.bindings.iter().find(|b| b.launch_id == kind).unwrap().clone();
    let expect_for = |node: &str| by_kind(match node.split('.').nth(1).unwrap() { "admin" => "node_admin", k => k });
    let mut launches = Vec::new();
    for node in &names {
        let o = observe_launch(&estate, node).await;
        let b = expect_for(node);
        assert_eq!(launched_path(&o), b.executable, "{node} runs the bound executable: {o}");
        assert_eq!(o["observed_sha256"], b.sha256.as_str(), "{node} runs the bound bytes: {o}");
        if provider() == "container" {
            assert_eq!(o["image"], RUNTIME_IMAGE, "{node} runs in the bound image: {o}");
            assert_eq!(o["mount_read_only"], true, "{node}'s executable is a read-only mount: {o}");
        }
        launches.push(o);
    }

    // The proof round-trip through the launched nodes.
    let put = estate.probe(&["put", "--target", "path:mesh1.broker.1", "--key", "7", "--value", "bound"]);
    assert_eq!(put["outcome"], "Reply", "{put}");
    let get = estate.probe(&["get", "--target", "path:mesh1.broker.1", "--key", "7"]);
    assert_eq!((get["outcome"].as_str(), get["reply"]["result"]["value"].as_str()), (Some("Reply"), Some("bound")), "{get}");

    // Restarts through the same binding: a role node, and the admin that launched the nodes.
    let mut restarts = Vec::new();
    for node in ["mesh1.broker.1", "mesh1.admin.2"] {
        let before = estate.node(node).await;
        let (status, r) = estate.post(&format!("/api/nodes/{node}/restart"), &Value::Null).await;
        assert_eq!(status, 202, "{node}: {r}");
        estate.await_attempt(r["build_id"].as_str().unwrap(), Estate::attempt_of(&r), Duration::from_secs(120)).await;
        let after = rafka_test_scenario::estate::wait_for(&format!("{node} reborn ready under a new incarnation"), Duration::from_secs(60), || async {
            let n = estate.node_opt(node).await?;
            (n["status"] == "ready-for-traffic" && n["incarnation_id"] != before["incarnation_id"]).then_some(n)
        })
        .await;
        assert_eq!(after["node_id"], before["node_id"], "{node}: a restart keeps the NodeId");
        let o = observe_launch(&estate, node).await;
        let b = expect_for(node);
        assert_eq!(launched_path(&o), b.executable, "{node} restarts on the bound executable: {o}");
        assert_eq!(o["observed_sha256"], b.sha256.as_str(), "{node} restarts on the bound bytes: {o}");
        restarts.push(json!({ "node": node, "old_incarnation_id": before["incarnation_id"], "new_incarnation_id": after["incarnation_id"], "launch": o }));
    }
    let get = estate.probe(&["get", "--target", "path:mesh1.broker.1", "--key", "7"]);
    assert_eq!(get["outcome"], "Reply", "the restarted broker answers: {get}");
    let manifest_bindings = std::fs::read_to_string(estate.artifacts.join("manifest.json")).unwrap();
    let manifest: Value = serde_json::from_str(&manifest_bindings).unwrap();
    assert_eq!(manifest["executable_bindings"]["mode"], "explicit", "{manifest}");

    estate.stop().await;
    let spans = estate.spans();
    let services: BTreeSet<String> = spans.iter().filter_map(|s| s["service"].as_str().map(String::from)).collect();
    assert!(services.contains("consumer-node-admin"), "the consumer admin ran: {services:?}");
    assert!(!services.contains("rafka-node-admin"), "no built-in node-admin ran: {services:?}");
    // Every restart was an operation of the Build rectifier: the REST call that opened the attempt,
    // the executor's reconcile of that attempt, the node's update under the reconcile and the
    // deployment steps under that. The reconcile is the child of the restart call's request span: the
    // fabric-primary's claim returns the context the call left on the attempt.
    let mut chains = Vec::new();
    for r in &restarts {
        let node = r["node"].as_str().unwrap();
        let rest = named(&spans, "rdm.node_admin.build.update.via-rest").into_iter().find(|s| s["attributes"]["node"] == node).cloned().unwrap_or_else(|| panic!("{node}: no build.update.via-rest span for the restart call"));
        let op = format!("restart-node:{node}");
        let reconcile = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|s| s["attributes"]["operations"].as_str().is_some_and(|o| o.split(',').any(|x| x == op))).cloned().unwrap_or_else(|| panic!("{node}: no reconcile executed {op}"));
        let update = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|s| s["attributes"]["node"] == node && descends_from(&spans, s, &reconcile)).cloned().unwrap_or_else(|| panic!("{node}: no node.update.via-build under the reconcile"));
        let steps: Vec<&Value> = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|s| descends_from(&spans, s, &update)).collect();
        assert!(!steps.is_empty(), "{node}: deployment steps ran under its node update");
        assert_eq!(rest["attributes"]["build_id"], reconcile["attributes"]["build_id"], "{node}: the restart call and the reconcile are of one Build");
        assert_eq!(reconcile["parent_span_id"], rest["span_id"], "{node}: the reconcile is the child of the restart call's request span");
        chains.push(json!({ "node": node, "rest": {"trace_id": rest["trace_id"], "span_id": rest["span_id"]}, "reconcile": {"trace_id": reconcile["trace_id"], "span_id": reconcile["span_id"], "parent_span_id": reconcile["parent_span_id"], "attempt": reconcile["attributes"]["attempt"]}, "update": update["span_id"], "steps": steps.len(), "reconcile_trace_is_restart_calls_trace": reconcile["trace_id"] == rest["trace_id"] }));
    }
    let leftover = provider_actions(&bound);

    write_result(
        &dir,
        &json!({
            "cell": cell, "provider": provider(), "candidate_sha": sha,
            "bindings": manifest["executable_bindings"]["receipt"],
            "refusals": refusals,
            "provider_actions_before_any_launch": before,
            "launches": launches, "restarts": restarts, "restart_span_chains": chains,
            "proof_round_trip": { "put": put, "get": get },
            "span_services": services,
            "provider_actions_after_stop": leftover,
        }),
    );
}

// ---- replay manifests ---------------------------------------------------------------------------

use rafka_test_scenario::replay::{schedule_from_seed, Fault, ManifestError, ReplayManifest, ScheduledFault, Source, StepOutcome, SCHEMA_VERSION};
use rafka_test_scenario::scenario::Scenario;

const REPLAY_SCENARIO: &str = "
version: 1
product: mesh
feature: i143-2776
subfeature: replay
rung: SN
provider: process
shape: SN
fabric: fabric1
meshes:
  - name: mesh1
    node_admin: 1
    rpc_node: 2
operations:
  - put: { target: mesh1.rpc.1, key: 10, value: \"a\" }
  - put: { target: mesh1.rpc.2, key: 11, value: \"b\" }
  - cas: { target: mesh1.rpc.1, key: 10, expected: \"a\", value: \"a2\" }
  - delete: { target: mesh1.rpc.2, key: 11 }
assert:
  - get: { target: mesh1.rpc.1, key: 10, equals: \"a2\" }
  - get: { target: mesh1.rpc.2, key: 11, absent: true }
";

const SEED: u64 = 2776;

fn source() -> Source {
    Source { sha: candidate_sha(), build: "built-in".into(), pin: String::new() }
}

/// A manifest as the scenario runner writes it, with synthetic typed outcomes.
fn synthetic_manifest() -> ReplayManifest {
    let scenario = Scenario::parse(REPLAY_SCENARIO).unwrap();
    let outcome = |step: &str, action: &str, target: &str, key: Option<u64>, result: Value| StepOutcome { step: step.into(), action: action.into(), target: target.into(), key, outcome: "Reply".into(), result };
    ReplayManifest {
        schema_version: SCHEMA_VERSION,
        owner: rafka_test_scenario::replay::EvidenceOwner { product: "mesh".into(), feature: "i143-2776".into(), subfeature: "replay".into(), rung: "SN".into(), provider: "process".into(), test: "synthetic".into() },
        seed: SEED,
        source: source(),
        shapes: scenario.meshes.clone(),
        fault_schedule: schedule_from_seed(SEED, &scenario),
        outcomes: vec![
            outcome("operation[0]", "put", "mesh1.rpc.1", Some(10), json!({})),
            outcome("operation[2]", "cas", "mesh1.rpc.1", Some(10), json!({"swapped": true})),
            outcome("assert[0]", "get", "mesh1.rpc.1", Some(10), json!({"found": true, "value": "a2"})),
        ],
        final_state: [("mesh1.rpc.1/10".to_string(), json!({"outcome": "Reply", "found": true, "value": "a2"}))].into(),
        scenario,
    }
}

/// CONTRACT: a replay manifest keeps its owner (product, feature, subfeature, rung, provider,
/// test), seed, source/build/pin, shapes, fault schedule and typed outcomes through a JSON round
/// trip unchanged; the same seed derives the same fault schedule; and a lost manifest, another
/// schema version, an unknown or disagreeing provider, a missing field, shapes that differ from
/// the scenario, and a fault at a step or node the scenario lacks are each refused by their own
/// named schema error. Role-like names in the synthetic scenario are labels, not authority rules.
#[test]
fn scenario_manifest_round_trips_owner_seed_and_fault_schedule() {
    let cell = "scenario_manifest_round_trips_owner_seed_and_fault_schedule";
    let m = synthetic_manifest();
    m.validate().expect("the synthetic manifest is valid");
    assert_eq!(m.fault_schedule.len(), 1, "a seed derives one scheduled fault");
    assert_eq!(m.fault_schedule, schedule_from_seed(SEED, &m.scenario), "the same seed derives the same schedule");
    let schedules: BTreeSet<String> = (0..64u64).map(|s| format!("{:?}", schedule_from_seed(s, &m.scenario))).collect();
    assert!(schedules.len() > 1, "different seeds derive different schedules");

    let text = m.to_json();
    let back = ReplayManifest::parse(&text).expect("a written manifest parses");
    assert_eq!(back, m, "the round trip keeps every field");
    assert_eq!((back.seed, &back.owner.provider, &back.source.sha), (SEED, &"process".to_string(), &candidate_sha()));

    let v: Value = serde_json::from_str(&text).unwrap();
    let mutate = |f: &dyn Fn(&mut Value)| {
        let mut x = v.clone();
        f(&mut x);
        ReplayManifest::parse(&x.to_string())
    };
    let mut refusals = Vec::new();
    let mut expect = |name: &str, got: Result<ReplayManifest, ManifestError>, want: fn(&ManifestError) -> bool| {
        let e = got.expect_err(name);
        assert!(want(&e), "{name}: refused by the wrong rule: {e:?}");
        refusals.push(json!({ "planted": name, "refusal": e.to_string() }));
    };
    expect("lost", ReplayManifest::parse("  \n"), |e| matches!(e, ManifestError::Lost));
    expect("truncated", ReplayManifest::parse(&text[..text.len() / 2]), |e| matches!(e, ManifestError::Schema(_)));
    expect("missing_seed", mutate(&|x| { x.as_object_mut().unwrap().remove("seed"); }), |e| matches!(e, ManifestError::Schema(m) if m.contains("seed")));
    expect("unknown_field", mutate(&|x| { x["invented"] = json!(1); }), |e| matches!(e, ManifestError::Schema(m) if m.contains("invented")));
    expect("schema_version", mutate(&|x| { x["schema_version"] = json!(2); }), |e| matches!(e, ManifestError::UnsupportedSchemaVersion(2)));
    expect("unknown_provider", mutate(&|x| { x["owner"]["provider"] = json!("vm"); }), |e| matches!(e, ManifestError::UnknownProvider(p) if p == "vm"));
    expect("provider_disagrees", mutate(&|x| { x["owner"]["provider"] = json!("container"); }), |e| matches!(e, ManifestError::ProviderDisagrees { .. }));
    expect("empty_owner_test", mutate(&|x| { x["owner"]["test"] = json!(""); }), |e| matches!(e, ManifestError::EmptyOwnerField("test")));
    expect("empty_source_sha", mutate(&|x| { x["source"]["sha"] = json!(""); }), |e| matches!(e, ManifestError::EmptySourceField("sha")));
    expect("shapes_disagree", mutate(&|x| { x["shapes"][0]["rpc_node"] = json!(9); }), |e| matches!(e, ManifestError::ShapesDisagree));
    expect("fault_step", mutate(&|x| { x["fault_schedule"][0]["before_operation"] = json!(99); }), |e| matches!(e, ManifestError::FaultStepOutOfRange { before_operation: 99, .. }));
    expect("fault_target", mutate(&|x| { x["fault_schedule"][0]["fault"]["restart_node"]["node"] = json!("mesh1.broker.1"); }), |e| matches!(e, ManifestError::FaultTargetUnknown(n) if n == "mesh1.broker.1"));

    let dir = acceptance_dir("unit", cell);
    write_result(
        &dir,
        &json!({
            "cell": cell, "seed": SEED, "owner": back.owner, "source": back.source, "shapes": back.shapes,
            "fault_schedule": back.fault_schedule, "outcomes": back.outcomes.len(),
            "round_trip_equal": true, "distinct_schedules_over_64_seeds": schedules.len(), "refusals": refusals,
        }),
    );
    // A unit cell runs no process, so there is no exported span to preserve.
    std::fs::write(dir.join("spans.json"), "[]").unwrap();
    let _ = Fault::RestartNode { node: String::new() };
    let _ = ScheduledFault { before_operation: 0, fault: Fault::RestartNode { node: String::new() } };
}

/// CONTRACT: running one declarative scenario twice - the second time from the first run's
/// written replay manifest alone, on a fresh estate - repeats the same ordered actions, the same
/// typed semantic outcomes and the same final proof state, with the exact seed and fault schedule
/// retained. The seeded fault is a restart through the node-admin Build rectifier: the restarted
/// node keeps its NodeId under a new incarnation in both runs. Each run leaves its complete
/// manifest, its per-host spans and its RPC ledger under its own estate directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scenario_runner_replays_manifest_preserves_semantic_outcomes() {
    let cell = "scenario_runner_replays_manifest_preserves_semantic_outcomes";
    let layer = provider();
    let dir = acceptance_dir(&layer, cell);
    let mut scenario = Scenario::parse(REPLAY_SCENARIO).unwrap();
    scenario.provider = layer.clone();
    let faults = schedule_from_seed(SEED, &scenario);

    let first = rafka_test_scenario::runner::run_replayable(&scenario, None, cell, SEED, source(), faults.clone()).await.expect("the first run's manifest is valid");
    assert!(first.failures.is_empty(), "the first run failed: {:?}", first.failures);

    // The replay has only the manifest the first run wrote: parsed from its estate directory.
    let written = std::fs::read_to_string(rafka_test_scenario::estate::artifacts_root().join("i143-2776").join(cell).join("replay-manifest.json")).expect("the first run left its manifest");
    let loaded = ReplayManifest::parse(&written).expect("the first run's manifest parses");
    assert_eq!(loaded, first.manifest, "the manifest on disk is the run's manifest");
    let replay_test = format!("{cell}-replay");
    let second = rafka_test_scenario::runner::run_replayable(&loaded.scenario, Some(&loaded.owner.provider), &replay_test, loaded.seed, loaded.source.clone(), loaded.fault_schedule.clone())
        .await
        .expect("the replay's manifest is valid");
    assert!(second.failures.is_empty(), "the replay failed: {:?}", second.failures);

    let (a, b) = (&first.manifest, &second.manifest);
    assert_eq!(a.seed, b.seed, "the exact seed is retained");
    assert_eq!(a.fault_schedule, b.fault_schedule, "the exact fault schedule is retained");
    assert_eq!(a.shapes, b.shapes);
    assert_eq!(a.actions(), b.actions(), "the same ordered actions");
    assert_eq!(a.outcomes, b.outcomes, "the same typed semantic outcomes");
    assert_eq!(a.final_state, b.final_state, "the same final proof state");
    let restarts: Vec<&StepOutcome> = a.outcomes.iter().filter(|o| o.action == "restart_node").collect();
    assert_eq!(restarts.len(), 1, "the seeded fault ran: {:?}", a.outcomes);
    assert_eq!(restarts[0].result, json!({"same_node_id": true, "new_incarnation": true}), "a restart keeps the NodeId under a new incarnation");
    assert_eq!(a.final_state["mesh1.rpc.1/10"]["value"], "a2");
    assert_eq!(a.final_state["mesh1.rpc.2/11"]["found"], false);

    let root = rafka_test_scenario::estate::artifacts_root().join("i143-2776");
    let mut hosts = Vec::new();
    for t in [cell.to_string(), replay_test.clone()] {
        let d = root.join(&t);
        let manifest: Value = serde_json::from_slice(&std::fs::read(d.join("manifest.json")).expect("the estate manifest")).unwrap();
        assert_eq!(manifest["seed"], SEED, "{t}: the estate manifest carries the seed");
        assert_eq!(manifest["provider"], layer.as_str());
        assert_eq!(manifest["feature"], "i143-2776");
        assert!(d.join("rpc-ledger.jsonl").metadata().map(|m| m.len() > 0).unwrap_or(false), "{t}: the RPC ledger is not empty");
        let spans: Vec<_> = std::fs::read_dir(d.join("spans")).unwrap().flatten().filter(|e| e.path().to_string_lossy().ends_with(".spans.jsonl")).collect();
        assert!(!spans.is_empty(), "{t}: per-host span files exist");
        hosts.push(json!({ "test": t, "span_files": spans.len(), "manifest": manifest }));
    }

    write_result(&dir, &json!({ "cell": cell, "provider": layer, "seed": SEED, "fault_schedule": a.fault_schedule, "run": a, "replay_equal": true, "replay_actions": b.actions(), "hosts": hosts }));
}

// ---- the acceptance runner's receipts ---------------------------------------------------------

use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().unwrap()
}

static FIXTURES: AtomicUsize = AtomicUsize::new(0);

/// A one-job registry under a private temp dir, run by the real runner.
struct Fixture {
    dir: PathBuf,
    job: String,
    section: &'static str,
}

impl Fixture {
    /// `cell` is the registry cell object (its `dir` is filled in), `job` extra job-level fields.
    fn new(tag: &str, layer: &str, section: &'static str, mut cell: Value, job_extra: Value) -> Self {
        let n = FIXTURES.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("i143-2776-runner-{}-{n}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let job = if section == "rshape_jobs" { format!("i143-rshape-fixture-{}-{n}-{tag}", std::process::id()) } else { format!("i143-fixture-{}-{n}-{tag}", std::process::id()) };
        cell["name"] = json!("fixture_cell");
        cell["source"] = json!(cell["source"].as_str().unwrap_or("none"));
        cell["dir"] = json!(dir.join("cell").to_str().unwrap());
        let mut j = json!({ "issue": 0, "layer": layer, "cells": [cell] });
        for (k, v) in job_extra.as_object().into_iter().flatten() {
            j[k] = v.clone();
        }
        let reg = json!({ "contract": "fixture", "jobs": if section == "jobs" { json!({ &job: j }) } else { json!({}) }, "rshape_jobs": if section == "rshape_jobs" { json!({ &job: j }) } else { json!({}) } });
        std::fs::write(dir.join("jobs.json"), serde_json::to_vec_pretty(&reg).unwrap()).unwrap();
        Self { dir, job, section }
    }

    fn receipt_path(&self) -> PathBuf {
        let root = if self.section == "jobs" { "target/i143-acceptance/jobs" } else { "target/i143-rshape/jobs" };
        workspace_root().join(root).join(format!("{}.json", self.job))
    }

    /// Run the runner on the job: (success, its whole output, the receipt if one was written).
    fn run(&self) -> (bool, String, Option<Value>) {
        let out = Command::new("bash")
            .arg(workspace_root().join("scripts/i143-acceptance-gate.sh"))
            .arg(&self.job)
            .env("I143_ACCEPTANCE_JOBS", self.dir.join("jobs.json"))
            .env("I143_ACCEPTANCE_SKIP_BUILD", "1")
            .current_dir(workspace_root())
            .output()
            .expect("the runner starts");
        let text = format!("{}{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));
        let receipt = std::fs::read(self.receipt_path()).ok().and_then(|b| serde_json::from_slice(&b).ok());
        (out.status.success(), text, receipt)
    }

    fn cell_dir(&self) -> PathBuf {
        self.dir.join("cell")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
        let _ = std::fs::remove_file(self.receipt_path());
    }
}

const PASS: &str = "echo 'running 1 test'; echo 'test fixture_cell ... ok'; echo; echo 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out; finished in 0.00s'";
const RESULT: &str = "echo '{\"observed\":true}' > \"$I143_ACCEPTANCE_DIR/result.json\"";
const SPANS: &str = "echo '[]' > \"$I143_ACCEPTANCE_DIR/spans.json\"";

fn cell(command: String) -> Value {
    json!({ "command": command })
}

/// CONTRACT: the acceptance runner refuses, each by its own named rule and with the refused
/// receipt written, a cell that matches no test, a cell left ignored, a cell that leaves no
/// result or no span artifact, an estate that ran on another provider than its job's, a receipt
/// whose source sha is not the tree's or whose artifact bytes changed, a consumer cell whose
/// executables are not the build manifest's or whose estate ran on built-ins, a registry that
/// declares a second generic runner or receipt schema, and a job id registered twice. A clean
/// execution writes exactly the canonical receipt: job, issue, layer, source sha, dirty paths,
/// start, finish, outcome and per cell its name, outcome, refusal and every artifact's sha256.
#[test]
fn acceptance_runner_rejects_invalid_job_execution_receipts() {
    let cell_name = "acceptance_runner_rejects_invalid_job_execution_receipts";
    let mut planted_results = Vec::new();
    let mut plant = |name: &str, rule: &str, f: &Fixture| {
        let (ok, text, receipt) = f.run();
        assert!(!ok, "planted `{name}` was accepted:\n{text}");
        assert!(text.contains("REFUSED") && text.contains(rule), "planted `{name}` is refused by `{rule}`:\n{text}");
        planted_results.push(json!({ "planted": name, "rule": rule, "refused": true, "receipt_outcome": receipt.as_ref().map(|r| r["outcome"].clone()) }));
        receipt
    };

    let f = Fixture::new("zero", "unit", "jobs", cell("echo 'running 0 tests'; echo; echo 'test result: ok. 0 passed; 0 failed; 0 ignored; 0 measured; 3 filtered out; finished in 0.00s'".into()), json!({}));
    let r = plant("zero_match", "matched no test", &f).expect("a refused job still writes its receipt");
    assert_eq!(r["outcome"], "refused");

    let f = Fixture::new("skip", "unit", "jobs", cell("echo 'running 1 test'; echo 'test fixture_cell ... ignored'; echo; echo 'test result: ok. 0 passed; 0 failed; 1 ignored; 0 measured; 0 filtered out; finished in 0.00s'".into()), json!({}));
    plant("ignored_cell", "was ignored", &f);

    let f = Fixture::new("noresult", "unit", "jobs", cell(PASS.into()), json!({}));
    plant("missing_result_artifact", "left no result.json", &f);

    let f = Fixture::new("nospans", "unit", "jobs", cell(format!("{RESULT}; {PASS}")), json!({}));
    plant("missing_span_artifact", "left no spans.json", &f);

    let f = Fixture::new("noestate", "process", "jobs", cell(format!("{RESULT}; {PASS}")), json!({}));
    plant("missing_estate_manifest", "left no estate manifest", &f);

    let wrong_provider = format!("{RESULT}; mkdir -p \"$I143_ACCEPTANCE_DIR/estate/x\"; echo '{{\"provider\":\"container\"}}' > \"$I143_ACCEPTANCE_DIR/estate/x/manifest.json\"; {PASS}");
    let f = Fixture::new("provider", "process", "jobs", cell(wrong_provider), json!({}));
    plant("wrong_provider", "provider substitution", &f);

    // A second generic runner / receipt schema: refused before any cell runs.
    let f = Fixture::new("runner2", "unit", "jobs", cell(PASS.into()), json!({ "runner": "scripts/another-runner.sh" }));
    plant("second_runner", "a second generic runner or receipt schema", &f);
    let mut c = cell(PASS.into());
    c["receipt_schema"] = json!("other.json");
    let f = Fixture::new("schema2", "unit", "jobs", c, json!({}));
    plant("second_receipt_schema", "a second generic runner or receipt schema", &f);

    // A job id registered in both namespaces.
    let f = Fixture::new("dup", "unit", "jobs", cell(PASS.into()), json!({}));
    {
        let mut reg: Value = serde_json::from_slice(&std::fs::read(f.dir.join("jobs.json")).unwrap()).unwrap();
        reg["rshape_jobs"][&f.job] = reg["jobs"][&f.job].clone();
        std::fs::write(f.dir.join("jobs.json"), serde_json::to_vec(&reg).unwrap()).unwrap();
    }
    plant("duplicate_job_id", "more than one place", &f);

    // An external consumer cell: the executables are the build manifest's, and the estate ran on them.
    let stage = std::env::temp_dir().join(format!("i143-2776-consumer-{}", std::process::id()));
    std::fs::create_dir_all(&stage).unwrap();
    std::fs::write(stage.join("rshape-x"), "binary").unwrap();
    let want = sha256_file(&stage.join("rshape-x")).unwrap();
    let manifest = |hash: &str| {
        let p = stage.join("build-manifest.json");
        std::fs::write(&p, json!({ "candidate_sha": "c", "binaries": { "rshape-x": hash } }).to_string()).unwrap();
        p
    };
    let consumer_cmd = |bin: &Path, estate_mode: &str| {
        let script = stage.join(format!("cell-{estate_mode}.sh"));
        std::fs::write(
            &script,
            format!("{RESULT}\nmkdir -p \"$I143_ACCEPTANCE_DIR/estate/x\"\necho '{{\"provider\":\"process\",\"executable_bindings\":{{\"mode\":\"{estate_mode}\"}}}}' > \"$I143_ACCEPTANCE_DIR/estate/x/manifest.json\"\n{PASS}\n"),
        )
        .unwrap();
        format!("RDM_RSHAPE_CONSUMER_BIN_DIR={} bash {}", bin.display(), script.display())
    };
    let mut c = cell(consumer_cmd(&stage, "explicit"));
    c["consumer_manifest"] = json!(stage.join("absent-manifest.json").to_str().unwrap());
    let f = Fixture::new("cmissing", "process", "rshape_jobs", c, json!({}));
    plant("consumer_manifest_missing", "build manifest", &f);

    let mut c = cell(consumer_cmd(&stage, "explicit"));
    c["consumer_manifest"] = json!(manifest(&"0".repeat(64)).to_str().unwrap());
    let f = Fixture::new("chash", "process", "rshape_jobs", c, json!({}));
    plant("consumer_binary_hash_differs_from_build_manifest", "hashes to", &f);

    let mut c = cell(consumer_cmd(&stage, "built-in"));
    c["consumer_manifest"] = json!(manifest(&want).to_str().unwrap());
    let f = Fixture::new("cbuiltin", "process", "rshape_jobs", c, json!({}));
    plant("consumer_cell_estate_ran_on_built_ins", "built-in substitution", &f);

    // A model cell: a result and no span. Declared on a UNIT cell it passes and the receipt says so; declared on another layer it is refused.
    let mut c = cell(format!("{RESULT}; {PASS}"));
    c["evidence"] = json!("model");
    let f = Fixture::new("model", "unit", "jobs", c, json!({}));
    let (ok, text, receipt) = f.run();
    assert!(ok, "a model cell with a result and no spans.json passes:\n{text}");
    assert_eq!(receipt.unwrap()["cells"][0]["evidence"], "model");
    assert!(!f.cell_dir().join("spans.json").exists(), "no placeholder span was written");
    let mut c = cell(format!("{RESULT}; {PASS}"));
    c["evidence"] = json!("model");
    let f = Fixture::new("modelproc", "process", "jobs", c, json!({}));
    let (ok, text, _) = f.run();
    assert!(!ok && text.contains("model is for UNIT cells only"), "{text}");
    planted_results.push(json!({ "planted": "model_evidence_on_a_process_layer", "rule": "model is for UNIT cells only", "refused": true }));

    // The control: the same consumer cell with true executables and an explicit estate passes.
    let mut c = cell(consumer_cmd(&stage, "explicit"));
    c["consumer_manifest"] = json!(manifest(&want).to_str().unwrap());
    let f = Fixture::new("cclean", "process", "rshape_jobs", c, json!({}));
    let (ok, text, _) = f.run();
    assert!(ok, "a consumer cell on the manifest's executables with explicit bindings passes:\n{text}");

    // The clean job in each namespace, and verification of its receipt.
    let head = candidate_sha();
    let mut canonical = Vec::new();
    for (section, layer) in [("jobs", "unit"), ("rshape_jobs", "static")] {
        let f = Fixture::new("clean", layer, section, cell(format!("{RESULT}; {SPANS}; {PASS}")), json!({}));
        let (ok, text, receipt) = f.run();
        assert!(ok, "the clean {section} job passes:\n{text}");
        let receipt = receipt.expect("the clean job's receipt");
        let keys = |v: &Value| v.as_object().unwrap().keys().cloned().collect::<BTreeSet<_>>();
        assert_eq!(keys(&receipt), ["job", "issue", "layer", "source_sha", "dirty_paths", "started", "finished", "outcome", "cells"].iter().map(|s| s.to_string()).collect::<BTreeSet<_>>(), "exactly the canonical receipt: {receipt}");
        assert_eq!(keys(&receipt["cells"][0]), ["name", "evidence", "outcome", "refusal", "artifacts"].iter().map(|s| s.to_string()).collect::<BTreeSet<_>>());
        assert_eq!((receipt["outcome"].as_str(), receipt["source_sha"].as_str()), (Some("ok"), Some(head.as_str())));
        assert!(receipt["cells"][0]["refusal"].is_null());
        let arts = receipt["cells"][0]["artifacts"].as_object().unwrap();
        for n in ["gate.log", "manifest.json", "result.json", "spans.json"] {
            assert!(arts.iter().any(|(p, h)| p.ends_with(&format!("/{n}")) && h.as_str().unwrap().len() == 64), "{n} is hashed in the receipt");
        }
        let verify = |path: &Path| {
            let o = Command::new("bash").arg(workspace_root().join("scripts/i143-acceptance-gate.sh")).arg("--verify-receipt").arg(path).current_dir(workspace_root()).output().unwrap();
            (o.status.success(), format!("{}{}", String::from_utf8_lossy(&o.stdout), String::from_utf8_lossy(&o.stderr)))
        };
        let (ok, text) = verify(&f.receipt_path());
        assert!(ok, "a clean receipt verifies: {text}");

        // A receipt of another source sha.
        let mut forged = receipt.clone();
        forged["source_sha"] = json!("0123456789abcdef0123456789abcdef01234567");
        let forged_path = f.dir.join("forged-sha.json");
        std::fs::write(&forged_path, forged.to_string()).unwrap();
        let (ok, text) = verify(&forged_path);
        assert!(!ok && text.contains("is not HEAD"), "a receipt of another source sha is refused by name: {text}");
        planted_results.push(json!({ "planted": "mismatched_source_sha", "section": section, "refused": true, "refusal": text.trim() }));

        // An artifact whose bytes changed after the receipt was written.
        std::fs::write(f.cell_dir().join("result.json"), "{\"observed\":false}").unwrap();
        let (ok, text) = verify(&f.receipt_path());
        assert!(!ok && text.contains("no longer hashes"), "a changed artifact is refused by name: {text}");
        planted_results.push(json!({ "planted": "artifact_bytes_changed", "section": section, "refused": true, "refusal": text.trim() }));

        // An artifact deleted after the receipt was written.
        std::fs::remove_file(f.cell_dir().join("spans.json")).unwrap();
        let (ok, text) = verify(&f.receipt_path());
        assert!(!ok && text.contains("is missing"), "a missing artifact is refused by name: {text}");
        canonical.push(json!({ "section": section, "layer": layer, "receipt_keys": keys(&receipt), "cell_keys": keys(&receipt["cells"][0]) }));
    }
    let _ = std::fs::remove_dir_all(&stage);

    // The registry the repository ships holds one runner and one receipt schema.
    let shipped: Value = serde_json::from_slice(&std::fs::read(workspace_root().join("tools/mesh-audit/i143-acceptance-jobs.json")).unwrap()).unwrap();
    let mut owners = Vec::new();
    for section in ["jobs", "rshape_jobs"] {
        for (id, j) in shipped[section].as_object().into_iter().flatten().filter(|(k, _)| !k.starts_with('_')) {
            for key in ["runner", "receipt_writer", "receipt_schema"] {
                if j.get(key).is_some() || j["cells"].as_array().unwrap().iter().any(|c| c.get(key).is_some()) {
                    owners.push(format!("{id}.{key}"));
                }
            }
        }
    }
    assert!(owners.is_empty(), "the shipped registry declares a second generic runner/schema: {owners:?}");
    let runners: Vec<String> = std::fs::read_dir(workspace_root().join("scripts")).unwrap().flatten().map(|e| e.file_name().to_string_lossy().to_string()).filter(|n| n.starts_with("i143-acceptance-gate")).collect();
    assert_eq!(runners, vec!["i143-acceptance-gate.sh".to_string()], "exactly one acceptance runner script");

    let dir = acceptance_dir("unit", cell_name);
    write_result(&dir, &json!({ "cell": cell_name, "planted": planted_results, "canonical_receipts": canonical, "shipped_registry_second_owners": owners, "runner_scripts": runners }));
    std::fs::write(dir.join("spans.json"), "[]").unwrap();
}
