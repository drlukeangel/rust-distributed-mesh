//! i143.e7.s4 acceptance (rafka-v2 #2776, hardened 2026-10-07): the scenario runner, its
//! explicit executable-binding seam, its replay manifests and the acceptance runner's receipts.
//! Run by `scripts/i143-acceptance-gate.sh i143-2776-{unit,process,container}`, which exports
//! `I143_ACCEPTANCE_DIR` (each cell's `result.json` goes there) and whose estate commands set
//! `RAFKA_ARTIFACTS_DIR` (feature `i143-2776`, test the cell's name).

use rafka_mesh_entity::binding::{sha256_file, Binding, BindingError, BindingSet, Candidate};
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

/// The four consumer executables, by launch id.
const CONSUMER: [(&str, &str); 4] = [
    ("node_admin", env!("CARGO_BIN_EXE_consumer-node-admin")),
    ("broker", env!("CARGO_BIN_EXE_consumer-broker")),
    ("gateway", env!("CARGO_BIN_EXE_consumer-gateway")),
    ("compute", env!("CARGO_BIN_EXE_consumer-compute")),
];

/// The complete, true binding set of the consumer executables for the run's provider.
fn consumer_set(sha: &str) -> BindingSet {
    let container = provider() == "container";
    BindingSet {
        candidate: Candidate { sha: sha.into(), build: "consumer-fixture".into() },
        launch_ids: CONSUMER.iter().map(|(id, _)| id.to_string()).collect(),
        bindings: CONSUMER
            .iter()
            .map(|(id, path)| Binding {
                launch_id: id.to_string(),
                executable: PathBuf::from(path).canonicalize().unwrap(),
                sha256: sha256_file(Path::new(path)).unwrap(),
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
        estate.await_build(r["build_id"].as_str().unwrap(), Duration::from_secs(120)).await;
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
    // deployment steps under that. (The reconcile parents to the Build's accepting span, executor.rs:162,
    // so it shares the accepting call's trace, not the restart call's: recorded as `reconcile_parent`.)
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
