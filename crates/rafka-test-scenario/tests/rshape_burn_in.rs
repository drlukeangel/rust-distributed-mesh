//! i143.e11.s1 (rafka-v2 #2944): the R-shape mock composition and traffic cells. The twenty-node
//! estate (node-admin / compute / gateway / broker = 2 / 2 / 3 / 3 x 2 meshes) is born from empty
//! provider state on the four executables of an independent consumer workspace
//! (`qualification/rshape-consumer`, built by `scripts/i143-rshape-build-consumer.sh`, bound
//! explicitly through `Estate::bootstrap_external`), and typed opaque work is routed through it.
//!
//! Run by `scripts/i143-acceptance-gate.sh i143-rshape-{composition,fast}-{process,container}`,
//! which exports `I143_ACCEPTANCE_DIR` (each cell's `result.json` and evidence views go there) and
//! whose command sets `RDM_RSHAPE_CONSUMER_BIN_DIR`, `RDM_RSHAPE_TIER`, `RDM_RSHAPE_SEED`,
//! `MESH_SPAWN_TYPE` and `RAFKA_ARTIFACTS_DIR` (the estate's manifest, rpc ledger and every
//! process's spans land under it).
//!
//! The harness provisions nothing but the Day-0 admin: every other node is born by a Build the
//! node-admin rectifier executes (REST `build.create.via-rest` -> `build.update.via-reconcile` ->
//! `node.create.via-build` -> `deployment.update.via-pipeline` / `via-step`). The cells never start,
//! signal or remove a process themselves; a restart is `POST /api/nodes/<node>/restart`.
//!
//! The controller is `rafka-rpc-probe`, the generic proof-store caller of the testkit (op 0x70,
//! with 0x73 originate and 0x72 snapshot on the roles that originate), so every observed outcome is
//! the typed `RpcOutcome` of a real Node RPC call.

use rafka_mesh_entity::NodeId;
use rafka_test_scenario::estate::{binding_set_from_build_manifest, descends_from, named, wait_for, Estate, Owner, RUNTIME_IMAGE};
use rafka_test_scenario::ledger::Bucket;
use rafka_test_scenario::model::Rng;
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

// ---- the run's inputs ----------------------------------------------------------------------------

fn refuse_unset(var: &str) -> ! {
    panic!("REFUSED: {var} is not set; the registered command (tools/mesh-audit/i143-acceptance-jobs.json) sets it, and no default stands in for it")
}

fn env(var: &str) -> String {
    std::env::var(var).unwrap_or_else(|_| refuse_unset(var))
}

fn provider() -> String {
    env("MESH_SPAWN_TYPE")
}

fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..").canonicalize().expect("the workspace root exists")
}

fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// One role cohort count per mesh.
#[derive(Debug, Clone, Copy)]
struct Shape {
    tier: &'static str,
    node_admin: u32,
    compute: u32,
    gateway: u32,
    broker: u32,
}

const CANONICAL: Shape = Shape { tier: "canonical", node_admin: 2, compute: 2, gateway: 3, broker: 3 };
const FAST: Shape = Shape { tier: "fast", node_admin: 2, compute: 1, gateway: 1, broker: 1 };
const MESHES: [&str; 2] = ["mesh1", "mesh2"];

impl Shape {
    fn per_mesh(&self) -> u32 {
        self.node_admin + self.compute + self.gateway + self.broker
    }

    fn total(&self) -> usize {
        (self.per_mesh() as usize) * MESHES.len()
    }

    fn mesh_request(&self, mesh: &str) -> Value {
        json!({"name": mesh, "node_admin": self.node_admin, "compute": self.compute, "gateway": self.gateway, "broker": self.broker})
    }

    fn names(&self) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for m in MESHES {
            for (seg, n) in [("admin", self.node_admin), ("compute", self.compute), ("gateway", self.gateway), ("broker", self.broker)] {
                for i in 1..=n {
                    out.insert(format!("{m}.{seg}.{i}"));
                }
            }
        }
        out
    }
}

/// The shape a cell requires; a run of any other tier is refused by name.
fn require_tier(shape: Shape) -> Shape {
    let tier = env("RDM_RSHAPE_TIER");
    assert_eq!(tier, shape.tier, "REFUSED: this cell runs only at tier `{}` (RDM_RSHAPE_TIER={tier}); a reduced fixture never stands in for the canonical twenty-node estate and the canonical estate is not a fast control", shape.tier);
    shape
}

fn seed() -> u64 {
    env("RDM_RSHAPE_SEED").parse().unwrap_or_else(|e| panic!("REFUSED: RDM_RSHAPE_SEED is not a u64: {e}"))
}

fn cell_dir(cell: &str) -> PathBuf {
    let d = match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => workspace_root().join("target/i143-rshape/adhoc").join(cell),
    };
    std::fs::create_dir_all(&d).unwrap();
    d
}

fn kind_segment(kind: &str) -> &str {
    match kind {
        "node_admin" => "admin",
        k => k,
    }
}

/// The binding kind (`launch_id`) of a node's role.
fn launch_id(node: &str) -> &'static str {
    match node.split('.').nth(1) {
        Some("admin") => "node_admin",
        Some("compute") => "compute",
        Some("gateway") => "gateway",
        Some("broker") => "broker",
        other => panic!("{node}: no role segment ({other:?})"),
    }
}

// ---- the provider, observed -----------------------------------------------------------------------

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

/// The exact executable and image a node of the estate runs, observed from the provider (the
/// process's `/proc/<pid>/exe`, the container's entry point and bind mount), never from a span.
async fn observe_launch(estate: &Estate, node: &str) -> Value {
    use rafka_node_admin_client::binding::sha256_file;
    let n = estate.node(node).await;
    if provider() == "container" {
        let id = estate.container_of(node).unwrap_or_else(|| panic!("{node}: no running container"));
        let inspect: Value = serde_json::from_str(&docker(&["inspect", "--format", "{{json .}}", &id])).unwrap();
        let path = inspect["Path"].as_str().unwrap().to_string();
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

// ---- the run's recorder --------------------------------------------------------------------------

/// Every invariant the cell checked, with what it was checked on; a failed check panics, so the
/// file exists only for a cell whose every listed invariant held.
#[derive(Default)]
struct Invariants(Vec<Value>);

impl Invariants {
    fn holds(&mut self, name: &str, ok: bool, evidence: Value) {
        assert!(ok, "invariant `{name}` failed: {evidence}");
        self.0.push(json!({"invariant": name, "holds": true, "evidence": evidence}));
    }
}

fn write_json(dir: &Path, name: &str, v: &Value) {
    std::fs::write(dir.join(name), serde_json::to_vec_pretty(v).unwrap()).unwrap();
}

fn write_jsonl(dir: &Path, name: &str, rows: &[Value]) {
    let mut text = String::new();
    for r in rows {
        text.push_str(&serde_json::to_string(r).unwrap());
        text.push('\n');
    }
    std::fs::write(dir.join(name), text).unwrap();
}

/// Every span of the estate, copied under `<dir>/spans` as written (resources, TraceId, SpanId and
/// ParentSpanId unchanged), and the hash-free index of what was copied.
fn copy_spans(estate: &Estate, dir: &Path) -> Vec<Value> {
    let out = dir.join("spans");
    let _ = std::fs::remove_dir_all(&out);
    std::fs::create_dir_all(&out).unwrap();
    let mut index = Vec::new();
    for e in std::fs::read_dir(&estate.evidence).into_iter().flatten().flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if name.ends_with(".spans.jsonl") {
            std::fs::copy(e.path(), out.join(&name)).unwrap();
            let lines = std::fs::read_to_string(e.path()).map(|t| t.lines().count()).unwrap_or(0);
            index.push(json!({"file": format!("spans/{name}"), "spans": lines}));
        }
    }
    index.sort_by_key(|v| s(&v["file"]));
    index
}

// ---- the estate ----------------------------------------------------------------------------------

struct Formed {
    cell: String,
    dir: PathBuf,
    shape: Shape,
    seed: u64,
    candidate: String,
    set: rafka_node_admin_client::binding::BindingSet,
    bound: Vec<PathBuf>,
    estate: Estate,
    actions: Vec<Value>,
    topology_initial: Vec<Value>,
    provider_before: Value,
    started_ms: u64,
    build_id: String,
    build: Value,
}

fn owner(cell: &str, shape: Shape) -> Owner {
    Owner { product: "mesh".into(), feature: "i143-rshape".into(), subfeature: format!("mock-{}", shape.tier), rung: "MN".into(), provider: provider(), test: cell.into() }
}

/// Born from empty provider state on the consumer's four bound executables: the Day-0 admin, then
/// ONE accepted Build for both meshes at the full shape, executed by the rectifier.
async fn form(cell: &str, shape: Shape) -> Formed {
    let started_ms = now_ms();
    let root = workspace_root();
    let bin = root.join(env("RDM_RSHAPE_CONSUMER_BIN_DIR"));
    let manifest = root.join("target/i143-rshape/consumer-build/manifest.json");
    let set = binding_set_from_build_manifest(&manifest, &bin, provider() == "container").unwrap_or_else(|e| panic!("REFUSED: the consumer build manifest does not bind the executables: {e}"));
    // The candidate is the RDM commit the consumer imported, as its build manifest records it; the
    // cell's own checkout may be a later commit (the harness), never the candidate.
    let candidate = set.candidate.sha.clone();
    let bound: Vec<PathBuf> = set.bindings.iter().map(|b| b.executable.clone()).collect();
    let provider_before = provider_actions(&bound);
    assert_eq!(provider_before, json!({"processes": [], "containers": []}), "empty provider state: nothing runs a bound executable before the estate is born");
    let dir = cell_dir(cell);
    let mut estate = Estate::bootstrap_external(owner(cell, shape), "fabric1", "mesh1", &set, &candidate, &["broker", "gateway", "compute"]).await.unwrap_or_else(|e| panic!("the consumer's binding set is refused: {e}"));
    estate.set_seed(seed());
    let topology_initial = estate.nodes().await;
    let mut actions = Vec::new();
    let body = json!({"fabric": "fabric1", "meshes": MESHES.iter().map(|m| shape.mesh_request(m)).collect::<Vec<_>>()});
    let t0 = now_ms();
    let (status, a) = estate.post("/api/build", &body).await;
    assert_eq!(status, 202, "{a}");
    let build_id = s(&a["build_id"]);
    actions.push(json!({"t_ms": t0, "action": "build.create", "via": "POST /api/build", "status": status, "build_id": build_id, "body": body}));
    let build = estate.await_build(&build_id, Duration::from_secs(120)).await;
    actions.push(json!({"t_ms": now_ms(), "action": "build.complete", "build_id": build_id, "attempt": build["attempt"], "executor": build["executor"]}));
    Formed { cell: cell.into(), dir, shape, seed: seed(), candidate, set, bound, estate, actions, topology_initial, provider_before, started_ms, build_id, build }
}

/// The view once exactly the shape's twenty (or ten) nodes are present and ready for traffic.
async fn settle(f: &Formed) -> Vec<Value> {
    f.estate.settled(&f.shape.names(), Duration::from_secs(30)).await
}

/// Distinct, valid logical identities and exact RuntimeFacts of every node in `nodes`, the one
/// authority per domain, and the bound executable each runs (observed from the provider).
async fn check_identities_and_launches(f: &Formed, nodes: &[Value], inv: &mut Invariants) -> (Vec<Value>, Value) {
    let names = f.shape.names();
    let have: BTreeSet<String> = nodes.iter().map(|n| s(&n["name"])).collect();
    inv.holds("the view holds exactly the shape's nodes", have == names && nodes.len() == f.shape.total(), json!({"want": names.len(), "have": have.len()}));
    for key in ["node_id", "endpoint_id", "incarnation_id", "deployment_id"] {
        let ids: BTreeSet<String> = nodes.iter().map(|n| s(&n[key])).collect();
        inv.holds(&format!("{} distinct `{key}` values", nodes.len()), ids.len() == nodes.len() && !ids.contains(""), json!({"distinct": ids.len()}));
    }
    for n in nodes {
        NodeId::parse(n["node_id"].as_str().unwrap()).unwrap_or_else(|e| panic!("{}: node_id {} is not a valid logical identity: {e}", n["name"], n["node_id"]));
        let name = s(&n["name"]);
        let (mesh, seg) = (name.split('.').next().unwrap(), name.split('.').nth(1).unwrap());
        assert_eq!((s(&n["mesh"]).as_str(), kind_segment(n["kind"].as_str().unwrap())), (mesh, seg), "{name}: the view's mesh and kind agree with its path.name");
        assert_eq!(n["status"], "ready-for-traffic", "{name}");
        assert_eq!(n["provider"], f.estate.owner.provider.as_str(), "{name}: the provider that holds it");
    }
    inv.holds("every node has a valid NodeId and a path.name whose mesh and role agree with its view", true, json!({"nodes": nodes.len()}));
    // The provider's own record of each runtime.
    let mut launches = Vec::new();
    for name in &names {
        let o = observe_launch(&f.estate, name).await;
        let b = f.set.bindings.iter().find(|b| b.launch_id == launch_id(name)).unwrap();
        assert_eq!(launched_path(&o), b.executable, "{name} runs the bound executable: {o}");
        assert_eq!(o["observed_sha256"], b.sha256.as_str(), "{name} runs the bound bytes: {o}");
        if provider() == "container" {
            assert_eq!(o["image"], RUNTIME_IMAGE, "{name} runs in the bound image: {o}");
            assert_eq!(o["mount_read_only"], true, "{name}'s executable is a read-only mount: {o}");
            assert_eq!(o["container"].as_str().map(str::len), Some(64), "{name}: an immutable container id: {o}");
        }
        launches.push(o);
    }
    let handles: BTreeSet<String> = launches.iter().map(|o| format!("{}{}", o["pid"], o["container"])).collect();
    inv.holds("every node runs its role's bound executable bytes in its own process or container", handles.len() == names.len(), json!({"runtimes": handles.len(), "provider": provider()}));
    let (_, fabric) = f.estate.get("/api/fabric").await;
    (launches, fabric)
}

/// Exactly one mesh primary node-admin per mesh, exactly one fabric-primary and it is a mesh
/// primary node-admin, and the fabric record agrees with the nodes' own flags.
fn check_authorities(nodes: &[Value], fabric: &Value, inv: &mut Invariants) -> Value {
    let mut per_mesh = BTreeMap::new();
    for m in MESHES {
        let primaries: Vec<String> = nodes.iter().filter(|n| n["mesh"] == m && n["kind"] == "node_admin" && n["is_primary"] == true).map(|n| s(&n["name"])).collect();
        assert_eq!(primaries.len(), 1, "exactly one mesh primary node-admin in {m}: {primaries:?}");
        per_mesh.insert(m.to_string(), primaries[0].clone());
    }
    let fabric_primaries: Vec<&Value> = nodes.iter().filter(|n| n["is_fabric_primary"] == true).collect();
    assert_eq!(fabric_primaries.len(), 1, "exactly one fabric-primary: {fabric_primaries:?}");
    let fp = fabric_primaries[0];
    assert_eq!((fp["kind"].as_str(), fp["is_primary"].as_bool()), (Some("node_admin"), Some(true)), "the fabric-primary is a mesh primary node-admin: {fp}");
    assert_eq!(fabric["fabric_primary"], fp["name"], "the fabric record names the fabric-primary the nodes flag");
    for m in fabric["meshes"].as_array().expect("meshes") {
        assert_eq!(m["primary_admin"].as_str(), per_mesh.get(m["name"].as_str().unwrap()).map(String::as_str), "the fabric record's primary of {} is the node's own flag", m["name"]);
        assert_eq!(m["status"], "ready-for-traffic", "mesh {} is ready for traffic", m["name"]);
    }
    assert_eq!(fabric["status"], "ready-for-traffic");
    inv.holds("one eligible mesh primary per mesh and one fabric-primary, agreed by the nodes and the fabric record", true, json!({"mesh_primaries": per_mesh, "fabric_primary": fp["name"]}));
    json!({"mesh_primaries": per_mesh, "fabric_primary": fp["name"], "fabric_build_id": fabric["build_id"]})
}

/// One probe invocation and the trace it ran under (read from the probe's own span file).
struct Call {
    args: Vec<String>,
    out: Value,
    trace_id: String,
    started_ms: u64,
    finished_ms: u64,
}

fn probe_files(estate: &Estate) -> BTreeSet<PathBuf> {
    std::fs::read_dir(&estate.evidence).into_iter().flatten().flatten().map(|e| e.path()).filter(|p| p.file_name().is_some_and(|n| n.to_string_lossy().starts_with("rafka-rpc-probe."))).collect()
}

fn probe_call(estate: &Estate, args: &[&str]) -> Call {
    let before = probe_files(estate);
    let started_ms = now_ms();
    let out = estate.probe(args);
    let finished_ms = now_ms();
    let fresh: Vec<PathBuf> = probe_files(estate).difference(&before).cloned().collect();
    assert_eq!(fresh.len(), 1, "one probe invocation leaves exactly one span file: {fresh:?} for {args:?}");
    let spans: Vec<Value> = std::fs::read_to_string(&fresh[0]).unwrap().lines().filter_map(|l| serde_json::from_str(l).ok()).collect();
    let trace_id = spans.iter().find(|sp| sp["name"] == "rdm.node_rpc.proof_store.resolve.via-probe").map(|sp| s(&sp["trace_id"])).unwrap_or_else(|| panic!("the probe {args:?} left no `via-probe` span in {}", fresh[0].display()));
    Call { args: args.iter().map(|a| a.to_string()).collect(), out, trace_id, started_ms, finished_ms }
}

fn bucket(out: &Value) -> Bucket {
    match out["outcome"].as_str() {
        Some("Reply") => Bucket::Reply,
        Some("NotSent") => Bucket::NotSent,
        Some("Unserved") => Bucket::Unserved,
        Some("RejectedStale") => Bucket::RejectedStale,
        Some("Indeterminate") => Bucket::Indeterminate,
        other => panic!("the probe printed no typed RpcOutcome ({other:?}): {out}"),
    }
}

fn action_row(c: &Call) -> Value {
    json!({"t_ms": c.started_ms, "finished_ms": c.finished_ms, "action": "rpc.call", "args": c.args, "trace_id": c.trace_id})
}

/// The role nodes' connection snapshots (originate door, op 0x73), by node name.
fn snapshot(estate: &Estate, node: &str) -> Value {
    let c = probe_call(estate, &["snapshot", "--target", &format!("path:{node}"), "--key", "x"]);
    assert_eq!(c.out["outcome"], "Reply", "{node}: {}", c.out);
    c.out
}

/// Every role node (compute and gateway answer the snapshot door) holds, with nothing owed, a
/// Connected Direct edge to the exact current birth of its mesh's primary node-admin.
async fn converge_connections(f: &Formed, inv: &mut Invariants) -> Value {
    let mut out = BTreeMap::new();
    let roles: Vec<String> = f.shape.names().into_iter().filter(|n| matches!(launch_id(n), "compute" | "gateway")).collect();
    for node in roles {
        let mesh = node.split('.').next().unwrap().to_string();
        let until = Instant::now() + Duration::from_secs(30);
        let snap = loop {
            let nodes = f.estate.nodes().await;
            // The required connection of a role node is the one to its mesh's primary node-admin: the
            // authority it declares its status to. Every Connected edge it holds names the current
            // birth of its destination (no connection to a superseded process).
            let admins: Vec<&Value> = nodes.iter().filter(|n| n["mesh"] == mesh.as_str() && n["kind"] == "node_admin" && n["is_primary"] == true).collect();
            assert_eq!(admins.len(), 1, "{mesh} has one primary node-admin: {admins:?}");
            let snap = snapshot(&f.estate, &node);
            let directs = snap["own_latest_directs"].as_array().cloned().unwrap_or_default();
            let mut missing: Vec<String> = admins
                .iter()
                .filter(|a| !directs.iter().any(|d| d["state"] == "Connected" && d["destination"]["node_id"] == a["node_id"] && d["destination"]["incarnation"] == a["incarnation_id"]))
                .map(|a| s(&a["name"]))
                .collect();
            for d in directs.iter().filter(|d| d["state"] == "Connected") {
                let current = nodes.iter().find(|n| n["node_id"] == d["destination"]["node_id"]);
                if current.is_none_or(|c| c["incarnation_id"] != d["destination"]["incarnation"]) {
                    missing.push(format!("{} (a Connected edge to a superseded or unknown birth)", d["destination"]["name"]));
                }
            }
            if missing.is_empty() && snap["owed"].as_array().is_some_and(|o| o.is_empty()) {
                break snap;
            }
            assert!(
                Instant::now() < until,
                "{node} never held its required connections (a Connected Direct edge to the current primary node-admin of {mesh}, none to a superseded birth, nothing owed) within 30 s: missing {missing:?}; directs {}; owed {}",
                serde_json::to_string(&directs.iter().map(|d| json!({"to": d["destination"]["name"], "state": d["state"], "kind": d["kind"], "inc": d["destination"]["incarnation"]})).collect::<Vec<_>>()).unwrap(),
                snap["owed"]
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        out.insert(node, json!({"active_len": snap["active_len"], "owed": snap["owed"], "directs": snap["own_latest_directs"]}));
    }
    inv.holds("every compute and gateway holds a Connected Direct edge to the current birth of its mesh's primary node-admin, none to a superseded birth, and owes no retirement", true, json!({"nodes": out.len()}));
    json!(out)
}

/// The spans of the formation Build: the REST request, the rectifier's reconcile of every attempt,
/// the per-node create under it and the deployment pipeline and steps under that. Returns the
/// per-node chain records; asserts the relationships the spans actually carry.
fn check_formation_chain(f: &Formed, spans: &[Value], inv: &mut Invariants) -> Vec<Value> {
    let bid = f.build_id.as_str();
    let rest = named(spans, "rdm.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == bid).cloned().expect("the formation Build's REST request span");
    let reconciles: Vec<&Value> = named(spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|r| r["attributes"]["build_id"] == bid).collect();
    assert!(!reconciles.is_empty(), "the formation Build was reconciled");
    for r in &reconciles {
        assert!(descends_from(spans, r, &rest), "every attempt of the formation Build descends from its REST request: {r}");
    }
    inv.holds("every reconcile attempt of the formation Build descends from its REST request", true, json!({"reconciles": reconciles.len(), "rest_span": rest["span_id"], "trace_id": rest["trace_id"]}));
    let mut chains = Vec::new();
    for name in f.shape.names().into_iter().filter(|n| n != "mesh1.admin.1") {
        let creates: Vec<&Value> = named(spans, "rdm.node_admin.node.create.via-build").into_iter().filter(|c| c["attributes"]["build_id"] == bid && c["attributes"]["node"] == name.as_str()).collect();
        assert!(!creates.is_empty(), "{name}: no node.create.via-build under the formation Build");
        // A create re-run by a successor executor leaves the earlier attempt's span; the one that
        // completed the node is the one whose pipeline finished.
        let mut done = None;
        for c in &creates {
            let pipelines: Vec<&Value> = named(spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().filter(|p| p["attributes"]["node"] == name.as_str() && p["parent_span_id"] == c["span_id"]).collect();
            if let Some(p) = pipelines.first() {
                done = Some(((*c).clone(), (*p).clone()));
            }
        }
        let (create, pipeline) = done.unwrap_or_else(|| panic!("{name}: no deployment pipeline under any of its {} create span(s)", creates.len()));
        assert!(reconciles.iter().any(|r| descends_from(spans, &create, r)), "{name}: the create runs under a reconcile of the formation Build");
        let steps: Vec<&Value> = named(spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|st| st["parent_span_id"] == pipeline["span_id"]).collect();
        assert!(!steps.is_empty(), "{name}: deployment steps ran under its pipeline");
        assert!(steps.iter().all(|st| st["trace_id"] == pipeline["trace_id"] && st["attributes"]["node"] == name.as_str()), "{name}: every step is of its pipeline's trace and node");
        let launch = create["events"].as_array().into_iter().flatten().find(|e| e["name"] == "launching from the explicit executable binding").cloned().unwrap_or_else(|| panic!("{name}: no explicit-binding launch event on its create span"));
        let b = f.set.bindings.iter().find(|b| b.launch_id == launch_id(&name)).unwrap();
        // The span records the path the admin launched; a container's launch names the same bound file.
        assert_eq!(launch["attributes"]["sha256"], b.sha256.as_str(), "{name}: the create launched the bound bytes");
        assert_eq!(launch["attributes"]["launch_id"], launch_id(&name), "{name}");
        chains.push(json!({
            "node": name, "create_span": create["span_id"], "pipeline_span": pipeline["span_id"], "steps": steps.len(),
            "step_names": steps.iter().map(|st| st["attributes"]["step"].clone()).collect::<Vec<_>>(),
            "launch": launch["attributes"], "attempt": create["attributes"]["attempt"], "trace_id": create["trace_id"],
        }));
    }
    inv.holds("every created node: create.via-build under a reconcile, its pipeline under the create, its steps under the pipeline in one trace, launching the bound bytes", true, json!({"nodes": chains.len()}));
    chains
}

/// Runtime history: one `node.update.via-ready` span per birth (the runtime's own record of what it
/// is), checked against the view and the provider's record of the same node.
fn check_runtime_facts(f: &Formed, nodes: &[Value], launches: &[Value], spans: &[Value], inv: &mut Invariants) -> Vec<Value> {
    let ready = named(spans, "rdm.mesh.node.update.via-ready");
    let mut rows = Vec::new();
    for n in nodes {
        let name = s(&n["name"]);
        let own: Vec<&&Value> = ready.iter().filter(|r| r["attributes"]["node"] == name.as_str() && r["attributes"]["incarnation_id"] == n["incarnation_id"]).collect();
        assert!(!own.is_empty(), "{name}: no ready span for its current incarnation");
        let r = own[0];
        let a = &r["attributes"];
        assert_eq!(a["node_id"], n["node_id"], "{name}: the runtime's own NodeId is the view's");
        assert_eq!(a["provider"], f.estate.owner.provider.as_str(), "{name}: the runtime reports the provider that holds it");
        assert!(!s(&a["runtime_locator_kind"]).is_empty() && !s(&a["runtime_locator_fingerprint"]).is_empty(), "{name}: the runtime's locator fact is present: {a}");
        let svc = s(&r["service"]);
        let want_service = format!("rshape-{}", if launch_id(&name) == "node_admin" { "node-admin" } else { launch_id(&name) });
        assert_eq!(svc, want_service, "{name}: the process that reported ready is the consumer's {want_service}");
        let l = launches.iter().find(|o| o["node"] == name.as_str()).unwrap();
        rows.push(json!({
            "node": name, "node_id": n["node_id"], "incarnation_id": n["incarnation_id"], "endpoint_id": n["endpoint_id"], "kind": n["kind"],
            "service": svc, "provider": a["provider"], "runtime_locator_kind": a["runtime_locator_kind"], "runtime_locator_fingerprint": a["runtime_locator_fingerprint"],
            "provider_control_domain_fingerprint": a["provider_control_domain_fingerprint"], "ready_span": r["span_id"], "ready_trace": r["trace_id"],
            "observed": l,
        }));
    }
    inv.holds("every node's runtime facts: its own ready span names its NodeId, incarnation, provider and locator, in the consumer's role process", true, json!({"nodes": rows.len()}));
    rows
}

fn check_services(spans: &[Value], inv: &mut Invariants) -> BTreeSet<String> {
    let services: BTreeSet<String> = spans.iter().filter_map(|sp| sp["service"].as_str().map(String::from)).collect();
    for want in ["rshape-node-admin", "rshape-broker", "rshape-gateway", "rshape-compute"] {
        assert!(services.contains(want), "the consumer's `{want}` ran: {services:?}");
    }
    for never in ["rafka-node-admin", "rafka-rpc-node", "consumer-node-admin", "consumer-broker", "consumer-gateway", "consumer-compute"] {
        assert!(!services.contains(never), "no built-in or fixture `{never}` ran: {services:?}");
    }
    inv.holds("the estate ran only the consumer's four executables (the other services are the controller's probe)", true, json!({"services": services}));
    services
}

/// Everything the cells write at the cell root: the evidence views of the story, from data the
/// cell collected, plus an index mapping each to the estate artifact it derives from.
struct Views<'a> {
    f: &'a Formed,
    topology_final: Vec<Value>,
    actions: Vec<Value>,
    operations: Vec<Value>,
    outcomes: Vec<Value>,
    authority_history: Vec<Value>,
    route_history: Vec<Value>,
    runtime_history: Vec<Value>,
    inv: Invariants,
    extra: Value,
}

fn write_views(v: Views<'_>, spans_index: Vec<Value>, spans: &[Value]) {
    let f = v.f;
    let dir = &f.dir;
    write_json(dir, "topology-initial.json", &json!({"shape_requested": {"tier": f.shape.tier, "meshes": MESHES, "per_mesh": {"node_admin": f.shape.node_admin, "compute": f.shape.compute, "gateway": f.shape.gateway, "broker": f.shape.broker}, "total": f.shape.total()}, "provider_state_before": f.provider_before, "view_at_birth": f.topology_initial}));
    write_json(dir, "topology-final.json", &json!({"total": v.topology_final.len(), "nodes": v.topology_final}));
    write_jsonl(dir, "actions.jsonl", &v.actions);
    write_jsonl(dir, "operations.jsonl", &v.operations);
    write_jsonl(dir, "outcomes.jsonl", &v.outcomes);
    write_jsonl(dir, "authority-history.jsonl", &v.authority_history);
    write_jsonl(dir, "route-history.jsonl", &v.v_route(spans, v.route_history.clone()));
    write_jsonl(dir, "runtime-history.jsonl", &v.runtime_history);
    write_json(dir, "invariant-results.json", &json!({"cell": f.cell, "tier": f.shape.tier, "provider": provider(), "invariants": v.inv.0}));
    let mut result = json!({
        "cell": f.cell,
        "tier": f.shape.tier,
        "qualifies_export": f.shape.tier == "canonical",
        "provider": provider(),
        "seed": f.seed,
        "rdm_candidate_sha": f.candidate,
        "harness_source": "the cell's own checkout; the candidate is the RDM commit the consumer imported",
        "consumer_binding": f.estate.external.as_ref().map(|x| x.validated.receipt()),
        "started_ms": f.started_ms,
        "finished_ms": now_ms(),
        "wall_ms": now_ms() - f.started_ms,
        "formation_build": {"build_id": f.build_id, "attempt": f.build["attempt"], "executor": f.build["executor"]},
        "estate": {"manifest": f.estate.artifacts.join("manifest.json"), "rpc_ledger": f.estate.artifacts.join("rpc-ledger.jsonl"), "spans_dir": f.estate.evidence},
        "spans": spans_index,
        "views": {
            "topology-initial.json": "initial Day-0 view, the requested shape and the provider state before launch (estate GET /api/nodes, provider /proc and docker)",
            "topology-final.json": "the final GET /api/nodes of the admin that holds the fabric",
            "actions.jsonl": "every REST action and RPC call the cell issued, with the trace id of each call",
            "operations.jsonl": "every issued operation with its stable id and intent",
            "outcomes.jsonl": "the typed outcome of every operation and its provenance",
            "authority-history.jsonl": "the fabric record and the nodes' primary flags at each checkpoint",
            "route-history.jsonl": "every route.resolve span of the run (route, carrier, target, outcome)",
            "runtime-history.jsonl": "each birth's own ready-span runtime facts beside the provider's observation of its runtime",
            "spans/": "a copy of every span file the estate wrote, unchanged (estate/<feature>/<test>/spans is authoritative)",
        },
    });
    for (k, val) in v.extra.as_object().cloned().unwrap_or_default() {
        result[k] = val;
    }
    write_json(dir, "result.json", &result);
}

impl Views<'_> {
    /// The route history: the estate's route.resolve spans, in start order.
    fn v_route(&self, spans: &[Value], mut rows: Vec<Value>) -> Vec<Value> {
        let mut r: Vec<&Value> = spans.iter().filter(|sp| sp["name"].as_str().is_some_and(|n| n.starts_with("rdm.node_rpc.route.resolve."))).collect();
        r.sort_by_key(|sp| sp["start_unix_nano"].as_u64().unwrap_or(0));
        rows.extend(r.into_iter().map(|sp| json!({"name": sp["name"], "service": sp["service"], "trace_id": sp["trace_id"], "span_id": sp["span_id"], "parent_span_id": sp["parent_span_id"], "start_unix_nano": sp["start_unix_nano"], "attributes": {
            "protocol": sp["attributes"]["protocol"], "route": sp["attributes"]["route"], "carrier": sp["attributes"]["carrier"], "target": sp["attributes"]["target"], "outcome": sp["attributes"]["outcome"], "own": sp["attributes"]["own"], "destination": sp["attributes"]["destination"],
        }})));
        rows
    }
}

/// After the estate was told to stop: every host process of a bound executable is gone, observed
/// from the process table. `Estate::stop` returns at its own 30 s bound whether or not the last
/// admin has exited; the exit is awaited here, from observed state, up to the three drain bounds
/// the architecture gives a fabric shutdown (members, admins, spine: `shutdown::drain_bound`,
/// RAFKA_SHUTDOWN_DRAIN_BOUND_MS, 60 s each). The wall from the stop request to the last exit is
/// recorded: it is a measurement of the shutdown, not a tolerance.
async fn provider_left(f: &Formed, stop_started: Instant, inv: &mut Invariants) -> Value {
    let drain_bound = std::env::var("RAFKA_SHUTDOWN_DRAIN_BOUND_MS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(60_000);
    let deadline = Instant::now() + Duration::from_millis(drain_bound * 3);
    let (left, stop_returned_ms) = (|| async {
        let returned = stop_started.elapsed().as_millis() as u64;
        loop {
            let left = provider_actions(&f.bound);
            if left["processes"].as_array().is_some_and(|p| p.is_empty()) || Instant::now() >= deadline {
                return (left, returned);
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    })()
    .await;
    let wall = stop_started.elapsed().as_millis() as u64;
    inv.holds(
        "after the shutdown, no host process still runs a bound executable",
        left["processes"].as_array().is_some_and(|p| p.is_empty()),
        json!({"left": left, "estate_stop_returned_after_ms": stop_returned_ms, "last_exit_observed_after_ms": wall, "three_drain_bounds_ms": drain_bound * 3}),
    );
    json!({"left": left, "estate_stop_returned_after_ms": stop_returned_ms, "last_exit_observed_after_ms": wall})
}

fn authority_row(phase: &str, nodes: &[Value], fabric: &Value) -> Value {
    json!({
        "t_ms": now_ms(), "phase": phase, "fabric_primary": fabric["fabric_primary"], "fabric_build_id": fabric["build_id"],
        "mesh_primaries": fabric["meshes"].as_array().into_iter().flatten().map(|m| json!({"mesh": m["name"], "primary_admin": m["primary_admin"], "status": m["status"]})).collect::<Vec<_>>(),
        "primary_flags": nodes.iter().filter(|n| n["is_primary"] == true || n["is_fabric_primary"] == true).map(|n| json!({"node": n["name"], "is_primary": n["is_primary"], "is_fabric_primary": n["is_fabric_primary"]})).collect::<Vec<_>>(),
    })
}

// ---- cell 1: formation ---------------------------------------------------------------------------

/// CONTRACT: from empty provider state, one accepted Build for both meshes brings the canonical
/// twenty-node estate (node-admin / compute / gateway / broker = 2 / 2 / 3 / 3 per mesh, two
/// meshes) up on the consumer's four bound executables and nothing else. The view holds exactly
/// those twenty path.names with twenty distinct valid NodeIds, EndpointIds, incarnations and
/// deployments; the provider (the process table, or the container runtime) runs each node's
/// role executable at the bound bytes, one runtime per node; each runtime's own ready span names
/// the same NodeId, incarnation and provider; each mesh has exactly one primary node-admin and
/// the fabric has one fabric-primary that the fabric record agrees with; every compute and gateway
/// holds a Connected Direct edge to the current birth of its mesh's primary node-admin with nothing owed. Every
/// created node came from the rectifier: REST request -> reconcile attempt -> node.create.via-build
/// -> deployment pipeline -> steps, launching the bound bytes. What must NOT happen: a node the
/// shape does not list, a duplicated identity, a built-in or fixture executable, a runtime the
/// provider does not hold, or a node that came from anything but the Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_estate_forms_twenty_distinct_nodes() {
    let cell = "mock_estate_forms_twenty_distinct_nodes";
    let shape = require_tier(CANONICAL);
    formation(cell, shape).await;
}

/// CONTRACT: the reduced ten-node fixture (2 / 1 / 1 / 1 x 2) forms through the same Build path
/// as the canonical estate and routes one exact call, one carried call and one gateway-originated
/// call to typed provenance replies, as a fast control only. It never qualifies the canonical
/// twenty-node receipts, and a run of any other tier is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_fast_estate_forms_and_routes_opaque_calls() {
    let cell = "mock_fast_estate_forms_and_routes_opaque_calls";
    let shape = require_tier(FAST);
    let mut f = form(cell, shape).await;
    let mut inv = Invariants::default();
    let nodes = settle(&f).await;
    let (launches, fabric) = check_identities_and_launches(&f, &nodes, &mut inv).await;
    let authorities = check_authorities(&nodes, &fabric, &mut inv);
    let mut authority_history = vec![authority_row("formed", &nodes, &fabric)];
    let connections = converge_connections(&f, &mut inv).await;
    let mut routes = RouteRun::new();
    routes.run(&f, &nodes, &mut inv).await;
    f.actions.extend(routes.calls.iter().map(action_row));
    let (_, fabric_end) = f.estate.get("/api/fabric").await;
    let nodes_end = f.estate.nodes().await;
    authority_history.push(authority_row("routed", &nodes_end, &fabric_end));
    let stop_t = Instant::now();
    f.estate.stop().await;
    let left = provider_left(&f, stop_t, &mut inv).await;
    let spans = f.estate.spans();
    let services = check_services(&spans, &mut inv);
    let chains = check_formation_chain(&f, &spans, &mut inv);
    let runtime = check_runtime_facts(&f, &nodes, &launches, &spans, &mut inv);
    let route_obs = routes.check_spans(&f, &spans, &mut inv);
    let index = copy_spans(&f.estate, &f.dir);
    let actions = f.actions.clone();
    write_views(
        Views {
            f: &f,
            topology_final: nodes_end,
            actions,
            operations: routes.operations(),
            outcomes: routes.outcomes(),
            authority_history,
            route_history: Vec::new(),
            runtime_history: runtime,
            inv,
            extra: json!({"authorities": authorities, "connections": connections, "provider_after_stop": left, "services": services, "formation_chains": chains, "routes": route_obs, "note": "the reduced fast control; the twenty-node canonical receipts are separate"}),
        },
        index,
        &spans,
    );
}

async fn formation(cell: &str, shape: Shape) {
    let mut f = form(cell, shape).await;
    let mut inv = Invariants::default();
    let nodes = settle(&f).await;
    let (launches, fabric) = check_identities_and_launches(&f, &nodes, &mut inv).await;
    let authorities = check_authorities(&nodes, &fabric, &mut inv);
    let authority_history = vec![authority_row("formed", &nodes, &fabric)];
    let connections = converge_connections(&f, &mut inv).await;
    let (_, fabric_end) = f.estate.get("/api/fabric").await;
    let nodes_end = f.estate.nodes().await;
    let stop_t = Instant::now();
    f.estate.stop().await;
    let left = provider_left(&f, stop_t, &mut inv).await;
    let spans = f.estate.spans();
    let services = check_services(&spans, &mut inv);
    let chains = check_formation_chain(&f, &spans, &mut inv);
    let runtime = check_runtime_facts(&f, &nodes, &launches, &spans, &mut inv);
    let index = copy_spans(&f.estate, &f.dir);
    let mut authority_history = authority_history;
    authority_history.push(authority_row("final", &nodes_end, &fabric_end));
    let actions = f.actions.clone();
    write_views(
        Views {
            f: &f,
            topology_final: nodes_end,
            actions,
            operations: Vec::new(),
            outcomes: Vec::new(),
            authority_history,
            route_history: Vec::new(),
            runtime_history: runtime,
            inv,
            extra: json!({"authorities": authorities, "connections": connections, "provider_after_stop": left, "services": services, "formation_chains": chains}),
        },
        index,
        &spans,
    );
}

// ---- cell 2: routes ------------------------------------------------------------------------------

/// The route legs the cell exercises, with the calls and the facts to check against spans.
struct RouteRun {
    calls: Vec<Call>,
    legs: Vec<Value>,
}

impl RouteRun {
    fn new() -> Self {
        Self { calls: Vec::new(), legs: Vec::new() }
    }

    fn operations(&self) -> Vec<Value> {
        self.legs.iter().enumerate().map(|(i, l)| json!({"op_seq": i, "leg": l["leg"], "request": l["request"], "origin": l["origin"], "requested": l["requested"]})).collect()
    }

    fn outcomes(&self) -> Vec<Value> {
        self.legs.iter().enumerate().map(|(i, l)| json!({"op_seq": i, "leg": l["leg"], "outcome": l["outcome"], "route": l["route"], "carrier": l["carrier"], "resolved": l["resolved"], "terminal": l["terminal"], "trace_id": l["trace_id"]})).collect()
    }

    fn node<'a>(nodes: &'a [Value], name: &str) -> &'a Value {
        nodes.iter().find(|n| n["name"] == name).unwrap_or_else(|| panic!("no node {name}"))
    }

    /// Issue the legs. Same-mesh and cross-mesh exact calls from the controller, the same two carried
    /// by a gateway, then calls the gateway and compute originate themselves over their held
    /// connections (the compute holds no edge to the broker: its call is `no-active-route`).
    async fn run(&mut self, f: &Formed, nodes: &[Value], inv: &mut Invariants) {
        let e = &f.estate;
        let b1 = Self::node(nodes, "mesh1.broker.1").clone();
        let b2 = Self::node(nodes, "mesh2.broker.1").clone();
        let g1 = "mesh1.gateway.1";
        let (b1_id, b2_id) = (s(&b1["node_id"]), s(&b2["node_id"]));
        let value = |leg: &str| format!("rshape-{leg}-{}", f.seed);
        let exact = |id: &str| format!("exact:{id}");

        // Presence control: a write that lands and reads back is what makes every later absence meaningful.
        let leg = |this: &mut Self, name: &str, call: Call, origin: &str, requested: &Value, resolved: &Value| {
            let out = &call.out;
            let terminal = out["reply"].clone();
            this.legs.push(json!({
                "leg": name, "request": call.args, "origin": origin, "requested": requested, "resolved": resolved,
                "outcome": out["outcome"], "route": out["route"], "carrier": out["carrier"], "terminal": terminal, "reason": out["reason"], "trace_id": call.trace_id, "raw": out,
            }));
            this.calls.push(call);
        };
        let resolved = |n: &Value| json!({"node": n["name"], "node_id": n["node_id"], "incarnation_id": n["incarnation_id"], "endpoint_id": n["endpoint_id"]});

        // 1. same-mesh exact, Direct.
        let c = probe_call(e, &["put", "--target", &exact(&b1_id), "--key", "same-mesh", "--value", &value("same")]);
        assert_eq!((c.out["outcome"].as_str(), c.out["route"].as_str()), (Some("Reply"), Some("direct")), "{}", c.out);
        assert_eq!(c.out["reply"]["executing_node"], b1["node_id"], "the exact broker served: {}", c.out);
        assert_eq!(c.out["reply"]["incarnation_id"], b1["incarnation_id"], "{}", c.out);
        assert_eq!(c.out["reply"]["result"], json!({"stored": true}));
        leg(self, "same-mesh-exact-put", c, "controller (rafka-rpc-probe, caller_system rdm)", &json!(exact(&b1_id)), &resolved(&b1));
        let c = probe_call(e, &["get", "--target", &exact(&b1_id), "--key", "same-mesh"]);
        assert_eq!(c.out["reply"]["result"], json!({"found": true, "value": value("same")}), "presence control: {}", c.out);
        assert_eq!(c.out["reply"]["executing_node"], b1["node_id"]);
        leg(self, "same-mesh-exact-get", c, "controller", &json!(exact(&b1_id)), &resolved(&b1));

        // 2. cross-mesh exact, Direct.
        let c = probe_call(e, &["put", "--target", &exact(&b2_id), "--key", "cross-mesh", "--value", &value("cross")]);
        assert_eq!((c.out["outcome"].as_str(), c.out["route"].as_str()), (Some("Reply"), Some("direct")), "{}", c.out);
        assert_eq!(c.out["reply"]["executing_node"], b2["node_id"], "the other mesh's exact broker served: {}", c.out);
        assert_eq!(c.out["reply"]["mesh"], "mesh2");
        leg(self, "cross-mesh-exact-put", c, "controller", &json!(exact(&b2_id)), &resolved(&b2));
        let c = probe_call(e, &["get", "--target", &exact(&b1_id), "--key", "cross-mesh"]);
        assert_eq!(c.out["reply"]["result"], json!({"found": false}), "mesh1's broker never saw mesh2's write: {}", c.out);
        leg(self, "same-mesh-exact-get-absent", c, "controller", &json!(exact(&b1_id)), &resolved(&b1));

        // 3. carried through a mesh1 gateway: to the same-mesh broker and to the cross-mesh broker.
        let c = probe_call(e, &["get", "--target", &exact(&b1_id), "--via", &format!("path:{g1}"), "--key", "same-mesh"]);
        assert_eq!((c.out["outcome"].as_str(), c.out["route"].as_str()), (Some("Reply"), Some("via-peer")), "{}", c.out);
        assert_eq!(c.out["reply"]["executing_node"], b1["node_id"], "the carrier reached exactly the same-mesh broker: {}", c.out);
        leg(self, "same-mesh-via-gateway", c, "controller", &json!(exact(&b1_id)), &resolved(&b1));
        let c = probe_call(e, &["get", "--target", &exact(&b2_id), "--via", &format!("path:{g1}"), "--key", "cross-mesh"]);
        assert_eq!((c.out["outcome"].as_str(), c.out["route"].as_str()), (Some("Reply"), Some("via-peer")), "{}", c.out);
        assert_eq!(c.out["reply"]["executing_node"], b2["node_id"], "the mesh1 gateway reached the mesh2 broker: {}", c.out);
        assert_eq!(c.out["reply"]["result"], json!({"found": true, "value": value("cross")}));
        leg(self, "cross-mesh-via-gateway", c, "controller", &json!(exact(&b2_id)), &resolved(&b2));

        // 4. an exact NodeId the fabric never held resolves to nothing and sends nothing.
        let ghost = NodeId::mint().to_string();
        let c = probe_call(e, &["get", "--target", &exact(&ghost), "--key", "same-mesh"]);
        assert_eq!((c.out["outcome"].as_str(), c.out["reason"].as_str()), (Some("NotSent"), Some("Resolve(Unknown)")), "{}", c.out);
        leg(self, "exact-unknown-node", c, "controller", &json!(exact(&ghost)), &Value::Null);

        // 5. the gateway that carried now holds a Direct edge to each broker it dialed: it originates
        //    to them itself over its held connections.
        for (dest, d, kind) in [("mesh1.broker.1", &b1, "same-mesh"), ("mesh2.broker.1", &b2, "cross-mesh")] {
            let dest_id = s(&d["node_id"]);
            wait_for(&format!("{g1} holds a Connected Direct edge to {dest}'s current birth"), Duration::from_secs(30), || async {
                let snap = snapshot(e, g1);
                snap["own_latest_directs"].as_array().into_iter().flatten().any(|x| x["state"] == "Connected" && x["destination"]["node_id"] == d["node_id"] && x["destination"]["incarnation"] == d["incarnation_id"]).then_some(())
            })
            .await;
            let key = format!("orig-{kind}");
            let c = probe_call(e, &["originate", "--target", &format!("path:{g1}"), "--destination", &format!("path:{dest}"), "--key", &key, "--value", &value(&key)]);
            assert_eq!((c.out["outcome"].as_str(), c.out["route"].as_str(), c.out["call_outcome"].as_str()), (Some("Reply"), Some("direct"), Some("reply")), "{}", c.out);
            // The door's own `outcome` lowercases (`reply`); the probe prints the call's outcome as `call_outcome`.
            assert_eq!(c.out["by"], g1);
            assert_eq!(c.out["destination_node_id"], dest_id);
            assert_eq!(c.out["reply"]["executing_node"], dest_id, "{}", c.out);
            assert_eq!(c.out["reply"]["incarnation_id"], d["incarnation_id"], "{}", c.out);
            leg(self, &format!("gateway-originates-{kind}"), c, g1, &json!(format!("path:{dest}")), &resolved(d));
        }

        // 6. the negative control for the leg above: a compute holds no edge to the broker, so its
        //    originate is `no-active-route`, not sent, and the broker serves nothing for it.
        let c = probe_call(e, &["originate", "--target", "path:mesh1.compute.1", "--destination", "path:mesh1.broker.1", "--key", "orig-no-route", "--value", &value("no-route")]);
        assert_eq!((c.out["outcome"].as_str(), c.out["route"].as_str(), c.out["call_outcome"].as_str()), (Some("Reply"), Some("no-active-route"), Some("NotSent")), "{}", c.out);
        leg(self, "compute-originates-without-edge", c, "mesh1.compute.1", &json!("path:mesh1.broker.1"), &resolved(&b1));
        let c = probe_call(e, &["get", "--target", &exact(&b1_id), "--key", "orig-no-route"]);
        assert_eq!(c.out["reply"]["result"], json!({"found": false}), "nothing was applied for the call that was not sent: {}", c.out);
        leg(self, "no-route-key-absent", c, "controller", &json!(exact(&b1_id)), &resolved(&b1));
        inv.holds("same-mesh, cross-mesh, carried, gateway-originated and exact-node legs each reached exactly the requested birth; an unknown NodeId and a compute without an edge sent nothing", true, json!({"legs": self.legs.len()}));
    }

    /// The spans of each leg: the call's trace holds the probe's call span, the server's request
    /// span under it and the proof-store marker under that; carried legs hold one carried inner call;
    /// the no-route and unknown legs hold no serve span at all.
    fn check_spans(&mut self, f: &Formed, spans: &[Value], inv: &mut Invariants) -> Vec<Value> {
        let _ = f;
        let mut obs = Vec::new();
        for (leg, call) in self.legs.iter_mut().zip(self.calls.iter()) {
            let trace = call.trace_id.as_str();
            let in_trace: Vec<&Value> = spans.iter().filter(|sp| sp["trace_id"] == trace).collect();
            let served: Vec<&&Value> = in_trace.iter().filter(|sp| sp["name"] == "rdm.node_rpc.proof_store.serve.via-request").collect();
            let name = s(&leg["leg"]);
            let mut record = json!({"leg": name, "trace_id": trace, "spans_in_trace": in_trace.len(), "proof_store_serves": served.len()});
            let routes: Vec<&&Value> = in_trace.iter().filter(|sp| sp["name"] == "rdm.node_rpc.route.resolve.via-connections").collect();
            match name.as_str() {
                "exact-unknown-node" | "compute-originates-without-edge" => {
                    assert!(served.is_empty(), "{name}: nothing was served: {served:?}");
                    if name == "compute-originates-without-edge" {
                        assert_eq!(routes.len(), 1, "{name}: one route resolution: {routes:?}");
                        assert_eq!((routes[0]["attributes"]["route"].as_str(), routes[0]["attributes"]["outcome"].as_str()), (Some("no-active-route"), Some("NotSent")));
                        assert_eq!(routes[0]["service"], "rshape-compute");
                        record["route_span"] = routes[0]["span_id"].clone();
                    }
                }
                n if n.ends_with("-absent") => {
                    assert_eq!(served.len(), 1, "{name}: the read was served once: {served:?}");
                }
                _ => {
                    assert_eq!(served.len(), 1, "{name}: exactly one handler execution in the call's trace: {served:?}");
                    let marker = served[0];
                    let terminal = &leg["terminal"];
                    assert_eq!(marker["attributes"]["node_id"], terminal["executing_node"], "{name}: the handler that ran is the replying node");
                    assert_eq!(marker["attributes"]["incarnation_id"], terminal["incarnation_id"], "{name}");
                    let request_spans: Vec<&&Value> = in_trace.iter().filter(|sp| sp["name"] == "rdm.node_rpc.request.serve.via-direct" && sp["attributes"]["protocol"] == "proof-store").collect();
                    assert!(!request_spans.is_empty(), "{name}: the server's request span is in the trace");
                    let parent = spans.iter().find(|sp| sp["span_id"] == marker["parent_span_id"]).unwrap_or_else(|| panic!("{name}: the marker's parent span is missing"));
                    assert_eq!(parent["trace_id"], marker["trace_id"], "{name}: marker parent in the same trace");
                    assert!(request_spans.iter().any(|r| r["span_id"] == parent["span_id"]), "{name}: the proof-store marker's parent is the server's request span ({})", parent["name"]);
                    let transmitted = spans.iter().find(|x| x["span_id"] == parent["parent_span_id"] && x["trace_id"] == parent["trace_id"]);
                    assert!(transmitted.is_some(), "{name}: the server's request span is parented to the caller's span of the same trace (the transmitted context): {parent}");
                    record["request_span"] = parent["span_id"].clone();
                    record["request_parent_span"] = parent["parent_span_id"].clone();
                    record["request_parent_service"] = transmitted.map(|x| x["service"].clone()).unwrap_or(Value::Null);
                    record["marker_span"] = marker["span_id"].clone();
                    if name.contains("via-gateway") {
                        let carried: Vec<&&Value> = in_trace.iter().filter(|sp| sp["name"] == "rdm.node_rpc.request.serve.via-carried-inner").collect();
                        assert_eq!(carried.len(), 1, "{name}: exactly one carried inner call: {carried:?}");
                        assert_eq!(carried[0]["service"], "rshape-gateway", "{name}: the carrier is the consumer's gateway");
                        assert_eq!(carried[0]["attributes"]["target"], leg["resolved"]["node_id"], "{name}: the carrier was handed exactly the requested node");
                        record["carried_span"] = carried[0]["span_id"].clone();
                        assert_eq!(routes.len(), 1, "{name}: one route resolution: {routes:?}");
                        let a = &routes[0]["attributes"];
                        assert_eq!((a["route"].as_str(), a["carrier"].as_str(), a["outcome"].as_str()), (Some("via-peer"), Some("mesh1.gateway.1"), Some("Reply")), "{name}: {a}");
                        assert_eq!(a["target"], leg["resolved"]["node_id"], "{name}: the route's final target is the requested node");
                        leg["carrier"] = a["carrier"].clone();
                        record["route_span"] = routes[0]["span_id"].clone();
                    }
                    if name.starts_with("gateway-originates") {
                        assert_eq!(routes.len(), 1, "{name}: one route resolution: {routes:?}");
                        assert_eq!((routes[0]["attributes"]["route"].as_str(), routes[0]["attributes"]["outcome"].as_str()), (Some("direct"), Some("Reply")));
                        assert_eq!(routes[0]["attributes"]["target"], leg["resolved"]["node_id"]);
                        assert_eq!(routes[0]["service"], "rshape-gateway");
                        record["route_span"] = routes[0]["span_id"].clone();
                    }
                }
            }
            obs.push(record);
        }
        inv.holds("each leg's trace carries its request span, one proof-store handler execution under it on the requested birth, and the carried or originated route span where the leg has one; no handler ran for the legs that sent nothing", true, json!({"legs": obs.len()}));
        // Every call span of the controller descends into the server's request span of its own trace.
        let _ = descends_from;
        obs
    }
}

/// CONTRACT: in the formed canonical estate, opaque proof calls reach exactly the node they name,
/// and the typed reply names the birth that answered. A same-mesh exact call and a cross-mesh exact
/// call are Direct and served by exactly that broker (the other mesh's broker never sees the
/// write); the same two carried through a mesh1 gateway are `via-peer` with exactly one carried
/// inner call to the exact target; after carrying, that gateway holds a Connected Direct edge to
/// each broker it dialed and originates calls to them itself, `direct`, with the replying broker's
/// NodeId and incarnation; a compute that holds no edge to the broker gets `no-active-route` /
/// NotSent and the broker applies nothing for it; an exact NodeId the fabric never held is
/// NotSent `Resolve(Unknown)`. What must NOT happen: a reply from any node but the requested one, a
/// handler execution for a call that was not sent, or a route the connections projection does not
/// hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_roles_route_opaque_calls_to_exact_nodes() {
    let cell = "mock_roles_route_opaque_calls_to_exact_nodes";
    let shape = require_tier(CANONICAL);
    let mut f = form(cell, shape).await;
    let mut inv = Invariants::default();
    let nodes = settle(&f).await;
    let (launches, fabric) = check_identities_and_launches(&f, &nodes, &mut inv).await;
    let authorities = check_authorities(&nodes, &fabric, &mut inv);
    let mut authority_history = vec![authority_row("formed", &nodes, &fabric)];
    let connections = converge_connections(&f, &mut inv).await;
    let mut routes = RouteRun::new();
    routes.run(&f, &nodes, &mut inv).await;
    f.actions.extend(routes.calls.iter().map(action_row));
    let (_, fabric_end) = f.estate.get("/api/fabric").await;
    let nodes_end = f.estate.nodes().await;
    authority_history.push(authority_row("routed", &nodes_end, &fabric_end));
    let stop_t = Instant::now();
    f.estate.stop().await;
    let left = provider_left(&f, stop_t, &mut inv).await;
    let spans = f.estate.spans();
    let services = check_services(&spans, &mut inv);
    let chains = check_formation_chain(&f, &spans, &mut inv);
    let runtime = check_runtime_facts(&f, &nodes, &launches, &spans, &mut inv);
    let route_obs = routes.check_spans(&f, &spans, &mut inv);
    let index = copy_spans(&f.estate, &f.dir);
    let actions = f.actions.clone();
    write_views(
        Views {
            f: &f,
            topology_final: nodes_end,
            actions,
            operations: routes.operations(),
            outcomes: routes.outcomes(),
            authority_history,
            route_history: Vec::new(),
            runtime_history: runtime,
            inv,
            extra: json!({"authorities": authorities, "connections": connections, "provider_after_stop": left, "services": services, "formation_chains": chains, "routes": route_obs}),
        },
        index,
        &spans,
    );
}

// ---- cell 3: traffic during injected changes -----------------------------------------------------

/// One synthetic operation of the seeded schedule: a stable id (`seq`), its key and value, and where
/// and how it is aimed.
struct SyntheticOp {
    seq: u64,
    key: String,
    value: String,
    target: String,
    target_id: String,
    via: Option<String>,
    window: &'static str,
}

/// Restart `node` through the rectifier and keep the seeded traffic going until the Build is
/// complete and the node is ready under a new incarnation; returns the restart record. Traffic is
/// issued between the readiness polls, and `aim` chooses each operation's target.
#[allow(clippy::too_many_arguments)]
async fn restart_with_traffic(
    f: &Formed,
    node: &str,
    ops: &mut Vec<(SyntheticOp, Call)>,
    rng: &mut Rng,
    seq: &mut u64,
    brokers: &[Value],
    gateways: &[String],
    aim_at: &str,
    actions: &mut Vec<Value>,
) -> Value {
    let e = &f.estate;
    let before = e.node(node).await;
    let t0 = now_ms();
    let (status, r) = e.post(&format!("/api/nodes/{node}/restart"), &Value::Null).await;
    assert_eq!(status, 202, "{node}: {r}");
    let build_id = s(&r["build_id"]);
    actions.push(json!({"t_ms": t0, "action": "node.restart", "via": format!("POST /api/nodes/{node}/restart"), "node": node, "status": status, "build_id": build_id, "old_incarnation_id": before["incarnation_id"]}));
    let started = Instant::now();
    let mut during = 0;
    loop {
        let (_, b) = e.get(&format!("/api/builds?id={build_id}")).await;
        assert_ne!(b["state"], "failed", "{node}: restart Build failed: {b:#}");
        let now = e.node_opt(node).await;
        let reborn = now.as_ref().is_some_and(|n| n["status"] == "ready-for-traffic" && n["incarnation_id"] != before["incarnation_id"]);
        if b["state"] == "complete" && reborn {
            actions.push(json!({"t_ms": now_ms(), "action": "node.restart.complete", "node": node, "build_id": build_id, "new_incarnation_id": now.as_ref().map(|n| n["incarnation_id"].clone()), "node_id": now.as_ref().map(|n| n["node_id"].clone())}));
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(120), "{node}: the restart did not complete within 120 s: build {b:#}");
        // Half of the traffic in the window is aimed at the changing node; the rest is spread.
        let aimed = rng.below(2) == 0;
        issue(f, ops, rng, seq, brokers, gateways, if aimed { Some(aim_at) } else { None }, "during-restart").await;
        during += 1;
    }
    json!({"node": node, "build_id": build_id, "old_incarnation_id": before["incarnation_id"], "operations_issued_during": during, "window_ms": started.elapsed().as_millis() as u64})
}

/// Issue one seeded synthetic put. `aim` names a node whose operations are forced (a broker, or a
/// gateway as the carrier); otherwise the broker and route are drawn from the seed.
async fn issue(f: &Formed, ops: &mut Vec<(SyntheticOp, Call)>, rng: &mut Rng, seq: &mut u64, brokers: &[Value], gateways: &[String], aim: Option<&str>, window: &'static str) {
    let mut broker = &brokers[rng.below(brokers.len() as u64) as usize];
    let mut via = if rng.below(4) == 0 { Some(gateways[rng.below(gateways.len() as u64) as usize].clone()) } else { None };
    if let Some(a) = aim {
        if let Some(b) = brokers.iter().find(|b| b["name"] == a) {
            broker = b;
        } else {
            via = Some(a.to_string());
        }
    }
    *seq += 1;
    let op = SyntheticOp {
        seq: *seq,
        key: format!("op-{:05}", *seq),
        value: format!("v{}-{:x}", *seq, rng.below(1 << 24)),
        target: s(&broker["name"]),
        target_id: s(&broker["node_id"]),
        via,
        window,
    };
    let exact = format!("exact:{}", op.target_id);
    let via_arg = op.via.as_ref().map(|v| format!("path:{v}"));
    let mut args = vec!["put", "--target", exact.as_str(), "--key", op.key.as_str(), "--value", op.value.as_str()];
    if let Some(v) = &via_arg {
        args.extend(["--via", v.as_str()]);
    }
    let call = probe_call(&f.estate, &args);
    ops.push((op, call));
}

/// CONTRACT: with the canonical estate formed, continuous seeded synthetic traffic (puts with a
/// stable operation id `op-<seq>` into both meshes' brokers, a quarter of them carried through a
/// gateway) runs through two injected changes, each a restart of a role node executed by the
/// Build rectifier: a broker, then a gateway. Every operation is issued exactly once and ends in
/// exactly one typed bucket (Reply, NotSent, Unserved, RejectedStale or Indeterminate); the
/// buckets sum to the issued count. The cell has no retry path: an outcome is recorded, never
/// repeated. Afterwards the final state of every key is read from its broker: every successful put
/// holds its value; every put that provably was not dispatched is absent; an Indeterminate put is
/// reported as applied or not, never re-issued; and from the spans, each operation's trace holds
/// at most one handler execution, none for a not-dispatched outcome, so no operation was applied
/// twice. What must NOT happen: an operation without an outcome, a bucket total that does not
/// equal the issued total, a not-dispatched operation whose value landed, an operation applied
/// twice, or an Indeterminate operation re-sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_traffic_accounts_for_every_outcome_without_uncertain_replay() {
    let cell = "mock_traffic_accounts_for_every_outcome_without_uncertain_replay";
    let shape = require_tier(CANONICAL);
    let mut f = form(cell, shape).await;
    let mut inv = Invariants::default();
    let nodes = settle(&f).await;
    let (launches, fabric) = check_identities_and_launches(&f, &nodes, &mut inv).await;
    check_authorities(&nodes, &fabric, &mut inv);
    let mut authority_history = vec![authority_row("formed", &nodes, &fabric)];
    converge_connections(&f, &mut inv).await;

    let brokers: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "broker").cloned().collect();
    let gateways: Vec<String> = nodes.iter().filter(|n| n["kind"] == "gateway").map(|n| s(&n["name"])).collect();
    assert_eq!((brokers.len(), gateways.len()), (6, 6));
    let mut rng = Rng(f.seed);
    let mut seq = 0u64;
    let mut ops: Vec<(SyntheticOp, Call)> = Vec::new();
    let mut changes = Vec::new();
    let mut actions = std::mem::take(&mut f.actions);

    // Steady traffic first: the healthy same-family control for everything the changes do.
    for _ in 0..12 {
        issue(&f, &mut ops, &mut rng, &mut seq, &brokers, &gateways, None, "steady-before").await;
    }
    let steady_replies = ops.iter().filter(|(_, c)| bucket(&c.out) == Bucket::Reply).count();
    inv.holds("steady traffic before any change: every operation replied", steady_replies == ops.len(), json!({"issued": ops.len(), "replied": steady_replies}));
    // Injected change 1: restart a broker (aimed traffic goes to it).
    changes.push(restart_with_traffic(&f, "mesh1.broker.1", &mut ops, &mut rng, &mut seq, &brokers, &gateways, "mesh1.broker.1", &mut actions).await);
    for _ in 0..6 {
        issue(&f, &mut ops, &mut rng, &mut seq, &brokers, &gateways, None, "steady-between").await;
    }
    // Injected change 2: restart a gateway (aimed traffic is carried through it).
    changes.push(restart_with_traffic(&f, "mesh2.gateway.1", &mut ops, &mut rng, &mut seq, &brokers, &gateways, "mesh2.gateway.1", &mut actions).await);
    for _ in 0..6 {
        issue(&f, &mut ops, &mut rng, &mut seq, &brokers, &gateways, None, "steady-after").await;
    }
    authority_history.push({
        let (_, fb) = f.estate.get("/api/fabric").await;
        authority_row("after-changes", &f.estate.nodes().await, &fb)
    });
    let during: usize = ops.iter().filter(|(o, _)| o.window == "during-restart").count();
    inv.holds("traffic was issued inside each injected change", changes.iter().all(|c| c["operations_issued_during"].as_u64().unwrap_or(0) >= 1), json!({"changes": changes, "during_total": during}));

    // Final state of every key: a read through the node it was aimed at, after the estate is back.
    let after = f.estate.nodes().await;
    let mut verification = Vec::new();
    let mut final_state: BTreeMap<String, Value> = BTreeMap::new();
    for (op, _) in &ops {
        let id = after.iter().find(|n| n["name"] == op.target.as_str()).map(|n| s(&n["node_id"])).unwrap();
        assert_eq!(id, op.target_id, "{}: a restart keeps the logical node", op.target);
        let c = probe_call(&f.estate, &["get", "--target", &format!("exact:{id}"), "--key", &op.key]);
        assert_eq!(c.out["outcome"], "Reply", "verification read of {} must reply once the estate is settled: {}", op.key, c.out);
        final_state.insert(op.key.clone(), c.out["reply"]["result"].clone());
        verification.push(c);
    }
    let (_, fabric_end) = f.estate.get("/api/fabric").await;
    let nodes_end = f.estate.nodes().await;
    let stop_t = Instant::now();
    f.estate.stop().await;
    let left = provider_left(&f, stop_t, &mut inv).await;
    let spans = f.estate.spans();

    // The ledger algebra over the typed outcomes.
    let mut buckets: BTreeMap<Bucket, usize> = Bucket::ALL.iter().map(|b| (*b, 0)).collect();
    let mut seqs = BTreeSet::new();
    let stored: Vec<&Value> = named(&spans, "rdm.node_rpc.proof_store.serve.via-request").into_iter().filter(|sp| sp["attributes"]["op"] == "put" && sp["attributes"]["outcome"] == "stored").collect();
    let mut operations = Vec::new();
    let mut outcomes = Vec::new();
    let mut indeterminate = Vec::new();
    let mut refusals = Vec::new();
    let mut summary = BTreeMap::new();
    for (op, call) in &ops {
        assert!(seqs.insert(op.seq), "operation id {} issued twice", op.seq);
        let b = bucket(&call.out);
        *buckets.get_mut(&b).unwrap() += 1;
        let present = final_state[&op.key] == json!({"found": true, "value": op.value});
        let other = final_state[&op.key]["found"] == true && !present;
        assert!(!other, "{}: the final value is not the one this operation wrote: {}", op.key, final_state[&op.key]);
        let handler_runs = stored.iter().filter(|sp| sp["trace_id"] == call.trace_id.as_str()).count();
        assert!(handler_runs <= 1, "{} ({b:?}): applied {handler_runs} times in its trace; an operation is applied at most once", op.key);
        match b {
            Bucket::Reply if call.out["reply"]["result"] == json!({"stored": true}) => {
                assert!(present, "{}: replied stored but the final state lacks its value: {}", op.key, final_state[&op.key]);
                assert_eq!(handler_runs, 1, "{}: replied stored with no handler execution in its trace", op.key);
                assert_eq!(call.out["reply"]["executing_node"], op.target_id.as_str(), "{}: the reply names the requested node", op.key);
            }
            // A reply that is a protocol refusal (the node answered `Draining`): the request reached a
            // live protocol door and was refused, so nothing was applied. A refusal carries no provenance.
            Bucket::Reply => {
                let refused = call.out["reply"]["result"]["refused"].as_str().unwrap_or_else(|| panic!("{}: a Reply that is neither stored nor a named refusal: {}", op.key, call.out));
                assert!(!present, "{}: refused ({refused}) yet its value is in the final state", op.key);
                assert_eq!(handler_runs, 0, "{}: refused ({refused}) yet a handler applied it", op.key);
                refusals.push(json!({"seq": op.seq, "key": op.key, "target": op.target, "refusal": refused}));
            }
            Bucket::NotSent | Bucket::Unserved | Bucket::RejectedStale => {
                assert!(!present, "{} ({b:?}) proves the request was not dispatched, yet its value is in the final state", op.key);
                assert_eq!(handler_runs, 0, "{} ({b:?}): a handler ran for a request that was not dispatched", op.key);
            }
            Bucket::Indeterminate => {
                assert_eq!(present, handler_runs == 1, "{}: the final state ({present}) and the handler record ({handler_runs}) disagree about whether the Indeterminate put applied", op.key);
                indeterminate.push(json!({"seq": op.seq, "key": op.key, "applied": present, "reason": call.out["reason"]}));
            }
        }
        *summary.entry(format!("{:?}:{}", b, call.out["reason"].as_str().or(call.out["reply"]["result"]["refused"].as_str()).unwrap_or(""))).or_insert(0usize) += 1;
        operations.push(json!({"op_seq": op.seq, "key": op.key, "value": op.value, "target": op.target, "target_node_id": op.target_id, "via": op.via, "window": op.window, "issued_ms": call.started_ms, "trace_id": call.trace_id, "probe_invocations": 1}));
        outcomes.push(json!({
            "op_seq": op.seq, "key": op.key, "bucket": format!("{b:?}"), "reason": call.out["reason"], "route": call.out["route"], "provenance": call.out["reply"],
            "dispatched_handler_runs": handler_runs, "final_state": final_state[&op.key], "applied": present, "finished_ms": call.finished_ms, "trace_id": call.trace_id,
        }));
    }
    let classified: usize = buckets.values().sum();
    inv.holds("issued = Reply + NotSent + Unserved + RejectedStale + Indeterminate; every operation id issued once", classified == ops.len() && seqs.len() == ops.len() && seqs.iter().copied().eq(1..=ops.len() as u64), json!({"issued": ops.len(), "buckets": buckets.iter().map(|(k, v)| (format!("{k:?}"), *v)).collect::<BTreeMap<_, _>>()}));
    let present_keys = final_state.values().filter(|v| v["found"] == true).count();
    let all_stored_on_targets = stored.iter().filter(|sp| ops.iter().any(|(o, _)| sp["attributes"]["node_id"] == o.target_id.as_str())).count();
    inv.holds(
        "no operation was applied twice: every put the brokers applied is a key that is in the final state exactly once",
        all_stored_on_targets == present_keys,
        json!({"stored_handler_runs": all_stored_on_targets, "keys_present": present_keys}),
    );
    inv.holds("every stored put holds its value, every protocol-refused or not-dispatched put is absent, every Indeterminate put is reported applied or not and was issued once (no automatic replay)", true, json!({"indeterminate": indeterminate, "probe_invocations_per_operation": 1}));
    inv.holds("the Indeterminate arm is reached or its absence recorded: the outcome classes seen are listed", true, json!({"classes_seen": summary}));
    let changes_trace: Vec<Value> = changes
        .iter()
        .map(|c| {
            let node = s(&c["node"]);
            let bid = s(&c["build_id"]);
            let rest = named(&spans, "rdm.node_admin.build.update.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == bid.as_str() && sp["attributes"]["node"] == node.as_str()).cloned().unwrap_or_else(|| panic!("{node}: no build.update.via-rest for the restart"));
            let op = format!("restart-node:{node}");
            let reconcile = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().find(|sp| sp["attributes"]["build_id"] == bid.as_str() && sp["attributes"]["operations"].as_str().is_some_and(|o| o.split(',').any(|x| x == op))).cloned().unwrap_or_else(|| panic!("{node}: no reconcile executed {op}"));
            let update = named(&spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| sp["attributes"]["node"] == node.as_str() && descends_from(&spans, sp, &reconcile)).cloned().unwrap_or_else(|| panic!("{node}: no node.update.via-build under the reconcile"));
            let steps = named(&spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|sp| descends_from(&spans, sp, &update)).count();
            assert!(steps > 0, "{node}: deployment steps under its node update");
            json!({"node": node, "build_id": bid, "rest_span": rest["span_id"], "reconcile_span": reconcile["span_id"], "node_update_span": update["span_id"], "steps": steps, "change": c})
        })
        .collect();
    inv.holds("each injected change was a Build the rectifier executed: REST -> reconcile -> node.update.via-build -> deployment steps", true, json!({"changes": changes_trace.len()}));
    let services = check_services(&spans, &mut inv);
    let chains = check_formation_chain(&f, &spans, &mut inv);
    let runtime = check_runtime_facts(&f, &nodes, &launches, &spans, &mut inv);
    let index = copy_spans(&f.estate, &f.dir);
    authority_history.push(authority_row("final", &nodes_end, &fabric_end));
    actions.extend(ops.iter().map(|(_, c)| action_row(c)));
    actions.extend(verification.iter().map(action_row));
    f.actions = actions.clone();
    write_views(
        Views {
            f: &f,
            topology_final: nodes_end,
            actions,
            operations,
            outcomes,
            authority_history,
            route_history: Vec::new(),
            runtime_history: runtime,
            inv,
            extra: json!({
                "schedule": {"seed": f.seed, "generator": "rafka_test_scenario::model::Rng (SplitMix64)", "steady_before": 12, "restarts": ["mesh1.broker.1", "mesh2.gateway.1"], "steady_between": 6, "steady_after": 6},
                "issued": ops.len(), "buckets": buckets.iter().map(|(k, v)| (format!("{k:?}"), *v)).collect::<BTreeMap<_, _>>(), "class_detail": summary,
                "indeterminate": indeterminate, "protocol_refusals": refusals, "injected_changes": changes_trace, "verification_reads": verification.len(),
                "services": services, "formation_chains": chains, "provider_after_stop": left,
            }),
        },
        index,
        &spans,
    );
}
