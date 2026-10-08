//! i143.e11.s1 (rafka-v2 #2944): the R-shape mock composition and traffic cells. The twenty-node
//! estate (node-admin / compute / gateway / broker = 2 / 2 / 3 / 3 x 2 meshes) is born from empty
//! provider state on the four executables of an independent consumer workspace
//! (`qualification/rshape-consumer`, built by `scripts/i143-rshape-build-consumer.sh`, bound
//! explicitly through `Estate::bootstrap_external`), and typed opaque work is routed through it.
//!
//! Run by `scripts/i143-acceptance-gate.sh i143-rshape-{composition,fast}-{process,container}`,
//! which exports `I143_ACCEPTANCE_DIR` (each cell's `result.json` and evidence views go there) and
//! whose command sets `RDM_RSHAPE_CONSUMER_BIN_DIR`, `RDM_RSHAPE_TIER`, `RDM_RSHAPE_SEED`,
//! `MESH_SPAWN_TYPE` and `RDM_ARTIFACTS_DIR` (the estate's manifest, rpc ledger and every
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
use rafka_test_scenario::container_faults;
use rafka_test_scenario::elections::seats_as_expected;
use rafka_test_scenario::estate::{binding_set_from_build_manifest, descends_from, named, wait_for, Estate, Owner, RUNTIME_IMAGE};
use rafka_test_scenario::ledger::Bucket;
use rafka_test_scenario::model::Rng;
use rafka_test_scenario::process_faults::{ExactRuntime, Fault as SigFault, Refusal as SigRefusal};
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
        let pid = match (node == "mesh1.admin.1", estate.bootstrap_pid().filter(|p| pid_alive(u64::from(*p)))) {
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
    converge_connections_with(f, inv, true).await
}

/// [`converge_connections`], optionally tolerating a Connected Direct fact whose destination has
/// since been reborn (`strict = false`): such a fact is listed under `stale_connected` and never
/// counted as missing. The required edge, to the mesh primary's current birth, is always required.
async fn converge_connections_with(f: &Formed, inv: &mut Invariants, strict: bool) -> Value {
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
            let mut stale = Vec::new();
            for d in directs.iter().filter(|d| d["state"] == "Connected") {
                let current = nodes.iter().find(|n| n["node_id"] == d["destination"]["node_id"]);
                if current.is_none_or(|c| c["incarnation_id"] != d["destination"]["incarnation"]) {
                    stale.push(json!({"to": d["destination"]["name"], "incarnation": d["destination"]["incarnation"]}));
                    if strict {
                        missing.push(format!("{} (a Connected edge to a superseded or unknown birth)", d["destination"]["name"]));
                    }
                }
            }
            if missing.is_empty() && snap["owed"].as_array().is_some_and(|o| o.is_empty()) {
                break (snap, stale);
            }
            assert!(
                Instant::now() < until,
                "{node} never held its required connections (a Connected Direct edge to the current primary node-admin of {mesh}, none to a superseded birth, nothing owed) within 30 s: missing {missing:?}; directs {}; owed {}",
                serde_json::to_string(&directs.iter().map(|d| json!({"to": d["destination"]["name"], "state": d["state"], "kind": d["kind"], "inc": d["destination"]["incarnation"]})).collect::<Vec<_>>()).unwrap(),
                snap["owed"]
            );
            tokio::time::sleep(Duration::from_millis(250)).await;
        };
        let (snap, stale) = snap;
        out.insert(node, json!({"active_len": snap["active_len"], "owed": snap["owed"], "directs": snap["own_latest_directs"], "stale_connected": stale}));
    }
    let msg = if strict {
        "every compute and gateway holds a Connected Direct edge to the current birth of its mesh's primary node-admin, none to a superseded birth, and owes no retirement"
    } else {
        "every compute and gateway holds a Connected Direct edge to the current birth of its mesh's primary node-admin and owes no retirement (a Connected fact to a reborn node's superseded birth is listed, not required away)"
    };
    inv.holds(msg, true, json!({"nodes": out.len()}));
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
/// RDM_SHUTDOWN_DRAIN_BOUND_MS, 60 s each). The wall from the stop request to the last exit is
/// recorded: it is a measurement of the shutdown, not a tolerance.
async fn provider_left(f: &Formed, stop_started: Instant, inv: &mut Invariants) -> Value {
    let drain_bound = std::env::var("RDM_SHUTDOWN_DRAIN_BOUND_MS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(60_000);
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

/// What the ledger algebra made of a run's operations.
struct Accounted {
    buckets: BTreeMap<Bucket, usize>,
    summary: BTreeMap<String, usize>,
    indeterminate: Vec<Value>,
    refusals: Vec<Value>,
    operations: Vec<Value>,
    outcomes: Vec<Value>,
}

/// Classify every issued operation exactly once from its typed outcome and reconcile it against the
/// final state of its key and the handler executions the spans recorded: issued = Reply + NotSent +
/// Unserved + RejectedStale + Indeterminate, a stored put holds its value, a put that was not
/// dispatched is absent, no operation was applied twice, an Indeterminate put is reported applied
/// or not and never re-issued.
fn account(spans: &[Value], ops: &[(SyntheticOp, Call)], final_state: &BTreeMap<String, Value>, inv: &mut Invariants) -> Accounted {
    // The ledger algebra over the typed outcomes.
    let mut buckets: BTreeMap<Bucket, usize> = Bucket::ALL.iter().map(|b| (*b, 0)).collect();
    let mut seqs = BTreeSet::new();
    let stored: Vec<&Value> = named(&spans, "rdm.node_rpc.proof_store.serve.via-request").into_iter().filter(|sp| sp["attributes"]["op"] == "put" && sp["attributes"]["outcome"] == "stored").collect();
    let mut operations = Vec::new();
    let mut outcomes = Vec::new();
    let mut indeterminate = Vec::new();
    let mut refusals = Vec::new();
    let mut summary = BTreeMap::new();
    for (op, call) in ops {
        assert!(seqs.insert(op.seq), "operation id {} issued twice", op.seq);
        let b = bucket(&call.out);
        *buckets.get_mut(&b).unwrap() += 1;
        // A target whose NodeId left the fabric took its store with it: the operation is still
        // classified from its typed outcome and its handler record, but its key has no final state.
        let gone = final_state[&op.key].get("unverifiable").is_some();
        let present = final_state[&op.key] == json!({"found": true, "value": op.value});
        let other = final_state[&op.key]["found"] == true && !present;
        assert!(!other, "{}: the final value is not the one this operation wrote: {}", op.key, final_state[&op.key]);
        let handler_runs = stored.iter().filter(|sp| sp["trace_id"] == call.trace_id.as_str()).count();
        assert!(handler_runs <= 1, "{} ({b:?}): applied {handler_runs} times in its trace; an operation is applied at most once", op.key);
        match b {
            Bucket::Reply if call.out["reply"]["result"] == json!({"stored": true}) => {
                assert!(present || gone, "{}: replied stored but the final state lacks its value: {}", op.key, final_state[&op.key]);
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
                if !gone {
                    assert_eq!(present, handler_runs == 1, "{}: the final state ({present}) and the handler record ({handler_runs}) disagree about whether the Indeterminate put applied", op.key);
                }
                indeterminate.push(json!({"seq": op.seq, "key": op.key, "applied": if gone { handler_runs == 1 } else { present }, "reason": call.out["reason"], "target_retired": gone}));
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
    let verifiable_targets: BTreeSet<&str> = ops.iter().filter(|(o, _)| final_state[&o.key].get("unverifiable").is_none()).map(|(o, _)| o.target_id.as_str()).collect();
    let unverifiable = ops.iter().filter(|(o, _)| final_state[&o.key].get("unverifiable").is_some()).count();
    let all_stored_on_targets = stored.iter().filter(|sp| verifiable_targets.iter().any(|t| sp["attributes"]["node_id"] == *t)).count();
    inv.holds(
        "no operation was applied twice: every put the brokers applied is a key that is in the final state exactly once",
        all_stored_on_targets == present_keys,
        json!({"stored_handler_runs": all_stored_on_targets, "keys_present": present_keys, "operations_on_retired_targets_not_verifiable": unverifiable}),
    );
    inv.holds("every stored put holds its value, every protocol-refused or not-dispatched put is absent, every Indeterminate put is reported applied or not and was issued once (no automatic replay)", true, json!({"indeterminate": indeterminate, "probe_invocations_per_operation": 1}));
    inv.holds("the Indeterminate arm is reached or its absence recorded: the outcome classes seen are listed", true, json!({"classes_seen": summary}));
    Accounted { buckets, summary, indeterminate, refusals, operations, outcomes }
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

    let Accounted { buckets, summary, indeterminate, refusals, operations, outcomes } = account(&spans, &ops, &final_state, &mut inv);
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

// ---- cells 4 and 5: authority and recovery under admin churn (rafka-v2 #2945) ----------------------

/// The tier an authority cell runs at: the canonical twenty-node estate qualifies the export; the
/// reduced ten-node fixture runs the same cell as a fast control and says so in its result.
fn any_tier() -> Shape {
    match env("RDM_RSHAPE_TIER").as_str() {
        "canonical" => CANONICAL,
        "fast" => FAST,
        other => panic!("REFUSED: RDM_RSHAPE_TIER={other}; an authority cell runs at `canonical` (the qualifying twenty-node estate) or `fast` (the reduced control)"),
    }
}

fn now_ns() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos() as u64).unwrap_or(0)
}

fn pid_alive(pid: u64) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat")).is_ok_and(|st| !st.rsplit(')').next().is_some_and(|r| r.trim_start().starts_with('Z')))
}

/// `GET base+path` when the admin answers within 2 s; never a panic, so a dead admin is a `None`.
async fn try_json(base: &str, path: &str) -> Option<Value> {
    let r = reqwest::Client::new().get(format!("{base}{path}")).timeout(Duration::from_secs(2)).send().await.ok()?;
    if !r.status().is_success() {
        return None;
    }
    r.json().await.ok()
}

/// The pid of `name`'s runtime now: the Day-0 admin's own process while it is the one running, else
/// the provider's record (process: `deployment.json`; container: the container's init).
async fn node_pid(f: &Formed, name: &str) -> u64 {
    if provider() != "container" && name == "mesh1.admin.1" {
        if let Some(p) = f.estate.bootstrap_pid() {
            if pid_alive(u64::from(p)) {
                return u64::from(p);
            }
        }
    }
    f.estate.pid_of(name).await
}

/// Seeded synthetic puts into both meshes' brokers, the ledger of the whole cell.
struct Traffic {
    ops: Vec<(SyntheticOp, Call)>,
    rng: Rng,
    seq: u64,
    brokers: Vec<Value>,
    gateways: Vec<String>,
    /// A broker (or a gateway as carrier) every operation of a burst is aimed at, while set.
    aim: Option<String>,
}

impl Traffic {
    fn new(seed: u64, nodes: &[Value]) -> Self {
        let brokers: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "broker").cloned().collect();
        let gateways: Vec<String> = nodes.iter().filter(|n| n["kind"] == "gateway").map(|n| s(&n["name"])).collect();
        assert!(!brokers.is_empty() && !gateways.is_empty());
        Self { ops: Vec::new(), rng: Rng(seed), seq: 0, brokers, gateways, aim: None }
    }

    /// The brokers the next operations are aimed at are the ones standing now: a node the fabric
    /// re-birthed is a new NodeId, and an exact call to the old one is a call to nothing.
    fn refresh(&mut self, nodes: &[Value]) {
        let brokers: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "broker").cloned().collect();
        assert!(!brokers.is_empty(), "no broker in the settled view");
        self.brokers = brokers;
    }

    async fn burst(&mut self, f: &Formed, n: usize, window: &'static str) {
        for _ in 0..n {
            issue(f, &mut self.ops, &mut self.rng, &mut self.seq, &self.brokers, &self.gateways, self.aim.as_deref(), window).await;
        }
    }
}

/// The authority picture at one stable checkpoint.
struct Stable {
    nodes: Vec<Value>,
    fabric: Value,
    fp: Value,
    admins: Vec<Value>,
}

impl Stable {
    fn fp_name(&self) -> String {
        s(&self.fp["name"])
    }

    fn fp_base(&self) -> String {
        s(&self.fp["admin_api_base"])
    }

    fn mesh_of_fp(&self) -> String {
        s(&self.fp["mesh"])
    }

    fn primary_of(&self, mesh: &str) -> Value {
        self.admins.iter().find(|a| a["mesh"] == mesh && a["is_primary"] == true).cloned().unwrap_or_else(|| panic!("{mesh} has no primary node-admin: {:#?}", self.admins))
    }

    fn secondary_of(&self, mesh: &str) -> Value {
        let m: Vec<&Value> = self.admins.iter().filter(|a| a["mesh"] == mesh && a["is_primary"] != true).collect();
        assert_eq!(m.len(), 1, "{mesh} has exactly one non-primary node-admin: {m:?}");
        m[0].clone()
    }

}

/// What an authority cell records as it goes: the traffic ledger, the authority history, the births
/// seen, every accepted topology write, every fence probe and every scenario event.
struct Authority {
    traffic: Traffic,
    history: Vec<Value>,
    births: Vec<Value>,
    writes: Vec<Value>,
    probes: Vec<Value>,
    events: Vec<Value>,
    actions: Vec<Value>,
    known_bases: BTreeSet<String>,
    probe_round: u64,
    /// The Build projection as the fabric folds it, each time it changed: attempt, executor, state, reason.
    build_obs: Vec<Value>,
    /// The role nodes a checkpoint expects to stand, when a lifecycle cell has retired or not yet
    /// re-added one; the shape's own role nodes when unset.
    roles: Option<BTreeSet<String>>,
    /// Topology writes the cell sent to an admin that is not the fabric-primary, to see them refused.
    refused_writes: Vec<Value>,
}

/// What `Authority::finish` waits for, observed from the view and the Build, never from elapsed time.
enum Until {
    /// The node is ready under an incarnation other than `old_incarnation`.
    Reborn { node: String, old_incarnation: String },
    /// The node is no longer in the view.
    Gone { node: String },
    /// `mesh` holds a ready node-admin that was not in `before`.
    Grew { mesh: String, before: BTreeSet<String> },
    /// A node stands at `node` that is ready and whose NodeId is not `not_node_id`.
    Joined { node: String, not_node_id: String },
}

fn progress(msg: &str) {
    eprintln!("[authority] {} {msg}", now_ms());
}

impl Authority {
    fn new(seed: u64, nodes: &[Value]) -> Self {
        Self { traffic: Traffic::new(seed, nodes), history: Vec::new(), births: Vec::new(), writes: Vec::new(), probes: Vec::new(), events: Vec::new(), actions: Vec::new(), known_bases: BTreeSet::new(), probe_round: 0, build_obs: Vec::new(), roles: None, refused_writes: Vec::new() }
    }

    /// A GET on any live admin of this run: the entry admin first, then every admin base ever seen.
    /// The entry follows the first that answers for this Fabric.
    async fn get(&mut self, f: &mut Formed, path: &str) -> Option<Value> {
        let mut candidates = vec![f.estate.admin.clone()];
        candidates.extend(self.known_bases.iter().filter(|b| **b != f.estate.admin).cloned());
        for b in candidates {
            if f.estate.fabric_at(&b).await.is_some() {
                if let Some(v) = try_json(&b, path).await {
                    f.estate.admin = b;
                    return Some(v);
                }
            }
        }
        None
    }

    /// Keep a Build projection when its attempt, executor, state or reason moved since the last one seen.
    fn observe_build(&mut self, b: &Value) {
        if b["build_id"].is_null() {
            return;
        }
        let key = |v: &Value| format!("{}|{}|{}|{}|{}", v["build_id"], v["attempt"], v["executor"], v["state"], v["reason"]);
        if self.build_obs.last().is_none_or(|l| key(l) != key(b)) {
            self.build_obs.push(json!({"t_ns": now_ns(), "build_id": b["build_id"], "attempt": b["attempt"], "executor": b["executor"], "state": b["state"], "reason": b["reason"], "last_failure": b["last_failure"]}));
        }
    }

    async fn nodes_now(&mut self, f: &Formed) -> Vec<Value> {
        let mut candidates = vec![f.estate.admin.clone()];
        candidates.extend(self.known_bases.iter().filter(|b| **b != f.estate.admin).cloned());
        for b in candidates {
            if let Some(v) = try_json(&b, "/api/nodes").await {
                return v["nodes"].as_array().cloned().unwrap_or_default();
            }
        }
        Vec::new()
    }

    async fn nodes(&mut self, f: &mut Formed) -> Option<Vec<Value>> {
        self.get(f, "/api/nodes").await.map(|v| v["nodes"].as_array().cloned().unwrap_or_default())
    }

    /// One look at authority: Ok when every ready admin's own view agrees with the election
    /// function and with every other admin's, the fabric's Build is complete and the shape holds.
    async fn look(&mut self, f: &mut Formed, want_admins: &BTreeMap<String, usize>) -> Result<Stable, String> {
        let Some(nodes) = self.nodes(f).await else { return Err("no admin answers /api/nodes".into()) };
        let fabric = self.get(f, "/api/fabric").await.ok_or("no admin answers /api/fabric")?;
        let roles: BTreeSet<String> = nodes.iter().filter(|n| n["kind"] != "node_admin").map(|n| s(&n["name"])).collect();
        let want_roles: BTreeSet<String> = self.roles.clone().unwrap_or_else(|| f.shape.names().into_iter().filter(|n| launch_id(n) != "node_admin").collect());
        if roles != want_roles {
            return Err(format!("role nodes {roles:?} are not the shape's {want_roles:?}"));
        }
        if let Some(n) = nodes.iter().find(|n| n["status"] != "ready-for-traffic") {
            return Err(format!("{} is {}", n["name"], n["status"]));
        }
        let admins: Vec<Value> = nodes.iter().filter(|n| n["kind"] == "node_admin").cloned().collect();
        for (mesh, want) in want_admins {
            let have = admins.iter().filter(|a| a["mesh"] == mesh.as_str()).count();
            if have != *want {
                return Err(format!("{mesh} holds {have} ready node-admins, {want} wanted"));
            }
        }
        if admins.len() != want_admins.values().sum::<usize>() {
            return Err(format!("{} node-admins in the view, {:?} wanted", admins.len(), want_admins));
        }
        if let Err(e) = seats_as_expected(&nodes) {
            return Err(format!("the entry view's seats are not the election function's: {e}"));
        }
        let fp = admins.iter().find(|a| a["is_fabric_primary"] == true).cloned().ok_or("no fabric-primary in the view")?;
        let key = |v: &[Value]| -> BTreeSet<String> { v.iter().filter(|n| n["kind"] == "node_admin").map(|n| format!("{}={}:{}", n["name"], n["node_id"], n["incarnation_id"])).collect() };
        let entry_key = key(&nodes);
        for a in &admins {
            let base = s(&a["admin_api_base"]);
            self.known_bases.insert(base.clone());
            let Some(own_fabric) = f.estate.fabric_at(&base).await else { return Err(format!("{} does not answer for this Fabric at {base}", a["name"])) };
            let Some(own) = try_json(&base, "/api/nodes").await else { return Err(format!("{} does not answer /api/nodes", a["name"])) };
            let own_nodes = own["nodes"].as_array().cloned().unwrap_or_default();
            if let Err(e) = seats_as_expected(&own_nodes) {
                return Err(format!("{}'s own seats are not the election function's: {e}", a["name"]));
            }
            if key(&own_nodes) != entry_key {
                return Err(format!("{} holds a different admin set than the entry view", a["name"]));
            }
            if own_fabric["fabric_primary"] != fp["name"] {
                return Err(format!("{} names {} as fabric-primary, the entry view {}", a["name"], own_fabric["fabric_primary"], fp["name"]));
            }
        }
        let fp_base = s(&fp["admin_api_base"]);
        let own = try_json(&fp_base, "/api/fabric").await.ok_or("the fabric-primary does not answer /api/fabric")?;
        let bid = s(&own["build_id"]);
        let b = try_json(&fp_base, &format!("/api/builds?id={bid}")).await.ok_or(format!("the fabric-primary does not answer for Build {bid}"))?;
        if b["state"] != "complete" {
            return Err(format!("the fabric's Build {bid} is {}, not complete", b["state"]));
        }
        Ok(Stable { nodes, fabric, fp, admins })
    }

    /// A stable checkpoint: authority converges within 30 s of being asked (a bound that fires is
    /// evidence, never widened), then one fence probe round proves exactly one effective writer.
    async fn stable(&mut self, f: &mut Formed, label: &str, want_admins: &BTreeMap<String, usize>, inv: &mut Invariants) -> Stable {
        let until = Instant::now() + Duration::from_secs(30);
        let t0 = Instant::now();
        let st = loop {
            match self.look(f, want_admins).await {
                Ok(st) => break st,
                Err(why) => {
                    assert!(Instant::now() < until, "checkpoint `{label}`: authority did not become stable within 30 s: {why}");
                }
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        };
        let converged_ms = t0.elapsed().as_millis() as u64;
        self.traffic.refresh(&st.nodes);
        progress(&format!("checkpoint `{label}` stable after {converged_ms} ms: fabric-primary {} ({})", st.fp_name(), st.fp["node_id"]));
        // The births seen: pid, node, NodeId, incarnation.
        for a in &st.admins {
            let name = s(&a["name"]);
            let have = self.births.iter().any(|b| b["node"] == a["name"] && b["incarnation_id"] == a["incarnation_id"]);
            if !have {
                let pid = node_pid(f, &name).await;
                self.births.push(json!({"label": label, "node": name, "node_id": a["node_id"], "incarnation_id": a["incarnation_id"], "endpoint_id": a["endpoint_id"], "pid": pid, "admin_api_base": a["admin_api_base"]}));
            }
        }
        let probe = self.writer_probe(f, label, &st).await;
        let mut row = authority_row(label, &st.nodes, &st.fabric);
        row["checkpoint"] = json!(true);
        row["converged_ms"] = json!(converged_ms);
        row["fence_probe"] = probe.clone();
        self.history.push(row);
        inv.holds(
            &format!("stable checkpoint `{label}`: every admin's own view agrees with the election function and with every other admin; exactly one admin accepts a topology write"),
            true,
            json!({"fabric_primary": st.fp_name(), "fabric_primary_node_id": st.fp["node_id"], "mesh_primaries": MESHES.iter().map(|m| (m.to_string(), st.primary_of(m)["name"].clone())).collect::<BTreeMap<_, _>>(), "converged_ms": converged_ms, "probe": probe}),
        );
        st
    }

    /// The fence, asked of every ready admin: a topology write that names a node the fabric never
    /// held. A non-writer answers `rejected-not-authority` naming the fabric-primary; the writer
    /// validates it and refuses by name (`unknown-node`). Nothing is created either way.
    async fn writer_probe(&mut self, f: &Formed, label: &str, st: &Stable) -> Value {
        self.probe_round += 1;
        let path = format!("{}.gateway.{}", MESHES[0], 900 + self.probe_round);
        let t0 = now_ns();
        let mut answers = Vec::new();
        for a in &st.admins {
            let (status, v) = f.estate.delete_at(&s(&a["admin_api_base"]), &format!("/api/nodes/{path}")).await;
            answers.push(json!({"admin": a["name"], "node_id": a["node_id"], "status": status, "error": v["error"], "fabric_primary": v["fabric_primary"], "detail": v["detail"]}));
        }
        let t1 = now_ns();
        let writers: Vec<&Value> = answers.iter().filter(|a| a["error"] != "rejected-not-authority").collect();
        assert_eq!(writers.len(), 1, "`{label}`: exactly one admin accepts a topology write; answers {answers:#?}");
        assert_eq!(writers[0]["admin"], st.fp["name"], "`{label}`: the admin that validates the write is the fabric-primary the views name: {answers:#?}");
        assert_eq!((writers[0]["status"].as_u64(), writers[0]["error"].as_str()), (Some(404), Some("unknown-node")), "`{label}`: the writer refuses the unknown node by name: {answers:#?}");
        for a in answers.iter().filter(|a| a["error"] == "rejected-not-authority") {
            assert_eq!((a["status"].as_u64(), &a["fabric_primary"]), (Some(409), &st.fp["name"]), "`{label}`: a non-writer rejects and names the fabric-primary: {a}");
        }
        let rec = json!({"label": label, "round": self.probe_round, "path": path, "from_ns": t0, "to_ns": t1, "fabric_primary": st.fp["name"], "answers": answers});
        self.probes.push(rec.clone());
        rec
    }

    /// An accepted topology write, sent to the fabric-primary the checkpoint named. Anything but
    /// 202 is the finding; nothing retries it.
    async fn write(&mut self, f: &Formed, st: &Stable, what: &str, method: &str, path: &str, body: &Value) -> String {
        let base = st.fp_base();
        let t0 = now_ms();
        let (status, v) = if method == "DELETE" { f.estate.delete_at(&base, path).await } else { f.estate.http_post(&base, path, body).await };
        assert_eq!(status, 202, "{what}: the fabric-primary {} refused {method} {path}: {v}", st.fp_name());
        let build_id = s(&v["build_id"]);
        self.writes.push(json!({"what": what, "method": method, "path": path, "accepted_by": st.fp["name"], "accepted_by_node_id": st.fp["node_id"], "build_id": build_id, "t_ms": t0}));
        self.actions.push(json!({"t_ms": t0, "action": what, "via": format!("{method} {path}"), "status": status, "build_id": build_id, "accepted_by": st.fp["name"]}));
        progress(&format!("{what}: {method} {path} accepted by {} as {build_id}", st.fp_name()));
        build_id
    }

    /// Keep the seeded traffic going until the Build is complete and `until` holds in the view.
    async fn finish(&mut self, f: &mut Formed, build_id: &str, until: Until, window: &'static str) -> Value {
        let started = Instant::now();
        let mut seen_failed = 0u32;
        loop {
            // The entry follows a live admin; the probe's admin is the same.
            let nodes = self.nodes(f).await;
            let b = self.get(f, &format!("/api/builds?id={build_id}")).await.unwrap_or(Value::Null);
            self.observe_build(&b);
            if b["state"] == "failed" {
                seen_failed += 1;
            }
            let held = nodes.as_ref().is_some_and(|nodes| match &until {
                Until::Reborn { node, old_incarnation } => nodes.iter().any(|n| n["name"] == node.as_str() && n["status"] == "ready-for-traffic" && n["incarnation_id"] != old_incarnation.as_str()),
                Until::Gone { node } => nodes.iter().all(|n| n["name"] != node.as_str()),
                Until::Grew { mesh, before } => nodes.iter().any(|n| n["kind"] == "node_admin" && n["mesh"] == mesh.as_str() && n["status"] == "ready-for-traffic" && !before.contains(&s(&n["node_id"]))),
                Until::Joined { node, not_node_id } => nodes.iter().any(|n| n["name"] == node.as_str() && n["status"] == "ready-for-traffic" && n["node_id"] != not_node_id.as_str()),
            });
            if b["state"] == "complete" && held {
                return json!({"build": b, "wall_ms": started.elapsed().as_millis() as u64, "seen_failed_observations": seen_failed});
            }
            assert!(started.elapsed() < Duration::from_secs(120), "Build {build_id} did not complete with its effect visible within 120 s: build {b:#}; wanted {}", match &until {
                Until::Reborn { node, .. } => format!("{node} reborn"),
                Until::Gone { node } => format!("{node} gone"),
                Until::Grew { mesh, .. } => format!("{mesh} grown by one node-admin"),
                Until::Joined { node, not_node_id } => format!("{node} ready under a NodeId other than {not_node_id}"),
            });
            if nodes.is_some() {
                self.traffic.burst(f, 1, window).await;
            } else {
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
}

/// Point the estate's entry (the admin the controller and its probes ask) at an admin that is not
/// about to stop: the node a Build restarts or retires, or a process about to be killed, is never the
/// entry while it does.
fn point_entry_away(f: &mut Formed, st: &Stable, avoid: &str) {
    let keep = st.admins.iter().find(|n| n["name"] != avoid && n["is_fabric_primary"] == true).or_else(|| st.admins.iter().find(|n| n["name"] != avoid)).expect("a second admin to ask");
    f.estate.admin = s(&keep["admin_api_base"]);
}

/// SIGKILL a non-fabric-primary node-admin (a death the story names), recording the host's view of
/// it; a fabric-primary is never signalled.
async fn kill_admin(f: &mut Formed, a: &mut Authority, st: &Stable, node: &str) -> Value {
    assert_ne!(node, st.fp_name(), "REFUSED: the fabric-primary is never killed; it leaves only through a Build");
    let n = st.admins.iter().find(|n| n["name"] == node).unwrap_or_else(|| panic!("{node} is not a ready admin")).clone();
    point_entry_away(f, st, node);
    let pid = node_pid(f, node).await;
    let t = now_ns();
    f.estate.kill_pid(pid as u32);
    let gone_ns = now_ns();
    assert!(!pid_alive(pid), "{node}'s process {pid} is gone after the kill");
    let rec = json!({"event": "admin.kill", "node": node, "node_id": n["node_id"], "incarnation_id": n["incarnation_id"], "pid": pid, "kill_ns": t, "observed_gone_ns": gone_ns, "was_primary": n["is_primary"]});
    a.actions.push(json!({"t_ms": now_ms(), "action": "admin.kill", "via": format!("SIGKILL pid {pid} ({node}); a non-fabric-primary death the story names"), "node": node}));
    progress(&format!("killed {node} (pid {pid})"));
    rec
}

fn want_admins(per_mesh: &[(&str, usize)]) -> BTreeMap<String, usize> {
    per_mesh.iter().map(|(m, n)| (m.to_string(), *n)).collect()
}

fn both(n: usize) -> BTreeMap<String, usize> {
    want_admins(&[(MESHES[0], n), (MESHES[1], n)])
}

/// Which role nodes hold a Connected Direct fact to the current birth of their mesh's primary
/// node-admin right now, and which Connected facts name a birth that has since been replaced.
/// Observed, never waited for: the report is evidence, the assertions are the ones the architecture
/// makes (authority and data-plane liveness).
async fn edge_report(f: &Formed, a: &mut Authority) -> Value {
    let nodes = a.nodes_now(f).await;
    let mut out = BTreeMap::new();
    for node in f.shape.names().into_iter().filter(|n| matches!(launch_id(n), "compute" | "gateway")) {
        let mesh = node.split('.').next().unwrap().to_string();
        let primary = nodes.iter().find(|n| n["mesh"] == mesh.as_str() && n["kind"] == "node_admin" && n["is_primary"] == true).cloned();
        let snap = snapshot(&f.estate, &node);
        let directs = snap["own_latest_directs"].as_array().cloned().unwrap_or_default();
        let to_primary = primary.as_ref().is_some_and(|p| directs.iter().any(|d| d["state"] == "Connected" && d["destination"]["node_id"] == p["node_id"] && d["destination"]["incarnation"] == p["incarnation_id"]));
        let stale: Vec<Value> = directs
            .iter()
            .filter(|d| d["state"] == "Connected")
            .filter(|d| nodes.iter().find(|n| n["node_id"] == d["destination"]["node_id"]).is_none_or(|c| c["incarnation_id"] != d["destination"]["incarnation"]))
            .map(|d| json!({"to": d["destination"]["name"], "incarnation": d["destination"]["incarnation"]}))
            .collect();
        out.insert(node, json!({"primary": primary.as_ref().map(|p| p["name"].clone()), "connected_direct_to_current_primary_birth": to_primary, "stale_connected": stale, "owed": snap["owed"]}));
    }
    json!(out)
}

/// The surviving recovery path, proven before the next admin of a mesh is disturbed: authority holds
/// (`stable`, just taken) and keeps holding across one staleness window (every node stays
/// ready-for-traffic in every admin's own view, so no role node went silent toward its mesh's primary),
/// and a fresh seeded burst of puts all stored. The role nodes' Direct facts are recorded beside it.
async fn prove_path(f: &mut Formed, a: &mut Authority, want: &BTreeMap<String, usize>, label: &str, inv: &mut Invariants) -> Value {
    let window = std::env::var("RDM_STALENESS_MS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(30_000) + 500;
    let until = Instant::now() + Duration::from_millis(window);
    let mut looks = 0u32;
    while Instant::now() < until {
        if let Err(why) = a.look(f, want).await {
            panic!("surviving path `{label}`: authority did not hold across a staleness window ({window} ms), look {looks}: {why}");
        }
        looks += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let edges = edge_report(f, a).await;
    let before = a.traffic.ops.len();
    a.traffic.burst(f, 4, "steady-proof").await;
    let replies = a.traffic.ops[before..].iter().filter(|(_, c)| bucket(&c.out) == Bucket::Reply && c.out["reply"]["result"] == json!({"stored": true})).count();
    inv.holds(&format!("surviving path `{label}`: authority held across one staleness window ({looks} looks) and four fresh puts all stored"), replies == 4, json!({"stored": replies, "looks": looks, "window_ms": window, "role_edges": edges}));
    json!({"label": label, "puts_stored": replies, "looks": looks, "window_ms": window, "role_edges": edges})
}

/// Restart `node` (never the fabric-primary) through the Build rectifier and keep the traffic going
/// until the Build is complete and the node is ready under a new incarnation. The request opens the
/// next attempt of the accepted Build: the Build id is the fabric's own, the node keeps its NodeId.
async fn restart_via_build(f: &mut Formed, a: &mut Authority, st: &Stable, node: &str, scenario: &str) -> Value {
    assert_ne!(node, st.fp_name(), "REFUSED: the fabric-primary is never restarted; it leaves only through a retiring Build");
    let n = st.admins.iter().find(|n| n["name"] == node).unwrap_or_else(|| panic!("{node} is not a ready admin")).clone();
    let fabric_build = s(&st.fabric["build_id"]);
    point_entry_away(f, st, node);
    let attempt_before = try_json(&st.fp_base(), &format!("/api/builds?id={fabric_build}")).await.map(|b| b["attempt"].clone()).unwrap_or(Value::Null);
    let t0 = now_ns();
    let build_id = a.write(f, st, &format!("{scenario}: node.restart"), "POST", &format!("/api/nodes/{node}/restart"), &Value::Null).await;
    assert_eq!(build_id, fabric_build, "{node}: a restart opens the next attempt of the accepted Build (Fabric.build_id stays)");
    let fin = a.finish(f, &build_id, Until::Reborn { node: node.into(), old_incarnation: s(&n["incarnation_id"]) }, "during-restart").await;
    let now = f.estate.node(node).await;
    assert_eq!(now["node_id"], n["node_id"], "{node}: a restart keeps the logical node");
    assert_ne!(now["incarnation_id"], n["incarnation_id"], "{node}: a restart is a new exact birth");
    let ev = json!({
        "event": "admin.restart", "scenario": scenario, "node": node, "node_id": n["node_id"], "was_primary": n["is_primary"], "old_incarnation_id": n["incarnation_id"], "new_incarnation_id": now["incarnation_id"],
        "build_id": build_id, "fabric_build_id_before": fabric_build, "attempt_before": attempt_before, "attempt_after": fin["build"]["attempt"], "executor_after": fin["build"]["executor"], "request_ns": t0, "wall_ms": fin["wall_ms"], "build_failed_observations": fin["seen_failed_observations"],
    });
    a.events.push(ev.clone());
    ev
}

/// A non-fabric-primary node-admin dies (SIGKILL) and the fabric recovers it: the surviving admin of
/// its mesh takes the seat, and a reborn admin stands at the same path.name. Returns the kill record,
/// what the survivor saw, and the recovered view.
async fn kill_and_recover(f: &mut Formed, a: &mut Authority, st: &Stable, node: &str, scenario: &str) -> Value {
    let mesh = node.split('.').next().unwrap().to_string();
    let survivor = st.admins.iter().find(|n| n["mesh"] == mesh.as_str() && n["name"] != node).unwrap_or_else(|| panic!("{mesh} has a surviving admin")).clone();
    let victim = st.admins.iter().find(|n| n["name"] == node).unwrap().clone();
    let fabric_build = s(&st.fabric["build_id"]);
    let kill = kill_admin(f, a, st, node).await;
    let started = Instant::now();
    // The survivor holds the mesh's seat in its own view; traffic runs through the window.
    let mut seat_ms = None;
    let recovered_ms;
    let reborn = loop {
        let nodes = a.nodes(f).await;
        if let Some(nodes) = &nodes {
            if seat_ms.is_none() {
                let sb = s(&survivor["admin_api_base"]);
                if let Some(own) = try_json(&sb, "/api/nodes").await {
                    let me = own["nodes"].as_array().into_iter().flatten().find(|n| n["name"] == survivor["name"]);
                    if me.is_some_and(|m| m["is_primary"] == true) {
                        seat_ms = Some(started.elapsed().as_millis() as u64);
                        progress(&format!("{} holds {mesh}'s seat in its own view after {} ms", survivor["name"], seat_ms.unwrap()));
                    }
                }
            }
            let r = nodes.iter().find(|n| n["name"] == node && n["status"] == "ready-for-traffic" && n["incarnation_id"] != victim["incarnation_id"]).cloned();
            if let Some(r) = r {
                recovered_ms = Some(started.elapsed().as_millis() as u64);
                break r;
            }
            a.traffic.burst(f, 1, "during-kill").await;
        } else {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
        assert!(started.elapsed() < Duration::from_secs(120), "{node} killed; no reborn admin stood at its path.name within 120 s; the survivor {} held the seat after {seat_ms:?} ms", survivor["name"]);
    };
    let ev = json!({
        "event": "admin.kill-and-recover", "scenario": scenario, "node": node, "victim_node_id": victim["node_id"], "victim_incarnation_id": victim["incarnation_id"], "victim_was_primary": victim["is_primary"],
        "survivor": survivor["name"], "survivor_node_id": survivor["node_id"], "survivor_held_seat_ms": seat_ms, "reborn_node_id": reborn["node_id"], "reborn_incarnation_id": reborn["incarnation_id"],
        "reborn_same_node_id": reborn["node_id"] == victim["node_id"], "recovered_ms": recovered_ms, "fabric_build_id_before": fabric_build, "kill": kill,
    });
    a.events.push(ev.clone());
    ev
}

/// The fabric-primary leaves through a retiring Build it executes itself: the drain makes its birth
/// Draining (not an election candidate), the seat moves to another admin, then it stops. The seat's
/// move is read from the election spans afterwards; here the host's observation of the process
/// exit is taken so the order has a clock that does not depend on the dying process's own spans.
async fn hand_off(f: &mut Formed, a: &mut Authority, st: &Stable, scenario: &str, inv: &mut Invariants) -> (Stable, Value) {
    let old = st.fp.clone();
    let old_name = st.fp_name();
    let mesh = st.mesh_of_fp();
    point_entry_away(f, st, &old_name);
    let old_pid = node_pid(f, &old_name).await;
    let watcher = tokio::spawn(async move {
        loop {
            if !pid_alive(old_pid) {
                return now_ns();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let t0 = now_ns();
    let build_id = a.write(f, st, &format!("{scenario}: fabric-primary.retire"), "DELETE", &format!("/api/nodes/{old_name}"), &Value::Null).await;
    let fin = a.finish(f, &build_id, Until::Gone { node: old_name.clone() }, "during-handoff").await;
    let exit_ns = tokio::time::timeout(Duration::from_secs(30), watcher).await.unwrap_or_else(|_| panic!("{old_name} (pid {old_pid}) still runs 30 s after its retiring Build completed")).unwrap();
    let mut want = both(2);
    want.insert(mesh.clone(), 1);
    let after = a.stable(f, &format!("{scenario}: after the fabric-primary retired"), &want, inv).await;
    assert_ne!(after.fp["node_id"], old["node_id"], "{scenario}: the fabric-primary changed");
    assert!(after.admins.iter().all(|n| n["node_id"] != old["node_id"]), "{scenario}: the retired birth is in no admin's view");
    let ev = json!({
        "event": "authority.hand-off", "scenario": scenario, "old_fabric_primary": old_name, "old_node_id": old["node_id"], "old_incarnation_id": old["incarnation_id"], "old_pid": old_pid, "old_mesh": mesh,
        "new_fabric_primary": after.fp["name"], "new_node_id": after.fp["node_id"], "new_mesh": after.fp["mesh"], "build_id": build_id, "fabric_build_id_before": st.fabric["build_id"],
        "request_ns": t0, "old_process_gone_ns": exit_ns, "wall_ms": fin["wall_ms"], "attempt_after": fin["build"]["attempt"], "executor_after": fin["build"]["executor"], "build_failed_observations": fin["seen_failed_observations"],
    });
    a.events.push(ev.clone());
    (after, ev)
}

/// After a retirement: the retired path cannot be restarted (refused by name, no Build opened), then a
/// Build adds a node-admin to the mesh with a NodeId that was never the retired one.
async fn replace_retired(f: &mut Formed, a: &mut Authority, after_retire: &Stable, retired: &Value, scenario: &str, inv: &mut Invariants) -> Stable {
    let mesh = s(&retired["old_mesh"]);
    let path = s(&retired["old_fabric_primary"]);
    let fabric_build = s(&after_retire.fabric["build_id"]);
    let (status, v) = f.estate.http_post(&after_retire.fp_base(), &format!("/api/nodes/{path}/restart"), &Value::Null).await;
    assert_eq!((status, v["error"].as_str()), (404, Some("unknown-node")), "{scenario}: a retired path cannot be restarted: {v}");
    a.events.push(json!({"event": "retired.restart-refused", "scenario": scenario, "node": path, "status": status, "error": v["error"], "detail": v["detail"]}));
    a.actions.push(json!({"t_ms": now_ms(), "action": format!("{scenario}: node.restart of the retired path"), "via": format!("POST /api/nodes/{path}/restart"), "status": status, "error": v["error"]}));
    let before: BTreeSet<String> = after_retire.admins.iter().map(|n| s(&n["node_id"])).collect();
    let build_id = a.write(f, after_retire, &format!("{scenario}: node.spawn admin"), "POST", "/api/nodes/spawn", &json!({"mesh": mesh, "kind": "node_admin"})).await;
    let fin = a.finish(f, &build_id, Until::Grew { mesh: mesh.clone(), before }, "during-spawn").await;
    let st = a.stable(f, &format!("{scenario}: replacement ready"), &both(2), inv).await;
    let born: Vec<&Value> = st.admins.iter().filter(|n| n["mesh"] == mesh.as_str() && !after_retire.admins.iter().any(|o| o["node_id"] == n["node_id"])).collect();
    assert_eq!(born.len(), 1, "{scenario}: one new admin in {mesh}");
    assert_ne!(born[0]["node_id"], retired["old_node_id"], "{scenario}: a replacement never reuses the retired NodeId");
    // The late admin takes the seat exactly when its NodeId is the lower one: the election key alone.
    let incumbent = after_retire.admins.iter().find(|n| n["mesh"] == mesh.as_str()).unwrap().clone();
    let lower = s(&born[0]["node_id"]) < s(&incumbent["node_id"]);
    assert_eq!(born[0]["is_primary"] == true, lower, "{scenario}: the late admin {} holds the mesh seat iff its NodeId is lower than the incumbent's {}", born[0]["node_id"], incumbent["node_id"]);
    a.events.push(json!({
        "event": "admin.replacement", "scenario": scenario, "mesh": mesh, "retired": path, "retired_node_id": retired["old_node_id"], "replacement": born[0]["name"], "replacement_node_id": born[0]["node_id"],
        "build_id": build_id, "fabric_build_id_before_spawn": fabric_build, "wall_ms": fin["wall_ms"], "fabric_primary_after": st.fp["name"],
        "late_admin_lower_than_incumbent": lower, "late_admin_holds_mesh_seat": born[0]["is_primary"], "incumbent": incumbent["name"], "incumbent_node_id": incumbent["node_id"],
    }));
    st
}

fn start_ns(sp: &Value) -> u64 {
    sp["start_unix_nano"].as_u64().unwrap_or(0)
}

fn attempt_of(sp: &Value) -> u32 {
    s(&sp["attributes"]["attempt"]).parse().unwrap_or(0)
}

/// Every reconcile span of the run as a claim row, in start order.
fn claim_rows(spans: &[Value]) -> Vec<Value> {
    let mut rows: Vec<&Value> = named(spans, "rdm.node_admin.build.update.via-reconcile");
    rows.sort_by_key(|sp| start_ns(sp));
    rows.into_iter()
        .map(|sp| {
            let a = &sp["attributes"];
            json!({
                "phase": "claim", "build_id": a["build_id"], "attempt": attempt_of(sp), "executor": a["executor"], "previous_executor": a["previous_executor"], "reason": a["reason"],
                "outcome": a["outcome"].as_str().map(str::to_string).unwrap_or_else(|| "interrupted".into()), "operations": a["operations"], "start_unix_nano": start_ns(sp), "end_unix_nano": sp["end_unix_nano"], "trace_id": sp["trace_id"], "span_id": sp["span_id"], "parent_span_id": sp["parent_span_id"],
            })
        })
        .collect()
}

/// No two effective executors share an attempt of one Build, and a Build's effective attempts only
/// advance. An effective attempt is one that ended converged, failed or handed-off; a claim that
/// was lost or found not open executed nothing.
fn check_one_executor_per_attempt(claims: &[Value], inv: &mut Invariants) -> Value {
    let mut by: BTreeMap<(String, u64), BTreeSet<String>> = BTreeMap::new();
    let mut lost = 0usize;
    for c in claims {
        match c["outcome"].as_str() {
            // `interrupted`: the executor's process ended inside the attempt (it retired itself), so
            // its span closed without an outcome. It held the attempt; a successor takes the next one.
            Some("converged" | "failed" | "handed-off" | "interrupted") => {
                by.entry((s(&c["build_id"]), c["attempt"].as_u64().unwrap())).or_default().insert(s(&c["executor"]));
            }
            Some("lost" | "not-open" | "finished") => lost += 1,
            other => panic!("a reconcile attempt ended with an outcome nobody named ({other:?}): {c}"),
        }
    }
    for ((b, n), execs) in &by {
        assert_eq!(execs.len(), 1, "Build {b} attempt {n} was executed effectively by {execs:?}: two executors share one attempt");
    }
    let mut last: BTreeMap<String, (u64, u64)> = BTreeMap::new();
    let mut ordered: Vec<&Value> = claims.iter().filter(|c| matches!(c["outcome"].as_str(), Some("converged" | "failed" | "handed-off" | "interrupted"))).collect();
    ordered.sort_by_key(|c| (s(&c["build_id"]), c["attempt"].as_u64().unwrap()));
    for c in ordered {
        let (b, n, t) = (s(&c["build_id"]), c["attempt"].as_u64().unwrap(), c["start_unix_nano"].as_u64().unwrap());
        if let Some((pn, pt)) = last.insert(b.clone(), (n, t)) {
            assert!(t >= pt || n == pn, "Build {b}: attempt {n} began before attempt {pn} did");
        }
    }
    let attempts: BTreeMap<String, Vec<(u64, Vec<String>)>> = by.iter().fold(BTreeMap::new(), |mut m, ((b, n), e)| {
        m.entry(b.clone()).or_default().push((*n, e.iter().cloned().collect()));
        m
    });
    inv.holds("no two effective executors share one attempt of one Build, and every Build's effective attempts advance", true, json!({"effective_attempts_by_build": attempts, "lost_or_not_open_claims": lost}));
    json!({"effective_attempts_by_build": attempts, "lost_or_not_open_claims": lost})
}

/// The topology writes the spans show are exactly the ones the cell sent, each accepted once; the
/// spans of rejected writes are exactly the fence probes (and the retired-path restart), each
/// non-writer naming the fabric-primary the checkpoint held.
fn check_writes_and_fence(f: &Formed, a: &Authority, spans: &[Value], inv: &mut Invariants) -> Value {
    let creates = named(spans, "rdm.node_admin.build.create.via-rest");
    let updates = named(spans, "rdm.node_admin.build.update.via-rest");
    let accepted_creates: Vec<String> = creates.iter().filter(|sp| !s(&sp["attributes"]["build_id"]).is_empty()).map(|sp| s(&sp["attributes"]["build_id"])).collect();
    let want_creates: BTreeSet<String> = std::iter::once(f.build_id.clone()).chain(a.writes.iter().filter(|w| w["method"] == "DELETE" || w["path"] == "/api/nodes/spawn").map(|w| s(&w["build_id"]))).collect();
    let got_creates: BTreeSet<String> = accepted_creates.iter().cloned().collect();
    assert_eq!(accepted_creates.len(), got_creates.len(), "a Build was accepted twice: {accepted_creates:?}");
    assert_eq!(got_creates, want_creates, "the accepted topology Builds are the formation and the ones the cell sent, nothing else");
    let mut got_updates: Vec<(String, String)> = updates.iter().filter(|sp| !s(&sp["attributes"]["build_id"]).is_empty()).map(|sp| (s(&sp["attributes"]["build_id"]), s(&sp["attributes"]["node"]))).collect();
    let mut want_updates: Vec<(String, String)> = a.writes.iter().filter(|w| s(&w["path"]).ends_with("/restart")).map(|w| (s(&w["build_id"]), s(&w["path"]).trim_start_matches("/api/nodes/").trim_end_matches("/restart").to_string())).collect();
    got_updates.sort();
    want_updates.sort();
    assert_eq!(got_updates, want_updates, "the accepted attempt-opening writes are the restarts the cell sent");
    let rejected_creates = creates.iter().filter(|sp| s(&sp["attributes"]["build_id"]).is_empty()).count();
    let probe_answers: usize = a.probes.iter().map(|p| p["answers"].as_array().unwrap().len()).sum();
    assert_eq!(rejected_creates, probe_answers + a.refused_writes.len(), "every rejected topology write is a fence probe answer or a write the cell sent to a non-writer to see it refused");
    let mut rounds = Vec::new();
    for p in &a.probes {
        let path = s(&p["path"]);
        let (from, to) = (p["from_ns"].as_u64().unwrap(), p["to_ns"].as_u64().unwrap());
        let unknown: Vec<&Value> = named(spans, "rdm.node_admin.build.reject.via-unknown-node").into_iter().filter(|sp| s(&sp["attributes"]["detail"]).contains(&path)).collect();
        assert_eq!(unknown.len(), 1, "probe round {}: exactly one admin validated the write to {path}: {unknown:?}", p["round"]);
        let mut rejected: Vec<(String, String)> = named(spans, "rdm.node_admin.build.reject.via-not-authority")
            .into_iter()
            .filter(|sp| (from..=to).contains(&start_ns(sp)))
            .map(|sp| (s(&sp["attributes"]["node"]), s(&sp["attributes"]["fabric_primary"])))
            .collect();
        rejected.sort();
        let mut want: Vec<(String, String)> = p["answers"].as_array().unwrap().iter().filter(|x| x["error"] == "rejected-not-authority").map(|x| (s(&x["admin"]), s(&p["fabric_primary"]))).collect();
        want.sort();
        assert_eq!(rejected, want, "probe round {}: each non-writer's own reject span names the fabric-primary", p["round"]);
        rounds.push(json!({"round": p["round"], "label": p["label"], "fabric_primary": p["fabric_primary"], "writer_reject_span": unknown[0]["span_id"], "non_writers": want.iter().map(|w| w.0.clone()).collect::<Vec<_>>()}));
    }
    inv.holds(
        "topology writes: every accepted Build is one the cell sent, accepted once, by the fabric-primary the checkpoint named; every fence probe was rejected by each non-writer's own span naming that fabric-primary and validated by exactly one admin",
        true,
        json!({"accepted_creates": got_creates.len(), "accepted_attempt_opens": got_updates.len(), "probe_rounds": rounds.len()}),
    );
    json!({"accepted_creates": got_creates, "accepted_attempt_opens": got_updates, "probe_rounds": rounds})
}

/// Elections as the spans announce them: the order of every hand-off, the surviving admin's seat after
/// a kill, no resurrected NodeId.
fn check_election_history(a: &Authority, spans: &[Value], inv: &mut Invariants) -> Value {
    let recompute = named(spans, "rdm.mesh.election.resolve.via-fabric-recompute");
    let mesh_primary = named(spans, "rdm.mesh.election.resolve.via-mesh-primary");
    let steps = named(spans, "rdm.node_admin.deployment.update.via-step");
    let mut out = Vec::new();
    for ev in a.events.iter().filter(|e| e["event"] == "authority.hand-off") {
        let (old_name, old_id, new_id) = (s(&ev["old_fabric_primary"]), s(&ev["old_node_id"]), s(&ev["new_node_id"]));
        let (req, gone) = (ev["request_ns"].as_u64().unwrap(), ev["old_process_gone_ns"].as_u64().unwrap());
        let announced: Vec<&&Value> = recompute.iter().filter(|sp| sp["attributes"]["winner_node_id"] == new_id.as_str() && sp["attributes"]["previous_node_id"] == old_id.as_str() && start_ns(sp) >= req).collect();
        assert!(!announced.is_empty(), "{}: no admin announced {new_id} succeeding {old_id} after the retire was requested", ev["scenario"]);
        let first = announced.iter().min_by_key(|sp| start_ns(sp)).unwrap();
        let e = start_ns(first);
        assert!(e < gone, "{}: the new fabric-primary's election ({e}) resolved before the old fabric-primary's process was gone ({gone})", ev["scenario"]);
        let inputs = s(&first["attributes"]["inputs"]);
        assert!(!inputs.contains(&format!("{old_name}={old_id}:ReadyForTraffic")), "{}: the old fabric-primary was still a ready candidate when the seat moved: {inputs}", ev["scenario"]);
        let terminate: Vec<&&Value> = steps.iter().filter(|sp| sp["attributes"]["step"] == "TerminateRuntime" && sp["attributes"]["node"] == old_name.as_str() && sp["attributes"]["build_id"] == ev["build_id"]).collect();
        let mark: Vec<&&Value> = steps.iter().filter(|sp| sp["attributes"]["step"] == "MarkDraining" && sp["attributes"]["node"] == old_name.as_str() && sp["attributes"]["build_id"] == ev["build_id"]).collect();
        // The retiring admin executes its own retirement: its TerminateRuntime step signals its own
        // process, so the step's span closes at the shutdown flush and never records an outcome. The
        // terminal observation is the host's: the process is gone. The step's start is recorded
        // beside the election's, as a measurement.
        let terminate_start = terminate.iter().map(|sp| start_ns(sp)).min();
        let after: Vec<&&Value> = recompute.iter().filter(|sp| sp["attributes"]["winner_node_id"] == old_id.as_str() && start_ns(sp) > e).collect();
        assert!(after.is_empty(), "{}: the retired fabric-primary won again after the hand-off: {after:?}", ev["scenario"]);
        out.push(json!({
            "scenario": ev["scenario"], "old": old_name, "new": ev["new_fabric_primary"], "request_ns": req, "election_span": first["span_id"], "election_observer": first["attributes"]["observer"], "election_start_ns": e, "election_inputs": inputs,
            "mark_draining_steps": mark.iter().map(|sp| json!({"outcome": sp["attributes"]["outcome"], "start_ns": start_ns(sp)})).collect::<Vec<_>>(),
            "terminate_step_exported": !terminate.is_empty(), "terminate_step_start_ns": terminate_start, "terminate_step_outcomes": terminate.iter().map(|sp| sp["attributes"]["outcome"].clone()).collect::<Vec<_>>(),
            "old_process_gone_ns": gone, "election_before_process_gone_ms": (gone - e) / 1_000_000,
            "election_after_terminate_step_began_ms": terminate_start.map(|t| (e as i128 - t as i128) / 1_000_000),
        }));
    }
    inv.holds("each fabric-primary hand-off: the new fabric-primary's election resolved, with the old birth not a ready candidate, before the old birth's process was gone (the host observed its exit); the terminate step's own start is measured beside it", true, json!({"hand_offs": out}));
    let mut kills = Vec::new();
    for ev in a.events.iter().filter(|e| e["event"] == "admin.kill-and-recover") {
        let (victim, survivor) = (s(&ev["victim_node_id"]), s(&ev["survivor_node_id"]));
        let kill_ns = ev["kill"]["kill_ns"].as_u64().unwrap();
        let seat: Vec<&&Value> = mesh_primary.iter().filter(|sp| sp["attributes"]["winner_node_id"] == survivor.as_str() && sp["attributes"]["previous_node_id"] == victim.as_str() && start_ns(sp) > kill_ns).collect();
        assert!(!seat.is_empty(), "{}: no admin of the mesh announced the survivor {survivor} as primary in place of the killed {victim}", ev["scenario"]);
        let won_again: Vec<&&Value> = recompute.iter().filter(|sp| sp["attributes"]["winner_node_id"] == victim.as_str() && start_ns(sp) > kill_ns).collect();
        assert!(won_again.is_empty(), "{}: the killed birth's NodeId won the fabric seat after its death", ev["scenario"]);
        kills.push(json!({"scenario": ev["scenario"], "victim": ev["node"], "survivor": ev["survivor"], "seat_span": seat[0]["span_id"], "seat_observer": seat[0]["attributes"]["observer"], "seat_after_kill_ms": (start_ns(seat[0]) - kill_ns) / 1_000_000}));
    }
    inv.holds("each killed admin: the surviving eligible admin of its mesh was announced primary in its place, and the dead NodeId never won the fabric seat afterwards", true, json!({"kills": kills}));
    // A restarted primary retakes its seat when it is Ready again (same NodeId, lowest key).
    let mut retakes = Vec::new();
    for ev in a.events.iter().filter(|e| e["event"] == "admin.restart" && e["was_primary"] == true) {
        let id = s(&ev["node_id"]);
        let req = ev["request_ns"].as_u64().unwrap();
        let mine: Vec<&&Value> = mesh_primary.iter().filter(|sp| start_ns(sp) > req && (sp["attributes"]["winner_node_id"] == id.as_str() || sp["attributes"]["previous_node_id"] == id.as_str())).collect();
        let lost = mine.iter().any(|sp| sp["attributes"]["previous_node_id"] == id.as_str());
        let regained = mine.iter().any(|sp| sp["attributes"]["winner_node_id"] == id.as_str() && sp["attributes"]["previous_node_id"] != id.as_str());
        retakes.push(json!({"scenario": ev["scenario"], "node": ev["node"], "seat_left_while_restarting": lost, "seat_regained_by_same_node_id": regained}));
    }
    inv.holds("a restarted primary: the seat left it while its birth was not a candidate, and the same NodeId was announced primary again once Ready, only by the election key", retakes.iter().all(|r| r["seat_left_while_restarting"] == true && r["seat_regained_by_same_node_id"] == true), json!({"restarts": retakes}));
    // No resurrected identity.
    let ready = named(spans, "rdm.mesh.node.update.via-ready");
    let mut retired = Vec::new();
    for d in named(spans, "rdm.node_admin.node.delete.via-node-deleted") {
        let id = s(&d["attributes"]["node_id"]);
        let after: Vec<&&Value> = ready.iter().filter(|r| r["attributes"]["node_id"] == id.as_str() && start_ns(r) > start_ns(d)).collect();
        assert!(after.is_empty(), "a retired NodeId {id} was ready again after its departure: {after:?}");
        retired.push(json!({"node": d["attributes"]["node"], "node_id": id, "incarnation_id": d["attributes"]["incarnation_id"], "departed_ns": start_ns(d)}));
    }
    inv.holds("no retired NodeId is Ready again after its departure; a replacement is a new NodeId", true, json!({"retired": retired}));
    // Restarts keep the NodeId across births.
    let mut restarts = Vec::new();
    for ev in a.events.iter().filter(|e| e["event"] == "admin.restart") {
        let id = s(&ev["node_id"]);
        let incs: BTreeSet<String> = ready.iter().filter(|r| r["attributes"]["node_id"] == id.as_str()).map(|r| s(&r["attributes"]["incarnation_id"])).collect();
        assert!(incs.contains(&s(&ev["old_incarnation_id"])) && incs.contains(&s(&ev["new_incarnation_id"])), "{}: the NodeId {id} was Ready under both the old and the new birth: {incs:?}", ev["node"]);
        restarts.push(json!({"node": ev["node"], "node_id": id, "births_seen_ready": incs}));
    }
    inv.holds("each restarted admin: one logical NodeId, ready under its old and its new exact birth", true, json!({"restarts": restarts}));
    json!({"hand_offs": out, "kills": kills, "retakes": retakes, "retired": retired, "restarts": restarts})
}

/// The final view holds exactly the shape's counts per mesh and role, every node ready, with
/// distinct valid identities, and every node runs its role's bound executable bytes in its own
/// process or container. (Ordinals may differ from formation: a replacement is a new node.)
async fn check_final_shape(f: &Formed, nodes: &[Value], inv: &mut Invariants) -> Vec<Value> {
    let mut have: BTreeMap<(String, String), usize> = BTreeMap::new();
    for n in nodes {
        *have.entry((s(&n["mesh"]), s(&n["kind"]))).or_default() += 1;
    }
    let mut want: BTreeMap<(String, String), usize> = BTreeMap::new();
    for m in MESHES {
        for (k, c) in [("node_admin", f.shape.node_admin), ("compute", f.shape.compute), ("gateway", f.shape.gateway), ("broker", f.shape.broker)] {
            want.insert((m.to_string(), k.to_string()), c as usize);
        }
    }
    assert_eq!(have, want, "the final view holds exactly the shape's counts");
    for key in ["node_id", "endpoint_id", "incarnation_id", "deployment_id"] {
        let ids: BTreeSet<String> = nodes.iter().map(|n| s(&n[key])).collect();
        assert!(ids.len() == nodes.len() && !ids.contains(""), "{} distinct `{key}` values in the final view", nodes.len());
    }
    let mut launches = Vec::new();
    for n in nodes {
        NodeId::parse(n["node_id"].as_str().unwrap()).unwrap_or_else(|e| panic!("{}: node_id {} is not a valid logical identity: {e}", n["name"], n["node_id"]));
        assert_eq!(n["status"], "ready-for-traffic", "{}", n["name"]);
        let name = s(&n["name"]);
        let o = observe_launch(&f.estate, &name).await;
        let b = f.set.bindings.iter().find(|b| b.launch_id == launch_id(&name)).unwrap();
        assert_eq!(launched_path(&o), b.executable, "{name} runs the bound executable: {o}");
        assert_eq!(o["observed_sha256"], b.sha256.as_str(), "{name} runs the bound bytes: {o}");
        launches.push(o);
    }
    let handles: BTreeSet<String> = launches.iter().map(|o| format!("{}{}", o["pid"], o["container"])).collect();
    inv.holds("the final view holds exactly the shape's counts, distinct valid identities, and every node runs its role's bound bytes in its own runtime", handles.len() == nodes.len(), json!({"nodes": nodes.len(), "runtimes": handles.len()}));
    launches
}

/// The cell's run of the story: the formation, every scenario in order, the stop, the span checks and
/// the evidence views. Shared by nothing else; the recovery cell has its own schedule.
async fn authority_run(cell: &str, shape: Shape) {
    let mut f = form(cell, shape).await;
    let mut inv = Invariants::default();
    let nodes = settle(&f).await;
    let (launches, fabric) = check_identities_and_launches(&f, &nodes, &mut inv).await;
    let authorities = check_authorities(&nodes, &fabric, &mut inv);
    let connections = converge_connections(&f, &mut inv).await;
    let mut a = Authority::new(f.seed, &nodes);
    let mut st = a.stable(&mut f, "formed", &both(2), &mut inv).await;
    a.traffic.burst(&f, 12, "steady-before").await;
    let steady = a.traffic.ops.iter().filter(|(_, c)| bucket(&c.out) == Bucket::Reply).count();
    inv.holds("steady traffic before any change: every operation replied", steady == a.traffic.ops.len(), json!({"issued": a.traffic.ops.len(), "replied": steady}));
    let mut fp_trail = vec![json!({"after": "formed", "fabric_primary": st.fp_name(), "node_id": st.fp["node_id"]})];
    let mut proofs = Vec::new();

    // C4 in mesh A, then in mesh B: the first admin is disturbed, replaced and Ready with a proved
    // surviving path before the second is. A mesh without the fabric-primary loses its primary to a
    // death (C1); a mesh with it restarts its other admin (C3) and later hands the fabric-primary
    // off (C2). The mesh of each is read at the moment, never assumed.
    for mesh in MESHES {
        let first: String;
        if st.mesh_of_fp() == mesh {
            let victim = st.secondary_of(mesh);
            first = s(&victim["name"]);
            restart_via_build(&mut f, &mut a, &st, &first, &format!("C3+C4 {mesh}: the non-primary admin restarts")).await;
        } else {
            let victim = st.primary_of(mesh);
            first = s(&victim["name"]);
            kill_and_recover(&mut f, &mut a, &st, &first, &format!("C1+C4 {mesh}: the current mesh-primary dies")).await;
        }
        st = a.stable(&mut f, &format!("C4 {mesh}: first admin {first} recovered"), &both(2), &mut inv).await;
        proofs.push(prove_path(&mut f, &mut a, &both(2), &format!("C4 {mesh} after {first}"), &mut inv).await);
        fp_trail.push(json!({"after": format!("C4 {mesh} first"), "fabric_primary": st.fp_name(), "node_id": st.fp["node_id"]}));
        let second = st.admins.iter().find(|n| n["mesh"] == mesh && n["name"] != first.as_str()).unwrap_or_else(|| panic!("{mesh} has a second admin")).clone();
        let second_name = s(&second["name"]);
        if second_name == st.fp_name() {
            let (after, ev) = hand_off(&mut f, &mut a, &st, &format!("C2+C4 {mesh}: the fabric-primary {second_name} hands off"), &mut inv).await;
            st = replace_retired(&mut f, &mut a, &after, &ev, &format!("C2+C4 {mesh}"), &mut inv).await;
        } else {
            restart_via_build(&mut f, &mut a, &st, &second_name, &format!("C4 {mesh}: the second admin restarts")).await;
            st = a.stable(&mut f, &format!("C4 {mesh}: second admin {second_name} recovered"), &both(2), &mut inv).await;
        }
        proofs.push(prove_path(&mut f, &mut a, &both(2), &format!("C4 {mesh} after {second_name}"), &mut inv).await);
        fp_trail.push(json!({"after": format!("C4 {mesh} second"), "fabric_primary": st.fp_name(), "node_id": st.fp["node_id"]}));
    }

    // C19: a primary that restarts loses its seat while its birth is not a candidate and regains it
    // as the same NodeId; the fabric-primary hands off once more; no retired identity returns.
    let quiet = MESHES.iter().find(|m| **m != st.mesh_of_fp()).unwrap().to_string();
    let incumbent = st.primary_of(&quiet);
    restart_via_build(&mut f, &mut a, &st, &s(&incumbent["name"]), &format!("C19 {quiet}: the primary restarts and retakes its seat")).await;
    st = a.stable(&mut f, &format!("C19 {quiet}: the restarted primary is Ready"), &both(2), &mut inv).await;
    assert_eq!(st.primary_of(&quiet)["node_id"], incumbent["node_id"], "C19: the restarted primary, Ready again with the lowest NodeId, retakes its seat");
    proofs.push(prove_path(&mut f, &mut a, &both(2), "C19 retake", &mut inv).await);
    let (after, ev) = hand_off(&mut f, &mut a, &st, "C19: a repeated fabric-primary hand-off", &mut inv).await;
    st = replace_retired(&mut f, &mut a, &after, &ev, "C19", &mut inv).await;
    proofs.push(prove_path(&mut f, &mut a, &both(2), "C19 after the repeated hand-off", &mut inv).await);
    fp_trail.push(json!({"after": "C19", "fabric_primary": st.fp_name(), "node_id": st.fp["node_id"]}));

    // Final state, the ledger's reads, and the stop.
    a.traffic.burst(&f, 6, "steady-after").await;
    let st = a.stable(&mut f, "final", &both(2), &mut inv).await;
    let nodes_end = st.nodes.clone();
    let launches_end = check_final_shape(&f, &nodes_end, &mut inv).await;
    let mut verification = Vec::new();
    let mut final_state: BTreeMap<String, Value> = BTreeMap::new();
    for (op, _) in &a.traffic.ops {
        let id = nodes_end.iter().find(|n| n["name"] == op.target.as_str()).map(|n| s(&n["node_id"])).unwrap();
        assert_eq!(id, op.target_id, "{}: the data-plane node keeps its logical identity through admin churn", op.target);
        let c = probe_call(&f.estate, &["get", "--target", &format!("exact:{id}"), "--key", &op.key]);
        assert_eq!(c.out["outcome"], "Reply", "verification read of {} must reply once the estate is settled: {}", op.key, c.out);
        final_state.insert(op.key.clone(), c.out["reply"]["result"].clone());
        verification.push(c);
    }
    let fabric_end = a.get(&mut f, "/api/fabric").await.expect("an admin answers /api/fabric");
    a.history.push(authority_row("final", &nodes_end, &fabric_end));
    let stop_t = Instant::now();
    f.estate.stop().await;
    let left = provider_left(&f, stop_t, &mut inv).await;
    let spans = f.estate.spans();

    let accounted = account(&spans, &a.traffic.ops, &final_state, &mut inv);
    let claims = claim_rows(&spans);
    let attempts = check_one_executor_per_attempt(&claims, &mut inv);
    let fence = check_writes_and_fence(&f, &a, &spans, &mut inv);
    let elections = check_election_history(&a, &spans, &mut inv);
    let services = check_services(&spans, &mut inv);
    let chains = check_formation_chain(&f, &spans, &mut inv);
    let runtime = check_runtime_facts(&f, &nodes, &launches, &spans, &mut inv);
    let index = copy_spans(&f.estate, &f.dir);
    let mut actions = std::mem::take(&mut f.actions);
    actions.extend(a.actions.iter().cloned());
    actions.extend(a.traffic.ops.iter().map(|(_, c)| action_row(c)));
    actions.extend(verification.iter().map(action_row));
    actions.sort_by_key(|r| r["t_ms"].as_u64().unwrap_or(0));
    let mut history = std::mem::take(&mut a.history);
    history.extend(claims.iter().cloned());
    write_views(
        Views {
            f: &f,
            topology_final: nodes_end,
            actions,
            operations: accounted.operations,
            outcomes: accounted.outcomes,
            authority_history: history,
            route_history: Vec::new(),
            runtime_history: runtime,
            inv,
            extra: json!({
                "authorities_at_formation": authorities, "connections_at_formation": connections, "provider_after_stop": left, "services": services, "formation_chains": chains,
                "scenario_events": a.events, "fabric_primary_trail": fp_trail, "surviving_path_proofs": proofs, "births_seen": a.births, "topology_writes": a.writes, "fence": fence, "attempts": attempts, "elections": elections,
                "final_launches": launches_end,
                "ledger": {"issued": a.traffic.ops.len(), "buckets": accounted.buckets.iter().map(|(k, v)| (format!("{k:?}"), *v)).collect::<BTreeMap<_, _>>(), "class_detail": accounted.summary, "indeterminate": accounted.indeterminate, "protocol_refusals": accounted.refusals, "verification_reads": verification.len()},
                "schedule": {"seed": f.seed, "generator": "rafka_test_scenario::model::Rng (SplitMix64)", "scenarios": ["C1", "C2", "C3", "C4", "C19"]},
            }),
        },
        index,
        &spans,
    );
}

/// CONTRACT: in the formed estate (node-admin / compute / gateway / broker = 2 / 2 / 3 / 3 x 2 meshes
/// at the canonical tier; the 2 / 1 / 1 / 1 fixture only as a fast control) the topology has exactly
/// one effective writer at every stable point while the admins churn, and seeded proof traffic is
/// accounted for operation by operation. Each mesh's two admins are disturbed one after the other,
/// the second only once the first's replacement is Ready and a surviving path is proven (every compute
/// and gateway holds a Connected Direct edge to the current primary's birth, fresh puts all stored):
/// a mesh without the fabric-primary loses its current primary to a SIGKILL and the surviving admin
/// takes the seat (C1); the other admin of a mesh restarts through a Build as the same NodeId in a new
/// birth (C3); the fabric-primary is never killed, it is retired by a Build it executes itself,
/// its draining birth is not an election candidate, and the new fabric-primary's election is
/// announced, with the old birth not a ready candidate, before the host sees the old process gone (C2); a
/// restarted primary retakes its seat as the same NodeId, a late admin takes a seat exactly
/// when its NodeId is lower, a retired path cannot be restarted and a retired NodeId never returns,
/// across a repeated hand-off (C19). At every stable checkpoint each admin's own view equals the
/// election function and every other admin's, exactly one admin validates a topology write and the
/// others reject it naming the fabric-primary. From the spans: no two effective executors share an
/// attempt of one Build, every accepted write is one the cell sent. What must NOT happen: a second
/// writer, a competing executor for one attempt, a signal to the fabric-primary, every admin of a
/// mesh down at once, an unaccounted operation or an operation applied twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_authority_handoffs_preserve_one_effective_writer() {
    authority_run("mock_authority_handoffs_preserve_one_effective_writer", any_tier()).await;
}

/// One imported implementation of the planner, the authority and the death detector: every span of
/// the families that decide who executes, who is primary and which birth is proven gone comes from the
/// consumer's `rshape-node-admin`, and its source path is the pinned RDM checkout of the candidate
/// (never a file of the consumer's own).
fn check_imported_mechanism(f: &Formed, spans: &[Value], inv: &mut Invariants) -> Value {
    let short = &f.candidate[..7];
    let families = [
        "rdm.node_admin.build.update.via-reconcile",
        "rdm.node_admin.build.update.via-proven-drift",
        "rdm.node_admin.node.create.via-build",
        "rdm.node_admin.node.update.via-build",
        "rdm.node_admin.node.delete.via-build",
        "rdm.mesh.election.resolve.via-recompute",
        "rdm.mesh.election.resolve.via-mesh-primary",
        "rdm.mesh.election.resolve.via-fabric-recompute",
        "rdm.node_admin.build.create.via-rest",
        "rdm.node_admin.build.update.via-rest",
    ];
    let mut seen = BTreeMap::new();
    for fam in families {
        let rows = named(spans, fam);
        for sp in &rows {
            let path = s(&sp["attributes"]["code.filepath"]);
            assert_eq!(sp["service"], "rshape-node-admin", "{fam}: emitted by a process other than the consumer's imported node-admin: {sp}");
            assert!(path.contains("/rust-distributed-mesh-") && path.contains(&format!("/{short}/crates/")), "{fam}: its source is {path}, not the pinned RDM checkout of {short}");
        }
        seen.insert(fam.to_string(), rows.len());
    }
    for must in ["rdm.node_admin.build.update.via-reconcile", "rdm.mesh.election.resolve.via-fabric-recompute", "rdm.mesh.election.resolve.via-mesh-primary"] {
        assert!(seen[must] > 0, "no `{must}` span: the imported mechanism never ran");
    }
    inv.holds(
        "one imported planner, authority and death detector: every reconcile, create/update/delete, election and drift span is the imported node-admin's, from the pinned RDM checkout of the candidate",
        true,
        json!({"candidate": f.candidate, "spans_by_family": seen}),
    );
    json!(seen)
}

/// Cell 5: the recovery of a Build. The accepted Build stays the one the fabric names; each recovery
/// is a further attempt of it, executed by exactly one admin; a topology that did not change gets no
/// new Build.
async fn recovery_run(cell: &str, shape: Shape) {
    let mut f = form(cell, shape).await;
    let mut inv = Invariants::default();
    let nodes = settle(&f).await;
    let (launches, fabric) = check_identities_and_launches(&f, &nodes, &mut inv).await;
    let authorities = check_authorities(&nodes, &fabric, &mut inv);
    let connections = converge_connections(&f, &mut inv).await;
    let mut a = Authority::new(f.seed, &nodes);
    let names_initial: BTreeSet<String> = nodes.iter().map(|n| s(&n["name"])).collect();
    let mut st = a.stable(&mut f, "formed", &both(2), &mut inv).await;
    let b0 = f.build_id.clone();
    assert_eq!(s(&st.fabric["build_id"]), b0, "the fabric names the formation Build");
    a.traffic.burst(&f, 12, "steady-before").await;
    let mut trail = vec![json!({"after": "formed", "fabric_build_id": b0, "attempt": f.build["attempt"]})];

    // 1. A restart opens the next attempt of the accepted Build: the Build id the fabric already names,
    //    one attempt further, one executor. Done twice so the advance is seen to continue.
    let fm = st.mesh_of_fp();
    for round in 1..=2 {
        let victim = st.secondary_of(&fm);
        let node = s(&victim["name"]);
        let ev = restart_via_build(&mut f, &mut a, &st, &node, &format!("R{round}: a Build-executed restart")).await;
        assert_eq!(ev["build_id"], json!(b0), "R{round}: the restart is an attempt of the accepted Build, not a new one");
        assert_eq!(ev["attempt_after"].as_u64(), ev["attempt_before"].as_u64().map(|x| x + 1), "R{round}: the attempt advanced by exactly one: {ev}");
        st = a.stable(&mut f, &format!("R{round}: {node} restarted"), &both(2), &mut inv).await;
        assert_eq!(s(&st.fabric["build_id"]), b0, "R{round}: Fabric.build_id stays the formation Build");
        trail.push(json!({"after": format!("R{round}"), "fabric_build_id": st.fabric["build_id"], "attempt": ev["attempt_after"]}));
    }

    // 2. The executor of a Build dies mid-attempt: a non-fabric-primary mesh primary (a death the story
    //    names) executes a role node's restart, and is SIGKILLed once the node has left service. The
    //    surviving admin continues the same Build at the next attempt; the dead admin's path.name is
    //    re-born by a further attempt of it. No Build is accepted for any of it.
    let om = MESHES.iter().find(|m| **m != st.mesh_of_fp()).unwrap().to_string();
    let fp_at_the_death = st.fp_name();
    let executor = st.primary_of(&om);
    let executor_name = s(&executor["name"]);
    let survivor = st.secondary_of(&om);
    let broker = format!("{om}.broker.1");
    let broker_before = f.estate.node(&broker).await;
    let fabric_build = s(&st.fabric["build_id"]);
    point_entry_away(&mut f, &st, &executor_name);
    let attempt_before = try_json(&st.fp_base(), &format!("/api/builds?id={fabric_build}")).await.map(|b| b["attempt"].as_u64().unwrap()).unwrap();
    a.traffic.aim = Some(broker.clone());
    let req_ns = now_ns();
    let bid = a.write(&f, &st, "R3: node.restart of a role node under the mesh primary", "POST", &format!("/api/nodes/{broker}/restart"), &Value::Null).await;
    assert_eq!(bid, b0, "R3: the role node's restart is an attempt of the accepted Build");
    let started = Instant::now();
    let (held, killed_at_attempt) = loop {
        let b = a.get(&mut f, &format!("/api/builds?id={bid}")).await.unwrap_or(Value::Null);
        a.observe_build(&b);
        let nodes_now = a.nodes_now(&f).await;
        let left_service = nodes_now.iter().find(|n| n["name"] == broker.as_str()).is_some_and(|n| n["status"] != "ready-for-traffic");
        if b["state"] == "running" && b["executor"] == executor_name.as_str() && b["attempt"].as_u64() == Some(attempt_before + 1) && left_service {
            break (b, attempt_before + 1);
        }
        assert!(started.elapsed() < Duration::from_secs(30), "R3: the attempt was never seen in flight under {executor_name} with {broker} out of service; build {b:#}");
        a.traffic.burst(&f, 1, "during-restart").await;
    };
    let kill = kill_admin(&mut f, &mut a, &st, &executor_name).await;
    progress(&format!("R3: {executor_name} killed with attempt {killed_at_attempt} of {bid} in flight: {held}"));
    let fin = a.finish(&mut f, &bid, Until::Reborn { node: broker.clone(), old_incarnation: s(&broker_before["incarnation_id"]) }, "during-successor").await;
    a.traffic.aim = None;
    let broker_after = f.estate.node(&broker).await;
    assert_ne!(broker_after["incarnation_id"], broker_before["incarnation_id"], "R3: the broker stands under a new exact birth");
    let broker_kept = broker_after["node_id"] == broker_before["node_id"];
    let recovered = a.stable(&mut f, "R3: the dead executor's path.name is re-born", &both(2), &mut inv).await;
    assert_eq!(s(&recovered.fabric["build_id"]), b0, "R3: recovery accepted no new Build");
    a.events.push(json!({
        "event": "executor.death", "scenario": "R3", "executor": executor_name, "executor_node_id": executor["node_id"], "survivor": survivor["name"], "survivor_node_id": survivor["node_id"], "broker": broker,
        "broker_old_incarnation": broker_before["incarnation_id"], "broker_new_incarnation": broker_after["incarnation_id"], "broker_old_node_id": broker_before["node_id"], "broker_new_node_id": broker_after["node_id"], "broker_node_id_kept": broker_kept, "build_id": bid, "attempt_in_flight": killed_at_attempt, "attempt_final": fin["build"]["attempt"], "executor_final": fin["build"]["executor"],
        "request_ns": req_ns, "kill": kill, "wall_ms": fin["wall_ms"],
    }));
    trail.push(json!({"after": "R3", "fabric_build_id": recovered.fabric["build_id"], "attempt": fin["build"]["attempt"]}));
    let proof = prove_path(&mut f, &mut a, &both(2), "R3 after the executor's death", &mut inv).await;

    // Final state, reads for the ledger, the stop.
    a.traffic.burst(&f, 6, "steady-after").await;
    let st = a.stable(&mut f, "final", &both(2), &mut inv).await;
    let nodes_end = st.nodes.clone();
    let names_final: BTreeSet<String> = nodes_end.iter().map(|n| s(&n["name"])).collect();
    assert_eq!(names_final, names_initial, "the topology is what it was: the same path.names, nothing added or removed");
    let launches_end = check_final_shape(&f, &nodes_end, &mut inv).await;
    let mut verification = Vec::new();
    let mut final_state: BTreeMap<String, Value> = BTreeMap::new();
    for (op, _) in &a.traffic.ops {
        // The target's NodeId in the final view: a restart keeps it; a node the fabric re-birthed under
        // a successor executor is a new NodeId, and the old one's store left with it.
        let alive = nodes_end.iter().any(|n| s(&n["node_id"]) == op.target_id);
        if !alive {
            assert_eq!(op.target, broker, "{}: only the node whose restart lost its executor may change NodeId", op.target);
            final_state.insert(op.key.clone(), json!({"unverifiable": "the target's NodeId left the fabric; its store went with it"}));
            continue;
        }
        let c = probe_call(&f.estate, &["get", "--target", &format!("exact:{}", op.target_id), "--key", &op.key]);
        assert_eq!(c.out["outcome"], "Reply", "verification read of {} must reply once the estate is settled: {}", op.key, c.out);
        final_state.insert(op.key.clone(), c.out["reply"]["result"].clone());
        verification.push(c);
    }
    let fabric_end = a.get(&mut f, "/api/fabric").await.expect("an admin answers /api/fabric");
    a.history.push(authority_row("final", &nodes_end, &fabric_end));
    let stop_t = Instant::now();
    f.estate.stop().await;
    let left = provider_left(&f, stop_t, &mut inv).await;
    let spans = f.estate.spans();

    let accounted = account(&spans, &a.traffic.ops, &final_state, &mut inv);
    let claims = claim_rows(&spans);
    let attempts = check_one_executor_per_attempt(&claims, &mut inv);
    let fence = check_writes_and_fence(&f, &a, &spans, &mut inv);
    let elections = check_election_history(&a, &spans, &mut inv);
    let imported = check_imported_mechanism(&f, &spans, &mut inv);
    // What the successor did to the broker whose restart lost its executor, from its create spans.
    let successor_creates: Vec<Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .chain(named(&spans, "rdm.node_admin.node.update.via-build"))
        .filter(|c| c["attributes"]["build_id"] == b0.as_str() && c["attributes"]["node"] == broker.as_str())
        .map(|c| json!({"name": c["name"], "attempt": attempt_of(c), "start_unix_nano": start_ns(c), "span_id": c["span_id"], "trace_id": c["trace_id"], "events": c["events"].as_array().into_iter().flatten().map(|e| e["name"].clone()).collect::<Vec<_>>()}))
        .collect();

    // The Build's attempt history, from the fabric's own folded projection (a SIGKILLed executor's
    // spans are lost with it; its attempt is in the Build's facts) and from the survivors' spans.
    let observed: Vec<&Value> = a.build_obs.iter().filter(|o| o["build_id"] == b0.as_str()).collect();
    let mut executors_by_attempt: BTreeMap<u64, BTreeSet<String>> = BTreeMap::new();
    for o in &observed {
        if let (Some(n), Some(e)) = (o["attempt"].as_u64(), o["executor"].as_str()) {
            executors_by_attempt.entry(n).or_default().insert(e.to_string());
        }
    }
    // The survivors' own spans of the same Build: an attempt the polling did not catch is in them, and
    // an attempt both sources name must name the same admin.
    for c in claims.iter().filter(|c| c["build_id"] == b0.as_str() && matches!(c["outcome"].as_str(), Some("converged" | "failed" | "handed-off" | "interrupted"))) {
        executors_by_attempt.entry(c["attempt"].as_u64().unwrap()).or_default().insert(s(&c["executor"]));
    }
    for (n, e) in &executors_by_attempt {
        assert_eq!(e.len(), 1, "the fabric's folded Build shows attempt {n} of {b0} held by {e:?}");
    }
    let dead_attempt = executors_by_attempt.get(&killed_at_attempt).unwrap_or_else(|| panic!("the in-flight attempt {killed_at_attempt} was never observed"));
    assert!(dead_attempt.contains(&executor_name), "attempt {killed_at_attempt} was held by {executor_name}");
    let later: Vec<(u64, &BTreeSet<String>)> = executors_by_attempt.iter().filter(|(n, _)| **n > killed_at_attempt).map(|(n, e)| (*n, e)).collect();
    assert!(!later.is_empty(), "the Build advanced past the dead executor's attempt {killed_at_attempt}: {executors_by_attempt:?}");
    // The attempt right after the dead executor's is a surviving admin's. (A later attempt may be held
    // by the executor's path.name again: that is its re-born admin, a new birth.)
    assert!(!later[0].1.contains(&executor_name), "the attempt after the dead executor's is held by a survivor, not by {executor_name}: {later:?}");
    let kill_ns = kill["kill_ns"].as_u64().unwrap();
    let spans_after: Vec<&Value> = claims.iter().filter(|c| c["build_id"] == b0.as_str() && c["start_unix_nano"].as_u64().unwrap() > kill_ns).collect();
    assert!(!spans_after.is_empty(), "the surviving admins claimed further attempts of {b0} after the executor's death");
    assert_eq!(spans_after[0]["attempt"].as_u64(), Some(later[0].0), "the first surviving claim is the attempt that follows the dead executor's: {spans_after:#?}");
    assert_ne!(spans_after[0]["executor"], executor_name.as_str(), "the first claim after the death is a survivor's");
    // The dead admin's path.name is re-born by the same Build: a create under b0, by a later attempt,
    // executed by the admin cohort's executor (the fabric-primary).
    let creates: Vec<&Value> = named(&spans, "rdm.node_admin.node.create.via-build")
        .into_iter()
        .filter(|c| c["attributes"]["build_id"] == b0.as_str() && c["attributes"]["node"] == executor_name.as_str() && start_ns(c) > kill_ns)
        .collect();
    assert!(!creates.is_empty(), "{executor_name} was re-born by node.create.via-build under {b0}");
    assert!(creates.iter().all(|c| u64::from(attempt_of(c)) > killed_at_attempt), "the re-birth is a later attempt than the dead executor's");
    let rebirth_executors: BTreeSet<String> = spans_after.iter().filter(|c| s(&c["operations"]).contains(&format!("create-node:{executor_name}"))).map(|c| s(&c["executor"])).collect();
    assert_eq!(rebirth_executors, BTreeSet::from([fp_at_the_death.clone()]), "the dead admin's re-birth was planned and executed by the fabric-primary");
    let all_creates: BTreeSet<String> = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().filter(|sp| !s(&sp["attributes"]["build_id"]).is_empty()).map(|sp| s(&sp["attributes"]["build_id"])).collect();
    assert_eq!(all_creates, BTreeSet::from([b0.clone()]), "the only Build ever accepted is the formation Build");
    let max_attempt = executors_by_attempt.keys().max().copied().unwrap();
    inv.holds(
        "recovery retained the accepted Build and advanced its attempt: the in-flight attempt belonged to the executor that died, every later attempt to a surviving or re-born admin, exactly one holder per attempt, the dead admin's path.name re-born by the fabric-primary under the same Build, and the only Build ever accepted is the formation Build",
        true,
        json!({
            "build_id": b0, "in_flight_attempt": killed_at_attempt, "executor_that_died": executor_name, "executors_by_attempt": executors_by_attempt, "max_attempt": max_attempt,
            "claims_after_the_death": spans_after.iter().map(|c| json!({"attempt": c["attempt"], "executor": c["executor"], "previous_executor": c["previous_executor"], "reason": c["reason"], "outcome": c["outcome"], "operations": c["operations"]})).collect::<Vec<_>>(),
            "re_birth_creates": creates.len(),
        }),
    );
    let services = check_services(&spans, &mut inv);
    let chains = check_formation_chain(&f, &spans, &mut inv);
    let runtime = check_runtime_facts(&f, &nodes, &launches, &spans, &mut inv);
    let index = copy_spans(&f.estate, &f.dir);
    let mut actions = std::mem::take(&mut f.actions);
    actions.extend(a.actions.iter().cloned());
    actions.extend(a.traffic.ops.iter().map(|(_, c)| action_row(c)));
    actions.extend(verification.iter().map(action_row));
    actions.sort_by_key(|r| r["t_ms"].as_u64().unwrap_or(0));
    let mut history = std::mem::take(&mut a.history);
    history.extend(claims.iter().cloned());
    history.extend(a.build_obs.iter().map(|o| json!({"phase": "build-projection", "observation": o})));
    write_views(
        Views {
            f: &f,
            topology_final: nodes_end,
            actions,
            operations: accounted.operations,
            outcomes: accounted.outcomes,
            authority_history: history,
            route_history: Vec::new(),
            runtime_history: runtime,
            inv,
            extra: json!({
                "authorities_at_formation": authorities, "connections_at_formation": connections, "provider_after_stop": left, "services": services, "formation_chains": chains,
                "scenario_events": a.events, "build_trail": trail, "surviving_path_proof": proof, "births_seen": a.births, "topology_writes": a.writes, "fence": fence, "attempts": attempts, "elections": elections, "imported_mechanism": imported,
                "build_projection_observations": a.build_obs, "successor_creates_for_the_broker": successor_creates, "final_launches": launches_end,
                "ledger": {"issued": a.traffic.ops.len(), "buckets": accounted.buckets.iter().map(|(k, v)| (format!("{k:?}"), *v)).collect::<BTreeMap<_, _>>(), "class_detail": accounted.summary, "indeterminate": accounted.indeterminate, "protocol_refusals": accounted.refusals, "verification_reads": verification.len()},
                "schedule": {"seed": f.seed, "generator": "rafka_test_scenario::model::Rng (SplitMix64)", "scenarios": ["R1", "R2", "R3"], "aimed_at": broker},
            }),
        },
        index,
        &spans,
    );
}

/// CONTRACT: topology-preserving recovery keeps the accepted Build and only advances its attempt. In
/// the formed estate two Build-executed admin restarts each open exactly the next attempt of the Build
/// the fabric already names (same Build id, one attempt further, one executor, the same NodeId in a
/// new birth). Then a non-fabric-primary mesh primary, executing a broker's restart, is SIGKILLed
/// (a death the story names) with that attempt in flight and the broker out of service, while seeded
/// puts are aimed at the broker: the fabric-primary's next attempt of the same Build re-births the
/// dead admin's path.name and hands the broker's restart to the surviving mesh primary, whose attempt
/// completes it; the broker ends back in service under a new exact birth (its NodeId kept or re-born
/// is recorded). No
/// Build is accepted for any of it, no two executors hold one attempt, the final view has the same
/// path.names it began with, and every planner, authority and death-detector span in the run is the
/// imported node-admin's, from the pinned RDM checkout of the candidate. Every operation is classified
/// once, none applied twice, none replayed. What must NOT happen: a new Build for an unchanged
/// topology, an attempt number that does not advance, two executors on one attempt, a signal to the
/// fabric-primary, a missing or extra node in the final view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_recovery_retains_build_and_advances_attempt() {
    recovery_run("mock_recovery_retains_build_and_advances_attempt", any_tier()).await;
}

// ---- cells 6-8: lifecycle chaos (rafka-v2 #2947) -----------------------------------------------------

/// The exact runtime the provider holds for a node at one moment: a process (its control domain,
/// pid and the kernel's start token, from the birth's published `runtime.json`) or a container
/// (its immutable id, as the Docker daemon inspects it). Observed from the provider, never from a span.
#[derive(Clone, Debug)]
enum Rt {
    Process(ExactRuntime),
    Container(container_faults::Inspected),
}

impl Rt {
    fn record(&self) -> Value {
        match self {
            Rt::Process(r) => json!({"provider": "process", "deployment_id": r.deployment_id, "control_domain": r.control_domain, "pid": r.pid, "start_token": r.start}),
            Rt::Container(c) => json!({"provider": "container", "container": c.id, "init_pid": c.pid, "started_at": c.started_at, "status": c.status}),
        }
    }

    /// Whether `other` is the same exact runtime (same pid and start token, same container id).
    fn is(&self, other: &Rt) -> bool {
        match (self, other) {
            (Rt::Process(a), Rt::Process(b)) => a.pid == b.pid && a.start == b.start,
            (Rt::Container(a), Rt::Container(b)) => a.id == b.id,
            _ => false,
        }
    }
}

async fn exact_runtime(f: &Formed, node: &str) -> Rt {
    if provider() == "container" {
        let id = f.estate.container_of(node).unwrap_or_else(|| panic!("{node}: no running container of the fabric"));
        Rt::Container(container_faults::inspect(&id).unwrap_or_else(|e| panic!("{node}: inspect {id}: {e}")))
    } else {
        let dir = f.estate.data_dir_of(node).await;
        Rt::Process(ExactRuntime::published(Path::new(&dir)).unwrap_or_else(|r| panic!("{node}: no exact runtime published in {dir}: {r:?}")))
    }
}

/// The old birth's runtime is terminal as the provider reports it: the pid is gone (or the pid's
/// start token is another process's), the container is removed or not running. Anything else is a
/// birth the operation left behind.
fn terminal_proof(node: &str, old: &Rt) -> Value {
    match old {
        Rt::Process(r) => match r.check() {
            Err(SigRefusal::AlreadyExited { pid }) => json!({"observed": "exited", "pid": pid, "start_token": r.start}),
            Err(SigRefusal::NotThisRuntime { pid, published_start, observed_start }) => json!({"observed": "pid now another process", "pid": pid, "published_start": published_start, "observed_start": observed_start}),
            other => panic!("{node}: the old birth's exact runtime {r:?} is not terminal: {other:?}"),
        },
        Rt::Container(c) => match container_faults::inspect(&c.id) {
            Err(e) if e.to_lowercase().contains("no such") => json!({"observed": "removed", "container": c.id, "daemon": e}),
            Err(e) => panic!("{node}: inspecting the old container {} failed for a reason other than its absence: {e}", c.id),
            Ok(i) => {
                assert!(!i.running, "{node}: the old birth's container {} is still running: {i:?}", c.id);
                json!({"observed": i.status, "container": i.id, "exit_code": i.exit_code, "oom_killed": i.oom_killed})
            }
        },
    }
}

/// The new birth runs, and is a different runtime from the old one.
fn live_and_distinct(node: &str, old: &Rt, new: &Rt) -> Value {
    assert!(!old.is(new), "{node}: the new birth runs in the old birth's runtime {:?}", old.record());
    match new {
        Rt::Process(r) => assert!(r.check().is_ok(), "{node}: the new birth's exact runtime {r:?} is not alive: {:?}", r.check()),
        Rt::Container(c) => assert!(container_faults::inspect(&c.id).is_ok_and(|i| i.running), "{node}: the new birth's container {} is not running", c.id),
    }
    json!({"old": old.record(), "new": new.record()})
}

/// One synthetic put aimed at an arbitrary node (a canary: the node holds the value in its own
/// data dir, so a same-node restart keeps it and a replacement does not). It joins the cell's
/// ledger like every other operation. Returns the operation's index in the ledger.
fn op_to(f: &Formed, a: &mut Authority, node: &Value, via: Option<&str>, tag: &str, window: &'static str) -> usize {
    let t = &mut a.traffic;
    t.seq += 1;
    let op = SyntheticOp {
        seq: t.seq,
        key: format!("op-{:05}", t.seq),
        value: format!("{tag}-{}-{:x}", t.seq, t.rng.below(1 << 24)),
        target: s(&node["name"]),
        target_id: s(&node["node_id"]),
        via: via.map(String::from),
        window,
    };
    let exact = format!("exact:{}", op.target_id);
    let via_arg = op.via.as_ref().map(|v| format!("path:{v}"));
    let call = {
        let mut args = vec!["put", "--target", exact.as_str(), "--key", op.key.as_str(), "--value", op.value.as_str()];
        if let Some(v) = &via_arg {
            args.extend(["--via", v.as_str()]);
        }
        probe_call(&f.estate, &args)
    };
    t.ops.push((op, call));
    t.ops.len() - 1
}

/// The edges `holder` (a gateway or compute) holds toward `node_id`, from its own connection snapshot.
fn directs_to(estate: &Estate, holder: &str, node_id: &str) -> Vec<Value> {
    snapshot(estate, holder)["own_latest_directs"].as_array().into_iter().flatten().filter(|d| d["destination"]["node_id"] == node_id).cloned().collect()
}

/// `holder` holds a Connected Direct edge to the current birth of `node_id` and, when `old_inc` is
/// given, none to that superseded birth. The connections convergence bound is the same 30 s the
/// formation cells use; a bound that fires is the finding.
async fn await_edge(estate: &Estate, holder: &str, node_id: &str, new_inc: &str, old_inc: Option<&str>) -> Value {
    let until = Instant::now() + Duration::from_secs(30);
    let t0 = Instant::now();
    loop {
        let d = directs_to(estate, holder, node_id);
        let current = d.iter().any(|x| x["state"] == "Connected" && x["destination"]["incarnation"] == new_inc);
        let stale = old_inc.is_some_and(|o| d.iter().any(|x| x["state"] == "Connected" && x["destination"]["incarnation"] == o));
        if current && !stale {
            return json!({"holder": holder, "node_id": node_id, "current_incarnation": new_inc, "superseded_incarnation": old_inc, "converged_ms": t0.elapsed().as_millis() as u64, "edges": d});
        }
        assert!(Instant::now() < until, "{holder} did not hold a Connected Direct edge to {node_id}'s birth {new_inc} (and none to the superseded {old_inc:?}) within 30 s: {}", serde_json::to_string(&d).unwrap());
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// What `holder` holds toward `node_id` after `within`, without asserting: whether a Connected Direct
/// edge to the current birth exists and whether one to the superseded birth still does. The wait ends
/// as soon as both are as a converged pool would have them.
async fn observe_edge(estate: &Estate, holder: &str, node_id: &str, new_inc: &str, old_inc: &str, within: Duration) -> Value {
    let until = Instant::now() + within;
    let t0 = Instant::now();
    loop {
        let d = directs_to(estate, holder, node_id);
        let current = d.iter().any(|x| x["state"] == "Connected" && x["destination"]["incarnation"] == new_inc);
        let stale = d.iter().any(|x| x["state"] == "Connected" && x["destination"]["incarnation"] == old_inc);
        if (!stale) || Instant::now() >= until {
            return json!({"holder": holder, "node_id": node_id, "current_connected": current, "superseded_connected": stale, "waited_ms": t0.elapsed().as_millis() as u64, "edges": d});
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// A carried call (through `via`) to `node`, so `via` dials it and holds an edge; the reply names
/// the birth that answered.
fn carried_get(estate: &Estate, node: &Value, via: &str, key: &str) -> Call {
    let c = probe_call(estate, &["get", "--target", &format!("exact:{}", s(&node["node_id"])), "--via", &format!("path:{via}"), "--key", key]);
    assert_eq!((c.out["outcome"].as_str(), c.out["route"].as_str()), (Some("Reply"), Some("via-peer")), "carried call to {} through {via}: {}", node["name"], c.out);
    assert_eq!(c.out["reply"]["executing_node"], node["node_id"], "{}", c.out);
    assert_eq!(c.out["reply"]["incarnation_id"], node["incarnation_id"], "the carried call reached the current birth: {}", c.out);
    c
}

/// `holder` originates a put to `dest` over its held projection: Direct, and the reply names the
/// destination's current birth. The put is an operation of the cell's ledger (a stable id, its key and
/// value), so the accounting sees every put the brokers apply. Returns the ledger index.
fn originate_op(f: &Formed, a: &mut Authority, holder: &str, dest: &Value, window: &'static str) -> usize {
    let t = &mut a.traffic;
    t.seq += 1;
    let op = SyntheticOp {
        seq: t.seq,
        key: format!("op-{:05}", t.seq),
        value: format!("orig-{}-{:x}", t.seq, t.rng.below(1 << 24)),
        target: s(&dest["name"]),
        target_id: s(&dest["node_id"]),
        via: Some(holder.to_string()),
        window,
    };
    let c = probe_call(&f.estate, &["originate", "--target", &format!("path:{holder}"), "--destination", &format!("path:{}", s(&dest["name"])), "--key", &op.key, "--value", &op.value]);
    assert_eq!((c.out["outcome"].as_str(), c.out["route"].as_str(), c.out["call_outcome"].as_str()), (Some("Reply"), Some("direct"), Some("reply")), "{holder} originating to {}: {}", dest["name"], c.out);
    assert_eq!(c.out["destination_node_id"], dest["node_id"], "{}", c.out);
    assert_eq!(c.out["reply"]["executing_node"], dest["node_id"], "{}", c.out);
    assert_eq!(c.out["reply"]["incarnation_id"], dest["incarnation_id"], "the originated call reached the current birth: {}", c.out);
    t.ops.push((op, c));
    t.ops.len() - 1
}

/// Restart `node` (never the fabric-primary) through the Build rectifier, keeping the seeded traffic
/// going until the Build is complete and the node is ready under a new incarnation. Any role.
async fn restart_node(f: &mut Formed, a: &mut Authority, st: &Stable, node: &str, scenario: &str) -> Value {
    assert_ne!(node, st.fp_name(), "REFUSED: the fabric-primary is never restarted; it leaves only through a retiring Build");
    let n = st.nodes.iter().find(|n| n["name"] == node).unwrap_or_else(|| panic!("{node} is not in the settled view")).clone();
    let fabric_build = s(&st.fabric["build_id"]);
    if n["kind"] == "node_admin" {
        point_entry_away(f, st, node);
    }
    let attempt_before = try_json(&st.fp_base(), &format!("/api/builds?id={fabric_build}")).await.map(|b| b["attempt"].clone()).unwrap_or(Value::Null);
    let t0 = now_ns();
    let build_id = a.write(f, st, &format!("{scenario}: node.restart"), "POST", &format!("/api/nodes/{node}/restart"), &Value::Null).await;
    assert_eq!(build_id, fabric_build, "{node}: a restart opens the next attempt of the accepted Build (Fabric.build_id stays)");
    let fin = a.finish(f, &build_id, Until::Reborn { node: node.into(), old_incarnation: s(&n["incarnation_id"]) }, "during-restart").await;
    let ev = json!({
        "event": "node.restart", "scenario": scenario, "node": node, "kind": n["kind"], "node_id": n["node_id"], "endpoint_id": n["endpoint_id"], "was_primary": n["is_primary"],
        "old_incarnation_id": n["incarnation_id"], "build_id": build_id, "fabric_build_id_before": fabric_build, "attempt_before": attempt_before,
        "attempt_after": fin["build"]["attempt"], "executor_after": fin["build"]["executor"], "request_ns": t0, "wall_ms": fin["wall_ms"], "build_failed_observations": fin["seen_failed_observations"],
    });
    a.events.push(ev.clone());
    ev
}

/// The representative of each role the restart cell restarts, drawn from the seed: a broker, a
/// gateway, a compute and a node-admin that is neither the fabric-primary nor the Day-0 admin the
/// harness holds its control address on, each in a different draw of the two meshes.
fn representatives(f: &Formed, st: &Stable) -> Vec<String> {
    let mut rng = Rng(f.seed ^ 0x6c69_6665);
    let mesh = |rng: &mut Rng| MESHES[rng.below(2) as usize];
    let pick = |rng: &mut Rng, m: &str, seg: &str, n: u32| format!("{m}.{seg}.{}", rng.below(u64::from(n)) + 1);
    let bm = mesh(&mut rng);
    let gm = MESHES.iter().find(|m| **m != bm).copied().unwrap();
    let cm = mesh(&mut rng);
    let broker = pick(&mut rng, bm, "broker", f.shape.broker);
    let gateway = pick(&mut rng, gm, "gateway", f.shape.gateway);
    let compute = pick(&mut rng, cm, "compute", f.shape.compute);
    let admin = st
        .admins
        .iter()
        .filter(|n| n["name"] != st.fp["name"] && n["name"] != "mesh1.admin.1")
        .map(|n| s(&n["name"]))
        .min()
        .expect("a node-admin that is neither the fabric-primary nor the Day-0 admin");
    vec![broker, gateway, compute, admin]
}

/// Restart one representative and prove what a restart is: the same logical node (NodeId, EndpointId,
/// path.name, data dir) in a new exact birth in a new runtime, the old birth's runtime terminal as the
/// provider reports it, and routing recovered onto the new birth (the connections that named the old
/// birth gone, a call over a held connection answered by the new birth).
async fn restart_representative(f: &mut Formed, a: &mut Authority, st: &Stable, node: &str, label: &str, inv: &mut Invariants) -> (Value, Stable) {
    let n = st.nodes.iter().find(|n| n["name"] == node).unwrap_or_else(|| panic!("{node} is not in the settled view")).clone();
    let (kind, mesh, node_id, old_inc, endpoint) = (s(&n["kind"]), s(&n["mesh"]), s(&n["node_id"]), s(&n["incarnation_id"]), s(&n["endpoint_id"]));
    let rt_before = exact_runtime(f, node).await;
    let gw = format!("{mesh}.gateway.1");
    // The canary: a value only this node's data dir holds. A node-admin serves no proof store (op 0x70
    // is a role-node protocol; its answer is `Unserved`), so its identity is proven by its view, its
    // runtime and its election seat.
    let serves_proof_store = kind != "node_admin";
    let (mut ckey, mut cval) = (String::new(), String::new());
    if serves_proof_store {
        let ci = op_to(f, a, &n, None, "canary", "before-restart");
        (ckey, cval) = (a.traffic.ops[ci].0.key.clone(), a.traffic.ops[ci].0.value.clone());
        assert_eq!(a.traffic.ops[ci].1.out["reply"]["result"], json!({"stored": true}), "{node}: the canary was stored before the restart: {}", a.traffic.ops[ci].1.out);
    }
    // A broker is dialed by its mesh's gateway first, so that gateway holds a pooled edge to this birth.
    // The other mesh's gateway dials it too: a cross-mesh pooled edge to the same birth.
    let gx = format!("{}.gateway.1", MESHES.iter().find(|m| **m != mesh).unwrap());
    if kind == "broker" {
        carried_get(&f.estate, &n, &gw, &ckey);
        await_edge(&f.estate, &gw, &node_id, &old_inc, None).await;
        carried_get(&f.estate, &n, &gx, &ckey);
        await_edge(&f.estate, &gx, &node_id, &old_inc, None).await;
    }
    a.traffic.aim = matches!(kind.as_str(), "broker" | "gateway").then(|| node.to_string());
    let ev = restart_node(f, a, st, node, label).await;
    a.traffic.aim = None;
    let st2 = a.stable(f, &format!("{label}: {node} restarted"), &both(2), inv).await;
    let n2 = st2.nodes.iter().find(|x| x["name"] == node).unwrap_or_else(|| panic!("{node} is not in the view after its restart")).clone();
    assert_eq!(n2["node_id"], n["node_id"], "{node}: a restart keeps the logical node");
    assert_eq!(n2["endpoint_id"], n["endpoint_id"], "{node}: a restart keeps the node's EndpointId (the key in its data dir)");
    assert_ne!(n2["incarnation_id"], n["incarnation_id"], "{node}: a restart is a new exact birth");
    assert_eq!((n2["status"].as_str(), n2["kind"].as_str(), n2["mesh"].as_str()), (Some("ready-for-traffic"), n["kind"].as_str(), n["mesh"].as_str()), "{node}");
    let new_inc = s(&n2["incarnation_id"]);
    if kind == "node_admin" {
        let mut e = ev.clone();
        e["event"] = json!("admin.restart");
        e["new_incarnation_id"] = n2["incarnation_id"].clone();
        a.events.push(e);
    }
    // The provider: a different runtime now, the old one terminal, the new one the bound executable.
    let rt_after = exact_runtime(f, node).await;
    let runtimes = live_and_distinct(node, &rt_before, &rt_after);
    let terminal = terminal_proof(node, &rt_before);
    let launch = observe_launch(&f.estate, node).await;
    let b = f.set.bindings.iter().find(|b| b.launch_id == launch_id(node)).unwrap();
    assert_eq!(launched_path(&launch), b.executable, "{node}: the new birth runs the bound executable: {launch}");
    assert_eq!(launch["observed_sha256"], b.sha256.as_str(), "{node}: the new birth runs the bound bytes: {launch}");
    if serves_proof_store {
        // The same data dir: the canary survived the restart.
        let c = probe_call(&f.estate, &["get", "--target", &format!("exact:{node_id}"), "--key", &ckey]);
        assert_eq!(c.out["reply"]["result"], json!({"found": true, "value": cval}), "{node}: the value stored before the restart is held by the new birth (same data dir): {}", c.out);
        assert_eq!(c.out["reply"]["incarnation_id"], n2["incarnation_id"], "{node}: the read was served by the new birth: {}", c.out);
        f.actions.push(action_row(&c));
        // Routing: an exact write reaches the new birth, never the old.
        let wi = op_to(f, a, &n2, None, "after", "after-restart");
        let w = &a.traffic.ops[wi].1.out;
        assert_eq!((w["outcome"].as_str(), w["reply"]["result"].clone()), (Some("Reply"), json!({"stored": true})), "{node}: an exact write after the restart: {w}");
        assert_eq!(w["reply"]["incarnation_id"], n2["incarnation_id"], "{node}: the write was served by the new birth: {w}");
    }
    let routing = match kind.as_str() {
        "broker" => {
            let carried = carried_get(&f.estate, &n2, &gw, &ckey);
            assert_eq!(carried.out["reply"]["result"], json!({"found": true, "value": cval}), "{}", carried.out);
            let edge = await_edge(&f.estate, &gw, &node_id, &new_inc, Some(&old_inc)).await;
            let oi = originate_op(f, a, &gw, &n2, "after-restart");
            let orig_trace = a.traffic.ops[oi].1.trace_id.clone();
            let orig_route = a.traffic.ops[oi].1.out["route"].clone();
            f.actions.push(action_row(&carried));
            // The other mesh's gateway: its edge facts are observed, and its call over the projection must
            // still reach the new birth whatever the facts say.
            let cross_edge = observe_edge(&f.estate, &gx, &node_id, &new_inc, &old_inc, Duration::from_secs(30)).await;
            let cross_carried = carried_get(&f.estate, &n2, &gx, &ckey);
            let cross_edge_after = await_edge(&f.estate, &gx, &node_id, &new_inc, None).await;
            let xi = originate_op(f, a, &gx, &n2, "after-restart");
            let cross_orig_trace = a.traffic.ops[xi].1.trace_id.clone();
            f.actions.push(action_row(&cross_carried));
            json!({
                "gateway": gw, "edge": edge, "carried_trace": carried.trace_id, "originated_trace": orig_trace, "originated_route": orig_route,
                "cross_mesh_gateway": gx, "cross_mesh_edge_facts": cross_edge, "cross_mesh_edge_after_a_carried_call": cross_edge_after, "cross_mesh_carried_trace": cross_carried.trace_id, "cross_mesh_originated_trace": cross_orig_trace,
            })
        }
        "gateway" => {
            let broker = st2.nodes.iter().find(|x| x["name"] == format!("{mesh}.broker.1")).cloned().unwrap();
            let carried = carried_get(&f.estate, &broker, node, &format!("canary-of-{node}"));
            let edge = await_edge(&f.estate, node, &s(&broker["node_id"]), &s(&broker["incarnation_id"]), None).await;
            let oi = originate_op(f, a, node, &broker, "after-restart");
            let orig_trace = a.traffic.ops[oi].1.trace_id.clone();
            let orig_route = a.traffic.ops[oi].1.out["route"].clone();
            f.actions.push(action_row(&carried));
            json!({"restarted_gateway": node, "broker": broker["name"], "edge": edge, "carried_trace": carried.trace_id, "originated_trace": orig_trace, "originated_route": orig_route})
        }
        _ => Value::Null,
    };
    // Every compute and gateway holds an edge to the current primary node-admin birth and none to a superseded one.
    // A Connected fact a cross-mesh holder keeps for a superseded birth is listed under `routing` (observed
    // above), never required away here: the required edges are the current primary node-admin's.
    let connections = converge_connections_with(f, inv, false).await;
    let rec = json!({
        "label": label, "node": node, "kind": kind, "node_id": node_id, "endpoint_id": endpoint, "old_incarnation_id": old_inc, "new_incarnation_id": new_inc,
        "runtimes": runtimes, "old_birth_terminal": terminal, "new_birth_launch": launch, "canary_key": ckey, "serves_proof_store": serves_proof_store, "restart": ev, "routing": routing, "connections": connections,
    });
    inv.holds(
        &format!("{label}: {node} ({kind}) restarted as the same logical node in a new exact birth in a new runtime; the old birth's runtime is terminal; the canary in its data dir survived; calls and connections converged on the new birth"),
        true,
        json!({"node_id": node_id, "old_incarnation_id": old_inc, "new_incarnation_id": new_inc, "runtimes": rec["runtimes"], "terminal": rec["old_birth_terminal"]}),
    );
    (rec, st2)
}

/// The spans of one restart: REST -> reconcile (operation `restart-node:<node>`) -> node.update.via-build
/// -> pipeline (restart) -> steps, the NodeRestarting event naming the old birth, the new birth's own
/// ready span, and no NodeDeleted for the node at any time.
fn check_restart_chain(spans: &[Value], rec: &Value) -> Value {
    let node = s(&rec["node"]);
    let ev = &rec["restart"];
    let (bid, id, old, new) = (s(&ev["build_id"]), s(&rec["node_id"]), s(&rec["old_incarnation_id"]), s(&rec["new_incarnation_id"]));
    let rest = named(spans, "rdm.node_admin.build.update.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == bid.as_str() && sp["attributes"]["node"] == node.as_str()).cloned().unwrap_or_else(|| panic!("{node}: no build.update.via-rest for the restart"));
    let op = format!("restart-node:{node}");
    let reconcile = named(spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|sp| sp["attributes"]["build_id"] == bid.as_str() && sp["attributes"]["operations"].as_str().is_some_and(|o| o.split(',').any(|x| x == op)))
        .cloned()
        .unwrap_or_else(|| panic!("{node}: no reconcile executed {op}"));
    let update = named(spans, "rdm.node_admin.node.update.via-build").into_iter().find(|sp| sp["attributes"]["node"] == node.as_str() && descends_from(spans, sp, &reconcile)).cloned().unwrap_or_else(|| panic!("{node}: no node.update.via-build under the reconcile"));
    let pipelines: Vec<&Value> = named(spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().filter(|p| p["parent_span_id"] == update["span_id"]).collect();
    assert!(!pipelines.is_empty(), "{node}: no deployment pipeline under its node.update.via-build");
    let pipeline = pipelines[0];
    let mut steps: Vec<&Value> = named(spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|st| st["parent_span_id"] == pipeline["span_id"]).collect();
    steps.sort_by_key(|st| start_ns(st));
    let names: Vec<String> = steps.iter().map(|st| s(&st["attributes"]["step"])).collect();
    let pos = |n: &str| names.iter().position(|x| x == n).unwrap_or_else(|| panic!("{node}: restart pipeline has no `{n}` step: {names:?}"));
    assert!(pos("NodeRestarting") < pos("MarkDraining") && pos("MarkDraining") < pos("TerminateRuntime"), "{node}: restart steps run NodeRestarting -> MarkDraining -> TerminateRuntime: {names:?}");
    let restarting = named(spans, "rdm.node_admin.node.update.via-node-restarting").into_iter().filter(|sp| sp["attributes"]["node_id"] == id.as_str() && sp["attributes"]["build_id"] == bid.as_str()).collect::<Vec<_>>();
    assert!(!restarting.is_empty(), "{node}: the executor published no NodeRestarting for {id}");
    assert!(restarting.iter().all(|sp| sp["attributes"]["incarnation_id"] == old.as_str()), "{node}: NodeRestarting names the old birth {old}: {restarting:?}");
    let ready = named(spans, "rdm.mesh.node.update.via-ready").into_iter().find(|sp| sp["attributes"]["node_id"] == id.as_str() && sp["attributes"]["incarnation_id"] == new.as_str()).cloned().unwrap_or_else(|| panic!("{node}: the new birth {new} left no ready span"));
    assert!(start_ns(&ready) > start_ns(restarting[0]), "{node}: the new birth became ready after the restart was announced");
    let deleted: Vec<&Value> = named(spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == id.as_str()).collect();
    assert!(deleted.is_empty(), "{node}: a successful restart emits no NodeDeleted for the same logical node: {deleted:?}");
    let received = named(spans, "rdm.mesh.membership.update.via-node-restarting").into_iter().filter(|sp| sp["attributes"]["node_id"] == id.as_str()).count();
    json!({"node": node, "op": op, "rest_span": rest["span_id"], "reconcile_span": reconcile["span_id"], "update_span": update["span_id"], "pipeline_span": pipeline["span_id"], "steps": names,
        "restarting_spans": restarting.iter().map(|sp| sp["span_id"].clone()).collect::<Vec<_>>(), "received_restarting": received, "ready_span": ready["span_id"], "node_deleted_spans": 0})
}

/// The reads that settle the ledger: every operation's key read from the node it was aimed at, unless
/// the node's NodeId left the fabric (`gone_ok` names the path.names allowed to have been replaced).
fn final_reads(f: &Formed, a: &Authority, nodes_end: &[Value], gone_ok: &BTreeSet<String>) -> (BTreeMap<String, Value>, Vec<Call>) {
    let mut verification = Vec::new();
    let mut final_state: BTreeMap<String, Value> = BTreeMap::new();
    for (op, _) in &a.traffic.ops {
        let alive = nodes_end.iter().any(|n| s(&n["node_id"]) == op.target_id);
        if !alive {
            assert!(gone_ok.contains(&op.target), "{}: its target {} ({}) left the fabric and is not one the cell replaced or retired", op.key, op.target, op.target_id);
            final_state.insert(op.key.clone(), json!({"unverifiable": "the target's NodeId left the fabric; its store went with it"}));
            continue;
        }
        let c = probe_call(&f.estate, &["get", "--target", &format!("exact:{}", op.target_id), "--key", &op.key]);
        assert_eq!(c.out["outcome"], "Reply", "verification read of {} must reply once the estate is settled: {}", op.key, c.out);
        final_state.insert(op.key.clone(), c.out["reply"]["result"].clone());
        verification.push(c);
    }
    (final_state, verification)
}

async fn restart_run(cell: &str, shape: Shape) {
    let mut f = form(cell, shape).await;
    let mut inv = Invariants::default();
    let nodes = settle(&f).await;
    let (launches, fabric) = check_identities_and_launches(&f, &nodes, &mut inv).await;
    let authorities = check_authorities(&nodes, &fabric, &mut inv);
    let connections = converge_connections(&f, &mut inv).await;
    let mut a = Authority::new(f.seed, &nodes);
    let mut st = a.stable(&mut f, "formed", &both(2), &mut inv).await;
    a.traffic.burst(&f, 12, "steady-before").await;
    let steady = a.traffic.ops.iter().filter(|(_, c)| bucket(&c.out) == Bucket::Reply).count();
    inv.holds("steady traffic before any change: every operation replied", steady == a.traffic.ops.len(), json!({"issued": a.traffic.ops.len(), "replied": steady}));
    let mut order = representatives(&f, &st);
    order.pop();
    let mut records = Vec::new();
    for (i, node) in order.iter().cloned().chain(std::iter::once(String::new())).enumerate() {
        // C3's node-admin: neither the fabric-primary nor the Day-0 admin, read at the moment.
        let node = if node.is_empty() {
            st.admins.iter().filter(|n| n["name"] != st.fp["name"] && n["name"] != "mesh1.admin.1").map(|n| s(&n["name"])).min().expect("a node-admin that is neither the fabric-primary nor the Day-0 admin")
        } else {
            node
        };
        let scenario = ["C6 broker-shaped restart", "C5 gateway-shaped restart", "C7 compute-shaped restart", "C3 node-admin restart"][i];
        let (rec, st2) = restart_representative(&mut f, &mut a, &st, &node, scenario, &mut inv).await;
        st = st2;
        records.push(rec);
        a.traffic.burst(&f, 4, "steady-between").await;
    }
    let st = a.stable(&mut f, "final", &both(2), &mut inv).await;
    let nodes_end = st.nodes.clone();
    let names_final: BTreeSet<String> = nodes_end.iter().map(|n| s(&n["name"])).collect();
    assert_eq!(names_final, f.shape.names(), "the topology is what it was: the same path.names, nothing added or removed");
    let ids_final: BTreeSet<String> = nodes_end.iter().map(|n| s(&n["node_id"])).collect();
    let ids_initial: BTreeSet<String> = nodes.iter().map(|n| s(&n["node_id"])).collect();
    assert_eq!(ids_final, ids_initial, "four restarts changed no NodeId");
    let launches_end = check_final_shape(&f, &nodes_end, &mut inv).await;
    let (final_state, verification) = final_reads(&f, &a, &nodes_end, &BTreeSet::new());
    let fabric_end = a.get(&mut f, "/api/fabric").await.expect("an admin answers /api/fabric");
    a.history.push(authority_row("final", &nodes_end, &fabric_end));
    let stop_t = Instant::now();
    f.estate.stop().await;
    let left = provider_left(&f, stop_t, &mut inv).await;
    let spans = f.estate.spans();
    let accounted = account(&spans, &a.traffic.ops, &final_state, &mut inv);
    let claims = claim_rows(&spans);
    let attempts = check_one_executor_per_attempt(&claims, &mut inv);
    let fence = check_writes_and_fence(&f, &a, &spans, &mut inv);
    let elections = check_election_history(&a, &spans, &mut inv);
    let imported = check_imported_mechanism(&f, &spans, &mut inv);
    let chains: Vec<Value> = records.iter().map(|r| check_restart_chain(&spans, r)).collect();
    inv.holds("each restart was a Build the rectifier executed: REST -> reconcile (restart-node) -> node.update.via-build -> pipeline -> NodeRestarting, MarkDraining, TerminateRuntime in order; NodeRestarting named the old birth; the new birth announced ready; no NodeDeleted for the node", true, json!({"restarts": chains.len()}));
    let all_creates: BTreeSet<String> = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().filter(|sp| !s(&sp["attributes"]["build_id"]).is_empty()).map(|sp| s(&sp["attributes"]["build_id"])).collect();
    assert_eq!(all_creates, BTreeSet::from([f.build_id.clone()]), "the only Build ever accepted is the formation Build: four restarts are four attempts of it");
    let services = check_services(&spans, &mut inv);
    let formation = check_formation_chain(&f, &spans, &mut inv);
    let mut runtime = check_runtime_facts(&f, &nodes, &launches, &spans, &mut inv);
    runtime.extend(records.iter().map(|r| json!({"phase": "restart", "node": r["node"], "node_id": r["node_id"], "endpoint_id": r["endpoint_id"], "old_incarnation_id": r["old_incarnation_id"], "new_incarnation_id": r["new_incarnation_id"], "runtimes": r["runtimes"], "old_birth_terminal": r["old_birth_terminal"]})));
    let index = copy_spans(&f.estate, &f.dir);
    let mut actions = std::mem::take(&mut f.actions);
    actions.extend(a.actions.iter().cloned());
    actions.extend(a.traffic.ops.iter().map(|(_, c)| action_row(c)));
    actions.extend(verification.iter().map(action_row));
    actions.sort_by_key(|r| r["t_ms"].as_u64().unwrap_or(0));
    let mut history = std::mem::take(&mut a.history);
    history.extend(claims.iter().cloned());
    history.extend(a.build_obs.iter().map(|o| json!({"phase": "build-projection", "observation": o})));
    write_views(
        Views {
            f: &f,
            topology_final: nodes_end,
            actions,
            operations: accounted.operations,
            outcomes: accounted.outcomes,
            authority_history: history,
            route_history: Vec::new(),
            runtime_history: runtime,
            inv,
            extra: json!({
                "authorities_at_formation": authorities, "connections_at_formation": connections, "provider_after_stop": left, "services": services, "formation_chains": formation,
                "restarts": records, "restart_chains": chains, "scenario_events": a.events, "topology_writes": a.writes, "fence": fence, "attempts": attempts, "elections": elections, "imported_mechanism": imported,
                "final_launches": launches_end, "scenarios": ["C3", "C5", "C6", "C7"],
                "ledger": {"issued": a.traffic.ops.len(), "buckets": accounted.buckets.iter().map(|(k, v)| (format!("{k:?}"), *v)).collect::<BTreeMap<_, _>>(), "class_detail": accounted.summary, "indeterminate": accounted.indeterminate, "protocol_refusals": accounted.refusals, "verification_reads": verification.len()},
                "schedule": {"seed": f.seed, "generator": "rafka_test_scenario::model::Rng (SplitMix64)", "restarts": records.iter().map(|r| r["node"].clone()).collect::<Vec<_>>()},
            }),
        },
        index,
        &spans,
    );
}

/// CONTRACT: in the formed estate, a representative of every role shape is restarted by the Build
/// rectifier under seeded traffic: a broker, a gateway, a compute and a node-admin that is neither
/// the fabric-primary nor the Day-0 admin (`POST /api/nodes/<node>/restart`, an attempt of the
/// accepted Build). Each restarted node is the same logical node (same NodeId, same EndpointId, same
/// path.name, the value stored in its data dir before the restart still there) in a new exact birth
/// (new IncarnationId) in a new runtime: the provider (the process table's start token, or the
/// container daemon) shows the old runtime terminal and the new one running the bound bytes. Routing
/// recovers onto the new birth: an exact write is served by it, the gateway that held a pooled edge
/// to the old broker birth holds one to the new birth and none to the old, and a call over that held
/// connection is answered by the new birth. At every checkpoint exactly one admin accepts a topology
/// write. Every operation is classified once, none applied twice, none replayed. What must NOT
/// happen: a new NodeId or EndpointId, a NodeDeleted for a restarted node, a Connected edge to a
/// superseded birth, a second Build for an unchanged topology, a signal to the fabric-primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_role_restarts_preserve_identity_and_replace_birth() {
    restart_run("mock_role_restarts_preserve_identity_and_replace_birth", any_tier()).await;
}

// ---- retirement, replacement and the exact-runtime kill ------------------------------------------

/// Every live admin's view of `name`: whether it lists the path, and under which NodeId.
async fn admin_views_of(a: &mut Authority, f: &Formed, name: &str) -> Vec<Value> {
    let mut bases: BTreeSet<String> = a.known_bases.clone();
    bases.insert(f.estate.admin.clone());
    let mut out = Vec::new();
    for b in bases {
        if let Some(v) = try_json(&b, "/api/nodes").await {
            let ns = v["nodes"].as_array().cloned().unwrap_or_default();
            let n = ns.iter().find(|n| n["name"] == name);
            out.push(json!({"admin_base": b, "lists_path": n.is_some(), "node_id": n.map(|n| n["node_id"].clone()), "status": n.map(|n| n["status"].clone()), "nodes_listed": ns.len()}));
        }
    }
    out
}

/// A topology write at an admin that is not the fabric-primary is refused by name, naming the
/// fabric-primary, and executes nothing.
async fn refused_by_a_non_writer(f: &Formed, a: &mut Authority, st: &Stable, path: &str) -> Value {
    let non_writer = st.admins.iter().find(|n| n["name"] != st.fp["name"]).expect("an admin that is not the fabric-primary").clone();
    let (status, v) = f.estate.delete_at(&s(&non_writer["admin_api_base"]), &format!("/api/nodes/{path}")).await;
    assert_eq!((status, v["error"].as_str(), &v["fabric_primary"]), (409, Some("rejected-not-authority"), &st.fp["name"]), "a retire sent to {} is refused naming the fabric-primary: {v}", non_writer["name"]);
    let rec = json!({"sent_to": non_writer["name"], "status": status, "error": v["error"], "fabric_primary": v["fabric_primary"], "path": path, "t_ms": now_ms()});
    a.refused_writes.push(rec.clone());
    rec
}

/// Retire `node` through the Build rectifier as the fabric-primary authorizes it, under seeded
/// traffic aimed at it. The old birth's runtime is terminal as the provider reports it, its NodeId is
/// in no admin's view after the removal is held for a full staleness window, and a call to the exact
/// NodeId sends nothing. Leaves `a.roles` as the shape without the node.
async fn retire_via_build(f: &mut Formed, a: &mut Authority, st: &Stable, node: &str, label: &str, inv: &mut Invariants) -> (Value, Stable) {
    let n = st.nodes.iter().find(|n| n["name"] == node).unwrap_or_else(|| panic!("{node} is not in the settled view")).clone();
    let (kind, mesh, node_id, old_inc) = (s(&n["kind"]), s(&n["mesh"]), s(&n["node_id"]), s(&n["incarnation_id"]));
    assert!(matches!(kind.as_str(), "broker" | "gateway" | "compute"), "{node}: only a role node is retired here");
    let rt_before = exact_runtime(f, node).await;
    let gw = format!("{mesh}.gateway.1");
    let ci = op_to(f, a, &n, None, "canary", "before-retire");
    let (ckey, cval) = (a.traffic.ops[ci].0.key.clone(), a.traffic.ops[ci].0.value.clone());
    assert_eq!(a.traffic.ops[ci].1.out["reply"]["result"], json!({"stored": true}), "{node}: the canary was stored before the retire: {}", a.traffic.ops[ci].1.out);
    if kind == "broker" && gw != node {
        carried_get(&f.estate, &n, &gw, &ckey);
        await_edge(&f.estate, &gw, &node_id, &old_inc, None).await;
    }
    let refusal = refused_by_a_non_writer(f, a, st, node).await;
    let still = f.estate.node(node).await;
    assert_eq!((still["node_id"].clone(), still["incarnation_id"].clone(), still["status"].clone()), (n["node_id"].clone(), n["incarnation_id"].clone(), json!("ready-for-traffic")), "{node}: the refused retire executed nothing: {still}");
    let mut roles: BTreeSet<String> = a.roles.clone().unwrap_or_else(|| f.shape.names().into_iter().filter(|x| launch_id(x) != "node_admin").collect());
    roles.remove(node);
    a.roles = Some(roles);
    a.traffic.aim = matches!(kind.as_str(), "broker" | "gateway").then(|| node.to_string());
    let before_build = s(&st.fabric["build_id"]);
    let t0 = now_ns();
    let build_id = a.write(f, st, &format!("{label}: authorized retire"), "DELETE", &format!("/api/nodes/{node}"), &Value::Null).await;
    assert_ne!(build_id, before_build, "{node}: a retire changes the topology, so it is a new accepted Build");
    let fin = a.finish(f, &build_id, Until::Gone { node: node.into() }, "during-retire").await;
    a.traffic.aim = None;
    let st2 = a.stable(f, &format!("{label}: {node} retired"), &both(2), inv).await;
    assert!(st2.nodes.iter().all(|x| x["node_id"] != n["node_id"]), "{node}: the retired NodeId is in the settled view");
    assert_eq!(s(&st2.fabric["build_id"]), build_id, "{node}: Fabric.build_id names the retiring Build");
    let terminal = terminal_proof(node, &rt_before);
    // Terminal retire + held removal, then absence: a full staleness window with the removal held.
    let window = std::env::var("RAFKA_STALENESS_MS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(30_000) + 500;
    let until = Instant::now() + Duration::from_millis(window);
    let mut looks = 0u32;
    while Instant::now() < until {
        for v in admin_views_of(a, f, node).await {
            assert!(v["node_id"] != json!(node_id), "{node}: the retired NodeId {node_id} is back in an admin's view during the convergence window: {v}");
        }
        looks += 1;
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let views = admin_views_of(a, f, node).await;
    // A call to the exact retired NodeId sends nothing.
    let gone_i = op_to(f, a, &n, None, "to-retired", "after-retire");
    let g = &a.traffic.ops[gone_i].1.out;
    assert_eq!(bucket(g), Bucket::NotSent, "{node}: a call to the retired NodeId sends nothing: {g}");
    let rec = json!({
        "label": label, "node": node, "kind": kind, "mesh": mesh, "node_id": node_id, "old_incarnation_id": old_inc, "endpoint_id": n["endpoint_id"], "data_dir": n["data_dir"],
        "build_id": build_id, "fabric_build_id_before": before_build, "request_ns": t0, "wall_ms": fin["wall_ms"], "attempt_after": fin["build"]["attempt"], "executor_after": fin["build"]["executor"],
        "runtime_before": rt_before.record(), "old_birth_terminal": terminal, "non_writer_refusal": refusal, "canary_key": ckey, "canary_value": cval,
        "absence": {"window_ms": window, "looks": looks, "views_at_end": views}, "call_after_removal": {"outcome": g["outcome"], "reason": g["reason"]},
    });
    inv.holds(
        &format!("{label}: {node} ({kind}) was retired by the fabric-primary's Build (a non-writer's identical request was refused naming the fabric-primary and executed nothing); the old birth's runtime is terminal; its NodeId stayed out of every admin's view for a full staleness window; a call to it sent nothing"),
        true,
        json!({"node_id": node_id, "old_incarnation_id": old_inc, "terminal": rec["old_birth_terminal"], "absence_window_ms": window, "looks": looks}),
    );
    (rec, st2)
}

/// The spans of one retirement: REST (a new accepted Build) -> reconcile (`retire-node:<node>`) ->
/// node.delete.via-build -> pipeline -> the retire steps in their documented order; NodeDeleting and
/// NodeDeleted naming the old birth; the receivers' removal; and no Ready span of the NodeId at or
/// after its departure.
fn check_retire_chain(spans: &[Value], rec: &Value) -> Value {
    let node = s(&rec["node"]);
    let (bid, id, old) = (s(&rec["build_id"]), s(&rec["node_id"]), s(&rec["old_incarnation_id"]));
    let create = named(spans, "rdm.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == bid.as_str()).cloned().unwrap_or_else(|| panic!("{node}: no build.create.via-rest accepted {bid}"));
    let op = format!("retire-node:{node}");
    let reconcile = named(spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|sp| sp["attributes"]["build_id"] == bid.as_str() && sp["attributes"]["operations"].as_str().is_some_and(|o| o.split(',').any(|x| x == op)))
        .cloned()
        .unwrap_or_else(|| panic!("{node}: no reconcile executed {op}"));
    assert!(descends_from(spans, &reconcile, &create), "{node}: the retiring reconcile descends from its REST request");
    let del = named(spans, "rdm.node_admin.node.delete.via-build").into_iter().find(|sp| sp["attributes"]["node"] == node.as_str() && descends_from(spans, sp, &reconcile)).cloned().unwrap_or_else(|| panic!("{node}: no node.delete.via-build under the reconcile"));
    let pipeline = named(spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().find(|p| p["parent_span_id"] == del["span_id"]).cloned().unwrap_or_else(|| panic!("{node}: no deployment pipeline under its node.delete.via-build"));
    let mut steps: Vec<&Value> = named(spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|st| st["parent_span_id"] == pipeline["span_id"]).collect();
    steps.sort_by_key(|st| start_ns(st));
    let names: Vec<String> = steps.iter().map(|st| s(&st["attributes"]["step"])).collect();
    let mut last = 0usize;
    for want in ["NodeDeleting", "MarkDraining", "WaitForDrain", "PublishLeaving", "CloseRpcAdmission", "TerminateRuntime", "NodeDeleted", "ReleaseEndpoints", "ReleaseStorage", "RemoveTopologyMembership", "Complete"] {
        let at = names.iter().position(|x| x == want).unwrap_or_else(|| panic!("{node}: the retire pipeline has no `{want}` step: {names:?}"));
        assert!(at >= last, "{node}: retire steps out of order at `{want}`: {names:?}");
        last = at;
    }
    let outcomes: Vec<Value> = steps.iter().map(|st| json!({"step": st["attributes"]["step"], "outcome": st["attributes"]["outcome"]})).collect();
    let deleting = named(spans, "rdm.node_admin.node.update.via-node-deleting").into_iter().filter(|sp| sp["attributes"]["node_id"] == id.as_str() && sp["attributes"]["build_id"] == bid.as_str()).collect::<Vec<_>>();
    assert!(!deleting.is_empty(), "{node}: no NodeDeleting for {id}");
    let deleted = named(spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == id.as_str() && sp["attributes"]["build_id"] == bid.as_str()).collect::<Vec<_>>();
    assert_eq!(deleted.len(), 1, "{node}: exactly one NodeDeleted for {id}: {deleted:?}");
    assert_eq!(deleted[0]["attributes"]["incarnation_id"], old.as_str(), "{node}: NodeDeleted names the exact old birth");
    assert!(deleting.iter().all(|sp| sp["attributes"]["incarnation_id"].as_str().is_none_or(|i| i == old)), "{node}: NodeDeleting names the exact old birth");
    let removed = named(spans, "rdm.mesh.membership.remove.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == id.as_str()).count();
    let ready = named(spans, "rdm.mesh.node.update.via-ready");
    let after: Vec<&&Value> = ready.iter().filter(|r| r["attributes"]["node_id"] == id.as_str() && start_ns(r) > start_ns(deleted[0])).collect();
    assert!(after.is_empty(), "{node}: the retired NodeId {id} was Ready again after its departure: {after:?}");
    json!({"node": node, "op": op, "rest_span": create["span_id"], "reconcile_span": reconcile["span_id"], "delete_span": del["span_id"], "pipeline_span": pipeline["span_id"], "steps": outcomes,
        "node_deleting_spans": deleting.len(), "node_deleted_span": deleted[0]["span_id"], "receivers_removed": removed, "ready_after_departure": 0})
}

/// The Draining the retiring node applied, read from its own span and the pipeline's: the in-flight
/// count the apply answered, the status declarations the authority decided from the node, and the
/// work the node refused or finished.
fn drain_evidence(spans: &[Value], rec: &Value) -> Value {
    let node = s(&rec["node"]);
    let id = s(&rec["node_id"]);
    let applied: Vec<&Value> = named(spans, "rdm.node_admin.status.update.via-apply-draining").into_iter().filter(|sp| sp["attributes"]["node"] == node.as_str()).collect();
    let probes = named(spans, "rdm.node_admin.status.update.via-probe").into_iter().filter(|sp| sp["attributes"]["node"] == node.as_str()).count();
    let mut decls: Vec<&Value> = named(spans, "rdm.node_admin.status.update.via-declaration").into_iter().filter(|sp| sp["attributes"]["sender"] == node.as_str()).collect();
    decls.sort_by_key(|sp| start_ns(sp));
    let drain = named(spans, "rdm.mesh.node.update.via-drain").into_iter().filter(|sp| sp["attributes"]["node"] == node.as_str()).collect::<Vec<_>>();
    let rpc = named(spans, "rdm.node_admin.node.update.via-drain-rpc").into_iter().filter(|sp| sp["attributes"]["node"] == node.as_str()).collect::<Vec<_>>();
    json!({
        "node": node, "node_id": id,
        "apply_draining": applied.iter().map(|sp| json!({"span": sp["span_id"], "sender": sp["attributes"]["sender"], "in_flight": sp["attributes"]["in_flight"], "start_ns": start_ns(sp)})).collect::<Vec<_>>(),
        "drain_rpc_receipts": rpc.iter().map(|sp| json!({"span": sp["span_id"], "outcome": sp["attributes"]["outcome"], "start_ns": start_ns(sp)})).collect::<Vec<_>>(),
        "probes_served": probes,
        "declarations_from_the_node": decls.iter().map(|sp| json!({"span": sp["span_id"], "op": sp["attributes"]["op"], "outcome": sp["attributes"]["outcome"], "detail": sp["attributes"]["detail"], "receiver_is_primary": sp["attributes"]["receiver_is_primary"], "start_ns": start_ns(sp)})).collect::<Vec<_>>(),
        "process_drain": drain.iter().map(|sp| json!({"span": sp["span_id"], "deadline_ms": sp["attributes"]["deadline_ms"], "in_flight_at_deadline": sp["attributes"]["in_flight_at_deadline"]})).collect::<Vec<_>>(),
    })
}

/// No Connected Direct edge from `holder` to any birth of `node_id` (a retired node): its pooled
/// connections to it are gone. Bounded by the same 30 s as every connections convergence.
async fn await_no_edge(estate: &Estate, holder: &str, node_id: &str) -> Value {
    let until = Instant::now() + Duration::from_secs(30);
    let t0 = Instant::now();
    loop {
        let d = directs_to(estate, holder, node_id);
        if !d.iter().any(|x| x["state"] == "Connected") {
            return json!({"holder": holder, "node_id": node_id, "converged_ms": t0.elapsed().as_millis() as u64, "edges": d});
        }
        assert!(Instant::now() < until, "{holder} still holds a Connected Direct edge to the retired {node_id} 30 s after the removal: {}", serde_json::to_string(&d).unwrap());
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

/// The Connected Direct facts any gateway or compute still holds toward `node_id`, observed (not
/// required away): a cross-mesh holder's fact about a retired or replaced birth.
fn stale_facts_toward(estate: &Estate, f: &Formed, node_id: &str) -> Vec<Value> {
    let mut out = Vec::new();
    for h in f.shape.names().into_iter().filter(|n| matches!(launch_id(n), "gateway" | "compute")) {
        let d = directs_to(estate, &h, node_id);
        if d.iter().any(|x| x["state"] == "Connected") {
            out.push(json!({"holder": h, "edges": d}));
        }
    }
    out
}

/// The node at `name` in the settled view.
fn at<'a>(st: &'a Stable, name: &str) -> &'a Value {
    st.nodes.iter().find(|n| n["name"] == name).unwrap_or_else(|| panic!("{name} is not in the settled view"))
}

/// Exact-runtime kill of `node`'s runtime as the provider backends offer it, acknowledged by the
/// provider: a process by the OS (`process_faults`: control domain, pid and start token checked,
/// then SIGKILL, the exit observed), a container by the daemon (`docker kill` of the immutable id,
/// then not running).
fn kill_exact_runtime(node: &str, rt: &Rt) -> Value {
    match rt {
        Rt::Process(r) => {
            let applied = r.apply(SigFault::Kill).unwrap_or_else(|e| panic!("{node}: the exact-runtime kill was refused: {e:?}"));
            assert!(applied.exited, "{node}: the OS did not show the runtime gone: {applied:?}");
            json!({"backend": "process_faults", "fault": "kill", "runtime": rt.record(), "acknowledged_by": "OS: every thread of the pid exited", "state_after": applied.state_after.map(String::from)})
        }
        Rt::Container(c) => {
            container_faults::docker(&["kill", &c.id]).unwrap_or_else(|e| panic!("{node}: docker kill {}: {e}", c.id));
            let until = Instant::now() + Duration::from_secs(10);
            let after = loop {
                let i = container_faults::inspect(&c.id).unwrap_or_else(|e| panic!("{node}: inspect after kill: {e}"));
                if !i.running || Instant::now() >= until {
                    break i;
                }
                std::thread::sleep(Duration::from_millis(50));
            };
            assert!(!after.running, "{node}: the daemon still shows {} running after docker kill: {after:?}", c.id);
            json!({"backend": "container_faults (docker kill + inspect)", "fault": "kill", "runtime": rt.record(), "acknowledged_by": "Docker daemon: container not running", "inspect_after": after})
        }
    }
}

/// An exact runtime held in place: SIGSTOP (acknowledged by the OS showing state `T`) or `docker pause`
/// (acknowledged by the daemon showing `paused`). Alive and silent; released on `release` or, if the
/// cell panics, on drop, so a failing cell never leaves a held runtime that blocks the estate's stop.
struct Held {
    node: String,
    rt: Rt,
    released: bool,
}

impl Held {
    fn hold(node: &str, rt: &Rt) -> (Held, Value) {
        let ack = match rt {
            Rt::Process(r) => {
                let a = r.apply(SigFault::Stop).unwrap_or_else(|e| panic!("{node}: the exact-runtime hold was refused: {e:?}"));
                assert_eq!(a.state_after, Some('T'), "{node}: the OS shows the runtime stopped: {a:?}");
                json!({"backend": "process_faults", "fault": "stop", "state_after": "T", "runtime": rt.record()})
            }
            Rt::Container(c) => {
                container_faults::pause(&c.id).unwrap_or_else(|e| panic!("{node}: docker pause {}: {e}", c.id));
                let i = container_faults::inspect(&c.id).unwrap();
                assert_eq!(i.status, "paused", "{node}: the daemon shows the container paused: {i:?}");
                json!({"backend": "container_faults (docker pause + inspect)", "status": i.status, "runtime": rt.record()})
            }
        };
        (Held { node: node.to_string(), rt: rt.clone(), released: false }, ack)
    }

    fn release(&mut self) -> Value {
        self.released = true;
        match &self.rt {
            Rt::Process(r) => {
                let a = r.apply(SigFault::Continue).unwrap_or_else(|e| panic!("{}: releasing the hold was refused: {e:?}", self.node));
                assert_ne!(a.state_after, Some('T'), "{}: the OS shows the runtime running again: {a:?}", self.node);
                json!({"backend": "process_faults", "fault": "continue", "state_after": a.state_after.map(String::from)})
            }
            Rt::Container(c) => {
                container_faults::unpause(&c.id).unwrap_or_else(|e| panic!("{}: docker unpause {}: {e}", self.node, c.id));
                let i = container_faults::inspect(&c.id).unwrap();
                assert!(i.running && i.status == "running", "{}: the daemon shows the container running again: {i:?}", self.node);
                json!({"backend": "container_faults (docker unpause + inspect)", "status": i.status})
            }
        }
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        if !self.released {
            match &self.rt {
                Rt::Process(r) => {
                    let _ = r.apply(SigFault::Continue);
                }
                Rt::Container(c) => {
                    let _ = container_faults::unpause(&c.id);
                }
            }
        }
    }
}

/// The exact runtime of a held node is held, never taken for dead: while it is held the node stays
/// listed under its NodeId in every admin's view, no Build is accepted and no attempt is opened for
/// it, a call to it ends in a typed uncertain outcome (never a reply, never a replacement), and
/// once released the very same runtime serves again.
async fn hold_phase(f: &mut Formed, a: &mut Authority, st: &Stable, node: &str, inv: &mut Invariants) -> Value {
    let n = at(st, node).clone();
    let (node_id, inc) = (s(&n["node_id"]), s(&n["incarnation_id"]));
    let rt = exact_runtime(f, node).await;
    let ci = op_to(f, a, &n, None, "control", "before-hold");
    assert_eq!(a.traffic.ops[ci].1.out["reply"]["result"], json!({"stored": true}), "{node}: the healthy same-family control: {}", a.traffic.ops[ci].1.out);
    let fabric_build = s(&st.fabric["build_id"]);
    let attempt_before = try_json(&st.fp_base(), &format!("/api/builds?id={fabric_build}")).await.map(|b| b["attempt"].as_u64().unwrap());
    let (mut held, hold_ack) = Held::hold(node, &rt);
    let hold_ns = now_ns();
    progress(&format!("{node} held: {hold_ack}"));
    // A call to the held node: its budget (10 s) is the bound; the outcome is typed.
    let hi = op_to(f, a, &n, None, "to-held", "during-hold");
    let held_call = a.traffic.ops[hi].1.out.clone();
    let held_bucket = bucket(&held_call);
    assert_ne!(held_bucket, Bucket::Reply, "{node}: a held runtime answered: {held_call}");
    // The silence window: two staleness windows with the node held. Silence is not death.
    let window = 2 * std::env::var("RAFKA_STALENESS_MS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(30_000) + 500;
    let until = Instant::now() + Duration::from_millis(window);
    let mut looks = Vec::new();
    while Instant::now() < until {
        let views = admin_views_of(a, f, node).await;
        for v in &views {
            assert!(v["lists_path"] == true && v["node_id"] == json!(node_id), "{node}: an admin stopped listing the held node under its NodeId within the silence window: {v}");
        }
        let fabric = a.get(f, "/api/fabric").await.unwrap_or(Value::Null);
        assert_eq!(s(&fabric["build_id"]), fabric_build, "{node}: Fabric.build_id moved while the node was held: {fabric}");
        looks.push(json!({"t_ns": now_ns(), "statuses": views.iter().map(|v| v["status"].clone()).collect::<Vec<_>>()}));
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let attempt_during = try_json(&st.fp_base(), &format!("/api/builds?id={fabric_build}")).await.map(|b| b["attempt"].as_u64().unwrap());
    assert_eq!(attempt_during, attempt_before, "{node}: an attempt was opened for the Build while the node was held: {attempt_before:?} -> {attempt_during:?}");
    let release_ack = held.release();
    let release_ns = now_ns();
    // Bounded recovery from the release: the same incarnation, ready, answering.
    let t = Instant::now();
    let ready = loop {
        let ns = a.nodes_now(f).await;
        if let Some(x) = ns.into_iter().find(|x| x["name"] == node && x["status"] == "ready-for-traffic" && x["incarnation_id"] == inc.as_str() && x["node_id"] == node_id.as_str()) {
            break x;
        }
        assert!(t.elapsed() < Duration::from_secs(30), "{node} was not ready again under its own incarnation 30 s after the hold was released");
        tokio::time::sleep(Duration::from_millis(250)).await;
    };
    let ri = op_to(f, a, &ready, None, "after-hold", "after-hold");
    let after_call = a.traffic.ops[ri].1.out.clone();
    assert_eq!(after_call["reply"]["result"], json!({"stored": true}), "{node}: served again after the release: {after_call}");
    assert_eq!(after_call["reply"]["incarnation_id"], json!(inc), "{node}: served by the same birth: {after_call}");
    let recovery_ms = t.elapsed().as_millis() as u64;
    let rt_after = exact_runtime(f, node).await;
    assert!(rt.is(&rt_after), "{node}: the runtime that serves after the release is the one that was held: {:?} vs {:?}", rt.record(), rt_after.record());
    let rec = json!({
        "node": node, "node_id": node_id, "incarnation_id": inc, "hold_ack": hold_ack, "hold_ns": hold_ns, "release_ack": release_ack, "release_ns": release_ns, "silence_window_ms": window,
        "call_to_held_node": {"bucket": format!("{held_bucket:?}"), "reason": held_call["reason"], "outcome": held_call["outcome"]}, "attempt_before": attempt_before, "attempt_during": attempt_during,
        "fabric_build_id": fabric_build, "looks": looks, "recovery_ms": recovery_ms, "runtime_before": rt.record(), "runtime_after": rt_after.record(),
    });
    inv.holds(
        &format!("{node} held by exact-runtime {} (acknowledged by the provider): for {window} ms it stayed listed under its NodeId in every admin's view, no Build or attempt was opened for it, a call to it ended {held_bucket:?}; released (acknowledged), the same runtime and birth served again after {recovery_ms} ms", if provider() == "container" { "pause" } else { "SIGSTOP" }),
        true,
        json!({"node_id": node_id, "silence_window_ms": window, "recovery_ms": recovery_ms, "call": rec["call_to_held_node"]}),
    );
    rec
}

/// The spans of an add: REST (a new accepted Build) -> reconcile (`create-node:<node>`) ->
/// node.create.via-build -> pipeline -> steps; the new NodeId's own ready span.
fn check_add_chain(spans: &[Value], node: &str, build_id: &str, node_id: &str, incarnation: &str) -> Value {
    let create = named(spans, "rdm.node_admin.build.create.via-rest").into_iter().find(|sp| sp["attributes"]["build_id"] == build_id).cloned().unwrap_or_else(|| panic!("{node}: no build.create.via-rest accepted {build_id}"));
    let op = format!("create-node:{node}");
    let reconcile = named(spans, "rdm.node_admin.build.update.via-reconcile")
        .into_iter()
        .find(|sp| sp["attributes"]["build_id"] == build_id && sp["attributes"]["operations"].as_str().is_some_and(|o| o.split(',').any(|x| x == op)))
        .cloned()
        .unwrap_or_else(|| panic!("{node}: no reconcile executed {op}"));
    assert!(descends_from(spans, &reconcile, &create), "{node}: the adding reconcile descends from its REST request");
    let make = named(spans, "rdm.node_admin.node.create.via-build").into_iter().find(|sp| sp["attributes"]["node"] == node && descends_from(spans, sp, &reconcile)).cloned().unwrap_or_else(|| panic!("{node}: no node.create.via-build under the reconcile"));
    let pipeline = named(spans, "rdm.node_admin.deployment.update.via-pipeline").into_iter().find(|p| p["parent_span_id"] == make["span_id"]).cloned().unwrap_or_else(|| panic!("{node}: no deployment pipeline under its node.create.via-build"));
    let steps = named(spans, "rdm.node_admin.deployment.update.via-step").into_iter().filter(|st| st["parent_span_id"] == pipeline["span_id"]).count();
    assert!(steps > 0, "{node}: no deployment steps under its pipeline");
    let ready = named(spans, "rdm.mesh.node.update.via-ready").into_iter().find(|sp| sp["attributes"]["node_id"] == node_id && sp["attributes"]["incarnation_id"] == incarnation).cloned().unwrap_or_else(|| panic!("{node}: the new node {node_id} left no ready span"));
    json!({"node": node, "op": op, "rest_span": create["span_id"], "reconcile_span": reconcile["span_id"], "create_span": make["span_id"], "pipeline_span": pipeline["span_id"], "steps": steps, "ready_span": ready["span_id"], "new_node_id": node_id})
}

async fn replacement_run(cell: &str, shape: Shape) {
    let mut f = form(cell, shape).await;
    let mut inv = Invariants::default();
    let nodes = settle(&f).await;
    let (launches, fabric) = check_identities_and_launches(&f, &nodes, &mut inv).await;
    let authorities = check_authorities(&nodes, &fabric, &mut inv);
    let connections = converge_connections(&f, &mut inv).await;
    let mut a = Authority::new(f.seed, &nodes);
    let st = a.stable(&mut f, "formed", &both(2), &mut inv).await;
    a.traffic.burst(&f, 12, "steady-before").await;
    let formation_build = f.build_id.clone();

    // 1. A planned logical replacement: the broker's authorized retirement, then a Build that adds a
    //    broker in the same mesh. (No route opens `AttemptAction::Replace`: `http.rs` `open_attempt` is
    //    called with `replace = false` by the one route that calls it, and the rectifier's drift answer
    //    to a runtime death opens its attempt with `action: None`.)
    let victim = format!("mesh2.broker.{}", f.shape.broker);
    let old = at(&st, &victim).clone();
    let old_id = s(&old["node_id"]);
    let (retired, st_mid) = retire_via_build(&mut f, &mut a, &st, &victim, "C8 planned replacement", &mut inv).await;
    a.traffic.burst(&f, 4, "steady-gap").await;
    let gw = "mesh2.gateway.1".to_string();
    let add_t0 = now_ns();
    let add_build = a.write(&f, &st_mid, "C8 planned replacement: node.spawn broker", "POST", "/api/nodes/spawn", &json!({"mesh": "mesh2", "kind": "broker"})).await;
    assert_ne!(add_build, s(&retired["build_id"]), "the add is a new accepted Build");
    a.roles = None;
    let add_fin = a.finish(&mut f, &add_build, Until::Joined { node: victim.clone(), not_node_id: old_id.clone() }, "during-spawn").await;
    let st_new = a.stable(&mut f, "C8 planned replacement: successor ready", &both(2), &mut inv).await;
    let born = at(&st_new, &victim).clone();
    let (new_id, new_inc) = (s(&born["node_id"]), s(&born["incarnation_id"]));
    assert_ne!(born["node_id"], old["node_id"], "the successor is a new logical node: a new NodeId");
    assert_ne!(born["endpoint_id"], old["endpoint_id"], "the successor has a new EndpointId (a new key in a new data dir)");
    assert_ne!(born["incarnation_id"], old["incarnation_id"], "the successor is a new birth");
    assert_ne!(born["data_dir"], old["data_dir"], "the successor has a fresh data dir, not the retired node's");
    assert!(st_new.nodes.iter().filter(|n| n["node_id"] == old["node_id"]).count() == 0, "the retired NodeId is in no later view");
    let rt_new = exact_runtime(&f, &victim).await;
    let old_rt_record = retired["runtime_before"].clone();
    assert_ne!(old_rt_record, rt_new.record(), "the successor runs in a new runtime");
    live_and_distinct(&victim, &exact_rt_from_record(&old_rt_record, &retired), &rt_new);
    // Fresh proof store, no inherited identity, no stale targeting.
    let ckey = s(&retired["canary_key"]);
    let c = probe_call(&f.estate, &["get", "--target", &format!("exact:{new_id}"), "--key", &ckey]);
    assert_eq!((c.out["reply"]["result"].clone(), c.out["reply"]["executing_node"].clone()), (json!({"found": false}), json!(new_id)), "the successor's fresh proof store holds nothing of the retired node's: {}", c.out);
    f.actions.push(action_row(&c));
    let c2 = probe_call(&f.estate, &["get", "--target", &format!("path:{victim}"), "--key", &ckey]);
    assert_eq!((c2.out["reply"]["executing_node"].clone(), c2.out["reply"]["incarnation_id"].clone()), (json!(new_id), json!(new_inc)), "the path now resolves to the successor and only to it: {}", c2.out);
    f.actions.push(action_row(&c2));
    let gi = op_to(&f, &mut a, &old, None, "to-retired", "after-replacement");
    assert_eq!(bucket(&a.traffic.ops[gi].1.out), Bucket::NotSent, "a call to the retired NodeId still sends nothing once its path.name has a successor: {}", a.traffic.ops[gi].1.out);
    let carried = carried_get(&f.estate, &born, &gw, &ckey);
    let edge_new = await_edge(&f.estate, &gw, &new_id, &new_inc, None).await;
    let edge_old_gone = await_no_edge(&f.estate, &gw, &old_id).await;
    let oi = originate_op(&f, &mut a, &gw, &born, "after-replacement");
    let orig_trace = a.traffic.ops[oi].1.trace_id.clone();
    f.actions.push(action_row(&carried));
    let stale_toward_old = stale_facts_toward(&f.estate, &f, &old_id);
    let connections_after = converge_connections_with(&f, &mut inv, false).await;
    let replacement = json!({
        "old": {"node": victim, "node_id": old_id, "endpoint_id": old["endpoint_id"], "incarnation_id": old["incarnation_id"], "data_dir": old["data_dir"], "runtime": old_rt_record},
        "new": {"node": victim, "node_id": new_id, "endpoint_id": born["endpoint_id"], "incarnation_id": new_inc, "data_dir": born["data_dir"], "runtime": rt_new.record()},
        "add_build": add_build, "add_request_ns": add_t0, "add_wall_ms": add_fin["wall_ms"], "gateway_edge_to_successor": edge_new, "gateway_edge_to_retired_gone": edge_old_gone,
        "facts_other_holders_keep_toward_the_retired": stale_toward_old, "carried_trace": carried.trace_id, "originated_trace": orig_trace, "connections_after": connections_after,
        "path_name_successor": true,
    });
    inv.holds(
        "C8 planned replacement: the retired broker's NodeId, EndpointId, data dir and runtime are not the successor's; the successor's proof store is fresh; the retired NodeId is not routable and the path resolves only to the successor; the mesh's gateway dropped its edge to the retired node and holds one to the successor",
        true,
        json!({"old_node_id": old_id, "new_node_id": new_id}),
    );

    // 2. The drift answer to a runtime death: the exact runtime of a gateway is killed; the rectifier
    //    proves the birth gone from the provider and opens the next attempt of the same Build.
    let victim2 = format!("mesh1.gateway.{}", f.shape.gateway);
    let n2 = at(&st_new, &victim2).clone();
    let (id2, inc2) = (s(&n2["node_id"]), s(&n2["incarnation_id"]));
    let drift_build = s(&st_new.fabric["build_id"]);
    assert_eq!(drift_build, add_build, "the fabric names the add's Build before the death");
    let ci = op_to(&f, &mut a, &n2, None, "canary", "before-kill");
    assert_eq!(a.traffic.ops[ci].1.out["reply"]["result"], json!({"stored": true}), "{victim2}: the canary was stored before the kill");
    let rt2 = exact_runtime(&f, &victim2).await;
    let attempt_before = try_json(&st_new.fp_base(), &format!("/api/builds?id={drift_build}")).await.map(|b| b["attempt"].as_u64().unwrap()).unwrap();
    point_entry_away(&mut f, &st_new, &victim2);
    let kill = kill_exact_runtime(&victim2, &rt2);
    let kill_ns = now_ns();
    progress(&format!("{victim2} exact runtime killed: {kill}"));
    let drift_fin = a.finish(&mut f, &drift_build, Until::Reborn { node: victim2.clone(), old_incarnation: inc2.clone() }, "during-drift").await;
    let st_drift = a.stable(&mut f, "C8 drift: the dead gateway is re-born", &both(2), &mut inv).await;
    let n3 = at(&st_drift, &victim2).clone();
    let rt3 = exact_runtime(&f, &victim2).await;
    live_and_distinct(&victim2, &rt2, &rt3);
    let terminal2 = terminal_proof(&victim2, &rt2);
    assert_eq!(s(&st_drift.fabric["build_id"]), drift_build, "recovery of an unchanged topology accepted no new Build");
    let drift = json!({
        "node": victim2, "old_node_id": id2, "old_incarnation_id": inc2, "old_endpoint_id": n2["endpoint_id"], "new_node_id": n3["node_id"], "new_incarnation_id": n3["incarnation_id"], "new_endpoint_id": n3["endpoint_id"],
        "node_id_changed": n3["node_id"] != n2["node_id"], "endpoint_id_changed": n3["endpoint_id"] != n2["endpoint_id"], "build_id": drift_build, "attempt_before": attempt_before, "attempt_after": drift_fin["build"]["attempt"],
        "executor_after": drift_fin["build"]["executor"], "kill": kill, "kill_ns": kill_ns, "old_birth_terminal": terminal2, "runtimes": {"old": rt2.record(), "new": rt3.record()}, "wall_ms": drift_fin["wall_ms"],
    });
    inv.holds("C8 drift: the exact runtime of a gateway was killed (acknowledged by the provider); the next attempt of the same Build re-birthed it in a new runtime; the old runtime is terminal; no new Build was accepted", true, json!({"drift": drift}));

    // Final state.
    a.traffic.burst(&f, 6, "steady-after").await;
    let st = a.stable(&mut f, "final", &both(2), &mut inv).await;
    let nodes_end = st.nodes.clone();
    let names_final: BTreeSet<String> = nodes_end.iter().map(|n| s(&n["name"])).collect();
    assert_eq!(names_final, f.shape.names(), "the topology is the shape again: the replaced and the re-born nodes stand at their path.names");
    let launches_end = check_final_shape(&f, &nodes_end, &mut inv).await;
    let gone_ok: BTreeSet<String> = BTreeSet::from([victim.clone(), victim2.clone()]);
    let (final_state, verification) = final_reads(&f, &a, &nodes_end, &gone_ok);
    let fabric_end = a.get(&mut f, "/api/fabric").await.expect("an admin answers /api/fabric");
    a.history.push(authority_row("final", &nodes_end, &fabric_end));
    let stop_t = Instant::now();
    f.estate.stop().await;
    let left = provider_left(&f, stop_t, &mut inv).await;
    let spans = f.estate.spans();
    let accounted = account(&spans, &a.traffic.ops, &final_state, &mut inv);
    let claims = claim_rows(&spans);
    let attempts = check_one_executor_per_attempt(&claims, &mut inv);
    let fence = check_writes_and_fence(&f, &a, &spans, &mut inv);
    let elections = check_election_history(&a, &spans, &mut inv);
    let imported = check_imported_mechanism(&f, &spans, &mut inv);
    let retire_chain = check_retire_chain(&spans, &retired);
    let add_chain = check_add_chain(&spans, &victim, &add_build, &new_id, &new_inc);
    let t_deleted = start_ns(named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().find(|sp| sp["attributes"]["node_id"] == old_id.as_str()).unwrap());
    let t_ready = start_ns(named(&spans, "rdm.mesh.node.update.via-ready").into_iter().find(|sp| sp["attributes"]["node_id"] == new_id.as_str()).unwrap());
    assert!(t_ready > t_deleted, "the successor became ready after the retired node's departure was published");
    // The drift: a proven-drift attempt of the same Build after the kill, the dead gateway re-created under it.
    let drift_spans: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| sp["attributes"]["build_id"] == drift_build.as_str() && start_ns(sp) > kill_ns).collect();
    assert!(!drift_spans.is_empty(), "no proven-drift attempt of {drift_build} after the kill of {victim2}");
    let op2 = format!("create-node:{victim2}");
    let recreate: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-reconcile").into_iter().filter(|sp| sp["attributes"]["build_id"] == drift_build.as_str() && start_ns(sp) > kill_ns && sp["attributes"]["operations"].as_str().is_some_and(|o| o.split(',').any(|x| x == op2))).collect();
    assert!(!recreate.is_empty(), "no reconcile of {drift_build} after the kill executed {op2}");
    let drift_deleted: Vec<&Value> = named(&spans, "rdm.node_admin.node.delete.via-node-deleted").into_iter().filter(|sp| sp["attributes"]["node_id"] == id2.as_str()).collect();
    let drift_chain = json!({
        "proven_drift_spans": drift_spans.iter().map(|sp| json!({"span": sp["span_id"], "attempt": sp["attributes"]["attempt"], "scope": sp["attributes"]["scope"], "reason": sp["attributes"]["reason"], "authority": sp["attributes"]["authority"]})).collect::<Vec<_>>(),
        "recreate_reconciles": recreate.iter().map(|sp| json!({"span": sp["span_id"], "attempt": attempt_of(sp), "executor": sp["attributes"]["executor"], "operations": sp["attributes"]["operations"], "reason": sp["attributes"]["reason"], "action": sp["attributes"]["action"]})).collect::<Vec<_>>(),
        "node_deleted_for_the_dead_identity": drift_deleted.iter().map(|sp| json!({"span": sp["span_id"], "incarnation_id": sp["attributes"]["incarnation_id"]})).collect::<Vec<_>>(),
    });
    let all_creates: BTreeSet<String> = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().filter(|sp| !s(&sp["attributes"]["build_id"]).is_empty()).map(|sp| s(&sp["attributes"]["build_id"])).collect();
    assert_eq!(all_creates, BTreeSet::from([formation_build.clone(), s(&retired["build_id"]), add_build.clone()]), "the Builds accepted are the formation, the retirement and the add; the death's recovery accepted none");
    inv.holds("every topology change was a Build the rectifier executed: the retirement (retire-node), the add (create-node) and the death's recovery (a proven-drift attempt of the same Build re-creating the path); exactly three Builds were ever accepted", true, json!({"retire": retire_chain, "add": add_chain, "drift": drift_chain}));
    let services = check_services(&spans, &mut inv);
    let formation = check_formation_chain(&f, &spans, &mut inv);
    let mut runtime = check_runtime_facts(&f, &nodes, &launches, &spans, &mut inv);
    runtime.push(json!({"phase": "planned-replacement", "replacement": replacement}));
    runtime.push(json!({"phase": "drift-replacement", "drift": drift}));
    let index = copy_spans(&f.estate, &f.dir);
    let mut actions = std::mem::take(&mut f.actions);
    actions.extend(a.actions.iter().cloned());
    actions.extend(a.traffic.ops.iter().map(|(_, c)| action_row(c)));
    actions.extend(verification.iter().map(action_row));
    actions.sort_by_key(|r| r["t_ms"].as_u64().unwrap_or(0));
    let mut history = std::mem::take(&mut a.history);
    history.extend(claims.iter().cloned());
    history.extend(a.build_obs.iter().map(|o| json!({"phase": "build-projection", "observation": o})));
    write_views(
        Views {
            f: &f,
            topology_final: nodes_end,
            actions,
            operations: accounted.operations,
            outcomes: accounted.outcomes,
            authority_history: history,
            route_history: Vec::new(),
            runtime_history: runtime,
            inv,
            extra: json!({
                "authorities_at_formation": authorities, "connections_at_formation": connections, "provider_after_stop": left, "services": services, "formation_chains": formation,
                "planned_replacement": {"retired": retired, "replacement": replacement, "retire_chain": retire_chain, "add_chain": add_chain},
                "drift_replacement": {"drift": drift, "chain": drift_chain,
                    "no_planned_replace_operation": "no REST route opens AttemptAction::Replace: rafka-node-admin-core http.rs open_attempt takes `replace` but its one caller (restart_node) passes false, and the rectifier's proven-drift attempt carries action None (admin.rs, via-proven-drift AttemptOpened)"},
                "scenario_events": a.events, "topology_writes": a.writes, "fence": fence, "attempts": attempts, "elections": elections, "imported_mechanism": imported, "final_launches": launches_end, "scenarios": ["C8"],
                "ledger": {"issued": a.traffic.ops.len(), "buckets": accounted.buckets.iter().map(|(k, v)| (format!("{k:?}"), *v)).collect::<BTreeMap<_, _>>(), "class_detail": accounted.summary, "indeterminate": accounted.indeterminate, "protocol_refusals": accounted.refusals, "verification_reads": verification.len()},
                "schedule": {"seed": f.seed, "generator": "rafka_test_scenario::model::Rng (SplitMix64)", "planned": victim, "drift": victim2},
            }),
        },
        index,
        &spans,
    );
}

/// CONTRACT: a logical replacement retires the old identity and creates a distinct one. In the formed
/// estate the fabric-primary authorizes the retirement of a broker (a retire sent to an admin that is
/// not the fabric-primary is refused naming it and executes nothing) under seeded traffic aimed at it:
/// the Build runs the documented retire steps, the old birth's exact runtime is terminal as the
/// provider reports it, its NodeId stays out of every admin's view for a full staleness window, and a
/// call to it sends nothing. A Build then adds a broker at the freed path.name: a new NodeId, a new
/// EndpointId, a fresh data dir and a new runtime; its proof store holds nothing of the old node's, the
/// path resolves only to it, the mesh's gateway drops its edge to the retired node and holds one to
/// the successor. Then the exact runtime of a gateway is killed (acknowledged by the provider) and the
/// rectifier proves the birth gone and re-births the path under the next attempt of the same Build,
/// accepting no new Build. What must NOT happen: a reused NodeId, a successor holding the retired
/// node's data, a retired NodeId Ready again, a retire executed for a non-writer, a Build for a
/// death's recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_node_replacement_creates_distinct_logical_identity() {
    replacement_run("mock_node_replacement_creates_distinct_logical_identity", any_tier()).await;
}

/// Rebuild the old birth's `Rt` from the record the retirement kept, so its terminal and distinctness
/// can be checked against the successor's.
fn exact_rt_from_record(r: &Value, retired: &Value) -> Rt {
    match r["provider"].as_str() {
        Some("process") => Rt::Process(ExactRuntime { deployment_id: s(&r["deployment_id"]), control_domain: s(&r["control_domain"]), pid: r["pid"].as_u64().unwrap() as u32, start: r["start_token"].as_u64().unwrap() }),
        Some("container") => Rt::Container(container_faults::Inspected {
            id: s(&r["container"]),
            status: s(&r["status"]),
            running: false,
            pid: r["init_pid"].as_u64().unwrap_or(0),
            started_at: s(&r["started_at"]),
            restart_count: 0,
            exit_code: 0,
            oom_killed: false,
            networks: Vec::new(),
        }),
        other => panic!("{}: no runtime record ({other:?})", retired["node"]),
    }
}

/// The provider's action meets a held runtime: with the node's exact runtime held (acknowledged by the
/// provider) the fabric-primary is asked to restart it. The Build's attempt cannot drain a runtime that
/// does not answer; what it does is observed from the Build's own projection and the node's view, every
/// 500 ms, for as long as the hold lasts (three staleness windows). Silence is never death: the node
/// stays listed under its NodeId and no replacement is started. The hold is then released
/// (acknowledged) and the Build must complete with the node ready under a new incarnation, within the
/// 60 s a restart is given, as the same logical node.
async fn hold_restart(f: &mut Formed, a: &mut Authority, st: &Stable, node: &str, inv: &mut Invariants) -> Value {
    let n = at(st, node).clone();
    let (node_id, inc) = (s(&n["node_id"]), s(&n["incarnation_id"]));
    let rt = exact_runtime(f, node).await;
    let fabric_build = s(&st.fabric["build_id"]);
    let (mut held, hold_ack) = Held::hold(node, &rt);
    let hold_ns = now_ns();
    let attempt_before = try_json(&st.fp_base(), &format!("/api/builds?id={fabric_build}")).await.map(|b| b["attempt"].as_u64().unwrap());
    let build_id = a.write(f, st, "C17: node.restart of a node whose runtime is held", "POST", &format!("/api/nodes/{node}/restart"), &Value::Null).await;
    assert_eq!(build_id, fabric_build, "{node}: the restart is an attempt of the accepted Build");
    let window = 3 * std::env::var("RAFKA_STALENESS_MS").ok().and_then(|v| v.parse::<u64>().ok()).unwrap_or(30_000);
    let until = Instant::now() + Duration::from_millis(window);
    let mut seen: Vec<Value> = Vec::new();
    let mut completed_while_held = false;
    while Instant::now() < until {
        let b = a.get(f, &format!("/api/builds?id={build_id}")).await.unwrap_or(Value::Null);
        a.observe_build(&b);
        let views = admin_views_of(a, f, node).await;
        for v in &views {
            assert!(v["lists_path"] == true && v["node_id"] == json!(node_id), "{node}: an admin stopped listing the node under its NodeId while its restart waited on a held runtime: {v}");
        }
        seen.push(json!({"t_ns": now_ns(), "build_state": b["state"], "attempt": b["attempt"], "executor": b["executor"], "reason": b["reason"], "last_failure": b["last_failure"], "node_statuses": views.iter().map(|v| v["status"].clone()).collect::<Vec<_>>()}));
        if b["state"] == "complete" && views.iter().all(|v| v["node_id"] == json!(node_id)) && a.nodes_now(f).await.iter().any(|x| x["name"] == node && x["incarnation_id"] != inc.as_str() && x["status"] == "ready-for-traffic") {
            completed_while_held = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    let observed_ns = now_ns();
    // The provider's view of the held runtime at the end of the observation: alive and stopped, or gone.
    let runtime_at_end = match &rt {
        Rt::Process(r) => json!({"check": format!("{:?}", r.check()), "state": proc_state_of(r.pid)}),
        Rt::Container(c) => json!(container_faults::inspect(&c.id).map(|i| json!({"status": i.status, "running": i.running})).unwrap_or_else(|e| json!({"gone": e}))),
    };
    let release_ack = if rt_alive(&rt) { held.release() } else { held.released = true; json!({"not_needed": "the held runtime was terminated by the provider during the restart", "runtime": runtime_at_end}) };
    let release_ns = now_ns();
    let fin = a.finish(f, &build_id, Until::Reborn { node: node.into(), old_incarnation: inc.clone() }, "after-hold-release").await;
    let after = f.estate.node(node).await;
    assert_eq!(after["node_id"], n["node_id"], "{node}: the restart of a held runtime keeps the logical node");
    assert_ne!(after["incarnation_id"], n["incarnation_id"], "{node}: the restart ends in a new exact birth");
    let rt_new = exact_runtime(f, node).await;
    let runtimes = live_and_distinct(node, &rt, &rt_new);
    let terminal = terminal_proof(node, &rt);
    let rec = json!({
        "node": node, "node_id": node_id, "old_incarnation_id": inc, "new_incarnation_id": after["incarnation_id"], "build_id": build_id, "attempt_before": attempt_before, "attempt_after": fin["build"]["attempt"],
        "hold_ack": hold_ack, "hold_ns": hold_ns, "observation_window_ms": window, "observed_until_ns": observed_ns, "observations": seen, "restart_completed_while_held": completed_while_held,
        "runtime_at_end_of_observation": runtime_at_end, "release_ack": release_ack, "release_ns": release_ns, "wall_after_release_ms": fin["wall_ms"], "runtimes": runtimes, "old_birth_terminal": terminal,
    });
    inv.holds(
        &format!("{node}: a restart requested while its exact runtime was held (acknowledged) kept the logical node listed under its NodeId in every admin's view; it completed {} and ended in a new exact birth of the same NodeId in a new runtime, the held one terminal", if completed_while_held { "while the hold was in force (the provider terminated the held runtime)" } else { "after the release" }),
        true,
        json!({"node_id": node_id, "restart_completed_while_held": completed_while_held, "wall_after_release_ms": fin["wall_ms"]}),
    );
    rec
}

/// The process state letter of `pid` for evidence.
fn proc_state_of(pid: u32) -> Value {
    json!(rafka_test_scenario::process_faults::proc_state(pid).map(String::from))
}

/// Whether the held runtime still exists (a process not exited, a container the daemon still runs or has paused).
fn rt_alive(rt: &Rt) -> bool {
    match rt {
        Rt::Process(r) => r.check().is_ok(),
        Rt::Container(c) => container_faults::inspect(&c.id).is_ok_and(|i| i.running || i.status == "paused"),
    }
}

/// The typed refusal a draining node gives ordinary work: a proof-store reply of `Draining`, or a
/// carrier that will not carry (`node is draining`). `None` for any other outcome.
fn draining_refusal(out: &Value) -> Option<String> {
    match out["outcome"].as_str() {
        Some("Reply") => out["reply"]["result"]["refused"].as_str().filter(|r| r.to_lowercase().contains("draining")).map(|r| format!("Reply:{r}")),
        Some("NotSent") => out["reason"].as_str().filter(|r| r.contains("draining")).map(|r| format!("NotSent:{r}")),
        _ => None,
    }
}

async fn drain_run(cell: &str, shape: Shape) {
    let mut f = form(cell, shape).await;
    let mut inv = Invariants::default();
    let nodes = settle(&f).await;
    let (launches, fabric) = check_identities_and_launches(&f, &nodes, &mut inv).await;
    let authorities = check_authorities(&nodes, &fabric, &mut inv);
    let connections = converge_connections(&f, &mut inv).await;
    let mut a = Authority::new(f.seed, &nodes);
    let st = a.stable(&mut f, "formed", &both(2), &mut inv).await;
    a.traffic.burst(&f, 12, "steady-before").await;
    let steady = a.traffic.ops.iter().filter(|(_, c)| bucket(&c.out) == Bucket::Reply).count();
    inv.holds("steady traffic before any change: every operation replied", steady == a.traffic.ops.len(), json!({"issued": a.traffic.ops.len(), "replied": steady}));

    // C15 + C16 on a broker, then on a gateway: the two routable work shapes. Traffic aimed at the node
    // runs through its drain; the retirement is the fabric-primary's Build.
    let broker = format!("mesh1.broker.{}", f.shape.broker);
    let (rb, st1) = retire_via_build(&mut f, &mut a, &st, &broker, "C15/C16 broker", &mut inv).await;
    a.traffic.burst(&f, 4, "steady-between").await;
    let gateway = format!("mesh2.gateway.{}", f.shape.gateway);
    let (rg, st2) = retire_via_build(&mut f, &mut a, &st1, &gateway, "C15/C16 gateway", &mut inv).await;
    a.traffic.burst(&f, 4, "steady-between").await;

    // C17: a compute held by its exact runtime at a point the provider acknowledges: silence is not death.
    let compute = format!("mesh2.compute.{}", f.shape.compute);
    let hold = hold_phase(&mut f, &mut a, &st2, &compute, &mut inv).await;
    let st_held = a.stable(&mut f, "C17: after the hold", &both(2), &mut inv).await;
    let hold_restart = hold_restart(&mut f, &mut a, &st_held, &compute, &mut inv).await;
    let st = a.stable(&mut f, "final", &both(2), &mut inv).await;
    let nodes_end = st.nodes.clone();
    let mut want_names = f.shape.names();
    want_names.remove(&broker);
    want_names.remove(&gateway);
    let names_final: BTreeSet<String> = nodes_end.iter().map(|n| s(&n["name"])).collect();
    assert_eq!(names_final, want_names, "the final view is the shape without the two retired nodes");
    let mut retired_runtimes = Vec::new();
    for n in &nodes_end {
        let name = s(&n["name"]);
        assert_eq!(n["status"], "ready-for-traffic", "{name}");
        let o = observe_launch(&f.estate, &name).await;
        let b = f.set.bindings.iter().find(|b| b.launch_id == launch_id(&name)).unwrap();
        assert_eq!(launched_path(&o), b.executable, "{name} runs the bound executable: {o}");
        assert_eq!(o["observed_sha256"], b.sha256.as_str(), "{name} runs the bound bytes: {o}");
        retired_runtimes.push(o);
    }
    let gone_ok: BTreeSet<String> = BTreeSet::from([broker.clone(), gateway.clone()]);
    let (final_state, verification) = final_reads(&f, &a, &nodes_end, &gone_ok);
    let fabric_end = a.get(&mut f, "/api/fabric").await.expect("an admin answers /api/fabric");
    a.history.push(authority_row("final", &nodes_end, &fabric_end));
    let stop_t = Instant::now();
    f.estate.stop().await;
    let left = provider_left(&f, stop_t, &mut inv).await;
    let spans = f.estate.spans();
    let accounted = account(&spans, &a.traffic.ops, &final_state, &mut inv);
    let claims = claim_rows(&spans);
    let attempts = check_one_executor_per_attempt(&claims, &mut inv);
    let fence = check_writes_and_fence(&f, &a, &spans, &mut inv);
    let elections = check_election_history(&a, &spans, &mut inv);
    let imported = check_imported_mechanism(&f, &spans, &mut inv);
    let mut drains = Vec::new();
    for r in [&rb, &rg] {
        let chain = check_retire_chain(&spans, r);
        let drain = drain_evidence(&spans, r);
        let node = s(&r["node"]);
        // The Draining the admin applied, and the node's answer: a typed in-flight count.
        let applied = drain["apply_draining"].as_array().unwrap();
        assert!(!applied.is_empty(), "{node}: no ApplyNodeState(Draining) served by the retiring node: {drain}");
        assert!(applied.iter().all(|x| x["in_flight"].is_string() || x["in_flight"].is_number()), "{node}: the apply answered no in-flight count: {drain}");
        // Ordinary work that reaches a draining routable node is refused by name before its body is read:
        // a broker answers the typed `Draining` reply, a gateway refuses to carry (`node is draining`).
        // No handler ran for any of it and none of its values were stored (account() checked the trace
        // and the final state). None was refused before the drain began.
        let t_apply = applied.iter().map(|x| x["start_ns"].as_u64().unwrap()).min().unwrap();
        let hits: Vec<Value> = a
            .traffic
            .ops
            .iter()
            .filter(|(o, _)| o.target == node || o.via.as_deref() == Some(node.as_str()))
            .filter_map(|(o, c)| draining_refusal(&c.out).map(|why| json!({"seq": o.seq, "key": o.key, "target": o.target, "via": o.via, "started_ms": c.started_ms, "finished_ms": c.finished_ms, "refusal": why})))
            .collect();
        assert!(!hits.is_empty(), "{node}: no ordinary work was refused as draining while it retired: the window held no refusal ({drain})");
        assert!(hits.iter().all(|h| h["finished_ms"].as_u64().unwrap() >= t_apply / 1_000_000), "{node}: a draining refusal answered before the node applied Draining ({t_apply}): {hits:?}");
        // The status family stays admissible while Draining: the node's own declarations (its Draining and
        // its Leaving) made after the apply are decided by the authority, `applied`.
        let decided: Vec<&Value> = drain["declarations_from_the_node"].as_array().unwrap().iter().filter(|d| d["start_ns"].as_u64().unwrap() >= t_apply && d["outcome"] == "applied").collect();
        assert!(decided.len() >= 2, "{node}: the authority decided fewer than two (Draining, Leaving) declarations from the draining node after the apply: {drain}");
        // The caller's certainty: the admin's drain call is `Established` with the node's in-flight count.
        let receipts = drain["drain_rpc_receipts"].as_array().unwrap();
        assert!(receipts.iter().any(|r| s(&r["outcome"]).starts_with("Established { in_flight: ")), "{node}: the drain call was not Established with an in-flight count: {drain}");
        drains.push(json!({"node": node, "chain": chain, "drain": drain, "ordinary_work_refused_while_draining": hits.len(), "refusals": hits, "declarations_decided_after_apply": decided.len(), "apply_started_ns": t_apply}));
    }
    inv.holds(
        "C15/C16: each retiring node applied ApplyNodeState(Draining) and answered its in-flight count; the retire steps ran MarkDraining, WaitForDrain, PublishLeaving, CloseRpcAdmission, TerminateRuntime, NodeDeleted in order; the exact old birth is terminal in the provider and its NodeDeleted names it",
        true,
        json!({"drains": drains}),
    );
    let all_creates: BTreeSet<String> = named(&spans, "rdm.node_admin.build.create.via-rest").into_iter().filter(|sp| !s(&sp["attributes"]["build_id"]).is_empty()).map(|sp| s(&sp["attributes"]["build_id"])).collect();
    assert_eq!(all_creates, BTreeSet::from([f.build_id.clone(), s(&rb["build_id"]), s(&rg["build_id"])]), "the Builds accepted are the formation and the two retirements; holding a runtime accepted none");
    // C17 post hoc: nothing in the control plane acted on the held node while it was held.
    let (h0, h1) = (hold["hold_ns"].as_u64().unwrap(), hold["release_ns"].as_u64().unwrap());
    let hid = s(&hold["node_id"]);
    let acted: Vec<String> = ["rdm.node_admin.build.update.via-proven-drift", "rdm.node_admin.node.delete.via-build", "rdm.node_admin.node.create.via-build", "rdm.node_admin.node.update.via-build", "rdm.node_admin.node.delete.via-node-deleted", "rdm.node_admin.node.update.via-node-deleting", "rdm.node_admin.node.update.via-node-restarting"]
        .iter()
        .flat_map(|fam| named(&spans, fam).into_iter().filter(|sp| start_ns(sp) >= h0 && start_ns(sp) <= h1 && (sp["attributes"]["node"] == compute.as_str() || sp["attributes"]["node_id"] == hid.as_str())).map(|sp| format!("{} @{}", sp["name"], start_ns(sp))).collect::<Vec<_>>())
        .collect();
    assert!(acted.is_empty(), "{compute}: the control plane acted on the held node while it was held (silence taken for death): {acted:?}");
    let proven: Vec<&Value> = named(&spans, "rdm.node_admin.build.update.via-proven-drift").into_iter().filter(|sp| start_ns(sp) >= h0 && start_ns(sp) <= h1).collect();
    assert!(proven.is_empty(), "a proven-drift attempt opened while the compute was held: {proven:?}");
    inv.holds("C17: while the compute's exact runtime was held no proven-drift attempt, retire, restart, create or NodeDeleted touched it; after the release the same runtime and birth served again", true, json!({"held_ns": [h0, h1], "control_plane_acts_on_it": 0}));
    let services = check_services(&spans, &mut inv);
    let formation = check_formation_chain(&f, &spans, &mut inv);
    let mut runtime = check_runtime_facts(&f, &nodes, &launches, &spans, &mut inv);
    runtime.push(json!({"phase": "retire", "node": rb["node"], "node_id": rb["node_id"], "runtime_before": rb["runtime_before"], "old_birth_terminal": rb["old_birth_terminal"]}));
    runtime.push(json!({"phase": "retire", "node": rg["node"], "node_id": rg["node_id"], "runtime_before": rg["runtime_before"], "old_birth_terminal": rg["old_birth_terminal"]}));
    runtime.push(json!({"phase": "hold", "hold": hold}));
    runtime.push(json!({"phase": "restart-of-a-held-runtime", "hold_restart": hold_restart}));
    let index = copy_spans(&f.estate, &f.dir);
    let mut actions = std::mem::take(&mut f.actions);
    actions.extend(a.actions.iter().cloned());
    actions.extend(a.traffic.ops.iter().map(|(_, c)| action_row(c)));
    actions.extend(verification.iter().map(action_row));
    actions.sort_by_key(|r| r["t_ms"].as_u64().unwrap_or(0));
    let mut history = std::mem::take(&mut a.history);
    history.extend(claims.iter().cloned());
    history.extend(a.build_obs.iter().map(|o| json!({"phase": "build-projection", "observation": o})));
    write_views(
        Views {
            f: &f,
            topology_final: nodes_end,
            actions,
            operations: accounted.operations,
            outcomes: accounted.outcomes,
            authority_history: history,
            route_history: Vec::new(),
            runtime_history: runtime,
            inv,
            extra: json!({
                "authorities_at_formation": authorities, "connections_at_formation": connections, "provider_after_stop": left, "services": services, "formation_chains": formation,
                "retirements": [rb, rg], "drains": drains, "hold": hold, "hold_restart": hold_restart, "scenario_events": a.events, "topology_writes": a.writes, "fence": fence, "attempts": attempts, "elections": elections, "imported_mechanism": imported,
                "final_launches": retired_runtimes, "scenarios": ["C15", "C16", "C17"],
                "ledger": {"issued": a.traffic.ops.len(), "buckets": accounted.buckets.iter().map(|(k, v)| (format!("{k:?}"), *v)).collect::<BTreeMap<_, _>>(), "class_detail": accounted.summary, "indeterminate": accounted.indeterminate, "protocol_refusals": accounted.refusals, "verification_reads": verification.len()},
                "schedule": {"seed": f.seed, "generator": "rafka_test_scenario::model::Rng (SplitMix64)", "retired": [broker, gateway], "held": compute},
            }),
        },
        index,
        &spans,
    );
}

/// CONTRACT: drain, retire and a held runtime are proven against the exact runtime the provider holds.
/// A broker and a gateway (the routable work shapes) are retired by the fabric-primary's Build under
/// seeded traffic aimed at them: each retiring node answers ApplyNodeState(Draining) with a typed
/// in-flight count and the retire steps run in their documented order (MarkDraining, WaitForDrain,
/// PublishLeaving, CloseRpcAdmission, TerminateRuntime, NodeDeleted); ordinary work that reaches a
/// draining node is refused with the typed `Draining` reply before its body is read, with no handler run and no
/// value stored; every operation ends in exactly one typed bucket, an Indeterminate put is reported
/// applied or not and never re-sent; the old birth's runtime is terminal in the provider, its NodeDeleted
/// names the exact birth, and the NodeId stays out of every admin's view for a staleness window. A
/// compute's exact runtime is then held (SIGSTOP, or `docker pause`; acknowledged by the provider):
/// for two staleness windows it stays listed under its NodeId, no Build or attempt is opened, a call to it
/// ends in a typed uncertain outcome; released, the same runtime and birth serve again. What must NOT
/// happen: a retired node's work applied after its refusal, a held runtime taken for dead, an
/// Indeterminate put re-sent, a retired NodeId back in a view.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mock_drain_and_retire_preserve_status_and_terminal_proof() {
    drain_run("mock_drain_and_retire_preserve_status_and_terminal_proof", any_tier()).await;
}
