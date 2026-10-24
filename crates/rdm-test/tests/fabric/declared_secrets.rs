//! product=mesh, feature=deployment, subfeature=declared-secrets, rung=MM, provider=process (and
//! container, opt-in).
//!
//! The declared secrets (Q150; deployment-provider.md §2.3): an application's wiring declares each
//! per-process variable by name with the node kinds it is set on. The bootstrap admin reads each value
//! once from its own environment; every node-admin RDM launches is written them beside the root
//! (`RDM_DECLARED_SECRETS`); a value is set in the process environment of the named kinds only, and no
//! process inherits a declared name from its launcher. A declared name RDM sets or passes through is
//! refused at the admin's start by name, with nothing bootstrapped.
//!
//! Every estate runs the testkit's faulted node-admin, whose application declares
//! `TESTKIT_DECLARED_SECRET` on the broker and the gateway (`rdm_test::test_app::declared`). Each
//! process's environment is read from `/proc/<pid>/environ`.

use rdm_client::{FabricDesired, MeshDesired};
use rdm_core::launch::{DeclaredSecret, ENV_DECLARED_SECRETS};
use rdm_core::NodeKind;
use rdm_test::estate::{Estate, Owner};
use rdm_test::faults::{binding_set_with_roles, candidate_sha};
use rdm_test::test_app::{DECLARED_KINDS, DECLARED_SECRET};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(180);

/// The estate's bootstrap admin: the declared value's source, started with it in its environment.
const BOOTSTRAP: &str = "mesh1.admin.1";

fn owner(test: &str, provider: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "deployment".into(), subfeature: "declared-secrets".into(), rung: "MM".into(), provider: provider.into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// A value no other run holds.
fn fresh_value() -> String {
    format!("declared-value-{}", rdm_core::NodeId::mint())
}

/// A fabric of the faulted node-admin with the role kinds bound, its bootstrap admin started with the
/// declared secret's `value` in its own environment.
async fn estate_holding(test: &str, provider: &str, value: &str) -> Estate {
    let sha = candidate_sha();
    let mut set = binding_set_with_roles(&sha);
    if provider == "container" {
        for b in set.bindings.iter_mut() {
            b.image = Some(rdm_test::estate::RUNTIME_IMAGE.into());
        }
    }
    Estate::bootstrap_external_with_env(owner(test, provider), "fabric1", "mesh1", &set, &sha, &["rpc_node", "broker", "gateway", "compute"], &[(DECLARED_SECRET, value)])
        .await
        .expect("the faulted-admin binding set with the role kinds is accepted")
}

/// A mesh of two node-admins and one node of each other kind.
fn mesh_of_every_kind(name: &str) -> MeshDesired {
    MeshDesired { name: name.into(), node_admin: 2, rpc_node: 1, broker: 1, gateway: 1, compute: 1 }
}

/// `pid`'s environment, by name, from `/proc/<pid>/environ`.
fn environ(node: &str, pid: u64) -> BTreeMap<String, String> {
    let raw = std::fs::read(format!("/proc/{pid}/environ")).unwrap_or_else(|e| panic!("{node} (pid {pid}): /proc environ: {e}"));
    raw.split(|b| *b == 0).filter_map(|e| std::str::from_utf8(e).ok()?.split_once('=').map(|(k, v)| (k.to_string(), v.to_string()))).collect()
}

/// Every node the estate's view holds but the bootstrap admin (the value's source), with its kind and
/// its process's environment.
async fn environs(estate: &Estate) -> Vec<(String, NodeKind, BTreeMap<String, String>)> {
    let bootstrap = BOOTSTRAP.to_string();
    let mut out = Vec::new();
    for n in estate.node_values().await {
        let name = s(&n["name"]);
        if name == bootstrap {
            continue;
        }
        let kind = NodeKind::ALL.into_iter().find(|k| k.name() == n["kind"]).unwrap_or_else(|| panic!("{name}: kind {}", n["kind"]));
        let pid = estate.pid_of(&name).await;
        out.push((name.clone(), kind, environ(&name, pid)));
    }
    out
}

/// The `RDM_DECLARED_SECRETS` entry `env` carries for the declared secret: its value's bytes as hex.
fn custody_of(env: &BTreeMap<String, String>) -> Option<String> {
    env.get(ENV_DECLARED_SECRETS)?.split(',').find_map(|e| e.strip_prefix(&format!("{DECLARED_SECRET}=")).map(str::to_string))
}

/// Cells 1 and 2 (Q150): on `provider`, a declared secret set in the bootstrap admin's environment
/// reaches the named kinds' processes, with its value, and no other process: a launched node-admin
/// (not a named kind) carries the custody copy `RDM_DECLARED_SECRETS` and never the app's name; an rpc
/// node and a compute node (not named) carry neither: the process provider's inheritance no longer
/// hands the bootstrap admin's variable on.
async fn a_declared_secret_reaches_only_the_named_kinds(test: &str, provider: &str) {
    let value = fresh_value();
    let mut estate = estate_holding(test, provider, &value).await;
    let mesh1 = mesh_of_every_kind("mesh1");
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1] }).await;
    let want: BTreeSet<String> = ["admin.1", "admin.2", "rpc.1", "broker.1", "gateway.1", "compute.1"].iter().map(|n| format!("mesh1.{n}")).collect();
    estate.settles_to_named(&want, SETTLE).await;
    let seen = environs(&estate).await;
    estate.artifact("kinds.json", &json!(seen.iter().map(|(n, k, env)| (n.clone(), json!({ "kind": k.name(), "holds": env.get(DECLARED_SECRET) == Some(&value), "custody": custody_of(env).is_some() }))).collect::<BTreeMap<_, _>>()));
    assert_eq!(seen.len(), want.len() - 1, "every node but the bootstrap admin is read: {:?}", seen.iter().map(|(n, ..)| n).collect::<Vec<_>>());
    let hex = rdm_core::launch::encode_hex(value.as_bytes());
    for (name, kind, env) in &seen {
        if DECLARED_KINDS.contains(kind) {
            assert_eq!(env.get(DECLARED_SECRET), Some(&value), "{name} ({}) is a named kind: its process holds the declared value", kind.name());
        } else {
            assert!(!env.contains_key(DECLARED_SECRET), "{name} ({}) is not a named kind: its process holds no {DECLARED_SECRET} (inherited or set)", kind.name());
        }
        if *kind == NodeKind::NodeAdmin {
            assert_eq!(custody_of(env).as_deref(), Some(hex.as_str()), "{name}: a launched node-admin is written the custody copy beside the root");
        } else {
            assert!(!env.contains_key(ENV_DECLARED_SECRETS), "{name} ({}): only a node-admin is written the custody copy", kind.name());
        }
    }
    estate.stop().await;
}

/// CONTRACT (Q150 cells 1, 2; process provider).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declared_secret_reaches_only_the_named_kinds_processes() {
    a_declared_secret_reaches_only_the_named_kinds("a_declared_secret_reaches_only_the_named_kinds_processes", "process").await;
}

/// CONTRACT (Q150 cells 1, 2; container provider: the value reaches a named kind through `-e`, and a
/// container inherits nothing). Opt-in: the container-proof step (`RDM_CONTAINER_PROOF=1`, or
/// `RDM_REQUIRE_CONTAINER=1`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declared_secret_reaches_only_the_named_kinds_containers() {
    if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
        eprintln!("SKIP container declared secrets: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
        return;
    }
    a_declared_secret_reaches_only_the_named_kinds("a_declared_secret_reaches_only_the_named_kinds_containers", "container").await;
}

/// CONTRACT (Q150 cells 4, 5): a create a launched admin runs sets the declared value. In a two-mesh
/// fabric, mesh2's broker is launched by a node-admin of mesh2 (its `RDM_LAUNCHER`), never the
/// bootstrap admin, and holds the value from that admin's custody copy; mesh2's compute holds none.
/// Then no span, log, row, Build record or receipt carries the value: every file the run wrote (the
/// span JSONL, every node's logs and data dir, every file under the store root) is read for it, plainly
/// and as hex.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_launched_admins_create_sets_the_declared_value_and_no_file_of_the_run_carries_it() {
    let test = "a_launched_admins_create_sets_the_declared_value_and_no_file_of_the_run_carries_it";
    let value = fresh_value();
    let mut estate = estate_holding(test, "process", &value).await;
    let mesh = |m: &str| MeshDesired { name: m.into(), node_admin: 2, rpc_node: 0, broker: 1, gateway: 0, compute: 1 };
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh("mesh1"), mesh("mesh2")] }).await;
    let want: BTreeSet<String> = ["mesh1", "mesh2"].iter().flat_map(|m| ["admin.1", "admin.2", "broker.1", "compute.1"].map(|n| format!("{m}.{n}"))).collect();
    estate.settles_to_named(&want, SETTLE).await;
    estate.until_fabric_ready(SETTLE).await;
    let bootstrap = BOOTSTRAP.to_string();
    let broker = environ("mesh2.broker.1", estate.pid_of("mesh2.broker.1").await);
    let launcher = broker.get("RDM_LAUNCHER").cloned().unwrap_or_default();
    let by = launcher.split(',').next().unwrap_or_default().to_string();
    assert!(by.starts_with("mesh2.admin.") && by != bootstrap, "mesh2.broker.1 was launched by a launched node-admin of mesh2, not the bootstrap admin ({bootstrap}): RDM_LAUNCHER={launcher}");
    let admin = environ(&by, estate.pid_of(&by).await);
    assert_eq!(custody_of(&admin).as_deref(), Some(rdm_core::launch::encode_hex(value.as_bytes()).as_str()), "{by}, which ran the create, holds the custody copy");
    assert!(!admin.contains_key(DECLARED_SECRET), "{by} is not a named kind: its process holds no {DECLARED_SECRET}");
    assert_eq!(broker.get(DECLARED_SECRET), Some(&value), "mesh2.broker.1 holds the declared value {by} launched it with");
    let compute = environ("mesh2.compute.1", estate.pid_of("mesh2.compute.1").await);
    assert!(!compute.contains_key(DECLARED_SECRET) && !compute.contains_key(ENV_DECLARED_SECRETS), "mesh2.compute.1 is not a named kind");
    estate.stop().await;
    // Cell 5: no file of the run carries the value: the spans, every node's logs and data dir (rows,
    // receipts, launch records), and every file under the store root (Build records, rows).
    let hex = rdm_core::launch::encode_hex(value.as_bytes());
    let mut read = 0usize;
    let mut carriers = Vec::new();
    for dir in [estate.root.clone(), estate.evidence.clone(), estate.fabric_store_root()] {
        for f in walk(&dir) {
            read += 1;
            let Ok(bytes) = std::fs::read(&f) else { continue };
            let text = String::from_utf8_lossy(&bytes);
            if text.contains(&value) || text.contains(&hex) {
                carriers.push(f.display().to_string());
            }
        }
    }
    let spans = estate.spans();
    assert!(!spans.is_empty() && read > 0, "the run wrote spans and files to read");
    assert!(carriers.is_empty(), "no span, log, row, Build record, receipt or launch record carries the declared value; {read} files read: {carriers:?}");
}

/// CONTRACT (Q150 cell 3, amendment 3): a bootstrap admin whose wiring declares a name RDM sets or
/// passes through (`RDM_*`, the trace context, the pass-through list) is refused at its start by name,
/// and bootstraps nothing: no file is written, no ca-secret, no row, no record. Must NOT happen: the
/// admin comes up and hands its own variable on under the app's declaration.
#[test]
fn a_declared_name_rdm_sets_or_passes_through_refuses_the_start_and_bootstraps_nothing() {
    let cap = crate::common::capture("a_declared_name_rdm_sets_or_passes_through_refuses_the_start_and_bootstraps_nothing");
    cap.run(crate::common::Threads::Current, async move {
        for (name, says) in [("RDM_CA_SECRET", "`RDM_*`"), ("TRACEPARENT", "trace context"), ("RUST_LOG", "passes it through")] {
            let dir = std::env::temp_dir().join(format!("declared-refused-{}", rdm_core::NodeId::mint()));
            std::fs::create_dir_all(&dir).unwrap();
            let d = dir.display().to_string();
            let cfg = rdm_admin::admin::AdminConfig::from_env(|k| match k {
                "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(d.clone()),
                "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
                "MESH_SPAWN_TYPE" => Some("process".into()),
                _ => None,
            })
            .expect("a bootstrap configuration");
            let wiring = rdm_admin::wiring::Wiring {
                certs: rdm_test::test_certs::root_choice(),
                fabric_hooks: rdm_admin::wiring::FabricHooks::no_app_work(),
                declared: vec![DeclaredSecret { name: name.into(), kinds: vec![NodeKind::Broker] }],
                ..rdm_admin::wiring::Wiring::default()
            };
            let why = rdm_admin::admin::start_with(cfg, wiring).await.err().unwrap_or_else(|| panic!("a declared {name} is refused at start"));
            assert!(why.contains("refusing to start") && why.contains(&format!("`{name}`")) && why.contains(says), "the refusal names {name} and why: {why}");
            let written = walk(&dir);
            assert!(written.is_empty(), "nothing bootstrapped for a declared {name}: no file is written: {written:?}");
            let _ = std::fs::remove_dir_all(&dir);
        }
    })
}

/// Every file under `dir`.
fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
        let p = e.path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}
