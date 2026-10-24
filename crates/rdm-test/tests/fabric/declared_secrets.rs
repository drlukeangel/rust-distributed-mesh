//! product=mesh, feature=deployment, subfeature=declared-secrets, rung=MM, provider=process and
//! container (the container cells in [`container`], opt-in).
//!
//! The declared secrets (Q150 §0.1; deployment-provider.md §2.3): an application's wiring declares each
//! per-process variable by name with the node kinds it is set on. The bootstrap admin reads each value
//! once from its own environment; every node-admin RDM launches is written them beside the root, in the
//! custody document (`RDM_DECLARED_SECRETS`, `{"v":1,"declared":[{"name","kinds","value"}]}`); every
//! admin keeps them beside its data dir (`declared-secrets`) and reads them back on a start without a
//! launcher, so the value never changes while the fabric runs. A value is set in the process
//! environment of the named kinds only, and no process inherits a declared name from its launcher. A
//! declared name RDM sets or passes through, and a name declared twice, are refused at the admin's
//! start by name, naming the fabric's provider, with nothing bootstrapped. RDM's diagnostics carry no
//! value: a failing child's last words are scrubbed of every declared value and RDM's four secrets
//! before they reach a step's failure, its receipt or its span.
//!
//! Every estate runs the testkit's faulted node-admin, whose application declares
//! `TESTKIT_DECLARED_SECRET` on the broker and the gateway (`rdm_test::test_app::declared`). Each
//! process's environment is read from `/proc/<pid>/environ`.

use rdm_admin::deployment::declared::DeclaredSecrets;
use rdm_client::{FabricDesired, MeshDesired};
use rdm_core::launch::{DeclaredSecret, IssuingMaterial, SecretHex, ENV_CA_SECRET, ENV_DECLARED_SECRETS, ENV_FABRIC_CA_KEY, ENV_ISSUING_KEY};
use rdm_core::NodeKind;
use rdm_test::estate::{Estate, Owner};
use rdm_test::faults::{binding_set_with_roles, candidate_sha};
use rdm_test::test_app::{DECLARED_KINDS, DECLARED_SECRET};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

const SETTLE: Duration = Duration::from_secs(180);

/// The estate's bootstrap admin: the declared value's source, started with it in its environment.
const BOOTSTRAP: &str = "mesh1.admin.1";

/// The file beside an admin's data dir that keeps the declared values (the custody document).
const KEPT: &str = "declared-secrets";

fn owner(test: &str, provider: &str) -> Owner {
    Owner { product: "mesh".into(), feature: "deployment".into(), subfeature: "declared-secrets".into(), rung: "MM".into(), provider: provider.into(), test: test.into() }
}

fn s(v: &Value) -> String {
    v.as_str().unwrap_or_default().to_string()
}

/// A value no other run holds, holding a separator, `=` and a newline: it must arrive unchanged.
fn fresh_value() -> String {
    format!("declared,value={}\nsecond={}", rdm_core::NodeId::mint(), rdm_core::NodeId::mint())
}

/// Every spelling of `value` a file of the run could carry it in: as given, each of its lines (a
/// diagnostic that joins a log's lines carries them apart), JSON-escaped, and its bytes as hex.
fn spellings(value: &str) -> Vec<String> {
    let escaped = serde_json::to_string(value).unwrap();
    let mut out = vec![value.to_string(), escaped[1..escaped.len() - 1].to_string(), rdm_core::launch::encode_hex(value.as_bytes())];
    out.extend(value.lines().filter(|l| !l.trim().is_empty()).map(str::to_string));
    out
}

/// Whether the opt-in container step runs this cell; otherwise it skips by name.
fn container_opted_in(what: &str) -> bool {
    if std::env::var("RDM_CONTAINER_PROOF").as_deref() != Ok("1") && std::env::var("RDM_REQUIRE_CONTAINER").as_deref() != Ok("1") {
        eprintln!("SKIP container {what}: opt-in with RDM_CONTAINER_PROOF=1 (the container-proof step)");
        return false;
    }
    true
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

/// Every node the estate's view holds but the bootstrap admin (the value's source), with its kind, its
/// data dir and its process's environment.
async fn environs(estate: &Estate) -> Vec<(String, NodeKind, PathBuf, BTreeMap<String, String>)> {
    let bootstrap = BOOTSTRAP.to_string();
    let mut out = Vec::new();
    for n in estate.node_values().await {
        let name = s(&n["name"]);
        if name == bootstrap {
            continue;
        }
        let kind = NodeKind::ALL.into_iter().find(|k| k.name() == n["kind"]).unwrap_or_else(|| panic!("{name}: kind {}", n["kind"]));
        let pid = estate.pid_of(&name).await;
        out.push((name.clone(), kind, PathBuf::from(estate.data_dir(&name).await), environ(&name, pid)));
    }
    out
}

/// The declared secret's value the custody document `doc` carries, with its kinds.
fn custody_value(doc: &str) -> Option<(String, Value)> {
    let doc: Value = serde_json::from_str(doc).ok()?;
    if doc["v"] != 1 {
        return None;
    }
    let entry = doc["declared"].as_array()?.iter().find(|e| e["name"] == DECLARED_SECRET)?.clone();
    Some((entry["value"].as_str()?.to_string(), entry["kinds"].clone()))
}

/// The declared value the custody document in `env` (`RDM_DECLARED_SECRETS`) carries.
fn custody_of(env: &BTreeMap<String, String>) -> Option<String> {
    custody_value(env.get(ENV_DECLARED_SECRETS)?).map(|(v, _)| v)
}

/// The declared value the admin's data dir `dir` keeps (`declared-secrets`), checked to name the
/// declared kinds.
fn kept_in(dir: &Path) -> Option<String> {
    let (value, kinds) = custody_value(&std::fs::read_to_string(dir.join(KEPT)).ok()?)?;
    let want: Vec<&str> = DECLARED_KINDS.iter().map(|k| k.name()).collect();
    assert_eq!(kinds, json!(want), "{}: the kept custody document names the declared kinds", dir.display());
    Some(value)
}

/// Every file under `dir`.
fn walk(dir: &Path) -> Vec<PathBuf> {
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

/// The files under `dirs` that carry `value` in any spelling, but the files `permitted` names, and how
/// many files were read.
fn carriers(dirs: &[PathBuf], value: &str, permitted: impl Fn(&Path) -> bool) -> (usize, Vec<String>) {
    let spelt = spellings(value);
    let mut read = 0usize;
    let mut out = Vec::new();
    for dir in dirs {
        for f in walk(dir) {
            if permitted(&f) {
                continue;
            }
            read += 1;
            let Ok(bytes) = std::fs::read(&f) else { continue };
            let text = String::from_utf8_lossy(&bytes);
            if spelt.iter().any(|v| text.contains(v.as_str())) {
                out.push(f.display().to_string());
            }
        }
    }
    (read, out)
}

/// Cells 1 and 2 (Q150): on `provider`, a declared secret set in the bootstrap admin's environment
/// reaches the named kinds' processes, with its value unchanged, and no other process: a launched
/// node-admin (not a named kind) carries the custody document `RDM_DECLARED_SECRETS` and keeps it beside
/// its data dir, and never the app's name; an rpc node and a compute node (not named) carry neither.
async fn a_declared_secret_reaches_only_the_named_kinds(test: &str, provider: &str) {
    let value = fresh_value();
    let mut estate = estate_holding(test, provider, &value).await;
    let mesh1 = mesh_of_every_kind("mesh1");
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1] }).await;
    let want: BTreeSet<String> = ["admin.1", "admin.2", "rpc.1", "broker.1", "gateway.1", "compute.1"].iter().map(|n| format!("mesh1.{n}")).collect();
    estate.settles_to_named(&want, SETTLE).await;
    let seen = environs(&estate).await;
    estate.artifact("kinds.json", &json!(seen.iter().map(|(n, k, _, env)| (n.clone(), json!({ "kind": k.name(), "holds": env.get(DECLARED_SECRET) == Some(&value), "custody": custody_of(env).is_some() }))).collect::<BTreeMap<_, _>>()));
    assert_eq!(seen.len(), want.len() - 1, "every node but the bootstrap admin is read: {:?}", seen.iter().map(|(n, ..)| n).collect::<Vec<_>>());
    assert_eq!(kept_in(&estate.bootstrap_data_dir("mesh1")).as_deref(), Some(value.as_str()), "the bootstrap admin keeps the value it read beside its data dir");
    for (name, kind, dir, env) in &seen {
        if DECLARED_KINDS.contains(kind) {
            assert_eq!(env.get(DECLARED_SECRET), Some(&value), "{name} ({}) is a named kind: its process holds the declared value unchanged", kind.name());
        } else {
            assert!(!env.contains_key(DECLARED_SECRET), "{name} ({}) is not a named kind: its process holds no {DECLARED_SECRET} (inherited or set)", kind.name());
        }
        if *kind == NodeKind::NodeAdmin {
            assert_eq!(custody_of(env).as_deref(), Some(value.as_str()), "{name}: a launched node-admin is written the custody document beside the root");
            assert_eq!(kept_in(dir).as_deref(), Some(value.as_str()), "{name}: a launched node-admin keeps the custody document beside its data dir");
        } else {
            assert!(!env.contains_key(ENV_DECLARED_SECRETS), "{name} ({}): only a node-admin is written the custody document", kind.name());
            assert!(!dir.join(KEPT).exists(), "{name} ({}): only a node-admin keeps the declared values", kind.name());
        }
    }
    estate.stop().await;
}

/// CONTRACT (Q150 cells 1, 2; process provider).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declared_secret_reaches_only_the_named_kinds_processes() {
    a_declared_secret_reaches_only_the_named_kinds("a_declared_secret_reaches_only_the_named_kinds_processes", "process").await;
}

/// Cells 4 and 5 (Q150): on `provider`, a create a launched admin runs sets the declared value. In a
/// two-mesh fabric, mesh2's broker is launched by a node-admin of mesh2 (its `RDM_LAUNCHER`), never the
/// bootstrap admin, and holds the value from that admin's custody document; mesh2's compute holds none.
/// Then the secrecy assertions as §0.1 states them: an admin's environment and its data-dir file hold
/// the custody document, which is permitted; no RDM diagnostic, receipt, span, row or Build record
/// carries the value: every other file the run wrote (the span JSONL, every node's logs and data dir,
/// every file under the store root) is read for it, as given, JSON-escaped and as hex.
async fn a_launched_admins_create_sets_the_value_and_no_file_carries_it(test: &str, provider: &str) {
    let value = fresh_value();
    let mut estate = estate_holding(test, provider, &value).await;
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
    assert_eq!(custody_of(&admin).as_deref(), Some(value.as_str()), "{by}, which ran the create, holds the custody document (permitted)");
    assert!(!admin.contains_key(DECLARED_SECRET), "{by} is not a named kind: its process holds no {DECLARED_SECRET}");
    assert_eq!(broker.get(DECLARED_SECRET), Some(&value), "mesh2.broker.1 holds the declared value {by} launched it with");
    let compute = environ("mesh2.compute.1", estate.pid_of("mesh2.compute.1").await);
    assert!(!compute.contains_key(DECLARED_SECRET) && !compute.contains_key(ENV_DECLARED_SECRETS), "mesh2.compute.1 is not a named kind");
    let mut admin_dirs = vec![estate.bootstrap_data_dir("mesh1")];
    for n in want.iter().filter(|n| n.contains(".admin.") && **n != bootstrap) {
        admin_dirs.push(PathBuf::from(estate.data_dir(n).await));
    }
    estate.stop().await;
    // Cell 5: the custody document kept beside each admin's data dir is the one permitted file; no
    // other file of the run carries the value.
    for dir in &admin_dirs {
        assert_eq!(kept_in(dir).as_deref(), Some(value.as_str()), "{}: every admin keeps the custody document beside its data dir (permitted)", dir.display());
    }
    let permitted = |f: &Path| f.file_name().is_some_and(|n| n == KEPT) && admin_dirs.iter().any(|d| f.parent() == Some(d.as_path()));
    let (read, carried) = carriers(&[estate.root.clone(), estate.evidence.clone(), estate.fabric_store_root()], &value, permitted);
    assert!(!estate.spans().is_empty() && read > 0, "the run wrote spans and files to read");
    assert!(carried.is_empty(), "no span, log, row, Build record, receipt or launch record carries the declared value; {read} files read: {carried:?}");
}

/// CONTRACT (Q150 cells 4, 5; process provider).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_launched_admins_create_sets_the_declared_value_and_no_file_of_the_run_carries_it() {
    let test = "a_launched_admins_create_sets_the_declared_value_and_no_file_of_the_run_carries_it";
    a_launched_admins_create_sets_the_value_and_no_file_carries_it(test, "process").await;
}

/// Q150 §0.1 ("read back on a start from disk without a launcher"): on `provider`, the bootstrap admin
/// reads the value from its environment once and keeps it beside its data dir; every node-admin is
/// lost; a person starts a node-admin again on that data dir with no launcher and without the value in
/// its environment (fabric recovery); the broker it then creates holds the value unchanged.
async fn an_admin_started_again_from_its_data_dir_launches_the_named_kinds_with_the_value(test: &str, provider: &str) {
    assert!(std::env::var(DECLARED_SECRET).is_err(), "the cell's own environment holds no {DECLARED_SECRET}: the restarted admin is started without it");
    let value = fresh_value();
    let mut estate = estate_holding(test, provider, &value).await;
    estate.until_fabric_ready(SETTLE).await;
    let data_dir = estate.bootstrap_data_dir("mesh1");
    assert_eq!(kept_in(&data_dir).as_deref(), Some(value.as_str()), "the bootstrap admin keeps the value it read beside its data dir");
    estate.kill_bootstrap();
    let restarted = estate.restart_admin_with(&data_dir, &[("RDM_MESH_PRIMARY", "1"), ("RDM_FABRIC_PRIMARY", "1")]);
    estate.enter(&restarted).await;
    estate.settles_to_named(&BTreeSet::from([BOOTSTRAP.to_string()]), SETTLE).await;
    estate.until_fabric_ready_at(&restarted, SETTLE).await;
    let mesh1 = MeshDesired { name: "mesh1".into(), node_admin: 1, rpc_node: 0, broker: 1, gateway: 0, compute: 1 };
    estate.reconcile(&FabricDesired { fabric: "fabric1".into(), meshes: vec![mesh1] }).await;
    let want: BTreeSet<String> = ["admin.1", "broker.1", "compute.1"].iter().map(|n| format!("mesh1.{n}")).collect();
    estate.settles_to_named(&want, SETTLE).await;
    let broker = environ("mesh1.broker.1", estate.pid_of("mesh1.broker.1").await);
    let launcher = broker.get("RDM_LAUNCHER").cloned().unwrap_or_default();
    assert!(launcher.starts_with(&format!("{BOOTSTRAP},")), "mesh1.broker.1 was launched by the admin started again on {}: RDM_LAUNCHER={launcher}", data_dir.display());
    assert_eq!(broker.get(DECLARED_SECRET), Some(&value), "the broker the restarted admin launched holds the value kept beside its data dir, unchanged");
    let compute = environ("mesh1.compute.1", estate.pid_of("mesh1.compute.1").await);
    assert!(!compute.contains_key(DECLARED_SECRET), "mesh1.compute.1 is not a named kind");
    assert_eq!(kept_in(&data_dir).as_deref(), Some(value.as_str()), "the kept value is unchanged");
    estate.stop().await;
}

/// CONTRACT (Q150 §0.1, the value's life; process provider).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_admin_started_again_from_its_data_dir_without_a_launcher_launches_the_named_kinds_with_the_value_processes() {
    let test = "an_admin_started_again_from_its_data_dir_without_a_launcher_launches_the_named_kinds_with_the_value_processes";
    an_admin_started_again_from_its_data_dir_launches_the_named_kinds_with_the_value(test, "process").await;
}

/// A bootstrap admin of `provider` whose wiring declares `declared` is refused at its start, and
/// bootstraps nothing: no file is written, no ca-secret, no row, no record, no kept value. The refusal
/// is returned.
async fn refused_at_start(provider: &str, declared: Vec<DeclaredSecret>) -> String {
    let dir = std::env::temp_dir().join(format!("declared-refused-{}", rdm_core::NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    let d = dir.display().to_string();
    let cfg = rdm_admin::admin::AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(d.clone()),
        "MESH_SPAWN_TYPE" => Some(provider.into()),
        _ => None,
    })
    .expect("a bootstrap configuration");
    let wiring = rdm_admin::wiring::Wiring { certs: rdm_test::test_certs::root_choice(), fabric_hooks: rdm_admin::wiring::FabricHooks::no_app_work(), declared, ..rdm_admin::wiring::Wiring::default() };
    let why = rdm_admin::admin::start_with(cfg, wiring).await.err().unwrap_or_else(|| panic!("{provider}: the declaration is refused at start"));
    let written = walk(&dir);
    assert!(written.is_empty(), "{provider}: nothing bootstrapped: no file is written: {written:?} ({why})");
    let _ = std::fs::remove_dir_all(&dir);
    why
}

/// CONTRACT (Q150 cell 3, amendment 3; R-D3.3), under both providers: a bootstrap admin whose wiring
/// declares a name RDM sets or passes through (`RDM_*`, the trace context, the pass-through list) is
/// refused at its start by name, naming the fabric's provider, and bootstraps nothing. Must NOT happen:
/// the admin comes up and hands its own variable on under the app's declaration.
#[test]
fn a_declared_name_rdm_sets_or_passes_through_refuses_the_start_and_bootstraps_nothing() {
    let cap = crate::common::capture("a_declared_name_rdm_sets_or_passes_through_refuses_the_start_and_bootstraps_nothing");
    cap.run(crate::common::Threads::Current, async move {
        for provider in ["process", "container"] {
            for (name, says) in [("RDM_CA_SECRET", "`RDM_*`"), ("TRACEPARENT", "trace context"), ("RUST_LOG", "passes it through")] {
                let why = refused_at_start(provider, vec![DeclaredSecret { name: name.into(), kinds: vec![NodeKind::Broker] }]).await;
                assert!(why.contains("refusing to start") && why.contains(&format!("`{name}`")) && why.contains(says), "the refusal names {name} and why: {why}");
                assert!(why.contains(&format!("the fabric's provider is {provider}")), "the refusal names the fabric's provider ({provider}): {why}");
            }
        }
    })
}

/// CONTRACT (Q150 §0.1, a duplicate declaration), under both providers: two declarations of one name
/// are refused at the admin's start, before anything is bootstrapped, naming the variable and the
/// fabric's provider; they are not merged and registration order chooses nothing.
#[test]
fn a_name_declared_twice_refuses_the_start_and_bootstraps_nothing() {
    let cap = crate::common::capture("a_name_declared_twice_refuses_the_start_and_bootstraps_nothing");
    cap.run(crate::common::Threads::Current, async move {
        for provider in ["process", "container"] {
            let twice = vec![DeclaredSecret { name: "APP_DECLARED_TWICE".into(), kinds: vec![NodeKind::Broker] }, DeclaredSecret { name: "APP_DECLARED_TWICE".into(), kinds: vec![NodeKind::Gateway] }];
            let why = refused_at_start(provider, twice).await;
            assert!(why.contains("refusing to start") && why.contains("`APP_DECLARED_TWICE`") && why.contains("declared twice"), "the refusal names the variable declared twice: {why}");
            assert!(why.contains(&format!("the fabric's provider is {provider}")), "the refusal names the fabric's provider ({provider}): {why}");
        }
    })
}

/// Q150 §0.1 (redaction: RDM's own diagnostics scrubbed before they reach a receipt or a span): on
/// `provider`, a node-admin child launched with the declared secret set on its kind, RDM's four secrets
/// in its launch (`RDM_CA_SECRET`, `RDM_ISSUING_KEY`, `RDM_FABRIC_CA_KEY`, `RDM_DECLARED_SECRETS`)
/// prints every one of them and exits before it binds. The create fails at `WaitForBind` with the
/// child's own last words, and neither the step's failure, its Failed receipt, any span the run
/// recorded (in memory and in its span JSONL) nor any other file under the launch's data root carries
/// any of them; the child's own logs, which it wrote its values into, are excluded by name.
fn a_failing_child_leaves_no_trace_of_a_secret(test: &str, spawn_type: &'static str) {
    use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
    use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, LaunchTemplate, NodeObserver, Timeouts, TopologySink};
    use rdm_admin::deployment::provider::FabricPolicy;
    use rdm_core::{FabricId, IncarnationId, NodeId};
    struct Discard;
    impl TopologySink for Discard {
        fn publish(&self, _: rdm_admin::model::Node) {}
        fn remove(&self, _: &rdm_admin::model::PathName) {}
        fn remove_birth(&self, _: &rdm_admin::model::PathName, _: &NodeId, _: &IncarnationId) {}
    }
    struct Never;
    #[async_trait::async_trait]
    impl NodeObserver for Never {
        async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<rdm_admin::deployment::pipeline::Publication> {
            None
        }
        async fn ready(&self, _: &rdm_admin::model::Node) -> Result<(), String> {
            Err("never".into())
        }
    }
    let cap = crate::common::capture(test);
    let spans_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit").join(test);
    let value = format!("sentinel,value={}\nsecond={}", NodeId::mint(), NodeId::mint());
    let secret = |what: &str| SecretHex(rdm_core::launch::encode_hex(format!("{what}-{}", NodeId::mint()).as_bytes()));
    let issuing = IssuingMaterial { key: secret("issuing-key"), cert: "00abff".into(), root: secret("root"), ca_key: secret("ca-key") };
    let rdm_secrets: Vec<(&str, String)> = vec![(ENV_CA_SECRET, issuing.root.0.clone()), (ENV_ISSUING_KEY, issuing.key.0.clone()), (ENV_FABRIC_CA_KEY, issuing.ca_key.0.clone())];
    let (held_value, held_issuing, held_root) = (value.clone(), issuing.clone(), std::env::temp_dir().join(format!("declared-failing-{}", NodeId::mint())));
    let failed = cap.run(crate::common::Threads::Workers(2), async move {
        let fabric_id = FabricId::mint();
        let fabric = format!("fab-{fabric_id}");
        let _cleanup = (spawn_type == "container").then(|| crate::common::ContainerFabricCleanup(fabric.clone()));
        let _reap = crate::common::ReapOnDrop(held_root.clone());
        let prepared = rdm_admin::deployment::prepare(FabricPolicy::bootstrap(Some(spawn_type)).unwrap(), &fabric).await.unwrap_or_else(|e| panic!("the {spawn_type} provider: {e}"));
        let store = held_root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        // The child: prints every secret it holds to its stderr, then exits before it binds.
        let bin = held_root.join("bin");
        std::fs::create_dir_all(&bin).unwrap();
        let script = bin.join("prints-its-secrets");
        std::fs::write(
            &script,
            format!("#!/usr/bin/sh\nprintf 'app %s\\n' \"${DECLARED_SECRET}\" >&2\nprintf 'rdm %s %s %s\\n' \"${ENV_CA_SECRET}\" \"${ENV_ISSUING_KEY}\" \"${ENV_FABRIC_CA_KEY}\" >&2\nprintf 'custody %s\\n' \"${ENV_DECLARED_SECRETS}\" >&2\nexit 3\n"),
        )
        .unwrap();
        std::fs::set_permissions(&script, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        let template = LaunchTemplate {
            reads: Vec::new(),
            fabric: "fabric1".into(),
            fabric_id,
            executable: script,
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: BTreeMap::new(),
            data_root: held_root.clone(),
            store_dir: store,
            issuing: Some(held_issuing),
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: DeclaredSecrets { declared: vec![DeclaredSecret { name: DECLARED_SECRET.into(), kinds: vec![NodeKind::NodeAdmin] }], values: BTreeMap::from([(DECLARED_SECRET.to_string(), SecretHex::of_value(&held_value))]) },
            binding: None,
        };
        let builds = MemoryBuildStateAdapter::new();
        let build_id = crate::common::publish_build(&builds, crate::common::add_node()).await;
        let joins = rdm_admin::join::Joins::default();
        let pipeline = DeploymentPipeline {
            provider: &*prepared.provider,
            joins: &joins,
            observer: &Never,
            sink: &Discard,
            lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts { bind: Duration::from_secs(20), ..Timeouts::default() },
        };
        let err = pipeline
            .create(&CreateRequest { build_id: build_id.clone(), attempt: 1, node: "mesh1.admin.2".parse().unwrap(), spec: &rdm_admin::deployment::endpoint::RPC_NODE, data_dir: None, held_runtimes: Vec::new() })
            .await
            .unwrap_err();
        let receipts: Vec<String> = builds
            .read_build(&build_id)
            .await
            .unwrap()
            .steps
            .iter()
            .filter_map(|r| match &r.outcome {
                StepOutcome::Failed { reason, .. } => Some(format!("{}: {reason}", r.step)),
                _ => None,
            })
            .collect();
        // Every file the launch wrote, read before the data root is removed.
        let files: Vec<(PathBuf, String)> = walk(&held_root).into_iter().filter_map(|f| Some((f.clone(), String::from_utf8_lossy(&std::fs::read(&f).ok()?).to_string()))).collect();
        (err.step, err.reason, receipts, files)
    });
    let (step, reason, receipts, files) = failed;
    assert_eq!(step, "WaitForBind", "the child exits before it binds: {reason}");
    assert!(reason.contains("runtime exited") && reason.contains("app ") && reason.contains("rdm ") && reason.contains("custody "), "the child's own last words are carried: {reason}");
    let mut secrets: Vec<(String, String)> = rdm_secrets.into_iter().map(|(k, v)| (k.to_string(), v)).collect();
    secrets.extend(spellings(&value).into_iter().map(|v| (DECLARED_SECRET.to_string(), v)));
    let held: Vec<(String, BTreeMap<String, String>)> = cap.spans.0.lock().unwrap().values().map(|(name, _, fields)| (name.clone(), fields.clone())).collect();
    let mut leaks = Vec::new();
    for (name, v) in &secrets {
        if reason.contains(v.as_str()) {
            leaks.push(format!("the step's failure carries {name}"));
        }
        for r in receipts.iter().filter(|r| r.contains(v.as_str())) {
            leaks.push(format!("the Failed receipt carries {name}: {r}"));
        }
        for (span, fields) in held.iter().filter(|(_, f)| f.values().any(|f| f.contains(v.as_str()))) {
            leaks.push(format!("span {span} carries {name}: {fields:?}"));
        }
    }
    assert!(!receipts.is_empty() && !held.is_empty(), "the run wrote a Failed receipt and spans to read");
    assert!(leaks.is_empty(), "no RDM diagnostic carries a secret: {leaks:#?}");
    // The span JSONL and every other file the launch wrote, but the child's own logs, by name.
    let own_logs = |f: &Path| f.file_name().is_some_and(|n| n == "stdout.log" || n == "stderr.log");
    assert!(files.iter().any(|(f, _)| own_logs(f)) || spawn_type == "container", "the child wrote its own logs: {:?}", files.iter().map(|(f, _)| f).collect::<Vec<_>>());
    for (name, v) in &secrets {
        let (_, mut carried) = carriers(&[spans_dir.clone()], v, |_| false);
        carried.extend(files.iter().filter(|(f, text)| !own_logs(f) && text.contains(v.as_str())).map(|(f, _)| f.display().to_string()));
        assert!(carried.is_empty(), "no span JSONL or file of the launch carries {name}: {carried:?}");
    }
}

/// CONTRACT (Q150 §0.1, redaction; process provider).
#[test]
fn a_child_that_fails_after_printing_its_secrets_leaves_no_trace_of_them_in_the_failure_the_receipt_or_a_span_processes() {
    a_failing_child_leaves_no_trace_of_a_secret("a_child_that_fails_after_printing_its_secrets_leaves_no_trace_of_them_in_the_failure_the_receipt_or_a_span_processes", "process");
}

/// The container provider's cells: opt-in, the container-proof step (`RDM_CONTAINER_PROOF=1`, or
/// `RDM_REQUIRE_CONTAINER=1`); otherwise each skips by name.
mod container {
    use super::*;

    /// CONTRACT (Q150 cells 1, 2; container provider: the value reaches a named kind through `-e K`,
    /// the value in the docker client's environment, and a container inherits nothing).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_declared_secret_reaches_only_the_named_kinds_containers() {
        if !container_opted_in("declared secrets") {
            return;
        }
        a_declared_secret_reaches_only_the_named_kinds("a_declared_secret_reaches_only_the_named_kinds_containers", "container").await;
    }

    /// CONTRACT (Q150 cells 4, 5; container provider).
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_launched_admins_create_sets_the_declared_value_and_no_file_of_the_run_carries_it_containers() {
        if !container_opted_in("declared secrets") {
            return;
        }
        let test = "a_launched_admins_create_sets_the_declared_value_and_no_file_of_the_run_carries_it_containers";
        a_launched_admins_create_sets_the_value_and_no_file_carries_it(test, "container").await;
    }

    /// CONTRACT (Q150 §0.1, the value's life; container provider). Ignored by name: the estate starts a
    /// node-admin again on a data dir as a host process (`Estate::restart_admin_with`), and a node-admin
    /// started on an inherited data dir binds the inherited birth's address (admin/boot.rs, the
    /// inherited arm), which in a container fabric is the lost container's address on the fabric
    /// network: the host cannot bind it ("mesh address 172.20.255.254:0: Failed to bind sockets").
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    #[ignore = "a host-process node-admin started again on a container fabric's data dir cannot bind the inherited container address (admin/boot.rs inherited arm)"]
    async fn an_admin_started_again_from_its_data_dir_without_a_launcher_launches_the_named_kinds_with_the_value_containers() {
        if !container_opted_in("declared secrets") {
            return;
        }
        let test = "an_admin_started_again_from_its_data_dir_without_a_launcher_launches_the_named_kinds_with_the_value_containers";
        an_admin_started_again_from_its_data_dir_launches_the_named_kinds_with_the_value(test, "container").await;
    }

    /// CONTRACT (Q150 §0.1, redaction; container provider: the child's last words are the daemon's
    /// log of its container).
    #[test]
    fn a_child_that_fails_after_printing_its_secrets_leaves_no_trace_of_them_in_the_failure_the_receipt_or_a_span_containers() {
        if !container_opted_in("declared secrets") {
            return;
        }
        a_failing_child_leaves_no_trace_of_a_secret("a_child_that_fails_after_printing_its_secrets_leaves_no_trace_of_them_in_the_failure_the_receipt_or_a_span_containers", "container");
    }
}
