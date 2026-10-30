//! product=mesh, feature=storage-backend, subfeature=memory-estate, rung=M1, provider=process.
//!
//! The runtime proof of the storage seam (i143 1b): a node-admin booted in this process on the
//! memory storage backend (`rdm_admin::storage_backend::MemoryBackend`) runs the node lifecycle with
//! every durable read and write of its own going through the six boundaries it opened. Built with the
//! file backend off (`--no-default-features`), the file store is absent from this executable; built
//! with it on, the admin still opens nothing but the memory boundaries. The rpc node the admin launches
//! is the gate's `rdm-node`, whose own connections rows are its node backend's (the file one).

use rdm_admin::admin::{start_on, AdminConfig};
use rdm_admin::storage_backend::MemoryBackend;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_client::{BuildCarrier, BuildId, Builds, Fabrics, LaunchKind, NodeStatus, Nodes, PathName, ScopeStatus};
use rdm_test::executables::{Current, Executables};
use rdm_core::build::BuildPhase;
use rdm_test::estate::{bin_dir, named, Estate};
use rdm_transport::{NodeRpcClient, ResolvedNode, StaticResolver};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

const WRONG: Duration = Duration::from_secs(120);
const NODE: &str = "mesh1.rpc.1";

/// The runtimes the cell's admin launched (every process whose `RDM_STORE_DIR` is the cell's store
/// dir) end when the cell does, a cell that panics included: the admin runs in this process, and
/// the rpc nodes it launched are processes of their own.
struct Launched(PathBuf);

impl Drop for Launched {
    fn drop(&mut self) {
        let want = format!("RDM_STORE_DIR={}", self.0.display());
        for e in std::fs::read_dir("/proc").into_iter().flatten().flatten() {
            let Some(pid) = e.file_name().to_str().and_then(|p| p.parse::<i32>().ok()) else { continue };
            let env = std::fs::read(e.path().join("environ")).unwrap_or_default();
            if env.split(|b| *b == 0).any(|kv| kv == want.as_bytes()) {
                unsafe { libc::kill(pid, libc::SIGKILL) };
            }
        }
    }
}

/// A Day-0 node-admin whose data dir is `<dir>/admin` and whose fabric's store dir is `<dir>/store`,
/// launching the built node executables of `bin_dir` on the process provider.
/// The name this cell registers the memory backend under (storage.md §2).
const MEMORY: &str = "memory";

fn day0_config(dir: &Path) -> AdminConfig {
    let (data, store) = (dir.join("admin"), dir.join("store"));
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(&store).unwrap();
    let bins = bin_dir().display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" => Some(data.display().to_string()),
        "RDM_STORE_DIR" => Some(store.display().to_string()),
        "RDM_BIN_DIR" => Some(bins.clone()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        rdm_core::launch::ENV_TOPOLOGY_STORAGE_PROVIDER => Some(MEMORY.into()),
        _ => None,
    })
    .expect("a readable environment")
}

/// Every file under `dir`, relative to it.
fn files_under(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for e in std::fs::read_dir(&d).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                stack.push(p);
            } else {
                out.push(p.strip_prefix(dir).unwrap().to_path_buf());
            }
        }
    }
    out.sort();
    out
}

/// Every span of the JSONL artifacts in `dir`.
fn spans_in(dir: &Path) -> Vec<Value> {
    std::fs::read_dir(dir)
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().to_string_lossy().ends_with(".spans.jsonl"))
        .flat_map(|e| std::fs::read_to_string(e.path()).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()).collect::<Vec<_>>())
        .collect()
}

/// `node`'s status as the admin's `mesh.nodes.get` of mesh1 lists it, once it is `want`.
async fn until_status(nodes: &Nodes, node: &PathName, want: NodeStatus) {
    tokio::time::timeout(WRONG, async {
        loop {
            let listed = rdm_test::typed::mesh_nodes(nodes, &node.mesh).await.unwrap_or_else(|e| panic!("node.get [mesh: {}]: {e}", node.mesh));
            if listed.iter().any(|n| &n.name == node && n.status == want) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{node} never listed {want:?}"));
}

/// The fabric's own status, as the admin's `fabric.get` answers it, once it is ready: the
/// initial fabric's first ready, until which the Day-0 Build is in progress (Q170). A node's
/// own ready and a Build's `Complete` are not it: a
/// `node.create` changes the accepted topology, which takes the fabric's checklists back to pending
/// before the fabric first reaches it.
async fn until_fabric_ready(fabrics: &Fabrics) {
    tokio::time::timeout(WRONG, async {
        loop {
            if rdm_test::typed::the_fabric(fabrics).await.unwrap_or_else(|e| panic!("fabric.get: {e}")).status == ScopeStatus::Ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("the fabric never first reached ready"));
}

/// CONTRACT (i143 1b, Arch: "the single-admin memory estate (birth, node.create, node.stop,
/// node.restart, build.get, from the scenario crate, file feature off) is the runtime proof"; node.restart
/// is removed, Q183: its node.stop then node.start is the stop and start below): a
/// Day-0 node-admin started on the memory storage backend comes up ready in both roles, its storage
/// step spanned `writes_to = "memory"`; `node.create` of two rpc nodes in mesh1 completes and lists
/// them ready; once the fabric has first reached ready, `node.stop` of the
/// first stops it (its cohort role moving to the second) and `node.start` brings it back; before
/// that, a Reconcile changing mesh1.rpc.1's config alone completes and the node carries the config
/// (build.md §1, "The Build applies the diff": `config` in place, as `node.config.update` applies a
/// map; the executor reads the entry in the accepted Build the memory log holds); `build.get [id]` of the
/// first create's Build answers it Complete with its step receipts. Through all of it the admin
/// writes nothing of its own to disk:
/// its data dir holds no store (no `nodes/`, no `attempt-context/`), and every file under the fabric's
/// store dir is the rpc node's own `connections/` rows. Must NOT happen: a row of the admin's
/// fabric, mesh, node, status or Build state on the file store.
#[test]
fn a_node_admin_on_the_memory_backend_runs_the_node_lifecycle_and_writes_nothing_to_the_file_store() {
    let cell = "a_node_admin_on_the_memory_backend_runs_the_node_lifecycle_and_writes_nothing_to_the_file_store";
    let artifacts = std::env::temp_dir().join("i143-testkit").join(cell);
    let exporter = rdm_transport::jsonl_span_exporter(&artifacts, "testkit").unwrap_or_else(|e| panic!("the span artifact dir {} is unusable: {e}", artifacts.display()));
    let (capture, layer) = rdm_transport::span_capture(cell, exporter);
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d))).build().unwrap();
    let _g = tracing::dispatcher::set_default(&dispatch);
    let dir = std::env::temp_dir().join(format!("memory-estate-{}", rdm_core::NodeId::mint()));
    let cfg = day0_config(&dir);
    let (data_dir, store_dir) = (cfg.data_dir.clone(), cfg.store_dir.clone());
    let launched = Launched(store_dir.clone());
    let started = std::time::Instant::now();
    rt.block_on(async {
        let running = start_on(cfg, Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, rdm_admin::storage_backend::TopologyStorageProviders::new().register(MEMORY, Arc::new(MemoryBackend))).await.expect("the Day-0 admin comes up on the memory backend");
        let me = running.digest.lock().unwrap().clone();
        tokio::time::timeout(WRONG, async {
            while !running.control.topology.read().await.births().any(|n| n.node_id == me.node.node_id && n.status == rdm_admin::model::NodeStatus::Ready && n.is_primary && n.is_fabric_primary) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the admin's view never held it ready in both roles");
        let admin = ResolvedNode { node_id: me.node.node_id.clone(), name: me.node.name.clone(), endpoint_id: me.node.endpoint_id.0.parse().unwrap(), incarnation: me.node.incarnation.clone(), transport_addr: me.node.transport_addr };
        let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller's endpoint binds");
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(admin);
        let client = Arc::new(NodeRpcClient::new(endpoint.clone(), resolver).with_caller_system("rdm"));
        let carrier = BuildCarrier::new(client, me.node.name.clone());
        let (nodes, builds, fabrics) = (Nodes::new(carrier.clone()), Builds::new(carrier.clone()), Fabrics::new(carrier.clone()));
        let node: PathName = NODE.parse().unwrap();
        // The admin started its Day-0 Build by starting: it is followed to its end before anything is
        // submitted, as an estate's is.
        let day_zero = rdm_test::typed::the_fabric(&fabrics).await.unwrap_or_else(|e| panic!("fabric.get: {e}")).build_id.expect("the Day-0 admin holds its Day-0 Build as the current one");
        let day_zero = BuildId::parse(&day_zero).unwrap_or_else(|e| panic!("fabric.get names {day_zero}, not a Build id: {e}"));
        Estate::until_complete(&mut carrier.resubmit(&day_zero, 1).await.unwrap_or_else(|e| panic!("the admin does not stream its Day-0 Build {day_zero}: {e}"))).await;

        // Each create names its path and the executable it runs (proposal executable-bindings-at-create.md
        // rule 1): the complete topology with one more rpc node, read from the admin now.
        let executables = Executables::testkit("process");
        let one_more = || async {
            let current = Current::read(&carrier).await.unwrap_or_else(|e| panic!("the current topology: {e}"));
            let nodes = current.with_one_more("mesh1", LaunchKind::RpcNode, &executables);
            (current.based_on, nodes)
        };
        let (based_on, request) = one_more().await;
        let created = Estate::workflow("node.create of an rpc node in mesh1", nodes.create_named(based_on, request).await).await;
        until_status(&nodes, &node, NodeStatus::Ready).await;
        // A stop moves the stopped node's cohort role to a secondary of its type: a second rpc node.
        let (based_on, request) = one_more().await;
        Estate::workflow("node.create of a second rpc node in mesh1", nodes.create_named(based_on, request).await).await;
        until_status(&nodes, &"mesh1.rpc.2".parse().unwrap(), NodeStatus::Ready).await;
        until_fabric_ready(&fabrics).await;
        // A Reconcile that changes mesh1.rpc.1's config alone: the Build's `node.config.update` reads the
        // entry in the accepted Build the admin's own log holds, and the node carries it.
        let zone: std::collections::BTreeMap<String, String> = [("zone".to_string(), "a".to_string())].into();
        let current = Current::read(&carrier).await.unwrap_or_else(|e| panic!("the current topology: {e}"));
        let request = current.held(&executables).unwrap_or_else(|e| panic!("{e}")).into_values().flat_map(|m| m.into_values()).map(|mut s| {
            if s.path == node {
                s.config = zone.clone();
            }
            s
        });
        let mut stream = carrier.create(rdm_core::build::BuildChange::Reconcile { topology_storage_provider: None, based_on: current.based_on, nodes: request.collect() }).await.unwrap_or_else(|e| panic!("build.create of the config-only Reconcile: {e}"));
        Estate::until_complete(&mut stream).await;
        tokio::time::timeout(WRONG, async {
            while !rdm_test::typed::mesh_nodes(&nodes, &node.mesh).await.unwrap_or_else(|e| panic!("node.get [mesh]: {e}")).iter().any(|n| n.name == node && n.config == zone) {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{node} never carried the config its entry names"));
        Estate::workflow("node.stop of mesh1.rpc.1", nodes.stop(&node).await).await;
        Estate::workflow("node.start of mesh1.rpc.1", nodes.start(&node).await).await;
        until_status(&nodes, &node, NodeStatus::Ready).await;
        let receipts = carrier.build(&created.build_id).await.unwrap_or_else(|e| panic!("build.get [id] of the create's Build {}: {e}", created.build_id));
        assert_eq!(receipts.phase, BuildPhase::Complete, "the create's Build is Complete: {receipts:#?}");
        assert!(!receipts.steps.is_empty(), "build.get answers the create's step receipts: {receipts:#?}");
        running.leave().await;
    });
    eprintln!("memory estate: boot to leave in {} ms", started.elapsed().as_millis());
    drop(launched);
    let _ = capture.flush(Duration::from_secs(10));

    // The admin's own durable state never reached a file: its data dir holds no store, and the store
    // dir holds the rpc node's own connection facts only (its lines of the root's connections.jsonl,
    // the root its store opened, with the root's layout marker).
    for store in ["nodes", "attempt-context"] {
        assert!(!data_dir.join(store).exists(), "the memory admin wrote {store}/ in its data dir {}", data_dir.display());
    }
    let stray: Vec<PathBuf> = files_under(&store_dir).into_iter().filter(|f| !f.ends_with(rdm_admin::store_root::CONNECTIONS_FILE) && !f.ends_with(rdm_admin::store_root::LAYOUT)).collect();
    assert!(stray.is_empty(), "the memory admin wrote under the fabric's store dir {}: {stray:?}", store_dir.display());

    let spans = spans_in(&artifacts);
    // The artifact dir keeps earlier runs' spans: this run's storage step names its own data dir.
    let opened: Vec<&Value> = named(&spans, "rdm.mesh.node.resolve.via-storage-opened").into_iter().filter(|sp| sp["attributes"]["data_dir"].as_str() == Some(&data_dir.display().to_string())).collect();
    assert_eq!(opened.len(), 1, "one storage step of this admin's boot: {opened:?}");
    assert_eq!(opened[0]["attributes"]["writes_to"].as_str(), Some("memory"), "the admin's boundaries are the memory backend's: {}", opened[0]);
    eprintln!("span rdm.mesh.node.resolve.via-storage-opened trace_id={} writes_to=memory; spans: {}", opened[0]["trace_id"], artifacts.display());
}
