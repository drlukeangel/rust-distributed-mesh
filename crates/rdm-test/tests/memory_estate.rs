//! product=mesh, feature=storage-backend, subfeature=memory-estate, rung=M1, provider=process.
//!
//! The runtime proof of the storage seam (i143 1b): a node-admin booted in this process on the
//! memory storage backend (`rdm_admin::storage_backend::MemoryBackend`) runs its Day-0 Build with
//! every durable read and write of its own going through the six boundaries it opened. Built with the
//! file backend off (`--no-default-features`), the file store is absent from this executable; built
//! with it on, the admin still opens nothing but the memory boundaries. The memory backend serves a
//! single-process fabric only (storage.md §2): the admin is the fabric's one process.

use rdm_admin::admin::{start_on, AdminConfig};
use rdm_admin::storage_backend::MemoryBackend;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_client::{BuildCarrier, BuildId, Fabrics, ScopeStatus};
use rdm_core::build::BuildPhase;
use rdm_test::estate::{bin_dir, named, Estate};
use rdm_transport::{NodeRpcClient, ResolvedNode, StaticResolver};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

const WRONG: Duration = Duration::from_secs(120);

/// The runtimes the cell's admin launched (every process whose `RDM_STORE_DIR` is the cell's store
/// dir) end when the cell does, a cell that panics included: the admin runs in this process, and
/// the nodes it launched are processes of their own.
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

/// CONTRACT (i143 1b; storage.md §2: "A backend that keeps its stores in one process, the testkit's
/// `memory` backend, serves a single-process fabric only"; Q356: a node's own record is written by the
/// node, in its own process): a Day-0 node-admin started on the memory storage backend, the fabric's one
/// process, comes up ready in both roles, its storage step spanned `writes_to = "memory"`; its Day-0
/// Build runs to its end, the fabric first reaches ready, and `build.get [id]` of that Build answers it
/// Complete with its events, `rdm.build.started` to `rdm.build.ended`. Through all of it the admin writes nothing of its own to disk: its
/// data dir holds no store (no `nodes/`), and nothing is written under the fabric's store dir but the
/// root's layout marker and connections file. Must NOT happen: a row of the admin's fabric, mesh, node,
/// status or Build state on the file store.
#[test]
fn a_single_process_fabric_on_the_memory_backend_runs_its_day0_build_and_writes_nothing_to_the_file_store() {
    let cell = "a_single_process_fabric_on_the_memory_backend_runs_its_day0_build_and_writes_nothing_to_the_file_store";
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
        let admin = ResolvedNode { mesh: me.node.mesh.clone(), r#type: me.node.r#type, node_id: me.node.node_id.clone(), path: me.node.path.clone(), name: me.node.name.clone(), endpoint_id: me.node.endpoint_id.0.parse().unwrap(), incarnation: me.node.incarnation.clone(), transport_addr: me.node.transport_addr };
        let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller's endpoint binds");
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(admin);
        let client = Arc::new(NodeRpcClient::new(endpoint.clone(), resolver).with_caller_system("rdm"));
        let carrier = BuildCarrier::at_node(client, me.node.node_id.clone());
        let fabrics = Fabrics::new(carrier.clone());
        // The admin started its Day-0 Build by starting: it is followed to its end, as an estate's is.
        let day_zero = rdm_test::typed::the_fabric(&fabrics).await.unwrap_or_else(|e| panic!("fabric.get: {e}")).build_id.expect("the Day-0 admin holds its Day-0 Build as the current one");
        let day_zero = BuildId::parse(&day_zero).unwrap_or_else(|e| panic!("fabric.get names {day_zero}, not a Build id: {e}"));
        Estate::until_complete(&mut carrier.resubmit(&day_zero).await.unwrap_or_else(|e| panic!("the admin does not stream its Day-0 Build {day_zero}: {e}"))).await;
        until_fabric_ready(&fabrics).await;
        let build = carrier.build(&day_zero).await.unwrap_or_else(|e| panic!("build.get [id] of the Day-0 Build {day_zero}: {e}"));
        assert_eq!(build.phase, BuildPhase::Complete, "the Day-0 Build is Complete: {build:#?}");
        let types: Vec<&str> = build.events.iter().filter_map(|e| e["type"].as_str()).collect();
        assert_eq!(types.first().copied(), Some("rdm.build.started"), "build.get answers the Day-0 Build's events from its start: {build:#?}");
        assert_eq!(types.last().copied(), Some("rdm.build.ended"), "build.get answers the Day-0 Build's events to its end: {build:#?}");
        running.leave().await;
    });
    eprintln!("memory estate: boot to leave in {} ms", started.elapsed().as_millis());
    drop(launched);
    let _ = capture.flush(Duration::from_secs(10));

    // The admin's own durable state never reached a file: its data dir holds no store, and the store
    // dir holds the node's own connection facts only (its lines of the root's connections.jsonl,
    // the root its store opened, with the root's layout marker).
    for store in ["nodes"] {
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
