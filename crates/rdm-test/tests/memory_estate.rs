//! product=mesh, feature=storage-backend, subfeature=memory-estate, rung=M1, provider=process.
//!
//! The runtime proof of the storage seam (i143 1b): a node-admin booted in this process on the
//! memory storage backend (`rdm_admin::storage_backend::MemoryBackend`) runs the node lifecycle with
//! every durable read and write of its own going through the six boundaries it opened. Built with the
//! file backend off (`--no-default-features`), the file store is absent from this executable; built
//! with it on, the admin still opens nothing but the memory boundaries. The rpc node the admin launches
//! is the gate's `rafka-node`, whose own connections rows are its node backend's (the file one).

use rdm_admin::admin::{start_on, AdminConfig};
use rdm_admin::storage_backend::MemoryBackend;
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_client::{BuildCarrier, Builds, LaunchKind, NodeSelector, NodeSpec, NodeStatus, Nodes, PathName};
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

/// A Day-0 node-admin whose data dir is `<dir>/admin` and whose fabric's store dir is `<dir>/store`,
/// launching the built node executables of `bin_dir` on the process provider.
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

/// `node`'s status as the admin's `node.get` of mesh1 lists it, once it is `want`.
async fn until_status(nodes: &Nodes, node: &PathName, want: NodeStatus) {
    tokio::time::timeout(WRONG, async {
        loop {
            let listed = nodes.get(&NodeSelector::Mesh(node.mesh.clone())).await.unwrap_or_else(|e| panic!("node.get of {}: {e}", node.mesh));
            if listed.iter().any(|n| &n.name == node && n.status == want) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{node} never listed {want:?}"));
}

/// CONTRACT (i143 1b, Arch: "the single-admin memory estate (birth, node.create, node.stop,
/// node.restart, build.get, from the scenario crate, file feature off) is the runtime proof"): a
/// Day-0 node-admin started on the memory storage backend comes up ready in both seats, its storage
/// step spanned `writes_to = "memory"`; `node.create` of an rpc node in mesh1 completes and lists it
/// ready-for-traffic; `node.stop` parks it and `node.start` brings it back; `node.restart` completes
/// and it is ready again; `build.get` of the create's Build answers it Complete with its step
/// receipts; `node.delete` ends it. Through all of it the admin writes nothing of its own to disk:
/// its data dir holds no store (no `nodes/`, no `attempt-context/`), and every file under the fabric's
/// store dir is the rpc node's own `connections/` rows. Must NOT happen: a row of the admin's
/// fabric, mesh, node, status or Build state on the file store.
#[test]
fn a_node_admin_on_the_memory_backend_runs_the_node_lifecycle_and_writes_nothing_to_the_file_store() {
    let cell = "a_node_admin_on_the_memory_backend_runs_the_node_lifecycle_and_writes_nothing_to_the_file_store";
    let artifacts = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/i143-testkit").join(cell);
    let exporter = rdm_transport::jsonl_span_exporter(&artifacts, "testkit").unwrap_or_else(|e| panic!("the span artifact dir {} is unusable: {e}", artifacts.display()));
    let (capture, layer) = rdm_transport::span_capture(cell, exporter);
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d))).build().unwrap();
    let _g = tracing::dispatcher::set_default(&dispatch);
    let dir = std::env::temp_dir().join(format!("memory-estate-{}", rdm_core::NodeId::mint()));
    let cfg = day0_config(&dir);
    let (data_dir, store_dir) = (cfg.data_dir.clone(), cfg.store_dir.clone());
    rt.block_on(async {
        let running = start_on(cfg, Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }, Arc::new(MemoryBackend)).await.expect("the Day-0 admin comes up on the memory backend");
        let me = running.digest.lock().unwrap().clone();
        tokio::time::timeout(WRONG, async {
            while !running.control.topology.read().await.births().any(|n| n.node_id == me.node.node_id && n.status == rdm_admin::model::NodeStatus::ReadyForTraffic && n.is_primary && n.is_fabric_primary) {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the admin's view never held it ready in both seats");
        let admin = ResolvedNode { node_id: me.node.node_id.clone(), name: me.node.name.clone(), endpoint_id: me.node.endpoint_id.0.parse().unwrap(), incarnation: me.node.incarnation.clone(), transport_addr: me.node.transport_addr };
        let endpoint = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.expect("the caller's endpoint binds");
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(admin);
        let client = Arc::new(NodeRpcClient::new(endpoint.clone(), resolver).with_caller_system("rdm"));
        let carrier = BuildCarrier::new(client, me.node.name.clone());
        let (nodes, builds) = (Nodes::new(carrier.clone()), Builds::new(carrier));
        let node: PathName = NODE.parse().unwrap();

        let created = Estate::workflow("node.create of an rpc node in mesh1", nodes.create(&NodeSpec { mesh: "mesh1".into(), kind: LaunchKind::RpcNode }).await).await;
        until_status(&nodes, &node, NodeStatus::ReadyForTraffic).await;
        Estate::workflow("node.stop of mesh1.rpc.1", nodes.stop(&node).await).await;
        Estate::workflow("node.start of mesh1.rpc.1", nodes.start(&node).await).await;
        until_status(&nodes, &node, NodeStatus::ReadyForTraffic).await;
        Estate::workflow("node.restart of mesh1.rpc.1", nodes.restart(&node).await).await;
        until_status(&nodes, &node, NodeStatus::ReadyForTraffic).await;
        let receipts = builds.get(&created.build_id).await.unwrap_or_else(|e| panic!("build.get of the create's Build {}: {e}", created.build_id));
        assert_eq!(receipts.phase, BuildPhase::Complete, "the create's Build is Complete: {receipts:#?}");
        assert!(!receipts.steps.is_empty(), "build.get answers the create's step receipts: {receipts:#?}");
        Estate::workflow("node.delete of mesh1.rpc.1", nodes.delete(&node).await).await;
        running.leave().await;
    });
    let _ = capture.flush(Duration::from_secs(10));

    // The admin's own durable state never reached a file: its data dir holds no store, and the store
    // dir holds the rpc node's own connections rows only.
    for store in ["nodes", "attempt-context"] {
        assert!(!data_dir.join(store).exists(), "the memory admin wrote {store}/ in its data dir {}", data_dir.display());
    }
    let stray: Vec<PathBuf> = files_under(&store_dir).into_iter().filter(|f| !f.components().any(|c| c.as_os_str() == "connections")).collect();
    assert!(stray.is_empty(), "the memory admin wrote under the fabric's store dir {}: {stray:?}", store_dir.display());

    let spans = spans_in(&artifacts);
    let opened = named(&spans, "rdm.mesh.node.resolve.via-storage-opened");
    assert_eq!(opened.len(), 1, "one storage step of the admin's boot: {opened:?}");
    assert_eq!(opened[0]["attributes"]["writes_to"].as_str(), Some("memory"), "the admin's boundaries are the memory backend's: {}", opened[0]);
    eprintln!("span rdm.mesh.node.resolve.via-storage-opened trace_id={} writes_to=memory; spans: {}", opened[0]["trace_id"], artifacts.display());
}
