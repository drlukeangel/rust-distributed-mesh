//! Functional test for `fabric.create` (`docs/architecture/lifecycles/fabric-create.md`).

use rdm_admin::admin::{start_on, AdminConfig};
use rdm_admin::model::{NodeStatus, ScopeStatus};
use rdm_admin::storage_backend::{MemoryBackend, TopologyStorageProviders};
use rdm_admin::wiring::{FabricHooks, FabricReadyCtx, Wiring};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt as _;

const MEMORY: &str = "memory";

struct Captured {
    attributes: BTreeMap<String, String>,
}

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

impl Capture {
    fn new(cell: &str) -> Self {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (capture, layer) = rdm_transport::span_capture(cell, exporter.clone());
        Self {
            exporter,
            capture,
            dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
        }
    }

    fn named(&self, name: &str) -> Vec<Captured> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| Captured {
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }
}

/// CONTRACT: docs/architecture/lifecycles/fabric-create.md:5 "`fabric.create(spec)` brings a fabric into being with its first mesh. The fabric id is minted with status pending, each mesh's first node-admin is created, the first mesh's primary becomes the fabric-primary, and the fabric changes to ready."
/// docs/architecture/lifecycles/fabric-create.md:17 "The first node-admin is the only eligible candidate and is both mesh-primary and fabric-primary"
/// docs/architecture/lifecycles/fabric-create.md:18 "The fabric-primary creates the fabric row and its own mesh's row at Day 0, written to the store root at once"
/// docs/architecture/lifecycles/fabric-create.md:35 "When every mesh the accepted Build names is ready, the fabric-primary sets the fabric ready (Q174)."
/// docs/architecture/lifecycles/fabric-create.md:37 "The application is told once (`on_fabric_ready`) and opens client traffic."
#[test]
fn day0_bootstrap_mints_identities_writes_fabric_and_mesh_rows_enters_ready_and_notifies_app_once() {
    let capture = Capture::new(
        "day0_bootstrap_mints_identities_writes_fabric_and_mesh_rows_enters_ready_and_notifies_app_once",
    );
    let dispatch = capture.dispatch.clone();
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
        .build()
        .unwrap();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let dir = std::env::temp_dir().join(format!("rdm-fabric-create-{}", rdm_core::NodeId::mint()));
    let data_dir = dir.join("admin");
    let store_dir = dir.join("store");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&store_dir).unwrap();

    let cfg = AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" => Some(data_dir.display().to_string()),
        "RDM_STORE_DIR" => Some(store_dir.display().to_string()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        rdm_core::launch::ENV_TOPOLOGY_STORAGE_PROVIDER => Some(MEMORY.into()),
        _ => None,
    })
    .expect("valid Day-0 AdminConfig");

    let expected_fabric_id = cfg.fabric_id.clone();
    let expected_node_id = cfg.node_id.clone();

    rt.block_on(async move {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<FabricReadyCtx>();
        let hooks = FabricHooks {
            on_fabric_ready: Some(Arc::new(move |ctx| {
                let _ = tx.send(ctx);
                Box::pin(async {})
            })),
            ..FabricHooks::no_app_work()
        };
        let providers = TopologyStorageProviders::new().register(MEMORY, Arc::new(MemoryBackend));
        let running = start_on(
            cfg,
            Wiring {
                fabric_hooks: hooks,
                ..Wiring::no_certs()
            },
            providers,
        )
        .await
        .expect("Day-0 node-admin starts on memory backend");

        let ready_ctx = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("on_fabric_ready called within 30s")
            .expect("on_fabric_ready channel open");

        assert_eq!(ready_ctx.fabric_id, expected_fabric_id);
        assert_eq!(ready_ctx.attempt, 1);
        assert!(!ready_ctx.build_id.is_empty());

        let view = running.control.topology.read().await.clone();
        assert_eq!(view.fabric.id, expected_fabric_id);
        assert_eq!(view.fabric.name, "fabric1");
        assert_eq!(view.fabric.status, ScopeStatus::Ready);
        assert_eq!(view.meshes.len(), 1);
        assert_eq!(view.meshes[0].name, "mesh1");
        assert_eq!(view.meshes[0].status, ScopeStatus::Ready);
        assert!(view.meshes[0].id.is_some(), "mesh1 has a minted MeshId");

        let admin_node = view
            .node(&"mesh1.admin.1".parse().unwrap())
            .expect("mesh1.admin.1 is in the topology view");
        assert_eq!(admin_node.node_id, expected_node_id);
        assert_eq!(admin_node.status, NodeStatus::Ready);
        assert!(admin_node.is_primary, "first node-admin is mesh-primary");
        assert!(admin_node.is_fabric_primary, "first node-admin is fabric-primary");

        running.leave().await;
    });

    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        !capture.named("rdm.mesh.node.create.via-identity-minted").is_empty(),
        "Day 0 must mint node identity"
    );
    let node_claims: Vec<_> = capture
        .named("rdm.node_admin.node.create.via-node-row")
        .into_iter()
        .filter(|s| {
            s.attributes.get("subject").map(String::as_str) == Some("mesh1.admin.1")
                && s.attributes.get("moment").map(String::as_str) == Some("claim")
        })
        .collect();
    assert_eq!(node_claims.len(), 1, "Day 0 claims mesh1.admin.1 once");
    assert_eq!(
        node_claims[0].attributes.get("outcome").map(String::as_str),
        Some("row-written")
    );

    let fabric_rows: Vec<_> = capture
        .named("rdm.node_admin.fabric.create.via-fabric-row")
        .into_iter()
        .filter(|s| s.attributes.get("outcome").map(String::as_str) == Some("row-written"))
        .collect();
    assert_eq!(fabric_rows.len(), 1, "Day 0 writes the Fabric row once");

    let mesh_rows: Vec<_> = capture
        .named("rdm.node_admin.mesh.create.via-mesh-row")
        .into_iter()
        .filter(|s| s.attributes.get("outcome").map(String::as_str) == Some("row-written"))
        .collect();
    assert_eq!(mesh_rows.len(), 1, "Day 0 writes mesh1's row once");
    assert_eq!(
        mesh_rows[0].attributes.get("mesh").map(String::as_str),
        Some("mesh1")
    );

    assert!(
        !capture.named("rdm.node_admin.build.create.via-accepted").is_empty(),
        "Day 0 accepts the root Build"
    );
    assert!(
        !capture.named("rdm.node_admin.fabric.update.via-every-mesh-ready").is_empty(),
        "every mesh ready span emitted"
    );
    let entered: Vec<_> = capture
        .named("rdm.node_admin.fabric.update.via-state-entered")
        .into_iter()
        .filter(|s| {
            s.attributes.get("from").map(String::as_str) == Some("pending")
                && s.attributes.get("to").map(String::as_str) == Some("ready")
        })
        .collect();
    assert_eq!(entered.len(), 1, "fabric transitions pending -> ready once");

    let notified = capture.named("rdm.node_admin.fabric.update.via-on-fabric-ready");
    assert_eq!(notified.len(), 1, "on_fabric_ready span emitted once");
    assert_eq!(
        notified[0].attributes.get("bound").map(String::as_str),
        Some("true")
    );
}
