//! Functional test for `fabric-states` (`docs/architecture/lifecycles/states.md`).

use rdm_admin::accepted::TopologyChange;
use rdm_admin::admin::{start_on, AdminConfig};
use rdm_admin::build_state::BuildState;
use rdm_admin::model::ScopeStatus;
use rdm_admin::storage_backend::{MemoryBackend, TopologyStorageProviders};
use rdm_admin::wiring::{FabricHooks, FabricReadyCtx, Wiring};
use rdm_core::meta::NodeSpec;
use rdm_core::NodeKind;
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt as _;

const MEMORY: &str = "memory";

struct Captured {
    trace_id: String,
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
                trace_id: s.span_context.trace_id().to_string(),
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }
}

/// CONTRACT: docs/architecture/lifecycles/states.md:52 "When every mesh the accepted Build names is ready and the Build's attempt is claimed, the fabric-primary sets the fabric ready, publishes FabricStatus ready under the frame catalog, and the application receives one `on_fabric_ready` notice"
/// docs/architecture/lifecycles/states.md:56 "The fabric-primary's change span (`rdm.node_admin.fabric.update.via-change`) holds, in order, `rdm.node_admin.fabric.update.via-every-mesh-ready`, `rdm.node_admin.fabric.update.via-state-entered` (to ready) and `rdm.node_admin.fabric.update.via-on-fabric-ready`."
/// docs/architecture/lifecycles/states.md:167 "A Build that changes only a node's `config`, `tags`, `storage` or `placement` is neither: it applies them as build.md §1 states, and the fabric stays ready."
/// docs/architecture/lifecycles/states.md:169 "A resize or an executable replacement puts the fabric back to pending; meshes already serving stay ready. Its Build is Complete only when the fabric is ready again from this Build's own change, as fabric.create's is: while the fabric changes, the Build stays in progress, never Complete."
#[test]
fn fabric_ready_trace_preserves_ready_on_property_change_and_resets_to_pending_on_resize() {
    let capture = Capture::new(
        "fabric_ready_trace_preserves_ready_on_property_change_and_resets_to_pending_on_resize",
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

    let dir = std::env::temp_dir().join(format!("rdm-fabric-states-{}", rdm_core::NodeId::mint()));
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

    let (prop_build_log, resize_build_log) = rt.block_on(async move {
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

        let _ready_ctx = tokio::time::timeout(Duration::from_secs(30), rx.recv())
            .await
            .expect("on_fabric_ready called within 30s")
            .expect("on_fabric_ready channel open");

        // Wait for the Day-0 Build to settle to Complete and for the ready state to be published.
        let root_build = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(b) = running.control.builds.current().await.unwrap() {
                    if b.state == BuildState::Complete {
                        return b;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("Day-0 Build settles to Complete");

        let admin_path: rdm_admin::model::PathName = "mesh1.admin.1".parse().unwrap();
        let mut admin_spec = root_build
            .topology
            .mesh("mesh1")
            .unwrap()
            .spec(&admin_path)
            .unwrap()
            .clone();
        admin_spec.tags.insert("tier-control".into());
        admin_spec.config.insert("custom_key".into(), "custom_val".into());

        // A Build changing only tags and config keeps the fabric ready.
        let prop_opened = running
            .control
            .submit(
                "reconcile-props",
                TopologyChange::Reconcile {
                    based_on: root_build.build_id.clone(),
                    nodes: vec![admin_spec.clone()],
                    topology_storage_provider: None,
                },
            )
            .await
            .expect("property-only Reconcile Build is accepted");

        let view_after_props = running.control.topology.read().await.clone();
        assert_eq!(
            view_after_props.fabric.status,
            ScopeStatus::Ready,
            "fabric stays ready on tags/config Build"
        );
        assert_eq!(
            view_after_props.meshes[0].status,
            ScopeStatus::Ready,
            "mesh1 stays ready on tags/config Build"
        );

        // Wait for the property-only Build to complete before submitting a resize Build.
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(b) = running.control.builds.current().await.unwrap() {
                    if b.build_id == prop_opened.build_id && b.state == BuildState::Complete {
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("property-only Build settles to Complete");

        // Quiesce background execution so the resize Build is observed at acceptance before any process spawns.
        running.gate().quiesce().await;

        let mut rpc_spec = NodeSpec::default_for("mesh1.rpc.1".parse().unwrap());
        rpc_spec.executable = Some(crate::test_executable(NodeKind::RpcNode));

        let resize_opened = running
            .control
            .submit(
                "reconcile-resize",
                TopologyChange::Reconcile {
                    based_on: prop_opened.build_id.clone(),
                    nodes: vec![admin_spec, rpc_spec],
                    topology_storage_provider: None,
                },
            )
            .await
            .expect("resize Reconcile Build is accepted");

        let view_after_resize = running.control.topology.read().await.clone();
        assert_eq!(
            view_after_resize.fabric.status,
            ScopeStatus::Pending,
            "resize Build puts the fabric back to pending at acceptance"
        );
        assert_eq!(
            view_after_resize.meshes[0].status,
            ScopeStatus::Ready,
            "meshes already serving stay ready when fabric resets to pending"
        );

        let resize_proj = running
            .control
            .builds
            .current()
            .await
            .unwrap()
            .expect("resize Build is current");
        assert_eq!(resize_proj.build_id, resize_opened.build_id);
        assert_ne!(
            resize_proj.state,
            BuildState::Complete,
            "while the fabric changes, the Build stays in progress, never Complete"
        );

        running.leave().await;

        (prop_opened.build_id.log().to_string(), resize_opened.build_id.log().to_string())
    });
    // The on_fabric_ready span closes after the hook returns, on a spawned task (`fabric_status.rs`,
    // `notify_fabric_ready`): the runtime's shutdown waits for it, so every span below is finished
    // before it is read.
    drop(rt);

    let _ = std::fs::remove_dir_all(&dir);

    let change_spans = capture.named("rdm.node_admin.fabric.update.via-change");
    assert!(!change_spans.is_empty(), "via-change span emitted");
    let change_trace = &change_spans[0].trace_id;

    let every_mesh = capture.named("rdm.node_admin.fabric.update.via-every-mesh-ready");
    assert_eq!(every_mesh.len(), 1);
    assert_eq!(&every_mesh[0].trace_id, change_trace);

    let entered = capture.named("rdm.node_admin.fabric.update.via-state-entered");
    assert!(
        entered.iter().any(|s| {
            &s.trace_id == change_trace
                && s.attributes.get("from").map(String::as_str) == Some("pending")
                && s.attributes.get("to").map(String::as_str) == Some("ready")
        }),
        "via-state-entered (pending -> ready) shares the via-change trace"
    );

    let on_ready = capture.named("rdm.node_admin.fabric.update.via-on-fabric-ready");
    assert_eq!(on_ready.len(), 1);
    assert_eq!(&on_ready[0].trace_id, change_trace);

    let recorded = capture.named("rdm.node_admin.fabric.update.via-executables-recorded");
    assert!(
        recorded.iter().any(|s| {
            s.attributes.get("build_id").map(String::as_str) == Some(prop_build_log.as_str())
                && s.attributes.get("state").map(String::as_str) == Some("ready")
        }),
        "property-only Build records via-executables-recorded with state=ready"
    );

    let resets = capture.named("rdm.node_admin.fabric.update.via-state-reset");
    assert_eq!(resets.len(), 1, "only the resize Build resets the fabric state");
    assert_eq!(
        resets[0].attributes.get("build_id").map(String::as_str),
        Some(resize_build_log.as_str())
    );
    assert_eq!(
        resets[0].attributes.get("from").map(String::as_str),
        Some("ready")
    );
    assert_eq!(
        resets[0].attributes.get("reason").map(String::as_str),
        Some("the accepted topology changed")
    );

    assert!(
        entered.iter().any(|s| {
            s.attributes.get("from").map(String::as_str) == Some("ready")
                && s.attributes.get("to").map(String::as_str) == Some("pending")
        }),
        "resize Build enters pending from ready"
    );
}
