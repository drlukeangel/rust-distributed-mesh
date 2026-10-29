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
    trace_id: String,
    span_id: String,
    parent_span_id: String,
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
                span_id: s.span_context.span_id().to_string(),
                parent_span_id: s.parent_span_id.to_string(),
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }
}

/// CONTRACT:
/// - `docs/architecture/lifecycles/fabric-create.md:5-8` — "`fabric.create(spec)` brings a fabric into being with its first mesh. The fabric id is minted with status pending, each mesh's first node-admin is created, the first mesh's primary becomes the fabric-primary, and the fabric changes to ready."
/// - `docs/architecture/lifecycles/fabric-create.md:16` — "Identity order | The fabric id is minted first, each mesh with that fabric id, each node with its mesh id ([identity creation order](../fabric-primary.md#11-identity-creation-order)); the bootstrap admin's own node id is minted before its mesh's, by the bootstrap's first step, before anything else of its boot (Q208)"
/// - `docs/architecture/lifecycles/fabric-create.md:17` — "Primaries | The first node-admin is the only eligible candidate and is both mesh-primary and fabric-primary"
/// - `docs/architecture/lifecycles/fabric-create.md:18` — "Rows | The fabric-primary creates the fabric row and its own mesh's row at Day 0, written to the store root at once"
/// - `docs/architecture/lifecycles/fabric-create.md:19` — "Status | pending, then ready ([States](states.md#setting-a-state))"
/// - `docs/architecture/lifecycles/fabric-create.md:20` — "Completion | `fabric.created` when the fabric is ready"
/// - `docs/architecture/lifecycles/fabric-create.md:21` — "Wire | [Typed-object mapping](../node-rpc-envelope.md#typed-object-contract); the accepted topology is carried by the [Build carrier](../node-rpc-envelope.md#build-op-0x20-rdm)"
/// - `docs/architecture/lifecycles/fabric-create.md:25-27` — "1. Mint the fabric id; its status is pending. `fabric.root.create`: the bootstrap admin takes the fabric root from `CertChoice::Ca` or, with no seeds and no anchor, mints one; `fabric.ca.create`: it mints RDM's CA under that root ([fabric certs](../fabric-certs.md#the-cert-workflows))."
/// - `docs/architecture/lifecycles/fabric-create.md:28-30` — "2. [node.create](node-create.md) of the first node-admin, whose own mesh comes into being with it ([mesh absent](node-create.md#mesh-absent)). Every further mesh comes into being with its first node-admin, created by a Build after the fabric's first ready (Q170)."
/// - `docs/architecture/lifecycles/fabric-create.md:31-34` — "3. The first mesh's first node-admin is the first fabric-primary. It is both mesh-primary and fabric-primary, accepts the first Build ([ready root](../build.md#101-ready-root--deadlock-guard)), and creates the fabric row and its own mesh's row. Every other mesh's row is created by that mesh's primary."
/// - `docs/architecture/lifecycles/fabric-create.md:35-36` — "4. When every mesh the accepted Build names is ready, the fabric-primary sets the fabric [ready](states.md#the-fabrics-ready) (Q174)."
/// - `docs/architecture/lifecycles/fabric-create.md:37` — "5. The application is told once (`on_fabric_ready`) and opens client traffic."
/// - `docs/architecture/lifecycles/fabric-create.md:38` — "6. Emit `fabric.created`. The call ends here."
/// - `docs/architecture/lifecycles/fabric-create.md:71-72` — "Failures and stream certainty follow the [caller execution contract](../node-rpc-envelope.md#caller-execution-and-certainty)."
/// - `docs/architecture/lifecycles/fabric-create.md:76-77` — "Trace propagation, step spans and correlation follow the [runtime spans](../node-rpc-envelope.md#runtime-spans)."
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

    // Also verify explicit CertChoice::Ca root is accepted before exercising Day-0 unseeded root minting.
    let explicit_dir = dir.join("explicit-ca");
    std::fs::create_dir_all(&explicit_dir).unwrap();
    let explicit_secret = rdm_core::certs::mint_key();
    let explicit_pub = explicit_secret.verifying_key();
    let explicit_issuer = rdm_admin::certs::CertChoice::Ca {
        secret: explicit_secret,
        issuing: None,
    }
    .issuer(&explicit_dir, true, rdm_transport::clock::RafkaTime::unadopted())
    .expect("CertChoice::Ca mints RDM's CA under the given root on Day 0");
    assert_eq!(explicit_issuer.root(), Some(explicit_pub));

    // Step 1 / Identity order (Q208): AdminConfig::from_env mints the fabric id and the bootstrap
    // admin's own node id before its mesh id is minted during start_on.
    let cfg = AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" => Some(data_dir.display().to_string()),
        "RDM_STORE_DIR" => Some(store_dir.display().to_string()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        rdm_core::launch::ENV_TOPOLOGY_STORAGE_PROVIDER => Some(MEMORY.into()),
        _ => None,
    })
    .expect("valid Day-0 AdminConfig");
    assert!(cfg.mesh_id.is_none(), "bootstrap admin's own node_id and fabric_id are minted before mesh_id");

    let expected_fabric_id = cfg.fabric_id.clone();
    let expected_node_id = cfg.node_id.clone();
    let day0_certs = rdm_admin::certs::CertChoice::from_material(&data_dir, &cfg.mesh, true, |_| None)
        .expect("Day-0 bootstrap with no seeds and no anchor mints the fabric root");

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
                certs: day0_certs,
                fabric_hooks: hooks,
                ..Wiring::default()
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
        let mesh_id = view.meshes[0].id.clone().expect("mesh1 has a minted MeshId");

        let admin_node = view
            .node(&"mesh1.admin.1".parse().unwrap())
            .expect("mesh1.admin.1 is in the topology view");
        assert_eq!(admin_node.node_id, expected_node_id);
        assert_eq!(admin_node.status, NodeStatus::Ready);
        assert!(admin_node.is_primary, "first node-admin is mesh-primary");
        assert!(admin_node.is_fabric_primary, "first node-admin is fabric-primary");

        // Wire / Build carrier & Completion: the Day-0 Build carries the accepted root topology
        // and reaches Complete once the fabric is ready.
        let build_id: rdm_admin::build::BuildId = ready_ctx.build_id.parse().unwrap();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let day0_proj = loop {
            let current = running
                .control
                .builds
                .read_build(&build_id)
                .await
                .expect("reads Day-0 Build");
            if current.state == rdm_admin::build_state::BuildState::Complete
                || tokio::time::Instant::now() >= deadline
            {
                break current;
            }
            tokio::task::yield_now().await;
        };
        assert_eq!(day0_proj.build_id, build_id);
        assert_eq!(day0_proj.state, rdm_admin::build_state::BuildState::Complete);
        assert!(day0_proj.submitted_change.is_none());
        assert_eq!(day0_proj.topology.meshes.get("mesh1").map(|m| &m.mesh_id), Some(&mesh_id));

        running.leave().await;
    });
    // The on_fabric_ready span closes after the hook returns, in the poll that sent the ready context
    // (`fabric_status.rs`, `notify_fabric_ready`): the runtime's shutdown waits for that poll to end, so
    // every span below is finished before it is read.
    drop(rt);

    let _ = std::fs::remove_dir_all(&dir);

    assert!(
        !capture.named("rdm.node_admin.cert.create.via-day-zero-root").is_empty(),
        "Day 0 without seeds or anchor must mint the fabric root (fabric.root.create)"
    );
    assert!(
        !capture.named("rdm.node_admin.cert.create.via-ca-create").is_empty(),
        "Day 0 must mint RDM's CA under the fabric root (fabric.ca.create)"
    );
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
    assert_eq!(
        node_claims[0].attributes.get("operation").map(String::as_str),
        Some("fabric-create")
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

    let accepted_spans = capture.named("rdm.node_admin.build.create.via-accepted");
    assert!(
        !accepted_spans.is_empty(),
        "Day 0 accepts the root Build"
    );
    let change_spans = capture.named("rdm.node_admin.fabric.update.via-change");
    assert_eq!(change_spans.len(), 1, "fabric change root span emitted once");
    let every_mesh = capture.named("rdm.node_admin.fabric.update.via-every-mesh-ready");
    assert_eq!(every_mesh.len(), 1, "every mesh ready span emitted once");
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

    // Observability contract: step spans correlate under the fabric change trace and parent to it.
    assert_eq!(every_mesh[0].trace_id, change_spans[0].trace_id);
    assert_eq!(every_mesh[0].parent_span_id, change_spans[0].span_id);
    assert_eq!(entered[0].trace_id, change_spans[0].trace_id);
    assert_eq!(entered[0].parent_span_id, change_spans[0].span_id);
    assert_eq!(notified[0].trace_id, change_spans[0].trace_id);
    assert_eq!(notified[0].parent_span_id, change_spans[0].span_id);
}
