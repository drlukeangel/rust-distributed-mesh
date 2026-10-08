//! i143.e4.s11 acceptance (rafka-v2 #2805, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2805-unit`, which exports `I143_ACCEPTANCE_DIR`; each
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span this process
//! emitted, captured in-process by the evidence exporter) there.
//!
//! The status authority is a real node-admin `StatusAuthority` served over Node RPC on its own
//! endpoint; every caller is a real `NodeRpcClient` speaking as a known birth.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointId, IncarnationId, MeshId, NodeId, NodeKind};
use rafka_node_admin_core::deployment::pipeline::{drain_outcome, DrainOutcome};
use rafka_node_admin_core::model::{Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rafka_node_admin_core::status_rpc::{Declared, StatusAuthority};
use rafka_node_admin_core::status_storage::{MemoryStatusStorage, StatusStorage};
use rafka_node_admin_core::storage::MemoryNodesStorage;
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_contract::status::{MeshState, NodeState, NotAuthority, Status, StatusReply, StatusRequest};
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// The cell's artifact directory: the gate's, else this cell's own under the workspace target.
fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2805/unit").join(cell),
    }
}

/// This cell's own span capture: an in-memory OTel exporter behind a dispatcher installed on
/// every thread of the cell's own runtime, so two cells in one test process never share spans.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
    /// The resource's `service.name` this capture exports under.
    service: String,
}

fn capture(cell: &str) -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let service = format!("i143-2805-{cell}");
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", service.clone())]))
        .build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-2805"));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)), service }
}

impl Capture {
    /// The cell's runtime: every worker thread (and the caller's, while it runs) emits into this
    /// capture only.
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    /// Every finished span, as exported: name, TraceId, SpanId, ParentSpanId, resource, attributes.
    fn spans(&self) -> Vec<Value> {
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": s.name,
                    "trace_id": s.span_context.trace_id().to_string(),
                    "span_id": s.span_context.span_id().to_string(),
                    "parent_span_id": s.parent_span_id.to_string(),
                    "resource": {"service.name": self.service},
                    "attributes": attributes,
                })
            })
            .collect()
    }
}

/// A birth in the fixture: its record and the key it speaks with.
struct Birth {
    node: Node,
    key: SecretKey,
}

fn birth(name: &str, kind: NodeKind, mesh: &str, is_primary: bool, is_fabric_primary: bool) -> Birth {
    let key = SecretKey::generate();
    let mut node = Node::allocated(name.parse().unwrap());
    node.kind = kind;
    node.mesh = mesh.into();
    node.node_id = NodeId::mint();
    node.endpoint_id = Some(EndpointId(key.public().to_string()));
    node.incarnation_id = Some(IncarnationId::mint());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::ReadyForTraffic;
    node.is_primary = is_primary;
    node.is_fabric_primary = is_fabric_primary;
    node.transport_addr = Some("127.0.0.1:1".parse().unwrap());
    Birth { node, key }
}

/// One authority (`me`) serving Status (and core ping, so a non-status call can be refused while
/// draining) on its own endpoint, over a view holding `me` and `others`.
struct Rig {
    me: Birth,
    server: rafka_node_rpc::NodeRpcServer,
    _router: Router,
    resolved: ResolvedNode,
    authority: Arc<StatusAuthority>,
}

async fn rig(me: Birth, others: &[&Birth], mesh_ids: BTreeMap<String, MeshId>, drain_in_flight: Option<u64>) -> Rig {
    rig_with(me, others, mesh_ids, drain_in_flight, Arc::new(MemoryStatusStorage::default()), Declared::default(), None).await
}

/// The rig over a given `status.storage` and a given starting `Declared` (what a restarted
/// authority folded from its rows).
async fn rig_with(me: Birth, others: &[&Birth], mesh_ids: BTreeMap<String, MeshId>, drain_in_flight: Option<u64>, status_storage: Arc<dyn StatusStorage>, declared: Declared, fabric: Option<rafka_mesh_entity::FabricId>) -> Rig {
    let mut nodes = vec![me.node.clone()];
    nodes.extend(others.iter().map(|b| b.node.clone()));
    let topology = Arc::new(tokio::sync::RwLock::new(Topology {
        fabric: Fabric { id: fabric.unwrap_or_else(rafka_mesh_entity::FabricId::mint), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes,
    }));
    let fabric_id = topology.read().await.fabric.id.clone();
    let drain: rafka_node_admin_core::status_rpc::Drain = Arc::new(OnceLock::new());
    if let Some(n) = drain_in_flight {
        let door: Arc<dyn Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = u64> + Send>> + Send + Sync> = Arc::new(move || Box::pin(async move { n }));
        let _ = drain.set(door);
    }
    let authority = Arc::new(StatusAuthority {
        me: me.node.name.clone(),
        fabric_id,
        topology,
        declared: Arc::new(Mutex::new(declared)),
        nodes_storage: Arc::new(MemoryNodesStorage::default()),
        status_storage,
        mesh_ids: Arc::new(move || mesh_ids.clone()),
        republish: Arc::new(OnceLock::new()),
        drain,
        hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
    });
    let slot: Arc<OnceLock<Arc<StatusAuthority>>> = Arc::new(OnceLock::new());
    let _ = slot.set(authority.clone());
    let core = ServerBuilder::new().serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
        let PingRequest::Ping { payload } = req;
        Ok(PingReply::Pong { payload })
    });
    let server = rafka_node_admin_core::status_rpc::serve(core, slot)
        .seal(ServedBirth { node_id: me.node.node_id.to_string(), incarnation: me.node.incarnation_id.clone().unwrap().0 })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(me.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let resolved = ResolvedNode {
        node_id: me.node.node_id.clone(),
        name: me.node.name.clone(),
        endpoint_id: me.key.public(),
        transport_addr: addr,
        incarnation: me.node.incarnation_id.clone().unwrap(),
    };
    Rig { me, server, _router: router, resolved, authority }
}

/// `b`'s client to the rig's authority.
async fn client_of(rig: &Rig, b: &Birth) -> NodeRpcClient {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(rig.resolved.clone());
    let ep = rafka_node_rpc::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    NodeRpcClient::new(ep, resolver).with_caller_system("rdm")
}

async fn call(rig: &Rig, b: &Birth, req: &StatusRequest, opts: &CallOptions) -> RpcOutcome<StatusReply> {
    client_of(rig, b).await.call::<Status>(&NodeTarget::ExactNode(rig.me.node.node_id.clone()), req, opts).await.0
}

fn reply(out: &RpcOutcome<StatusReply>) -> StatusReply {
    match out {
        RpcOutcome::Reply(r) => r.value().clone(),
        other => panic!("expected a reply: {other:?}"),
    }
}

/// CONTRACT (#2805): the fabric-primary applies Pending at a mesh's bootstrap admin by the natural
/// key (MeshId, state): a lost reply is Indeterminate at the caller, and the same request again is
/// AlreadyApplied with no second write; the wire carries no transition id. Refused by name: a stale
/// MeshId, a sender that is not the fabric-primary, a backward transition, and a DeclareNodeState
/// for anyone but the sender itself (upward self-report only).
#[test]
fn status_authority_retries_pending_by_natural_key_returns_already_applied() {
    let cell = "status_authority_retries_pending_by_natural_key_returns_already_applied";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let capture = capture(cell);
    capture.run(pending_by_natural_key(&capture, cell, &dir));
}

async fn pending_by_natural_key(capture: &Capture, cell: &str, dir: &std::path::Path) {

    let fabric_primary = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
    let not_primary = birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", false, false);
    let member = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
    let other = birth("mesh2.rpc.2", NodeKind::RpcNode, "mesh2", false, false);
    let mesh2 = MeshId::mint();
    let mesh_ids: BTreeMap<String, MeshId> = [("mesh2".to_string(), mesh2.clone())].into_iter().collect();
    // The receiver: mesh2's bootstrap admin, not yet any seat's holder.
    let me = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
    let rig = rig(me, &[&fabric_primary, &not_primary, &member, &other], mesh_ids, None).await;
    let pending = StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh2".into(), state: MeshState::Pending };

    // Applied, its reply lost: Indeterminate at the caller, and the state is held once.
    rig.authority.hold_next_reply.store(true, Ordering::SeqCst);
    let lost = call(&rig, &fabric_primary, &pending, &CallOptions { budget: Budget::Overall(Duration::from_millis(800)), ..Default::default() }).await;
    assert!(matches!(lost, RpcOutcome::Indeterminate(_)), "{lost:?}");
    let held_after_lost = rig.authority.declared.lock().unwrap().meshes.clone();
    assert_eq!(held_after_lost.get(&mesh2), Some(&MeshState::Pending), "applied before the reply was lost");
    // The retry by the same natural key: AlreadyApplied, nothing written a second time.
    let retry = reply(&call(&rig, &fabric_primary, &pending, &CallOptions::default()).await);
    assert_eq!(retry, StatusReply::AlreadyApplied, "one logical event");
    assert_eq!(rig.authority.declared.lock().unwrap().meshes, held_after_lost, "no second write");

    // Refusals, each by its name.
    let stale = StatusRequest::ApplyMeshState { mesh_id: MeshId::mint(), mesh_name: "mesh2".into(), state: MeshState::Pending };
    let stale_reply = reply(&call(&rig, &fabric_primary, &stale, &CallOptions::default()).await);
    assert_eq!(stale_reply, StatusReply::RejectedStaleMesh { held: mesh2.clone() });
    let wrong_sender = reply(&call(&rig, &not_primary, &pending, &CallOptions::default()).await);
    assert!(matches!(wrong_sender, StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { .. } }), "{wrong_sender:?}");
    let forward = StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh2".into(), state: MeshState::ReadyForTraffic };
    assert_eq!(reply(&call(&rig, &fabric_primary, &forward, &CallOptions::default()).await), StatusReply::Applied);
    let backward = reply(&call(&rig, &fabric_primary, &pending, &CallOptions::default()).await);
    assert!(matches!(backward, StatusReply::RejectedInvalidMeshTransition { current: MeshState::ReadyForTraffic }), "{backward:?}");
    let forged = StatusRequest::DeclareNodeState { node_id: other.node.node_id.clone(), incarnation: other.node.incarnation_id.clone().unwrap(), state: NodeState::Leaving };
    let forged_reply = reply(&call(&rig, &member, &forged, &CallOptions::default()).await);
    assert!(matches!(forged_reply, StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { .. } }), "a declaration is the sender's own: {forged_reply:?}");
    let meshes_final = rig.authority.declared.lock().unwrap().meshes.clone();
    assert_eq!(meshes_final.len(), 1, "one mesh held, by its own id");

    let spans = capture.spans();
    let decisions: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.status.update.via-declaration").collect();
    assert!(decisions.iter().any(|s| s["attributes"]["op"] == "apply-mesh-state" && s["attributes"]["outcome"] == "already-applied"), "the retry is spanned as already-applied");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let result = json!({
        "cell": cell,
        "mesh_id": mesh2.to_string(),
        "lost_reply": format!("{lost:?}").split('(').next(),
        "retry": format!("{retry:?}"),
        "held_after_lost": format!("{held_after_lost:?}"),
        "stale_mesh": format!("{stale_reply:?}"),
        "sender_not_fabric_primary": format!("{wrong_sender:?}"),
        "backward": format!("{backward:?}"),
        "forged_declaration": format!("{forged_reply:?}"),
        "meshes_final": format!("{meshes_final:?}"),
        "declaration_spans": decisions.len(),
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// CONTRACT (#2805): after the exact birth applies Draining, its status door stays admitted while
/// every other call is refused as Draining: a probe answers the current state, a repeated drain
/// answers the in-flight count again, a stale incarnation is refused by name. Every drain call
/// classifies as exactly one DrainOutcome arm and a refusal is never Established: Established
/// (NodeDrainingApplied), Refused (a stale incarnation), NotSent (cut before the send),
/// Indeterminate (the reply lost); Deadline is the retire pipeline's WaitForDrain arm when an
/// established drain never finishes.
#[test]
fn draining_authority_serves_status_checks_exact_birth() {
    let cell = "draining_authority_serves_status_checks_exact_birth";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let capture = capture(cell);
    capture.run(draining_status_door(&capture, cell, &dir));
}

async fn draining_status_door(capture: &Capture, cell: &str, dir: &std::path::Path) {

    // The subject: mesh1.admin.2, drained by its executor mesh1.admin.1.
    let executor = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
    let subject = birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false);
    let (node_id, incarnation) = (subject.node.node_id.clone(), subject.node.incarnation_id.clone().unwrap());
    let rig = rig(subject, &[&executor], BTreeMap::new(), Some(3)).await;
    let drain = StatusRequest::ApplyNodeState { node_id: node_id.clone(), incarnation: incarnation.clone(), state: NodeState::Draining };

    // Healthy control before draining: a ping is served.
    let c = client_of(&rig, &executor).await;
    let target = NodeTarget::ExactNode(node_id.clone());
    let (ping, _) = c.call::<Ping>(&target, &PingRequest::Ping { payload: b"before".to_vec() }, &CallOptions::default()).await;
    assert!(matches!(&ping, RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. })), "{ping:?}");

    // Established: the drain is applied; the server now refuses ordinary calls as Draining.
    let established = call(&rig, &executor, &drain, &CallOptions::default()).await;
    assert_eq!(drain_outcome(&established), DrainOutcome::Established { in_flight: 3 });
    rig.server.drain();
    let (refused_ping, _) = c.call::<Ping>(&target, &PingRequest::Ping { payload: b"after".to_vec() }, &CallOptions::default()).await;
    assert!(matches!(&refused_ping, RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Draining { .. })), "a non-status call is refused as Draining: {refused_ping:?}");
    // The status door stays admitted: a repeat drain answers again, a probe answers the state.
    assert_eq!(drain_outcome(&call(&rig, &executor, &drain, &CallOptions::default()).await), DrainOutcome::Established { in_flight: 3 }, "a repeat drain answers the count again");
    let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
    let probed = reply(&call(&rig, &executor, &probe, &CallOptions::default()).await);
    assert!(matches!(&probed, StatusReply::Current { node_id: n, .. } if *n == node_id), "{probed:?}");

    // Refused: a stale incarnation, by name, never Established.
    let stale = StatusRequest::ApplyNodeState { node_id: node_id.clone(), incarnation: IncarnationId::mint(), state: NodeState::Draining };
    let stale_out = call(&rig, &executor, &stale, &CallOptions::default()).await;
    assert!(matches!(reply(&stale_out), StatusReply::RejectedStaleIncarnation { .. }), "{stale_out:?}");
    let refused = drain_outcome(&stale_out);
    assert!(matches!(&refused, DrainOutcome::Refused { reply } if reply.contains("stale")), "{refused:?}");
    // NotSent: cut before the request's FIN.
    let (cut, _) = c.call::<Status>(&target, &drain, &CallOptions { cut_before_finish: true, ..Default::default() }).await;
    let not_sent = drain_outcome(&cut);
    assert!(matches!(not_sent, DrainOutcome::NotSent { .. }), "{not_sent:?}");
    // Indeterminate: applied, the reply lost.
    rig.authority.hold_next_reply.store(true, Ordering::SeqCst);
    let (lost, _) = c.call::<Status>(&target, &drain, &CallOptions { budget: Budget::Overall(Duration::from_millis(800)), ..Default::default() }).await;
    let indeterminate = drain_outcome(&lost);
    assert!(matches!(indeterminate, DrainOutcome::Indeterminate { .. }), "{indeterminate:?}");
    // Deadline: the retire pipeline's WaitForDrain over an established drain that never finishes.
    let deadline = deadline_arm().await;
    assert_eq!(deadline, DrainOutcome::Deadline { last_in_flight: Some(3) });

    let spans = capture.spans();
    let applies = spans.iter().filter(|s| s["name"] == "rdm.node_admin.status.update.via-apply-draining").count();
    assert!(applies >= 2, "every applied drain is spanned: {applies}");
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let result = json!({
        "cell": cell,
        "subject": node_id.to_string(),
        "ping_before": "pong",
        "ping_while_draining": "draining",
        "probe": format!("{probed:?}"),
        "arms": {
            "established": format!("{:?}", DrainOutcome::Established { in_flight: 3 }),
            "refused": format!("{refused:?}"),
            "not_sent": format!("{not_sent:?}"),
            "indeterminate": format!("{indeterminate:?}"),
            "deadline": format!("{deadline:?}"),
        },
        "server_draining_refusals": rig.server.stats().draining.load(Ordering::SeqCst),
        "apply_draining_spans": applies,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// The retire pipeline run to its WaitForDrain over a birth whose drain is established and never
/// finishes: the step's receipt is the arm.
async fn deadline_arm() -> DrainOutcome {
    use rafka_node_admin_core::build::BuildId;
    use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
    use rafka_node_admin_core::deployment::pipeline::{DeploymentPipeline, LaunchTemplate, NodeObserver, NoLifecycleEvents, RetireKind, RetireRequest, RetireStep, Timeouts, TopologySink};
    use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};

    struct Running;
    #[async_trait::async_trait]
    impl DeploymentProvider for Running {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            "i143-2805".into()
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
            Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this cell births nothing".into() })
        }
        async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
            Ok(())
        }
        async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
            DeploymentStatus::Running
        }
        async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, _: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
            None
        }
    }
    /// The birth answers the drain with three calls in flight and never finishes them.
    struct NeverDrained;
    #[async_trait::async_trait]
    impl NodeObserver for NeverDrained {
        async fn drain(&self, _: &Node) -> DrainOutcome {
            DrainOutcome::Established { in_flight: 3 }
        }
        async fn drained(&self, _: &Node) -> bool {
            false
        }
        async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<rafka_node_admin_core::deployment::pipeline::Publication> {
            None
        }
        async fn ready(&self, _: &Node) -> Result<(), String> {
            Ok(())
        }
        async fn admission_closed(&self, _: &Node) -> Result<(), String> {
            Ok(())
        }
    }
    struct Discard;
    impl TopologySink for Discard {
        fn publish(&self, _: Node) {}
        fn remove(&self, _: &rafka_node_admin_core::model::PathName) {}
    }

    // The drawing's first box: the operation is claimed under an accepted Build.
    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&rafka_node_admin_core::build_state::BuildAccepted {
            build_id: build_id.clone(),
            topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"),
            submitted_change: None,
            submitted_at_ms: 0,
        })
        .await
        .unwrap();
    builds.claim_attempt(&rafka_node_admin_core::build_state::BuildAttemptClaim { build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let node = birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", false, false).node;
    let template = LaunchTemplate {
        fabric: "fabric1".into(),
        fabric_id: rafka_mesh_entity::FabricId::mint(),
        executable: "/nonexistent/rshape-none".into(),
        seeds: vec![],
        launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: rafka_mesh_entity::IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-2805-{}", NodeId::mint())),
    };
    let joins = rafka_node_admin_core::join::Joins::default();
    let pipeline = DeploymentPipeline {
        provider: &Running,
        joins: &joins,
        observer: &NeverDrained,
        sink: &Discard,
        lifecycle: &NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { drain: Duration::from_millis(300), stop_grace: Duration::from_millis(300), ..Timeouts::default() },
    };
    let handle = DeploymentHandle {
        deployment_id: rafka_node_admin_core::model::DeploymentId::mint(),
        provider: ProviderKind::Process,
        pid: Some(1),
        start: Some(1),
        container: None,
        domain: Some("i143-2805".into()),
    };
    let req = RetireRequest { build_id: build_id.clone(), attempt: 1, node, handle, kind: RetireKind::Removal, observe_departure: false };
    let _ = pipeline.retire(&req).await;
    // The drawing's order, as far as this birth lets the retire go: NodeDeleting, the drain
    // (MarkDraining), its result (WaitForDrain: the deadline arm); and never NodeDeleted, because
    // the provider never proves the runtime dead.
    let view = builds.read_build(&build_id).await.unwrap();
    let order: Vec<&str> = view.steps.iter().map(|s| s.step.as_str()).collect();
    let pos = |name: &str| order.iter().position(|s| *s == name);
    let (deleting, marked, waited) = (pos(RetireStep::NodeDeleting.name()), pos(RetireStep::MarkDraining.name()), pos(RetireStep::WaitForDrain.name()));
    assert!(deleting.is_some() && deleting < marked && marked < waited, "NodeDeleting, then the drain, then its result: {order:?}");
    assert!(pos(RetireStep::NodeDeleted.name()).is_none(), "no NodeDeleted without the provider's proof of death: {order:?}");
    let step = &view.steps[waited.unwrap()];
    serde_json::from_value(step.output.clone().expect("WaitForDrain records its arm")).unwrap()
}

// ---------------------------------------------------------------------------------------------
// The adversary's cells: every refusal by its name, the durable row before the answer, the seat
// and the birth moving under an in-flight declaration.

/// Every span of `name` this cell emitted whose attributes carry the given pairs.
fn spans_where<'a>(spans: &'a [Value], name: &str, want: &[(&str, &str)]) -> Vec<&'a Value> {
    spans.iter().filter(|s| s["name"] == name && want.iter().all(|(k, v)| s["attributes"][*k] == *v)).collect()
}

const DECLARATION: &str = "rdm.node_admin.status.update.via-declaration";

fn finish(capture: &Capture, dir: &std::path::Path, result: Value) -> Vec<Value> {
    let spans = capture.spans();
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    spans
}

fn cell_dir(cell: &str) -> PathBuf {
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A `status.storage` whose puts are refused while `refuse` is set, over a real memory store.
struct Refusing {
    inner: MemoryStatusStorage,
    refuse: std::sync::atomic::AtomicBool,
}

impl Refusing {
    fn refusing() -> Arc<Self> {
        Arc::new(Self { inner: MemoryStatusStorage::default(), refuse: std::sync::atomic::AtomicBool::new(true) })
    }
    fn check(&self) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(rafka_node_admin_core::record_store::StorageError::Io { file: "status/refused.json".into(), reason: "the disk refused the put".into() });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl StatusStorage for Refusing {
    async fn put_mesh_status(&self, row: &rafka_node_admin_core::status_storage::MeshStatusRow) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        self.check()?;
        self.inner.put_mesh_status(row).await
    }
    async fn mesh_statuses(&self) -> Result<std::collections::HashMap<MeshId, MeshState>, rafka_node_admin_core::record_store::StorageError> {
        self.inner.mesh_statuses().await
    }
    async fn put_fabric_event(&self, row: &rafka_node_admin_core::status_storage::FabricEventRow) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        self.check()?;
        self.inner.put_fabric_event(row).await
    }
    async fn fabric_events(&self) -> Result<Vec<rafka_node_admin_core::status_storage::FabricEventRow>, rafka_node_admin_core::record_store::StorageError> {
        self.inner.fabric_events().await
    }
}

/// CONTRACT (#2805, acceptance 9): a Mesh status or Fabric event is answered `Applied` only after
/// its own keyed row was acknowledged. A refused put answers NotReady naming status.storage and
/// holds nothing, so the same declaration again is decided afresh and applies (it is never
/// AlreadyApplied over a fact nothing recorded). An authority restarted over the same rows folds
/// them and answers the same natural keys AlreadyApplied. What must NOT happen: an Applied or
/// AlreadyApplied for a row that was refused.
#[test]
fn status_authority_answers_applied_only_after_its_row_is_acknowledged() {
    let cell = "status_authority_answers_applied_only_after_its_row_is_acknowledged";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        let fabric_primary = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let mesh2 = MeshId::mint();
        let fabric = rafka_mesh_entity::FabricId::mint();
        let ids: BTreeMap<String, MeshId> = [("mesh2".to_string(), mesh2.clone())].into_iter().collect();
        let store = Refusing::refusing();
        let me = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let rig = rig_with(me, &[&fabric_primary], ids.clone(), None, store.clone(), Declared::default(), Some(fabric.clone())).await;
        let pending = StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh2".into(), state: MeshState::Pending };

        let refused = reply(&call(&rig, &fabric_primary, &pending, &CallOptions::default()).await);
        assert!(matches!(&refused, StatusReply::NotReady { reason } if reason.contains("status.storage refused the Mesh status row") && reason.contains("the disk refused the put")), "{refused:?}");
        assert!(rig.authority.declared.lock().unwrap().meshes.is_empty(), "a refused row holds nothing as applied");
        assert!(store.inner.mesh_statuses().await.unwrap().is_empty());

        store.refuse.store(false, Ordering::SeqCst);
        let first = reply(&call(&rig, &fabric_primary, &pending, &CallOptions::default()).await);
        assert_eq!(first, StatusReply::Applied, "the retry of a refused put applies; it is not AlreadyApplied");
        assert_eq!(store.inner.mesh_statuses().await.unwrap().get(&mesh2), Some(&MeshState::Pending), "the row was put before the answer");
        let again = reply(&call(&rig, &fabric_primary, &pending, &CallOptions::default()).await);
        assert_eq!(again, StatusReply::AlreadyApplied);

        // A Fabric event: refused, then applied, then a repeat.
        let mesh1_primary = birth("mesh3.admin.1", NodeKind::NodeAdmin, "mesh3", true, false);
        let rig3 = rig_with(mesh1_primary, &[&fabric_primary], BTreeMap::new(), None, store.clone(), Declared::default(), Some(fabric.clone())).await;
        let event = StatusRequest::ApplyFabricEvent { fabric_id: fabric.clone(), event: rafka_node_rpc_contract::status::FabricEvent::ReadyForTraffic };
        store.refuse.store(true, Ordering::SeqCst);
        let ev_refused = reply(&call(&rig3, &fabric_primary, &event, &CallOptions::default()).await);
        assert!(matches!(&ev_refused, StatusReply::NotReady { reason } if reason.contains("status.storage refused the Fabric event row")), "{ev_refused:?}");
        assert!(rig3.authority.declared.lock().unwrap().fabric.is_empty());
        store.refuse.store(false, Ordering::SeqCst);
        assert_eq!(reply(&call(&rig3, &fabric_primary, &event, &CallOptions::default()).await), StatusReply::Applied);

        // The authority restarts over the same rows: what it folds is what it applied.
        let folded = Declared::rehydrate(&*store, &MemoryNodesStorage::default()).await.unwrap();
        assert_eq!(folded.meshes.get(&mesh2), Some(&MeshState::Pending));
        let me_again = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let restarted = rig_with(me_again, &[&fabric_primary], ids, None, store.clone(), folded, Some(fabric.clone())).await;
        let after_restart = reply(&call(&restarted, &fabric_primary, &pending, &CallOptions::default()).await);
        assert_eq!(after_restart, StatusReply::AlreadyApplied, "the folded row answers the natural key");

        let spans = finish(
            &capture,
            &dir,
            json!({"cell": cell, "refused_mesh": format!("{refused:?}"), "retry": format!("{first:?}"), "repeat": format!("{again:?}"), "refused_event": format!("{ev_refused:?}"), "after_restart": format!("{after_restart:?}")}),
        );
        let nr = spans_where(&spans, DECLARATION, &[("outcome", "not-ready")]);
        assert_eq!(nr.len(), 2, "both refusals are spanned as not-ready: {nr:?}");
        assert_eq!(spans_where(&spans, DECLARATION, &[("op", "apply-mesh-state"), ("outcome", "applied")]).len(), 1);
        assert_eq!(spans_where(&spans, DECLARATION, &[("op", "apply-mesh-state"), ("outcome", "already-applied")]).len(), 2);
    });
}

/// CONTRACT (#2805, adversary): every backward move through the door is refused by its name with
/// the current state it ran into, for each of the four node states and each of the four Mesh
/// states; the same state again is AlreadyApplied, three times over, the same typed reply; a skip
/// forward applies. What must NOT happen: a backward move applied, or a repeat answered anything
/// but AlreadyApplied.
#[test]
fn status_authority_refuses_every_backward_move_by_name() {
    let cell = "status_authority_refuses_every_backward_move_by_name";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        let nodes = [NodeState::Pending, NodeState::ReadyForTraffic, NodeState::Draining, NodeState::Leaving];
        let meshes = [MeshState::Pending, MeshState::ReadyForTraffic, MeshState::Draining, MeshState::Retired];
        let me = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let senders: Vec<Birth> = (0..4).map(|i| birth(&format!("mesh1.rpc.{}", i + 1), NodeKind::RpcNode, "mesh1", false, false)).collect();
        let fabric_primary = birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false);
        let receiver = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let node_rig = rig(me, &senders.iter().collect::<Vec<_>>(), BTreeMap::new(), None).await;
        let mut fp = fabric_primary;
        fp.node.is_fabric_primary = true;
        let mesh_rig = rig(receiver, &[&fp], BTreeMap::new(), None).await;
        let mut refused_node = 0;
        let mut refused_mesh = 0;
        let mut repeats = 0;
        for (i, start) in nodes.iter().enumerate() {
            let s = &senders[i];
            let decl = |st: NodeState| StatusRequest::DeclareNodeState { node_id: s.node.node_id.clone(), incarnation: s.node.incarnation_id.clone().unwrap(), state: st };
            assert_eq!(reply(&call(&node_rig, s, &decl(*start), &CallOptions::default()).await), StatusReply::Applied, "{start:?}");
            for lower in nodes.iter().filter(|l| *l < start) {
                let r = reply(&call(&node_rig, s, &decl(*lower), &CallOptions::default()).await);
                assert_eq!(r, StatusReply::RejectedInvalidNodeTransition { current: *start }, "{lower:?} after {start:?}");
                refused_node += 1;
            }
            for _ in 0..3 {
                assert_eq!(reply(&call(&node_rig, s, &decl(*start), &CallOptions::default()).await), StatusReply::AlreadyApplied, "a repeat of {start:?}");
                repeats += 1;
            }
            assert_eq!(node_rig.authority.declared.lock().unwrap().node(&s.node.node_id).map(|(_, st)| st), Some(*start), "refusals moved nothing");
        }
        for start in meshes {
            let id = MeshId::mint();
            let apply = |st: MeshState| StatusRequest::ApplyMeshState { mesh_id: id.clone(), mesh_name: "mesh2".into(), state: st };
            assert_eq!(reply(&call(&mesh_rig, &fp, &apply(start), &CallOptions::default()).await), StatusReply::Applied, "{start:?}");
            for lower in meshes.iter().filter(|l| **l < start) {
                let r = reply(&call(&mesh_rig, &fp, &apply(*lower), &CallOptions::default()).await);
                assert_eq!(r, StatusReply::RejectedInvalidMeshTransition { current: start }, "{lower:?} after {start:?}");
                refused_mesh += 1;
            }
            assert_eq!(reply(&call(&mesh_rig, &fp, &apply(start), &CallOptions::default()).await), StatusReply::AlreadyApplied);
            repeats += 1;
            assert_eq!(mesh_rig.authority.declared.lock().unwrap().meshes.get(&id), Some(&start));
        }
        // A skip forward is a forward move.
        let skip = MeshId::mint();
        let to_retired = StatusRequest::ApplyMeshState { mesh_id: skip.clone(), mesh_name: "mesh2".into(), state: MeshState::Retired };
        assert_eq!(reply(&call(&mesh_rig, &fp, &to_retired, &CallOptions::default()).await), StatusReply::Applied, "an unseen Mesh may be told Retired: a skip is forward");

        let spans = finish(&capture, &dir, json!({"cell": cell, "refused_node_moves": refused_node, "refused_mesh_moves": refused_mesh, "repeats_already_applied": repeats}));
        assert_eq!(refused_node, 6, "0+1+2+3 backward pairs");
        assert_eq!(refused_mesh, 6);
        assert_eq!(spans_where(&spans, DECLARATION, &[("outcome", "rejected-invalid-node-transition")]).len(), refused_node);
        assert_eq!(spans_where(&spans, DECLARATION, &[("outcome", "rejected-invalid-mesh-transition")]).len(), refused_mesh);
        assert_eq!(spans_where(&spans, DECLARATION, &[("outcome", "already-applied")]).len(), repeats);
    });
}

/// CONTRACT (#2805, adversary): a declaration to a receiver that does not hold the seat it needs
/// is refused naming the seat, and one from a sender that is not the subject or the authority is
/// refused naming the sender; a stranger whose key the view does not hold is refused as unknown;
/// a stale Fabric id and a stale Mesh id are refused naming the id the receiver holds. What must
/// NOT happen: any of these applied, or answered as a transport failure.
#[test]
fn status_declaration_to_a_non_authority_is_refused_naming_the_seat() {
    let cell = "status_declaration_to_a_non_authority_is_refused_naming_the_seat";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        // The receiver: a mesh1 admin that holds no seat.
        let fp = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let rpc = birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", false, false);
        let other_admin = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let stranger = birth("mesh9.rpc.1", NodeKind::RpcNode, "mesh9", false, false);
        let receiver = birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false);
        let rig = rig(receiver, &[&fp, &rpc, &other_admin], BTreeMap::new(), None).await;
        let not_authority = |r: &StatusReply, name: &str| match r {
            StatusReply::RejectedNotAuthority { why } => assert_eq!(why.as_str(), name, "{r:?}"),
            other => panic!("expected rejected-not-authority/{name}: {other:?}"),
        };
        let decl_node = |b: &Birth| StatusRequest::DeclareNodeState { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic };
        // An ordinary node declares to a non-primary admin.
        let r = reply(&call(&rig, &rpc, &decl_node(&rpc), &CallOptions::default()).await);
        not_authority(&r, "receiver-not-primary");
        assert!(matches!(&r, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } } if needed == "mesh-primary of mesh1"), "{r:?}");
        // A node-admin declares to a non-fabric-primary.
        let r = reply(&call(&rig, &other_admin, &decl_node(&other_admin), &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } } if needed == "fabric-primary of mesh2"), "{r:?}");
        // A Mesh primary declares its Mesh to a non-fabric-primary.
        let mesh2 = MeshId::mint();
        let r = reply(&call(&rig, &other_admin, &StatusRequest::DeclareMeshState { mesh_id: mesh2.clone(), state: MeshState::ReadyForTraffic }, &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } } if needed == "fabric-primary"), "{r:?}");
        // The fabric-primary applies an event at an admin that is not a mesh primary.
        let event = StatusRequest::ApplyFabricEvent { fabric_id: rig.authority.fabric_id.clone(), event: rafka_node_rpc_contract::status::FabricEvent::ReadyForTraffic };
        let r = reply(&call(&rig, &fp, &event, &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } } if needed == "mesh-primary"), "{r:?}");
        // The fabric-primary applies a Mesh state at an admin of another mesh.
        let r = reply(&call(&rig, &fp, &StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh2".into(), state: MeshState::Pending }, &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } } if needed == "an admin of mesh2"), "{r:?}");
        // A sender that is not the fabric-primary applies an event or a Mesh state.
        let r = reply(&call(&rig, &other_admin, &event, &CallOptions::default()).await);
        not_authority(&r, "sender-not-subject");
        let r = reply(&call(&rig, &rpc, &StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh1".into(), state: MeshState::Pending }, &CallOptions::default()).await);
        not_authority(&r, "sender-not-subject");
        // A stranger the view does not hold.
        let r = reply(&call(&rig, &stranger, &decl_node(&stranger), &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender } } if sender == "unknown peer"), "{r:?}");
        // Stale ids: another Fabric's event at the mesh primary; another Mesh id for a held name.
        let mp_receiver = birth("mesh1.admin.3", NodeKind::NodeAdmin, "mesh1", true, false);
        let held = MeshId::mint();
        let ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), held.clone())].into_iter().collect();
        let rig2 = rig_with(mp_receiver, &[&fp], ids, None, Arc::new(MemoryStatusStorage::default()), Declared::default(), None).await;
        let wrong_fabric = rafka_mesh_entity::FabricId::mint();
        let r = reply(&call(&rig2, &fp, &StatusRequest::ApplyFabricEvent { fabric_id: wrong_fabric, event: rafka_node_rpc_contract::status::FabricEvent::ReadyForTraffic }, &CallOptions::default()).await);
        assert_eq!(r, StatusReply::RejectedStaleFabric { held: rig2.authority.fabric_id.clone() });
        let r = reply(&call(&rig2, &fp, &StatusRequest::ApplyMeshState { mesh_id: MeshId::mint(), mesh_name: "mesh1".into(), state: MeshState::Pending }, &CallOptions::default()).await);
        assert_eq!(r, StatusReply::RejectedStaleMesh { held });
        assert!(rig.authority.declared.lock().unwrap().nodes.is_empty() && rig.authority.declared.lock().unwrap().meshes.is_empty() && rig.authority.declared.lock().unwrap().fabric.is_empty(), "nothing applied by any refusal");

        let spans = finish(&capture, &dir, json!({"cell": cell, "refusals": 10}));
        for outcome in ["rejected-not-authority", "rejected-stale-fabric", "rejected-stale-mesh"] {
            assert!(!spans_where(&spans, DECLARATION, &[("outcome", outcome)]).is_empty(), "{outcome} is spanned");
        }
        assert_eq!(spans_where(&spans, DECLARATION, &[("outcome", "rejected-not-authority")]).len(), 8);
        assert!(!spans_where(&spans, DECLARATION, &[("outcome", "rejected-not-authority"), ("detail", "receiver-not-primary")]).is_empty());
    });
}

/// CONTRACT (#2805, acceptance 10): an event applied by the fabric-primary stays applied when the
/// seat moves: the old authority's in-flight application is refused (not the sender), the new
/// authority's repeat of the same natural key is AlreadyApplied, a new event applies, and a
/// receiver restarted over its rows still answers the first event AlreadyApplied. What must NOT
/// happen: the old authority applied after it lost the seat, or an applied event applied twice.
#[test]
fn applied_events_survive_authority_change_and_the_old_authority_is_refused() {
    let cell = "applied_events_survive_authority_change_and_the_old_authority_is_refused";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        use rafka_node_rpc_contract::status::FabricEvent;
        let fp1 = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let fp2 = birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false);
        let receiver = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let fabric = rafka_mesh_entity::FabricId::mint();
        let store: Arc<MemoryStatusStorage> = Arc::new(MemoryStatusStorage::default());
        let rig = rig_with(receiver, &[&fp1, &fp2], BTreeMap::new(), None, store.clone(), Declared::default(), Some(fabric.clone())).await;
        let ready = StatusRequest::ApplyFabricEvent { fabric_id: fabric.clone(), event: FabricEvent::ReadyForTraffic };
        assert_eq!(reply(&call(&rig, &fp1, &ready, &CallOptions::default()).await), StatusReply::Applied);
        // The seat moves: fp2 holds it, fp1 does not.
        {
            let mut t = rig.authority.topology.write().await;
            for n in t.nodes.iter_mut() {
                if n.name == fp1.node.name {
                    n.is_fabric_primary = false;
                }
                if n.name == fp2.node.name {
                    n.is_fabric_primary = true;
                }
            }
        }
        let shutdown = StatusRequest::ApplyFabricEvent { fabric_id: fabric.clone(), event: FabricEvent::ShutdownInitiated { initiated_by: "mesh1.admin.1".into() } };
        let old = reply(&call(&rig, &fp1, &shutdown, &CallOptions::default()).await);
        assert!(matches!(&old, StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender } } if sender == "mesh1.admin.1"), "the old authority's in-flight work: {old:?}");
        assert_eq!(reply(&call(&rig, &fp1, &ready, &CallOptions::default()).await), reply_of_not_authority(&old), "even a repeat of an applied key is refused to the old authority");
        assert_eq!(reply(&call(&rig, &fp2, &ready, &CallOptions::default()).await), StatusReply::AlreadyApplied, "previously applied events survive the move");
        assert_eq!(reply(&call(&rig, &fp2, &shutdown, &CallOptions::default()).await), StatusReply::Applied, "the new authority applies new work");
        // The receiver restarts over its rows.
        let folded = Declared::rehydrate(&*store, &MemoryNodesStorage::default()).await.unwrap();
        let receiver2 = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let rig2 = rig_with(receiver2, &[&fp1, &fp2], BTreeMap::new(), None, store.clone(), folded, Some(fabric.clone())).await;
        {
            let mut t = rig2.authority.topology.write().await;
            for n in t.nodes.iter_mut().filter(|n| n.name == fp1.node.name) {
                n.is_fabric_primary = false;
            }
            for n in t.nodes.iter_mut().filter(|n| n.name == fp2.node.name) {
                n.is_fabric_primary = true;
            }
        }
        assert_eq!(reply(&call(&rig2, &fp2, &ready, &CallOptions::default()).await), StatusReply::AlreadyApplied, "the restarted receiver folded the event");
        assert_eq!(reply(&call(&rig2, &fp2, &shutdown, &CallOptions::default()).await), StatusReply::AlreadyApplied);
        let spans = finish(&capture, &dir, json!({"cell": cell, "old_authority": format!("{old:?}"), "events_held": store.fabric_events().await.unwrap().len()}));
        assert_eq!(store.fabric_events().await.unwrap().len(), 2, "each event is one row");
        assert_eq!(spans_where(&spans, DECLARATION, &[("op", "apply-fabric-event"), ("outcome", "applied")]).len(), 2);
        assert_eq!(spans_where(&spans, DECLARATION, &[("op", "apply-fabric-event"), ("outcome", "rejected-not-authority"), ("sender", "mesh1.admin.1")]).len(), 2);
        assert_eq!(spans_where(&spans, DECLARATION, &[("op", "apply-fabric-event"), ("outcome", "already-applied")]).len(), 3);
    });
}

fn reply_of_not_authority(r: &StatusReply) -> StatusReply {
    r.clone()
}

/// CONTRACT (#2805, adversary): a declaration racing a restart. The subject's birth is replaced in
/// the receiver's view (same node, new incarnation, same key): the old incarnation's in-flight
/// declaration is refused naming the incarnation the receiver holds, nothing is applied under it,
/// and the new incarnation's declaration applies on a clean key (the old birth's state is not
/// its). What must NOT happen: the old birth's state applied, or carried to the new birth.
#[test]
fn status_declaration_racing_a_restart_is_refused_stale_then_the_new_birth_applies() {
    let cell = "status_declaration_racing_a_restart_is_refused_stale_then_the_new_birth_applies";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        let me = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let subject = birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", false, false);
        let rig = rig(me, &[&subject], BTreeMap::new(), None).await;
        let old_inc = subject.node.incarnation_id.clone().unwrap();
        let decl = |inc: &IncarnationId, st: NodeState| StatusRequest::DeclareNodeState { node_id: subject.node.node_id.clone(), incarnation: inc.clone(), state: st };
        assert_eq!(reply(&call(&rig, &subject, &decl(&old_inc, NodeState::Draining), &CallOptions::default()).await), StatusReply::Applied, "the old birth reached Draining");
        // The restart: same node and key, a new incarnation, now what the view holds.
        let new_inc = IncarnationId::mint();
        {
            let mut t = rig.authority.topology.write().await;
            t.nodes.iter_mut().find(|n| n.node_id == subject.node.node_id).unwrap().incarnation_id = Some(new_inc.clone());
        }
        let in_flight = reply(&call(&rig, &subject, &decl(&old_inc, NodeState::Leaving), &CallOptions::default()).await);
        assert_eq!(in_flight, StatusReply::RejectedStaleIncarnation { held: new_inc.clone() });
        assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node.node_id), Some((old_inc.clone(), NodeState::Draining)), "the stale declaration moved nothing");
        let fresh = reply(&call(&rig, &subject, &decl(&new_inc, NodeState::Pending), &CallOptions::default()).await);
        assert_eq!(fresh, StatusReply::Applied, "the new birth starts on a clean key: Pending after the old birth's Draining is not backward");
        assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node.node_id), Some((new_inc.clone(), NodeState::Pending)));
        let spans = finish(&capture, &dir, json!({"cell": cell, "in_flight": format!("{in_flight:?}"), "fresh": format!("{fresh:?}"), "new_incarnation": new_inc.to_string()}));
        let stale = spans_where(&spans, DECLARATION, &[("outcome", "rejected-stale-incarnation")]);
        assert_eq!(stale.len(), 1, "{stale:?}");
        assert_eq!(stale[0]["attributes"]["detail"], new_inc.to_string().as_str(), "the span names the incarnation held");
    });
}

/// CONTRACT (#2805, acceptance 12): a lost route never makes a node Dead. After a healthy control
/// call applied a declaration, a call to a target the caller has no route for is NotSent with
/// nothing dispatched; the receiver's view of every node keeps its status (none Dead), the applied
/// state is unchanged, and no declaration span is emitted for the unsent call. What must NOT
/// happen: a status of Dead or Leaving appearing from the failed call.
#[test]
fn status_route_loss_never_synthesizes_dead() {
    let cell = "status_route_loss_never_synthesizes_dead";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        let me = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let subject = birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", false, false);
        let rig = rig(me, &[&subject], BTreeMap::new(), None).await;
        let decl = StatusRequest::DeclareNodeState { node_id: subject.node.node_id.clone(), incarnation: subject.node.incarnation_id.clone().unwrap(), state: NodeState::ReadyForTraffic };
        // The healthy control, in the same capture.
        assert_eq!(reply(&call(&rig, &subject, &decl, &CallOptions::default()).await), StatusReply::Applied);
        let served_before = spans_where(&capture.spans(), DECLARATION, &[]).len();
        // No route: a client whose resolver holds nothing for the target.
        let ep = rafka_node_rpc::endpoint::bind(subject.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let routeless = NodeRpcClient::new(ep, Arc::new(StaticResolver::new())).with_caller_system("rdm");
        let to = NodeTarget::ExactNode(rig.me.node.node_id.clone());
        let again = StatusRequest::DeclareNodeState { node_id: subject.node.node_id.clone(), incarnation: subject.node.incarnation_id.clone().unwrap(), state: NodeState::Leaving };
        let (out, _) = routeless.call::<Status>(&to, &again, &CallOptions::default()).await;
        assert!(matches!(out, RpcOutcome::NotSent(_)), "no route is NotSent, nothing dispatched: {out:?}");
        assert!(out.proves_not_dispatched());
        let t = rig.authority.topology.read().await;
        assert!(t.nodes.iter().all(|n| n.status == NodeStatus::ReadyForTraffic), "no node became Dead or Leaving: {:?}", t.nodes.iter().map(|n| (n.name.to_string(), n.status)).collect::<Vec<_>>());
        drop(t);
        assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node.node_id).map(|(_, s)| s), Some(NodeState::ReadyForTraffic), "the applied state is unchanged");
        let spans = finish(&capture, &dir, json!({"cell": cell, "no_route": out.name(), "declaration_spans_before": served_before}));
        assert_eq!(spans_where(&spans, DECLARATION, &[]).len(), served_before, "the unsent call reached no handler");
        assert!(spans.iter().all(|s| !s["name"].as_str().unwrap_or("").contains("dead")), "no span names a death");
    });
}
