//! i143.e4.s11 acceptance (rafka-v2 #2805, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2805-unit`, which exports `I143_ACCEPTANCE_DIR`; each
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span this process
//! emitted, captured in-process by the evidence exporter) there.
//!
//! The status authority is a real node-admin `StatusAuthority` served over Node RPC on its own
//! endpoint; every caller is a real `NodeRpcClient` speaking as a known birth.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{EndpointId, IncarnationId, MeshId, NodeId, NodeKind};
use rdm_admin::model::{Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rdm_admin::status_rpc::{Declared, StatusAuthority};
use rdm_admin::fabric_storage::{FabricStorage, MemoryFabricStorage};
use rdm_admin::storage::{MemoryMeshStorage, MeshStorage};
use rdm_admin::topology::Topology;
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rdm_core::catalog::OpOwner;
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingReply, PingRequest};
use rdm_core::status::{MeshState, NodeState, NotPrimary, Status, StatusReply, StatusRequest};
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

/// This cell's own span capture: the product span pipeline (`rdm_transport::span_capture`: its span filter and
/// `BatchSpanProcessor`) in front of an in-memory exporter, behind a dispatcher installed on
/// every thread of the cell's own runtime, so two cells in one test process never share spans.
/// When it drops, whether the cell passed or panicked, every span it holds goes to the cell's
/// `spans.json`, so a red is read from its own trace and never from a rerun.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
    /// The resource's `service.name` this capture exports under.
    service: String,
    /// The cell's artifact directory.
    dir: PathBuf,
}

fn capture(cell: &str) -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let service = format!("i143-2805-{cell}");
    let (capture, layer) = rdm_transport::span_capture(&service, exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)), service, dir: acceptance_dir(cell) }
}

impl Drop for Capture {
    fn drop(&mut self) {
        let flushed = self.capture.flush(Duration::from_secs(10));
        let spans = self.rows();
        let path = self.dir.join("spans.json");
        let written = std::fs::create_dir_all(&self.dir).and_then(|_| std::fs::write(&path, serde_json::to_vec_pretty(&spans).unwrap()));
        eprintln!("i143_acceptance_2805: the cell's {} spans -> {} (flush: {flushed:?}, write: {written:?})", spans.len(), path.display());
    }
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
        self.capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let spans = self.rows();
        crate::assert_own_story(spans.iter().map(|s| s["name"].as_str().unwrap_or("")));
        spans
    }

    /// Every span the exporter holds, as [`Capture::spans`] gives them, read without a flush.
    fn rows(&self) -> Vec<Value> {
        self.exporter
            .get_finished_spans()
            .unwrap_or_default()
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

/// A birth in the fixture: its record, the key it speaks with, and its one endpoint of that key,
/// bound on its first call and shared by every client it speaks through, so no two live endpoints
/// of the cell ever hold the same key.
struct Birth {
    node: Node,
    key: SecretKey,
    endpoint: tokio::sync::OnceCell<iroh::Endpoint>,
    caller: tokio::sync::OnceCell<(NodeRpcClient, Arc<StaticResolver>)>,
}

impl Birth {
    /// The birth's one endpoint.
    async fn endpoint(&self) -> iroh::Endpoint {
        self.endpoint.get_or_init(|| async { rdm_transport::endpoint::bind(self.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap() }).await.clone()
    }
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
    node.status = NodeStatus::Ready;
    node.is_primary = is_primary;
    node.is_fabric_primary = is_fabric_primary;
    node.transport_addr = Some("127.0.0.1:1".parse().unwrap());
    Birth { node, key, endpoint: tokio::sync::OnceCell::new(), caller: tokio::sync::OnceCell::new() }
}

/// One authority (`me`) serving Status (and core ping, so a non-status call can be refused while
/// connections-deleting) on its own endpoint, over a view holding `me` and `others`.
struct Rig {
    me: Birth,
    server: rdm_transport::NodeRpcServer,
    _router: Router,
    resolved: ResolvedNode,
    authority: Arc<StatusAuthority>,
    /// The router's ready gate: open unless a cell closes it.
    ready: Arc<std::sync::atomic::AtomicBool>,
}

async fn rig(me: Birth, others: &[&Birth], mesh_ids: BTreeMap<String, MeshId>) -> Rig {
    rig_with(me, others, mesh_ids, Refusing::memory(), Declared::default(), None).await
}

/// The rig over given mesh and fabric stores and a given starting `Declared` (what a restarted
/// authority folded from its facts).
async fn rig_with(me: Birth, others: &[&Birth], mesh_ids: BTreeMap<String, MeshId>, stores: Arc<Refusing>, declared: Declared, fabric: Option<rdm_core::FabricId>) -> Rig {
    let mut nodes = vec![me.node.clone()];
    nodes.extend(others.iter().map(|b| b.node.clone()));
    let topology = Arc::new(tokio::sync::RwLock::new(Topology {
        fabric: Fabric { id: fabric.unwrap_or_else(rdm_core::FabricId::mint), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes,
    }));
    let fabric_id = topology.read().await.fabric.id.clone();
    let authority = Arc::new(StatusAuthority {
        certs: None,
        app_channels: None,
        declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
        stopped: Default::default(),
        me: me.node.name.clone(),
        node_id: me.node.node_id.clone(),
        fabric_id,
        topology,
        declared: Arc::new(Mutex::new(declared)),
        meshes_storage: stores.clone(),
        fabric_storage: stores,
        nodes_storage: Arc::new(rdm_admin::storage::MemoryNodesStorage::default()),
        caches: Default::default(),
        mesh_ids: Arc::new(move || mesh_ids.clone()),
        durable: Default::default(),
        commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
        own: Arc::new(OnceLock::new()),
        mesh_deleter: Arc::new(OnceLock::new()),
        hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
        configs: Default::default(),
        republish: Default::default(), configure: Default::default(),
        pre_node_ready: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(),
        loads: None,
        digest_view: None,
        builds: std::sync::Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
    });
    let slot: Arc<OnceLock<Arc<StatusAuthority>>> = Arc::new(OnceLock::new());
    let _ = slot.set(authority.clone());
    let core = ServerBuilder::new().serve::<Ping, _, _>(OpOwner::Core, |_peer, req: PingRequest| async move {
        let PingRequest::Ping { payload } = req;
        Ok(PingReply::Pong { payload })
    });
    let ready = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let server = rdm_admin::status_rpc::serve(core, slot)
        .with_ready_gate(ready.clone())
        .build(ServedBirth { node_id: me.node.node_id.to_string(), incarnation: me.node.incarnation_id.clone().unwrap().to_string() })
        .unwrap();
    let ep = me.endpoint().await;
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server.clone()).spawn();
    let resolved = ResolvedNode {
        node_id: me.node.node_id.clone(),
        name: me.node.name.clone(),
        endpoint_id: me.key.public(),
        transport_addr: addr,
        incarnation: me.node.incarnation_id.clone().unwrap(),
    };
    Rig { me, server, _router: router, resolved, authority, ready }
}

/// `b`'s one client, over its one endpoint, resolving the rig's authority.
async fn client_of<'a>(rig: &Rig, b: &'a Birth) -> &'a NodeRpcClient {
    let (client, resolver) = b
        .caller
        .get_or_init(|| async {
            let resolver = Arc::new(StaticResolver::new());
            (NodeRpcClient::new(b.endpoint().await, resolver.clone()).with_caller_system("rdm"), resolver)
        })
        .await;
    resolver.insert(rig.resolved.clone());
    client
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
/// MeshId and a backward transition. Nothing checks who is calling (node-rpc.md §14): the same
/// Pending from a sender that is not the fabric-primary is decided for the mesh it names
/// (AlreadyApplied), and a DeclareNodeState sent by another node is decided for the birth it names.
#[test]
fn mesh_primary_retries_pending_by_natural_key_returns_already_applied() {
    let cell = "mesh_primary_retries_pending_by_natural_key_returns_already_applied";
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
    let rig = rig(me, &[&fabric_primary, &not_primary, &member, &other], mesh_ids).await;
    let pending = StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh2".into(), state: MeshState::Pending, changed_at_rafka_ms: 0 };

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
    let stale = StatusRequest::ApplyMeshState { mesh_id: MeshId::mint(), mesh_name: "mesh2".into(), state: MeshState::Pending, changed_at_rafka_ms: 0 };
    let stale_reply = reply(&call(&rig, &fabric_primary, &stale, &CallOptions::default()).await);
    assert_eq!(stale_reply, StatusReply::RejectedStaleMesh { held: mesh2.clone() });
    let wrong_sender = reply(&call(&rig, &not_primary, &pending, &CallOptions::default()).await);
    assert_eq!(wrong_sender, StatusReply::AlreadyApplied, "decided for the mesh it names, whoever sends it");
    let forward = StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh2".into(), state: MeshState::Ready, changed_at_rafka_ms: 0 };
    assert_eq!(reply(&call(&rig, &fabric_primary, &forward, &CallOptions::default()).await), StatusReply::Applied);
    let backward = reply(&call(&rig, &fabric_primary, &pending, &CallOptions::default()).await);
    assert!(matches!(backward, StatusReply::RejectedInvalidMeshTransition { current: MeshState::Ready }), "{backward:?}");
    let forged = StatusRequest::DeclareNodeState { node_id: other.node.node_id.clone(), incarnation: other.node.incarnation_id.clone().unwrap(), state: NodeState::Stopped, operation: None };
    let forged_reply = reply(&call(&rig, &member, &forged, &CallOptions::default()).await);
    assert_eq!(forged_reply, StatusReply::Applied, "a declaration is decided for the birth it names, whoever sends it");
    let never_born = birth("mesh2.rpc.9", NodeKind::RpcNode, "mesh2", false, false);
    let unknown = StatusRequest::DeclareNodeState { node_id: never_born.node.node_id.clone(), incarnation: never_born.node.incarnation_id.clone().unwrap(), state: NodeState::Stopped, operation: None };
    assert_eq!(reply(&call(&rig, &member, &unknown, &CallOptions::default()).await), StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "a birth this admin does not hold");
    let meshes_final = rig.authority.declared.lock().unwrap().meshes.clone();
    assert_eq!(meshes_final.len(), 1, "one mesh held, by its own id");

    let spans = capture.spans();
    let decisions: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.status.update.via-declaration").collect();
    assert!(decisions.iter().any(|s| s["attributes"]["op"] == "apply-mesh-state" && s["attributes"]["outcome"] == "already-applied"), "the retry is spanned as already-applied");
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

/// CONTRACT (#2805): after the exact birth's server deletes its connections, its status door stays admitted while
/// every other call is refused `ConnectionsDeleting`: a probe answers the current state. A node command
/// classifies as exactly one CommandAdmission arm and a refusal is never admitted: Refused (a
/// stale incarnation, `ApplyNodeState` as a connections delete), NotSent (cut before the send), Indeterminate
/// (the reply lost). The completion wait of an admitted command that never completes is the delete
/// pipeline's Deadline arm, never a success.
#[test]
fn connections_deleting_mesh_primary_serves_status_checks_exact_birth() {
    let cell = "connections_deleting_mesh_primary_serves_status_checks_exact_birth";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let capture = capture(cell);
    capture.run(connections_deleting_status_door(&capture, cell, &dir));
}

async fn connections_deleting_status_door(capture: &Capture, cell: &str, dir: &std::path::Path) {
    use rdm_admin::deployment::pipeline::{command_admission, CommandAdmission, Completion};

    // The subject: mesh1.admin.2, commanded by its owning mesh-admin mesh1.admin.1.
    let executor = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
    let subject = birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false);
    let (node_id, incarnation) = (subject.node.node_id.clone(), subject.node.incarnation_id.clone().unwrap());
    let rig = rig(subject, &[&executor], BTreeMap::new()).await;
    // The exact-birth Status command control is JoinMesh; StopNode is now reserved (Q47).
    // This cell still proves the admitted status door, its refusal and transport certainty arms.
    let command = |inc: IncarnationId| StatusRequest::JoinMesh { node_id: node_id.clone(), incarnation: inc, build_id: "8pe1w1tk1f0n".into(), attempt: 1, operation: "node.stop:mesh1.admin.2".into() };

    // Healthy control before connections-deleting: a ping is served.
    let c = client_of(&rig, &executor).await;
    let target = NodeTarget::ExactNode(node_id.clone());
    let (ping, _) = c.call::<Ping>(&target, &PingRequest::Ping { payload: b"before".to_vec() }, &CallOptions::default()).await;
    assert!(matches!(&ping, RpcOutcome::Reply(r) if matches!(r.value(), PingReply::Pong { .. })), "{ping:?}");

    // The server deletes its connections: every ordinary call is refused as ConnectionsDeleting, the status door stays admitted.
    rig.server.begin_connections_delete();
    let (refused_ping, _) = c.call::<Ping>(&target, &PingRequest::Ping { payload: b"after".to_vec() }, &CallOptions::default()).await;
    assert!(matches!(&refused_ping, RpcOutcome::Reply(r) if matches!(r.value(), PingReply::ConnectionsDeleting { .. })), "a non-status call is refused as ConnectionsDeleting: {refused_ping:?}");
    let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
    let probed = reply(&call(&rig, &executor, &probe, &CallOptions::default()).await);
    assert!(matches!(&probed, StatusReply::Current { node_id: n, .. } if *n == node_id), "{probed:?}");

    // ApplyNodeState is not a connections delete command: refused with the held state, never admitted.
    let apply = StatusRequest::ApplyNodeState { node_id: node_id.clone(), incarnation: incarnation.clone(), state: NodeState::ConnectionsDeleting };
    let applied = call(&rig, &executor, &apply, &CallOptions::default()).await;
    assert!(matches!(reply(&applied), StatusReply::RejectedInvalidNodeTransition { current: NodeState::Ready }), "{applied:?}");
    let apply_arm = command_admission(&applied);
    assert!(matches!(&apply_arm, CommandAdmission::Refused { reply , .. } if reply.contains("rejected-invalid-node-transition")), "{apply_arm:?}");

    // Refused: a stale incarnation, by name, never admitted.
    let stale_out = call(&rig, &executor, &command(IncarnationId::mint()), &CallOptions::default()).await;
    assert!(matches!(reply(&stale_out), StatusReply::RejectedStaleIncarnation { .. }), "{stale_out:?}");
    let refused = command_admission(&stale_out);
    assert!(matches!(&refused, CommandAdmission::Refused { reply , .. } if reply.contains("stale")), "{refused:?}");
    // NotSent: cut before the request's FIN.
    let (cut, _) = c.call::<Status>(&target, &command(incarnation.clone()), &CallOptions { cut_before_finish: true, ..Default::default() }).await;
    let not_sent = command_admission(&cut);
    assert!(matches!(not_sent, CommandAdmission::NotSent { .. }), "{not_sent:?}");
    // Indeterminate: decided, the reply lost.
    rig.authority.hold_next_reply.store(true, Ordering::SeqCst);
    let (lost, _) = c.call::<Status>(&target, &command(incarnation.clone()), &CallOptions { budget: Budget::Overall(Duration::from_millis(800)), ..Default::default() }).await;
    let indeterminate = command_admission(&lost);
    assert!(matches!(indeterminate, CommandAdmission::Indeterminate { .. }), "{indeterminate:?}");
    // Deadline: the delete pipeline's AwaitNodeLeft over an admitted command that never completes.
    let deadline = deadline_arm().await;
    assert_eq!(deadline, Completion::Deadline {});

    let spans = capture.spans();
    let commands = spans.iter().filter(|s| s["name"] == "rdm.node_admin.status.update.via-command").count();
    assert!(commands >= 2, "every node command naming the admin is spanned: {commands}");
    let result = json!({
        "cell": cell,
        "subject": node_id.to_string(),
        "ping_before": "pong",
        "ping_while_connections_deleting": "connections-deleting",
        "probe": format!("{probed:?}"),
        "arms": {
            "apply_node_state": format!("{apply_arm:?}"),
            "refused": format!("{refused:?}"),
            "not_sent": format!("{not_sent:?}"),
            "indeterminate": format!("{indeterminate:?}"),
            "deadline": format!("{deadline:?}"),
        },
        "server_connections_deleting_refusals": rig.server.stats().connections_deleting.load(Ordering::SeqCst),
        "command_spans": commands,
    });
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}

/// The delete pipeline run to its AwaitNodeLeft over a birth that admits the stop and never
/// completes it: the step's receipt is the arm.
async fn deadline_arm() -> rdm_admin::deployment::pipeline::Completion {
    use rdm_admin::build::BuildId;
    use rdm_admin::build_state::{BuildStateAdapter, MemoryBuildStateAdapter};
    use rdm_admin::deployment::pipeline::{DeploymentPipeline, LaunchTemplate, NodeObserver, NoLifecycleEvents, DeleteRequest, DeleteStep, Timeouts, TopologySink};
    use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};

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
        async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
            Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
        }
    }
    /// The birth admits every command and never calls a completion back.
    struct NeverConnectionsDeleted;
    #[async_trait::async_trait]
    impl NodeObserver for NeverConnectionsDeleted {
        async fn send_command(&self, _: &Node, _: rdm_admin::node_commands::NodeCommand, _: &rdm_admin::deployment::pipeline::CommandContext) -> rdm_admin::deployment::pipeline::CommandAdmission {
            rdm_admin::deployment::pipeline::CommandAdmission::Admitted {}
        }
        async fn await_completion(&self, _: &Node, _: rdm_admin::node_commands::NodeCommand, _: &rdm_admin::deployment::pipeline::CommandContext, within: Duration) -> rdm_admin::deployment::pipeline::Completion {
            tokio::time::sleep(within).await;
            rdm_admin::deployment::pipeline::Completion::Deadline {}
        }
        async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<rdm_admin::deployment::pipeline::Publication> {
            None
        }
        async fn ready(&self, _: &Node) -> Result<(), String> {
            Ok(())
        }
    }
    struct Discard;
    impl TopologySink for Discard {
        fn publish(&self, _: Node) {}
        fn remove(&self, _: &rdm_admin::model::PathName) {}
        fn remove_birth(&self, _: &rdm_admin::model::PathName, _: &rdm_core::NodeId, _: &rdm_core::IncarnationId) {}
    }

    // The drawing's first box: the operation is claimed under an accepted Build.
    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&rdm_admin::build_state::BuildAccepted {
            build_id: build_id.clone(),
            topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()),
            submitted_change: None,
            submitted_at_ms: 0,
        })
        .await
        .unwrap();
    builds.claim_attempt(&rdm_admin::build_state::BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let node = birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", false, false).node;
    let template = LaunchTemplate { fabric: "fabric1".into(),
        fabric_id: rdm_core::FabricId::mint(),
        executable: "/nonexistent/rshape-none".into(),
        seeds: vec![],
        launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: rdm_core::IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-2805-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-2805-{}", NodeId::mint())),
        issuing: None,
        roots: Default::default(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
        binding: None,
    };
    let joins = rdm_admin::join::Joins::default();
    let pipeline = DeploymentPipeline {
        provider: &Running,
        joins: &joins,
        observer: &NeverConnectionsDeleted,
        sink: &Discard,
        lifecycle: &NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { completion: Duration::from_millis(300), stop_grace: Duration::from_millis(300), ..Timeouts::default() },
    };
    let handle = DeploymentHandle {
        deployment_id: rdm_admin::model::DeploymentId::mint(),
        provider: ProviderKind::Process,
        pid: Some(1),
        start: Some(1),
        container: None,
        domain: Some("i143-2805".into()),
    };
    let req = DeleteRequest { build_id: build_id.clone(), attempt: 1, node, handle, unreached: false };
    let _ = pipeline.delete_node(&req).await;
    // The drawing's order, as far as this birth lets the delete go: NodeDeleting, the stop
    // command (StopNode), its completion wait (AwaitNodeLeft: the deadline arm); and never
    // NodeDeleted, because the provider never proves the runtime dead.
    let view = builds.read_build(&build_id).await.unwrap();
    let order: Vec<&str> = view.steps.iter().map(|s| s.step.as_str()).collect();
    let pos = |name: &str| order.iter().position(|s| *s == name);
    let (deleting, marked, waited) = (pos(DeleteStep::NodeDeleting.name()), pos(DeleteStep::StopNode.name()), pos(DeleteStep::AwaitNodeLeft.name()));
    assert!(deleting.is_some() && deleting < marked && marked < waited, "NodeDeleting, then the stop, then its result: {order:?}");
    assert!(pos(DeleteStep::NodeDeleted.name()).is_none(), "no NodeDeleted without the provider's proof of death: {order:?}");
    let step = &view.steps[waited.unwrap()];
    serde_json::from_value(step.output.clone().expect("AwaitNodeLeft records its arm")).unwrap()
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
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
    spans
}

/// How many connections the cell's authorities accepted from `b`'s endpoint.
fn accepts_from(spans: &[Value], b: &Birth) -> usize {
    let peer = b.key.public().fmt_short().to_string();
    spans.iter().filter(|s| s["name"] == "rdm.node_rpc.connection.create.via-accept" && s["attributes"]["peer"] == peer.as_str()).count()
}

fn cell_dir(cell: &str) -> PathBuf {
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A mesh and fabric store whose puts are refused while `refuse` is set, over real memory stores.
#[derive(Default)]
struct Refusing {
    meshes: MemoryMeshStorage,
    fabric: MemoryFabricStorage,
    refuse: std::sync::atomic::AtomicBool,
}

impl Refusing {
    fn memory() -> Arc<Self> {
        Arc::new(Self::default())
    }
    fn refusing() -> Arc<Self> {
        Arc::new(Self { refuse: std::sync::atomic::AtomicBool::new(true), ..Self::default() })
    }
    fn check(&self, file: &str) -> Result<(), rdm_admin::storage_error::StorageError> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(rdm_admin::storage_error::StorageError::Io { file: file.into(), reason: "the disk refused the put".into() });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl MeshStorage for Refusing {
    async fn mesh(&self, mesh_id: &MeshId) -> Result<Option<rdm_admin::storage::MeshRecord>, rdm_admin::storage_error::StorageError> {
        self.meshes.mesh(mesh_id).await
    }
    async fn mesh_named(&self, name: &str) -> Result<Option<rdm_admin::storage::MeshRecord>, rdm_admin::storage_error::StorageError> {
        self.meshes.mesh_named(name).await
    }
    async fn put_mesh(&self, record: &rdm_admin::storage::MeshRecord) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.check("meshes.jsonl")?;
        self.meshes.put_mesh(record).await
    }
    async fn meshes(&self) -> Result<Vec<rdm_admin::storage::MeshRecord>, rdm_admin::storage_error::StorageError> {
        self.meshes.meshes().await
    }
    async fn claim_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<Result<(), MeshId>, rdm_admin::storage_error::StorageError> {
        self.meshes.claim_mesh_name(name, mesh_id).await
    }
    async fn release_mesh_name(&self, name: &str, mesh_id: &MeshId) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.meshes.release_mesh_name(name, mesh_id).await
    }
    async fn take_in_flight(&self, mesh: &str, claim: &rdm_admin::mesh_claims::InFlight) -> Result<rdm_admin::mesh_claims::Taken, rdm_admin::storage_error::StorageError> {
        self.meshes.take_in_flight(mesh, claim).await
    }
    async fn release_in_flight(&self, mesh: &str, attempt: &rdm_admin::mesh_claims::InFlight) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.meshes.release_in_flight(mesh, attempt).await
    }
    async fn in_flight(&self, mesh: &str) -> Result<Option<rdm_admin::mesh_claims::InFlight>, rdm_admin::storage_error::StorageError> {
        self.meshes.in_flight(mesh).await
    }
}

#[async_trait::async_trait]
impl FabricStorage for Refusing {
    async fn fabric(&self) -> Result<Option<rdm_admin::fabric_storage::FabricRecord>, rdm_admin::storage_error::StorageError> {
        self.fabric.fabric().await
    }
    async fn put_fabric(&self, record: &rdm_admin::fabric_storage::FabricRecord) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.check("fabric.jsonl")?;
        self.fabric.put_fabric(record).await
    }
    async fn name_claim(&self, name: &str) -> Result<Option<rdm_admin::fabric_storage::FabricIdentity>, rdm_admin::storage_error::StorageError> {
        self.fabric.name_claim(name).await
    }
    async fn put_name_claim(&self, identity: &rdm_admin::fabric_storage::FabricIdentity) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.fabric.put_name_claim(identity).await
    }
    async fn remove_name_claim(&self, name: &str) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.fabric.remove_name_claim(name).await
    }
}

/// CONTRACT (#2805, acceptance 9): a Mesh status or Fabric event is answered `Applied` only once its
/// own keyed fact is durable. A refused put of a Mesh status answers NotReady naming mesh.storage and
/// holds nothing, so the same declaration again is decided afresh and applies (it is never
/// AlreadyApplied over a fact nothing recorded). A Fabric event at a mesh-primary that is not the
/// fabric-primary is the fabric-primary's to put (states.md, "Who sets each state"): until the fabric
/// holds it the apply answers NotReady naming fabric.storage, and once it does, Applied. An authority
/// restarted over the same facts folds them and answers the same natural keys AlreadyApplied. What
/// must NOT happen: an Applied or AlreadyApplied for a fact that is not durable.
#[test]
fn mesh_primary_answers_applied_only_after_its_row_is_acknowledged() {
    let cell = "mesh_primary_answers_applied_only_after_its_row_is_acknowledged";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        let fabric_primary = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let mesh2 = MeshId::mint();
        let fabric = rdm_core::FabricId::mint();
        let ids: BTreeMap<String, MeshId> = [("mesh2".to_string(), mesh2.clone())].into_iter().collect();
        let store = Refusing::refusing();
        let me = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let rig = rig_with(me, &[&fabric_primary], ids.clone(), store.clone(), Declared::default(), Some(fabric.clone())).await;
        let pending = StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh2".into(), state: MeshState::Pending, changed_at_rafka_ms: 0 };

        let refused = reply(&call(&rig, &fabric_primary, &pending, &CallOptions::default()).await);
        assert!(matches!(&refused, StatusReply::NotReady { reason } if reason.contains("mesh.storage refused the record") && reason.contains("the disk refused the put")), "{refused:?}");
        assert!(rig.authority.declared.lock().unwrap().meshes.is_empty(), "a refused row holds nothing as applied");
        assert!(store.meshes.meshes().await.unwrap().into_iter().filter_map(|r| Some((r.mesh_id.clone(), r.state()?))).collect::<std::collections::HashMap<_, _>>().is_empty());

        store.refuse.store(false, Ordering::SeqCst);
        let first = reply(&call(&rig, &fabric_primary, &pending, &CallOptions::default()).await);
        assert_eq!(first, StatusReply::Applied, "the retry of a refused put applies; it is not AlreadyApplied");
        assert_eq!(store.meshes.meshes().await.unwrap().into_iter().filter_map(|r| Some((r.mesh_id.clone(), r.state()?))).collect::<std::collections::HashMap<_, _>>().get(&mesh2), Some(&MeshState::Pending), "the row was put before the answer");
        let again = reply(&call(&rig, &fabric_primary, &pending, &CallOptions::default()).await);
        assert_eq!(again, StatusReply::AlreadyApplied);

        // A Fabric event: refused, then applied, then a repeat.
        let mesh1_primary = birth("mesh3.admin.1", NodeKind::NodeAdmin, "mesh3", true, false);
        let rig3 = rig_with(mesh1_primary, &[&fabric_primary], BTreeMap::new(), store.clone(), Declared::default(), Some(fabric.clone())).await;
        let event = StatusRequest::ApplyFabricEvent { fabric_id: fabric.clone(), event: rdm_core::status::FabricEvent::Ready };
        let ev_refused = reply(&call(&rig3, &fabric_primary, &event, &CallOptions::default()).await);
        assert!(matches!(&ev_refused, StatusReply::NotReady { reason } if reason.contains("fabric.storage does not hold the Fabric event")), "{ev_refused:?}");
        assert!(rig3.authority.declared.lock().unwrap().fabric.is_empty());
        // The fabric-primary puts the fact on the fabric, then applies it.
        { let mut r = store.fabric.fabric().await.unwrap().unwrap_or(rdm_admin::fabric_storage::FabricRecord { fabric_id: fabric.clone(), name: "fabric1".into(), events: Vec::new(), provider: None, topology_storage_provider: None, previous_topology_storage_providers: Vec::new(), ca: None, roots: None}); r.events.push("ready".into()); store.fabric.put_fabric(&r).await.unwrap(); }
        assert_eq!(reply(&call(&rig3, &fabric_primary, &event, &CallOptions::default()).await), StatusReply::Applied);

        // The authority restarts over the same facts: what it folds is what it applied.
        let folded = folded(&rdm_admin::storage::MemoryNodesStorage::default(), &*store, &*store, "mesh2").await;
        assert_eq!(folded.meshes.get(&mesh2), Some(&MeshState::Pending));
        let me_again = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let restarted = rig_with(me_again, &[&fabric_primary], ids, store.clone(), folded, Some(fabric.clone())).await;
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
/// forward applies. Each sender speaks from its one endpoint, so its calls ride one connection.
/// What must NOT happen: a backward move applied, a repeat answered anything but AlreadyApplied, or
/// a call from a fresh endpoint of a key another live endpoint holds.
#[test]
fn mesh_primary_refuses_every_backward_move_by_name() {
    let cell = "mesh_primary_refuses_every_backward_move_by_name";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        let nodes = [NodeState::Pending, NodeState::Ready, NodeState::ConnectionsDeleting, NodeState::Stopped];
        let meshes = [MeshState::Pending, MeshState::Ready, MeshState::Leaving, MeshState::Dead];
        let me = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let senders: Vec<Birth> = (0..4).map(|i| birth(&format!("mesh1.rpc.{}", i + 1), NodeKind::RpcNode, "mesh1", false, false)).collect();
        let fabric_primary = birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false);
        let receiver = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let node_rig = rig(me, &senders.iter().collect::<Vec<_>>(), BTreeMap::new()).await;
        let mut fp = fabric_primary;
        fp.node.is_fabric_primary = true;
        let mesh_rig = rig(receiver, &[&fp], BTreeMap::new()).await;
        let mut refused_node = 0;
        let mut refused_mesh = 0;
        let mut repeats = 0;
        for (i, start) in nodes.iter().enumerate() {
            let s = &senders[i];
            let decl = |st: NodeState| StatusRequest::DeclareNodeState { node_id: s.node.node_id.clone(), incarnation: s.node.incarnation_id.clone().unwrap(), state: st, operation: None };
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
            assert_eq!(node_rig.authority.declared.lock().unwrap().node(&s.node.node_id, s.node.incarnation_id.as_ref().unwrap()), Some(*start), "refusals moved nothing");
        }
        for start in meshes {
            let id = MeshId::mint();
            let apply = |st: MeshState| StatusRequest::ApplyMeshState { mesh_id: id.clone(), mesh_name: "mesh2".into(), state: st, changed_at_rafka_ms: 0 };
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
        let to_dead = StatusRequest::ApplyMeshState { mesh_id: skip.clone(), mesh_name: "mesh2".into(), state: MeshState::Dead, changed_at_rafka_ms: 0 };
        assert_eq!(reply(&call(&mesh_rig, &fp, &to_dead, &CallOptions::default()).await), StatusReply::Applied, "an unseen Mesh may be told Dead: a skip is forward");
        // A birth speaks from its one endpoint: each sender's calls rode one connection, never a fresh
        // endpoint of the same key per call.
        let accepted = capture.spans();
        for b in senders.iter().chain([&fp]) {
            assert_eq!(accepts_from(&accepted, b), 1, "{}: every call of the birth rode the one connection of its one endpoint", b.node.name);
        }

        let spans = finish(&capture, &dir, json!({"cell": cell, "refused_node_moves": refused_node, "refused_mesh_moves": refused_mesh, "repeats_already_applied": repeats}));
        assert_eq!(refused_node, 6, "0+1+2+3 backward pairs");
        assert_eq!(refused_mesh, 6);
        assert_eq!(spans_where(&spans, DECLARATION, &[("outcome", "rejected-invalid-node-transition")]).len(), refused_node);
        assert_eq!(spans_where(&spans, DECLARATION, &[("outcome", "rejected-invalid-mesh-transition")]).len(), refused_mesh);
        assert_eq!(spans_where(&spans, DECLARATION, &[("outcome", "already-applied")]).len(), repeats);
    });
}

/// CONTRACT (#2805, adversary): a declaration to a receiver that does not hold the seat it needs
/// is refused naming the seat, whoever sends it (nothing checks who is calling, node-rpc.md §14); a
/// node declaration to an admin that is no primary is refused naming the role it lacks before any
/// subject is looked up, a subject no view holds included (rust-distributed-mesh#99);
/// a stale Fabric id and a stale Mesh id are refused naming the id the receiver holds. What must
/// NOT happen: any of these applied, or answered as a transport failure.
#[test]
fn status_declaration_to_a_secondary_is_refused_naming_the_mesh_primary() {
    let cell = "status_declaration_to_a_secondary_is_refused_naming_the_mesh_primary";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        // The receiver: a mesh1 admin that holds no seat.
        let fp = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let rpc = birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", false, false);
        let other_admin = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let stranger = birth("mesh9.rpc.1", NodeKind::RpcNode, "mesh9", false, false);
        let receiver = birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false);
        let rig = rig(receiver, &[&fp, &rpc, &other_admin], BTreeMap::new()).await;
        let not_primary = |r: &StatusReply, name: &str| match r {
            StatusReply::RejectedNotPrimary { why } => assert_eq!(why.as_str(), name, "{r:?}"),
            other => panic!("expected rejected-not-primary/{name}: {other:?}"),
        };
        let decl_node = |b: &Birth| StatusRequest::DeclareNodeState { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), state: NodeState::Ready, operation: None };
        // An ordinary node declares to a non-primary admin.
        let r = reply(&call(&rig, &rpc, &decl_node(&rpc), &CallOptions::default()).await);
        not_primary(&r, "receiver-not-primary");
        assert!(matches!(&r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed == "mesh-primary of mesh1"), "{r:?}");
        // A node-admin declares to an admin that is not its mesh's primary.
        let r = reply(&call(&rig, &other_admin, &decl_node(&other_admin), &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed == "mesh-primary of mesh2"), "{r:?}");
        // A Mesh primary declares its Mesh to a non-fabric-primary.
        let mesh2 = MeshId::mint();
        let r = reply(&call(&rig, &other_admin, &StatusRequest::DeclareMeshState { mesh_id: mesh2.clone(), state: MeshState::Ready, changed_at_rafka_ms: 0 }, &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed == "fabric-primary"), "{r:?}");
        // The fabric-primary applies an event at an admin that is not a mesh primary.
        let event = StatusRequest::ApplyFabricEvent { fabric_id: rig.authority.fabric_id.clone(), event: rdm_core::status::FabricEvent::Ready };
        let r = reply(&call(&rig, &fp, &event, &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed == "mesh-primary"), "{r:?}");
        // The fabric-primary applies a Mesh state at an admin of another mesh.
        let r = reply(&call(&rig, &fp, &StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh2".into(), state: MeshState::Pending, changed_at_rafka_ms: 0 }, &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed == "an admin of mesh2"), "{r:?}");
        // The same applies from a sender that is not the fabric-primary are refused for the
        // receiver, never the caller (node-rpc.md §14).
        let r = reply(&call(&rig, &other_admin, &event, &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed == "mesh-primary"), "{r:?}");
        let r = reply(&call(&rig, &rpc, &StatusRequest::ApplyMeshState { mesh_id: mesh2.clone(), mesh_name: "mesh1".into(), state: MeshState::Pending, changed_at_rafka_ms: 0 }, &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed.starts_with("the mesh-primary of mesh1")), "{r:?}");
        // A subject the view does not hold, from a stranger the view does not hold either: the
        // receiver, no primary, says so before it looks for the subject.
        let r = reply(&call(&rig, &stranger, &decl_node(&stranger), &CallOptions::default()).await);
        assert!(matches!(&r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed == "mesh-primary of mesh1"), "{r:?}");
        // Stale ids: another Fabric's event at the mesh primary; another Mesh id for a held name.
        let mp_receiver = birth("mesh1.admin.3", NodeKind::NodeAdmin, "mesh1", true, false);
        let held = MeshId::mint();
        let ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), held.clone())].into_iter().collect();
        let rig2 = rig_with(mp_receiver, &[&fp], ids, Refusing::memory(), Declared::default(), None).await;
        let wrong_fabric = rdm_core::FabricId::mint();
        let r = reply(&call(&rig2, &fp, &StatusRequest::ApplyFabricEvent { fabric_id: wrong_fabric, event: rdm_core::status::FabricEvent::Ready }, &CallOptions::default()).await);
        assert_eq!(r, StatusReply::RejectedStaleFabric { held: rig2.authority.fabric_id.clone() });
        let r = reply(&call(&rig2, &fp, &StatusRequest::ApplyMeshState { mesh_id: MeshId::mint(), mesh_name: "mesh1".into(), state: MeshState::Pending, changed_at_rafka_ms: 0 }, &CallOptions::default()).await);
        assert_eq!(r, StatusReply::RejectedStaleMesh { held });
        assert!(rig.authority.declared.lock().unwrap().nodes.is_empty() && rig.authority.declared.lock().unwrap().meshes.is_empty() && rig.authority.declared.lock().unwrap().fabric.is_empty(), "nothing applied by any refusal");

        let spans = finish(&capture, &dir, json!({"cell": cell, "refusals": 10}));
        for outcome in ["rejected-not-primary", "rejected-stale-fabric", "rejected-stale-mesh"] {
            assert!(!spans_where(&spans, DECLARATION, &[("outcome", outcome)]).is_empty(), "{outcome} is spanned");
        }
        assert_eq!(spans_where(&spans, DECLARATION, &[("outcome", "rejected-not-primary")]).len(), 8);
        assert!(!spans_where(&spans, DECLARATION, &[("outcome", "rejected-not-primary"), ("detail", "receiver-not-primary")]).is_empty());
    });
}

/// CONTRACT (#2805, acceptance 10): an event applied by the fabric-primary stays applied when the
/// seat moves. Each event applies once at the mesh primary on its natural key, whoever sends it
/// (node-rpc.md §14: nothing inside the mesh checks who is calling; the target that stops an old
/// fabric-primary is its own moved seat record, at the sender): the old authority's repeat of an
/// applied key and the new authority's are AlreadyApplied, a new event applies once, and a
/// receiver restarted over its rows still answers both AlreadyApplied. What must NOT happen: an
/// applied event applied twice, or a refusal naming the caller.
#[test]
fn applied_events_survive_a_mesh_primary_change_and_each_applies_once_whoever_sends_it() {
    let cell = "applied_events_survive_a_mesh_primary_change_and_each_applies_once_whoever_sends_it";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        use rdm_core::status::FabricEvent;
        let fp1 = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let fp2 = birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false);
        let receiver = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let fabric = rdm_core::FabricId::mint();
        let store = Refusing::memory();
        let rig = rig_with(receiver, &[&fp1, &fp2], BTreeMap::new(), store.clone(), Declared::default(), Some(fabric.clone())).await;
        let ready = StatusRequest::ApplyFabricEvent { fabric_id: fabric.clone(), event: FabricEvent::Ready };
        // Each event is the fabric-primary's fact on the fabric before it applies it at a mesh-primary.
        { let mut r = store.fabric.fabric().await.unwrap().unwrap_or(rdm_admin::fabric_storage::FabricRecord { fabric_id: fabric.clone(), name: "fabric1".into(), events: Vec::new(), provider: None, topology_storage_provider: None, previous_topology_storage_providers: Vec::new(), ca: None, roots: None}); r.events.push("ready".into()); store.fabric.put_fabric(&r).await.unwrap(); }
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
        { let mut r = store.fabric.fabric().await.unwrap().unwrap_or(rdm_admin::fabric_storage::FabricRecord { fabric_id: fabric.clone(), name: "fabric1".into(), events: Vec::new(), provider: None, topology_storage_provider: None, previous_topology_storage_providers: Vec::new(), ca: None, roots: None}); r.events.push("shutdown-initiated:mesh1.admin.1".into()); store.fabric.put_fabric(&r).await.unwrap(); }
        let old = reply(&call(&rig, &fp1, &ready, &CallOptions::default()).await);
        assert_eq!(old, StatusReply::AlreadyApplied, "the old authority's repeat of an applied key is the same event");
        assert_eq!(reply(&call(&rig, &fp2, &ready, &CallOptions::default()).await), StatusReply::AlreadyApplied, "previously applied events survive the move");
        assert_eq!(reply(&call(&rig, &fp2, &shutdown, &CallOptions::default()).await), StatusReply::Applied, "a new event applies");
        assert_eq!(reply(&call(&rig, &fp1, &shutdown, &CallOptions::default()).await), StatusReply::AlreadyApplied, "once, whoever sends it");
        // The receiver restarts over the fabric's facts.
        let folded = folded(&rdm_admin::storage::MemoryNodesStorage::default(), &*store, &*store, "mesh2").await;
        let receiver2 = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false);
        let rig2 = rig_with(receiver2, &[&fp1, &fp2], BTreeMap::new(), store.clone(), folded, Some(fabric.clone())).await;
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
        let spans = finish(&capture, &dir, json!({"cell": cell, "old_authority": format!("{old:?}"), "events_held": store.fabric().await.unwrap().map_or(0, |r| r.events.len())}));
        assert_eq!(store.fabric().await.unwrap().map_or(0, |r| r.events.len()), 2, "each event is on the Fabric record once");
        assert_eq!(spans_where(&spans, DECLARATION, &[("op", "apply-fabric-event"), ("outcome", "applied")]).len(), 2);
        assert!(spans_where(&spans, DECLARATION, &[("op", "apply-fabric-event"), ("outcome", "rejected-not-primary")]).is_empty(), "no event refused for its caller");
        assert_eq!(spans_where(&spans, DECLARATION, &[("op", "apply-fabric-event"), ("outcome", "already-applied")]).len(), 5);
    });
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
        let rig = rig(me, &[&subject], BTreeMap::new()).await;
        let old_inc = subject.node.incarnation_id.clone().unwrap();
        let decl = |inc: &IncarnationId, st: NodeState| StatusRequest::DeclareNodeState { node_id: subject.node.node_id.clone(), incarnation: inc.clone(), state: st, operation: None };
        assert_eq!(reply(&call(&rig, &subject, &decl(&old_inc, NodeState::ConnectionsDeleting), &CallOptions::default()).await), StatusReply::Applied, "the old birth reached connections-deleting");
        // The restart: same node and key, a new incarnation, now what the view holds.
        let new_inc = IncarnationId::mint();
        {
            let mut t = rig.authority.topology.write().await;
            t.nodes.iter_mut().find(|n| n.node_id == subject.node.node_id).unwrap().incarnation_id = Some(new_inc.clone());
        }
        let in_flight = reply(&call(&rig, &subject, &decl(&old_inc, NodeState::Stopped), &CallOptions::default()).await);
        assert_eq!(in_flight, StatusReply::RejectedStaleIncarnation { held: new_inc.clone() });
        assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node.node_id, &old_inc), Some(NodeState::ConnectionsDeleting), "the stale declaration moved nothing");
        let fresh = reply(&call(&rig, &subject, &decl(&new_inc, NodeState::Pending), &CallOptions::default()).await);
        assert_eq!(fresh, StatusReply::Applied, "the new birth starts on a clean key: Pending after the old birth's connections-deleting is not backward");
        assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node.node_id, &new_inc), Some(NodeState::Pending));
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
        let rig = rig(me, &[&subject], BTreeMap::new()).await;
        let decl = StatusRequest::DeclareNodeState { node_id: subject.node.node_id.clone(), incarnation: subject.node.incarnation_id.clone().unwrap(), state: NodeState::Ready, operation: None };
        // The healthy control, in the same capture.
        assert_eq!(reply(&call(&rig, &subject, &decl, &CallOptions::default()).await), StatusReply::Applied);
        let served_before = spans_where(&capture.spans(), DECLARATION, &[]).len();
        // No route: a client whose resolver holds nothing for the target.
        let routeless = NodeRpcClient::new(subject.endpoint().await, Arc::new(StaticResolver::new())).with_caller_system("rdm");
        let to = NodeTarget::ExactNode(rig.me.node.node_id.clone());
        let again = StatusRequest::DeclareNodeState { node_id: subject.node.node_id.clone(), incarnation: subject.node.incarnation_id.clone().unwrap(), state: NodeState::Stopped, operation: None };
        let (out, _) = routeless.call::<Status>(&to, &again, &CallOptions::default()).await;
        assert!(matches!(out, RpcOutcome::NotSent(_)), "no route is NotSent, nothing dispatched: {out:?}");
        assert!(out.proves_not_dispatched());
        let t = rig.authority.topology.read().await;
        assert!(t.nodes.iter().all(|n| n.status == NodeStatus::Ready), "no node became Dead or Leaving: {:?}", t.nodes.iter().map(|n| (n.name.to_string(), n.status)).collect::<Vec<_>>());
        drop(t);
        assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node.node_id, subject.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready), "the applied state is unchanged");
        let spans = finish(&capture, &dir, json!({"cell": cell, "no_route": out.name(), "declaration_spans_before": served_before}));
        assert_eq!(spans_where(&spans, DECLARATION, &[]).len(), served_before, "the unsent call reached no handler");
        assert!(spans.iter().all(|s| !s["name"].as_str().unwrap_or("").contains("dead")), "no span names a death");
    });
}

/// CONTRACT (#2805, acceptances 6 and 7): the fabric-primary's Pending hand-off at a mesh's first
/// admin returns Ok only when the target answered Applied or AlreadyApplied. A target holding
/// another MeshId refuses by name and no replacement id is minted; a target already past Pending
/// refuses the backward move; a target that cannot be reached (NotSent every attempt) ends in an
/// error saying Pending is not certain, with nothing applied at the target; and an Applied whose
/// reply was lost (Indeterminate) is resolved by the same natural key answering AlreadyApplied.
/// The create pipeline runs this as its ApplyMeshPending step and `?`s its error, so a non-Ok
/// ends the birth before the shape proceeds. Every attempt is one pending-handoff span naming
/// the target birth, the MeshId, the natural key and the outcome. This cell takes about half a
/// minute: the Indeterminate arm waits the call's 10 s budget and the unreachable arm the
/// hand-off's own 20 s reachability bound.
#[test]
fn pending_handoff_ends_ok_only_on_certainty_and_otherwise_stops_the_shape() {
    let cell = "pending_handoff_ends_ok_only_on_certainty_and_otherwise_stops_the_shape";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        let fp = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let handoff = |rig: &Rig, mesh_id: MeshId, target: Node| {
            let (topology, me, me_id) = (rig.authority.topology.clone(), fp.node.name.clone(), fp.node.node_id.clone());
            let fp = &fp;
            async move {
                let ep = fp.endpoint().await;
                let resolver = Arc::new(rdm_transport::LiveNodeResolver::default());
                let client = NodeRpcClient::new(ep, resolver.clone()).with_caller_system("rdm");
                rdm_admin::admin::apply_mesh_state(&client, &resolver, &me, &me_id, &topology, "mesh2", &mesh_id.to_string(), rdm_core::status::MeshState::Pending, 0, &target).await
            }
        };
        let target_of = |rig: &Rig| {
            let mut n = rig.me.node.clone();
            n.transport_addr = Some(rig.resolved.transport_addr);
            n
        };

        // Arm 1: the target holds another MeshId under mesh2's name.
        let held = MeshId::mint();
        let ids: BTreeMap<String, MeshId> = [("mesh2".to_string(), held.clone())].into_iter().collect();
        let r1 = rig(birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false), &[&fp], ids).await;
        let other = MeshId::mint();
        let e1 = handoff(&r1, other, target_of(&r1)).await.expect_err("a stale MeshId is refused");
        assert!(e1.contains("no replacement id is minted") && e1.contains(&held.to_string()), "{e1}");
        assert!(r1.authority.declared.lock().unwrap().meshes.is_empty(), "nothing applied under the wrong id");

        // Arm 2: the target is already past Pending.
        let mesh_id = MeshId::mint();
        let ids: BTreeMap<String, MeshId> = [("mesh2".to_string(), mesh_id.clone())].into_iter().collect();
        let r2 = rig(birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false), &[&fp], ids).await;
        let ahead = StatusRequest::ApplyMeshState { mesh_id: mesh_id.clone(), mesh_name: "mesh2".into(), state: MeshState::Ready, changed_at_rafka_ms: 0 };
        assert_eq!(reply(&call(&r2, &fp, &ahead, &CallOptions::default()).await), StatusReply::Applied);
        let e2 = handoff(&r2, mesh_id.clone(), target_of(&r2)).await.expect_err("Pending after Ready is backward");
        assert!(e2.contains("Pending is a backward move") && e2.contains("Ready"), "{e2}");

        // Arm 3: applied, the reply lost. The hand-off makes ONE call: Indeterminate fails the step
        // typed (the rectifier is the retry), and the next hand-off of the same key is AlreadyApplied.
        let mesh_id3 = MeshId::mint();
        let ids: BTreeMap<String, MeshId> = [("mesh2".to_string(), mesh_id3.clone())].into_iter().collect();
        let r3 = rig(birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false), &[&fp], ids).await;
        r3.authority.hold_next_reply.store(true, Ordering::SeqCst);
        let t3 = std::time::Instant::now();
        let e3 = handoff(&r3, mesh_id3.clone(), target_of(&r3)).await.expect_err("a lost reply is not certainty: the step fails and is not retried inside");
        let lost_reply_wall = t3.elapsed();
        assert!(e3.contains("Indeterminate") && e3.contains("Pending is not certain"), "{e3}");
        assert_eq!(r3.authority.declared.lock().unwrap().meshes.get(&mesh_id3), Some(&MeshState::Pending), "applied once");
        handoff(&r3, mesh_id3.clone(), target_of(&r3)).await.expect("the rectifier's next hand-off of the same key is AlreadyApplied");

        // Arm 4: the target cannot be reached at all: the reachability wait ends at its bound and
        // no Status is sent.
        let mesh_id4 = MeshId::mint();
        let r4 = rig(birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false), &[&fp], BTreeMap::new()).await;
        let mut dark = target_of(&r4);
        dark.transport_addr = Some("127.0.0.1:1".parse().unwrap());
        let t4 = std::time::Instant::now();
        let e4 = handoff(&r4, mesh_id4.clone(), dark).await.expect_err("an unreachable target is never certain");
        let dark_wall = t4.elapsed();
        assert!(e4.contains("unreachable for 20 s") && e4.contains("nothing was sent"), "{e4}");
        assert!(r4.authority.declared.lock().unwrap().meshes.is_empty(), "nothing applied at an unreached target");

        let spans = finish(
            &capture,
            &dir,
            json!({"cell": cell, "stale_mesh": e1, "backward": e2, "lost_reply": e3, "lost_reply_wall_ms": lost_reply_wall.as_millis() as u64, "unreachable": e4, "unreachable_wall_ms": dark_wall.as_millis() as u64}),
        );
        let hand = |outcome_prefix: &str, mesh_id: &MeshId| {
            spans
                .iter()
                .filter(|s| s["name"] == "rdm.node_admin.mesh.update.via-pending-handoff" && s["attributes"]["mesh_id"] == mesh_id.to_string().as_str() && s["attributes"]["outcome"].as_str().is_some_and(|o| o.starts_with(outcome_prefix)))
                .count()
        };
        assert_eq!(hand("rejected-invalid-mesh-transition", &mesh_id), 1, "one call, one span");
        assert_eq!(hand("Indeterminate", &mesh_id3), 1, "the lost reply is spanned as Indeterminate, once");
        assert_eq!(hand("already-applied", &mesh_id3), 1, "and the next hand-off of the same key resolves it");
        assert_eq!(hand("", &mesh_id4), 0, "an unreachable target is sent no Status at all");
        let reach: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.mesh.update.via-pending-reachability").collect();
        assert!(reach.iter().any(|s| s["attributes"]["reachable"].to_string().contains("false")), "the unreachable wait is spanned");
    });
}

/// CONTRACT (R-S2): the Pending hand-off waits for the exact launched birth to be reachable with
/// a Ping and sends no Status meanwhile. A node whose router is up but whose Status authority is
/// not filled refuses even the Ping with a typed NotReady, so the wait does not pass early; once
/// the authority is filled the wait ends and exactly one Status call applies Pending. What must
/// NOT happen: a Status call, or an Applied, while the gate is closed.
#[test]
fn pending_handoff_waits_for_an_elected_mesh_primary_before_its_one_call() {
    let cell = "pending_handoff_waits_for_an_elected_mesh_primary_before_its_one_call";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        let fp = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let mesh_id = MeshId::mint();
        let ids: BTreeMap<String, MeshId> = [("mesh2".to_string(), mesh_id.clone())].into_iter().collect();
        let r = rig(birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false), &[&fp], ids).await;
        r.ready.store(false, Ordering::SeqCst);
        let mut target = r.me.node.clone();
        target.transport_addr = Some(r.resolved.transport_addr);
        let (topology, me, me_id) = (r.authority.topology.clone(), fp.node.name.clone(), fp.node.node_id.clone());
        let resolver = Arc::new(rdm_transport::LiveNodeResolver::default());
        let client = NodeRpcClient::new(fp.endpoint().await, resolver.clone()).with_caller_system("rdm");
        let gate = r.ready.clone();
        let opener = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(1200)).await;
            gate.store(true, Ordering::SeqCst);
        });
        let started = std::time::Instant::now();
        rdm_admin::admin::apply_mesh_state(&client, &resolver, &me, &me_id, &topology, "mesh2", &mesh_id.to_string(), rdm_core::status::MeshState::Pending, 0, &target).await.expect("Pending applies once the authority is filled");
        let waited = started.elapsed();
        opener.await.unwrap();
        assert!(waited >= Duration::from_millis(1000), "the hand-off did not pass while the gate was closed: {waited:?}");
        assert_eq!(r.authority.declared.lock().unwrap().meshes.get(&mesh_id), Some(&MeshState::Pending));
        let spans = finish(&capture, &dir, json!({"cell": cell, "waited_ms": waited.as_millis() as u64}));
        let reach = spans_where(&spans, "rdm.node_admin.mesh.update.via-pending-reachability", &[]);
        assert_eq!(reach.len(), 1);
        assert!(reach[0]["attributes"]["pings"].to_string().trim_matches('"').parse::<u64>().unwrap() > 1, "NotReady pings were refused before the Pong: {}", reach[0]["attributes"]);
        assert!(reach[0]["attributes"]["last"].to_string().contains("Pong"), "the wait ended on a Pong");
        let hand = spans_where(&spans, "rdm.node_admin.mesh.update.via-pending-handoff", &[]);
        assert_eq!(hand.len(), 1, "exactly one Status call");
        assert!(spans.iter().all(|s| !s["attributes"]["outcome"].to_string().contains("NotReady")), "no Status call was answered NotReady");
    });
}

/// CONTRACT (#2805, acceptance 10 and the elected-primary legs of 4 and 5): the fabric primary is
/// the admin with the lowest node id fabric-wide, so the seat moves to a newly born admin of
/// another mesh. That new holder validates a Mesh declaration against the Mesh ids it hears in its
/// view, not only against the ones its own Build created: a Mesh it never created is applied,
/// whoever sends the declaration (node-rpc.md §14), and a Mesh id it holds neither way is refused
/// `SubjectUnknown`. What must NOT happen: a perpetual refusal for a Mesh the new holder only heard
/// of, or a refusal naming the caller.
#[test]
fn new_fabric_primary_applies_a_mesh_declaration_for_a_mesh_it_only_heard_of() {
    let cell = "new_fabric_primary_applies_a_mesh_declaration_for_a_mesh_it_only_heard_of";
    let dir = cell_dir(cell);
    let capture = capture(cell);
    capture.run(async {
        let me = birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, true);
        let mesh1_primary = birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", true, false);
        let mesh1_other = birth("mesh1.admin.3", NodeKind::NodeAdmin, "mesh1", false, false);
        let (id1, id2) = (MeshId::mint(), MeshId::mint());
        // Its own records hold only the Mesh it was born into.
        let records: BTreeMap<String, MeshId> = [("mesh2".to_string(), id2.clone())].into_iter().collect();
        let rig = rig(me, &[&mesh1_primary, &mesh1_other], records).await;
        {
            use rdm_admin::model::Mesh;
            let mut t = rig.authority.topology.write().await;
            t.meshes = vec![
                Mesh { id: Some(id1.clone()), name: "mesh1".into(), status: ScopeStatus::Ready },
                Mesh { id: Some(id2.clone()), name: "mesh2".into(), status: ScopeStatus::Ready },
            ];
        }
        let declare = |id: &MeshId| StatusRequest::DeclareMeshState { mesh_id: id.clone(), state: MeshState::Ready, changed_at_rafka_ms: 0 };
        let applied = reply(&call(&rig, &mesh1_primary, &declare(&id1), &CallOptions::default()).await);
        assert_eq!(applied, StatusReply::Applied, "mesh1's primary, a Mesh this holder only heard of");
        let not_primary = reply(&call(&rig, &mesh1_other, &declare(&id1), &CallOptions::default()).await);
        assert_eq!(not_primary, StatusReply::Applied, "decided for mesh1, whoever sends it, and answered Applied again (Q144)");
        let own_mesh = reply(&call(&rig, &mesh1_primary, &declare(&id2), &CallOptions::default()).await);
        assert_eq!(own_mesh, StatusReply::Applied, "the Mesh this holder's own Build created");
        let unknown = reply(&call(&rig, &mesh1_primary, &declare(&MeshId::mint()), &CallOptions::default()).await);
        assert_eq!(unknown, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "a Mesh this holder holds neither way");
        assert_eq!(reply(&call(&rig, &mesh1_primary, &declare(&id1), &CallOptions::default()).await), StatusReply::Applied, "a repeated declaration is answered Applied again (Q144)");
        let spans = finish(&capture, &dir, json!({"cell": cell, "applied": format!("{applied:?}"), "non_primary": format!("{not_primary:?}"), "own_mesh": format!("{own_mesh:?}"), "unknown_mesh": format!("{unknown:?}")}));
        assert_eq!(spans_where(&spans, DECLARATION, &[("op", "declare-mesh-state"), ("outcome", "applied")]).len(), 4, "the two repeats answered Applied (Q144)");
        assert_eq!(spans_where(&spans, DECLARATION, &[("op", "declare-mesh-state"), ("outcome", "rejected-not-primary")]).len(), 1);
    });
}

/// How a scripted member answers the node command op 0x22 (i144 R22).
#[derive(Clone)]
enum Script {
    /// One refusal frame before any acceptance.
    Refuse(rdm_core::node_command::NodeCommandReply),
    /// `CommandAccepted`, then `ConnectionsDeleting` for `All`, then `end`: the completion, or, when
    /// `None`, nothing more: the stream stays open until the member goes away.
    Accept { end: Option<rdm_core::node_command::NodeCommandReply> },
    /// As `Accept`, each command ending on the next of these completions in turn.
    AcceptEach(Arc<Mutex<std::collections::VecDeque<rdm_core::node_command::NodeCommandReply>>>),
}

/// A member birth serving only the node command op 0x22, answering as `script` says.
async fn scripted_member(b: &Birth, script: Script) -> (ResolvedNode, Router) {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply as N, NodeCommandRequest};
    let server = ServerBuilder::new()
        .serve_stream::<NodeCommand, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: NodeCommandRequest, sink| {
            let script = script.clone();
            async move {
                let NodeCommandRequest::DeleteConnections { node_id, incarnation, scope } = req else { panic!("expected DeleteConnections") };
                match script {
                    Script::Refuse(refusal) => Ok(refusal),
                    Script::AcceptEach(ends) => {
                        let end = ends.lock().unwrap().pop_front().expect("the script holds a completion for each command");
                        let mut sink = sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                        if matches!(scope, rdm_core::status::ConnectionScope::All) {
                            sink.data(N::ConnectionsDeleting { node_id, incarnation }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                        }
                        Ok(end)
                    }
                    Script::Accept { end } => {
                        let mut sink = sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                        if matches!(scope, rdm_core::status::ConnectionScope::All) {
                            sink.data(N::ConnectionsDeleting { node_id, incarnation }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                        }
                        match end {
                            Some(end) => Ok(end),
                            None => std::future::pending().await,
                        }
                    }
                }
            }
        })
        .build(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().to_string() })
        .unwrap();
    let ep = b.endpoint().await;
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    (ResolvedNode { node_id: b.node.node_id.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() }, router)
}

// @feature: node-lifecycle
/// CONTRACT (i144 R22; Luke Q6, Q7: the node reports admission and completion on the command's
/// ordered reply stream, op 0x22; the mesh-primary records only what the node reported; NotSent
/// changes nothing; a stream broken after the acceptance is incomplete; Deadline keeps
/// connections-deleting, Established completes it): the mesh-primary carries `node.connection.delete`
/// to five members of its mesh, each answering one way. An `Established` and a `Deadline` completion
/// end `Deleted` with that receipt, each after the record of the node's `ConnectionsDeleting`, and the
/// receipt is held for the birth's rename; a break after the acceptance ends `Incomplete`, reported,
/// with no receipt held; a refusal before acceptance ends `RefusedByNode` with nothing recorded; a
/// member this mesh-primary cannot resolve ends `NotSent` with nothing recorded and its Status answer
/// names it. Must NOT happen: a record before the node's report, a completion claimed for a broken
/// stream, or a NotSent folded into a refusal.
#[test]
fn a_carried_connections_delete_ends_each_way_by_name_and_records_only_what_the_node_reported() {
    let cell = "a_carried_connections_delete_ends_each_way_by_name_and_records_only_what_the_node_reported";
    let capture = capture(cell);
    capture.run(carried_connections_delete_ends(&capture));
}

async fn carried_connections_delete_ends(capture: &Capture) {
    use rdm_admin::status_rpc::CarryEnd;
    use rdm_core::node_command::NodeCommandReply as N;
    use rdm_core::status::{ConnectionScope, ConnectionsReceipt};
    let mp = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
    let members: Vec<Birth> = (1..=5).map(|i| birth(&format!("mesh1.rpc.{i}"), NodeKind::RpcNode, "mesh1", false, false)).collect();
    let established = ConnectionsReceipt::Established { in_flight: 0, closed: vec![] };
    let deadline = ConnectionsReceipt::Deadline { last_in_flight: 1, closed: vec![], held: vec![] };
    let deleted = |r: &ConnectionsReceipt| N::ConnectionsDeleted { closed: vec![], not_open: vec![], held: vec![], receipt: Some(r.clone()) };
    let scripts = [
        Script::Accept { end: Some(deleted(&established)) },
        Script::Accept { end: Some(deleted(&deadline)) },
        Script::Accept { end: None },
        Script::Refuse(N::RejectedStaleIncarnation { held: IncarnationId::mint() }),
    ];
    let mut routers = Vec::new();
    let resolver = Arc::new(StaticResolver::new());
    for (b, script) in members.iter().zip(scripts) {
        let (resolved, router) = scripted_member(b, script).await;
        resolver.insert(resolved);
        routers.push(router);
    }
    // The fifth member is never served and never resolved: its command cannot leave.
    let others: Vec<&Birth> = members.iter().collect();
    let rig = rig(mp, &others, BTreeMap::new()).await;
    let client = NodeRpcClient::new(rig.me.endpoint().await, resolver.clone()).with_caller_system("rdm");
    let _ = rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(client), Default::default(), Arc::new(rdm_admin::node_rows::WrittenRows::default()))));
    let carry = |i: usize| {
        let (authority, node_id, incarnation) = (rig.authority.clone(), members[i].node.node_id.clone(), members[i].node.incarnation_id.clone().unwrap());
        async move { authority.carry_connections_delete(&node_id, &incarnation, &ConnectionScope::All).await }
    };
    let mut ends = vec![carry(0).await, carry(1).await];
    // The third member accepts, reports connections-deleting and holds its stream: once the mesh-primary
    // has recorded the report, the member goes away, and the stream breaks after the acceptance.
    let broken = tokio::spawn(carry(2));
    let third = members[2].node.node_id.to_string();
    let until = std::time::Instant::now() + Duration::from_secs(10);
    while !capture.spans().iter().any(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleting" && sp["attributes"]["node_id"].as_str() == Some(third.as_str())) {
        assert!(std::time::Instant::now() < until, "the mesh-primary never recorded the third member's report");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = routers.remove(2).shutdown().await;
    ends.push(broken.await.unwrap());
    ends.push(carry(3).await);
    ends.push(carry(4).await);
    assert!(matches!(&ends[0], CarryEnd::Deleted { receipt: Some(r), .. } if *r == established), "{:?}", ends[0]);
    assert!(matches!(&ends[1], CarryEnd::Deleted { receipt: Some(r), .. } if *r == deadline), "{:?}", ends[1]);
    assert!(matches!(&ends[2], CarryEnd::Incomplete { reported: true, .. }), "a break after the acceptance is incomplete, the node's report standing: {:?}", ends[2]);
    assert!(matches!(&ends[3], CarryEnd::RefusedByNode(N::RejectedStaleIncarnation { .. })), "{:?}", ends[3]);
    assert!(matches!(&ends[4], CarryEnd::NotSent(_)), "a member that cannot be resolved is never sent the command: {:?}", ends[4]);
    assert!(matches!(ends[4].status_reply(), StatusReply::NotReady { reason } if reason.contains("not-sent")), "the Status answer names it: {:?}", ends[4].status_reply());
    let held = rig.authority.declared.lock().unwrap().connections_deleted.keys().map(|(n, _)| n.clone()).collect::<Vec<_>>();
    assert!(held.contains(&members[0].node.node_id) && held.contains(&members[1].node.node_id), "both completions are held for the births' rename: {held:?}");
    for b in &members[2..] {
        assert!(!held.contains(&b.node.node_id), "{}: no completion is held: {held:?}", b.node.name);
    }

    let spans = capture.spans();
    let attr = |sp: &Value, k: &str| sp["attributes"][k].as_str().unwrap_or_default().to_string();
    let recorded = |b: &Birth| spans.iter().filter(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleting" && attr(sp, "node_id") == b.node.node_id.to_string()).count();
    let completed = |b: &Birth| spans.iter().find(|sp| sp["name"] == "rdm.node_admin.node.update.via-connections-deleted" && attr(sp, "node_id") == b.node.node_id.to_string()).map(|sp| attr(sp, "receipt"));
    assert_eq!((recorded(&members[0]), completed(&members[0]).as_deref()), (1, Some("established")), "the node's report, then Established");
    assert_eq!((recorded(&members[1]), completed(&members[1]).as_deref()), (1, Some("deadline")), "the node's report, then Deadline: the birth stays connections-deleting");
    assert_eq!((recorded(&members[2]), completed(&members[2])), (1, None), "the report stands, no completion");
    assert_eq!((recorded(&members[3]), recorded(&members[4])), (0, 0), "a refused or unsent command records nothing");
    drop(routers);
}

// @feature: node-lifecycle
/// CONTRACT (node-connections-delete.md step 4: "The node's mesh-primary sets connections-deleted from
/// that completion. Graceful success is never reported after interrupting work"; step 5: "At
/// `RDM_CONNECTIONS_DELETE_DEADLINE_MS` an `All` that has not reached zero returns incomplete closure
/// ... The node stays connections-deleting. A new `node.connection.delete` attempt, or node.stop,
/// leaves that state"; states.md "Who sets each state"): the mesh-primary carries `node.connection.delete
/// { All }` to a member twice. The first ends `Deadline`: the member's row says connections-deleting
/// and never connections-deleted. The second, a new attempt, ends `Established`: the row says
/// connections-deleted. Must NOT happen: connections-deleted on the row from a `Deadline`, or the
/// second attempt's `Established` left unrecorded.
#[test]
fn a_deadline_all_leaves_the_row_connections_deleting_and_a_new_attempt_ending_established_sets_connections_deleted() {
    let capture = capture("a_deadline_all_leaves_the_row_connections_deleting_and_a_new_attempt_ending_established_sets_connections_deleted");
    capture.run(async {
        use rdm_admin::status_rpc::CarryEnd;
        use rdm_core::node_command::NodeCommandReply as N;
        use rdm_core::status::{ConnectionScope, ConnectionsReceipt};
        let mp = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
        let member = birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", false, false);
        let deadline = ConnectionsReceipt::Deadline { last_in_flight: 1, closed: vec![], held: vec![] };
        let established = ConnectionsReceipt::Established { in_flight: 0, closed: vec![] };
        let deleted = |r: &ConnectionsReceipt| N::ConnectionsDeleted { closed: vec![], not_open: vec![], held: vec![], receipt: Some(r.clone()) };
        let ends = Arc::new(Mutex::new(std::collections::VecDeque::from([deleted(&deadline), deleted(&established)])));
        let (resolved, router) = scripted_member(&member, Script::AcceptEach(ends)).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(resolved);
        let rig = rig(mp, &[&member], BTreeMap::new()).await;
        let client = NodeRpcClient::new(rig.me.endpoint().await, resolver.clone()).with_caller_system("rdm");
        let _ = rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(client), Default::default(), Arc::new(rdm_admin::node_rows::WrittenRows::default()))));
        rig.authority.nodes_storage.put_node(&rdm_admin::storage::NodeRow::new(member.node.name.clone(), member.node.node_id.clone(), NodeStatus::Ready)).await.unwrap();
        let (node_id, incarnation) = (member.node.node_id.clone(), member.node.incarnation_id.clone().unwrap());
        let status = || async { rig.authority.nodes_storage.node(&node_id).await.unwrap().expect("the member's row").status };

        let first = rig.authority.carry_connections_delete(&node_id, &incarnation, &ConnectionScope::All).await;
        assert!(matches!(&first, CarryEnd::Deleted { receipt: Some(r), .. } if *r == deadline), "{first:?}");
        assert_eq!(status().await, NodeStatus::ConnectionsDeleting, "a Deadline leaves the row connections-deleting");

        let second = rig.authority.carry_connections_delete(&node_id, &incarnation, &ConnectionScope::All).await;
        assert!(matches!(&second, CarryEnd::Deleted { receipt: Some(r), .. } if *r == established), "{second:?}");
        assert_eq!(status().await, NodeStatus::ConnectionsDeleted, "the new attempt's Established sets connections-deleted");
        drop(router);
    });
    let rows: Vec<String> = capture.spans().iter().filter(|sp| sp["name"] == "rdm.node_admin.node.update.via-row-status" && sp["attributes"]["subject"] == "mesh1.rpc.1").map(|sp| format!("{}:{}", sp["attributes"]["status"].as_str().unwrap_or_default(), sp["attributes"]["outcome"].as_str().unwrap_or_default())).collect();
    assert_eq!(rows, ["ConnectionsDeleting:written", "ConnectionsDeleting:unchanged", "ConnectionsDeleted:written"], "the mesh-primary's row writes, in order: connections-deleted only from the Established completion");
}

/// What a mesh-primary folds from the stores as it takes `mesh` (`SeatFold::on_install`'s read): the
/// facts on every mesh and on the fabric, and on every birth of `mesh`'s node records.
async fn folded(nodes: &dyn rdm_admin::storage::NodesStorage, meshes: &dyn rdm_admin::storage::MeshStorage, fabric: &dyn rdm_admin::fabric_storage::FabricStorage, mesh: &str) -> rdm_admin::status_rpc::Declared {
    let (m, f, n) = (meshes.meshes().await.unwrap(), fabric.fabric().await.unwrap(), nodes.nodes(mesh).await.unwrap());
    let mut declared = rdm_admin::status_rpc::Declared::default();
    declared.fold_records(&m, f.as_ref(), &n, |_, _| true);
    declared
}
