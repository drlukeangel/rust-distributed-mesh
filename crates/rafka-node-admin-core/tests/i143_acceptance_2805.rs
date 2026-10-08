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

/// Every span this process emitted, from the evidence exporter's JSONL files in `dir`.
fn collect_spans(dir: &std::path::Path) -> Vec<Value> {
    let mut spans = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.file_name().is_some_and(|n| n.to_string_lossy().ends_with(".spans.jsonl")) {
            for line in std::fs::read_to_string(&p).unwrap().lines() {
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    spans.push(v);
                }
            }
        }
    }
    spans
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
    let mut nodes = vec![me.node.clone()];
    nodes.extend(others.iter().map(|b| b.node.clone()));
    let topology = Arc::new(tokio::sync::RwLock::new(Topology {
        fabric: Fabric { id: rafka_mesh_entity::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
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
        declared: Arc::new(Mutex::new(Declared::default())),
        nodes_storage: Arc::new(MemoryNodesStorage::default()),
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_authority_retries_pending_by_natural_key_returns_already_applied() {
    let cell = "status_authority_retries_pending_by_natural_key_returns_already_applied";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RAFKA_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-node-admin-core-acceptance");

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

    drop(telemetry);
    let spans = collect_spans(&dir);
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
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn draining_authority_serves_status_checks_exact_birth() {
    let cell = "draining_authority_serves_status_checks_exact_birth";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RAFKA_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-node-admin-core-acceptance");

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

    drop(telemetry);
    let spans = collect_spans(&dir);
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
    use rafka_node_admin_core::deployment::endpoint::EndpointAllocator;
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
            traceparent: None,
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
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-2805-{}", NodeId::mint())),
    };
    let allocator = Mutex::new(EndpointAllocator::new("127.0.0.1".parse().unwrap(), 1, 1));
    let pipeline = DeploymentPipeline {
        provider: &Running,
        allocator: &allocator,
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
    let req = RetireRequest { build_id: build_id.clone(), attempt: 1, node, handle, kind: RetireKind::Removal, observe_departure: false, keep_endpoints: false };
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
