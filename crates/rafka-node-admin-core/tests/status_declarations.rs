//! i143.e6.s7 functional: a node-admin applies lifecycle declarations as an authority.
//!
//! The admin's `Status` service runs in-process over a view fixture (one fabric, two meshes, this
//! admin the primary of mesh1 and the fabric-primary); nodes declare over real Node RPC on
//! loopback. The cells prove the decision table of `status_rpc`: who may declare what, the
//! natural-key idempotency, forward-only transitions, stale births, and that `Applied` is on the
//! subject's `nodes.storage` row before it is answered. Certainty: a cut before the request
//! commits is `NotSent` with nothing applied; a reply lost after the apply is `Indeterminate`,
//! and the same declaration again is `AlreadyApplied` — one logical event.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind};
use rafka_node_admin_core::model::{Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
use rafka_node_admin_core::status_rpc::{Declared, StatusAuthority};
use rafka_node_admin_core::storage::{MemoryNodesStorage, NodesStorage};
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget, ReplyWithhold, ResolvedNode, ServedBirth, ServerBuilder, ServerStats, StaticResolver};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{FabricEvent, MeshState, NodeState, NotAuthority, Status, StatusReply, StatusRequest};
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

/// A birth in the fixture: its keys, and the client it declares with.
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

struct Rig {
    admin: Birth,
    server: rafka_node_rpc::NodeRpcServer,
    _router: Router,
    resolved: ResolvedNode,
    authority: Arc<StatusAuthority>,
    storage: Arc<MemoryNodesStorage>,
    topology: Arc<tokio::sync::RwLock<Topology>>,
    mesh_ids: BTreeMap<String, MeshId>,
    /// Inert until a cell arms it.
    withhold: Arc<ReplyWithhold>,
}

impl Rig {
    fn mesh_id(&self, mesh: &str) -> MeshId {
        self.mesh_ids[mesh].clone()
    }
}

/// mesh1: admin.1 (this authority: mesh-primary and fabric-primary), admin.2, rpc.1, rpc.2;
/// mesh2: admin.1 (mesh2's primary).
async fn rig() -> (Rig, BTreeMap<&'static str, Birth>) {
    let admin = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
    let mut others = BTreeMap::new();
    others.insert("admin2", birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false));
    others.insert("rpc1", birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", true, false));
    others.insert("rpc2", birth("mesh1.rpc.2", NodeKind::RpcNode, "mesh1", false, false));
    others.insert("m2admin", birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false));
    let mut nodes = vec![admin.node.clone()];
    nodes.extend(others.values().map(|b| b.node.clone()));
    let topology = Arc::new(tokio::sync::RwLock::new(Topology {
        fabric: Fabric { id: rafka_mesh_entity::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes,
    }));
    let fabric_id = topology.read().await.fabric.id.clone();
    let storage = Arc::new(MemoryNodesStorage::default());
    let (m1id, m2id) = (MeshId::mint(), MeshId::mint());
    let mesh_ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), m1id.clone()), ("mesh2".to_string(), m2id.clone())].into_iter().collect();
    let rig_mesh_ids = mesh_ids.clone();
    let authority = Arc::new(StatusAuthority {
        me: admin.node.name.clone(),
        fabric_id,
        topology: topology.clone(),
        declared: Arc::new(Mutex::new(Declared::default())),
        nodes_storage: storage.clone(),
        status_storage: Arc::new(rafka_node_admin_core::status_storage::MemoryStatusStorage::default()),
        mesh_ids: Arc::new(move || mesh_ids.clone()),
        republish: Arc::new(std::sync::OnceLock::new()),
        commands: Arc::new(rafka_node_admin_core::node_commands::CommandBook::default()),
        own: Arc::new(std::sync::OnceLock::new()),
        leaver: Arc::new(std::sync::OnceLock::new()),
        hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        rounds: Default::default(),
    });
    let slot: Arc<OnceLock<Arc<StatusAuthority>>> = Arc::new(OnceLock::new());
    let _ = slot.set(authority.clone());
    let withhold = Arc::new(ReplyWithhold::default());
    let server = rafka_node_admin_core::status_rpc::serve(ServerBuilder::new().withhold_replies(withhold.clone()), slot)
        .seal(ServedBirth { node_id: admin.node.node_id.to_string(), incarnation: admin.node.incarnation_id.clone().unwrap().0 })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
    let resolved = ResolvedNode {
        node_id: admin.node.node_id.clone(),
        name: admin.node.name.clone(),
        endpoint_id: admin.key.public(),
        transport_addr: addr,
        incarnation: admin.node.incarnation_id.clone().unwrap(),
    };
    (Rig { admin, server, _router: router, resolved, authority, storage, topology, mesh_ids: rig_mesh_ids, withhold }, others)
}

/// A birth's client to the authority, speaking as that birth.
async fn client_of(rig: &Rig, b: &Birth) -> NodeRpcClient {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(rig.resolved.clone());
    let ep = rafka_node_rpc::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    NodeRpcClient::new(ep, resolver).with_caller_system("rdm")
}

fn declare(b: &Birth, state: NodeState) -> StatusRequest {
    StatusRequest::DeclareNodeState { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), state }
}

async fn call(rig: &Rig, b: &Birth, req: &StatusRequest) -> RpcOutcome<StatusReply> {
    client_of(rig, b).await.call::<Status>(&NodeTarget::ExactNode(rig.admin.node.node_id.clone()), req, &CallOptions::default()).await.0
}

fn reply(out: &RpcOutcome<StatusReply>) -> StatusReply {
    match out {
        RpcOutcome::Reply(r) => r.value().clone(),
        other => panic!("expected a reply: {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_declares_its_own_state_forward_only_and_applied_is_on_its_row_first() {
    let (rig, b) = rig().await;
    let rpc1 = &b["rpc1"];
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::ReadyForTraffic)).await), StatusReply::Applied);
    // Applied is on the subject's row before it was answered.
    let row = rig.storage.contacts().await.unwrap().into_iter().find(|c| c.node_id == rpc1.node.node_id).expect("the subject's row");
    assert_eq!(row.declared.as_deref(), Some("ReadyForTraffic"));
    assert_eq!(row.incarnation_id, rpc1.node.incarnation_id.clone().unwrap());
    // The same natural key again: one logical event.
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::ReadyForTraffic)).await), StatusReply::AlreadyApplied);
    // Backward is refused naming the current state; a skip forward applies.
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Pending)).await), StatusReply::RejectedInvalidNodeTransition { current: NodeState::ReadyForTraffic });
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Leaving)).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Draining)).await), StatusReply::RejectedInvalidNodeTransition { current: NodeState::Leaving });
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id).map(|(_, s)| s), Some(NodeState::Leaving));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authority_is_the_peer_and_the_seat_never_the_request() {
    let (rig, b) = rig().await;
    let (rpc1, rpc2, admin2, m2admin) = (&b["rpc1"], &b["rpc2"], &b["admin2"], &b["m2admin"]);
    // rpc.2 declaring rpc.1's state: the sender is not the subject.
    let r = reply(&call(&rig, rpc2, &declare(rpc1, NodeState::ReadyForTraffic)).await);
    assert_eq!(r, StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: "mesh1.rpc.2".into() } });
    assert!(rig.authority.declared.lock().unwrap().nodes.is_empty(), "nothing applied");
    // A peer the view does not hold at all.
    let stranger = birth("mesh1.rpc.9", NodeKind::RpcNode, "mesh1", false, false);
    let r = reply(&call(&rig, &stranger, &declare(&stranger, NodeState::ReadyForTraffic)).await);
    assert!(matches!(r, StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { .. } }), "{r:?}");
    // A stale birth: the view holds a newer incarnation of the subject.
    let stale = StatusRequest::DeclareNodeState { node_id: rpc1.node.node_id.clone(), incarnation: IncarnationId::mint(), state: NodeState::ReadyForTraffic };
    assert_eq!(reply(&call(&rig, rpc1, &stale).await), StatusReply::RejectedStaleIncarnation { held: rpc1.node.incarnation_id.clone().unwrap() });
    // A node of another mesh: this admin is not its mesh-primary.
    let foreign = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
    rig.topology.write().await.nodes.push(foreign.node.clone());
    let r = reply(&call(&rig, &foreign, &declare(&foreign, NodeState::ReadyForTraffic)).await);
    assert_eq!(r, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } });
    // A node-admin declares to the fabric-primary (this admin holds it): Applied.
    assert_eq!(reply(&call(&rig, admin2, &declare(admin2, NodeState::ReadyForTraffic)).await), StatusReply::Applied);
    // Once the fabric seat moves away, an admin's declaration here is refused by the seat.
    rig.topology.write().await.nodes.iter_mut().for_each(|n| {
        if n.name == rig.admin.node.name {
            n.is_fabric_primary = false
        }
    });
    let r = reply(&call(&rig, m2admin, &declare(m2admin, NodeState::ReadyForTraffic)).await);
    assert_eq!(r, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "fabric-primary of mesh2".into() } });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_and_fabric_declarations_follow_the_same_rules() {
    let (rig, b) = rig().await;
    let (m2admin, rpc1, admin2) = (&b["m2admin"], &b["rpc1"], &b["admin2"]);
    // mesh2's primary declares mesh2's state to the fabric-primary.
    let m2 = |s| StatusRequest::DeclareMeshState { mesh_id: rig.mesh_id("mesh2"), state: s };
    assert_eq!(reply(&call(&rig, m2admin, &m2(MeshState::ReadyForTraffic)).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, m2admin, &m2(MeshState::ReadyForTraffic)).await), StatusReply::AlreadyApplied);
    assert_eq!(reply(&call(&rig, m2admin, &m2(MeshState::Pending)).await), StatusReply::RejectedInvalidMeshTransition { current: MeshState::ReadyForTraffic });
    // Not mesh2's primary (an ordinary node; a non-primary admin of another mesh): refused.
    assert!(matches!(reply(&call(&rig, rpc1, &m2(MeshState::Leaving)).await), StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { .. } }));
    assert!(matches!(reply(&call(&rig, admin2, &m2(MeshState::Leaving)).await), StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { .. } }));
    // A fabric event applied here must come from the fabric-primary: while this admin holds the
    // fabric seat, mesh2's primary is not it. Then the seat moves to mesh2's primary (the view
    // says so), and its events and Pending hand-offs apply here, each once.
    let ev = StatusRequest::ApplyFabricEvent { fabric_id: rig.authority.fabric_id.clone(), event: FabricEvent::ReadyForTraffic };
    assert!(matches!(reply(&call(&rig, m2admin, &ev).await), StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { .. } }));
    rig.topology.write().await.nodes.iter_mut().for_each(|n| {
        if n.name == rig.admin.node.name {
            n.is_fabric_primary = false
        }
        if n.name == m2admin.node.name {
            n.is_fabric_primary = true
        }
    });
    assert_eq!(reply(&call(&rig, m2admin, &ev).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, m2admin, &ev).await), StatusReply::AlreadyApplied);
    let wrong = StatusRequest::ApplyFabricEvent { fabric_id: FabricId::mint(), event: FabricEvent::ReadyForTraffic };
    assert_eq!(reply(&call(&rig, m2admin, &wrong).await), StatusReply::RejectedStaleFabric { held: rig.authority.fabric_id.clone() });
    // ApplyMeshState(Pending) from the fabric-primary at an admin of that mesh: this admin is
    // mesh1's; mesh2's Pending is not its to apply, mesh1's is, and a foreign mesh id is refused.
    let pend = |id: &MeshId, name: &str| StatusRequest::ApplyMeshState { mesh_id: id.clone(), mesh_name: name.into(), state: MeshState::Pending };
    let (m1id, m2id, other) = (rig.mesh_id("mesh1"), rig.mesh_id("mesh2"), MeshId::mint());
    assert!(matches!(reply(&call(&rig, m2admin, &pend(&m2id, "mesh2")).await), StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { .. } }));
    assert_eq!(reply(&call(&rig, m2admin, &pend(&other, "mesh1")).await), StatusReply::RejectedStaleMesh { held: m1id.clone() }, "never a replacement mesh id");
    assert_eq!(reply(&call(&rig, m2admin, &pend(&m1id, "mesh1")).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, rpc1, &pend(&m1id, "mesh1")).await), StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { sender: "mesh1.rpc.1".into() } }, "only the fabric-primary applies a Pending");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cut_applies_nothing_and_a_lost_reply_is_indeterminate_then_already_applied() {
    let (rig, b) = rig().await;
    let rpc1 = &b["rpc1"];
    let c = client_of(&rig, rpc1).await;
    let target = NodeTarget::ExactNode(rig.admin.node.node_id.clone());
    // Cut before the request commits: NotSent, nothing applied, no row.
    let (out, _) = c.call::<Status>(&target, &declare(rpc1, NodeState::ReadyForTraffic), &CallOptions { cut_before_finish: true, ..Default::default() }).await;
    assert!(matches!(out, RpcOutcome::NotSent(_)), "{out:?}");
    assert!(rig.authority.declared.lock().unwrap().nodes.is_empty());
    assert!(rig.storage.contacts().await.unwrap().iter().all(|c| c.node_id != rpc1.node.node_id));
    // Applied, reply lost: Indeterminate at the caller; the same declaration again is AlreadyApplied.
    rig.authority.hold_next_reply.store(true, Ordering::SeqCst);
    let (out, ev) = c.call::<Status>(&target, &declare(rpc1, NodeState::ReadyForTraffic), &CallOptions { budget: Budget::Overall(Duration::from_millis(800)), ..Default::default() }).await;
    assert!(matches!(out, RpcOutcome::Indeterminate(_)), "{out:?}");
    assert!(ev.unwrap().committed);
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id).map(|(_, s)| s), Some(NodeState::ReadyForTraffic), "applied before the reply was lost");
    let (out, _) = c.call::<Status>(&target, &declare(rpc1, NodeState::ReadyForTraffic), &CallOptions::default()).await;
    assert_eq!(reply(&out), StatusReply::AlreadyApplied, "one logical event");
}


/// CONTRACT: a declaration is applied and its reply is withheld past the caller's reply deadline, so the
/// caller classifies the call `Indeterminate`. The reply is released only AFTER that; the caller had
/// already abandoned the call's reply stream, so the late reply is refused at the stream and cannot
/// change the completed call. The same declaration again is `AlreadyApplied`, and the node applied it
/// exactly once (the handler ran for the first call and the retry, and the state was moved by the first
/// alone).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reply_released_after_indeterminate_is_dropped_and_the_retry_is_already_applied() {
    let (rig, b) = rig().await;
    let rpc1 = &b["rpc1"];
    let c = client_of(&rig, rpc1).await;
    let target = NodeTarget::ExactNode(rig.admin.node.node_id.clone());
    rig.withhold.arm();
    let opts = CallOptions { budget: Budget::Split { send: Duration::from_secs(5), reply: Duration::from_millis(600) }, ..Default::default() };
    let (out, ev) = c.call::<Status>(&target, &declare(rpc1, NodeState::ReadyForTraffic), &opts).await;
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == rafka_node_rpc_contract::outcome::IndeterminateReason::ReplyDeadline), "{out:?}");
    assert!(ev.unwrap().committed, "the request crossed the commit cut before the reply was withheld");
    rig.withhold.reached().await;
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id).map(|(_, s)| s), Some(NodeState::ReadyForTraffic), "applied before the reply was withheld");
    assert_eq!(ServerStats::get(&rig.server.stats().dispatched), 1, "the handler ran once for the lost call");

    // The reply goes out only now, after the caller classified the call.
    rig.withhold.release();
    let late = tokio::time::timeout(Duration::from_secs(10), rig.withhold.late_reply())
        .await
        .expect("the caller never abandoned the reply stream of a call it had already classified");
    assert!(matches!(late.stopped, Ok(Some(_))), "the caller stopped the reply stream when it classified the call: {late:?}");
    assert!(late.written.is_err(), "the late reply was refused at the abandoned stream: {late:?}");

    // The completed call is unchanged and the retry is one logical event.
    let (out, _) = c.call::<Status>(&target, &declare(rpc1, NodeState::ReadyForTraffic), &CallOptions::default()).await;
    assert_eq!(reply(&out), StatusReply::AlreadyApplied, "one logical event");
    assert_eq!(ServerStats::get(&rig.server.stats().dispatched), 2, "the lost call and the retry, nothing replayed");
    let row = rig.storage.contacts().await.unwrap().into_iter().find(|c| c.node_id == rpc1.node.node_id).expect("the subject's row");
    assert_eq!(row.declared.as_deref(), Some("ReadyForTraffic"));
}

/// Drain admission never prevents an authority from receiving the upward certainty calls
/// that finish lifecycle work. The status handler must still enforce its own authority checks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_authority_accepts_upward_status_but_still_refuses_an_impostor() {
    let (rig, b) = rig().await;
    rig.server.drain();
    for subject in [&b["rpc1"], &b["admin2"]] {
        for state in [NodeState::Draining, NodeState::Leaving] {
            let request = declare(subject, state);
            assert_eq!(reply(&call(&rig, subject, &request).await), StatusReply::Applied);
            assert_eq!(reply(&call(&rig, subject, &request).await), StatusReply::AlreadyApplied);
        }
    }
    let forged = declare(&b["rpc1"], NodeState::Leaving);
    assert!(matches!(reply(&call(&rig, &b["rpc2"], &forged).await),
        StatusReply::RejectedNotAuthority { why: NotAuthority::SenderNotSubject { .. } }));
}
