//! i143.e6.s7 functional: a node-admin applies lifecycle declarations as an authority.
//!
//! The admin's `Status` service runs in-process over a view fixture (one fabric, two meshes, this
//! admin the primary of mesh1 and the fabric-primary); nodes declare over real Node RPC on
//! loopback. The cells prove the decision table of `status_rpc`: which seat decides which
//! subject's declaration, whoever calls (node-rpc.md §14), the natural-key idempotency, forward-only transitions, stale births, and that `Applied` is on the
//! subject's declared-state row in `status.storage` before it is answered. Certainty: a cut before the request
//! commits is `NotSent` with nothing applied; a reply lost after the apply is `Indeterminate`,
//! and the same declaration again is `AlreadyApplied` — one logical event.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind};
use rafka_node_admin_core::model::{Fabric, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rafka_node_admin_core::status_rpc::{Declared, StatusAuthority};
use rafka_node_admin_core::status_storage::{MemoryStatusStorage, StatusStorage};
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
    storage: Arc<MemoryStatusStorage>,
    /// The nodes store: the node rows the fabric-primary puts for a config update.
    nodes: Arc<MemoryNodesStorage>,
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
    let storage = Arc::new(MemoryStatusStorage::default());
    let nodes = Arc::new(MemoryNodesStorage::default());
    let (m1id, m2id) = (MeshId::mint(), MeshId::mint());
    let mesh_ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), m1id.clone()), ("mesh2".to_string(), m2id.clone())].into_iter().collect();
    let rig_mesh_ids = mesh_ids.clone();
    let authority = Arc::new(StatusAuthority {
        me: admin.node.name.clone(),
        node_id: admin.node.node_id.clone(),
        fabric_id,
        topology: topology.clone(),
        declared: Arc::new(Mutex::new(Declared::default())),
        status_storage: storage.clone(),
        nodes_storage: nodes.clone(),
        mesh_ids: Arc::new(move || mesh_ids.clone()),
        republish: Arc::new(std::sync::OnceLock::new()),
        commands: Arc::new(rafka_node_admin_core::node_commands::CommandBook::default()),
        own: Arc::new(std::sync::OnceLock::new()),
        leaver: Arc::new(std::sync::OnceLock::new()),
        hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        rounds: Default::default(),
        configs: Default::default(),
        configure: Default::default(),
        hydration: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(),
        loads: None,
    });
    // The rig's view stands for the admin's projection: a moved row moves the birth's entry.
    let moved_view = topology.clone();
    let _ = authority.row_moved.set(Arc::new(move |_from: PathName, node_id: NodeId, _incarnation: IncarnationId, to: PathName| {
        let view = moved_view.clone();
        Box::pin(async move {
            if let Some(n) = view.write().await.nodes.iter_mut().find(|n| n.node_id == node_id) {
                n.name = to;
            }
        })
    }));
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
    (Rig { admin, server, _router: router, resolved, authority, storage, nodes, topology, mesh_ids: rig_mesh_ids, withhold }, others)
}

/// A birth's client to the authority, speaking as that birth.
async fn client_of(rig: &Rig, b: &Birth) -> NodeRpcClient {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(rig.resolved.clone());
    let ep = rafka_node_rpc::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    tracing::info!(step = "client-bound", node = %b.node.name, peer = %b.key.public().fmt_short(), local = ?ep.bound_sockets(), "a client endpoint is bound on the birth's key");
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
    let row = rig.storage.node_declared().await.unwrap().into_iter().find(|c| c.node_id == rpc1.node.node_id).expect("the subject's row");
    assert_eq!(row.state, NodeState::ReadyForTraffic);
    assert_eq!(row.incarnation_id, rpc1.node.incarnation_id.clone().unwrap());
    // The same natural key again: one logical event.
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::ReadyForTraffic)).await), StatusReply::AlreadyApplied);
    // Backward is refused naming the current state; a skip forward applies.
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Pending)).await), StatusReply::RejectedInvalidNodeTransition { current: NodeState::ReadyForTraffic });
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Leaving)).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Draining)).await), StatusReply::RejectedInvalidNodeTransition { current: NodeState::Leaving });
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Leaving));
}

/// CONTRACT: the authority is the seat the subject's declaration needs, never the caller
/// (node-rpc.md §14). rpc.2 sending rpc.1's declaration is decided for rpc.1, on rpc.1's row; a
/// subject the view does not hold is `SubjectUnknown`; a birth the view no longer holds is stale; a
/// subject of another mesh, or a node-admin's declaration once the fabric seat moved away, is
/// refused naming the seat it needs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authority_is_the_seat_the_subject_needs_never_the_caller() {
    let (rig, b) = rig().await;
    let (rpc1, rpc2, admin2, m2admin) = (&b["rpc1"], &b["rpc2"], &b["admin2"], &b["m2admin"]);
    // rpc.2 sending rpc.1's state: decided for rpc.1.
    assert_eq!(reply(&call(&rig, rpc2, &declare(rpc1, NodeState::ReadyForTraffic)).await), StatusReply::Applied);
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::ReadyForTraffic), "applied for the subject");
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc2.node.node_id, rpc2.node.incarnation_id.as_ref().unwrap()), None, "nothing applied for the caller");
    // A subject the view does not hold at all.
    let stranger = birth("mesh1.rpc.9", NodeKind::RpcNode, "mesh1", false, false);
    let r = reply(&call(&rig, &stranger, &declare(&stranger, NodeState::ReadyForTraffic)).await);
    assert_eq!(r, StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown });
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

/// CONTRACT: nothing inside the mesh checks who is calling (node-rpc.md §14; node-rpc-envelope.md,
/// Status: "Nothing inside the mesh checks who is calling"): a declaration from an endpoint this
/// authority's view holds no birth for is decided for the subject the request names, on the
/// receiver's own seat. The direction refusals stay, decided by the subject and the seat: a subject
/// the view does not hold is `SubjectUnknown`, a subject of a mesh this admin is not the primary of
/// is `ReceiverNotPrimary`, and a birth the view no longer holds is `RejectedStaleIncarnation`.
/// What must NOT happen: a refusal naming the caller (`SenderNotSubject`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_from_an_endpoint_the_authority_never_heard_of_is_decided_for_its_subject() {
    let (rig, b) = rig().await;
    let (rpc1, admin2) = (&b["rpc1"], &b["admin2"]);
    // A fresh endpoint key, in no birth of the view.
    let stranger = birth("mesh1.rpc.9", NodeKind::RpcNode, "mesh1", false, false);
    assert!(rig.topology.read().await.births().all(|n| n.endpoint_id != stranger.node.endpoint_id), "the stranger is in no birth of the view");
    // A member's declaration is decided for that member: Applied, on its row first.
    assert_eq!(reply(&call(&rig, &stranger, &declare(rpc1, NodeState::ReadyForTraffic)).await), StatusReply::Applied);
    let row = rig.storage.node_declared().await.unwrap().into_iter().find(|c| c.node_id == rpc1.node.node_id).expect("the subject's row");
    assert_eq!((row.state, row.name.clone()), (NodeState::ReadyForTraffic, rpc1.node.name.clone()), "the row is the subject's, at its own path.name");
    // A node-admin's declaration reaches the fabric seat this admin holds.
    assert_eq!(reply(&call(&rig, &stranger, &declare(admin2, NodeState::ReadyForTraffic)).await), StatusReply::Applied);
    // A mesh's declaration reaches the fabric-primary for a mesh it holds.
    let m2 = StatusRequest::DeclareMeshState { mesh_id: rig.mesh_id("mesh2"), state: MeshState::ReadyForTraffic };
    assert_eq!(reply(&call(&rig, &stranger, &m2).await), StatusReply::Applied);
    // The direction refusals stay.
    assert_eq!(reply(&call(&rig, &stranger, &declare(&stranger, NodeState::ReadyForTraffic)).await), StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, "a subject the view does not hold");
    let unknown_mesh = StatusRequest::DeclareMeshState { mesh_id: MeshId::mint(), state: MeshState::ReadyForTraffic };
    assert_eq!(reply(&call(&rig, &stranger, &unknown_mesh).await), StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, "a mesh the fabric-primary does not hold");
    let stale = StatusRequest::DeclareNodeState { node_id: rpc1.node.node_id.clone(), incarnation: IncarnationId::mint(), state: NodeState::Leaving };
    assert_eq!(reply(&call(&rig, &stranger, &stale).await), StatusReply::RejectedStaleIncarnation { held: rpc1.node.incarnation_id.clone().unwrap() });
    let foreign = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
    rig.topology.write().await.nodes.push(foreign.node.clone());
    assert_eq!(reply(&call(&rig, &stranger, &declare(&foreign, NodeState::ReadyForTraffic)).await), StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } });
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
    // The declaration is decided for the mesh it names, whoever calls (an ordinary node; a
    // non-primary admin of another mesh): forward applies once, a mesh the holder does not hold is
    // `SubjectUnknown`.
    assert_eq!(reply(&call(&rig, rpc1, &m2(MeshState::Leaving)).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, admin2, &m2(MeshState::Leaving)).await), StatusReply::AlreadyApplied);
    let unknown = StatusRequest::DeclareMeshState { mesh_id: MeshId::mint(), state: MeshState::ReadyForTraffic };
    assert_eq!(reply(&call(&rig, m2admin, &unknown).await), StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown });
    // A fabric event is applied at a mesh primary (this admin is mesh1's), whoever sends it, once.
    let ev = StatusRequest::ApplyFabricEvent { fabric_id: rig.authority.fabric_id.clone(), event: FabricEvent::ReadyForTraffic };
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
    assert_eq!(reply(&call(&rig, rpc1, &pend(&m1id, "mesh1")).await), StatusReply::AlreadyApplied, "the Pending is decided for the mesh it names, whoever sends it");
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
    assert!(rig.storage.node_declared().await.unwrap().iter().all(|c| c.node_id != rpc1.node.node_id));
    // Applied, reply lost: Indeterminate at the caller; the same declaration again is AlreadyApplied.
    rig.authority.hold_next_reply.store(true, Ordering::SeqCst);
    let (out, ev) = c.call::<Status>(&target, &declare(rpc1, NodeState::ReadyForTraffic), &CallOptions { budget: Budget::Overall(Duration::from_millis(800)), ..Default::default() }).await;
    assert!(matches!(out, RpcOutcome::Indeterminate(_)), "{out:?}");
    assert!(ev.unwrap().committed);
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::ReadyForTraffic), "applied before the reply was lost");
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
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::ReadyForTraffic), "applied before the reply was withheld");
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
    let row = rig.storage.node_declared().await.unwrap().into_iter().find(|c| c.node_id == rpc1.node.node_id).expect("the subject's row");
    assert_eq!(row.state, NodeState::ReadyForTraffic);
}

/// Drain admission never prevents an authority from receiving the upward certainty calls
/// that finish lifecycle work. The status handler still decides each for its subject: a
/// declaration sent by another node is decided for the subject it names (here already applied),
/// and a subject the view does not hold is refused by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_draining_authority_accepts_upward_status_and_decides_it_for_the_subject() {
    let (rig, b) = rig().await;
    rig.server.drain();
    for subject in [&b["rpc1"], &b["admin2"]] {
        for state in [NodeState::Draining, NodeState::Leaving] {
            let request = declare(subject, state);
            assert_eq!(reply(&call(&rig, subject, &request).await), StatusReply::Applied);
            assert_eq!(reply(&call(&rig, subject, &request).await), StatusReply::AlreadyApplied);
        }
    }
    let for_another = declare(&b["rpc1"], NodeState::Leaving);
    assert_eq!(reply(&call(&rig, &b["rpc2"], &for_another).await), StatusReply::AlreadyApplied, "decided for rpc.1, whoever sends it");
    let stranger = birth("mesh1.rpc.9", NodeKind::RpcNode, "mesh1", false, false);
    assert_eq!(reply(&call(&rig, &b["rpc2"], &declare(&stranger, NodeState::Leaving)).await), StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown });
}

/// A birth of the fixture serving Status on its own key, recording every request it decodes and
/// answering `Applied`: the node end of a config apply.
struct ServedNode {
    resolved: ResolvedNode,
    seen: Arc<Mutex<Vec<StatusRequest>>>,
    /// Notified once per request served (a permit is kept when nobody waits yet).
    heard: Arc<tokio::sync::Notify>,
    _router: Router,
}

async fn serve_node(b: &Birth) -> ServedNode {
    serve_node_answering(b, StatusReply::Applied).await
}

/// A birth serving Status that records every request and answers each with `reply`.
async fn serve_node_answering(b: &Birth, reply: StatusReply) -> ServedNode {
    serve_node_with(b, move |_| reply.clone()).await
}

/// A birth serving Status that records every request and answers each with what `answer` says.
async fn serve_node_with(b: &Birth, answer: impl Fn(&StatusRequest) -> StatusReply + Send + Sync + 'static) -> ServedNode {
    use rafka_node_rpc_contract::catalog::OpOwner;
    let seen: Arc<Mutex<Vec<StatusRequest>>> = Arc::default();
    let heard: Arc<tokio::sync::Notify> = Arc::default();
    let (s, h) = (seen.clone(), heard.clone());
    let answer = Arc::new(answer);
    let server = ServerBuilder::new()
        .serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
            let (s, h, answer) = (s.clone(), h.clone(), answer.clone());
            async move {
                let reply = answer(&req);
                s.lock().unwrap().push(req);
                h.notify_one();
                Ok(reply)
            }
        })
        .seal(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().0 })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id: b.node.node_id.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    ServedNode { resolved, seen, heard, _router: router }
}

/// mesh1's primary's config door, telling births through a client that resolves `node`.
async fn configs_door(rig: &Rig, node: &ServedNode) {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(node.resolved.clone());
    let ep = rafka_node_rpc::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _ = rig.authority.configs.set(Arc::new(rafka_node_admin_core::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), rig.authority.configure.clone(), Default::default())));
}

fn exact(b: &Birth) -> rafka_node_admin_client::ExactBirth {
    rafka_node_admin_client::ExactBirth { target: NodeTarget::ExactNode(b.node.node_id.clone()), node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap() }
}

/// CONTRACT (ops-naming "Node data": a config update is a single call, not a Build; the node's
/// mesh-primary is the only writer and writes the whole row): a caller's `node.config.update` to
/// mesh1's primary ends `node.config.updated` (`ConfigUpdated` naming the exact birth); the node's
/// record at its path.name holds the config; the birth was told the whole map exactly once
/// (`ApplyNodeConfig`) before the mesh-primary answered; and one `rdm.node_admin.node.update.via-config`
/// span says `config-updated`.
#[test]
fn a_config_update_at_the_mesh_primary_ends_updated_and_the_node_carries_it() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let node = serve_node(rpc1).await;
        configs_door(&rig, &node).await;
        let config: BTreeMap<String, String> = [("tags".to_string(), "edge,blue".to_string()), ("zone".to_string(), "a".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rafka_node_admin_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rafka_node_rpc::RouteChoice::Direct, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap() });
        let row = rig.nodes.node_row_at(&rpc1.node.name).await.unwrap().expect("the mesh-primary wrote the node's row");
        assert_eq!((row.node_id.clone(), row.config.clone()), (rpc1.node.node_id.clone(), config.clone()), "the row at mesh1.rpc.1 is this node with the whole config");
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config }],
            "the birth was told the whole config once, before the update was answered"
        );
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(attrs.get("outcome").map(String::as_str), Some("config-updated"), "{attrs:?}");
    assert_eq!(attrs.get("subject").map(String::as_str), Some("mesh1.rpc.1"), "{attrs:?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config subject=mesh1.rpc.1 outcome=config-updated");
}

/// CONTRACT (ops-naming "Node data": config is capped in key count and bytes, refused by name past
/// the cap): a config of more keys than the cap is refused `RejectedConfig` naming the keys, the bytes
/// and both bounds; nothing is told to the node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_config_over_its_cap_is_refused_by_name_and_nothing_is_applied() {
    let (rig, b) = rig().await;
    let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
    let node = serve_node(rpc1).await;
    configs_door(&rig, &node).await;
    let keys = rafka_mesh_entity::MAX_CONFIG_KEYS + 1;
    let config: BTreeMap<String, String> = (0..keys).map(|i| (format!("k{i:02}"), "v".to_string())).collect();
    let bytes: usize = config.iter().map(|(k, v)| k.len() + v.len()).sum();
    let client = client_of(&rig, admin2).await;
    let reply = rafka_node_admin_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rafka_node_rpc::RouteChoice::Direct, &exact(rpc1), config, &CallOptions::default()).await.expect("a typed refusal is a reply");
    assert_eq!(
        reply,
        StatusReply::RejectedConfig { over: rafka_mesh_entity::ConfigOverCap { keys: keys as u32, bytes: bytes as u32, max_keys: rafka_mesh_entity::MAX_CONFIG_KEYS as u32, max_bytes: rafka_mesh_entity::MAX_CONFIG_BYTES as u32 } }
    );
    assert!(node.seen.lock().unwrap().is_empty(), "nothing is told to the node");
}

/// A nodes store that refuses every node-row write (put, claim, release), by name; every other row is
/// the memory store's.
#[derive(Default)]
struct RefusingRows(MemoryNodesStorage);

#[async_trait::async_trait]
impl NodesStorage for RefusingRows {
    async fn put_runtime(&self, row: &rafka_node_admin_core::storage::RuntimeRow) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        self.0.put_runtime(row).await
    }
    async fn runtimes(&self) -> Result<Vec<rafka_node_admin_core::storage::RuntimeRow>, rafka_node_admin_core::record_store::StorageError> {
        self.0.runtimes().await
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &rafka_mesh_entity::IncarnationId) -> Result<Option<rafka_node_admin_core::storage::RuntimeRow>, rafka_node_admin_core::record_store::StorageError> {
        self.0.runtime(node_id, incarnation).await
    }
    async fn own(&self) -> Result<Option<rafka_node_admin_core::storage::NodeRecord>, rafka_node_admin_core::record_store::StorageError> {
        self.0.own().await
    }
    async fn put_own(&self, record: &rafka_node_admin_core::storage::NodeRecord) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        self.0.put_own(record).await
    }
    async fn contacts(&self) -> Result<Vec<rafka_node_admin_core::storage::NodeRecord>, rafka_node_admin_core::record_store::StorageError> {
        self.0.contacts().await
    }
    async fn put_contact(&self, record: &rafka_node_admin_core::storage::NodeRecord) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        self.0.put_contact(record).await
    }
    async fn remove_contact(&self, mesh: &str, node_id: &rafka_mesh_entity::NodeId) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        self.0.remove_contact(mesh, node_id).await
    }
    async fn put_node_row(&self, row: &rafka_node_admin_core::storage::NodeRow) -> Result<Result<(), rafka_node_admin_core::storage::NameHeld>, rafka_node_admin_core::record_store::StorageError> {
        Err(rafka_node_admin_core::record_store::StorageError::Io { file: format!("nodes/rows/{}", row.name), reason: "disk refused the write".into() })
    }
    async fn node_row_at(&self, name: &rafka_node_admin_core::model::PathName) -> Result<Option<rafka_node_admin_core::storage::NodeRow>, rafka_node_admin_core::record_store::StorageError> {
        self.0.node_row_at(name).await
    }
    async fn release_node_row(&self, name: &rafka_node_admin_core::model::PathName, _holder: &rafka_node_admin_core::model::NodeId) -> Result<bool, rafka_node_admin_core::record_store::StorageError> {
        Err(rafka_node_admin_core::record_store::StorageError::Io { file: format!("nodes/rows/{name}"), reason: "disk refused the release".into() })
    }
}

/// CONTRACT (ops-naming "Node data": the mesh-primary writes the record, then the node applies it):
/// when the birth does not apply the config, the update ends `ConfigNotApplied`, naming the node and
/// the birth's answer by name; the row was written first, so the node's record at its path.name holds
/// the new config. The mesh-primary is ready; the birth refused. The birth's answer here is longer than
/// the reply's reason budget, so the reply carries it cut (ending `…`) and the via-config span carries
/// it whole: nothing of the refusal is lost.
#[test]
fn a_config_the_birth_does_not_apply_ends_not_applied_with_the_row_written() {
    let cap = capture();
    let cut = cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let refusal = format!("hydration has not passed: {}", "the store of mesh1.rpc.1 is still behind; ".repeat(20));
        let node = serve_node_answering(rpc1, StatusReply::NotReady { reason: refusal.clone() }).await;
        configs_door(&rig, &node).await;
        let config: BTreeMap<String, String> = [("zone".to_string(), "b".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rafka_node_admin_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rafka_node_rpc::RouteChoice::Direct, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        let StatusReply::ConfigNotApplied { node: named, answered, reason } = &reply else { panic!("the row is written and the birth refused: {reply:?}") };
        assert_eq!((named.as_str(), answered.as_str()), ("mesh1.rpc.1", "not-ready"), "{reply:?}");
        assert!(reason.ends_with('…'), "the birth's long answer is cut to the reply's budget: {reason}");
        assert_eq!(node.seen.lock().unwrap().len(), 1, "the birth was told once");
        let row = rig.nodes.node_row_at(&rpc1.node.name).await.unwrap().expect("the mesh-primary wrote the row before telling the birth");
        assert_eq!(row.config, config);
        (reason.clone(), refusal)
    });
    let (cut, refusal) = cut;
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(attrs.get("outcome").map(String::as_str), Some("config-not-applied"), "{spans:?}");
    assert_eq!(attrs.get("answered").map(String::as_str), Some("not-ready"), "{attrs:?}");
    let detail = attrs.get("detail").expect("the via-config span carries the refusal's detail");
    assert!(detail.len() > rafka_node_rpc_contract::status::MAX_REFUSAL_REASON_BYTES && !detail.ends_with('…'), "the span's detail is whole: {detail}");
    assert!(detail.starts_with(cut.trim_end_matches('…')) && detail.contains(&refusal), "the reply's cut detail is the head of the span's whole one: {detail}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config outcome=config-not-applied answered=not-ready detail={} bytes (reply reason {} bytes, cut)", detail.len(), cut.len());
}

/// CONTRACT (ops-naming "Node data": the mesh-primary writes the record, then tells the node): when
/// the nodes store refuses the row, the update ends `ConfigRowRefused` naming the node and the store's
/// refusal, and nothing is told to the birth.
#[test]
fn a_row_the_store_refuses_ends_row_refused_and_nothing_is_told() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let rpc1 = &b["rpc1"];
        let node = serve_node(rpc1).await;
        configs_door(&rig, &node).await;
        let view = rig.topology.read().await.clone();
        let req = StatusRequest::UpdateNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config: [("zone".to_string(), "c".to_string())].into() };
        let reply = rig.authority.configs.get().unwrap().update(&rig.admin.node.name, &rig.admin.node.node_id, &view, &RefusingRows::default(), &req).await;
        assert!(
            matches!(&reply, StatusReply::ConfigRowRefused { node, reason } if node == "mesh1.rpc.1" && reason.contains("disk refused the write")),
            "the store refused the row: {reply:?}"
        );
        assert!(node.seen.lock().unwrap().is_empty(), "nothing is told to the birth when its row was not written");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    assert_eq!(spans[0].1.get("outcome").map(String::as_str), Some("config-row-refused"), "{spans:?}");
    assert!(spans[0].1.get("detail").is_some_and(|d| d.contains("disk refused the write") && !d.ends_with('…')), "the via-config span carries the store's whole refusal: {spans:?}");
    eprintln!("trace {}: rdm.node_admin.node.update.via-config outcome=config-row-refused", spans[0].0);
}

/// A door at the admin keyed `key`, telling births through a client that resolves only `node`.
async fn door_at(key: &SecretKey, node: &ServedNode) -> rafka_node_admin_core::node_config::NodeConfigs {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(node.resolved.clone());
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    rafka_node_admin_core::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), Default::default(), Default::default())
}

/// CONTRACT (ops-naming "Node data" :78-79, fabric-node-lifecycle §4.3: the mesh-primary of a node's
/// mesh is the only writer of that mesh's rows; caller → the node's mesh-primary, which writes the
/// record → the node): a config update for mesh2.rpc.1 sent to the fabric-primary (mesh1's primary)
/// is refused by name, `ReceiverNotPrimary` naming mesh2's primary node-admin; no row is written and
/// nothing is told. The same update at mesh2's primary writes the row and tells the birth directly,
/// once, and ends `ConfigUpdated`. Two via-config spans: `rejected-not-authority` at mesh1.admin.1,
/// `config-updated` at mesh2.admin.1.
#[test]
fn a_config_update_is_refused_by_name_at_the_fabric_primary_and_written_by_the_nodes_mesh_primary() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, m2admin) = (&b["admin2"], &b["m2admin"]);
        let m2rpc = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
        rig.topology.write().await.nodes.push(m2rpc.node.clone());
        let node = serve_node(&m2rpc).await;
        let _ = rig.authority.configs.set(Arc::new(door_at(&rig.admin.key, &node).await));
        let config: BTreeMap<String, String> = [("tags".to_string(), "far".to_string())].into();
        let req = StatusRequest::UpdateNodeConfig { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap(), config: config.clone() };
        // At the fabric-primary, over the wire: refused by name.
        let refused = reply(&call(&rig, admin2, &req).await);
        let StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed } } = &refused else { panic!("the fabric-primary is not mesh2's writer: {refused:?}") };
        assert!(needed.contains("mesh2") && needed.contains("mesh2.admin.1"), "the refusal names mesh2's primary node-admin: {needed}");
        assert!(rig.nodes.node_row_at(&m2rpc.node.name).await.unwrap().is_none(), "the fabric-primary wrote no row of mesh2");
        assert!(node.seen.lock().unwrap().is_empty(), "nothing is told by an admin that is not the writer");
        // At mesh2's primary: the row is written, then the birth is told directly.
        let mp_rows = MemoryNodesStorage::default();
        let door = door_at(&m2admin.key, &node).await;
        let view = rig.topology.read().await.clone();
        let updated = door.update(&m2admin.node.name, &m2admin.node.node_id, &view, &mp_rows, &req).await;
        assert_eq!(updated, StatusReply::ConfigUpdated { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap() });
        let row = mp_rows.node_row_at(&m2rpc.node.name).await.unwrap().expect("mesh2's primary wrote the row");
        assert_eq!((row.node_id.clone(), row.config.clone()), (m2rpc.node.node_id.clone(), config.clone()));
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap(), config }],
            "the birth was told once, by its mesh's primary"
        );
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 2, "{spans:?}");
    let at = |n: &str| spans.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some(n)).unwrap_or_else(|| panic!("a via-config span at {n}: {spans:?}"));
    let (fp_trace, fp) = at("mesh1.admin.1");
    assert_eq!(fp.get("outcome").map(String::as_str), Some("rejected-not-authority"), "{fp:?}");
    let (mp_trace, mp) = at("mesh2.admin.1");
    assert_eq!(mp.get("outcome").map(String::as_str), Some("config-updated"), "{mp:?}");
    assert_eq!(mp.get("subject").map(String::as_str), Some("mesh2.rpc.1"), "{mp:?}");
    eprintln!("trace {fp_trace}: rdm.node_admin.node.update.via-config node=mesh1.admin.1 subject=mesh2.rpc.1 outcome=rejected-not-authority");
    eprintln!("trace {mp_trace}: rdm.node_admin.node.update.via-config node=mesh2.admin.1 subject=mesh2.rpc.1 outcome=config-updated");
}

/// CONTRACT (ops-naming "Node data" :79: caller → the node's mesh-primary, Forward `0x1A` when not
/// directly reachable): a caller that resolves only a carrier (mesh2's primary) reaches mesh1's
/// primary through it, and mesh1's primary, which here does NOT hold the fabric seat, writes
/// mesh1.rpc.1's row and tells it: `ConfigUpdated`, the birth told once, and the caller's
/// `via-connections` span names `via-peer` with carrier mesh2.admin.1 inside the via-config trace.
#[test]
fn a_caller_reaches_the_nodes_mesh_primary_through_a_carrier_and_it_writes_and_applies() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1, rpc2, m2admin) = (&b["admin2"], &b["rpc1"], &b["rpc2"], &b["m2admin"]);
        // The fabric seat is mesh1.admin.2's in this view: the receiver is mesh1's primary only.
        for n in rig.topology.write().await.nodes.iter_mut() {
            n.is_fabric_primary = n.node_id == admin2.node.node_id;
        }
        let node = serve_node(rpc1).await;
        configs_door(&rig, &node).await;
        let (carrier, _carrier_router) = carrier_admin(m2admin, &rig.resolved).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(carrier);
        let ep = rafka_node_rpc::endpoint::bind(rpc2.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
        let config: BTreeMap<String, String> = [("zone".to_string(), "far".to_string())].into();
        let route = rafka_node_rpc::RouteChoice::ViaPeer { carrier: m2admin.node.node_id.clone(), path: m2admin.node.name.clone() };
        let reply = rafka_node_admin_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &route, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap() });
        let row = rig.nodes.node_row_at(&rpc1.node.name).await.unwrap().expect("mesh1's primary wrote the row");
        assert_eq!(row.config, config);
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config }],
            "the birth was told once, by its mesh's primary"
        );
    });
    let config_spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(config_spans.len(), 1, "{config_spans:?}");
    let (trace_id, attrs) = &config_spans[0];
    assert_eq!(attrs.get("outcome").map(String::as_str), Some("config-updated"), "{attrs:?}");
    assert_eq!(attrs.get("node").map(String::as_str), Some("mesh1.admin.1"), "{attrs:?}");
    let routes = cap.spans("rdm.node_rpc.route.resolve.via-connections");
    let carried: Vec<_> = routes.iter().filter(|(t, a)| t == trace_id && a.get("route").map(String::as_str) == Some("via-peer")).collect();
    assert_eq!(carried.len(), 1, "the caller's one carried leg is in the via-config trace: {routes:?}");
    assert_eq!(carried[0].1.get("carrier").map(String::as_str), Some("mesh2.admin.1"));
    assert_eq!(carried[0].1.get("protocol").map(String::as_str), Some("status"));
    eprintln!("trace {trace_id}: rdm.node_rpc.route.resolve.via-connections route=via-peer carrier=mesh2.admin.1 > rdm.node_admin.node.update.via-config node=mesh1.admin.1 outcome=config-updated");
}

/// CONTRACT (ops-naming "Node data" :78-79: the mesh-primary writes its mesh's rows, its own
/// included, and the node applies the config; a node-admin is a node like any other): a config
/// update naming mesh1's primary itself, sent to it, writes its row and the admin applies the
/// config in-process (what holds it receives the whole map once) before `ConfigUpdated` is
/// answered; no call to itself is made. The via-config span says `config-updated`, `apply=in-process`.
#[test]
fn a_config_update_naming_the_mesh_primary_itself_is_written_and_applied_by_it() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let held: Arc<Mutex<Vec<BTreeMap<String, String>>>> = Arc::default();
        let h = held.clone();
        let _ = rig.authority.configure.set(Arc::new(move |config: BTreeMap<String, String>| {
            let h = h.clone();
            Box::pin(async move { h.lock().unwrap().push(config) })
        }));
        let node = serve_node(rpc1).await;
        configs_door(&rig, &node).await;
        let me = &rig.admin.node;
        let config: BTreeMap<String, String> = [("admin_api_base".to_string(), "http://127.0.0.1:9".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rafka_node_admin_client::NodeRpc::new(&client).config_update(&me.node_id, &rafka_node_rpc::RouteChoice::Direct, &exact(&rig.admin), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: me.node_id.clone(), incarnation: me.incarnation_id.clone().unwrap() });
        let row = rig.nodes.node_row_at(&me.name).await.unwrap().expect("mesh1's primary wrote its own row");
        assert_eq!((row.node_id.clone(), row.config.clone()), (me.node_id.clone(), config.clone()));
        assert_eq!(held.lock().unwrap().clone(), vec![config], "the admin holds the whole config, once");
        assert!(node.seen.lock().unwrap().is_empty(), "no other node is told");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(attrs.get("outcome").map(String::as_str), Some("config-updated"), "{attrs:?}");
    assert_eq!(attrs.get("subject").map(String::as_str), Some("mesh1.admin.1"), "{attrs:?}");
    assert_eq!(attrs.get("apply").map(String::as_str), Some("in-process"), "{attrs:?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config node=mesh1.admin.1 subject=mesh1.admin.1 apply=in-process outcome=config-updated");
}

/// The spans a cell's own run exported through the product span pipeline
/// (`rafka_mesh_telemetry::span_capture`), with their trace ids.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rafka_mesh_telemetry::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture("status-declarations", exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

impl Capture {
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

    /// Every finished span called `name`: its trace id, its attributes and its events (each event's
    /// message and fields), in the order they happened.
    fn spans_with_events(&self, name: &str) -> Vec<(String, BTreeMap<String, String>, Vec<String>)> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| {
                let events = s.events.iter().map(|e| format!("{} {:?}", e.name, e.attributes.iter().map(|kv| format!("{}={}", kv.key, kv.value)).collect::<Vec<_>>())).collect();
                (s.span_context.trace_id().to_string(), s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect(), events)
            })
            .collect()
    }

    /// Every finished span called `name`: its trace id and its attributes.
    fn spans(&self, name: &str) -> Vec<(String, BTreeMap<String, String>)> {
        self.capture.flush(std::time::Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| (s.span_context.trace_id().to_string(), s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect()))
            .collect()
    }
}

/// mesh2's primary admin as a carrier: the admin composition (`admin::rpc_server`) on its own key,
/// its live resolver holding `target`, the birth it carries to.
async fn carrier_admin(b: &Birth, target: &ResolvedNode) -> (ResolvedNode, Router) {
    use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionsHeld};
    let ep = rafka_node_rpc::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id: b.node.node_id.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    let live = Arc::new(rafka_node_rpc::LiveNodeResolver::default());
    live.apply(target.clone(), None);
    let connections = Arc::new(rafka_node_admin_core::connections_writer::ConnectionsWriter::new(
        ConnectionEnd { name: resolved.name.clone(), node_id: resolved.node_id.clone(), incarnation: Some(resolved.incarnation.clone()) },
        Arc::new(rafka_node_admin_core::storage::MemoryConnectionsStorage::default()),
        Arc::new(Mutex::new(ConnectionsHeld::new())),
    ));
    connections.held().lock().unwrap().set_membership(live.clone());
    connections.hydrate().await.unwrap();
    let node_rpc = rafka_node_admin_core::node_rpc::ProcessNodeRpc::new(live.clone(), ep.clone(), Some(connections.clone()));
    let server = rafka_node_admin_core::admin::rpc_server(live, connections, node_rpc.client.clone(), Arc::new(OnceLock::new()), Arc::new(OnceLock::new()), Arc::new(OnceLock::new()))
        .seal(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().0 })
        .unwrap();
    (resolved, Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn())
}

/// CONTRACT (ops-naming "Node data": the node applies the config and carries it in its next digest; a
/// node-admin is a node like any other): `ApplyNodeConfig` naming this admin is answered by the admin
/// itself, whoever sends it (node-rpc.md §14): `Applied`, after it handed the whole map to what holds
/// it and publishes its digest, from another node-admin and from a member alike. One naming another
/// incarnation of it is `RejectedStaleIncarnation` with the one held, and holds nothing.
#[test]
fn a_node_admin_told_its_own_config_holds_it_and_answers_applied() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let held: Arc<Mutex<Vec<BTreeMap<String, String>>>> = Arc::default();
        let h = held.clone();
        let _ = rig.authority.configure.set(Arc::new(move |config: BTreeMap<String, String>| {
            let h = h.clone();
            Box::pin(async move { h.lock().unwrap().push(config) })
        }));
        let config: BTreeMap<String, String> = [("admin_api_base".to_string(), "http://127.0.0.1:9".to_string())].into();
        let me = &rig.admin.node;
        let told = |incarnation: IncarnationId, config: BTreeMap<String, String>| StatusRequest::ApplyNodeConfig { node_id: me.node_id.clone(), incarnation, config };
        assert_eq!(reply(&call(&rig, admin2, &told(me.incarnation_id.clone().unwrap(), config.clone())).await), StatusReply::Applied);
        assert_eq!(held.lock().unwrap().clone(), vec![config.clone()], "the admin holds the whole config it was told");
        assert_eq!(reply(&call(&rig, admin2, &told(IncarnationId::mint(), config.clone())).await), StatusReply::RejectedStaleIncarnation { held: me.incarnation_id.clone().unwrap() });
        assert_eq!(reply(&call(&rig, rpc1, &told(me.incarnation_id.clone().unwrap(), config.clone())).await), StatusReply::Applied, "the subject answers whoever sends it");
        assert_eq!(held.lock().unwrap().clone(), vec![config.clone(), config], "the refusal held nothing; each apply held the whole config");
    });
    for name in ["rdm.node_rpc.request.update.via-call", "rdm.node_rpc.connection.create.via-dial"] {
        for (trace, attrs, events) in cap.spans_with_events(name) {
            eprintln!("trace {trace} {name} {attrs:?}");
            for e in events {
                eprintln!("    {e}");
            }
        }
    }
}

/// CONTRACT (a restart is the same key in a new process: node-lifecycle restart vocabulary, same
/// key and path.name, new socket): a sender whose endpoint ended without a close, so the authority
/// still holds its old connection, reaches the authority from a new endpoint bound on the same key:
/// its first call dials and is answered, a second call on that pooled connection is answered after
/// the authority took the new connection in, and the same holds for a second re-bind while two old
/// connections are held. Every call is a reply within the default budget; none ends `NotSent`.
#[test]
fn a_sender_rebound_on_its_key_while_its_old_connection_is_held_reaches_the_authority() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let admin2 = &b["admin2"];
        let me = &rig.admin.node;
        let fp = NodeTarget::ExactNode(me.node_id.clone());
        // A told config naming another birth of the authority: answered by name, nothing held.
        let stale = StatusRequest::ApplyNodeConfig { node_id: me.node_id.clone(), incarnation: IncarnationId::mint(), config: BTreeMap::new() };
        let first = client_of(&rig, admin2).await;
        assert!(matches!(first.call::<Status>(&fp, &stale, &CallOptions::default()).await.0, RpcOutcome::Reply(_)), "the first endpoint is answered");
        // The process ends without a close: the authority keeps the old connection until it idles out.
        drop(first);
        for rebind in 1..=2 {
            let next = client_of(&rig, admin2).await;
            for call in ["dialed", "pooled"] {
                let out = next.call::<Status>(&fp, &stale, &CallOptions::default()).await.0;
                assert!(matches!(out, RpcOutcome::Reply(_)), "re-bind {rebind}, {call} call: {out:?}");
            }
            drop(next);
        }
    });
    let calls = cap.spans("rdm.node_rpc.request.update.via-call");
    assert_eq!(calls.len(), 5, "{calls:?}");
    assert!(calls.iter().all(|(_, a)| a.get("outcome").map(String::as_str) == Some("Reply")), "{calls:?}");
    let dials = cap.spans("rdm.node_rpc.connection.create.via-dial");
    let locals: std::collections::BTreeSet<_> = dials.iter().filter_map(|(_, a)| a.get("local").cloned()).collect();
    assert_eq!((dials.len(), locals.len()), (3, 3), "one dial per endpoint, each from its own socket: {dials:?}");
    assert!(dials.iter().all(|(_, a)| a.get("outcome").map(String::as_str) == Some("connected")), "{dials:?}");
    for name in ["rdm.node_rpc.request.update.via-call", "rdm.node_rpc.connection.create.via-dial"] {
        for (trace, attrs, events) in cap.spans_with_events(name) {
            eprintln!("trace {trace} {name} outcome={:?} reason={:?} elapsed_ms={:?} local={:?}", attrs.get("outcome"), attrs.get("reason"), attrs.get("elapsed_ms"), attrs.get("local"));
            for e in events.iter().filter(|e| e.contains("step=")) {
                eprintln!("    {e}");
            }
        }
    }
}

/// CONTRACT (ops-naming "Node data": a node created at a path.name seeds its config from the
/// previous row at that path.name, and the node applies it and carries it): a birth that enters the
/// fabric-primary's view at a `Deleted` row's path.name claims its row seeded with that row's config,
/// and the fabric-primary tells the birth that config through its config door (`ApplyNodeConfig`,
/// the whole map, naming the exact birth), so the node holds it in its own answer.
#[test]
fn a_node_created_at_a_deleted_rows_name_is_told_its_seeded_config() {
    let cap = capture();
    let trace = cap.run(async {
        let (rig, _b) = rig().await;
        let newcomer = birth("mesh1.rpc.3", NodeKind::RpcNode, "mesh1", false, false);
        let seeded: BTreeMap<String, String> = [("zone".to_string(), "b".to_string()), ("tags".to_string(), "edge".to_string())].into();
        rig.nodes
            .put_node_row(&rafka_node_admin_core::storage::NodeRow { name: newcomer.node.name.clone(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: seeded.clone() })
            .await
            .unwrap()
            .unwrap();
        let node = serve_node(&newcomer).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(node.resolved.clone());
        let ep = rafka_node_rpc::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let configs: rafka_node_admin_core::node_config::NodeConfigsSlot = Arc::new(OnceLock::new());
        let _ = configs.set(Arc::new(rafka_node_admin_core::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), Default::default(), Default::default())));
        let records = rafka_node_admin_core::admin::Records::default();
        records.set_rows(Arc::new(rafka_node_admin_core::node_rows::NodeRows::new(rig.admin.node.name.clone(), rig.admin.node.node_id.clone(), rig.topology.clone(), rig.nodes.clone()).with_configs(configs)));
        let mut t = rig.topology.read().await.clone();
        t.nodes.push(newcomer.node.clone());
        records.install_projected(&rig.topology, || t.clone()).await;
        tokio::time::timeout(Duration::from_secs(10), node.heard.notified()).await.expect("the fabric-primary told the birth its seeded config");
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: newcomer.node.node_id.clone(), incarnation: newcomer.node.incarnation_id.clone().unwrap(), config: seeded.clone() }],
            "the birth was told the Deleted row's whole config, once"
        );
        let row = rig.nodes.node_row_at(&newcomer.node.name).await.unwrap().expect("the birth's row");
        assert_eq!((row.node_id, row.config), (newcomer.node.node_id.clone(), seeded), "the birth holds the row with the seeded config");
    });
    let _ = trace;
    let spans = cap.spans("rdm.node_admin.node.create.via-node-row");
    let ours: Vec<_> = spans.iter().filter(|(_, a)| a.get("subject").map(String::as_str) == Some("mesh1.rpc.3")).collect();
    assert_eq!(ours.len(), 1, "{spans:?}");
    assert_eq!(ours[0].1.get("outcome").map(String::as_str), Some("row-seeded"), "{ours:?}");
    eprintln!("trace {}: rdm.node_admin.node.create.via-node-row subject=mesh1.rpc.3 outcome=row-seeded, then ApplyNodeConfig to the birth", ours[0].0);
}

/// CONTRACT (ops-naming "Node data", node-recovery.md "Retained": config seeded from the Deleted
/// row; R13: the mesh-primary of a node's mesh is the only writer of that mesh's rows): in a mesh
/// whose primary does not hold the fabric seat, a birth entering mesh2's primary's view at a
/// `Deleted` row's path.name claims its row seeded with that row's config, written by mesh2's
/// primary, and mesh2's primary tells the birth that config through its own config door
/// (`ApplyNodeConfig`, the whole map, the exact birth, once). Spans: `via-node-row` at
/// mesh2.admin.1 `row-seeded`, then `via-config` at mesh2.admin.1 `config-updated` in its trace.
#[test]
fn a_node_created_at_a_deleted_rows_name_in_a_mesh_whose_primary_is_not_the_fabric_primary_is_told_its_seeded_config_by_that_primary() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let m2admin = &b["m2admin"];
        assert!(!m2admin.node.is_fabric_primary && m2admin.node.is_primary, "mesh2's primary does not hold the fabric seat");
        let newcomer = birth("mesh2.rpc.2", NodeKind::RpcNode, "mesh2", false, false);
        let seeded: BTreeMap<String, String> = [("zone".to_string(), "c".to_string()), ("tags".to_string(), "far".to_string())].into();
        let mp_rows = Arc::new(MemoryNodesStorage::default());
        mp_rows
            .put_node_row(&rafka_node_admin_core::storage::NodeRow { name: newcomer.node.name.clone(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: seeded.clone() })
            .await
            .unwrap()
            .unwrap();
        let node = serve_node(&newcomer).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(node.resolved.clone());
        let ep = rafka_node_rpc::endpoint::bind(m2admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let configs: rafka_node_admin_core::node_config::NodeConfigsSlot = Arc::new(OnceLock::new());
        let _ = configs.set(Arc::new(rafka_node_admin_core::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), Default::default(), Default::default())));
        let records = rafka_node_admin_core::admin::Records::default();
        records.set_rows(Arc::new(rafka_node_admin_core::node_rows::NodeRows::new(m2admin.node.name.clone(), m2admin.node.node_id.clone(), rig.topology.clone(), mp_rows.clone()).with_configs(configs)));
        let mut t = rig.topology.read().await.clone();
        t.nodes.push(newcomer.node.clone());
        records.install_projected(&rig.topology, || t.clone()).await;
        tokio::time::timeout(Duration::from_secs(10), node.heard.notified()).await.expect("mesh2's primary told the birth its seeded config");
        // The birth hears the call before mesh2's primary reads its answer: the door's tell runs on
        // its own task, so the cell keeps its runtime until that door's span has ended with the
        // answer, or the runtime's end would cancel it mid-call.
        let answered = || cap.spans("rdm.node_admin.node.update.via-config").iter().any(|(_, a)| a.get("subject").map(String::as_str) == Some("mesh2.rpc.2") && a.contains_key("outcome"));
        tokio::time::timeout(Duration::from_secs(10), async {
            while !answered() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("mesh2's primary's config door answered");
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: newcomer.node.node_id.clone(), incarnation: newcomer.node.incarnation_id.clone().unwrap(), config: seeded.clone() }],
            "the birth was told the Deleted row's whole config, once, by its mesh's primary"
        );
        let row = mp_rows.node_row_at(&newcomer.node.name).await.unwrap().expect("the birth's row");
        assert_eq!((row.node_id, row.config), (newcomer.node.node_id.clone(), seeded), "mesh2's primary wrote the birth's row with the seeded config");
        assert!(rig.nodes.node_row_at(&newcomer.node.name).await.unwrap().is_none(), "the fabric-primary wrote no row of mesh2");
    });
    let rows = cap.spans("rdm.node_admin.node.create.via-node-row");
    let ours: Vec<_> = rows.iter().filter(|(_, a)| a.get("subject").map(String::as_str) == Some("mesh2.rpc.2")).collect();
    assert_eq!(ours.len(), 1, "{rows:?}");
    assert_eq!(ours[0].1.get("node").map(String::as_str), Some("mesh2.admin.1"), "{ours:?}");
    assert_eq!(ours[0].1.get("outcome").map(String::as_str), Some("row-seeded"), "{ours:?}");
    let trace = &ours[0].0;
    let told = cap.spans("rdm.node_admin.node.update.via-config");
    let told: Vec<_> = told.iter().filter(|(t, _)| t == trace).collect();
    assert_eq!(told.len(), 1, "one via-config span in the row write's trace: {told:?}");
    assert_eq!(told[0].1.get("node").map(String::as_str), Some("mesh2.admin.1"), "{told:?}");
    assert_eq!(told[0].1.get("outcome").map(String::as_str), Some("config-updated"), "{told:?}");
    eprintln!("trace {trace}: rdm.node_admin.node.create.via-node-row node=mesh2.admin.1 subject=mesh2.rpc.2 outcome=row-seeded > rdm.node_admin.node.update.via-config node=mesh2.admin.1 outcome=config-updated");
}

/// A successor born `mesh1.rpc.3.new`, live in the mesh-primary's view, serving Status that answers
/// every request `Applied` and records it; the mesh-primary's node-RPC client resolves it.
async fn successor_new(rig: &Rig) -> (Birth, ServedNode) {
    let b = birth("mesh1.rpc.3.new", NodeKind::RpcNode, "mesh1", false, false);
    rig.topology.write().await.nodes.push(b.node.clone());
    let node = serve_node(&b).await;
    configs_door(rig, &node).await;
    (b, node)
}

/// [`successor_new`], drained (Draining in the mesh-primary's view, its declared state Draining),
/// answering `node.connections.delete` with every connection cut and nothing held, and every other
/// request `Applied`.
async fn successor_drained(rig: &Rig) -> (Birth, ServedNode) {
    let mut b = birth("mesh1.rpc.3.new", NodeKind::RpcNode, "mesh1", false, false);
    (b.node.status, b.node.declared) = (NodeStatus::Draining, Some("Draining".into()));
    rig.topology.write().await.nodes.push(b.node.clone());
    let node = serve_node_with(&b, |req| match req {
        StatusRequest::DeleteConnections { .. } => StatusReply::ConnectionsDeleted { closed: vec![rafka_mesh_entity::ConnectionId::mint()], not_open: vec![], held: vec![] },
        _ => StatusReply::Applied,
    })
    .await;
    configs_door(rig, &node).await;
    (b, node)
}

fn delete_all(b: &Birth) -> StatusRequest {
    StatusRequest::DeleteConnections { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), scope: rafka_node_rpc_contract::status::ConnectionScope::All }
}

fn update_node(b: &Birth, name: &str) -> StatusRequest {
    StatusRequest::UpdateNode { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), name: name.into() }
}

/// CONTRACT (R-CR1a, R-CR2, rulings 2026-10-11: a node changes its name only when it is not
/// ready-for-traffic and holds no connection, stopped or drained with `node.connections.deleted`; the
/// node's mesh-primary refuses any other rename by name): a caller's `UpdateNode` renaming the live,
/// ready-for-traffic successor `mesh1.rpc.3.new` to `mesh1.rpc.3` at mesh1's primary is refused `Busy`
/// naming the held status and that the mesh-primary holds no `node.connections.deleted` for the birth.
/// Nothing is claimed (no row at `mesh1.rpc.3`), the view keeps the birth at `.new`, and the birth is
/// told nothing. One `rdm.node_admin.node.update.via-update-node` span says `busy` with
/// `traffic=ReadyForTraffic` and `connections=none-deleted`. What must NOT happen: the row written,
/// the birth told `ApplyNodeName`, or `NodeUpdated`.
#[test]
fn a_ready_for_traffic_nodes_rename_is_refused_by_name_at_the_mesh_primary_and_nothing_is_claimed_or_told() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_new(&rig).await;
        let reply = reply(&call(&rig, &b["admin2"], &update_node(&succ, "mesh1.rpc.3")).await);
        assert!(
            matches!(&reply, StatusReply::Busy { reason } if reason.contains("ReadyForTraffic") && reason.contains("no node.connections.deleted")),
            "a ready-for-traffic node keeps its name, refused naming its held status and the missing node.connections.deleted: {reply:?}"
        );
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        assert_eq!(rig.nodes.node_row_at(&taken).await.unwrap(), None, "nothing is claimed at the new name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == succ.node.node_id && n.name == succ.node.name), "the view keeps the birth at its name");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-update-node");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(
        (attrs.get("outcome").map(String::as_str), attrs.get("traffic").map(String::as_str), attrs.get("connections").map(String::as_str), attrs.get("answered")),
        (Some("busy"), Some("ReadyForTraffic"), Some("none-deleted"), None),
        "{attrs:?}"
    );
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-update-node from=mesh1.rpc.3.new to=mesh1.rpc.3 traffic=ReadyForTraffic connections=none-deleted outcome=busy");
}

/// CONTRACT (R-CR1a, R-CR2: drained, then `node.connections.delete` has completed): the successor
/// `mesh1.rpc.3.new`, drained (Draining in the mesh-primary's view, its declared state Draining), is
/// refused the rename `Busy` naming its held status and that no `node.connections.deleted` is held for
/// the birth, nothing claimed and nothing told. A caller's `node.connections.delete { All }` through
/// the mesh-primary is carried to the exact birth and answered with the birth's
/// `node.connections.deleted`, which the mesh-primary holds; the same `node.update` then writes the
/// row at `mesh1.rpc.3` and the birth is told `ApplyNodeName`, ending `NodeUpdated`. A start begins
/// the birth's next cycle: the receipt is not carried over, and a rename after it is refused again.
/// What must NOT happen: a drained node that still holds its connections takes another name, or a
/// started one renamed on its last cycle's cut.
#[test]
fn a_drained_nodes_rename_is_refused_until_node_connections_deleted_then_applied() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_drained(&rig).await;
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        let refused = reply(&call(&rig, &b["admin2"], &update_node(&succ, "mesh1.rpc.3")).await);
        assert!(
            matches!(&refused, StatusReply::Busy { reason } if reason.contains("Draining") && reason.contains("no node.connections.deleted")),
            "a drained node whose connections were not deleted keeps its name: {refused:?}"
        );
        assert_eq!(rig.nodes.node_row_at(&taken).await.unwrap(), None, "nothing is claimed at the new name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");

        let deleted = reply(&call(&rig, &b["admin2"], &delete_all(&succ)).await);
        assert!(matches!(&deleted, StatusReply::ConnectionsDeleted { held, .. } if held.is_empty()), "the mesh-primary answers the birth's node.connections.deleted: {deleted:?}");
        assert_eq!(node.seen.lock().unwrap().clone(), vec![delete_all(&succ)], "the delete is carried to the exact birth");
        let applied = reply(&call(&rig, &b["admin2"], &update_node(&succ, "mesh1.rpc.3")).await);
        assert_eq!(applied, StatusReply::NodeUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.node_row_at(&taken).await.unwrap().map(|r| r.node_id), Some(succ.node.node_id.clone()), "the row at the new name holds the birth");
        assert_eq!(
            node.seen.lock().unwrap().last().cloned(),
            Some(StatusRequest::ApplyNodeName { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap(), name: taken.to_string() }),
            "the live birth is told its name"
        );

        // A start begins the birth's next cycle: its last cycle's cut does not carry over.
        rig.authority.begin_cycle(&succ.node, succ.node.incarnation_id.as_ref().unwrap()).await;
        let again = reply(&call(&rig, &b["admin2"], &update_node(&succ, "mesh1.rpc.3.new")).await);
        assert!(matches!(&again, StatusReply::Busy { reason } if reason.contains("no node.connections.deleted")), "a started birth is refused until its connections are deleted again: {again:?}");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-update-node");
    let outcomes: Vec<_> = spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("connections").cloned().unwrap_or_default())).collect();
    assert_eq!(
        outcomes,
        vec![("busy".to_string(), "none-deleted".to_string()), ("node-updated".to_string(), "deleted".to_string()), ("busy".to_string(), "none-deleted".to_string())],
        "{spans:?}"
    );
    let carried = cap.spans("rdm.node_admin.node.connections.delete.via-mesh-primary");
    assert_eq!(carried.len(), 1, "{carried:?}");
    assert_eq!((carried[0].1.get("scope").map(String::as_str), carried[0].1.get("status_at_cut").map(String::as_str), carried[0].1.get("outcome").map(String::as_str)), (Some("all"), Some("Draining"), Some("connections-deleted")), "{carried:?}");
    eprintln!("trace {}: rdm.node_admin.node.connections.delete.via-mesh-primary scope=all status_at_cut=Draining outcome=connections-deleted", carried[0].0);
    for (trace, attrs) in &spans {
        eprintln!("trace {trace}: rdm.node_admin.node.update.via-update-node outcome={:?} connections={:?} traffic={:?}", attrs.get("outcome"), attrs.get("connections"), attrs.get("traffic"));
    }
}

/// CONTRACT (R13 step 3): a rename of a stopped successor into a name another node holds is refused
/// `NameHeld { by }` by the store adapter's insert-and-fail; nothing is written and the birth is told
/// nothing. A live node that can carry traffic renamed to `.old` is refused `Busy` naming its status
/// (R-CR1a).
#[test]
fn a_rename_into_a_held_name_is_refused_name_held_and_nothing_is_written_or_told() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_new(&rig).await;
        // Stopped: the name check is the claim's alone.
        if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == succ.node.node_id) {
            (n.parked, n.status) = (true, NodeStatus::Leaving);
        }
        let holder = NodeId::mint();
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        rig.nodes.put_node_row(&rafka_node_admin_core::storage::NodeRow { name: taken.clone(), node_id: holder.clone(), status: NodeStatus::Leaving, config: Default::default() }).await.unwrap().unwrap();
        let held = reply(&call(&rig, &b["admin2"], &update_node(&succ, "mesh1.rpc.3")).await);
        assert_eq!(held, StatusReply::NameHeld { name: taken.to_string(), by: holder.clone() });
        assert_eq!(rig.nodes.node_row_at(&taken).await.unwrap().map(|r| r.node_id), Some(holder), "the holder keeps the name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
        let routed = &b["rpc1"];
        let refused = reply(&call(&rig, &b["admin2"], &update_node(routed, "mesh1.rpc.1.old")).await);
        assert!(matches!(&refused, StatusReply::Busy { reason } if reason.contains("ReadyForTraffic")), "a live node that can carry traffic keeps its name, refused naming its status: {refused:?}");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-update-node");
    let outcomes: Vec<_> = spans.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect();
    assert_eq!(outcomes, vec!["name-held".to_string(), "busy".to_string()], "{spans:?}");
    eprintln!("trace {}: rdm.node_admin.node.update.via-update-node outcome=name-held", spans[0].0);
}

/// CONTRACT (R13; ops-naming "A rename is the same write": caller → the node's mesh-primary, Forward
/// `0x1A` when not directly reachable): a caller of another mesh whose client reaches mesh1's primary
/// only through a carrier (mesh1.admin.2) renames the drained successor `mesh1.rpc.3.new`, whose
/// `node.connections.deleted` mesh1's primary holds, to
/// `mesh1.rpc.3` with `node.update`: the call ends `NodeUpdated` naming the exact birth, the row at
/// `mesh1.rpc.3` holds it, the birth was told `ApplyNodeName` once; the route span says `via-peer`
/// with the carrier, and the mesh-primary's `via-update-node` span says `node-updated` after the
/// apply answered `applied`. What must NOT happen: the call ending `NotBackedToday` on the caller.
#[test]
fn a_callers_rename_reaches_the_nodes_mesh_primary_through_a_carrier_and_ends_node_updated() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_drained(&rig).await;
        assert!(matches!(reply(&call(&rig, &b["admin2"], &delete_all(&succ)).await), StatusReply::ConnectionsDeleted { .. }), "the drained successor's connections are deleted first");
        let (carrier, _carrier_router) = carrier_admin(&b["admin2"], &rig.resolved).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(carrier.clone());
        let caller = &b["m2admin"];
        let ep = rafka_node_rpc::endpoint::bind(caller.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
        let route = rafka_node_rpc::RouteChoice::ViaPeer { carrier: carrier.node_id.clone(), path: carrier.name.clone() };
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        let reply = rafka_node_admin_client::NodeRpc::new(&client).node_update(&rig.admin.node.node_id, &route, &exact(&succ), &taken, &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::NodeUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() });
        let row = rig.nodes.node_row_at(&taken).await.unwrap().expect("the mesh-primary wrote the row at the new name");
        assert_eq!(row.node_id, succ.node.node_id);
        assert_eq!(node.seen.lock().unwrap().clone(), vec![delete_all(&succ), StatusRequest::ApplyNodeName { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap(), name: taken.to_string() }]);
    });
    let updates = cap.spans("rdm.node_admin.node.update.via-update-node");
    assert_eq!(updates.len(), 1, "{updates:?}");
    let (mp_trace, attrs) = &updates[0];
    assert_eq!((attrs.get("outcome").map(String::as_str), attrs.get("answered").map(String::as_str)), (Some("node-updated"), Some("applied")), "{attrs:?}");
    let routes = cap.spans("rdm.node_rpc.route.resolve.via-connections");
    let carried: Vec<_> = routes.iter().filter(|(_, a)| a.get("route").map(String::as_str) == Some("via-peer")).collect();
    assert_eq!(carried.len(), 1, "one carried leg: {routes:?}");
    assert_eq!((carried[0].1.get("carrier").map(String::as_str), carried[0].1.get("protocol").map(String::as_str)), (Some("mesh1.admin.2"), Some("status")));
    eprintln!("trace {}: rdm.node_rpc.route.resolve.via-connections route=via-peer carrier=mesh1.admin.2 protocol=status; trace {mp_trace}: rdm.node_admin.node.update.via-update-node answered=applied outcome=node-updated", carried[0].0);
}

/// CONTRACT (R13; node-rpc-envelope.md "Meta and config": row first, mesh second; a stopped node is
/// sent nothing and reads its name in its join answer at its next start): `node.update` renaming the
/// stopped `mesh1.rpc.2` (parked in the mesh-primary's view) to `mesh1.rpc.2.old` ends `NodeUpdated`:
/// the row at `mesh1.rpc.2.old` holds the node, the old key is released, the mesh-primary's view
/// lists it at the new name, and the birth is told nothing (the span records no apply answer).
#[test]
fn a_stopped_nodes_rename_at_its_mesh_primary_writes_the_row_and_tells_the_node_nothing() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let stopped = &b["rpc2"];
        if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == stopped.node.node_id) {
            (n.parked, n.status) = (true, NodeStatus::Leaving);
        }
        rig.nodes.put_node_row(&rafka_node_admin_core::storage::NodeRow { name: stopped.node.name.clone(), node_id: stopped.node.node_id.clone(), status: NodeStatus::Leaving, config: Default::default() }).await.unwrap().unwrap();
        let to: PathName = "mesh1.rpc.2.old".parse().unwrap();
        let reply = reply(&call(&rig, &b["admin2"], &update_node(stopped, &to.to_string())).await);
        assert_eq!(reply, StatusReply::NodeUpdated { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.node_row_at(&to).await.unwrap().map(|r| r.node_id), Some(stopped.node.node_id.clone()), "the row moved to the new name");
        assert_eq!(rig.nodes.node_row_at(&stopped.node.name).await.unwrap(), None, "the old key is released");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == stopped.node.node_id && n.name == to), "the view lists the stopped node at the row's name");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-update-node");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!((attrs.get("outcome").map(String::as_str), attrs.get("traffic").map(String::as_str), attrs.get("answered")), (Some("node-updated"), Some("Stopped"), None), "{attrs:?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-update-node from=mesh1.rpc.2 to=mesh1.rpc.2.old traffic=Stopped outcome=node-updated (no apply sent)");
}

/// CONTRACT (R12; node-rpc-envelope.md :434, the ApplyNodeName answers): `ApplyNodeName` naming this
/// admin, from another node-admin, is answered by the admin itself from what its name decision says:
/// `Applied` once it holds the name, `AlreadyApplied` for the name it holds, `Busy` naming its
/// held connections while it holds any, and the same from a member (nothing checks who is calling,
/// node-rpc.md §14). One naming another incarnation is `RejectedStaleIncarnation` and one whose name
/// is not a path.name `Malformed`; neither reaches the decision.
#[test]
fn a_node_admin_told_its_name_answers_from_its_own_decision() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let asked: Arc<Mutex<Vec<PathName>>> = Arc::default();
        let a = asked.clone();
        let decisions = Arc::new(Mutex::new(vec![rafka_node_admin_core::node_name::NameApply::AlreadyApplied, rafka_node_admin_core::node_name::NameApply::Busy { routes: vec!["mesh1.rpc.1 (direct, entry 3123456789ab)".into(), "mesh1.rpc.2 (direct, entry 4123456789ab)".into()] }, rafka_node_admin_core::node_name::NameApply::AlreadyApplied, rafka_node_admin_core::node_name::NameApply::Applied]));
        let held: PathName = rig.admin.node.name.clone();
        let _ = rig.authority.rename.set(Arc::new(move |name: PathName| {
            let (a, d, held) = (a.clone(), decisions.clone(), held.clone());
            Box::pin(async move {
                a.lock().unwrap().push(name);
                (held, d.lock().unwrap().pop().unwrap())
            })
        }));
        let me = &rig.admin.node;
        let told = |incarnation: IncarnationId, name: &str| StatusRequest::ApplyNodeName { node_id: me.node_id.clone(), incarnation, name: name.into() };
        let inc = me.incarnation_id.clone().unwrap();
        assert_eq!(reply(&call(&rig, admin2, &told(inc.clone(), "mesh1.admin.1.old")).await), StatusReply::Applied);
        assert_eq!(reply(&call(&rig, admin2, &told(inc.clone(), "mesh1.admin.1.old")).await), StatusReply::AlreadyApplied);
        assert!(matches!(reply(&call(&rig, admin2, &told(inc.clone(), "mesh1.admin.1.old")).await), StatusReply::Busy { reason } if reason.contains("2 connections: mesh1.rpc.1 (direct, entry 3123456789ab), mesh1.rpc.2")));
        assert_eq!(reply(&call(&rig, admin2, &told(IncarnationId::mint(), "mesh1.admin.1.old")).await), StatusReply::RejectedStaleIncarnation { held: inc.clone() });
        assert_eq!(reply(&call(&rig, rpc1, &told(inc.clone(), "mesh1.admin.1.old")).await), StatusReply::AlreadyApplied, "the subject answers a member from its own decision");
        assert!(matches!(reply(&call(&rig, admin2, &told(inc, "not a path name")).await), StatusReply::Malformed { .. }));
        assert_eq!(asked.lock().unwrap().len(), 4, "only the four admitted applies reached the decision");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-apply-node-name");
    let outcomes: Vec<_> = spans.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect();
    assert_eq!(outcomes.len(), 4, "{spans:?}");
    eprintln!("trace {}: rdm.node_admin.node.update.via-apply-node-name outcomes={outcomes:?}", spans[0].0);
}
