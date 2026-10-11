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
        configs: Default::default(),
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

/// A birth of the fixture serving Status on its own key, recording every request it decodes and
/// answering `Applied`: the node end of a config apply.
struct ServedNode {
    resolved: ResolvedNode,
    seen: Arc<Mutex<Vec<StatusRequest>>>,
    _router: Router,
}

async fn serve_node(b: &Birth) -> ServedNode {
    serve_node_answering(b, StatusReply::Applied).await
}

/// A birth serving Status that records every request and answers each with `reply`.
async fn serve_node_answering(b: &Birth, reply: StatusReply) -> ServedNode {
    use rafka_node_rpc_contract::catalog::OpOwner;
    let seen: Arc<Mutex<Vec<StatusRequest>>> = Arc::default();
    let s = seen.clone();
    let server = ServerBuilder::new()
        .serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
            let (s, reply) = (s.clone(), reply.clone());
            async move {
                s.lock().unwrap().push(req);
                Ok(reply)
            }
        })
        .seal(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().0 })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id: b.node.node_id.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    ServedNode { resolved, seen, _router: router }
}

/// The fabric-primary's config door, telling births through a client that resolves `node`.
async fn configs_door(rig: &Rig, node: &ServedNode) {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(node.resolved.clone());
    let ep = rafka_node_rpc::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _ = rig.authority.configs.set(Arc::new(rafka_node_admin_core::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")))));
}

fn exact(b: &Birth) -> rafka_node_admin_client::ExactBirth {
    rafka_node_admin_client::ExactBirth { target: NodeTarget::ExactNode(b.node.node_id.clone()), node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap() }
}

/// CONTRACT (ops-naming "Node data": a config update is a single call, not a Build; the fabric-primary
/// is the only writer and writes the whole row): a caller's `node.config.update` to the fabric-primary
/// ends `node.config.updated` (`ConfigUpdated` naming the exact birth); the node's record at its
/// path.name holds the config; the birth was told the whole map exactly once (`ApplyNodeConfig`) before
/// the fabric-primary answered; and one `rdm.node_admin.node.update.via-config` span says
/// `config-updated` over a direct route (the fabric-primary is mesh1's primary).
#[test]
fn a_config_update_at_the_fabric_primary_ends_updated_and_the_node_carries_it() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let node = serve_node(rpc1).await;
        configs_door(&rig, &node).await;
        let config: BTreeMap<String, String> = [("tags".to_string(), "edge,blue".to_string()), ("zone".to_string(), "a".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let fp = NodeTarget::ExactNode(rig.admin.node.node_id.clone());
        let reply = rafka_node_admin_client::NodeRpc::new(&client).config_update(&fp, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap() });
        let row = rig.storage.node_row_at(&rpc1.node.name).await.unwrap().expect("the fabric-primary wrote the node's row");
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
    assert_eq!(attrs.get("route").map(String::as_str), Some("direct"), "{attrs:?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config subject=mesh1.rpc.1 route=direct outcome=config-updated");
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
    let fp = NodeTarget::ExactNode(rig.admin.node.node_id.clone());
    let reply = rafka_node_admin_client::NodeRpc::new(&client).config_update(&fp, &exact(rpc1), config, &CallOptions::default()).await.expect("a typed refusal is a reply");
    assert_eq!(
        reply,
        StatusReply::RejectedConfig { over: rafka_mesh_entity::ConfigOverCap { keys: keys as u32, bytes: bytes as u32, max_keys: rafka_mesh_entity::MAX_CONFIG_KEYS as u32, max_bytes: rafka_mesh_entity::MAX_CONFIG_BYTES as u32 } }
    );
    assert!(node.seen.lock().unwrap().is_empty(), "nothing is told to the node");
}

/// A nodes store that refuses every node-row put, by name; every other row is the memory store's.
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
    async fn remove_contact(&self, node_id: &NodeId) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        self.0.remove_contact(node_id).await
    }
    async fn put_node_row(&self, row: &rafka_node_admin_core::storage::NodeRow) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        Err(rafka_node_admin_core::record_store::StorageError::Io { file: format!("nodes/rows/{}", row.name), reason: "disk refused the write".into() })
    }
    async fn node_row_at(&self, name: &rafka_node_admin_core::model::PathName) -> Result<Option<rafka_node_admin_core::storage::NodeRow>, rafka_node_admin_core::record_store::StorageError> {
        self.0.node_row_at(name).await
    }
}

/// CONTRACT (ops-naming "Node data": the fabric-primary writes the record, then the node applies it):
/// when the birth does not apply the config, the update ends `ConfigNotApplied`, naming the node and
/// the birth's answer by name; the row was written first, so the node's record at its path.name holds
/// the new config. Not `NotReady`: the fabric-primary is ready, the birth refused.
#[test]
fn a_config_the_birth_does_not_apply_ends_not_applied_with_the_row_written() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let node = serve_node_answering(rpc1, StatusReply::RejectedStaleIncarnation { held: IncarnationId::mint() }).await;
        configs_door(&rig, &node).await;
        let config: BTreeMap<String, String> = [("zone".to_string(), "b".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let fp = NodeTarget::ExactNode(rig.admin.node.node_id.clone());
        let reply = rafka_node_admin_client::NodeRpc::new(&client).config_update(&fp, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert!(
            matches!(&reply, StatusReply::ConfigNotApplied { node, answered, .. } if node == "mesh1.rpc.1" && answered == "rejected-stale-incarnation"),
            "the row is written and the birth refused: {reply:?}"
        );
        assert_eq!(node.seen.lock().unwrap().len(), 1, "the birth was told once");
        let row = rig.storage.node_row_at(&rpc1.node.name).await.unwrap().expect("the fabric-primary wrote the row before telling the birth");
        assert_eq!(row.config, config);
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    assert_eq!(spans[0].1.get("outcome").map(String::as_str), Some("config-not-applied"), "{spans:?}");
    eprintln!("trace {}: rdm.node_admin.node.update.via-config outcome=config-not-applied", spans[0].0);
}

/// CONTRACT (ops-naming "Node data": the fabric-primary writes the record, then tells the node): when
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
        let reply = rig.authority.configs.get().unwrap().update(&rig.admin.node.name, &view, &RefusingRows::default(), &req).await;
        assert!(
            matches!(&reply, StatusReply::ConfigRowRefused { node, reason } if node == "mesh1.rpc.1" && reason.contains("disk refused the write")),
            "the store refused the row: {reply:?}"
        );
        assert!(node.seen.lock().unwrap().is_empty(), "nothing is told to the birth when its row was not written");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    assert_eq!(spans[0].1.get("outcome").map(String::as_str), Some("config-row-refused"), "{spans:?}");
    eprintln!("trace {}: rdm.node_admin.node.update.via-config outcome=config-row-refused", spans[0].0);
}

/// The spans a cell's own run exported, with their trace ids.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("status-declarations"));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
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

    /// Every finished span called `name`: its trace id and its attributes.
    fn spans(&self, name: &str) -> Vec<(String, BTreeMap<String, String>)> {
        let _ = self.provider.force_flush();
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

/// CONTRACT (ops-naming "Node data": caller → fabric-primary, which writes the record → the node's
/// mesh-primary (Forward `0x1A` when not directly reachable) → the node): a config put for a member
/// of another mesh reaches it through that mesh's primary. The fabric-primary's client cannot
/// resolve the member at all, only mesh2's primary, so a direct leg is impossible: the update ends
/// `ConfigUpdated`, the member was told the config once, and the route span names `via-peer` with
/// mesh2's primary as the carrier, inside the via-config span's trace.
#[test]
fn a_config_update_for_another_meshs_member_reaches_it_through_that_meshs_primary() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let m2admin = &b["m2admin"];
        let m2rpc = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
        rig.topology.write().await.nodes.push(m2rpc.node.clone());
        let node = serve_node(&m2rpc).await;
        let (carrier, _carrier_router) = carrier_admin(m2admin, &node.resolved).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(carrier);
        let ep = rafka_node_rpc::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let door = rafka_node_admin_core::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")));
        let config: BTreeMap<String, String> = [("tags".to_string(), "far".to_string())].into();
        let req = StatusRequest::UpdateNodeConfig { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap(), config: config.clone() };
        let view = rig.topology.read().await.clone();
        let reply = door.update(&rig.admin.node.name, &view, &*rig.storage, &req).await;
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap() });
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap(), config }],
            "the member was told once, through its mesh's primary"
        );
    });
    let config_spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(config_spans.len(), 1, "{config_spans:?}");
    let (trace_id, attrs) = &config_spans[0];
    assert_eq!(attrs.get("outcome").map(String::as_str), Some("config-updated"), "{attrs:?}");
    let routes = cap.spans("rdm.node_rpc.route.resolve.via-connections");
    let carried: Vec<_> = routes.iter().filter(|(t, a)| t == trace_id && a.get("route").map(String::as_str) == Some("via-peer")).collect();
    assert_eq!(carried.len(), 1, "one carried leg in the via-config trace: {routes:?}");
    assert_eq!(carried[0].1.get("carrier").map(String::as_str), Some("mesh2.admin.1"));
    assert_eq!(carried[0].1.get("protocol").map(String::as_str), Some("status"));
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config outcome=config-updated > rdm.node_rpc.route.resolve.via-connections route=via-peer carrier=mesh2.admin.1");
}
