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
use rdm_core::{EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind};
use rdm_admin::model::{Fabric, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rdm_admin::status_rpc::{Declared, StatusAuthority};
use rdm_admin::status_storage::{MemoryStatusStorage, StatusStorage};
use rdm_admin::storage::{MemoryNodesStorage, NodesStorage};
use rdm_admin::topology::Topology;
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget, ReplyWithhold, ResolvedNode, ServedBirth, ServerBuilder, ServerStats, StaticResolver};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{FabricEvent, MeshState, NodeState, NotAuthority, Status, StatusReply, StatusRequest};
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
    endpoint: iroh::Endpoint,
    server: rdm_transport::NodeRpcServer,
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
    /// Armed by a cell: the authority's nodes store refuses the next release of a row's key.
    refuse_release: Arc<std::sync::atomic::AtomicBool>,
}

/// The authority's nodes store: the rig's memory store, whose next release of a row's key is refused
/// by name while `refuse_release` is armed (a failure between a rename's put and its release).
struct Rows {
    inner: Arc<MemoryNodesStorage>,
    refuse_release: Arc<std::sync::atomic::AtomicBool>,
}

#[async_trait::async_trait]
impl NodesStorage for Rows {
    async fn put_runtime(&self, row: &rdm_admin::storage::RuntimeRow) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.put_runtime(row).await
    }
    async fn runtimes(&self) -> Result<Vec<rdm_admin::storage::RuntimeRow>, rdm_admin::storage_error::StorageError> {
        self.inner.runtimes().await
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Result<Option<rdm_admin::storage::RuntimeRow>, rdm_admin::storage_error::StorageError> {
        self.inner.runtime(node_id, incarnation).await
    }
    async fn own(&self) -> Result<Option<rdm_admin::storage::NodeRecord>, rdm_admin::storage_error::StorageError> {
        self.inner.own().await
    }
    async fn put_own(&self, record: &rdm_admin::storage::NodeRecord) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.put_own(record).await
    }
    async fn contacts(&self) -> Result<Vec<rdm_admin::storage::NodeRecord>, rdm_admin::storage_error::StorageError> {
        self.inner.contacts().await
    }
    async fn put_contact(&self, record: &rdm_admin::storage::NodeRecord) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.put_contact(record).await
    }
    async fn remove_contact(&self, mesh: &str, node_id: &NodeId) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.remove_contact(mesh, node_id).await
    }
    async fn put_node_row(&self, row: &rdm_admin::storage::NodeRow) -> Result<Result<(), rdm_admin::storage::NameHeld>, rdm_admin::storage_error::StorageError> {
        self.inner.put_node_row(row).await
    }
    async fn node_row_at(&self, name: &PathName) -> Result<Option<rdm_admin::storage::NodeRow>, rdm_admin::storage_error::StorageError> {
        self.inner.node_row_at(name).await
    }
    async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<PathName>, rdm_admin::storage_error::StorageError> {
        self.inner.node_row_name(mesh, node_id).await
    }
    async fn hold_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, rdm_admin::storage_error::StorageError> {
        self.inner.hold_node_row(name, holder).await
    }
    async fn release_node_row(&self, name: &PathName, holder: &NodeId) -> Result<bool, rdm_admin::storage_error::StorageError> {
        if self.refuse_release.swap(false, Ordering::SeqCst) {
            return Err(rdm_admin::storage_error::StorageError::Io { file: format!("nodes/rows/{name}"), reason: "disk refused the release".into() });
        }
        self.inner.release_node_row(name, holder).await
    }
    async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, rdm_admin::storage_error::StorageError> {
        self.inner.tagged(label).await
    }
}

impl Rig {
    fn mesh_id(&self, mesh: &str) -> MeshId {
        self.mesh_ids[mesh].clone()
    }
}

/// mesh1: admin.1 (this authority: mesh-primary and fabric-primary), admin.2, rpc.1, rpc.2;
/// mesh2: admin.1 (mesh2's primary).
async fn rig() -> (Rig, BTreeMap<&'static str, Birth>) {
    rig_with_book(None).await
}

async fn rig_with_book(book: Option<rdm_transport::membership::DigestBook>) -> (Rig, BTreeMap<&'static str, Birth>) {
    let admin = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
    let mut others = BTreeMap::new();
    others.insert("admin2", birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false));
    others.insert("rpc1", birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", true, false));
    others.insert("rpc2", birth("mesh1.rpc.2", NodeKind::RpcNode, "mesh1", false, false));
    others.insert("m2admin", birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false));
    let mut nodes = vec![admin.node.clone()];
    nodes.extend(others.values().map(|b| b.node.clone()));
    let topology = Arc::new(tokio::sync::RwLock::new(Topology {
        fabric: Fabric { id: rdm_core::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes,
    }));
    let fabric_id = topology.read().await.fabric.id.clone();
    let storage = Arc::new(MemoryStatusStorage::default());
    let nodes = Arc::new(MemoryNodesStorage::default());
    let refuse_release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (m1id, m2id) = (MeshId::mint(), MeshId::mint());
    let mesh_ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), m1id.clone()), ("mesh2".to_string(), m2id.clone())].into_iter().collect();
    let rig_mesh_ids = mesh_ids.clone();
    let authority = Arc::new(StatusAuthority {
        certs: None,
        app_channels: None,
        held_takeover: Default::default(),
        declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
        stopped: Default::default(),
        me: admin.node.name.clone(),
        node_id: admin.node.node_id.clone(),
        fabric_id,
        topology: topology.clone(),
        declared: Arc::new(Mutex::new(Declared::default())),
        status_storage: storage.clone(),
        nodes_storage: Arc::new(Rows { inner: nodes.clone(), refuse_release: refuse_release.clone() }),
        mesh_ids: Arc::new(move || mesh_ids.clone()),
        durable: Default::default(),
        commands: Arc::new(rdm_admin::node_commands::CommandBook::default()),
        own: Arc::new(std::sync::OnceLock::new()),
        mesh_deleter: Arc::new(std::sync::OnceLock::new()),
        hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        rounds: Default::default(),
        configs: Default::default(),
        configure: Default::default(),
        before_ready: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(),
        loads: None,
        book,
        first_ready: Default::default(),
        builds: std::sync::Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
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
    let server = rdm_admin::status_rpc::serve(ServerBuilder::new().withhold_replies(withhold.clone()), slot)
        .seal(ServedBirth { node_id: admin.node.node_id.to_string(), incarnation: admin.node.incarnation_id.clone().unwrap().to_string() })
        .unwrap();
    let ep = rdm_transport::endpoint::bind(admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep.clone()).accept(rdm_transport::ALPN, server.clone()).spawn();
    let resolved = ResolvedNode {
        node_id: admin.node.node_id.clone(),
        name: admin.node.name.clone(),
        endpoint_id: admin.key.public(),
        transport_addr: addr,
        incarnation: admin.node.incarnation_id.clone().unwrap(),
    };
    (Rig { admin, endpoint: ep, server, _router: router, resolved, authority, storage, nodes, topology, mesh_ids: rig_mesh_ids, withhold, refuse_release }, others)
}

/// A birth's client to the authority, speaking as that birth.
async fn client_of(rig: &Rig, b: &Birth) -> NodeRpcClient {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(rig.resolved.clone());
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    tracing::info!(step = "client-bound", node = %b.node.name, peer = %b.key.public().fmt_short(), local = ?ep.bound_sockets(), "a client endpoint is bound on the birth's key");
    NodeRpcClient::new(ep, resolver).with_caller_system("rdm")
}

fn declare(b: &Birth, state: NodeState) -> StatusRequest {
    StatusRequest::DeclareNodeState { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), state, operation: None }
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
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Stopped)).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::ConnectionsDeleting)).await), StatusReply::RejectedInvalidNodeTransition { current: NodeState::Stopped });
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Stopped));
}

/// CONTRACT: the authority is the seat the subject's declaration needs, never the caller
/// (node-rpc.md §14). rpc.2 sending rpc.1's declaration is decided for rpc.1, on rpc.1's row; a
/// subject the view does not hold is `SubjectUnknown`; a birth the view no longer holds is stale; a
/// subject of another mesh, a node-admin included, is refused naming the seat it needs: its own
/// mesh's primary.
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
    let stale = StatusRequest::DeclareNodeState { node_id: rpc1.node.node_id.clone(), incarnation: IncarnationId::mint(), state: NodeState::ReadyForTraffic, operation: None };
    assert_eq!(reply(&call(&rig, rpc1, &stale).await), StatusReply::RejectedStaleIncarnation { held: rpc1.node.incarnation_id.clone().unwrap() });
    // A node of another mesh: this admin is not its mesh-primary.
    let foreign = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
    rig.topology.write().await.nodes.push(foreign.node.clone());
    let r = reply(&call(&rig, &foreign, &declare(&foreign, NodeState::ReadyForTraffic)).await);
    assert_eq!(r, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } });
    // A node-admin declares to its own mesh's primary (this admin is mesh1's): Applied.
    assert_eq!(reply(&call(&rig, admin2, &declare(admin2, NodeState::ReadyForTraffic)).await), StatusReply::Applied);
    // A node-admin of another mesh is refused by the seat it needs, its own mesh's primary, though
    // this admin holds the fabric seat (node-status.md: "A node reports to its mesh's primary").
    let r = reply(&call(&rig, m2admin, &declare(m2admin, NodeState::ReadyForTraffic)).await);
    assert_eq!(r, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } });
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
    let stale = StatusRequest::DeclareNodeState { node_id: rpc1.node.node_id.clone(), incarnation: IncarnationId::mint(), state: NodeState::Stopped, operation: None };
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
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == rdm_core::outcome::IndeterminateReason::ReplyDeadline), "{out:?}");
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
async fn a_connections_deleting_authority_accepts_upward_status_and_decides_it_for_the_subject() {
    let (rig, b) = rig().await;
    rig.server.begin_connections_delete();
    for subject in [&b["rpc1"], &b["admin2"]] {
        for state in [NodeState::ConnectionsDeleting, NodeState::Stopped] {
            let request = declare(subject, state);
            assert_eq!(reply(&call(&rig, subject, &request).await), StatusReply::Applied);
            assert_eq!(reply(&call(&rig, subject, &request).await), StatusReply::AlreadyApplied);
        }
    }
    let for_another = declare(&b["rpc1"], NodeState::Stopped);
    assert_eq!(reply(&call(&rig, &b["rpc2"], &for_another).await), StatusReply::AlreadyApplied, "decided for rpc.1, whoever sends it");
    let stranger = birth("mesh1.rpc.9", NodeKind::RpcNode, "mesh1", false, false);
    assert_eq!(reply(&call(&rig, &b["rpc2"], &declare(&stranger, NodeState::Stopped)).await), StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown });
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
    use rdm_core::catalog::OpOwner;
    let seen: Arc<Mutex<Vec<StatusRequest>>> = Arc::default();
    let heard: Arc<tokio::sync::Notify> = Arc::default();
    let (s, h) = (seen.clone(), heard.clone());
    let answer = Arc::new(answer);
    let (cs, ch, canswer) = (seen.clone(), heard.clone(), answer.clone());
    let server = ServerBuilder::new()
        // The node command op 0x22 (i144 R22): `node.connections.delete` reaches the birth here, answered
        // through the same `answer` and logged as the request it carries, on the command's ordered stream.
        .serve_stream::<rdm_core::node_command::NodeCommand, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: rdm_core::node_command::NodeCommandRequest, sink| {
            let (s, h, answer) = (cs.clone(), ch.clone(), canswer.clone());
            async move {
                use rdm_core::node_command::{NodeCommandReply as N, NodeCommandRequest};
                let NodeCommandRequest::DeleteConnections { node_id, incarnation, scope } = req else { panic!("expected DeleteConnections") };
                let carried = StatusRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: scope.clone() };
                let reply = answer(&carried);
                s.lock().unwrap().push(carried);
                h.notify_one();
                let StatusReply::ConnectionsDeleted { closed, not_open, held, receipt } = reply else {
                    return Ok(match reply {
                        StatusReply::RejectedConnectionScope { reason } => N::RejectedConnectionScope { reason },
                        StatusReply::RejectedStaleIncarnation { held } => N::RejectedStaleIncarnation { held },
                        other => N::NotReady { reason: format!("{}: {other:?}", other.name()) },
                    });
                };
                let mut sink = sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                if matches!(scope, rdm_core::status::ConnectionScope::All) {
                    sink.data(N::ConnectionsDeleting { node_id, incarnation }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                }
                Ok(N::ConnectionsDeleted { closed, not_open, held, receipt })
            }
        })
        .serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
            let (s, h, answer) = (s.clone(), h.clone(), answer.clone());
            async move {
                let reply = answer(&req);
                s.lock().unwrap().push(req);
                h.notify_one();
                Ok(reply)
            }
        })
        .seal(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().to_string() })
        .unwrap();
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id: b.node.node_id.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    ServedNode { resolved, seen, heard, _router: router }
}

/// mesh1's primary's config door, telling births through a client that resolves `node`.
async fn configs_door(rig: &Rig, node: &ServedNode) {
    configs_door_for(rig, &[node]).await
}

/// mesh1's primary's config door, telling births through a client that resolves each of `nodes`.
async fn configs_door_for(rig: &Rig, nodes: &[&ServedNode]) {
    let resolver = Arc::new(StaticResolver::new());
    for node in nodes {
        resolver.insert(node.resolved.clone());
    }
    let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _ = rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), rig.authority.configure.clone(), Default::default())));
}

/// mesh1's primary's config door, the one door its record changes take turns at, its client
/// resolving no node: what a cell whose subject is told nothing (a stopped birth) needs.
async fn quiet_door(rig: &Rig) {
    let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _ = rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, Arc::new(StaticResolver::new())).with_caller_system("rdm")), rig.authority.configure.clone(), Default::default())));
}

fn exact(b: &Birth) -> rdm_client::ExactBirth {
    rdm_client::ExactBirth { target: NodeTarget::ExactNode(b.node.node_id.clone()), node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap() }
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
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
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

/// CONTRACT (topology.rs `own`: a node is found by its exact birth, "never by path.name, its shape
/// included"; node-replace.md: the predecessor stays the same exact birth at `<path>.old`): a config
/// update naming a node whose row its mesh-primary moved to `<path>.old` is decided for that exact
/// birth: it ends `node.config.updated`, the row at `mesh1.rpc.1.old` holds the config, the birth was
/// told it once, and the via-config span names the `.old` subject. What must NOT happen: the update
/// refused as "not a member" because the view's members leave `.old` rows out.
#[test]
fn a_config_update_naming_a_node_at_its_old_row_name_is_decided_for_that_exact_birth() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let old: PathName = "mesh1.rpc.1.old".parse().unwrap();
        rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == rpc1.node.node_id).expect("rpc1 is in the view").name = old.clone();
        let node = serve_node(rpc1).await;
        configs_door(&rig, &node).await;
        let config: BTreeMap<String, String> = [("zone".to_string(), "d".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap() });
        let row = rig.nodes.node_row_at(&old).await.unwrap().expect("the mesh-primary wrote the node's row at its .old name");
        assert_eq!((row.node_id.clone(), row.config.clone()), (rpc1.node.node_id.clone(), config.clone()));
        assert_eq!(node.seen.lock().unwrap().len(), 1, "the birth was told once");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!((attrs.get("outcome").map(String::as_str), attrs.get("subject").map(String::as_str)), (Some("config-updated"), Some("mesh1.rpc.1.old")), "{attrs:?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config subject=mesh1.rpc.1.old outcome=config-updated");
}

/// CONTRACT (node-rpc-envelope.md "Node properties": config "is a field of the node record ... a
/// rejoining node gets it back in its join answer"; node-config-update.md: row first, node second, as
/// node.name.update completes a stopped node on the row, proposal node-properties-own-commands.md §2.3):
/// `node.config.update` of `mesh1.rpc.2`, listed stopped in the mesh-primary's view (its row at its
/// name) ends `node.config.updated` on the row: the row at `mesh1.rpc.2` holds the whole config, keeping
/// the tags it carried, and the birth is told nothing (it serves Status here, and hears no call). The
/// via-config span says `config-updated` with `apply=stopped` and no answer. What must NOT happen: an
/// ApplyNodeConfig sent to the stopped node, or `ConfigNotApplied` for it.
#[test]
fn a_stopped_nodes_config_update_completes_on_its_row_and_tells_the_node_nothing() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let stopped = &b["rpc2"];
        let node = serve_node(stopped).await;
        configs_door(&rig, &node).await;
        stopped_with_row(&rig, stopped, &["edge"]).await;
        let config: BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
        let client = client_of(&rig, &b["admin2"]).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(stopped), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap() }, "a stopped node's config update completes on its row");
        let row = rig.nodes.node_row_at(&stopped.node.name).await.unwrap().expect("the mesh-primary wrote the node's row");
        assert_eq!((row.node_id.clone(), row.config.clone(), row.tags.clone()), (stopped.node.node_id.clone(), config, labels(&["edge"])), "the row holds the whole config and keeps its tags");
        assert!(node.seen.lock().unwrap().is_empty(), "the stopped node is told nothing: {:?}", node.seen.lock().unwrap());
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(
        (attrs.get("outcome").map(String::as_str), attrs.get("apply").map(String::as_str), attrs.get("subject").map(String::as_str), attrs.get("answered")),
        (Some("config-updated"), Some("stopped"), Some("mesh1.rpc.2"), None),
        "{attrs:?}"
    );
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config subject=mesh1.rpc.2 apply=stopped outcome=config-updated (nothing sent)");
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
    let keys = rdm_core::MAX_CONFIG_KEYS + 1;
    let config: BTreeMap<String, String> = (0..keys).map(|i| (format!("k{i:02}"), "v".to_string())).collect();
    let bytes: usize = config.iter().map(|(k, v)| k.len() + v.len()).sum();
    let client = client_of(&rig, admin2).await;
    let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(rpc1), config, &CallOptions::default()).await.expect("a typed refusal is a reply");
    assert_eq!(
        reply,
        StatusReply::RejectedConfig { over: rdm_core::ConfigOverCap { keys: keys as u32, bytes: bytes as u32, max_keys: rdm_core::MAX_CONFIG_KEYS as u32, max_bytes: rdm_core::MAX_CONFIG_BYTES as u32 } }
    );
    assert!(node.seen.lock().unwrap().is_empty(), "nothing is told to the node");
}

/// A nodes store that refuses every node-row write (put, claim, release), by name; every other row is
/// the memory store's.
#[derive(Default)]
struct RefusingRows(MemoryNodesStorage);

#[async_trait::async_trait]
impl NodesStorage for RefusingRows {
    async fn put_runtime(&self, row: &rdm_admin::storage::RuntimeRow) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.put_runtime(row).await
    }
    async fn runtimes(&self) -> Result<Vec<rdm_admin::storage::RuntimeRow>, rdm_admin::storage_error::StorageError> {
        self.0.runtimes().await
    }
    async fn runtime(&self, node_id: &NodeId, incarnation: &rdm_core::IncarnationId) -> Result<Option<rdm_admin::storage::RuntimeRow>, rdm_admin::storage_error::StorageError> {
        self.0.runtime(node_id, incarnation).await
    }
    async fn own(&self) -> Result<Option<rdm_admin::storage::NodeRecord>, rdm_admin::storage_error::StorageError> {
        self.0.own().await
    }
    async fn put_own(&self, record: &rdm_admin::storage::NodeRecord) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.put_own(record).await
    }
    async fn contacts(&self) -> Result<Vec<rdm_admin::storage::NodeRecord>, rdm_admin::storage_error::StorageError> {
        self.0.contacts().await
    }
    async fn put_contact(&self, record: &rdm_admin::storage::NodeRecord) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.put_contact(record).await
    }
    async fn remove_contact(&self, mesh: &str, node_id: &rdm_core::NodeId) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.remove_contact(mesh, node_id).await
    }
    async fn put_node_row(&self, row: &rdm_admin::storage::NodeRow) -> Result<Result<(), rdm_admin::storage::NameHeld>, rdm_admin::storage_error::StorageError> {
        Err(rdm_admin::storage_error::StorageError::Io { file: format!("nodes/rows/{}", row.name), reason: "disk refused the write".into() })
    }
    async fn node_row_at(&self, name: &rdm_admin::model::PathName) -> Result<Option<rdm_admin::storage::NodeRow>, rdm_admin::storage_error::StorageError> {
        self.0.node_row_at(name).await
    }
    async fn node_row_name(&self, mesh: &str, node_id: &NodeId) -> Result<Option<rdm_admin::model::PathName>, rdm_admin::storage_error::StorageError> {
        self.0.node_row_name(mesh, node_id).await
    }
    async fn hold_node_row(&self, name: &rdm_admin::model::PathName, _holder: &NodeId) -> Result<bool, rdm_admin::storage_error::StorageError> {
        Err(rdm_admin::storage_error::StorageError::Io { file: format!("nodes/rows/{name}"), reason: "disk refused the hold".into() })
    }
    async fn release_node_row(&self, name: &rdm_admin::model::PathName, _holder: &rdm_admin::model::NodeId) -> Result<bool, rdm_admin::storage_error::StorageError> {
        Err(rdm_admin::storage_error::StorageError::Io { file: format!("nodes/rows/{name}"), reason: "disk refused the release".into() })
    }
    async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<NodeId>, rdm_admin::storage_error::StorageError> {
        self.0.tagged(label).await
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
        let refusal = format!("before_ready has not passed: {}", "the store of mesh1.rpc.1 is still behind; ".repeat(20));
        let node = serve_node_answering(rpc1, StatusReply::NotReady { reason: refusal.clone() }).await;
        configs_door(&rig, &node).await;
        let config: BTreeMap<String, String> = [("zone".to_string(), "b".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
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
    assert!(detail.len() > rdm_core::status::MAX_REFUSAL_REASON_BYTES && !detail.ends_with('…'), "the span's detail is whole: {detail}");
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
async fn door_at(key: &SecretKey, node: &ServedNode) -> rdm_admin::node_config::NodeConfigs {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(node.resolved.clone());
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), Default::default(), Default::default())
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
        let ep = rdm_transport::endpoint::bind(rpc2.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
        let config: BTreeMap<String, String> = [("zone".to_string(), "far".to_string())].into();
        let route = rdm_transport::RouteChoice::ViaPeer { carrier: m2admin.node.node_id.clone(), path: m2admin.node.name.clone() };
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &route, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
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
        let reply = rdm_client::NodeRpc::new(&client).config_update(&me.node_id, &rdm_transport::RouteChoice::Direct, &exact(&rig.admin), config.clone(), &CallOptions::default()).await.expect("a typed reply");
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
/// (`rdm_transport::span_capture`), with their trace ids.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("status-declarations", exporter.clone());
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
    use rdm_core::connections::{ConnectionEnd, ConnectionsHeld};
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { node_id: b.node.node_id.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    let live = Arc::new(rdm_transport::LiveNodeResolver::default());
    live.apply(target.clone(), None);
    let connections = Arc::new(rdm_admin::connections_writer::ConnectionsWriter::new(
        ConnectionEnd { name: resolved.name.clone(), node_id: resolved.node_id.clone(), incarnation: Some(resolved.incarnation.clone()) },
        Arc::new(rdm_admin::storage::MemoryConnectionsStorage::default()),
        Arc::new(Mutex::new(ConnectionsHeld::new())),
    ));
    connections.held().lock().unwrap().set_membership(live.clone());
    connections.hydrate().await.unwrap();
    let node_rpc = rdm_admin::node_rpc::ProcessNodeRpc::new(live.clone(), ep.clone());
    let server = rdm_admin::admin::rpc_server(connections, node_rpc.client.clone(), Arc::new(OnceLock::new()), Arc::new(OnceLock::new()), Arc::new(OnceLock::new()))
        .seal(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().to_string() })
        .unwrap();
    (resolved, Router::builder(ep).accept(rdm_transport::ALPN, server).spawn())
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
            .put_node_row(&rdm_admin::storage::NodeRow { name: newcomer.node.name.clone(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: seeded.clone(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None })
            .await
            .unwrap()
            .unwrap();
        let node = serve_node(&newcomer).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(node.resolved.clone());
        let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let configs: rdm_admin::node_config::NodeConfigsSlot = Arc::new(OnceLock::new());
        let _ = configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), Default::default(), Default::default())));
        let records = rdm_admin::admin::Records::default();
        records.set_rows(Arc::new(rdm_admin::node_rows::NodeRows::new(rig.admin.node.name.clone(), rig.admin.node.node_id.clone(), rig.topology.clone(), rig.nodes.clone()).with_configs(configs)));
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
            .put_node_row(&rdm_admin::storage::NodeRow { name: newcomer.node.name.clone(), node_id: NodeId::mint(), status: NodeStatus::Deleted, config: seeded.clone(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None })
            .await
            .unwrap()
            .unwrap();
        let node = serve_node(&newcomer).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(node.resolved.clone());
        let ep = rdm_transport::endpoint::bind(m2admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let configs: rdm_admin::node_config::NodeConfigsSlot = Arc::new(OnceLock::new());
        let _ = configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), Default::default(), Default::default())));
        let records = rdm_admin::admin::Records::default();
        records.set_rows(Arc::new(rdm_admin::node_rows::NodeRows::new(m2admin.node.name.clone(), m2admin.node.node_id.clone(), rig.topology.clone(), mp_rows.clone()).with_configs(configs)));
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

/// A successor born `mesh1.rpc.8`, live in the mesh-primary's view, serving Status that answers
/// every request `Applied` and records it; the mesh-primary's node-RPC client resolves it.
async fn successor_new(rig: &Rig) -> (Birth, ServedNode) {
    let b = birth("mesh1.rpc.8", NodeKind::RpcNode, "mesh1", false, false);
    rig.topology.write().await.nodes.push(b.node.clone());
    let node = serve_node(&b).await;
    configs_door(rig, &node).await;
    (b, node)
}

/// [`successor_new`], its connections deleted (connections-deleting in the mesh-primary's view and its declared state),
/// answering `node.connections.delete` with every connection cut and nothing held, `node.mesh.leave`
/// with its mesh left, and every other request `Applied`.
async fn successor_connections_deleted(rig: &Rig) -> (Birth, ServedNode) {
    let mut b = birth("mesh1.rpc.8", NodeKind::RpcNode, "mesh1", false, false);
    (b.node.status, b.node.declared) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()));
    rig.topology.write().await.nodes.push(b.node.clone());
    let node = serve_node_with(&b, |req| match req {
        StatusRequest::DeleteConnections { .. } => StatusReply::ConnectionsDeleted { closed: vec![rdm_core::ConnectionId::mint()], not_open: vec![], held: vec![], receipt: None },
        StatusRequest::LeaveMesh { node_id, incarnation, .. } => StatusReply::MeshLeft { node_id: node_id.clone(), incarnation: incarnation.clone(), left: vec![rdm_core::status::WhichMesh::Own] },
        _ => StatusReply::Applied,
    })
    .await;
    configs_door(rig, &node).await;
    (b, node)
}

fn delete_all(b: &Birth) -> StatusRequest {
    StatusRequest::DeleteConnections { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), scope: rdm_core::status::ConnectionScope::All }
}

fn update_name(b: &Birth, name: &str) -> StatusRequest {
    StatusRequest::UpdateName { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), name: name.into() }
}

/// The span of a mesh-primary's `node.name.update`.
const NAME_SPAN: &str = "rdm.node_admin.node.name.update.via-writer";

/// `b` stopped in the mesh-primary's view (parked, `Leaving`), its row at its name carrying `tags`.
async fn stopped_with_row(rig: &Rig, b: &Birth, tags: &[&str]) {
    if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == b.node.node_id) {
        (n.parked, n.status) = (true, NodeStatus::Leaving);
    }
    let row = rdm_admin::storage::NodeRow { name: b.node.name.clone(), node_id: b.node.node_id.clone(), status: NodeStatus::Leaving, config: Default::default(), tags: labels(tags), allocated: None, renamed_from: None, app_types: None, inherited_by: None };
    rig.nodes.put_node_row(&row).await.unwrap().unwrap();
}

/// The mesh-primary holds `b` as a rename needs it (node-name-update.md Precondition): live and
/// connections-deleting in its view, its `node.connections.deleted` of an `All` held with nothing held,
/// then its `node.mesh.left`.
async fn hold_left(rig: &Rig, b: &Birth) {
    if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == b.node.node_id) {
        (n.status, n.declared, n.parked) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()), false);
    }
    let birth = (b.node.node_id.clone(), b.node.incarnation_id.clone().unwrap());
    let mut declared = rig.authority.declared.lock().unwrap();
    declared.connections_deleted.insert(birth.clone(), rdm_admin::status_rpc::ConnectionsDeletedReceipt { all: true, held: vec![], status_at_cut: NodeStatus::ConnectionsDeleting });
    declared.meshes_left.insert(birth);
}

/// `b` live and off its mesh as a rename needs it ([`hold_left`]), its row at its name carrying
/// `tags`; served, answering every request `Applied`, and mesh1's primary's config door resolving it.
async fn left_with_row(rig: &Rig, b: &Birth, tags: &[&str]) -> ServedNode {
    hold_left(rig, b).await;
    let row = rdm_admin::storage::NodeRow { name: b.node.name.clone(), node_id: b.node.node_id.clone(), status: NodeStatus::ConnectionsDeleting, config: Default::default(), tags: labels(tags), allocated: None, renamed_from: None, app_types: None, inherited_by: None };
    rig.nodes.put_node_row(&row).await.unwrap().unwrap();
    let node = serve_node(b).await;
    configs_door(rig, &node).await;
    node
}

fn apply_name(b: &Birth, to: &PathName) -> StatusRequest {
    StatusRequest::ApplyNodeName { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), name: to.to_string() }
}

fn leave_mesh(b: &Birth, operation: &str) -> StatusRequest {
    StatusRequest::LeaveMesh { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), build_id: "3vzt99cnx0xe".into(), attempt: 1, operation: operation.into() }
}

/// Each span's `(outcome, connections)`.
fn outcomes_and_connections(spans: &[(String, BTreeMap<String, String>)]) -> Vec<(String, String)> {
    spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("connections").cloned().unwrap_or_default())).collect()
}

/// CONTRACT (proposal node-properties-own-commands.md §2.1 Precondition and Validation: "A
/// ready-for-traffic node, or one still holding a connection, is refused"; "the precondition holds,
/// else `RejectedAdmittingConnections { status, held_connections }`"; §2.4 "Refused before the claim
/// ...: nothing written, nothing changed"): a caller's `node.name.update` of the live,
/// ready-for-traffic successor `mesh1.rpc.8` to the name `3` at mesh1's primary is refused
/// `RejectedAdmittingConnections { status: ReadyForTraffic, held_connections: [] }`. Nothing is claimed
/// (no row at `mesh1.rpc.3`), the view keeps the birth at `mesh1.rpc.8`, and the birth is told nothing. One
/// `rdm.node_admin.node.name.update.via-writer` span says `rejected-admitting-connections` with
/// `traffic=ReadyForTraffic` and `connections=none-deleted`. What must NOT happen: the row written, the
/// birth told `ApplyNodeName`, or `NameUpdated`.
#[test]
fn a_ready_for_traffic_nodes_rename_is_refused_admitting_connections_and_nothing_is_claimed_or_told() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_new(&rig).await;
        let reply = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(reply, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::ReadyForTraffic, held_connections: vec![] });
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        assert_eq!(rig.nodes.node_row_at(&taken).await.unwrap(), None, "nothing is claimed at the new name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == succ.node.node_id && n.name == succ.node.name), "the view keeps the birth at its name");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(
        (attrs.get("outcome").map(String::as_str), attrs.get("traffic").map(String::as_str), attrs.get("connections").map(String::as_str), attrs.get("answered")),
        (Some("rejected-admitting-connections"), Some("ReadyForTraffic"), Some("none-deleted"), None),
        "{attrs:?}"
    );
    eprintln!("trace {trace_id}: {NAME_SPAN} from=mesh1.rpc.8 name=3 traffic=ReadyForTraffic connections=none-deleted outcome=rejected-admitting-connections");
}

/// CONTRACT (node-name-update.md Precondition: "The node is live and admits no connection: this
/// mesh-primary holds the birth's `node.connections.deleted` of an `All`, then its `node.mesh.left`
/// ..., and it holds zero connections"; steps 4-5; Completion "The left node: `node.name.updated` after
/// `Applied`"): the successor `mesh1.rpc.8`, ConnectionsDeleting in the mesh-primary's view, is
/// refused the rename to `3` `RejectedAdmittingConnections { status: ConnectionsDeleting,
/// held_connections: [] }`, nothing claimed and nothing told. A caller's `node.connections.delete {
/// All }` through the mesh-primary is carried to the exact birth and answered with the birth's
/// `node.connections.deleted`, which the mesh-primary holds; the same rename is refused again, the
/// birth still on its mesh (`connections=deleted`, `mesh=on-mesh`). The mesh-primary carries
/// `LeaveMesh` to the exact birth (node-mesh-leave.md) and holds its `MeshLeft`; the same
/// `node.name.update` then claims `mesh1.rpc.3`, puts the row there carrying `renamed_from:
/// mesh1.rpc.8`, releases the old key, tells the birth `ApplyNodeName` and ends `NameUpdated` on its
/// `Applied`. A start begins the birth's next cycle: neither receipt is carried over, and a rename after
/// it is refused again. What must NOT happen: a node whose connections were not deleted, or one still
/// on its mesh, taking another name, or a started one renamed on its last cycle's receipts.
#[test]
fn a_nodes_rename_is_refused_until_its_connections_are_deleted_and_it_has_left_its_mesh_then_applied() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_connections_deleted(&rig).await;
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        let refused = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(refused, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::ConnectionsDeleting, held_connections: vec![] });
        assert_eq!(rig.nodes.node_row_at(&taken).await.unwrap(), None, "nothing is claimed at the new name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");

        let deleted = reply(&call(&rig, &b["admin2"], &delete_all(&succ)).await);
        assert!(matches!(&deleted, StatusReply::ConnectionsDeleted { held, .. } if held.is_empty()), "the mesh-primary answers the birth's node.connections.deleted: {deleted:?}");
        assert_eq!(node.seen.lock().unwrap().clone(), vec![delete_all(&succ)], "the delete is carried to the exact birth");
        let on_mesh = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(on_mesh, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::ConnectionsDeleting, held_connections: vec![] }, "a birth still on its mesh keeps its name");
        assert_eq!(rig.nodes.node_row_at(&taken).await.unwrap(), None, "nothing is claimed while the birth is on its mesh");

        let left = rig.authority.carry_mesh_leave(&succ.node.node_id, succ.node.incarnation_id.as_ref().unwrap(), "3vzt99cnx0xe", 1, "node.mesh.leave:mesh1.rpc.8").await;
        assert!(matches!(&left, Ok(StatusReply::MeshLeft { .. })), "the mesh-primary carries the leave and the birth answers MeshLeft: {left:?}");
        assert_eq!(node.seen.lock().unwrap().last().cloned(), Some(leave_mesh(&succ, "node.mesh.leave:mesh1.rpc.8")), "the leave is carried to the exact birth");
        let applied = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(applied, StatusReply::NameUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() });
        let row = rig.nodes.node_row_at(&taken).await.unwrap().expect("the row at the new name");
        assert_eq!((row.node_id, row.renamed_from), (succ.node.node_id.clone(), Some(succ.node.name.clone())), "the row at the new name holds the birth and names the key it moved from");
        assert_eq!(rig.nodes.node_row_at(&succ.node.name).await.unwrap(), None, "the old key is released");
        assert_eq!(node.seen.lock().unwrap().last().cloned(), Some(apply_name(&succ, &taken)), "the left birth is told its name");

        // A start begins the birth's next cycle: its last cycle's receipts do not carry over.
        rig.authority.begin_cycle(&succ.node, succ.node.incarnation_id.as_ref().unwrap()).await;
        let again = reply(&call(&rig, &b["admin2"], &update_name(&succ, "8")).await);
        assert!(matches!(&again, StatusReply::RejectedAdmittingConnections { held_connections, .. } if held_connections.is_empty()), "a started birth is refused until its connections are deleted and it leaves its mesh again: {again:?}");
    });
    let spans = cap.spans(NAME_SPAN);
    let labels_of = |spans: &[(String, BTreeMap<String, String>)]| spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("connections").cloned().unwrap_or_default(), a.get("mesh").cloned().unwrap_or_default())).collect::<Vec<_>>();
    let s = |v: &str| v.to_string();
    assert_eq!(
        labels_of(&spans),
        vec![
            (s("rejected-admitting-connections"), s("none-deleted"), s("on-mesh")),
            (s("rejected-admitting-connections"), s("deleted"), s("on-mesh")),
            (s("name-updated"), s("deleted"), s("left")),
            (s("rejected-admitting-connections"), s("none-deleted"), s("on-mesh")),
        ],
        "{spans:?}"
    );
    assert_eq!(spans[2].1.get("answered").map(String::as_str), Some("applied"), "{spans:?}");
    let carried = cap.spans("rdm.node_admin.node.connections.delete.via-mesh-primary");
    assert_eq!(carried.len(), 1, "{carried:?}");
    assert_eq!((carried[0].1.get("scope").map(String::as_str), carried[0].1.get("status_at_cut").map(String::as_str), carried[0].1.get("outcome").map(String::as_str)), (Some("all"), Some("ConnectionsDeleting"), Some("connections-deleted")), "{carried:?}");
    let leaves = cap.spans("rdm.node_admin.node.mesh.update.via-mesh-primary");
    assert_eq!(leaves.iter().map(|(_, a)| (a.get("subject").cloned().unwrap_or_default(), a.get("outcome").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![(s("mesh1.rpc.8"), s("mesh-left"))], "{leaves:?}");
    eprintln!("trace {}: rdm.node_admin.node.connections.delete.via-mesh-primary scope=all status_at_cut=ConnectionsDeleting outcome=connections-deleted", carried[0].0);
    eprintln!("trace {}: rdm.node_admin.node.mesh.update.via-mesh-primary subject=mesh1.rpc.8 outcome=mesh-left", leaves[0].0);
    for (trace, attrs) in &spans {
        eprintln!("trace {trace}: {NAME_SPAN} outcome={:?} connections={:?} mesh={:?} traffic={:?} to={:?} answered={:?}", attrs.get("outcome"), attrs.get("connections"), attrs.get("mesh"), attrs.get("traffic"), attrs.get("to"), attrs.get("answered"));
    }
}

/// CONTRACT (proposal §7: "connections-deleted node still holding one connection: refused
/// `RejectedAdmittingConnections { held_connections: [id] }`, nothing written"): the successor
/// `mesh1.rpc.8`, ConnectionsDeleting, whose mesh-primary holds its `node.connections.deleted` of an `All`
/// that left one connection held, is refused the rename to `3` naming that connection by id. No row
/// at `mesh1.rpc.3`, nothing told. The span says `connections=held`.
#[test]
fn a_connections_deleted_node_still_holding_a_connection_is_refused_naming_it_and_nothing_is_written() {
    let cap = capture();
    let held_id = rdm_core::ConnectionId::mint();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_new(&rig).await;
        let inc = succ.node.incarnation_id.clone().unwrap();
        if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == succ.node.node_id) {
            (n.status, n.declared) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()));
        }
        rig.authority.declared.lock().unwrap().connections_deleted.insert((succ.node.node_id.clone(), inc), rdm_admin::status_rpc::ConnectionsDeletedReceipt { all: true, held: vec![held_id.clone()], status_at_cut: NodeStatus::ConnectionsDeleting });
        let refused = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(refused, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::ConnectionsDeleting, held_connections: vec![held_id.clone()] });
        assert_eq!(rig.nodes.node_row_at(&"mesh1.rpc.3".parse().unwrap()).await.unwrap(), None, "nothing is claimed");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(outcomes_and_connections(&spans), vec![("rejected-admitting-connections".to_string(), "held".to_string())], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} traffic=ConnectionsDeleting connections=held held_connections={held_id} outcome=rejected-admitting-connections", spans[0].0);
}

/// CONTRACT (proposal §2.1 Claim and move: "a held name answers `NameHeld { by: node.id }` and
/// nothing is written"): a rename of a successor off its mesh (its connections deleted, its mesh left)
/// into a name another node holds is refused
/// `NameHeld { by }` by the store adapter's insert-and-fail; nothing is written and the birth is told
/// nothing. A live node that can carry traffic renamed to `1.old` is refused
/// `RejectedAdmittingConnections` naming its status.
#[test]
fn a_rename_into_a_held_name_is_refused_name_held_and_nothing_is_written_or_told() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_new(&rig).await;
        // Live and off its mesh: the name check is the claim's alone.
        hold_left(&rig, &succ).await;
        let holder = NodeId::mint();
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        rig.nodes.put_node_row(&rdm_admin::storage::NodeRow { name: taken.clone(), node_id: holder.clone(), status: NodeStatus::Leaving, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None }).await.unwrap().unwrap();
        let held = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(held, StatusReply::NameHeld { name: taken.to_string(), by: holder.clone() });
        assert_eq!(rig.nodes.node_row_at(&taken).await.unwrap().map(|r| r.node_id), Some(holder), "the holder keeps the name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
        let routed = &b["rpc1"];
        let refused = reply(&call(&rig, &b["admin2"], &update_name(routed, "1.old")).await);
        assert_eq!(refused, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::ReadyForTraffic, held_connections: vec![] }, "a live node that can carry traffic keeps its name");
    });
    let spans = cap.spans(NAME_SPAN);
    let outcomes: Vec<_> = spans.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect();
    assert_eq!(outcomes, vec!["name-held".to_string(), "rejected-admitting-connections".to_string()], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} to=mesh1.rpc.3 outcome=name-held", spans[0].0);
}

/// CONTRACT (proposal §2.1 Caller / executor: "Caller to the node's mesh-primary ... (Forward `0x1A`
/// when not directly reachable)"): a caller of another mesh whose client reaches mesh1's primary only
/// through a carrier (mesh1.admin.2) renames the successor `mesh1.rpc.8`, whose
/// `node.connections.deleted` and then `node.mesh.left` mesh1's primary holds, to the name `3` with
/// `node.name.update`: the call
/// ends `NameUpdated` naming the exact birth, the row at `mesh1.rpc.3` holds it, the birth was told
/// `ApplyNodeName` once; the route span says `via-peer` with the carrier, and the mesh-primary's
/// `via-writer` span says `name-updated` after the apply answered `applied`. What must NOT happen: the
/// call ending `NotBackedToday` on the caller.
#[test]
fn a_callers_rename_reaches_the_nodes_mesh_primary_through_a_carrier_and_ends_name_updated() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_connections_deleted(&rig).await;
        assert!(matches!(reply(&call(&rig, &b["admin2"], &delete_all(&succ)).await), StatusReply::ConnectionsDeleted { .. }), "the successor's connections are deleted first");
        let leave = leave_mesh(&succ, "node.mesh.leave:mesh1.rpc.8");
        assert!(matches!(rig.authority.carry_mesh_leave(&succ.node.node_id, succ.node.incarnation_id.as_ref().unwrap(), "3vzt99cnx0xe", 1, "node.mesh.leave:mesh1.rpc.8").await, Ok(StatusReply::MeshLeft { .. })), "then it leaves its mesh");
        let (carrier, _carrier_router) = carrier_admin(&b["admin2"], &rig.resolved).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(carrier.clone());
        let caller = &b["m2admin"];
        let ep = rdm_transport::endpoint::bind(caller.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
        let route = rdm_transport::RouteChoice::ViaPeer { carrier: carrier.node_id.clone(), path: carrier.name.clone() };
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        let reply = rdm_client::NodeRpc::new(&client).name_update(&rig.admin.node.node_id, &route, &exact(&succ), "3", &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::NameUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() });
        let row = rig.nodes.node_row_at(&taken).await.unwrap().expect("the mesh-primary wrote the row at the new name");
        assert_eq!(row.node_id, succ.node.node_id);
        assert_eq!(node.seen.lock().unwrap().clone(), vec![delete_all(&succ), leave, apply_name(&succ, &taken)]);
    });
    let updates = cap.spans(NAME_SPAN);
    assert_eq!(updates.len(), 1, "{updates:?}");
    let (mp_trace, attrs) = &updates[0];
    assert_eq!((attrs.get("outcome").map(String::as_str), attrs.get("answered").map(String::as_str)), (Some("name-updated"), Some("applied")), "{attrs:?}");
    let routes = cap.spans("rdm.node_rpc.route.resolve.via-connections");
    let carried: Vec<_> = routes.iter().filter(|(_, a)| a.get("route").map(String::as_str) == Some("via-peer")).collect();
    assert_eq!(carried.len(), 1, "one carried leg: {routes:?}");
    assert_eq!((carried[0].1.get("carrier").map(String::as_str), carried[0].1.get("protocol").map(String::as_str)), (Some("mesh1.admin.2"), Some("status")));
    eprintln!("trace {}: rdm.node_rpc.route.resolve.via-connections route=via-peer carrier=mesh1.admin.2 protocol=status; trace {mp_trace}: {NAME_SPAN} answered=applied outcome=name-updated", carried[0].0);
}

/// CONTRACT (node-name-update.md Precondition: "A stopped node is never renamed"; Validation: "the
/// precondition holds, else `RejectedAdmittingConnections { status, held_connections }`"; Failure and
/// receipts: "Refused before the claim ...: nothing written, nothing changed"; node-replace.md: "A node
/// is renamed only while it is live ... it is stopped after"): `node.name.update` renaming the stopped
/// `mesh1.rpc.2` (parked in the mesh-primary's view, its row at `mesh1.rpc.2`) to `2.old` is refused
/// `RejectedAdmittingConnections { status: Leaving, held_connections: [] }`, `Leaving` being the status
/// a stopped birth is held at. Nothing is claimed at `mesh1.rpc.2.old`, the row stays at `mesh1.rpc.2`
/// as it was, the view lists the node at `mesh1.rpc.2`, and the birth is told nothing. The same rename
/// again is refused the same way. One `via-writer` span per call says `rejected-admitting-connections`
/// with `traffic=Stopped` and `connections=stopped`, its detail naming the rule. What must NOT happen:
/// `NameUpdated`, a row at the new key, or a stopped node renamed to read its name at its next join.
#[test]
fn a_stopped_nodes_rename_is_refused_admitting_connections_and_nothing_is_claimed_or_told() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        quiet_door(&rig).await;
        let stopped = &b["rpc2"];
        stopped_with_row(&rig, stopped, &[]).await;
        let row = rig.nodes.node_row_at(&stopped.node.name).await.unwrap();
        let to: PathName = "mesh1.rpc.2.old".parse().unwrap();
        let refused = StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::Leaving, held_connections: vec![] };
        assert_eq!(reply(&call(&rig, &b["admin2"], &update_name(stopped, "2.old")).await), refused);
        assert_eq!(rig.nodes.node_row_at(&to).await.unwrap(), None, "nothing is claimed at the new name");
        assert_eq!(rig.nodes.node_row_at(&stopped.node.name).await.unwrap(), row, "the row stands at its key as it was");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == stopped.node.node_id && n.name == stopped.node.name), "the view lists the stopped node at its name");
        assert_eq!(reply(&call(&rig, &b["admin2"], &update_name(stopped, "2.old")).await), refused, "a repeat is refused the same way");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.len(), 2, "{spans:?}");
    for (_, attrs) in &spans {
        assert_eq!(
            (attrs.get("outcome").map(String::as_str), attrs.get("traffic").map(String::as_str), attrs.get("connections").map(String::as_str), attrs.get("answered")),
            (Some("rejected-admitting-connections"), Some("Stopped"), Some("stopped"), None),
            "{attrs:?}"
        );
        assert!(attrs.get("detail").is_some_and(|d| d.contains("a stopped node is never renamed")), "the refusal names the rule: {attrs:?}");
    }
    eprintln!("trace {}: {NAME_SPAN} from=mesh1.rpc.2 name=2.old traffic=Stopped connections=stopped outcome=rejected-admitting-connections (nothing claimed, nothing told)", spans[0].0);
}

/// CONTRACT (proposal §9, ruled A: "a rename may take any name within the name grammar ..., unique
/// within its path ... Cell: a stopped node renamed to an arbitrary name within the same path
/// succeeds"; node-name-update.md Precondition): the live `mesh1.rpc.2`, its connections deleted and
/// its mesh left, renamed to the name `9` ends `NameUpdated`: the row is at `mesh1.rpc.9`,
/// `mesh1.rpc.2` is released, the view lists the node at `mesh1.rpc.9`, and the birth is told
/// `ApplyNodeName { mesh1.rpc.9 }`. What must NOT happen: a refusal because `9` is not a `.old` toggle
/// of the old name.
#[test]
fn a_left_node_renamed_to_any_name_within_its_path_ends_name_updated() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let left = &b["rpc2"];
        let node = left_with_row(&rig, left, &[]).await;
        let to: PathName = "mesh1.rpc.9".parse().unwrap();
        let reply = reply(&call(&rig, &b["admin2"], &update_name(left, "9")).await);
        assert_eq!(reply, StatusReply::NameUpdated { node_id: left.node.node_id.clone(), incarnation: left.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.node_row_at(&to).await.unwrap().map(|r| r.node_id), Some(left.node.node_id.clone()), "the row is at the new name");
        assert_eq!(rig.nodes.node_row_at(&left.node.name).await.unwrap(), None, "the old key is released");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == left.node.node_id && n.name == to), "the view lists the node at the new name");
        assert_eq!(node.seen.lock().unwrap().clone(), vec![apply_name(left, &to)], "the birth is told its name");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("to").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("name-updated".to_string(), "mesh1.rpc.9".to_string())], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} from=mesh1.rpc.2 name=9 to=mesh1.rpc.9 outcome=name-updated", spans[0].0);
}

/// CONTRACT (proposal §2.1 Validation: "the name is within the name grammar, else `RejectedName {
/// reason }`"; §4: path and mesh immutable by construction; §2.4 "Refused before the claim ...: nothing
/// written"): the left `mesh1.rpc.2` (its connections deleted, its mesh left) asked for the names
/// `Two` (outside the grammar), `rpc.2` and
/// `2.older` (whose join at its path `mesh1.rpc` is no `path.name` this build keys) is refused
/// `RejectedName` naming the name each time; its row stays at `mesh1.rpc.2`, its id index names that
/// key, and the view keeps it there. What must NOT happen: a row written at any other key, or the path
/// moved.
#[test]
fn a_name_outside_the_grammar_or_the_path_name_shape_is_refused_by_name_and_nothing_is_written() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let left = &b["rpc2"];
        let node = left_with_row(&rig, left, &[]).await;
        for bad in ["Two", "rpc.2", "2.older"] {
            let refused = reply(&call(&rig, &b["admin2"], &update_name(left, bad)).await);
            assert!(matches!(&refused, StatusReply::RejectedName { reason } if reason.contains(bad)), "{bad}: {refused:?}");
        }
        assert_eq!(rig.nodes.node_row_name("mesh1", &left.node.node_id).await.unwrap(), Some(left.node.name.clone()), "the node's row stays at its key");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == left.node.node_id && n.name == left.node.name), "the view keeps the node at its name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["rejected-name"; 3], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} name=Two outcome=rejected-name", spans[0].0);
}

/// CONTRACT (proposal §2.4: "The put succeeded and the release of the old key fails or is
/// interrupted: the node holds both keys, and the destination row's `renamed_from` names the source.
/// The command ends incomplete naming the new key as claimed and the old key's release as pending";
/// §7: "the retry finishes it and continues"): the left `mesh1.rpc.2` (its connections deleted, its
/// mesh left) renamed to `2.old` while the nodes store refuses the release ends `NameNotApplied { node:
/// mesh1.rpc.2.old, answered: release-pending }` naming the old key: the row at `mesh1.rpc.2.old` holds
/// the node with `renamed_from: mesh1.rpc.2`, `mesh1.rpc.2` still holds it, and the birth is told
/// nothing yet. The same command again releases the old key, tells the birth its name and ends
/// `NameUpdated` on its `Applied`. What must NOT happen: `NodeRowRefused` (which says nothing was
/// written) after the put, a second row written, or the put rolled back.
#[test]
fn a_rename_whose_release_fails_ends_incomplete_with_both_keys_held_and_its_retry_finishes_the_release() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let stopped = &b["rpc2"];
        let node = left_with_row(&rig, stopped, &[]).await;
        let to: PathName = "mesh1.rpc.2.old".parse().unwrap();
        rig.refuse_release.store(true, Ordering::SeqCst);
        let incomplete = reply(&call(&rig, &b["admin2"], &update_name(stopped, "2.old")).await);
        assert!(
            matches!(&incomplete, StatusReply::NameNotApplied { node, answered, reason } if node == "mesh1.rpc.2.old" && answered == "release-pending" && reason.contains("mesh1.rpc.2 ")),
            "the command ends incomplete naming the new key and the pending release: {incomplete:?}"
        );
        let moved = rig.nodes.node_row_at(&to).await.unwrap().expect("the put at the new key stands");
        assert_eq!((moved.node_id.clone(), moved.renamed_from.clone()), (stopped.node.node_id.clone(), Some(stopped.node.name.clone())));
        assert_eq!(rig.nodes.node_row_at(&stopped.node.name).await.unwrap().map(|r| r.node_id), Some(stopped.node.node_id.clone()), "the node holds both keys");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing while the release is pending");
        let done = reply(&call(&rig, &b["admin2"], &update_name(stopped, "2.old")).await);
        assert_eq!(done, StatusReply::NameUpdated { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.node_row_at(&stopped.node.name).await.unwrap(), None, "the retry released the old key");
        assert_eq!(rig.nodes.node_row_at(&to).await.unwrap(), Some(moved), "the destination row stands as written");
        assert_eq!(node.seen.lock().unwrap().clone(), vec![apply_name(stopped, &to)], "the retry told the birth its name");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("answered").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("name-not-applied".to_string(), "release-pending".to_string()), ("name-updated".to_string(), "applied".to_string())], "{spans:?}");
    let releases = cap.spans("rdm.node_admin.node.name.update.via-pending-release");
    assert_eq!(releases.iter().map(|(_, a)| (a.get("from").cloned().unwrap_or_default(), a.get("outcome").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("mesh1.rpc.2".to_string(), "released".to_string())], "{releases:?}");
    eprintln!("trace {}: {NAME_SPAN} to=mesh1.rpc.2.old outcome=name-not-applied answered=release-pending; trace {}: rdm.node_admin.node.name.update.via-pending-release from=mesh1.rpc.2 outcome=released", spans[0].0, releases[0].0);
}

/// CONTRACT (proposal §2.1 "Claim and move": "a row whose `renamed_from` key still holds the same node
/// id has a pending release, and the writer releases it ... It runs wherever a writer meets the row";
/// §2.4: "the release is finished by whichever writer next meets the row"): the left `mesh1.rpc.2`
/// renamed to `2.old` while the nodes store refuses the release ends `release-pending`, both keys
/// held; the next `node.config.update` of the node releases `mesh1.rpc.2` before its put. Renamed back
/// to `2` under a second refused release, `mesh1.rpc.2.old` is held twice over the same way, and the
/// next `node.tags.create` releases it before its put. One
/// `rdm.node_admin.node.name.update.via-pending-release` span names each release, `moment=config-put`
/// and `moment=tags-put`. What must NOT happen: a config or tags put leaving the old key held until the
/// next rename or a successor's rebuild.
#[test]
fn a_config_or_tags_put_that_meets_a_pending_release_finishes_it() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let stopped = &b["rpc2"];
        let _node = left_with_row(&rig, stopped, &[]).await;
        let (base, old): (PathName, PathName) = (stopped.node.name.clone(), "mesh1.rpc.2.old".parse().unwrap());
        let holds = |name: PathName| {
            let nodes = rig.nodes.clone();
            async move { nodes.node_row_at(&name).await.unwrap().map(|r| r.node_id) }
        };
        rig.refuse_release.store(true, Ordering::SeqCst);
        let pending = reply(&call(&rig, &b["admin2"], &update_name(stopped, "2.old")).await);
        assert!(matches!(&pending, StatusReply::NameNotApplied { answered, .. } if answered == "release-pending"), "{pending:?}");
        assert_eq!(holds(base.clone()).await, Some(stopped.node.node_id.clone()), "the release of {base} is pending");
        let config = StatusRequest::UpdateNodeConfig { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap(), config: [("zone".to_string(), "a".to_string())].into() };
        let configured = reply(&call(&rig, &b["admin2"], &config).await);
        assert_eq!(holds(base.clone()).await, None, "the config put released {base}: {configured:?}");
        assert_eq!(rig.nodes.node_row_at(&old).await.unwrap().map(|r| r.config), Some([("zone".to_string(), "a".to_string())].into()), "the config put wrote the row at {old}");

        rig.refuse_release.store(true, Ordering::SeqCst);
        let back = reply(&call(&rig, &b["admin2"], &update_name(stopped, "2")).await);
        assert!(matches!(&back, StatusReply::NameNotApplied { answered, .. } if answered == "release-pending"), "{back:?}");
        assert_eq!(holds(old.clone()).await, Some(stopped.node.node_id.clone()), "the release of {old} is pending");
        let tagged = reply(&call(&rig, &b["admin2"], &create_tags(stopped, &["red"])).await);
        assert_eq!(tagged, StatusReply::TagsCreated { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap(), already_carried: Default::default() });
        assert_eq!(holds(old.clone()).await, None, "the tags put released {old}");
        assert_eq!(rig.nodes.node_row_at(&base).await.unwrap().map(|r| r.tags), Some(labels(&["red"])), "the tags put wrote the row at {base}");
    });
    let releases = cap.spans("rdm.node_admin.node.name.update.via-pending-release");
    let seen: Vec<(String, String, String)> = releases.iter().map(|(_, a)| (a.get("moment").cloned().unwrap_or_default(), a.get("from").cloned().unwrap_or_default(), a.get("outcome").cloned().unwrap_or_default())).collect();
    assert_eq!(seen, vec![("config-put".to_string(), "mesh1.rpc.2".to_string(), "released".to_string()), ("tags-put".to_string(), "mesh1.rpc.2.old".to_string(), "released".to_string())], "{releases:?}");
    eprintln!("trace {}: rdm.node_admin.node.name.update.via-pending-release moment=config-put from=mesh1.rpc.2 outcome=released; trace {}: moment=tags-put from=mesh1.rpc.2.old outcome=released", releases[0].0, releases[1].0);
}

/// CONTRACT (proposal §2.1 Idempotency: "A repeat naming the name the row already holds ... for a
/// live node, resends `ApplyNodeName` and completes on `Applied` or `AlreadyApplied`"; §2.3 "Row
/// written, apply unresolved ... the resubmit resends `ApplyNodeName`, and completes on `Applied` or
/// `AlreadyApplied` from the node, never from the row"): the successor `mesh1.rpc.8`, its
/// connections deleted and its mesh left, renamed to `3` answers the apply `NotReady`: the command ends `NameNotApplied {
/// node: mesh1.rpc.3, answered: not-ready }` with the row moved. The same command again resends
/// `ApplyNodeName`, which the node answers `AlreadyApplied`, and ends `NameUpdated`. What must NOT
/// happen: `NameUpdated` on the row alone, or the resubmit refused because the row already holds the name.
#[test]
fn a_same_name_retry_after_an_unresolved_apply_resends_the_apply_and_completes_on_the_nodes_answer() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let mut succ = birth("mesh1.rpc.8", NodeKind::RpcNode, "mesh1", false, false);
        (succ.node.status, succ.node.declared) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()));
        rig.topology.write().await.nodes.push(succ.node.clone());
        let applies = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let a = applies.clone();
        let node = serve_node_with(&succ, move |req| match req {
            StatusRequest::DeleteConnections { .. } => StatusReply::ConnectionsDeleted { closed: vec![], not_open: vec![], held: vec![], receipt: None },
            StatusRequest::LeaveMesh { node_id, incarnation, .. } => StatusReply::MeshLeft { node_id: node_id.clone(), incarnation: incarnation.clone(), left: vec![rdm_core::status::WhichMesh::Own] },
            StatusRequest::ApplyNodeName { .. } if a.fetch_add(1, Ordering::SeqCst) == 0 => StatusReply::NotReady { reason: "the node's digest does not exist yet".into() },
            _ => StatusReply::AlreadyApplied,
        })
        .await;
        configs_door(&rig, &node).await;
        assert!(matches!(reply(&call(&rig, &b["admin2"], &delete_all(&succ)).await), StatusReply::ConnectionsDeleted { .. }));
        let leave = leave_mesh(&succ, "node.mesh.leave:mesh1.rpc.8");
        assert!(matches!(rig.authority.carry_mesh_leave(&succ.node.node_id, succ.node.incarnation_id.as_ref().unwrap(), "3vzt99cnx0xe", 1, "node.mesh.leave:mesh1.rpc.8").await, Ok(StatusReply::MeshLeft { .. })));
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        let incomplete = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert!(matches!(&incomplete, StatusReply::NameNotApplied { node, answered, .. } if node == "mesh1.rpc.3" && answered == "not-ready"), "{incomplete:?}");
        assert_eq!(rig.nodes.node_row_at(&taken).await.unwrap().map(|r| r.node_id), Some(succ.node.node_id.clone()), "the row moved");
        let done = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(done, StatusReply::NameUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() });
        let apply = StatusRequest::ApplyNodeName { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap(), name: taken.to_string() };
        assert_eq!(node.seen.lock().unwrap().clone(), vec![delete_all(&succ), leave, apply.clone(), apply], "the resubmit resent the apply");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("answered").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("name-not-applied".to_string(), "not-ready".to_string()), ("name-updated".to_string(), "already-applied".to_string())], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} to=mesh1.rpc.3 answered=not-ready outcome=name-not-applied; trace {}: {NAME_SPAN} answered=already-applied outcome=name-updated", spans[0].0, spans[1].0);
}

fn labels(l: &[&str]) -> std::collections::BTreeSet<String> {
    l.iter().map(|s| s.to_string()).collect()
}

/// CONTRACT (R-PC1, states.md "The initial fabric before ready-for-traffic"; proposal §1: each command
/// "is refused `fabric-not-ready` until the initial fabric has first reached ready-for-traffic"): until
/// then, a caller's `node.name.update` of the left `mesh1.rpc.2` (its connections deleted, its mesh
/// left) at its mesh-primary is refused
/// `RejectedFabricNotReady`, naming the workflow with its node and the fabric's status (state-sync).
/// Nothing is written: no row at `mesh1.rpc.2.old`, the row at `mesh1.rpc.2` as it was. One
/// `rdm.node_admin.node.reject.via-fabric-not-ready` span says the same, and the name span ends
/// `rejected-fabric-not-ready`. Once the fabric has first reached ready-for-traffic the same rename ends
/// `NameUpdated`, and it still does when a resize has put the fabric back to pending. What must NOT
/// happen: a row written to a scratchpad the successor of this mesh-primary cannot read.
#[test]
fn a_node_name_update_before_the_fabric_first_reaches_ready_for_traffic_is_refused_by_name_and_writes_nothing() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let rpc2 = &b["rpc2"];
        let _node = left_with_row(&rig, rpc2, &[]).await;
        rig.topology.write().await.fabric.status = ScopeStatus::StateSync;
        let to: PathName = "mesh1.rpc.2.old".parse().unwrap();

        let refused = reply(&call(&rig, &b["admin2"], &update_name(rpc2, "2.old")).await);
        assert_eq!(refused, StatusReply::RejectedFabricNotReady { workflow: "node.name.update mesh1.rpc.2".into(), status: "state-sync".into() });
        assert_eq!(rig.nodes.node_row_at(&to).await.unwrap(), None, "no row is written");
        assert_eq!(rig.nodes.node_row_at(&rpc2.node.name).await.unwrap().map(|r| r.node_id), Some(rpc2.node.node_id.clone()), "the row stands at its key");

        // The fabric reaches ready-for-traffic: the same rename is admitted.
        rig.authority.first_ready.reach("the test fabric reached ready-for-traffic");
        rig.topology.write().await.fabric.status = ScopeStatus::Pending;
        let renamed = reply(&call(&rig, &b["admin2"], &update_name(rpc2, "2.old")).await);
        assert_eq!(renamed, StatusReply::NameUpdated { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap() }, "admitted once the fabric has first been ready-for-traffic, a resize notwithstanding");
        assert_eq!(rig.nodes.node_row_at(&to).await.unwrap().map(|r| r.node_id), Some(rpc2.node.node_id.clone()));
    });
    let rejects = cap.spans("rdm.node_admin.node.reject.via-fabric-not-ready");
    assert_eq!(rejects.len(), 1, "{rejects:?}");
    let (trace_id, attrs) = &rejects[0];
    assert_eq!((attrs.get("workflow").map(String::as_str), attrs.get("status").map(String::as_str)), (Some("node.name.update mesh1.rpc.2"), Some("state-sync")), "{attrs:?}");
    let updates = cap.spans(NAME_SPAN);
    let outcomes: Vec<_> = updates.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect();
    assert_eq!(outcomes, vec!["rejected-fabric-not-ready".to_string(), "name-updated".to_string()], "{updates:?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.reject.via-fabric-not-ready workflow=node.name.update mesh1.rpc.2 status=state-sync; {NAME_SPAN} outcome=rejected-fabric-not-ready");
}

fn create_tags(b: &Birth, l: &[&str]) -> StatusRequest {
    StatusRequest::CreateTags { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), labels: labels(l) }
}

fn delete_tags(b: &Birth, scope: rdm_core::status::TagScope) -> StatusRequest {
    StatusRequest::DeleteTags { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), scope }
}

/// CONTRACT (proposal node-properties-own-commands.md §1: "Each command goes to the node's
/// mesh-primary as the row writer ..., is refused `fabric-not-ready` until the initial fabric has first
/// reached ready-for-traffic"; §3: tags and config have no precondition beyond the fabric being ready,
/// and neither touches the name; node-rpc-envelope.md node.tags: "Create is a blind put of that key, so
/// creating a label the node already carries is ignored and writes no duplicate; delete tombstones the
/// named keys ... a node carries at most 32; a label ... past the 32, is refused by name and nothing is
/// written"): while the initial fabric is in state-sync, a caller's `node.tags.create`,
/// `node.tags.delete` and `node.config.update` of `mesh1.rpc.1` at its mesh-primary are each refused
/// `RejectedFabricNotReady` naming the workflow with its node and the fabric's status, and nothing is
/// written or told. Once the fabric has first reached ready-for-traffic: create {rabbit} ends
/// `TagsCreated` and the tags index answers rabbit; create {rabbit, red} names rabbit already carried;
/// delete {rabbit, blue} names blue not carried and rabbit leaves the index; a create that would carry
/// the node past 32 labels is refused `RejectedTags` naming the count and writes nothing; delete
/// `All` leaves the row with no label and the index with none of its labels. The node is told
/// nothing by any tags command. One `rdm.node_admin.node.reject.via-fabric-not-ready` span per refusal;
/// each tags command's `via-writer` span ends with the reply's name. What must NOT happen: a row
/// written before the fabric first reached ready-for-traffic, a duplicate label row, a label past the
/// cap, or a tags command that calls the node.
#[test]
fn node_tags_and_config_commands_before_the_fabric_first_reaches_ready_for_traffic_are_refused_by_name_and_after_it_write_the_row_and_its_tags_index() {
    use rdm_core::status::TagScope;
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let rpc1 = &b["rpc1"];
        let node = serve_node(rpc1).await;
        configs_door(&rig, &node).await;
        rig.topology.write().await.fabric.status = ScopeStatus::StateSync;
        let not_ready = |workflow: &str| StatusReply::RejectedFabricNotReady { workflow: format!("{workflow} mesh1.rpc.1"), status: "state-sync".into() };
        assert_eq!(reply(&call(&rig, &b["admin2"], &create_tags(rpc1, &["rabbit"])).await), not_ready("node.tags.create"));
        assert_eq!(reply(&call(&rig, &b["admin2"], &delete_tags(rpc1, TagScope::All)).await), not_ready("node.tags.delete"));
        let config = StatusRequest::UpdateNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config: [("zone".to_string(), "a".to_string())].into() };
        assert_eq!(reply(&call(&rig, &b["admin2"], &config).await), not_ready("node.config.update"));
        assert_eq!(rig.nodes.node_row_at(&rpc1.node.name).await.unwrap(), None, "no row is written");
        assert!(rig.nodes.tagged("rabbit").await.unwrap().is_empty(), "no tags index entry is written");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");

        rig.authority.first_ready.reach("the test fabric reached ready-for-traffic");
        let (id, birth) = (rpc1.node.node_id.clone(), rpc1.node.incarnation_id.clone().unwrap());
        let created = |already: &[&str]| StatusReply::TagsCreated { node_id: id.clone(), incarnation: birth.clone(), already_carried: labels(already) };
        let deleted = |not: &[&str]| StatusReply::TagsDeleted { node_id: id.clone(), incarnation: birth.clone(), not_carried: labels(not) };
        let row_tags = || async { rig.nodes.node_row_at(&rpc1.node.name).await.unwrap().map(|r| r.tags).unwrap_or_default() };
        assert_eq!(reply(&call(&rig, &b["admin2"], &create_tags(rpc1, &["rabbit"])).await), created(&[]));
        assert_eq!(rig.nodes.tagged("rabbit").await.unwrap(), [id.clone()].into());
        assert_eq!(reply(&call(&rig, &b["admin2"], &create_tags(rpc1, &["rabbit", "red"])).await), created(&["rabbit"]));
        assert_eq!(row_tags().await, labels(&["rabbit", "red"]));
        assert_eq!(reply(&call(&rig, &b["admin2"], &delete_tags(rpc1, TagScope::Labels(labels(&["rabbit", "blue"])))).await), deleted(&["blue"]));
        assert!(rig.nodes.tagged("rabbit").await.unwrap().is_empty(), "rabbit's key is tombstoned");
        assert_eq!(row_tags().await, labels(&["red"]));
        let thirty_two: Vec<String> = (0..32).map(|i| format!("t{i}")).collect();
        let over = reply(&call(&rig, &b["admin2"], &create_tags(rpc1, &thirty_two.iter().map(String::as_str).collect::<Vec<_>>())).await);
        assert!(matches!(&over, StatusReply::RejectedTags { reason } if reason.contains("33 tag labels, over the 32")), "a create past the cap is refused naming the count: {over:?}");
        assert_eq!(row_tags().await, labels(&["red"]), "a refused create writes nothing");
        assert!(rig.nodes.tagged("t0").await.unwrap().is_empty(), "no label of a refused create is in the index");
        assert_eq!(reply(&call(&rig, &b["admin2"], &delete_tags(rpc1, TagScope::All)).await), deleted(&[]));
        assert!(row_tags().await.is_empty(), "the row carries no label");
        assert!(rig.nodes.tagged("red").await.unwrap().is_empty(), "red's key is tombstoned");
        assert!(node.seen.lock().unwrap().is_empty(), "no tags command tells the node anything");
    });
    let rejects = cap.spans("rdm.node_admin.node.reject.via-fabric-not-ready");
    let workflows: Vec<_> = rejects.iter().map(|(_, a)| a.get("workflow").cloned().unwrap_or_default()).collect();
    assert_eq!(workflows, vec!["node.tags.create mesh1.rpc.1".to_string(), "node.tags.delete mesh1.rpc.1".into(), "node.config.update mesh1.rpc.1".into()], "{rejects:?}");
    let outcomes = |name: &str| cap.spans(name).iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>();
    assert_eq!(outcomes("rdm.node_admin.node.tags.create.via-writer"), vec!["rejected-fabric-not-ready", "tags-created", "tags-created", "rejected-tags"]);
    assert_eq!(outcomes("rdm.node_admin.node.tags.delete.via-writer"), vec!["rejected-fabric-not-ready", "tags-deleted", "tags-deleted"]);
    let created = cap.spans("rdm.node_admin.node.tags.create.via-writer");
    eprintln!("trace {}: rdm.node_admin.node.reject.via-fabric-not-ready workflow=node.tags.create mesh1.rpc.1 status=state-sync; trace {}: rdm.node_admin.node.tags.create.via-writer outcome=tags-created arrived=rabbit", rejects[0].0, created[1].0);
}

/// CONTRACT (R-PC1, states.md "The initial fabric before ready-for-traffic": the ready-for-traffic row
/// among the mesh's status rows is the durable fact that the initial fabric first reached
/// ready-for-traffic; proposal Part C1 step 2's R-PB1 pre-check is decided on the same fact): mesh1's
/// primary has not heard the fabric ready (its view still holds the initial fabric in state-commit, as
/// the fabric state gossip brings can lag the row, and it never knew it ready) while its mesh's status
/// rows already hold the ready-for-traffic row. A caller's `node.name.update` of the left
/// `mesh1.rpc.2` (its connections deleted, its mesh left) is decided on the row and ends `NameUpdated`,
/// the admin saying how it knows
/// (`rdm.node_admin.fabric.update.via-first-ready`, reason `ready-for-traffic-row-on-store-root`). What
/// must NOT happen: `RejectedFabricNotReady` for a fabric whose ready row is on the root.
#[test]
fn a_node_name_update_at_a_mesh_primary_that_has_not_heard_the_fabric_ready_is_decided_on_the_ready_row_on_its_root() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let rpc2 = &b["rpc2"];
        let _node = left_with_row(&rig, rpc2, &[]).await;
        rig.topology.write().await.fabric.status = ScopeStatus::StateCommit;
        let fabric_id = rig.topology.read().await.fabric.id.clone();
        rig.storage.put_fabric_event(&rdm_admin::status_storage::FabricEventRow { fabric_id, event: "ready-for-traffic".into() }).await.unwrap();
        let renamed = reply(&call(&rig, &b["admin2"], &update_name(rpc2, "2.old")).await);
        assert_eq!(renamed, StatusReply::NameUpdated { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap() }, "decided on the ready row on the root");
    });
    assert!(cap.spans("rdm.node_admin.node.reject.via-fabric-not-ready").is_empty(), "no refusal");
    let ready: Vec<_> = cap.spans("rdm.node_admin.fabric.update.via-first-ready").into_iter().map(|(t, a)| (t, a.get("reason").cloned().unwrap_or_default())).collect();
    assert_eq!(ready.iter().map(|(_, r)| r.as_str()).collect::<Vec<_>>(), vec!["ready-for-traffic-row-on-store-root"], "{ready:?}");
    let updates = cap.spans(NAME_SPAN);
    assert_eq!(updates.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["name-updated".to_string()], "{updates:?}");
    eprintln!("trace {}: rdm.node_admin.fabric.update.via-first-ready reason=ready-for-traffic-row-on-store-root; trace {}: {NAME_SPAN} outcome=name-updated", ready[0].0, updates[0].0);
}

/// CONTRACT (R-TG1; proposal §3: tags and config "Neither has a precondition beyond the fabric being
/// ready, neither touches the name"; node-identity-and-state.md "A rewrite is lossless for every field its
/// writer knows"): a caller's `node.tags.create { rabbit, red }` of the live `mesh1.rpc.1` writes its row
/// and the tags index with the birth told nothing. A `node.config.update` of rpc.1 then writes its row
/// with the new config and the same tags, the index unchanged. The left `mesh1.rpc.2` (its connections
/// deleted, its mesh left), whose row carries {red}, renamed to `2.old` keeps its tags at the row's new
/// name and in the index. What must NOT happen: a tags command sending `ApplyNodeName`, or a config put
/// or a rename dropping the tags.
#[test]
fn a_tags_create_tells_the_node_nothing_and_a_config_put_or_a_rename_keeps_the_tags() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (rpc1, rpc2) = (&b["rpc1"], &b["rpc2"]);
        let (node, rpc2_node) = (serve_node(rpc1).await, serve_node(rpc2).await);
        configs_door_for(&rig, &[&node, &rpc2_node]).await;
        let tagged = |l: &'static str| {
            let nodes = rig.nodes.clone();
            async move { nodes.tagged(l).await.unwrap() }
        };

        let created = reply(&call(&rig, &b["admin2"], &create_tags(rpc1, &["rabbit", "red"])).await);
        assert_eq!(created, StatusReply::TagsCreated { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), already_carried: Default::default() });
        assert!(node.seen.lock().unwrap().is_empty(), "a tags command tells the birth nothing: {:?}", node.seen.lock().unwrap());
        let row = rig.nodes.node_row_at(&rpc1.node.name).await.unwrap().expect("mesh1's primary wrote rpc.1's row");
        assert_eq!((row.node_id.clone(), row.tags.clone()), (rpc1.node.node_id.clone(), labels(&["rabbit", "red"])), "the row holds the tags the create added");
        assert_eq!(tagged("rabbit").await, [rpc1.node.node_id.clone()].into(), "the tags index names rpc.1 under rabbit");

        let config: BTreeMap<String, String> = [("zone".to_string(), "c".to_string())].into();
        let req = StatusRequest::UpdateNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config: config.clone() };
        assert_eq!(reply(&call(&rig, &b["admin2"], &req).await), StatusReply::ConfigUpdated { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap() });
        let row = rig.nodes.node_row_at(&rpc1.node.name).await.unwrap().expect("rpc.1's row");
        assert_eq!((row.config, row.tags), (config, labels(&["rabbit", "red"])), "the config put writes the new config and keeps the tags");
        assert_eq!(tagged("rabbit").await, [rpc1.node.node_id.clone()].into(), "the index is unchanged by a config put");

        hold_left(&rig, rpc2).await;
        let row = rdm_admin::storage::NodeRow { name: rpc2.node.name.clone(), node_id: rpc2.node.node_id.clone(), status: NodeStatus::ConnectionsDeleting, config: Default::default(), tags: labels(&["red"]), allocated: None, renamed_from: None, app_types: None, inherited_by: None };
        rig.nodes.put_node_row(&row).await.unwrap().unwrap();
        let to: PathName = "mesh1.rpc.2.old".parse().unwrap();
        let renamed = reply(&call(&rig, &b["admin2"], &update_name(rpc2, "2.old")).await);
        assert_eq!(renamed, StatusReply::NameUpdated { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.node_row_at(&to).await.unwrap().map(|r| r.tags), Some(labels(&["red"])), "the row at the new name keeps the tags");
        assert_eq!(tagged("red").await, [rpc1.node.node_id.clone(), rpc2.node.node_id.clone()].into(), "the index still names the renamed node: it is keyed by node id");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("answered").cloned())).collect::<Vec<_>>(), vec![("name-updated".to_string(), Some("applied".to_string()))], "{spans:?}");
    let creates = cap.spans("rdm.node_admin.node.tags.create.via-writer");
    assert_eq!(creates.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["tags-created".to_string()], "{creates:?}");
    let configs = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(configs.iter().map(|(_, a)| a.get("tags").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["rabbit,red".to_string()], "the config put wrote the tags it holds: {configs:?}");
    eprintln!("trace {}: rdm.node_admin.node.tags.create.via-writer arrived=rabbit,red outcome=tags-created (no apply sent)", creates[0].0);
    eprintln!("trace {}: rdm.node_admin.node.update.via-config tags=rabbit,red outcome=config-updated; trace {}: {NAME_SPAN} to=mesh1.rpc.2.old outcome=name-updated", configs[0].0, spans[0].0);
}

/// CONTRACT (proposal §2.2 step 6: "A live connections-deleted node is sent `ApplyNodeName`";
/// node_config.rs: the mesh-primary is a member of its mesh and the writer of its own row, and applies
/// its own config in-process): a caller's `node.name.update` renaming mesh1's primary itself, ConnectionsDeleting
/// and holding the `node.connections.deleted` of an `All` cut that left nothing held and its own
/// `node.mesh.left`, to the name
/// `1.old` ends `NameUpdated`: the row moves, and the mesh-primary takes the name through its own name
/// decision in-process, which is asked exactly that name. What must NOT happen: the mesh-primary
/// calling itself over Node RPC ("Connecting to ourself is not supported") and ending `NameNotApplied`
/// with its row renamed and its name not.
#[test]
fn a_mesh_primarys_rename_of_itself_is_applied_in_process() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let node = serve_node(&b["rpc1"]).await;
        configs_door(&rig, &node).await;
        let me = &rig.admin;
        let inc = me.node.incarnation_id.clone().unwrap();
        if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == me.node.node_id) {
            (n.status, n.declared) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()));
        }
        rig.authority.declared.lock().unwrap().connections_deleted.insert((me.node.node_id.clone(), inc.clone()), rdm_admin::status_rpc::ConnectionsDeletedReceipt { all: true, held: vec![], status_at_cut: NodeStatus::ConnectionsDeleting });
        rig.authority.declared.lock().unwrap().meshes_left.insert((me.node.node_id.clone(), inc.clone()));
        let asked: Arc<Mutex<Vec<PathName>>> = Arc::default();
        let a = asked.clone();
        let held: PathName = me.node.name.clone();
        let _ = rig.authority.rename.set(Arc::new(move |name: PathName| {
            let (a, held) = (a.clone(), held.clone());
            Box::pin(async move {
                a.lock().unwrap().push(name);
                (held, rdm_admin::node_name::NameApply::Applied)
            })
        }));
        let to: PathName = "mesh1.admin.1.old".parse().unwrap();
        let update = StatusRequest::UpdateName { node_id: me.node.node_id.clone(), incarnation: inc.clone(), name: "1.old".into() };
        let renamed = reply(&call(&rig, &b["admin2"], &update).await);
        assert_eq!(renamed, StatusReply::NameUpdated { node_id: me.node.node_id.clone(), incarnation: inc });
        assert_eq!(rig.nodes.node_row_at(&to).await.unwrap().map(|r| r.node_id), Some(me.node.node_id.clone()), "the row moved to the new name");
        assert_eq!(asked.lock().unwrap().clone(), vec![to.clone()], "the mesh-primary's own name decision was asked the new name, once");
        assert!(node.seen.lock().unwrap().is_empty(), "no other node is told anything");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace, attrs) = &spans[0];
    assert_eq!((attrs.get("outcome").map(String::as_str), attrs.get("apply").map(String::as_str), attrs.get("answered").map(String::as_str)), (Some("name-updated"), Some("in-process"), Some("applied")), "{attrs:?}");
    eprintln!("trace {trace}: {NAME_SPAN} from=mesh1.admin.1 to=mesh1.admin.1.old apply=in-process answered=applied outcome=name-updated");
}

/// CONTRACT (R12; proposal node-properties-own-commands.md §2.2 step 6, the ApplyNodeName answers):
/// `ApplyNodeName` naming this admin, from another node-admin, is answered by the admin itself from
/// what its name decision says: `Applied` once it holds the name, `AlreadyApplied` for the name it
/// holds, `BusyHoldingConnections` naming each held connection by id while it holds any, and the same from a member (nothing checks who is calling,
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
        let decisions = Arc::new(Mutex::new(vec![rdm_admin::node_name::NameApply::AlreadyApplied, rdm_admin::node_name::NameApply::Busy { connections: vec![rdm_core::ConnectionId::parse("3123456789ab").unwrap(), rdm_core::ConnectionId::parse("4123456789ab").unwrap()], routes: vec!["mesh1.rpc.1 (direct, entry 3123456789ab)".into(), "mesh1.rpc.2 (direct, entry 4123456789ab)".into()] }, rdm_admin::node_name::NameApply::AlreadyApplied, rdm_admin::node_name::NameApply::Applied]));
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
        assert_eq!(
            reply(&call(&rig, admin2, &told(inc.clone(), "mesh1.admin.1.old")).await),
            StatusReply::BusyHoldingConnections { connections: vec![rdm_core::ConnectionId::parse("3123456789ab").unwrap(), rdm_core::ConnectionId::parse("4123456789ab").unwrap()] }
        );
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

/// What a node-left call is answered: no command, by name.
const NODE_LEFT_EXPECTED: &str = "no command: node-left is never a call; a stopped birth reports left on the reply of its stop-node call";

/// CONTRACT (node-rpc-envelope.md, "Node stop command pair": "NodeLeft keeps its position and is never
/// sent; a receiver refuses it by name"; R-NR1: NotReady is the readiness gate's answer only): a
/// `NodeLeft` call reaching the node-admin, from the birth it names, is refused
/// `RejectedUnmatchedCompletion { field: "command" }`, the receiver holding no command a node-left
/// could complete, with what it holds instead (`expected`) and what the call reported (`reported`). The
/// admin holds an open stop-node of that exact birth under the same key, and it stays open: the stop's
/// `Stopped` reply is its completion, never a node-left call. The refusal is the admin's reject span
/// `rdm.node_admin.status.reject.via-node-left` naming the field, expected, reported, the subject (its
/// path.name as the view holds it) and the caller. What must NOT happen: NotReady (the door is serving), the open stop completed, or a
/// completion accepted.
#[test]
fn a_node_left_call_is_refused_by_name_and_completes_no_open_stop() {
    let cap = capture();
    let (stop_open, reply) = cap.run(async {
        let (rig, b) = rig().await;
        let rpc1 = &b["rpc1"];
        let incarnation = rpc1.node.incarnation_id.clone().unwrap();
        let key = rdm_admin::node_commands::CommandKey { build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into(), node_id: rpc1.node.node_id.clone(), incarnation: incarnation.clone() };
        let completed = rig.authority.commands.open(key);
        let left = StatusRequest::NodeLeft { node_id: rpc1.node.node_id.clone(), incarnation, build_id: "66h8qrqvaajk".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        let reply = reply(&call(&rig, rpc1, &left).await);
        let stop_open = !*completed.borrow();
        (stop_open, reply)
    });
    assert_eq!(
        reply,
        StatusReply::RejectedUnmatchedCompletion { field: "command".into(), expected: NODE_LEFT_EXPECTED.into(), reported: "node-left node.stop:mesh1.rpc.1 (build 66h8qrqvaajk, attempt 1)".into() },
        "a node-left call is refused by name"
    );
    assert!(stop_open, "the open stop-node of the birth is not completed by a node-left call");
    let spans = cap.spans("rdm.node_admin.status.reject.via-node-left");
    assert_eq!(spans.len(), 1, "one reject span: {spans:?}");
    let (trace_id, attrs) = &spans[0];
    let a = |k: &str| attrs.get(k).cloned().unwrap_or_default();
    assert_eq!(
        (a("node"), a("op"), a("field"), a("expected"), a("reported"), a("name"), a("caller"), a("operation"), a("build_id"), a("attempt")),
        ("mesh1.admin.1".to_string(), "node-left".to_string(), "command".to_string(), NODE_LEFT_EXPECTED.to_string(), "node-left node.stop:mesh1.rpc.1 (build 66h8qrqvaajk, attempt 1)".to_string(), "mesh1.rpc.1".to_string(), "mesh1.rpc.1".to_string(), "node.stop:mesh1.rpc.1".to_string(), "66h8qrqvaajk".to_string(), "1".to_string()),
        "{attrs:?}"
    );
    assert!(cap.spans("rdm.node_admin.status.update.via-completion-accepted").is_empty(), "no completion is accepted");
    eprintln!("trace {trace_id}: rdm.node_admin.status.reject.via-node-left field=command reported=node-left node.stop:mesh1.rpc.1 (build 66h8qrqvaajk, attempt 1)");
}

/// CONTRACT (build.md §4: the current Build is the first in progress or completed, read last in,
/// first out; R-BI3, docs/plans/rulings/i143-rulings-2026-10-12-rx-authz.md: "Each accepted Build is its
/// own entity in the builds store, written once" and "The Fabric row carries no pointer to the newest
/// Build"; node-rpc-envelope.md typed objects: `fabric.get` answers the fabric): `fabric.get` (op 0x1B
/// `GetFabric`) at a node-admin answers the fabric's `build_id`, the current Build its own builds store
/// returns, skipping a failed Build, and an admin whose builds store holds no Build in progress or
/// completed answers none. Must NOT happen: a caller that needs the current Build reading it from
/// anywhere but the fabric's own read, or `fabric.get` naming a failed Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fabric_get_answers_the_current_build_its_builds_store_returns_and_skips_a_failed_one() {
    use rdm_admin::accepted::{AcceptedStore, FabricTopology};
    use rdm_admin::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, MemoryBuildStateAdapter};
    let admin = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes: vec![admin.node.clone()],
    };
    let builds = Arc::new(MemoryBuildStateAdapter::default());
    AcceptedStore::seeded(&*builds, view.fabric.id.clone(), FabricTopology::of_observed(&view), "mesh1.admin.1").await.unwrap();
    let held = builds.current_build().await.unwrap().into_build_id().expect("the seeded Build is current").to_string();
    let empty = Arc::new(MemoryBuildStateAdapter::default());
    let answer = |builds: Arc<dyn BuildStateAdapter>| {
        let view = view.clone();
        let admin = admin.node.clone();
        async move {
            let authority = StatusAuthority {
                certs: None,
                app_channels: None,
                held_takeover: Default::default(),
                declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
                stopped: Default::default(),
                me: admin.name.clone(),
                node_id: admin.node_id.clone(),
                fabric_id: view.fabric.id.clone(),
                topology: Arc::new(tokio::sync::RwLock::new(view)),
                declared: Arc::new(Mutex::new(Declared::default())),
                status_storage: Arc::new(MemoryStatusStorage::default()),
                nodes_storage: Arc::new(MemoryNodesStorage::default()),
                mesh_ids: Arc::new(BTreeMap::new),
                durable: Default::default(),
                commands: Arc::new(rdm_admin::node_commands::CommandBook::default()),
                own: Arc::new(std::sync::OnceLock::new()),
                mesh_deleter: Arc::new(std::sync::OnceLock::new()),
                hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                wake: Default::default(),
                rounds: Default::default(),
                configs: Default::default(),
                configure: Default::default(),
                before_ready: Default::default(),
                rename: Default::default(),
                row_moved: Default::default(),
                loads: Some(Arc::new(BTreeMap::new)),
                book: None,
                builds,
                first_ready: Default::default(),
            };
            match authority.apply(None, &StatusRequest::GetFabric).await {
                StatusReply::Fabric { fabric } => fabric,
                other => panic!("fabric.get answered {other:?}"),
            }
        }
    };
    assert_eq!(answer(builds.clone()).await.build_id, Some(held.to_string()), "fabric.get answers the current Build the admin's builds store returns");
    // A newer Build that failed is skipped: the older completed one is still the current Build.
    let newer = rdm_admin::build::BuildId::mint();
    builds.publish_accepted(&BuildAccepted { build_id: newer.clone(), topology: FabricTopology::of_observed(&view), submitted_change: None, submitted_at_ms: u64::MAX - 1 }).await.unwrap();
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: newer.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    assert_eq!(answer(builds.clone()).await.build_id, Some(newer.to_string()), "the newer Build in progress is the current Build");
    builds.append_attempt_receipt(&BuildAttemptReceipt { mesh: None, build_id: newer, attempt: 1, outcome: AttemptOutcome::Failed { reason: "node.create: PrepareStorage".into(), code: rdm_core::outcome::Code::FailedPrecondition } }).await.unwrap();
    assert_eq!(answer(builds).await.build_id, Some(held.to_string()), "a failed Build is skipped: fabric.get answers the Build before it");
    assert_eq!(answer(empty).await.build_id, None, "an admin whose builds store holds no Build in progress or completed answers none");
}

/// CONTRACT (node-rpc-envelope.md Status: "`RejectedNotAuthority` says the receiver is not the
/// authority for the call: ... `SubjectUnknown` when the receiver does not hold the subject at all.
/// ... a caller-side `PeerUnresolved` likewise [keeps its codec position and a mesh receiver never
/// sends it]"; node-rpc.md §14): mesh1's primary asked to put the config of, put the record of, or
/// delete the connections of a node its view does not hold, or of a node whose birth it does not
/// know, answers `RejectedNotAuthority { SubjectUnknown }` each time: the fact is the receiver's own.
/// Must NOT happen: `PeerUnresolved`, which names a peer the caller could not resolve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subject_the_mesh_primary_does_not_hold_is_answered_subject_unknown_never_peer_unresolved() {
    let (rig, others) = rig().await;
    quiet_door(&rig).await;
    let rpc2 = &others["rpc2"];
    if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == rpc2.node.node_id) {
        n.incarnation_id = None;
    }
    let caller = &others["rpc1"];
    let unknown = (NodeId::mint(), IncarnationId::mint());
    let no_birth = (rpc2.node.node_id.clone(), IncarnationId::mint());
    for (case, (node_id, incarnation)) in [("not in the view", unknown), ("no known birth", no_birth)] {
        let requests = [
            StatusRequest::UpdateNodeConfig { node_id: node_id.clone(), incarnation: incarnation.clone(), config: [("zone".to_string(), "a".to_string())].into() },
            StatusRequest::UpdateName { node_id: node_id.clone(), incarnation: incarnation.clone(), name: "2".into() },
            StatusRequest::DeleteConnections { node_id: node_id.clone(), incarnation: incarnation.clone(), scope: rdm_core::status::ConnectionScope::All },
        ];
        for req in requests {
            let got = reply(&call(&rig, caller, &req).await);
            assert_eq!(got, StatusReply::RejectedNotAuthority { why: NotAuthority::SubjectUnknown }, "{case}: {} answers the receiver's own fact", req.op());
        }
    }
}

/// CONTRACT (docs/architecture/lifecycles/node-replace.md Inherited, Luke 2026-10-13 D2: "Config and
/// tags are inherited from the retained `<name>.old` row ... The successor's tag-index entries use its
/// own id. Retained predecessor labels are removed from the active index, not erased from its row. A
/// repeated cutover keeps the successor's already-inherited values and never overwrites a later
/// update"; Rollback step 3: the predecessor renamed to `<name>`, "its tag-index entries restored under
/// its own id"):
/// 1. The predecessor `mesh1.rpc.2`, live and off its mesh with config `{zone: a}` and tags `{rabbit,
///    red}`, is renamed `mesh1.rpc.2.old` (step 2). The successor's claim at `mesh1.rpc.2`, the name it
///    released (step 3, `ClaimFor::Successor`), carries `{zone: a}` and `{rabbit, red}` under the
///    successor's id, and the retained row at `mesh1.rpc.2.old` is marked `inherited_by` the
///    successor: it keeps `{rabbit, red}` on the row while the active tags index names only the
///    successor.
/// 2. The successor's later config and tags (`{zone: b, tier: gold}`, `{rabbit, blue}`) survive a
///    repeated claim (the create's resume), which writes nothing and finds the retained row marked.
/// 3. Rollback: the successor deleted (its `Deleted` row at `mesh1.rpc.2`), the predecessor renamed
///    back to `mesh1.rpc.2` stands there with `{zone: a}`, `{rabbit, red}` and no `inherited_by`,
///    `mesh1.rpc.2.old` is released, and the tags index names the predecessor again under its own id.
#[test]
fn a_replace_successor_inherits_the_retained_row_keeps_its_later_updates_on_a_repeated_claim_and_the_rollback_rename_restores_the_predecessor() {
    use rdm_admin::node_rows::{ClaimFor, NodeRows};
    use rdm_core::status::TagScope;
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let pred = &b["rpc2"];
        hold_left(&rig, pred).await;
        let row = rdm_admin::storage::NodeRow { name: pred.node.name.clone(), node_id: pred.node.node_id.clone(), status: NodeStatus::ConnectionsDeleting, config: Default::default(), tags: labels(&["rabbit", "red"]), allocated: None, renamed_from: None, app_types: None, inherited_by: None };
        rig.nodes.put_node_row(&row).await.unwrap().unwrap();
        let mut succ = birth("mesh1.rpc.2", NodeKind::RpcNode, "mesh1", false, false);
        succ.node.status = NodeStatus::Pending;
        let (pred_node, succ_node) = (serve_node(pred).await, serve_node(&succ).await);
        configs_door_for(&rig, &[&pred_node, &succ_node]).await;
        let pred_cfg: BTreeMap<String, String> = [("zone".to_string(), "a".to_string())].into();
        assert_eq!(
            reply(&call(&rig, &b["admin2"], &StatusRequest::UpdateNodeConfig { node_id: pred.node.node_id.clone(), incarnation: pred.node.incarnation_id.clone().unwrap(), config: pred_cfg.clone() }).await),
            StatusReply::ConfigUpdated { node_id: pred.node.node_id.clone(), incarnation: pred.node.incarnation_id.clone().unwrap() }
        );
        // Step 2: the predecessor, off its mesh, takes `.old`.
        let (base_name, old_name): (PathName, PathName) = ("mesh1.rpc.2".parse().unwrap(), "mesh1.rpc.2.old".parse().unwrap());
        assert_eq!(rig.authority.update_name(&update_name(pred, "2.old")).await, StatusReply::NameUpdated { node_id: pred.node.node_id.clone(), incarnation: pred.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.tagged("rabbit").await.unwrap(), [pred.node.node_id.clone()].into());
        assert_eq!(rig.nodes.tagged("red").await.unwrap(), [pred.node.node_id.clone()].into());

        // Step 3: the successor's claim at the name the predecessor released.
        rig.topology.write().await.nodes.push(succ.node.clone());
        let rows = NodeRows::new(rig.admin.node.name.clone(), rig.admin.node.node_id.clone(), rig.topology.clone(), rig.nodes.clone());
        let claim = || rows.claim(&base_name, &succ.node.node_id, "node.create:mesh1.rpc.2", None, ClaimFor::Successor);
        assert!(matches!(claim().await, Ok(rdm_admin::node_rows::Claimed::Written { .. })), "the successor claims the released name");
        let succ_row = rig.nodes.node_row_at(&base_name).await.unwrap().expect("successor row at mesh1.rpc.2");
        assert_eq!((succ_row.node_id.clone(), succ_row.config.clone(), succ_row.tags.clone()), (succ.node.node_id.clone(), pred_cfg.clone(), labels(&["rabbit", "red"])));
        let retained = rig.nodes.node_row_at(&old_name).await.unwrap().expect("retained predecessor row at mesh1.rpc.2.old");
        assert_eq!((retained.node_id.clone(), retained.tags.clone(), retained.inherited_by.clone()), (pred.node.node_id.clone(), labels(&["rabbit", "red"]), Some(succ.node.node_id.clone())));
        assert_eq!(rig.nodes.tagged("rabbit").await.unwrap(), [succ.node.node_id.clone()].into(), "only the successor's id is in the active index after its claim");
        assert_eq!(rig.nodes.tagged("red").await.unwrap(), [succ.node.node_id.clone()].into());

        // Later updates on the successor, then a repeated claim (the create's resume).
        let succ_cfg: BTreeMap<String, String> = [("zone".to_string(), "b".to_string()), ("tier".to_string(), "gold".to_string())].into();
        assert_eq!(
            reply(&call(&rig, &b["admin2"], &StatusRequest::UpdateNodeConfig { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap(), config: succ_cfg.clone() }).await),
            StatusReply::ConfigUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() }
        );
        assert_eq!(
            reply(&call(&rig, &b["admin2"], &create_tags(&succ, &["blue"])).await),
            StatusReply::TagsCreated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap(), already_carried: Default::default() }
        );
        assert_eq!(
            reply(&call(&rig, &b["admin2"], &delete_tags(&succ, TagScope::Labels(labels(&["red"])))).await),
            StatusReply::TagsDeleted { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap(), not_carried: Default::default() }
        );
        assert!(matches!(claim().await, Ok(rdm_admin::node_rows::Claimed::HeldByNode(_))), "a repeated claim finds the successor's own row");
        let row_after = rig.nodes.node_row_at(&base_name).await.unwrap().unwrap();
        assert_eq!((row_after.config, row_after.tags), (succ_cfg, labels(&["rabbit", "blue"])), "a repeated claim keeps the successor's later updates");
        assert!(rig.nodes.tagged("red").await.unwrap().is_empty(), "the deleted tag red is not resurrected from .old");

        // Rollback: the successor deleted, the predecessor renamed back to `mesh1.rpc.2`.
        rows.deleted(&succ.node, "node.delete:mesh1.rpc.2").await.expect("the successor's Deleted row at mesh1.rpc.2");
        assert_eq!(
            rig.authority.update_name(&update_name(pred, "2")).await,
            StatusReply::NameUpdated { node_id: pred.node.node_id.clone(), incarnation: pred.node.incarnation_id.clone().unwrap() }
        );
        let restored = rig.nodes.node_row_at(&base_name).await.unwrap().expect("predecessor row restored at mesh1.rpc.2");
        assert_eq!(
            (restored.node_id.clone(), restored.config, restored.tags, restored.inherited_by),
            (pred.node.node_id.clone(), pred_cfg, labels(&["rabbit", "red"]), None),
            "rollback restores the predecessor row at its original name with inherited_by cleared"
        );
        assert_eq!(rig.nodes.node_row_at(&old_name).await.unwrap(), None, "mesh1.rpc.2.old is released");
        assert_eq!(rig.nodes.tagged("rabbit").await.unwrap(), [pred.node.node_id.clone()].into(), "rollback restores rabbit in the tag index under the predecessor's original id");
        assert_eq!(rig.nodes.tagged("red").await.unwrap(), [pred.node.node_id.clone()].into(), "rollback restores red in the tag index under the predecessor's original id");
    });
    let marks = cap.spans("rdm.node_admin.node.update.via-inherited-row");
    assert_eq!(marks.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["inherited".to_string(), "already-inherited".to_string()], "{marks:?}");
    eprintln!("trace {}: rdm.node_admin.node.update.via-inherited-row subject=mesh1.rpc.2.old outcome=inherited", marks[0].0);
}

/// CONTRACT (i144 R22; Luke Q9: "Caller → mesh-primary: Status DeleteConnections, direct or forwarded,
/// for either scope. Mesh-primary → subject node: streaming 0x22 NodeCommand"; "a forwarded caller
/// request, and the Connections([ids]) scope, stay functional"): `mesh1.rpc.2` sends
/// `node.connections.delete { Connections([one id]) }` of `mesh1.rpc.1` to mesh1's primary through a
/// carrier (`ViaPeer` `mesh2.admin.1`, Forward 0x1A). The primary carries it to the exact birth on the
/// node command op 0x22 (`carried=dialled`) and answers the birth's completion naming the id closed;
/// the birth received the request once. A list of ids reports no connections-deleting, so nothing is
/// recorded as connections-deleting. The caller's carried leg (`via-peer`, protocol status) and the
/// primary's carrying are one trace.
#[test]
fn a_forwarded_callers_connections_delete_of_named_ids_is_carried_to_the_birth_on_its_command_op() {
    let cap = capture();
    let id = rdm_core::ConnectionId::mint();
    cap.run(async {
        let (rig, b) = rig().await;
        let (rpc1, rpc2, m2admin) = (&b["rpc1"], &b["rpc2"], &b["m2admin"]);
        let node = serve_node_with(rpc1, |req| match req {
            StatusRequest::DeleteConnections { scope: rdm_core::status::ConnectionScope::Connections(ids), .. } => StatusReply::ConnectionsDeleted { closed: ids.clone(), not_open: vec![], held: vec![], receipt: None },
            _ => StatusReply::Applied,
        })
        .await;
        configs_door(&rig, &node).await;
        let (carrier, _carrier_router) = carrier_admin(m2admin, &rig.resolved).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(carrier);
        let ep = rdm_transport::endpoint::bind(rpc2.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
        let route = rdm_transport::RouteChoice::ViaPeer { carrier: m2admin.node.node_id.clone(), path: m2admin.node.name.clone() };
        let scope = rdm_core::status::ConnectionScope::Connections(vec![id.clone()]);
        let reply = rdm_client::NodeRpc::new(&client).connections_delete(&rig.admin.node.node_id, &route, &exact(rpc1), &scope, &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::ConnectionsDeleted { closed: vec![id.clone()], not_open: vec![], held: vec![], receipt: None });
        assert_eq!(node.seen.lock().unwrap().clone(), vec![StatusRequest::DeleteConnections { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), scope }], "the birth received the request once, on its command op");
    });
    let carried = cap.spans("rdm.node_admin.node.connections.delete.via-mesh-primary");
    assert_eq!(carried.len(), 1, "{carried:?}");
    let (trace_id, attrs) = &carried[0];
    let attr = |k: &str| attrs.get(k).map(String::as_str);
    assert_eq!((attr("node"), attr("subject"), attr("scope"), attr("carried"), attr("outcome")), (Some("mesh1.admin.1"), Some("mesh1.rpc.1"), Some("connections"), Some("dialled"), Some("connections-deleted")), "{attrs:?}");
    assert!(cap.spans("rdm.node_admin.node.update.via-connections-deleting").is_empty(), "a list of ids records no connections-deleting");
    let routes = cap.spans("rdm.node_rpc.route.resolve.via-connections");
    let leg: Vec<_> = routes.iter().filter(|(t, a)| t == trace_id && a.get("route").map(String::as_str) == Some("via-peer")).collect();
    assert_eq!(leg.len(), 1, "the caller's one carried leg is in the primary's carrying trace: {routes:?}");
    assert_eq!((leg[0].1.get("carrier").map(String::as_str), leg[0].1.get("protocol").map(String::as_str)), (Some("mesh2.admin.1"), Some("status")), "{leg:?}");
    eprintln!("trace {trace_id}: rdm.node_rpc.route.resolve.via-connections route=via-peer carrier=mesh2.admin.1 > rdm.node_admin.node.connections.delete.via-mesh-primary node=mesh1.admin.1 carried=dialled outcome=connections-deleted");
}


/// i146 e2: lose the progress ack after the mesh-primary recorded it. The responder resends that
/// very frame on a new stream, not a new ConnectionsDeleting at another sequence position.
#[test]
fn a_progress_frame_delivered_twice_is_suppressed_and_the_record_is_written_once() {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply as N, NodeCommandRequest};
    let cap = capture();
    cap.run(async {
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _gossip_router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let fabric = FabricId::mint();
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(5_000_000);
        let membership = rdm_transport::membership::Membership::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), "mesh1.admin.1", Arc::new(time), vec![], &[]).await.unwrap();
        membership.set_mesh_primary(true);
        let (rig, b) = rig_with_book(Some(membership.book.clone())).await;
        let rpc = &b["rpc2"];
        assert!(membership.book.record(rdm_core::MeshDigest { held_hash: Default::default(),
            fabric_id: fabric,
            node: rdm_core::MeshNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: rpc.node.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Pending, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: None, in_flight: None, config: Default::default(), load: None, gossip: None,
        }));
        let (server_delivery, caller_delivery) = (Arc::new(OnceLock::<rdm_transport::Delivery>::new()), Arc::new(OnceLock::<rdm_transport::Delivery>::new()));
        let (sd, cd) = (server_delivery.clone(), caller_delivery.clone());
        let server = ServerBuilder::new().serve_stream::<NodeCommand, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_, req: NodeCommandRequest, sink| {
            let (sd, cd) = (sd.clone(), cd.clone());
            async move {
                let NodeCommandRequest::DeleteConnections { node_id, incarnation, .. } = req else { panic!("expected DeleteConnections") };
                let mut sink = sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while rdm_transport::DeliveryStats::get(&sd.get().unwrap().stats().acks_received) == 0 { tokio::task::yield_now().await; }
                }).await.expect("Started recovered before the progress fault is armed");
                cd.get().unwrap().seams().drop_next_response_ack();
                sink.data(N::ConnectionsDeleting { node_id, incarnation }).await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while rdm_transport::DeliveryStats::get(&sd.get().unwrap().stats().responses_resent) == 0 || sd.get().unwrap().owed_frames() != 0 { tokio::task::yield_now().await; }
                }).await.expect("the unrecovered progress is resent on a new stream and acknowledged");
                Ok(N::ConnectionsDeleted { closed: vec![], not_open: vec![], held: vec![], receipt: None })
            }
        }).seal(ServedBirth { node_id: rpc.node.node_id.to_string(), incarnation: rpc.node.incarnation_id.as_ref().unwrap().to_string() }).unwrap();
        server_delivery.set(server.delivery().clone()).unwrap_or_else(|_| panic!("one responder"));
        let ep = rdm_transport::endpoint::bind(rpc.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: rpc.key.public(), transport_addr: addr, incarnation: rpc.node.incarnation_id.clone().unwrap() });
        let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm"));
        caller_delivery.set(client.delivery().clone()).unwrap_or_else(|_| panic!("one caller"));
        assert!(rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(client, rig.authority.configure.clone(), Default::default()))).is_ok());
        let answer = call(&rig, &b["rpc1"], &StatusRequest::DeleteConnections { node_id: rpc.node.node_id.clone(), incarnation: rpc.node.incarnation_id.clone().unwrap(), scope: rdm_core::status::ConnectionScope::All }).await;
        assert!(matches!(reply(&answer), StatusReply::ConnectionsDeleted { .. }), "{answer:?}");
        assert!(rdm_transport::DeliveryStats::get(&server_delivery.get().unwrap().stats().responses_resent) >= 1);
    });
    let records = cap.spans("rdm.node_admin.node.update.via-connections-deleting");
    assert_eq!(records.len(), 1, "the mesh-primary records the repeated progress once: {records:?}");
    assert_eq!(records[0].1.get("recorded").map(String::as_str), Some("true"), "the progress changed the actual book: {records:?}");
    assert_eq!(cap.spans("rdm.mesh.membership.update.from-status-report").len(), 1, "one status write, not just one carrier span");
    assert!(!records[0].1.get("invocation_id").unwrap_or(&String::new()).is_empty(), "the record names the invocation: {records:?}");
    assert_eq!(records[0].1.get("seq").map(String::as_str), Some("1"), "the progress position: {records:?}");
}

/// Q47: a command provably never sent writes neither a lifecycle state nor an operation effect.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_never_sent_stop_or_start_writes_no_lifecycle_state() {
    use rdm_admin::deployment::pipeline::{CommandAdmission, CommandContext};
    let (rig, births) = rig().await;
    let subject = &births["rpc2"].node;
    for start in [false, true] {
        let before = rig.authority.declared.lock().unwrap().clone();
        let ctx = CommandContext { build_id: "b".into(), attempt: 1, operation: format!("node.{}:{}", if start { "start" } else { "stop" }, subject.name) };
        assert!(matches!(rig.authority.carry_lifecycle(subject, start, &ctx).await, CommandAdmission::NotSent { .. }));
        assert_eq!(*rig.authority.declared.lock().unwrap(), before);
        assert!(rig.storage.node_declared().await.unwrap().is_empty());
        assert!(rig.storage.node_operations().await.unwrap().is_empty());
    }
}

/// Q47: Status's former stop/start positions are reserved, even for an exact held birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_stop_and_start_positions_are_refused_by_name_without_execution() {
    let (rig, births) = rig().await;
    let subject = &births["rpc2"];
    for start in [false, true] {
        let request = if start { StatusRequest::StartNode { node_id: subject.node.node_id.clone(), incarnation: subject.node.incarnation_id.clone().unwrap(), build_id: "b".into(), attempt: 1, operation: "start".into() } } else { StatusRequest::StopNode { node_id: subject.node.node_id.clone(), incarnation: subject.node.incarnation_id.clone().unwrap(), build_id: "b".into(), attempt: 1, operation: "stop".into() } };
        let before = rig.authority.declared.lock().unwrap().clone();
        let result = call(&rig, subject, &request).await;
        assert!(matches!(reply(&result), StatusReply::RejectedUnmatchedCompletion { expected, reported, .. } if expected.contains("reserved") && reported.contains(request.op())), "{result:?}");
        assert_eq!(*rig.authority.declared.lock().unwrap(), before);
        assert!(rig.storage.node_declared().await.unwrap().is_empty());
    }
}

/// Q47: the stream owner records only the node's frames, before recovery; the start's Ready
/// record precedes its return to the Build. An accepted failure keeps its step, reason and code.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lifecycle_stream_frames_alone_record_the_reported_state_and_typed_failure() {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply as N, NodeCommandRequest as Q};
    use rdm_admin::deployment::pipeline::{CommandAdmission as A, CommandContext};
    for (start, fail) in [(false, false), (true, false), (true, true)] {
        let (rig, births) = rig().await;
        let subject = births["rpc2"].node.clone();
        let (arrived, admit, progressed, finish) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
        let (a, b, c, d) = (arrived.clone(), admit.clone(), progressed.clone(), finish.clone());
        let server = ServerBuilder::new().serve_stream::<NodeCommand, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_, req: Q, sink| {
            let (a, b, c, d) = (a.clone(), b.clone(), c.clone(), d.clone());
            async move {
                a.notify_one();
                b.notified().await;
                let (node_id, incarnation) = req.subject();
                let mut sink = sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.unwrap();
                if fail { return Ok(N::Failed { step: "before_ready".into(), reason: "app refused".into(), code: rdm_core::outcome::Code::FailedPrecondition }); }
                sink.data(if start { N::NodeJoined } else { N::StopConnectionsDeleted { receipt: rdm_core::status::ConnectionsReceipt::Established { in_flight: 0, closed: vec![] } } }).await.unwrap();
                c.notify_one();
                d.notified().await;
                Ok(if start { N::NodeStarted } else { N::NodeStopped })
            }
        }).seal(ServedBirth { node_id: subject.node_id.to_string(), incarnation: subject.incarnation_id.as_ref().unwrap().to_string() }).unwrap();
        let ep = rdm_transport::endpoint::bind(births["rpc2"].key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { node_id: subject.node_id.clone(), name: subject.name.clone(), endpoint_id: births["rpc2"].key.public(), transport_addr: addr, incarnation: subject.incarnation_id.clone().unwrap() });
        let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm"));
        assert!(rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(client, Default::default(), Default::default()))).is_ok());
        let ctx = CommandContext { build_id: "b".into(), attempt: 1, operation: format!("node.{}:{}", if start { "start" } else { "stop" }, subject.name) };
        let (authority, target) = (rig.authority.clone(), subject.clone());
        let call = tokio::spawn(async move { authority.carry_lifecycle(&target, start, &ctx).await });
        tokio::time::timeout(Duration::from_secs(5), arrived.notified()).await.unwrap();
        assert!(rig.storage.node_declared().await.unwrap().is_empty(), "no state before CommandAccepted");
        admit.notify_one();
        if !fail {
            tokio::time::timeout(Duration::from_secs(5), progressed.notified()).await.unwrap();
            if start {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while rig.authority.declared.lock().unwrap().node(&subject.node_id, subject.incarnation_id.as_ref().unwrap()) != Some(NodeState::Pending) { tokio::task::yield_now().await; }
                }).await.expect("NodeJoined is recorded before NodeStarted exists");
            }
            finish.notify_one();
        }
        let result = tokio::time::timeout(Duration::from_secs(5), call).await.unwrap().unwrap();
        if fail {
            assert!(matches!(result, A::StartFailed { step, reason, code: rdm_core::outcome::Code::FailedPrecondition } if step == "before_ready" && reason == "app refused"));
            assert!(rig.storage.node_operations().await.unwrap().is_empty());
        } else {
            assert!(matches!((&result, start), (A::Started {}, true) | (A::Stopped { .. }, false)), "{result:?}");
            let state = if start { NodeState::ReadyForTraffic } else { NodeState::Stopped };
            assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node_id, subject.incarnation_id.as_ref().unwrap()), Some(state), "recorded before the outer Build can complete");
            assert_eq!(rig.storage.node_operations().await.unwrap().len(), 1);
        }
    }
}

/// Q47: the NodeJoined position is recovered once, even when its ack is lost.
#[test]
fn a_lifecycle_progress_frame_replayed_after_a_lost_ack_is_recorded_once() {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply as N, NodeCommandRequest};
    let cap = capture();
    cap.run(async {
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _gossip_router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let fabric = FabricId::mint();
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(5_000_000);
        let membership = rdm_transport::membership::Membership::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), "mesh1.admin.1", Arc::new(time), vec![], &[]).await.unwrap();
        membership.set_mesh_primary(true);
        let (rig, b) = rig_with_book(Some(membership.book.clone())).await;
        let rpc = &b["rpc2"];
        assert!(membership.book.record(rdm_core::MeshDigest { held_hash: Default::default(),
            fabric_id: fabric,
            node: rdm_core::MeshNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: rpc.node.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Pending, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: None, in_flight: None, config: Default::default(), load: None, gossip: None,
        }));
        let (server_delivery, caller_delivery) = (Arc::new(OnceLock::<rdm_transport::Delivery>::new()), Arc::new(OnceLock::<rdm_transport::Delivery>::new()));
        let (sd, cd) = (server_delivery.clone(), caller_delivery.clone());
        let server = ServerBuilder::new().serve_stream::<NodeCommand, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_, req: NodeCommandRequest, sink| {
            let (sd, cd) = (sd.clone(), cd.clone());
            async move {
                let NodeCommandRequest::StartNode { node_id, incarnation, .. } = req else { panic!("expected StartNode") };
                let mut sink = sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while rdm_transport::DeliveryStats::get(&sd.get().unwrap().stats().acks_received) == 0 { tokio::task::yield_now().await; }
                }).await.expect("Started recovered before the progress fault is armed");
                cd.get().unwrap().seams().drop_next_response_ack();
                sink.data(N::NodeJoined).await.unwrap();
                tokio::time::timeout(Duration::from_secs(5), async {
                    while rdm_transport::DeliveryStats::get(&sd.get().unwrap().stats().responses_resent) == 0 || sd.get().unwrap().owed_frames() != 0 { tokio::task::yield_now().await; }
                }).await.expect("the unrecovered progress is resent on a new stream and acknowledged");
                Ok(N::NodeStarted)
            }
        }).seal(ServedBirth { node_id: rpc.node.node_id.to_string(), incarnation: rpc.node.incarnation_id.as_ref().unwrap().to_string() }).unwrap();
        server_delivery.set(server.delivery().clone()).unwrap_or_else(|_| panic!("one responder"));
        let ep = rdm_transport::endpoint::bind(rpc.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: rpc.key.public(), transport_addr: addr, incarnation: rpc.node.incarnation_id.clone().unwrap() });
        let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm"));
        caller_delivery.set(client.delivery().clone()).unwrap_or_else(|_| panic!("one caller"));
        assert!(rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(client, rig.authority.configure.clone(), Default::default()))).is_ok());
        let context = rdm_admin::deployment::pipeline::CommandContext { build_id: "b".into(), attempt: 1, operation: format!("node.start:{}", rpc.node.name) };
        let answer = rig.authority.carry_lifecycle(&rpc.node, true, &context).await;
        assert!(matches!(answer, rdm_admin::deployment::pipeline::CommandAdmission::Started {}), "{answer:?}");
        assert_eq!(rig.storage.node_operations().await.unwrap().len(), 1);
        assert!(rdm_transport::DeliveryStats::get(&server_delivery.get().unwrap().stats().responses_resent) >= 1);
    });
    let records: Vec<_> = cap.spans("rdm.node_admin.node.update.via-command-frame").into_iter().filter(|(_, a)| a.get("frame").map(String::as_str) == Some("node-joined")).collect();
    assert_eq!(records.len(), 1, "the mesh-primary records the repeated progress once: {records:?}");
    let applied: Vec<_> = cap.spans("rdm.mesh.membership.update.from-status-report").into_iter().filter(|(_, a)| a.get("status").map(String::as_str) == Some("Pending")).collect();
    assert_eq!(applied.len(), 1, "one Pending status write, not just one carrier span: {applied:?}");
    assert!(!records[0].1.get("invocation_id").unwrap_or(&String::new()).is_empty(), "the record names the invocation: {records:?}");
    assert_eq!(records[0].1.get("seq").map(String::as_str), Some("1"), "the progress position: {records:?}");
}

/// Q47/Q49: the stop's physical stream is cut after its closure report. The live responder sends
/// the operation-named Q36 declaration on its endpoint, then its recovered terminal is AlreadyApplied.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_stream_lost_after_the_closure_is_recorded_once_from_the_owed_declaration() {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply as N, NodeCommandRequest as Q};
    use rdm_admin::deployment::pipeline::{CommandContext, CommandAdmission as A};
    let (rig, births) = rig().await;
    let subject = &births["rpc2"];
    let published = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let count = published.clone();
    assert!(rig.authority.stopped.set(Arc::new(move |_, _| {
        count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {})
    })).is_ok());
    let responder = Arc::new(OnceLock::<rdm_transport::NodeRpcServer>::new());
    let node_client = Arc::new(OnceLock::<NodeRpcClient>::new());
    let (server_slot, client_slot) = (responder.clone(), node_client.clone());
    let primary = rig.admin.node.node_id.clone();
    let server = ServerBuilder::new().serve_stream::<NodeCommand, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_, request: Q, sink| {
        let (server_slot, client_slot, primary) = (server_slot.clone(), client_slot.clone(), primary.clone());
        async move {
            let Q::StopNode { node_id, incarnation, build_id, attempt, operation } = request else { panic!("a stop") };
            let mut sink = sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.unwrap();
            sink.data(N::StopConnectionsDeleted { receipt: rdm_core::status::ConnectionsReceipt::Established { in_flight: 0, closed: vec![] } }).await.unwrap();
            let server = server_slot.get().unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while rdm_transport::DeliveryStats::get(&server.delivery().stats().acks_received) < 2 { tokio::task::yield_now().await; }
            }).await.expect("the stop's acceptance and closure are recovered before the connection is cut");
            assert!(server.close_connections_except(&[], "the cell cuts the stop stream after closure") > 0);
            let declaration = StatusRequest::DeclareNodeState { node_id, incarnation, state: NodeState::Stopped, operation: Some(rdm_core::status::DeclaredOperation { build_id, attempt, operation }) };
            let (out, _) = client_slot.get().unwrap().call::<Status>(&NodeTarget::ExactNode(primary), &declaration, &CallOptions::default()).await;
            assert!(matches!(out, RpcOutcome::Reply(r) if matches!(r.value(), StatusReply::Applied)), "the owed stop declaration is applied before the terminal exists");
            Ok(N::NodeStopped)
        }
    }).seal(ServedBirth { node_id: subject.node.node_id.to_string(), incarnation: subject.node.incarnation_id.as_ref().unwrap().to_string() }).unwrap();
    let server_delivery = server.delivery().clone();
    responder.set(server.clone()).unwrap_or_else(|_| panic!("one responder"));
    let ep = rdm_transport::endpoint::bind(subject.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(rig.resolved.clone());
    node_client.set(NodeRpcClient::new(ep.clone(), resolver).with_delivery(server_delivery).with_caller_system("rdm")).unwrap_or_else(|_| panic!("one node client"));
    let _router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: subject.node.node_id.clone(), name: subject.node.name.clone(), endpoint_id: subject.key.public(), transport_addr: addr, incarnation: subject.node.incarnation_id.clone().unwrap() });
    // The caller owns and serves one birth on one endpoint: recovered responses arrive
    // through this router and find the invocation in the same Delivery as the call.
    let client = Arc::new(NodeRpcClient::new(rig.endpoint.clone(), resolver).with_delivery(rig.server.delivery().clone()).with_caller_system("rdm"));
    assert!(rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(client, Default::default(), Default::default()))).is_ok());
    let context = CommandContext { build_id: "b".into(), attempt: 1, operation: format!("node.stop:{}", subject.node.name) };
    let end = tokio::time::timeout(Duration::from_secs(5), rig.authority.carry_lifecycle(&subject.node, false, &context)).await.expect("the lost physical stream recovers its terminal on the same invocation");
    assert!(matches!(end, A::Stopped { .. }), "{end:?}");
    assert_eq!(published.load(Ordering::SeqCst), 1, "the declaration publishes stopped; the recovered terminal publishes nothing twice");
    assert_eq!(rig.storage.node_operations().await.unwrap().len(), 1);
    assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node.node_id, subject.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Stopped));
}
