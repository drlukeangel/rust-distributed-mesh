//! i143.e6.s7 functional: a node-admin applies lifecycle declarations as an authority.
//!
//! The admin's `Status` service runs in-process over a view fixture (one fabric, two meshes, this
//! admin the primary of mesh1 and the fabric-primary); nodes declare over real Node RPC on
//! loopback. The cells prove the decision table of `status_rpc`: which seat decides which
//! subject's declaration, whoever calls (node-rpc.md §14), the natural-key idempotency, forward-only transitions, stale births, and that `Applied` is on the
//! subject's declared-state row in `status.storage` before it is answered. Certainty: a cut before the request
//! commits is `NotSent` with nothing applied; a reply lost after the apply is `Indeterminate`,
//! and the same declaration again is `AlreadyApplied` — one logical event.

use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::node_rows::RecordAt as _;
use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind};
use rdm_admin::model::{Fabric, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rdm_admin::status_rpc::{Declared, StatusAuthority};
use rdm_admin::node_config::SentBy;
use rdm_admin::storage::{MemoryNodesStorage, MeshStorage, NodesStorage};
use rdm_admin::topology::Topology;
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget, ReplyWithhold, ResolvedNode, ServedBirth, ServerBuilder, ServerStats, StaticResolver};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{FabricEvent, MeshState, NodeState, NotPrimary, Status, StatusReply, StatusRequest};
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
    node.status = NodeStatus::Ready;
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
    storage: Arc<MemoryNodesStorage>,
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
    async fn runtime(&self, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<rdm_admin::storage::RuntimeRow>, rdm_admin::storage_error::StorageError> {
        self.inner.runtime(node_id, incarnation).await
    }
    async fn node(&self, node_id: &rdm_admin::model::NodeId) -> Result<Option<rdm_admin::storage::NodeRow>, rdm_admin::storage_error::StorageError> {
        self.inner.node(node_id).await
    }
    async fn node_at(&self, name: &rdm_admin::model::PathName) -> Result<Option<rdm_admin::storage::NodeRow>, rdm_admin::storage_error::StorageError> {
        self.inner.node_at(name).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<rdm_admin::storage::NodeRow>, rdm_admin::storage_error::StorageError> {
        self.inner.nodes(mesh).await
    }
    async fn put_node(&self, row: &rdm_admin::storage::NodeRow) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.put_node(row).await
    }
    async fn index_path(&self, name: &rdm_admin::model::PathName, node_id: &rdm_admin::model::NodeId) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.index_path(name, node_id).await
    }
    async fn index_tags(&self, mesh: &str, node_id: &rdm_admin::model::NodeId, leaving: &std::collections::BTreeSet<String>, arriving: &std::collections::BTreeSet<String>) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.index_tags(mesh, node_id, leaving, arriving).await
    }
    async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<rdm_admin::model::NodeId>, rdm_admin::storage_error::StorageError> {
        self.inner.tagged(label).await
    }
    async fn name_claim(&self, name: &rdm_admin::model::PathName) -> Result<Option<rdm_admin::model::NodeId>, rdm_admin::storage_error::StorageError> {
        self.inner.name_claim(name).await
    }
    async fn claim_node_name(&self, name: &rdm_admin::model::PathName, node_id: &rdm_admin::model::NodeId) -> Result<Result<(), rdm_admin::storage::NameHeld>, rdm_admin::storage_error::StorageError> {
        self.inner.claim_node_name(name, node_id).await
    }
    async fn release_node_name(&self, name: &rdm_admin::model::PathName, node_id: &rdm_admin::model::NodeId) -> Result<(), rdm_admin::storage_error::StorageError> {
        if self.refuse_release.swap(false, Ordering::SeqCst) {
            return Err(rdm_admin::storage_error::StorageError::Io { file: format!("nodes/names/{name}"), reason: "disk refused the release".into() });
        }
        self.inner.release_node_name(name, node_id).await
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
    rig_with_digest_view(None).await
}

async fn rig_with_digest_view(digest_view: Option<rdm_transport::topology::DigestView>) -> (Rig, BTreeMap<&'static str, Birth>) {
    let admin = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
    let mut others = BTreeMap::new();
    others.insert("admin2", birth("mesh1.admin.2", NodeKind::NodeAdmin, "mesh1", false, false));
    others.insert("rpc1", birth("mesh1.rpc.1", NodeKind::RpcNode, "mesh1", true, false));
    others.insert("rpc2", birth("mesh1.rpc.2", NodeKind::RpcNode, "mesh1", false, false));
    others.insert("m2admin", birth("mesh2.admin.1", NodeKind::NodeAdmin, "mesh2", true, false));
    let mut nodes = vec![admin.node.clone()];
    nodes.extend(others.values().map(|b| b.node.clone()));
    let topology = Arc::new(tokio::sync::RwLock::new(Topology {
        fabric: Fabric { id: rdm_core::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes,
    }));
    let fabric_id = topology.read().await.fabric.id.clone();
    let nodes = Arc::new(MemoryNodesStorage::default());
    let storage = nodes.clone();
    let refuse_release = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let (m1id, m2id) = (MeshId::mint(), MeshId::mint());
    let mesh_ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), m1id.clone()), ("mesh2".to_string(), m2id.clone())].into_iter().collect();
    let rig_mesh_ids = mesh_ids.clone();
    let authority = Arc::new(StatusAuthority {
        certs: None,
        app_channels: None,
        declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
        stopped: Default::default(),
        me: admin.node.name.clone(),
        node_id: admin.node.node_id.clone(),
        fabric_id,
        topology: topology.clone(),
        declared: Arc::new(Mutex::new(Declared::default())),
        meshes_storage: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
        fabric_storage: Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()),
        nodes_storage: Arc::new(Rows { inner: nodes.clone(), refuse_release: refuse_release.clone() }),
        caches: Default::default(),
        mesh_ids: Arc::new(move || mesh_ids.clone()),
        durable: Default::default(),
        commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
        own: Arc::new(std::sync::OnceLock::new()),
        mesh_deleter: Arc::new(std::sync::OnceLock::new()),
        hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
        configs: Default::default(),
        republish: Default::default(), configure: Default::default(),
        pre_node_ready: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(),
        // node.get answers beside the view: no node's load is held here.
        loads: Some(Arc::new(BTreeMap::new)),
        digest_view,
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
        .build(ServedBirth { node_id: admin.node.node_id.to_string(), incarnation: admin.node.incarnation_id.clone().unwrap().to_string() })
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
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Ready)).await), StatusReply::Applied);
    // Applied is on the subject's row before it was answered.
    let row = rdm_admin::status_rpc::Declaration::of_records(&rig.storage.nodes("mesh1").await.unwrap()).into_iter().find(|c| c.node_id == rpc1.node.node_id).expect("the subject's row");
    assert_eq!(row.state, NodeState::Ready);
    assert_eq!(row.incarnation_id, rpc1.node.incarnation_id.clone().unwrap());
    // The same natural key again: one logical event.
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Ready)).await), StatusReply::AlreadyApplied);
    // Backward is refused naming the current state; a skip forward applies.
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Pending)).await), StatusReply::RejectedInvalidNodeTransition { current: NodeState::Ready });
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
async fn the_deciding_primary_is_the_one_the_subject_needs_never_the_callers() {
    let (rig, b) = rig().await;
    let (rpc1, rpc2, admin2, m2admin) = (&b["rpc1"], &b["rpc2"], &b["admin2"], &b["m2admin"]);
    // rpc.2 sending rpc.1's state: decided for rpc.1.
    assert_eq!(reply(&call(&rig, rpc2, &declare(rpc1, NodeState::Ready)).await), StatusReply::Applied);
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready), "applied for the subject");
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc2.node.node_id, rpc2.node.incarnation_id.as_ref().unwrap()), None, "nothing applied for the caller");
    // A subject the view does not hold at all.
    let stranger = birth("mesh1.rpc.9", NodeKind::RpcNode, "mesh1", false, false);
    let r = reply(&call(&rig, &stranger, &declare(&stranger, NodeState::Ready)).await);
    assert_eq!(r, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown });
    // A stale birth: the view holds a newer incarnation of the subject.
    let stale = StatusRequest::DeclareNodeState { node_id: rpc1.node.node_id.clone(), incarnation: IncarnationId::mint(), state: NodeState::Ready, operation: None };
    assert_eq!(reply(&call(&rig, rpc1, &stale).await), StatusReply::RejectedStaleIncarnation { held: rpc1.node.incarnation_id.clone().unwrap() });
    // A node of another mesh: this admin is not its mesh-primary.
    let foreign = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
    rig.topology.write().await.nodes.push(foreign.node.clone());
    let r = reply(&call(&rig, &foreign, &declare(&foreign, NodeState::Ready)).await);
    assert_eq!(r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } });
    // A node-admin declares to its own mesh's primary (this admin is mesh1's): Applied.
    assert_eq!(reply(&call(&rig, admin2, &declare(admin2, NodeState::Ready)).await), StatusReply::Applied);
    // A node-admin of another mesh is refused by the seat it needs, its own mesh's primary, though
    // this admin holds the fabric seat (node-status.md: "A node reports to its mesh's primary").
    let r = reply(&call(&rig, m2admin, &declare(m2admin, NodeState::Ready)).await);
    assert_eq!(r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } });
}

/// CONTRACT: nothing inside the mesh checks who is calling (node-rpc.md §14; node-rpc-envelope.md,
/// Status: "Nothing inside the mesh checks who is calling"): a declaration from an endpoint this
/// authority's view holds no birth for is decided for the subject the request names, on the
/// receiver's own seat. The direction refusals stay, decided by the subject and the seat: a subject
/// the view does not hold is `SubjectUnknown`, a subject of a mesh this admin is not the primary of
/// is `ReceiverNotPrimary`, and a birth the view no longer holds is `RejectedStaleIncarnation`.
/// What must NOT happen: a refusal naming the caller (`SenderNotSubject`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_from_an_endpoint_the_mesh_primary_never_heard_of_is_decided_for_its_subject() {
    let (rig, b) = rig().await;
    let (rpc1, admin2) = (&b["rpc1"], &b["admin2"]);
    // A fresh endpoint key, in no birth of the view.
    let stranger = birth("mesh1.rpc.9", NodeKind::RpcNode, "mesh1", false, false);
    assert!(rig.topology.read().await.births().all(|n| n.endpoint_id != stranger.node.endpoint_id), "the stranger is in no birth of the view");
    // A member's declaration is decided for that member: Applied, on its row first.
    assert_eq!(reply(&call(&rig, &stranger, &declare(rpc1, NodeState::Ready)).await), StatusReply::Applied);
    let row = rdm_admin::status_rpc::Declaration::of_records(&rig.storage.nodes("mesh1").await.unwrap()).into_iter().find(|c| c.node_id == rpc1.node.node_id).expect("the subject's row");
    assert_eq!((row.state, row.name.clone()), (NodeState::Ready, rpc1.node.name.clone()), "the row is the subject's, at its own path.name");
    // A node-admin's declaration reaches the fabric seat this admin holds.
    assert_eq!(reply(&call(&rig, &stranger, &declare(admin2, NodeState::Ready)).await), StatusReply::Applied);
    // A mesh's declaration reaches the fabric-primary for a mesh it holds.
    let m2 = StatusRequest::DeclareMeshState { mesh_id: rig.mesh_id("mesh2"), state: MeshState::Ready, changed_at_rafka_ms: 0 };
    assert_eq!(reply(&call(&rig, &stranger, &m2).await), StatusReply::Applied);
    // The direction refusals stay.
    assert_eq!(reply(&call(&rig, &stranger, &declare(&stranger, NodeState::Ready)).await), StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "a subject the view does not hold");
    let unknown_mesh = StatusRequest::DeclareMeshState { mesh_id: MeshId::mint(), state: MeshState::Ready, changed_at_rafka_ms: 0 };
    assert_eq!(reply(&call(&rig, &stranger, &unknown_mesh).await), StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "a mesh the fabric-primary does not hold");
    let stale = StatusRequest::DeclareNodeState { node_id: rpc1.node.node_id.clone(), incarnation: IncarnationId::mint(), state: NodeState::Stopped, operation: None };
    assert_eq!(reply(&call(&rig, &stranger, &stale).await), StatusReply::RejectedStaleIncarnation { held: rpc1.node.incarnation_id.clone().unwrap() });
    let foreign = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
    rig.topology.write().await.nodes.push(foreign.node.clone());
    assert_eq!(reply(&call(&rig, &stranger, &declare(&foreign, NodeState::Ready)).await), StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_and_fabric_declarations_follow_the_same_rules() {
    let (rig, b) = rig().await;
    let (m2admin, rpc1, admin2) = (&b["m2admin"], &b["rpc1"], &b["admin2"]);
    // mesh2's primary declares mesh2's state to the fabric-primary.
    let m2 = |s| StatusRequest::DeclareMeshState { mesh_id: rig.mesh_id("mesh2"), state: s, changed_at_rafka_ms: 0 };
    assert_eq!(reply(&call(&rig, m2admin, &m2(MeshState::Ready)).await), StatusReply::Applied);
    // Q144: a repeated declaration is answered `Applied` again, so a mesh-primary that lost its record
    // write before it landed records it on the answer to its next declaration.
    assert_eq!(reply(&call(&rig, m2admin, &m2(MeshState::Ready)).await), StatusReply::Applied);
    assert!(rig.authority.meshes_storage.mesh(&rig.mesh_id("mesh2")).await.unwrap().is_none_or(|r| r.states.is_empty()), "the fabric-primary writes nothing of mesh2's record: its primary does");
    assert_eq!(reply(&call(&rig, m2admin, &m2(MeshState::Pending)).await), StatusReply::RejectedInvalidMeshTransition { current: MeshState::Ready });
    // The declaration is decided for the mesh it names, whoever calls (an ordinary node; a
    // non-primary admin of another mesh): forward applies once, a mesh the holder does not hold is
    // `SubjectUnknown`.
    assert_eq!(reply(&call(&rig, rpc1, &m2(MeshState::Leaving)).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, admin2, &m2(MeshState::Leaving)).await), StatusReply::Applied, "a repeated declaration is answered Applied again (Q144)");
    let unknown = StatusRequest::DeclareMeshState { mesh_id: MeshId::mint(), state: MeshState::Ready, changed_at_rafka_ms: 0 };
    assert_eq!(reply(&call(&rig, m2admin, &unknown).await), StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown });
    // A fabric event is applied at a mesh primary (this admin is mesh1's), whoever sends it, once.
    let ev = StatusRequest::ApplyFabricEvent { fabric_id: rig.authority.fabric_id.clone(), event: FabricEvent::Ready };
    assert_eq!(reply(&call(&rig, m2admin, &ev).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, m2admin, &ev).await), StatusReply::AlreadyApplied);
    let wrong = StatusRequest::ApplyFabricEvent { fabric_id: FabricId::mint(), event: FabricEvent::Ready };
    assert_eq!(reply(&call(&rig, m2admin, &wrong).await), StatusReply::RejectedStaleFabric { held: rig.authority.fabric_id.clone() });
    // ApplyMeshState(Pending) from the fabric-primary at an admin of that mesh: this admin is
    // mesh1's; mesh2's Pending is not its to apply, mesh1's is, and a foreign mesh id is refused.
    let pend = |id: &MeshId, name: &str| StatusRequest::ApplyMeshState { mesh_id: id.clone(), mesh_name: name.into(), state: MeshState::Pending, changed_at_rafka_ms: 0 };
    let (m1id, m2id, other) = (rig.mesh_id("mesh1"), rig.mesh_id("mesh2"), MeshId::mint());
    assert!(matches!(reply(&call(&rig, m2admin, &pend(&m2id, "mesh2")).await), StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { .. } }));
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
    let (out, _) = c.call::<Status>(&target, &declare(rpc1, NodeState::Ready), &CallOptions { cut_before_finish: true, ..Default::default() }).await;
    assert!(matches!(out, RpcOutcome::NotSent(_)), "{out:?}");
    assert!(rig.authority.declared.lock().unwrap().nodes.is_empty());
    assert!(rdm_admin::status_rpc::Declaration::of_records(&rig.storage.nodes("mesh1").await.unwrap()).iter().all(|c| c.node_id != rpc1.node.node_id));
    // Applied, reply lost: Indeterminate at the caller; the same declaration again is AlreadyApplied.
    rig.authority.hold_next_reply.store(true, Ordering::SeqCst);
    let (out, ev) = c.call::<Status>(&target, &declare(rpc1, NodeState::Ready), &CallOptions { budget: Budget::Overall(Duration::from_millis(800)), ..Default::default() }).await;
    assert!(matches!(out, RpcOutcome::Indeterminate(_)), "{out:?}");
    assert!(ev.unwrap().committed);
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready), "applied before the reply was lost");
    let (out, _) = c.call::<Status>(&target, &declare(rpc1, NodeState::Ready), &CallOptions::default()).await;
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
    let (out, ev) = c.call::<Status>(&target, &declare(rpc1, NodeState::Ready), &opts).await;
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == rdm_core::outcome::IndeterminateReason::ReplyDeadline), "{out:?}");
    assert!(ev.unwrap().committed, "the request crossed the commit cut before the reply was withheld");
    rig.withhold.reached().await;
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready), "applied before the reply was withheld");
    assert_eq!(ServerStats::get(&rig.server.stats().dispatched), 1, "the handler ran once for the lost call");

    // The reply goes out only now, after the caller classified the call.
    rig.withhold.release();
    let late = tokio::time::timeout(Duration::from_secs(10), rig.withhold.late_reply())
        .await
        .expect("the caller never abandoned the reply stream of a call it had already classified");
    assert!(matches!(late.stopped, Ok(Some(_))), "the caller stopped the reply stream when it classified the call: {late:?}");
    assert!(late.written.is_err(), "the late reply was refused at the abandoned stream: {late:?}");

    // The completed call is unchanged and the retry is one logical event.
    let (out, _) = c.call::<Status>(&target, &declare(rpc1, NodeState::Ready), &CallOptions::default()).await;
    assert_eq!(reply(&out), StatusReply::AlreadyApplied, "one logical event");
    assert_eq!(ServerStats::get(&rig.server.stats().dispatched), 2, "the lost call and the retry, nothing replayed");
    let row = rdm_admin::status_rpc::Declaration::of_records(&rig.storage.nodes("mesh1").await.unwrap()).into_iter().find(|c| c.node_id == rpc1.node.node_id).expect("the subject's row");
    assert_eq!(row.state, NodeState::Ready);
}

/// Connections delete admission never prevents an authority from receiving the upward certainty calls
/// that finish lifecycle work. The status handler still decides each for its subject: a
/// declaration sent by another node is decided for the subject it names (here already applied),
/// and a subject the view does not hold is refused by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connections_deleting_mesh_primary_accepts_upward_status_and_decides_it_for_the_subject() {
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
    assert_eq!(reply(&call(&rig, &b["rpc2"], &declare(&stranger, NodeState::Stopped)).await), StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown });
}

/// A birth of the fixture serving Status on its own key, recording every request it decodes and
/// answering `Applied`: the node end of a config apply.
struct ServedNode {
    resolved: ResolvedNode,
    seen: Arc<Mutex<Vec<StatusRequest>>>,
    /// Notified once per request served (a permit is kept when nobody waits yet).
    heard: Arc<tokio::sync::Notify>,
    /// The endpoint it serves on: a cell's call made as this birth goes out on it.
    endpoint: iroh::Endpoint,
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
                // node.mesh.leave reaches the birth here too (Q205): its acknowledgement, then its
                // completion, or its refusal before acceptance.
                if let NodeCommandRequest::LeaveMesh { node_id, incarnation, build_id, attempt, operation } = req {
                    let carried = StatusRequest::LeaveMesh { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id, attempt, operation };
                    let reply = answer(&carried);
                    s.lock().unwrap().push(carried);
                    h.notify_one();
                    let StatusReply::MeshLeft { left, .. } = reply else {
                        return Ok(match reply {
                            StatusReply::BusyHoldingConnections { connections } => N::BusyHoldingConnections { connections },
                            StatusReply::RejectedStaleIncarnation { held } => N::RejectedStaleIncarnation { held },
                            other => N::NotReady { reason: format!("{}: {other:?}", other.name()) },
                        });
                    };
                    sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                    return Ok(N::MeshLeft { node_id, incarnation, left });
                }
                let NodeCommandRequest::DeleteConnections { node_id, incarnation, scope } = req else { panic!("expected DeleteConnections or LeaveMesh") };
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
        .build(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().to_string() })
        .unwrap();
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep.clone()).accept(rdm_transport::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id: b.node.node_id.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    ServedNode { resolved, seen, heard, endpoint: ep, _router: router }
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
    // The door writes from the one copy of the records this admin holds, its authority's.
    let _ = rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), rig.authority.configure.clone(), rig.authority.caches.nodes.clone())));
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

/// A `ConfigUpdated` as the birth and the map it carries, its row's revision checked stepped (Q235 P1:
/// every write of the row steps it); any other reply panics.
fn updated(reply: &StatusReply) -> (NodeId, IncarnationId, BTreeMap<String, String>) {
    match reply {
        StatusReply::ConfigUpdated { node_id, incarnation, config, revision } => {
            assert!(*revision > 0, "the row as written carries its stepped revision: {reply:?}");
            (node_id.clone(), incarnation.clone(), config.clone())
        }
        other => panic!("expected node.config.updated: {other:?}"),
    }
}

/// [`updated`] of `b` carrying `config`.
fn updated_of(b: &Birth, config: &BTreeMap<String, String>) -> (NodeId, IncarnationId, BTreeMap<String, String>) {
    (b.node.node_id.clone(), b.node.incarnation_id.clone().unwrap(), config.clone())
}

/// What a held-config closure records: each map installed or held, in order (`ConfigHeld`).
fn holding() -> (Arc<Mutex<Vec<rdm_admin::status_rpc::ConfigHeld>>>, Arc<dyn Fn(rdm_admin::status_rpc::ConfigHeld) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>> + Send + Sync>) {
    let held: Arc<Mutex<Vec<rdm_admin::status_rpc::ConfigHeld>>> = Arc::default();
    let h = held.clone();
    (held, Arc::new(move |c: rdm_admin::status_rpc::ConfigHeld| {
        let h = h.clone();
        Box::pin(async move { h.lock().unwrap().push(c) })
    }))
}

/// CONTRACT (`docs/architecture/lifecycles/node-config-update.md:5` — "`node.config.update` replaces a
/// node's application config, the whole map: a PUT, never a patch. It is a single call, not a Build."
/// and `docs/architecture/lifecycles/node-config-update.md:23` — "3. A live node is sent
/// `ApplyNodeConfig` with the map and its revision; it installs the map over an older revision. The
/// command completes after `Applied` or `AlreadyApplied`."; ops-naming "Node data": a config update is
/// a single call, not a Build; the node's
/// mesh-primary is the only writer and writes the whole row): a caller's `node.config.update` to
/// mesh1's primary ends `node.config.updated` (`ConfigUpdated` naming the exact birth); the node's
/// record at its path.name holds the config; the birth was told the whole map exactly once
/// (`ApplyNodeConfig`) before the mesh-primary answered; and one `rdm.node_admin.node.update.via-config`
/// span says `config-updated`.
// @feature: node-config
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
        assert_eq!(updated(&reply), updated_of(rpc1, &config));
        let row = rig.nodes.record_at(&rpc1.node.name).await.unwrap().expect("the mesh-primary wrote the node's row");
        assert_eq!((row.node_id.clone(), row.config.clone()), (rpc1.node.node_id.clone(), config.clone()), "the row at mesh1.rpc.1 is this node with the whole config");
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config, revision: row.revision }],
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
        assert_eq!(updated(&reply), updated_of(rpc1, &config));
        let row = rig.nodes.record_at(&old).await.unwrap().expect("the mesh-primary wrote the node's row at its .old name");
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
        assert_eq!(updated(&reply), updated_of(stopped, &config), "a stopped node's config update completes on its row");
        let row = rig.nodes.record_at(&stopped.node.name).await.unwrap().expect("the mesh-primary wrote the node's row");
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
    async fn runtime(&self, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<rdm_admin::storage::RuntimeRow>, rdm_admin::storage_error::StorageError> {
        self.0.runtime(node_id, incarnation).await
    }
    async fn node(&self, node_id: &rdm_admin::model::NodeId) -> Result<Option<rdm_admin::storage::NodeRow>, rdm_admin::storage_error::StorageError> {
        self.0.node(node_id).await
    }
    async fn node_at(&self, name: &rdm_admin::model::PathName) -> Result<Option<rdm_admin::storage::NodeRow>, rdm_admin::storage_error::StorageError> {
        self.0.node_at(name).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<rdm_admin::storage::NodeRow>, rdm_admin::storage_error::StorageError> {
        self.0.nodes(mesh).await
    }
    async fn put_node(&self, row: &rdm_admin::storage::NodeRow) -> Result<(), rdm_admin::storage_error::StorageError> {
        Err(rdm_admin::storage_error::StorageError::Io { file: format!("nodes/records/{}", row.node_id), reason: "disk refused the write".into() })
    }
    async fn index_path(&self, name: &rdm_admin::model::PathName, node_id: &rdm_admin::model::NodeId) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.index_path(name, node_id).await
    }
    async fn index_tags(&self, mesh: &str, node_id: &rdm_admin::model::NodeId, leaving: &std::collections::BTreeSet<String>, arriving: &std::collections::BTreeSet<String>) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.index_tags(mesh, node_id, leaving, arriving).await
    }
    async fn tagged(&self, label: &str) -> Result<std::collections::BTreeSet<rdm_admin::model::NodeId>, rdm_admin::storage_error::StorageError> {
        self.0.tagged(label).await
    }
    async fn name_claim(&self, name: &rdm_admin::model::PathName) -> Result<Option<rdm_admin::model::NodeId>, rdm_admin::storage_error::StorageError> {
        self.0.name_claim(name).await
    }
    async fn claim_node_name(&self, name: &rdm_admin::model::PathName, node_id: &rdm_admin::model::NodeId) -> Result<Result<(), rdm_admin::storage::NameHeld>, rdm_admin::storage_error::StorageError> {
        self.0.claim_node_name(name, node_id).await
    }
    async fn release_node_name(&self, name: &rdm_admin::model::PathName, _node_id: &rdm_admin::model::NodeId) -> Result<(), rdm_admin::storage_error::StorageError> {
        Err(rdm_admin::storage_error::StorageError::Io { file: format!("nodes/names/{name}"), reason: "disk refused the release".into() })
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
        let refusal = format!("pre_node_ready has not passed: {}", "the store of mesh1.rpc.1 is still behind; ".repeat(20));
        let node = serve_node_answering(rpc1, StatusReply::NotReady { reason: refusal.clone() }).await;
        configs_door(&rig, &node).await;
        let config: BTreeMap<String, String> = [("zone".to_string(), "b".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        let StatusReply::ConfigNotApplied { node: named, answered, reason } = &reply else { panic!("the row is written and the birth refused: {reply:?}") };
        assert_eq!((named.as_str(), answered.as_str()), ("mesh1.rpc.1", "not-ready"), "{reply:?}");
        assert!(reason.ends_with('…'), "the birth's long answer is cut to the reply's budget: {reason}");
        assert_eq!(node.seen.lock().unwrap().len(), 1, "the birth was told once");
        let row = rig.nodes.record_at(&rpc1.node.name).await.unwrap().expect("the mesh-primary wrote the row before telling the birth");
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
            matches!(&reply, StatusReply::ConfigRowRefused { node, reason, .. } if node == "mesh1.rpc.1" && reason.contains("disk refused the write")),
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

/// CONTRACT (node-config-update.md steps 1-2: "The caller sends `node.config.update` to the
/// fabric-primary ... and sends the command to the node's mesh-primary"; "The mesh-primary writes the
/// record"; fabric-node-lifecycle §4.3: the mesh-primary of a node's mesh is the only writer of that
/// mesh's rows): a node-admin's config update for mesh2.rpc.1 (`SentBy::Other`: the fabric-primary's
/// command sent down to an admin that does not hold the mesh-primary) at mesh1.admin.2, which is neither
/// the fabric-primary nor mesh2's primary, is refused by name, `ReceiverNotPrimary` naming the
/// fabric-primary; no row is written and nothing is told. The same command at mesh2's primary, the
/// fabric-primary's down hop, writes the row and tells the birth directly, once, and ends
/// `ConfigUpdated`. Spans: `via-not-primary` at mesh1.admin.2, `via-config` `config-updated` at
/// mesh2.admin.1.
#[test]
fn a_config_update_is_refused_by_name_at_an_admin_that_is_neither_primary_and_written_by_the_nodes_mesh_primary() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, m2admin) = (&b["admin2"], &b["m2admin"]);
        let m2rpc = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
        rig.topology.write().await.nodes.push(m2rpc.node.clone());
        let node = serve_node(&m2rpc).await;
        let config: BTreeMap<String, String> = [("tags".to_string(), "far".to_string())].into();
        let req = StatusRequest::UpdateNodeConfig { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap(), config: config.clone() };
        let view = rig.topology.read().await.clone();
        // At an admin that is neither the fabric-primary nor mesh2's primary: refused by name.
        let neither_rows = MemoryNodesStorage::default();
        let neither = door_at(&admin2.key, &node).await;
        let refused = neither.serve(&admin2.node.name, &admin2.node.node_id, &view, &rig.topology, &neither_rows, None, SentBy::Other, &req).await;
        let StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } = &refused else { panic!("mesh1.admin.2 is neither primary: {refused:?}") };
        assert!(needed.contains("fabric-primary") && needed.contains("mesh1.admin.1"), "the refusal names the fabric-primary: {needed}");
        assert!(neither_rows.record_at(&m2rpc.node.name).await.unwrap().is_none(), "no row is written");
        assert!(node.seen.lock().unwrap().is_empty(), "nothing is told by an admin that is neither primary");
        // At mesh2's primary: the row is written, then the birth is told directly.
        let mp_rows = MemoryNodesStorage::default();
        let door = door_at(&m2admin.key, &node).await;
        let served = door.serve(&m2admin.node.name, &m2admin.node.node_id, &view, &rig.topology, &mp_rows, None, SentBy::Other, &req).await;
        assert_eq!(updated(&served), updated_of(&m2rpc, &config));
        let row = mp_rows.record_at(&m2rpc.node.name).await.unwrap().expect("mesh2's primary wrote the row");
        assert_eq!((row.node_id.clone(), row.config.clone()), (m2rpc.node.node_id.clone(), config.clone()));
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap(), config, revision: row.revision }],
            "the birth was told once, by its mesh's primary"
        );
    });
    let refused = cap.spans("rdm.node_admin.node.reject.via-not-primary");
    let (np_trace, np) = refused.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.admin.2")).unwrap_or_else(|| panic!("a via-not-primary span at mesh1.admin.2: {refused:?}"));
    assert_eq!(np.get("subject").map(String::as_str), Some("mesh2.rpc.1"), "{np:?}");
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (mp_trace, mp) = &spans[0];
    assert_eq!((mp.get("node").map(String::as_str), mp.get("outcome").map(String::as_str), mp.get("subject").map(String::as_str)), (Some("mesh2.admin.1"), Some("config-updated"), Some("mesh2.rpc.1")), "{mp:?}");
    eprintln!("trace {np_trace}: rdm.node_admin.node.reject.via-not-primary node=mesh1.admin.2 subject=mesh2.rpc.1");
    eprintln!("trace {mp_trace}: rdm.node_admin.node.update.via-config node=mesh2.admin.1 subject=mesh2.rpc.1 outcome=config-updated");
}

/// CONTRACT (node-config-update.md step 1: the fabric-primary "sends the command to the node's
/// mesh-primary"; node-rpc.md §43: delivery is owed until acked, a named refusal ends one request's
/// retry and the owning workflow retargets it; D12): the fabric-primary sends mesh2.rpc.1's
/// `node.config.update` down to mesh2.admin.1, which its view names mesh2's primary but which does
/// not hold it yet: it answers `RejectedNotPrimary`. The caller is not failed: the command stays
/// owed, and once mesh2.admin.1 holds the primary (and the fabric-primary's view changes) it
/// is sent again to it and ends `ConfigUpdated`; the fabric-primary's atlas holds the map.
/// Span: `rdm.node_admin.node.update.via-fabric-primary` with `sends` = the sends made, the last
/// answered, outcome `config-updated`. Must NOT happen: the caller answered `RejectedNotPrimary` or
/// `NotReady`, or the atlas left without the map.
#[test]
fn a_config_update_sent_down_while_the_meshs_primary_changes_completes_once_the_new_primary_holds_it() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let m2admin = &b["m2admin"];
        let m2rpc = birth("mesh2.rpc.1", NodeKind::RpcNode, "mesh2", false, false);
        rig.topology.write().await.nodes.push(m2rpc.node.clone());
        let landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let holds = landed.clone();
        let (node_id, incarnation) = (m2rpc.node.node_id.clone(), m2rpc.node.incarnation_id.clone().unwrap());
        let (n, i) = (node_id.clone(), incarnation.clone());
        // mesh2.admin.1: elected in the fabric-primary's view, not holding it until `landed`.
        let mp = serve_node_with(m2admin, move |req| match req {
            StatusRequest::UpdateNodeConfig { .. } if !holds.load(Ordering::SeqCst) => rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() }),
            StatusRequest::UpdateNodeConfig { config, .. } => StatusReply::ConfigUpdated { node_id: n.clone(), incarnation: i.clone(), config: config.clone(), revision: 7 },
            other => panic!("only the config command reaches mesh2's primary: {other:?}"),
        })
        .await;
        let door = door_at(&rig.admin.key, &mp).await;
        let (tick, views) = tokio::sync::watch::channel(0u64);
        door.watch_views(views);
        let atlas = rdm_admin::atlas::Atlas::default();
        atlas.write_record(&rdm_admin::storage::NodeRow::new(m2rpc.node.name.clone(), node_id.clone(), NodeStatus::Ready));
        let told: BTreeMap<String, String> = [("admin_api_base".to_string(), "rdm://mesh2".to_string())].into();
        let req = StatusRequest::UpdateNodeConfig { node_id: node_id.clone(), incarnation: incarnation.clone(), config: told.clone() };
        let view = rig.topology.read().await.clone();
        let rows = MemoryNodesStorage::default();
        let serve = door.serve(&rig.admin.node.name, &rig.admin.node.node_id, &view, &rig.topology, &rows, Some(&atlas), SentBy::Caller, &req);
        let land = async {
            tokio::time::timeout(Duration::from_secs(10), mp.heard.notified()).await.expect("the fabric-primary sent the command down");
            // mesh2.admin.1 holds the primary now, and the fabric-primary's view changes.
            landed.store(true, Ordering::SeqCst);
            tick.send_modify(|t| *t += 1);
        };
        let (reply, ()) = tokio::time::timeout(Duration::from_secs(20), async { tokio::join!(serve, land) }).await.expect("the owed command completes once mesh2.admin.1 holds the primary");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), config: told.clone(), revision: 7 }, "the caller is answered once the primary is held, with the row as written");
        let sent: Vec<StatusRequest> = mp.seen.lock().unwrap().clone();
        assert!(sent.len() >= 2 && sent.iter().all(|r| *r == req), "the same command, sent down again to mesh2.admin.1: {sent:?}");
        let held = atlas.record(&node_id).expect("the atlas holds mesh2.rpc.1");
        assert_eq!((held.config, held.revision), (told, 7), "the fabric-primary's atlas holds the row as written, at its revision");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-fabric-primary");
    let (trace, a) = spans.iter().find(|(_, a)| a.get("subject").map(String::as_str) == Some("mesh2.rpc.1")).unwrap_or_else(|| panic!("a via-fabric-primary span: {spans:?}"));
    assert_eq!((a.get("outcome").map(String::as_str), a.get("mesh_primary").map(String::as_str), a.get("atlas").map(String::as_str), a.get("revision").map(String::as_str)), (Some("config-updated"), Some("mesh2.admin.1"), Some("installed"), Some("7")), "{a:?}");
    let sends: u32 = a.get("sends").and_then(|v| v.parse().ok()).unwrap_or(0);
    assert!(sends >= 2, "sent down again after the refusal: {a:?}");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-fabric-primary subject=mesh2.rpc.1 sends={sends} outcome=config-updated");
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
        assert_eq!(updated(&reply), updated_of(rpc1, &config));
        let row = rig.nodes.record_at(&rpc1.node.name).await.unwrap().expect("mesh1's primary wrote the row");
        assert_eq!(row.config, config);
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config, revision: row.revision }],
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
        let (held, hold) = holding();
        let _ = rig.authority.configure.set(hold);
        let node = serve_node(rpc1).await;
        configs_door(&rig, &node).await;
        let me = &rig.admin.node;
        let config: BTreeMap<String, String> = [("admin_api_base".to_string(), "http://127.0.0.1:9".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&me.node_id, &rdm_transport::RouteChoice::Direct, &exact(&rig.admin), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(updated(&reply), (me.node_id.clone(), me.incarnation_id.clone().unwrap(), config.clone()));
        let row = rig.nodes.record_at(&me.name).await.unwrap().expect("mesh1's primary wrote its own row");
        assert_eq!((row.node_id.clone(), row.config.clone()), (me.node_id.clone(), config.clone()));
        assert_eq!(held.lock().unwrap().clone(), vec![rdm_admin::status_rpc::ConfigHeld::Told { config, revision: row.revision }], "the admin holds the whole config at the row's revision, once");
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

/// A second node-admin of the rig, `b`, holding no primary of its own unless the view says so: its own
/// `StatusAuthority` over `rig`'s view (the views agree), served on `b`'s key, its config door telling
/// through a client that resolves each of `resolves`. Its nodes store is its own.
struct SecondAdmin {
    status: Arc<StatusAuthority>,
    resolved: ResolvedNode,
    /// What its config door's client resolves: a cell adds a birth it reaches later.
    resolver: Arc<StaticResolver>,
    /// Its atlas, fed every record its writer holds (Q224, as the boot wires it).
    atlas: Arc<rdm_admin::atlas::Atlas>,
    _router: Router,
}

async fn second_admin(rig: &Rig, b: &Birth, resolves: &[ResolvedNode]) -> SecondAdmin {
    let mesh_ids = rig.mesh_ids.clone();
    let status = Arc::new(StatusAuthority {
        certs: None,
        app_channels: None,
        declaration_seam: None,
        command_frame_seam: None,
        operation_effects: Default::default(),
        stopped: Default::default(),
        me: b.node.name.clone(),
        node_id: b.node.node_id.clone(),
        fabric_id: rig.topology.read().await.fabric.id.clone(),
        topology: rig.topology.clone(),
        declared: Arc::new(Mutex::new(Declared::default())),
        meshes_storage: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
        fabric_storage: Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()),
        nodes_storage: Arc::new(MemoryNodesStorage::default()),
        caches: Default::default(),
        mesh_ids: Arc::new(move || mesh_ids.clone()),
        durable: Default::default(),
        commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
        own: Arc::new(std::sync::OnceLock::new()),
        mesh_deleter: Arc::new(std::sync::OnceLock::new()),
        hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
        configs: Default::default(),
        republish: Default::default(), configure: Default::default(),
        pre_node_ready: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(),
        loads: Some(Arc::new(BTreeMap::new)),
        digest_view: None,
        builds: std::sync::Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
    });
    let resolver = Arc::new(StaticResolver::new());
    for r in resolves {
        resolver.insert(r.clone());
    }
    let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
    status.caches.nodes.feeds(atlas.clone());
    let _ = status.atlas.set(atlas.clone());
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _ = status.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver.clone()).with_caller_system("rdm")), status.configure.clone(), status.caches.nodes.clone())));
    let slot: Arc<OnceLock<Arc<StatusAuthority>>> = Arc::new(OnceLock::new());
    let _ = slot.set(status.clone());
    let server = rdm_admin::status_rpc::serve(ServerBuilder::new(), slot).build(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().to_string() }).unwrap();
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let resolved = ResolvedNode { node_id: b.node.node_id.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    SecondAdmin { status, resolved, resolver, atlas, _router: router }
}

/// The rig's admin (the fabric-primary and mesh1's primary) with its atlas holding a record of
/// each of `births` and its config door telling through a client that resolves each of `resolves`.
async fn fabric_primary_door(rig: &Rig, births: &[&Birth], resolves: &[ResolvedNode]) -> Arc<rdm_admin::atlas::Atlas> {
    let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
    for b in births {
        atlas.write_record(&rdm_admin::storage::NodeRow::new(b.node.name.clone(), b.node.node_id.clone(), NodeStatus::Ready));
    }
    let _ = rig.authority.atlas.set(atlas.clone());
    // Every record its writer holds is its atlas's too (Q203, as the boot wires it).
    rig.authority.caches.nodes.feeds(atlas.clone());
    let resolver = Arc::new(StaticResolver::new());
    for r in resolves {
        resolver.insert(r.clone());
    }
    let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _ = rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")), rig.authority.configure.clone(), rig.authority.caches.nodes.clone())));
    atlas
}

/// `nodes.get [id: b]` at the rig's fabric-primary: the config its answer carries.
async fn config_read_from_the_fabric_primary(rig: &Rig, b: &Birth) -> BTreeMap<String, String> {
    let read = StatusRequest::Get { entity: rdm_core::filter::Entity::Nodes, node: None, filter: rdm_core::filter::Filter::new().eq("id", [b.node.node_id.to_string()]) };
    match rig.authority.apply(None, &read).await {
        StatusReply::Nodes { nodes } => {
            assert_eq!(nodes.len(), 1, "nodes.get [id] answers the one node: {nodes:?}");
            nodes[0].config.clone()
        }
        other => panic!("nodes.get at the fabric-primary answered {other:?}"),
    }
}

/// The node's own client: its endpoint, resolving only `to`.
fn own_client(node: &ServedNode, to: &ResolvedNode) -> NodeRpcClient {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(to.clone());
    NodeRpcClient::new(node.endpoint.clone(), resolver).with_caller_system("rdm")
}

/// Wait until `done` holds, polling: what an owed push's acknowledgement is read by.
async fn until(what: &str, done: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(20), async {
        while !done() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("{what}"));
}

/// CONTRACT (node-config-update.md, a node's own update, steps 1-3; Q235 P1-P3): mesh1.rpc.1 puts its
/// own config with one call to its mesh-primary, mesh1.admin.2, which is not the fabric-primary.
/// mesh1.admin.2 writes the row, stepping its revision, and its atlas; it answers `ConfigUpdated`
/// carrying the row as written (the map and its revision) and tells the node nothing; it owes the row
/// up to the fabric-primary mesh1.admin.1, whose atlas takes it at that revision and whose `nodes.get`
/// answers it. Spans: `via-own-config` at mesh1.admin.2 (route in-process, config-updated);
/// `via-config` apply=own push=owed; `via-config-push` at mesh1.admin.2 (applied); `via-config-pushed`
/// at mesh1.admin.1 (atlas installed). Must NOT happen: the update carried up to the fabric-primary
/// first (`via-forward`, `via-fabric-primary`), an `ApplyNodeConfig` to the node, or a row written by
/// the fabric-primary.
#[test]
fn a_nodes_own_config_update_is_written_by_its_mesh_primary_which_pushes_the_row_up_to_the_fabric_primarys_atlas() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        mesh1_primary_is_admin2(&rig, admin2).await;
        let node = serve_node(rpc1).await;
        let fp_atlas = fabric_primary_door(&rig, &[rpc1], &[]).await;
        let joined = second_admin(&rig, admin2, &[rig.resolved.clone(), node.resolved.clone()]).await;
        let config: BTreeMap<String, String> = [("listener.data".to_string(), "127.0.0.1:41001".to_string())].into();
        let opts = CallOptions { budget: Budget::Owed, ..CallOptions::default() };
        let reply = tokio::time::timeout(Duration::from_secs(20), rdm_client::NodeRpc::new(&own_client(&node, &joined.resolved)).own_config_update(&NodeTarget::ExactNode(admin2.node.node_id.clone()), &exact(rpc1), config.clone(), &opts)).await.expect("the owed call is answered").expect("a typed reply");
        assert_eq!(updated(&reply), updated_of(rpc1, &config));
        let StatusReply::ConfigUpdated { revision, .. } = reply else { unreachable!() };
        let row = joined.status.nodes_storage.node_at(&rpc1.node.name).await.unwrap().expect("mesh1's primary, mesh1.admin.2, wrote the node's row");
        assert_eq!((row.config.clone(), row.revision), (config.clone(), revision), "the answer carries the row as written");
        let mp_held = joined.atlas.record(&rpc1.node.node_id).expect("mesh1.admin.2's atlas holds the row");
        assert_eq!((mp_held.config, mp_held.revision), (config.clone(), revision), "the mesh-primary's atlas holds the map with the row");
        assert!(node.seen.lock().unwrap().is_empty(), "the node is told nothing: it holds the map it sent: {:?}", node.seen.lock().unwrap());
        assert!(rig.nodes.record_at(&rpc1.node.name).await.unwrap().is_none(), "the fabric-primary writes no row");
        until("the fabric-primary's atlas takes the pushed row", || fp_atlas.record(&rpc1.node.node_id).is_some_and(|r| r.revision == revision)).await;
        assert_eq!(fp_atlas.record(&rpc1.node.node_id).unwrap().config, config, "the fabric-primary's atlas holds the map at the row's revision");
        assert_eq!(config_read_from_the_fabric_primary(&rig, rpc1).await, config, "node.get at the fabric-primary answers the map");
        until("the push is answered", || joined.status.configs.get().unwrap().pushes_owed().is_empty()).await;
    });
    let own = cap.spans("rdm.node_admin.node.update.via-own-config");
    let (trace, o) = own.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.admin.2")).unwrap_or_else(|| panic!("a via-own-config span at mesh1.admin.2: {own:?}"));
    assert_eq!((o.get("route").map(String::as_str), o.get("subject").map(String::as_str), o.get("outcome").map(String::as_str)), (Some("in-process"), Some("mesh1.rpc.1"), Some("config-updated")), "{o:?}");
    let written = cap.spans("rdm.node_admin.node.update.via-config");
    let (_, w) = written.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.admin.2")).unwrap_or_else(|| panic!("a via-config span at mesh1.admin.2: {written:?}"));
    assert_eq!((w.get("apply").map(String::as_str), w.get("push").map(String::as_str), w.get("outcome").map(String::as_str)), (Some("own"), Some("owed"), Some("config-updated")), "{w:?}");
    let push = cap.spans("rdm.node_admin.node.update.via-config-push");
    let (push_trace, p) = push.iter().find(|(_, a)| a.get("subject").map(String::as_str) == Some("mesh1.rpc.1")).unwrap_or_else(|| panic!("a via-config-push span: {push:?}"));
    assert_eq!((p.get("node").map(String::as_str), p.get("fabric_primary").map(String::as_str), p.get("outcome").map(String::as_str)), (Some("mesh1.admin.2"), Some("mesh1.admin.1"), Some("applied")), "{p:?}");
    let pushed = cap.spans("rdm.node_admin.node.update.via-config-pushed");
    let (_, f) = pushed.iter().find(|(_, a)| a.get("subject").map(String::as_str) == Some("mesh1.rpc.1")).unwrap_or_else(|| panic!("a via-config-pushed span: {pushed:?}"));
    assert_eq!((f.get("node").map(String::as_str), f.get("atlas").map(String::as_str), f.get("outcome").map(String::as_str)), (Some("mesh1.admin.1"), Some("installed"), Some("applied")), "{f:?}");
    assert!(cap.spans("rdm.node_admin.node.update.via-forward").is_empty() && cap.spans("rdm.node_admin.node.update.via-fabric-primary").is_empty(), "nothing goes to the fabric-primary first");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-own-config node=mesh1.admin.2 > via-config apply=own push=owed; trace {push_trace}: via-config-push fabric_primary=mesh1.admin.1 outcome=applied > via-config-pushed atlas=installed");
}

/// CONTRACT (node-config-update.md, a node's own update, step 3; Q235 P3; node-rpc.md §43: owed until
/// acknowledged, with backoff): mesh1.rpc.1's own update at its mesh-primary mesh1.admin.2 is answered
/// once the row and mesh1.admin.2's atlas hold the map, before the fabric-primary holds it: the
/// fabric-primary its view names, mesh2.admin.1, refuses the push `ReceiverNotPrimary` until it holds
/// it. The push stays owed and is sent again (the view's next change or the backoff's next delay), the
/// same row at the same revision, until mesh2.admin.1 answers `Applied`. Span: `via-config-push`,
/// `sends` >= 2, outcome `applied`. Must NOT happen: the node's answer waiting on the fabric-primary,
/// or the push dropped on the refusal.
#[test]
fn a_push_the_fabric_primary_has_not_acknowledged_stays_owed_and_lands_once_it_answers() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1, m2admin) = (&b["admin2"], &b["rpc1"], &b["m2admin"]);
        mesh1_primary_is_admin2(&rig, admin2).await;
        for n in rig.topology.write().await.nodes.iter_mut() {
            n.is_fabric_primary = n.node_id == m2admin.node.node_id;
        }
        let landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let holds = landed.clone();
        let fp = serve_node_with(m2admin, move |req| match req {
            StatusRequest::NodeConfigUpdated { .. } if !holds.load(Ordering::SeqCst) => rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "the fabric-primary".into() }),
            StatusRequest::NodeConfigUpdated { .. } => StatusReply::Applied,
            other => panic!("only the push reaches the fabric-primary: {other:?}"),
        })
        .await;
        let node = serve_node(rpc1).await;
        let joined = second_admin(&rig, admin2, &[fp.resolved.clone(), node.resolved.clone()]).await;
        let (tick, views) = tokio::sync::watch::channel(0u64);
        joined.status.configs.get().unwrap().watch_views(views);
        let config: BTreeMap<String, String> = [("zone".to_string(), "b".to_string())].into();
        let opts = CallOptions { budget: Budget::Owed, ..CallOptions::default() };
        let reply = tokio::time::timeout(Duration::from_secs(20), rdm_client::NodeRpc::new(&own_client(&node, &joined.resolved)).own_config_update(&NodeTarget::ExactNode(admin2.node.node_id.clone()), &exact(rpc1), config.clone(), &opts)).await.expect("answered without the fabric-primary").expect("a typed reply");
        assert_eq!(updated(&reply), updated_of(rpc1, &config));
        let StatusReply::ConfigUpdated { revision, .. } = reply else { unreachable!() };
        tokio::time::timeout(Duration::from_secs(10), fp.heard.notified()).await.expect("the push was sent");
        assert_eq!(joined.status.configs.get().unwrap().pushes_owed(), vec![(rpc1.node.node_id.clone(), revision)], "refused, the push stays owed");
        assert_eq!(joined.atlas.record(&rpc1.node.node_id).map(|r| (r.config, r.revision)), Some((config.clone(), revision)), "the mesh-primary's atlas holds the map before the fabric-primary does");
        landed.store(true, Ordering::SeqCst);
        tick.send_modify(|t| *t += 1);
        until("the push is acknowledged once the fabric-primary answers", || joined.status.configs.get().unwrap().pushes_owed().is_empty()).await;
        let sent = fp.seen.lock().unwrap().clone();
        let push = StatusRequest::NodeConfigUpdated { node_id: rpc1.node.node_id.clone(), config, revision };
        assert!(sent.len() >= 2 && sent.iter().all(|r| *r == push), "the same row at the same revision, sent again: {sent:?}");
    });
    let push = cap.spans("rdm.node_admin.node.update.via-config-push");
    let (trace, p) = push.iter().find(|(_, a)| a.get("subject").map(String::as_str) == Some("mesh1.rpc.1")).unwrap_or_else(|| panic!("a via-config-push span: {push:?}"));
    let sends: u32 = p.get("sends").and_then(|v| v.parse().ok()).unwrap_or(0);
    assert!(sends >= 2 && p.get("fabric_primary").map(String::as_str) == Some("mesh2.admin.1") && p.get("outcome").map(String::as_str) == Some("applied"), "{p:?}");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-config-push node=mesh1.admin.2 fabric_primary=mesh2.admin.1 sends={sends} outcome=applied");
}

/// CONTRACT (node-config-update.md, a node's own update, step 1: the node sends it to the mesh-primary
/// its topology view names; Q235 P5: the mesh-primary is the row's one writer): mesh1.rpc.1's own
/// update reaching mesh1.admin.1, the fabric-primary but not mesh1's primary (mesh1.admin.2 is), is
/// refused `ReceiverNotPrimary` naming mesh1's primary; nothing is written and nothing is carried. The
/// node sends it again to the mesh-primary its view names. Span: `via-own-config` at mesh1.admin.1,
/// outcome `rejected-not-primary`. Must NOT happen: the fabric-primary writing the row or sending the
/// command down (a caller's path).
#[test]
fn a_nodes_own_config_update_at_an_admin_that_is_not_its_mesh_primary_is_refused_naming_it() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        mesh1_primary_is_admin2(&rig, admin2).await;
        let node = serve_node(rpc1).await;
        let fp_atlas = fabric_primary_door(&rig, &[rpc1], &[]).await;
        let config: BTreeMap<String, String> = [("zone".to_string(), "c".to_string())].into();
        let reply = rdm_client::NodeRpc::new(&own_client(&node, &rig.resolved)).own_config_update(&NodeTarget::ExactNode(rig.admin.node.node_id.clone()), &exact(rpc1), config, &CallOptions::default()).await.expect("a typed reply");
        let StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } = &reply else { panic!("refused, naming mesh1's primary: {reply:?}") };
        assert!(needed.contains("mesh1's mesh-primary") && needed.contains("mesh1.admin.2"), "{needed}");
        assert!(rig.nodes.record_at(&rpc1.node.name).await.unwrap().is_none(), "nothing is written");
        assert!(fp_atlas.record(&rpc1.node.node_id).is_some_and(|r| r.config.is_empty() && r.revision == 0), "the atlas is unchanged");
    });
    let own = cap.spans("rdm.node_admin.node.update.via-own-config");
    let (trace, o) = own.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.admin.1")).unwrap_or_else(|| panic!("a via-own-config span at mesh1.admin.1: {own:?}"));
    assert_eq!(o.get("outcome").map(String::as_str), Some("rejected-not-primary"), "{o:?}");
    assert!(cap.spans("rdm.node_admin.node.update.via-fabric-primary").is_empty(), "nothing is sent down");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-own-config node=mesh1.admin.1 outcome=rejected-not-primary");
}

/// CONTRACT (node-rpc.md §43: a named refusal ends one request's retry and the owning workflow
/// retargets it; the fabric-primary is the one the forwarding admin's records name, never a digest):
/// mesh1.admin.2 carries a caller's config update of mesh1.rpc.2 (sent by mesh1.rpc.1, a node that is not
/// a node-admin, about another node) up while the fabric-primary is changing: the
/// admin its view names (mesh2.admin.1) answers `ReceiverNotPrimary` until it holds it. The caller is
/// not failed: once mesh2.admin.1 holds it and mesh1.admin.2's view changes, the same command is carried
/// up again and its answer handed back. Span: `via-forward` with `sends` >= 2, outcome `config-updated`.
#[test]
fn a_callers_command_carried_up_while_the_fabric_primary_changes_completes_once_the_new_fabric_primary_holds_it() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc2, m2admin) = (&b["admin2"], &b["rpc2"], &b["m2admin"]);
        // The view names mesh2.admin.1 the fabric-primary.
        for n in rig.topology.write().await.nodes.iter_mut() {
            n.is_fabric_primary = n.node_id == m2admin.node.node_id;
        }
        let landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let holds = landed.clone();
        let (n, i) = (rpc2.node.node_id.clone(), rpc2.node.incarnation_id.clone().unwrap());
        let fp = serve_node_with(m2admin, move |req| match req {
            StatusRequest::UpdateNodeConfig { .. } if !holds.load(Ordering::SeqCst) => rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "the fabric-primary".into() }),
            StatusRequest::UpdateNodeConfig { config, .. } => StatusReply::ConfigUpdated { node_id: n.clone(), incarnation: i.clone(), config: config.clone(), revision: 3 },
            other => panic!("only the config command reaches the fabric-primary: {other:?}"),
        })
        .await;
        let joined = second_admin(&rig, admin2, &[fp.resolved.clone()]).await;
        let (tick, views) = tokio::sync::watch::channel(0u64);
        joined.status.configs.get().unwrap().watch_views(views);
        // The caller is mesh1.rpc.1, about mesh1.rpc.2.
        let node = serve_node(&b["rpc1"]).await;
        let client = own_client(&node, &joined.resolved);
        let config: BTreeMap<String, String> = [("zone".to_string(), "b".to_string())].into();
        let opts = CallOptions { budget: Budget::Owed, ..CallOptions::default() };
        let (rpc, admin_target, birth) = (rdm_client::NodeRpc::new(&client), NodeTarget::ExactNode(admin2.node.node_id.clone()), exact(rpc2));
        let call = rpc.own_config_update(&admin_target, &birth, config.clone(), &opts);
        let land = async {
            tokio::time::timeout(Duration::from_secs(10), fp.heard.notified()).await.expect("mesh1.admin.2 carried the command up");
            landed.store(true, Ordering::SeqCst);
            tick.send_modify(|t| *t += 1);
        };
        let (reply, ()) = tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(call, land) }).await.expect("the carried command completes once the fabric-primary is held");
        assert_eq!(reply.expect("a typed reply"), StatusReply::ConfigUpdated { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap(), config: config.clone(), revision: 3 });
        let sent = fp.seen.lock().unwrap().clone();
        let req = StatusRequest::UpdateNodeConfig { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap(), config };
        assert!(sent.len() >= 2 && sent.iter().all(|r| *r == req), "the same command, carried up again: {sent:?}");
    });
    let up = cap.spans("rdm.node_admin.node.update.via-forward");
    let (trace, a) = up.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.admin.2")).unwrap_or_else(|| panic!("a via-forward span at mesh1.admin.2: {up:?}"));
    let sends: u32 = a.get("sends").and_then(|v| v.parse().ok()).unwrap_or(0);
    assert!(sends >= 2 && a.get("fabric_primary").map(String::as_str) == Some("mesh2.admin.1") && a.get("outcome").map(String::as_str) == Some("config-updated"), "{a:?}");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-forward node=mesh1.admin.2 fabric_primary=mesh2.admin.1 sends={sends} outcome=config-updated");
}

/// CONTRACT (node-rpc-envelope.md node.tags; node-config-update.md step 1, the same door for a node's
/// own properties; D21): mesh1.rpc.1 creates its own tags with a call to the node-admin it joined
/// through, mesh1.admin.2, which is not the fabric-primary: mesh1.admin.2 carries it up to the
/// fabric-primary, mesh1's primary, which writes the row carrying the labels: `TagsCreated`, the row
/// holds them, and mesh1.admin.2 writes nothing. Span: `via-forward` at mesh1.admin.2, op
/// `node.tags.create`, outcome `tags-created`. Must NOT happen: `ReceiverNotPrimary` at mesh1.admin.2.
#[test]
fn a_nodes_own_tags_create_through_an_admin_that_is_not_the_fabric_primary_is_carried_up_and_written() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let node = serve_node(rpc1).await;
        fabric_primary_door(&rig, &[rpc1], &[node.resolved.clone()]).await;
        let joined = second_admin(&rig, admin2, &[rig.resolved.clone()]).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(joined.resolved.clone());
        let client = NodeRpcClient::new(node.endpoint.clone(), resolver).with_caller_system("rdm");
        let opts = CallOptions { budget: Budget::Owed, ..CallOptions::default() };
        let (out, _) = tokio::time::timeout(Duration::from_secs(20), client.call::<Status>(&NodeTarget::ExactNode(admin2.node.node_id.clone()), &create_tags(rpc1, &["edge"]), &opts)).await.expect("the owed call is answered");
        let StatusReply::TagsCreated { node_id, .. } = reply(&out) else { panic!("tags created: {out:?}") };
        assert_eq!(node_id, rpc1.node.node_id);
        let row = rig.nodes.record_at(&rpc1.node.name).await.unwrap().expect("mesh1's primary wrote the node's row");
        assert_eq!(row.tags, labels(&["edge"]));
        assert!(joined.status.nodes_storage.node_at(&rpc1.node.name).await.unwrap().is_none(), "mesh1.admin.2 writes no row");
    });
    let up = cap.spans("rdm.node_admin.node.update.via-forward");
    let (trace, a) = up.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.admin.2")).unwrap_or_else(|| panic!("a via-forward span at mesh1.admin.2: {up:?}"));
    assert_eq!((a.get("op").map(String::as_str), a.get("fabric_primary").map(String::as_str), a.get("outcome").map(String::as_str)), (Some("node.tags.create"), Some("mesh1.admin.1"), Some("tags-created")), "{a:?}");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-forward node=mesh1.admin.2 op=node.tags.create outcome=tags-created");
}

/// The rig's view with mesh1.admin.2 mesh1's primary and mesh1.admin.1 the fabric-primary only.
async fn mesh1_primary_is_admin2(rig: &Rig, admin2: &Birth) {
    for n in rig.topology.write().await.nodes.iter_mut() {
        if n.kind == NodeKind::NodeAdmin && n.mesh == "mesh1" {
            n.is_primary = n.node_id == admin2.node.node_id;
        }
    }
}

/// CONTRACT (node-config-update.md, a node's own update; Q235 P2, P4: no special case for a node-admin;
/// D12): mesh1.admin.2, mesh1's primary and not the fabric-primary, puts its own `admin_api_base` in
/// process: it holds the map at once, writes its own row and atlas in process (its own mesh-primary),
/// installs the answer's row, and owes the row up to the fabric-primary, whose atlas takes it.
/// `ConfigUpdated` carrying the row as written; what holds its config sees `Sent` then `Answered`, never
/// a `Told`. Must NOT happen: the command carried up to the fabric-primary and sent back down to it.
#[test]
fn a_mesh_primarys_own_config_update_is_written_in_process_held_at_once_and_pushed_up() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let admin2 = &b["admin2"];
        mesh1_primary_is_admin2(&rig, admin2).await;
        let fp_atlas = fabric_primary_door(&rig, &[admin2], &[]).await;
        let joined = second_admin(&rig, admin2, &[rig.resolved.clone()]).await;
        let (held, hold) = holding();
        let _ = joined.status.configure.set(hold);
        let config: BTreeMap<String, String> = [("admin_api_base".to_string(), "rdm://mesh1.admin.2".to_string())].into();
        let reply = tokio::time::timeout(Duration::from_secs(20), joined.status.own_config_update(admin2.node.incarnation_id.clone().unwrap(), config.clone())).await.expect("the command completes");
        assert_eq!(updated(&reply), updated_of(admin2, &config));
        let StatusReply::ConfigUpdated { revision, .. } = reply else { unreachable!() };
        let row = joined.status.nodes_storage.node_at(&admin2.node.name).await.unwrap().expect("mesh1.admin.2 wrote its own row");
        assert_eq!((row.config.clone(), row.revision), (config.clone(), revision));
        use rdm_admin::status_rpc::ConfigHeld;
        assert_eq!(held.lock().unwrap().clone(), vec![ConfigHeld::Sent(config.clone()), ConfigHeld::Answered(Some((config.clone(), revision)))], "held at once, then the row as written installed; never told");
        until("the fabric-primary's atlas takes the pushed row", || fp_atlas.record(&admin2.node.node_id).is_some_and(|r| r.revision == revision)).await;
        assert_eq!(config_read_from_the_fabric_primary(&rig, admin2).await, config, "node.get at the fabric-primary answers the map");
        until("the push is answered", || joined.status.configs.get().unwrap().pushes_owed().is_empty()).await;
    });
    let written = cap.spans("rdm.node_admin.node.update.via-config");
    let (trace, w) = written.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.admin.2")).unwrap_or_else(|| panic!("a via-config span at mesh1.admin.2: {written:?}"));
    assert_eq!((w.get("apply").map(String::as_str), w.get("push").map(String::as_str), w.get("outcome").map(String::as_str)), (Some("own"), Some("owed"), Some("config-updated")), "{w:?}");
    assert!(cap.spans("rdm.node_admin.node.update.via-forward").is_empty(), "nothing is carried up first");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-config node=mesh1.admin.2 subject=mesh1.admin.2 apply=own push=owed outcome=config-updated");
}

/// CONTRACT (node-config-update.md, a node's own update, steps 1-2; Q235 P4: the destination is the
/// mesh-primary its view names, in process only when that is this process): mesh1.admin.2, holding no
/// primary, puts its own config: it holds the map at once and sends it to mesh1's primary, mesh1.admin.1
/// (also the fabric-primary), owed until answered; mesh1.admin.1 writes the row and its atlas, which is
/// the fabric-primary's (nothing to push), and tells mesh1.admin.2 nothing; the answer carries the row
/// as written, which mesh1.admin.2 installs. Spans: `via-own-config` at mesh1.admin.2 naming
/// mesh_primary mesh1.admin.1, route owed; `via-config` at mesh1.admin.1 apply=own push=fabric-primary.
/// Must NOT happen: an `ApplyNodeConfig` to mesh1.admin.2, or a row written by mesh1.admin.2.
#[test]
fn a_node_admins_own_config_update_goes_to_its_mesh_primary_which_writes_it_and_tells_it_nothing() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let admin2 = &b["admin2"];
        let joined = second_admin(&rig, admin2, &[rig.resolved.clone()]).await;
        let fp_atlas = fabric_primary_door(&rig, &[admin2], &[joined.resolved.clone()]).await;
        let (held, hold) = holding();
        let _ = joined.status.configure.set(hold);
        let config: BTreeMap<String, String> = [("admin_api_base".to_string(), "rdm://mesh1.admin.2".to_string())].into();
        let reply = tokio::time::timeout(Duration::from_secs(20), joined.status.own_config_update(admin2.node.incarnation_id.clone().unwrap(), config.clone())).await.expect("the command completes");
        assert_eq!(updated(&reply), updated_of(admin2, &config));
        let StatusReply::ConfigUpdated { revision, .. } = reply else { unreachable!() };
        let row = rig.nodes.record_at(&admin2.node.name).await.unwrap().expect("mesh1's primary wrote mesh1.admin.2's row");
        assert_eq!((row.config.clone(), row.revision), (config.clone(), revision));
        assert_eq!(fp_atlas.record(&admin2.node.node_id).map(|r| (r.config, r.revision)), Some((config.clone(), revision)), "the fabric-primary's atlas, mesh1's primary's own, holds it with the row");
        use rdm_admin::status_rpc::ConfigHeld;
        assert_eq!(held.lock().unwrap().clone(), vec![ConfigHeld::Sent(config.clone()), ConfigHeld::Answered(Some((config.clone(), revision)))], "held at once, then the row as written; never told");
        assert!(joined.status.nodes_storage.node_at(&admin2.node.name).await.unwrap().is_none(), "mesh1.admin.2 writes no row of its own");
    });
    let own = cap.spans("rdm.node_admin.node.update.via-own-config");
    let (trace, o) = own.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.admin.2")).unwrap_or_else(|| panic!("a via-own-config span at mesh1.admin.2: {own:?}"));
    assert_eq!((o.get("mesh_primary").map(String::as_str), o.get("route").map(String::as_str), o.get("outcome").map(String::as_str)), (Some("mesh1.admin.1"), Some("owed"), Some("config-updated")), "{o:?}");
    let written = cap.spans("rdm.node_admin.node.update.via-config");
    let (_, w) = written.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.admin.1")).unwrap_or_else(|| panic!("a via-config span at mesh1.admin.1: {written:?}"));
    assert_eq!((w.get("apply").map(String::as_str), w.get("push").map(String::as_str), w.get("subject").map(String::as_str)), (Some("own"), Some("fabric-primary"), Some("mesh1.admin.2")), "{w:?}");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-own-config node=mesh1.admin.2 mesh_primary=mesh1.admin.1 route=owed > via-config node=mesh1.admin.1 apply=own push=fabric-primary");
}

/// CONTRACT (node-config-update.md, a node's own update, step 3: "A successor mesh-primary owes it again
/// from the row"; Q235 P1, P3): mesh1.admin.2, taking mesh1's writing with a row of mesh1.rpc.1 at
/// revision 5 whose push its predecessor never made, owes the row up again; the fabric-primary's atlas
/// takes it over its older revision. A push at an older revision than the atlas holds is answered
/// `AlreadyApplied` and changes nothing; the push at an admin that is not the fabric-primary is refused
/// `ReceiverNotPrimary` naming it. Must NOT happen: an older map over a newer one in the atlas.
#[test]
fn a_successor_mesh_primary_owes_every_rows_push_again_and_the_fabric_primary_takes_only_a_newer_revision() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        mesh1_primary_is_admin2(&rig, admin2).await;
        let fp_atlas = fabric_primary_door(&rig, &[rpc1], &[]).await;
        let joined = second_admin(&rig, admin2, &[rig.resolved.clone()]).await;
        let config: BTreeMap<String, String> = [("zone".to_string(), "e".to_string())].into();
        let row = rdm_admin::storage::NodeRow { config: config.clone(), revision: 5, ..rdm_admin::storage::NodeRow::new(rpc1.node.name.clone(), rpc1.node.node_id.clone(), NodeStatus::Ready) };
        joined.status.caches.nodes.hold(&row);
        let view = rig.topology.read().await.clone();
        joined.status.configs.get().unwrap().owe_pushes(&admin2.node.name, &admin2.node.node_id, &view, "mesh1");
        until("the successor's push lands", || fp_atlas.record(&rpc1.node.node_id).is_some_and(|r| r.revision == 5)).await;
        assert_eq!(fp_atlas.record(&rpc1.node.node_id).unwrap().config, config);
        until("the push is answered", || joined.status.configs.get().unwrap().pushes_owed().is_empty()).await;
        // An older revision changes nothing.
        let older = StatusRequest::NodeConfigUpdated { node_id: rpc1.node.node_id.clone(), config: [("zone".to_string(), "old".to_string())].into(), revision: 4 };
        assert_eq!(reply(&call(&rig, admin2, &older).await), StatusReply::AlreadyApplied);
        assert_eq!(fp_atlas.record(&rpc1.node.node_id).map(|r| (r.config, r.revision)), Some((config.clone(), 5)), "the newer map stands");
        // At an admin that is not the fabric-primary: refused by name.
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(joined.resolved.clone());
        let ep = rdm_transport::endpoint::bind(rpc1.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let at_joined = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
        let (out, _) = at_joined.call::<Status>(&NodeTarget::ExactNode(admin2.node.node_id.clone()), &older, &CallOptions::default()).await;
        let refused = match out {
            RpcOutcome::Reply(r) => r.value().clone(),
            other => panic!("a reply: {other:?}"),
        };
        assert!(matches!(&refused, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } if needed.contains("mesh1.admin.1")), "{refused:?}");
    });
    let pushed = cap.spans("rdm.node_admin.node.update.via-config-pushed");
    let atlas: Vec<_> = pushed.iter().filter_map(|(_, a)| a.get("atlas").cloned()).collect();
    assert!(atlas.contains(&"installed".to_string()) && atlas.contains(&"held:5".to_string()), "{pushed:?}");
    eprintln!("trace {}: rdm.node_admin.node.update.via-config-pushed atlas={atlas:?}", pushed[0].0);
}

/// The spans a cell's own run exported through the product span pipeline
/// (`rdm_transport::span_capture`), with their trace ids.
struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
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
    connections.held().lock().unwrap().set_topology_view(live.clone());
    let node_rpc = rdm_admin::node_rpc::ProcessNodeRpc::new(live.clone(), ep.clone());
    let server = rdm_admin::admin::rpc_server(connections, node_rpc.client.clone(), Arc::new(OnceLock::new()), Arc::new(OnceLock::new()), Arc::new(OnceLock::new()))
        .build(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().to_string() })
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
        let (held, hold) = holding();
        let _ = rig.authority.configure.set(hold);
        let config: BTreeMap<String, String> = [("admin_api_base".to_string(), "http://127.0.0.1:9".to_string())].into();
        let me = &rig.admin.node;
        let told = |incarnation: IncarnationId, config: BTreeMap<String, String>| StatusRequest::ApplyNodeConfig { node_id: me.node_id.clone(), incarnation, config, revision: 2 };
        let as_told = |config: BTreeMap<String, String>| rdm_admin::status_rpc::ConfigHeld::Told { config, revision: 2 };
        assert_eq!(reply(&call(&rig, admin2, &told(me.incarnation_id.clone().unwrap(), config.clone())).await), StatusReply::Applied);
        assert_eq!(held.lock().unwrap().clone(), vec![as_told(config.clone())], "the admin holds the whole config it was told, at its revision");
        assert_eq!(reply(&call(&rig, admin2, &told(IncarnationId::mint(), config.clone())).await), StatusReply::RejectedStaleIncarnation { held: me.incarnation_id.clone().unwrap() });
        assert_eq!(reply(&call(&rig, rpc1, &told(me.incarnation_id.clone().unwrap(), config.clone())).await), StatusReply::Applied, "the subject answers whoever sends it");
        assert_eq!(held.lock().unwrap().clone(), vec![as_told(config.clone()), as_told(config)], "the refusal held nothing; each apply handed the whole config at its revision");
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
fn a_sender_rebound_on_its_key_while_its_old_connection_is_held_reaches_the_mesh_primary() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let admin2 = &b["admin2"];
        let me = &rig.admin.node;
        let fp = NodeTarget::ExactNode(me.node_id.clone());
        // A told config naming another birth of the authority: answered by name, nothing held.
        let stale = StatusRequest::ApplyNodeConfig { node_id: me.node_id.clone(), incarnation: IncarnationId::mint(), config: BTreeMap::new(), revision: 1 };
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
const NAME_SPAN: &str = "rdm.node_admin.name.update.via-writer";

/// `b` stopped in the mesh-primary's view (`Leaving`), its row at its name carrying `tags`.
async fn stopped_with_row(rig: &Rig, b: &Birth, tags: &[&str]) {
    if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == b.node.node_id) {
        (n.stopped, n.status) = (true, NodeStatus::Leaving);
    }
    let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: b.node.name.clone(), node_id: b.node.node_id.clone(), status: NodeStatus::Leaving, config: Default::default(), tags: labels(tags), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
    rig.nodes.write_record(&row).await.unwrap();
}

/// The mesh-primary holds `b` as a rename needs it (node-name-update.md Precondition): live and
/// connections-deleting in its view, its `node.connections.deleted` of an `All` held with nothing held,
/// then its `node.mesh.left`.
async fn hold_left(rig: &Rig, b: &Birth) {
    if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == b.node.node_id) {
        (n.status, n.declared, n.stopped) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()), false);
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
    let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: b.node.name.clone(), node_id: b.node.node_id.clone(), status: NodeStatus::ConnectionsDeleting, config: Default::default(), tags: labels(tags), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
    rig.nodes.write_record(&row).await.unwrap();
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
/// ready node, or one still holding a connection, is refused"; "the precondition holds,
/// else `RejectedAdmittingConnections { status, held_connections }`"; §2.4 "Refused before the claim
/// ...: nothing written, nothing changed"): a caller's `node.name.update` of the live,
/// ready successor `mesh1.rpc.8` to the name `3` at mesh1's primary is refused
/// `RejectedAdmittingConnections { status: Ready, held_connections: [] }`. Nothing is claimed
/// (no row at `mesh1.rpc.3`), the view keeps the birth at `mesh1.rpc.8`, and the birth is told nothing. One
/// `rdm.node_admin.name.update.via-writer` span says `rejected-admitting-connections` with
/// `traffic=Ready` and `connections=none-deleted`. What must NOT happen: the row written, the
/// birth told `ApplyNodeName`, or `NameUpdated`.
#[test]
fn a_ready_nodes_rename_is_refused_admitting_connections_and_nothing_is_claimed_or_told() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_new(&rig).await;
        let reply = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(reply, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::Ready, held_connections: vec![] });
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap(), None, "nothing is claimed at the new name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == succ.node.node_id && n.name == succ.node.name), "the view keeps the birth at its name");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(
        (attrs.get("outcome").map(String::as_str), attrs.get("traffic").map(String::as_str), attrs.get("connections").map(String::as_str), attrs.get("answered")),
        (Some("rejected-admitting-connections"), Some("Ready"), Some("none-deleted"), None),
        "{attrs:?}"
    );
    eprintln!("trace {trace_id}: {NAME_SPAN} from=mesh1.rpc.8 name=3 traffic=Ready connections=none-deleted outcome=rejected-admitting-connections");
}

/// CONTRACT (`docs/architecture/lifecycles/node-name-update.md:16` — "Precondition: The node is live
/// and admits no connection: this mesh-primary holds the birth's `node.connections.deleted` of an `All`,
/// then its `node.mesh.left` (`node.mesh.leave`), and it holds zero connections" and
/// `docs/architecture/lifecycles/node-name-update.md:18` — "The move is the name claim at the new
/// path.name (`NameHeld { by: node.id }` names the node it is for), then one write of the node's record
/// carrying `renamed_from: <old path.name>`, then the old claim's release"; steps 4-5; Completion "The
/// left node: `node.name.updated` after `Applied`"): the successor `mesh1.rpc.8`, ConnectionsDeleting in the mesh-primary's view, is
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
// @feature: node-name-update
#[test]
fn a_nodes_rename_is_refused_until_its_connections_are_deleted_and_it_has_left_its_mesh_then_applied() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_connections_deleted(&rig).await;
        let taken: PathName = "mesh1.rpc.3".parse().unwrap();
        let refused = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(refused, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::ConnectionsDeleting, held_connections: vec![] });
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap(), None, "nothing is claimed at the new name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");

        let deleted = reply(&call(&rig, &b["admin2"], &delete_all(&succ)).await);
        assert!(matches!(&deleted, StatusReply::ConnectionsDeleted { held, .. } if held.is_empty()), "the mesh-primary answers the birth's node.connections.deleted: {deleted:?}");
        assert_eq!(node.seen.lock().unwrap().clone(), vec![delete_all(&succ)], "the delete is carried to the exact birth");
        let on_mesh = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(on_mesh, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::ConnectionsDeleting, held_connections: vec![] }, "a birth still on its mesh keeps its name");
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap(), None, "nothing is claimed while the birth is on its mesh");

        let left = rig.authority.carry_mesh_leave(&succ.node.node_id, succ.node.incarnation_id.as_ref().unwrap(), "3vzt99cnx0xe", 1, "node.mesh.leave:mesh1.rpc.8").await;
        assert!(matches!(&left, Ok(StatusReply::MeshLeft { .. })), "the mesh-primary carries the leave and the birth answers MeshLeft: {left:?}");
        assert_eq!(node.seen.lock().unwrap().last().cloned(), Some(leave_mesh(&succ, "node.mesh.leave:mesh1.rpc.8")), "the leave is carried to the exact birth");
        let applied = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(applied, StatusReply::NameUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() });
        let row = rig.nodes.record_at(&taken).await.unwrap().expect("the row at the new name");
        assert_eq!((row.node_id, row.renamed_from), (succ.node.node_id.clone(), Some(succ.node.name.clone())), "the row at the new name holds the birth and names the key it moved from");
        assert_eq!(rig.nodes.record_at(&succ.node.name).await.unwrap(), None, "the old key is released");
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
    let carried = cap.spans("rdm.node_admin.connection.delete.via-mesh-primary");
    assert_eq!(carried.len(), 1, "{carried:?}");
    assert_eq!((carried[0].1.get("scope").map(String::as_str), carried[0].1.get("status_at_cut").map(String::as_str), carried[0].1.get("outcome").map(String::as_str)), (Some("all"), Some("ConnectionsDeleting"), Some("connections-deleted")), "{carried:?}");
    let leaves = cap.spans("rdm.node_admin.node.remove.via-mesh-leave");
    assert_eq!(leaves.iter().map(|(_, a)| (a.get("subject").cloned().unwrap_or_default(), a.get("outcome").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![(s("mesh1.rpc.8"), s("mesh-left"))], "{leaves:?}");
    eprintln!("trace {}: rdm.node_admin.connection.delete.via-mesh-primary scope=all status_at_cut=ConnectionsDeleting outcome=connections-deleted", carried[0].0);
    eprintln!("trace {}: rdm.node_admin.node.remove.via-mesh-leave subject=mesh1.rpc.8 outcome=mesh-left", leaves[0].0);
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
        assert_eq!(rig.nodes.record_at(&"mesh1.rpc.3".parse().unwrap()).await.unwrap(), None, "nothing is claimed");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(outcomes_and_connections(&spans), vec![("rejected-admitting-connections".to_string(), "held".to_string())], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} traffic=ConnectionsDeleting connections=held held_connections={held_id} outcome=rejected-admitting-connections", spans[0].0);
}

/// CONTRACT (proposal §2.1 Claim and move: "a held name answers `NameHeld { by: node.id }` and
/// nothing is written"): a rename of a successor off its mesh (its connections deleted, its mesh left)
/// into a name another node holds is refused
/// `NameHeld { by }` by its claim; nothing is written and the birth is told
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
        rig.nodes.write_record(&rdm_admin::storage::NodeRow { topology_storage_provider: None, name: taken.clone(), node_id: holder.clone(), status: NodeStatus::Leaving, config: Default::default(), tags: Default::default(), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None }).await.unwrap();
        let held = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(held, StatusReply::NameHeld { name: taken.to_string(), by: holder.clone() });
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap().map(|r| r.node_id), Some(holder), "the holder keeps the name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
        let routed = &b["rpc1"];
        let refused = reply(&call(&rig, &b["admin2"], &update_name(routed, "1.old")).await);
        assert_eq!(refused, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::Ready, held_connections: vec![] }, "a live node that can carry traffic keeps its name");
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
        let row = rig.nodes.record_at(&taken).await.unwrap().expect("the mesh-primary wrote the row at the new name");
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
/// `mesh1.rpc.2` (stopped in the mesh-primary's view, its row at `mesh1.rpc.2`) to `2.old` is refused
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
        let row = rig.nodes.record_at(&stopped.node.name).await.unwrap();
        let to: PathName = "mesh1.rpc.2.old".parse().unwrap();
        let refused = StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::Leaving, held_connections: vec![] };
        assert_eq!(reply(&call(&rig, &b["admin2"], &update_name(stopped, "2.old")).await), refused);
        assert_eq!(rig.nodes.record_at(&to).await.unwrap(), None, "nothing is claimed at the new name");
        assert_eq!(rig.nodes.record_at(&stopped.node.name).await.unwrap(), row, "the row stands at its key as it was");
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
        assert_eq!(rig.nodes.record_at(&to).await.unwrap().map(|r| r.node_id), Some(left.node.node_id.clone()), "the row is at the new name");
        assert_eq!(rig.nodes.record_at(&left.node.name).await.unwrap(), None, "the old key is released");
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
        assert_eq!(rig.nodes.node(&left.node.node_id).await.unwrap().map(|r| r.name), Some(left.node.name.clone()), "the node's record stays at its name");
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
/// written) after the put, a second row written, or the put undone.
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
        let moved = rig.nodes.record_at(&to).await.unwrap().expect("the put at the new key stands");
        assert_eq!((moved.node_id.clone(), moved.renamed_from.clone()), (stopped.node.node_id.clone(), Some(stopped.node.name.clone())));
        assert_eq!((rig.nodes.name_claim(&stopped.node.name).await.unwrap(), rig.nodes.name_claim(&to).await.unwrap()), (Some(stopped.node.node_id.clone()), Some(stopped.node.node_id.clone())), "the node holds both names' claims");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing while the release is pending");
        let done = reply(&call(&rig, &b["admin2"], &update_name(stopped, "2.old")).await);
        assert_eq!(done, StatusReply::NameUpdated { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.name_claim(&stopped.node.name).await.unwrap(), None, "the retry released the old name's claim");
        assert_eq!(rig.nodes.record_at(&to).await.unwrap(), Some(moved), "the destination row stands as written");
        assert_eq!(node.seen.lock().unwrap().clone(), vec![apply_name(stopped, &to)], "the retry told the birth its name");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("answered").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("name-not-applied".to_string(), "release-pending".to_string()), ("name-updated".to_string(), "applied".to_string())], "{spans:?}");
    let releases = cap.spans("rdm.node_admin.name.update.via-pending-release");
    assert_eq!(releases.iter().map(|(_, a)| (a.get("from").cloned().unwrap_or_default(), a.get("outcome").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("mesh1.rpc.2".to_string(), "released".to_string())], "{releases:?}");
    eprintln!("trace {}: {NAME_SPAN} to=mesh1.rpc.2.old outcome=name-not-applied answered=release-pending; trace {}: rdm.node_admin.name.update.via-pending-release from=mesh1.rpc.2 outcome=released", spans[0].0, releases[0].0);
}

/// CONTRACT (proposal §2.1 "Claim and move": "a row whose `renamed_from` key still holds the same node
/// id has a pending release, and the writer releases it ... It runs wherever a writer meets the row";
/// §2.4: "the release is finished by whichever writer next meets the row"): the left `mesh1.rpc.2`
/// renamed to `2.old` while the nodes store refuses the release ends `release-pending`, both keys
/// held; the next `node.config.update` of the node releases `mesh1.rpc.2` before its put. Renamed back
/// to `2` under a second refused release, `mesh1.rpc.2.old` is held twice over the same way, and the
/// next `node.tags.create` releases it before its put. One
/// `rdm.node_admin.name.update.via-pending-release` span names each release, `moment=config-put`
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
            async move { nodes.name_claim(&name).await.unwrap() }
        };
        rig.refuse_release.store(true, Ordering::SeqCst);
        let pending = reply(&call(&rig, &b["admin2"], &update_name(stopped, "2.old")).await);
        assert!(matches!(&pending, StatusReply::NameNotApplied { answered, .. } if answered == "release-pending"), "{pending:?}");
        assert_eq!(holds(base.clone()).await, Some(stopped.node.node_id.clone()), "the release of {base} is pending");
        let config = StatusRequest::UpdateNodeConfig { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap(), config: [("zone".to_string(), "a".to_string())].into() };
        let configured = reply(&call(&rig, &b["admin2"], &config).await);
        assert_eq!(holds(base.clone()).await, None, "the config put released {base}: {configured:?}");
        assert_eq!(rig.nodes.record_at(&old).await.unwrap().map(|r| r.config), Some([("zone".to_string(), "a".to_string())].into()), "the config put wrote the row at {old}");

        rig.refuse_release.store(true, Ordering::SeqCst);
        let back = reply(&call(&rig, &b["admin2"], &update_name(stopped, "2")).await);
        assert!(matches!(&back, StatusReply::NameNotApplied { answered, .. } if answered == "release-pending"), "{back:?}");
        assert_eq!(holds(old.clone()).await, Some(stopped.node.node_id.clone()), "the release of {old} is pending");
        let tagged = reply(&call(&rig, &b["admin2"], &create_tags(stopped, &["red"])).await);
        assert_eq!(tagged, StatusReply::TagsCreated { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap(), already_carried: Default::default() });
        assert_eq!(holds(old.clone()).await, None, "the tags put released {old}");
        assert_eq!(rig.nodes.record_at(&base).await.unwrap().map(|r| r.tags), Some(labels(&["red"])), "the tags put wrote the row at {base}");
    });
    let releases = cap.spans("rdm.node_admin.name.update.via-pending-release");
    let seen: Vec<(String, String, String)> = releases.iter().map(|(_, a)| (a.get("moment").cloned().unwrap_or_default(), a.get("from").cloned().unwrap_or_default(), a.get("outcome").cloned().unwrap_or_default())).collect();
    assert_eq!(seen, vec![("config-put".to_string(), "mesh1.rpc.2".to_string(), "released".to_string()), ("tags-put".to_string(), "mesh1.rpc.2.old".to_string(), "released".to_string())], "{releases:?}");
    eprintln!("trace {}: rdm.node_admin.name.update.via-pending-release moment=config-put from=mesh1.rpc.2 outcome=released; trace {}: moment=tags-put from=mesh1.rpc.2.old outcome=released", releases[0].0, releases[1].0);
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
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap().map(|r| r.node_id), Some(succ.node.node_id.clone()), "the row moved");
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

/// CONTRACT (Q171: no workflow waits for the initial fabric's first ready; proposal
/// node-properties-own-commands.md §1): while the initial fabric is pending and has never been
/// ready, a caller's `node.name.update` of the left `mesh1.rpc.2` (its connections deleted,
/// its mesh left) at its mesh-primary ends `NameUpdated`: the row moves to `mesh1.rpc.2.old`. What must
/// NOT happen: a refusal for a fabric not yet ready.
#[test]
fn a_node_name_update_before_the_fabric_first_reaches_ready_is_admitted() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let rpc2 = &b["rpc2"];
        let _node = left_with_row(&rig, rpc2, &[]).await;
        rig.topology.write().await.fabric.status = ScopeStatus::Pending;
        let to: PathName = "mesh1.rpc.2.old".parse().unwrap();
        let renamed = reply(&call(&rig, &b["admin2"], &update_name(rpc2, "2.old")).await);
        assert_eq!(renamed, StatusReply::NameUpdated { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap() }, "admitted before the first ready");
        assert_eq!(rig.nodes.record_at(&to).await.unwrap().map(|r| r.node_id), Some(rpc2.node.node_id.clone()));
    });
    let updates = cap.spans(NAME_SPAN);
    let outcomes: Vec<_> = updates.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect();
    assert_eq!(outcomes, vec!["name-updated".to_string()], "{updates:?}");
    eprintln!("trace {}: {NAME_SPAN} outcome=name-updated", updates[0].0);
}

fn create_tags(b: &Birth, l: &[&str]) -> StatusRequest {
    StatusRequest::CreateTags { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), labels: labels(l) }
}

fn delete_tags(b: &Birth, scope: rdm_core::status::TagScope) -> StatusRequest {
    StatusRequest::DeleteTags { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), scope }
}

/// CONTRACT (`docs/architecture/node-rpc-envelope.md:323` — "`node.tags.create { label | [labels] }` adds
/// one label or a list; `node.tags.delete { All | [labels] }` removes every label or the named ones ...
/// Each label is its own keyed row (label, node id) in the node store's tags index. Create is a blind put
/// of that key, so creating a label the node already carries is ignored and writes no duplicate; delete
/// tombstones the named keys. A label is 1-64 characters of a-z 0-9 - _ . ~, with no leading '.', and a
/// node carries at most 32; a label outside that grammar, or one past the 32, is refused by name and
/// nothing is written" and `docs/architecture/node-rpc-envelope.md:365` — "Additive and removal commands
/// on keyed label rows, not a replacement of a tags property"; proposal node-properties-own-commands.md §1: "Each command goes to the node's
/// mesh-primary as the row writer"; Q166, Q171: each is admitted at any time): while the initial fabric is
/// pending and has never been ready, a caller's `node.config.update` {zone: a} of
/// `mesh1.rpc.1` at its mesh-primary ends `ConfigUpdated`, the row carries the map (and no tag) and the
/// birth is sent `ApplyNodeConfig` with it. Then: create {rabbit} ends `TagsCreated` and the tags index
/// answers rabbit; create {rabbit, red} names rabbit already carried; delete {rabbit, blue} names blue
/// not carried and rabbit leaves the index; a create that would carry the node past 32 labels is refused
/// `RejectedTags` naming the count and writes nothing; delete `All` leaves the row with no label and the
/// index with none of its labels. The node is told nothing by any tags command; each tags command's
/// `via-writer` span ends with the reply's name. What must NOT happen: a duplicate label row, a label past
/// the cap, a tags command that calls the node, or a command refused for a fabric not yet ready.
// @feature: node-tags
#[test]
fn node_tags_commands_and_a_config_update_before_the_fabric_first_reaches_ready_write_the_row_and_its_tags_index() {
    use rdm_core::status::TagScope;
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let rpc1 = &b["rpc1"];
        let node = serve_node(rpc1).await;
        configs_door(&rig, &node).await;
        rig.topology.write().await.fabric.status = ScopeStatus::Pending;
        let zone: BTreeMap<String, String> = [("zone".to_string(), "a".to_string())].into();
        let config = StatusRequest::UpdateNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config: zone.clone() };
        assert_eq!(updated(&reply(&call(&rig, &b["admin2"], &config).await)), updated_of(rpc1, &zone), "node.config.update is admitted before the first ready");
        let row = rig.nodes.record_at(&rpc1.node.name).await.unwrap().expect("the config update writes the row");
        assert_eq!((row.config.clone(), row.tags.clone()), (zone.clone(), Default::default()), "the row carries the map and no tag");
        assert!(rig.nodes.tagged("rabbit").await.unwrap().is_empty(), "no tags index entry is written");
        assert_eq!(node.seen.lock().unwrap().drain(..).collect::<Vec<_>>(), vec![StatusRequest::ApplyNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config: zone, revision: row.revision }], "the birth is sent the map");

        let (id, birth) = (rpc1.node.node_id.clone(), rpc1.node.incarnation_id.clone().unwrap());
        let created = |already: &[&str]| StatusReply::TagsCreated { node_id: id.clone(), incarnation: birth.clone(), already_carried: labels(already) };
        let deleted = |not: &[&str]| StatusReply::TagsDeleted { node_id: id.clone(), incarnation: birth.clone(), not_carried: labels(not) };
        let row_tags = || async { rig.nodes.record_at(&rpc1.node.name).await.unwrap().map(|r| r.tags).unwrap_or_default() };
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
    let outcomes = |name: &str| cap.spans(name).iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>();
    assert_eq!(outcomes("rdm.node_admin.tag.create.via-writer"), vec!["tags-created", "tags-created", "rejected-tags"]);
    assert_eq!(outcomes("rdm.node_admin.tag.delete.via-writer"), vec!["tags-deleted", "tags-deleted"]);
    let created = cap.spans("rdm.node_admin.tag.create.via-writer");
    eprintln!("trace {}: rdm.node_admin.tag.create.via-writer outcome=tags-created arrived=rabbit", created[0].0);
}

/// CONTRACT (R-TG1; proposal §3: tags and config: "Neither has a precondition ..., neither touches the
/// name"; node-identity-and-state.md "A rewrite is lossless for every field its
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
        let row = rig.nodes.record_at(&rpc1.node.name).await.unwrap().expect("mesh1's primary wrote rpc.1's row");
        assert_eq!((row.node_id.clone(), row.tags.clone()), (rpc1.node.node_id.clone(), labels(&["rabbit", "red"])), "the row holds the tags the create added");
        assert_eq!(tagged("rabbit").await, [rpc1.node.node_id.clone()].into(), "the tags index names rpc.1 under rabbit");

        let config: BTreeMap<String, String> = [("zone".to_string(), "c".to_string())].into();
        let req = StatusRequest::UpdateNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config: config.clone() };
        assert_eq!(updated(&reply(&call(&rig, &b["admin2"], &req).await)), updated_of(rpc1, &config));
        let row = rig.nodes.record_at(&rpc1.node.name).await.unwrap().expect("rpc.1's row");
        assert_eq!((row.config, row.tags), (config, labels(&["rabbit", "red"])), "the config put writes the new config and keeps the tags");
        assert_eq!(tagged("rabbit").await, [rpc1.node.node_id.clone()].into(), "the index is unchanged by a config put");

        hold_left(&rig, rpc2).await;
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: rpc2.node.name.clone(), node_id: rpc2.node.node_id.clone(), status: NodeStatus::ConnectionsDeleting, config: Default::default(), tags: labels(&["red"]), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
        rig.nodes.write_record(&row).await.unwrap();
        let to: PathName = "mesh1.rpc.2.old".parse().unwrap();
        let renamed = reply(&call(&rig, &b["admin2"], &update_name(rpc2, "2.old")).await);
        assert_eq!(renamed, StatusReply::NameUpdated { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.record_at(&to).await.unwrap().map(|r| r.tags), Some(labels(&["red"])), "the row at the new name keeps the tags");
        assert_eq!(tagged("red").await, [rpc1.node.node_id.clone(), rpc2.node.node_id.clone()].into(), "the index still names the renamed node: it is keyed by node id");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("answered").cloned())).collect::<Vec<_>>(), vec![("name-updated".to_string(), Some("applied".to_string()))], "{spans:?}");
    let creates = cap.spans("rdm.node_admin.tag.create.via-writer");
    assert_eq!(creates.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["tags-created".to_string()], "{creates:?}");
    let configs = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(configs.iter().map(|(_, a)| a.get("tags").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["rabbit,red".to_string()], "the config put wrote the tags it holds: {configs:?}");
    eprintln!("trace {}: rdm.node_admin.tag.create.via-writer arrived=rabbit,red outcome=tags-created (no apply sent)", creates[0].0);
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
        assert_eq!(rig.nodes.record_at(&to).await.unwrap().map(|r| r.node_id), Some(me.node.node_id.clone()), "the row moved to the new name");
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
/// Build"; node-rpc-envelope.md typed objects: `fabrics.get` answers the fabric): `fabrics.get` (op 0x1B
/// `Get { Fabrics }`) at a node-admin answers the fabric's `build_id`, the current Build its own builds store
/// returns, skipping a failed Build, and an admin whose builds store holds no Build in progress or
/// completed answers none. Must NOT happen: a caller that needs the current Build reading it from
/// anywhere but the fabric's own read, or `fabric.get` naming a failed Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fabric_get_answers_the_current_build_its_builds_store_returns_and_skips_a_failed_one() {
    use rdm_admin::accepted::{AcceptedStore, FabricTopology};
    use rdm_admin::build_state::{AttemptOutcome, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildStateAdapter, MemoryBuildStateAdapter};
    let admin = birth("mesh1.admin.1", NodeKind::NodeAdmin, "mesh1", true, true);
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
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
                declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
                stopped: Default::default(),
                me: admin.name.clone(),
                node_id: admin.node_id.clone(),
                fabric_id: view.fabric.id.clone(),
                topology: Arc::new(tokio::sync::RwLock::new(view)),
                declared: Arc::new(Mutex::new(Declared::default())),
                meshes_storage: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
                fabric_storage: Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()),
                nodes_storage: Arc::new(MemoryNodesStorage::default()),
                caches: Default::default(),
                mesh_ids: Arc::new(BTreeMap::new),
                durable: Default::default(),
                commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
                own: Arc::new(std::sync::OnceLock::new()),
                mesh_deleter: Arc::new(std::sync::OnceLock::new()),
                hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                wake: Default::default(),
                fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
                configs: Default::default(),
                republish: Default::default(), configure: Default::default(),
                pre_node_ready: Default::default(),
                rename: Default::default(),
                row_moved: Default::default(),
                loads: Some(Arc::new(BTreeMap::new)),
                digest_view: None,
                builds,
            };
            let read = StatusRequest::Get { entity: rdm_core::filter::Entity::Fabrics, node: None, filter: rdm_core::filter::Filter::new() };
            match authority.apply(None, &read).await {
                StatusReply::Fabrics { mut fabrics } if fabrics.len() == 1 => fabrics.remove(0),
                other => panic!("fabrics.get answered {other:?}"),
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

/// CONTRACT (node-rpc-envelope.md Status: "`RejectedNotPrimary` says the receiver is not the
/// authority for the call: ... `SubjectUnknown` when the receiver does not hold the subject at all.
/// ... a caller-side `PeerUnresolved` likewise [keeps its codec position and a mesh receiver never
/// sends it]"; node-rpc.md §14): mesh1's primary asked to put the config of, put the record of, or
/// delete the connections of a node its view does not hold, or of a node whose birth it does not
/// know, answers `RejectedNotPrimary { SubjectUnknown }` each time: the fact is the receiver's own.
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
            assert_eq!(got, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "{case}: {} answers the receiver's own fact", req.op());
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
        let row = rdm_admin::storage::NodeRow { topology_storage_provider: None, name: pred.node.name.clone(), node_id: pred.node.node_id.clone(), status: NodeStatus::ConnectionsDeleting, config: Default::default(), tags: labels(&["rabbit", "red"]), allocated: None, renamed_from: None, app_types: None, inherited_by: None, executable: None, births: Vec::new(), predecessor: None, mesh_id: None, storage_persistence: None, placement: None, revision: 0, writer: None };
        rig.nodes.write_record(&row).await.unwrap();
        let mut succ = birth("mesh1.rpc.2", NodeKind::RpcNode, "mesh1", false, false);
        succ.node.status = NodeStatus::Pending;
        let (pred_node, succ_node) = (serve_node(pred).await, serve_node(&succ).await);
        configs_door_for(&rig, &[&pred_node, &succ_node]).await;
        let pred_cfg: BTreeMap<String, String> = [("zone".to_string(), "a".to_string())].into();
        assert_eq!(
            updated(&reply(&call(&rig, &b["admin2"], &StatusRequest::UpdateNodeConfig { node_id: pred.node.node_id.clone(), incarnation: pred.node.incarnation_id.clone().unwrap(), config: pred_cfg.clone() }).await)),
            updated_of(pred, &pred_cfg)
        );
        // Step 2: the predecessor, off its mesh, takes `.old`.
        let (base_name, old_name): (PathName, PathName) = ("mesh1.rpc.2".parse().unwrap(), "mesh1.rpc.2.old".parse().unwrap());
        assert_eq!(rig.authority.update_name(&update_name(pred, "2.old")).await, StatusReply::NameUpdated { node_id: pred.node.node_id.clone(), incarnation: pred.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.tagged("rabbit").await.unwrap(), [pred.node.node_id.clone()].into());
        assert_eq!(rig.nodes.tagged("red").await.unwrap(), [pred.node.node_id.clone()].into());

        // Step 3: the successor's claim at the name the predecessor released.
        rig.topology.write().await.nodes.push(succ.node.clone());
        let rows = NodeRows::new(rig.admin.node.name.clone(), rig.admin.node.node_id.clone(), rig.topology.clone(), rig.nodes.clone()).with_written(rig.authority.caches.nodes.clone());
        let claim = || rows.claim(&base_name, &succ.node.node_id, "node.create:mesh1.rpc.2", None, None, ClaimFor::Successor);
        assert!(matches!(claim().await, Ok(rdm_admin::node_rows::Claimed::Written { .. })), "the successor claims the released name");
        let succ_row = rig.nodes.record_at(&base_name).await.unwrap().expect("successor row at mesh1.rpc.2");
        assert_eq!((succ_row.node_id.clone(), succ_row.config.clone(), succ_row.tags.clone()), (succ.node.node_id.clone(), pred_cfg.clone(), labels(&["rabbit", "red"])));
        let retained = rig.nodes.record_at(&old_name).await.unwrap().expect("retained predecessor row at mesh1.rpc.2.old");
        assert_eq!((retained.node_id.clone(), retained.tags.clone(), retained.inherited_by.clone()), (pred.node.node_id.clone(), labels(&["rabbit", "red"]), Some(succ.node.node_id.clone())));
        assert_eq!(rig.nodes.tagged("rabbit").await.unwrap(), [succ.node.node_id.clone()].into(), "only the successor's id is in the active index after its claim");
        assert_eq!(rig.nodes.tagged("red").await.unwrap(), [succ.node.node_id.clone()].into());

        // Later updates on the successor, then a repeated claim (the create's resume).
        let succ_cfg: BTreeMap<String, String> = [("zone".to_string(), "b".to_string()), ("tier".to_string(), "gold".to_string())].into();
        assert_eq!(
            updated(&reply(&call(&rig, &b["admin2"], &StatusRequest::UpdateNodeConfig { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap(), config: succ_cfg.clone() }).await)),
            updated_of(&succ, &succ_cfg)
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
        let row_after = rig.nodes.record_at(&base_name).await.unwrap().unwrap();
        assert_eq!((row_after.config, row_after.tags), (succ_cfg, labels(&["rabbit", "blue"])), "a repeated claim keeps the successor's later updates");
        assert!(rig.nodes.tagged("red").await.unwrap().is_empty(), "the deleted tag red is not resurrected from .old");

        // Rollback: the successor deleted, the predecessor renamed back to `mesh1.rpc.2`.
        rows.deleted(&succ.node, "node.delete:mesh1.rpc.2").await.expect("the successor's Deleted row at mesh1.rpc.2");
        assert_eq!(
            rig.authority.update_name(&update_name(pred, "2")).await,
            StatusReply::NameUpdated { node_id: pred.node.node_id.clone(), incarnation: pred.node.incarnation_id.clone().unwrap() }
        );
        let restored = rig.nodes.record_at(&base_name).await.unwrap().expect("predecessor row restored at mesh1.rpc.2");
        assert_eq!(
            (restored.node_id.clone(), restored.config, restored.tags, restored.inherited_by),
            (pred.node.node_id.clone(), pred_cfg, labels(&["rabbit", "red"]), None),
            "rollback restores the predecessor row at its original name with inherited_by cleared"
        );
        assert_eq!(rig.nodes.record_at(&old_name).await.unwrap(), None, "mesh1.rpc.2.old is released");
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
    let carried = cap.spans("rdm.node_admin.connection.delete.via-mesh-primary");
    assert_eq!(carried.len(), 1, "{carried:?}");
    let (trace_id, attrs) = &carried[0];
    let attr = |k: &str| attrs.get(k).map(String::as_str);
    assert_eq!((attr("node"), attr("subject"), attr("scope"), attr("carried"), attr("outcome")), (Some("mesh1.admin.1"), Some("mesh1.rpc.1"), Some("connections"), Some("dialled"), Some("connections-deleted")), "{attrs:?}");
    assert!(cap.spans("rdm.node_admin.node.update.via-connections-deleting").is_empty(), "a list of ids records no connections-deleting");
    let routes = cap.spans("rdm.node_rpc.route.resolve.via-connections");
    let leg: Vec<_> = routes.iter().filter(|(t, a)| t == trace_id && a.get("route").map(String::as_str) == Some("via-peer")).collect();
    assert_eq!(leg.len(), 1, "the caller's one carried leg is in the primary's carrying trace: {routes:?}");
    assert_eq!((leg[0].1.get("carrier").map(String::as_str), leg[0].1.get("protocol").map(String::as_str)), (Some("mesh2.admin.1"), Some("status")), "{leg:?}");
    eprintln!("trace {trace_id}: rdm.node_rpc.route.resolve.via-connections route=via-peer carrier=mesh2.admin.1 > rdm.node_admin.connection.delete.via-mesh-primary node=mesh1.admin.1 carried=dialled outcome=connections-deleted");
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
        let topology_view = rdm_transport::topology::TopologyView::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), "mesh1.admin.1", Arc::new(time), vec![], &[]).await.unwrap();
        topology_view.set_mesh_primary(true);
        let (rig, b) = rig_with_digest_view(Some(topology_view.digest_view.clone())).await;
        let rpc = &b["rpc2"];
        assert!(topology_view.digest_view.record(rdm_core::MeshDigest { held_hash: Default::default(),
            fabric_id: fabric,
            node: rdm_core::MeshNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: rpc.node.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Pending, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: None, in_flight: None, load: None, gossip: None, is_primary: None, is_fabric_primary: None
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
        }).build(ServedBirth { node_id: rpc.node.node_id.to_string(), incarnation: rpc.node.incarnation_id.as_ref().unwrap().to_string() }).unwrap();
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
    assert_eq!(records[0].1.get("recorded").map(String::as_str), Some("true"), "the progress changed the actual view: {records:?}");
    assert_eq!(cap.spans("rdm.mesh.topology.update.from-status-report").len(), 1, "one status write, not just one carrier span");
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
        assert!(rdm_admin::status_rpc::Declaration::of_records(&rig.storage.nodes("mesh1").await.unwrap()).is_empty());
        assert!(rdm_admin::status_rpc::OperationEffect::of_records(&rig.storage.nodes("mesh1").await.unwrap()).is_empty());
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
        assert!(rdm_admin::status_rpc::Declaration::of_records(&rig.storage.nodes("mesh1").await.unwrap()).is_empty());
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
                if fail { return Ok(N::Failed { step: "pre_node_ready".into(), reason: "app refused".into(), code: rdm_core::outcome::Code::FailedPrecondition }); }
                sink.data(if start { N::NodeJoined } else { N::StopConnectionsDeleted { receipt: rdm_core::status::ConnectionsReceipt::Established { in_flight: 0, closed: vec![] } } }).await.unwrap();
                c.notify_one();
                d.notified().await;
                Ok(if start { N::NodeStarted } else { N::NodeStopped })
            }
        }).build(ServedBirth { node_id: subject.node_id.to_string(), incarnation: subject.incarnation_id.as_ref().unwrap().to_string() }).unwrap();
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
        assert!(rdm_admin::status_rpc::Declaration::of_records(&rig.storage.nodes("mesh1").await.unwrap()).is_empty(), "no state before CommandAccepted");
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
            assert!(matches!(result, A::StartFailed { step, reason, code: rdm_core::outcome::Code::FailedPrecondition } if step == "pre_node_ready" && reason == "app refused"));
            assert!(rdm_admin::status_rpc::OperationEffect::of_records(&rig.storage.nodes("mesh1").await.unwrap()).is_empty());
        } else {
            assert!(matches!((&result, start), (A::Started {}, true) | (A::Stopped { .. }, false)), "{result:?}");
            let state = if start { NodeState::Ready } else { NodeState::Stopped };
            assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node_id, subject.incarnation_id.as_ref().unwrap()), Some(state), "recorded before the outer Build can complete");
            assert_eq!(rdm_admin::status_rpc::OperationEffect::of_records(&rig.storage.nodes("mesh1").await.unwrap()).len(), 1);
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
        let topology_view = rdm_transport::topology::TopologyView::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), "mesh1.admin.1", Arc::new(time), vec![], &[]).await.unwrap();
        topology_view.set_mesh_primary(true);
        let (rig, b) = rig_with_digest_view(Some(topology_view.digest_view.clone())).await;
        let rpc = &b["rpc2"];
        assert!(topology_view.digest_view.record(rdm_core::MeshDigest { held_hash: Default::default(),
            fabric_id: fabric,
            node: rdm_core::MeshNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: rpc.node.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Pending, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: None, in_flight: None, load: None, gossip: None, is_primary: None, is_fabric_primary: None
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
        }).build(ServedBirth { node_id: rpc.node.node_id.to_string(), incarnation: rpc.node.incarnation_id.as_ref().unwrap().to_string() }).unwrap();
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
        assert_eq!(rdm_admin::status_rpc::OperationEffect::of_records(&rig.storage.nodes("mesh1").await.unwrap()).len(), 1);
        assert!(rdm_transport::DeliveryStats::get(&server_delivery.get().unwrap().stats().responses_resent) >= 1);
    });
    let records: Vec<_> = cap.spans("rdm.node_admin.node.update.via-command-frame").into_iter().filter(|(_, a)| a.get("frame").map(String::as_str) == Some("node-joined")).collect();
    assert_eq!(records.len(), 1, "the mesh-primary records the repeated progress once: {records:?}");
    let applied: Vec<_> = cap.spans("rdm.mesh.topology.update.from-status-report").into_iter().filter(|(_, a)| a.get("status").map(String::as_str) == Some("Pending")).collect();
    assert_eq!(applied.len(), 1, "one Pending status write, not just one carrier span: {applied:?}");
    assert!(!records[0].1.get("invocation_id").unwrap_or(&String::new()).is_empty(), "the record names the invocation: {records:?}");
    assert_eq!(records[0].1.get("seq").map(String::as_str), Some("1"), "the progress position: {records:?}");
}

/// A start after a stop of mesh1.rpc.2 at its mesh-primary, carried on a real command stream: the
/// birth's stop is recorded (its Stopped row, the view's stop), the start's join report is accepted
/// and its Pending recorded there, before the join is answered (node-mesh-join.md step 10). With
/// `ready_first`, the node's own Ready declaration is decided before its mesh-primary takes
/// the start's `NodeJoined` frame; otherwise after it. What the mesh-primary holds of the birth once
/// it took `NodeJoined` is checked by `joined`, before `NodeStarted` is sent; the spans the run
/// exported are returned.
fn start_after_a_stop_with_the_pending_recorded_at_the_join(ready_first: bool, joined: impl Fn(&Rig, &Birth, &rdm_transport::topology::DigestView) + Send + Sync + 'static) -> Capture {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply as N, NodeCommandRequest};
    let cap = capture();
    cap.run(async move {
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _gossip_router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let fabric = FabricId::mint();
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(5_000_000);
        let topology_view = rdm_transport::topology::TopologyView::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), "mesh1.admin.1", Arc::new(time), vec![], &[]).await.unwrap();
        topology_view.set_mesh_primary(true);
        let digest_view = topology_view.digest_view.clone();
        let (rig, b) = rig_with_digest_view(Some(digest_view.clone())).await;
        let rpc = &b["rpc2"];
        let incarnation = rpc.node.incarnation_id.clone().unwrap();
        assert!(digest_view.record(rdm_core::MeshDigest { held_hash: Default::default(),
            fabric_id: fabric,
            node: rdm_core::MeshNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: incarnation.clone(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Pending, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: None, in_flight: None, load: None, gossip: None, is_primary: None, is_fabric_primary: None
        }));
        // The birth's stop: its Stopped row, and the view's stop of it.
        let stop = rdm_core::LifecycleOp { node_id: rpc.node.node_id.clone(), incarnation: incarnation.clone(), name: rpc.node.name.clone(), build_id: "b-stop".into(), attempt: 1, operation: format!("node.stop:{}", rpc.node.name), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        assert_eq!(rig.authority.stop_received(&stop).await, StatusReply::Applied);
        assert!(digest_view.stopped(stop), "the view holds the birth's stop");
        // The start's join report, accepted at the door: the birth's next cycle begins and its Pending
        // is recorded there, before the join is answered.
        assert_eq!(rig.authority.joined_pending(&rpc.node.node_id, &rpc.node.name, &incarnation, true).await, StatusReply::Applied, "the join report's Pending is recorded");
        assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc.node.node_id, &incarnation), Some(NodeState::Pending));
        // A later report of that join (a retry of the same report) belongs to that Pending and starts no other.
        assert_eq!(rig.authority.joined_pending(&rpc.node.node_id, &rpc.node.name, &incarnation, false).await, StatusReply::AlreadyApplied, "a retried join report applies as already applied");
        if ready_first {
            // Answered, the node runs its start and declares Ready before its NodeJoined frame is taken.
            assert_eq!(reply(&call(&rig, rpc, &declare(rpc, NodeState::Ready)).await), StatusReply::Applied, "the node's Ready, declared after its join was answered, applies over the join's Pending");
        }
        let server_delivery = Arc::new(OnceLock::<rdm_transport::Delivery>::new());
        let (taken, release) = (Arc::new(tokio::sync::Notify::new()), Arc::new(tokio::sync::Notify::new()));
        let (sd, t, r) = (server_delivery.clone(), taken.clone(), release.clone());
        let server = ServerBuilder::new().serve_stream::<NodeCommand, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_, req: NodeCommandRequest, sink| {
            let (sd, t, r) = (sd.clone(), t.clone(), r.clone());
            async move {
                let NodeCommandRequest::StartNode { node_id, incarnation, .. } = req else { panic!("expected StartNode") };
                let mut sink = sink.started(N::CommandAccepted { node_id, incarnation }).await.unwrap();
                sink.data(N::NodeJoined).await.unwrap();
                // The mesh-primary acknowledges a frame only once it recorded it: NodeJoined is taken.
                tokio::time::timeout(Duration::from_secs(5), async {
                    while rdm_transport::DeliveryStats::get(&sd.get().unwrap().stats().acks_received) < 2 { tokio::task::yield_now().await; }
                }).await.expect("CommandAccepted and NodeJoined are recorded and acknowledged");
                t.notify_one();
                r.notified().await;
                Ok(N::NodeStarted)
            }
        }).build(ServedBirth { node_id: rpc.node.node_id.to_string(), incarnation: incarnation.to_string() }).unwrap();
        server_delivery.set(server.delivery().clone()).unwrap_or_else(|_| panic!("one responder"));
        let node_ep = rdm_transport::endpoint::bind(rpc.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = node_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _router = Router::builder(node_ep).accept(rdm_transport::ALPN, server).spawn();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: rpc.key.public(), transport_addr: addr, incarnation: incarnation.clone() });
        let admin_ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(admin_ep, resolver).with_caller_system("rdm"));
        assert!(rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(client, rig.authority.configure.clone(), Default::default()))).is_ok());
        let context = rdm_admin::deployment::pipeline::CommandContext { build_id: "b-start".into(), attempt: 1, operation: format!("node.start:{}", rpc.node.name) };
        let (authority, target) = (rig.authority.clone(), rpc.node.clone());
        let start = tokio::spawn(async move { authority.carry_lifecycle(&target, true, &context).await });
        tokio::time::timeout(Duration::from_secs(10), taken.notified()).await.expect("the mesh-primary took the start's NodeJoined");
        joined(&rig, rpc, &digest_view);
        if !ready_first {
            assert_eq!(reply(&call(&rig, rpc, &declare(rpc, NodeState::Ready)).await), StatusReply::Applied, "the node's Ready applies over the join's Pending");
        }
        release.notify_one();
        let answer = tokio::time::timeout(Duration::from_secs(10), start).await.unwrap().unwrap();
        assert!(matches!(answer, rdm_admin::deployment::pipeline::CommandAdmission::Started {}), "{answer:?}");
        assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc.node.node_id, &incarnation), Some(NodeState::Ready));
        assert_eq!(digest_view.member_status(&rpc.node.node_id, &incarnation), Some(rdm_core::MemberStatus::Ready));
    });
    cap
}

/// What the mesh-primary holds of mesh1.rpc.2's birth once it took the start's `NodeJoined`: the
/// declared `state`, the member status `status`, its stop over and the birth held again.
fn held_after_node_joined(rig: &Rig, rpc: &Birth, digest_view: &rdm_transport::topology::DigestView, state: NodeState, status: rdm_core::MemberStatus) {
    let incarnation = rpc.node.incarnation_id.clone().unwrap();
    assert_eq!(rig.authority.declared.lock().unwrap().node(&rpc.node.node_id, &incarnation), Some(state), "the start's NodeJoined belongs to the Pending its join report recorded: it applies as already applied and the birth stays declared {state:?}");
    assert_eq!(digest_view.member_status(&rpc.node.node_id, &incarnation), Some(status), "the view holds the birth {status:?}: NodeJoined sets nothing back");
    assert!(digest_view.get(rpc.node.node_id.as_str()).is_some_and(|(d, _)| d.node.incarnation == incarnation), "the birth is held again from node.mesh.joined");
    assert!(!digest_view.in_flight().into_iter().any(|op| op.node_id == rpc.node.node_id && op.is_stop()), "its stop is cleared at node.mesh.joined");
}

/// The spans of `name` the cell's run exported for mesh1.rpc.2.
fn of_rpc2(cap: &Capture, name: &str) -> Vec<BTreeMap<String, String>> {
    cap.spans(name).into_iter().map(|(_, a)| a).filter(|a| a.get("node").map(String::as_str) == Some("mesh1.rpc.2") || a.get("member").map(String::as_str) == Some("mesh1.rpc.2")).collect()
}

/// CONTRACT (node-mesh-join.md step 10: the mesh-primary "records and publishes the node once as
/// Pending when it accepts the node's join report, before it answers it. Every later report of that
/// join, its retries and its `NodeJoined` included, belongs to that Pending and starts no other"): a
/// start after a stop whose node declared Ready, once answered, before its mesh-primary
/// took its `NodeJoined` frame stays Ready through that frame: declared Ready,
/// its member status Ready, its stop cleared at node.mesh.joined and the birth held again;
/// the birth's cycle begun once, at its join report. Must NOT happen: `NodeJoined` beginning another
/// cycle and recording Pending again, or the view setting the ready birth back to Pending.
#[test]
fn a_starts_node_joined_taken_after_the_births_ready_declaration_leaves_it_ready() {
    let cap = start_after_a_stop_with_the_pending_recorded_at_the_join(true, |rig, rpc, digest_view| held_after_node_joined(rig, rpc, digest_view, NodeState::Ready, rdm_core::MemberStatus::Ready));
    let begun = of_rpc2(&cap, "rdm.node_admin.status.update.via-cycle-begun");
    assert_eq!(begun.len(), 1, "the birth's cycle begins once, at its join report: {begun:?}");
    let admitted = of_rpc2(&cap, "rdm.node_admin.status.update.via-mesh-join-admitted");
    assert_eq!(admitted.iter().map(|a| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["applied".to_string(), "already-applied".to_string()], "the join report's Pending is recorded once; its retry applies as already applied: {admitted:?}");
    let joined: Vec<_> = of_rpc2(&cap, "rdm.mesh.topology.update.via-node-joined");
    assert_eq!(joined.iter().map(|a| (a.get("outcome").cloned().unwrap_or_default(), a.get("status").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("already-applied".to_string(), "Ready".to_string())], "the view took node.mesh.joined as already applied, holding the ready birth ready: {joined:?}");
    let frames: Vec<_> = of_rpc2(&cap, "rdm.node_admin.node.update.via-command-frame").into_iter().filter(|a| a.get("frame").map(String::as_str) == Some("node-joined")).collect();
    assert_eq!(frames.iter().map(|a| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["already-applied".to_string()], "{frames:?}");
}

/// CONTRACT (node-mesh-join.md step 10, as above): a start after a stop whose mesh-primary takes its
/// `NodeJoined` frame while the birth is still Pending from its join report records nothing again:
/// declared Pending, its member status Pending, its stop cleared at node.mesh.joined and the birth
/// held again; the birth's cycle begun once, at its join report; its Ready then applies.
/// Must NOT happen: `NodeJoined` beginning a second cycle and putting a second Pending row.
#[test]
fn a_starts_node_joined_taken_while_the_birth_is_pending_from_its_join_report_begins_no_second_cycle() {
    let cap = start_after_a_stop_with_the_pending_recorded_at_the_join(false, |rig, rpc, digest_view| held_after_node_joined(rig, rpc, digest_view, NodeState::Pending, rdm_core::MemberStatus::Pending));
    let begun = of_rpc2(&cap, "rdm.node_admin.status.update.via-cycle-begun");
    assert_eq!(begun.len(), 1, "the birth's cycle begins once, at its join report: {begun:?}");
    let joined: Vec<_> = of_rpc2(&cap, "rdm.mesh.topology.update.via-node-joined");
    assert_eq!(joined.iter().map(|a| (a.get("outcome").cloned().unwrap_or_default(), a.get("status").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("already-applied".to_string(), "Pending".to_string())], "the view took node.mesh.joined as already applied: {joined:?}");
    let frames: Vec<_> = of_rpc2(&cap, "rdm.node_admin.node.update.via-command-frame").into_iter().filter(|a| a.get("frame").map(String::as_str) == Some("node-joined")).collect();
    assert_eq!(frames.iter().map(|a| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["already-applied".to_string()], "{frames:?}");
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
    }).build(ServedBirth { node_id: subject.node.node_id.to_string(), incarnation: subject.node.incarnation_id.as_ref().unwrap().to_string() }).unwrap();
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
    assert_eq!(rdm_admin::status_rpc::OperationEffect::of_records(&rig.storage.nodes("mesh1").await.unwrap()).len(), 1);
    assert_eq!(rig.authority.declared.lock().unwrap().node(&subject.node.node_id, subject.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Stopped));
}

/// A start of mesh1.rpc.2 carried by its mesh-primary on a real command stream, answered the way a
/// birth answers a start an earlier attempt of the same Build completed (`AlreadyApplied`: the
/// acknowledgement, then `NodeStarted`, no `NodeJoined`; node_self.rs `Cycle::Live`). The view holds a
/// stop of the node's birth `marked` (the command's own birth, or another). Answers whether the view
/// still holds that birth stopped once the start is carried.
fn replayed_start_leaves_stopped(marked_other_birth: bool) -> bool {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply as N, NodeCommandRequest};
    let cap = capture();
    cap.run(async move {
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _gossip_router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let fabric = FabricId::mint();
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(5_000_000);
        let topology_view = rdm_transport::topology::TopologyView::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), "mesh1.admin.1", Arc::new(time), vec![], &[]).await.unwrap();
        topology_view.set_mesh_primary(true);
        let digest_view = topology_view.digest_view.clone();
        let (rig, b) = rig_with_digest_view(Some(digest_view.clone())).await;
        let rpc = &b["rpc2"];
        let incarnation = rpc.node.incarnation_id.clone().unwrap();
        let marked = if marked_other_birth { IncarnationId::mint() } else { incarnation.clone() };
        assert!(digest_view.record(rdm_core::MeshDigest { held_hash: Default::default(),
            fabric_id: fabric,
            node: rdm_core::MeshNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: marked.clone(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Ready, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: None, in_flight: None, load: None, gossip: None, is_primary: None, is_fabric_primary: None
        }));
        let stop = rdm_core::LifecycleOp { node_id: rpc.node.node_id.clone(), incarnation: marked.clone(), name: rpc.node.name.clone(), build_id: "b-stop".into(), attempt: 1, operation: format!("node.stop:{}", rpc.node.name), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        assert!(digest_view.stopped(stop), "the view holds the stop of the marked birth");
        assert!(digest_view.holds_stopped(rpc.node.node_id.as_str(), &marked));
        let server = ServerBuilder::new().serve_stream::<NodeCommand, _, _>(rdm_core::catalog::OpOwner::Product("rdm".into()), move |_, req: NodeCommandRequest, sink| async move {
            let NodeCommandRequest::StartNode { node_id, incarnation, .. } = req else { panic!("expected StartNode") };
            let _ = sink.started(N::CommandAccepted { node_id, incarnation }).await.unwrap();
            Ok(N::NodeStarted)
        }).build(ServedBirth { node_id: rpc.node.node_id.to_string(), incarnation: incarnation.to_string() }).unwrap();
        let node_ep = rdm_transport::endpoint::bind(rpc.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = node_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _router = Router::builder(node_ep).accept(rdm_transport::ALPN, server).spawn();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { node_id: rpc.node.node_id.clone(), name: rpc.node.name.clone(), endpoint_id: rpc.key.public(), transport_addr: addr, incarnation: incarnation.clone() });
        let admin_ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(admin_ep, resolver).with_caller_system("rdm"));
        assert!(rig.authority.configs.set(Arc::new(rdm_admin::node_config::NodeConfigs::new(client, rig.authority.configure.clone(), Default::default()))).is_ok());
        let context = rdm_admin::deployment::pipeline::CommandContext { build_id: "b".into(), attempt: 2, operation: format!("node.start:{}", rpc.node.name) };
        let answer = rig.authority.carry_lifecycle(&rpc.node, true, &context).await;
        assert!(matches!(answer, rdm_admin::deployment::pipeline::CommandAdmission::Started {}), "{answer:?}");
        digest_view.holds_stopped(rpc.node.node_id.as_str(), &marked)
    })
}

/// CONTRACT (node-start.md step 2, Q249): a start an earlier attempt completed, reissued by the next
/// attempt's executor, is answered without a `NodeJoined`; the mesh-primary whose overlay holds that
/// exact birth stopped clears the mark at that answer, as at the join it did not see. Must NOT happen:
/// the birth held stopped after its start completed, every view refusing its digests.
#[test]
fn a_replayed_start_answered_already_applied_clears_its_births_stop_mark() {
    assert!(!replayed_start_leaves_stopped(false), "the replayed start's answer clears the birth's stop mark");
}

/// CONTRACT (Q249): the clear is the exact birth's: a stop mark of another birth of the node is left
/// as it is by a start answered for this one.
#[test]
fn a_replayed_start_clears_no_other_births_stop_mark() {
    assert!(replayed_start_leaves_stopped(true), "another birth's stop mark is left");
}
