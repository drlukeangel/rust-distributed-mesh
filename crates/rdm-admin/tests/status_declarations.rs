//! i143.e6.s7 functional: a node-admin applies lifecycle declarations as an authority.
//!
//! The admin's `Status` service runs in-process over a view fixture (one fabric, two meshes, this
//! admin the primary of mesh1 and the fabric-primary); nodes declare over real Node RPC on
//! loopback. The cells prove the decision table of `status_rpc`: which role decides which
//! subject's declaration, whoever calls (node-rpc.md §14), the natural-key idempotency, forward-only transitions, stale births, and that `Applied` is on the
//! subject's declared-state row in `status.storage` before it is answered. Certainty: a cut before the request
//! commits is `NotSent` with nothing applied; a reply lost after the apply is `Indeterminate`,
//! and the same declaration again is `AlreadyApplied` — one logical event.

use rdm_admin::node_rows::RecordAt as _;
use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::{EndpointId, FabricId, IncarnationId, MeshId, NodeId};
use rdm_admin::model::{Fabric, Node, NodeStatus, PathName, ProviderKind, ScopeStatus};
use rdm_admin::status_rpc::{Declared, StatusService};
use rdm_admin::node_config::SentBy;
use rdm_admin::storage::{MemoryNodesStorage, MeshStorage, NodesStorage};
use rdm_admin::topology::Topology;
use rdm_transport::{CallOptions, NodeRpcClient, NodeTarget, ReplyWithhold, ResolvedNode, ServedBirth, ServerBuilder, ServerStats, StaticResolver};
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

fn birth(name: rdm_core::PathName, r#type: &str, mesh: &str, is_primary: bool, is_fabric_primary: bool) -> Birth {
    let key = SecretKey::generate();
    let mut node = Node::allocated(name, mesh, r#type);
    node.r#type = r#type.to_string();
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
    service: Arc<StatusService>,
    storage: Arc<MemoryNodesStorage>,
    /// The nodes store: the node rows the fabric-primary puts for a config update.
    nodes: Arc<MemoryNodesStorage>,
    topology: Arc<tokio::sync::RwLock<Topology>>,
    mesh_ids: BTreeMap<String, MeshId>,
    /// Inert until a cell arms it.
    withhold: Arc<ReplyWithhold>,
    /// The nodes store the rig's `NodeConfigs` and served nodes write their own parts to
    /// (`crate::bound_configs_on`, [`node_records`]).
    records: Arc<MemoryNodesStorage>,
    /// Each renamed live birth the mesh-primary commanded back on its mesh (node-name-update.md step
    /// 6), by node id and the name it rejoined at; answered `MeshJoined`.
    rejoins: Arc<Mutex<Vec<(NodeId, PathName)>>>,
}

/// The authority's nodes store: the rig's memory store.
struct Rows {
    inner: Arc<MemoryNodesStorage>,
}

#[async_trait::async_trait]
impl NodesStorage for Rows {
    async fn node(&self, node_id: &rdm_admin::model::NodeId) -> Result<Option<rdm_admin::storage::NodeLine>, rdm_admin::storage_error::StorageError> {
        self.inner.node(node_id).await
    }
    async fn nodes(&self, mesh: &str) -> Result<Vec<rdm_admin::storage::NodeRecord>, rdm_admin::storage_error::StorageError> {
        self.inner.nodes(mesh).await
    }
    async fn put_node(&self, row: &rdm_admin::storage::NodeRecord) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.put_node(row).await
    }
    async fn records_at(&self, name: &rdm_admin::model::PathName) -> Result<Vec<rdm_admin::storage::NodeRecord>, rdm_admin::storage_error::StorageError> {
        self.inner.records_at(name).await
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

async fn rig_with_digest_view(digest_view: Option<rdm_gossip::digest_view::DigestView>) -> (Rig, BTreeMap<&'static str, Birth>) {
    let admin = birth(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "node-admin", "mesh1", true, true);
    let mut others = BTreeMap::new();
    others.insert("admin2", birth(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "node-admin", "mesh1", false, false));
    others.insert("rpc1", birth(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "node", "mesh1", true, false));
    others.insert("rpc2", birth(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "node", "mesh1", false, false));
    others.insert("m2admin", birth(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "node-admin", "mesh2", true, false));
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
    let (m1id, m2id) = (MeshId::mint(), MeshId::mint());
    let mesh_ids: BTreeMap<String, MeshId> = [("mesh1".to_string(), m1id.clone()), ("mesh2".to_string(), m2id.clone())].into_iter().collect();
    let rig_mesh_ids = mesh_ids.clone();
    let service = Arc::new(StatusService {
        certs: None,
        topology_view: None,
        declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
        stopped: Default::default(),
        me: admin.node.location(),
        node_id: admin.node.node_id.clone(),
        fabric_id,
        topology: topology.clone(),
        declared: Arc::new(Mutex::new(Declared::default())),
        meshes_storage: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()),
        fabric_storage: Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()),
        nodes_storage: Arc::new(Rows { inner: nodes.clone() }),
        caches: Default::default(),
        mesh_ids: Arc::new(move || mesh_ids.clone()),
        durable: Default::default(),
        commands: Arc::new(rdm_admin::node_commands::CommandView::default()),
        own: Arc::new(std::sync::OnceLock::new()),
        mesh_deleter: Arc::new(std::sync::OnceLock::new()),
        primary_commands: Default::default(),
        storage_changes: Default::default(), hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
        configs: Default::default(),
        republish: Default::default(),
        pre_node_ready: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(), rejoin: Default::default(),
        // node.get answers beside the view: no node's load is held here.
        loads: Some(Arc::new(BTreeMap::new)),
        digest_view,
        unreachable: Default::default(),
        builds: std::sync::Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
    });
    // The rig's view stands for the admin's projection: a moved row moves the birth's entry.
    let moved_view = topology.clone();
    let _ = service.row_moved.set(Arc::new(move |_from: PathName, node_id: NodeId, _incarnation: IncarnationId, to: PathName| {
        let view = moved_view.clone();
        Box::pin(async move {
            if let Some(n) = view.write().await.nodes.iter_mut().find(|n| n.node_id == node_id) {
                { let at = to; n.path = at.path; n.name = at.name; }
            }
        })
    }));
    // The rig's rejoin: a renamed live birth's node.mesh.join, recorded and answered `MeshJoined`.
    let rejoins: Arc<Mutex<Vec<(NodeId, PathName)>>> = Arc::default();
    let r = rejoins.clone();
    let _ = service.rejoin.set(Arc::new(move |node: rdm_admin::model::Node, incarnation: IncarnationId| {
        let r = r.clone();
        Box::pin(async move {
            r.lock().unwrap().push((node.node_id.clone(), node.location()));
            Ok(StatusReply::MeshJoined { node_id: node.node_id, incarnation, joined: vec![rdm_core::status::WhichMesh::Own] })
        })
    }));
    let slot: Arc<OnceLock<Arc<StatusService>>> = Arc::new(OnceLock::new());
    let _ = slot.set(service.clone());
    let withhold = Arc::new(ReplyWithhold::default());
    let server = rdm_admin::status_rpc::serve(ServerBuilder::new().withhold_replies(withhold.clone()), slot)
        .build(ServedBirth { node_id: admin.node.node_id.to_string(), incarnation: admin.node.incarnation_id.clone().unwrap().to_string() })
        .unwrap();
    let ep = rdm_transport::endpoint::bind(admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep.clone()).accept(rdm_transport::ALPN, server.clone()).spawn();
    let resolved = ResolvedNode { mesh: admin.node.mesh.clone(), r#type: admin.node.r#type.clone(),
        node_id: admin.node.node_id.clone(),
        path: admin.node.path.clone(), name: admin.node.name.clone(),
        endpoint_id: admin.key.public(),
        transport_addr: addr,
        incarnation: admin.node.incarnation_id.clone().unwrap(),
    };
    // Every birth's own record, as it writes it at its start before a command reaches it (Q356).
    for b in std::iter::once(&admin).chain(others.values()) {
        rdm_admin::storage::NodesStorage::put_node(&*nodes, &own_record(b, b.node.status, &[])).await.unwrap();
    }
    let records = node_records();
    (Rig { admin, endpoint: ep, server, _router: router, resolved, service, storage, nodes, topology, mesh_ids: rig_mesh_ids, withhold, records, rejoins }, others)
}

/// The node property records every birth of these cells writes its own record to and every admin
/// service of them reads: one store for the process (each birth's node id is minted, so no two cells
/// share a record).
fn node_records() -> Arc<MemoryNodesStorage> {
    static RECORDS: OnceLock<Arc<MemoryNodesStorage>> = OnceLock::new();
    RECORDS.get_or_init(Default::default).clone()
}

/// The config and tags the node record of `node_id` holds (empty when none is written): the rig
/// admin's own in the rig's nodes store, where its own writer puts it; a served birth's in the rig's
/// records, where its own writer puts it.
async fn props(rig: &Rig, node_id: &NodeId) -> (BTreeMap<String, String>, std::collections::BTreeSet<String>) {
    use rdm_admin::storage::NodesStorage as _;
    let store = if *node_id == rig.admin.node.node_id { &rig.nodes } else { &rig.records };
    store.node(node_id).await.unwrap().map(|l| (l.record.config, l.record.tags)).unwrap_or_default()
}

/// The record `b` writes of itself in `state` carrying `tags` (Q356).
fn own_record(b: &Birth, state: NodeStatus, tags: &[&str]) -> rdm_admin::storage::NodeRecord {
    let mut r = rdm_node::nodes_storage::fixture_record(&b.node.location(), &b.node.r#type, b.node.node_id.clone(), b.node.incarnation_id.clone().unwrap(), state);
    r.tags = tags.iter().map(|t| t.to_string()).collect();
    r
}

/// The config the node property record of `node_id` holds in [`node_records`] (empty when none is
/// written): what a service other than the rig's wrote.
async fn config_of(node_id: &NodeId) -> BTreeMap<String, String> {
    use rdm_admin::storage::NodesStorage as _;
    node_records().node(node_id).await.unwrap().map(|l| l.record.config).unwrap_or_default()
}

/// A birth's client to the authority, speaking as that birth.
async fn client_of(rig: &Rig, b: &Birth) -> NodeRpcClient {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(rig.resolved.clone());
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    tracing::info!(step = "client-bound", node = %b.node.node_id, node_path = %b.node.path, node_name = %b.node.name, peer = %b.key.public().fmt_short(), local = ?ep.bound_sockets(), "a client endpoint is bound on the birth's key");
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
async fn a_node_declares_its_own_state_forward_only() {
    let (rig, b) = rig().await;
    let rpc1 = &b["rpc1"];
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Ready)).await), StatusReply::Applied);
    // The same natural key again: one logical event.
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Ready)).await), StatusReply::AlreadyApplied);
    // Backward is refused naming the current state; a skip forward applies.
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Pending)).await), StatusReply::RejectedInvalidNodeTransition { current: NodeState::Ready });
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::Stopped)).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, rpc1, &declare(rpc1, NodeState::ConnectionsDeleting)).await), StatusReply::RejectedInvalidNodeTransition { current: NodeState::Stopped });
    assert_eq!(rig.service.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Stopped));
}

/// CONTRACT: the authority is the role the subject's declaration needs, never the caller
/// (node-rpc.md §14). rpc.2 sending rpc.1's declaration is decided for rpc.1, on rpc.1's row; a
/// subject the view does not hold is `SubjectUnknown`; a birth the view no longer holds is stale; a
/// subject of another mesh, a node-admin included, is refused naming the role it needs: its own
/// mesh's primary.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_deciding_primary_is_the_one_the_subject_needs_never_the_callers() {
    let (rig, b) = rig().await;
    let (rpc1, rpc2, admin2, m2admin) = (&b["rpc1"], &b["rpc2"], &b["admin2"], &b["m2admin"]);
    // rpc.2 sending rpc.1's state: decided for rpc.1.
    assert_eq!(reply(&call(&rig, rpc2, &declare(rpc1, NodeState::Ready)).await), StatusReply::Applied);
    assert_eq!(rig.service.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready), "applied for the subject");
    assert_eq!(rig.service.declared.lock().unwrap().node(&rpc2.node.node_id, rpc2.node.incarnation_id.as_ref().unwrap()), None, "nothing applied for the caller");
    // A subject the view does not hold at all.
    let stranger = birth(rdm_core::PathName::new("mesh1.node", "9").unwrap(), "node", "mesh1", false, false);
    let r = reply(&call(&rig, &stranger, &declare(&stranger, NodeState::Ready)).await);
    assert_eq!(r, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown });
    // A stale birth: the view holds a newer incarnation of the subject.
    let stale = StatusRequest::DeclareNodeState { node_id: rpc1.node.node_id.clone(), incarnation: IncarnationId::mint(), state: NodeState::Ready, operation: None };
    assert_eq!(reply(&call(&rig, rpc1, &stale).await), StatusReply::RejectedStaleIncarnation { held: rpc1.node.incarnation_id.clone().unwrap() });
    // A node of another mesh: this admin is not its mesh-primary.
    let foreign = birth(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "node", "mesh2", false, false);
    rig.topology.write().await.nodes.push(foreign.node.clone());
    let r = reply(&call(&rig, &foreign, &declare(&foreign, NodeState::Ready)).await);
    assert_eq!(r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } });
    // A node-admin declares to its own mesh's primary (this admin is mesh1's): Applied.
    assert_eq!(reply(&call(&rig, admin2, &declare(admin2, NodeState::Ready)).await), StatusReply::Applied);
    // A node-admin of another mesh is refused by the role it needs, its own mesh's primary, though
    // this admin holds the fabric-primary role (node-status.md: "A node reports to its mesh's primary").
    let r = reply(&call(&rig, m2admin, &declare(m2admin, NodeState::Ready)).await);
    assert_eq!(r, StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() } });
}

/// CONTRACT: nothing inside the mesh checks who is calling (node-rpc.md §14; node-rpc-envelope.md,
/// Status: "Nothing inside the mesh checks who is calling"): a declaration from an endpoint this
/// authority's view holds no birth for is decided for the subject the request names, on the
/// receiver's own role. The direction refusals stay, decided by the subject and the role: a subject
/// the view does not hold is `SubjectUnknown`, a subject of a mesh this admin is not the primary of
/// is `ReceiverNotPrimary`, and a birth the view no longer holds is `RejectedStaleIncarnation`.
/// What must NOT happen: a refusal naming the caller (`SenderNotSubject`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_declaration_from_an_endpoint_the_mesh_primary_never_heard_of_is_decided_for_its_subject() {
    let (rig, b) = rig().await;
    let (rpc1, admin2) = (&b["rpc1"], &b["admin2"]);
    // A fresh endpoint key, in no birth of the view.
    let stranger = birth(rdm_core::PathName::new("mesh1.node", "9").unwrap(), "node", "mesh1", false, false);
    assert!(rig.topology.read().await.births().all(|n| n.endpoint_id != stranger.node.endpoint_id), "the stranger is in no birth of the view");
    // A member's declaration is decided for that member: Applied, held for the subject.
    assert_eq!(reply(&call(&rig, &stranger, &declare(rpc1, NodeState::Ready)).await), StatusReply::Applied);
    assert_eq!(rig.service.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready), "the state held is the subject's");
    // A node-admin's declaration reaches the fabric-primary role this admin holds.
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
    let foreign = birth(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "node", "mesh2", false, false);
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
    assert!(rig.service.meshes_storage.mesh(&rig.mesh_id("mesh2")).await.unwrap().is_none_or(|r| r.state == rdm_core::status::MeshState::Pending), "the fabric-primary writes nothing of mesh2's record: its primary does");
    assert_eq!(reply(&call(&rig, m2admin, &m2(MeshState::Pending)).await), StatusReply::RejectedInvalidMeshTransition { current: MeshState::Ready });
    // The declaration is decided for the mesh it names, whoever calls (an ordinary node; a
    // non-primary admin of another mesh): forward applies once, a mesh the holder does not hold is
    // `SubjectUnknown`.
    assert_eq!(reply(&call(&rig, rpc1, &m2(MeshState::Deleting)).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, admin2, &m2(MeshState::Deleting)).await), StatusReply::Applied, "a repeated declaration is answered Applied again (Q144)");
    let unknown = StatusRequest::DeclareMeshState { mesh_id: MeshId::mint(), state: MeshState::Ready, changed_at_rafka_ms: 0 };
    assert_eq!(reply(&call(&rig, m2admin, &unknown).await), StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown });
    // A fabric event is applied at a mesh primary (this admin is mesh1's), whoever sends it, once.
    let ev = StatusRequest::ApplyFabricEvent { fabric_id: rig.service.fabric_id.clone(), event: FabricEvent::Ready };
    assert_eq!(reply(&call(&rig, m2admin, &ev).await), StatusReply::Applied);
    assert_eq!(reply(&call(&rig, m2admin, &ev).await), StatusReply::AlreadyApplied);
    let wrong = StatusRequest::ApplyFabricEvent { fabric_id: FabricId::mint(), event: FabricEvent::Ready };
    assert_eq!(reply(&call(&rig, m2admin, &wrong).await), StatusReply::RejectedStaleFabric { held: rig.service.fabric_id.clone() });
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
    assert!(rig.service.declared.lock().unwrap().nodes.is_empty());
    // Applied, reply lost: Indeterminate at the caller; the same declaration again is AlreadyApplied.
    rig.service.hold_next_reply.store(true, Ordering::SeqCst);
    let (out, ev) = c.call::<Status>(&target, &declare(rpc1, NodeState::Ready), &CallOptions { send: rdm_transport::Send::with_time_limit(Duration::from_millis(800)), ..Default::default() }).await;
    assert!(matches!(out, RpcOutcome::Indeterminate(_)), "{out:?}");
    assert!(ev.unwrap().committed);
    assert_eq!(rig.service.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready), "applied before the reply was lost");
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
    let opts = CallOptions { send: rdm_transport::Send::with_time_limit(Duration::from_millis(600)), ..Default::default() };
    let (out, ev) = c.call::<Status>(&target, &declare(rpc1, NodeState::Ready), &opts).await;
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == rdm_core::outcome::IndeterminateReason::ReplyDeadline), "{out:?}");
    assert!(ev.unwrap().committed, "the request crossed the commit cut before the reply was withheld");
    rig.withhold.reached().await;
    assert_eq!(rig.service.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready), "applied before the reply was withheld");
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
    assert_eq!(rig.service.declared.lock().unwrap().node(&rpc1.node.node_id, rpc1.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Ready));
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
    let stranger = birth(rdm_core::PathName::new("mesh1.node", "9").unwrap(), "node", "mesh1", false, false);
    assert_eq!(reply(&call(&rig, &b["rpc2"], &declare(&stranger, NodeState::Stopped)).await), StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown });
}

/// A birth of the fixture serving Status on its own key, recording every request it decodes and
/// answering `Applied`: the node end of a config apply.
struct ServedNode {
    resolved: ResolvedNode,
    seen: Arc<Mutex<Vec<StatusRequest>>>,
    /// Every `node.connection.delete` it is sent on the node command op 0x22, in order.
    deletes: Arc<Mutex<Vec<rdm_core::node_command::NodeCommandRequest>>>,
    /// Notified once per request served (a permit is kept when nobody waits yet).
    heard: Arc<tokio::sync::Notify>,
    /// The endpoint it serves on: a cell's call made as this birth goes out on it.
    endpoint: iroh::Endpoint,
    _router: Router,
    /// The nodes store this birth writes its own record to on `ApplyNodeName` (Q356): the rig's, set
    /// by [`node_configs_for`].
    store: Arc<OnceLock<Arc<MemoryNodesStorage>>>,
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
    let deletes: Arc<Mutex<Vec<rdm_core::node_command::NodeCommandRequest>>> = Arc::default();
    let heard: Arc<tokio::sync::Notify> = Arc::default();
    let (s, h) = (seen.clone(), heard.clone());
    let answer = Arc::new(answer);
    let (cs, ch, canswer, cd) = (seen.clone(), heard.clone(), answer.clone(), deletes.clone());
    let writer = Arc::new(rdm_node::node_records::NodePropertyWriter::new(node_records(), || "memory".into()));
    let store: Arc<OnceLock<Arc<MemoryNodesStorage>>> = Arc::default();
    let own_store = store.clone();
    let server = ServerBuilder::new()
        // The node command op 0x22 (i144 R22): `node.connection.delete` reaches the birth here, logged in
        // `deletes` and answered with every named connection cut, on the command's ordered stream.
        .serve_stream::<rdm_core::node_command::NodeCommand, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: rdm_core::node_command::NodeCommandRequest, sink| {
            let (s, h, answer, d) = (cs.clone(), ch.clone(), canswer.clone(), cd.clone());
            async move {
                use rdm_core::node_command::{NodeCommandReply as N, NodeCommandRequest};
                let NodeCommandRequest::DeleteConnections { node_id, incarnation, scope } = req.clone() else { panic!("expected DeleteConnections") };
                d.lock().unwrap().push(req);
                h.notify_one();
                let closed = match &scope { rdm_core::status::ConnectionScope::Connections(ids) => ids.clone(), rdm_core::status::ConnectionScope::All => vec![] };
                let (not_open, held, receipt) = (vec![], vec![], None);
                let mut sink = sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                if matches!(scope, rdm_core::status::ConnectionScope::All) {
                    sink.data(N::ConnectionsDeleting { node_id, incarnation }).await.map_err(|e| rdm_transport::HandlerFault::invariant_broken(format!("{e:?}")))?;
                }
                Ok(N::ConnectionsDeleted { closed, not_open, held, receipt })
            }
        })
        .serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
            let (s, h, answer, writer, own_store) = (s.clone(), h.clone(), answer.clone(), writer.clone(), own_store.clone());
            async move {
                // A birth that takes a property command puts it on its own record through its own
                // writer, as a node does (node writes its own); `answer` refusing it writes nothing.
                let reply = match answer(&req) {
                    applied @ (StatusReply::Applied | StatusReply::AlreadyApplied) => match (&req, own_store.get()) {
                        // A told name: the node writes its own record at its node.id with the new name.
                        (StatusRequest::ApplyNodeName { node_id, name, .. }, Some(store)) => {
                            use rdm_admin::storage::NodesStorage as _;
                            if let Some(line) = store.node(node_id).await.unwrap() {
                                store.put_node(&rdm_admin::storage::NodeRecord { path: name.path.clone(), name: name.name.clone(), ..line.record }).await.unwrap();
                            }
                            applied
                        }
                        _ => written_by_the_node(&writer, &req).await.unwrap_or(applied),
                    },
                    other => other,
                };
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
    let resolved = ResolvedNode { mesh: b.node.mesh.clone(), r#type: b.node.r#type.clone(), node_id: b.node.node_id.clone(), path: b.node.path.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    ServedNode { resolved, seen, deletes, heard, endpoint: ep, _router: router, store }
}

/// What a node's own writer answers `req` with once it is put on its own record in [`node_records`]:
/// the config of `ApplyNodeConfig` (`Applied`), the labels of `CreateTags` or `DeleteTags` (their
/// reply); `None` for any other request.
async fn written_by_the_node(writer: &rdm_node::node_records::NodePropertyWriter, req: &StatusRequest) -> Option<StatusReply> {
    use rdm_node::node_records::TagChange;
    let (node_id, incarnation) = match req {
        StatusRequest::ApplyNodeConfig { node_id, incarnation, .. } | StatusRequest::CreateTags { node_id, incarnation, .. } | StatusRequest::DeleteTags { node_id, incarnation, .. } => (node_id, incarnation),
        _ => return None,
    };
    // The record the node holds when it held none before: its own start's, at its birth.
    let initial = rdm_node::nodes_storage::fixture_record(&PathName::new("mesh1.node", "1").unwrap(), "node", node_id.clone(), incarnation.clone(), NodeStatus::Ready);
    let written = match req {
        StatusRequest::ApplyNodeConfig { config, .. } => {
            let config = config.clone();
            writer.update(initial, move |record| record.config = config, |_| {}).await.map(|_| StatusReply::Applied)
        }
        StatusRequest::CreateTags { labels, .. } => writer.tags(initial, TagChange::Create(labels.clone()), |_| {}).await,
        StatusRequest::DeleteTags { scope, .. } => writer.tags(initial, TagChange::Delete(scope.clone()), |_| {}).await,
        _ => unreachable!("property commands only"),
    };
    Some(written.unwrap_or_else(|e| panic!("the node's own put: {e}")))
}

/// `configs` with the admin `admin`'s own record written at its start (Q356), as its start writes it
/// before a command reaches it: what its own commands hand its writer.
async fn own_started(configs: rdm_admin::node_config::NodeConfigs, admin: &rdm_admin::model::Node) -> rdm_admin::node_config::NodeConfigs {
    let bound = rdm_admin::model::Node { endpoint_id: Some(rdm_admin::model::EndpointId("0".repeat(64))), transport_addr: Some("127.0.0.1:1".parse().unwrap()), ..admin.clone() };
    configs.bound_own(&bound, Default::default(), rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("own-started")).unwrap(), (None, None)).await.expect("the admin writes its own record at its start");
    configs
}

/// mesh1's primary's `NodeConfigs`, telling births through a client that resolves `node`.
async fn node_configs(rig: &Rig, node: &ServedNode) {
    node_configs_for(rig, &[node]).await
}

/// mesh1's primary's `NodeConfigs`, telling births through a client that resolves each of `nodes`.
async fn node_configs_for(rig: &Rig, nodes: &[&ServedNode]) {
    let resolver = Arc::new(StaticResolver::new());
    for node in nodes {
        resolver.insert(node.resolved.clone());
        let _ = node.store.set(rig.nodes.clone());
    }
    let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    // The `NodeConfigs` writes from the one copy of the records this admin holds, its authority's.
    let _ = rig.service.configs.set(Arc::new(own_started(crate::bound_configs_on(rig.nodes.clone(), rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")))).await, &rig.admin.node).await));
}

/// mesh1's primary's `NodeConfigs`, the one place its record changes take turns at, its client
/// resolving no node: what a cell whose subject is told nothing (a stopped birth) needs.
async fn quiet_node_configs(rig: &Rig) {
    let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _ = rig.service.configs.set(Arc::new(own_started(crate::bound_configs_on(rig.nodes.clone(), rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, Arc::new(StaticResolver::new())).with_caller_system("rdm")))).await, &rig.admin.node).await));
}

fn exact(b: &Birth) -> rdm_client::ExactBirth {
    rdm_client::ExactBirth { target: NodeTarget::ExactNode(b.node.node_id.clone()), node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap() }
}

/// A `ConfigUpdated` as the birth and the map it carries (a node record carries no revision: the
/// node's own property record is written whole by its one writer); any other reply panics.
fn updated(reply: &StatusReply) -> (NodeId, IncarnationId, BTreeMap<String, String>) {
    match reply {
        StatusReply::ConfigUpdated { node_id, incarnation, config, .. } => (node_id.clone(), incarnation.clone(), config.clone()),
        other => panic!("expected node.config.updated: {other:?}"),
    }
}

/// [`updated`] of `b` carrying `config`.
fn updated_of(b: &Birth, config: &BTreeMap<String, String>) -> (NodeId, IncarnationId, BTreeMap<String, String>) {
    (b.node.node_id.clone(), b.node.incarnation_id.clone().unwrap(), config.clone())
}

/// CONTRACT (`docs/architecture/lifecycles/node-config-update.md:5` — "`node.config.update` replaces a
/// node's application config, the whole map: a PUT, never a patch. It is a single call, not a Build."
/// and `docs/architecture/lifecycles/node-config-update.md`, a caller's update, step 3 — "Completion
/// follows durable persistence. The completion carries no node-record revision"; ops-naming "Node
/// data": a config update is a single call, not a Build; the node's
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
        node_configs(&rig, &node).await;
        let config: BTreeMap<String, String> = [("tags".to_string(), "edge,blue".to_string()), ("zone".to_string(), "a".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(updated(&reply), updated_of(rpc1, &config));
        let row = rig.nodes.record_at(&rpc1.node.location()).await.unwrap().expect("the mesh-primary wrote the node's row");
        let (row_config, row_tags) = props(&rig, &row.node_id).await;
        assert_eq!((row.node_id.clone(), row_config.clone()), (rpc1.node.node_id.clone(), config.clone()), "the row at mesh1.node.1 is this node with the whole config");
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
    assert_eq!(attrs.get("subject").map(String::as_str), Some("mesh1.node.1"), "{attrs:?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config subject=mesh1.node.1 outcome=config-updated");
}

/// CONTRACT (topology.rs `own`: a node is found by its exact birth, "never by path.name, its shape
/// included"; node-name-update.md: a renamed node stays the same exact birth): a config
/// update naming a node its mesh-primary's view holds at the predecessor name `1-old` is decided for
/// that exact birth: it ends `node.config.updated`, the node's own record holds the config, the birth
/// was told it once, and the via-config span names the `1-old` subject. What must NOT happen: the
/// update refused as "not a member" because the view's members leave predecessor names out.
#[test]
fn a_config_update_naming_a_node_at_its_old_row_name_is_decided_for_that_exact_birth() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let old: PathName = rdm_core::PathName::new("mesh1.node", "1-old").unwrap();
        rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == rpc1.node.node_id).expect("rpc1 is in the view").name = old.name.clone();
        let node = serve_node(rpc1).await;
        node_configs(&rig, &node).await;
        let config: BTreeMap<String, String> = [("zone".to_string(), "d".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(updated(&reply), updated_of(rpc1, &config));
        assert_eq!(props(&rig, &rpc1.node.node_id).await.0, config, "the node's own record holds the whole config");
        assert_eq!(node.seen.lock().unwrap().len(), 1, "the birth was told once");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!((attrs.get("outcome").map(String::as_str), attrs.get("subject").map(String::as_str)), (Some("config-updated"), Some("mesh1.node.1-old")), "{attrs:?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config subject=mesh1.node.1-old outcome=config-updated");
}

/// CONTRACT (node-config-update.md, Deferred application and startup: "A stopped process remains alive
/// with its Node RPC endpoint bound"; a caller's update, step 2: "The node's property writer applies the
/// whole map to its latest owned state and issues one put"; "Config and tags are independent fields;
/// changing config does not replace tags"): `node.config.update` of `mesh1.node.2`, stopped in the
/// mesh-primary's view, is sent to the stopped birth once and ends `node.config.updated`: its own record
/// holds the whole config and keeps the tags it carried. The via-config span says `config-updated` with
/// `apply=direct`. What must NOT happen: the update completed with nothing applied, or the tags dropped.
#[test]
fn a_stopped_nodes_config_update_is_applied_by_its_bound_writer_and_keeps_its_tags() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let stopped = &b["rpc2"];
        let node = serve_node(stopped).await;
        node_configs(&rig, &node).await;
        stopped_with_row(&rig, stopped, &["edge"]).await;
        let config: BTreeMap<String, String> = [("tier".to_string(), "gold".to_string())].into();
        let client = client_of(&rig, &b["admin2"]).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(stopped), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(updated(&reply), updated_of(stopped, &config), "a stopped node's config update completes once its writer put it");
        assert_eq!(props(&rig, &stopped.node.node_id).await, (config.clone(), labels(&["edge"])), "its own record holds the whole config and keeps its tags");
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap(), config }],
            "the stopped birth, still bound, is told once"
        );
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(
        (attrs.get("outcome").map(String::as_str), attrs.get("apply").map(String::as_str), attrs.get("subject").map(String::as_str), attrs.get("answered")),
        (Some("config-updated"), Some("direct"), Some("mesh1.node.2"), None),
        "{attrs:?}"
    );
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config subject=mesh1.node.2 apply=direct outcome=config-updated");
}

/// CONTRACT (node-config-update.md, a caller's update, steps 2-3: the node's property writer puts the
/// map, and "Completion follows durable persistence"): when the birth does not apply the config, the
/// update ends `ConfigNotApplied`, naming the node and the birth's answer by name; its own record holds
/// nothing new. The mesh-primary is ready; the birth refused. The birth's answer here is longer than
/// the reply's reason budget, so the reply carries it cut (ending `…`) and the via-config span carries
/// it whole: nothing of the refusal is lost.
#[test]
fn a_config_the_birth_does_not_apply_ends_not_applied_with_the_row_written() {
    let cap = capture();
    let cut = cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let refusal = format!("pre_node_ready has not passed: {}", "the store of mesh1.node.1 is still behind; ".repeat(20));
        let node = serve_node_answering(rpc1, StatusReply::NotReady { reason: refusal.clone() }).await;
        node_configs(&rig, &node).await;
        let config: BTreeMap<String, String> = [("zone".to_string(), "b".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &rdm_transport::RouteChoice::Direct, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        let StatusReply::ConfigNotApplied { node: named, answered, reason } = &reply else { panic!("the row is written and the birth refused: {reply:?}") };
        assert_eq!((named.as_str(), answered.as_str()), (rpc1.node.node_id.as_str(), "not-ready"), "{reply:?}");
        assert!(reason.ends_with('…'), "the birth's long answer is cut to the reply's budget: {reason}");
        assert_eq!(node.seen.lock().unwrap().len(), 1, "the birth was told once");
        assert!(props(&rig, &rpc1.node.node_id).await.0.is_empty(), "the birth refused: its own record holds nothing new");
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

/// A nodes store that refuses every put of a node's record, by name; every other call is the memory
/// store's.
#[derive(Default)]
struct RefusingRecords(MemoryNodesStorage);

type Answer<T> = Result<T, rdm_node::storage_error::StorageError>;

#[async_trait::async_trait]
impl rdm_admin::storage::NodesStorage for RefusingRecords {
    async fn node(&self, node_id: &NodeId) -> Answer<Option<rdm_admin::storage::NodeLine>> {
        self.0.node(node_id).await
    }
    async fn nodes(&self, mesh: &str) -> Answer<Vec<rdm_admin::storage::NodeRecord>> {
        self.0.nodes(mesh).await
    }
    async fn put_node(&self, row: &rdm_admin::storage::NodeRecord) -> Answer<()> {
        Err(rdm_node::storage_error::StorageError::Io { file: format!("nodes.jsonl ({})", row.node_id), reason: "disk refused the write".into() })
    }
    async fn records_at(&self, name: &PathName) -> Answer<Vec<rdm_admin::storage::NodeRecord>> {
        self.0.records_at(name).await
    }
}

/// CONTRACT (node-config-update.md, a caller's update, steps 2-3: "The node's property writer applies
/// the whole map to its latest owned state and issues one put"; "Completion follows durable
/// persistence"): when the store refuses the put of mesh1.node-admin.1's own property record, the update of
/// mesh1.node-admin.1 at itself ends `ConfigRowRefused` naming the node and the store's refusal, and
/// nothing is held as written. The via-config span says `config-row-refused` with the store's whole
/// refusal in `detail`.
#[test]
fn a_row_the_store_refuses_ends_row_refused_and_nothing_is_told() {
    let cap = capture();
    cap.run(async {
        let (rig, _) = rig().await;
        let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let refusing = Arc::new(RefusingRecords::default());
        let configs = crate::bound_configs_over(refusing.clone(), rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, Arc::new(StaticResolver::new())).with_caller_system("rdm")))).await;
        let view = rig.topology.read().await.clone();
        let me = &rig.admin.node;
        // Its start's own record, which the refusing store does not take either: the record its
        // commands hand its writer.
        let bound = rdm_admin::model::Node { endpoint_id: Some(rdm_admin::model::EndpointId("0".repeat(64))), transport_addr: Some("127.0.0.1:1".parse().unwrap()), ..me.clone() };
        let _ = configs.bound_own(&bound, Default::default(), rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("own-started")).unwrap(), (None, None)).await;
        let req = StatusRequest::UpdateNodeConfig { node_id: me.node_id.clone(), incarnation: me.incarnation_id.clone().unwrap(), config: [("zone".to_string(), "c".to_string())].into() };
        let reply = configs.update(&me.location(), &me.node_id, &view, &MemoryNodesStorage::default(), &req).await;
        assert!(
            matches!(&reply, StatusReply::ConfigRowRefused { node, reason, .. } if *node == me.node_id.to_string() && reason.contains("disk refused the write")),
            "the store refused the put: {reply:?}"
        );
        use rdm_admin::storage::NodesStorage as _;
        assert!(refusing.node(&me.node_id).await.unwrap().is_none(), "nothing is written");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    assert_eq!(spans[0].1.get("outcome").map(String::as_str), Some("config-row-refused"), "{spans:?}");
    assert!(spans[0].1.get("detail").is_some_and(|d| d.contains("disk refused the write") && !d.ends_with('…')), "the via-config span carries the store's whole refusal: {spans:?}");
    eprintln!("trace {}: rdm.node_admin.node.update.via-config outcome=config-row-refused", spans[0].0);
}

/// A `NodeConfigs` at the admin keyed `key`, telling births through a client that resolves only `node`.
async fn node_configs_at(key: &SecretKey, node: &ServedNode) -> rdm_admin::node_config::NodeConfigs {
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(node.resolved.clone());
    // A node-admin served here is the mesh-primary the command is sent down to: its role names it, as
    // the topology feed names a primary (Q313).
    if node.resolved.r#type == "node-admin" {
        resolver.set_primary(Some(node.resolved.mesh.clone()), Some(node.resolved.node_id.clone()));
    }
    let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    crate::bound_configs_on(node_records(), rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")))).await
}

/// CONTRACT (node-config-update.md steps 1-2: "The caller sends `node.config.update` to the
/// fabric-primary ... and sends the command to the node's mesh-primary"; "The mesh-primary writes the
/// record"; fabric-node-lifecycle §4.3: the mesh-primary of a node's mesh is the only writer of that
/// mesh's rows): a node-admin's config update for mesh2.node.1 (`SentBy::Other`: the fabric-primary's
/// command sent down to an admin that does not hold the mesh-primary) at mesh1.node-admin.2, which is neither
/// the fabric-primary nor mesh2's primary, is refused by name, `ReceiverNotPrimary` naming the
/// fabric-primary; no row is written and nothing is told. The same command at mesh2's primary, the
/// fabric-primary's down hop, writes the row and tells the birth directly, once, and ends
/// `ConfigUpdated`. Spans: `via-not-primary` at mesh1.node-admin.2, `via-config` `config-updated` at
/// mesh2.node-admin.1.
#[test]
fn a_config_update_is_refused_by_name_at_an_admin_that_is_neither_primary_and_written_by_the_nodes_mesh_primary() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, m2admin) = (&b["admin2"], &b["m2admin"]);
        let m2rpc = birth(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "node", "mesh2", false, false);
        rig.topology.write().await.nodes.push(m2rpc.node.clone());
        let node = serve_node(&m2rpc).await;
        let config: BTreeMap<String, String> = [("tags".to_string(), "far".to_string())].into();
        let req = StatusRequest::UpdateNodeConfig { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap(), config: config.clone() };
        let view = rig.topology.read().await.clone();
        // At an admin that is neither the fabric-primary nor mesh2's primary: refused by name.
        let neither_rows = MemoryNodesStorage::default();
        let neither = node_configs_at(&admin2.key, &node).await;
        let refused = neither.serve(&admin2.node.location(), &admin2.node.node_id, &view, &rig.topology, &neither_rows, SentBy::Other, &req).await;
        let StatusReply::RejectedNotPrimary { why: NotPrimary::ReceiverNotPrimary { needed } } = &refused else { panic!("mesh1.node-admin.2 is neither primary: {refused:?}") };
        assert!(needed.contains("fabric-primary") && needed.contains(rig.admin.node.node_id.as_str()), "the refusal names the fabric-primary: {needed}");
        assert!(neither_rows.record_at(&m2rpc.node.location()).await.unwrap().is_none(), "no row is written");
        assert!(node.seen.lock().unwrap().is_empty(), "nothing is told by an admin that is neither primary");
        // At mesh2's primary: the row is written, then the birth is told directly.
        let mp_rows = MemoryNodesStorage::default();
        let configs = node_configs_at(&m2admin.key, &node).await;
        let served = configs.serve(&m2admin.node.location(), &m2admin.node.node_id, &view, &rig.topology, &mp_rows, SentBy::Other, &req).await;
        assert_eq!(updated(&served), updated_of(&m2rpc, &config));
        assert_eq!(config_of(&m2rpc.node.node_id).await, config, "the node's own record holds the whole config");
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: m2rpc.node.node_id.clone(), incarnation: m2rpc.node.incarnation_id.clone().unwrap(), config }],
            "the birth was told once, by its mesh's primary"
        );
    });
    let refused = cap.spans("rdm.node_admin.node.reject.via-not-primary");
    let (np_trace, np) = refused.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.node-admin.2")).unwrap_or_else(|| panic!("a via-not-primary span at mesh1.node-admin.2: {refused:?}"));
    assert_eq!(np.get("subject").map(String::as_str), Some("mesh2.node.1"), "{np:?}");
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (mp_trace, mp) = &spans[0];
    assert_eq!((mp.get("node").map(String::as_str), mp.get("outcome").map(String::as_str), mp.get("subject").map(String::as_str)), (Some("mesh2.node-admin.1"), Some("config-updated"), Some("mesh2.node.1")), "{mp:?}");
    eprintln!("trace {np_trace}: rdm.node_admin.node.reject.via-not-primary node=mesh1.node-admin.2 subject=mesh2.node.1");
    eprintln!("trace {mp_trace}: rdm.node_admin.node.update.via-config node=mesh2.node-admin.1 subject=mesh2.node.1 outcome=config-updated");
}

/// CONTRACT (node-config-update.md step 1: the fabric-primary "sends the command to the node's
/// mesh-primary"; node-rpc.md §43: delivery is owed until acked, a named refusal ends one request's
/// retry and the owning workflow retargets it; D12): the fabric-primary sends mesh2.node.1's
/// `node.config.update` down to mesh2.node-admin.1, which its view names mesh2's primary but which does
/// not hold it yet: it answers `RejectedNotPrimary`. The caller is not failed: the command stays
/// owed, and once mesh2.node-admin.1 holds the primary (and the fabric-primary's view changes) it
/// is sent again to it and ends `ConfigUpdated`; no copy of the map is installed in the
/// fabric-primary's atlas (node-config-update.md step 3). Span:
/// `rdm.node_admin.node.update.via-fabric-primary` with `mesh_primary` = mesh2.node-admin.1's node id and
/// `sends` = the sends made, the last answered, outcome `config-updated`. Must NOT happen: the caller
/// answered `RejectedNotPrimary` or `NotReady`, or the map copied into the atlas.
#[test]
fn a_config_update_sent_down_while_the_meshs_primary_changes_completes_once_the_new_primary_holds_it() {
    let cap = capture();
    let mesh_primary = cap.run(async {
        let (rig, b) = rig().await;
        let m2admin = &b["m2admin"];
        let m2rpc = birth(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "node", "mesh2", false, false);
        rig.topology.write().await.nodes.push(m2rpc.node.clone());
        let landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let holds = landed.clone();
        let (node_id, incarnation) = (m2rpc.node.node_id.clone(), m2rpc.node.incarnation_id.clone().unwrap());
        let (n, i) = (node_id.clone(), incarnation.clone());
        // mesh2.node-admin.1: elected in the fabric-primary's view, not holding it until `landed`.
        let mp = serve_node_with(m2admin, move |req| match req {
            StatusRequest::UpdateNodeConfig { .. } if !holds.load(Ordering::SeqCst) => rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "mesh-primary of mesh2".into() }),
            StatusRequest::UpdateNodeConfig { config, .. } => StatusReply::ConfigUpdated { node_id: n.clone(), incarnation: i.clone(), config: config.clone() },
            other => panic!("only the config command reaches mesh2's primary: {other:?}"),
        })
        .await;
        let configs = node_configs_at(&rig.admin.key, &mp).await;
        let (tick, views) = tokio::sync::watch::channel(0u64);
        configs.watch_views(views);
        let told: BTreeMap<String, String> = [("admin_api_base".to_string(), "rdm://mesh2".to_string())].into();
        let req = StatusRequest::UpdateNodeConfig { node_id: node_id.clone(), incarnation: incarnation.clone(), config: told.clone() };
        let view = rig.topology.read().await.clone();
        let rows = MemoryNodesStorage::default();
        let admin_at = rig.admin.node.location();
        let serve = configs.serve(&admin_at, &rig.admin.node.node_id, &view, &rig.topology, &rows, SentBy::Caller, &req);
        let land = async {
            tokio::time::timeout(Duration::from_secs(10), mp.heard.notified()).await.expect("the fabric-primary sent the command down");
            // mesh2.node-admin.1 holds the primary now, and the fabric-primary's view changes.
            landed.store(true, Ordering::SeqCst);
            tick.send_modify(|t| *t += 1);
        };
        let (reply, ()) = tokio::time::timeout(Duration::from_secs(20), async { tokio::join!(serve, land) }).await.expect("the owed command completes once mesh2.node-admin.1 holds the primary");
        assert_eq!(reply, StatusReply::ConfigUpdated { node_id: node_id.clone(), incarnation: incarnation.clone(), config: told.clone() }, "the caller is answered once the primary is held");
        let sent: Vec<StatusRequest> = mp.seen.lock().unwrap().clone();
        assert!(sent.len() >= 2 && sent.iter().all(|r| *r == req), "the same command, sent down again to mesh2.node-admin.1: {sent:?}");
        m2admin.node.node_id.to_string()
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-fabric-primary");
    let (trace, a) = spans.iter().find(|(_, a)| a.get("subject").map(String::as_str) == Some("mesh2.node.1")).unwrap_or_else(|| panic!("a via-fabric-primary span: {spans:?}"));
    assert_eq!((a.get("outcome").map(String::as_str), a.get("mesh_primary"), a.get("atlas")), (Some("config-updated"), Some(&mesh_primary), None), "{a:?}");
    let sends: u32 = a.get("sends").and_then(|v| v.parse().ok()).unwrap_or(0);
    assert!(sends >= 2, "sent down again after the refusal: {a:?}");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-fabric-primary subject=mesh2.node.1 sends={sends} outcome=config-updated");
}

/// CONTRACT (ops-naming "Node data" :79: caller → the node's mesh-primary, Forward `0x1A` when not
/// directly reachable): a caller that resolves only a carrier (mesh2's primary) reaches mesh1's
/// primary through it, and mesh1's primary, which here does NOT hold the fabric-primary role, writes
/// mesh1.node.1's row and tells it: `ConfigUpdated`, the birth told once, and the caller's
/// `via-connections` span names `via-peer` with carrier mesh2.node-admin.1 inside the via-config trace.
#[test]
fn a_caller_reaches_the_nodes_mesh_primary_through_a_carrier_and_it_writes_and_applies() {
    let cap = capture();
    let carrier_id = cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1, rpc2, m2admin) = (&b["admin2"], &b["rpc1"], &b["rpc2"], &b["m2admin"]);
        // The fabric-primary role is mesh1.node-admin.2's in this view: the receiver is mesh1's primary only.
        for n in rig.topology.write().await.nodes.iter_mut() {
            n.is_fabric_primary = n.node_id == admin2.node.node_id;
        }
        let node = serve_node(rpc1).await;
        node_configs(&rig, &node).await;
        let (carrier, _carrier_router) = carrier_admin(m2admin, &rig.resolved).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(carrier);
        let ep = rdm_transport::endpoint::bind(rpc2.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
        let config: BTreeMap<String, String> = [("zone".to_string(), "far".to_string())].into();
        let route = rdm_transport::RouteChoice::ViaPeer { carrier: m2admin.node.node_id.clone() };
        let reply = rdm_client::NodeRpc::new(&client).config_update(&rig.admin.node.node_id, &route, &exact(rpc1), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(updated(&reply), updated_of(rpc1, &config));
        let row = rig.nodes.record_at(&rpc1.node.location()).await.unwrap().expect("mesh1's primary wrote the row");
        let (row_config, row_tags) = props(&rig, &row.node_id).await;
        assert_eq!(row_config, config);
        assert_eq!(
            node.seen.lock().unwrap().clone(),
            vec![StatusRequest::ApplyNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config }],
            "the birth was told once, by its mesh's primary"
        );
        m2admin.node.node_id.to_string()
    });
    let config_spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(config_spans.len(), 1, "{config_spans:?}");
    let (trace_id, attrs) = &config_spans[0];
    assert_eq!(attrs.get("outcome").map(String::as_str), Some("config-updated"), "{attrs:?}");
    assert_eq!(attrs.get("node").map(String::as_str), Some("mesh1.node-admin.1"), "{attrs:?}");
    let routes = cap.spans("rdm.node_rpc.route.resolve.via-connections");
    let carried: Vec<_> = routes.iter().filter(|(t, a)| t == trace_id && a.get("route").map(String::as_str) == Some("via-peer")).collect();
    assert_eq!(carried.len(), 1, "the caller's one carried leg is in the via-config trace: {routes:?}");
    assert_eq!(carried[0].1.get("carrier"), Some(&carrier_id));
    assert_eq!(carried[0].1.get("protocol").map(String::as_str), Some("status"));
    eprintln!("trace {trace_id}: rdm.node_rpc.route.resolve.via-connections route=via-peer carrier=mesh2.node-admin.1 > rdm.node_admin.node.update.via-config node=mesh1.node-admin.1 outcome=config-updated");
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
        let node = serve_node(rpc1).await;
        node_configs(&rig, &node).await;
        let me = &rig.admin.node;
        let config: BTreeMap<String, String> = [("admin_api_base".to_string(), "http://127.0.0.1:9".to_string())].into();
        let client = client_of(&rig, admin2).await;
        let reply = rdm_client::NodeRpc::new(&client).config_update(&me.node_id, &rdm_transport::RouteChoice::Direct, &exact(&rig.admin), config.clone(), &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(updated(&reply), (me.node_id.clone(), me.incarnation_id.clone().unwrap(), config.clone()));
        let row = rig.nodes.record_at(&me.location()).await.unwrap().expect("mesh1's primary wrote its own row");
        let (row_config, row_tags) = props(&rig, &row.node_id).await;
        assert_eq!((row.node_id.clone(), row_config.clone()), (me.node_id.clone(), config.clone()));
        assert!(node.seen.lock().unwrap().is_empty(), "no other node is told");
    });
    let spans = cap.spans("rdm.node_admin.node.update.via-config");
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(attrs.get("outcome").map(String::as_str), Some("config-updated"), "{attrs:?}");
    assert_eq!(attrs.get("subject").map(String::as_str), Some("mesh1.node-admin.1"), "{attrs:?}");
    assert_eq!(attrs.get("apply").map(String::as_str), Some("in-process"), "{attrs:?}");
    eprintln!("trace {trace_id}: rdm.node_admin.node.update.via-config node=mesh1.node-admin.1 subject=mesh1.node-admin.1 apply=in-process outcome=config-updated");
}

/// A second node-admin of the rig, `b`, holding no primary of its own unless the view says so: its own
/// `StatusAuthority` over `rig`'s view (the views agree), served on `b`'s key, its `NodeConfigs` telling
/// through a client that resolves each of `resolves`. Its nodes store is its own.
struct SecondAdmin {
    status: Arc<StatusService>,
    resolved: ResolvedNode,
    /// What its `NodeConfigs`' client resolves: a cell adds a birth it reaches later.
    resolver: Arc<StaticResolver>,
    /// Its atlas, fed every record its writer holds (Q224, as the boot wires it).
    atlas: Arc<rdm_admin::atlas::Atlas>,
    _router: Router,
}

async fn second_admin(rig: &Rig, b: &Birth, resolves: &[ResolvedNode]) -> SecondAdmin {
    let mesh_ids = rig.mesh_ids.clone();
    let status = Arc::new(StatusService {
        certs: None,
        topology_view: None,
        declaration_seam: None,
        command_frame_seam: None,
        operation_effects: Default::default(),
        stopped: Default::default(),
        me: b.node.location(),
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
        primary_commands: Default::default(),
        storage_changes: Default::default(), hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        wake: Default::default(),
        fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
        configs: Default::default(),
        republish: Default::default(),
        pre_node_ready: Default::default(),
        rename: Default::default(),
        row_moved: Default::default(), rejoin: Default::default(),
        loads: Some(Arc::new(BTreeMap::new)),
        digest_view: None,
        unreachable: Default::default(),
        builds: std::sync::Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
    });
    let resolver = Arc::new(StaticResolver::new());
    for r in resolves {
        resolver.insert(r.clone());
    }
    name_primaries(&resolver, &*rig.topology.read().await);
    let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
    let _ = status.atlas.set(atlas.clone());
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _ = status.configs.set(Arc::new(own_started(crate::bound_configs_on(node_records(), rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver.clone()).with_caller_system("rdm")))).await, &b.node).await));
    let slot: Arc<OnceLock<Arc<StatusService>>> = Arc::new(OnceLock::new());
    let _ = slot.set(status.clone());
    let server = rdm_admin::status_rpc::serve(ServerBuilder::new(), slot).build(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().to_string() }).unwrap();
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
    let resolved = ResolvedNode { mesh: b.node.mesh.clone(), r#type: b.node.r#type.clone(), node_id: b.node.node_id.clone(), path: b.node.path.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    SecondAdmin { status, resolved, resolver, atlas, _router: router }
}

/// The rig's admin (the fabric-primary and mesh1's primary) with its atlas and its `NodeConfigs` telling
/// through a client that resolves each of `resolves`.
async fn fabric_primary_atlas(rig: &Rig, resolves: &[ResolvedNode]) -> Arc<rdm_admin::atlas::Atlas> {
    let atlas = Arc::new(rdm_admin::atlas::Atlas::default());
    let _ = rig.service.atlas.set(atlas.clone());
    let resolver = Arc::new(StaticResolver::new());
    for r in resolves {
        resolver.insert(r.clone());
    }
    name_primaries(&resolver, &*rig.topology.read().await);
    let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let _ = rig.service.configs.set(Arc::new(own_started(crate::bound_configs_on(rig.nodes.clone(), rdm_admin::node_config::NodeConfigs::new(Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm")))).await, &rig.admin.node).await));
    atlas
}

/// Name on `resolver` the primaries `view` holds, as the process's topology feed names them from the
/// primary records (Q313): the fabric-primary and each mesh's mesh-primary.
fn name_primaries(resolver: &StaticResolver, view: &rdm_admin::topology::Topology) {
    for n in view.nodes.iter() {
        if n.is_fabric_primary {
            resolver.set_primary(None, Some(n.node_id.clone()));
        }
        if n.is_primary && n.r#type == "node-admin" {
            resolver.set_primary(Some(n.mesh.clone()), Some(n.node_id.clone()));
        }
    }
}

/// `node.get [id: b]` at the rig's fabric-primary: the config its answer carries.
async fn config_read_from_the_fabric_primary(rig: &Rig, b: &Birth) -> BTreeMap<String, String> {
    // The node's record, as it wrote it ([`node_records`]), in the rig's nodes store: one store in a
    // running fabric (Q356).
    {
        use rdm_admin::storage::NodesStorage as _;
        if let Some(own) = node_records().node(&b.node.node_id).await.unwrap() {
            rig.nodes.put_node(&own.record).await.unwrap();
        }
    }
    let read = rdm_core::status::Read { entity: rdm_core::filter::Entity::Nodes, node: None, filter: rdm_core::filter::Filter::new().eq("id", [b.node.node_id.to_string()]) };
    match rig.service.read_rows(&read).await {
        Ok(rdm_core::read::Rows::Nodes(nodes)) => {
            assert_eq!(nodes.len(), 1, "node.get [id] answers the one node: {nodes:?}");
            nodes[0].config.clone()
        }
        other => panic!("node.get at the fabric-primary answered {other:?}"),
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





/// CONTRACT (node-rpc.md §43: a named refusal ends one request's retry and the owning workflow
/// retargets it; the fabric-primary is the one the forwarding admin's records name, never a digest):
/// mesh1.node-admin.2 carries a caller's config update of mesh1.node.2 (sent by mesh1.node.1, a node that is not
/// a node-admin, about another node) up while the fabric-primary is changing: the
/// admin its view names (mesh2.node-admin.1) answers `ReceiverNotPrimary` until it holds it. The caller is
/// not failed: once mesh2.node-admin.1 holds it and mesh1.node-admin.2's view changes, the same command is carried
/// up again and its answer handed back. Span: `via-forward` with `sends` >= 2, outcome `config-updated`.
#[test]
fn a_callers_command_carried_up_while_the_fabric_primary_changes_completes_once_the_new_fabric_primary_holds_it() {
    let cap = capture();
    let fabric_primary = cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc2, m2admin) = (&b["admin2"], &b["rpc2"], &b["m2admin"]);
        // The view names mesh2.node-admin.1 the fabric-primary.
        for n in rig.topology.write().await.nodes.iter_mut() {
            n.is_fabric_primary = n.node_id == m2admin.node.node_id;
        }
        let landed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let holds = landed.clone();
        let (n, i) = (rpc2.node.node_id.clone(), rpc2.node.incarnation_id.clone().unwrap());
        let fp = serve_node_with(m2admin, move |req| match req {
            StatusRequest::UpdateNodeConfig { .. } if !holds.load(Ordering::SeqCst) => rdm_core::reply::status::rejected_not_primary(NotPrimary::ReceiverNotPrimary { needed: "the fabric-primary".into() }),
            StatusRequest::UpdateNodeConfig { config, .. } => StatusReply::ConfigUpdated { node_id: n.clone(), incarnation: i.clone(), config: config.clone() },
            other => panic!("only the config command reaches the fabric-primary: {other:?}"),
        })
        .await;
        let joined = second_admin(&rig, admin2, &[fp.resolved.clone()]).await;
        let (tick, views) = tokio::sync::watch::channel(0u64);
        joined.status.configs.get().unwrap().watch_views(views);
        // The caller is mesh1.node.1, about mesh1.node.2.
        let node = serve_node(&b["rpc1"]).await;
        let client = own_client(&node, &joined.resolved);
        let config: BTreeMap<String, String> = [("zone".to_string(), "b".to_string())].into();
        let opts = CallOptions { send: rdm_transport::Send::default(), ..CallOptions::default() };
        let (rpc, birth) = (rdm_client::NodeRpc::new(&client), exact(rpc2));
        let call = rpc.config_update(&admin2.node.node_id, &rdm_transport::RouteChoice::Direct, &birth, config.clone(), &opts);
        let land = async {
            tokio::time::timeout(Duration::from_secs(10), fp.heard.notified()).await.expect("mesh1.node-admin.2 carried the command up");
            landed.store(true, Ordering::SeqCst);
            tick.send_modify(|t| *t += 1);
        };
        let (reply, ()) = tokio::time::timeout(Duration::from_secs(30), async { tokio::join!(call, land) }).await.expect("the carried command completes once the fabric-primary is held");
        assert_eq!(reply.expect("a typed reply"), StatusReply::ConfigUpdated { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap(), config: config.clone() });
        let sent = fp.seen.lock().unwrap().clone();
        let req = StatusRequest::UpdateNodeConfig { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap(), config };
        assert!(sent.len() >= 2 && sent.iter().all(|r| *r == req), "the same command, carried up again: {sent:?}");
        m2admin.node.node_id.to_string()
    });
    let up = cap.spans("rdm.node_admin.node.update.via-forward");
    let (trace, a) = up.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.node-admin.2")).unwrap_or_else(|| panic!("a via-forward span at mesh1.node-admin.2: {up:?}"));
    let sends: u32 = a.get("sends").and_then(|v| v.parse().ok()).unwrap_or(0);
    assert!(sends >= 2 && a.get("fabric_primary") == Some(&fabric_primary) && a.get("outcome").map(String::as_str) == Some("config-updated"), "{a:?}");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-forward node=mesh1.node-admin.2 fabric_primary=mesh2.node-admin.1 sends={sends} outcome=config-updated");
}

/// CONTRACT (node-rpc-envelope.md node.tags; node-config-update.md step 1, the same `NodeConfigs` for a node's
/// own properties; D21): mesh1.node.1 creates its own tags with a call to the node-admin it joined
/// through, mesh1.node-admin.2, which is not the fabric-primary: mesh1.node-admin.2 carries it up to the
/// fabric-primary, mesh1's primary, which sends it down to the node's own writer: `TagsCreated`, the
/// node's own record carries them, and mesh1.node-admin.2 writes nothing. Span: `via-forward` at mesh1.node-admin.2, op
/// `node.tag.create`, outcome `tags-created`. Must NOT happen: `ReceiverNotPrimary` at mesh1.node-admin.2.
#[test]
fn a_nodes_own_tags_create_through_an_admin_that_is_not_the_fabric_primary_is_carried_up_and_written() {
    let cap = capture();
    let fabric_primary = cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        let node = serve_node(rpc1).await;
        fabric_primary_atlas(&rig, &[node.resolved.clone()]).await;
        let joined = second_admin(&rig, admin2, &[rig.resolved.clone()]).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(joined.resolved.clone());
        let client = NodeRpcClient::new(node.endpoint.clone(), resolver).with_caller_system("rdm");
        let opts = CallOptions { send: rdm_transport::Send::default(), ..CallOptions::default() };
        let (out, _) = tokio::time::timeout(Duration::from_secs(20), client.call::<Status>(&NodeTarget::ExactNode(admin2.node.node_id.clone()), &create_tags(rpc1, &["edge"]), &opts)).await.expect("the owed call is answered");
        let StatusReply::TagsCreated { node_id, .. } = reply(&out) else { panic!("tags created: {out:?}") };
        assert_eq!(node_id, rpc1.node.node_id);
        assert_eq!(props(&rig, &rpc1.node.node_id).await.1, labels(&["edge"]), "the node's own record carries the labels");
        assert!(rdm_admin::node_rows::record_at(&*joined.status.nodes_storage, &rpc1.node.location()).await.unwrap().is_none(), "mesh1.node-admin.2 writes no row");
        rig.admin.node.node_id.to_string()
    });
    let up = cap.spans("rdm.node_admin.node.update.via-forward");
    let (trace, a) = up.iter().find(|(_, a)| a.get("node").map(String::as_str) == Some("mesh1.node-admin.2")).unwrap_or_else(|| panic!("a via-forward span at mesh1.node-admin.2: {up:?}"));
    assert_eq!((a.get("op").map(String::as_str), a.get("fabric_primary"), a.get("outcome").map(String::as_str)), (Some("node.tag.create"), Some(&fabric_primary), Some("tags-created")), "{a:?}");
    eprintln!("trace {trace}: rdm.node_admin.node.update.via-forward node=mesh1.node-admin.2 op=node.tag.create outcome=tags-created");
}

/// The rig's view with mesh1.node-admin.2 mesh1's primary and mesh1.node-admin.1 the fabric-primary only.
async fn mesh1_primary_is_admin2(rig: &Rig, admin2: &Birth) {
    for n in rig.topology.write().await.nodes.iter_mut() {
        if n.r#type == "node-admin" && n.mesh == "mesh1" {
            n.is_primary = n.node_id == admin2.node.node_id;
        }
    }
}



/// CONTRACT (node-config-update.md, a node's own update: "1. The node calls its local property writer
/// with the whole map. 2. The writer persists it and publishes the durable result locally before
/// answering. 3. No command or map travels upward to either primary."): mesh1.node-admin.2, holding no
/// primary, puts its own config: its own record holds the map when `ConfigUpdated` is answered, and
/// nothing reaches mesh1's primary, mesh1.node-admin.1 (also the fabric-primary): no via-config span there,
/// no copy in its atlas, no row written. Must NOT happen: a `node.config.update` carried to a primary,
/// or a map copied into an atlas.
#[test]
fn a_node_admins_own_config_update_is_written_by_its_own_writer_and_nothing_travels_upward() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let admin2 = &b["admin2"];
        let joined = second_admin(&rig, admin2, &[rig.resolved.clone()]).await;
        fabric_primary_atlas(&rig, &[joined.resolved.clone()]).await;
        let config: BTreeMap<String, String> = [("admin_api_base".to_string(), "rdm://mesh1.node-admin.2".to_string())].into();
        let reply = tokio::time::timeout(Duration::from_secs(20), joined.status.own_config_update(admin2.node.incarnation_id.clone().unwrap(), config.clone())).await.expect("the command completes");
        assert_eq!(updated(&reply), updated_of(admin2, &config));
        assert_eq!(config_of(&admin2.node.node_id).await, config, "mesh1.node-admin.2's own record holds the whole config");
        assert!(rdm_admin::node_rows::record_at(&*joined.status.nodes_storage, &admin2.node.location()).await.unwrap().is_none(), "mesh1.node-admin.2 writes no row");
    });
    for name in ["rdm.node_admin.node.update.via-config", "rdm.node_admin.node.update.via-forward", "rdm.node_admin.node.update.via-fabric-primary"] {
        assert!(cap.spans(name).is_empty(), "nothing travels upward: {name} {:?}", cap.spans(name));
    }
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
                (s.span_context.trace_id().to_string(), dotted(s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect()), events)
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
            .map(|s| (s.span_context.trace_id().to_string(), dotted(s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect())))
            .collect()
    }
}

/// `attrs` with each `<x>_path` and `<x>_name` pair also read as `<x>` = `path.name`, the way a cell
/// names a node; an attribute already called `<x>` is kept as recorded.
fn dotted(mut attrs: BTreeMap<String, String>) -> BTreeMap<String, String> {
    let pairs: Vec<(String, String)> = attrs
        .iter()
        .filter_map(|(k, path)| k.strip_suffix("_path").and_then(|x| attrs.get(&format!("{x}_name")).map(|name| (x.to_string(), format!("{path}.{name}")))))
        .collect();
    for (x, dotted) in pairs {
        attrs.entry(x).or_insert(dotted);
    }
    attrs
}

/// mesh2's primary admin as a carrier: the admin composition (`admin::rpc_server`) on its own key,
/// its live resolver holding `target`, the birth it carries to.
async fn carrier_admin(b: &Birth, target: &ResolvedNode) -> (ResolvedNode, Router) {
    use rdm_core::connections::{ConnectionEnd, ConnectionsHeld};
    let ep = rdm_transport::endpoint::bind(b.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolved = ResolvedNode { mesh: b.node.mesh.clone(), r#type: b.node.r#type.clone(), node_id: b.node.node_id.clone(), path: b.node.path.clone(), name: b.node.name.clone(), endpoint_id: b.key.public(), transport_addr: addr, incarnation: b.node.incarnation_id.clone().unwrap() };
    let live = Arc::new(rdm_transport::LiveNodeResolver::default());
    live.apply(target.clone(), None);
    let connections = Arc::new(rdm_admin::connections_writer::ConnectionsWriter::new(
        ConnectionEnd { mesh: resolved.mesh.clone(), r#type: resolved.r#type.clone(), path: resolved.path.clone(), name: resolved.name.clone(), node_id: resolved.node_id.clone(), incarnation: Some(resolved.incarnation.clone()) },
        Arc::new(rdm_admin::storage::MemoryConnectionsStorage::default()),
        Arc::new(Mutex::new(ConnectionsHeld::new())),
    ));
    connections.held().lock().unwrap().set_topology_view(live.clone());
    let node_rpc = rdm_admin::node_rpc::ProcessNodeRpc::new(live.clone(), ep.clone());
    let server = rdm_admin::admin::rpc_server(connections, node_rpc.client.clone(), Arc::new(OnceLock::new()), Arc::new(OnceLock::new()))
        .build(ServedBirth { node_id: b.node.node_id.to_string(), incarnation: b.node.incarnation_id.clone().unwrap().to_string() })
        .unwrap();
    (resolved, Router::builder(ep).accept(rdm_transport::ALPN, server).spawn())
}

/// CONTRACT (node-config-update.md, a caller's update, step 2: "The node's property writer applies
/// the whole map to its latest owned state and issues one put"; a node-admin is a node like any other):
/// `ApplyNodeConfig` naming this admin is answered by the admin itself, whoever sends it (node-rpc.md
/// §14): `Applied`, after its own writer put the whole map on its own property record, from another
/// node-admin and from a node alike. One naming another incarnation of it is `RejectedStaleIncarnation`
/// with the one held, and writes nothing.
#[test]
fn a_node_admin_told_its_own_config_holds_it_and_answers_applied() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (admin2, rpc1) = (&b["admin2"], &b["rpc1"]);
        quiet_node_configs(&rig).await;
        let config: BTreeMap<String, String> = [("admin_api_base".to_string(), "http://127.0.0.1:9".to_string())].into();
        let later: BTreeMap<String, String> = [("admin_api_base".to_string(), "http://127.0.0.1:10".to_string())].into();
        let me = &rig.admin.node;
        let told = |incarnation: IncarnationId, config: BTreeMap<String, String>| StatusRequest::ApplyNodeConfig { node_id: me.node_id.clone(), incarnation, config };
        assert_eq!(reply(&call(&rig, admin2, &told(me.incarnation_id.clone().unwrap(), config.clone())).await), StatusReply::Applied);
        assert_eq!(props(&rig, &me.node_id).await.0, config, "the admin's own record holds the whole config it was told");
        assert_eq!(reply(&call(&rig, admin2, &told(IncarnationId::mint(), later.clone())).await), StatusReply::RejectedStaleIncarnation { held: me.incarnation_id.clone().unwrap() });
        assert_eq!(props(&rig, &me.node_id).await.0, config, "the refusal wrote nothing");
        assert_eq!(reply(&call(&rig, rpc1, &told(me.incarnation_id.clone().unwrap(), later.clone())).await), StatusReply::Applied, "the subject answers whoever sends it");
        assert_eq!(props(&rig, &me.node_id).await.0, later, "each apply puts the whole map");
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

/// A successor born `mesh1.node.8`, live in the mesh-primary's view, serving Status that answers
/// every request `Applied` and records it; the mesh-primary's node-RPC client resolves it.
async fn successor_new(rig: &Rig) -> (Birth, ServedNode) {
    let b = birth(rdm_core::PathName::new("mesh1.node", "8").unwrap(), "node", "mesh1", false, false);
    rig.topology.write().await.nodes.push(b.node.clone());
    rig.nodes.put_node(&own_record(&b, b.node.status, &[])).await.unwrap();
    let node = serve_node(&b).await;
    node_configs(rig, &node).await;
    (b, node)
}

/// [`successor_new`], its connections deleting (connections-deleting in the mesh-primary's view and its
/// declared state), answering `node.connection.delete` with every connection cut and nothing held, and
/// every other request `Applied`.
async fn successor_connections_deleted(rig: &Rig) -> (Birth, ServedNode) {
    let mut b = birth(rdm_core::PathName::new("mesh1.node", "8").unwrap(), "node", "mesh1", false, false);
    (b.node.status, b.node.declared) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()));
    rig.topology.write().await.nodes.push(b.node.clone());
    rig.nodes.put_node(&own_record(&b, b.node.status, &[])).await.unwrap();
    let node = serve_node_with(&b, |_| StatusReply::Applied).await;
    node_configs(rig, &node).await;
    (b, node)
}

fn delete_all(b: &Birth) -> rdm_core::node_command::NodeCommandRequest {
    rdm_core::node_command::NodeCommandRequest::DeleteConnections { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), scope: rdm_core::status::ConnectionScope::All }
}

/// The caller's `node.connection.delete { All }` of `b`, as the mesh-primary's `StatusService` runs it: its carrier
/// to the exact birth (D18).
async fn deleted_all(rig: &Rig, b: &Birth) -> rdm_admin::status_rpc::CarryEnd {
    rig.service.carry_connections_delete(&b.node.node_id, b.node.incarnation_id.as_ref().unwrap(), &rdm_core::status::ConnectionScope::All).await
}

fn update_name(b: &Birth, name: &str) -> StatusRequest {
    StatusRequest::UpdateName { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), name: PathName { path: b.node.path.clone(), name: name.into() } }
}

/// The span of a mesh-primary's `node.name.update`.
const NAME_SPAN: &str = "rdm.node_admin.name.update.via-writer";

/// `b`'s own property record in [`node_records`] carrying `tags`, as its own writer put them.
async fn own_tags(b: &Birth, tags: &[&str]) {
    use rdm_admin::storage::NodesStorage as _;
    node_records().put_node(&own_record(b, NodeStatus::Ready, tags)).await.unwrap();
}

/// `b` stopped in the mesh-primary's view (`Leaving`), its own record at its name carrying `tags`.
async fn stopped_with_row(rig: &Rig, b: &Birth, tags: &[&str]) {
    own_tags(b, tags).await;
    if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == b.node.node_id) {
        (n.stopped, n.status) = (true, NodeStatus::Leaving);
    }
    rig.nodes.put_node(&own_record(b, NodeStatus::Leaving, tags)).await.unwrap();
}

/// The mesh-primary holds `b` as a rename needs it (node-name-update.md Precondition): live and
/// connections-deleting in its view, its `node.connections.deleted` of an `All` held with nothing held.
async fn hold_connections_deleted(rig: &Rig, b: &Birth) {
    if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == b.node.node_id) {
        (n.status, n.declared, n.stopped) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()), false);
    }
    let birth = (b.node.node_id.clone(), b.node.incarnation_id.clone().unwrap());
    rig.service.declared.lock().unwrap().connections_deleted.insert(birth, rdm_admin::status_rpc::ConnectionsDeletedReceipt { all: true, held: vec![], status_at_cut: NodeStatus::ConnectionsDeleting });
}

/// `b` live with no connection as a rename needs it ([`hold_connections_deleted`]), its own record at
/// its name carrying `tags`; served, answering every request `Applied`, and mesh1's primary's `NodeConfigs`
/// resolving it.
async fn deleted_with_row(rig: &Rig, b: &Birth, tags: &[&str]) -> ServedNode {
    hold_connections_deleted(rig, b).await;
    own_tags(b, tags).await;
    rig.nodes.put_node(&own_record(b, NodeStatus::ConnectionsDeleting, tags)).await.unwrap();
    let node = serve_node(b).await;
    node_configs(rig, &node).await;
    node
}

fn apply_name(b: &Birth, to: &PathName) -> StatusRequest {
    StatusRequest::ApplyNodeName { node_id: b.node.node_id.clone(), incarnation: b.node.incarnation_id.clone().unwrap(), name: to.clone() }
}

/// Each span's `(outcome, connections)`.
fn outcomes_and_connections(spans: &[(String, BTreeMap<String, String>)]) -> Vec<(String, String)> {
    spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("connections").cloned().unwrap_or_default())).collect()
}

/// CONTRACT (proposal node-properties-own-commands.md §2.1 Precondition and Validation: "A
/// ready node, or one still holding a connection, is refused"; "the precondition holds,
/// else `RejectedAdmittingConnections { status, held_connections }`"; §2.4 "Refused before the claim
/// ...: nothing written, nothing changed"): a caller's `node.name.update` of the live,
/// ready successor `mesh1.node.8` to the name `3` at mesh1's primary is refused
/// `RejectedAdmittingConnections { status: Ready, held_connections: [] }`. Nothing is claimed
/// (no row at `mesh1.node.3`), the view keeps the birth at `mesh1.node.8`, and the birth is told nothing. One
/// `rdm.node_admin.name.update.via-writer` span says `rejected-admitting-connections` with
/// `traffic=Ready` and `connections=none-deleted`. What must NOT happen: the row written, the
/// birth told `ApplyNodeName`, or `NameUpdated`.
#[test]
fn a_ready_nodes_rename_is_refused_admitting_connections_and_nothing_is_written_or_told() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_new(&rig).await;
        let reply = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(reply, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::Ready, held_connections: vec![] });
        let taken: PathName = rdm_core::PathName::new("mesh1.node", "3").unwrap();
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap(), None, "nothing is claimed at the new name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == succ.node.node_id && n.location() == succ.node.location()), "the view keeps the birth at its name");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace_id, attrs) = &spans[0];
    assert_eq!(
        (attrs.get("outcome").map(String::as_str), attrs.get("traffic").map(String::as_str), attrs.get("connections").map(String::as_str), attrs.get("answered")),
        (Some("rejected-admitting-connections"), Some("Ready"), Some("none-deleted"), None),
        "{attrs:?}"
    );
    eprintln!("trace {trace_id}: {NAME_SPAN} from=mesh1.node.8 name=3 traffic=Ready connections=none-deleted outcome=rejected-admitting-connections");
}

/// CONTRACT (node-name-update.md Precondition: "The node holds no connection: it is stopped, or this
/// mesh-primary holds the birth's `node.connection.deleted` of an `All` that left zero connections
/// held"; Check and move; steps 4-6; Completion by case, "The live node: `node.name.updated` after
/// `Applied` or `AlreadyApplied` and its `MeshJoined`"; Q347): the successor `mesh1.node.8`,
/// ConnectionsDeleting in the mesh-primary's view, is refused the rename to `3`
/// `RejectedAdmittingConnections { status: ConnectionsDeleting, held_connections: [] }`, nothing
/// claimed and nothing told. A caller's `node.connection.delete { All }` through the mesh-primary is
/// carried to the exact birth and answered with the birth's `node.connections.deleted`, which the
/// mesh-primary holds; the same `node.name.update`, the birth still on its mesh, then puts the row at
/// `mesh1.node.3` carrying `renamed_from: mesh1.node.8`, releases the old key, tells the birth
/// `ApplyNodeName`, commands its `node.mesh.join` at `mesh1.node.3` once, and ends `NameUpdated` on its
/// `MeshJoined`. A start begins the birth's next cycle: no receipt is carried over, and a rename after
/// it is refused again. What must NOT happen: a node whose connections were not deleted taking another
/// name, a rename completed before its rejoin, or a started one renamed on its last cycle's receipt.
// @feature: node-name-update
#[test]
fn a_nodes_rename_is_refused_until_its_connections_are_deleted_then_applied_and_the_node_rejoins_under_it() {
    let cap = capture();
    let successor = cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_connections_deleted(&rig).await;
        let taken: PathName = rdm_core::PathName::new("mesh1.node", "3").unwrap();
        let refused = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(refused, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::ConnectionsDeleting, held_connections: vec![] });
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap(), None, "nothing is claimed at the new name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");

        let deleted = deleted_all(&rig, &succ).await;
        assert!(matches!(&deleted, rdm_admin::status_rpc::CarryEnd::Deleted { held, .. } if held.is_empty()), "the mesh-primary holds the birth's node.connections.deleted: {deleted:?}");
        assert_eq!(node.deletes.lock().unwrap().clone(), vec![delete_all(&succ)], "the delete is carried to the exact birth");
        assert!(node.seen.lock().unwrap().is_empty(), "and nothing else");
        let applied = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(applied, StatusReply::NameUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() });
        assert_eq!(node.seen.lock().unwrap().last().cloned(), Some(apply_name(&succ, &taken)), "the birth is told its name");
        assert_eq!(rig.rejoins.lock().unwrap().clone(), vec![(succ.node.node_id.clone(), taken.clone())], "the birth is commanded back on its mesh under the new name, once");

        // A start begins the birth's next cycle: its last cycle's receipt does not carry over.
        rig.service.begin_cycle(&succ.node, succ.node.incarnation_id.as_ref().unwrap()).await;
        let again = reply(&call(&rig, &b["admin2"], &update_name(&succ, "8")).await);
        assert!(matches!(&again, StatusReply::RejectedAdmittingConnections { held_connections, .. } if held_connections.is_empty()), "a started birth is refused until its connections are deleted again: {again:?}");
        succ.node.node_id.to_string()
    });
    let _ = successor;
    let spans = cap.spans(NAME_SPAN);
    let labels_of = |spans: &[(String, BTreeMap<String, String>)]| spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("connections").cloned().unwrap_or_default(), a.get("rejoin").cloned().unwrap_or_default())).collect::<Vec<_>>();
    let s = |v: &str| v.to_string();
    assert_eq!(
        labels_of(&spans),
        vec![
            (s("rejected-admitting-connections"), s("none-deleted"), s("")),
            (s("name-updated"), s("deleted"), s("mesh-joined")),
            (s("rejected-admitting-connections"), s("none-deleted"), s("")),
        ],
        "{spans:?}"
    );
    assert_eq!(spans[1].1.get("answered").map(String::as_str), Some("applied"), "{spans:?}");
    let carried = cap.spans("rdm.node_admin.connection.delete.via-mesh-primary");
    assert_eq!(carried.len(), 1, "{carried:?}");
    assert_eq!((carried[0].1.get("scope").map(String::as_str), carried[0].1.get("status_at_cut").map(String::as_str), carried[0].1.get("outcome").map(String::as_str)), (Some("all"), Some("ConnectionsDeleting"), Some("connections-deleted")), "{carried:?}");
    eprintln!("trace {}: rdm.node_admin.connection.delete.via-mesh-primary scope=all status_at_cut=ConnectionsDeleting outcome=connections-deleted", carried[0].0);
    for (trace, attrs) in &spans {
        eprintln!("trace {trace}: {NAME_SPAN} outcome={:?} connections={:?} rejoin={:?} traffic={:?} to={:?} answered={:?}", attrs.get("outcome"), attrs.get("connections"), attrs.get("rejoin"), attrs.get("traffic"), attrs.get("to"), attrs.get("answered"));
    }
}

/// CONTRACT (proposal §7: "connections-deleted node still holding one connection: refused
/// `RejectedAdmittingConnections { held_connections: [id] }`, nothing written"): the successor
/// `mesh1.node.8`, ConnectionsDeleting, whose mesh-primary holds its `node.connections.deleted` of an `All`
/// that left one connection held, is refused the rename to `3` naming that connection by id. No row
/// at `mesh1.node.3`, nothing told. The span says `connections=held`.
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
        rig.service.declared.lock().unwrap().connections_deleted.insert((succ.node.node_id.clone(), inc), rdm_admin::status_rpc::ConnectionsDeletedReceipt { all: true, held: vec![held_id.clone()], status_at_cut: NodeStatus::ConnectionsDeleting });
        let refused = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(refused, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::ConnectionsDeleting, held_connections: vec![held_id.clone()] });
        assert_eq!(rig.nodes.record_at(&rdm_core::PathName::new("mesh1.node", "3").unwrap()).await.unwrap(), None, "nothing is claimed");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(outcomes_and_connections(&spans), vec![("rejected-admitting-connections".to_string(), "held".to_string())], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} traffic=ConnectionsDeleting connections=held held_connections={held_id} outcome=rejected-admitting-connections", spans[0].0);
}

/// CONTRACT (proposal §2.1 Claim and move: "a held name answers `NameHeld { by: node.id }` and
/// nothing is written"): a rename of a successor with no connection (its connections deleted) into a
/// name another node holds is refused `NameHeld { by }` by its claim; nothing is written and the birth
/// is told nothing. A live node that can carry traffic renamed to `renamed` is refused
/// `RejectedAdmittingConnections` naming its status.
#[test]
fn a_rename_into_a_held_name_is_refused_name_held_and_nothing_is_written_or_told() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_new(&rig).await;
        // Live with no connection: the name check is the claim's alone.
        hold_connections_deleted(&rig, &succ).await;
        let holder = NodeId::mint();
        let taken: PathName = rdm_core::PathName::new("mesh1.node", "3").unwrap();
        rig.nodes.put_node(&rdm_node::nodes_storage::fixture_record(&rdm_core::PathName::new(taken.path.clone(), taken.name.clone()).unwrap(), "node", holder.clone(), rdm_core::IncarnationId::mint(), NodeStatus::Leaving)).await.unwrap();
        let held = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(held, StatusReply::NameHeld { path: taken.path.clone(), name: taken.name.clone(), by: holder.clone() });
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap().map(|r| r.node_id), Some(holder), "the holder keeps the name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
        let routed = &b["rpc1"];
        let refused = reply(&call(&rig, &b["admin2"], &update_name(routed, "renamed")).await);
        assert_eq!(refused, StatusReply::RejectedAdmittingConnections { status: rdm_core::views::NodeStatus::Ready, held_connections: vec![] }, "a live node that can carry traffic keeps its name");
    });
    let spans = cap.spans(NAME_SPAN);
    let outcomes: Vec<_> = spans.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect();
    assert_eq!(outcomes, vec!["name-held".to_string(), "rejected-admitting-connections".to_string()], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} to=mesh1.node.3 outcome=name-held", spans[0].0);
}

/// CONTRACT (proposal §2.1 Caller / executor: "Caller to the node's mesh-primary ... (Forward `0x1A`
/// when not directly reachable)"): a caller of another mesh whose client reaches mesh1's primary only
/// through a carrier (mesh1.node-admin.2) renames the successor `mesh1.node.8`, whose
/// `node.connections.deleted` mesh1's primary holds, to the name `3` with `node.name.update`: the call
/// ends `NameUpdated` naming the exact birth, the row at `mesh1.node.3` holds it, the birth was told
/// `ApplyNodeName` once and commanded back on its mesh; the route span says `via-peer` with the carrier, and the mesh-primary's
/// `via-writer` span says `name-updated` after the apply answered `applied`. What must NOT happen: the
/// call ending `NotBackedToday` on the caller.
#[test]
fn a_callers_rename_reaches_the_nodes_mesh_primary_through_a_carrier_and_ends_name_updated() {
    let cap = capture();
    let carrier_id = cap.run(async {
        let (rig, b) = rig().await;
        let (succ, node) = successor_connections_deleted(&rig).await;
        assert!(matches!(deleted_all(&rig, &succ).await, rdm_admin::status_rpc::CarryEnd::Deleted { .. }), "the successor's connections are deleted first");
        let (carrier, _carrier_router) = carrier_admin(&b["admin2"], &rig.resolved).await;
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(carrier.clone());
        let caller = &b["m2admin"];
        let ep = rdm_transport::endpoint::bind(caller.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm");
        let route = rdm_transport::RouteChoice::ViaPeer { carrier: carrier.node_id.clone() };
        let taken: PathName = rdm_core::PathName::new("mesh1.node", "3").unwrap();
        let reply = rdm_client::NodeRpc::new(&client).name_update(&rig.admin.node.node_id, &route, &exact(&succ), &taken, &CallOptions::default()).await.expect("a typed reply");
        assert_eq!(reply, StatusReply::NameUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() });
        let row = rig.nodes.record_at(&taken).await.unwrap().expect("the mesh-primary wrote the row at the new name");
        assert_eq!(row.node_id, succ.node.node_id);
        assert_eq!(node.deletes.lock().unwrap().clone(), vec![delete_all(&succ)]);
        assert_eq!(node.seen.lock().unwrap().clone(), vec![apply_name(&succ, &taken)]);
        assert_eq!(rig.rejoins.lock().unwrap().clone(), vec![(succ.node.node_id.clone(), taken.clone())]);
        b["admin2"].node.node_id.to_string()
    });
    let updates = cap.spans(NAME_SPAN);
    assert_eq!(updates.len(), 1, "{updates:?}");
    let (mp_trace, attrs) = &updates[0];
    assert_eq!((attrs.get("outcome").map(String::as_str), attrs.get("answered").map(String::as_str)), (Some("name-updated"), Some("applied")), "{attrs:?}");
    let routes = cap.spans("rdm.node_rpc.route.resolve.via-connections");
    let carried: Vec<_> = routes.iter().filter(|(_, a)| a.get("route").map(String::as_str) == Some("via-peer")).collect();
    assert_eq!(carried.len(), 1, "one carried leg: {routes:?}");
    assert_eq!((carried[0].1.get("carrier").map(String::as_str), carried[0].1.get("protocol").map(String::as_str)), (Some(carrier_id.as_str()), Some("status")));
    eprintln!("trace {}: rdm.node_rpc.route.resolve.via-connections route=via-peer carrier=mesh1.node-admin.2 protocol=status; trace {mp_trace}: {NAME_SPAN} answered=applied outcome=name-updated", carried[0].0);
}

/// CONTRACT (node-name-update.md Precondition: "The node holds no connection: it is stopped, or ...";
/// Completion by case, "The stopped node: `node.name.updated` after `Applied` or `AlreadyApplied`; its
/// start rejoins it under the name"; Q345: "the old node is stopped, and its name changed with
/// node.name.update"): `node.name.update` renaming the stopped `mesh1.node.2` (stopped in the
/// mesh-primary's view, its row at `mesh1.node.2`), its process bound and served, to `kept` ends
/// `NameUpdated`: the row is at `mesh1.node.kept`, `mesh1.node.2` is released, the view lists the node
/// at the new name, and the birth is told `ApplyNodeName` once and commanded no join (its start
/// rejoins it). One `via-writer` span says `name-updated` with `traffic=Stopped`,
/// `connections=stopped` and `rejoin=at-start`. What must NOT happen: a refusal, or a join commanded
/// to a stopped node.
#[test]
fn a_stopped_nodes_rename_is_applied_and_its_start_rejoins_it() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let stopped = &b["rpc2"];
        let node = serve_node(stopped).await;
        node_configs(&rig, &node).await;
        stopped_with_row(&rig, stopped, &[]).await;
        let to: PathName = rdm_core::PathName::new("mesh1.node", "kept").unwrap();
        let renamed = reply(&call(&rig, &b["admin2"], &update_name(stopped, "kept")).await);
        assert_eq!(renamed, StatusReply::NameUpdated { node_id: stopped.node.node_id.clone(), incarnation: stopped.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.record_at(&to).await.unwrap().map(|r| r.node_id), Some(stopped.node.node_id.clone()), "the row is at the new name");
        assert_eq!(rig.nodes.record_at(&stopped.node.location()).await.unwrap(), None, "the old key is released");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == stopped.node.node_id && n.location() == to), "the view lists the stopped node at the new name");
        assert_eq!(node.seen.lock().unwrap().clone(), vec![apply_name(stopped, &to)], "the bound process is told its name");
        assert!(rig.rejoins.lock().unwrap().is_empty(), "a stopped node is commanded no join: its start rejoins it");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.len(), 1, "{spans:?}");
    let (trace, attrs) = &spans[0];
    assert_eq!(
        (attrs.get("outcome").map(String::as_str), attrs.get("traffic").map(String::as_str), attrs.get("connections").map(String::as_str), attrs.get("rejoin").map(String::as_str)),
        (Some("name-updated"), Some("Stopped"), Some("stopped"), Some("at-start")),
        "{attrs:?}"
    );
    eprintln!("trace {trace}: {NAME_SPAN} from=mesh1.node.2 to=mesh1.node.kept traffic=Stopped connections=stopped rejoin=at-start outcome=name-updated");
}

/// CONTRACT (proposal §9, ruled A: "a rename may take any name within the name grammar ..., unique
/// within its path ... Cell: a stopped node renamed to an arbitrary name within the same path
/// succeeds"; node-name-update.md Precondition): the live `mesh1.node.2`, its connections deleted,
/// renamed to the name `9` ends `NameUpdated`: the row is at `mesh1.node.9`,
/// `mesh1.node.2` is released, the view lists the node at `mesh1.node.9`, and the birth is told
/// `ApplyNodeName { mesh1.node.9 }`. What must NOT happen: a refusal because `9` is not derived from
/// the old name.
#[test]
fn a_node_with_no_connection_renamed_to_any_name_within_its_path_ends_name_updated() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let left = &b["rpc2"];
        let node = deleted_with_row(&rig, left, &[]).await;
        let to: PathName = rdm_core::PathName::new("mesh1.node", "9").unwrap();
        let reply = reply(&call(&rig, &b["admin2"], &update_name(left, "9")).await);
        assert_eq!(reply, StatusReply::NameUpdated { node_id: left.node.node_id.clone(), incarnation: left.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.record_at(&to).await.unwrap().map(|r| r.node_id), Some(left.node.node_id.clone()), "the row is at the new name");
        assert_eq!(rig.nodes.record_at(&left.node.location()).await.unwrap(), None, "the old key is released");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == left.node.node_id && n.location() == to), "the view lists the node at the new name");
        assert_eq!(node.seen.lock().unwrap().clone(), vec![apply_name(left, &to)], "the birth is told its name");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("to").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("name-updated".to_string(), "mesh1.node.9".to_string())], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} from=mesh1.node.2 name=9 to=mesh1.node.9 outcome=name-updated", spans[0].0);
}

/// CONTRACT (proposal §2.1 Validation: "the name is within the name grammar, else `RejectedName {
/// reason }`"; §4: path and mesh immutable by construction; §2.4 "Refused before the claim ...: nothing
/// written"): `mesh1.node.2` with no connection (its connections deleted) asked for the names
/// `Two` (outside the grammar), `rpc.2` and
/// `2.older` (whose join at its path `mesh1.node` is no `path.name` this build keys) is refused
/// `RejectedName` naming the name each time; its row stays at `mesh1.node.2`, its id index names that
/// key, and the view keeps it there. What must NOT happen: a row written at any other key, or the path
/// moved.
#[test]
fn a_name_outside_the_grammar_or_the_path_name_shape_is_refused_by_name_and_nothing_is_written() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let left = &b["rpc2"];
        let node = deleted_with_row(&rig, left, &[]).await;
        for bad in ["Two", "rpc.2", "2.older"] {
            let refused = reply(&call(&rig, &b["admin2"], &update_name(left, bad)).await);
            assert!(matches!(&refused, StatusReply::RejectedName { reason } if reason.contains(bad)), "{bad}: {refused:?}");
        }
        assert_eq!(rig.nodes.node(&left.node.node_id).await.unwrap().map(|l| l.record.name), Some(left.node.name.clone()), "the node's record stays at its name");
        assert!(rig.topology.read().await.nodes.iter().any(|n| n.node_id == left.node.node_id && n.location() == left.node.location()), "the view keeps the node at its name");
        assert!(node.seen.lock().unwrap().is_empty(), "the birth is told nothing");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["rejected-name"; 3], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} name=Two outcome=rejected-name", spans[0].0);
}

/// CONTRACT (proposal §2.1 Idempotency: "A repeat naming the name the row already holds ... for a
/// live node, resends `ApplyNodeName` and completes on `Applied` or `AlreadyApplied`"; §2.3 "Row
/// written, apply unresolved ... the resubmit resends `ApplyNodeName`, and completes on `Applied` or
/// `AlreadyApplied` from the node, never from the row"): the successor `mesh1.node.8`, its
/// connections deleted, renamed to `3` answers the apply `NotReady`: the command ends `NameNotApplied {
/// node: mesh1.node.3, answered: not-ready }` with the row moved. The same command again resends
/// `ApplyNodeName`, which the node answers `AlreadyApplied`, and ends `NameUpdated`. What must NOT
/// happen: `NameUpdated` on the row alone, or the resubmit refused because the row already holds the name.
#[test]
fn a_same_name_retry_after_an_unresolved_apply_resends_the_apply_and_completes_on_the_nodes_answer() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let mut succ = birth(rdm_core::PathName::new("mesh1.node", "8").unwrap(), "node", "mesh1", false, false);
        (succ.node.status, succ.node.declared) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()));
        rig.topology.write().await.nodes.push(succ.node.clone());
        rig.nodes.put_node(&own_record(&succ, succ.node.status, &[])).await.unwrap();
        let applies = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let a = applies.clone();
        let node = serve_node_with(&succ, move |req| match req {
            StatusRequest::ApplyNodeName { .. } if a.fetch_add(1, Ordering::SeqCst) == 0 => StatusReply::NotReady { reason: "the node's digest does not exist yet".into() },
            _ => StatusReply::AlreadyApplied,
        })
        .await;
        node_configs(&rig, &node).await;
        assert!(matches!(deleted_all(&rig, &succ).await, rdm_admin::status_rpc::CarryEnd::Deleted { .. }));
        let taken: PathName = rdm_core::PathName::new("mesh1.node", "3").unwrap();
        let incomplete = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert!(matches!(&incomplete, StatusReply::NameNotApplied { node, answered, .. } if node == succ.node.node_id.as_str() && answered == "not-ready"), "{incomplete:?}");
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap(), None, "the node, whose apply did not answer, wrote nothing (Q356)");
        let done = reply(&call(&rig, &b["admin2"], &update_name(&succ, "3")).await);
        assert_eq!(done, StatusReply::NameUpdated { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap() });
        assert_eq!(rig.nodes.record_at(&taken).await.unwrap().map(|r| r.node_id), Some(succ.node.node_id.clone()), "the node wrote its record at the new name on the resent apply");
        let apply = StatusRequest::ApplyNodeName { node_id: succ.node.node_id.clone(), incarnation: succ.node.incarnation_id.clone().unwrap(), name: taken.clone() };
        assert_eq!(node.deletes.lock().unwrap().clone(), vec![delete_all(&succ)]);
        assert_eq!(node.seen.lock().unwrap().clone(), vec![apply.clone(), apply], "the resubmit resent the apply");
        assert_eq!(rig.rejoins.lock().unwrap().clone(), vec![(succ.node.node_id.clone(), taken.clone())], "the birth is commanded back on its mesh once, after the apply answered");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("answered").cloned().unwrap_or_default())).collect::<Vec<_>>(), vec![("name-not-applied".to_string(), "not-ready".to_string()), ("name-updated".to_string(), "already-applied".to_string())], "{spans:?}");
    eprintln!("trace {}: {NAME_SPAN} to=mesh1.node.3 answered=not-ready outcome=name-not-applied; trace {}: {NAME_SPAN} answered=already-applied outcome=name-updated", spans[0].0, spans[1].0);
}

fn labels(l: &[&str]) -> std::collections::BTreeSet<String> {
    l.iter().map(|s| s.to_string()).collect()
}

/// CONTRACT (Q171: no workflow waits for the initial fabric's first ready; proposal
/// node-properties-own-commands.md §1): while the initial fabric is pending and has never been
/// ready, a caller's `node.name.update` of `mesh1.node.2` with no connection (its connections
/// deleted) at its mesh-primary ends `NameUpdated`: the row moves to `mesh1.node.renamed`. What must
/// NOT happen: a refusal for a fabric not yet ready.
#[test]
fn a_node_name_update_before_the_fabric_first_reaches_ready_is_admitted() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let rpc2 = &b["rpc2"];
        let _node = deleted_with_row(&rig, rpc2, &[]).await;
        rig.topology.write().await.fabric.status = ScopeStatus::Pending;
        let to: PathName = rdm_core::PathName::new("mesh1.node", "renamed").unwrap();
        let renamed = reply(&call(&rig, &b["admin2"], &update_name(rpc2, "renamed")).await);
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

/// CONTRACT (`docs/architecture/node-rpc-envelope.md:323` — "`node.tag.create { label | [labels] }` adds
/// one label or a list; `node.tag.delete { All | [labels] }` removes every label or the named ones ...
/// Each label is its own keyed row (label, node id) in the node store's tags index. Create is a blind put
/// of that key, so creating a label the node already carries is ignored and writes no duplicate; delete
/// tombstones the named keys. A label is 1-64 characters of a-z 0-9 - _ . ~, with no leading '.', and a
/// node carries at most 32; a label outside that grammar, or one past the 32, is refused by name and
/// nothing is written" and `docs/architecture/node-rpc-envelope.md:365` — "Additive and removal commands
/// on keyed label rows, not a replacement of a tags property"; proposal node-properties-own-commands.md §1: "Each command goes to the node's
/// mesh-primary as the row writer"; Q166, Q171: each is admitted at any time): while the initial fabric is
/// pending and has never been ready, a caller's `node.config.update` {zone: a} of
/// `mesh1.node.1` at its mesh-primary ends `ConfigUpdated`, the birth is sent `ApplyNodeConfig` with it
/// and its own record carries the map (and no tag). Then, each put by the node's own writer: create
/// {rabbit} ends `TagsCreated`; create {rabbit, red} names rabbit already carried; delete {rabbit, blue}
/// names blue not carried; a create that would carry the node past 32 labels is refused `RejectedTags`
/// naming the count and writes nothing; delete `All` leaves the record with no label. Each tags
/// command's `via-writer` span ends with the reply's name. What must NOT happen: a duplicate label, a
/// label past the cap, or a command refused for a fabric not yet ready.
// @feature: node-tags
#[test]
fn node_tags_commands_and_a_config_update_before_the_fabric_first_reaches_ready_write_the_nodes_own_record() {
    use rdm_core::status::TagScope;
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let rpc1 = &b["rpc1"];
        let node = serve_node(rpc1).await;
        node_configs(&rig, &node).await;
        rig.topology.write().await.fabric.status = ScopeStatus::Pending;
        let zone: BTreeMap<String, String> = [("zone".to_string(), "a".to_string())].into();
        let config = StatusRequest::UpdateNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config: zone.clone() };
        assert_eq!(updated(&reply(&call(&rig, &b["admin2"], &config).await)), updated_of(rpc1, &zone), "node.config.update is admitted before the first ready");
        assert_eq!(props(&rig, &rpc1.node.node_id).await, (zone.clone(), Default::default()), "the node's own record carries the map and no tag");
        assert_eq!(node.seen.lock().unwrap().drain(..).collect::<Vec<_>>(), vec![StatusRequest::ApplyNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config: zone }], "the birth is sent the map");

        let (id, birth) = (rpc1.node.node_id.clone(), rpc1.node.incarnation_id.clone().unwrap());
        let created = |already: &[&str]| StatusReply::TagsCreated { node_id: id.clone(), incarnation: birth.clone(), already_carried: labels(already) };
        let deleted = |not: &[&str]| StatusReply::TagsDeleted { node_id: id.clone(), incarnation: birth.clone(), not_carried: labels(not) };
        let row_tags = || async { props(&rig, &rpc1.node.node_id).await.1 };
        assert_eq!(reply(&call(&rig, &b["admin2"], &create_tags(rpc1, &["rabbit"])).await), created(&[]));
        assert_eq!(row_tags().await, labels(&["rabbit"]));
        assert_eq!(reply(&call(&rig, &b["admin2"], &create_tags(rpc1, &["rabbit", "red"])).await), created(&["rabbit"]));
        assert_eq!(row_tags().await, labels(&["rabbit", "red"]));
        assert_eq!(reply(&call(&rig, &b["admin2"], &delete_tags(rpc1, TagScope::Labels(labels(&["rabbit", "blue"])))).await), deleted(&["blue"]));
        assert_eq!(row_tags().await, labels(&["red"]));
        let thirty_two: Vec<String> = (0..32).map(|i| format!("t{i}")).collect();
        let over = reply(&call(&rig, &b["admin2"], &create_tags(rpc1, &thirty_two.iter().map(String::as_str).collect::<Vec<_>>())).await);
        assert!(matches!(&over, StatusReply::RejectedTags { reason } if reason.contains("33 tag labels, over the 32")), "a create past the cap is refused naming the count: {over:?}");
        assert_eq!(row_tags().await, labels(&["red"]), "a refused create writes nothing");
        assert_eq!(reply(&call(&rig, &b["admin2"], &delete_tags(rpc1, TagScope::All)).await), deleted(&[]));
        assert!(row_tags().await.is_empty(), "the row carries no label");
        assert!(node.seen.lock().unwrap().iter().all(|r| matches!(r, StatusRequest::CreateTags { .. } | StatusRequest::DeleteTags { .. })), "only the tags commands reach the node's own writer: {:?}", node.seen.lock().unwrap());
    });
    let outcomes = |name: &str| cap.spans(name).iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>();
    assert_eq!(outcomes("rdm.node_admin.tag.create.via-writer"), vec!["tags-created", "tags-created", "rejected-tags"]);
    assert_eq!(outcomes("rdm.node_admin.tag.delete.via-writer"), vec!["tags-deleted", "tags-deleted"]);
    let created = cap.spans("rdm.node_admin.tag.create.via-writer");
    eprintln!("trace {}: rdm.node_admin.tag.create.via-writer outcome=tags-created arrived=rabbit", created[0].0);
}

/// CONTRACT (node-config-update.md: "The same writer serializes local changes, caller commands, tags
/// ...; Config and tags are independent fields; changing config does not replace tags"; proposal §3:
/// tags and config: "Neither has a precondition ..., neither touches the name"): a caller's
/// `node.tag.create { rabbit, red }` of the live `mesh1.node.1` reaches the birth once and its own
/// writer puts the labels on its own record. A `node.config.update` of rpc.1 then puts the new config
/// and the record keeps the same tags. The left `mesh1.node.2` (its connections deleted, its mesh left),
/// whose own record carries {red}, renamed to `2-old` keeps its tags: its record is keyed by its node
/// id. What must NOT happen: a tags command sending `ApplyNodeName`, or a config put or a rename
/// dropping the tags.
#[test]
fn a_tags_create_tells_the_node_nothing_and_a_config_put_or_a_rename_keeps_the_tags() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let (rpc1, rpc2) = (&b["rpc1"], &b["rpc2"]);
        let (node, rpc2_node) = (serve_node(rpc1).await, serve_node(rpc2).await);
        node_configs_for(&rig, &[&node, &rpc2_node]).await;

        let created = reply(&call(&rig, &b["admin2"], &create_tags(rpc1, &["rabbit", "red"])).await);
        assert_eq!(created, StatusReply::TagsCreated { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), already_carried: Default::default() });
        assert_eq!(node.seen.lock().unwrap().clone(), vec![create_tags(rpc1, &["rabbit", "red"])], "the tags command reaches the birth once");
        assert_eq!(props(&rig, &rpc1.node.node_id).await.1, labels(&["rabbit", "red"]), "the node's own record holds the tags the create added");

        let config: BTreeMap<String, String> = [("zone".to_string(), "c".to_string())].into();
        let req = StatusRequest::UpdateNodeConfig { node_id: rpc1.node.node_id.clone(), incarnation: rpc1.node.incarnation_id.clone().unwrap(), config: config.clone() };
        assert_eq!(updated(&reply(&call(&rig, &b["admin2"], &req).await)), updated_of(rpc1, &config));
        assert_eq!(props(&rig, &rpc1.node.node_id).await, (config, labels(&["rabbit", "red"])), "the config put writes the new config and keeps the tags");

        hold_connections_deleted(&rig, rpc2).await;
        own_tags(rpc2, &["red"]).await;
        rig.nodes.put_node(&own_record(rpc2, NodeStatus::ConnectionsDeleting, &["red"])).await.unwrap();
        let renamed = reply(&call(&rig, &b["admin2"], &update_name(rpc2, "renamed")).await);
        assert_eq!(renamed, StatusReply::NameUpdated { node_id: rpc2.node.node_id.clone(), incarnation: rpc2.node.incarnation_id.clone().unwrap() });
        assert_eq!(props(&rig, &rpc2.node.node_id).await.1, labels(&["red"]), "the renamed node's own record keeps its tags");
        assert!(!rpc2_node.seen.lock().unwrap().iter().any(|r| matches!(r, StatusRequest::CreateTags { .. } | StatusRequest::DeleteTags { .. })), "a rename sends no tags command");
    });
    let spans = cap.spans(NAME_SPAN);
    assert_eq!(spans.iter().map(|(_, a)| (a.get("outcome").cloned().unwrap_or_default(), a.get("answered").cloned())).collect::<Vec<_>>(), vec![("name-updated".to_string(), Some("applied".to_string()))], "{spans:?}");
    let creates = cap.spans("rdm.node_admin.tag.create.via-writer");
    assert_eq!(creates.iter().map(|(_, a)| a.get("outcome").cloned().unwrap_or_default()).collect::<Vec<_>>(), vec!["tags-created".to_string()], "{creates:?}");
    eprintln!("trace {}: rdm.node_admin.tag.create.via-writer arrived=rabbit,red outcome=tags-created; trace {}: {NAME_SPAN} outcome=name-updated", creates[0].0, spans[0].0);
}

/// CONTRACT (proposal §2.2 step 6: "A live connections-deleted node is sent `ApplyNodeName`";
/// node_config.rs: the mesh-primary is a member of its mesh and the writer of its own row, and applies
/// its own config in-process): a caller's `node.name.update` renaming mesh1's primary itself, ConnectionsDeleting
/// and holding the `node.connections.deleted` of an `All` cut that left nothing held, to the name
/// `renamed` ends `NameUpdated`: the row moves, and the mesh-primary takes the name through its own name
/// decision in-process, which is asked exactly that name. What must NOT happen: the mesh-primary
/// calling itself over Node RPC ("Connecting to ourself is not supported") and ending `NameNotApplied`
/// with its row renamed and its name not.
#[test]
fn a_mesh_primarys_rename_of_itself_is_applied_in_process() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let node = serve_node(&b["rpc1"]).await;
        node_configs(&rig, &node).await;
        let me = &rig.admin;
        let inc = me.node.incarnation_id.clone().unwrap();
        if let Some(n) = rig.topology.write().await.nodes.iter_mut().find(|n| n.node_id == me.node.node_id) {
            (n.status, n.declared) = (NodeStatus::ConnectionsDeleting, Some("ConnectionsDeleting".into()));
        }
        rig.service.declared.lock().unwrap().connections_deleted.insert((me.node.node_id.clone(), inc.clone()), rdm_admin::status_rpc::ConnectionsDeletedReceipt { all: true, held: vec![], status_at_cut: NodeStatus::ConnectionsDeleting });
        let asked: Arc<Mutex<Vec<PathName>>> = Arc::default();
        let a = asked.clone();
        let held: PathName = me.node.location();
        let (own_store, own_id) = (rig.nodes.clone(), me.node.node_id.clone());
        let _ = rig.service.rename.set(Arc::new(move |name: PathName| {
            let (a, held, own_store, own_id) = (a.clone(), held.clone(), own_store.clone(), own_id.clone());
            Box::pin(async move {
                // Its own name decision writes its own record at its node.id with the new name.
                let line = own_store.node(&own_id).await.unwrap().expect("its own record");
                own_store.put_node(&rdm_admin::storage::NodeRecord { path: name.path.clone(), name: name.name.clone(), ..line.record }).await.unwrap();
                a.lock().unwrap().push(name);
                (held, rdm_admin::node_name::NameApply::Applied)
            })
        }));
        let to: PathName = rdm_core::PathName::new("mesh1.node-admin", "renamed").unwrap();
        let update = StatusRequest::UpdateName { node_id: me.node.node_id.clone(), incarnation: inc.clone(), name: to.clone() };
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
    eprintln!("trace {trace}: {NAME_SPAN} from=mesh1.node-admin.1 to=mesh1.node-admin.renamed apply=in-process answered=applied outcome=name-updated");
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
        let decisions = Arc::new(Mutex::new(vec![rdm_admin::node_name::NameApply::AlreadyApplied, rdm_admin::node_name::NameApply::Busy { connections: vec![rdm_core::ConnectionId::parse("3123456789ab").unwrap(), rdm_core::ConnectionId::parse("4123456789ab").unwrap()], routes: vec!["mesh1.node.1 (direct, entry 3123456789ab)".into(), "mesh1.node.2 (direct, entry 4123456789ab)".into()] }, rdm_admin::node_name::NameApply::AlreadyApplied, rdm_admin::node_name::NameApply::Applied]));
        let held: PathName = rig.admin.node.location();
        let _ = rig.service.rename.set(Arc::new(move |name: PathName| {
            let (a, d, held) = (a.clone(), decisions.clone(), held.clone());
            Box::pin(async move {
                a.lock().unwrap().push(name);
                (held, d.lock().unwrap().pop().unwrap())
            })
        }));
        let me = &rig.admin.node;
        let told = |incarnation: IncarnationId, name: PathName| StatusRequest::ApplyNodeName { node_id: me.node_id.clone(), incarnation, name };
        let inc = me.incarnation_id.clone().unwrap();
        assert_eq!(reply(&call(&rig, admin2, &told(inc.clone(), PathName::new("mesh1.node-admin", "1-old").unwrap())).await), StatusReply::Applied);
        assert_eq!(reply(&call(&rig, admin2, &told(inc.clone(), PathName::new("mesh1.node-admin", "1-old").unwrap())).await), StatusReply::AlreadyApplied);
        assert_eq!(
            reply(&call(&rig, admin2, &told(inc.clone(), PathName::new("mesh1.node-admin", "1-old").unwrap())).await),
            StatusReply::BusyHoldingConnections { connections: vec![rdm_core::ConnectionId::parse("3123456789ab").unwrap(), rdm_core::ConnectionId::parse("4123456789ab").unwrap()] }
        );
        assert_eq!(reply(&call(&rig, admin2, &told(IncarnationId::mint(), PathName::new("mesh1.node-admin", "1-old").unwrap())).await), StatusReply::RejectedStaleIncarnation { held: inc.clone() });
        assert_eq!(reply(&call(&rig, rpc1, &told(inc.clone(), PathName::new("mesh1.node-admin", "1-old").unwrap())).await), StatusReply::AlreadyApplied, "the subject answers a member from its own decision");
        assert!(matches!(reply(&call(&rig, admin2, &told(inc, PathName { path: "mesh1.node-admin".into(), name: "not a name".into() })).await), StatusReply::Malformed { .. }));
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
/// path.name as the view holds it) and the caller. What must NOT happen: NotReady (the readiness gate is open), the open stop completed, or a
/// completion accepted.
#[test]
fn a_node_left_call_is_refused_by_name_and_completes_no_open_stop() {
    let cap = capture();
    let operation = rdm_core::OperationId::node("node.stop", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap()));
    let reported = format!("node-left {operation:?} (build 66h8qrqvaajk)");
    let (stop_open, reply, admin, rpc) = cap.run(async {
        let (rig, b) = rig().await;
        let rpc1 = &b["rpc1"];
        let incarnation = rpc1.node.incarnation_id.clone().unwrap();
        let key = rdm_admin::node_commands::CommandKey { operation: operation.clone(), node_id: rpc1.node.node_id.clone(), incarnation: incarnation.clone() };
        let completed = rig.service.commands.open(key);
        let left = StatusRequest::NodeLeft { node_id: rpc1.node.node_id.clone(), incarnation, build_id: "66h8qrqvaajk".into(), operation: operation.clone() };
        let reply = reply(&call(&rig, rpc1, &left).await);
        let stop_open = !*completed.borrow();
        (stop_open, reply, rig.admin.node.node_id.to_string(), rpc1.node.node_id.to_string())
    });
    assert_eq!(
        reply,
        StatusReply::RejectedUnmatchedCompletion { field: "command".into(), expected: NODE_LEFT_EXPECTED.into(), reported: reported.clone() },
        "a node-left call is refused by name"
    );
    assert!(stop_open, "the open stop-node of the birth is not completed by a node-left call");
    let spans = cap.spans("rdm.node_admin.status.reject.via-node-left");
    assert_eq!(spans.len(), 1, "one reject span: {spans:?}");
    let (trace_id, attrs) = &spans[0];
    let a = |k: &str| attrs.get(k).cloned().unwrap_or_default();
    assert_eq!(
        (a("node"), a("op"), a("field"), a("expected"), a("reported"), a("name"), a("caller"), a("operation"), a("build_id")),
        (admin, "node-left".to_string(), "command".to_string(), NODE_LEFT_EXPECTED.to_string(), reported.clone(), rpc.clone(), rpc, "node.stop".to_string(), "66h8qrqvaajk".to_string()),
        "{attrs:?}"
    );
    assert!(cap.spans("rdm.node_admin.status.update.via-completion-accepted").is_empty(), "no completion is accepted");
    eprintln!("trace {trace_id}: rdm.node_admin.status.reject.via-node-left field=command reported={reported}");
}

/// CONTRACT (build.md §4: the current Build is the first in progress or completed, read last in,
/// first out; R-BI3, docs/plans/rulings/i143-rulings-2026-10-12-rx-authz.md: "Each accepted Build is its
/// own entity in the builds store, written once" and "The Fabric row carries no pointer to the newest
/// Build"; node-rpc-envelope.md typed objects: `fabric.get` answers the fabric): `fabric.get` (op 0x1B
/// `Get { Fabrics }`) at a node-admin answers the fabric's `build_id`, the current Build its own builds store
/// returns, skipping a failed Build, and an admin whose builds store holds no Build in progress or
/// completed answers none. Must NOT happen: a caller that needs the current Build reading it from
/// anywhere but the fabric's own read, or `fabric.get` naming a failed Build.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fabric_get_answers_the_current_build_its_builds_store_returns_and_skips_a_failed_one() {
    use rdm_admin::accepted::{AcceptedStore, FabricTopology};
    use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter};
    let admin = birth(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), "node-admin", "mesh1", true, true);
    let view = Topology {
        fabric: Fabric { id: FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes: vec![admin.node.clone()],
    };
    let builds = Arc::new(MemoryBuildStateAdapter::default());
    AcceptedStore::seeded(&*builds, view.fabric.id.clone(), FabricTopology::of_observed(&view), &admin.node.birth().unwrap()).await.unwrap();
    let held = builds.current_build().await.unwrap().into_build_id().expect("the seeded Build is current").to_string();
    let empty = Arc::new(MemoryBuildStateAdapter::default());
    let answer = |builds: Arc<dyn BuildStateAdapter>| {
        let view = view.clone();
        let admin = admin.node.clone();
        async move {
            let service = StatusService {
                certs: None,
                topology_view: None,
                declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
                stopped: Default::default(),
                me: admin.location(),
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
                primary_commands: Default::default(),
                storage_changes: Default::default(), hold_next_reply: Arc::new(std::sync::atomic::AtomicBool::new(false)),
                wake: Default::default(),
                fabric_states: Default::default(), atlas: Default::default(), fabric_recovery: Default::default(),
                configs: Default::default(),
                republish: Default::default(),
                pre_node_ready: Default::default(),
                rename: Default::default(),
                row_moved: Default::default(), rejoin: Default::default(),
                loads: Some(Arc::new(BTreeMap::new)),
                digest_view: None,
                unreachable: Default::default(),
                builds,
            };
            let read = rdm_core::status::Read { entity: rdm_core::filter::Entity::Fabrics, node: None, filter: rdm_core::filter::Filter::new() };
            match service.read_rows(&read).await {
                Ok(rdm_core::read::Rows::Fabrics(mut fabrics)) if fabrics.len() == 1 => fabrics.remove(0),
                other => panic!("fabric.get answered {other:?}"),
            }
        }
    };
    assert_eq!(answer(builds.clone()).await.build_id, Some(held.to_string()), "fabric.get answers the current Build the admin's builds store returns");
    // A newer Build that failed is skipped: the older completed one is still the current Build.
    let newer = rdm_admin::build::BuildId::mint();
    builds.publish_accepted(&BuildAccepted { build_id: newer.clone(), topology: FabricTopology::of_observed(&view), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: u64::MAX - 1 }).await.unwrap();
    crate::start_build(&*builds, &newer, &admin.node.birth().unwrap()).await;
    assert_eq!(answer(builds.clone()).await.build_id, Some(newer.to_string()), "the newer Build in progress is the current Build");
    let source = rdm_admin::events_storage::EventSource { fabric_id: crate::cell_fabric(), node_id: admin.node.node_id.clone(), incarnation: admin.node.birth().unwrap().incarnation };
    builds.events().append(&rdm_admin::events_storage::Event::build_ended(&source, 2, &newer, &rdm_admin::events_storage::EndedState::Failed { reason: "node.create: PrepareStorage".into(), code: "FAILED_PRECONDITION".into() })).await.unwrap();
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
    quiet_node_configs(&rig).await;
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
            StatusRequest::UpdateName { node_id: node_id.clone(), incarnation: incarnation.clone(), name: rdm_core::PathName::new("mesh1.node", "2").unwrap() },
        ];
        for req in requests {
            let got = reply(&call(&rig, caller, &req).await);
            assert_eq!(got, StatusReply::RejectedNotPrimary { why: NotPrimary::SubjectUnknown }, "{case}: {} answers the receiver's own fact", req.op());
        }
    }
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
        let topology_view = rdm_gossip::topology::TopologyView::new(&fabric, "mesh1", &MeshId::mint(), "mesh1.node-admin.1", Arc::new(time), Default::default());
        let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![]).await.unwrap();
        topology_view.set_mesh_primary(true);
        let (rig, b) = rig_with_digest_view(Some(topology_view.digest_view.clone())).await;
        let rpc = &b["rpc2"];
        assert!(topology_view.digest_view.record(rdm_core::MeshDigest { 
            fabric_id: fabric,
            node: rdm_core::MeshNode { mesh: rpc.node.mesh.clone(), r#type: rpc.node.r#type.clone(), node_id: rpc.node.node_id.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: rpc.node.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
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
        resolver.insert(ResolvedNode { mesh: rpc.node.mesh.clone(), r#type: rpc.node.r#type.clone(), node_id: rpc.node.node_id.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), endpoint_id: rpc.key.public(), transport_addr: addr, incarnation: rpc.node.incarnation_id.clone().unwrap() });
        let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm"));
        caller_delivery.set(client.delivery().clone()).unwrap_or_else(|_| panic!("one caller"));
        assert!(rig.service.configs.set(Arc::new(crate::bound_configs_on(rig.records.clone(), rdm_admin::node_config::NodeConfigs::new(client)).await)).is_ok());
        let answer = rig.service.carry_connections_delete(&rpc.node.node_id, rpc.node.incarnation_id.as_ref().unwrap(), &rdm_core::status::ConnectionScope::All).await;
        assert!(matches!(&answer, rdm_admin::status_rpc::CarryEnd::Deleted { .. }), "{answer:?}");
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
        let before = rig.service.declared.lock().unwrap().clone();
        let ctx = CommandContext { build_id: "b".into(), operation: rdm_core::OperationId::node(if start { "node.start" } else { "node.stop" }, &subject.node_id) };
        assert!(matches!(rig.service.carry_lifecycle(subject, start, &ctx).await, CommandAdmission::NotSent { .. }));
        assert_eq!(*rig.service.declared.lock().unwrap(), before);
    }
}

/// Q47: Status's former stop/start positions are reserved, even for an exact held birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_stop_and_start_positions_are_refused_by_name_without_execution() {
    let (rig, births) = rig().await;
    let subject = &births["rpc2"];
    for start in [false, true] {
        let request = if start { StatusRequest::StartNode { node_id: subject.node.node_id.clone(), incarnation: subject.node.incarnation_id.clone().unwrap(), build_id: "b".into(), operation: rdm_core::OperationId::unscoped("start") } } else { StatusRequest::StopNode { node_id: subject.node.node_id.clone(), incarnation: subject.node.incarnation_id.clone().unwrap(), build_id: "b".into(), operation: rdm_core::OperationId::unscoped("stop") } };
        let before = rig.service.declared.lock().unwrap().clone();
        let result = call(&rig, subject, &request).await;
        assert!(matches!(reply(&result), StatusReply::RejectedUnmatchedCompletion { expected, reported, .. } if expected.contains("reserved") && reported.contains(request.op())), "{result:?}");
        assert_eq!(*rig.service.declared.lock().unwrap(), before);
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
        resolver.insert(ResolvedNode { mesh: subject.mesh.clone(), r#type: subject.r#type.clone(), node_id: subject.node_id.clone(), path: subject.path.clone(), name: subject.name.clone(), endpoint_id: births["rpc2"].key.public(), transport_addr: addr, incarnation: subject.incarnation_id.clone().unwrap() });
        let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm"));
        assert!(rig.service.configs.set(Arc::new(crate::bound_configs_on(rig.records.clone(), rdm_admin::node_config::NodeConfigs::new(client)).await)).is_ok());
        let ctx = CommandContext { build_id: "b".into(), operation: rdm_core::OperationId::node(if start { "node.start" } else { "node.stop" }, &subject.node_id) };
        let (service, target) = (rig.service.clone(), subject.clone());
        let call = tokio::spawn(async move { service.carry_lifecycle(&target, start, &ctx).await });
        tokio::time::timeout(Duration::from_secs(5), arrived.notified()).await.unwrap();
        assert_eq!(rig.service.declared.lock().unwrap().node(&subject.node_id, subject.incarnation_id.as_ref().unwrap()), None, "no state before CommandAccepted");
        admit.notify_one();
        if !fail {
            tokio::time::timeout(Duration::from_secs(5), progressed.notified()).await.unwrap();
            if start {
                tokio::time::timeout(Duration::from_secs(5), async {
                    while rig.service.declared.lock().unwrap().node(&subject.node_id, subject.incarnation_id.as_ref().unwrap()) != Some(NodeState::Pending) { tokio::task::yield_now().await; }
                }).await.expect("NodeJoined is recorded before NodeStarted exists");
            }
            finish.notify_one();
        }
        let result = tokio::time::timeout(Duration::from_secs(5), call).await.unwrap().unwrap();
        if fail {
            assert!(matches!(result, A::StartFailed { step, reason, code: rdm_core::outcome::Code::FailedPrecondition } if step == "pre_node_ready" && reason == "app refused"));
            assert!(rig.service.declared.lock().unwrap().operations.is_empty());
        } else {
            assert!(matches!((&result, start), (A::Started {}, true) | (A::Stopped { .. }, false)), "{result:?}");
            let state = if start { NodeState::Ready } else { NodeState::Stopped };
            assert_eq!(rig.service.declared.lock().unwrap().node(&subject.node_id, subject.incarnation_id.as_ref().unwrap()), Some(state), "recorded before the outer Build can complete");
            assert_eq!(rig.service.declared.lock().unwrap().operations.len(), 1);
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
        let topology_view = rdm_gossip::topology::TopologyView::new(&fabric, "mesh1", &MeshId::mint(), "mesh1.node-admin.1", Arc::new(time), Default::default());
        let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![]).await.unwrap();
        topology_view.set_mesh_primary(true);
        let (rig, b) = rig_with_digest_view(Some(topology_view.digest_view.clone())).await;
        let rpc = &b["rpc2"];
        assert!(topology_view.digest_view.record(rdm_core::MeshDigest { 
            fabric_id: fabric,
            node: rdm_core::MeshNode { mesh: rpc.node.mesh.clone(), r#type: rpc.node.r#type.clone(), node_id: rpc.node.node_id.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: rpc.node.incarnation_id.clone().unwrap(), supersedes: None, runtime: None },
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
        resolver.insert(ResolvedNode { mesh: rpc.node.mesh.clone(), r#type: rpc.node.r#type.clone(), node_id: rpc.node.node_id.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), endpoint_id: rpc.key.public(), transport_addr: addr, incarnation: rpc.node.incarnation_id.clone().unwrap() });
        let ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(ep, resolver).with_caller_system("rdm"));
        caller_delivery.set(client.delivery().clone()).unwrap_or_else(|_| panic!("one caller"));
        assert!(rig.service.configs.set(Arc::new(crate::bound_configs_on(rig.records.clone(), rdm_admin::node_config::NodeConfigs::new(client)).await)).is_ok());
        let context = rdm_admin::deployment::pipeline::CommandContext { build_id: "b".into(), operation: rdm_core::OperationId::node("node.start", &rpc.node.node_id) };
        let answer = rig.service.carry_lifecycle(&rpc.node, true, &context).await;
        assert!(matches!(answer, rdm_admin::deployment::pipeline::CommandAdmission::Started {}), "{answer:?}");
        assert_eq!(rig.service.declared.lock().unwrap().operations.len(), 1);
        assert!(rdm_transport::DeliveryStats::get(&server_delivery.get().unwrap().stats().responses_resent) >= 1);
    });
    let records: Vec<_> = cap.spans("rdm.node_admin.node.update.via-command-frame").into_iter().filter(|(_, a)| a.get("frame").map(String::as_str) == Some("node-joined")).collect();
    assert_eq!(records.len(), 1, "the mesh-primary records the repeated progress once: {records:?}");
    let applied: Vec<_> = cap.spans("rdm.mesh.topology.update.from-status-report").into_iter().filter(|(_, a)| a.get("status").map(String::as_str) == Some("Pending")).collect();
    assert_eq!(applied.len(), 1, "one Pending status write, not just one carrier span: {applied:?}");
    assert!(!records[0].1.get("invocation_id").unwrap_or(&String::new()).is_empty(), "the record names the invocation: {records:?}");
    assert_eq!(records[0].1.get("seq").map(String::as_str), Some("1"), "the progress position: {records:?}");
}

/// A start after a stop of mesh1.node.2 at its mesh-primary, carried on a real command stream: the
/// birth's stop is recorded (its Stopped row, the view's stop), the start's join report is accepted
/// and its Pending recorded there, before the join is answered (node-mesh-join.md step 10). With
/// `ready_first`, the node's own Ready declaration is decided before its mesh-primary takes
/// the start's `NodeJoined` frame; otherwise after it. What the mesh-primary holds of the birth once
/// it took `NodeJoined` is checked by `joined`, before `NodeStarted` is sent; the spans the run
/// exported are returned.
fn start_after_a_stop_with_the_pending_recorded_at_the_join(ready_first: bool, joined: impl Fn(&Rig, &Birth, &rdm_gossip::digest_view::DigestView) + Send + Sync + 'static) -> Capture {
    use rdm_core::node_command::{NodeCommand, NodeCommandReply as N, NodeCommandRequest};
    let cap = capture();
    cap.run(async move {
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
        let _gossip_router = Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
        let fabric = FabricId::mint();
        let time = rdm_transport::clock::RafkaTime::unadopted();
        time.adopt(5_000_000);
        let topology_view = rdm_gossip::topology::TopologyView::new(&fabric, "mesh1", &MeshId::mint(), "mesh1.node-admin.1", Arc::new(time), Default::default());
        let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![]).await.unwrap();
        topology_view.set_mesh_primary(true);
        let digest_view = topology_view.digest_view.clone();
        let (rig, b) = rig_with_digest_view(Some(digest_view.clone())).await;
        let rpc = &b["rpc2"];
        let incarnation = rpc.node.incarnation_id.clone().unwrap();
        assert!(digest_view.record(rdm_core::MeshDigest { 
            fabric_id: fabric,
            node: rdm_core::MeshNode { mesh: rpc.node.mesh.clone(), r#type: rpc.node.r#type.clone(), node_id: rpc.node.node_id.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: incarnation.clone(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Pending, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: None, in_flight: None, load: None, gossip: None, is_primary: None, is_fabric_primary: None
        }));
        // The birth's stop: its Stopped row, and the view's stop of it.
        let stop = rdm_core::LifecycleOp { r#type: rpc.node.r#type.clone(), mesh: rpc.node.mesh.clone(), node_id: rpc.node.node_id.clone(), incarnation: incarnation.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), build_id: "b-stop".into(), operation: rdm_core::OperationId::node("node.stop", &rpc.node.node_id), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
        assert_eq!(rig.service.stop_received(&stop).await, StatusReply::Applied);
        assert!(digest_view.stopped(stop), "the view holds the birth's stop");
        // The start's join report, accepted at the `StatusService`: the birth's next cycle begins and its Pending
        // is recorded there, before the join is answered.
        assert_eq!(rig.service.joined_pending(&rpc.node.node_id, &rpc.node.location(), &incarnation, true).await, StatusReply::Applied, "the join report's Pending is recorded");
        assert_eq!(rig.service.declared.lock().unwrap().node(&rpc.node.node_id, &incarnation), Some(NodeState::Pending));
        // A later report of that join (a retry of the same report) belongs to that Pending and starts no other.
        assert_eq!(rig.service.joined_pending(&rpc.node.node_id, &rpc.node.location(), &incarnation, false).await, StatusReply::AlreadyApplied, "a retried join report applies as already applied");
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
        resolver.insert(ResolvedNode { mesh: rpc.node.mesh.clone(), r#type: rpc.node.r#type.clone(), node_id: rpc.node.node_id.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), endpoint_id: rpc.key.public(), transport_addr: addr, incarnation: incarnation.clone() });
        let admin_ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(admin_ep, resolver).with_caller_system("rdm"));
        assert!(rig.service.configs.set(Arc::new(crate::bound_configs_on(rig.records.clone(), rdm_admin::node_config::NodeConfigs::new(client)).await)).is_ok());
        let context = rdm_admin::deployment::pipeline::CommandContext { build_id: "b-start".into(), operation: rdm_core::OperationId::node("node.start", &rpc.node.node_id) };
        let (service, target) = (rig.service.clone(), rpc.node.clone());
        let start = tokio::spawn(async move { service.carry_lifecycle(&target, true, &context).await });
        tokio::time::timeout(Duration::from_secs(10), taken.notified()).await.expect("the mesh-primary took the start's NodeJoined");
        joined(&rig, rpc, &digest_view);
        if !ready_first {
            assert_eq!(reply(&call(&rig, rpc, &declare(rpc, NodeState::Ready)).await), StatusReply::Applied, "the node's Ready applies over the join's Pending");
        }
        release.notify_one();
        let answer = tokio::time::timeout(Duration::from_secs(10), start).await.unwrap().unwrap();
        assert!(matches!(answer, rdm_admin::deployment::pipeline::CommandAdmission::Started {}), "{answer:?}");
        assert_eq!(rig.service.declared.lock().unwrap().node(&rpc.node.node_id, &incarnation), Some(NodeState::Ready));
        assert_eq!(digest_view.member_status(&rpc.node.node_id, &incarnation), Some(rdm_core::MemberStatus::Ready));
    });
    cap
}

/// What the mesh-primary holds of mesh1.node.2's birth once it took the start's `NodeJoined`: the
/// declared `state`, the member status `status`, its stop over and the birth held again.
fn held_after_node_joined(rig: &Rig, rpc: &Birth, digest_view: &rdm_gossip::digest_view::DigestView, state: NodeState, status: rdm_core::MemberStatus) {
    let incarnation = rpc.node.incarnation_id.clone().unwrap();
    assert_eq!(rig.service.declared.lock().unwrap().node(&rpc.node.node_id, &incarnation), Some(state), "the start's NodeJoined belongs to the Pending its join report recorded: it applies as already applied and the birth stays declared {state:?}");
    assert_eq!(digest_view.member_status(&rpc.node.node_id, &incarnation), Some(status), "the view holds the birth {status:?}: NodeJoined sets nothing back");
    assert!(digest_view.get(rpc.node.node_id.as_str()).is_some_and(|(d, _)| d.node.incarnation == incarnation), "the birth is held again from node.mesh.joined");
    assert!(!digest_view.in_flight().into_iter().any(|op| op.node_id == rpc.node.node_id && op.is_stop()), "its stop is cleared at node.mesh.joined");
}

/// The spans of `name` the cell's run exported for mesh1.node.2.
fn of_rpc2(cap: &Capture, name: &str) -> Vec<BTreeMap<String, String>> {
    cap.spans(name).into_iter().map(|(_, a)| a).filter(|a| a.get("node").map(String::as_str) == Some("mesh1.node.2") || a.get("member").map(String::as_str) == Some("mesh1.node.2")).collect()
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
    assert!(rig.service.stopped.set(Arc::new(move |_, _| {
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
            let Q::StopNode { node_id, incarnation, operation } = request else { panic!("a stop") };
            let mut sink = sink.started(N::CommandAccepted { node_id: node_id.clone(), incarnation: incarnation.clone() }).await.unwrap();
            sink.data(N::StopConnectionsDeleted { receipt: rdm_core::status::ConnectionsReceipt::Established { in_flight: 0, closed: vec![] } }).await.unwrap();
            let server = server_slot.get().unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                while rdm_transport::DeliveryStats::get(&server.delivery().stats().acks_received) < 2 { tokio::task::yield_now().await; }
            }).await.expect("the stop's acceptance and closure are recovered before the connection is cut");
            assert!(server.close_connections_except(&[], "the cell cuts the stop stream after closure") > 0);
            let declaration = StatusRequest::DeclareNodeState { node_id, incarnation, state: NodeState::Stopped, operation: Some(rdm_core::status::DeclaredOperation { operation }) };
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
    resolver.insert(ResolvedNode { mesh: subject.node.mesh.clone(), r#type: subject.node.r#type.clone(), node_id: subject.node.node_id.clone(), path: subject.node.path.clone(), name: subject.node.name.clone(), endpoint_id: subject.key.public(), transport_addr: addr, incarnation: subject.node.incarnation_id.clone().unwrap() });
    // The caller owns and serves one birth on one endpoint: recovered responses arrive
    // through this router and find the invocation in the same Delivery as the call.
    let client = Arc::new(NodeRpcClient::new(rig.endpoint.clone(), resolver).with_delivery(rig.server.delivery().clone()).with_caller_system("rdm"));
    assert!(rig.service.configs.set(Arc::new(crate::bound_configs_on(rig.records.clone(), rdm_admin::node_config::NodeConfigs::new(client)).await)).is_ok());
    let context = CommandContext { build_id: "b".into(), operation: rdm_core::OperationId::node("node.stop", &subject.node.node_id) };
    let end = tokio::time::timeout(Duration::from_secs(5), rig.service.carry_lifecycle(&subject.node, false, &context)).await.expect("the lost physical stream recovers its terminal on the same invocation");
    assert!(matches!(end, A::Stopped { .. }), "{end:?}");
    assert_eq!(published.load(Ordering::SeqCst), 1, "the declaration publishes stopped; the recovered terminal publishes nothing twice");
    assert_eq!(rig.service.declared.lock().unwrap().operations.len(), 1);
    assert_eq!(rig.service.declared.lock().unwrap().node(&subject.node.node_id, subject.node.incarnation_id.as_ref().unwrap()), Some(NodeState::Stopped));
}

/// A start of mesh1.node.2 carried by its mesh-primary on a real command stream, answered the way a
/// birth answers a start an earlier run of the same Build completed (`AlreadyApplied`: the
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
        let topology_view = rdm_gossip::topology::TopologyView::new(&fabric, "mesh1", &MeshId::mint(), "mesh1.node-admin.1", Arc::new(time), Default::default());
        let channels = rdm_gossip::channel::Channels::new(&gossip, &ep, &topology_view).unwrap();
        channels.join(rdm_gossip::channel::ChannelId::mesh(topology_view.fabric_id(), topology_view.mesh_id()), vec![]).await.unwrap();
        topology_view.set_mesh_primary(true);
        let digest_view = topology_view.digest_view.clone();
        let (rig, b) = rig_with_digest_view(Some(digest_view.clone())).await;
        let rpc = &b["rpc2"];
        let incarnation = rpc.node.incarnation_id.clone().unwrap();
        let marked = if marked_other_birth { IncarnationId::mint() } else { incarnation.clone() };
        assert!(digest_view.record(rdm_core::MeshDigest { 
            fabric_id: fabric,
            node: rdm_core::MeshNode { mesh: rpc.node.mesh.clone(), r#type: rpc.node.r#type.clone(), node_id: rpc.node.node_id.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), endpoint_id: EndpointId(rpc.key.public().to_string()), transport_addr: "127.0.0.1:20001".parse().unwrap(), incarnation: marked.clone(), supersedes: None, runtime: None },
            status: rdm_core::MemberStatus::Ready, node_state: Default::default(), admin_api_base: None, digest_seq: 1, emitted_at_rafka_ms: 1, data_dir: None, mesh_id: None, in_flight: None, load: None, gossip: None, is_primary: None, is_fabric_primary: None
        }));
        let stop = rdm_core::LifecycleOp { r#type: rpc.node.r#type.clone(), mesh: rpc.node.mesh.clone(), node_id: rpc.node.node_id.clone(), incarnation: marked.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), build_id: "b-stop".into(), operation: rdm_core::OperationId::node("node.stop", &rpc.node.node_id), event_at_rafka_ms: 1, traceparent: None, tracestate: None };
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
        resolver.insert(ResolvedNode { mesh: rpc.node.mesh.clone(), r#type: rpc.node.r#type.clone(), node_id: rpc.node.node_id.clone(), path: rpc.node.path.clone(), name: rpc.node.name.clone(), endpoint_id: rpc.key.public(), transport_addr: addr, incarnation: incarnation.clone() });
        let admin_ep = rdm_transport::endpoint::bind(rig.admin.key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = Arc::new(NodeRpcClient::new(admin_ep, resolver).with_caller_system("rdm"));
        assert!(rig.service.configs.set(Arc::new(crate::bound_configs_on(rig.records.clone(), rdm_admin::node_config::NodeConfigs::new(client)).await)).is_ok());
        let context = rdm_admin::deployment::pipeline::CommandContext { build_id: "b".into(), operation: rdm_core::OperationId::node("node.start", &rpc.node.node_id) };
        let answer = rig.service.carry_lifecycle(&rpc.node, true, &context).await;
        assert!(matches!(answer, rdm_admin::deployment::pipeline::CommandAdmission::Started {}), "{answer:?}");
        digest_view.holds_stopped(rpc.node.node_id.as_str(), &marked)
    })
}

/// CONTRACT (node-start.md step 2, Q249): a start an earlier run completed, reissued by the next
/// run's executor, is answered without a `NodeJoined`; the mesh-primary whose overlay holds that
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

/// i150: removed upward property copies are refused at the wire position, without writing a row.
#[test]
fn the_removed_upward_property_copy_is_refused_without_a_durable_effect() {
    let cap = capture();
    cap.run(async {
        let (rig, b) = rig().await;
        let rpc = &b["rpc1"];
        let req = StatusRequest::Reserved34 { node_id: rpc.node.node_id.clone(), config: [("removed".into(), "copy".into())].into(), revision: 99 };
        let before = rig.nodes.record_at(&rpc.node.location()).await.unwrap();
        let answer = rig.service.apply(None, &req).await;
        assert!(!matches!(answer, StatusReply::Applied | StatusReply::AlreadyApplied | StatusReply::ConfigUpdated { .. }), "reserved request cannot apply: {answer:?}");
        assert_eq!(rig.nodes.record_at(&rpc.node.location()).await.unwrap(), before);
    });
}
