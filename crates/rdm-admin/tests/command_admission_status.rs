//! The mesh-primary records its members' states (states.md "Who sets each state"): stopping at "the
//! stop call's admission (node.stop)"; connections-deleting from the node's own report on its
//! `node.connection.delete` command's stream (i144 R22, Luke Q6), never at the command's admission.

use iroh::SecretKey;
use rdm_core::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rdm_gossip::topology::{Backbone, TopologyView};
use rdm_admin::admin::GossipLifecycle;
use rdm_admin::deployment::pipeline::LifecycleEvents;
use std::net::SocketAddr;
use std::sync::Arc;

fn member(fabric: &FabricId, name: rdm_core::PathName, mesh: &str, r#type: &str, seq: u64) -> MeshDigest {
    let node_id = NodeId::mint();
    MeshDigest { 
        fabric_id: fabric.clone(),
        node: MeshNode { mesh: mesh.into(), r#type: r#type.to_string(),
            node_id: node_id.clone(),
            path: name.path.clone(), name: name.name.clone(),
            endpoint_id: EndpointId(format!("key-{node_id}-{}", "x".repeat(40))),
            transport_addr: "127.0.0.1:20001".parse().unwrap(),
            incarnation: IncarnationId::mint(),
            supersedes: None,
            runtime: None,
        },
        status: MemberStatus::Pending,
        node_state: Default::default(),
        admin_api_base: None,
        digest_seq: seq,
        emitted_at_rafka_ms: seq,
        data_dir: None,
        mesh_id: None,
        in_flight: None,
        load: None,
        gossip: None, is_primary: None, is_fabric_primary: None
    }
}

fn op(d: &MeshDigest, operation: &str) -> LifecycleOp {
    LifecycleOp { r#type: d.node.r#type.clone(), mesh: d.node.mesh.clone(), build_id: "3vzt99cnx0xe".into(), operation: rdm_core::OperationId::node(operation, &d.node.node_id), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), path: d.node.path.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1, traceparent: None, tracestate: None }
}

// @feature: node-lifecycle
/// CONTRACT (states.md "Who sets each state"): at the mesh-primary of mesh1, the accepted
/// `stop-node` command's hook sets the member it names stopping (`Leaving`), with no heartbeat of the
/// member saying it: the status is held for the birth and its member projection, what the publication
/// checklist puts into the mesh's topology, carries it. The member's connections-deleting is its own
/// report (the node command's `ConnectionsDeleting` frame), recorded as `digest_view.reported`; no command
/// hook sets it. A member of another mesh is not this primary's to set. Must NOT happen: a member's
/// stopping left to its own heartbeat, or a connections-deleting set before the node reported it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mesh_primary_sets_its_member_stopping_at_the_stops_admission_and_records_connections_deleting_from_its_report() {
    let fabric = FabricId::mint();
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let admin_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let topology_view = TopologyView::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), admin_birth.node_id.as_str(), Arc::new(time.clone()), vec![]).await.unwrap();
    let backbone = Backbone::join(&gossip, &ep, &topology_view, "mesh1", rdm_core::PublisherId { incarnation: admin_birth.incarnation.clone(), node_id: admin_birth.node_id.clone() }, vec![]).await.unwrap();
    backbone.set_mesh_primary(true);
    let lifecycle = GossipLifecycle { topology_view: topology_view.clone(), backbone: backbone.clone(), me_path: "mesh1.node-admin".into(), me_name: "1".into(), mesh_deleter: std::sync::Arc::new(std::sync::OnceLock::new()), project: None, status_service: Default::default() };

    let rpc = member(&fabric, rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node", 10);
    assert!(topology_view.digest_view.record(rpc.clone()), "the member's heartbeat is held");
    assert!(topology_view.digest_view.started(&rpc.node.node_id, &rpc.node.incarnation, &rpc.node.location(), &rpc.node.mesh), "node.started of the member");
    let status = |d: &MeshDigest| topology_view.digest_view.get(d.node.node_id.as_str()).map(|(h, _)| h.status);
    let projected = |d: &MeshDigest| backbone.own_members(&topology_view).into_iter().find(|m| m.node.node_id == d.node.node_id).map(|m| m.status);
    assert_eq!(status(&rpc), Some(MemberStatus::Ready));

    assert!(topology_view.digest_view.reported(&rpc.node.node_id, &rpc.node.incarnation, MemberStatus::ConnectionsDeleting), "the member's report of connections-deleting is recorded");
    assert_eq!(status(&rpc), Some(MemberStatus::ConnectionsDeleting), "the node's report of connections-deleting sets the member connections-deleting");
    assert_eq!(projected(&rpc), Some(MemberStatus::ConnectionsDeleting), "the member projection carries it");

    lifecycle.leaving(&op(&rpc, "node.stop")).await;
    assert_eq!(status(&rpc), Some(MemberStatus::Leaving), "the stop's admission sets the member stopping");
    assert_eq!(projected(&rpc), Some(MemberStatus::Leaving), "the member projection carries it");

    let peer = member(&fabric, rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node", 10);
    assert!(!topology_view.digest_view.reported(&peer.node.node_id, &peer.node.incarnation, MemberStatus::ConnectionsDeleting), "a member of another mesh is not this primary's to record");
    lifecycle.leaving(&op(&peer, "node.stop")).await;
    assert_eq!(status(&peer), None, "a member of another mesh is not this primary's to set");
}

// @feature: node-lifecycle
/// CONTRACT (node-status.md "Status report": "A node reports to its mesh's primary, which sets its
/// status"; gossip.md §3.1: a Digest "carries no status"): a member's `DeclareNodeState` applied by its
/// mesh-primary's status authority sets the member's status in that primary's view, which its member
/// projection carries: its Ready (node.started), then its ConnectionsDeleting (a node-admin frozen by the
/// fabric shutdown says it so, and the shutdown's freeze barrier reads it from the projection). A
/// refused report sets nothing. Must NOT happen: a member's reported state applied to its declared row
/// and nowhere else, so every other node holds it as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_members_applied_report_sets_its_status_in_its_mesh_primarys_view() {
    use rdm_admin::model::{Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
    use rdm_admin::status_rpc::{Declared, StatusService};
    use rdm_core::status::{NodeState, StatusReply, StatusRequest};
    let fabric = FabricId::mint();
    let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let admin_birth = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let topology_view = TopologyView::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), admin_birth.node_id.as_str(), Arc::new(time.clone()), vec![]).await.unwrap();
    topology_view.set_mesh_primary(true);

    let member = member(&fabric, rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), "mesh1", "node-admin", 10);
    assert!(topology_view.digest_view.record(member.clone()), "the member's heartbeat is held");
    let node = |name: rdm_core::PathName, primary: bool, id: NodeId, inc: IncarnationId| {
        let mut n = Node::allocated(name, "mesh1", "node-admin");
        n.r#type = "node-admin".to_string();
        n.mesh = "mesh1".into();
        n.node_id = id;
        n.incarnation_id = Some(inc);
        n.provider = Some(ProviderKind::Process);
        n.status = NodeStatus::Ready;
        n.is_primary = primary;
        n.is_fabric_primary = primary;
        n
    };
    let me = node(rdm_core::PathName::new("mesh1.node-admin", "1").unwrap(), true, admin_birth.node_id.clone(), admin_birth.incarnation.clone());
    let subject = node(rdm_core::PathName::new("mesh1.node-admin", "2").unwrap(), false, member.node.node_id.clone(), member.node.incarnation.clone());
    let topology = Arc::new(tokio::sync::RwLock::new(rdm_admin::topology::Topology {
        fabric: Fabric { id: fabric.clone(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes: vec![me.clone(), subject.clone()],
    }));
    let status_service = StatusService {
        certs: None,
        topology_view: None,
        declaration_seam: None,
            command_frame_seam: None,
            operation_effects: Default::default(),
        stopped: Default::default(),
        me: me.location(),
        node_id: me.node_id.clone(),
        fabric_id: fabric.clone(),
        topology,
        declared: Arc::new(std::sync::Mutex::new(Declared::default())),
        meshes_storage: Arc::new(rdm_admin::storage::MemoryMeshStorage::default()), fabric_storage: Arc::new(rdm_admin::fabric_storage::MemoryFabricStorage::new()),
        nodes_storage: Arc::new(rdm_admin::storage::MemoryNodesStorage::default()),
        caches: Default::default(),
        mesh_ids: Arc::new(std::collections::BTreeMap::new),
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
        loads: None,
        digest_view: Some(topology_view.digest_view.clone()),
        unreachable: Default::default(),
        builds: std::sync::Arc::new(rdm_admin::build_state::MemoryBuildStateAdapter::new()),
    };
    let status = || topology_view.digest_view.get(member.node.node_id.as_str()).map(|(h, _)| h.status);
    let declare = |state| StatusRequest::DeclareNodeState { node_id: member.node.node_id.clone(), incarnation: member.node.incarnation.clone(), state, operation: None };
    assert_eq!(status(), Some(MemberStatus::Pending));
    assert_eq!(status_service.apply(Some(subject.clone()), &declare(NodeState::Ready)).await, StatusReply::Applied);
    assert_eq!(status(), Some(MemberStatus::Ready), "its node.started sets it ready");
    assert_eq!(status_service.apply(Some(subject.clone()), &declare(NodeState::ConnectionsDeleting)).await, StatusReply::Applied);
    assert_eq!(status(), Some(MemberStatus::ConnectionsDeleting), "its reported ConnectionsDeleting sets it ConnectionsDeleting");
    let backbone = Backbone::join(&gossip, &ep, &topology_view, "mesh1", rdm_core::PublisherId { incarnation: me.incarnation_id.clone().unwrap(), node_id: me.node_id.clone() }, vec![]).await.unwrap();
    assert_eq!(backbone.own_members(&topology_view).into_iter().find(|d| d.node.node_id == member.node.node_id).map(|d| d.status), Some(MemberStatus::ConnectionsDeleting), "its projection carries it");
    assert!(matches!(status_service.apply(Some(subject.clone()), &declare(NodeState::Pending)).await, StatusReply::RejectedInvalidNodeTransition { .. }), "a backward report is refused");
    assert_eq!(status(), Some(MemberStatus::ConnectionsDeleting), "and sets nothing");
}
