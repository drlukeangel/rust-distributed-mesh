//! The mesh-primary authors its members' connections-deleting and stopping from its own steps
//! (states.md "Who sets each state": node connections-deleting is set by "the node's mesh-primary"
//! at "the admission of `node.connections.delete { scope: All }`; inside node.stop the visible status
//! is stopping"; node stopping by "the node's mesh-primary" at "the stop call's admission (node.stop)").

use iroh::SecretKey;
use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, LifecycleOp, MemberStatus, MeshDigest, MeshId, MeshNode, NodeId};
use rafka_mesh_transport::membership::{Backbone, Membership};
use rafka_node_admin_core::admin::GossipLifecycle;
use rafka_node_admin_core::deployment::pipeline::LifecycleEvents;
use std::net::SocketAddr;
use std::sync::Arc;

fn member(fabric: &FabricId, name: &str, seq: u64) -> MeshDigest {
    MeshDigest {
        fabric_id: fabric.clone(),
        node: MeshNode {
            node_id: NodeId::mint(),
            name: name.parse().unwrap(),
            endpoint_id: EndpointId(format!("key-{name}-{}", "x".repeat(40))),
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
        config: Default::default(),
        load: None,
        gossip: None,
    }
}

fn op(d: &MeshDigest, operation: &str) -> LifecycleOp {
    LifecycleOp { build_id: "bld_admission".into(), attempt: 1, operation: operation.into(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone(), name: d.node.name.clone(), event_at_rafka_ms: 1 }
}

// @feature: node-lifecycle
/// CONTRACT (states.md "Who sets each state"): at the mesh-primary of mesh1, the accepted
/// `drain-node` command's hook sets the member it names connections-deleting (`Draining`) and the
/// accepted `stop-node` command's hook sets it stopping (`Leaving`), with no heartbeat of the member
/// saying either: the status is held for the birth and its member projection, what the publication
/// round puts into the mesh's aggregate, carries it. A member of another mesh is not this primary's to
/// set. Must NOT happen: a member's connections-deleting or stopping left to its own heartbeat.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_mesh_primary_sets_its_members_draining_and_leaving_at_the_commands_admission() {
    let fabric = FabricId::mint();
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let time = rafka_mesh_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let membership = Membership::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), "mesh1.admin.1", Arc::new(time.clone()), vec![]).await.unwrap();
    let backbone = Backbone::join(&gossip, &ep, &membership, "mesh1", "mesh1.admin.1", IncarnationId::mint(), vec![]).await.unwrap();
    backbone.set_mesh_primary(true);
    let lifecycle = GossipLifecycle { membership: membership.clone(), backbone: backbone.clone(), me: "mesh1.admin.1".parse().unwrap(), leaver: std::sync::Arc::new(std::sync::OnceLock::new()) };

    let rpc = member(&fabric, "mesh1.rpc.1", 10);
    assert!(membership.book.record(rpc.clone()), "the member's heartbeat is held");
    assert!(membership.book.started(&rpc.node.node_id, &rpc.node.incarnation), "node.started of the member");
    let status = |d: &MeshDigest| membership.book.get(d.node.node_id.as_str()).map(|(h, _)| h.status);
    let projected = |d: &MeshDigest| backbone.own_members(&membership).into_iter().find(|m| m.node.node_id == d.node.node_id).map(|m| m.status);
    assert_eq!(status(&rpc), Some(MemberStatus::ReadyForTraffic));

    lifecycle.draining(&op(&rpc, "drain-node:mesh1.rpc.1")).await;
    assert_eq!(status(&rpc), Some(MemberStatus::Draining), "the drain's admission sets the member connections-deleting");
    assert_eq!(projected(&rpc), Some(MemberStatus::Draining), "the member projection carries it");

    lifecycle.leaving(&op(&rpc, "stop-node:mesh1.rpc.1")).await;
    assert_eq!(status(&rpc), Some(MemberStatus::Leaving), "the stop's admission sets the member stopping");
    assert_eq!(projected(&rpc), Some(MemberStatus::Leaving), "the member projection carries it");

    let peer = member(&fabric, "mesh2.rpc.1", 10);
    lifecycle.draining(&op(&peer, "drain-node:mesh2.rpc.1")).await;
    assert_eq!(status(&peer), None, "a member of another mesh is not this primary's to set");
}

// @feature: node-lifecycle
/// CONTRACT (node-status.md "Status report": "A node reports to its mesh's primary, which sets its
/// status"; gossip.md §3.1: a Digest "carries no status"): a member's `DeclareNodeState` applied by its
/// mesh-primary's status authority sets the member's status in that primary's book, which its member
/// projection carries: its ReadyForTraffic (node.started), then its Draining (a node-admin frozen by the
/// fabric shutdown says it so, and the shutdown's freeze barrier reads it from the projection). A
/// refused report sets nothing. Must NOT happen: a member's reported state applied to its declared row
/// and nowhere else, so every other node holds it as it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_members_applied_report_sets_its_status_in_its_mesh_primarys_book() {
    use rafka_node_admin_core::model::{Fabric, Node, NodeStatus, ProviderKind, ScopeStatus};
    use rafka_node_admin_core::status_rpc::{Declared, StatusAuthority};
    use rafka_node_rpc_contract::status::{NodeState, StatusReply, StatusRequest};
    let fabric = FabricId::mint();
    let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse::<SocketAddr>().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let _router = iroh::protocol::Router::builder(ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let time = rafka_mesh_transport::clock::RafkaTime::unadopted();
    time.adopt(5_000_000);
    let membership = Membership::join(&gossip, &ep, &fabric, "mesh1", &MeshId::mint(), "mesh1.admin.1", Arc::new(time.clone()), vec![]).await.unwrap();
    membership.set_mesh_primary(true);

    let member = member(&fabric, "mesh1.admin.2", 10);
    assert!(membership.book.record(member.clone()), "the member's heartbeat is held");
    let node = |name: &str, primary: bool, id: NodeId, inc: IncarnationId| {
        let mut n = Node::allocated(name.parse().unwrap());
        n.kind = rafka_mesh_entity::NodeKind::NodeAdmin;
        n.mesh = "mesh1".into();
        n.node_id = id;
        n.incarnation_id = Some(inc);
        n.provider = Some(ProviderKind::Process);
        n.status = NodeStatus::ReadyForTraffic;
        n.is_primary = primary;
        n.is_fabric_primary = primary;
        n
    };
    let me = node("mesh1.admin.1", true, NodeId::mint(), IncarnationId::mint());
    let subject = node("mesh1.admin.2", false, member.node.node_id.clone(), member.node.incarnation.clone());
    let topology = Arc::new(tokio::sync::RwLock::new(rafka_node_admin_core::topology::Topology {
        fabric: Fabric { id: fabric.clone(), name: "fabric1".into(), status: ScopeStatus::ReadyForTraffic, provider: ProviderKind::Process },
        meshes: Vec::new(),
        nodes: vec![me.clone(), subject.clone()],
    }));
    let authority = StatusAuthority {
        me: me.name.clone(),
        node_id: me.node_id.clone(),
        fabric_id: fabric.clone(),
        topology,
        declared: Arc::new(std::sync::Mutex::new(Declared::default())),
        status_storage: Arc::new(rafka_node_admin_core::status_storage::MemoryStatusStorage::default()),
        nodes_storage: Arc::new(rafka_node_admin_core::storage::MemoryNodesStorage::default()),
        mesh_ids: Arc::new(std::collections::BTreeMap::new),
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
        book: Some(membership.book.clone()),
        first_ready: Default::default(),
        accepted: std::sync::Arc::new(rafka_node_admin_core::accepted::AcceptedStore::new(std::sync::Arc::new(rafka_node_admin_core::fabric_storage::MemoryFabricStorage::new()), "test")),
    };
    let status = || membership.book.get(member.node.node_id.as_str()).map(|(h, _)| h.status);
    let declare = |state| StatusRequest::DeclareNodeState { node_id: member.node.node_id.clone(), incarnation: member.node.incarnation.clone(), state };
    assert_eq!(status(), Some(MemberStatus::Pending));
    assert_eq!(authority.apply(Some(subject.clone()), &declare(NodeState::ReadyForTraffic)).await, StatusReply::Applied);
    assert_eq!(status(), Some(MemberStatus::ReadyForTraffic), "its node.started sets it ready-for-traffic");
    assert_eq!(authority.apply(Some(subject.clone()), &declare(NodeState::Draining)).await, StatusReply::Applied);
    assert_eq!(status(), Some(MemberStatus::Draining), "its reported Draining sets it Draining");
    let backbone = Backbone::join(&gossip, &ep, &membership, "mesh1", "mesh1.admin.1", me.incarnation_id.clone().unwrap(), vec![]).await.unwrap();
    assert_eq!(backbone.own_members(&membership).into_iter().find(|d| d.node.node_id == member.node.node_id).map(|d| d.status), Some(MemberStatus::Draining), "its projection carries it");
    assert!(matches!(authority.apply(Some(subject.clone()), &declare(NodeState::Pending)).await, StatusReply::RejectedInvalidNodeTransition { .. }), "a backward report is refused");
    assert_eq!(status(), Some(MemberStatus::Draining), "and sets nothing");
}
