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
    let lifecycle = GossipLifecycle { membership: membership.clone(), backbone: backbone.clone(), me: "mesh1.admin.1".parse().unwrap() };

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
