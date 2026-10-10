//! A stopped node parks and a started node rejoins as itself (node-stop.md, node-start.md).
//!
//! CONTRACT: `stop-node` is answered `Left` on its own call, after the node drained, cut its mesh
//! connections and entered Leaving; the node gossips nothing for it, its process stays alive with its
//! endpoint bound, and a probe answers `CurrentParked`. `start-node` is answered `Started` once the
//! same birth (same node id, incarnation, endpoint key and port) rejoined through the admin that
//! commanded it, and a probe answers `Current` again. What must NOT happen: a stop answered before the
//! node parked, a digest of a parked node reaching the admin, a start that makes a new birth, or a start
//! of a node that is not parked.
//!
//! Its own process: the cell holds process-wide state (the parking flag).

use crate::common::admin_side_serving;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MemberStatus, MeshId, NodeId};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{DrainReceipt, NodeState, Status, StatusReply, StatusRequest};
use rafka_node_rpc_testkit::node;
use std::time::Duration;

async fn call(admin: &crate::common::AdminSide, target: &NodeTarget, req: &StatusRequest) -> StatusReply {
    let opts = CallOptions { budget: rafka_node_rpc::Budget::Split { send: Duration::from_secs(5), reply: Duration::from_secs(30) }, ..CallOptions::default() };
    match admin.observer.client.call::<Status>(target, req, &opts).await.0 {
        RpcOutcome::Reply(r) => r.into_value(),
        other => panic!("{} did not end in a reply: {other:?}", req.op()),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stopped_node_parks_and_a_started_node_rejoins_as_the_same_birth() {
    if crate::own_process::delegated(module_path!(), "a_stopped_node_parks_and_a_started_node_rejoins_as_the_same_birth") {
        return;
    }
    let fabric = FabricId::mint();
    let admin = admin_side_serving("127.0.0.1".parse().unwrap(), &fabric, MemberStatus::ReadyForTraffic, |b| b).await;
    let dir = std::env::temp_dir().join(format!("park-start-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    RuntimeFact::of_this_process("cell").unwrap().write_record(&dir).unwrap();
    let key = node::load_or_mint_key(&dir).unwrap();
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id: fabric,
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![admin.seed.clone()],
        launcher: Some(admin.launcher.clone()),
        data_dir: dir.clone(),
        mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
        mesh_issuer: None,
    };
    admin.deployed(&launch, &key);
    let running = node::start(&launch, |b, _| b).await.expect("the node comes up");

    let book = &admin.observer.membership.book;
    let mut heard = book.heard_changes();
    while book.get(launch.node_id.as_str()).is_none_or(|(d, _)| d.status != MemberStatus::ReadyForTraffic) {
        heard.changed().await.unwrap();
    }
    let (born, _) = book.get(launch.node_id.as_str()).unwrap();
    admin.observer.resolver.insert(ResolvedNode {
        node_id: born.node.node_id.clone(),
        name: born.node.name.clone(),
        endpoint_id: born.node.endpoint_id.0.parse().unwrap(),
        incarnation: born.node.incarnation.clone(),
        transport_addr: born.node.transport_addr,
    });
    let target = NodeTarget::ExactNode(born.node.node_id.clone());
    let (node_id, incarnation) = (born.node.node_id.clone(), born.node.incarnation.clone());
    let probe = StatusRequest::ProbeNodeState { node_id: node_id.clone(), incarnation: incarnation.clone() };
    let stop = StatusRequest::StopNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_park".into(), attempt: 1, operation: "stop-node:mesh1.rpc.1".into() };
    let start = StatusRequest::StartNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld_park".into(), attempt: 2, operation: "start-node:mesh1.rpc.1".into() };

    // A live node refuses a start: only a parked one starts.
    assert!(matches!(call(&admin, &target, &start).await, StatusReply::RejectedInvalidNodeTransition { current: NodeState::ReadyForTraffic }), "a start of a live node is refused naming its state");

    // The stop is answered Left on its own call, with the drain's receipt.
    let left = call(&admin, &target, &stop).await;
    assert!(matches!(left, StatusReply::Left { receipt: DrainReceipt::Established { .. } }), "{left:?}");
    assert!(matches!(call(&admin, &target, &probe).await, StatusReply::CurrentParked { .. }), "a parked node answers CurrentParked");
    assert_eq!(running.membership.neighbours(), 0, "the node left its gossip topic");
    // Stopping a parked node is done: the same receipt again.
    assert_eq!(call(&admin, &target, &stop).await, left);

    // The node gossips nothing while parked: the admin hears no digest of it after the stop.
    let seq_at_stop = book.get(launch.node_id.as_str()).map(|(d, _)| d.digest_seq).unwrap();
    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(book.get(launch.node_id.as_str()).map(|(d, _)| d.digest_seq), Some(seq_at_stop), "no digest of a parked node reaches the admin");
    assert!(node::stop_commanded(), "the process is parked");

    // The start: the same birth rejoins through the admin that commands it.
    admin.deployed(&launch, &key);
    assert_eq!(call(&admin, &target, &start).await, StatusReply::Started);
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    loop {
        if matches!(call(&admin, &target, &probe).await, StatusReply::Current { state: NodeState::ReadyForTraffic, .. }) {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "the started node never reported ready");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let (after, _) = book.get(launch.node_id.as_str()).unwrap();
    assert_eq!((after.node.node_id.clone(), after.node.incarnation.clone(), after.node.endpoint_id.clone(), after.node.transport_addr), (born.node.node_id.clone(), born.node.incarnation.clone(), born.node.endpoint_id.clone(), born.node.transport_addr), "the same birth at the same endpoint and port");
    assert!(after.digest_seq > seq_at_stop, "its digest sequence continued across the cycle");
    assert!(!node::stop_commanded(), "the process is live again");
    assert!(running.membership.neighbours() >= 1, "the node is a neighbour again");
    let _ = std::fs::remove_dir_all(&dir);
}
