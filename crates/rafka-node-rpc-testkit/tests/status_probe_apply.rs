//! The downward exact-node operations at a live rpc node (the 0x1B reseal): a node-admin probes
//! the exact birth and gets its current state; applies `Draining` and gets the work in flight,
//! the current count on a repeat; a stale incarnation is refused with the one held; an upward
//! declaration sent downward is refused by direction.

mod common;

use rafka_mesh_entity::{FabricId, IncarnationId};
use rafka_node_admin_core::build_state::MemoryBuildStateAdapter;
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, DeploymentPipeline, Timeouts};
use rafka_node_admin_core::deployment::provider::{DeployError, FabricPolicy, TerminationMode};
use rafka_node_rpc::{CallOptions, NodeTarget, ResolvedNode};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::status::{NodeState, NotAuthority, Status, StatusReply, StatusRequest};

/// CONTRACT: at a live rpc node, from its mesh's node-admin: `ProbeNodeState` answers `Current`
/// with the node's state and changes nothing; `ApplyNodeState(Draining)` answers
/// `NodeDrainingApplied { in_flight }` and so does a repeat; a probe naming another incarnation
/// is `RejectedStaleIncarnation` with the held one; a `DeclareNodeState` sent downward is
/// refused by direction. What must NOT happen: a probe moving the state, or the generic
/// `Draining { reason }` refusal standing in for the drain's success.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_node_admin_probes_and_drains_the_exact_birth_over_the_status_family() {
    let fabric_id = FabricId::mint();
    let fabric = format!("fab-{fabric_id}");
    let policy = FabricPolicy::bootstrap(Some("process")).expect("a known MESH_SPAWN_TYPE");
    let prepared = match rafka_node_admin_core::deployment::prepare(policy, &fabric).await {
        Ok(p) => p,
        Err(DeployError::Unsupported { reason, .. }) => {
            eprintln!("process provider unsupported here: {reason}");
            return;
        }
        Err(e) => panic!("preparing the process provider failed: {e}"),
    };
    let provider = prepared.provider.clone();
    let admin = common::admin_side(prepared.admin_ip, &fabric_id).await;
    let observer = &admin.observer;
    // The admin side publishes its digest on joining (`common::admin_side`), so the node holds it as a node-admin.
    let template = common::template(&fabric_id, admin.seed.clone(), admin.launcher.clone());
    let builds = MemoryBuildStateAdapter::new();
    let build_id = common::publish_build(&builds, common::add_node()).await;
    let sink = common::Published::default();
    let pipeline = DeploymentPipeline {
        provider: &*provider,
        joins: &admin.joins,
        observer,
        sink: &sink,
        lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };
    let created = pipeline
        .create(&CreateRequest { build_id: build_id.clone(), attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, restart_of: None })
        .await
        .expect("the node is born");
    let node = created.node.clone();
    let incarnation = node.incarnation_id.clone().expect("a born node has its incarnation");
    let endpoint_id = node.endpoint_id.as_ref().expect("endpoint id").0.parse::<iroh::PublicKey>().unwrap();
    observer.resolver.insert(ResolvedNode {
        node_id: node.node_id.clone(),
        name: node.name.clone(),
        endpoint_id,
        incarnation: incarnation.clone(),
        transport_addr: node.transport_addr.expect("transport address"),
    });
    let target = NodeTarget::ExactNode(node.node_id.clone());
    let call = |req: StatusRequest| {
        let target = target.clone();
        async move {
            match observer.client.call::<Status>(&target, &req, &CallOptions::default()).await.0 {
                RpcOutcome::Reply(r) => r.value().clone(),
                other => panic!("expected a reply: {other:?}"),
            }
        }
    };

    // The node admits a downward operation only from a node-admin it holds in its own membership
    // book; the admin's digest reaches it by gossip after its join. Probe until the node has heard
    // the admin (bounded), then hold it to its answer.
    let probe = StatusRequest::ProbeNodeState { node_id: node.node_id.clone(), incarnation: incarnation.clone() };
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    let first = loop {
        let reply = call(probe.clone()).await;
        if !matches!(reply, StatusReply::RejectedNotAuthority { .. }) || tokio::time::Instant::now() >= deadline {
            break reply;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    };
    assert_eq!(first, StatusReply::Current { node_id: node.node_id.clone(), incarnation: incarnation.clone(), state: NodeState::ReadyForTraffic });
    // A stale incarnation: refused with the one held.
    let stale = StatusRequest::ProbeNodeState { node_id: node.node_id.clone(), incarnation: IncarnationId::mint() };
    assert_eq!(call(stale).await, StatusReply::RejectedStaleIncarnation { held: incarnation.clone() });
    // An upward declaration sent downward: refused by direction.
    let downward_declare = StatusRequest::DeclareNodeState { node_id: node.node_id.clone(), incarnation: incarnation.clone(), state: NodeState::ReadyForTraffic };
    assert!(matches!(call(downward_declare).await, StatusReply::RejectedNotAuthority { why: NotAuthority::ReceiverNotPrimary { .. } }));
    // Draining applied: the work in flight, and the current count on a repeat.
    let drain = StatusRequest::ApplyNodeState { node_id: node.node_id.clone(), incarnation: incarnation.clone(), state: NodeState::Draining };
    assert_eq!(call(drain.clone()).await, StatusReply::NodeDrainingApplied { in_flight: 0 });
    assert_eq!(call(drain).await, StatusReply::NodeDrainingApplied { in_flight: 0 }, "a repeat answers the current count, never AlreadyApplied");
    // The probe after the drain reports it.
    assert_eq!(call(probe).await, StatusReply::Current { node_id: node.node_id.clone(), incarnation, state: NodeState::Draining });

    let _ = provider.terminate(&created.handle, TerminationMode::Graceful { grace: std::time::Duration::from_secs(2) }).await;
}
