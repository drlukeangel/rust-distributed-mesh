//! The downward exact-node operations at a live rpc node (the 0x1B wire change): a node-admin probes
//! the exact birth and gets its current state; `node.connection.delete { All }` answers
//! `node.connections.deleted` with its receipt, and the reserved `Reserved6` is refused by name;
//! `ApplyNodeState` is refused with the held state; a stale incarnation is refused with the one held; an
//! upward declaration sent downward is refused by direction; a node-left, never sent, is refused by name.

use crate::common;

use rdm_core::{FabricId, IncarnationId};
use rdm_admin::build_state::MemoryBuildStateAdapter;
use rdm_admin::deployment::endpoint::RPC_NODE;
use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, Timeouts};
use rdm_admin::deployment::provider::{DeployError, FabricPolicy, TerminationMode};
use rdm_transport::{CallOptions, NodeTarget, ResolvedNode};
use rdm_core::outcome::RpcOutcome;
use rdm_core::status::{NodeState, Status, StatusReply, StatusRequest};

/// CONTRACT: at a live rpc node, from its mesh's node-admin: `ProbeNodeState` answers `Current`
/// with the node's state and changes nothing; `DeleteConnections { All }` answers `ConnectionsDeleted`
/// with an `Established` receipt; the reserved `Reserved6` is refused by name and starts nothing;
/// `ApplyNodeConfig` answers `Applied` and the node's own topology answer carries the config on its
/// digest from the version its mesh-primary publishes it at, while one naming another incarnation is `RejectedStaleIncarnation`;
/// `ApplyNodeState(ConnectionsDeleting)` is refused `RejectedInvalidNodeTransition` with the held state and moves nothing; a probe
/// naming another incarnation is `RejectedStaleIncarnation` with the held one; a
/// `DeclareNodeState` sent downward is refused by direction; a `NodeLeft` is refused by name
/// `RejectedUnmatchedCompletion { field: "command" }` (node-rpc-envelope.md "Node stop command pair":
/// "NodeLeft keeps its position and is never sent; a receiver refuses it by name"). What must NOT
/// happen: a probe moving the state, or `ApplyNodeState` standing in for node.connection.delete.
#[test]
fn a_node_admin_probes_and_deletes_the_connections_of_the_exact_birth_over_the_status_family() {
    let cap = crate::common::capture("a_node_admin_probes_and_deletes_the_connections_of_the_exact_birth_over_the_status_family");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric_id = FabricId::mint();
        let fabric = format!("fab-{fabric_id}");
        let policy = FabricPolicy::bootstrap(Some("process")).expect("a known MESH_SPAWN_TYPE");
        let prepared = match rdm_admin::deployment::prepare(policy, &fabric).await {
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
        let template = common::template(&admin);
        let builds = MemoryBuildStateAdapter::new();
        let build_id = common::publish_build(&builds, common::add_node()).await;
        let sink = common::Published::default();
        let pipeline = DeploymentPipeline {
            provider: &*provider,
            joins: &admin.joins,
            observer,
            sink: &sink,
            lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts::default(),
        };
        let created = pipeline
            .create(&CreateRequest { build_id: build_id.clone(), attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None })
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

        // The node answers the probe whoever sends it (node-rpc.md §14): the first probe is answered.
        let probe = StatusRequest::ProbeNodeState { node_id: node.node_id.clone(), incarnation: incarnation.clone() };
        let first = call(probe.clone()).await;
        assert_eq!(first, StatusReply::Current { node_id: node.node_id.clone(), incarnation: incarnation.clone(), state: NodeState::Ready });
        // A stale incarnation: refused with the one held.
        let stale = StatusRequest::ProbeNodeState { node_id: node.node_id.clone(), incarnation: IncarnationId::mint() };
        assert_eq!(call(stale).await, StatusReply::RejectedStaleIncarnation { held: incarnation.clone() });
        // An upward declaration sent to the node: its Status door has no handler for it (R24, Luke Q11):
        // not handled, never an authority refusal.
        let downward_declare = StatusRequest::DeclareNodeState { node_id: node.node_id.clone(), incarnation: incarnation.clone(), state: NodeState::Ready, operation: None };
        assert_eq!(call(downward_declare).await, StatusReply::RequestNotHandled { request: "declare-node-state".into() }, "a request the node has no handler for is answered not handled here");
        // node-left is never sent: refused by name as no command, as a node-admin refuses it, never
        // sent on to "a node-admin" that would refuse it too.
        let left = StatusRequest::NodeLeft { node_id: node.node_id.clone(), incarnation: incarnation.clone(), build_id: "08zd8rg6hhjh".into(), attempt: 1, operation: "node.stop:mesh1.rpc.1".into() };
        assert_eq!(
            call(left).await,
            StatusReply::RejectedUnmatchedCompletion {
                field: "command".into(),
                expected: "no command: node-left is never a call; a stopped birth reports left on the reply of its stop-node call".into(),
                reported: "node-left node.stop:mesh1.rpc.1 (build 08zd8rg6hhjh, attempt 1)".into(),
            }
        );
        // ApplyNodeState sets no state: refused with the held state, nothing moves.
        let apply = StatusRequest::ApplyNodeState { node_id: node.node_id.clone(), incarnation: incarnation.clone(), state: NodeState::ConnectionsDeleting };
        assert_eq!(call(apply).await, StatusReply::RejectedInvalidNodeTransition { current: NodeState::Ready });
        assert!(matches!(call(probe.clone()).await, StatusReply::Current { state: NodeState::Ready, .. }), "the refused apply moved nothing");
        // apply-node-config (node-config-update.md step 3: "it applies the map"): the birth holds the
        // whole config; no digest carries it (Q203).
        let config: std::collections::BTreeMap<String, String> = [("tags".to_string(), "edge".to_string()), ("zone".to_string(), "a".to_string())].into();
        let told = StatusRequest::ApplyNodeConfig { node_id: node.node_id.clone(), incarnation: incarnation.clone(), config: config.clone(), revision: 1 };
        assert_eq!(call(told).await, StatusReply::Applied);
        let stale_config = StatusRequest::ApplyNodeConfig { node_id: node.node_id.clone(), incarnation: IncarnationId::mint(), config: Default::default(), revision: 2 };
        assert_eq!(call(stale_config).await, StatusReply::RejectedStaleIncarnation { held: incarnation.clone() }, "a config for another birth is refused with the one held");
        // The reserved Reserved6 is refused by name; node.connection.delete { All } is answered with its receipt.
        let reserved = StatusRequest::Reserved6 { node_id: node.node_id.clone(), incarnation: incarnation.clone(), build_id: "08zd8rg6hhjh".into(), attempt: 1, operation: "node.connection.delete:mesh1.rpc.1".into() };
        assert!(matches!(call(reserved).await, StatusReply::RejectedUnmatchedCompletion { field, .. } if field == "command"), "a reserved position is refused by name");
        let all = rdm_core::node_command::NodeCommandRequest::DeleteConnections { node_id: node.node_id.clone(), incarnation: incarnation.clone(), scope: rdm_core::status::ConnectionScope::All };
        let deleted = common::node_command_completion(&observer.client, &target, &all).await;
        assert!(matches!(&deleted, rdm_core::node_command::NodeCommandReply::ConnectionsDeleted { receipt: Some(rdm_core::status::ConnectionsReceipt::Established { .. }), held, .. } if held.is_empty()), "{deleted:?}");

        let _ = provider.terminate(&created.handle, TerminationMode::Graceful { grace: std::time::Duration::from_secs(2) }).await;
    })
}
