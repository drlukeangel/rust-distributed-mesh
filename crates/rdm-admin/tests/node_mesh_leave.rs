//! Functional cell for `node.mesh.leave` (`docs/architecture/lifecycles/node-mesh-leave.md`).

use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome,
};
use rdm_admin::deployment::pipeline::{
    CommandAdmission, CommandContext, Completion, DeploymentPipeline, LaunchTemplate, LeaveStep,
    NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink,
};
use rdm_admin::deployment::provider::{
    DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch,
    TerminationMode,
};
use rdm_admin::join::Joins;
use rdm_admin::model::{IncarnationId, Node, NodeId, PathName, ProviderKind};
use rdm_core::connections::{ConnectionEnd, ConnectionsHeld};
use rdm_core::node_command::{NodeCommandReply, NodeCommandRequest};
use rdm_core::ping::{Ping, PingRequest};
use rdm_core::status::{ConnectionScope, WhichMesh};
use rdm_core::{EndpointId, FabricId, MemberStatus, MeshDigest, MeshId, MeshNode};
use rdm_node::connections_writer::ConnectionsWriter;
use rdm_node::node_self::{ChannelFrames, NodeSelf};
use rdm_node::storage::MemoryConnectionsStorage;
use rdm_transport::topology::TopologyView;
use rdm_transport::{LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, ServerBuilder};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt as _;

struct Captured {
    attributes: BTreeMap<String, String>,
}

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

impl Capture {
    fn new(cell: &str) -> Self {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (capture, layer) = rdm_transport::span_capture(cell, exporter.clone());
        Self {
            exporter,
            capture,
            dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
        }
    }

    fn named(&self, name: &str) -> Vec<Captured> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| Captured {
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }
}

struct LeaveTestProvider;

#[async_trait::async_trait]
impl DeploymentProvider for LeaveTestProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }

    fn control_domain(&self) -> String {
        "process:leave-cell".into()
    }

    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn {
            node: spec.node.to_string(),
            reason: "this cell tests node.mesh.leave, not spawn".into(),
        })
    }

    async fn terminate(
        &self,
        _handle: &DeploymentHandle,
        _mode: TerminationMode,
    ) -> Result<(), DeployError> {
        Ok(())
    }

    async fn inspect(&self, _handle: &DeploymentHandle) -> DeploymentStatus {
        DeploymentStatus::Running
    }

    async fn signal_stop(&self, _handle: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }

    async fn find(&self, _spec: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
        Ok(Found::NeverLaunched)
    }
}

struct NoObserver;

#[async_trait::async_trait]
impl NodeObserver for NoObserver {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }

    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }

    async fn send_command(&self, _: &Node, _: rdm_admin::node_commands::NodeCommand, _: &CommandContext) -> CommandAdmission {
        CommandAdmission::Admitted {}
    }

    async fn await_completion(
        &self,
        _: &Node,
        _: rdm_admin::node_commands::NodeCommand,
        _: &CommandContext,
        _: Duration,
    ) -> Completion {
        Completion::Received {}
    }
}

struct NoSink;

impl TopologySink for NoSink {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, _: &PathName, _: &NodeId, _: &IncarnationId) {}
}

// @feature: node-mesh-leave
/// Page: docs/architecture/lifecycles/node-mesh-leave.md:18
/// "Entry: The exact `(node.id, incarnation)`, connections-deleted: `node.connections.delete { All }`
/// completed with its receipt (`Established`, or `Deadline` with the held ids recorded). A node that still
/// holds a connection is refused `BusyHoldingConnections { connections }` and leaves nothing"
/// Page: docs/architecture/lifecycles/node-mesh-leave.md:27
/// "1. The commanding workflow has completed `node.connections.delete { All }` on the node and holds its receipt.
///  2. `leaving`: the node is commanded to leave. It checks it has no connection open... one still open is
///     `BusyHoldingConnections { connections }`...
///  5. `node.mesh.left`: `MeshLeft { left }` standalone... A node already left since its last start answers
///     the acknowledgement and the completion at once and leaves nothing again."
#[test]
fn node_mesh_leave_refuses_busy_holding_connections_until_deleted_then_leaves_mesh_and_records_pipeline_receipts() {
    let capture = Capture::new("node_mesh_leave_refuses_busy_then_leaves");
    let _guard = tracing::dispatcher::set_default(&capture.dispatch);
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let root_dir = std::env::temp_dir().join(format!(
            "rdm-node-mesh-leave-cell-{}-{}",
            std::process::id(),
            BuildId::mint()
        ));
        std::fs::create_dir_all(&root_dir).unwrap();

        let fabric_id = FabricId::mint();
        let mesh_id = MeshId::mint();
        let node_name: PathName = "mesh1.rpc.1".parse().unwrap();
        let node_id = NodeId::mint();
        let incarnation = IncarnationId::mint();

        let peer_name: PathName = "mesh1.rpc.2".parse().unwrap();
        let peer_id = NodeId::mint();
        let peer_incarnation = IncarnationId::mint();

        // Set up a peer server so `data_plane` can open a real pooled data-plane connection to it.
        let peer_key = iroh::SecretKey::generate();
        let peer_ep = rdm_transport::endpoint::bind(
            peer_key.clone(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let peer_addr = peer_ep
            .bound_sockets()
            .into_iter()
            .find(|a| a.is_ipv4())
            .unwrap();
        let peer_resolver = Arc::new(LiveNodeResolver::default());
        let peer_client = Arc::new(NodeRpcClient::new(peer_ep.clone(), peer_resolver));
        let peer_server = ServerBuilder::new()
            .serve_core(peer_client, None)
            .build(rdm_transport::ServedBirth {
                node_id: peer_id.to_string(),
                incarnation: peer_incarnation.to_string(),
            })
            .unwrap();
        let _peer_router = iroh::protocol::Router::builder(peer_ep.clone())
            .accept(rdm_transport::ALPN, peer_server)
            .spawn();

        // Set up the subject node's NodeSelf surface with a real ConnectionsWriter and data-plane client.
        let node_key = iroh::SecretKey::generate();
        let node_ep = rdm_transport::endpoint::bind(
            node_key.clone(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let node_addr = node_ep
            .bound_sockets()
            .into_iter()
            .find(|a| a.is_ipv4())
            .unwrap();

        let resolver = Arc::new(LiveNodeResolver::default());
        resolver.apply(
            ResolvedNode {
                node_id: peer_id.clone(),
                name: peer_name.clone(),
                endpoint_id: peer_key.public(),
                transport_addr: peer_addr,
                incarnation: peer_incarnation.clone(),
            },
            None,
        );

        let connections_store = Arc::new(MemoryConnectionsStorage::default());
        let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
        let writer = Arc::new(ConnectionsWriter::new(
            ConnectionEnd {
                name: node_name.clone(),
                node_id: node_id.clone(),
                incarnation: Some(incarnation.clone()),
            },
            connections_store,
            held,
        ));

        let control_client = Arc::new(
            NodeRpcClient::new(node_ep.clone(), resolver.clone()).with_caller_system("rdm"),
        );
        let data_plane = Arc::new(
            NodeRpcClient::new(node_ep.clone(), resolver.clone())
                .with_caller_system("rdm")
                .with_connection_observer(writer.clone()),
        );
        let node_server = ServerBuilder::new()
            .serve_core(control_client.clone(), None)
            .build(rdm_transport::ServedBirth {
                node_id: node_id.to_string(),
                incarnation: incarnation.to_string(),
            })
            .unwrap();

        let clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
        clock.adopt(1_000_000);
        let g = iroh_gossip::net::Gossip::builder().spawn(node_ep.clone());
        let topology_view = TopologyView::join(
            &g,
            &node_ep,
            &fabric_id,
            "mesh1",
            &mesh_id,
            &node_name.to_string(),
            clock,
            Vec::new(),
            &[],
        )
        .await
        .unwrap();

        let status_cell = Arc::new(Mutex::new(MemberStatus::Ready));
        let digest_base = MeshDigest {
            held_hash: Default::default(),
            fabric_id: fabric_id.clone(),
            node: MeshNode {
                node_id: node_id.clone(),
                name: node_name.clone(),
                endpoint_id: EndpointId(node_key.public().to_string()),
                incarnation: incarnation.clone(),
                supersedes: None,
                transport_addr: node_addr,
                runtime: None,
            },
            status: MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            emitted_at_rafka_ms: 0,
            digest_seq: 1,
            mesh_id: Some(mesh_id.clone()),
            in_flight: None,
            load: None,
            gossip: None,
            data_dir: None,
            is_primary: None,
            is_fabric_primary: None,
        };

        let status_for_set = status_cell.clone();
        let digest_for_set = digest_base.clone();
        let set_status = Arc::new(move |s: MemberStatus| {
            *status_for_set.lock().unwrap() = s;
            let mut d = digest_for_set.clone();
            d.status = s;
            d
        });
        let status_for_get = status_cell.clone();
        let get_status = Arc::new(move || *status_for_get.lock().unwrap());

        let own = Arc::new(NodeSelf::new(
            node_id.clone(),
            incarnation.clone(),
            node_name.clone(),
            node_server,
            control_client,
            resolver,
            topology_view,
            set_status,
            get_status,
            None,
            Duration::from_secs(2),
        ));
        own.set_connections(writer.clone());
        own.set_data_plane(data_plane.clone());

        // Open a live Direct data-plane connection to `mesh1.rpc.2`.
        let (ping_out, _) = data_plane
            .call::<Ping>(
                &NodeTarget::ExactNode(peer_id.clone()),
                &PingRequest::Ping {
                    payload: b"hello".to_vec(),
                },
                &rdm_transport::CallOptions::default(),
            )
            .await;
        assert_eq!(ping_out.name(), "Reply");
        writer.observations_landed().await;
        assert_eq!(
            own.live_routes().unwrap().len(),
            1,
            "data_plane call must record one open Direct connection"
        );

        // Set up DeploymentPipeline::leave_mesh.
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 1000,
            })
            .await
            .unwrap();
        builds
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_id.clone(),
                attempt: 1,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();

        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: fabric_id.clone(),
            executable: std::env::current_exe().unwrap(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher {
                name: "mesh1.admin.1".parse().unwrap(),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
            },
            env: BTreeMap::new(),
            data_root: root_dir.clone(),
            store_dir: root_dir.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: Duration::from_secs(5),
            declared: Default::default(),
            binding: None,
        };
        let provider = LeaveTestProvider;
        let joins = Joins::default();
        let observer = NoObserver;
        let sink = NoSink;
        let pipeline = DeploymentPipeline {
            provider: &provider,
            joins: &joins,
            observer: &observer,
            sink: &sink,
            lifecycle: &NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts::default(),
        };

        let leave_req = NodeCommandRequest::LeaveMesh {
            node_id: node_id.clone(),
            incarnation: incarnation.clone(),
            build_id: build_id.to_string(),
            attempt: 1,
            operation: "node.mesh.leave:mesh1.rpc.1".into(),
        };

        // 1. Attempting `node.mesh.leave` while holding an open connection is refused with
        //    `BusyHoldingConnections` before acceptance, and the pipeline step fails.
        let busy_err = pipeline
            .leave_mesh(&build_id, 1, &node_name, async {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let reply = own
                    .serve_node_command(None, &leave_req, Box::new(ChannelFrames(tx)))
                    .await;
                assert!(
                    rx.try_recv().is_err(),
                    "BusyHoldingConnections must refuse before CommandAccepted"
                );
                match reply {
                    NodeCommandReply::BusyHoldingConnections { connections } => {
                        assert_eq!(connections.len(), 1);
                        Err(format!("busy-holding-connections: {connections:?}"))
                    }
                    other => panic!("expected BusyHoldingConnections, got {other:?}"),
                }
            })
            .await
            .expect_err("leave_mesh must fail when node holds open connections");
        assert_eq!(busy_err.step, LeaveStep::LeaveMesh.name());
        assert!(busy_err.reason.contains("busy-holding-connections"));
        assert_eq!(
            *status_cell.lock().unwrap(),
            MemberStatus::Ready,
            "refused leave must not transition status to Leaving"
        );

        // 2. Delete connections (`node.connections.delete { All }`) first, then run `leave_mesh` on attempt 2.
        let delete_conns_req = NodeCommandRequest::DeleteConnections {
            node_id: node_id.clone(),
            incarnation: incarnation.clone(),
            scope: ConnectionScope::All,
        };
        let (tx_del, _rx_del) = tokio::sync::mpsc::unbounded_channel();
        let del_reply = own
            .serve_node_command(None, &delete_conns_req, Box::new(ChannelFrames(tx_del)))
            .await;
        assert!(
            matches!(del_reply, NodeCommandReply::ConnectionsDeleted { .. }),
            "expected ConnectionsDeleted, got {del_reply:?}"
        );
        assert!(
            own.live_routes().unwrap().is_empty(),
            "all connections must be closed after DeleteConnections All"
        );

        builds
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_id.clone(),
                attempt: 2,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();

        pipeline
            .leave_mesh(&build_id, 2, &node_name, async {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                let reply = own
                    .serve_node_command(None, &leave_req, Box::new(ChannelFrames(tx)))
                    .await;
                let accepted = rx.try_recv().expect("CommandAccepted must be sent first");
                assert_eq!(
                    accepted,
                    NodeCommandReply::CommandAccepted {
                        node_id: node_id.clone(),
                        incarnation: incarnation.clone(),
                    }
                );
                match reply {
                    NodeCommandReply::MeshLeft {
                        node_id: left_node,
                        incarnation: left_inc,
                        left,
                    } => {
                        assert_eq!(left_node, node_id);
                        assert_eq!(left_inc, incarnation);
                        assert_eq!(left, vec![WhichMesh::Own]);
                        Ok(())
                    }
                    other => Err(format!("unexpected leave reply: {other:?}")),
                }
            })
            .await
            .expect("leave_mesh must succeed once connections are deleted");

        assert_eq!(
            *status_cell.lock().unwrap(),
            MemberStatus::Leaving,
            "node.mesh.leave must set status to Leaving"
        );

        // 3. A node already left since its last start answers CommandAccepted and MeshLeft at once.
        let (tx_again, mut rx_again) = tokio::sync::mpsc::unbounded_channel();
        let again_reply = own
            .serve_node_command(None, &leave_req, Box::new(ChannelFrames(tx_again)))
            .await;
        assert!(matches!(
            rx_again.try_recv(),
            Ok(NodeCommandReply::CommandAccepted { .. })
        ));
        assert_eq!(
            again_reply,
            NodeCommandReply::MeshLeft {
                node_id: node_id.clone(),
                incarnation: incarnation.clone(),
                left: vec![WhichMesh::Own],
            }
        );

        let view = builds.read_build(&build_id).await.unwrap();
        let attempt2_steps: Vec<&str> = view
            .steps
            .iter()
            .filter(|r| r.attempt == 2 && r.outcome == StepOutcome::Complete)
            .map(|r| r.step.as_str())
            .collect();
        assert_eq!(
            attempt2_steps,
            vec![LeaveStep::LeaveMesh.name(), LeaveStep::Complete.name()]
        );

        node_ep.close().await;
        peer_ep.close().await;
        let _ = std::fs::remove_dir_all(&root_dir);
    });

    drop(rt);

    assert_eq!(
        capture
            .named("rdm.node.mesh.reject.via-busy-holding-connections")
            .len(),
        1,
        "expected one busy-holding-connections rejection span"
    );
    assert_eq!(
        capture.named("rdm.node.mesh.update.via-leaving").len(),
        1,
        "expected one via-leaving span (second call after already left does not leave again)"
    );
    let left_spans = capture.named("rdm.node.mesh.update.via-left");
    assert_eq!(left_spans.len(), 1, "expected one via-left span");
    assert_eq!(
        left_spans[0].attributes.get("which").map(String::as_str),
        Some("own")
    );
}
