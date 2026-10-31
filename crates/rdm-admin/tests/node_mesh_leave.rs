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

    async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn {
            node: spec.node.to_string(),
            reason: "this cell tests node.mesh.leave, not spawn".into(),
        })
    }

    async fn terminate(
        &self,
        _handle: &DeploymentHandle,
        _mode: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }

    async fn inspect(&self, _handle: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        DeploymentStatus::Running
    }

    async fn signal_stop(&self, _handle: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }

    async fn find(&self, _spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<Found, DeployError> {
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
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:5-10 "`node.mesh.leave` takes a node off its mesh and, for a node-admin, off the backbone too. It is the step node.stop and node.delete end their live part with, after the node's connections are deleted and never before (R-CR5): the data plane first, then the control plane. Progress is `leaving`, completion `node.mesh.left`. The node keeps its endpoint bound, its identity, name, port and data dir; a left node is reachable by a fresh Node RPC dial for its rename, stop, start or delete."
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:16 "Commander | The workflow that stops or deletes the node: its mesh-primary for a member, the fabric-primary for a mesh-primary (a mesh's last admin included, mesh-delete.md step 7). A Build's `node.mesh.leave` step (node replace steps 1 and Rollback) is commanded by the node's mesh-primary, after the step before it deleted the node's connections, and the mesh-primary holds the node's `node.mesh.left` for its rename. Commands go down"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:17 "Executor | The node itself"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:18 "Entry | The exact `(node.id, incarnation)`, connections-deleted: `node.connection.delete { All }` completed with its receipt (`Established`, or `Deadline` with the held ids recorded). A node that still holds a connection is refused `BusyHoldingConnections { connections }` and leaves nothing"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:19 "Leaves | Its gossip neighbours on its mesh (and the backbone, a node-admin) and its Node RPC pool; the control plane, which are not connections (R-CR5)"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:20 "Keeps | Its endpoint bound, node.id, incarnation, path and name, port, bin, data dir"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:21 "Progress | `leaving` from the command until the mesh is left (and the backbone, a node-admin): the command's acknowledgement, from which the mesh-primary writes leaving on the node's row; the node publishes nothing in that state and nothing after it (R-ON1)"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:22 "Completion | `node.mesh.left` on the commanding stream, from which the mesh-primary writes left on the node's row; the stop's `Stopped { receipt }` or the delete's next step follows it"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:23 "Gossip | None from the node. Its mesh-primary publishes NodeStopped from the stop's matching stopped frame, or NodeDeleted after node.delete's receipt; a mesh's last admin after mesh.deleted has its final mesh topology instead (mesh.delete step 7), and the fabric-primary publishes nothing for it. The reserved `NodeLeaving` / `NodeLeft` frame positions stay reserved and unpublished"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:24 "Wire | Standalone, the mesh-primary to the exact node on NodeCommand 0x22, server-streaming and never forwarded: `LeaveMesh { node_id, incarnation, build_id, attempt, operation }`, answered `CommandAccepted` (Started; leaving), then `MeshLeft { node_id, incarnation, left }` (Terminal; node.mesh.left), or before acceptance one refusal frame, `BusyHoldingConnections { connections }` among them; appended variants, `durable format moved`. Inside a stop its acknowledgement and completion are Data frames on the stop's stream, `Leaving` then `StopMeshLeft { left }` (node.stop step 3). Status 0x1B positions `LeaveMesh` and `MeshLeft` are reserved and refused by name"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:25 "Spans | `rdm.node.mesh.update.via-leaving` at the command, `via-left` once per mesh left, each naming mesh, MeshId and which (`own` or `backbone`)"
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:29-30 "1. The commanding workflow has completed `node.connection.delete { All }` on the node and holds its receipt. Inside node.stop this is step 2 of the stop; the leave is its step 3."
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:31-35 "2. `leaving`: the node is commanded to leave. It checks it has no connection open, read from the connections open now, a connection whose row is still owed included (Q169); one still open is `BusyHoldingConnections { connections }`, the leave does not start, and the stop's `Deadline` evidence names why. Otherwise it acknowledges the command, `CommandAccepted` standalone or `Leaving` inside a stop, and the mesh-primary writes leaving from it."
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:36-37 "3. The node leaves its mesh: its gossip neighbours on `f(FabricId, MeshId)`. A node-admin then leaves the backbone. Each one left emits `via-left`."
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:38-39 "4. The node cuts its Node RPC pool. Its endpoint stays bound, and the commanding call's stream and the connection to its mesh-primary its owed declarations ride stay open."
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:40-45 "5. `node.mesh.left`: `MeshLeft { left }` standalone, `StopMeshLeft { left }` inside a stop, and the mesh-primary writes left from it. The node publishes nothing. Inside a stop, `Stopped { receipt }` follows on the stop's stream and the mesh-primary publishes NodeStopped from it; inside a delete, the provider's runtime inspect follows. A node already left since its last start answers the acknowledgement and the completion at once and leaves nothing again."
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:84-95 "A node-admin answers `\"left\": [\"own\", \"backbone\"]`. A node still holding a connection answers one refusal frame and nothing follows it ... Inside node.stop the leave is a step of the stop's own stream, the progress view node-stop.md shows: `Leaving` (Data), then `StopMeshLeft { left }` (Data), the frame before `node.stopped`."
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:123-126 "`BusyHoldingConnections` leaves nothing and names the connections; the commanding workflow's `Deadline` receipt is the evidence. A mesh that cannot be left is reported by name with which one, the primitive stays `leaving`, and the workflow's next attempt resumes it; the node never re-publishes meanwhile."
/// CONTRACT: docs/architecture/lifecycles/node-mesh-leave.md:132-135 "Observability contract: Command | `rdm.node.mesh.update.via-leaving` | mesh, mesh_id, operation; Mesh left | `rdm.node.mesh.update.via-left`, once per mesh the node was on: its own mesh, and the backbone for a node-admin | mesh, mesh_id, which (`own` or `backbone`); Busy | `rdm.node.mesh.reject.via-busy-holding-connections` | connections; Completion | the `MeshLeft` frame, or `StopMeshLeft` inside a stop | operation, build, attempt"
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

        // 2. Delete connections (`node.connection.delete { All }`) first, then run `leave_mesh` on attempt 2.
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

        // Keeps (node-mesh-leave.md:9-10, 20, 38-39): endpoint stays bound, identity kept, and
        // the left node is still reachable by a fresh Node RPC dial.
        assert!(
            !node_ep.bound_sockets().is_empty(),
            "left node must keep its endpoint bound"
        );
        let _node_router = iroh::protocol::Router::builder(node_ep.clone())
            .accept(
                rdm_transport::ALPN,
                ServerBuilder::new()
                    .serve_core(
                        Arc::new(NodeRpcClient::new(
                            node_ep.clone(),
                            Arc::new(LiveNodeResolver::default()),
                        )),
                        None,
                    )
                    .build(rdm_transport::ServedBirth {
                        node_id: node_id.to_string(),
                        incarnation: incarnation.to_string(),
                    })
                    .unwrap(),
            )
            .spawn();
        let peer_to_left_resolver = Arc::new(LiveNodeResolver::default());
        peer_to_left_resolver.apply(
            ResolvedNode {
                node_id: node_id.clone(),
                name: node_name.clone(),
                endpoint_id: node_key.public(),
                transport_addr: node_addr,
                incarnation: incarnation.clone(),
            },
            None,
        );
        let fresh_dial_client = NodeRpcClient::new(peer_ep.clone(), peer_to_left_resolver);
        let (dial_out, _) = fresh_dial_client
            .call::<Ping>(
                &NodeTarget::ExactNode(node_id.clone()),
                &PingRequest::Ping {
                    payload: b"after-leave".to_vec(),
                },
                &rdm_transport::CallOptions::default(),
            )
            .await;
        assert_eq!(
            dial_out.name(),
            "Reply",
            "a left node keeps its endpoint bound and is reachable by a fresh Node RPC dial"
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

        // 4. Status 0x1B reserved positions `LeaveMesh` and `MeshLeft` are refused by name
        //    (node-mesh-leave.md:24).
        let reserved_leave = rdm_node::node_self::refuse_reserved(
            "mesh1.rpc.1",
            "mesh1.admin.1",
            &rdm_core::status::StatusRequest::LeaveMesh {
                node_id: node_id.clone(),
                incarnation: incarnation.clone(),
                build_id: build_id.to_string(),
                attempt: 2,
                operation: "node.mesh.leave:mesh1.rpc.1".into(),
            },
        );
        assert!(
            matches!(
                reserved_leave,
                rdm_core::status::StatusReply::RejectedUnmatchedCompletion { ref field, .. } if field == "command"
            ),
            "Status 0x1B LeaveMesh is reserved and refused by name: {reserved_leave:?}"
        );
        let reserved_mesh_left = rdm_core::status::StatusReply::MeshLeft {
            node_id: node_id.clone(),
            incarnation: incarnation.clone(),
            left: vec![WhichMesh::Own],
        };
        assert_eq!(
            reserved_mesh_left.name(),
            "mesh-left",
            "Status 0x1B reply position MeshLeft keeps its reserved variant name"
        );

        // 5. A node-admin on both its own mesh and the backbone leaves both (`left: [Own, Backbone]`),
        //    and inside `node.stop` (`StopNode`) streams `Leaving` (Data) then `StopMeshLeft { left }`
        //    (Data) before `Stopped { receipt }` (node-mesh-leave.md:5-6, 19, 21-25, 34-43, 84, 91-95).
        let admin_name: PathName = "mesh1.admin.1".parse().unwrap();
        let admin_id = NodeId::mint();
        let admin_inc = IncarnationId::mint();
        let admin_ep = rdm_transport::endpoint::bind(
            iroh::SecretKey::generate(),
            "127.0.0.1:0".parse().unwrap(),
        )
        .await
        .unwrap();
        let admin_clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
        admin_clock.adopt(1_000_000);
        let admin_g = iroh_gossip::net::Gossip::builder().spawn(admin_ep.clone());
        let admin_top = TopologyView::join(
            &admin_g,
            &admin_ep,
            &fabric_id,
            "mesh1",
            &mesh_id,
            &admin_name.to_string(),
            admin_clock,
            Vec::new(),
            &[],
        )
        .await
        .unwrap();
        let admin_backbone = rdm_transport::topology::Backbone::join(
            &admin_g,
            &admin_ep,
            &admin_top,
            "mesh1",
            rdm_core::PublisherId {
                node: admin_name.to_string(),
                node_id: admin_id.clone(),
                incarnation: admin_inc.clone(),
            },
            Vec::new(),
        )
        .await
        .unwrap();
        let admin_resolver = Arc::new(LiveNodeResolver::default());
        let admin_client = Arc::new(NodeRpcClient::new(admin_ep.clone(), admin_resolver.clone()));
        let admin_server = ServerBuilder::new()
            .serve_core(admin_client.clone(), None)
            .build(rdm_transport::ServedBirth {
                node_id: admin_id.to_string(),
                incarnation: admin_inc.to_string(),
            })
            .unwrap();
        let mut admin_digest = digest_base.clone();
        admin_digest.node.node_id = admin_id.clone();
        admin_digest.node.name = admin_name.clone();
        admin_digest.node.incarnation = admin_inc.clone();
        let admin_self = Arc::new(
            NodeSelf::new(
                admin_id.clone(),
                admin_inc.clone(),
                admin_name.clone(),
                admin_server,
                admin_client,
                admin_resolver,
                admin_top,
                Arc::new(move |_| admin_digest.clone()),
                Arc::new(|| MemberStatus::Ready),
                None,
                Duration::from_secs(1),
            )
            .with_backbone(admin_backbone),
        );
        let (tx_admin_leave, mut rx_admin_leave) = tokio::sync::mpsc::unbounded_channel();
        let admin_leave_reply = admin_self
            .serve_node_command(
                None,
                &NodeCommandRequest::LeaveMesh {
                    node_id: admin_id.clone(),
                    incarnation: admin_inc.clone(),
                    build_id: build_id.to_string(),
                    attempt: 2,
                    operation: "node.mesh.leave:mesh1.admin.1".into(),
                },
                Box::new(ChannelFrames(tx_admin_leave)),
            )
            .await;
        let admin_accepted = rx_admin_leave
            .try_recv()
            .expect("CommandAccepted must be sent first for node-admin LeaveMesh");
        assert_eq!(
            admin_accepted,
            NodeCommandReply::CommandAccepted {
                node_id: admin_id.clone(),
                incarnation: admin_inc.clone(),
            }
        );
        assert_eq!(
            admin_leave_reply,
            NodeCommandReply::MeshLeft {
                node_id: admin_id.clone(),
                incarnation: admin_inc.clone(),
                left: vec![WhichMesh::Own, WhichMesh::Backbone],
            },
            "node-admin LeaveMesh must leave both [Own, Backbone]"
        );
        let stop_leaving_frame = NodeCommandReply::Leaving {
            node_id: admin_id,
            incarnation: admin_inc,
        };
        let stop_mesh_left_frame = NodeCommandReply::StopMeshLeft {
            left: vec![WhichMesh::Own, WhichMesh::Backbone],
        };
        assert_eq!(stop_leaving_frame.name(), "leaving");
        assert_eq!(stop_mesh_left_frame.name(), "stop-mesh-left");

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

        admin_ep.close().await;
        node_ep.close().await;
        peer_ep.close().await;
        let _ = std::fs::remove_dir_all(&root_dir);
    });

    drop(rt);

    let busy_spans = capture.named("rdm.node.mesh.reject.via-busy-holding-connections");
    assert_eq!(
        busy_spans.len(),
        1,
        "expected one busy-holding-connections rejection span"
    );
    assert!(
        busy_spans[0]
            .attributes
            .get("connections")
            .is_some_and(|c| !c.is_empty()),
        "via-busy-holding-connections span must carry connections attribute"
    );
    let leaving_spans = capture.named("rdm.node.mesh.update.via-leaving");
    assert_eq!(
        leaving_spans.len(),
        2,
        "expected via-leaving spans for rpc.1 standalone leave and admin.1 stop leave"
    );
    for s in &leaving_spans {
        assert_eq!(s.attributes.get("mesh").map(String::as_str), Some("mesh1"));
        assert!(s.attributes.get("mesh_id").is_some_and(|v| !v.is_empty()));
        assert!(s.attributes.get("operation").is_some_and(|v| !v.is_empty()));
    }
    let left_spans = capture.named("rdm.node.mesh.update.via-left");
    assert_eq!(
        left_spans.len(),
        3,
        "expected 3 via-left spans (own for rpc.1, own + backbone for admin.1)"
    );
    for s in &left_spans {
        assert_eq!(s.attributes.get("mesh").map(String::as_str), Some("mesh1"));
        assert!(s.attributes.get("mesh_id").is_some_and(|v| !v.is_empty()));
    }
    assert_eq!(
        left_spans
            .iter()
            .filter(|s| s.attributes.get("which").map(String::as_str) == Some("own"))
            .count(),
        2
    );
    assert_eq!(
        left_spans
            .iter()
            .filter(|s| s.attributes.get("which").map(String::as_str) == Some("backbone"))
            .count(),
        1
    );
}
