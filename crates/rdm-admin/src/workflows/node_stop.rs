//! Node stop workflow (`docs/architecture/lifecycles/node-stop.md`).
//!
//! Stops a node: executes `node.connection.delete { scope: All }`, cuts transport,
//! and transitions the live process to stopped.

pub use crate::node_self::{CutReport, NodeSelf, StartFailed, TasksStopped};
pub use crate::stop_gate::{GateState, StopGate, Turn};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EndpointId, IncarnationId, NodeId, PathName};
    use crate::node_self::{refuse_node_left, refuse_reserved, ChannelFrames, Session};
    use rdm_core::connections::{
        ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection,
    };
    use rdm_core::node_command::{NodeCommandReply, NodeCommandRequest};
    use rdm_core::status::{
        ConnectionsReceipt, DeclaredOperation, NodeState, StatusReply, StatusRequest, WhichMesh,
    };
    use rdm_core::{MemberStatus, MeshDigest, MeshNode};
    use rdm_node::connections_writer::{ConnectionsWriter, RowsHeld};
    use rdm_node::storage::MemoryConnectionsStorage;
    use rdm_transport::topology::TopologyView;
    use rdm_transport::{CancelToken, LiveNodeResolver, NodeRpcClient, ServerBuilder};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct GateSession {
        gate: StopGate,
        cuts: Arc<Mutex<usize>>,
    }

    #[async_trait::async_trait]
    impl Session for GateSession {
        async fn hold_tasks(&self, commanded_by: Option<&str>) -> TasksStopped {
            let at = match commanded_by {
                Some(b) => self.gate.quiesce_commanded_by(b).await,
                None => self.gate.quiesce().await,
            };
            TasksStopped {
                turns_in_progress: at.in_progress(),
                waited_for: at.named(),
            }
        }

        async fn cut(&self, _spare: &[iroh::PublicKey], _mesh_primary: Option<iroh::PublicKey>) -> CutReport {
            *self.cuts.lock().unwrap() += 1;
            CutReport { neighbours: 2, pooled: 1, accepted: 1, turns_in_progress: 0 }
        }

        async fn leave_channels(&self) {}

        async fn rejoin(
            &self,
            _birth_cancel: CancelToken,
            _commander: tokio::sync::watch::Receiver<iroh::PublicKey>,
            _rows: Option<RowsHeld>,
            _operation: DeclaredOperation,
            _progress: Option<crate::node_self::CommandProgress>,
            _held_again: Option<tokio::sync::oneshot::Sender<()>>,
        ) -> Result<(), StartFailed> {
            Ok(())
        }
    }

    // Feature: node-stop
    /// CONTRACT (docs/architecture/lifecycles/node-stop.md:17-59):
    /// 1. `StopNode` requires an exact incarnation (`node-stop.md:17,33`): a stale incarnation is
    ///    refused `RejectedStaleIncarnation` before `CommandAccepted`, writing no state.
    /// 2. `StopNode` on the exact birth emits `CommandAccepted`, runs `node.connection.delete { All }`
    ///    (`ConnectionsDeleting` -> `StopConnectionsDeleted { receipt: Established }`), holds gated tasks
    ///    (`rdm.node_admin.node.update.via-stop-quiesce`), runs `node.mesh.leave` (`Leaving` ->
    ///    `StopMeshLeft { left: [Own] }`), owes `Stopped` naming the stop operation
    ///    (`rdm.node_rpc.status.update.via-owed-on-stop`), and returns `NodeStopped` (`node-stop.md:31-54`).
    /// 3. A repeat `StopNode` on the stopped birth answers all frames at once with the held receipt
    ///    and repeats no transport cut (`node-stop.md:47-50`).
    /// 4. Reserved `StatusRequest::StopNode` and `StatusRequest::NodeLeft` on 0x1B are refused by name.
    /// Must NOT happen: a stale stop accepted, a repeat stop cutting transport a second time, or a
    /// stop skipping `ConnectionsDeleting` / `Leaving` frames.
    #[test]
    fn stop_node_streams_primitive_frames_quiesces_tasks_owes_stopped_and_replays_when_already_stopped() {
        let capture = crate::span_capture::Capture::new("node-stop-unit");
        let _g = tracing::dispatcher::set_default(&capture.dispatch);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let node_key = iroh::SecretKey::generate();
            let endpoint = rdm_transport::endpoint::bind(
                node_key.clone(),
                "127.0.0.1:0".parse().unwrap(),
            )
            .await
            .expect("bind endpoint");
            let node_addr = endpoint
                .bound_sockets()
                .into_iter()
                .find(|a| a.is_ipv4())
                .unwrap();
            let ep_id = EndpointId(node_key.public().to_string());
            let node_id = NodeId::mint();
            let incarnation = IncarnationId::mint();
            let stale_inc = IncarnationId::mint();
            let commander_id = NodeId::mint();
            let name: PathName = "mesh1.rpc.1".parse().unwrap();
            let fabric_id = rdm_core::FabricId::mint();
            let mesh_id = rdm_core::MeshId::mint();

            let clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
            clock.adopt(1_000_000);
            let g = iroh_gossip::net::Gossip::builder().spawn(endpoint.clone());
            let topology_view = TopologyView::join(
                &g,
                &endpoint,
                &fabric_id,
                "mesh1",
                &mesh_id,
                &name.to_string(),
                clock,
                Vec::new(),
                &[],
            )
            .await
            .expect("topology view joins");

            let resolver = Arc::new(LiveNodeResolver::default());
            let client = Arc::new(
                NodeRpcClient::new(endpoint.clone(), resolver.clone()).with_caller_system("rdm"),
            );
            let server = ServerBuilder::new()
                .serve_core(client.clone(), None)
                .build(rdm_transport::ServedBirth {
                    node_id: node_id.to_string(),
                    incarnation: incarnation.to_string(),
                })
                .unwrap();

            let conn_store = Arc::new(MemoryConnectionsStorage::default());
            let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
            let own_end = ConnectionEnd {
                name: name.clone(),
                node_id: node_id.clone(),
                incarnation: Some(incarnation.clone()),
            };
            let writer = Arc::new(ConnectionsWriter::new(own_end, conn_store, held));

            let status_cell = Arc::new(Mutex::new(MemberStatus::Ready));
            let digest_base = MeshDigest {
                held_hash: [0u8; 32],
                fabric_id,
                node: MeshNode {
                    node_id: node_id.clone(),
                    name: name.clone(),
                    endpoint_id: ep_id,
                    transport_addr: node_addr,
                    incarnation: incarnation.clone(),
                    supersedes: None,
                    runtime: None,
                },
                status: MemberStatus::Ready,
                node_state: Default::default(),
                admin_api_base: None,
                digest_seq: 1,
                emitted_at_rafka_ms: 1_000,
                data_dir: None,
                mesh_id: Some(mesh_id),
                in_flight: None,
                load: None,
                gossip: None,
                is_primary: None,
                is_fabric_primary: None,
            };
            let set_status = {
                let status_cell = status_cell.clone();
                let digest_base = digest_base.clone();
                Arc::new(move |st: MemberStatus| {
                    *status_cell.lock().unwrap() = st;
                    let mut d = digest_base.clone();
                    d.status = st;
                    d
                })
            };
            let get_status = {
                let status_cell = status_cell.clone();
                Arc::new(move || *status_cell.lock().unwrap())
            };

            let owed_calls: Arc<Mutex<Vec<(NodeState, Option<DeclaredOperation>)>>> =
                Arc::new(Mutex::new(Vec::new()));
            let declare: crate::node_self::Declare = {
                let owed_calls = owed_calls.clone();
                Arc::new(move |state, op| {
                    owed_calls.lock().unwrap().push((state, op));
                    Box::pin(async {})
                })
            };

            let own = Arc::new(NodeSelf::new(
                node_id.clone(),
                incarnation.clone(),
                name.clone(),
                server,
                client,
                resolver,
                topology_view.clone(),
                set_status,
                get_status,
                Some(declare),
                Duration::from_secs(2),
            ));
            own.set_connections(writer);
            let gate = StopGate::new();
            let cuts = Arc::new(Mutex::new(0usize));
            own.set_session(Arc::new(GateSession {
                gate: gate.clone(),
                cuts: cuts.clone(),
            }));

            // 1. Stale incarnation is refused before CommandAccepted.
            let (stale_tx, mut stale_rx) = tokio::sync::mpsc::unbounded_channel();
            let stale_reply = own
                .serve_node_command(
                    Some(&commander_id),
                    &NodeCommandRequest::StopNode {
                        node_id: node_id.clone(),
                        incarnation: stale_inc,
                        build_id: "b-stop-1".into(),
                        attempt: 1,
                        operation: "node.stop:mesh1.rpc.1".into(),
                    },
                    Box::new(ChannelFrames(stale_tx)),
                )
                .await;
            assert!(
                matches!(stale_reply, NodeCommandReply::RejectedStaleIncarnation { ref held } if *held == incarnation),
                "stale incarnation must be refused before acceptance: {stale_reply:?}"
            );
            assert!(stale_rx.try_recv().is_err(), "no frame is streamed on a stale incarnation refusal");
            assert!(!own.is_stopped() && !own.stop_admitted());

            // 2. Exact StopNode streams all primitive acknowledgements and completions, quiesces the gate,
            // closes the open connection, owes Stopped naming the operation, and ends NodeStopped.
            let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
            let stop_req = NodeCommandRequest::StopNode {
                node_id: node_id.clone(),
                incarnation: incarnation.clone(),
                build_id: "b-stop-1".into(),
                attempt: 2,
                operation: "node.stop:mesh1.rpc.1".into(),
            };
            let terminal1 = own
                .serve_node_command(Some(&commander_id), &stop_req, Box::new(ChannelFrames(tx1)))
                .await;
            assert_eq!(terminal1, NodeCommandReply::NodeStopped);
            assert!(own.is_stopped() && own.stop_admitted());
            assert!(gate.is_stopped(), "stop quiesced the StopGate");
            assert_eq!(*cuts.lock().unwrap(), 1, "transport cut ran once");

            let mut frames1 = Vec::new();
            while let Ok(f) = rx1.try_recv() {
                frames1.push(f);
            }
            assert_eq!(
                frames1,
                vec![
                    NodeCommandReply::CommandAccepted {
                        node_id: node_id.clone(),
                        incarnation: incarnation.clone(),
                    },
                    NodeCommandReply::ConnectionsDeleting {
                        node_id: node_id.clone(),
                        incarnation: incarnation.clone(),
                    },
                    NodeCommandReply::StopConnectionsDeleted {
                        receipt: ConnectionsReceipt::Established {
                            in_flight: 0,
                            closed: Vec::new(),
                        },
                    },
                    NodeCommandReply::Leaving {
                        node_id: node_id.clone(),
                        incarnation: incarnation.clone(),
                    },
                    NodeCommandReply::StopMeshLeft {
                        left: vec![WhichMesh::Own],
                    },
                ],
                "StopNode must stream CommandAccepted -> ConnectionsDeleting -> StopConnectionsDeleted -> Leaving -> StopMeshLeft in order"
            );

            let owed = owed_calls.lock().unwrap().clone();
            assert_eq!(
                owed,
                vec![(
                    NodeState::Stopped,
                    Some(DeclaredOperation {
                        build_id: "b-stop-1".into(),
                        attempt: 2,
                        operation: "node.stop:mesh1.rpc.1".into(),
                    })
                )],
                "StopNode owes Stopped naming its operation to the mesh-primary"
            );

            // 3. Repeat StopNode on the already-stopped birth replays the frames with the held receipt and repeats no cut.
            let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
            let terminal2 = own
                .serve_node_command(Some(&commander_id), &stop_req, Box::new(ChannelFrames(tx2)))
                .await;
            assert_eq!(terminal2, NodeCommandReply::NodeStopped);
            assert_eq!(*cuts.lock().unwrap(), 1, "repeat StopNode must not repeat the transport cut");
            let mut frames2 = Vec::new();
            while let Ok(f) = rx2.try_recv() {
                frames2.push(f);
            }
            assert_eq!(frames2, frames1, "repeat StopNode replays the primitive frames and held receipt");

            // 4. Reserved StatusRequest::StopNode and StatusRequest::NodeLeft on 0x1B are refused by name.
            let reserved_stop = refuse_reserved(
                "mesh1.rpc.1",
                "mesh1.admin.1",
                &StatusRequest::StopNode {
                    node_id: node_id.clone(),
                    incarnation: incarnation.clone(),
                    build_id: "b-stop-1".into(),
                    attempt: 2,
                    operation: "node.stop:mesh1.rpc.1".into(),
                },
            );
            assert!(matches!(
                reserved_stop,
                StatusReply::RejectedUnmatchedCompletion { ref field, .. } if field == "command"
            ));
            let reserved_left = refuse_node_left(
                "mesh1.admin.1",
                "mesh1.rpc.1",
                "mesh1.rpc.1",
                &StatusRequest::NodeLeft {
                    node_id,
                    incarnation,
                    build_id: "b-stop-1".into(),
                    attempt: 2,
                    operation: "node.stop:mesh1.rpc.1".into(),
                },
            );
            assert!(matches!(
                reserved_left,
                StatusReply::RejectedUnmatchedCompletion { ref field, .. } if field == "command"
            ));

            topology_view.stop().await;
            let _ = g.shutdown().await;
            endpoint.close().await;
        });
        drop(rt);

        let stop_spans = capture.named("rdm.node_admin.status.update.via-stop-node");
        assert_eq!(stop_spans.len(), 1);
        assert_eq!(
            stop_spans[0].attributes.get("receipt").map(String::as_str),
            Some("established"),
            "via-stop-node records the ConnectionsReceipt name: {:?}",
            stop_spans[0].attributes
        );
        let quiesce_spans = capture.named("rdm.node_admin.node.update.via-stop-quiesce");
        assert_eq!(quiesce_spans.len(), 1);
        let cut_spans = capture.named("rdm.node_admin.node.update.via-stop-transport-cut");
        assert_eq!(cut_spans.len(), 1);
        assert_eq!(
            cut_spans[0].attributes.get("neighbours").map(String::as_str),
            Some("2")
        );
        let owed_spans = capture.named("rdm.node_rpc.status.update.via-owed-on-stop");
        assert_eq!(owed_spans.len(), 1);
        assert_eq!(
            owed_spans[0].attributes.get("operation").map(String::as_str),
            Some("node.stop:mesh1.rpc.1")
        );
        assert_eq!(
            capture.named("rdm.node_admin.status.reject.via-reserved-position").len(),
            1
        );
        assert_eq!(
            capture.named("rdm.node_admin.status.reject.via-node-left").len(),
            1
        );
    }
}
