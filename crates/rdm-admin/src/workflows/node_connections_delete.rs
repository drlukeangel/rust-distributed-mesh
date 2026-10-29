//! Node connections delete primitive (`docs/architecture/lifecycles/node-connections-delete.md`).
//!
//! Gracefully cuts connections of a node: all connections (`scope: All`) or a specific
//! list of connection IDs (`scope: Connections(ids)`).

pub use crate::connections_writer::{ConnectionsWriter, HeldConnections, RecoveryAttempt, RowsHeld};
pub use crate::node_self::{Deleted, CONNECTIONS_DELETE};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName};
    use crate::span_capture::Capture;
    use iroh::SecretKey;
    use rdm_core::connections::{ConnectionEnd, ConnectionsHeld};
    use rdm_core::node_command::{NodeCommandReply, NodeCommandRequest};
    use rdm_core::ping::{Ping, PingRequest};
    use rdm_core::status::{ConnectionScope, ConnectionsReceipt};
    use rdm_core::{ConnectionId, MemberStatus, MeshDigest, MeshNode};
    use rdm_node::node_self::{ChannelFrames, NodeSelf};
    use rdm_node::storage::MemoryConnectionsStorage;
    use rdm_transport::topology::TopologyView;
    use rdm_transport::{LiveNodeResolver, NodeRpcClient, NodeTarget, ResolvedNode, ServerBuilder};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    #[test]
    fn delete_connections_refuses_empty_scope_and_stale_birth_closes_named_ids_and_completes_all_with_established_receipt() {
        // docs/architecture/lifecycles/node-connections-delete.md:19-24 — "`All`: Graceful
        // node-wide closure: close every open connection of the node. Status connections-deleting
        // once the node reports it on the command's stream, then connections-deleted from its
        // `Established` completion... `Connections([id, …])`: Close exactly the named connections,
        // each by the minted id of its open entry as `node.connection.get` returns it. Sets no
        // node-wide status and does not make the node non-routable. An empty list is refused by
        // name. An id the node does not know, or finds already closed, is named in the completion,
        // never ignored."
        let capture = Capture::new("node-connections-delete-unit");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");

        rt.block_on(async {
            let fabric_id = FabricId::mint();
            let mesh_id = MeshId::mint();
            let node_name = PathName::new("mesh1", NodeKind::RpcNode, 1);
            let node_id = NodeId::mint();
            let incarnation = IncarnationId::mint();

            let peer_name = PathName::new("mesh1", NodeKind::RpcNode, 2);
            let peer_id = NodeId::mint();
            let peer_incarnation = IncarnationId::mint();

            let peer_key = SecretKey::generate();
            let peer_ep = rdm_transport::endpoint::bind(
                peer_key.clone(),
                "127.0.0.1:0".parse().expect("addr"),
            )
            .await
            .expect("bind peer ep");
            let peer_addr = peer_ep
                .bound_sockets()
                .into_iter()
                .find(|a| a.is_ipv4())
                .expect("ipv4");
            let peer_resolver = Arc::new(LiveNodeResolver::default());
            let peer_client = Arc::new(NodeRpcClient::new(peer_ep.clone(), peer_resolver));
            let peer_server = ServerBuilder::new()
                .serve_core(peer_client, None)
                .build(rdm_transport::ServedBirth {
                    node_id: peer_id.to_string(),
                    incarnation: peer_incarnation.to_string(),
                })
                .expect("build peer server");
            let peer_router = iroh::protocol::Router::builder(peer_ep.clone())
                .accept(rdm_transport::ALPN, peer_server)
                .spawn();

            let node_key = SecretKey::generate();
            let node_ep = rdm_transport::endpoint::bind(
                node_key.clone(),
                "127.0.0.1:0".parse().expect("addr"),
            )
            .await
            .expect("bind node ep");
            let node_addr = node_ep
                .bound_sockets()
                .into_iter()
                .find(|a| a.is_ipv4())
                .expect("ipv4");

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
                .expect("build node server");

            let clock = Arc::new(rdm_transport::clock::RafkaTime::unadopted());
            clock.adopt(1_000_000);
            let gossip = iroh_gossip::net::Gossip::builder().spawn(node_ep.clone());
            let topology_view = TopologyView::join(
                &gossip,
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
            .expect("join topology view");

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
                mesh_id: Some(mesh_id),
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

            // 1. Stale incarnation is refused before CommandAccepted.
            let stale_inc = IncarnationId::mint();
            let (tx_stale, mut rx_stale) = tokio::sync::mpsc::unbounded_channel();
            let stale_reply = own
                .serve_node_command(
                    None,
                    &NodeCommandRequest::DeleteConnections {
                        node_id: node_id.clone(),
                        incarnation: stale_inc,
                        scope: ConnectionScope::All,
                    },
                    Box::new(ChannelFrames(tx_stale)),
                )
                .await;
            assert!(matches!(
                stale_reply,
                NodeCommandReply::RejectedStaleIncarnation { ref held } if *held == incarnation
            ));
            assert!(rx_stale.try_recv().is_err());

            // 2. Empty Connections([]) list is refused by name before CommandAccepted.
            let (tx_empty, mut rx_empty) = tokio::sync::mpsc::unbounded_channel();
            let empty_reply = own
                .serve_node_command(
                    None,
                    &NodeCommandRequest::DeleteConnections {
                        node_id: node_id.clone(),
                        incarnation: incarnation.clone(),
                        scope: ConnectionScope::Connections(Vec::new()),
                    },
                    Box::new(ChannelFrames(tx_empty)),
                )
                .await;
            assert!(matches!(
                empty_reply,
                NodeCommandReply::RejectedConnectionScope { .. }
            ));
            assert!(rx_empty.try_recv().is_err());

            // 3. Open a live Direct connection, then close it by id alongside an unknown id.
            let (ping1, _) = data_plane
                .call::<Ping>(
                    &NodeTarget::ExactNode(peer_id.clone()),
                    &PingRequest::Ping {
                        payload: b"one".to_vec(),
                    },
                    &rdm_transport::CallOptions::default(),
                )
                .await;
            assert_eq!(ping1.name(), "Reply");
            writer.observations_landed().await;
            let live_routes = own.live_routes().expect("live routes");
            assert_eq!(live_routes.len(), 1);
            let live_id = live_routes[0].id.clone();
            let unknown_id = ConnectionId::mint();

            let (tx_named, mut rx_named) = tokio::sync::mpsc::unbounded_channel();
            let named_reply = own
                .serve_node_command(
                    None,
                    &NodeCommandRequest::DeleteConnections {
                        node_id: node_id.clone(),
                        incarnation: incarnation.clone(),
                        scope: ConnectionScope::Connections(vec![live_id.clone(), unknown_id.clone()]),
                    },
                    Box::new(ChannelFrames(tx_named)),
                )
                .await;
            let mut named_frames = Vec::new();
            while let Ok(f) = rx_named.try_recv() {
                named_frames.push(f);
            }
            assert_eq!(
                named_frames,
                vec![NodeCommandReply::CommandAccepted {
                    node_id: node_id.clone(),
                    incarnation: incarnation.clone(),
                }]
            );
            assert_eq!(
                named_reply,
                NodeCommandReply::ConnectionsDeleted {
                    closed: vec![live_id],
                    not_open: vec![unknown_id],
                    held: Vec::new(),
                    receipt: None,
                }
            );
            writer.observations_landed().await;

            // 4. Open another live Direct connection, then run DeleteConnections { All }.
            let (ping2, _) = data_plane
                .call::<Ping>(
                    &NodeTarget::ExactNode(peer_id.clone()),
                    &PingRequest::Ping {
                        payload: b"two".to_vec(),
                    },
                    &rdm_transport::CallOptions::default(),
                )
                .await;
            assert_eq!(ping2.name(), "Reply");
            writer.observations_landed().await;
            let live_routes_2 = own.live_routes().expect("live routes 2");
            assert_eq!(live_routes_2.len(), 1);
            let live_id_2 = live_routes_2[0].id.clone();

            let (tx_all, mut rx_all) = tokio::sync::mpsc::unbounded_channel();
            let all_reply = own
                .serve_node_command(
                    None,
                    &NodeCommandRequest::DeleteConnections {
                        node_id: node_id.clone(),
                        incarnation: incarnation.clone(),
                        scope: ConnectionScope::All,
                    },
                    Box::new(ChannelFrames(tx_all)),
                )
                .await;
            let mut all_frames = Vec::new();
            while let Ok(f) = rx_all.try_recv() {
                all_frames.push(f);
            }
            assert_eq!(
                all_frames,
                vec![
                    NodeCommandReply::CommandAccepted {
                        node_id: node_id.clone(),
                        incarnation: incarnation.clone(),
                    },
                    NodeCommandReply::ConnectionsDeleting {
                        node_id: node_id.clone(),
                        incarnation: incarnation.clone(),
                    },
                ]
            );
            assert_eq!(
                all_reply,
                NodeCommandReply::ConnectionsDeleted {
                    closed: vec![live_id_2.clone()],
                    not_open: Vec::new(),
                    held: Vec::new(),
                    receipt: Some(ConnectionsReceipt::Established {
                        in_flight: 0,
                        closed: vec![live_id_2],
                    }),
                }
            );
            writer.observations_landed().await;

            let _ = peer_router.shutdown().await;
            peer_ep.close().await;
            node_ep.close().await;
        });

        drop(rt);

        assert!(
            !capture.named("rdm.node_admin.connection.delete.via-delete-connections").is_empty(),
            "expected rdm.node_admin.connection.delete.via-delete-connections"
        );
        assert!(
            !capture.named("rdm.node_admin.connection.delete.via-connections-deleting").is_empty(),
            "expected rdm.node_admin.connection.delete.via-connections-deleting"
        );
        assert!(
            !capture.named("rdm.node_admin.connection.delete.via-connections-delete").is_empty(),
            "expected rdm.node_admin.connection.delete.via-connections-delete"
        );
    }
}

