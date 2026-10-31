//! Node topology get primitive (`docs/architecture/lifecycles/node-topology-get.md`).
//!
//! An admitted member requests topology from its mesh-primary on op `0x1E` (`GetTopology`),
//! installing complete snapshots and refreshing rafka-time.

pub use crate::topology_read::{
    get_topology, ReadMesh, ServedTime, StoredMap, StoredMesh, TimeNotAdopted, TopologyDoor,
    TopologyFailure, TopologyRead, TopologySlot,
};
pub use crate::admin::{project, project_at, project_with, view_projector, ViewReads};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{EndpointId, FabricId, IncarnationId, MeshId, NodeId, NodeKind, PathName};
    use crate::span_capture::Capture;
    use crate::topology_read::snapshot_replies;
    use iroh::SecretKey;
    use iroh::protocol::Router;
    use rdm_core::topology::{SourceVersion, StoredNode, TopologyReply};
    use rdm_core::views::{NodeStatus, NodeView, ProviderKind};
    use rdm_core::{MemberStatus, MeshDigest, MeshNode, PublisherId};
    use rdm_transport::snapshot::{Chunk, SourceSnapshot, Taken};
    use rdm_transport::topology::TopologyView;
    use rdm_transport::{NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
    use std::collections::{BTreeMap, BTreeSet};
    use std::net::SocketAddr;
    use std::sync::{Arc, OnceLock};

    #[test]
    fn get_topology_installs_snapshot_carries_own_row_and_stored_map_adopts_rafka_time_and_answers_unchanged_for_held_since() {
        // docs/architecture/lifecycles/node-topology-get.md:7-10 — "An already-held member
        // requests node.topology.get from its OWN mesh-primary. It supplies its held source
        // version under that schema, installs only complete snapshots, then resumes applicable
        // deltas. The answer also carries the node's own whole row from its mesh-primary's
        // Atlas, never the store; the node holds it on its own entry (Q234)."
        let capture = Capture::new("node-topology-get-unit");
        let _guard = tracing::dispatcher::set_default(&capture.dispatch);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build current-thread runtime");

        rt.block_on(async {
            let fabric_id = FabricId::mint();
            let mesh_id = MeshId::mint();

            let server_ep = rdm_transport::endpoint::bind(
                SecretKey::generate(),
                "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
            )
            .await
            .expect("bind server ep");
            let server_gossip = iroh_gossip::net::Gossip::builder().spawn(server_ep.clone());
            let server_time = rdm_transport::clock::RafkaTime::unadopted();
            server_time.adopt(7_500_000);
            let server_view = TopologyView::join(
                &server_gossip,
                &server_ep,
                &fabric_id,
                "mesh1",
                &mesh_id,
                "mesh1.admin.1",
                Arc::new(server_time.clone()),
                Vec::new(),
                &[],
            )
            .await
            .expect("join server topology view");

            let caller_ep = rdm_transport::endpoint::bind(
                SecretKey::generate(),
                "127.0.0.1:0".parse::<SocketAddr>().expect("addr"),
            )
            .await
            .expect("bind caller ep");
            let caller_gossip = iroh_gossip::net::Gossip::builder().spawn(caller_ep.clone());
            let caller_time = rdm_transport::clock::RafkaTime::unadopted();
            let caller_view = TopologyView::join(
                &caller_gossip,
                &caller_ep,
                &fabric_id,
                "mesh1",
                &mesh_id,
                "mesh1.rpc.1",
                Arc::new(caller_time.clone()),
                Vec::new(),
                &[],
            )
            .await
            .expect("join caller topology view");

            let server_node_id = NodeId::mint();
            let server_inc = IncarnationId::mint();
            let caller_node_id = NodeId::mint();
            let caller_inc = IncarnationId::mint();
            let caller_path = PathName::new("mesh1", NodeKind::RpcNode, 1);

            let pub_id = PublisherId {
                node: "mesh1.admin.1".to_string(),
                incarnation: server_inc.clone(),
                node_id: server_node_id.clone(),
            };
            let own_digest = MeshDigest {
                held_hash: Default::default(),
                fabric_id: fabric_id.clone(),
                node: MeshNode {
                    node_id: server_node_id.clone(),
                    name: PathName::new("mesh1", NodeKind::NodeAdmin, 1),
                    endpoint_id: EndpointId(server_ep.id().to_string()),
                    transport_addr: server_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("ipv4"),
                    incarnation: server_inc.clone(),
                    supersedes: None,
                    runtime: None,
                },
                status: MemberStatus::Ready,
                node_state: Default::default(),
                admin_api_base: None,
                digest_seq: 1,
                emitted_at_rafka_ms: 7_500_000,
                data_dir: None,
                mesh_id: Some(mesh_id.clone()),
                in_flight: None,
                load: None,
                gossip: None,
                is_primary: Some(1),
                is_fabric_primary: Some(1),
            };

            let snap = SourceSnapshot {
                mesh: "mesh1".to_string(),
                publisher: pub_id.clone(),
                topology_version: 3,
                digests: vec![own_digest.clone()],
                in_flight: Vec::new(),
                deleted: Vec::new(),
            };
            for r in snapshot_replies(&snap, 1) {
                let TopologyReply::Snapshot {
                    mesh,
                    publisher,
                    topology_version,
                    snapshot_id,
                    chunk_index,
                    chunk_count,
                    digests,
                    in_flight,
                    deleted,
                } = r
                else {
                    unreachable!()
                };
                let taken = server_view.take_read_chunk(
                    Chunk {
                        mesh,
                        publisher,
                        forwarded_by: Some("topology-read".to_string()),
                        topology_version,
                        snapshot_id,
                        chunk_index,
                        chunk_count,
                        digests: digests.into_iter().map(MeshDigest::from).collect(),
                        in_flight,
                        deleted,
                    },
                    "peer",
                );
                assert!(!matches!(taken, Taken::Refused(_)), "{taken:?}");
            }

            let caller_ep_id = caller_ep.id().to_string();
            let caller_row_node_id = caller_node_id.clone();
            let caller_row_inc = caller_inc.clone();
            let caller_row_path = caller_path.clone();
            let own_row_fn = Arc::new(move |ep_key: String| {
                let expected = caller_ep_id.clone();
                let node_id = caller_row_node_id.clone();
                let inc = caller_row_inc.clone();
                let name = caller_row_path.clone();
                Box::pin(async move {
                    if ep_key != expected {
                        return None;
                    }
                    let view = NodeView {
                        name,
                        kind: NodeKind::RpcNode,
                        mesh: "mesh1".to_string(),
                        node_id,
                        endpoint_id: Some(expected),
                        incarnation_id: Some(inc),
                        deployment_id: None,
                        provider: Some(ProviderKind::Process),
                        data_dir: None,
                        status: NodeStatus::Ready,
                        is_primary: true,
                        is_fabric_primary: false,
                        admin_api_base: None,
                        transport_addr: None,
                        listeners: Vec::new(),
                        declared: None,
                        stopped: false,
                        load: None,
                        gossip: None,
                        tags: BTreeSet::from(["role-a".to_string()]),
                        config: BTreeMap::from([("tier".to_string(), "gold".to_string())]),
                        data_dir_lost: None,
                        data_dir_unreadable: None,
                        types: BTreeSet::new(),
                        held: BTreeSet::new(),
                        held_hash: [0u8; 32],
                        cert: None,
                        executable: None,
                        storage_persistence: None,
                        placement: None,
                        topology_storage_provider: None,
                    };
                    Some((view, 4u64))
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = Option<(NodeView, u64)>> + Send>>
            });

            let stored_mesh2_id = MeshId::mint();
            let stored_mesh2_id_clone = stored_mesh2_id.clone();
            let stored_fn = Arc::new(move || {
                let m2 = stored_mesh2_id_clone.clone();
                Box::pin(async move {
                    let mut map = StoredMap::default();
                    map.mesh_ids.insert("mesh2".to_string(), m2);
                    map.nodes.push(StoredNode {
                        node_id: NodeId::mint(),
                        name: "mesh2.admin.1".to_string(),
                        endpoint_id: EndpointId("ep-mesh2-admin-1".to_string()),
                        incarnation: IncarnationId::mint(),
                        transport_addr: "127.0.0.1:29001".parse().expect("addr"),
                    });
                    Ok(map)
                }) as std::pin::Pin<Box<dyn std::future::Future<Output = Result<StoredMap, String>> + Send>>
            });

            let slot: TopologySlot = Arc::new(OnceLock::new());
            let own_for_door = own_digest.clone();
            let door = TopologyDoor::new(
                server_view.clone(),
                Arc::new(move || own_for_door.clone()),
                server_time.clone(),
            )
            .with_own_row(own_row_fn)
            .with_stored(stored_fn);
            let _ = slot.set(Arc::new(door));

            let rpc_server = crate::topology_read::serve(ServerBuilder::new(), slot.clone())
                .build(ServedBirth {
                    node_id: server_node_id.to_string(),
                    incarnation: server_inc.to_string(),
                })
                .expect("build server");
            let server_router = Router::builder(server_ep.clone())
                .accept(iroh_gossip::ALPN, server_gossip.clone())
                .accept(rdm_transport::ALPN, rpc_server)
                .spawn();

            let resolver = Arc::new(StaticResolver::new());
            let server_addr = server_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).expect("ipv4");
            resolver.insert(ResolvedNode {
                node_id: server_node_id.clone(),
                name: PathName::new("mesh1", NodeKind::NodeAdmin, 1),
                endpoint_id: server_ep.id(),
                transport_addr: server_addr,
                incarnation: server_inc.clone(),
            });
            let client = NodeRpcClient::new(caller_ep.clone(), resolver);
            let target = NodeTarget::ExactNode(server_node_id.clone());

            // 1. Full read installs mesh1 snapshot, carries mesh2 stored map, installs own_row at revision 4, and serves rafka-time.
            let read = get_topology(&client, &target, "mesh1", &caller_view, None, None)
                .await
                .expect("get_topology succeeds");
            assert_eq!(read.installed.len(), 1);
            assert_eq!(read.installed[0].mesh, "mesh1");
            assert_eq!(read.installed[0].topology_version, 3);
            assert_eq!(read.stored.len(), 1);
            assert_eq!(read.stored[0].mesh, "mesh2");
            assert_eq!(read.stored[0].mesh_id, Some(stored_mesh2_id));
            let (own_row, rev) = read.own_row.as_ref().expect("own_row present");
            assert_eq!(*rev, 4);
            assert_eq!(own_row.config.get("tier").map(String::as_str), Some("gold"));
            assert!(read.rafka_time.is_some());
            read.adopt_rafka_time(
                &caller_time,
                &caller_view,
                "mesh1.rpc.1",
                "mesh1",
                &caller_node_id,
                "mesh1.admin.1",
            )
            .expect("member adopts rafka-time");
            assert!(caller_time.try_now_ms().is_some());

            // 2. Subsequent read of mesh1 with matching `since` answers Unchanged.
            let read_unchanged = get_topology(
                &client,
                &target,
                "mesh1",
                &caller_view,
                Some("mesh1"),
                Some(SourceVersion {
                    publisher: pub_id,
                    topology_version: 3,
                }),
            )
            .await
            .expect("get_topology unchanged succeeds");
            assert!(read_unchanged.installed.is_empty());
            assert_eq!(read_unchanged.unchanged, vec![("mesh1".to_string(), 3)]);

            let _ = server_router.shutdown().await;
            server_ep.close().await;
            caller_ep.close().await;
        });

        drop(rt);

        let serve_spans = capture.named("rdm.mesh.topology.serve.via-read");
        assert!(
            serve_spans.iter().any(|s| s.attributes.get("outcome").map(String::as_str) == Some("served") && s.attributes.get("snapshots").map(String::as_str) == Some("1")),
            "expected rdm.mesh.topology.serve.via-read with snapshots=1"
        );
        assert!(
            serve_spans.iter().any(|s| s.attributes.get("outcome").map(String::as_str) == Some("served") && s.attributes.get("unchanged").map(String::as_str) == Some("1")),
            "expected rdm.mesh.topology.serve.via-read with unchanged=1"
        );
        assert!(
            !capture.named("rdm.mesh.topology.update.via-read-install").is_empty(),
            "expected rdm.mesh.topology.update.via-read-install"
        );
    }
}

