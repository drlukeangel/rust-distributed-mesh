//! End-to-end cell for `connections-reconnect` (`docs/architecture/connections.md` §9, §10; R-CN1; Q348).

use crate::common::admin_side_serving;
use rdm_core::connections::{
    ConnectionEnd, ConnectionKind, ConnectionState, NodeConnection,
};
use rdm_core::launch::Launch;
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingRequest};
use rdm_core::runtime::RuntimeFact;
use rdm_core::{ConnectionId, FabricId, IncarnationId, model::NodeStatus, MeshId, NodeId, PathName};
use rdm_node::connection_reconnect::{adapt_recovery_dial, run_pending, RecoveryDial};
use rdm_test::node;
use rdm_test::originate::Seams;
use rdm_transport::{CallOptions, NodeResolver, NodeTarget};
use std::sync::{Arc, OnceLock};

fn kept_log(store: &std::path::Path, fabric: &FabricId, node_id: &NodeId) -> Vec<NodeConnection> {
    let root = rdm_admin::store_root::StoreRoot::open(store, fabric).expect("the store root opens");
    rdm_node::storage::connection_history(root.dir(), node_id)
        .unwrap_or_else(|e| panic!("{node_id}'s kept log: {e}"))
}

// @feature: connections-reconnect
/// CONTRACT (connections.md §9, §10, R-CN1; Q348): a source holding a `Direct Failed` row and an active
/// Proxy to a peer has nothing scheduled from the row; an accept from the peer's exact birth wakes the
/// source's one dial, made on `Send` over the real Node RPC client: its Direct Connected and the owed
/// `Proxy Disconnected(direct-restored)` land in the store root, and the woken dial ends.
#[test]
fn source_node_reconciler_dials_the_destination_an_accept_woke_and_deletes_active_proxy() {
    let cap = crate::common::capture(
        "source_node_reconciler_redials_failed_destination_at_due_time_and_deletes_active_proxy",
    );
    let spans = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side_serving(
            "127.0.0.1".parse().unwrap(),
            &fabric,
            NodeStatus::Ready,
            crate::common::serve_ping,
        )
        .await;
        let root = std::env::temp_dir().join(format!("connections-reconnect-{}", NodeId::mint()));
        let store = root.join("store");
        std::fs::create_dir_all(&store).unwrap();
        let src_name: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let peer_name: PathName = rdm_core::PathName::new("mesh1.node", "2").unwrap();

        let launch_of = |name: &PathName| {
            let node_id = NodeId::mint();
            let dir = root.join(node_id.as_str());
            std::fs::create_dir_all(&dir).unwrap();
            RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell"))
                .unwrap()
                .write_record(&dir)
                .unwrap();
            let key = node::load_or_mint_key(&dir).unwrap();
            let launch = Launch {
                initial_properties: Default::default(),
                topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
                fabric: "fabric1".into(),
                fabric_id: fabric.clone(),
                mesh: "mesh1".into(),
                r#type: "node".into(),
                path: name.path.clone(), name: name.name.clone(),
                node_id,
                incarnation: IncarnationId::mint(),
                supersedes: None,
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                listeners: Vec::new(),
                seeds: vec![admin.seed.clone()],
                launcher: Some(admin.launcher.clone()),
                data_dir: dir,
                store_dir: store.clone(),
                mesh_id: Some(MeshId::parse(crate::common::TEST_MESH_ID).unwrap()),
                issuing: None,
                roots: Vec::new(),
                connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
                declared: Default::default(),
            };
            admin.deployed(&launch, &key);
            launch
        };
        let (src_launch, peer_launch) = (launch_of(&src_name), launch_of(&peer_name));
        let start = |launch: Launch| async move {
            let seams: Arc<OnceLock<Seams>> = Arc::default();
            let slot = seams.clone();
            let running = node::start_with_seams(&launch, move |b, s| {
                let _ = slot.set(s);
                b
            })
            .await
            .unwrap_or_else(|e| panic!("{:?} comes up: {e}", launch.name));
            (running, seams.get().cloned().expect("the node handed its seams"))
        };
        let (src_node, src_seams) = start(src_launch.clone()).await;
        let (peer_node, _peer_seams) = start(peer_launch.clone()).await;

        let digest_view = &admin.observer.topology_view.digest_view;
        let mut heard = digest_view.heard_changes();
        while [&src_launch, &peer_launch].iter().any(|l| {
            digest_view
                .get(l.node_id.as_str())
                .is_none_or(|(d, _)| d.status != NodeStatus::Ready)
        }) {
            heard.changed().await.unwrap();
        }
        let src_view = &src_node.topology_view.digest_view;
        let mut src_digests = src_view.digest_changes();
        while src_view.get(peer_launch.node_id.as_str()).is_none() {
            src_digests.changed().await.unwrap();
        }
        // The resolver is fed from the view on its own task once the birth is held (Q387: the peer's
        // own Digest is how the source first learns it): it is waited for, bounded.
        let resolved_peer = tokio::time::timeout(std::time::Duration::from_secs(10), async {
            loop {
                if let Ok(r) = src_seams.resolver.resolve(&NodeTarget::ExactNode(peer_launch.node_id.clone())) {
                    break r;
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("source resolver holds peer");

        let src_end = ConnectionEnd { mesh: src_launch.mesh.clone(), r#type: src_launch.r#type,
            path: src_launch.path.clone(), name: src_launch.name.clone(),
            node_id: src_launch.node_id.clone(),
            incarnation: Some(src_launch.incarnation.clone()),
        };
        let carrier_end = ConnectionEnd { mesh: admin.launcher.mesh.clone(), r#type: "node-admin".into(),
            path: admin.launcher.path.clone(), name: admin.launcher.name.clone(),
            node_id: admin.launcher.node_id.clone(),
            incarnation: Some(admin.launcher.incarnation.clone()),
        };
        let peer_end = ConnectionEnd { mesh: resolved_peer.mesh.clone(), r#type: resolved_peer.r#type,
            path: resolved_peer.path.clone(), name: resolved_peer.name.clone(),
            node_id: resolved_peer.node_id.clone(),
            incarnation: Some(resolved_peer.incarnation.clone()),
        };

        let base_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
            + 60_000;
        let failed_row = NodeConnection {
            id: ConnectionId::mint(),
            source: src_end.clone(),
            destination: peer_end.clone(),
            kind: ConnectionKind::Direct,
            alpn: rdm_core::connections::ALPN.to_vec(),
            state: ConnectionState::Failed,
            carrier: None,
            reason: Some("NotSent".into()),
            logged_at_ms: base_ms,
        };
        src_seams
            .connections
            .record(failed_row.clone())
            .await
            .expect("record failed");
        src_seams
            .connections
            .record(NodeConnection {
                id: ConnectionId::mint(),
                source: src_end.clone(),
                destination: peer_end.clone(),
                kind: ConnectionKind::Proxy,
                alpn: rdm_core::connections::ALPN.to_vec(),
                state: ConnectionState::Connected,
                carrier: Some(carrier_end),
                reason: None,
                logged_at_ms: base_ms + 1,
            })
            .await
            .expect("record proxy");

        assert!(src_seams.connections.recovery_pending().is_empty(), "the Failed row schedules no dial (Q348)");
        let peer_birth = rdm_transport::connections::Birth { mesh: peer_end.mesh.clone(), r#type: peer_end.r#type.clone(), path: peer_end.path.clone(), name: peer_end.name.clone(), node_id: peer_end.node_id.clone(), incarnation: peer_end.incarnation.clone().expect("the peer's birth") };
        src_seams.connections.accepted_on_alpn(&peer_birth, rdm_core::connections::ALPN);
        assert_eq!(src_seams.connections.recovery_pending(), vec![peer_end.clone()], "the accept woke one dial");

        let client = src_seams.client.clone();
        let dial: RecoveryDial = Arc::new(move |dest: ConnectionEnd| {
            let client = client.clone();
            Box::pin(async move {
                let (out, _) = client
                    .call::<Ping>(
                        &NodeTarget::ExactNode(dest.node_id),
                        &PingRequest::Ping {
                            payload: b"reconnect".to_vec(),
                        },
                        &CallOptions::default(),
                    )
                    .await;
                match out {
                    RpcOutcome::Reply(_) => "Reply".to_string(),
                    other => other.name().to_string(),
                }
            })
        });
        let alpn_dial = adapt_recovery_dial(dial);

        // The woken dial reaches the peer over the real NodeRpcClient; Direct Connected and Proxy
        // Disconnected(direct-restored) land in the store root, and the dial has ended.
        run_pending(&src_seams.connections, &alpn_dial).await;
        src_seams.connections.observations_landed().await;

        assert!(src_seams.connections.recovery_pending().is_empty());
        let log = kept_log(&store, &fabric, &src_launch.node_id);
        assert!(
            log.iter().any(|r| {
                r.kind == ConnectionKind::Direct
                    && r.destination.node_id == peer_launch.node_id
                    && r.state == ConnectionState::Connected
            }),
            "expected Direct Connected row in durable log: {log:#?}"
        );
        assert!(
            log.iter().any(|r| {
                r.kind == ConnectionKind::Proxy
                    && r.destination.node_id == peer_launch.node_id
                    && r.state == ConnectionState::Disconnected
                    && r.reason.as_deref() == Some("direct-restored")
            }),
            "expected Proxy Disconnected(direct-restored) row in durable log: {log:#?}"
        );

        drop(peer_node);
        drop(src_node);
        let _ = std::fs::remove_dir_all(&root);
    });

    let recorded = spans.0.lock().unwrap();
    assert!(
        recorded.values().any(|(name, _, fields)| {
            name == "rdm.node_admin.connection.create.via-recovery-dial"
                && fields.get("outcome").map(String::as_str) == Some("Reply")
                && fields.get("fact").map(String::as_str) == Some("connected")
        }),
        "expected rdm.node_admin.connection.create.via-recovery-dial span"
    );
    assert!(
        recorded.values().any(|(name, _, fields)| {
            name == "rdm.node_admin.connection.remove.via-recovery-attempt"
                && fields.get("reason").map(String::as_str) == Some("direct-connected")
        }),
        "expected rdm.node_admin.connection.remove.via-recovery-attempt span"
    );
}
