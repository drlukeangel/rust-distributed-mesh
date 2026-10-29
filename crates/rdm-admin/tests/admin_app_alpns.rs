//! A node-admin's app protocols are registered `Connections` like every node's (i145 e2;
//! fabric-certs.md §On a connection; node-connections-admit.md): an inbound connection on an app
//! ALPN the embedding registered at bind passes `node.connection.create` before the app sees it,
//! and a registration naming RDM's own Node RPC or gossip ALPN is refused at bind.

use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::{IncarnationId, NodeId, NodeKind};
use rdm_transport::connections::{Alpn, Birth, ConnectionsRegistry, MemoryConnectionsSink};
use rdm_transport::{ResolvedNode, StaticResolver};
use std::sync::Arc;
use std::time::Duration;

const APP: &[u8] = b"test-admin-app/1";

fn day0_config(dir: &std::path::Path) -> AdminConfig {
    let dir = dir.display().to_string();
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.clone()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .expect("a readable environment")
}

fn app_alpn(name: &[u8]) -> Alpn {
    Alpn { name: name.to_vec(), proxy: false, carriers: vec![NodeKind::RpcNode] }
}

fn wiring_with(alpn: &[u8]) -> Wiring {
    Wiring { alpns: vec![app_alpn(alpn)], fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }
}

fn resolved_admin(running: &Running) -> ResolvedNode {
    let me = running.birth();
    ResolvedNode {
        node_id: me.node_id.clone(),
        name: me.name.clone(),
        endpoint_id: me.endpoint_id.0.parse().unwrap(),
        transport_addr: me.transport_addr,
        incarnation: me.incarnation.clone(),
    }
}

/// A node of the fabric dialing the admin's app protocol through its own registry.
async fn member_dialer(admin: &ResolvedNode) -> (ConnectionsRegistry, iroh::protocol::Router, Birth) {
    let key = iroh::SecretKey::generate();
    let ep = rdm_transport::endpoint::bind_with_alpns(key, "127.0.0.1:0".parse().unwrap(), &[APP.to_vec()]).await.unwrap();
    let birth = Birth { name: "mesh1.rpc.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(admin.clone());
    let sink = Arc::new(MemoryConnectionsSink::new(birth.clone()));
    let (registry, handlers) = ConnectionsRegistry::new(ep.clone(), resolver, sink, &[app_alpn(APP)], b"cert-v1".to_vec()).unwrap();
    let mut rb = iroh::protocol::Router::builder(ep);
    for (alpn, handler) in handlers {
        rb = rb.accept(alpn, handler);
    }
    (registry, rb.spawn(), birth)
}

/// CONTRACT (fabric-certs.md §On a connection: "On every inbound connection of an ALPN the embedding
/// app registered at `bind`, the acceptor reads the dialer's member cert ... and admits or drops the
/// connection"; node-connections-admit.md): an admin's app ALPN is such a registration. A dial on it
/// that presents nothing is refused `no-credential` (the 412 close carrying the reason) and never
/// reaches the app; a member's dial through its registry is admitted and reaches the app.
#[test]
fn an_admins_app_alpn_dial_passes_admit() {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    rt.block_on(async {
        let dir = std::env::temp_dir().join(format!("admin-app-alpn-{}", NodeId::mint()));
        let running = start_with(day0_config(&dir), wiring_with(APP)).await.expect("the Day-0 admin comes up");
        let (_, mut app) = running.connections(APP).expect("the app takes its protocol's Connections from the admin").split();
        let admin = resolved_admin(&running);
        let floor = rdm_core::cadence::staleness_floor();

        let raw = rdm_transport::endpoint::bind_with_alpns(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), &[APP.to_vec()]).await.unwrap();
        let conn = raw.connect(iroh::EndpointAddr::new(admin.endpoint_id).with_ip_addr(admin.transport_addr), APP).await.unwrap();
        let closed = tokio::time::timeout(floor * 2, conn.closed()).await;
        let close = match closed {
            Ok(iroh::endpoint::ConnectionError::ApplicationClosed(frame)) => Some((frame.error_code.into_inner(), String::from_utf8_lossy(&frame.reason).into_owned())),
            _ => None,
        };
        assert_eq!(close, Some((412, "no-credential".to_string())), "a dial that presents nothing on the admin's app ALPN is refused at admission");
        assert!(tokio::time::timeout(Duration::from_millis(200), app.accept()).await.is_err(), "a refused connection never reaches the app");

        let (registry, router, _) = member_dialer(&admin).await;
        let (opener, _) = registry.connections(APP).unwrap().split();
        let accepted = tokio::spawn(async move { app.accept().await });
        let opened = tokio::time::timeout(floor * 2, opener.open(&Birth { name: admin.name.clone(), node_id: admin.node_id.clone(), incarnation: admin.incarnation.clone() })).await;
        let Ok(Ok(conn)) = opened else { panic!("a member's dial is admitted: {opened:?}") };
        let reached = tokio::time::timeout(floor, accepted).await.expect("the admitted connection reaches the app").unwrap();
        assert_eq!(reached.map(|c| c.id().clone()), Some(conn.id().clone()), "the app accepts the member's admitted connection");
        let _ = router.shutdown().await;
        running.leave().await;
        let _ = std::fs::remove_dir_all(&dir);
    });
    drop(rt);
}

/// CONTRACT (fabric-certs.md §On a connection: "a registration naming `rdm_transport::ALPN` or
/// `iroh_gossip::ALPN` is refused by name at `bind`"; node-connections-admit.md): an admin started
/// with an app protocol that names RDM's own Node RPC or gossip ALPN is refused at start, by name.
#[test]
fn binding_rdm_own_alpn_through_wiring_is_refused() {
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(4).enable_all().build().unwrap();
    rt.block_on(async {
        for (own, named) in [(rdm_transport::ALPN, "Node RPC"), (iroh_gossip::ALPN, "gossip")] {
            let dir = std::env::temp_dir().join(format!("admin-own-alpn-{}", NodeId::mint()));
            let started = start_with(day0_config(&dir), wiring_with(own)).await;
            match started {
                Err(e) => assert!(e.to_string().contains(&format!("RDM's own {named} ALPN")), "refused by name: {e}"),
                Ok(running) => {
                    running.leave().await;
                    panic!("an admin bound RDM's own {named} ALPN as an app protocol");
                }
            }
            let _ = std::fs::remove_dir_all(&dir);
        }
    });
    drop(rt);
}
