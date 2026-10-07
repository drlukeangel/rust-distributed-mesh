//! e11.s2 — a role process on the imported substrate: one endpoint carrying gossip and Node RPC,
//! one sealed catalog (core + the role's families + the product's transitional adapters), one
//! server on the Node RPC ALPN. The cells call the role the way a peer does, over the wire.

use iroh::SecretKey;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId};
use rafka_node_base::{compose, Role, LEGACY_ADAPTERS};
use rafka_node_rpc::{CallOptions, Decode, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rafka_node_rpc_contract::catalog::{EntryKind, TagOwner};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_testkit::node::{self, RunningNode};
use std::sync::Arc;
use std::time::Duration;

/// A role process as node-admin would launch it, in this process: its provider's runtime record
/// written first, its one endpoint bound on a loopback port.
struct RoleProcess {
    running: RunningNode,
    launch: Launch,
    key: SecretKey,
    _dir: tempdir::Dir,
}

async fn born(role: Role, name: &str) -> RoleProcess {
    let dir = tempdir::Dir::new();
    RuntimeFact::of_this_process("cell").unwrap().write_record(dir.path()).unwrap();
    let key = node::load_or_mint_key(dir.path()).unwrap();
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id: FabricId::mint(),
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        transport_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![],
        data_dir: dir.path().to_path_buf(),
        mesh_id: Some(MeshId::mint()),
    };
    let running = node::start_with_client(&launch, |b, _resolver, client| compose(role, b, client)).await.unwrap();
    RoleProcess { running, launch, key, _dir: dir }
}

impl RoleProcess {
    /// A peer's client, resolving this role by its exact node id.
    async fn peer(&self) -> (NodeRpcClient, NodeTarget) {
        let addr = self.running.routers[0].endpoint().bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode {
            node_id: self.launch.node_id.clone(),
            name: self.launch.name.clone(),
            endpoint_id: self.key.public(),
            transport_addr: addr,
            incarnation: self.launch.incarnation.clone(),
        });
        let ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        (NodeRpcClient::new(ep, resolver), NodeTarget::ExactNode(self.launch.node_id.clone()))
    }
}

#[tokio::test]
async fn a_role_process_serves_core_ping_on_its_one_endpoint() {
    let broker = born(Role::broker(), "mesh1.broker.1").await;
    assert_eq!(broker.running.routers.len(), 1, "one Iroh endpoint carries gossip and Node RPC");
    let (client, target) = broker.peer().await;
    let (out, _) = client.call::<Ping>(&target, &PingRequest::Ping { payload: b"role".to_vec() }, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|r| r.value().clone()), Some(PingReply::Pong { payload: b"role".to_vec() }), "{out:?}");
    broker.running.stop(Duration::ZERO).await;
}

#[tokio::test]
async fn a_tag_the_catalog_does_not_hold_is_unserved_421() {
    let gateway = born(Role::gateway(), "mesh1.gateway.1").await;
    let absent = 0x6E;
    assert!(gateway.running.server.catalog().lookup(absent).is_none());
    let (client, target) = gateway.peer().await;
    let (out, _) = client
        .invoke_raw::<PingReply, _>(&target, absent, vec![1], 1024, &CallOptions::default(), |d| match d {
            Decode::Committed(c, b) => c.reply::<Ping>(b),
            Decode::Early(e, b) => e.reply::<Ping>(b),
        })
        .await;
    assert!(matches!(&out, RpcOutcome::Unserved(u) if u.tag() == absent), "{out:?}");
    assert_eq!(rafka_node_rpc::ServerStats::get(&gateway.running.server.stats().dispatched), 0, "an unserved tag is never dispatched");
    gateway.running.stop(Duration::ZERO).await;
}

#[tokio::test]
async fn the_products_adapter_is_catalogued_and_unserved_on_the_node_rpc_alpn() {
    let compute = born(Role::compute(), "mesh1.compute.1").await;
    let adapter = &LEGACY_ADAPTERS[0];
    let held = compute.running.server.catalog().lookup(adapter.tag).expect("the adapter is catalogued");
    assert_eq!(held.owner, TagOwner::Product(adapter.owner.into()), "sealed under the ledger's owner");
    assert!(matches!(&held.kind, EntryKind::Transitional { migration_unit } if migration_unit == adapter.migration_unit));
    let (client, target) = compute.peer().await;
    let (out, _) = client
        .invoke_raw::<PingReply, _>(&target, adapter.tag, vec![1, 2, 3], 1024, &CallOptions::default(), |d| match d {
            Decode::Committed(c, b) => c.reply::<Ping>(b),
            Decode::Early(e, b) => e.reply::<Ping>(b),
        })
        .await;
    assert!(matches!(&out, RpcOutcome::Unserved(u) if u.tag() == adapter.tag), "{out:?}");
    compute.running.stop(Duration::ZERO).await;
}

mod tempdir {
    use std::path::{Path, PathBuf};
    pub struct Dir(PathBuf);
    impl Dir {
        pub fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let p = std::env::temp_dir().join(format!("role-process-{}-{}", std::process::id(), N.fetch_add(1, Ordering::Relaxed)));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
        pub fn path(&self) -> &Path {
            &self.0
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
