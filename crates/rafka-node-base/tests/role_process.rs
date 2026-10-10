//! e11.s2 — a role process on the imported substrate: one endpoint carrying gossip and Node RPC,
//! one sealed catalog (core + the role's families + the product's transitional adapters), one
//! server on the Node RPC ALPN. The cells call the role the way a peer does, over the wire.

use iroh::SecretKey;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::runtime::RuntimeFact;
use rafka_mesh_entity::{FabricId, IncarnationId, MeshId, NodeId};
use rafka_node_base::{compose, Role, LEGACY_ADAPTERS};
use rafka_node_rpc::{Budget, CallOptions, Decode, NodeRpcClient, NodeTarget, ResolvedNode, ServerStats, StaticResolver};
use rafka_node_rpc_contract::catalog::{EntryKind, OpOwner};
use rafka_node_rpc_contract::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use rafka_node_rpc_contract::ping::{Ping, PingReply, PingRequest};
use rafka_node_rpc_contract::protocol::NodeProtocol;
use rafka_node_rpc_testkit::node::{self, RunningNode};
use rafka_node_rpc_testkit::rig::{admin_side, AdminSide, TEST_MESH_ID};
use std::sync::Arc;
use std::time::Duration;

/// A role process as node-admin would launch it, in this process: its provider's runtime record
/// written first, its one endpoint bound on a loopback port.
struct RoleProcess {
    running: RunningNode,
    _launcher: AdminSide,
    launch: Launch,
    key: SecretKey,
    _dir: tempdir::Dir,
}

async fn born(role: Role, name: &str) -> RoleProcess {
    let dir = tempdir::Dir::new();
    RuntimeFact::of_this_process("cell").unwrap().write_record(dir.path()).unwrap();
    let key = node::load_or_mint_key(dir.path()).unwrap();
    // The admin that deployed this birth: it takes the node's JoinNode and serves its topology.
    let fabric_id = FabricId::mint();
    let launcher = admin_side("127.0.0.1".parse().unwrap(), &fabric_id).await;
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id,
        name: name.parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        seeds: vec![launcher.seed.clone()],
        launcher: Some(launcher.launcher.clone()),
        data_dir: dir.path().to_path_buf(),
        mesh_id: Some(MeshId::parse(TEST_MESH_ID).unwrap()),
    };
    launcher.deployed(&launch, &key);
    let oracles = rafka_node_base::Oracles::open(&launch).unwrap();
    let running = node::start_with_client(&launch, |b, resolver, client| oracles.serve(compose(role, &launch.node_id.to_string(), b, client), &launch, resolver)).await.unwrap();
    let _ = oracles.declare_client.set(running.node_rpc.clone());
    RoleProcess { running, _launcher: launcher, launch, key, _dir: dir }
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
    assert!(matches!(&out, RpcOutcome::Unserved(u) if u.op() == absent), "{out:?}");
    assert_eq!(rafka_node_rpc::ServerStats::get(&gateway.running.server.stats().dispatched), 0, "an unserved op is never dispatched");
    gateway.running.stop(Duration::ZERO).await;
}

#[tokio::test]
async fn the_products_adapter_is_catalogued_and_unserved_on_the_node_rpc_alpn() {
    let compute = born(Role::compute(), "mesh1.compute.1").await;
    let adapter = &LEGACY_ADAPTERS[0];
    let held = compute.running.server.catalog().lookup(adapter.op).expect("the adapter is catalogued");
    assert_eq!(held.owner, OpOwner::Product(adapter.owner.into()), "sealed under the ledger's owner");
    assert!(matches!(&held.kind, EntryKind::Transitional { migration_unit } if migration_unit == adapter.migration_unit));
    let (client, target) = compute.peer().await;
    let (out, _) = client
        .invoke_raw::<PingReply, _>(&target, adapter.op, vec![1, 2, 3], 1024, &CallOptions::default(), |d| match d {
            Decode::Committed(c, b) => c.reply::<Ping>(b),
            Decode::Early(e, b) => e.reply::<Ping>(b),
        })
        .await;
    assert!(matches!(&out, RpcOutcome::Unserved(u) if u.op() == adapter.op), "{out:?}");
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

/// e11.s8 — the product's own unary family: `broker_data` (0x20, forwardable), ledgered under
/// the product and served only by the broker. A compute catalogues the family (one ledger per
/// process) and carries it for others, but serves no handler for it: a call there is unserved, 421.
#[tokio::test]
async fn the_product_family_round_trips_to_a_broker_and_is_unserved_at_a_compute() {
    use rafka_node_base::families::{BrokerData, BrokerDataReply, BrokerDataRequest};
    let broker = born(Role::broker(), "mesh1.broker.1").await;
    let compute = born(Role::compute(), "mesh1.compute.1").await;
    let (client, target) = broker.peer().await;
    let (out, _) = client.call::<BrokerData>(&target, &BrokerDataRequest::Append { key: "k".into(), value: b"v1".to_vec() }, &CallOptions::default()).await;
    let served_by = broker.launch.node_id.to_string();
    assert_eq!(out.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Appended { key: "k".into(), offset: 0, served_by: served_by.clone() }), "{out:?}");
    let (out, _) = client.call::<BrokerData>(&target, &BrokerDataRequest::Read { key: "k".into() }, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Value { key: "k".into(), value: Some(b"v1".to_vec()), served_by }), "{out:?}");
    assert!(broker.running.server.catalog().lookup(BrokerData::OP).is_some());

    let (client, target) = compute.peer().await;
    assert!(compute.running.server.catalog().lookup(BrokerData::OP).is_none(), "the compute carries the family for others and serves no handler for it");
    let (out, _) = client.call::<BrokerData>(&target, &BrokerDataRequest::Read { key: "k".into() }, &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::Unserved(u) if u.op() == BrokerData::OP), "a compute serves no broker_data: {out:?}");
    assert_eq!(rafka_node_rpc::ServerStats::get(&compute.running.server.stats().dispatched), 0);
    broker.running.stop(Duration::ZERO).await;
    compute.running.stop(Duration::ZERO).await;
}

/// e11.s6 — certainty on a gateway -> broker call, over the gateway's own one client:
/// a request cut before FIN is NotSent (499) and never applied; a reply lost after commit is
/// Indeterminate (the reply deadline); a handler fault is Indeterminate 423; and the reply
/// budget alone is not death proof: the broker still serves, on the same pooled connection.
#[tokio::test]
async fn a_gateways_call_to_a_broker_carries_the_four_certainty_outcomes() {
    use rafka_node_base::families::{BrokerData, BrokerDataReply, BrokerDataRequest, PROOF_FAULT, PROOF_HANG};
    let broker = born(Role::broker(), "mesh1.broker.1").await;
    let gateway = born(Role::gateway(), "mesh1.gateway.1").await;
    // The gateway's live resolver learns the broker's birth, as membership would feed it.
    let addr = broker.running.routers[0].endpoint().bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    gateway.running.node_rpc.resolver.apply(
        ResolvedNode {
            node_id: broker.launch.node_id.clone(),
            name: broker.launch.name.clone(),
            endpoint_id: broker.key.public(),
            transport_addr: addr,
            incarnation: broker.launch.incarnation.clone(),
        },
        None,
    );
    let client = gateway.running.node_rpc.client.clone();
    let target = NodeTarget::ExactNode(broker.launch.node_id.clone());
    let stats = broker.running.server.stats();
    let append = |key: &str| BrokerDataRequest::Append { key: key.into(), value: b"v".to_vec() };

    // 1. Cut before FIN: NotSent (499), never dispatched, nothing applied.
    let opts = CallOptions { cut_before_finish: true, ..Default::default() };
    let (out, ev) = client.call::<BrokerData>(&target, &append("c1"), &opts).await;
    assert!(matches!(&out, RpcOutcome::NotSent(n) if *n.reason() == NotSentReason::FrameNotSent), "{out:?}");
    assert!(!ev.unwrap().committed);
    assert!(out.proves_not_dispatched());
    let (read, _) = client.call::<BrokerData>(&target, &BrokerDataRequest::Read { key: "c1".into() }, &CallOptions::default()).await;
    assert!(matches!(read.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Value { value: None, .. })), "nothing was applied: {read:?}");
    assert_eq!(ServerStats::get(&stats.dropped_unfinished), 1, "the broker dropped the unfinished request");

    // 2. Reply lost after commit: Indeterminate on the reply deadline; the request did cross.
    let opts = CallOptions { budget: Budget::Split { send: Duration::from_secs(5), reply: Duration::from_millis(300) }, ..Default::default() };
    let (out, ev) = client.call::<BrokerData>(&target, &append(PROOF_HANG), &opts).await;
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::ReplyDeadline), "{out:?}");
    assert!(ev.unwrap().committed, "the request crossed the commit cut");
    assert!(!out.proves_not_dispatched());

    // 3. A handler fault after dispatch: Indeterminate, reset 423.
    let (out, _) = client.call::<BrokerData>(&target, &append(PROOF_FAULT), &CallOptions::default()).await;
    assert!(matches!(&out, RpcOutcome::Indeterminate(i) if *i.reason() == IndeterminateReason::Reset(423)), "{out:?}");
    assert_eq!(ServerStats::get(&stats.faults), 1);

    // 4. The reply budget alone is not death proof: the broker serves the next call, and the
    //    gateway still resolves and reaches the same birth.
    let (out, _) = client.call::<BrokerData>(&target, &append("c2"), &CallOptions::default()).await;
    assert_eq!(out.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Appended { key: "c2".into(), offset: 0, served_by: broker.launch.node_id.to_string() }), "{out:?}");
    assert_eq!(client.pooled().iter().filter(|k| k.peer == broker.key.public()).count(), 1, "one pooled connection to the broker's one birth, kept through every outcome");
    gateway.running.stop(Duration::ZERO).await;
    broker.running.stop(Duration::ZERO).await;
}

/// e11.s9 — the malformed-context arm: a bad traceparent is dropped by the caller (named on a
/// `via-context-dropped` span, proven in `rafka-node-rpc/tests/context.rs`) and the call's outcome
/// is unchanged.
#[tokio::test]
async fn a_malformed_traceparent_is_dropped_and_the_call_is_unchanged() {
    use rafka_node_base::families::{BrokerData, BrokerDataReply, BrokerDataRequest};
    use rafka_node_rpc_contract::context::CallContext;
    let broker = born(Role::broker(), "mesh1.broker.1").await;
    let (client, target) = broker.peer().await;
    let opts = CallOptions {
        context: Some(CallContext { caller_system: Some("rdm".into()), traceparent: Some("not-a-traceparent".into()), tracestate: None, baggage: None }),
        ..Default::default()
    };
    let (out, _) = client.call::<BrokerData>(&target, &BrokerDataRequest::Append { key: "ctx".into(), value: b"v".to_vec() }, &opts).await;
    assert_eq!(out.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Appended { key: "ctx".into(), offset: 0, served_by: broker.launch.node_id.to_string() }), "{out:?}");
    broker.running.stop(Duration::ZERO).await;
}
