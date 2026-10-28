//! e11.s2 — a role process on the imported substrate: one endpoint carrying gossip and Node RPC,
//! one built catalog (core + the role's families + the product's transitional adapters), one
//! server on the Node RPC ALPN. The cells call the role the way a peer does, over the wire.

use iroh::SecretKey;
use rdm_core::launch::Launch;
use rdm_core::runtime::RuntimeFact;
use rdm_core::{FabricId, IncarnationId, MeshId, NodeId};
use rdm_test::{compose, Role, LEGACY_ADAPTERS};
use rdm_transport::{Budget, CallOptions, Decode, NodeRpcClient, NodeTarget, ResolvedNode, ServerStats, StaticResolver};
use rdm_core::catalog::{EntryKind, OpOwner};
use rdm_core::outcome::{IndeterminateReason, NotSentReason, RpcOutcome};
use rdm_core::ping::{Ping, PingReply, PingRequest};
use rdm_core::protocol::NodeProtocol;
use rdm_test::node::{self, RunningNode};
use rdm_test::rig::{admin_side, AdminSide, TEST_MESH_ID};
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

/// The role is the node_type `name`'s launch names ([`Role::of`]), as the one node executable takes it.
async fn born(name: &str) -> RoleProcess {
    born_on(name, None).await.0
}

/// [`born`], also registering the data-plane `alpn` when one is given: its accept half returned.
async fn born_on(name: &str, alpn: Option<rdm_transport::connections::Alpn>) -> (RoleProcess, Option<rdm_transport::connections::Acceptor>) {
    let dir = tempdir::Dir::new();
    RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("cell")).unwrap().write_record(dir.path()).unwrap();
    let key = node::load_or_mint_key(dir.path()).unwrap();
    // The admin that deployed this birth: it takes the node's JoinNode and serves its topology.
    let fabric_id = FabricId::mint();
    let launcher = admin_side("127.0.0.1".parse().unwrap(), &fabric_id).await;
    let launch = Launch { topology_storage_provider: rdm_core::launch::TOPOLOGY_STORAGE_FILE.into(),
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
        store_dir: dir.path().to_path_buf(),
        mesh_id: Some(MeshId::parse(TEST_MESH_ID).unwrap()),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
    };
    launcher.deployed(&launch, &key);
    let role = Role::of(&launch);
    let oracles = rdm_test::Oracles::open(&launch).unwrap();
    let register = |b: rdm_transport::ServerBuilder, seams: rdm_test::originate::Seams| oracles.serve(compose(role, &launch.node_id.to_string(), b, seams.client), &launch, seams.resolver, &seams.name);
    let (running, acceptor) = match alpn {
        None => (node::start_with_seams(&launch, register).await.unwrap(), None),
        Some(alpn) => {
            let (running, acceptor) = node::start_with_seams_and_alpn(&launch, alpn, register).await.unwrap();
            (running, Some(acceptor))
        }
    };
    let _ = oracles.declare_client.set(running.node_rpc.clone());
    (RoleProcess { running, _launcher: launcher, launch, key, _dir: dir }, acceptor)
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
        let ep = rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        (NodeRpcClient::new(ep, resolver), NodeTarget::ExactNode(self.launch.node_id.clone()))
    }
}

#[tokio::test]
async fn a_role_process_serves_core_ping_on_its_one_endpoint() {
    let broker = born("mesh1.broker.1").await;
    assert_eq!(broker.running.routers.len(), 1, "one Iroh endpoint carries gossip and Node RPC");
    let (client, target) = broker.peer().await;
    let (out, _) = client.call::<Ping>(&target, &PingRequest::Ping { payload: b"role".to_vec() }, &CallOptions::default()).await;
    assert_eq!(out.reply().map(|r| r.value().clone()), Some(PingReply::Pong { payload: b"role".to_vec() }), "{out:?}");
    broker.running.stop().await;
}

#[tokio::test]
async fn a_tag_the_catalog_does_not_hold_is_unserved_421() {
    let gateway = born("mesh1.gateway.1").await;
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
    assert_eq!(rdm_transport::ServerStats::get(&gateway.running.server.stats().dispatched), 0, "an unserved op is never dispatched");
    gateway.running.stop().await;
}

#[tokio::test]
async fn the_products_adapter_is_catalogued_and_unserved_on_the_node_rpc_alpn() {
    let compute = born("mesh1.compute.1").await;
    let adapter = &LEGACY_ADAPTERS[0];
    let held = compute.running.server.catalog().lookup(adapter.op).expect("the adapter is catalogued");
    assert_eq!(held.owner, OpOwner::Product(adapter.owner.into()), "built under the ledger's owner");
    assert!(matches!(&held.kind, EntryKind::Transitional { migration_unit } if migration_unit == adapter.migration_unit));
    let (client, target) = compute.peer().await;
    let (out, _) = client
        .invoke_raw::<PingReply, _>(&target, adapter.op, vec![1, 2, 3], 1024, &CallOptions::default(), |d| match d {
            Decode::Committed(c, b) => c.reply::<Ping>(b),
            Decode::Early(e, b) => e.reply::<Ping>(b),
        })
        .await;
    assert!(matches!(&out, RpcOutcome::Unserved(u) if u.op() == adapter.op), "{out:?}");
    compute.running.stop().await;
}

/// The birth `p` runs, as a peer's resolver holds it.
fn birth_of(p: &RoleProcess) -> ResolvedNode {
    let addr = p.running.routers[0].endpoint().bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    ResolvedNode { node_id: p.launch.node_id.clone(), name: p.launch.name.clone(), endpoint_id: p.key.public(), transport_addr: addr, incarnation: p.launch.incarnation.clone() }
}

// @feature: mesh-rpc
/// CONTRACT (the testkit's Node RPC direct dial block, `rdm_test::connection_faults::block_direct_node_rpc`):
/// gateway mesh1.gateway.1 holds a pooled connection to broker mesh2.broker.1 from a direct
/// `broker_data` append. Blocked toward the broker, its pooled connection is closed (a hard-cut
/// delete naming the broker's peer) and a direct read asked once ends `NotSent` `Connection`, never
/// committed, exactly as an unreachable dial ends, refused before any dial by the block (its
/// reject span). The same gateway's `call_routed` through mesh1.gateway.2, a carrier of its own
/// mesh, reaches the broker, which executes the append exactly once (the next append under its key
/// takes offset 1), and the gateway opens no direct connection to the broker meanwhile. Released,
/// the gateway's direct call dials again and is served by the broker. Must NOT happen: a blocked
/// direct call reaching the broker, the carrier's dial blocked, or a second execution of the
/// carried append.
#[test]
fn a_blocked_direct_node_rpc_dial_ends_not_sent_and_a_carrier_of_the_sources_mesh_still_reaches_the_destination() {
    use rdm_test::connection_faults::{block_direct_node_rpc, unblock_direct_node_rpc};
    use rdm_test::families::{BrokerData, BrokerDataReply, BrokerDataRequest};
    use rdm_transport::{RouteChoice, RouteLeg};
    let cap = crate::common::capture("a_blocked_direct_node_rpc_dial_ends_not_sent_and_a_carrier_of_the_sources_mesh_still_reaches_the_destination");
    let seen = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let broker = born("mesh2.broker.1").await;
        let source = born("mesh1.gateway.1").await;
        let carrier = born("mesh1.gateway.2").await;
        // Each view holds the births it calls, as the topology view would feed them.
        let (b, c) = (birth_of(&broker), birth_of(&carrier));
        source.running.node_rpc.resolver.apply(b.clone(), None);
        source.running.node_rpc.resolver.apply(c.clone(), None);
        carrier.running.node_rpc.resolver.apply(b.clone(), None);
        let client = source.running.node_rpc.client.clone();
        let target = NodeTarget::ExactNode(b.node_id.clone());
        let once = CallOptions { once: true, ..Default::default() };
        let served_by = b.node_id.to_string();
        let append = |key: &str| BrokerDataRequest::Append { key: key.into(), value: b"v".to_vec() };
        let to_broker = |c: &NodeRpcClient| c.pooled().iter().filter(|k| k.peer == b.endpoint_id).count();

        // Direct, before the block: served, one pooled connection to the broker.
        let (out, _) = client.call::<BrokerData>(&target, &append("d0"), &once).await;
        assert_eq!(out.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Appended { key: "d0".into(), offset: 0, served_by: served_by.clone() }), "{out:?}");
        assert_eq!(to_broker(&client), 1, "one pooled connection to the broker");

        // Blocked: the pooled connection is closed and a direct call ends NotSent Connection.
        let blocked = block_direct_node_rpc(&source.running, &broker.launch.name);
        assert_eq!(blocked, b, "the block keys the birth the source's view names at the broker's path");
        assert_eq!(to_broker(&client), 0, "the pooled connection to the blocked broker is closed");
        let (out, ev) = client.call::<BrokerData>(&target, &BrokerDataRequest::Read { key: "d0".into() }, &once).await;
        assert!(matches!(&out, RpcOutcome::NotSent(n) if matches!(n.reason(), NotSentReason::Connection(_))), "a blocked direct dial ends as an unreachable one: {out:?}");
        assert!(!ev.is_some_and(|e| e.committed), "nothing crossed the commit cut");
        let peer = b.endpoint_id.fmt_short().to_string();
        let named = |name: &str| seen.0.lock().unwrap().values().filter(|(n, _, f)| n == name && f.get("peer") == Some(&peer)).count();
        assert_eq!(named("rdm.node_rpc.connection.delete.via-hard-cut"), 1, "the block closed the one pooled connection to the broker");
        assert_eq!(named("rdm.node_rpc.connection.reject.via-direct-dial-block"), 1, "the blocked direct call was refused by the block, before any dial");

        // Carried through a carrier of the source's own mesh: the broker executes it, once.
        let via = RouteChoice::ViaPeer { carrier: c.node_id.clone(), path: c.name.clone() };
        let (out, _, leg) = client.call_routed::<BrokerData>(&b.node_id, &via, &append("r1"), &once).await;
        assert_eq!(leg, RouteLeg::ViaPeer { carrier: c.name.to_string() });
        assert_eq!(out.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Appended { key: "r1".into(), offset: 0, served_by: served_by.clone() }), "{out:?}");
        assert_eq!(to_broker(&client), 0, "the carried call opened no direct connection from the source to the broker");
        let (out, _) = carrier.running.node_rpc.client.call::<BrokerData>(&target, &append("r1"), &once).await;
        assert_eq!(out.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Appended { key: "r1".into(), offset: 1, served_by: served_by.clone() }), "the carried append executed exactly once: {out:?}");

        // Released: the direct call dials and is served.
        unblock_direct_node_rpc(&source.running, &broker.launch.name);
        let (out, _) = client.call::<BrokerData>(&target, &BrokerDataRequest::Read { key: "r1".into() }, &once).await;
        assert_eq!(out.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Value { key: "r1".into(), value: Some(b"v".to_vec()), served_by }), "{out:?}");
        assert_eq!(to_broker(&client), 1, "the direct call dialled the broker again");

        source.running.stop().await;
        carrier.running.stop().await;
        broker.running.stop().await;
    });
}

// @feature: connections
/// CONTRACT (states.md "Stopped nodes": "Stopped: in the system with its identity kept, receiving no
/// traffic, silent on gossip and not advancing"; connections.md §9.4: the node's one reconciler):
/// rpc node mesh1.rpc.1's Direct dials toward mesh1.rpc.2 on the app protocol are blocked, so its one
/// dial lands a `Direct Failed` whose next attempt is due one backoff later. mesh1.rpc.1 stops before
/// that due time (its stop's span `rdm.mesh.node.update.via-leave-shutdown`). Past the due time and two
/// reconciler ticks past it, the stopped node has dialled nothing: no
/// `rdm.node_admin.connection.create.via-recovery-dial` of its own begins after its stop's span.
/// Must NOT happen: the stopped birth's reconciler running on past its stop and dialling its recovery
/// series (the release red demo-rshape-consumer-connections row 6: a `via-recovery-dial` at ordinal 4,
/// 20 s after the stop, `NotSent`).
#[test]
fn a_stopped_node_dials_no_recovery_attempt_after_its_stop() {
    use rdm_core::reconnect::{deterministic_jitter, reconnect_backoff};
    use rdm_node::connection_reconnect::RECONCILER_TICK;
    use rdm_test::node::{app_protocol, APP_PROTOCOL};
    use rdm_transport::connections::{Birth, ConnectError};
    let cap = crate::common::capture("a_stopped_node_dials_no_recovery_attempt_after_its_stop");
    let seen = cap.spans.clone();
    cap.run(crate::common::Threads::Workers(4), async move {
        let (dest, _dest_accepts) = born_on("mesh1.rpc.2", Some(app_protocol())).await;
        let (source, _source_accepts) = born_on("mesh1.rpc.1", Some(app_protocol())).await;
        let (src, dst) = (source.launch.name.clone(), dest.launch.name.clone());
        let (src_s, dst_s) = (src.to_string(), dst.to_string());
        let to = Birth { name: dst.clone(), node_id: dest.launch.node_id.clone(), incarnation: dest.launch.incarnation.clone() };

        // One blocked Direct dial: its Direct Failed lands and arms the series (connections.md §9.4).
        rdm_test::connection_faults::block_direct(&source.running, &dst, APP_PROTOCOL);
        let dialled = source.running.data_connections_for_testkit().probe_direct(&to, APP_PROTOCOL).await;
        assert!(matches!(dialled, Err(ConnectError::NotSent { .. })), "a blocked Direct dial ends NotSent: {dialled:?}");
        let failed = |f: &std::collections::BTreeMap<String, String>| {
            f.get("source") == Some(&src_s) && f.get("destination") == Some(&dst_s) && f.get("kind").map(String::as_str) == Some("direct") && f.get("state").map(String::as_str) == Some("failed") && f.get("outcome").map(String::as_str) == Some("landed")
        };
        tokio::time::timeout(Duration::from_secs(30), seen.recorded("rdm.node_admin.connection.update.via-observed", failed)).await.expect("the blocked dial's Direct Failed lands");
        let (logged_at, ordinal) = seen.0.lock().unwrap().values().filter(|(n, _, f)| n == "rdm.node_admin.connection.update.via-observed" && failed(f)).map(|(_, _, f)| (f["logged_at_ms"].parse::<u64>().unwrap(), f["attempt_ordinal"].parse::<u32>().unwrap())).next().unwrap();
        let due_ms = logged_at + reconnect_backoff(ordinal) + deterministic_jitter(&src, &dst, ordinal);

        // The node stops while its series owes that next attempt.
        source.running.stop().await;
        let now_ms = || std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64;
        let stopped_ms = now_ms();
        assert!(stopped_ms < due_ms, "the node stopped before its series' next attempt was due (stopped {stopped_ms}, due {due_ms}, ordinal {ordinal})");

        // Past the due time and two reconciler ticks past it.
        tokio::time::sleep(Duration::from_millis(due_ms - stopped_ms) + 2 * RECONCILER_TICK).await;
        let spans = seen.0.lock().unwrap().clone();
        let stop = spans.iter().filter(|(_, (n, _, f))| n == "rdm.mesh.node.update.via-leave-shutdown" && f.get("node") == Some(&src_s)).map(|(k, _)| *k).min().expect("the node's stop is spanned");
        let after: Vec<_> = spans.iter().filter(|(k, (n, _, f))| **k > stop && n == "rdm.node_admin.connection.create.via-recovery-dial" && f.get("source") == Some(&src_s)).map(|(_, (_, _, f))| f.clone()).collect();
        assert!(after.is_empty(), "a stopped node dials nothing: {} recovery dial(s) of {src_s} began after its stop: {after:?}", after.len());

        dest.running.stop().await;
    });
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

/// e11.s8 — the product's own unary family: `broker_data` (0x40, forwardable), ledgered under
/// the product and served only by the broker. A compute catalogues the family (one ledger per
/// process) and carries it for others, but serves no handler for it: a call there is unserved, 421.
#[tokio::test]
async fn the_product_family_round_trips_to_a_broker_and_is_unserved_at_a_compute() {
    use rdm_test::families::{BrokerData, BrokerDataReply, BrokerDataRequest};
    let broker = born("mesh1.broker.1").await;
    let compute = born("mesh1.compute.1").await;
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
    assert_eq!(rdm_transport::ServerStats::get(&compute.running.server.stats().dispatched), 0);
    broker.running.stop().await;
    compute.running.stop().await;
}

/// e11.s6 — certainty on a gateway -> broker call, over the gateway's own one client:
/// a request cut before FIN is NotSent (499) and never applied; a reply lost after commit is
/// Indeterminate (the reply deadline); a handler fault is Indeterminate 423; and the reply
/// budget alone is not death proof: the broker still serves, on the same pooled connection.
#[tokio::test]
async fn a_gateways_call_to_a_broker_carries_the_four_certainty_outcomes() {
    use rdm_test::families::{BrokerData, BrokerDataReply, BrokerDataRequest, PROOF_FAULT, PROOF_HANG};
    let broker = born("mesh1.broker.1").await;
    let gateway = born("mesh1.gateway.1").await;
    // The gateway's live resolver learns the broker's birth, as the topology view would feed it.
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
    gateway.running.stop().await;
    broker.running.stop().await;
}

/// e11.s9 — the malformed-context arm: a bad traceparent is dropped by the caller (named on a
/// `via-context-dropped` span, proven in `rafka-node-rpc/tests/context.rs`) and the call's outcome
/// is unchanged.
#[tokio::test]
async fn a_malformed_traceparent_is_dropped_and_the_call_is_unchanged() {
    use rdm_test::families::{BrokerData, BrokerDataReply, BrokerDataRequest};
    use rdm_core::context::CallContext;
    let broker = born("mesh1.broker.1").await;
    let (client, target) = broker.peer().await;
    let opts = CallOptions {
        context: Some(CallContext { caller_system: Some("rdm".into()), traceparent: Some("not-a-traceparent".into()), tracestate: None, baggage: None }),
        ..Default::default()
    };
    let (out, _) = client.call::<BrokerData>(&target, &BrokerDataRequest::Append { key: "ctx".into(), value: b"v".to_vec() }, &opts).await;
    assert_eq!(out.reply().map(|r| r.value().clone()), Some(BrokerDataReply::Appended { key: "ctx".into(), offset: 0, served_by: broker.launch.node_id.to_string() }), "{out:?}");
    broker.running.stop().await;
}
