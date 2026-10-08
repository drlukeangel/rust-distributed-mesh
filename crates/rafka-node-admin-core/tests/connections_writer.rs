//! i143.e6.s11 functional: the source-owned connections writer. A node's own Direct and Proxy
//! facts are rows it writes (the raw log grows, the index keeps one entry per pair), applied to
//! the held projection the route resolver reads, and hydrated from the index at birth so a
//! restart keeps its latest Proxy (connections.md §13 B) and the reconnect series continues from
//! its latest failure while the index stays bounded (§13 D).

use rafka_mesh_entity::connections::{resolve, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rafka_mesh_entity::reconnect::reconnect_plan;
use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind, PathName};
use rafka_node_admin_core::connections_writer::ConnectionsWriter;
use rafka_node_admin_core::storage::{ConnectionsStorage, FileConnectionsStorage};
use rafka_node_rpc::{ConnectionObserver, ResolvedNode};
use std::sync::{Arc, Mutex};

const POLICY: CarrierPolicy = CarrierPolicy::Forwardable { carrier_kind: NodeKind::RpcNode };

fn end(name: &str) -> ConnectionEnd {
    ConnectionEnd { name: name.parse().unwrap(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) }
}

fn resolved(e: &ConnectionEnd) -> ResolvedNode {
    ResolvedNode {
        node_id: e.node_id.clone(),
        name: e.name.clone(),
        endpoint_id: iroh::SecretKey::generate().public(),
        transport_addr: "127.0.0.1:1".parse().unwrap(),
        incarnation: e.incarnation.clone().unwrap(),
    }
}

fn row(source: &ConnectionEnd, destination: &ConnectionEnd, kind: ConnectionKind, state: ConnectionState, carrier: Option<&ConnectionEnd>, at: u64) -> NodeConnection {
    NodeConnection { source: source.clone(), destination: destination.clone(), kind, state, carrier: carrier.cloned(), recovery: None, reason: None, logged_at_ms: at }
}

fn data_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("i143-e6s11-{tag}-{}", NodeId::mint()))
}

/// §13 B: the latest rows this node wrote are hydrated at birth, and the first resolution after
/// the restart is the Proxy, with no rediscovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_hydrates_the_latest_proxy_row_and_the_first_resolution_uses_it() {
    let dir = data_dir("hydrate");
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    // The previous birth wrote: Direct to dest failed, the carrier's edge to dest, a Proxy via the carrier.
    {
        let storage = FileConnectionsStorage::open(&dir).unwrap();
        let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, 10);
        failed.recovery = Some(rafka_mesh_entity::connections::DirectRecovery { recovery_epoch: 1, attempt_ordinal: 2 });
        for r in [failed, row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 11), row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 12)] {
            storage.append_history(&r).await.unwrap();
            storage.put_connection(&r).await.unwrap();
        }
    }
    // The next birth: a fresh writer over the same storage hydrates its held projection.
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&dir).unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    assert_eq!(writer.hydrate().await.unwrap(), 3);
    let r = resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY);
    assert!(matches!(r.route, EffectiveRoute::ViaPeer { ref carrier, .. } if *carrier == "mesh1.rpc.2".parse::<PathName>().unwrap()), "the Proxy is used without rediscovery: {:?}", r.route);
    assert!(r.retire.is_none());
    // The reconnect series continues from the latest failure: ordinal 2, not 1.
    let plan = reconnect_plan(&held.lock().unwrap(), |_| None);
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].recovery.attempt_ordinal, 2);
    let _ = std::fs::remove_dir_all(&dir);
}

/// §13 D: many failed attempts for one pair leave the raw log growing while the index holds one
/// Direct member for the pair; the ordinal counts up in one epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_failures_grow_the_raw_log_and_keep_the_index_at_one_direct_per_pair() {
    let dir = data_dir("bounded");
    let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&dir).unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.hydrate().await.unwrap();
    let target = resolved(&dest);
    for i in 0..7 {
        writer.direct_failed(&target, &format!("refused {i}"));
        writer.drain().await;
    }
    let history = storage.history().await.unwrap();
    assert_eq!(history.len(), 7, "every failure joins the raw log");
    // The index write trails the history append by one task step: read it once it names the
    // seventh ordinal.
    let mut index = storage.connections().await.unwrap();
    for _ in 0..200 {
        if index.first().and_then(|r| r.recovery.as_ref()).is_some_and(|r| r.attempt_ordinal == 7) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        index = storage.connections().await.unwrap();
    }
    assert_eq!(index.len(), 1, "one Direct member for the pair: {index:?}");
    assert_eq!(index[0].recovery.as_ref().map(|r| (r.recovery_epoch, r.attempt_ordinal)), Some((1, 7)), "the ordinal counts up in one epoch");
    assert_eq!(held.lock().unwrap().own_latest_directs().len(), 1);
    // A Direct Connected ends the series and is the pair's one Direct member now.
    writer.direct_connected(&target);
    writer.drain().await;
    let mut index = storage.connections().await.unwrap();
    for _ in 0..200 {
        if index.first().is_some_and(|r| r.state == ConnectionState::Connected) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        index = storage.connections().await.unwrap();
    }
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].state, ConnectionState::Connected);
    assert!(reconnect_plan(&held.lock().unwrap(), |_| None).is_empty(), "nothing is owed after a connect");
    let _ = std::fs::remove_dir_all(&dir);
}

/// §10: a Direct Connected beside an active Proxy owes the Proxy's retirement, written durably
/// as Disconnected(direct-restored); the route cuts back once it lands (the refused-write cell
/// below proves the Proxy stays effective until then).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_direct_connected_beside_a_proxy_is_retired_durably_and_the_route_cuts_back() {
    let dir = data_dir("retire");
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&dir).unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.hydrate().await.unwrap();
    writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 11)).await.unwrap();
    writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 12)).await.unwrap();
    writer.direct_connected(&resolved(&dest));
    // The observation's own write has landed and settled the retirement it owed.
    writer.drain().await;
    let retired = storage.history().await.unwrap().into_iter().last().unwrap();
    assert_eq!((retired.kind, retired.state, retired.reason.as_deref()), (ConnectionKind::Proxy, ConnectionState::Disconnected, Some("direct-restored")));
    assert_eq!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true });
    assert_eq!(writer.retire_owed().await.unwrap(), 0, "nothing owed once the retirement landed");
    let _ = std::fs::remove_dir_all(&dir);
}

/// §13 G: a connection the destination dialled in, accepted by this node, is Direct Connected from
/// this node to it and creates the same retirement obligation as a dial of its own: the Proxy's
/// retirement lands durably once the accepted fact's write lands, and the route cuts back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accepted_inbound_direct_connected_retires_the_proxy_like_a_dial() {
    let dir = data_dir("accepted");
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&dir).unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.hydrate().await.unwrap();
    writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 11)).await.unwrap();
    writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 12)).await.unwrap();
    writer.direct_accepted(&resolved(&dest));
    writer.drain().await;
    let history = storage.history().await.unwrap();
    let accepted = history.iter().find(|r| r.source.name == me.name && r.kind == ConnectionKind::Direct).expect("the accepted connection is a Direct row of this node");
    assert_eq!((accepted.destination.name.clone(), accepted.state), (dest.name.clone(), ConnectionState::Connected));
    let retired = history.last().unwrap();
    assert_eq!((retired.kind, retired.state, retired.reason.as_deref()), (ConnectionKind::Proxy, ConnectionState::Disconnected, Some("direct-restored")), "the accepted Direct settled the owed retirement");
    assert_eq!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true });
    assert_eq!(writer.retire_owed().await.unwrap(), 0, "nothing owed once the retirement landed");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A storage that refuses the next N index writes by name: the writer's reconciliation of an
/// owed retirement (connections.md §10) is proven against it.
struct RefusingIndex {
    inner: Arc<dyn ConnectionsStorage>,
    refuse: std::sync::atomic::AtomicU32,
}

#[async_trait::async_trait]
impl ConnectionsStorage for RefusingIndex {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        if self.refuse.fetch_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
            return Err(rafka_node_admin_core::record_store::StorageError::Io { file: "connections index".into(), reason: "refused by the cell".into() });
        }
        self.inner.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, rafka_node_admin_core::record_store::StorageError> {
        self.inner.connections().await
    }
    async fn remove_connection(&self, index: &rafka_mesh_entity::connections::ConnectionIndex) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        self.inner.remove_connection(index).await
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), rafka_node_admin_core::record_store::StorageError> {
        self.inner.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, rafka_node_admin_core::record_store::StorageError> {
        self.inner.history().await
    }
}

/// §10: a Direct Connected beside an active Proxy owes the Proxy's retirement; a refused
/// retirement write leaves the Proxy effective and the obligation standing, derived again from
/// the rows; once the write lands, new calls cut back to Direct. The bounded reconciler lands it
/// by itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_retirement_write_keeps_the_proxy_effective_until_it_lands() {
    let dir = data_dir("refused-retirement");
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage = Arc::new(RefusingIndex { inner: Arc::new(FileConnectionsStorage::open(&dir).unwrap()), refuse: std::sync::atomic::AtomicU32::new(0) });
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.hydrate().await.unwrap();
    writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 10)).await.unwrap();
    writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 11)).await.unwrap();
    // The Direct comes back (recorded durably here, as a dial's own report would land it).
    writer.record(row(&me, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 12)).await.unwrap();
    assert_eq!(writer.owed().len(), 1, "the Proxy's retirement is owed");
    // The retirement's index write is refused: the Proxy stays effective, the obligation stands.
    storage.refuse.store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(writer.settle_owed().await.is_err(), "the refused write is named");
    assert!(matches!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::ViaPeer { .. }), "new calls keep the Proxy");
    assert_eq!(writer.owed().len(), 1);
    assert!(!storage.connections().await.unwrap().iter().any(|r| r.kind == ConnectionKind::Proxy && r.state == ConnectionState::Disconnected), "nothing durable says the Proxy retired");
    // The bounded reconciler attempts it again until it lands; then Direct is effective.
    let _task = writer.spawn_retirement_reconciler(std::time::Duration::from_millis(50));
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while matches!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::ViaPeer { .. }) {
        assert!(std::time::Instant::now() < deadline, "the retirement never landed");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(matches!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true }));
    assert!(writer.owed().is_empty());
    let retired: Vec<NodeConnection> = storage.connections().await.unwrap().into_iter().filter(|r| r.kind == ConnectionKind::Proxy).collect();
    assert_eq!(retired.len(), 1);
    assert_eq!(retired[0].state, ConnectionState::Disconnected);
    assert_eq!(retired[0].reason.as_deref(), Some(rafka_mesh_entity::reconnect::DIRECT_RESTORED));
    assert!(storage.history().await.unwrap().iter().any(|r| r.kind == ConnectionKind::Proxy && r.state == ConnectionState::Disconnected), "the retirement joined the raw log");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The carrier's account of its own edge (connections.md section 8): a Failed or Disconnected
/// latest Direct toward the exact node is named; a Connected one and no fact at all are not.
///
/// CONTRACT: `edge_not_active` reports the carrier's own latest Direct fact toward the exact node
/// and nothing else: no fact is "not named", never "lost".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carrier_names_its_own_edge_only_when_its_latest_direct_is_not_connected() {
    use rafka_node_rpc::CarrierEdges;
    let dir = data_dir("edges");
    let (me, dest) = (end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&dir).unwrap());
    let writer = ConnectionsWriter::new(me.clone(), storage, Arc::new(Mutex::new(ConnectionsHeld::new())));
    writer.hydrate().await.unwrap();
    assert_eq!(writer.edge_not_active(&dest.node_id), None, "no fact is not an edge fact");
    writer.record(row(&me, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 10)).await.unwrap();
    assert_eq!(writer.edge_not_active(&dest.node_id), None, "a Connected edge is Active");
    let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, 20);
    failed.recovery = Some(rafka_mesh_entity::connections::DirectRecovery { recovery_epoch: 1, attempt_ordinal: 1 });
    failed.reason = Some("dial failed".into());
    writer.record(failed).await.unwrap();
    let why = writer.edge_not_active(&dest.node_id).expect("a Failed edge is not Active");
    assert_eq!(why, "mesh1.rpc.2 -> mesh1.rpc.3 Direct failed (dial failed)");
    assert_eq!(writer.edge_not_active(&NodeId::mint()), None, "another node's edge is not named");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT: a fact is held only after its durable write lands. A refused write leaves no fact
/// in the held projection, the route still reads "never observed", and the next observation of
/// the same connection writes it afresh: nothing resends it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_observation_write_leaves_no_held_fact_and_the_next_observation_lands_it() {
    let dir = data_dir("refused-observation");
    let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let storage = Arc::new(RefusingIndex { inner: Arc::new(FileConnectionsStorage::open(&dir).unwrap()), refuse: std::sync::atomic::AtomicU32::new(0) });
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.hydrate().await.unwrap();
    storage.refuse.store(1, std::sync::atomic::Ordering::SeqCst);
    writer.direct_connected(&resolved(&dest));
    writer.drain().await;
    {
        let h = held.lock().unwrap();
        assert!(!h.holds_direct_fact(&me.name, &dest.name), "a refused fact is not held");
        assert!(h.own_latest_directs().is_empty());
        assert_eq!(resolve(&h, &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: false });
    }
    assert!(storage.connections().await.unwrap().is_empty(), "nothing durable in the index");
    // Nothing resends it.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert!(!held.lock().unwrap().holds_direct_fact(&me.name, &dest.name));
    // The next observation of the connection lands it.
    writer.direct_connected(&resolved(&dest));
    writer.drain().await;
    assert_eq!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true });
    let index = storage.connections().await.unwrap();
    assert_eq!((index.len(), index[0].state), (1, ConnectionState::Connected));
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT: a node that has never connected to a peer calls it directly; when the peer is dead
/// the call ends `NotSent` and the failed dial is recorded as this node's Direct Failed fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dial_to_a_dead_peer_with_no_prior_fact_is_not_sent_and_records_direct_failed() {
    use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, StaticResolver};
    use rafka_node_rpc_contract::outcome::RpcOutcome;
    use rafka_node_rpc_contract::ping::{Ping, PingRequest};
    let dir = data_dir("dead-cold");
    let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&dir).unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = Arc::new(ConnectionsWriter::new(me.clone(), storage.clone(), held.clone()));
    writer.hydrate().await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(resolved(&dest));
    let ep = rafka_node_rpc::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm").with_connection_observer(writer.clone());
    let opts = CallOptions { budget: Budget::Overall(std::time::Duration::from_millis(300)), ..Default::default() };
    let resolution = resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY);
    let call = client.call_resolved::<Ping>(resolution, &me.name, &dest.name, &dest.node_id, &PingRequest::Ping { payload: vec![1] }, &opts).await;
    assert_eq!(call.route, EffectiveRoute::Direct { known: false });
    assert!(matches!(call.outcome, RpcOutcome::NotSent(_)), "{:?}", call.outcome);
    writer.drain().await;
    let index = storage.connections().await.unwrap();
    assert_eq!(index.len(), 1, "{index:?}");
    assert_eq!((index[0].kind, index[0].state), (ConnectionKind::Direct, ConnectionState::Failed));
    assert_eq!(index[0].recovery.map(|r| r.attempt_ordinal), Some(1));
    assert_eq!(held.lock().unwrap().own_latest_directs().len(), 1, "the failure is held once it is durable");
    let _ = std::fs::remove_dir_all(&dir);
}
