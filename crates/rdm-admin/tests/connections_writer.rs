//! i143.e6.s11 functional: the source-owned connections writer. A node's own Direct and Proxy
//! facts are rows it writes (the raw log grows, the index keeps one entry per pair), applied to
//! the held projection the route resolver reads. A birth holds no connection and reads none back
//! (Q227): a restart holds no Proxy (connections.md §13 B) and no Failed Direct (§13 C), and the
//! index stays bounded (§13 D).

use rdm_core::connections::{resolve, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rdm_core::{IncarnationId, NodeId, PathName};
use rdm_admin::connections_writer::{ConnectionsWriter, RecoveryDial};
use rdm_admin::storage::{ConnectionsStorage, FileConnectionsStorage};
use rdm_transport::{AcceptTrigger, ConnectionObserver, ResolvedNode};
use std::sync::{Arc, Mutex};

fn policy() -> CarrierPolicy {

    CarrierPolicy::Proxy { carrier_type: "node".into() }

}

fn end(name: rdm_core::PathName, mesh: &str, r#type: &str) -> ConnectionEnd {
    ConnectionEnd { mesh: mesh.into(), r#type: r#type.to_string(), path: name.path.clone(), name: name.name.clone(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) }
}

fn resolved(e: &ConnectionEnd) -> ResolvedNode {
    ResolvedNode { mesh: e.mesh.clone(), r#type: e.r#type.clone(),
        node_id: e.node_id.clone(),
        path: e.path.clone(), name: e.name.clone(),
        endpoint_id: iroh::SecretKey::generate().public(),
        transport_addr: "127.0.0.1:1".parse().unwrap(),
        incarnation: e.incarnation.clone().unwrap(),
    }
}

fn row(source: &ConnectionEnd, destination: &ConnectionEnd, kind: ConnectionKind, state: ConnectionState, carrier: Option<&ConnectionEnd>, at: u64) -> NodeConnection {
    NodeConnection { id: rdm_core::ConnectionId::mint(), source: source.clone(), destination: destination.clone(), kind, alpn: rdm_core::connections::ALPN.to_vec(), state, carrier: carrier.cloned(), reason: None, logged_at_ms: at }
}

fn data_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("i143-e6s11-{tag}-{}", NodeId::mint()))
}

/// §13 B (Q227): the rows a previous birth wrote stay in the store as written, and a fresh writer over
/// the same storage holds none of them: the first resolution after the restart dials the destination
/// directly, and holds no Direct row of the previous birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_holds_none_of_the_previous_births_rows_and_the_first_resolution_dials() {
    let dir = data_dir("restart-holds-none");
    let (me, carrier, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let open = || async { FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap() };
    {
        let storage = open().await;
        let failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, 10);
        for r in [failed, row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 11), row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 12)] {
            storage.append_history(&r).await.unwrap();
            storage.put_connection(&r).await.unwrap();
        }
    }
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(open().await);
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let _writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    let r = resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy());
    assert_eq!(r.route, EffectiveRoute::Direct { known: false }, "the restarted birth holds no Proxy and dials");
    assert!(held.lock().unwrap().own_latest_directs().is_empty(), "no Direct row of the previous birth is held");
    assert_eq!(storage.history().await.unwrap().len(), 3, "the store keeps the previous birth's rows as written");
    let _ = std::fs::remove_dir_all(&dir);
}

/// §13 D: many failed dials for one pair leave the raw log growing while the index holds one
/// Direct member for the pair, the latest failure.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_failures_grow_the_raw_log_and_keep_the_index_at_one_direct_per_pair() {
    let dir = data_dir("bounded");
    let (me, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    let target = resolved(&dest);
    for i in 0..7 {
        writer.direct_failed(&target, &format!("refused {i}"));
        writer.observations_landed().await;
    }
    let history = storage.history().await.unwrap();
    assert_eq!(history.len(), 7, "every failure joins the raw log");
    let index = storage.connections().await.unwrap();
    assert_eq!(index.len(), 1, "one Direct member for the pair: {index:?}");
    assert_eq!(index[0].reason.as_deref(), Some("refused 6"), "the index holds the latest failure");
    assert_eq!(held.lock().unwrap().own_latest_directs().len(), 1);
    // A Direct Connected is the pair's one Direct member now.
    writer.direct_connected(&target);
    writer.observations_landed().await;
    let index = storage.connections().await.unwrap();
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].state, ConnectionState::Connected);
    let _ = std::fs::remove_dir_all(&dir);
}

/// §10: a Direct Connected beside an active Proxy owes the Proxy's delete, written durably
/// as Disconnected(direct-restored); the route cuts back once it lands (the refused-write cell
/// below proves the Proxy stays effective until then).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_direct_connected_beside_a_proxy_disconnects_it_durably_and_the_route_cuts_back() {
    let dir = data_dir("delete");
    let (me, carrier, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 11)).await.unwrap();
    writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 12)).await.unwrap();
    writer.direct_connected(&resolved(&dest));
    // The observation's own write has landed and settled the delete it owed.
    writer.observations_landed().await;
    let deleted = storage.history().await.unwrap().into_iter().last().unwrap();
    assert_eq!((deleted.kind, deleted.state, deleted.reason.as_deref()), (ConnectionKind::Proxy, ConnectionState::Disconnected, Some("direct-restored")));
    assert_eq!(resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::Direct { known: true });
    assert_eq!(writer.disconnect_owed().await.unwrap(), 0, "nothing owed once the delete landed");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The data-plane dial a cell hands the reconciler (connections.md §9.3 step 3): every call is
/// recorded with its local instant, waits for the cell to let it go, then reports its outcome to
/// the writer as the data-plane client's observer would (§9.3 step 5: the reconnect module invents
/// no success fact of its own).
struct CellDial {
    writer: ConnectionsWriter,
    target: ResolvedNode,
    calls: Mutex<Vec<(ConnectionEnd, u64)>>,
    go: tokio::sync::Semaphore,
    outcomes: Mutex<std::collections::VecDeque<Result<(), String>>>,
}

impl CellDial {
    fn new(writer: &ConnectionsWriter, target: ResolvedNode, outcomes: Vec<Result<(), String>>) -> Arc<Self> {
        Arc::new(Self { writer: writer.clone(), target, calls: Mutex::new(Vec::new()), go: tokio::sync::Semaphore::new(0), outcomes: Mutex::new(outcomes.into()) })
    }

    fn dial(self: &Arc<Self>) -> RecoveryDial {
        let me = self.clone();
        Arc::new(move |destination: ConnectionEnd| {
            let me = me.clone();
            Box::pin(async move {
                me.calls.lock().unwrap().push((destination, now_ms()));
                me.go.acquire().await.expect("the cell's gate stays open").forget();
                let outcome = me.outcomes.lock().unwrap().pop_front().unwrap_or(Ok(()));
                match outcome {
                    Ok(()) => {
                        me.writer.direct_connected(&me.target);
                        "Reply".to_string()
                    }
                    Err(reason) => {
                        me.writer.direct_failed(&me.target, &reason);
                        "NotSent".to_string()
                    }
                }
            })
        })
    }

    fn calls(&self) -> Vec<(ConnectionEnd, u64)> {
        self.calls.lock().unwrap().clone()
    }
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as u64
}

/// Wait (bounded) until `cond` holds; `what` names it when it never does.
async fn until(what: &str, cond: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    while !cond() {
        assert!(std::time::Instant::now() < deadline, "never: {what}");
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// A source holding a Proxy to `dest` through `carrier`, its latest Direct to `dest` Failed at
/// `failed_at`: what a proven carried path leaves (connections.md §7).
async fn proxied(tag: &str, failed_at: u64) -> (std::path::PathBuf, ConnectionEnd, ConnectionEnd, Arc<dyn ConnectionsStorage>, Arc<Mutex<ConnectionsHeld>>, ConnectionsWriter, NodeConnection) {
    let dir = data_dir(tag);
    let (me, carrier, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    let failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, failed_at);
    writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, failed_at)).await.unwrap();
    writer.record(failed.clone()).await.unwrap();
    writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), failed_at + 1)).await.unwrap();
    (dir, me, dest, storage, held, writer, failed)
}

/// CONTRACT (connections.md §10 and §13 G, R-CN1): a connection this source accepts from the
/// exact destination birth it reaches through its Proxy is a recovery trigger, never Direct
/// evidence. The accept writes nothing; it wakes the source's reconciler, which makes one dial of
/// the source's own; only that dial's Direct Connected owes the Proxy's delete, and the route
/// cuts back once the delete lands. What must NOT happen: a Direct row written for the accept,
/// or the Proxy deleted before the source's own dial connected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accept_from_the_proxy_destination_writes_nothing_and_wakes_the_source_dial_whose_connected_disconnects_the_proxy() {
    let (dir, me, dest, storage, held, writer, _) = proxied("accept-trigger", now_ms() - 60_000).await;
    let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
    let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(50), Some(dial.dial()));
    let rows_before = storage.history().await.unwrap().len();
    writer.accepted(&resolved(&dest));
    writer.observations_landed().await;
    assert_eq!(storage.history().await.unwrap().len(), rows_before, "the accept wrote nothing");
    until("the source dials its own connection", || dial.calls().len() == 1).await;
    assert!(matches!(resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::ViaPeer { .. }), "the Proxy stays the route while the dial is out");
    assert_eq!(dial.calls()[0].0, dest, "the dial is toward the exact birth the Proxy names");
    dial.go.add_permits(1);
    until("the route cuts back", || matches!(resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::Direct { known: true })).await;
    let history = storage.history().await.unwrap();
    let after: Vec<(ConnectionKind, ConnectionState, Option<String>)> = history[rows_before..].iter().map(|r| (r.kind, r.state, r.reason.clone())).collect();
    assert_eq!(
        after,
        vec![(ConnectionKind::Direct, ConnectionState::Connected, None), (ConnectionKind::Proxy, ConnectionState::Disconnected, Some("direct-restored".to_string()))],
        "the source's own Direct Connected, then the delete it owed"
    );
    until("the attempt is spent once Direct is Connected", || writer.recovery_pending().is_empty()).await;
    assert_eq!(dial.calls().len(), 1, "one dial");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (connections.md §9, R-CN1 (2); Q348): accepts repeated from the destination coalesce into
/// one woken dial per (destination, destination incarnation), made once on `Send`, with no backoff from
/// the Failed row. What must NOT happen: one dial per accept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_accepts_coalesce_into_one_dial() {
    let (dir, me, dest, _storage, held, writer, _) = proxied("accept-coalesce", now_ms()).await;
    let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
    for _ in 0..5 {
        writer.accepted(&resolved(&dest));
    }
    assert_eq!(writer.recovery_pending(), vec![dest.clone()], "five accepts, one woken dial");
    let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(50), Some(dial.dial()));
    until("the one dial is made", || dial.calls().len() == 1).await;
    dial.go.add_permits(1);
    until("the route cuts back", || matches!(resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::Direct { known: true })).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(dial.calls().len(), 1, "one dial for five accepts");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (connections.md §9, R-CN1 (5); Q348): when the dial an accept woke fails, the Proxy stays the
/// route, the failure is one more Direct Failed row (one dial is one row), and nothing dials
/// again: the woken dial has ended, and only the next accept wakes another. What must NOT happen: the
/// Proxy deleted, or a second dial from the Failed row.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_dial_after_an_accept_keeps_the_proxy_and_is_not_made_again() {
    let (dir, me, dest, storage, held, writer, _) = proxied("accept-failed-dial", now_ms() - 60_000).await;
    let reason = "cell: the source's dial ended NotSent";
    let dial = CellDial::new(&writer, resolved(&dest), vec![Err(reason.to_string())]);
    let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(50), Some(dial.dial()));
    writer.accepted(&resolved(&dest));
    until("the source dials its own connection", || dial.calls().len() == 1).await;
    dial.go.add_permits(1);
    until("the failed dial's row is held", || held.lock().unwrap().own_latest_directs().iter().any(|d| d.reason.as_deref() == Some(reason))).await;
    let latest = storage.history().await.unwrap().into_iter().last().unwrap();
    assert_eq!((latest.kind, latest.state, latest.reason.as_deref()), (ConnectionKind::Direct, ConnectionState::Failed, Some(reason)), "the dial's one Failed row: {latest:?}");
    assert!(matches!(resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::ViaPeer { .. }), "the Proxy stays the route");
    assert!(writer.owed().is_empty(), "no delete is owed");
    until("the woken dial has ended", || writer.recovery_pending().is_empty()).await;
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    assert_eq!(dial.calls().len(), 1, "nothing dials again from the Failed row (Q348)");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (connections.md §9, R-CN1 (1)): only an accept from the exact birth the Proxy names wakes a
/// dial, and a dial is never made against a birth that is gone. An
/// accept from the destination's next birth (same path.name, new node and incarnation) arms
/// nothing; a pending attempt whose birth the topology view no longer holds is dropped undialled. What
/// must NOT happen: a dial toward the gone birth, or an attempt armed by a birth the Proxy does
/// not name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accept_from_another_birth_arms_nothing_and_an_attempt_for_a_gone_birth_is_dropped_undialled() {
    let (dir, _me, dest, _storage, held, writer, _) = proxied("accept-stale-birth", now_ms()).await;
    let topology_view = Arc::new(rdm_core::connections::StaticIncarnations::default());
    topology_view.set(dest.node_id.clone(), dest.location(), dest.incarnation.clone().unwrap());
    held.lock().unwrap().set_topology_view(topology_view.clone());
    let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
    let next = end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node");
    writer.accepted(&resolved(&next));
    assert!(writer.recovery_pending().is_empty(), "the next birth is not the birth the Proxy names, and the Failed row wakes nothing (Q348)");
    // The Proxy's own birth, gone from the view, then accepted: its woken dial is dropped undialled.
    topology_view.depart(dest.node_id.clone(), dest.location());
    writer.accepted(&resolved(&dest));
    assert_eq!(writer.recovery_pending(), vec![dest.clone()]);
    let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(50), Some(dial.dial()));
    until("the dial for the gone birth is dropped", || writer.recovery_pending().is_empty()).await;
    assert!(dial.calls().is_empty(), "no dial toward a birth that is gone");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A storage that refuses the next N index writes by name: the writer's reconciliation of an
/// owed delete (connections.md §10) is proven against it.
struct RefusingIndex {
    inner: Arc<dyn ConnectionsStorage>,
    refuse: std::sync::atomic::AtomicU32,
}

#[async_trait::async_trait]
impl ConnectionsStorage for RefusingIndex {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), rdm_admin::storage_error::StorageError> {
        if self.refuse.fetch_update(std::sync::atomic::Ordering::SeqCst, std::sync::atomic::Ordering::SeqCst, |n| n.checked_sub(1)).is_ok() {
            return Err(rdm_admin::storage_error::StorageError::Io { file: "connections index".into(), reason: "refused by the cell".into() });
        }
        self.inner.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, rdm_admin::storage_error::StorageError> {
        self.inner.connections().await
    }
    async fn connections_to(&self, destination: &rdm_core::NodeId) -> Result<Vec<NodeConnection>, rdm_admin::storage_error::StorageError> {
        self.inner.connections_to(destination).await
    }
    async fn remove_connection(&self, index: &rdm_core::connections::ConnectionIndex) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.remove_connection(index).await
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.inner.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, rdm_admin::storage_error::StorageError> {
        self.inner.history().await
    }
}

/// §10: a Direct Connected beside an active Proxy owes the Proxy's delete; a refused
/// delete write leaves the Proxy effective and the obligation standing, derived again from
/// the rows; once the write lands, new calls cut back to Direct. The bounded reconciler lands it
/// by itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_disconnected_write_keeps_the_proxy_effective_until_it_lands() {
    let dir = data_dir("refused-delete");
    let (me, carrier, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let storage = Arc::new(RefusingIndex { inner: Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap()), refuse: std::sync::atomic::AtomicU32::new(0) });
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 10)).await.unwrap();
    writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 11)).await.unwrap();
    // The Direct comes back (recorded durably here, as a dial's own report would land it).
    writer.record(row(&me, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 12)).await.unwrap();
    assert_eq!(writer.owed().len(), 1, "the Proxy's delete is owed");
    // The delete's index write is refused: the Proxy stays effective, the obligation stands.
    storage.refuse.store(1, std::sync::atomic::Ordering::SeqCst);
    assert!(writer.settle_owed().await.is_err(), "the refused write is named");
    assert!(matches!(resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::ViaPeer { .. }), "new calls keep the Proxy");
    assert_eq!(writer.owed().len(), 1);
    assert!(!storage.connections().await.unwrap().iter().any(|r| r.kind == ConnectionKind::Proxy && r.state == ConnectionState::Disconnected), "nothing durable says the Proxy deleted");
    // The bounded reconciler attempts it again until it lands; then Direct is effective.
    let _task = writer.spawn_reconciler(std::time::Duration::from_millis(50), None);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while matches!(resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::ViaPeer { .. }) {
        assert!(std::time::Instant::now() < deadline, "the delete never landed");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    assert!(matches!(resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::Direct { known: true }));
    assert!(writer.owed().is_empty());
    let deleted: Vec<NodeConnection> = storage.connections().await.unwrap().into_iter().filter(|r| r.kind == ConnectionKind::Proxy).collect();
    assert_eq!(deleted.len(), 1);
    assert_eq!(deleted[0].state, ConnectionState::Disconnected);
    assert_eq!(deleted[0].reason.as_deref(), Some(rdm_core::reconnect::DIRECT_RESTORED));
    assert!(storage.history().await.unwrap().iter().any(|r| r.kind == ConnectionKind::Proxy && r.state == ConnectionState::Disconnected), "the delete joined the raw log");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The carrier's account of its own connection (connections.md section 8): a Failed or Disconnected
/// latest Direct toward the exact node is named; a Connected one and no fact at all are not.
///
/// CONTRACT: `connection_not_active` reports the carrier's own latest Direct fact toward the exact node
/// and nothing else: no fact is "not named", never "lost".
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carrier_names_its_own_connection_only_when_its_latest_direct_is_not_connected() {
    use rdm_transport::CarrierConnections;
    let dir = data_dir("connections");
    let (me, dest) = (end(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
    let writer = ConnectionsWriter::new(me.clone(), storage, Arc::new(Mutex::new(ConnectionsHeld::new())));
    assert_eq!(writer.connection_not_active(&dest.node_id).await, None, "no fact is not a connection fact");
    writer.record(row(&me, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 10)).await.unwrap();
    assert_eq!(writer.connection_not_active(&dest.node_id).await, None, "a Connected connection is Active");
    let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, 20);
    failed.reason = Some("dial failed".into());
    writer.record(failed).await.unwrap();
    let why = writer.connection_not_active(&dest.node_id).await.expect("a Failed connection is not Active");
    assert_eq!(why, format!("{} -> {} Direct failed (dial failed)", me.node_id, dest.node_id));
    assert_eq!(writer.connection_not_active(&NodeId::mint()).await, None, "another node's connection is not named");
    let _ = std::fs::remove_dir_all(&dir);
}

/// A storage whose index write takes its time.
struct SlowIndex(Arc<dyn ConnectionsStorage>);

#[async_trait::async_trait]
impl ConnectionsStorage for SlowIndex {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), rdm_admin::storage_error::StorageError> {
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        self.0.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, rdm_admin::storage_error::StorageError> {
        self.0.connections().await
    }
    async fn connections_to(&self, destination: &rdm_core::NodeId) -> Result<Vec<NodeConnection>, rdm_admin::storage_error::StorageError> {
        self.0.connections_to(destination).await
    }
    async fn remove_connection(&self, index: &rdm_core::connections::ConnectionIndex) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.remove_connection(index).await
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, rdm_admin::storage_error::StorageError> {
        self.0.history().await
    }
}

/// CONTRACT (#2803 detection; Q169): the carrier names its connection from the dial that just failed,
/// from the dial's own outcome, however long the fact's write takes, so a member that cannot reach
/// its own node-admin says `connection-lost` every time, never "not sent" for want of a write. The
/// read of its held connections never waits for a write: until the Failed row lands it holds no fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carrier_names_the_connection_a_dial_has_just_failed_however_slow_the_write() {
    use rdm_transport::CarrierConnections;
    let dir = data_dir("connections-slow");
    let (me, dest) = (end(rdm_core::PathName::new("mesh2.node", "1").unwrap(), "mesh2", "node"), end(rdm_core::PathName::new("mesh2.node-admin", "1").unwrap(), "mesh2", "node-admin"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(SlowIndex(Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap())));
    let writer = ConnectionsWriter::new(me.clone(), storage, Arc::new(Mutex::new(ConnectionsHeld::new())));
    writer.direct_failed(&resolved(&dest), "timed out");
    let why = writer.connection_after_dial(&resolved(&dest), "timed out").await.expect("the failed dial's fact is named");
    assert_eq!(why, format!("{} -> {} Direct failed (timed out)", me.node_id, dest.node_id));
    let held_now = tokio::time::timeout(std::time::Duration::from_millis(100), writer.connection_not_active(&dest.node_id)).await.expect("the read answers from what is held, never waiting for the write");
    assert_eq!(held_now, None, "the Failed row has not landed, so no fact is held yet");
    writer.observations_landed().await;
    assert_eq!(writer.connection_not_active(&dest.node_id).await.as_deref(), Some(format!("{} -> {} Direct failed (timed out)", me.node_id, dest.node_id).as_str()), "once landed the fact is held");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (Q169): a fact is held only after its durable write lands. A refused write leaves no
/// fact in the held projection, the route still reads "never observed", and the write stays owed:
/// it is retried with backoff and lands without a second observation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_observation_write_is_held_only_once_its_retry_lands() {
    let dir = data_dir("refused-observation");
    let (me, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let storage = Arc::new(RefusingIndex { inner: Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap()), refuse: std::sync::atomic::AtomicU32::new(0) });
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    storage.refuse.store(1, std::sync::atomic::Ordering::SeqCst);
    writer.direct_connected(&resolved(&dest));
    // The first attempt is refused; the retry waits at least three quarters of the publish cadence.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while storage.refuse.load(std::sync::atomic::Ordering::SeqCst) > 0 {
        assert!(std::time::Instant::now() < deadline, "the write was never attempted");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
    {
        let h = held.lock().unwrap();
        assert!(!h.holds_direct_fact(&me.node_id, &dest.node_id), "a refused fact is not held");
        assert!(h.own_latest_directs().is_empty());
        assert_eq!(resolve(&h, &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::Direct { known: false });
    }
    assert!(storage.connections().await.unwrap().is_empty(), "nothing durable in the index");
    assert_eq!(writer.in_flight(), 1, "the write stays owed");
    // The retry lands it, with no second observation.
    writer.observations_landed().await;
    assert_eq!(resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy()).route, EffectiveRoute::Direct { known: true });
    let index = storage.connections().await.unwrap();
    assert_eq!((index.len(), index[0].state), (1, ConnectionState::Connected));
    assert_eq!(storage.history().await.unwrap().len(), 1, "the raw log took the row once: only the refused index write was attempted again");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT: a node that has never connected to a peer calls it directly; when the peer is dead
/// the call ends `NotSent` and the failed dial is recorded as this node's Direct Failed fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dial_to_a_dead_peer_with_no_prior_fact_is_not_sent_and_records_direct_failed() {
    use rdm_transport::{CallOptions, NodeRpcClient, StaticResolver};
    use rdm_core::outcome::RpcOutcome;
    use rdm_core::ping::{Ping, PingRequest};
    let dir = data_dir("dead-cold");
    let (me, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = Arc::new(ConnectionsWriter::new(me.clone(), storage.clone(), held.clone()));
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(resolved(&dest));
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm").with_connection_observer(writer.clone());
    let opts = CallOptions { send: rdm_transport::Send { attempts: 1, time_limit: rdm_transport::TimeLimit(std::time::Duration::from_millis(300)) }, ..Default::default() };
    let resolution = resolve(&held.lock().unwrap(), &me.node_id, &dest.node_id, policy());
    let call = client.call_resolved::<Ping>(resolution, &me.node_id, &dest.location(), &dest.node_id, &PingRequest::Ping { payload: vec![1] }, &opts).await;
    assert_eq!(call.route, EffectiveRoute::Direct { known: false });
    assert!(matches!(call.outcome, RpcOutcome::NotSent(_)), "{:?}", call.outcome);
    writer.observations_landed().await;
    let index = storage.connections().await.unwrap();
    assert_eq!(index.len(), 1, "{index:?}");
    assert_eq!((index[0].kind, index[0].state), (ConnectionKind::Direct, ConnectionState::Failed));
    assert_eq!(held.lock().unwrap().own_latest_directs().len(), 1, "the failure is held once it is durable");
    let _ = std::fs::remove_dir_all(&dir);
}

/// An index whose put of the first failure's fact is slow: the interleaving where an earlier
/// observation's index write would land after a later one's.
struct SlowFirstPut(rdm_admin::storage::MemoryConnectionsStorage);

#[async_trait::async_trait]
impl ConnectionsStorage for SlowFirstPut {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), rdm_admin::storage_error::StorageError> {
        if fact.reason.as_deref() == Some("refused 0") {
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        self.0.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, rdm_admin::storage_error::StorageError> {
        self.0.connections().await
    }
    async fn connections_to(&self, destination: &rdm_core::NodeId) -> Result<Vec<NodeConnection>, rdm_admin::storage_error::StorageError> {
        self.0.connections_to(destination).await
    }
    async fn remove_connection(&self, index: &rdm_core::connections::ConnectionIndex) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.remove_connection(index).await
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), rdm_admin::storage_error::StorageError> {
        self.0.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, rdm_admin::storage_error::StorageError> {
        self.0.history().await
    }
}

/// CONTRACT: observations of one pair reach the index in the order they were observed, so after
/// every observation has landed, the index member of the pair is the latest one even when the first
/// observation's index write is the slowest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_earlier_index_write_never_overwrites_a_later_observation() {
    let (me, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(SlowFirstPut(Default::default()));
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held);
    let target = resolved(&dest);
    for i in 0..3 {
        writer.direct_failed(&target, &format!("refused {i}"));
    }
    writer.observations_landed().await;
    let index = storage.connections().await.unwrap();
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].reason.as_deref(), Some("refused 2"), "the index holds the latest observation: {index:?}");
}

/// CONTRACT (node-identity-and-state.md §8: `source` is the end's path.name and `source_node_id` the
/// instance that wrote the entry; R12: the node owns its name): a writer reading its name from the
/// process's held name writes its next row under a name the node took, with the same node id and
/// birth, and holds that row as its own Direct; the row it wrote under the birth's name stays in the
/// raw log exactly as written and is no longer its own. The writer reads its name from one place: a
/// second binding is refused by name. What must NOT happen: a row after the rename naming the name
/// the node gave up, or the earlier row rewritten.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_writer_names_the_name_its_node_took_on_the_next_row_and_keeps_the_rows_written_before() {
    let dir = data_dir("held-name");
    let (me, peer) = (end(rdm_core::PathName::new("mesh1.node", "7").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"));
    let taken: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    // The process's held name, as its topology view is handed it: none told yet, so the birth's.
    let told: Arc<Mutex<Option<PathName>>> = Arc::default();
    let (cell, birth) = (told.clone(), me.location());
    writer.follow_held_name(move || cell.lock().unwrap().clone().unwrap_or_else(|| birth.clone())).unwrap();
    assert!(writer.follow_held_name(|| rdm_core::PathName::new("mesh1.node", "9").unwrap()).is_err_and(|e| e.contains(me.node_id.as_str()) && e.contains("already reads its name")), "one place holds the name");
    let target = resolved(&peer);
    writer.direct_connected(&target);
    writer.observations_landed().await;
    let before = storage.history().await.unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!((&before[0].source.name, &before[0].source.node_id), (&me.name, &me.node_id), "the row before the rename names the birth's name: {:?}", before[0]);

    *told.lock().unwrap() = Some(taken.clone());
    assert_eq!(writer.own(), ConnectionEnd { path: taken.path.clone(), name: taken.name.clone(), ..me.clone() }, "the source end is the name the node holds, with its node id and birth");
    writer.direct_broken(&target, "the pooled connection closed");
    writer.observations_landed().await;
    let after = storage.history().await.unwrap();
    assert_eq!(after.len(), 2);
    assert_eq!(after[0], before[0], "the row written before the rename stays exactly as written");
    assert_eq!((&after[1].source, &after[1].destination.name, after[1].state), (&ConnectionEnd { path: taken.path.clone(), name: taken.name.clone(), ..me.clone() }, &peer.name, ConnectionState::Disconnected), "{:?}", after[1]);
    let own: Vec<NodeConnection> = held.lock().unwrap().own_latest_directs().into_iter().cloned().collect();
    assert_eq!(own, vec![after[1].clone()], "the row under the held name is this node's own Direct, the birth name's is not");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (node-identity-and-state.md §8: `source_node_id` is the instance at each end when the
/// entry was written; connections.md §13 C, Q227: a restart holds no Failed Direct): a restarted birth
/// of `mesh1.node.1` (same node id, next incarnation) reads none of the previous birth's Failed Direct
/// back, and its failed dial toward the destination writes a row naming its OWN birth as the source.
/// What must NOT happen: the row naming the previous birth.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_births_failed_dial_row_names_its_own_birth() {
    let dir = data_dir("restart-birth");
    let (previous, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let me = ConnectionEnd { incarnation: Some(IncarnationId::mint()), ..previous.clone() };
    let open = || async { FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap() };
    {
        let storage = open().await;
        let failed = row(&previous, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, 10);
        storage.append_history(&failed).await.unwrap();
        storage.put_connection(&failed).await.unwrap();
    }
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(open().await);
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), Arc::new(Mutex::new(ConnectionsHeld::new())));
    writer.direct_failed(&resolved(&dest), "refused after the restart");
    writer.observations_landed().await;
    let history = storage.history().await.unwrap();
    let next = history.last().unwrap();
    assert_eq!(next.source, me, "the row names the birth that wrote it: {next:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (node-identity-and-state.md §8: `destination_node_id` is the instance at that end when the
/// entry was written; connections.md §9.3 step 1: "Verify the destination's `Node.incarnation_id` is
/// the one the dial names"): failed dials against `mesh1.node.3`'s birth, then a failed dial to its
/// next birth (same node id, next incarnation), write one row per dial, the last naming the birth that
/// was dialled. What must NOT happen: a row naming the gone birth as the destination of a dial to its
/// successor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_dial_to_the_destinations_next_birth_names_that_birth() {
    let dir = data_dir("next-birth");
    let (me, gone) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let next = ConnectionEnd { incarnation: Some(IncarnationId::mint()), ..gone.clone() };
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), Arc::new(Mutex::new(ConnectionsHeld::new())));
    for i in 0..2 {
        writer.direct_failed(&resolved(&gone), &format!("refused {i}"));
        writer.observations_landed().await;
    }
    writer.direct_failed(&resolved(&next), "refused by the next birth");
    writer.observations_landed().await;
    let history = storage.history().await.unwrap();
    assert_eq!(history.iter().map(|r| (r.state, r.destination.incarnation.clone())).collect::<Vec<_>>(), vec![(ConnectionState::Failed, gone.incarnation.clone()), (ConnectionState::Failed, gone.incarnation.clone()), (ConnectionState::Failed, next.incarnation.clone())], "one row per dial, each naming the birth dialled: {history:#?}");
    assert_eq!(history[2].destination, next, "the row names the birth that was dialled: {:?}", history[2]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// The spans named `name` open at each moment. A close takes `close_takes` before it is counted:
/// the thread that ends the span is held there, as a thread descheduled while it ends one would be.
#[derive(Clone)]
struct OpenSpans {
    name: &'static str,
    close_takes: std::time::Duration,
    open: Arc<std::sync::atomic::AtomicUsize>,
}

impl OpenSpans {
    fn named(name: &'static str, close_takes: std::time::Duration) -> Self {
        Self { name, close_takes, open: Arc::default() }
    }

    fn open(&self) -> usize {
        self.open.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> tracing_subscriber::Layer<S> for OpenSpans {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, _: &tracing::span::Id, _: tracing_subscriber::layer::Context<'_, S>) {
        if attrs.metadata().name() == self.name {
            self.open.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
    fn on_close(&self, id: tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        if ctx.span(&id).is_some_and(|s| s.name() == self.name) {
            std::thread::sleep(self.close_takes);
            self.open.fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        }
    }
}

/// CONTRACT (Q169; `observations_landed`: "the moment after which the index and the evidence hold
/// every fact this writer was handed"): a write owed inside a call's span (a recovery dial's) is a
/// child of that span, and once every owed write has landed the writer holds the span no longer:
/// the call's span has ended by the moment `observations_landed` resolves, however long the thread
/// that ends it takes to end it. What must NOT happen: `observations_landed` resolving while the
/// writer still holds the span of the call that owed the write, so its evidence is not complete.
#[test]
fn the_span_that_owed_a_write_has_ended_once_the_write_has_landed() {
    let owing = OpenSpans::named("rdm.test.connections_writer.owing", std::time::Duration::from_millis(100));
    spans_of("the_span_that_owed_a_write_has_ended_once_the_write_has_landed", Some(owing.clone()), || async {
        let dir = data_dir("owing-span");
        let (me, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
        let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
        let writer = ConnectionsWriter::new(me.clone(), storage.clone(), Arc::new(Mutex::new(ConnectionsHeld::new())));
        // The call owes its fact inside its own span and lets the span go: the owed write holds it.
        tracing::info_span!("rdm.test.connections_writer.owing").in_scope(|| writer.direct_connected(&resolved(&dest)));
        writer.observations_landed().await;
        let open = owing.open();
        tracing::info_span!("rdm.test.connections_writer.open-owing", open).in_scope(|| tracing::info!("the owing spans open once every owed write has landed"));
        assert_eq!(open, 0, "the span that owed the write ended before the write was counted landed");
        let _ = std::fs::remove_dir_all(&dir);
    });
}

/// CONTRACT (the writer's stamp: every row it writes carries a later one than the rows it wrote
/// before; connections.md §3: "A later event of the same kind replaces that kind's index row"): a
/// fact this writer observes after a row it recorded for the same pair is the pair's latest, whatever
/// the clock reads when the observation is stamped: a Direct Connected observed after a recorded
/// Direct Failed stamped at the clock's own millisecond, or later, is held.
/// What must NOT happen: the Connected row landing in the store while the held projection keeps the
/// Failed row as the pair's latest (one millisecond, and a Failed row wins a tie), so a destination
/// already connected resolves to no active route.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_observation_after_a_recorded_row_of_its_pair_is_held_as_the_latest_whatever_the_clock_reads() {
    let dir = data_dir("recorded-then-observed");
    let (me, dest) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    // Recorded at a millisecond the clock has not passed when the observation is stamped.
    let failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, now_ms() + 1_000);
    writer.record(failed.clone()).await.unwrap();
    assert!(writer.recovery_pending().is_empty(), "the recorded Failed row wakes no dial (Q348)");
    writer.direct_connected(&resolved(&dest));
    writer.observations_landed().await;
    let connected = storage.history().await.unwrap().into_iter().last().unwrap();
    assert_eq!((connected.kind, connected.state), (ConnectionKind::Direct, ConnectionState::Connected), "the observation landed: {connected:?}");
    assert!(connected.logged_at_ms > failed.logged_at_ms, "the observation is stamped after the row recorded before it: {} after {}", connected.logged_at_ms, failed.logged_at_ms);
    let latest: Vec<(ConnectionState, rdm_core::ConnectionId)> = held.lock().unwrap().own_latest_directs().into_iter().map(|d| (d.state, d.id.clone())).collect();
    assert_eq!(latest, vec![(ConnectionState::Connected, connected.id.clone())], "the landed observation is the pair's latest held");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Run `body` on a two-worker runtime whose every thread exports its spans, under the cell's root
/// span, to the cell's own JSONL artifact, `$TMPDIR/i143-testkit/<cell>/` (`cell` is the test
/// function's name: the directory the gate keeps for a red cell, scripts/keep-gate-evidence.py),
/// each span written as it ends; `extra` (when given) sees every span too. A panic in `body` ends
/// the runtime and flushes the capture before it leaves the cell, so a red's spans are kept and
/// readable. Every finished span, in the order it ended, as written (name, ids, start and end,
/// attributes as text).
fn spans_of<F: std::future::Future<Output = ()>>(cell: &str, extra: Option<OpenSpans>, body: impl FnOnce() -> F) -> Vec<serde_json::Value> {
    use tracing_subscriber::layer::SubscriberExt;
    let dir = std::env::temp_dir().join("i143-testkit").join(cell);
    let exporter = rdm_transport::jsonl_span_exporter(&dir, "connections-writer").unwrap_or_else(|e| panic!("the span artifact dir {} is unusable: {e}", dir.display()));
    let (capture, layer) = rdm_transport::span_capture("connections-writer", exporter);
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer).with(extra));
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread().worker_threads(2).enable_all().on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d))).build().unwrap();
    let _g = tracing::dispatcher::set_default(&dispatch);
    let root = tracing::info_span!("rdm.test.connections_writer", cell = %cell);
    let trace_id = rdm_transport::traceparent_of(&root).and_then(|tp| tp.split('-').nth(1).map(str::to_string)).unwrap_or_default();
    let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| rt.block_on(tracing::Instrument::instrument(body(), root))));
    // The runtime ends first: every task it held lets its spans go, and they end into the capture.
    drop(rt);
    let flushed = capture.flush(std::time::Duration::from_secs(10));
    eprintln!("spans of this cell: {} trace_id={trace_id}", dir.display());
    let prefix = format!("connections-writer.{}-", std::process::id());
    let spans: Vec<serde_json::Value> = std::fs::read_dir(&dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with(&prefix) && n.ends_with(".spans.jsonl")))
                .flat_map(|p| std::fs::read_to_string(p).unwrap_or_default().lines().filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok()).collect::<Vec<_>>())
                .collect()
        })
        .unwrap_or_default();
    if let Err(panic) = ran {
        // The red's spans in its own output too (a run that removes its temp dir keeps them here):
        // one line each, in the order they ended.
        for s in &spans {
            eprintln!("span {} id={} parent={} start={} end={} {}", s["name"].as_str().unwrap_or_default(), s["span_id"].as_str().unwrap_or_default(), s["parent_span_id"].as_str().unwrap_or_default(), s["start_unix_nano"], s["end_unix_nano"], s["attributes"]);
        }
        std::panic::resume_unwind(panic);
    }
    flushed.unwrap_or_else(|e| panic!("the cell's spans did not all reach its artifact: {e}"));
    assert!(!spans.is_empty(), "the cell's artifact {} holds its spans", dir.display());
    crate::assert_own_story(spans.iter().filter_map(|s| s["name"].as_str()));
    println!("cell={cell} trace_id={trace_id}");
    spans
}

/// The text of attribute `k` on span `sp`; `None` when the span does not carry it.
fn attr(sp: &serde_json::Value, k: &str) -> Option<String> {
    sp["attributes"].get(k).and_then(|v| v.as_str()).map(str::to_string)
}

/// CONTRACT (connections.md §12: "Every durable connection transition retains its existing
/// connection update evidence and names at least: source destination kind state reason. Proxy
/// transitions additionally identify the carrier"): this node's writer lands each kind of
/// transition it owns, each on its span: an observed Proxy Connected (`via-observed`), the dial an
/// accept woke (`via-recovery-dial`), its Direct Connected (`via-observed`), the Proxy's
/// `Disconnected (direct-restored)` (`via-direct-restored`), a Direct Failed toward another
/// destination (`via-observed`) and a connections delete's drop (`via-connection-delete`). Every
/// transition span names source, destination, kind, state and reason; every Proxy transition its
/// carrier. Must NOT happen: a landed transition whose span leaves out any of them.
#[test]
fn every_landed_connection_transition_is_spanned_with_its_pair_kind_state_reason_and_carrier() {
    let (me, carrier, dest, other) = (end(rdm_core::PathName::new("mesh1.node", "1").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "2").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "3").unwrap(), "mesh1", "node"), end(rdm_core::PathName::new("mesh1.node", "4").unwrap(), "mesh1", "node"));
    let names = (me.node_id.to_string(), carrier.node_id.to_string(), dest.node_id.to_string(), other.node_id.to_string());
    let dir = data_dir("transition-spans");
    let spans = spans_of("every_landed_connection_transition_is_spanned_with_its_pair_kind_state_reason_and_carrier", None, || async {
        let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir).unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_core::FabricId::mint(), &me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap(), &rdm_transport::clock::RafkaTime::unadopted())).await.unwrap());
        let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
        let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
        // Seeded: the carrier's connection, and a Direct Failed toward the destination.
        let failed_at = now_ms() - 1;
        let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, failed_at);
        failed.reason = Some("seeded".into());
        writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, failed_at)).await.unwrap();
        writer.record(failed).await.unwrap();
        // Observed: the Proxy through the carrier.
        writer.record_data_plane(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), now_ms()));
        writer.observations_landed().await;
        assert_eq!(held.lock().unwrap().own_active_proxies().len(), 1, "the Proxy is held");
        // An accept from the destination wakes the source's dial (R-CN1), which connects; its Direct
        // Connected owes the Proxy's delete.
        let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
        dial.go.add_permits(1);
        writer.accepted(&resolved(&dest));
        let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(25), Some(dial.dial()));
        until("the attempt connects and the Proxy's delete lands", || held.lock().unwrap().own_active_proxies().is_empty()).await;
        writer.observations_landed().await;
        // A failed dial toward another destination, then the connection to the destination dropped.
        writer.direct_failed(&resolved(&other), "refused by the cell");
        writer.observations_landed().await;
        let open = writer.held_routes().await.into_iter().filter(|r| r.destination.location() == dest.location()).collect::<Vec<_>>();
        assert_eq!(open.len(), 1, "the Direct to the destination is held: {open:?}");
        writer.write_drops(&open, "node.connection.delete");
        writer.observations_landed().await;
    });
    let _ = std::fs::remove_dir_all(&dir);
    let named = |n: &str| -> Vec<&serde_json::Value> { spans.iter().filter(|s| s["name"] == n).collect() };
    let transitions: Vec<&serde_json::Value> = ["rdm.node_admin.connection.update.via-observed", "rdm.node_admin.connection.delete.via-direct-restored", "rdm.node_admin.connection.update.via-connection-delete"].into_iter().flat_map(named).collect();
    let kinds: Vec<(String, String, Option<String>)> = transitions.iter().map(|sp| (sp["name"].as_str().unwrap().rsplit('.').next().unwrap().to_string(), format!("{}->{}", attr(sp, "source_id").unwrap_or_default(), attr(sp, "destination_id").unwrap_or_default()), attr(sp, "state"))).collect();
    assert_eq!(transitions.len(), 5, "five transitions landed (Proxy Connected, Direct Connected, Proxy Disconnected, Direct Failed, the drop): {kinds:?}");
    for sp in &transitions {
        for k in ["source_id", "destination_id", "kind", "state", "reason"] {
            assert!(attr(sp, k).is_some(), "a transition span names its {k}: {sp}");
        }
        assert_eq!(attr(sp, "source_id").as_deref(), Some(names.0.as_str()), "{sp}");
        assert!(!attr(sp, "kind").unwrap().is_empty() && !attr(sp, "state").unwrap().is_empty(), "{sp}");
        if attr(sp, "kind").as_deref() == Some("proxy") {
            assert_eq!(attr(sp, "carrier").as_deref(), Some(names.1.as_str()), "a Proxy transition names its carrier: {sp}");
        }
        if attr(sp, "state").as_deref() != Some("connected") {
            assert!(!attr(sp, "reason").unwrap().is_empty(), "a Failed or Disconnected transition names its reason: {sp}");
        }
    }
    let delete = named("rdm.node_admin.connection.delete.via-direct-restored");
    assert_eq!(delete.iter().map(|sp| (attr(sp, "kind"), attr(sp, "state"), attr(sp, "reason"), attr(sp, "outcome"))).collect::<Vec<_>>(), vec![(Some("proxy".into()), Some("disconnected".into()), Some("direct-restored".into()), Some("landed".into()))], "{delete:?}");
    let drop = named("rdm.node_admin.connection.update.via-connection-delete");
    assert_eq!(drop.iter().map(|sp| (attr(sp, "destination_id"), attr(sp, "kind"), attr(sp, "state"))).collect::<Vec<_>>(), vec![(Some(names.2.clone()), Some("direct".into()), Some("disconnected".into()))], "{drop:?}");
    let failed: Vec<&&serde_json::Value> = transitions.iter().filter(|sp| attr(sp, "state").as_deref() == Some("failed")).collect();
    assert_eq!(failed.iter().map(|sp| attr(sp, "destination_id")).collect::<Vec<_>>(), vec![Some(names.3.clone())], "one Direct Failed, toward the other destination: {failed:?}");
    let dials = named("rdm.node_admin.connection.create.via-recovery-dial");
    assert_eq!(dials.iter().map(|sp| (attr(sp, "destination_id"), attr(sp, "fact"))).collect::<Vec<_>>(), vec![(Some(names.2.clone()), Some("connected".into()))], "the woken dial names its destination and its fact: {dials:?}");
}

/// CONTRACT (connections.md §12: "The reconnect attempt span is owned by node-base and records
/// whether it: connected / failed / was superseded before dial / was already satisfied by Direct
/// Connected"; §9, R-CN1): two dials an accept woke that are never made. One's destination birth
/// leaves the topology view before the reconciler runs: its dial ends on `via-recovery-attempt` with
/// reason `destination-superseded`. The other's destination connects Direct by ordinary traffic
/// (`direct_connected`, not the reconciler's dial) before the reconciler runs: its dial ends on
/// `via-recovery-attempt` with reason `direct-connected`. Must NOT happen: a dial span for either,
/// or a pending attempt that ends with no span saying why.
#[test]
fn a_pending_reconnect_attempt_never_dialled_ends_on_a_span_naming_its_superseded_birth_or_the_direct_connected_that_satisfied_it() {
    let dirs = std::cell::RefCell::new(Vec::new());
    let dests = std::cell::RefCell::new(Vec::new());
    let spans = spans_of("a_pending_reconnect_attempt_never_dialled_ends_on_a_span_naming_its_superseded_birth_or_the_direct_connected_that_satisfied_it", None, || async {
        // The superseded birth.
        let (dir, _me, dest, _storage, held, writer, _) = proxied("pending-superseded", now_ms()).await;
        let topology_view = Arc::new(rdm_core::connections::StaticIncarnations::default());
        topology_view.set(dest.node_id.clone(), dest.location(), dest.incarnation.clone().unwrap());
        held.lock().unwrap().set_topology_view(topology_view.clone());
        let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
        writer.accepted(&resolved(&dest));
        assert_eq!(writer.recovery_pending().len(), 1, "one dial woken");
        topology_view.depart(dest.node_id.clone(), dest.location());
        let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(25), Some(dial.dial()));
        until("the attempt for the superseded birth ends", || writer.recovery_pending().is_empty()).await;
        assert!(dial.calls().is_empty(), "no dial toward a superseded birth");
        dirs.borrow_mut().push(dir);
        dests.borrow_mut().push(dest.node_id.to_string());

        // The birth whose Direct connects by ordinary traffic.
        let (dir, _me, dest, _storage, _held, writer, _) = proxied("pending-satisfied", now_ms()).await;
        let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
        writer.accepted(&resolved(&dest));
        assert_eq!(writer.recovery_pending().len(), 1, "one dial woken");
        writer.direct_connected(&resolved(&dest));
        writer.observations_landed().await;
        let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(25), Some(dial.dial()));
        until("the attempt the Direct Connected satisfied ends", || writer.recovery_pending().is_empty()).await;
        writer.observations_landed().await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(dial.calls().is_empty(), "no dial once the Direct is connected");
        dirs.borrow_mut().push(dir);
        dests.borrow_mut().push(dest.node_id.to_string());
    });
    for dir in dirs.into_inner() {
        let _ = std::fs::remove_dir_all(dir);
    }
    assert!(spans.iter().all(|s| s["name"] != "rdm.node_admin.connection.create.via-recovery-dial"), "nothing was dialled");
    let ended: Vec<(Option<String>, Option<String>)> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.connection.remove.via-recovery-attempt").map(|sp| (attr(sp, "destination_id"), attr(sp, "reason"))).collect();
    let d = dests.into_inner();
    assert_eq!(ended, vec![(Some(d[0].clone()), Some("destination-superseded".into())), (Some(d[1].clone()), Some("direct-connected".into()))], "each pending attempt ends on one span naming why");
}
