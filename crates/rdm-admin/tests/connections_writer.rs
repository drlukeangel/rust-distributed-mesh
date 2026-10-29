//! i143.e6.s11 functional: the source-owned connections writer. A node's own Direct and Proxy
//! facts are rows it writes (the raw log grows, the index keeps one entry per pair), applied to
//! the held projection the route resolver reads. A birth holds no connection and reads none back
//! (Q227): a restart holds no Proxy (connections.md §13 B) and starts its reconnect series fresh
//! (§13 C), and the index stays bounded (§13 D).

use rdm_core::connections::{resolve, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rdm_core::reconnect::reconnect_plan;
use rdm_core::{IncarnationId, NodeId, NodeKind, PathName};
use rdm_admin::connections_writer::{ConnectionsWriter, RecoveryDial};
use rdm_admin::storage::{ConnectionsStorage, FileConnectionsStorage};
use rdm_transport::{AcceptTrigger, ConnectionObserver, ResolvedNode};
use std::sync::{Arc, Mutex};

const POLICY: CarrierPolicy = CarrierPolicy::Proxy { carrier_kind: NodeKind::RpcNode };

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
    NodeConnection { id: rdm_core::ConnectionId::mint(), source: source.clone(), destination: destination.clone(), kind, alpn: rdm_core::connections::ALPN.to_vec(), state, carrier: carrier.cloned(), recovery: None, reason: None, logged_at_ms: at }
}

fn data_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("i143-e6s11-{tag}-{}", NodeId::mint()))
}

/// §13 B (Q227): the rows a previous birth wrote stay in the store as written, and a fresh writer over
/// the same storage holds none of them: the first resolution after the restart dials the destination
/// directly, and no reconnect series stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_holds_none_of_the_previous_births_rows_and_the_first_resolution_dials() {
    let dir = data_dir("restart-holds-none");
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let open = || async { FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&me.node_id, &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap() };
    {
        let storage = open().await;
        let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, 10);
        failed.recovery = Some(rdm_core::connections::DirectRecovery { recovery_round: 1, attempt_ordinal: 2 });
        for r in [failed, row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 11), row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 12)] {
            storage.append_history(&r).await.unwrap();
            storage.put_connection(&r).await.unwrap();
        }
    }
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(open().await);
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let _writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    let r = resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY);
    assert_eq!(r.route, EffectiveRoute::Direct { known: false }, "the restarted birth holds no Proxy and dials");
    assert!(reconnect_plan(&held.lock().unwrap(), |_| None).is_empty(), "no reconnect series stands");
    assert_eq!(storage.history().await.unwrap().len(), 3, "the store keeps the previous birth's rows as written");
    let _ = std::fs::remove_dir_all(&dir);
}

/// §13 D: many failed attempts for one pair leave the raw log growing while the index holds one
/// Direct member for the pair; the ordinal counts up in one round.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_failures_grow_the_raw_log_and_keep_the_index_at_one_direct_per_pair() {
    let dir = data_dir("bounded");
    let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
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
    assert_eq!(index[0].recovery.as_ref().map(|r| (r.recovery_round, r.attempt_ordinal)), Some((1, 7)), "the ordinal counts up in one round");
    assert_eq!(held.lock().unwrap().own_latest_directs().len(), 1);
    // A Direct Connected ends the series and is the pair's one Direct member now.
    writer.direct_connected(&target);
    writer.observations_landed().await;
    let index = storage.connections().await.unwrap();
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].state, ConnectionState::Connected);
    assert!(reconnect_plan(&held.lock().unwrap(), |_| None).is_empty(), "nothing is owed after a connect");
    let _ = std::fs::remove_dir_all(&dir);
}

/// §10: a Direct Connected beside an active Proxy owes the Proxy's retirement, written durably
/// as Disconnected(direct-restored); the route cuts back once it lands (the refused-write cell
/// below proves the Proxy stays effective until then).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_direct_connected_beside_a_proxy_disconnects_it_durably_and_the_route_cuts_back() {
    let dir = data_dir("retire");
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 11)).await.unwrap();
    writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 12)).await.unwrap();
    writer.direct_connected(&resolved(&dest));
    // The observation's own write has landed and settled the retirement it owed.
    writer.observations_landed().await;
    let retired = storage.history().await.unwrap().into_iter().last().unwrap();
    assert_eq!((retired.kind, retired.state, retired.reason.as_deref()), (ConnectionKind::Proxy, ConnectionState::Disconnected, Some("direct-restored")));
    assert_eq!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true });
    assert_eq!(writer.disconnect_owed().await.unwrap(), 0, "nothing owed once the retirement landed");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The data-plane dial a cell hands the reconciler (connections.md §9.5 step 3): every call is
/// recorded with its local instant, waits for the cell to let it go, then reports its outcome to
/// the writer as the data-plane client's observer would (§9.5 step 5: the reconnect module invents
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

/// A source holding a Proxy to `dest` through `carrier`, its latest Direct to `dest` Failed
/// (round 1, ordinal 1) at `failed_at`: what a proven carried path leaves (connections.md §7).
async fn proxied(tag: &str, failed_at: u64) -> (std::path::PathBuf, ConnectionEnd, ConnectionEnd, Arc<dyn ConnectionsStorage>, Arc<Mutex<ConnectionsHeld>>, ConnectionsWriter, NodeConnection) {
    let dir = data_dir(tag);
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &me.name, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, failed_at);
    failed.recovery = Some(rdm_core::connections::DirectRecovery { recovery_round: 1, attempt_ordinal: 1 });
    writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, failed_at)).await.unwrap();
    writer.record(failed.clone()).await.unwrap();
    writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), failed_at + 1)).await.unwrap();
    (dir, me, dest, storage, held, writer, failed)
}

/// CONTRACT (connections.md §10 and §13 G, R-CN1): a connection this source accepts from the
/// exact destination birth it reaches through its Proxy is a recovery trigger, never Direct
/// evidence. The accept writes nothing; it wakes the source's reconciler, which makes one dial of
/// the source's own; only that dial's Direct Connected owes the Proxy's retirement, and the route
/// cuts back once the retirement lands. What must NOT happen: a Direct row written for the accept,
/// or the Proxy retired before the source's own dial connected.
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
    assert!(matches!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::ViaPeer { .. }), "the Proxy stays the route while the dial is out");
    assert_eq!(dial.calls()[0].0, dest, "the dial is toward the exact birth the Proxy names");
    dial.go.add_permits(1);
    until("the route cuts back", || matches!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true })).await;
    let history = storage.history().await.unwrap();
    let after: Vec<(ConnectionKind, ConnectionState, Option<String>)> = history[rows_before..].iter().map(|r| (r.kind, r.state, r.reason.clone())).collect();
    assert_eq!(
        after,
        vec![(ConnectionKind::Direct, ConnectionState::Connected, None), (ConnectionKind::Proxy, ConnectionState::Disconnected, Some("direct-restored".to_string()))],
        "the source's own Direct Connected, then the retirement it owed"
    );
    until("the attempt is spent once Direct is Connected", || writer.recovery_pending().is_empty()).await;
    assert_eq!(dial.calls().len(), 1, "one dial");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (connections.md §9.3, §9.4, R-CN1 (2)): accepts repeated from the destination coalesce
/// into one pending attempt per (destination, destination incarnation), and the attempt waits for
/// the series' reconnect backoff from its latest Failed row: an accept never brings a dial earlier
/// than `next_due`. What must NOT happen: one dial per accept, or a dial before the backoff ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn repeated_accepts_coalesce_into_one_attempt_that_waits_for_the_series_backoff() {
    // The Failed row is written so that its backoff ends in about a second.
    let failed_at = now_ms() - rdm_core::reconnect::reconnect_backoff(1) + 300;
    let (dir, me, dest, _storage, held, writer, failed) = proxied("accept-coalesce", failed_at).await;
    let due = rdm_core::reconnect::next_due(&failed).unwrap();
    let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
    let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(50), Some(dial.dial()));
    for _ in 0..5 {
        writer.accepted(&resolved(&dest));
    }
    let pending = writer.recovery_pending();
    assert_eq!(pending.len(), 1, "five accepts, one pending attempt: {pending:?}");
    assert_eq!((pending[0].destination.clone(), pending[0].due_ms), (dest.clone(), due), "due when the series' backoff ends");
    until("the one attempt is made", || dial.calls().len() == 1).await;
    assert!(dial.calls()[0].1 >= due, "the dial waited for the backoff: dialled at {} ms, due at {due} ms", dial.calls()[0].1);
    dial.go.add_permits(1);
    until("the route cuts back", || matches!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true })).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(dial.calls().len(), 1, "one dial for five accepts");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (connections.md §9.3, §9.5 step 4, R-CN1 (5)): when the dial an accept woke fails, the
/// Proxy stays the route, the failure is the series' next Failed row (same round, next ordinal),
/// and the pending attempt is rescheduled from that row's backoff. What must NOT happen: the Proxy
/// retired, a second dial before the new backoff ends, or the attempt forgotten.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_dial_after_an_accept_keeps_the_proxy_and_reschedules_from_its_own_failed_row() {
    let (dir, me, dest, storage, held, writer, _) = proxied("accept-failed-dial", now_ms() - 60_000).await;
    let dial = CellDial::new(&writer, resolved(&dest), vec![Err("cell: the source's dial ended NotSent".to_string())]);
    let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(50), Some(dial.dial()));
    writer.accepted(&resolved(&dest));
    until("the source dials its own connection", || dial.calls().len() == 1).await;
    dial.go.add_permits(1);
    until("the failed dial's row is held", || held.lock().unwrap().own_latest_directs().iter().any(|d| d.recovery.map(|r| r.attempt_ordinal) == Some(2))).await;
    let latest = storage.history().await.unwrap().into_iter().last().unwrap();
    assert_eq!((latest.kind, latest.state, latest.recovery.map(|r| (r.recovery_round, r.attempt_ordinal))), (ConnectionKind::Direct, ConnectionState::Failed, Some((1, 2))), "the series' next Failed row: {latest:?}");
    assert!(matches!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::ViaPeer { .. }), "the Proxy stays the route");
    assert!(writer.owed().is_empty(), "no retirement is owed");
    let due = rdm_core::reconnect::next_due(&latest).unwrap();
    until("the attempt is rescheduled from the new row", || writer.recovery_pending().iter().any(|p| p.destination == dest && p.due_ms == due)).await;
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    assert_eq!(dial.calls().len(), 1, "no second dial before the new backoff ends");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (connections.md §9.1, §9.5 step 1, R-CN1 (1)): only an accept from the exact birth the
/// Proxy names wakes an attempt, and an attempt is never made against a birth that is gone. An
/// accept from the destination's next birth (same path.name, new node and incarnation) arms
/// nothing; a pending attempt whose birth the topology view no longer holds is dropped undialled. What
/// must NOT happen: a dial toward the gone birth, or an attempt armed by a birth the Proxy does
/// not name.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_accept_from_another_birth_arms_nothing_and_an_attempt_for_a_gone_birth_is_dropped_undialled() {
    let (dir, _me, dest, _storage, held, writer, _) = proxied("accept-stale-birth", now_ms()).await;
    let topology_view = Arc::new(rdm_core::connections::StaticIncarnations::default());
    topology_view.set(dest.node_id.clone(), dest.name.clone(), dest.incarnation.clone().unwrap());
    held.lock().unwrap().set_topology_view(topology_view.clone());
    let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
    let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(50), Some(dial.dial()));
    let next = end("mesh1.rpc.3");
    writer.accepted(&resolved(&next));
    assert_eq!(writer.recovery_pending().into_iter().map(|p| p.destination).collect::<Vec<_>>(), vec![dest.clone()], "the next birth is not the birth the Proxy names; only the Failed row's birth is pending");
    // The Proxy's own birth coalesces into its pending attempt (due when its backoff ends, seconds away); then it is gone.
    writer.accepted(&resolved(&dest));
    assert_eq!(writer.recovery_pending().len(), 1);
    topology_view.depart(dest.node_id.clone(), dest.name.clone());
    until("the attempt for the gone birth is dropped", || writer.recovery_pending().is_empty()).await;
    assert!(dial.calls().is_empty(), "no dial toward a birth that is gone");
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

/// §10: a Direct Connected beside an active Proxy owes the Proxy's retirement; a refused
/// retirement write leaves the Proxy effective and the obligation standing, derived again from
/// the rows; once the write lands, new calls cut back to Direct. The bounded reconciler lands it
/// by itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_disconnected_write_keeps_the_proxy_effective_until_it_lands() {
    let dir = data_dir("refused-retirement");
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage = Arc::new(RefusingIndex { inner: Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap()), refuse: std::sync::atomic::AtomicU32::new(0) });
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
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
    let _task = writer.spawn_reconciler(std::time::Duration::from_millis(50), None);
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
    assert_eq!(retired[0].reason.as_deref(), Some(rdm_core::reconnect::DIRECT_RESTORED));
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
    use rdm_transport::CarrierEdges;
    let dir = data_dir("edges");
    let (me, dest) = (end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
    let writer = ConnectionsWriter::new(me.clone(), storage, Arc::new(Mutex::new(ConnectionsHeld::new())));
    assert_eq!(writer.edge_not_active(&dest.node_id).await, None, "no fact is not an edge fact");
    writer.record(row(&me, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 10)).await.unwrap();
    assert_eq!(writer.edge_not_active(&dest.node_id).await, None, "a Connected edge is Active");
    let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, 20);
    failed.recovery = Some(rdm_core::connections::DirectRecovery { recovery_round: 1, attempt_ordinal: 1 });
    failed.reason = Some("dial failed".into());
    writer.record(failed).await.unwrap();
    let why = writer.edge_not_active(&dest.node_id).await.expect("a Failed edge is not Active");
    assert_eq!(why, "mesh1.rpc.2 -> mesh1.rpc.3 Direct failed (dial failed)");
    assert_eq!(writer.edge_not_active(&NodeId::mint()).await, None, "another node's edge is not named");
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

/// CONTRACT (#2803 detection; Q169): the carrier names its edge from the dial that just failed,
/// from the dial's own outcome, however long the fact's write takes, so a member that cannot reach
/// its own node-admin says `carrier-edge-lost` every time, never "not sent" for want of a write. The
/// read of its held edges never waits for a write: until the Failed row lands it holds no fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_carrier_names_the_edge_a_dial_has_just_failed_however_slow_the_write() {
    use rdm_transport::CarrierEdges;
    let dir = data_dir("edges-slow");
    let (me, dest) = (end("mesh2.rpc.1"), end("mesh2.admin.1"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(SlowIndex(Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap())));
    let writer = ConnectionsWriter::new(me.clone(), storage, Arc::new(Mutex::new(ConnectionsHeld::new())));
    writer.direct_failed(&resolved(&dest), "timed out");
    let why = writer.edge_after_dial(&resolved(&dest), "timed out").await.expect("the failed dial's fact is named");
    assert_eq!(why, "mesh2.rpc.1 -> mesh2.admin.1 Direct failed (timed out)");
    let held_now = tokio::time::timeout(std::time::Duration::from_millis(100), writer.edge_not_active(&dest.node_id)).await.expect("the read answers from what is held, never waiting for the write");
    assert_eq!(held_now, None, "the Failed row has not landed, so no fact is held yet");
    writer.observations_landed().await;
    assert_eq!(writer.edge_not_active(&dest.node_id).await.as_deref(), Some("mesh2.rpc.1 -> mesh2.admin.1 Direct failed (timed out)"), "once landed the fact is held");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (Q169): a fact is held only after its durable write lands. A refused write leaves no
/// fact in the held projection, the route still reads "never observed", and the write stays owed:
/// it is retried with backoff and lands without a second observation.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_observation_write_is_held_only_once_its_retry_lands() {
    let dir = data_dir("refused-observation");
    let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let storage = Arc::new(RefusingIndex { inner: Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap()), refuse: std::sync::atomic::AtomicU32::new(0) });
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
        assert!(!h.holds_direct_fact(&me.name, &dest.name), "a refused fact is not held");
        assert!(h.own_latest_directs().is_empty());
        assert_eq!(resolve(&h, &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: false });
    }
    assert!(storage.connections().await.unwrap().is_empty(), "nothing durable in the index");
    assert_eq!(writer.in_flight(), 1, "the write stays owed");
    // The retry lands it, with no second observation.
    writer.observations_landed().await;
    assert_eq!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true });
    let index = storage.connections().await.unwrap();
    assert_eq!((index.len(), index[0].state), (1, ConnectionState::Connected));
    assert_eq!(storage.history().await.unwrap().len(), 1, "the raw log took the row once: only the refused index write was attempted again");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT: a node that has never connected to a peer calls it directly; when the peer is dead
/// the call ends `NotSent` and the failed dial is recorded as this node's Direct Failed fact.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dial_to_a_dead_peer_with_no_prior_fact_is_not_sent_and_records_direct_failed() {
    use rdm_transport::{Budget, CallOptions, NodeRpcClient, StaticResolver};
    use rdm_core::outcome::RpcOutcome;
    use rdm_core::ping::{Ping, PingRequest};
    let dir = data_dir("dead-cold");
    let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = Arc::new(ConnectionsWriter::new(me.clone(), storage.clone(), held.clone()));
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(resolved(&dest));
    let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm").with_connection_observer(writer.clone());
    let opts = CallOptions { budget: Budget::Overall(std::time::Duration::from_millis(300)), ..Default::default() };
    let resolution = resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY);
    let call = client.call_resolved::<Ping>(resolution, &me.name, &dest.name, &dest.node_id, &PingRequest::Ping { payload: vec![1] }, &opts).await;
    assert_eq!(call.route, EffectiveRoute::Direct { known: false });
    assert!(matches!(call.outcome, RpcOutcome::NotSent(_)), "{:?}", call.outcome);
    writer.observations_landed().await;
    let index = storage.connections().await.unwrap();
    assert_eq!(index.len(), 1, "{index:?}");
    assert_eq!((index[0].kind, index[0].state), (ConnectionKind::Direct, ConnectionState::Failed));
    assert_eq!(index[0].recovery.map(|r| r.attempt_ordinal), Some(1));
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
/// every observation has landed, the index member of the pair is the latest ordinal even when the first
/// observation's index write is the slowest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_earlier_index_write_never_overwrites_a_later_observation() {
    let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
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
    assert_eq!(index[0].recovery.as_ref().map(|r| (r.recovery_round, r.attempt_ordinal)), Some((1, 3)), "the index holds the latest observation: {index:?}");
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
    let (me, peer) = (end("mesh1.rpc.7"), end("mesh1.rpc.2"));
    let taken: PathName = "mesh1.rpc.1".parse().unwrap();
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &me.name, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    // The process's held name, as its topology view holds it: none told yet, so the birth's.
    let told: Arc<Mutex<Option<PathName>>> = Arc::default();
    let (cell, birth) = (told.clone(), me.name.clone());
    writer.follow_held_name(move || cell.lock().unwrap().clone().unwrap_or_else(|| birth.clone())).unwrap();
    assert!(writer.follow_held_name(|| "mesh1.rpc.9".parse().unwrap()).is_err_and(|e| e.contains("mesh1.rpc.7") && e.contains("already reads its name")), "one place holds the name");
    let target = resolved(&peer);
    writer.direct_connected(&target);
    writer.observations_landed().await;
    let before = storage.history().await.unwrap();
    assert_eq!(before.len(), 1);
    assert_eq!((&before[0].source.name, &before[0].source.node_id), (&me.name, &me.node_id), "the row before the rename names the birth's name: {:?}", before[0]);

    *told.lock().unwrap() = Some(taken.clone());
    assert_eq!(writer.own(), ConnectionEnd { name: taken.clone(), ..me.clone() }, "the source end is the name the node holds, with its node id and birth");
    writer.direct_broken(&target, "the pooled connection closed");
    writer.observations_landed().await;
    let after = storage.history().await.unwrap();
    assert_eq!(after.len(), 2);
    assert_eq!(after[0], before[0], "the row written before the rename stays exactly as written");
    assert_eq!((&after[1].source, &after[1].destination.name, after[1].state), (&ConnectionEnd { name: taken.clone(), ..me.clone() }, &peer.name, ConnectionState::Disconnected), "{:?}", after[1]);
    let own: Vec<NodeConnection> = held.lock().unwrap().own_latest_directs().into_iter().cloned().collect();
    assert_eq!(own, vec![after[1].clone()], "the row under the held name is this node's own Direct, the birth name's is not");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (node-identity-and-state.md §8: `source_node_id` is the instance at each end when the
/// entry was written; connections.md §13 C, Q227: a restart's backoff starts fresh): a restarted birth
/// of `mesh1.rpc.1` (same node id, next incarnation) reads none of the previous birth's failing Direct
/// back, and its first failure toward the destination is a new series, round 1 ordinal 1, naming its
/// OWN birth as the source. What must NOT happen: the row continuing the previous birth's series.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restarted_birth_starts_its_own_series_naming_its_own_birth() {
    let dir = data_dir("restart-series");
    let (previous, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let me = ConnectionEnd { incarnation: Some(IncarnationId::mint()), ..previous.clone() };
    let open = || async { FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap() };
    {
        let storage = open().await;
        let mut failed = row(&previous, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, 10);
        failed.recovery = Some(rdm_core::connections::DirectRecovery { recovery_round: 4, attempt_ordinal: 2 });
        storage.append_history(&failed).await.unwrap();
        storage.put_connection(&failed).await.unwrap();
    }
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(open().await);
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), Arc::new(Mutex::new(ConnectionsHeld::new())));
    writer.direct_failed(&resolved(&dest), "refused after the restart");
    writer.observations_landed().await;
    let history = storage.history().await.unwrap();
    let next = history.last().unwrap();
    assert_eq!(next.recovery.map(|r| (r.recovery_round, r.attempt_ordinal)), Some((1, 1)), "the restarted birth starts its own series: {next:?}");
    assert_eq!(next.source, me, "the row names the birth that wrote it: {next:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (node-identity-and-state.md §8: `destination_node_id` is the instance at that end when the
/// entry was written; connections.md §9.1 and §9.4: a reconnect series is for the current destination
/// incarnation): a failing series against `mesh1.rpc.3`'s birth, then a failed dial to its next
/// birth (same node id, next incarnation), is a new incident: a new round at ordinal one, naming the
/// birth that was dialled. What must NOT happen: the series of the gone birth continued one ordinal
/// up, or a row naming the gone birth as the destination of a dial to its successor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_dial_to_the_destinations_next_birth_starts_a_new_series_naming_that_birth() {
    let dir = data_dir("next-birth-series");
    let (me, gone) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let next = ConnectionEnd { incarnation: Some(IncarnationId::mint()), ..gone.clone() };
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &me.name, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), Arc::new(Mutex::new(ConnectionsHeld::new())));
    for i in 0..2 {
        writer.direct_failed(&resolved(&gone), &format!("refused {i}"));
        writer.observations_landed().await;
    }
    writer.direct_failed(&resolved(&next), "refused by the next birth");
    writer.observations_landed().await;
    let history = storage.history().await.unwrap();
    assert_eq!(history.iter().map(|r| r.recovery.map(|r| (r.recovery_round, r.attempt_ordinal))).collect::<Vec<_>>(), vec![Some((1, 1)), Some((1, 2)), Some((2, 1))], "the next birth's failure is a new incident: {history:#?}");
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
        let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
        let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &me.name, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
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
/// before; connections.md §9.1: a newer Direct Connected supersedes a series): a fact this writer
/// observes after a row it recorded for the same pair is the pair's latest, whatever the clock reads
/// when the observation is stamped: a Direct Connected observed after a recorded Direct Failed
/// stamped at the clock's own millisecond, or later, is held, and the series it satisfied ends.
/// What must NOT happen: the Connected row landing in the store while the held projection keeps the
/// Failed row as the pair's latest (one millisecond, and a Failed row wins a tie), so the series
/// stands and the reconciler dials a destination already connected.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_observation_after_a_recorded_row_of_its_pair_is_held_as_the_latest_whatever_the_clock_reads() {
    let dir = data_dir("recorded-then-observed");
    let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &me.name, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    // Recorded at a millisecond the clock has not passed when the observation is stamped.
    let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, now_ms() + 1_000);
    failed.recovery = Some(rdm_core::connections::DirectRecovery { recovery_round: 1, attempt_ordinal: 1 });
    writer.record(failed.clone()).await.unwrap();
    assert_eq!(writer.recovery_pending().len(), 1, "the recorded Failed row armed its series");
    writer.direct_connected(&resolved(&dest));
    writer.observations_landed().await;
    let connected = storage.history().await.unwrap().into_iter().last().unwrap();
    assert_eq!((connected.kind, connected.state), (ConnectionKind::Direct, ConnectionState::Connected), "the observation landed: {connected:?}");
    assert!(connected.logged_at_ms > failed.logged_at_ms, "the observation is stamped after the row recorded before it: {} after {}", connected.logged_at_ms, failed.logged_at_ms);
    let latest: Vec<(ConnectionState, rdm_core::ConnectionId)> = held.lock().unwrap().own_latest_directs().into_iter().map(|d| (d.state, d.id.clone())).collect();
    assert_eq!(latest, vec![(ConnectionState::Connected, connected.id.clone())], "the landed observation is the pair's latest held");
    assert!(writer.recovery_pending().is_empty(), "the series the Direct Connected satisfied ended");
    let _ = std::fs::remove_dir_all(&dir);
}

/// CONTRACT (connections.md §9.4: one pending item per (destination, destination incarnation,
/// alpn), and a reconnect on one pair never touches another's; §9.5, Q169: the reconciler decides
/// from its dial's own result, in memory; §12: each attempt ends on one span): two destinations
/// due at once, each dialled and each Reply spending its own attempt while both Direct Connected
/// rows are still owed; the rows then land one after the other. Each attempt ends on exactly one
/// `via-recovery-attempt` span (`direct-connected`), and each destination is dialled once. What
/// must NOT happen: the first destination's row landing arming the second destination's attempt
/// again from its Failed row (its own Connected row not landed yet), so that attempt ends twice
/// and, due at once, may be dialled again.
#[test]
fn an_attempt_its_dials_reply_spent_is_not_armed_again_by_another_destinations_row_landing() {
    let calls: Arc<Mutex<Vec<String>>> = Arc::default();
    let dests = std::cell::RefCell::new(Vec::new());
    let spans = spans_of("an_attempt_its_dials_reply_spent_is_not_armed_again_by_another_destinations_row_landing", None, || async {
        let dir = data_dir("spent-not-armed-again");
        let (me, a, b) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"), end("mesh1.rpc.4"));
        let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &me.name, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
        let writer = ConnectionsWriter::new(me.clone(), storage.clone(), Arc::new(Mutex::new(ConnectionsHeld::new())));
        // Both series failed a minute ago: both attempts are due at once.
        for dest in [&a, &b] {
            let mut failed = row(&me, dest, ConnectionKind::Direct, ConnectionState::Failed, None, now_ms() - 60_000);
            failed.recovery = Some(rdm_core::connections::DirectRecovery { recovery_round: 1, attempt_ordinal: 1 });
            writer.record(failed).await.unwrap();
        }
        assert_eq!(writer.recovery_pending().len(), 2, "one attempt per destination");
        // Every row waits while the dials are made: both Direct Connected rows are owed when both Replies are in.
        let rows = writer.hold_rows().await;
        let (w, seen) = (writer.clone(), calls.clone());
        let ends = [a.clone(), b.clone()];
        let dial: RecoveryDial = Arc::new(move |destination: ConnectionEnd| {
            let (w, seen) = (w.clone(), seen.clone());
            let target = ends.iter().find(|e| **e == destination).map(resolved);
            Box::pin(async move {
                seen.lock().unwrap().push(destination.name.to_string());
                w.direct_connected(&target.expect("a destination the cell seeded"));
                "Reply".to_string()
            })
        });
        let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(25), Some(dial));
        until("both destinations are dialled and both attempts spent", || calls.lock().unwrap().len() == 2 && writer.recovery_pending().is_empty()).await;
        assert_eq!(writer.in_flight(), 2, "both Direct Connected rows are owed");
        drop(rows);
        writer.observations_landed().await;
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(writer.recovery_pending().is_empty(), "no attempt stands once both rows have landed: {:?}", writer.recovery_pending());
        dests.borrow_mut().extend([a.name.to_string(), b.name.to_string()]);
        let _ = std::fs::remove_dir_all(&dir);
    });
    let mut dialled = calls.lock().unwrap().clone();
    dialled.sort();
    let dests = dests.into_inner();
    assert_eq!(dialled, dests, "each destination dialled once");
    for dest in &dests {
        let ended: Vec<Option<String>> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.connection.remove.via-recovery-attempt" && attr(s, "destination").as_deref() == Some(dest.as_str())).map(|s| attr(s, "reason")).collect();
        let story: Vec<String> = spans
            .iter()
            .filter(|s| ["rdm.node_admin.connection.remove.via-recovery-attempt", "rdm.node_admin.connection.update.via-observed", "rdm.node_admin.connection.create.via-recovery-dial"].contains(&s["name"].as_str().unwrap_or_default()))
            .map(|s| format!("{} start={} end={} destination={:?} reason={:?} state={:?}", s["name"].as_str().unwrap_or_default(), s["start_unix_nano"], s["end_unix_nano"], attr(s, "destination"), attr(s, "reason"), attr(s, "state")))
            .collect();
        assert_eq!(ended, vec![Some("direct-connected".to_string())], "the attempt toward {dest} ends on one span:\n{}", story.join("\n"));
    }
}

/// CONTRACT (connections.md §9.3, §9.4, §9.5, Q7 ruled Luke 2026-10-13): one reconciler, two
/// arming paths, one pending item per (destination, destination_incarnation). A latest Direct
/// Failed row arms the reconciler on its own without any inbound accept trigger. When `due_ms` is
/// reached, the reconciler makes one direct dial toward that exact destination birth: on failure it
/// appends the next Direct Failed row (same round, ordinal + 1) and reschedules from that durable
/// row's `next_due`; an inbound accept arriving while the item is pending coalesces into it
/// (`outcome = "coalesced"`), never creating a second attempt and never skipping the backoff the
/// durable rows set; when the rescheduled `due_ms` arrives, the reconciler dials again, and on
/// success its Direct Connected retires the Proxy (`Proxy Disconnected(direct-restored)`), cuts
/// the route back to Direct, and spends the pending item.
#[test]
fn a_failed_row_arms_the_due_time_reconciler_without_an_accept_reschedules_on_failure_and_coalesces_with_accepts() {
    let open_dials = OpenSpans::named("rdm.node_admin.connection.create.via-recovery-dial", std::time::Duration::ZERO);
    let spans = spans_of("a_failed_row_arms_the_due_time_reconciler_without_an_accept_reschedules_on_failure_and_coalesces_with_accepts", Some(open_dials.clone()), || async {
        let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
        let jitter1 = rdm_core::reconnect::deterministic_jitter(&me.name, &dest.name, 1);
        let failed_at = now_ms() - rdm_core::reconnect::reconnect_backoff(1) - jitter1 + 250;
        let dir = data_dir("due-time-reconnect");
        let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &me.name, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
        let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
        let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
        let mut failed1 = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, failed_at);
        failed1.recovery = Some(rdm_core::connections::DirectRecovery { recovery_round: 1, attempt_ordinal: 1 });
        writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, failed_at)).await.unwrap();
        writer.record(failed1.clone()).await.unwrap();
        writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), failed_at + 1)).await.unwrap();
        let due1 = rdm_core::reconnect::next_due(&failed1).unwrap();

        // 1. Armed by the Direct Failed row alone, with no inbound accept trigger.
        let pending = writer.recovery_pending();
        assert_eq!(pending.len(), 1, "the Direct Failed row armed one pending attempt with no accept: {pending:?}");
        assert_eq!((pending[0].destination.clone(), pending[0].due_ms), (dest.clone(), due1));

        let dial = CellDial::new(&writer, resolved(&dest), vec![Err("due-time attempt 1 refused".to_string()), Ok(())]);
        let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(25), Some(dial.dial()));
        assert!(dial.calls().is_empty(), "no dial before due1");

        // 2. When due1 arrives, the reconciler dials attempt 1 without any accept.
        until("the due-time reconciler dials attempt 1", || dial.calls().len() == 1).await;
        let (dial1_dest, dial1_at) = dial.calls()[0].clone();
        assert_eq!(dial1_dest, dest, "attempt 1 dials the exact destination birth");
        assert!(dial1_at >= due1, "attempt 1 waited for due1: dialled at {dial1_at} ms, due at {due1} ms");
        dial.go.add_permits(1);

        // 3. Attempt 1 fails -> writes Direct Failed (E1, 2) and reschedules from that durable row.
        until("attempt 1's Direct Failed (E1, 2) row is held", || held.lock().unwrap().own_latest_directs().iter().any(|d| d.recovery.map(|r| r.attempt_ordinal) == Some(2))).await;
        let failed2 = storage.history().await.unwrap().into_iter().last().unwrap();
        assert_eq!((failed2.kind, failed2.state, failed2.recovery.map(|r| (r.recovery_round, r.attempt_ordinal))), (ConnectionKind::Direct, ConnectionState::Failed, Some((1, 2))));
        let due2 = rdm_core::reconnect::next_due(&failed2).unwrap();
        assert!(due2 > due1, "rescheduled from the new Failed row: due2={due2} > due1={due1}");
        assert_eq!(writer.recovery_pending(), vec![rdm_admin::connections_writer::RecoveryAttempt { destination: dest.clone(), due_ms: due2 }]);

        // 4. An inbound accept arriving while the due-time item is pending coalesces into it and never skips due2.
        writer.accepted(&resolved(&dest));
        assert_eq!(writer.recovery_pending(), vec![rdm_admin::connections_writer::RecoveryAttempt { destination: dest.clone(), due_ms: due2 }], "the accept coalesced into the single pending attempt without changing due2");
        tokio::time::sleep(std::time::Duration::from_millis(150)).await;
        assert_eq!(dial.calls().len(), 1, "the accept did not skip the backoff the durable rows set");

        // 5. When due2 arrives, the reconciler dials attempt 2, which connects, retires the Proxy, and spends the item.
        until("the reconciler dials attempt 2 at due2", || dial.calls().len() == 2).await;
        let (dial2_dest, dial2_at) = dial.calls()[1].clone();
        assert_eq!(dial2_dest, dest);
        assert!(dial2_at >= due2, "attempt 2 waited for due2: dialled at {dial2_at} ms, due at {due2} ms");
        dial.go.add_permits(1);
        until("the route cuts back to Direct after Proxy retirement", || matches!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true })).await;
        until("the pending item is spent once Direct is Connected", || writer.recovery_pending().is_empty()).await;
        // The dial decides from its own result and never waits for its row (Q169): its span,
        // the parent of the row's, ends once that row has landed.
        writer.observations_landed().await;
        let open = open_dials.open();
        tracing::info_span!("rdm.test.connections_writer.open-dials", open).in_scope(|| tracing::info!("the recovery-dial spans open once every owed write has landed"));
        assert_eq!(open, 0, "every recovery-dial span ended once its fact landed");
        let _ = std::fs::remove_dir_all(&dir);
    });
    let named = |n: &str| -> Vec<&serde_json::Value> { spans.iter().filter(|s| s["name"] == n).collect() };
    let dials = named("rdm.node_admin.connection.create.via-recovery-dial");
    assert_eq!(dials.len(), 2, "two recovery dials in the flame (failed attempt 1, connected attempt 2): {dials:?}");
    assert_eq!((dials[0]["attributes"]["outcome"].as_str(), dials[0]["attributes"]["fact"].as_str()), (Some("NotSent"), Some("failed")));
    assert_eq!((dials[1]["attributes"]["outcome"].as_str(), dials[1]["attributes"]["fact"].as_str()), (Some("Reply"), Some("connected")));
    let triggers = named("rdm.node_admin.connection.update.via-accept-trigger");
    assert_eq!(triggers.len(), 1);
    assert_eq!(triggers[0]["attributes"]["outcome"], "coalesced", "the accept while pending coalesced into the due-time item: {:?}", triggers[0]);
    let retirements = named("rdm.node_admin.connection.update.via-retirement");
    assert_eq!(retirements.len(), 1);
    assert_eq!(retirements[0]["attributes"]["outcome"], "landed");
    let spent = named("rdm.node_admin.connection.remove.via-recovery-attempt");
    assert_eq!(spent.len(), 1);
    assert_eq!(spent[0]["attributes"]["reason"], "direct-connected");
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
/// transitions additionally identify the carrier. Reconnect attempts identify the recovery round
/// and attempt ordinal"): this node's writer lands each kind of transition it owns, each on its
/// span: an observed Proxy Connected (`via-observed`), a reconnect attempt of a failing series
/// (`via-recovery-dial`, the series' round and the attempt's ordinal, one above the latest Failed
/// row's), the attempt's Direct Connected (`via-observed`), the Proxy's `Disconnected
/// (direct-restored)` (`via-retirement`), a first Direct Failed toward another destination
/// (`via-observed`, its round and ordinal 1) and a connections delete's drop
/// (`via-connection-delete`). Every transition span names source, destination, kind, state and
/// reason; every Proxy transition its carrier; every Direct Failed and every reconnect attempt its
/// round and ordinal. Must NOT happen: a landed transition whose span leaves out any of them.
#[test]
fn every_landed_connection_transition_is_spanned_with_its_pair_kind_state_reason_carrier_and_series_position() {
    let (me, carrier, dest, other) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"), end("mesh1.rpc.4"));
    let names = (me.name.to_string(), carrier.name.to_string(), dest.name.to_string(), other.name.to_string());
    let dir = data_dir("transition-spans");
    let spans = spans_of("every_landed_connection_transition_is_spanned_with_its_pair_kind_state_reason_carrier_and_series_position", None, || async {
        let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&dir), &me.name, &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
        let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
        let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
        // Seeded: the carrier's edge, and a failing series at (1, 2) whose next attempt is due now.
        let failed_at = now_ms() - rdm_core::reconnect::reconnect_backoff(2) - rdm_core::reconnect::deterministic_jitter(&me.name, &dest.name, 2) - 1;
        let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, failed_at);
        failed.recovery = Some(rdm_core::connections::DirectRecovery { recovery_round: 1, attempt_ordinal: 2 });
        failed.reason = Some("seeded".into());
        writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, failed_at)).await.unwrap();
        writer.record(failed).await.unwrap();
        // Observed: the Proxy through the carrier.
        writer.record_data_plane(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), now_ms()));
        writer.observations_landed().await;
        assert_eq!(held.lock().unwrap().own_active_proxies().len(), 1, "the Proxy is held");
        // The reconnect attempt connects; its Direct Connected owes the Proxy's retirement.
        let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
        dial.go.add_permits(1);
        let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(25), Some(dial.dial()));
        until("the attempt connects and the Proxy's retirement lands", || held.lock().unwrap().own_active_proxies().is_empty()).await;
        writer.observations_landed().await;
        // A first failure toward another destination, then the connection to the destination dropped.
        writer.direct_failed(&resolved(&other), "refused by the cell");
        writer.observations_landed().await;
        let open = writer.held_routes().await.into_iter().filter(|r| r.destination.name == dest.name).collect::<Vec<_>>();
        assert_eq!(open.len(), 1, "the Direct to the destination is held: {open:?}");
        writer.write_drops(&open, "node.connection.delete");
        writer.observations_landed().await;
    });
    let _ = std::fs::remove_dir_all(&dir);
    let named = |n: &str| -> Vec<&serde_json::Value> { spans.iter().filter(|s| s["name"] == n).collect() };
    let transitions: Vec<&serde_json::Value> = ["rdm.node_admin.connection.update.via-observed", "rdm.node_admin.connection.update.via-retirement", "rdm.node_admin.connection.update.via-connection-delete"].into_iter().flat_map(named).collect();
    let kinds: Vec<(String, String, Option<String>)> = transitions.iter().map(|sp| (sp["name"].as_str().unwrap().rsplit('.').next().unwrap().to_string(), format!("{}->{}", attr(sp, "source").unwrap_or_default(), attr(sp, "destination").unwrap_or_default()), attr(sp, "state"))).collect();
    assert_eq!(transitions.len(), 5, "five transitions landed (Proxy Connected, Direct Connected, Proxy Disconnected, Direct Failed, the drop): {kinds:?}");
    for sp in &transitions {
        for k in ["source", "destination", "kind", "state", "reason"] {
            assert!(attr(sp, k).is_some(), "a transition span names its {k}: {sp}");
        }
        assert_eq!(attr(sp, "source").as_deref(), Some(names.0.as_str()), "{sp}");
        assert!(!attr(sp, "kind").unwrap().is_empty() && !attr(sp, "state").unwrap().is_empty(), "{sp}");
        if attr(sp, "kind").as_deref() == Some("proxy") {
            assert_eq!(attr(sp, "carrier").as_deref(), Some(names.1.as_str()), "a Proxy transition names its carrier: {sp}");
        }
        if attr(sp, "state").as_deref() != Some("connected") {
            assert!(!attr(sp, "reason").unwrap().is_empty(), "a Failed or Disconnected transition names its reason: {sp}");
        }
    }
    let retirement = named("rdm.node_admin.connection.update.via-retirement");
    assert_eq!(retirement.iter().map(|sp| (attr(sp, "kind"), attr(sp, "state"), attr(sp, "reason"), attr(sp, "outcome"))).collect::<Vec<_>>(), vec![(Some("proxy".into()), Some("disconnected".into()), Some("direct-restored".into()), Some("landed".into()))], "{retirement:?}");
    let drop = named("rdm.node_admin.connection.update.via-connection-delete");
    assert_eq!(drop.iter().map(|sp| (attr(sp, "destination"), attr(sp, "kind"), attr(sp, "state"))).collect::<Vec<_>>(), vec![(Some(names.2.clone()), Some("direct".into()), Some("disconnected".into()))], "{drop:?}");
    let failed: Vec<&&serde_json::Value> = transitions.iter().filter(|sp| attr(sp, "state").as_deref() == Some("failed")).collect();
    assert_eq!(failed.iter().map(|sp| (attr(sp, "destination"), attr(sp, "recovery_round"), attr(sp, "attempt_ordinal"))).collect::<Vec<_>>(), vec![(Some(names.3.clone()), Some("1".into()), Some("1".into()))], "a Direct Failed names its round and ordinal: {failed:?}");
    let dials = named("rdm.node_admin.connection.create.via-recovery-dial");
    assert_eq!(dials.iter().map(|sp| (attr(sp, "destination"), attr(sp, "recovery_round"), attr(sp, "attempt_ordinal"), attr(sp, "fact"))).collect::<Vec<_>>(), vec![(Some(names.2.clone()), Some("1".into()), Some("3".into()), Some("connected".into()))], "the reconnect attempt names its round and its ordinal, one above the latest Failed row's: {dials:?}");
}

/// CONTRACT (connections.md §12: "The reconnect attempt span is owned by node-base and records
/// whether it: connected / failed / was superseded before dial / was already satisfied by Direct
/// Connected"; §9.5 steps 1-2): two pending attempts that never dial. One's destination birth
/// leaves the topology view before it is due: its attempt ends on `via-recovery-attempt` with
/// reason `destination-superseded`. The other's destination connects Direct by ordinary traffic
/// (`direct_connected`, not the reconciler's dial) before it is due: its attempt ends on
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
        topology_view.set(dest.node_id.clone(), dest.name.clone(), dest.incarnation.clone().unwrap());
        held.lock().unwrap().set_topology_view(topology_view.clone());
        let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
        let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(25), Some(dial.dial()));
        assert_eq!(writer.recovery_pending().len(), 1, "one attempt pending, seconds from due");
        topology_view.depart(dest.node_id.clone(), dest.name.clone());
        until("the attempt for the superseded birth ends", || writer.recovery_pending().is_empty()).await;
        assert!(dial.calls().is_empty(), "no dial toward a superseded birth");
        dirs.borrow_mut().push(dir);
        dests.borrow_mut().push(dest.name.to_string());

        // The birth whose Direct connects by ordinary traffic.
        let (dir, _me, dest, _storage, _held, writer, _) = proxied("pending-satisfied", now_ms()).await;
        let dial = CellDial::new(&writer, resolved(&dest), vec![Ok(())]);
        let _reconciler = writer.spawn_reconciler(std::time::Duration::from_millis(25), Some(dial.dial()));
        assert_eq!(writer.recovery_pending().len(), 1, "one attempt pending, seconds from due");
        writer.direct_connected(&resolved(&dest));
        until("the attempt the Direct Connected satisfied ends", || writer.recovery_pending().is_empty()).await;
        writer.observations_landed().await;
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        assert!(dial.calls().is_empty(), "no dial once the Direct is connected");
        dirs.borrow_mut().push(dir);
        dests.borrow_mut().push(dest.name.to_string());
    });
    for dir in dirs.into_inner() {
        let _ = std::fs::remove_dir_all(dir);
    }
    assert!(spans.iter().all(|s| s["name"] != "rdm.node_admin.connection.create.via-recovery-dial"), "nothing was dialled");
    let ended: Vec<(Option<String>, Option<String>)> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.connection.remove.via-recovery-attempt").map(|sp| (attr(sp, "destination"), attr(sp, "reason"))).collect();
    let d = dests.into_inner();
    assert_eq!(ended, vec![(Some(d[0].clone()), Some("destination-superseded".into())), (Some(d[1].clone()), Some("direct-connected".into()))], "each pending attempt ends on one span naming why");
}
