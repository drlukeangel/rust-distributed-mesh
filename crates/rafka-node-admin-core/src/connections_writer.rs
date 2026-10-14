//! The source-owned connections writer (connections.md sections 3, 4.2, 9 and 10; i143.e6.s11):
//! every node writes its own Direct and Proxy facts, and holds its own projection of them.
//!
//! The node's data-plane Node RPC client reports what it observes of the connections it opens
//! through [`ConnectionObserver`]; this writer turns each observation into one row, applies it to the held projection the route
//! resolver reads, appends it to the raw log and keeps it as the latest of its pair in the index.
//! At birth the held projection is hydrated from the index, so a restart keeps its latest Proxy
//! and the backoff of its latest Direct failure. The writer decides no route and chooses no
//! carrier: it records facts. Every row names this node as it is when the row is written
//! (node-identity-and-state.md §8): the name it holds now (R12, read from the process's one held
//! name), its node id and its process birth.
//!
//! One obligation is reconciled here (connections.md §10): whenever this node's latest Direct to
//! a destination is Connected beside its own active Proxy to it, the Proxy's retirement
//! (`Disconnected(direct-restored)`) is owed. It is written durably first and applied to the held
//! projection only once the write lands, so new calls keep the proven Proxy until then. A refused
//! write leaves the obligation standing: it is derived again from the two latest rows at the next
//! observation, at the next settlement a caller asks for, and every [`RECONCILER_TICK`] while
//! owed. Nothing is queued; a restart reconstructs the same obligation from the same rows.
//!
//! Two paths arm the one pending direct-recovery attempt per destination birth (connections.md
//! §9.4, Q7): a latest Direct Failed row in `held`, and a connection accepted from the exact
//! destination birth this node reaches through its own Proxy (R-CN1,
//! [`rafka_node_rpc::AcceptTrigger`], never a row). An accept arriving while an item is pending
//! coalesces into it and wakes the reconciler, which dials this node's own data-plane connection
//! once the series' reconnect backoff from its latest Failed row has ended (§9.3), never earlier.
//! Only that dial's Direct Connected owes the Proxy's retirement; a failed dial is the series'
//! next Failed row and the attempt waits for its backoff; an attempt toward a birth membership no
//! longer holds is dropped undialled (§9.5 step 1).

use crate::storage::ConnectionsStorage;
use rafka_mesh_entity::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
use rafka_mesh_entity::reconnect::{first_failure, next_due, owed_retirements};
use rafka_mesh_entity::{IncarnationId, NodeId, PathName};
use std::collections::HashMap;
use rafka_node_rpc::{ConnectionObserver, ResolvedNode};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// How often the reconciler looks at what is owed without a wake: an owed Proxy retirement whose
/// write was refused is attempted again, and a pending recovery attempt whose backoff has ended is
/// dialled.
pub const RECONCILER_TICK: Duration = Duration::from_millis(500);

/// One direct dial of this node's own data plane toward an exact destination birth
/// (connections.md §9.5 step 3), answering the call outcome's name for the attempt's span. The
/// fact reaches the writer through the data-plane client's observer (Direct Connected or Failed,
/// `origin=dial`), never through the reconciler.
pub type RecoveryDial = Arc<dyn Fn(ConnectionEnd) -> std::pin::Pin<Box<dyn std::future::Future<Output = String> + Send>> + Send + Sync>;

/// One pending direct-recovery attempt (connections.md §9.4): toward an exact destination birth,
/// due at `due_ms` on the local clock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryAttempt {
    /// The destination birth the attempt dials.
    pub destination: ConnectionEnd,
    /// When the attempt is due.
    pub due_ms: u64,
}

/// Writes the connection facts of one source node and holds the projection they build.
#[derive(Clone)]
pub struct ConnectionsWriter {
    inner: Arc<Inner>,
}

/// A start's hold on every row of its connections writer ([`ConnectionsWriter::hold_rows`]):
/// dropping it lets the rows observed meanwhile be written, naming the node as it is then.
pub struct RowsHeld {
    _gate: tokio::sync::OwnedMutexGuard<()>,
}

impl std::fmt::Debug for RowsHeld {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RowsHeld")
    }
}

/// Makes an observation's row from the held projection, inside the write gate, naming this node as
/// it is at that moment (the source end the held projection's own source was just set to).
type Build = Box<dyn FnOnce(&ConnectionsWriter, ConnectionEnd) -> NodeConnection + Send>;

/// One queued observation: how its row is built, where it was observed (`dial`) and the
/// peer endpoint it was observed with.
type Observation = (Build, &'static str, String);

/// Where the process holds the name this node publishes (R12): read, never copied.
type HeldName = Arc<dyn Fn() -> PathName + Send + Sync>;

struct Inner {
    /// This node at its birth: its node id and process birth, and the name it was born with.
    own: ConnectionEnd,
    /// The name this node holds, read at every row ([`ConnectionsWriter::follow_held_name`]);
    /// unset, the birth's.
    held_name: std::sync::OnceLock<HeldName>,
    storage: Arc<dyn ConnectionsStorage>,
    held: Arc<Mutex<ConnectionsHeld>>,
    runtime: tokio::runtime::Handle,
    /// The last stamp this writer gave a row: every row it writes carries a later one, so two
    /// facts in one millisecond never tie (the held projection keeps one stamp per key).
    last_stamp: Mutex<u64>,
    /// Observation writes spawned and not yet landed or refused; [`ConnectionsWriter::observations_landed`]
    /// waits for this to reach zero, `idle` wakes it when a write finishes.
    in_flight: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
    /// The observations in the order they were observed: one consumer builds, stamps, writes and
    /// applies them one at a time, so the index member of a pair is always the last observed.
    queue: tokio::sync::mpsc::UnboundedSender<Observation>,
    /// The one write gate: held across every build, durable write and apply, whether it is an
    /// observation, a [`ConnectionsWriter::record`] or a retirement settlement; and by a start
    /// until it holds the name its join answer handed ([`ConnectionsWriter::hold_rows`]).
    write_gate: Arc<tokio::sync::Mutex<()>>,
    /// The pending direct-recovery attempts (connections.md §9.4, Q7): at most one per destination
    /// birth, armed by a latest Direct Failed row or by an accept from that birth while this node
    /// reaches it through its Proxy.
    recovery: Mutex<HashMap<(NodeId, IncarnationId), Pending>>,
    /// Wakes the reconciler once an accept armed an attempt: one stored wake however many accepts.
    wake: tokio::sync::Notify,
}

/// The local OS clock: a writer orders its own rows by it and ages its owed retirements by it. No
/// other node reads these stamps as a time; rafka-time is not adopted when a node first observes a
/// connection (its join).
fn local_now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

fn end_of(node: &ResolvedNode) -> ConnectionEnd {
    ConnectionEnd { name: node.name.clone(), node_id: node.node_id.clone(), incarnation: Some(node.incarnation.clone()) }
}

/// This node's open routes in `held`: its own latest Direct entries that are `connected`, then its
/// own active Proxy entries, each by destination.
/// The connections a node holds, as one read of its writer sees them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldConnections {
    /// The name the node holds, and writes its rows under.
    pub name: PathName,
    /// Whether its held set is complete.
    pub complete: bool,
    /// Its own latest Direct entries that are `connected` and its own active Proxy entries.
    pub routes: Vec<NodeConnection>,
}

fn open_routes(held: &ConnectionsHeld) -> Vec<NodeConnection> {
    held.own_latest_directs().into_iter().filter(|e| e.state == ConnectionState::Connected).chain(held.own_active_proxies()).cloned().collect()
}

/// One pending recovery attempt: the destination birth, and the trace of the accept that armed it
/// (its dial's span is a child of that accept's wake).
#[derive(Clone)]
struct Pending {
    destination: ConnectionEnd,
    traceparent: Option<String>,
}

/// What one reconciler pass does with a pending recovery attempt.
enum Step {
    /// Its backoff has not ended.
    Wait,
    /// Spent undialled, for the named reason.
    Spent(&'static str),
    /// Dial it: due at the instant, scheduled from the latest Direct row named (if any).
    Dial(u64, Option<rafka_mesh_entity::ConnectionId>),
}

/// When an attempt toward `destination` is due, and the latest Direct row it is scheduled from: the
/// series' backoff from this node's latest Direct Failed row toward that birth (connections.md
/// §9.3), else at once.
fn recovery_due(held: &ConnectionsHeld, destination: &ConnectionEnd) -> (u64, Option<rafka_mesh_entity::ConnectionId>) {
    match held.own_latest_directs().into_iter().find(|d| d.destination.name == destination.name) {
        Some(row) if (&row.destination.node_id, &row.destination.incarnation) == (&destination.node_id, &destination.incarnation) => (next_due(row).unwrap_or(0), Some(row.id.clone())),
        Some(row) => (0, Some(row.id.clone())),
        None => (0, None),
    }
}

fn kind_name(k: ConnectionKind) -> &'static str {
    match k {
        ConnectionKind::Direct => "direct",
        ConnectionKind::Proxy => "proxy",
    }
}

fn state_name(s: ConnectionState) -> &'static str {
    match s {
        ConnectionState::Connected => "connected",
        ConnectionState::Disconnected => "disconnected",
        ConnectionState::Failed => "failed",
    }
}

impl ConnectionsWriter {
    /// A writer for `own`, over its storage, holding `held` (shared with the route resolver).
    pub fn new(own: ConnectionEnd, storage: Arc<dyn ConnectionsStorage>, held: Arc<Mutex<ConnectionsHeld>>) -> Self {
        let runtime = tokio::runtime::Handle::current();
        let (queue, mut observations) = tokio::sync::mpsc::unbounded_channel::<Observation>();
        let inner = Arc::new(Inner {
            own,
            held_name: std::sync::OnceLock::new(),
            storage,
            held,
            runtime: runtime.clone(),
            last_stamp: Mutex::new(0),
            in_flight: std::sync::atomic::AtomicUsize::new(0),
            idle: tokio::sync::Notify::new(),
            queue,
            write_gate: Arc::new(tokio::sync::Mutex::new(())),
            recovery: Mutex::new(HashMap::new()),
            wake: tokio::sync::Notify::new(),
        });
        let weak = Arc::downgrade(&inner);
        runtime.spawn(async move {
            while let Some((build, origin, peer)) = observations.recv().await {
                let Some(inner) = weak.upgrade() else { break };
                ConnectionsWriter { inner }.write_observed(build, origin, peer).await;
            }
        });
        Self { inner }
    }

    /// Hold every row: until the returned hold is dropped no row is built, written or applied, and
    /// every connection observed meanwhile waits for it. A start takes it before its join and lets
    /// it go once it holds the name its join answer handed (node-start.md step 2), so the join's
    /// connection, and any other the start makes or accepts, is recorded under that name: a node
    /// renamed while it was stopped never writes a row naming the name it gave up at its join.
    pub async fn hold_rows(&self) -> RowsHeld {
        RowsHeld { _gate: self.inner.write_gate.clone().lock_owned().await }
    }

    /// The projection the writer holds.
    pub fn held(&self) -> Arc<Mutex<ConnectionsHeld>> {
        self.inner.held.clone()
    }

    /// Read this node's name from `held`, the one place the process holds the name it publishes
    /// (its membership's, R12): from then on every row this writer writes names it as the source
    /// (node-identity-and-state.md §8: `source` is the end's path.name), so a name the node takes
    /// (`ApplyNodeName`, or the name its join answer hands it) is the source of its next row.
    /// Until then the source is the birth's name, which is the name the node holds while nothing
    /// can have given it another. Bound once, at boot; a second binding is refused by name.
    pub fn follow_held_name(&self, held: impl Fn() -> PathName + Send + Sync + 'static) -> Result<(), String> {
        self.inner.held_name.set(Arc::new(held)).map_err(|_| format!("{}: this connections writer already reads its name from the process's held name", self.inner.own.name))
    }

    /// This node, as the source every row it writes now names: the name it holds, its node id and
    /// its process birth.
    pub fn own(&self) -> ConnectionEnd {
        let name = self.inner.held_name.get().map_or_else(|| self.inner.own.name.clone(), |held| held());
        ConnectionEnd { name, ..self.inner.own.clone() }
    }

    /// [`Self::own`], with the held projection's own source following it: the rows this node
    /// writes under the name it holds are its own facts from here on, and the facts of a name it
    /// gave up are kept as written, no longer its own (connections.md §4.2: a node holds the
    /// overrides whose source is itself). Called inside the write gate, before a row is built.
    fn own_now(&self) -> ConnectionEnd {
        let own = self.own();
        let mut held = self.inner.held.lock().unwrap();
        let before = held.own_source().cloned();
        if before.as_ref() != Some(&own.name) {
            held.set_own_source(own.name.clone());
            if let Some(from) = before {
                tracing::info_span!("rdm.node_admin.connection.update.via-held-name", node_id = %own.node_id, from = %from, to = %own.name)
                    .in_scope(|| tracing::info!("this node took another name: its rows name it as their source from here on, and the rows it wrote before stay as written"));
            }
        }
        own
    }

    /// Observation writes spawned by [`ConnectionObserver`] calls that have not yet landed or
    /// been refused. The raw log, the index, the held projection and the observation's span follow
    /// when its write completes.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Resolves once every observation write spawned before the call has completed: the moment
    /// after which the index and the evidence hold every fact this writer was handed.
    pub async fn observations_landed(&self) {
        loop {
            let mut notified = std::pin::pin!(self.inner.idle.notified());
            notified.as_mut().enable();
            if self.in_flight() == 0 {
                return;
            }
            notified.await;
        }
    }

    /// Hydrate the held projection from the index: every latest row this node wrote (its own
    /// Direct and Proxy facts) is applied, so the first call after a restart resolves over the
    /// latest Proxy and the reconnect series continues from the latest failure. A retirement the
    /// two latest rows of a pair owe is settled right after, from those rows (connections.md §10).
    pub async fn hydrate(&self) -> Result<usize, String> {
        let rows = self.inner.storage.connections().await.map_err(|e| e.to_string())?;
        let own = self.own_now();
        let applied = {
            let mut held = self.inner.held.lock().unwrap();
            let mut applied = 0;
            for row in rows {
                if held.apply(row).is_ok() {
                    applied += 1;
                }
            }
            held.mark_complete();
            applied
        };
        tracing::info_span!("rdm.node_admin.connection.update.via-hydrate", node = %own.name, applied)
            .in_scope(|| tracing::info!("the held projection is hydrated from this node's own index"));
        self.sync_recovery();
        let _ = self.settle_owed().await;
        Ok(applied)
    }

    /// Record one fact: the raw log first, then the index, then the held projection.
    pub async fn record(&self, row: NodeConnection) -> Result<(), String> {
        let _gate = self.inner.write_gate.lock().await;
        self.own_now();
        self.persist(&row).await?;
        let _ = self.inner.held.lock().unwrap().apply(row);
        self.sync_recovery();
        Ok(())
    }

    /// The connections this node holds now (R-CR1): its own latest Direct entries that are
    /// `connected` and its own active Proxy entries, under the name it holds, once every observation
    /// handed to the writer before the call has landed. Ordered by destination, Direct first.
    pub async fn held_routes(&self) -> Vec<NodeConnection> {
        self.held_connections().await.routes
    }

    /// [`Self::held_routes`] with the name the node holds them under and whether its held set is
    /// complete: what `node.connections.get` answers.
    pub async fn held_connections(&self) -> HeldConnections {
        self.observations_landed().await;
        let _gate = self.inner.write_gate.lock().await;
        let own = self.own_now();
        let held = self.inner.held.lock().unwrap();
        HeldConnections { name: own.name, complete: held.is_complete(), routes: open_routes(&held) }
    }

    /// `node.connections.delete`'s drops (R-CR3): each of `routes` (open entries this node held,
    /// whose connections were just cut) gets one `disconnected` entry with its own id and `reason`,
    /// naming this node as it is now, written durably and then applied. A route whose open entry is
    /// no longer the latest of its pair (a newer entry was written meanwhile) is cut and writes no
    /// drop: the newer entry is the pair's state. Returns the ids of the routes cut, and those whose
    /// drop the storage refused, with its refusal: the log still holds those open.
    pub async fn write_drops(&self, routes: &[NodeConnection], reason: &str) -> (Vec<rafka_mesh_entity::ConnectionId>, Vec<(rafka_mesh_entity::ConnectionId, String)>) {
        let _gate = self.inner.write_gate.lock().await;
        let own = self.own_now();
        let (mut closed, mut refused) = (Vec::new(), Vec::new());
        for route in routes {
            let current = {
                let held = self.inner.held.lock().unwrap();
                open_routes(&held).into_iter().any(|r| r.id == route.id)
            };
            if !current {
                closed.push(route.id.clone());
                continue;
            }
            let drop = NodeConnection {
                id: rafka_mesh_entity::ConnectionId::mint(),
                source: own.clone(),
                destination: route.destination.clone(),
                kind: route.kind,
                state: ConnectionState::Disconnected,
                carrier: route.carrier.clone(),
                recovery: None,
                reason: Some(reason.to_string()),
                logged_at_ms: self.stamp(),
            };
            let span = tracing::info_span!(
                "rdm.node_admin.connection.update.via-connections-delete",
                source = %drop.source.name,
                destination = %drop.destination.name,
                kind = kind_name(drop.kind),
                closed_entry = %route.id,
                entry = %drop.id,
                reason,
                outcome = tracing::field::Empty,
            );
            match self.persist(&drop).await {
                Ok(()) => {
                    let _ = self.inner.held.lock().unwrap().apply(drop);
                    span.record("outcome", "landed");
                    closed.push(route.id.clone());
                }
                Err(e) => {
                    span.record("outcome", format!("refused: {e}").as_str());
                    refused.push((route.id.clone(), e));
                }
            }
            span.in_scope(|| tracing::info!("the drop of a connection node.connections.delete cut"));
        }
        (closed, refused)
    }

    /// The raw log first, then the index; an error names the storage's own refusal.
    async fn persist(&self, row: &NodeConnection) -> Result<(), String> {
        self.inner.storage.append_history(row).await.map_err(|e| e.to_string())?;
        self.inner.storage.put_connection(row).await.map_err(|e| e.to_string())
    }

    /// The latest Direct row this node holds toward `destination`, if any.
    fn latest_direct(&self, destination: &ConnectionEnd) -> Option<NodeConnection> {
        self.inner.held.lock().unwrap().own_latest_directs().into_iter().find(|d| d.destination.name == destination.name).cloned()
    }

    fn direct(own: ConnectionEnd, destination: ConnectionEnd, state: ConnectionState, reason: Option<String>) -> NodeConnection {
        NodeConnection {
            id: rafka_mesh_entity::ConnectionId::mint(),
            source: own,
            destination,
            kind: ConnectionKind::Direct,
            state,
            carrier: None,
            recovery: None,
            reason,
            logged_at_ms: local_now_ms(),
        }
    }

    /// The Proxy retirements this node owes now (connections.md §10), derived from the held
    /// projection's two latest rows per pair: never a queue.
    pub fn owed(&self) -> Vec<NodeConnection> {
        owed_retirements(&self.inner.held.lock().unwrap(), local_now_ms())
    }

    /// Settle every Proxy retirement this node owes: each is written durably as
    /// `Disconnected(direct-restored)` and applied to the held projection once its write lands,
    /// so new calls cut back to Direct only then. A refused write is named and leaves the
    /// obligation standing for the next attempt. Returns how many landed.
    pub async fn settle_owed(&self) -> Result<usize, String> {
        let _gate = self.inner.write_gate.lock().await;
        self.own_now();
        let at = self.stamp();
        let owed = owed_retirements(&self.inner.held.lock().unwrap(), at);
        let mut landed = 0;
        for row in owed {
            let span = tracing::info_span!(
                "rdm.node_admin.connection.update.via-retirement",
                source = %row.source.name,
                destination = %row.destination.name,
                carrier = %row.carrier.as_ref().map(|c| c.name.to_string()).unwrap_or_default(),
                outcome = tracing::field::Empty,
                reason = tracing::field::Empty,
            );
            match self.persist(&row).await {
                Ok(()) => {
                    let _ = self.inner.held.lock().unwrap().apply(row);
                    span.record("outcome", "landed");
                    span.in_scope(|| tracing::info!("the owed Proxy retirement landed; new calls cut back to Direct"));
                    landed += 1;
                }
                Err(e) => {
                    span.record("outcome", "refused");
                    span.record("reason", e.as_str());
                    span.in_scope(|| tracing::info!("the owed Proxy retirement write was refused; the Proxy stays effective and the obligation stands"));
                    return Err(e);
                }
            }
        }
        Ok(landed)
    }

    /// [`Self::settle_owed`], as the integration cells name it.
    pub async fn retire_owed(&self) -> Result<usize, String> {
        self.settle_owed().await
    }

    /// The node's one connections reconciler (connections.md §9.4, §10): each `every`, or at once
    /// when an accept armed an attempt, it attempts every owed retirement again and dials every
    /// pending recovery attempt whose backoff has ended through `dial`, this node's own data plane
    /// (none on a node with no data plane). Derived from the rows each time; the task ends with
    /// the runtime.
    pub fn spawn_reconciler(&self, every: Duration, dial: Option<RecoveryDial>) -> tokio::task::JoinHandle<()> {
        let w = self.clone();
        self.inner.runtime.spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(every) => {}
                    _ = w.inner.wake.notified() => {}
                }
                if !w.owed().is_empty() {
                    let _ = w.settle_owed().await;
                }
                if let Some(dial) = &dial {
                    w.recover(dial).await;
                }
            }
        })
    }

    /// The pending recovery attempts, each with when it is due.
    pub fn recovery_pending(&self) -> Vec<RecoveryAttempt> {
        let pending: Vec<ConnectionEnd> = self.inner.recovery.lock().unwrap().values().map(|p| p.destination.clone()).collect();
        let held = self.inner.held.lock().unwrap();
        pending.into_iter().map(|destination| RecoveryAttempt { due_ms: recovery_due(&held, &destination).0, destination }).collect()
    }

    /// One pass over the pending recovery attempts (connections.md §9.5): an attempt whose birth
    /// is gone, or toward which a Direct is Connected, is spent undialled; one whose backoff has
    /// not ended waits; the rest are dialled once each. A dial's fact is the data-plane client's
    /// observation; once it has landed, a Failed row reschedules the attempt from its own backoff,
    /// a Connected one spends it, and a dial that produced no fact spends it by name.
    async fn recover(&self, dial: &RecoveryDial) {
        let pending: Vec<((NodeId, IncarnationId), Pending)> = self.inner.recovery.lock().unwrap().iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        for (key, Pending { destination, traceparent }) in pending {
            let source = self.own().name;
            let step = {
                let held = self.inner.held.lock().unwrap();
                if held.end_superseded(&destination) == Some(true) {
                    Step::Spent("destination-superseded")
                } else if matches!(held.active_direct(&source, &destination.name), Some(Some(edge)) if edge.destination.incarnation == destination.incarnation) {
                    Step::Spent("direct-connected")
                } else {
                    let (due_ms, basis) = recovery_due(&held, &destination);
                    if local_now_ms() < due_ms { Step::Wait } else { Step::Dial(due_ms, basis) }
                }
            };
            match step {
                Step::Wait => {}
                Step::Spent(reason) => self.spend(&key, &source, &destination, reason),
                Step::Dial(due_ms, basis) => {
                    let span = tracing::info_span!(
                        "rdm.node_admin.connection.create.via-recovery-dial",
                        source = %source,
                        destination = %destination.name,
                        destination_node_id = %destination.node_id,
                        destination_incarnation = %destination.incarnation.as_ref().map(ToString::to_string).unwrap_or_default(),
                        due_ms,
                        outcome = tracing::field::Empty,
                        fact = tracing::field::Empty,
                        entry = tracing::field::Empty,
                    );
                    if let Some(tp) = &traceparent {
                        rafka_mesh_telemetry::set_parent(&span, tp);
                    }
                    let outcome = tracing::Instrument::instrument(dial(destination.clone()), span.clone()).await;
                    self.observations_landed().await;
                    let latest = self.latest_direct(&destination);
                    span.record("outcome", outcome.as_str());
                    let fact = latest.as_ref().filter(|r| Some(&r.id) != basis.as_ref());
                    span.record("fact", fact.map_or("none", |r| state_name(r.state)));
                    if let Some(r) = fact {
                        span.record("entry", r.id.to_string().as_str());
                    }
                    span.in_scope(|| tracing::info!("this node dialled its own connection toward the destination birth"));
                    match latest {
                        Some(r) if Some(&r.id) != basis.as_ref() && r.state == ConnectionState::Connected => self.spend(&key, &source, &destination, "direct-connected"),
                        Some(r) if Some(&r.id) != basis.as_ref() => {
                            if let Some(p) = self.inner.recovery.lock().unwrap().get_mut(&key) {
                                p.traceparent = None;
                            }
                        }
                        _ => self.spend(&key, &source, &destination, "no-fact"),
                    }
                }
            }
        }
    }

    /// Arm the one pending recovery attempt per `(destination, destination_incarnation)` from each
    /// own latest Direct Failed row in `held` (connections.md §9.4, §9.5, Q7). A series for a
    /// superseded birth at the same path is replaced by the current birth's series.
    fn sync_recovery(&self) {
        let directs: Vec<NodeConnection> = {
            let held = self.inner.held.lock().unwrap();
            held.own_latest_directs()
                .into_iter()
                .filter(|r| held.end_superseded(&r.destination) != Some(true))
                .cloned()
                .collect()
        };
        let mut recovery = self.inner.recovery.lock().unwrap();
        for row in directs {
            if row.state == ConnectionState::Failed && row.recovery.is_some() {
                if let Some(inc) = row.destination.incarnation.clone() {
                    recovery.retain(|(id, i), p| p.destination.name != row.destination.name || (id == &row.destination.node_id && i == &inc));
                    recovery.entry((row.destination.node_id.clone(), inc)).or_insert_with(|| Pending {
                        destination: row.destination.clone(),
                        traceparent: None,
                    });
                }
            }
        }
    }

    /// Spend the pending attempt `key` undialled, naming why.
    fn spend(&self, key: &(NodeId, IncarnationId), source: &PathName, destination: &ConnectionEnd, reason: &'static str) {
        self.inner.recovery.lock().unwrap().remove(key);
        tracing::info_span!(
            "rdm.node_admin.connection.remove.via-recovery-attempt",
            source = %source,
            destination = %destination.name,
            destination_node_id = %destination.node_id,
            destination_incarnation = %destination.incarnation.as_ref().map(ToString::to_string).unwrap_or_default(),
            reason,
        )
        .in_scope(|| tracing::info!("the pending recovery attempt is spent"));
    }

    /// A strictly increasing stamp for this writer's rows.
    fn stamp(&self) -> u64 {
        let mut last = self.inner.last_stamp.lock().unwrap();
        *last = local_now_ms().max(*last + 1);
        *last
    }

    /// An observed fact: `build` makes the row from the held projection, the row is written to the
    /// raw log and the index, and it is applied to the held projection only once that write has
    /// landed. A refused write leaves the fact unheld and names the refusal; the next observation
    /// of the connection writes it afresh. Observations take the write gate one at a time, so a
    /// row built from the held projection (a failing series' next ordinal) sees every earlier
    /// observation already applied. A Direct Connected settles the Proxy retirement it may owe
    /// once its own write has landed.
    fn observed(&self, build: impl FnOnce(&Self, ConnectionEnd) -> NodeConnection + Send + 'static, origin: &'static str, node: &ResolvedNode) {
        self.inner.in_flight.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let _ = self.inner.queue.send((Box::new(build), origin, node.endpoint_id.fmt_short().to_string()));
    }

    /// Write one queued observation inside the write gate, then settle the retirement it may owe.
    async fn write_observed(&self, build: Build, origin: &'static str, peer: String) {
        let w = self;
        {
            let gate = w.inner.write_gate.lock().await;
            let own = w.own_now();
            let mut row = build(w, own.clone());
            row.logged_at_ms = w.stamp();
            let settles = row.kind == ConnectionKind::Direct && row.state == ConnectionState::Connected;
            let span = tracing::info_span!(
                "rdm.node_admin.connection.update.via-observed",
                entry = %row.id,
                source = %row.source.name,
                destination = %row.destination.name,
                peer = %peer,
                kind = kind_name(row.kind),
                state = state_name(row.state),
                reason = %row.reason.clone().unwrap_or_default(),
                origin,
                outcome = tracing::field::Empty,
            );
            let landed = match w.persist(&row).await {
                Ok(()) => {
                    let _ = w.inner.held.lock().unwrap().apply(row.clone());
                    w.sync_recovery();
                    span.record("outcome", "landed");
                    true
                }
                Err(e) => {
                    span.record("outcome", format!("refused: {e}").as_str());
                    tracing::info_span!(
                        "rdm.node_admin.connection.reject.via-refused-write",
                        node = %own.name,
                        source = %row.source.name,
                        destination = %row.destination.name,
                        destination_node_id = %row.destination.node_id,
                        kind = kind_name(row.kind),
                        state = state_name(row.state),
                        reason = %row.reason.clone().unwrap_or_default(),
                        origin,
                        logged_at_ms = row.logged_at_ms,
                        refusal = %e,
                    )
                    .in_scope(|| tracing::info!("the observed connection fact was refused by storage; it is not held, and the next observation of this connection writes it afresh"));
                    false
                }
            };
            span.in_scope(|| tracing::info!("one connection fact this node observed"));
            drop(gate);
            if settles && landed {
                let _ = tracing::Instrument::instrument(w.settle_owed(), span.clone()).await;
            }
            drop(span);
            w.inner.in_flight.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            w.inner.idle.notify_waiters();
        }
    }
}

impl ConnectionObserver for ConnectionsWriter {
    fn direct_connected(&self, node: &ResolvedNode) {
        let destination = end_of(node);
        self.observed(move |_, own| Self::direct(own, destination, ConnectionState::Connected, None), "dial", node);
    }

    fn direct_failed(&self, node: &ResolvedNode, reason: &str) {
        let destination = end_of(node);
        let reason = reason.to_string();
        self.observed(
            move |w, own| {
                let previous = w.latest_direct(&destination);
                // A failing series of the birth dialled continues in its round, one ordinal up;
                // anything else (never tried, connected, dropped, or a failure against another
                // birth at the destination: a series is for the current destination incarnation,
                // connections.md §9.1) is a new incident: a new round at ordinal one.
                match previous.as_ref() {
                    Some(p) if p.state == ConnectionState::Failed && (&p.destination.node_id, &p.destination.incarnation) == (&destination.node_id, &destination.incarnation) => match p.recovery {
                        // The row names both ends as they are now: this process's birth, never the
                        // one that wrote the previous attempt, and the birth dialled.
                        Some(r) => NodeConnection {
                            id: rafka_mesh_entity::ConnectionId::mint(),
                            source: own,
                            destination,
                            recovery: Some(rafka_mesh_entity::connections::DirectRecovery { recovery_round: r.recovery_round, attempt_ordinal: r.attempt_ordinal.saturating_add(1) }),
                            reason: Some(reason),
                            logged_at_ms: local_now_ms(),
                            ..p.clone()
                        },
                        None => first_failure(own, destination, previous.as_ref(), &reason, local_now_ms()),
                    },
                    _ => first_failure(own, destination, previous.as_ref(), &reason, local_now_ms()),
                }
            },
            "dial",
            node,
        );
    }

    fn direct_broken(&self, node: &ResolvedNode, reason: &str) {
        let destination = end_of(node);
        let reason = reason.to_string();
        self.observed(move |_, own| Self::direct(own, destination, ConnectionState::Disconnected, Some(reason)), "dial", node);
    }
}

impl rafka_node_rpc::AcceptTrigger for ConnectionsWriter {
    /// An accept writes nothing (R-CN1). From the exact birth this node's own active Proxy names,
    /// with no Direct to it Connected, it arms one recovery attempt (or finds it armed) and wakes
    /// the reconciler; from another birth at that path it arms nothing, by name; from a node this
    /// node reaches by no Proxy it is an accept like any other.
    fn accepted(&self, node: &ResolvedNode) {
        let destination = end_of(node);
        let (source, proxied) = {
            let held = self.inner.held.lock().unwrap();
            let Some(proxy) = held.own_active_proxies().into_iter().find(|p| p.destination.name == destination.name) else { return };
            let direct = matches!(held.active_direct(&proxy.source.name, &destination.name), Some(Some(_)));
            (proxy.source.name.clone(), (proxy.destination.clone(), direct))
        };
        let (named, direct) = proxied;
        let key = (destination.node_id.clone(), node.incarnation.clone());
        let span = tracing::info_span!(
            "rdm.node_admin.connection.update.via-accept-trigger",
            source = %source,
            destination = %destination.name,
            destination_node_id = %destination.node_id,
            destination_incarnation = %node.incarnation,
            proxy_destination_node_id = %named.node_id,
            proxy_destination_incarnation = %named.incarnation.as_ref().map(ToString::to_string).unwrap_or_default(),
            peer = %node.endpoint_id.fmt_short(),
            outcome = tracing::field::Empty,
        );
        let outcome = if (&named.node_id, &named.incarnation) != (&destination.node_id, &destination.incarnation) {
            "another-birth"
        } else if direct {
            "direct-connected"
        } else {
            match self.inner.recovery.lock().unwrap().entry(key) {
                std::collections::hash_map::Entry::Occupied(mut e) => {
                    if e.get().traceparent.is_none() {
                        e.get_mut().traceparent = rafka_mesh_telemetry::traceparent_of(&span);
                    }
                    "coalesced"
                }
                std::collections::hash_map::Entry::Vacant(v) => {
                    v.insert(Pending { destination: destination.clone(), traceparent: rafka_mesh_telemetry::traceparent_of(&span) });
                    "pending"
                }
            }
        };
        span.record("outcome", outcome);
        span.in_scope(|| tracing::info!("an accept from the destination: a recovery trigger for this node's own dial, never a Direct entry"));
        if matches!(outcome, "pending" | "coalesced") {
            self.inner.wake.notify_one();
        }
    }
}

#[async_trait::async_trait]
impl rafka_node_rpc::CarrierEdges for ConnectionsWriter {
    /// The carrier's dial to `node` just failed: the edge is Failed by that very outcome, and the
    /// text is the one the fact carries, named without waiting for the fact's write.
    async fn edge_after_dial(&self, node: &ResolvedNode, reason: &str) -> Option<String> {
        Some(format!("{} -> {} Direct {} ({reason})", self.own().name, node.name, state_name(ConnectionState::Failed)))
    }

    async fn edge_not_active(&self, target: &rafka_mesh_entity::NodeId) -> Option<String> {
        // The dial that just failed handed its Failed fact to this writer; it is held once its write
        // has landed. The carrier answers from the facts, so it waits for them.
        self.observations_landed().await;
        let held = self.inner.held.lock().unwrap();
        let latest = held.own_latest_directs().into_iter().find(|d| &d.destination.node_id == target)?;
        (latest.state != ConnectionState::Connected).then(|| {
            format!(
                "{} -> {} Direct {}{}",
                latest.source.name,
                latest.destination.name,
                state_name(latest.state),
                latest.reason.as_deref().map(|r| format!(" ({r})")).unwrap_or_default()
            )
        })
    }
}
