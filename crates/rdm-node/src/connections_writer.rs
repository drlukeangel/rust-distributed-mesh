//! The source-owned connections writer (connections.md sections 3, 4.2, 9 and 10; i143.e6.s11):
//! every node writes its own Direct and Proxy facts, and holds its own projection of them.
//!
//! The node's data-plane Node RPC client reports what it observes of the connections it opens
//! through [`ConnectionObserver`]; this writer turns each observation into one row, applies it to the held projection the route
//! resolver reads, appends it to the raw log and keeps it as the latest of its pair in the index.
//! A birth holds no connection: a restart closed every connection the node held, so the writer is
//! born with its held projection complete and empty, and reads nothing back from the store (Q227).
//! The writer decides no route and chooses no carrier: it records facts. Every row names this node as it is when the row is written
//! (node-identity-and-state.md §8): the name it holds now (R12, read from the process's one held
//! name), its node id and its process birth.
//!
//! One obligation is reconciled here (connections.md §10): whenever this node's latest Direct to
//! a destination is Connected beside its own active Proxy to it, the Proxy's delete
//! (`Disconnected(direct-restored)`) is owed. It is written durably first and applied to the held
//! projection only once the write lands, so new calls keep the proven Proxy until then. A refused
//! write leaves the obligation standing: it is derived again from the two latest rows at the next
//! observation, at the next settlement a caller asks for, and every [`RECONCILER_TICK`] while
//! owed. Nothing is queued for it.
//!
//! Every connection write is asynchronous (Q169; connections.md §1): it is owed on the node's one
//! ordered writer queue, retried with backoff until it lands; no open, dial, accept or read waits
//! for it; the held projection takes a fact once its write lands.
//!
//! Two paths arm the one pending direct-recovery attempt per destination birth (connections.md
//! §9.4, Q7): a latest Direct Failed row in `held`, and a connection accepted from the exact
//! destination birth this node reaches through its own Proxy (R-CN1,
//! [`rdm_transport::AcceptTrigger`], never a row). An accept arriving while an item is pending
//! coalesces into it and wakes the reconciler, which dials this node's own data-plane connection
//! once the series' reconnect backoff from its latest Failed row has ended (§9.3), never earlier.
//! Only that dial's Direct Connected owes the Proxy's delete; a failed dial is the series'
//! next Failed row and the attempt waits for its backoff; an attempt toward a birth the topology view no
//! longer holds is dropped undialled (§9.5 step 1).

use crate::storage::ConnectionsStorage;
/// The direct-recovery reconciler tick and attempt types (`connections.md` §9.4).
pub use crate::connection_reconnect::{RECONCILER_TICK, RecoveryAttempt, RecoveryDial};
use rdm_core::connections::{ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
use rdm_core::reconnect::{first_failure, owed_direct_restored};
use rdm_core::{IncarnationId, PathName};
use rdm_transport::{ConnectionObserver, ResolvedNode};
use std::sync::{Arc, Mutex};
use std::time::Duration;

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

/// Makes an owed row from the held projection, inside the write gate, naming this node as it is at
/// that moment (the source end the held projection's own source was just set to); `None` when the
/// fact no longer stands (a drop whose connection is no longer an open entry).
type Build = Box<dyn FnOnce(&ConnectionsWriter, ConnectionEnd) -> Option<NodeConnection> + Send>;

/// What an owed row records, for the span its write is named on.
enum Origin {
    /// A fact observed of a connection: where (`dial`) and the peer endpoint it was observed with.
    Observed { origin: &'static str, peer: String },
    /// A drop of an open entry (`node.connection.delete`, an invalid carrier connection).
    Drop { closed_entry: rdm_core::ConnectionId, reason: String },
}

/// One owed connection write on the node's one ordered queue (Q169): how its row is built, what it
/// records, and the span of the call that owed it, which its write's span is a child of.
struct Owed {
    build: Build,
    origin: Origin,
    parent: tracing::Span,
}

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
    /// The last stamp this writer gave a row, or the stamp of a row it recorded
    /// ([`ConnectionsWriter::record`]) when that is later: every row it writes after carries a
    /// later one, so two facts in one millisecond never tie (the held projection keeps one stamp
    /// per key, and a Failed row wins a tie).
    last_stamp: Mutex<u64>,
    /// Owed writes queued and not yet landed; [`ConnectionsWriter::observations_landed`] waits for
    /// this to reach zero, `idle` wakes it when a write finishes.
    in_flight: std::sync::atomic::AtomicUsize,
    idle: tokio::sync::Notify,
    /// The node's one ordered writer queue (Q169): every connection write in the order it was owed;
    /// one consumer builds, stamps, writes (retrying with backoff until it lands) and applies them
    /// one at a time, so rows land in queue order and the index member of a pair is the last owed.
    queue: tokio::sync::mpsc::UnboundedSender<Owed>,
    /// The one write gate: held across every build, durable write attempt and apply, whether it is
    /// an observation, a [`ConnectionsWriter::record`] or a delete settlement, and let go
    /// before a refused write's backoff; and by a start until it holds the name its join answer
    /// handed ([`ConnectionsWriter::hold_rows`]).
    write_gate: Arc<tokio::sync::Mutex<()>>,
    /// Per-node reconnect scheduler state (`crate::connection_reconnect`).
    reconnect: crate::connection_reconnect::ReconnectState,
    /// The Direct entry of the latest connection the data-plane client opened to each birth, by
    /// its endpoint and incarnation, as observed when it opened, whether its Connected row has
    /// landed or is still owed: what [`ConnectionsWriter::live_routes`] reads against the client's
    /// pool, which says what is open now.
    opened: Mutex<std::collections::HashMap<(iroh::PublicKey, IncarnationId), NodeConnection>>,
}

/// The local OS clock: a writer orders its own rows by it and ages its owed deletes by it. No
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
        ConnectionState::Refused => "refused",
    }
}

impl ConnectionsWriter {
    /// A writer for `own`, over its storage, holding `held` (shared with the route resolver). A birth
    /// holds no connection (Q227): `held` is complete from here, its own source `own`, and nothing is
    /// read from the storage, now or later.
    pub fn new(own: ConnectionEnd, storage: Arc<dyn ConnectionsStorage>, held: Arc<Mutex<ConnectionsHeld>>) -> Self {
        {
            let mut h = held.lock().unwrap();
            h.set_own_source(own.name.clone());
            h.mark_complete();
        }
        let runtime = tokio::runtime::Handle::current();
        let (queue, mut owed) = tokio::sync::mpsc::unbounded_channel::<Owed>();
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
            reconnect: crate::connection_reconnect::ReconnectState::default(),
            opened: Mutex::new(std::collections::HashMap::new()),
        });
        let weak = Arc::downgrade(&inner);
        runtime.spawn(async move {
            while let Some(item) = owed.recv().await {
                let Some(inner) = weak.upgrade() else { break };
                ConnectionsWriter { inner }.write_owed(item).await;
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
    /// (its topology view's, R12): from then on every row this writer writes names it as the source
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

    /// Owed connection writes that have not yet landed. The raw log, the index, the held projection
    /// and the row's span follow when its write lands.
    pub fn in_flight(&self) -> usize {
        self.inner.in_flight.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Resolves once every connection write owed before the call has landed: the moment after which
    /// the index and the evidence hold every fact this writer was handed. For cells that read the
    /// store after a write; no open, dial, accept or read waits on it (Q169).
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

    /// Record one fact: the raw log first, then the index, then the held projection.
    pub async fn record(&self, row: NodeConnection) -> Result<(), String> {
        let _gate = self.inner.write_gate.lock().await;
        self.own_now();
        self.persist(&row).await?;
        // A row this writer writes after this one is stamped later than it.
        {
            let mut last = self.inner.last_stamp.lock().unwrap();
            *last = (*last).max(row.logged_at_ms);
        }
        let pair = (row.destination.name.clone(), row.alpn.clone());
        let _ = self.inner.held.lock().unwrap().apply(row);
        self.sync_recovery(&pair.0, &pair.1);
        Ok(())
    }

    /// Owe a row (Connected, Disconnected, Failed, or Refused) from the data plane's `Connections`
    /// surface (Q169): queued and never awaited; written, stamped with this writer's strictly
    /// increasing stamp, retried until it lands and applied once it has.
    pub fn record_data_plane(&self, row: NodeConnection) {
        self.owe_row(rdm_transport::connections::owed(row));
    }

    fn owe_row(&self, build: rdm_transport::connections::OwedRow) {
        let held = self.inner.held.clone();
        self.owe(
            Box::new(move |_, own| {
                let mut row = build(&held.lock().unwrap())?;
                if row.state != ConnectionState::Refused {
                    row.source = own;
                }
                Some(row)
            }),
            Origin::Observed { origin: "dial", peer: String::new() },
        );
    }

    /// Queue one owed write behind every write owed before it.
    fn owe(&self, build: Build, origin: Origin) {
        self.inner.in_flight.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let _ = self.inner.queue.send(Owed { build, origin, parent: tracing::Span::current() });
    }

    /// The connections this node holds now (R-CR1): its own latest Direct entries that are
    /// `connected` and its own active Proxy entries, under the name it holds, answered from what is
    /// held: a write owed and not yet landed is not held, and no read waits for one (Q169).
    /// Ordered by destination, Direct first.
    pub async fn held_routes(&self) -> Vec<NodeConnection> {
        self.held_connections().await.routes
    }

    /// [`Self::held_routes`] with the name the node holds them under and whether its held set is
    /// complete: what `node.connection.get` answers.
    pub async fn held_connections(&self) -> HeldConnections {
        let own = self.own_now();
        let held = self.inner.held.lock().unwrap();
        HeldConnections { name: own.name, complete: held.is_complete(), routes: open_routes(&held) }
    }

    /// The connections this node has open now (R-CR1; Q169), whatever their rows: each Direct the
    /// data-plane client `data_plane` pools to a birth it observed opening, and each Direct or Proxy
    /// `registry` holds active, every one by the minted id its entry has or will have when its row
    /// lands. Directs first, then Proxies, each by destination. What a rename and a leave decide
    /// from: a Connected row still owed is no connection closed.
    pub fn live_routes(&self, data_plane: Option<&rdm_transport::NodeRpcClient>, registry: Option<&rdm_transport::connections::ConnectionsRegistry>) -> Vec<NodeConnection> {
        let mut live: Vec<NodeConnection> = Vec::new();
        if let Some(client) = data_plane {
            let opened = self.inner.opened.lock().unwrap();
            for key in client.pooled() {
                if let Some(entry) = key.incarnation.and_then(|inc| opened.get(&(key.peer, inc))) {
                    live.push(entry.clone());
                }
            }
        }
        if let Some(registry) = registry {
            live.extend(registry.open_rows());
        }
        live.sort_by_key(|r| (r.kind == ConnectionKind::Proxy, r.destination.name.clone(), r.alpn.clone()));
        let mut seen = std::collections::HashSet::new();
        live.retain(|r| seen.insert(r.id.clone()));
        live
    }

    /// `node.connection.delete`'s drops (R-CR3): each of `routes` (connections this node had open,
    /// just cut) owes one `disconnected` entry with its own id and `reason`, naming the pair it
    /// closes as that pair landed (a rename that lands before the drop does not move it), queued
    /// behind every write owed before it and applied once it lands (Q169). A route whose open entry
    /// is no longer the latest of its pair when its drop is written (a newer entry landed meanwhile)
    /// writes no drop: the newer entry is the pair's state. Returns the ids of the routes cut.
    pub fn write_drops(&self, routes: &[NodeConnection], reason: &str) -> Vec<rdm_core::ConnectionId> {
        for route in routes {
            let build = rdm_transport::connections::owed_drop(route, reason);
            let held = self.inner.held.clone();
            self.owe(
                Box::new(move |_, _| build(&held.lock().unwrap())),
                Origin::Drop { closed_entry: route.id.clone(), reason: reason.to_string() },
            );
        }
        routes.iter().map(|r| r.id.clone()).collect()
    }

    /// The entry of the data-plane connection open now to the birth `destination`, as it opened,
    /// its row landed or still owed (Q169).
    pub fn opened_to(&self, destination: &ConnectionEnd) -> Option<rdm_core::ConnectionId> {
        let opened = self.inner.opened.lock().unwrap();
        opened.values().find(|e| (&e.destination.node_id, &e.destination.incarnation) == (&destination.node_id, &destination.incarnation)).map(|e| e.id.clone())
    }

    /// The data-plane connection to the birth at `peer` and `incarnation` closed: it leaves the
    /// connections opened, so [`Self::live_routes`] no longer reads it (Q169).
    pub fn direct_closed(&self, peer: &iroh::PublicKey, incarnation: &IncarnationId) {
        self.inner.opened.lock().unwrap().remove(&(*peer, incarnation.clone()));
    }

    /// The raw log first, then the index; an error names the storage's own refusal.
    async fn persist(&self, row: &NodeConnection) -> Result<(), String> {
        self.inner.storage.append_history(row).await.map_err(|e| e.to_string())?;
        self.inner.storage.put_connection(row).await.map_err(|e| e.to_string())
    }

    /// Write `row` until it lands (Q169): the raw log, then the index, each step attempted again
    /// after a refusal on the delivery schedule (the publish cadence, doubling with jitter, capped
    /// at the staleness floor), so a step that landed is never written twice. Each refusal is named
    /// on its own span, a child of the row's, with its attempt and the delay before the next.
    /// `gate` is the write gate, held for one attempt and let go before each backoff, so a start's
    /// hold and a settlement never wait behind a refused write; the next attempt takes it again.
    /// Returns the attempts it took and the gate, held, for the row's apply; or `None` when every
    /// handle on this writer is gone.
    async fn land<'a>(&'a self, row: &NodeConnection, span: &tracing::Span, mut gate: tokio::sync::MutexGuard<'a, ()>) -> Option<(u32, tokio::sync::MutexGuard<'a, ()>)> {
        let pacing = rdm_transport::delivery::Pacing::from_cadences();
        let mut appended = false;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let refusal = if appended {
                self.inner.storage.put_connection(row).await.err()
            } else {
                match self.inner.storage.append_history(row).await {
                    Ok(()) => {
                        appended = true;
                        self.inner.storage.put_connection(row).await.err()
                    }
                    Err(e) => Some(e),
                }
            };
            let Some(refusal) = refusal else { return Some((attempt, gate)) };
            drop(gate);
            let next = pacing.after(attempt);
            tracing::info_span!(
                parent: span,
                "rdm.node_admin.connection.reject.via-refused-write",
                entry = %row.id,
                source = %row.source.name,
                destination = %row.destination.name,
                destination_node_id = %row.destination.node_id,
                kind = kind_name(row.kind),
                state = state_name(row.state),
                reason = %row.reason.clone().unwrap_or_default(),
                logged_at_ms = row.logged_at_ms,
                step = if appended { "index" } else { "history" },
                attempt,
                next_in_ms = next.as_millis() as u64,
                refusal = %refusal,
            )
            .in_scope(|| tracing::info!("the connection write was refused by storage; it stays owed and is written again after its backoff"));
            tokio::time::sleep(next).await;
            // Only the queue's own handle is left: the node that owed the write is gone.
            if std::sync::Arc::strong_count(&self.inner) <= 1 {
                return None;
            }
            gate = self.inner.write_gate.lock().await;
        }
    }

    /// The latest Direct row this node holds toward `destination` on the default ALPN, if any.
    fn latest_direct(&self, destination: &ConnectionEnd) -> Option<NodeConnection> {
        self.latest_direct_for(destination, rdm_core::connections::ALPN)
    }

    /// The latest Direct row this node holds toward `destination` on `alpn`, if any.
    pub fn latest_direct_for(&self, destination: &ConnectionEnd, alpn: &[u8]) -> Option<NodeConnection> {
        self.inner.held.lock().unwrap().own_latest_directs().into_iter().find(|d| d.destination.name == destination.name && d.alpn.as_slice() == alpn).cloned()
    }

    fn direct(own: ConnectionEnd, destination: ConnectionEnd, state: ConnectionState, reason: Option<String>) -> NodeConnection {
        NodeConnection {
            id: rdm_core::ConnectionId::mint(),
            source: own,
            destination,
            kind: ConnectionKind::Direct,
            alpn: rdm_core::connections::ALPN.to_vec(),
            state,
            carrier: None,
            recovery: None,
            reason,
            logged_at_ms: local_now_ms(),
        }
    }

    /// The Proxy deletes this node owes now (connections.md §10), derived from the held
    /// projection's two latest rows per pair: never a queue.
    pub fn owed(&self) -> Vec<NodeConnection> {
        owed_direct_restored(&self.inner.held.lock().unwrap(), local_now_ms())
    }

    /// Settle every Proxy delete this node owes: each is written durably as
    /// `Disconnected(direct-restored)` and applied to the held projection once its write lands,
    /// so new calls cut back to Direct only then. A refused write is named and leaves the
    /// obligation standing for the next attempt. Returns how many landed.
    pub async fn settle_owed(&self) -> Result<usize, String> {
        let _gate = self.inner.write_gate.lock().await;
        self.own_now();
        let at = self.stamp();
        let owed = owed_direct_restored(&self.inner.held.lock().unwrap(), at);
        let mut landed = 0;
        for row in owed {
            let span = tracing::info_span!(
                "rdm.node_admin.connection.delete.via-direct-restored",
                entry = %row.id,
                source = %row.source.name,
                destination = %row.destination.name,
                carrier = %row.carrier.as_ref().map(|c| c.name.to_string()).unwrap_or_default(),
                kind = kind_name(row.kind),
                state = state_name(row.state),
                reason = %row.reason.clone().unwrap_or_default(),
                outcome = tracing::field::Empty,
                detail = tracing::field::Empty,
            );
            match self.persist(&row).await {
                Ok(()) => {
                    let _ = self.inner.held.lock().unwrap().apply(row);
                    span.record("outcome", "landed");
                    span.in_scope(|| tracing::info!("the owed Proxy delete landed; new calls cut back to Direct"));
                    landed += 1;
                }
                Err(e) => {
                    span.record("outcome", "refused");
                    span.record("detail", e.as_str());
                    span.in_scope(|| tracing::info!("the owed Proxy delete write was refused; the Proxy stays effective and the obligation stands"));
                    return Err(e);
                }
            }
        }
        Ok(landed)
    }

    /// [`Self::settle_owed`], as the integration cells name it.
    pub async fn disconnect_owed(&self) -> Result<usize, String> {
        self.settle_owed().await
    }

    pub(crate) fn reconnect_state(&self) -> &crate::connection_reconnect::ReconnectState {
        &self.inner.reconnect
    }

    pub(crate) fn runtime_handle(&self) -> &tokio::runtime::Handle {
        &self.inner.runtime
    }

    /// The node's one connections reconciler (connections.md §9.4, §10): each `every`, or at once
    /// when an accept armed an attempt, it attempts every owed delete again and dials every
    /// pending recovery attempt whose backoff has ended through `dial`, this node's own data plane
    /// (none on a node with no data plane). Derived from the rows each time; the task ends with
    /// the runtime.
    pub fn spawn_reconciler(&self, every: Duration, dial: Option<RecoveryDial>) -> tokio::task::JoinHandle<()> {
        crate::connection_reconnect::spawn_reconciler(self, every, dial)
    }

    /// The pending recovery attempts, each with when it is due.
    pub fn recovery_pending(&self) -> Vec<RecoveryAttempt> {
        crate::connection_reconnect::recovery_pending(self)
    }

    /// Arm (or spend) the one pending recovery attempt of the pair `(destination, alpn)` of a row
    /// just applied, from this node's own latest Direct row of that pair in `held` (connections.md
    /// §9.4, §9.5, Q7): another pair's attempt is never touched.
    fn sync_recovery(&self, destination: &PathName, alpn: &[u8]) {
        crate::connection_reconnect::sync_recovery(self, destination, alpn);
    }

    /// An accept on `alpn` from `peer` (connections.md §9.4, §10, §13 G): writes nothing (R-CN1),
    /// and if `peer` is the exact birth this node's own active Proxy on `alpn` names, arms or
    /// coalesces the one recovery attempt and wakes the reconciler.
    pub fn accepted_on_alpn(&self, peer: &rdm_transport::connections::Birth, alpn: &[u8]) {
        crate::connection_reconnect::accepted_on_alpn(self, peer, alpn, &peer.node_id.to_string());
    }

    /// A strictly increasing stamp for this writer's rows.
    fn stamp(&self) -> u64 {
        let mut last = self.inner.last_stamp.lock().unwrap();
        *last = local_now_ms().max(*last + 1);
        *last
    }

    /// An observed fact: `build` makes the row from the held projection when the writer takes it,
    /// queued behind every write owed before it (Q169). The row is written until it lands and is
    /// applied to the held projection only once it has; a row built from the held projection (a
    /// failing series' next ordinal) sees every earlier owed row already applied. A Direct
    /// Connected settles the Proxy delete it may owe once its own write has landed.
    fn observed(&self, build: impl FnOnce(&Self, ConnectionEnd) -> NodeConnection + Send + 'static, origin: &'static str, node: &ResolvedNode) {
        self.owe(Box::new(move |w, own| Some(build(w, own))), Origin::Observed { origin, peer: node.endpoint_id.fmt_short().to_string() });
    }

    /// Write one owed row until it lands, each attempt and the apply inside the write gate, then
    /// settle the delete it may owe. The queue's consumer takes the next row only once this
    /// one has landed, so rows land in queue order.
    async fn write_owed(&self, owed: Owed) {
        let w = self;
        let Owed { build, origin, parent } = owed;
        {
            let gate = w.inner.write_gate.lock().await;
            let own = w.own_now();
            let Some(mut row) = build(w, own) else {
                drop(gate);
                drop(parent);
                w.inner.in_flight.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
                w.inner.idle.notify_waiters();
                return;
            };
            row.logged_at_ms = w.stamp();
            let settles = row.kind == ConnectionKind::Direct && row.state == ConnectionState::Connected;
            let span = match &origin {
                Origin::Observed { origin, peer } => tracing::info_span!(
                    parent: &parent,
                    "rdm.node_admin.connection.update.via-observed",
                    entry = %row.id,
                    source = %row.source.name,
                    destination = %row.destination.name,
                    carrier = %row.carrier.as_ref().map(|c| c.name.to_string()).unwrap_or_default(),
                    peer = %peer,
                    kind = kind_name(row.kind),
                    state = state_name(row.state),
                    reason = %row.reason.clone().unwrap_or_default(),
                    logged_at_ms = row.logged_at_ms,
                    recovery_round = tracing::field::Empty,
                    attempt_ordinal = tracing::field::Empty,
                    origin = *origin,
                    attempts = tracing::field::Empty,
                    outcome = tracing::field::Empty,
                ),
                Origin::Drop { closed_entry, reason } => tracing::info_span!(
                    parent: &parent,
                    "rdm.node_admin.connection.update.via-connection-delete",
                    source = %row.source.name,
                    destination = %row.destination.name,
                    carrier = %row.carrier.as_ref().map(|c| c.name.to_string()).unwrap_or_default(),
                    kind = kind_name(row.kind),
                    state = state_name(row.state),
                    closed_entry = %closed_entry,
                    entry = %row.id,
                    reason = %reason,
                    attempts = tracing::field::Empty,
                    outcome = tracing::field::Empty,
                ),
            };
            if let Some(r) = row.recovery {
                span.record("recovery_round", r.recovery_round).record("attempt_ordinal", r.attempt_ordinal);
            }
            let landed = match w.land(&row, &span, gate).await {
                Some((attempts, gate)) => {
                    span.record("attempts", attempts);
                    let _ = w.inner.held.lock().unwrap().apply(row.clone());
                    w.sync_recovery(&row.destination.name, &row.alpn);
                    span.record("outcome", "landed");
                    drop(gate);
                    true
                }
                None => {
                    span.record("outcome", "writer-gone");
                    false
                }
            };
            span.in_scope(|| match origin {
                Origin::Observed { .. } => tracing::info!("one connection fact this node observed"),
                Origin::Drop { .. } => tracing::info!("the drop of a connection this node cut"),
            });
            if settles && landed {
                let _ = tracing::Instrument::instrument(w.settle_owed(), span.clone()).await;
            }
            drop(span);
            // The span of the call that owed the write goes before the write is counted landed:
            // once `observations_landed` resolves, the writer holds no span of a landed write.
            drop(parent);
            w.inner.in_flight.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
            w.inner.idle.notify_waiters();
        }
    }
}

impl ConnectionObserver for ConnectionsWriter {
    fn direct_connected(&self, node: &ResolvedNode) {
        let destination = end_of(node);
        // The entry's id is minted as the connection opens: the connection is open now under it,
        // and its row, owed, lands under it (Q169).
        let opened = Self::direct(self.own(), destination.clone(), ConnectionState::Connected, None);
        let id = opened.id.clone();
        self.inner.opened.lock().unwrap().insert((node.endpoint_id, node.incarnation.clone()), opened);
        self.observed(move |_, own| rdm_core::connections::NodeConnection { id, ..Self::direct(own, destination, ConnectionState::Connected, None) }, "dial", node);
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
                            id: rdm_core::ConnectionId::mint(),
                            source: own,
                            destination,
                            recovery: Some(rdm_core::connections::DirectRecovery { recovery_round: r.recovery_round, attempt_ordinal: r.attempt_ordinal.saturating_add(1) }),
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
        self.direct_closed(&node.endpoint_id, &node.incarnation);
        let destination = end_of(node);
        let reason = reason.to_string();
        self.observed(move |_, own| Self::direct(own, destination, ConnectionState::Disconnected, Some(reason)), "dial", node);
    }
}

impl rdm_transport::AcceptTrigger for ConnectionsWriter {
    /// An accept writes nothing (R-CN1). From the exact birth this node's own active Proxy names,
    /// with no Direct to it Connected, it arms one recovery attempt (or finds it armed) and wakes
    /// the reconciler; from another birth at that path it arms nothing, by name; from a node this
    /// node reaches by no Proxy it is an accept like any other.
    fn accepted(&self, node: &ResolvedNode) {
        crate::connection_reconnect::accepted_resolved(self, node);
    }
}

#[async_trait::async_trait]
impl rdm_transport::CarrierConnections for ConnectionsWriter {
    /// The carrier's dial to `node` just failed: the connection is Failed by that very outcome, and the
    /// text is the one the fact carries, named without waiting for the fact's write.
    async fn connection_after_dial(&self, node: &ResolvedNode, reason: &str) -> Option<String> {
        Some(format!("{} -> {} Direct {} ({reason})", self.own().name, node.name, state_name(ConnectionState::Failed)))
    }

    async fn connection_not_active(&self, target: &rdm_core::NodeId) -> Option<String> {
        // Answered from what is held: a fact owed and not yet landed is not held, and no read waits
        // for a write (Q169). A dial's own outcome is named by `connection_after_dial`.
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

#[async_trait::async_trait]
impl rdm_transport::connections::ConnectionsSink for ConnectionsWriter {
    fn own_birth(&self) -> rdm_transport::connections::Birth {
        let own = self.own();
        rdm_transport::connections::Birth {
            name: own.name,
            node_id: own.node_id,
            incarnation: own.incarnation.unwrap_or_else(IncarnationId::mint),
        }
    }

    fn held(&self) -> Arc<Mutex<ConnectionsHeld>> {
        self.held()
    }

    fn record_row(&self, row: rdm_transport::connections::OwedRow) {
        self.owe_row(row);
    }

    fn record_drops(&self, routes: &[NodeConnection], reason: &str) {
        self.write_drops(routes, reason);
    }

    fn accepted(&self, peer: &rdm_transport::connections::Birth, alpn: &[u8]) {
        self.accepted_on_alpn(peer, alpn);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::MemoryConnectionsStorage;
    use rdm_core::{EndpointId, NodeId};

    /// CONTRACT: docs/architecture/connections.md:282
    /// "A node holds the connection facts it writes, each from the moment its write lands: its latest Direct per
    /// `(destination, alpn)`"
    /// CONTRACT: docs/architecture/connections.md:530
    /// "Node-base runs one scheduler/reconciler per node process, with at most one pending direct-reconnect item per:"
    #[tokio::test]
    async fn opened_to_direct_closed_latest_direct_for_reconnect_state_and_runtime_handle_track_live_and_held_facts() {
        let own = ConnectionEnd {
            name: "mesh1.rpc.1".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: Some(IncarnationId::mint()),
        };
        let storage = Arc::new(MemoryConnectionsStorage::default());
        let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
        let writer = ConnectionsWriter::new(own, storage, held);

        // `runtime_handle` returns the runtime handle bound at construction, and `reconnect_state` starts empty.
        let _rt_handle = writer.runtime_handle();
        let _reconnect = writer.reconnect_state();
        assert!(writer.recovery_pending().is_empty());

        let secret = iroh::SecretKey::from_bytes(&[7u8; 32]);
        let peer_key = secret.public();
        let peer_inc = IncarnationId::mint();
        let peer_node = ResolvedNode {
            name: "mesh1.rpc.2".parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: peer_inc.clone(),
            endpoint_id: peer_key,
            transport_addr: "127.0.0.1:19999".parse().unwrap(),
        };
        let dest_end = end_of(&peer_node);

        assert_eq!(writer.opened_to(&dest_end), None);
        assert_eq!(writer.latest_direct_for(&dest_end, rdm_core::connections::ALPN), None);

        // Observing `direct_connected` immediately populates `opened_to` and queues the Connected row.
        writer.direct_connected(&peer_node);
        let opened_id = writer.opened_to(&dest_end).expect("opened_to records the newly opened connection id");
        writer.observations_landed().await;

        let latest = writer
            .latest_direct_for(&dest_end, rdm_core::connections::ALPN)
            .expect("latest_direct_for returns the landed Direct row on its ALPN");
        assert_eq!(latest.id, opened_id);
        assert_eq!(latest.state, ConnectionState::Connected);
        assert_eq!(writer.latest_direct_for(&dest_end, b"other/alpn"), None);

        // `direct_closed` removes the open connection from `opened_to`.
        writer.direct_closed(&peer_key, &peer_inc);
        assert_eq!(writer.opened_to(&dest_end), None);

        // Observing `direct_failed` lands a Failed row and arms `reconnect_state`.
        let _ = EndpointId(peer_key.to_string());
        writer.direct_failed(&peer_node, "dial refused");
        writer.observations_landed().await;
        let failed = writer
            .latest_direct_for(&dest_end, rdm_core::connections::ALPN)
            .expect("latest_direct_for returns the Failed row");
        assert_eq!(failed.state, ConnectionState::Failed);
        assert_eq!(writer.recovery_pending().len(), 1);
    }
}


