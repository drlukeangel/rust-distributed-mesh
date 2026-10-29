//! Data-plane `Connections` (`i148`): one endpoint, RDM owns every registered `Alpn` and the
//! accept loop, with `node.connection.create` and the intent handshake on the first bi-stream.
//!
//! `iroh` never crosses this surface: [`SendStream`] and [`RecvStream`] wrap QUIC bi-streams and
//! surface [` finish`](SendStream::finish), [`reset`](SendStream::reset) and [`stop`](RecvStream::stop)
//! with their typed [`StreamError`] and [`Closed`] outcomes.

use crate::resolve::{NodeResolver, NodeTarget};
use rdm_core::connections::{
    CarrierPolicy, ConnectionEnd, ConnectionKind as CoreKind, ConnectionState, ConnectionsHeld,
    DirectRecovery, EffectiveRoute, INVALID_CARRIER_EDGE_LOST, NodeConnection,
    carrier_order, resolve_for_carriers,
};
use rdm_core::reconnect::first_failure_for;
use rdm_core::{ConnectionId, IncarnationId, NodeId, NodeKind, PathName};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::sync::{mpsc, watch};

const CLOSE_CODE_PEER_LEFT: u32 = 400;
const CLOSE_CODE_CONNECTIONS_DELETED: u32 = 410;
const CLOSE_CODE_CARRIER_GONE: u32 = 411;
const CLOSE_CODE_REFUSED: u32 = 412;
const MAX_CONTROL_FRAME: usize = 64 * 1024;

/// A data-plane ALPN registered with RDM when the endpoint binds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Alpn {
    /// The ALPN wire bytes.
    pub name: Vec<u8>,
    /// Whether this protocol may travel over a Proxy when a Direct dial provably sent nothing.
    pub proxy: bool,
    /// The node kinds eligible to carry a Proxy for this protocol when `proxy` is `true`.
    pub carriers: Vec<NodeKind>,
}

impl Alpn {
    /// The [`CarrierPolicy`] this registration states.
    pub fn carrier_policy(&self) -> CarrierPolicy {
        if self.proxy {
            if let Some(&carrier_kind) = self.carriers.first() {
                return CarrierPolicy::Proxy { carrier_kind };
            }
        }
        CarrierPolicy::NoProxy
    }
}

/// Validate a set of application [`Alpn`] registrations before binding the endpoint.
///
/// Refuses an empty ALPN name, a duplicate ALPN, or RDM's own control-plane ALPNs
/// (`rdm_transport::ALPN` and `iroh_gossip::ALPN`; `node-connections-admit.md`).
pub fn validate_alpns(alpns: &[Alpn]) -> Result<(), String> {
    let mut seen = HashSet::new();
    for a in alpns {
        if a.name.is_empty() {
            return Err("an Alpn registration cannot have an empty name".into());
        }
        if a.name.as_slice() == crate::ALPN {
            return Err(format!(
                "alpn {:?} is RDM's own Node RPC ALPN and cannot be registered as an application Alpn",
                String::from_utf8_lossy(&a.name)
            ));
        }
        if a.name.as_slice() == iroh_gossip::ALPN {
            return Err(format!(
                "alpn {:?} is RDM's own gossip ALPN and cannot be registered as an application Alpn",
                String::from_utf8_lossy(&a.name)
            ));
        }
        if !seen.insert(a.name.clone()) {
            return Err(format!(
                "duplicate Alpn registration for {:?}",
                String::from_utf8_lossy(&a.name)
            ));
        }
    }
    Ok(())
}

/// One exact process birth: its path, logical node id and incarnation.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Birth {
    /// The birth's `path.name`.
    pub name: PathName,
    /// The birth's logical node id.
    pub node_id: NodeId,
    /// The birth's process incarnation.
    pub incarnation: IncarnationId,
}

impl Birth {
    /// Convert this birth to a [`ConnectionEnd`].
    pub fn to_end(&self) -> ConnectionEnd {
        ConnectionEnd {
            name: self.name.clone(),
            node_id: self.node_id.clone(),
            incarnation: Some(self.incarnation.clone()),
        }
    }
}

/// How a [`Connection`] reaches its peer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConnectionKind {
    /// A direct connection between source and destination.
    Direct,
    /// A two-hop tunnel through `via`.
    Proxy {
        /// The carrier birth this tunnel traverses.
        via: Birth,
    },
}

/// Why a [`Connection`] closed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Closed {
    /// The peer closed the connection or deleted.
    PeerLeft,
    /// Closed by `node.connection.delete`.
    ConnectionsDeleted,
    /// The carrier leg of a Proxy tunnel closed.
    CarrierGone,
    /// The peer refused the connection.
    Refused {
        /// The refusal reason.
        reason: String,
    },
    /// Closed with an application reset code.
    Reset {
        /// The reset code.
        code: u32,
    },
}

/// An error on a [`SendStream`] or [`RecvStream`].
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum StreamError {
    /// The peer reset its send direction with `code`.
    #[error("stream reset with code {code}")]
    Reset {
        /// The application reset code.
        code: u32,
    },
    /// The peer stopped its receive direction with `code`.
    #[error("stream stopped with code {code}")]
    Stopped {
        /// The application stop code.
        code: u32,
    },
    /// The stream was already finished or closed.
    #[error("stream already finished")]
    Finished,
    /// The underlying connection closed.
    #[error("connection closed: {0:?}")]
    Closed(Closed),
}

impl StreamError {
    /// Extract a typed [`StreamError`] from an [`io::Error`] produced by [`AsyncRead`] or [`AsyncWrite`].
    pub fn from_io(err: &io::Error) -> Option<Self> {
        err.get_ref()
            .and_then(|inner| inner.downcast_ref::<StreamError>())
            .cloned()
    }

    fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::BrokenPipe, self)
    }
}

/// Why [`Node::connections`](ConnectionsRegistry::connections) or [`Connections::open`] failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConnectError {
    /// The requested ALPN was not registered at bind.
    #[error("unregistered alpn {:?}", String::from_utf8_lossy(alpn))]
    UnregisteredAlpn {
        /// The unregistered ALPN bytes.
        alpn: Vec<u8>,
    },
    /// `connections(alpn)` was already called for this ALPN.
    #[error("connections for alpn {:?} already taken", String::from_utf8_lossy(alpn))]
    AlreadyTaken {
        /// The ALPN bytes whose `Connections` handle was already taken.
        alpn: Vec<u8>,
    },
    /// No active Direct or valid Proxy route exists to the destination on this ALPN.
    ///
    /// `direct` is `Some(reason)` when a Direct dial was made and ended not sent (`NotSent`'s
    /// reason) before every carrier refused, and `None` when nothing was dialled.
    #[error("no active route{}", direct.as_ref().map(|r| format!(" (direct dial not sent: {r})")).unwrap_or_default())]
    NoActiveRoute {
        /// The Direct dial's `NotSent` reason; `None` when no dial was made.
        direct: Option<String>,
    },
    /// The dial ended before anything was sent to the destination.
    #[error("not sent: {reason}")]
    NotSent {
        /// Why the dial could not be sent.
        reason: String,
    },
    /// The acceptor refused the connection at admission or intent handshake.
    #[error("refused: {reason}")]
    Refused {
        /// The acceptor's refusal reason.
        reason: String,
    },
    /// This birth's connections have been deleted (`node.connection.delete All`).
    #[error("connections deleted")]
    Deleted,
    /// The destination answered as another incarnation than `open` requested.
    #[error("wrong incarnation: held {held}, found {found}")]
    WrongIncarnation {
        /// The incarnation `open` targeted.
        held: IncarnationId,
        /// The incarnation that answered at the destination.
        found: IncarnationId,
    },
}

mod admit;
pub use admit::{Admitter, CertAdmitter, NoCertsAdmitter, RootSet};
use admit::{AdmitVerdict, PresentFrame};

/// One owed connection row (Q169; connections.md §1), built when the node's writer takes it from
/// its one ordered queue: from the held projection as it is then, every row owed before it already
/// applied once it landed, or `None` when the fact it would record no longer stands (a drop whose
/// connection is no longer the latest entry of its pair).
pub type OwedRow = Box<dyn FnOnce(&ConnectionsHeld) -> Option<NodeConnection> + Send>;

/// Where the data-plane `Connections` runtime records `NodeConnection` rows and reads the held projection.
#[async_trait::async_trait]
pub trait ConnectionsSink: Send + Sync {
    /// This node's current `Birth` (following its held name).
    fn own_birth(&self) -> Birth;
    /// The held projection, shared with route resolution.
    fn held(&self) -> Arc<Mutex<ConnectionsHeld>>;
    /// Owe one connection row (`Connected`, `Disconnected`, `Failed`, or `Refused`) on the node's
    /// one ordered writer queue (Q169): it is written, retried with backoff until it lands, and
    /// applied to the held projection once it has. The caller never waits for it.
    fn record_row(&self, row: OwedRow);
    /// Owe a `Disconnected` drop for each of `routes` with `reason`, each written only if its route
    /// is still an open entry of this node when the writer takes it ([`owed_drop`]).
    fn record_drops(&self, routes: &[NodeConnection], reason: &str) {
        for route in routes {
            self.record_row(owed_drop(route, reason));
        }
    }
    /// Notify the sink that an inbound Direct connection from `peer` on `alpn` was admitted
    /// (`connections.md` §9.4, §10, §13 G: a recovery trigger, never a Direct row).
    fn accepted(&self, _peer: &Birth, _alpn: &[u8]) {}
}

/// The source of the pair `route` opened while that pair is still open in `held`, found by the
/// entry's id and never by the name the node holds now, since a rename may land before the drop
/// does (Q169): a Direct's active entry as it landed, or this node's own active Proxy. A Proxy no
/// longer among the node's own because the node has taken another name since is named by `route`'s
/// own source: nothing the node writes under its new name is a newer entry of that pair.
pub fn open_pair_source(held: &ConnectionsHeld, route: &NodeConnection) -> Option<ConnectionEnd> {
    match route.kind {
        CoreKind::Direct => held.active_direct_by_id(&route.id).map(|e| e.source.clone()),
        CoreKind::Proxy => match held.own_active_proxies().into_iter().find(|r| r.id == route.id) {
            Some(e) => Some(e.source.clone()),
            None if held.own_source() != Some(&route.source.name) => Some(route.source.clone()),
            None => None,
        },
    }
}

/// The drop `route` owes with `reason`: built when the writer takes it, as a `Disconnected` entry
/// with its own id toward the route's destination under the source its pair landed with, or
/// nothing when `route` is no longer open then (a newer entry of its pair is its state).
pub fn owed_drop(route: &NodeConnection, reason: &str) -> OwedRow {
    let route = route.clone();
    let reason = reason.to_string();
    Box::new(move |held: &ConnectionsHeld| {
        open_pair_source(held, &route).map(|source| NodeConnection {
            id: ConnectionId::mint(),
            source,
            destination: route.destination.clone(),
            kind: route.kind,
            alpn: route.alpn.clone(),
            state: ConnectionState::Disconnected,
            carrier: route.carrier.clone(),
            recovery: None,
            reason: Some(reason),
            logged_at_ms: 0,
        })
    })
}

/// An owed row that is `row` as built at the call (a fact that reads nothing held).
pub fn owed(row: NodeConnection) -> OwedRow {
    Box::new(move |_: &ConnectionsHeld| Some(row))
}

/// An in-memory [`ConnectionsSink`] for standalone transport tests that do not boot a full `rdm-node`.
pub struct MemoryConnectionsSink {
    own: Mutex<Birth>,
    held: Arc<Mutex<ConnectionsHeld>>,
    history: Mutex<Vec<NodeConnection>>,
    clock: std::sync::atomic::AtomicU64,
}

impl MemoryConnectionsSink {
    /// Create a new in-memory sink for `own`.
    pub fn new(own: Birth) -> Self {
        let mut h = ConnectionsHeld::new();
        h.set_own_source(own.name.clone());
        h.mark_complete();
        Self {
            own: Mutex::new(own),
            held: Arc::new(Mutex::new(h)),
            history: Mutex::new(Vec::new()),
            clock: std::sync::atomic::AtomicU64::new(1),
        }
    }

    /// Update this node's birth (for example when simulating a restart at a new incarnation).
    pub fn set_own_birth(&self, birth: Birth) {
        self.held.lock().unwrap().set_own_source(birth.name.clone());
        *self.own.lock().unwrap() = birth;
    }

    /// Every connection row recorded in order.
    pub fn history(&self) -> Vec<NodeConnection> {
        self.history.lock().unwrap().clone()
    }
}

#[async_trait::async_trait]
impl ConnectionsSink for MemoryConnectionsSink {
    fn own_birth(&self) -> Birth {
        self.own.lock().unwrap().clone()
    }

    fn held(&self) -> Arc<Mutex<ConnectionsHeld>> {
        self.held.clone()
    }

    fn record_row(&self, row: OwedRow) {
        let built = row(&self.held.lock().unwrap());
        if let Some(row) = built {
            self.record(row);
        }
    }
}

impl MemoryConnectionsSink {
    /// Write and apply `row` at once (a memory sink applies synchronously), stamping it and naming
    /// this node as its source unless it is a `Refused` the acceptor writes for its dialer.
    pub fn record(&self, mut row: NodeConnection) -> NodeConnection {
        let now = self.clock.fetch_add(1, Ordering::SeqCst);
        row.logged_at_ms = now;
        if row.state != ConnectionState::Refused {
            row.source = self.own_birth().to_end();
        }
        let settles = row.kind == CoreKind::Direct && row.state == ConnectionState::Connected;
        self.history.lock().unwrap().push(row.clone());
        let _ = self.held.lock().unwrap().apply(row.clone());
        tracing::info_span!(
            "rdm.node_admin.connection.update.via-observed",
            entry = %row.id,
            source = %row.source.name,
            destination = %row.destination.name,
            carrier = %row.carrier.as_ref().map(|c| c.name.to_string()).unwrap_or_default(),
            kind = match row.kind {
                CoreKind::Direct => "direct",
                CoreKind::Proxy => "proxy",
            },
            state = match row.state {
                ConnectionState::Connected => "connected",
                ConnectionState::Disconnected => "disconnected",
                ConnectionState::Failed => "failed",
                ConnectionState::Refused => "refused",
            },
            reason = %row.reason.clone().unwrap_or_default(),
            origin = "dial",
            outcome = "landed",
        )
        .in_scope(|| tracing::info!("one connection fact this node observed"));
        if settles {
            let at = self.clock.fetch_add(1, Ordering::SeqCst);
            let owed = rdm_core::reconnect::owed_direct_restored(&self.held.lock().unwrap(), at);
            for ended in owed {
                self.history.lock().unwrap().push(ended.clone());
                let _ = self.held.lock().unwrap().apply(ended);
            }
        }
        row
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ControlIntent {
    Direct {
        id: ConnectionId,
        origin: Birth,
        destination: Birth,
    },
    Tunnel {
        id: ConnectionId,
        origin: Birth,
        destination: Birth,
        alpn: Vec<u8>,
        via: Option<Birth>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
enum ControlAnswer {
    Opened { peer: Birth },
    Refused { reason: String },
}

async fn write_control_msg<T: Serialize>(send: &mut iroh::endpoint::SendStream, msg: &T) -> Result<(), String> {
    let bytes = postcard::to_allocvec(msg).map_err(|e| e.to_string())?;
    let len = u32::try_from(bytes.len()).map_err(|_| "control frame too large".to_string())?;
    send.write_all(&len.to_be_bytes()).await.map_err(|e| e.to_string())?;
    send.write_all(&bytes).await.map_err(|e| e.to_string())?;
    Ok(())
}

async fn read_control_msg<T: for<'de> Deserialize<'de>>(recv: &mut iroh::endpoint::RecvStream) -> Result<Option<T>, String> {
    let mut len_buf = [0u8; 4];
    match recv.read(&mut len_buf[..1]).await.map_err(|e| e.to_string())? {
        None => return Ok(None),
        Some(0) => return Ok(None),
        Some(_) => {}
    }
    recv.read_exact(&mut len_buf[1..]).await.map_err(|e| e.to_string())?;
    let len = u32::from_be_bytes(len_buf) as usize;
    if len > MAX_CONTROL_FRAME {
        return Err(format!("control frame length {len} exceeds {MAX_CONTROL_FRAME}"));
    }
    let mut buf = vec![0u8; len];
    recv.read_exact(&mut buf).await.map_err(|e| e.to_string())?;
    let msg = postcard::from_bytes(&buf).map_err(|e| e.to_string())?;
    Ok(Some(msg))
}

struct RegisteredSlot {
    alpn: Alpn,
    taken: bool,
    accept_rx: Option<mpsc::UnboundedReceiver<Connection>>,
}

struct ActiveEntry {
    conn: iroh::endpoint::Connection,
    closed_tx: watch::Sender<Option<Closed>>,
    row: Option<NodeConnection>,
    _keep_alive: Option<Arc<ConnectionInner>>,
}

/// The node's registry of registered data-plane [`Alpn`]s and their active connections.
#[derive(Clone)]
pub struct ConnectionsRegistry {
    inner: Arc<RegistryInner>,
}

/// One-shot fault injected on a destination's registered ALPN handler after admission and
/// reading `ControlIntent`, before answering `Opened` (`Q62`, `connections.md` §13 E).
#[cfg(feature = "testkit-gossip")]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PreOpenedFault {
    /// Write raw application bytes on the control stream instead of a framed `Opened` answer,
    /// then finish the control stream.
    ApplicationBytes(Vec<u8>),
    /// Close the control stream at EOF without writing `Opened`.
    Eof,
    /// Hold the control stream open past `staleness_floor()` without writing `Opened` so the
    /// handshake lapses.
    HandshakeLapse,
}

struct RegistryInner {
    endpoint: iroh::Endpoint,
    resolver: Arc<dyn NodeResolver>,
    sink: Arc<dyn ConnectionsSink>,
    admitter: Mutex<Arc<dyn Admitter>>,
    /// The root set the admitter judges on: one holder, replaced at runtime by `node.roots.update`.
    roots: RootSet,
    member_cert: Mutex<Vec<u8>>,
    slots: Mutex<HashMap<Vec<u8>, RegisteredSlot>>,
    accept_txs: HashMap<Vec<u8>, mpsc::UnboundedSender<Connection>>,
    active: Mutex<HashMap<ConnectionId, ActiveEntry>>,
    carrier_legs: Mutex<Vec<(iroh::endpoint::Connection, iroh::endpoint::Connection)>>,
    deleted: AtomicBool,
    #[cfg(feature = "testkit-gossip")]
    blocked_direct_dials: Mutex<std::collections::HashSet<(PathName, Vec<u8>)>>,
    #[cfg(feature = "testkit-gossip")]
    pre_opened_faults: Mutex<HashMap<Vec<u8>, PreOpenedFault>>,
}

impl ConnectionsRegistry {
    /// Build a registry for `alpns` on `endpoint`, validating that none names RDM's own ALPNs.
    pub fn new(
        endpoint: iroh::Endpoint,
        resolver: Arc<dyn NodeResolver>,
        sink: Arc<dyn ConnectionsSink>,
        alpns: &[Alpn],
        member_cert: Vec<u8>,
    ) -> Result<(Self, Vec<(Vec<u8>, AlpnProtocolHandler)>), String> {
        validate_alpns(alpns)?;
        let mut slots = HashMap::new();
        let mut accept_txs = HashMap::new();
        for alpn in alpns {
            let (tx, rx) = mpsc::unbounded_channel();
            accept_txs.insert(alpn.name.clone(), tx);
            slots.insert(
                alpn.name.clone(),
                RegisteredSlot {
                    alpn: alpn.clone(),
                    taken: false,
                    accept_rx: Some(rx),
                },
            );
        }
        let registry = Self {
            inner: Arc::new(RegistryInner {
                endpoint,
                resolver,
                sink,
                admitter: Mutex::new(Arc::new(NoCertsAdmitter)),
                roots: RootSet::default(),
                member_cert: Mutex::new(member_cert),
                slots: Mutex::new(slots),
                accept_txs,
                active: Mutex::new(HashMap::new()),
                carrier_legs: Mutex::new(Vec::new()),
                deleted: AtomicBool::new(false),
                #[cfg(feature = "testkit-gossip")]
                blocked_direct_dials: Mutex::new(std::collections::HashSet::new()),
                #[cfg(feature = "testkit-gossip")]
                pre_opened_faults: Mutex::new(HashMap::new()),
            }),
        };
        let handlers = alpns
            .iter()
            .map(|a| {
                (
                    a.name.clone(),
                    AlpnProtocolHandler {
                        alpn: a.clone(),
                        registry: registry.clone(),
                    },
                )
            })
            .collect();
        Ok((registry, handlers))
    }

    /// Block Direct dials from this source node toward `(destination, alpn)`, answering
    /// [`ConnectError::NotSent`] and recording `Direct Failed` as an unreachable direct dial does.
    #[cfg(feature = "testkit-gossip")]
    pub fn block_direct_dial(&self, destination: &PathName, alpn: &[u8]) {
        self.inner
            .blocked_direct_dials
            .lock()
            .unwrap()
            .insert((destination.clone(), alpn.to_vec()));
    }

    /// Release a Direct dial block toward `(destination, alpn)`.
    #[cfg(feature = "testkit-gossip")]
    pub fn unblock_direct_dial(&self, destination: &PathName, alpn: &[u8]) {
        self.inner
            .blocked_direct_dials
            .lock()
            .unwrap()
            .remove(&(destination.clone(), alpn.to_vec()));
    }

    /// Arm a one-shot pre-`Opened` fault on this destination's registered `alpn` handler.
    #[cfg(feature = "testkit-gossip")]
    pub fn fault_pre_opened_once(&self, alpn: &[u8], fault: PreOpenedFault) {
        self.inner
            .pre_opened_faults
            .lock()
            .unwrap()
            .insert(alpn.to_vec(), fault);
    }

    /// Install a custom [`Admitter`] for inbound connections on registered ALPNs.
    pub fn set_admitter(&self, admitter: Arc<dyn Admitter>) {
        *self.inner.admitter.lock().unwrap() = admitter;
    }

    /// Admit inbound connections on the fabric's root set `roots`, judged on `time`
    /// (node-connections-admit.md): the set is held in this registry's [`RootSet`], which
    /// [`Self::roots`] hands to whoever replaces it at runtime. An empty set is a node started with no
    /// certs: it admits by name.
    pub fn admit_on(&self, roots: Vec<rdm_core::certs::VerifyingKey>, time: crate::clock::RafkaTime) {
        let admitter: Arc<dyn Admitter> = if roots.is_empty() { Arc::new(NoCertsAdmitter) } else { Arc::new(CertAdmitter::new(self.inner.roots.clone(), time)) };
        self.inner.roots.replace(roots);
        *self.inner.admitter.lock().unwrap() = admitter;
    }

    /// The root set this registry's admissions are judged on: the one holder `node.roots.update`
    /// replaces at runtime.
    pub fn roots(&self) -> RootSet {
        self.inner.roots.clone()
    }

    /// Update the member cert bytes presented on outbound dials.
    pub fn set_member_cert(&self, cert: Vec<u8>) {
        *self.inner.member_cert.lock().unwrap() = cert;
    }

    /// The member cert bytes this registry presents on outbound dials: empty for a node with no certs.
    pub fn member_cert(&self) -> Vec<u8> {
        self.inner.member_cert.lock().unwrap().clone()
    }

    /// Whether `alpn` is registered on this registry.
    pub fn has_alpn(&self, alpn: &[u8]) -> bool {
        self.inner.slots.lock().unwrap().contains_key(alpn)
    }

    /// Perform one bounded direct transport connection attempt on `alpn` toward `destination`
    /// (`connections.md` §9.5 step 3). On success, `Direct Connected` is recorded through the
    /// sink; on failure, the series' next `Direct Failed` row is recorded through the sink.
    pub async fn probe_direct(&self, destination: &Birth, alpn: &[u8]) -> Result<Connection, ConnectError> {
        if self.inner.deleted.load(Ordering::SeqCst) {
            return Err(ConnectError::Deleted);
        }
        let slot_alpn = {
            let slots = self.inner.slots.lock().unwrap();
            let Some(slot) = slots.get(alpn) else {
                return Err(ConnectError::UnregisteredAlpn {
                    alpn: alpn.to_vec(),
                });
            };
            slot.alpn.clone()
        };
        let dialer = Dialer {
            alpn: slot_alpn,
            registry: self.clone(),
        };
        let caller_span = tracing::Span::current();
        let trace = Trace::of(&caller_span, Trace::default());
        let own = self.inner.sink.own_birth();
        let conn = dialer
            .open_direct(&own, destination, &caller_span, trace)
            .await?;
        if let Some(entry) = self.inner.active.lock().unwrap().get_mut(conn.id()) {
            entry._keep_alive = Some(conn.inner.clone());
        }
        Ok(conn)
    }

    /// Take the one [`Connections`] handle for `alpn`.
    ///
    /// Returns [`ConnectError::UnregisteredAlpn`] if `alpn` was not registered at bind, and
    /// [`ConnectError::AlreadyTaken`] on any second call for the same `alpn`.
    pub fn connections(&self, alpn: &[u8]) -> Result<Connections, ConnectError> {
        let mut slots = self.inner.slots.lock().unwrap();
        let Some(slot) = slots.get_mut(alpn) else {
            return Err(ConnectError::UnregisteredAlpn {
                alpn: alpn.to_vec(),
            });
        };
        if slot.taken {
            return Err(ConnectError::AlreadyTaken {
                alpn: alpn.to_vec(),
            });
        }
        slot.taken = true;
        let accept_rx = slot.accept_rx.take().expect("taken is false");
        let dialer = Dialer {
            alpn: slot.alpn.clone(),
            registry: self.clone(),
        };
        let acceptor = Acceptor { accept_rx };
        Ok(Connections { dialer, acceptor })
    }

    /// Block new outbound connections (`node.connection.delete All`).
    pub fn block_new_connections(&self) {
        self.inner.deleted.store(true, Ordering::SeqCst);
    }

    /// Unblock outbound connections after `ApplyNodeName` or restart.
    pub fn unblock_new_connections(&self) {
        self.inner.deleted.store(false, Ordering::SeqCst);
    }

    /// Still-open Proxy tunnels whose route in `held` was superseded by Direct restoration (§10),
    /// tagged with `reason: Some("route: retired")` for `node.connection.get` and `node.connection.delete`.
    pub fn held_open_tunnels(&self) -> Vec<NodeConnection> {
        let active = self.inner.active.lock().unwrap();
        let held = self.inner.sink.held();
        let held_guard = held.lock().unwrap();
        let mut out = Vec::new();
        for entry in active.values() {
            if let Some(row) = &entry.row {
                if row.kind == CoreKind::Proxy {
                    let still_active_route = held_guard
                        .own_active_proxies()
                        .into_iter()
                        .any(|r| r.id == row.id);
                    if !still_active_route {
                        let mut tunnel_view = row.clone();
                        tunnel_view.reason = Some("route: retired".to_string());
                        out.push(tunnel_view);
                    }
                }
            }
        }
        out
    }

    fn emit_tunnel_ended_if_needed(&self, row: &NodeConnection) {
        if row.kind != CoreKind::Proxy {
            return;
        }
        let still_active_route = {
            let held = self.inner.sink.held();
            let g = held.lock().unwrap();
            g.own_active_proxies().into_iter().any(|r| r.id == row.id)
        };
        if !still_active_route {
            tracing::info_span!(
                "rdm.node.connection.delete.via-tunnel-ended",
                source = %row.source.name,
                carrier = %row.carrier.as_ref().map(|c| c.name.to_string()).unwrap_or_default(),
                destination = %row.destination.name,
                alpn = %String::from_utf8_lossy(&row.alpn),
                entry = %row.id,
            )
            .in_scope(|| tracing::info!("proxy tunnel ended without writing a second Disconnected row"));
        }
    }

    /// Every connection this node opened that is open now, Direct or Proxy, by its entry as it
    /// opened, whether its row has landed or is still owed (Q169); an accepted connection is none.
    pub fn open_rows(&self) -> Vec<NodeConnection> {
        self.inner.active.lock().unwrap().values().filter_map(|e| e.row.clone()).collect()
    }

    /// Close an active data-plane connection by its [`ConnectionId`] (`node.connection.delete`),
    /// waking its streams and [`Connection::closed`] with `reason`.
    pub fn close_by_id(&self, id: &ConnectionId, reason: Closed) -> bool {
        let entry = self.inner.active.lock().unwrap().remove(id);
        let Some(entry) = entry else {
            return false;
        };
        if let Some(row) = &entry.row {
            self.emit_tunnel_ended_if_needed(row);
        }
        let _ = entry.closed_tx.send(Some(reason.clone()));
        let (code, msg) = match &reason {
            Closed::ConnectionsDeleted => (CLOSE_CODE_CONNECTIONS_DELETED, "node.connection.delete"),
            Closed::CarrierGone => (CLOSE_CODE_CARRIER_GONE, "carrier-gone"),
            Closed::PeerLeft => (CLOSE_CODE_PEER_LEFT, "peer-left"),
            Closed::Refused { reason } => (CLOSE_CODE_REFUSED, reason.as_str()),
            Closed::Reset { code } => (*code, "reset"),
        };
        entry
            .conn
            .close(iroh::endpoint::VarInt::from_u32(code), msg.as_bytes());
        true
    }

    /// Close active data-plane connections matching `scope` and write their `Disconnected` drop
    /// rows through the sink.
    pub async fn delete_connections(&self, scope: &rdm_core::status::ConnectionScope) -> Vec<ConnectionId> {
        if matches!(scope, rdm_core::status::ConnectionScope::All) {
            self.block_new_connections();
        }
        let ids: Vec<ConnectionId> = match scope {
            rdm_core::status::ConnectionScope::All => {
                self.inner.active.lock().unwrap().keys().cloned().collect()
            }
            rdm_core::status::ConnectionScope::Connections(ids) => ids.clone(),
        };
        let mut rows = Vec::new();
        for id in &ids {
            if let Some(entry) = self.inner.active.lock().unwrap().remove(id) {
                let _ = entry.closed_tx.send(Some(Closed::ConnectionsDeleted));
                entry.conn.close(
                    iroh::endpoint::VarInt::from_u32(CLOSE_CODE_CONNECTIONS_DELETED),
                    b"node.connection.delete",
                );
                if let Some(row) = entry.row {
                    self.emit_tunnel_ended_if_needed(&row);
                    rows.push(row);
                }
            }
        }
        if !rows.is_empty() {
            self.inner.sink.record_drops(&rows, "node.connection.delete");
        }
        ids
    }

    fn register_active(
        &self,
        id: ConnectionId,
        conn: iroh::endpoint::Connection,
        closed_tx: watch::Sender<Option<Closed>>,
        row: Option<NodeConnection>,
    ) {
        self.inner
            .active
            .lock()
            .unwrap()
            .insert(
                id.clone(),
                ActiveEntry {
                    conn: conn.clone(),
                    closed_tx,
                    row: row.clone(),
                    _keep_alive: None,
                },
            );
        if let Some(r) = row {
            if r.kind == CoreKind::Proxy {
                let reg = self.clone();
                tokio::spawn(async move {
                    let _ = conn.closed().await;
                    if let Some(entry) = reg.inner.active.lock().unwrap().remove(&id) {
                        if let Some(row) = &entry.row {
                            reg.emit_tunnel_ended_if_needed(row);
                        }
                    }
                });
            }
        }
    }

    fn shutdown_all(&self) {
        let legs: Vec<_> = self.inner.carrier_legs.lock().unwrap().drain(..).collect();
        for (src, dst) in legs {
            src.close(
                iroh::endpoint::VarInt::from_u32(CLOSE_CODE_CARRIER_GONE),
                b"carrier-gone",
            );
            dst.close(
                iroh::endpoint::VarInt::from_u32(CLOSE_CODE_CARRIER_GONE),
                b"carrier-gone",
            );
        }
        let active: Vec<_> = self.inner.active.lock().unwrap().drain().collect();
        for (_, entry) in active {
            if entry.closed_tx.borrow().is_none() {
                entry.conn.close(
                    iroh::endpoint::VarInt::from_u32(CLOSE_CODE_PEER_LEFT),
                    b"peer-left",
                );
            }
        }
    }
}

/// The `iroh::protocol::ProtocolHandler` for one registered [`Alpn`].
#[derive(Clone)]
pub struct AlpnProtocolHandler {
    alpn: Alpn,
    registry: ConnectionsRegistry,
}

impl std::fmt::Debug for AlpnProtocolHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AlpnProtocolHandler")
            .field("alpn", &String::from_utf8_lossy(&self.alpn.name))
            .finish()
    }
}

impl iroh::protocol::ProtocolHandler for AlpnProtocolHandler {
    async fn accept(&self, connection: iroh::endpoint::Connection) -> Result<(), iroh::protocol::AcceptError> {
        let floor = rdm_core::cadence::staleness_floor();
        let Some(admit::Admitted { mut ctrl_send, mut ctrl_recv, present }) = admit::admit(&self.registry, &self.alpn, &connection).await else {
            return Ok(());
        };
        // The opener's context, as its present frame carried it: what the accepted connection hands
        // the application (telemetry.md §3), and what a carrier's tunnel span parents to.
        let opener = Trace::presented(&present);

        let intent: ControlIntent = match tokio::time::timeout(floor, read_control_msg(&mut ctrl_recv)).await {
            Ok(Ok(Some(i))) => i,
            _ => return Ok(()),
        };
        #[cfg(feature = "testkit-gossip")]
        if matches!(
            intent,
            ControlIntent::Direct { .. } | ControlIntent::Tunnel { via: Some(_), .. }
        ) {
            let fault = {
                self.registry
                    .inner
                    .pre_opened_faults
                    .lock()
                    .unwrap()
                    .remove(&self.alpn.name)
            };
            if let Some(fault) = fault {
                match fault {
                    PreOpenedFault::ApplicationBytes(bytes) => {
                        let _ = ctrl_send.write_all(&bytes).await;
                        let _ = ctrl_send.finish();
                        let _ = tokio::time::timeout(floor, connection.closed()).await;
                        return Ok(());
                    }
                    PreOpenedFault::Eof => {
                        let _ = ctrl_send.finish();
                        connection.close(
                            iroh::endpoint::VarInt::from_u32(CLOSE_CODE_PEER_LEFT),
                            b"pre-opened-eof",
                        );
                        return Ok(());
                    }
                    PreOpenedFault::HandshakeLapse => {
                        tokio::time::sleep(floor + std::time::Duration::from_millis(250)).await;
                        let _ = ctrl_send.finish();
                        return Ok(());
                    }
                }
            }
        }
        match intent {
            ControlIntent::Direct {
                id,
                origin,
                destination,
            } => {
                let own_now = self.registry.inner.sink.own_birth();
                if destination.incarnation != own_now.incarnation || destination.node_id != own_now.node_id {
                    let _ = write_control_msg(&mut ctrl_send, &ControlAnswer::Opened { peer: own_now }).await;
                    let _ = ctrl_send.finish();
                    let _ = tokio::time::timeout(floor, connection.closed()).await;
                    return Ok(());
                }
                if write_control_msg(&mut ctrl_send, &ControlAnswer::Opened { peer: own_now }).await.is_err() {
                    return Ok(());
                }
                self.registry.inner.sink.accepted(&origin, &self.alpn.name);
                let (closed_tx, closed_rx) = watch::channel(None);
                self.registry
                    .register_active(id.clone(), connection.clone(), closed_tx, None);
                let conn = Connection::new_direct(
                    id,
                    origin,
                    self.alpn.name.clone(),
                    connection.clone(),
                    ctrl_send,
                    ctrl_recv,
                    closed_rx,
                    opener.clone(),
                );
                if let Some(tx) = self.registry.inner.accept_txs.get(&self.alpn.name) {
                    let _ = tx.send(conn);
                }
                let _ = connection.closed().await;
            }
            ControlIntent::Tunnel {
                id,
                origin,
                destination,
                alpn,
                via: Some(carrier_birth),
            } => {
                let _ = alpn;
                let own_now = self.registry.inner.sink.own_birth();
                if destination.incarnation != own_now.incarnation || destination.node_id != own_now.node_id {
                    let _ = write_control_msg(&mut ctrl_send, &ControlAnswer::Opened { peer: own_now }).await;
                    let _ = ctrl_send.finish();
                    let _ = tokio::time::timeout(floor, connection.closed()).await;
                    return Ok(());
                }
                if write_control_msg(&mut ctrl_send, &ControlAnswer::Opened { peer: own_now }).await.is_err() {
                    return Ok(());
                }
                let (closed_tx, closed_rx) = watch::channel(None);
                self.registry
                    .register_active(id.clone(), connection.clone(), closed_tx, None);
                let conn = Connection::new_proxy(
                    id,
                    origin,
                    carrier_birth,
                    self.alpn.name.clone(),
                    connection.clone(),
                    ctrl_send,
                    ctrl_recv,
                    closed_rx,
                    opener.clone(),
                );
                if let Some(tx) = self.registry.inner.accept_txs.get(&self.alpn.name) {
                    let _ = tx.send(conn);
                }
                let _ = connection.closed().await;
            }
            ControlIntent::Tunnel {
                id,
                origin,
                destination,
                alpn,
                via: None,
            } => {
                self.carry(connection, ctrl_send, ctrl_recv, opener, id, origin, destination, alpn).await;
            }
        }
        Ok(())
    }

    async fn shutdown(&self) {
        self.registry.shutdown_all();
    }
}

/// What the carrier's open of a tunnel came to (connections.md §7).
enum CarrierOpen {
    /// Opened: the destination's leg, and its control streams, kept for the tunnel's life.
    Opened {
        dst_conn: iroh::endpoint::Connection,
        dst_ctrl: (iroh::endpoint::SendStream, iroh::endpoint::RecvStream),
    },
    /// Not opened, the source answered (a refusal by name, or the destination's answer as another
    /// birth): the source's leg is held until it closes, within the staleness floor.
    Answered { outcome: String },
    /// Not opened, nothing answered: the source's leg ends as the handler returns.
    Ended { outcome: String },
}

impl AlpnProtocolHandler {
    /// The carrier's side of a Proxy (connections.md §7). Its tunnel span
    /// `rdm.node.connection.create.via-tunnel`, the opener's child (telemetry.md §3), ends once the
    /// tunnel is opened or not, never held for the tunnel's life (§4). An opened tunnel is carried
    /// until either leg closes; the carrier then closes the other and spans the tunnel's end,
    /// `rdm.node.connection.delete.via-carrier-tunnel-ended`, in the opener's trace.
    #[allow(clippy::too_many_arguments)]
    async fn carry(
        &self,
        connection: iroh::endpoint::Connection,
        mut ctrl_send: iroh::endpoint::SendStream,
        ctrl_recv: iroh::endpoint::RecvStream,
        opener: Trace,
        id: ConnectionId,
        origin: Birth,
        destination: Birth,
        alpn: Vec<u8>,
    ) {
        let floor = rdm_core::cadence::staleness_floor();
        let alpn_str = String::from_utf8_lossy(&self.alpn.name).into_owned();
        let own_now = self.registry.inner.sink.own_birth();
        let cert = self.registry.inner.member_cert.lock().unwrap().clone();
        let presented_at = admit::presented_expiry(&cert);
        let tunnel_span = tracing::info_span!(
            "rdm.node.connection.create.via-tunnel",
            source = %origin.name,
            carrier = %own_now.name,
            destination = %destination.name,
            alpn = %alpn_str,
            entry = %id,
            cert_expires_at = presented_at,
            outcome = tracing::field::Empty,
        );
        if let Some(tp) = opener.traceparent.as_deref() {
            crate::set_remote_parent(&tunnel_span, tp, opener.tracestate.as_deref());
        }
        tunnel_span.in_scope(|| tracing::info!("carrier splicing data-plane proxy tunnel to destination"));
        let carrier_tp = Trace::of(&tunnel_span, opener);
        let ended_names = (origin.name.to_string(), own_now.name.to_string(), destination.name.to_string(), id.to_string());
        let opened = self
            .open_carrier_legs(&mut ctrl_send, carrier_tp.clone(), id, origin, destination, alpn, own_now, cert, presented_at, &tunnel_span)
            .await;
        tunnel_span.record(
            "outcome",
            match &opened {
                CarrierOpen::Opened { .. } => "opened",
                CarrierOpen::Answered { outcome } | CarrierOpen::Ended { outcome } => outcome.as_str(),
            },
        );
        drop(tunnel_span);
        match opened {
            CarrierOpen::Opened { dst_conn, dst_ctrl } => {
                self.registry.inner.carrier_legs.lock().unwrap().push((connection.clone(), dst_conn.clone()));
                let _keep_ctrl = (ctrl_send, ctrl_recv, dst_ctrl);
                let (closed_by, closed) = splice_tunnel(&connection, &dst_conn).await;
                self.registry
                    .inner
                    .carrier_legs
                    .lock()
                    .unwrap()
                    .retain(|(src, _)| src.stable_id() != connection.stable_id());
                let (source, carrier, destination, entry) = ended_names;
                let ended = tracing::info_span!(
                    "rdm.node.connection.delete.via-carrier-tunnel-ended",
                    source = %source,
                    carrier = %carrier,
                    destination = %destination,
                    alpn = %alpn_str,
                    entry = %entry,
                    closed_by = closed_by,
                    closed = %closed,
                );
                if let Some(tp) = carrier_tp.traceparent.as_deref() {
                    crate::set_remote_parent(&ended, tp, carrier_tp.tracestate.as_deref());
                }
                ended.in_scope(|| tracing::info!("carrier closed both legs of the proxy tunnel"));
            }
            CarrierOpen::Answered { .. } => {
                let _ = ctrl_send.finish();
                let _ = tokio::time::timeout(floor, connection.closed()).await;
            }
            CarrierOpen::Ended { .. } => {}
        }
    }

    /// The carrier's open of the destination's leg: the checks of connections.md §7-§8, the dial, the
    /// carrier's presentation, the destination's verdict and answer, passed back to the source.
    #[allow(clippy::too_many_arguments)]
    async fn open_carrier_legs(
        &self,
        ctrl_send: &mut iroh::endpoint::SendStream,
        carrier_tp: Trace,
        id: ConnectionId,
        origin: Birth,
        destination: Birth,
        alpn: Vec<u8>,
        own_now: Birth,
        cert: Vec<u8>,
        presented_at: u64,
        tunnel_span: &tracing::Span,
    ) -> CarrierOpen {
        let floor = rdm_core::cadence::staleness_floor();
        async fn refuse(ctrl_send: &mut iroh::endpoint::SendStream, reason: &str) -> CarrierOpen {
            let _ = write_control_msg(ctrl_send, &ControlAnswer::Refused { reason: reason.to_string() }).await;
            CarrierOpen::Answered { outcome: format!("refused: {reason}") }
        }

        let has_active_direct = {
            let held = self.registry.inner.sink.held();
            let g = held.lock().unwrap();
            matches!(
                g.active_direct_for(&own_now.name, &destination.name, &self.alpn.name),
                Some(Some(_))
            )
        };
        if !has_active_direct {
            return refuse(ctrl_send, INVALID_CARRIER_EDGE_LOST).await;
        }
        #[cfg(feature = "testkit-gossip")]
        if self
            .registry
            .inner
            .blocked_direct_dials
            .lock()
            .unwrap()
            .contains(&(destination.name.clone(), self.alpn.name.clone()))
        {
            let _ = ctrl_send.finish();
            return CarrierOpen::Ended { outcome: "not-opened: the direct dial is blocked".to_string() };
        }

        let resolved_dest = self
            .registry
            .inner
            .resolver
            .resolve(&NodeTarget::ExactNode(destination.node_id.clone()))
            .or_else(|_| {
                self.registry
                    .inner
                    .resolver
                    .resolve(&NodeTarget::CurrentPath(destination.name.clone()))
            });
        let Ok(resolved_dest) = resolved_dest else {
            return refuse(ctrl_send, INVALID_CARRIER_EDGE_LOST).await;
        };

        let dst_addr =
            iroh::EndpointAddr::new(resolved_dest.endpoint_id).with_ip_addr(resolved_dest.transport_addr);
        // The destination's leg is dialled as a root linked to the tunnel span, so its driver never
        // holds the tunnel span for the leg's life (telemetry.md §4).
        let Ok(Ok((dst_conn, mut dst_ctrl_send, mut dst_ctrl_recv, dial))) = tokio::time::timeout(
            floor,
            dial_root(&self.registry.inner.endpoint, &self.alpn.name, &destination, dst_addr, presented_at, tunnel_span),
        )
        .await
        else {
            return refuse(ctrl_send, INVALID_CARRIER_EDGE_LOST).await;
        };

        let dst_present = PresentFrame {
            caller: own_now.clone(),
            cert,
            traceparent: carrier_tp.traceparent,
            tracestate: carrier_tp.tracestate,
        };
        if write_control_msg(&mut dst_ctrl_send, &dst_present).await.is_err() {
            let outcome = "not-opened: the presentation to the destination was not written";
            dial.ended(outcome);
            return CarrierOpen::Ended { outcome: outcome.to_string() };
        }

        let dst_verdict: AdmitVerdict =
            match tokio::time::timeout(floor, read_control_msg(&mut dst_ctrl_recv)).await {
                Ok(Ok(Some(v))) => v,
                Ok(Ok(None)) | Ok(Err(_)) if refused_close(&dst_conn).is_some() => {
                    AdmitVerdict::Refused { reason: refused_close(&dst_conn).unwrap_or_default() }
                }
                _ => {
                    let outcome = "not-opened: the destination's verdict did not arrive";
                    dial.ended(outcome);
                    return CarrierOpen::Ended { outcome: outcome.to_string() };
                }
            };
        if let AdmitVerdict::Refused { reason } = dst_verdict {
            dial.refused(&reason);
            dst_conn.close(iroh::endpoint::VarInt::from_u32(CLOSE_CODE_REFUSED), reason.as_bytes());
            return refuse(ctrl_send, &reason).await;
        }

        let dst_intent = ControlIntent::Tunnel {
            id,
            origin,
            destination: destination.clone(),
            alpn,
            via: Some(own_now),
        };
        if write_control_msg(&mut dst_ctrl_send, &dst_intent).await.is_err() {
            let outcome = "not-opened: the intent to the destination was not written";
            dial.ended(outcome);
            return CarrierOpen::Ended { outcome: outcome.to_string() };
        }

        let dst_answer: ControlAnswer =
            match tokio::time::timeout(floor, read_control_msg(&mut dst_ctrl_recv)).await {
                Ok(Ok(Some(a))) => a,
                _ => {
                    let outcome = "not-opened: the destination's answer did not arrive";
                    dial.ended(outcome);
                    return CarrierOpen::Ended { outcome: outcome.to_string() };
                }
            };
        if write_control_msg(ctrl_send, &dst_answer).await.is_err() {
            let outcome = "not-opened: the answer to the source was not written";
            dial.ended(outcome);
            return CarrierOpen::Ended { outcome: outcome.to_string() };
        }

        match &dst_answer {
            ControlAnswer::Opened { peer } if peer.incarnation == destination.incarnation && peer.node_id == destination.node_id => {
                dial.connected();
                CarrierOpen::Opened { dst_conn, dst_ctrl: (dst_ctrl_send, dst_ctrl_recv) }
            }
            answer => {
                match answer {
                    ControlAnswer::Refused { reason } => dial.refused(reason),
                    ControlAnswer::Opened { .. } => dial.ended("not-opened: the destination answered as another birth"),
                }
                dst_conn.close(iroh::endpoint::VarInt::from_u32(CLOSE_CODE_PEER_LEFT), b"tunnel-rejected");
                CarrierOpen::Answered {
                    outcome: match answer {
                        ControlAnswer::Refused { reason } => format!("refused: {reason}"),
                        ControlAnswer::Opened { .. } => "not-opened: the destination answered as another birth".to_string(),
                    },
                }
            }
        }
    }
}

async fn proxy_stream_dir(
    mut recv: iroh::endpoint::RecvStream,
    mut send: iroh::endpoint::SendStream,
) {
    let mut buf = vec![0u8; 16 * 1024];
    loop {
        let read_res = tokio::select! {
            res = recv.read(&mut buf) => res,
            stopped = send.stopped() => {
                if let Ok(Some(code)) = stopped {
                    let _ = recv.stop(code);
                }
                return;
            }
        };
        match read_res {
            Ok(Some(0)) | Ok(None) => {
                let _ = send.finish();
                if let Ok(Some(code)) = send.stopped().await {
                    let _ = recv.stop(code);
                }
                return;
            }
            Ok(Some(n)) => match send.write_all(&buf[..n]).await {
                Ok(()) => {}
                Err(iroh::endpoint::WriteError::Stopped(code)) => {
                    let _ = recv.stop(code);
                    return;
                }
                Err(_) => return,
            },
            Err(iroh::endpoint::ReadError::Reset(code)) => {
                let _ = send.reset(code);
                return;
            }
            Err(_) => return,
        }
    }
}

/// The reason the acceptor's 412 close carries, when it refused this connection at admission.
fn refused_close(conn: &iroh::endpoint::Connection) -> Option<String> {
    match conn.close_reason()? {
        iroh::endpoint::ConnectionError::ApplicationClosed(frame)
            if u32::try_from(frame.error_code.into_inner()).ok() == Some(CLOSE_CODE_REFUSED) =>
        {
            Some(String::from_utf8_lossy(&frame.reason).into_owned())
        }
        _ => None,
    }
}

fn propagate_leg_close(
    err: &iroh::endpoint::ConnectionError,
    other_leg: &iroh::endpoint::Connection,
) {
    match err {
        iroh::endpoint::ConnectionError::ApplicationClosed(frame) => {
            let code = u32::try_from(frame.error_code.into_inner()).unwrap_or(0);
            if code == CLOSE_CODE_CARRIER_GONE {
                other_leg.close(
                    iroh::endpoint::VarInt::from_u32(CLOSE_CODE_CARRIER_GONE),
                    b"carrier-gone",
                );
            } else {
                other_leg.close(
                    iroh::endpoint::VarInt::from_u32(CLOSE_CODE_PEER_LEFT),
                    b"peer-left",
                );
            }
        }
        _ => {
            other_leg.close(
                iroh::endpoint::VarInt::from_u32(CLOSE_CODE_CARRIER_GONE),
                b"carrier-gone",
            );
        }
    }
}

/// Carry a tunnel's streams between its two legs until either leg closes, then close the other
/// with the close it carries ([`propagate_leg_close`]). Returns which leg closed first and the reason
/// it closed with. The accept loops end when a leg closes, so the tunnel's end is always a leg's
/// close, never a loop's: the other leg is never left open.
async fn splice_tunnel(
    src_conn: &iroh::endpoint::Connection,
    dst_conn: &iroh::endpoint::Connection,
) -> (&'static str, String) {
    let s_to_d = {
        let src = src_conn.clone();
        let dst = dst_conn.clone();
        async move {
            while let Ok((src_send, src_recv)) = src.accept_bi().await {
                let Ok((dst_send, dst_recv)) = dst.open_bi().await else {
                    break;
                };
                tokio::spawn(proxy_stream_dir(src_recv, dst_send));
                tokio::spawn(proxy_stream_dir(dst_recv, src_send));
            }
        }
    };
    let d_to_s = {
        let src = src_conn.clone();
        let dst = dst_conn.clone();
        async move {
            while let Ok((dst_send, dst_recv)) = dst.accept_bi().await {
                let Ok((src_send, src_recv)) = src.open_bi().await else {
                    break;
                };
                tokio::spawn(proxy_stream_dir(dst_recv, src_send));
                tokio::spawn(proxy_stream_dir(src_recv, dst_send));
            }
        }
    };
    let carried = async {
        tokio::join!(s_to_d, d_to_s);
        std::future::pending::<std::convert::Infallible>().await
    };
    let (closed_by, err, other_leg) = tokio::select! {
        err = src_conn.closed() => ("source", err, dst_conn),
        err = dst_conn.closed() => ("destination", err, src_conn),
        never = carried => match never {},
    };
    propagate_leg_close(&err, other_leg);
    let reason = match &err {
        iroh::endpoint::ConnectionError::ApplicationClosed(frame) => String::from_utf8_lossy(&frame.reason).into_owned(),
        other => other.to_string(),
    };
    (closed_by, reason)
}

/// One registered ALPN's dial and accept handle. Not `Clone`: there is one accept owner at
/// compile time as well as at runtime.
pub struct Connections {
    dialer: Dialer,
    acceptor: Acceptor,
}

impl std::fmt::Debug for Connections {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connections")
            .field("alpn", &String::from_utf8_lossy(&self.dialer.alpn.name))
            .finish()
    }
}

impl Connections {
    /// Open a connection to the exact `destination` birth on this ALPN.
    pub async fn open(&self, destination: &Birth) -> Result<Connection, ConnectError> {
        self.dialer.open(destination).await
    }

    /// Accept the next admitted inbound connection on this ALPN.
    pub async fn accept(&mut self) -> Option<Connection> {
        self.acceptor.accept().await
    }

    /// Split this handle once into its cloneable [`Dialer`] and its single-owner [`Acceptor`].
    pub fn split(self) -> (Dialer, Acceptor) {
        (self.dialer, self.acceptor)
    }
}

/// The dial half of [`Connections`], produced by [`Connections::split`]. Cloneable so multiple
/// callers can open connections on the same ALPN concurrently.
#[derive(Clone)]
pub struct Dialer {
    alpn: Alpn,
    registry: ConnectionsRegistry,
}

impl std::fmt::Debug for Dialer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Dialer")
            .field("alpn", &String::from_utf8_lossy(&self.alpn.name))
            .finish()
    }
}

/// The accept half of [`Connections`], produced by [`Connections::split`]. Not `Clone`: only one
/// owner accepts inbound connections on an ALPN.
pub struct Acceptor {
    accept_rx: mpsc::UnboundedReceiver<Connection>,
}

impl std::fmt::Debug for Acceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Acceptor").finish()
    }
}

impl Acceptor {
    /// Accept the next admitted inbound connection on this ALPN.
    pub async fn accept(&mut self) -> Option<Connection> {
        self.accept_rx.recv().await
    }
}

impl Dialer {
    /// Open a connection to the exact `destination` birth on this ALPN.
    pub async fn open(&self, destination: &Birth) -> Result<Connection, ConnectError> {
        if self.registry.inner.deleted.load(Ordering::SeqCst) {
            return Err(ConnectError::Deleted);
        }
        let caller_span = tracing::Span::current();
        let trace = Trace::of(&caller_span, Trace::default());
        // A cold discovery's cost runs from the open's start to its Proxy Connected (connections.md §12).
        let begun = std::time::Instant::now();
        let own = self.registry.inner.sink.own_birth();
        let res = {
            let held = self.registry.inner.sink.held();
            let g = held.lock().unwrap();
            resolve_for_carriers(
                &g,
                &own.name,
                &destination.name,
                &self.alpn.name,
                self.alpn.proxy,
                &self.alpn.carriers,
            )
        };
        if let Some((invalid_proxy, reason)) = res.retire {
            self.registry
                .inner
                .sink
                .record_drops(&[invalid_proxy], reason);
        }
        match res.route {
            EffectiveRoute::NoActiveRoute => return Err(ConnectError::NoActiveRoute { direct: None }),
            EffectiveRoute::ViaPeer { proxy, .. } => {
                let Some(carrier_end) = proxy.carrier.as_ref() else {
                    self.registry
                        .inner
                        .sink
                        .record_drops(std::slice::from_ref(&proxy), INVALID_CARRIER_EDGE_LOST);
                    return Err(ConnectError::NoActiveRoute { direct: None });
                };
                let Some(carrier_inc) = carrier_end.incarnation.clone() else {
                    self.registry
                        .inner
                        .sink
                        .record_drops(std::slice::from_ref(&proxy), INVALID_CARRIER_EDGE_LOST);
                    return Err(ConnectError::NoActiveRoute { direct: None });
                };
                let carrier_birth = Birth {
                    name: carrier_end.name.clone(),
                    node_id: carrier_end.node_id.clone(),
                    incarnation: carrier_inc,
                };
                return self
                    .open_tunnel(&own, &carrier_birth, destination, Some(&proxy), None, &caller_span, trace)
                    .await;
            }
            EffectiveRoute::Direct { .. } => {}
        }

        match self
            .open_direct(&own, destination, &caller_span, trace.clone())
            .await
        {
            Ok(conn) => Ok(conn),
            Err(ConnectError::NotSent { reason }) if self.alpn.proxy => {
                // The carrier is a live node of this node's mesh from the topology it holds (Q227),
                // asked in the spread order; its own Direct to the destination decides, and one
                // without it refuses by name, so the next is asked. Nothing was sent to the
                // destination by a refused or undialled carrier, so asking the next is safe.
                let live = self.registry.inner.resolver.live_nodes();
                let order = carrier_order(
                    &own.name,
                    &destination.name,
                    &destination.incarnation,
                    self.alpn.proxy,
                    &self.alpn.carriers,
                    live.iter().map(|n| n.name.clone()),
                );
                if order.is_empty() {
                    return Err(ConnectError::NotSent { reason });
                }
                for name in order {
                    let Some(node) = live.iter().find(|n| n.name == name) else { continue };
                    let carrier_birth = Birth { name: node.name.clone(), node_id: node.node_id.clone(), incarnation: node.incarnation.clone() };
                    match self
                        .open_tunnel(&own, &carrier_birth, destination, None, Some(begun), &caller_span, trace.clone())
                        .await
                    {
                        Ok(conn) => return Ok(conn),
                        Err(ConnectError::Refused { reason }) if reason == INVALID_CARRIER_EDGE_LOST => continue,
                        Err(ConnectError::NotSent { .. }) => continue,
                        Err(e) => return Err(e),
                    }
                }
                caller_span.in_scope(|| tracing::info!(direct = %reason, "rdm.node.connection.open.via-no-active-route: the Direct dial was not sent and every carrier refused"));
                Err(ConnectError::NoActiveRoute { direct: Some(reason) })
            }
            Err(e) => Err(e),
        }
    }

    async fn open_direct(
        &self,
        own: &Birth,
        destination: &Birth,
        caller_span: &tracing::Span,
        trace: Trace,
    ) -> Result<Connection, ConnectError> {
        #[cfg(feature = "testkit-gossip")]
        if self
            .registry
            .inner
            .blocked_direct_dials
            .lock()
            .unwrap()
            .contains(&(destination.name.clone(), self.alpn.name.clone()))
        {
            let reason = format!(
                "direct dial to {} on {:?} blocked by testkit fault",
                destination.name,
                String::from_utf8_lossy(&self.alpn.name)
            );
            self.record_direct_failure(own, destination, &reason, caller_span);
            return Err(ConnectError::NotSent { reason });
        }
        let resolved = match self
            .registry
            .inner
            .resolver
            .resolve(&NodeTarget::ExactNode(destination.node_id.clone()))
            .or_else(|_| {
                self.registry
                    .inner
                    .resolver
                    .resolve(&NodeTarget::CurrentPath(destination.name.clone()))
            }) {
            Ok(r) => r,
            Err(e) => {
                let reason = format!("{e:?}");
                self.record_direct_failure(own, destination, &reason, caller_span);
                return Err(ConnectError::NotSent { reason });
            }
        };

        let cert = self.registry.inner.member_cert.lock().unwrap().clone();
        let presented_at = admit::presented_expiry(&cert);
        let addr = iroh::EndpointAddr::new(resolved.endpoint_id).with_ip_addr(resolved.transport_addr);
        let (quic_conn, mut ctrl_send, mut ctrl_recv, dial) = match self.dial(destination, addr, presented_at, caller_span).await {
            Ok(dialled) => dialled,
            Err(reason) => {
                self.record_direct_failure(own, destination, &reason, caller_span);
                return Err(ConnectError::NotSent { reason });
            }
        };

        let present_tp = if trace.traceparent.is_some() { trace } else { Trace::of(&dial.span, Trace::default()) };
        let present = PresentFrame {
            caller: own.clone(),
            cert,
            traceparent: present_tp.traceparent,
            tracestate: present_tp.tracestate,
        };

        if let Err(reason) = write_control_msg(&mut ctrl_send, &present).await {
            dial.ended(&reason);
            self.record_direct_failure(own, destination, &reason, caller_span);
            return Err(ConnectError::NotSent { reason });
        }

        let floor = rdm_core::cadence::staleness_floor();
        let verdict: AdmitVerdict = match tokio::time::timeout(floor, read_control_msg(&mut ctrl_recv)).await {
            Ok(Ok(Some(v))) => v,
            // The acceptor's 412 close carries its refusal when the verdict itself did not arrive.
            Ok(Ok(None)) | Ok(Err(_)) if refused_close(&quic_conn).is_some() => {
                AdmitVerdict::Refused { reason: refused_close(&quic_conn).unwrap_or_default() }
            }
            Ok(Ok(None)) => {
                let reason = "acceptor closed control stream before verdict".to_string();
                dial.ended(&reason);
                self.record_direct_failure(own, destination, &reason, caller_span);
                return Err(ConnectError::NotSent { reason });
            }
            Ok(Err(reason)) => {
                dial.ended(&reason);
                self.record_direct_failure(own, destination, &reason, caller_span);
                return Err(ConnectError::NotSent { reason });
            }
            Err(_) => {
                let reason = "timed out waiting for admission verdict".to_string();
                dial.ended(&reason);
                self.record_direct_failure(own, destination, &reason, caller_span);
                return Err(ConnectError::NotSent { reason });
            }
        };

        match verdict {
            // A refusal is a named end of this dial (node-connections-admit.md step 5): a Direct
            // Failed row with the acceptor's reason, so this source's reconnect series exists and
            // continues from it (connections.md §9.1, §9.3).
            AdmitVerdict::Refused { reason } => {
                dial.refused(&reason);
                self.record_direct_failure(own, destination, &format!("refused: {reason}"), caller_span);
                quic_conn.close(
                    iroh::endpoint::VarInt::from_u32(CLOSE_CODE_REFUSED),
                    reason.as_bytes(),
                );
                return Err(ConnectError::Refused { reason });
            }
            AdmitVerdict::Admitted => {}
        }

        let row_id = ConnectionId::mint();
        let intent = ControlIntent::Direct {
            id: row_id.clone(),
            origin: own.clone(),
            destination: destination.clone(),
        };
        if let Err(reason) = write_control_msg(&mut ctrl_send, &intent).await {
            dial.ended(&reason);
            self.record_direct_failure(own, destination, &reason, caller_span);
            return Err(ConnectError::NotSent { reason });
        }

        let answer: ControlAnswer = match tokio::time::timeout(floor, read_control_msg(&mut ctrl_recv)).await {
            Ok(Ok(Some(a))) => a,
            Ok(Ok(None)) => {
                let reason = "acceptor closed control stream before intent answer".to_string();
                dial.ended(&reason);
                self.record_direct_failure(own, destination, &reason, caller_span);
                return Err(ConnectError::NotSent { reason });
            }
            Ok(Err(reason)) => {
                dial.ended(&reason);
                self.record_direct_failure(own, destination, &reason, caller_span);
                return Err(ConnectError::NotSent { reason });
            }
            Err(_) => {
                let reason = "timed out waiting for intent answer".to_string();
                dial.ended(&reason);
                self.record_direct_failure(own, destination, &reason, caller_span);
                return Err(ConnectError::NotSent { reason });
            }
        };

        match answer {
            ControlAnswer::Refused { reason } => {
                dial.refused(&reason);
                Err(ConnectError::Refused { reason })
            }
            ControlAnswer::Opened { peer } => {
                if peer.incarnation != destination.incarnation || peer.node_id != destination.node_id {
                    dial.ended("wrong-incarnation");
                    quic_conn.close(iroh::endpoint::VarInt::from_u32(CLOSE_CODE_PEER_LEFT), b"wrong-incarnation");
                    return Err(ConnectError::WrongIncarnation {
                        held: destination.incarnation.clone(),
                        found: peer.incarnation,
                    });
                }
                dial.connected();
                let row = NodeConnection {
                    id: row_id.clone(),
                    source: own.to_end(),
                    destination: destination.to_end(),
                    kind: CoreKind::Direct,
                    alpn: self.alpn.name.clone(),
                    state: ConnectionState::Connected,
                    carrier: None,
                    recovery: None,
                    reason: None,
                    logged_at_ms: 0,
                };
                // The Connected row is owed, never awaited (Q169): the open returns once Opened.
                caller_span.in_scope(|| self.registry.inner.sink.record_row(owed(row.clone())));
                let recorded = row;
                let (closed_tx, closed_rx) = watch::channel(None);
                self.registry.register_active(
                    row_id.clone(),
                    quic_conn.clone(),
                    closed_tx,
                    Some(recorded),
                );
                Ok(Connection::new_direct(
                    row_id,
                    destination.clone(),
                    self.alpn.name.clone(),
                    quic_conn,
                    ctrl_send,
                    ctrl_recv,
                    closed_rx,
                    Trace::presented(&present),
                ))
            }
        }
    }

    async fn open_tunnel(
        &self,
        own: &Birth,
        carrier: &Birth,
        destination: &Birth,
        held_proxy: Option<&NodeConnection>,
        cold_discovery: Option<std::time::Instant>,
        caller_span: &tracing::Span,
        trace: Trace,
    ) -> Result<Connection, ConnectError> {
        let alpn_str = String::from_utf8_lossy(&self.alpn.name).into_owned();
        let tunnel_span = caller_span.in_scope(|| {
            tracing::info_span!(
                "rdm.node.connection.create.via-tunnel",
                source = %own.name,
                carrier = %carrier.name,
                destination = %destination.name,
                alpn = %alpn_str,
                entry = tracing::field::Empty,
                kind = tracing::field::Empty,
                state = tracing::field::Empty,
                outcome = tracing::field::Empty,
                effective_route_cold_discovery_ms = tracing::field::Empty,
            )
        });
        tunnel_span.in_scope(|| tracing::info!("opening data-plane proxy tunnel through carrier"));
        let tunnel_tp = Trace::of(&tunnel_span, trace);

        let resolved = self
            .registry
            .inner
            .resolver
            .resolve(&NodeTarget::ExactNode(carrier.node_id.clone()))
            .or_else(|_| {
                self.registry
                    .inner
                    .resolver
                    .resolve(&NodeTarget::CurrentPath(carrier.name.clone()))
            })
            .map_err(|e| ConnectError::NotSent {
                reason: format!("{e:?}"),
            })?;

        let cert = self.registry.inner.member_cert.lock().unwrap().clone();
        let presented_at = admit::presented_expiry(&cert);
        let addr = iroh::EndpointAddr::new(resolved.endpoint_id).with_ip_addr(resolved.transport_addr);
        let (quic_conn, mut ctrl_send, mut ctrl_recv, dial) = self
            .dial(carrier, addr, presented_at, &tunnel_span)
            .await
            .map_err(|reason| ConnectError::NotSent { reason })?;

        let present = PresentFrame {
            caller: own.clone(),
            cert,
            traceparent: tunnel_tp.traceparent,
            tracestate: tunnel_tp.tracestate,
        };

        if let Err(reason) = write_control_msg(&mut ctrl_send, &present).await {
            dial.ended(&reason);
            return Err(ConnectError::NotSent { reason });
        }

        let floor = rdm_core::cadence::staleness_floor();
        let verdict: AdmitVerdict = match tokio::time::timeout(floor, read_control_msg(&mut ctrl_recv)).await {
            Ok(Ok(Some(v))) => v,
            Ok(Ok(None)) | Ok(Err(_)) if refused_close(&quic_conn).is_some() => {
                AdmitVerdict::Refused { reason: refused_close(&quic_conn).unwrap_or_default() }
            }
            Ok(Ok(None)) => {
                let reason = "carrier closed control stream before verdict".to_string();
                dial.ended(&reason);
                return Err(ConnectError::NotSent { reason });
            }
            Ok(Err(reason)) => {
                dial.ended(&reason);
                return Err(ConnectError::NotSent { reason });
            }
            Err(_) => {
                let reason = "timed out waiting for carrier admission verdict".to_string();
                dial.ended(&reason);
                return Err(ConnectError::NotSent { reason });
            }
        };

        match verdict {
            AdmitVerdict::Refused { reason } => {
                dial.refused(&reason);
                tunnel_span.record("outcome", format!("refused: {reason}").as_str());
                quic_conn.close(
                    iroh::endpoint::VarInt::from_u32(CLOSE_CODE_REFUSED),
                    reason.as_bytes(),
                );
                return Err(ConnectError::Refused { reason });
            }
            AdmitVerdict::Admitted => {}
        }

        let row_id = match held_proxy {
            Some(p) if !self.registry.inner.active.lock().unwrap().contains_key(&p.id) => {
                p.id.clone()
            }
            _ => ConnectionId::mint(),
        };
        let intent = ControlIntent::Tunnel {
            id: row_id.clone(),
            origin: own.clone(),
            destination: destination.clone(),
            alpn: self.alpn.name.clone(),
            via: None,
        };
        if let Err(reason) = write_control_msg(&mut ctrl_send, &intent).await {
            dial.ended(&reason);
            return Err(ConnectError::NotSent { reason });
        }

        let answer: ControlAnswer = match tokio::time::timeout(floor, read_control_msg(&mut ctrl_recv)).await {
            Ok(Ok(Some(a))) => a,
            Ok(Ok(None)) => {
                let reason = "carrier closed control stream before tunnel answer".to_string();
                dial.ended(&reason);
                return Err(ConnectError::NotSent { reason });
            }
            Ok(Err(reason)) => {
                dial.ended(&reason);
                return Err(ConnectError::NotSent { reason });
            }
            Err(_) => {
                let reason = "timed out waiting for tunnel answer".to_string();
                dial.ended(&reason);
                return Err(ConnectError::NotSent { reason });
            }
        };

        match answer {
            ControlAnswer::Refused { reason } => {
                dial.refused(&reason);
                tunnel_span.record("outcome", format!("refused: {reason}").as_str());
                quic_conn.close(
                    iroh::endpoint::VarInt::from_u32(CLOSE_CODE_REFUSED),
                    reason.as_bytes(),
                );
                if reason == INVALID_CARRIER_EDGE_LOST {
                    if let Some(proxy) = held_proxy {
                        self.registry
                            .inner
                            .sink
                            .record_drops(std::slice::from_ref(proxy), INVALID_CARRIER_EDGE_LOST);
                    }
                }
                Err(ConnectError::Refused { reason })
            }
            ControlAnswer::Opened { peer } => {
                if peer.incarnation != destination.incarnation || peer.node_id != destination.node_id {
                    dial.ended("wrong-incarnation");
                    quic_conn.close(
                        iroh::endpoint::VarInt::from_u32(CLOSE_CODE_PEER_LEFT),
                        b"wrong-incarnation",
                    );
                    return Err(ConnectError::WrongIncarnation {
                        held: destination.incarnation.clone(),
                        found: peer.incarnation,
                    });
                }
                dial.connected();
                let recorded = match held_proxy {
                    Some(existing) => NodeConnection {
                        id: row_id.clone(),
                        ..existing.clone()
                    },
                    None => {
                        let row = NodeConnection {
                            id: row_id.clone(),
                            source: own.to_end(),
                            destination: destination.to_end(),
                            kind: CoreKind::Proxy,
                            alpn: self.alpn.name.clone(),
                            state: ConnectionState::Connected,
                            carrier: Some(carrier.to_end()),
                            recovery: None,
                            reason: None,
                            logged_at_ms: 0,
                        };
                        // The new Proxy's row is owed, never awaited (Q169).
                        tunnel_span.in_scope(|| self.registry.inner.sink.record_row(owed(row.clone())));
                        row
                    }
                };
                tunnel_span.record("entry", recorded.id.to_string().as_str());
                tunnel_span.record("kind", "proxy");
                tunnel_span.record("state", "connected");
                tunnel_span.record("outcome", "opened");
                if let Some(begun) = cold_discovery {
                    tunnel_span.record("effective_route_cold_discovery_ms", begun.elapsed().as_millis() as u64);
                }
                let (closed_tx, closed_rx) = watch::channel(None);
                self.registry.register_active(
                    row_id.clone(),
                    quic_conn.clone(),
                    closed_tx,
                    Some(recorded),
                );
                Ok(Connection::new_proxy(
                    row_id,
                    destination.clone(),
                    carrier.clone(),
                    self.alpn.name.clone(),
                    quic_conn,
                    ctrl_send,
                    ctrl_recv,
                    closed_rx,
                    Trace::presented(&present),
                ))
            }
        }
    }

    /// The transport connect and the connection's control stream, as a dial of their own. The
    /// transport spawns a connection's driver task with the span current at its connect, and that
    /// task lives as long as the connection, so the dial never runs where `opener` is current: its
    /// span is a root linked (follows_from) to `opener`, the OpenTelemetry link for causally related
    /// work that is not nested (telemetry.md: a call span's children are the serve span, the
    /// handler's spans and the invocation's end), as the pooled dial's is. It is never the current
    /// span while the dial runs: the dial runs on a task of its own, on the dispatcher of its span,
    /// and the span ends with the dial, so neither it nor the opener's span, nor any span above it,
    /// is held open past the open. A dropped open aborts its dial, as it ends its own connect.
    async fn dial(
        &self,
        to: &Birth,
        addr: iroh::EndpointAddr,
        cert_expires_at: u64,
        opener: &tracing::Span,
    ) -> Result<(iroh::endpoint::Connection, iroh::endpoint::SendStream, iroh::endpoint::RecvStream, DialSpan), String> {
        dial_root(&self.registry.inner.endpoint, &self.alpn.name, to, addr, cert_expires_at, opener).await
    }

    /// Owe the dial's `Failed` row (Q169): built when the writer takes it, from the held
    /// projection then, so a failing series of the birth dialled continues one ordinal up from the
    /// failure owed before it; anything else is a new incident at ordinal one. Never awaited.
    fn record_direct_failure(
        &self,
        own: &Birth,
        destination: &Birth,
        reason: &str,
        caller_span: &tracing::Span,
    ) {
        let own = own.to_end();
        let dest_end = destination.to_end();
        let alpn = self.alpn.name.clone();
        let reason = reason.to_string();
        let build: OwedRow = Box::new(move |held: &ConnectionsHeld| {
            let previous = held
                .own_latest_directs()
                .into_iter()
                .find(|d| d.destination.name == dest_end.name && d.alpn == alpn)
                .cloned();
            Some(match previous.as_ref() {
                Some(p)
                    if p.state == ConnectionState::Failed
                        && (&p.destination.node_id, &p.destination.incarnation)
                            == (&dest_end.node_id, &dest_end.incarnation) =>
                {
                    match p.recovery {
                        Some(r) => NodeConnection {
                            id: ConnectionId::mint(),
                            source: own,
                            destination: dest_end,
                            recovery: Some(DirectRecovery {
                                recovery_round: r.recovery_round,
                                attempt_ordinal: r.attempt_ordinal.saturating_add(1),
                            }),
                            reason: Some(reason),
                            logged_at_ms: 0,
                            ..p.clone()
                        },
                        None => first_failure_for(own, dest_end, alpn, previous.as_ref(), &reason, 0),
                    }
                }
                _ => first_failure_for(own, dest_end, alpn, previous.as_ref(), &reason, 0),
            })
        });
        caller_span.in_scope(|| self.registry.inner.sink.record_row(build));
    }
}

/// The caller's `rdm.node.connection.create.via-dial` span, held across the presentation and
/// verdict so it ends with `outcome = "connected"` or `outcome = "refused"` and `reason`, and
/// dropped before `open` returns so it is never held open for the connection's life.
struct DialSpan {
    span: tracing::Span,
    started: std::time::Instant,
    finished: bool,
}

impl DialSpan {
    fn connected(mut self) {
        self.span.record("outcome", "connected");
        self.span.record("elapsed_ms", self.started.elapsed().as_millis() as u64);
        self.finished = true;
    }

    fn refused(mut self, reason: &str) {
        self.span.record("outcome", "refused");
        self.span.record("reason", reason);
        self.span.record("elapsed_ms", self.started.elapsed().as_millis() as u64);
        self.finished = true;
    }

    fn ended(mut self, outcome: &str) {
        self.span.record("outcome", outcome);
        self.span.record("elapsed_ms", self.started.elapsed().as_millis() as u64);
        self.finished = true;
    }
}

impl Drop for DialSpan {
    fn drop(&mut self) {
        if !self.finished {
            self.span.record("outcome", "closed");
            self.span.record("elapsed_ms", self.started.elapsed().as_millis() as u64);
        }
    }
}

/// A data-plane connection's dial: `rdm.node.connection.create.via-dial`, a root linked (follows_from)
/// to `opener`, run on a task of its own so the connection's driver takes the dial's span, never the
/// opener's (telemetry.md §4). The source's open and the carrier's leg to the destination dial here.
async fn dial_root(
    endpoint: &iroh::Endpoint,
    alpn: &[u8],
    to: &Birth,
    addr: iroh::EndpointAddr,
    cert_expires_at: u64,
    opener: &tracing::Span,
) -> Result<(iroh::endpoint::Connection, iroh::endpoint::SendStream, iroh::endpoint::RecvStream, DialSpan), String> {
    let span = tracing::info_span!(
        parent: None,
        "rdm.node.connection.create.via-dial",
        destination = %to.name,
        alpn = %String::from_utf8_lossy(alpn),
        cert_expires_at = cert_expires_at,
        outcome = tracing::field::Empty,
        reason = tracing::field::Empty,
        elapsed_ms = tracing::field::Empty,
    );
    span.follows_from(opener);
    let dispatch = tracing::Span::with_subscriber(&span, |(_, d): (&tracing::Id, &tracing::Dispatch)| d.clone());
    let endpoint = endpoint.clone();
    let alpn = alpn.to_vec();
    let dial = async move {
        let started = std::time::Instant::now();
        let out: Result<_, String> = async {
            let conn = endpoint.connect(addr, &alpn).await.map_err(|e| e.to_string())?;
            let (send, recv) = conn.open_bi().await.map_err(|e| e.to_string())?;
            Ok((conn, send, recv))
        }
        .await;
        match out {
            Ok((conn, send, recv)) => Ok((conn, send, recv, DialSpan { span, started, finished: false })),
            Err(e) => {
                span.record("outcome", e.as_str());
                span.record("elapsed_ms", started.elapsed().as_millis() as u64);
                drop(span);
                Err(e)
            }
        }
    };
    let mut task = DialTask(match dispatch {
        Some(d) => tokio::spawn(tracing::instrument::WithSubscriber::with_subscriber(dial, d)),
        None => tokio::spawn(dial),
    });
    match (&mut task.0).await {
        Ok(out) => out,
        Err(e) => Err(format!("the dial ended without an outcome: {e}")),
    }
}

/// A dial's task ([`dial_root`]), aborted when the open awaiting it is dropped.
struct DialTask<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for DialTask<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One established data-plane connection (corresponding to one row of the connections log).
pub struct Connection {
    inner: Arc<ConnectionInner>,
}

struct ConnectionInner {
    id: ConnectionId,
    peer: Birth,
    alpn: Vec<u8>,
    kind: ConnectionKind,
    quic: iroh::endpoint::Connection,
    _ctrl_send: Mutex<iroh::endpoint::SendStream>,
    _ctrl_recv: Mutex<iroh::endpoint::RecvStream>,
    closed_rx: watch::Receiver<Option<Closed>>,
    trace: Trace,
}

/// The opener's W3C trace context a connection carries (telemetry.md §3, the accepted connection).
#[derive(Debug, Clone, Default)]
struct Trace {
    traceparent: Option<String>,
    tracestate: Option<String>,
}

impl Trace {
    /// `span`'s context, or `or` whole when `span` has none (no layer gives it one).
    fn of(span: &tracing::Span, or: Trace) -> Trace {
        match crate::traceparent_of(span) {
            Some(traceparent) => Trace { traceparent: Some(traceparent), tracestate: crate::tracestate_of(span) },
            None => or,
        }
    }

    /// What a present frame carried.
    fn presented(present: &PresentFrame) -> Trace {
        Trace { traceparent: present.traceparent.clone(), tracestate: present.tracestate.clone() }
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("id", &self.inner.id)
            .field("peer", &self.inner.peer)
            .field("alpn", &String::from_utf8_lossy(&self.inner.alpn))
            .field("kind", &self.inner.kind)
            .finish()
    }
}

impl Connection {
    fn new_direct(
        id: ConnectionId,
        peer: Birth,
        alpn: Vec<u8>,
        quic: iroh::endpoint::Connection,
        ctrl_send: iroh::endpoint::SendStream,
        ctrl_recv: iroh::endpoint::RecvStream,
        closed_rx: watch::Receiver<Option<Closed>>,
        trace: Trace,
    ) -> Self {
        Self {
            inner: Arc::new(ConnectionInner {
                id,
                peer,
                alpn,
                kind: ConnectionKind::Direct,
                quic,
                _ctrl_send: Mutex::new(ctrl_send),
                _ctrl_recv: Mutex::new(ctrl_recv),
                closed_rx,
                trace,
            }),
        }
    }

    fn new_proxy(
        id: ConnectionId,
        peer: Birth,
        via: Birth,
        alpn: Vec<u8>,
        quic: iroh::endpoint::Connection,
        ctrl_send: iroh::endpoint::SendStream,
        ctrl_recv: iroh::endpoint::RecvStream,
        closed_rx: watch::Receiver<Option<Closed>>,
        trace: Trace,
    ) -> Self {
        Self {
            inner: Arc::new(ConnectionInner {
                id,
                peer,
                alpn,
                kind: ConnectionKind::Proxy { via },
                quic,
                _ctrl_send: Mutex::new(ctrl_send),
                _ctrl_recv: Mutex::new(ctrl_recv),
                closed_rx,
                trace,
            }),
        }
    }

    /// The connection row's id (`node.connection.get` lists it; `node.connection.delete` closes it).
    pub fn id(&self) -> &ConnectionId {
        &self.inner.id
    }

    /// The far peer's exact birth.
    pub fn peer(&self) -> &Birth {
        &self.inner.peer
    }

    /// The registered ALPN this connection carries.
    pub fn alpn(&self) -> &[u8] {
        &self.inner.alpn
    }

    /// Whether this connection is `Direct` or `Proxy { via }`.
    pub fn kind(&self) -> ConnectionKind {
        self.inner.kind.clone()
    }

    /// The opener's W3C `traceparent`, as the connection's present frame carried it: the opener's span
    /// around `open` (a Proxy's, the carrier's tunnel span, that span's child). An application span made
    /// from it is the open's child and shares the opener's trace (telemetry.md §3). Observability only.
    pub fn traceparent(&self) -> Option<&str> {
        self.inner.trace.traceparent.as_deref()
    }

    /// The opener's W3C `tracestate`, beside [`Self::traceparent`].
    pub fn tracestate(&self) -> Option<&str> {
        self.inner.trace.tracestate.as_deref()
    }

    /// Open a new bidirectional stream on this connection.
    pub async fn open_bi(&self) -> Result<(SendStream, RecvStream), StreamError> {
        if let Some(c) = self.inner.closed_rx.borrow().clone() {
            return Err(StreamError::Closed(c));
        }
        match self.inner.quic.open_bi().await {
            Ok((send, recv)) => Ok((
                SendStream {
                    inner: send,
                    conn: self.inner.clone(),
                },
                RecvStream {
                    inner: recv,
                    conn: self.inner.clone(),
                },
            )),
            Err(err) => Err(StreamError::Closed(classify_conn_err(&self.inner, &err))),
        }
    }

    /// Accept the next bidirectional stream opened by the peer on this connection.
    pub async fn accept_bi(&self) -> Result<(SendStream, RecvStream), StreamError> {
        let mut closed_rx = self.inner.closed_rx.clone();
        if let Some(c) = closed_rx.borrow().clone() {
            return Err(StreamError::Closed(c));
        }
        tokio::select! {
            res = self.inner.quic.accept_bi() => match res {
                Ok((send, recv)) => Ok((
                    SendStream {
                        inner: send,
                        conn: self.inner.clone(),
                    },
                    RecvStream {
                        inner: recv,
                        conn: self.inner.clone(),
                    },
                )),
                Err(err) => Err(StreamError::Closed(classify_conn_err(&self.inner, &err))),
            },
            Ok(c_ref) = closed_rx.wait_for(|c| c.is_some()) => {
                let c = c_ref.clone().expect("is_some");
                Err(StreamError::Closed(c))
            }
        }
    }

    /// Resolves with the typed [`Closed`] reason once this connection closes.
    pub async fn closed(&self) -> Closed {
        let mut closed_rx = self.inner.closed_rx.clone();
        if let Some(c) = closed_rx.borrow().clone() {
            return c;
        }
        tokio::select! {
            err = self.inner.quic.closed() => classify_conn_err(&self.inner, &err),
            Ok(c_ref) = closed_rx.wait_for(|c| c.is_some()) => {
                c_ref.clone().expect("is_some")
            }
        }
    }
}

fn classify_conn_err(inner: &ConnectionInner, err: &iroh::endpoint::ConnectionError) -> Closed {
    if let Some(c) = inner.closed_rx.borrow().clone() {
        return c;
    }
    match err {
        iroh::endpoint::ConnectionError::ApplicationClosed(frame) => {
            let code = u32::try_from(frame.error_code.into_inner()).unwrap_or(0);
            let reason_str = String::from_utf8_lossy(&frame.reason).into_owned();
            if code == CLOSE_CODE_CONNECTIONS_DELETED || reason_str == "node.connection.delete" {
                Closed::ConnectionsDeleted
            } else if code == CLOSE_CODE_CARRIER_GONE {
                Closed::CarrierGone
            } else if code == CLOSE_CODE_REFUSED {
                Closed::Refused { reason: reason_str }
            } else if code == CLOSE_CODE_PEER_LEFT {
                Closed::PeerLeft
            } else if matches!(inner.kind, ConnectionKind::Proxy { .. }) && code == 0 {
                Closed::CarrierGone
            } else {
                Closed::Reset { code }
            }
        }
        iroh::endpoint::ConnectionError::LocallyClosed => {
            inner.closed_rx.borrow().clone().unwrap_or(Closed::PeerLeft)
        }
        _ => {
            if matches!(inner.kind, ConnectionKind::Proxy { .. }) {
                Closed::CarrierGone
            } else {
                Closed::PeerLeft
            }
        }
    }
}

/// The send half of a bidirectional stream on a [`Connection`].
///
/// Application stream bytes are never spanned by RDM.
pub struct SendStream {
    inner: iroh::endpoint::SendStream,
    conn: Arc<ConnectionInner>,
}

impl std::fmt::Debug for SendStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SendStream").finish()
    }
}

impl SendStream {
    fn map_write_err(&self, err: iroh::endpoint::WriteError) -> StreamError {
        match err {
            iroh::endpoint::WriteError::Stopped(code) => StreamError::Stopped {
                code: u32::try_from(code.into_inner()).unwrap_or(u32::MAX),
            },
            iroh::endpoint::WriteError::ClosedStream => StreamError::Finished,
            iroh::endpoint::WriteError::ConnectionLost(c) => {
                StreamError::Closed(classify_conn_err(&self.conn, &c))
            }
            iroh::endpoint::WriteError::ZeroRttRejected => StreamError::Closed(Closed::PeerLeft),
        }
    }

    /// Write bytes to the stream.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, StreamError> {
        if let Some(c) = self.conn.closed_rx.borrow().clone() {
            return Err(StreamError::Closed(c));
        }
        self.inner.write(buf).await.map_err(|e| self.map_write_err(e))
    }

    /// Write the entire `buf` to the stream.
    pub async fn write_all(&mut self, buf: &[u8]) -> Result<(), StreamError> {
        if let Some(c) = self.conn.closed_rx.borrow().clone() {
            return Err(StreamError::Closed(c));
        }
        self.inner
            .write_all(buf)
            .await
            .map_err(|e| self.map_write_err(e))
    }

    /// Signal the end of this send stream (`FIN`).
    pub fn finish(&mut self) -> Result<(), StreamError> {
        self.inner.finish().map_err(|_| StreamError::Finished)
    }

    /// Abruptly reset this send stream with `code`.
    pub fn reset(&mut self, code: u32) -> Result<(), StreamError> {
        self.inner
            .reset(iroh::endpoint::VarInt::from_u32(code))
            .map_err(|_| StreamError::Finished)
    }

    /// Wait for the peer to finish reading or stop this stream; returns `Err(StreamError::Stopped { code })`
    /// when the peer called `RecvStream::stop(code)`.
    pub async fn stopped(&mut self) -> Result<(), StreamError> {
        match self.inner.stopped().await {
            Ok(None) => Ok(()),
            Ok(Some(code)) => Err(StreamError::Stopped {
                code: u32::try_from(code.into_inner()).unwrap_or(u32::MAX),
            }),
            Err(iroh::endpoint::StoppedError::ConnectionLost(c)) => {
                Err(StreamError::Closed(classify_conn_err(&self.conn, &c)))
            }
            Err(iroh::endpoint::StoppedError::ZeroRttRejected) => {
                Err(StreamError::Closed(Closed::PeerLeft))
            }
        }
    }
}

impl AsyncWrite for SendStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if let Some(c) = self.conn.closed_rx.borrow().clone() {
            return Poll::Ready(Err(StreamError::Closed(c).into_io()));
        }
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(n)) => Poll::Ready(Ok(n)),
            Poll::Ready(Err(e)) => Poll::Ready(Err(self.map_write_err(e).into_io())),
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// The receive half of a bidirectional stream on a [`Connection`].
///
/// Application stream bytes are never spanned by RDM.
pub struct RecvStream {
    inner: iroh::endpoint::RecvStream,
    conn: Arc<ConnectionInner>,
}

impl std::fmt::Debug for RecvStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecvStream").finish()
    }
}

impl RecvStream {
    fn map_read_err(&self, err: iroh::endpoint::ReadError) -> StreamError {
        match err {
            iroh::endpoint::ReadError::Reset(code) => StreamError::Reset {
                code: u32::try_from(code.into_inner()).unwrap_or(u32::MAX),
            },
            iroh::endpoint::ReadError::ClosedStream => StreamError::Finished,
            iroh::endpoint::ReadError::ConnectionLost(c) => {
                StreamError::Closed(classify_conn_err(&self.conn, &c))
            }
            iroh::endpoint::ReadError::ZeroRttRejected => StreamError::Closed(Closed::PeerLeft),
        }
    }

    /// Read the next chunk of bytes from the stream; returns `Ok(None)` when the peer called
    /// [`SendStream::finish`].
    pub async fn read(&mut self, buf: &mut [u8]) -> Result<Option<usize>, StreamError> {
        let mut closed_rx = self.conn.closed_rx.clone();
        if let Some(c) = closed_rx.borrow().clone() {
            return Err(StreamError::Closed(c));
        }
        tokio::select! {
            res = self.inner.read(buf) => res.map_err(|e| self.map_read_err(e)),
            Ok(c_ref) = closed_rx.wait_for(|c| c.is_some()) => {
                let c = c_ref.clone().expect("is_some");
                Err(StreamError::Closed(c))
            }
        }
    }

    /// Read exact bytes into `buf`.
    pub async fn read_exact(&mut self, buf: &mut [u8]) -> Result<(), StreamError> {
        let mut closed_rx = self.conn.closed_rx.clone();
        if let Some(c) = closed_rx.borrow().clone() {
            return Err(StreamError::Closed(c));
        }
        tokio::select! {
            res = self.inner.read_exact(buf) => match res {
                Ok(()) => Ok(()),
                Err(iroh::endpoint::ReadExactError::FinishedEarly(_)) => Err(StreamError::Finished),
                Err(iroh::endpoint::ReadExactError::ReadError(e)) => Err(self.map_read_err(e)),
            },
            Ok(c_ref) = closed_rx.wait_for(|c| c.is_some()) => {
                let c = c_ref.clone().expect("is_some");
                Err(StreamError::Closed(c))
            }
        }
    }

    /// Read all remaining bytes up to `size_limit` until the peer finishes the stream.
    pub async fn read_to_end(&mut self, size_limit: usize) -> Result<Vec<u8>, StreamError> {
        let mut closed_rx = self.conn.closed_rx.clone();
        if let Some(c) = closed_rx.borrow().clone() {
            return Err(StreamError::Closed(c));
        }
        tokio::select! {
            res = self.inner.read_to_end(size_limit) => match res {
                Ok(bytes) => Ok(bytes),
                Err(iroh::endpoint::ReadToEndError::Read(e)) => Err(self.map_read_err(e)),
                Err(iroh::endpoint::ReadToEndError::TooLong) => Err(StreamError::Finished),
            },
            Ok(c_ref) = closed_rx.wait_for(|c| c.is_some()) => {
                let c = c_ref.clone().expect("is_some");
                Err(StreamError::Closed(c))
            }
        }
    }

    /// Tell the peer to stop sending on this stream with `code`.
    pub fn stop(&mut self, code: u32) -> Result<(), StreamError> {
        self.inner
            .stop(iroh::endpoint::VarInt::from_u32(code))
            .map_err(|_| StreamError::Finished)
    }
}

impl AsyncRead for RecvStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if let Some(c) = self.conn.closed_rx.borrow().clone() {
            return Poll::Ready(Err(StreamError::Closed(c).into_io()));
        }
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(())) => Poll::Ready(Ok(())),
            Poll::Ready(Err(e)) => {
                let se = e
                    .get_ref()
                    .and_then(|inner| inner.downcast_ref::<iroh::endpoint::ReadError>())
                    .map(|re| self.map_read_err(re.clone()))
                    .unwrap_or(StreamError::Closed(Closed::PeerLeft));
                Poll::Ready(Err(se.into_io()))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing_subscriber::layer::SubscriberExt as _;

    #[test]
    fn validate_alpns_carrier_policy_and_memory_sink_settle_proxy_on_direct_restore_and_drop_open_routes() {
        // docs/architecture/connections.md:49,89 — "`Direct` means an open data-plane
        // connection the source originated to the destination for traffic... `Proxy` means
        // the source has proven a usable two-hop path to the destination through one exact
        // carrier: `source -> carrier -> destination`. It is still logically a connection
        // from `source` to `destination`; `carrier` records how it is realized."
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (capture, layer) = crate::span_capture("connections-unit", exporter.clone());
        let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
        let _guard = tracing::dispatcher::set_default(&dispatch);

        // 1. validate_alpns refuses empty, duplicate, and control-plane ALPNs, and Alpn::carrier_policy maps proxy policy.
        let app_alpn = Alpn {
            name: b"app-data/1".to_vec(),
            proxy: true,
            carriers: vec![NodeKind::RpcNode],
        };
        let direct_only_alpn = Alpn {
            name: b"direct-only/1".to_vec(),
            proxy: false,
            carriers: vec![NodeKind::RpcNode],
        };
        assert_eq!(
            app_alpn.carrier_policy(),
            CarrierPolicy::Proxy {
                carrier_kind: NodeKind::RpcNode
            }
        );
        assert_eq!(direct_only_alpn.carrier_policy(), CarrierPolicy::NoProxy);
        assert!(validate_alpns(&[app_alpn.clone(), direct_only_alpn]).is_ok());
        assert!(validate_alpns(&[Alpn {
            name: Vec::new(),
            proxy: false,
            carriers: Vec::new(),
        }])
        .unwrap_err()
        .contains("empty name"));
        assert!(validate_alpns(&[app_alpn.clone(), app_alpn.clone()])
            .unwrap_err()
            .contains("duplicate Alpn"));
        assert!(validate_alpns(&[Alpn {
            name: crate::ALPN.to_vec(),
            proxy: false,
            carriers: Vec::new(),
        }])
        .unwrap_err()
        .contains("Node RPC ALPN"));
        assert!(validate_alpns(&[Alpn {
            name: iroh_gossip::ALPN.to_vec(),
            proxy: false,
            carriers: Vec::new(),
        }])
        .unwrap_err()
        .contains("gossip ALPN"));

        // 2. StreamError::from_io round-trips through into_io.
        let reset_err = StreamError::Reset { code: 42 };
        let io_err = reset_err.clone().into_io();
        assert_eq!(StreamError::from_io(&io_err), Some(reset_err));

        // 3. MemoryConnectionsSink records Proxy Connected, settles it with a Disconnected row when Direct Connected lands,
        // and owed_drop disconnects only open routes.
        let src_birth = Birth {
            name: PathName::new("mesh1", NodeKind::Gateway, 1),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
        };
        let carrier_birth = Birth {
            name: PathName::new("mesh1", NodeKind::Gateway, 2),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
        };
        let dst_birth = Birth {
            name: PathName::new("mesh1", NodeKind::Broker, 1),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
        };
        let sink = MemoryConnectionsSink::new(src_birth.clone());

        let proxy_row = sink.record(NodeConnection {
            id: ConnectionId::mint(),
            source: src_birth.to_end(),
            destination: dst_birth.to_end(),
            kind: CoreKind::Proxy,
            alpn: b"app-data/1".to_vec(),
            state: ConnectionState::Connected,
            carrier: Some(carrier_birth.to_end()),
            recovery: None,
            reason: None,
            logged_at_ms: 0,
        });
        assert_eq!(
            open_pair_source(&sink.held().lock().unwrap(), &proxy_row),
            Some(src_birth.to_end())
        );

        let direct_row = sink.record(NodeConnection {
            id: ConnectionId::mint(),
            source: src_birth.to_end(),
            destination: dst_birth.to_end(),
            kind: CoreKind::Direct,
            alpn: b"app-data/1".to_vec(),
            state: ConnectionState::Connected,
            carrier: None,
            recovery: None,
            reason: None,
            logged_at_ms: 0,
        });

        // Direct Connected automatically retired the Proxy entry with a Disconnected row.
        let history = sink.history();
        assert_eq!(history.len(), 3);
        assert_eq!(history[2].kind, CoreKind::Proxy);
        assert_eq!(history[2].state, ConnectionState::Disconnected);
        assert_eq!(
            open_pair_source(&sink.held().lock().unwrap(), &proxy_row),
            None
        );

        // Dropping the already-retired proxy_row writes nothing.
        sink.record_drops(&[proxy_row], "peer-left");
        assert_eq!(sink.history().len(), 3);

        // Even if the node renames its birth, owed_drop on the still-open direct_row uses the source the pair landed with.
        let renamed_birth = Birth {
            name: PathName::new("mesh1", NodeKind::Gateway, 9),
            node_id: src_birth.node_id.clone(),
            incarnation: src_birth.incarnation.clone(),
        };
        sink.set_own_birth(renamed_birth);
        sink.record_drops(&[direct_row.clone()], "peer-left");
        let after_drops = sink.history();
        assert_eq!(after_drops.len(), 4);
        assert_eq!(after_drops[3].kind, CoreKind::Direct);
        assert_eq!(after_drops[3].state, ConnectionState::Disconnected);
        assert_eq!(after_drops[3].reason.as_deref(), Some("peer-left"));

        capture
            .flush(std::time::Duration::from_secs(5))
            .expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        assert!(
            spans
                .iter()
                .any(|s| s.name == "rdm.node_admin.connection.update.via-observed"),
            "expected rdm.node_admin.connection.update.via-observed span"
        );
    }
}

