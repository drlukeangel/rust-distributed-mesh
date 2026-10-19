//! Data-plane `Connections` (`i148`): one endpoint, RDM owns every registered `Alpn` and the
//! accept loop, with `node.connections.admit` and the intent handshake on the first bi-stream.
//!
//! `iroh` never crosses this surface: [`SendStream`] and [`RecvStream`] wrap QUIC bi-streams and
//! surface [` finish`](SendStream::finish), [`reset`](SendStream::reset) and [`stop`](RecvStream::stop)
//! with their typed [`StreamError`] and [`Closed`] outcomes.

use crate::resolve::{NodeResolver, NodeTarget};
use rdm_core::connections::{
    CarrierPolicy, ConnectionEnd, ConnectionKind as CoreKind, ConnectionState, ConnectionsHeld,
    DirectRecovery, EffectiveRoute, INVALID_CARRIER_EDGE_LOST, NodeConnection,
    resolve_for_carriers, select_carrier_for_carriers,
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
use tracing::Instrument;

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
    pub forwardable: bool,
    /// The node kinds eligible to carry a Proxy for this protocol when `forwardable` is `true`.
    pub carriers: Vec<NodeKind>,
}

impl Alpn {
    /// The [`CarrierPolicy`] this registration states.
    pub fn carrier_policy(&self) -> CarrierPolicy {
        if self.forwardable {
            if let Some(&carrier_kind) = self.carriers.first() {
                return CarrierPolicy::Forwardable { carrier_kind };
            }
        }
        CarrierPolicy::NotForwardable
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
    /// The peer closed the connection or departed.
    PeerLeft,
    /// Closed by `node.connections.delete`.
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
    #[error("no active route")]
    NoActiveRoute,
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
    /// This birth's connections have been deleted (`node.connections.delete All`).
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

/// Where the data-plane `Connections` runtime records `NodeConnection` rows and reads the held projection.
#[async_trait::async_trait]
pub trait ConnectionsSink: Send + Sync {
    /// This node's current `Birth` (following its held name).
    fn own_birth(&self) -> Birth;
    /// The held projection, shared with route resolution.
    fn held(&self) -> Arc<Mutex<ConnectionsHeld>>;
    /// Persist and apply one connection row (`Connected`, `Disconnected`, `Failed`, or `Refused`).
    async fn record_row(&self, row: NodeConnection) -> Result<NodeConnection, String>;
    /// Write `Disconnected` drop rows for `routes` with `reason`.
    async fn record_drops(&self, routes: &[NodeConnection], reason: &str) -> Vec<ConnectionId>;
    /// Notify the sink that an inbound Direct connection from `peer` on `alpn` was admitted
    /// (`connections.md` §9.4, §10, §13 G: a recovery trigger, never a Direct row).
    fn accepted(&self, _peer: &Birth, _alpn: &[u8]) {}
    /// Refresh foreign resident `Direct` rows from the shared connections store before carrier
    /// discovery (`connections.md` §3.2, §4.1).
    async fn refresh_fleet_directs(&self) {}
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

    async fn record_row(&self, mut row: NodeConnection) -> Result<NodeConnection, String> {
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
            let owed = rdm_core::reconnect::owed_retirements(&self.held.lock().unwrap(), at);
            for retired in owed {
                self.history.lock().unwrap().push(retired.clone());
                let _ = self.held.lock().unwrap().apply(retired);
            }
        }
        Ok(row)
    }

    async fn record_drops(&self, routes: &[NodeConnection], reason: &str) -> Vec<ConnectionId> {
        let own = self.own_birth().to_end();
        let mut closed = Vec::new();
        for route in routes {
            let current = {
                let held = self.held.lock().unwrap();
                match route.kind {
                    CoreKind::Direct => held
                        .own_latest_directs()
                        .into_iter()
                        .any(|r| r.state == ConnectionState::Connected && r.id == route.id),
                    CoreKind::Proxy => held
                        .own_active_proxies()
                        .into_iter()
                        .any(|r| r.id == route.id),
                }
            };
            if !current {
                closed.push(route.id.clone());
                continue;
            }
            let now = self.clock.fetch_add(1, Ordering::SeqCst);
            let drop_row = NodeConnection {
                id: ConnectionId::mint(),
                source: own.clone(),
                destination: route.destination.clone(),
                kind: route.kind,
                alpn: route.alpn.clone(),
                state: ConnectionState::Disconnected,
                carrier: route.carrier.clone(),
                recovery: None,
                reason: Some(reason.to_string()),
                logged_at_ms: now,
            };
            self.history.lock().unwrap().push(drop_row.clone());
            let _ = self.held.lock().unwrap().apply(drop_row);
            closed.push(route.id.clone());
        }
        closed
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
        let _ = (roots, time);
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
        let traceparent = crate::traceparent_of(&caller_span);
        let own = self.inner.sink.own_birth();
        let conn = dialer
            .open_direct(&own, destination, &caller_span, traceparent)
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

    /// Block new outbound connections (`node.connections.delete All`).
    pub fn block_new_connections(&self) {
        self.inner.deleted.store(true, Ordering::SeqCst);
    }

    /// Unblock outbound connections after `ApplyNodeName` or restart.
    pub fn unblock_new_connections(&self) {
        self.inner.deleted.store(false, Ordering::SeqCst);
    }

    /// Still-open Proxy tunnels whose route in `held` was superseded by Direct restoration (§10),
    /// tagged with `reason: Some("route: retired")` for `node.connections.get` and `node.connections.delete`.
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
                "rdm.node.connections.delete.via-tunnel-ended",
                source = %row.source.name,
                carrier = %row.carrier.as_ref().map(|c| c.name.to_string()).unwrap_or_default(),
                destination = %row.destination.name,
                alpn = %String::from_utf8_lossy(&row.alpn),
                entry = %row.id,
            )
            .in_scope(|| tracing::info!("proxy tunnel ended without writing a second Disconnected row"));
        }
    }

    /// Close an active data-plane connection by its [`ConnectionId`] (`node.connections.delete`),
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
            Closed::ConnectionsDeleted => (CLOSE_CODE_CONNECTIONS_DELETED, "node.connections.delete"),
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
                    b"node.connections.delete",
                );
                if let Some(row) = entry.row {
                    self.emit_tunnel_ended_if_needed(&row);
                    rows.push(row);
                }
            }
        }
        if !rows.is_empty() {
            self.inner.sink.record_drops(&rows, "node.connections.delete").await;
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
        let alpn_str = String::from_utf8_lossy(&self.alpn.name).into_owned();
        let Some(admit::Admitted { mut ctrl_send, mut ctrl_recv, present }) = admit::admit(&self.registry, &self.alpn, &connection).await else {
            return Ok(());
        };

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
                let own_now = self.registry.inner.sink.own_birth();
                let tunnel_span = tracing::info_span!(
                    "rdm.node.connections.create.via-tunnel",
                    source = %origin.name,
                    carrier = %own_now.name,
                    destination = %destination.name,
                    alpn = %alpn_str,
                );
                if let Some(tp) = present.traceparent.as_deref() {
                    crate::set_parent(&tunnel_span, tp);
                }
                tunnel_span.in_scope(|| tracing::info!("carrier splicing data-plane proxy tunnel to destination"));
                let carrier_tp = crate::traceparent_of(&tunnel_span).or(present.traceparent.clone());

                let has_active_direct = {
                    let held = self.registry.inner.sink.held();
                    let g = held.lock().unwrap();
                    matches!(
                        g.active_direct_for(&own_now.name, &destination.name, &self.alpn.name),
                        Some(Some(_))
                    )
                };
                if !has_active_direct {
                    let _ = write_control_msg(
                        &mut ctrl_send,
                        &ControlAnswer::Refused {
                            reason: INVALID_CARRIER_EDGE_LOST.to_string(),
                        },
                    )
                    .await;
                    let _ = ctrl_send.finish();
                    let _ = tokio::time::timeout(floor, connection.closed()).await;
                    return Ok(());
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
                    return Ok(());
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
                    let _ = write_control_msg(
                        &mut ctrl_send,
                        &ControlAnswer::Refused {
                            reason: INVALID_CARRIER_EDGE_LOST.to_string(),
                        },
                    )
                    .await;
                    let _ = ctrl_send.finish();
                    let _ = tokio::time::timeout(floor, connection.closed()).await;
                    return Ok(());
                };

                let dst_addr =
                    iroh::EndpointAddr::new(resolved_dest.endpoint_id).with_ip_addr(resolved_dest.transport_addr);
                let Ok(Ok(dst_conn)) = tokio::time::timeout(
                    floor,
                    self.registry
                        .inner
                        .endpoint
                        .connect(dst_addr, &self.alpn.name)
                        .instrument(tunnel_span.clone()),
                )
                .await
                else {
                    let _ = write_control_msg(
                        &mut ctrl_send,
                        &ControlAnswer::Refused {
                            reason: INVALID_CARRIER_EDGE_LOST.to_string(),
                        },
                    )
                    .await;
                    let _ = ctrl_send.finish();
                    let _ = tokio::time::timeout(floor, connection.closed()).await;
                    return Ok(());
                };

                let Ok(Ok((mut dst_ctrl_send, mut dst_ctrl_recv))) =
                    tokio::time::timeout(floor, dst_conn.open_bi().instrument(tunnel_span.clone())).await
                else {
                    let _ = write_control_msg(
                        &mut ctrl_send,
                        &ControlAnswer::Refused {
                            reason: INVALID_CARRIER_EDGE_LOST.to_string(),
                        },
                    )
                    .await;
                    let _ = ctrl_send.finish();
                    let _ = tokio::time::timeout(floor, connection.closed()).await;
                    return Ok(());
                };

                let cert = self.registry.inner.member_cert.lock().unwrap().clone();
                let dst_present = PresentFrame {
                    caller: own_now.clone(),
                    cert,
                    traceparent: carrier_tp,
                };
                tunnel_span.in_scope(|| {
                    tracing::info_span!(
                        "rdm.node.connections.update.via-presented",
                        destination = %destination.name,
                        alpn = %alpn_str,
                        cert_expires_at = 0u64,
                    )
                    .in_scope(|| tracing::info!("carrier presented member credential to destination"));
                });
                if write_control_msg(&mut dst_ctrl_send, &dst_present).await.is_err() {
                    return Ok(());
                }

                let dst_verdict: AdmitVerdict =
                    match tokio::time::timeout(floor, read_control_msg(&mut dst_ctrl_recv)).await {
                        Ok(Ok(Some(v))) => v,
                        _ => return Ok(()),
                    };
                match dst_verdict {
                    AdmitVerdict::Refused { reason } => {
                        let _ = write_control_msg(
                            &mut ctrl_send,
                            &ControlAnswer::Refused {
                                reason: reason.clone(),
                            },
                        )
                        .await;
                        let _ = ctrl_send.finish();
                        dst_conn.close(
                            iroh::endpoint::VarInt::from_u32(CLOSE_CODE_REFUSED),
                            reason.as_bytes(),
                        );
                        let _ = tokio::time::timeout(floor, connection.closed()).await;
                        return Ok(());
                    }
                    AdmitVerdict::Admitted => {}
                }

                let dst_intent = ControlIntent::Tunnel {
                    id,
                    origin,
                    destination: destination.clone(),
                    alpn,
                    via: Some(own_now),
                };
                if write_control_msg(&mut dst_ctrl_send, &dst_intent).await.is_err() {
                    return Ok(());
                }

                let dst_answer: ControlAnswer =
                    match tokio::time::timeout(floor, read_control_msg(&mut dst_ctrl_recv)).await {
                        Ok(Ok(Some(a))) => a,
                        _ => return Ok(()),
                    };
                if write_control_msg(&mut ctrl_send, &dst_answer).await.is_err() {
                    return Ok(());
                }

                let opened_exact = match &dst_answer {
                    ControlAnswer::Opened { peer } => {
                        peer.incarnation == destination.incarnation && peer.node_id == destination.node_id
                    }
                    ControlAnswer::Refused { .. } => false,
                };
                if !opened_exact {
                    let _ = ctrl_send.finish();
                    let _ = tokio::time::timeout(floor, connection.closed()).await;
                    dst_conn.close(
                        iroh::endpoint::VarInt::from_u32(CLOSE_CODE_PEER_LEFT),
                        b"tunnel-rejected",
                    );
                    return Ok(());
                }

                self.registry
                    .inner
                    .carrier_legs
                    .lock()
                    .unwrap()
                    .push((connection.clone(), dst_conn.clone()));
                let _keep_ctrl = (ctrl_send, ctrl_recv, dst_ctrl_send, dst_ctrl_recv);
                splice_tunnel(connection, dst_conn).await;
            }
        }
        Ok(())
    }

    async fn shutdown(&self) {
        self.registry.shutdown_all();
    }
}

async fn forward_stream_dir(
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

async fn splice_tunnel(
    src_conn: iroh::endpoint::Connection,
    dst_conn: iroh::endpoint::Connection,
) {
    let s_to_d = {
        let src = src_conn.clone();
        let dst = dst_conn.clone();
        async move {
            while let Ok((src_send, src_recv)) = src.accept_bi().await {
                let Ok((dst_send, dst_recv)) = dst.open_bi().await else {
                    break;
                };
                tokio::spawn(forward_stream_dir(src_recv, dst_send));
                tokio::spawn(forward_stream_dir(dst_recv, src_send));
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
                tokio::spawn(forward_stream_dir(dst_recv, src_send));
                tokio::spawn(forward_stream_dir(src_recv, dst_send));
            }
        }
    };
    tokio::select! {
        _ = s_to_d => {}
        _ = d_to_s => {}
        err = src_conn.closed() => {
            propagate_leg_close(&err, &dst_conn);
        }
        err = dst_conn.closed() => {
            propagate_leg_close(&err, &src_conn);
        }
    }
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
        let traceparent = crate::traceparent_of(&caller_span);
        let own = self.registry.inner.sink.own_birth();
        let res = {
            let held = self.registry.inner.sink.held();
            let g = held.lock().unwrap();
            resolve_for_carriers(
                &g,
                &own.name,
                &destination.name,
                &self.alpn.name,
                self.alpn.forwardable,
                &self.alpn.carriers,
            )
        };
        if let Some((invalid_proxy, reason)) = res.retire {
            self.registry
                .inner
                .sink
                .record_drops(&[invalid_proxy], reason)
                .await;
        }
        match res.route {
            EffectiveRoute::NoActiveRoute => return Err(ConnectError::NoActiveRoute),
            EffectiveRoute::ViaPeer { proxy, .. } => {
                let Some(carrier_end) = proxy.carrier.as_ref() else {
                    self.registry
                        .inner
                        .sink
                        .record_drops(std::slice::from_ref(&proxy), INVALID_CARRIER_EDGE_LOST)
                        .await;
                    return Err(ConnectError::NoActiveRoute);
                };
                let Some(carrier_inc) = carrier_end.incarnation.clone() else {
                    self.registry
                        .inner
                        .sink
                        .record_drops(std::slice::from_ref(&proxy), INVALID_CARRIER_EDGE_LOST)
                        .await;
                    return Err(ConnectError::NoActiveRoute);
                };
                let carrier_birth = Birth {
                    name: carrier_end.name.clone(),
                    node_id: carrier_end.node_id.clone(),
                    incarnation: carrier_inc,
                };
                return self
                    .open_tunnel(&own, &carrier_birth, destination, Some(&proxy), &caller_span, traceparent)
                    .await;
            }
            EffectiveRoute::Direct { .. } => {}
        }

        match self
            .open_direct(&own, destination, &caller_span, traceparent.clone())
            .await
        {
            Ok(conn) => Ok(conn),
            Err(ConnectError::NotSent { reason }) if self.alpn.forwardable => {
                self.registry.inner.sink.refresh_fleet_directs().await;
                let choice = {
                    let held = self.registry.inner.sink.held();
                    let g = held.lock().unwrap();
                    select_carrier_for_carriers(
                        &g,
                        &own.name,
                        &destination.name,
                        &self.alpn.name,
                        self.alpn.forwardable,
                        &self.alpn.carriers,
                    )
                };
                if let Ok(Some(choice)) = choice {
                    if let Some(carrier_inc) = choice.edge.source.incarnation.clone() {
                        let carrier_birth = Birth {
                            name: choice.edge.source.name.clone(),
                            node_id: choice.edge.source.node_id.clone(),
                            incarnation: carrier_inc,
                        };
                        return self
                            .open_tunnel(&own, &carrier_birth, destination, None, &caller_span, traceparent)
                            .await;
                    }
                }
                Err(ConnectError::NotSent { reason })
            }
            Err(e) => Err(e),
        }
    }

    async fn open_direct(
        &self,
        own: &Birth,
        destination: &Birth,
        caller_span: &tracing::Span,
        traceparent: Option<String>,
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
            self.record_direct_failure(own, destination, &reason, caller_span)
                .await;
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
                self.record_direct_failure(own, destination, &reason, caller_span).await;
                return Err(ConnectError::NotSent { reason });
            }
        };

        let addr = iroh::EndpointAddr::new(resolved.endpoint_id).with_ip_addr(resolved.transport_addr);
        let connect_fut = self
            .registry
            .inner
            .endpoint
            .connect(addr, &self.alpn.name)
            .instrument(caller_span.clone());
        let quic_conn = match connect_fut.await {
            Ok(c) => c,
            Err(e) => {
                let reason = e.to_string();
                self.record_direct_failure(own, destination, &reason, caller_span).await;
                return Err(ConnectError::NotSent { reason });
            }
        };

        let (mut ctrl_send, mut ctrl_recv) = match quic_conn.open_bi().instrument(caller_span.clone()).await {
            Ok(bi) => bi,
            Err(e) => {
                let reason = e.to_string();
                self.record_direct_failure(own, destination, &reason, caller_span).await;
                return Err(ConnectError::NotSent { reason });
            }
        };

        let cert = self.registry.inner.member_cert.lock().unwrap().clone();
        let alpn_str = String::from_utf8_lossy(&self.alpn.name).into_owned();
        let present = PresentFrame {
            caller: own.clone(),
            cert,
            traceparent,
        };
        caller_span.in_scope(|| {
            tracing::info_span!(
                "rdm.node.connections.update.via-presented",
                destination = %destination.name,
                alpn = %alpn_str,
                cert_expires_at = 0u64,
            )
            .in_scope(|| tracing::info!("presented member credential on the connection's control stream"));
        });

        if let Err(reason) = write_control_msg(&mut ctrl_send, &present).await {
            self.record_direct_failure(own, destination, &reason, caller_span).await;
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
                self.record_direct_failure(own, destination, &reason, caller_span).await;
                return Err(ConnectError::NotSent { reason });
            }
            Ok(Err(reason)) => {
                self.record_direct_failure(own, destination, &reason, caller_span).await;
                return Err(ConnectError::NotSent { reason });
            }
            Err(_) => {
                let reason = "timed out waiting for admission verdict".to_string();
                self.record_direct_failure(own, destination, &reason, caller_span).await;
                return Err(ConnectError::NotSent { reason });
            }
        };

        match verdict {
            // A refusal is a named end of this dial (node-connections-admit.md step 5): a Direct
            // Failed row with the acceptor's reason, so this source's reconnect series exists and
            // continues from it (connections.md §9.1, §9.3).
            AdmitVerdict::Refused { reason } => {
                self.record_direct_failure(own, destination, &format!("refused: {reason}"), caller_span).await;
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
            self.record_direct_failure(own, destination, &reason, caller_span).await;
            return Err(ConnectError::NotSent { reason });
        }

        let answer: ControlAnswer = match tokio::time::timeout(floor, read_control_msg(&mut ctrl_recv)).await {
            Ok(Ok(Some(a))) => a,
            Ok(Ok(None)) => {
                let reason = "acceptor closed control stream before intent answer".to_string();
                self.record_direct_failure(own, destination, &reason, caller_span).await;
                return Err(ConnectError::NotSent { reason });
            }
            Ok(Err(reason)) => {
                self.record_direct_failure(own, destination, &reason, caller_span).await;
                return Err(ConnectError::NotSent { reason });
            }
            Err(_) => {
                let reason = "timed out waiting for intent answer".to_string();
                self.record_direct_failure(own, destination, &reason, caller_span).await;
                return Err(ConnectError::NotSent { reason });
            }
        };

        match answer {
            ControlAnswer::Refused { reason } => Err(ConnectError::Refused { reason }),
            ControlAnswer::Opened { peer } => {
                if peer.incarnation != destination.incarnation || peer.node_id != destination.node_id {
                    quic_conn.close(iroh::endpoint::VarInt::from_u32(CLOSE_CODE_PEER_LEFT), b"wrong-incarnation");
                    return Err(ConnectError::WrongIncarnation {
                        held: destination.incarnation.clone(),
                        found: peer.incarnation,
                    });
                }
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
                let recorded = self
                    .registry
                    .inner
                    .sink
                    .record_row(row)
                    .instrument(caller_span.clone())
                    .await
                    .map_err(|reason| ConnectError::NotSent { reason })?;
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
        caller_span: &tracing::Span,
        traceparent: Option<String>,
    ) -> Result<Connection, ConnectError> {
        let alpn_str = String::from_utf8_lossy(&self.alpn.name).into_owned();
        let tunnel_span = caller_span.in_scope(|| {
            tracing::info_span!(
                "rdm.node.connections.create.via-tunnel",
                source = %own.name,
                carrier = %carrier.name,
                destination = %destination.name,
                alpn = %alpn_str,
                entry = tracing::field::Empty,
                kind = tracing::field::Empty,
                state = tracing::field::Empty,
            )
        });
        tunnel_span.in_scope(|| tracing::info!("opening data-plane proxy tunnel through carrier"));
        let tunnel_tp = crate::traceparent_of(&tunnel_span).or(traceparent);

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

        let addr = iroh::EndpointAddr::new(resolved.endpoint_id).with_ip_addr(resolved.transport_addr);
        let quic_conn = self
            .registry
            .inner
            .endpoint
            .connect(addr, &self.alpn.name)
            .instrument(tunnel_span.clone())
            .await
            .map_err(|e| ConnectError::NotSent {
                reason: e.to_string(),
            })?;

        let (mut ctrl_send, mut ctrl_recv) = quic_conn
            .open_bi()
            .instrument(tunnel_span.clone())
            .await
            .map_err(|e| ConnectError::NotSent {
                reason: e.to_string(),
            })?;

        let cert = self.registry.inner.member_cert.lock().unwrap().clone();
        let present = PresentFrame {
            caller: own.clone(),
            cert,
            traceparent: tunnel_tp,
        };
        tunnel_span.in_scope(|| {
            tracing::info_span!(
                "rdm.node.connections.update.via-presented",
                destination = %carrier.name,
                alpn = %alpn_str,
                cert_expires_at = 0u64,
            )
            .in_scope(|| tracing::info!("presented member credential to carrier on tunnel control stream"));
        });

        write_control_msg(&mut ctrl_send, &present)
            .await
            .map_err(|reason| ConnectError::NotSent { reason })?;

        let floor = rdm_core::cadence::staleness_floor();
        let verdict: AdmitVerdict = match tokio::time::timeout(floor, read_control_msg(&mut ctrl_recv)).await {
            Ok(Ok(Some(v))) => v,
            Ok(Ok(None)) => {
                return Err(ConnectError::NotSent {
                    reason: "carrier closed control stream before verdict".to_string(),
                });
            }
            Ok(Err(reason)) => return Err(ConnectError::NotSent { reason }),
            Err(_) => {
                return Err(ConnectError::NotSent {
                    reason: "timed out waiting for carrier admission verdict".to_string(),
                });
            }
        };

        match verdict {
            AdmitVerdict::Refused { reason } => {
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
        write_control_msg(&mut ctrl_send, &intent)
            .await
            .map_err(|reason| ConnectError::NotSent { reason })?;

        let answer: ControlAnswer = match tokio::time::timeout(floor, read_control_msg(&mut ctrl_recv)).await {
            Ok(Ok(Some(a))) => a,
            Ok(Ok(None)) => {
                return Err(ConnectError::NotSent {
                    reason: "carrier closed control stream before tunnel answer".to_string(),
                });
            }
            Ok(Err(reason)) => return Err(ConnectError::NotSent { reason }),
            Err(_) => {
                return Err(ConnectError::NotSent {
                    reason: "timed out waiting for tunnel answer".to_string(),
                });
            }
        };

        match answer {
            ControlAnswer::Refused { reason } => {
                quic_conn.close(
                    iroh::endpoint::VarInt::from_u32(CLOSE_CODE_REFUSED),
                    reason.as_bytes(),
                );
                if reason == INVALID_CARRIER_EDGE_LOST {
                    if let Some(proxy) = held_proxy {
                        self.registry
                            .inner
                            .sink
                            .record_drops(std::slice::from_ref(proxy), INVALID_CARRIER_EDGE_LOST)
                            .await;
                    }
                }
                Err(ConnectError::Refused { reason })
            }
            ControlAnswer::Opened { peer } => {
                if peer.incarnation != destination.incarnation || peer.node_id != destination.node_id {
                    quic_conn.close(
                        iroh::endpoint::VarInt::from_u32(CLOSE_CODE_PEER_LEFT),
                        b"wrong-incarnation",
                    );
                    return Err(ConnectError::WrongIncarnation {
                        held: destination.incarnation.clone(),
                        found: peer.incarnation,
                    });
                }
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
                        self.registry
                            .inner
                            .sink
                            .record_row(row)
                            .instrument(tunnel_span.clone())
                            .await
                            .map_err(|reason| ConnectError::NotSent { reason })?
                    }
                };
                tunnel_span.record("entry", recorded.id.to_string().as_str());
                tunnel_span.record("kind", "proxy");
                tunnel_span.record("state", "connected");
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
                ))
            }
        }
    }

    async fn record_direct_failure(
        &self,
        own: &Birth,
        destination: &Birth,
        reason: &str,
        caller_span: &tracing::Span,
    ) {
        let dest_end = destination.to_end();
        let previous = {
            let held = self.registry.inner.sink.held();
            let g = held.lock().unwrap();
            g.own_latest_directs()
                .into_iter()
                .find(|d| d.destination.name == dest_end.name && d.alpn == self.alpn.name)
                .cloned()
        };
        let row = match previous.as_ref() {
            Some(p)
                if p.state == ConnectionState::Failed
                    && (&p.destination.node_id, &p.destination.incarnation)
                        == (&dest_end.node_id, &dest_end.incarnation) =>
            {
                match p.recovery {
                    Some(r) => NodeConnection {
                        id: ConnectionId::mint(),
                        source: own.to_end(),
                        destination: dest_end,
                        recovery: Some(DirectRecovery {
                            recovery_round: r.recovery_round,
                            attempt_ordinal: r.attempt_ordinal.saturating_add(1),
                        }),
                        reason: Some(reason.to_string()),
                        logged_at_ms: 0,
                        ..p.clone()
                    },
                    None => first_failure_for(
                        own.to_end(),
                        dest_end,
                        self.alpn.name.clone(),
                        previous.as_ref(),
                        reason,
                        0,
                    ),
                }
            }
            _ => first_failure_for(
                own.to_end(),
                dest_end,
                self.alpn.name.clone(),
                previous.as_ref(),
                reason,
                0,
            ),
        };
        let _ = self
            .registry
            .inner
            .sink
            .record_row(row)
            .instrument(caller_span.clone())
            .await;
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
            }),
        }
    }

    /// The connection row's id (`node.connections.get` lists it; `node.connections.delete` closes it).
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
            if code == CLOSE_CODE_CONNECTIONS_DELETED || reason_str == "node.connections.delete" {
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
    /// when the peer called [`RecvStream::stop(code)`].
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
