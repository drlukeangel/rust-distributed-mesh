//! Node-to-node connections: the generic connection model (i143.e4.s2).
//!
//! A [`NodeConnection`] is one fact about one `(source, destination, kind)`: a `Direct` pooled
//! connection between the two ends, or a `Proxy`, a proven two-hop path through one exact
//! carrier. Its state is `Connected`, `Disconnected`, or `Failed` (this source's own direct
//! attempt failed; never proof the destination is dead).
//!
//! Each end names its path, its logical [`NodeId`] and, when the writer knew it, the process
//! [`IncarnationId`] it served. Incarnations are compared by equality only.
//!
//! [`ConnectionsHeld`] keeps the latest stamp per `(source, destination, kind)`. Only an active
//! Direct fact is resident fleet-wide; a later Disconnected or Failed entry removes its edge and
//! keeps only the stamp, so an older connect that arrives after it cannot bring the edge back.
//! The one exception is source-owned: this node's own active Proxy per destination.
//!
//! [`resolve`] answers how a new invocation from this node reaches one exact destination under
//! its protocol's [`CarrierPolicy`]: a valid own Proxy, else an active Direct, else no route.
//! A Proxy is valid only while the destination process and the carrier process it recorded are the
//! ones the topology view holds now (a superseded one is fenced by name) and the carrier's own Direct
//! edge to the destination is Active. The incarnations reach the source through the topology view
//! ([`CurrentIncarnations`]); the carrier's edge is the carrier's own fact, learned from the
//! carried call that returns `carrier-edge-lost` (connections.md section 8), never held here.
//! A Direct fact naming a destination process the topology view no longer holds is not current either.

use crate::ids::{IncarnationId, NodeId};
use crate::path::{NodeKind, PathName};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// What the topology view holds of one node a connection fact names.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Birth {
    /// The node's current process birth.
    Current(IncarnationId),
    /// The node left, or another node holds its path now: no birth of it is current.
    Departed,
    /// The topology view does not hold the node: nothing is judged.
    Unknown,
}

/// The births the topology view holds now, as the holder's topology view knows them. Facts, never policy.
pub trait CurrentIncarnations: Send + Sync + std::fmt::Debug {
    /// The process birth the topology view holds for `node_id` at `path`.
    fn birth(&self, node_id: &NodeId, path: &PathName) -> Birth;
}

/// A fixed table of current births per node: the topology view for a holder fed by hand.
#[derive(Debug, Default)]
pub struct StaticIncarnations(std::sync::RwLock<HashMap<NodeId, (PathName, Option<IncarnationId>)>>);

impl StaticIncarnations {
    /// `node_id` at `path` holds `incarnation` now.
    pub fn set(&self, node_id: NodeId, path: PathName, incarnation: IncarnationId) {
        self.0.write().unwrap().insert(node_id, (path, Some(incarnation)));
    }

    /// `node_id` departed.
    pub fn depart(&self, node_id: NodeId, path: PathName) {
        self.0.write().unwrap().insert(node_id, (path, None));
    }
}

impl CurrentIncarnations for StaticIncarnations {
    fn birth(&self, node_id: &NodeId, _path: &PathName) -> Birth {
        match self.0.read().unwrap().get(node_id) {
            Some((_, Some(i))) => Birth::Current(i.clone()),
            Some((_, None)) => Birth::Departed,
            None => Birth::Unknown,
        }
    }
}

/// The Node RPC control-plane ALPN, used as the default when a caller does not name an application ALPN.
pub const ALPN: &[u8] = b"rafka-node-rpc/1";

/// An entry's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ConnectionState {
    /// The connection stands.
    Connected,
    /// The connection ended.
    Disconnected,
    /// This source failed to establish the Direct connection. Never proof the destination is dead.
    Failed,
    /// An acceptor dropped an inbound connection at admission (connections.md §1.2). Never proof about the dialer.
    Refused,
}

/// An entry's kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum ConnectionKind {
    /// A pooled connection between source and destination.
    Direct,
    /// A proven two-hop path `source -> carrier -> destination`.
    Proxy,
}

/// One end of an entry, or a Proxy's carrier: its path, logical node and process birth.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionEnd {
    /// The end's `path.name`.
    pub name: PathName,
    /// The end's logical node id.
    pub node_id: NodeId,
    /// `None` when the writer could not learn the process birth; such an end is never fencing
    /// evidence.
    pub incarnation: Option<IncarnationId>,
}

/// Where a Direct `Failed` entry sits in its reconnect series. `recovery_round` names one failure
/// incident and is compared for equality only; `attempt_ordinal` counts its attempts from 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DirectRecovery {
    /// The incident the recovery series belongs to, compared for equality only.
    pub recovery_round: u64,
    /// This attempt's ordinal within the series, from 1.
    pub attempt_ordinal: u32,
}

/// What the projection keys an entry by: one `(source, destination, alpn)` has at most its Direct and its Proxy.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ConnectionIndex {
    /// The source node's `path.name`.
    pub source: PathName,
    /// The destination node's `path.name`.
    pub destination: PathName,
    /// The registered ALPN this connection carries (connections.md §3).
    pub alpn: Vec<u8>,
    /// Whether the entry is the pair's Direct or its Proxy.
    pub kind: ConnectionKind,
}

/// One connection fact.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeConnection {
    /// The entry's own minted id: every fact written is a new entry with a new id, never a copy of
    /// an earlier one's. An entry without one is refused when it is read.
    pub id: crate::ConnectionId,
    /// The node that holds the connection.
    pub source: ConnectionEnd,
    /// The node it reaches.
    pub destination: ConnectionEnd,
    /// Whether the entry is a Direct or a Proxy.
    pub kind: ConnectionKind,
    /// The registered ALPN this connection carries; a row is one transport connection on one protocol (connections.md §2).
    #[serde(default = "default_alpn")]
    pub alpn: Vec<u8>,
    /// The state of the entry.
    pub state: ConnectionState,
    /// `Some` on a Proxy entry, `None` on a Direct one.
    pub carrier: Option<ConnectionEnd>,
    /// `Some` on a Direct `Failed` entry, `None` on every other.
    pub recovery: Option<DirectRecovery>,
    /// Why a drop or a failure happened; `None` on a connect.
    pub reason: Option<String>,
    /// When the fact was written (ms).
    pub logged_at_ms: u64,
}

fn default_alpn() -> Vec<u8> {
    ALPN.to_vec()
}

impl NodeConnection {
    /// The index the entry is keyed by.
    pub fn index(&self) -> ConnectionIndex {
        ConnectionIndex {
            source: self.source.name.clone(),
            destination: self.destination.name.clone(),
            alpn: self.alpn.clone(),
            kind: self.kind,
        }
    }

    /// The only kind of entry the fleet-wide projection holds.
    pub(crate) fn is_active_direct(&self) -> bool {
        self.kind == ConnectionKind::Direct && self.state == ConnectionState::Connected
    }

    /// The order entries of one key settle in: a later entry wins, and at one instant a drop or a
    /// failure wins over a connect, so every holder settles on the same entry whichever arrives
    /// first.
    pub fn stamp(&self) -> u64 {
        self.logged_at_ms.saturating_mul(2) + u64::from(self.state != ConnectionState::Connected)
    }

    /// Why this entry's shape is not one the model admits, or `None` when it is.
    pub(crate) fn shape_refusal(&self) -> Option<String> {
        let at = || format!("connection {}->{} ({:?}, {:?})", self.source.name, self.destination.name, self.kind, self.state);
        if self.alpn.is_empty() {
            return Some(format!("{}: an entry must name a non-empty alpn", at()));
        }
        match (self.kind, self.state, self.carrier.is_some(), self.recovery.is_some()) {
            (ConnectionKind::Proxy, ConnectionState::Failed, _, _) => Some(format!("{}: Failed is a Direct state only", at())),
            (ConnectionKind::Proxy, ConnectionState::Refused, _, _) => Some(format!("{}: Refused is a Direct state only", at())),
            (ConnectionKind::Direct, ConnectionState::Failed, _, false) => {
                Some(format!("{}: a Direct Failed entry carries its recovery round and attempt ordinal", at()))
            }
            (_, ConnectionState::Connected | ConnectionState::Disconnected | ConnectionState::Refused, _, true) => {
                Some(format!("{}: only a Direct Failed entry carries a recovery", at()))
            }
            (ConnectionKind::Proxy, _, false, _) => Some(format!("{}: a Proxy entry names its carrier", at())),
            (ConnectionKind::Direct, _, true, _) => Some(format!("{}: a Direct entry names no carrier", at())),
            _ => None,
        }
    }
}

/// One key's latest stamp, its entry only when that entry is an active Direct fact, and whether
/// that entry is a Direct Failed one (connections.md section 9.1: the only latest Direct a reconnect
/// series stands on).
#[derive(Debug, Clone)]
struct Cell {
    stamp: u64,
    active: Option<NodeConnection>,
    /// The destination end a latest Direct Failed entry names: a series stands on it only while the
    /// topology view still holds that process (§9.1).
    failed: Option<ConnectionEnd>,
}

/// The held connections projection: active Direct facts fleet-wide, plus this node's own active
/// Proxy per `(destination, alpn)`.
#[derive(Debug, Default)]
pub struct ConnectionsHeld {
    cells: HashMap<ConnectionIndex, Cell>,
    /// Set once the holder has every route (born full); until then it cannot answer absence.
    complete: bool,
    own_source: Option<PathName>,
    own_proxies: HashMap<(PathName, Vec<u8>), NodeConnection>,
    /// This node's latest Direct entry per `(destination, alpn)`, whatever its state: what its reconnect
    /// series is reconstructed from.
    own_directs: HashMap<(PathName, Vec<u8>), NodeConnection>,
    /// The topology view's current process birth per path; absent until the holder is given one.
    topology_view: Option<std::sync::Arc<dyn CurrentIncarnations>>,
}

/// Why an entry was not applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyRefusal {
    /// Its shape is not one the model admits.
    Shape(String),
    /// Its key already stands at this stamp or a later one.
    Stale {
        /// The stamp held.
        held: u64,
        /// The stamp offered.
        offered: u64,
    },
}

impl ConnectionsHeld {
    /// An empty holder: not complete, no entries.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply one entry: it becomes the latest for its key when its stamp is newer than the one
    /// held. Only an active Direct entry (or this node's own Proxy) stays resident.
    pub fn apply(&mut self, entry: NodeConnection) -> Result<(), ApplyRefusal> {
        if let Some(refusal) = entry.shape_refusal() {
            return Err(ApplyRefusal::Shape(refusal));
        }
        let key = entry.index();
        let stamp = entry.stamp();
        if let Some(held) = self.cells.get(&key) {
            if held.stamp >= stamp {
                return Err(ApplyRefusal::Stale { held: held.stamp, offered: stamp });
            }
        }
        if entry.kind == ConnectionKind::Direct && entry.state != ConnectionState::Refused && self.own_source.as_ref() == Some(&entry.source.name) {
            self.own_directs.insert((entry.destination.name.clone(), entry.alpn.clone()), entry.clone());
        }
        if entry.kind == ConnectionKind::Proxy && self.own_source.as_ref() == Some(&entry.source.name) {
            if entry.state == ConnectionState::Connected {
                self.own_proxies.insert((entry.destination.name.clone(), entry.alpn.clone()), entry.clone());
            } else {
                self.own_proxies.remove(&(entry.destination.name.clone(), entry.alpn.clone()));
            }
        }
        // A `Refused` fact is the acceptor's fact about one dial, never proof about the dialer
        // (node-connections-admit.md Never): it removes no active edge and clears no failure; only a
        // Disconnected or Failed entry removes an edge (connections.md), so its key keeps what it held.
        if entry.state == ConnectionState::Refused {
            let (active, failed) = self.cells.remove(&key).map_or((None, None), |held| (held.active, held.failed));
            self.cells.insert(key, Cell { stamp, active, failed });
            return Ok(());
        }
        let failed = (entry.kind == ConnectionKind::Direct && entry.state == ConnectionState::Failed).then(|| entry.destination.clone());
        let active = entry.is_active_direct().then_some(entry);
        self.cells.insert(key, Cell { stamp, active, failed });
        Ok(())
    }

    /// Name this node's own path: from here on its active Proxy entries are held.
    pub fn set_own_source(&mut self, name: PathName) {
        if self.own_source.as_ref() != Some(&name) {
            self.own_source = Some(name);
            self.own_proxies.clear();
            self.own_directs.clear();
        }
    }

    /// This node's own path, as last named ([`Self::set_own_source`]): `None` until it is named.
    pub fn own_source(&self) -> Option<&PathName> {
        self.own_source.as_ref()
    }

    /// Give the holder the topology view its incarnation judgements read.
    pub fn set_topology_view(&mut self, topology_view: std::sync::Arc<dyn CurrentIncarnations>) {
        self.topology_view = Some(topology_view);
    }

    /// Whether `end` names a process the topology view does not hold now: its node departed, another
    /// node holds its path, or the node is here under another birth. An end naming no process is
    /// judged only on departure; a node the topology view does not hold is not judged.
    pub fn end_superseded(&self, end: &ConnectionEnd) -> Option<bool> {
        let m = self.topology_view.as_ref()?;
        match m.birth(&end.node_id, &end.name) {
            Birth::Current(now) => Some(end.incarnation.as_ref().is_some_and(|named| *named != now)),
            Birth::Departed => Some(true),
            Birth::Unknown => None,
        }
    }

    /// Whether `edge` names a destination process the topology view no longer holds: such a fact is
    /// evidence about a birth that is gone, never about the current one.
    pub(crate) fn names_superseded_destination(&self, edge: &NodeConnection) -> bool {
        self.end_superseded(&edge.destination).unwrap_or(false)
    }

    /// The holder now has every route.
    pub fn mark_complete(&mut self) {
        self.complete = true;
    }

    /// Whether the holder has every route.
    pub fn is_complete(&self) -> bool {
        self.complete
    }

    /// This node's active Proxy to `destination` on the default ALPN: `Some(None)` when it holds
    /// none, `None` when it cannot answer (not complete, or no own source named).
    pub(crate) fn own_proxy(&self, destination: &PathName) -> Option<Option<&NodeConnection>> {
        self.own_proxy_for(destination, ALPN)
    }

    /// This node's active Proxy to `destination` on `alpn`: `Some(None)` when it holds none,
    /// `None` when it cannot answer (not complete, or no own source named).
    pub fn own_proxy_for(&self, destination: &PathName, alpn: &[u8]) -> Option<Option<&NodeConnection>> {
        if !self.complete || self.own_source.is_none() {
            return None;
        }
        Some(self.own_proxies.get(&(destination.clone(), alpn.to_vec())))
    }

    /// This node's latest Direct entry per `(destination, alpn)`, whatever its state, sorted by destination then ALPN.
    pub fn own_latest_directs(&self) -> Vec<&NodeConnection> {
        let mut out: Vec<&NodeConnection> = self.own_directs.values().collect();
        out.sort_by(|a, b| (&a.destination.name, &a.alpn).cmp(&(&b.destination.name, &b.alpn)));
        out
    }

    /// Every active Direct fact held, whatever its source, leaving out a fact that names a
    /// superseded destination process; sorted by source then destination then ALPN.
    pub fn active_directs(&self) -> Vec<&NodeConnection> {
        let mut out: Vec<&NodeConnection> =
            self.cells.values().filter_map(|c| c.active.as_ref()).filter(|e| !self.names_superseded_destination(e)).collect();
        out.sort_by(|a, b| (&a.source.name, &a.destination.name, &a.alpn).cmp(&(&b.source.name, &b.destination.name, &b.alpn)));
        out
    }

    /// The active Direct entry whose id is `id`, whatever its source and whether or not its
    /// destination is superseded: the pair that connection opened, as it landed.
    pub fn active_direct_by_id(&self, id: &crate::ConnectionId) -> Option<&NodeConnection> {
        self.cells.values().filter_map(|c| c.active.as_ref()).find(|e| &e.id == id)
    }

    /// This node's active Proxy entries, sorted by destination then ALPN.
    pub fn own_active_proxies(&self) -> Vec<&NodeConnection> {
        let mut out: Vec<&NodeConnection> = self.own_proxies.values().collect();
        out.sort_by(|a, b| (&a.destination.name, &a.alpn).cmp(&(&b.destination.name, &b.alpn)));
        out
    }

    /// The active Direct fact `source -> destination` on the default ALPN: `Some(None)` when none
    /// is held, `None` when the holder is not complete.
    pub fn active_direct(&self, source: &PathName, destination: &PathName) -> Option<Option<&NodeConnection>> {
        self.active_direct_for(source, destination, ALPN)
    }

    /// The active Direct fact `source -> destination` on `alpn`: `Some(None)` when none is held,
    /// `None` when the holder is not complete.
    pub fn active_direct_for(&self, source: &PathName, destination: &PathName, alpn: &[u8]) -> Option<Option<&NodeConnection>> {
        if !self.complete {
            return None;
        }
        let key = ConnectionIndex { source: source.clone(), destination: destination.clone(), alpn: alpn.to_vec(), kind: ConnectionKind::Direct };
        Some(self.cells.get(&key).and_then(|c| c.active.as_ref()))
    }

    /// Whether any Direct fact (active or not) `source -> destination` on the default ALPN is held
    /// at all: the key stands at a stamp. `false` when none was ever observed.
    pub fn holds_direct_fact(&self, source: &PathName, destination: &PathName) -> bool {
        self.holds_direct_fact_for(source, destination, ALPN)
    }

    /// Whether any Direct fact (active or not) `source -> destination` on `alpn` is held at all:
    /// the key stands at a stamp. `false` when none was ever observed.
    pub fn holds_direct_fact_for(&self, source: &PathName, destination: &PathName, alpn: &[u8]) -> bool {
        let key = ConnectionIndex { source: source.clone(), destination: destination.clone(), alpn: alpn.to_vec(), kind: ConnectionKind::Direct };
        self.cells.contains_key(&key)
    }

    /// Whether the latest Direct fact `source -> destination` on `alpn` is Failed for the destination
    /// process the topology view holds now: the one state a reconnect series stands on (connections.md
    /// section 9.1: "Failed for the current destination incarnation"). `false` when none is held, when
    /// the latest is Connected, a drop (Disconnected) or an acceptor's Refused, and when the Failed
    /// fact names a destination process that is gone (§13 I: stale evidence is never used for a new
    /// process birth).
    pub fn latest_direct_failed_for(&self, source: &PathName, destination: &PathName, alpn: &[u8]) -> bool {
        let key = ConnectionIndex { source: source.clone(), destination: destination.clone(), alpn: alpn.to_vec(), kind: ConnectionKind::Direct };
        self.cells.get(&key).and_then(|c| c.failed.as_ref()).is_some_and(|end| self.end_superseded(end) != Some(true))
    }

    /// Every active Direct fact on the default ALPN whose destination is `destination`, sorted by
    /// source, leaving out a fact that names a superseded destination process; `None` when the
    /// holder is not complete.
    pub fn active_directs_to(&self, destination: &PathName) -> Option<Vec<&NodeConnection>> {
        self.active_directs_to_for(destination, ALPN)
    }

    /// Every active Direct fact on `alpn` whose destination is `destination`, sorted by source,
    /// leaving out a fact that names a superseded destination process; `None` when the holder is
    /// not complete.
    pub fn active_directs_to_for(&self, destination: &PathName, alpn: &[u8]) -> Option<Vec<&NodeConnection>> {
        if !self.complete {
            return None;
        }
        let mut out: Vec<&NodeConnection> = self
            .cells
            .values()
            .filter_map(|c| c.active.as_ref())
            .filter(|e| &e.destination.name == destination && e.alpn.as_slice() == alpn && !self.names_superseded_destination(e))
            .collect();
        out.sort_by(|a, b| a.source.name.cmp(&b.source.name));
        Some(out)
    }

    /// How many active Direct facts are held that name a destination process still current.
    pub fn active_len(&self) -> usize {
        self.cells.values().filter_map(|c| c.active.as_ref()).filter(|e| !self.names_superseded_destination(e)).count()
    }

    /// How many keys stand at a stamp, resident or not.
    pub fn keys(&self) -> usize {
        self.cells.len()
    }
}

/// A protocol's carrier policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CarrierPolicy {
    /// The protocol never travels over a Proxy, even when one exists.
    NotForwardable,
    /// The protocol may travel over a Proxy whose carrier is a node of this kind.
    Forwardable {
        /// The kind a carrier must be.
        carrier_kind: NodeKind,
    },
}

/// The carrier's own Direct edge to the destination is not Active: the carried call answered it.
pub const INVALID_CARRIER_EDGE_LOST: &str = "carrier-edge-lost";
/// The carrier's edge names another carrier process than the Proxy recorded.
pub const INVALID_CARRIER_SUPERSEDED: &str = "carrier-incarnation-superseded";
/// The carrier's edge names another destination process than the Proxy recorded.
pub const INVALID_DESTINATION_SUPERSEDED: &str = "destination-incarnation-superseded";

/// How a new invocation reaches its destination.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EffectiveRoute {
    /// Dial the destination directly. `known` is `false` when no current Direct fact is held: the
    /// holder is not complete, no Direct fact toward the destination was ever observed, or the
    /// Direct fact it holds names a destination process that is gone. The direct dial is the only
    /// thing to try.
    Direct {
        /// Whether a current Direct fact is held.
        known: bool,
    },
    /// Forward through `carrier`: the valid own Proxy `proxy` names it.
    ViaPeer {
        /// The carrier's `path.name`.
        carrier: PathName,
        /// The own Proxy that names the carrier.
        proxy: NodeConnection,
    },
    /// No active Direct and no valid Proxy.
    NoActiveRoute,
}

impl EffectiveRoute {
    /// The outcome token spans name the route by.
    pub fn token(&self) -> &'static str {
        match self {
            EffectiveRoute::Direct { known: true } => "direct",
            EffectiveRoute::Direct { known: false } => "direct-unknown",
            EffectiveRoute::ViaPeer { .. } => "via-peer",
            EffectiveRoute::NoActiveRoute => "no-active-route",
        }
    }
}

/// One resolution: the route, beside the own Proxy found invalid on the way and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RouteResolution {
    /// The route.
    pub route: EffectiveRoute,
    /// The own Proxy found invalid and the reason, for the caller to retire.
    pub retire: Option<(NodeConnection, &'static str)>,
}

/// Why `proxy` is not valid now over `held`, or `None` when it is. The destination and carrier
/// processes it recorded must be the ones the topology view holds at their paths now. The carrier's own
/// Direct edge to the destination is not judged here: it is the carrier's fact, and the carried
/// call answers `carrier-edge-lost` when it is not Active.
pub fn proxy_invalid(held: &ConnectionsHeld, proxy: &NodeConnection) -> Option<&'static str> {
    let Some(carrier) = proxy.carrier.as_ref() else {
        return Some(INVALID_CARRIER_EDGE_LOST);
    };
    if held.end_superseded(&proxy.destination) == Some(true) {
        return Some(INVALID_DESTINATION_SUPERSEDED);
    }
    if held.end_superseded(carrier) == Some(true) {
        return Some(INVALID_CARRIER_SUPERSEDED);
    }
    None
}

/// The route from `own` to `destination` under `policy` on the default ALPN, over `held`: a valid
/// own Proxy, else an active Direct, else a direct dial when no Direct fact was ever observed,
/// else no route. Pure: the caller retires [`RouteResolution::retire`].
pub fn resolve(held: &ConnectionsHeld, own: &PathName, destination: &PathName, policy: CarrierPolicy) -> RouteResolution {
    resolve_for(held, own, destination, ALPN, policy)
}

/// [`resolve`] on `alpn`.
pub fn resolve_for(held: &ConnectionsHeld, own: &PathName, destination: &PathName, alpn: &[u8], policy: CarrierPolicy) -> RouteResolution {
    match policy {
        CarrierPolicy::NotForwardable => resolve_for_carriers(held, own, destination, alpn, false, &[]),
        CarrierPolicy::Forwardable { carrier_kind } => {
            resolve_for_carriers(held, own, destination, alpn, true, &[carrier_kind])
        }
    }
}

/// [`resolve_for`] with the protocol's `forwardable` flag and eligible `carriers` kinds.
pub fn resolve_for_carriers(
    held: &ConnectionsHeld,
    own: &PathName,
    destination: &PathName,
    alpn: &[u8],
    forwardable: bool,
    carriers: &[NodeKind],
) -> RouteResolution {
    let mut retire = None;
    if forwardable && !carriers.is_empty() {
        if let Some(Some(proxy)) = held.own_proxy_for(destination, alpn) {
            // A valid Proxy through a carrier kind this protocol does not allow is skipped, not
            // retired: another protocol may still travel over it.
            let kind_allowed = proxy
                .carrier
                .as_ref()
                .is_some_and(|c| carriers.contains(&c.name.kind));
            match proxy_invalid(held, proxy) {
                None if kind_allowed => {
                    let carrier = proxy.carrier.as_ref().map(|c| c.name.clone()).expect("a valid Proxy names its carrier");
                    return RouteResolution { route: EffectiveRoute::ViaPeer { carrier, proxy: proxy.clone() }, retire: None };
                }
                None => {}
                Some(reason) => retire = Some((proxy.clone(), reason)),
            }
        }
    }
    let route = match held.active_direct_for(own, destination, alpn) {
        // A Direct fact naming a destination process that is gone is no evidence about the
        // current one: the dial is the only thing to try.
        Some(Some(edge)) if held.names_superseded_destination(edge) => EffectiveRoute::Direct { known: false },
        Some(Some(_)) => EffectiveRoute::Direct { known: true },
        // No Direct fact was ever observed toward this destination, or the latest one is a drop
        // (Disconnected: a connection this node closed, as node.connections.delete does at every
        // node.stop) or an acceptor's Refused: nothing says it is unreachable and
        // no reconnect series stands on it (connections.md §9.1), so the node calls it by its
        // transport key and address (§7).
        Some(None) if !held.latest_direct_failed_for(own, destination, alpn) => EffectiveRoute::Direct { known: false },
        // The latest Direct fact is Failed: its reconnect series owns the retry, and no new call
        // dials over it.
        Some(None) => EffectiveRoute::NoActiveRoute,
        None => EffectiveRoute::Direct { known: false },
    };
    RouteResolution { route, retire }
}

/// A chosen carrier and the active Direct edge `carrier -> destination` it was chosen by.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CarrierChoice {
    /// The carrier's `path.name`.
    pub carrier: PathName,
    /// The active Direct edge from the carrier to the destination.
    pub edge: NodeConnection,
}

/// The carrier `own` forwards to `destination` through on the default ALPN. Eligible carriers are
/// nodes of the policy's kind holding an active Direct edge to the destination (this node
/// excluded), each edge naming both processes, all naming the destination process the freshest
/// such edge names. One is picked by `fnv1a(source, destination, destination incarnation) % len`
/// over the path order: spread by source, never "freshest wins". `Ok(None)` when no such carrier
/// exists; `Err` names why none can be chosen.
pub fn select_carrier(
    held: &ConnectionsHeld,
    own: &PathName,
    destination: &PathName,
    policy: CarrierPolicy,
) -> Result<Option<CarrierChoice>, String> {
    select_carrier_for(held, own, destination, ALPN, policy)
}

/// [`select_carrier`] on `alpn`.
pub fn select_carrier_for(
    held: &ConnectionsHeld,
    own: &PathName,
    destination: &PathName,
    alpn: &[u8],
    policy: CarrierPolicy,
) -> Result<Option<CarrierChoice>, String> {
    match policy {
        CarrierPolicy::NotForwardable => {
            select_carrier_for_carriers(held, own, destination, alpn, false, &[])
        }
        CarrierPolicy::Forwardable { carrier_kind } => {
            select_carrier_for_carriers(held, own, destination, alpn, true, &[carrier_kind])
        }
    }
}

/// [`select_carrier_for`] with the protocol's `forwardable` flag and eligible `carriers` kinds.
pub fn select_carrier_for_carriers(
    held: &ConnectionsHeld,
    own: &PathName,
    destination: &PathName,
    alpn: &[u8],
    forwardable: bool,
    carriers: &[NodeKind],
) -> Result<Option<CarrierChoice>, String> {
    if !forwardable || carriers.is_empty() {
        return Err(format!("the protocol is not forwardable, so {destination} is never reached through a carrier"));
    }
    let Some(edges) = held.active_directs_to_for(destination, alpn) else {
        return Err(format!("the connections projection is not complete, so it does not hold every route to {destination}"));
    };
    let (named, blind): (Vec<&NodeConnection>, Vec<&NodeConnection>) = edges
        .into_iter()
        .filter(|e| carriers.contains(&e.source.name.kind) && &e.source.name != own)
        .partition(|e| e.destination.incarnation.is_some() && e.source.incarnation.is_some());
    let Some(freshest) = named.iter().max_by(|a, b| a.stamp().cmp(&b.stamp()).then(b.source.name.cmp(&a.source.name))) else {
        if blind.is_empty() {
            return Ok(None);
        }
        let names: Vec<String> = blind.iter().map(|e| e.source.name.to_string()).collect();
        return Err(format!(
            "{} carrier edge(s) to {destination} ({}) name no destination process or no carrier process, so none is evidence for a carrier",
            blind.len(),
            names.join(",")
        ));
    };
    let current = freshest.destination.incarnation.clone();
    let mut eligible: Vec<&NodeConnection> = named.into_iter().filter(|e| e.destination.incarnation == current).collect();
    eligible.sort_by(|a, b| a.source.name.cmp(&b.source.name));
    let current = current.map(|c| c.to_string()).unwrap_or_default();
    let at = spread(&own.to_string(), &destination.to_string(), &current) % eligible.len() as u64;
    let edge = eligible[at as usize].clone();
    Ok(Some(CarrierChoice { carrier: edge.source.name.clone(), edge }))
}

/// FNV-1a 64 over `source`, `destination` and `destination_incarnation`, each followed by a 0xff
/// separator: the same value in every process and every build.
pub fn spread(source: &str, destination: &str, destination_incarnation: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in [source, destination, destination_incarnation] {
        for byte in part.as_bytes().iter().chain(std::iter::once(&0xffu8)) {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    hash
}
