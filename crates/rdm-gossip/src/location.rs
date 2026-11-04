//! The peer-location view: the one address view an endpoint's address lookup reads, fed by the
//! digests a node holds (gossip.md, contact fields).


use anyhow::Result;
use iroh::address_lookup::MemoryLookup;
use iroh::{Endpoint, EndpointAddr};
use rdm_core::{MeshDigest, NodeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};



/// Tell `endpoint` where `peers` are (no discovery service runs): a lookup of its own that only
/// adds. For a raw gossip observer with no [`TopologyView`]; an endpoint that holds a
/// [`LocationView`] installs every address through that view.
pub fn learn_addresses(endpoint: &Endpoint, peers: &[EndpointAddr]) -> Result<()> {
    let lookup = MemoryLookup::new();
    for p in peers {
        lookup.add_endpoint_info(p.clone());
    }
    endpoint.address_lookup().map_err(|e| anyhow::anyhow!("address_lookup: {e}"))?.add(lookup);
    Ok(())
}

/// A member's gossip address: its key at its one transport address.
pub fn gossip_addr(d: &MeshDigest) -> Option<EndpointAddr> {
    let key = d.node.endpoint_id.0.parse::<iroh::PublicKey>().ok()?;
    Some(EndpointAddr::new(key).with_ip_addr(d.node.transport_addr))
}

/// The newest birth and heartbeat each key's address was registered from (rafka-v2 node-base
/// `peer_location_wall_time`), and the socket that birth named: governs the address view only,
/// never the held view. Ordered as the held view is: by `digest_seq` within one incarnation, and
/// along the lineage across incarnations.
pub(crate) type LocationMark = (rdm_core::IncarnationId, u64, Option<rdm_core::IncarnationId>, std::net::SocketAddr);

/// The marks of one peer-location view, by key. Each view holds its own: a newer birth one node of
/// a process consumed is still newer to every other node of that process.
pub type LocationMarks = Mutex<HashMap<iroh::PublicKey, LocationMark>>;

/// The peer-location view of one endpoint: the ONE address view its address lookup reads. Every
/// channel the endpoint joins installs where a key is through it, and only a birth's
/// ordered digest changes a key's address (gossip.md, contact fields: "Contact changes continue
/// through the birth's ordered digest updates"). A move deletes every other direct path the
/// endpoint holds to the key (R-I1, `Endpoint::replace_direct_addrs`). An address from anything
/// else (a seed, a peer list) fills a key the view holds no address for and never adds a second
/// one: iroh merges every address lookup service's answer when it dials a key with no selected
/// path, so a socket kept in a second view is dialled again after its key moved.
#[derive(Clone)]
pub struct LocationView {
    pub(crate) lookup: MemoryLookup,
    pub(crate) endpoint: Endpoint,
    pub(crate) marks: Arc<LocationMarks>,
    /// The node this view belongs to: the observer every location span names.
    pub(crate) observer: String,
    pub(crate) held_name: Arc<Mutex<Option<rdm_core::PathName>>>,
}

impl LocationView {
    /// The view of `endpoint`, held by `observer`, installed as an address lookup of the endpoint.
    pub fn new(endpoint: &Endpoint, observer: &str) -> Result<Self> {
        let lookup = MemoryLookup::new();
        endpoint.address_lookup().map_err(|e| anyhow::anyhow!("address_lookup: {e}"))?.add(lookup.clone());
        Ok(Self { lookup, endpoint: endpoint.clone(), marks: Arc::default(), observer: observer.to_string(), held_name: Arc::default() })
    }

    pub(crate) fn with_held_name(mut self, held_name: Arc<Mutex<Option<rdm_core::PathName>>>) -> Self {
        self.held_name = held_name;
        self
    }

    pub(crate) fn observer_name(&self) -> String {
        self.observer.clone()
    }

    /// The path name the endpoint's topology view holds (`Some` once installed or renamed).
    pub fn held_path_name(&self) -> Option<rdm_core::PathName> {
        self.held_name.lock().unwrap().clone()
    }

    /// Register where the birth `d` names is, and delete the paths a moved key leaves behind.
    pub fn register(&self, d: &MeshDigest) {
        replace_superseded(&self.endpoint, &self.observer_name(), register_location(&self.marks, &self.lookup, d));
    }

    /// Fill `addr` for its key when the view holds no address for that key; an address the view
    /// holds is changed only by a newer birth's digest ([`LocationView::register`]).
    pub fn fill(&self, addr: &EndpointAddr) {
        if self.lookup.get_endpoint_info(addr.id).is_none_or(|e| e.ip_addrs().next().is_none()) {
            self.lookup.add_endpoint_info(addr.clone());
        }
    }

    /// Forget every address of `key`: a birth under an open removal is never handed out.
    pub fn forget(&self, key: iroh::PublicKey) {
        self.lookup.remove_endpoint_info(key);
    }

    /// The direct addresses this view holds for `key`.
    pub fn addrs(&self, key: iroh::PublicKey) -> Vec<std::net::SocketAddr> {
        self.lookup.get_endpoint_info(key).map(|e| e.ip_addrs().cloned().collect()).unwrap_or_default()
    }
}

/// The `(node.id, incarnation)` pairs a `node.deleted` targeted in the peer-location view: a digest of
/// one never registers its socket for its key. A new node over a deleted one's data dir keeps the
/// key and sets no `supersedes`, so lineage alone cannot refuse the deleted instance's late digest.
pub(crate) fn deleted_incarnations() -> &'static Mutex<std::collections::HashSet<(String, String)>> {
    static F: std::sync::OnceLock<Mutex<std::collections::HashSet<(String, String)>>> = std::sync::OnceLock::new();
    F.get_or_init(Mutex::default)
}

/// Target the exact birth `(node_id, incarnation)` in the peer-location view.
pub fn delete_incarnation(node_id: &NodeId, incarnation: &rdm_core::IncarnationId) {
    deleted_incarnations().lock().unwrap().insert((node_id.to_string(), incarnation.to_string()));
}

/// Whether `(node_id, incarnation)` is targeted in the peer-location view.
pub(crate) fn incarnation_deleted(node_id: &NodeId, incarnation: &rdm_core::IncarnationId) -> bool {
    deleted_incarnations().lock().unwrap().contains(&(node_id.to_string(), incarnation.to_string()))
}

/// Register where the birth `d` names is (rafka-v2 node-base `register_peer_location_if_fresher`).
/// A digest newer than the last one this key was registered from REPLACES the key's addresses
/// (`set_endpoint_info`, never a union): a node that restarts keeps its key and binds a fresh
/// port, and its old port, now free for anyone, must never be dialled for it again. Newer is
/// decided by `digest_seq` for the same incarnation, and by lineage across incarnations (the
/// successor restarts its sequence at 1; the incarnation it replaced is older). A digest that is
/// not newer (replayed, late, or a mesh topology copy of an older view) only fills a key that has no
/// address, so it never puts an old socket back.
///
/// Returns the key, the socket it moved from and the new socket when a newer birth MOVED the key:
/// the caller then deletes the endpoint's paths to every other socket (`delete_superseded`). The
/// socket it moved from is the one the view last registered for the key, or else the one it holds:
/// a view that forgot the key under an open removal still knows where the key was.
pub fn register_location(marks: &LocationMarks, lookup: &MemoryLookup, d: &MeshDigest) -> Option<LocationMove> {
    let addr = gossip_addr(d)?;
    let key = addr.id;
    if incarnation_deleted(&d.node.node_id, &d.node.incarnation) {
        tracing::info_span!(
            "rdm.mesh.connection.reject.via-deleted-incarnation",
            node_path = %d.node.path, node_name = %d.node.name,
            node_id = %d.node.node_id,
            incarnation_id = %d.node.incarnation.as_str(),
            socket = %d.node.transport_addr,
        )
        .in_scope(|| tracing::info!("a digest of a deleted incarnation: its socket is not registered for the key"));
        return None;
    }
    let (fresher, marked) = {
        let mut w = marks.lock().unwrap();
        let marked = w.get(&key).map(|m| m.3);
        let fresher = match w.get(&key) {
            None => true,
            Some((incarnation, seq, _, _)) if *incarnation == d.node.incarnation => d.digest_seq > *seq,
            Some((_, _, supersedes, _)) => supersedes.as_ref() != Some(&d.node.incarnation),
        };
        if fresher {
            w.insert(key, (d.node.incarnation.clone(), d.digest_seq, d.node.supersedes.clone(), d.node.transport_addr));
        }
        (fresher, marked)
    };
    let held = lookup.get_endpoint_info(key).and_then(|e| e.ip_addrs().next().cloned());
    let mut moved = None;
    if fresher {
        if let Some(old) = marked.or(held).filter(|o| *o != d.node.transport_addr) {
            moved = Some(LocationMove { key, node: d.node.node_id.to_string(), incarnation: d.node.incarnation.to_string(), old, new: d.node.transport_addr });
        }
        lookup.set_endpoint_info(addr);
    } else if held.is_none() {
        lookup.add_endpoint_info(addr);
    }
    moved
}

/// A newer birth of `key` (node `node`, birth `incarnation`) moved it from `old` to `new`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocationMove {
    /// The key that moved.
    pub key: iroh::PublicKey,
    /// The node the newer birth names.
    pub node: String,
    /// The newer birth.
    pub incarnation: String,
    /// The socket the view held for the key before.
    pub old: std::net::SocketAddr,
    /// The socket the newer birth names.
    pub new: std::net::SocketAddr,
}

/// A newer birth moved a key: the endpoint deletes every direct path it holds to any other socket
/// of the key, open ones included (iroh `Endpoint::replace_direct_addrs`), so a connection to the
/// old socket, or a dial's first packet to it, can no longer reach whoever took that port. The span
/// ends when the endpoint has deleted them (`deleted=true`).
pub(crate) fn replace_superseded(endpoint: &Endpoint, observer: &str, moved: Option<LocationMove>) {
    let Some(m) = moved else { return };
    let span = tracing::info_span!(
        "rdm.mesh.connection.update.via-peer-location-refreshed",
        observer = %observer,
        node = %m.node,
        incarnation_id = %m.incarnation,
        peer = %m.key.fmt_short(),
        old_socket = %m.old,
        new_socket = %m.new,
        deleted = tracing::field::Empty,
    );
    span.in_scope(|| tracing::info!("a newer birth of this key moved: its old socket is no longer dialled"));
    let endpoint = endpoint.clone();
    tokio::spawn(tracing::Instrument::instrument(
        async move {
            endpoint.replace_direct_addrs(m.key, [m.new]).await;
            tracing::Span::current().record("deleted", true);
            tracing::info!("every direct path to another socket of the key is deleted");
        },
        span,
    ));
}
