//! Endpoint authority (PRD §8, gap R8; mesh-control-plane.md §6).
//!
//! Node-admin assigns every externally advertised address before a provider
//! launches anything: one Iroh transport address per process (gossip and Node
//! RPC share it), and one address per non-Iroh listener a kind declares (a
//! node-admin's `control` HTTP API). A provider must honour the assignment; it
//! never invents an advertised port. `WaitForBind` checks that independently:
//! it asks the operating system whether each assigned socket is actually held,
//! rather than trusting the runtime's own report.
//!
//! On a shared host every node-admin (of any fabric) draws from the same port
//! range, and a probe bind sees a port free until the launched runtime binds
//! it. A port is therefore also claimed host-wide before it is handed out:
//! an exclusive create of `<temp dir>/rafka-endpoint-ports/<ip>-<port>`,
//! holding the claiming process's pid. A claim whose process is gone is
//! stale and is taken over.

use crate::model::PathName;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::{IpAddr, SocketAddr, UdpSocket};

/// What an assigned socket is: the Iroh transport (UDP) or a listener (TCP).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotTransport {
    Udp,
    Tcp,
}

/// What a node kind binds and serves.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KindSpec {
    /// Non-Iroh listeners, each its own TCP address.
    pub listeners: &'static [&'static str],
}

/// An rpc node: the one transport, no listener.
pub const RPC_NODE: KindSpec = KindSpec { listeners: &[] };

/// A node-admin: its mesh transport (gossip and the Node RPC declarations it applies as an
/// authority) and its control API (HTTP).
pub const NODE_ADMIN: KindSpec = KindSpec { listeners: &["control"] };

/// What a node kind binds and serves.
pub fn spec_for(kind: crate::model::NodeKind) -> &'static KindSpec {
    match kind {
        // A product kind binds the one transport here; its own listeners are the product's to
        // assign through its node-admin binding.
        crate::model::NodeKind::RpcNode | crate::model::NodeKind::Broker | crate::model::NodeKind::Gateway | crate::model::NodeKind::Compute => &RPC_NODE,
        crate::model::NodeKind::NodeAdmin => &NODE_ADMIN,
    }
}

/// Everything node-admin assigned one process: its transport address and its
/// listeners.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Assignment {
    pub transport: SocketAddr,
    pub listeners: Vec<(String, SocketAddr)>,
}

impl Assignment {
    /// Every socket the process must hold: the transport (UDP) and each listener (TCP).
    pub fn sockets(&self) -> Vec<(String, SocketAddr, SlotTransport)> {
        let mut out = vec![("transport".to_string(), self.transport, SlotTransport::Udp)];
        out.extend(self.listeners.iter().map(|(n, a)| (n.clone(), *a, SlotTransport::Tcp)));
        out
    }

    /// The assignment a held node record describes.
    pub fn of_node(node: &crate::model::Node) -> Option<Self> {
        Some(Self { transport: node.transport_addr?, listeners: node.listeners.clone() })
    }
}

/// Why an allocation was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllocationError {
    /// No free port left in the configured range.
    Exhausted { first: u16, last: u16 },
    /// Every node address of the range is held.
    AddressesExhausted,
    /// A restart was asked to keep what it has no prior assignment for.
    NoPriorAssignment { node: PathName, socket: String },
}

impl fmt::Display for AllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted { first, last } => write!(f, "no free port left in {first}..={last}"),
            Self::AddressesExhausted => write!(f, "every node address of the range is held"),
            Self::NoPriorAssignment { node, socket } => write!(f, "{node} {socket}: a restart keeps it, but none was assigned"),
        }
    }
}

/// `WaitForBind` refusals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindRefusal {
    /// Nothing holds the assigned socket: the runtime bound somewhere else or not at all.
    NotBoundAtAssigned { socket: String, assigned: SocketAddr },
}

impl fmt::Display for BindRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBoundAtAssigned { socket, assigned } => write!(f, "{socket}: nothing is bound at the assigned {assigned}"),
        }
    }
}

/// Where a node's advertised addresses live.
#[derive(Debug, Clone)]
enum Addressing {
    /// Every node shares one host address (process provider): a port is unique
    /// per host, and one another process holds is skipped.
    SharedHost(IpAddr),
    /// Every node owns one address from `first..=last` in its own network
    /// namespace (container provider): ports are unique per node address, and
    /// the host's own sockets cannot collide with them.
    PerNode { first: u32, last: u32, next: u32, held_elsewhere: HeldElsewhere },
}

/// The node addresses something outside this allocator holds on the network right now (every
/// container of the fabric, whichever admin launched it). An admin taking over a fabric's
/// execution never hands out an address another admin's birth holds.
#[derive(Clone, Default)]
pub struct HeldElsewhere(Option<std::sync::Arc<dyn Fn() -> BTreeSet<IpAddr> + Send + Sync>>);

impl HeldElsewhere {
    pub fn new(f: impl Fn() -> BTreeSet<IpAddr> + Send + Sync + 'static) -> Self {
        Self(Some(std::sync::Arc::new(f)))
    }

    fn now(&self) -> BTreeSet<IpAddr> {
        self.0.as_ref().map(|f| f()).unwrap_or_default()
    }
}

impl std::fmt::Debug for HeldElsewhere {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() { "HeldElsewhere(probe)" } else { "HeldElsewhere(none)" })
    }
}

/// Every socket the topology names right now, by the node that holds it: the record check.
/// A port a node's record names is never handed out, whichever process reserved it and however
/// that reservation ended (an executor that died with its claims, a receipt of a dead attempt).
#[derive(Clone, Default)]
pub struct HeldSockets(Option<std::sync::Arc<dyn Fn() -> Vec<(PathName, SocketAddr)> + Send + Sync>>);

impl HeldSockets {
    pub fn new(f: impl Fn() -> Vec<(PathName, SocketAddr)> + Send + Sync + 'static) -> Self {
        Self(Some(std::sync::Arc::new(f)))
    }

    fn now(&self) -> Vec<(PathName, SocketAddr)> {
        self.0.as_ref().map(|f| f()).unwrap_or_default()
    }
}

impl std::fmt::Debug for HeldSockets {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(if self.0.is_some() { "HeldSockets(topology)" } else { "HeldSockets(none)" })
    }
}

/// Allocates advertised endpoints. Addresses already handed out are never
/// handed out again until released.
#[derive(Debug)]
pub struct EndpointAllocator {
    addressing: Addressing,
    first: u16,
    last: u16,
    next: u16,
    held: BTreeMap<PathName, Assignment>,
    in_use: BTreeSet<SocketAddr>,
    held_sockets: HeldSockets,
}

impl EndpointAllocator {
    pub fn new(host: IpAddr, first: u16, last: u16) -> Self {
        assert!(first <= last, "empty port range");
        Self { addressing: Addressing::SharedHost(host), first, last, next: first, held: BTreeMap::new(), in_use: BTreeSet::new(), held_sockets: HeldSockets::default() }
    }

    /// One address per node from `first_ip..=last_ip`, ports from `first..=last`.
    /// [`Self::per_node`] that also skips every address `held_elsewhere` reports.
    /// Skip every socket `held` reports (the topology's records), on any addressing.
    pub fn with_held_sockets(mut self, held: HeldSockets) -> Self {
        self.held_sockets = held;
        self
    }

    /// A socket of `a` that the topology names under a node other than `node`, if any: an
    /// assignment that is somebody else's now and is never reused.
    pub fn named_by_another(&self, node: &PathName, a: &Assignment) -> Option<(PathName, SocketAddr)> {
        let named = self.held_sockets.now();
        a.sockets().into_iter().find_map(|(_, addr, _)| named.iter().find(|(n, s)| *s == addr && n != node).cloned())
    }

    pub fn with_held_elsewhere(mut self, probe: HeldElsewhere) -> Self {
        if let Addressing::PerNode { held_elsewhere, .. } = &mut self.addressing {
            *held_elsewhere = probe;
        }
        self
    }

    pub fn per_node(first_ip: std::net::Ipv4Addr, last_ip: std::net::Ipv4Addr, first: u16, last: u16) -> Self {
        assert!(first <= last, "empty port range");
        let (a, b) = (u32::from(first_ip), u32::from(last_ip));
        assert!(a <= b, "empty address range");
        Self {
            addressing: Addressing::PerNode { first: a, last: b, next: a, held_elsewhere: HeldElsewhere::default() },
            first,
            last,
            next: first,
            held: BTreeMap::new(),
            in_use: BTreeSet::new(), held_sockets: HeldSockets::default(),
        }
    }

    /// `RAFKA_ENDPOINT_PORT_RANGE=<first>-<last>` (default `20000-29999`) on `127.0.0.1`.
    pub fn from_env() -> Self {
        let (first, last) = port_range_from_env();
        Self::new(IpAddr::from([127, 0, 0, 1]), first, last)
    }

    /// The address `node` advertises on: the shared host, the node's own
    /// address when it holds one, or the next address no node holds.
    fn node_ip(&mut self, node: &PathName) -> Result<IpAddr, AllocationError> {
        match &mut self.addressing {
            Addressing::SharedHost(h) => Ok(*h),
            Addressing::PerNode { first, last, next, held_elsewhere } => {
                if let Some(a) = self.held.get(node) {
                    return Ok(a.transport.ip());
                }
                let mut taken: BTreeSet<IpAddr> = self.held.values().map(|a| a.transport.ip()).collect();
                taken.extend(held_elsewhere.now());
                for _ in 0..=(*last - *first) {
                    let ip = IpAddr::from(std::net::Ipv4Addr::from(*next));
                    *next = if *next == *last { *first } else { *next + 1 };
                    if !taken.contains(&ip) {
                        return Ok(ip);
                    }
                }
                Err(AllocationError::AddressesExhausted)
            }
        }
    }

    /// The next free port on `ip`. On a shared host a port bound by any other
    /// process is skipped (checked by a probe bind of UDP and TCP).
    fn take_addr(&mut self, ip: IpAddr) -> Result<SocketAddr, AllocationError> {
        let span = (self.last - self.first) as u32 + 1;
        for _ in 0..span {
            let a = SocketAddr::new(ip, self.next);
            self.next = if self.next == self.last { self.first } else { self.next + 1 };
            if self.in_use.contains(&a) {
                continue;
            }
            // The record check: a socket any node's record names is taken, whoever reserved it.
            if self.held_sockets.now().iter().any(|(_, s)| *s == a) {
                continue;
            }
            let free = match self.addressing {
                // Claim first, probe after: a port another live admin claims is never probe-bound,
                // so a probe here never holds, for an instant, a port another admin's birth is
                // binding. A claimed port that a probe finds held is released again.
                Addressing::SharedHost(_) => {
                    reserve_on_host(a) && {
                        let unbound = UdpSocket::bind(a).is_ok() && std::net::TcpListener::bind(a).is_ok();
                        if !unbound {
                            release_on_host(a);
                        }
                        unbound
                    }
                }
                Addressing::PerNode { .. } => true,
            };
            if free {
                self.in_use.insert(a);
                return Ok(a);
            }
        }
        Err(AllocationError::Exhausted { first: self.first, last: self.last })
    }

    /// Assign `node` everything a process birth needs.
    ///
    /// - first birth or replacement (`restart = false`): a new transport
    ///   address and a new address per listener;
    /// - restart (`restart = true`): the transport and listener addresses are
    ///   kept.
    pub fn assign(&mut self, node: &PathName, spec: &KindSpec, restart: bool) -> Result<Assignment, AllocationError> {
        if restart {
            let prior = self.held.get(node).cloned().ok_or_else(|| AllocationError::NoPriorAssignment { node: node.clone(), socket: "transport".into() })?;
            // A restart keeps every socket its kind declares: one the prior assignment lacks is
            // refused by name, never launched without.
            if let Some(missing) = spec.listeners.iter().find(|l| !prior.listeners.iter().any(|(n, _)| n == *l)) {
                return Err(AllocationError::NoPriorAssignment { node: node.clone(), socket: missing.to_string() });
            }
            self.held.insert(node.clone(), prior.clone());
            return Ok(prior);
        }
        self.release(node);
        let ip = self.node_ip(node)?;
        let mut taken = Vec::new();
        let mut take = |me: &mut Self| match me.take_addr(ip) {
            Ok(a) => {
                taken.push(a);
                Ok(a)
            }
            Err(e) => Err(e),
        };
        let transport = match take(self) {
            Ok(a) => a,
            Err(e) => return Err(e),
        };
        let mut listeners = Vec::new();
        for name in spec.listeners {
            match take(self) {
                Ok(a) => listeners.push((name.to_string(), a)),
                Err(e) => {
                    for a in taken {
                        self.free(a);
                    }
                    return Err(e);
                }
            }
        }
        let out = Assignment { transport, listeners };
        self.held.insert(node.clone(), out.clone());
        Ok(out)
    }

    /// One reserved, probed transport address for `node`, held under its name: what a restarted
    /// admin binds when the address it last held is taken. Never an OS-chosen port: the ephemeral
    /// range overlaps this allocator's, and a port handed to a birth between its reservation and
    /// its bind would be taken from under it.
    pub fn take_transport(&mut self, node: &PathName) -> Result<SocketAddr, AllocationError> {
        self.release(node);
        let ip = self.node_ip(node)?;
        let transport = self.take_addr(ip)?;
        self.held.insert(node.clone(), Assignment { transport, listeners: Vec::new() });
        Ok(transport)
    }

    /// Reclaim `addr` for `node` on a restart, before it is bound again. The claim the previous
    /// incarnation held died with that process, and a dead owner's claim is any allocator's to
    /// take over, so a bind without the claim races a birth that was just handed the same port.
    /// `false` when a live process holds the claim now: the address is theirs, and the caller
    /// takes a fresh one.
    pub fn reclaim_transport(&mut self, node: &PathName, addr: SocketAddr) -> bool {
        let claimed = match self.addressing {
            Addressing::SharedHost(_) => reserve_on_host(addr),
            Addressing::PerNode { .. } => true,
        };
        if claimed {
            self.in_use.insert(addr);
            self.held.insert(node.clone(), Assignment { transport: addr, listeners: Vec::new() });
        }
        claimed
    }

    /// Release every address of `node` (retire pipeline `ReleaseEndpoints`).
    pub fn release(&mut self, node: &PathName) {
        if let Some(a) = self.held.remove(node) {
            for (_, addr, _) in a.sockets() {
                self.free(addr);
            }
        }
    }

    /// Return `addr` to the range, and drop its host-wide claim.
    fn free(&mut self, addr: SocketAddr) {
        if self.in_use.remove(&addr) && matches!(self.addressing, Addressing::SharedHost(_)) {
            release_on_host(addr);
        }
    }

    /// Record an assignment that already exists (an admin adopting a running
    /// node after a takeover), so its addresses are never handed out again.
    pub fn adopt(&mut self, node: &PathName, assignment: Assignment) {
        for (_, addr, _) in assignment.sockets() {
            if self.in_use.insert(addr) && matches!(self.addressing, Addressing::SharedHost(_)) {
                claim_on_host(addr);
            }
        }
        self.held.insert(node.clone(), assignment);
    }

    /// How many advertised addresses are held across all nodes.
    pub fn in_use_count(&self) -> usize {
        self.in_use.len()
    }

    pub fn held(&self, node: &PathName) -> Option<&Assignment> {
        self.held.get(node)
    }
}

impl Drop for EndpointAllocator {
    /// A node-admin that goes away drops its host-wide claims; a runtime it
    /// leaves running still holds its port, which the probe bind sees.
    fn drop(&mut self) {
        if matches!(self.addressing, Addressing::SharedHost(_)) {
            for a in std::mem::take(&mut self.in_use) {
                release_on_host(a);
            }
        }
    }
}

/// Where the host-wide claim of `addr` lives.
fn reservation_path(addr: SocketAddr) -> std::path::PathBuf {
    std::env::temp_dir().join("rafka-endpoint-ports").join(format!("{}-{}", addr.ip(), addr.port()))
}

/// Is process `pid` alive on this host?
fn process_is_alive(pid: u32) -> bool {
    pid == std::process::id() || std::path::Path::new(&format!("/proc/{pid}")).exists()
}

/// Run `f` holding the host-wide lock over every endpoint claim, so a claim
/// is decided (free, live owner, or stale) and written as one step. `None`
/// when the claims directory cannot be locked.
fn with_claims_locked<T>(f: impl FnOnce(&std::path::Path) -> T) -> Option<T> {
    let dir = std::env::temp_dir().join("rafka-endpoint-ports");
    let lock = std::fs::create_dir_all(&dir)
        .and_then(|()| std::fs::OpenOptions::new().create(true).truncate(false).write(true).open(dir.join(".lock")))
        .and_then(|f| f.lock().map(|()| f));
    match lock {
        Ok(_held) => Some(f(&dir)),
        Err(e) => {
            tracing::warn!(dir = %dir.display(), error = %e, "endpoint claims cannot be locked");
            None
        }
    }
}

fn claim_owner(path: &std::path::Path) -> Option<u32> {
    std::fs::read_to_string(path).ok().and_then(|s| s.trim().parse().ok())
}

/// Claim `addr` host-wide. `false` when a live process holds the claim or it
/// cannot be written; a claim whose owner process is gone is taken over.
fn reserve_on_host(addr: SocketAddr) -> bool {
    let path = reservation_path(addr);
    with_claims_locked(|_| {
        if claim_owner(&path).is_some_and(process_is_alive) {
            return false;
        }
        match std::fs::write(&path, std::process::id().to_string()) {
            Ok(()) => true,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "endpoint claim cannot be written; port {addr} is not handed out");
                false
            }
        }
    })
    .unwrap_or(false)
}

/// Record a claim for an address this admin adopted (it already runs).
fn claim_on_host(addr: SocketAddr) {
    let path = reservation_path(addr);
    with_claims_locked(|_| {
        let _ = std::fs::write(&path, std::process::id().to_string());
    });
}

/// Drop this process's claim on `addr` (another process's claim is kept).
fn release_on_host(addr: SocketAddr) {
    let path = reservation_path(addr);
    with_claims_locked(|_| {
        if claim_owner(&path) == Some(std::process::id()) {
            let _ = std::fs::remove_file(&path);
        }
    });
}

/// `RAFKA_ENDPOINT_PORT_RANGE=<first>-<last>`, default `20000-29999`: below the kernel's ephemeral
/// range, so a port handed to a birth cannot be taken between its assignment and its bind by any
/// process that binds port 0. A configured range that overlaps the ephemeral range is named.
pub fn port_range_from_env() -> (u16, u16) {
    let (first, last) = std::env::var("RAFKA_ENDPOINT_PORT_RANGE")
        .ok()
        .and_then(|r| {
            let (a, b) = r.split_once('-')?;
            Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
        })
        .unwrap_or((20000, 29999));
    if let Some((lo, hi)) = ephemeral_range() {
        if first <= hi && last >= lo {
            tracing::info_span!("rdm.node_admin.endpoint.reject.via-ephemeral-overlap", first, last, ephemeral_first = lo, ephemeral_last = hi)
                .in_scope(|| tracing::warn!("the endpoint port range overlaps the kernel's ephemeral range: a port assigned here can be taken by any port-0 bind before the birth binds it"));
        }
    }
    (first, last)
}

/// The kernel's ephemeral port range (`/proc/sys/net/ipv4/ip_local_port_range`), when readable.
pub fn ephemeral_range() -> Option<(u16, u16)> {
    let s = std::fs::read_to_string("/proc/sys/net/ipv4/ip_local_port_range").ok()?;
    let mut it = s.split_whitespace().filter_map(|v| v.parse::<u16>().ok());
    Some((it.next()?, it.next()?))
}

/// Is something on this host holding UDP `addr`? A bind that fails with
/// `AddrInUse` means yes.
pub fn udp_port_is_held(addr: SocketAddr) -> bool {
    matches!(UdpSocket::bind(addr), Err(e) if e.kind() == std::io::ErrorKind::AddrInUse)
}

/// Is something on this host listening on TCP `addr`?
pub fn tcp_port_is_held(addr: SocketAddr) -> bool {
    matches!(std::net::TcpListener::bind(addr), Err(e) if e.kind() == std::io::ErrorKind::AddrInUse)
}

/// `WaitForBind`: every assigned socket (the transport and each listener) is
/// held at its assigned address.
pub fn verify_bound(assigned: &Assignment) -> Result<(), BindRefusal> {
    verify_bound_with(assigned, |a, t| match t {
        SlotTransport::Udp => udp_port_is_held(a),
        SlotTransport::Tcp => tcp_port_is_held(a),
    })
}

/// [`verify_bound`] with the provider's own view of what the runtime holds
/// (a container's sockets live in its network namespace, not the host's).
pub fn verify_bound_with(assigned: &Assignment, held: impl Fn(SocketAddr, SlotTransport) -> bool) -> Result<(), BindRefusal> {
    for (socket, addr, transport) in assigned.sockets() {
        if !held(addr, transport) {
            return Err(BindRefusal::NotBoundAtAssigned { socket, assigned: addr });
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> PathName {
        s.parse().unwrap()
    }

    fn alloc() -> EndpointAllocator {
        // A range unlikely to collide with the other tests of this binary.
        EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 27000, 29999)
    }

    #[test]
    fn no_collisions_across_a_large_allocation() {
        let mut a = alloc();
        let mut seen = BTreeSet::new();
        for i in 1..=400 {
            let got = a.assign(&p(&format!("mesh1.rpc.{i}")), &RPC_NODE, false).unwrap();
            assert!(seen.insert(got.transport), "port {} handed out twice", got.transport);
            assert!(got.listeners.is_empty());
        }
        assert_eq!(seen.len(), 400, "one socket per process");
        assert_eq!(a.in_use_count(), 400);
    }

    #[test]
    fn a_restart_keeps_the_transport_and_a_replacement_takes_a_new_one() {
        let mut a = alloc();
        let node = p("mesh1.rpc.2");
        let first = a.assign(&node, &RPC_NODE, false).unwrap();
        let again = a.assign(&node, &RPC_NODE, true).unwrap();
        assert_eq!(first.transport, again.transport, "the transport stays across a restart");
        assert_eq!(a.in_use_count(), 1, "a restart takes no new port");
        let replaced = a.assign(&node, &RPC_NODE, false).unwrap();
        assert_ne!(replaced.transport, again.transport, "a replacement is a new assignment");
        assert_eq!(a.in_use_count(), 1, "the replaced assignment was released");
    }

    #[test]
    fn a_listener_kind_gets_its_own_tcp_address_beside_the_transport() {
        let mut a = alloc();
        let got = a.assign(&p("mesh1.admin.1"), &NODE_ADMIN, false).unwrap();
        assert_eq!(got.listeners.len(), 1);
        assert_eq!(got.listeners[0].0, "control");
        assert_ne!(got.listeners[0].1, got.transport);
        let sockets = got.sockets();
        assert_eq!(sockets[0].2, SlotTransport::Udp);
        assert_eq!(sockets[1].2, SlotTransport::Tcp);
        let again = a.assign(&p("mesh1.admin.1"), &NODE_ADMIN, true).unwrap();
        assert_eq!(again.listeners, got.listeners, "a restart keeps the listener address");
    }

    #[test]
    fn released_ports_return_and_a_restart_without_prior_is_named() {
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30000, 30001);
        a.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).unwrap();
        a.assign(&p("mesh1.rpc.2"), &RPC_NODE, false).unwrap();
        assert!(matches!(a.assign(&p("mesh1.rpc.3"), &RPC_NODE, false), Err(AllocationError::Exhausted { .. })));
        assert!(matches!(a.assign(&p("mesh1.rpc.3"), &RPC_NODE, false), Err(AllocationError::Exhausted { .. })), "a failed allocation leaks nothing");
        a.release(&p("mesh1.rpc.1"));
        assert!(a.assign(&p("mesh1.rpc.3"), &RPC_NODE, false).is_ok());
        assert_eq!(
            a.assign(&p("mesh1.rpc.9"), &RPC_NODE, true),
            Err(AllocationError::NoPriorAssignment { node: p("mesh1.rpc.9"), socket: "transport".into() })
        );
    }

    #[test]
    fn a_restart_missing_a_declared_listener_is_refused_by_name() {
        let mut a = alloc();
        let node = p("mesh1.admin.2");
        a.adopt(&node, Assignment { transport: "127.0.0.1:27999".parse().unwrap(), listeners: vec![] });
        assert_eq!(a.assign(&node, &NODE_ADMIN, true), Err(AllocationError::NoPriorAssignment { node: node.clone(), socket: "control".into() }));
    }

    #[test]
    fn a_container_address_held_on_the_network_by_another_admins_birth_is_never_handed_out() {
        let held: BTreeSet<IpAddr> = ["10.9.0.2".parse().unwrap(), "10.9.0.3".parse().unwrap()].into_iter().collect();
        let mut a = EndpointAllocator::per_node("10.9.0.2".parse().unwrap(), "10.9.0.9".parse().unwrap(), 20000, 20010)
            .with_held_elsewhere(HeldElsewhere::new(move || held.clone()));
        let got = a.assign(&"mesh1.rpc.1".parse().unwrap(), &RPC_NODE, false).unwrap();
        assert_eq!(got.transport.ip(), "10.9.0.4".parse::<IpAddr>().unwrap(), "the two addresses other births hold are skipped");
        let next = a.assign(&"mesh1.rpc.2".parse().unwrap(), &RPC_NODE, false).unwrap();
        assert_eq!(next.transport.ip(), "10.9.0.5".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn the_default_range_lies_below_the_ephemeral_range() {
        if std::env::var("RAFKA_ENDPOINT_PORT_RANGE").is_ok() {
            return;
        }
        let (first, last) = port_range_from_env();
        assert_eq!((first, last), (20000, 29999));
        if let Some((lo, hi)) = ephemeral_range() {
            assert!(last < lo || first > hi, "{first}-{last} overlaps the ephemeral range {lo}-{hi}");
        }
    }

    #[test]
    fn a_partial_listener_allocation_frees_what_it_took() {
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30010, 30010);
        assert!(matches!(a.assign(&p("mesh1.admin.1"), &NODE_ADMIN, false), Err(AllocationError::Exhausted { .. })));
        assert_eq!(a.in_use_count(), 0, "the transport taken before the listener failed was freed");
        assert!(a.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).is_ok());
    }

    #[test]
    fn per_node_addressing_gives_each_node_its_own_address_kept_across_a_restart() {
        let first = std::net::Ipv4Addr::new(10, 9, 0, 2);
        let mut a = EndpointAllocator::per_node(first, std::net::Ipv4Addr::new(10, 9, 0, 3), 41000, 41000);
        let one = a.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).unwrap();
        let two = a.assign(&p("mesh1.rpc.2"), &RPC_NODE, false).unwrap();
        assert_eq!(one.transport.ip(), IpAddr::from(first));
        assert_eq!(two.transport.ip(), IpAddr::from([10, 9, 0, 3]));
        assert_eq!(one.transport.port(), two.transport.port(), "ports repeat across node addresses");
        assert_eq!(a.assign(&p("mesh1.rpc.3"), &RPC_NODE, false), Err(AllocationError::AddressesExhausted));
        a.release(&p("mesh1.rpc.2"));
        a.assign(&p("mesh1.rpc.3"), &RPC_NODE, false).unwrap();
        // A restart keeps the node's address.
        let mut b = EndpointAllocator::per_node(first, first, 41000, 41000);
        let before = b.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).unwrap();
        let after = b.assign(&p("mesh1.rpc.1"), &RPC_NODE, true).unwrap();
        assert_eq!(after.transport, before.transport);
    }

    #[test]
    fn a_port_bound_by_another_process_is_never_handed_out() {
        let squatter = UdpSocket::bind("127.0.0.1:30100").unwrap();
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30100, 30102);
        let got = a.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).unwrap();
        assert_ne!(got.transport.port(), 30100);
        drop(squatter);
    }

    /// Two node-admins on one host (two fabrics, or two admins of one fabric)
    /// each assign before either launch binds: the probe bind alone sees
    /// every port free, so only the host-wide reservation keeps them apart.
    #[test]
    fn two_allocators_on_one_host_never_hand_out_the_same_port() {
        let (mut a, mut b) = (
            EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30200, 30207),
            EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30200, 30207),
        );
        let x = a.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).unwrap();
        let y = b.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).unwrap();
        assert_ne!(x.transport, y.transport, "{} handed out twice", x.transport);
        // A released port is free to the other allocator again.
        a.release(&p("mesh1.rpc.1"));
        b.release(&p("mesh1.rpc.1"));
        let mut c = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30200, 30200);
        assert!(c.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).is_ok(), "released reservations are gone");
    }

    /// A restarted admin reclaims the address it last held before binding it: the claim died
    /// with its previous process and any allocator takes a dead owner's claim over, so a bind
    /// without the claim races a birth handed the same port. A claim a live process holds is
    /// theirs, and the restart takes a fresh reserved port instead.
    #[test]
    fn a_restart_reclaims_its_kept_transport_unless_a_live_process_holds_the_claim() {
        let kept = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 30400);
        let path = reservation_path(kept);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, "4194305").unwrap();
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30400, 30401);
        assert!(a.reclaim_transport(&p("mesh1.admin.2"), kept), "a dead owner's claim is reclaimed");
        assert_eq!(claim_owner(&path), Some(std::process::id()));
        let mut b = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30400, 30401);
        assert_eq!(b.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).unwrap().transport.port(), 30401, "a reclaimed address is never handed out");
        let mut live = std::process::Command::new("sleep").arg("30").stdout(std::process::Stdio::null()).spawn().unwrap();
        let theirs = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 30402);
        std::fs::write(reservation_path(theirs), live.id().to_string()).unwrap();
        let mut c = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30402, 30403);
        assert!(!c.reclaim_transport(&p("mesh1.admin.3"), theirs), "a live process's claim is theirs");
        assert_eq!(c.take_transport(&p("mesh1.admin.3")).unwrap().port(), 30403);
        live.kill().ok();
        live.wait().ok();
        std::fs::remove_file(reservation_path(theirs)).ok();
        a.release(&p("mesh1.admin.2"));
        b.release(&p("mesh1.rpc.1"));
        c.release(&p("mesh1.admin.3"));
    }

    /// The record check: a socket any node's record names is never handed out, and an assignment
    /// a different node's record names is reported as theirs.
    #[test]
    fn a_socket_the_topology_names_is_never_handed_out_and_another_nodes_assignment_is_named() {
        let theirs = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 30500);
        let holder = p("mesh1.admin.2");
        let (h, s) = (holder.clone(), theirs);
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30500, 30501).with_held_sockets(HeldSockets::new(move || vec![(h.clone(), s)]));
        assert_eq!(a.assign(&p("mesh1.rpc.1"), &RPC_NODE, false).unwrap().transport.port(), 30501, "the named port is skipped without a probe");
        let dead = Assignment { transport: theirs, listeners: vec![] };
        assert_eq!(a.named_by_another(&p("mesh1.rpc.4"), &dead), Some((holder.clone(), theirs)));
        assert_eq!(a.named_by_another(&holder, &dead), None, "a node's own record is not another's");
        a.release(&p("mesh1.rpc.1"));
    }

    /// A reservation whose owner process is gone is stale and is taken over.
    #[test]
    fn a_reservation_left_by_a_dead_process_is_taken_over() {
        let addr = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), 30300);
        let path = reservation_path(addr);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // pid_max on Linux is at most 2^22: this pid cannot be live.
        std::fs::write(&path, "4194305").unwrap();
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 30300, 30300);
        assert!(a.assign(&p("mesh1.rpc.2"), &RPC_NODE, false).is_ok());
        a.release(&p("mesh1.rpc.2"));
        assert!(!path.exists(), "release removes the reservation");
    }
}
