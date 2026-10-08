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
}

impl fmt::Display for AllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted { first, last } => write!(f, "no free port left in {first}..={last}"),
            Self::AddressesExhausted => write!(f, "every node address of the range is held"),
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

    /// `RDM_ENDPOINT_PORT_RANGE=<first>-<last>` (default `20000-29999`) on `127.0.0.1`.
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

    /// The next free port on `ip`: rafka-v2 node-admin's one port search ([`find_free_port`])
    /// over this allocator's range, from its cursor, once around. Held = every socket this
    /// allocator holds and every socket the topology names on `ip`; free = not held, claimed on
    /// the host (a shared host's cross-admin reservation, the record other admins read) and
    /// bindable over TCP and UDP ([`port_bindable`]).
    fn take_addr(&mut self, ip: IpAddr) -> Result<SocketAddr, AllocationError> {
        let mut held: std::collections::BTreeSet<u16> = self.in_use.iter().filter(|a| a.ip() == ip).map(|a| a.port()).collect();
        held.extend(self.held_sockets.now().iter().filter(|(_, s)| s.ip() == ip).map(|(_, s)| s.port()));
        let shared = matches!(self.addressing, Addressing::SharedHost(_));
        // Claim first, probe after: a port another live admin claims is never probe-bound, so a
        // probe here never holds, for an instant, a port another admin's birth is binding. A
        // claimed port that a probe finds held is released again.
        let free = |p: u16| {
            if !shared {
                return true;
            }
            let a = SocketAddr::new(ip, p);
            reserve_on_host(a) && {
                let bindable = port_bindable(p);
                if !bindable {
                    release_on_host(a);
                }
                bindable
            }
        };
        let port = find_free_port(self.next, self.last, &held, &free).or_else(|_| find_free_port(self.first, self.last, &held, &free));
        match port {
            Ok(p) => {
                self.next = if p == self.last { self.first } else { p + 1 };
                let a = SocketAddr::new(ip, p);
                self.in_use.insert(a);
                Ok(a)
            }
            Err(reason) => {
                tracing::info!(%reason, "no port free in this allocator's range");
                Err(AllocationError::Exhausted { first: self.first, last: self.last })
            }
        }
    }

    /// Assign `node` everything a process birth needs.
    ///
    /// - first birth or replacement (`restart = false`): a new transport
    ///   address and a new address per listener;
    /// - restart (`restart = true`): the transport and listener addresses are
    ///   kept.
    pub fn assign(&mut self, node: &PathName, spec: &KindSpec) -> Result<Assignment, AllocationError> {
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
    std::fs::read_to_string(path).ok().and_then(|s| s.split_whitespace().next().and_then(|p| p.parse().ok()))
}

/// rafka-v2 node-admin's one port search: the lowest port at or above `hint` (and at most
/// `ceiling`) that is not `held` (every port a record or the topology names) and is `free` on the
/// system ([`port_bindable`]). `Err` names the searched range and how many ports each rule refused.
fn find_free_port(hint: u16, ceiling: u16, held: &std::collections::BTreeSet<u16>, free: impl Fn(u16) -> bool) -> Result<u16, String> {
    let floor = hint.max(1024);
    let (mut by_record, mut by_system) = (0u32, 0u32);
    for p in floor..=ceiling {
        if held.contains(&p) {
            by_record += 1;
        } else if !free(p) {
            by_system += 1;
        } else {
            return Ok(p);
        }
    }
    Err(format!(
        "no free port in {floor}..={ceiling}: \
         {by_record} held by a spawn record or the topology, {by_system} not bindable over TCP and UDP"
    ))
}

/// Whether `port` can be bound right now on every interface, over TCP AND UDP: a node binds
/// its mesh port over UDP and a listener over TCP, so a port free on one protocol only is not
/// free. (rafka-v2 node-admin's `port_bindable`, the system check of its one port search.)
pub fn port_bindable(port: u16) -> bool {
    std::net::TcpListener::bind(("0.0.0.0", port)).is_ok() && std::net::UdpSocket::bind(("0.0.0.0", port)).is_ok()
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

/// A runtime holds the host-wide claims of the addresses it was handed, as itself, right before
/// it binds them, wherever no live process holds them: the admin that reserved them may die
/// (its claims die with its pid, and a dead owner's claim is any allocator's to take over) while
/// this runtime lives on the ports. A claim the admin still holds stays the admin's: a restart
/// keeps its addresses through the admin's allocator, and the admin outlives the runtime.
pub fn hold_as_runtime(addrs: impl IntoIterator<Item = SocketAddr>) {
    for a in addrs {
        let _ = reserve_on_host(a);
    }
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

/// `RDM_ENDPOINT_PORT_RANGE=<first>-<last>`, default `20000-29999`: below the kernel's ephemeral
/// range, so a port handed to a birth cannot be taken between its assignment and its bind by any
/// process that binds port 0. A configured range that overlaps the ephemeral range is named.
pub fn port_range_from_env() -> (u16, u16) {
    let (first, last) = std::env::var("RDM_ENDPOINT_PORT_RANGE")
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

/// Who holds `addr` right now, from `/proc`: every socket of that transport bound to the
/// port (its state and inode) and the process whose fd table holds each inode (pid and
/// command). "no socket" means the kernel lists none on the port, so a refused bind had another
/// cause. For the error that names a port the operating system still holds after a runtime
/// exited: the holder is a fact, never a guess.
/// One socket on a port, as `/proc` lists it: its state, inode and the live process (if any)
/// whose fd table holds it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PortHolder {
    pub state: String,
    pub inode: u64,
    pub pid: Option<u32>,
    pub comm: String,
}

/// Whether a port a runtime held is now released by it: no socket at all, or every socket on it
/// is kernel-owned (the dead process's lingering connections, which a bind rides over) or held
/// by a live process other than `exited_pid`. A port another live process listens on is proof the
/// exited runtime no longer holds it, however the probe's bind fares; only a socket still held by
/// the exited pid (its group still tearing down) keeps the port "still held".
pub fn released_by(addr: SocketAddr, transport: SlotTransport, exited_pid: Option<u32>) -> bool {
    let holders = port_holders(addr, transport);
    !holders.iter().any(|h| h.pid.is_some() && h.pid == exited_pid)
        && (holders.iter().any(|h| h.pid.is_some()) || !match transport {
            SlotTransport::Udp => udp_port_is_held(addr),
            SlotTransport::Tcp => tcp_port_is_held(addr),
        })
}

pub fn port_holder(addr: SocketAddr, transport: SlotTransport) -> String {
    let holders = port_holders(addr, transport);
    if holders.is_empty() {
        return "no socket".to_string();
    }
    holders
        .iter()
        .map(|h| format!("{} inode {} held by {}", h.state, h.inode, match h.pid { Some(p) => format!("pid {p} {}", h.comm), None => "no process (kernel-owned)".to_string() }))
        .collect::<Vec<_>>()
        .join("; ")
}

/// Every socket on `addr`'s port for `transport`, with its holder (see [`port_holder`]).
pub fn port_holders(addr: SocketAddr, transport: SlotTransport) -> Vec<PortHolder> {
    let tables: &[&str] = match transport {
        SlotTransport::Tcp => &["/proc/net/tcp", "/proc/net/tcp6"],
        SlotTransport::Udp => &["/proc/net/udp", "/proc/net/udp6"],
    };
    let port = format!("{:04X}", addr.port());
    let mut sockets: Vec<(String, u64)> = Vec::new();
    for t in tables {
        let Ok(text) = std::fs::read_to_string(t) else { continue };
        for line in text.lines().skip(1) {
            let f: Vec<&str> = line.split_whitespace().collect();
            if f.len() < 10 {
                continue;
            }
            let Some((_, local_port)) = f[1].rsplit_once(':') else { continue };
            if local_port != port {
                continue;
            }
            let state = match f[3] {
                "01" => "established",
                "06" => "time-wait",
                "07" => "close",
                "08" => "close-wait",
                "0A" => "listen",
                other => other,
            };
            if let Ok(inode) = f[9].parse::<u64>() {
                sockets.push((state.to_string(), inode));
            }
        }
    }
    if sockets.is_empty() {
        return Vec::new();
    }
    let mut owners: BTreeMap<u64, (u32, String)> = BTreeMap::new();
    if let Ok(procs) = std::fs::read_dir("/proc") {
        for p in procs.flatten() {
            let Some(pid) = p.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else { continue };
            let Ok(fds) = std::fs::read_dir(p.path().join("fd")) else { continue };
            for fd in fds.flatten() {
                if let Ok(target) = std::fs::read_link(fd.path()) {
                    let t = target.to_string_lossy();
                    if let Some(inode) = t.strip_prefix("socket:[").and_then(|r| r.strip_suffix(']')).and_then(|n| n.parse::<u64>().ok()) {
                        if sockets.iter().any(|(_, i)| *i == inode) {
                            let comm = std::fs::read_to_string(p.path().join("comm")).map(|c| c.trim().to_string()).unwrap_or_default();
                            owners.entry(inode).or_insert((pid, comm));
                        }
                    }
                }
            }
        }
    }
    sockets
        .into_iter()
        .map(|(state, inode)| {
            let (pid, comm) = owners.get(&inode).cloned().map(|(p, c)| (Some(p), c)).unwrap_or((None, String::new()));
            PortHolder { state, inode, pid, comm }
        })
        .collect()
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

    /// CONTRACT: a TCP port this process listens on is named as held by this process (listen
    /// state, our pid); a port nobody holds is "no socket".
    #[test]
    fn port_holder_names_this_process_for_its_own_listener_and_no_socket_for_a_free_port() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        let who = port_holder(addr, SlotTransport::Tcp);
        assert!(who.contains("listen"), "{who}");
        assert!(who.contains(&format!("pid {} ", std::process::id())), "{who}");
        drop(l);
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        assert_eq!(port_holder(free, SlotTransport::Tcp), "no socket");
    }

    /// CONTRACT: a port another live process listens on is released by the exited runtime (its
    /// pid is not among the holders); a port this very process still listens on is not released
    /// when this process is the one said to have exited; a free port is released.
    #[test]
    fn a_port_another_live_process_listens_on_is_released_by_the_exited_runtime() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        assert!(released_by(addr, SlotTransport::Tcp, Some(1)), "another live pid listens: released");
        assert!(!released_by(addr, SlotTransport::Tcp, Some(std::process::id())), "the exited pid itself still listens: not released");
        drop(l);
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        assert!(released_by(free, SlotTransport::Tcp, Some(1)));
    }

    fn p(s: &str) -> PathName {
        s.parse().unwrap()
    }

    /// An allocator over the lane's whole range: a port is taken one at a time, each checked
    /// against the host-wide claims and bindable.
    fn alloc() -> EndpointAllocator {
        let (first, last) = port_range_from_env();
        EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), first, last)
    }

    /// `n` distinct ports from the top of the lane's range that are unclaimed and bindable now,
    /// for a test that needs a range of known size. Allocators scan upward from the bottom, so
    /// these are not taken by a sibling test's allocator.
    fn top_free_ports(n: usize) -> Vec<u16> {
        // Ports this helper already handed to a sibling test are not handed again.
        static HANDED: std::sync::Mutex<BTreeSet<u16>> = std::sync::Mutex::new(BTreeSet::new());
        let mut handed = HANDED.lock().unwrap_or_else(|e| e.into_inner());
        let (first, last) = port_range_from_env();
        let got: Vec<u16> = (first..=last)
            .rev()
            .filter(|p| !handed.contains(p))
            .filter(|p| !claim_owner(&reservation_path(SocketAddr::from(([127, 0, 0, 1], *p)))).is_some_and(process_is_alive) && port_bindable(*p))
            .take(n)
            .collect();
        assert_eq!(got.len(), n, "the lane's range {first}-{last} has {n} free ports");
        handed.extend(got.iter().copied());
        got
    }

    fn one_port_allocator() -> (u16, EndpointAllocator) {
        let p = top_free_ports(1)[0];
        (p, EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), p, p))
    }

    #[test]
    fn no_collisions_across_a_large_allocation() {
        let mut a = alloc();
        let mut seen = BTreeSet::new();
        for i in 1..=400 {
            let got = a.assign(&p(&format!("mesh1.rpc.{i}")), &RPC_NODE).unwrap();
            assert!(seen.insert(got.transport), "port {} handed out twice", got.transport);
            assert!(got.listeners.is_empty());
        }
        assert_eq!(seen.len(), 400, "one socket per process");
        assert_eq!(a.in_use_count(), 400);
    }

    /// CONTRACT (fabric-node-lifecycle.md: a restart "reuses identity and data dir, binds fresh
    /// ports"; rafka-v2 node-admin's respawn rule): a node assigned again is handed fresh ports,
    /// never its recorded ones, and the recorded ones are released.
    #[test]
    fn a_respawn_is_handed_fresh_ports_never_the_recorded_ones() {
        let mut a = alloc();
        let node = p("mesh1.rpc.2");
        let first = a.assign(&node, &RPC_NODE).unwrap();
        let again = a.assign(&node, &RPC_NODE).unwrap();
        assert_ne!(first.transport, again.transport, "a respawn is handed a fresh transport");
        assert_eq!(a.in_use_count(), 1, "the recorded assignment was released");
    }

    #[test]
    fn a_listener_kind_gets_its_own_tcp_address_beside_the_transport() {
        let mut a = alloc();
        let got = a.assign(&p("mesh1.admin.1"), &NODE_ADMIN).unwrap();
        assert_eq!(got.listeners.len(), 1);
        assert_eq!(got.listeners[0].0, "control");
        assert_ne!(got.listeners[0].1, got.transport);
        let sockets = got.sockets();
        assert_eq!(sockets[0].2, SlotTransport::Udp);
        assert_eq!(sockets[1].2, SlotTransport::Tcp);
        let again = a.assign(&p("mesh1.admin.1"), &NODE_ADMIN).unwrap();
        assert_ne!(again.listeners, got.listeners, "a respawn is handed a fresh listener address");
    }

    fn held_ports(ports: &[u16]) -> std::collections::BTreeSet<u16> {
        ports.iter().copied().collect()
    }

    /// CONTRACT (rafka-v2 node-admin port_lease): a port an in-flight spawn's placeholder holds is
    /// never handed to a second spawn, though nothing has bound it yet.
    #[test]
    fn a_port_held_by_an_in_flight_placeholder_is_never_handed_out_again() {
        assert_eq!(find_free_port(24_006, 60_000, &held_ports(&[24_006]), |_| true), Ok(24_007));
    }

    /// CONTRACT (rafka-v2 node-admin port_lease): a port the system will not bind is skipped.
    #[test]
    fn a_port_the_system_will_not_bind_is_skipped() {
        assert_eq!(find_free_port(24_000, 60_000, &held_ports(&[]), |p| p != 24_000 && p != 24_001), Ok(24_002));
    }

    /// CONTRACT (rafka-v2 node-admin port_lease): an exhausted range is refused by name, with how
    /// many ports each rule refused, never answered with a port that may be taken.
    #[test]
    fn an_exhausted_range_is_refused_by_name() {
        let err = find_free_port(59_999, 60_000, &held_ports(&[59_999]), |p| p != 60_000).expect_err("nothing free");
        assert!(err.contains("59999..=60000"), "{err}");
        assert!(err.contains("1 held by a spawn record or the topology") && err.contains("1 not bindable"), "{err}");
    }

    #[test]
    fn released_ports_return() {
        let (_, mut a) = one_port_allocator();
        a.assign(&p("mesh1.rpc.1"), &RPC_NODE).unwrap();
        assert!(matches!(a.assign(&p("mesh1.rpc.2"), &RPC_NODE), Err(AllocationError::Exhausted { .. })));
        assert!(matches!(a.assign(&p("mesh1.rpc.2"), &RPC_NODE), Err(AllocationError::Exhausted { .. })), "a failed allocation leaks nothing");
        a.release(&p("mesh1.rpc.1"));
        assert!(a.assign(&p("mesh1.rpc.2"), &RPC_NODE).is_ok());
    }

    #[test]
    fn a_container_address_held_on_the_network_by_another_admins_birth_is_never_handed_out() {
        let held: BTreeSet<IpAddr> = ["10.9.0.2".parse().unwrap(), "10.9.0.3".parse().unwrap()].into_iter().collect();
        let mut a = EndpointAllocator::per_node("10.9.0.2".parse().unwrap(), "10.9.0.9".parse().unwrap(), 20000, 20010)
            .with_held_elsewhere(HeldElsewhere::new(move || held.clone()));
        let got = a.assign(&"mesh1.rpc.1".parse().unwrap(), &RPC_NODE).unwrap();
        assert_eq!(got.transport.ip(), "10.9.0.4".parse::<IpAddr>().unwrap(), "the two addresses other births hold are skipped");
        let next = a.assign(&"mesh1.rpc.2".parse().unwrap(), &RPC_NODE).unwrap();
        assert_eq!(next.transport.ip(), "10.9.0.5".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn the_default_range_lies_below_the_ephemeral_range() {
        if std::env::var("RDM_ENDPOINT_PORT_RANGE").is_ok() {
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
        let (_, mut a) = one_port_allocator();
        assert!(matches!(a.assign(&p("mesh1.admin.1"), &NODE_ADMIN), Err(AllocationError::Exhausted { .. })));
        assert_eq!(a.in_use_count(), 0, "the transport taken before the listener failed was freed");
        assert!(a.assign(&p("mesh1.rpc.1"), &RPC_NODE).is_ok());
    }

    #[test]
    fn per_node_addressing_gives_each_node_its_own_address_kept_across_a_restart() {
        let first = std::net::Ipv4Addr::new(10, 9, 0, 2);
        let mut a = EndpointAllocator::per_node(first, std::net::Ipv4Addr::new(10, 9, 0, 3), 41000, 41000);
        let one = a.assign(&p("mesh1.rpc.1"), &RPC_NODE).unwrap();
        let two = a.assign(&p("mesh1.rpc.2"), &RPC_NODE).unwrap();
        assert_eq!(one.transport.ip(), IpAddr::from(first));
        assert_eq!(two.transport.ip(), IpAddr::from([10, 9, 0, 3]));
        assert_eq!(one.transport.port(), two.transport.port(), "ports repeat across node addresses");
        assert_eq!(a.assign(&p("mesh1.rpc.3"), &RPC_NODE), Err(AllocationError::AddressesExhausted));
        a.release(&p("mesh1.rpc.2"));
        a.assign(&p("mesh1.rpc.3"), &RPC_NODE).unwrap();
    }

    #[test]
    fn a_port_bound_by_another_process_is_never_handed_out() {
        // The squatter holds the port the allocator would try first; a squatter on a one-port
        // range leaves nothing to hand out.
        let squat = top_free_ports(1)[0];
        let squatter = UdpSocket::bind(SocketAddr::new(IpAddr::from([127, 0, 0, 1]), squat)).unwrap();
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), squat, squat);
        assert!(matches!(a.assign(&p("mesh1.rpc.1"), &RPC_NODE), Err(AllocationError::Exhausted { .. })), "the squatted port is never handed out");
        drop(squatter);
        assert!(a.assign(&p("mesh1.rpc.1"), &RPC_NODE).is_ok(), "once the squatter lets go the port is free");
    }

    /// Two node-admins on one host (two fabrics, or two admins of one fabric)
    /// each assign before either launch binds: the probe bind alone sees
    /// every port free, so only the host-wide reservation keeps them apart.
    #[test]
    fn two_allocators_on_one_host_never_hand_out_the_same_port() {
        let (mut a, mut b) = (alloc(), alloc());
        let mut seen = BTreeSet::new();
        for i in 1..=100 {
            for (who, al) in [("a", &mut a), ("b", &mut b)] {
                let got = al.assign(&p(&format!("mesh1.rpc.{i}")), &RPC_NODE).unwrap();
                assert!(seen.insert(got.transport), "{who} was handed {}, already handed out", got.transport);
            }
        }
        assert_eq!(seen.len(), 200);
        // A released port is free to the other allocator again.
        let freed = a.held.get(&p("mesh1.rpc.1")).unwrap().transport;
        a.release(&p("mesh1.rpc.1"));
        let mut c = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), freed.port(), freed.port());
        assert!(c.assign(&p("mesh1.rpc.1"), &RPC_NODE).is_ok(), "released reservations are gone");
    }

    /// The record check: a socket any node's record names is never handed out.
    #[test]
    fn a_socket_the_topology_names_is_never_handed_out() {
        let (first, last) = port_range_from_env();
        let theirs = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), first);
        let holder = p("mesh1.admin.2");
        let (h, s) = (holder.clone(), theirs);
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), first, last).with_held_sockets(HeldSockets::new(move || vec![(h.clone(), s)]));
        assert_ne!(a.assign(&p("mesh1.rpc.1"), &RPC_NODE).unwrap().transport.port(), first, "the named port is skipped without a probe");
        a.release(&p("mesh1.rpc.1"));
    }

    /// A runtime holds its handed addresses as itself: once it has, the death of the admin that
    /// reserved them leaves no dead-owner claim for another allocator to take over.
    #[test]
    fn a_runtime_holding_its_addresses_keeps_them_out_of_another_allocators_hands() {
        let ports = top_free_ports(2);
        let (block, spare) = (ports[0], ports[1]);
        let handed = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), block);
        let path = reservation_path(handed);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // The reserving admin died: its claim names a pid that cannot be live.
        std::fs::write(&path, "4194305").unwrap();
        // The runtime it launched takes the claim as itself (a live process).
        hold_as_runtime([handed]);
        assert_eq!(claim_owner(&path), Some(std::process::id()));
        // A claim a live process holds (the admin, across this runtime's restarts) stays theirs.
        let mut live = std::process::Command::new("sleep").arg("30").stdout(std::process::Stdio::null()).spawn().unwrap();
        let admins = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), spare);
        std::fs::write(reservation_path(admins), live.id().to_string()).unwrap();
        hold_as_runtime([admins]);
        assert_eq!(claim_owner(&reservation_path(admins)), Some(live.id()), "the admin's live claim is not overwritten");
        live.kill().ok();
        live.wait().ok();
        std::fs::remove_file(reservation_path(admins)).ok();
        let mut other = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), block, block);
        assert!(matches!(other.assign(&p("mesh1.rpc.1"), &RPC_NODE), Err(AllocationError::Exhausted { .. })), "the runtime's port is never handed out");
        std::fs::remove_file(&path).ok();
    }

    /// A reservation whose owner process is gone is stale and is taken over.
    #[test]
    fn a_reservation_left_by_a_dead_process_is_taken_over() {
        let one = top_free_ports(1)[0];
        let addr = SocketAddr::new(IpAddr::from([127, 0, 0, 1]), one);
        let path = reservation_path(addr);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        // pid_max on Linux is at most 2^22: this pid cannot be live.
        std::fs::write(&path, "4194305").unwrap();
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), one, one);
        assert!(a.assign(&p("mesh1.rpc.2"), &RPC_NODE).is_ok());
        a.release(&p("mesh1.rpc.2"));
        assert!(!path.exists(), "release removes the reservation");
    }
}

/// Run `f`, a short synchronous section that touches the file system or binds probe sockets, off
/// the runtime's core: on a multi-thread runtime the worker hands its queued tasks to the rest of
/// the runtime first (`block_in_place`); elsewhere `f` runs as is.
pub fn off_the_core<T>(f: impl FnOnce() -> T) -> T {
    match tokio::runtime::Handle::try_current() {
        Ok(h) if h.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => tokio::task::block_in_place(f),
        _ => f(),
    }
}
