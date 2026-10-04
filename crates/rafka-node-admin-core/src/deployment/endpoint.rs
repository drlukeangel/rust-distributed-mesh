//! Endpoint authority (PRD §8, gap R8; mesh-control-plane.md §6).
//!
//! Node-admin assigns every externally advertised endpoint before a provider
//! launches anything. A provider must honour the assignment; it never invents
//! an advertised port. `WaitForBind` checks that independently: it asks the
//! operating system whether the assigned UDP port is actually held, rather than
//! trusting the runtime's own report.

use crate::model::{EndpointSlot, FreshnessToken, PathName, SlotPolicy};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::net::{IpAddr, SocketAddr, UdpSocket};

/// One slot a node kind declares.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotSpec {
    pub slot: &'static str,
    pub policy: SlotPolicy,
}

/// The two Node RPC endpoint slots of an rpc node (`docs/i143/design.md` §2).
pub const RPC_NODE_SLOTS: &[SlotSpec] =
    &[SlotSpec { slot: "rpc-0", policy: SlotPolicy::Fresh }, SlotSpec { slot: "rpc-1", policy: SlotPolicy::Stable }];

/// A node-admin's mesh endpoint and its control API (both fresh on restart).
pub const NODE_ADMIN_SLOTS: &[SlotSpec] =
    &[SlotSpec { slot: "mesh", policy: SlotPolicy::Fresh }, SlotSpec { slot: "control", policy: SlotPolicy::Fresh }];

/// Why an allocation was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AllocationError {
    /// No free port left in the configured range.
    Exhausted { first: u16, last: u16 },
    /// Every node address of the range is held.
    AddressesExhausted,
    /// A stable slot was asked to survive a restart it has no prior assignment for.
    NoPriorAssignment { node: PathName, slot: String },
}

impl fmt::Display for AllocationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exhausted { first, last } => write!(f, "no free port left in {first}..={last}"),
            Self::AddressesExhausted => write!(f, "every node address of the range is held"),
            Self::NoPriorAssignment { node, slot } => write!(f, "{node} slot {slot}: a restart keeps a stable slot, but none was assigned"),
        }
    }
}

/// `WaitForBind` refusals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindRefusal {
    /// Nothing holds the assigned port: the runtime bound somewhere else or not at all.
    NotBoundAtAssigned { slot: String, assigned: SocketAddr },
    /// The runtime reports a different address for the slot than node-admin assigned.
    ReportedElsewhere { slot: String, assigned: SocketAddr, reported: SocketAddr },
    /// The runtime reported no address for an assigned slot.
    SlotMissing { slot: String },
}

impl fmt::Display for BindRefusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotBoundAtAssigned { slot, assigned } => write!(f, "slot {slot}: nothing is bound at the assigned {assigned}"),
            Self::ReportedElsewhere { slot, assigned, reported } => {
                write!(f, "slot {slot}: assigned {assigned} but the runtime bound {reported}; a provider cannot invent an advertised port")
            }
            Self::SlotMissing { slot } => write!(f, "slot {slot}: the runtime reported no address"),
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
    PerNode { first: u32, last: u32, next: u32 },
}

/// Allocates advertised endpoints. Addresses already handed out are never
/// handed out again until released.
#[derive(Debug)]
pub struct EndpointAllocator {
    addressing: Addressing,
    first: u16,
    last: u16,
    next: u16,
    held: BTreeMap<PathName, Vec<EndpointSlot>>,
    in_use: BTreeSet<SocketAddr>,
}

impl EndpointAllocator {
    pub fn new(host: IpAddr, first: u16, last: u16) -> Self {
        assert!(first <= last, "empty port range");
        Self { addressing: Addressing::SharedHost(host), first, last, next: first, held: BTreeMap::new(), in_use: BTreeSet::new() }
    }

    /// One address per node from `first_ip..=last_ip`, ports from `first..=last`.
    pub fn per_node(first_ip: std::net::Ipv4Addr, last_ip: std::net::Ipv4Addr, first: u16, last: u16) -> Self {
        assert!(first <= last, "empty port range");
        let (a, b) = (u32::from(first_ip), u32::from(last_ip));
        assert!(a <= b, "empty address range");
        Self {
            addressing: Addressing::PerNode { first: a, last: b, next: a },
            first,
            last,
            next: first,
            held: BTreeMap::new(),
            in_use: BTreeSet::new(),
        }
    }

    /// `RAFKA_ENDPOINT_PORT_RANGE=<first>-<last>` (default `41000-48999`) on `127.0.0.1`.
    pub fn from_env() -> Self {
        let (first, last) = port_range_from_env();
        Self::new(IpAddr::from([127, 0, 0, 1]), first, last)
    }

    /// The address `node` advertises on: the shared host, the node's own
    /// address when it holds one, or the next address no node holds.
    fn node_ip(&mut self, node: &PathName) -> Result<IpAddr, AllocationError> {
        match &mut self.addressing {
            Addressing::SharedHost(h) => Ok(*h),
            Addressing::PerNode { first, last, next } => {
                if let Some(e) = self.held.get(node).and_then(|s| s.first()) {
                    return Ok(e.addr.ip());
                }
                let taken: BTreeSet<IpAddr> = self.held.values().flatten().map(|e| e.addr.ip()).collect();
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
            let free = match self.addressing {
                Addressing::SharedHost(_) => UdpSocket::bind(a).is_ok() && std::net::TcpListener::bind(a).is_ok(),
                Addressing::PerNode { .. } => true,
            };
            if free {
                self.in_use.insert(a);
                return Ok(a);
            }
        }
        Err(AllocationError::Exhausted { first: self.first, last: self.last })
    }

    /// Assign every slot of `node` for a new process birth.
    ///
    /// - first birth or replacement (`restart = false`): every slot gets a new
    ///   port and a new freshness token;
    /// - restart (`restart = true`): `stable` slots keep their address and
    ///   token, `fresh` slots get a new port and token.
    pub fn assign(&mut self, node: &PathName, slots: &[SlotSpec], restart: bool) -> Result<Vec<EndpointSlot>, AllocationError> {
        let prior = if restart { self.held.get(node).cloned().unwrap_or_default() } else { Vec::new() };
        // Refuse before taking anything, so a refusal leaks no port.
        if restart {
            if let Some(s) = slots.iter().find(|s| s.policy == SlotPolicy::Stable && !prior.iter().any(|e| e.slot == s.slot)) {
                return Err(AllocationError::NoPriorAssignment { node: node.clone(), slot: s.slot.into() });
            }
        } else {
            self.release(node);
        }
        let ip = self.node_ip(node)?;
        let mut out = Vec::new();
        let mut taken = Vec::new();
        for s in slots {
            if restart && s.policy == SlotPolicy::Stable {
                out.push(prior.iter().find(|e| e.slot == s.slot).expect("checked above").clone());
                continue;
            }
            match self.take_addr(ip) {
                Ok(addr) => {
                    taken.push(addr);
                    out.push(EndpointSlot { slot: s.slot.into(), addr, freshness: FreshnessToken::mint() });
                }
                Err(e) => {
                    for a in taken {
                        self.in_use.remove(&a);
                    }
                    return Err(e);
                }
            }
        }
        // Release the fresh slots' old ports now that the new ones are held.
        for p in &prior {
            if !out.iter().any(|e| e.addr == p.addr) {
                self.in_use.remove(&p.addr);
            }
        }
        self.held.insert(node.clone(), out.clone());
        Ok(out)
    }

    /// Release every port of `node` (retire pipeline `ReleaseEndpoints`).
    pub fn release(&mut self, node: &PathName) {
        if let Some(slots) = self.held.remove(node) {
            for s in slots {
                self.in_use.remove(&s.addr);
            }
        }
    }

    /// Record an assignment that already exists (an admin adopting a running
    /// node after a takeover), so its ports are never handed out again.
    pub fn adopt(&mut self, node: &PathName, slots: Vec<EndpointSlot>) {
        for s in &slots {
            self.in_use.insert(s.addr);
        }
        self.held.insert(node.clone(), slots);
    }

    /// How many advertised addresses are held across all nodes.
    pub fn in_use_count(&self) -> usize {
        self.in_use.len()
    }

    pub fn held(&self, node: &PathName) -> Option<&[EndpointSlot]> {
        self.held.get(node).map(Vec::as_slice)
    }
}

/// `RAFKA_ENDPOINT_PORT_RANGE=<first>-<last>`, default `41000-48999`.
pub fn port_range_from_env() -> (u16, u16) {
    std::env::var("RAFKA_ENDPOINT_PORT_RANGE")
        .ok()
        .and_then(|r| {
            let (a, b) = r.split_once('-')?;
            Some((a.trim().parse().ok()?, b.trim().parse().ok()?))
        })
        .unwrap_or((41000, 48999))
}

/// Is something on this host holding UDP `addr`? A bind that fails with
/// `AddrInUse` means yes.
pub fn udp_port_is_held(addr: SocketAddr) -> bool {
    matches!(UdpSocket::bind(addr), Err(e) if e.kind() == std::io::ErrorKind::AddrInUse)
}

/// `WaitForBind`: every assigned slot is held at its assigned address, and the
/// runtime's own report (when it gives one) names exactly that address.
pub fn verify_bound(assigned: &[EndpointSlot], reported: &[(String, SocketAddr)]) -> Result<(), BindRefusal> {
    verify_bound_with(assigned, reported, udp_port_is_held)
}

/// [`verify_bound`] with the provider's own view of what the runtime holds
/// (a container's sockets live in its network namespace, not the host's).
pub fn verify_bound_with(
    assigned: &[EndpointSlot],
    reported: &[(String, SocketAddr)],
    held: impl Fn(SocketAddr) -> bool,
) -> Result<(), BindRefusal> {
    for a in assigned {
        match reported.iter().find(|(s, _)| *s == a.slot) {
            None if !reported.is_empty() => return Err(BindRefusal::SlotMissing { slot: a.slot.clone() }),
            Some((_, r)) if *r != a.addr => {
                return Err(BindRefusal::ReportedElsewhere { slot: a.slot.clone(), assigned: a.addr, reported: *r })
            }
            _ => {}
        }
        if !held(a.addr) {
            return Err(BindRefusal::NotBoundAtAssigned { slot: a.slot.clone(), assigned: a.addr });
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
        EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 52000, 54999)
    }

    #[test]
    fn no_collisions_across_a_large_allocation() {
        let mut a = alloc();
        let mut seen = BTreeSet::new();
        for i in 1..=400 {
            for e in a.assign(&p(&format!("mesh1.rpc.{i}")), RPC_NODE_SLOTS, false).unwrap() {
                assert!(seen.insert(e.addr), "port {} handed out twice", e.addr);
            }
        }
        assert_eq!(seen.len(), 800);
    }

    #[test]
    fn a_restart_moves_only_the_fresh_slot_and_keeps_the_stable_token() {
        let mut a = alloc();
        let node = p("mesh1.rpc.2");
        let first = a.assign(&node, RPC_NODE_SLOTS, false).unwrap();
        let again = a.assign(&node, RPC_NODE_SLOTS, true).unwrap();
        assert_ne!(first[0].addr, again[0].addr, "rpc-0 is fresh");
        assert_ne!(first[0].freshness, again[0].freshness);
        assert_eq!(first[1], again[1], "rpc-1 is stable: same address and token");
        let replaced = a.assign(&node, RPC_NODE_SLOTS, false).unwrap();
        assert_ne!(replaced[1].freshness, again[1].freshness, "a replacement gets new tokens everywhere");
    }

    #[test]
    fn released_ports_return_and_a_restart_without_prior_is_named() {
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 55000, 55003);
        a.assign(&p("mesh1.rpc.1"), RPC_NODE_SLOTS, false).unwrap();
        a.assign(&p("mesh1.rpc.2"), RPC_NODE_SLOTS, false).unwrap();
        assert!(matches!(a.assign(&p("mesh1.rpc.3"), RPC_NODE_SLOTS, false), Err(AllocationError::Exhausted { .. })));
        assert!(matches!(a.assign(&p("mesh1.rpc.3"), RPC_NODE_SLOTS, false), Err(AllocationError::Exhausted { .. })), "a failed allocation leaks nothing");
        a.release(&p("mesh1.rpc.1"));
        assert!(a.assign(&p("mesh1.rpc.3"), RPC_NODE_SLOTS, false).is_ok());
        assert_eq!(
            a.assign(&p("mesh1.rpc.9"), RPC_NODE_SLOTS, true),
            Err(AllocationError::NoPriorAssignment { node: p("mesh1.rpc.9"), slot: "rpc-1".into() })
        );
    }

    #[test]
    fn per_node_addressing_gives_each_node_its_own_address_kept_across_a_restart() {
        let first = std::net::Ipv4Addr::new(10, 9, 0, 2);
        let mut a = EndpointAllocator::per_node(first, std::net::Ipv4Addr::new(10, 9, 0, 3), 41000, 41001);
        let one = a.assign(&p("mesh1.rpc.1"), RPC_NODE_SLOTS, false).unwrap();
        let two = a.assign(&p("mesh1.rpc.2"), RPC_NODE_SLOTS, false).unwrap();
        assert!(one.iter().all(|e| e.addr.ip() == IpAddr::from(first)));
        assert!(two.iter().all(|e| e.addr.ip() == IpAddr::from([10, 9, 0, 3])));
        assert_eq!(one.iter().map(|e| e.addr.port()).collect::<Vec<_>>(), two.iter().map(|e| e.addr.port()).collect::<Vec<_>>(), "ports repeat across node addresses");
        assert_eq!(a.assign(&p("mesh1.rpc.3"), RPC_NODE_SLOTS, false), Err(AllocationError::AddressesExhausted));
        a.release(&p("mesh1.rpc.2"));
        a.assign(&p("mesh1.rpc.3"), &RPC_NODE_SLOTS[1..], false).unwrap();
        // A restart keeps the node's address and its stable slot; the fresh
        // slot moves to the one free port left on that address.
        let mut b = EndpointAllocator::per_node(first, first, 41000, 41002);
        let before = b.assign(&p("mesh1.rpc.1"), RPC_NODE_SLOTS, false).unwrap();
        let after = b.assign(&p("mesh1.rpc.1"), RPC_NODE_SLOTS, true).unwrap();
        assert_eq!(after[1], before[1]);
        assert_eq!(after[0].addr, SocketAddr::new(IpAddr::from(first), 41002));
    }

    #[test]
    fn a_port_bound_by_another_process_is_never_handed_out() {
        let squatter = UdpSocket::bind("127.0.0.1:56000").unwrap();
        let mut a = EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 56000, 56002);
        let got = a.assign(&p("mesh1.rpc.1"), RPC_NODE_SLOTS, false).unwrap();
        assert!(got.iter().all(|e| e.addr.port() != 56000));
        drop(squatter);
    }
}
