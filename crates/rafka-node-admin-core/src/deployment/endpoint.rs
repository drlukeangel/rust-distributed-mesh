//! What a node kind binds, and what the operating system holds after a runtime exits.
//!
//! A node binds port 0: the operating system assigns every port, and the node reports the
//! address it really bound in its `JoinNode` digest (`crate::join`). Node-admin picks no port,
//! so no window opens between a probe and a bind. What remains here is the shape of each kind's
//! bindings and the `/proc` reading that proves a stopped runtime released its sockets.

use std::collections::BTreeMap;
use std::net::{SocketAddr, UdpSocket};

/// What a socket is: the Iroh transport (UDP) or a listener (TCP).
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
}
