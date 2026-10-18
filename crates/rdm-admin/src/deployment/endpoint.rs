//! What a node kind binds, and what the operating system holds after a runtime exits.
//!
//! A node binds port 0: the operating system assigns every port, and the node reports the
//! address it really bound in its `JoinNode` digest (`crate::join`). Node-admin picks no port,
//! so no window opens between a probe and a bind. What remains here is the shape of each kind's
//! bindings and the `/proc` reading that proves a stopped runtime released its sockets.

use std::collections::BTreeMap;
use std::net::{SocketAddr, UdpSocket};
use std::path::Path;

/// What a socket is: the Iroh transport (UDP) or a listener (TCP).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotTransport {
    /// The Iroh transport.
    Udp,
    /// A listener.
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

/// A node-admin: its mesh transport (gossip and Node RPC), no other listener: RDM opens no HTTP
/// listener (rdm-crate-and-test-layout.md §2).
pub(crate) const NODE_ADMIN: KindSpec = KindSpec { listeners: &[] };

/// What a node kind binds and serves.
pub(crate) fn spec_for(kind: crate::model::NodeKind) -> &'static KindSpec {
    match kind {
        // A product kind binds the one transport here; its own listeners are the product's to
        // assign through its node-admin binding.
        crate::model::NodeKind::RpcNode | crate::model::NodeKind::Broker | crate::model::NodeKind::Gateway | crate::model::NodeKind::Compute => &RPC_NODE,
        crate::model::NodeKind::NodeAdmin => &NODE_ADMIN,
    }
}

/// Is something on this host holding UDP `addr`? A bind that fails with
/// `AddrInUse` means yes.
pub(crate) fn udp_port_is_held(addr: SocketAddr) -> bool {
    matches!(UdpSocket::bind(addr), Err(e) if e.kind() == std::io::ErrorKind::AddrInUse)
}

/// Is something on this host listening on TCP `addr`?
pub(crate) fn tcp_port_is_held(addr: SocketAddr) -> bool {
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
pub(crate) struct PortHolder {
    pub state: String,
    pub inode: u64,
    pub pid: Option<u32>,
    pub comm: String,
}

/// Whether a port a runtime held is now released by it: no socket at all, or every socket on it
/// is kernel-owned (the dead process's lingering connections, which a bind rides over) or held
/// by a live process other than `exited_pid`. A port another live process listens on is proof the
/// exited runtime no longer holds it, however the probe's bind fares; only a socket still held by
/// the exited pid (its group still tearing down) keeps the port "still held". `Err` names the
/// `/proc` table or directory that could not be read: a port is never released on a table never
/// read. A blocking call (it reads `/proc` and binds probes): run it on the blocking pool.
pub(crate) fn released_by(addr: SocketAddr, transport: SlotTransport, exited_pid: Option<u32>) -> Result<bool, String> {
    let holders = port_holders(addr, transport)?;
    Ok(!holders.iter().any(|h| h.pid.is_some() && h.pid == exited_pid)
        && (holders.iter().any(|h| h.pid.is_some()) || !match transport {
            SlotTransport::Udp => udp_port_is_held(addr),
            SlotTransport::Tcp => tcp_port_is_held(addr),
        }))
}

/// Who holds the socket at `addr` for `transport`, in words; `no socket` when none does. `Err`
/// names the `/proc` table or directory that could not be read. A blocking call: run it on the
/// blocking pool.
pub fn port_holder(addr: SocketAddr, transport: SlotTransport) -> Result<String, String> {
    let holders = port_holders(addr, transport)?;
    if holders.is_empty() {
        return Ok("no socket".to_string());
    }
    Ok(holders
        .iter()
        .map(|h| {
            format!(
                "{} inode {} held by {}",
                h.state,
                h.inode,
                match h.pid {
                    Some(p) => format!("pid {p} {}", h.comm),
                    None => "no process whose descriptors this node-admin can read (kernel-owned, or another user's)".to_string(),
                }
            )
        })
        .collect::<Vec<_>>()
        .join("; "))
}

/// Every socket on `addr`'s port for `transport`, with its holder (see [`port_holder`]).
pub(crate) fn port_holders(addr: SocketAddr, transport: SlotTransport) -> Result<Vec<PortHolder>, String> {
    let tables: [&Path; 2] = match transport {
        SlotTransport::Tcp => [Path::new("/proc/net/tcp"), Path::new("/proc/net/tcp6")],
        SlotTransport::Udp => [Path::new("/proc/net/udp"), Path::new("/proc/net/udp6")],
    };
    port_holders_in(&tables, Path::new("/proc"), addr)
}

/// Every socket on `addr`'s port listed in `tables`, with the process under `proc_root` whose
/// descriptor table holds it. A table the kernel does not provide (`tcp6`/`udp6` on a host
/// without IPv6) is skipped; a table that exists and cannot be read is an `Err` naming it. In
/// the descriptor walk, a process that exited mid-walk and a process whose descriptors this
/// node-admin may not read (another user's: never a runtime this node-admin started, which runs
/// as its own user) name no holder; any other failure to read a process directory, descriptor
/// table or descriptor is an `Err` naming it.
pub(crate) fn port_holders_in(tables: &[&Path], proc_root: &Path, addr: SocketAddr) -> Result<Vec<PortHolder>, String> {
    use std::io::ErrorKind::{NotFound, PermissionDenied};
    let named = |p: &Path, e: std::io::Error| format!("{}: {e}", p.display());
    let port = format!("{:04X}", addr.port());
    let mut sockets: Vec<(String, u64)> = Vec::new();
    for t in tables {
        let text = match std::fs::read_to_string(t) {
            Ok(text) => text,
            Err(e) if e.kind() == NotFound => continue,
            Err(e) => return Err(format!("socket table {}", named(t, e))),
        };
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
        return Ok(Vec::new());
    }
    let mut owners: BTreeMap<u64, (u32, String)> = BTreeMap::new();
    let procs = std::fs::read_dir(proc_root).map_err(|e| format!("process directory {}", named(proc_root, e)))?;
    for p in procs {
        let p = p.map_err(|e| format!("process directory {}", named(proc_root, e)))?;
        let Some(pid) = p.file_name().to_str().and_then(|n| n.parse::<u32>().ok()) else { continue };
        let fd_dir = p.path().join("fd");
        let fds = match std::fs::read_dir(&fd_dir) {
            Ok(fds) => fds,
            Err(e) if matches!(e.kind(), NotFound | PermissionDenied) => continue,
            Err(e) => return Err(format!("descriptor table {}", named(&fd_dir, e))),
        };
        for fd in fds {
            let fd = match fd {
                Ok(fd) => fd,
                Err(e) if matches!(e.kind(), NotFound | PermissionDenied) => continue,
                Err(e) => return Err(format!("descriptor table {}", named(&fd_dir, e))),
            };
            let target = match std::fs::read_link(fd.path()) {
                Ok(t) => t,
                Err(e) if matches!(e.kind(), NotFound | PermissionDenied) => continue,
                Err(e) => return Err(format!("descriptor {}", named(&fd.path(), e))),
            };
            let t = target.to_string_lossy();
            let Some(inode) = t.strip_prefix("socket:[").and_then(|r| r.strip_suffix(']')).and_then(|n| n.parse::<u64>().ok()) else { continue };
            if !sockets.iter().any(|(_, i)| *i == inode) {
                continue;
            }
            let comm_file = p.path().join("comm");
            let comm = match std::fs::read_to_string(&comm_file) {
                Ok(c) => c.trim().to_string(),
                // Exited after its descriptor was read: it holds nothing now.
                Err(e) if e.kind() == NotFound => continue,
                Err(e) => return Err(format!("process name {}", named(&comm_file, e))),
            };
            owners.entry(inode).or_insert((pid, comm));
        }
    }
    Ok(sockets
        .into_iter()
        .map(|(state, inode)| {
            let (pid, comm) = owners.get(&inode).cloned().map(|(p, c)| (Some(p), c)).unwrap_or((None, String::new()));
            PortHolder { state, inode, pid, comm }
        })
        .collect())
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
        let who = port_holder(addr, SlotTransport::Tcp).unwrap();
        assert!(who.contains("listen"), "{who}");
        assert!(who.contains(&format!("pid {} ", std::process::id())), "{who}");
        drop(l);
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        assert_eq!(port_holder(free, SlotTransport::Tcp).unwrap(), "no socket");
    }

    /// CONTRACT: a port another live process listens on is released by the exited runtime (its
    /// pid is not among the holders); a port this very process still listens on is not released
    /// when this process is the one said to have exited; a free port is released.
    #[test]
    fn a_port_another_live_process_listens_on_is_released_by_the_exited_runtime() {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = l.local_addr().unwrap();
        assert!(released_by(addr, SlotTransport::Tcp, Some(1)).unwrap(), "another live pid listens: released");
        assert!(!released_by(addr, SlotTransport::Tcp, Some(std::process::id())).unwrap(), "the exited pid itself still listens: not released");
        drop(l);
        let free = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        assert!(released_by(free, SlotTransport::Tcp, Some(1)).unwrap());
    }

    /// CONTRACT: a socket table that exists but cannot be read is an error naming the table and
    /// why, never "no holder": a port must not read as released when its table was never read.
    #[test]
    fn an_unreadable_socket_table_is_an_error_naming_it() {
        let dir = crate::test_support::tempdir("port-table-unreadable");
        // A directory where the table is: it exists, and reading it fails (EISDIR).
        let table = dir.join("tcp");
        std::fs::create_dir_all(&table).unwrap();
        let addr: SocketAddr = "127.0.0.1:4242".parse().unwrap();
        let err = port_holders_in(&[table.as_path()], std::path::Path::new("/proc"), addr).unwrap_err();
        assert!(err.contains(&table.display().to_string()), "the error names the table: {err}");
    }

    /// CONTRACT: a table the kernel does not provide (`/proc/net/tcp6` on a host without IPv6)
    /// is skipped; the tables that exist still answer their sockets.
    #[test]
    fn a_missing_tcp6_table_is_skipped_and_the_tcp_table_answers() {
        let dir = crate::test_support::tempdir("port-table-missing-tcp6");
        let tcp = dir.join("tcp");
        let tcp6 = dir.join("tcp6");
        std::fs::write(
            &tcp,
            "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n   0: 0100007F:1092 00000000:0000 0A 00000000:00000000 00:00000000 00000000  1000        0 987654321 1 0000000000000000 100 0 0 10 0\n",
        )
        .unwrap();
        let addr: SocketAddr = "127.0.0.1:4242".parse().unwrap();
        let holders = port_holders_in(&[tcp.as_path(), tcp6.as_path()], std::path::Path::new("/proc"), addr).unwrap();
        assert_eq!(holders, vec![PortHolder { state: "listen".into(), inode: 987654321, pid: None, comm: String::new() }]);
    }
}
