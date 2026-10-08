//! A foreign process that holds ports (i143.e8.s5, #2783): the collision soak's squatter.
//!
//! `rafka-port-squatter --tcp 22001,22002 --udp 22001,22005` binds each named port on 0.0.0.0
//! (TCP listener, UDP socket) and prints one JSON line `{"pid":..,"held":[{"proto","port"}],
//! "refused":[{"proto","port","error","os_error"}]}`: a refused bind is the OS answering that a
//! live socket (an estate runtime, another squatter) already holds the port. It then holds
//! everything until its stdin closes or it is signalled. It knows nothing of the estate.

use std::io::Read;
use std::net::{TcpListener, UdpSocket};

fn ports(args: &[String], flag: &str) -> Vec<u16> {
    args.iter().position(|a| a == flag).and_then(|i| args.get(i + 1)).map(|v| v.split(',').filter(|p| !p.is_empty()).filter_map(|p| p.parse().ok()).collect()).unwrap_or_default()
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let (mut tcp, mut udp) = (Vec::new(), Vec::new());
    let (mut held, mut refused) = (Vec::new(), Vec::new());
    for p in ports(&args, "--tcp") {
        match TcpListener::bind(("0.0.0.0", p)) {
            Ok(l) => {
                held.push(format!(r#"{{"proto":"tcp","port":{p}}}"#));
                tcp.push(l);
            }
            Err(e) => refused.push(format!(r#"{{"proto":"tcp","port":{p},"error":"{e}","os_error":{}}}"#, e.raw_os_error().unwrap_or(0))),
        }
    }
    for p in ports(&args, "--udp") {
        match UdpSocket::bind(("0.0.0.0", p)) {
            Ok(s) => {
                held.push(format!(r#"{{"proto":"udp","port":{p}}}"#));
                udp.push(s);
            }
            Err(e) => refused.push(format!(r#"{{"proto":"udp","port":{p},"error":"{e}","os_error":{}}}"#, e.raw_os_error().unwrap_or(0))),
        }
    }
    println!(r#"{{"pid":{},"held":[{}],"refused":[{}]}}"#, std::process::id(), held.join(","), refused.join(","));
    let mut sink = Vec::new();
    let _ = std::io::stdin().read_to_end(&mut sink);
    drop((tcp, udp));
}
