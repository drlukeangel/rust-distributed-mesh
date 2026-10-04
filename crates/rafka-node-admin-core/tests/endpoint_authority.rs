//! i143.e2.s2 functional: node-admin assigns advertised endpoints first, and a
//! provider cannot invent an advertised port.
//!
//! The "runtime" here binds real UDP sockets the way a deployed node would; the
//! WaitForBind check asks the OS whether the assigned port is actually held.

use rafka_node_admin_core::deployment::endpoint::{verify_bound, BindRefusal, EndpointAllocator, RPC_NODE_SLOTS};
use std::net::{IpAddr, UdpSocket};

/// Each test gets its own range: the tests run in parallel and bind real ports.
fn allocator(first: u16) -> EndpointAllocator {
    EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), first, first + 99)
}

#[test]
fn a_provider_honouring_the_assignment_passes_wait_for_bind() {
    let mut a = allocator(57000);
    let slots = a.assign(&"mesh1.rpc.1".parse().unwrap(), RPC_NODE_SLOTS, false).unwrap();
    let held: Vec<UdpSocket> = slots.iter().map(|s| UdpSocket::bind(s.addr).unwrap()).collect();
    let reported: Vec<(String, std::net::SocketAddr)> = slots.iter().map(|s| (s.slot.clone(), s.addr)).collect();
    assert_eq!(verify_bound(&slots, &reported), Ok(()));
    drop(held);
}

#[test]
fn a_provider_that_binds_a_different_port_is_refused() {
    let mut a = allocator(57200);
    let slots = a.assign(&"mesh1.rpc.2".parse().unwrap(), RPC_NODE_SLOTS, false).unwrap();
    // The runtime ignores the assignment and binds an OS-chosen port instead.
    let elsewhere = UdpSocket::bind("127.0.0.1:0").unwrap();
    let invented = elsewhere.local_addr().unwrap();
    let reported = vec![(slots[0].slot.clone(), invented), (slots[1].slot.clone(), slots[1].addr)];
    let _held1 = UdpSocket::bind(slots[1].addr).unwrap();
    assert_eq!(
        verify_bound(&slots, &reported),
        Err(BindRefusal::ReportedElsewhere { slot: "rpc-0".into(), assigned: slots[0].addr, reported: invented })
    );
    // Even when the runtime does not report at all, the unheld assigned port is caught.
    assert_eq!(verify_bound(&slots, &[]), Err(BindRefusal::NotBoundAtAssigned { slot: "rpc-0".into(), assigned: slots[0].addr }));
}

#[test]
fn a_runtime_that_reports_the_assignment_but_never_binds_it_is_refused() {
    let mut a = allocator(57400);
    let slots = a.assign(&"mesh1.rpc.3".parse().unwrap(), RPC_NODE_SLOTS, false).unwrap();
    let reported: Vec<_> = slots.iter().map(|s| (s.slot.clone(), s.addr)).collect();
    assert_eq!(verify_bound(&slots, &reported), Err(BindRefusal::NotBoundAtAssigned { slot: "rpc-0".into(), assigned: slots[0].addr }));
}
