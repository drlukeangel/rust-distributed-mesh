//! i143.e2.s2 functional: node-admin assigns advertised addresses first, and a
//! provider cannot invent an advertised port.
//!
//! The "runtime" here binds real sockets the way a deployed node would: one UDP
//! transport, plus a TCP listener for a kind that declares one. The WaitForBind
//! check asks the OS whether each assigned socket is actually held.

use rafka_node_admin_core::deployment::endpoint::{verify_bound, BindRefusal, EndpointAllocator, NODE_ADMIN, RPC_NODE};
use std::net::{IpAddr, TcpListener, UdpSocket};

/// Each test gets its own range: the tests run in parallel and bind real ports.
fn allocator(first: u16) -> EndpointAllocator {
    EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), first, first + 99)
}

#[test]
fn a_provider_honouring_the_assignment_passes_wait_for_bind() {
    let mut a = allocator(57000);
    let assigned = a.assign(&"mesh1.rpc.1".parse().unwrap(), &RPC_NODE, false).unwrap();
    let held = UdpSocket::bind(assigned.transport).unwrap();
    assert_eq!(verify_bound(&assigned), Ok(()));
    drop(held);
}

#[test]
fn a_provider_that_binds_a_different_port_is_refused() {
    let mut a = allocator(57200);
    let assigned = a.assign(&"mesh1.rpc.2".parse().unwrap(), &RPC_NODE, false).unwrap();
    // The runtime ignores the assignment and binds an OS-chosen port instead.
    let _elsewhere = UdpSocket::bind("127.0.0.1:0").unwrap();
    assert_eq!(verify_bound(&assigned), Err(BindRefusal::NotBoundAtAssigned { socket: "transport".into(), assigned: assigned.transport }));
}

#[test]
fn a_listener_kind_must_hold_its_tcp_address_as_well_as_the_transport() {
    let mut a = allocator(57400);
    let assigned = a.assign(&"mesh1.admin.1".parse().unwrap(), &NODE_ADMIN, false).unwrap();
    let control = assigned.listeners.iter().find(|(n, _)| n == "control").map(|(_, a)| *a).unwrap();
    let _udp = UdpSocket::bind(assigned.transport).unwrap();
    // The transport is held but the control listener is not.
    assert_eq!(verify_bound(&assigned), Err(BindRefusal::NotBoundAtAssigned { socket: "control".into(), assigned: control }));
    let _tcp = TcpListener::bind(control).unwrap();
    assert_eq!(verify_bound(&assigned), Ok(()));
}
