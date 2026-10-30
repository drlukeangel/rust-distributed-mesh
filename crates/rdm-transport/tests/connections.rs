use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::connections::{
    ConnectionKind as CoreConnectionKind, ConnectionState, NodeConnection,
};
use rdm_core::reconnect::owed_direct_restored;
use rdm_core::status::ConnectionScope;
use rdm_core::{ConnectionId, IncarnationId, NodeId, NodeKind};
use rdm_transport::connections::{
    Acceptor, Admitter, Alpn, Birth, Closed, ConnectError, ConnectionKind, Connections,
    ConnectionsRegistry, ConnectionsSink, Dialer, MemoryConnectionsSink, StreamError,
    validate_alpns,
};
use rdm_transport::{ResolvedNode, StaticResolver};

const ALPN_A: &[u8] = b"test-data-a/1";
const ALPN_B: &[u8] = b"test-data-b/1";
const ALPN_UNREGISTERED: &[u8] = b"test-unregistered/1";

fn alpn(name: &[u8]) -> Alpn {
    Alpn {
        name: name.to_vec(),
        proxy: true,
        carriers: vec![NodeKind::RpcNode],
    }
}

struct TestPeer {
    birth: Birth,
    resolved: ResolvedNode,
    resolver: Arc<StaticResolver>,
    sink: Arc<MemoryConnectionsSink>,
    registry: ConnectionsRegistry,
    router: Router,
}

impl TestPeer {
    async fn bind(name: &str, alpns: &[Alpn]) -> Self {
        let key = SecretKey::generate();
        let raw_alpns: Vec<Vec<u8>> = alpns.iter().map(|a| a.name.clone()).collect();
        let ep = rdm_transport::endpoint::bind_with_alpns(
            key.clone(),
            "127.0.0.1:0".parse().unwrap(),
            &raw_alpns,
        )
        .await
        .unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let birth = Birth {
            name: name.parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
        };
        let resolved = ResolvedNode {
            node_id: birth.node_id.clone(),
            name: birth.name.clone(),
            endpoint_id: key.public(),
            transport_addr: addr,
            incarnation: birth.incarnation.clone(),
        };
        let resolver = Arc::new(StaticResolver::new());
        let sink = Arc::new(MemoryConnectionsSink::new(birth.clone()));
        let (registry, handlers) = ConnectionsRegistry::new(
            ep.clone(),
            resolver.clone(),
            sink.clone(),
            alpns,
            b"cert-v1".to_vec(),
        )
        .unwrap();
        let mut rb = Router::builder(ep);
        for (alpn_bytes, handler) in handlers {
            rb = rb.accept(alpn_bytes, handler);
        }
        let router = rb.spawn();
        Self {
            birth,
            resolved,
            resolver,
            sink,
            registry,
            router,
        }
    }

    fn know(&self, other: &TestPeer) {
        self.resolver.insert(other.resolved.clone());
    }

    async fn shutdown(self) {
        let _ = self.router.shutdown().await;
    }
}

#[tokio::test]
async fn an_unregistered_alpn_is_refused_by_name_on_connections() {
    let peer = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;

    let err = peer
        .registry
        .connections(ALPN_UNREGISTERED)
        .expect_err("unregistered ALPN must be refused");
    assert_eq!(
        err,
        ConnectError::UnregisteredAlpn {
            alpn: ALPN_UNREGISTERED.to_vec()
        }
    );
    assert!(
        err.to_string().contains("test-unregistered/1"),
        "error message must name the unregistered ALPN: {err}"
    );

    // Also verify validate_alpns refuses RDM's own control-plane ALPNs by name:
    let own_rpc_err = validate_alpns(&[alpn(rdm_transport::ALPN)]).unwrap_err();
    assert!(own_rpc_err.contains("Node RPC ALPN"), "{own_rpc_err}");

    peer.shutdown().await;
}

#[tokio::test]
async fn a_second_connections_call_for_one_alpn_is_already_taken_by_name() {
    let peer = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;

    let _first = peer
        .registry
        .connections(ALPN_A)
        .expect("first connections(ALPN_A) call succeeds");

    let err = peer
        .registry
        .connections(ALPN_A)
        .expect_err("second connections(ALPN_A) call must fail with AlreadyTaken");
    assert_eq!(
        err,
        ConnectError::AlreadyTaken {
            alpn: ALPN_A.to_vec()
        }
    );
    assert!(
        err.to_string().contains("test-data-a/1"),
        "error message must name the already-taken ALPN: {err}"
    );

    peer.shutdown().await;
}

#[tokio::test]
async fn a_direct_open_writes_connected_with_its_alpn_and_the_acceptor_sees_the_same_row_id() {
    let peer_a = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let peer_b = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    peer_a.know(&peer_b);
    peer_b.know(&peer_a);

    let (dialer_a, _acceptor_a) = peer_a.registry.connections(ALPN_A).unwrap().split();
    let (_dialer_b, mut acceptor_b) = peer_b.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_b.accept().await.unwrap() });
    let conn_a = dialer_a.open(&peer_b.birth).await.unwrap();
    let conn_b = accept_task.await.unwrap();

    assert_eq!(conn_a.id(), conn_b.id(), "dialer and acceptor share row id");
    assert_eq!(conn_a.kind(), ConnectionKind::Direct);
    assert_eq!(conn_b.kind(), ConnectionKind::Direct);
    assert_eq!(conn_a.alpn(), ALPN_A);
    assert_eq!(conn_b.alpn(), ALPN_A);
    assert_eq!(conn_a.peer(), &peer_b.birth);
    assert_eq!(conn_b.peer(), &peer_a.birth);

    let (mut send_a, mut recv_a) = conn_a.open_bi().await.unwrap();
    send_a.write_all(b"ping").await.unwrap();
    send_a.finish().unwrap();

    let (mut send_b, mut recv_b) = conn_b.accept_bi().await.unwrap();
    let got = recv_b.read_to_end(64).await.unwrap();
    assert_eq!(got, b"ping");
    send_b.write_all(b"pong").await.unwrap();
    send_b.finish().unwrap();

    let reply = recv_a.read_to_end(64).await.unwrap();
    assert_eq!(reply, b"pong");

    let rows = peer_a.sink.history();
    let row = rows
        .iter()
        .find(|r| &r.id == conn_a.id() && r.alpn == ALPN_A)
        .expect("Direct Connected row with ALPN_A written to sink");
    assert_eq!(row.kind, CoreConnectionKind::Direct);
    assert_eq!(row.state, ConnectionState::Connected);
    assert_eq!(row.source, peer_a.birth.to_end());
    assert_eq!(row.destination, peer_b.birth.to_end());

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}

#[tokio::test]
async fn a_working_alpn_a_proves_nothing_for_alpn_b_and_a_reconnect_on_a_never_disconnects_bs_proxy() {
    let peer_a = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A), alpn(ALPN_B)]).await;
    // peer_b registers only ALPN_A, not ALPN_B:
    let peer_b = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    peer_a.know(&peer_b);
    peer_b.know(&peer_a);

    // Seed an active Proxy on ALPN_B and a Failed Direct on ALPN_A in peer_a's held projection:
    let carrier_birth = Birth {
        name: "mesh1.rpc.3".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
    };
    let proxy_on_b = NodeConnection {
        id: ConnectionId::mint(),
        source: peer_a.birth.to_end(),
        destination: peer_b.birth.to_end(),
        kind: CoreConnectionKind::Proxy,
        alpn: ALPN_B.to_vec(),
        state: ConnectionState::Connected,
        carrier: Some(carrier_birth.to_end()),
        recovery: None,
        reason: None,
        logged_at_ms: 10,
    };
    peer_a.sink.record(proxy_on_b.clone());

    let (dialer_a_on_a, _) = peer_a.registry.connections(ALPN_A).unwrap().split();
    let (_dialer_b, mut acceptor_b_on_a) = peer_b.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_b_on_a.accept().await.unwrap() });
    let _conn_a = dialer_a_on_a.open(&peer_b.birth).await.unwrap();
    let _conn_b = accept_task.await.unwrap();

    // Direct Connected on ALPN_A must NOT delete the Proxy on ALPN_B:
    {
        let held_guard = peer_a.sink.held();
        let held = held_guard.lock().unwrap();
        assert!(
            held.active_direct_for(&peer_a.birth.name, &peer_b.birth.name, ALPN_A)
                .unwrap()
                .is_some(),
            "ALPN_A has an active Direct"
        );
        assert!(
            held.active_direct_for(&peer_a.birth.name, &peer_b.birth.name, ALPN_B)
                .unwrap()
                .is_none(),
            "working ALPN_A proves nothing for ALPN_B"
        );
        assert!(
            held.own_proxy_for(&peer_b.birth.name, ALPN_B)
                .unwrap()
                .is_some(),
            "ALPN_B's Proxy must remain active when ALPN_A reconnects Direct"
        );
        assert!(
            owed_direct_restored(&held, 999).is_empty(),
            "owed_direct_restored must not delete ALPN_B's Proxy when only ALPN_A has Direct Connected"
        );
    }

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}

#[tokio::test]
async fn an_open_against_another_incarnation_is_wrong_incarnation_and_never_served() {
    let peer_a = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let peer_b = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    peer_a.know(&peer_b);
    peer_b.know(&peer_a);

    let (dialer_a, _) = peer_a.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_b) = peer_b.registry.connections(ALPN_A).unwrap().split();

    let stale_incarnation = IncarnationId::mint();
    assert_ne!(stale_incarnation, peer_b.birth.incarnation);
    let stale_target = Birth {
        name: peer_b.birth.name.clone(),
        node_id: peer_b.birth.node_id.clone(),
        incarnation: stale_incarnation.clone(),
    };

    let err = dialer_a
        .open(&stale_target)
        .await
        .expect_err("open against wrong incarnation must fail");
    assert_eq!(
        err,
        ConnectError::WrongIncarnation {
            held: stale_incarnation,
            found: peer_b.birth.incarnation.clone(),
        }
    );

    // And acceptor_b must never yield the rejected connection:
    let accept_res = tokio::time::timeout(Duration::from_millis(100), acceptor_b.accept()).await;
    assert!(
        accept_res.is_err(),
        "acceptor must never yield a connection whose target incarnation mismatched"
    );

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}

#[tokio::test]
async fn finish_reset_and_stop_codes_arrive_typed_on_the_far_end() {
    let peer_a = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let peer_b = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    peer_a.know(&peer_b);
    peer_b.know(&peer_a);

    let (dialer_a, _) = peer_a.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_b) = peer_b.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_b.accept().await.unwrap() });
    let conn_a = dialer_a.open(&peer_b.birth).await.unwrap();
    let conn_b = accept_task.await.unwrap();

    // 1. finish() -> clean EOF on far end; stop(42) -> stopped() returns StreamError::Stopped { code: 42 }
    let (mut send_a1, mut recv_a1) = conn_a.open_bi().await.unwrap();
    send_a1.write_all(b"hello").await.unwrap();
    send_a1.finish().unwrap();

    let (mut send_b1, mut recv_b1) = conn_b.accept_bi().await.unwrap();
    let got = recv_b1.read_to_end(64).await.unwrap();
    assert_eq!(got, b"hello");

    recv_a1.stop(42).unwrap();
    let stop_err = send_b1
        .stopped()
        .await
        .expect_err("expected StreamError::Stopped from stopped()");
    assert_eq!(stop_err, StreamError::Stopped { code: 42 });

    // 2. reset(77) on send stream -> far end recv stream sees StreamError::Reset { code: 77 }
    let (mut send_a2, _recv_a2) = conn_a.open_bi().await.unwrap();
    send_a2.write_all(b"partial").await.unwrap();

    let (_send_b2, mut recv_b2) = conn_b.accept_bi().await.unwrap();
    let mut buf = [0u8; 7];
    recv_b2.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"partial");

    send_a2.reset(77).unwrap();
    let reset_err = recv_b2
        .read_to_end(64)
        .await
        .expect_err("expected StreamError::Reset on recv_b2");
    assert_eq!(reset_err, StreamError::Reset { code: 77 });

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}

struct PolicyAdmitter {
    allow: AtomicBool,
    calls: AtomicUsize,
}

impl Admitter for PolicyAdmitter {
    fn verify(
        &self,
        _caller: &Birth,
        _alpn: &[u8],
        _cert: &[u8],
    ) -> Result<(u64, &'static str), String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.allow.load(Ordering::SeqCst) {
            Ok((12345, "rafka-time"))
        } else {
            Err("expired member cert".to_string())
        }
    }
}

#[tokio::test]
async fn an_inbound_connection_on_a_registered_alpn_passes_admit_before_accept() {
    let peer_a = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let peer_b = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    peer_a.know(&peer_b);
    peer_b.know(&peer_a);

    let admitter = Arc::new(PolicyAdmitter {
        allow: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    peer_b.registry.set_admitter(admitter.clone());

    let (dialer_a, _) = peer_a.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_b) = peer_b.registry.connections(ALPN_A).unwrap().split();

    // First attempt: admitter rejects -> open returns ConnectError::Refused, acceptor_b never sees it,
    // and peer_b records a Refused row on its sink.
    let err = dialer_a
        .open(&peer_b.birth)
        .await
        .expect_err("admitter refusal must refuse dialer open");
    assert_eq!(
        err,
        ConnectError::Refused {
            reason: "expired member cert".to_string()
        }
    );
    assert_eq!(admitter.calls.load(Ordering::SeqCst), 1);

    let accept_timeout =
        tokio::time::timeout(Duration::from_millis(100), acceptor_b.accept()).await;
    assert!(
        accept_timeout.is_err(),
        "refused connection must not reach accept()"
    );

    let b_rows = peer_b.sink.history();
    let refused_row = b_rows
        .iter()
        .find(|r| r.state == ConnectionState::Refused && r.alpn == ALPN_A)
        .expect("acceptor must record a Refused row when admit rejects");
    assert_eq!(
        refused_row.reason.as_deref(),
        Some("expired member cert")
    );

    // Flip admitter to allow -> the dialer's reconnect series' next attempt (the refusal left its
    // Direct Failed row, connections.md §9.1) is admitted and reaches accept().
    admitter.allow.store(true, Ordering::SeqCst);
    let accept_task = tokio::spawn(async move { acceptor_b.accept().await.unwrap() });
    let conn_a = peer_a.registry.probe_direct(&peer_b.birth, ALPN_A).await.unwrap();
    let conn_b = accept_task.await.unwrap();
    assert_eq!(conn_a.id(), conn_b.id());

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}

/// CONTRACT (node-connections-admit.md step 5, connections.md §9.1, §9.3; decided 2026-10-19): a
/// refusal is the acceptor's `Refused` fact and, on the dialer, a Direct `Failed` row with the named
/// reason, so the dialer's reconnect series exists and continues: a new call dials nothing over the
/// standing `Failed` row (`NoActiveRoute`), each refused attempt of the series is the next ordinal,
/// and the attempt that is admitted is Direct `Connected`.
#[tokio::test]
async fn a_refusal_is_a_refused_fact_on_the_acceptor_and_a_failed_row_on_the_dialer() {
    let dialer = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let acceptor = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    dialer.know(&acceptor);
    acceptor.know(&dialer);
    let admitter = Arc::new(PolicyAdmitter { allow: AtomicBool::new(false), calls: AtomicUsize::new(0) });
    acceptor.registry.set_admitter(admitter.clone());
    let (opener, _) = dialer.registry.connections(ALPN_A).unwrap().split();
    let (_, mut accepts) = acceptor.registry.connections(ALPN_A).unwrap().split();

    let refused = ConnectError::Refused { reason: "expired member cert".into() };
    assert_eq!(opener.open(&acceptor.birth).await.unwrap_err(), refused);
    let facts: Vec<_> = acceptor.sink.history().into_iter().filter(|r| r.state == ConnectionState::Refused).collect();
    assert_eq!(facts.len(), 1, "{facts:#?}");
    assert_eq!((facts[0].source.name.clone(), facts[0].reason.as_deref()), (dialer.birth.name.clone(), Some("expired member cert")));

    let direct_rows = |peer: &TestPeer| -> Vec<NodeConnection> {
        peer.sink.history().into_iter().filter(|r| r.kind == CoreConnectionKind::Direct && r.destination.name == acceptor.birth.name && r.alpn == ALPN_A).collect()
    };
    let rows = direct_rows(&dialer);
    assert_eq!(rows.len(), 1, "the dialer writes one Direct row for the refused dial: {rows:#?}");
    assert_eq!(rows[0].state, ConnectionState::Failed);
    assert_eq!(rows[0].reason.as_deref(), Some("refused: expired member cert"), "the acceptor's named reason");
    assert_eq!(rows[0].recovery.map(|r| r.attempt_ordinal), Some(1));
    assert_eq!(rows[0].destination.incarnation.as_ref(), Some(&acceptor.birth.incarnation));

    assert_eq!(opener.open(&acceptor.birth).await.unwrap_err(), ConnectError::NoActiveRoute { direct: None }, "the series owns the retry; a new call dials nothing over it");
    assert_eq!(admitter.calls.load(Ordering::SeqCst), 1);

    assert_eq!(dialer.registry.probe_direct(&acceptor.birth, ALPN_A).await.unwrap_err(), refused, "the series' next attempt is refused again");
    let rows = direct_rows(&dialer);
    assert_eq!(rows.iter().map(|r| (r.state, r.recovery.map(|x| x.attempt_ordinal))).collect::<Vec<_>>(), vec![(ConnectionState::Failed, Some(1)), (ConnectionState::Failed, Some(2))]);
    assert_eq!(rows[0].recovery.map(|r| r.recovery_round), rows[1].recovery.map(|r| r.recovery_round), "one recovery round");

    admitter.allow.store(true, Ordering::SeqCst);
    let accepted = tokio::spawn(async move { accepts.accept().await.unwrap() });
    let conn = dialer.registry.probe_direct(&acceptor.birth, ALPN_A).await.unwrap();
    assert_eq!(accepted.await.unwrap().id(), conn.id());
    assert_eq!(direct_rows(&dialer).last().map(|r| r.state), Some(ConnectionState::Connected), "the admitted attempt is Direct Connected");
    dialer.shutdown().await;
    acceptor.shutdown().await;
}

/// One process endpoint serving Node RPC, gossip and a registered product ALPN, as a node does.
struct Process {
    peer: TestPeer,
    ep: iroh::Endpoint,
    gossip: iroh_gossip::net::Gossip,
    _server: rdm_transport::NodeRpcServer,
}

async fn process(name: &str) -> Process {
    use rdm_core::ping::{Ping, PingReply, PingRequest};
    let key = SecretKey::generate();
    let ep = rdm_transport::endpoint::bind_with_alpns(key.clone(), "127.0.0.1:0".parse().unwrap(), &[ALPN_A.to_vec()]).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let birth = Birth { name: name.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
    let resolved = ResolvedNode { node_id: birth.node_id.clone(), name: birth.name.clone(), endpoint_id: key.public(), transport_addr: addr, incarnation: birth.incarnation.clone() };
    let resolver = Arc::new(StaticResolver::new());
    let sink = Arc::new(MemoryConnectionsSink::new(birth.clone()));
    let (registry, handlers) = ConnectionsRegistry::new(ep.clone(), resolver.clone(), sink.clone(), &[alpn(ALPN_A)], b"cert-v1".to_vec()).unwrap();
    let server = rdm_transport::ServerBuilder::new()
        .serve::<Ping, _, _>(rdm_core::catalog::OpOwner::Core, |_peer, req: PingRequest| async move {
            let PingRequest::Ping { payload, .. } = req;
            Ok(PingReply::Pong { payload })
        })
        .build(rdm_transport::ServedBirth { node_id: birth.node_id.to_string(), incarnation: birth.incarnation.to_string() })
        .unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(ep.clone());
    let mut rb = Router::builder(ep.clone()).accept(rdm_transport::ALPN, server.clone()).accept(iroh_gossip::ALPN, gossip.clone());
    for (alpn_bytes, handler) in handlers {
        rb = rb.accept(alpn_bytes, handler);
    }
    let router = rb.spawn();
    Process { peer: TestPeer { birth, resolved, resolver, sink, registry, router }, ep, gossip, _server: server }
}

/// CONTRACT (node-connections-admit.md, Observability, the control; fabric-certs.md §On a
/// connection): the step runs on no Node RPC and no gossip connection. A Node RPC call and a gossip
/// connection to a process whose endpoint also serves a registered product ALPN carry no admission
/// span, while the one product dial on the same endpoint carries exactly one.
#[test]
fn a_node_rpc_and_a_gossip_connection_carry_no_admission_span() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-admit-control").expect("the cell's evidence file can be created");
    let (acceptor_birth, rpc_served, heard) = capture.run(4, async {
        let acceptor = process("mesh1.rpc.2").await;
        let dialer = process("mesh1.rpc.1").await;
        dialer.peer.know(&acceptor.peer);
        acceptor.peer.know(&dialer.peer);

        // Node RPC: one call from the dialer's endpoint to the acceptor's.
        let rpc_resolver = Arc::new(StaticResolver::new());
        rpc_resolver.insert(acceptor.peer.resolved.clone());
        let client = rdm_transport::NodeRpcClient::new(dialer.ep.clone(), rpc_resolver);
        let ping = rdm_core::ping::PingRequest::Ping { payload: b"control".to_vec() };
        let (out, _) = client
            .call::<rdm_core::ping::Ping>(&rdm_transport::NodeTarget::ExactNode(acceptor.peer.birth.node_id.clone()), &ping, &rdm_transport::CallOptions::default())
            .await;
        let rpc_served = matches!(out, rdm_core::outcome::RpcOutcome::Reply(_));

        // Gossip: the dialer joins the acceptor's mesh channel through it and is heard there.
        let fabric = rdm_core::FabricId::parse("fab000000001").unwrap();
        let mesh_id = rdm_core::MeshId::mint();
        let clock = rdm_transport::clock::os_clock;
        let on_acceptor = rdm_transport::topology::TopologyView::join(&acceptor.gossip, &acceptor.ep, &fabric, "mesh1", &mesh_id, "mesh1.rpc.2", clock(), vec![], &[]).await.unwrap();
        let seed = iroh::EndpointAddr::new(acceptor.ep.id()).with_ip_addr(acceptor.peer.resolved.transport_addr);
        let on_dialer = rdm_transport::topology::TopologyView::join(&dialer.gossip, &dialer.ep, &fabric, "mesh1", &mesh_id, "mesh1.rpc.1", clock(), vec![seed], &[]).await.unwrap();
        let own = rdm_core::MeshDigest {
            held_hash: Default::default(),
            fabric_id: fabric.clone(),
            node: rdm_core::MeshNode {
                node_id: dialer.peer.birth.node_id.clone(),
                name: dialer.peer.birth.name.clone(),
                endpoint_id: rdm_core::EndpointId(dialer.ep.id().to_string()),
                transport_addr: dialer.peer.resolved.transport_addr,
                incarnation: dialer.peer.birth.incarnation.clone(),
                supersedes: None,
                runtime: None,
            },
            status: rdm_core::MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        };
        let publishing = on_dialer.publish_every(Duration::from_millis(100), move || own.clone());
        let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
        let mut heard = false;
        while tokio::time::Instant::now() < deadline && !heard {
            heard = on_acceptor.digest_view.current(on_acceptor.digest_view.staleness_floor()).iter().any(|d| d.node.node_id == dialer.peer.birth.node_id);
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        publishing.abort();

        // The product ALPN on the same endpoint: one dial, admitted.
        let (opener, _) = dialer.peer.registry.connections(ALPN_A).unwrap().split();
        let (_, mut accepts) = acceptor.peer.registry.connections(ALPN_A).unwrap().split();
        let accepted = tokio::spawn(async move { accepts.accept().await.unwrap() });
        let conn = opener.open(&acceptor.peer.birth).await.unwrap();
        assert_eq!(accepted.await.unwrap().id(), conn.id());
        let name = acceptor.peer.birth.name.to_string();
        drop((on_acceptor, on_dialer));
        dialer.peer.shutdown().await;
        acceptor.peer.shutdown().await;
        (name, rpc_served, heard)
    });
    assert!(rpc_served, "the Node RPC call was served on the acceptor's endpoint");
    assert!(heard, "the gossip connection formed: the acceptor heard the dialer's digest");
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let admission: Vec<&serde_json::Value> = spans
        .iter()
        .filter(|s| matches!(s["name"].as_str(), Some("rdm.node.connection.serve.via-admitted" | "rdm.node.connection.serve.via-no-certs-configured" | "rdm.node.connection.reject.via-refused" | "rdm.node.connection.create.via-dial")))
        .collect();
    let product = String::from_utf8_lossy(ALPN_A).into_owned();
    assert!(
        admission.iter().all(|s| s["attributes"]["alpn"] == product.as_str()),
        "no admission span names Node RPC's or gossip's ALPN: {admission:#?}"
    );
    // The acceptor holds no certs: its one admission is by name (`NoCerts`).
    let admitted: Vec<_> = admission.iter().filter(|s| s["name"] == "rdm.node.connection.serve.via-no-certs-configured" && s["attributes"]["destination"] == acceptor_birth.as_str()).collect();
    assert_eq!(admitted.len(), 1, "exactly the one product dial was admitted: {admission:#?}");
}

/// A dialer that holds a raw QUIC connection on a registered ALPN and presents what the cell says.
struct RawDialer {
    ep: iroh::Endpoint,
    birth: Birth,
}

impl RawDialer {
    /// A raw endpoint; `known` registers its key with `acceptor` under a birth of its own.
    async fn bind(name: &str, acceptor: &TestPeer, known: bool) -> Self {
        let key = SecretKey::generate();
        let ep = rdm_transport::endpoint::bind_with_alpns(key.clone(), "127.0.0.1:0".parse().unwrap(), &[ALPN_A.to_vec()]).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let birth = Birth { name: name.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        if known {
            acceptor.resolver.insert(ResolvedNode {
                node_id: birth.node_id.clone(),
                name: birth.name.clone(),
                endpoint_id: key.public(),
                transport_addr: addr,
                incarnation: birth.incarnation.clone(),
            });
        }
        Self { ep, birth }
    }

    async fn connect(&self, to: &TestPeer) -> iroh::endpoint::Connection {
        let addr = iroh::EndpointAddr::new(to.resolved.endpoint_id).with_ip_addr(to.resolved.transport_addr);
        self.ep.connect(addr, ALPN_A).await.unwrap()
    }
}

/// How the acceptor closed `conn`: its application code and reason, and when, from `from`.
async fn closed_by_acceptor(conn: &iroh::endpoint::Connection, from: tokio::time::Instant) -> (u64, String, Duration) {
    match conn.closed().await {
        iroh::endpoint::ConnectionError::ApplicationClosed(frame) => {
            (frame.error_code.into_inner(), String::from_utf8_lossy(&frame.reason).into_owned(), from.elapsed())
        }
        other => panic!("the acceptor closes with an application code, not {other:?}"),
    }
}

/// CONTRACT (node-connections-admit.md, Failure and receipts; decided 2026-10-19): a dialer that
/// presents nothing within one staleness floor of the accept is refused `no-credential`, the 412
/// close carrying the reason, and an empty or undecodable presentation is `no-credential` at once;
/// the `Refused` fact names the birth the acceptor's topology view names for the dialer's key, and a
/// key it cannot name gets the span and no row. A read the acceptor cannot complete (the dialer
/// gone) is neither verdict: nothing is written.
#[tokio::test]
async fn a_dial_that_presents_nothing_within_the_floor_is_no_credential() {
    let acceptor = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    let floor = rdm_core::cadence::staleness_floor();
    let silent = RawDialer::bind("mesh1.rpc.11", &acceptor, true).await;
    let stalled = RawDialer::bind("mesh1.rpc.12", &acceptor, true).await;
    let empty = RawDialer::bind("mesh1.rpc.13", &acceptor, true).await;
    let garbage = RawDialer::bind("mesh1.rpc.14", &acceptor, true).await;
    let stranger = RawDialer::bind("mesh1.rpc.15", &acceptor, false).await;
    let gone = RawDialer::bind("mesh1.rpc.16", &acceptor, true).await;

    let silent_case = async {
        let at = tokio::time::Instant::now();
        let conn = silent.connect(&acceptor).await;
        closed_by_acceptor(&conn, at).await
    };
    // A stream opened half a floor in, with one byte of a frame and nothing more: still one floor
    // from the accept, never a floor from the stream.
    let stalled_case = async {
        let at = tokio::time::Instant::now();
        let conn = stalled.connect(&acceptor).await;
        tokio::time::sleep(floor / 2).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.write_all(&[0u8]).await.unwrap();
        closed_by_acceptor(&conn, at).await
    };
    let empty_case = async {
        let at = tokio::time::Instant::now();
        let conn = empty.connect(&acceptor).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.finish().unwrap();
        closed_by_acceptor(&conn, at).await
    };
    let garbage_case = async {
        let at = tokio::time::Instant::now();
        let conn = garbage.connect(&acceptor).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.write_all(&[0, 0, 0, 3, 0xff, 0xff, 0xff]).await.unwrap();
        send.finish().unwrap();
        closed_by_acceptor(&conn, at).await
    };
    let stranger_case = async {
        let at = tokio::time::Instant::now();
        let conn = stranger.connect(&acceptor).await;
        closed_by_acceptor(&conn, at).await
    };
    let gone_case = async {
        let conn = gone.connect(&acceptor).await;
        let (mut send, _recv) = conn.open_bi().await.unwrap();
        send.write_all(&[0u8]).await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
        conn.close(0u32.into(), b"the dialer left");
    };
    let (s, st, e, g, x, ()) = tokio::join!(silent_case, stalled_case, empty_case, garbage_case, stranger_case, gone_case);

    for (case, (code, reason, _)) in [("silent", &s), ("stalled", &st), ("empty", &e), ("undecodable", &g), ("stranger", &x)] {
        assert_eq!((*code, reason.as_str()), (412, "no-credential"), "{case}: the 412 close carries the reason");
    }
    assert!(s.2 < floor + floor / 3, "silent: refused one floor from the accept, at {:?}", s.2);
    assert!(st.2 < floor + floor / 3, "stalled: one floor from the accept, never a floor from the stream: {:?}", st.2);
    assert!(e.2 < floor / 2 && g.2 < floor / 2, "an empty or undecodable presentation is refused at once: {:?} {:?}", e.2, g.2);

    tokio::time::sleep(floor + floor / 3).await;
    let refused: Vec<_> = acceptor.sink.history().into_iter().filter(|r| r.state == ConnectionState::Refused).collect();
    let mut named: Vec<(String, Option<String>)> = refused.iter().map(|r| (r.source.name.to_string(), r.reason.clone())).collect();
    named.sort();
    let no_credential = Some("no-credential".to_string());
    assert_eq!(
        named,
        vec![
            ("mesh1.rpc.11".to_string(), no_credential.clone()),
            ("mesh1.rpc.12".to_string(), no_credential.clone()),
            ("mesh1.rpc.13".to_string(), no_credential.clone()),
            ("mesh1.rpc.14".to_string(), no_credential.clone()),
        ],
        "one Refused fact per named dialer, none for the stranger and none for the dialer that left"
    );
    for r in &refused {
        assert_eq!(r.destination.name, acceptor.birth.name);
        assert_eq!(r.alpn, ALPN_A);
        assert_eq!(r.source.node_id, [&silent, &stalled, &empty, &garbage].iter().find(|d| d.birth.name == r.source.name).unwrap().birth.node_id);
    }
    acceptor.shutdown().await;
}

#[tokio::test]
async fn a_second_accept_owner_is_refused_by_the_type_system_and_split_is_once() {
    // Compile-time ownership invariants:
    // - `Connections` is `!Clone` and `Connections::split(self) -> (Dialer, Acceptor)` consumes `self` by value.
    // - `Acceptor` is `!Clone` and `Acceptor::accept(&mut self)` takes `&mut self`.
    // - `Dialer` is `Clone` + `Send` + `Sync` so callers can share the dial side after one `split()`.
    fn assert_clone<T: Clone>() {}
    fn assert_send<T: Send>() {}
    assert_clone::<Dialer>();
    assert_send::<Connections>();
    assert_send::<Acceptor>();

    let peer = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let conns = peer.registry.connections(ALPN_A).unwrap();
    let (dialer, _acceptor) = conns.split();
    let _dialer_clone = dialer.clone();

    // And trying to obtain a second Connections handle at runtime is refused with AlreadyTaken:
    assert_eq!(
        peer.registry.connections(ALPN_A).unwrap_err(),
        ConnectError::AlreadyTaken {
            alpn: ALPN_A.to_vec()
        }
    );

    peer.shutdown().await;
}

#[tokio::test]
async fn node_connections_delete_by_id_closes_the_stream_and_closed_names_it() {
    let peer_a = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let peer_b = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    peer_a.know(&peer_b);
    peer_b.know(&peer_a);

    let (dialer_a, _) = peer_a.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_b) = peer_b.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_b.accept().await.unwrap() });
    let conn_a = dialer_a.open(&peer_b.birth).await.unwrap();
    let conn_b = accept_task.await.unwrap();
    let row_id = conn_a.id().clone();

    let (mut send_a, _recv_a) = conn_a.open_bi().await.unwrap();
    send_a.write_all(b"hello").await.unwrap();

    let (_send_b, mut recv_b) = conn_b.accept_bi().await.unwrap();
    let mut buf = [0u8; 5];
    recv_b.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"hello");

    // Delete connection by id through `delete_connections`:
    let deleted = peer_a
        .registry
        .delete_connections(&ConnectionScope::Connections(vec![row_id.clone()]))
        .await;
    assert_eq!(deleted, vec![row_id]);

    // Both ends resolve closed() with Closed::ConnectionsDeleted, and reading from the open stream
    // yields StreamError::Closed(Closed::ConnectionsDeleted):
    let closed_a = tokio::time::timeout(Duration::from_secs(5), conn_a.closed())
        .await
        .expect("conn_a.closed() must resolve");
    assert_eq!(closed_a, Closed::ConnectionsDeleted);

    let closed_b = tokio::time::timeout(Duration::from_secs(5), conn_b.closed())
        .await
        .expect("conn_b.closed() must resolve");
    assert_eq!(closed_b, Closed::ConnectionsDeleted);

    let read_err = recv_b
        .read_to_end(64)
        .await
        .expect_err("recv_b must fail after connection is deleted");
    assert_eq!(read_err, StreamError::Closed(Closed::ConnectionsDeleted));

    peer_a.shutdown().await;
    peer_b.shutdown().await;
}

/// CONTRACT (connections.md §12; i148 Q61): a dial's spans parent to the caller's span, the stream's
/// bytes are not spanned, and a dial begun under another dispatcher (none, or a second registry) and
/// finished under the caller's never clones a foreign span id on the caller's registry. The cell builds
/// its own registries, so it runs in a process of its own, as RDM's other subscriber-sensitive cells
/// do: no other cell's subscriber shares its threads. In that process a panic prints its full stack,
/// so a registry clone of a foreign id names the site that cloned it. Must NOT happen:
/// `Registry::clone_span` panicking on an id its registry never issued (the e4s8 land gate at
/// 6c04336b: "tried to clone Id(11), but no span exists with that ID", in the whole binary).
#[tokio::test]
async fn a_dial_parents_to_the_callers_span_and_the_stream_bytes_are_not_spanned() {
    if crate::own_process::delegated(module_path!(), "a_dial_parents_to_the_callers_span_and_the_stream_bytes_are_not_spanned") {
        return;
    }
    // This process is the cell's own: any panic prints its stack, whatever RUST_BACKTRACE says.
    let report = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        eprintln!("{info}\nthe panicking thread's stack:\n{}", std::backtrace::Backtrace::force_capture());
        report(info);
    }));
    use tracing::instrument::WithSubscriber;
    use tracing::span::{Attributes, Id};
    use tracing::Subscriber;
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Registry;

    #[derive(Default, Clone)]
    struct RecordedSpans {
        spans: Arc<Mutex<Vec<(Id, String, Option<Id>)>>>,
    }

    impl<S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>>
        tracing_subscriber::Layer<S> for RecordedSpans
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
            let parent = if let Some(p) = attrs.parent() {
                Some(p.clone())
            } else if attrs.is_contextual() {
                ctx.current_span().id().cloned()
            } else {
                None
            };
            self.spans.lock().unwrap().push((
                id.clone(),
                attrs.metadata().name().to_string(),
                parent,
            ));
        }
    }

    let recorded = RecordedSpans::default();
    let subscriber = Registry::default().with(recorded.clone());
    let dispatch = tracing::Dispatch::new(subscriber);
    rdm_transport::enable_callsites();

    let (peer_a, peer_b) = async {
        let a = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
        let b = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
        a.know(&b);
        b.know(&a);
        (a, b)
    }
    .with_subscriber(dispatch.clone())
    .await;

    let (dialer_a, _) = peer_a.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_b) = peer_b.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(
        async move { acceptor_b.accept().await.unwrap() }.with_subscriber(dispatch.clone()),
    );
    let caller_span = tracing::dispatcher::with_default(&dispatch, || {
        rdm_transport::enable_callsites();
        tracing::info_span!("caller_operation")
    });
    let caller_id = caller_span.id().expect("caller span has id");

    let conn_a = tracing::Instrument::instrument(
        dialer_a.open(&peer_b.birth),
        caller_span.clone(),
    )
    .with_subscriber(dispatch.clone())
    .await
    .unwrap();
    let conn_b = accept_task.await.unwrap();

    let spans_after_open = recorded.spans.lock().unwrap().clone();
    assert!(
        spans_after_open.iter().any(|(_, name, _)| name == "rdm.node.connection.create.via-dial"),
        "rdm.node.connection.create.via-dial span not in {:?}",
        spans_after_open
    );
    let observed_span = spans_after_open
        .iter()
        .find(|(_, name, _)| name == "rdm.node_admin.connection.update.via-observed")
        .unwrap_or_else(|| {
            panic!(
                "rdm.node_admin.connection.update.via-observed span not in {:?}",
                spans_after_open
            )
        });
    assert_eq!(
        observed_span.2.as_ref(),
        Some(&caller_id),
        "via-observed span must parent to the caller's span"
    );

    let before_bytes_len = recorded.spans.lock().unwrap().len();
    let bytes_fut = async {
        let (mut send_a, mut recv_a) = conn_a.open_bi().await.unwrap();
        send_a.write_all(b"unspanned-payload-1").await.unwrap();
        send_a.finish().unwrap();

        let (mut send_b, mut recv_b) = conn_b.accept_bi().await.unwrap();
        let got = recv_b.read_to_end(64).await.unwrap();
        assert_eq!(got, b"unspanned-payload-1");
        send_b.write_all(b"unspanned-payload-2").await.unwrap();
        send_b.finish().unwrap();

        let got2 = recv_a.read_to_end(64).await.unwrap();
        assert_eq!(got2, b"unspanned-payload-2");
    };
    bytes_fut.with_subscriber(dispatch.clone()).await;

    let spans_after_bytes = recorded.spans.lock().unwrap()[before_bytes_len..].to_vec();
    let rdm_stream_spans: Vec<_> = spans_after_bytes
        .iter()
        .filter(|(_, name, _)| name.starts_with("rdm."))
        .collect();
    assert!(
        rdm_stream_spans.is_empty(),
        "stream byte reads/writes must not emit rdm.* spans, got {rdm_stream_spans:?}"
    );

    peer_a.shutdown().await;
    peer_b.shutdown().await;

    // Also verify when Dialer::open starts under `Dispatch::none()` (`NoSubscriber`, where
    // `new_span` mints `Id(0xDEAD) = Id(57005)`) or another `Registry` subscriber and continues on
    // a task/thread under `dispatch`, opening child spans does not clone a foreign `Id` on
    // `dispatch`'s `Registry`:
    let other_dispatch = tracing::Dispatch::new(Registry::default());
    let foreign_caller_span = tracing::dispatcher::with_default(&other_dispatch, || {
        let mut s = tracing::Span::none();
        for _ in 0..32 {
            s = tracing::info_span!("caller_on_other_registry");
            std::mem::forget(s.clone());
        }
        s
    });

    let alpns = [gateway_alpn(ALPN_A)];
    let (src, gw, dst) = async {
        let s = TestPeer::bind("mesh1.rpc.1", &alpns).await;
        let g = TestPeer::bind("mesh1.gateway.1", &alpns).await;
        let d = TestPeer::bind("mesh1.rpc.2", &alpns).await;
        s.know(&g);
        g.know(&d);
        seed_carrier_direct_edge(&[&s.sink, &g.sink], &g.birth, &d.birth, ALPN_A).await;
        (s, g, d)
    }
    .with_subscriber(dispatch.clone())
    .await;

    let (dialer_s, _) = src.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dst.registry.connections(ALPN_A).unwrap().split();

    // 1. `open_tunnel` starting under `Dispatch::none()` (`NoSubscriber` -> `tunnel_span` `Id(57005)`)
    // and completing under `dispatch`:
    let accept_t = tokio::spawn(
        async move {
            let c = acceptor_d.accept().await.unwrap();
            (c, acceptor_d)
        }
        .with_subscriber(dispatch.clone()),
    );
    let mut open_fut = Box::pin(dialer_s.open(&dst.birth));
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let first_poll = tracing::dispatcher::with_default(&tracing::Dispatch::none(), || {
        std::future::Future::poll(open_fut.as_mut(), &mut cx)
    });
    assert!(first_poll.is_pending(), "first poll of tunnel open must yield pending on connect");
    let conn_s = open_fut
        .with_subscriber(dispatch.clone())
        .await
        .expect("open across NoSubscriber and Registry must not panic in Registry::clone_span");
    let (_conn_d, mut acceptor_d) = accept_t.await.unwrap();
    drop(conn_s);

    // 2. `open` (Direct + Tunnel) capturing `caller_span` on `other_dispatch` and completing under `dispatch`:
    src.know(&dst);
    let accept_t2 = tokio::spawn(
        async move { acceptor_d.accept().await.unwrap() }.with_subscriber(dispatch.clone()),
    );
    let mut open_fut2 = Box::pin(dialer_s.open(&dst.birth));
    let first_poll2 = tracing::dispatcher::with_default(&other_dispatch, || {
        foreign_caller_span.in_scope(|| std::future::Future::poll(open_fut2.as_mut(), &mut cx))
    });
    assert!(first_poll2.is_pending(), "first poll of open must yield pending on connect");
    let conn_s2 = open_fut2
        .with_subscriber(dispatch.clone())
        .await
        .expect("open across separate Registries must not panic in Registry::clone_span");
    let _conn_d2 = accept_t2.await.unwrap();
    drop(conn_s2);

    src.shutdown().await;
    gw.shutdown().await;
    dst.shutdown().await;
}

/// CONTRACT (telemetry.md §3: a call span's children are the serve span, the handler's spans and
/// the invocation's end; pool.rs: a dial's span is a root of its own, linked (follows_from) to the
/// call that started it, never current while the dial runs): the transport spawns a connection's
/// driver task with the span current at its connect, and that task lives as long as the
/// connection. A Direct or Proxy open therefore connects under `rdm.node.connection.create.via-dial`,
/// a root linked to the opener's span (the Proxy's: to its tunnel span), so the caller's span
/// closes when its last handle drops while its connection still carries streams, and the dial's
/// span closes with the dial. Must NOT happen: the caller's span, and every span above it, held
/// open (and unexported) until the connection closes (rafka-v2 #3221; i141 #3164, trace
/// 889244ddf61ac4043385977c17a95eb8: request spans exported 53 min late, at the connections'
/// delete). Its own process, as the subscriber-sensitive cell above.
#[tokio::test]
async fn a_callers_span_closes_while_its_direct_and_proxy_connections_live() {
    if crate::own_process::delegated(module_path!(), "a_callers_span_closes_while_its_direct_and_proxy_connections_live") {
        return;
    }
    use tracing::instrument::WithSubscriber;
    use tracing::span::{Attributes, Id};
    use tracing::Subscriber;
    use tracing_subscriber::layer::{Context, SubscriberExt};
    use tracing_subscriber::Registry;

    #[derive(Default, Clone)]
    struct SpanLives {
        opened: Arc<Mutex<Vec<(Id, String, Option<Id>)>>>,
        closed: Arc<Mutex<Vec<Id>>>,
        follows: Arc<Mutex<Vec<(Id, Id)>>>,
    }

    impl<S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>>
        tracing_subscriber::Layer<S> for SpanLives
    {
        fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
            let parent = if let Some(p) = attrs.parent() {
                Some(p.clone())
            } else if attrs.is_contextual() {
                ctx.current_span().id().cloned()
            } else {
                None
            };
            self.opened.lock().unwrap().push((id.clone(), attrs.metadata().name().to_string(), parent));
        }
        fn on_follows_from(&self, span: &Id, follows: &Id, _ctx: Context<'_, S>) {
            self.follows.lock().unwrap().push((span.clone(), follows.clone()));
        }
        fn on_close(&self, id: Id, _ctx: Context<'_, S>) {
            self.closed.lock().unwrap().push(id);
        }
    }

    impl SpanLives {
        fn is_closed(&self, id: &Id) -> bool {
            self.closed.lock().unwrap().contains(id)
        }
        /// The `via-dial` spans linked to `from`, each with its parent.
        fn dials_linked_to(&self, from: &Id) -> Vec<(Id, Option<Id>)> {
            let follows = self.follows.lock().unwrap().clone();
            self.opened
                .lock()
                .unwrap()
                .iter()
                .filter(|(id, name, _)| {
                    name == "rdm.node.connection.create.via-dial" && follows.contains(&(id.clone(), from.clone()))
                })
                .map(|(id, _, parent)| (id.clone(), parent.clone()))
                .collect()
        }
    }

    let lives = SpanLives::default();
    let dispatch = tracing::Dispatch::new(Registry::default().with(lives.clone()));
    rdm_transport::enable_callsites();
    let caller = |route: &'static str| tracing::dispatcher::with_default(&dispatch, || tracing::info_span!("caller_operation", route));

    // Direct.
    let (a, b) = async {
        let a = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
        let b = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
        a.know(&b);
        b.know(&a);
        (a, b)
    }
    .with_subscriber(dispatch.clone())
    .await;
    let (dialer_a, _) = a.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_b) = b.registry.connections(ALPN_A).unwrap().split();
    let accept_b = tokio::spawn(async move { acceptor_b.accept().await.unwrap() }.with_subscriber(dispatch.clone()));
    let caller_direct = caller("direct");
    let caller_direct_id = caller_direct.id().expect("the caller's span has an id");
    let conn_a = tracing::Instrument::instrument(dialer_a.open(&b.birth), caller_direct.clone())
        .with_subscriber(dispatch.clone())
        .await
        .expect("the Direct open");
    let conn_b = accept_b.await.unwrap();
    assert_eq!(conn_a.kind(), ConnectionKind::Direct);
    drop(caller_direct);
    assert!(
        lives.is_closed(&caller_direct_id),
        "the caller's span is closed once its last handle drops, while its Direct connection lives: {:?}",
        lives.opened.lock().unwrap()
    );
    let dials = lives.dials_linked_to(&caller_direct_id);
    assert_eq!(dials.len(), 1, "one via-dial span linked (follows_from) to the caller's: {:?}", lives.opened.lock().unwrap());
    assert_eq!(dials[0].1, None, "the dial's span is a root");
    assert!(lives.is_closed(&dials[0].0), "the dial's span closed with the dial");
    // The connection the caller's span no longer covers carries a stream both ways.
    async {
        let (mut send, mut recv) = conn_a.open_bi().await.unwrap();
        send.write_all(b"direct-after-caller").await.unwrap();
        send.finish().unwrap();
        let (mut send_b, mut recv_b) = conn_b.accept_bi().await.unwrap();
        assert_eq!(recv_b.read_to_end(64).await.unwrap(), b"direct-after-caller");
        send_b.write_all(b"direct-back").await.unwrap();
        send_b.finish().unwrap();
        assert_eq!(recv.read_to_end(64).await.unwrap(), b"direct-back");
    }
    .with_subscriber(dispatch.clone())
    .await;

    // Proxy, through the carrier.
    let alpns = [gateway_alpn(ALPN_B)];
    let (src, gw, dst) = async {
        let s = TestPeer::bind("mesh1.rpc.3", &alpns).await;
        let g = TestPeer::bind("mesh1.gateway.1", &alpns).await;
        let d = TestPeer::bind("mesh1.rpc.4", &alpns).await;
        s.know(&g);
        g.know(&d);
        seed_carrier_direct_edge(&[&s.sink, &g.sink], &g.birth, &d.birth, ALPN_B).await;
        (s, g, d)
    }
    .with_subscriber(dispatch.clone())
    .await;
    let (dialer_s, _) = src.registry.connections(ALPN_B).unwrap().split();
    let (_, mut acceptor_d) = dst.registry.connections(ALPN_B).unwrap().split();
    let accept_d = tokio::spawn(async move { acceptor_d.accept().await.unwrap() }.with_subscriber(dispatch.clone()));
    let caller_proxy = caller("proxy");
    let caller_proxy_id = caller_proxy.id().expect("the caller's span has an id");
    let conn_s = tracing::Instrument::instrument(dialer_s.open(&dst.birth), caller_proxy.clone())
        .with_subscriber(dispatch.clone())
        .await
        .expect("the Proxy open");
    let conn_d = accept_d.await.unwrap();
    assert_eq!(conn_s.kind(), ConnectionKind::Proxy { via: gw.birth.clone() });
    drop(caller_proxy);
    assert!(
        lives.is_closed(&caller_proxy_id),
        "the caller's span is closed once its last handle drops, while its Proxy connection lives: {:?}",
        lives.opened.lock().unwrap()
    );
    let tunnel = lives
        .opened
        .lock()
        .unwrap()
        .iter()
        .find(|(_, name, parent)| name == "rdm.node.connection.create.via-tunnel" && parent.as_ref() == Some(&caller_proxy_id))
        .map(|(id, _, _)| id.clone())
        .expect("the opener's tunnel span is the caller's child");
    assert!(lives.is_closed(&tunnel), "the opener's tunnel span closed with the open");
    let dials = lives.dials_linked_to(&tunnel);
    assert_eq!(dials.len(), 1, "one via-dial span linked (follows_from) to the tunnel span: {:?}", lives.opened.lock().unwrap());
    assert_eq!(dials[0].1, None, "the dial's span is a root");
    assert!(lives.is_closed(&dials[0].0), "the dial's span closed with the dial");
    async {
        let (mut send, mut recv) = conn_s.open_bi().await.unwrap();
        send.write_all(b"proxy-after-caller").await.unwrap();
        send.finish().unwrap();
        let (mut send_d, mut recv_d) = conn_d.accept_bi().await.unwrap();
        assert_eq!(recv_d.read_to_end(64).await.unwrap(), b"proxy-after-caller");
        send_d.write_all(b"proxy-back").await.unwrap();
        send_d.finish().unwrap();
        assert_eq!(recv.read_to_end(64).await.unwrap(), b"proxy-back");
    }
    .with_subscriber(dispatch.clone())
    .await;

    a.shutdown().await;
    b.shutdown().await;
    src.shutdown().await;
    gw.shutdown().await;
    dst.shutdown().await;
}

fn gateway_alpn(name: &[u8]) -> Alpn {
    Alpn {
        name: name.to_vec(),
        proxy: true,
        carriers: vec![NodeKind::Gateway],
    }
}

/// A protocol that never travels over a Proxy, though its carrier list names the gateway kind: only
/// its `proxy: false` keeps a gateway from carrying it.
fn no_proxy_alpn(name: &[u8]) -> Alpn {
    Alpn {
        name: name.to_vec(),
        proxy: false,
        carriers: vec![NodeKind::Gateway],
    }
}

async fn seed_carrier_direct_edge(
    observer_sinks: &[&Arc<MemoryConnectionsSink>],
    carrier: &Birth,
    destination: &Birth,
    alpn: &[u8],
) {
    let row = NodeConnection {
        id: ConnectionId::mint(),
        source: carrier.to_end(),
        destination: destination.to_end(),
        kind: CoreConnectionKind::Direct,
        alpn: alpn.to_vec(),
        state: ConnectionState::Connected,
        carrier: None,
        recovery: None,
        reason: None,
        logged_at_ms: 1,
    };
    for sink in observer_sinks {
        let held = sink.held();
        let _ = held.lock().unwrap().apply(row.clone());
    }
}

struct CountingAdmitter {
    direct_from_source: AtomicUsize,
    from_carrier: AtomicUsize,
    source_id: NodeId,
    carrier_id: NodeId,
    allow: AtomicBool,
    reason: String,
}

impl Admitter for CountingAdmitter {
    fn verify(
        &self,
        caller: &Birth,
        _alpn: &[u8],
        _cert: &[u8],
    ) -> Result<(u64, &'static str), String> {
        if caller.node_id == self.source_id {
            self.direct_from_source.fetch_add(1, Ordering::SeqCst);
        } else if caller.node_id == self.carrier_id {
            self.from_carrier.fetch_add(1, Ordering::SeqCst);
        }
        if self.allow.load(Ordering::SeqCst) {
            Ok((0, "no-certs"))
        } else {
            Err(self.reason.clone())
        }
    }
}

#[tokio::test]
async fn a_held_proxy_is_taken_without_a_direct_dial() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

    // Source knows both carrier and dest, carrier knows dest:
    source.know(&carrier);
    source.know(&dest);
    carrier.know(&dest);
    carrier.know(&source);
    dest.know(&carrier);
    dest.know(&source);

    // Seed carrier -> dest Direct Connected in source and carrier held projections,
    // and seed a held Proxy Connected row in source's sink:
    seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;
    let held_proxy = NodeConnection {
        id: ConnectionId::mint(),
        source: source.birth.to_end(),
        destination: dest.birth.to_end(),
        kind: CoreConnectionKind::Proxy,
        alpn: ALPN_A.to_vec(),
        state: ConnectionState::Connected,
        carrier: Some(carrier.birth.to_end()),
        recovery: None,
        reason: None,
        logged_at_ms: 5,
    };
    source.sink.record(held_proxy);

    let dest_admitter = Arc::new(CountingAdmitter {
        direct_from_source: AtomicUsize::new(0),
        from_carrier: AtomicUsize::new(0),
        source_id: source.birth.node_id.clone(),
        carrier_id: carrier.birth.node_id.clone(),
        allow: AtomicBool::new(true),
        reason: String::new(),
    });
    dest.registry.set_admitter(dest_admitter.clone());

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let conn_s = dialer_s
        .open(&dest.birth)
        .await
        .expect("held Proxy must open through the carrier without a Direct dial");
    let conn_d = accept_task.await.unwrap();

    assert_eq!(
        dest_admitter.direct_from_source.load(Ordering::SeqCst),
        0,
        "a held Proxy must be taken without a Direct dial from source to destination"
    );
    assert_eq!(
        dest_admitter.from_carrier.load(Ordering::SeqCst),
        1,
        "destination must be dialled by the carrier"
    );
    assert_eq!(
        conn_s.kind(),
        ConnectionKind::Proxy {
            via: carrier.birth.clone()
        }
    );
    assert_eq!(
        conn_d.kind(),
        ConnectionKind::Proxy {
            via: carrier.birth.clone()
        }
    );
    assert_eq!(conn_s.peer(), &dest.birth);
    assert_eq!(conn_d.peer(), &source.birth);
    assert_eq!(conn_s.id(), conn_d.id());

    source.shutdown().await;
    carrier.shutdown().await;
    dest.shutdown().await;
}

#[tokio::test]
async fn a_not_sent_direct_on_a_proxy_alpn_goes_once_through_the_hashed_carrier() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let gw1 = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let gw2 = TestPeer::bind("mesh1.gateway.2", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

    // Source knows gw1 and gw2, but NOT dest (so Direct to dest is provably NotSent):
    source.know(&gw1);
    source.know(&gw2);
    gw1.know(&dest);
    gw2.know(&dest);

    seed_carrier_direct_edge(
        &[&source.sink, &gw1.sink, &gw2.sink],
        &gw1.birth,
        &dest.birth,
        ALPN_A,
    )
    .await;
    seed_carrier_direct_edge(
        &[&source.sink, &gw1.sink, &gw2.sink],
        &gw2.birth,
        &dest.birth,
        ALPN_A,
    )
    .await;

    let gw1_admitter = Arc::new(PolicyAdmitter {
        allow: AtomicBool::new(true),
        calls: AtomicUsize::new(0),
    });
    let gw2_admitter = Arc::new(PolicyAdmitter {
        allow: AtomicBool::new(true),
        calls: AtomicUsize::new(0),
    });
    gw1.registry.set_admitter(gw1_admitter.clone());
    gw2.registry.set_admitter(gw2_admitter.clone());

    let expected_idx = rdm_core::connections::spread(
        &source.birth.name.to_string(),
        &dest.birth.name.to_string(),
        dest.birth.incarnation.as_str(),
    ) % 2;
    let expected_carrier = if expected_idx == 0 {
        &gw1.birth
    } else {
        &gw2.birth
    };

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let conn_s = dialer_s
        .open(&dest.birth)
        .await
        .expect("NotSent Direct on a proxy ALPN must succeed via hashed carrier");
    let conn_d = accept_task.await.unwrap();

    assert_eq!(
        conn_s.kind(),
        ConnectionKind::Proxy {
            via: expected_carrier.clone()
        }
    );
    assert_eq!(conn_d.id(), conn_s.id());
    if expected_idx == 0 {
        assert_eq!(gw1_admitter.calls.load(Ordering::SeqCst), 1);
        assert_eq!(gw2_admitter.calls.load(Ordering::SeqCst), 0);
    } else {
        assert_eq!(gw1_admitter.calls.load(Ordering::SeqCst), 0);
        assert_eq!(gw2_admitter.calls.load(Ordering::SeqCst), 1);
    }

    // Verify Direct Failed then Proxy Connected were written to source.sink:
    let history = source.sink.history();
    assert!(
        history
            .iter()
            .any(|r| r.kind == CoreConnectionKind::Direct && r.state == ConnectionState::Failed),
        "Direct Failed must be recorded before carrier fallback: {history:?}"
    );
    let proxy_row = history
        .iter()
        .find(|r| r.kind == CoreConnectionKind::Proxy && r.state == ConnectionState::Connected)
        .expect("Proxy Connected must be recorded");
    assert_eq!(proxy_row.carrier, Some(expected_carrier.to_end()));

    source.shutdown().await;
    gw1.shutdown().await;
    gw2.shutdown().await;
    dest.shutdown().await;
}

/// A source, two gateway carriers and a destination the source cannot dial: the source's topology
/// holds both carriers and its held connections hold no other node's Direct (Q227: no node reads
/// another's connection facts); each carrier's own held connections hold its own Direct to the
/// destination when `edge`, given the index of the carrier the hash names first, says so. Returns
/// the peers and that index.
async fn carriers_from_topology(edge: impl Fn(usize) -> [bool; 2]) -> (TestPeer, [TestPeer; 2], TestPeer, usize) {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let gw = [TestPeer::bind("mesh1.gateway.1", &alpns).await, TestPeer::bind("mesh1.gateway.2", &alpns).await];
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;
    let first = (rdm_core::connections::spread(&source.birth.name.to_string(), &dest.birth.name.to_string(), dest.birth.incarnation.as_str()) % 2) as usize;
    for (g, holds) in gw.iter().zip(edge(first)) {
        source.know(g);
        g.know(&dest);
        g.know(&source);
        dest.know(g);
        if holds {
            seed_carrier_direct_edge(&[&g.sink], &g.birth, &dest.birth, ALPN_A).await;
        }
    }
    dest.know(&source);
    (source, gw, dest, first)
}

/// CONTRACT (Q227; connections.md §6, §7 step 2): after a direct dial that provably sent nothing, the
/// source takes its carrier from the topology it holds, a live node of its mesh of a kind its protocol
/// lets carry, spread by `hash(source, destination, destination incarnation)` over the path order,
/// with no other node's Direct fact in its held connections; the carrier's own Direct decides. Must
/// NOT happen: the source ending NotSent because it holds no fleet-wide Direct graph.
#[tokio::test]
async fn a_not_sent_direct_takes_its_carrier_from_the_topology_the_source_holds() {
    let (source, gw, dest, first) = carriers_from_topology(|_| [true, true]).await;
    assert!(source.sink.held().lock().unwrap().active_directs().is_empty(), "the source holds no other node's Direct");
    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();
    let accept_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let conn_s = dialer_s.open(&dest.birth).await.expect("a NotSent Direct goes through the hashed carrier of the topology");
    let conn_d = accept_task.await.unwrap();
    assert_eq!(conn_s.kind(), ConnectionKind::Proxy { via: gw[first].birth.clone() });
    assert_eq!(conn_d.id(), conn_s.id());
    let proxy_row = source.sink.history().into_iter().find(|r| r.kind == CoreConnectionKind::Proxy && r.state == ConnectionState::Connected).expect("Proxy Connected is recorded");
    assert_eq!(proxy_row.carrier, Some(gw[first].birth.to_end()));
    source.shutdown().await;
    let [g1, g2] = gw;
    g1.shutdown().await;
    g2.shutdown().await;
    dest.shutdown().await;
}

/// CONTRACT (Q227; connections.md §8): a carrier without its own Direct to the destination refuses
/// by name (`carrier-edge-lost`), and the source takes the next carrier in the path order, which
/// carries it. Must NOT happen: the source ending on the first refusal while another carrier stands.
#[tokio::test]
async fn a_carrier_without_its_own_direct_refuses_by_name_and_the_source_takes_the_next() {
    let (source, gw, dest, first) = carriers_from_topology(|first| {
        let mut edge = [true, true];
        edge[first] = false;
        edge
    })
    .await;
    let admitters: Vec<Arc<PolicyAdmitter>> = gw.iter().map(|g| {
        let a = Arc::new(PolicyAdmitter { allow: AtomicBool::new(true), calls: AtomicUsize::new(0) });
        g.registry.set_admitter(a.clone());
        a
    }).collect();
    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();
    let accept_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let next = 1 - first;
    let conn_s = dialer_s.open(&dest.birth).await.expect("the next carrier carries it");
    accept_task.await.unwrap();
    assert_eq!(conn_s.kind(), ConnectionKind::Proxy { via: gw[next].birth.clone() }, "the next carrier in the path order carries it");
    assert_eq!(admitters[first].calls.load(Ordering::SeqCst), 1, "the hashed carrier was asked first and refused");
    assert_eq!(admitters[next].calls.load(Ordering::SeqCst), 1, "the next carrier was asked once");
    source.shutdown().await;
    let [g1, g2] = gw;
    g1.shutdown().await;
    g2.shutdown().await;
    dest.shutdown().await;
}

/// CONTRACT (Q227): when every carrier of the topology refuses (none holds its own Direct to the
/// destination), the open ends with no route, by name. Must NOT happen: a Proxy recorded through a
/// carrier that refused.
#[tokio::test]
async fn every_carrier_refusing_ends_the_open_with_no_active_route() {
    let (source, gw, dest, _) = carriers_from_topology(|_| [false, false]).await;
    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, _acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();
    let ended = dialer_s.open(&dest.birth).await;
    assert!(matches!(&ended, Err(ConnectError::NoActiveRoute { direct: Some(_) })), "every carrier refused: {:?}", ended.map(|c| c.kind()));
    assert!(!source.sink.history().iter().any(|r| r.kind == CoreConnectionKind::Proxy), "no Proxy is recorded: {:?}", source.sink.history());
    source.shutdown().await;
    let [g1, g2] = gw;
    g1.shutdown().await;
    g2.shutdown().await;
    dest.shutdown().await;
}

#[test]
fn the_tunnel_handshake_carries_the_destinations_admitted_at_the_exact_birth_and_only_then_writes_proxy_connected() {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("rdm-evidence", exporter.clone());
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let d_rt = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d_rt)))
        .build()
        .unwrap();
    let _g = tracing::dispatcher::set_default(&dispatch);

    let (trace_id, proxy_row_id) = rt.block_on(async {
        let root_span = tracing::info_span!(
            "rdm.test.connection.create.via-proxy-tunnel",
            source = "mesh1.rpc.1",
            carrier = "mesh1.gateway.1",
            destination = "mesh1.rpc.2",
        );
        let trace_id = root_span
            .in_scope(rdm_transport::current_traceparent)
            .and_then(|tp| tp.split('-').nth(1).map(str::to_string))
            .expect("root span has trace_id");

        let proxy_row_id = tracing::Instrument::instrument(
            async {
                let alpns = [gateway_alpn(ALPN_A)];
                let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
                let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
                let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

                source.know(&carrier);
                carrier.know(&dest);

                let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
                let (dialer_c, _) = carrier.registry.connections(ALPN_A).unwrap().split();
                let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

                // 1. Carrier opens Direct to destination (Admit #1 + carrier->dest Direct Connected row):
                let accept_c_task = tokio::spawn(async move {
                    let c = acceptor_d.accept().await.unwrap();
                    (c, acceptor_d)
                });
                let conn_c_d = dialer_c.open(&dest.birth).await.unwrap();
                let (conn_d_c, mut acceptor_d) = accept_c_task.await.unwrap();
                assert_eq!(conn_c_d.kind(), ConnectionKind::Direct);
                assert_eq!(conn_d_c.kind(), ConnectionKind::Direct);

                // Propagate carrier's Direct Connected row into source's held projection (as gossip does):
                for row in carrier.sink.history() {
                    let held = source.sink.held();
                    let _ = held.lock().unwrap().apply(row);
                }

                // 2. Source opens to destination (Direct NotSent -> Tunnel through carrier: Admit #2, Admit #3, Proxy row):
                let accept_s_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
                let conn_s = dialer_s.open(&dest.birth).await.unwrap();
                let conn_d = accept_s_task.await.unwrap();

                assert_eq!(conn_s.peer(), &dest.birth);
                assert_eq!(conn_d.peer(), &source.birth);
                assert_eq!(
                    conn_s.kind(),
                    ConnectionKind::Proxy {
                        via: carrier.birth.clone()
                    }
                );
                assert_eq!(
                    conn_d.kind(),
                    ConnectionKind::Proxy {
                        via: carrier.birth.clone()
                    }
                );

                let history = source.sink.history();
                let proxy_rows: Vec<_> = history
                    .iter()
                    .filter(|r| r.kind == CoreConnectionKind::Proxy && r.state == ConnectionState::Connected)
                    .collect();
                assert_eq!(proxy_rows.len(), 1);
                assert_eq!(&proxy_rows[0].id, conn_s.id());
                assert_eq!(proxy_rows[0].destination, dest.birth.to_end());
                assert_eq!(proxy_rows[0].carrier, Some(carrier.birth.to_end()));
                let row_id = proxy_rows[0].id.to_string();

                source.shutdown().await;
                carrier.shutdown().await;
                dest.shutdown().await;
                row_id
            },
            root_span,
        )
        .await;
        (trace_id, proxy_row_id)
    });

    capture
        .flush(Duration::from_secs(10))
        .expect("all spans flushed to exporter");
    let finished = exporter.get_finished_spans().unwrap();
    let mut trace_spans = Vec::new();
    for s in finished {
        if s.span_context.trace_id().to_string() != trace_id {
            continue;
        }
        let attributes: serde_json::Map<String, serde_json::Value> = s
            .attributes
            .iter()
            .map(|kv| (kv.key.to_string(), serde_json::Value::String(kv.value.to_string())))
            .collect();
        let start_ns = s
            .start_time
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let end_ns = s
            .end_time
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as u64)
            .unwrap_or(0);
        let row = serde_json::json!({
            "name": s.name,
            "trace_id": s.span_context.trace_id().to_string(),
            "span_id": s.span_context.span_id().to_string(),
            "parent_span_id": s.parent_span_id.to_string(),
            "start_ns": start_ns,
            "end_ns": end_ns,
            "start_unix_nano": start_ns,
            "end_unix_nano": end_ns,
            "attributes": attributes,
        });
        println!("I148_E2_SPAN {}", serde_json::to_string(&row).unwrap());
        trace_spans.push(row);
    }

    // The peers hold no certs: each admission is by name (node-connections-admit.md `NoCerts`).
    let admits: Vec<_> = trace_spans
        .iter()
        .filter(|s| s["name"] == "rdm.node.connection.serve.via-admitted" || s["name"] == "rdm.node.connection.serve.via-no-certs-configured")
        .collect();
    assert_eq!(
        admits.len(),
        3,
        "three admits (carrier->dest Direct, source->carrier Tunnel leg 1, carrier->dest Tunnel leg 2) must be in the trace: {admits:?}"
    );
    let tunnels: Vec<_> = trace_spans
        .iter()
        .filter(|s| s["name"] == "rdm.node.connection.create.via-tunnel")
        .collect();
    assert_eq!(
        tunnels.len(),
        2,
        "via-tunnel span on both source and carrier must be in the trace: {tunnels:?}"
    );
    let proxy_observed: Vec<_> = trace_spans
        .iter()
        .filter(|s| {
            s["name"] == "rdm.node_admin.connection.update.via-observed"
                && s["attributes"]["kind"] == "proxy"
                && s["attributes"]["state"] == "connected"
                && s["attributes"]["entry"] == proxy_row_id
        })
        .collect();
    assert_eq!(
        proxy_observed.len(),
        1,
        "Proxy Connected row must be spanned in the trace: {trace_spans:?}"
    );
}

#[tokio::test]
async fn a_tunnel_refused_wrong_incarnation_or_lapsed_writes_nothing_and_ends_by_name() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

    source.know(&carrier);
    carrier.know(&dest);

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    // Case 1: Destination refuses at admission -> Refused by name, no Proxy Connected written:
    seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;
    let dest_admitter = Arc::new(PolicyAdmitter {
        allow: AtomicBool::new(false),
        calls: AtomicUsize::new(0),
    });
    dest.registry.set_admitter(dest_admitter.clone());

    let err1 = dialer_s
        .open(&dest.birth)
        .await
        .expect_err("tunnel refused by destination must fail");
    assert_eq!(
        err1,
        ConnectError::Refused {
            reason: "expired member cert".to_string()
        }
    );
    assert!(
        !source
            .sink
            .history()
            .iter()
            .any(|r| r.kind == CoreConnectionKind::Proxy && r.state == ConnectionState::Connected),
        "refused tunnel must write no Proxy Connected row"
    );

    // Case 2: Wrong incarnation at destination -> WrongIncarnation by name, no Proxy Connected written:
    dest_admitter.allow.store(true, Ordering::SeqCst);
    let wrong_inc = IncarnationId::mint();
    let stale_dest = Birth {
        name: dest.birth.name.clone(),
        node_id: dest.birth.node_id.clone(),
        incarnation: wrong_inc.clone(),
    };
    // Reset source's held state so open attempts against stale_dest:
    let fresh_source = TestPeer::bind("mesh1.rpc.3", &alpns).await;
    fresh_source.know(&carrier);
    seed_carrier_direct_edge(
        &[&fresh_source.sink, &carrier.sink],
        &carrier.birth,
        &stale_dest,
        ALPN_A,
    )
    .await;
    let (dialer_fs, _) = fresh_source.registry.connections(ALPN_A).unwrap().split();

    let err2 = dialer_fs
        .open(&stale_dest)
        .await
        .expect_err("tunnel to wrong destination incarnation must fail");
    assert_eq!(
        err2,
        ConnectError::WrongIncarnation {
            held: wrong_inc,
            found: dest.birth.incarnation.clone(),
        }
    );
    assert!(
        !fresh_source
            .sink
            .history()
            .iter()
            .any(|r| r.kind == CoreConnectionKind::Proxy && r.state == ConnectionState::Connected),
        "wrong-incarnation tunnel must write no Proxy Connected row"
    );
    let accept_timeout =
        tokio::time::timeout(Duration::from_millis(50), acceptor_d.accept()).await;
    assert!(
        accept_timeout.is_err(),
        "destination acceptor must not yield a wrong-incarnation tunnel"
    );

    // Case 3: Carrier cannot reach destination (carrier-edge-lost): the one carrier refused by name, so
    // the open ends with no active route (Q227), no Proxy Connected:
    let dead_dest = Birth {
        name: "mesh1.rpc.4".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
    };
    let fresh_source2 = TestPeer::bind("mesh1.rpc.5", &alpns).await;
    fresh_source2.know(&carrier);
    seed_carrier_direct_edge(
        &[&fresh_source2.sink, &carrier.sink],
        &carrier.birth,
        &dead_dest,
        ALPN_A,
    )
    .await;
    let (dialer_fs2, _) = fresh_source2.registry.connections(ALPN_A).unwrap().split();
    let err3 = dialer_fs2
        .open(&dead_dest)
        .await
        .expect_err("tunnel whose destination leg cannot be opened by carrier must fail");
    assert!(matches!(&err3, ConnectError::NoActiveRoute { direct: Some(_) }), "every carrier refused by name, the Direct dial named: {err3:?}");
    assert!(
        !fresh_source2
            .sink
            .history()
            .iter()
            .any(|r| r.kind == CoreConnectionKind::Proxy && r.state == ConnectionState::Connected),
        "failed tunnel must write no Proxy Connected row"
    );

    source.shutdown().await;
    fresh_source.shutdown().await;
    fresh_source2.shutdown().await;
    carrier.shutdown().await;
    dest.shutdown().await;
}

#[tokio::test]
async fn the_carrier_pairs_concurrent_streams_one_to_one_in_both_directions_and_reads_no_app_byte() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

    source.know(&carrier);
    carrier.know(&dest);
    seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_c) = carrier.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let conn_s = Arc::new(dialer_s.open(&dest.birth).await.unwrap());
    let conn_d = Arc::new(accept_task.await.unwrap());

    // Carrier's acceptor never receives a Connection for a tunnel:
    assert!(
        tokio::time::timeout(Duration::from_millis(50), acceptor_c.accept())
            .await
            .is_err(),
        "carrier must splice the tunnel internally and never hand it to the carrier's app acceptor"
    );

    // Spawn 4 concurrent source->dest streams and 4 concurrent dest->source streams:
    let mut s_to_d_open = Vec::new();
    for i in 0..4u8 {
        let cs = conn_s.clone();
        s_to_d_open.push(tokio::spawn(async move {
            let (mut send, mut recv) = cs.open_bi().await.unwrap();
            let msg = vec![b'S', i, 1, 2, 3];
            send.write_all(&msg).await.unwrap();
            send.finish().unwrap();
            let reply = recv.read_to_end(64).await.unwrap();
            assert_eq!(reply, vec![b'D', i, 9, 8, 7]);
        }));
    }

    let cd_for_s = conn_d.clone();
    let d_accept_s = tokio::spawn(async move {
        let mut handlers = Vec::new();
        for _ in 0..4 {
            let (mut send, mut recv) = cd_for_s.accept_bi().await.unwrap();
            handlers.push(tokio::spawn(async move {
                let got = recv.read_to_end(64).await.unwrap();
                assert_eq!(got[0], b'S');
                let idx = got[1];
                assert_eq!(&got[2..], &[1, 2, 3]);
                send.write_all(&[b'D', idx, 9, 8, 7]).await.unwrap();
                send.finish().unwrap();
            }));
        }
        for h in handlers {
            h.await.unwrap();
        }
    });

    for t in s_to_d_open {
        t.await.unwrap();
    }
    d_accept_s.await.unwrap();

    source.shutdown().await;
    carrier.shutdown().await;
    dest.shutdown().await;
}

#[tokio::test]
async fn a_destination_opened_stream_reaches_the_source_through_the_tunnel() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

    source.know(&carrier);
    carrier.know(&dest);
    seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let conn_s = dialer_s.open(&dest.birth).await.unwrap();
    let conn_d = accept_task.await.unwrap();

    // Destination opens a bi-stream toward source through the tunnel:
    let (mut send_d, mut recv_d) = conn_d.open_bi().await.unwrap();
    send_d.write_all(b"from-dest-to-source").await.unwrap();
    send_d.finish().unwrap();

    let (mut send_s, mut recv_s) = conn_s.accept_bi().await.unwrap();
    let got = recv_s.read_to_end(64).await.unwrap();
    assert_eq!(got, b"from-dest-to-source");
    send_s.write_all(b"ack-from-source").await.unwrap();
    send_s.finish().unwrap();

    let ack = recv_d.read_to_end(64).await.unwrap();
    assert_eq!(ack, b"ack-from-source");

    source.shutdown().await;
    carrier.shutdown().await;
    dest.shutdown().await;
}

#[tokio::test]
async fn a_reset_code_on_one_leg_is_the_same_code_on_the_other() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

    source.know(&carrier);
    carrier.know(&dest);
    seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let conn_s = dialer_s.open(&dest.birth).await.unwrap();
    let conn_d = accept_task.await.unwrap();

    // 1. Source resets send stream with code 321 -> dest recv stream sees StreamError::Reset { code: 321 }:
    let (mut send_s1, _recv_s1) = conn_s.open_bi().await.unwrap();
    send_s1.write_all(b"head").await.unwrap();
    let (_send_d1, mut recv_d1) = conn_d.accept_bi().await.unwrap();
    let mut b4 = [0u8; 4];
    recv_d1.read_exact(&mut b4).await.unwrap();
    assert_eq!(&b4, b"head");

    send_s1.reset(321).unwrap();
    let err_d1 = recv_d1
        .read_to_end(64)
        .await
        .expect_err("dest recv must observe source's reset code");
    assert_eq!(err_d1, StreamError::Reset { code: 321 });

    // 2. Dest stops recv stream with code 654 -> source send stream sees StreamError::Stopped { code: 654 }:
    let (mut send_s2, _recv_s2) = conn_s.open_bi().await.unwrap();
    send_s2.write_all(b"ping").await.unwrap();
    let (_send_d2, mut recv_d2) = conn_d.accept_bi().await.unwrap();
    recv_d2.read_exact(&mut b4).await.unwrap();
    recv_d2.stop(654).unwrap();
    let err_s2 = send_s2
        .stopped()
        .await
        .expect_err("source send must observe dest's stop code");
    assert_eq!(err_s2, StreamError::Stopped { code: 654 });

    source.shutdown().await;
    carrier.shutdown().await;
    dest.shutdown().await;
}

#[tokio::test]
async fn a_carrier_leg_closing_closes_every_paired_stream_carrier_gone() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

    source.know(&carrier);
    carrier.know(&dest);
    seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let conn_s = dialer_s.open(&dest.birth).await.unwrap();
    let conn_d = accept_task.await.unwrap();

    let (mut send_s, mut recv_s) = conn_s.open_bi().await.unwrap();
    send_s.write_all(b"active").await.unwrap();
    let (_send_d, mut recv_d) = conn_d.accept_bi().await.unwrap();
    let mut buf = [0u8; 6];
    recv_d.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"active");

    // Shut down the carrier so its legs close:
    carrier.shutdown().await;

    let closed_s = tokio::time::timeout(Duration::from_secs(5), conn_s.closed())
        .await
        .expect("conn_s.closed() must resolve when carrier closes");
    assert_eq!(closed_s, Closed::CarrierGone);

    let closed_d = tokio::time::timeout(Duration::from_secs(5), conn_d.closed())
        .await
        .expect("conn_d.closed() must resolve when carrier closes");
    assert_eq!(closed_d, Closed::CarrierGone);

    assert_eq!(
        recv_s.read_to_end(64).await.unwrap_err(),
        StreamError::Closed(Closed::CarrierGone)
    );
    assert_eq!(
        recv_d.read_to_end(64).await.unwrap_err(),
        StreamError::Closed(Closed::CarrierGone)
    );

    source.shutdown().await;
    dest.shutdown().await;
}

#[tokio::test]
async fn an_open_tunnel_and_its_streams_outlive_direct_restoration_and_the_next_open_goes_direct() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

    // Initially source knows only carrier, not dest:
    source.know(&carrier);
    carrier.know(&dest);
    seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move {
        let c1 = acceptor_d.accept().await.unwrap();
        (c1, acceptor_d)
    });
    let tunnel_s = dialer_s.open(&dest.birth).await.unwrap();
    let (tunnel_d, mut acceptor_d) = accept_task.await.unwrap();
    assert_eq!(
        tunnel_s.kind(),
        ConnectionKind::Proxy {
            via: carrier.birth.clone()
        }
    );

    // Open a long-lived stream on the tunnel before Direct restoration:
    let (mut t_send_s, mut t_recv_s) = tunnel_s.open_bi().await.unwrap();
    t_send_s.write_all(b"before-restore").await.unwrap();
    let (mut t_send_d, mut t_recv_d) = tunnel_d.accept_bi().await.unwrap();
    let mut b14 = [0u8; 14];
    t_recv_d.read_exact(&mut b14).await.unwrap();
    assert_eq!(&b14, b"before-restore");

    // Restore Direct: source now knows dest and records Direct Connected (which deletes the Proxy route):
    source.know(&dest);
    dest.know(&source);
    let direct_row = NodeConnection {
        id: ConnectionId::mint(),
        source: source.birth.to_end(),
        destination: dest.birth.to_end(),
        kind: CoreConnectionKind::Direct,
        alpn: ALPN_A.to_vec(),
        state: ConnectionState::Connected,
        carrier: None,
        recovery: None,
        reason: None,
        logged_at_ms: 100,
    };
    source.sink.record(direct_row);

    // 1. The open tunnel and its open stream outlive Direct restoration:
    t_send_s.write_all(b"after-restore").await.unwrap();
    t_send_s.finish().unwrap();
    let rest = t_recv_d.read_to_end(64).await.unwrap();
    assert_eq!(rest, b"after-restore");
    t_send_d.write_all(b"tunnel-reply").await.unwrap();
    t_send_d.finish().unwrap();
    let reply = t_recv_s.read_to_end(64).await.unwrap();
    assert_eq!(reply, b"tunnel-reply");

    // 2. The next open() goes Direct:
    let accept_task2 = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let next_s = dialer_s.open(&dest.birth).await.unwrap();
    let next_d = accept_task2.await.unwrap();
    assert_eq!(next_s.kind(), ConnectionKind::Direct);
    assert_eq!(next_d.kind(), ConnectionKind::Direct);

    // 3. Before delete, held_open_tunnels lists the tunnel by its row id with `route: deleted`,
    // and node.connection.delete by that id closes the tunnel without writing a second Disconnected row:
    let tunnel_id = tunnel_s.id().clone();
    let tunnel_list = source.registry.held_open_tunnels();
    assert_eq!(tunnel_list.len(), 1);
    assert_eq!(tunnel_list[0].id, tunnel_id);
    assert_eq!(tunnel_list[0].reason.as_deref(), Some("route: deleted"));

    let deleted = source
        .registry
        .delete_connections(&ConnectionScope::Connections(vec![tunnel_id.clone()]))
        .await;
    assert_eq!(deleted, vec![tunnel_id]);
    let closed = tokio::time::timeout(Duration::from_secs(5), tunnel_s.closed())
        .await
        .unwrap();
    assert_eq!(closed, Closed::ConnectionsDeleted);

    let proxy_disconnects: Vec<_> = source
        .sink
        .history()
        .into_iter()
        .filter(|r| {
            r.kind == CoreConnectionKind::Proxy && r.state == ConnectionState::Disconnected
        })
        .collect();
    assert_eq!(
        proxy_disconnects.len(),
        1,
        "ending a deleted tunnel must write no second Disconnected row: {proxy_disconnects:?}"
    );
    assert_eq!(proxy_disconnects[0].reason.as_deref(), Some("direct-restored"));

    source.shutdown().await;
    carrier.shutdown().await;
    dest.shutdown().await;
}

/// CONTRACT (connections.md §10: "A stream Proxy (i148, a tunnel) outlives its delete ...
/// `node.connection.get` lists it by its row id with `route: deleted`, and `node.connection.delete
/// { All | [ids] }` closes it by that id like any other. Its end writes no second row; it is spanned
/// `rdm.node.connection.delete.via-tunnel-ended`"): a tunnel opened through the carrier, its route
/// then deleted by a Direct Connected (`Disconnected(direct-restored)` held), is closed by
/// `node.connection.delete { [its id] }`: its end is one `via-tunnel-ended` span naming the entry,
/// the source, the carrier, the destination and the alpn, and the log holds the one Proxy
/// Disconnected the delete wrote. Must NOT happen: the end unspanned, spanned twice, or a second
/// Proxy Disconnected row.
#[test]
fn a_tunnel_outliving_its_proxy_route_closed_by_id_ends_on_one_tunnel_ended_span_and_writes_no_second_row() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-tunnel-ended").expect("the cell's evidence file can be created");
    let (tunnel_id, names, proxy_disconnects) = capture.run(4, async {
        let alpns = [gateway_alpn(ALPN_A)];
        let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
        let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
        let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;
        source.know(&carrier);
        carrier.know(&dest);
        seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;
        let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
        let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();
        let accept = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
        let tunnel = dialer_s.open(&dest.birth).await.unwrap();
        let _far = accept.await.unwrap();
        assert_eq!(tunnel.kind(), ConnectionKind::Proxy { via: carrier.birth.clone() });
        // The route is deleted: a Direct Connected beside the Proxy owes its Disconnected(direct-restored).
        source.sink.record(NodeConnection {
            id: ConnectionId::mint(),
            source: source.birth.to_end(),
            destination: dest.birth.to_end(),
            kind: CoreConnectionKind::Direct,
            alpn: ALPN_A.to_vec(),
            state: ConnectionState::Connected,
            carrier: None,
            recovery: None,
            reason: None,
            logged_at_ms: 100,
        });
        assert_eq!(source.registry.held_open_tunnels().iter().map(|r| (r.id.clone(), r.reason.clone())).collect::<Vec<_>>(), vec![(tunnel.id().clone(), Some("route: deleted".to_string()))], "the open tunnel is listed by its id, its route: deleted");
        let deleted = source.registry.delete_connections(&ConnectionScope::Connections(vec![tunnel.id().clone()])).await;
        assert_eq!(deleted, vec![tunnel.id().clone()]);
        assert_eq!(tokio::time::timeout(Duration::from_secs(5), tunnel.closed()).await.unwrap(), Closed::ConnectionsDeleted);
        let proxy_disconnects: Vec<Option<String>> = source.sink.history().into_iter().filter(|r| r.kind == CoreConnectionKind::Proxy && r.state == ConnectionState::Disconnected).map(|r| r.reason).collect();
        let names = (source.birth.name.to_string(), carrier.birth.name.to_string(), dest.birth.name.to_string());
        let id = tunnel.id().to_string();
        source.shutdown().await;
        carrier.shutdown().await;
        dest.shutdown().await;
        (id, names, proxy_disconnects)
    });
    assert_eq!(proxy_disconnects, vec![Some("direct-restored".to_string())], "the route delete's row is the only Proxy Disconnected: the tunnel's end writes none");
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let ended: Vec<Vec<String>> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node.connection.delete.via-tunnel-ended")
        .map(|s| ["entry", "source", "carrier", "destination", "alpn"].iter().map(|k| s["attributes"][*k].as_str().unwrap_or_default().to_string()).collect())
        .collect();
    assert_eq!(ended, vec![vec![tunnel_id, names.0, names.1, names.2, String::from_utf8_lossy(ALPN_A).to_string()]], "the tunnel's end is one span naming it");
}

/// The carrier's `rdm.node.connection.create.via-tunnel` span among `spans`: the child of the
/// source's tunnel span for `entry` (telemetry.md §3, data-plane row).
fn carriers_tunnel_span<'a>(spans: &'a [serde_json::Value], entry: &str) -> Option<&'a serde_json::Value> {
    let source = spans.iter().find(|s| s["name"] == "rdm.node.connection.create.via-tunnel" && s["attributes"]["entry"] == entry && s["attributes"]["kind"] == "proxy")?;
    spans.iter().find(|s| s["name"] == "rdm.node.connection.create.via-tunnel" && s["parent_span_id"] == source["span_id"])
}

/// CONTRACT (telemetry.md §3, data-plane row: "a carrier's tunnel span
/// `rdm.node.connection.create.via-tunnel` is its child and passes its own context on to the
/// destination"; §4: a connection's life is never held by the span that opened it): a Proxy opened
/// through the carrier and carrying a stream both ways; while it still carries, the carrier's tunnel
/// span is already exported, the source's tunnel span its parent, naming the entry, with outcome
/// `opened`; and the tunnel carries on after the read. Must NOT happen: the carrier's span held open
/// for the tunnel's life (exported only at its end, or never).
#[test]
fn a_carriers_tunnel_span_is_exported_once_the_tunnel_opens_while_the_tunnel_still_carries() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-carrier-tunnel-span").expect("the cell's evidence file can be created");
    let (entry, names, carrier_span) = capture.run(4, async {
        let alpns = [gateway_alpn(ALPN_A)];
        let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
        let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
        let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;
        source.know(&carrier);
        carrier.know(&dest);
        seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;
        let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
        let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();
        let accept = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
        let tunnel = dialer_s.open(&dest.birth).await.unwrap();
        let far = accept.await.unwrap();
        assert_eq!(tunnel.kind(), ConnectionKind::Proxy { via: carrier.birth.clone() });
        let carry = |tag: &'static str| {
            let (tunnel, far) = (&tunnel, &far);
            async move {
                let (mut send, mut recv) = tunnel.open_bi().await.unwrap();
                send.write_all(tag.as_bytes()).await.unwrap();
                send.finish().unwrap();
                let (mut send_d, mut recv_d) = far.accept_bi().await.unwrap();
                assert_eq!(recv_d.read_to_end(64).await.unwrap(), tag.as_bytes());
                send_d.write_all(b"back").await.unwrap();
                send_d.finish().unwrap();
                assert_eq!(recv.read_to_end(64).await.unwrap(), b"back");
            }
        };
        carry("before-the-read").await;
        let entry = tunnel.id().to_string();
        let spans = capture.spans().expect("the cell's spans reached its evidence file");
        let carrier_span = carriers_tunnel_span(&spans, &entry).cloned();
        carry("after-the-read").await;
        let names = (source.birth.name.to_string(), carrier.birth.name.to_string(), dest.birth.name.to_string());
        source.shutdown().await;
        carrier.shutdown().await;
        dest.shutdown().await;
        (entry, names, carrier_span)
    });
    let span = carrier_span.expect("the carrier's tunnel span is exported while the tunnel still carries");
    let attrs: Vec<String> = ["source", "carrier", "destination", "alpn", "entry", "outcome"].iter().map(|k| span["attributes"][*k].as_str().unwrap_or_default().to_string()).collect();
    assert_eq!(attrs, vec![names.0, names.1, names.2, String::from_utf8_lossy(ALPN_A).to_string(), entry, "opened".to_string()], "the carrier's tunnel span names the tunnel and its outcome: {span}");
}

/// CONTRACT (connections.md §10: "`node.connection.delete { All | [ids] }` closes it by that id like
/// any other"; §1: the carrier carries traffic for as long as the connection stands): a Proxy open
/// through the carrier is deleted by id on the source (`node.connection.delete { [its id] }`). The
/// carrier's two legs close: the destination's connection ends within the bound; and the carrier ends
/// its tunnel on its own span, `rdm.node.connection.delete.via-carrier-tunnel-ended`, naming the
/// entry, the source, the carrier, the destination and the alpn, and the close the source's leg
/// carried (`node.connection.delete`), in the opener's trace. Must NOT happen: the destination's leg
/// left open, or the carrier's tunnel ending unspanned.
#[test]
fn a_proxy_deleted_by_id_closes_the_carriers_legs_and_ends_its_tunnel_on_the_carriers_span() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-carrier-tunnel-ended").expect("the cell's evidence file can be created");
    let (entry, names, far_closed) = capture.run(4, async {
        let alpns = [gateway_alpn(ALPN_A)];
        let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
        let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
        let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;
        source.know(&carrier);
        carrier.know(&dest);
        seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;
        let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
        let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();
        let accept = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
        let tunnel = dialer_s.open(&dest.birth).await.unwrap();
        let far = accept.await.unwrap();
        assert_eq!(tunnel.kind(), ConnectionKind::Proxy { via: carrier.birth.clone() });
        let deleted = source.registry.delete_connections(&ConnectionScope::Connections(vec![tunnel.id().clone()])).await;
        assert_eq!(deleted, vec![tunnel.id().clone()]);
        assert_eq!(tokio::time::timeout(Duration::from_secs(5), tunnel.closed()).await.unwrap(), Closed::ConnectionsDeleted);
        // Read before any shutdown: a router's shutdown closes every leg it carries.
        let far_closed = tokio::time::timeout(Duration::from_secs(5), far.closed()).await.ok();
        let names = (source.birth.name.to_string(), carrier.birth.name.to_string(), dest.birth.name.to_string());
        let entry = tunnel.id().to_string();
        source.shutdown().await;
        carrier.shutdown().await;
        dest.shutdown().await;
        (entry, names, far_closed)
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let ended: Vec<&serde_json::Value> = spans.iter().filter(|s| s["name"] == "rdm.node.connection.delete.via-carrier-tunnel-ended").collect();
    assert_eq!(ended.len(), 1, "the carrier's tunnel ends on one span of its own: {ended:#?}");
    let attrs: Vec<String> = ["entry", "source", "carrier", "destination", "alpn", "closed"].iter().map(|k| ended[0]["attributes"][*k].as_str().unwrap_or_default().to_string()).collect();
    assert_eq!(attrs, vec![entry.clone(), names.0, names.1, names.2, String::from_utf8_lossy(ALPN_A).to_string(), "node.connection.delete".to_string()], "{}", ended[0]);
    let source_tunnel = spans.iter().find(|s| s["name"] == "rdm.node.connection.create.via-tunnel" && s["attributes"]["entry"] == entry.as_str() && s["attributes"]["kind"] == "proxy").expect("the source's tunnel span");
    assert_eq!(ended[0]["trace_id"], source_tunnel["trace_id"], "the carrier's tunnel end is in the opener's trace");
    assert!(far_closed.is_some(), "the carrier closed the destination's leg once the source's leg was deleted");
}

/// CONTRACT (connections.md §6): a protocol whose `Alpn` says `proxy: false` never consumes a Proxy as a
/// legal route even when one is held, and a Direct that sent nothing asks no carrier, though its carrier
/// list names the carrier's kind and the carrier holds an Active Direct to the destination. Must NOT
/// happen: the held Proxy taken, or the gateway asked to carry the connection.
#[tokio::test]
async fn a_protocol_with_proxy_false_never_consumes_a_proxy_even_when_one_is_held() {
    let alpns = [no_proxy_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;
    let fresh = TestPeer::bind("mesh1.rpc.3", &alpns).await;

    source.know(&carrier);
    fresh.know(&carrier);
    carrier.know(&dest);
    seed_carrier_direct_edge(&[&source.sink, &carrier.sink, &fresh.sink], &carrier.birth, &dest.birth, ALPN_A).await;
    let carrier_admitter = Arc::new(PolicyAdmitter { allow: AtomicBool::new(true), calls: AtomicUsize::new(0) });
    carrier.registry.set_admitter(carrier_admitter.clone());

    // Seed both a Failed Direct and a Connected Proxy through the gateway on ALPN_A in source's sink:
    let failed_direct = NodeConnection {
        id: ConnectionId::mint(),
        source: source.birth.to_end(),
        destination: dest.birth.to_end(),
        kind: CoreConnectionKind::Direct,
        alpn: ALPN_A.to_vec(),
        state: ConnectionState::Failed,
        carrier: None,
        recovery: Some(rdm_core::connections::DirectRecovery {
            recovery_round: 1,
            attempt_ordinal: 1,
        }),
        reason: Some("unreachable".into()),
        logged_at_ms: 10,
    };
    let held_proxy = NodeConnection {
        id: ConnectionId::mint(),
        source: source.birth.to_end(),
        destination: dest.birth.to_end(),
        kind: CoreConnectionKind::Proxy,
        alpn: ALPN_A.to_vec(),
        state: ConnectionState::Connected,
        carrier: Some(carrier.birth.to_end()),
        recovery: None,
        reason: None,
        logged_at_ms: 11,
    };
    source.sink.record(failed_direct);
    source.sink.record(held_proxy);

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let err = dialer_s
        .open(&dest.birth)
        .await
        .expect_err("a protocol with proxy false must refuse with NoActiveRoute when Direct is Failed");
    assert_eq!(err, ConnectError::NoActiveRoute { direct: None }, "the held Proxy is not a route for a protocol with proxy false");

    // A source holding nothing, whose direct dial sends nothing (it does not know the destination):
    // the dial's NotSent is the answer, and no carrier is asked.
    let (dialer_f, _) = fresh.registry.connections(ALPN_A).unwrap().split();
    let err = dialer_f.open(&dest.birth).await.expect_err("the destination is unknown to the fresh source");
    assert!(matches!(err, ConnectError::NotSent { .. }), "a Direct that sent nothing ends NotSent, no carrier asked: {err:?}");
    assert_eq!(carrier_admitter.calls.load(Ordering::SeqCst), 0, "the gateway was never asked to carry a protocol with proxy false");

    source.shutdown().await;
    carrier.shutdown().await;
    dest.shutdown().await;
    fresh.shutdown().await;
}

/// CONTRACT (connections.md §6): the carriers a source asks are of a node type its protocol's carrier list
/// names. An rpc node holding an Active Direct to the destination is never picked for a protocol whose
/// list names only the gateway kind: beside a gateway the gateway carries, and alone it leaves the
/// source's NotSent standing. Must NOT happen: the rpc node asked to carry the connection.
#[tokio::test]
async fn a_carrier_outside_the_protocols_list_is_never_picked() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let outside = TestPeer::bind("mesh1.rpc.3", &alpns).await;
    let gw = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;
    let lone = TestPeer::bind("mesh1.rpc.4", &alpns).await;

    // Neither source knows the destination, so its Direct is provably NotSent:
    source.know(&outside);
    source.know(&gw);
    lone.know(&outside);
    outside.know(&dest);
    gw.know(&dest);
    seed_carrier_direct_edge(&[&source.sink, &outside.sink, &gw.sink, &lone.sink], &outside.birth, &dest.birth, ALPN_A).await;
    seed_carrier_direct_edge(&[&source.sink, &outside.sink, &gw.sink, &lone.sink], &gw.birth, &dest.birth, ALPN_A).await;
    let outside_admitter = Arc::new(PolicyAdmitter { allow: AtomicBool::new(true), calls: AtomicUsize::new(0) });
    let gw_admitter = Arc::new(PolicyAdmitter { allow: AtomicBool::new(true), calls: AtomicUsize::new(0) });
    outside.registry.set_admitter(outside_admitter.clone());
    gw.registry.set_admitter(gw_admitter.clone());

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();
    let accept_task = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
    let conn_s = dialer_s.open(&dest.birth).await.expect("the gateway carries the connection");
    let _conn_d = accept_task.await.unwrap();
    assert_eq!(conn_s.kind(), ConnectionKind::Proxy { via: gw.birth.clone() }, "the gateway, the one kind the list names, carries");
    assert_eq!(gw_admitter.calls.load(Ordering::SeqCst), 1);

    let (dialer_l, _) = lone.registry.connections(ALPN_A).unwrap().split();
    let err = dialer_l.open(&dest.birth).await.expect_err("no node of a kind the list names stands for this source");
    assert!(matches!(err, ConnectError::NotSent { .. }), "with no eligible carrier the Direct's NotSent stands: {err:?}");
    assert_eq!(outside_admitter.calls.load(Ordering::SeqCst), 0, "the rpc node, outside the protocol's list, was never asked to carry");

    source.shutdown().await;
    outside.shutdown().await;
    gw.shutdown().await;
    dest.shutdown().await;
    lone.shutdown().await;
}

#[tokio::test]
async fn the_carrier_choice_is_kept_until_a_named_event_and_spread_by_source() {
    let alpns = [gateway_alpn(ALPN_A)];
    let gw1 = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let gw2 = TestPeer::bind("mesh1.gateway.2", &alpns).await;
    let gw3 = TestPeer::bind("mesh1.gateway.3", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.99", &alpns).await;
    gw1.know(&dest);
    gw2.know(&dest);
    gw3.know(&dest);

    // 1. Verify spread by source across eligible carriers:
    let mut chosen_carriers = std::collections::BTreeSet::new();
    for ord in 1..=12 {
        let s_name: rdm_core::PathName = format!("mesh1.rpc.{ord}").parse().unwrap();
        let idx = rdm_core::connections::spread(
            &s_name.to_string(),
            &dest.birth.name.to_string(),
            dest.birth.incarnation.as_str(),
        ) % 2;
        chosen_carriers.insert(idx);
    }
    assert_eq!(
        chosen_carriers.len(),
        2,
        "spread must distribute sources across eligible carriers"
    );

    // 2. Establish a Proxy from source to dest when gw1 and gw2 are eligible:
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    source.know(&gw1);
    source.know(&gw2);
    source.know(&gw3);
    seed_carrier_direct_edge(
        &[&source.sink, &gw1.sink, &gw2.sink, &gw3.sink],
        &gw1.birth,
        &dest.birth,
        ALPN_A,
    )
    .await;
    seed_carrier_direct_edge(
        &[&source.sink, &gw1.sink, &gw2.sink, &gw3.sink],
        &gw2.birth,
        &dest.birth,
        ALPN_A,
    )
    .await;

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    let accept1 = tokio::spawn(async move {
        let c = acceptor_d.accept().await.unwrap();
        (c, acceptor_d)
    });
    let conn1 = dialer_s.open(&dest.birth).await.unwrap();
    let (_conn1_d, mut acceptor_d) = accept1.await.unwrap();
    let ConnectionKind::Proxy { via: first_carrier } = conn1.kind() else {
        panic!("expected Proxy connection");
    };

    // Now gw3 appears with a newer Direct Connected edge to dest:
    seed_carrier_direct_edge(
        &[&source.sink, &gw1.sink, &gw2.sink, &gw3.sink],
        &gw3.birth,
        &dest.birth,
        ALPN_A,
    )
    .await;

    // Kept: a second open() still uses `first_carrier` because the held Proxy is valid:
    let accept2 = tokio::spawn(async move {
        let c = acceptor_d.accept().await.unwrap();
        (c, acceptor_d)
    });
    let conn2 = dialer_s.open(&dest.birth).await.unwrap();
    let (_conn2_d, _acceptor_d) = accept2.await.unwrap();
    assert_eq!(
        conn2.kind(),
        ConnectionKind::Proxy {
            via: first_carrier.clone()
        },
        "a newly available carrier must not reshuffle a working held Proxy"
    );

    source.shutdown().await;
    gw1.shutdown().await;
    gw2.shutdown().await;
    gw3.shutdown().await;
    dest.shutdown().await;
}

#[tokio::test]
async fn a_direct_connected_disconnects_the_proxy() {
    let alpns = [gateway_alpn(ALPN_A)];
    let source = TestPeer::bind("mesh1.rpc.1", &alpns).await;
    let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
    let dest = TestPeer::bind("mesh1.rpc.2", &alpns).await;

    source.know(&carrier);
    carrier.know(&dest);
    seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;

    let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acceptor_d) = dest.registry.connections(ALPN_A).unwrap().split();

    // 1. First open goes via Proxy because Direct is unreachable:
    let accept1 = tokio::spawn(async move {
        let c = acceptor_d.accept().await.unwrap();
        (c, acceptor_d)
    });
    let conn_proxy = dialer_s.open(&dest.birth).await.unwrap();
    let (_conn_proxy_d, _acceptor_d) = accept1.await.unwrap();
    assert_eq!(
        conn_proxy.kind(),
        ConnectionKind::Proxy {
            via: carrier.birth.clone()
        }
    );
    assert!(
        source
            .sink
            .held()
            .lock()
            .unwrap()
            .own_proxy_for(&dest.birth.name, ALPN_A)
            .unwrap()
            .is_some(),
        "Proxy Connected is active before Direct Connected"
    );

    // 2. Direct Connected lands on ALPN_A -> Proxy Disconnected(reason=direct-restored) deletes the Proxy:
    let direct_connected = NodeConnection {
        id: ConnectionId::mint(),
        source: source.birth.to_end(),
        destination: dest.birth.to_end(),
        kind: CoreConnectionKind::Direct,
        alpn: ALPN_A.to_vec(),
        state: ConnectionState::Connected,
        carrier: None,
        recovery: None,
        reason: None,
        logged_at_ms: 50,
    };
    source.sink.record(direct_connected);

    {
        let held_guard = source.sink.held();
        let held = held_guard.lock().unwrap();
        assert!(
            held.own_proxy_for(&dest.birth.name, ALPN_A)
                .unwrap()
                .is_none(),
            "Direct Connected must delete the Proxy on the same ALPN"
        );
        assert!(
            held.active_direct_for(&source.birth.name, &dest.birth.name, ALPN_A)
                .unwrap()
                .is_some(),
            "Direct Connected is now active"
        );
    }
    let history = source.sink.history();
    let drop_row = history
        .iter()
        .find(|r| {
            r.kind == CoreConnectionKind::Proxy
                && r.state == ConnectionState::Disconnected
                && r.alpn == ALPN_A
        })
        .expect("Proxy Disconnected row must be written");
    assert_eq!(drop_row.reason.as_deref(), Some("direct-restored"));

    source.shutdown().await;
    carrier.shutdown().await;
    dest.shutdown().await;
}


/// A reference no OS clock reads (1970-03-23): every cert of the chain cells below is judged on it.
const CHAIN_NOW_MS: u64 = 7_000_000_000;

/// A testkit fabric chain (`rdm_core::certs`): a root and its mesh's issuing key.
struct Chain {
    root: rdm_core::certs::SigningKey,
    ca: rdm_core::certs::CaKey,
    issuer: rdm_core::certs::IssuingKey,
}

impl Chain {
    /// A fresh root, a fabric CA under it, and `mesh`'s issuing key under that CA, minted at `at_ms`
    /// for `ttl_secs`.
    fn minted(mesh: &str, ttl_secs: u64, at_ms: u64) -> Self {
        let root = rdm_core::certs::mint_key();
        let ca = rdm_core::certs::CaKey::mint(&root, rdm_core::certs::CA_CERT_TTL_SECS, at_ms);
        let issuer = rdm_core::certs::IssuingKey::mint(&ca, mesh, at_ms, at_ms + ttl_secs * 1000).expect("within the CA's life");
        Self { root, ca, issuer }
    }

    /// The signed member cert of `peer`'s birth, expiring at `expiry_ms`.
    fn member(&self, peer: &TestPeer, expiry_ms: u64) -> rdm_core::certs::SignedCert {
        let birth = rdm_core::certs::BirthIdentity {
            node_id: peer.birth.node_id.clone(),
            incarnation: peer.birth.incarnation.clone(),
            mesh: peer.birth.name.mesh.clone(),
            name: peer.birth.name.clone(),
            endpoint_key: rdm_core::EndpointId(peer.resolved.endpoint_id.to_string()),
        };
        rdm_core::certs::issue(&self.issuer, &birth, self.issuer.cert().cert.issued_ms, expiry_ms).expect("the testkit chain issues the member cert")
    }
}

/// Rafka-time adopted at [`CHAIN_NOW_MS`].
fn chain_time() -> rdm_transport::clock::RafkaTime {
    let time = rdm_transport::clock::RafkaTime::unadopted();
    time.adopt(CHAIN_NOW_MS);
    time
}

/// One floor of skew, in ms: what a presented cert's expiry is allowed on either side.
fn floor_ms() -> u64 {
    rdm_core::cadence::staleness_floor().as_millis() as u64
}

/// A dialer and an acceptor on [`ALPN_A`], the acceptor admitting on `roots` at [`CHAIN_NOW_MS`], the
/// dialer presenting `cert`. Returns the dialer's open: the admitted connection or the refusal.
async fn dial_presenting(roots: Vec<rdm_core::certs::VerifyingKey>, cert: impl FnOnce(&TestPeer) -> Vec<u8>) -> Result<(), ConnectError> {
    let dialer = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let acceptor = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    dialer.know(&acceptor);
    acceptor.know(&dialer);
    acceptor.registry.admit_on(roots, chain_time());
    dialer.registry.set_member_cert(cert(&dialer));
    let (opener, _) = dialer.registry.connections(ALPN_A).unwrap().split();
    let (_, mut accepts) = acceptor.registry.connections(ALPN_A).unwrap().split();
    let accepted = tokio::spawn(async move { accepts.accept().await.map(|c| c.id().clone()) });
    let opened = opener.open(&acceptor.birth).await;
    let out = match opened {
        Ok(conn) => {
            let id = tokio::time::timeout(Duration::from_secs(10), accepted).await.expect("the admitted connection reaches accept").unwrap().unwrap();
            assert_eq!(&id, conn.id(), "the connection the acceptor serves is the one opened");
            Ok(())
        }
        Err(e) => {
            accepted.abort();
            let refused: Vec<_> = acceptor.sink.history().into_iter().filter(|r| r.state == ConnectionState::Refused).collect();
            if let ConnectError::Refused { reason } = &e {
                assert_eq!(refused.iter().map(|r| r.reason.clone()).collect::<Vec<_>>(), vec![Some(reason.clone())], "the acceptor's Refused fact carries the verdict's reason");
            }
            Err(e)
        }
    };
    dialer.shutdown().await;
    acceptor.shutdown().await;
    out
}

/// The verdict on a presented cert that `tamper` makes of a valid one of the dialer's birth, signed
/// by a mesh1 chain minted at [`CHAIN_NOW_MS`] and admitted on that chain's root.
async fn refusal_of(tamper: impl FnOnce(&Chain, &TestPeer) -> rdm_core::certs::SignedCert) -> String {
    let chain = Chain::minted("mesh1", 3_600, CHAIN_NOW_MS);
    let root = chain.root.verifying_key();
    match dial_presenting(vec![root], |dialer| rdm_core::certs::cert_to_bytes(&tamper(&chain, dialer))).await {
        Err(ConnectError::Refused { reason }) => reason,
        other => panic!("the tampered cert must be refused by name: {other:?}"),
    }
}

/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:20 — "Caller | The acceptor,
/// for every inbound connection whose ALPN the embedding app registered the step for at `bind`"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:21 — "Target | One inbound
/// connection, identified by the dialer's endpoint key and the connection id"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:22 — "Exchange | The dialer
/// writes its member cert on the connection's first bi-stream and keeps the send half open; the
/// acceptor reads it within one staleness floor... writes the verdict back on the same stream,
/// then serves or drops."
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:23 — "Verification | The
/// member's signature against its issuer's key; the issuer's signature against the fabric CA's key
/// and the CA's against a root of the node's set; the issuer's mesh is the first segment of the
/// member's path.name; the three expiries (CA, issuer, member), judged on rafka-time (else
/// `local-bootstrap`) with one staleness floor of skew on either side"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:24 — "Result | `admitted`: the
/// connection is served."
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:25 — "Row | ... an admission
/// is the ordinary `Connected` fact"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:28 — "Spans |
/// `rdm.node.connection.serve.via-admitted` (source, destination, connection, alpn,
/// cert_expires_at, clock)"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:32 — "1. The dialer's QUIC
/// connection on a registered ALPN completes; iroh has checked the peer holds the private key of
/// the endpoint id dialed"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:34 — "2. The dialer opens the
/// first bi-stream, writes the member cert it holds... and waits."
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:36 — "3. The acceptor reads it
/// within the floor and verifies as the contract says."
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:37 — "4. `admitted`: the
/// verdict is written back; the connection is served."
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:45 — "There is no wire op. The
/// exchange on the first bi-stream, as the spans record it"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:81 — "Trace propagation, step
/// spans and correlation follow the runtime spans. The cell: one admitted dial with a test-kit
/// cert and one refused by each named reason, with the connection facts beside the spans"
#[test]
fn an_inbound_connection_on_a_registered_alpn_is_admitted_on_a_valid_cert() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-admit-valid-cert").expect("the cell's evidence file can be created");
    let expiries = capture.run(4, async {
        let mut expiries = Vec::new();
        for expiry in [CHAIN_NOW_MS + 60_000, CHAIN_NOW_MS - floor_ms() + 2_000] {
            let chain = Chain::minted("mesh1", 3_600, CHAIN_NOW_MS);
            let root = chain.root.verifying_key();
            let admitted = dial_presenting(vec![root], |dialer| rdm_core::certs::cert_to_bytes(&chain.member(dialer, expiry))).await;
            assert_eq!(admitted, Ok(()), "a valid cert expiring at {expiry} is admitted");
            expiries.push(expiry);
        }
        expiries
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let product = String::from_utf8_lossy(ALPN_A).into_owned();
    let admitted: Vec<(String, String, bool, String, u64, String)> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node.connection.serve.via-admitted")
        .map(|s| {
            let conn = s["attributes"]["connection"]
                .as_u64()
                .map(|n| n > 0)
                .or_else(|| s["attributes"]["connection"].as_str().map(|v| !v.is_empty()))
                .unwrap_or(false);
            (
                s["attributes"]["source"].as_str().unwrap_or_default().to_string(),
                s["attributes"]["destination"].as_str().unwrap_or_default().to_string(),
                conn,
                s["attributes"]["alpn"].as_str().unwrap_or_default().to_string(),
                s["attributes"]["cert_expires_at"].as_str().and_then(|v| v.parse().ok()).or_else(|| s["attributes"]["cert_expires_at"].as_u64()).unwrap_or(0),
                s["attributes"]["clock"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    assert_eq!(
        admitted,
        expiries
            .iter()
            .map(|e| ("mesh1.rpc.1".to_string(), "mesh1.rpc.2".to_string(), true, product.clone(), *e, "rafka-time".to_string()))
            .collect::<Vec<_>>(),
        "each admission names source, destination, connection, alpn, the presented cert's expiry and the clock it was judged on"
    );
}

/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:28 — "Spans | ...
/// `rdm.node.connection.create.via-dial` (destination, alpn, cert_expires_at, outcome, reason,
/// elapsed_ms) on the dialer"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:34 — "2. The dialer opens the
/// first bi-stream, writes the member cert it holds (the one most recently installed: its
/// JoinNode's or its last node.cert.renew's), and waits."
#[test]
fn a_presented_dial_is_spanned_on_the_dialer_with_destination_alpn_and_the_presented_certs_expiry() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-admit-presented-fields").expect("the cell's evidence file can be created");
    let expiry = CHAIN_NOW_MS + 60_000;
    capture.run(4, async {
        let chain = Chain::minted("mesh1", 3_600, CHAIN_NOW_MS);
        let root = chain.root.verifying_key();
        let admitted = dial_presenting(vec![root], |dialer| rdm_core::certs::cert_to_bytes(&chain.member(dialer, expiry))).await;
        assert_eq!(admitted, Ok(()));
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let product = String::from_utf8_lossy(ALPN_A).into_owned();
    let dials: Vec<&serde_json::Value> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node.connection.create.via-dial")
        .collect();
    assert_eq!(dials.len(), 1, "one dial span on the dialer: {dials:#?}");
    let dial = dials[0];
    let dial_fields = (
        dial["attributes"]["destination"].as_str().unwrap_or_default().to_string(),
        dial["attributes"]["alpn"].as_str().unwrap_or_default().to_string(),
        dial["attributes"]["cert_expires_at"].as_str().and_then(|v| v.parse().ok()).or_else(|| dial["attributes"]["cert_expires_at"].as_u64()).unwrap_or(0),
        dial["attributes"]["outcome"].as_str().unwrap_or_default().to_string(),
    );
    assert_eq!(
        dial_fields,
        ("mesh1.rpc.2".to_string(), product, expiry, "connected".to_string()),
        "the dialer's via-dial span names destination, alpn, the presented cert's expiry, and outcome"
    );
    let verdicts: Vec<&serde_json::Value> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node.connection.serve.via-admitted")
        .collect();
    assert_eq!(verdicts.len(), 1, "one verdict span on the acceptor: {verdicts:#?}");
    assert_eq!(verdicts[0]["trace_id"], dial["trace_id"], "the receiver's verdict span shares the dial's trace");
    assert_eq!(verdicts[0]["parent_span_id"], dial["span_id"], "the receiver's verdict span is the dial's child in the trace");
}

/// CONTRACT (node-connections-admit.md Result; fabric-certs.md §chain 6): a member cert the issuing
/// key does not verify (its signed payload altered) is refused `bad-signature`.
#[tokio::test]
async fn an_inbound_connection_is_refused_by_name_on_bad_signature() {
    let reason = refusal_of(|chain, dialer| {
        let mut signed = chain.member(dialer, CHAIN_NOW_MS + 60_000);
        signed.cert.expiry_ms += 1;
        signed
    })
    .await;
    assert_eq!(reason, "bad-signature");
}

/// CONTRACT (node-connections-admit.md Result; fabric-certs.md §chain 1): a member cert whose CA cert
/// no root of the acceptor's set signed (another fabric's chain) is refused `ca-bad-signature`.
#[tokio::test]
async fn an_inbound_connection_is_refused_by_name_on_ca_bad_signature() {
    let other = Chain::minted("mesh1", 3_600, CHAIN_NOW_MS);
    let reason = refusal_of(move |_, dialer| other.member(dialer, CHAIN_NOW_MS + 60_000)).await;
    assert_eq!(reason, "ca-bad-signature");
}

/// CONTRACT (node-connections-admit.md Result; fabric-certs.md §chain 2, §Time): a member cert whose
/// CA cert is past its expiry by more than one floor is refused `ca-expired`, before its issuer is
/// judged.
#[tokio::test]
async fn an_inbound_connection_is_refused_by_name_on_ca_expired() {
    let reason = refusal_of(|chain, dialer| {
        let ca = rdm_core::certs::CaKey::mint(&chain.root, 1, CHAIN_NOW_MS - 2 * floor_ms() - 1_000);
        let lapsed = Chain { root: chain.root.clone(), issuer: rdm_core::certs::IssuingKey::mint(&ca, "mesh1", CHAIN_NOW_MS - 2 * floor_ms() - 1_000, ca.expiry_ms()).expect("within the CA's life"), ca };
        lapsed.member(dialer, lapsed.issuer.expiry_ms())
    })
    .await;
    assert_eq!(reason, "ca-expired");
}

/// CONTRACT (node-connections-admit.md Result; fabric-certs.md §chain 3): a member cert whose issuing
/// cert carries the fabric's CA cert but was signed by another CA is refused `issuer-bad-signature`:
/// its CA verifies to the acceptor's root, and that CA does not verify the issuing cert.
#[tokio::test]
async fn an_inbound_connection_is_refused_by_name_on_issuer_bad_signature() {
    let reason = refusal_of(|chain, dialer| {
        let stranger = rdm_core::certs::CaKey::mint(&rdm_core::certs::mint_key(), rdm_core::certs::CA_CERT_TTL_SECS, CHAIN_NOW_MS);
        let signed_by_stranger = rdm_core::certs::IssuingKey::mint(&stranger, "mesh1", CHAIN_NOW_MS, CHAIN_NOW_MS + 3_600_000).expect("within the CA's life");
        let mut forged = signed_by_stranger.cert().clone();
        forged.ca = chain.ca.cert().clone();
        let forged = Chain { root: chain.root.clone(), ca: chain.ca.clone(), issuer: rdm_core::certs::IssuingKey::from_parts(signed_by_stranger.secret().clone(), forged) };
        forged.member(dialer, CHAIN_NOW_MS + 60_000)
    })
    .await;
    assert_eq!(reason, "issuer-bad-signature");
}

/// CONTRACT (node-connections-admit.md Result; fabric-certs.md §chain 5): a member cert whose mesh is
/// not its issuer's is refused `issuer-mesh-mismatch`, before its signature is judged.
#[tokio::test]
async fn an_inbound_connection_is_refused_by_name_on_issuer_mesh_mismatch() {
    let reason = refusal_of(|chain, dialer| {
        let mut signed = chain.member(dialer, CHAIN_NOW_MS + 60_000);
        signed.cert.mesh = "mesh2".into();
        signed
    })
    .await;
    assert_eq!(reason, "issuer-mesh-mismatch");
}

/// CONTRACT (node-connections-admit.md Result; fabric-certs.md §chain 7, §Time): a member cert past
/// its expiry by more than one floor on the acceptor's rafka-time is refused `expired`.
#[tokio::test]
async fn an_inbound_connection_is_refused_by_name_on_expired() {
    let reason = refusal_of(|chain, dialer| chain.member(dialer, CHAIN_NOW_MS - floor_ms() - 1)).await;
    assert_eq!(reason, "expired");
}

/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:28 — "Spans |
/// `rdm.node.connection.serve.via-admitted` (source, destination, connection, alpn,
/// cert_expires_at, clock) or `rdm.node.connection.reject.via-refused` (the same, plus `reason`)
/// on the acceptor"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:37 — "`refused { reason }`:
/// the verdict is written back, the `Refused` fact is written, the connection is closed."
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:39 — "5. The dialer reads the
/// verdict before it sends anything else: a refusal is a named end of this dial"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:45 — "There is no wire op. The
/// exchange on the first bi-stream, as the spans record it"
/// CONTRACT: docs/architecture/lifecycles/node-connections-admit.md:81 — "Trace propagation, step
/// spans and correlation follow the runtime spans. The cell: one admitted dial with a test-kit
/// cert and one refused by each named reason, with the connection facts beside the spans"
#[test]
fn a_refused_dial_is_spanned_with_the_presented_certs_expiry_and_the_clock_it_was_judged_on() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-admit-refused-fields").expect("the cell's evidence file can be created");
    let expiry = CHAIN_NOW_MS - 2 * floor_ms() - 1_000;
    let carrier_expiry = CHAIN_NOW_MS + 90_000;
    capture.run(4, async {
        let chain = Chain::minted("mesh1", 3_600, CHAIN_NOW_MS);
        let root = chain.root.verifying_key();
        let refused = dial_presenting(vec![root], |dialer| rdm_core::certs::cert_to_bytes(&chain.member(dialer, expiry))).await;
        assert_eq!(refused, Err(ConnectError::Refused { reason: "expired".into() }), "a cert expired by more than a floor is refused `expired`");

        // Also verify Proxy refusals (at the carrier, and at the destination through the carrier)
        // end the caller's via-dial span with outcome="refused" and reason, and record
        // cert_expires_at on the carrier's via-tunnel span.
        let alpns = [gateway_alpn(ALPN_A)];
        let source = TestPeer::bind("mesh1.rpc.3", &alpns).await;
        let carrier = TestPeer::bind("mesh1.gateway.1", &alpns).await;
        let dest = TestPeer::bind("mesh1.rpc.4", &alpns).await;
        source.know(&carrier);
        carrier.know(&dest);
        carrier.know(&source);
        dest.know(&carrier);
        seed_carrier_direct_edge(&[&source.sink, &carrier.sink], &carrier.birth, &dest.birth, ALPN_A).await;
        source.registry.set_member_cert(rdm_core::certs::cert_to_bytes(&chain.member(&source, expiry)));
        carrier.registry.set_member_cert(rdm_core::certs::cert_to_bytes(&chain.member(&carrier, carrier_expiry)));
        let carrier_admitter = Arc::new(PolicyAdmitter { allow: AtomicBool::new(false), calls: AtomicUsize::new(0) });
        let dest_admitter = Arc::new(PolicyAdmitter { allow: AtomicBool::new(false), calls: AtomicUsize::new(0) });
        carrier.registry.set_admitter(carrier_admitter.clone());
        dest.registry.set_admitter(dest_admitter.clone());
        let (dialer_s, _) = source.registry.connections(ALPN_A).unwrap().split();
        assert_eq!(dialer_s.open(&dest.birth).await.unwrap_err(), ConnectError::Refused { reason: "expired member cert".into() });
        *source.sink.held().lock().unwrap() = rdm_core::connections::ConnectionsHeld::new();
        carrier_admitter.allow.store(true, Ordering::SeqCst);
        assert_eq!(dialer_s.open(&dest.birth).await.unwrap_err(), ConnectError::Refused { reason: "expired member cert".into() });
        source.shutdown().await;
        carrier.shutdown().await;
        dest.shutdown().await;
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let product = String::from_utf8_lossy(ALPN_A).into_owned();
    let refused_spans: Vec<&serde_json::Value> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node.connection.reject.via-refused" && s["attributes"]["destination"] == "mesh1.rpc.2")
        .collect();
    let refused: Vec<(String, String, bool, String, u64, String, String)> = refused_spans
        .iter()
        .map(|s| {
            let conn = s["attributes"]["connection"]
                .as_u64()
                .map(|n| n > 0)
                .or_else(|| s["attributes"]["connection"].as_str().map(|v| !v.is_empty()))
                .unwrap_or(false);
            (
                s["attributes"]["source"].as_str().unwrap_or_default().to_string(),
                s["attributes"]["destination"].as_str().unwrap_or_default().to_string(),
                conn,
                s["attributes"]["alpn"].as_str().unwrap_or_default().to_string(),
                s["attributes"]["cert_expires_at"].as_str().and_then(|v| v.parse().ok()).or_else(|| s["attributes"]["cert_expires_at"].as_u64()).unwrap_or(0),
                s["attributes"]["clock"].as_str().unwrap_or_default().to_string(),
                s["attributes"]["reason"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    assert_eq!(
        refused,
        vec![("mesh1.rpc.1".to_string(), "mesh1.rpc.2".to_string(), true, product.clone(), expiry, "rafka-time".to_string(), "expired".to_string())],
        "the refusal names source, destination, connection, alpn, the presented cert's expiry, the clock it was judged on and the reason"
    );
    let direct_dial = spans
        .iter()
        .find(|s| s["name"] == "rdm.node.connection.create.via-dial" && s["attributes"]["destination"] == "mesh1.rpc.2")
        .expect("the direct dial span");
    assert_eq!(direct_dial["attributes"]["outcome"], "refused");
    assert_eq!(direct_dial["attributes"]["reason"], "expired");
    assert_eq!(refused_spans[0]["trace_id"], direct_dial["trace_id"], "the refusal verdict span shares the dial's trace");
    assert_eq!(refused_spans[0]["parent_span_id"], direct_dial["span_id"], "the refusal verdict span is the dial's child in the trace");

    let proxy_dials: Vec<_> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node.connection.create.via-dial" && (s["attributes"]["destination"] == "mesh1.gateway.1" || s["attributes"]["destination"] == "mesh1.rpc.4"))
        .collect();
    assert_eq!(proxy_dials.len(), 3, "two caller dials to carrier and one carrier dial to destination: {proxy_dials:#?}");
    assert!(
        proxy_dials.iter().all(|s| s["attributes"]["outcome"] == "refused" && s["attributes"]["reason"] == "expired member cert"),
        "every refused dial (Direct, Proxy, and through a carrier) ends with outcome=refused and reason: {proxy_dials:#?}"
    );
    let carrier_tunnel = spans
        .iter()
        .find(|s| s["name"] == "rdm.node.connection.create.via-tunnel" && s["attributes"]["carrier"] == "mesh1.gateway.1" && s["attributes"]["cert_expires_at"].as_str().and_then(|v| v.parse::<u64>().ok()).or_else(|| s["attributes"]["cert_expires_at"].as_u64()) == Some(carrier_expiry))
        .expect("the carrier's via-tunnel span carries cert_expires_at");
    assert_eq!(carrier_tunnel["attributes"]["outcome"], "refused: expired member cert");
}

/// CONTRACT (node-connections-admit.md Result; fabric-certs.md §chain 4, §Time): a member cert whose
/// issuing cert is past its expiry by more than one floor is refused `issuer-expired`.
#[tokio::test]
async fn an_inbound_connection_is_refused_by_name_on_issuer_expired() {
    let reason = refusal_of(|chain, dialer| {
        let lapsed = Chain { root: chain.root.clone(), ca: chain.ca.clone(), issuer: rdm_core::certs::IssuingKey::mint(&chain.ca, "mesh1", chain.issuer.cert().cert.issued_ms, CHAIN_NOW_MS - 2 * floor_ms()).expect("within the CA's life") };
        lapsed.member(dialer, lapsed.issuer.expiry_ms())
    })
    .await;
    assert_eq!(reason, "issuer-expired");
}

/// CONTRACT (fabric-certs.md §Custody: a root rotation refreshes "the public set to every other node"
/// through `node.roots.update`; fabric-ca-renew.md): the acceptor's root set is one holder its
/// admissions read afresh. A cert of a second root is refused `ca-bad-signature` while the set
/// holds only the first; once the holder is replaced by both, the next admission admits it, and once
/// replaced by the second alone, a cert of the first is refused.
#[tokio::test]
async fn a_root_set_replaced_at_runtime_is_read_by_the_next_admission() {
    let (first, second) = (Chain::minted("mesh1", 3_600, CHAIN_NOW_MS), Chain::minted("mesh1", 3_600, CHAIN_NOW_MS));
    let dialer = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let acceptor = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    dialer.know(&acceptor);
    acceptor.know(&dialer);
    acceptor.registry.admit_on(vec![first.root.verifying_key()], chain_time());
    let (opener, _) = dialer.registry.connections(ALPN_A).unwrap().split();
    let (_, mut accepts) = acceptor.registry.connections(ALPN_A).unwrap().split();
    let served = tokio::spawn(async move {
        let mut n = 0;
        while accepts.accept().await.is_some() {
            n += 1;
        }
        n
    });

    dialer.registry.set_member_cert(rdm_core::certs::cert_to_bytes(&second.member(&dialer, CHAIN_NOW_MS + 60_000)));
    assert_eq!(opener.open(&acceptor.birth).await.unwrap_err(), ConnectError::Refused { reason: "ca-bad-signature".into() }, "the second root is not in the set");

    let holder = acceptor.registry.roots();
    holder.replace(vec![first.root.verifying_key(), second.root.verifying_key()]);
    assert_eq!(acceptor.registry.roots().current().len(), 2, "the registry hands out the one holder");
    assert!(dialer.registry.probe_direct(&acceptor.birth, ALPN_A).await.is_ok(), "the next admission is judged on the replaced set: the second root's cert is admitted");

    holder.replace(vec![second.root.verifying_key()]);
    dialer.registry.set_member_cert(rdm_core::certs::cert_to_bytes(&first.member(&dialer, CHAIN_NOW_MS + 60_000)));
    match dialer.registry.probe_direct(&acceptor.birth, ALPN_A).await {
        Err(ConnectError::Refused { reason }) => assert_eq!(reason, "ca-bad-signature", "the first root left the set"),
        other => panic!("a cert of a root no longer in the set must be refused: {other:?}"),
    }
    dialer.shutdown().await;
    acceptor.shutdown().await;
    served.abort();
}

/// CONTRACT (node-connections-admit.md `NoCerts`: "A node-admin started `NoCerts` admits every
/// connection by name, spanned `via-no-certs-configured`"): an acceptor admitting on no root set (its
/// launcher holds no certs) admits a dialer whatever it presents, a cert of no fabric and empty bytes
/// alike. Each admission is spanned `rdm.node.connection.serve.via-no-certs-configured` (source,
/// destination, alpn) and never `via-admitted`: no cert was judged, so none is named admitted.
#[test]
fn a_no_certs_acceptor_admits_by_name() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-admit-no-certs").expect("the cell's evidence file can be created");
    let (dialer_name, acceptor_name) = capture.run(4, async {
        let mut names = (String::new(), String::new());
        for some_cert in [true, false] {
            let dialer = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
            let presented = if some_cert { rdm_core::certs::cert_to_bytes(&Chain::minted("mesh1", 3_600, CHAIN_NOW_MS).member(&dialer, CHAIN_NOW_MS + 60_000)) } else { Vec::new() };
            let acceptor = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
            dialer.know(&acceptor);
            acceptor.know(&dialer);
            acceptor.registry.admit_on(vec![], chain_time());
            dialer.registry.set_member_cert(presented.clone());
            let (opener, _) = dialer.registry.connections(ALPN_A).unwrap().split();
            let (_, mut accepts) = acceptor.registry.connections(ALPN_A).unwrap().split();
            let accepted = tokio::spawn(async move { accepts.accept().await.map(|c| c.id().clone()) });
            let conn = opener.open(&acceptor.birth).await.unwrap_or_else(|e| panic!("a no-certs acceptor admits {} presented bytes: {e:?}", presented.len()));
            let served = tokio::time::timeout(Duration::from_secs(10), accepted).await.expect("the admitted connection reaches accept").unwrap().unwrap();
            assert_eq!(&served, conn.id());
            names = (dialer.birth.name.to_string(), acceptor.birth.name.to_string());
            dialer.shutdown().await;
            acceptor.shutdown().await;
        }
        names
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let named = |name: &str| spans.iter().filter(|s| s["name"] == name && s["attributes"]["destination"] == acceptor_name.as_str()).collect::<Vec<_>>();
    let by_name = named("rdm.node.connection.serve.via-no-certs-configured");
    assert_eq!(by_name.len(), 2, "each admission is spanned by name: {by_name:#?}");
    let product = String::from_utf8_lossy(ALPN_A).into_owned();
    assert!(by_name.iter().all(|s| s["attributes"]["source"] == dialer_name.as_str() && s["attributes"]["alpn"] == product.as_str()), "{by_name:#?}");
    assert_eq!(named("rdm.node.connection.serve.via-admitted"), Vec::<&serde_json::Value>::new(), "no cert was judged admitted");
}

/// CONTRACT (fabric-certs.md §On a connection: "RDM never registers the step for its own two ALPNs: a
/// registration naming `rdm_transport::ALPN` or `iroh_gossip::ALPN` is refused by name at `bind`";
/// node-connections-admit.md): binding the app's protocols on an endpoint with either of RDM's own
/// ALPNs among them is refused, naming which, and no registry is made.
#[tokio::test]
async fn registering_the_step_for_node_rpc_or_gossip_is_refused_at_bind() {
    let ep = rdm_transport::endpoint::bind_with_alpns(SecretKey::generate(), "127.0.0.1:0".parse().unwrap(), &[ALPN_A.to_vec()]).await.unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let sink = Arc::new(MemoryConnectionsSink::new(Birth { name: "mesh1.rpc.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() }));
    for (own, says) in [(rdm_transport::ALPN, "RDM's own Node RPC ALPN"), (iroh_gossip::ALPN, "RDM's own gossip ALPN")] {
        let refused = ConnectionsRegistry::new(ep.clone(), resolver.clone(), sink.clone(), &[alpn(ALPN_A), alpn(own)], Vec::new()).err().unwrap_or_else(|| panic!("binding {} must be refused", String::from_utf8_lossy(own)));
        assert!(refused.contains(says) && refused.contains(&*String::from_utf8_lossy(own)), "the refusal names the ALPN and whose it is: {refused}");
    }
    ep.close().await;
}

/// Bind a live dialer admitted to an acceptor, then refuse every later dial (the policy admitter
/// turned off): returns the acceptor, the admitter, and the live dialer kept so its edge stays up.
async fn an_acceptor_with_one_live_edge() -> (TestPeer, Arc<PolicyAdmitter>, TestPeer, impl Sized) {
    let live = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
    let acceptor = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
    live.know(&acceptor);
    acceptor.know(&live);
    let admitter = Arc::new(PolicyAdmitter { allow: AtomicBool::new(true), calls: AtomicUsize::new(0) });
    acceptor.registry.set_admitter(admitter.clone());
    let (opener, _) = live.registry.connections(ALPN_A).unwrap().split();
    let (_, mut accepts) = acceptor.registry.connections(ALPN_A).unwrap().split();
    let accepted = tokio::spawn(async move { accepts.accept().await.map(|c| c.id().clone()) });
    let conn = opener.open(&acceptor.birth).await.expect("the live birth is admitted");
    tokio::time::timeout(Duration::from_secs(10), accepted).await.expect("the admitted connection reaches accept").unwrap().unwrap();
    admitter.allow.store(false, Ordering::SeqCst);
    (acceptor, admitter, live, conn)
}

/// A dial from a key the acceptor's topology view does not name, presenting `name`: refused, and
/// the acceptor's `Refused` spans' sources and its connection rows after it.
async fn an_unknown_key_dials(acceptor: &TestPeer, name: &str) -> Vec<NodeConnection> {
    let stranger = TestPeer::bind(name, &[alpn(ALPN_A)]).await;
    stranger.know(acceptor);
    let (opener, _) = stranger.registry.connections(ALPN_A).unwrap().split();
    assert!(matches!(opener.open(&acceptor.birth).await, Err(ConnectError::Refused { .. })), "a dial from a key no view names is refused");
    tokio::time::sleep(Duration::from_millis(200)).await;
    stranger.shutdown().await;
    acceptor.sink.history()
}

/// The `rdm.node.connection.reject.via-refused` spans' `source` attributes.
fn refused_sources(spans: &[serde_json::Value]) -> Vec<String> {
    spans.iter().filter(|s| s["name"] == "rdm.node.connection.reject.via-refused").map(|s| s["attributes"]["source"].as_str().unwrap_or_default().to_string()).collect()
}

/// CONTRACT (node-connections-admit.md Row and Never, Q212): a dialer whose key no topology view
/// names is refused with the span only and no row, even when it presents a live birth's name: that
/// birth's live edge is left exactly as it was (no row written, so nothing in its key changes).
#[test]
fn an_unknown_key_presenting_a_live_births_name_writes_no_row_and_leaves_that_births_edge() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-refused-unknown-key-live-name").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (acceptor, _admitter, live, _conn) = an_acceptor_with_one_live_edge().await;
        // The live edge's fact is the source's own Direct Connected (connections.md §13 G): the live
        // dialer's row; the acceptor writes no row for an admission.
        let (before, edge_before) = (acceptor.sink.history(), live.sink.history());
        assert!(edge_before.iter().any(|r| r.destination.name == acceptor.birth.name && r.state == ConnectionState::Connected), "the live birth's edge is held: {edge_before:#?}");
        let after = an_unknown_key_dials(&acceptor, "mesh1.rpc.1").await;
        assert_eq!(after, before, "no row is written for a key no view names");
        assert_eq!(live.sink.history(), edge_before, "the live birth's edge is unchanged");
        acceptor.shutdown().await;
        live.shutdown().await;
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    let sources = refused_sources(&spans);
    assert_eq!(sources.len(), 1, "one refusal span: {sources:?}");
    assert_ne!(sources[0], "mesh1.rpc.1", "the span names the key, never the name the dialer presented");
}

/// CONTRACT (node-connections-admit.md Row, Q212): a dialer whose key no topology view names,
/// presenting a birth nobody holds, is refused with the span only: no row.
#[test]
fn an_unknown_key_presenting_an_unknown_birth_writes_no_row_only_the_span() {
    let capture = rdm_transport::EvidenceCapture::start(&crate::evidence_dir(), "rdm-transport-refused-unknown-key-unknown-birth").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let (acceptor, _admitter, live, _conn) = an_acceptor_with_one_live_edge().await;
        let before = acceptor.sink.history();
        let after = an_unknown_key_dials(&acceptor, "mesh1.rpc.9").await;
        assert_eq!(after, before, "no row for a key no view names");
        acceptor.shutdown().await;
        live.shutdown().await;
    });
    let spans = capture.spans().expect("the cell's spans reached its evidence file");
    assert_eq!(refused_sources(&spans).len(), 1, "the refusal is its span");
}

/// The trace id of a W3C `traceparent`.
fn trace_id_of(traceparent: Option<&str>) -> Option<String> {
    traceparent.and_then(|tp| tp.split('-').nth(1)).map(str::to_string)
}

/// CONTRACT (telemetry.md §3, the accepted connection; Q231): the accepted `Connection` hands the
/// application its opener's W3C context, from the connection's present frame, Direct and Proxy alike.
/// An application span made from it is the open's child: it shares the opener's trace, and the
/// opener's `tracestate` comes through as it was. Before Q231 the accepted connection carried none and
/// Rafka's accept span was a root of its own (be010064f578409b97250f412d17066a, the tunnel's trace
/// 72a69346726c83a3aea26343b613c88a).
#[test]
fn an_application_span_made_from_an_accepted_connection_shares_the_openers_trace() {
    let dir = crate::evidence_dir();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rdm-connection-accepted-trace").expect("the cell's evidence file can be created");
    capture.run(4, async {
        // The opener's span carries a tracestate of its own: an application's request carried in.
        let opener = |route: &'static str| {
            let request = tracing::info_span!("app.connection.request", route);
            let open = tracing::info_span!("app.connection.open", route);
            rdm_transport::set_remote_parent(&open, &rdm_transport::traceparent_of(&request).expect("the capture's layer gives the request a context"), Some("rafka=open-1"));
            open
        };
        let accepted_span = |conn: &rdm_transport::connections::Connection, route: &'static str| {
            let span = tracing::info_span!("app.connection.accept", route);
            if let Some(tp) = conn.traceparent() {
                rdm_transport::set_remote_parent(&span, tp, conn.tracestate());
            }
            span.in_scope(|| tracing::info!("the application takes the accepted connection"));
            span
        };
        let mut lines = String::new();

        // Direct.
        let a = TestPeer::bind("mesh1.rpc.1", &[alpn(ALPN_A)]).await;
        let b = TestPeer::bind("mesh1.rpc.2", &[alpn(ALPN_A)]).await;
        a.know(&b);
        b.know(&a);
        let (dialer_a, _) = a.registry.connections(ALPN_A).unwrap().split();
        let (_, mut acceptor_b) = b.registry.connections(ALPN_A).unwrap().split();
        let accept_b = tokio::spawn(async move { acceptor_b.accept().await.unwrap() });
        let open_direct = opener("direct");
        let opened_direct = rdm_transport::traceparent_of(&open_direct);
        let _conn_a = tracing::Instrument::instrument(dialer_a.open(&b.birth), open_direct.clone()).await.expect("the Direct open");
        let conn_b = accept_b.await.unwrap();
        assert_eq!(conn_b.kind(), ConnectionKind::Direct);
        let app_direct = accepted_span(&conn_b, "direct");
        let app_direct_tp = rdm_transport::traceparent_of(&app_direct);
        assert_eq!(trace_id_of(app_direct_tp.as_deref()), trace_id_of(opened_direct.as_deref()), "the application's span at the accepted Direct connection shares the opener's trace: the connection carried {:?}", conn_b.traceparent());
        assert_eq!(conn_b.tracestate(), Some("rafka=open-1"), "the opener's tracestate reaches the accepted Direct connection as it was");
        lines.push_str(&format!("direct {}\n", trace_id_of(opened_direct.as_deref()).unwrap_or_default()));

        // Proxy, through the carrier.
        let alpns = [gateway_alpn(ALPN_B)];
        let s = TestPeer::bind("mesh1.rpc.3", &alpns).await;
        let g = TestPeer::bind("mesh1.gateway.1", &alpns).await;
        let d = TestPeer::bind("mesh1.rpc.4", &alpns).await;
        s.know(&g);
        g.know(&d);
        seed_carrier_direct_edge(&[&s.sink, &g.sink], &g.birth, &d.birth, ALPN_B).await;
        let (dialer_s, _) = s.registry.connections(ALPN_B).unwrap().split();
        let (_, mut acceptor_d) = d.registry.connections(ALPN_B).unwrap().split();
        let accept_d = tokio::spawn(async move { acceptor_d.accept().await.unwrap() });
        let open_proxy = opener("proxy");
        let opened_proxy = rdm_transport::traceparent_of(&open_proxy);
        let _conn_s = tracing::Instrument::instrument(dialer_s.open(&d.birth), open_proxy.clone()).await.expect("the Proxy open");
        let conn_d = accept_d.await.unwrap();
        assert_eq!(conn_d.kind(), ConnectionKind::Proxy { via: g.birth.clone() });
        let app_proxy = accepted_span(&conn_d, "proxy");
        let app_proxy_tp = rdm_transport::traceparent_of(&app_proxy);
        assert_eq!(trace_id_of(app_proxy_tp.as_deref()), trace_id_of(opened_proxy.as_deref()), "the application's span at the accepted Proxy connection shares the opener's trace: the connection carried {:?}", conn_d.traceparent());
        assert_eq!(conn_d.tracestate(), Some("rafka=open-1"), "the opener's tracestate reaches the accepted Proxy connection through the carrier as it was");
        lines.push_str(&format!("proxy {}\n", trace_id_of(opened_proxy.as_deref()).unwrap_or_default()));

        drop((app_direct, app_proxy, open_direct, open_proxy));
        for p in [a, b, s, g, d] {
            p.shutdown().await;
        }
        std::fs::write(dir.join("connection-accepted-trace-ids.txt"), lines).unwrap();
    });
}
