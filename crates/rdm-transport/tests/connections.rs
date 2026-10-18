use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::connections::{
    ConnectionKind as CoreConnectionKind, ConnectionState, NodeConnection,
};
use rdm_core::reconnect::owed_retirements;
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
        forwardable: true,
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
async fn a_working_alpn_a_proves_nothing_for_alpn_b_and_a_reconnect_on_a_never_retires_bs_proxy() {
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
    peer_a.sink.record_row(proxy_on_b.clone()).await.unwrap();

    let (dialer_a_on_a, _) = peer_a.registry.connections(ALPN_A).unwrap().split();
    let (_dialer_b, mut acceptor_b_on_a) = peer_b.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acceptor_b_on_a.accept().await.unwrap() });
    let _conn_a = dialer_a_on_a.open(&peer_b.birth).await.unwrap();
    let _conn_b = accept_task.await.unwrap();

    // Direct Connected on ALPN_A must NOT retire the Proxy on ALPN_B:
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
            owed_retirements(&held, 999).is_empty(),
            "owed_retirements must not retire ALPN_B's Proxy when only ALPN_A has Direct Connected"
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

    // Flip admitter to allow -> open succeeds and reaches accept().
    admitter.allow.store(true, Ordering::SeqCst);
    let accept_task = tokio::spawn(async move { acceptor_b.accept().await.unwrap() });
    let conn_a = dialer_a.open(&peer_b.birth).await.unwrap();
    let conn_b = accept_task.await.unwrap();
    assert_eq!(conn_a.id(), conn_b.id());

    peer_a.shutdown().await;
    peer_b.shutdown().await;
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

#[tokio::test]
async fn a_dial_parents_to_the_callers_span_and_the_stream_bytes_are_not_spanned() {
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
    let presented_span = spans_after_open
        .iter()
        .find(|(_, name, _)| name == "rdm.node.connections.update.via-presented")
        .unwrap_or_else(|| {
            panic!(
                "rdm.node.connections.update.via-presented span not in {:?}",
                spans_after_open
            )
        });
    assert_eq!(
        presented_span.2.as_ref(),
        Some(&caller_id),
        "via-presented span must parent to the caller's span"
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
}
