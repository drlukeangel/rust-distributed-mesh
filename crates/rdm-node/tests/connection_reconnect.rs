//! i148 e3 acceptance cells (`connections.md` §9, §10, §13 A–D):
//! - `one_discovered_proxy_serves_every_consumer_of_the_same_destination` (§13 A)
//! - `a_proxy_survives_source_restart` (§13 B)
//! - `a_failed_reconnect_backoff_survives_restart` (§13 C, Demo row 6)
//! - `the_connections_index_stays_bounded_while_the_raw_log_keeps_history` (§13 D)
//! - `one_reconnect_attempt_at_a_time_per_node` (§9.4, §9.5)

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::connections::{
    ConnectionEnd, ConnectionKind as CoreConnectionKind, ConnectionState, ConnectionsHeld,
    DirectRecovery, NodeConnection,
};
use rdm_core::reconnect::{deterministic_jitter, next_due, reconnect_backoff};
use rdm_core::{ConnectionId, IncarnationId, NodeId, NodeKind};
use rdm_node::connection_reconnect::{
    AlpnRecoveryDial, Reconciler, ReconnectObligation, obligations, registry_recovery_dial,
    run_due_at, spawn_alpn_reconciler,
};
use rdm_node::connections_writer::ConnectionsWriter;
use rdm_node::storage::{ConnectionsStorage, FileConnectionsStorage};
use rdm_node::store_root::StoreRoot;
use rdm_transport::connections::{Admitter, Alpn, Birth, ConnectError, ConnectionKind, ConnectionsRegistry};
use rdm_transport::{ResolvedNode, StaticResolver};

const ALPN_A: &[u8] = b"test-reconnect-a/1";
const ALPN_B: &[u8] = b"test-reconnect-b/1";

fn forwardable_alpn(name: &[u8]) -> Alpn {
    Alpn {
        name: name.to_vec(),
        forwardable: true,
        carriers: vec![NodeKind::Gateway],
    }
}

fn temp_data_dir(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("i148-e3-{tag}-{}", NodeId::mint()))
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

async fn until(what: &str, cond: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(15);
    while !cond() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting for: {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

struct DurableNode {
    birth: Birth,
    resolved: ResolvedNode,
    resolver: Arc<StaticResolver>,
    storage: Arc<dyn ConnectionsStorage>,
    held: Arc<Mutex<ConnectionsHeld>>,
    writer: Arc<ConnectionsWriter>,
    registry: ConnectionsRegistry,
    router: Router,
}

impl DurableNode {
    async fn boot(dir: &std::path::Path, birth: Birth, alpns: &[Alpn]) -> Self {
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
        let resolved = ResolvedNode {
            node_id: birth.node_id.clone(),
            name: birth.name.clone(),
            endpoint_id: key.public(),
            transport_addr: addr,
            incarnation: birth.incarnation.clone(),
        };
        let resolver = Arc::new(StaticResolver::new());
        let storage: Arc<dyn ConnectionsStorage> = Arc::new(
            FileConnectionsStorage::open(
                &StoreRoot::at(dir),
                &birth.name, &rdm_node::store_log::LogWriter::new(&birth.node_id, &birth.incarnation),
            )
            .await
            .unwrap(),
        );
        let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
        let writer = Arc::new(ConnectionsWriter::new(
            birth.to_end(),
            storage.clone(),
            held.clone(),
        ));
        writer.hydrate().await.unwrap();
        let (registry, handlers) = ConnectionsRegistry::new(
            ep.clone(),
            resolver.clone(),
            writer.clone(),
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
            storage,
            held,
            writer,
            registry,
            router,
        }
    }

    async fn bind_fresh(dir: &std::path::Path, name: &str, alpns: &[Alpn]) -> Self {
        let birth = Birth {
            name: name.parse().unwrap(),
            node_id: NodeId::mint(),
            incarnation: IncarnationId::mint(),
        };
        Self::boot(dir, birth, alpns).await
    }

    fn know(&self, other: &DurableNode) {
        self.resolver.insert(other.resolved.clone());
    }

    async fn shutdown(self) {
        let _ = self.router.shutdown().await;
    }
}

async fn establish_carrier_direct(
    carrier: &DurableNode,
    dest: &DurableNode,
    acc_dest: &mut rdm_transport::connections::Acceptor,
    alpn: &[u8],
) {
    let dest_birth = dest.birth.clone();
    let reg = carrier.registry.clone();
    let alpn_vec = alpn.to_vec();
    let dial_task = tokio::spawn(async move { reg.probe_direct(&dest_birth, &alpn_vec).await.unwrap() });
    let _srv = acc_dest.accept().await.unwrap();
    let conn = dial_task.await.unwrap();
    assert_eq!(conn.kind(), ConnectionKind::Direct);
    carrier.writer.observations_landed().await;
}

/// §13 A: one discovered Proxy serves every consumer of the same destination without paying the
/// direct dial ladder again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_discovered_proxy_serves_every_consumer_of_the_same_destination() {
    let dir = temp_data_dir("13a");
    let alpns = [forwardable_alpn(ALPN_A)];
    let gw1 = DurableNode::bind_fresh(&dir, "mesh1.gateway.1", &alpns).await;
    let gw2 = DurableNode::bind_fresh(&dir, "mesh1.gateway.2", &alpns).await;
    let brk2 = DurableNode::bind_fresh(&dir, "mesh1.broker.2", &alpns).await;

    let (dialer_consumer_1, _acc_gw1) = gw1.registry.connections(ALPN_A).unwrap().split();
    let dialer_consumer_2 = dialer_consumer_1.clone();
    let (_dialer_brk2, mut acc_brk2) = brk2.registry.connections(ALPN_A).unwrap().split();

    // gw1 can reach gw2, and gw2 opens an active Direct edge to brk2 on the shared store root;
    // gw1 cannot reach brk2 Direct:
    gw1.know(&gw2);
    gw2.know(&brk2);
    establish_carrier_direct(&gw2, &brk2, &mut acc_brk2, ALPN_A).await;

    // Consumer 1's first operation discovers and writes Proxy Connected via gw2 after one failed Direct dial:
    let accept_1 = tokio::spawn(async move {
        let c = acc_brk2.accept().await.unwrap();
        (c, acc_brk2)
    });
    let conn_1 = dialer_consumer_1.open(&brk2.birth).await.unwrap();
    let (srv_1, mut acc_brk2) = accept_1.await.unwrap();
    assert_eq!(
        conn_1.kind(),
        ConnectionKind::Proxy {
            via: gw2.birth.clone()
        }
    );

    let history_after_first = gw1.storage.history().await.unwrap();
    let failed_directs_after_first: Vec<_> = history_after_first
        .iter()
        .filter(|r| r.kind == CoreConnectionKind::Direct && r.state == ConnectionState::Failed)
        .collect();
    assert_eq!(
        failed_directs_after_first.len(),
        1,
        "first operation paid exactly one Direct Failed attempt before proving Proxy"
    );

    // Consumer 2 (e.g. a second topic selecting the same brk2) opens a connection: must use the Proxy
    // immediately with NO second direct dial attempt:
    let accept_2 = tokio::spawn(async move {
        let c = acc_brk2.accept().await.unwrap();
        (c, acc_brk2)
    });
    let conn_2 = dialer_consumer_2.open(&brk2.birth).await.unwrap();
    let (srv_2, _acc_brk2) = accept_2.await.unwrap();
    assert_eq!(
        conn_2.kind(),
        ConnectionKind::Proxy {
            via: gw2.birth.clone()
        }
    );

    // Verify end-to-end payload transfer on both consumers' connections:
    for (c_client, c_server, msg) in [(&conn_1, &srv_1, b"topic-a"), (&conn_2, &srv_2, b"topic-b")] {
        let (mut s, mut r) = c_client.open_bi().await.unwrap();
        s.write_all(msg).await.unwrap();
        s.finish().unwrap();
        let (mut ss, mut sr) = c_server.accept_bi().await.unwrap();
        assert_eq!(sr.read_to_end(64).await.unwrap(), msg);
        ss.write_all(b"ack").await.unwrap();
        ss.finish().unwrap();
        assert_eq!(r.read_to_end(64).await.unwrap(), b"ack");
    }

    let history_after_second = gw1.storage.history().await.unwrap();
    let failed_directs_after_second: Vec<_> = history_after_second
        .iter()
        .filter(|r| r.kind == CoreConnectionKind::Direct && r.state == ConnectionState::Failed)
        .collect();
    assert_eq!(
        failed_directs_after_second.len(),
        1,
        "forbidden: another direct dial ladder when a valid Proxy is already held"
    );

    gw1.shutdown().await;
    gw2.shutdown().await;
    brk2.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// §13 B: a proven Proxy survives a source restart; the first operation after hydration uses the
/// Proxy without rediscovering it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_proxy_survives_source_restart() {
    let dir = temp_data_dir("13b");
    let alpns = [forwardable_alpn(ALPN_A)];
    let gw1_first_birth = Birth {
        name: "mesh1.gateway.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
    };
    let gw2 = DurableNode::bind_fresh(&dir, "mesh1.gateway.2", &alpns).await;
    let brk2 = DurableNode::bind_fresh(&dir, "mesh1.broker.2", &alpns).await;
    let (_, mut acc_brk2) = brk2.registry.connections(ALPN_A).unwrap().split();
    gw2.know(&brk2);
    establish_carrier_direct(&gw2, &brk2, &mut acc_brk2, ALPN_A).await;

    // First birth of gw1 discovers and persists Direct Failed (E1, 1) and Proxy Connected (via=gw2)
    // from the shared store root without any hand-fed row:
    let mut acc_brk2 = {
        let gw1 = DurableNode::boot(&dir, gw1_first_birth.clone(), &alpns).await;
        gw1.know(&gw2);
        let (dialer_gw1, _) = gw1.registry.connections(ALPN_A).unwrap().split();
        let accept_task = tokio::spawn(async move {
            let c = acc_brk2.accept().await.unwrap();
            (c, acc_brk2)
        });
        let conn = dialer_gw1.open(&brk2.birth).await.unwrap();
        let (_srv, acc_brk2) = accept_task.await.unwrap();
        assert_eq!(
            conn.kind(),
            ConnectionKind::Proxy {
                via: gw2.birth.clone()
            }
        );
        gw1.writer.observations_landed().await;
        gw1.shutdown().await;
        acc_brk2
    };

    // Restart gw1 at a new incarnation over the same durable store:
    let gw1_restarted_birth = Birth {
        name: gw1_first_birth.name.clone(),
        node_id: gw1_first_birth.node_id.clone(),
        incarnation: IncarnationId::mint(),
    };
    let gw1 = DurableNode::boot(&dir, gw1_restarted_birth, &alpns).await;
    gw1.know(&gw2);

    let history_before_open = gw1.storage.history().await.unwrap().len();
    let (dialer_gw1, _) = gw1.registry.connections(ALPN_A).unwrap().split();

    let accept_task = tokio::spawn(async move { acc_brk2.accept().await.unwrap() });
    let conn = dialer_gw1.open(&brk2.birth).await.unwrap();
    let srv = accept_task.await.unwrap();
    assert_eq!(
        conn.kind(),
        ConnectionKind::Proxy {
            via: gw2.birth.clone()
        },
        "first operation after restart uses the hydrated Proxy"
    );
    assert_eq!(
        gw1.storage.history().await.unwrap().len(),
        history_before_open,
        "using the hydrated Proxy writes no new Direct Failed or Proxy Connected rediscovery row"
    );

    let (mut s, mut r) = conn.open_bi().await.unwrap();
    s.write_all(b"after-restart").await.unwrap();
    s.finish().unwrap();
    let (mut ss, mut sr) = srv.accept_bi().await.unwrap();
    assert_eq!(sr.read_to_end(64).await.unwrap(), b"after-restart");
    ss.write_all(b"ok").await.unwrap();
    ss.finish().unwrap();
    assert_eq!(r.read_to_end(64).await.unwrap(), b"ok");

    gw1.shutdown().await;
    gw2.shutdown().await;
    brk2.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// §13 C and Demo acceptance row 6: a failed reconnect backoff survives source restart; restarting
/// before `next_due` makes no early attempt, and when `next_due` arrives the right ALPN resumes
/// its obligation at the persisted ordinal (never resetting to attempt 1) with no duplicate attempt.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_reconnect_backoff_survives_restart() {
    let dir = temp_data_dir("13c");
    let alpns = [forwardable_alpn(ALPN_A), forwardable_alpn(ALPN_B)];
    let gw1_birth_1 = Birth {
        name: "mesh1.gateway.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
    };
    let gw2 = DurableNode::bind_fresh(&dir, "mesh1.gateway.2", &alpns).await;
    let brk2 = DurableNode::bind_fresh(&dir, "mesh1.broker.2", &alpns).await;
    let (_, mut acc_brk2) = brk2.registry.connections(ALPN_A).unwrap().split();
    gw2.know(&brk2);
    establish_carrier_direct(&gw2, &brk2, &mut acc_brk2, ALPN_A).await;

    // Persist Direct Failed(E1, attempt=3, event_time=T) on ALPN_A such that next_due is ~350ms in the future:
    let ordinal = 3;
    let jitter = deterministic_jitter(&gw1_birth_1.name, &brk2.birth.name, ordinal);
    let failed_at = now_ms() - reconnect_backoff(ordinal) - jitter + 350;
    let failed_row = NodeConnection {
        id: ConnectionId::mint(),
        source: gw1_birth_1.to_end(),
        destination: brk2.birth.to_end(),
        kind: CoreConnectionKind::Direct,
        alpn: ALPN_A.to_vec(),
        state: ConnectionState::Failed,
        carrier: None,
        recovery: Some(DirectRecovery {
            recovery_round: 1,
            attempt_ordinal: ordinal,
        }),
        reason: Some("unreachable before restart".into()),
        logged_at_ms: failed_at,
    };
    let due_ms = next_due(&failed_row).unwrap();
    {
        let gw1 = DurableNode::boot(&dir, gw1_birth_1.clone(), &alpns).await;
        gw1.know(&gw2);
        gw1.writer.record(failed_row.clone()).await.unwrap();
        gw1.writer
            .record(NodeConnection {
                id: ConnectionId::mint(),
                source: gw1.birth.to_end(),
                destination: brk2.birth.to_end(),
                kind: CoreConnectionKind::Proxy,
                alpn: ALPN_A.to_vec(),
                state: ConnectionState::Connected,
                carrier: Some(gw2.birth.to_end()),
                recovery: None,
                reason: None,
                logged_at_ms: failed_at + 1,
            })
            .await
            .unwrap();
        gw1.shutdown().await;
    }

    // Restart gw1 BEFORE due_ms at a new process incarnation:
    let gw1_birth_2 = Birth {
        name: gw1_birth_1.name.clone(),
        node_id: gw1_birth_1.node_id.clone(),
        incarnation: IncarnationId::mint(),
    };
    let gw1 = DurableNode::boot(&dir, gw1_birth_2.clone(), &alpns).await;
    gw1.know(&gw2);

    // 1. Reconstructed schedule has exactly one obligation: on ALPN_A at ordinal 3 with due_ms:
    let obs = obligations(&gw1.writer);
    assert_eq!(obs.len(), 1, "one obligation reconstructed from index: {obs:?}");
    assert_eq!(
        (
            obs[0].destination.clone(),
            obs[0].alpn.as_slice(),
            obs[0].recovery,
            obs[0].due_ms,
        ),
        (
            brk2.birth.to_end(),
            ALPN_A,
            DirectRecovery {
                recovery_round: 1,
                attempt_ordinal: 3,
            },
            due_ms,
        )
    );

    let dial_log: Arc<Mutex<Vec<(Vec<u8>, u64)>>> = Arc::new(Mutex::new(Vec::new()));
    let inner_dial = registry_recovery_dial(&gw1.registry, None);
    let dial_log_clone = dial_log.clone();
    let tracked_dial: AlpnRecoveryDial = Arc::new(move |dest, alpn| {
        let dial_log = dial_log_clone.clone();
        let inner = inner_dial.clone();
        Box::pin(async move {
            dial_log.lock().unwrap().push((alpn.clone(), now_ms()));
            inner(dest, alpn).await
        })
    });
    let reconciler_task =
        spawn_alpn_reconciler(&gw1.writer, Duration::from_millis(20), tracked_dial.clone());

    // 2. Before due_ms, NO early attempt occurs:
    tokio::time::sleep(Duration::from_millis(120)).await;
    if now_ms() + 30 < due_ms {
        assert!(
            dial_log.lock().unwrap().is_empty(),
            "forbidden: early reconnect attempt before persisted next_due ({due_ms})"
        );
    }

    // 3. When due_ms arrives (brk2 still unreachable Direct from gw1), ALPN_A dials once (not ALPN_B),
    // fails, and writes Direct Failed(round=1, attempt_ordinal=4, alpn=ALPN_A):
    until("the persisted obligation on ALPN_A dials at due_ms", || {
        !dial_log.lock().unwrap().is_empty()
    })
    .await;
    until("attempt 4's Direct Failed row is persisted", || {
        gw1.held
            .lock()
            .unwrap()
            .own_latest_directs()
            .iter()
            .any(|d| d.alpn == ALPN_A && d.recovery.map(|r| r.attempt_ordinal) == Some(4))
    })
    .await;

    let calls = dial_log.lock().unwrap().clone();
    assert_eq!(calls.len(), 1, "no duplicate attempt: {calls:?}");
    assert_eq!(calls[0].0, ALPN_A, "only the obligated ALPN dials");
    assert!(
        calls[0].1 >= due_ms,
        "attempt waited for persisted next_due: dialled at {}, due at {due_ms}",
        calls[0].1
    );

    let latest_failed = gw1
        .storage
        .connections()
        .await
        .unwrap()
        .into_iter()
        .find(|r| r.kind == CoreConnectionKind::Direct && r.alpn == ALPN_A)
        .unwrap();
    assert_eq!(
        latest_failed.recovery,
        Some(DirectRecovery {
            recovery_round: 1,
            attempt_ordinal: 4
        }),
        "forbidden: restart at attempt 1; must continue at attempt 4"
    );
    assert_eq!(
        latest_failed.source,
        gw1_birth_2.to_end(),
        "continued row names the restarted birth that wrote it"
    );

    reconciler_task.abort();
    gw1.shutdown().await;
    gw2.shutdown().await;
    brk2.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// §13 D: many failed reconnect attempts for one pair grow the raw history log while keeping the
/// current index bounded at one Direct and one Proxy member for the `(source, destination, alpn)` pair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_connections_index_stays_bounded_while_the_raw_log_keeps_history() {
    let dir = temp_data_dir("13d");
    let alpns = [forwardable_alpn(ALPN_A)];
    let gw1 = DurableNode::bind_fresh(&dir, "mesh1.gateway.1", &alpns).await;
    let gw2 = DurableNode::bind_fresh(&dir, "mesh1.gateway.2", &alpns).await;
    let brk2 = DurableNode::bind_fresh(&dir, "mesh1.broker.2", &alpns).await;

    let (dialer_gw1, _) = gw1.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acc_brk2) = brk2.registry.connections(ALPN_A).unwrap().split();
    gw1.know(&gw2);
    gw2.know(&brk2);
    establish_carrier_direct(&gw2, &brk2, &mut acc_brk2, ALPN_A).await;

    // First open establishes Proxy Connected(via=gw2) and writes Direct Failed(round=1, ordinal=1):
    let accept_task = tokio::spawn(async move {
        let c = acc_brk2.accept().await.unwrap();
        (c, acc_brk2)
    });
    let _proxy_conn = dialer_gw1.open(&brk2.birth).await.unwrap();
    let (_srv_conn, _acc_brk2) = accept_task.await.unwrap();

    // Drive 5 more reconnect attempts as each attempt's due_ms is reached (total 6 Direct Failed attempts):
    let dial = registry_recovery_dial(&gw1.registry, None);
    for expected_ordinal in 2..=6u32 {
        let obs = obligations(&gw1.writer);
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].recovery.attempt_ordinal, expected_ordinal - 1);
        run_due_at(&gw1.writer, &dial, obs[0].due_ms).await;
        until("next Direct Failed ordinal is held", || {
            gw1.held
                .lock()
                .unwrap()
                .own_latest_directs()
                .iter()
                .any(|d| d.alpn == ALPN_A && d.recovery.map(|r| r.attempt_ordinal) == Some(expected_ordinal))
        })
        .await;
    }

    let history = gw1.storage.history().await.unwrap();
    let direct_failures: Vec<_> = history
        .iter()
        .filter(|r| {
            r.source.name == gw1.birth.name
                && r.destination.name == brk2.birth.name
                && r.alpn == ALPN_A
                && r.kind == CoreConnectionKind::Direct
                && r.state == ConnectionState::Failed
        })
        .collect();
    assert_eq!(
        direct_failures
            .iter()
            .map(|r| r.recovery.unwrap().attempt_ordinal)
            .collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5, 6],
        "raw log keeps the full retry history"
    );

    let index = gw1.storage.connections().await.unwrap();
    let pair_index: Vec<_> = index
        .iter()
        .filter(|r| {
            r.source.name == gw1.birth.name
                && r.destination.name == brk2.birth.name
                && r.alpn == ALPN_A
        })
        .collect();
    assert_eq!(
        pair_index
            .iter()
            .filter(|r| r.kind == CoreConnectionKind::Direct)
            .count(),
        1,
        "index keeps at most one Direct member for the pair: {pair_index:?}"
    );
    assert_eq!(
        pair_index
            .iter()
            .filter(|r| r.kind == CoreConnectionKind::Proxy)
            .count(),
        1,
        "index keeps at most one Proxy member for the pair: {pair_index:?}"
    );
    let indexed_direct = pair_index
        .iter()
        .find(|r| r.kind == CoreConnectionKind::Direct)
        .unwrap();
    assert_eq!(
        indexed_direct.recovery,
        Some(DirectRecovery {
            recovery_round: 1,
            attempt_ordinal: 6
        })
    );

    gw1.shutdown().await;
    gw2.shutdown().await;
    brk2.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// §9.4 and §9.5: one scheduler per node, at most one pending obligation per
/// `(destination, destination_incarnation, alpn)`, and at most one reconnect attempt executing at a
/// time on the node.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_reconnect_attempt_at_a_time_per_node() {
    let dir = temp_data_dir("one-at-a-time");
    let alpns = [forwardable_alpn(ALPN_A), forwardable_alpn(ALPN_B)];
    let gw1 = DurableNode::bind_fresh(&dir, "mesh1.gateway.1", &alpns).await;
    let gw2 = DurableNode::bind_fresh(&dir, "mesh1.gateway.2", &alpns).await;
    let brk2 = DurableNode::bind_fresh(&dir, "mesh1.broker.2", &alpns).await;
    let brk3 = DurableNode::bind_fresh(&dir, "mesh1.broker.3", &alpns).await;

    let past = now_ms().saturating_sub(60_000);

    // Seed two due obligations on gw1 (toward brk2 and brk3 on ALPN_A), plus repeated failures and accepts:
    for (dest, ord) in [(&brk2.birth, 1u32), (&brk2.birth, 2u32), (&brk3.birth, 1u32)] {
        gw1.writer
            .record(NodeConnection {
                id: ConnectionId::mint(),
                source: gw1.birth.to_end(),
                destination: dest.to_end(),
                kind: CoreConnectionKind::Direct,
                alpn: ALPN_A.to_vec(),
                state: ConnectionState::Failed,
                carrier: None,
                recovery: Some(DirectRecovery {
                    recovery_round: 1,
                    attempt_ordinal: ord,
                }),
                reason: Some("seed failure".into()),
                logged_at_ms: past + u64::from(ord),
            })
            .await
            .unwrap();
        gw1.writer
            .record(NodeConnection {
                id: ConnectionId::mint(),
                source: gw1.birth.to_end(),
                destination: dest.to_end(),
                kind: CoreConnectionKind::Proxy,
                alpn: ALPN_A.to_vec(),
                state: ConnectionState::Connected,
                carrier: Some(gw2.birth.to_end()),
                recovery: None,
                reason: None,
                logged_at_ms: past + 10,
            })
            .await
            .unwrap();
    }

    // Repeated inbound accepts from brk2 on ALPN_A coalesce into the single pending obligation:
    for _ in 0..4 {
        gw1.writer.accepted_on_alpn(&brk2.birth, ALPN_A);
    }
    let obs: Vec<ReconnectObligation> = obligations(&gw1.writer);
    assert_eq!(
        obs.len(),
        2,
        "one obligation per (destination, incarnation, alpn): {obs:?}"
    );

    // Gate the reconnect dial so we can observe concurrency when both obligations are due:
    let started: Arc<Mutex<Vec<ConnectionEnd>>> = Arc::new(Mutex::new(Vec::new()));
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    let started_c = started.clone();
    let gate_c = gate.clone();
    let writer_c = gw1.writer.clone();
    let reconciler = Reconciler::new(gw1.writer.clone());
    let gated_dial: AlpnRecoveryDial = Arc::new(move |dest: ConnectionEnd, alpn: Vec<u8>| {
        let started = started_c.clone();
        let gate = gate_c.clone();
        let writer = writer_c.clone();
        Box::pin(async move {
            started.lock().unwrap().push(dest.clone());
            gate.acquire().await.unwrap().forget();
            let _ = writer
                .record_data_plane(NodeConnection {
                    id: ConnectionId::mint(),
                    source: writer.own(),
                    destination: dest,
                    kind: CoreConnectionKind::Direct,
                    alpn,
                    state: ConnectionState::Connected,
                    carrier: None,
                    recovery: None,
                    reason: None,
                    logged_at_ms: 0,
                })
                .await;
            "Reply".to_string()
        })
    });

    // Spawn two concurrent run_due passes on the same node's reconciler to prove the node-wide
    // single-attempt gate serializes every reconnect attempt:
    let r1 = reconciler.clone();
    let d1 = gated_dial.clone();
    let pass1 = tokio::spawn(async move { r1.run_due(&d1).await });
    let r2 = reconciler.clone();
    let d2 = gated_dial.clone();
    let pass2 = tokio::spawn(async move { r2.run_due(&d2).await });

    until("first reconnect attempt starts", || {
        started.lock().unwrap().len() == 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(80)).await;
    assert_eq!(
        started.lock().unwrap().len(),
        1,
        "second reconnect attempt must wait while the first is in flight"
    );
    assert_eq!(reconciler.in_flight_attempts(), 1);
    assert_eq!(reconciler.max_concurrent_attempts(), 1);

    // Release the first attempt -> the second attempt starts and still runs alone:
    gate.add_permits(1);
    until("second reconnect attempt starts", || {
        started.lock().unwrap().len() == 2
    })
    .await;
    assert_eq!(reconciler.max_concurrent_attempts(), 1);

    gate.add_permits(1);
    pass1.await.unwrap();
    pass2.await.unwrap();

    assert_eq!(
        reconciler.max_concurrent_attempts(),
        1,
        "at most one reconnect attempt ever ran at a time on the node"
    );
    assert!(
        obligations(&gw1.writer).is_empty(),
        "both obligations are spent once their Direct Connected rows land"
    );

    gw1.shutdown().await;
    gw2.shutdown().await;
    brk2.shutdown().await;
    brk3.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Q62 / Demo acceptance row 3 (`connections.md` §4.1, §3.2): register ALPN_A and ALPN_B between
/// the same births on a shared store root; the carrier's own `ConnectionsWriter` writes its
/// `Direct Connected` rows to the store root with no hand-fed row on the source; restore ALPN_A
/// while ALPN_B uses its Proxy: ALPN_B's Proxy route and open streams are unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alpn_interference_restores_a_while_b_keeps_its_proxy_from_the_shared_store() {
    let dir = temp_data_dir("row3-shared-store");
    let alpns = [forwardable_alpn(ALPN_A), forwardable_alpn(ALPN_B)];
    let src = DurableNode::bind_fresh(&dir, "mesh1.rpc.1", &alpns).await;
    let carrier = DurableNode::bind_fresh(&dir, "mesh1.gateway.1", &alpns).await;
    let dst = DurableNode::bind_fresh(&dir, "mesh1.rpc.2", &alpns).await;

    let (dialer_a, _) = src.registry.connections(ALPN_A).unwrap().split();
    let (dialer_b, _) = src.registry.connections(ALPN_B).unwrap().split();
    let (_, mut acc_dst_a) = dst.registry.connections(ALPN_A).unwrap().split();
    let (_, mut acc_dst_b) = dst.registry.connections(ALPN_B).unwrap().split();

    // src can reach carrier; carrier can reach dst and establishes real Direct edges on both ALPNs
    // (written by carrier's own ConnectionsWriter to the shared store root; no row is fed by hand):
    src.know(&carrier);
    carrier.know(&dst);
    establish_carrier_direct(&carrier, &dst, &mut acc_dst_a, ALPN_A).await;
    establish_carrier_direct(&carrier, &dst, &mut acc_dst_b, ALPN_B).await;

    // Both ALPN_A and ALPN_B discover Proxy via carrier from the shared connections store:
    let accept_a1 = tokio::spawn(async move {
        let c = acc_dst_a.accept().await.unwrap();
        (c, acc_dst_a)
    });
    let conn_a_proxy = dialer_a.open(&dst.birth).await.unwrap();
    let (_srv_a1, mut acc_dst_a) = accept_a1.await.unwrap();
    assert_eq!(
        conn_a_proxy.kind(),
        ConnectionKind::Proxy {
            via: carrier.birth.clone()
        }
    );

    let accept_b1 = tokio::spawn(async move {
        let c = acc_dst_b.accept().await.unwrap();
        (c, acc_dst_b)
    });
    let conn_b_proxy = dialer_b.open(&dst.birth).await.unwrap();
    let (srv_b1, mut acc_dst_b) = accept_b1.await.unwrap();
    assert_eq!(
        conn_b_proxy.kind(),
        ConnectionKind::Proxy {
            via: carrier.birth.clone()
        }
    );
    src.writer.observations_landed().await;

    // Open a stream on ALPN_B's Proxy before ALPN_A is restored:
    let (mut b_send, mut b_recv) = conn_b_proxy.open_bi().await.unwrap();
    b_send.write_all(b"b-before-a-restored").await.unwrap();
    let (mut b_srv_send, mut b_srv_recv) = srv_b1.accept_bi().await.unwrap();
    let mut buf = [0u8; 19];
    b_srv_recv.read_exact(&mut buf).await.unwrap();
    assert_eq!(&buf, b"b-before-a-restored");

    // Restore Direct on ALPN_A ONLY (src learns dst's address, probes Direct on ALPN_A, and forgets
    // dst's address again so ALPN_B stays on its Proxy):
    src.know(&dst);
    let accept_a2 = tokio::spawn(async move {
        let c = acc_dst_a.accept().await.unwrap();
        (c, acc_dst_a)
    });
    assert!(src.registry.probe_direct(&dst.birth, ALPN_A).await.is_ok());
    let (_srv_a2, mut acc_dst_a) = accept_a2.await.unwrap();
    src.writer.observations_landed().await;

    // Next open on ALPN_A is Direct, while ALPN_B's Proxy route and in-flight stream are unchanged:
    let accept_a3 = tokio::spawn(async move { acc_dst_a.accept().await.unwrap() });
    let conn_a_direct = dialer_a.open(&dst.birth).await.unwrap();
    let _srv_a3 = accept_a3.await.unwrap();
    assert_eq!(conn_a_direct.kind(), ConnectionKind::Direct);

    let accept_b2 = tokio::spawn(async move { acc_dst_b.accept().await.unwrap() });
    let conn_b_still_proxy = dialer_b.open(&dst.birth).await.unwrap();
    let _srv_b2 = accept_b2.await.unwrap();
    assert_eq!(
        conn_b_still_proxy.kind(),
        ConnectionKind::Proxy {
            via: carrier.birth.clone()
        },
        "restoring ALPN_A must not retire ALPN_B's Proxy"
    );

    b_send.write_all(b"-and-after").await.unwrap();
    b_send.finish().unwrap();
    assert_eq!(b_srv_recv.read_to_end(64).await.unwrap(), b"-and-after");
    b_srv_send.write_all(b"b-reply-ok").await.unwrap();
    b_srv_send.finish().unwrap();
    assert_eq!(b_recv.read_to_end(64).await.unwrap(), b"b-reply-ok");

    let routes = src.writer.held_routes().await;
    assert!(
        routes.iter().any(|r| r.alpn == ALPN_A
            && r.kind == CoreConnectionKind::Direct
            && r.state == ConnectionState::Connected),
        "ALPN_A holds Direct Connected: {routes:?}"
    );
    assert!(
        !routes
            .iter()
            .any(|r| r.alpn == ALPN_A && r.kind == CoreConnectionKind::Proxy),
        "ALPN_A retired its Proxy: {routes:?}"
    );
    assert!(
        routes.iter().any(|r| r.alpn == ALPN_B
            && r.kind == CoreConnectionKind::Proxy
            && r.state == ConnectionState::Connected),
        "ALPN_B still holds its active Proxy Connected: {routes:?}"
    );

    src.shutdown().await;
    carrier.shutdown().await;
    dst.shutdown().await;
    let _ = std::fs::remove_dir_all(&dir);
}

/// Admits while its gate is open; otherwise refuses `expired` by name.
struct GateAdmitter(Arc<std::sync::atomic::AtomicBool>);

impl Admitter for GateAdmitter {
    fn verify(&self, _caller: &Birth, _alpn: &[u8], _cert: &[u8]) -> Result<(u64, &'static str), String> {
        if self.0.load(std::sync::atomic::Ordering::SeqCst) { Ok((0, "no-certs")) } else { Err("expired".into()) }
    }
}

/// CONTRACT (node-connections-admit.md step 5, connections.md §9.1, §9.5; decided 2026-10-19): a
/// refused dial is a Direct `Failed` row, so the source's reconnect series exists; a refused attempt
/// of the series continues it at the next ordinal (never spent as `no-fact`), and the attempt the
/// acceptor admits ends it, with no command from anyone.
#[tokio::test]
async fn a_refused_dial_starts_a_series_a_refused_attempt_continues_and_an_admitted_one_ends() {
    let dir = temp_data_dir("refused-series");
    let alpns = [forwardable_alpn(ALPN_A)];
    let gw = DurableNode::bind_fresh(&dir, "mesh1.gateway.1", &alpns).await;
    let brk = DurableNode::bind_fresh(&dir, "mesh1.broker.1", &alpns).await;
    gw.know(&brk);
    brk.know(&gw);
    let open = Arc::new(std::sync::atomic::AtomicBool::new(false));
    brk.registry.set_admitter(Arc::new(GateAdmitter(open.clone())));
    let (opener, _) = gw.registry.connections(ALPN_A).unwrap().split();
    let (_, mut accepts) = brk.registry.connections(ALPN_A).unwrap().split();

    assert_eq!(opener.open(&brk.birth).await.unwrap_err(), ConnectError::Refused { reason: "expired".into() });
    let obs = obligations(&gw.writer);
    assert_eq!(obs.len(), 1, "the refused dial starts the source's series: {obs:?}");
    assert_eq!((obs[0].destination.name.clone(), obs[0].alpn.as_slice(), obs[0].recovery.attempt_ordinal), (brk.birth.name.clone(), ALPN_A, 1));

    let dial = registry_recovery_dial(&gw.registry, None);
    run_due_at(&gw.writer, &dial, obs[0].due_ms).await;
    let obs = obligations(&gw.writer);
    assert_eq!(obs.len(), 1, "a refused attempt continues the series, never spent as no-fact: {obs:?}");
    assert_eq!(obs[0].recovery.attempt_ordinal, 2);

    open.store(true, std::sync::atomic::Ordering::SeqCst);
    let accepted = tokio::spawn(async move { accepts.accept().await.unwrap() });
    run_due_at(&gw.writer, &dial, obs[0].due_ms).await;
    accepted.await.unwrap();
    assert!(obligations(&gw.writer).is_empty(), "the admitted attempt is Direct Connected and ends the series");
    gw.shutdown().await;
    brk.shutdown().await;
}
