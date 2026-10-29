//! Connection writes are asynchronous (Q169; connections.md §1): every connection row is owed on the
//! node's one ordered writer queue and retried with backoff until it lands; no open, dial, accept or
//! read waits for it; the held projection takes a fact once its write lands.
//!
//! - `an_open_returns_before_its_row_lands_on_a_store_reached_over_that_connection`: the store's
//!   write waits until the connection being opened exists (a broker-hosted store); the open returns, the row lands after, the held projection then holds it, and a
//!   read during the stalled write answers at once from what is held.
//! - `a_refused_connection_write_is_retried_with_backoff_until_it_lands`: each refusal is named on
//!   its own span with its attempt, and the row lands once.
//! - `a_disconnected_owed_after_a_connected_lands_after_it`: rows land in queue order.
//! - `a_start_hold_and_a_settlement_never_wait_behind_a_refused_writes_backoff`: the write gate is
//!   taken for one write attempt and let go before the backoff, so a start's hold and the
//!   reconciler's settlement proceed while a refused row waits to be written again.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::connections::{ConnectionKind as CoreConnectionKind, ConnectionState, ConnectionsHeld, NodeConnection};
use rdm_core::status::ConnectionScope;
use rdm_core::{IncarnationId, NodeId};
use rdm_node::connections_writer::ConnectionsWriter;
use rdm_node::storage::{ConnectionsStorage, MemoryConnectionsStorage};
use rdm_node::storage_error::StorageError;
use rdm_transport::connections::{Alpn, Birth, ConnectionsRegistry};
use rdm_transport::{CarrierEdges, ResolvedNode, StaticResolver};

const ALPN: &[u8] = b"test-connection-writes/1";

/// A connections store whose writes wait until `reachable` is set (a store reached over the very
/// connection whose row it is asked to keep), and refuse the first `refuse` attempts.
struct OverTheConnection {
    rows: MemoryConnectionsStorage,
    reachable: tokio::sync::watch::Sender<bool>,
    refuse: AtomicUsize,
}

impl OverTheConnection {
    fn new() -> Arc<Self> {
        Arc::new(Self { rows: MemoryConnectionsStorage::default(), reachable: tokio::sync::watch::channel(false).0, refuse: AtomicUsize::new(0) })
    }

    async fn reached(&self) -> Result<(), StorageError> {
        let mut rx = self.reachable.subscribe();
        let _ = rx.wait_for(|r| *r).await;
        if self.refuse.load(Ordering::SeqCst) > 0 {
            self.refuse.fetch_sub(1, Ordering::SeqCst);
            return Err(StorageError::Io { file: "ops.nodes.connections".into(), reason: "the store refused the write".into() });
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ConnectionsStorage for OverTheConnection {
    async fn put_connection(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.reached().await?;
        self.rows.put_connection(fact).await
    }
    async fn connections(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.rows.connections().await
    }
    async fn remove_connection(&self, index: &rdm_core::connections::ConnectionIndex) -> Result<(), StorageError> {
        self.rows.remove_connection(index).await
    }
    async fn append_history(&self, fact: &NodeConnection) -> Result<(), StorageError> {
        self.rows.append_history(fact).await
    }
    async fn history(&self) -> Result<Vec<NodeConnection>, StorageError> {
        self.rows.history().await
    }
    async fn connections_to(&self, destination: &NodeId) -> Result<Vec<NodeConnection>, StorageError> {
        self.rows.connections_to(destination).await
    }
}

struct Node {
    birth: Birth,
    resolved: ResolvedNode,
    resolver: Arc<StaticResolver>,
    writer: Arc<ConnectionsWriter>,
    registry: ConnectionsRegistry,
    router: Router,
}

impl Node {
    async fn boot(name: &str, storage: Arc<dyn ConnectionsStorage>) -> Self {
        let birth = Birth { name: name.parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let key = SecretKey::generate();
        let ep = rdm_transport::endpoint::bind_with_alpns(key.clone(), "127.0.0.1:0".parse().unwrap(), &[ALPN.to_vec()]).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let resolved = ResolvedNode { node_id: birth.node_id.clone(), name: birth.name.clone(), endpoint_id: key.public(), transport_addr: addr, incarnation: birth.incarnation.clone() };
        let resolver = Arc::new(StaticResolver::new());
        let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
        let writer = Arc::new(ConnectionsWriter::new(birth.to_end(), storage, held));
        let alpn = Alpn { name: ALPN.to_vec(), proxy: false, carriers: vec![] };
        let (registry, handlers) = ConnectionsRegistry::new(ep.clone(), resolver.clone(), writer.clone(), &[alpn], b"cert-v1".to_vec()).unwrap();
        let mut rb = Router::builder(ep);
        for (alpn_bytes, handler) in handlers {
            rb = rb.accept(alpn_bytes, handler);
        }
        Self { birth, resolved, resolver, writer, registry, router: rb.spawn() }
    }

    /// Accept every inbound connection on this node, keeping each open.
    fn accept_all(&self) {
        let mut conns = self.registry.connections(ALPN).unwrap();
        tokio::spawn(async move {
            let mut kept = Vec::new();
            while let Some(c) = conns.accept().await {
                kept.push(c);
            }
        });
    }

    async fn shutdown(self) {
        let _ = self.router.shutdown().await;
    }
}

async fn pair(source_store: Arc<OverTheConnection>) -> (Node, Node) {
    let source = Node::boot("mesh1.gateway.1", source_store).await;
    let dest = Node::boot("mesh1.broker.1", Arc::new(MemoryConnectionsStorage::default())).await;
    source.resolver.insert(dest.resolved.clone());
    dest.resolver.insert(source.resolved.clone());
    dest.accept_all();
    (source, dest)
}

fn own_directs(source: &Node) -> Vec<NodeConnection> {
    source.writer.held().lock().unwrap().own_latest_directs().into_iter().cloned().collect()
}

fn capture(tag: &str) -> rdm_transport::EvidenceCapture {
    let dir = std::env::temp_dir().join(format!("rdm-connection-writes-{tag}-{}", NodeId::mint()));
    std::fs::create_dir_all(&dir).unwrap();
    rdm_transport::EvidenceCapture::start(&dir, "rdm-connection-writes").expect("the cell's evidence file can be created")
}

#[test]
fn an_open_returns_before_its_row_lands_on_a_store_reached_over_that_connection() {
    let capture = capture("open");
    capture.run(4, async {
        let store = OverTheConnection::new();
        let (source, dest) = pair(store.clone()).await;
        let dialer = source.registry.connections(ALPN).unwrap();

        // The store is reached only over the connection this open makes: an open that waits for its
        // own row never returns.
        let conn = tokio::time::timeout(Duration::from_secs(10), dialer.open(&dest.birth))
            .await
            .expect("the open returns once Opened, without waiting for its Connected row (Q169)")
            .expect("the open succeeds");

        // The row is owed, not landed: nothing is held yet, and a read answers at once from what is held.
        assert!(own_directs(&source).is_empty(), "the held projection takes the Connected only once its write lands");
        let read = tokio::time::timeout(Duration::from_secs(1), source.writer.held_connections())
            .await
            .expect("node.connections.get answers from what is held, never waiting for a write");
        assert!(read.routes.is_empty(), "nothing has landed, so nothing is held: {:?}", read.routes);
        let edge = tokio::time::timeout(Duration::from_secs(1), source.writer.edge_not_active(&dest.birth.node_id))
            .await
            .expect("a carrier's edge read answers from what is held, never waiting for a write");
        assert_eq!(edge, None, "no Direct fact toward the destination is held yet");

        // The connection exists: the store is reached and the row lands after the open.
        store.reachable.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), source.writer.observations_landed()).await.expect("the owed row lands once its store is reached");
        let directs = own_directs(&source);
        assert_eq!(directs.len(), 1, "one Direct fact held: {directs:?}");
        assert_eq!(directs[0].id, *conn.id(), "the held row is the open's own entry");
        assert_eq!(directs[0].state, ConnectionState::Connected);
        let routes = source.writer.held_routes().await;
        assert_eq!(routes.iter().map(|r| r.id.clone()).collect::<Vec<_>>(), vec![conn.id().clone()], "node.connections.get lists the landed connection");
        let stored = store.rows.connections().await.unwrap();
        assert!(stored.iter().any(|r| r.id == *conn.id() && r.state == ConnectionState::Connected), "the index keeps the Connected row");
        drop(conn);
        source.shutdown().await;
        dest.shutdown().await;
    });
}

#[test]
fn a_refused_connection_write_is_retried_with_backoff_until_it_lands() {
    let capture = capture("refused");
    let entry = capture.run(4, async {
        let store = OverTheConnection::new();
        store.reachable.send_replace(true);
        store.refuse.store(2, Ordering::SeqCst);
        let (source, dest) = pair(store.clone()).await;
        let dialer = source.registry.connections(ALPN).unwrap();
        let conn = tokio::time::timeout(Duration::from_secs(10), dialer.open(&dest.birth))
            .await
            .expect("the open never waits for its row")
            .expect("a refused write is no refused open: the row is owed and retried");
        tokio::time::timeout(Duration::from_secs(20), source.writer.observations_landed()).await.expect("the refused write is retried until it lands");
        let directs = own_directs(&source);
        assert_eq!(directs.len(), 1, "the row is held once it lands: {directs:?}");
        assert_eq!(directs[0].id, *conn.id());
        let history = store.rows.history().await.unwrap();
        assert_eq!(history.iter().filter(|r| r.id == *conn.id()).count(), 1, "the raw log took the row once: only the refused step is attempted again");
        assert!(store.rows.connections().await.unwrap().iter().any(|r| r.id == *conn.id()), "the index keeps the row");
        let id = conn.id().to_string();
        drop(conn);
        source.shutdown().await;
        dest.shutdown().await;
        id
    });
    let spans = capture.spans().expect("the cell's spans");
    let refused: Vec<_> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node_admin.connection.reject.via-refused-write" && s["attributes"]["entry"].as_str() == Some(entry.as_str()))
        .collect();
    let attempts: Vec<_> = refused.iter().map(|s| s["attributes"]["attempt"].to_string()).collect();
    assert_eq!(attempts.len(), 2, "each refused attempt is named on its own span: {refused:?}");
    assert!(refused.iter().all(|s| s["attributes"].get("next_in_ms").is_some()), "each names its backoff: {refused:?}");
    let landed: Vec<_> = spans
        .iter()
        .filter(|s| s["name"] == "rdm.node_admin.connection.update.via-observed" && s["attributes"]["entry"].as_str() == Some(entry.as_str()))
        .collect();
    assert_eq!(landed.len(), 1, "one fact span for the row: {landed:?}");
    assert_eq!(landed[0]["attributes"]["outcome"], "landed");
}

#[test]
fn a_disconnected_owed_after_a_connected_lands_after_it() {
    let capture = capture("order");
    capture.run(4, async {
        let store = OverTheConnection::new();
        let (source, dest) = pair(store.clone()).await;
        let dialer = source.registry.connections(ALPN).unwrap();
        let conn = tokio::time::timeout(Duration::from_secs(10), dialer.open(&dest.birth))
            .await
            .expect("the open never waits for its row")
            .expect("the open succeeds");
        let id = conn.id().clone();
        // Closed before its Connected lands: the drop is owed behind it.
        let closed = tokio::time::timeout(Duration::from_secs(5), source.registry.delete_connections(&ConnectionScope::Connections(vec![id.clone()])))
            .await
            .expect("a delete never waits for its drop's write");
        assert_eq!(closed, vec![id.clone()]);
        store.reachable.send_replace(true);
        tokio::time::timeout(Duration::from_secs(10), source.writer.observations_landed()).await.expect("both rows land");
        let history: Vec<_> = store.rows.history().await.unwrap().into_iter().filter(|r| r.kind == CoreConnectionKind::Direct).collect();
        let states: Vec<_> = history.iter().map(|r| r.state).collect();
        assert_eq!(states, vec![ConnectionState::Connected, ConnectionState::Disconnected], "rows land in queue order: {history:?}");
        assert_eq!(history[0].id, id);
        let directs = own_directs(&source);
        assert_eq!(directs.len(), 1);
        assert_eq!(directs[0].state, ConnectionState::Disconnected, "the close ends right: the pair's latest is the drop");
        assert!(source.writer.held_routes().await.is_empty(), "nothing is held open");
        drop(conn);
        source.shutdown().await;
        dest.shutdown().await;
    });
}

#[test]
fn a_start_hold_and_a_settlement_never_wait_behind_a_refused_writes_backoff() {
    let capture = capture("backoff");
    capture.run(4, async {
        let store = OverTheConnection::new();
        store.reachable.send_replace(true);
        store.refuse.store(3, Ordering::SeqCst);
        let (source, dest) = pair(store.clone()).await;
        let dialer = source.registry.connections(ALPN).unwrap();
        let conn = tokio::time::timeout(Duration::from_secs(10), dialer.open(&dest.birth))
            .await
            .expect("the open never waits for its row")
            .expect("the open succeeds");
        // The row's first attempt is refused: it now waits its backoff (the publish cadence, then
        // doubling) before it is written again, and two more attempts are still to be refused.
        tokio::time::timeout(Duration::from_secs(10), async {
            while store.refuse.load(Ordering::SeqCst) > 2 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the row's first write is attempted and refused");
        let hold = tokio::time::timeout(Duration::from_millis(200), source.writer.hold_rows())
            .await
            .expect("a start's hold is taken while a refused write waits its backoff, never behind it");
        assert!(own_directs(&source).is_empty(), "the hold was taken inside the backoff: the refused row has not landed yet");
        drop(hold);
        tokio::time::timeout(Duration::from_millis(200), source.writer.settle_owed())
            .await
            .expect("the reconciler's settlement never waits behind a refused write's backoff")
            .expect("nothing is owed, so the settlement writes nothing");
        tokio::time::timeout(Duration::from_secs(20), source.writer.observations_landed()).await.expect("the refused write is retried until it lands");
        let directs = own_directs(&source);
        assert_eq!(directs.len(), 1, "the row lands once its backoff ends: {directs:?}");
        assert_eq!(directs[0].id, *conn.id());
        drop(conn);
        source.shutdown().await;
        dest.shutdown().await;
    });
}

/// CONTRACT (fabric-ca-renew.md "node.roots.update, the primitive": "one or two roots, never none";
/// "a set that drops a root the node still admits a chain under is refused by name
/// (`root-still-chained`)"): a birth whose member cert chains under root A installs [A, B] and answers
/// `RootsUpdated`; a set of B alone, which drops A while its cert still chains under A, is refused
/// `RejectedRootStillChained` naming A and nothing is installed; no root, three roots and a value that
/// is no key are refused by name. Must NOT happen: a birth left trusting no root its own cert chains to.
#[tokio::test]
async fn a_root_set_that_drops_the_root_a_births_cert_chains_under_is_refused_by_name() {
    use rdm_core::certs::{BirthIdentity, CaKey, IssuingKey};
    use rdm_core::status::{RootsPhase, StatusReply};
    let node = Node::boot("mesh1.rpc.1", Arc::new(MemoryConnectionsStorage::default())).await;
    let (root_a, root_b) = (rdm_core::certs::mint_key(), rdm_core::certs::mint_key());
    let ca = CaKey::mint(&root_a, 3600, 1_000);
    let issuing = IssuingKey::mint(&ca, "mesh1", 1_000, 2_000_000).unwrap();
    let birth = BirthIdentity { node_id: node.birth.node_id.clone(), incarnation: node.birth.incarnation.clone(), name: node.birth.name.clone(), mesh: "mesh1".into(), endpoint_key: rdm_core::EndpointId(node.resolved.endpoint_id.to_string()) };
    let cert = rdm_core::certs::issue(&issuing, &birth, 1_000, 1_000_000).unwrap();
    node.registry.set_member_cert(rdm_core::certs::cert_to_bytes(&cert));
    node.registry.roots().replace(vec![root_a.verifying_key()]);
    let hex = |k: &rdm_core::certs::SigningKey| rdm_core::launch::encode_hex(k.verifying_key().as_bytes());
    let install = |roots: Vec<String>| rdm_node::roots_update::install_roots(&node.registry, "mesh1.rpc.1", &node.birth.node_id, &node.birth.incarnation, &roots, RootsPhase::Distributed, false);

    let both = install(vec![hex(&root_a), hex(&root_b)]);
    assert!(matches!(&both, StatusReply::RootsUpdated { roots, .. } if roots.len() == 2), "{both:?}");
    assert_eq!(node.registry.roots().current(), vec![root_a.verifying_key(), root_b.verifying_key()]);

    let dropped = install(vec![hex(&root_b)]);
    assert_eq!(dropped, StatusReply::RejectedRootStillChained { root: hex(&root_a) });
    assert_eq!(node.registry.roots().current().len(), 2, "nothing is installed on a refusal");

    for bad in [vec![], vec![hex(&root_a), hex(&root_b), hex(&root_a)], vec!["not-a-key".to_string()]] {
        assert!(matches!(install(bad.clone()), StatusReply::RejectedCert { .. }), "{bad:?} is refused by name");
    }
}
