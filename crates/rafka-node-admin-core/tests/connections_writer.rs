//! i143.e6.s11 functional: the source-owned connections writer. A node's own Direct and Proxy
//! facts are rows it writes (the raw log grows, the index keeps one entry per pair), applied to
//! the held projection the route resolver reads, and hydrated from the index at birth so a
//! restart keeps its latest Proxy (connections.md §13 B) and the reconnect series continues from
//! its latest failure while the index stays bounded (§13 D).

use rafka_mesh_entity::connections::{resolve, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rafka_mesh_entity::reconnect::reconnect_plan;
use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind, PathName};
use rafka_node_admin_core::connections_writer::ConnectionsWriter;
use rafka_node_admin_core::storage::{ConnectionsStorage, FileConnectionsStorage};
use rafka_node_rpc::{ConnectionObserver, ResolvedNode};
use std::sync::{Arc, Mutex};

const POLICY: CarrierPolicy = CarrierPolicy::Forwardable { carrier_kind: NodeKind::RpcNode };

fn end(name: &str) -> ConnectionEnd {
    ConnectionEnd { name: name.parse().unwrap(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) }
}

fn resolved(e: &ConnectionEnd) -> ResolvedNode {
    ResolvedNode {
        node_id: e.node_id.clone(),
        name: e.name.clone(),
        endpoint_id: iroh::SecretKey::generate().public(),
        transport_addr: "127.0.0.1:1".parse().unwrap(),
        incarnation: e.incarnation.clone().unwrap(),
    }
}

fn row(source: &ConnectionEnd, destination: &ConnectionEnd, kind: ConnectionKind, state: ConnectionState, carrier: Option<&ConnectionEnd>, at: u64) -> NodeConnection {
    NodeConnection { source: source.clone(), destination: destination.clone(), kind, state, carrier: carrier.cloned(), recovery: None, reason: None, logged_at_ms: at }
}

fn data_dir(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("i143-e6s11-{tag}-{}", NodeId::mint()))
}

async fn wait_rows(storage: &dyn ConnectionsStorage, n: usize) -> Vec<NodeConnection> {
    for _ in 0..200 {
        let h = storage.history().await.unwrap();
        if h.len() >= n {
            return h;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    panic!("the writer never reached {n} history rows");
}

/// §13 B: the latest rows this node wrote are hydrated at birth, and the first resolution after
/// the restart is the Proxy, with no rediscovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_hydrates_the_latest_proxy_row_and_the_first_resolution_uses_it() {
    let dir = data_dir("hydrate");
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    // The previous birth wrote: Direct to dest failed, the carrier's edge to dest, a Proxy via the carrier.
    {
        let storage = FileConnectionsStorage::open(&dir).unwrap();
        let mut failed = row(&me, &dest, ConnectionKind::Direct, ConnectionState::Failed, None, 10);
        failed.recovery = Some(rafka_mesh_entity::connections::DirectRecovery { recovery_epoch: 1, attempt_ordinal: 2 });
        for r in [failed, row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 11), row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 12)] {
            storage.append_history(&r).await.unwrap();
            storage.put_connection(&r).await.unwrap();
        }
    }
    // The next birth: a fresh writer over the same storage hydrates its held projection.
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&dir).unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    assert_eq!(writer.hydrate().await.unwrap(), 3);
    let r = resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY);
    assert!(matches!(r.route, EffectiveRoute::ViaPeer { ref carrier, .. } if *carrier == "mesh1.rpc.2".parse::<PathName>().unwrap()), "the Proxy is used without rediscovery: {:?}", r.route);
    assert!(r.retire.is_none());
    // The reconnect series continues from the latest failure: ordinal 2, not 1.
    let plan = reconnect_plan(&held.lock().unwrap(), |_| None);
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].recovery.attempt_ordinal, 2);
    let _ = std::fs::remove_dir_all(&dir);
}

/// §13 D: many failed attempts for one pair leave the raw log growing while the index holds one
/// Direct member for the pair; the ordinal counts up in one epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_failures_grow_the_raw_log_and_keep_the_index_at_one_direct_per_pair() {
    let dir = data_dir("bounded");
    let (me, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&dir).unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.hydrate().await.unwrap();
    let target = resolved(&dest);
    for i in 0..7 {
        writer.direct_failed(&target, &format!("refused {i}"));
        wait_rows(storage.as_ref(), i + 1).await;
    }
    let history = storage.history().await.unwrap();
    assert_eq!(history.len(), 7, "every failure joins the raw log");
    // The index write trails the history append by one task step: read it once it names the
    // seventh ordinal.
    let mut index = storage.connections().await.unwrap();
    for _ in 0..200 {
        if index.first().and_then(|r| r.recovery.as_ref()).is_some_and(|r| r.attempt_ordinal == 7) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        index = storage.connections().await.unwrap();
    }
    assert_eq!(index.len(), 1, "one Direct member for the pair: {index:?}");
    assert_eq!(index[0].recovery.as_ref().map(|r| (r.recovery_epoch, r.attempt_ordinal)), Some((1, 7)), "the ordinal counts up in one epoch");
    assert_eq!(held.lock().unwrap().own_latest_directs().len(), 1);
    // A Direct Connected ends the series and is the pair's one Direct member now.
    writer.direct_connected(&target);
    wait_rows(storage.as_ref(), 8).await;
    let mut index = storage.connections().await.unwrap();
    for _ in 0..200 {
        if index.first().is_some_and(|r| r.state == ConnectionState::Connected) {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        index = storage.connections().await.unwrap();
    }
    assert_eq!(index.len(), 1);
    assert_eq!(index[0].state, ConnectionState::Connected);
    assert!(reconnect_plan(&held.lock().unwrap(), |_| None).is_empty(), "nothing is owed after a connect");
    let _ = std::fs::remove_dir_all(&dir);
}

/// §10: a Direct Connected beside an active Proxy owes the Proxy's retirement, written durably
/// as Disconnected(direct-restored); the route cuts back only once it lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_direct_connected_beside_a_proxy_is_retired_durably_and_the_route_cuts_back() {
    let dir = data_dir("retire");
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&dir).unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = ConnectionsWriter::new(me.clone(), storage.clone(), held.clone());
    writer.hydrate().await.unwrap();
    writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 11)).await.unwrap();
    writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 12)).await.unwrap();
    writer.direct_connected(&resolved(&dest));
    wait_rows(storage.as_ref(), 3).await;
    assert!(matches!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::ViaPeer { .. }), "the Proxy stays effective until retired");
    assert_eq!(writer.retire_owed().await.unwrap(), 1);
    let retired = storage.history().await.unwrap().into_iter().last().unwrap();
    assert_eq!((retired.kind, retired.state, retired.reason.as_deref()), (ConnectionKind::Proxy, ConnectionState::Disconnected, Some("direct-restored")));
    assert_eq!(resolve(&held.lock().unwrap(), &me.name, &dest.name, POLICY).route, EffectiveRoute::Direct { known: true });
    assert_eq!(writer.retire_owed().await.unwrap(), 0, "nothing owed once the retirement landed");
    let _ = std::fs::remove_dir_all(&dir);
}
