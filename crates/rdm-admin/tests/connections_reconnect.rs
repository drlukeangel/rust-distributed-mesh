//! i143.e6.s11 acceptance (rafka-v2 #2938, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2938-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span the cell's own
//! threads emitted, captured by its `EvidenceCapture`) there.

use rdm_core::connections::{resolve, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rdm_core::reconnect::reconnect_plan;
use rdm_core::{IncarnationId, NodeId, NodeKind};
use rdm_admin::connections_writer::ConnectionsWriter;
use rdm_admin::storage::{ConnectionsStorage, FileConnectionsStorage};
use rdm_transport::{Budget, CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rdm_core::outcome::RpcOutcome;
use rdm_core::ping::{Ping, PingRequest};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const POLICY: CarrierPolicy = CarrierPolicy::Proxy { carrier_kind: NodeKind::RpcNode };
const ATTEMPTS: u32 = 7;

/// The cell's artifact directory: the gate's, else this cell's own under the workspace target.
fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2938/unit").join(cell),
    }
}

fn end(name: &str) -> ConnectionEnd {
    ConnectionEnd { name: name.parse().unwrap(), node_id: NodeId::mint(), incarnation: Some(IncarnationId::mint()) }
}

fn row(source: &ConnectionEnd, destination: &ConnectionEnd, kind: ConnectionKind, state: ConnectionState, carrier: Option<&ConnectionEnd>, at: u64) -> NodeConnection {
    NodeConnection { id: rdm_core::ConnectionId::mint(), source: source.clone(), destination: destination.clone(), kind, alpn: rdm_core::connections::ALPN.to_vec(), state, carrier: carrier.cloned(), recovery: None, reason: None, logged_at_ms: at }
}

/// CONN-D (connections.md §13 D, §3, §9): many completed NotSent dial attempts from this source
/// to one exact destination birth advance the Direct Failed round/ordinal durably, grow the raw
/// log by one row each, and leave the current index holding one Direct and one Proxy for the
/// pair; superseded evidence never revives.
#[test]
fn source_reconnect_failures_grow_log_keep_two_current_members() {
    let cell = "source_reconnect_failures_grow_log_keep_two_current_members";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let capture = rdm_transport::EvidenceCapture::start(&dir, "rafka-node-admin-core-acceptance").expect("the cell's evidence file can be created");
    capture.run(4, async {
        let data_dir = std::env::temp_dir().join(format!("i143-2938-unit-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&data_dir);

        // The source, its carrier's connection and its own Proxy to the destination (the previous birth's
        // facts), then the destination made unreachable: a key nobody serves, a port nobody listens on.
        let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
        let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&rdm_admin::store_root::StoreRoot::at(&data_dir), &"mesh1.rpc.1".parse().unwrap(), &rdm_admin::store_log::LogWriter::new(&rdm_admin::model::NodeId::mint(), &rdm_admin::model::IncarnationId::parse(&rdm_core::fixture_crockford60("test")).unwrap())).await.unwrap());
        let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
        let writer = Arc::new(ConnectionsWriter::new(me.clone(), storage.clone(), held.clone()));
        writer.record(row(&carrier, &dest, ConnectionKind::Direct, ConnectionState::Connected, None, 10)).await.unwrap();
        writer.record(row(&me, &dest, ConnectionKind::Proxy, ConnectionState::Connected, Some(&carrier), 11)).await.unwrap();
        let seeded = storage.history().await.unwrap().len();
        let destination = ResolvedNode {
            node_id: dest.node_id.clone(),
            name: dest.name.clone(),
            endpoint_id: iroh::SecretKey::generate().public(),
            transport_addr: "127.0.0.1:1".parse().unwrap(),
            incarnation: dest.incarnation.clone().unwrap(),
        };
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(destination.clone());
        let ep = rdm_transport::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let client = NodeRpcClient::new(ep, resolver).with_caller_system("rdm").with_connection_observer(writer.clone());
        let opts = CallOptions { budget: Budget::Overall(Duration::from_millis(300)), ..Default::default() };

        // ATTEMPTS completed NotSent dials, each one's completion paired with the rows it left.
        let mut attempts = Vec::new();
        for ordinal in 1..=ATTEMPTS {
            let (out, _) = client.call::<Ping>(&NodeTarget::ExactNode(destination.node_id.clone()), &PingRequest::Ping { payload: vec![ordinal as u8] }, &opts).await;
            let reason = match &out {
                RpcOutcome::NotSent(n) => format!("{:?}", n.reason()),
                other => panic!("attempt {ordinal} did not end NotSent: {other:?}"),
            };
            // The index write of this attempt's Direct Failed row, as the storage holds it.
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            let indexed = loop {
                let rows = storage.connections().await.unwrap();
                if let Some(r) = rows.iter().find(|r| r.kind == ConnectionKind::Direct && r.source.name == me.name && r.destination.name == dest.name && r.recovery.is_some_and(|x| x.attempt_ordinal == ordinal)) {
                    break r.clone();
                }
                assert!(std::time::Instant::now() < deadline, "attempt {ordinal}'s Direct Failed row never reached the index: {rows:?}");
                tokio::time::sleep(Duration::from_millis(10)).await;
            };
            let history_len = storage.history().await.unwrap().len();
            assert_eq!(indexed.state, ConnectionState::Failed);
            assert_eq!(indexed.recovery.unwrap().recovery_round, 1, "one incident, one round");
            assert_eq!(history_len, seeded + ordinal as usize, "the raw log grows by one row per attempt");
            attempts.push(json!({ "ordinal": ordinal, "outcome": out.name(), "reason": reason, "indexed_row": indexed, "history_len": history_len }));
        }

        // The current index: one Direct and one Proxy for the pair; the held projection the same
        // (a fact is held once its write has landed, so the last attempt's write completes first).
        writer.observations_landed().await;
        let index = storage.connections().await.unwrap();
        let pair: Vec<&NodeConnection> = index.iter().filter(|r| r.source.name == me.name && r.destination.name == dest.name).collect();
        assert_eq!(pair.iter().filter(|r| r.kind == ConnectionKind::Direct).count(), 1, "{pair:?}");
        assert_eq!(pair.iter().filter(|r| r.kind == ConnectionKind::Proxy).count(), 1, "{pair:?}");
        let direct = pair.iter().find(|r| r.kind == ConnectionKind::Direct).unwrap();
        assert_eq!(direct.recovery.unwrap().attempt_ordinal, ATTEMPTS);
        let history = storage.history().await.unwrap();
        assert_eq!(history.len(), seeded + ATTEMPTS as usize);
        let (latest_directs, active_proxies, route, plan) = {
            let h = held.lock().unwrap();
            (
                h.own_latest_directs().into_iter().cloned().collect::<Vec<_>>(),
                h.own_active_proxies().into_iter().cloned().collect::<Vec<_>>(),
                resolve(&h, &me.name, &dest.name, POLICY).route,
                reconnect_plan(&h, |_| None),
            )
        };
        assert_eq!(latest_directs.len(), 1);
        assert_eq!(active_proxies.len(), 1, "the Proxy is the pair's other current member");
        assert!(matches!(route, EffectiveRoute::ViaPeer { .. }), "new calls ride the Proxy while Direct fails: {route:?}");
        assert_eq!(plan.len(), 1);
        assert_eq!(plan[0].recovery.attempt_ordinal, ATTEMPTS, "the reconnect schedule continues from the latest failure");

        // The evidence: direct observations, then every span the cell's own threads emitted. Every
        // observation write has landed (and closed its span) before the capture is read.
        writer.observations_landed().await;
        assert_eq!(writer.in_flight(), 0);
        let spans = capture.spans().expect("the cell's spans can be read");
        let observed: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.connection.update.via-observed").collect();
        assert!(observed.len() >= ATTEMPTS as usize, "every observed attempt carries its span: {} of {ATTEMPTS}", observed.len());
        std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
        let result = json!({
            "cell": cell,
            "clause": "CONN-D",
            "source": me, "destination": dest, "carrier": carrier,
            "destination_transport_addr": destination.transport_addr.to_string(),
            "attempts": attempts,
            "seeded_history_rows": seeded,
            "history_len": history.len(),
            "index_for_pair": pair,
            "held": { "own_latest_directs": latest_directs, "own_active_proxies": active_proxies, "route": format!("{route:?}") },
            "reconnect_plan_ordinal": plan[0].recovery.attempt_ordinal,
            "spans_emitted": spans.len(),
            "observed_spans": observed.len(),
        });
        std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
        let _ = std::fs::remove_dir_all(&data_dir);
    });
}
