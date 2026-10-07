//! i143.e6.s11 acceptance (rafka-v2 #2938, hardened 2026-10-07), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2938-unit`, which exports `I143_ACCEPTANCE_DIR`; the
//! cell leaves `result.json` (its direct observations) and `spans.json` (every span this process
//! emitted, captured in-process by the evidence exporter) there.

use rafka_mesh_entity::connections::{resolve, CarrierPolicy, ConnectionEnd, ConnectionKind, ConnectionState, ConnectionsHeld, EffectiveRoute, NodeConnection};
use rafka_mesh_entity::reconnect::reconnect_plan;
use rafka_mesh_entity::{IncarnationId, NodeId, NodeKind};
use rafka_node_admin_core::connections_writer::ConnectionsWriter;
use rafka_node_admin_core::storage::{ConnectionsStorage, FileConnectionsStorage};
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_contract::ping::{Ping, PingRequest};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

const POLICY: CarrierPolicy = CarrierPolicy::Forwardable { carrier_kind: NodeKind::RpcNode };
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
    NodeConnection { source: source.clone(), destination: destination.clone(), kind, state, carrier: carrier.cloned(), recovery: None, reason: None, logged_at_ms: at }
}

/// Every span this process emitted, from the evidence exporter's JSONL files in `dir`.
fn collect_spans(dir: &std::path::Path) -> Vec<Value> {
    let mut spans = Vec::new();
    for e in std::fs::read_dir(dir).unwrap().flatten() {
        let p = e.path();
        if p.extension().is_some_and(|x| x == "jsonl") && p.file_name().is_some_and(|n| n.to_string_lossy().ends_with(".spans.jsonl")) {
            for line in std::fs::read_to_string(&p).unwrap().lines() {
                if let Ok(v) = serde_json::from_str::<Value>(line) {
                    spans.push(v);
                }
            }
        }
    }
    spans
}

/// CONN-D (connections.md §13 D, §3, §9): many completed NotSent dial attempts from this source
/// to one exact destination birth advance the Direct Failed epoch/ordinal durably, grow the raw
/// log by one row each, and leave the current index holding one Direct and one Proxy for the
/// pair; superseded evidence never revives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_reconnect_failures_grow_log_keep_two_current_members() {
    let cell = "source_reconnect_failures_grow_log_keep_two_current_members";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    std::env::set_var("RAFKA_EVIDENCE_DIR", &dir);
    let telemetry = rafka_mesh_telemetry::init_evidence_telemetry("rafka-node-admin-core-acceptance");
    let data_dir = std::env::temp_dir().join(format!("i143-2938-unit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data_dir);

    // The source, its carrier's edge and its own Proxy to the destination (the previous birth's
    // facts), then the destination made unreachable: a key nobody serves, a port nobody listens on.
    let (me, carrier, dest) = (end("mesh1.rpc.1"), end("mesh1.rpc.2"), end("mesh1.rpc.3"));
    let storage: Arc<dyn ConnectionsStorage> = Arc::new(FileConnectionsStorage::open(&data_dir).unwrap());
    let held = Arc::new(Mutex::new(ConnectionsHeld::new()));
    let writer = Arc::new(ConnectionsWriter::new(me.clone(), storage.clone(), held.clone()));
    writer.hydrate().await.unwrap();
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
    let ep = rafka_node_rpc::endpoint::bind(iroh::SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
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
        assert_eq!(indexed.recovery.unwrap().recovery_epoch, 1, "one incident, one epoch");
        assert_eq!(history_len, seeded + ordinal as usize, "the raw log grows by one row per attempt");
        attempts.push(json!({ "ordinal": ordinal, "outcome": out.name(), "reason": reason, "indexed_row": indexed, "history_len": history_len }));
    }

    // The current index: one Direct and one Proxy for the pair; the held projection the same.
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

    // The evidence: direct observations, then every span this process emitted. Every observation
    // write has landed (and closed its span) before the exporter is shut down.
    writer.drain().await;
    assert_eq!(writer.in_flight(), 0);
    drop(telemetry);
    let spans = collect_spans(&dir);
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
}
