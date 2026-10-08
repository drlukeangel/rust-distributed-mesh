//! Functional: the probe's root span is exported on every run, whatever the call did.
//!
//! The probe's `rdm.node_rpc.proof_store.resolve.via-probe` span is the root of the one trace of
//! one invocation; a scenario reads the call's trace from it. A call that dials a live node starts
//! connection work under the root; that work must not keep the root open past the process's exit.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_rpc::{ServedBirth, ServerBuilder};
use rafka_node_rpc_testkit::proof_store::{self, FileProofStore};
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;

fn serve_view(view: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    std::thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            let mut stream = stream;
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = write!(stream, "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{view}", view.len());
        }
    });
    base
}

/// CONTRACT: forty probe invocations (twenty direct, twenty carried through a gateway), each a real Get against a live proof-store node (the call
/// dials, opens a stream, reads a reply), each leave their `via-probe` root span in their own span
/// file, in one trace with the call's spans. What must NOT happen: a run whose file holds the call's
/// spans and not their root.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forty_probe_runs_direct_and_carried_each_export_their_root_span() {
    let data = std::env::temp_dir().join(format!("rdm-probe-root-data-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&data);
    std::fs::create_dir_all(&data).unwrap();
    let key = SecretKey::generate();
    let launch = Launch {
        fabric: "fabric1".into(),
        fabric_id: FabricId::mint(),
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: Vec::new(),
        seeds: Vec::new(),
        launcher: None,
        data_dir: data.clone(),
        mesh_id: None,
    };
    let store = Arc::new(FileProofStore::open(&launch.data_dir).unwrap());
    let server = proof_store::serve(ServerBuilder::new(), store, &launch)
        .carry::<rafka_node_rpc_testkit::proof_store::ProofStore>()
        .seal(ServedBirth { node_id: launch.node_id.to_string(), incarnation: launch.incarnation.to_string() })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let _router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    // A carrier: a node that serves Forward and makes one inner call to the target.
    let target_record = rafka_node_rpc::ResolvedNode { node_id: launch.node_id.clone(), name: launch.name.clone(), endpoint_id: key.public(), transport_addr: addr, incarnation: launch.incarnation.clone() };
    let carrier_key = SecretKey::generate();
    let (carrier_id, carrier_inc) = (NodeId::mint(), IncarnationId::mint());
    let carrier_resolver = Arc::new(rafka_node_rpc::StaticResolver::new());
    carrier_resolver.insert(target_record.clone());
    let carrier_client = Arc::new(rafka_node_rpc::NodeRpcClient::new(rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap(), carrier_resolver));
    let carrier_server = ServerBuilder::new()
        .carry::<rafka_node_rpc_testkit::proof_store::ProofStore>()
        .serve_forward(carrier_client)
        .seal(ServedBirth { node_id: carrier_id.to_string(), incarnation: carrier_inc.0.clone() })
        .unwrap();
    let cep = rafka_node_rpc::endpoint::bind(carrier_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let caddr = cep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let _crouter = Router::builder(cep).accept(rafka_node_rpc::ALPN, carrier_server).spawn();
    let view = serde_json::json!({"nodes": [
        {"node_id": launch.node_id.to_string(), "name": "mesh1.rpc.1", "endpoint_id": key.public().to_string(), "transport_addr": addr.to_string(), "incarnation_id": launch.incarnation.to_string()},
        {"node_id": carrier_id.to_string(), "name": "mesh1.gateway.1", "endpoint_id": carrier_key.public().to_string(), "transport_addr": caddr.to_string(), "incarnation_id": carrier_inc.0},
    ]})
    .to_string();
    let admin = serve_view(view);
    let evidence = std::env::temp_dir().join(format!("rdm-probe-root-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&evidence);
    let mut missing = Vec::new();
    for run in 0..40 {
        // Runs 0-19 call the node directly; runs 20-39 are carried through the gateway.
        let via: Vec<String> = if run >= 20 { vec!["--via".into(), "path:mesh1.gateway.1".into()] } else { vec![] };
        let (admin, evidence_dir, id) = (admin.clone(), evidence.clone(), launch.node_id.to_string());
        let before: std::collections::BTreeSet<_> = std::fs::read_dir(&evidence).into_iter().flatten().flatten().map(|e| e.path()).collect();
        let out = tokio::task::spawn_blocking(move || {
            std::process::Command::new(env!("CARGO_BIN_EXE_rafka-rpc-probe"))
                .args(["--admin", &admin, "get", "--target", &format!("exact:{id}"), "--key", "k"])
                .args(&via)
                .env("RDM_EVIDENCE_DIR", &evidence_dir)
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        let v: serde_json::Value = serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_else(|e| panic!("run {run}: no JSON ({e}): {}", String::from_utf8_lossy(&out.stderr)));
        assert_eq!(v["outcome"], "Reply", "run {run}: the call reaches the live node: {v}");
        let fresh: Vec<_> = std::fs::read_dir(&evidence).unwrap().flatten().map(|e| e.path()).filter(|p| !before.contains(p)).collect();
        assert_eq!(fresh.len(), 1, "run {run}: one invocation leaves one span file");
        let spans = std::fs::read_to_string(&fresh[0]).unwrap();
        if !spans.contains("rdm.node_rpc.proof_store.resolve.via-probe") {
            missing.push(format!("run {run}: {} spans, no root", spans.lines().count()));
        }
    }
    let _ = std::fs::remove_dir_all(&evidence);
    let _ = std::fs::remove_dir_all(&data);
    assert!(missing.is_empty(), "runs whose span file holds the call's spans and not their root: {missing:?}");
}

/// CONTRACT: a probe that refuses its arguments exits with status 2, prints the typed `Refused` line with
/// the reason, AND leaves a span file holding the refusal span with that reason. An error state keeps its
/// evidence: the process ends by returning from `main`, so the telemetry guard flushes first.
#[test]
fn a_probe_that_refuses_its_arguments_exits_two_and_keeps_its_refusal_span() {
    let evidence = std::env::temp_dir().join(format!("rdm-probe-refusal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&evidence);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-rpc-probe")).args(["--admin", "http://127.0.0.1:9", "--no-such-flag", "x"]).env("RDM_EVIDENCE_DIR", &evidence).output().unwrap();
    assert_eq!(out.status.code(), Some(2), "{:?}", out.status);
    let v: serde_json::Value = serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap();
    assert_eq!(v["outcome"], "Refused", "{v}");
    let reason = v["reason"].as_str().unwrap().to_string();
    assert!(reason.contains("--no-such-flag"), "the refusal names the argument: {reason}");
    let spans: String = std::fs::read_dir(&evidence).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().starts_with("rafka-rpc-probe.")).map(|e| std::fs::read_to_string(e.path()).unwrap_or_default()).collect();
    assert!(spans.contains("rdm.node_rpc.proof_store.reject.via-probe-arguments"), "the refusal left no span: {spans:?}");
    assert!(spans.contains("--no-such-flag"), "the refusal span carries its reason: {spans:?}");
    let _ = std::fs::remove_dir_all(&evidence);
}
