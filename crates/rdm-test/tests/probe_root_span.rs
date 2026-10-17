//! Functional: the probe's root span is exported on every run, whatever the call did.
//!
//! The probe's `rdm.node_rpc.proof_store.resolve.via-probe` span is the root of the one trace of
//! one invocation; a scenario reads the call's trace from it. A call that dials a live node starts
//! connection work under the root; that work must not keep the root open past the process's exit.

use iroh::protocol::Router;
use iroh::SecretKey;
use rdm_core::launch::Launch;
use rdm_core::{FabricId, IncarnationId, NodeId};
use rdm_transport::{ServedBirth, ServerBuilder};
use rdm_test::proof_store::{self, FileProofStore};
use crate::common::admin_view::{self, node};
use std::sync::Arc;

/// CONTRACT: forty probe invocations (twenty direct, twenty carried through a gateway), each a real Get against a live proof-store node (the call
/// dials, opens a stream, reads a reply), each leave their `via-probe` root span in their own span
/// file, in one trace with the call's spans. What must NOT happen: a run whose file holds the call's
/// spans and not their root.
#[test]
fn forty_probe_runs_direct_and_carried_each_export_their_root_span() {
    let cap = crate::common::capture("forty_probe_runs_direct_and_carried_each_export_their_root_span");
    cap.run(crate::common::Threads::Workers(4), async move {
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
            store_dir: data.clone(),
            mesh_id: None,
            mesh_issuer: None,
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        };
        let store = Arc::new(FileProofStore::open(&launch.data_dir).unwrap());
        let server = proof_store::serve(ServerBuilder::new(), store, &launch, &rdm_test::held_name::HeldName::of_birth(launch.name.clone()))
            .carry::<rdm_test::proof_store::ProofStore>()
            .seal(ServedBirth { node_id: launch.node_id.to_string(), incarnation: launch.incarnation.to_string() })
            .unwrap();
        let ep = rdm_transport::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _router = Router::builder(ep).accept(rdm_transport::ALPN, server).spawn();
        // A carrier: a node that serves Forward and makes one inner call to the target.
        let target_record = rdm_transport::ResolvedNode { node_id: launch.node_id.clone(), name: launch.name.clone(), endpoint_id: key.public(), transport_addr: addr, incarnation: launch.incarnation.clone() };
        let carrier_key = SecretKey::generate();
        let (carrier_id, carrier_inc) = (NodeId::mint(), IncarnationId::mint());
        let carrier_resolver = Arc::new(rdm_transport::StaticResolver::new());
        carrier_resolver.insert(target_record.clone());
        let carrier_client = Arc::new(rdm_transport::NodeRpcClient::new(rdm_transport::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap(), carrier_resolver));
        let carrier_server = ServerBuilder::new()
            .carry::<rdm_test::proof_store::ProofStore>()
            .serve_forward(carrier_client)
            .seal(ServedBirth { node_id: carrier_id.to_string(), incarnation: carrier_inc.to_string() })
            .unwrap();
        let cep = rdm_transport::endpoint::bind(carrier_key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let caddr = cep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _crouter = Router::builder(cep).accept(rdm_transport::ALPN, carrier_server).spawn();
        let served = admin_view::serve(vec![
            node("mesh1.rpc.1", &launch.node_id, Some(key.public().to_string()), Some(addr), Some(launch.incarnation.clone())),
            node("mesh1.gateway.1", &carrier_id, Some(carrier_key.public().to_string()), Some(caddr), Some(carrier_inc.clone())),
        ])
        .await;
        let admin = served.birth_arg();
        let evidence = std::env::temp_dir().join(format!("rdm-probe-root-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&evidence);
        let mut missing = Vec::new();
        for run in 0..40 {
            // Runs 0-19 call the node directly; runs 20-39 are carried through the gateway.
            let via: Vec<String> = if run >= 20 { vec!["--via".into(), "path:mesh1.gateway.1".into()] } else { vec![] };
            let (admin, evidence_dir, id) = (admin.clone(), evidence.clone(), launch.node_id.to_string());
            // CONTRACT: the cell counts span files (`*.spans.jsonl`) only. The evidence dir is the
            // probe's one log location, so it also holds the probe's transport diagnostic file
            // (R-QH1), which is not a span file.
            let span_files = |dir: &std::path::Path| -> Vec<std::path::PathBuf> {
                std::fs::read_dir(dir).into_iter().flatten().flatten().map(|e| e.path()).filter(|p| p.to_string_lossy().ends_with(".spans.jsonl")).collect()
            };
            let before: std::collections::BTreeSet<_> = span_files(&evidence).into_iter().collect();
            let out = tokio::task::spawn_blocking(move || {
                std::process::Command::new(env!("CARGO_BIN_EXE_rafka-rpc-probe"))
                    .args(["--admin-birth", &admin, "get", "--target", &format!("exact:{id}"), "--key", "k"])
                    .args(&via)
                    .env("RDM_EVIDENCE_DIR", &evidence_dir)
                    .output()
                    .unwrap()
            })
            .await
            .unwrap();
            let v: serde_json::Value = serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap_or_else(|e| panic!("run {run}: no JSON ({e}): {}", String::from_utf8_lossy(&out.stderr)));
            assert_eq!(v["outcome"], "Reply", "run {run}: the call reaches the live node: {v}");
            let fresh: Vec<_> = span_files(&evidence).into_iter().filter(|p| !before.contains(p)).collect();
            assert_eq!(fresh.len(), 1, "run {run}: one invocation leaves one span file");
            let spans = std::fs::read_to_string(&fresh[0]).unwrap();
            if !spans.contains("rdm.node_rpc.proof_store.resolve.via-probe") {
                missing.push(format!("run {run}: {} spans, no root", spans.lines().count()));
            }
        }
        served.shutdown().await;
        let _ = std::fs::remove_dir_all(&evidence);
        let _ = std::fs::remove_dir_all(&data);
        assert!(missing.is_empty(), "runs whose span file holds the call's spans and not their root: {missing:?}");
    })
}

/// CONTRACT: a probe that refuses its arguments exits with status 2, prints the typed `Refused` line with
/// the reason, AND leaves a span file holding the refusal span with that reason. An error state keeps its
/// evidence: the process ends by returning from `main`, so the telemetry guard flushes first.
#[test]
fn a_probe_that_refuses_its_arguments_exits_two_and_keeps_its_refusal_span() {
    let evidence = std::env::temp_dir().join(format!("rdm-probe-refusal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&evidence);
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_rafka-rpc-probe")).args(["--no-such-flag", "x"]).env("RDM_EVIDENCE_DIR", &evidence).output().unwrap();
    assert_eq!(out.status.code(), Some(2), "{:?}", out.status);
    let v: serde_json::Value = serde_json::from_str(String::from_utf8_lossy(&out.stdout).trim()).unwrap();
    assert_eq!(v["outcome"], "Refused", "{v}");
    let reason = v["reason"].as_str().unwrap().to_string();
    assert!(reason.contains("--no-such-flag"), "the refusal names the argument: {reason}");
    let spans: String = std::fs::read_dir(&evidence).unwrap().flatten().filter(|e| e.file_name().to_string_lossy().starts_with("rafka-rpc-probe.")).map(|e| std::fs::read_to_string(e.path()).unwrap_or_default()).collect();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(spans.contains("rdm.node_rpc.proof_store.reject.via-probe-arguments"), "the refusal left no span: {spans:?}; the probe's stderr: {stderr}");
    assert!(spans.contains("--no-such-flag"), "the refusal span carries its reason: {spans:?}");
    let _ = std::fs::remove_dir_all(&evidence);
}
