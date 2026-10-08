//! i143.e7.s3 acceptance (rafka-v2 #2775), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-2775-unit`, which exports `I143_ACCEPTANCE_DIR`; the cell
//! leaves `result.json` (its direct observations) and `spans.json` (every span it emitted, captured
//! in-process by its own OTel exporter) there.
//!
//! The testkit proof store (op 0x70) is served by a real `NodeRpcServer` on its own endpoint, over
//! a store file in its own data dir, and called by a real `NodeRpcClient` naming the exact birth.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::launch::Launch;
use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use rafka_node_rpc_testkit::proof_store::{self, FileProofStore, ProofOp, ProofReply, ProofRequest, ProofStore, Provenance, MAX_KEY_BYTES, MAX_VALUE_BYTES};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::Arc;

fn acceptance_dir(cell: &str) -> PathBuf {
    let dir = match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/2775/unit").join(cell),
    };
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// This cell's own span capture: an in-memory OTel exporter behind a dispatcher installed on every
/// thread of the cell's own runtime.
struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
    service: String,
}

fn capture(cell: &str) -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let service = format!("i143-2775-{cell}");
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", service.clone())]))
        .build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-2775"));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)), service }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    fn spans(&self) -> Vec<Value> {
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({
                    "name": s.name,
                    "trace_id": s.span_context.trace_id().to_string(),
                    "span_id": s.span_context.span_id().to_string(),
                    "parent_span_id": s.parent_span_id.to_string(),
                    "resource": {"service.name": self.service},
                    "attributes": attributes,
                })
            })
            .collect()
    }
}

fn reply(out: RpcOutcome<ProofReply>) -> ProofReply {
    match out {
        RpcOutcome::Reply(r) => r.value().clone(),
        other => panic!("expected a reply from the proof store: {other:?}"),
    }
}

/// CONTRACT (#2775): every proof-store op served over real Node RPC answers its typed reply, and
/// each reply names exactly the birth that executed it (logical NodeId, path.name, mesh,
/// incarnation) and the op. A compare-and-swap whose expected value does not match answers
/// Mismatch with the current value and changes nothing; one that matches swaps to exactly the new
/// value. A key or value over its limit is refused by name (field, limit, size) and the store file
/// is byte-for-byte unchanged. Every call is served under its caller's trace. What must NOT
/// happen: a reply without provenance or naming another birth, a mismatched CAS that writes, or an
/// oversized write that touches the store.
#[test]
fn proof_holder_applies_cas_reports_exact_executing_birth() {
    let cell = "proof_holder_applies_cas_reports_exact_executing_birth";
    let dir = acceptance_dir(cell);
    let cap = capture(cell);
    let data_dir = dir.join("data");
    let _ = std::fs::remove_dir_all(&data_dir);
    std::fs::create_dir_all(&data_dir).unwrap();
    let observations = cap.run(async {
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
            data_dir: data_dir.clone(),
            mesh_id: None,
        };
        let store = Arc::new(FileProofStore::open(&launch.data_dir).expect("the store opens in its data dir"));
        let server = proof_store::serve(ServerBuilder::new(), store.clone(), &launch)
            .seal(ServedBirth { node_id: launch.node_id.to_string(), incarnation: launch.incarnation.to_string() })
            .expect("the proof store seals");
        let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
        let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
        let _router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server.clone()).spawn();
        let resolver = Arc::new(StaticResolver::new());
        resolver.insert(ResolvedNode { node_id: launch.node_id.clone(), name: launch.name.clone(), endpoint_id: key.public(), transport_addr: addr, incarnation: launch.incarnation.clone() });
        let client = NodeRpcClient::new(rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap(), resolver).with_caller_system("rdm");
        let target = NodeTarget::ExactNode(launch.node_id.clone());
        let call = |req: ProofRequest| {
            let (client, target) = (&client, &target);
            async move { reply(client.call::<ProofStore>(target, &req, &CallOptions::default()).await.0) }
        };
        let at = |op: ProofOp| Provenance {
            node_id: launch.node_id.to_string(),
            node: launch.name.to_string(),
            mesh: launch.name.mesh.clone(),
            incarnation_id: launch.incarnation.to_string(),
            op,
        };
        let k = b"k1".to_vec();
        let mut seen = Vec::new();
        let mut step = |label: &str, req: ProofRequest, r: ProofReply, want: ProofReply| {
            assert_eq!(r, want, "{label}");
            seen.push(json!({"step": label, "request": format!("{req:?}"), "reply": r}));
        };

        let r = call(ProofRequest::Put { key: k.clone(), value: b"v1".to_vec() }).await;
        step("put", ProofRequest::Put { key: k.clone(), value: b"v1".to_vec() }, r, ProofReply::Stored { at: at(ProofOp::Put) });
        let r = call(ProofRequest::Get { key: k.clone() }).await;
        step("get", ProofRequest::Get { key: k.clone() }, r, ProofReply::Value { at: at(ProofOp::Get), value: b"v1".to_vec() });
        // A mismatched CAS answers the current value and writes nothing.
        let before = std::fs::read(store.path()).unwrap();
        let req = ProofRequest::CompareAndSwap { key: k.clone(), expected: Some(b"nope".to_vec()), new: Some(b"x".to_vec()) };
        let r = call(req.clone()).await;
        step("cas-mismatch", req, r, ProofReply::Mismatch { at: at(ProofOp::CompareAndSwap), current: Some(b"v1".to_vec()) });
        assert_eq!(std::fs::read(store.path()).unwrap(), before, "a mismatched CAS leaves the store file unchanged");
        let r = call(ProofRequest::Get { key: k.clone() }).await;
        step("get-after-mismatch", ProofRequest::Get { key: k.clone() }, r, ProofReply::Value { at: at(ProofOp::Get), value: b"v1".to_vec() });
        // A matching CAS swaps to exactly the new value.
        let req = ProofRequest::CompareAndSwap { key: k.clone(), expected: Some(b"v1".to_vec()), new: Some(b"v2".to_vec()) };
        let r = call(req.clone()).await;
        step("cas-swap", req, r, ProofReply::Swapped { at: at(ProofOp::CompareAndSwap) });
        let r = call(ProofRequest::Get { key: k.clone() }).await;
        step("get-after-swap", ProofRequest::Get { key: k.clone() }, r, ProofReply::Value { at: at(ProofOp::Get), value: b"v2".to_vec() });
        let r = call(ProofRequest::Delete { key: k.clone() }).await;
        step("delete", ProofRequest::Delete { key: k.clone() }, r, ProofReply::Deleted { at: at(ProofOp::Delete) });
        let r = call(ProofRequest::Get { key: k.clone() }).await;
        step("get-after-delete", ProofRequest::Get { key: k.clone() }, r, ProofReply::Absent { at: at(ProofOp::Get) });
        // Limits: refused by name, the store file untouched.
        let r = call(ProofRequest::Put { key: b"kept".to_vec(), value: b"kv".to_vec() }).await;
        step("put-kept", ProofRequest::Put { key: b"kept".to_vec(), value: b"kv".to_vec() }, r, ProofReply::Stored { at: at(ProofOp::Put) });
        let before = std::fs::read(store.path()).unwrap();
        let long_key = vec![b'k'; MAX_KEY_BYTES + 1];
        let r = call(ProofRequest::Put { key: long_key.clone(), value: b"v".to_vec() }).await;
        step("put-key-too-large", ProofRequest::Put { key: b"<257 bytes>".to_vec(), value: b"v".to_vec() }, r, ProofReply::TooLarge { at: at(ProofOp::Put), field: "key".into(), limit: MAX_KEY_BYTES as u32, got: (MAX_KEY_BYTES + 1) as u32 });
        let big = vec![b'v'; MAX_VALUE_BYTES + 1];
        let r = call(ProofRequest::Put { key: b"big".to_vec(), value: big }).await;
        step("put-value-too-large", ProofRequest::Put { key: b"big".to_vec(), value: b"<65537 bytes>".to_vec() }, r, ProofReply::TooLarge { at: at(ProofOp::Put), field: "value".into(), limit: MAX_VALUE_BYTES as u32, got: (MAX_VALUE_BYTES + 1) as u32 });
        assert_eq!(std::fs::read(store.path()).unwrap(), before, "an oversized write leaves the store file unchanged");
        let r = call(ProofRequest::Get { key: b"big".to_vec() }).await;
        step("get-big-absent", ProofRequest::Get { key: b"big".to_vec() }, r, ProofReply::Absent { at: at(ProofOp::Get) });
        let r = call(ProofRequest::Get { key: b"kept".to_vec() }).await;
        step("get-kept", ProofRequest::Get { key: b"kept".to_vec() }, r, ProofReply::Value { at: at(ProofOp::Get), value: b"kv".to_vec() });
        json!({
            "birth": {"node_id": launch.node_id.to_string(), "node": launch.name.to_string(), "mesh": launch.name.mesh, "incarnation_id": launch.incarnation.to_string()},
            "store_file": store.path().display().to_string(),
            "steps": seen,
        })
    });
    let spans = cap.spans();
    // Every call is served at this birth, under its caller's trace, naming the op and its outcome.
    let served: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.proof_store.serve.via-request").collect();
    let calls: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_rpc.request.update.via-call").collect();
    let outcomes: Vec<(String, String)> = served.iter().map(|s| (s["attributes"]["op"].as_str().unwrap().to_string(), s["attributes"]["outcome"].as_str().unwrap_or("").to_string())).collect();
    let want: Vec<(String, String)> = [
        ("put", "stored"), ("get", "value"), ("compare-and-swap", "mismatch"), ("get", "value"), ("compare-and-swap", "swapped"), ("get", "value"),
        ("delete", "deleted"), ("get", "absent"), ("put", "stored"), ("put", "too-large"), ("put", "too-large"), ("get", "absent"), ("get", "value"),
    ]
    .iter()
    .map(|(a, b)| (a.to_string(), b.to_string()))
    .collect();
    let mut got_sorted = outcomes.clone();
    let mut want_sorted = want.clone();
    got_sorted.sort();
    want_sorted.sort();
    assert_eq!(got_sorted, want_sorted, "one served span per call, with its op and outcome: {outcomes:?}");
    let birth = &observations["birth"];
    for s in &served {
        assert_eq!(s["attributes"]["node_id"], birth["node_id"], "{s}");
        assert_eq!(s["attributes"]["incarnation_id"], birth["incarnation_id"], "{s}");
        assert_eq!(s["attributes"]["node"], birth["node"], "{s}");
        assert!(calls.iter().any(|c| c["trace_id"] == s["trace_id"]), "served under its caller's trace: {s}");
        assert_ne!(s["parent_span_id"], "0000000000000000", "a served span has a parent: {s}");
    }
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    let result = json!({"cell": cell, "observations": observations, "served": served.len(), "calls": calls.len()});
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
