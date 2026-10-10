//! i143 R-J1 (Luke): the join is a Node RPC op (`JoinNode`, `0x1D`), UNIT layer: run by
//! `scripts/i143-acceptance-gate.sh i143-rj1-unit`, which exports `I143_ACCEPTANCE_DIR`; the cell
//! leaves `result.json` (its direct observations) and `spans.json` (every span of its own
//! runtime, captured by an in-memory OTel exporter) there.
//!
//! The admin holds what it deployed and verifies the digest a node reports against it. A digest
//! that disagrees (another incarnation, another node id, another endpoint) is refused by name,
//! with both values, and installs nothing. A digest that matches installs the address the node
//! reported for its key and completes the wait for the node's own report.

use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId, RuntimeFact};
use rafka_node_admin_core::join::{Deployed, JoinDoor, Joins};
use rafka_node_rpc_contract::join::{JoinReply, JoinRequest};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

fn acceptance_dir(cell: &str) -> PathBuf {
    match std::env::var("I143_ACCEPTANCE_DIR") {
        Ok(d) => PathBuf::from(d),
        Err(_) => PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join("target/i143-acceptance/rj1/unit").join(cell),
    }
}

/// This cell's own span capture: an in-memory OTel exporter behind a dispatcher installed on
/// every thread of the cell's own runtime.
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
    let service = format!("i143-rj1-{cell}");
    let provider = opentelemetry_sdk::trace::TracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .with_resource(opentelemetry_sdk::Resource::new([opentelemetry::KeyValue::new("service.name", service.clone())]))
        .build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-rj1"));
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


fn deployed() -> Deployed {
    Deployed {
        name: "mesh1.rpc.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        endpoint_id: EndpointId("node-key".into()),
        runtime: RuntimeFact::of_this_process("dep-rj1").unwrap(),
        data_dir: "/data/mesh1.rpc.1".into(),
    }
}

fn digest_of(d: &Deployed, addr: &str) -> MeshDigest {
    MeshDigest {
        fabric_id: FabricId::mint(),
        node: MeshNode {
            node_id: d.node_id.clone(),
            name: d.name.clone(),
            endpoint_id: d.endpoint_id.clone(),
            transport_addr: addr.parse().unwrap(),
            incarnation: d.incarnation.clone(),
            supersedes: d.supersedes.clone(),
            runtime: Some(d.runtime.clone()),
        },
        status: MemberStatus::Pending,
        admin_api_base: None,
        digest_seq: 0,
        emitted_at_rafka_ms: 0,
        data_dir: Some(d.data_dir.clone()),
        mesh_id: None,
        in_flight: None,
        extra: Default::default(),
        load: None,
        gossip: None,
    }
}

/// CONTRACT (R-J1): the admin that deployed a birth answers its `JoinNode` only for the birth it
/// deployed. A digest with another incarnation is refused as `JoinMismatch` naming the field and
/// both incarnations; a digest of a node this admin deployed nothing for is `NotAuthority`;
/// neither installs an address or completes the wait for the node's report. The matching digest
/// installs exactly the address the node reported (the OS-assigned port), and that report is what
/// the deployment's `WaitForBind` receives. A digest sent by an endpoint other than the one it
/// names is `Unauthorized`.
#[test]
fn join_refuses_a_digest_that_disagrees_with_the_deployment_and_installs_the_reported_address_on_a_match() {
    let cell = "join_refuses_a_digest_that_disagrees_with_the_deployment_and_installs_the_reported_address_on_a_match";
    let dir = acceptance_dir(cell);
    std::fs::create_dir_all(&dir).unwrap();
    let cap = capture(cell);
    let result = cap.run(async {
        let joins = Arc::new(Joins::default());
        let installed: Arc<Mutex<Vec<std::net::SocketAddr>>> = Arc::default();
        let log = installed.clone();
        let door = JoinDoor {
            me: "mesh1.admin.1".parse().unwrap(),
            joins: joins.clone(),
            answer: Arc::new(|| {
                Box::pin(async {
                    Ok(rafka_node_admin_core::wire::JoinAnswer {
                        served_by: "mesh1.admin.1".into(),
                        control: rafka_node_admin_core::wire::JoinControl { provider: rafka_node_admin_core::model::ProviderKind::Process, fabric: None, shutdown: None, build: None, rafka_time_ms: 1_000 },
                        statuses: vec![],
                    })
                })
            }),
            install: Arc::new(move |d| log.lock().unwrap().push(d.node.transport_addr)),
            known: Arc::new(|| Box::pin(async {})),
            primary: Arc::new(|| Some("mesh1.admin.2".into())),
        };
        let dep = deployed();
        let reported = joins.expect(dep.clone());
        let ask = |d: &MeshDigest| JoinRequest::JoinNode { digest: d.into() };
        let peer = dep.endpoint_id.clone();

        let mut wrong_incarnation = digest_of(&dep, "127.0.0.1:34567");
        wrong_incarnation.node.incarnation = IncarnationId::mint();
        let mismatch = door.serve(peer.clone(), ask(&wrong_incarnation)).await;
        match &mismatch {
            JoinReply::JoinMismatch { field, deployed, reported } => {
                assert_eq!(field, "incarnation");
                assert!(deployed.contains(&dep.incarnation.0) && reported.contains(&wrong_incarnation.node.incarnation.0), "{mismatch:?}");
            }
            other => panic!("{other:?}"),
        }
        let mut wrong_node = digest_of(&dep, "127.0.0.1:34567");
        wrong_node.node.node_id = NodeId::mint();
        let unknown = door.serve(peer.clone(), ask(&wrong_node)).await;
        assert_eq!(unknown, JoinReply::NotAuthority { primary: Some("mesh1.admin.2".into()) });
        assert!(installed.lock().unwrap().is_empty(), "a refused join installs nothing");
        assert!(reported.borrow().is_none(), "a refused join completes no WaitForBind");

        let good = digest_of(&dep, "127.0.0.1:34567");
        let joined = door.serve(peer.clone(), ask(&good)).await;
        assert!(matches!(joined, JoinReply::Joined { .. }), "{joined:?}");
        assert_eq!(*installed.lock().unwrap(), vec![good.node.transport_addr], "the reported address is installed for the key");
        assert_eq!(reported.borrow().as_ref().map(|d| d.node.transport_addr), Some(good.node.transport_addr), "WaitForBind receives the node's own report");
        let forged = door.serve(EndpointId("someone-else".into()), ask(&good)).await;
        assert!(matches!(forged, JoinReply::Unauthorized { .. }), "{forged:?}");
        json!({
            "cell": cell,
            "mismatch": format!("{mismatch:?}"),
            "unknown_node": format!("{unknown:?}"),
            "installed": installed.lock().unwrap().iter().map(|a| a.to_string()).collect::<Vec<_>>(),
            "forged_sender": format!("{forged:?}"),
        })
    });
    let spans = cap.spans();
    let names = |n: &str| spans.iter().filter(|s| s["name"] == n).count();
    assert_eq!(names("rdm.node_admin.node.reject.via-join-mismatch"), 1, "the refusal is a span of its own");
    assert_eq!(names("rdm.node_admin.node.reject.via-join-unknown"), 1);
    let refusal = spans.iter().find(|s| s["name"] == "rdm.node_admin.node.reject.via-join-mismatch").unwrap();
    assert_eq!(refusal["attributes"]["field"], "incarnation", "{refusal}");
    assert!(spans.iter().any(|s| s["name"] == "rdm.node_admin.node.update.via-join" && s["attributes"]["outcome"] == "installed"));
    std::fs::write(dir.join("spans.json"), serde_json::to_vec_pretty(&spans).unwrap()).unwrap();
    std::fs::write(dir.join("result.json"), serde_json::to_vec_pretty(&result).unwrap()).unwrap();
}
