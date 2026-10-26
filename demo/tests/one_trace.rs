//! One trace across two nodes, under the consumer's own tracing subscriber: the consumer installs its
//! own OpenTelemetry layer (no RDM telemetry set-up), opens its own span, and a Node RPC call from the
//! node-admin to the gateway inside it is one trace on both nodes: RDM's client sends the caller's
//! traceparent and the gateway's serve span takes it as its parent (node-rpc.md, the call's context).
//! Rafka's own subscriber reads RDM's spans this way.
//!
//! The call is the testkit's op `0x74` (`BeforeReadyPull`), standing in for a product's own op.
use opentelemetry::trace::TracerProvider as _;
use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
use rdm_admin::admin::{start_with, AdminConfig, Running};
use rdm_admin::wiring::{FabricHooks, Wiring};
use rdm_core::catalog::OpOwner;
use rdm_core::{IncarnationId, NodeId};
use rdm_node::before_ready::{BeforeReady, BeforeReadyCtx, BeforeReadyHook, HookOutcome};
use rdm_test::before_ready_probe::{BeforeReadyPull, PullReply, PullRequest, Row};
use rdm_transport::{CallOptions, NodeTarget};
use std::sync::Arc;
use std::time::Duration;
use tracing::Instrument as _;
use tracing_subscriber::layer::SubscriberExt as _;

const BOUND: Duration = Duration::from_secs(60);

fn config(dir: &std::path::Path) -> AdminConfig {
    AdminConfig::from_env(|k| match k {
        "RDM_DATA_DIR" | "RDM_STORE_DIR" | "RDM_BIN_DIR" => Some(dir.display().to_string()),
        "RDM_NODE_ADMIN_API_BIND" => Some("127.0.0.1:0".into()),
        "MESH_SPAWN_TYPE" => Some("process".into()),
        _ => None,
    })
    .unwrap()
}

/// The gateway's `before_ready`: nothing to pull.
struct Ready;
#[async_trait::async_trait]
impl BeforeReady for Ready {
    async fn before_ready(&self, _ctx: &BeforeReadyCtx) -> HookOutcome {
        HookOutcome::Ok
    }
}

/// The gateway, launched as the root admin's member, answering op `0x74`.
async fn gateway(root: &Running, dir: &std::path::Path, shared: &std::path::Path) -> (rdm_node::node::RunningNode, NodeId) {
    let d = root.digest.lock().unwrap().clone();
    std::fs::create_dir_all(dir).unwrap();
    let fact = rdm_core::RuntimeFact::of_this_process(&rdm_core::fixture_crockford60("demo-trace")).unwrap();
    fact.write_record(dir).unwrap();
    let key = rdm_node::node::load_or_mint_key(dir).unwrap();
    let launch = rdm_core::launch::Launch {
        fabric: config(shared).fabric,
        fabric_id: d.fabric_id.clone(),
        name: "mesh1.gateway.1".parse().unwrap(),
        node_id: NodeId::mint(),
        incarnation: IncarnationId::mint(),
        supersedes: None,
        bind_addr: "127.0.0.1:0".parse().unwrap(),
        listeners: vec![],
        declared: Vec::new(),
        seeds: vec![(d.node.endpoint_id.0.clone(), d.node.transport_addr)],
        launcher: Some(rdm_core::launch::Launcher { name: d.node.name.clone(), node_id: d.node.node_id.clone(), incarnation: d.node.incarnation.clone() }),
        data_dir: dir.into(),
        store_dir: shared.into(),
        mesh_id: d.mesh_id.clone(),
        issuing: None,
        roots: Vec::new(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
    };
    let _ = root.runner.joins.expect(rdm_admin::join::Deployed { listeners: Vec::new(),
        name: launch.name.clone(),
        node_id: launch.node_id.clone(),
        incarnation: launch.incarnation.clone(),
        supersedes: None,
        endpoint_id: rdm_core::EndpointId(key.public().to_string()),
        runtime: fact,
        data_dir: dir.display().to_string(),
        last_accepted_name: None,
        held: false,
    });
    let node_id = launch.node_id.clone();
    let running = rdm_node::node::start_with_rafka_time_before_ready_and(
        &launch,
        rdm_transport::clock::RafkaTime::unadopted(),
        BeforeReadyHook::new(Arc::new(Ready)),
        |s| s,
        None,
        |b, _| b.serve::<BeforeReadyPull, _, _>(OpOwner::Testkit, |_, _| async { Ok(PullReply::Rows { rows: vec![Row { version: 1, value: "work".into() }] }) }),
        &[],
    )
    .await
    .unwrap();
    (running, node_id)
}

/// CONTRACT (node-rpc.md: a call carries its caller's trace context and the receiver's span is its
/// child; the consumer's own subscriber): under the consumer's own OpenTelemetry layer, the consumer's
/// span, the node-admin's call to the gateway made inside it and the gateway's serve of that call are
/// one trace, the serve span a child of a span on the caller's side. Must NOT happen: the gateway's serve
/// span in another trace, or the consumer's span alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_from_the_node_admin_to_the_gateway_is_one_trace_under_the_consumers_own_subscriber() {
    // The consumer's own subscriber: its OpenTelemetry layer over an in-memory exporter.
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    let subscriber = tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(provider.tracer("rshape-consumer")));
    tracing::subscriber::set_global_default(subscriber).expect("the consumer's subscriber is the process's");

    let dir = std::env::temp_dir().join(format!("demo-trace-{}", NodeId::mint()));
    let root_dir = dir.join("admin");
    let root = start_with(config(&root_dir), Wiring { fabric_hooks: FabricHooks::no_app_work(), ..Wiring::no_certs() }).await.unwrap();
    // The Day-0 change begins its open-traffic checklist; then this admin reconciles nothing more, so the
    // hand-launched gateway is no drift pass's surplus.
    let reached = |name: &str| exporter.get_finished_spans().unwrap().iter().any(|s| s.name == name);
    tokio::time::timeout(BOUND, async {
        while !reached("rdm.node_admin.fabric.update.via-every-mesh-ready") {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the Day-0 change begins its open-traffic checklist");
    root.stop_reconciling();
    let (g, gateway_id) = gateway(&root, &dir.join("gateway.1"), &root_dir).await;

    // The consumer's span, and the node-admin's call to the gateway inside it.
    let client = root.node_rpc.client.clone();
    let call = tracing::info_span!(target: "rshape", "rafka.demo.call.via-node-admin");
    async {
        tokio::time::timeout(BOUND, async {
            loop {
                let (out, _) = client.call::<BeforeReadyPull>(&NodeTarget::ExactNode(gateway_id.clone()), &PullRequest::Pull, &CallOptions::default()).await;
                if matches!(out.reply().map(|r| r.value().clone()), Some(PullReply::Rows { .. })) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .expect("the gateway answers the call");
    }
    .instrument(call)
    .await;
    g.stop().await;
    provider.force_flush().unwrap();

    let spans = exporter.get_finished_spans().unwrap();
    let consumer = spans.iter().find(|s| s.name == "rafka.demo.call.via-node-admin").expect("the consumer's span");
    let trace = consumer.span_context.trace_id();
    let in_trace: Vec<_> = spans.iter().filter(|s| s.span_context.trace_id() == trace).collect();
    let serve = in_trace
        .iter()
        .find(|s| s.name == "rdm.node_rpc.request.serve.via-direct" && s.attributes.iter().any(|kv| kv.key.as_str() == "op" && kv.value.to_string() == "116"))
        .unwrap_or_else(|| panic!("the gateway's serve of op 0x74 is in the consumer's trace: {:?}", in_trace.iter().map(|s| &s.name).collect::<Vec<_>>()));
    assert!(
        in_trace.iter().any(|s| s.span_context.span_id() == serve.parent_span_id),
        "the gateway's serve span is a child of a span on the caller's side, in the one trace: {:?}",
        in_trace.iter().map(|s| (&s.name, s.span_context.span_id(), s.parent_span_id)).collect::<Vec<_>>()
    );
    assert!(in_trace.iter().any(|s| s.name == "rdm.node_rpc.request.update.via-call"), "the node-admin's call is in the trace");
    let _ = std::fs::remove_dir_all(&dir);
}
