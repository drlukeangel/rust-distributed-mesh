//! The `node` objects that travel over node-RPC: each builds today's request, sends it to the
//! exact birth and returns the typed reply. A real server records the request it decoded, so the
//! assertion is on what crossed the wire.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_node_admin_client::{BuildId, CallEnd, DrainContext, ExactBirth, NodeRpc};
use rafka_node_rpc::{Budget, CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, ServedBirth, ServerBuilder, StaticResolver};
use rafka_node_rpc_contract::catalog::OpOwner;
use rafka_node_rpc_contract::status::{NodeState, Status, StatusReply, StatusRequest};
use rafka_node_rpc_contract::topology::{Topology, TopologyReply, TopologyRequest};
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;

struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    crate::enable_callsites();
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("node-rpc-objects"));
    Capture { exporter, provider, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
}

impl Capture {
    fn run<F: std::future::Future>(&self, f: F) -> F::Output {
        let d = self.dispatch.clone();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
            .build()
            .unwrap();
        let _g = tracing::dispatcher::set_default(&self.dispatch);
        rt.block_on(f)
    }

    fn spans(&self, name: &str) -> Vec<Value> {
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({"name": s.name, "attributes": attributes})
            })
            .collect()
    }
}

/// A birth serving Status and Topology on its own endpoint, recording every request it decodes.
struct Served {
    birth: ExactBirth,
    seen: Arc<Mutex<Vec<StatusRequest>>>,
    topology_seen: Arc<Mutex<Vec<TopologyRequest>>>,
    client: NodeRpcClient,
    _router: Router,
}

/// `reply` is what the birth answers every Status call; `delay` holds the answer back.
async fn serve(reply: StatusReply, delay: Duration) -> Served {
    let node_id = NodeId::mint();
    let incarnation = IncarnationId::mint();
    let key = SecretKey::generate();
    let seen: Arc<Mutex<Vec<StatusRequest>>> = Arc::default();
    let topology_seen: Arc<Mutex<Vec<TopologyRequest>>> = Arc::default();
    let (s, t) = (seen.clone(), topology_seen.clone());
    let server = ServerBuilder::new()
        .serve::<Status, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: StatusRequest| {
            let (s, reply) = (s.clone(), reply.clone());
            async move {
                s.lock().unwrap().push(req);
                tokio::time::sleep(delay).await;
                Ok(reply)
            }
        })
        .serve_stream::<Topology, _, _>(OpOwner::Product("rdm".into()), move |_peer, req: TopologyRequest, sink| {
            let t = t.clone();
            async move {
                t.lock().unwrap().push(req);
                let mut sink = sink.started(TopologyReply::Started).await.map_err(|_| rafka_node_rpc::HandlerFault::invariant_broken("the sink refused Started"))?;
                sink.data(TopologyReply::Unchanged { mesh: "mesh1".into(), publisher: rafka_mesh_entity::PublisherId { node: "mesh1.admin.1".into(), incarnation: IncarnationId::mint() }, topology_version: 3 }).await.map_err(|_| rafka_node_rpc::HandlerFault::invariant_broken("the sink refused Data"))?;
                Ok(TopologyReply::End { meshes: 1 })
            }
        })
        .seal(ServedBirth { node_id: node_id.to_string(), incarnation: incarnation.0.clone() })
        .unwrap();
    let ep = rafka_node_rpc::endpoint::bind(key.clone(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let addr = ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let router = Router::builder(ep).accept(rafka_node_rpc::ALPN, server).spawn();
    let resolver = Arc::new(StaticResolver::new());
    resolver.insert(ResolvedNode { node_id: node_id.clone(), name: "mesh1.rpc.1".parse().unwrap(), endpoint_id: key.public(), transport_addr: addr, incarnation: incarnation.clone() });
    let caller = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let client = NodeRpcClient::new(caller, resolver).with_caller_system("rdm");
    Served { birth: ExactBirth { target: NodeTarget::ExactNode(node_id.clone()), node_id, incarnation }, seen, topology_seen, client, _router: router }
}

/// CONTRACT: `node.drain` and `node.get` each put today's Status
/// request on the wire for the exact birth, the birth's typed reply comes back, and each call
/// leaves one `rdm.node_admin.node.<verb>.via-call` span carrying the reply's name. The drain
/// request names the Build, the attempt and the operation key the context derives.
#[test]
fn each_status_object_sends_todays_request_and_returns_the_typed_reply() {
    let cap = capture();
    let (seen, birth, replies) = cap.run(async {
        let served = serve(StatusReply::Applied, Duration::ZERO).await;
        let rpc = NodeRpc::new(&served.client);
        let opts = CallOptions::default();
        let ctx = DrainContext::new(BuildId("bld-7".into()), 3, "mesh1.rpc.1".parse().unwrap());
        let replies = vec![
            rpc.drain(&served.birth, &ctx, &opts).await.unwrap(),
            rpc.get(&served.birth, &opts).await.unwrap(),
        ];
        let seen = served.seen.lock().unwrap().clone();
        (seen, served.birth.clone(), replies)
    });
    let (node_id, incarnation) = (birth.node_id, birth.incarnation);
    assert_eq!(
        seen,
        vec![
            StatusRequest::DrainNode { node_id: node_id.clone(), incarnation: incarnation.clone(), build_id: "bld-7".into(), attempt: 3, operation: "drain-node:mesh1.rpc.1".into() },
            StatusRequest::ProbeNodeState { node_id, incarnation },
        ]
    );
    assert!(replies.iter().all(|r| *r == StatusReply::Applied));
    for verb in ["drain", "get"] {
        let spans = cap.spans(&format!("rdm.node_admin.node.{verb}.via-call"));
        assert_eq!(spans.len(), 1, "node.{verb} left one span");
        assert_eq!(spans[0]["attributes"]["outcome"], "applied");
    }
}

/// CONTRACT: a refusal the birth answers is a typed reply, not a call failure; a call whose target
/// resolves to no node ends `NotSent` (it never reached a handler); a call whose answer does not
/// arrive within its reply budget ends `Indeterminate` (it may have run). The span says which.
#[test]
fn a_status_object_distinguishes_a_refusal_from_not_sent_and_indeterminate() {
    let cap = capture();
    cap.run(async {
        let served = serve(StatusReply::RejectedInvalidNodeTransition { current: NodeState::Leaving }, Duration::ZERO).await;
        let rpc = NodeRpc::new(&served.client);
        let refused = rpc.get(&served.birth, &CallOptions::default()).await.unwrap();
        assert_eq!(refused, StatusReply::RejectedInvalidNodeTransition { current: NodeState::Leaving });

        let nowhere = ExactBirth { target: NodeTarget::ExactNode(NodeId::mint()), node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let not_sent = rpc.get(&nowhere, &CallOptions::default()).await.unwrap_err();
        assert!(matches!(not_sent, CallEnd::NotSent { .. }), "{not_sent:?}");

        let slow = serve(StatusReply::Applied, Duration::from_secs(3)).await;
        let opts = CallOptions { budget: Budget::Split { send: Duration::from_secs(5), reply: Duration::from_millis(200) }, ..CallOptions::default() };
        let indeterminate = NodeRpc::new(&slow.client).get(&slow.birth, &opts).await.unwrap_err();
        assert!(matches!(indeterminate, CallEnd::Indeterminate { .. }), "{indeterminate:?}");
    });
    let spans = cap.spans("rdm.node_admin.node.apply.via-call");
    assert_eq!(spans[0]["attributes"]["outcome"], "rejected-invalid-node-transition");
    let get: Vec<String> = cap.spans("rdm.node_admin.node.get.via-call").iter().map(|s| s["attributes"]["outcome"].as_str().unwrap().to_string()).collect();
    assert!(get.contains(&"not-sent".to_string()) && get.contains(&"indeterminate".to_string()), "{get:?}");
}

/// CONTRACT: `node.topology.get` sends `GetTopology` for the mesh asked and returns every frame of
/// the read in order, ending at the terminal `End`.
#[test]
fn the_topology_object_returns_every_frame_of_the_read() {
    let cap = capture();
    let (frames, asked) = cap.run(async {
        let served = serve(StatusReply::Applied, Duration::ZERO).await;
        let frames = NodeRpc::new(&served.client).topology_get(&served.birth.target, Some("mesh1".into()), None, &CallOptions::default()).await.unwrap();
        let asked = served.topology_seen.lock().unwrap().clone();
        (frames, asked)
    });
    assert_eq!(asked, vec![TopologyRequest::GetTopology { mesh: Some("mesh1".into()), since: None }]);
    assert_eq!(frames.len(), 3, "{frames:?}");
    assert!(matches!(frames[0], TopologyReply::Started) && matches!(frames[2], TopologyReply::End { meshes: 1 }));
    assert_eq!(cap.spans("rdm.node_admin.node.topology.get.via-call")[0]["attributes"]["outcome"], "read");
}
