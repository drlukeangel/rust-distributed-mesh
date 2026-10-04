//! i143.e2.s3 functional: a process node is deployed through the create
//! `DeploymentPipeline`, every step leaves a receipt, and every step runs as a
//! child span of the pipeline span carrying `step`, `build_id`, `provider`,
//! `outcome`, `attempt` and `elapsed_ms`.
//!
//! The admin side is in-process (a gossip seed plus a Node RPC client); the
//! node is a real `rafka-rpc-node` process started by the process provider.

use iroh::protocol::Router;
use iroh::SecretKey;
use rafka_mesh_entity::{IncarnationId, NodeId};
use rafka_mesh_transport::membership::Membership;
use rafka_node_admin_core::build::{BuildId, BuildIntent};
use rafka_node_admin_core::build_state::{BuildIntentFact, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::{EndpointAllocator, RPC_NODE_SLOTS};
use rafka_node_admin_core::deployment::pipeline::{
    CreatePipeline, CreateRequest, CreateStep, LaunchTemplate, NodeObserver, Timeouts, TopologySink,
};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeploymentProvider, DeploymentStatus, TerminationMode};
use rafka_node_admin_core::model::{Node, NodeStatus};
use rafka_node_rpc::{CallOptions, NodeRpcClient, NodeTarget, ResolvedNode, StaticResolver};
use rafka_node_rpc_contract::echo::{Echo, EchoReply, EchoRequest};
use rafka_node_rpc_contract::outcome::RpcOutcome;
use std::collections::{BTreeMap, HashMap};
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

/// Every span: name, parent name, and its recorded fields.
#[derive(Clone, Default)]
struct Spans(Arc<Mutex<HashMap<u64, (String, Option<String>, BTreeMap<String, String>)>>>);

struct Fields<'a>(&'a mut BTreeMap<String, String>);
impl tracing::field::Visit for Fields<'_> {
    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
        self.0.insert(f.name().into(), v.into());
    }
    fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
        self.0.insert(f.name().into(), v.to_string());
    }
    fn record_i64(&mut self, f: &tracing::field::Field, v: i64) {
        self.0.insert(f.name().into(), v.to_string());
    }
    fn record_bool(&mut self, f: &tracing::field::Field, v: bool) {
        self.0.insert(f.name().into(), v.to_string());
    }
    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        self.0.insert(f.name().into(), format!("{v:?}"));
    }
}

impl<S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>> tracing_subscriber::Layer<S> for Spans {
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        let parent = ctx.span(id).and_then(|s| s.parent().map(|p| p.name().to_string()));
        self.0.lock().unwrap().insert(id.into_u64(), (attrs.metadata().name().into(), parent, fields));
    }
    fn on_record(&self, id: &tracing::span::Id, values: &tracing::span::Record<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        if let Some((_, _, fields)) = self.0.lock().unwrap().get_mut(&id.into_u64()) {
            values.record(&mut Fields(fields));
        }
    }
}

#[derive(Default)]
struct Published(Mutex<Vec<Node>>);
impl TopologySink for Published {
    fn publish(&self, node: Node) {
        self.0.lock().unwrap().push(node);
    }
}

/// Joined = the node's own digest for exactly this birth reached the admin's
/// membership; ready = an Echo answered on every advertised slot.
struct LiveMesh {
    membership: Membership,
    client: NodeRpcClient,
    resolver: Arc<StaticResolver>,
}

#[async_trait::async_trait]
impl NodeObserver for LiveMesh {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> bool {
        self.membership.book.get(&node_id.0).is_some_and(|(d, _)| &d.node.incarnation == incarnation)
    }

    async fn ready(&self, node: &Node) -> Result<(), String> {
        let fabric_id = node.fabric_id.as_ref().ok_or("no fabric id")?.0.parse::<iroh::PublicKey>().map_err(|e| e.to_string())?;
        self.resolver.insert(ResolvedNode {
            node_id: node.node_id.clone(),
            name: node.name.clone(),
            fabric_id,
            incarnation: node.incarnation_id.clone().ok_or("no incarnation")?,
            endpoints: node.endpoints.clone(),
        });
        for slot in &node.endpoints {
            let opts = CallOptions { slot: Some(slot.slot.clone()), ..CallOptions::default() };
            let req = EchoRequest::Echo { traceparent: None, payload: b"ready?".to_vec() };
            match self.client.call::<Echo>(&NodeTarget::ExactNode(node.node_id.clone()), &req, &opts).await.0 {
                RpcOutcome::Reply(r) if matches!(r.value(), EchoReply::Echoed { .. }) => {}
                other => return Err(format!("slot {} answered {}", slot.slot, other.name())),
            }
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_deploys_a_process_node_through_every_pipeline_step() {
    let spans = Spans::default();
    let _sub = tracing::subscriber::set_default(tracing_subscriber::registry().with(spans.clone()));

    // The admin side: one endpoint serving gossip, used as the node's seed.
    let fabric = format!("fab-{}", NodeId::mint());
    let admin_ep = rafka_node_rpc::endpoint::bind(SecretKey::generate(), "127.0.0.1:0".parse().unwrap()).await.unwrap();
    let gossip = iroh_gossip::net::Gossip::builder().spawn(admin_ep.clone());
    let _router = Router::builder(admin_ep.clone()).accept(iroh_gossip::ALPN, gossip.clone()).spawn();
    let membership = Membership::join(&gossip, &admin_ep, &fabric, vec![]).await.unwrap();
    let admin_addr: SocketAddr = admin_ep.bound_sockets().into_iter().find(|a| a.is_ipv4()).unwrap();
    let resolver = Arc::new(StaticResolver::new());
    let observer = LiveMesh { membership, client: NodeRpcClient::new(admin_ep.clone(), resolver.clone()), resolver };

    let data_root = std::env::temp_dir().join(format!("i143-e2s3-{}", NodeId::mint()));
    let template = LaunchTemplate {
        fabric: fabric.clone(),
        executable: env!("CARGO_BIN_EXE_rafka-rpc-node").into(),
        seeds: vec![(admin_ep.id().to_string(), admin_addr)],
        env: BTreeMap::new(),
        data_root: data_root.clone(),
    };
    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    let node_name = "mesh1.rpc.1".parse().unwrap();
    builds
        .publish_intent(&BuildIntentFact {
            build_id: build_id.clone(),
            intent: BuildIntent::AddNode { mesh: "mesh1".into(), node_kind: rafka_node_admin_core::model::NodeKind::RpcNode },
            traceparent: None,
            submitted_at_ms: 0,
        })
        .await
        .unwrap();
    let allocator = Mutex::new(EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), 58000, 58199));
    let provider = ProcessDeploymentProvider::new();
    let sink = Published::default();
    let pipeline = CreatePipeline {
        provider: &provider,
        allocator: &allocator,
        observer: &observer,
        sink: &sink,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };

    let created = pipeline
        .run(&CreateRequest { build_id: build_id.clone(), attempt: 1, node: node_name, slots: RPC_NODE_SLOTS, restart_of: None })
        .await;
    let created = match created {
        Ok(c) => c,
        Err(e) => panic!("pipeline failed: {e}"),
    };

    // The node is live, published Pending then ReadyForTraffic, at the assigned endpoints.
    assert_eq!(provider.inspect(&created.handle).await, DeploymentStatus::Running);
    let published = sink.0.lock().unwrap().clone();
    assert_eq!(published.iter().map(|n| n.status).collect::<Vec<_>>(), vec![NodeStatus::Pending, NodeStatus::ReadyForTraffic]);
    assert_eq!(created.node.endpoints, allocator.lock().unwrap().held(&created.node.name).unwrap().to_vec());

    // Every step has exactly one Complete receipt, in pipeline order.
    let view = builds.read_build(&build_id).await.unwrap();
    let receipts: Vec<_> = view.steps.iter().map(|r| (r.step.as_str(), &r.outcome, r.operation.as_str(), r.attempt)).collect();
    let want: Vec<_> = CreateStep::ORDER.iter().map(|s| (s.name(), &StepOutcome::Complete, "create-node:mesh1.rpc.1", 1)).collect();
    assert_eq!(receipts, want);

    // One child span per step under the pipeline span, each fully attributed.
    let all = spans.0.lock().unwrap().clone();
    assert!(all.values().any(|(n, _, f)| n == "rafka.node_admin.deployment.update.via-pipeline"
        && f.get("build_id") == Some(&build_id.0)));
    for step in CreateStep::ORDER {
        let (_, parent, f) = all
            .values()
            .find(|(n, _, f)| n == "rafka.node_admin.deployment.update.via-step" && f.get("step").map(String::as_str) == Some(step.name()))
            .unwrap_or_else(|| panic!("no span for step {}", step.name()));
        assert_eq!(parent.as_deref(), Some("rafka.node_admin.deployment.update.via-pipeline"), "{}", step.name());
        assert_eq!(f.get("build_id"), Some(&build_id.0), "{}", step.name());
        assert_eq!(f.get("provider").map(String::as_str), Some("Process"), "{}", step.name());
        assert_eq!(f.get("outcome").map(String::as_str), Some("complete"), "{}", step.name());
        assert_eq!(f.get("attempt").map(String::as_str), Some("1"), "{}", step.name());
        assert!(f.get("elapsed_ms").is_some_and(|v| v.parse::<u64>().is_ok()), "{}: elapsed_ms {f:?}", step.name());
    }

    provider.terminate(&created.handle, TerminationMode::Graceful { grace: Duration::from_secs(5) }).await.unwrap();
    assert!(matches!(provider.inspect(&created.handle).await, DeploymentStatus::Exited { .. }));
    let _ = std::fs::remove_dir_all(&data_root);
}
