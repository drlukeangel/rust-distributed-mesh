//! i143 R-B2 (Luke), D: a `JoinNode` that arrives after the deploying admin ended the birth's
//! deployment is refused by name (`DeploymentAbandoned`), never as `NotAuthority`.

use rafka_mesh_entity::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId, RuntimeFact};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::join::{JoinDoor, Joins};
use rafka_node_admin_core::model::{Node, ProviderKind};
use rafka_node_rpc_contract::join::{JoinReply, JoinRequest};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

struct Capture {
    exporter: opentelemetry_sdk::testing::trace::InMemorySpanExporter,
    provider: opentelemetry_sdk::trace::TracerProvider,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use opentelemetry::trace::TracerProvider as _;
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let provider = opentelemetry_sdk::trace::TracerProvider::builder().with_simple_exporter(exporter.clone()).build();
    let layer = tracing_opentelemetry::layer().with_tracer(provider.tracer("i143-rb2"));
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

    fn spans(&self) -> Vec<Value> {
        let _ = self.provider.force_flush();
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({"name": s.name, "attributes": attributes})
            })
            .collect()
    }
}

/// A runtime that stays up and never reports: the create stops waiting for it.
struct Silent;
#[async_trait::async_trait]
impl DeploymentProvider for Silent {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "i143-rb2".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Ok(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("i143-rb2".into()) })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        DeploymentStatus::Running
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        None
    }
}

struct Never;
#[async_trait::async_trait]
impl NodeObserver for Never {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<rafka_node_admin_core::deployment::pipeline::Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Err("never".into())
    }
    async fn drain(&self, _: &Node) -> rafka_node_admin_core::deployment::pipeline::DrainOutcome {
        rafka_node_admin_core::deployment::pipeline::DrainOutcome::NotSent { reason: "no Node RPC".into() }
    }
    async fn drained(&self, _: &Node) -> bool {
        false
    }
    async fn admission_closed(&self, _: &Node) -> Result<(), String> {
        Err("never".into())
    }
}

struct Discard;
impl TopologySink for Discard {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &rafka_node_admin_core::model::PathName) {}
}

fn receipt_output(view: &rafka_node_admin_core::build_state::BuildProjection, step: &str) -> Value {
    view.steps.iter().find(|s| s.step == step).unwrap_or_else(|| panic!("no {step} receipt")).output.clone().unwrap_or_else(|| panic!("{step} recorded no output"))
}

/// CONTRACT (R-B2 D): the deploying admin ends a create whose birth never reported (`WaitForBind`
/// expires). The birth's `JoinNode` that arrives afterwards is answered `DeploymentAbandoned`
/// naming the Build, the attempt, the node id and the incarnation of exactly that birth, and its
/// `via-join` span records outcome `deployment-abandoned` with the Build and attempt. A digest of
/// the same node under another incarnation is not that birth: it is refused as a mismatch by its
/// field, never as abandoned. The reply is never `NotAuthority`.
#[test]
fn join_after_the_deployment_was_abandoned_is_refused_naming_the_build_attempt_and_birth() {
    let cap = capture();
    cap.run(async {
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1"), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        builds.claim_attempt(&BuildAttemptClaim { build_id: build_id.clone(), attempt: 3, executor: "mesh1.admin.1".into() }).await.unwrap();
        let data_root = std::env::temp_dir().join(format!("i143-rb2-{}", NodeId::mint()));
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rb2-none".into(),
            seeds: vec![],
            launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.clone(),
        };
        let joins = Arc::new(Joins::default());
        let pipeline = DeploymentPipeline {
            provider: &Silent,
            joins: &joins,
            observer: &Never,
            sink: &Discard,
            lifecycle: &NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts { bind: Duration::from_millis(300), ..Timeouts::default() },
        };
        let name: rafka_node_admin_core::model::PathName = "mesh1.rpc.1".parse().unwrap();
        let err = pipeline.create(&CreateRequest { build_id: build_id.clone(), attempt: 3, node: name.clone(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() }).await.unwrap_err();
        assert_eq!(err.step, "WaitForBind", "{err}");

        // The exact birth the create deployed, from its own receipts and data dir.
        let view = builds.read_build(&build_id).await.unwrap();
        let identity = receipt_output(&view, "AllocateIdentity");
        let node_id = NodeId::parse(identity["node_id"].as_str().unwrap()).unwrap();
        let incarnation = IncarnationId(identity["incarnation"].as_str().unwrap().to_string());
        let endpoint_id = EndpointId(receipt_output(&view, "PrepareStorage").as_str().unwrap().to_string());
        let data_dir = data_root.join(format!("{name}-{node_id}"));
        let runtime: RuntimeFact = RuntimeFact::read_record(&data_dir).expect("the create wrote the runtime record").unwrap();
        let digest = |incarnation: &IncarnationId| MeshDigest {
            fabric_id: FabricId::mint(),
            node: MeshNode {
                node_id: node_id.clone(),
                name: name.clone(),
                endpoint_id: endpoint_id.clone(),
                transport_addr: "127.0.0.1:34567".parse().unwrap(),
                incarnation: incarnation.clone(),
                supersedes: None,
                runtime: Some(runtime.clone()),
            },
            status: MemberStatus::Pending,
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(data_dir.display().to_string()),
            mesh_id: None,
            in_flight: None,
            extra: Default::default(),
            load: None,
            gossip: None,
        };
        let installed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = installed.clone();
        let door = JoinDoor {
            me: "mesh1.admin.1".parse().unwrap(),
            joins: joins.clone(),
            answer: Arc::new(|| Box::pin(async { Err("a refused join is never answered".to_string()) })),
            install: Arc::new(move |_| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }),
            known: Arc::new(|| Box::pin(async {})),
            primary: Arc::new(|| Some("mesh1.admin.1".into())),
        };
        let ask = |d: &MeshDigest| JoinRequest::JoinNode { digest: d.into() };

        let late = door.serve(endpoint_id.clone(), ask(&digest(&incarnation))).await;
        assert_eq!(
            late,
            JoinReply::DeploymentAbandoned { build_id: build_id.to_string(), attempt: 3, node_id: node_id.to_string(), incarnation: incarnation.0.clone() },
            "the late join names the Build, the attempt and the exact birth"
        );
        let other = IncarnationId::mint();
        let fenced = door.serve(endpoint_id.clone(), ask(&digest(&other))).await;
        assert!(matches!(&fenced, JoinReply::JoinMismatch { field, .. } if field == "incarnation"), "another incarnation is not the abandoned birth: {fenced:?}");
        assert_eq!(installed.load(std::sync::atomic::Ordering::SeqCst), 0, "a refused join installs nothing");
        let _ = std::fs::remove_dir_all(&data_root);
    });
    let spans = cap.spans();
    let via_join: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.update.via-join").collect();
    let abandoned: Vec<&&Value> = via_join.iter().filter(|s| s["attributes"]["outcome"] == "deployment-abandoned").collect();
    assert_eq!(abandoned.len(), 1, "{via_join:?}");
    assert_eq!(abandoned[0]["attributes"]["attempt"], "3");
    assert!(abandoned[0]["attributes"]["build_id"].as_str().unwrap().starts_with("bld-"));
    assert!(via_join.iter().all(|s| s["attributes"]["outcome"] != "not-authority"), "the authority never answers not-authority about itself");
}
