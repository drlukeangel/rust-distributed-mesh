//! i143 R-B2 (Luke), D: a `JoinNode` that arrives after the deploying admin ended the birth's
//! deployment is refused by name (`DeploymentAbandoned`), never as `NotPrimary`.

use rdm_core::{EndpointId, FabricId, IncarnationId, MemberStatus, MeshDigest, MeshNode, NodeId, RuntimeFact};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::deployment::endpoint::NODE_SPEC;
use rdm_admin::deployment::pipeline::{CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::join::{JoinService, Joins};
use rdm_admin::model::{Node, ProviderKind};
use rdm_core::join::{JoinReply, JoinRequest};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

fn capture() -> Capture {
    use tracing_subscriber::layer::SubscriberExt;
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("i143-rb2", exporter.clone());
    Capture { exporter, capture, dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)) }
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
        self.capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        let finished = self.exporter.get_finished_spans().unwrap();
        crate::assert_own_story(finished.iter().map(|s| s.name.as_ref()));
        finished
            .into_iter()
            .map(|s| {
                let attributes: serde_json::Map<String, Value> = s.attributes.iter().map(|kv| (kv.key.to_string(), Value::String(kv.value.to_string()))).collect();
                json!({"name": s.name, "attributes": attributes})
            })
            .collect()
    }
}

/// A runtime that stays up and never reports: the create stops waiting for it. It keeps the
/// incarnation each launch carried.
#[derive(Default)]
struct Silent(std::sync::Mutex<Vec<String>>);
#[async_trait::async_trait]
impl DeploymentProvider for Silent {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "i143-rb2".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        self.0.lock().unwrap().extend(spec.env.get(rdm_core::launch::ENV_INCARNATION).cloned());
        Ok(DeploymentHandle { deployment_id: spec.deployment_id.clone(), provider: ProviderKind::Process, pid: Some(1), start: Some(1), container: None, domain: Some("i143-rb2".into()) })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        DeploymentStatus::Running
    }
    async fn signal_stop(&self, _: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

struct Never;
#[async_trait::async_trait]
impl NodeObserver for Never {
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<rdm_admin::deployment::pipeline::Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Err("never".into())
    }
}

struct Discard;
impl TopologySink for Discard {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &rdm_admin::model::PathName) {}
    fn remove_birth(&self, _: &rdm_admin::model::PathName, _: &rdm_core::NodeId, _: &rdm_core::IncarnationId) {}
}

/// CONTRACT (R-B2 D): the deploying admin ends a create whose birth never reported (`WaitForBind`
/// expires). The birth's `JoinNode` that arrives afterwards is answered `DeploymentAbandoned`
/// naming the Build, the node id and the incarnation of exactly that birth, and its
/// `via-join` span records outcome `deployment-abandoned` with the Build. A digest of
/// the same node under another incarnation is not that birth: it is refused as a mismatch by its
/// field, never as abandoned. The reply is never `NotPrimary`.
#[test]
fn join_after_the_deployment_was_abandoned_is_refused_naming_the_build_and_birth() {
    let cap = capture();
    cap.run(async {
        let executor = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let builds = rdm_admin::build_run::FramedBuilds::stepping(Arc::new(MemoryBuildStateAdapter::new()), &FabricId::mint(), &executor.node_id, "mesh1");
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() }, source: None, time: 0 })
            .await
            .unwrap();
        let data_root = std::env::temp_dir().join(format!("i143-rb2-{}", NodeId::mint()));
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: FabricId::mint(),
            executable: "/nonexistent/rb2-none".into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { mesh: "mesh1".into(), path: "mesh1.node-admin".into(), name: "1".into(), node_id: executor.node_id.clone(), incarnation: executor.incarnation.clone() },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let joins = Arc::new(Joins::default());
        let silent = Silent::default();
        let pipeline = DeploymentPipeline {
            provider: &silent,
            joins: &joins,
            observer: &Never,
            sink: &Discard,
            lifecycle: &NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts { bind: Duration::from_millis(300), ..Timeouts::default() },
        };
        let name: rdm_admin::model::PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let err = pipeline.create(&CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&(name))), node_id: (rdm_core::fixture_node_id(&(name))).clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build_id.clone()), path: name.path.clone(), name: name.name.clone(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None }).await.unwrap_err();
        assert_eq!(err.step, "WaitForBind", "{err}");

        // The exact birth the create deployed: the Build record's node id (D58), the incarnation its
        // launch carried, and the transport key its data dir holds.
        let node_id = rdm_core::fixture_node_id(&name);
        let incarnation = IncarnationId::parse(silent.0.lock().unwrap().last().expect("the create launched its birth")).unwrap();
        let data_dir = rdm_admin::deployment::node_data_dir(&data_root, &node_id);
        let endpoint_id = rdm_admin::deployment::pipeline::endpoint_id(&data_dir).expect("the create wrote its node key");
        let runtime: RuntimeFact = RuntimeFact::read_record(&data_dir).expect("the create wrote the runtime record").unwrap();
        let digest = |incarnation: &IncarnationId| MeshDigest { held_hash: Default::default(),
            fabric_id: FabricId::mint(),
            node: MeshNode { mesh: "mesh1".to_owned(), r#type: "node".into(),
                node_id: node_id.clone(),
                path: name.path.clone(), name: name.name.clone(),
                endpoint_id: endpoint_id.clone(),
                transport_addr: "127.0.0.1:34567".parse().unwrap(),
                incarnation: incarnation.clone(),
                supersedes: None,
                runtime: Some(runtime.clone()),
            },
            status: MemberStatus::Pending,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(data_dir.display().to_string()),
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None, is_primary: None, is_fabric_primary: None
        };
        let installed = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count = installed.clone();
        let service = JoinService {
            nodes: None,
            me_path: "mesh1.node-admin".into(), me_name: "1".into(),
            joins: joins.clone(),
            answer: Arc::new(|| Box::pin(async { Err("a refused join is never answered".to_string()) })),
            types: Arc::new(|_, _| Box::pin(async { Ok(()) })),
            install: Arc::new(move |_| {
                count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }),
            known: Arc::new(|| Box::pin(async {})),
            pending: Arc::new(|_, _| Box::pin(async {})),
            primary: { let id = executor.node_id.clone(); Arc::new(move || Some(id.clone())) },
            atlas: None,
            issuer: Arc::new(rdm_admin::certs::CertIssuer::no_certs({
                let t = rdm_transport::clock::RafkaTime::unadopted();
                t.adopt(1_000);
                t
            })),
        };
        let ask = |d: &MeshDigest| JoinRequest::JoinNode { types: Vec::new(), digest: d.into(), listeners: Vec::new() };

        let late = service.serve(endpoint_id.clone(), ask(&digest(&incarnation))).await;
        assert_eq!(
            late,
            JoinReply::DeploymentAbandoned { build_id: build_id.to_string(), node_id: node_id.clone(), incarnation: incarnation.clone() },
            "the late join names the Build and the exact birth"
        );
        let other = IncarnationId::mint();
        let targeted = service.serve(endpoint_id.clone(), ask(&digest(&other))).await;
        assert!(matches!(&targeted, JoinReply::JoinMismatch { field, .. } if field == "incarnation"), "another incarnation is not the abandoned birth: {targeted:?}");
        assert_eq!(installed.load(std::sync::atomic::Ordering::SeqCst), 0, "a refused join installs nothing");
        let _ = std::fs::remove_dir_all(&data_root);
    });
    let spans = cap.spans();
    let via_join: Vec<&Value> = spans.iter().filter(|s| s["name"] == "rdm.node_admin.node.update.via-join").collect();
    let abandoned: Vec<&&Value> = via_join.iter().filter(|s| s["attributes"]["outcome"] == "deployment-abandoned").collect();
    assert_eq!(abandoned.len(), 1, "{via_join:?}");
    assert!(rdm_core::BuildId::parse(abandoned[0]["attributes"]["build_id"].as_str().unwrap()).is_ok());
    assert!(via_join.iter().all(|s| s["attributes"]["outcome"] != "not-authority"), "the authority never answers not-authority about itself");
}
