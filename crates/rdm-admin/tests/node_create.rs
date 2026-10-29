//! Functional cell for `node.create` (`docs/architecture/lifecycles/node-create.md`).

use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome,
};
use rdm_admin::deployment::endpoint::RPC_NODE;
use rdm_admin::deployment::pipeline::{
    CommandAdmission, CommandContext, Completion, CreateRequest, CreateStep, DeploymentPipeline,
    LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink,
};
use rdm_admin::deployment::provider::{
    DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch,
    TerminationMode,
};
use rdm_admin::join::{JoinReport, Joins};
use rdm_admin::model::{
    EndpointId, Fabric, IncarnationId, Mesh, Node, NodeId, NodeStatus, PathName, ProviderKind,
    ScopeStatus,
};
use rdm_admin::node_commands::NodeCommand;
use rdm_admin::node_rows::WriteRecord as _;
use rdm_admin::topology::Topology;
use rdm_core::{MemberStatus, MeshDigest, MeshNode, RuntimeFact, RuntimeLocator};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;
use tracing_subscriber::layer::SubscriberExt as _;

struct Captured {
    attributes: BTreeMap<String, String>,
}

struct Capture {
    exporter: opentelemetry_sdk::trace::InMemorySpanExporter,
    capture: rdm_transport::SpanCapture,
    dispatch: tracing::Dispatch,
}

impl Capture {
    fn new(cell: &str) -> Self {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let (capture, layer) = rdm_transport::span_capture(cell, exporter.clone());
        Self {
            exporter,
            capture,
            dispatch: tracing::Dispatch::new(tracing_subscriber::registry().with(layer)),
        }
    }

    fn named(&self, name: &str) -> Vec<Captured> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .filter(|s| s.name == name)
            .map(|s| Captured {
                attributes: s
                    .attributes
                    .iter()
                    .map(|kv| (kv.key.to_string(), kv.value.to_string()))
                    .collect(),
            })
            .collect()
    }

    fn all_names(&self) -> Vec<String> {
        self.capture
            .flush(Duration::from_secs(10))
            .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
        self.exporter
            .get_finished_spans()
            .unwrap()
            .into_iter()
            .map(|s| s.name.to_string())
            .collect()
    }
}

struct ImmediateReportProvider {
    joins: Arc<Joins>,
    pending_report: Mutex<Option<JoinReport>>,
    spawn_calls: Mutex<usize>,
}

#[async_trait::async_trait]
impl DeploymentProvider for ImmediateReportProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }

    fn control_domain(&self) -> String {
        rdm_core::runtime::process_control_domain().expect("process control domain")
    }

    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        *self.spawn_calls.lock().unwrap() += 1;
        let refused = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason };
        let fact = RuntimeFact::of_this_process(&spec.deployment_id.to_string()).map_err(refused)?;
        let (pid, start) = match fact.locator {
            RuntimeLocator::Process { pid, start } => (pid, start),
            other => return Err(refused(format!("unexpected locator: {other:?}"))),
        };
        let handle = DeploymentHandle {
            deployment_id: spec.deployment_id.clone(),
            provider: ProviderKind::Process,
            pid: Some(pid),
            start: Some(start),
            container: None,
            domain: Some(fact.control_domain.clone()),
        };
        let key = rdm_node::node::load_or_mint_key(&spec.data_dir)
            .map_err(|e| refused(e.to_string()))?;
        let env = |k: &str| {
            spec.env
                .get(k)
                .cloned()
                .ok_or_else(|| refused(format!("launch missing {k}")))
        };
        let digest = MeshDigest {
            held_hash: Default::default(),
            fabric_id: rdm_core::FabricId::mint(),
            node: MeshNode {
                node_id: NodeId::parse(&env(rdm_core::launch::ENV_NODE_ID)?)
                    .map_err(|e| refused(e.to_string()))?,
                name: spec.node.clone(),
                endpoint_id: EndpointId(key.public().to_string()),
                transport_addr: "127.0.0.1:45678".parse().unwrap(),
                incarnation: IncarnationId::parse(&env(rdm_core::launch::ENV_INCARNATION)?)
                    .map_err(|e| refused(e.to_string()))?,
                supersedes: None,
                runtime: handle.fact(),
            },
            status: MemberStatus::Pending,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(spec.data_dir.display().to_string()),
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        };
        *self.pending_report.lock().unwrap() = Some(JoinReport {
            digest,
            listeners: Vec::new(),
        });
        Ok(handle)
    }

    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        Ok(())
    }

    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        let maybe_report = self.pending_report.lock().unwrap().take();
        if let Some(report) = maybe_report {
            self.joins.report(&report);
        }
        DeploymentStatus::Running
    }

    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }

    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
        Ok(Found::NeverLaunched)
    }
}

struct ReadyBirthObserver;

#[async_trait::async_trait]
impl NodeObserver for ReadyBirthObserver {
    async fn send_command(&self, _: &Node, _: NodeCommand, _: &CommandContext) -> CommandAdmission {
        CommandAdmission::Admitted {}
    }

    async fn await_completion(
        &self,
        _: &Node,
        _: NodeCommand,
        _: &CommandContext,
        _: Duration,
    ) -> Completion {
        Completion::Received {}
    }

    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }

    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

struct RecordingSink {
    rows: Arc<rdm_admin::node_rows::NodeRows>,
    published: Mutex<Vec<Node>>,
}

impl TopologySink for RecordingSink {
    fn publish(&self, node: Node) {
        self.published.lock().unwrap().push(node);
    }

    fn remove(&self, _: &PathName) {}

    fn remove_birth(&self, _: &PathName, _: &NodeId, _: &IncarnationId) {}

    fn node_rows(&self) -> Option<Arc<rdm_admin::node_rows::NodeRows>> {
        Some(self.rows.clone())
    }
}

/// Page: `docs/architecture/lifecycles/node-create.md:37`
/// "1. Record the exact deployment, prepare its identity/data dir and launch its executable.
/// The executor never picks, reserves or predicts a transport port. Before this step the
/// node's row writer claims the name, reading the row at it; `NameHeld { by: node.id }` refuses a
/// held name and nothing is launched."
/// Page: `docs/architecture/lifecycles/node-create.md:51`
/// "5. On acceptance it installs the actual address and completes WaitForBind / WaitForMeshJoin
/// from the validated report, never from gossip."
#[test]
fn node_create_pipeline_refuses_held_name_before_launch_and_completes_all_steps_from_validated_report()
{
    let capture = Capture::new(
        "node_create_pipeline_refuses_held_name_before_launch_and_completes_all_steps_from_validated_report",
    );
    let dispatch = capture.dispatch.clone();
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
        .build()
        .unwrap();
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let data_root = std::env::temp_dir().join(format!("node-create-cell-{}", NodeId::mint()));

    rt.block_on(async {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let mut admin = Node::allocated(me.clone());
        admin.status = NodeStatus::Ready;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        admin.incarnation_id = Some(IncarnationId::mint());
        let me_id = admin.node_id.clone();

        let held_path: PathName = "mesh1.rpc.1".parse().unwrap();
        let held_id = NodeId::mint();
        let store = Arc::new(rdm_admin::storage::MemoryNodesStorage::default());
        let mut held_row = rdm_admin::storage::NodeRow::new(
            held_path.clone(),
            held_id.clone(),
            NodeStatus::Ready,
        );
        held_row.birth_mut(&IncarnationId::mint(), &held_path);
        store.write_record(&held_row).await.unwrap();

        let view = Arc::new(RwLock::new(Topology {
            fabric: Fabric {
                id: rdm_core::FabricId::mint(),
                name: "fabric1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: vec![Mesh {
                id: Some(rdm_core::MeshId::mint()),
                name: "mesh1".into(),
                status: ScopeStatus::Ready,
            }],
            nodes: vec![admin],
        }));
        let rows = Arc::new(rdm_admin::node_rows::NodeRows::new(
            me.clone(),
            me_id.clone(),
            view,
            store,
        ));
        let sink = RecordingSink {
            rows,
            published: Mutex::new(Vec::new()),
        };

        let joins = Arc::new(Joins::default());
        let provider = ImmediateReportProvider {
            joins: joins.clone(),
            pending_report: Mutex::new(None),
            spawn_calls: Mutex::new(0),
        };
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 0,
            })
            .await
            .unwrap();
        builds
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_id.clone(),
                attempt: 1,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();

        let binding = crate::test_executable(rdm_core::NodeKind::RpcNode);
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: rdm_core::FabricId::mint(),
            executable: binding.executable.clone(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher {
                name: me,
                node_id: me_id,
                incarnation: IncarnationId::mint(),
            },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: Some(binding),
        };

        let pipeline = DeploymentPipeline {
            provider: &provider,
            joins: &joins,
            observer: &ReadyBirthObserver,
            sink: &sink,
            lifecycle: &NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts::default(),
        };

        // 1. A held name at `mesh1.rpc.1` is refused at PrepareStorage before DeployRuntime is called.
        let held_req = CreateRequest {
            build_id: build_id.clone(),
            attempt: 1,
            node: held_path,
            spec: &RPC_NODE,
            data_dir: None,
            held_runtimes: Vec::new(),
            entry: None,
        };
        let held_err = pipeline
            .create(&held_req)
            .await
            .map(|_| ())
            .expect_err("held name must be refused");
        assert_eq!(held_err.step, CreateStep::PrepareStorage.name());
        assert!(
            held_err.reason.contains(&held_id.to_string()),
            "refusal must name the holding node id {held_id}: {}",
            held_err.reason
        );
        assert_eq!(
            *provider.spawn_calls.lock().unwrap(),
            0,
            "nothing may be launched when the name claim is refused"
        );

        // 2. Creating an available path `mesh1.rpc.2` completes all steps through Complete from the
        //    validated join report while gossip is silent.
        let new_path: PathName = "mesh1.rpc.2".parse().unwrap();
        let create_req = CreateRequest {
            build_id: build_id.clone(),
            attempt: 1,
            node: new_path.clone(),
            spec: &RPC_NODE,
            data_dir: None,
            held_runtimes: Vec::new(),
            entry: None,
        };
        let created = pipeline
            .create(&create_req)
            .await
            .expect("node.create on available path must complete");
        assert_eq!(created.node.name, new_path);
        assert_eq!(
            created.node.transport_addr,
            Some("127.0.0.1:45678".parse().unwrap()),
            "the validated report's bound transport_addr is installed on the node"
        );
        assert_eq!(*provider.spawn_calls.lock().unwrap(), 1);

        let proj = builds.read_build(&build_id).await.unwrap();
        let rpc2_steps: Vec<(&str, StepOutcome)> = proj
            .steps
            .iter()
            .filter(|r| r.operation == "node.create:mesh1.rpc.2")
            .map(|r| (r.step.as_str(), r.outcome.clone()))
            .collect();
        let expected: Vec<(&str, StepOutcome)> = CreateStep::ORDER
            .iter()
            .map(|s| (s.name(), StepOutcome::Complete))
            .collect();
        assert_eq!(
            rpc2_steps, expected,
            "every CreateStep::ORDER receipt for mesh1.rpc.2 must be Complete in order"
        );
    });
    drop(rt);
    let _ = std::fs::remove_dir_all(&data_root);

    let step_spans = capture.named("rdm.node_admin.deployment.update.via-step");
    assert!(
        step_spans.iter().any(|s| {
            s.attributes.get("step").map(String::as_str) == Some("WaitForMeshJoin")
                && s.attributes.get("outcome").map(String::as_str) == Some("complete")
        }),
        "expected rdm.node_admin.deployment.update.via-step for WaitForMeshJoin with outcome=complete"
    );
    let names = capture.all_names();
    crate::assert_own_story(names.iter().map(String::as_str));
}

struct StepFaultProvider {
    joins: Arc<Joins>,
    deliver_join_on_inspect: Mutex<bool>,
    inspect_status: Mutex<DeploymentStatus>,
    find_result: Mutex<Found>,
    pending_report: Mutex<Option<JoinReport>>,
    spawn_calls: Mutex<usize>,
    terminate_calls: Mutex<Vec<rdm_admin::model::DeploymentId>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for StepFaultProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }

    fn control_domain(&self) -> String {
        rdm_core::runtime::process_control_domain().expect("process control domain")
    }

    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        *self.spawn_calls.lock().unwrap() += 1;
        let refused = |reason: String| DeployError::Spawn { node: spec.node.to_string(), reason };
        let fact = RuntimeFact::of_this_process(&spec.deployment_id.to_string()).map_err(refused)?;
        let (pid, start) = match fact.locator {
            RuntimeLocator::Process { pid, start } => (pid, start),
            other => return Err(refused(format!("unexpected locator: {other:?}"))),
        };
        let handle = DeploymentHandle {
            deployment_id: spec.deployment_id.clone(),
            provider: ProviderKind::Process,
            pid: Some(pid),
            start: Some(start),
            container: None,
            domain: Some(fact.control_domain.clone()),
        };
        let key = rdm_node::node::load_or_mint_key(&spec.data_dir)
            .map_err(|e| refused(e.to_string()))?;
        let env = |k: &str| {
            spec.env
                .get(k)
                .cloned()
                .ok_or_else(|| refused(format!("launch missing {k}")))
        };
        let digest = MeshDigest {
            held_hash: Default::default(),
            fabric_id: rdm_core::FabricId::mint(),
            node: MeshNode {
                node_id: NodeId::parse(&env(rdm_core::launch::ENV_NODE_ID)?)
                    .map_err(|e| refused(e.to_string()))?,
                name: spec.node.clone(),
                endpoint_id: EndpointId(key.public().to_string()),
                transport_addr: "127.0.0.1:45679".parse().unwrap(),
                incarnation: IncarnationId::parse(&env(rdm_core::launch::ENV_INCARNATION)?)
                    .map_err(|e| refused(e.to_string()))?,
                supersedes: None,
                runtime: handle.fact(),
            },
            status: MemberStatus::Pending,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 0,
            emitted_at_rafka_ms: 0,
            data_dir: Some(spec.data_dir.display().to_string()),
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        };
        *self.pending_report.lock().unwrap() = Some(JoinReport {
            digest,
            listeners: Vec::new(),
        });
        Ok(handle)
    }

    async fn terminate(
        &self,
        handle: &DeploymentHandle,
        _: TerminationMode,
    ) -> Result<(), DeployError> {
        self.terminate_calls
            .lock()
            .unwrap()
            .push(handle.deployment_id.clone());
        Ok(())
    }

    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        if *self.deliver_join_on_inspect.lock().unwrap() {
            if let Some(report) = self.pending_report.lock().unwrap().take() {
                self.joins.report(&report);
            }
        }
        self.inspect_status.lock().unwrap().clone()
    }

    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }

    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
        Ok(self.find_result.lock().unwrap().clone())
    }
}

struct ConfigurableReadyObserver {
    ready_ok: Mutex<bool>,
    ready_entered: tokio::sync::Notify,
    kill_on_first_ready_check: Mutex<Option<Arc<StepFaultProvider>>>,
}

#[async_trait::async_trait]
impl NodeObserver for ConfigurableReadyObserver {
    async fn send_command(&self, _: &Node, _: NodeCommand, _: &CommandContext) -> CommandAdmission {
        CommandAdmission::Admitted {}
    }

    async fn await_completion(
        &self,
        _: &Node,
        _: NodeCommand,
        _: &CommandContext,
        _: Duration,
    ) -> Completion {
        Completion::Received {}
    }

    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }

    async fn ready(&self, _: &Node) -> Result<(), String> {
        self.ready_entered.notify_waiters();
        if let Some(provider) = self.kill_on_first_ready_check.lock().unwrap().take() {
            *provider.inspect_status.lock().unwrap() = DeploymentStatus::Exited { code: Some(137) };
            return Err("birth killed before ready".into());
        }
        if *self.ready_ok.lock().unwrap() {
            Ok(())
        } else {
            Err("birth not ready yet".into())
        }
    }
}

/// CONTRACT: `docs/architecture/lifecycles/node-create.md:37-41`
/// "1. Record the exact deployment, prepare its identity/data dir and launch its executable.
/// The executor never picks, reserves or predicts a transport port. Before this step the
/// node's row writer claims the name, reading the row at it; `NameHeld { by: node.id }` refuses a
/// held name and nothing is launched."
/// CONTRACT: `docs/architecture/lifecycles/node-create.md:65-69`
/// "Only an accepted full report completes the deployment's bind/join waits. Provider
/// launch, a listening socket, a digest, a timeout or an unanswered call is insufficient.
/// Join acceptance does not complete start or bypass the hook. A typed refusal names
/// its step; an uncertain answer is not Joined. Runtime exit requires independent exact
/// provider proof, even for a deployment that never joined."
/// CONTRACT: `docs/architecture/deployment-provider.md:77`
/// "`os-stopped`: the exact runtime exists and the operating system holds every task of it suspended"
#[test]
fn node_create_step_faults_recover_or_fail_as_the_page_specifies() {
    let capture = Capture::new("node_create_step_faults_recover_or_fail_as_the_page_specifies");
    let dispatch = capture.dispatch.clone();
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
        .build()
        .unwrap();
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let data_root = std::env::temp_dir().join(format!("node-create-chaos-{}", NodeId::mint()));

    rt.block_on(async {
        let me: PathName = "mesh1.admin.1".parse().unwrap();
        let mut admin = Node::allocated(me.clone());
        admin.status = NodeStatus::Ready;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        admin.incarnation_id = Some(IncarnationId::mint());
        let me_id = admin.node_id.clone();

        let store = Arc::new(rdm_admin::storage::MemoryNodesStorage::default());
        let view = Arc::new(RwLock::new(Topology {
            fabric: Fabric {
                id: rdm_core::FabricId::mint(),
                name: "fabric1".into(),
                status: ScopeStatus::Ready,
                provider: ProviderKind::Process,
            },
            meshes: vec![Mesh {
                id: Some(rdm_core::MeshId::mint()),
                name: "mesh1".into(),
                status: ScopeStatus::Ready,
            }],
            nodes: vec![admin],
        }));
        let rows = Arc::new(rdm_admin::node_rows::NodeRows::new(
            me.clone(),
            me_id.clone(),
            view,
            store,
        ));
        let sink = RecordingSink {
            rows: rows.clone(),
            published: Mutex::new(Vec::new()),
        };

        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 0,
            })
            .await
            .unwrap();
        builds
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_id.clone(),
                attempt: 1,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();

        let binding = crate::test_executable(rdm_core::NodeKind::RpcNode);
        let make_template = |seed_port: u16| LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: rdm_core::FabricId::mint(),
            executable: binding.executable.clone(),
            seeds: vec![(
                "adminkey".into(),
                std::net::SocketAddr::from(([127, 0, 0, 1], seed_port)),
            )],
            launcher: rdm_core::launch::Launcher {
                name: me.clone(),
                node_id: me_id.clone(),
                incarnation: IncarnationId::mint(),
            },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: Some(binding.clone()),
        };

        let joins = Arc::new(Joins::default());
        let provider = Arc::new(StepFaultProvider {
            joins: joins.clone(),
            deliver_join_on_inspect: Mutex::new(true),
            inspect_status: Mutex::new(DeploymentStatus::Running),
            find_result: Mutex::new(Found::NeverLaunched),
            pending_report: Mutex::new(None),
            spawn_calls: Mutex::new(0),
            terminate_calls: Mutex::new(Vec::new()),
        });
        let observer = ConfigurableReadyObserver {
            ready_ok: Mutex::new(true),
            ready_entered: tokio::sync::Notify::new(),
            kill_on_first_ready_check: Mutex::new(None),
        };
        let fast_timeouts = Timeouts {
            bind: Duration::from_millis(80),
            join: Duration::from_millis(80),
            ready: Duration::from_millis(120),
            ..Timeouts::default()
        };

        // 1. Cut after claiming the name at PrepareStorage before DeployRuntime:
        //    When the provider proves the claimed allocation was NeverLaunched, the next attempt
        //    continues the exact allocated identity (node_id, incarnation, deployment_id).
        let cut_path: PathName = "mesh1.rpc.1".parse().unwrap();
        let pre_node_id = NodeId::mint();
        let pre_incarnation = IncarnationId::mint();
        let pre_deployment = rdm_admin::model::DeploymentId::mint();
        let allocated = rdm_admin::storage::Allocation {
            build_id: build_id.clone(),
            operation: format!("node.create:{cut_path}"),
            incarnation: pre_incarnation.clone(),
            deployment_id: pre_deployment.clone(),
        };
        rows.claim(
            &cut_path,
            &pre_node_id,
            &format!("node.create:{cut_path}"),
            Some(&allocated),
            None,
            rdm_admin::node_rows::ClaimFor::Create,
        )
        .await
        .expect("cut attempt claims the name with its allocated identity");

        let template_exec1 = make_template(41001);
        let pipeline = DeploymentPipeline {
            provider: &*provider,
            joins: &joins,
            observer: &observer,
            sink: &sink,
            lifecycle: &NoLifecycleEvents,
            builds: &builds,
            template: &template_exec1,
            timeouts: fast_timeouts.clone(),
        };
        let created_cut = pipeline
            .create(&CreateRequest {
                build_id: build_id.clone(),
                attempt: 1,
                node: cut_path.clone(),
                spec: &RPC_NODE,
                data_dir: None,
                held_runtimes: Vec::new(),
                entry: None,
            })
            .await
            .expect("resumed create after cut at PrepareStorage continues the never-launched birth");
        assert_eq!(created_cut.node.node_id, pre_node_id);
        assert_eq!(created_cut.node.incarnation_id, Some(pre_incarnation));
        assert_eq!(created_cut.node.deployment_id, Some(pre_deployment));

        // 2. Lost reply at WaitForBind, then Kill (Exited) at WaitForBind, then Kill at WaitForNodeReady,
        //    then SIGSTOP (OsStopped) across attempts and resume to Complete:
        let fault_path: PathName = "mesh1.rpc.2".parse().unwrap();

        // 2a. Lost reply at WaitForBind: join report never arrives within bind timeout -> DeadlineExceeded,
        //     and the name claim is withdrawn.
        *provider.deliver_join_on_inspect.lock().unwrap() = false;
        let lost_reply_err = pipeline
            .create(&CreateRequest {
                build_id: build_id.clone(),
                attempt: 1,
                node: fault_path.clone(),
                spec: &RPC_NODE,
                data_dir: None,
                held_runtimes: Vec::new(),
                entry: None,
            })
            .await
            .map(|_| ())
            .expect_err("lost join reply must fail WaitForBind on deadline");
        assert_eq!(lost_reply_err.step, CreateStep::WaitForBind.name());
        assert_eq!(lost_reply_err.code, rdm_core::outcome::Code::DeadlineExceeded);

        // 2b. Kill at WaitForBind: runtime exits before reporting where it bound -> Unavailable,
        //     and the name claim is withdrawn.
        builds
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_id.clone(),
                attempt: 2,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();
        *provider.inspect_status.lock().unwrap() = DeploymentStatus::Exited { code: Some(137) };
        let kill_bind_err = pipeline
            .create(&CreateRequest {
                build_id: build_id.clone(),
                attempt: 2,
                node: fault_path.clone(),
                spec: &RPC_NODE,
                data_dir: None,
                held_runtimes: Vec::new(),
                entry: None,
            })
            .await
            .map(|_| ())
            .expect_err("killed runtime at WaitForBind must fail with Unavailable");
        assert_eq!(kill_bind_err.step, CreateStep::WaitForBind.name());
        assert_eq!(kill_bind_err.code, rdm_core::outcome::Code::Unavailable);
        assert!(kill_bind_err.reason.contains("runtime exited"));

        // 2c. Kill at WaitForNodeReady: join report arrives, then runtime exits while waiting for Ready.
        builds
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_id.clone(),
                attempt: 3,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();
        *provider.deliver_join_on_inspect.lock().unwrap() = true;
        *provider.inspect_status.lock().unwrap() = DeploymentStatus::Running;
        *observer.kill_on_first_ready_check.lock().unwrap() = Some(provider.clone());
        let kill_ready_err = pipeline
            .create(&CreateRequest {
                build_id: build_id.clone(),
                attempt: 3,
                node: fault_path.clone(),
                spec: &RPC_NODE,
                data_dir: None,
                held_runtimes: Vec::new(),
                entry: None,
            })
            .await
            .map(|_| ())
            .expect_err("killed runtime at WaitForNodeReady must fail with Unavailable");
        assert_eq!(kill_ready_err.step, CreateStep::WaitForNodeReady.name());
        assert_eq!(kill_ready_err.code, rdm_core::outcome::Code::Unavailable);
        assert!(kill_ready_err.reason.contains("runtime exited"));

        // 2d. SIGSTOP (OsStopped) then resume across an interrupted attempt:
        //     Attempt 4 launches a fresh birth, completes through WaitForMeshJoin, and is cut short
        //     during WaitForNodeReady while the runtime is held OsStopped; attempt 5 inspects the
        //     handle as OsStopped (alive), keeps the exact same birth without spawning a second runtime,
        //     and completes once resumed and ready.
        builds
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_id.clone(),
                attempt: 4,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();
        *provider.inspect_status.lock().unwrap() = DeploymentStatus::Running;
        *observer.ready_ok.lock().unwrap() = false;
        let entered = observer.ready_entered.notified();
        let req4 = CreateRequest {
            build_id: build_id.clone(),
            attempt: 4,
            node: fault_path.clone(),
            spec: &RPC_NODE,
            data_dir: None,
            held_runtimes: Vec::new(),
            entry: None,
        };
        tokio::select! {
            _ = entered => {}
            res = pipeline.create(&req4) => panic!("attempt 4 must be cut at WaitForNodeReady before completing: {:?}", res.map(|c| c.node.node_id)),
        }
        let spawns_after_attempt4 = *provider.spawn_calls.lock().unwrap();
        let proj4 = builds.read_build(&build_id).await.unwrap();
        assert!(
            proj4.steps.iter().any(|r| {
                r.attempt == 4
                    && r.operation == format!("node.create:{fault_path}")
                    && r.step == CreateStep::WaitForMeshJoin.name()
                    && r.outcome == StepOutcome::Complete
            }),
            "attempt 4 completed through WaitForMeshJoin before being cut short"
        );

        // Now the OS holds the runtime OsStopped at attempt 5's start, then resumes it so Ready succeeds.
        builds
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_id.clone(),
                attempt: 5,
                executor: "mesh1.admin.1".into(),
            })
            .await
            .unwrap();
        *provider.inspect_status.lock().unwrap() = DeploymentStatus::OsStopped;
        *observer.ready_ok.lock().unwrap() = true;
        let resumed_created = pipeline
            .create(&CreateRequest {
                build_id: build_id.clone(),
                attempt: 5,
                node: fault_path.clone(),
                spec: &RPC_NODE,
                data_dir: None,
                held_runtimes: Vec::new(),
                entry: None,
            })
            .await
            .expect("OsStopped runtime is kept alive across attempts and completes once resumed");
        assert_eq!(
            *provider.spawn_calls.lock().unwrap(),
            spawns_after_attempt4,
            "an OsStopped runtime must never be spawned over by the next attempt"
        );
        assert_eq!(resumed_created.node.name, fault_path);
    });
    drop(rt);
    let _ = std::fs::remove_dir_all(&data_root);

    let claimed_spans = capture.named("rdm.node_admin.node.resolve.via-claimed-allocation");
    assert!(
        claimed_spans
            .iter()
            .any(|s| s.attributes.get("outcome").map(String::as_str) == Some("never-launched")),
        "expected via-claimed-allocation span with outcome=never-launched"
    );
    let names = capture.all_names();
    crate::assert_own_story(names.iter().map(String::as_str));
}
