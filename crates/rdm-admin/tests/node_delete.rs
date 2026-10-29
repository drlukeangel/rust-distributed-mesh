//! Functional cell for `node.delete` (`docs/architecture/lifecycles/node-delete.md`).

use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome,
};
use rdm_admin::deployment::pipeline::{
    CommandAdmission, CommandContext, Completion, DeleteRequest, DeleteStep, DeploymentPipeline,
    LaunchTemplate, LifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink,
};
use rdm_admin::deployment::provider::{
    DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, Found, ResolvedNodeLaunch,
    TerminationMode,
};
use rdm_admin::join::Joins;
use rdm_admin::model::{
    DeploymentId, EndpointId, Fabric, IncarnationId, Mesh, Node, NodeId, NodeStatus, PathName,
    ProviderKind, ScopeStatus,
};
use rdm_admin::node_commands::NodeCommand;
use rdm_admin::node_rows::{ClaimFor, WriteRecord as _};
use rdm_admin::storage::{MemoryNodesStorage, NodeRow, NodesStorage as _};
use rdm_admin::topology::Topology;
use rdm_core::status::ConnectionsReceipt;
use rdm_core::{LifecycleOp, RuntimeFact, RuntimeLocator};
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

struct ControlledExitProvider {
    status_after_terminate: Mutex<DeploymentStatus>,
    terminates: Mutex<Vec<(DeploymentId, TerminationMode)>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for ControlledExitProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }

    fn control_domain(&self) -> String {
        rdm_core::runtime::process_control_domain().expect("process control domain")
    }

    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn {
            node: spec.node.to_string(),
            reason: "this cell tests node.delete, not spawn".into(),
        })
    }

    async fn terminate(
        &self,
        handle: &DeploymentHandle,
        mode: TerminationMode,
    ) -> Result<(), DeployError> {
        self.terminates
            .lock()
            .unwrap()
            .push((handle.deployment_id.clone(), mode));
        Ok(())
    }

    async fn inspect(&self, _handle: &DeploymentHandle) -> DeploymentStatus {
        self.status_after_terminate.lock().unwrap().clone()
    }

    async fn signal_stop(&self, _handle: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }

    async fn find(&self, _spec: &ResolvedNodeLaunch) -> Result<Found, DeployError> {
        Ok(Found::NeverLaunched)
    }
}

struct LiveStopObserver {
    commands: Mutex<Vec<(PathName, NodeCommand, String)>>,
}

#[async_trait::async_trait]
impl NodeObserver for LiveStopObserver {
    async fn joined(&self, _node_id: &NodeId, _incarnation: &IncarnationId) -> Option<Publication> {
        None
    }

    async fn ready(&self, _node: &Node) -> Result<(), String> {
        Ok(())
    }

    async fn send_command(
        &self,
        node: &Node,
        cmd: NodeCommand,
        ctx: &CommandContext,
    ) -> CommandAdmission {
        self.commands
            .lock()
            .unwrap()
            .push((node.name.clone(), cmd, ctx.operation.clone()));
        match cmd {
            NodeCommand::Stop => CommandAdmission::Stopped {
                receipt: ConnectionsReceipt::Established {
                    in_flight: 0,
                    closed: Vec::new(),
                },
            },
            _ => CommandAdmission::Refused {
                reply: "unexpected command".into(),
                code: rdm_core::outcome::Code::FailedPrecondition,
            },
        }
    }

    async fn await_completion(
        &self,
        _node: &Node,
        _cmd: NodeCommand,
        _ctx: &CommandContext,
        _within: Duration,
    ) -> Completion {
        Completion::Received {}
    }
}

struct RecordingEvents {
    events: Mutex<Vec<(&'static str, PathName, NodeId, IncarnationId)>>,
}

#[async_trait::async_trait]
impl LifecycleEvents for RecordingEvents {
    async fn deleting(&self, op: &LifecycleOp) {
        self.events.lock().unwrap().push((
            "deleting",
            op.name.clone(),
            op.node_id.clone(),
            op.incarnation.clone(),
        ));
    }

    async fn deleted(&self, op: &LifecycleOp) {
        self.events.lock().unwrap().push((
            "deleted",
            op.name.clone(),
            op.node_id.clone(),
            op.incarnation.clone(),
        ));
    }

    async fn stopped(&self, op: &LifecycleOp) {
        self.events.lock().unwrap().push((
            "stopped",
            op.name.clone(),
            op.node_id.clone(),
            op.incarnation.clone(),
        ));
    }

    async fn joined(&self, op: &LifecycleOp) {
        self.events.lock().unwrap().push((
            "joined",
            op.name.clone(),
            op.node_id.clone(),
            op.incarnation.clone(),
        ));
    }

    async fn leaving(&self, op: &LifecycleOp) {
        self.events.lock().unwrap().push((
            "leaving",
            op.name.clone(),
            op.node_id.clone(),
            op.incarnation.clone(),
        ));
    }

    fn now_rafka_ms(&self) -> u64 {
        1_000_000
    }
}

struct SinkWithRows {
    removed_births: Mutex<Vec<(PathName, NodeId, IncarnationId)>>,
    rows: Arc<rdm_admin::node_rows::NodeRows>,
}

impl TopologySink for SinkWithRows {
    fn publish(&self, _node: Node) {}

    fn remove(&self, _name: &PathName) {}

    fn remove_birth(&self, name: &PathName, node_id: &NodeId, incarnation: &IncarnationId) {
        self.removed_births.lock().unwrap().push((
            name.clone(),
            node_id.clone(),
            incarnation.clone(),
        ));
    }

    fn node_rows(&self) -> Option<Arc<rdm_admin::node_rows::NodeRows>> {
        Some(self.rows.clone())
    }
}

// @feature: node-delete
/// Page: docs/architecture/lifecycles/node-delete.md:5
/// "`node.delete` composes the whole node.stop, then terminates the exact runtime
/// through its provider, reads it `Exited`, and releases identity. It releases node.id and name, **not storage**. The data
/// dir and node-identity.json / endpoint key stay with that data dir."
/// Page: docs/architecture/lifecycles/node-delete.md:26
/// "1. For a live process compose the whole node.stop.
///  2. Terminate and inspect the exact runtime through provider control.
///  3. The node's row writer writes the node's row with status deleted; the record and its config stay. Release node.id
///     and the name claim. Retain the data dir, node-identity.json and endpoint key."
#[test]
fn node_delete_pipeline_requires_exact_exited_verdict_releases_name_and_retains_storage() {
    let capture = Capture::new("node_delete_pipeline_requires_exact_exited_verdict");
    let dispatch = capture.dispatch.clone();
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
        .build()
        .unwrap();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    rt.block_on(async {
        let root_dir = std::env::temp_dir().join(format!(
            "rdm-node-delete-cell-{}-{}",
            std::process::id(),
            BuildId::mint()
        ));
        std::fs::create_dir_all(&root_dir).unwrap();

        let admin_name: PathName = "mesh1.admin.1".parse().unwrap();
        let mut admin_node = Node::allocated(admin_name.clone());
        admin_node.status = NodeStatus::Ready;
        admin_node.is_primary = true;
        admin_node.is_fabric_primary = true;
        admin_node.incarnation_id = Some(IncarnationId::mint());
        let admin_id = admin_node.node_id.clone();

        let target_name: PathName = "mesh1.rpc.1".parse().unwrap();
        let target_id = NodeId::mint();
        let target_incarnation = IncarnationId::mint();
        let deployment_id = DeploymentId::mint();

        let target_data_dir = root_dir.join(format!("{target_name}-{target_id}"));
        std::fs::create_dir_all(&target_data_dir).unwrap();
        let key = rdm_node::node::load_or_mint_key(&target_data_dir).expect("mint endpoint key");
        let endpoint_id = EndpointId(key.public().to_string());
        let identity_marker = target_data_dir.join("node-identity.json");
        std::fs::write(&identity_marker, b"{\"retained\":true}").unwrap();

        let mut target_node = Node::allocated(target_name.clone());
        target_node.node_id = target_id.clone();
        target_node.incarnation_id = Some(target_incarnation.clone());
        target_node.deployment_id = Some(deployment_id.clone());
        target_node.endpoint_id = Some(endpoint_id.clone());
        target_node.provider = Some(ProviderKind::Process);
        target_node.data_dir = Some(target_data_dir.display().to_string());
        target_node.status = NodeStatus::Ready;

        let store = Arc::new(MemoryNodesStorage::default());
        let mut initial_row = NodeRow::new(
            target_name.clone(),
            target_id.clone(),
            NodeStatus::Ready,
        );
        initial_row
            .config
            .insert("retention.role".into(), "rpc-worker".into());
        initial_row.birth_mut(&target_incarnation, &target_name);
        store.write_record(&initial_row).await.unwrap();

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
            nodes: vec![admin_node, target_node.clone()],
        }));

        let rows = Arc::new(rdm_admin::node_rows::NodeRows::new(
            admin_name.clone(),
            admin_id.clone(),
            view,
            store.clone(),
        ));
        rows.written().hold(&initial_row);

        let fact = RuntimeFact::of_this_process(deployment_id.as_str()).unwrap();
        let (pid, start) = match fact.locator {
            RuntimeLocator::Process { pid, start } => (pid, start),
            other => panic!("unexpected locator: {other:?}"),
        };
        let handle = DeploymentHandle {
            deployment_id: deployment_id.clone(),
            provider: ProviderKind::Process,
            pid: Some(pid),
            start: Some(start),
            container: None,
            domain: Some(fact.control_domain.clone()),
        };

        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: rdm_core::FabricId::mint(),
            executable: std::env::current_exe().unwrap(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher {
                name: admin_name.clone(),
                node_id: admin_id,
                incarnation: IncarnationId::mint(),
            },
            env: BTreeMap::new(),
            data_root: root_dir.clone(),
            store_dir: root_dir.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: Duration::from_secs(5),
            declared: Default::default(),
            binding: None,
        };

        // 1. If the provider still reports Running after terminate, TerminateRuntime fails and NodeDeleted does not run.
        let builds_fail = MemoryBuildStateAdapter::new();
        let build_fail_id = BuildId::mint();
        builds_fail
            .publish_accepted(&BuildAccepted {
                build_id: build_fail_id.clone(),
                topology: FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 1000,
            })
            .await
            .unwrap();
        builds_fail
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_fail_id.clone(),
                attempt: 1,
                executor: admin_name.to_string(),
            })
            .await
            .unwrap();

        let provider_running = ControlledExitProvider {
            status_after_terminate: Mutex::new(DeploymentStatus::Running),
            terminates: Mutex::new(Vec::new()),
        };
        let observer_fail = LiveStopObserver {
            commands: Mutex::new(Vec::new()),
        };
        let events_fail = RecordingEvents {
            events: Mutex::new(Vec::new()),
        };
        let sink_fail = SinkWithRows {
            removed_births: Mutex::new(Vec::new()),
            rows: rows.clone(),
        };
        let joins = Joins::default();
        let pipeline_fail = DeploymentPipeline {
            provider: &provider_running,
            joins: &joins,
            observer: &observer_fail,
            sink: &sink_fail,
            lifecycle: &events_fail,
            builds: &builds_fail,
            template: &template,
            timeouts: Timeouts::default(),
        };

        let req_fail = DeleteRequest {
            build_id: build_fail_id.clone(),
            attempt: 1,
            node: target_node.clone(),
            handle: handle.clone(),
            unreached: false,
        };
        let err = pipeline_fail
            .delete_node(&req_fail)
            .await
            .expect_err("delete must fail when provider still reports Running after terminate");
        assert_eq!(err.step, DeleteStep::TerminateRuntime.name());
        assert!(
            err.reason.contains("still runs after the stop ladder"),
            "unexpected failure reason: {}",
            err.reason
        );
        assert!(
            sink_fail.removed_births.lock().unwrap().is_empty(),
            "birth must not be removed when runtime has not exited"
        );
        assert!(
            !events_fail
                .events
                .lock()
                .unwrap()
                .iter()
                .any(|(kind, _, _, _)| *kind == "deleted"),
            "node.deleted must not be emitted without provider Exited"
        );

        // Restore the live target row to Ready for the full delete run.
        store.write_record(&initial_row).await.unwrap();
        rows.written().hold(&initial_row);

        // 2. When the provider reports Exited after terminate, node.delete composes node.stop,
        // verifies Exited, writes the row Deleted (keeping record & config, releasing name claim),
        // emits deleted, removes topology membership, and retains data_dir and node-key on disk.
        let builds_ok = MemoryBuildStateAdapter::new();
        let build_ok_id = BuildId::mint();
        builds_ok
            .publish_accepted(&BuildAccepted {
                build_id: build_ok_id.clone(),
                topology: FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 2000,
            })
            .await
            .unwrap();
        builds_ok
            .claim_attempt(&BuildAttemptClaim {
                claimant: None,
                mesh: None,
                build_id: build_ok_id.clone(),
                attempt: 1,
                executor: admin_name.to_string(),
            })
            .await
            .unwrap();

        let provider_exited = ControlledExitProvider {
            status_after_terminate: Mutex::new(DeploymentStatus::Exited { code: Some(0) }),
            terminates: Mutex::new(Vec::new()),
        };
        let observer_ok = LiveStopObserver {
            commands: Mutex::new(Vec::new()),
        };
        let events_ok = RecordingEvents {
            events: Mutex::new(Vec::new()),
        };
        let sink_ok = SinkWithRows {
            removed_births: Mutex::new(Vec::new()),
            rows: rows.clone(),
        };
        let pipeline_ok = DeploymentPipeline {
            provider: &provider_exited,
            joins: &joins,
            observer: &observer_ok,
            sink: &sink_ok,
            lifecycle: &events_ok,
            builds: &builds_ok,
            template: &template,
            timeouts: Timeouts::default(),
        };

        let req_ok = DeleteRequest {
            build_id: build_ok_id.clone(),
            attempt: 1,
            node: target_node.clone(),
            handle: handle.clone(),
            unreached: false,
        };
        let terminal = pipeline_ok
            .delete_node(&req_ok)
            .await
            .expect("node.delete must complete when provider reports Exited");
        assert_eq!(terminal.node, target_name.to_string());
        assert_eq!(terminal.node_id, target_id.to_string());
        assert_eq!(terminal.incarnation_id, target_incarnation.to_string());
        assert_eq!(terminal.exit_code, Some(0));

        // Composed node.stop command was sent before provider terminate.
        let sent_cmds = observer_ok.commands.lock().unwrap().clone();
        assert_eq!(
            sent_cmds,
            vec![(
                target_name.clone(),
                NodeCommand::Stop,
                "node.stop:mesh1.rpc.1".to_string()
            )]
        );
        assert_eq!(provider_exited.terminates.lock().unwrap().len(), 1);

        // Lifecycle events in order: deleting -> stopped -> deleted.
        let recorded_events: Vec<&str> = events_ok
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|(k, _, _, _)| *k)
            .collect();
        assert_eq!(recorded_events, vec!["deleting", "stopped", "deleted"]);

        // Row is written Deleted at its path.name with config kept, and name claim is released
        // so a successor node_id can claim mesh1.rpc.1.
        let deleted_row = store
            .node(&target_id)
            .await
            .unwrap()
            .expect("deleted row must remain in store");
        assert_eq!(deleted_row.status, NodeStatus::Deleted);
        assert_eq!(
            deleted_row.config.get("retention.role").map(String::as_str),
            Some("rpc-worker"),
            "deleted row must keep its config"
        );
        let successor_id = NodeId::mint();
        rows.claim(
            &target_name,
            &successor_id,
            "node.create:mesh1.rpc.1",
            None,
            None,
            ClaimFor::Create,
        )
        .await
        .expect("deleted row must release the name claim for a successor node_id");

        // Birth removed from topology sink.
        assert_eq!(
            *sink_ok.removed_births.lock().unwrap(),
            vec![(
                target_name.clone(),
                target_id.clone(),
                target_incarnation.clone()
            )]
        );

        // Build receipts match DeleteStep::ORDER.
        let view = builds_ok.read_build(&build_ok_id).await.unwrap();
        let completed_steps: Vec<&str> = view
            .steps
            .iter()
            .filter(|r| r.outcome == StepOutcome::Complete)
            .map(|r| r.step.as_str())
            .collect();
        let expected_steps: Vec<&str> = DeleteStep::ORDER.iter().map(|s| s.name()).collect();
        assert_eq!(completed_steps, expected_steps);

        // Storage (data dir, node-identity.json, and endpoint key) is retained on disk.
        assert!(
            target_data_dir.is_dir(),
            "node.delete must retain the node data directory"
        );
        assert!(
            identity_marker.is_file(),
            "node.delete must retain node-identity.json in the data directory"
        );
        let retained_key = rdm_admin::deployment::pipeline::inspect_transport_key(&target_data_dir)
            .expect("endpoint key must remain intact in data_dir");
        assert_eq!(retained_key, endpoint_id);

        let _ = std::fs::remove_dir_all(&root_dir);
    });

    drop(rt);

    let step_spans = capture.named("rdm.node_admin.deployment.update.via-step");
    let completed_span_steps: Vec<&str> = step_spans
        .iter()
        .filter(|s| s.attributes.get("outcome").map(String::as_str) == Some("complete"))
        .filter_map(|s| s.attributes.get("step").map(String::as_str))
        .collect();
    for expected in DeleteStep::ORDER {
        assert!(
            completed_span_steps.contains(&expected.name()),
            "missing completed step span {} in {completed_span_steps:?}",
            expected.name()
        );
    }
    crate::assert_own_story(capture.all_names().iter().map(String::as_str));
}
