//! Functional cell for `node.delete` (`docs/architecture/lifecycles/node-delete.md`).

use rdm_admin::accepted::FabricTopology;
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
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
use rdm_admin::node_rows::{RowFor, WriteRecord as _};
use rdm_admin::storage::{MemoryNodesStorage, NodeRecord, NodesStorage as _};
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

    async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn {
            path: spec.path.clone(), name: spec.name.clone(),
            reason: "this cell tests node.delete, not spawn".into(),
        })
    }

    async fn terminate(
        &self,
        handle: &DeploymentHandle,
        mode: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        self.terminates
            .lock()
            .unwrap()
            .push((handle.deployment_id.clone(), mode));
        Ok(())
    }

    async fn inspect(&self, _handle: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        self.status_after_terminate.lock().unwrap().clone()
    }

    async fn signal_stop(&self, _handle: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        Ok(())
    }

    async fn find(&self, _spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<Found, DeployError> {
        Ok(Found::NeverLaunched)
    }
}

struct LiveStopObserver {
    commands: Mutex<Vec<(NodeId, NodeCommand, rdm_core::OperationId)>>,
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
            .push((node.node_id.clone(), cmd, ctx.operation.clone()));
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
            op.location(),
            op.node_id.clone(),
            op.incarnation.clone(),
        ));
    }

    async fn deleted(&self, op: &LifecycleOp) {
        self.events.lock().unwrap().push((
            "deleted",
            op.location(),
            op.node_id.clone(),
            op.incarnation.clone(),
        ));
    }

    async fn stopped(&self, op: &LifecycleOp) {
        self.events.lock().unwrap().push((
            "stopped",
            op.location(),
            op.node_id.clone(),
            op.incarnation.clone(),
        ));
    }

    async fn joined(&self, op: &LifecycleOp) {
        self.events.lock().unwrap().push((
            "joined",
            op.location(),
            op.node_id.clone(),
            op.incarnation.clone(),
        ));
    }

    async fn leaving(&self, op: &LifecycleOp) {
        self.events.lock().unwrap().push((
            "leaving",
            op.location(),
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

        let admin_name: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let mut admin_node = Node::allocated(admin_name.clone(), "mesh1", "node-admin");
        admin_node.status = NodeStatus::Ready;
        admin_node.is_primary = true;
        admin_node.is_fabric_primary = true;
        admin_node.incarnation_id = Some(IncarnationId::mint());
        let admin_id = admin_node.node_id.clone();
        let executor_birth = admin_node.birth().unwrap();

        let target_name: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let target_id = NodeId::mint();
        let target_incarnation = IncarnationId::mint();
        let deployment_id = DeploymentId::mint();

        let target_data_dir = rdm_admin::deployment::node_data_dir(&root_dir, &target_id);
        std::fs::create_dir_all(&target_data_dir).unwrap();
        let key = rdm_node::node::load_or_mint_key(&target_data_dir).expect("mint endpoint key");
        let endpoint_id = EndpointId(key.public().to_string());
        let identity_marker = target_data_dir.join("node-identity.json");
        std::fs::write(&identity_marker, b"{\"retained\":true}").unwrap();

        let mut target_node = Node::allocated(target_name.clone(), "mesh1", "node");
        target_node.node_id = target_id.clone();
        target_node.incarnation_id = Some(target_incarnation.clone());
        target_node.deployment_id = Some(deployment_id.clone());
        target_node.endpoint_id = Some(endpoint_id.clone());
        target_node.provider = Some(ProviderKind::Process);
        target_node.data_dir = Some(target_data_dir.display().to_string());
        target_node.status = NodeStatus::Ready;

        let store = Arc::new(MemoryNodesStorage::default());
        let mut initial_row = rdm_node::nodes_storage::fixture_record(&target_name.clone(), "node", target_id.clone(), rdm_core::IncarnationId::mint(), NodeStatus::Ready);
        initial_row.birth_mut(&target_incarnation, &target_name);
        store.put_node(&initial_row).await.unwrap();

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
            launcher: rdm_core::launch::Launcher { mesh: "mesh1".into(),
                path: admin_name.path.clone(), name: admin_name.name.clone(),
                node_id: admin_id,
                incarnation: executor_birth.incarnation.clone(),
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
        let builds_fail = crate::stepping(&executor_birth.node_id, "mesh1");
        let build_fail_id = BuildId::mint();
        builds_fail
            .publish_accepted(&BuildAccepted {
                build_id: build_fail_id.clone(),
                topology: FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
                source: None, time: 1000,
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

        let req_fail = DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(target_node).node_id),
            build_id: Some(build_fail_id.clone()),
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
        store.put_node(&initial_row).await.unwrap();
        rows.written().hold(&initial_row);

        // 2. When the provider reports Exited after terminate, node.delete composes node.stop,
        // verifies Exited, writes the row Deleted (keeping record & config, releasing name claim),
        // emits deleted, removes topology membership, and retains data_dir and node-key on disk.
        let builds_ok = crate::stepping(&executor_birth.node_id, "mesh1");
        let build_ok_id = BuildId::mint();
        builds_ok
            .publish_accepted(&BuildAccepted {
                build_id: build_ok_id.clone(),
                topology: FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
                source: None, time: 2000,
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

        let req_ok = DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(target_node).node_id),
            build_id: Some(build_ok_id.clone()),
            node: target_node.clone(),
            handle: handle.clone(),
            unreached: false,
        };
        let terminal = pipeline_ok
            .delete_node(&req_ok)
            .await
            .expect("node.delete must complete when provider reports Exited");
        assert_eq!(terminal.location(), target_name);
        assert_eq!(terminal.node_id, target_id.to_string());
        assert_eq!(terminal.incarnation_id, target_incarnation.to_string());
        assert_eq!(terminal.exit_code, Some(0));

        // Composed node.stop command was sent before provider terminate.
        let sent_cmds = observer_ok.commands.lock().unwrap().clone();
        assert_eq!(
            sent_cmds,
            vec![(
                target_id.clone(),
                NodeCommand::Stop,
                rdm_core::OperationId::node("node.stop", &target_id)
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
        // so a successor node_id can claim mesh1.node.1.
        let deleted_row = store
            .node(&target_id)
            .await
            .unwrap()
            .expect("deleted row must remain in store");
        assert_eq!(deleted_row.status, NodeStatus::Deleted);
        assert_eq!(deleted_row.births.last().map(|b| b.incarnation_id.clone()), Some(target_incarnation.clone()), "the deleted row keeps its record, its birth included");
        let successor_id = NodeId::mint();
        rows.create_row(&target_name, "mesh1", "node", &successor_id, &rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap())), None, None, RowFor::Create)
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
        let retained_key = rdm_admin::deployment::pipeline::endpoint_id(&target_data_dir)
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

// @feature: node-delete
/// CONTRACT (docs/architecture/lifecycles/node-delete.md:17-19, 26-39, 55-60, 82-83, 87-88):
/// "OS-suspended target: The provider reports the exact runtime `os-stopped`: the stop is skipped
/// by name (a suspended process answers no call) and the delete begins at `TerminateRuntime` on the
/// exact handle, as `terminate(Immediate)` ... the delete completes only on the provider's `Exited`."
/// "1. For a live process compose the whole node.stop. For a crashed process those stop steps are
/// already done; do not fabricate node replies or receipts. 2. Terminate and inspect the exact runtime
/// through provider control."
/// "5. Block the old `(node.id, incarnation)` in the peer-location view. Peers refuse a late digest of
/// the deleted birth under the frame catalog's receiver rules, even when a successor uses the same
/// endpoint key."
/// "Failures and stream certainty follow the caller execution contract."
/// "Trace propagation, step spans and correlation follow the runtime spans."
/// Injects one fault per step of `node.delete`:
/// - OS-suspended target (`OsStopped`) that remains `OsStopped` after `terminate(Immediate)` -> skips
///   `StopNode` and `AwaitNodeLeft` by name (`rdm.node_admin.node.delete.via-os-stopped`) and fails
///   `TerminateRuntime` with no `NodeDeleted`;
/// - Cut after `NodeDeleting` and `TerminateRuntime` before `NodeDeleted` -> the next run reuses
///   `NodeDeleting` and `TerminateRuntime` without re-stopping or re-terminating and completes
///   `NodeDeleted`, `RemoveTopologyMembership`, and `Complete`;
/// - Lost reply at `StopNode` (`CommandAdmission::Indeterminate`) on a live delete -> records
///   `Completion::NotAwaited` at `AwaitNodeLeft` without fabricating a `stopped` event, terminates the
///   exact runtime through the provider, and completes `NodeDeleted` from the provider's `Exited`.
#[test]
fn node_delete_step_faults_recover_or_fail_as_the_page_specifies() {

    use rdm_admin::deployment::pipeline::TerminalReceipt;
    use std::collections::VecDeque;

    struct ScriptedDeleteProvider {
        inspects: Mutex<VecDeque<DeploymentStatus>>,
        terminates: Mutex<Vec<(DeploymentId, TerminationMode)>>,
    }

    #[async_trait::async_trait]
    impl DeploymentProvider for ScriptedDeleteProvider {
        fn kind(&self) -> ProviderKind {
            ProviderKind::Process
        }
        fn control_domain(&self) -> String {
            rdm_core::runtime::process_control_domain().expect("process control domain")
        }
        async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
            Err(DeployError::Spawn {
                path: spec.path.clone(), name: spec.name.clone(),
                reason: "node_delete_step_faults spawns nothing".into(),
            })
        }
        async fn terminate(
            &self,
            handle: &DeploymentHandle,
            mode: TerminationMode, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            self.terminates.lock().unwrap().push((handle.deployment_id.clone(), mode));
            Ok(())
        }
        async fn inspect(&self, _handle: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
            self.inspects
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(DeploymentStatus::Exited { code: Some(0) })
        }
        async fn signal_stop(&self, _handle: &DeploymentHandle, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
            Ok(())
        }
        async fn find(&self, _spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<Found, DeployError> {
            Ok(Found::NeverLaunched)
        }
    }

    struct IndeterminateStopObserver {
        commands: Mutex<Vec<(PathName, NodeCommand)>>,
    }

    #[async_trait::async_trait]
    impl NodeObserver for IndeterminateStopObserver {
        async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
            None
        }
        async fn ready(&self, _: &Node) -> Result<(), String> {
            Ok(())
        }
        async fn send_command(
            &self,
            node: &Node,
            cmd: NodeCommand,
            _ctx: &CommandContext,
        ) -> CommandAdmission {
            self.commands.lock().unwrap().push((node.location(), cmd));
            CommandAdmission::Indeterminate {
                reason: "stop stream cut before Stopped".into(),
                code: rdm_core::outcome::Code::Unavailable,
            }
        }
        async fn await_completion(
            &self,
            _node: &Node,
            _cmd: NodeCommand,
            _ctx: &CommandContext,
            _within: Duration,
        ) -> Completion {
            Completion::Deadline {}
        }
    }

    let capture = Capture::new("node_delete_step_faults");
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
            "rdm-node-delete-faults-{}-{}",
            std::process::id(),
            BuildId::mint()
        ));
        std::fs::create_dir_all(&root_dir).unwrap();

        let admin_name: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let mut admin_node = Node::allocated(admin_name.clone(), "mesh1", "node-admin");
        admin_node.status = NodeStatus::Ready;
        admin_node.is_primary = true;
        admin_node.is_fabric_primary = true;
        admin_node.incarnation_id = Some(IncarnationId::mint());
        let admin_id = admin_node.node_id.clone();
        let executor_birth = admin_node.birth().unwrap();

        let target_name: PathName = rdm_core::PathName::new("mesh1.node", "1").unwrap();
        let target_id = NodeId::mint();
        let target_incarnation = IncarnationId::mint();
        let deployment_id = DeploymentId::mint();
        let target_data_dir = rdm_admin::deployment::node_data_dir(&root_dir, &target_id);
        std::fs::create_dir_all(&target_data_dir).unwrap();

        let mut target_node = Node::allocated(target_name.clone(), "mesh1", "node");
        target_node.node_id = target_id.clone();
        target_node.incarnation_id = Some(target_incarnation.clone());
        target_node.deployment_id = Some(deployment_id.clone());
        target_node.provider = Some(ProviderKind::Process);
        target_node.data_dir = Some(target_data_dir.display().to_string());
        target_node.status = NodeStatus::Ready;

        let store = Arc::new(MemoryNodesStorage::default());
        let mut initial_row = rdm_node::nodes_storage::fixture_record(&target_name.clone(), "node", target_id.clone(), rdm_core::IncarnationId::mint(), NodeStatus::Ready);
        initial_row.birth_mut(&target_incarnation, &target_name);
        store.put_node(&initial_row).await.unwrap();

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
            launcher: rdm_core::launch::Launcher { mesh: "mesh1".into(),
                path: admin_name.path.clone(), name: admin_name.name.clone(),
                node_id: admin_id,
                incarnation: executor_birth.incarnation.clone(),
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

        let builds = crate::stepping(&executor_birth.node_id, "mesh1");
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
                source: None, time: 1000,
            })
            .await
            .unwrap();

        let provider = ScriptedDeleteProvider {
            inspects: Mutex::new(VecDeque::from([
                // The first run: inspected as OsStopped at delete start (skips StopNode/AwaitNodeLeft),
                // then still OsStopped after terminate(Immediate) -> fails TerminateRuntime.
                DeploymentStatus::OsStopped,
                DeploymentStatus::OsStopped,
                // Build 2: live target (Running at stop_skipped_os_stopped), then Exited after terminate.
                DeploymentStatus::Running,
                DeploymentStatus::Exited { code: Some(0) },
            ])),
            terminates: Mutex::new(Vec::new()),
        };
        let observer = IndeterminateStopObserver {
            commands: Mutex::new(Vec::new()),
        };
        let events = RecordingEvents {
            events: Mutex::new(Vec::new()),
        };
        let sink = SinkWithRows {
            removed_births: Mutex::new(Vec::new()),
            rows: rows.clone(),
        };
        let joins = Joins::default();
        let pipeline = DeploymentPipeline {
            provider: &provider,
            joins: &joins,
            observer: &observer,
            sink: &sink,
            lifecycle: &events,
            builds: &builds,
            template: &template,
            timeouts: Timeouts::default(),
        };

        // 1. The first run: an OS-suspended target (`OsStopped`) skips `StopNode`/`AwaitNodeLeft`, calls
        // `terminate(Immediate)`, and fails `TerminateRuntime` when still `OsStopped`.
        let err1 = pipeline
            .delete_node(&DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(target_node).node_id),
                build_id: Some(build_id.clone()),
                node: target_node.clone(),
                handle: handle.clone(),
                unreached: false,
            })
            .await
            .expect_err("OsStopped after terminate(Immediate) must fail TerminateRuntime");
        assert_eq!(err1.step, DeleteStep::TerminateRuntime.name());
        assert!(
            err1.reason.contains("suspended after the stop ladder (os-stopped)"),
            "unexpected reason: {}",
            err1.reason
        );
        assert!(
            observer.commands.lock().unwrap().is_empty(),
            "OS-suspended delete must skip StopNode and send no command"
        );
        assert_eq!(
            *provider.terminates.lock().unwrap(),
            vec![(deployment_id.clone(), TerminationMode::Immediate)],
            "OS-suspended delete must call terminate(Immediate)"
        );

        // 2. A run is cut after recording `NodeDeleting` and `TerminateRuntime` (`Complete`) before
        // `NodeDeleted`; the run after it reuses both steps without calling `provider.terminate` or sending
        // `StopNode`, and completes `NodeDeleted`, `RemoveTopologyMembership`, and `Complete`.
        builds
            .write_step(&rdm_admin::build_state::StepWrite { build_id: build_id.clone(), mesh: None, operation: rdm_core::OperationId::node("node.delete", &target_id), step: DeleteStep::NodeDeleting.name().into(), outcome: StepOutcome::Complete })
            .await
            .unwrap();
        builds
            .write_step(&rdm_admin::build_state::StepWrite { build_id: build_id.clone(), mesh: None, operation: rdm_core::OperationId::node("node.delete", &target_id), step: DeleteStep::TerminateRuntime.name().into(), outcome: StepOutcome::Complete })
            .await
            .unwrap();

        let out3 = pipeline
            .delete_node(&DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(target_node).node_id),
                build_id: Some(build_id.clone()),
                node: target_node.clone(),
                handle: handle.clone(),
                unreached: false,
            })
            .await
            .expect("the run after the cut must reuse TerminateRuntime and complete NodeDeleted");
        assert_eq!((out3.exit_code, out3.verdict.as_deref()), (None, None), "the reused TerminateRuntime hands on the runtime the durable row names, and reads no exit again (D67)");
        assert_eq!(
            provider.terminates.lock().unwrap().len(),
            1,
            "the run after the cut must reuse TerminateRuntime without calling provider.terminate again"
        );
        assert!(observer.commands.lock().unwrap().is_empty());

        // 3. Live target whose `StopNode` reply is lost (`CommandAdmission::Indeterminate`): under
        // `delete_node` (`RequiredCompletion::Lenient`), `AwaitNodeLeft` records `Completion::NotAwaited` without
        // fabricating a `stopped` event, and the delete proceeds to `TerminateRuntime`, verifies `Exited`,
        // and completes `NodeDeleted`.
        store.put_node(&initial_row).await.unwrap();
        rows.written().hold(&initial_row);
        events.events.lock().unwrap().clear();

        let build_id_live = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id_live.clone(),
                topology: FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                change: rdm_core::build::BuildChange::Create { meshes: Vec::new(), nodes: Vec::new() },
                source: None, time: 2000,
            })
            .await
            .unwrap();
        let out_live = pipeline
            .delete_node(&DeleteRequest { operation: rdm_core::OperationId::node("node.delete", &(target_node).node_id),
                build_id: Some(build_id_live.clone()),
                node: target_node.clone(),
                handle: handle.clone(),
                unreached: false,
            })
            .await
            .expect("live delete with lost stop reply must still terminate runtime and complete");
        assert_eq!(out_live.exit_code, Some(0));
        assert_eq!(observer.commands.lock().unwrap().len(), 1);
        let live_events: Vec<&str> = events
            .events
            .lock()
            .unwrap()
            .iter()
            .map(|(k, _, _, _)| *k)
            .collect();
        assert_eq!(
            live_events,
            vec!["deleting", "deleted"],
            "lost stop reply must not fabricate a stopped event before deleted: {live_events:?}"
        );

        // Step 5 (node-delete.md:37-39): block the old `(node.id, incarnation)` in the peer-location
        // view; peers refuse a late digest of the deleted birth once a successor holds its (path, name),
        // the successor at its own new endpoint (Q358).
        let peer_view = rdm_transport::topology::DigestView::default();
        let deleted_endpoint = EndpointId("deleted-birth-endpoint-key".into());
        let old_digest = rdm_core::MeshDigest {
            fabric_id: template.fabric_id.clone(),
            node: rdm_core::MeshNode { mesh: "mesh1".to_owned(), r#type: "node".into(),
                node_id: target_id.clone(),
                path: target_name.path.clone(), name: target_name.name.clone(),
                endpoint_id: deleted_endpoint,
                transport_addr: "127.0.0.1:41001".parse().unwrap(),
                incarnation: target_incarnation.clone(),
                supersedes: None,
                runtime: None,
            },
            status: rdm_core::MemberStatus::Ready,
            node_state: Default::default(),
            admin_api_base: None,
            digest_seq: 1,
            emitted_at_rafka_ms: 1_000,
            data_dir: None,
            mesh_id: None,
            in_flight: None,
            load: None,
            gossip: None,
            is_primary: None,
            is_fabric_primary: None,
        };
        assert!(peer_view.record(old_digest.clone()));
        assert!(peer_view.depart(LifecycleOp { r#type: "node".into(), mesh: "mesh1".to_owned(),
            build_id: build_id_live.to_string(),
            operation: rdm_core::OperationId::node("node.delete", &target_id),
            node_id: target_id.clone(),
            incarnation: target_incarnation.clone(),
            path: target_name.path.clone(), name: target_name.name.clone(),
            event_at_rafka_ms: 2_000,
            traceparent: None,
            tracestate: None,
        }));
        let (successor_id, successor_inc) = (NodeId::mint(), IncarnationId::mint());
        let successor_digest = rdm_core::MeshDigest {
            node: rdm_core::MeshNode { mesh: "mesh1".to_owned(), r#type: "node".into(),
                node_id: successor_id.clone(),
                path: target_name.path.clone(), name: target_name.name.clone(),
                endpoint_id: EndpointId("successor-endpoint-key".into()),
                transport_addr: "127.0.0.1:41002".parse().unwrap(),
                incarnation: successor_inc.clone(),
                supersedes: None,
                runtime: None,
            },
            digest_seq: 1,
            emitted_at_rafka_ms: 3_000,
            ..old_digest.clone()
        };
        assert!(peer_view.record(successor_digest), "the successor at the same (path, name) and its own endpoint is admitted");
        let late_old_digest = rdm_core::MeshDigest {
            digest_seq: 99,
            emitted_at_rafka_ms: 4_000,
            ..old_digest
        };
        assert!(
            !peer_view.record(late_old_digest),
            "a late digest of the deleted birth is refused once its successor holds the (path, name)"
        );
        assert!(peer_view.get(target_id.as_str()).is_none());
        assert!(peer_view.get(successor_id.as_str()).is_some());

        let _ = std::fs::remove_dir_all(&root_dir);
    });

    drop(rt);

    let os_stopped_spans = capture.named("rdm.node_admin.node.delete.via-os-stopped");
    assert_eq!(
        os_stopped_spans.len(),
        1,
        "expected rdm.node_admin.node.delete.via-os-stopped span on the first run"
    );
    assert_eq!(
        os_stopped_spans[0].attributes.get("skipped").map(String::as_str),
        Some("StopNode AwaitNodeLeft")
    );
    let step_spans = capture.named("rdm.node_admin.deployment.update.via-step");
    assert!(
        step_spans.iter().any(|s| {
            s.attributes.get("step").map(String::as_str) == Some(DeleteStep::TerminateRuntime.name())
                && s.attributes.get("outcome").map(String::as_str) == Some("failed")
        }),
        "expected failed TerminateRuntime span on the first run"
    );
    assert!(
        step_spans.iter().any(|s| {
            s.attributes.get("step").map(String::as_str) == Some(DeleteStep::TerminateRuntime.name())
                && s.attributes.get("outcome").map(String::as_str) == Some("reused")
        }),
        "expected reused TerminateRuntime span on the run after the cut"
    );
    assert!(
        step_spans.iter().any(|s| {
            s.attributes.get("step").map(String::as_str) == Some(DeleteStep::Complete.name())
                && s.attributes.get("outcome").map(String::as_str) == Some("complete")
        }),
        "expected complete Complete span on the run after the cut"
    );
    crate::assert_own_story(capture.all_names().iter().map(String::as_str));
}
