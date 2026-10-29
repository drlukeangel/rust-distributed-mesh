//! A node.delete of a stopped node (lifecycles/node-delete.md step 1, node-stop.md step 6): the stopped
//! birth already ran its whole stop (connections delete receipt, connection cut, `stopped`, `NodeStopped`), so
//! its delete composes nothing of that stop again. It goes from the `NodeDeleting` pre-notice
//! straight to the provider's exact runtime exited verdict, then `NodeDeleted`, and releases no storage
//! (node-delete.md "Storage": "Retain data dir and endpoint key"). The cell drives the delete
//! pipeline over a recording provider and a recording observer: nothing here depends on timing.

use rdm_core::{IncarnationId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::deployment::pipeline::{
    CommandAdmission, CommandContext, Completion, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, DeleteRequest, DeleteStep, Timeouts, TopologySink,
};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::{DeploymentId, Node, NodeStatus, PathName, ProviderKind};
use rdm_admin::node_commands::NodeCommand;
use std::sync::Mutex;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

/// The provider: terminating the exact runtime succeeds and its inspection then says it exited.
#[derive(Default)]
struct Exits {
    terminated: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for Exits {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "i143-3072-delete".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this cell births nothing".into() })
    }
    async fn terminate(&self, h: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        self.terminated.lock().unwrap().push(h.deployment_id.to_string());
        Ok(())
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        if self.terminated.lock().unwrap().contains(&h.deployment_id.to_string()) {
            DeploymentStatus::Exited { code: Some(0) }
        } else {
            DeploymentStatus::Running
        }
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

/// The stopped birth: every command or seat transfer sent to it is recorded. A stopped birth has
/// cut its connections; anything sent to it is a defect of the delete.
#[derive(Default)]
struct Recorded {
    sent: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl NodeObserver for Recorded {
    async fn send_command(&self, node: &Node, cmd: NodeCommand, _: &CommandContext) -> CommandAdmission {
        self.sent.lock().unwrap().push(format!("{cmd:?} to {}", node.name));
        CommandAdmission::Admitted {}
    }
    async fn await_completion(&self, node: &Node, cmd: NodeCommand, _: &CommandContext, _: Duration) -> Completion {
        self.sent.lock().unwrap().push(format!("await {cmd:?} from {}", node.name));
        Completion::Received {}
    }
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

struct Discard;
impl TopologySink for Discard {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, _: &PathName, _: &NodeId, _: &IncarnationId) {}
}

/// CONTRACT (lifecycles/node-delete.md 1-2: "For a crashed process those stop steps are already done; do
/// not fabricate node replies or receipts. Stop and inspect the exact runtime through provider
/// control"; node-stop.md 6: "Delete proceeds to external exact runtime exited verdict instead"): the
/// delete of a stopped birth (Leaving, stopped) records `NodeDeleting`, then `TerminateRuntime` (the
/// provider's stop and its `Exited` inspection of the exact runtime), then `NodeDeleted`, and
/// `Complete`. Must NOT happen: a stop sent again to the stopped birth, or any of its receipts
/// (`StopNode`, `AwaitNodeLeft`), or a step that releases storage (`ReleaseStorage`: node-delete.md step 3,
/// "Retain the data dir, node-identity.json and endpoint key").
#[tokio::test]
async fn a_stopped_nodes_delete_terminates_its_exact_runtime_and_sends_no_stop() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("i143-3072-stopped-delete", exporter.clone());
    let _g = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));

    let builds = MemoryBuildStateAdapter::new();
    let build_id = BuildId::mint();
    builds
        .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
        .await
        .unwrap();
    builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
    let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
    node.node_id = NodeId::mint();
    node.incarnation_id = Some(IncarnationId::mint());
    node.provider = Some(ProviderKind::Process);
    node.status = NodeStatus::Leaving;
    node.stopped = true;
    let template = LaunchTemplate { fabric: "fabric1".into(),
        fabric_id: rdm_core::FabricId::mint(),
        executable: "/nonexistent/stopped-delete".into(),
        seeds: vec![],
        launcher: rdm_core::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
        env: Default::default(),
        data_root: std::env::temp_dir().join(format!("i143-3072-delete-{}", NodeId::mint())),
        store_dir: std::env::temp_dir().join(format!("i143-3072-delete-store-{}", NodeId::mint())),
        issuing: None,
        roots: Default::default(),
        connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        declared: Default::default(),
        binding: None,
    };
    let (provider, observer, joins) = (Exits::default(), Recorded::default(), rdm_admin::join::Joins::default());
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &joins,
        observer: &observer,
        sink: &Discard,
        lifecycle: &NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts { completion: Duration::from_millis(300), stop_grace: Duration::from_millis(300), ..Timeouts::default() },
    };
    let handle = DeploymentHandle { deployment_id: DeploymentId::mint(), provider: ProviderKind::Process, pid: None, start: None, container: None, domain: Some("i143-3072-delete".into()) };
    let req = DeleteRequest { build_id: build_id.clone(), attempt: 1, node, handle: handle.clone(), unreached: false };
    let out = pipeline.delete_node(&req).await;
    // The trace first: a red below still names the steps the delete ran, with their trace id.
    capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
    let spans = exporter.get_finished_spans().unwrap();
    for s in spans.iter().filter(|s| s.name.starts_with("rdm.node_admin.deployment.update.via-")) {
        let step = s.attributes.iter().find(|kv| kv.key.as_str() == "step").map(|kv| kv.value.to_string()).unwrap_or_default();
        eprintln!("span {} trace_id={} step={step}", s.name, s.span_context.trace_id());
    }

    let view = builds.read_build(&build_id).await.unwrap();
    let order: Vec<&str> = view.steps.iter().map(|s| s.step.as_str()).collect();
    assert_eq!(*observer.sent.lock().unwrap(), Vec::<String>::new(), "nothing is sent to a stopped birth: its stop is done; steps {order:?}");
    assert_eq!(
        order,
        [DeleteStep::NodeDeleting, DeleteStep::TerminateRuntime, DeleteStep::NodeDeleted, DeleteStep::RemoveTopologyMembership, DeleteStep::Complete].map(|s| s.name()),
        "the pre-notice, the exact runtime's exited verdict, the departure, and no storage release: {out:?}"
    );
    assert!(out.is_ok(), "{out:?}");
    assert_eq!(*provider.terminated.lock().unwrap(), vec![handle.deployment_id.to_string()], "the exact runtime is stopped through provider control, once");

    let steps: Vec<String> = spans
        .iter()
        .filter(|s| s.name == "rdm.node_admin.deployment.update.via-step")
        .filter_map(|s| s.attributes.iter().find(|kv| kv.key.as_str() == "step").map(|kv| kv.value.to_string()))
        .collect();
    assert!(!steps.iter().any(|s| s == DeleteStep::StopNode.name()), "no stop step span: {steps:?}");
    assert!(steps.iter().any(|s| s == DeleteStep::TerminateRuntime.name()), "{steps:?}");
    assert!(!steps.iter().any(|s| s == "ReleaseStorage"), "no step span releases storage: {steps:?}");
}

struct StopStepFaultProvider {
    status: Mutex<DeploymentStatus>,
}

#[async_trait::async_trait]
impl DeploymentProvider for StopStepFaultProvider {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "stop-step-faults".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn {
            node: spec.node.to_string(),
            reason: "stop deploys nothing".into(),
        })
    }
    async fn terminate(&self, _: &DeploymentHandle, _: TerminationMode) -> Result<(), DeployError> {
        Ok(())
    }
    async fn inspect(&self, _: &DeploymentHandle) -> DeploymentStatus {
        self.status.lock().unwrap().clone()
    }
    async fn signal_stop(&self, _: &DeploymentHandle) -> Result<(), DeployError> {
        Ok(())
    }
    async fn find(
        &self,
        _: &ResolvedNodeLaunch,
    ) -> Result<rdm_admin::deployment::provider::Found, DeployError> {
        Ok(rdm_admin::deployment::provider::Found::NeverLaunched)
    }
}

struct StopStepFaultObserver {
    admission: Mutex<CommandAdmission>,
    completion: Mutex<Completion>,
    hold_completion: Mutex<bool>,
    commands_sent: Mutex<usize>,
}

#[async_trait::async_trait]
impl NodeObserver for StopStepFaultObserver {
    async fn send_command(&self, _: &Node, _: NodeCommand, _: &CommandContext) -> CommandAdmission {
        *self.commands_sent.lock().unwrap() += 1;
        self.admission.lock().unwrap().clone()
    }
    async fn await_completion(
        &self,
        _: &Node,
        _: NodeCommand,
        _: &CommandContext,
        _within: Duration,
    ) -> Completion {
        if *self.hold_completion.lock().unwrap() {
            std::future::pending::<()>().await;
        }
        self.completion.lock().unwrap().clone()
    }
    async fn joined(&self, _: &NodeId, _: &IncarnationId) -> Option<Publication> {
        None
    }
    async fn ready(&self, _: &Node) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Default)]
struct StopLifecycleRecorder {
    stopped_ops: Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl rdm_admin::deployment::pipeline::LifecycleEvents for StopLifecycleRecorder {
    async fn deleting(&self, _: &rdm_core::LifecycleOp) {}
    async fn deleted(&self, _: &rdm_core::LifecycleOp) {}
    async fn stopped(&self, op: &rdm_core::LifecycleOp) {
        self.stopped_ops.lock().unwrap().push(op.operation.clone());
    }
    async fn joined(&self, _: &rdm_core::LifecycleOp) {}
    async fn leaving(&self, _: &rdm_core::LifecycleOp) {}
    fn now_rafka_ms(&self) -> u64 {
        2_000_000
    }
}

/// CONTRACT: `docs/architecture/lifecycles/node-stop.md:25`
/// "Completion | The matching `node.stopped` on the stop stream and the recorded stop receipt; no runtime Exited receipt is required"
/// CONTRACT: `docs/architecture/lifecycles/node-stop.md:33-34`
/// "The node validates the command and sends `CommandAccepted`; only this admission makes the mesh-primary
/// record stopping. A never-sent command writes no state."
/// CONTRACT: `docs/architecture/lifecycles/node-stop.md:54`
/// "5. The mesh-primary takes the matching stopped frame, or the node's declaration of `Stopped`
/// naming the stop's operation, whichever reaches it first, and publishes NodeStopped with that
/// operation in its in-flight overlay, once."
/// CONTRACT: `docs/architecture/lifecycles/node-stop.md:116-117, 121-122`
/// "Failures and stream certainty follow the caller execution contract."
/// "Trace propagation, step spans and correlation follow the runtime spans."
#[test]
fn node_stop_step_faults_recover_or_fail_as_the_page_specifies() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) =
        rdm_transport::span_capture("node_stop_step_faults", exporter.clone());
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let d = dispatch.clone();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .on_thread_start(move || std::mem::forget(tracing::dispatcher::set_default(&d)))
        .build()
        .unwrap();
    let _guard = tracing::dispatcher::set_default(&dispatch);

    rt.block_on(async {
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted {
                build_id: build_id.clone(),
                topology: rdm_admin::accepted::FabricTopology::root(
                    "fabric1",
                    "mesh1",
                    rdm_admin::model::MeshId::parse("meshd0000001").unwrap(),
                ),
                submitted_change: None,
                submitted_at_ms: 0,
            })
            .await
            .unwrap();

        let mut node = Node::allocated("mesh1.rpc.1".parse().unwrap());
        node.node_id = NodeId::mint();
        node.incarnation_id = Some(IncarnationId::mint());
        node.provider = Some(ProviderKind::Process);
        node.status = NodeStatus::Ready;
        let handle = DeploymentHandle {
            deployment_id: DeploymentId::mint(),
            provider: ProviderKind::Process,
            pid: Some(4242),
            start: Some(1),
            container: None,
            domain: Some("stop-step-faults".into()),
        };
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: rdm_core::FabricId::mint(),
            executable: "/nonexistent/node-stop-chaos".into(),
            seeds: vec![("adminkey".into(), "127.0.0.1:41002".parse().unwrap())],
            launcher: rdm_core::launch::Launcher {
                name: "mesh1.admin.1".parse().unwrap(),
                node_id: NodeId::mint(),
                incarnation: IncarnationId::mint(),
            },
            env: Default::default(),
            data_root: std::env::temp_dir().join(format!("stop-chaos-{}", NodeId::mint())),
            store_dir: std::env::temp_dir().join(format!("stop-chaos-store-{}", NodeId::mint())),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let provider = StopStepFaultProvider {
            status: Mutex::new(DeploymentStatus::Running),
        };
        let observer = StopStepFaultObserver {
            admission: Mutex::new(CommandAdmission::Indeterminate {
                reason: "reply lost".into(),
                code: rdm_core::outcome::Code::Unknown,
            }),
            completion: Mutex::new(Completion::Received {}),
            hold_completion: Mutex::new(false),
            commands_sent: Mutex::new(0),
        };
        let lifecycle = StopLifecycleRecorder::default();
        let joins = rdm_admin::join::Joins::default();
        let pipeline = DeploymentPipeline {
            provider: &provider,
            joins: &joins,
            observer: &observer,
            sink: &Discard,
            lifecycle: &lifecycle,
            builds: &builds,
            template: &template,
            timeouts: Timeouts {
                completion: Duration::from_millis(80),
                stop_grace: Duration::from_millis(80),
                ..Timeouts::default()
            },
        };

        // 1. Lost reply at StopNode (CommandAdmission::Indeterminate):
        //    AwaitNodeLeft fails because stop-node was not admitted; no NodeStopped is published.
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
        let err_lost = pipeline
            .stop(&DeleteRequest {
                build_id: build_id.clone(),
                attempt: 1,
                node: node.clone(),
                handle: handle.clone(),
                unreached: false,
            })
            .await
            .expect_err("lost reply at StopNode must fail AwaitNodeLeft");
        assert_eq!(err_lost.step, DeleteStep::AwaitNodeLeft.name());
        assert_eq!(err_lost.code, rdm_core::outcome::Code::Unknown);
        assert!(lifecycle.stopped_ops.lock().unwrap().is_empty());

        // 2. Kill at AwaitNodeLeft (runtime Exited before Stopped reply arrives):
        //    StopNode is admitted, then runtime exits while AwaitNodeLeft waits; fails Unavailable.
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
        *observer.admission.lock().unwrap() = CommandAdmission::Admitted {};
        *observer.hold_completion.lock().unwrap() = true;
        *provider.status.lock().unwrap() = DeploymentStatus::Exited { code: Some(137) };
        let err_kill = pipeline
            .stop(&DeleteRequest {
                build_id: build_id.clone(),
                attempt: 2,
                node: node.clone(),
                handle: handle.clone(),
                unreached: false,
            })
            .await
            .expect_err("killed runtime during AwaitNodeLeft must fail with Unavailable");
        assert_eq!(err_kill.step, DeleteStep::AwaitNodeLeft.name());
        assert_eq!(err_kill.code, rdm_core::outcome::Code::Unavailable);
        assert!(err_kill.reason.contains("RuntimeExited"));
        assert!(lifecycle.stopped_ops.lock().unwrap().is_empty());

        // 3. SIGSTOP (OsStopped) at AwaitNodeLeft:
        //    OsStopped is not Exited; when the stopped reply does not arrive before the completion
        //    deadline, AwaitNodeLeft fails DeadlineExceeded and publishes no NodeStopped.
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
        *provider.status.lock().unwrap() = DeploymentStatus::OsStopped;
        *observer.hold_completion.lock().unwrap() = false;
        *observer.completion.lock().unwrap() = Completion::Deadline {};
        let err_os_stop = pipeline
            .stop(&DeleteRequest {
                build_id: build_id.clone(),
                attempt: 3,
                node: node.clone(),
                handle: handle.clone(),
                unreached: false,
            })
            .await
            .expect_err("OsStopped runtime that misses completion deadline must fail AwaitNodeLeft");
        assert_eq!(err_os_stop.step, DeleteStep::AwaitNodeLeft.name());
        assert_eq!(err_os_stop.code, rdm_core::outcome::Code::DeadlineExceeded);
        assert!(lifecycle.stopped_ops.lock().unwrap().is_empty());

        // 4. Cut after StopNode (Stopped receipt recorded at attempt 4, cut before AwaitNodeLeft),
        //    then resumed at attempt 5 once the runtime is Running again:
        //    Attempt 5 reuses the StopNode receipt without sending a second stop-node command,
        //    completes AwaitNodeLeft from the recorded Stopped receipt, publishes NodeStopped once,
        //    and records Complete.
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
        let stopped_admission = CommandAdmission::Stopped {
            receipt: rdm_core::status::ConnectionsReceipt::Established {
                in_flight: 0,
                closed: Vec::new(),
            },
        };
        builds
            .append_step_receipt(&rdm_admin::build_state::BuildStepReceipt {
                build_id: build_id.clone(),
                mesh: None,
                attempt: 4,
                operation: "node.stop:mesh1.rpc.1".into(),
                step: DeleteStep::StopNode.name().into(),
                outcome: rdm_admin::build_state::StepOutcome::Complete,
                output: Some(serde_json::to_value(&stopped_admission).unwrap()),
                executor: Some("adminkey@127.0.0.1:41002".into()),
            })
            .await
            .unwrap();

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
        *provider.status.lock().unwrap() = DeploymentStatus::Running;
        let sent_before_resume = *observer.commands_sent.lock().unwrap();
        pipeline
            .stop(&DeleteRequest {
                build_id: build_id.clone(),
                attempt: 5,
                node: node.clone(),
                handle: handle.clone(),
                unreached: false,
            })
            .await
            .expect("resumed stop completes from the cut StopNode receipt");
        assert_eq!(
            *observer.commands_sent.lock().unwrap(),
            sent_before_resume,
            "resumed stop must reuse the completed StopNode receipt without re-sending stop-node"
        );
        assert_eq!(
            *lifecycle.stopped_ops.lock().unwrap(),
            vec!["node.stop:mesh1.rpc.1".to_string()],
            "NodeStopped must be published exactly once upon completion"
        );
    });
    drop(rt);

    capture
        .flush(Duration::from_secs(10))
        .unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
    let spans = exporter.get_finished_spans().unwrap();
    assert!(
        spans
            .iter()
            .any(|s| s.name == "rdm.node_admin.node.update.via-completion-awaited"),
        "expected rdm.node_admin.node.update.via-completion-awaited span"
    );
    let step_spans: Vec<std::collections::BTreeMap<String, String>> = spans
        .iter()
        .filter(|s| s.name == "rdm.node_admin.deployment.update.via-step")
        .map(|s| s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect())
        .collect();
    assert!(
        step_spans.iter().any(|s| {
            s.get("step").map(String::as_str) == Some(DeleteStep::AwaitNodeLeft.name())
                && s.get("attempt").map(String::as_str) == Some("1")
                && s.get("outcome").map(String::as_str) == Some("failed")
        }),
        "expected failed AwaitNodeLeft span on attempt 1: {step_spans:?}"
    );
    assert!(
        step_spans.iter().any(|s| {
            s.get("step").map(String::as_str) == Some(DeleteStep::StopNode.name())
                && s.get("attempt").map(String::as_str) == Some("5")
                && s.get("outcome").map(String::as_str) == Some("reused")
        }),
        "expected reused StopNode span on attempt 5: {step_spans:?}"
    );
    assert!(
        step_spans.iter().any(|s| {
            s.get("step").map(String::as_str) == Some(DeleteStep::Complete.name())
                && s.get("attempt").map(String::as_str) == Some("5")
                && s.get("outcome").map(String::as_str) == Some("complete")
        }),
        "expected complete Complete span on attempt 5: {step_spans:?}"
    );
    crate::assert_own_story(spans.iter().map(|s| s.name.as_ref()));
}
