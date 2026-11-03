//! Functional: a deployment pipeline step's span says where its blocking disk time went. A step whose
//! work runs blocking calls on tokio's blocking pool (`blocking::blocking_io`: a key minted, a
//! record written and read back) names on its own `rdm.node_admin.deployment.update.via-step` span
//! how many it ran (`io_calls`), how long they waited for a blocking thread (`io_queued_ms`) and how
//! long they ran on it (`io_ms`). A step that took seconds says which of the two it was, never only
//! its `elapsed_ms` (CLAUDE.md "a bound that fires is evidence: read why the operation took that
//! long"). A step with no blocking call records none of the three.

use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rdm_admin::deployment::endpoint::NODE_SPEC;
use rdm_admin::deployment::pipeline::{CommandAdmission, CommandContext, Completion, CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink};
use rdm_admin::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rdm_admin::model::{IncarnationId, Node, NodeId, PathName, ProviderKind};
use rdm_admin::node_commands::NodeCommand;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;

/// A provider that launches nothing: the create stops at `DeployRuntime`, after the steps under test.
struct LaunchesNothing;
#[async_trait::async_trait]
impl DeploymentProvider for LaunchesNothing {
    fn kind(&self) -> ProviderKind {
        ProviderKind::Process
    }
    fn control_domain(&self) -> String {
        "pipeline-step-io".into()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch, _deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { path: spec.path.clone(), name: spec.name.clone(), reason: "this cell launches nothing".into() })
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

struct NoBirth;
#[async_trait::async_trait]
impl NodeObserver for NoBirth {
    async fn send_command(&self, _: &Node, _: NodeCommand, _: &CommandContext) -> CommandAdmission {
        CommandAdmission::Admitted {}
    }
    async fn await_completion(&self, _: &Node, _: NodeCommand, _: &CommandContext, _: Duration) -> Completion {
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

/// How long the cell holds the runtime's one blocking thread once the create is under way.
const HELD: Duration = Duration::from_millis(400);

/// CONTRACT: a create's `PrepareStorage` step mints the birth's transport key on the blocking pool;
/// while the pool's one thread is held for 400 ms by other work, its span names one blocking call
/// (`io_calls = 1`) that waited for a thread at least 200 ms (`io_queued_ms`, the hold less what ran
/// before the step) and the time it ran on it (`io_ms`), beside its `elapsed_ms`.
/// `MakeRuntimeFactAvailableToBirth` is never reached (the provider launches nothing), and
/// `AllocateIdentity` and `PrepareNetwork`, which run no blocking call, record no `io_*` field. Must
/// NOT happen: a step that waited seconds for a blocking thread reporting only its `elapsed_ms`, so
/// its trace cannot say whether the pool or the disk took the time.
#[test]
fn a_step_span_names_the_time_its_blocking_calls_waited_for_a_thread_apart_from_their_work() {
    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("pipeline-step-io", exporter.clone());
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let data_root = std::env::temp_dir().join(format!("pipeline-step-io-{}", NodeId::mint()));
    let rt = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap();
    let err = rt.block_on(async {
        let executor = rdm_core::framing::Birth { node_id: NodeId::mint(), incarnation: IncarnationId::mint() };
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: None, source: None, time: 0 })
            .await
            .unwrap();
        builds.claim_attempt(&BuildAttemptClaim { claimed_mesh: None, executor_mesh: "mesh1".into(), claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: executor.clone() }).await.unwrap();
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: rdm_core::FabricId::mint(),
            executable: "/nonexistent/pipeline-step-io".into(),
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
        let joins = rdm_admin::join::Joins::default();
        let pipeline = DeploymentPipeline { provider: &LaunchesNothing, joins: &joins, observer: &NoBirth, sink: &Discard, lifecycle: &NoLifecycleEvents, builds: &builds, template: &template, timeouts: Timeouts::default() };
        // The runtime's one blocking thread is held by other work, released `HELD` after the create began.
        let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = tokio::task::spawn_blocking(move || {
            let _ = held_tx.send(());
            let _ = release_rx.recv();
        });
        held_rx.await.unwrap();
        let release = tokio::spawn(async move {
            tokio::time::sleep(HELD).await;
            release_tx.send(()).unwrap();
        });
        let req = CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "3").unwrap()))), node_id: (rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "3").unwrap()))).clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build_id.clone()), attempt: 1, path: "mesh1.node".into(), name: "3".into(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None };
        let err = pipeline.create(&req).await.map(|_| ()).expect_err("the provider launches nothing: the create fails at DeployRuntime");
        release.await.unwrap();
        holder.await.unwrap();
        err
    });
    let _ = std::fs::remove_dir_all(&data_root);
    assert_eq!(err.step, "DeployRuntime", "{err:?}");

    capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
    let steps: Vec<(String, String, std::collections::BTreeMap<String, String>)> = exporter
        .get_finished_spans()
        .unwrap()
        .into_iter()
        .filter(|s| s.name == "rdm.node_admin.deployment.update.via-step")
        .map(|s| {
            let attrs: std::collections::BTreeMap<String, String> = s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect();
            (attrs.get("step").cloned().unwrap_or_default(), s.span_context.trace_id().to_string(), attrs)
        })
        .collect();
    let step = |name: &str| steps.iter().find(|(s, _, _)| s == name).unwrap_or_else(|| panic!("no {name} step span: {steps:?}"));
    let (_, trace_id, storage) = step("PrepareStorage");
    let ms = |attrs: &std::collections::BTreeMap<String, String>, f: &str| attrs.get(f).and_then(|v| v.parse::<u64>().ok()).unwrap_or_else(|| panic!("{f} is not recorded: {attrs:?}"));
    assert_eq!(ms(storage, "io_calls"), 1, "the key mint is one blocking call: {storage:?}");
    assert!(ms(storage, "io_queued_ms") >= 200, "the call waited for the held thread: {storage:?}");
    let _ = ms(storage, "io_ms");
    for none in ["AllocateIdentity", "PrepareNetwork"] {
        let (_, _, attrs) = step(none);
        assert!(!attrs.contains_key("io_calls") && !attrs.contains_key("io_queued_ms") && !attrs.contains_key("io_ms"), "{none} runs no blocking call: {attrs:?}");
    }
    eprintln!(
        "trace {trace_id}: rdm.node_admin.deployment.update.via-step step=PrepareStorage elapsed_ms={} io_calls={} io_queued_ms={} io_ms={}",
        storage.get("elapsed_ms").cloned().unwrap_or_default(),
        storage["io_calls"],
        storage["io_queued_ms"],
        storage["io_ms"]
    );
}

struct WithRows(std::sync::Arc<rdm_admin::node_rows::NodeRows>);
impl TopologySink for WithRows {
    fn publish(&self, _: Node) {}
    fn remove(&self, _: &PathName) {}
    fn remove_birth(&self, _: &PathName, _: &NodeId, _: &IncarnationId) {}
    fn node_rows(&self) -> Option<std::sync::Arc<rdm_admin::node_rows::NodeRows>> {
        Some(self.0.clone())
    }
}

/// CONTRACT (e12.s83): `PrepareStorage` names every await it holds (`awaiting` while in flight,
/// `awaited` as each completes), and `NodeRows::claim` opens `rdm.node_admin.node.create.via-node-row`
/// (`moment = "create"`) before its first await (`writer-check`). When `PrepareStorage` is cancelled
/// while awaiting the claim's first await (`writer-check`, with the view write lock held), both spans
/// close with `outcome = "cancelled"` naming the exact await that did not return (`awaiting = "claim"`
/// on `via-step`, `awaiting = "writer-check"` on `via-node-row`); when cancelled while awaiting the
/// blocking pool (`transport-key`), `via-step` names `awaited = "claim"`, `awaiting = "transport-key"`,
/// `outcome = "cancelled"` and records `io_calls = 1` and `io_queued_ms` for the in-flight blocking call.
#[test]
fn prepare_storage_names_every_await_and_opens_the_node_row_span_before_its_first_await() {
    use rdm_admin::model::{Fabric, Mesh, NodeStatus, ScopeStatus};
    use rdm_admin::topology::Topology;
    use std::sync::Arc;
    use tokio::sync::RwLock;

    let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
    let (capture, layer) = rdm_transport::span_capture("pipeline-step-io-s83", exporter.clone());
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let data_root = std::env::temp_dir().join(format!("pipeline-step-io-s83-{}", NodeId::mint()));
    let rt = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap();
    rt.block_on(async {
        let me: PathName = rdm_core::PathName::new("mesh1.node-admin", "1").unwrap();
        let mut admin = Node::allocated(me.clone(), "mesh1", "node-admin");
        admin.status = NodeStatus::Ready;
        admin.is_primary = true;
        admin.is_fabric_primary = true;
        admin.incarnation_id = Some(IncarnationId::mint());
        let me_id = admin.node_id.clone();
        let executor = rdm_core::framing::Birth { node_id: me_id.clone(), incarnation: admin.incarnation_id.clone().unwrap() };
        let view = Arc::new(RwLock::new(Topology {
            fabric: Fabric { id: rdm_core::FabricId::mint(), name: "fabric1".into(), status: ScopeStatus::Ready, provider: ProviderKind::Process },
            meshes: vec![Mesh { id: Some(rdm_core::MeshId::mint()), name: "mesh1".into(), status: ScopeStatus::Ready }],
            nodes: vec![admin],
        }));
        let rows = Arc::new(rdm_admin::node_rows::NodeRows::new(
            me.clone(),
            me_id.clone(),
            view.clone(),
            Arc::new(rdm_admin::storage::MemoryNodesStorage::default()),
        ));
        let sink = WithRows(rows);
        let template = LaunchTemplate { fabric: "fabric1".into(),
            fabric_id: rdm_core::FabricId::mint(),
            executable: "/nonexistent/pipeline-step-io-s83".into(),
            seeds: vec![],
            launcher: rdm_core::launch::Launcher { mesh: "mesh1".into(), path: me.path.clone(), name: me.name.clone(), node_id: me_id, incarnation: executor.incarnation.clone() },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            issuing: None,
            roots: Default::default(),
            connections_delete_deadline: rdm_core::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
            declared: Default::default(),
            binding: None,
        };
        let joins = rdm_admin::join::Joins::default();

        // 1. Hold the view write lock so the claim's very first await (`writer_check` -> `view.read().await`) blocks.
        let builds1 = MemoryBuildStateAdapter::new();
        let build1 = BuildId::mint();
        builds1
            .publish_accepted(&BuildAccepted { build_id: build1.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: None, source: None, time: 0 })
            .await
            .unwrap();
        builds1.claim_attempt(&BuildAttemptClaim { claimed_mesh: None, executor_mesh: "mesh1".into(), claimant: None, mesh: None, build_id: build1.clone(), attempt: 1, executor: executor.clone() }).await.unwrap();
        let pipeline1 = DeploymentPipeline { provider: &LaunchesNothing, joins: &joins, observer: &NoBirth, sink: &sink, lifecycle: &NoLifecycleEvents, builds: &builds1, template: &template, timeouts: Timeouts::default() };
        let req1 = CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "3").unwrap()))), node_id: (rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "3").unwrap()))).clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build1.clone()), attempt: 1, path: "mesh1.node".into(), name: "3".into(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None };
        // Pre-seed AllocateIdentity so `create` enters `PrepareStorage` without calling `row_allocation`.
        builds1
            .append_step_receipt(&rdm_admin::build_state::BuildStepReceipt {
                mesh: None,
                build_id: build1.clone(),
                attempt: 1,
                operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "3").unwrap())),
                step: "AllocateIdentity".into(),
                outcome: rdm_admin::build_state::StepOutcome::Complete,
                output: Some(serde_json::json!({
                    "node_id": NodeId::mint(),
                    "incarnation": IncarnationId::mint(),
                    "supersedes": null,
                    "deployment_id": rdm_core::DeploymentId::mint(),
                })),
                executor: Some(executor.clone()),
            })
            .await
            .unwrap();
        let held_view = view.write().await;
        let timed_out = tokio::time::timeout(Duration::from_millis(80), pipeline1.create(&req1)).await;
        assert!(timed_out.is_err(), "the create waits at the claim's first await while the view write lock is held");
        drop(held_view);

        // 2. Let the claim complete, and hold the one blocking thread so `PrepareStorage` cancels at `transport-key`.
        let builds2 = MemoryBuildStateAdapter::new();
        let build2 = BuildId::mint();
        builds2
            .publish_accepted(&BuildAccepted { build_id: build2.clone(), topology: rdm_admin::accepted::FabricTopology::root("fabric1", "mesh1", rdm_admin::model::MeshId::parse("meshd0000001").unwrap()), change: None, source: None, time: 0 })
            .await
            .unwrap();
        builds2.claim_attempt(&BuildAttemptClaim { claimed_mesh: None, executor_mesh: "mesh1".into(), claimant: None, mesh: None, build_id: build2.clone(), attempt: 1, executor: executor.clone() }).await.unwrap();
        let pipeline2 = DeploymentPipeline { provider: &LaunchesNothing, joins: &joins, observer: &NoBirth, sink: &sink, lifecycle: &NoLifecycleEvents, builds: &builds2, template: &template, timeouts: Timeouts::default() };
        let (held_tx, held_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let holder = tokio::task::spawn_blocking(move || {
            let _ = held_tx.send(());
            let _ = release_rx.recv();
        });
        held_rx.await.unwrap();
        let req2 = CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "4").unwrap()))), node_id: (rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "4").unwrap()))).clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build2.clone()), attempt: 1, path: "mesh1.node".into(), name: "4".into(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None };
        let timed_out2 = tokio::time::timeout(Duration::from_millis(120), pipeline2.create(&req2)).await;
        assert!(timed_out2.is_err(), "the create waits at transport-key while the blocking thread is held");
        release_tx.send(()).unwrap();
        holder.await.unwrap();
    });
    let _ = std::fs::remove_dir_all(&data_root);

    capture.flush(Duration::from_secs(10)).unwrap_or_else(|e| panic!("the cell's spans did not all reach its exporter: {e}"));
    let finished = exporter.get_finished_spans().unwrap();
    let attrs_of = |s: &opentelemetry_sdk::trace::SpanData| -> std::collections::BTreeMap<String, String> {
        s.attributes.iter().map(|kv| (kv.key.to_string(), kv.value.to_string())).collect()
    };

    // Case 1 (`mesh1.node.3`): cancelled at `claim` -> `writer-check`.
    let step1 = finished
        .iter()
        .map(attrs_of)
        .find(|a| a.get("step").map(String::as_str) == Some("PrepareStorage") && a.get("node_path").map(String::as_str) == Some("mesh1.node") && a.get("node_name").map(String::as_str) == Some("3"))
        .unwrap_or_else(|| panic!("no PrepareStorage span for mesh1.node.3: {finished:?}"));
    assert_eq!(step1.get("outcome").map(String::as_str), Some("cancelled"), "cancelled PrepareStorage records outcome=cancelled: {step1:?}");
    assert_eq!(step1.get("awaiting").map(String::as_str), Some("create-row"), "PrepareStorage names the claim await it was holding: {step1:?}");
    assert!(step1.contains_key("elapsed_ms"), "cancelled PrepareStorage records elapsed_ms: {step1:?}");

    let claim1 = finished
        .iter()
        .filter(|s| s.name == "rdm.node_admin.node.create.via-node-row")
        .map(attrs_of)
        .find(|a| a.get("subject_path").map(String::as_str) == Some("mesh1.node") && a.get("subject_name").map(String::as_str) == Some("3") && a.get("moment").map(String::as_str) == Some("create"))
        .unwrap_or_else(|| panic!("the claim via-node-row span must open before writer_check: {finished:?}"));
    assert_eq!(claim1.get("outcome").map(String::as_str), Some("cancelled"), "cancelled claim span records outcome=cancelled: {claim1:?}");
    assert_eq!(claim1.get("awaiting").map(String::as_str), Some("writer-check"), "claim span names writer-check as the await that did not return: {claim1:?}");

    // Case 2 (`mesh1.node.4`): claim completed, cancelled at `transport-key` while waiting for the blocking thread.
    let step2 = finished
        .iter()
        .map(attrs_of)
        .find(|a| a.get("step").map(String::as_str) == Some("PrepareStorage") && a.get("node_path").map(String::as_str) == Some("mesh1.node") && a.get("node_name").map(String::as_str) == Some("4"))
        .unwrap_or_else(|| panic!("no PrepareStorage span for mesh1.node.4: {finished:?}"));
    assert_eq!(step2.get("outcome").map(String::as_str), Some("cancelled"), "cancelled PrepareStorage records outcome=cancelled: {step2:?}");
    assert_eq!(step2.get("awaited").map(String::as_str), Some("entry-spec,create-row"), "PrepareStorage names the completed entry-spec read and claim awaits: {step2:?}");
    assert_eq!(step2.get("awaiting").map(String::as_str), Some("transport-key"), "PrepareStorage names transport-key as the await in flight: {step2:?}");
    assert_eq!(step2.get("io_calls").and_then(|v| v.parse::<u64>().ok()), Some(1), "in-flight blocking call is counted on cancellation: {step2:?}");
    assert!(step2.get("io_queued_ms").and_then(|v| v.parse::<u64>().ok()).unwrap_or(0) >= 50, "in-flight queue wait is recorded on cancellation: {step2:?}");

    let claim2 = finished
        .iter()
        .filter(|s| s.name == "rdm.node_admin.node.create.via-node-row")
        .map(attrs_of)
        .find(|a| a.get("subject_path").map(String::as_str) == Some("mesh1.node") && a.get("subject_name").map(String::as_str) == Some("4") && a.get("moment").map(String::as_str) == Some("create"))
        .unwrap_or_else(|| panic!("no claim span for mesh1.node.4: {finished:?}"));
    assert_eq!(claim2.get("outcome").map(String::as_str), Some("row-written"), "{claim2:?}");
    assert_eq!(claim2.get("awaited").map(String::as_str), Some("writer-check,read-previous-row,put-node-row"), "{claim2:?}");
    assert_eq!(claim2.get("awaiting").map(String::as_str), Some("done"), "{claim2:?}");
}

