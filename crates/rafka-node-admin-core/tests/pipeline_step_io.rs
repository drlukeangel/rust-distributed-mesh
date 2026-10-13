//! Functional: a deployment pipeline step's span says where its blocking disk time went. A step whose
//! work runs blocking calls on tokio's blocking pool (`record_store::blocking_io`: a key minted, a
//! record written and read back) names on its own `rdm.node_admin.deployment.update.via-step` span
//! how many it ran (`io_calls`), how long they waited for a blocking thread (`io_queued_ms`) and how
//! long they ran on it (`io_ms`). A step that took seconds says which of the two it was, never only
//! its `elapsed_ms` (CLAUDE.md "a bound that fires is evidence: read why the operation took that
//! long"). A step with no blocking call records none of the three.

use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{BuildAccepted, BuildAttemptClaim, BuildStateAdapter, MemoryBuildStateAdapter};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{CommandAdmission, CommandContext, Completion, CreateRequest, DeploymentPipeline, LaunchTemplate, NoLifecycleEvents, NodeObserver, Publication, Timeouts, TopologySink};
use rafka_node_admin_core::deployment::provider::{DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode};
use rafka_node_admin_core::model::{IncarnationId, Node, NodeId, PathName, ProviderKind};
use rafka_node_admin_core::node_commands::NodeCommand;
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
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        Err(DeployError::Spawn { node: spec.node.to_string(), reason: "this cell launches nothing".into() })
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
    async fn find(&self, _: &ResolvedNodeLaunch) -> Result<rafka_node_admin_core::deployment::provider::Found, DeployError> {
        Ok(rafka_node_admin_core::deployment::provider::Found::NeverLaunched)
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
    let exporter = opentelemetry_sdk::testing::trace::InMemorySpanExporter::default();
    let (capture, layer) = rafka_mesh_telemetry::span_capture("pipeline-step-io", exporter.clone());
    let dispatch = tracing::Dispatch::new(tracing_subscriber::registry().with(layer));
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let data_root = std::env::temp_dir().join(format!("pipeline-step-io-{}", NodeId::mint()));
    let rt = tokio::runtime::Builder::new_current_thread().max_blocking_threads(1).enable_all().build().unwrap();
    let err = rt.block_on(async {
        let builds = MemoryBuildStateAdapter::new();
        let build_id = BuildId::mint();
        builds
            .publish_accepted(&BuildAccepted { build_id: build_id.clone(), topology: rafka_node_admin_core::accepted::FabricTopology::root("fabric1", "mesh1", rafka_node_admin_core::model::MeshId::parse("meshd0000001").unwrap()), submitted_change: None, submitted_at_ms: 0 })
            .await
            .unwrap();
        builds.claim_attempt(&BuildAttemptClaim { claimant: None, mesh: None, build_id: build_id.clone(), attempt: 1, executor: "mesh1.admin.1".into() }).await.unwrap();
        let template = LaunchTemplate {
            fabric: "fabric1".into(),
            fabric_id: rafka_mesh_entity::FabricId::mint(),
            executable: "/nonexistent/pipeline-step-io".into(),
            seeds: vec![],
            launcher: rafka_mesh_entity::launch::Launcher { name: "mesh1.admin.1".parse().unwrap(), node_id: NodeId::mint(), incarnation: IncarnationId::mint() },
            env: Default::default(),
            data_root: data_root.clone(),
            store_dir: data_root.clone(),
            mesh_issuer: None,
            connections_delete_deadline: rafka_mesh_entity::launch::DEFAULT_CONNECTIONS_DELETE_DEADLINE,
        };
        let joins = rafka_node_admin_core::join::Joins::default();
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
        let req = CreateRequest { build_id: build_id.clone(), attempt: 1, node: "mesh1.rpc.3".parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() };
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
