//! i143.e2.s5 functional: a create pipeline killed mid-step is re-run as the
//! next attempt of the same Build and completes without duplicating effects:
//! one runtime, the address it reported, every step `Complete` exactly once.
//!
//! The kill drops the pipeline's future at a chosen point (the executor
//! dying), so nothing after that point runs, not even the receipt.

use crate::common;

use common::{add_node, admin_side, publish_build, template, Published};
use rdm_core::{FabricId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt,
    ClaimOutcome, MemoryBuildStateAdapter, StepOutcome,
};
use rdm_admin::deployment::endpoint::RPC_NODE;
use rdm_admin::deployment::pipeline::{CreateRequest, CreateStep, DeploymentPipeline, Timeouts};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{
    DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode,
};
use rdm_admin::model::ProviderKind;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// Where the executor dies.
#[derive(Clone, Copy, PartialEq)]
enum KillAt {
    /// The runtime is spawned; the `DeployRuntime` receipt is never written.
    AfterSpawn,
    /// `WaitForMeshJoin` completed from the birth's report; its receipt is never written.
    InWaitForMeshJoin,
}

/// The real provider, pausing forever right after `spawn` when armed.
struct Killable {
    inner: Arc<ProcessDeploymentProvider>,
    kill_at: Option<KillAt>,
    reached: Arc<Notify>,
    spawned: Mutex<Vec<u32>>,
}

#[async_trait::async_trait]
impl DeploymentProvider for Killable {
    fn kind(&self) -> ProviderKind {
        self.inner.kind()
    }
    fn control_domain(&self) -> String {
        self.inner.control_domain()
    }
    async fn spawn(&self, spec: &ResolvedNodeLaunch) -> Result<DeploymentHandle, DeployError> {
        let h = self.inner.spawn(spec).await?;
        self.spawned.lock().unwrap().push(h.pid.unwrap());
        if self.kill_at == Some(KillAt::AfterSpawn) {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(h)
    }
    async fn terminate(&self, h: &DeploymentHandle, m: TerminationMode) -> Result<(), DeployError> {
        self.inner.terminate(h, m).await
    }
    async fn inspect(&self, h: &DeploymentHandle) -> DeploymentStatus {
        self.inner.inspect(h).await
    }
    async fn signal_stop(&self, h: &DeploymentHandle) -> Result<(), DeployError> {
        self.inner.signal_stop(h).await
    }
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Result<rdm_admin::deployment::provider::Found, rdm_admin::deployment::provider::DeployError> {
        self.inner.find(spec).await
    }
}

/// The in-memory Build state, pausing forever before one step's receipt when armed: the executor
/// dying after the step ran and before its receipt was written.
struct Pausing<'a> {
    inner: &'a MemoryBuildStateAdapter,
    before: Option<&'static str>,
    reached: Arc<Notify>,
}

#[async_trait::async_trait]
impl BuildStateAdapter for Pausing<'_> {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await
    }
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(opened).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
    }
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, BuildStateError> {
        self.inner.newest_accepted_at_ms().await
    }
    async fn list_active(&self) -> Result<Vec<BuildProjection>, BuildStateError> {
        self.inner.list_active().await
    }
    async fn claim_attempt(&self, claim: &BuildAttemptClaim) -> Result<ClaimOutcome, BuildStateError> {
        self.inner.claim_attempt(claim).await
    }
    async fn adopt_claim(&self, claim: &BuildAttemptClaim) -> Result<(), BuildStateError> {
        self.inner.adopt_claim(claim).await
    }
    async fn append_step_receipt(&self, receipt: &BuildStepReceipt) -> Result<(), BuildStateError> {
        if self.before == Some(receipt.step.as_str()) {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.append_step_receipt(receipt).await
    }
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn recovery_of(&self, path: &rdm_admin::model::PathName, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn terminate_failures_of(&self, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Vec<rdm_admin::build_state::FailedTerminate>, BuildStateError> {
        self.inner.terminate_failures_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }
}

/// Live processes whose environment names `node_id`.
fn runtimes_of(node_id: &NodeId) -> Vec<u32> {
    let needle = format!("RDM_NODE_ID={}", node_id.as_str());
    std::fs::read_dir("/proc")
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| {
            std::fs::read(format!("/proc/{pid}/environ"))
                .is_ok_and(|env| env.split(|b| *b == 0).any(|kv| kv == needle.as_bytes()))
        })
        .collect()
}

async fn killed_then_rerun(kill_at: KillAt) {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&admin);
    let builds = MemoryBuildStateAdapter::new();
    let build_id = publish_build(&builds, add_node()).await;
    let sink = Published::default();
    let reached = Arc::new(Notify::new());
    let process = Arc::new(ProcessDeploymentProvider::new());
    let req = |attempt| CreateRequest { build_id: build_id.clone(), attempt, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new() };

    // Attempt 1 dies at the kill point.
    let first = Killable {
        inner: process.clone(),
        kill_at: (kill_at == KillAt::AfterSpawn).then_some(kill_at),
        reached: reached.clone(),
        spawned: Mutex::new(vec![]),
    };
    let paused = Pausing { inner: &builds, before: (kill_at == KillAt::InWaitForMeshJoin).then_some(CreateStep::WaitForMeshJoin.name()), reached: reached.clone() };
    let pipeline = DeploymentPipeline {
        provider: &first,
        joins: &admin.joins,
        observer: &admin.observer,
        sink: &sink,
        lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
        builds: &paused,
        template: &template,
        timeouts: Timeouts::default(),
    };
    let attempt1 = req(1);
    tokio::select! {
        r = pipeline.create(&attempt1) => panic!("attempt 1 was to die mid-step, but finished: {:?}", r.map(|c| c.node.name)),
        _ = reached.notified() => {}
    }
    let first_pid = *first.spawned.lock().unwrap().first().expect("attempt 1 spawned the runtime");
    // Where the first attempt's birth reported it bound, when it got as far as publishing it.
    let reported_before_kill = sink.nodes.lock().unwrap().first().and_then(|n| n.transport_addr);

    // Attempt 2: the real provider and observer, the same Build.
    let second = Killable { inner: process.clone(), kill_at: None, reached, spawned: Mutex::new(vec![]) };
    let pipeline = DeploymentPipeline {
        provider: &second,
        joins: &admin.joins,
        observer: &admin.observer,
        sink: &sink,
        lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };
    let attempt2 = req(2);
    let created = pipeline.create(&attempt2).await.unwrap_or_else(|e| panic!("the re-run failed: {e}"));

    // No duplicated effect: one runtime (the one attempt 1 started), the
    // same endpoints, no extra port held.
    assert!(second.spawned.lock().unwrap().is_empty(), "the re-run spawned a second runtime");
    assert_eq!(created.handle.pid, Some(first_pid));
    assert_eq!(runtimes_of(&created.node.node_id), vec![first_pid], "exactly one runtime runs for the node");
    if let Some(addr) = reported_before_kill {
        assert_eq!(created.node.transport_addr, Some(addr), "the re-run keeps the address the birth reported");
    }

    // Every step Complete exactly once across both attempts; the steps
    // before the kill point keep attempt 1's receipt.
    let view = builds.read_build(&build_id).await.unwrap();
    let split = match kill_at {
        KillAt::AfterSpawn => CreateStep::DeployRuntime,
        KillAt::InWaitForMeshJoin => CreateStep::WaitForMeshJoin,
    };
    let split_at = CreateStep::ORDER.iter().position(|s| *s == split).unwrap();
    for (i, step) in CreateStep::ORDER.iter().enumerate() {
        let done: Vec<u32> = view
            .steps
            .iter()
            .filter(|r| r.step == step.name() && r.outcome == StepOutcome::Complete)
            .map(|r| r.attempt)
            .collect();
        let want = if i < split_at { 1 } else { 2 };
        assert_eq!(done, vec![want], "{}: Complete receipts by attempt", step.name());
    }

    process.terminate(&created.handle, TerminationMode::Graceful { grace: Duration::from_secs(5) }).await.unwrap();
    let _ = std::fs::remove_dir_all(&template.data_root);
}

#[test]
fn a_pipeline_killed_after_spawning_reruns_without_a_second_runtime() {
    let cap = crate::common::capture("a_pipeline_killed_after_spawning_reruns_without_a_second_runtime");
    cap.run(crate::common::Threads::Workers(4), async move {
        killed_then_rerun(KillAt::AfterSpawn).await;
    })
}

#[test]
fn a_pipeline_killed_while_waiting_for_the_join_reruns_from_that_step() {
    let cap = crate::common::capture("a_pipeline_killed_while_waiting_for_the_join_reruns_from_that_step");
    cap.run(crate::common::Threads::Workers(4), async move {
        killed_then_rerun(KillAt::InWaitForMeshJoin).await;
    })
}
