//! i143.e2.s5 functional: a create pipeline killed mid-step is re-run as the
//! next attempt of the same Build and completes without duplicating effects:
//! one runtime, the same endpoints, every step `Complete` exactly once.
//!
//! The kill drops the pipeline's future at a chosen point (the executor
//! dying), so nothing after that point runs, not even the receipt.

mod common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published};
use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_admin_core::build_state::{BuildStateAdapter, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::deployment::endpoint::{EndpointAllocator, RPC_NODE_SLOTS};
use rafka_node_admin_core::deployment::pipeline::{CreateRequest, CreateStep, DeploymentPipeline, NodeObserver, Timeouts};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{
    DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode,
};
use rafka_node_admin_core::model::{Node, ProviderKind};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// Where the executor dies.
#[derive(Clone, Copy, PartialEq)]
enum KillAt {
    /// The runtime is spawned; the `DeployRuntime` receipt is never written.
    AfterSpawn,
    /// Waiting for the node to join membership.
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
    async fn find(&self, spec: &ResolvedNodeLaunch) -> Option<DeploymentHandle> {
        self.inner.find(spec).await
    }
}

/// The live observer, pausing forever in `joined` when armed.
struct Pausing<'a> {
    live: &'a LiveMesh,
    pause: bool,
    reached: Arc<Notify>,
}

#[async_trait::async_trait]
impl NodeObserver for Pausing<'_> {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<rafka_node_admin_core::deployment::pipeline::Publication> {
        if self.pause {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        self.live.joined(node_id, incarnation).await
    }
    async fn ready(&self, node: &Node) -> Result<(), String> {
        self.live.ready(node).await
    }
    async fn drained(&self, node: &Node) -> bool {
        self.live.drained(node).await
    }
    async fn admission_closed(&self, node: &Node) -> Result<(), String> {
        self.live.admission_closed(node).await
    }
}

/// Live processes whose environment names `node_id`.
fn runtimes_of(node_id: &NodeId) -> Vec<u32> {
    let needle = format!("RAFKA_NODE_ID={}", node_id.as_str());
    std::fs::read_dir("/proc")
        .unwrap()
        .filter_map(|e| e.ok()?.file_name().to_str()?.parse::<u32>().ok())
        .filter(|pid| {
            std::fs::read(format!("/proc/{pid}/environ"))
                .is_ok_and(|env| env.split(|b| *b == 0).any(|kv| kv == needle.as_bytes()))
        })
        .collect()
}

async fn killed_then_rerun(kill_at: KillAt, ports: (u16, u16)) {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone());
    let builds = MemoryBuildStateAdapter::new();
    let build_id = publish_build(&builds, add_node()).await;
    let allocator = Mutex::new(EndpointAllocator::new(IpAddr::from([127, 0, 0, 1]), ports.0, ports.1));
    let sink = Published::default();
    let reached = Arc::new(Notify::new());
    let process = Arc::new(ProcessDeploymentProvider::new());
    let req = |attempt| CreateRequest { build_id: build_id.clone(), attempt, node: "mesh1.rpc.1".parse().unwrap(), slots: RPC_NODE_SLOTS, restart_of: None };

    // Attempt 1 dies at the kill point.
    let first = Killable {
        inner: process.clone(),
        kill_at: (kill_at == KillAt::AfterSpawn).then_some(kill_at),
        reached: reached.clone(),
        spawned: Mutex::new(vec![]),
    };
    let observer = Pausing { live: &admin.observer, pause: kill_at == KillAt::InWaitForMeshJoin, reached: reached.clone() };
    let pipeline = DeploymentPipeline {
        provider: &first,
        allocator: &allocator,
        observer: &observer,
        sink: &sink,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };
    let attempt1 = req(1);
    tokio::select! {
        r = pipeline.create(&attempt1) => panic!("attempt 1 was to die mid-step, but finished: {:?}", r.map(|c| c.node.name)),
        _ = reached.notified() => {}
    }
    let first_pid = *first.spawned.lock().unwrap().first().expect("attempt 1 spawned the runtime");
    let held_after_kill = allocator.lock().unwrap().held(&"mesh1.rpc.1".parse().unwrap()).unwrap().to_vec();

    // Attempt 2: the real provider and observer, the same Build.
    let second = Killable { inner: process.clone(), kill_at: None, reached, spawned: Mutex::new(vec![]) };
    let pipeline = DeploymentPipeline {
        provider: &second,
        allocator: &allocator,
        observer: &admin.observer,
        sink: &sink,
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
    assert_eq!(created.node.endpoints, held_after_kill);
    assert_eq!(allocator.lock().unwrap().in_use_count(), RPC_NODE_SLOTS.len(), "no port taken twice");

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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pipeline_killed_after_spawning_reruns_without_a_second_runtime() {
    killed_then_rerun(KillAt::AfterSpawn, (58400, 58499)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pipeline_killed_while_waiting_for_the_join_reruns_from_that_step() {
    killed_then_rerun(KillAt::InWaitForMeshJoin, (58500, 58599)).await;
}
