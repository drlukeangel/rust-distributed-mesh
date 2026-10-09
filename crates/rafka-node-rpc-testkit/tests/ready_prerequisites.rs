//! i143.e4.s16 functional: `WaitForNodeReady` begins only over committed
//! runtime prerequisites (PRD §5.3).
//!
//! A real rpc node is created through the process pipeline while the Build
//! state loses one prerequisite's receipt, as an append that failed would.
//! The pipeline refuses Ready naming that step and never asks the node
//! whether it is ready; with every receipt kept, the same pipeline completes.

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published};
use rafka_mesh_entity::{FabricId, IncarnationId, NodeId};
use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt,
    ClaimOutcome, MemoryBuildStateAdapter, StepOutcome,
};
use rafka_node_admin_core::deployment::endpoint::RPC_NODE;
use rafka_node_admin_core::deployment::pipeline::{
    CreateRequest, CreateStep, DeploymentPipeline, NodeObserver, Publication, Timeouts, READY_PREREQUISITES,
};
use rafka_node_admin_core::deployment::process::ProcessDeploymentProvider;
use rafka_node_admin_core::deployment::provider::{DeploymentProvider, TerminationMode};
use rafka_node_admin_core::model::Node;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// The in-memory Build state, except one step's receipts are never kept.
struct Losing {
    inner: MemoryBuildStateAdapter,
    lose: Option<&'static str>,
}

#[async_trait::async_trait]
impl BuildStateAdapter for Losing {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await
    }
    async fn open_attempt(&self, opened: &AttemptOpened) -> Result<(), BuildStateError> {
        self.inner.open_attempt(opened).await
    }
    async fn read_build(&self, build_id: &BuildId) -> Result<BuildProjection, BuildStateError> {
        self.inner.read_build(build_id).await
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
        if self.lose == Some(receipt.step.as_str()) {
            return Ok(());
        }
        self.inner.append_step_receipt(receipt).await
    }
    async fn append_attempt_receipt(&self, receipt: &BuildAttemptReceipt) -> Result<(), BuildStateError> {
        self.inner.append_attempt_receipt(receipt).await
    }
    async fn facts(&self) -> Result<Vec<BuildFact>, BuildStateError> {
        self.inner.facts().await
    }
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }
}

/// The live observer, counting readiness questions.
struct Counting<'a> {
    live: &'a LiveMesh,
    asked_ready: AtomicUsize,
}

#[async_trait::async_trait]
impl NodeObserver for Counting<'_> {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication> {
        self.live.joined(node_id, incarnation).await
    }
    async fn ready(&self, node: &Node) -> Result<(), String> {
        self.asked_ready.fetch_add(1, Ordering::SeqCst);
        self.live.ready(node).await
    }
}

async fn create_losing(lose: Option<&'static str>) {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&fabric, admin.seed.clone(), admin.launcher.clone());
    let inner = MemoryBuildStateAdapter::new();
    let build_id = publish_build(&inner, add_node()).await;
    let builds = Losing { inner, lose };
    let provider = ProcessDeploymentProvider::new();
    let observer = Counting { live: &admin.observer, asked_ready: AtomicUsize::new(0) };
    let sink = Published::default();
    let pipeline = DeploymentPipeline {
        provider: &provider,
        joins: &admin.joins,
        observer: &observer,
        sink: &sink,
        lifecycle: &rafka_node_admin_core::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };
    let req = CreateRequest { build_id: build_id.clone(), attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, restart_of: None, held_runtimes: Vec::new() };
    let result = pipeline.create(&req).await;
    let view = builds.read_build(&build_id).await.unwrap();
    let complete = |s: CreateStep| view.steps.iter().any(|r| r.step == s.name() && r.outcome == StepOutcome::Complete);
    match lose {
        Some(lost) => {
            let err = result.err().expect("Ready over a lost prerequisite receipt");
            assert_eq!(err.step, CreateStep::WaitForNodeReady.name());
            assert!(err.reason.contains("no Complete receipt for") && err.reason.contains(lost), "{}", err.reason);
            assert_eq!(observer.asked_ready.load(Ordering::SeqCst), 0, "WaitForNodeReady never began");
            assert!(!complete(CreateStep::Complete));
            // Every other prerequisite was committed; the birth itself is up.
            for s in READY_PREREQUISITES.iter().filter(|s| s.name() != lost) {
                assert!(complete(*s), "{}", s.name());
            }
        }
        None => {
            let created = result.unwrap_or_else(|e| panic!("{e}"));
            assert!(observer.asked_ready.load(Ordering::SeqCst) > 0);
            // Each prerequisite's receipt precedes WaitForNodeReady's.
            let at = |s: &str| view.steps.iter().position(|r| r.step == s).unwrap_or_else(|| panic!("no receipt for {s}"));
            for s in READY_PREREQUISITES {
                assert!(at(s.name()) < at(CreateStep::WaitForNodeReady.name()), "{} before Ready", s.name());
            }
            provider.terminate(&created.handle, TerminationMode::Graceful { grace: Duration::from_secs(5) }).await.unwrap();
        }
    }
    // Whatever runs for this node stops with the test.
    if let Some(h) = view.steps.iter().find(|r| r.step == CreateStep::DeployRuntime.name()).and_then(|r| r.output.clone()) {
        if let Ok(h) = serde_json::from_value(h) {
            let _ = provider.terminate(&h, TerminationMode::Immediate).await;
        }
    }
    let _ = std::fs::remove_dir_all(&template.data_root);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_waits_on_a_complete_receipt_for_every_runtime_prerequisite() {
    for (i, lost) in READY_PREREQUISITES.iter().enumerate() {
        let _ = i;
        create_losing(Some(lost.name())).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ready_follows_every_runtime_prerequisite_receipt() {
    create_losing(None).await;
}
