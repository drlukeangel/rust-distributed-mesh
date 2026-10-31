//! i143.e4.s16 functional: `WaitForNodeReady` begins only over committed
//! runtime prerequisites (PRD §5.3).
//!
//! A real rpc node is created through the process pipeline while the Build
//! state loses one prerequisite's receipt, as an append that failed would.
//! The pipeline refuses Ready naming that step and never asks the node
//! whether it is ready; with every receipt kept, the same pipeline completes.

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published};
use rdm_core::{FabricId, IncarnationId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{
    AttemptOpened, BuildAccepted, BuildAttemptClaim, BuildAttemptReceipt, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, BuildStepReceipt,
    ClaimOutcome, MemoryBuildStateAdapter, StepOutcome,
};
use rdm_admin::deployment::endpoint::RPC_NODE;
use rdm_admin::deployment::pipeline::{
    CreateRequest, CreateStep, DeploymentPipeline, NodeObserver, Publication, Timeouts, READY_PREREQUISITES,
};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{DeploymentProvider, TerminationMode};
use rdm_admin::model::Node;
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
    async fn current_build(&self) -> Result<rdm_admin::build_state::CurrentBuild, rdm_admin::build_state::BuildStateError> {
        self.inner.current_build().await
    }
    async fn newest_accepted_at_ms(&self) -> Result<Option<u64>, rdm_admin::build_state::BuildStateError> {
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
    async fn committed_facts(&self, build: Option<&rdm_admin::build::BuildId>) -> Result<Vec<rdm_admin::build_state::BuildFact>, rdm_admin::build_state::BuildStateError> {
        self.inner.committed_facts(build).await
    }
    async fn recovery_of(&self, path: &rdm_admin::model::PathName, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(rdm_admin::build::BuildId, Option<String>, u32, String)>, BuildStateError> {
        self.inner.recovery_of(path, node_id, incarnation).await
    }
    async fn mesh_deleted_of(&self, node_id: &rdm_admin::model::NodeId, incarnation: &rdm_admin::model::IncarnationId) -> Result<Option<(rdm_admin::build::BuildId, u32, String)>, BuildStateError> {
        self.inner.mesh_deleted_of(node_id, incarnation).await
    }
    async fn exits_owed(&self, build_id: &rdm_admin::build::BuildId, mesh: &str) -> Result<Vec<rdm_admin::build_state::OwedExit>, BuildStateError> {
        self.inner.exits_owed(build_id, mesh).await
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

/// Create toward one rpc node with `lose`'s receipts never kept; returns the pid of the runtime it launched.
async fn create_losing(lose: Option<&'static str>) -> u32 {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&admin);
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
        lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
        builds: &builds,
        template: &template,
        timeouts: Timeouts::default(),
    };
    let req = CreateRequest { build_id: build_id.clone(), attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None };
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
            provider.terminate(&created.handle, TerminationMode::Graceful { grace: Duration::from_secs(5) }, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
        }
    }
    // Whatever runs for this node stops with the test, by signal: it deletes its connections, stops and exports its
    // queued spans before it exits.
    let handle: rdm_admin::deployment::provider::DeploymentHandle = view
        .steps
        .iter()
        .find(|r| r.step == CreateStep::DeployRuntime.name())
        .and_then(|r| r.output.clone())
        .and_then(|h| serde_json::from_value(h).ok())
        .expect("the runtime was deployed and its handle receipted");
    provider.terminate(&handle, TerminationMode::Graceful { grace: Duration::from_secs(5) }, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
    let _ = std::fs::remove_dir_all(&template.data_root);
    handle.pid.expect("a process handle carries its pid")
}

#[test]
fn ready_waits_on_a_complete_receipt_for_every_runtime_prerequisite() {
    let cap = crate::common::capture("ready_waits_on_a_complete_receipt_for_every_runtime_prerequisite");
    let pids = cap.run(crate::common::Threads::Workers(4), async move {
        let mut pids = Vec::new();
        for lost in READY_PREREQUISITES {
            pids.push(create_losing(Some(lost.name())).await);
        }
        pids
    });
    for pid in pids {
        crate::common::assert_launched_node_stopped_with_its_spans(&cap.dir, pid);
    }
}

#[test]
fn ready_follows_every_runtime_prerequisite_receipt() {
    let cap = crate::common::capture("ready_follows_every_runtime_prerequisite_receipt");
    let pid = cap.run(crate::common::Threads::Workers(4), async move { create_losing(None).await });
    crate::common::assert_launched_node_stopped_with_its_spans(&cap.dir, pid);
}

/// The live observer, except the birth's runtime exits the moment readiness is first asked, as a
/// node that joined and then failed to start does.
struct ExitsWhenAsked<'a> {
    live: &'a LiveMesh,
    provider: &'a ProcessDeploymentProvider,
    builds: &'a MemoryBuildStateAdapter,
    build_id: BuildId,
}

#[async_trait::async_trait]
impl NodeObserver for ExitsWhenAsked<'_> {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication> {
        self.live.joined(node_id, incarnation).await
    }
    async fn ready(&self, _node: &Node) -> Result<(), String> {
        let view = self.builds.read_build(&self.build_id).await.unwrap();
        let handle = view.steps.iter().find(|r| r.step == CreateStep::DeployRuntime.name()).and_then(|r| r.output.clone()).expect("the runtime was deployed");
        self.provider.terminate(&serde_json::from_value(handle).unwrap(), TerminationMode::Immediate, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
        Err("not Ready: the node does not answer".to_string())
    }
}

/// CONTRACT: a create whose runtime has exited is refused at `WaitForNodeReady` naming the exit,
/// as soon as the provider proves it; it does not wait out the readiness deadline.
#[test]
fn ready_refuses_a_runtime_that_exited_without_waiting_out_the_deadline() {
    let cap = crate::common::capture("ready_refuses_a_runtime_that_exited_without_waiting_out_the_deadline");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
        let template = template(&admin);
        let builds = MemoryBuildStateAdapter::new();
        let build_id = publish_build(&builds, add_node()).await;
        let provider = ProcessDeploymentProvider::new();
        let observer = ExitsWhenAsked { live: &admin.observer, provider: &provider, builds: &builds, build_id: build_id.clone() };
        let sink = Published::default();
        let pipeline = DeploymentPipeline {
            provider: &provider,
            joins: &admin.joins,
            observer: &observer,
            sink: &sink,
            lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts { ready: Duration::from_secs(120), ..Timeouts::default() },
        };
        let req = CreateRequest { build_id, attempt: 1, node: "mesh1.rpc.1".parse().unwrap(), spec: &RPC_NODE, data_dir: None, held_runtimes: Vec::new(), entry: None };
        let asked = std::time::Instant::now();
        let err = pipeline.create(&req).await.err().expect("a create over an exited runtime");
        assert!(asked.elapsed() < Duration::from_secs(60), "the create waited {:?} on a runtime already proven exited", asked.elapsed());
        assert_eq!(err.step, CreateStep::WaitForNodeReady.name());
        assert!(err.reason.contains("runtime exited") && err.reason.contains("before it was Ready"), "{}", err.reason);
        let _ = std::fs::remove_dir_all(&template.data_root);
    })
}
