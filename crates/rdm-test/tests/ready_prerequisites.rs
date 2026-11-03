//! i143.e4.s16 functional: `WaitForNodeReady` begins only over committed
//! runtime prerequisites (PRD §5.3).
//!
//! A real node is created through the process pipeline while the Build
//! state loses one prerequisite's receipt, as an append that failed would.
//! The pipeline refuses Ready naming that step and never asks the node
//! whether it is ready; with every receipt kept, the same pipeline completes.

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published};
use rdm_core::{FabricId, IncarnationId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::deployment::endpoint::NODE_SPEC;
use rdm_admin::deployment::pipeline::{
    CreateRequest, CreateStep, DeploymentPipeline, NodeObserver, Publication, Timeouts, READY_PREREQUISITES,
};
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{DeploymentProvider, TerminationMode};
use rdm_admin::model::Node;
use std::net::IpAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

/// A run's step log over memory, except one step's `rdm.build.step` event is never written.
struct Losing {
    inner: rdm_admin::build_run::FramedBuilds,
    lose: Option<&'static str>,
}

#[async_trait::async_trait]
impl BuildStateAdapter for Losing {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await
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
    async fn forget(&self, build_id: &BuildId) -> Result<(), BuildStateError> {
        self.inner.forget(build_id).await
    }
    async fn accepted_newest_first(&self) -> Result<Vec<rdm_admin::build::BuildId>, rdm_admin::build_state::BuildStateError> {
        self.inner.accepted_newest_first().await
    }
    fn events(&self) -> std::sync::Arc<dyn rdm_admin::events_storage::EventsStorage> {
        self.inner.events()
    }
    async fn write_step(&self, step: &rdm_admin::build_state::StepWrite) -> Result<(), BuildStateError> {
        if self.lose == Some(step.step.as_str()) {
            return Ok(());
        }
        self.inner.write_step(step).await
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

/// Create toward one node with `lose`'s step event never written; returns the pid of the runtime it launched.
async fn create_losing(lose: Option<&'static str>) -> u32 {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&admin);
    let inner = common::stepping(&fabric, "mesh1");
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
    let req = CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "1").unwrap()))), node_id: (rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "1").unwrap()))).clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build_id.clone()), path: "mesh1.node".into(), name: "1".into(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None };
    let result = pipeline.create(&req).await;
    let view = builds.read_build(&build_id).await.unwrap();
    let complete = |s: CreateStep| view.steps.iter().any(|r| r.step == s.name() && r.outcome == StepOutcome::Complete);
    match lose {
        Some(lost) => {
            let err = result.err().expect("Ready over a lost prerequisite receipt");
            assert_eq!(err.step, CreateStep::WaitForNodeReady.name());
            assert!(err.reason.contains("no step event ended complete for") && err.reason.contains(lost), "{}", err.reason);
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
    assert!(complete(CreateStep::DeployRuntime), "the runtime was deployed");
    let handle = common::launched_handle(&template.data_root, &req.node_id);
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
    builds: &'a rdm_admin::build_run::FramedBuilds,
    build_id: BuildId,
    data_root: std::path::PathBuf,
    node_id: NodeId,
}

#[async_trait::async_trait]
impl NodeObserver for ExitsWhenAsked<'_> {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication> {
        self.live.joined(node_id, incarnation).await
    }
    async fn ready(&self, _node: &Node) -> Result<(), String> {
        let view = self.builds.read_build(&self.build_id).await.unwrap();
        assert!(view.steps.iter().any(|r| r.step == CreateStep::DeployRuntime.name() && r.outcome == StepOutcome::Complete), "the runtime was deployed");
        let handle = common::launched_handle(&self.data_root, &self.node_id);
        self.provider.terminate(&handle, TerminationMode::Immediate, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
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
        let builds = common::stepping(&fabric, "mesh1");
        let build_id = publish_build(&builds, add_node()).await;
        let provider = ProcessDeploymentProvider::new();
        let node_id = rdm_core::fixture_node_id(&rdm_core::PathName::new("mesh1.node", "1").unwrap());
        let observer = ExitsWhenAsked { live: &admin.observer, provider: &provider, builds: &builds, build_id: build_id.clone(), data_root: template.data_root.clone(), node_id };
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
        let req = CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "1").unwrap()))), node_id: (rdm_core::fixture_node_id(&(rdm_core::PathName::new("mesh1.node", "1").unwrap()))).clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build_id), path: "mesh1.node".into(), name: "1".into(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None };
        let asked = std::time::Instant::now();
        let err = pipeline.create(&req).await.err().expect("a create over an exited runtime");
        assert!(asked.elapsed() < Duration::from_secs(60), "the create waited {:?} on a runtime already proven exited", asked.elapsed());
        assert_eq!(err.step, CreateStep::WaitForNodeReady.name());
        assert!(err.reason.contains("runtime exited") && err.reason.contains("before it was Ready"), "{}", err.reason);
        let _ = std::fs::remove_dir_all(&template.data_root);
    })
}
