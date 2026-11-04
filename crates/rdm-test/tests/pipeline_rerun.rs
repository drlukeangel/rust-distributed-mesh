//! i143.e2.s5 functional: a create pipeline killed mid-step is re-run as the
//! next run of the same Build and completes without duplicating effects:
//! one runtime, the address it reported, every step `Complete` exactly once.
//!
//! The kill drops the pipeline's future at a chosen point (the executor
//! dying), so nothing after that point runs, not even its step event.

use crate::common;

use common::{add_node, admin_side, publish_build, template, LiveMesh, Published};
use rdm_core::{FabricId, IncarnationId, NodeId};
use rdm_admin::build::BuildId;
use rdm_admin::build_state::{BuildAccepted, BuildFact, BuildProjection, BuildStateAdapter, BuildStateError, MemoryBuildStateAdapter, StepOutcome};
use rdm_admin::deployment::endpoint::NODE_SPEC;
use rdm_admin::deployment::pipeline::{CreateRequest, CreateStep, DeploymentPipeline, NodeObserver, Publication, Timeouts};
use rdm_admin::join::Joins;
use rdm_admin::storage::NodesStorage;
use rdm_admin::deployment::process::ProcessDeploymentProvider;
use rdm_admin::deployment::provider::{
    DeployError, DeploymentHandle, DeploymentProvider, DeploymentStatus, ResolvedNodeLaunch, TerminationMode,
};
use rdm_admin::model::{Node, ProviderKind};
use std::net::IpAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;

/// Where the executor dies.
#[derive(Clone, Copy, PartialEq)]
enum KillAt {
    /// The runtime is spawned; the `DeployRuntime` step event is never written.
    AfterSpawn,
    /// `WaitForMeshJoin` completed from the birth's report; its step event is never written.
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
    async fn spawn(&self, spec: &ResolvedNodeLaunch, deadline: rdm_admin::deployment::deadline::Deadline) -> Result<DeploymentHandle, DeployError> {
        let h = self.inner.spawn(spec, deadline).await?;
        self.spawned.lock().unwrap().push(h.pid.unwrap());
        if self.kill_at == Some(KillAt::AfterSpawn) {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        Ok(h)
    }
    async fn terminate(&self, h: &DeploymentHandle, m: TerminationMode, deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        self.inner.terminate(h, m, deadline).await
    }
    async fn inspect(&self, h: &DeploymentHandle, deadline: rdm_admin::deployment::deadline::Deadline) -> DeploymentStatus {
        self.inner.inspect(h, deadline).await
    }
    async fn signal_stop(&self, h: &DeploymentHandle, deadline: rdm_admin::deployment::deadline::Deadline) -> Result<(), DeployError> {
        self.inner.signal_stop(h, deadline).await
    }
    async fn find(&self, spec: &ResolvedNodeLaunch, deadline: rdm_admin::deployment::deadline::Deadline) -> Result<rdm_admin::deployment::provider::Found, rdm_admin::deployment::provider::DeployError> {
        self.inner.find(spec, deadline).await
    }
}

/// A run's step log over memory, pausing forever before one step's `rdm.build.step` event when armed:
/// the executor dying after the step ran and before its event was written.
struct Pausing<'a> {
    inner: &'a rdm_admin::build_run::FramedBuilds,
    before: Option<&'static str>,
    reached: Arc<Notify>,
}

#[async_trait::async_trait]
impl BuildStateAdapter for Pausing<'_> {
    async fn publish_accepted(&self, accepted: &BuildAccepted) -> Result<(), BuildStateError> {
        self.inner.publish_accepted(accepted).await
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
        if self.before == Some(step.step.as_str()) {
            self.reached.notify_one();
            std::future::pending::<()>().await;
        }
        self.inner.write_step(step).await
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

/// Every runtime of `node_id` still running when the cell ends, its assertions failed included, is
/// killed: a red cell leaves no runtime for the gate's leaked-processes step to find.
struct Reap(NodeId);

impl Drop for Reap {
    fn drop(&mut self) {
        for pid in runtimes_of(&self.0) {
            let _ = std::process::Command::new("kill").args(["-9", &pid.to_string()]).status();
        }
    }
}

async fn killed_then_rerun(kill_at: KillAt) {
    let fabric = FabricId::mint();
    let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
    let template = template(&admin);
    let builds = crate::common::stepping_as(&fabric, "mesh1", &admin.launcher);
    let build_id = publish_build(&builds, add_node()).await;
    let nodes = crate::common::store_root_nodes(&admin, &template).await;
    let sink = Published::with_rows(admin.node_rows_on(nodes.clone()));
    let reached = Arc::new(Notify::new());
    let process = Arc::new(ProcessDeploymentProvider::new());
    // The node.id the Build minted for the node, one per cell: a cell beside it never runs a runtime of it.
    let node_id = rdm_core::NodeId::mint();
    let _reap = Reap(node_id.clone());
    let req = || CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &node_id), node_id: node_id.clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build_id.clone()), path: "mesh1.node".into(), name: "1".into(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None };

    // The first run dies at the kill point.
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
    let first_run = req();
    tokio::select! {
        r = pipeline.create(&first_run) => panic!("the first run was to die mid-step, but finished: {:?}", r.map(|c| c.node.name)),
        _ = reached.notified() => {}
    }
    let first_pid = *first.spawned.lock().unwrap().first().expect("the first run spawned the runtime");
    // A run past WaitForBind: the birth writes its own part of its record as it bound at its join, the
    // value the rerun reads for that step (D67).
    if kill_at == KillAt::InWaitForMeshJoin {
        crate::common::until_bound(&nodes, &node_id).await;
    }
    // Where the first run's birth reported it bound, when it got as far as publishing it.
    let reported_before_kill = sink.nodes.lock().unwrap().first().and_then(|n| n.transport_addr);

    // The rerun: the real provider and observer, the same Build.
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
    let rerun = req();
    let created = pipeline.create(&rerun).await.unwrap_or_else(|e| panic!("the re-run failed: {e}"));

    // No duplicated effect: one runtime (the one the first run started), the
    // same endpoints, no extra port held.
    assert!(second.spawned.lock().unwrap().is_empty(), "the re-run spawned a second runtime");
    assert_eq!(created.handle.pid, Some(first_pid));
    assert_eq!(runtimes_of(&created.node.node_id), vec![first_pid], "exactly one runtime runs for the node");
    if let Some(addr) = reported_before_kill {
        assert_eq!(created.node.transport_addr, Some(addr), "the re-run keeps the address the birth reported");
    }

    // Every step Complete exactly once across both runs: the steps before the kill point keep the
    // first run's event and the rerun runs only the rest.
    let view = builds.read_build(&build_id).await.unwrap();
    let split = match kill_at {
        KillAt::AfterSpawn => CreateStep::DeployRuntime,
        KillAt::InWaitForMeshJoin => CreateStep::WaitForMeshJoin,
    };
    let split_at = CreateStep::ORDER.iter().position(|s| *s == split).unwrap();
    for (i, step) in CreateStep::ORDER.iter().enumerate() {
        let done = view.steps.iter().filter(|r| r.step == step.name() && r.outcome == StepOutcome::Complete).count();
        assert_eq!(done, 1, "{} (step {i} of {}, split at {split_at}): Complete step events", step.name(), CreateStep::ORDER.len());
    }

    process.terminate(&created.handle, TerminationMode::Graceful { grace: Duration::from_secs(5) }, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
    let _ = std::fs::remove_dir_all(&template.data_root);
}

/// The live observer, except a gossip read of a birth's join is a defect of the re-run.
struct NoGossip<'a> {
    live: &'a LiveMesh,
}

#[async_trait::async_trait]
impl NodeObserver for NoGossip<'_> {
    async fn joined(&self, node_id: &NodeId, incarnation: &IncarnationId) -> Option<Publication> {
        panic!("the re-run read gossip for the join of {node_id} incarnation {}", incarnation.as_str())
    }
    async fn ready(&self, node: &Node) -> Result<(), String> {
        self.live.ready(node).await
    }
}

/// CONTRACT (node-create.md step 5: the accepting authority completes WaitForBind / WaitForMeshJoin
/// "from the validated report, never from gossip"; "Join completion and failure evidence": a digest
/// is insufficient): the admin process is restarted after the birth's join was admitted and before
/// the WaitForMeshJoin step event. The re-run, holding nothing of the admitted birth in memory,
/// completes the step from the report the authority kept durable at admission (the birth's runtime
/// row in nodes.storage: the exact birth, the runtime and data dir the report carried), reading no
/// gossip; the birth never sends its JoinNode twice, one runtime runs, and every step is Complete
/// exactly once. Must NOT happen: the step ending by deadline, or completing from a topology digest.
// @feature: node-lifecycle
#[test]
fn a_pipeline_rerun_on_a_restarted_admin_completes_the_join_from_the_durable_report() {
    let cap = crate::common::capture("a_pipeline_rerun_on_a_restarted_admin_completes_the_join_from_the_durable_report");
    cap.run(crate::common::Threads::Workers(4), async move {
        let fabric = FabricId::mint();
        let admin = admin_side(IpAddr::from([127, 0, 0, 1]), &fabric).await;
        let template = template(&admin);
        let builds = crate::common::stepping_as(&fabric, "mesh1", &admin.launcher);
        let build_id = publish_build(&builds, add_node()).await;
        let nodes = crate::common::store_root_nodes(&admin, &template).await;
        let sink = Published::with_rows(admin.node_rows_on(nodes.clone()));
        let reached = Arc::new(Notify::new());
        let process = Arc::new(ProcessDeploymentProvider::new());
        // The node.id the Build minted for the node, one per cell: a cell beside it never runs a runtime of it.
        let node_id = rdm_core::NodeId::mint();
        let req = || CreateRequest { predecessor: None, operation: rdm_core::OperationId::node("node.create", &node_id), node_id: node_id.clone(), mesh: "mesh1".into(), r#type: "node".into(), build_id: Some(build_id.clone()), path: "mesh1.node".into(), name: "1".into(), spec: &NODE_SPEC, data_dir: None, held_runtimes: Vec::new(), entry: None };

        // The first run dies after the join was admitted (WaitForBind's step event is written) and before
        // the WaitForMeshJoin step event.
        let first = Killable { inner: process.clone(), kill_at: None, reached: reached.clone(), spawned: Mutex::new(vec![]) };
        let paused = Pausing { inner: &builds, before: Some(CreateStep::WaitForMeshJoin.name()), reached: reached.clone() };
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
        let first_run = req();
        tokio::select! {
            r = pipeline.create(&first_run) => panic!("the first run was to die before the WaitForMeshJoin step event, but finished: {:?}", r.map(|c| c.node.name)),
            _ = reached.notified() => {}
        }
        let first_pid = *first.spawned.lock().unwrap().first().expect("the first run spawned the runtime");
        // The birth writes its own part of its record as it bound at its join: the value the rerun reads
        // for WaitForBind (D67).
        crate::common::until_bound(&nodes, &node_id).await;

        // The admin process restarts: nothing of the admitted birth is in memory; its durable rows stand.
        let restarted = Joins::over(admin.nodes_storage.clone());
        let no_gossip = NoGossip { live: &admin.observer };
        let second = Killable { inner: process.clone(), kill_at: None, reached, spawned: Mutex::new(vec![]) };
        let pipeline = DeploymentPipeline {
            provider: &second,
            joins: &restarted,
            observer: &no_gossip,
            sink: &sink,
            lifecycle: &rdm_admin::deployment::pipeline::NoLifecycleEvents,
            builds: &builds,
            template: &template,
            timeouts: Timeouts { join: Duration::from_secs(5), ..Timeouts::default() },
        };
        let rerun = req();
        let created = pipeline.create(&rerun).await.unwrap_or_else(|e| panic!("the re-run on the restarted admin failed: {e}"));

        assert!(second.spawned.lock().unwrap().is_empty(), "the re-run spawned a second runtime");
        assert_eq!(created.handle.pid, Some(first_pid));
        assert_eq!(runtimes_of(&created.node.node_id), vec![first_pid], "exactly one runtime runs for the node");
        // The durable report: the birth's own record, which the node wrote at its start (Q356): the runtime
        // made available to it and its data dir.
        let incarnation = created.node.incarnation_id.clone().expect("the created node carries its incarnation");
        let row = rdm_admin::node_rows::birth_record(&*admin.nodes_storage, &created.node.node_id, &incarnation).await.unwrap().expect("the node wrote its own record at its start");
        assert_eq!(Some(row.runtime), created.handle.fact(), "the record carries the runtime the report carried");
        assert_eq!(row.storage.data_dir, created.node.data_dir, "the record carries the data dir the report carried");

        // Every step Complete exactly once across both runs; WaitForMeshJoin and after are the rerun's.
        let view = builds.read_build(&build_id).await.unwrap();
        let split_at = CreateStep::ORDER.iter().position(|s| *s == CreateStep::WaitForMeshJoin).unwrap();
        for (i, step) in CreateStep::ORDER.iter().enumerate() {
            let done = view.steps.iter().filter(|r| r.step == step.name() && r.outcome == StepOutcome::Complete).count();
            assert_eq!(done, 1, "{} (step {i} of {}, split at {split_at}): Complete step events", step.name(), CreateStep::ORDER.len());
        }

        process.terminate(&created.handle, TerminationMode::Graceful { grace: Duration::from_secs(5) }, rdm_admin::deployment::deadline::Deadline::step()).await.unwrap();
        let _ = std::fs::remove_dir_all(&template.data_root);
    })
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
