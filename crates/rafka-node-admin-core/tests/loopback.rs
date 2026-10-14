//! A fabric-primary's dispatcher over executors that live in this process: each admin of the
//! fixture has its own door (`RunDoor`) over the fixture's Build log, its view and its runner, and
//! `build.attempt.run` reaches it without a transport. An executor the fixture has lost is
//! unreachable, as a dead admin's address is.

use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_drive::{Dispatched, Dispatcher, DepartureProof, VerdictSink};
use rafka_node_admin_core::build_op::local_dispatched;
use rafka_node_admin_core::build_run::{AttemptRuns, FramedBuilds, RunDoor};
use rafka_node_admin_core::build_state::{BuildAttemptReceipt, BuildStateAdapter, BuildStepReceipt, LocalBuildLog, MemoryBuildStateAdapter, StepOutcome};
use rafka_node_admin_core::executor::{BuildExecutor, OperationRunner};
use rafka_node_admin_core::model::PathName;
use rafka_node_admin_core::topology::Topology;
use rafka_node_rpc_contract::context::CallContext;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

pub struct Loopback {
    builds: Arc<dyn BuildStateAdapter>,
    local: Arc<dyn LocalBuildLog>,
    topology: Arc<RwLock<Topology>>,
    runner: Arc<dyn OperationRunner>,
    doors: Mutex<BTreeMap<String, Arc<RunDoor>>>,
    /// The view an executor runs from, when it is not the fixture's one.
    views: Mutex<BTreeMap<String, Arc<RwLock<Topology>>>>,
    lost: Mutex<BTreeSet<String>>,
    /// The executors that keep their own Build log, as an admin on another host does: the fabric-primary's
    /// log holds what they write only once the Build topic carries it.
    own: Mutex<BTreeMap<String, Arc<MemoryBuildStateAdapter>>>,
    /// Every executor a dispatch reached, in order: `(executor, attempt)`.
    pub dispatched: Mutex<Vec<(String, u32)>>,
}

/// An executor's runner on its own log: each operation's `Complete` receipt is written through the
/// executor's framed Build state once the operation ran, so the receipt is durable on that executor and
/// its `Step` frame goes out on the call's stream (`FramedBuilds`), as a pipeline's step runner writes it.
struct OwnReceipts {
    inner: Arc<dyn OperationRunner>,
    builds: Arc<dyn BuildStateAdapter>,
}

#[async_trait::async_trait]
impl OperationRunner for OwnReceipts {
    async fn run(&self, build: &BuildId, lane: Option<&str>, attempt: u32, op: &rafka_node_admin_core::build::BuildOperation) -> Result<(), rafka_node_admin_core::executor::RunEnd> {
        self.inner.run(build, lane, attempt, op).await?;
        let receipt = BuildStepReceipt { mesh: lane.map(str::to_string), build_id: build.clone(), attempt, operation: op.key(), step: "Complete".into(), outcome: StepOutcome::Complete, output: None, executor: None };
        self.builds.append_step_receipt(&receipt).await.map_err(|e| e.to_string().into())
    }
}

impl Loopback {
    pub fn new(builds: Arc<dyn BuildStateAdapter>, local: Arc<dyn LocalBuildLog>, topology: Arc<RwLock<Topology>>, runner: Arc<dyn OperationRunner>) -> Arc<Self> {
        Arc::new(Self { builds, local, topology, runner, doors: Mutex::new(BTreeMap::new()), views: Mutex::new(BTreeMap::new()), lost: Mutex::new(BTreeSet::new()), own: Mutex::new(BTreeMap::new()), dispatched: Mutex::new(Vec::new()) })
    }

    /// `executor` keeps its own Build log (returned): the intent a call carries is absorbed there, its
    /// run writes its receipts there and frames them on the call's stream, and nothing of it reaches
    /// the fixture's log but the frames and the terminal.
    pub fn own_log(&self, executor: &str) -> Arc<MemoryBuildStateAdapter> {
        self.own.lock().unwrap().entry(executor.to_string()).or_insert_with(|| Arc::new(MemoryBuildStateAdapter::new())).clone()
    }

    /// `executor` plans and hands off from `view`, which may disagree with the fabric-primary's.
    pub fn sees(&self, executor: &str, view: Arc<RwLock<Topology>>) {
        self.views.lock().unwrap().insert(executor.to_string(), view);
    }

    /// `executor` is gone: nothing reaches it.
    pub fn lose(&self, executor: &str) {
        self.lost.lock().unwrap().insert(executor.to_string());
    }

    /// `executor`'s own door for `build.attempt.run`: its view, its runner, the fixture's log or its own.
    pub fn door(&self, executor: &PathName) -> Arc<RunDoor> {
        self.doors
            .lock()
            .unwrap()
            .entry(executor.to_string())
            .or_insert_with(|| {
                if let Some(own) = self.own.lock().unwrap().get(&executor.to_string()).cloned() {
                    let runs = Arc::new(AttemptRuns::default());
                    let framed: Arc<dyn BuildStateAdapter> = Arc::new(FramedBuilds::new(own.clone(), runs.clone()));
                    let topology = self.views.lock().unwrap().get(&executor.to_string()).cloned().unwrap_or_else(|| self.topology.clone());
                    let runner = Arc::new(OwnReceipts { inner: self.runner.clone(), builds: framed.clone() });
                    let exec = Arc::new(BuildExecutor { executor: executor.to_string(), builds: framed.clone(), topology, runner });
                    return Arc::new(RunDoor { me: executor.clone(), builds: framed, exec, runs, local: own, gate: rafka_node_admin_core::stop_gate::StopGate::new() });
                }
                let exec = Arc::new(BuildExecutor { executor: executor.to_string(), builds: self.builds.clone(), topology: self.views.lock().unwrap().get(&executor.to_string()).cloned().unwrap_or_else(|| self.topology.clone()), runner: self.runner.clone() });
                Arc::new(RunDoor { me: executor.clone(), builds: self.builds.clone(), exec, runs: Arc::new(AttemptRuns::default()), local: self.local.clone(), gate: rafka_node_admin_core::stop_gate::StopGate::new() })
            })
            .clone()
    }
}

#[async_trait::async_trait]
impl Dispatcher for Loopback {
    fn hold(&self, me: &PathName, build_id: &BuildId, attempt: u32) -> Option<rafka_node_admin_core::build_run::HeldRun> {
        self.door(me).hold(build_id, attempt)
    }
    async fn dispatch(&self, _turn: &rafka_node_admin_core::stop_gate::Turn, executor: &PathName, build_id: &BuildId, attempt: u32, context: CallContext, intent: Vec<Vec<u8>>, plan: rafka_node_admin_core::executor::RunPlan) -> Dispatched {
        if self.lost.lock().unwrap().contains(&executor.to_string()) {
            return Dispatched::Unreached(format!("{executor} is gone: nothing answers at its address"));
        }
        self.dispatched.lock().unwrap().push((executor.to_string(), attempt));
        local_dispatched(self.door(executor).attempt_run(build_id, attempt, &executor.to_string(), &context, &intent, &plan).await)
    }
}

/// Departure proof the fixture holds: the executors it has proven exited.
#[derive(Default)]
pub struct Proofs {
    pub exited: Mutex<BTreeSet<String>>,
    /// The executors a call of `proven` proved, as the product records an exit it proved.
    pub recorded: Mutex<BTreeSet<String>>,
    /// How many times `proven` was called.
    pub calls: std::sync::atomic::AtomicUsize,
    /// The meshes the drive told it had been given a node-admin on the proof of a fully crashed mesh,
    /// with the Build and attempt that claimed the create.
    pub given: Mutex<Vec<(String, String, u32)>>,
}

#[async_trait::async_trait]
impl DepartureProof for Proofs {
    async fn proven(&self, executor: &PathName) -> Result<(), String> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.exited.lock().unwrap().contains(&executor.to_string()) {
            self.recorded.lock().unwrap().insert(executor.to_string());
            Ok(())
        } else {
            Err(format!("{executor}'s runtime is not proven exited"))
        }
    }
    async fn recorded(&self, executor: &PathName) -> bool {
        self.recorded.lock().unwrap().contains(&executor.to_string())
    }
    async fn mesh_given_node_admin(&self, mesh: &str, build_id: &rafka_node_admin_core::build::BuildId, attempt: u32, _from: std::time::Instant) {
        self.given.lock().unwrap().push((mesh.to_string(), build_id.to_string(), attempt));
    }
    async fn exits_of(&self, nodes: &[PathName]) -> rafka_node_admin_core::build_drive::ExitProofs {
        let exited = self.exited.lock().unwrap();
        let (proven, unproven): (Vec<&PathName>, Vec<&PathName>) = nodes.iter().partition(|a| exited.contains(&a.to_string()));
        rafka_node_admin_core::build_drive::ExitProofs { proven: proven.iter().map(|a| a.to_string()).collect(), unproven: unproven.iter().map(|a| format!("{a}: its runtime is not proven exited: Running")).collect() }
    }
}

/// Fixtures whose admins share one Build log need no verdict recorded.
pub struct NoVerdicts;

#[async_trait::async_trait]
impl VerdictSink for NoVerdicts {
    async fn record(&self, _receipt: BuildAttemptReceipt) {}
}
