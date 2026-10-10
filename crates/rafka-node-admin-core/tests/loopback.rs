//! A fabric-primary's dispatcher over executors that live in this process: each admin of the
//! fixture has its own door (`RunDoor`) over the fixture's Build log, its view and its runner, and
//! `build.attempt.run` reaches it without a transport. An executor the fixture has lost is
//! unreachable, as a dead admin's address is.

use rafka_node_admin_core::build::BuildId;
use rafka_node_admin_core::build_drive::{Dispatched, Dispatcher, DepartureProof, VerdictSink};
use rafka_node_admin_core::build_op::local_dispatched;
use rafka_node_admin_core::build_run::{AttemptRuns, RunDoor};
use rafka_node_admin_core::build_state::{BuildAttemptReceipt, BuildStateAdapter, LocalBuildLog};
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
    /// Every executor a dispatch reached, in order: `(executor, attempt)`.
    pub dispatched: Mutex<Vec<(String, u32)>>,
}

impl Loopback {
    pub fn new(builds: Arc<dyn BuildStateAdapter>, local: Arc<dyn LocalBuildLog>, topology: Arc<RwLock<Topology>>, runner: Arc<dyn OperationRunner>) -> Arc<Self> {
        Arc::new(Self { builds, local, topology, runner, doors: Mutex::new(BTreeMap::new()), views: Mutex::new(BTreeMap::new()), lost: Mutex::new(BTreeSet::new()), dispatched: Mutex::new(Vec::new()) })
    }

    /// `executor` plans and hands off from `view`, which may disagree with the fabric-primary's.
    pub fn sees(&self, executor: &str, view: Arc<RwLock<Topology>>) {
        self.views.lock().unwrap().insert(executor.to_string(), view);
    }

    /// `executor` is gone: nothing reaches it.
    pub fn lose(&self, executor: &str) {
        self.lost.lock().unwrap().insert(executor.to_string());
    }

    fn door(&self, executor: &PathName) -> Arc<RunDoor> {
        self.doors
            .lock()
            .unwrap()
            .entry(executor.to_string())
            .or_insert_with(|| {
                let exec = Arc::new(BuildExecutor { executor: executor.to_string(), builds: self.builds.clone(), topology: self.views.lock().unwrap().get(&executor.to_string()).cloned().unwrap_or_else(|| self.topology.clone()), runner: self.runner.clone() });
                Arc::new(RunDoor { me: executor.clone(), builds: self.builds.clone(), exec, runs: Arc::new(AttemptRuns::default()), local: self.local.clone() })
            })
            .clone()
    }
}

#[async_trait::async_trait]
impl Dispatcher for Loopback {
    async fn dispatch(&self, executor: &PathName, build_id: &BuildId, attempt: u32, context: CallContext, intent: Vec<Vec<u8>>, plan: rafka_node_admin_core::executor::RunPlan) -> Dispatched {
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
}

#[async_trait::async_trait]
impl DepartureProof for Proofs {
    async fn proven(&self, executor: &PathName) -> Result<(), String> {
        if self.exited.lock().unwrap().contains(&executor.to_string()) {
            Ok(())
        } else {
            Err(format!("{executor}'s runtime is not proven exited"))
        }
    }
}

/// Fixtures whose admins share one Build log need no verdict recorded.
pub struct NoVerdicts;

#[async_trait::async_trait]
impl VerdictSink for NoVerdicts {
    async fn record(&self, _receipt: BuildAttemptReceipt) {}
}
