//! A fabric-primary's dispatcher over admins that live in this process: each admin of the fixture has
//! its own `SectionRunService` over the fixture's Build store, its view and its runner, and
//! `build.section.run` reaches it without a transport. Each admin writes the steps it runs as its own
//! `rdm.build.step` events into the fixture's one events store (`FramedBuilds`), as a mesh-primary
//! writes its mesh's steps (build.md §5). An admin the fixture has lost is unreachable, as a dead
//! admin's address is.

use rdm_admin::build::{BuildId, BuildOperation};
use rdm_admin::build_drive::{Dispatched, Dispatcher};
use rdm_admin::build_op::local_dispatched;
use rdm_admin::build_run::{FramedBuilds, SectionRunService, SectionRuns};
use rdm_admin::build_state::{BuildStateAdapter, LocalBuildLog, StepOutcome, StepWrite};
use rdm_admin::executor::{BuildExecutor, OperationRunner, RunEnd};
use rdm_admin::topology::Topology;
use rdm_core::context::CallContext;
use rdm_core::{NodeId, framing::Birth};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;

pub struct Loopback {
    builds: Arc<dyn BuildStateAdapter>,
    local: Arc<dyn LocalBuildLog>,
    topology: Arc<RwLock<Topology>>,
    runner: Arc<dyn OperationRunner>,
    services: Mutex<std::collections::HashMap<Birth, Arc<SectionRunService>>>,
    /// The view an admin plans its section from, when it is not the fixture's one.
    views: Mutex<BTreeMap<NodeId, Arc<RwLock<Topology>>>>,
    lost: Mutex<BTreeSet<NodeId>>,
    /// Every section that reached an admin, in order: `(admin, mesh)`, the fabric-primary's own
    /// sections among them.
    pub dispatched: Mutex<Vec<(Birth, String)>>,
}

/// An admin's runner as its pipelines run an operation: the operation's `Complete` step, or the step a
/// `<step> failed: <reason>` failure names (a `PipelineError` reads so), is written through the admin's
/// step log, so its event is durable and its frame goes out on the section's stream.
pub struct Steps {
    inner: Arc<dyn OperationRunner>,
    builds: Arc<dyn BuildStateAdapter>,
}

impl Steps {
    /// `inner`, each operation's step written through `builds`, an admin's step log.
    pub fn new(inner: Arc<dyn OperationRunner>, builds: Arc<dyn BuildStateAdapter>) -> Self {
        Self { inner, builds }
    }
}

#[async_trait::async_trait]
impl OperationRunner for Steps {
    async fn run(&self, build: &BuildId, op: &BuildOperation) -> Result<(), RunEnd> {
        let write = |step: &str, outcome| StepWrite { build_id: build.clone(), mesh: None, operation: op.key(), step: step.into(), outcome };
        if let Err(end) = self.inner.run(build, op).await {
            if let RunEnd::Failed { reason, code } = &end {
                if let Some((step, why)) = reason.split_once(" failed: ") {
                    self.builds.write_step(&write(step, StepOutcome::Failed { reason: why.into(), code: *code })).await.map_err(|e| RunEnd::from(e.to_string()))?;
                }
            }
            return Err(end);
        }
        self.builds.write_step(&write("Complete", StepOutcome::Complete)).await.map_err(|e| e.to_string().into())
    }
}

impl Loopback {
    pub fn new(builds: Arc<dyn BuildStateAdapter>, local: Arc<dyn LocalBuildLog>, topology: Arc<RwLock<Topology>>, runner: Arc<dyn OperationRunner>) -> Arc<Self> {
        Arc::new(Self { builds, local, topology, runner, services: Mutex::new(std::collections::HashMap::new()), views: Mutex::new(BTreeMap::new()), lost: Mutex::new(BTreeSet::new()), dispatched: Mutex::new(Vec::new()) })
    }

    /// `admin` plans its section from `view`, which may disagree with the fabric-primary's.
    pub fn sees(&self, admin: &NodeId, view: Arc<RwLock<Topology>>) {
        self.views.lock().unwrap().insert(admin.clone(), view);
    }

    /// `admin` is gone: nothing reaches it.
    pub fn lose(&self, admin: &NodeId) {
        self.lost.lock().unwrap().insert(admin.clone());
    }

    /// The sections that reached an admin, as `(admin, mesh)` in order.
    pub fn sections(&self) -> Vec<(Birth, String)> {
        self.dispatched.lock().unwrap().clone()
    }

    /// `admin`'s own `SectionRunService` for `build.section.run`: its view, its runner, and its step log over the
    /// fixture's Build store, writing as `admin` of its own mesh.
    pub fn service(&self, admin: &Birth) -> Arc<SectionRunService> {
        self.services
            .lock()
            .unwrap()
            .entry(admin.clone())
            .or_insert_with(|| {
                let (me, mesh, fabric_id) = {
                    let t = self.topology.try_read().expect("fixture topology is unlocked while opening a `SectionRunService`");
                    let n = t.own(&admin.node_id).expect("fixture admin is listed");
                    (n.location(), n.mesh.clone(), t.fabric.id.clone())
                };
                let runs = Arc::new(SectionRuns::default());
                let mut events = rdm_admin::events_storage::EventsWriter::on(self.builds.events(), &fabric_id, &admin.node_id);
                events.source.incarnation = admin.incarnation.clone();
                let builds: Arc<dyn BuildStateAdapter> = Arc::new(FramedBuilds::new(self.builds.clone(), runs.clone()).writing_steps(events, mesh));
                let topology = self.views.lock().unwrap().get(&admin.node_id).cloned().unwrap_or_else(|| self.topology.clone());
                let runner = Arc::new(Steps { inner: self.runner.clone(), builds: builds.clone() });
                let exec = Arc::new(BuildExecutor { executor: admin.clone(), builds: builds.clone(), topology, runner });
                Arc::new(SectionRunService { me_path: me.path.clone(), me_name: me.name.clone(), builds, exec, runs, local: self.local.clone(), gate: rdm_admin::stop_gate::StopGate::new(), moved: Default::default() })
            })
            .clone()
    }

    /// The fabric-primary of the fixture's view now.
    async fn fabric_primary(&self) -> Birth {
        self.topology.read().await.fabric_primary().and_then(|n| n.birth()).expect("the fixture's view names a fabric-primary")
    }
}

#[async_trait::async_trait]
impl Dispatcher for Loopback {
    async fn dispatch(&self, build_id: &BuildId, mesh: &str, context: CallContext) -> Dispatched {
        let primary = rdm_admin::executor::mesh_primary(mesh, &*self.topology.read().await);
        let Some(primary) = primary else {
            return Dispatched::Unreached(format!("mesh {mesh} has no primary in the view: the call's attempts are spent"));
        };
        if self.lost.lock().unwrap().contains(&primary.node_id) {
            return Dispatched::Unreached(format!("{primary} is gone: nothing answers at its address"));
        }
        self.dispatched.lock().unwrap().push((primary.clone(), mesh.to_string()));
        local_dispatched(self.service(&primary).section_run(build_id, mesh, &context).await)
    }

    async fn run_own(&self, turn: &rdm_admin::stop_gate::Turn, build_id: &BuildId, mesh: &str, context: CallContext, operations: Vec<BuildOperation>) -> Dispatched {
        let me = self.fabric_primary().await;
        self.dispatched.lock().unwrap().push((me.clone(), mesh.to_string()));
        local_dispatched(self.service(&me).run_under(turn, build_id, mesh, &context, operations).await)
    }
}
